//! Backend-neutral `wcmp` host adapter for WIT `presentation-client` micro components
//! (ADR 018 §2, §4).
//!
//! Generated bindings, the resource table, and store semantics live here; native and
//! browser adapters supply only their engine. Surface and placement state, grant checks,
//! handle caps, ingress budgets, and document delivery are delegated to
//! `clay_guest_session`, so engines cannot diverge. Presentation calls poll the guest
//! once and panic on `Pending` (ADR 018 §3); activation is async.

mod documents;
mod wasi;

use clay_docs::DocumentRef;
use clay_guest_session::DocumentSession;
use clay_guest_session::Limits as DocumentLimits;
use clay_guest_session::Work;
use clay_guest_session::ingress_budget;
use clay_guest_session::presentation::map_mutations;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::Instance;
use clay_surface::AttributeValue;
use clay_surface::Event;
use clay_surface::EventResponse;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::SurfaceSession;
use clay_surface::SurfaceSink;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tokio::sync::mpsc;
use wcmp::Component;
use wcmp::Engine;
use wcmp::HasSelf;
use wcmp::Linker;
use wcmp::Resource;
use wcmp::ResourceTable;
use wcmp::Store;

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] clay_host::AccessError),
    #[error("Wasm initialization failed: {0}")]
    Initialization(String),
}

mod bindings {
    wcmp::bindgen!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        imports: { default: trappable },
        with: {
            "townframe:dayframe/presentation.surface": super::WcmpSurface,
            "townframe:dayframe/presentation.mount-point": super::WcmpPlacement,
            "townframe:dayframe/presentation.component-ref": super::WcmpComponentRef,
            "townframe:dayframe/documents.document-access": super::documents::WasmDocumentAccess,
            "townframe:dayframe/documents.document-watch": super::documents::WasmDocumentWatch,
            "townframe:dayframe/documents.operation": super::documents::WasmOperation,
        },
    });
}

use bindings::MicroClient;
use bindings::exports::townframe::dayframe::presentation_events;
pub use bindings::townframe::dayframe::presentation as wit_presentation;

pub struct WcmpComponentRef(ComponentRef);

pub use clay_guest_session::Placement as WcmpPlacement;
pub use clay_guest_session::Surface as WcmpSurface;

pub struct WcmpStoreState {
    pub table: ResourceTable,
    pub documents: DocumentSession,
    pub presentation: clay_guest_session::PresentationHost,
}

impl WcmpStoreState {
    pub fn new(
        host: Host,
        instance: Instance,
        limits: DocumentLimits,
    ) -> Result<(Self, mpsc::Receiver<Work>), clay_host::AccessError> {
        let (documents, incoming) = DocumentSession::new(host.clone(), instance.clone(), limits)?;
        Ok((
            Self {
                table: ResourceTable::new(),
                documents,
                presentation: clay_guest_session::PresentationHost::new(host, instance),
            },
            incoming,
        ))
    }

    pub fn grant_components(
        &mut self,
        components: &[ComponentRef],
    ) -> Result<Vec<Resource<WcmpComponentRef>>, PresentationError> {
        self.presentation.admit_components(components.len())?;
        Ok(components
            .iter()
            .map(|&reference| {
                self.table
                    .push(WcmpComponentRef(reference))
                    .expect("bounded component resource table")
            })
            .collect())
    }
}

/// Drive a presentation call to completion on its first poll. Our WIT has no async function,
/// stream, or future, so a guest call that returns `Pending` has suspended where the contract has
/// no suspension point: that is a broken invariant (ADR 018 §3), not a condition to wait out.
pub fn first_poll<F: Future>(call: F) -> F::Output {
    match std::pin::pin!(call).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("guest presentation call suspended on its first poll"),
    }
}

pub fn convert_mutation(
    mutation: wit_presentation::Mutation,
) -> Mutation<Resource<WcmpComponentRef>> {
    use wit_presentation::Mutation as Wire;
    match mutation {
        Wire::PushId(id) => Mutation::PushId(id),
        Wire::SetId(id) => Mutation::SetId(id),
        Wire::Child(index) => Mutation::Child(index),
        Wire::Pop => Mutation::Pop,
        Wire::CreateElement(element) => Mutation::CreateElement(element),
        Wire::CreateText(text) => Mutation::CreateText(text),
        Wire::MountComponent(reference) => Mutation::MountComponent(reference),
        Wire::CloneNode => Mutation::CloneNode,
        Wire::AppendChildren(count) => Mutation::AppendChildren(count),
        Wire::ReplaceWith(count) => Mutation::ReplaceWith(count),
        Wire::InsertAfter(count) => Mutation::InsertAfter(count),
        Wire::InsertBefore(count) => Mutation::InsertBefore(count),
        Wire::SetAttribute((name, namespace, value)) => Mutation::SetAttribute((
            name,
            namespace,
            match value {
                wit_presentation::AttributeValue::Text(text) => AttributeValue::Text(text),
                wit_presentation::AttributeValue::Float(float) => AttributeValue::Float(float),
                wit_presentation::AttributeValue::Int(int) => AttributeValue::Int(int),
                wit_presentation::AttributeValue::Boolean(bool) => AttributeValue::Boolean(bool),
                wit_presentation::AttributeValue::None => AttributeValue::None,
            },
        )),
        Wire::SetText(text) => Mutation::SetText(text),
        Wire::AddEventListener(name) => Mutation::AddEventListener(name),
        Wire::RemoveEventListener(name) => Mutation::RemoveEventListener(name),
        Wire::Remove => Mutation::Remove,
    }
}

fn convert_error(error: PresentationError) -> wit_presentation::PresentationError {
    match error {
        PresentationError::Revoked => wit_presentation::PresentationError::Revoked,
        PresentationError::Unsupported(message) => {
            wit_presentation::PresentationError::Unsupported(message)
        }
        PresentationError::InvalidBatch(message) => {
            wit_presentation::PresentationError::InvalidBatch(message)
        }
    }
}

impl wit_presentation::Host for WcmpStoreState {}

impl wit_presentation::HostComponentRef for WcmpStoreState {
    fn drop(&mut self, component: Resource<WcmpComponentRef>) -> wcmp::Result<()> {
        self.table.delete(component)?;
        self.presentation.release_component();
        Ok(())
    }
}

impl wit_presentation::HostMountPoint for WcmpStoreState {
    fn open(
        &mut self,
        placement: Resource<WcmpPlacement>,
    ) -> wcmp::Result<Result<Resource<WcmpSurface>, wit_presentation::PresentationError>> {
        let surface = {
            let placement = self.table.get_mut(&placement)?;
            match self.presentation.open(placement) {
                Ok(surface) => surface,
                Err(error) => return Ok(Err(convert_error(error))),
            }
        };
        Ok(Ok(self.table.push(surface)?))
    }

    fn drop(&mut self, placement: Resource<WcmpPlacement>) -> wcmp::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}

impl wit_presentation::HostSurface for WcmpStoreState {
    fn apply(
        &mut self,
        surface: Resource<WcmpSurface>,
        edits: Vec<wit_presentation::Mutation>,
    ) -> wcmp::Result<Result<u64, wit_presentation::PresentationError>> {
        let edits: Vec<_> = edits.into_iter().map(convert_mutation).collect();
        if let Err(reason) = ingress_budget(&edits) {
            self.table.get(&surface)?.writer().withdraw();
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                reason.into(),
            )));
        }
        let converted = map_mutations(edits, |reference| {
            self.table.get(&reference).map(|component| component.0)
        })?;
        let surface = self.table.get(&surface)?.clone();
        Ok(self
            .presentation
            .apply(&surface, &converted)
            .map_err(convert_error))
    }

    fn retain(
        &mut self,
        surface: Resource<WcmpSurface>,
    ) -> wcmp::Result<Result<Resource<WcmpSurface>, wit_presentation::PresentationError>> {
        let surface = self.table.get(&surface)?.clone();
        let retained = match self.presentation.retain(&surface) {
            Ok(retained) => retained,
            Err(error) => return Ok(Err(convert_error(error))),
        };
        Ok(Ok(self.table.push(retained)?))
    }

    fn identity(&mut self, surface: Resource<WcmpSurface>) -> wcmp::Result<u64> {
        let surface = self.table.get(&surface)?;
        self.presentation
            .identity(surface)
            .map_err(|error| wcmp::Error::Internal {
                message: error.to_string(),
            })
    }

    fn drop(&mut self, surface: Resource<WcmpSurface>) -> wcmp::Result<()> {
        self.table.delete(surface)?;
        self.presentation.release_surface();
        Ok(())
    }
}

/// Compiled Wasm component and pre-configured linker reusable across incarnations.
pub struct WcmpPackage {
    engine: Engine,
    component: Component,
    linker: Linker<WcmpStoreState>,
}

impl WcmpPackage {
    pub async fn load(engine: Engine, bytes: &[u8]) -> Result<Self, String> {
        let component = Component::new(&engine, bytes)
            .await
            .map_err(|error| error.to_string())?;
        let mut linker = Linker::new(&engine);
        MicroClient::add_to_linker::<WcmpStoreState, HasSelf<WcmpStoreState>>(
            &mut linker,
            |state| state,
        )
        .map_err(|error| error.to_string())?;
        wasi::define(&mut linker, "dayframe").map_err(|error| error.to_string())?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    fn instantiate_with_incoming(
        &self,
        host: &Host,
        instance: Instance,
    ) -> Result<(WcmpStoreState, mpsc::Receiver<Work>), ActivationError> {
        WcmpStoreState::new(host.clone(), instance, DocumentLimits::default())
            .map_err(ActivationError::from)
    }
}

struct WcmpIncarnation {
    store: Store<WcmpStoreState>,
    client: MicroClient,
}

/// One activated incarnation. It is `!Send`: the `wcmp` store is not thread-safe, so it lives
/// behind an `Rc` in the surface factory's closures, which already admit non-thread-safe state
/// (ADR 018 §4).
#[derive(Clone)]
pub struct WcmpRuntime {
    host: Host,
    instance: Instance,
    incarnation: Rc<RefCell<Option<WcmpIncarnation>>>,
    revoked: Arc<AtomicBool>,
}

impl WcmpRuntime {
    pub async fn activate(
        host: &Host,
        instance: Instance,
        package: &WcmpPackage,
        configs: BTreeMap<String, DocumentRef>,
    ) -> Result<(Self, impl Future<Output = ()> + 'static), ActivationError> {
        let (state, incoming) = package.instantiate_with_incoming(host, instance.clone())?;
        let mut store = Store::new(&package.engine, state).map_err(|error| {
            ActivationError::Initialization(format!("creating Wcmp store: {error}"))
        })?;
        let client =
            MicroClient::instantiate_async(&mut store, &package.component, &package.linker)
                .await
                .map_err(|error| {
                    ActivationError::Initialization(format!(
                        "instantiating Wcmp component: {error}"
                    ))
                })?;
        let mut active = WcmpIncarnation { store, client };
        documents::activate(&mut active, configs).await?;
        let pending_limit = active.store.data().documents.limits().pending_deliveries;
        let incarnation = Rc::new(RefCell::new(Some(active)));
        let driver_incarnation = Rc::clone(&incarnation);
        let driver_host = host.clone();
        let driver_instance = instance.clone();
        let driver = async move {
            clay_guest_session::drive(
                driver_host,
                driver_instance,
                incoming,
                pending_limit,
                move |work, delivery| documents::deliver(&driver_incarnation, work, delivery),
            )
            .await
        };
        let revoked = Arc::new(AtomicBool::new(false));
        let cleanup_revoked = Arc::clone(&revoked);
        host.own(
            &instance,
            Box::new(move || {
                cleanup_revoked.store(true, Ordering::Release);
            }),
        )?;
        Ok((
            Self {
                host: host.clone(),
                instance,
                incarnation,
                revoked,
            },
            driver,
        ))
    }

    pub fn mount(
        &self,
        writer: Arc<dyn SurfaceSink<ComponentRef>>,
        components: &[ComponentRef],
    ) -> Result<SurfaceSession, PresentationError> {
        for &reference in components {
            self.host
                .check_component(reference)
                .map_err(|error| match error {
                    clay_host::AccessError::Revoked => PresentationError::Revoked,
                    clay_host::AccessError::Denied => PresentationError::InvalidBatch(
                        "component grant belongs to another host".into(),
                    ),
                })?;
        }
        let surface = self.mount_guest_surface(&writer, components)?;
        let dispatch = self.clone();
        let dispatch_writer = Arc::clone(&writer);
        let unmount = self.clone();
        Ok(SurfaceSession::with_drop(
            move |event: Event| dispatch.dispatch(surface, &dispatch_writer, event),
            move || unmount.unmount(surface, &writer),
        ))
    }

    fn mount_guest_surface(
        &self,
        writer: &Arc<dyn SurfaceSink<ComponentRef>>,
        components: &[ComponentRef],
    ) -> Result<u32, PresentationError> {
        let mut guard = self.incarnation.borrow_mut();
        if self.host.check(&self.instance).is_err() {
            return Err(PresentationError::Revoked);
        }
        let Some(active) = guard.as_mut() else {
            return Err(PresentationError::Revoked);
        };
        let components = active.store.data_mut().grant_components(components)?;
        let placement = active
            .store
            .data_mut()
            .table
            .push(WcmpPlacement::new(
                Arc::clone(writer),
                Arc::clone(&self.revoked),
            ))
            .map_err(|_| PresentationError::Revoked)?;
        let placement_rep = placement.rep();
        let mounted = first_poll(
            active
                .client
                .townframe_dayframe_presentation_events()
                .call_mount(
                    &mut active.store,
                    Resource::new_borrow(placement_rep),
                    components,
                ),
        );
        let placement = active
            .store
            .data_mut()
            .table
            .delete(Resource::<WcmpPlacement>::new_own(placement_rep))
            .expect("borrowed placement must remain in the host resource table");
        match mounted {
            Ok(Ok(surface)) => {
                let granted = active
                    .store
                    .data()
                    .table
                    .get(&surface)
                    .expect("returned surface must be in the host resource table");
                if placement.surface_identity() != Some(granted.identity()) {
                    wit_presentation::HostSurface::drop(active.store.data_mut(), surface)
                        .expect("returned surface must remain in the host resource table");
                    writer.withdraw();
                    return Err(PresentationError::InvalidBatch(
                        "guest returned a surface from another placement".into(),
                    ));
                }
                Ok(surface.rep())
            }
            Ok(Err(error)) => {
                writer.withdraw();
                Err(match error {
                    wit_presentation::PresentationError::Revoked => PresentationError::Revoked,
                    wit_presentation::PresentationError::Unsupported(message) => {
                        PresentationError::Unsupported(message)
                    }
                    wit_presentation::PresentationError::InvalidBatch(message) => {
                        PresentationError::InvalidBatch(message)
                    }
                })
            }
            Err(_trap) => {
                guard.take();
                drop(guard);
                writer.withdraw();
                self.host.close_instance(&self.instance).unwrap();
                Err(PresentationError::Revoked)
            }
        }
    }

    fn dispatch(
        &self,
        surface: u32,
        writer: &Arc<dyn SurfaceSink<ComponentRef>>,
        event: Event,
    ) -> EventResponse {
        let mut guard = self.incarnation.borrow_mut();
        if self.host.check(&self.instance).is_err() {
            return EventResponse::default();
        }
        let Some(active) = guard.as_mut() else {
            return EventResponse::default();
        };
        let outcome = first_poll(
            active
                .client
                .townframe_dayframe_presentation_events()
                .call_dispatch(
                    &mut active.store,
                    Resource::new_borrow(surface),
                    &presentation_events::Event {
                        revision: event.revision,
                        element: event.element,
                        name: event.name,
                        bubbles: event.bubbles,
                        data_json: event.data_json,
                    },
                ),
        );
        match outcome {
            Ok(response) => EventResponse {
                prevent_default: response.prevent_default,
                stop_propagation: response.stop_propagation,
            },
            Err(_trap) => {
                guard.take();
                drop(guard);
                self.trapped(writer);
                EventResponse::default()
            }
        }
    }

    fn unmount(&self, surface: u32, writer: &Arc<dyn SurfaceSink<ComponentRef>>) {
        let mut guard = self.incarnation.borrow_mut();
        let trapped = match guard.as_mut() {
            Some(active) if self.host.check(&self.instance).is_ok() => {
                let outcome = first_poll(
                    active
                        .client
                        .townframe_dayframe_presentation_events()
                        .call_unmount(&mut active.store, Resource::new_borrow(surface)),
                );
                match outcome {
                    Ok(()) => {
                        wit_presentation::HostSurface::drop(
                            active.store.data_mut(),
                            Resource::new_own(surface),
                        )
                        .expect("mounted surface must remain in the host resource table");
                        false
                    }
                    Err(_trap) => {
                        guard.take();
                        true
                    }
                }
            }
            _ => false,
        };
        drop(guard);
        writer.withdraw();
        if trapped {
            self.host.close_instance(&self.instance).unwrap();
        }
    }

    fn trapped(&self, writer: &Arc<dyn SurfaceSink<ComponentRef>>) {
        writer.withdraw();
        self.host.close_instance(&self.instance).unwrap();
    }
}

#[cfg(feature = "test-utils")]
pub mod test_utils {
    pub use crate::WcmpStoreState;
    pub use crate::convert_mutation;
    pub use crate::documents::receipt_to_wire;
    pub use crate::documents::wit;
    pub use crate::wit_presentation;

    pub fn resolve_component(
        table: &wcmp::ResourceTable,
        reference: &wcmp::Resource<crate::WcmpComponentRef>,
    ) -> wcmp::Result<clay_host::ComponentRef> {
        Ok(table.get(reference)?.0)
    }
}
