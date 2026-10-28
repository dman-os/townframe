//! ADR 018 migration step 1: the `wcmp` feasibility spike.
//!
//! This crate hosts the real `micro-client` counter component on `wcmp`, the Component Model
//! polyfill, with one host that compiles natively (over Wasmi) and for the browser (over the
//! browser's own `WebAssembly` engine). It answers the ADR's spike questions:
//!
//! - `wcmp::bindgen!` accepts our WIT with the options the Wasmtime adapter uses;
//! - `wcmp` accepts our `wit-bindgen`-built component;
//! - activation is awaited, and `mount`, `dispatch`, and `unmount` complete on their first poll
//!   ([`first_poll`] panics otherwise), so presentation calls stay synchronous;
//! - in the browser, guest surfaces render through `clay_dioxus`/`clay_surface::render` on
//!   `dioxus-web`, and DOM events serialize back to the guest through the same path.
//!
//! Out of scope, by the ADR's own migration order: documents (the spike grants none, so the
//! counter runs standalone), ingress budgets, and handle caps. Those are the engine-neutral pieces
//! migration step 3 moves out of `clay_rt_wasmtime` into the shared seam; until then this host is
//! deliberately thin rather than a second copy of them.
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
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use wcmp::Component;
use wcmp::Engine;
use wcmp::HasSelf;
use wcmp::Linker;
use wcmp::Resource;
use wcmp::ResourceTable;
use wcmp::Store;

#[cfg(target_arch = "wasm32")]
mod web;

mod bindings {
    wcmp::bindgen!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        imports: { default: trappable },
        with: {
            "townframe:dayframe/presentation.surface": super::WcmpSurface,
            "townframe:dayframe/presentation.mount-point": super::WcmpPlacement,
            "townframe:dayframe/presentation.component-ref": super::WcmpComponentRef,
        },
    });
}

use bindings::MicroClient;
use bindings::exports::townframe::dayframe::presentation_events;
use bindings::townframe::dayframe::documents as wit_documents;
use bindings::townframe::dayframe::presentation as wit_presentation;

pub struct WcmpComponentRef(ComponentRef);

pub struct WcmpPlacement {
    writer: Arc<dyn SurfaceSink<ComponentRef>>,
    opened: bool,
}

#[derive(Clone)]
pub struct WcmpSurface {
    writer: Arc<dyn SurfaceSink<ComponentRef>>,
    identity: u64,
}

struct SpikeState {
    table: ResourceTable,
    host: Host,
    instance: Instance,
    next_surface: u64,
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

fn convert_mutation(
    mutation: wit_presentation::Mutation,
    table: &ResourceTable,
) -> wcmp::Result<Mutation<ComponentRef>> {
    use wit_presentation::Mutation as Wire;
    Ok(match mutation {
        Wire::PushId(id) => Mutation::PushId(id),
        Wire::SetId(id) => Mutation::SetId(id),
        Wire::Child(index) => Mutation::Child(index),
        Wire::Pop => Mutation::Pop,
        Wire::CreateElement(element) => Mutation::CreateElement(element),
        Wire::CreateText(text) => Mutation::CreateText(text),
        Wire::MountComponent(reference) => Mutation::MountComponent(table.get(&reference)?.0),
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
    })
}

fn wire_error(error: PresentationError) -> wit_presentation::PresentationError {
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

impl wit_presentation::Host for SpikeState {}

impl wit_presentation::HostComponentRef for SpikeState {
    fn drop(&mut self, component: Resource<WcmpComponentRef>) -> wcmp::Result<()> {
        self.table.delete(component)?;
        Ok(())
    }
}

impl wit_presentation::HostMountPoint for SpikeState {
    fn open(
        &mut self,
        placement: Resource<WcmpPlacement>,
    ) -> wcmp::Result<Result<Resource<WcmpSurface>, wit_presentation::PresentationError>> {
        if self.host.check(&self.instance).is_err() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let placement = self.table.get_mut(&placement)?;
        if !placement.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        if placement.opened {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "placement already opened".into(),
            )));
        }
        placement.opened = true;
        self.next_surface += 1;
        let surface = WcmpSurface {
            writer: Arc::clone(&placement.writer),
            identity: self.next_surface,
        };
        Ok(Ok(self.table.push(surface)?))
    }

    fn drop(&mut self, placement: Resource<WcmpPlacement>) -> wcmp::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}

impl wit_presentation::HostSurface for SpikeState {
    fn apply(
        &mut self,
        surface: Resource<WcmpSurface>,
        edits: Vec<wit_presentation::Mutation>,
    ) -> wcmp::Result<Result<u64, wit_presentation::PresentationError>> {
        if self.host.check(&self.instance).is_err() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let surface = self.table.get(&surface)?;
        if !surface.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let edits = edits
            .into_iter()
            .map(|mutation| convert_mutation(mutation, &self.table))
            .collect::<wcmp::Result<Vec<_>>>()?;
        Ok(surface.writer.apply(&edits).map_err(wire_error))
    }

    fn retain(
        &mut self,
        surface: Resource<WcmpSurface>,
    ) -> wcmp::Result<Result<Resource<WcmpSurface>, wit_presentation::PresentationError>> {
        let surface = self.table.get(&surface)?.clone();
        Ok(Ok(self.table.push(surface)?))
    }

    fn identity(&mut self, surface: Resource<WcmpSurface>) -> wcmp::Result<u64> {
        Ok(self.table.get(&surface)?.identity)
    }

    fn drop(&mut self, surface: Resource<WcmpSurface>) -> wcmp::Result<()> {
        self.table.delete(surface)?;
        Ok(())
    }
}

/// The spike grants no documents, so no `document-access`, `document-watch`, or `operation`
/// handle ever enters the guest's table, and `wcmp` refuses a handle the host never issued before
/// it reaches these methods. The resource types are uninhabited for the same reason.
impl wit_documents::Host for SpikeState {}

impl wit_documents::HostDocumentAccess for SpikeState {
    fn watch(
        &mut self,
        access: Resource<wit_documents::DocumentAccess>,
        _selection: wit_documents::WatchSelection,
    ) -> wcmp::Result<Result<Resource<wit_documents::DocumentWatch>, wit_documents::DocumentError>>
    {
        unreachable!("spike granted no document access: {}", access.rep())
    }

    fn mutate(
        &mut self,
        access: Resource<wit_documents::DocumentAccess>,
        _route: wit_documents::FacetRoute,
        _mutation: wit_documents::FacetMutation,
    ) -> wcmp::Result<Result<Resource<wit_documents::Operation>, wit_documents::DocumentError>>
    {
        unreachable!("spike granted no document access: {}", access.rep())
    }

    fn drop(&mut self, access: Resource<wit_documents::DocumentAccess>) -> wcmp::Result<()> {
        unreachable!("spike granted no document access: {}", access.rep())
    }
}

impl wit_documents::HostDocumentWatch for SpikeState {
    fn current(
        &mut self,
        watch: Resource<wit_documents::DocumentWatch>,
    ) -> wcmp::Result<Result<wit_documents::DocumentSnapshot, wit_documents::DocumentError>> {
        unreachable!("spike granted no document watch: {}", watch.rep())
    }

    fn select(
        &mut self,
        watch: Resource<wit_documents::DocumentWatch>,
        _selection: wit_documents::WatchSelection,
    ) -> wcmp::Result<Result<(), wit_documents::DocumentError>> {
        unreachable!("spike granted no document watch: {}", watch.rep())
    }

    fn refresh(
        &mut self,
        watch: Resource<wit_documents::DocumentWatch>,
    ) -> wcmp::Result<Result<(), wit_documents::DocumentError>> {
        unreachable!("spike granted no document watch: {}", watch.rep())
    }

    fn drop(&mut self, watch: Resource<wit_documents::DocumentWatch>) -> wcmp::Result<()> {
        unreachable!("spike granted no document watch: {}", watch.rep())
    }
}

impl wit_documents::HostOperation for SpikeState {
    fn current(
        &mut self,
        operation: Resource<wit_documents::Operation>,
    ) -> wcmp::Result<Result<wit_documents::OperationReceipt, wit_documents::DocumentError>> {
        unreachable!("spike granted no document operation: {}", operation.rep())
    }

    fn drop(&mut self, operation: Resource<wit_documents::Operation>) -> wcmp::Result<()> {
        unreachable!("spike granted no document operation: {}", operation.rep())
    }
}

/// A compiled component and its linker. Compiling is awaited: the browser compiles a module of
/// this size only asynchronously, and the same signature serves native.
pub struct WcmpPackage {
    engine: Engine,
    component: Component,
    linker: Linker<SpikeState>,
}

impl WcmpPackage {
    pub async fn load(bytes: &[u8]) -> wcmp::Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let backend = wcmp_wasm_core_wasmi::Wasmi::new();
        #[cfg(target_arch = "wasm32")]
        let backend = wcmp_wasm_core_web::Web::new();
        let engine = Engine::with_backend(backend)?;
        let component = Component::new(&engine, bytes).await?;
        let mut linker = Linker::new(&engine);
        MicroClient::add_to_linker::<SpikeState, HasSelf<SpikeState>>(&mut linker, |state| state)?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }
}

struct Incarnation {
    store: Store<SpikeState>,
    client: MicroClient,
}

/// One activated counter incarnation. Unlike `clay_rt_wasmtime::WasmRuntime` it is `!Send`: the
/// `wcmp` store is not thread-safe in the browser, so it lives in the surface factory's closures,
/// which already admit non-thread-safe state (ADR 018 §4).
#[derive(Clone)]
pub struct WcmpRuntime {
    host: Host,
    instance: Instance,
    incarnation: Rc<RefCell<Option<Incarnation>>>,
}

impl WcmpRuntime {
    /// Instantiate the component and run `micro-lifecycle.activate` with no document bindings.
    pub async fn activate(
        host: &Host,
        instance: Instance,
        package: &WcmpPackage,
    ) -> Result<Self, String> {
        let state = SpikeState {
            table: ResourceTable::new(),
            host: host.clone(),
            instance: instance.clone(),
            next_surface: 0,
        };
        let mut store = Store::new(&package.engine, state).map_err(|error| error.to_string())?;
        let client =
            MicroClient::instantiate_async(&mut store, &package.component, &package.linker)
                .await
                .map_err(|error| format!("instantiating the component: {error}"))?;
        client
            .townframe_dayframe_micro_lifecycle()
            .call_activate(&mut store, Vec::new())
            .await
            .map_err(|error| format!("activate trapped: {error}"))??;
        Ok(Self {
            host: host.clone(),
            instance,
            incarnation: Rc::new(RefCell::new(Some(Incarnation { store, client }))),
        })
    }

    /// Mount the guest's surface into `writer`. A trap in any guest call discards the store,
    /// withdraws the surface, and closes the instance, as on Wasmtime.
    pub fn mount(
        &self,
        writer: Arc<dyn SurfaceSink<ComponentRef>>,
        components: &[ComponentRef],
    ) -> Result<SurfaceSession, PresentationError> {
        let surface = {
            let mut guard = self.incarnation.borrow_mut();
            let Some(active) = guard.as_mut() else {
                return Err(PresentationError::Revoked);
            };
            let state = active.store.data_mut();
            let components = components
                .iter()
                .map(|&reference| state.table.push(WcmpComponentRef(reference)))
                .collect::<Result<Vec<_>, _>>()
                .expect("component handles fit the store's table");
            let placement = state
                .table
                .push(WcmpPlacement {
                    writer: Arc::clone(&writer),
                    opened: false,
                })
                .expect("placement fits the store's table");
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
            active
                .store
                .data_mut()
                .table
                .delete(Resource::<WcmpPlacement>::new_own(placement_rep))
                .expect("the borrowed placement stays in the host table");
            match mounted {
                Ok(Ok(surface)) => surface.rep(),
                Ok(Err(error)) => {
                    writer.withdraw();
                    return Err(match error {
                        wit_presentation::PresentationError::Revoked => PresentationError::Revoked,
                        wit_presentation::PresentationError::Unsupported(message) => {
                            PresentationError::Unsupported(message)
                        }
                        wit_presentation::PresentationError::InvalidBatch(message) => {
                            PresentationError::InvalidBatch(message)
                        }
                    });
                }
                Err(_trap) => {
                    guard.take();
                    drop(guard);
                    self.trapped(&writer);
                    return Err(PresentationError::Revoked);
                }
            }
        };
        let dispatch = self.clone();
        let dispatch_writer = Arc::clone(&writer);
        let unmount = self.clone();
        Ok(SurfaceSession::with_drop(
            move |event: Event| dispatch.dispatch(surface, &dispatch_writer, event),
            move || unmount.unmount(surface, &writer),
        ))
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
                        .expect("the mounted surface stays in the host table");
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
