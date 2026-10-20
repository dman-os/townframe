//! Wasmtime host adapter for WIT `presentation-client` micro components.
//!
//! Each activated micro [`Instance`] owns an isolated `wasmtime::Store` so a guest trap never
//! taints sibling micros or a later re-enabled incarnation. If a guest traps during `mount` or
//! `dispatch`, the adapter immediately withdraws the surface and closes the micro's [`Instance`],
//! which revokes its [`clay_host::ComponentRef`] and any host collections bound to that instance.

mod documents;

use clay_data::DocumentRef;
use clay_guest_session::{DocumentSession, Limits as DocumentLimits, Work};
use clay_host::{ComponentRef, Host, Instance};
use clay_surface::{
    AttributeValue, Event, EventResponse, Mutation, PresentationError, SurfaceSession, SurfaceSink,
    SurfaceTree,
};

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] clay_host::AccessError),
    #[error("Wasm initialization failed: {0}")]
    Initialization(String),
}
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;
use wasmtime::{
    Config, Engine, Store, StoreLimits, StoreLimitsBuilder,
    component::{Component, HasSelf, Linker, Resource, ResourceTable},
};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        imports: { default: trappable },
        with: {
            "townframe:dayframe/presentation.surface": super::WasmSurface,
            "townframe:dayframe/presentation.mount-point": super::WasmPlacement,
            "townframe:dayframe/presentation.component-ref": super::WasmComponentRef,
            "townframe:dayframe/documents.document-access": super::documents::WasmDocumentAccess,
            "townframe:dayframe/documents.document-watch": super::documents::WasmDocumentWatch,
            "townframe:dayframe/documents.operation": super::documents::WasmOperation,
        },
    });
}

use bindings::{
    MicroClient,
    exports::townframe::dayframe::presentation_events,
    townframe::dayframe::presentation::{self as wit_presentation},
};

pub struct WasmComponentRef(ComponentRef);

pub struct WasmPlacement {
    writer: Arc<dyn SurfaceSink<ComponentRef>>,
    revoked: Arc<AtomicBool>,
    surface_identity: Option<u64>,
}

#[derive(Clone)]
pub struct WasmSurface {
    writer: Arc<dyn SurfaceSink<ComponentRef>>,
    revoked: Arc<AtomicBool>,
    identity: u64,
    ownership: Arc<()>,
}

impl Drop for WasmSurface {
    fn drop(&mut self) {
        if Arc::strong_count(&self.ownership) == 1 {
            self.writer.withdraw();
        }
    }
}

struct WasmStoreState {
    table: ResourceTable,
    host: Host,
    instance: Instance,
    documents: DocumentSession,
    next_surface: u64,
    surface_handles: usize,
    component_handles: usize,
    limits: StoreLimits,
}

impl WasmStoreState {
    fn grant_components(
        &mut self,
        components: &[ComponentRef],
    ) -> Result<Vec<Resource<WasmComponentRef>>, PresentationError> {
        if components.len() > Self::MAX_COMPONENT_HANDLES - self.component_handles {
            return Err(PresentationError::Unsupported(
                "component handle limit".into(),
            ));
        }
        Ok(components
            .iter()
            .map(|&reference| {
                let resource = self
                    .table
                    .push(WasmComponentRef(reference))
                    .expect("bounded component resource table");
                self.component_handles += 1;
                resource
            })
            .collect())
    }

    const MAX_SURFACE_HANDLES: usize = 1024;
    const MAX_COMPONENT_HANDLES: usize = 1024;

    fn new(
        host: Host,
        instance: Instance,
        limits: DocumentLimits,
    ) -> Result<(Self, mpsc::Receiver<Work>), clay_host::AccessError> {
        let (documents, incoming) = DocumentSession::new(host.clone(), instance.clone(), limits)?;
        Ok((
            Self {
                table: ResourceTable::new(),
                host,
                instance,
                documents,
                next_surface: 0,
                surface_handles: 0,
                component_handles: 0,
                limits: StoreLimitsBuilder::new()
                    .memory_size(64 * 1024 * 1024)
                    .table_elements(8192)
                    .build(),
            },
            incoming,
        ))
    }
}

fn convert_attribute_value(value: wit_presentation::AttributeValue) -> AttributeValue {
    match value {
        wit_presentation::AttributeValue::Text(text) => AttributeValue::Text(text),
        wit_presentation::AttributeValue::Float(float) => AttributeValue::Float(float),
        wit_presentation::AttributeValue::Int(int) => AttributeValue::Int(int),
        wit_presentation::AttributeValue::Boolean(bool) => AttributeValue::Boolean(bool),
        wit_presentation::AttributeValue::None => AttributeValue::None,
    }
}

fn convert_mutation(
    mutation: wit_presentation::Mutation,
    table: &ResourceTable,
) -> wasmtime::Result<Mutation<ComponentRef>> {
    Ok(match mutation {
        wit_presentation::Mutation::PushId(id) => Mutation::PushId(id),
        wit_presentation::Mutation::SetId(id) => Mutation::SetId(id),
        wit_presentation::Mutation::Child(index) => Mutation::Child(index),
        wit_presentation::Mutation::Pop => Mutation::Pop,
        wit_presentation::Mutation::CreateElement((tag, ns)) => Mutation::CreateElement((tag, ns)),
        wit_presentation::Mutation::CreateText(value) => Mutation::CreateText(value),
        wit_presentation::Mutation::MountComponent(reference) => {
            Mutation::MountComponent(table.get(&reference)?.0)
        }
        wit_presentation::Mutation::CloneNode => Mutation::CloneNode,
        wit_presentation::Mutation::AppendChildren(m) => Mutation::AppendChildren(m),
        wit_presentation::Mutation::ReplaceWith(m) => Mutation::ReplaceWith(m),
        wit_presentation::Mutation::InsertAfter(m) => Mutation::InsertAfter(m),
        wit_presentation::Mutation::InsertBefore(m) => Mutation::InsertBefore(m),
        wit_presentation::Mutation::SetAttribute((name, ns, value)) => {
            Mutation::SetAttribute((name, ns, convert_attribute_value(value)))
        }
        wit_presentation::Mutation::SetText(value) => Mutation::SetText(value),
        wit_presentation::Mutation::AddEventListener(name) => Mutation::AddEventListener(name),
        wit_presentation::Mutation::RemoveEventListener(name) => {
            Mutation::RemoveEventListener(name)
        }
        wit_presentation::Mutation::Remove => Mutation::Remove,
    })
}

fn convert_error(error: PresentationError) -> wit_presentation::PresentationError {
    match error {
        PresentationError::Revoked => wit_presentation::PresentationError::Revoked,
        PresentationError::Unsupported(msg) => {
            wit_presentation::PresentationError::Unsupported(msg)
        }
        PresentationError::InvalidBatch(msg) => {
            wit_presentation::PresentationError::InvalidBatch(msg)
        }
    }
}

fn ingress_budget(edits: &[wit_presentation::Mutation]) -> Result<(), &'static str> {
    if edits.len() > SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS {
        return Err("batch mutation limit");
    }
    let mut bytes = 0_usize;
    for mutation in edits {
        let strings: [Option<&str>; 3] = match mutation {
            wit_presentation::Mutation::CreateElement((tag, ns)) => {
                [Some(tag), ns.as_deref(), None]
            }
            wit_presentation::Mutation::CreateText(text)
            | wit_presentation::Mutation::SetText(text)
            | wit_presentation::Mutation::AddEventListener(text)
            | wit_presentation::Mutation::RemoveEventListener(text) => [Some(text), None, None],
            wit_presentation::Mutation::SetAttribute((name, ns, value)) => [
                Some(name),
                ns.as_deref(),
                match value {
                    wit_presentation::AttributeValue::Text(text) => Some(text),
                    _ => None,
                },
            ],
            wit_presentation::Mutation::PushId(_)
            | wit_presentation::Mutation::SetId(_)
            | wit_presentation::Mutation::Child(_)
            | wit_presentation::Mutation::Pop
            | wit_presentation::Mutation::MountComponent(_)
            | wit_presentation::Mutation::CloneNode
            | wit_presentation::Mutation::AppendChildren(_)
            | wit_presentation::Mutation::ReplaceWith(_)
            | wit_presentation::Mutation::InsertAfter(_)
            | wit_presentation::Mutation::InsertBefore(_)
            | wit_presentation::Mutation::Remove => [None, None, None],
        };
        bytes += std::mem::size_of::<Mutation<ComponentRef>>();
        for value in strings.into_iter().flatten() {
            if value.len() > SurfaceTree::<ComponentRef>::MAX_STRING_BYTES {
                return Err("string byte limit");
            }
            bytes += value.len();
        }
        if bytes > SurfaceTree::<ComponentRef>::MAX_BATCH_BYTES {
            return Err("batch byte limit");
        }
    }
    Ok(())
}

impl wit_presentation::Host for WasmStoreState {}

impl wit_presentation::HostComponentRef for WasmStoreState {
    fn drop(&mut self, component: Resource<WasmComponentRef>) -> wasmtime::Result<()> {
        self.table.delete(component)?;
        self.component_handles -= 1;
        Ok(())
    }
}

impl wit_presentation::HostMountPoint for WasmStoreState {
    fn open(
        &mut self,
        placement: Resource<WasmPlacement>,
    ) -> wasmtime::Result<Result<Resource<WasmSurface>, wit_presentation::PresentationError>> {
        if self.host.check(&self.instance).is_err() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let granted = self.table.get_mut(&placement)?;
        if granted.revoked.load(Ordering::Acquire) || !granted.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        if granted.surface_identity.is_some() {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "placement already opened".into(),
            )));
        }
        if self.surface_handles >= Self::MAX_SURFACE_HANDLES {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "surface handle limit".into(),
            )));
        }
        self.next_surface = self
            .next_surface
            .checked_add(1)
            .expect("surface identity overflow");
        granted.surface_identity = Some(self.next_surface);
        let surface = WasmSurface {
            writer: granted.writer.clone(),
            revoked: granted.revoked.clone(),
            identity: self.next_surface,
            ownership: Arc::new(()),
        };
        let resource = self.table.push(surface)?;
        self.surface_handles += 1;
        Ok(Ok(resource))
    }

    fn drop(&mut self, placement: Resource<WasmPlacement>) -> wasmtime::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}

impl wit_presentation::HostSurface for WasmStoreState {
    fn apply(
        &mut self,
        surface: Resource<WasmSurface>,
        edits: Vec<wit_presentation::Mutation>,
    ) -> wasmtime::Result<Result<u64, wit_presentation::PresentationError>> {
        if self.host.check(&self.instance).is_err() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let surface = self.table.get(&surface)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        if let Err(reason) = ingress_budget(&edits) {
            surface.writer.withdraw();
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                reason.into(),
            )));
        }
        let converted: Vec<Mutation<ComponentRef>> = edits
            .into_iter()
            .map(|mutation| convert_mutation(mutation, &self.table))
            .collect::<wasmtime::Result<_>>()?;
        for mutation in &converted {
            if let Mutation::MountComponent(reference) = mutation
                && let Err(error) = self.host.check_component(*reference)
            {
                return Ok(Err(match error {
                    clay_host::AccessError::Revoked => wit_presentation::PresentationError::Revoked,
                    clay_host::AccessError::Denied => {
                        wit_presentation::PresentationError::InvalidBatch(
                            "component grant belongs to another host".into(),
                        )
                    }
                }));
            }
        }
        Ok(surface.writer.apply(&converted).map_err(convert_error))
    }

    fn retain(
        &mut self,
        surface: Resource<WasmSurface>,
    ) -> wasmtime::Result<Result<Resource<WasmSurface>, wit_presentation::PresentationError>> {
        if self.host.check(&self.instance).is_err() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        let surface = self.table.get(&surface)?;
        if surface.revoked.load(Ordering::Acquire) || !surface.writer.is_live() {
            return Ok(Err(wit_presentation::PresentationError::Revoked));
        }
        if self.surface_handles >= Self::MAX_SURFACE_HANDLES {
            return Ok(Err(wit_presentation::PresentationError::Unsupported(
                "surface handle limit".into(),
            )));
        }
        let retained = self.table.push(surface.clone())?;
        self.surface_handles += 1;
        Ok(Ok(retained))
    }

    fn identity(&mut self, surface: Resource<WasmSurface>) -> wasmtime::Result<u64> {
        self.host.check(&self.instance)?;
        Ok(self.table.get(&surface)?.identity)
    }

    fn drop(&mut self, surface: Resource<WasmSurface>) -> wasmtime::Result<()> {
        self.table.delete(surface)?;
        self.surface_handles -= 1;
        Ok(())
    }
}

/// Compiled Wasm component and pre-configured linker reusable across incarnations.
#[derive(Clone)]
pub struct WasmPackage {
    engine: Engine,
    component: Component,
    linker: Arc<Linker<WasmStoreState>>,
}

impl WasmPackage {
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, String> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(|error| error.to_string())?;
        let component = Component::new(&engine, bytes).map_err(|err| err.to_string())?;
        let mut linker = Linker::new(&engine);
        MicroClient::add_to_linker::<_, HasSelf<WasmStoreState>>(&mut linker, |state| state)
            .map_err(|err| err.to_string())?;
        Ok(Self {
            engine,
            component,
            linker: Arc::new(linker),
        })
    }

    fn instantiate(
        &self,
        host: &Host,
        instance: Instance,
    ) -> Result<(WasmIncarnation, mpsc::Receiver<Work>), ActivationError> {
        let (state, incoming) =
            WasmStoreState::new(host.clone(), instance, DocumentLimits::default())?;
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| &mut state.limits);
        store.set_fuel(WasmIncarnation::FUEL_PER_CALL).unwrap();
        let client = MicroClient::instantiate(&mut store, &self.component, &self.linker).map_err(
            |error| {
                ActivationError::Initialization(format!("instantiating Wasm component: {error:#}"))
            },
        )?;
        Ok((
            WasmIncarnation {
                store,
                client,
                fuel_per_call: WasmIncarnation::FUEL_PER_CALL,
            },
            incoming,
        ))
    }
}

struct WasmIncarnation {
    store: Store<WasmStoreState>,
    client: MicroClient,
    fuel_per_call: u64,
}

impl WasmIncarnation {
    const FUEL_PER_CALL: u64 = 50_000_000;

    /// Every host-initiated guest entry gets a fresh, bounded instruction budget, so a guest that
    /// loops forever inside one synchronous call traps with `OutOfFuel` instead of hanging the UI
    /// or document driver that called it.
    fn refuel(&mut self) {
        self.store.set_fuel(self.fuel_per_call).unwrap();
    }
}

#[derive(Clone)]
pub struct WasmRuntime {
    host: Host,
    instance: Instance,
    incarnation: Arc<Mutex<Option<WasmIncarnation>>>,
    revoked: Arc<AtomicBool>,
}

impl WasmRuntime {
    pub fn activate(
        host: &Host,
        instance: Instance,
        package: &WasmPackage,
        configs: BTreeMap<String, DocumentRef>,
    ) -> Result<(Self, impl std::future::Future<Output = ()> + 'static), ActivationError> {
        let (mut active, incoming) = package.instantiate(host, instance.clone())?;
        documents::activate(&mut active, configs)?;
        let pending_limit = active.store.data().documents.limits().pending_deliveries;
        let incarnation = Arc::new(Mutex::new(Some(active)));
        let driver = documents::drive(
            host.clone(),
            instance.clone(),
            incarnation.clone(),
            incoming,
            pending_limit,
        );
        let cleanup_incarnation = incarnation.clone();
        let revoked = Arc::new(AtomicBool::new(false));
        let cleanup_revoked = revoked.clone();
        host.own(
            &instance,
            Box::new(move || {
                cleanup_revoked.store(true, Ordering::Release);
                cleanup_incarnation.lock().unwrap().take();
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
        let instance_for_surface = self.instance.clone();
        let incarnation_for_surface = self.incarnation.clone();
        let revoked_for_surface = self.revoked.clone();
        let surface_rep = {
            let mut guard = incarnation_for_surface.lock().unwrap();
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
                .push(WasmPlacement {
                    writer: writer.clone(),
                    revoked: revoked_for_surface.clone(),
                    surface_identity: None,
                })
                .map_err(|_| PresentationError::Revoked)?;
            let placement_rep = placement.rep();
            active.refuel();
            let mount_result = active
                .client
                .townframe_dayframe_presentation_events()
                .call_mount(
                    &mut active.store,
                    Resource::new_borrow(placement_rep),
                    &components,
                );
            let placement = active
                .store
                .data_mut()
                .table
                .delete(Resource::<WasmPlacement>::new_own(placement_rep))
                .expect("borrowed placement must remain in the host resource table");
            match mount_result {
                Ok(Ok(surface)) => {
                    let granted = active
                        .store
                        .data()
                        .table
                        .get(&surface)
                        .expect("returned surface must be in the host resource table");
                    if placement.surface_identity != Some(granted.identity) {
                        wit_presentation::HostSurface::drop(active.store.data_mut(), surface)
                            .expect("returned surface must remain in the host resource table");
                        writer.withdraw();
                        return Err(PresentationError::InvalidBatch(
                            "guest returned a surface from another placement".into(),
                        ));
                    }
                    surface.rep()
                }
                Ok(Err(err)) => {
                    writer.withdraw();
                    return Err(match err {
                        wit_presentation::PresentationError::Revoked => PresentationError::Revoked,
                        wit_presentation::PresentationError::Unsupported(msg) => {
                            PresentationError::Unsupported(msg)
                        }
                        wit_presentation::PresentationError::InvalidBatch(msg) => {
                            PresentationError::InvalidBatch(msg)
                        }
                    });
                }
                Err(_trap) => {
                    guard.take();
                    drop(guard);
                    writer.withdraw();
                    self.host.close_instance(&instance_for_surface).unwrap();
                    return Err(PresentationError::Revoked);
                }
            }
        };

        let dispatch_host = self.host.clone();
        let dispatch_incarnation = incarnation_for_surface.clone();
        let dispatch_instance = instance_for_surface.clone();
        let dispatch_writer = writer.clone();
        let drop_host = self.host.clone();
        let drop_incarnation = incarnation_for_surface.clone();
        let drop_instance = instance_for_surface.clone();
        let drop_writer = writer;

        Ok(SurfaceSession::with_drop(
            move |event: Event| {
                let wit_event = presentation_events::Event {
                    revision: event.revision,
                    element: event.element,
                    name: event.name,
                    bubbles: event.bubbles,
                    data_json: event.data_json,
                };
                let result = {
                    let mut guard = dispatch_incarnation.lock().unwrap();
                    if dispatch_host.check(&dispatch_instance).is_err() {
                        return EventResponse::default();
                    }
                    let Some(active) = guard.as_mut() else {
                        return EventResponse::default();
                    };
                    active.refuel();
                    let outcome = active
                        .client
                        .townframe_dayframe_presentation_events()
                        .call_dispatch(
                            &mut active.store,
                            Resource::new_borrow(surface_rep),
                            &wit_event,
                        );
                    if outcome.is_err() {
                        guard.take();
                    }
                    outcome
                };
                match result {
                    Ok(response) => EventResponse {
                        prevent_default: response.prevent_default,
                        stop_propagation: response.stop_propagation,
                    },
                    Err(_trap) => {
                        dispatch_writer.withdraw();
                        dispatch_host.close_instance(&dispatch_instance).unwrap();
                        EventResponse::default()
                    }
                }
            },
            move || {
                let mut guard = drop_incarnation.lock().unwrap();
                let trapped = if let Some(active) = guard.as_mut() {
                    let outcome = if drop_host.check(&drop_instance).is_ok() {
                        active.refuel();
                        active
                            .client
                            .townframe_dayframe_presentation_events()
                            .call_unmount(&mut active.store, Resource::new_borrow(surface_rep))
                    } else {
                        Ok(())
                    };
                    if outcome.is_err() {
                        guard.take();
                        true
                    } else {
                        wit_presentation::HostSurface::drop(
                            active.store.data_mut(),
                            Resource::new_own(surface_rep),
                        )
                        .expect("mounted surface must remain in the host resource table");
                        false
                    }
                } else {
                    false
                };
                drop(guard);
                drop_writer.withdraw();
                if trapped {
                    drop_host.close_instance(&drop_instance).unwrap();
                }
            },
        ))
    }

    /// Withdraws this incarnation's execution budget, so the next guest entry (event dispatch,
    /// mount, or document callback) traps with `OutOfFuel`. This drives the same trap recovery a
    /// runaway guest takes: the store is discarded, the instance closes, and its contributions are
    /// withdrawn while sibling micros keep running.
    pub fn exhaust_fuel(&self) {
        if let Some(active) = self.incarnation.lock().unwrap().as_mut() {
            active.fuel_per_call = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::component::Resource;

    fn counter_package() -> WasmPackage {
        let path = std::env::var("DAYFRAME_COUNTER_COMPONENT")
            .expect("prepare the counter fixture with x/check-dayframe.ts --prepare-only and set DAYFRAME_COUNTER_COMPONENT");
        WasmPackage::from_bytes(
            std::fs::read(path).expect("prepared counter component is readable"),
        )
        .expect("counter component is valid")
    }

    #[test]
    fn component_resource_borrows_resolve_only_granted_handles_and_release_capacity() {
        use wit_presentation::{HostComponentRef, Mutation as Wire};
        let host = Host::new();
        let owner = host.start("layout");
        let component = host.publish_component(&owner).unwrap();
        let (mut state, _incoming) =
            WasmStoreState::new(host.clone(), owner, DocumentLimits::default()).unwrap();
        let mut resources = state
            .grant_components(&vec![component; WasmStoreState::MAX_COMPONENT_HANDLES])
            .unwrap();
        let borrowed = Resource::new_borrow(resources[0].rep());
        assert_eq!(
            convert_mutation(Wire::MountComponent(borrowed), &state.table).unwrap(),
            Mutation::MountComponent(component)
        );
        assert!(matches!(
            state.grant_components(&[component]),
            Err(PresentationError::Unsupported(_))
        ));
        let removed = resources.pop().unwrap();
        let stale_rep = removed.rep();
        HostComponentRef::drop(&mut state, removed).unwrap();
        assert!(
            convert_mutation(
                Wire::MountComponent(Resource::new_borrow(stale_rep)),
                &state.table
            )
            .is_err()
        );
        let replacement = state.grant_components(&[component]).unwrap().pop().unwrap();
        let wrong_type = state.table.push(17_u32).unwrap();
        assert!(
            convert_mutation(
                Wire::MountComponent(Resource::new_borrow(wrong_type.rep())),
                &state.table
            )
            .is_err()
        );
        state.table.delete(wrong_type).unwrap();
        HostComponentRef::drop(&mut state, replacement).unwrap();
        for resource in resources {
            HostComponentRef::drop(&mut state, resource).unwrap();
        }
        assert_eq!(state.component_handles, 0);
        host.close();
    }

    #[test]
    fn ingress_budget_rejects_oversized_batches_before_conversion() {
        use wit_presentation::Mutation as Wire;
        assert_eq!(
            ingress_budget(&[Wire::Pop, Wire::CreateText("ok".into())]),
            Ok(())
        );
        assert_eq!(
            ingress_budget(
                &std::iter::repeat_with(|| Wire::Pop)
                    .take(SurfaceTree::<ComponentRef>::MAX_BATCH_MUTATIONS + 1)
                    .collect::<Vec<_>>()
            ),
            Err("batch mutation limit")
        );
        assert_eq!(
            ingress_budget(&[Wire::SetAttribute((
                "title".into(),
                None,
                wit_presentation::AttributeValue::Text(
                    "x".repeat(SurfaceTree::<ComponentRef>::MAX_STRING_BYTES + 1)
                ),
            ))]),
            Err("string byte limit")
        );
        let text = "x".repeat(SurfaceTree::<ComponentRef>::MAX_STRING_BYTES);
        let batch = std::iter::repeat_with(|| Wire::CreateText(text.clone()))
            .take(
                SurfaceTree::<ComponentRef>::MAX_BATCH_BYTES
                    / SurfaceTree::<ComponentRef>::MAX_STRING_BYTES
                    + 1,
            )
            .collect::<Vec<_>>();
        assert_eq!(ingress_budget(&batch), Err("batch byte limit"));
    }

    #[test]
    fn execution_and_linear_memory_are_bounded_per_incarnation() {
        let host = Host::new();
        let package = counter_package();
        let (mut active, _incoming) = package.instantiate(&host, host.start("bounded")).unwrap();
        let module = wasmtime::Module::new(
            &package.engine,
            r#"(module
            (memory (export "memory") 1)
            (func (export "loop") (loop br 0))
        )"#,
        )
        .unwrap();
        let instance = wasmtime::Instance::new(&mut active.store, &module, &[]).unwrap();
        let memory = instance.get_memory(&mut active.store, "memory").unwrap();
        assert!(memory.grow(&mut active.store, 1024).is_err());
        let looping = instance
            .get_typed_func::<(), ()>(&mut active.store, "loop")
            .unwrap();
        active.store.set_fuel(16).unwrap();
        let trap = looping.call(&mut active.store, ()).unwrap_err();
        assert_eq!(
            trap.downcast_ref::<wasmtime::Trap>(),
            Some(&wasmtime::Trap::OutOfFuel)
        );
        active.refuel();
        assert_eq!(active.store.get_fuel().unwrap(), 50_000_000);
        host.close();
    }

    #[tokio::test]
    async fn document_driver_delivers_neutral_updates_and_closes_on_guest_trap() {
        use clay_data::{DocumentGrant, DocumentService, FacetRoute};
        use std::{
            future::Future,
            task::{Context, Waker},
        };

        struct RecordingSink(Mutex<Option<SurfaceTree<ComponentRef>>>);

        impl SurfaceSink<ComponentRef> for RecordingSink {
            fn is_live(&self) -> bool {
                self.0.lock().unwrap().is_some()
            }

            fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
                self.0
                    .lock()
                    .unwrap()
                    .as_mut()
                    .ok_or(PresentationError::Revoked)?
                    .apply(edits)
            }

            fn withdraw(&self) {
                self.0.lock().unwrap().take();
            }
        }

        let host = Host::new();
        let owner = host.start("counter");
        let ended = host.observe_instance(&owner).unwrap();
        let service = DocumentService::new(32);
        let node = service.mock_documents();
        let route = FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: "org.dayframe.counter.config".into(),
            key_id: "count".into(),
        };
        node.seed(route.clone(), serde_json::json!(3));
        let access = service
            .grant_documents(
                &host,
                &owner,
                vec![DocumentGrant {
                    document: route.document.clone(),
                    branch: route.branch.clone(),
                    facets: vec![daybook_types::manifest::RoutineFacetAccess {
                        tag: route.tag.clone().into(),
                        key_id: Some(route.key_id.clone()),
                        read: true,
                        write: true,
                        ..Default::default()
                    }],
                }],
            )
            .unwrap();
        let document = access
            .document(route.document.clone(), route.branch.clone())
            .unwrap();
        let (runtime, driver) = WasmRuntime::activate(
            &host,
            owner.clone(),
            &counter_package(),
            [("counter".into(), document)].into(),
        )
        .unwrap();
        let writer = Arc::new(RecordingSink(Mutex::new(Some(SurfaceTree::new()))));
        let surface = runtime.mount(writer.clone(), &[]).unwrap();
        assert_eq!(
            writer.0.lock().unwrap().as_ref().unwrap().text_content(),
            "3Pending counter writes: 0Refresh counter"
        );
        let mut driver = std::pin::pin!(driver);
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        node.external_change(route.clone(), Some(serde_json::json!(7)));
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(
            writer.0.lock().unwrap().as_ref().unwrap().text_content(),
            "7Pending counter writes: 0Refresh counter"
        );
        runtime.exhaust_fuel();
        node.external_change(route, Some(serde_json::json!(8)));
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        assert_eq!(*ended.borrow(), Some(clay_host::CloseReason::Requested));
        assert_eq!(host.check(&owner), Err(clay_host::AccessError::Revoked));
        assert!(!writer.is_live());
        drop(surface);
        host.close();
    }

    #[derive(Default)]
    struct TestSink(AtomicBool);

    impl SurfaceSink<ComponentRef> for TestSink {
        fn is_live(&self) -> bool {
            !self.0.load(Ordering::Acquire)
        }

        fn apply(&self, _edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
            unreachable!("handle-budget test does not render")
        }

        fn withdraw(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn retained_surface_handles_are_bounded_and_release_capacity() {
        use wit_presentation::{HostMountPoint, HostSurface};

        let host = Host::new();
        let owner = host.start("surface-budget");
        let writer: Arc<dyn SurfaceSink<ComponentRef>> = Arc::new(TestSink::default());
        let (mut state, _incoming) =
            WasmStoreState::new(host.clone(), owner.clone(), DocumentLimits::default()).unwrap();
        let placement = state
            .table
            .push(WasmPlacement {
                writer: writer.clone(),
                revoked: Arc::new(AtomicBool::new(false)),
                surface_identity: None,
            })
            .unwrap();
        let surface = HostMountPoint::open(&mut state, Resource::new_borrow(placement.rep()))
            .unwrap()
            .unwrap();
        let identity =
            HostSurface::identity(&mut state, Resource::new_borrow(surface.rep())).unwrap();
        let mut retained = Vec::new();
        for _ in 1..WasmStoreState::MAX_SURFACE_HANDLES {
            retained.push(
                HostSurface::retain(&mut state, Resource::new_borrow(surface.rep()))
                    .unwrap()
                    .unwrap(),
            );
        }
        assert!(matches!(
            HostSurface::retain(&mut state, Resource::new_borrow(surface.rep())).unwrap(),
            Err(wit_presentation::PresentationError::Unsupported(_))
        ));
        assert!(writer.is_live());
        HostSurface::drop(&mut state, retained.pop().unwrap()).unwrap();
        let replacement = HostSurface::retain(&mut state, Resource::new_borrow(surface.rep()))
            .unwrap()
            .unwrap();
        assert_eq!(
            HostSurface::identity(&mut state, Resource::new_borrow(replacement.rep())).unwrap(),
            identity
        );
        let supplier = host.start("supplier");
        let reference = host.publish_component(&supplier).unwrap();
        let component = state.grant_components(&[reference]).unwrap().pop().unwrap();
        host.close_instance(&supplier).unwrap();
        assert!(matches!(
            HostSurface::apply(
                &mut state,
                Resource::new_borrow(replacement.rep()),
                vec![wit_presentation::Mutation::MountComponent(
                    Resource::new_borrow(component.rep())
                )],
            )
            .unwrap(),
            Err(wit_presentation::PresentationError::Revoked)
        ));
        wit_presentation::HostComponentRef::drop(&mut state, component).unwrap();
        host.close_instance(&owner).unwrap();
        assert!(matches!(
            HostMountPoint::open(&mut state, Resource::new_borrow(placement.rep())).unwrap(),
            Err(wit_presentation::PresentationError::Revoked)
        ));
        assert!(matches!(
            HostSurface::retain(&mut state, Resource::new_borrow(replacement.rep())).unwrap(),
            Err(wit_presentation::PresentationError::Revoked)
        ));
        assert!(matches!(
            HostSurface::apply(
                &mut state,
                Resource::new_borrow(replacement.rep()),
                Vec::new()
            )
            .unwrap(),
            Err(wit_presentation::PresentationError::Revoked)
        ));
        assert_eq!(
            HostSurface::identity(&mut state, Resource::new_borrow(replacement.rep()))
                .unwrap_err()
                .downcast::<clay_host::AccessError>()
                .unwrap(),
            clay_host::AccessError::Revoked
        );
        HostSurface::drop(&mut state, surface).unwrap();
        for resource in retained {
            HostSurface::drop(&mut state, resource).unwrap();
        }
        assert!(writer.is_live());
        HostSurface::drop(&mut state, replacement).unwrap();
        assert!(!writer.is_live());
        assert_eq!(state.surface_handles, 0);
        HostMountPoint::drop(&mut state, placement).unwrap();
        host.close();
    }
}
