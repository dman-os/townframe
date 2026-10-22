//! Wasmtime host adapter for WIT `presentation-client` micro components.
//!
//! Each activated micro [`Instance`] owns an isolated `wasmtime::Store` so a guest trap never
//! taints sibling micros or a later re-enabled incarnation. If a guest traps during `mount` or
//! `dispatch`, the adapter immediately withdraws the surface and closes the micro's [`Instance`],
//! which revokes its [`clay_host::ComponentRef`] and any host collections bound to that instance.

mod documents;

use clay_docs::DocumentRef;
use clay_guest_session::DocumentSession;
use clay_guest_session::Limits as DocumentLimits;
use clay_guest_session::Work;
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
use clay_surface::SurfaceTree;

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] clay_host::AccessError),
    #[error("Wasm initialization failed: {0}")]
    Initialization(String),
}
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use wasmtime::Config;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::StoreLimits;
use wasmtime::StoreLimitsBuilder;
use wasmtime::component::Component;
use wasmtime::component::HasSelf;
use wasmtime::component::Linker;
use wasmtime::component::Resource;
use wasmtime::component::ResourceTable;

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

use bindings::MicroClient;
use bindings::exports::townframe::dayframe::presentation_events;
use bindings::townframe::dayframe::presentation::{self as wit_presentation};

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
    /// Load a component artifact prepared by `x/check-dayframe.ts`, named by the environment
    /// variable `DAYFRAME_<name>_COMPONENT` (uppercased, `-` → `_`; e.g. `slots` →
    /// `DAYFRAME_SLOTS_COMPONENT`). Test and fixture code only: compositions that ship a package
    /// embed or fetch it themselves.
    pub fn from_env(name: &str) -> Result<Self, String> {
        let variable = format!(
            "DAYFRAME_{}_COMPONENT",
            name.to_uppercase().replace('-', "_")
        );
        let path = std::env::var(&variable).map_err(|_| {
            format!(
                "{variable} must name a prepared {name} component; run deno run --allow-all x/check-dayframe.ts --prepare-only and export the printed path"
            )
        })?;
        let path = std::path::PathBuf::from(path);
        if !path.is_absolute() {
            return Err(format!(
                "{variable} must be an absolute path: {}",
                path.display()
            ));
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| format!("cannot read {variable} at {}: {error}", path.display()))?;
        Self::from_bytes(bytes)
    }

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
        let surface_rep = self.mount_guest_surface(
            &writer,
            components,
            &instance_for_surface,
            &incarnation_for_surface,
        )?;
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

    fn mount_guest_surface(
        &self,
        writer: &Arc<dyn SurfaceSink<ComponentRef>>,
        components: &[ComponentRef],
        instance: &Instance,
        incarnation: &Arc<Mutex<Option<WasmIncarnation>>>,
    ) -> Result<u32, PresentationError> {
        let revoked = self.revoked.clone();
        let mut guard = incarnation.lock().unwrap();
        if self.host.check(instance).is_err() {
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
                revoked,
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
                Ok(surface.rep())
            }
            Ok(Err(err)) => {
                writer.withdraw();
                Err(match err {
                    wit_presentation::PresentationError::Revoked => PresentationError::Revoked,
                    wit_presentation::PresentationError::Unsupported(msg) => {
                        PresentationError::Unsupported(msg)
                    }
                    wit_presentation::PresentationError::InvalidBatch(msg) => {
                        PresentationError::InvalidBatch(msg)
                    }
                })
            }
            Err(_trap) => {
                guard.take();
                drop(guard);
                writer.withdraw();
                self.host.close_instance(instance).unwrap();
                Err(PresentationError::Revoked)
            }
        }
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
mod tests;
