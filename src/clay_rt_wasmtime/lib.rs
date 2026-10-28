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
use clay_guest_session::Placement;
use clay_guest_session::PresentationHost;
use clay_guest_session::Surface;
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

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] clay_host::AccessError),
    #[error("Wasm initialization failed: {0}")]
    Initialization(String),
}
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tokio::sync::mpsc;
use wasmtime_wasi::WasiCtx;
use wasmtime_wasi::WasiCtxView;
use wasmtime_wasi::WasiView;

/// Drive a presentation or document call to completion on its first poll. The WIT world has no
/// async export (ADR 018 §3); WASI p3 lifts the *imports* through component-model async, which
/// forces async `call_*` bindings, but a guest call that returns `Pending` has suspended where
/// the contract has no suspension point — that is a broken invariant, not a condition to wait out.
pub(crate) fn first_poll<F: Future>(call: F) -> F::Output {
    match std::pin::pin!(call).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("guest presentation call suspended on its first poll"),
    }
}

/// Wasmtime reaches this crate only through wash-runtime's re-export, so the
/// host and the runtime can never resolve to two wasmtime copies. The
/// `wasmtime_crate` option below points the generated bindings at the same
/// path instead of a bare `wasmtime::` dependency.
pub(crate) use wash_runtime::wasmtime;

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
    wash_runtime::wasmtime::component::bindgen!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        wasmtime_crate: wash_runtime::wasmtime,
        imports: { default: trappable },
        exports: { default: async },
        with: {
            "townframe:dayframe/presentation.surface": clay_guest_session::Surface,
            "townframe:dayframe/presentation.mount-point": clay_guest_session::Placement,
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

struct WasmStoreState {
    wasi: WasiCtx,
    table: ResourceTable,
    documents: DocumentSession,
    presentation: PresentationHost,
    limits: StoreLimits,
}

impl WasiView for WasmStoreState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasmStoreState {
    fn grant_components(
        &mut self,
        components: &[ComponentRef],
    ) -> Result<Vec<Resource<WasmComponentRef>>, PresentationError> {
        self.presentation.admit_components(components.len())?;
        let table = &mut self.table;
        Ok(components
            .iter()
            .map(|&reference| {
                table
                    .push(WasmComponentRef(reference))
                    .expect("bounded component resource table")
            })
            .collect())
    }

    fn new(
        host: Host,
        instance: Instance,
        limits: DocumentLimits,
    ) -> Result<(Self, mpsc::Receiver<Work>), clay_host::AccessError> {
        let (documents, incoming) = DocumentSession::new(host.clone(), instance.clone(), limits)?;
        let presentation = PresentationHost::new(host, instance);
        let mut wasi = WasiCtx::builder();
        wasi.inherit_stderr();
        Ok((
            Self {
                wasi: wasi.build(),
                table: ResourceTable::new(),
                documents,
                presentation,
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

fn convert_mutation(mutation: wit_presentation::Mutation) -> Mutation<Resource<WasmComponentRef>> {
    match mutation {
        wit_presentation::Mutation::PushId(id) => Mutation::PushId(id),
        wit_presentation::Mutation::SetId(id) => Mutation::SetId(id),
        wit_presentation::Mutation::Child(index) => Mutation::Child(index),
        wit_presentation::Mutation::Pop => Mutation::Pop,
        wit_presentation::Mutation::CreateElement((tag, ns)) => Mutation::CreateElement((tag, ns)),
        wit_presentation::Mutation::CreateText(value) => Mutation::CreateText(value),
        wit_presentation::Mutation::MountComponent(reference) => {
            Mutation::MountComponent(reference)
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
    }
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

impl wit_presentation::Host for WasmStoreState {}

impl wit_presentation::HostComponentRef for WasmStoreState {
    fn drop(&mut self, component: Resource<WasmComponentRef>) -> wasmtime::Result<()> {
        self.table.delete(component)?;
        self.presentation.release_component();
        Ok(())
    }
}

impl wit_presentation::HostMountPoint for WasmStoreState {
    fn open(
        &mut self,
        placement: Resource<Placement>,
    ) -> wasmtime::Result<Result<Resource<Surface>, wit_presentation::PresentationError>> {
        let surface = match self.presentation.open(self.table.get_mut(&placement)?) {
            Ok(surface) => surface,
            Err(error) => return Ok(Err(convert_error(error))),
        };
        Ok(Ok(self.table.push(surface)?))
    }

    fn drop(&mut self, placement: Resource<Placement>) -> wasmtime::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}

impl wit_presentation::HostSurface for WasmStoreState {
    fn apply(
        &mut self,
        surface: Resource<Surface>,
        edits: Vec<wit_presentation::Mutation>,
    ) -> wasmtime::Result<Result<u64, wit_presentation::PresentationError>> {
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
        let surface = self.table.get(&surface)?;
        Ok(self
            .presentation
            .apply(surface, &converted)
            .map_err(convert_error))
    }

    fn retain(
        &mut self,
        surface: Resource<Surface>,
    ) -> wasmtime::Result<Result<Resource<Surface>, wit_presentation::PresentationError>> {
        let retained = match self.presentation.retain(self.table.get(&surface)?) {
            Ok(retained) => retained,
            Err(error) => return Ok(Err(convert_error(error))),
        };
        Ok(Ok(self.table.push(retained)?))
    }

    fn identity(&mut self, surface: Resource<Surface>) -> wasmtime::Result<u64> {
        Ok(self.presentation.identity(self.table.get(&surface)?)?)
    }

    fn drop(&mut self, surface: Resource<Surface>) -> wasmtime::Result<()> {
        self.table.delete(surface)?;
        self.presentation.release_surface();
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
    pub async fn from_env(name: &str) -> Result<Self, String> {
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
        Self::from_bytes(bytes).await
    }

    pub async fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, String> {
        let mut config = Config::new();
        config.consume_fuel(true);
        // WASI p3 lifts stream/future types through the component-model async builtins, and
        // `p3::add_to_linker` registers at least one `async func` import (`monotonic-clock.wait-for`),
        // so the store requires `*_async` entrypoints: async export bindings + `instantiate_async`.
        config.wasm_component_model_async(true);
        let engine = Engine::new(&config).map_err(|error| error.to_string())?;
        let component = Component::new(&engine, bytes).map_err(|err| err.to_string())?;
        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p3::add_to_linker(&mut linker).map_err(|err| err.to_string())?;
        MicroClient::add_to_linker::<_, HasSelf<WasmStoreState>>(&mut linker, |state| state)
            .map_err(|err| err.to_string())?;
        Ok(Self {
            engine,
            component,
            linker: Arc::new(linker),
        })
    }

    async fn instantiate(
        &self,
        host: &Host,
        instance: Instance,
    ) -> Result<(WasmIncarnation, mpsc::Receiver<Work>), ActivationError> {
        let (state, incoming) =
            WasmStoreState::new(host.clone(), instance, DocumentLimits::default())?;
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| &mut state.limits);
        let fuel_per_call = Arc::new(AtomicU64::new(WasmIncarnation::FUEL_PER_CALL));
        store
            .set_fuel(fuel_per_call.load(Ordering::Relaxed))
            .unwrap();
        let client = MicroClient::instantiate_async(&mut store, &self.component, &self.linker)
            .await
            .map_err(|error| {
                ActivationError::Initialization(format!("instantiating Wasm component: {error:#}"))
            })?;
        Ok((
            WasmIncarnation {
                store,
                client,
                fuel_per_call,
            },
            incoming,
        ))
    }
}

struct WasmIncarnation {
    store: Store<WasmStoreState>,
    client: MicroClient,
    fuel_per_call: Arc<AtomicU64>,
}

impl WasmIncarnation {
    const FUEL_PER_CALL: u64 = 50_000_000;

    /// Every host-initiated guest entry gets a fresh, bounded instruction budget, so a guest that
    /// loops forever inside one synchronous call traps with `OutOfFuel` instead of hanging the UI
    /// or document driver that called it.
    fn refuel(&mut self) {
        self.store
            .set_fuel(self.fuel_per_call.load(Ordering::Relaxed))
            .unwrap();
    }
}

#[derive(Clone)]
pub struct WasmRuntime {
    host: Host,
    instance: Instance,
    incarnation: Arc<Mutex<Option<WasmIncarnation>>>,
    revoked: Arc<AtomicBool>,
    fuel: Arc<AtomicU64>,
}

impl WasmRuntime {
    pub async fn activate(
        host: &Host,
        instance: Instance,
        package: &WasmPackage,
        configs: BTreeMap<String, DocumentRef>,
    ) -> Result<(Self, impl std::future::Future<Output = ()> + 'static), ActivationError> {
        let (mut active, incoming) = package.instantiate(host, instance.clone()).await?;
        documents::activate(&mut active, configs).await?;
        let pending_limit = active.store.data().documents.limits().pending_deliveries;
        let fuel = active.fuel_per_call.clone();
        let incarnation = Arc::new(Mutex::new(Some(active)));
        let driver_incarnation = incarnation.clone();
        let driver = clay_guest_session::drive(
            host.clone(),
            instance.clone(),
            incoming,
            pending_limit,
            move |work, delivery| documents::deliver(&driver_incarnation, work, delivery),
        );
        let revoked = Arc::new(AtomicBool::new(false));
        let cleanup_revoked = revoked.clone();
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
                fuel,
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
                    let outcome = first_poll(
                        active
                            .client
                            .townframe_dayframe_presentation_events()
                            .call_dispatch(
                                &mut active.store,
                                Resource::new_borrow(surface_rep),
                                &wit_event,
                            ),
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
                        first_poll(
                            active
                                .client
                                .townframe_dayframe_presentation_events()
                                .call_unmount(&mut active.store, Resource::new_borrow(surface_rep)),
                        )
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
            .push(Placement::new(writer.clone(), revoked))
            .map_err(|_| PresentationError::Revoked)?;
        let placement_rep = placement.rep();
        active.refuel();
        let mount_result = first_poll(
            active
                .client
                .townframe_dayframe_presentation_events()
                .call_mount(
                    &mut active.store,
                    Resource::new_borrow(placement_rep),
                    &components,
                ),
        );
        let placement = active
            .store
            .data_mut()
            .table
            .delete(Resource::<Placement>::new_own(placement_rep))
            .expect("borrowed placement must remain in the host resource table");
        match mount_result {
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
        self.fuel.store(0, Ordering::Relaxed);
    }

    /// The shared per-call fuel budget for this incarnation. Cloning it gives a caller a test
    /// control knob (`store(0)` makes the next guest entry trap with `OutOfFuel`) without
    /// handing out the runtime or its store.
    pub fn fuel_budget(&self) -> Arc<AtomicU64> {
        self.fuel.clone()
    }
}

#[cfg(test)]
mod tests;
