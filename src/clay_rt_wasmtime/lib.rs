//! Wasmtime host adapter for WIT `presentation-client` micro components.
//!
//! Each activated micro [`Instance`] owns an isolated `wasmtime::Store` so a guest trap never
//! taints sibling micros or a later re-enabled incarnation. If a guest traps during `mount` or
//! `dispatch`, the adapter immediately withdraws the surface and closes the micro's [`Instance`],
//! which revokes its [`clay_host::ComponentRef`] and any host collections bound to that instance.

mod collections;
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
use std::task::Wake;
use std::task::Waker;
use tokio::sync::mpsc;
use wasmtime_wasi::WasiCtx;
use wasmtime_wasi::WasiCtxView;
use wasmtime_wasi::WasiView;

/// Marks a presentation call that suspended on its first poll: a guest contract violation
/// (the sync exports promise a decision before the listener closure returns), handled at
/// every call site exactly like a trap — withdraw the surface, close the instance.
#[derive(Debug, thiserror::Error)]
#[error("guest presentation call suspended on its first poll")]
pub(crate) struct Suspended;

/// Test probe: how many presentation calls hit [`Suspended`] (i.e. returned `Pending`)
/// rather than completing or failing on the first poll. The W2 spike asserts a guest that
/// blocks inside `dispatch` fails with the engine's `CannotBlock` (`Ready(Err)`) and never
/// increments this counter.
#[cfg(test)]
pub(crate) fn suspended_count() -> u64 {
    SUSPENDED.load(Ordering::Relaxed)
}

#[cfg(test)]
static SUSPENDED: AtomicU64 = AtomicU64::new(0);

/// Drive a presentation or document call to completion on its first poll. The WIT world has no
/// async export (ADR 018 §3); WASI p3 lifts the *imports* through component-model async, which
/// forces async `call_*` bindings. A conforming guest that blocks inside a sync-typed call fails
/// with the spec's `CannotBlock` cause on the first poll (`Ready(Err)`); a guest that actually
/// suspends returns `Pending` and is treated as [`Suspended`], i.e. as a trap. Either way the
/// caller withdraws the surface and closes the instance — never a host panic.
pub(crate) fn first_poll<F: Future>(call: F, waker: &Waker) -> Result<F::Output, Suspended> {
    match std::pin::pin!(call).poll(&mut Context::from_waker(waker)) {
        Poll::Ready(output) => Ok(output),
        Poll::Pending => {
            #[cfg(test)]
            SUSPENDED.fetch_add(1, Ordering::Relaxed);
            Err(Suspended)
        }
    }
}

struct IncarnationWake {
    notified: AtomicBool,
    driver: futures::task::AtomicWaker,
}

struct IncarnationCallWake {
    call: Waker,
    incarnation: Arc<IncarnationWake>,
}

impl Wake for IncarnationCallWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.call.wake_by_ref();
        self.incarnation.wake_by_ref();
    }
}

impl IncarnationWake {
    fn new() -> Self {
        Self {
            notified: AtomicBool::new(true),
            driver: futures::task::AtomicWaker::new(),
        }
    }

    async fn wait(&self) {
        std::future::poll_fn(|context| {
            self.driver.register(context.waker());
            if self.notified.swap(false, Ordering::AcqRel) {
                self.driver.register(context.waker());
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    async fn drive<F: Future>(self: &Arc<Self>, call: F) -> F::Output {
        let mut call = std::pin::pin!(call);
        std::future::poll_fn(|context| {
            let waker = Waker::from(Arc::new(IncarnationCallWake {
                call: context.waker().clone(),
                incarnation: Arc::clone(self),
            }));
            let result = call.as_mut().poll(&mut Context::from_waker(&waker));
            if result.is_ready() {
                self.wake_by_ref();
            }
            result
        })
        .await
    }
}

impl Wake for IncarnationWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.driver.wake();
    }
}

/// host and the runtime can never resolve to two wasmtime copies. The
/// `wasmtime_crate` option below points the generated bindings at the same
/// path instead of a bare `wasmtime::` dependency.
pub(crate) use wash_runtime::wasmtime;

use std::path::PathBuf;
use std::sync::OnceLock;
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
            "townframe:dayframe/collections.collection-reader": clay_guest_session::collections::CollectionReader,
            "townframe:dayframe/collections.projection-lease": clay_guest_session::collections::ProjectionReader,
            "townframe:dayframe/collection-registration.registration-writer": clay_guest_session::collections::RegistrationWriter,
            "townframe:dayframe/capability-bank.capability-ref": clay_guest_session::capabilities::CapabilityRef,
            "townframe:dayframe/capability-bank.published-capability": clay_guest_session::capabilities::PublishedCapability,
            "townframe:dayframe/capability-bank.presentation-component": clay_guest_session::capabilities::PresentationComponent,
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
    collections: clay_guest_session::collections::CollectionSession,
    capabilities: clay_guest_session::capabilities::CapabilitySession,
    collection_wake: Option<Arc<IncarnationWake>>,
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
        let presentation = PresentationHost::new(host.clone(), instance.clone());
        let collections = clay_guest_session::collections::CollectionSession::new(
            host.clone(),
            instance.clone(),
            clay_guest_session::collections::CollectionLimits::default(),
        );
        let capabilities = clay_guest_session::capabilities::CapabilitySession::new(
            host,
            instance,
            clay_guest_session::capabilities::CapabilityLimits::default(),
        );
        let mut wasi = WasiCtx::builder();
        wasi.inherit_stderr();
        Ok((
            Self {
                wasi: wasi.build(),
                table: ResourceTable::new(),
                documents,
                collections,
                capabilities,
                collection_wake: None,
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

/// The process-wide engine. Wasmtime's model is one engine per process: it owns the code and
/// type registries, the allocator, and every policy that has to be uniform (memory reservation,
/// epoch interruption, compilation cache). One engine per package (`F10`) leaves nowhere to put
/// those, so packages share this.
pub fn engine() -> Result<Engine, String> {
    static ENGINE: OnceLock<Result<Engine, String>> = OnceLock::new();
    ENGINE
        .get_or_init(|| {
            let mut config = Config::new();
            config.consume_fuel(true);
            // WASI p3 lifts stream/future types through the component-model async builtins, and
            // `p3::add_to_linker` registers at least one `async func` import
            // (`monotonic-clock.wait-for`), so the store requires `*_async` entrypoints: async
            // export bindings + `instantiate_async`.
            config.wasm_component_model_async(true);
            // Stores cap linear memory at 64 MiB (`WasmStoreState::new`). Wasmtime otherwise
            // reserves 4 GiB of address space per memory, which caps a 39-bit user address space
            // at tens of instances; reserving the store limit keeps static memories (no bounds
            // checks) while making the per-instance cost 64 MiB of untouched VA.
            config.memory_reservation(RESERVED_MEMORY_BYTES);
            config.memory_guard_size(MEMORY_GUARD_BYTES);
            Engine::new(&config).map_err(|error| error.to_string())
        })
        .clone()
}

/// Linear-memory reservation per instance, matching the store limit in [`WasmStoreState::new`].
const RESERVED_MEMORY_BYTES: u64 = 64 * 1024 * 1024;
/// Wasmtime requires a guard size that is a multiple of the host page size; one page catches the
/// overflow a guest would otherwise write into its neighbour.
const MEMORY_GUARD_BYTES: u64 = 64 * 1024;

/// The linker is identical for every package on the shared engine (WASI p3 plus the micro
/// client), so it is built once rather than rebuilt per micro.
fn linker(engine: &Engine) -> Result<Arc<Linker<WasmStoreState>>, String> {
    static LINKER: OnceLock<Result<Arc<Linker<WasmStoreState>>, String>> = OnceLock::new();
    LINKER
        .get_or_init(|| {
            let mut linker = Linker::new(engine);
            wasmtime_wasi::p3::add_to_linker(&mut linker).map_err(|error| error.to_string())?;
            MicroClient::add_to_linker::<_, HasSelf<WasmStoreState>>(&mut linker, |state| state)
                .map_err(|error| error.to_string())?;
            Ok(Arc::new(linker))
        })
        .clone()
}

/// Compile a component, reusing a precompiled artifact when one exists.
///
/// A cold compile of a Dioxus-sized micro is ~160 ms, and demand-driven activation now puts that
/// on the user's first click. The cache is keyed by the artifact bytes plus the engine's own
/// compatibility hash, so a wasmtime upgrade or a config change invalidates it instead of loading
/// incompatible machine code. A missing or unusable cache entry is repaired by compiling.
fn compile_component(engine: &Engine, bytes: &[u8]) -> Result<Component, String> {
    let Some(path) = cache_path(engine, bytes) else {
        return Component::new(engine, bytes).map_err(|error| error.to_string());
    };
    if path.exists() {
        // SAFETY: this file was written by this module and is only ever read afterwards; nothing
        // may modify it while it is mapped. A corrupt or incompatible file fails to load and is
        // recompiled rather than trusted.
        if let Ok(component) = unsafe { Component::deserialize_file(engine, &path) } {
            return Ok(component);
        }
    }
    let compiled = engine
        .precompile_component(bytes)
        .map_err(|error| error.to_string())?;
    if let Some(directory) = path.parent() {
        std::fs::create_dir_all(directory).map_err(|error| {
            format!(
                "cannot create component cache {}: {error}",
                directory.display()
            )
        })?;
    }
    // Write-then-rename so a concurrent reader never maps a partial artifact.
    static TEMP: AtomicU64 = AtomicU64::new(0);
    let temporary = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&temporary, &compiled).map_err(|error| {
        format!(
            "cannot write component cache {}: {error}",
            temporary.display()
        )
    })?;
    std::fs::rename(&temporary, &path)
        .map_err(|error| format!("cannot publish component cache {}: {error}", path.display()))?;
    Component::new(engine, bytes).map_err(|error| error.to_string())
}

/// `$DAYFRAME_COMPONENT_CACHE`, else the XDG/home cache, keyed by content and engine
/// compatibility. `None` means the process has no cache location at all (no `HOME`, no
/// `XDG_CACHE_HOME`); compilation then happens in memory only.
fn cache_path(engine: &Engine, bytes: &[u8]) -> Option<PathBuf> {
    let directory = match std::env::var_os("DAYFRAME_COMPONENT_CACHE") {
        Some(directory) => PathBuf::from(directory),
        None => {
            let base = std::env::var_os("XDG_CACHE_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache"))
                })?;
            base.join("dayframe").join("components")
        }
    };
    let mut key = blake3::Hasher::new();
    key.update(bytes);
    let mut compatibility = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&engine.precompile_compatibility_hash(), &mut compatibility);
    key.update(&std::hash::Hasher::finish(&compatibility).to_le_bytes());
    Some(directory.join(format!("{}.cwasm", key.finalize().to_hex())))
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
        let engine = engine()?;
        let component = compile_component(&engine, bytes.as_ref())?;
        let linker = linker(&engine)?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    async fn instantiate(
        &self,
        host: &Host,
        instance: Instance,
    ) -> Result<(WasmIncarnation, mpsc::Receiver<Work>), ActivationError> {
        let (mut state, incoming) =
            WasmStoreState::new(host.clone(), instance, DocumentLimits::default())?;
        let wake = Arc::new(IncarnationWake::new());
        let waker = Waker::from(Arc::clone(&wake));
        state.collection_wake = Some(Arc::clone(&wake));
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
                wake,
                waker,
            },
            incoming,
        ))
    }
}

struct WasmIncarnation {
    store: Store<WasmStoreState>,
    client: MicroClient,
    fuel_per_call: Arc<AtomicU64>,
    wake: Arc<IncarnationWake>,
    waker: Waker,
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

    async fn tick(&mut self) -> wasmtime::Result<()> {
        self.refuel();
        self.store.drive_until_stalled().await
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
        collections: BTreeMap<String, clay_guest_session::collections::BytesProjection>,
        registrations: BTreeMap<String, clay_guest_session::collections::BytesRegistrations>,
    ) -> Result<(Self, impl std::future::Future<Output = ()> + 'static), ActivationError> {
        let (mut active, incoming) = package.instantiate(host, instance.clone()).await?;
        documents::activate(&mut active, configs, collections, registrations).await?;
        let pending_limit = active.store.data().documents.limits().pending_deliveries;
        let fuel = active.fuel_per_call.clone();
        let incarnation = Arc::new(Mutex::new(Some(active)));
        let driver_incarnation = incarnation.clone();
        let pump_incarnation = incarnation.clone();
        let pump_host = host.clone();
        let pump_instance = instance.clone();
        let documents = clay_guest_session::drive(
            host.clone(),
            instance.clone(),
            incoming,
            pending_limit,
            move |work, delivery| documents::deliver(&driver_incarnation, work, delivery),
        );
        // Stall-driving must retain exclusive store access across its await so synchronous
        // mount/dispatch entry cannot borrow the same incarnation while the engine is active.
        #[expect(
            clippy::await_holding_lock,
            reason = "store access is serialized across stall-driving"
        )]
        let driver = async move {
            tokio::pin!(documents);
            loop {
                let wake = {
                    let guard = pump_incarnation.lock().unwrap();
                    let Some(active) = guard.as_ref() else {
                        return;
                    };
                    active.wake.clone()
                };
                tokio::select! {
                    () = &mut documents => return,
                    () = wake.wait() => {
                        let trapped = {
                            let mut guard = pump_incarnation.lock().unwrap();
                            let Some(active) = guard.as_mut() else {
                                return;
                            };
                            let trapped = active.tick().await.is_err();
                            if trapped {
                                guard.take();
                            }
                            trapped
                        };
                        if trapped {
                            if pump_host.check(&pump_instance).is_ok() {
                                pump_host.close_instance(&pump_instance).unwrap();
                            }
                            return;
                        }
                    }
                }
            }
        };
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
                    match first_poll(
                        active
                            .client
                            .townframe_dayframe_presentation_events()
                            .call_dispatch(
                                &mut active.store,
                                Resource::new_borrow(surface_rep),
                                &wit_event,
                            ),
                        &active.waker,
                    ) {
                        Ok(outcome) => {
                            if outcome.is_err() {
                                guard.take();
                            }
                            outcome
                        }
                        Err(Suspended) => {
                            guard.take();
                            Err(wasmtime::Error::msg(
                                "guest presentation call suspended on its first poll",
                            ))
                        }
                    }
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
                        match first_poll(
                            active
                                .client
                                .townframe_dayframe_presentation_events()
                                .call_unmount(&mut active.store, Resource::new_borrow(surface_rep)),
                            &active.waker,
                        ) {
                            Ok(outcome) => outcome,
                            Err(Suspended) => Err(wasmtime::Error::msg(
                                "guest presentation call suspended on its first poll",
                            )),
                        }
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
        let mount_result = match first_poll(
            active
                .client
                .townframe_dayframe_presentation_events()
                .call_mount(
                    &mut active.store,
                    Resource::new_borrow(placement_rep),
                    &components,
                ),
            &active.waker,
        ) {
            Ok(outcome) => outcome,
            Err(Suspended) => Err(wasmtime::Error::msg(
                "guest presentation call suspended on its first poll",
            )),
        };
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
