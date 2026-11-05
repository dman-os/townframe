//! Backend-neutral `wcmp` host adapter for WIT `presentation-client` micro components
//! (ADR 018 §2, §4).
//!
//! Generated bindings, the resource table, and store semantics live here; native and
//! browser adapters supply only their engine. Surface and placement state, grant checks,
//! handle caps, ingress budgets, and document delivery are delegated to
//! `clay_guest_session`, so engines cannot diverge. Presentation calls poll the guest
//! once and treat a suspension as a trap (ADR 018 §3 as amended); activation is async.

mod collections;
mod documents;
mod wasi;

use clay_guest_session::DocumentSession;
pub use clay_guest_session::InitialContext;
pub use clay_guest_session::InitializationFailure;
use clay_guest_session::Limits as DocumentLimits;
use clay_guest_session::Work;
use clay_guest_session::capabilities::CallError;
use clay_guest_session::capabilities::CapabilityFailure;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_guest_session::capabilities::GuestCall;
use clay_guest_session::capabilities::InFlightCalls;
use clay_guest_session::capabilities::PublishedCapability;
use clay_guest_session::collections::CollectionLimits;
use clay_guest_session::collections::CollectionSession;
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
use std::task::Wake;
use std::task::Waker;
use tokio::sync::mpsc;
use wcmp::Component;
use wcmp::Engine;
use wcmp::HasSelf;
use wcmp::Linker;
use wcmp::Resource;
use wcmp::ResourceAny;
use wcmp::ResourceTable;
use wcmp::Stalled;
use wcmp::Store;

#[derive(Debug, thiserror::Error)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] clay_host::AccessError),
    #[error("Wasm initialization failed: {0}")]
    Initialization(String),
    /// The guest declined to initialize under this configuration (no trap).
    #[error("guest refused initialization: {0}")]
    Refused(#[from] InitializationFailure),
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
            "townframe:dayframe/collections.collection-reader": clay_guest_session::collections::CollectionReader,
            "townframe:dayframe/collections.projection-lease": clay_guest_session::collections::ProjectionReader,
            "townframe:dayframe/collection-registration.registration-writer": clay_guest_session::collections::RegistrationWriter,
            "townframe:dayframe/capability-bank.capability-ref": clay_guest_session::capabilities::CapabilityRef,
            "townframe:dayframe/capability-bank.call-binding": clay_guest_session::capabilities::CallBinding,
            "townframe:dayframe/capability-bank.published-capability": clay_guest_session::capabilities::PublishedCapability,
            "townframe:dayframe/capability-bank.presentation-component": clay_guest_session::capabilities::PresentationComponent,
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
    pub collections: CollectionSession,
    pub capabilities: clay_guest_session::capabilities::CapabilitySession,
    collection_limits: CollectionLimits,
    collection_wake: Option<Arc<IncarnationWake>>,
    /// Where this incarnation's call publications queue their requests (see [`GuestCall`]).
    calls: mpsc::UnboundedSender<GuestCall>,
    /// How many call handlers are in the store, including those whose caller gave up.
    calls_in_flight: InFlightCalls,
    pub presentation: clay_guest_session::PresentationHost,
}

#[cfg(feature = "test-utils")]
impl WcmpStoreState {
    /// A store state for tests that drive host imports directly, with nothing to serve its
    /// call publications: the call queue's receiving end is dropped.
    pub fn detached(
        host: Host,
        instance: Instance,
        limits: DocumentLimits,
    ) -> Result<(Self, mpsc::Receiver<Work>), clay_host::AccessError> {
        Self::new(host, instance, limits, mpsc::unbounded_channel().0)
    }

    /// Push component handles the way a capability redemption does (and under its limit), for
    /// tests that drive the presentation imports with handles of their own.
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

    /// The call handlers in the store.
    pub fn calls_in_flight(&self) -> &InFlightCalls {
        &self.calls_in_flight
    }
}

impl WcmpStoreState {
    fn new(
        host: Host,
        instance: Instance,
        limits: DocumentLimits,
        calls: mpsc::UnboundedSender<GuestCall>,
    ) -> Result<(Self, mpsc::Receiver<Work>), clay_host::AccessError> {
        let (documents, incoming) = DocumentSession::new(host.clone(), instance.clone(), limits)?;
        let collection_limits = CollectionLimits::default();
        let collections = CollectionSession::new(host.clone(), instance.clone(), collection_limits);
        let capabilities = clay_guest_session::capabilities::CapabilitySession::new(
            host.clone(),
            instance.clone(),
            clay_guest_session::capabilities::CapabilityLimits::default(),
        );
        Ok((
            Self {
                table: ResourceTable::new(),
                documents,
                collections,
                capabilities,
                collection_limits,
                collection_wake: None,
                calls,
                calls_in_flight: InFlightCalls::default(),
                presentation: clay_guest_session::PresentationHost::new(host, instance),
            },
            incoming,
        ))
    }
}

/// Drive a synchronous guest entry (`mount`, `dispatch`, `unmount`, a document delivery) to
/// completion on its first poll. The WIT *exports* are sync-typed (ADR 018 §3), so a call that
/// returns `Pending` broke the contract between the host and its guest runtime: nothing can
/// resume the call (the future is dropped here, abandoning the guest callback half run), and
/// closing the instance quietly would only hide the break, as it hid a layout's collapse in the
/// browser (a sync cancel of a host import used to suspend there). It is an invariant break and
/// panics, naming `export`. A guest that blocks inside a sync-typed call is a different case: it
/// fails with the spec's `CannotBlock` cause (`Ready(Err)`), which the caller treats as a trap.
///
/// `waker` is the incarnation's: it is rung once the entry returns, whatever it answered. An
/// entry can make guest tasks runnable with no host waker involved: spawning a task from it
/// queues the task and wakes the guest's executor through a stream write inside the store (see
/// `Tasks::start_doorbell` in `clay_mdk`), and a store only runs its event loop when the
/// incarnation's driver ticks it. Nothing else would wake the driver for that, so the new task
/// would sit unpolled until an unrelated wake: a collection feed's long poll expiring (30 s) is
/// the only one a layout has.
pub fn first_poll<F: Future>(
    call: F,
    waker: &Waker,
    export: impl std::fmt::Display,
) -> F::Output {
    let entered = match std::pin::pin!(call).poll(&mut Context::from_waker(waker)) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!(
            "guest export {export} suspended on its first poll; a synchronous export must \
             decide before it returns"
        ),
    };
    waker.wake_by_ref();
    entered
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
}

struct WcmpIncarnation {
    store: Store<WcmpStoreState>,
    /// Shared so a call task can own a handle to the `call` export beyond a borrow of the
    /// incarnation.
    client: Arc<MicroClient>,
    wake: Arc<IncarnationWake>,
    waker: Waker,
    /// The guest's `instance-root`, held from a successful `initialize` until [`Self::close`].
    root: Option<ResourceAny>,
    /// Publication leases of the publications `initialize` returned, by name. Dropping one
    /// revokes the publication and every consumer proxy derived from it.
    publications: BTreeMap<String, PublishedCapability>,
    /// Set by a call task whose `call` export trapped, so the next tick fails the incarnation.
    /// A spawned task's output is not an error of the store the way Wasmtime's is.
    call_trapped: Arc<AtomicBool>,
}

impl WcmpIncarnation {
    /// One bounded burst of the store's event loop, which advances every call task in it.
    /// Sync entries (`mount`, `dispatch`, document deliveries) borrow the store between bursts,
    /// so a call suspended on something external never holds the incarnation. The store's
    /// waker is a no-op: whatever a task awaits wakes the incarnation itself (see
    /// [`IncarnationWake::drive`]).
    async fn tick(&mut self) -> wcmp::Result<()> {
        // A guest that yielded is still runnable but the drive returns so the other arms of the
        // driver get their turn: ask for another tick instead of looping here.
        if self.store.run_until_stalled()? == Stalled::Yielded {
            self.wake.wake_by_ref();
        }
        if self.call_trapped.load(Ordering::Acquire) {
            return Err(wcmp::Error::Internal {
                message: "guest trapped during a call".into(),
            });
        }
        Ok(())
    }

    /// Start the guest's exported `call` for one queued request as a task in this
    /// incarnation's store and return without waiting for it. Any number of calls may be in
    /// flight: each is a component-model task of its own, and the guest keeps their state apart
    /// (see `clay_mdk`'s task hook). The reply is sent from the task when the handler returns;
    /// dropping the store first (a trap, a forced close) drops the task and with it the reply,
    /// which the caller reads as `Revoked`. A caller that gave up just loses the reply: the
    /// host cannot cancel a guest task.
    fn start_call(&mut self, call: GuestCall) -> wcmp::Result<()> {
        let GuestCall {
            publication,
            request,
            reply,
        } = call;
        let client = Arc::clone(&self.client);
        let in_flight = self.store.data().calls_in_flight.start();
        let trapped = Arc::clone(&self.call_trapped);
        let wake = Arc::clone(&self.wake);
        // The handle is dropped on purpose: dropping it cancels nothing.
        self.store.spawn(move |accessor| async move {
            let _in_flight = in_flight;
            match client
                .townframe_dayframe_micro_lifecycle()
                .call_call(&accessor, publication, request)
                .await
            {
                Ok(handled) => drop(reply.send(handled.map_err(CallError::Provider))),
                Err(trap) => {
                    drop(reply.send(Err(CallError::Provider(format!(
                        "guest trapped during call: {trap}"
                    )))));
                    trapped.store(true, Ordering::Release);
                    wake.wake_by_ref();
                }
            }
        })?;
        Ok(())
    }

    /// The guest half of an orderly close (ADR 030 §6): run `instance-root.close` while the
    /// incarnation's host authority and dependencies are still live, then drop the root (its
    /// guest destructor releases whatever the root held) and only then revoke the publications.
    /// The store itself stays up so a caller can audit what the guest still holds.
    async fn close(&mut self) -> Result<(), String> {
        let root = self
            .root
            .take()
            .expect("only an initialized incarnation is closed");
        let lifecycle = self.client.townframe_dayframe_micro_lifecycle();
        let closed = self
            .store
            .run_concurrent(async |accessor| {
                lifecycle.instance_root().call_close(accessor, root).await
            })
            .await
            .and_then(|trapped| trapped)
            .map_err(|trap| format!("guest trapped while closing: {trap}"));
        let dropped = root
            .resource_drop(&mut self.store.as_context_mut())
            .map_err(|trap| format!("guest trapped dropping its instance root: {trap}"));
        self.publications.clear();
        closed??;
        dropped
    }
}

/// One activated incarnation. It is `!Send`: the `wcmp` store is not thread-safe, so it lives
/// behind an `Rc` in the surface factory's closures, which already admit non-thread-safe state
/// (ADR 018 §4).
#[derive(Clone)]
pub struct WcmpRuntime {
    host: Host,
    instance: Instance,
    incarnation: Rc<RefCell<Option<WcmpIncarnation>>>,
    /// What each returned publication publishes, by name. The leases themselves live in the
    /// incarnation, so a close revokes them.
    publications: Rc<BTreeMap<String, CapabilityPublication>>,
    revoked: Arc<AtomicBool>,
}

impl WcmpRuntime {
    /// Instantiate `package` for `instance` and run its `micro-lifecycle.initialize` with
    /// `context`. The returned driver future delivers document events and drives the guest's
    /// background tasks until the incarnation ends; the caller owns it.
    pub async fn initialize(
        host: &Host,
        instance: Instance,
        package: &WcmpPackage,
        context: InitialContext,
    ) -> Result<(Self, impl Future<Output = ()> + 'static), ActivationError> {
        let (calls, mut incoming_calls) = mpsc::unbounded_channel();
        let (mut state, incoming) = WcmpStoreState::new(
            host.clone(),
            instance.clone(),
            DocumentLimits::default(),
            calls,
        )?;
        let wake = Arc::new(IncarnationWake::new());
        let waker = Waker::from(Arc::clone(&wake));
        state.collection_wake = Some(Arc::clone(&wake));
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
        let mut active = WcmpIncarnation {
            store,
            client: Arc::new(client),
            wake,
            waker,
            root: None,
            publications: BTreeMap::new(),
            call_trapped: Arc::new(AtomicBool::new(false)),
        };
        let (root, leases) = documents::initialize(&mut active, context).await?;
        active.root = Some(root);
        let publications = leases
            .iter()
            .map(|(name, lease)| (name.clone(), lease.publication()))
            .collect();
        active.publications = leases;
        let pending_limit = active.store.data().documents.limits().pending_deliveries;
        let incarnation = Rc::new(RefCell::new(Some(active)));
        let driver_incarnation = Rc::clone(&incarnation);
        let pump_incarnation = Rc::clone(&incarnation);
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
            clippy::await_holding_refcell_ref,
            reason = "store access is serialized across stall-driving"
        )]
        let driver = async move {
            tokio::pin!(documents);
            loop {
                let wake = {
                    let guard = pump_incarnation.borrow();
                    let Some(active) = guard.as_ref() else {
                        return;
                    };
                    Arc::clone(&active.wake)
                };
                tokio::select! {
                    () = &mut documents => return,
                    Some(call) = incoming_calls.recv() => {
                        // Start the handler as a task in the provider's own store and go back
                        // to the loop: the wake arm drives it in bounded bursts, so this
                        // incarnation's sync entries and other calls are never held up
                        // behind a handler that is suspended on something external.
                        let mut guard = pump_incarnation.borrow_mut();
                        let Some(active) = guard.as_mut() else {
                            // Closing or closed: the guest half ended before this call began.
                            drop(call.reply.send(Err(CapabilityFailure::Revoked.into())));
                            return;
                        };
                        active
                            .start_call(call)
                            .expect("store spawns the call task");
                        active.wake.wake_by_ref();
                    }
                    () = wake.wait() => {
                        let trapped = {
                            let mut guard = pump_incarnation.borrow_mut();
                            let Some(active) = guard.as_mut() else {
                                return;
                            };
                            let outcome = active.tick().await;
                            if outcome.is_err() {
                                guard.take();
                            }
                            outcome
                        };
                        if let Err(error) = trapped {
                            if pump_host.check(&pump_instance).is_ok() {
                                pump_host.record_fault(
                                    &pump_instance,
                                    format!("guest store failed while driven: {error}"),
                                );
                                pump_host.close_instance(&pump_instance).unwrap();
                            }
                            return;
                        }
                    }
                }
            }
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
                publications: Rc::new(publications),
                revoked,
            },
            driver,
        ))
    }

    /// The publications this incarnation's `initialize` returned, by name. They become
    /// reachable to other micros only once the host commits the initialization.
    pub fn publications(&self) -> BTreeMap<String, CapabilityPublication> {
        (*self.publications).clone()
    }

    /// Close the incarnation's guest half (ADR 030 §6 step 2). The incarnation stops taking
    /// presentation and document calls, its `instance-root.close` runs to completion, the
    /// root and the publication leases are released, and the store is dropped. The caller has
    /// already withdrawn outward visibility and tears the host incarnation down afterwards.
    pub async fn close(&self) -> Result<(), String> {
        let taken = self.incarnation.borrow_mut().take();
        let mut active = taken.ok_or_else(|| "the incarnation already ended".to_owned())?;
        active.close().await
    }

    /// Mount the presentation this incarnation published as `publication`.
    pub fn mount(
        &self,
        publication: &str,
        writer: Arc<dyn SurfaceSink<ComponentRef>>,
    ) -> Result<SurfaceSession, PresentationError> {
        let surface = self.mount_guest_surface(publication, &writer)?;
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
        publication: &str,
        writer: &Arc<dyn SurfaceSink<ComponentRef>>,
    ) -> Result<u32, PresentationError> {
        let mut guard = self.incarnation.borrow_mut();
        if self.host.check(&self.instance).is_err() {
            return Err(PresentationError::Revoked);
        }
        let Some(active) = guard.as_mut() else {
            return Err(PresentationError::Revoked);
        };
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
                    publication,
                    Resource::new_borrow(placement_rep),
                ),
            &active.waker,
            format_args!("presentation.mount of {:?}", self.instance),
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
            Err(trap) => {
                guard.take();
                drop(guard);
                writer.withdraw();
                self.host.record_fault(
                    &self.instance,
                    format!("guest trapped in presentation.mount: {trap}"),
                );
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
            &active.waker,
            format_args!("presentation.dispatch of {:?}", self.instance),
        );
        match outcome {
            Ok(response) => EventResponse {
                prevent_default: response.prevent_default,
                stop_propagation: response.stop_propagation,
            },
            Err(trap) => {
                guard.take();
                drop(guard);
                self.trapped(writer, format!("guest trapped in presentation.dispatch: {trap}"));
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
                    &active.waker,
                    format_args!("presentation.unmount of {:?}", self.instance),
                );
                match outcome {
                    Ok(()) => {
                        wit_presentation::HostSurface::drop(
                            active.store.data_mut(),
                            Resource::new_own(surface),
                        )
                        .expect("mounted surface must remain in the host resource table");
                        None
                    }
                    Err(trap) => {
                        guard.take();
                        Some(format!("guest trapped in presentation.unmount: {trap}"))
                    }
                }
            }
            _ => None,
        };
        drop(guard);
        writer.withdraw();
        if let Some(cause) = trapped {
            self.host.record_fault(&self.instance, cause);
            self.host.close_instance(&self.instance).unwrap();
        }
    }

    fn trapped(&self, writer: &Arc<dyn SurfaceSink<ComponentRef>>, cause: String) {
        writer.withdraw();
        self.host.record_fault(&self.instance, cause);
        self.host.close_instance(&self.instance).unwrap();
    }
}

#[cfg(feature = "test-utils")]
pub mod test_utils {
    pub use crate::WcmpStoreState;
    pub use crate::convert_mutation;
    pub use crate::documents::receipt_to_wire;
    pub use crate::documents::wit;
    pub use crate::wasi::define as define_wasi;
    pub use crate::wit_presentation;

    /// [`crate::WcmpRuntime::close`], inspecting the store once the guest released its root
    /// and provisions and before the store drops: whatever `inspect` still finds there is what
    /// the close path leaked.
    pub async fn close_inspecting<R>(
        runtime: &crate::WcmpRuntime,
        inspect: impl FnOnce(&WcmpStoreState) -> R,
    ) -> (Result<(), String>, R) {
        let taken = runtime.incarnation.borrow_mut().take();
        let mut active = taken.expect("only a live incarnation is closed");
        let closed = active.close().await;
        (closed, inspect(active.store.data()))
    }

    pub fn inspect<R>(
        runtime: &crate::WcmpRuntime,
        inspect: impl FnOnce(&WcmpStoreState) -> R,
    ) -> R {
        let active = runtime.incarnation.borrow();
        inspect(
            active
                .as_ref()
                .expect("only a live incarnation is inspected")
                .store
                .data(),
        )
    }

    pub fn resolve_component(
        table: &wcmp::ResourceTable,
        reference: &wcmp::Resource<crate::WcmpComponentRef>,
    ) -> wcmp::Result<clay_host::ComponentRef> {
        Ok(table.get(reference)?.0)
    }
}
