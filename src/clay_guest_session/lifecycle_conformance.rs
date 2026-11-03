//! Engine conformance for the ADR 024 activation boundary, run against the prepared
//! `lifecycle` fixture component (`src/clay_mdk/lifecycle_fixture.rs`).
//!
//! Each engine crate implements [`Incarnations`] over its runtime and runs these scenarios from
//! its own tests, so every engine is held to one chain: configured requirement → strong binding
//! (or external lease) → provider initialization → `serve` main loop started → named publication
//! visible only once committed → inbox closed and `serve` drained with dependencies live →
//! publications revoked → reverse-order teardown that leaves the guest holding nothing.
//!
//! Scenarios must run inside a [`tokio::task::LocalSet`]: incarnation drivers are spawned with
//! [`tokio::task::spawn_local`] and joined (panics propagate) once the host closes.
use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::collections::BytesRegistrations;
use crate::collections::CollectionSession;
use crate::collections::bytes_registrations;
use clay_collection::Change;
use clay_collection::ReadLimits;
use clay_host::CloseMode;
use clay_host::ConfiguredMicro;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Initialized;
use clay_host::Instance;
use clay_host::Lease;
use clay_host::Lifecycle;
use clay_host::LocalFuture;
use clay_host::MicroId;
use clay_host::MicroState;
use clay_host::Requirement;
use clay_host::Root;
use clay_host::StartError;
use clay_host::Supervisor;

use crate::GuestClose;
use crate::InitialContext;
use crate::capabilities::CallError;
use crate::capabilities::CallTarget;
use crate::capabilities::CapabilityFailure;
use crate::capabilities::CapabilityLimits;
use crate::capabilities::CapabilityPublication;
use crate::capabilities::CapabilityRef;
use crate::capabilities::CapabilitySession;
use crate::capabilities::ContractIdentity;
use crate::capabilities::DeclaredPublication;
use crate::capabilities::LazyEndpoint;
use crate::capabilities::PublicationKind;
use crate::capabilities::PublishedCapability;
use crate::capabilities::RefStatus;

/// The manifest's names (`provides[].id`) for the fixture's two publications: a presentation
/// and an `echo` call whose handler answers `echo:` plus the request.
const PUBLICATION: &str = "presentation";
const ECHO: &str = "echo";
const RELAY: &str = "relay";

fn echo_contract() -> ContractIdentity {
    ContractIdentity::new("dayframe.lifecycle", ECHO, (1, 0, 0))
}

fn relay_contract() -> ContractIdentity {
    ContractIdentity::new("dayframe.lifecycle", RELAY, (1, 0, 0))
}

/// What the fixture's manifest declares it publishes (the manifest vocabulary has no kind yet,
/// so the scenarios state it).
fn declared_publications() -> BTreeMap<String, DeclaredPublication> {
    BTreeMap::from([
        (
            PUBLICATION.to_owned(),
            DeclaredPublication {
                kind: PublicationKind::Presentation,
                contract: ContractIdentity::presentation_component(),
            },
        ),
        (
            ECHO.to_owned(),
            DeclaredPublication {
                kind: PublicationKind::Call,
                contract: echo_contract(),
            },
        ),
        (
            RELAY.to_owned(),
            DeclaredPublication {
                kind: PublicationKind::Call,
                contract: relay_contract(),
            },
        ),
    ])
}

/// What an incarnation's store still holds once its guest half closed. Anything but
/// [`GuestResidue::NONE`] is a resource the close path leaked to the store's drop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestResidue {
    pub table_empty: bool,
    pub publications: usize,
    pub references: usize,
    pub proxies: usize,
    pub readers: usize,
    pub writers: usize,
}

impl GuestResidue {
    pub const NONE: Self = Self {
        table_empty: true,
        publications: 0,
        references: 0,
        proxies: 0,
        readers: 0,
        writers: 0,
    };

    pub fn of(
        table_empty: bool,
        capabilities: &CapabilitySession,
        collections: &CollectionSession,
    ) -> Self {
        Self {
            table_empty,
            publications: capabilities.publication_count(),
            references: capabilities.reference_count(),
            proxies: capabilities.proxy_count(),
            readers: collections.reader_count(),
            writers: collections.writer_count(),
        }
    }
}

/// A presentation an engine mounted, and the text the guest rendered into it.
pub struct Mounted {
    pub session: Box<dyn Any>,
    pub rendered: String,
}

/// One engine's incarnations of the prepared lifecycle fixture.
pub trait Incarnations: 'static {
    type Runtime: 'static;

    /// Instantiate the fixture for `instance` and run its `initialize` with `context`,
    /// returning the runtime and the driver future the caller must poll until the
    /// incarnation ends. A failure is the engine's [`std::fmt::Display`] of it.
    #[expect(
        clippy::type_complexity,
        reason = "the nested Result of (runtime, driver) is the trait's whole contract; an alias would only rename it"
    )]
    fn initialize(
        self: Rc<Self>,
        host: Host,
        instance: Instance,
        context: InitialContext,
    ) -> LocalFuture<'static, Result<(Self::Runtime, LocalFuture<'static, ()>), String>>;

    /// The publications `initialize` returned, by name (admitted against the declarations in
    /// the [`InitialContext`]).
    fn publications(runtime: &Self::Runtime) -> BTreeMap<String, CapabilityPublication>;

    /// Mount the presentation the runtime published as `publication` into an engine-owned
    /// sink and report what the guest rendered into it. Dropping the [`Mounted`] unmounts.
    fn mount(runtime: &Self::Runtime, publication: &str) -> Result<Mounted, String>;

    fn inspect(runtime: &Self::Runtime) -> GuestResidue;

    /// Close the guest half and report which path ran and what the store still held
    /// afterwards, before it drops. A [`CloseMode::Orderly`] close closes the guest's inbox and
    /// waits for `serve` to return, however long its in-flight handlers take
    /// ([`GuestClose::Drained`]). A [`CloseMode::Forced`] close does the same unless requests
    /// are in flight, in which case it abandons `serve` without waiting
    /// ([`GuestClose::Abandoned`]); every caller still waiting is then answered `Revoked` and
    /// the residue is meaningless.
    fn close(
        runtime: Self::Runtime,
        mode: CloseMode,
    ) -> LocalFuture<'static, (Result<GuestClose, String>, GuestResidue)>;
}

/// The consumer requires `provider`'s presentation and redeems it while initializing and again
/// while closing. An external lease on the same publication is a second strong edge. Disabling
/// the provider must close the consumer first, with the provider still live, and revoke the
/// lease's publication.
pub async fn provider_consumer_graph<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor("provider");
    let (provider_id, consumer_id) = (MicroId::new("provider"), MicroId::new("consumer"));
    assert!(harness.log().is_empty(), "configuring instantiates nothing");

    assert_eq!(supervisor.reconcile().await, BTreeMap::new());
    assert_eq!(
        harness.log(),
        ["initialize provider", "initialize consumer"]
    );
    let provider = supervisor.instance(&provider_id).unwrap();
    let consumer = supervisor.instance(&consumer_id).unwrap();
    assert!(
        harness.host.is_bound(&provider),
        "the consumer holds its provider"
    );
    let lease = supervisor
        .acquire(&provider_id, PUBLICATION)
        .await
        .expect("the committed provider publishes its presentation");
    assert_eq!(
        harness.log().len(),
        2,
        "acquiring a live micro joins its incarnation"
    );
    let observed = harness.observe(&lease);
    assert!(
        observed.redeem_presentation().await.is_ok(),
        "a committed publication is visible outside its incarnation"
    );
    assert!(
        supervisor.acquire(&consumer_id, PUBLICATION).await.is_err(),
        "the consumer publishes nothing"
    );
    // The consumer's own initialization already called the provider's `echo` from its store
    // into the provider's (it fails unless the provider answered `echo:ping`); a host-side
    // recipient does the same through the same publication.
    let echo = supervisor
        .acquire(&provider_id, ECHO)
        .await
        .expect("the committed provider publishes its echo call");
    assert_eq!(
        harness.observe(&echo).call(b"ping".to_vec()).await,
        Ok(b"echo:ping".to_vec())
    );
    drop(echo);

    // Closing the provider closes its dependent first; the consumer's guest `close`
    // redeems its proxy and fails unless the provider is still live at that point.
    supervisor.disable(&provider_id).await.unwrap();
    assert_eq!(
        harness.log(),
        [
            "initialize provider",
            "initialize consumer",
            "close consumer",
            "close provider"
        ]
    );
    assert_eq!(supervisor.state(&provider_id), MicroState::Disabled);
    assert_eq!(supervisor.state(&consumer_id), MicroState::Idle);
    assert_eq!(harness.host.lifecycle(&consumer), Lifecycle::Closed);
    assert_eq!(harness.host.lifecycle(&provider), Lifecycle::Closed);
    assert!(lease.is_revoked());
    assert_eq!(
        observed.redeem_presentation().await.err(),
        Some(CapabilityFailure::Revoked)
    );
    drop((observed, lease));
    harness.finish(supervisor).await;
}

/// ADR 025 §§6 and 9 against a lazy reference to the fixture's provider: `status` reports
/// `Dormant` without starting anything; a `call` activates the provider, answers, and lets it
/// idle-close once the call's lease drops; a `retain-call` binding keeps it up until dropped;
/// a redeemed presentation can be mounted, keeps it up while held, and lets it close when
/// dropped.
pub async fn lazy_references_activate_retain_and_release<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor("provider");
    let provider_id = MicroId::new("provider");
    let lazy = |publication: &str, contract: ContractIdentity| {
        harness
            .shared
            .observer
            .mint_lazy(LazyEndpoint {
                micro: provider_id.clone(),
                publication: publication.to_owned(),
                contract,
                activator: supervisor.activator(),
            })
            .unwrap()
    };
    let echo = lazy(ECHO, echo_contract());
    let view = lazy(PUBLICATION, ContractIdentity::presentation_component());
    let closed = |closes: usize| {
        let mut expected = Vec::new();
        for _ in 0..closes {
            expected.extend([
                "initialize provider".to_owned(),
                "close provider".to_owned(),
            ]);
        }
        expected
    };
    let idle = async |expected: Vec<String>| {
        while supervisor.state(&provider_id) != MicroState::Idle {
            tokio::task::yield_now().await;
        }
        assert_eq!(harness.log(), expected);
    };
    let stays_up = async |lives: usize| {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(supervisor.state(&provider_id), MicroState::Active);
        assert_eq!(
            harness.log().len(),
            lives * 2 - 1,
            "no further activation or close"
        );
    };
    tokio::select! {
        never = supervisor.drive() => never,
        () = async {
            assert_eq!(echo.status(), RefStatus::Dormant);
            assert_eq!(view.status(), RefStatus::Dormant);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert!(harness.log().is_empty(), "reading a status never starts the provider");

            // A call activates the provider and holds it only for the call.
            assert_eq!(echo.call(b"one".to_vec()).await, Ok(b"echo:one".to_vec()));
            idle(closed(1)).await;
            assert_eq!(echo.status(), RefStatus::Dormant);

            // A retained binding keeps the provider up for as long as it is held.
            let binding = echo.retain_call().await.unwrap();
            assert_eq!(binding.call(b"two".to_vec()).await, Ok(b"echo:two".to_vec()));
            assert_eq!(echo.status(), RefStatus::Live);
            stays_up(2).await;
            assert_eq!(binding.call(b"three".to_vec()).await, Ok(b"echo:three".to_vec()));
            drop(binding);
            idle(closed(2)).await;

            // A redeemed presentation retains it too, and it mounts while held.
            let component = view.redeem_presentation().await.unwrap();
            assert!(component.component().is_ok());
            let runtime = harness
                .shared
                .runtime("provider")
                .downcast::<E::Runtime>()
                .unwrap_or_else(|_| panic!("the provider runs this engine"));
            let mounted = E::mount(&runtime, PUBLICATION).expect("a live presentation mounts");
            drop(runtime);
            stays_up(3).await;
            drop(mounted);
            stays_up(3).await;
            drop(component);
            idle(closed(3)).await;
            assert_eq!(view.status(), RefStatus::Dormant);
            assert!(matches!(
                echo.call(b"after".to_vec()).await,
                Ok(response) if response == b"echo:after"
            ));
            idle(closed(4)).await;
        } => {}
    }
    drop((echo, view));
    harness.finish(supervisor).await;
}

/// A call the scenario controls, standing in for "something external" a guest handler awaits.
/// Each call announces itself on `entered` and then waits until the scenario releases the
/// request it carried, by value; a call whose future is dropped before its release (its caller
/// in the guest gave up, or the guest was torn down) counts on `abandoned`.
struct Gate {
    entered: Arc<tokio::sync::Semaphore>,
    abandoned: Arc<tokio::sync::Semaphore>,
    waiting: Mutex<BTreeMap<Vec<u8>, tokio::sync::oneshot::Sender<()>>>,
}

/// Counts a held call as abandoned unless it was released.
struct Abandonment(Option<Arc<tokio::sync::Semaphore>>);

impl Abandonment {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for Abandonment {
    fn drop(&mut self) {
        if let Some(abandoned) = self.0.take() {
            abandoned.add_permits(1);
        }
    }
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Arc::new(tokio::sync::Semaphore::new(0)),
            abandoned: Arc::new(tokio::sync::Semaphore::new(0)),
            waiting: Mutex::default(),
        })
    }

    /// Wait for one more call to reach the gate.
    async fn entered(&self) {
        self.entered.acquire().await.unwrap().forget();
    }

    /// Wait for one more held call to be dropped before its release.
    async fn abandoned(&self) {
        self.abandoned.acquire().await.unwrap().forget();
    }

    /// Let the held call that carried `request` answer.
    fn release(&self, request: &[u8]) {
        self.waiting
            .lock()
            .unwrap()
            .remove(request)
            .expect("a call carrying that request is held")
            .send(())
            .expect("the held call still waits");
    }
}

impl CallTarget for Gate {
    fn call(
        &self,
        request: Vec<u8>,
        retain: Option<Lease<CapabilityPublication>>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let (release, released) = tokio::sync::oneshot::channel();
        assert!(
            self.waiting
                .lock()
                .unwrap()
                .insert(request.clone(), release)
                .is_none(),
            "one held call per request"
        );
        self.entered.add_permits(1);
        let mut abandonment = Abandonment(Some(Arc::clone(&self.abandoned)));
        Box::pin(async move {
            let _retain = retain;
            released
                .await
                .map_err(|_unreleased| CallError::Provider("the gate was dropped".to_owned()))?;
            abandonment.disarm();
            Ok([b"held:".as_slice(), &request].concat())
        })
    }
}

/// A call that forwards to a publication that only exists after the caller's own graph does,
/// which is how a scenario closes a loop between incarnations. It is owned by a node of its
/// own (`from`'s incarnation).
struct Loopback {
    from: CapabilitySession,
    to: Arc<OnceLock<CapabilityPublication>>,
}

impl CallTarget for Loopback {
    fn call(
        &self,
        request: Vec<u8>,
        retain: Option<Lease<CapabilityPublication>>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let (from, to) = (self.from.clone(), Arc::clone(&self.to));
        Box::pin(async move {
            let _retain = retain;
            let target = from
                .mint(to.get().expect("the loop is closed before any call"))
                .unwrap();
            target.call(request).await
        })
    }
}

/// One incarnation of the fixture started outside a supervisor.
struct Started<E: Incarnations> {
    instance: Instance,
    runtime: E::Runtime,
    publications: BTreeMap<String, CapabilityPublication>,
}

/// Incarnations of the fixture's `provider` role wired by hand, so a scenario can bind each one's
/// `service` requirement to a call it controls and drive concurrent calls from outside.
struct Rig<E> {
    engine: Rc<E>,
    host: Host,
    observer: CapabilitySession,
    observer_instance: Instance,
    drivers: RefCell<Vec<tokio::task::JoinHandle<()>>>,
}

impl<E: Incarnations> Rig<E> {
    fn new(engine: E) -> Self {
        let host = Host::new();
        let observer_instance = host.start("observer");
        let observer = CapabilitySession::new(
            host.clone(),
            observer_instance.clone(),
            CapabilityLimits::default(),
        );
        Self {
            engine: Rc::new(engine),
            host,
            observer,
            observer_instance,
            drivers: RefCell::default(),
        }
    }

    /// A host-side incarnation of its own, publishing `loopback` forwarding to `to`, so a
    /// scenario's loop passes through a node that is nobody else.
    fn loopback(
        &self,
        name: &str,
        to: &Arc<OnceLock<CapabilityPublication>>,
    ) -> PublishedCapability {
        let instance = self.host.start(name);
        let session =
            CapabilitySession::new(self.host.clone(), instance, CapabilityLimits::default());
        session
            .publish_call(
                name,
                echo_contract(),
                Arc::new(Loopback {
                    from: session.clone(),
                    to: Arc::clone(to),
                }),
            )
            .unwrap()
    }

    /// A gate bound as a `service`: the lease keeps it published and the [`Gate`] lets the
    /// scenario count the calls that entered it and let them out.
    fn gate(&self) -> (PublishedCapability, Arc<Gate>) {
        let gate = Gate::new();
        let owner = self.host.start("gate");
        let owner = CapabilitySession::new(self.host.clone(), owner, CapabilityLimits::default());
        let lease = owner
            .publish_call(
                "gate",
                echo_contract(),
                Arc::clone(&gate) as Arc<dyn CallTarget>,
            )
            .unwrap();
        (lease, gate)
    }

    /// Start a committed `provider` incarnation, publishing `relay` when `service` is bound.
    async fn provider(&self, name: &str, service: Option<CapabilityPublication>) -> Started<E> {
        self.provider_limited(name, service, None).await
    }

    /// [`Self::provider`] serving at most `in_flight_requests` requests at once.
    async fn provider_limited(
        &self,
        name: &str,
        service: Option<CapabilityPublication>,
        in_flight_requests: Option<usize>,
    ) -> Started<E> {
        let instance = self.host.create(name).unwrap();
        self.host.begin_initialization(&instance).unwrap();
        let mut arguments = Vec::new();
        ciborium::into_writer("provider", &mut arguments).unwrap();
        let (runtime, driver) = Rc::clone(&self.engine)
            .initialize(
                self.host.clone(),
                instance.clone(),
                InitialContext {
                    arguments,
                    requirements: service
                        .into_iter()
                        .map(|publication| ("service".to_owned(), publication))
                        .collect(),
                    publications: declared_publications(),
                    in_flight_requests,
                    ..InitialContext::default()
                },
            )
            .await
            .unwrap();
        self.drivers
            .borrow_mut()
            .push(tokio::task::spawn_local(driver));
        let publications = E::publications(&runtime);
        self.host.commit_initialization(&instance).unwrap();
        Started {
            instance,
            runtime,
            publications,
        }
    }

    /// Call `publication` of `provider` from outside it, on a task of its own.
    fn call(
        &self,
        provider: &Started<E>,
        publication: &str,
        request: &[u8],
    ) -> tokio::task::JoinHandle<Result<Vec<u8>, CallError>> {
        let reference = self
            .observer
            .mint(&provider.publications[publication])
            .unwrap();
        let request = request.to_vec();
        tokio::task::spawn_local(async move { reference.call(request).await })
    }

    /// Close what is still live, the host, and join every driver.
    async fn finish(self, live: Vec<Started<E>>) {
        for Started {
            instance, runtime, ..
        } in live
        {
            let (closed, residue) = E::close(runtime, CloseMode::Orderly).await;
            assert_eq!(closed.unwrap(), GuestClose::Drained);
            assert_eq!(residue, GuestResidue::NONE);
            self.host.close_instance(&instance).unwrap();
        }
        self.host.close_instance(&self.observer_instance).unwrap();
        self.host.close();
        for driver in self.drivers.take() {
            driver.await.unwrap();
        }
    }
}

async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// A provider call that is suspended on something external does not hold its incarnation: a
/// sync `mount` and its unmount enter the same guest meanwhile, and the call still completes
/// once what it awaited is released.
pub async fn suspended_call_does_not_block_sync_entry<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig.provider("provider", Some(service.publication())).await;
    let call = rig.call(&provider, RELAY, b"one");
    gate.entered().await;
    let mounted = E::mount(&provider.runtime, PUBLICATION)
        .expect("a presentation mounts while a call is suspended in the same guest");
    assert!(!call.is_finished(), "the call is still waiting on its gate");
    drop(mounted);
    gate.release(b"one");
    assert_eq!(call.await.unwrap(), Ok(b"held:one".to_vec()));
    drop(service);
    rig.finish(vec![provider]).await;
}

/// Two calls into one provider are in flight together, each suspended on its own held call
/// inside the one guest, and they complete out of order, each with its own answer.
pub async fn two_calls_into_one_micro_are_in_flight_together<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig.provider("provider", Some(service.publication())).await;
    let first = rig.call(&provider, RELAY, b"first");
    let second = rig.call(&provider, RELAY, b"second");
    // Both reached the gate, so the second started while the first was suspended.
    gate.entered().await;
    gate.entered().await;
    assert!(!first.is_finished() && !second.is_finished());
    gate.release(b"second");
    assert_eq!(second.await.unwrap(), Ok(b"held:second".to_vec()));
    assert!(
        !first.is_finished(),
        "the first call is still waiting on its gate"
    );
    gate.release(b"first");
    assert_eq!(first.await.unwrap(), Ok(b"held:first".to_vec()));
    drop(service);
    rig.finish(vec![provider]).await;
}

/// `a` relays to `b`, whose service reaches back into `a` while `a`'s relay is still waiting on
/// `b`. `a` serves the re-entering call beside its suspended relay, so the chain completes.
pub async fn reentrant_call_chain_completes<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let back_into_a = Arc::new(OnceLock::new());
    let loopback = rig.loopback("loopback", &back_into_a);
    let b = rig.provider("b", Some(loopback.publication())).await;
    let a = rig.provider("a", Some(b.publications[RELAY].clone())).await;
    back_into_a.set(a.publications[ECHO].clone()).unwrap();
    assert_eq!(
        rig.call(&a, RELAY, b"ping").await.unwrap(),
        Ok(b"echo:ping".to_vec())
    );
    drop(loopback);
    rig.finish(vec![a, b]).await;
}

/// A forced close with calls in flight revokes their callers. The guest's `serve` is abandoned
/// without waiting for its handlers, and the host teardown that follows does not depend on it
/// (ADR 024 §6).
pub async fn close_revokes_calls_in_flight<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig.provider("provider", Some(service.publication())).await;
    let first = rig.call(&provider, RELAY, b"first");
    let second = rig.call(&provider, RELAY, b"second");
    gate.entered().await;
    gate.entered().await;
    let Started {
        instance, runtime, ..
    } = provider;
    let (closed, _residue) = E::close(runtime, CloseMode::Forced).await;
    assert_eq!(closed.unwrap(), GuestClose::Abandoned);
    rig.host.close_instance(&instance).unwrap();
    assert_eq!(rig.host.lifecycle(&instance), Lifecycle::Closed);
    for call in [first, second] {
        assert_eq!(
            call.await.unwrap(),
            Err(CallError::Capability(CapabilityFailure::Revoked))
        );
    }
    drop(service);
    rig.finish(Vec::new()).await;
}

/// An orderly close waits for the guest to drain: a handler still suspended when the inbox
/// closes finishes during the drain and answers its caller, and only then does `serve` return
/// and the close complete, leaving the guest holding nothing.
pub async fn orderly_close_drains<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig.provider("provider", Some(service.publication())).await;
    let call = rig.call(&provider, RELAY, b"held");
    gate.entered().await;
    let Started {
        instance, runtime, ..
    } = provider;
    let close = tokio::task::spawn_local(E::close(runtime, CloseMode::Orderly));
    settle().await;
    assert!(!close.is_finished(), "the close waits for the handler");
    assert!(!call.is_finished());
    gate.release(b"held");
    assert_eq!(call.await.unwrap(), Ok(b"held:held".to_vec()));
    let (closed, residue) = close.await.unwrap();
    assert_eq!(closed.unwrap(), GuestClose::Drained);
    assert_eq!(residue, GuestResidue::NONE);
    rig.host.close_instance(&instance).unwrap();
    drop(service);
    rig.finish(Vec::new()).await;
}

/// A caller that gives up reaches the handler: the guest sees its `responder.cancelled` resolve
/// and stops the handler, which drops what it was awaiting (here a held call into a service),
/// and the provider serves on.
pub async fn cancellation_reaches_the_guest<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig.provider("provider", Some(service.publication())).await;
    let call = rig.call(&provider, RELAY, b"given-up");
    gate.entered().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    gate.abandoned().await;
    assert_eq!(
        rig.call(&provider, ECHO, b"after").await.unwrap(),
        Ok(b"echo:after".to_vec()),
        "the provider serves on after a cancelled request"
    );
    drop(service);
    rig.finish(vec![provider]).await;
}

/// An incarnation serves at most its bound of requests at once: one more is refused with
/// `limit-exceeded` and never reaches the guest, and a slot frees when a handler answers.
pub async fn requests_over_the_limit_are_refused<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig
        .provider_limited("provider", Some(service.publication()), Some(2))
        .await;
    let first = rig.call(&provider, RELAY, b"first");
    let second = rig.call(&provider, RELAY, b"second");
    gate.entered().await;
    gate.entered().await;
    assert_eq!(
        rig.call(&provider, RELAY, b"third").await.unwrap(),
        Err(CallError::Capability(CapabilityFailure::LimitExceeded))
    );
    assert_eq!(
        gate.entered.available_permits(),
        0,
        "the refused request never reached the guest"
    );
    gate.release(b"first");
    assert_eq!(first.await.unwrap(), Ok(b"held:first".to_vec()));
    let fourth = rig.call(&provider, RELAY, b"fourth");
    gate.entered().await;
    gate.release(b"fourth");
    assert_eq!(fourth.await.unwrap(), Ok(b"held:fourth".to_vec()));
    gate.release(b"second");
    assert_eq!(second.await.unwrap(), Ok(b"held:second".to_vec()));
    drop(service);
    rig.finish(vec![provider]).await;
}

/// Pins what a guest built on the MDK sees today: `clay_mdk` overrides wasi-libc's per-task TLS
/// hook so every component-model task shares one thread-local block, so a thread-local written
/// by a handler running in the `serve` task IS what a sync export entered while that handler is
/// suspended reads. A guest without that hook gets a separate block per task (the inbox spike,
/// which linked stock wasi-libc v34, measured `thread-local=0` here), so ADR 024 §7a still tells
/// authors to share state between handlers and presentation through a `static` or the MDK. The
/// fixture's `mount` renders both counts. Revisit this when the toolchain's TLS handling
/// changes.
pub async fn mdk_guest_shares_thread_locals_between_serve_and_sync_exports<E: Incarnations>(
    engine: E,
) {
    let rig = Rig::new(engine);
    let (service, gate) = rig.gate();
    let provider = rig.provider("provider", Some(service.publication())).await;
    let call = rig.call(&provider, RELAY, b"held");
    gate.entered().await;
    let mounted = E::mount(&provider.runtime, PUBLICATION).expect("a presentation mounts");
    assert!(
        mounted.rendered.contains("static=1 thread-local=1"),
        "{}",
        mounted.rendered
    );
    drop(mounted);
    gate.release(b"held");
    assert_eq!(call.await.unwrap(), Ok(b"held:held".to_vec()));
    drop(service);
    rig.finish(vec![provider]).await;
}

/// A guest that returns a publication its manifest does not declare, or declares as another
/// kind or under another contract, fails its initialization (ADR 025 §8).
pub async fn undeclared_publications_fail_initialization<E: Incarnations>(engine: E) {
    let engine = Rc::new(engine);
    let declared = declared_publications();
    let mut wrong_kind = declared.clone();
    wrong_kind.get_mut(ECHO).unwrap().kind = PublicationKind::Presentation;
    let mut wrong_contract = declared.clone();
    wrong_contract.get_mut(ECHO).unwrap().contract =
        ContractIdentity::new("dayframe.lifecycle", ECHO, (2, 0, 0));
    let mut undeclared = declared;
    undeclared.remove(ECHO);
    for (publications, refusal) in [
        (
            undeclared,
            "guest published \"echo\", which its manifest does not declare",
        ),
        (
            wrong_kind,
            "guest published \"echo\" as Call, but its manifest declares Presentation",
        ),
        (wrong_contract, "but its manifest declares"),
    ] {
        let host = Host::new();
        let instance = host.create("provider").unwrap();
        host.begin_initialization(&instance).unwrap();
        let mut arguments = Vec::new();
        ciborium::into_writer("provider", &mut arguments).unwrap();
        let result = Rc::clone(&engine)
            .initialize(
                host.clone(),
                instance.clone(),
                InitialContext {
                    arguments,
                    publications,
                    ..InitialContext::default()
                },
            )
            .await;
        let error = result
            .err()
            .expect("an undeclared publication fails initialization");
        assert!(error.contains(refusal), "{error}");
        host.close_instance(&instance).unwrap();
        host.close();
    }
}

/// A lease alone retains a lazy provider; dropping the last one lets the supervisor's idle
/// driver close it, which revokes the publication the lease handed out.
pub async fn leased_provider_closes_when_idle<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor("provider");
    let provider_id = MicroId::new("provider");
    tokio::select! {
        never = supervisor.drive() => never,
        () = async {
            let lease = supervisor.acquire(&provider_id, PUBLICATION).await.unwrap();
            assert_eq!(harness.log(), ["initialize provider"]);
            let instance = supervisor.instance(&provider_id).unwrap();
            let observed = harness.observe(&lease);
            assert!(observed.redeem_presentation().await.is_ok());
            drop(lease);
            while supervisor.state(&provider_id) != MicroState::Idle {
                tokio::task::yield_now().await;
            }
            assert_eq!(harness.log(), ["initialize provider", "close provider"]);
            assert_eq!(harness.host.lifecycle(&instance), Lifecycle::Closed);
            assert_eq!(
                observed.redeem_presentation().await.err(),
                Some(CapabilityFailure::Revoked)
            );
        } => {}
    }
    harness.finish(supervisor).await;
}

/// A provider that publishes and then rejects its initialization publishes nothing: the
/// consumer is never initialized and records the unavailable requirement.
pub async fn failing_provider_publishes_nothing<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor("failing-provider");
    let (provider_id, consumer_id) = (MicroId::new("provider"), MicroId::new("consumer"));
    let refusal = "guest refused initialization: rejected: failing provider rejects its \
                   initialization";
    assert_eq!(
        supervisor.reconcile().await,
        BTreeMap::from([(
            consumer_id.clone(),
            StartError::Unavailable {
                binding: "presentation".to_owned(),
                cause: Box::new(StartError::Faulted(refusal.to_owned())),
            }
        )])
    );
    assert_eq!(harness.log(), ["initialize provider"]);
    assert_eq!(
        supervisor.state(&provider_id),
        MicroState::Faulted(refusal.to_owned())
    );
    assert_eq!(
        supervisor.state(&consumer_id),
        MicroState::Blocked(vec!["presentation".to_owned()])
    );
    assert_eq!(supervisor.instance(&provider_id), None);
    assert_eq!(supervisor.instance(&consumer_id), None);
    harness.finish(supervisor).await;
}

pub async fn registration_scope_ownership<E: Incarnations>(engine: E) {
    let engine = Rc::new(engine);
    for initial_state in [Lifecycle::Initializing, Lifecycle::Active] {
        let host = Host::new();
        let registrations = bytes_registrations(std::num::NonZeroUsize::new(64).unwrap(), [81, 82]);
        let mut observer = registrations
            .projection()
            .acquire(Vec::new(), None)
            .unwrap();
        let bootstrap = observer.read(None, ReadLimits::default()).await.unwrap();
        assert!(bootstrap.bootstrap_complete);
        assert!(bootstrap.transactions.is_empty());
        let mut through = bootstrap.through;
        let mut incarnations = Vec::new();
        let mut owned_rows = Vec::new();
        for name in ["first", "other"] {
            let instance = host.create(name).unwrap();
            host.begin_initialization(&instance).unwrap();
            if initial_state == Lifecycle::Active {
                host.commit_initialization(&instance).unwrap();
            }
            let before = registrations.snapshot();
            let (runtime, driver) = Rc::clone(&engine)
                .initialize(
                    host.clone(),
                    instance.clone(),
                    registration_context("registrations", &registrations),
                )
                .await
                .unwrap();
            assert_eq!(E::inspect(&runtime), GuestResidue::NONE);
            if initial_state == Lifecycle::Initializing {
                assert_eq!(registrations.snapshot(), before);
                let staged = observer
                    .read(Some(through.clone()), ReadLimits::default())
                    .complete_timeout()
                    .unwrap();
                assert!(staged.transactions.is_empty());
                assert_eq!(staged.through, through);
                host.commit_initialization(&instance).unwrap();
            }
            let rows = registrations
                .snapshot()
                .into_iter()
                .filter(|(key, _)| !before.contains_key(key))
                .collect::<BTreeMap<_, _>>();
            let mut values = rows
                .values()
                .map(|entry| {
                    assert!(entry.value.capabilities.is_empty());
                    ciborium::from_reader::<String, _>(entry.value.body.as_slice()).unwrap()
                })
                .collect::<Vec<_>>();
            values.sort();
            assert_eq!(values, ["a-x", "a-y", "b-x"]);
            let published = observer
                .read(Some(through), ReadLimits::default())
                .await
                .unwrap();
            if initial_state == Lifecycle::Initializing {
                assert_eq!(published.transactions.len(), 1);
                assert_eq!(published.transactions[0].changes.len(), 3);
                assert!(published.transactions[0].changes.iter().all(|change| {
                    matches!(change, Change::Upsert { key, value, row_version, .. }
                        if rows.get(key).is_some_and(|entry| entry.value == *value && entry.version == *row_version))
                }));
            } else {
                assert_eq!(
                    published
                        .transactions
                        .iter()
                        .map(|transaction| transaction.changes.len())
                        .collect::<Vec<_>>(),
                    [1, 1, 1, 1, 1, 2, 1, 1]
                );
                let cleared = &published.transactions[5].changes;
                let cleared_keys = cleared
                    .iter()
                    .map(|change| {
                        assert!(matches!(change, Change::Remove { .. }));
                        change.key().clone()
                    })
                    .collect::<BTreeSet<_>>();
                let first_scope = rows
                    .iter()
                    .filter_map(|(key, entry)| {
                        let value: String =
                            ciborium::from_reader(entry.value.body.as_slice()).unwrap();
                        value.starts_with("a-").then_some(key.clone())
                    })
                    .collect::<BTreeSet<_>>();
                assert_eq!(cleared_keys, first_scope);
            }
            through = published.through;
            incarnations.push((instance, runtime, driver));
            owned_rows.push(rows);
        }
        let [
            (first, runtime, driver),
            (other, other_runtime, other_driver),
        ]: [_; 2] = incarnations.try_into().ok().unwrap();
        let [first_rows, other_rows]: [_; 2] = owned_rows.try_into().unwrap();
        assert_eq!(registrations.snapshot().len(), 6);
        assert!(host.begin_close(&first).unwrap());
        assert_eq!(registrations.snapshot().len(), 6);
        let closing = observer
            .read(Some(through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(closing.transactions.is_empty());
        let (closed, residue) = E::close(runtime, CloseMode::Orderly).await;
        assert_eq!(closed.unwrap(), GuestClose::Drained);
        assert_eq!(residue, GuestResidue::NONE);
        drop(driver);
        assert_eq!(registrations.snapshot().len(), 6);
        host.close_instance(&first).unwrap();
        let removed = observer
            .read(
                Some(through),
                ReadLimits {
                    max_transactions: Some(1),
                    max_changes: Some(1),
                },
            )
            .await
            .unwrap();
        assert_eq!(removed.transactions.len(), 1);
        assert_eq!(removed.transactions[0].changes.len(), first_rows.len());
        let removed_keys = removed.transactions[0]
            .changes
            .iter()
            .map(|change| {
                let Change::Remove { key, row_version } = change else {
                    panic!("incarnation closure published an upsert");
                };
                assert!(*row_version > first_rows[key].version);
                key.clone()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(removed_keys, first_rows.into_keys().collect());
        assert_eq!(registrations.snapshot(), other_rows);
        let quiet = observer
            .read(Some(removed.through.clone()), ReadLimits::default())
            .complete_timeout()
            .unwrap();
        assert!(quiet.transactions.is_empty());
        drop((other_runtime, other_driver));
        assert_eq!(registrations.snapshot(), other_rows);
        host.close_instance(&other).unwrap();
        let removed = observer
            .read(Some(removed.through), ReadLimits::default())
            .await
            .unwrap();
        assert_eq!(removed.transactions.len(), 1);
        assert_eq!(removed.transactions[0].changes.len(), other_rows.len());
        assert!(registrations.snapshot().is_empty());
        host.close();
    }
}

pub async fn failed_registration_initialization_publishes_nothing<E: Incarnations>(engine: E) {
    let host = Host::new();
    let registrations = bytes_registrations(std::num::NonZeroUsize::new(16).unwrap(), [83, 84]);
    let mut observer = registrations
        .projection()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = observer.read(None, ReadLimits::default()).await.unwrap();
    let instance = host.create("failing-registrations").unwrap();
    host.begin_initialization(&instance).unwrap();
    let result = Rc::new(engine)
        .initialize(
            host.clone(),
            instance.clone(),
            registration_context("failing-registrations", &registrations),
        )
        .await;
    assert_eq!(
        result.err().unwrap(),
        "guest refused initialization: rejected: registration fixture rejects its initialization"
    );
    host.close_instance(&instance).unwrap();
    assert!(registrations.snapshot().is_empty());
    let unseen = observer
        .read(Some(bootstrap.through), ReadLimits::default())
        .complete_timeout()
        .unwrap();
    assert!(unseen.transactions.is_empty());
    host.close();
}

fn registration_context(role: &str, registrations: &BytesRegistrations) -> InitialContext {
    let mut arguments = Vec::new();
    ciborium::into_writer(role, &mut arguments).unwrap();
    InitialContext {
        arguments,
        registrations: BTreeMap::from([
            ("a".to_owned(), registrations.clone()),
            ("b".to_owned(), registrations.clone()),
        ]),
        ..InitialContext::default()
    }
}

struct Harness<E> {
    engine: Rc<E>,
    host: Host,
    shared: Rc<Shared>,
}

/// State every fixture factory and root of one scenario writes to.
struct Shared {
    log: RefCell<Vec<String>>,
    drivers: RefCell<Vec<tokio::task::JoinHandle<()>>>,
    /// A live incarnation outside the graph, standing in for every other recipient.
    observer: CapabilitySession,
    observer_instance: Instance,
    /// Each live incarnation's runtime by identity, shared with its root, so a scenario can
    /// reach the guest the supervisor started (for example to mount its presentation).
    runtimes: RefCell<BTreeMap<&'static str, Rc<dyn Any>>>,
}

impl Shared {
    fn runtime(&self, identity: &str) -> Rc<dyn Any> {
        Rc::clone(&self.runtimes.borrow()[identity])
    }
}

impl<E: Incarnations> Harness<E> {
    fn new(engine: E) -> Self {
        let host = Host::new();
        let observer_instance = host.start("observer");
        let observer = CapabilitySession::new(
            host.clone(),
            observer_instance.clone(),
            CapabilityLimits::default(),
        );
        Self {
            engine: Rc::new(engine),
            host,
            shared: Rc::new(Shared {
                log: RefCell::default(),
                drivers: RefCell::default(),
                observer,
                observer_instance,
                runtimes: RefCell::default(),
            }),
        }
    }

    /// The configured graph: an eager `consumer` that requires the `presentation` and `echo`
    /// publications (bound as `presentation` and `service`) of a lazy `provider` playing
    /// `provider_role`.
    fn supervisor(&self, provider_role: &'static str) -> Supervisor<CapabilityPublication> {
        let provider = ConfiguredMicro {
            identity: MicroId::new("provider"),
            eager: false,
            requirements: BTreeMap::new(),
            factory: self.factory("provider", provider_role),
        };
        let consumer = ConfiguredMicro {
            identity: MicroId::new("consumer"),
            eager: true,
            requirements: BTreeMap::from([
                (
                    "presentation".to_owned(),
                    Requirement {
                        provider: MicroId::new("provider"),
                        publication: PUBLICATION.to_owned(),
                    },
                ),
                (
                    "service".to_owned(),
                    Requirement {
                        provider: MicroId::new("provider"),
                        publication: ECHO.to_owned(),
                    },
                ),
            ]),
            factory: self.factory("consumer", "consumer"),
        };
        Supervisor::new(self.host.clone(), [provider, consumer])
    }

    fn factory(
        &self,
        identity: &'static str,
        role: &'static str,
    ) -> Rc<dyn Factory<CapabilityPublication>> {
        Rc::new(Fixture {
            engine: Rc::clone(&self.engine),
            shared: Rc::clone(&self.shared),
            identity,
            role,
        })
    }

    fn log(&self) -> Vec<String> {
        self.shared.log.borrow().clone()
    }

    fn observe(&self, publication: &CapabilityPublication) -> CapabilityRef {
        self.shared.observer.mint(publication).unwrap()
    }

    /// Shut the graph down, close the host, and join every incarnation driver.
    async fn finish(self, supervisor: Supervisor<CapabilityPublication>) {
        supervisor.shutdown().await.unwrap();
        drop(supervisor);
        self.host
            .close_instance(&self.shared.observer_instance)
            .unwrap();
        self.host.close();
        let drivers = std::mem::take(&mut *self.shared.drivers.borrow_mut());
        for driver in drivers {
            driver.await.unwrap();
        }
    }
}

struct Fixture<E> {
    engine: Rc<E>,
    shared: Rc<Shared>,
    identity: &'static str,
    role: &'static str,
}

impl<E: Incarnations> Factory<CapabilityPublication> for Fixture<E> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        let engine = Rc::clone(&self.engine);
        let shared = Rc::clone(&self.shared);
        let identity = self.identity;
        let role = self.role;
        Box::pin(async move {
            shared
                .log
                .borrow_mut()
                .push(format!("initialize {identity}"));
            let mut arguments = Vec::new();
            ciborium::into_writer(role, &mut arguments).unwrap();
            let context = InitialContext {
                arguments,
                requirements,
                publications: declared_publications(),
                ..InitialContext::default()
            };
            let (runtime, driver) = engine
                .initialize(host.clone(), instance.clone(), context)
                .await?;
            shared
                .drivers
                .borrow_mut()
                .push(tokio::task::spawn_local(driver));
            assert_eq!(host.lifecycle(&instance), Lifecycle::Initializing);
            let publications = E::publications(&runtime);
            for (name, publication) in &publications {
                // Staged until the supervisor commits this initialization (ADR 024 §5).
                let staged = shared.observer.mint(publication).unwrap();
                let outcome = match publication.kind() {
                    PublicationKind::Presentation => staged.redeem_presentation().await.err(),
                    PublicationKind::Call => match staged.call(Vec::new()).await {
                        Err(CallError::Capability(failure)) => Some(failure),
                        other => panic!("staged call {name} answered {other:?}"),
                    },
                };
                assert_eq!(
                    outcome,
                    Some(CapabilityFailure::Revoked),
                    "{identity}'s {name} is visible before its initialization committed"
                );
            }
            let runtime = Rc::new(runtime);
            shared
                .runtimes
                .borrow_mut()
                .insert(identity, Rc::clone(&runtime) as Rc<dyn Any>);
            let root = Box::new(FixtureRoot::<E> {
                runtime,
                shared,
                identity,
            });
            Ok(Initialized { root, publications })
        })
    }
}

struct FixtureRoot<E: Incarnations> {
    runtime: Rc<E::Runtime>,
    shared: Rc<Shared>,
    identity: &'static str,
}

impl<E: Incarnations> Root for FixtureRoot<E> {
    fn close(self: Box<Self>, mode: CloseMode) -> LocalFuture<'static, Result<(), String>> {
        let Self {
            runtime,
            shared,
            identity,
        } = *self;
        Box::pin(async move {
            shared.runtimes.borrow_mut().remove(identity);
            let runtime = Rc::try_unwrap(runtime)
                .unwrap_or_else(|_| panic!("{identity}'s runtime is still shared at its close"));
            let (closed, residue) = E::close(runtime, mode).await;
            assert_eq!(
                residue,
                GuestResidue::NONE,
                "{identity} still held guest resources after its close"
            );
            shared.log.borrow_mut().push(format!("close {identity}"));
            assert_eq!(
                closed?,
                GuestClose::Drained,
                "{identity} had no call in flight"
            );
            Ok(())
        })
    }
}
