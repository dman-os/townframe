//! Engine conformance for the ADR 030 activation boundary, run against the shared `context`
//! micro (`src/clay_mdk/context_micro.rs`).
//!
//! Each engine crate implements [`Incarnations`] over its runtime and runs these scenarios from
//! its own tests, so every engine is held to one chain: configured requirement → strong binding
//! (or external lease) → provider initialization → named publication visible only once committed
//! → close with dependencies live → publications revoked → reverse-order teardown that leaves
//! the incarnation holding nothing. The micro is written against `clay_mdk::Context`, so the
//! native engine runs its source in-process and the guest engines run it as a component: one
//! micro, one scenario list, three engines. The micro takes its role from what the scenario
//! grants it (requirements, `feed` rows; see its docs), never from arguments, and it has no
//! close hook: what a scenario learns about closing it learns from values the incarnation owns
//! being dropped (ADR 030 §6).
//!
//! The concurrency scenarios (`overlapping_*`, `many_*`, `calls_reenter_*`, `disable_*`,
//! `shutdown_*`) hold an engine to ordinary per-call async exports: many calls into one
//! incarnation in flight at once, each call a task of its own, with nothing serializing them.
//! Three scenarios (`guest_trap_*`, `sync_exports_*`, `dropped_call_*`) are properties of a guest
//! engine's store (a trap, synchronous WIT exports, the store's residue), run over a raw-WIT
//! component with the same wire (`src/clay_mdk/lifecycle_fixture.rs`), and require
//! [`GuestIncarnations`].
//!
//! Scenarios must run inside a [`tokio::task::LocalSet`]: incarnation drivers are spawned with
//! [`tokio::task::spawn_local`] and joined (panics propagate) once the host closes.
use std::any::Any;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use clay_host::Activator;
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

use crate::InitialContext;
use crate::capabilities::CallError;
use crate::capabilities::CallTarget;
use crate::capabilities::CapabilityFailure;
use crate::capabilities::CapabilityLimits;
use crate::capabilities::CapabilityPublication;
use crate::capabilities::CapabilityRef;
use crate::capabilities::CapabilitySession;
use crate::capabilities::DeclaredPublication;
use crate::capabilities::InFlightCalls;
use crate::capabilities::PublicationEndpoint;
use crate::capabilities::PublicationKind;
use crate::capabilities::PublishedCapability;
use crate::capabilities::RefStatus;
use crate::capabilities::RowAttachment;
use crate::capabilities::presentation_component;
use crate::collections::BytesRegistrations;
use crate::collections::CollectionSession;
use crate::collections::bytes_registrations;
use clay_collection::Nsid;
use clay_collection::ReadLimits;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use tokio::task::JoinHandle;

mod calls;
mod context;
mod registrations;
pub use calls::calls_reenter_their_provider;
pub use calls::disable_with_calls_in_flight_revokes_callers;
pub use calls::dropped_call_finishes_in_the_guest_without_leaks;
pub use calls::guest_trap_fails_outstanding_calls;
pub use calls::many_overlapping_calls_complete;
pub use calls::overlapping_calls_complete_out_of_order;
pub use calls::shutdown_with_calls_in_flight_revokes_callers;
pub use calls::sync_exports_interleave_with_suspended_calls;
use context::ContextWorld;
use context::cbor_text;
pub use context::context_collections_and_calls;
pub use context::context_follows_a_provider;
pub use context::context_peers_demanding_each_other_fail_as_unavailable;
pub use context::context_peers_retaining_each_other_close_together;
pub use context::context_retains_a_call;
pub use context::context_sees_a_closed_provider_revoked;
use context::seeder;
pub use registrations::failed_registration_initialization_publishes_nothing;
pub use registrations::registration_scope_ownership;

/// The micro's names: a presentation, and an `echo` call that answers its request.
const PUBLICATION: &str = "detail";
const ECHO: &str = "echo";
/// The micro's `relay` call forwards its request to its `peer` requirement and answers `relay:`
/// plus the peer's answer.
const RELAY: &str = "relay";

/// The writer identity `micro` has on the one replica the conformance scenarios run on.
fn writer_of(micro: &str) -> clay_collection::WriterId {
    clay_collection::WriterId::new(
        &clay_collection::ReplicaId::from_bytes([1; clay_collection::ReplicaId::LEN]),
        micro,
    )
}

fn echo_contract() -> Nsid {
    Nsid::literal(context::ECHO_CONTRACT)
}

fn relay_contract() -> Nsid {
    Nsid::literal(context::RELAY_CONTRACT)
}

/// What an incarnation's store holds. After its guest half closed, anything but
/// [`GuestResidue::NONE`] is a resource the close path leaked to the store's drop, unless
/// `calls` is nonzero: a forced close abandons the handlers still in flight, and their guest
/// state dies with the store.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestResidue {
    pub table_empty: bool,
    pub publications: usize,
    pub references: usize,
    pub proxies: usize,
    pub readers: usize,
    pub writers: usize,
    /// Guest `call` handlers that have not returned yet, including those whose caller gave up.
    pub calls: usize,
}

impl GuestResidue {
    pub const NONE: Self = Self {
        table_empty: true,
        publications: 0,
        references: 0,
        proxies: 0,
        readers: 0,
        writers: 0,
        calls: 0,
    };

    /// The resources the guest holds through its sessions; `calls` is the engine's to fill in
    /// (`..GuestResidue::of(..)`), and is zero for an engine that runs one call at a time.
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
            calls: 0,
        }
    }
}

/// One engine's incarnations of the `context` micro: the guest engines run its component, a
/// native engine runs the same source in-process.
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

    /// What the incarnation currently holds through its sessions.
    fn inspect(runtime: &Self::Runtime) -> GuestResidue;

    /// The incarnation's live call handlers, awaitable until they all ended.
    fn calls_in_flight(runtime: &Self::Runtime) -> InFlightCalls;

    /// Close the guest half and report what the store still held afterwards, before it drops.
    fn close(runtime: Self::Runtime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)>;
}

/// An engine that hosts the raw guest fixture, the `lifecycle` component: the same wire as the
/// micro's `echo` and `relay` over raw WIT, plus what only a guest store has (a call that traps,
/// synchronous WIT exports, a resource table to leave residue in). Native micros author against
/// the MDK's `Context`, which has none of these, so only the guest engines implement it and the
/// scenarios over the raw fixture require it; every other scenario needs only [`Incarnations`].
pub trait GuestIncarnations: Incarnations {
    /// Mount the presentation the runtime published as `publication` into an engine-owned
    /// sink and check that the guest rendered into it. Dropping the result unmounts.
    fn mount(runtime: &Self::Runtime, publication: &str) -> Result<Box<dyn Any>, String>;
}

/// The consumer requires `provider`'s presentation and redeems it while initializing, and keeps
/// the proxy in a value its incarnation owns, whose `Drop` redeems it once more as the
/// incarnation closes. An external lease on the same publication is a second strong edge.
/// Disabling the provider must close the consumer first, with the provider still live (the
/// consumer's drop crashes the close otherwise), and revoke the lease's publication.
pub async fn provider_consumer_graph<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor(false);
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
    // The consumer's own initialization already called the provider's `echo` from its
    // incarnation into the provider's (it refuses to activate unless the provider answered);
    // a host-side recipient does the same through the same publication.
    let echo = supervisor
        .acquire(&provider_id, ECHO)
        .await
        .expect("the committed provider publishes its echo call");
    assert_eq!(
        harness.observe(&echo).call(cbor_text("ping")).await,
        Ok(cbor_text("ping"))
    );
    drop(echo);

    // Closing the provider closes its dependent first, while the provider is still live: the
    // consumer's owned proxy is dropped in that close and a drop that found the provider
    // withdrawn would crash it.
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

/// ADR 031 §§6 and 9 against a lazy reference to the provider: `status` reports `Dormant`
/// without starting anything; a `call` activates the provider, answers, and lets it idle-close
/// once the call's lease drops; a `retain-call` binding keeps it up until dropped; a redeemed
/// presentation keeps it up while held and lets it close when dropped. (Mounting a redeemed
/// presentation is the renderer's: `sync_exports_*` mounts one on the guest engines.)
pub async fn lazy_references_activate_retain_and_release<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let supervisor = harness.supervisor(false);
    let provider_id = MicroId::new("provider");
    let lazy = |publication: &str, contract: Nsid| {
        harness.lazy_ref(&supervisor, provider_id.as_str(), publication, contract)
    };
    let echo = lazy(ECHO, echo_contract());
    let view = lazy(PUBLICATION, presentation_component());
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
        settle(&supervisor, || {
            supervisor.state(&provider_id) == MicroState::Idle
        })
        .await;
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
            assert_eq!(echo.call(cbor_text("one")).await, Ok(cbor_text("one")));
            idle(closed(1)).await;
            assert_eq!(echo.status(), RefStatus::Dormant);

            // A retained binding keeps the provider up for as long as it is held.
            let binding = echo.retain_call().await.unwrap();
            assert_eq!(binding.call(cbor_text("two")).await, Ok(cbor_text("two")));
            assert_eq!(echo.status(), RefStatus::Live);
            stays_up(2).await;
            assert_eq!(binding.call(cbor_text("three")).await, Ok(cbor_text("three")));
            drop(binding);
            idle(closed(2)).await;

            // A redeemed presentation retains it too, for as long as it is held.
            let component = view.redeem_presentation().await.unwrap();
            assert!(component.component().is_ok());
            stays_up(3).await;
            drop(component);
            idle(closed(3)).await;
            assert_eq!(view.status(), RefStatus::Dormant);
            assert_eq!(echo.call(cbor_text("after")).await, Ok(cbor_text("after")));
            idle(closed(4)).await;
        } => {}
    }
    drop((echo, view));
    harness.finish(supervisor).await;
}

/// A micro whose package declares fewer publications than it returns, or declares one as another
/// kind or under another contract, fails its initialization (ADR 031 §8). The mismatch is the
/// host's declaration (the package's sidecar) disagreeing with the component.
pub async fn undeclared_publications_fail_initialization<E: Incarnations>(engine: E) {
    let engine = Rc::new(engine);
    let declared = ContextWorld::declared();
    let mut wrong_kind = declared.clone();
    wrong_kind.get_mut(ECHO).unwrap().kind = PublicationKind::Presentation;
    let mut wrong_contract = declared.clone();
    wrong_contract.get_mut(ECHO).unwrap().contract =
        Nsid::literal("org.example.clay.test.context.echoV2");
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
        let registrations = bytes_registrations(std::num::NonZeroUsize::new(16).unwrap(), [85, 86]);
        // No micro is configured, so nothing can be activated through the demand handle.
        let supervisor = Supervisor::<CapabilityPublication>::new(host.clone(), []);
        let result = Rc::clone(&engine)
            .initialize(
                host.clone(),
                instance.clone(),
                InitialContext {
                    publications,
                    ..ContextWorld::granted("provider", &registrations, supervisor.activator())
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
    let supervisor = harness.supervisor(false);
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
            settle(&supervisor, || {
                supervisor.state(&provider_id) == MicroState::Idle
            })
            .await;
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
    let supervisor = harness.supervisor(true);
    let (provider_id, consumer_id) = (MicroId::new("provider"), MicroId::new("consumer"));
    // Each engine words its refusal its own way; the micro's reason is in all of them.
    let reason = "failing provider rejects its initialization";
    let failures = supervisor.reconcile().await;
    assert_eq!(failures.keys().collect::<Vec<_>>(), [&consumer_id]);
    let StartError::Unavailable { binding, cause } = &failures[&consumer_id] else {
        panic!(
            "the consumer is unavailable, not {:?}",
            failures[&consumer_id]
        );
    };
    assert_eq!(binding, "presentation");
    let StartError::Faulted(refusal) = &**cause else {
        panic!("the provider faulted, not {cause:?}");
    };
    assert!(refusal.contains(reason), "{refusal}");
    assert_eq!(harness.log(), ["initialize provider"]);
    assert_eq!(
        supervisor.state(&provider_id),
        MicroState::Faulted(refusal.clone())
    );
    assert_eq!(
        supervisor.state(&consumer_id),
        MicroState::Blocked(vec!["presentation".to_owned()])
    );
    assert_eq!(supervisor.instance(&provider_id), None);
    assert_eq!(supervisor.instance(&consumer_id), None);
    harness.finish(supervisor).await;
}

/// Wait until `condition` holds over the supervisor's recorded micro states, woken by its
/// change ticks (sent after the mirror `Supervisor::state` reads was updated). The receiver is
/// taken before the first check, so a change between the two cannot be missed.
async fn settle(
    supervisor: &Supervisor<CapabilityPublication>,
    mut condition: impl FnMut() -> bool,
) {
    supervisor
        .changes()
        .wait_for(|_| condition())
        .await
        .expect("the supervisor outlives the wait");
}

struct Harness<E> {
    engine: Rc<E>,
    host: Host,
    shared: Rc<Shared>,
}

/// State every factory and root of one scenario writes to.
struct Shared {
    log: RefCell<Vec<String>>,
    drivers: RefCell<Vec<tokio::task::JoinHandle<()>>>,
    /// A live incarnation outside the graph, standing in for every other recipient.
    observer: CapabilitySession,
    observer_instance: Instance,
    /// Each live incarnation's runtime by identity, shared with its root, so a scenario can
    /// reach the incarnation the supervisor started.
    runtimes: RefCell<BTreeMap<&'static str, Rc<dyn Any>>>,
    /// The host's writers of the `reject` rows seeded into incarnations' feeds, kept so the
    /// rows stay for as long as the scenario runs.
    seeders: RefCell<Vec<crate::collections::RegistrationWriter>>,
    /// The runtime epoch the next incarnation's `feed` collection gets.
    epoch: Cell<u64>,
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
                seeders: RefCell::default(),
                epoch: Cell::new(1000),
            }),
        }
    }

    /// The configured graph: an eager `consumer` that requires the `detail` presentation and
    /// `echo` call (bound as `presentation` and `service`) of a lazy `provider`, which fails
    /// its own initialization if `provider_rejects`.
    fn supervisor(&self, provider_rejects: bool) -> Supervisor<CapabilityPublication> {
        let provider = ConfiguredMicro {
            identity: MicroId::new("provider"),
            eager: false,
            enabled: true,
            requirements: BTreeMap::new(),
            unresolved_bindings: Vec::new(),
            factory: self.factory("provider", None, provider_rejects),
        };
        let consumer = ConfiguredMicro {
            identity: MicroId::new("consumer"),
            eager: true,
            enabled: true,
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
            unresolved_bindings: Vec::new(),
            factory: self.factory("consumer", None, false),
        };
        Supervisor::new(self.host.clone(), [provider, consumer])
    }

    fn factory(
        &self,
        identity: &'static str,
        peer: Option<CapabilityPublication>,
        rejects: bool,
    ) -> Rc<dyn Factory<CapabilityPublication>> {
        Rc::new(Fixture {
            engine: Rc::clone(&self.engine),
            host: self.host.clone(),
            shared: Rc::clone(&self.shared),
            identity,
            peer,
            rejects,
        })
    }

    /// A graph of lazy micros without configured requirements, one per `(identity, peer)`.
    /// Each one's `relay` forwards to its `peer`.
    fn relays(
        &self,
        relays: impl IntoIterator<Item = (&'static str, CapabilityPublication)>,
    ) -> Supervisor<CapabilityPublication> {
        Supervisor::new(
            self.host.clone(),
            relays.into_iter().map(|(identity, peer)| ConfiguredMicro {
                identity: MicroId::new(identity),
                eager: false,
                enabled: true,
                requirements: BTreeMap::new(),
                unresolved_bindings: Vec::new(),
                factory: self.factory(identity, Some(peer), false),
            }),
        )
    }

    /// A lazy reference, held by the outside observer, to `micro`'s `publication`.
    fn lazy_ref(
        &self,
        supervisor: &Supervisor<CapabilityPublication>,
        micro: &str,
        publication: &str,
        contract: Nsid,
    ) -> CapabilityRef {
        // The observer's instance, with this scenario's supervisor behind its lazy references.
        let session = CapabilitySession::new(
            self.host.clone(),
            self.shared.observer_instance.clone(),
            CapabilityLimits::default(),
        );
        session.attach_activator(supervisor.activator());
        session
            .mint_lazy(&PublicationEndpoint {
                micro: MicroId::new(micro),
                publication: publication.to_owned(),
                contract,
            })
            .unwrap()
    }

    /// Publish a host-side call as the outside observer, to hand a micro as its `peer`.
    fn publish_peer<T: CallTarget + 'static>(&self, target: &Arc<T>) -> PublishedCapability {
        let target: Arc<dyn CallTarget> = Arc::clone(target) as _;
        self.shared
            .observer
            .publish_call(
                "peer",
                Nsid::literal("org.example.clay.test.context.peer"),
                target,
            )
            .unwrap()
    }

    /// What the live incarnation `identity` holds right now.
    fn residue(&self, identity: &str) -> GuestResidue {
        let runtime = self
            .shared
            .runtime(identity)
            .downcast::<E::Runtime>()
            .unwrap_or_else(|_| panic!("{identity} runs this engine"));
        E::inspect(&runtime)
    }

    /// Resolves once every call handler in the live incarnation `identity` has returned.
    async fn calls_drained(&self, identity: &str) {
        let runtime = self
            .shared
            .runtime(identity)
            .downcast::<E::Runtime>()
            .unwrap_or_else(|_| panic!("{identity} runs this engine"));
        let calls = E::calls_in_flight(&runtime);
        drop(runtime);
        calls.drained().await;
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

/// The factory of one micro of a scenario: each incarnation is the `context` micro on `E`, with a
/// `feed` collection of its own.
struct Fixture<E> {
    engine: Rc<E>,
    host: Host,
    shared: Rc<Shared>,
    identity: &'static str,
    /// Granted as the micro's `peer` requirement, which its `relay` forwards to.
    peer: Option<CapabilityPublication>,
    /// Seeds the `reject` row, which makes the micro fail its activation after publishing.
    rejects: bool,
}

impl<E: Incarnations> Factory<CapabilityPublication> for Fixture<E> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, CapabilityPublication>,
        activator: Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        let engine = Rc::clone(&self.engine);
        let shared = Rc::clone(&self.shared);
        let identity = self.identity;
        let peer = self.peer.clone();
        let rejects = self.rejects;
        let seeding = self.host.clone();
        Box::pin(async move {
            shared
                .log
                .borrow_mut()
                .push(format!("initialize {identity}"));
            let epoch = shared.epoch.get();
            shared.epoch.set(epoch + 2);
            let registrations =
                bytes_registrations(std::num::NonZeroUsize::new(16).unwrap(), [epoch, epoch + 1]);
            if rejects {
                let seeder = seeder(&seeding, &format!("seeder-{identity}"), &registrations);
                seeder
                    .put_value(
                        cbor_text("reject"),
                        clay_collection::WriteBasis::Concurrent,
                        crate::collections::CollectionValue {
                            body: cbor_text("reject"),
                            attachments: Vec::new(),
                        },
                    )
                    .unwrap();
                shared.seeders.borrow_mut().push(seeder);
            }
            let mut context = ContextWorld::granted(identity, &registrations, activator);
            context.requirements = requirements;
            context
                .requirements
                .extend(peer.map(|peer| ("peer".to_owned(), peer)));
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
                // Staged until the supervisor commits this initialization (ADR 030 §5).
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
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        let Self {
            runtime,
            shared,
            identity,
        } = *self;
        Box::pin(async move {
            shared.runtimes.borrow_mut().remove(identity);
            let runtime = Rc::try_unwrap(runtime)
                .unwrap_or_else(|_| panic!("{identity}'s runtime is still shared at its close"));
            let (closed, residue) = E::close(runtime).await;
            if residue.calls == 0 {
                assert_eq!(
                    residue,
                    GuestResidue::NONE,
                    "{identity} still held resources after its close"
                );
            }
            shared.log.borrow_mut().push(format!("close {identity}"));
            closed
        })
    }
}
