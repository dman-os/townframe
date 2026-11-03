//! Engine conformance for the ADR 024 activation boundary, run against the prepared
//! `lifecycle` fixture component (`src/clay_mdk/lifecycle_fixture.rs`).
//!
//! Each engine crate implements [`Incarnations`] over its runtime and runs these scenarios from
//! its own tests, so every engine is held to one chain: configured requirement → strong binding
//! (or external lease) → provider initialization → named publication visible only once committed
//! → async `instance-root.close` with dependencies live → publications revoked →
//! reverse-order teardown that leaves the guest holding nothing.
//!
//! The concurrency scenarios (`overlapping_*`, `many_*`, `sync_exports_*`, `calls_reenter_*`,
//! `guest_trap_*`, `close_with_*`, `dropped_call_*`) hold an engine to ordinary per-call async
//! exports: many calls into one incarnation in flight at once, each call a component-model task
//! of its own, with nothing serializing them.
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
use futures::FutureExt as _;
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

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
/// The fixture's `relay` call: it forwards its request to its `peer` requirement and answers
/// `relay:` plus the peer's answer; the request `trap` traps the guest.
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
    /// sink and check that the guest rendered into it. Dropping the result unmounts.
    fn mount(runtime: &Self::Runtime, publication: &str) -> Result<Box<dyn Any>, String>;

    fn inspect(runtime: &Self::Runtime) -> GuestResidue;

    /// Close the guest half and report what the store still held afterwards, before it drops.
    fn close(runtime: Self::Runtime) -> LocalFuture<'static, (Result<(), String>, GuestResidue)>;
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
        harness.lazy_ref(&supervisor, provider_id.as_str(), publication, contract)
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
        let (closed, residue) = E::close(runtime).await;
        closed.unwrap();
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

/// A host-side call the scenario answers by hand: each call parks until [`Gate::release`] names
/// its request, then answers with that request. A guest handler that awaits it is a call
/// suspended on something the test controls.
struct Gate {
    held: Mutex<BTreeMap<Vec<u8>, oneshot::Sender<()>>>,
    arrivals: watch::Sender<usize>,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            held: Mutex::default(),
            arrivals: watch::channel(0).0,
        })
    }

    /// Resolves once `count` calls have reached the gate in total.
    async fn arrived(&self, count: usize) {
        self.arrivals
            .subscribe()
            .wait_for(|&arrivals| arrivals >= count)
            .await
            .unwrap();
    }

    fn release(&self, request: &[u8]) {
        let parked = self
            .held
            .lock()
            .unwrap()
            .remove(request)
            .expect("the released call reached the gate");
        parked
            .send(())
            .expect("the released call is still waiting at the gate");
    }
}

impl CallTarget for Gate {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let (release, released) = oneshot::channel();
        let previous = self.held.lock().unwrap().insert(request.clone(), release);
        assert!(previous.is_none(), "scenario requests are unique");
        self.arrivals.send_modify(|arrivals| *arrivals += 1);
        async move {
            released
                .await
                .map_err(|_dropped| CapabilityFailure::Revoked)?;
            Ok(request)
        }
        .boxed()
    }
}

/// A host-side call that forwards to a reference the scenario points it at after the graph
/// exists (a guest's `peer` must be granted before the micros it reaches are started).
#[derive(Default)]
struct Forward(OnceLock<CapabilityRef>);

impl CallTarget for Forward {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let target = self
            .0
            .get()
            .expect("the scenario points the forward before any call")
            .share();
        async move { target.call(request).await }.boxed()
    }
}

/// Wait for a condition that other tasks bring about.
async fn until(mut condition: impl FnMut() -> bool) {
    while !condition() {
        tokio::task::yield_now().await;
    }
}

/// One lazy relay provider `a`, held live by a lease, whose `peer` is a [`Gate`]: every call of
/// its `relay` parks in the guest until the scenario releases it.
struct Gated<E: Incarnations> {
    harness: Harness<E>,
    supervisor: Supervisor<CapabilityPublication>,
    gate: Arc<Gate>,
    /// The gate as published to the guest; revoked when dropped.
    _peer: PublishedCapability,
    lease: Lease<CapabilityPublication>,
    relay: CapabilityRef,
}

impl<E: Incarnations> Gated<E> {
    async fn start(engine: E) -> Self {
        let harness = Harness::new(engine);
        let gate = Gate::new();
        let peer = harness.publish_peer(&gate);
        let supervisor = harness.relays([("a", peer.publication())]);
        let lease = supervisor
            .acquire(&MicroId::new("a"), RELAY)
            .await
            .expect("the relay provider starts");
        let relay = harness.observe(&lease);
        Self {
            harness,
            supervisor,
            gate,
            _peer: peer,
            lease,
            relay,
        }
    }

    /// Start one `relay` call from outside the incarnation. The returned task is the caller:
    /// aborting it drops the call.
    fn call(&self, request: impl Into<Vec<u8>>) -> JoinHandle<Result<Vec<u8>, CallError>> {
        let relay = self.relay.share();
        let request = request.into();
        tokio::task::spawn_local(async move { relay.call(request).await })
    }

    fn residue(&self) -> GuestResidue {
        self.harness.residue("a")
    }

    fn instance(&self) -> Instance {
        self.supervisor.instance(&MicroId::new("a")).unwrap()
    }

    /// Run `scenario` while the supervisor's driver serves idle closes.
    async fn run(&self, scenario: impl Future<Output = ()>) {
        tokio::select! {
            never = self.supervisor.drive() => never,
            () = scenario => {}
        }
    }

    async fn finish(self) {
        let Self {
            harness,
            supervisor,
            lease,
            relay,
            ..
        } = self;
        drop((relay, lease));
        harness.finish(supervisor).await;
    }
}

/// Two calls into one incarnation are both in flight, suspended in the guest, and complete in
/// the opposite order to the one they started in. Each call is a task of its own: the second
/// does not wait for the first, and neither's state is mistaken for the other's.
pub async fn overlapping_calls_complete_out_of_order<E: Incarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let baseline = g.residue();
        let first = g.call("one");
        let second = g.call("two");
        g.gate.arrived(2).await;
        assert_eq!(g.residue().calls, 2, "both handlers are in the guest");
        g.gate.release(b"two");
        assert_eq!(second.await.unwrap(), Ok(b"relay:two".to_vec()));
        assert!(!first.is_finished(), "the first call is still suspended");
        g.gate.release(b"one");
        assert_eq!(first.await.unwrap(), Ok(b"relay:one".to_vec()));
        until(|| g.residue().calls == 0).await;
        assert_eq!(g.residue(), baseline, "no call left anything behind");
    })
    .await;
    g.finish().await;
}

/// Fifty calls in flight at once, released in an interleaved order, all answered with their own
/// request.
pub async fn many_overlapping_calls_complete<E: Incarnations>(engine: E) {
    const CALLS: usize = 50;
    let g = Gated::start(engine).await;
    g.run(async {
        let baseline = g.residue();
        let calls: Vec<_> = (0..CALLS)
            .map(|index| (index, g.call(format!("call-{index}"))))
            .collect();
        g.gate.arrived(CALLS).await;
        assert_eq!(g.residue().calls, CALLS);
        // Odd calls first, newest first, then the even ones oldest first.
        let order = (0..CALLS)
            .rev()
            .filter(|index| index % 2 == 1)
            .chain((0..CALLS).filter(|index| index % 2 == 0));
        for index in order {
            g.gate.release(format!("call-{index}").as_bytes());
        }
        for (index, call) in calls {
            assert_eq!(
                call.await.unwrap(),
                Ok(format!("relay:call-{index}").into_bytes())
            );
        }
        until(|| g.residue().calls == 0).await;
        assert_eq!(g.residue(), baseline);
    })
    .await;
    g.finish().await;
}

/// Synchronous exports (`mount`, unmount) and an unrelated async export (`echo`) run while a
/// `relay` call is suspended in the guest. `mount` reads the guest's `ROLE`, which `initialize`
/// (a different task, long finished) wrote: it only works because all tasks share one TLS.
pub async fn sync_exports_interleave_with_suspended_calls<E: Incarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let parked = g.call("parked");
        g.gate.arrived(1).await;
        let runtime = g
            .harness
            .shared
            .runtime("a")
            .downcast::<E::Runtime>()
            .unwrap_or_else(|_| panic!("a runs this engine"));
        let mounted = E::mount(&runtime, PUBLICATION).expect("a live presentation mounts");
        let echo = g
            .supervisor
            .acquire(&MicroId::new("a"), ECHO)
            .await
            .unwrap();
        assert_eq!(
            g.harness.observe(&echo).call(b"x".to_vec()).await,
            Ok(b"echo:x".to_vec()),
            "another call completes while the relay is suspended"
        );
        assert!(!parked.is_finished());
        drop((mounted, echo));
        drop(runtime);
        g.gate.release(b"parked");
        assert_eq!(parked.await.unwrap(), Ok(b"relay:parked".to_vec()));
    })
    .await;
    g.finish().await;
}

/// A guest import that reaches back into its own incarnation completes: A calls B which calls
/// A's `echo` while A's `relay` is suspended (A to B to A), and `c` calls its own `echo` (A to
/// A). The host answers an import by queueing into the provider's driver, never by entering the
/// provider's store from the import's first poll. The hops go through lazy references.
pub async fn calls_reenter_their_provider<E: Incarnations>(engine: E) {
    let harness = Harness::new(engine);
    let (a_to_b, b_to_a, c_to_c) = (
        Arc::new(Forward::default()),
        Arc::new(Forward::default()),
        Arc::new(Forward::default()),
    );
    let peers = [&a_to_b, &b_to_a, &c_to_c].map(|forward| harness.publish_peer(forward));
    let supervisor = harness.relays([
        ("a", peers[0].publication()),
        ("b", peers[1].publication()),
        ("c", peers[2].publication()),
    ]);
    let lazy = |micro: &str, publication: &str, contract| {
        harness.lazy_ref(&supervisor, micro, publication, contract)
    };
    assert!(a_to_b.0.set(lazy("b", RELAY, relay_contract())).is_ok());
    assert!(b_to_a.0.set(lazy("a", ECHO, echo_contract())).is_ok());
    assert!(c_to_c.0.set(lazy("c", ECHO, echo_contract())).is_ok());
    let relay_a = lazy("a", RELAY, relay_contract());
    let relay_c = lazy("c", RELAY, relay_contract());
    tokio::select! {
        never = supervisor.drive() => never,
        () = async {
            assert_eq!(
                relay_a.call(b"x".to_vec()).await,
                Ok(b"relay:relay:echo:x".to_vec())
            );
            assert_eq!(
                relay_c.call(b"y".to_vec()).await,
                Ok(b"relay:echo:y".to_vec())
            );
            for micro in ["a", "b", "c"] {
                let micro = MicroId::new(micro);
                until(|| supervisor.state(&micro) == MicroState::Idle).await;
            }
        } => {}
    }
    drop((relay_a, relay_c, a_to_b, b_to_a, c_to_c));
    harness.finish(supervisor).await;
}

/// A guest trap fails every call outstanding on the incarnation instead of leaving them
/// suspended forever: they answer `Revoked`, the incarnation closes, and host teardown runs.
pub async fn guest_trap_fails_outstanding_calls<E: Incarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let instance = g.instance();
        let outstanding: Vec<_> = (0..3)
            .map(|index| g.call(format!("call-{index}")))
            .collect();
        g.gate.arrived(3).await;
        let trapper = g.call("trap").await.unwrap();
        assert!(trapper.is_err(), "the trapping call fails: {trapper:?}");
        for call in outstanding {
            assert_eq!(
                call.await.unwrap(),
                Err(CapabilityFailure::Revoked.into()),
                "a call suspended in a trapped guest is revoked"
            );
        }
        g.harness.host.await_close(&instance).await;
        assert_eq!(g.harness.host.lifecycle(&instance), Lifecycle::Closed);
        assert_eq!(
            g.relay.call(b"after".to_vec()).await,
            Err(CapabilityFailure::Revoked.into())
        );
    })
    .await;
    g.finish().await;
}

/// The guest's async `instance-root.close` runs beside calls that are still in flight, and the
/// forced close then drops the store: the callers answer `Revoked` rather than hang. This one
/// is the supervisor disabling the micro.
pub async fn disable_with_calls_in_flight_revokes_callers<E: Incarnations>(engine: E) {
    close_with_calls_in_flight(engine, false).await;
}

/// [`disable_with_calls_in_flight_revokes_callers`], with the supervisor shutting down.
pub async fn shutdown_with_calls_in_flight_revokes_callers<E: Incarnations>(engine: E) {
    close_with_calls_in_flight(engine, true).await;
}

async fn close_with_calls_in_flight<E: Incarnations>(engine: E, shutdown: bool) {
    let g = Gated::start(engine).await;
    g.run(async {
        let instance = g.instance();
        let outstanding: Vec<_> = (0..3)
            .map(|index| g.call(format!("call-{index}")))
            .collect();
        g.gate.arrived(3).await;
        if shutdown {
            g.supervisor.shutdown().await.unwrap();
        } else {
            g.supervisor.disable(&MicroId::new("a")).await.unwrap();
        }
        assert_eq!(g.harness.log(), ["initialize a", "close a"]);
        assert_eq!(g.harness.host.lifecycle(&instance), Lifecycle::Closed);
        for call in outstanding {
            assert_eq!(
                call.await.unwrap(),
                Err(CapabilityFailure::Revoked.into()),
                "a call in flight at a forced close is revoked"
            );
        }
    })
    .await;
    g.finish().await;
}

/// A caller that gives up on a call cannot cancel the guest handler: it runs to completion and
/// its answer is discarded. Nothing leaks when it does: the guest holds what it did before the
/// call, and the lease that kept the provider live is released normally, after which the
/// provider idles closed with nothing left behind (the fixture root asserts that at its close).
pub async fn dropped_call_finishes_in_the_guest_without_leaks<E: Incarnations>(engine: E) {
    let g = Gated::start(engine).await;
    g.run(async {
        let baseline = g.residue();
        let call = g.call("abandoned");
        g.gate.arrived(1).await;
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert_eq!(
            g.residue().calls,
            1,
            "the guest handler outlives its caller"
        );
        g.gate.release(b"abandoned");
        until(|| g.residue().calls == 0).await;
        assert_eq!(g.residue(), baseline);
    })
    .await;
    let Gated {
        harness,
        supervisor,
        relay,
        lease,
        ..
    } = g;
    drop((relay, lease));
    let micro = MicroId::new("a");
    tokio::select! {
        never = supervisor.drive() => never,
        () = until(|| supervisor.state(&micro) == MicroState::Idle) => {}
    }
    assert_eq!(harness.log(), ["initialize a", "close a"]);
    harness.finish(supervisor).await;
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
            factory: self.factory("provider", provider_role, None),
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
            factory: self.factory("consumer", "consumer", None),
        };
        Supervisor::new(self.host.clone(), [provider, consumer])
    }

    fn factory(
        &self,
        identity: &'static str,
        role: &'static str,
        peer: Option<CapabilityPublication>,
    ) -> Rc<dyn Factory<CapabilityPublication>> {
        Rc::new(Fixture {
            engine: Rc::clone(&self.engine),
            shared: Rc::clone(&self.shared),
            identity,
            role,
            peer,
        })
    }

    /// A graph of lazy `provider` micros without requirements, one per `(identity, peer)`.
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
                requirements: BTreeMap::new(),
                factory: self.factory(identity, "provider", Some(peer)),
            }),
        )
    }

    /// A lazy reference, held by the outside observer, to `micro`'s `publication`.
    fn lazy_ref(
        &self,
        supervisor: &Supervisor<CapabilityPublication>,
        micro: &str,
        publication: &str,
        contract: ContractIdentity,
    ) -> CapabilityRef {
        self.shared
            .observer
            .mint_lazy(LazyEndpoint {
                micro: MicroId::new(micro),
                publication: publication.to_owned(),
                contract,
                activator: supervisor.activator(),
            })
            .unwrap()
    }

    /// Publish a host-side call as the outside observer, to hand a guest as its `peer`.
    fn publish_peer<T: CallTarget + 'static>(&self, target: &Arc<T>) -> PublishedCapability {
        let target: Arc<dyn CallTarget> = Arc::clone(target) as _;
        self.shared
            .observer
            .publish_call(
                "peer",
                ContractIdentity::new("dayframe.lifecycle", "peer", (1, 0, 0)),
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
    /// Granted to the guest as its `peer` requirement, which the `relay` publication forwards to.
    peer: Option<CapabilityPublication>,
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
        let peer = self.peer.clone();
        Box::pin(async move {
            shared
                .log
                .borrow_mut()
                .push(format!("initialize {identity}"));
            let mut arguments = Vec::new();
            ciborium::into_writer(role, &mut arguments).unwrap();
            let mut requirements = requirements;
            requirements.extend(peer.map(|peer| ("peer".to_owned(), peer)));
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
                    "{identity} still held guest resources after its close"
                );
            }
            shared.log.borrow_mut().push(format!("close {identity}"));
            closed
        })
    }
}
