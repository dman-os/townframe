//! Engine conformance for the ADR 024 activation boundary, run against the prepared
//! `lifecycle` fixture component (`src/clay_mdk/lifecycle_fixture.rs`).
//!
//! Each engine crate implements [`Incarnations`] over its runtime and runs these scenarios from
//! its own tests, so every engine is held to one chain: configured requirement → strong binding
//! (or external lease) → provider initialization → named publication visible only once committed
//! → async `instance-root.close` with dependencies live → publications revoked →
//! reverse-order teardown that leaves the guest holding nothing.
//!
//! Scenarios must run inside a [`tokio::task::LocalSet`]: incarnation drivers are spawned with
//! [`tokio::task::spawn_local`] and joined (panics propagate) once the host closes.
use std::any::Any;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
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
    /// With a call in flight the engine must skip the guest's `close` (it cannot run beside a
    /// suspended call), cancel the call and revoke every queued one, and say so with
    /// [`GuestClose::Skipped`]; the residue is then meaningless.
    fn close(
        runtime: Self::Runtime,
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

/// A call the scenario controls, standing in for "something external" a guest handler awaits:
/// each call announces itself on `entered` and then waits for a permit on `release`.
struct Gate {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

impl CallTarget for Gate {
    fn call(
        &self,
        request: Vec<u8>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let (entered, release) = (Arc::clone(&self.entered), Arc::clone(&self.release));
        Box::pin(async move {
            entered.add_permits(1);
            release.acquire().await.unwrap().forget();
            Ok([b"held:".as_slice(), &request].concat())
        })
    }
}

/// A call that forwards to a publication that only exists after the caller's own graph does,
/// which is how a scenario closes a cycle between incarnations. It is owned by a node of its own
/// (`from`'s incarnation), so its edges are distinct from the test's. With a `gate` it first
/// announces itself and waits to be released, holding the chain that reached it in flight.
struct Loopback {
    from: CapabilitySession,
    to: Arc<OnceLock<CapabilityPublication>>,
    gate: Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>,
}

impl CallTarget for Loopback {
    fn call(
        &self,
        request: Vec<u8>,
    ) -> futures::future::BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let (from, to, gate) = (self.from.clone(), Arc::clone(&self.to), self.gate.clone());
        Box::pin(async move {
            if let Some((entered, release)) = gate {
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
            }
            let target = from
                .mint(to.get().expect("the cycle is closed before any call"))
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

    /// A host-side incarnation of its own, publishing `loopback` forwarding to `to` (optionally
    /// after a `gate`), so a scenario's cycle passes through a node that is nobody else.
    fn loopback(
        &self,
        name: &str,
        to: &Arc<OnceLock<CapabilityPublication>>,
        gate: Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>,
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
                    gate,
                }),
            )
            .unwrap()
    }

    /// A gate bound as a `service`: the lease keeps it published, the semaphores let the
    /// scenario count calls that entered it and let them out.
    fn gate(
        &self,
    ) -> (
        PublishedCapability,
        Arc<tokio::sync::Semaphore>,
        Arc<tokio::sync::Semaphore>,
    ) {
        let (entered, release) = (
            Arc::new(tokio::sync::Semaphore::new(0)),
            Arc::new(tokio::sync::Semaphore::new(0)),
        );
        // Published by a node of its own: were the observer both the caller of the provider
        // and the owner of its service, the provider's call would look like a cycle.
        let owner = self.host.start("gate");
        let owner = CapabilitySession::new(self.host.clone(), owner, CapabilityLimits::default());
        let lease = owner
            .publish_call(
                "gate",
                echo_contract(),
                Arc::new(Gate {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                }),
            )
            .unwrap();
        (lease, entered, release)
    }

    /// Start a committed `provider` incarnation, publishing `relay` when `service` is bound.
    async fn provider(&self, name: &str, service: Option<CapabilityPublication>) -> Started<E> {
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
            let (closed, residue) = E::close(runtime).await;
            assert_eq!(closed.unwrap(), GuestClose::Completed);
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

/// A provider call that is suspended on something external does not hold its incarnation: a
/// sync `mount` and its unmount enter the same guest meanwhile, and the call still completes
/// once what it awaited is released.
pub async fn suspended_call_does_not_block_sync_entry<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (gate, entered, release) = rig.gate();
    let provider = rig.provider("provider", Some(gate.publication())).await;
    let call = rig.call(&provider, RELAY, b"one");
    entered.acquire().await.unwrap().forget();
    let mounted = E::mount(&provider.runtime, PUBLICATION)
        .expect("a presentation mounts while a call is suspended in the same guest");
    assert!(!call.is_finished(), "the call is still waiting on its gate");
    drop(mounted);
    release.add_permits(1);
    assert_eq!(call.await.unwrap(), Ok(b"held:one".to_vec()));
    drop(gate);
    rig.finish(vec![provider]).await;
}

/// Calls into one provider are serialized, because a guest instance holds one suspended async
/// task at a time: while the first is suspended the second waits in the provider's queue, and
/// each completes with its own answer, in order.
pub async fn calls_into_one_micro_are_serialized<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (gate, entered, release) = rig.gate();
    let provider = rig.provider("provider", Some(gate.publication())).await;
    let first = rig.call(&provider, RELAY, b"first");
    entered.acquire().await.unwrap().forget();
    let second = rig.call(&provider, RELAY, b"second");
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        entered.available_permits(),
        0,
        "the second call does not start while the first is suspended"
    );
    assert!(!first.is_finished() && !second.is_finished());
    release.add_permits(1);
    assert_eq!(first.await.unwrap(), Ok(b"held:first".to_vec()));
    entered.acquire().await.unwrap().forget();
    assert!(
        !second.is_finished(),
        "the second call runs once the first ended"
    );
    release.add_permits(1);
    assert_eq!(second.await.unwrap(), Ok(b"held:second".to_vec()));
    drop(gate);
    rig.finish(vec![provider]).await;
}

/// The message a relay carries back when the call it forwarded was refused as a cycle.
fn is_cycle_refusal(outcome: &Result<Vec<u8>, CallError>) -> bool {
    matches!(outcome, Err(CallError::Provider(message)) if message.contains("CallCycle"))
}

/// `a` relays to `b`, whose service reaches back into `a` while `a`'s relay is still waiting on
/// `b`. `a` serves one call at a time, so the call that would close the cycle is refused
/// promptly with `call-cycle` to the innermost caller, and the relays carry that refusal out.
pub async fn reentrant_call_chain_never_hangs<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let back_into_a = Arc::new(OnceLock::new());
    let loopback = rig.loopback("loopback", &back_into_a, None);
    let b = rig.provider("b", Some(loopback.publication())).await;
    let a = rig.provider("a", Some(b.publications[RELAY].clone())).await;
    back_into_a.set(a.publications[ECHO].clone()).unwrap();
    let outcome = rig.call(&a, RELAY, b"ping").await.unwrap();
    assert!(is_cycle_refusal(&outcome), "{outcome:?}");
    // Nothing is left waiting, and the same providers serve once the cycle is gone.
    assert_eq!(
        rig.call(&a, ECHO, b"again").await.unwrap(),
        Ok(b"echo:again".to_vec())
    );
    drop(loopback);
    rig.finish(vec![a, b]).await;
}

/// A cycle through a *queued* call. `b` is busy with a call that waits on `a`, and `c`'s call
/// into `b` queues behind it. `a`'s handler then calls `c` through a node of its own: `c` waits
/// on `b` (queued), `b` on `a`, `a` on that node, so the node's call into `c` closes a cycle
/// even though no call along it is running yet. It is refused, and so is the queued call when
/// it later runs into the same shape.
pub async fn cycle_through_queued_call_is_refused<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let into_c = Arc::new(OnceLock::new());
    let (entered, release) = (
        Arc::new(tokio::sync::Semaphore::new(0)),
        Arc::new(tokio::sync::Semaphore::new(0)),
    );
    let late = rig.loopback(
        "late",
        &into_c,
        Some((Arc::clone(&entered), Arc::clone(&release))),
    );
    let a = rig.provider("a", Some(late.publication())).await;
    let b = rig.provider("b", Some(a.publications[RELAY].clone())).await;
    let c = rig.provider("c", Some(b.publications[RELAY].clone())).await;
    into_c.set(c.publications[RELAY].clone()).unwrap();
    // b's call is running and has reached `late`, held at its gate.
    let running = rig.call(&b, RELAY, b"running");
    entered.acquire().await.unwrap().forget();
    // c's call runs, and its call into b queues behind b's.
    let queued = rig.call(&c, RELAY, b"queued");
    while !rig.host.waits_on(&c.instance, &b.instance) {
        tokio::task::yield_now().await;
    }
    // Releasing `late` makes it call c; both outer calls end in the refusal.
    release.add_permits(2);
    let running = running.await.unwrap();
    assert!(is_cycle_refusal(&running), "{running:?}");
    let queued = queued.await.unwrap();
    assert!(is_cycle_refusal(&queued), "{queued:?}");
    drop(late);
    rig.finish(vec![a, b, c]).await;
}

/// Closing a provider while a call is in flight and another is queued revokes both callers.
/// The guest's own `close` cannot run beside its suspended call, so the engine skips it and the
/// host teardown that follows does not depend on it (ADR 024 §6).
pub async fn close_revokes_calls_in_flight<E: Incarnations>(engine: E) {
    let rig = Rig::new(engine);
    let (gate, entered, _release) = rig.gate();
    let provider = rig.provider("provider", Some(gate.publication())).await;
    let running = rig.call(&provider, RELAY, b"running");
    entered.acquire().await.unwrap().forget();
    let queued = rig.call(&provider, RELAY, b"queued");
    let Started {
        instance, runtime, ..
    } = provider;
    let (closed, _residue) = E::close(runtime).await;
    assert_eq!(closed.unwrap(), GuestClose::Skipped);
    rig.host.close_instance(&instance).unwrap();
    assert_eq!(rig.host.lifecycle(&instance), Lifecycle::Closed);
    for call in [running, queued] {
        assert_eq!(
            call.await.unwrap(),
            Err(CallError::Capability(CapabilityFailure::Revoked))
        );
    }
    assert_eq!(
        entered.available_permits(),
        0,
        "the queued call never started"
    );
    drop(gate);
    rig.finish(Vec::new()).await;
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
        assert_eq!(closed.unwrap(), GuestClose::Completed);
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
            assert_eq!(
                residue,
                GuestResidue::NONE,
                "{identity} still held guest resources after its close"
            );
            shared.log.borrow_mut().push(format!("close {identity}"));
            assert_eq!(
                closed?,
                GuestClose::Completed,
                "{identity} had no call in flight"
            );
            Ok(())
        })
    }
}
