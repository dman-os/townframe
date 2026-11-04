use super::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::rc::Rc;
use tokio::sync::Notify;

const PRESENTATION: &str = "presentation";

/// What a fake provider hands its consumers: the providing incarnation and the publication's
/// name. The witness records when the last copy of the publication drops.
#[derive(Clone)]
struct Provision {
    owner: Instance,
    _witness: Rc<Witness>,
}

struct Witness {
    name: String,
    host: Host,
    owner: Instance,
    controls: Controls,
}

impl Drop for Witness {
    fn drop(&mut self) {
        let lifecycle = self.host.lifecycle(&self.owner);
        let entry = format!("drop {} while {lifecycle:?}", self.name);
        self.controls.0.borrow_mut().drops.push(entry);
    }
}

/// Shared script for every fake micro of one test: a log of lifecycle calls and the knobs that
/// make a named micro fail, withhold its publication, or park its initialization or close.
#[derive(Clone, Default)]
struct Controls(Rc<RefCell<Script>>);

#[derive(Default)]
struct Script {
    log: Vec<String>,
    /// Publications dropping, with the incarnation lifecycle at that moment.
    drops: Vec<String>,
    fail_init: BTreeSet<String>,
    fail_close: BTreeSet<String>,
    withhold: BTreeSet<String>,
    gates: BTreeMap<String, Rc<Notify>>,
    close_gates: BTreeMap<String, Rc<Notify>>,
}

struct Fake {
    name: String,
    controls: Controls,
}

struct FakeRoot {
    name: String,
    controls: Controls,
    host: Host,
    instance: Instance,
    requirements: BTreeMap<String, Provision>,
}

impl Controls {
    fn record(&self, entry: String) {
        self.0.borrow_mut().log.push(entry);
    }

    fn log(&self) -> Vec<String> {
        self.0.borrow().log.clone()
    }

    fn gate(&self, name: &str) -> Rc<Notify> {
        let gate = Rc::new(Notify::new());
        self.0
            .borrow_mut()
            .gates
            .insert(name.to_owned(), Rc::clone(&gate));
        gate
    }

    fn close_gate(&self, name: &str) -> Rc<Notify> {
        let gate = Rc::new(Notify::new());
        self.0
            .borrow_mut()
            .close_gates
            .insert(name.to_owned(), Rc::clone(&gate));
        gate
    }

    fn drops(&self) -> Vec<String> {
        self.0.borrow().drops.clone()
    }

    fn count(&self, entry: &str) -> usize {
        self.log().iter().filter(|logged| *logged == entry).count()
    }

    fn micro(&self, identity: &str) -> ConfiguredMicro<Provision> {
        ConfiguredMicro {
            identity: id(identity),
            eager: false,
            requirements: BTreeMap::new(),
            factory: Rc::new(Fake {
                name: identity.to_owned(),
                controls: self.clone(),
            }),
        }
    }
}

impl Factory<Provision> for Fake {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, Provision>,
        _activator: Activator<Provision>,
    ) -> LocalFuture<'static, Result<Initialized<Provision>, String>> {
        let name = self.name.clone();
        let controls = self.controls.clone();
        Box::pin(async move {
            assert_eq!(host.lifecycle(&instance), Lifecycle::Initializing);
            for provision in requirements.values() {
                assert_eq!(host.lifecycle(&provision.owner), Lifecycle::Active);
            }
            controls.record(format!("init {name}"));
            let gate = controls.0.borrow().gates.get(&name).cloned();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            if controls.0.borrow().fail_init.contains(&name) {
                return Err(format!("{name} refused"));
            }
            let publications = if controls.0.borrow().withhold.contains(&name) {
                BTreeMap::new()
            } else {
                let witness = Rc::new(Witness {
                    name: name.clone(),
                    host: host.clone(),
                    owner: instance.clone(),
                    controls: controls.clone(),
                });
                let provision = Provision {
                    owner: instance.clone(),
                    _witness: witness,
                };
                BTreeMap::from([(PRESENTATION.to_owned(), provision)])
            };
            let root = Box::new(FakeRoot {
                name,
                controls,
                host,
                instance,
                requirements,
            });
            Ok(Initialized { root, publications })
        })
    }
}

impl Root for FakeRoot {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            assert_eq!(self.host.lifecycle(&self.instance), Lifecycle::Closing);
            let gate = self
                .controls
                .0
                .borrow()
                .close_gates
                .get(&self.name)
                .cloned();
            match gate {
                Some(gate) => gate.notified().await,
                None => tokio::task::yield_now().await,
            }
            // A closing incarnation's dependencies are still live and usable.
            for provision in self.requirements.values() {
                assert_eq!(
                    self.host.lifecycle(&provision.owner),
                    Lifecycle::Active,
                    "{} closed before its consumer {}",
                    provision.owner.name,
                    self.name
                );
                self.host.check(&provision.owner).unwrap();
            }
            self.controls.record(format!("close {}", self.name));
            if self.controls.0.borrow().fail_close.contains(&self.name) {
                return Err(format!("{} close failed", self.name));
            }
            Ok(())
        })
    }
}

fn id(identity: &str) -> MicroId {
    MicroId::new(identity)
}

fn requires(
    mut micro: ConfiguredMicro<Provision>,
    binding: &str,
    provider: &str,
) -> ConfiguredMicro<Provision> {
    micro.requirements.insert(
        binding.to_owned(),
        Requirement {
            provider: id(provider),
            publication: PRESENTATION.to_owned(),
        },
    );
    micro
}

fn eager(mut micro: ConfiguredMicro<Provision>) -> ConfiguredMicro<Provision> {
    micro.eager = true;
    micro
}

/// Run `scenario` while the supervisor's idle driver runs beside it.
async fn driven<T>(supervisor: &Supervisor<Provision>, scenario: impl Future<Output = T>) -> T {
    tokio::select! {
        never = supervisor.drive() => never,
        out = scenario => out,
    }
}

async fn settle(mut done: impl FnMut() -> bool) {
    while !done() {
        tokio::task::yield_now().await;
    }
}

fn assert_quiescent(host: &Host) {
    let state = host.inner.lock().unwrap();
    assert!(
        state.instances.is_empty(),
        "an incarnation outlived shutdown"
    );
    assert!(state.dependencies.is_empty(), "a binding outlived shutdown");
    assert!(state.tearing_down.is_empty());
}

#[tokio::test]
async fn concurrent_acquires_share_one_initialization() {
    let controls = Controls::default();
    let gate = controls.gate("solo");
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    let (first, second, third, ()) = tokio::join!(
        supervisor.acquire(&solo, PRESENTATION),
        supervisor.acquire(&solo, PRESENTATION),
        supervisor.acquire(&solo, PRESENTATION),
        async {
            settle(|| !controls.log().is_empty()).await;
            gate.notify_one();
        }
    );
    let leases = [first.unwrap(), second.unwrap(), third.unwrap()];
    assert_eq!(controls.log(), ["init solo"]);
    let incarnation = supervisor.instance(&solo).unwrap();
    assert!(leases.iter().all(|lease| lease.owner == incarnation));
}

#[tokio::test]
async fn dropping_the_last_lease_closes_the_micro_exactly_once() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    driven(&supervisor, async {
        let first = supervisor.acquire(&solo, PRESENTATION).await.unwrap();
        let second = supervisor.acquire(&solo, PRESENTATION).await.unwrap();
        let incarnation = first.owner.clone();
        drop(first);
        // One lease still retains it: give the driver every chance to close it wrongly.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(supervisor.state(&solo), MicroState::Active);
        drop(second);
        settle(|| supervisor.state(&solo) == MicroState::Idle).await;
        assert_eq!(host.lifecycle(&incarnation), Lifecycle::Closed);
    })
    .await;
    assert_eq!(controls.log(), ["init solo", "close solo"]);
    assert_eq!(controls.drops(), ["drop solo while Closing"]);
    assert_quiescent(&host);
}

#[tokio::test]
async fn an_eager_root_survives_lease_churn() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [eager(controls.micro("root"))]);
    let root = id("root");
    assert!(supervisor.reconcile().await.is_empty());
    driven(&supervisor, async {
        for _ in 0..3 {
            drop(supervisor.acquire(&root, PRESENTATION).await.unwrap());
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        }
    })
    .await;
    assert_eq!(supervisor.state(&root), MicroState::Active);
    assert_eq!(controls.log(), ["init root"]);
}

#[tokio::test]
async fn a_lease_keeps_a_provider_a_binding_would_release() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("consumer"), "presentation", "provider"),
        ],
    );
    let (provider, consumer) = (id("provider"), id("consumer"));
    driven(&supervisor, async {
        supervisor.start(&consumer).await.unwrap();
        let lease = supervisor.acquire(&provider, PRESENTATION).await.unwrap();
        // The consumer's binding is gone, but the lease still retains the provider.
        supervisor.disable(&consumer).await.unwrap();
        assert_eq!(supervisor.state(&provider), MicroState::Active);
        drop(lease);
        settle(|| supervisor.state(&provider) == MicroState::Idle).await;
    })
    .await;
    assert_quiescent(&host);
}

#[tokio::test]
async fn a_lease_dropped_while_a_consumer_binds_does_not_close_the_provider() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("consumer"), "presentation", "provider"),
        ],
    );
    let (provider, consumer) = (id("provider"), id("consumer"));
    driven(&supervisor, async {
        supervisor.start(&consumer).await.unwrap();
        drop(supervisor.acquire(&provider, PRESENTATION).await.unwrap());
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(supervisor.state(&provider), MicroState::Active);
    })
    .await;
    supervisor.disable(&consumer).await.unwrap();
    assert_eq!(supervisor.state(&provider), MicroState::Idle);
}

#[tokio::test]
async fn acquiring_during_closing_waits_for_a_fresh_incarnation() {
    let controls = Controls::default();
    let close_gate = controls.close_gate("solo");
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    driven(&supervisor, async {
        let first = supervisor.acquire(&solo, PRESENTATION).await.unwrap();
        let old = first.owner.clone();
        // The last lease drops; the driver parks inside the guest's close.
        drop(first);
        settle(|| supervisor.state(&solo) == MicroState::Closing).await;
        let (lease, ()) = tokio::join!(supervisor.acquire(&solo, PRESENTATION), async {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                controls.count("init solo"),
                1,
                "demand entered a closing incarnation"
            );
            close_gate.notify_one();
        });
        let lease = lease.unwrap();
        assert_ne!(lease.owner, old);
        assert_eq!(host.lifecycle(&old), Lifecycle::Closed);
        assert_eq!(controls.count("init solo"), 2);
    })
    .await;
}

#[tokio::test]
async fn demand_does_not_retry_a_faulted_micro() {
    let controls = Controls::default();
    controls.0.borrow_mut().fail_init.insert("solo".to_owned());
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    let fault = StartError::Faulted("solo refused".to_owned());
    assert_eq!(
        supervisor.acquire(&solo, PRESENTATION).await.err(),
        Some(fault.clone())
    );
    assert_eq!(
        supervisor.acquire(&solo, PRESENTATION).await.err(),
        Some(fault)
    );
    assert_eq!(controls.log(), ["init solo"]);
    controls.0.borrow_mut().fail_init.clear();
    supervisor.retry(&solo);
    assert!(supervisor.acquire(&solo, PRESENTATION).await.is_ok());
    assert_eq!(controls.log(), ["init solo", "init solo"]);
}

#[tokio::test]
async fn acquiring_an_unpublished_name_fails_and_leaves_the_incarnation_up() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    let held = supervisor.acquire(&solo, PRESENTATION).await.unwrap();
    let missing = supervisor.acquire(&solo, "nonexistent").await;
    assert_eq!(
        missing.err(),
        Some(StartError::Unpublished {
            provider: solo.clone(),
            publication: "nonexistent".to_owned(),
        })
    );
    assert_eq!(supervisor.state(&solo), MicroState::Active);
    assert_eq!(held.owner, supervisor.instance(&solo).unwrap());
    // The failed demand took no lease: only `held` retains it.
    drop(held);
    driven(
        &supervisor,
        settle(|| supervisor.state(&solo) == MicroState::Idle),
    )
    .await;
    assert_eq!(controls.count("init solo"), 1);
}

#[tokio::test]
async fn an_acquire_resolves_only_after_initialization_returns() {
    let controls = Controls::default();
    let gate = controls.gate("solo");
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    let (lease, ()) = tokio::join!(supervisor.acquire(&solo, PRESENTATION), async {
        settle(|| !controls.log().is_empty()).await;
        // Mid-initialization the publication is staged: nothing can lease it.
        assert_eq!(supervisor.state(&solo), MicroState::Initializing);
        assert!(!matches!(
            supervisor.inner.borrow().nodes[&solo].state,
            NodeState::Active { .. }
        ));
        gate.notify_one();
    });
    assert_eq!(lease.unwrap().owner, supervisor.instance(&solo).unwrap());
}

#[tokio::test]
async fn publications_are_dropped_before_the_incarnation_is_torn_down() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    drop(supervisor.acquire(&solo, PRESENTATION).await.unwrap());
    supervisor.shutdown().await.unwrap();
    // Closing, not Closed: revocation precedes removal of the incarnation's scopes.
    assert_eq!(controls.log(), ["init solo", "close solo"]);
    assert_eq!(controls.drops(), ["drop solo while Closing"]);
    assert_quiescent(&host);
}

#[tokio::test]
async fn a_shared_provider_stays_live_while_any_consumer_binds_it() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("first"), "presentation", "provider"),
            requires(controls.micro("second"), "presentation", "provider"),
        ],
    );
    supervisor.start(&id("first")).await.unwrap();
    supervisor.start(&id("second")).await.unwrap();
    let provider = supervisor.instance(&id("provider")).unwrap();
    assert_eq!(
        controls.log(),
        ["init provider", "init first", "init second"]
    );

    supervisor.disable(&id("first")).await.unwrap();
    assert_eq!(supervisor.state(&id("provider")), MicroState::Active);
    assert!(host.is_bound(&provider));

    supervisor.disable(&id("second")).await.unwrap();
    assert_eq!(supervisor.state(&id("provider")), MicroState::Idle);
    assert!(!host.is_bound(&provider));
    assert_eq!(host.lifecycle(&provider), Lifecycle::Closed);
    assert_quiescent(&host);
}

#[tokio::test]
async fn shutdown_closes_dependents_while_their_providers_are_live() {
    let controls = Controls::default();
    let host = Host::new();
    // Names sort providers first, so shutdown must reorder rather than follow map order.
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("a-base"),
            requires(controls.micro("b-middle"), "presentation", "a-base"),
            eager(requires(
                controls.micro("c-top"),
                "presentation",
                "b-middle",
            )),
            eager(controls.micro("d-unrelated")),
        ],
    );
    assert!(supervisor.reconcile().await.is_empty());
    let instances = ["a-base", "b-middle", "c-top", "d-unrelated"]
        .map(|identity| supervisor.instance(&id(identity)).unwrap());
    supervisor.shutdown().await.unwrap();
    assert_eq!(
        controls.log(),
        [
            "init a-base",
            "init b-middle",
            "init c-top",
            "init d-unrelated",
            "close c-top",
            "close b-middle",
            "close a-base",
            "close d-unrelated",
        ]
    );
    for instance in &instances {
        assert_eq!(host.lifecycle(instance), Lifecycle::Closed);
    }
    assert_quiescent(&host);
}

#[tokio::test]
async fn a_close_failure_is_surfaced_after_teardown_completes() {
    let controls = Controls::default();
    controls
        .0
        .borrow_mut()
        .fail_close
        .insert("consumer".to_owned());
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("consumer"), "presentation", "provider"),
        ],
    );
    supervisor.start(&id("consumer")).await.unwrap();
    let consumer = supervisor.instance(&id("consumer")).unwrap();
    let provider = supervisor.instance(&id("provider")).unwrap();

    let error = supervisor.disable(&id("consumer")).await.unwrap_err();
    assert_eq!(
        error,
        CloseError(BTreeMap::from([(
            id("consumer"),
            "consumer close failed".to_owned()
        )]))
    );
    // The failure is recorded, not swallowed, and teardown still ran for both.
    assert_eq!(
        supervisor.state(&id("consumer")),
        MicroState::Faulted("consumer close failed".to_owned())
    );
    assert_eq!(host.lifecycle(&consumer), Lifecycle::Closed);
    assert_eq!(host.lifecycle(&provider), Lifecycle::Closed);
    assert_eq!(supervisor.state(&id("provider")), MicroState::Idle);
    assert_quiescent(&host);

    // A fault is not retried by demand, only by an explicit retry.
    assert_eq!(
        supervisor.start(&id("consumer")).await,
        Err(StartError::Faulted("consumer close failed".to_owned()))
    );
    supervisor.retry(&id("consumer"));
    supervisor.start(&id("consumer")).await.unwrap();
    assert_eq!(supervisor.state(&id("consumer")), MicroState::Active);
}

#[tokio::test]
async fn concurrent_closes_drive_one_root_close() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    supervisor.start(&id("solo")).await.unwrap();
    let solo = id("solo");
    let (first, second) = tokio::join!(supervisor.disable(&solo), supervisor.disable(&solo));
    assert_eq!((first, second), (Ok(()), Ok(())));
    assert_eq!(controls.log(), ["init solo", "close solo"]);
    assert_eq!(supervisor.state(&id("solo")), MicroState::Disabled);
    // Closing an already closed micro is deterministic and does nothing further.
    supervisor.disable(&id("solo")).await.unwrap();
    assert_eq!(controls.log(), ["init solo", "close solo"]);
    assert_quiescent(&host);
}

#[tokio::test]
async fn a_failed_provider_blocks_its_consumer_and_leaves_nothing_live() {
    let controls = Controls::default();
    controls
        .0
        .borrow_mut()
        .fail_init
        .insert("provider".to_owned());
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("consumer"), "presentation", "provider"),
        ],
    );
    let failure = StartError::Unavailable {
        binding: "presentation".to_owned(),
        cause: Box::new(StartError::Faulted("provider refused".to_owned())),
    };
    assert_eq!(
        supervisor.start(&id("consumer")).await,
        Err(failure.clone())
    );
    assert_eq!(
        supervisor.state(&id("consumer")),
        MicroState::Blocked(vec!["presentation".to_owned()])
    );
    assert_eq!(
        supervisor.state(&id("provider")),
        MicroState::Faulted("provider refused".to_owned())
    );
    assert_quiescent(&host);

    // Demand does not re-run a faulted initialization.
    assert_eq!(supervisor.start(&id("consumer")).await, Err(failure));
    assert_eq!(controls.log(), ["init provider"]);

    controls.0.borrow_mut().fail_init.clear();
    supervisor.retry(&id("provider"));
    supervisor.start(&id("consumer")).await.unwrap();
    assert_eq!(
        controls.log(),
        ["init provider", "init provider", "init consumer"]
    );
}

#[tokio::test]
async fn a_provider_without_the_required_publication_is_unavailable() {
    let controls = Controls::default();
    controls
        .0
        .borrow_mut()
        .withhold
        .insert("provider".to_owned());
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("consumer"), "presentation", "provider"),
        ],
    );
    assert_eq!(
        supervisor.start(&id("consumer")).await,
        Err(StartError::Unavailable {
            binding: "presentation".to_owned(),
            cause: Box::new(StartError::Unpublished {
                provider: id("provider"),
                publication: PRESENTATION.to_owned(),
            }),
        })
    );
    // The provider was started only for this consumer, so it is released again.
    assert_eq!(supervisor.state(&id("provider")), MicroState::Idle);
    assert_quiescent(&host);
}

#[tokio::test]
async fn disabling_a_pending_initialization_cancels_it_without_exposure() {
    let controls = Controls::default();
    let gate = controls.gate("consumer");
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            controls.micro("provider"),
            requires(controls.micro("consumer"), "presentation", "provider"),
        ],
    );
    let consumer_id = id("consumer");
    let (started, ()) = tokio::join!(supervisor.start(&consumer_id), async {
        while controls.log().len() < 2 {
            tokio::task::yield_now().await;
        }
        // Mid-initialization: nothing the consumer would provide is observable.
        assert_eq!(supervisor.state(&id("consumer")), MicroState::Initializing);
        assert!(!matches!(
            supervisor.inner.borrow().nodes[&id("consumer")].state,
            NodeState::Active { .. }
        ));
        let consumer = supervisor.instance(&id("consumer")).unwrap();
        assert_eq!(host.lifecycle(&consumer), Lifecycle::Initializing);
        supervisor.disable(&id("consumer")).await.unwrap();
        assert_eq!(host.lifecycle(&consumer), Lifecycle::Closed);
    });
    assert_eq!(started, Err(StartError::Access(AccessError::Revoked)));
    assert_eq!(supervisor.state(&id("consumer")), MicroState::Disabled);
    // The cancelled consumer's binding is gone, so its lazy provider is released.
    assert_eq!(supervisor.state(&id("provider")), MicroState::Idle);
    assert_eq!(
        controls.log(),
        ["init provider", "init consumer", "close provider"]
    );
    assert_quiescent(&host);
    drop(gate);
}

#[tokio::test]
async fn requirement_cycles_are_rejected_before_anything_initializes() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            requires(controls.micro("a"), "presentation", "b"),
            requires(controls.micro("b"), "presentation", "a"),
        ],
    );
    assert_eq!(
        supervisor.start(&id("a")).await,
        Err(StartError::Cycle(id("a")))
    );
    assert_eq!(supervisor.state(&id("a")), MicroState::Idle);
    assert_eq!(supervisor.state(&id("b")), MicroState::Idle);
    assert!(controls.log().is_empty());
    assert_quiescent(&host);
}

#[tokio::test]
async fn a_stale_incarnation_cannot_affect_its_replacement() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    supervisor.start(&id("solo")).await.unwrap();
    let stale = supervisor.instance(&id("solo")).unwrap();
    supervisor.disable(&id("solo")).await.unwrap();
    supervisor.enable(&id("solo"));
    supervisor.start(&id("solo")).await.unwrap();
    let replacement = supervisor.instance(&id("solo")).unwrap();
    assert_ne!(stale, replacement);

    host.close_instance(&stale).unwrap();
    assert!(!host.begin_close(&stale).unwrap());
    assert_eq!(host.lifecycle(&replacement), Lifecycle::Active);
    assert_eq!(supervisor.state(&id("solo")), MicroState::Active);
}

#[tokio::test]
async fn an_incarnation_ended_outside_supervision_is_a_fault() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    supervisor.start(&id("solo")).await.unwrap();
    host.close_instance(&supervisor.instance(&id("solo")).unwrap())
        .unwrap();
    let fault = "incarnation ended outside supervision".to_owned();
    assert_eq!(
        supervisor.state(&id("solo")),
        MicroState::Faulted(fault.clone())
    );
    assert_eq!(
        supervisor.start(&id("solo")).await,
        Err(StartError::Faulted(fault))
    );
}

#[tokio::test]
async fn a_failed_demand_that_started_the_micro_lets_it_idle_close() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    driven(&supervisor, async {
        assert!(supervisor.acquire(&solo, "absent").await.is_err());
        settle(|| supervisor.state(&solo) == MicroState::Idle).await;
    })
    .await;
    assert_eq!(controls.log(), ["init solo", "close solo"]);
}

#[tokio::test]
async fn an_activator_is_served_by_the_driver_and_status_never_activates() {
    fn send_and_sync<T: Send + Sync>() {}
    send_and_sync::<Activator<u32>>();
    send_and_sync::<Lease<u32>>();

    let controls = Controls::default();
    let gate = controls.gate("solo");
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let (solo, activator) = (id("solo"), supervisor.activator());
    assert_eq!(activator.status(&solo), Some(MicroState::Idle));
    assert_eq!(activator.status(&id("nobody")), None);
    assert!(controls.log().is_empty());
    driven(&supervisor, async {
        let (lease, ()) = tokio::join!(activator.acquire(&solo, PRESENTATION), async {
            settle(|| !controls.log().is_empty()).await;
            assert_eq!(activator.status(&solo), Some(MicroState::Initializing));
            gate.notify_one();
        });
        let lease = lease.unwrap();
        assert_eq!(activator.status(&solo), Some(MicroState::Active));
        assert_eq!(lease.owner, supervisor.instance(&solo).unwrap());
        drop(lease);
        settle(|| activator.status(&solo) == Some(MicroState::Idle)).await;
    })
    .await;
    assert_eq!(controls.log(), ["init solo", "close solo"]);
}

#[tokio::test]
#[should_panic(expected = "driver ended while a request was outstanding")]
async fn dropping_a_driver_with_queued_requests_fails_them_instead_of_hanging() {
    let controls = Controls::default();
    let supervisor = Supervisor::new(Host::new(), [controls.micro("solo")]);
    let (solo, activator) = (id("solo"), supervisor.activator());
    let served = ServedRequests {
        receiver: supervisor.served.borrow_mut().take(),
        home: &supervisor.served,
    };
    let acquire = activator.acquire(&solo, PRESENTATION);
    futures::pin_mut!(acquire);
    // The first poll queues the request; nothing serves it.
    assert!(futures::poll!(acquire.as_mut()).is_pending());
    drop(served);
    // The channel is handed back for a later driver, empty, and the requester is released.
    assert!(supervisor.served.borrow().is_some());
    acquire.await.unwrap();
}

#[tokio::test]
async fn recorded_state_changes_tick_the_change_receiver() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    let solo = id("solo");
    let mut changes = supervisor.changes();
    changes.mark_unchanged();
    supervisor.disable(&solo).await.unwrap();
    assert!(changes.has_changed().unwrap());
    changes.mark_unchanged();
    supervisor.enable(&solo);
    assert!(changes.has_changed().unwrap());
    assert_eq!(supervisor.state(&solo), MicroState::Idle);
}
