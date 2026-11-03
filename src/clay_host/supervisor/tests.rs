use super::*;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;
use tokio::sync::Notify;

const PRESENTATION: &str = "dayframe.lifecycle.presentation";

/// What a fake provider hands its consumers: the providing incarnation and the contract.
#[derive(Clone, Debug, PartialEq)]
struct Provision {
    owner: Instance,
    contract: String,
}

/// Shared script for every fake micro of one test: a log of lifecycle calls and the knobs that
/// make a named micro fail, withhold a provision, or park its initialization.
#[derive(Clone, Default)]
struct Controls(Rc<RefCell<Script>>);

#[derive(Default)]
struct Script {
    log: Vec<String>,
    fail_init: BTreeSet<String>,
    fail_close: BTreeSet<String>,
    withhold: BTreeSet<String>,
    gates: BTreeMap<String, Rc<Notify>>,
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

    fn micro(&self, identity: &str) -> ConfiguredMicro<Provision> {
        ConfiguredMicro {
            identity: identity.to_owned(),
            eager: false,
            requirements: BTreeMap::new(),
            accepts: BTreeSet::new(),
            registration_grants: Vec::new(),
            routes: BTreeMap::new(),
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
    ) -> LocalFuture<'static, Result<Box<dyn Root<Provision>>, String>> {
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
            let root: Box<dyn Root<Provision>> = Box::new(FakeRoot {
                name,
                controls,
                host,
                instance,
                requirements,
            });
            Ok(root)
        })
    }
}

impl Root<Provision> for FakeRoot {
    fn provision(&self, contract: &str) -> Option<Provision> {
        (!self.controls.0.borrow().withhold.contains(&self.name)).then(|| Provision {
            owner: self.instance.clone(),
            contract: contract.to_owned(),
        })
    }

    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            assert_eq!(self.host.lifecycle(&self.instance), Lifecycle::Closing);
            tokio::task::yield_now().await;
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

fn requires(
    mut micro: ConfiguredMicro<Provision>,
    binding: &str,
    provider: Option<&str>,
) -> ConfiguredMicro<Provision> {
    micro.requirements.insert(
        binding.to_owned(),
        Requirement {
            contract: PRESENTATION.to_owned(),
            provider: provider.map(str::to_owned),
        },
    );
    micro
}

fn eager(mut micro: ConfiguredMicro<Provision>) -> ConfiguredMicro<Provision> {
    micro.eager = true;
    micro
}

/// A target that accepts presentation registrations through its `presentation` requirement.
fn target(controls: &Controls, identity: &str) -> ConfiguredMicro<Provision> {
    let mut micro = requires(controls.micro(identity), "presentation", None);
    micro.accepts.insert(PRESENTATION.to_owned());
    micro
}

/// A subject whose `host` contribution binding is routed to `target` and authorized there.
fn subject(controls: &Controls, identity: &str, target: &str) -> ConfiguredMicro<Provision> {
    let mut micro = controls.micro(identity);
    micro.routes.insert("host".to_owned(), target.to_owned());
    micro.registration_grants.push(RegistrationGrant {
        target: target.to_owned(),
        contract: PRESENTATION.to_owned(),
    });
    micro
}

fn serve_host() -> RegistrationTemplate {
    RegistrationTemplate {
        id: "serve-host".to_owned(),
        target_binding: "host".to_owned(),
        contract: PRESENTATION.to_owned(),
        declaration_id: "serve-host".to_owned(),
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
async fn an_accepted_registration_binds_the_target_to_its_provider() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            eager(target(&controls, "shell")),
            subject(&controls, "provider", "shell"),
        ],
    );
    let registration = supervisor.install("provider", serve_host()).unwrap();
    assert_eq!(
        registration,
        ActivationRegistration {
            id: "serve-host".to_owned(),
            subject: "provider".to_owned(),
            target: "shell".to_owned(),
            contract: PRESENTATION.to_owned(),
            declaration_id: "serve-host".to_owned(),
        }
    );
    // Installation is data: nothing is instantiated until the target starts.
    assert_eq!(supervisor.state("provider"), MicroState::Idle);
    assert!(controls.log().is_empty());

    assert!(supervisor.reconcile().await.is_empty());
    assert_eq!(controls.log(), ["init provider", "init shell"]);
    let provider = supervisor.instance("provider").unwrap();
    assert!(host.is_bound(&provider));
    let provision = supervisor.provision("provider", PRESENTATION).unwrap();
    assert_eq!(provision.owner, provider);
    assert_eq!(provision.contract, PRESENTATION);

    // Closing the target releases its binding; a lazy provider with no binder closes after it.
    supervisor.disable("shell").await.unwrap();
    assert_eq!(
        controls.log(),
        [
            "init provider",
            "init shell",
            "close shell",
            "close provider"
        ]
    );
    assert_eq!(supervisor.state("shell"), MicroState::Disabled);
    assert_eq!(supervisor.state("provider"), MicroState::Idle);
    assert_eq!(host.lifecycle(&provider), Lifecycle::Closed);
    assert_quiescent(&host);
}

#[tokio::test]
async fn registration_requires_routing_authority_and_target_acceptance() {
    let controls = Controls::default();
    let host = Host::new();
    let mut unauthorized = subject(&controls, "unauthorized", "shell");
    unauthorized.registration_grants.clear();
    let refusing = requires(controls.micro("refusing"), "presentation", None);
    let supervisor = Supervisor::new(
        host.clone(),
        [
            target(&controls, "shell"),
            refusing,
            controls.micro("unrouted"),
            unauthorized,
            subject(&controls, "provider", "refusing"),
        ],
    );
    assert_eq!(
        supervisor.install("ghost", serve_host()),
        Err(RegistrationError::UnknownSubject("ghost".to_owned()))
    );
    assert_eq!(
        supervisor.install("unrouted", serve_host()),
        Err(RegistrationError::Unrouted("host".to_owned()))
    );
    assert_eq!(
        supervisor.install("unauthorized", serve_host()),
        Err(RegistrationError::Unauthorized {
            subject: "unauthorized".to_owned(),
            target: "shell".to_owned(),
            contract: PRESENTATION.to_owned(),
        })
    );
    // Without an accepted registration the target's requirement stays unresolved.
    assert_eq!(
        supervisor.start("shell").await,
        Err(StartError::Blocked(vec!["presentation".to_owned()]))
    );
    assert_eq!(
        supervisor.state("shell"),
        MicroState::Blocked(vec!["presentation".to_owned()])
    );
    // An authorized registration on a target that does not accept the contract binds nothing.
    supervisor.install("provider", serve_host()).unwrap();
    assert_eq!(
        supervisor.start("refusing").await,
        Err(StartError::Blocked(vec!["presentation".to_owned()]))
    );
    // Uninstalling withdraws the data; nothing was ever instantiated.
    assert!(supervisor.uninstall("serve-host").is_some());
    assert!(controls.log().is_empty());
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
            requires(controls.micro("first"), "presentation", Some("provider")),
            requires(controls.micro("second"), "presentation", Some("provider")),
        ],
    );
    supervisor.start("first").await.unwrap();
    supervisor.start("second").await.unwrap();
    let provider = supervisor.instance("provider").unwrap();
    assert_eq!(
        controls.log(),
        ["init provider", "init first", "init second"]
    );

    supervisor.disable("first").await.unwrap();
    assert_eq!(supervisor.state("provider"), MicroState::Active);
    assert!(host.is_bound(&provider));

    supervisor.disable("second").await.unwrap();
    assert_eq!(supervisor.state("provider"), MicroState::Idle);
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
            requires(controls.micro("b-middle"), "presentation", Some("a-base")),
            eager(requires(
                controls.micro("c-top"),
                "presentation",
                Some("b-middle"),
            )),
            eager(controls.micro("d-unrelated")),
        ],
    );
    assert!(supervisor.reconcile().await.is_empty());
    let instances = ["a-base", "b-middle", "c-top", "d-unrelated"]
        .map(|identity| supervisor.instance(identity).unwrap());
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
            requires(controls.micro("consumer"), "presentation", Some("provider")),
        ],
    );
    supervisor.start("consumer").await.unwrap();
    let consumer = supervisor.instance("consumer").unwrap();
    let provider = supervisor.instance("provider").unwrap();

    let error = supervisor.disable("consumer").await.unwrap_err();
    assert_eq!(
        error,
        CloseError(BTreeMap::from([(
            "consumer".to_owned(),
            "consumer close failed".to_owned()
        )]))
    );
    // The failure is recorded, not swallowed, and teardown still ran for both.
    assert_eq!(
        supervisor.state("consumer"),
        MicroState::Faulted("consumer close failed".to_owned())
    );
    assert_eq!(host.lifecycle(&consumer), Lifecycle::Closed);
    assert_eq!(host.lifecycle(&provider), Lifecycle::Closed);
    assert_eq!(supervisor.state("provider"), MicroState::Idle);
    assert_quiescent(&host);

    // A fault is not retried by demand, only by an explicit retry.
    assert_eq!(
        supervisor.start("consumer").await,
        Err(StartError::Faulted("consumer close failed".to_owned()))
    );
    supervisor.retry("consumer");
    supervisor.start("consumer").await.unwrap();
    assert_eq!(supervisor.state("consumer"), MicroState::Active);
}

#[tokio::test]
async fn concurrent_closes_drive_one_root_close() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    supervisor.start("solo").await.unwrap();
    let (first, second) = tokio::join!(supervisor.disable("solo"), supervisor.disable("solo"));
    assert_eq!((first, second), (Ok(()), Ok(())));
    assert_eq!(controls.log(), ["init solo", "close solo"]);
    assert_eq!(supervisor.state("solo"), MicroState::Disabled);
    // Closing an already closed micro is deterministic and does nothing further.
    supervisor.disable("solo").await.unwrap();
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
            requires(controls.micro("consumer"), "presentation", Some("provider")),
        ],
    );
    let failure = StartError::Unavailable {
        binding: "presentation".to_owned(),
        cause: Box::new(StartError::Faulted("provider refused".to_owned())),
    };
    assert_eq!(supervisor.start("consumer").await, Err(failure.clone()));
    assert_eq!(
        supervisor.state("consumer"),
        MicroState::Blocked(vec!["presentation".to_owned()])
    );
    assert_eq!(
        supervisor.state("provider"),
        MicroState::Faulted("provider refused".to_owned())
    );
    assert_quiescent(&host);

    // Demand does not re-run a faulted initialization.
    assert_eq!(supervisor.start("consumer").await, Err(failure));
    assert_eq!(controls.log(), ["init provider"]);

    controls.0.borrow_mut().fail_init.clear();
    supervisor.retry("provider");
    supervisor.start("consumer").await.unwrap();
    assert_eq!(
        controls.log(),
        ["init provider", "init provider", "init consumer"]
    );
}

#[tokio::test]
async fn a_provider_without_the_required_provision_is_unavailable() {
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
            requires(controls.micro("consumer"), "presentation", Some("provider")),
        ],
    );
    assert_eq!(
        supervisor.start("consumer").await,
        Err(StartError::Unavailable {
            binding: "presentation".to_owned(),
            cause: Box::new(StartError::Faulted(format!(
                "provider does not provide {PRESENTATION}"
            ))),
        })
    );
    // The provider was started only for this consumer, so it is released again.
    assert_eq!(supervisor.state("provider"), MicroState::Idle);
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
            requires(controls.micro("consumer"), "presentation", Some("provider")),
        ],
    );
    let (started, ()) = tokio::join!(supervisor.start("consumer"), async {
        while controls.log().len() < 2 {
            tokio::task::yield_now().await;
        }
        // Mid-initialization: nothing the consumer would provide is observable.
        assert_eq!(supervisor.state("consumer"), MicroState::Initializing);
        assert_eq!(supervisor.provision("consumer", PRESENTATION), None);
        let consumer = supervisor.instance("consumer").unwrap();
        assert_eq!(host.lifecycle(&consumer), Lifecycle::Initializing);
        supervisor.disable("consumer").await.unwrap();
        assert_eq!(host.lifecycle(&consumer), Lifecycle::Closed);
    });
    assert_eq!(started, Err(StartError::Access(AccessError::Revoked)));
    assert_eq!(supervisor.state("consumer"), MicroState::Disabled);
    // The cancelled consumer's binding is gone, so its lazy provider is released.
    assert_eq!(supervisor.state("provider"), MicroState::Idle);
    assert_eq!(
        controls.log(),
        ["init provider", "init consumer", "close provider"]
    );
    assert_quiescent(&host);
    drop(gate);
}

#[tokio::test]
async fn concurrent_starts_share_one_initialization() {
    let controls = Controls::default();
    let gate = controls.gate("solo");
    let supervisor = Supervisor::new(Host::new(), [controls.micro("solo")]);
    let (first, second, ()) =
        tokio::join!(supervisor.start("solo"), supervisor.start("solo"), async {
            while controls.log().is_empty() {
                tokio::task::yield_now().await;
            }
            gate.notify_one();
        });
    assert_eq!((first, second), (Ok(()), Ok(())));
    assert_eq!(controls.log(), ["init solo"]);
}

#[tokio::test]
async fn requirement_cycles_are_rejected_before_anything_initializes() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(
        host.clone(),
        [
            requires(controls.micro("a"), "presentation", Some("b")),
            requires(controls.micro("b"), "presentation", Some("a")),
        ],
    );
    assert_eq!(
        supervisor.start("a").await,
        Err(StartError::Cycle("a".to_owned()))
    );
    assert_eq!(supervisor.state("a"), MicroState::Idle);
    assert_eq!(supervisor.state("b"), MicroState::Idle);
    assert!(controls.log().is_empty());
    assert_quiescent(&host);
}

#[tokio::test]
async fn a_stale_incarnation_cannot_affect_its_replacement() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    supervisor.start("solo").await.unwrap();
    let stale = supervisor.instance("solo").unwrap();
    supervisor.disable("solo").await.unwrap();
    supervisor.enable("solo");
    supervisor.start("solo").await.unwrap();
    let replacement = supervisor.instance("solo").unwrap();
    assert_ne!(stale, replacement);

    host.close_instance(&stale).unwrap();
    assert!(!host.begin_close(&stale).unwrap());
    assert_eq!(host.lifecycle(&replacement), Lifecycle::Active);
    assert_eq!(supervisor.state("solo"), MicroState::Active);
}

#[tokio::test]
async fn an_incarnation_ended_outside_supervision_is_a_fault() {
    let controls = Controls::default();
    let host = Host::new();
    let supervisor = Supervisor::new(host.clone(), [controls.micro("solo")]);
    supervisor.start("solo").await.unwrap();
    host.close_instance(&supervisor.instance("solo").unwrap())
        .unwrap();
    let fault = "incarnation ended outside supervision".to_owned();
    assert_eq!(supervisor.state("solo"), MicroState::Faulted(fault.clone()));
    assert_eq!(
        supervisor.start("solo").await,
        Err(StartError::Faulted(fault))
    );
}
