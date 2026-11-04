//! Lazy references against a real supervisor: activation, retention, and revocation.
use super::*;
use clay_host::ConfiguredMicro;
use clay_host::Factory;
use clay_host::Initialized;
use clay_host::LocalFuture;
use clay_host::Root;
use clay_host::StartError;
use clay_host::Supervisor;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use tokio::sync::Notify;

/// What the one fake provider did, and the gates that park it mid-initialization, mid-call
/// or mid-close.
#[derive(Default)]
struct Script {
    inits: AtomicUsize,
    closes: AtomicUsize,
    calls: AtomicUsize,
    gate_init: AtomicBool,
    gate_call: AtomicBool,
    gate_close: AtomicBool,
    init: Notify,
    call: Notify,
    close: Notify,
    /// Stored permits (`notify_one`) that say the provider's initialization or a call has
    /// begun, so a scenario waits on the event itself instead of polling the counters.
    entered_init: Notify,
    entered_call: Notify,
}

impl Script {
    fn count(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }
}

/// The provider's `echo` call contract: a manifest-declared identity, not a bank constant.
fn echo_contract() -> Nsid {
    Nsid::literal("org.example.clay.test.echo")
}

struct Provider(Arc<Script>);
struct ProviderRoot {
    script: Arc<Script>,
    _published: Vec<PublishedCapability>,
}
/// The provider's call handler: echoes the request.
struct Echo(Arc<Script>);

impl Factory<CapabilityPublication> for Provider {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        _requirements: BTreeMap<String, CapabilityPublication>,
        _activator: Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        let script = Arc::clone(&self.0);
        Box::pin(async move {
            script.inits.fetch_add(1, Ordering::SeqCst);
            script.entered_init.notify_one();
            if script.gate_init.load(Ordering::SeqCst) {
                script.init.notified().await;
            }
            let bank = CapabilitySession::new(host, instance, CapabilityLimits::default());
            let echo = bank
                .publish_call("echo", echo_contract(), Arc::new(Echo(Arc::clone(&script))))
                .unwrap();
            let view = bank.publish_presentation("view").unwrap();
            let publications = BTreeMap::from([
                ("echo".to_owned(), echo.publication()),
                ("view".to_owned(), view.publication()),
            ]);
            let root = Box::new(ProviderRoot {
                script,
                _published: vec![echo, view],
            });
            Ok(Initialized { root, publications })
        })
    }
}

impl Root for ProviderRoot {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            self.script.closes.fetch_add(1, Ordering::SeqCst);
            if self.script.gate_close.load(Ordering::SeqCst) {
                self.script.close.notified().await;
            }
            Ok(())
        })
    }
}

impl CallTarget for Echo {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, CallError>> {
        let script = Arc::clone(&self.0);
        async move {
            script.calls.fetch_add(1, Ordering::SeqCst);
            script.entered_call.notify_one();
            if script.gate_call.load(Ordering::SeqCst) {
                script.call.notified().await;
            }
            Ok(request)
        }
        .boxed()
    }
}

struct World {
    host: Host,
    script: Arc<Script>,
    supervisor: Supervisor<CapabilityPublication>,
}

fn provider() -> MicroId {
    MicroId::new("provider")
}

impl World {
    fn new() -> Self {
        let host = Host::new();
        let script = Arc::new(Script::default());
        let micro = ConfiguredMicro {
            identity: provider(),
            eager: false,
            enabled: true,
            requirements: BTreeMap::new(),
            unresolved_bindings: Vec::new(),
            factory: Rc::new(Provider(Arc::clone(&script))),
        };
        Self {
            supervisor: Supervisor::new(host.clone(), [micro]),
            host,
            script,
        }
    }

    /// A fresh recipient incarnation's bank session.
    fn recipient(&self, name: &str) -> CapabilitySession {
        let session = CapabilitySession::new(
            self.host.clone(),
            self.host.start(name),
            CapabilityLimits::default(),
        );
        session.attach_activator(self.supervisor.activator());
        session
    }

    fn lazy(
        &self,
        recipient: &CapabilitySession,
        publication: &str,
        contract: Nsid,
    ) -> CapabilityRef {
        recipient
            .mint_lazy(&PublicationEndpoint {
                micro: provider(),
                publication: publication.to_owned(),
                contract,
            })
            .unwrap()
    }

    fn state(&self) -> MicroState {
        self.supervisor.state(&provider())
    }

    /// Run `scenario` while the supervisor serves activations and idle closes beside it.
    async fn driven<T>(&self, scenario: impl Future<Output = T>) -> T {
        tokio::select! {
            never = self.supervisor.drive() => never,
            out = scenario => out,
        }
    }

    /// Wait until `done` holds, woken by the supervisor's change ticks (it reads recorded
    /// micro states, which tick after the mirror was updated).
    async fn settle(&self, done: impl Fn() -> bool) {
        self.supervisor
            .changes()
            .wait_for(|_| done())
            .await
            .unwrap();
    }

    /// Give the driver every chance to close the provider; it must not while retained.
    async fn stay_live(&self) {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(self.state(), MicroState::Active);
    }
}

#[tokio::test]
async fn reading_a_lazy_status_never_starts_the_provider() {
    let world = World::new();
    let recipient = world.recipient("recipient");
    let reference = world.lazy(&recipient, "echo", echo_contract());
    world
        .driven(async {
            for _ in 0..4 {
                assert_eq!(reference.status(), RefStatus::Dormant);
                tokio::task::yield_now().await;
            }
        })
        .await;
    assert_eq!(Script::count(&world.script.inits), 0);

    let unknown = recipient
        .mint_lazy(&PublicationEndpoint {
            micro: MicroId::new("nobody"),
            publication: "echo".to_owned(),
            contract: echo_contract(),
        })
        .unwrap();
    assert_eq!(unknown.status(), RefStatus::Unavailable);
    world.supervisor.disable(&provider()).await.unwrap();
    assert_eq!(reference.status(), RefStatus::Disabled);
}

// ADR 031 validation 7: calls and a redemption demanded together, from three recipients while
// the provider's initialization is parked, start one incarnation, and each of them gets the
// publication `initialize` returned (the echo answers, the presentation redeems and mounts).
#[tokio::test]
async fn concurrent_lazy_calls_and_redemptions_from_three_recipients_initialize_once() {
    let world = World::new();
    world.script.gate_init.store(true, Ordering::SeqCst);
    let (first, second, third) = (
        world.recipient("first"),
        world.recipient("second"),
        world.recipient("third"),
    );
    let (a, b) = (
        world.lazy(&first, "echo", echo_contract()),
        world.lazy(&second, "echo", echo_contract()),
    );
    let view = world.lazy(&third, "view", presentation_component());
    let (from_a, from_b, redeemed, ()) = world
        .driven(async {
            tokio::join!(
                a.call(b"a".to_vec()),
                b.call(b"b".to_vec()),
                view.redeem_presentation(),
                async {
                    world.script.entered_init.notified().await;
                    assert_eq!(a.status(), RefStatus::Dormant);
                    world.script.init.notify_one();
                }
            )
        })
        .await;
    assert_eq!(from_a, Ok(b"a".to_vec()));
    assert_eq!(from_b, Ok(b"b".to_vec()));
    assert!(redeemed.unwrap().component().is_ok());
    assert_eq!(Script::count(&world.script.inits), 1);
}

#[tokio::test]
async fn a_lazy_call_holds_the_provider_only_for_the_call() {
    let world = World::new();
    world.script.gate_call.store(true, Ordering::SeqCst);
    let recipient = world.recipient("recipient");
    let reference = world.lazy(&recipient, "echo", echo_contract());
    world
        .driven(async {
            let (response, ()) = tokio::join!(reference.call(b"ping".to_vec()), async {
                world.script.entered_call.notified().await;
                // Mid-call nothing else retains the provider, but the call's lease does.
                world.stay_live().await;
                world.script.call.notify_one();
            });
            assert_eq!(response, Ok(b"ping".to_vec()));
            world.settle(|| world.state() == MicroState::Idle).await;
        })
        .await;
    assert_eq!(Script::count(&world.script.closes), 1);
    assert_eq!(reference.status(), RefStatus::Dormant);
}

#[tokio::test]
async fn retained_proxies_keep_the_provider_live_until_dropped() {
    let world = World::new();
    let recipient = world.recipient("recipient");
    let echo = world.lazy(&recipient, "echo", echo_contract());
    let view = world.lazy(&recipient, "view", presentation_component());
    world
        .driven(async {
            let binding = echo.retain_call().await.unwrap();
            world.stay_live().await;
            assert_eq!(binding.call(b"held".to_vec()).await, Ok(b"held".to_vec()));
            drop(binding);
            world.settle(|| world.state() == MicroState::Idle).await;
            assert_eq!(Script::count(&world.script.closes), 1);

            let component = view.redeem_presentation().await.unwrap();
            assert_eq!(Script::count(&world.script.inits), 2);
            world.stay_live().await;
            assert!(component.component().is_ok());
            drop(component);
            world.settle(|| world.state() == MicroState::Idle).await;
            assert_eq!(Script::count(&world.script.closes), 2);
        })
        .await;
}

#[tokio::test]
async fn a_bound_ref_is_revoked_after_its_incarnation_closes_and_never_reactivates() {
    let world = World::new();
    let recipient = world.recipient("recipient");
    let activator = world.supervisor.activator();
    world
        .driven(async {
            let lease = activator.acquire(&provider(), "echo").await.unwrap();
            let bound = recipient.mint(&lease).unwrap();
            assert_eq!(bound.status(), RefStatus::Live);
            assert_eq!(bound.call(b"x".to_vec()).await, Ok(b"x".to_vec()));
            drop(lease);
            world.settle(|| world.state() == MicroState::Idle).await;
            assert_eq!(bound.status(), RefStatus::Revoked);
            assert_eq!(
                bound.call(b"x".to_vec()).await,
                Err(CallError::Capability(CapabilityFailure::Revoked))
            );
            assert_eq!(
                bound.retain_call().await.err(),
                Some(CapabilityFailure::Revoked)
            );
        })
        .await;
    assert_eq!(world.state(), MicroState::Idle);
    assert_eq!(Script::count(&world.script.inits), 1);
}

#[tokio::test]
async fn a_lazy_ref_used_while_the_provider_closes_gets_a_fresh_incarnation() {
    let world = World::new();
    world.script.gate_close.store(true, Ordering::SeqCst);
    let recipient = world.recipient("recipient");
    let reference = world.lazy(&recipient, "echo", echo_contract());
    let activator = world.supervisor.activator();
    world
        .driven(async {
            drop(activator.acquire(&provider(), "echo").await.unwrap());
            world.settle(|| world.state() == MicroState::Closing).await;
            let (response, ()) = tokio::join!(reference.call(b"again".to_vec()), async {
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                }
                assert_eq!(Script::count(&world.script.inits), 1);
                world.script.close.notify_one();
            });
            assert_eq!(response, Ok(b"again".to_vec()));
            assert_eq!(Script::count(&world.script.inits), 2);
        })
        .await;
}

#[tokio::test]
async fn a_wrong_contract_use_fails_without_activating_the_provider() {
    let world = World::new();
    let recipient = world.recipient("recipient");
    // The endpoint promises a call; asking for a presentation is refused before activation.
    let reference = world.lazy(&recipient, "echo", echo_contract());
    world
        .driven(async {
            assert_eq!(
                reference.redeem_presentation().await.err(),
                Some(CapabilityFailure::ContractMismatch)
            );
        })
        .await;
    assert_eq!(Script::count(&world.script.inits), 0);
    assert_eq!(world.state(), MicroState::Idle);
    assert_eq!(recipient.proxy_count(), 0);
}

#[tokio::test]
async fn an_endpoint_naming_no_publication_is_unavailable() {
    let world = World::new();
    let recipient = world.recipient("recipient");
    let reference = world.lazy(&recipient, "absent", echo_contract());
    world
        .driven(async {
            assert_eq!(
                reference.call(Vec::new()).await,
                Err(CallError::Capability(CapabilityFailure::Unavailable))
            );
            let (activator, micro) = (world.supervisor.activator(), provider());
            assert_eq!(
                activator.acquire(&micro, "absent").await.err(),
                Some(StartError::Unpublished {
                    provider: provider(),
                    publication: "absent".to_owned()
                })
            );
            // The provider started for the demand stays up only while retained.
            world.settle(|| world.state() == MicroState::Idle).await;
        })
        .await;
}

#[tokio::test]
async fn an_endpoint_attachment_mints_a_lazy_reference_that_stores_back_as_the_endpoint() {
    let world = World::new();
    let recipient = world.recipient("recipient");
    let attachment = RowAttachment::Endpoint(PublicationEndpoint {
        micro: provider(),
        publication: "echo".to_owned(),
        contract: echo_contract(),
    });
    let reference = recipient.mint_attachment(&attachment).unwrap();
    assert_eq!(reference.attachment(), attachment);
    // Decoding a row activates nothing; only using the reference does.
    assert_eq!(reference.status(), RefStatus::Dormant);
    assert_eq!(Script::count(&world.script.inits), 0);
    let response = world.driven(reference.call(b"ping".to_vec())).await;
    assert_eq!(response, Ok(b"ping".to_vec()));
    assert_eq!(Script::count(&world.script.inits), 1);
}

/// Two supervised incarnations whose sessions retain each other's `echo` through
/// `retain_call`. Each session's activator was handed to its own incarnation, so the lease a
/// retained binding holds is held by that incarnation, not by an outside root.
struct Peers {
    sessions: std::cell::RefCell<BTreeMap<String, CapabilitySession>>,
    /// What each incarnation retains; its root drops them on close, as a guest's teardown does.
    bindings: std::cell::RefCell<BTreeMap<String, Vec<CallBinding>>>,
    closes: AtomicUsize,
}

struct Peer(&'static str, Rc<Peers>);
/// The last field keeps the incarnation's publication leases until the root drops.
struct PeerRoot(
    &'static str,
    Rc<Peers>,
    #[expect(
        dead_code,
        reason = "held for its drop: the leases end with the incarnation"
    )]
    Vec<PublishedCapability>,
);

impl Factory<CapabilityPublication> for Peer {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        _requirements: BTreeMap<String, CapabilityPublication>,
        activator: Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        let (name, peers) = (self.0, Rc::clone(&self.1));
        Box::pin(async move {
            let session = CapabilitySession::new(host, instance, CapabilityLimits::default());
            session.attach_activator(activator);
            let echo = session
                .publish_call("echo", echo_contract(), Arc::new(Echo(Arc::default())))
                .unwrap();
            let publications = BTreeMap::from([("echo".to_owned(), echo.publication())]);
            peers.sessions.borrow_mut().insert(name.to_owned(), session);
            Ok(Initialized {
                root: Box::new(PeerRoot(name, peers, vec![echo])),
                publications,
            })
        })
    }
}

impl Root for PeerRoot {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            self.1.bindings.borrow_mut().remove(self.0);
            self.1.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

// ADR 030 validation 8: retained lazy bindings between two incarnations are edges of the
// retention graph, so the pair is live while an outside lease reaches either and closes as a
// group once none does, instead of keeping itself live.
#[tokio::test]
async fn incarnations_retaining_each_others_calls_close_once_no_outside_lease_reaches_them() {
    let host = Host::new();
    let peers = Rc::new(Peers {
        sessions: Default::default(),
        bindings: Default::default(),
        closes: AtomicUsize::new(0),
    });
    let micro = |name: &'static str| ConfiguredMicro {
        identity: MicroId::new(name),
        eager: false,
        enabled: true,
        requirements: BTreeMap::new(),
        unresolved_bindings: Vec::new(),
        factory: Rc::new(Peer(name, Rc::clone(&peers))),
    };
    let supervisor = Supervisor::new(host, [micro("a"), micro("b")]);
    let both = |state: MicroState| {
        ["a", "b"]
            .iter()
            .all(|name| supervisor.state(&MicroId::new(*name)) == state)
    };
    tokio::select! {
        never = supervisor.drive() => never,
        () = async {
            let from_a = supervisor.acquire(&MicroId::new("a"), "echo").await.unwrap();
            let from_b = supervisor.acquire(&MicroId::new("b"), "echo").await.unwrap();
            for (holder, target) in [("a", "b"), ("b", "a")] {
                let session = peers.sessions.borrow()[holder].clone();
                let reference = session
                    .mint_lazy(&PublicationEndpoint {
                        micro: MicroId::new(target),
                        publication: "echo".to_owned(),
                        contract: echo_contract(),
                    })
                    .unwrap();
                let binding = reference.retain_call().await.unwrap();
                peers.bindings.borrow_mut().entry(holder.to_owned()).or_default().push(binding);
            }
            // The lease on `a` reaches `b` through `a`'s retained binding.
            drop(from_b);
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert!(both(MicroState::Active));
            drop(from_a);
            supervisor.changes().wait_for(|_| both(MicroState::Idle)).await.unwrap();
        } => {}
    }
    assert_eq!(Script::count(&peers.closes), 2);
}
