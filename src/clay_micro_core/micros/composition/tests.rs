use super::*;
use crate::micros::MicroFactory;
use crate::micros::MicroProvision;
use crate::micros::Provision;
use clay_host::AccessError;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone)]
struct Facility(Instance);
impl Provision for Facility {
    const ID: &'static str = "test.facility";
}

struct Fixture {
    host: Host,
    registry: MicroRegistry,
    composition: Composition,
    desired: BTreeMap<String, ConfiguredMicro>,
    activations: Arc<Mutex<Vec<String>>>,
}

impl Fixture {
    fn new() -> Self {
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let activations = Arc::new(Mutex::new(Vec::new()));
        let declaration = MicroProvision {
            id: Facility::ID.into(),
            version: 1,
        };
        // Deliberately register the consumer first. Registration and configured identity ordering
        // must not decide dependency order.
        for (provider, requirements) in
            [("dependent", vec![declaration.clone()]), ("source", vec![])]
        {
            let log = activations.clone();
            registry
                .register(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new(provider),
                        version: 1,
                        requirements,
                        provisions: vec![declaration.clone()],
                    },
                    move |activation| {
                        if provider == "dependent" {
                            activation.require::<Facility>()?;
                        }
                        log.lock().unwrap().push(activation.instance().name.clone());
                        activation.publish(Facility(activation.instance()));
                        Ok(())
                    },
                ))
                .unwrap();
        }
        let desired = BTreeMap::from([
            (
                "a-dependent".into(),
                configuration(
                    "dependent",
                    BTreeMap::from([(Facility::ID.into(), "z-source".into())]),
                ),
            ),
            ("z-source".into(), configuration("source", BTreeMap::new())),
            ("unrelated".into(), configuration("source", BTreeMap::new())),
        ]);
        Self {
            host,
            registry,
            composition: Composition::default(),
            desired,
            activations,
        }
    }

    fn try_reconcile(&mut self) -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(self.reconcile_async())
    }

    async fn reconcile_async(
        &mut self,
    ) -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
        loop {
            let states = self
                .composition
                .update(&self.host, &self.registry, &self.desired)?;
            if self.composition.is_settled() {
                return Ok(states);
            }
            let name = self.composition.next().await;
            self.composition.complete(name, &self.desired);
        }
    }

    fn reconcile(&mut self) -> BTreeMap<String, InstanceState> {
        self.try_reconcile().unwrap()
    }

    fn instance(&self, name: &str) -> Instance {
        self.registry.output::<Facility>(name).unwrap().0
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.host.close();
    }
}

fn configuration(provider: &str, bindings: BTreeMap<String, String>) -> ConfiguredMicro {
    ConfiguredMicro {
        provider: MicroId::new(provider),
        enablement: Enablement::Enabled,
        bindings,
    }
}

fn active_states() -> BTreeMap<String, InstanceState> {
    BTreeMap::from([
        ("a-dependent".into(), InstanceState::Active),
        ("z-source".into(), InstanceState::Active),
        ("unrelated".into(), InstanceState::Active),
    ])
}

#[test]
fn dependency_order_is_derived_and_unchanged_reconciliation_retains_incarnations() {
    let mut fixture = Fixture::new();
    assert_eq!(fixture.reconcile(), active_states());
    assert_eq!(
        *fixture.activations.lock().unwrap(),
        ["unrelated", "z-source", "a-dependent"]
    );
    let dependent = fixture.instance("a-dependent");
    let source = fixture.instance("z-source");
    assert_eq!(fixture.reconcile(), active_states());
    assert!(fixture.instance("a-dependent") == dependent);
    assert!(fixture.instance("z-source") == source);
    assert_eq!(fixture.activations.lock().unwrap().len(), 3);
}

#[test]
fn disabling_a_provider_blocks_dependents_and_reenabling_restores_fresh_incarnations() {
    let mut fixture = Fixture::new();
    fixture.reconcile();
    let dependent = fixture.instance("a-dependent");
    let source = fixture.instance("z-source");
    let unrelated = fixture.instance("unrelated");
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Disabled;
    assert_eq!(
        fixture.reconcile(),
        BTreeMap::from([
            (
                "a-dependent".into(),
                InstanceState::Blocked {
                    requirements: vec![Facility::ID.into()]
                }
            ),
            ("z-source".into(), InstanceState::Disabled),
            ("unrelated".into(), InstanceState::Active),
        ])
    );
    assert_eq!(fixture.host.check(&dependent), Err(AccessError::Revoked));
    assert_eq!(fixture.host.check(&source), Err(AccessError::Revoked));
    assert!(fixture.instance("unrelated") == unrelated);
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Enabled;
    assert_eq!(fixture.reconcile(), active_states());
    assert!(fixture.instance("a-dependent") != dependent);
    assert!(fixture.instance("z-source") != source);
    assert!(fixture.instance("unrelated") == unrelated);
}

#[test]
fn terminal_observers_distinguish_requested_stop_from_dependency_withdrawal() {
    let mut fixture = Fixture::new();
    fixture.reconcile();
    let source = fixture.instance("z-source");
    let dependent = fixture.instance("a-dependent");
    fixture.host.close_instance(&source).unwrap();
    assert!(matches!(
        fixture.host.observe_instance(&source),
        Err(AccessError::Revoked)
    ));
    let mut stopped = active_states();
    stopped.insert("z-source".into(), InstanceState::Stopped);
    stopped.insert(
        "a-dependent".into(),
        InstanceState::Blocked {
            requirements: vec![Facility::ID.into()],
        },
    );
    assert_eq!(fixture.reconcile(), stopped);
    assert_eq!(fixture.reconcile(), stopped);
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Disabled;
    fixture.reconcile();
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Enabled;
    assert_eq!(fixture.reconcile(), active_states());
    let replacement = fixture.instance("z-source");
    let replacement_dependent = fixture.instance("a-dependent");
    assert_ne!(source, replacement);
    assert_ne!(dependent, replacement_dependent);
    fixture.host.close_instance(&source).unwrap();
    fixture.host.close_instance(&dependent).unwrap();
    assert_eq!(fixture.reconcile(), active_states());
    assert_eq!(fixture.instance("z-source"), replacement);
    assert_eq!(fixture.instance("a-dependent"), replacement_dependent);
}

#[test]
fn pending_observer_records_closure_before_first_poll() {
    let mut fixture = Fixture::new();
    fixture
        .composition
        .update(&fixture.host, &fixture.registry, &fixture.desired)
        .unwrap();
    let source = fixture.composition.pending["z-source"]
        .activation
        .instance
        .clone();
    fixture.host.close_instance(&source).unwrap();
    let states = fixture.reconcile();
    assert_eq!(states["z-source"], InstanceState::Stopped);
    assert_eq!(
        states["a-dependent"],
        InstanceState::Blocked {
            requirements: vec![Facility::ID.into()],
        }
    );
    assert_eq!(*fixture.activations.lock().unwrap(), ["unrelated"]);
    assert!(fixture.composition.pending.is_empty());
}

#[test]
fn closure_inside_activation_is_observed_before_completion() {
    let mut fixture = Fixture::new();
    let host = fixture.host.clone();
    fixture
        .registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("self-closing"),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| host.close_instance(&activation.instance()),
        ))
        .unwrap();
    fixture.desired = BTreeMap::from([(
        "self-closing".into(),
        configuration("self-closing", BTreeMap::new()),
    )]);
    assert_eq!(
        fixture.reconcile(),
        BTreeMap::from([("self-closing".into(), InstanceState::Stopped)])
    );
    assert!(fixture.composition.active.is_empty());
    assert!(fixture.composition.pending.is_empty());
    fixture.desired.clear();
    assert!(fixture.reconcile().is_empty());
    assert!(fixture.composition.stopped.is_empty());
}

#[tokio::test]
async fn pending_completion_retains_observer_until_concurrent_cleanup_finishes() {
    use std::future::Future;
    use std::task::Poll;

    let mut fixture = Fixture::new();
    fixture.desired.retain(|name, _| name == "z-source");
    fixture
        .composition
        .update(&fixture.host, &fixture.registry, &fixture.desired)
        .unwrap();
    let name = fixture.composition.next().await;
    let instance = fixture.composition.pending[&name]
        .activation
        .instance
        .clone();
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    fixture
        .host
        .own(
            &instance,
            Box::new(move || {
                entered.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        )
        .unwrap();
    let host = fixture.host.clone();
    let closing = std::thread::spawn(move || host.close_instance(&instance).unwrap());
    entered_rx.await.unwrap();
    fixture.composition.complete(name, &fixture.desired);
    assert!(!fixture.composition.is_settled());
    let mut next = Box::pin(fixture.composition.next());
    std::future::poll_fn(|context| {
        assert!(next.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    release.send(()).unwrap();
    let name = next.await;
    closing.join().unwrap();
    fixture.composition.complete(name, &fixture.desired);
    assert_eq!(
        fixture
            .composition
            .update(&fixture.host, &fixture.registry, &fixture.desired)
            .unwrap(),
        BTreeMap::from([("z-source".into(), InstanceState::Stopped)])
    );
    assert!(fixture.composition.active.is_empty());
    assert!(fixture.composition.pending.is_empty());
}

async fn block_close_driver(
    host: &Host,
) -> (std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let blocker = host.start("close-driver-blocker");
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    host.own(
        &blocker,
        Box::new(move || {
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        }),
    )
    .unwrap();
    let host = host.clone();
    let thread = std::thread::spawn(move || host.close_instance(&blocker).unwrap());
    entered_rx.await.unwrap();
    (release, thread)
}

#[tokio::test]
async fn settlement_waits_for_retiring_incarnation_under_concurrent_close() {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    struct WakeSignal(tokio::sync::mpsc::UnboundedSender<()>);
    impl Wake for WakeSignal {
        fn wake(self: Arc<Self>) {
            self.0.send(()).unwrap();
        }
    }

    let mut fixture = Fixture::new();
    fixture.desired.retain(|name, _| name == "z-source");
    fixture.reconcile_async().await.unwrap();
    let original = fixture.instance("z-source");
    let ended = fixture.host.observe_instance(&original).unwrap();
    let (release, thread) = block_close_driver(&fixture.host).await;
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Disabled;
    assert_eq!(
        fixture
            .composition
            .update(&fixture.host, &fixture.registry, &fixture.desired)
            .unwrap(),
        BTreeMap::from([("z-source".into(), InstanceState::Disabled)])
    );
    assert_eq!(*ended.borrow(), None);
    assert!(
        !fixture.composition.is_settled(),
        "settlement overtook retiring incarnation cleanup"
    );
    let (wake_tx, mut wake_rx) = tokio::sync::mpsc::unbounded_channel();
    let waker = Waker::from(Arc::new(WakeSignal(wake_tx)));
    {
        let mut next = Box::pin(fixture.composition.next());
        assert!(
            next.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Enabled;
    assert_eq!(
        fixture
            .composition
            .update(&fixture.host, &fixture.registry, &fixture.desired)
            .unwrap(),
        BTreeMap::from([("z-source".into(), InstanceState::Activating)])
    );
    assert!(fixture.composition.pending.is_empty());
    assert_eq!(*fixture.activations.lock().unwrap(), ["z-source"]);
    let mut next = Box::pin(fixture.composition.next());
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        wake_rx.try_recv().is_err(),
        "waiting for cleanup must not self-wake"
    );
    release.send(()).unwrap();
    wake_rx.recv().await.unwrap();
    thread.join().unwrap();
    let Poll::Ready(name) = next.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("terminal cleanup did not wake reconciliation");
    };
    drop(next);
    fixture.composition.complete(name, &fixture.desired);
    assert_eq!(
        fixture.reconcile_async().await.unwrap(),
        BTreeMap::from([("z-source".into(), InstanceState::Active)])
    );
    assert_ne!(fixture.instance("z-source"), original);
    assert_eq!(*ended.borrow(), Some(CloseReason::Requested));
    assert_eq!(
        *fixture.activations.lock().unwrap(),
        ["z-source", "z-source"]
    );
    assert!(fixture.composition.closing.is_empty());
}

#[tokio::test]
async fn settlement_retains_cancelled_activation_until_terminal_before_first_poll() {
    for cancel_pending in [false, true] {
        let mut fixture = Fixture::new();
        fixture.desired.retain(|name, _| name == "z-source");
        fixture
            .composition
            .update(&fixture.host, &fixture.registry, &fixture.desired)
            .unwrap();
        let name = fixture.composition.next().await;
        let original = fixture.composition.pending[&name]
            .activation
            .instance
            .clone();
        let ended = fixture.host.observe_instance(&original).unwrap();
        let (release, thread) = block_close_driver(&fixture.host).await;
        if cancel_pending {
            fixture.composition.cancel_pending();
        }
        fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Disabled;
        fixture
            .composition
            .update(&fixture.host, &fixture.registry, &fixture.desired)
            .unwrap();
        assert_eq!(*ended.borrow(), None);
        assert!(
            !fixture.composition.is_settled(),
            "cancelled activation cleanup is still outstanding"
        );
        fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Enabled;
        assert_eq!(
            fixture
                .composition
                .update(&fixture.host, &fixture.registry, &fixture.desired)
                .unwrap(),
            BTreeMap::from([("z-source".into(), InstanceState::Activating)])
        );
        assert!(fixture.composition.pending.is_empty());
        fixture.desired.clear();
        fixture
            .composition
            .update(&fixture.host, &fixture.registry, &fixture.desired)
            .unwrap();
        assert!(!fixture.composition.is_settled());
        release.send(()).unwrap();
        thread.join().unwrap();
        let name = fixture.composition.next().await;
        fixture.composition.complete(name, &fixture.desired);
        assert!(fixture.reconcile_async().await.unwrap().is_empty());
        assert!(fixture.composition.closing.is_empty());
        assert_eq!(*ended.borrow(), Some(CloseReason::Requested));
        assert_eq!(*fixture.activations.lock().unwrap(), ["z-source"]);
    }
}

#[test]
fn rebinding_restarts_only_the_consumer_and_removing_its_supplier_blocks_it() {
    let mut fixture = Fixture::new();
    fixture.reconcile();
    let dependent = fixture.instance("a-dependent");
    let original_source = fixture.instance("z-source");
    let other_source = fixture.instance("unrelated");
    fixture
        .desired
        .get_mut("a-dependent")
        .unwrap()
        .bindings
        .insert(Facility::ID.into(), "unrelated".into());
    assert_eq!(fixture.reconcile(), active_states());
    assert_eq!(fixture.host.check(&dependent), Err(AccessError::Revoked));
    assert!(fixture.instance("a-dependent") != dependent);
    assert!(fixture.instance("z-source") == original_source);
    assert!(fixture.instance("unrelated") == other_source);
    fixture.desired.remove("unrelated");
    assert_eq!(
        fixture.reconcile(),
        BTreeMap::from([
            (
                "a-dependent".into(),
                InstanceState::Blocked {
                    requirements: vec![Facility::ID.into()]
                }
            ),
            ("z-source".into(), InstanceState::Active),
        ])
    );
}

#[test]
fn invalid_edits_are_rejected_before_live_instances_are_withdrawn() {
    let mut fixture = Fixture::new();
    fixture.reconcile();
    let source = fixture.instance("z-source");
    let dependent = fixture.instance("a-dependent");
    fixture
        .desired
        .get_mut("a-dependent")
        .unwrap()
        .bindings
        .clear();
    assert_eq!(
        fixture.try_reconcile(),
        Err(ConfigurationError::MissingBinding {
            instance: "a-dependent".into(),
            contract: Facility::ID.into()
        })
    );
    assert_eq!(fixture.host.check(&source), Ok(()));
    assert_eq!(fixture.host.check(&dependent), Ok(()));
    fixture
        .desired
        .get_mut("a-dependent")
        .unwrap()
        .bindings
        .insert(Facility::ID.into(), "z-source".into());
    fixture.desired.insert(
        "z-source".into(),
        configuration(
            "dependent",
            BTreeMap::from([(Facility::ID.into(), "a-dependent".into())]),
        ),
    );
    assert_eq!(
        fixture.try_reconcile(),
        Err(ConfigurationError::Cycle(vec![
            "a-dependent".into(),
            "z-source".into()
        ]))
    );
    assert_eq!(fixture.host.check(&source), Ok(()));
    assert_eq!(fixture.host.check(&dependent), Ok(()));
}

#[test]
fn unavailable_implementations_remain_inspectable_and_activate_when_registered() {
    let mut fixture = Fixture::new();
    fixture.desired.get_mut("z-source").unwrap().provider = MicroId::new("later");
    assert_eq!(
        fixture.reconcile(),
        BTreeMap::from([
            (
                "a-dependent".into(),
                InstanceState::Blocked {
                    requirements: vec![Facility::ID.into()]
                }
            ),
            (
                "z-source".into(),
                InstanceState::UnavailableProvider(MicroId::new("later"))
            ),
            ("unrelated".into(), InstanceState::Active),
        ])
    );
    let mut registrar = fixture.registry.clone();
    registrar
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("later"),
                version: 1,
                requirements: vec![],
                provisions: vec![MicroProvision {
                    id: Facility::ID.into(),
                    version: 1,
                }],
            },
            |activation| {
                activation.publish(Facility(activation.instance()));
                Ok(())
            },
        ))
        .unwrap();
    assert_eq!(fixture.reconcile(), active_states());
}

#[test]
fn failed_activations_are_not_retried_on_unrelated_edits_but_new_inputs_allow_retry() {
    let mut fixture = Fixture::new();
    let attempts = Arc::new(Mutex::new(0));
    let counted = attempts.clone();
    fixture
        .registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("failing"),
                version: 1,
                requirements: vec![MicroProvision {
                    id: Facility::ID.into(),
                    version: 1,
                }],
                provisions: vec![],
            },
            move |_| {
                *counted.lock().unwrap() += 1;
                Err(AccessError::Denied)
            },
        ))
        .unwrap();
    fixture.desired.get_mut("a-dependent").unwrap().provider = MicroId::new("failing");
    let mut expected = active_states();
    expected.insert(
        "a-dependent".into(),
        InstanceState::Failed(MicroRegistryError::Access(AccessError::Denied)),
    );
    assert_eq!(fixture.reconcile(), expected);
    fixture
        .desired
        .insert("extra".into(), configuration("source", BTreeMap::new()));
    expected.insert("extra".into(), InstanceState::Active);
    assert_eq!(fixture.reconcile(), expected);
    assert_eq!(*attempts.lock().unwrap(), 1);
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Disabled;
    fixture.reconcile();
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Enabled;
    assert_eq!(fixture.reconcile(), expected);
    assert_eq!(*attempts.lock().unwrap(), 2);
    fixture.desired.get_mut("a-dependent").unwrap().enablement = Enablement::Disabled;
    fixture.reconcile();
    fixture.desired.get_mut("a-dependent").unwrap().enablement = Enablement::Enabled;
    assert_eq!(fixture.reconcile(), expected);
    assert_eq!(*attempts.lock().unwrap(), 3);
}

#[test]
fn unfulfilled_provider_outputs_fail_the_provider_and_block_consumers_without_retrying() {
    let mut fixture = Fixture::new();
    let attempts = Arc::new(Mutex::new(0));
    let counted = attempts.clone();
    fixture
        .registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("unfulfilled"),
                version: 1,
                requirements: vec![],
                provisions: vec![MicroProvision {
                    id: Facility::ID.into(),
                    version: 1,
                }],
            },
            move |_| {
                *counted.lock().unwrap() += 1;
                Ok(())
            },
        ))
        .unwrap();
    fixture.desired.get_mut("z-source").unwrap().provider = MicroId::new("unfulfilled");
    let expected = BTreeMap::from([
        (
            "a-dependent".into(),
            InstanceState::Blocked {
                requirements: vec![Facility::ID.into()],
            },
        ),
        (
            "z-source".into(),
            InstanceState::Failed(MicroRegistryError::MissingProvision(Facility::ID.into())),
        ),
        ("unrelated".into(), InstanceState::Active),
    ]);
    assert_eq!(fixture.reconcile(), expected);
    assert_eq!(fixture.reconcile(), expected);
    assert_eq!(*attempts.lock().unwrap(), 1);
    assert!(matches!(
        fixture.registry.output::<Facility>("z-source"),
        Err(AccessError::Revoked)
    ));
}

#[test]
fn clearing_configuration_withdraws_all_instances_and_their_provisions() {
    let mut fixture = Fixture::new();
    fixture.reconcile();
    let instances: Vec<_> = fixture
        .desired
        .keys()
        .map(|name| fixture.instance(name))
        .collect();
    fixture.desired.clear();
    assert_eq!(fixture.reconcile(), BTreeMap::new());
    for instance in instances {
        assert_eq!(fixture.host.check(&instance), Err(AccessError::Revoked));
        assert!(matches!(
            fixture.registry.output::<Facility>(&instance.name),
            Err(AccessError::Revoked)
        ));
    }
    assert_eq!(fixture.reconcile(), BTreeMap::new());
}

#[test]
fn async_provider_activates_through_reconciliation() {
    let mut fixture = Fixture::new();
    fixture
        .registry
        .register(MicroFactory::new_async(
            MicroDescriptor {
                id: MicroId::new("async-source"),
                version: 1,
                requirements: vec![],
                provisions: vec![MicroProvision {
                    id: Facility::ID.into(),
                    version: 1,
                }],
            },
            |activation| async move {
                tokio::task::yield_now().await;
                activation.publish(Facility(activation.instance()));
                Ok(())
            },
        ))
        .unwrap();
    fixture.desired.insert(
        "async".into(),
        configuration("async-source", BTreeMap::new()),
    );
    assert_eq!(
        fixture.reconcile().get("async"),
        Some(&InstanceState::Active)
    );
    assert!(fixture.host.check(&fixture.instance("async")).is_ok());
}

// ── Metamorphic property: settled states are a pure function of the input ────

const NAMES: [&str; 4] = ["a", "b", "c", "d"];

#[derive(Debug, Clone)]
struct EntryGen {
    present: bool,
    dependent: bool,
    enabled: bool,
    rank: u8,
    bind_sel: u8,
}

#[derive(Debug, Clone)]
enum EditGen {
    Toggle(u8),
    Remove(u8),
    Add {
        slot: u8,
        dependent: bool,
        enabled: bool,
        bind_sel: u8,
    },
    Rebind {
        slot: u8,
        bind_sel: u8,
    },
}

fn entry_gen() -> impl proptest::strategy::Strategy<Value = EntryGen> {
    use proptest::prelude::*;
    (
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<u8>(),
        any::<u8>(),
    )
        .prop_map(|(present, dependent, enabled, rank, bind_sel)| EntryGen {
            present,
            dependent,
            enabled,
            rank,
            bind_sel,
        })
}

fn edit_gen() -> impl proptest::strategy::Strategy<Value = EditGen> {
    use proptest::prelude::*;
    prop_oneof![
        2 => (0u8..4).prop_map(EditGen::Toggle),
        1 => (0u8..4).prop_map(EditGen::Remove),
        2 => (0u8..4, any::<bool>(), any::<bool>(), any::<u8>()).prop_map(
            |(slot, dependent, enabled, bind_sel)| EditGen::Add { slot, dependent, enabled, bind_sel }
        ),
        2 => (0u8..4, any::<u8>()).prop_map(|(slot, bind_sel)| EditGen::Rebind { slot, bind_sel }),
    ]
}

/// Registry with the two-provider vocabulary the model reasons about: `source` provides the
/// contract, `dependent` requires and re-provides it, so generated graphs can chain.
fn metamorphic_registry(activations: Arc<Mutex<Vec<String>>>) -> MicroRegistry {
    let mut registry = MicroRegistry::default();
    let declaration = MicroProvision {
        id: Facility::ID.into(),
        version: 1,
    };
    for (provider, requirements) in [("dependent", vec![declaration.clone()]), ("source", vec![])] {
        let log = activations.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: MicroId::new(provider),
                    version: 1,
                    requirements,
                    provisions: vec![declaration.clone()],
                },
                move |activation| {
                    if provider == "dependent" {
                        activation.require::<Facility>()?;
                    }
                    log.lock().unwrap().push(activation.instance().name.clone());
                    activation.publish(Facility(activation.instance()));
                    Ok(())
                },
            ))
            .unwrap();
    }
    registry
}

/// Names reachable from `supplier` through binding edges (its transitive suppliers).
fn reachable(desired: &BTreeMap<String, ConfiguredMicro>, supplier: &str) -> BTreeSet<String> {
    let mut reached = BTreeSet::new();
    let mut frontier = vec![supplier.to_owned()];
    while let Some(name) = frontier.pop() {
        if reached.insert(name.clone())
            && let Some(configuration) = desired.get(&name)
        {
            frontier.extend(configuration.bindings.values().cloned());
        }
    }
    reached
}

/// Lower a generated entry vector into a valid desired map: bindings only ever point at
/// strictly later names in the generated rank order, which makes cycles unrepresentable; a
/// dependent with no valid supplier degrades to `source`.
fn build_desired(entries: &[EntryGen]) -> BTreeMap<String, ConfiguredMicro> {
    let mut present: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.present)
        .map(|(index, _)| index)
        .collect();
    present.sort_by_key(|&index| (entries[index].rank, index));
    let mut desired = BTreeMap::new();
    for (position, &index) in present.iter().enumerate() {
        let entry = &entries[index];
        let mut bindings = BTreeMap::new();
        let mut provider = "source";
        if entry.dependent {
            let candidates = &present[position + 1..];
            if !candidates.is_empty() {
                let supplier = candidates[entry.bind_sel as usize % candidates.len()];
                bindings.insert(Facility::ID.to_owned(), NAMES[supplier].to_owned());
                provider = "dependent";
            }
        }
        desired.insert(
            NAMES[index].to_owned(),
            ConfiguredMicro {
                provider: MicroId::new(provider),
                enablement: if entry.enabled {
                    Enablement::Enabled
                } else {
                    Enablement::Disabled
                },
                bindings,
            },
        );
    }
    desired
}

/// The pure model: enabled names whose binding suppliers are (transitively) active settle
/// Active; the rest of the enabled settle Blocked on the unsatisfied contract.
fn model_states(desired: &BTreeMap<String, ConfiguredMicro>) -> BTreeMap<String, InstanceState> {
    let mut active = BTreeSet::new();
    loop {
        let mut changed = false;
        for (name, configuration) in desired {
            if configuration.enablement == Enablement::Disabled || active.contains(name) {
                continue;
            }
            if configuration
                .bindings
                .values()
                .all(|supplier| active.contains(supplier))
            {
                active.insert(name.clone());
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    desired
        .iter()
        .map(|(name, configuration)| {
            let state = if configuration.enablement == Enablement::Disabled {
                InstanceState::Disabled
            } else if active.contains(name) {
                InstanceState::Active
            } else {
                InstanceState::Blocked {
                    requirements: configuration
                        .bindings
                        .iter()
                        .filter(|(_, supplier)| !active.contains(*supplier))
                        .map(|(contract, _)| contract.clone())
                        .collect(),
                }
            };
            (name.clone(), state)
        })
        .collect()
}

struct Metamorphic {
    host: Host,
    registry: MicroRegistry,
    composition: Composition,
    activations: Arc<Mutex<Vec<String>>>,
}

impl Metamorphic {
    fn new() -> Self {
        let activations = Arc::new(Mutex::new(Vec::new()));
        Self {
            host: Host::new(),
            registry: metamorphic_registry(activations.clone()),
            composition: Composition::default(),
            activations,
        }
    }

    async fn settle(
        &mut self,
        desired: &BTreeMap<String, ConfiguredMicro>,
    ) -> BTreeMap<String, InstanceState> {
        loop {
            let states = self
                .composition
                .update(&self.host, &self.registry, desired)
                .unwrap();
            if self.composition.is_settled() {
                return states;
            }
            let name = self.composition.next().await;
            self.composition.complete(name, desired);
        }
    }

    /// Pin the metamorphic core: settled states equal the pure model, a second reconcile is a
    /// no-op (no new activations, incarnations retained), and liveness in the host matches.
    async fn verify(&mut self, desired: &BTreeMap<String, ConfiguredMicro>) {
        let expected = model_states(desired);
        let states = self.settle(desired).await;
        assert_eq!(states, expected);
        let instances: BTreeMap<String, Instance> = states
            .iter()
            .filter(|(_, state)| **state == InstanceState::Active)
            .map(|(name, _)| {
                (
                    name.clone(),
                    self.registry.output::<Facility>(name).unwrap().0,
                )
            })
            .collect();
        let activations_before = self.activations.lock().unwrap().len();
        let second = self.settle(desired).await;
        assert_eq!(second, states);
        assert_eq!(
            self.activations.lock().unwrap().len(),
            activations_before,
            "unchanged reconciliation restarted an incarnation"
        );
        for (name, instance) in &instances {
            assert_eq!(
                &self.registry.output::<Facility>(name).unwrap().0,
                instance,
                "unchanged reconciliation replaced an incarnation"
            );
            assert_eq!(self.host.check(instance), Ok(()));
        }
        for (name, state) in &states {
            if *state != InstanceState::Active {
                assert!(
                    matches!(
                        self.registry.output::<Facility>(name),
                        Err(AccessError::Revoked)
                    ),
                    "non-active instance retains a live provision"
                );
            }
        }
    }
}

fn apply_edit(desired: &mut BTreeMap<String, ConfiguredMicro>, edit: &EditGen) {
    match *edit {
        EditGen::Toggle(slot) => {
            if let Some(configuration) = desired.get_mut(NAMES[slot as usize % 4]) {
                configuration.enablement = match configuration.enablement {
                    Enablement::Enabled => Enablement::Disabled,
                    Enablement::Disabled => Enablement::Enabled,
                };
            }
        }
        EditGen::Remove(slot) => {
            // Dangling bindings are legal: they settle Blocked, not an error.
            desired.remove(NAMES[slot as usize % 4]);
        }
        EditGen::Add {
            slot,
            dependent,
            enabled,
            bind_sel,
        } => {
            let name = NAMES[slot as usize % 4];
            if desired.contains_key(name) {
                return;
            }
            let mut bindings = BTreeMap::new();
            let mut provider = "source";
            // A removed name can still be the target of dangling bindings (Remove leaves them,
            // settling Blocked), so re-adding it does not guarantee acyclicity: binding the new
            // node to a candidate that can already reach it through those edges closes a cycle.
            // Exclude any candidate that can reach the fresh name, matching Rebind.
            let candidates: Vec<String> = desired
                .keys()
                .filter(|candidate| !reachable(desired, candidate).contains(name))
                .cloned()
                .collect();
            if dependent && !candidates.is_empty() {
                let supplier = candidates[bind_sel as usize % candidates.len()].clone();
                bindings.insert(Facility::ID.to_owned(), supplier);
                provider = "dependent";
            }
            desired.insert(
                name.to_owned(),
                ConfiguredMicro {
                    provider: MicroId::new(provider),
                    enablement: if enabled {
                        Enablement::Enabled
                    } else {
                        Enablement::Disabled
                    },
                    bindings,
                },
            );
        }
        EditGen::Rebind { slot, bind_sel } => {
            let name = NAMES[slot as usize % 4];
            let Some(configuration) = desired.get(name) else {
                return;
            };
            if configuration.bindings.is_empty() {
                return;
            }
            // Rebinding consumer→supplier closes a cycle iff the supplier can already reach the
            // consumer through binding edges.
            let candidates: Vec<String> = desired
                .keys()
                .filter(|candidate| {
                    *candidate != name && !reachable(desired, candidate).contains(name)
                })
                .cloned()
                .collect();
            if candidates.is_empty() {
                return;
            }
            let supplier = candidates[bind_sel as usize % candidates.len()].clone();
            for binding in desired.get_mut(name).unwrap().bindings.values_mut() {
                *binding = supplier.clone();
            }
        }
    }
}

async fn run_metamorphic_script(entries: Vec<EntryGen>, edits: Vec<EditGen>) {
    let mut harness = Metamorphic::new();
    let mut desired = build_desired(&entries);
    harness.verify(&desired).await;
    for edit in &edits {
        apply_edit(&mut desired, edit);
        harness.verify(&desired).await;
    }
    harness.host.close();
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 32,
        ..Default::default()
    })]
    #[test]
    fn settled_reconciliation_matches_the_model_and_never_restarts_unchanged_incarnations(
        entries in proptest::collection::vec(entry_gen(), 4..=4),
        edits in proptest::collection::vec(edit_gen(), 0..4),
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_metamorphic_script(entries, edits));
    }
}

// ── Wire format: persisted configuration shape is a settled contract ─────────

fn configured_micro_gen() -> impl proptest::strategy::Strategy<Value = ConfiguredMicro> {
    use proptest::prelude::*;
    (
        prop::sample::select(vec!["source", "dependent", "dayframe.settings"]),
        any::<bool>(),
        prop::collection::btree_map(
            prop::sample::select(vec!["test.facility", "dual.alpha"]),
            prop::sample::select(vec!["a", "b", "supplier"]),
            0..3,
        ),
    )
        .prop_map(
            |(provider, enabled, bindings): (&str, bool, BTreeMap<&str, &str>)| ConfiguredMicro {
                provider: MicroId::new(provider),
                enablement: if enabled {
                    Enablement::Enabled
                } else {
                    Enablement::Disabled
                },
                bindings: bindings
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .collect(),
            },
        )
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 64,
        ..Default::default()
    })]
    #[test]
    fn configured_micro_serde_roundtrips(configuration in configured_micro_gen()) {
        let value = serde_json::to_value(&configuration).unwrap();
        let back: ConfiguredMicro = serde_json::from_value(value).unwrap();
        assert_eq!(back, configuration);
    }

    #[test]
    fn micro_id_serde_roundtrips(id in "[a-z][a-z0-9.]{0,16}") {
        let id = MicroId::new(id);
        let value = serde_json::to_value(&id).unwrap();
        let back: MicroId = serde_json::from_value(value).unwrap();
        assert_eq!(back, id);
    }
}

/// The facet-persisted shape of a desired composition. Configuration documents outlive binary
/// releases, so silent format drift here is a breaking change for stored state.
#[test]
fn persisted_composition_shape_is_pinned() {
    let desired = BTreeMap::from([
        (
            "consumer".to_owned(),
            ConfiguredMicro {
                provider: MicroId::new("dependent"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::from([("test.facility".to_owned(), "supplier".to_owned())]),
            },
        ),
        (
            "supplier".to_owned(),
            ConfiguredMicro {
                provider: MicroId::new("source"),
                enablement: Enablement::Disabled,
                bindings: BTreeMap::new(),
            },
        ),
    ]);
    insta::assert_yaml_snapshot!(serde_json::to_value(&desired).unwrap());
}
