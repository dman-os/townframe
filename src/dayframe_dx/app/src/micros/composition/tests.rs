use super::*;
use crate::{
    host::AccessError,
    micros::{AsyncMicroFactory, MicroFactory, MicroProvision, Provision},
};
use std::sync::{Arc, Mutex};

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
            .block_on(async {
                loop {
                    let states =
                        self.composition
                            .update(&self.host, &self.registry, &self.desired)?;
                    if self.composition.is_settled() {
                        return Ok(states);
                    }
                    let name = self.composition.next().await;
                    self.composition.complete(name, &self.desired);
                }
            })
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
    assert_eq!(dependent.check(), Err(AccessError::Revoked));
    assert_eq!(source.check(), Err(AccessError::Revoked));
    assert!(fixture.instance("unrelated") == unrelated);
    fixture.desired.get_mut("z-source").unwrap().enablement = Enablement::Enabled;
    assert_eq!(fixture.reconcile(), active_states());
    assert!(fixture.instance("a-dependent") != dependent);
    assert!(fixture.instance("z-source") != source);
    assert!(fixture.instance("unrelated") == unrelated);
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
    assert_eq!(dependent.check(), Err(AccessError::Revoked));
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
    assert_eq!(source.check(), Ok(()));
    assert_eq!(dependent.check(), Ok(()));
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
    assert_eq!(source.check(), Ok(()));
    assert_eq!(dependent.check(), Ok(()));
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
        assert_eq!(instance.check(), Err(AccessError::Revoked));
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
        .register(AsyncMicroFactory::new(
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
    assert!(
        fixture
            .registry
            .output::<Facility>("async")
            .unwrap()
            .0
            .check()
            .is_ok()
    );
}
