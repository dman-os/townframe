use super::*;
use crate::{
    host::{AccessError, CloseReason},
    micros::composition::Enablement,
    micros::{MicroFactory, MicroId, MicroProvision, Provision, ProvisionKind},
};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Facility(Instance);
impl Provision for Facility {
    const ID: &'static str = "test.facility";
}

struct Fixture {
    runtime: ApplicationRuntime,
    desired: BTreeMap<String, ConfiguredMicro>,
    reports:
        tokio::sync::watch::Receiver<Result<BTreeMap<String, InstanceState>, ConfigurationError>>,
    activations: Arc<Mutex<Vec<String>>>,
}

impl Fixture {
    fn new() -> Self {
        let mut runtime = ApplicationRuntime::new();
        let activations = Arc::new(Mutex::new(Vec::new()));
        let counted = activations.clone();
        runtime
            .register_provider(MicroFactory::new(
                MicroDescriptor {
                    id: MicroId::new("consumer"),
                    version: 1,
                    requirements: vec![declaration()],
                    provisions: vec![declaration()],
                },
                move |activation| {
                    activation.require::<Facility>()?;
                    counted.lock().unwrap().push("consumer".into());
                    activation.publish(Facility(activation.instance()));
                    Ok(())
                },
            ))
            .unwrap();
        let desired = BTreeMap::from([
            (
                "consumer".into(),
                ConfiguredMicro {
                    provider: MicroId::new("consumer"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
                },
            ),
            (
                "supplier".into(),
                ConfiguredMicro {
                    provider: MicroId::new("supplier"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            ),
        ]);
        runtime.configure(&desired).unwrap();
        let reports = runtime.reports();
        Self {
            runtime,
            desired,
            reports,
            activations,
        }
    }

    fn register_supplier(&mut self) {
        let counted = self.activations.clone();
        self.runtime
            .register_provider(MicroFactory::new(
                MicroDescriptor {
                    id: MicroId::new("supplier"),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![declaration()],
                },
                move |activation| {
                    counted.lock().unwrap().push("supplier".into());
                    activation.publish(Facility(activation.instance()));
                    Ok(())
                },
            ))
            .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.runtime.host().close();
    }
}

fn declaration() -> MicroProvision {
    MicroProvision {
        id: Facility::ID.into(),
        version: 1,
        kind: ProvisionKind::Context,
    }
}

fn active() -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
    Ok(BTreeMap::from([
        ("consumer".into(), InstanceState::Active),
        ("supplier".into(), InstanceState::Active),
    ]))
}

#[tokio::test]
async fn late_registration_activates_blocked_instances_without_manual_reconciliation() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new();
            fixture.reports.borrow_and_update();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture.reports.changed().await.unwrap();
            assert_eq!(
                fixture.reports.borrow_and_update().clone(),
                Ok(BTreeMap::from([
                    (
                        "consumer".into(),
                        InstanceState::Blocked {
                            requirements: vec![Facility::ID.into()]
                        }
                    ),
                    (
                        "supplier".into(),
                        InstanceState::UnavailableProvider(MicroId::new("supplier"))
                    ),
                ]))
            );
            // There is deliberately no configure/reconcile call after registration.
            fixture.register_supplier();
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer"]
            );
            assert_eq!(
                fixture
                    .runtime
                    .output::<Facility>("consumer")
                    .unwrap()
                    .0
                    .check(),
                Ok(())
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
            assert_eq!(
                fixture.reports.borrow().clone(),
                Ok(BTreeMap::from([
                    ("consumer".into(), InstanceState::Stopped),
                    ("supplier".into(), InstanceState::Stopped),
                ]))
            );
        })
        .await;
}

#[tokio::test]
async fn registration_before_observer_first_poll_is_not_lost() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new();
            fixture.register_supplier();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer"]
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn independent_stop_withdraws_dependents_but_does_not_restart_the_provider() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new();
            fixture.register_supplier();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            let supplier = fixture.runtime.output::<Facility>("supplier").unwrap().0;
            let consumer = fixture.runtime.output::<Facility>("consumer").unwrap().0;
            supplier.close();
            let stopped = Ok(BTreeMap::from([
                (
                    "consumer".into(),
                    InstanceState::Blocked {
                        requirements: vec![Facility::ID.into()],
                    },
                ),
                ("supplier".into(), InstanceState::Stopped),
            ]));
            fixture
                .reports
                .wait_for(|report| report == &stopped)
                .await
                .unwrap();
            assert_eq!(supplier.close_reason(), Some(CloseReason::Requested));
            assert_eq!(
                consumer.close_reason(),
                Some(CloseReason::DependencyWithdrawn)
            );
            assert_eq!(consumer.check(), Err(AccessError::Revoked));
            // Force another observation pass without changing the stopped instance's inputs.
            fixture.reports.borrow_and_update();
            fixture
                .runtime
                .register_provider(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new("unrelated"),
                        version: 1,
                        requirements: vec![],
                        provisions: vec![],
                    },
                    |_| Ok(()),
                ))
                .unwrap();
            fixture.reports.changed().await.unwrap();
            assert_eq!(fixture.reports.borrow_and_update().clone(), stopped);
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer"]
            );
            fixture.desired.get_mut("supplier").unwrap().enablement = Enablement::Disabled;
            fixture.runtime.configure(&fixture.desired).unwrap();
            fixture.desired.get_mut("supplier").unwrap().enablement = Enablement::Enabled;
            fixture.runtime.configure(&fixture.desired).unwrap();
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert!(fixture.runtime.output::<Facility>("supplier").unwrap().0 != supplier);
            assert!(fixture.runtime.output::<Facility>("consumer").unwrap().0 != consumer);
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["supplier", "consumer", "supplier", "consumer"]
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn newly_available_but_incompatible_provider_reports_error_and_observation_can_recover() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new();
            fixture.reports.borrow_and_update();
            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture.reports.changed().await.unwrap();
            let mut incompatible = declaration();
            incompatible.version = 2;
            fixture
                .runtime
                .register_provider(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new("supplier"),
                        version: 1,
                        requirements: vec![],
                        provisions: vec![incompatible],
                    },
                    |_| panic!("incompatible graph must not activate"),
                ))
                .unwrap();
            let error = Err(ConfigurationError::IncompatibleBinding {
                instance: "consumer".into(),
                contract: Facility::ID.into(),
            });
            fixture
                .reports
                .wait_for(|report| report == &error)
                .await
                .unwrap();
            let counted = fixture.activations.clone();
            fixture
                .runtime
                .register_provider(MicroFactory::new(
                    MicroDescriptor {
                        id: MicroId::new("replacement"),
                        version: 1,
                        requirements: vec![],
                        provisions: vec![declaration()],
                    },
                    move |activation| {
                        counted.lock().unwrap().push("replacement".into());
                        activation.publish(Facility(activation.instance()));
                        Ok(())
                    },
                ))
                .unwrap();
            fixture.desired.get_mut("supplier").unwrap().provider = MicroId::new("replacement");
            fixture.runtime.configure(&fixture.desired).unwrap();
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();
            assert_eq!(
                *fixture.activations.lock().unwrap(),
                ["replacement", "consumer"]
            );
            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}

#[tokio::test]
async fn cancelling_an_observer_releases_ownership_for_a_replacement() {
    use std::{future::Future, task::Poll};
    let fixture = Fixture::new();
    let mut observer = Box::pin(fixture.runtime.observe());
    std::future::poll_fn(|context| {
        assert!(observer.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(observer);
    fixture.runtime.host().close();
    fixture.runtime.observe().await;
}

#[tokio::test]
async fn facet_and_static_configuration_sources_enforce_locked_entries_and_drive_observation() {
    use crate::{
        documents::{DocumentGrant, FacetRoute},
        micros::composition::{FacetConfiguration, MockConfiguration, StaticConfiguration},
    };
    use daybook_types::manifest::RoutineFacetAccess;

    tokio::task::LocalSet::new()
        .run_until(async {
            let mut fixture = Fixture::new();
            fixture.register_supplier();

            let locked = BTreeMap::from([(
                "supplier".into(),
                ConfiguredMicro {
                    provider: MicroId::new("supplier"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::new(),
                },
            )]);
            let dynamic = BTreeMap::from([(
                "consumer".into(),
                ConfiguredMicro {
                    provider: MicroId::new("consumer"),
                    enablement: Enablement::Enabled,
                    bindings: BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
                },
            )]);

            let static_source = StaticConfiguration::new(locked.clone());
            assert_eq!(
                fixture
                    .runtime
                    .set_configuration_source(static_source)
                    .unwrap(),
                BTreeMap::from([("supplier".into(), InstanceState::Active)])
            );
            assert!(fixture.runtime.is_locked("supplier"));
            assert!(!fixture.runtime.is_locked("consumer"));

            let mock_source = MockConfiguration::with_locked(locked.clone(), dynamic.clone());
            assert_eq!(
                mock_source.set_enablement("supplier", Enablement::Disabled),
                Err(ConfigurationError::LockedInstance("supplier".into()))
            );

            let host = fixture.runtime.host();
            let route = FacetRoute {
                document: "composition-doc".into(),
                branch: "main".into(),
                tag: "org.dayframe.composition".into(),
                key_id: "main".into(),
            };
            host.mock_documents().seed(
                route.clone(),
                serde_json::to_value(&dynamic).unwrap(),
            );
            let access = host
                .grant_documents(
                    &fixture.runtime.root(),
                    vec![DocumentGrant {
                        document: route.document.clone(),
                        branch: route.branch.clone(),
                        facets: vec![RoutineFacetAccess {
                            tag: route.tag.clone().into(),
                            key_id: Some(route.key_id.clone()),
                            read: true,
                            write: true,
                            ..Default::default()
                        }],
                    }],
                )
                .unwrap();
            let doc = access
                .document(route.document.clone(), route.branch.clone())
                .unwrap();
            let facet = doc
                .facet::<BTreeMap<String, ConfiguredMicro>>(route.tag.clone(), route.key_id.clone())
                .unwrap();
            let facet_source = FacetConfiguration::new(facet, locked);

            assert_eq!(
                facet_source.set_enablement("supplier", Enablement::Disabled),
                Err(ConfigurationError::LockedInstance("supplier".into()))
            );
            assert_eq!(
                fixture
                    .runtime
                    .set_configuration_source(facet_source.clone())
                    .unwrap(),
                BTreeMap::from([
                    ("consumer".into(), InstanceState::Active),
                    ("supplier".into(), InstanceState::Active),
                ])
            );
            assert!(fixture.runtime.is_locked("supplier"));
            assert!(!fixture.runtime.is_locked("consumer"));

            let runtime = fixture.runtime.clone();
            let observation = tokio::task::spawn_local(async move { runtime.observe().await });
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();

            facet_source
                .set_enablement("consumer", Enablement::Disabled)
                .unwrap();
            let disabled = Ok(BTreeMap::from([
                ("consumer".into(), InstanceState::Disabled),
                ("supplier".into(), InstanceState::Active),
            ]));
            fixture
                .reports
                .wait_for(|report| report == &disabled)
                .await
                .unwrap();

            host.mock_documents().settle(false);
            fixture
                .reports
                .wait_for(|report| report == &active())
                .await
                .unwrap();

            let mut tampered = dynamic;
            tampered.insert(
                "supplier".into(),
                ConfiguredMicro {
                    provider: MicroId::new("supplier"),
                    enablement: Enablement::Disabled,
                    bindings: BTreeMap::new(),
                },
            );
            host.mock_documents()
                .external_change(route, Some(serde_json::to_value(tampered).unwrap()));
            assert_eq!(
                fixture.runtime.reconcile_configuration().unwrap(),
                BTreeMap::from([
                    ("consumer".into(), InstanceState::Active),
                    ("supplier".into(), InstanceState::Active),
                ])
            );

            fixture.runtime.host().close();
            observation.await.unwrap();
        })
        .await;
}
