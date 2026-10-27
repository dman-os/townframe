use crate::micros::MicroDescriptor;
use crate::micros::MicroFactory;
use crate::micros::MicroId;
use crate::micros::MicroProvision;
use crate::micros::MicroRegistry;
use crate::micros::MicroRegistryError;
use crate::micros::Provision;
use clay_host::AccessError;
use clay_host::Host;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

#[tokio::test]
async fn retained_activation_cannot_resolve_replacement_or_use_revoked_consumer() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let count = Rc::new(std::cell::Cell::new(0));
    let counted = count.clone();
    provider(&mut registry, move |activation| {
        counted.set(counted.get() + 1);
        activation.publish(Facility(if counted.get() == 1 {
            "original"
        } else {
            "replacement"
        }));
        Ok(())
    });
    let retained = Rc::new(RefCell::new(None));
    let saved = retained.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("consumer"),
                version: 1,
                requirements: vec![declaration()],
                provisions: vec![],
            },
            move |activation| {
                assert_eq!(activation.require::<Facility>()?, Facility("original"));
                *saved.borrow_mut() = Some(activation);
                Ok(())
            },
        ))
        .unwrap();
    let supplier = registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let consumer = registry
        .activate(
            &host,
            &MicroId::new("consumer"),
            "consumer",
            BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
        )
        .await
        .unwrap();
    host.close_instance(&consumer).unwrap();
    assert_eq!(
        retained.borrow().as_ref().unwrap().require::<Facility>(),
        Err(AccessError::Revoked)
    );
    host.close_instance(&supplier).unwrap();
    registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Ok(Facility("replacement"))
    );
    assert_eq!(
        retained.borrow().as_ref().unwrap().require::<Facility>(),
        Err(AccessError::Revoked)
    );
    host.close();
}

#[tokio::test]
async fn supplier_withdrawal_cancels_awaiting_consumer_and_pins_its_context() {
    use crate::micros::ActivationError;
    use std::future::Future;
    use std::task::Poll;
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    provider(&mut registry, |activation| {
        activation.publish(Facility("supplier"));
        Ok(())
    });
    let retained = Rc::new(RefCell::new(None));
    let saved = retained.clone();
    let (finish, receiver) = tokio::sync::oneshot::channel();
    let receiver = Rc::new(RefCell::new(Some(receiver)));
    registry
        .register(MicroFactory::new_async(
            MicroDescriptor {
                id: MicroId::new("consumer"),
                version: 1,
                requirements: vec![declaration()],
                provisions: vec![],
            },
            move |activation| {
                *saved.borrow_mut() = Some(activation.clone());
                let receiver = receiver.borrow_mut().take().unwrap();
                async move {
                    receiver.await.unwrap();
                    activation.require::<Facility>()?;
                    Ok::<(), ActivationError>(())
                }
            },
        ))
        .unwrap();
    let supplier = registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let bindings = BTreeMap::from([(Facility::ID.into(), "supplier".into())]);
    let id = MicroId::new("consumer");
    let mut consumer = Box::pin(registry.activate(&host, &id, "consumer", bindings));
    std::future::poll_fn(|context| {
        assert!(consumer.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    host.close_instance(&supplier).unwrap();
    registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        retained.borrow().as_ref().unwrap().require::<Facility>(),
        Err(AccessError::Revoked)
    );
    assert!(matches!(
        consumer.await,
        Err(MicroRegistryError::Access(AccessError::Revoked))
    ));
    assert!(finish.send(()).is_err());
    host.close();
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Facility(&'static str);
impl Provision for Facility {
    const ID: &'static str = "test.facility";
}

fn declaration() -> MicroProvision {
    MicroProvision {
        id: Facility::ID.into(),
        version: Facility::VERSION,
    }
}

fn provider(
    registry: &mut MicroRegistry,
    activation: impl Fn(crate::micros::MicroActivation) -> Result<(), AccessError> + 'static,
) {
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("provider"),
                version: 1,
                requirements: vec![],
                provisions: vec![declaration()],
            },
            activation,
        ))
        .unwrap();
}

#[tokio::test]
async fn live_provisions_bind_consumers_and_withdraw_on_provider_shutdown() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    provider(&mut registry, |activation| {
        activation.publish(Facility("live"));
        Ok(())
    });
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("consumer"),
                version: 1,
                requirements: vec![declaration()],
                provisions: vec![],
            },
            |activation| {
                assert_eq!(activation.require::<Facility>()?, Facility("live"));
                Ok(())
            },
        ))
        .unwrap();
    let supplier = registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let consumer = registry
        .activate(
            &host,
            &MicroId::new("consumer"),
            "consumer",
            BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
        )
        .await
        .unwrap();
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Ok(Facility("live"))
    );
    host.close_instance(&supplier).unwrap();
    assert_eq!(host.check(&consumer), Err(AccessError::Revoked));
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Err(AccessError::Revoked)
    );
    let replacement = registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    host.close_instance(&supplier).unwrap();
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Ok(Facility("live"))
    );
    host.close_instance(&replacement).unwrap();
    host.close();
}

#[tokio::test]
async fn declaration_does_not_satisfy_a_dependency_and_missing_bindings_do_not_activate() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    provider(&mut registry, |_| Ok(()));
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("consumer"),
                version: 1,
                requirements: vec![declaration()],
                provisions: vec![],
            },
            |_| panic!("unsatisfied requirements must prevent activation"),
        ))
        .unwrap();
    assert!(matches!(
        registry.activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new()
        ).await,
        Err(MicroRegistryError::MissingProvision(id)) if id == Facility::ID
    ));
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Err(AccessError::Revoked)
    );
    assert!(
        matches!(registry.activate(&host, &MicroId::new("consumer"), "consumer", BTreeMap::new()).await,
        Err(MicroRegistryError::InvalidBinding(id)) if id == Facility::ID)
    );
    assert!(matches!(
        registry
            .activate(
                &host,
                &MicroId::new("consumer"),
                "consumer",
                BTreeMap::from([(Facility::ID.into(), "supplier".into())])
            )
            .await,
        Err(MicroRegistryError::Access(AccessError::Revoked))
    ));
    host.close();
}

#[tokio::test]
async fn failed_activation_never_publishes_partial_outputs() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    provider(&mut registry, |activation| {
        activation.publish(Facility("partial"));
        Err(AccessError::Denied)
    });
    assert!(matches!(
        registry
            .activate(
                &host,
                &MicroId::new("provider"),
                "supplier",
                BTreeMap::new()
            )
            .await,
        Err(MicroRegistryError::Access(AccessError::Denied))
    ));
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Err(AccessError::Revoked)
    );
    host.close();
}

#[tokio::test]
async fn incompatible_live_contract_is_rejected_before_consumer_activation() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    provider(&mut registry, |activation| {
        activation.publish(Facility("v1"));
        Ok(())
    });
    let mut requirement = declaration();
    requirement.version = 2;
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: MicroId::new("consumer"),
                version: 1,
                requirements: vec![requirement],
                provisions: vec![],
            },
            |_| panic!("incompatible requirements must prevent activation"),
        ))
        .unwrap();
    registry
        .activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(
        matches!(registry.activate(&host, &MicroId::new("consumer"), "consumer",
        BTreeMap::from([(Facility::ID.into(), "supplier".into())])).await,
        Err(MicroRegistryError::InvalidBinding(id)) if id == Facility::ID)
    );
    host.close();
}

// ── Typed/erased duality: both surfaces resolve through one authority ────────

#[derive(Clone, Debug, PartialEq, Eq)]
struct Alpha(u8);
impl Provision for Alpha {
    const ID: &'static str = "dual.alpha";
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Beta(u8);
impl Provision for Beta {
    const ID: &'static str = "dual.beta";
    const VERSION: u32 = 2;
}

fn dual_declaration<T: Provision>() -> MicroProvision {
    MicroProvision {
        id: T::ID.into(),
        version: T::VERSION,
    }
}

fn dual_descriptor(id: &str, provides: bool, requires: bool) -> MicroDescriptor {
    MicroDescriptor {
        id: MicroId::new(id),
        version: 1,
        provisions: if provides {
            vec![dual_declaration::<Alpha>(), dual_declaration::<Beta>()]
        } else {
            vec![]
        },
        requirements: if requires {
            vec![dual_declaration::<Alpha>(), dual_declaration::<Beta>()]
        } else {
            vec![]
        },
    }
}

/// Registry with the same supplier/consumer pair implemented twice: once staging and reading
/// through the typed API, once through the erased API. Suppliers take their values from `staged`
/// (keyed by instance name); consumers record what they resolved into `readings`.
fn dual_registry(
    staged: Rc<RefCell<BTreeMap<String, (u8, u8)>>>,
    readings: Rc<RefCell<BTreeMap<String, (u8, u8)>>>,
) -> MicroRegistry {
    let mut registry = MicroRegistry::default();
    let values = staged.clone();
    registry
        .register(MicroFactory::new(
            dual_descriptor("supplier.typed", true, false),
            move |activation| {
                let (alpha, beta) = values.borrow()[&activation.instance().name];
                activation.publish(Alpha(alpha));
                activation.publish(Beta(beta));
                Ok(())
            },
        ))
        .unwrap();
    let values = staged.clone();
    registry
        .register(MicroFactory::new(
            dual_descriptor("supplier.erased", true, false),
            move |activation| {
                let (alpha, beta) = values.borrow()[&activation.instance().name];
                activation.publish_erased(Alpha::ID, Alpha::VERSION, Arc::new(Alpha(alpha)));
                activation.publish_erased(Beta::ID, Beta::VERSION, Arc::new(Beta(beta)));
                Ok(())
            },
        ))
        .unwrap();
    let resolved = readings.clone();
    registry
        .register(MicroFactory::new(
            dual_descriptor("consumer.typed", false, true),
            move |activation| {
                let alpha = activation.require::<Alpha>()?;
                let beta = activation.require::<Beta>()?;
                resolved
                    .borrow_mut()
                    .insert(activation.instance().name.clone(), (alpha.0, beta.0));
                Ok(())
            },
        ))
        .unwrap();
    let resolved = readings.clone();
    registry
        .register(MicroFactory::new(
            dual_descriptor("consumer.erased", false, true),
            move |activation| {
                let alpha = activation.require_erased(Alpha::ID, Alpha::VERSION)?;
                let beta = activation.require_erased(Beta::ID, Beta::VERSION)?;
                let alpha = alpha
                    .downcast_ref::<Alpha>()
                    .expect("erased read stored the supplier's value")
                    .clone();
                let beta = beta
                    .downcast_ref::<Beta>()
                    .expect("erased read stored the supplier's value")
                    .clone();
                resolved
                    .borrow_mut()
                    .insert(activation.instance().name.clone(), (alpha.0, beta.0));
                Ok(())
            },
        ))
        .unwrap();
    registry
}

#[derive(Debug, Clone, Copy)]
enum DualOp {
    /// Activate a supplier instance at slot `slot`, staging through the typed or erased API.
    Supply {
        slot: u8,
        typed: bool,
        alpha: u8,
        beta: u8,
    },
    /// Activate a consumer bound to supplier `supplier`, resolving typed or erased.
    Consume {
        supplier: u8,
        typed: bool,
    },
    Close {
        slot: u8,
    },
    /// Compare `output` against `output_erased` for slot (and check the version fence).
    Read {
        slot: u8,
        erased: bool,
    },
}

fn dual_op() -> impl proptest::strategy::Strategy<Value = DualOp> {
    use proptest::prelude::*;
    prop_oneof![
        3 => (0u8..4, any::<bool>(), any::<u8>(), any::<u8>())
            .prop_map(|(slot, typed, alpha, beta)| DualOp::Supply { slot, typed, alpha, beta }),
        3 => (0u8..4, any::<bool>())
            .prop_map(|(supplier, typed)| DualOp::Consume { supplier, typed }),
        2 => (0u8..4).prop_map(|slot| DualOp::Close { slot }),
        2 => (0u8..4, any::<bool>())
            .prop_map(|(slot, erased)| DualOp::Read { slot, erased }),
    ]
}

fn supplier_name(slot: u8) -> String {
    format!("supplier-{slot}")
}

/// Assert both read surfaces agree with `values` (or both report revocation when `None`).
/// A read at any version other than the contract's is an invariant failure on the erased path;
/// the typed path cannot name a wrong version, so only the erased surface is checked here.
fn assert_dual_output(registry: &MicroRegistry, slot: u8, values: Option<(u8, u8)>) {
    let name = supplier_name(slot);
    match values {
        Some((alpha, beta)) => {
            assert_eq!(registry.output::<Alpha>(&name), Ok(Alpha(alpha)));
            assert_eq!(registry.output::<Beta>(&name), Ok(Beta(beta)));
            let erased_alpha = registry.output_erased(&name, Alpha::ID, Alpha::VERSION);
            let erased_beta = registry.output_erased(&name, Beta::ID, Beta::VERSION);
            assert_eq!(
                erased_alpha.unwrap().downcast_ref::<Alpha>(),
                Some(&Alpha(alpha))
            );
            assert_eq!(
                erased_beta.unwrap().downcast_ref::<Beta>(),
                Some(&Beta(beta))
            );
            let wrong_version = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                registry.output_erased(&name, Alpha::ID, Alpha::VERSION + 1)
            }));
            assert!(wrong_version.is_err(), "version mismatch must be caught");
        }
        None => {
            assert_eq!(registry.output::<Alpha>(&name), Err(AccessError::Revoked));
            assert!(matches!(
                registry.output_erased(&name, Alpha::ID, Alpha::VERSION),
                Err(AccessError::Revoked)
            ));
        }
    }
}

async fn run_dual_script(ops: Vec<DualOp>) {
    let host = Host::new();
    let staged = Rc::new(RefCell::new(BTreeMap::new()));
    let readings = Rc::new(RefCell::new(BTreeMap::new()));
    let registry = dual_registry(staged.clone(), readings.clone());
    let mut suppliers: BTreeMap<u8, (clay_host::Instance, u8, u8)> = BTreeMap::new();
    let mut consumer_seq = 0u32;
    for op in ops {
        match op {
            DualOp::Supply {
                slot,
                typed,
                alpha,
                beta,
            } => {
                if suppliers.contains_key(&slot) {
                    continue;
                }
                let name = supplier_name(slot);
                staged.borrow_mut().insert(name.clone(), (alpha, beta));
                let provider = if typed {
                    MicroId::new("supplier.typed")
                } else {
                    MicroId::new("supplier.erased")
                };
                let instance = registry
                    .activate(&host, &provider, name, BTreeMap::new())
                    .await
                    .unwrap();
                suppliers.insert(slot, (instance, alpha, beta));
                assert_dual_output(&registry, slot, Some((alpha, beta)));
            }
            DualOp::Consume { supplier, typed } => {
                let Some((_, alpha, beta)) = suppliers.get(&supplier) else {
                    continue;
                };
                let (alpha, beta) = (*alpha, *beta);
                let provider = if typed {
                    MicroId::new("consumer.typed")
                } else {
                    MicroId::new("consumer.erased")
                };
                let name = format!("consumer-{consumer_seq}");
                consumer_seq += 1;
                let bindings = BTreeMap::from([
                    (Alpha::ID.to_owned(), supplier_name(supplier)),
                    (Beta::ID.to_owned(), supplier_name(supplier)),
                ]);
                registry
                    .activate(&host, &provider, name.clone(), bindings)
                    .await
                    .unwrap();
                assert_eq!(readings.borrow()[&name], (alpha, beta));
            }
            DualOp::Close { slot } => {
                let Some((instance, _, _)) = suppliers.remove(&slot) else {
                    continue;
                };
                host.close_instance(&instance).unwrap();
                assert_dual_output(&registry, slot, None);
            }
            DualOp::Read { slot, erased } => {
                let values = suppliers.get(&slot).map(|(_, alpha, beta)| (*alpha, *beta));
                if erased {
                    // The typed read already ran inside `assert_dual_output`; exercise the
                    // registry's runtime-named surface as the primary check here.
                    let name = supplier_name(slot);
                    match values {
                        Some((alpha, beta)) => {
                            assert_eq!(
                                registry
                                    .output_erased(&name, Alpha::ID, Alpha::VERSION)
                                    .unwrap()
                                    .downcast_ref::<Alpha>(),
                                Some(&Alpha(alpha))
                            );
                            assert_eq!(
                                registry
                                    .output_erased(&name, Beta::ID, Beta::VERSION)
                                    .unwrap()
                                    .downcast_ref::<Beta>(),
                                Some(&Beta(beta))
                            );
                        }
                        None => {
                            assert!(matches!(
                                registry.output_erased(&name, Alpha::ID, Alpha::VERSION),
                                Err(AccessError::Revoked)
                            ));
                        }
                    }
                }
                assert_dual_output(&registry, slot, values);
            }
        }
    }
    // Whatever survives the script must still read identically on both surfaces.
    for slot in suppliers.keys() {
        let (_, alpha, beta) = &suppliers[slot];
        assert_dual_output(&registry, *slot, Some((*alpha, *beta)));
    }
    host.close();
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 48,
        ..Default::default()
    })]
    #[test]
    fn typed_and_erased_publication_paths_are_interchangeable(
        ops in proptest::collection::vec(dual_op(), 1..48)
    ) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_dual_script(ops));
    }
}
