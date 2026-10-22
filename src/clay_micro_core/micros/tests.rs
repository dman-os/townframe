use super::*;

#[tokio::test]
async fn registry_dispatches_a_provider_through_host_lifecycle() {
    let host = Host::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let provider_id = MicroId::new("dayframe.test");
    let mut registry = MicroRegistry::default();
    let seen_by_provider = seen.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: provider_id.clone(),
                version: 1,
                provisions: Vec::new(),
                requirements: Vec::new(),
            },
            move |context| {
                seen_by_provider
                    .lock()
                    .unwrap()
                    .push(context.instance().name.clone());
                Ok(())
            },
        ))
        .unwrap();
    let second_id = MicroId::new("dayframe.other");
    let second_seen = seen.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: second_id.clone(),
                version: 1,
                provisions: Vec::new(),
                requirements: Vec::new(),
            },
            move |context| {
                second_seen
                    .lock()
                    .unwrap()
                    .push(format!("other:{}", context.instance().name));
                Ok(())
            },
        ))
        .unwrap();
    let other = registry
        .activate(&host, &second_id, "other-test", BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(other.name, "other-test");
    host.close_instance(&other).unwrap();

    let instance = registry
        .activate(&host, &provider_id, "configured-test", BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["other:other-test", "configured-test"]
    );
    assert_eq!(instance.name, "configured-test");
    host.close_instance(&instance).unwrap();
    assert_eq!(host.check(&instance), Err(AccessError::Revoked));
    host.close();
}

#[tokio::test]
async fn closed_host_rejects_registry_activation_without_calling_provider() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = MicroId::new("closed-host");
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: id.clone(),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            |_| panic!("closed host must reject activation before invoking provider"),
        ))
        .unwrap();
    host.close();
    assert!(matches!(
        registry
            .activate(&host, &id, "closed", BTreeMap::new())
            .await,
        Err(MicroRegistryError::Access(AccessError::Revoked))
    ));
}

#[tokio::test]
async fn closure_before_first_poll_does_not_invoke_async_provider_factory() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = MicroId::new("closed-before-poll");
    registry
        .register(MicroFactory::new_async(
            MicroDescriptor {
                id: id.clone(),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            |_| -> std::future::Ready<Result<(), ActivationError>> {
                panic!("closed activation must not invoke provider factory")
            },
        ))
        .unwrap();
    let mut activation = registry
        .begin(&host, &id, "closed".into(), &BTreeMap::new())
        .unwrap();
    activation
        .host
        .close_instance(&activation.instance)
        .unwrap();
    assert_eq!(
        std::future::poll_fn(|context| activation.poll(context)).await,
        Err(ActivationError::Access(AccessError::Revoked))
    );
    host.close();
}

#[tokio::test]
async fn registry_driver_polls_local_work_without_application_or_surface() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = MicroId::new("local-work");
    let seen = Rc::new(std::cell::Cell::new(0));
    let observed = seen.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: id.clone(),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                let observed = observed.clone();
                activation.own_future(async move { observed.set(observed.get() + 1) })
            },
        ))
        .unwrap();
    let instance = registry
        .activate(&host, &id, "local", BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(seen.get(), 0);
    let mut driver = Box::pin(registry.drive_owned_futures());
    std::future::poll_fn(|context| {
        assert!(driver.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(seen.get(), 1);
    std::future::poll_fn(|context| {
        assert!(driver.as_mut().poll(context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(seen.get(), 1);
    host.close_instance(&instance).unwrap();
    drop(driver);
    host.close();
}

#[tokio::test]
async fn registry_failure_drops_queued_work_without_polling_a_driver() {
    struct Dropped(Rc<std::cell::Cell<bool>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = MicroId::new("queued-failure");
    let dropped = Rc::new(std::cell::Cell::new(false));
    let observed = dropped.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: id.clone(),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                let guard = Dropped(observed.clone());
                activation.own_future(async move {
                    let _guard = guard;
                    panic!("failed activation's queued work must not be polled");
                })?;
                Err(AccessError::Denied)
            },
        ))
        .unwrap();
    assert!(matches!(
        registry
            .activate(&host, &id, "failed", BTreeMap::new())
            .await,
        Err(MicroRegistryError::Access(AccessError::Denied))
    ));
    assert!(dropped.get());
    host.close();
}

#[tokio::test]
async fn closing_an_owner_drops_its_pending_future_on_reap_without_polling() {
    struct Dropped(Rc<std::cell::Cell<bool>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = MicroId::new("closing-owner");
    let dropped = Rc::new(std::cell::Cell::new(false));
    let observed = dropped.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: id.clone(),
                version: 1,
                requirements: vec![],
                provisions: vec![],
            },
            move |activation| {
                let guard = Dropped(observed.clone());
                activation.own_future(async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                })
            },
        ))
        .unwrap();
    let first = registry
        .activate(&host, &id, "first", BTreeMap::new())
        .await
        .unwrap();
    let second = registry
        .activate(&host, &id, "second", BTreeMap::new())
        .await
        .unwrap();
    let mut driver = registry.future_driver();
    driver.reap();
    assert!(!dropped.get());
    host.close_instance(&first).unwrap();
    driver.reap();
    assert!(dropped.get());
    assert_eq!(driver.futures.as_ref().unwrap().owners.len(), 1);
    drop(driver);
    host.close_instance(&second).unwrap();
    host.close();
}

#[tokio::test]
async fn async_micro_factory_activates_and_rolls_back_on_error() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let ok_id = MicroId::new("dayframe.async.ok");
    let fail_id = MicroId::new("dayframe.async.fail");
    let leaked = Arc::new(Mutex::new(None));
    let capture = leaked.clone();
    let provider_host = host.clone();
    registry
        .register(MicroFactory::new_async(
            MicroDescriptor {
                id: ok_id.clone(),
                version: 1,
                provisions: vec![],
                requirements: vec![],
            },
            |context| async move {
                tokio::task::yield_now().await;
                context.host.check(&context.instance())?;
                Ok(())
            },
        ))
        .unwrap();
    registry
        .register(MicroFactory::new_async(
            MicroDescriptor {
                id: fail_id.clone(),
                version: 1,
                provisions: vec![],
                requirements: vec![],
            },
            move |context| {
                let capture = capture.clone();
                let host = provider_host.clone();
                async move {
                    tokio::task::yield_now().await;
                    let component = host.publish_component(&context.instance())?;
                    *capture.lock().unwrap() = Some((context.instance(), component));
                    Err(AccessError::Denied.into())
                }
            },
        ))
        .unwrap();
    let instance = registry
        .activate(&host, &ok_id, "async-ok", BTreeMap::new())
        .await
        .unwrap();
    assert!(host.check(&instance).is_ok());
    assert!(matches!(
        registry
            .activate(&host, &fail_id, "async-fail", BTreeMap::new())
            .await,
        Err(MicroRegistryError::Access(AccessError::Denied))
    ));
    let (failed_instance, _) = leaked.lock().unwrap().take().unwrap();
    assert_eq!(host.check(&failed_instance), Err(AccessError::Revoked));
    host.close();
}

#[tokio::test]
async fn failed_provider_activation_rolls_back_published_effects() {
    let host = Host::new();
    let root = host.start("root");
    let provider_id = MicroId::new("dayframe.failing");
    let published = Arc::new(Mutex::new(None));
    let published_by_provider = published.clone();
    let mut registry = MicroRegistry::default();
    let provider_host = host.clone();
    registry
        .register(MicroFactory::new(
            MicroDescriptor {
                id: provider_id.clone(),
                version: 1,
                provisions: Vec::new(),
                requirements: Vec::new(),
            },
            move |context| {
                let component = provider_host.publish_component(&context.instance())?;
                *published_by_provider.lock().unwrap() = Some((context.instance(), component));
                Err(AccessError::Denied)
            },
        ))
        .unwrap();

    assert!(matches!(
        registry
            .activate(&host, &provider_id, "failing", BTreeMap::new())
            .await,
        Err(MicroRegistryError::Access(AccessError::Denied))
    ));
    let (instance, component) = published.lock().unwrap().take().unwrap();
    assert_eq!(host.check(&instance), Err(AccessError::Revoked));
    assert!(matches!(
        host.mount(&root, component),
        Err(AccessError::Revoked)
    ));
    host.close();
}

// ── Adapter contract violations are invariant failures, not errors ───────

#[derive(Clone, Debug, PartialEq, Eq)]
struct Contract(u8);
impl Provision for Contract {
    const ID: &'static str = "test.contract";
}

fn contract_declaration() -> MicroProvision {
    MicroProvision {
        id: Contract::ID.into(),
        version: Contract::VERSION,
    }
}

fn register_provider(
    registry: &mut MicroRegistry,
    descriptor: MicroDescriptor,
    activate: impl Fn(MicroActivation) -> Result<(), AccessError> + 'static,
) -> MicroId {
    let id = descriptor.id.clone();
    registry
        .register(MicroFactory::new(descriptor, activate))
        .unwrap();
    id
}

#[tokio::test]
#[should_panic(expected = "adapter published an undeclared provision")]
async fn publishing_an_undeclared_provision_is_an_invariant_failure() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = register_provider(
        &mut registry,
        MicroDescriptor {
            id: MicroId::new("undeclared-publication"),
            version: 1,
            requirements: vec![],
            provisions: vec![],
        },
        |activation| {
            activation.publish(Contract(1));
            Ok(())
        },
    );
    let _ = registry
        .activate(&host, &id, "instance", BTreeMap::new())
        .await;
}

#[tokio::test]
#[should_panic(expected = "adapter published a provision twice")]
async fn publishing_a_provision_twice_is_an_invariant_failure() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = register_provider(
        &mut registry,
        MicroDescriptor {
            id: MicroId::new("double-publication"),
            version: 1,
            requirements: vec![],
            provisions: vec![contract_declaration()],
        },
        |activation| {
            activation.publish(Contract(1));
            activation.publish_erased(
                Contract::ID,
                Contract::VERSION,
                std::sync::Arc::new(Contract(2)),
            );
            Ok(())
        },
    );
    let _ = registry
        .activate(&host, &id, "instance", BTreeMap::new())
        .await;
}

#[tokio::test]
#[should_panic(expected = "adapter requested an undeclared requirement")]
async fn requiring_an_undeclared_requirement_is_an_invariant_failure() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = register_provider(
        &mut registry,
        MicroDescriptor {
            id: MicroId::new("undeclared-requirement"),
            version: 1,
            requirements: vec![],
            provisions: vec![],
        },
        |activation| {
            let _ = activation.require::<Contract>();
            Ok(())
        },
    );
    let _ = registry
        .activate(&host, &id, "instance", BTreeMap::new())
        .await;
}

#[tokio::test]
#[should_panic(expected = "publication after activation completed")]
async fn publishing_after_activation_completed_is_an_invariant_failure() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let retained = Rc::new(RefCell::new(None));
    let saved = retained.clone();
    let id = register_provider(
        &mut registry,
        MicroDescriptor {
            id: MicroId::new("late-publication"),
            version: 1,
            requirements: vec![],
            provisions: vec![contract_declaration()],
        },
        move |activation| {
            activation.publish(Contract(1));
            *saved.borrow_mut() = Some(activation);
            Ok(())
        },
    );
    registry
        .activate(&host, &id, "instance", BTreeMap::new())
        .await
        .unwrap();
    // The staging area is torn down at commit; a retained activation handle must not
    // resurrect it.
    retained.borrow().as_ref().unwrap().publish(Contract(2));
}

#[tokio::test]
#[should_panic(expected = "native contract version mismatch")]
async fn erased_read_at_the_wrong_version_is_an_invariant_failure() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = register_provider(
        &mut registry,
        MicroDescriptor {
            id: MicroId::new("versioned"),
            version: 1,
            requirements: vec![],
            provisions: vec![contract_declaration()],
        },
        |activation| {
            activation.publish(Contract(1));
            Ok(())
        },
    );
    registry
        .activate(&host, &id, "instance", BTreeMap::new())
        .await
        .unwrap();
    let _ = registry.output_erased("instance", Contract::ID, Contract::VERSION + 1);
}

#[tokio::test]
#[should_panic(expected = "duplicate requirement declaration")]
async fn duplicate_requirement_declarations_are_an_invariant_failure() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = register_provider(
        &mut registry,
        MicroDescriptor {
            id: MicroId::new("duplicate-requirements"),
            version: 1,
            requirements: vec![contract_declaration(), contract_declaration()],
            provisions: vec![],
        },
        |_| Ok(()),
    );
    let _ = registry
        .activate(
            &host,
            &id,
            "instance",
            BTreeMap::from([(Contract::ID.to_owned(), "supplier".to_owned())]),
        )
        .await;
}

#[tokio::test]
async fn duplicate_provider_registration_is_rejected_and_reported_on_changes() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let mut changes = registry.changes();
    let descriptor = || MicroDescriptor {
        id: MicroId::new("singleton"),
        version: 1,
        requirements: vec![],
        provisions: vec![],
    };
    register_provider(&mut registry, descriptor(), |_| Ok(()));
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    assert_eq!(
        registry.register(MicroFactory::new(descriptor(), |_| Ok(()))),
        Err(MicroRegistryError::DuplicateProvider("singleton".into()))
    );
    // A rejected registration is not a catalog change.
    assert!(!changes.has_changed().unwrap());
    // The original provider is still the registered one.
    registry
        .activate(
            &host,
            &MicroId::new("singleton"),
            "instance",
            BTreeMap::new(),
        )
        .await
        .unwrap();
    host.close();
}

#[tokio::test]
#[should_panic(expected = "registry futures already driven")]
async fn a_second_future_driver_is_an_invariant_failure() {
    let registry = MicroRegistry::default();
    let _driver = registry.future_driver();
    let _second = registry.future_driver();
}

// ── Pending-activation interposition: begin → poll → commit seam ────────

fn contract_provider(registry: &mut MicroRegistry) -> MicroId {
    register_provider(
        registry,
        MicroDescriptor {
            id: MicroId::new("contract-supplier"),
            version: 1,
            requirements: vec![],
            provisions: vec![contract_declaration()],
        },
        |activation| {
            activation.publish(Contract(1));
            Ok(())
        },
    )
}

#[tokio::test]
async fn commit_publishes_staged_provisions() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = contract_provider(&mut registry);
    let mut activation = registry
        .begin(&host, &id, "instance".into(), &BTreeMap::new())
        .unwrap();
    assert_eq!(
        std::future::poll_fn(|context| activation.poll(context)).await,
        Ok(())
    );
    let instance = activation.commit().unwrap();
    assert_eq!(host.check(&instance), Ok(()));
    assert_eq!(registry.output::<Contract>("instance"), Ok(Contract(1)));
    host.close();
}

#[tokio::test]
async fn closing_between_poll_and_commit_fails_commit_without_publishing() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = contract_provider(&mut registry);
    let mut activation = registry
        .begin(&host, &id, "instance".into(), &BTreeMap::new())
        .unwrap();
    assert_eq!(
        std::future::poll_fn(|context| activation.poll(context)).await,
        Ok(())
    );
    let instance = activation.instance.clone();
    host.close_instance(&instance).unwrap();
    // A ready provider future does not entitle a dead incarnation to publish.
    assert_eq!(
        activation.commit(),
        Err(MicroRegistryError::Access(AccessError::Revoked))
    );
    assert!(matches!(
        registry.output::<Contract>("instance"),
        Err(AccessError::Revoked)
    ));
    host.close();
}

#[tokio::test]
async fn dropping_a_ready_pending_activation_rolls_back_without_commit() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    let id = contract_provider(&mut registry);
    let mut activation = registry
        .begin(&host, &id, "instance".into(), &BTreeMap::new())
        .unwrap();
    assert_eq!(
        std::future::poll_fn(|context| activation.poll(context)).await,
        Ok(())
    );
    let instance = activation.instance.clone();
    drop(activation);
    assert_eq!(host.check(&instance), Err(AccessError::Revoked));
    assert!(matches!(
        registry.output::<Contract>("instance"),
        Err(AccessError::Revoked)
    ));
    host.close();
}
