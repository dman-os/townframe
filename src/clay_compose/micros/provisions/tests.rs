use crate::micros::{
    MicroDescriptor, MicroFactory, MicroId, MicroProvision, MicroRegistry, MicroRegistryError,
    Provision,
};
use clay_host::{AccessError, Host};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

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
    use std::{future::Future, task::Poll};
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
