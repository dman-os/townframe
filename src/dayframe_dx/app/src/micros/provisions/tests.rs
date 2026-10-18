use crate::{
    host::{AccessError, Host},
    micros::{
        MicroDescriptor, MicroFactory, MicroId, MicroProvision, MicroRegistry, MicroRegistryError,
        Provision,
    },
};
use std::collections::BTreeMap;

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

#[test]
fn live_provisions_bind_consumers_and_withdraw_on_provider_shutdown() {
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
        .unwrap();
    let consumer = registry
        .activate(
            &host,
            &MicroId::new("consumer"),
            "consumer",
            BTreeMap::from([(Facility::ID.into(), "supplier".into())]),
        )
        .unwrap();
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Ok(Facility("live"))
    );
    supplier.close();
    assert_eq!(consumer.check(), Err(AccessError::Revoked));
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
        .unwrap();
    supplier.close();
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Ok(Facility("live"))
    );
    replacement.close();
    host.close();
}

#[test]
fn declaration_does_not_satisfy_a_dependency_and_missing_bindings_do_not_activate() {
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
        ),
        Err(MicroRegistryError::MissingProvision(id)) if id == Facility::ID
    ));
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Err(AccessError::Revoked)
    );
    assert!(
        matches!(registry.activate(&host, &MicroId::new("consumer"), "consumer", BTreeMap::new()),
        Err(MicroRegistryError::InvalidBinding(id)) if id == Facility::ID)
    );
    assert!(matches!(
        registry.activate(
            &host,
            &MicroId::new("consumer"),
            "consumer",
            BTreeMap::from([(Facility::ID.into(), "supplier".into())])
        ),
        Err(MicroRegistryError::Access(AccessError::Revoked))
    ));
    host.close();
}

#[test]
fn failed_activation_never_publishes_partial_outputs() {
    let host = Host::new();
    let mut registry = MicroRegistry::default();
    provider(&mut registry, |activation| {
        activation.publish(Facility("partial"));
        Err(AccessError::Denied)
    });
    assert!(matches!(
        registry.activate(
            &host,
            &MicroId::new("provider"),
            "supplier",
            BTreeMap::new()
        ),
        Err(MicroRegistryError::Access(AccessError::Denied))
    ));
    assert_eq!(
        registry.output::<Facility>("supplier"),
        Err(AccessError::Revoked)
    );
    host.close();
}

#[test]
fn incompatible_live_contract_is_rejected_before_consumer_activation() {
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
        .unwrap();
    assert!(
        matches!(registry.activate(&host, &MicroId::new("consumer"), "consumer",
        BTreeMap::from([(Facility::ID.into(), "supplier".into())])),
        Err(MicroRegistryError::InvalidBinding(id)) if id == Facility::ID)
    );
    host.close();
}
