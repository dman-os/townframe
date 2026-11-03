use super::*;

fn bank(limits: CapabilityLimits) -> (Host, CapabilitySession, Instance) {
    let host = Host::new();
    let instance = host.start("provider");
    (
        host.clone(),
        CapabilitySession::new(host, instance.clone(), limits),
        instance,
    )
}

#[tokio::test]
async fn two_recipients_get_distinct_refs_and_redeem_independently() {
    let (host, provider, _instance) = bank(CapabilityLimits::default());
    let lease = provider.publish_presentation("view").unwrap();
    let publication = lease.publication();
    let consumer_a =
        CapabilitySession::new(host.clone(), host.start("a"), CapabilityLimits::default());
    let consumer_b =
        CapabilitySession::new(host.clone(), host.start("b"), CapabilityLimits::default());
    let ref_a = consumer_a.mint(&publication).unwrap();
    let ref_b = consumer_b.mint(&publication).unwrap();
    assert_eq!(
        ref_a.publication().unwrap().id(),
        ref_b.publication().unwrap().id()
    );
    assert_ne!(ref_a.recipient_scope(), ref_b.recipient_scope());
    let proxy_a = ref_a.redeem_presentation().await.unwrap();
    let proxy_b = ref_b.redeem_presentation().await.unwrap();
    assert_eq!(proxy_a.component().unwrap(), proxy_b.component().unwrap());
    assert_eq!(consumer_a.proxy_count(), 1);
    drop(proxy_a);
    assert_eq!(consumer_a.proxy_count(), 0);
    assert!(proxy_b.component().is_ok());
}

#[tokio::test]
async fn wrong_contract_use_fails_without_exposing_the_resource() {
    let (_host, provider, _instance) = bank(CapabilityLimits::default());
    let lease = provider.publish_presentation("view").unwrap();
    let reference = lease.reference().unwrap();
    // A presentation publication is not a call: nothing is exposed under the other contract.
    assert_eq!(
        reference.retain_call().await.err(),
        Some(CapabilityFailure::ContractMismatch)
    );
    assert_eq!(
        reference.call(Vec::new()).await.err(),
        Some(CallError::Capability(CapabilityFailure::ContractMismatch))
    );
    assert_eq!(provider.proxy_count(), 0);
    assert!(reference.redeem_presentation().await.is_ok());
}

#[tokio::test]
async fn lease_drop_and_provider_closure_make_retained_proxies_inert_but_droppable() {
    let (host, provider, instance) = bank(CapabilityLimits::default());
    let consumer = CapabilitySession::new(
        host.clone(),
        host.start("consumer"),
        CapabilityLimits::default(),
    );
    let lease = provider.publish_presentation("view").unwrap();
    let proxy = consumer
        .mint(&lease.publication())
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    assert!(proxy.component().is_ok());
    drop(lease);
    assert_eq!(proxy.component().err(), Some(CapabilityFailure::Revoked));
    assert!(proxy.is_revoked());
    drop(proxy);
    assert_eq!(consumer.proxy_count(), 0);

    let lease = provider.publish_presentation("view").unwrap();
    let proxy = consumer
        .mint(&lease.publication())
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    host.close_instance(&instance).unwrap();
    assert_eq!(proxy.component().err(), Some(CapabilityFailure::Revoked));
    drop(proxy);
    drop(lease);
}

#[tokio::test]
async fn stale_cleanup_cannot_revoke_a_replacement_publication() {
    let (host, provider, instance) = bank(CapabilityLimits::default());
    let consumer = CapabilitySession::new(
        host.clone(),
        host.start("consumer"),
        CapabilityLimits::default(),
    );
    let stale = provider.publish_presentation("view").unwrap();
    let stale_proxy = consumer
        .mint(&stale.publication())
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    host.close_instance(&instance).unwrap();
    let replacement_instance = host.start("provider-replacement");
    let replacement = CapabilitySession::new(
        host.clone(),
        replacement_instance,
        CapabilityLimits::default(),
    );
    let fresh = replacement.publish_presentation("view").unwrap();
    // Stale lease cleanup happens only now, after the replacement exists.
    drop(stale);
    assert_eq!(
        stale_proxy.component().err(),
        Some(CapabilityFailure::Revoked)
    );
    let fresh_proxy = consumer
        .mint(&fresh.publication())
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    assert!(fresh_proxy.component().is_ok());
}

#[tokio::test]
async fn limits_release_capacity_on_drop() {
    let limits = CapabilityLimits {
        publications: 1,
        references: 1,
        proxies: 1,
    };
    let (_host, provider, _instance) = bank(limits);
    let lease = provider.publish_presentation("view").unwrap();
    assert_eq!(
        provider.publish_presentation("view").err(),
        Some(CapabilityFailure::LimitExceeded)
    );
    let reference = lease.reference().unwrap();
    assert_eq!(
        lease.reference().err(),
        Some(CapabilityFailure::LimitExceeded)
    );
    let proxy = reference.redeem_presentation().await.unwrap();
    assert_eq!(
        reference.redeem_presentation().await.err(),
        Some(CapabilityFailure::LimitExceeded)
    );
    drop(proxy);
    assert!(reference.redeem_presentation().await.is_ok());
    drop(reference);
    assert_eq!(provider.reference_count(), 0);
    assert!(lease.reference().is_ok());
    drop(lease);
    assert_eq!(provider.publication_count(), 0);
    assert!(provider.publish_presentation("view").is_ok());
}
