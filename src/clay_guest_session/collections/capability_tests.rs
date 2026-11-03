use super::*;
use crate::capabilities::CapabilityLimits;
use crate::capabilities::CapabilitySession;

fn tagged_body(index: u8) -> Bytes {
    vec![0xd9, 0xa7, 0xf8, index]
}

fn setup() -> (Host, Instance, CollectionSession, BytesRegistrations) {
    let host = Host::new();
    let instance = host.start("capability-writer");
    let session =
        CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
    let registrations = bytes_registrations(NonZeroUsize::new(8).unwrap(), [5, 6]);
    (host, instance, session, registrations)
}

#[test]
fn attachment_indices_scan_nested_cb_or_and_opaque_bodies_stay_opaque() {
    assert_eq!(attachment_indices(&tagged_body(0)), Some(vec![0]));
    // [tag(1), "x"]
    let mut nested = vec![0x82, 0xd9, 0xa7, 0xf8, 0x01, 0x61, b'x'];
    assert_eq!(attachment_indices(&nested), Some(vec![1]));
    nested.truncate(3);
    assert_eq!(attachment_indices(&nested), None);
    assert_eq!(attachment_indices(&[0xff, 0x00]), None);
    assert_eq!(attachment_indices(b"\x61x"), Some(vec![]));
}

#[test]
fn fabricated_indices_and_copied_cbor_convey_no_authority() {
    let (host, _instance, session, registrations) = setup();
    let writer = session.grant_writer(&registrations).unwrap();
    let provider = CapabilitySession::new(
        host.clone(),
        host.start("provider"),
        CapabilityLimits::default(),
    );
    let lease = provider.publish_presentation("view").unwrap();

    // Index 0 named, but no attachment list at all.
    assert_eq!(
        writer.put_value(vec![1], None, CollectionValue::plain(tagged_body(0))),
        Err(WriteFailure::InvalidAttachment)
    );
    // Copied body bytes with an empty attachment list are equally authority-free.
    let copied = writer.put_value(
        vec![1],
        None,
        CollectionValue {
            body: tagged_body(0),
            capabilities: Vec::new(),
        },
    );
    assert_eq!(copied, Err(WriteFailure::InvalidAttachment));
    assert!(registrations.snapshot().is_empty());

    let version = writer
        .put_value(
            vec![1],
            None,
            CollectionValue {
                body: tagged_body(0),
                capabilities: vec![lease.publication()],
            },
        )
        .unwrap();
    assert!(version.seq > 0);
    // Attachment-count limit.
    let many = CollectionValue {
        body: vec![0x80],
        capabilities: vec![lease.publication(); 33],
    };
    assert_eq!(
        writer.put_value(vec![2], None, many),
        Err(WriteFailure::CapabilityLimit)
    );
}

#[tokio::test]
async fn row_replace_and_remove_release_mirror_refs_but_not_retained_proxies() {
    let (host, _instance, session, registrations) = setup();
    let writer = session.grant_writer(&registrations).unwrap();
    let provider = CapabilitySession::new(
        host.clone(),
        host.start("provider"),
        CapabilityLimits::default(),
    );
    let lease = provider.publish_presentation("view").unwrap();
    let consumer = CapabilitySession::new(
        host.clone(),
        host.start("consumer"),
        CapabilityLimits::default(),
    );
    let other = CapabilitySession::new(
        host.clone(),
        host.start("other"),
        CapabilityLimits::default(),
    );

    let version = writer
        .put_value(
            vec![1],
            None,
            CollectionValue {
                body: tagged_body(0),
                capabilities: vec![lease.publication()],
            },
        )
        .unwrap();
    let stored = registrations.snapshot().into_values().next().unwrap().value;
    // Two recipients mint distinct local refs to the one publication.
    let mirror_ref = consumer.mint(&stored.capabilities[0]).unwrap();
    let other_ref = other.mint(&stored.capabilities[0]).unwrap();
    assert_eq!(
        mirror_ref.publication().unwrap().id(),
        other_ref.publication().unwrap().id()
    );
    assert_ne!(mirror_ref.recipient_scope(), other_ref.recipient_scope());
    let proxy = mirror_ref.redeem_presentation().await.unwrap();

    // Replace then remove the row: substrate drops its stored clones.
    let version = writer
        .put_value(vec![1], Some(version), CollectionValue::plain(vec![0x80]))
        .unwrap();
    writer.remove(vec![1], Some(version)).unwrap();
    assert!(registrations.snapshot().is_empty());
    // The guest mirror dropping its delivered ref releases only that ref.
    drop(mirror_ref);
    assert_eq!(consumer.reference_count(), 0);
    assert!(proxy.component().is_ok());
    assert!(other_ref.redeem_presentation().await.is_ok());

    drop(lease);
    assert!(proxy.component().is_err());
    drop(proxy);
    assert_eq!(consumer.proxy_count(), 0);
}
