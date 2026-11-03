//! Staged initialization (ADR 024 §5) as seen through a collection session: rows and capability
//! publications made while an incarnation initializes are invisible until it commits, appear
//! together when it does, vanish unseen when it fails, and are withdrawn first when it closes.
use super::*;
use crate::capabilities::CapabilityFailure;
use crate::capabilities::CapabilityLimits;
use crate::capabilities::CapabilitySession;
use std::task::Poll;

fn registrations(epoch: u64) -> BytesRegistrations {
    bytes_registrations(NonZeroUsize::new(8).unwrap(), [epoch, 1])
}

fn tagged_body() -> Bytes {
    vec![0xd9, 0xa7, 0xf8, 0x00]
}

/// A committed reader on another incarnation: its bootstrap page, and the lease to park on.
async fn observer(host: &Host, registrations: &BytesRegistrations) -> (ProjectionReader, Cursor) {
    let session = CollectionSession::new(
        host.clone(),
        host.start("observer"),
        CollectionLimits::default(),
    );
    let lease = session
        .grant(registrations.projection())
        .unwrap()
        .acquire(Vec::new(), None)
        .unwrap();
    let bootstrap = lease
        .read(None, ReadLimits::default(), std::future::pending())
        .await
        .unwrap();
    assert!(bootstrap.transactions.is_empty());
    (lease, bootstrap.through)
}

#[tokio::test]
async fn staged_rows_and_capabilities_become_visible_together_on_commit() {
    let host = Host::new();
    let shelf = registrations(1);
    let menu = registrations(2);
    let (lease, through) = observer(&host, &shelf).await;
    let consumer = CapabilitySession::new(
        host.clone(),
        host.start("consumer"),
        CapabilityLimits::default(),
    );

    let provider = host.create("provider").unwrap();
    host.begin_initialization(&provider).unwrap();
    let collections =
        CollectionSession::new(host.clone(), provider.clone(), CollectionLimits::default());
    let capabilities =
        CapabilitySession::new(host.clone(), provider.clone(), CapabilityLimits::default());
    let published = capabilities.publish_presentation_component().unwrap();
    let first = collections.grant_writer(&shelf).unwrap();
    let second = collections.grant_writer(&shelf).unwrap();
    let elsewhere = collections.grant_writer(&menu).unwrap();
    let version = first
        .put_value(
            vec![1],
            None,
            CollectionValue {
                body: tagged_body(),
                capabilities: vec![published.publication()],
            },
        )
        .unwrap();
    // Compare-and-set works against staged rows exactly as against published ones.
    assert_eq!(
        first.put(vec![1], None, vec![7]),
        Err(WriteFailure::Conflict {
            current: Some(version)
        })
    );
    second.put(vec![2], None, vec![2]).unwrap();
    elsewhere.put(vec![3], None, vec![3]).unwrap();

    let parked = lease.read(Some(through), ReadLimits::default(), std::future::pending());
    let mut parked = std::pin::pin!(parked);
    assert!(futures::poll!(parked.as_mut()).is_pending());
    assert!(shelf.snapshot().is_empty());
    assert!(menu.snapshot().is_empty());
    // The staged publication is redeemable by its owner only.
    let staged = consumer.mint(&published.publication()).unwrap();
    assert_eq!(
        staged.as_presentation_component().err(),
        Some(CapabilityFailure::Revoked)
    );
    published
        .reference()
        .unwrap()
        .as_presentation_component()
        .unwrap();

    host.commit_initialization(&provider).unwrap();
    let Poll::Ready(Ok(page)) = futures::poll!(parked.as_mut()) else {
        panic!("committing initialization must wake the parked reader");
    };
    // Every staged row of one collection lands in a single transaction.
    assert_eq!(page.transactions.len(), 1);
    assert_eq!(page.transactions[0].changes.len(), 2);
    let row = page.transactions[0]
        .changes
        .iter()
        .find_map(|change| match change {
            Change::Upsert { value, .. } if !value.capabilities.is_empty() => Some(value),
            _ => None,
        })
        .unwrap();
    // The capability the row carries is redeemable as soon as the row is readable.
    consumer
        .mint(&row.capabilities[0])
        .unwrap()
        .as_presentation_component()
        .unwrap();
    staged.as_presentation_component().unwrap();
    assert_eq!(shelf.snapshot().len(), 2);
    assert_eq!(menu.snapshot().len(), 1);

    // Once committed, a newly granted writer publishes directly.
    let late = collections.grant_writer(&menu).unwrap();
    late.put(vec![4], None, vec![4]).unwrap();
    assert_eq!(menu.snapshot().len(), 2);
}

#[tokio::test]
async fn failed_initialization_exposes_no_rows_or_capabilities() {
    let host = Host::new();
    let shelf = registrations(3);
    let (lease, through) = observer(&host, &shelf).await;
    let consumer = CapabilitySession::new(
        host.clone(),
        host.start("consumer"),
        CapabilityLimits::default(),
    );
    let parked = lease.read(Some(through), ReadLimits::default(), std::future::pending());
    let mut parked = std::pin::pin!(parked);
    assert!(futures::poll!(parked.as_mut()).is_pending());

    let mut leaked = None;
    let result = host
        .initialize("provider", |provider| {
            let collections =
                CollectionSession::new(host.clone(), provider.clone(), CollectionLimits::default());
            let capabilities =
                CapabilitySession::new(host.clone(), provider, CapabilityLimits::default());
            let published = capabilities.publish_presentation_component().unwrap();
            let writer = collections.grant_writer(&shelf).unwrap();
            writer
                .put_value(
                    vec![1],
                    None,
                    CollectionValue {
                        body: tagged_body(),
                        capabilities: vec![published.publication()],
                    },
                )
                .unwrap();
            let publication = published.publication();
            // Even a publication that escaped during initialization conveys nothing.
            assert_eq!(
                consumer
                    .mint(&publication)
                    .unwrap()
                    .as_presentation_component()
                    .err(),
                Some(CapabilityFailure::Revoked)
            );
            leaked = Some((writer, published, publication));
            async { Err::<(), _>(AccessError::Denied) }
        })
        .await;
    assert_eq!(result.map(drop), Err(AccessError::Denied));
    let (writer, published, publication) = leaked.unwrap();

    assert!(futures::poll!(parked.as_mut()).is_pending());
    assert!(shelf.snapshot().is_empty());
    assert_eq!(
        writer.put(vec![2], None, vec![2]),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(
        consumer
            .mint(&publication)
            .unwrap()
            .as_presentation_component()
            .err(),
        Some(CapabilityFailure::Revoked)
    );
    drop((writer, published));
    // Nothing was ever published, so the reader still sees an untouched collection.
    assert!(futures::poll!(parked.as_mut()).is_pending());
}

#[tokio::test]
async fn closing_withdraws_rows_before_any_owned_teardown() {
    let host = Host::new();
    let shelf = registrations(4);
    let provider = host.start("provider");
    let collections =
        CollectionSession::new(host.clone(), provider.clone(), CollectionLimits::default());
    let writer = collections.grant_writer(&shelf).unwrap();
    writer.put(vec![1], None, vec![1]).unwrap();
    assert_eq!(shelf.snapshot().len(), 1);
    // Registered after the writer, so teardown runs it first.
    let observed = shelf.clone();
    host.own(
        &provider,
        Box::new(move || assert!(observed.snapshot().is_empty())),
    )
    .unwrap();

    assert!(host.begin_close(&provider).unwrap());
    // Outward withdrawal alone removes the rows; the incarnation itself is still live.
    assert!(shelf.snapshot().is_empty());
    assert_eq!(host.check(&provider), Ok(()));
    assert_eq!(
        writer.put(vec![2], None, vec![2]),
        Err(WriteFailure::Revoked)
    );
    host.close_instance(&provider).unwrap();
    drop(writer);
    assert_eq!(collections.writer_count(), 0);
}
