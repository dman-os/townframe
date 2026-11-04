//! Staged initialization (ADR 024 §5) as seen through a collection session: rows and capability
//! publications made while an incarnation initializes are invisible until it commits, appear
//! together when it does, vanish unseen when it fails, and are withdrawn when it is torn down after closing.
use super::*;
use crate::capabilities::CapabilityFailure;
use crate::capabilities::CapabilityLimits;
use crate::capabilities::CapabilitySession;
use futures::FutureExt as _;
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
    let published = capabilities.publish_presentation("view").unwrap();
    let first = collections
        .grant_writer(full_grant(&shelf, "provider"))
        .unwrap();
    let second = collections
        .grant_writer(full_grant(&shelf, "provider"))
        .unwrap();
    let elsewhere = collections
        .grant_writer(full_grant(&menu, "provider"))
        .unwrap();
    let stamp = first
        .put_value(
            vec![1],
            WriteBasis::Concurrent,
            CollectionValue {
                body: tagged_body(),
                capabilities: vec![published.publication()],
            },
        )
        .unwrap();
    // Compare-and-set works against staged rows exactly as against published ones.
    assert_eq!(
        first.put(vec![1], WriteBasis::CompareAndSet(None), vec![7]),
        Err(WriteFailure::Conflict {
            current: Some(stamp)
        })
    );
    second
        .put(vec![2], WriteBasis::Concurrent, vec![2])
        .unwrap();
    elsewhere
        .put(vec![3], WriteBasis::Concurrent, vec![3])
        .unwrap();

    let parked = lease.read(Some(through), ReadLimits::default(), std::future::pending());
    let mut parked = std::pin::pin!(parked);
    assert!(futures::poll!(parked.as_mut()).is_pending());
    assert!(shelf.snapshot().is_empty());
    assert!(menu.snapshot().is_empty());
    // The staged publication is redeemable by its owner only.
    let staged = consumer.mint(&published.publication()).unwrap();
    assert_eq!(
        staged.redeem_presentation().await.err(),
        Some(CapabilityFailure::Revoked)
    );
    published
        .reference()
        .unwrap()
        .redeem_presentation()
        .await
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
            Change::Upsert { value, .. }
                if value
                    .value
                    .as_ref()
                    .is_some_and(|value| !value.capabilities.is_empty()) =>
            {
                value.value.as_ref()
            }
            _ => None,
        })
        .unwrap();
    // The capability the row carries is redeemable as soon as the row is readable.
    consumer
        .mint(&row.capabilities[0])
        .unwrap()
        .redeem_presentation()
        .await
        .unwrap();
    staged.redeem_presentation().await.unwrap();
    assert_eq!(shelf.snapshot().len(), 2);
    assert_eq!(menu.snapshot().len(), 1);

    // Once committed, a newly granted writer publishes directly.
    let late = collections
        .grant_writer(full_grant(&menu, "provider"))
        .unwrap();
    late.put(vec![4], WriteBasis::Concurrent, vec![4]).unwrap();
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
            let published = capabilities.publish_presentation("view").unwrap();
            let writer = collections
                .grant_writer(full_grant(&shelf, "provider"))
                .unwrap();
            writer
                .put_value(
                    vec![1],
                    WriteBasis::Concurrent,
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
                    .redeem_presentation()
                    .now_or_never()
                    .unwrap()
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
        writer.put(vec![2], WriteBasis::Concurrent, vec![2]),
        Err(WriteFailure::Revoked)
    );
    assert_eq!(
        consumer
            .mint(&publication)
            .unwrap()
            .redeem_presentation()
            .await
            .err(),
        Some(CapabilityFailure::Revoked)
    );
    drop((writer, published));
    // Nothing was ever published, so the reader still sees an untouched collection.
    assert!(futures::poll!(parked.as_mut()).is_pending());
}

#[tokio::test]
async fn closing_keeps_rows_through_cleanup_and_teardown_withdraws_them() {
    let host = Host::new();
    let shelf = registrations(4);
    let provider = host.start("provider");
    let collections =
        CollectionSession::new(host.clone(), provider.clone(), CollectionLimits::default());
    let writer = collections
        .grant_writer(full_grant(&shelf, "provider"))
        .unwrap();
    writer
        .put(vec![1], WriteBasis::Concurrent, vec![1])
        .unwrap();
    assert_eq!(shelf.snapshot().len(), 1);
    // Registered after the writer, so teardown runs it first: cleanup still sees the rows.
    let observed = shelf.clone();
    host.own(
        &provider,
        Box::new(move || assert_eq!(observed.snapshot().len(), 2)),
    )
    .unwrap();

    assert!(host.begin_close(&provider).unwrap());
    // Outward withdrawal alone keeps the scope: the incarnation's authority is intact until
    // teardown (ADR 024 §6).
    assert_eq!(shelf.snapshot().len(), 1);
    assert_eq!(host.check(&provider), Ok(()));
    writer
        .put(vec![2], WriteBasis::Concurrent, vec![2])
        .unwrap();
    host.close_instance(&provider).unwrap();
    assert!(shelf.snapshot().is_empty());
    assert_eq!(
        writer.put(vec![3], WriteBasis::Concurrent, vec![3]),
        Err(WriteFailure::Revoked)
    );
    drop(writer);
    assert_eq!(collections.writer_count(), 0);
}
