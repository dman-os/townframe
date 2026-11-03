use super::*;
use clay_collection::RowEvidence;
use clay_collection::Version;
use std::num::NonZeroUsize;
use std::task::Poll;

fn projection() -> BytesProjection {
    Projection::with_source(BytesSource, NonZeroUsize::new(8).unwrap(), [1, 2])
}

fn session(limits: CollectionLimits) -> (CollectionSession, BytesProjection, Host, Instance) {
    let host = Host::new();
    let instance = host.start("collections");
    (
        CollectionSession::new(host.clone(), instance.clone(), limits),
        projection(),
        host,
        instance,
    )
}

fn upsert(
    key: &[u8],
    value: &[u8],
    write_context: Option<Vec<u8>>,
) -> Change<Bytes, CollectionValue> {
    Change::Upsert {
        key: key.to_vec(),
        row_version: Version::new(1, 1),
        value: CollectionValue::plain(value.to_vec()),
        evidence: RowEvidence::Authoritative,
        write_context,
    }
}

fn one() -> ReadLimits {
    ReadLimits {
        max_transactions: Some(1),
        max_changes: Some(1),
    }
}

#[test]
fn reader_and_lease_clones_share_permits_and_drops_release_them() {
    let (session, reader_projection, _, _) = session(CollectionLimits {
        readers: 2,
        ..CollectionLimits::default()
    });
    let reader = session.grant(reader_projection).unwrap();
    let reader_clone = reader.clone();
    assert_eq!(session.reader_count(), 1);

    let lease = reader.acquire(Vec::new(), None).unwrap();
    let lease_clone = lease.clone();
    assert_eq!(session.reader_count(), 2);
    assert_eq!(
        reader.acquire(Vec::new(), None).err(),
        Some(AcquireError::UnauthorizedSelector)
    );

    drop(lease_clone);
    assert_eq!(session.reader_count(), 2);
    drop(lease);
    assert_eq!(session.reader_count(), 1);
    drop(reader_clone);
    assert_eq!(session.reader_count(), 1);
    drop(reader);
    assert_eq!(session.reader_count(), 0);
    let _reader = session.grant(projection()).unwrap();
    assert_eq!(session.reader_count(), 1);
}

#[test]
fn default_reader_capacity_admits_64_and_releases_on_drop() {
    let (session, projection, _, _) = session(CollectionLimits::default());
    let mut readers = (0..64)
        .map(|_| session.grant(projection.clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(session.reader_count(), 64);
    assert_eq!(
        session.grant(projection.clone()).err(),
        Some(AccessError::Denied)
    );
    assert_eq!(session.reader_count(), 64);

    drop(readers.pop());
    assert_eq!(session.reader_count(), 63);
    let _reader = session.grant(projection).unwrap();
    assert_eq!(session.reader_count(), 64);
}

#[tokio::test]
async fn acquire_enforces_selector_and_resume_cursor_ceilings() {
    let (session, projection, _, _) = session(CollectionLimits {
        selector_bytes: 3,
        cursor_bytes: 56,
        ..CollectionLimits::default()
    });
    projection.publish(vec![upsert(b"key", b"value", None)]);
    let reader = session.grant(projection).unwrap();
    assert_eq!(
        reader.acquire(vec![0; 4], None).err(),
        Some(AcquireError::InvalidSelector)
    );
    assert_eq!(session.reader_count(), 1);

    let lease = reader.acquire(vec![0; 3], None).unwrap();
    let page = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let resumed = reader
        .acquire(vec![0; 3], Some(page.through.clone()))
        .unwrap();
    assert_eq!(session.reader_count(), 3);
    drop(resumed);
    assert_eq!(
        reader.acquire(vec![0; 3], Some(Cursor(vec![0; 57]))).err(),
        Some(AcquireError::ResetRequired)
    );
    assert_eq!(session.reader_count(), 2);
}

#[tokio::test]
async fn read_enforces_committed_cursor_ceiling_without_invalidating_lease() {
    let (session, projection, _, _) = session(CollectionLimits {
        cursor_bytes: 56,
        ..CollectionLimits::default()
    });
    projection.publish(vec![upsert(b"a", b"1", None)]);
    projection.publish(vec![upsert(b"b", b"2", None)]);
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let first = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(
        lease
            .read(Some(Cursor(vec![0; 57])), one(), std::future::pending())
            .await,
        Err(ReadFailure::Read(ReadError::ResetRequired))
    );
    let second = lease
        .read(Some(first.through), one(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(second.transactions.len(), 1);
}

#[tokio::test]
async fn read_clamps_transaction_change_and_poll_limits() {
    let (session, projection, _, _) = session(CollectionLimits {
        max_transactions: 1,
        max_changes: 1,
        max_poll_timeout: Duration::from_millis(25),
        ..CollectionLimits::default()
    });
    projection.publish(vec![upsert(b"a", b"1", None)]);
    projection.publish(vec![upsert(b"b", b"2", None)]);
    projection.publish(vec![upsert(b"c", b"3", None)]);
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    assert_eq!(
        lease.poll_timeout(Duration::from_millis(10)),
        Duration::from_millis(10)
    );
    assert_eq!(
        lease.poll_timeout(Duration::from_secs(1)),
        Duration::from_millis(25)
    );

    let first = lease
        .read(None, ReadLimits::default(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(first.transactions.len(), 1);
    assert_eq!(first.transactions[0].changes.len(), 1);
    let second = lease
        .read(
            Some(first.through),
            ReadLimits {
                max_transactions: Some(u32::MAX),
                max_changes: Some(u32::MAX),
            },
            std::future::pending(),
        )
        .await
        .unwrap();
    assert_eq!(second.transactions.len(), 1);
    assert_eq!(second.transactions[0].changes.len(), 1);
}

#[tokio::test]
async fn concurrent_read_returns_busy() {
    let (session, projection, _, _) = session(CollectionLimits::default());
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let initial = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let mut read =
        Box::pin(lease.read(Some(initial.through.clone()), one(), std::future::pending()));
    std::future::poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;

    assert_eq!(
        lease
            .read(Some(initial.through), one(), std::future::ready(()))
            .await,
        Err(ReadFailure::Busy)
    );
}

#[tokio::test]
async fn publication_wakes_a_long_poll() {
    let (session, projection, _, _) = session(CollectionLimits::default());
    let reader = session.grant(projection.clone()).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let initial = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let mut read = Box::pin(lease.read(Some(initial.through), one(), std::future::pending()));
    std::future::poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;

    projection.publish(vec![upsert(b"key", b"value", None)]);
    let page = read.await.unwrap();
    assert_eq!(page.transactions.len(), 1);
    assert_eq!(
        page.transactions[0].changes,
        vec![upsert(b"key", b"value", None)]
    );
}

#[tokio::test]
async fn cancellation_completes_a_long_poll_as_revoked() {
    let (session, projection, _, _) = session(CollectionLimits {
        readers: 2,
        ..CollectionLimits::default()
    });
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let initial = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let mut read = Box::pin(lease.read(Some(initial.through), one(), std::future::pending()));
    std::future::poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;

    lease.cancel();
    assert_eq!(read.await, Err(ReadFailure::Read(ReadError::Revoked)));
    assert!(lease.inner.lease.lock().unwrap().is_some());
    assert_eq!(session.reader_count(), 2);
    drop(lease);
    assert_eq!(session.reader_count(), 1);
    let _replacement = reader.acquire(Vec::new(), None).unwrap();
    assert_eq!(session.reader_count(), 2);
}

#[tokio::test]
async fn source_reset_completes_a_long_poll_and_permits_a_fresh_lease() {
    let (session, projection, _, _) = session(CollectionLimits::default());
    projection.publish(vec![upsert(b"old", b"value", None)]);
    let reader = session.grant(projection.clone()).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let initial = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let mut read = Box::pin(lease.read(Some(initial.through), one(), std::future::pending()));
    std::future::poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;

    projection.reset();
    assert_eq!(read.await, Err(ReadFailure::Read(ReadError::ResetRequired)));
    projection.publish(vec![upsert(b"fresh", b"value", None)]);
    let fresh = reader.acquire(Vec::new(), None).unwrap();
    let page = fresh
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(
        page.transactions[0].changes,
        vec![upsert(b"fresh", b"value", None)]
    );
}

#[tokio::test]
async fn host_revocation_completes_a_long_poll_and_blocks_acquire() {
    let (session, projection, host, instance) = session(CollectionLimits::default());
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let initial = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let mut read = Box::pin(lease.read(Some(initial.through), one(), std::future::pending()));
    std::future::poll_fn(|context| {
        assert!(read.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;

    host.close_instance(&instance).unwrap();
    assert_eq!(read.await, Err(ReadFailure::Read(ReadError::Revoked)));
    assert_eq!(
        reader.acquire(Vec::new(), None).err(),
        Some(AcquireError::Revoked)
    );
    assert_eq!(session.reader_count(), 2);
}

#[tokio::test]
async fn page_byte_limit_rejects_oversized_real_pages() {
    let (session, projection, _, _) = session(CollectionLimits {
        page_bytes: 65,
        ..CollectionLimits::default()
    });
    projection.publish(vec![upsert(b"key", b"value", Some(vec![1, 2]))]);
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    assert_eq!(
        lease.read(None, one(), std::future::pending()).await,
        Err(ReadFailure::PageLimit)
    );
    assert!(lease.inner.lease.lock().unwrap().is_some());
}

#[tokio::test]
async fn timeout_completes_a_caught_up_read_with_an_empty_page() {
    let (session, projection, _, _) = session(CollectionLimits::default());
    let reader = session.grant(projection).unwrap();
    let lease = reader.acquire(Vec::new(), None).unwrap();
    let initial = lease
        .read(None, one(), std::future::pending())
        .await
        .unwrap();
    let page = lease
        .read(Some(initial.through.clone()), one(), std::future::ready(()))
        .await
        .unwrap();
    assert!(page.transactions.is_empty());
    assert_eq!(page.through, initial.through);
    assert!(page.bootstrap_complete);
}
