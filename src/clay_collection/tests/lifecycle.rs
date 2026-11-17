use std::task::Wake;

use super::*;

#[test]
fn acknowledged_snapshot_and_live_progress_cannot_be_rolled_back() {
    for (live, timeout) in [(false, false), (false, true), (true, false), (true, true)] {
        let (source, _) = projection(8);
        source.publish(vec![upsert(0, 1, Group::X), upsert(1, 2, Group::X)]);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let partial = read(&mut lease, None, one()).unwrap();
        assert!(!partial.bootstrap_complete);
        let completed = read(&mut lease, Some(partial.through.clone()), one()).unwrap();
        assert!(completed.bootstrap_complete);
        let acknowledged = lease
            .read(Some(completed.through.clone()), one())
            .complete_timeout()
            .unwrap();
        assert!(acknowledged.bootstrap_complete);
        let rollback = if live {
            source.publish(vec![remove(0, 3)]);
            let page = read(&mut lease, Some(acknowledged.through), one()).unwrap();
            assert!(page.bootstrap_complete);
            assert!(
                lease
                    .read(Some(page.through), one())
                    .complete_timeout()
                    .unwrap()
                    .bootstrap_complete
            );
            completed.through
        } else {
            partial.through
        };
        let result = if timeout {
            lease.read(Some(rollback), one()).complete_timeout()
        } else {
            read(&mut lease, Some(rollback), one())
        };
        assert_eq!(result, Err(ReadError::ResetRequired));
    }
}

#[derive(Default)]
struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn canceling_a_pending_read_releases_its_waiter_without_releasing_lease_interest() {
    let (source, interests) = projection(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let baseline = read(&mut lease, None, one()).unwrap();
    let count = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&count));
    let refs = Arc::strong_count(&count);
    let mut pending = lease.read(Some(baseline.through.clone()), one());
    assert!(poll(&mut pending, &waker).is_pending());
    assert_eq!(Arc::strong_count(&count), refs + 1);
    drop(pending);
    assert_eq!(Arc::strong_count(&count), refs);
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    source.publish(vec![upsert(0, 1, Group::X)]);
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    let page = read(&mut lease, Some(baseline.through), one()).unwrap();
    assert_eq!(
        page.transactions,
        vec![Transaction {
            changes: vec![upsert(0, 1, Group::X)]
        }]
    );
    let mut pending = lease.read(Some(page.through), one());
    assert!(poll(&mut pending, &waker).is_pending());
    source.publish(vec![upsert(1, 2, Group::Y)]);
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&count), refs);
    let Poll::Ready(Ok(page)) = poll(&mut pending, &waker) else {
        panic!("published progress must wake and complete the pending read");
    };
    assert_eq!(
        page.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, Group::Y)]
        }]
    );
    drop(pending);
    drop(lease);
    assert_eq!(Arc::strong_count(&count), refs);
    assert_eq!(interests.load(Ordering::SeqCst), 0);
}

#[test]
fn independent_waiters_wake_and_dropping_one_lease_releases_only_its_interest() {
    let (source, interests) = projection(8);
    let mut left = source.acquire(Selector::X, None).unwrap();
    let mut right = source.acquire(Selector::All, None).unwrap();
    let a = read(&mut left, None, one()).unwrap().through;
    let b = read(&mut right, None, one()).unwrap().through;
    let count = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&count));
    let refs = Arc::strong_count(&count);
    let mut a = left.read(Some(a), one());
    let mut b = right.read(Some(b), one());
    assert!(poll(&mut a, &waker).is_pending());
    assert!(poll(&mut b, &waker).is_pending());
    assert_eq!(Arc::strong_count(&count), refs + 2);
    drop(a);
    drop(left);
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&count), refs + 1);
    source.publish(vec![upsert(0, 1, Group::Y)]);
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert!(matches!(poll(&mut b, &waker), Poll::Ready(Ok(_))));
    drop(b);
    drop(source);
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    drop(right);
    assert_eq!(interests.load(Ordering::SeqCst), 0);
}

#[test]
fn timeout_is_empty_progress_and_does_not_acknowledge_undelivered_changes() {
    let (source, _) = projection(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let baseline = read(&mut lease, None, one()).unwrap();
    let count = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&count));
    let refs = Arc::strong_count(&count);
    let mut pending = lease.read(Some(baseline.through.clone()), one());
    assert!(poll(&mut pending, &waker).is_pending());
    let timeout = pending.complete_timeout().unwrap();
    assert!(timeout.transactions.is_empty());
    assert_eq!(timeout.through, baseline.through);
    assert!(timeout.bootstrap_complete);
    assert_eq!(Arc::strong_count(&count), refs);
    source.publish(vec![upsert(0, 1, Group::X)]);
    let timeout = lease
        .read(Some(timeout.through), one())
        .complete_timeout()
        .unwrap();
    assert!(timeout.transactions.is_empty());
    assert_eq!(timeout.through, baseline.through);
    let page = read(&mut lease, Some(timeout.through), one()).unwrap();
    assert_eq!(
        page.transactions,
        vec![Transaction {
            changes: vec![upsert(0, 1, Group::X)]
        }]
    );
}

#[test]
fn timeout_cannot_bypass_reset_or_revocation_and_both_wake_pending_reads() {
    for revoked in [false, true] {
        let (source, interests) = projection(8);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let baseline = read(&mut lease, None, one()).unwrap();
        let count = Arc::new(Counter::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut pending = lease.read(Some(baseline.through), one());
        assert!(poll(&mut pending, &waker).is_pending());
        if revoked {
            source.revoke();
        } else {
            source.reset();
        }
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        let expected = if revoked {
            ReadError::Revoked
        } else {
            ReadError::ResetRequired
        };
        assert_eq!(pending.complete_timeout(), Err(expected));
        assert_eq!(read(&mut lease, None, one()), Err(expected));
        drop(lease);
        assert_eq!(interests.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn unavailable_source_drains_cached_progress_and_recovers_on_the_same_lease() {
    let (source, interests) = projection(8);
    source.publish(vec![upsert(0, 1, Group::X)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    source.publish(vec![upsert(1, 2, Group::Y)]);
    source.mark_source_unavailable();
    assert_eq!(
        source.acquire(Selector::All, None).unwrap_err(),
        AcquireError::SourceUnavailable
    );
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    let mut mirror = Mirror::empty();
    assert_eq!(
        apply(&mut lease, &mut mirror, one()).transactions,
        vec![Transaction {
            changes: vec![upsert(0, 1, Group::X)],
        }]
    );
    apply(&mut lease, &mut mirror, one());
    assert_eq!(mirror.len(), 2);
    assert_eq!(
        read(&mut lease, mirror.committed().cloned(), one()),
        Err(ReadError::SourceUnavailable)
    );
    assert_eq!(
        lease
            .read(mirror.committed().cloned(), one())
            .complete_timeout(),
        Err(ReadError::SourceUnavailable)
    );
    source.mark_live();
    let count = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&count));
    let mut pending = lease.read(mirror.committed().cloned(), one());
    assert!(poll(&mut pending, &waker).is_pending());
    source.mark_source_unavailable();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        poll(&mut pending, &waker),
        Poll::Ready(Err(ReadError::SourceUnavailable))
    );
    drop(pending);
    source.mark_live();
    source.publish(vec![remove(0, 3)]);
    apply(&mut lease, &mut mirror, one());
    assert_eq!(
        mirror.rows(),
        vec![(
            1,
            Row {
                group: Group::Y,
                value: 2
            }
        )]
    );
    source.publish(vec![upsert(2, 4, Group::X)]);
    source.revoke();
    assert_eq!(
        read(&mut lease, mirror.committed().cloned(), one()),
        Err(ReadError::Revoked)
    );
    source.mark_live();
    assert_eq!(
        source.acquire(Selector::All, None).unwrap_err(),
        AcquireError::Revoked
    );
    assert_eq!(
        lease
            .read(mirror.committed().cloned(), one())
            .complete_timeout(),
        Err(ReadError::Revoked)
    );
    drop(lease);
    assert_eq!(interests.load(Ordering::SeqCst), 0);
}
