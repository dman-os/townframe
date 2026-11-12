use super::*;

#[test]
fn timeout_preserves_published_progress_before_source_unavailable() {
    for (changes, transactions) in [
        (
            vec![upsert(1, 1, 1)],
            vec![Transaction {
                changes: vec![upsert(1, 1, 1)],
            }],
        ),
        (vec![upsert(1, 1, 20)], Vec::new()),
        (Vec::new(), Vec::new()),
    ] {
        let source = source(8);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let initial = page(&mut lease, None, one()).unwrap();
        let mut read = lease.read(Some(initial.through.clone()), one());
        assert!(
            Pin::new(&mut read)
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        source.publish(changes);
        source.mark_source_unavailable();
        let timeout = read.complete_timeout().unwrap();
        assert_eq!(timeout, initial);
        let cached = page(&mut lease, Some(timeout.through), one()).unwrap();
        assert_eq!(cached.transactions, transactions);
        assert_ne!(cached.through, initial.through);
        assert!(cached.bootstrap_complete);
        assert_eq!(
            lease
                .read(Some(initial.through.clone()), one())
                .complete_timeout()
                .unwrap(),
            initial
        );
        assert_eq!(
            lease
                .read(Some(cached.through.clone()), one())
                .complete_timeout(),
            Err(ReadError::SourceUnavailable)
        );
        assert_eq!(
            page(&mut lease, Some(cached.through.clone()), one()),
            Err(ReadError::SourceUnavailable)
        );
        source.mark_live();
        assert_eq!(
            lease
                .read(Some(cached.through.clone()), one())
                .complete_timeout()
                .unwrap(),
            Page {
                transactions: Vec::new(),
                through: cached.through,
                bootstrap_complete: true,
            }
        );
    }
}

#[test]
fn parked_reads_unregister_on_cancel_and_timeout_without_acknowledging() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let mut context = Context::from_waker(&waker);
    for _ in 0..100 {
        let mut read = lease.read(Some(initial.through.clone()), one());
        assert!(Pin::new(&mut read).poll(&mut context).is_pending());
        assert!(Pin::new(&mut read).poll(&mut context).is_pending());
        assert_eq!(Arc::strong_count(&counter), 3);
        drop(read);
        assert_eq!(Arc::strong_count(&counter), 2);
    }
    let mut read = lease.read(Some(initial.through.clone()), one());
    assert!(Pin::new(&mut read).poll(&mut context).is_pending());
    source.publish(vec![upsert(1, 1, 1)]);
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    let timed_out = read.complete_timeout().unwrap();
    assert_eq!(timed_out, initial);
    assert_eq!(Arc::strong_count(&counter), 2);
    assert_eq!(
        page(&mut lease, Some(timed_out.through), one())
            .unwrap()
            .transactions,
        vec![Transaction {
            changes: vec![upsert(1, 1, 1)]
        }]
    );
}

#[test]
fn reset_wakes_reads_and_timeout_checks_invalidation() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(Arc::clone(&counter));
    let mut read = lease.read(Some(initial.through.clone()), one());
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    source.reset();
    assert_eq!(counter.0.load(Ordering::SeqCst), 1);
    assert_eq!(read.complete_timeout(), Err(ReadError::ResetRequired));
    assert_eq!(page(&mut lease, None, one()), Err(ReadError::ResetRequired));
    assert!(matches!(
        source.acquire(Selector::All, Some(initial.through)),
        Err(AcquireError::ResetRequired)
    ));
    let mut fresh = source.acquire(Selector::All, None).unwrap();
    let empty = page(&mut fresh, None, one()).unwrap();
    assert!(empty.transactions.is_empty());
    assert!(empty.bootstrap_complete);
}

#[test]
fn unavailable_is_recoverable_but_revocation_is_terminal() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 20)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    source.publish(vec![upsert(1, 2, 3)]);
    source.publish(vec![upsert(3, 2, 30)]);
    source.mark_source_unavailable();
    assert!(matches!(
        source.acquire(Selector::All, None),
        Err(AcquireError::SourceUnavailable)
    ));
    let timeout = lease.read(None, one()).complete_timeout().unwrap();
    assert!(timeout.transactions.is_empty());
    assert!(!timeout.bootstrap_complete);
    assert_eq!(lease.read(None, one()).complete_timeout().unwrap(), timeout);
    let first = page(&mut lease, None, one()).unwrap();
    assert_eq!(
        first.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 1, 1)]
        }]
    );
    assert!(!first.bootstrap_complete);
    assert_eq!(
        lease
            .read(Some(first.through.clone()), one())
            .complete_timeout()
            .unwrap(),
        Page {
            transactions: Vec::new(),
            through: first.through.clone(),
            bootstrap_complete: false,
        }
    );
    let second = page(&mut lease, Some(first.through), one()).unwrap();
    assert_eq!(
        second.transactions,
        vec![Transaction {
            changes: vec![upsert(2, 1, 2)]
        }]
    );
    assert!(second.bootstrap_complete);
    let cached = page(&mut lease, Some(second.through), one()).unwrap();
    assert_eq!(
        cached.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, 3)]
        }]
    );
    let empty = page(&mut lease, Some(cached.through.clone()), one()).unwrap();
    assert!(empty.transactions.is_empty());
    assert_ne!(empty.through, cached.through);
    assert_eq!(
        page(&mut lease, Some(empty.through.clone()), one()),
        Err(ReadError::SourceUnavailable)
    );
    assert_eq!(
        lease
            .read(Some(empty.through.clone()), one())
            .complete_timeout(),
        Err(ReadError::SourceUnavailable)
    );
    source.mark_live();
    source.publish(vec![upsert(1, 3, 4)]);
    let recovered = page(&mut lease, Some(empty.through), one()).unwrap();
    assert_eq!(
        recovered.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 3, 4)]
        }]
    );
    source.publish(vec![upsert(1, 4, 5)]);
    let mut bootstrap = source.acquire(Selector::All, None).unwrap();
    let mut pending = lease.read(Some(recovered.through), one());
    source.revoke();
    assert_eq!(
        Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(ReadError::Revoked))
    );
    drop(pending);
    assert_eq!(page(&mut bootstrap, None, one()), Err(ReadError::Revoked));
    source.mark_live();
    source.reset();
    assert_eq!(
        lease.read(None, one()).complete_timeout(),
        Err(ReadError::Revoked)
    );
    assert_eq!(page(&mut lease, None, one()), Err(ReadError::Revoked));
    assert!(matches!(
        source.acquire(Selector::All, None),
        Err(AcquireError::Revoked)
    ));
}

#[test]
fn cancelled_parked_reads_preserve_explicit_acknowledgement() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    let mut read = lease.read(Some(initial.through), one());
    assert!(
        Pin::new(&mut read)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(read);
    assert_eq!(page(&mut lease, None, one()), Err(ReadError::ResetRequired));
}

#[test]
fn source_narrows_selectors_and_lease_drop_releases_interest() {
    let fixture = Fixture::default();
    let interests = Arc::clone(&fixture.interests);
    let source = Projection::with_source(fixture, NonZeroUsize::new(8).unwrap(), [1, 2]);
    assert!(matches!(
        source.acquire(Selector::AtMost(-1), None),
        Err(AcquireError::InvalidSelector)
    ));
    assert!(matches!(
        source.acquire(Selector::AtMost(11), None),
        Err(AcquireError::UnauthorizedSelector)
    ));
    assert!(matches!(
        source.acquire(Selector::All, Some(Cursor(Vec::new()))),
        Err(AcquireError::ResetRequired)
    ));
    assert_eq!(interests.load(Ordering::SeqCst), 0);
    let first = source.acquire(Selector::All, None).unwrap();
    let second = source.acquire(Selector::AtMost(3), None).unwrap();
    assert_eq!(interests.load(Ordering::SeqCst), 2);
    drop(source);
    drop(first);
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    drop(second);
    assert_eq!(interests.load(Ordering::SeqCst), 0);
}
