use super::*;

#[test]
fn page_limits_and_timeouts_do_not_clone_undelivered_candidates() {
    struct Counted(Arc<AtomicUsize>);

    impl Clone for Counted {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::SeqCst);
            Self(Arc::clone(&self.0))
        }
    }

    struct CountingSource(Arc<AtomicUsize>);

    impl Source<u32, Counted> for CountingSource {
        type Selector = ();
        type Interest = ();

        fn acquire(&self, selector: ()) -> Result<((), ()), AcquireError> {
            Ok((selector, ()))
        }

        fn matches(&self, _selector: &(), key: &u32, _value: &Counted) -> bool {
            if *key > 1 {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            true
        }
    }

    let candidate_matches = Arc::new(AtomicUsize::new(0));
    let source = Projection::with_source(
        CountingSource(Arc::clone(&candidate_matches)),
        NonZeroUsize::new(8).unwrap(),
        [1, 2],
    );
    let mut lease = source.acquire((), None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    let counts = std::array::from_fn::<_, 4, _>(|_| Arc::new(AtomicUsize::new(0)));
    for keys in [1..2, 2..4, 4..5] {
        source.publish(
            keys.map(|key| Change::Upsert {
                key,
                row_version: Version::new(1, 1),
                value: Counted(Arc::clone(&counts[key as usize - 1])),
                evidence: RowEvidence::Authoritative,
                write_context: None,
            })
            .collect(),
        );
    }
    for unavailable in [false, true] {
        if unavailable {
            source.mark_source_unavailable();
        }
        for count in &counts {
            count.store(0, Ordering::SeqCst);
        }
        candidate_matches.store(0, Ordering::SeqCst);
        let timeout = lease
            .read(Some(initial.through.clone()), one())
            .complete_timeout()
            .unwrap();
        assert!(timeout.transactions.is_empty());
        assert_eq!(timeout.through, initial.through);
        assert_eq!(
            counts.each_ref().map(|count| count.load(Ordering::SeqCst)),
            [0; 4]
        );
        assert_eq!(candidate_matches.load(Ordering::SeqCst), 0);
    }
    source.mark_live();
    let cursors = [
        ReadLimits {
            max_transactions: Some(1),
            max_changes: None,
        },
        ReadLimits {
            max_transactions: None,
            max_changes: Some(1),
        },
    ]
    .map(|limits| {
        for count in &counts {
            count.store(0, Ordering::SeqCst);
        }
        candidate_matches.store(0, Ordering::SeqCst);
        let result = page(&mut lease, Some(initial.through.clone()), limits).unwrap();
        assert_eq!(result.transactions.len(), 1);
        assert_eq!(
            counts.each_ref().map(|count| count.load(Ordering::SeqCst)),
            [1, 0, 0, 0]
        );
        if limits.max_transactions.is_some() {
            assert_eq!(candidate_matches.load(Ordering::SeqCst), 0);
        }
        result.through
    });
    assert_eq!(cursors[0], cursors[1]);
    candidate_matches.store(0, Ordering::SeqCst);
    let next = page(&mut lease, Some(cursors[1].clone()), one()).unwrap();
    assert_eq!(next.transactions.len(), 1);
    assert_eq!(
        next.transactions[0]
            .changes
            .iter()
            .map(Change::key)
            .copied()
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert!(next.bootstrap_complete);
    assert_eq!(
        counts.each_ref().map(|count| count.load(Ordering::SeqCst)),
        [1, 1, 1, 0]
    );
    assert_eq!(candidate_matches.load(Ordering::SeqCst), 2);
}

#[test]
fn oversized_transactions_are_atomic_and_empty_work_advances() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    assert!(initial.bootstrap_complete);
    source.publish(vec![upsert(1, 1, 30)]);
    let empty = page(&mut lease, Some(initial.through.clone()), one()).unwrap();
    assert!(empty.transactions.is_empty());
    assert_ne!(empty.through, initial.through);
    let changes = vec![upsert(2, 1, 1), upsert(3, 1, 2), upsert(4, 1, 3)];
    source.publish(changes.clone());
    source.publish(vec![upsert(5, 1, 4)]);
    let result = page(&mut lease, Some(empty.through.clone()), one()).unwrap();
    assert_eq!(result.transactions, vec![Transaction { changes }]);
    assert_eq!(
        page(&mut lease, Some(empty.through), one()).unwrap(),
        result
    );
}

#[test]
fn live_retries_use_the_committed_boundary_not_the_last_delivery() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    let oversized = vec![upsert(1, 1, 1), upsert(2, 1, 2)];
    source.publish(oversized.clone());
    source.publish(vec![upsert(3, 1, 3)]);
    source.publish(vec![upsert(4, 1, 30)]);
    let complete = page(
        &mut lease,
        Some(initial.through.clone()),
        ReadLimits::default(),
    )
    .unwrap();
    let first = page(&mut lease, Some(initial.through.clone()), one()).unwrap();
    assert_eq!(first.transactions, vec![Transaction { changes: oversized }]);
    assert!(first.bootstrap_complete);
    assert_eq!(
        page(&mut lease, Some(initial.through.clone()), one()).unwrap(),
        first
    );
    assert_eq!(
        page(&mut lease, Some(initial.through), ReadLimits::default()).unwrap(),
        complete
    );
    let second = page(&mut lease, Some(first.through), one()).unwrap();
    assert_eq!(
        second.transactions,
        vec![Transaction {
            changes: vec![upsert(3, 1, 3)]
        }]
    );
    let filtered = page(&mut lease, Some(second.through), one()).unwrap();
    assert!(filtered.transactions.is_empty());
    assert_eq!(filtered.through, complete.through);
    assert!(filtered.bootstrap_complete);
}
