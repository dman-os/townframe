use super::*;

#[test]
fn foreign_selector_source_and_lease_cursors_reset_and_invalidate() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    assert!(matches!(
        source.acquire(Selector::AtMost(5), Some(initial.through.clone())),
        Err(AcquireError::ResetRequired)
    ));
    let other = self::source(8);
    assert!(matches!(
        other.acquire(Selector::All, Some(initial.through.clone())),
        Err(AcquireError::ResetRequired)
    ));
    let mut replacement = source
        .acquire(Selector::All, Some(initial.through.clone()))
        .unwrap();
    assert_eq!(
        page(&mut replacement, Some(initial.through), one()),
        Err(ReadError::ResetRequired)
    );
    assert_eq!(
        page(&mut replacement, None, one()),
        Err(ReadError::ResetRequired)
    );
    assert_eq!(
        page(&mut lease, Some(Cursor(vec![0])), one()),
        Err(ReadError::ResetRequired)
    );
    assert_eq!(page(&mut lease, None, one()), Err(ReadError::ResetRequired));
}

#[test]
fn acknowledgement_rejects_none_and_older_issued_boundaries() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 3)]);
    for rollback in [false, true] {
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let first = page(&mut lease, None, one()).unwrap();
        let second = page(&mut lease, Some(first.through.clone()), one()).unwrap();
        let third = page(&mut lease, Some(second.through), one()).unwrap();
        let committed = rollback.then_some(first.through);
        assert_eq!(
            page(&mut lease, committed, one()),
            Err(ReadError::ResetRequired)
        );
        assert_eq!(
            lease.read(Some(third.through), one()).complete_timeout(),
            Err(ReadError::ResetRequired)
        );
    }
}

#[test]
fn decoded_but_unissued_snapshot_boundaries_invalidate_the_lease() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 3)]);
    for snapshot in [0_u64, 2, 3] {
        for timeout in [false, true] {
            let mut lease = source.acquire(Selector::All, None).unwrap();
            let first = page(&mut lease, None, one()).unwrap();
            let mut forged = first.through.clone();
            forged.0[40..48].copy_from_slice(&snapshot.to_be_bytes());
            let result = if timeout {
                lease.read(Some(forged), one()).complete_timeout()
            } else {
                page(&mut lease, Some(forged), one())
            };
            assert_eq!(result, Err(ReadError::ResetRequired));
            assert_eq!(
                page(&mut lease, Some(first.through), one()),
                Err(ReadError::ResetRequired)
            );
        }
    }
}

#[test]
fn decoded_but_unissued_live_boundaries_cannot_skip_transactions() {
    let source = source(8);
    for sequence in [1_u64, 2] {
        for timeout in [false, true] {
            source.reset();
            let mut lease = source.acquire(Selector::All, None).unwrap();
            let initial = page(&mut lease, None, one()).unwrap();
            source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2)]);
            source.publish(vec![upsert(3, 1, 30)]);
            source.publish(vec![upsert(4, 1, 4)]);
            let complete = page(&mut lease, Some(initial.through), ReadLimits::default()).unwrap();
            let mut forged = complete.through.clone();
            forged.0[48..56].copy_from_slice(&sequence.to_be_bytes());
            let result = if timeout {
                lease.read(Some(forged), one()).complete_timeout()
            } else {
                page(&mut lease, Some(forged), one())
            };
            assert_eq!(result, Err(ReadError::ResetRequired));
            assert_eq!(
                page(&mut lease, Some(complete.through), one()),
                Err(ReadError::ResetRequired)
            );
        }
    }
}

#[test]
fn scope_eviction_preserves_partial_bootstrap_and_acknowledged_retries() {
    let source = source(1);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 3)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let first = page(&mut lease, None, one()).unwrap();
    drop(source.acquire(Selector::All, None).unwrap());
    assert_eq!(page(&mut lease, None, one()).unwrap(), first);
    let complete = page(&mut lease, None, ReadLimits::default()).unwrap();
    assert!(complete.bootstrap_complete);
    let second = page(&mut lease, Some(first.through.clone()), one()).unwrap();
    assert!(!second.bootstrap_complete);
    let remaining = page(
        &mut lease,
        Some(first.through.clone()),
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(remaining.transactions, complete.transactions[1..]);
    assert!(remaining.bootstrap_complete);
    assert_eq!(
        page(&mut lease, Some(first.through), one()).unwrap(),
        second
    );
    assert!(matches!(
        source.acquire(Selector::All, Some(remaining.through.clone())),
        Err(AcquireError::ResetRequired)
    ));
    assert_eq!(
        lease
            .read(Some(remaining.through.clone()), one())
            .complete_timeout()
            .unwrap(),
        Page {
            transactions: Vec::new(),
            through: remaining.through.clone(),
            bootstrap_complete: true,
        }
    );
    source.publish(vec![upsert(1, 2, 4)]);
    let live = page(&mut lease, Some(remaining.through), one()).unwrap();
    assert_eq!(
        live.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, 4)],
        }]
    );
    assert!(live.bootstrap_complete);
}

#[test]
fn retained_issued_boundaries_survive_compaction_and_acknowledgement_pruning() {
    let source = source(2);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    source.publish(vec![upsert(1, 1, 1)]);
    source.publish(vec![upsert(1, 2, 2)]);
    let first = page(&mut lease, Some(initial.through.clone()), one()).unwrap();
    let second = page(&mut lease, Some(initial.through), ReadLimits::default()).unwrap();
    source.publish(vec![upsert(1, 3, 3)]);
    let third = page(&mut lease, Some(second.through.clone()), one()).unwrap();
    assert_eq!(
        third.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 3, 3)]
        }]
    );
    source.publish(vec![upsert(1, 4, 4)]);
    assert_eq!(
        page(&mut lease, Some(second.through), one()).unwrap(),
        third
    );
    let fourth = page(&mut lease, Some(third.through), one()).unwrap();
    assert_eq!(
        fourth.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 4, 4)]
        }]
    );
    assert_eq!(
        page(&mut lease, Some(first.through), one()),
        Err(ReadError::ResetRequired)
    );
}

#[test]
fn retention_and_scope_eviction_fail_safely_without_losing_current_state() {
    let source = source(1);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    source.publish(vec![upsert(1, 1, 1)]);
    source.publish(vec![upsert(1, 2, 2)]);
    assert_eq!(
        page(&mut lease, Some(initial.through.clone()), one()),
        Err(ReadError::ResetRequired)
    );
    assert!(matches!(
        source.acquire(Selector::All, Some(initial.through)),
        Err(AcquireError::ResetRequired)
    ));
    let mut fresh = source.acquire(Selector::All, None).unwrap();
    let current = page(&mut fresh, None, one()).unwrap();
    assert_eq!(
        current.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, 2)]
        }]
    );
    drop(source.acquire(Selector::All, None).unwrap());
    assert!(matches!(
        source.acquire(Selector::All, Some(current.through.clone())),
        Err(AcquireError::ResetRequired)
    ));
    source.publish(vec![upsert(1, 3, 3)]);
    let live = page(&mut fresh, Some(current.through), one()).unwrap();
    assert_eq!(
        live.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 3, 3)]
        }]
    );
    assert!(live.bootstrap_complete);
}
