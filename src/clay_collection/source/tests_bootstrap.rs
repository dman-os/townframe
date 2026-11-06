use super::*;

#[test]
fn bootstrap_is_selected_current_state_not_retained_history() {
    let source = source(1);
    source.publish(vec![upsert(1, 1, 3), upsert(2, 1, 4)]);
    source.publish(vec![upsert(1, 2, 5), remove(2, 2), upsert(3, 1, 20)]);
    source.publish(Vec::new());
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let result = page(&mut lease, None, one()).unwrap();
    assert_eq!(
        result.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, 5)],
        }]
    );
    assert!(result.bootstrap_complete);
    assert_eq!(page(&mut lease, None, one()).unwrap(), result);
}

#[test]
fn bootstrap_boundary_has_no_live_gap_and_overlaps_are_independent() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 3), upsert(2, 1, 7)]);
    let mut narrow = source.acquire(Selector::AtMost(5), None).unwrap();
    let mut broad = source.acquire(Selector::All, None).unwrap();
    source.publish(vec![upsert(1, 2, 8), upsert(2, 2, 9)]);
    let narrow_bootstrap = page(&mut narrow, None, one()).unwrap();
    assert!(narrow_bootstrap.bootstrap_complete);
    let first = page(&mut broad, None, one()).unwrap();
    assert!(!first.bootstrap_complete);
    let second = page(&mut broad, Some(first.through), one()).unwrap();
    assert!(second.bootstrap_complete);
    let narrow_live = page(&mut narrow, Some(narrow_bootstrap.through), one()).unwrap();
    assert_eq!(
        narrow_live.transactions,
        vec![Transaction {
            changes: vec![remove(1, 2)]
        }]
    );
    assert!(narrow_live.bootstrap_complete);
    let broad_live = page(&mut broad, Some(second.through), one()).unwrap();
    assert_eq!(
        broad_live.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, 8), upsert(2, 2, 9)],
        }]
    );
    assert!(broad_live.bootstrap_complete);
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
fn resume_first_read_uses_none_and_catches_up_to_its_opening_boundary() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1)]);
    let mut first = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut first, None, one()).unwrap();
    drop(first);
    source.publish(vec![upsert(1, 2, 2)]);
    source.publish(vec![upsert(1, 3, 3)]);
    let mut resumed = source
        .acquire(Selector::AtMost(10), Some(initial.through))
        .unwrap();
    source.publish(vec![upsert(1, 4, 4)]);
    let first = page(&mut resumed, None, one()).unwrap();
    assert_eq!(
        first.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, 2)]
        }]
    );
    assert!(!first.bootstrap_complete);
    assert_eq!(page(&mut resumed, None, one()).unwrap(), first);
    let second = page(&mut resumed, Some(first.through), one()).unwrap();
    assert!(second.bootstrap_complete);
    let third = page(&mut resumed, Some(second.through), one()).unwrap();
    assert!(third.bootstrap_complete);
}

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
fn incomplete_bootstrap_resume_is_safely_refused() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let incomplete = page(&mut lease, None, one()).unwrap();
    assert!(!incomplete.bootstrap_complete);
    drop(lease);
    assert!(matches!(
        source.acquire(Selector::All, Some(incomplete.through)),
        Err(AcquireError::ResetRequired)
    ));
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

#[test]
fn bootstrap_retries_report_completion_at_the_returned_boundary() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 3)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let first = page(&mut lease, None, one()).unwrap();
    assert!(!first.bootstrap_complete);
    let complete = page(&mut lease, None, ReadLimits::default()).unwrap();
    assert!(complete.bootstrap_complete);
    assert_eq!(page(&mut lease, None, one()).unwrap(), first);
    assert!(
        !lease
            .read(None, one())
            .complete_timeout()
            .unwrap()
            .bootstrap_complete
    );
    let second = page(&mut lease, Some(first.through.clone()), one()).unwrap();
    assert_eq!(
        second.transactions,
        vec![Transaction {
            changes: vec![upsert(2, 1, 2)]
        }]
    );
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
    let acknowledged = lease
        .read(Some(remaining.through.clone()), one())
        .complete_timeout()
        .unwrap();
    assert_eq!(
        acknowledged,
        Page {
            transactions: Vec::new(),
            through: remaining.through.clone(),
            bootstrap_complete: true,
        }
    );
    source.publish(vec![upsert(1, 2, 4)]);
    let live = page(&mut lease, Some(remaining.through.clone()), one()).unwrap();
    assert!(live.bootstrap_complete);
    assert_eq!(
        page(&mut lease, Some(remaining.through), one()).unwrap(),
        live
    );
    assert_eq!(
        page(&mut lease, Some(second.through), one()),
        Err(ReadError::ResetRequired)
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
