use super::*;

#[test]
fn acknowledged_snapshot_values_are_released_without_ending_the_lease() {
    let source = Projection::new([1, 2]);
    let values = [Arc::new(1), Arc::new(2)];
    source.publish(
        values
            .iter()
            .enumerate()
            .map(|(key, value)| Change::Upsert {
                key,
                row_version: Version::new(1, 1),
                value: Arc::clone(value),
                evidence: RowEvidence::Authoritative,
                write_context: None,
            })
            .collect(),
    );
    let retained = values.each_ref().map(Arc::strong_count);
    let mut lease = source.acquire((), None).unwrap();
    let first = page(&mut lease, None, one()).unwrap();
    let first_cursor = first.through.clone();
    drop(first);
    assert_eq!(Arc::strong_count(&values[0]), retained[0] + 1);
    drop(lease.read(Some(first_cursor.clone()), one()));
    assert_eq!(Arc::strong_count(&values[0]), retained[0] + 1);
    let second = page(&mut lease, Some(first_cursor.clone()), one()).unwrap();
    assert_eq!(Arc::strong_count(&values[0]), retained[0]);
    assert_eq!(page(&mut lease, Some(first_cursor), one()).unwrap(), second);
    let second_cursor = second.through.clone();
    drop(second);
    assert_eq!(Arc::strong_count(&values[1]), retained[1] + 1);
    let complete = lease
        .read(Some(second_cursor), one())
        .complete_timeout()
        .unwrap();
    assert!(complete.bootstrap_complete);
    assert_eq!(values.each_ref().map(Arc::strong_count), retained);
    assert_eq!(
        lease
            .read(Some(complete.through.clone()), one())
            .complete_timeout()
            .unwrap(),
        complete
    );
}

#[test]
fn previously_issued_snapshot_boundary_survives_acknowledging_a_shorter_prefix() {
    let source = Projection::new([1, 2]);
    let values = [1, 2, 3, 4, 5].map(Arc::new);
    let change = |key: usize| Change::Upsert {
        key,
        row_version: Version::new(1, 1),
        value: Arc::clone(&values[key]),
        evidence: RowEvidence::Authoritative,
        write_context: None,
    };
    source.publish((0..5).map(change).collect());
    let retained = values.each_ref().map(Arc::strong_count);
    let mut lease = source.acquire((), None).unwrap();
    let first = page(&mut lease, None, one()).unwrap();
    assert_eq!(
        first.transactions,
        vec![Transaction {
            changes: vec![change(0)]
        }]
    );
    assert!(!first.bootstrap_complete);
    let first_cursor = first.through;
    let fourth = page(
        &mut lease,
        None,
        ReadLimits {
            max_transactions: Some(4),
            max_changes: Some(4),
        },
    )
    .unwrap();
    assert_eq!(
        fourth.transactions,
        (0..4)
            .map(|key| Transaction {
                changes: vec![change(key)]
            })
            .collect::<Vec<_>>()
    );
    assert!(!fourth.bootstrap_complete);
    let fourth_cursor = fourth.through;
    assert_ne!(first_cursor, fourth_cursor);
    drop(first.transactions);
    drop(fourth.transactions);
    assert_eq!(
        values.each_ref().map(Arc::strong_count),
        retained.map(|count| count + 1)
    );
    let second = page(&mut lease, Some(first_cursor.clone()), one()).unwrap();
    assert_eq!(
        second.transactions,
        vec![Transaction {
            changes: vec![change(1)]
        }]
    );
    assert!(!second.bootstrap_complete);
    drop(second);
    assert_eq!(
        values.each_ref().map(Arc::strong_count),
        std::array::from_fn(|index| retained[index] + usize::from(index > 0))
    );
    let fifth = page(&mut lease, Some(fourth_cursor.clone()), one()).unwrap();
    assert_eq!(
        fifth.transactions,
        vec![Transaction {
            changes: vec![change(4)]
        }]
    );
    assert!(fifth.bootstrap_complete);
    let fifth_cursor = fifth.through;
    drop(fifth.transactions);
    for limits in [one(), ReadLimits::default()] {
        assert_eq!(
            values.each_ref().map(Arc::strong_count),
            std::array::from_fn(|index| retained[index] + usize::from(index == 4))
        );
        let retry = page(&mut lease, Some(fourth_cursor.clone()), limits).unwrap();
        assert_eq!(
            retry.transactions,
            vec![Transaction {
                changes: vec![change(4)]
            }]
        );
        assert_eq!(retry.through, fifth_cursor);
        assert!(retry.bootstrap_complete);
    }
    assert_eq!(
        values.each_ref().map(Arc::strong_count),
        std::array::from_fn(|index| retained[index] + usize::from(index == 4))
    );
    assert_eq!(
        page(&mut lease, Some(first_cursor), one()),
        Err(ReadError::ResetRequired)
    );
    assert_eq!(
        page(&mut lease, Some(fourth_cursor), one()),
        Err(ReadError::ResetRequired)
    );
}

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
fn cancelled_unpolled_reads_do_not_acknowledge_delivered_pages() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 3)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let first = page(&mut lease, None, one()).unwrap();
    drop(lease.read(Some(first.through.clone()), one()));
    assert_eq!(page(&mut lease, None, one()).unwrap(), first);
    let complete = page(&mut lease, None, ReadLimits::default()).unwrap();
    assert!(complete.bootstrap_complete);
    drop(lease.read(Some(complete.through), one()));
    assert_eq!(page(&mut lease, None, one()).unwrap(), first);
    let second = page(&mut lease, Some(first.through.clone()), one()).unwrap();
    drop(lease.read(Some(second.through.clone()), one()));
    assert_eq!(
        page(&mut lease, Some(first.through), one()).unwrap(),
        second
    );
    assert_eq!(page(&mut lease, None, one()), Err(ReadError::ResetRequired));
}
