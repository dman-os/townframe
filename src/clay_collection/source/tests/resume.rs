use super::*;

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
fn resumed_retries_and_timeouts_report_their_own_opening_boundary() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    drop(lease);
    source.publish(vec![upsert(1, 2, 2)]);
    source.publish(vec![upsert(1, 3, 3)]);
    let mut resumed = source
        .acquire(Selector::All, Some(initial.through))
        .unwrap();
    let first = page(&mut resumed, None, one()).unwrap();
    assert!(!first.bootstrap_complete);
    let complete = page(&mut resumed, None, ReadLimits::default()).unwrap();
    assert!(complete.bootstrap_complete);
    assert_eq!(page(&mut resumed, None, one()).unwrap(), first);
    let timeout = resumed.read(None, one()).complete_timeout().unwrap();
    assert!(timeout.transactions.is_empty());
    assert!(!timeout.bootstrap_complete);
    assert_ne!(timeout.through, first.through);
    assert_eq!(
        resumed
            .read(Some(first.through.clone()), one())
            .complete_timeout()
            .unwrap(),
        Page {
            transactions: Vec::new(),
            through: first.through.clone(),
            bootstrap_complete: false,
        }
    );
    assert_eq!(
        page(&mut resumed, Some(first.through.clone()), one()).unwrap(),
        Page {
            transactions: vec![Transaction {
                changes: vec![upsert(1, 3, 3)],
            }],
            through: complete.through.clone(),
            bootstrap_complete: true,
        }
    );
    assert_eq!(
        resumed
            .read(Some(complete.through.clone()), one())
            .complete_timeout()
            .unwrap(),
        Page {
            transactions: Vec::new(),
            through: complete.through.clone(),
            bootstrap_complete: true,
        }
    );
    source.publish(vec![upsert(1, 4, 4)]);
    assert!(
        page(&mut resumed, Some(complete.through), one())
            .unwrap()
            .bootstrap_complete
    );
    assert_eq!(
        page(&mut resumed, Some(first.through), one()),
        Err(ReadError::ResetRequired)
    );
}

#[test]
fn resume_requires_an_issued_complete_snapshot_boundary() {
    let source = source(8);
    source.publish(vec![upsert(1, 1, 1), upsert(2, 1, 2), upsert(3, 1, 3)]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let partial = page(&mut lease, None, one()).unwrap();
    let mut forged = partial.through.clone();
    forged.0[40..48].copy_from_slice(&3_u64.to_be_bytes());
    assert!(matches!(
        source.acquire(Selector::All, Some(forged)),
        Err(AcquireError::ResetRequired)
    ));
    let complete = page(&mut lease, None, ReadLimits::default()).unwrap();
    let mut resumed = source
        .acquire(Selector::All, Some(complete.through))
        .unwrap();
    let initial = page(&mut resumed, None, one()).unwrap();
    assert!(initial.transactions.is_empty());
    assert!(initial.bootstrap_complete);
    assert_eq!(page(&mut resumed, None, one()).unwrap(), initial);
    assert!(matches!(
        source.acquire(Selector::All, Some(partial.through)),
        Err(AcquireError::ResetRequired)
    ));
}

#[test]
fn resume_rejects_unissued_live_boundaries_even_inside_a_delivered_page() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    for sequence in 1..=3 {
        source.publish(vec![upsert(1, sequence, sequence as i32)]);
    }
    for sequence in 1_u64..=3 {
        let mut forged = initial.through.clone();
        forged.0[48..56].copy_from_slice(&sequence.to_be_bytes());
        assert!(matches!(
            source.acquire(Selector::All, Some(forged)),
            Err(AcquireError::ResetRequired)
        ));
    }
    let complete = page(&mut lease, Some(initial.through), ReadLimits::default()).unwrap();
    for sequence in 1_u64..=2 {
        let mut forged = complete.through.clone();
        forged.0[48..56].copy_from_slice(&sequence.to_be_bytes());
        assert!(matches!(
            source.acquire(Selector::All, Some(forged)),
            Err(AcquireError::ResetRequired)
        ));
    }
    drop(lease);
    let mut resumed = source
        .acquire(Selector::All, Some(complete.through))
        .unwrap();
    assert!(
        page(&mut resumed, None, one())
            .unwrap()
            .transactions
            .is_empty()
    );
}

#[test]
fn resume_cannot_transfer_issuance_to_an_unread_scope() {
    let source = source(8);
    let mut first = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut first, None, one()).unwrap();
    let unread = source.acquire(Selector::All, None).unwrap();
    let mut forged = initial.through.clone();
    let scope = u64::from_be_bytes(forged.0[32..40].try_into().unwrap());
    forged.0[32..40].copy_from_slice(&(scope + 1).to_be_bytes());
    assert!(matches!(
        source.acquire(Selector::All, Some(forged)),
        Err(AcquireError::ResetRequired)
    ));
    drop(unread);
    assert!(source.acquire(Selector::All, Some(initial.through)).is_ok());
}

#[test]
fn resume_keeps_genuine_issued_boundaries_after_acknowledgement_and_lease_drop() {
    let source = source(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    source.publish(vec![upsert(1, 1, 1)]);
    source.publish(vec![upsert(1, 2, 2)]);
    let first = page(&mut lease, Some(initial.through.clone()), one()).unwrap();
    let second = page(&mut lease, Some(first.through.clone()), one()).unwrap();
    assert!(
        lease
            .read(Some(second.through.clone()), one())
            .complete_timeout()
            .unwrap()
            .bootstrap_complete
    );
    drop(lease);
    for (cursor, start) in [
        (initial.through, 1),
        (first.through, 2),
        (second.through, 3),
    ] {
        let mut resumed = source.acquire(Selector::All, Some(cursor)).unwrap();
        let result = page(&mut resumed, None, ReadLimits::default()).unwrap();
        assert!(result.bootstrap_complete);
        assert_eq!(
            result.transactions,
            (start..=2)
                .map(|sequence| Transaction {
                    changes: vec![upsert(1, sequence, sequence as i32)],
                })
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn resume_issuance_expires_with_compaction_without_invalidating_retained_tokens() {
    let source = source(2);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let initial = page(&mut lease, None, one()).unwrap();
    source.publish(vec![upsert(1, 1, 1)]);
    let first = page(&mut lease, Some(initial.through.clone()), one()).unwrap();
    source.publish(vec![upsert(1, 2, 2)]);
    let second = page(&mut lease, Some(first.through.clone()), one()).unwrap();
    source.publish(vec![upsert(1, 3, 3)]);
    assert!(matches!(
        source.acquire(Selector::All, Some(initial.through)),
        Err(AcquireError::ResetRequired)
    ));
    drop(lease);
    for cursor in [first.through, second.through] {
        let mut resumed = source.acquire(Selector::All, Some(cursor)).unwrap();
        let result = page(&mut resumed, None, ReadLimits::default()).unwrap();
        assert!(result.bootstrap_complete);
        assert_eq!(
            result.transactions.last().unwrap().changes,
            vec![upsert(1, 3, 3)]
        );
    }
}
