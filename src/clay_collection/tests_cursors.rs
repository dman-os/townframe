use super::*;

#[test]
fn fabricated_in_range_own_scope_boundaries_are_not_acknowledgments() {
    for timeout in [false, true] {
        let (source, _) = projection(8);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let issued = read(&mut lease, None, one()).unwrap().through;
        for key in 0..3 {
            source.publish(vec![upsert(key, u64::from(key) + 1, Group::X)]);
        }
        let mut fabricated = issued.clone();
        *fabricated.0.last_mut().unwrap() = 1;
        assert_ne!(fabricated, issued);
        let result = if timeout {
            lease.read(Some(fabricated), one()).complete_timeout()
        } else {
            read(&mut lease, Some(fabricated), one())
        };
        assert_eq!(result, Err(ReadError::ResetRequired));
        assert_eq!(
            read(&mut lease, Some(issued), one()),
            Err(ReadError::ResetRequired)
        );
    }
}

#[test]
fn none_cannot_restart_a_lease_after_an_issued_cursor_was_acknowledged() {
    for timeout in [false, true] {
        let (source, _) = projection(8);
        source.publish(vec![upsert(0, 1, Group::X), upsert(1, 2, Group::X)]);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let first = read(&mut lease, None, one()).unwrap();
        assert_eq!(read(&mut lease, None, one()).unwrap(), first);
        let acknowledged = lease
            .read(Some(first.through), one())
            .complete_timeout()
            .unwrap();
        assert!(!acknowledged.bootstrap_complete);
        let result = if timeout {
            lease.read(None, one()).complete_timeout()
        } else {
            read(&mut lease, None, one())
        };
        assert_eq!(result, Err(ReadError::ResetRequired));
    }
}

#[test]
fn bootstrap_completion_describes_each_retry_until_completion_is_acknowledged() {
    let (source, _) = projection(8);
    source.publish(vec![
        upsert(0, 1, Group::X),
        upsert(1, 2, Group::X),
        upsert(2, 3, Group::X),
    ]);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let first = read(&mut lease, None, one()).unwrap();
    assert!(!first.bootstrap_complete);
    let committed = Some(first.through);
    let partial = read(&mut lease, committed.clone(), one()).unwrap();
    assert!(!partial.bootstrap_complete);
    let completed = read(&mut lease, committed.clone(), ReadLimits::default()).unwrap();
    assert!(completed.bootstrap_complete);
    assert_eq!(read(&mut lease, committed.clone(), one()).unwrap(), partial);
    let timeout = lease.read(committed, one()).complete_timeout().unwrap();
    assert!(timeout.transactions.is_empty());
    assert!(!timeout.bootstrap_complete);
    let acknowledged = lease
        .read(Some(completed.through.clone()), one())
        .complete_timeout()
        .unwrap();
    assert!(acknowledged.bootstrap_complete);
    assert_eq!(acknowledged.through, completed.through);
    source.publish(vec![remove(0, 4)]);
    let live = read(&mut lease, Some(acknowledged.through), one()).unwrap();
    assert!(live.bootstrap_complete);
    assert_eq!(
        live.transactions,
        vec![Transaction {
            changes: vec![remove(0, 4)]
        }]
    );
}

#[test]
fn cursor_scope_rejects_malformed_foreign_selector_source_and_generation_tokens() {
    let (source, interests) = projection(16);
    assert_eq!(
        source.acquire(Selector::Invalid, None).unwrap_err(),
        AcquireError::InvalidSelector
    );
    let mut original = source.acquire(Selector::X, None).unwrap();
    let token = read(&mut original, None, one()).unwrap().through;
    let (foreign, _) = projection(4);
    for (producer, selector, cursor) in [
        (&source, Selector::X, Cursor(vec![])),
        (&source, Selector::X, Cursor(vec![255; 7])),
        (&source, Selector::All, token.clone()),
        (&foreign, Selector::X, token.clone()),
    ] {
        assert_eq!(
            producer.acquire(selector, Some(cursor)).unwrap_err(),
            AcquireError::ResetRequired
        );
    }
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    let mut other = source.acquire(Selector::X, None).unwrap();
    let other_token = read(&mut other, None, one()).unwrap().through;
    assert_eq!(
        read(&mut original, Some(other_token), one()),
        Err(ReadError::ResetRequired)
    );
    assert_eq!(
        read(&mut original, None, one()),
        Err(ReadError::ResetRequired)
    );
    for cursor in [Cursor(vec![1]), token.clone()] {
        let mut lease = foreign.acquire(Selector::X, None).unwrap();
        read(&mut lease, None, one()).unwrap();
        assert_eq!(
            read(&mut lease, Some(cursor), one()),
            Err(ReadError::ResetRequired)
        );
    }
    source.reset();
    assert_eq!(
        source
            .acquire(Selector::X, Some(token.clone()))
            .unwrap_err(),
        AcquireError::ResetRequired
    );
    assert_eq!(read(&mut other, None, one()), Err(ReadError::ResetRequired));
    let mut new_generation = source.acquire(Selector::X, None).unwrap();
    read(&mut new_generation, None, one()).unwrap();
    assert_eq!(
        read(&mut new_generation, Some(token), one()),
        Err(ReadError::ResetRequired)
    );
}

#[test]
fn resumed_forks_are_independent_and_complete_at_their_new_opening_boundary() {
    let (source, interests) = projection(8);
    source.publish(vec![upsert(0, 1, Group::X)]);
    let mut original = source.acquire(Selector::All, None).unwrap();
    let mut baseline = Mirror::empty();
    let token = apply(&mut original, &mut baseline, one()).through;
    assert!(baseline.bootstrap_complete());
    let retained = baseline.row(&0).cloned();
    let mut alternate = source.acquire(Selector::All, None).unwrap();
    let mut b = Mirror::empty();
    let other_token = apply(&mut alternate, &mut b, one()).through;
    drop(alternate);
    source.publish(vec![upsert(1, 2, Group::X)]);
    source.publish(vec![remove(0, 3)]);
    let mut left = source.acquire(Selector::All, Some(token.clone())).unwrap();
    let mut right = source
        .acquire(Selector::All, Some(other_token.clone()))
        .unwrap();
    source.publish(vec![upsert(2, 4, Group::Y)]);
    let mut a = baseline;
    a.begin_lease(Some(&token));
    b.begin_lease(Some(&other_token));
    for mirror in [&mut a, &mut b] {
        assert_eq!(mirror.committed(), None);
        assert!(!mirror.bootstrap_complete());
        assert_eq!(mirror.row(&0), retained.as_ref());
    }
    let first = apply(&mut left, &mut a, one());
    assert_eq!(
        first.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, Group::X)]
        }]
    );
    assert!(!first.bootstrap_complete);
    assert!(!a.bootstrap_complete());
    assert_eq!(a.row(&0), retained.as_ref());
    assert_eq!(a.len(), 2);
    let second = apply(&mut left, &mut a, one());
    assert!(second.bootstrap_complete);
    assert_eq!(second.transactions.len(), 1);
    assert_eq!(second.transactions[0].changes, vec![remove(0, 3)]);
    assert!(a.row(&0).is_none());
    assert_eq!(a.row(&1).unwrap().version, Version::new(1, 2));
    assert_eq!(b.committed(), None);
    assert!(!b.bootstrap_complete());
    assert_eq!(b.row(&0), retained.as_ref());
    let whole = apply(&mut right, &mut b, ReadLimits::default());
    assert!(whole.bootstrap_complete);
    assert_eq!(whole.transactions.len(), 3);
    assert_eq!(a.len(), 1);
    assert!(apply(&mut left, &mut a, one()).bootstrap_complete);
    assert_eq!(a.rows(), b.rows());
    assert_ne!(a.committed(), b.committed());
    drop(original);
    drop(left);
    assert_eq!(interests.load(Ordering::SeqCst), 1);
    drop(right);
    assert_eq!(interests.load(Ordering::SeqCst), 0);
}

#[test]
fn retained_resume_replays_while_compacted_resume_requires_a_fresh_snapshot() {
    let (source, _) = projection(2);
    source.publish(vec![upsert(0, 1, Group::X)]);
    let mut original = source.acquire(Selector::X, None).unwrap();
    let mut mirror = Mirror::empty();
    let token = apply(&mut original, &mut mirror, one()).through;
    drop(original);
    source.publish(vec![upsert(1, 2, Group::X)]);
    let mut resumed = source.acquire(Selector::X, Some(token.clone())).unwrap();
    mirror.begin_lease(Some(&token));
    let page = apply(&mut resumed, &mut mirror, one());
    assert_eq!(
        page.transactions,
        vec![Transaction {
            changes: vec![upsert(1, 2, Group::X)]
        }]
    );
    assert!(page.bootstrap_complete);
    source.publish(vec![remove(0, 3)]);
    source.publish(vec![upsert(1, 4, Group::Y)]);
    assert_eq!(
        source.acquire(Selector::X, Some(token)).unwrap_err(),
        AcquireError::ResetRequired
    );
    mirror.clear();
    let mut fresh = source.acquire(Selector::X, None).unwrap();
    assert!(apply(&mut fresh, &mut mirror, one()).bootstrap_complete);
    assert!(mirror.is_empty());
}

#[test]
fn partial_bootstrap_resume_either_continues_without_a_gap_or_requires_reset() {
    let (source, interests) = projection(8);
    source.publish(vec![upsert(0, 1, Group::X), upsert(1, 2, Group::X)]);
    let mut original = source.acquire(Selector::X, None).unwrap();
    let mut mirror = Mirror::empty();
    let first = apply(&mut original, &mut mirror, one());
    assert!(!first.bootstrap_complete);
    drop(original);
    match source.acquire(Selector::X, Some(first.through.clone())) {
        Ok(mut resumed) => {
            mirror.begin_lease(Some(&first.through));
            let page = apply(&mut resumed, &mut mirror, ReadLimits::default());
            assert!(page.bootstrap_complete);
        }
        Err(AcquireError::ResetRequired) => {
            mirror.clear();
            let mut fresh = source.acquire(Selector::X, None).unwrap();
            assert!(apply(&mut fresh, &mut mirror, ReadLimits::default()).bootstrap_complete);
        }
        Err(error) => panic!("unexpected partial-bootstrap resume error: {error:?}"),
    }
    assert_eq!(
        mirror.rows(),
        vec![
            (
                0,
                Row {
                    group: Group::X,
                    value: 1
                }
            ),
            (
                1,
                Row {
                    group: Group::X,
                    value: 2
                }
            ),
        ]
    );
    assert_eq!(interests.load(Ordering::SeqCst), 0);
}
