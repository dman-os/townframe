use super::*;

#[test]
fn retries_use_the_committed_boundary_not_the_last_delivered_page() {
    let (source, _) = projection(8);
    let mut lease = source.acquire(Selector::All, None).unwrap();
    let mut mirror = Mirror::empty();
    let baseline = apply(&mut lease, &mut mirror, one());
    for key in 0..3 {
        source.publish(vec![upsert(key, u64::from(key) + 1, Group::X)]);
    }
    let small_ticket = mirror.begin_read();
    let small = read(&mut lease, small_ticket.committed().cloned(), one()).unwrap();
    assert!(mirror.cancel_read(&small_ticket));
    let wide_ticket = mirror.begin_read();
    let wide = read(
        &mut lease,
        wide_ticket.committed().cloned(),
        ReadLimits::default(),
    )
    .unwrap();
    assert_eq!(small.transactions.len(), 1);
    assert_eq!(wide.transactions.len(), 3);
    assert_eq!(small.transactions, wide.transactions[..1]);
    assert_ne!(small.through, wide.through);
    assert!(mirror.cancel_read(&wide_ticket));
    let retry = mirror.begin_read();
    let retried = read(&mut lease, retry.committed().cloned(), one()).unwrap();
    assert_eq!(retried, small);
    assert_eq!(retry.base(), Some(&baseline.through));
    assert!(!mirror.apply(&small_ticket, &small));
    assert!(!mirror.apply(&wide_ticket, &wide));
    assert!(mirror.apply(&retry, &retried));
    assert!(!mirror.apply(&retry, &retried));
    apply(&mut lease, &mut mirror, ReadLimits::default());
    assert_eq!(mirror.len(), 3);
    assert_eq!(mirror.committed(), Some(&wide.through));
    let pending = mirror.begin_read();
    assert!(
        poll(
            &mut lease.read(pending.committed().cloned(), one()),
            Waker::noop()
        )
        .is_pending()
    );
    assert!(mirror.cancel_read(&pending));
}

#[test]
fn late_narrow_page_cannot_resurrect_a_key_removed_by_an_accepted_wide_page() {
    for cancel in [false, true] {
        let (source, _) = projection(8);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let mut mirror = Mirror::empty();
        let baseline = apply(&mut lease, &mut mirror, one());
        source.publish(vec![upsert(0, 1, Group::X)]);
        source.publish(vec![remove(0, 2)]);
        let narrow = mirror.begin_read();
        let late = read(&mut lease, narrow.committed().cloned(), one()).unwrap();
        assert_eq!(late.transactions[0].changes, vec![upsert(0, 1, Group::X)]);
        let wide = if cancel {
            assert!(mirror.cancel_read(&narrow));
            mirror.begin_read()
        } else {
            narrow.clone()
        };
        assert_eq!(wide.base(), Some(&baseline.through));
        let accepted = read(&mut lease, wide.committed().cloned(), ReadLimits::default()).unwrap();
        assert_eq!(accepted.transactions.len(), 2);
        assert_ne!(accepted.through, late.through);
        assert!(mirror.apply(&wide, &accepted));
        assert!(mirror.is_empty());
        assert!(!mirror.apply(&narrow, &late));
        let next = mirror.begin_read();
        assert!(!mirror.apply(&narrow, &late));
        assert!(!mirror.cancel_read(&narrow));
        assert!(!mirror.apply(&wide, &accepted));
        assert!(mirror.is_empty());
        assert_eq!(mirror.cursor(), Some(&accepted.through));
        assert!(mirror.cancel_read(&next));
    }
}

#[test]
fn cancelled_empty_timeouts_stay_invalid_when_the_logical_base_does_not_change() {
    for bootstrapped in [false, true] {
        let (source, _) = projection(8);
        let mut lease = source.acquire(Selector::All, None).unwrap();
        let mut mirror = Mirror::empty();
        if bootstrapped {
            apply(&mut lease, &mut mirror, one());
        }
        let old = mirror.begin_read();
        let late = lease
            .read(old.committed().cloned(), one())
            .complete_timeout()
            .unwrap();
        assert!(late.transactions.is_empty());
        assert!(mirror.cancel_read(&old));
        let current = mirror.begin_read();
        assert_eq!(old.base(), current.base());
        assert_eq!(old.committed(), current.committed());
        assert!(!mirror.apply(&old, &late));
        assert_eq!(mirror.bootstrap_complete(), bootstrapped);
        let timeout = lease
            .read(current.committed().cloned(), one())
            .complete_timeout()
            .unwrap();
        assert_eq!(timeout, late);
        assert!(mirror.apply(&current, &timeout));
        let next = mirror.begin_read();
        assert!(!mirror.apply(&old, &late));
        assert!(!mirror.apply(&current, &timeout));
        assert_eq!(mirror.cursor(), Some(&timeout.through));
        assert!(mirror.cancel_read(&next));
    }
}

#[test]
fn replacement_lease_rejects_old_responses_and_retains_only_the_matching_resume_state() {
    for resume in [false, true] {
        let (source, _) = projection(8);
        source.publish(vec![upsert(0, 1, Group::X)]);
        let mut original = source.acquire(Selector::All, None).unwrap();
        let mut mirror = Mirror::empty();
        let baseline = apply(&mut original, &mut mirror, one());
        let retained = mirror.row(&0).unwrap().clone();
        source.publish(vec![upsert(1, 2, Group::X)]);
        let old = mirror.begin_read();
        let late = read(&mut original, old.committed().cloned(), one()).unwrap();
        let cursor = resume.then_some(baseline.through);
        let mut replacement = source.acquire(Selector::All, cursor.clone()).unwrap();
        mirror.begin_lease(cursor.as_ref());
        assert_eq!(mirror.committed(), None);
        assert_eq!(mirror.cursor(), cursor.as_ref());
        assert!(!mirror.bootstrap_complete());
        assert_eq!(mirror.row(&0), resume.then_some(&retained));
        let current = mirror.begin_read();
        assert_eq!(current.base(), cursor.as_ref());
        assert_eq!(current.committed(), None);
        assert!(!mirror.apply(&old, &late));
        assert!(!mirror.cancel_read(&old));
        let page = read(
            &mut replacement,
            current.committed().cloned(),
            ReadLimits::default(),
        )
        .unwrap();
        assert!(mirror.apply(&current, &page));
        assert!(!mirror.apply(&old, &late));
        assert_eq!(mirror.row(&0), Some(&retained));
        assert_eq!(mirror.row(&1).unwrap().version, Version::new(1, 2));
        assert!(mirror.bootstrap_complete());
        assert_eq!(mirror.committed(), Some(&page.through));
        assert_ne!(mirror.cursor(), Some(&late.through));
    }
}
