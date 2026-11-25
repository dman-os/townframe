//! The machine's laws (4 to 13, 15), one named test each at least.

use super::*;

fn rig_with(config: Config, peers: &[u8]) -> Rig {
    let mut rig = Rig::new(config);
    for peer in peers {
        rig.outbox.peer_connected(super::peer(*peer));
    }
    rig
}

fn one_at_a_time() -> Config {
    Config {
        max_fetches: 1,
        max_in_flight: 4,
        ..quick()
    }
}

// ── law 4: the index ────────────────────────────────────────────────────────────────────────

/// A restart reads every live row and every want is due at once.
#[test]
fn law_4_a_restart_finds_every_live_want_due() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[
        (1, Some(want(1, 0, 0, &[1]))),
        (2, Some(want(2, 0, 0, &[1]))),
        (3, Some(want(3, 0, 0, &[1]))),
    ]);
    assert_eq!(fetches(&rig.flush()).len(), 3);
}

#[test]
fn law_4_a_tombstone_for_an_unknown_want_is_a_no_op() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, None)]);
    assert_eq!(rig.flush(), vec![]);
    assert!(rig.outbox.is_empty());
}

/// The index holds every want, not the `M` that are scheduled: a high-priority want read after
/// many low ones is started first.
#[test]
fn law_4_a_late_high_priority_want_is_not_stuck_behind_the_budget() {
    let mut rig = rig_with(
        Config {
            max_fetches: 1,
            max_in_flight: 1,
            ..quick()
        },
        &[1],
    );
    let low: Vec<_> = (1..=6)
        .map(|n| (n, Some(want(n.into(), u64::from(n), 0, &[1]))))
        .collect();
    rig.rows(&low);
    let first = fetches(&rig.flush());
    assert_eq!(first.len(), 1);
    assert_eq!(rig.outbox.len(), 6, "the index is not bounded by M");

    rig.rows(&[(9, Some(want(9, 100, 3, &[1])))]);
    rig.end(first[0].0, DURABLE);
    let cmds = rig.flush();
    assert_eq!(lands(&cmds), vec![(first[0].1, first[0].3)]);
    rig.outbox.landed(&obj(first[0].1), first[0].3, true);
    let next = fetches(&rig.flush());
    assert_eq!(next.iter().map(|f| f.1).collect::<Vec<_>>(), vec![9]);
}

// ── law 5: one fetch per object, stopped before replaced ───────────────────────────────────

#[test]
fn law_5_a_superseding_want_waits_for_the_old_fetch_to_end() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let old = fetches(&rig.flush())[0].0;

    rig.rows(&[(1, Some(want(2, 0, 0, &[1])))]);
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), vec![old]);
    assert_eq!(fetches(&cmds), vec![], "no fetch beside the old one");
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Draining));
    assert_eq!(rig.outbox.fetching(), 1, "the stopping call holds its slot");

    rig.end(old, Ended::Cancelled);
    let next = fetches(&rig.flush());
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].3, 2, "the new version");
    assert_ne!(next[0].0, old, "a new call");
}

/// A result the old call reports after the cancel is dropped: nothing lands for a version the
/// want has given up on.
#[test]
fn law_5_a_cancelled_call_that_raced_to_a_result_lands_nothing() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let old = fetches(&rig.flush())[0].0;
    rig.rows(&[(1, Some(want(2, 0, 0, &[1])))]);
    rig.flush();
    rig.end(old, DURABLE);
    let cmds = rig.flush();
    assert_eq!(lands(&cmds), vec![]);
    assert_eq!(
        fetches(&cmds).iter().map(|f| f.3).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(rig.outbox.landings(), 0);
}

/// A want withdrawn and recorded again while its fetch is stopping waits for the stop too.
#[test]
fn law_5_a_re_recorded_want_waits_for_the_stopping_call() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let old = fetches(&rig.flush())[0].0;
    rig.rows(&[(1, None)]);
    rig.rows(&[(1, Some(want(5, 0, 0, &[1])))]);
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), vec![old]);
    assert_eq!(fetches(&cmds), vec![]);
    rig.end(old, Ended::Cancelled);
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.3)
            .collect::<Vec<_>>(),
        vec![5]
    );
}

// ── law 6: an equal version leaves a running fetch alone ───────────────────────────────────

#[test]
fn law_6_an_overwrite_with_an_equal_version_touches_nothing_running() {
    let mut rig = rig_with(quick(), &[1, 2]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let call = fetches(&rig.flush())[0].0;
    // other providers, another priority: the same identity
    rig.rows(&[(1, Some(want(1, 0, 3, &[1, 2])))]);
    assert_eq!(rig.flush(), vec![]);
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Fetching(call)));
    assert_eq!(rig.outbox.fetching(), 1);
}

/// A burst of re-advertisements cannot shorten a backoff or reset its rounds.
#[test]
fn law_6_re_advertising_does_not_reset_a_backoff() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let call = fetches(&rig.flush())[0].0;
    rig.end(call, Ended::Failed);
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Backoff));
    let due = rig.outbox.next_due();
    for priority in 1..4 {
        rig.rows(&[(1, Some(want(1, 0, priority, &[1])))]);
        assert_eq!(rig.flush(), vec![]);
    }
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Backoff));
    assert_eq!(rig.outbox.next_due(), due);
    assert_eq!(rig.outbox.rounds_of(&obj(1)), Some(1));
}

// ── law 7: priority, then age, with aging ──────────────────────────────────────────────────

/// Which object starts first, with `C = 1`.
fn first_of(config: Config, wants: &[(u8, Want)]) -> u8 {
    let mut rig = rig_with(config, &[1]);
    let rows: Vec<_> = wants.iter().map(|(n, w)| (*n, Some(w.clone()))).collect();
    rig.rows(&rows);
    let started = fetches(&rig.flush());
    assert_eq!(started.len(), 1);
    started[0].1
}

#[test]
fn law_7_higher_priority_starts_first() {
    let first = first_of(
        one_at_a_time(),
        &[(1, want(1, 0, 0, &[1])), (2, want(2, 1_000, 2, &[1]))],
    );
    assert_eq!(first, 2);
}

#[test]
fn law_7_older_starts_first_among_equals() {
    let first = first_of(
        one_at_a_time(),
        &[(1, want(1, 5, 1, &[1])), (2, want(2, 3, 1, &[1]))],
    );
    assert_eq!(first, 2);
}

#[test]
fn law_7_ties_go_to_the_smaller_key() {
    let first = first_of(
        one_at_a_time(),
        &[(7, want(1, 5, 1, &[1])), (3, want(2, 5, 1, &[1]))],
    );
    assert_eq!(first, 3);
}

/// A want that has waited `aging` longer than another overtakes one level of priority.
#[test]
fn law_7_an_old_low_priority_want_overtakes() {
    // one level is worth 60s
    let waited_less = first_of(
        one_at_a_time(),
        &[(1, want(1, 0, 0, &[1])), (2, want(2, 59_000, 1, &[1]))],
    );
    assert_eq!(waited_less, 2, "59s younger loses to one level of priority");
    let waited_more = first_of(
        one_at_a_time(),
        &[(1, want(1, 0, 0, &[1])), (2, want(2, 61_000, 1, &[1]))],
    );
    assert_eq!(waited_more, 1, "61s younger does not");
}

#[test]
fn law_7_the_maximum_aging_is_strict_priority() {
    let strict = Config {
        aging: Duration::MAX,
        ..one_at_a_time()
    };
    let first = first_of(
        strict,
        &[
            (1, want(1, 0, 0, &[1])),
            (2, want(2, u64::MAX / 4, 1, &[1])),
        ],
    );
    assert_eq!(first, 2);
}

// ── law 8: bounded work ───────────────────────────────────────────────────────────────────────

#[test]
fn law_8_at_most_c_fetches_run() {
    let mut rig = rig_with(
        Config {
            max_fetches: 2,
            ..quick()
        },
        &[1],
    );
    let rows: Vec<_> = (1..=5)
        .map(|n| (n, Some(want(n.into(), 0, 0, &[1]))))
        .collect();
    rig.rows(&rows);
    let started = fetches(&rig.flush());
    assert_eq!(started.len(), 2);
    rig.end(started[0].0, Ended::Failed);
    assert_eq!(
        fetches(&rig.flush()).len(),
        1,
        "a freed slot starts one more"
    );
}

/// A fetch holds its slot until its terminal event, a cancelled one included.
#[test]
fn law_8_a_cancelled_fetch_holds_its_slot_until_it_reports() {
    let mut rig = rig_with(one_at_a_time(), &[1]);
    rig.rows(&[
        (1, Some(want(1, 0, 0, &[1]))),
        (2, Some(want(2, 9, 0, &[1]))),
    ]);
    let old = fetches(&rig.flush())[0].0;
    rig.rows(&[(1, None)]);
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), vec![old]);
    assert_eq!(fetches(&cmds), vec![], "the other want waits for the slot");
    rig.end(old, Ended::Cancelled);
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.1)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

/// Landings count against `M`: a store too slow to land holds new fetches back.
#[test]
fn law_8_landings_count_against_the_in_flight_bound() {
    let mut rig = rig_with(
        Config {
            max_fetches: 2,
            max_in_flight: 2,
            ..quick()
        },
        &[1],
    );
    let rows: Vec<_> = (1..=4)
        .map(|n| (n, Some(want(n.into(), 0, 0, &[1]))))
        .collect();
    rig.rows(&rows);
    let started = fetches(&rig.flush());
    assert_eq!(started.len(), 2);
    rig.end(started[0].0, DURABLE);
    let cmds = rig.flush();
    assert_eq!(fetches(&cmds), vec![], "one fetch and one landing fill M");
    assert_eq!(lands(&cmds).len(), 1);
    rig.outbox.landed(&obj(started[0].1), started[0].3, true);
    assert_eq!(fetches(&rig.flush()).len(), 1);
}

// ── law 9: providers ────────────────────────────────────────────────────────────────────────

#[test]
fn law_9_a_want_with_no_connected_provider_parks_until_one_connects() {
    let mut rig = rig_with(quick(), &[]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1, 2])))]);
    assert_eq!(rig.flush(), vec![]);
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Parked));
    assert_eq!(rig.outbox.fetching(), 0);

    rig.outbox.peer_connected(peer(3));
    assert_eq!(rig.flush(), vec![], "a stranger is not a provider");
    rig.outbox.peer_connected(peer(2));
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.2)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

/// The connection that came first is not lost: eligibility reads the connections when the
/// want is considered.
#[test]
fn law_9_a_connection_before_the_want_is_seen() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    assert_eq!(fetches(&rig.flush()).len(), 1);
}

#[test]
fn law_9_a_parked_want_holds_no_slot() {
    let mut rig = rig_with(one_at_a_time(), &[2]);
    rig.rows(&[
        (1, Some(want(1, 0, 3, &[1]))),
        (2, Some(want(2, 9, 0, &[2]))),
    ]);
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.1)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn law_9_a_disconnect_cancels_the_fetches_against_it_without_a_failure() {
    let mut rig = rig_with(quick(), &[1, 2]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1, 2])))]);
    let first = fetches(&rig.flush());
    assert_eq!((first.len(), first[0].2), (1, 1), "the smallest provider");

    rig.outbox.peer_disconnected(&peer(1));
    let cmds = rig.flush();
    assert_eq!(cancels(&cmds), vec![first[0].0]);
    rig.end(first[0].0, Ended::Cancelled);
    let next = fetches(&rig.flush());
    assert_eq!(next.iter().map(|f| f.2).collect::<Vec<_>>(), vec![2]);
    assert_eq!(rig.outbox.rounds_of(&obj(1)), Some(0), "not a failure");
}

#[test]
fn law_9_a_disconnect_of_another_provider_leaves_the_fetch_alone() {
    let mut rig = rig_with(quick(), &[1, 2]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1, 2])))]);
    rig.flush();
    rig.outbox.peer_disconnected(&peer(2));
    assert_eq!(rig.flush(), vec![]);
}

// ── law 10: failure ─────────────────────────────────────────────────────────────────────────

#[test]
fn law_10_a_failure_tries_the_next_provider_at_once() {
    let mut rig = rig_with(quick(), &[1, 2]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1, 2])))]);
    let call = fetches(&rig.flush())[0].0;
    rig.end(call, Ended::Failed);
    let next = fetches(&rig.flush());
    assert_eq!(next.iter().map(|f| f.2).collect::<Vec<_>>(), vec![2]);
    assert_eq!(rig.outbox.next_due(), None, "no backoff yet");
}

#[test]
fn law_10_no_provider_left_backs_off_doubling_to_the_cap() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let mut waited = vec![];
    let mut call = fetches(&rig.flush())[0].0;
    for _ in 0..6 {
        rig.end(call, Ended::Failed);
        assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Backoff));
        let due = rig.outbox.next_due().expect("armed");
        waited.push(due.duration_since(rig.now).as_secs());
        rig.now = due - Duration::from_millis(1);
        assert_eq!(rig.flush(), vec![], "not due yet");
        rig.now = due;
        let again = fetches(&rig.flush());
        assert_eq!(again.len(), 1, "every provider is tried again");
        call = again[0].0;
    }
    assert_eq!(waited, vec![2, 4, 8, 16, 16, 16]);
}

#[test]
fn law_10_a_backoff_holds_no_slot() {
    let mut rig = rig_with(one_at_a_time(), &[1]);
    rig.rows(&[
        (1, Some(want(1, 0, 0, &[1]))),
        (2, Some(want(2, 9, 0, &[1]))),
    ]);
    let call = fetches(&rig.flush())[0].0;
    rig.end(call, Ended::Failed);
    let next = fetches(&rig.flush());
    assert_eq!(next.iter().map(|f| f.1).collect::<Vec<_>>(), vec![2]);
}

#[test]
fn law_10_a_new_version_resets_the_backoff() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let call = fetches(&rig.flush())[0].0;
    rig.end(call, Ended::Failed);
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Backoff));
    rig.rows(&[(1, Some(want(2, 0, 0, &[1])))]);
    assert_eq!(rig.outbox.next_due(), None);
    assert_eq!(rig.outbox.rounds_of(&obj(1)), Some(0));
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.3)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

// ── law 11: withdrawal cancels ──────────────────────────────────────────────────────────────

#[test]
fn law_11_a_tombstone_cancels_the_fetch_and_the_call_reports_before_its_slot_frees() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let call = fetches(&rig.flush())[0].0;
    rig.rows(&[(1, None)]);
    assert_eq!(cancels(&rig.flush()), vec![call]);
    assert!(rig.outbox.is_empty());
    assert_eq!(rig.outbox.fetching(), 1);
    rig.end(call, Ended::Cancelled);
    assert_eq!(rig.outbox.fetching(), 0);
}

#[test]
fn law_11_a_tombstone_for_a_waiting_want_cancels_nothing() {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let call = fetches(&rig.flush())[0].0;
    rig.end(call, Ended::Failed);
    assert!(rig.outbox.next_due().is_some());
    rig.rows(&[(1, None)]);
    assert_eq!(rig.flush(), vec![]);
    assert_eq!(rig.outbox.next_due(), None, "its backoff went with it");
}

// ── law 12: landing ─────────────────────────────────────────────────────────────────────────

fn landing_rig() -> (Rig, CallId) {
    let mut rig = rig_with(quick(), &[1]);
    rig.rows(&[(1, Some(want(1, 0, 0, &[1])))]);
    let call = fetches(&rig.flush())[0].0;
    (rig, call)
}

#[test]
fn law_12_a_landing_that_deleted_forgets_the_want() {
    let (mut rig, call) = landing_rig();
    rig.end(call, DURABLE);
    assert_eq!(lands(&rig.flush()), vec![(1, 1)]);
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Landing));
    rig.outbox.landed(&obj(1), 1, true);
    assert!(rig.outbox.is_empty());
    rig.rows(&[(1, None)]);
    assert_eq!(rig.flush(), vec![], "its tombstone is a no-op");
}

#[test]
fn law_12_a_landing_in_the_same_transaction_needs_no_land_command() {
    let (mut rig, call) = landing_rig();
    rig.end(call, Ended::Fetched(Fetched::Landed { deleted: true }));
    assert_eq!(rig.flush(), vec![]);
    assert!(rig.outbox.is_empty());
    assert_eq!(rig.outbox.landings(), 0);
}

/// A landing that found another version leaves the want stale: the same version is not
/// fetched again, and the stream's next row ends it.
#[test]
fn law_12_a_landing_that_did_not_delete_waits_for_the_stream() {
    let (mut rig, call) = landing_rig();
    rig.end(call, DURABLE);
    rig.flush();
    rig.outbox.landed(&obj(1), 1, false);
    assert_eq!(rig.outbox.phase_of(&obj(1)), Some(Phase::Stale));
    assert_eq!(rig.flush(), vec![], "nothing refetches the fetched version");
    // a providers-only overwrite does not end it either
    rig.rows(&[(1, Some(want(1, 0, 1, &[1])))]);
    assert_eq!(rig.flush(), vec![]);
    // the newer version does
    rig.rows(&[(1, Some(want(2, 0, 0, &[1])))]);
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.3)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[test]
fn law_12_the_tombstone_ends_a_stale_want() {
    let (mut rig, call) = landing_rig();
    rig.end(call, DURABLE);
    rig.flush();
    rig.outbox.landed(&obj(1), 1, false);
    rig.rows(&[(1, None)]);
    assert!(rig.outbox.is_empty());
}

/// A newer version the stream already delivered while the old one landed is fetched next.
#[test]
fn law_12_a_newer_version_seen_during_the_landing_is_fetched_after_it() {
    let (mut rig, call) = landing_rig();
    rig.end(call, DURABLE);
    rig.flush();
    rig.rows(&[(1, Some(want(2, 0, 0, &[1])))]);
    assert_eq!(
        rig.flush(),
        vec![],
        "the landing is not cancelled, nor is v2 started yet"
    );
    rig.outbox.landed(&obj(1), 1, false);
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.3)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

/// A deletion of the old version never forgets a newer want.
#[test]
fn law_12_landing_the_old_version_keeps_the_newer_want() {
    let (mut rig, call) = landing_rig();
    rig.end(call, DURABLE);
    rig.flush();
    rig.rows(&[(1, Some(want(2, 0, 0, &[1])))]);
    rig.outbox.landed(&obj(1), 1, true);
    assert_eq!(rig.outbox.version_of(&obj(1)), Some(2));
    assert_eq!(
        fetches(&rig.flush())
            .iter()
            .map(|f| f.3)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

// ── law 13: landing is never cancelled ──────────────────────────────────────────────────────

#[test]
fn law_13_a_withdrawal_during_a_landing_cancels_nothing() {
    let (mut rig, call) = landing_rig();
    rig.end(call, DURABLE);
    rig.flush();
    rig.rows(&[(1, None)]);
    assert_eq!(rig.flush(), vec![], "no cancel for a landing");
    assert_eq!(rig.outbox.landings(), 1, "it still counts until it ends");
    rig.outbox.landed(&obj(1), 1, false);
    assert_eq!(rig.outbox.landings(), 0);
    assert!(rig.outbox.is_empty());
}

// ── law 15: bugs crash ──────────────────────────────────────────────────────────────────────

#[test]
#[should_panic(expected = "terminal event for unknown call")]
fn law_15_an_event_for_an_unknown_call_crashes() {
    let mut rig = rig_with(quick(), &[1]);
    rig.end(7, Ended::Failed);
}

#[test]
#[should_panic(expected = "was never cancelled")]
fn law_15_a_cancelled_report_for_a_live_call_crashes() {
    let (mut rig, call) = landing_rig();
    rig.end(call, Ended::Cancelled);
}

#[test]
#[should_panic(expected = "landed answers a landing never issued")]
fn law_15_a_landing_never_issued_crashes() {
    let mut rig = rig_with(quick(), &[1]);
    rig.outbox.landed(&obj(1), 1, true);
}
