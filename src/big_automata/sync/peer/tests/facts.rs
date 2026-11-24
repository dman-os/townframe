//! Laws 12-14 and the machine's own call lanes: bounded work, derived sync facts, disconnect.

use super::*;

use std::collections::BTreeSet;

#[test]
fn law_12_a_round_asks_for_no_more_than_the_room_the_window_has_left() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    assert_eq!(page.limit, 4, "the whole window");
    let events = [(5, 1), (6, 2), (7, 3)]
        .map(|(rev, o)| rig.changed(rev, o, &[part_route(1)]))
        .to_vec();
    let cmds = rig.answer_round(call, &page, events, 8, false);
    let records = cmds.records();
    assert_eq!(records.len(), 3);
    let (call, _, page) = cmds.only_round();
    assert_eq!(page.limit, 1, "three unsettled objects hold three of four");
    let fourth = rig.changed(9, 4, &[part_route(1)]);
    let cmds = rig.answer_round(call, &page, vec![fourth], 9, false);
    assert!(cmds.rounds().is_empty(), "no room, no round");
    let freed = rig.record_done(records[0].0);
    assert_eq!(freed.only_round().2.limit, 1, "a settlement frees one");
}

#[test]
fn law_12_backend_calls_are_bounded_by_the_fetch_cap() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let events = [(5, 1), (6, 2), (7, 3)]
        .map(|(rev, o)| rig.changed(rev, o, &[part_route(1)]))
        .to_vec();
    let records = rig.answer_round(call, &page, events, 7, false).records();
    for (call, _) in &records {
        rig.sync
            .record_ended(*call, Outcome::Succeeded(Settle::Fetch));
    }
    assert_eq!(rig.flush().fetches().len(), 2, "fetch cap 2 of 3 objects");
}

#[test]
fn law_13_synced_is_emitted_once_when_every_condition_holds() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    assert_eq!(rig.sync.synced(&part(1)), Some(false));
    let cmds = rig.answer_round(call, &page, vec![], 5, true);
    assert_eq!(cmds.edges(), [(part(1), true)]);
    assert_eq!(rig.sync.synced(&part(1)), Some(true));
    rig.wait(60);
    assert!(rig.flush().edges().is_empty(), "an edge, not a level");
}

#[test]
fn law_13_a_route_that_is_not_drained_is_not_synced() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let behind = rig.answer_round(call, &page, vec![], 5, false);
    assert!(behind.edges().is_empty());
    let (call, _, page) = behind.only_round();
    let caught_up = rig.answer_round(call, &page, vec![], 9, true);
    assert_eq!(caught_up.edges(), [(part(1), true)]);
}

#[test]
fn law_13_outstanding_object_work_holds_the_fact_back() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let event = rig.changed(5, 1, &[part_route(1)]);
    let cmds = rig.answer_round(call, &page, vec![event], 5, true);
    assert!(
        cmds.edges().is_empty(),
        "drained, but its work is unsettled"
    );
    let done = rig.record_done(cmds.only_record().0);
    assert_eq!(done.persists(), [(part(1), 5)]);
    assert_eq!(done.edges(), [(part(1), true)]);
}

#[test]
fn law_13_new_events_unsync_a_synced_part() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.synced_at(1, 5);
    let event = rig.changed(8, 2, &[part_route(1)]);
    let cmds = rig.answer_round(call, &page, vec![event], 8, true);
    assert_eq!(cmds.edges(), [(part(1), false)], "new work unsyncs it");
    let done = rig.record_done(cmds.only_record().0);
    assert_eq!(done.edges(), [(part(1), true)]);
}

#[test]
fn law_13_removing_a_synced_part_emits_unsynced() {
    let mut rig = Rig::new();
    rig.synced_at(1, 5);
    let gone = rig.remove_part(1);
    assert_eq!(gone.edges(), [(part(1), false)]);
    assert_eq!(rig.sync.synced(&part(1)), None);
}

#[test]
fn law_13_a_reset_session_unsyncs_until_the_parts_drain_again() {
    let mut rig = Rig::new();
    let (call, ..) = rig.synced_at(1, 5);
    rig.sync
        .round_replied(call, Reply::Answer(Err(SessionError::UnknownSession)));
    let reset = rig.flush();
    assert_eq!(reset.edges(), [(part(1), false)]);
    let (call, _, page) = rig.ack(&reset).only_round();
    let back = rig.answer_round(call, &page, vec![], 5, true);
    assert_eq!(back.edges(), [(part(1), true)]);
}

#[test]
fn law_13_a_refused_route_is_unsynced_and_comes_back_on_the_ladder() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.synced_at(1, 5);
    let verdicts = page
        .targets
        .iter()
        .map(|(id, _)| (*id, Verdict::Refused))
        .collect();
    let refused = SessionPage {
        events: vec![],
        verdicts,
    };
    rig.sync.round_replied(call, Reply::Answer(Ok(refused)));
    let cmds = rig.flush();
    assert_eq!(cmds.edges(), [(part(1), false)]);
    let (removal, update) = cmds.sessions().remove(0);
    assert_eq!(update.removals.len(), 1, "the server may still hold the id");
    rig.sync.update_replied(removal, updated(&update));
    assert!(rig.flush().sessions().is_empty(), "backing off");
    rig.wait(1);
    let (readd, update) = rig.flush().sessions().remove(0);
    assert_eq!(update.additions[0].1, part_route(1));
    rig.sync.update_replied(readd, updated(&update));
    let (call, _, page) = rig.flush().only_round();
    assert_eq!(
        rig.answer_round(call, &page, vec![], 5, true).edges(),
        [(part(1), true)]
    );
}

#[test]
fn a_lost_session_update_is_resent_unchanged_after_backoff() {
    let mut rig = Rig::new();
    let (call, update) = rig.add_object(5).sessions().remove(0);
    rig.sync.update_replied(call, Reply::Lost);
    assert!(rig.flush().sessions().is_empty());
    rig.wait(1);
    let (again, resent) = rig.flush().sessions().remove(0);
    assert_ne!(again, call);
    assert_eq!(resent, update);
}

#[test]
fn law_14_disconnect_cancels_every_call_in_flight_and_reports_nothing_else() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let summary = rig.add_part(2, 0).summaries()[0].0;
    let event = rig.changed(5, 1, &[part_route(1)]);
    let cmds = rig.answer_round(call, &page, vec![event], 5, false);
    let (record, _) = cmds.only_record();
    let (round, ..) = cmds.only_round();
    rig.sync
        .record_ended(record, Outcome::Succeeded(Settle::Fetch));
    let fetch = rig.flush().fetches().remove(0).0;
    let mut out = vec![];
    rig.sync.disconnect(&mut out);
    assert!(
        out.iter()
            .all(|cmd| matches!(cmd, PeerCommand::Cancel { .. })),
        "{out:?}"
    );
    let cancelled: BTreeSet<_> = Cmds(out).cancels().into_iter().collect();
    assert_eq!(cancelled, BTreeSet::from([summary, round, fetch]));
}
