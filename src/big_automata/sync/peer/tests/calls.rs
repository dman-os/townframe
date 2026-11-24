//! Law 2: one call-id space, every outcome matched by id.

use super::*;

use std::collections::BTreeSet;

#[test]
fn law_2_every_kind_of_call_draws_from_one_counter() {
    let mut rig = Rig::new();
    let mut ids = vec![];
    rig.sync
        .update(&[(part(1), 0), (part(2), 0)], &[], &[], &[]);
    let asked = rig.flush();
    ids.push(asked.summaries()[0].0);
    let decided = rig.decide(&asked, &[(1, walk_summary(10)), (2, cursor_summary(0))]);
    let (list, ..) = decided.lists().remove(0);
    let (session, update) = decided.sessions().remove(0);
    ids.extend([list, session]);
    rig.sync.update_replied(session, updated(&update));
    ids.push(rig.flush().only_round().0);
    let (leaf, _) = rig.list_root(list, 10, 2);
    ids.push(leaf);
    let page = leaf_page(
        None,
        &[(1, PartObjDelta::New), (2, PartObjDelta::New)],
        None,
    );
    rig.sync.leaf_replied(leaf, Reply::Answer(page));
    let records = rig.flush().records();
    assert_eq!(records.len(), 2);
    ids.extend(records.iter().map(|(call, _)| *call));
    rig.sync
        .record_ended(records[0].0, Outcome::Succeeded(Settle::Fetch));
    ids.push(rig.flush().fetches().remove(0).0);
    let unique: BTreeSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "ids repeat across kinds: {ids:?}");
    assert!(ids.iter().all(|id| *id < rig.sync.next_call));
}

#[test]
fn law_2_an_outcome_for_a_call_not_in_flight_changes_nothing() {
    let mut rig = Rig::new();
    rig.follow(1, 0);
    let before = rig.sync.calls_in_flight();
    let never = 9999;
    rig.sync.summary_replied(never, SummaryReply::Lost);
    rig.sync.update_replied(never, Reply::Lost);
    rig.sync.round_replied(never, Reply::Lost);
    rig.sync.record_ended(never, Outcome::Failed);
    rig.sync.fetch_ended(never, Outcome::Failed);
    rig.sync.list_replied(never, Reply::Lost);
    rig.sync.leaf_replied(never, Reply::Lost);
    assert!(rig.flush().is_empty());
    assert_eq!(rig.sync.calls_in_flight(), before);
}

#[test]
fn law_2_a_duplicate_outcome_changes_nothing() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let event = rig.changed(5, 1, &[part_route(1)]);
    let first = rig.answer_round(call, &page, vec![event.clone()], 5, false);
    assert_eq!(first.records().len(), 1);
    let again = rig.answer_round(call, &page, vec![event], 5, false);
    assert!(again.records().is_empty() && again.rounds().is_empty());
}

#[test]
fn law_2_an_outcome_closes_its_call() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let event = rig.changed(5, 1, &[part_route(1)]);
    let record = rig
        .answer_round(call, &page, vec![event], 5, false)
        .only_record()
        .0;
    let open = rig.sync.calls_in_flight();
    rig.sync
        .record_ended(record, Outcome::Succeeded(Settle::Done));
    assert_eq!(rig.sync.calls_in_flight(), open - 1);
    rig.sync
        .record_ended(record, Outcome::Succeeded(Settle::Done));
    assert_eq!(
        rig.sync.calls_in_flight(),
        open - 1,
        "a duplicate closes nothing more"
    );
}

#[test]
fn law_2_outcomes_from_before_a_part_was_retired_are_stale() {
    // a walk call in flight
    let mut rig = Rig::new();
    let (list, ..) = rig.walk(1, 0, 10);
    let (leaf, _) = rig.list_root(list, 10, 1);
    rig.remove_part(1);
    let page = leaf_page(None, &[(1, PartObjDelta::New)], None);
    rig.sync.leaf_replied(leaf, Reply::Answer(page));
    assert!(rig.flush().records().is_empty());
    // a summary call in flight
    let mut rig = Rig::new();
    let asked = rig.add_part(1, 0);
    rig.remove_part(1);
    let decided = rig.decide(&asked, &[(1, cursor_summary(5))]);
    assert!(decided.is_empty());
    assert_eq!(rig.sync.phase(&part(1)), None);
}

#[test]
#[should_panic(expected = "is not a round")]
fn law_2_an_outcome_of_the_wrong_kind_is_a_driver_bug() {
    let mut rig = Rig::new();
    let (call, _) = rig.add_part(1, 0).summaries().remove(0);
    rig.sync.round_replied(call, Reply::Lost);
}
