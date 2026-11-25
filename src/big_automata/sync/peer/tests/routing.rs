//! Laws 1 and 4-8: routes in, events routed, settlements back, retirement.

use super::*;

#[test]
fn law_1_adding_a_wanted_route_or_removing_an_unwanted_one_is_a_no_op() {
    let mut rig = Rig::new();
    assert_eq!(rig.add_part(1, 0).summaries().len(), 1);
    // the call in flight is not restarted, and nothing is queued
    assert!(rig.add_part(1, 9).is_empty());
    rig.sync.update(&[], &[part(9)], &[], &[obj(9)]);
    assert!(rig.flush().is_empty());
}

#[test]
fn law_1_an_object_route_is_added_and_removed_incrementally() {
    let mut rig = Rig::new();
    let (call, update) = rig.add_object(5).sessions().remove(0);
    assert_eq!(update.additions.len(), 1);
    assert_eq!(update.additions[0].1, obj_route(5));
    let id = update.additions[0].0;
    assert!(rig.add_object(5).is_empty(), "wanted already");
    rig.sync.update_replied(call, updated(&update));
    rig.flush();
    rig.sync.update(&[], &[], &[], &[obj(5)]);
    let (_, update) = rig.flush().sessions().remove(0);
    assert_eq!(update.removals, [id]);
    rig.sync.update(&[], &[], &[], &[obj(5)]);
    assert!(rig.flush().sessions().is_empty(), "unwanted already");
}

#[test]
fn law_1_a_part_is_added_with_its_persisted_cursor() {
    let mut rig = Rig::new();
    let cmds = rig.add_part(1, 42);
    assert_eq!(cmds.summaries()[0].1, Map::from([(part(1), 42)]));
}

#[test]
fn law_4_a_part_route_is_wanted_exactly_while_the_part_catches_up() {
    let mut rig = Rig::new();
    let asked = rig.add_part(1, 0);
    assert!(asked.sessions().is_empty(), "no route while deciding");
    let decided = rig.decide(&asked, &[(1, cursor_summary(5))]);
    let (call, update) = decided.sessions().remove(0);
    assert_eq!(
        update
            .additions
            .iter()
            .map(|(_, route)| route.clone())
            .collect::<Vec<_>>(),
        [part_route(1)]
    );
    let id = update.additions[0].0;
    rig.sync.update_replied(call, updated(&update));
    rig.flush();
    let gone = rig.remove_part(1);
    assert_eq!(gone.sessions()[0].1.removals, [id], "retiring unwants it");
}

#[test]
fn law_4_an_unanswered_part_has_no_route_and_leaves_without_a_removal() {
    let mut rig = Rig::new();
    let asked = rig.add_part(2, 0);
    let unanswered = rig.decide(&asked, &[]);
    assert!(unanswered.sessions().is_empty());
    assert!(rig.remove_part(2).sessions().is_empty());
}

/// An object route (5) and part 1 both acknowledged and drained: the next `Live` round names
/// both.
fn live_round_over_both(rig: &mut Rig) -> (CallId, Lane, PageSession) {
    let first = rig.add_object(5);
    let (call, update) = first.sessions().remove(0);
    rig.sync.update_replied(call, updated(&update));
    let obj_round = rig.flush().only_round();
    let asked = rig.add_part(1, 0);
    let decided = rig.decide(&asked, &[(1, cursor_summary(0))]);
    let part_round = rig.ack(&decided).only_round();
    // the part drains and moves to `Live`; its lane is free, the `Live` one is busy
    rig.answer_round(part_round.0, &part_round.2, vec![], 0, true);
    rig.answer_round(obj_round.0, &obj_round.2, vec![], 0, true)
        .only_round()
}

#[test]
fn law_5_an_object_route_event_becomes_a_content_job_with_no_part() {
    let mut rig = Rig::new();
    let (call, lane, page) = live_round_over_both(&mut rig);
    assert_eq!((lane, page.targets.len()), (Lane::Live, 2));
    let event = rig.changed(5, 5, &[obj_route(5)]);
    let cmds = rig.answer_round(call, &page, vec![event], 5, true);
    let (_, job) = cmds.only_record();
    assert_eq!(
        (job.obj, job.add, job.remove),
        (obj(5), vec![], vec![]),
        "the cursor machine never saw it"
    );
    assert!(job.payload.is_some());
    assert!(cmds.persists().is_empty());
    // the object route's session cursor advanced by the round's resume; both targets are at 5
    let next = cmds.only_round().2;
    assert!(next.targets.iter().all(|(_, cursor)| *cursor == 5));
}

#[test]
fn law_5_an_event_naming_both_kinds_of_route_is_split() {
    let mut rig = Rig::new();
    let (call, _, page) = live_round_over_both(&mut rig);
    let event = rig.changed(5, 5, &[part_route(1), obj_route(5)]);
    let verdicts = page
        .targets
        .iter()
        .map(|(id, _)| {
            (
                *id,
                Verdict::Events {
                    resume: 5,
                    drained: false,
                },
            )
        })
        .collect();
    let answer = SessionPage {
        events: vec![event],
        verdicts,
    };
    rig.sync.round_replied(call, Reply::Answer(Ok(answer)));
    // retiring the part takes its want away: only the object route's content want is left
    rig.sync.update(&[], &[part(1)], &[], &[]);
    let (_, job) = rig.flush().only_record();
    assert_eq!((job.add, job.remove), (vec![], vec![]));
    assert!(
        job.payload.is_some(),
        "the content want carries the event's payload"
    );
}

/// Follow part 1 and answer its first round with one event for object 1 at revision 5.
fn first_record(rig: &mut Rig) -> (CallId, Cmds) {
    let (call, _, page) = rig.follow(1, 0).only_round();
    let event = rig.changed(5, 1, &[part_route(1)]);
    let cmds = rig.answer_round(call, &page, vec![event], 5, false);
    (cmds.only_record().0, cmds)
}

#[test]
fn law_6_cursor_machine_commands_become_wants_and_persisted_cursors() {
    let mut rig = Rig::new();
    let (record, cmds) = first_record(&mut rig);
    let (_, job) = cmds.only_record();
    assert_eq!(
        (job.obj, job.add, job.remove),
        (obj(1), vec![part(1)], vec![])
    );
    let (next_call, _, next) = cmds.only_round();
    assert_eq!(
        next.targets[0].1, 5,
        "the next round resumes where the page stopped"
    );
    assert!(
        cmds.persists().is_empty(),
        "nothing persists before the work settles"
    );
    assert_eq!(rig.record_done(record).persists(), [(part(1), 5)]);

    // a removal becomes an absent want
    let removal = rig.removed(7, 1, &part_route(1));
    let cmds = rig.answer_round(next_call, &next, vec![removal], 7, false);
    let (record, job) = cmds.only_record();
    assert_eq!((job.add, job.remove), (vec![], vec![part(1)]));
    assert_eq!(rig.record_done(record).persists(), [(part(1), 7)]);
}

#[test]
fn law_7_a_cursor_settlement_flows_back_once() {
    let mut rig = Rig::new();
    let (record, _) = first_record(&mut rig);
    assert_eq!(rig.record_done(record).persists().len(), 1);
    rig.sync
        .record_ended(record, Outcome::Succeeded(Settle::Done));
    assert!(
        rig.flush().persists().is_empty(),
        "a duplicate reports nothing"
    );
}

#[test]
fn law_8_retiring_a_part_retires_it_everywhere_in_one_step() {
    let mut rig = Rig::new();
    let (record, cmds) = first_record(&mut rig);
    let (round, _, page) = cmds.only_round();
    assert!(rig.sync.jobs.part_outstanding(&part(1)));
    let gone = rig.remove_part(1);
    assert_eq!(gone.sessions().len(), 1, "its route is unwanted");
    assert_eq!(rig.sync.phase(&part(1)), None);
    assert!(!rig.sync.jobs.part_outstanding(&part(1)), "jobs let go");
    assert!(
        rig.sync.cursor.is_settled(&part(1)),
        "the cursor machine owes nothing"
    );
    assert!(!rig.sync.client.is_acked(&part_route(1)));
    // what was in flight for it is stale: nothing applies, nothing persists
    assert!(rig.record_done(record).persists().is_empty());
    let late = rig.changed_for(&page, 9, 1);
    let cmds = rig.answer_round(round, &page, vec![late], 9, true);
    assert!(cmds.records().is_empty() && cmds.persists().is_empty());
}

impl Rig {
    /// An event for object `o` naming the first target of `page`, whether or not its route
    /// is still held.
    fn changed_for(&self, page: &PageSession, rev: Rev, o: u8) -> PageEvent {
        PageEvent::Changed {
            rev,
            obj_key: obj(o),
            targets: vec![page.targets[0].0],
            payload: serde_json::json!(rev),
        }
    }
}

#[test]
fn law_8_a_walk_is_retired_with_its_part() {
    let mut rig = Rig::new();
    let (list, ..) = rig.walk(1, 0, 10);
    assert!(rig.sync.walking(&part(1)));
    rig.remove_part(1);
    assert!(!rig.sync.walking(&part(1)));
    rig.sync.list_replied(list, Reply::Answer(vec![]));
    assert!(rig.flush().is_empty(), "the walk's call is stale");
}

#[test]
fn law_8_removing_an_object_route_unwants_it_and_drops_its_content() {
    let mut rig = Rig::new();
    let first = rig.add_object(5);
    let (call, update) = first.sessions().remove(0);
    rig.sync.update_replied(call, updated(&update));
    let (round, _, page) = rig.flush().only_round();
    let event = rig.changed(5, 5, &[obj_route(5)]);
    let (record, _) = rig
        .answer_round(round, &page, vec![event], 5, true)
        .only_record();
    rig.sync
        .record_ended(record, Outcome::Succeeded(Settle::Fetch));
    let fetch = rig.flush().fetches().remove(0).0;
    rig.sync.update(&[], &[], &[], &[obj(5)]);
    let gone = rig.flush();
    assert_eq!(gone.sessions().len(), 1, "the route is unwanted");
    assert_eq!(gone.cancels(), [fetch], "nothing keeps its content wanted");
    assert!(rig.sync.objects.is_empty());
}
