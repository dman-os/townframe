use super::*;
use crate::ByteKey;

fn obj(seed: u8) -> ObjKey {
    ObjKey(ByteKey::new(&[seed; 32]))
}

fn part(seed: u8) -> PartKey {
    PartKey(ByteKey::new(&[seed; 32]))
}

/// A membership touch: the object is present in `parts` as of `cursor`. There is no
/// separate "added" kind, so a first membership and a changed one are the same event.
fn touched(cursor: Rev, obj_key: &ObjKey, parts: &[PartKey]) -> CursorEvent {
    CursorEvent::ObjChanged(ObjChanged {
        cursor,
        parts: parts.to_vec(),
        obj_key: obj_key.clone(),
        payload: serde_json::json!({ "k": cursor }),
    })
}

fn removed(cursor: Rev, obj_key: &ObjKey, part_key: &PartKey) -> CursorEvent {
    CursorEvent::ObjRemovedFromPart(ObjRemovedFromPart {
        cursor,
        part_key: part_key.clone(),
        obj_key: obj_key.clone(),
    })
}

fn sync_obj(obj_key: &ObjKey, cursor: Rev, parts: Vec<PartKey>) -> CursorMachineCommand {
    CursorMachineCommand::SyncObj {
        obj_key: obj_key.clone(),
        remote_payload: serde_json::json!({ "k": cursor }),
        cursor,
        parts,
    }
}

fn feed(machine: &mut CursorSyncMachine, evt: CursorEvent) -> Vec<CursorMachineCommand> {
    let mut out = Vec::new();
    machine.on_subscription_evt(evt, &mut out);
    out
}

fn done(machine: &mut CursorSyncMachine, job: JobKey, cursor: Rev) -> Vec<CursorMachineCommand> {
    let mut out = Vec::new();
    machine.on_obj_sync_job_evt(job, cursor, &mut out);
    out
}

fn sync_done(
    machine: &mut CursorSyncMachine,
    obj_key: &ObjKey,
    cursor: Rev,
) -> Vec<CursorMachineCommand> {
    done(machine, JobKey::Sync(obj_key.clone()), cursor)
}

fn remove_done(
    machine: &mut CursorSyncMachine,
    obj_key: &ObjKey,
    cursor: Rev,
) -> Vec<CursorMachineCommand> {
    done(machine, JobKey::RemoveFromPart(obj_key.clone()), cursor)
}

fn set_cursor(part_key: &PartKey, cursor: Rev) -> CursorMachineCommand {
    CursorMachineCommand::SetPartCursor {
        part_key: part_key.clone(),
        cursor,
    }
}

fn removal_command(obj_key: &ObjKey, part_key: &PartKey, cursor: Rev) -> CursorMachineCommand {
    CursorMachineCommand::RemoveObjFromParts {
        obj_key: obj_key.clone(),
        part_key: part_key.clone(),
        cursor,
    }
}

fn advances_part(commands: &[CursorMachineCommand], part_key: &PartKey) -> bool {
    commands.iter().any(|cmd| {
        matches!(cmd, CursorMachineCommand::SetPartCursor { part_key: advanced, .. } if advanced == part_key)
    })
}

/// A membership touch schedules exactly one object sync naming that part, and completing
/// the sync advances the part.
#[test]
fn law_1_a_membership_touch_schedules_one_sync_and_advances_on_completion() {
    let (o, p) = (obj(1), part(2));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
        vec![sync_obj(&o, 5, vec![p.clone()])]
    );
    assert_eq!(sync_done(&mut machine, &o, 5), vec![set_cursor(&p, 5)]);
}

/// A touch spanning several parts is one object sync listing them in event order, and one
/// completion gates all of them.
#[test]
fn law_4_a_multi_part_touch_is_one_sync_listing_parts_in_event_order() {
    let (o, a, b) = (obj(3), part(4), part(5));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()])),
        vec![sync_obj(&o, 7, vec![a.clone(), b.clone()])]
    );
    assert_eq!(
        sync_done(&mut machine, &o, 7),
        vec![set_cursor(&a, 7), set_cursor(&b, 7)]
    );
}

/// A part already admitted for this cursor is dropped from `parts` rather than tracked
/// again, and the slot it holds still finishes.
#[test]
fn law_4_a_readmitted_part_is_not_tracked_twice_and_does_not_strand_its_slot() {
    let (o, a, b) = (obj(6), part(7), part(8));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(7, &o, std::slice::from_ref(&a))),
        vec![sync_obj(&o, 7, vec![a.clone()])]
    );
    assert_eq!(
        feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()])),
        vec![sync_obj(&o, 7, vec![b.clone()])],
        "the repeat names only the part it newly admitted"
    );
    assert_eq!(
        sync_done(&mut machine, &o, 7),
        vec![set_cursor(&a, 7), set_cursor(&b, 7)]
    );
}

/// A touch whose every part was already admitted is a pure re-delivery and schedules
/// nothing. (The original machine fell through to an object-only sync here.)
#[test]
fn law_4_a_fully_rejected_touch_schedules_nothing() {
    let (o, a) = (obj(10), part(11));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(4, &o, std::slice::from_ref(&a))),
        vec![sync_obj(&o, 4, vec![a.clone()])]
    );
    assert_eq!(
        feed(&mut machine, touched(4, &o, std::slice::from_ref(&a))),
        vec![]
    );
}

/// A removal pushes the removal command after any advances its supersede freed, and the
/// removal's own cursor is gated until the removal completes: advancing earlier would
/// persist a cursor for a membership write that has not happened.
#[test]
fn law_3_a_removal_gates_its_cursor_until_the_removal_completes() {
    let (o, p) = (obj(12), part(13));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    assert_eq!(sync_done(&mut machine, &o, 5), vec![set_cursor(&p, 5)]);

    assert_eq!(
        feed(&mut machine, removed(6, &o, &p)),
        vec![removal_command(&o, &p, 6)]
    );
    assert_eq!(remove_done(&mut machine, &o, 6), vec![set_cursor(&p, 6)]);
}

/// A removal frees the removed part from the object's outstanding sync at once, while the
/// sync keeps gating the object's other parts.
#[test]
fn law_3_a_removal_supersedes_the_sync_only_on_the_removed_part() {
    let (o, a, b) = (obj(14), part(15), part(16));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()]));

    assert_eq!(
        feed(&mut machine, removed(8, &o, &a)),
        vec![set_cursor(&a, 7), removal_command(&o, &a, 8)],
        "a's sync slot is freed, then the removal is scheduled"
    );
    let after = remove_done(&mut machine, &o, 8);
    assert_eq!(after, vec![set_cursor(&a, 8)]);
    assert!(!advances_part(&after, &b));

    assert_eq!(sync_done(&mut machine, &o, 7), vec![set_cursor(&b, 7)]);
}

/// A sync and a removal of the same object at the same cursor, on different parts, gate
/// only their own part. Superseding the sync's part later frees it without touching the
/// removal's part.
#[test]
fn law_3_a_supersede_frees_only_the_superseded_part() {
    let (o, p, q) = (obj(19), part(20), part(21));
    let mut machine = CursorSyncMachine::default();

    feed(&mut machine, touched(5, &o, std::slice::from_ref(&q)));
    feed(&mut machine, removed(5, &o, &p));

    let superseded = feed(&mut machine, removed(9, &o, &q));
    assert!(superseded.contains(&set_cursor(&q, 5)), "{superseded:?}");
    assert!(!advances_part(&superseded, &p), "{superseded:?}");

    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![],
        "the sync no longer gates any part"
    );
    assert_eq!(remove_done(&mut machine, &o, 5), vec![set_cursor(&p, 5)]);
}

/// Two removals of one object at one cursor share a waiter: the layer above runs them as
/// one removal task and reports it once, when every part it holds is done.
#[test]
fn law_1_removals_of_one_object_at_one_cursor_settle_together() {
    let (o, a, b) = (obj(31), part(32), part(33));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, removed(5, &o, &a));
    feed(&mut machine, removed(5, &o, &b));

    assert_eq!(
        remove_done(&mut machine, &o, 5),
        vec![set_cursor(&a, 5), set_cursor(&b, 5)]
    );
}

/// The machine advances a part only to the cursors that settled.
#[test]
fn law_1_a_later_cursor_holds_the_part() {
    let (o, p) = (obj(17), part(18));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    feed(&mut machine, touched(9, &o, std::slice::from_ref(&p)));

    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 5)],
        "the later cursor is still outstanding"
    );
    assert_eq!(sync_done(&mut machine, &o, 9), vec![set_cursor(&p, 9)]);
}

/// The layer above coalesces an object's syncs into one task and reports each cursor it
/// held. Settling only the newer cursor does not free the older one: a part with a slot
/// still open below it stays put.
#[test]
fn law_1_a_coalesced_sync_is_settled_per_cursor() {
    let (o, p, q) = (obj(34), part(35), part(36));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    feed(&mut machine, touched(9, &o, &[p.clone(), q.clone()]));

    assert_eq!(
        sync_done(&mut machine, &o, 9),
        vec![set_cursor(&q, 9)],
        "p still has its slot at 5 open"
    );
    assert_eq!(sync_done(&mut machine, &o, 5), vec![set_cursor(&p, 9)]);
}

/// A completion for a cursor the job has no waiter on, or for a job never tracked, is stale:
/// a cancelled or superseded task can still report back.
#[test]
fn law_7_a_stale_completion_is_a_no_op() {
    let (o, p) = (obj(21), part(22));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));

    assert_eq!(sync_done(&mut machine, &o, 3), vec![]);
    assert_eq!(remove_done(&mut machine, &o, 5), vec![]);
    assert!(machine.owes_obj_job(&JobKey::Sync(o.clone()), 5));
}

/// `remove_part` retires the part's bookkeeping silently: the part is gone, so there is no
/// cursor left to advance.
#[test]
fn law_5_remove_part_retires_silently() {
    let (o, p) = (obj(25), part(26));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    machine.remove_part(p.clone());
    assert_eq!(sync_done(&mut machine, &o, 5), vec![]);
}

/// Completions reported out of order hold the part at the lowest unsettled slot, and the
/// emitted cursors only ever grow.
#[test]
fn law_2_part_cursors_only_go_forward() {
    let (o, p) = (obj(40), part(41));
    let mut machine = CursorSyncMachine::default();
    for cursor in [3, 5, 7] {
        feed(&mut machine, touched(cursor, &o, std::slice::from_ref(&p)));
    }
    assert_eq!(sync_done(&mut machine, &o, 7), vec![]);
    assert_eq!(sync_done(&mut machine, &o, 3), vec![set_cursor(&p, 3)]);
    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 7)],
        "5 frees the settled 7 behind it"
    );
    assert_eq!(
        feed(&mut machine, touched(6, &o, std::slice::from_ref(&p))),
        vec![],
        "a cursor below the emitted one is not admitted, so the cursor cannot go back"
    );
}

/// A part skips a cursor it already emitted past, and a removal it already saw schedules no
/// removal command.
#[test]
fn law_4_a_cursor_the_part_emitted_past_is_skipped() {
    let (o, p, q) = (obj(42), part(43), part(44));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    sync_done(&mut machine, &o, 5);

    assert_eq!(
        feed(&mut machine, touched(5, &o, &[p.clone(), q.clone()])),
        vec![sync_obj(&o, 5, vec![q.clone()])],
        "p is at 5 already, q is new"
    );
    assert_eq!(
        feed(&mut machine, touched(4, &o, std::slice::from_ref(&p))),
        vec![]
    );
    assert_eq!(feed(&mut machine, removed(5, &o, &p)), vec![]);
    assert_eq!(feed(&mut machine, removed(3, &o, &p)), vec![]);
    assert_eq!(
        feed(&mut machine, removed(6, &o, &p)),
        vec![removal_command(&o, &p, 6)],
        "the first cursor past the watermark is admitted"
    );
}

/// A removal frees only the object's syncs *below* its cursor on that part: a later sync of
/// the same object, and another object's earlier sync, keep gating it.
#[test]
fn law_3_a_removal_releases_only_the_objects_earlier_syncs() {
    let (o, other, p) = (obj(45), obj(46), part(47));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(3, &other, std::slice::from_ref(&p)));
    feed(&mut machine, touched(4, &o, std::slice::from_ref(&p)));
    feed(&mut machine, touched(8, &o, std::slice::from_ref(&p)));

    assert_eq!(
        feed(&mut machine, removed(6, &o, &p)),
        vec![removal_command(&o, &p, 6)],
        "the other object's sync at 3 still holds p, though o's sync at 4 was released"
    );
    assert_eq!(sync_done(&mut machine, &other, 3), vec![set_cursor(&p, 4)]);
    assert_eq!(remove_done(&mut machine, &o, 6), vec![set_cursor(&p, 6)]);
    assert!(!machine.is_settled(&p), "o's sync at 8 is still owed");
    assert_eq!(sync_done(&mut machine, &o, 8), vec![set_cursor(&p, 8)]);
    assert!(machine.is_settled(&p));
}

/// A retired part's slots go with it: a job it shared with another part settles that other
/// part, and the other part never waits on the retired one.
#[test]
fn law_5_a_retired_part_never_stalls_another() {
    let (o, p, q) = (obj(48), part(49), part(50));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, &[p.clone(), q.clone()]));
    feed(&mut machine, touched(9, &o, std::slice::from_ref(&p)));
    machine.remove_part(p.clone());

    assert!(machine.is_settled(&p));
    assert!(
        machine.owes_obj_job(&JobKey::Sync(o.clone()), 5),
        "q still owes it"
    );
    assert!(
        !machine.owes_obj_job(&JobKey::Sync(o.clone()), 9),
        "only p held 9"
    );
    assert_eq!(sync_done(&mut machine, &o, 5), vec![set_cursor(&q, 5)]);
    assert_eq!(sync_done(&mut machine, &o, 9), vec![]);
}

/// A part added again after its retirement starts fresh: cursors it had passed are admitted.
#[test]
fn law_5_a_retired_part_comes_back_fresh() {
    let (o, p) = (obj(51), part(52));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    sync_done(&mut machine, &o, 5);
    machine.advance_part_replay_cursor(p.clone(), 9);
    machine.remove_part(p.clone());

    assert_eq!(
        machine.part_replay_cursor(&p, 0),
        0,
        "the replay cursor went with it"
    );
    assert_eq!(
        feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
        vec![sync_obj(&o, 5, vec![p.clone()])]
    );
}

/// A part owes nothing until an event opens a slot, and again once every slot passed.
#[test]
fn law_6_is_settled_tracks_what_the_part_owes() {
    let (o, p, q) = (obj(53), part(54), part(55));
    let mut machine = CursorSyncMachine::default();
    assert!(machine.is_settled(&p), "a part never heard of owes nothing");

    feed(&mut machine, touched(5, &o, &[p.clone(), q.clone()]));
    assert!(!machine.is_settled(&p));
    assert!(!machine.is_settled(&q));
    feed(&mut machine, removed(6, &o, &p));
    sync_done(&mut machine, &o, 5);
    assert!(!machine.is_settled(&p), "p still owes its removal");
    assert!(machine.is_settled(&q));
    remove_done(&mut machine, &o, 6);
    assert!(machine.is_settled(&p));
}

/// A duplicate completion changes nothing once the first one passed the part.
#[test]
fn law_7_a_duplicate_completion_is_a_no_op() {
    let (o, p) = (obj(56), part(57));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    assert_eq!(sync_done(&mut machine, &o, 5), vec![set_cursor(&p, 5)]);
    assert_eq!(sync_done(&mut machine, &o, 5), vec![]);
    assert!(machine.is_settled(&p));
}

/// The cursors reached do not depend on the order two jobs' completions arrive in.
#[test]
fn law_8_completions_commute() {
    let (a, b, p) = (obj(58), obj(59), part(60));
    let reached = |first_a: bool| {
        let mut machine = CursorSyncMachine::default();
        feed(&mut machine, touched(3, &a, std::slice::from_ref(&p)));
        feed(&mut machine, touched(4, &b, std::slice::from_ref(&p)));
        let mut cmds = Vec::new();
        let order = if first_a {
            [(&a, 3), (&b, 4)]
        } else {
            [(&b, 4), (&a, 3)]
        };
        for (o, cursor) in order {
            cmds.extend(sync_done(&mut machine, o, cursor));
        }
        (cmds.last().cloned(), machine.is_settled(&p))
    };
    assert_eq!(reached(true), reached(false));
    assert_eq!(reached(true), (Some(set_cursor(&p, 4)), true));
}

#[test]
fn law_9_replay_cursors_are_scheduling_state() {
    let p = part(61);
    let mut machine = CursorSyncMachine::default();
    assert_eq!(
        machine.part_replay_cursor(&p, 4),
        4,
        "falls back to the applied cursor"
    );
    machine.advance_part_replay_cursor(p.clone(), 9);
    assert_eq!(machine.part_replay_cursor(&p, 4), 9);
    machine.advance_part_replay_cursor(p.clone(), 6);
    assert_eq!(machine.part_replay_cursor(&p, 4), 9, "never goes backwards");
    assert_eq!(
        machine.part_replay_cursor(&p, 12),
        12,
        "never behind the applied cursor"
    );
}

/// An event naming no part is an object route event: a caller bug, not input to recover from.
#[test]
#[should_panic(expected = "belongs to ObjectJobs")]
fn an_event_naming_no_part_is_a_caller_bug() {
    feed(&mut CursorSyncMachine::default(), touched(5, &obj(62), &[]));
}
