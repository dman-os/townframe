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

/// A touch that names no part cursor: the object is the target, so its own
/// cursor is the only thing that can order the replay.
fn touched_obj_only(cursor: Rev, obj_key: &ObjKey) -> CursorEvent {
    touched(cursor, obj_key, &[])
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

fn idle(part_key: &PartKey) -> CursorMachineCommand {
    CursorMachineCommand::PartIdle {
        part_key: part_key.clone(),
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
/// the sync advances the part and then marks it idle.
#[test]
fn a_membership_touch_schedules_one_sync_and_advances_on_completion() {
    let (o, p) = (obj(1), part(2));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
        vec![sync_obj(&o, 5, vec![p.clone()])]
    );
    assert_eq!(machine.pending_replay_work(), 1);
    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 5), idle(&p)]
    );
    assert_eq!(machine.pending_replay_work(), 0);
}

#[test]
fn replay_resume_is_scheduling_state_not_acknowledgement() {
    let o = obj(13);
    let mut machine = CursorSyncMachine::default();

    machine.advance_obj_replay_cursor(o.clone(), 7);
    assert_eq!(machine.obj_resume_cursor(&o), 7);
    assert_eq!(machine.object_replays[&o].acknowledged, 0);

    machine.abandon_obj_sync(&o);
    assert_eq!(machine.obj_resume_cursor(&o), 0);
}

/// An object-target touch is deduped against the replay the sync backend has
/// *acknowledged*, never against one we merely emitted: a touch that arrives while an
/// emitted replay runs must still reach the backend, or one lost notification strands the
/// object for good.
#[test]
fn an_object_touch_is_deduped_against_acknowledged_replays_only() {
    let o = obj(9);
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched_obj_only(3, &o)),
        vec![sync_obj(&o, 3, Vec::new())],
        "a touch naming no part cursor schedules the object sync"
    );
    assert_eq!(
        feed(&mut machine, touched_obj_only(3, &o)),
        vec![],
        "a duplicate of the in-flight replay collapses into it"
    );
    assert_eq!(
        sync_done(&mut machine, &o, 3),
        vec![],
        "an object-only replay gates no part cursor"
    );
    assert_eq!(
        feed(&mut machine, touched_obj_only(4, &o)),
        vec![sync_obj(&o, 4, Vec::new())],
        "a later touch is admitted once the previous replay was acknowledged"
    );
    assert_eq!(
        feed(&mut machine, touched_obj_only(4, &o)),
        vec![],
        "and that acknowledgement dedups its own duplicates"
    );
    assert_eq!(feed(&mut machine, touched_obj_only(2, &o)), vec![]);
}

/// A part-scoped sync of the same object fetches the object's content, so its completion
/// acknowledges the object cursor as well: a later object-only touch at or below it is a
/// duplicate.
#[test]
fn a_part_scoped_completion_acknowledges_the_object_too() {
    let (o, p) = (obj(10), part(11));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched_obj_only(5, &o)),
        vec![sync_obj(&o, 5, Vec::new())]
    );
    assert_eq!(
        feed(&mut machine, touched(7, &o, std::slice::from_ref(&p))),
        vec![sync_obj(&o, 7, vec![p.clone()])]
    );
    assert_eq!(
        sync_done(&mut machine, &o, 7),
        vec![set_cursor(&p, 7), idle(&p)]
    );
    assert_eq!(
        feed(&mut machine, touched_obj_only(7, &o)),
        vec![],
        "the acknowledged replay is not repeated for the object target"
    );
}

/// A part-scoped sync is often the *first* completion the machine sees for an object. The
/// cursor it records is what a later object route resumes from; without it that route
/// starts at 0 and re-reads events the backend already observed.
#[test]
fn a_part_scoped_sync_records_the_object_cursor_of_an_object_with_no_replay_yet() {
    let (o, p) = (obj(27), part(28));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
        vec![sync_obj(&o, 5, vec![p.clone()])]
    );
    assert_eq!(machine.obj_resume_cursor(&o), 0);
    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 5), idle(&p)]
    );
    assert_eq!(machine.obj_resume_cursor(&o), 5);
    assert_eq!(feed(&mut machine, touched_obj_only(5, &o)), vec![]);
}

/// The claim an emitted replay holds is released when the job that owed it dies, so the
/// same cursor is replayed again instead of being suppressed forever. The acknowledged
/// position survives: a dying job does not un-observe it.
#[test]
fn an_abandoned_object_replay_is_owed_again() {
    let o = obj(12);
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched_obj_only(6, &o)),
        vec![sync_obj(&o, 6, Vec::new())]
    );
    machine.abandon_obj_sync(&o);
    assert_eq!(
        feed(&mut machine, touched_obj_only(6, &o)),
        vec![sync_obj(&o, 6, Vec::new())],
        "the re-delivery of an abandoned replay reaches the backend again"
    );

    assert_eq!(sync_done(&mut machine, &o, 6), vec![]);
    machine.abandon_obj_sync(&o);
    assert_eq!(machine.obj_resume_cursor(&o), 6);
    assert_eq!(
        feed(&mut machine, touched_obj_only(7, &o)),
        vec![sync_obj(&o, 7, Vec::new())],
        "and a later touch is still admitted"
    );
}

/// A removal completion is not evidence about the object's *content*: counting it as an
/// acknowledgement would advance the object route past a content replay the backend never
/// observed.
#[test]
fn a_removal_completion_does_not_acknowledge_the_object_replay() {
    let (o, p) = (obj(13), part(14));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched_obj_only(6, &o)),
        vec![sync_obj(&o, 6, Vec::new())]
    );
    assert_eq!(
        feed(&mut machine, removed(7, &o, &p)),
        vec![removal_command(&o, &p, 7)]
    );
    assert_eq!(
        remove_done(&mut machine, &o, 7),
        vec![set_cursor(&p, 7), idle(&p)]
    );
    assert_eq!(machine.obj_resume_cursor(&o), 0);
    assert_eq!(
        feed(&mut machine, touched_obj_only(6, &o)),
        vec![],
        "the content replay at 6 is still the claim in flight"
    );
    assert_eq!(sync_done(&mut machine, &o, 6), vec![]);
    assert_eq!(machine.obj_resume_cursor(&o), 6);
}

/// An object-only content replay and a removal can share one cursor. The sync completion
/// acknowledges the object, and the removal stays owed until its own completion: they are
/// different jobs.
#[test]
fn a_sync_completion_acknowledges_the_object_beside_a_removal_at_the_same_cursor() {
    let (o, p) = (obj(29), part(30));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched_obj_only(7, &o)),
        vec![sync_obj(&o, 7, Vec::new())]
    );
    assert_eq!(
        feed(&mut machine, removed(7, &o, &p)),
        vec![removal_command(&o, &p, 7)]
    );
    assert_eq!(sync_done(&mut machine, &o, 7), vec![]);
    assert_eq!(machine.obj_resume_cursor(&o), 7);
    assert!(machine.owes_obj_job(&JobKey::RemoveFromPart(o.clone()), 7));
    assert_eq!(
        remove_done(&mut machine, &o, 7),
        vec![set_cursor(&p, 7), idle(&p)]
    );
}

/// A touch spanning several parts is one object sync listing them in event order, and one
/// completion gates all of them.
#[test]
fn a_multi_part_touch_is_one_sync_and_advances_each_part() {
    let (o, a, b) = (obj(3), part(4), part(5));
    let mut machine = CursorSyncMachine::default();

    assert_eq!(
        feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()])),
        vec![sync_obj(&o, 7, vec![a.clone(), b.clone()])]
    );
    assert_eq!(
        sync_done(&mut machine, &o, 7),
        vec![set_cursor(&a, 7), idle(&a), set_cursor(&b, 7), idle(&b)]
    );
}

/// A part already admitted for this cursor is dropped from `parts` rather than tracked
/// again, and the slot it holds still finishes.
#[test]
fn a_readmitted_part_is_not_tracked_twice_and_does_not_strand_its_slot() {
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
        vec![set_cursor(&a, 7), idle(&a), set_cursor(&b, 7), idle(&b)]
    );
}

/// A touch whose every part was already admitted is a pure re-delivery and schedules
/// nothing. (The original machine fell through to an object-only sync here.)
#[test]
fn a_fully_rejected_touch_schedules_nothing() {
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
fn a_removal_gates_its_cursor_until_the_removal_completes() {
    let (o, p) = (obj(12), part(13));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 5), idle(&p)]
    );

    assert_eq!(
        feed(&mut machine, removed(6, &o, &p)),
        vec![removal_command(&o, &p, 6)]
    );
    assert_eq!(
        remove_done(&mut machine, &o, 6),
        vec![set_cursor(&p, 6), idle(&p)]
    );
}

/// A removal frees the removed part from the object's outstanding sync at once, while the
/// sync keeps gating the object's other parts.
#[test]
fn a_removal_supersedes_the_sync_only_on_the_removed_part() {
    let (o, a, b) = (obj(14), part(15), part(16));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()]));

    assert_eq!(
        feed(&mut machine, removed(8, &o, &a)),
        vec![set_cursor(&a, 7), removal_command(&o, &a, 8)],
        "a's sync slot is freed, then the removal is scheduled"
    );
    let after = remove_done(&mut machine, &o, 8);
    assert_eq!(after, vec![set_cursor(&a, 8), idle(&a)]);
    assert!(!advances_part(&after, &b));

    assert_eq!(
        sync_done(&mut machine, &o, 7),
        vec![set_cursor(&b, 7), idle(&b)]
    );
}

/// A sync and a removal of the same object at the same cursor, on different parts, gate
/// only their own part. Superseding the sync's part later frees it without touching the
/// removal's part.
#[test]
fn a_supersede_frees_only_the_superseded_part() {
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
    assert_eq!(
        remove_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 5), idle(&p)]
    );
}

/// Two removals of one object at one cursor share a waiter: the layer above runs them as
/// one removal task and reports it once, when every part it holds is done.
#[test]
fn removals_of_one_object_at_one_cursor_settle_together() {
    let (o, a, b) = (obj(31), part(32), part(33));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, removed(5, &o, &a));
    feed(&mut machine, removed(5, &o, &b));

    assert_eq!(
        remove_done(&mut machine, &o, 5),
        vec![set_cursor(&a, 5), idle(&a), set_cursor(&b, 5), idle(&b)]
    );
}

/// The machine advances a part only to the cursors that settled, and reports idle only when
/// nothing is left pending.
#[test]
fn a_later_cursor_holds_the_part_and_idle_waits_for_it() {
    let (o, p) = (obj(17), part(18));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    feed(&mut machine, touched(9, &o, std::slice::from_ref(&p)));

    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 5)],
        "the part is not idle while a later cursor is still outstanding"
    );
    assert_eq!(
        sync_done(&mut machine, &o, 9),
        vec![set_cursor(&p, 9), idle(&p)]
    );
}

/// The layer above coalesces an object's syncs into one task and reports each cursor it
/// held. Settling only the newer cursor does not free the older one: a part with a slot
/// still open below it stays put.
#[test]
fn a_coalesced_sync_is_settled_per_cursor() {
    let (o, p, q) = (obj(34), part(35), part(36));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    feed(&mut machine, touched(9, &o, &[p.clone(), q.clone()]));

    assert_eq!(
        sync_done(&mut machine, &o, 9),
        vec![set_cursor(&q, 9), idle(&q)],
        "p still has its slot at 5 open"
    );
    assert_eq!(
        sync_done(&mut machine, &o, 5),
        vec![set_cursor(&p, 9), idle(&p)]
    );
}

/// A completion for a cursor the job has no waiter on, or for a job never tracked, is stale:
/// a cancelled or superseded task can still report back.
#[test]
fn a_stale_completion_is_a_no_op() {
    let (o, p) = (obj(21), part(22));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));

    assert_eq!(sync_done(&mut machine, &o, 3), vec![]);
    assert_eq!(remove_done(&mut machine, &o, 5), vec![]);
    assert!(machine.owes_obj_job(&JobKey::Sync(o.clone()), 5));
}

/// `remove_part` retires the part's bookkeeping silently: the part is gone, so there is no
/// cursor left to advance and no idle to report.
#[test]
fn remove_part_retires_silently() {
    let (o, p) = (obj(25), part(26));
    let mut machine = CursorSyncMachine::default();
    feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
    machine.remove_part(p.clone());
    assert_eq!(sync_done(&mut machine, &o, 5), vec![]);
    assert_eq!(machine.pending_replay_work(), 0);
}
