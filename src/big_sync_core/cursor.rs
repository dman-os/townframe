//! TODO: emit sync stats event for use in full.rs
//! FIXME: figure out the delete obj story

use crate::interlude::*;

use crate::part_store::{CursorIndex, ObjPayload};
use crate::watermark::{StreamDrop, WatermarkMachine};

use std::collections::HashMap;

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CursorMachineCommand {
        SyncObj {
            obj_id: ObjKey,
            remote_payload: ObjPayload,
            cursor: CursorIndex,
            /// Upsert the object at the sync backend to these
            /// parts
            parts: Vec<PartKey>,
        },
        SetPartCursor {
            part_id: PartKey,
            cursor: CursorIndex
        },
        /// Scheduling signal only: the outer machine turns this into a
        /// `SyncTaskKind::RemoveFromParts` task executed by the backend.
        /// The machine itself never mutates part membership.
        RemoveObjFromParts {
            obj_id: ObjKey,
            part_id: PartKey,
            cursor: CursorIndex,
        },
        PartIdle {
            part_id: PartKey,
        }
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CursorJobCompletionKind {
        Membership,
        Sync,
    }
}

structstruck::strike! {
#[derive(Debug, Default)]
pub struct CursorSyncMachine {
    /// Object-target events have no real part cursor to advance. Keep the
    /// last cursor locally so duplicate deliveries do not schedule the same
    /// object sync twice; durable replay bounds remain request-level.
    object_cursors: HashMap<ObjKey, CursorIndex>,
    /// Per-part slot bookkeeping AND the obj-job aggregation gating it: the
    /// [`WatermarkMachine`] primitive rather than a machine-local copy, so
    /// "what is outstanding, what has settled, when may the cursor advance"
    /// has one implementation, shared with the delta walker. The job is the
    /// object, a stream is a part whose cursor it gates, and the lane is which
    /// half of the work — the membership write or the object sync — is owed.
    ///
    /// The primitive owns the shared-slot rule too: a `(stream, cursor)` slot
    /// advances only once every job gating it has settled. This machine admits
    /// a given `(part, cursor)` only once (`mark_pending_cursor` rejects a
    /// repeat), so its slots have a single waiter — the rule matters for the
    /// delta walker's single-stream shape, and the machine no longer needs an
    /// opinion of its own about it.
    jobs: WatermarkMachine<PartKey, ObjKey, CursorJobCompletionKind, (), CursorIndex>,
    }
}

impl CursorSyncMachine {
    pub(crate) fn remove_part(&mut self, part_id: PartKey) {
        // Drops the part's slot book and every waiter that gated only it.
        self.jobs.retire_stream(part_id);
    }

    pub(crate) fn supersede_obj_part(
        &mut self,
        obj_id: ObjKey,
        part_id: PartKey,
        before_cursor: CursorIndex,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        let advances = self
            .jobs
            .drop_stream(part_id, obj_id, before_cursor, |waiter| {
                if waiter.lanes.contains(&CursorJobCompletionKind::Membership)
                    && waiter.streams.len() == 1
                {
                    // The already-queued membership mutation still has to finish
                    // before its cursor can advance, but the later removal makes
                    // fetching the object's contents unnecessary.
                    StreamDrop::Retain {
                        keep: vec![CursorJobCompletionKind::Membership],
                    }
                } else {
                    StreamDrop::Release
                }
            });
        self.emit_advances(advances, out);
    }
    fn mark_pending_cursor(&mut self, part_id: &PartKey, cursor: CursorIndex) -> bool {
        // The admission guard (duplicate cursor, or at-or-below the emitted
        // watermark) lives in the primitive.
        let emitted = self.jobs.watermark(part_id);
        let admitted = self.jobs.admit(part_id.clone(), cursor);
        if !admitted {
            // Upstream's diagnostic lived at this site before the guard moved into
            // the primitive. `emitted` is logged so the two rejections stay apart: a
            // cursor at or below it is a duplicate of the emitted watermark, otherwise
            // it is an already-pending duplicate.
            tracing::debug!(
                ?part_id,
                ?cursor,
                ?emitted,
                "cursor machine ignored event: cursor not admitted by watermark book",
            );
        }
        admitted
    }
    pub fn on_subscription_evt(
        &mut self,
        evt: crate::rpc::SubEvent,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        use crate::rpc::*;

        match evt {
            SubEvent::Changed(evt) => {
                tracing::trace!(
                    ?evt.obj_id,
                    ?evt.cursor,
                    part_count = evt.part_ids.len(),
                    payload = !evt.payload.is_null(),
                    "subscription Changed event",
                );
                let mut parts = vec![];
                for part_id in &evt.part_ids {
                    if !self.mark_pending_cursor(part_id, evt.cursor) {
                        continue;
                    }
                    parts.push(part_id.clone());
                }
                if parts.is_empty() {
                    let last_cursor = self.object_cursors.entry(evt.obj_id.clone()).or_default();
                    if evt.cursor <= *last_cursor {
                        tracing::debug!(
                            ?evt.obj_id,
                            ?evt.cursor,
                            ?last_cursor,
                            "cursor machine ignored object-only event: cursor not newer",
                        );
                        return;
                    }
                    *last_cursor = evt.cursor;
                    out.push(CursorMachineCommand::SyncObj {
                        obj_id: evt.obj_id,
                        remote_payload: evt.payload,
                        cursor: evt.cursor,
                        parts: Vec::new(),
                    });
                    return;
                }
                self.track_obj_job(
                    evt.obj_id.clone(),
                    evt.cursor,
                    parts.iter().cloned(),
                    CursorJobCompletionKind::Sync,
                );
                out.push(CursorMachineCommand::SyncObj {
                    obj_id: evt.obj_id,
                    parts,
                    cursor: evt.cursor,
                    remote_payload: evt.payload,
                });
            }
            SubEvent::Removed(evt) => {
                tracing::trace!(
                    ?evt.obj_id,
                    ?evt.cursor,
                    ?evt.part_id,
                    "subscription Removed event",
                );
                if !self.mark_pending_cursor(&evt.part_id, evt.cursor) {
                    return;
                }
                self.supersede_obj_part(evt.obj_id.clone(), evt.part_id.clone(), evt.cursor, out);
                self.track_obj_job(
                    evt.obj_id.clone(),
                    evt.cursor,
                    [evt.part_id.clone()],
                    CursorJobCompletionKind::Membership,
                );
                out.push(CursorMachineCommand::RemoveObjFromParts {
                    cursor: evt.cursor,
                    obj_id: evt.obj_id,
                    part_id: evt.part_id,
                });
            }
            SubEvent::ReplayComplete => unreachable!(),
        }
    }

    pub fn on_obj_sync_job_evt(
        &mut self,
        obj_id: ObjKey,
        cursor: CursorIndex,
        kind: CursorJobCompletionKind,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        // A completion for a job/cursor that is not tracked is stale, not an
        // error; the primitive returns nothing for it. A completion for a lane
        // the waiter never owed still panics inside the primitive.
        let advances = self.jobs.settle(obj_id, cursor, kind);
        self.emit_advances(advances, out);
    }

    /// Emit the part-cursor commands for streams that just reached a new
    /// watermark. `PartIdle` follows `SetPartCursor` only when the part has no
    /// un-advanced cursors left.
    fn emit_advances(
        &mut self,
        advances: Vec<(PartKey, Option<CursorIndex>)>,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        for (part_id, reached) in advances {
            let Some(cursor) = reached else {
                continue;
            };
            out.push(CursorMachineCommand::SetPartCursor {
                part_id: part_id.clone(),
                cursor,
            });
            if self.jobs.is_settled(&part_id) {
                out.push(CursorMachineCommand::PartIdle { part_id });
            }
        }
    }

    /// One obj job gating one cursor across several parts. The machine tracks
    /// per stream, so this is one `track` call per part and the waiter merges
    /// them — a repeated part is not double-counted, or the slot could never
    /// finish.
    fn track_obj_job(
        &mut self,
        obj_id: ObjKey,
        cursor: CursorIndex,
        parts: impl IntoIterator<Item = PartKey>,
        lane: CursorJobCompletionKind,
    ) {
        for part_id in parts {
            self.jobs.track(part_id, obj_id.clone(), cursor, [lane], ());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ByteKey;
    use crate::rpc::{ObjChanged, ObjRemovedFromPart, SubEvent};

    fn obj(seed: u8) -> ObjKey {
        ObjKey(ByteKey::new([seed; 32]))
    }

    fn part(seed: u8) -> PartKey {
        PartKey(ByteKey::new([seed; 32]))
    }

    /// A membership touch: the object is present in `parts` as of `cursor`. There is no
    /// separate "added" kind, so a first membership and a changed one are the same event.
    fn touched(cursor: CursorIndex, obj_id: &ObjKey, parts: &[PartKey]) -> SubEvent {
        SubEvent::Changed(ObjChanged {
            cursor,
            part_ids: parts.to_vec(),
            obj_id: obj_id.clone(),
            payload: serde_json::json!({ "k": cursor }),
        })
    }

    fn removed(cursor: CursorIndex, obj_id: &ObjKey, part_id: &PartKey) -> SubEvent {
        SubEvent::Removed(ObjRemovedFromPart {
            cursor,
            part_id: part_id.clone(),
            obj_id: obj_id.clone(),
        })
    }

    fn sync_obj(obj_id: &ObjKey, cursor: CursorIndex, parts: Vec<PartKey>) -> CursorMachineCommand {
        CursorMachineCommand::SyncObj {
            obj_id: obj_id.clone(),
            remote_payload: serde_json::json!({ "k": cursor }),
            cursor,
            parts,
        }
    }

    fn feed(machine: &mut CursorSyncMachine, evt: SubEvent) -> Vec<CursorMachineCommand> {
        let mut out = Vec::new();
        machine.on_subscription_evt(evt, &mut out);
        out
    }

    fn settle(
        machine: &mut CursorSyncMachine,
        obj_id: &ObjKey,
        cursor: CursorIndex,
        kind: CursorJobCompletionKind,
    ) -> Vec<CursorMachineCommand> {
        let mut out = Vec::new();
        machine.on_obj_sync_job_evt(obj_id.clone(), cursor, kind, &mut out);
        out
    }

    fn set_cursor(part_id: &PartKey, cursor: CursorIndex) -> CursorMachineCommand {
        CursorMachineCommand::SetPartCursor {
            part_id: part_id.clone(),
            cursor,
        }
    }

    fn idle(part_id: &PartKey) -> CursorMachineCommand {
        CursorMachineCommand::PartIdle {
            part_id: part_id.clone(),
        }
    }

    fn removal_command(
        obj_id: &ObjKey,
        part_id: &PartKey,
        cursor: CursorIndex,
    ) -> CursorMachineCommand {
        CursorMachineCommand::RemoveObjFromParts {
            obj_id: obj_id.clone(),
            part_id: part_id.clone(),
            cursor,
        }
    }

    /// Regression guard: a membership touch schedules exactly one object sync naming that part,
    /// and completing the sync advances the part and then marks it idle.
    #[test]
    fn a_membership_touch_schedules_one_sync_and_advances_on_completion() {
        let (o, p) = (obj(1), part(2));
        let mut machine = CursorSyncMachine::default();

        assert_eq!(
            feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
            vec![sync_obj(&o, 5, vec![p.clone()])]
        );
        assert_eq!(
            settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync),
            vec![set_cursor(&p, 5), idle(&p)]
        );
    }

    /// Regression guard: a touch spanning several parts is one object sync listing them in
    /// event order, and one completion gates all of them, so they advance in that order.
    #[test]
    fn a_multi_part_touch_is_one_sync_and_advances_each_part_in_order() {
        let (o, a, b) = (obj(3), part(4), part(5));
        let mut machine = CursorSyncMachine::default();

        assert_eq!(
            feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()])),
            vec![sync_obj(&o, 7, vec![a.clone(), b.clone()])]
        );
        assert_eq!(
            settle(&mut machine, &o, 7, CursorJobCompletionKind::Sync),
            vec![set_cursor(&a, 7), idle(&a), set_cursor(&b, 7), idle(&b)]
        );
    }

    /// A part already admitted for this cursor is dropped from `parts` rather than tracked
    /// again, and the slot it holds still finishes. Tracking it twice would leave a waiter that
    /// nothing could settle, which stalls the part's cursor forever.
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
            settle(&mut machine, &o, 7, CursorJobCompletionKind::Sync),
            vec![set_cursor(&a, 7), idle(&a), set_cursor(&b, 7), idle(&b)]
        );
    }

    /// An object-only touch has no part cursor to advance, so it goes through the object-target
    /// dedup: a repeat at the same or an older cursor schedules nothing, a newer one schedules
    /// the object sync again.
    #[test]
    fn an_object_only_touch_dedups_on_the_object_cursor() {
        let o = obj(9);
        let mut machine = CursorSyncMachine::default();

        assert_eq!(
            feed(&mut machine, touched(3, &o, &[])),
            vec![sync_obj(&o, 3, vec![])]
        );
        assert_eq!(feed(&mut machine, touched(3, &o, &[])), vec![]);
        assert_eq!(feed(&mut machine, touched(2, &o, &[])), vec![]);
        assert_eq!(
            feed(&mut machine, touched(5, &o, &[])),
            vec![sync_obj(&o, 5, vec![])]
        );
    }

    /// The object-only branch does not distinguish "names no parts" from "every part it named
    /// was already admitted": both leave nothing to track, so an event that was fully rejected --
    /// including an exact duplicate of one already admitted -- still schedules an object-target
    /// sync, guarded only by the object cursor. Recorded because a reader would otherwise expect
    /// a rejected or repeated event to emit nothing at all.
    #[test]
    fn a_fully_rejected_touch_takes_the_object_only_branch() {
        let (o, a) = (obj(10), part(11));
        let mut machine = CursorSyncMachine::default();

        assert_eq!(
            feed(&mut machine, touched(4, &o, std::slice::from_ref(&a))),
            vec![sync_obj(&o, 4, vec![a.clone()])]
        );
        assert_eq!(
            feed(&mut machine, touched(4, &o, std::slice::from_ref(&a))),
            vec![sync_obj(&o, 4, vec![])],
            "the part is already admitted, so this is an object-only event"
        );
    }

    /// A removal runs the part's supersede first and pushes the removal command last, and the
    /// cursor of the removal is gated by the membership lane: advancing it before the
    /// membership write lands would persist a cursor for work that has not happened.
    #[test]
    fn a_removal_emits_the_removal_command_last_and_gates_on_the_membership_lane() {
        let (o, p) = (obj(12), part(13));
        let mut machine = CursorSyncMachine::default();
        assert_eq!(
            feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
            vec![sync_obj(&o, 5, vec![p.clone()])]
        );
        assert_eq!(
            settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync),
            vec![set_cursor(&p, 5), idle(&p)]
        );

        assert_eq!(
            feed(&mut machine, removed(6, &o, &p)),
            vec![removal_command(&o, &p, 6)],
            "no cursor advances for the removal until its membership write is done"
        );
        assert_eq!(
            settle(&mut machine, &o, 6, CursorJobCompletionKind::Membership).first(),
            Some(&set_cursor(&p, 6))
        );
    }

    /// A removal for an object gating two parts frees only the removed part: the other stays
    /// gated by the object's outstanding sync, so it cannot advance past it.
    #[test]
    fn a_removal_of_one_part_does_not_release_another_part_of_the_same_object() {
        let (o, a, b) = (obj(14), part(15), part(16));
        let mut machine = CursorSyncMachine::default();
        assert_eq!(
            feed(&mut machine, touched(7, &o, &[a.clone(), b.clone()])),
            vec![sync_obj(&o, 7, vec![a.clone(), b.clone()])]
        );

        let removal = feed(&mut machine, removed(8, &o, &a));
        assert!(
            removal.contains(&removal_command(&o, &a, 8)),
            "the removal is scheduled: {removal:?}"
        );
        let after = settle(&mut machine, &o, 8, CursorJobCompletionKind::Membership);
        assert!(
            after.contains(&set_cursor(&a, 8)),
            "the removed part's own cursor still advances: {after:?}"
        );
        assert!(
            !after.iter().any(|cmd| matches!(
                cmd,
                CursorMachineCommand::SetPartCursor { part_id, .. } if *part_id == b
            )),
            "the other part is still gated by the object's outstanding sync: {after:?}"
        );
    }

    /// Minimal forwarding check, and the `PartIdle` rule with it: the machine advances a part
    /// only to the cursors that settled, and reports idle only when nothing is left pending.
    /// The contiguous-prefix algorithm itself is pinned in `watermark.rs`, so this stays a
    /// check that the machine forwards its answer rather than one that re-tests it.
    #[test]
    fn a_later_cursor_holds_the_part_and_idle_waits_for_it() {
        let (o, p) = (obj(17), part(18));
        let mut machine = CursorSyncMachine::default();
        assert_eq!(
            feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
            vec![sync_obj(&o, 5, vec![p.clone()])]
        );
        assert_eq!(
            feed(&mut machine, touched(9, &o, std::slice::from_ref(&p))),
            vec![sync_obj(&o, 9, vec![p.clone()])]
        );

        assert_eq!(
            settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync),
            vec![set_cursor(&p, 5)],
            "the part is not idle while a later cursor is still outstanding"
        );
        assert_eq!(
            settle(&mut machine, &o, 9, CursorJobCompletionKind::Sync),
            vec![set_cursor(&p, 9), idle(&p)]
        );
    }

    /// Regression guard: a completion for a job the machine never tracked is stale, not an
    /// error, because a cancelled or superseded job can still report back.
    #[test]
    fn a_stale_completion_is_a_no_op() {
        let (o, p) = (obj(21), part(22));
        let mut machine = CursorSyncMachine::default();
        assert_eq!(
            feed(&mut machine, touched(5, &o, std::slice::from_ref(&p))),
            vec![sync_obj(&o, 5, vec![p.clone()])]
        );
        assert_eq!(
            settle(&mut machine, &o, 42, CursorJobCompletionKind::Sync),
            vec![]
        );
    }

    /// A completion for a lane the job never owed is a programming error, not a silent no-op:
    /// accepting it would advance a cursor whose work never ran.
    #[test]
    #[should_panic]
    fn a_wrong_lane_completion_panics() {
        let (o, p) = (obj(23), part(24));
        let mut machine = CursorSyncMachine::default();
        feed(&mut machine, touched(5, &o, &[p]));
        settle(&mut machine, &o, 5, CursorJobCompletionKind::Membership);
    }

    /// `remove_part` retires the part's bookkeeping silently: the part is gone, so there is no
    /// cursor left to advance and no idle to report.
    #[test]
    fn remove_part_retires_silently() {
        let (o, p) = (obj(25), part(26));
        let mut machine = CursorSyncMachine::default();
        feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
        machine.remove_part(p.clone());
        assert_eq!(
            settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync),
            vec![]
        );
    }

    /// The machine is only ever fed replay-derived events, and a replay page's end is not one of
    /// them, so this arm is an invariant rather than a case to handle.
    #[test]
    #[should_panic]
    fn replay_complete_is_a_programming_error() {
        let mut machine = CursorSyncMachine::default();
        feed(&mut machine, SubEvent::ReplayComplete);
    }
}
