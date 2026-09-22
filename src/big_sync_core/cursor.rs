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
    /// What each object-target's *content* replay has reached, in flight, for this session.
    ///
    /// An object target has no part cursor to advance, so `SyncTarget::Object { cursor }`
    /// carries the object's own position — its index in that object's own event stream
    /// (the keyed frontier for the object), not a position in any part's stream. The
    /// route is session-scoped: nothing here is persisted per peer (the durable
    /// `big_sync_peer_cursors` table is keyed by `part_ref` and has no object column), so a
    /// route with no recorded position starts at 0, as `replay_page` seeds it, rather than
    /// at "the lowest cursor of the parts this peer is tracking" — that derivation is not
    /// even defined for an object that belongs to no tracked part, and would silently
    /// under-read rather than fail.
    ///
    /// The machine may not infer the position from having emitted the trigger: an
    /// emitted-but-unacknowledged replay is still outstanding, so a re-delivery of the
    /// *same* cursor still has to reach the backend. Keeping the emitted cursor here
    /// instead let one replay whose job died strand the object for good — every later
    /// delivery read as "not newer" and nothing re-emitted it.
    object_replays: HashMap<ObjKey, ObjectReplay>,
    /// The newest cursor scanned for each part route. This is a scheduling cursor,
    /// distinct from the applied watermark owned by `jobs`.
    part_replay_cursors: HashMap<PartKey, CursorIndex>,
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
/// One object target's replay state: what the sync backend has *acknowledged*,
/// and what is emitted but not acknowledged yet.
///
/// `acknowledged` is the newest cursor whose replay the backend completed. It is
/// the only cursor that may suppress a re-delivery: the backend observed that
/// replay and decided what the object needed.
///
/// `replay_cursor` is the newest cursor scanned by the responder. It is separate
/// from `acknowledged`, so a page can make scheduling progress without claiming
/// that the backend applied the work.
///
/// `in_flight` is the newest cursor emitted and still owed. It only collapses a
/// burst of duplicate deliveries of the *same* cursor into one job; the claim is
/// released by the acknowledgement, or by [`CursorSyncMachine::abandon_obj_sync`]
/// when the job dies without one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ObjectReplay {
    /// The newest cursor the backend has acknowledged as applied.
    acknowledged: CursorIndex,
    /// The newest cursor the replay responder has scanned for this object route.
    /// This may be ahead of `acknowledged` while work is pending.
    replay_cursor: CursorIndex,
    in_flight: Option<CursorIndex>,
}

impl CursorSyncMachine {
    pub(crate) fn remove_part(&mut self, part_id: PartKey) {
        // Drops the part's slot book and every waiter that gated only it.
        self.jobs.retire_stream(part_id.clone());
        self.part_replay_cursors.remove(&part_id);
    }

    /// Number of replay work units admitted by this cursor machine but not yet
    /// settled. This is deliberately independent of the task scheduler's live
    /// capacity: the outer machine uses it only as a replay receive window.
    pub(crate) fn pending_replay_work(&self) -> usize {
        self.jobs.pending_jobs().len()
            + self
                .object_replays
                .values()
                .filter(|replay| replay.in_flight.is_some())
                .count()
    }

    pub(crate) fn part_replay_cursor(
        &self,
        part_id: &PartKey,
        applied: CursorIndex,
    ) -> CursorIndex {
        self.part_replay_cursors
            .get(part_id)
            .copied()
            .unwrap_or(applied)
            .max(applied)
    }

    pub(crate) fn advance_part_replay_cursor(&mut self, part_id: PartKey, cursor: CursorIndex) {
        let entry = self.part_replay_cursors.entry(part_id).or_default();
        *entry = (*entry).max(cursor);
    }

    /// Release the *claim* an object's replay held because the job that owed it is
    /// gone.
    ///
    /// A dropped job never acknowledges, and the machine must not keep a claim it
    /// can no longer settle: releasing it re-owes the object, so the next delivery
    /// (or the abandoned one, re-delivered) reaches the backend again and the
    /// backend decides afresh whether the object needs anything. The acknowledged
    /// position is *not* forgotten — it records what the backend has observed, and
    /// a dropped job does not un-observe it. Forgetting it would also send the
    /// object route back to the start of its replay.
    pub(crate) fn abandon_obj_sync(&mut self, obj_id: &ObjKey) {
        if let Some(replay) = self.object_replays.get_mut(obj_id) {
            replay.in_flight = None;
            replay.replay_cursor = replay.acknowledged;
        }
    }

    /// Where an object route's replay must resume.
    ///
    /// An object route has no part cursor of its own to advance, so this has to
    /// travel with the route. A store that is not told it replays the object's
    /// derived part from the start on every page, which re-reads the first page
    /// forever and never reaches the second.
    pub(crate) fn obj_resume_cursor(&self, obj_id: &ObjKey) -> CursorIndex {
        self.object_replays
            .get(obj_id)
            .map(|replay| replay.replay_cursor)
            .unwrap_or_default()
    }

    pub(crate) fn advance_obj_replay_cursor(&mut self, obj_id: ObjKey, cursor: CursorIndex) {
        let replay = self.object_replays.entry(obj_id).or_default();
        replay.replay_cursor = replay.replay_cursor.max(cursor);
    }

    /// Whether `(obj_id, cursor)` still owes `kind` on some part.
    ///
    /// A completion must ask this before settling: the waiter is the authority on
    /// what is owed, and [`Self::supersede_obj_part`] drops a cursor's sync lane
    /// when a removal supersedes it, while the sync task already scheduled for
    /// that cursor stays in flight.
    pub(crate) fn owes_obj_job_lane(
        &self,
        obj_id: &ObjKey,
        cursor: CursorIndex,
        kind: CursorJobCompletionKind,
    ) -> bool {
        self.jobs.owes_lane(obj_id, cursor, &kind)
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
        evt: crate::rpc::PartEvent,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        use crate::rpc::*;

        match evt {
            PartEvent::Changed(evt) => {
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
                    let replay = self.object_replays.entry(evt.obj_id.clone()).or_default();
                    if evt.cursor <= replay.acknowledged {
                        tracing::debug!(
                            ?evt.obj_id,
                            ?evt.cursor,
                            acknowledged = ?replay.acknowledged,
                            "cursor machine dropped an object-only event the backend already acknowledged",
                        );
                        return;
                    }
                    if replay
                        .in_flight
                        .is_some_and(|in_flight| evt.cursor <= in_flight)
                    {
                        tracing::debug!(
                            ?evt.obj_id,
                            ?evt.cursor,
                            in_flight = ?replay.in_flight,
                            "cursor machine collapsed a duplicate object-only event into the replay in flight",
                        );
                        return;
                    }
                    replay.in_flight = Some(evt.cursor);
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
            PartEvent::Removed(evt) => {
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
        }
    }

    pub fn on_obj_sync_job_evt(
        &mut self,
        obj_id: ObjKey,
        cursor: CursorIndex,
        kind: CursorJobCompletionKind,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        // A *sync* completion is the backend acknowledging a replay: it observed
        // the replay and decided what the object needed, so only now may the
        // object's replay position advance. A part-scoped sync of the same object
        // proves the same thing about its content, so that advances it too.
        //
        // A membership completion is not that evidence: it reports a part
        // membership mutation being applied, so treating it as a content
        // acknowledgement would suppress a content replay the backend never saw.
        if kind == CursorJobCompletionKind::Sync {
            // Seed the entry: a part-scoped sync can be the *first* completion
            // this machine sees for the object, and the cursor it records is
            // what a later object route resumes from. Without the seed nothing
            // is recorded and that route replays the object from the start.
            let replay = self.object_replays.entry(obj_id.clone()).or_default();
            if cursor > replay.acknowledged {
                replay.acknowledged = cursor;
            }
            replay.replay_cursor = replay.replay_cursor.max(cursor);
            if replay
                .in_flight
                .is_some_and(|in_flight| cursor >= in_flight)
            {
                replay.in_flight = None;
            }
            // The claim released above is the *object's*, not the board's, so the
            // acknowledgement is applied whether or not the part board owes a lane
            // for this cursor. The board owes one only where a part-scoped replay
            // registered it: an object-target replay registers no part job at all
            // ([`Self::on_subscription_evt`] takes its object-only branch), and a
            // removal's membership lane can sit at the same cursor (a `Changed`
            // and a `Removed` sharing one). That lane stays owed — settling it
            // here would free a cursor whose membership write never landed.
            if !self.jobs.owes_lane(&obj_id, cursor, &kind) {
                tracing::debug!(
                    ?obj_id,
                    cursor,
                    "sync completion acknowledged an object claim the part board owes no sync lane for",
                );
                return;
            }
        }
        // A completion for a job/cursor that is not tracked is stale, not an
        // error; the primitive returns nothing for it. A membership completion
        // for a lane the waiter never owed still panics inside the primitive, and
        // a sync completion only reaches this call where the board owes its lane.
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
    use crate::rpc::{ObjChanged, ObjRemovedFromPart, PartEvent};

    fn obj(seed: u8) -> ObjKey {
        ObjKey(ByteKey::new([seed; 32]))
    }

    fn part(seed: u8) -> PartKey {
        PartKey(ByteKey::new([seed; 32]))
    }

    /// A membership touch: the object is present in `parts` as of `cursor`. There is no
    /// separate "added" kind, so a first membership and a changed one are the same event.
    fn touched(cursor: CursorIndex, obj_id: &ObjKey, parts: &[PartKey]) -> PartEvent {
        PartEvent::Changed(ObjChanged {
            cursor,
            part_ids: parts.to_vec(),
            obj_id: obj_id.clone(),
            payload: serde_json::json!({ "k": cursor }),
        })
    }

    /// A touch that names no part cursor: the object is the target, so its own
    /// cursor is the only thing that can order the replay.
    fn touched_obj_only(cursor: CursorIndex, obj_id: &ObjKey) -> PartEvent {
        PartEvent::Changed(ObjChanged {
            cursor,
            part_ids: Vec::new(),
            obj_id: obj_id.clone(),
            payload: serde_json::json!({ "k": cursor }),
        })
    }

    fn removed(cursor: CursorIndex, obj_id: &ObjKey, part_id: &PartKey) -> PartEvent {
        PartEvent::Removed(ObjRemovedFromPart {
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

    fn feed(machine: &mut CursorSyncMachine, evt: PartEvent) -> Vec<CursorMachineCommand> {
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
        assert_eq!(machine.pending_replay_work(), 1);
        assert_eq!(
            settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync),
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

    /// Regression guard: an object-target touch is deduped against the replay the
    /// sync backend has *acknowledged*, never against one we merely emitted.
    ///
    /// An emitted-but-unacknowledged replay is still outstanding, so a touch that
    /// arrives while it runs (a duplicate delivery, or a change that landed
    /// meanwhile) must still reach the backend. Deduping on emission let one lost
    /// notification strand the object for good: every later touch read as "not
    /// newer" and nothing re-emitted the sync.
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
            Vec::<CursorMachineCommand>::new(),
            "a duplicate of the in-flight replay collapses into it"
        );
        assert_eq!(
            settle(&mut machine, &o, 3, CursorJobCompletionKind::Sync),
            Vec::<CursorMachineCommand>::new(),
            "an object-only replay gates no part cursor"
        );
        assert_eq!(
            feed(&mut machine, touched_obj_only(4, &o)),
            vec![sync_obj(&o, 4, Vec::new())],
            "a later touch is admitted once the previous replay was acknowledged"
        );
        assert_eq!(
            feed(&mut machine, touched_obj_only(4, &o)),
            Vec::<CursorMachineCommand>::new(),
            "and that acknowledgement dedups its own duplicates"
        );
    }

    /// A part-scoped sync of the same object fetches the object's content, so its
    /// completion acknowledges the object cursor as well: a later object-only touch
    /// at or below it is a duplicate.
    #[test]
    fn a_part_scoped_completion_acknowledges_the_object_too() {
        let (o, p) = (obj(10), part(11));
        let mut machine = CursorSyncMachine::default();

        // The object was delivered as a target once, so the machine tracks it.
        assert_eq!(
            feed(&mut machine, touched_obj_only(5, &o)),
            vec![sync_obj(&o, 5, Vec::new())]
        );
        // A part-scoped sync of the same object completes at a higher cursor. That
        // replay fetched the object's content, so it acknowledges the object too.
        assert_eq!(
            feed(&mut machine, touched(7, &o, std::slice::from_ref(&p))),
            vec![sync_obj(&o, 7, vec![p.clone()])]
        );
        assert_eq!(
            settle(&mut machine, &o, 7, CursorJobCompletionKind::Sync),
            vec![set_cursor(&p, 7), idle(&p)]
        );
        assert_eq!(
            feed(&mut machine, touched_obj_only(7, &o)),
            Vec::<CursorMachineCommand>::new(),
            "the acknowledged replay is not repeated for the object target"
        );
    }

    /// A part-scoped sync is often the *first* completion the machine sees for an
    /// object: the object was replayed as part of a part and no object-target
    /// touch ever created its replay entry. The cursor it records is what a later
    /// object route resumes from — without it that route starts at 0 and re-reads
    /// events the backend already observed.
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
            settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync),
            vec![set_cursor(&p, 5), idle(&p)]
        );

        assert_eq!(
            machine.obj_resume_cursor(&o),
            5,
            "the part-scoped sync acknowledged the object's content at its cursor"
        );
        assert_eq!(
            feed(&mut machine, touched_obj_only(5, &o)),
            Vec::<CursorMachineCommand>::new(),
            "and a later object-target touch at that cursor is a duplicate"
        );
    }

    /// Regression guard: the claim an emitted replay holds is released when the job
    /// that owed it dies, so the same cursor is replayed again instead of being
    /// suppressed forever. Without the release the object is owed a replay nobody
    /// will ever acknowledge, and every later delivery reads as a duplicate.
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

        // The acknowledged position is what the object route resumes from, and a job
        // dying does not un-observe it: only the claim is released.
        assert_eq!(
            settle(&mut machine, &o, 6, CursorJobCompletionKind::Sync),
            Vec::<CursorMachineCommand>::new(),
        );
        machine.abandon_obj_sync(&o);
        assert_eq!(
            machine.obj_resume_cursor(&o),
            6,
            "an abandoned replay keeps the position it reached; forgetting it would \
             send the object route back to the start of its replay",
        );
        assert_eq!(
            feed(&mut machine, touched_obj_only(7, &o)),
            vec![sync_obj(&o, 7, Vec::new())],
            "and a later touch is still admitted"
        );
    }

    /// A membership completion is not evidence about the object's *content*: it
    /// reports a part-membership mutation being applied. Counting it as an
    /// acknowledgement would advance the object route past a content replay the
    /// backend never observed.
    #[test]
    fn a_membership_completion_does_not_acknowledge_the_object_replay() {
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
        assert!(
            settle(&mut machine, &o, 7, CursorJobCompletionKind::Membership)
                .contains(&set_cursor(&p, 7)),
            "the removal's own cursor still advances on its membership completion",
        );

        assert_eq!(
            machine.obj_resume_cursor(&o),
            0,
            "a membership completion must not advance the object route's position",
        );
        assert_eq!(
            feed(&mut machine, touched_obj_only(6, &o)),
            Vec::<CursorMachineCommand>::new(),
            "and the content replay at 6 is still the claim in flight"
        );
        assert_eq!(
            settle(&mut machine, &o, 6, CursorJobCompletionKind::Sync),
            Vec::<CursorMachineCommand>::new(),
            "only a sync completion settles the object's own replay"
        );
        assert_eq!(machine.obj_resume_cursor(&o), 6);
    }

    /// Regression guard: an object-only content replay and a removal's membership
    /// lane can share one cursor, and the sync completion must still release the
    /// object's claim. The claim is what an object route resumes from, so dropping
    /// the completion left `acknowledged` at 0 and the route re-read its first page
    /// forever — while the membership lane, which that completion does not own, has
    /// to stay owed until its own task completes.
    #[test]
    fn a_sync_completion_acknowledges_the_object_beside_a_membership_lane() {
        let (o, p) = (obj(29), part(30));
        let mut machine = CursorSyncMachine::default();

        // The object's content replay at cursor 7, and the removal of the same
        // object from a part, at that same cursor.
        assert_eq!(
            feed(&mut machine, touched_obj_only(7, &o)),
            vec![sync_obj(&o, 7, Vec::new())]
        );
        assert_eq!(
            feed(&mut machine, removed(7, &o, &p)),
            vec![removal_command(&o, &p, 7)]
        );
        assert_eq!(machine.obj_resume_cursor(&o), 0);

        // The content replay the backend was owed completes. Nothing advances on
        // the part: the membership write has not landed.
        assert_eq!(
            settle(&mut machine, &o, 7, CursorJobCompletionKind::Sync),
            Vec::<CursorMachineCommand>::new()
        );

        assert_eq!(
            machine.obj_resume_cursor(&o),
            7,
            "the acknowledged replay is what the object route resumes from"
        );
        assert!(
            machine.owes_obj_job_lane(&o, 7, CursorJobCompletionKind::Membership),
            "the removal's membership lane is not the sync completion's to settle"
        );

        // The membership completion still finishes its own lane, and the part only
        // then advances to the shared cursor.
        assert_eq!(
            settle(&mut machine, &o, 7, CursorJobCompletionKind::Membership),
            vec![set_cursor(&p, 7), idle(&p)]
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

    /// The supersede rule at its hardest: a waiter owing *both* halves of one cursor
    /// across two parts. Superseding one part frees that part's cursor at once while
    /// the waiter keeps gating the other, so the surviving part cannot advance until
    /// the membership write and the sync have both landed.
    #[test]
    fn a_supersede_leaves_a_two_lane_waiter_gating_its_other_part_on_both_halves() {
        let (o, p, q) = (obj(19), part(20), part(21));
        let mut machine = CursorSyncMachine::default();

        // One cursor, both halves: the object's content in part q, and the membership
        // mutation that removes it from part p.
        assert_eq!(
            feed(&mut machine, touched(5, &o, std::slice::from_ref(&q))),
            vec![sync_obj(&o, 5, vec![q.clone()])]
        );
        assert_eq!(
            feed(&mut machine, removed(5, &o, &p)),
            vec![removal_command(&o, &p, 5)]
        );

        // A later removal supersedes q, the part the sync was fetching.
        let superseded = feed(&mut machine, removed(9, &o, &q));
        assert!(
            superseded.contains(&set_cursor(&q, 5)),
            "the superseded part's cursor is freed immediately: {superseded:?}"
        );
        assert!(
            !superseded.iter().any(|cmd| matches!(
                cmd,
                CursorMachineCommand::SetPartCursor { part_id, .. } if *part_id == p
            )),
            "the surviving part is still gated: {superseded:?}"
        );

        let after_sync = settle(&mut machine, &o, 5, CursorJobCompletionKind::Sync);
        assert!(
            !after_sync.iter().any(|cmd| matches!(
                cmd,
                CursorMachineCommand::SetPartCursor { part_id, .. } if *part_id == p
            )),
            "the membership half is still owed, so the surviving part cannot advance: {after_sync:?}"
        );
        let after_membership = settle(&mut machine, &o, 5, CursorJobCompletionKind::Membership);
        assert!(
            after_membership.contains(&set_cursor(&p, 5)),
            "both halves landed, so the surviving part advances: {after_membership:?}"
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
}
