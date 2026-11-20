//! TODO: emit sync stats event for use in full.rs
//! FIXME: figure out the delete obj story

use crate::interlude::*;

use crate::store::{CursorIndex, ObjPayload};
use crate::watermark::{StreamDrop, WatermarkMachine};

strike! {
    pub enum CursorEvent {
        ObjChanged(pub struct {
            pub cursor: CursorIndex,
            // if empty, this came from an object only replay
            pub parts: Vec<PartKey>,
            pub obj_key: ObjKey,
            pub payload: ObjPayload,
        }),
        ObjRemovedFromPart(pub struct {
            pub cursor: CursorIndex,
            pub part_key: PartKey,
            pub obj_key: ObjKey,
        }),
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CursorMachineCommand {
        SyncObj {
            obj_key: ObjKey,
            remote_payload: ObjPayload,
            cursor: CursorIndex,
            /// Upsert the object at the sync backend to these
            /// parts
            parts: Vec<PartKey>,
        },
        SetPartCursor {
            part_key: PartKey,
            cursor: CursorIndex
        },
        RemoveObjFromParts {
            obj_key: ObjKey,
            part_key: PartKey,
            cursor: CursorIndex,
        },
        PartIdle {
            part_key: PartKey,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobLanes {
    Sync,
    RemoveFromPart,
}

structstruck::strike! {
    pub struct CursorSyncMachine {
        /// What each object-target's replay has reached for this session.
        object_replays: HashMap<ObjKey, struct ObjectReplay {
            #![derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
            /// The newest cursor the backend has acknowledged as applied.
            acknowledged: CursorIndex,
            /// The newest cursor the replay responder has scanned for this object route.
            /// This may be ahead of `acknowledged` while work is pending.
            replay_cursor: CursorIndex,
            in_flight: Option<CursorIndex>,
        }>,
        /// The newest cursor scanned for each part route. This is a scheduling cursor,
        /// distinct from the applied watermark owned by `jobs`.
        part_replay_cursors: HashMap<PartKey, CursorIndex>,
        jobs: WatermarkMachine<ObjKey, PartKey, CursorIndex>,
    }
}

impl CursorSyncMachine {
    pub(crate) fn remove_part(&mut self, part_key: PartKey) {
        // Drops the part's slot book and every waiter that gated only it.
        self.jobs.retire_stream(&part_key);
        self.part_replay_cursors.remove(&part_key);
    }

    /// Number of replay work units admitted by this cursor machine but not yet
    /// settled. This is deliberately independent of the task scheduler's live
    /// capacity: the outer machine uses it only as a replay receive window.
    pub(crate) fn pending_replay_work(&self) -> usize {
        self.jobs.pending_jobs_len()
            + self
                .object_replays
                .values()
                .filter(|replay| replay.in_flight.is_some())
                .count()
    }

    pub(crate) fn part_replay_cursor(
        &self,
        part_key: &PartKey,
        applied: CursorIndex,
    ) -> CursorIndex {
        self.part_replay_cursors
            .get(part_key)
            .copied()
            .unwrap_or(applied)
            .max(applied)
    }

    pub(crate) fn advance_part_replay_cursor(&mut self, part_key: PartKey, cursor: CursorIndex) {
        let entry = self.part_replay_cursors.entry(part_key).or_default();
        *entry = (*entry).max(cursor);
    }

    pub(crate) fn abandon_obj_sync(&mut self, obj_key: &ObjKey) {
        // we don't remove the object_replay object to avoid
        // clearing state incase of new work
        if let Some(replay) = self.object_replays.get_mut(obj_key) {
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
    pub(crate) fn obj_resume_cursor(&self, obj_key: &ObjKey) -> CursorIndex {
        self.object_replays
            .get(obj_key)
            .map(|replay| replay.replay_cursor)
            .unwrap_or_default()
    }

    pub(crate) fn advance_obj_replay_cursor(&mut self, obj_key: ObjKey, cursor: CursorIndex) {
        let replay = self.object_replays.entry(obj_key).or_default();
        replay.replay_cursor = replay.replay_cursor.max(cursor);
    }

    /// Whether `(obj_key, cursor)` still owes `kind` on some part.
    ///
    /// A completion must ask this before settling: the waiter is the authority on
    /// what is owed, and [`Self::supersede_obj_part`] drops a cursor's sync lane
    /// when a removal supersedes it, while the sync task already scheduled for
    /// that cursor stays in flight.
    pub(crate) fn owes_obj_job_lane(
        &self,
        obj_key: &ObjKey,
        cursor: CursorIndex,
        kind: JobLanes,
    ) -> bool {
        self.jobs.owes_lane(obj_key, cursor, &kind)
    }

    pub fn on_subscription_evt(&mut self, evt: CursorEvent, out: &mut Vec<CursorMachineCommand>) {
        match evt {
            CursorEvent::ObjChanged(evt) => {
                if evt.parts.is_empty() {
                    let replay = self.object_replays.entry(evt.obj_key.clone()).or_default();
                    if evt.cursor <= replay.acknowledged {
                        tracing::warn!(
                            ?evt.obj_key,
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
                        tracing::warn!(
                            ?evt.obj_key,
                            ?evt.cursor,
                            in_flight = ?replay.in_flight,
                            "cursor machine collapsed a duplicate object-only event into the replay in flight",
                        );
                        return;
                    }
                    replay.in_flight = Some(evt.cursor);
                    out.push(CursorMachineCommand::SyncObj {
                        obj_key: evt.obj_key,
                        remote_payload: evt.payload,
                        cursor: evt.cursor,
                        parts: Vec::new(),
                    });
                    return;
                }
                tracing::trace!(
                    ?evt.obj_key,
                    ?evt.cursor,
                    part_count = evt.parts.len(),
                    payload = !evt.payload.is_null(),
                    "subscription Changed event",
                );
                self.jobs
                    .settle_all_on_lane(evt.obj_key.clone(), JobLanes::RemoveFromPart);
                let parts = self.jobs.add_job(
                    evt.obj_key.clone(),
                    evt.cursor,
                    evt.parts.iter().cloned(),
                    [JobLanes::Sync],
                );
                if parts.len() > 0 {
                    out.push(CursorMachineCommand::SyncObj {
                        obj_key: evt.obj_key,
                        parts,
                        cursor: evt.cursor,
                        remote_payload: evt.payload,
                    });
                }
            }
            CursorEvent::ObjRemovedFromPart(evt) => {
                tracing::trace!(
                    ?evt.obj_key,
                    ?evt.cursor,
                    ?evt.part_key,
                    "subscription Removed event",
                );
                {
                    // remove the part from any active sync jobs on the object
                    let advances =
                        self.jobs
                            .drop_stream(part_key, obj_key, before_cursor, |waiter| {
                                if waiter.lanes.contains(&JobLanes::Membership)
                                    && waiter.streams.len() == 1
                                {
                                    // The already-queued membership mutation still has to finish
                                    // before its cursor can advance, but the later removal makes
                                    // fetching the object's contents unnecessary.
                                    StreamDrop::Retain {
                                        keep: vec![JobLanes::Membership],
                                    }
                                } else {
                                    StreamDrop::Release
                                }
                            });
                    self.emit_advances(advances, out);
                }
                let parts =
                    self.jobs
                        .add_job(evt.obj_key.clone(), evt.cursor, [evt.part_key.clone()]);
                if parts.len() > 0 {
                    out.push(CursorMachineCommand::RemoveObjFromParts {
                        cursor: evt.cursor,
                        obj_key: evt.obj_key,
                        part_key: evt.part_key,
                    });
                }
            }
        }
    }

    pub fn on_obj_sync_job_evt(
        &mut self,
        obj_key: ObjKey,
        cursor: CursorIndex,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        // create the object replay even if no object replays were
        // requested to allow continuing in case it's added
        // FIXME: this is questionable, cursor machine grows per object forever
        let replay = self.object_replays.entry(obj_key.clone()).or_default();
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
        let advances = self
            .jobs
            .settle_on_cursor(&obj_key, &cursor, &JobLanes::Sync);
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
        for (part_key, reached) in advances {
            let Some(cursor) = reached else {
                continue;
            };
            out.push(CursorMachineCommand::SetPartCursor {
                part_key: part_key.clone(),
                cursor,
            });
            if self.jobs.is_settled(&part_key) {
                out.push(CursorMachineCommand::PartIdle { part_key });
            }
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
    fn touched(cursor: CursorIndex, obj_key: &ObjKey, parts: &[PartKey]) -> CursorEvent {
        CursorEvent::Changed(ObjChanged {
            cursor,
            part_keys: parts.to_vec(),
            obj_key: obj_key.clone(),
            payload: serde_json::json!({ "k": cursor }),
        })
    }

    /// A touch that names no part cursor: the object is the target, so its own
    /// cursor is the only thing that can order the replay.
    fn touched_obj_only(cursor: CursorIndex, obj_key: &ObjKey) -> CursorEvent {
        CursorEvent::Changed(ObjChanged {
            cursor,
            part_keys: Vec::new(),
            obj_key: obj_key.clone(),
            payload: serde_json::json!({ "k": cursor }),
        })
    }

    fn removed(cursor: CursorIndex, obj_key: &ObjKey, part_key: &PartKey) -> CursorEvent {
        CursorEvent::Removed(ObjRemovedFromPart {
            cursor,
            part_key: part_key.clone(),
            obj_key: obj_key.clone(),
        })
    }

    fn sync_obj(
        obj_key: &ObjKey,
        cursor: CursorIndex,
        parts: Vec<PartKey>,
    ) -> CursorMachineCommand {
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

    fn settle(
        machine: &mut CursorSyncMachine,
        obj_key: &ObjKey,
        cursor: CursorIndex,
        kind: JobLanes,
    ) -> Vec<CursorMachineCommand> {
        let mut out = Vec::new();
        machine.on_obj_sync_job_evt(obj_key.clone(), cursor, kind, &mut out);
        out
    }

    fn set_cursor(part_key: &PartKey, cursor: CursorIndex) -> CursorMachineCommand {
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

    fn removal_command(
        obj_key: &ObjKey,
        part_key: &PartKey,
        cursor: CursorIndex,
    ) -> CursorMachineCommand {
        CursorMachineCommand::RemoveObjFromParts {
            obj_key: obj_key.clone(),
            part_key: part_key.clone(),
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
            settle(&mut machine, &o, 5, JobLanes::Sync),
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
            settle(&mut machine, &o, 3, JobLanes::Sync),
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
            settle(&mut machine, &o, 7, JobLanes::Sync),
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
            settle(&mut machine, &o, 5, JobLanes::Sync),
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
            settle(&mut machine, &o, 6, JobLanes::Sync),
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
            settle(&mut machine, &o, 7, JobLanes::Membership).contains(&set_cursor(&p, 7)),
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
            settle(&mut machine, &o, 6, JobLanes::Sync),
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
            settle(&mut machine, &o, 7, JobLanes::Sync),
            Vec::<CursorMachineCommand>::new()
        );

        assert_eq!(
            machine.obj_resume_cursor(&o),
            7,
            "the acknowledged replay is what the object route resumes from"
        );
        assert!(
            machine.owes_obj_job_lane(&o, 7, JobLanes::Membership),
            "the removal's membership lane is not the sync completion's to settle"
        );

        // The membership completion still finishes its own lane, and the part only
        // then advances to the shared cursor.
        assert_eq!(
            settle(&mut machine, &o, 7, JobLanes::Membership),
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
            settle(&mut machine, &o, 7, JobLanes::Sync),
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
            settle(&mut machine, &o, 7, JobLanes::Sync),
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
            settle(&mut machine, &o, 5, JobLanes::Sync),
            vec![set_cursor(&p, 5), idle(&p)]
        );

        assert_eq!(
            feed(&mut machine, removed(6, &o, &p)),
            vec![removal_command(&o, &p, 6)],
            "no cursor advances for the removal until its membership write is done"
        );
        assert_eq!(
            settle(&mut machine, &o, 6, JobLanes::Membership).first(),
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
        let after = settle(&mut machine, &o, 8, JobLanes::Membership);
        assert!(
            after.contains(&set_cursor(&a, 8)),
            "the removed part's own cursor still advances: {after:?}"
        );
        assert!(
            !after.iter().any(|cmd| matches!(
                cmd,
                CursorMachineCommand::SetPartCursor { part_key, .. } if *part_key == b
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
                CursorMachineCommand::SetPartCursor { part_key, .. } if *part_key == p
            )),
            "the surviving part is still gated: {superseded:?}"
        );

        let after_sync = settle(&mut machine, &o, 5, JobLanes::Sync);
        assert!(
            !after_sync.iter().any(|cmd| matches!(
                cmd,
                CursorMachineCommand::SetPartCursor { part_key, .. } if *part_key == p
            )),
            "the membership half is still owed, so the surviving part cannot advance: {after_sync:?}"
        );
        let after_membership = settle(&mut machine, &o, 5, JobLanes::Membership);
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
            settle(&mut machine, &o, 5, JobLanes::Sync),
            vec![set_cursor(&p, 5)],
            "the part is not idle while a later cursor is still outstanding"
        );
        assert_eq!(
            settle(&mut machine, &o, 9, JobLanes::Sync),
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
        assert_eq!(settle(&mut machine, &o, 42, JobLanes::Sync), vec![]);
    }

    /// A completion for a lane the job never owed is a programming error, not a silent no-op:
    /// accepting it would advance a cursor whose work never ran.
    #[test]
    #[should_panic]
    fn a_wrong_lane_completion_panics() {
        let (o, p) = (obj(23), part(24));
        let mut machine = CursorSyncMachine::default();
        feed(&mut machine, touched(5, &o, &[p]));
        settle(&mut machine, &o, 5, JobLanes::Membership);
    }

    /// `remove_part` retires the part's bookkeeping silently: the part is gone, so there is no
    /// cursor left to advance and no idle to report.
    #[test]
    fn remove_part_retires_silently() {
        let (o, p) = (obj(25), part(26));
        let mut machine = CursorSyncMachine::default();
        feed(&mut machine, touched(5, &o, std::slice::from_ref(&p)));
        machine.remove_part(p.clone());
        assert_eq!(settle(&mut machine, &o, 5, JobLanes::Sync), vec![]);
    }
}
