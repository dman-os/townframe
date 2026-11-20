//! TODO: emit sync stats event for use in full.rs
//! FIXME: figure out the delete obj story

use crate::interlude::*;

use crate::store::{CursorIndex, ObjPayload};
use crate::watermark::WatermarkMachine;

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

/// The jobs the cursor machine hands out, and the key their completion comes back under.
///
/// A sync is cross part: one sync of an object waits on every part the event named. A
/// removal is per part: its waiter holds the parts it removes the object from. They are
/// different jobs, so a removal superseding an object's sync on a part never touches the
/// removal's own slot, and settling one never settles the other.
///
/// Contract with the layer above: every emitted `SyncObj` with parts and every
/// `RemoveObjFromParts` comes back through [`CursorSyncMachine::on_obj_sync_job_evt`], or its
/// part is retired with [`CursorSyncMachine::remove_part`]. Work cancelled because newer work
/// covers it is still reported for each cursor it held; a slot nobody reports stalls its part.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobKey {
    Sync(ObjKey),
    RemoveFromPart(ObjKey),
}

structstruck::strike! {
    #[derive(Debug, Default)]
    pub struct CursorSyncMachine {
        /// What each object-target's replay has reached for this session.
        object_replays: Map<ObjKey, struct ObjectReplay {
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
        part_replay_cursors: Map<PartKey, CursorIndex>,
        jobs: WatermarkMachine<JobKey, PartKey, CursorIndex>,
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

    /// Whether the job still waits on `cursor`. A completion can ask this first: a removal
    /// releases an object's earlier syncs on its part while the sync task stays in flight.
    pub(crate) fn owes_obj_job(&self, job: &JobKey, cursor: CursorIndex) -> bool {
        self.jobs.owes(job, &cursor)
    }

    pub fn on_subscription_evt(&mut self, evt: CursorEvent, out: &mut Vec<CursorMachineCommand>) {
        match evt {
            CursorEvent::ObjChanged(evt) => {
                if evt.parts.is_empty() {
                    let replay = self.object_replays.entry(evt.obj_key.clone()).or_default();
                    if evt.cursor <= replay.acknowledged {
                        tracing::debug!(
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
                        tracing::debug!(
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
                let added = self.jobs.add_job(
                    JobKey::Sync(evt.obj_key.clone()),
                    evt.cursor,
                    evt.parts.iter().cloned(),
                );
                if added.len() < evt.parts.len() {
                    tracing::debug!(
                        ?evt.obj_key,
                        ?evt.cursor,
                        skipped = evt.parts.len() - added.len(),
                        "cursor machine skipped parts that already saw this cursor",
                    );
                }
                // keep the event's part order
                let parts: Vec<_> = evt
                    .parts
                    .into_iter()
                    .filter(|part_key| added.contains(part_key))
                    .collect();
                if !parts.is_empty() {
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
                let added = self.jobs.add_job(
                    JobKey::RemoveFromPart(evt.obj_key.clone()),
                    evt.cursor,
                    [evt.part_key.clone()],
                );
                if added.is_empty() {
                    tracing::debug!(
                        ?evt.obj_key,
                        ?evt.cursor,
                        ?evt.part_key,
                        "cursor machine skipped a removal the part already saw",
                    );
                    return;
                }
                // The removal makes fetching the object's content for this part moot:
                // earlier syncs stop waiting on it here, and keep waiting on their other parts.
                let reached = self.jobs.release_stream(
                    &JobKey::Sync(evt.obj_key.clone()),
                    &evt.part_key,
                    &evt.cursor,
                );
                self.emit_advances([(evt.part_key.clone(), reached)], out);
                out.push(CursorMachineCommand::RemoveObjFromParts {
                    cursor: evt.cursor,
                    obj_key: evt.obj_key,
                    part_key: evt.part_key,
                });
            }
        }
    }

    pub fn on_obj_sync_job_evt(
        &mut self,
        job: JobKey,
        cursor: CursorIndex,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        if let JobKey::Sync(obj_key) = &job {
            // A sync completion is the backend acknowledging a replay of the object's
            // content, part scoped or object only alike. A removal completion is not: it
            // reports a membership mutation and must not suppress a content replay the
            // backend never saw.
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
        }
        let advances = self.jobs.settle_on_cursor(&job, &cursor);
        self.emit_advances(advances, out);
    }

    /// Emit the part-cursor commands for streams that just reached a new
    /// watermark. `PartIdle` follows `SetPartCursor` only when the part has no
    /// un-advanced cursors left.
    fn emit_advances(
        &mut self,
        advances: impl IntoIterator<Item = (PartKey, Option<CursorIndex>)>,
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
mod tests;
