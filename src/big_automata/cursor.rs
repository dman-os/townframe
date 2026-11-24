//! The cursor machine: per-part applied cursors for one peer's cursor-strategy parts.
//!
//! Every event the machine sees names parts (an object route's events never reach it: the
//! parent hands those to `ObjectJobs` and keeps the object route's resume cursor itself). A
//! membership touch opens a `JobKey::Sync(obj)` slot at its cursor on each named part; a
//! removal opens a `JobKey::RemoveFromPart(obj)` slot on its part. `ObjectJobs` reports each
//! settled `(JobKey, cursor)` back through [`CursorSyncMachine::on_obj_sync_job_evt`], and a
//! part's cursor passes a slot once the job holding it settled.
//!
//! # Laws
//!
//! 1. **A part's cursor never passes unsettled work.** `SetPartCursor { part, cursor }` is
//!    emitted only when every slot the part holds at or below `cursor` has settled (or been
//!    released by law 3), and `cursor` is the highest such slot.
//! 2. **Part cursors only go forward.** The cursors emitted for one part strictly increase
//!    between its creation and its [`remove_part`](CursorSyncMachine::remove_part).
//! 3. **A removal releases earlier syncs on its part only.** A removal of an object at `c` on
//!    part `p` frees `p`'s slots of the object's syncs below `c`. The same syncs keep gating
//!    the object's other parts, other objects' syncs keep gating `p`, and the removal's own
//!    slot gates `p` until its completion is reported.
//! 4. **A part admits a cursor once.** A part that already emitted past a cursor, or holds a
//!    slot at it (a re-delivery), skips it: the `SyncObj` names only the parts that admitted
//!    it, in event order, and an event every part skipped emits nothing.
//! 5. **Retiring a part is local.** `remove_part` drops the part's slots and replay cursor
//!    and reports nothing; completions reported afterwards are silent for it and still settle
//!    the job's other parts, so no other part stalls on a retired one. A part added again
//!    starts fresh.
//! 6. **`is_settled(part)` is true exactly when the part owes nothing**: it holds no slot
//!    that has not passed. A part the machine has never heard of owes nothing.
//! 7. **Completions are idempotent.** A completion for a job or cursor with no waiter (stale,
//!    duplicated, released by law 3, or for a retired part) changes nothing.
//! 8. **Completions commute.** The cursors a part has reached depend on which slots have
//!    settled, not on the order the completions arrived in.
//! 9. **Replay cursors are scheduling state.** `part_replay_cursor` is the larger of the
//!    applied cursor and the newest scanned one, and `advance_part_replay_cursor` only moves
//!    it forward.
//!
//! Contract with the layer above: every emitted `SyncObj` and `RemoveObjFromParts` comes back
//! through [`CursorSyncMachine::on_obj_sync_job_evt`], or its part is retired with
//! [`CursorSyncMachine::remove_part`]. Work cancelled because newer work covers it is still
//! reported for each cursor it held; a slot nobody reports stalls its part.

use crate::interlude::*;

use crate::part_store::ObjPayload;
use crate::rev::Rev;
use crate::watermark::WatermarkMachine;

strike! {
    pub enum CursorEvent {
        ObjChanged(pub struct {
            pub cursor: Rev,
            /// Never empty: an event naming no part is an object route event, which does not
            /// reach this machine.
            pub parts: Vec<PartKey>,
            pub obj_key: ObjKey,
            pub payload: ObjPayload,
        }),
        ObjRemovedFromPart(pub struct {
            pub cursor: Rev,
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
            cursor: Rev,
            /// Upsert the object at the sync backend to these
            /// parts
            parts: Vec<PartKey>,
        },
        SetPartCursor {
            part_key: PartKey,
            cursor: Rev
        },
        RemoveObjFromParts {
            obj_key: ObjKey,
            part_key: PartKey,
            cursor: Rev,
        },
    }
}

/// The jobs the cursor machine hands out, and the key their completion comes back under.
///
/// A sync is cross part: one sync of an object waits on every part the event named. A
/// removal is per part: its waiter holds the parts it removes the object from. They are
/// different jobs, so a removal superseding an object's sync on a part never touches the
/// removal's own slot, and settling one never settles the other.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JobKey {
    Sync(ObjKey),
    RemoveFromPart(ObjKey),
}

#[derive(Debug, Default)]
pub struct CursorSyncMachine {
    /// The newest cursor scanned for each part route. This is a scheduling cursor,
    /// distinct from the applied watermark owned by `jobs`.
    part_replay_cursors: Map<PartKey, Rev>,
    jobs: WatermarkMachine<JobKey, PartKey, Rev>,
}

impl CursorSyncMachine {
    pub(crate) fn remove_part(&mut self, part_key: PartKey) {
        // Drops the part's slot book and every waiter that gated only it.
        self.jobs.retire_stream(&part_key);
        self.part_replay_cursors.remove(&part_key);
    }

    /// Whether the part owes nothing: every slot it holds has passed.
    pub(crate) fn is_settled(&self, part_key: &PartKey) -> bool {
        self.jobs.is_settled(part_key)
    }

    pub(crate) fn part_replay_cursor(&self, part_key: &PartKey, applied: Rev) -> Rev {
        self.part_replay_cursors
            .get(part_key)
            .copied()
            .unwrap_or(applied)
            .max(applied)
    }

    pub(crate) fn advance_part_replay_cursor(&mut self, part_key: PartKey, cursor: Rev) {
        let entry = self.part_replay_cursors.entry(part_key).or_default();
        *entry = (*entry).max(cursor);
    }

    /// Whether the job still waits on `cursor`. A completion can ask this first: a removal
    /// releases an object's earlier syncs on its part while the sync task stays in flight.
    pub(crate) fn owes_obj_job(&self, job: &JobKey, cursor: Rev) -> bool {
        self.jobs.owes(job, &cursor)
    }

    pub fn on_subscription_evt(&mut self, evt: CursorEvent, out: &mut Vec<CursorMachineCommand>) {
        match evt {
            CursorEvent::ObjChanged(evt) => {
                assert!(
                    !evt.parts.is_empty(),
                    "an event naming no part is an object route event and belongs to ObjectJobs"
                );
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
                Self::emit_advances([(evt.part_key.clone(), reached)], out);
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
        cursor: Rev,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        let advances = self.jobs.settle_on_cursor(&job, &cursor);
        Self::emit_advances(advances, out);
    }

    /// Emit the part-cursor commands for streams that just reached a new watermark.
    fn emit_advances(
        advances: impl IntoIterator<Item = (PartKey, Option<Rev>)>,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        for (part_key, reached) in advances {
            if let Some(cursor) = reached {
                out.push(CursorMachineCommand::SetPartCursor { part_key, cursor });
            }
        }
    }
}

#[cfg(test)]
mod oracle;
#[cfg(test)]
mod tests;
