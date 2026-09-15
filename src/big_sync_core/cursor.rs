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
        let advances = self.jobs.drop_stream(part_id, obj_id, before_cursor, |waiter| {
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
            SubEvent::Added(evt) => {
                tracing::trace!(
                    ?evt.obj_id,
                    ?evt.cursor,
                    ?evt.part_id,
                    payload = !evt.payload.is_null(),
                    "subscription Added event",
                );
                if !self.mark_pending_cursor(&evt.part_id, evt.cursor) {
                    return;
                }
                self.track_obj_job(
                    evt.obj_id.clone(),
                    evt.cursor,
                    [evt.part_id.clone()],
                    CursorJobCompletionKind::Sync,
                );
                out.push(CursorMachineCommand::SyncObj {
                    cursor: evt.cursor,
                    obj_id: evt.obj_id,
                    remote_payload: evt.payload,
                    parts: vec![evt.part_id],
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
                self.supersede_obj_part(
                    evt.obj_id.clone(),
                    evt.part_id.clone(),
                    evt.cursor,
                    out,
                );
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
            out.push(CursorMachineCommand::SetPartCursor { part_id: part_id.clone(), cursor });
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
