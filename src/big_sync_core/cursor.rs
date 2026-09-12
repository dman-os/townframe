//! TODO: emit sync stats event for use in full.rs
//! FIXME: figure out the delete obj story

use crate::interlude::*;

use crate::part_store::{CursorIndex, ObjPayload};
use crate::watermark::WatermarkBook;

use std::collections::{BTreeMap, HashMap};

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
    /// Per-part slot bookkeeping: which cursors are pending work and where the
    /// emitted watermark stands. This is the [`WatermarkBook`] primitive rather
    /// than a machine-local copy of it, so "what is outstanding, what has
    /// settled, when may the cursor advance" has one implementation, shared with
    /// the delta walker.
    watermarks: HashMap<PartKey, WatermarkBook<CursorIndex>>,
        active_obj_jobs: BTreeMap<
            ObjKey,

            /// Look at [`SyncMachine::on_obj_sync_completed`] impl for how this
            /// actually works in more detail.
            struct ObjectJobState {
                #![derive(Debug, Clone, Default)]
                /// Since a obj can be a member of multiple partitions
                /// from a single peer and be involved in multiple
                /// events (consquetive changes) we, have these events
                /// wait on the same job to dedpe work.
                waiters: BTreeMap<
                    CursorIndex,
                    struct CursorWaiter {
                        #![derive(Debug, Default, Clone)]
                        pub parts: Vec<PartKey>,
                        pub pending_membership: bool,
                        pub pending_sync: bool,
                    }
                >,
                // removed_from_parts: Set<PartKey>,
            }

        >,
    }
}

impl CursorSyncMachine {
    pub(crate) fn remove_part(&mut self, part_id: PartKey) {
        self.watermarks.remove(&part_id);
        self.active_obj_jobs.retain(|_, job| {
            job.waiters.retain(|_, waiter| {
                waiter.parts.retain(|candidate| *candidate != part_id);
                !waiter.parts.is_empty()
            });
            !job.waiters.is_empty()
        });
    }

    pub(crate) fn supersede_obj_part(
        &mut self,
        obj_id: ObjKey,
        part_id: PartKey,
        before_cursor: CursorIndex,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        let mut ready_cursors = Vec::new();
        let mut empty_waiters = Vec::new();
        if let Some(job) = self.active_obj_jobs.get_mut(&obj_id) {
            for (&cursor, waiter) in job.waiters.range_mut(..before_cursor) {
                if !waiter.parts.contains(&part_id) {
                    continue;
                }
                if waiter.pending_membership && waiter.parts.len() == 1 {
                    // The already-queued membership mutation still has to finish
                    // before its cursor can advance, but the later removal makes
                    // fetching the object's contents unnecessary.
                    waiter.pending_sync = false;
                    continue;
                }
                waiter.parts.retain(|candidate| *candidate != part_id);
                ready_cursors.push(cursor);
                if waiter.parts.is_empty() {
                    empty_waiters.push(cursor);
                }
            }
            for cursor in empty_waiters {
                job.waiters.remove(&cursor);
            }
            if job.waiters.is_empty() {
                self.active_obj_jobs.remove(&obj_id);
            }
        }
        for cursor in ready_cursors {
            self.watermarks
                .entry(part_id)
                .or_default()
                .force_finish(cursor);
            self.drain_ready_cursor_advances(part_id, out);
        }
    }
    fn mark_pending_cursor(&mut self, part_id: PartKey, cursor: CursorIndex) -> bool {
        // The admission guard (duplicate cursor, or at-or-below the emitted
        // watermark) lives in the primitive.
        let book = self.watermarks.entry(part_id).or_default();
        let emitted = book.watermark();
        let admitted = book.begin(cursor);
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
                for &part_id in &evt.part_ids {
                    if !self.mark_pending_cursor(part_id, evt.cursor) {
                        continue;
                    }
                    parts.push(part_id);
                }
                if parts.is_empty() {
                    let last_cursor = self.object_cursors.entry(evt.obj_id).or_default();
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
                let job = self.active_obj_jobs.entry(evt.obj_id).or_default();
                let waiter = job.waiters.entry(evt.cursor).or_default();
                waiter.parts.extend(parts.iter().copied());
                waiter.pending_sync = true;
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
                if !self.mark_pending_cursor(evt.part_id, evt.cursor) {
                    return;
                }
                let job = self.active_obj_jobs.entry(evt.obj_id).or_default();
                let waiter = job.waiters.entry(evt.cursor).or_default();
                waiter.parts.push(evt.part_id);
                waiter.pending_sync = true;
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
                if !self.mark_pending_cursor(evt.part_id, evt.cursor) {
                    return;
                }
                self.supersede_obj_part(evt.obj_id, evt.part_id, evt.cursor, out);
                let job = self.active_obj_jobs.entry(evt.obj_id).or_default();
                let waiter = job.waiters.entry(evt.cursor).or_default();
                waiter.parts.push(evt.part_id);
                waiter.pending_membership = true;
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
        let Some(mut job) = self.active_obj_jobs.remove(&obj_id) else {
            return;
        };
        let Some(waiter) = job.waiters.get_mut(&cursor) else {
            self.active_obj_jobs.insert(obj_id, job);
            return;
        };
        match kind {
            CursorJobCompletionKind::Membership => {
                if !waiter.pending_membership {
                    panic!("cursor membership completion without pending membership");
                }
                waiter.pending_membership = false;
            }
            CursorJobCompletionKind::Sync => {
                if !waiter.pending_sync {
                    panic!("cursor sync completion without pending sync");
                }
                waiter.pending_sync = false;
            }
        }
        if waiter.pending_membership || waiter.pending_sync {
            self.active_obj_jobs.insert(obj_id, job);
            return;
        }
        let waiter = job.waiters.remove(&cursor).expect(ERROR_UNRECONIZED);
        for part_id in waiter.parts {
            self.watermarks
                .entry(part_id)
                .or_default()
                .force_finish(cursor);
            self.drain_ready_cursor_advances(part_id, out);
        }
        if !job.waiters.is_empty() {
            self.active_obj_jobs.insert(obj_id, job);
        }
    }

    fn drain_ready_cursor_advances(
        &mut self,
        part_id: PartKey,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        // Advance to the contiguous prefix of finished slots; the primitive owns
        // the highmark search, the covered-slot sweep and the emitted watermark.
        let Some(cursor) = self.watermarks.entry(part_id).or_default().drain() else {
            return;
        };

        // update sync store
        out.push(CursorMachineCommand::SetPartCursor { part_id, cursor });
        if self
            .watermarks
            .get(&part_id)
            .is_none_or(WatermarkBook::is_settled)
        {
            out.push(CursorMachineCommand::PartIdle { part_id });
        }
    }
}
