//! TODO: emit sync stats event for use in full.rs
//! FIXME: figure out the delete obj story

use crate::interlude::*;

use crate::part_store::{CursorIndex, ObjPayload};

use std::collections::{BTreeMap, HashMap};

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CursorMachineCommand {
        SyncObj {
            obj_id: ObjId,
            remote_payload: ObjPayload,
            cursor: CursorIndex,
            /// Upsert the object at the sync backend to these
            /// parts
            parts: Vec<PartId>,
        },
        SetPartCursor {
            part_id: PartId,
            cursor: CursorIndex
        },
        AddObjToPart {
            obj_id: ObjId,
            part_id: PartId,
            cursor: CursorIndex,
        },
        RemoveObjFromPart {
            obj_id: ObjId,
            part_id: PartId,
            cursor: CursorIndex,
        },
        PartIdle {
            part_id: PartId,
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
    cursor_state: HashMap<
        PartId,
        struct CursorStreamState {
                #![derive(Debug, Default)]
                last_emitted_cursor: Option<CursorIndex>,
                slots: BTreeMap<
                    CursorIndex,
                    enum CursorSlotState {
                        #![derive(Debug, Clone, Copy, PartialEq, Eq)]
                        Pending,
                        Ready,
                    }
                >,
            }
        >,
        active_obj_jobs: BTreeMap<
            ObjId,

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
                        pub parts: Vec<PartId>,
                        pub pending_membership: bool,
                        pub pending_sync: bool,
                    }
                >,
                // removed_from_parts: Set<PartId>,
            }

        >,
    }
}

impl CursorSyncMachine {
    fn mark_pending_cursor(&mut self, part_id: PartId, cursor: CursorIndex) -> bool {
        let state = self.cursor_state.entry(part_id).or_default();
        if cursor <= state.last_emitted_cursor.unwrap_or_default() {
            panic!(
                "cursority trap: cursor ({cursor}) seen below floor ({:?})",
                state.last_emitted_cursor
            );
        }
        if let Some(_old) = state.slots.get_mut(&cursor) {
            // duplicate cursor
            return false;
        }
        state.slots.insert(cursor, CursorSlotState::Pending);
        true
    }
    pub fn on_subscription_evt(
        &mut self,
        evt: crate::rpc::SubEvent,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        use crate::rpc::*;

        match evt {
            SubEvent::Changed(evt) => {
                let mut parts = vec![];
                for &part_id in &evt.part_ids {
                    if !self.mark_pending_cursor(part_id, evt.cursor) {
                        continue;
                    }
                    parts.push(part_id);
                }
                if parts.is_empty() {
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
                if !self.mark_pending_cursor(evt.part_id, evt.cursor) {
                    return;
                }
                let job = self.active_obj_jobs.entry(evt.obj_id).or_default();
                let waiter = job.waiters.entry(evt.cursor).or_default();
                waiter.parts.push(evt.part_id);
                if let Some(payload) = evt.payload {
                    waiter.pending_membership = true;
                    waiter.pending_sync = true;
                    out.push(CursorMachineCommand::AddObjToPart {
                        cursor: evt.cursor,
                        obj_id: evt.obj_id,
                        part_id: evt.part_id,
                    });
                    out.push(CursorMachineCommand::SyncObj {
                        cursor: evt.cursor,
                        obj_id: evt.obj_id,
                        remote_payload: payload,
                        parts: vec![evt.part_id],
                    });
                } else {
                    waiter.pending_membership = true;
                    out.push(CursorMachineCommand::AddObjToPart {
                        cursor: evt.cursor,
                        obj_id: evt.obj_id,
                        part_id: evt.part_id,
                    });
                }
            }
            SubEvent::Removed(evt) => {
                if !self.mark_pending_cursor(evt.part_id, evt.cursor) {
                    return;
                }
                let job = self.active_obj_jobs.entry(evt.obj_id).or_default();
                let waiter = job.waiters.entry(evt.cursor).or_default();
                waiter.parts.push(evt.part_id);
                waiter.pending_membership = true;
                out.push(CursorMachineCommand::RemoveObjFromPart {
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
        obj_id: ObjId,
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
            let state = self.cursor_state.entry(part_id).or_default();
            state.slots.insert(cursor, CursorSlotState::Ready);
            self.drain_ready_cursor_advances(part_id, out);
        }
        if !job.waiters.is_empty() {
            self.active_obj_jobs.insert(obj_id, job);
        }
    }

    fn drain_ready_cursor_advances(
        &mut self,
        part_id: PartId,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        let state = self.cursor_state.entry(part_id).or_default();
        // calculate Ready highmark
        let latest_ready = {
            let mut latest_ready = None;
            for (cursor, slot) in state.slots.range(..) {
                match slot {
                    CursorSlotState::Ready => latest_ready = Some(*cursor),
                    CursorSlotState::Pending => break,
                }
            }
            latest_ready
        };

        let Some(cursor) = latest_ready else {
            return;
        };

        // update sync store
        out.push(CursorMachineCommand::SetPartCursor { part_id, cursor });
        while state
            .slots
            .first_key_value()
            .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
        {
            state.slots.pop_first();
        }
        state.last_emitted_cursor = Some(cursor);
        if state.slots.is_empty() {
            out.push(CursorMachineCommand::PartIdle { part_id });
        }
    }
}
