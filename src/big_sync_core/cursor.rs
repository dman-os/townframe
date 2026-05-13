//! TODO: emit sync stats event for use in full.rs
//! FIXME: figure out the delete obj story

use crate::interlude::*;

use crate::part_store::{CursorIndex, ObjPayload};
use crate::SyncJobEvt;

use std::collections::{BTreeMap, HashMap};

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CursorMachineCommand {
        SyncObj {
            obj_id: ObjId,
            remote_payload: ObjPayload,
            cursors: Vec<CursorIndex>,
            /// Upsert the object at the sync backend to these
            /// parts
            part_hints: Vec<PartId>,
        },
        SetPartCursor {
            part_id: PartId,
            cursor: CursorIndex
        },
        RemoveObjFromPart {
            obj_id: ObjId,
            part_id: PartId,
        }
    }
}

structstruck::strike! {
    #[derive(Debug, Default)]
    pub struct CursorSyncMachine {
        cursor_state: HashMap<
            PartId,
            struct CursorStreamState {
                #![derive(Debug, Default)]
                // FIXME: last_emitted_cursor and persisted_cursor are identical
                persisted_cursor: Option<CursorIndex>,
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
                        #![derive(Debug, Clone)]
                        pub part_id: PartId,
                    }
                >,
                removed_from_parts: Set<PartId>,
            }

        >,
    }
}

impl CursorStreamState {
    fn floor(&self) -> CursorIndex {
        self.persisted_cursor
            .unwrap_or(0)
            .max(self.last_emitted_cursor.unwrap_or(0))
    }
}

impl CursorSyncMachine {
    pub fn on_subscription_evt(
        &mut self,
        evt: crate::rpc::SubEvent,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        use crate::rpc::*;

        let (obj_id, part_id, cursor) = match &evt {
            SubEvent::ReplayComplete => return,
            SubEvent::Deleted(event) => (event.obj_id, event.part_id, event.cursor),
            SubEvent::Upserted(event) => (event.obj_id, event.part_id, event.cursor),
        };
        let state = self.cursor_state.entry(part_id).or_default();
        if cursor <= state.floor() {
            panic!(
                "cursority trap: cursor ({cursor}) seen below floor ({})",
                state.floor()
            );
        }
        let old = state.slots.insert(cursor, CursorSlotState::Pending);
        if let Some(old) = old {
            panic!("duplicate cursor {cursor} for part {part_id}: {old:?}");
        }

        let job = self.active_obj_jobs.entry(obj_id).or_default();
        let cmd = match evt {
            SubEvent::Upserted(evt) => {
                job.waiters.insert(cursor, CursorWaiter { part_id });
                let part_hints = job.waiters.values().map(|ww| ww.part_id).collect();
                CursorMachineCommand::SyncObj {
                    obj_id: obj_id,
                    part_hints,
                    cursors: vec![cursor],
                    remote_payload: evt.payload,
                }
            }
            SubEvent::Deleted(_) => {
                job.removed_from_parts.insert(part_id);
                state.slots.insert(cursor, CursorSlotState::Ready);
                self.drain_ready_cursor_advances(part_id, out);
                CursorMachineCommand::RemoveObjFromPart { obj_id, part_id }
            }
            SubEvent::ReplayComplete => unreachable!(),
        };
        out.push(cmd);
    }

    pub fn on_obj_sync_job_evt(&mut self, evt: &SyncJobEvt, out: &mut Vec<CursorMachineCommand>) {
        // remove it temporarily to allow mutable borrows
        // on self
        let Some(mut job) = self.active_obj_jobs.remove(&evt.obj_id) else {
            return;
        };
        let mut affected_parts = vec![];
        for &ii in &evt.cursors {
            let waiter = job.waiters.remove(&ii).expect(ERROR_UNRECONIZED);
            if job.removed_from_parts.contains(&waiter.part_id) {
                // remove from part again incase the sync job
                // re-added it
                out.push(CursorMachineCommand::RemoveObjFromPart {
                    obj_id: evt.obj_id,
                    part_id: waiter.part_id,
                });
            }
            let state = self.cursor_state.entry(waiter.part_id).or_default();
            state.slots.insert(ii, CursorSlotState::Ready);
            affected_parts.push(waiter.part_id);
        }
        for part_id in affected_parts {
            self.drain_ready_cursor_advances(part_id, out);
        }
        if !job.waiters.is_empty() {
            self.active_obj_jobs.insert(evt.obj_id, job);
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
            let floor = state.floor();
            for (cursor, slot) in state.slots.range(floor.saturating_add(1)..) {
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
        state.persisted_cursor = Some(cursor);
    }
}
