//! TODO: emit sync stats event for use in full.rs

use crate::{interlude::*, part_store::PartitionStore};

use crate::part_store::CursorIndex;
use crate::rpc::SubStreamKind;

use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectSyncKind {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectSyncKey {
    pub peer: PeerId,
    pub kind: ObjectSyncKind,
    pub obj_id: ObjId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ObjectJobId {
    peer: PeerId,
    obj_id: ObjId,
}

#[derive(Debug, Clone)]
pub struct CursorWaiter {
    pub peer: PeerId,
    pub part_id: PartId,
    pub sync_kind: ObjectSyncKind,
    pub stream_event: SubStreamKind,
}

/// Look at [`on_obj_sync_completed`] impl for how this
/// actually works in more detail.
///
/// [`on_obj_sync_completed`]: SyncMachine::on_obj_sync_completed
#[derive(Debug, Clone)]
struct ObjectJobState {
    /// Since a obj can be a member of multiple partitions
    /// from a single peer and be involved in multiple
    /// events (consquetive changes) we, have these events
    /// wait on the same job to dedpe work.
    waiters: BTreeMap<CursorIndex, CursorWaiter>,
    /// Since the job represents a waiting state for multiple
    /// cursor events, we must ensure to tend to the last one.
    ///
    /// This is only ever Delete if the obj is deleted from all
    /// local partitions.
    last_change_kind: ObjectSyncKind,
    /// This is true if no new cursor waiters
    /// have been added since we last dispatched
    /// a command for this job.
    ///
    /// We must dispatch a command again for an obj
    /// if a new change comes in for it while we're
    /// waiting for a previous sync command to complete.
    /// We only advance the waiting cursors as we detect
    /// this rest state.
    dirty: bool,
    /// Any cursors below these can be assumed
    /// to be processsed
    high_water_at_last_dispatch: CursorIndex,
}

// FIXME: part_ids are provided to commands
// only as hints as obj ids across partitions are
// supposed to represent a single entity.
structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CursorMachineCommand {
        pub peer_id: PeerId,
        pub kind: ObjectSyncKind,
        pub obj_id: ObjId,
        pub part_hints: Vec<PartId>,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncCompletion {
    AddedMember {
        peer: PeerId,
        obj_id: ObjId,
        obj_payload: serde_json::Value,
    },
    /// NOTE: changed obj doesn't carry payloads.
    /// Local systems are expected to persist the payload separately
    /// through the obj store before or alongside membership updates.
    ChangedObject {
        peer: PeerId,
        obj_id: ObjId,
    },
    DeletedMember {
        peer: PeerId,
        obj_id: ObjId,
    },
    Noop {
        peer: PeerId,
        obj_id: ObjId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorSlotState {
    Pending,
    Ready,
}

#[derive(Debug, Default)]
struct CursorStreamState {
    // FIXME: last_emitted_cursor and persisted_cursor are identical
    persisted_cursor: Option<CursorIndex>,
    last_emitted_cursor: Option<CursorIndex>,
    slots: BTreeMap<CursorIndex, CursorSlotState>,
}

impl CursorStreamState {
    fn floor(&self) -> CursorIndex {
        self.persisted_cursor
            .unwrap_or(0)
            .max(self.last_emitted_cursor.unwrap_or(0))
    }
}

#[derive(Debug, Default)]
pub struct CursorSyncMachine {
    cursor_state: HashMap<PeerId, HashMap<PartId, PartitionCursorState>>,
    active_obj_jobs: BTreeMap<ObjectJobId, ObjectJobState>,
}

#[derive(Debug, Default)]
struct PartitionCursorState {
    member: CursorStreamState,
    obj: CursorStreamState,
}

impl CursorSyncMachine {
    pub fn new() -> Self {
        Self {
            cursor_state: HashMap::new(),
            active_obj_jobs: BTreeMap::new(),
        }
    }

    pub fn clear_peer(&mut self, peer: PeerId) {
        self.cursor_state.remove(&peer);
        self.active_obj_jobs.retain(|job_id, _| job_id.peer != peer);
    }

    pub async fn on_subscription_evt<K: FutureForm, S: PartitionStore<K>>(
        &mut self,
        peer: PeerId,
        evt: crate::rpc::SubEvent,
        part_store: &S,
        out: &mut Vec<CursorMachineCommand>,
    ) {
        use crate::rpc::*;

        let (sync_kind, obj_id, part_id, cursor, sub_kind) = match evt {
            SubEvent::ReplayComplete { .. } => return,
            SubEvent::MemberEvent(event) => {
                let (kind, obj_id) = match event.deets {
                    PartitionMemberEventDeets::MemberUpsert(obj_id) => {
                        (ObjectSyncKind::New, obj_id)
                    }
                    PartitionMemberEventDeets::MemberRemove(obj_id) => {
                        (ObjectSyncKind::Delete, obj_id)
                    }
                };
                (
                    kind,
                    obj_id,
                    event.part_id,
                    event.cursor,
                    SubStreamKind::Member,
                )
            }
            SubEvent::ObjChangeEvent(event) => (
                ObjectSyncKind::Change,
                event.obj_id,
                event.part_id,
                event.cursor,
                SubStreamKind::Objects,
            ),
        };
        // clone self field AOT to avoid stream_state_mut mutable borrow
        // leading to borrow issues
        let state = self.stream_state_mut(peer, part_id, sub_kind);
        if cursor <= state.floor() {
            panic!(
                "cursority trap: cursor ({cursor}) seen below floor ({})",
                state.floor()
            );
        }
        if matches!(sync_kind, ObjectSyncKind::Delete) {
            let obj_partition_count = part_store.obj_parts(obj_id).await.len();
            if obj_partition_count > 1 {
                part_store.remove_obj_from_part(obj_id, part_id).await;
                let old = state.slots.insert(cursor, CursorSlotState::Ready);
                assert!(old.is_none(), "fishy");
                self.drain_ready_cursor_advances(peer, part_id, part_store)
                    .await;
                // if obj is still in other partitons, no need for a delete command
                return;
            }
        }
        let old = state.slots.insert(cursor, CursorSlotState::Pending);
        assert!(old.is_none(), "fishy");

        let key = ObjectSyncKey {
            peer,
            kind: sync_kind,
            obj_id,
        };
        let job_id = ObjectJobId { peer, obj_id };
        let entry = self
            .active_obj_jobs
            .entry(job_id)
            .or_insert_with(|| ObjectJobState {
                dirty: false,
                high_water_at_last_dispatch: cursor,
                last_change_kind: key.kind,
                waiters: default(),
            });
        if !entry.waiters.is_empty() {
            entry.dirty = true;
            assert!(entry.high_water_at_last_dispatch < cursor, "fishy");
            entry.high_water_at_last_dispatch = cursor;
        }
        match (entry.last_change_kind, sync_kind) {
            // obj added to new partition
            (ObjectSyncKind::Change, ObjectSyncKind::New)
            | (ObjectSyncKind::New, ObjectSyncKind::New)
            // obj changed
            | (ObjectSyncKind::New, ObjectSyncKind::Change)
            | (ObjectSyncKind::Change, ObjectSyncKind::Change)
            // obj deleted
            |(ObjectSyncKind::Change, ObjectSyncKind::Delete)
            | (ObjectSyncKind::Delete, ObjectSyncKind::Delete)
            | (ObjectSyncKind::New, ObjectSyncKind::Delete) => {}
            // FIXME: I'm not sure how well we've modeled deletes
            // in one partitions
            (ObjectSyncKind::Delete, ObjectSyncKind::New)
            | (ObjectSyncKind::Delete, ObjectSyncKind::Change)
            => panic!("curiosity trap: event for deleted obj"),
        }
        entry.waiters.insert(
            cursor,
            CursorWaiter {
                peer,
                part_id,
                sync_kind,
                stream_event: sub_kind,
            },
        );
        let part_hints = entry.waiters.values().map(|ww| ww.part_id).collect();
        // FIXME: consider waiting until next dispatch for new commands
        out.push(CursorMachineCommand {
            peer_id: key.peer,
            obj_id: key.obj_id,
            part_hints,
            kind: sync_kind,
        });
    }

    pub async fn on_obj_sync_completed<K: FutureForm, S: PartitionStore<K>>(
        &mut self,
        completion: SyncCompletion,
        part_store: &S,
        out: &mut Vec<CursorMachineCommand>,
    ) -> Res<()> {
        let job_id = match &completion {
            SyncCompletion::AddedMember { peer, obj_id, .. }
            | SyncCompletion::ChangedObject { peer, obj_id }
            | SyncCompletion::DeletedMember { peer, obj_id }
            | SyncCompletion::Noop { peer, obj_id } => ObjectJobId {
                peer: *peer,
                obj_id: *obj_id,
            },
        };
        let Some(job) = self.active_obj_jobs.get_mut(&job_id) else {
            return Ok(());
        };
        let next_job_kind = match (&job.last_change_kind, &completion) {
            (ObjectSyncKind::New, SyncCompletion::AddedMember { .. })
            | (ObjectSyncKind::Change, SyncCompletion::AddedMember { .. })
            | (ObjectSyncKind::New, SyncCompletion::Noop { .. })
            | (ObjectSyncKind::New, SyncCompletion::ChangedObject { .. })
            | (ObjectSyncKind::Change, SyncCompletion::Noop { .. })
            | (ObjectSyncKind::Change, SyncCompletion::ChangedObject { .. }) => ObjectSyncKind::Change,
            (ObjectSyncKind::Change, SyncCompletion::DeletedMember { .. })
            | (ObjectSyncKind::New, SyncCompletion::DeletedMember { .. }) => ObjectSyncKind::New,
            (ObjectSyncKind::Delete, SyncCompletion::AddedMember { .. })
            // NOTE: we still enqueue a delete on previous delete in case
            // there was a transient Added flip
            | (ObjectSyncKind::Delete, SyncCompletion::DeletedMember { .. })
            | (ObjectSyncKind::Delete, SyncCompletion::ChangedObject { .. })
            | (ObjectSyncKind::Delete, SyncCompletion::Noop { .. }) => ObjectSyncKind::Delete,
        };
        let should_requeue_current = job.dirty
            || matches!(
                (&job.last_change_kind, &completion),
                (ObjectSyncKind::New, SyncCompletion::Noop { .. })
            );
        if should_requeue_current {
            job.last_change_kind = next_job_kind;
            let partition_hints = job.waiters.values().map(|ww| ww.part_id).collect();
            out.push(CursorMachineCommand {
                peer_id: job_id.peer,
                obj_id: job_id.obj_id,
                kind: next_job_kind,
                part_hints: partition_hints,
            });
            if job.dirty {
                job.dirty = false;
                return Ok(());
            }
        }
        // remove it temporarily to allow mutable borrows
        // on self
        let Some(mut job) = self.active_obj_jobs.remove(&job_id) else {
            return Ok(());
        };

        let mut per_partition_cursors = HashMap::new();
        for (&ii, waiter) in job.waiters.iter() {
            if ii > job.high_water_at_last_dispatch {
                continue;
            }
            if !per_partition_cursors.contains_key(&waiter.part_id) {
                per_partition_cursors.insert(waiter.part_id, vec![]);
            }
            let part_cursors = per_partition_cursors
                .get_mut(&waiter.part_id)
                .expect(ERROR_IMPOSSIBLE);
            part_cursors.push(ii);
        }

        for (part_id, cursors) in per_partition_cursors {
            debug_assert!(cursors.is_sorted());
            for &ii in &cursors {
                let waiter = job.waiters.remove(&ii).expect(ERROR_IMPOSSIBLE);
                let state = self.stream_state_mut(waiter.peer, part_id, waiter.stream_event);
                // mark slot ready
                state.slots.insert(ii, CursorSlotState::Ready);
            }

            match &completion {
                SyncCompletion::AddedMember { obj_payload, .. } => {
                    part_store
                        .upsert_obj(job_id.obj_id, obj_payload, &[part_id])
                        .await;
                }
                SyncCompletion::DeletedMember { .. } => {
                    part_store
                        .remove_obj_from_part(job_id.obj_id, part_id)
                        .await;
                }
                SyncCompletion::ChangedObject { .. } | SyncCompletion::Noop { .. } => {}
            }
            self.drain_ready_cursor_advances(job_id.peer, part_id, part_store)
                .await;
        }

        // emit sync requests on any other peers that
        // have job on obj
        for (ii_id, ii_job) in &mut self.active_obj_jobs {
            if ii_id.obj_id != job_id.obj_id {
                continue;
            }
            if ii_job.last_change_kind != ObjectSyncKind::New {
                continue;
            }
            if job_id.peer == ii_id.peer {
                continue;
            }
            ii_job.last_change_kind = ObjectSyncKind::Change;
            // FIXME: consider not setting it dirty
            ii_job.dirty = true;
            let part_ids = ii_job.waiters.values().map(|ww| ww.part_id).collect();
            out.push(CursorMachineCommand {
                part_hints: part_ids,
                peer_id: ii_id.peer,
                obj_id: ii_id.obj_id,
                kind: ObjectSyncKind::Change,
            });
        }

        if !job.waiters.is_empty() {
            self.active_obj_jobs.insert(job_id, job);
        }

        Ok(())
    }

    async fn drain_ready_cursor_advances<K: FutureForm, S: PartitionStore<K>>(
        &mut self,
        peer: PeerId,
        part_id: PartId,
        part_store: &S,
    ) {
        let state = {
            if !self.cursor_state.contains_key(&peer) {
                self.cursor_state.insert(peer, default());
            }
            let state = self.cursor_state.get_mut(&peer).expect(ERROR_IMPOSSIBLE);
            if !state.contains_key(&part_id) {
                state.insert(part_id, default());
            }
            state.get_mut(&part_id).expect(ERROR_IMPOSSIBLE)
        };
        // calculate Ready highmark
        let (member_cursor, obj_cursor) = {
            let member_cursor = {
                let mut latest_ready = None;
                let floor = state.member.floor();
                for (cursor, slot) in state.member.slots.range(floor.saturating_add(1)..) {
                    match slot {
                        CursorSlotState::Ready => latest_ready = Some(*cursor),
                        CursorSlotState::Pending => break,
                    }
                }
                latest_ready
            };
            let obj_cursor = {
                let mut latest_ready = None;
                let floor = state.obj.floor();
                for (cursor, slot) in state.obj.slots.range(floor.saturating_add(1)..) {
                    match slot {
                        CursorSlotState::Ready => latest_ready = Some(*cursor),
                        CursorSlotState::Pending => break,
                    }
                }
                latest_ready
            };
            (member_cursor, obj_cursor)
        };

        if let (None, None) = (member_cursor, obj_cursor) {
            return;
        }

        // update sync store
        // FIXME: get rid of the get_ call and have the part store impl do
        // increment only increases on set removing one fetch call in the
        // impl
        let existing = part_store.get_peer_part_cursor(peer, part_id).await;
        let next_member_cursor = member_cursor.unwrap_or(existing.member_cursor);
        let next_obj_cursor = obj_cursor.unwrap_or(existing.obj_cursor);
        part_store
            .set_peer_part_cursor(
                peer,
                part_id,
                crate::part_store::PeerPartCursors {
                    member_cursor: next_member_cursor,
                    obj_cursor: next_obj_cursor,
                },
            )
            .await;

        // remove cursor slots and update state
        if let Some(cursor) = member_cursor {
            while state
                .member
                .slots
                .first_key_value()
                .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
            {
                state.member.slots.pop_first();
            }
            state.member.last_emitted_cursor = Some(cursor);
            state.member.persisted_cursor = Some(cursor);
        }
        if let Some(cursor) = obj_cursor {
            while state
                .obj
                .slots
                .first_key_value()
                .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
            {
                state.obj.slots.pop_first();
            }
            state.obj.last_emitted_cursor = Some(cursor);
            state.obj.persisted_cursor = Some(cursor);
        }
    }

    fn stream_state_mut(
        &mut self,
        peer: PeerId,
        part_id: PartId,
        stream: SubStreamKind,
    ) -> &mut CursorStreamState {
        if !self.cursor_state.contains_key(&peer) {
            self.cursor_state.insert(peer, default());
        }
        let state = self.cursor_state.get_mut(&peer).expect(ERROR_IMPOSSIBLE);
        if !state.contains_key(&part_id) {
            state.insert(part_id, default());
        }
        let partition = state.get_mut(&part_id).expect(ERROR_IMPOSSIBLE);
        match stream {
            SubStreamKind::Member => &mut partition.member,
            SubStreamKind::Objects => &mut partition.obj,
        }
    }
}
