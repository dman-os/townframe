//! FIXME: find a way to avoid blocking on BigSyncMachineCommands
//! FIXME: wire up reporting for UnkownParts errors
//! FIXME: the machine will break on UnkownParts actually

mod interlude {
    pub use utils_rs::prelude::*;

    pub use future_form::FutureForm;

    // FIXME: consider using indexed map instead
    pub use std::collections::{HashMap as Map, HashSet as Set};

    pub use crate::ids::{BuckId, ObjId, PartId, PeerId};
}

use std::collections::VecDeque;

use crate::interlude::*;

mod bucket;
use bucket::*;
mod cursor;
use cursor::*;
mod fingerprint;
mod ids;
use bucket::BucketMachine;
pub mod mpsc;
pub mod part_store;
use part_store::*;
pub mod rpc;
use rpc::*;
mod tasks;
use crate::tasks::leaf_buckets::*;
use crate::tasks::list_bucket::*;
use tasks::decide_peer_strat::*;
use tasks::peer_replay::*;
use tasks::*;

pub use fingerprint::{Fingerprint, FingerprintSeed};
pub use ids::{BuckId, Byte32Id, ObjId, PartId, PeerId};
#[cfg(any(test, feature = "test-support"))]
pub use tasks::TaskCounts;
pub use tasks::{MachineTask, MachineTaskMsg, SyncTask, SyncTaskDeets, TaskCtx, TaskId};

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
uniffi::custom_type!(Byte32Id, String, {
    remote,
    lower: |id| format!("{id}"),
    try_lift: |str| {
        use std::str::FromStr;
        Byte32Id::from_str(&str).map_err(|err| uniffi::deps::anyhow::anyhow!("unable to parse Byte32Id from {str:?}: {err:?}"))
    }
});
#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(PeerId, Byte32Id);
#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(PartId, Byte32Id);
#[cfg(feature = "uniffi")]
uniffi::custom_newtype!(ObjId, Byte32Id);

structstruck::strike! {
    pub enum BigSyncEvent {
        SetPeer (
            pub struct SetPeerEvent {
                pub peer_id: PeerId,
                /// Partitions to sync from the peer
                pub parts: Set<PartId>,
            }
        ),
        RemovePeer (
            pub struct RemovePeerEvent {
                pub peer_id: PeerId,
            }
        ),
        WaitForFullSync (
            pub struct WaitForFullSyncEvent {
                pub waiter_id: u64,
                pub peer_ids: Set<PeerId>,
                pub part_ids: Set<PartId>,
            }
        ),
        SyncCompleted (
            pub struct SyncCompletedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerId,
                pub completion: SyncTaskCompletion,
            }
        ),
        SyncFailed (
            pub struct SyncFailedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerId,
                pub obj_id: ObjId,
                pub err: eyre::Report,
            }
        ),
        SyncStale (
            pub struct SyncStaleEvent {
                pub task_id: TaskId,
                pub peer_id: PeerId,
                pub obj_id: ObjId,
            }
        ),
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncCompletionDeets {
        AddedMember,
        RemovedMember,
        ChangedObject,
        Noop,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncTaskCompletion {
        pub obj_id: ObjId,
        pub deets: SyncCompletionDeets,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncJobEvt {
        pub obj_id: ObjId,
        pub cursors: Set<CursorIndex>,
        pub deets: SyncCompletionDeets,
    }
}

structstruck::strike! {
    /// Commands must be done immediately blocking the machine and the next response
    /// must be the command result. Failing commands are not recoverable.
    /// This is different from tasks which can be retried are scheduled concurrently to machine.
    #[derive(Clone)]
    pub enum BigSyncMachineCommand {
        AddObjToPart {
            obj_id: ObjId,
            part_id: PartId,
        },
        RemoveObjFromPart {
            obj_id: ObjId,
            part_id: PartId,
        },
        SetPartCursor {
            peer_id: PeerId,
            part_id: PartId,
            cursor: CursorIndex
        },
    }
}
structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncStatEvent {
        ObjectSynced {
            peer_id: PeerId,
            obj_id: ObjId,
        },
        PeerPartFullySynced {
            peer_id: PeerId,
            part_id: PartId,
        },
        PeerPartStale {
            peer_id: PeerId,
            part_id: PartId,
        },
        PartFullySynced {
            part_id: PartId,
        },
        PartStale {
            part_id: PartId,
        },
        PeerFullySynced {
            peer_id: PeerId,
        },
        PeerStale {
            peer_id: PeerId,
        },
        FullSyncWaiterSatisfied {
            waiter_id: u64,
        },
    }
}

structstruck::strike! {
    struct PeerState {
        sync_workers: Map<ObjId, struct SyncWorkerState {
            task_id: TaskId,
            cursors: Set<CursorIndex>,
            part_hints: Set<PartId>,
            remote_payload: Option<crate::part_store::ObjPayload>,
        }>,
        replay_worker: Option<struct PeerReplayWorkerState {
            task_id: TaskId,
            parts: Set<PartId>,
        }>,

        cursor_machine: CursorSyncMachine,

        cursors_cmd_buf: Vec<CursorMachineCommand>,
        bucket_cmd_buf: Vec<BucketMachineCommand>,

        parts: Map<PartId, struct PeerPartState {
            strat: enum PeerPartStrategy {
                Pending(TaskId),
                Bucket(struct BucketState {
                    replay_cursor: CursorIndex,
                    machine: Box<BucketMachine>,
                    active_list_tasks: Map<TaskId, ListBucketsTask>,
                    active_leaf_tasks: Map<TaskId, LeafBucketsTask>,
                }),
                Cursor(struct CursorState {
                    replay_cursor: CursorIndex,
                }),
            }
        }>
    }
}

impl PeerState {
    fn cursors_for_peer_replay_worker_parts<'a>(
        &self,
        parts: impl std::iter::Iterator<Item = &'a PartId>,
    ) -> Map<PartId, CursorIndex> {
        parts
            .map(
                |&part_id| match self.parts.get(&part_id).expect(ERROR_IMPOSSIBLE).strat {
                    PeerPartStrategy::Pending(_) => unreachable!(),
                    PeerPartStrategy::Bucket(BucketState { replay_cursor, .. }) => {
                        (part_id, replay_cursor)
                    }
                    PeerPartStrategy::Cursor(CursorState { replay_cursor, .. }) => {
                        (part_id, replay_cursor)
                    }
                },
            )
            .collect()
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Default)]]
    struct SyncStatMachine {
        stat_evts: Vec<SyncStatEvent>,
        last_object_syncs: Map<(PeerId, PartId), (ObjId, std::time::Instant)>,
        waiters: Map<u64, struct FullSyncWaiterState {
            done_set: Set<(PeerId, PartId)>,
            need_set: Set<(PeerId, PartId)>,
        }>,
        peers: Map<PeerId, struct PeerSyncState {
            replay_phase_done: bool,
            emitted_full_synced: bool,
            parts: Map<PartId, struct PeerPartStatState {
                emitted_full_synced: bool,
                cursor_active: bool,
                multi_strat: bool,
            }>,
            fully_synced_parts: Set<PartId>,
        }>,
        parts: Map<PartId, struct PartSyncState {
            emitted_full_synced: bool,
            peers: Set<PeerId>,
            fully_synced_peers: Set<PeerId>,
        }>,
    }
}

impl SyncStatMachine {
    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_full_sync_waiters(&self) -> Map<u64, Vec<(PeerId, PartId)>> {
        self.waiters
            .iter()
            .map(|(&waiter_id, waiter)| {
                (
                    waiter_id,
                    waiter
                        .need_set
                        .difference(&waiter.done_set)
                        .copied()
                        .collect(),
                )
            })
            .collect()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_last_object_syncs(&self) -> Vec<(PeerId, PartId, ObjId, std::time::Instant)> {
        self.last_object_syncs
            .iter()
            .map(|(&(peer_id, part_id), &(obj_id, at))| (peer_id, part_id, obj_id, at))
            .collect()
    }

    fn peer_part_is_fully_synced(&self, peer_id: PeerId, part_id: PartId) -> bool {
        let Some(peer_state) = self.peers.get(&peer_id) else {
            return false;
        };
        let Some(peer_part_state) = peer_state.parts.get(&part_id) else {
            return false;
        };
        !peer_part_state.multi_strat
            && peer_state.replay_phase_done
            && !peer_part_state.cursor_active
    }

    fn add_full_sync_waiter(
        &mut self,
        waiter_id: u64,
        peer_ids: Set<PeerId>,
        part_ids: Set<PartId>,
    ) {
        let peer_count = peer_ids.len();
        let part_count = part_ids.len();
        let mut done_set = Set::new();
        let mut need_set = Set::new();
        for &peer_id in &peer_ids {
            for &part_id in &part_ids {
                need_set.insert((peer_id, part_id));
                if self.peer_part_is_fully_synced(peer_id, part_id) {
                    done_set.insert((peer_id, part_id));
                }
            }
        }
        if done_set.len() == need_set.len() {
            tracing::debug!(
                waiter_id,
                peer_count,
                part_count,
                "full sync waiter satisfied immediately"
            );
            self.stat_evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
            return;
        }
        tracing::debug!(
            waiter_id,
            peer_count = peer_ids.len(),
            part_count = part_ids.len(),
            done_count = done_set.len(),
            "register full sync waiter"
        );
        let old = self
            .waiters
            .insert(waiter_id, FullSyncWaiterState { done_set, need_set });
        assert!(old.is_none(), "fishy");
    }

    fn remove_peer(&mut self, peer_id: PeerId) {
        let Some(peer_state) = self.peers.remove(&peer_id) else {
            return;
        };
        for (part_id, _peer_part_state) in peer_state.parts {
            self.last_object_syncs.remove(&(peer_id, part_id));
            let Some(part_state) = self.parts.get_mut(&part_id) else {
                continue;
            };
            part_state.peers.remove(&peer_id);
            part_state.fully_synced_peers.remove(&peer_id);
            for waiter in self.waiters.values_mut() {
                waiter.done_set.remove(&(peer_id, part_id));
                waiter.need_set.remove(&(peer_id, part_id));
            }
            if part_state.peers.is_empty() {
                self.parts.remove(&part_id);
            } else if part_state.peers.len() == part_state.fully_synced_peers.len() {
                if !part_state.emitted_full_synced {
                    part_state.emitted_full_synced = true;
                    self.stat_evts
                        .push(SyncStatEvent::PartFullySynced { part_id });
                }
            } else if part_state.emitted_full_synced {
                part_state.emitted_full_synced = false;
                self.stat_evts.push(SyncStatEvent::PartStale { part_id });
            }
        }
        for (waiter_id, _waiter) in self
            .waiters
            .extract_if(|_id, waiter| waiter.done_set.len() == waiter.need_set.len())
        {
            self.stat_evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
        }
    }

    fn mark_peer_replay_done(&mut self, peer_id: PeerId, replay_done: bool) {
        let peer_state = self.peers.entry(peer_id).or_default();
        peer_state.replay_phase_done = replay_done;
        for part_id in peer_state.parts.keys().copied().collect::<Vec<_>>() {
            if replay_done {
                self.__check_peer_part_synced(peer_id, part_id);
            } else {
                self.__check_peer_part_stale(peer_id, part_id);
            }
        }
    }

    fn mark_peer_part_only_cursor_strat(
        &mut self,
        peer_id: PeerId,
        part_id: PartId,
        only_cursor_strat: bool,
    ) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        peer_part_state.multi_strat = !only_cursor_strat;
        if only_cursor_strat {
            self.__check_peer_part_synced(peer_id, part_id);
        } else {
            self.__check_peer_part_stale(peer_id, part_id);
        }
    }

    fn mark_peer_part_idle(&mut self, peer_id: PeerId, part_id: PartId) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        peer_part_state.cursor_active = false;
        self.__check_peer_part_synced(peer_id, part_id);
    }

    fn mark_peer_part_cursor_active(&mut self, peer_id: PeerId, part_id: PartId) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let part_state = self.parts.entry(part_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        part_state.peers.insert(peer_id);

        peer_part_state.cursor_active = true;
        self.__check_peer_part_stale(peer_id, part_id);
    }

    fn __check_peer_part_synced(&mut self, peer_id: PeerId, part_id: PartId) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let part_state = self.parts.entry(part_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        part_state.peers.insert(peer_id);

        if peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active
        {
            return;
        }

        peer_state.fully_synced_parts.insert(part_id);
        part_state.fully_synced_peers.insert(peer_id);

        for waiter in self.waiters.values_mut() {
            if waiter.need_set.contains(&(peer_id, part_id)) {
                waiter.done_set.insert((peer_id, part_id));
            }
        }
        for (waiter_id, _waiter) in self
            .waiters
            .extract_if(|_id, waiter| waiter.done_set.len() == waiter.need_set.len())
        {
            self.stat_evts
                .push(SyncStatEvent::FullSyncWaiterSatisfied { waiter_id });
        }

        if !peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = true;
            self.stat_evts
                .push(SyncStatEvent::PeerPartFullySynced { peer_id, part_id });
        }
        if peer_state.fully_synced_parts.len() == peer_state.parts.len()
            && !peer_state.emitted_full_synced
        {
            peer_state.emitted_full_synced = true;
            self.stat_evts
                .push(SyncStatEvent::PeerFullySynced { peer_id });
        }
        if part_state.peers.len() == part_state.fully_synced_peers.len()
            && !part_state.emitted_full_synced
        {
            part_state.emitted_full_synced = true;
            self.stat_evts
                .push(SyncStatEvent::PartFullySynced { part_id });
        }
    }

    fn __check_peer_part_stale(&mut self, peer_id: PeerId, part_id: PartId) {
        let peer_state = self.peers.entry(peer_id).or_default();
        let part_state = self.parts.entry(part_id).or_default();
        let peer_part_state = peer_state.parts.entry(part_id).or_default();
        part_state.peers.insert(peer_id);

        if !(peer_part_state.multi_strat
            || !peer_state.replay_phase_done
            || peer_part_state.cursor_active)
        {
            return;
        }

        peer_state.fully_synced_parts.remove(&part_id);
        part_state.fully_synced_peers.remove(&peer_id);

        for waiter in self.waiters.values_mut() {
            waiter.done_set.remove(&(peer_id, part_id));
        }

        if peer_state.emitted_full_synced {
            peer_state.emitted_full_synced = false;
            self.stat_evts.push(SyncStatEvent::PeerStale { peer_id });
        }

        if peer_part_state.emitted_full_synced {
            peer_part_state.emitted_full_synced = false;
            self.stat_evts
                .push(SyncStatEvent::PeerPartStale { peer_id, part_id });
        }
        if part_state.emitted_full_synced {
            part_state.emitted_full_synced = false;
            self.stat_evts.push(SyncStatEvent::PartStale { part_id });
        }
    }

    fn record_object_synced(&mut self, peer_id: PeerId, part_id: PartId, obj_id: ObjId) {
        self.last_object_syncs
            .insert((peer_id, part_id), (obj_id, std::time::Instant::now()));
    }
}

structstruck::strike! {
    #[derive(Default)]
    pub struct BigSyncMachine {
        all_seen_peer: Set<PeerId>,
        peers: Map<PeerId, PeerState>,
        stat_machine: SyncStatMachine,

        cmds: VecDeque<(Uuid, BigSyncMachineCommand, Option<CursorIndex>, PeerId)>,
        tasks: Tasks,
    }
}

// public surface
impl BigSyncMachine {
    #[cfg(any(test, feature = "test-support"))]
    pub fn task_counts(&self) -> TaskCounts {
        self.tasks.task_counts()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_full_sync_waiters(&self) -> Map<u64, Vec<(PeerId, PartId)>> {
        self.stat_machine.debug_full_sync_waiters()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn debug_last_object_syncs(&self) -> Vec<(PeerId, PartId, ObjId, std::time::Instant)> {
        self.stat_machine.debug_last_object_syncs()
    }

    pub fn drain_sync_spawn_queue(&mut self) -> std::vec::Drain<'_, SyncTask> {
        self.tasks.drain_sync_spawn_queue()
    }

    pub fn drain_machine_spawn_queue(&mut self) -> std::vec::Drain<'_, MachineTask> {
        self.tasks.drain_machine_spawn_queue()
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, u64> {
        self.tasks.drain_stop_queue()
    }

    pub fn drain_stat_evts(&mut self) -> std::vec::Drain<'_, SyncStatEvent> {
        self.stat_machine.stat_evts.drain(..)
    }

    pub fn get_cmd(&mut self) -> Option<(Uuid, BigSyncMachineCommand)> {
        let (id, cmd, _, _) = self.cmds.front()?;
        Some((*id, cmd.clone()))
    }

    pub fn handle_evt(&mut self, evt: BigSyncEvent) {
        match evt {
            BigSyncEvent::SetPeer(evt) => self.handle_set_peer_evt(evt),
            BigSyncEvent::RemovePeer(evt) => self.handle_remove_peer_evt(evt),
            BigSyncEvent::WaitForFullSync(evt) => {
                self.stat_machine
                    .add_full_sync_waiter(evt.waiter_id, evt.peer_ids, evt.part_ids);
            }
            BigSyncEvent::SyncCompleted(evt) => {
                if self.tasks.stop_task(evt.task_id).is_some() {
                    self.handle_sync_completed(evt);
                }
            }
            BigSyncEvent::SyncFailed(evt) => {
                if let Some(state) = self.tasks.stop_task(evt.task_id) {
                    self.handle_sync_failed(evt, state.retry);
                }
            }
            BigSyncEvent::SyncStale(evt) => {
                if let Some(state) = self.tasks.stop_task(evt.task_id) {
                    self.handle_sync_stale(evt, state.retry);
                }
            }
        }
    }

    pub fn handle_cmd_success(&mut self, id: Uuid) {
        let (found_id, last_cmd, cursor, peer_id) = self
            .cmds
            .pop_front()
            .expect("success for a cmd that wasn't sent");
        if id != found_id {
            panic!("unexpected cmd success, cmds must be performed serially");
        }
        if let Some(cursor) = cursor {
            match last_cmd {
                BigSyncMachineCommand::SetPartCursor { .. } => unreachable!(),
                BigSyncMachineCommand::RemoveObjFromPart { obj_id, .. }
                | BigSyncMachineCommand::AddObjToPart { obj_id, .. } => {
                    let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
                    peer_state.cursor_machine.on_obj_sync_job_evt(
                        obj_id,
                        cursor,
                        CursorJobCompletionKind::Membership,
                        &mut peer_state.cursors_cmd_buf,
                    );
                    self.drain_cursor_machine_cmds(peer_id);
                }
            }
        }
    }

    pub fn handle_tick(&mut self, now: std::time::Instant) {
        self.tasks.enqueue_due_tasks(now);
    }

    pub fn handle_task_msg(&mut self, msg: MachineTaskMsg) {
        match msg {
            MachineTaskMsg::MachineTaskResult(MachineTaskResult { task_id, deets }) => {
                let Some(state) = self.tasks.stop_task(task_id) else {
                    return;
                };
                match deets {
                    TaskResultDeets::SetPeerStrategy(evt) => {
                        self.handle_set_peer_strat(task_id, state.retry, evt);
                    }
                    TaskResultDeets::ListBuckets(list_buckets_result) => {
                        self.handle_list_buckets_result(task_id, list_buckets_result);
                    }
                    TaskResultDeets::LeafBuckets(leaf_buckets_result) => {
                        self.handle_leaf_buckets_result(task_id, leaf_buckets_result);
                    }
                }
            }
            MachineTaskMsg::MachineTaskError(MachineTaskError { task_id, deets }) => {
                let Some(state) = self.tasks.stop_task(task_id) else {
                    return;
                };
                match deets {
                    MachineTaskErrDeets::DecidePeerStrategy(err) => {
                        self.handle_decide_peer_strat_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::PeerReplayWorker(err) => {
                        self.handle_peer_replay_worker_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::ListBuckets(err) => {
                        self.handle_list_buckets_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::LeafBuckets(err) => {
                        self.handle_leaf_buckets_err(task_id, state.retry, err)
                    }
                }
            }
            MachineTaskMsg::PeerReplayWorker(msg) => self.handle_peer_replay_worker_msg(msg),
        }
    }
}

// peer support
impl BigSyncMachine {
    fn handle_set_peer_evt(&mut self, SetPeerEvent { peer_id, parts }: SetPeerEvent) {
        tracing::debug!(peer_id = %peer_id, part_count = parts.len(), "set peer event");
        // clear out everything, avoid reuising any old state
        // treating SetPeer as a refresh peer cmd in a way
        self.handle_remove_peer_evt(RemovePeerEvent { peer_id });
        self.all_seen_peer.insert(peer_id);
        let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
            peer_id,
            parts: parts.iter().copied().collect(),
        });
        let decide_task = self.tasks.spawn_task(TaskSeed::Machine(deets));
        self.peers.insert(
            peer_id,
            PeerState {
                sync_workers: default(),
                replay_worker: default(),
                cursor_machine: default(),
                cursors_cmd_buf: default(),
                bucket_cmd_buf: default(),
                parts: parts
                    .into_iter()
                    .map(|id| {
                        (
                            id,
                            PeerPartState {
                                strat: PeerPartStrategy::Pending(decide_task),
                            },
                        )
                    })
                    .collect(),
            },
        );
    }

    fn handle_remove_peer_evt(&mut self, RemovePeerEvent { peer_id }: RemovePeerEvent) {
        if let Some(old) = self.peers.remove(&peer_id) {
            tracing::debug!(
                peer_id = %peer_id,
                part_count = old.parts.len(),
                sync_worker_count = old.sync_workers.len(),
                "remove peer event"
            );
            for worker in old.sync_workers.into_values() {
                let _state = self
                    .tasks
                    .stop_task(worker.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
            for (_old_part_id, state) in old.parts {
                match state.strat {
                    PeerPartStrategy::Pending(_) => {}
                    PeerPartStrategy::Cursor(_strat) => {}
                    PeerPartStrategy::Bucket(strat) => {
                        for task_id in strat
                            .active_leaf_tasks
                            .into_keys()
                            .chain(strat.active_list_tasks.into_keys())
                        {
                            let _state = self.tasks.stop_task(task_id).expect(ERROR_UNRECONIZED);
                        }
                    }
                }
            }
            if let Some(replay_worker) = old.replay_worker {
                let _state = self
                    .tasks
                    .stop_task(replay_worker.task_id)
                    .expect(ERROR_UNRECONIZED);
            }
            self.stat_machine.remove_peer(peer_id);
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_set_peer_strat(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        SetPeerStrategy {
            peer_id,
            part_strats,
        }: SetPeerStrategy,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let response_len = part_strats.len();
        let bucket_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Bucket(_)))
            .count();
        let cursor_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Cursor(_)))
            .count();
        let unknown_count = part_strats
            .values()
            .filter(|deets| matches!(*deets, PeerPartStratDecision::Unkown))
            .count();
        tracing::debug!(
            peer_id = %peer_id,
            response_len,
            bucket_count,
            cursor_count,
            unknown_count,
            "set peer strategy result"
        );
        let mut parts_retry = Set::new();

        for (part_id, decision) in part_strats {
            let old = peer_state.parts.remove(&part_id);
            let strat = match decision {
                PeerPartStratDecision::Unkown => {
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, false);
                    parts_retry.insert(part_id);
                    continue;
                }
                PeerPartStratDecision::Bucket(strat) => {
                    let mut machine = Box::new(BucketMachine::new(
                        part_id,
                        strat.remote_depth,
                        strat.remote_len,
                        strat.last_cursor,
                    ));
                    machine.on_bucket_page(
                        strat.initial_filtered_buckets,
                        &mut peer_state.bucket_cmd_buf,
                    );
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, false);
                    PeerPartStrategy::Bucket(BucketState {
                        machine,
                        // NOTE: we replay from the latest
                        // on bucket instead from the last_cursor
                        replay_cursor: strat.latest_cursor,
                        active_list_tasks: default(),
                        active_leaf_tasks: default(),
                    })
                }
                PeerPartStratDecision::Cursor(strat) => {
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, true);
                    PeerPartStrategy::Cursor(CursorState {
                        replay_cursor: strat.last_cursor,
                    })
                }
            };
            peer_state.parts.insert(part_id, PeerPartState { strat });
            if let Some(old) = old {
                match old.strat {
                    PeerPartStrategy::Pending(old_task_id) => {
                        assert!(task_id == old_task_id, "fishy");
                    }
                    PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
                }
            }
        }

        // retry peer summary requests for any parts
        // that were not resolved in the last request
        // FIXME: probably not a good idea since we're going
        // to abort any work queued by the machines if
        // this immediately works. i.e. evalute if this leads
        // to loops,
        if !parts_retry.is_empty() {
            if retry.attempt_no > 0 && response_len > 0 {
                warn!("retry attempt is retrying again after minimal improvement");
            }
            let deets = TaskSeed::Machine(MachineTaskDeets::DecidePeerStrategy(
                DecidePeerStrategyTask {
                    peer_id,
                    parts: parts_retry.clone(),
                },
            ));
            let decide_task = if parts_retry.len() == response_len {
                self.tasks
                    .spawn_delayed_task(deets, retry, Duration::from_secs(2))
            } else {
                self.tasks.spawn_task(deets)
            };
            for part_id in parts_retry {
                peer_state.parts.insert(
                    part_id,
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
            }
        }

        let bucket_cmd_count = peer_state.bucket_cmd_buf.len();
        let cursor_cmd_count = peer_state.cursors_cmd_buf.len();
        // refresh the PeerReplayWorker if needed
        self.refresh_peer_replay_worker(peer_id);
        tracing::debug!(
            peer_id = %peer_id,
            bucket_cmd_count,
            cursor_cmd_count,
            "drain bucket commands from set peer strategy"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    fn handle_decide_peer_strat_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        DecidePeerStrategyTaskError { peer_id, deets }: DecidePeerStrategyTaskError,
    ) {
        tracing::warn!(peer_id = %peer_id, task_id, ?deets, "decide peer strategy failed");
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        match deets {
            DecidePeerStrategyErrorDeets::ListError(ListPartsError::UnkownParts {
                unkown_parts,
            }) => {
                for part_id in unkown_parts {
                    peer_state.parts.remove(&part_id);
                }
                let parts = peer_state.parts.keys().copied().collect();
                self.handle_set_peer_evt(SetPeerEvent { peer_id, parts });
                return;
            }
            DecidePeerStrategyErrorDeets::Rpc(_) => {
                // noop, retry with backoff
            }
        }
        let mut parts_retry = Set::new();

        for (part_id, state) in &peer_state.parts {
            match &state.strat {
                PeerPartStrategy::Pending(old_task_id) => {
                    if *old_task_id != task_id {
                        // the task that errored out must have been stale
                        // FIXME: this assumes that all parts for a peer
                        // are decided in the same task (having per part task ids can be misleading)
                        return;
                    }
                    parts_retry.insert(*part_id);
                }
                PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => {}
            };
        }
        if !parts_retry.is_empty() {
            let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id,
                parts: parts_retry.clone(),
            });
            let decide_task = self.tasks.spawn_delayed_task(
                TaskSeed::Machine(deets),
                retry,
                Duration::from_secs(2),
            );
            for part_id in parts_retry {
                let old = peer_state.parts.insert(
                    part_id,
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
                assert!(matches!(
                    old,
                    Some(PeerPartState {
                        strat: PeerPartStrategy::Pending(_),
                        ..
                    })
                ));
            }
        }
    }
}

// cursor support
impl BigSyncMachine {
    fn refresh_peer_replay_worker(&mut self, peer_id: PeerId) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        let replay_req_parts: Set<_> = peer_state
            .parts
            .iter()
            .filter_map(|(&part_id, state)| match state.strat {
                PeerPartStrategy::Pending(_) => None,
                PeerPartStrategy::Bucket(_) | PeerPartStrategy::Cursor(_) => Some(part_id),
            })
            .collect();
        let refresh = match &peer_state.replay_worker {
            Some(old_state) => {
                let refresh = replay_req_parts != old_state.parts;
                if refresh {
                    let _state = self
                        .tasks
                        .stop_task(old_state.task_id)
                        .expect(ERROR_UNRECONIZED);
                }
                refresh
            }
            None => true,
        };
        if refresh {
            tracing::debug!(
                peer_id = %peer_id,
                part_count = replay_req_parts.len(),
                "refresh peer replay worker"
            );
            let deets = TaskSeed::Machine(MachineTaskDeets::PeerReplay(PeerReplayTask {
                peer_id,
                parts: peer_state.cursors_for_peer_replay_worker_parts(replay_req_parts.iter()),
            }));
            let replay_task = self.tasks.spawn_task(deets);
            peer_state.replay_worker = Some(PeerReplayWorkerState {
                task_id: replay_task,
                parts: replay_req_parts,
            });
            self.stat_machine.mark_peer_replay_done(peer_id, false);
        }
    }
    fn handle_peer_replay_worker_msg(&mut self, msg: PeerReplayWorkerMsg) {
        let Some(peer_state) = self.peers.get_mut(&msg.peer_id) else {
            assert!(self.all_seen_peer.contains(&msg.peer_id), "fishy");
            return;
        };
        let Some(worker) = &mut peer_state.replay_worker else {
            // there is no worker in ba sing se
            return;
        };
        if worker.task_id != msg.task_id {
            // stale peer, ignore the message to avoid dupe events
            // from two peers
            return;
        }

        if let SubEvent::ReplayComplete = &msg.evt {
            self.stat_machine.mark_peer_replay_done(msg.peer_id, true);
            return;
        };
        peer_state
            .cursor_machine
            .on_subscription_evt(msg.evt, &mut peer_state.cursors_cmd_buf);
        self.drain_cursor_machine_cmds(msg.peer_id);
    }

    fn handle_peer_replay_worker_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        PeerReplayWorkerError { peer_id, deets }: PeerReplayWorkerError,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let Some(worker) = &peer_state.replay_worker else {
            panic!("curiosity trap: peer doesn't have replay worker, zero partitions?");
        };
        if worker.task_id != task_id {
            // the task that died must have been stale
            return;
        }
        match deets {
            PeerReplayWorkerErrorDeets::SubError(ListPartsError::UnkownParts { unkown_parts }) => {
                // FIXME: this is a bug, if the host says set peer with this parts
                // we shouldn't override that here. instead, decide peer policy
                // should still decide for known parts instead of erroring out
                // immediately. let's then wire up msgs to the hosts indicating
                // troublesome partition, we should retry those on a backoff
                // change the RPC layer to expose partial summary resutls and
                // so on

                // remote part policy must have changed since last check
                // reset those
                for part_id in unkown_parts {
                    peer_state.parts.remove(&part_id);
                }
                let parts = peer_state.parts.keys().copied().collect();
                tracing::debug!(
                    peer_id = %peer_id,
                    remaining_part_count = peer_state.parts.len(),
                    "peer replay worker saw unknown parts"
                );
                self.handle_set_peer_evt(SetPeerEvent { peer_id, parts });
                return;
            }
            PeerReplayWorkerErrorDeets::StreamClosed
            | PeerReplayWorkerErrorDeets::Rpc(_)
            | PeerReplayWorkerErrorDeets::MpscSend(_)
            | PeerReplayWorkerErrorDeets::MpscRecv(_) => {
                // noop
            }
        }
        let deets = MachineTaskDeets::PeerReplay(PeerReplayTask {
            peer_id,
            parts: peer_state.cursors_for_peer_replay_worker_parts(worker.parts.iter()),
        });
        let replay_task =
            self.tasks
                .spawn_delayed_task(TaskSeed::Machine(deets), retry, Duration::from_secs(2));
        peer_state.replay_worker = Some(PeerReplayWorkerState {
            task_id: replay_task,
            parts: worker.parts.clone(),
        });
        self.stat_machine.mark_peer_replay_done(peer_id, false);
    }

    fn drain_cursor_machine_cmds(&mut self, peer_id: PeerId) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        for cmd in peer_state.cursors_cmd_buf.drain(..) {
            trace!(peer_id = %peer_id, ?cmd,"processing cursor cmd");
            match cmd {
                CursorMachineCommand::PartIdle { part_id } => {
                    self.stat_machine.mark_peer_part_idle(peer_id, part_id);
                }
                CursorMachineCommand::SyncObj {
                    obj_id,
                    remote_payload,
                    parts,
                    cursor,
                } => {
                    let (cursors, part_hints, remote_payload) =
                        if let Some(mut worker) = peer_state.sync_workers.remove(&obj_id) {
                            let _state = self
                                .tasks
                                .stop_task(worker.task_id)
                                .expect(ERROR_UNRECONIZED);

                            worker.part_hints.extend(parts.iter().copied());
                            worker.cursors.insert(cursor);
                            (
                                worker.cursors,
                                worker.part_hints,
                                Some(remote_payload).or(worker.remote_payload),
                            )
                        } else {
                            (
                                [cursor].into(),
                                parts.iter().copied().collect(),
                                Some(remote_payload),
                            )
                        };
                    let deets = SyncTaskDeets {
                        peer_id,
                        obj_id,
                        remote_payload: remote_payload.clone(),
                    };
                    let task_id = self.tasks.spawn_task(TaskSeed::Sync(SyncTaskSeed {
                        part_hints: part_hints.iter().copied().collect(),
                        deets: deets.clone(),
                    }));
                    peer_state.sync_workers.insert(
                        obj_id,
                        SyncWorkerState {
                            task_id,
                            cursors,
                            part_hints,
                            remote_payload,
                        },
                    );
                    for part_id in parts {
                        self.stat_machine
                            .mark_peer_part_cursor_active(peer_id, part_id);
                    }
                }
                CursorMachineCommand::SetPartCursor { part_id, cursor } => {
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    match &mut part.strat {
                        PeerPartStrategy::Bucket(state) => {
                            state.replay_cursor = cursor;
                        }
                        PeerPartStrategy::Cursor(state) => {
                            // we only update the peer part cursor
                            // in the cursor phase
                            self.cmds.push_back((
                                Uuid::new_v4(),
                                BigSyncMachineCommand::SetPartCursor {
                                    peer_id,
                                    part_id,
                                    cursor,
                                },
                                None,
                                peer_id,
                            ));
                            state.replay_cursor = cursor;
                        }
                        _ => unreachable!(),
                    }
                }
                CursorMachineCommand::RemoveObjFromPart {
                    obj_id,
                    part_id,
                    cursor,
                } => {
                    self.cmds.push_back((
                        Uuid::new_v4(),
                        BigSyncMachineCommand::RemoveObjFromPart { obj_id, part_id },
                        Some(cursor),
                        peer_id,
                    ));
                }
                CursorMachineCommand::AddObjToPart {
                    obj_id,
                    part_id,
                    cursor,
                } => {
                    self.cmds.push_back((
                        Uuid::new_v4(),
                        BigSyncMachineCommand::AddObjToPart { obj_id, part_id },
                        Some(cursor),
                        peer_id,
                    ));
                }
            }
        }
    }
}

// bucket support
impl BigSyncMachine {
    fn drain_bucket_machine_cmds(&mut self, peer_id: PeerId) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        // NOTE: ordering is important here, execute commands
        // in given order
        let mut refresh_peer_replaly = false;
        for cmd in peer_state.bucket_cmd_buf.drain(..) {
            trace!(peer_id = %peer_id, ?cmd, "processing bucket cmd");
            match cmd {
                BucketMachineCommand::SyncObj {
                    obj_id,
                    part_id,
                    remote_payload,
                } => {
                    let (cursors, part_hints, remote_payload) =
                        if let Some(mut worker) = peer_state.sync_workers.remove(&obj_id) {
                            let _state = self
                                .tasks
                                .stop_task(worker.task_id)
                                .expect(ERROR_UNRECONIZED);

                            worker.part_hints.insert(part_id);
                            (
                                worker.cursors,
                                worker.part_hints,
                                remote_payload.or(worker.remote_payload),
                            )
                        } else {
                            (default(), [part_id].into(), remote_payload)
                        };
                    let deets = SyncTaskDeets {
                        peer_id,
                        obj_id,
                        remote_payload: remote_payload.clone(),
                    };
                    let task_id = self.tasks.spawn_task(TaskSeed::Sync(SyncTaskSeed {
                        part_hints: part_hints.iter().copied().collect(),
                        deets: deets.clone(),
                    }));
                    peer_state.sync_workers.insert(
                        obj_id,
                        SyncWorkerState {
                            task_id,
                            cursors,
                            part_hints,
                            remote_payload,
                        },
                    );
                }
                BucketMachineCommand::RemoveObjFromPart { obj_id, part_id } => {
                    self.cmds.push_back((
                        Uuid::new_v4(),
                        BigSyncMachineCommand::RemoveObjFromPart { obj_id, part_id },
                        None,
                        peer_id,
                    ));
                }
                BucketMachineCommand::ListBuckets {
                    offset,
                    since,
                    part_id,
                    working_level,
                } => {
                    let task = ListBucketsTask {
                        peer_id,
                        part_id,
                        offset,
                        since,
                        working_level,
                    };
                    let deets = MachineTaskDeets::ListBuckets(task.clone());
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    let PeerPartStrategy::Bucket(state) = &mut part.strat else {
                        unreachable!()
                    };
                    let task_id = self.tasks.spawn_task(TaskSeed::Machine(deets));
                    let old = state.active_list_tasks.insert(task_id, task);
                    assert!(old.is_none(), "fishy");
                }
                BucketMachineCommand::LeafBuckets {
                    since,
                    buckets,
                    part_id,
                } => {
                    let task = LeafBucketsTask {
                        peer_id,
                        part_id,
                        since,
                        buckets,
                    };
                    let deets = MachineTaskDeets::LeafBuckets(task.clone());
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    let PeerPartStrategy::Bucket(state) = &mut part.strat else {
                        unreachable!()
                    };
                    let task_id = self.tasks.spawn_task(TaskSeed::Machine(deets));
                    let old = state.active_leaf_tasks.insert(task_id, task);
                    assert!(old.is_none(), "fishy");
                }
                BucketMachineCommand::UpgradeToCursor { part_id } => {
                    let Some(PeerPartState {
                        strat: PeerPartStrategy::Bucket(old),
                    }) = peer_state.parts.remove(&part_id)
                    else {
                        unreachable!()
                    };
                    assert!(old.active_list_tasks.is_empty());
                    assert!(old.active_leaf_tasks.is_empty());
                    self.cmds.push_back((
                        Uuid::new_v4(),
                        BigSyncMachineCommand::SetPartCursor {
                            peer_id,
                            part_id,
                            cursor: old.replay_cursor,
                        },
                        None,
                        peer_id,
                    ));
                    peer_state.parts.insert(
                        part_id,
                        PeerPartState {
                            strat: PeerPartStrategy::Cursor(CursorState {
                                replay_cursor: old.replay_cursor,
                            }),
                        },
                    );
                    self.stat_machine
                        .mark_peer_part_only_cursor_strat(peer_id, part_id, true);
                    refresh_peer_replaly = true;
                }
            }
        }
        if refresh_peer_replaly {
            self.refresh_peer_replay_worker(peer_id);
        }
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_list_buckets_result(
        &mut self,
        task_id: TaskId,
        ListBucketsResult {
            peer_id,
            part_id,
            filtered_buckets,
        }: ListBucketsResult,
    ) {
        let mut bucket_cmd_buf = Vec::new();
        {
            let Some(peer_state) = self.peers.get_mut(&peer_id) else {
                assert!(self.all_seen_peer.contains(&peer_id), "fishy");
                return;
            };
            let Some(part_state) = peer_state.parts.get_mut(&part_id) else {
                return;
            };
            let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
                return;
            };
            let Some(task) = strat.active_list_tasks.remove(&task_id) else {
                return;
            };
            assert_eq!(task.peer_id, peer_id, "fishy");
            assert_eq!(task.part_id, part_id, "fishy");
            strat
                .machine
                .on_bucket_page(filtered_buckets, &mut bucket_cmd_buf);
        }
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        peer_state.bucket_cmd_buf.extend(bucket_cmd_buf);
        tracing::debug!(
            peer_id = %peer_id,
            part_id = %part_id,
            filtered_bucket_count = peer_state.bucket_cmd_buf.len(),
            "list buckets result"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    #[tracing::instrument(
        skip_all,
        fields(
            peer_id = %peer_id,
            task_id = %task_id,
        )
    )]
    fn handle_leaf_buckets_result(
        &mut self,
        task_id: TaskId,
        LeafBucketsResult {
            peer_id,
            filtered_objs,
        }: LeafBucketsResult,
    ) {
        let mut bucket_cmd_buf = Vec::new();
        let mut handled = false;
        {
            let Some(peer_state) = self.peers.get_mut(&peer_id) else {
                assert!(self.all_seen_peer.contains(&peer_id), "fishy");
                return;
            };
            for part_state in peer_state.parts.values_mut() {
                let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
                    continue;
                };
                let Some(task) = strat.active_leaf_tasks.remove(&task_id) else {
                    continue;
                };
                assert_eq!(task.peer_id, peer_id, "fishy");
                let _part_id = task.part_id;
                strat
                    .machine
                    .on_obj_page(filtered_objs, &mut bucket_cmd_buf);
                handled = true;
                break;
            }
        }
        if !handled {
            return;
        }
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        peer_state.bucket_cmd_buf.extend(bucket_cmd_buf);
        tracing::debug!(
            peer_id = %peer_id,
            filtered_cmd_count = peer_state.bucket_cmd_buf.len(),
            "leaf buckets result"
        );
        self.drain_bucket_machine_cmds(peer_id);
    }

    fn handle_list_buckets_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        ListBucketsTaskError {
            peer_id,
            part_id,
            _deets: _,
        }: ListBucketsTaskError,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let Some(part_state) = peer_state.parts.get_mut(&part_id) else {
            return;
        };
        let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
            return;
        };
        let Some(task) = strat.active_list_tasks.remove(&task_id) else {
            return;
        };
        assert_eq!(task.peer_id, peer_id, "fishy");
        let task_id = self.tasks.spawn_delayed_task(
            TaskSeed::Machine(MachineTaskDeets::ListBuckets(task.clone())),
            retry,
            Duration::from_secs(2),
        );
        let old = strat.active_list_tasks.insert(task_id, task);
        assert!(old.is_none(), "fishy");
    }

    fn handle_leaf_buckets_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        LeafBucketsTaskError {
            peer_id,
            part_id,
            _deets: _,
        }: LeafBucketsTaskError,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let Some(part_state) = peer_state.parts.get_mut(&part_id) else {
            return;
        };
        let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
            return;
        };
        let Some(task) = strat.active_leaf_tasks.remove(&task_id) else {
            return;
        };
        assert_eq!(task.peer_id, peer_id, "fishy");
        let task_id = self.tasks.spawn_delayed_task(
            TaskSeed::Machine(MachineTaskDeets::LeafBuckets(task.clone())),
            retry,
            Duration::from_secs(2),
        );
        let old = strat.active_leaf_tasks.insert(task_id, task);
        assert!(old.is_none(), "fishy");
    }
}

// sync support
impl BigSyncMachine {
    fn handle_sync_completed(&mut self, evt: SyncCompletedEvent) {
        let completion = evt.completion;
        let Some(peer_state) = self.peers.get(&evt.peer_id) else {
            assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
            return;
        };
        let Some(worker) = peer_state.sync_workers.get(&completion.obj_id) else {
            return;
        };
        // FIXME: consider progressing cursors/parts
        // still on the incoming event?
        if worker.task_id != evt.task_id {
            return;
        }

        let (completion, part_hints) = {
            let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
                return;
            };
            let Some(worker) = peer_state.sync_workers.remove(&completion.obj_id) else {
                return;
            };
            let completion = SyncJobEvt {
                obj_id: completion.obj_id,
                cursors: worker.cursors,
                deets: completion.deets,
            };
            let part_hints = worker.part_hints.clone();
            for part_id in &part_hints {
                let Some(part) = peer_state.parts.get_mut(part_id) else {
                    warn!("sync completed for unknown part");
                    continue;
                };
                match &mut part.strat {
                    PeerPartStrategy::Pending(_) => {
                        unreachable!("unexpected pending peer part strategy")
                    }
                    PeerPartStrategy::Bucket(state) => {
                        state
                            .machine
                            .on_obj_sync_completed(&completion, &mut peer_state.bucket_cmd_buf);
                    }
                    PeerPartStrategy::Cursor(_state) => {}
                }
            }
            for &cursor in &completion.cursors {
                peer_state.cursor_machine.on_obj_sync_job_evt(
                    completion.obj_id,
                    cursor,
                    CursorJobCompletionKind::Sync,
                    &mut peer_state.cursors_cmd_buf,
                );
            }
            (completion, part_hints)
        };
        for part_id in &part_hints {
            self.stat_machine
                .record_object_synced(evt.peer_id, *part_id, completion.obj_id);
        }
        self.stat_machine
            .stat_evts
            .push(SyncStatEvent::ObjectSynced {
                peer_id: evt.peer_id,
                obj_id: completion.obj_id,
            });
        self.drain_cursor_machine_cmds(evt.peer_id);
        self.drain_bucket_machine_cmds(evt.peer_id);
    }

    fn handle_sync_failed(&mut self, evt: SyncFailedEvent, retry: Retry) {
        let (deets, part_hints) = {
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.sync_workers.get(&evt.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }
            (
                SyncTaskDeets {
                    peer_id: evt.peer_id,
                    obj_id: evt.obj_id,
                    remote_payload: worker.remote_payload.clone(),
                },
                worker.part_hints.clone(),
            )
        };

        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        if let Some(worker) = peer_state.sync_workers.get_mut(&evt.obj_id) {
            let task_id = self.tasks.spawn_delayed_task(
                TaskSeed::Sync(SyncTaskSeed {
                    part_hints: part_hints.clone(),
                    deets,
                }),
                retry,
                Duration::from_secs(2),
            );
            worker.task_id = task_id;
        }
    }

    fn handle_sync_stale(&mut self, evt: SyncStaleEvent, retry: Retry) {
        let (deets, part_hints) = {
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.sync_workers.get(&evt.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }
            (
                SyncTaskDeets {
                    peer_id: evt.peer_id,
                    obj_id: evt.obj_id,
                    remote_payload: worker.remote_payload.clone(),
                },
                worker.part_hints.clone(),
            )
        };

        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        if let Some(worker) = peer_state.sync_workers.get_mut(&evt.obj_id) {
            let task_id = self.tasks.spawn_delayed_task(
                TaskSeed::Sync(SyncTaskSeed {
                    part_hints: part_hints.clone(),
                    deets,
                }),
                retry,
                Duration::from_secs(2),
            );
            worker.task_id = task_id;
        }
    }
}
