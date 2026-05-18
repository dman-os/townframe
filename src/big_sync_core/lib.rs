//! TODO: merkle catchup
//! FIXME: minimize awaits on the BigSyncMachine lest it block work
//! for example, talking to the part store

mod interlude {
    pub use utils_rs::prelude::*;

    pub use future_form::FutureForm;

    // FIXME: consider using indexed map instead
    pub use std::collections::{HashMap as Map, HashSet as Set, VecDeque};

    pub use crate::ids::{BuckId, ObjId, PartId, PeerId};
}

use crate::interlude::*;
use crate::tasks::leaf_buckets::{LeafBucketsResult, LeafBucketsTask, LeafBucketsTaskError};
use crate::tasks::list_bucket::{ListBucketsResult, ListBucketsTask, ListBucketsTaskError};

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
use tasks::decide_peer_strat::*;
use tasks::peer_replay::*;
use tasks::*;

pub use ids::{BuckId, Byte32Id, ObjId, PartId, PeerId};
pub use fingerprint::{Fingerprint, FingerprintSeed};
pub use tasks::{MachineTask, MachineTaskMsg, SyncTask, SyncTaskDeets, TaskCtx, TaskId};
#[cfg(any(test, feature = "test-support"))]
pub use tasks::TaskCounts;

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
        LocalPartsChanged (
            pub struct LocalPartsChangedEvent {
                pub parts: Set<PartId>,
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
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncCompletionDeets {
        AddedMember,
        ChangedObject,
        Noop,
        // DeletedObj
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
        pub cursors: Vec<CursorIndex>,
        pub deets: SyncCompletionDeets,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncStatEvent {
        PartFullySynced {
            peer_id: PeerId,
            part_id: PartId,
        },
        PartStale {
            peer_id: PeerId,
            part_id: PartId,
        },
        PeerFullySynced {
            peer_id: PeerId,
        },
        PeerStale {
            peer_id: PeerId,
        },
    }
}

structstruck::strike! {
    struct PeerState {
        fully_synced_parts: Set<PartId>,
        emitted_full_synced: bool,
        sync_workers: Map<ObjId, struct SyncWorkerState {
            task_id: TaskId,
            deets: SyncTaskDeets,
            cursors: Vec<CursorIndex>,
        }>,
        replay_worker: Option<struct PeerReplayWorkerState {
            task_id: TaskId,
            parts: Set<PartId>,
        }>,

        cursor_machine: Option<CursorSyncMachine>,

        cursors_cmd_buf: Vec<CursorMachineCommand>,
        bucket_cmd_buf: Vec<BucketMachineCommand>,

        parts: Map<PartId, struct PeerPartState {
            strat: enum PeerPartStrategy {
                Pending(TaskId),
                Bucket(struct BucketState {
                    latest_cursor: CursorIndex,
                    machine: BucketMachine,
                    active_list_tasks: Map<TaskId, ListBucketsTask>,
                    active_leaf_tasks: Map<TaskId, LeafBucketsTask>,
                }),
                Cursor(struct CursorState {
                    last_cursor: CursorIndex,
                }),
            }
        }>
    }
}

impl PeerState {
    fn part_is_fully_synced(&self, part_id: PartId) -> bool {
        matches!(
            self.parts.get(&part_id).map(|part| &part.strat),
            Some(PeerPartStrategy::Cursor(_))
        ) && !self
            .sync_workers
            .values()
            .any(|worker| worker.deets.part_hints.contains(&part_id))
    }

    fn cursors_for_peer_replay_worker_parts<'a>(
        &self,
        parts: impl std::iter::Iterator<Item = &'a PartId>,
    ) -> Map<PartId, CursorIndex> {
        parts
            .map(|&part_id| {
                match self.parts.get(&part_id).expect(ERROR_IMPOSSIBLE).strat {
                    PeerPartStrategy::Pending(_) => unreachable!(),
                    PeerPartStrategy::Bucket(BucketState { latest_cursor, .. }) => (
                        part_id,
                        // on merkle, start the replay since the latest
                        // on a focus on live events
                        latest_cursor,
                    ),
                    PeerPartStrategy::Cursor(CursorState { last_cursor, .. }) => {
                        (part_id, last_cursor)
                    }
                }
            })
            .collect()
    }
}

#[derive(Default)]
pub struct BigSyncMachine {
    all_seen_peer: Set<PeerId>,
    peers: Map<PeerId, PeerState>,

    tasks: Tasks,
}

impl BigSyncMachine {
    #[cfg(any(test, feature = "test-support"))]
    pub fn task_counts(&self) -> TaskCounts {
        self.tasks.task_counts()
    }

    pub fn pop_sync_spawn_queue(&mut self) -> Option<SyncTask> {
        self.tasks.pop_sync_spawn_queue()
    }

    pub fn pop_machine_spawn_queue(&mut self) -> Option<MachineTask> {
        self.tasks.pop_machine_spawn_queue()
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, u64> {
        self.tasks.drain_stop_queue()
    }

    pub async fn handle_evt<K: FutureForm, S: PartStore<K>>(
        &mut self,
        evt: BigSyncEvent,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
    ) {
        match evt {
            BigSyncEvent::SetPeer(evt) => self.handle_set_peer_evt(evt, stats_out),
            BigSyncEvent::RemovePeer(evt) => self.handle_remove_peer_evt(evt, stats_out),
            BigSyncEvent::LocalPartsChanged(evt) => {
                self.handle_local_parts_changed(evt, stats_out)
            }
            BigSyncEvent::SyncCompleted(evt) => {
                if self.tasks.stop_task(evt.task_id).is_some() {
                    self.handle_sync_completed(evt, part_store, stats_out).await;
                }
            }
            BigSyncEvent::SyncFailed(evt) => {
                if let Some(state) = self.tasks.stop_task(evt.task_id) {
                    self.handle_sync_failed(evt, state.retry);
                }
            }
        }
    }

    pub fn handle_tick(&mut self, now: std::time::Instant) {
        self.tasks.enqueue_due_tasks(now);
    }

    pub async fn handle_task_msg<K: FutureForm, S: PartStore<K>>(
        &mut self,
        msg: MachineTaskMsg,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
    ) {
        match msg {
            MachineTaskMsg::MachineTaskResult(MachineTaskResult { task_id, deets }) => {
                let Some(state) = self.tasks.stop_task(task_id) else {
                    return;
                };
                match deets {
                    TaskResultDeets::SetPeerStrategy(evt) => {
                        self.handle_set_peer_strat(task_id, state.retry, evt, part_store, stats_out)
                            .await
                    }
                    TaskResultDeets::ListBuckets(list_buckets_result) => {
                        self.handle_list_buckets_result(
                            task_id,
                            list_buckets_result,
                            part_store,
                            stats_out,
                        )
                            .await
                    }
                    TaskResultDeets::LeafBuckets(leaf_buckets_result) => {
                        self.handle_leaf_buckets_result(
                            task_id,
                            leaf_buckets_result,
                            part_store,
                            stats_out,
                        )
                            .await
                    }
                }
            }
            MachineTaskMsg::MachineTaskError(MachineTaskError { task_id, deets }) => {
                let Some(state) = self.tasks.stop_task(task_id) else {
                    return;
                };
                match deets {
                    MachineTaskErrDeets::DecidePeerStrategyError(err) => {
                        self.handle_decide_peer_strat_err(task_id, state.retry, err, stats_out)
                    }
                    MachineTaskErrDeets::PeerReplayWorkerError(err) => {
                        self.handle_peer_replay_worker_err(task_id, state.retry, err, stats_out)
                    }
                    MachineTaskErrDeets::ListBucketsError(err) => {
                        self.handle_list_buckets_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::LeafBucketsError(err) => {
                        self.handle_leaf_buckets_err(task_id, state.retry, err)
                    }
                }
            }
            MachineTaskMsg::PeerReplayWorker(msg) => {
                self.handle_peer_replay_worker_msg(msg, part_store, stats_out).await
            }
        }
    }
}

// peer support
impl BigSyncMachine {
    fn refresh_peer_sync_state(&mut self, peer_id: PeerId, out: &mut Vec<SyncStatEvent>) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            return;
        };

        let old_fully_synced_parts = peer_state.fully_synced_parts.clone();
        let mut new_fully_synced_parts = Set::new();
        for part_id in peer_state.parts.keys().copied().collect::<Vec<_>>() {
            if peer_state.part_is_fully_synced(part_id) {
                new_fully_synced_parts.insert(part_id);
            }
        }

        for part_id in new_fully_synced_parts.difference(&old_fully_synced_parts) {
            out.push(SyncStatEvent::PartFullySynced {
                peer_id,
                part_id: *part_id,
            });
        }
        for part_id in old_fully_synced_parts.difference(&new_fully_synced_parts) {
            out.push(SyncStatEvent::PartStale {
                peer_id,
                part_id: *part_id,
            });
        }

        peer_state.fully_synced_parts = new_fully_synced_parts;
        let peer_fully_synced =
            !peer_state.parts.is_empty() && peer_state.fully_synced_parts.len() == peer_state.parts.len();
        if peer_fully_synced && !peer_state.emitted_full_synced {
            peer_state.emitted_full_synced = true;
            out.push(SyncStatEvent::PeerFullySynced { peer_id });
        } else if !peer_fully_synced && peer_state.emitted_full_synced {
            peer_state.emitted_full_synced = false;
            out.push(SyncStatEvent::PeerStale { peer_id });
        }
    }

    pub fn handle_set_peer_evt(
        &mut self,
        SetPeerEvent { peer_id, parts }: SetPeerEvent,
        out: &mut Vec<SyncStatEvent>,
    ) {
        // clear out everything, avoid reuising any old state
        // treating SetPeer as a refresh peer cmd in a way
        self.handle_remove_peer_evt(RemovePeerEvent { peer_id }, out);
        self.all_seen_peer.insert(peer_id);
        let deets = MachineTaskDeets::DecidePeerStrategy(DecidePeerStrategyTask {
            peer_id,
            parts: parts.iter().copied().collect(),
        });
        let decide_task = self.tasks.spawn_task(TaskSeed::Machine(deets));
        self.peers.insert(
            peer_id,
            PeerState {
                fully_synced_parts: default(),
                emitted_full_synced: false,
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

    fn handle_remove_peer_evt(
        &mut self,
        RemovePeerEvent { peer_id }: RemovePeerEvent,
        out: &mut Vec<SyncStatEvent>,
    ) {
        if let Some(old) = self.peers.remove(&peer_id) {
            if old.emitted_full_synced {
                out.push(SyncStatEvent::PeerStale { peer_id });
            }
            for part_id in old.fully_synced_parts {
                out.push(SyncStatEvent::PartStale { peer_id, part_id });
            }
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
        }
    }

    fn handle_local_parts_changed(
        &mut self,
        LocalPartsChangedEvent { parts }: LocalPartsChangedEvent,
        out: &mut Vec<SyncStatEvent>,
    ) {
        for (&peer_id, peer_state) in &mut self.peers {
            let mut peer_stale = false;
            for part_id in &parts {
                if peer_state.fully_synced_parts.remove(part_id) {
                    out.push(SyncStatEvent::PartStale {
                        peer_id,
                        part_id: *part_id,
                    });
                    peer_stale = true;
                }
            }
            if peer_stale && peer_state.emitted_full_synced {
                peer_state.emitted_full_synced = false;
                out.push(SyncStatEvent::PeerStale { peer_id });
            }
        }
    }

    async fn handle_set_peer_strat<K: FutureForm, S: PartStore<K>>(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        SetPeerStrategy {
            peer_id,
            part_strats,
        }: SetPeerStrategy,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
    ) {
        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
        let response_len = part_strats.len();
        let mut parts_retry = Set::new();

        for (part_id, decision) in part_strats {
            let old = peer_state.parts.remove(&part_id);
            let strat = match decision {
                PeerPartStratDecision::Unkown => {
                    parts_retry.insert(part_id);
                    continue;
                }
                PeerPartStratDecision::Bucket(strat) => {
                    let mut machine = BucketMachine::new(
                        part_id,
                        strat.remote_depth,
                        strat.remote_len,
                        strat.latest_cursor,
                        strat.last_cursor,
                    );
                    machine.on_bucket_page(
                        strat.initial_filtered_buckets,
                        &mut peer_state.bucket_cmd_buf,
                    );
                    PeerPartStrategy::Bucket(BucketState {
                        machine,
                        latest_cursor: strat.latest_cursor,
                        active_list_tasks: default(),
                        active_leaf_tasks: default(),
                    })
                }
                PeerPartStratDecision::Cursor(strat) => {
                    peer_state.cursor_machine.get_or_insert_default();
                    PeerPartStrategy::Cursor(CursorState {
                        last_cursor: strat.last_cursor,
                    })
                }
            };
            peer_state.parts.insert(part_id, PeerPartState { strat });
            if let Some(old) = old {
                match old.strat {
                    PeerPartStrategy::Pending(old_task_id) => {
                        assert!(task_id == old_task_id, "fishy");
                    }
                    _ => {}
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

        // refresh the PeerReplayWorker if needed
        self.refres_peer_replay_worker(peer_id);
        self.drain_bucket_machine_cmds(peer_id, part_store, stats_out)
            .await;
    }

    fn refres_peer_replay_worker(&mut self, peer_id: PeerId) {
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
            let deets = TaskSeed::Machine(MachineTaskDeets::PeerReplay(PeerReplayTask {
                peer_id,
                parts: peer_state.cursors_for_peer_replay_worker_parts(replay_req_parts.iter()),
            }));
            let replay_task = self.tasks.spawn_task(deets);
            peer_state.replay_worker = Some(PeerReplayWorkerState {
                task_id: replay_task,
                parts: replay_req_parts,
            });
        }
    }

    pub fn handle_decide_peer_strat_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        DecidePeerStrategyTaskError { peer_id, deets }: DecidePeerStrategyTaskError,
        out: &mut Vec<SyncStatEvent>,
    ) {
        match deets {
            DecidePeerStrategyErrorDeets::ListError(_) | DecidePeerStrategyErrorDeets::Rpc(_) => {
                // noop, retry with backoff
                // TODO: consider narrowing part set
            }
        }

        let Some(peer_state) = self.peers.get_mut(&peer_id) else {
            assert!(self.all_seen_peer.contains(&peer_id), "fishy");
            return;
        };
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
                peer_state.parts.insert(
                    part_id,
                    PeerPartState {
                        strat: PeerPartStrategy::Pending(decide_task),
                    },
                );
            }
            self.refresh_peer_sync_state(peer_id, out);
        }
    }
}

// cursor support
impl BigSyncMachine {
    async fn handle_peer_replay_worker_msg<K: FutureForm, S: PartStore<K>>(
        &mut self,
        msg: PeerReplayWorkerMsg,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
    ) {
        let Some(peer_state) = self.peers.get_mut(&msg.peer_id) else {
            assert!(self.all_seen_peer.contains(&msg.peer_id), "fishy");
            return;
        };
        let mut cursors_strat = false;
        let Some(worker) = &peer_state.replay_worker else {
            // there is no worker in ba sing se
            return;
        };
        if worker.task_id != msg.task_id {
            // stale peer, ignore the message to avoid dupe events
            // from two peers
            return;
        }

        for (_part_id, part_state) in &peer_state.parts {
            match &part_state.strat {
                PeerPartStrategy::Pending(_) => {}
                PeerPartStrategy::Bucket(_strat) => {}
                PeerPartStrategy::Cursor(_strat) => cursors_strat = true,
            }
        }
        if cursors_strat {
            peer_state
                .cursor_machine
                .as_mut()
                .expect(ERROR_IMPOSSIBLE)
                .on_subscription_evt(msg.evt, &mut peer_state.cursors_cmd_buf);
        }
        self.drain_cursor_machine_cmds(msg.peer_id, part_store, stats_out)
            .await;
    }

    fn handle_peer_replay_worker_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        PeerReplayWorkerError { peer_id, deets }: PeerReplayWorkerError,
        out: &mut Vec<SyncStatEvent>,
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
                self.handle_set_peer_evt(SetPeerEvent { peer_id, parts }, out);
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
    }

    async fn drain_cursor_machine_cmds<K: FutureForm, S: PartStore<K>>(
        &mut self,
        peer_id: PeerId,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
    ) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        peer_state
            .bucket_cmd_buf
            .extend(peer_state.cursors_cmd_buf.drain(..).map(|cmd| match cmd {
                CursorMachineCommand::SyncObj {
                    obj_id,
                    remote_payload,
                    cursors,
                    part_hints,
                } => BucketMachineCommand::SyncObj {
                    obj_id,
                    cursors,
                    remote_payload: Some(remote_payload),
                    part_hints,
                },
                CursorMachineCommand::SetPartCursor { part_id, cursor } => {
                    BucketMachineCommand::SetPartCursor { part_id, cursor }
                }
                CursorMachineCommand::RemoveObjFromPart { obj_id, part_id } => {
                    BucketMachineCommand::RemoveObjFromPart { obj_id, part_id }
                }
            }));
        self.drain_bucket_machine_cmds(peer_id, part_store, stats_out)
            .await;
    }
}

// bucket support
impl BigSyncMachine {
    async fn drain_bucket_machine_cmds<K: FutureForm, S: PartStore<K>>(
        &mut self,
        peer_id: PeerId,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
    ) {
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        // NOTE: ordering is important here, execute commands
        // in given order
        let mut refresh_peer_replaly = false;
        for cmd in peer_state.bucket_cmd_buf.drain(..) {
            match cmd {
                BucketMachineCommand::SyncObj {
                    obj_id,
                    part_hints,
                    remote_payload: _remote_payload,
                    cursors,
                } => {
                    if let Some(worker) = peer_state.sync_workers.remove(&obj_id) {
                        let _state = self
                            .tasks
                            .stop_task(worker.task_id)
                            .expect(ERROR_UNRECONIZED);
                    }
                    let deets = SyncTaskDeets {
                        peer_id,
                        obj_id,
                        part_hints,
                    };
                    let task_id = self.tasks.spawn_task(TaskSeed::Sync(deets.clone()));
                    peer_state
                        .sync_workers
                        .insert(
                            obj_id,
                            SyncWorkerState {
                                task_id,
                                deets,
                                cursors,
                            },
                        );
                }
                BucketMachineCommand::SetPartCursor { part_id, cursor } => {
                    part_store
                        .set_peer_part_cursor(peer_id, part_id, cursor)
                        .await
                }
                BucketMachineCommand::RemoveObjFromPart { obj_id, part_id } => {
                    part_store.remove_obj_from_part(obj_id, part_id).await;
                    let parts = part_store.obj_parts(obj_id).await;
                    if parts.is_empty() {
                        // TODO: tell sync backend that it should go
                        info!("object is to go");
                    }
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
                    match &mut part.strat {
                        PeerPartStrategy::Bucket(state) => {
                            let task_id = self.tasks.spawn_task(TaskSeed::Machine(deets));
                            let old = state.active_list_tasks.insert(task_id, task);
                            assert!(old.is_none(), "fishy");
                        }
                        _ => unreachable!(),
                    }
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
                    match &mut part.strat {
                        PeerPartStrategy::Bucket(state) => {
                            let task_id = self.tasks.spawn_task(TaskSeed::Machine(deets));
                            let old = state.active_leaf_tasks.insert(task_id, task);
                            assert!(old.is_none(), "fishy");
                        }
                        _ => unreachable!(),
                    }
                }
                BucketMachineCommand::UpgradeToCursor { floor, part_id } => {
                    part_store
                        .set_peer_part_cursor(peer_id, part_id, floor)
                        .await;
                    let old = peer_state
                        .parts
                        .insert(
                            part_id,
                            PeerPartState {
                                strat: PeerPartStrategy::Cursor(CursorState { last_cursor: floor }),
                            },
                        )
                        .expect(ERROR_UNRECONIZED);
                    match old.strat {
                        PeerPartStrategy::Bucket(old) => {
                            for task_id in old
                                .active_leaf_tasks
                                .into_keys()
                                .chain(old.active_list_tasks.into_keys())
                            {
                                let _state =
                                    self.tasks.stop_task(task_id).expect(ERROR_UNRECONIZED);
                            }
                        }
                        _ => unreachable!(),
                    }
                    refresh_peer_replaly = true;
                }
            }
        }
        if refresh_peer_replaly {
            self.refres_peer_replay_worker(peer_id);
        }
        self.refresh_peer_sync_state(peer_id, stats_out);
    }

    async fn handle_list_buckets_result<K: FutureForm, S: PartStore<K>>(
        &mut self,
        task_id: TaskId,
        ListBucketsResult {
            peer_id,
            part_id,
            filtered_buckets,
        }: ListBucketsResult,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
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
        self.drain_bucket_machine_cmds(peer_id, part_store, stats_out)
            .await;
    }

    async fn handle_leaf_buckets_result<K: FutureForm, S: PartStore<K>>(
        &mut self,
        task_id: TaskId,
        LeafBucketsResult {
            peer_id,
            filtered_objs,
        }: LeafBucketsResult,
        part_store: &S,
        stats_out: &mut Vec<SyncStatEvent>,
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
                strat.machine.on_obj_page(filtered_objs, &mut bucket_cmd_buf);
                handled = true;
                break;
            }
        }
        if !handled {
            return;
        }
        let peer_state = self.peers.get_mut(&peer_id).expect(ERROR_UNRECONIZED);
        peer_state.bucket_cmd_buf.extend(bucket_cmd_buf);
        self.drain_bucket_machine_cmds(peer_id, part_store, stats_out)
            .await;
    }

    fn handle_list_buckets_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        ListBucketsTaskError {
            peer_id,
            part_id,
            deets: _,
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
            deets: _,
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

mod sync {
    use super::*;
    impl BigSyncMachine {
        pub async fn handle_sync_completed<K: FutureForm, S: PartStore<K>>(
            &mut self,
            evt: SyncCompletedEvent,
            part_store: &S,
            stats_out: &mut Vec<SyncStatEvent>,
        ) {
            let completion = evt.completion;
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.sync_workers.get(&completion.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }

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
            let task_uses_bucket_path = worker.deets.part_hints.iter().any(|part_id| {
                matches!(
                    peer_state.parts.get(part_id).map(|part| &part.strat),
                    Some(PeerPartStrategy::Bucket(_))
                )
            });
            if task_uses_bucket_path {
                for (&_part_id, part_state) in &mut peer_state.parts {
                    let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
                        continue;
                    };
                    strat
                        .machine
                        .on_obj_sync_completed(&completion, &mut peer_state.bucket_cmd_buf);
                }
                self.drain_bucket_machine_cmds(evt.peer_id, part_store, stats_out)
                    .await;
            } else {
                let Some(cursor_m) = &mut peer_state.cursor_machine else {
                    unreachable!("cursor completion without cursor machine");
                };
                cursor_m.on_obj_sync_job_evt(&completion, &mut peer_state.cursors_cmd_buf);
                self.drain_cursor_machine_cmds(evt.peer_id, part_store, stats_out)
                    .await;
            }
        }

        pub fn handle_sync_failed(&mut self, evt: SyncFailedEvent, retry: Retry) {
            let deets = {
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
                worker.deets.clone()
            };

            let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
                return;
            };
            if let Some(worker) = peer_state.sync_workers.get_mut(&evt.obj_id) {
                let task_id = self.tasks.spawn_delayed_task(
                    TaskSeed::Sync(deets),
                    retry,
                    Duration::from_secs(2),
                );
                worker.task_id = task_id;
            }
        }
    }
}
