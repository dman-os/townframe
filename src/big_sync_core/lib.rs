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
use crate::tasks::leaf_buckets::LeafBucketsTask;
use crate::tasks::list_bucket::ListBucketsTask;

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
        SyncCompleted (
            pub struct SyncCompletedEvent {
                pub task_id: TaskId,
                pub peer_id: PeerId,
                pub completion: SyncJobEvt,
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
    pub struct SyncJobEvt {
        pub obj_id: ObjId,
        pub cursors: Vec<CursorIndex>,
        pub deets: pub enum SyncCompletionDeets {
            #![derive(Debug, Clone, PartialEq, Eq)]
            AddedMember,
            ChangedObject,
            Noop,
            // DeletedObj
        }
    }
}

structstruck::strike! {
    struct PeerState {
        fully_synced_parts: Set<PartId>,
        sync_workers: Map<ObjId, struct SyncWorkerState {
            task_id: TaskId,
            deets: SyncTaskDeets,
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
                    active_list_tasks: Set<TaskId>,
                    active_leaf_tasks: Set<TaskId>,
                }),
                Cursor(struct CursorState {
                    last_cursor: CursorIndex,
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
    ) {
        match evt {
            BigSyncEvent::SetPeer(evt) => self.handle_set_peer_evt(evt),
            BigSyncEvent::RemovePeer(evt) => self.handle_remove_peer_evt(evt),
            BigSyncEvent::SyncCompleted(evt) => {
                let _state = self.tasks.stop_task(evt.task_id).expect(ERROR_UNRECONIZED);
                self.handle_sync_completed(evt, part_store).await;
            }
            BigSyncEvent::SyncFailed(evt) => {
                let state = self.tasks.stop_task(evt.task_id).expect(ERROR_UNRECONIZED);
                self.handle_sync_failed(evt, state.retry);
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
    ) {
        match msg {
            MachineTaskMsg::MachineTaskResult(MachineTaskResult { task_id, deets }) => {
                let state = self.tasks.stop_task(task_id).expect(ERROR_UNRECONIZED);
                match deets {
                    TaskResultDeets::SetPeerStrategy(evt) => {
                        self.handle_set_peer_strat(task_id, state.retry, evt, part_store)
                            .await
                    }
                    TaskResultDeets::ListBuckets(list_buckets_result) => todo!(),
                    TaskResultDeets::LeafBuckets(leaf_buckets_result) => todo!(),
                }
            }
            MachineTaskMsg::MachineTaskError(MachineTaskError { task_id, deets }) => {
                let state = self.tasks.stop_task(task_id).expect(ERROR_UNRECONIZED);
                match deets {
                    MachineTaskErrDeets::DecidePeerStrategyError(err) => {
                        self.handle_decide_peer_strat_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::PeerReplayWorkerError(err) => {
                        self.handle_peer_replay_worker_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::ListBucketsError(err) => todo!(),
                    MachineTaskErrDeets::LeafBucketsError(err) => todo!(),
                }
            }
            MachineTaskMsg::PeerReplayWorker(msg) => {
                self.handle_peer_replay_worker_msg(msg, part_store).await
            }
        }
    }
}

// peer support
impl BigSyncMachine {
    pub fn handle_set_peer_evt(&mut self, SetPeerEvent { peer_id, parts }: SetPeerEvent) {
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
                fully_synced_parts: default(),
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
                            .into_iter()
                            .chain(strat.active_list_tasks.into_iter())
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

    async fn handle_set_peer_strat<K: FutureForm, S: PartStore<K>>(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        SetPeerStrategy {
            peer_id,
            part_strats,
        }: SetPeerStrategy,
        part_store: &S,
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
        self.drain_bucket_machine_cmds(peer_id, part_store).await;
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
        }
    }
}

// cursor support
impl BigSyncMachine {
    async fn handle_peer_replay_worker_msg<K: FutureForm, S: PartStore<K>>(
        &mut self,
        msg: PeerReplayWorkerMsg,
        part_store: &S,
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
        self.drain_cursor_machine_cmds(msg.peer_id, part_store)
            .await;
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
    }

    async fn drain_cursor_machine_cmds<K: FutureForm, S: PartStore<K>>(
        &mut self,
        peer_id: PeerId,
        part_store: &S,
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
        self.drain_bucket_machine_cmds(peer_id, part_store).await;
    }
}

// bucket support
impl BigSyncMachine {
    async fn drain_bucket_machine_cmds<K: FutureForm, S: PartStore<K>>(
        &mut self,
        peer_id: PeerId,
        part_store: &S,
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
                    remote_payload,
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
                        .insert(obj_id, SyncWorkerState { task_id, deets });
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
                    let deets = MachineTaskDeets::ListBuckets(ListBucketsTask {
                        peer_id,
                        part_id,
                        offset,
                        since,
                        working_level,
                    });
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    match &mut part.strat {
                        PeerPartStrategy::Bucket(state) => {
                            let task_id = self.tasks.spawn_task(TaskSeed::Machine(deets));
                            state.active_list_tasks.insert(task_id);
                        }
                        _ => unreachable!(),
                    }
                }
                BucketMachineCommand::LeafBuckets {
                    since,
                    buckets,
                    part_id,
                } => {
                    let deets = MachineTaskDeets::LeafBuckets(LeafBucketsTask {
                        peer_id,
                        part_id,
                        since,
                        buckets,
                    });
                    let part = peer_state.parts.get_mut(&part_id).expect(ERROR_UNRECONIZED);
                    match &mut part.strat {
                        PeerPartStrategy::Bucket(state) => {
                            let task_id = self.tasks.spawn_task(TaskSeed::Machine(deets));
                            state.active_list_tasks.insert(task_id);
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
                                .into_iter()
                                .chain(old.active_list_tasks.into_iter())
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
    }
}

mod sync {
    use super::*;
    impl BigSyncMachine {
        pub async fn handle_sync_completed<K: FutureForm, S: PartStore<K>>(
            &mut self,
            evt: SyncCompletedEvent,
            part_store: &S,
        ) {
            let Some(peer_state) = self.peers.get(&evt.peer_id) else {
                assert!(self.all_seen_peer.contains(&evt.peer_id), "fishy");
                return;
            };
            let Some(worker) = peer_state.sync_workers.get(&evt.completion.obj_id) else {
                return;
            };
            if worker.task_id != evt.task_id {
                return;
            }

            let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
                return;
            };
            if evt.completion.cursors.is_empty() {
                if let Some(cursor_m) = &mut peer_state.cursor_machine {
                    cursor_m.on_obj_sync_job_evt(&evt.completion, &mut peer_state.cursors_cmd_buf);
                }
                self.drain_cursor_machine_cmds(evt.peer_id, part_store)
                    .await;
            } else {
                for (&_part_id, part_state) in &mut peer_state.parts {
                    let PeerPartStrategy::Bucket(strat) = &mut part_state.strat else {
                        continue;
                    };
                    strat
                        .machine
                        .on_obj_sync_completed(&evt.completion, &mut peer_state.bucket_cmd_buf);
                }
                self.drain_bucket_machine_cmds(evt.peer_id, part_store)
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
