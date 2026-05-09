// Needs:
// - pluggable PartititionStore,
// - pluggable SyncStore,
// - RPC protocol
// - peer handshake
//  - Decide strategy
mod interlude {
    pub use utils_rs::prelude::*;

    // FIXME: consider using indexed map instead
    pub use future_form::FutureForm;

    pub use std::collections::{HashMap as Map, HashSet as Set};

    pub use crate::{ObjId, PartId, PeerId};
}

use std::time::Instant;

use crate::{
    interlude::*,
    part_store::{PartitionStore, PeerPartCursors},
    rpc::BigSyncRpcClient,
};

mod cursor;
mod ids;
pub mod mpsc;
pub mod part_store;
use part_store::CursorIndex;
pub mod rpc;
pub use ids::*;

structstruck::strike! {
    pub enum BigSyncEvent {
        SetPeer (
            struct SetPeerEvent {
                peer_id: PeerId,
                /// Partitions to sync from the peer
                parts: Set<PartId>,
            }
        ),
        RemovePeer (
            struct RemovePeerEvent {
                peer_id: PeerId,
            }
        ),
        SyncCompleted (
            struct SyncCompletedEvent {
                task_id: TaskId,
                peer_id: PeerId,
                obj_id: ObjId,
                completion: SyncCompletion,
            }
        ),
        SyncFailed (
            struct SyncFailedEvent {
                task_id: TaskId,
                peer_id: PeerId,
                obj_id: ObjId,
                err: eyre::Report,
            }
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SyncTaskKind {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncTask {
    pub peer_id: PeerId,
    pub kind: SyncTaskKind,
    pub obj_id: ObjId,
    pub part_hints: Vec<PartId>,
}

impl From<cursor::CursorMachineCommand> for SyncTask {
    fn from(command: cursor::CursorMachineCommand) -> Self {
        let kind = match command.kind {
            cursor::ObjectSyncKind::New => SyncTaskKind::New,
            cursor::ObjectSyncKind::Change => SyncTaskKind::Change,
            cursor::ObjectSyncKind::Delete => SyncTaskKind::Delete,
        };
        Self {
            peer_id: command.peer_id,
            kind,
            obj_id: command.obj_id,
            part_hints: command.part_hints,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncCompletion {
    AddedMember {
        peer: PeerId,
        obj_id: ObjId,
        obj_payload: serde_json::Value,
    },
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

structstruck::strike! {
    pub enum BigSyncMsg {
        MachineTaskResult (struct {
            task_id: TaskId,
            deets: enum TaskResultDeets {
                SetPeerStrategy (pub struct SetPeerStrategy {
                    peer_id: PeerId,
                    part_strats: Map<PartId, enum PeerPartStratDecision{
                        Unkown,
                        Merkle(MerkleStrat),
                        Cursor(CursorStrat),
                    }>
                }),
            },
        }),
        MachineTaskError (struct {
            task_id: TaskId,
            deets: enum MachineTaskErrDeets{
                DecidePeerStrategyError(struct {
                    peer_id: PeerId,
                    deets: GenericTaskError
                })
                PeerReplayWorkerError(struct {
                    peer_id: PeerId,
                    deets:
                        enum PeerReplayWorkerErrorDeets {
                            #![derive(Debug, thiserror::Error, displaydoc::Display)]
                            /// StreamClosed
                            StreamClosed,
                            /// {0}
                            SubError(#[from] rpc::SubPartsError)
                            /// {0}
                            Rpc(#[from] rpc::RpcError),
                            /// {0}
                            MpscSend(#[from] mpsc::SendError),
                            /// {0}
                            MpscRecv(#[from] mpsc::RecvError),
                        }
                })
            },
        })
        PeerReplayWorker (struct PeerReplayWorkerMsg {
            task_id: TaskId,
            peer_id: PeerId,
            evt: rpc::SubEvent
        })
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
enum GenericTaskError {
    /// {0}
    Rpc(#[from] rpc::RpcError),
    /// {0}
    MpscSend(#[from] mpsc::SendError),
    /// {0}
    MpscRecv(#[from] mpsc::RecvError),
}

pub type CmdId = u64;

pub type TaskId = u64;
structstruck::strike! {
    pub struct Task {
        /// A task is a single threaded work that's enqueued
        /// by the sync machine and is supposed run concurrently
        /// to the main event loop
        pub id: TaskId,
        /// NOTE: host doesn't need to know the details
        pub deets: pub enum TaskDeets {
            SyncTask(SyncTask),
            MachineTask(pub enum MachineTask {
                DecidePeerStrategy (struct DecidePeerStrategyTask {
                    peer_id: PeerId,
                    parts: Set<PartId>
                })
                PeerReplay(struct PeerReplayTask {
                    peer_id: PeerId,
                    parts: Map<PartId, part_store::PeerPartCursors>
                })
            })
        }
    }
}

pub struct TaskCtx<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>> {
    pub task_id: TaskId,
    pub main_tx: mpsc::Sender<BigSyncMsg>,
    pub rpc_clients: Map<PeerId, R>,
    pub part_store: S,
    pub _phantom: std::marker::PhantomData<K>,
}

impl MachineTask {
    pub async fn run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: TaskCtx<K, S, R>,
    ) {
        let res = match self {
            Self::DecidePeerStrategy(inner) => inner
                .run(&cx)
                .await
                .map_err(|err| MachineTaskErrDeets::DecidePeerStrategyError(err)),
            Self::PeerReplay(inner) => inner
                .run(&cx)
                .await
                .map_err(|err| MachineTaskErrDeets::PeerReplayWorkerError(err)),
        };
        let msg = match res {
            Ok(deets) => BigSyncMsg::MachineTaskResult(MachineTaskResult {
                task_id: cx.task_id,
                deets,
            }),
            Err(deets) => BigSyncMsg::MachineTaskError(MachineTaskError {
                task_id: cx.task_id,
                deets,
            }),
        };
        cx.main_tx.send(msg).await.expect(ERROR_CHANNEL)
    }
}

impl DecidePeerStrategyTask {
    async fn run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: &TaskCtx<K, S, R>,
    ) -> Result<TaskResultDeets, DecidePeerStrategyError> {
        let peer_id = self.peer_id;
        self.run_run(cx)
            .await
            .map_err(|deets| DecidePeerStrategyError { peer_id, deets })
    }
    async fn run_run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: &TaskCtx<K, S, R>,
    ) -> Result<TaskResultDeets, GenericTaskError> {
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);

        let summary = peer_rpc
            .peer_summary(rpc::PeerSummaryRequest {
                parts: self.parts.clone(),
            })
            .await?;

        let mut part_strats: Map<_, _> = default();
        for part_id in self.parts {
            let Some(summary) = summary.parts.get(&part_id) else {
                part_strats.insert(part_id, PeerPartStratDecision::Unkown);
                continue;
            };
            let last_peer_cursor = cx
                .part_store
                .get_peer_part_cursor(self.peer_id, part_id)
                .await;
            let diff = summary.latest_cursor.abs_diff(
                last_peer_cursor
                    .member_cursor
                    .min(last_peer_cursor.obj_cursor),
            );
            const MERKLE_DIFF_THRESHOLD: u64 = 256;
            let strat = if diff > MERKLE_DIFF_THRESHOLD {
                // FIXME: merkle is not yet implemented
                PeerPartStratDecision::Cursor(CursorStrat {
                    latest_cursor: summary.latest_cursor,
                    since_member: last_peer_cursor.member_cursor,
                    since_obj: last_peer_cursor.obj_cursor,
                })
                // PeerPartStratDecision::Merkle(MerkleStrat {
                //     latest_cursor: summary.latest_cursor,
                //     since_member: last_peer_cursor.member_cursor,
                //     since_obj: last_peer_cursor.obj_cursor,
                // })
            } else {
                PeerPartStratDecision::Cursor(CursorStrat {
                    latest_cursor: summary.latest_cursor,
                    since_member: last_peer_cursor.member_cursor,
                    since_obj: last_peer_cursor.obj_cursor,
                })
            };
            part_strats.insert(part_id, strat);
        }
        Ok(TaskResultDeets::SetPeerStrategy(SetPeerStrategy {
            peer_id: self.peer_id,
            part_strats,
        }))
    }
}

impl PeerReplayTask {
    async fn run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: &TaskCtx<K, S, R>,
    ) -> Result<TaskResultDeets, PeerReplayWorkerError> {
        let peer_id = self.peer_id;
        self.run_run(cx)
            .await
            .map_err(|deets| PeerReplayWorkerError { peer_id, deets })
    }

    async fn run_run<K: FutureForm, S: PartitionStore<K>, R: BigSyncRpcClient<K>>(
        self,
        cx: &TaskCtx<K, S, R>,
    ) -> Result<TaskResultDeets, PeerReplayWorkerErrorDeets> {
        let peer_rpc = cx.rpc_clients.get(&self.peer_id).expect(ERROR_UNRECONIZED);
        let rx = peer_rpc
            .sub_parts(rpc::SubPartsRequest {
                parts: self
                    .parts
                    .into_iter()
                    .map(|(part_id, cursors)| rpc::PartitionStreamCursorRequest {
                        part_id,
                        cursors,
                    })
                    .collect(),
            })
            .await??;
        loop {
            let evt = rx.recv().await;
            match evt {
                Err(_) => {
                    return Err(PeerReplayWorkerErrorDeets::StreamClosed);
                }
                Ok(evt) => {
                    cx.main_tx
                        .send(BigSyncMsg::PeerReplayWorker(PeerReplayWorkerMsg {
                            peer_id: self.peer_id,
                            task_id: cx.task_id,
                            evt,
                        }))
                        .await?;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Retry {
    attempt_no: usize,
    backoff: Duration,
    queued_at: Instant,
}

struct TaskState {
    retry: Retry,
}

#[derive(Default)]
struct Tasks {
    next_id: TaskId,
    all: Map<TaskId, TaskState>,
    pending: Map<TaskId, (Task, Instant)>,
    spawn_queue: Vec<Task>,
    stop_queue: Set<TaskId>,
}

impl Tasks {
    fn stop_task(&mut self, id: TaskId) {
        if self.pending.remove(&id).is_some() {
            // we only remove the sttate if the
            // task isn't alive
            self.all.remove(&id);
            return;
        }
        self.spawn_queue.retain(|task| task.id != id);
        self.stop_queue.insert(id);
    }

    fn spawn_task(&mut self, deets: TaskDeets) -> TaskId {
        let id = self.next_id;
        self.next_id += 1;
        self.all.insert(
            id,
            TaskState {
                retry: Retry {
                    attempt_no: 1,
                    backoff: default(),
                    queued_at: std::time::Instant::now(),
                },
            },
        );
        self.spawn_queue.push(Task { id, deets });
        id
    }

    #[must_use]
    fn spawn_delayed_task(
        &mut self,
        deets: TaskDeets,
        prev_retry: Retry,
        min_delay: Duration,
    ) -> TaskId {
        const MAX_BACKOFF: Duration = Duration::from_mins(10);
        let backoff = if prev_retry.backoff.is_zero() {
            min_delay.min(MAX_BACKOFF)
        } else {
            prev_retry
                .backoff
                .saturating_mul(2)
                .max(min_delay)
                .min(MAX_BACKOFF)
        };
        let now = std::time::Instant::now();
        let retry = Retry {
            attempt_no: prev_retry.attempt_no + 1,
            queued_at: now,
            backoff,
        };
        let due_at = retry.queued_at + retry.backoff;

        let id = self.next_id;
        self.next_id += 1;
        self.all.insert(id, TaskState { retry });
        self.pending.insert(id, (Task { id, deets }, due_at));
        id
    }

    fn enqueue_due_tasks(&mut self, now: Instant) {
        let due_task_ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(task_id, (_, due_at))| (*due_at <= now).then_some(*task_id))
            .collect();
        for task_id in due_task_ids {
            let Some((task, _)) = self.pending.remove(&task_id) else {
                continue;
            };
            self.spawn_queue.push(task);
        }
    }

    fn replace_task(&mut self, task_id: TaskId, deets: TaskDeets) -> bool {
        if let Some((task, _due_at)) = self.pending.get_mut(&task_id) {
            *task = Task { id: task_id, deets };
            return true;
        }
        if let Some(task) = self.spawn_queue.iter_mut().find(|task| task.id == task_id) {
            task.deets = deets;
            return true;
        }
        false
    }

    pub fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, Task> {
        self.spawn_queue.drain(..)
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, u64> {
        self.stop_queue.drain()
    }
}

structstruck::strike! {
    struct PeerState {
        fully_synced_parts: Set<PartId>,
        sync_workers: Map<ObjId, struct SyncWorkerState {
            task_id: TaskId,
            task: SyncTask,
        }>,
        replay_worker: Option<struct PeerReplayWorkerState {
            task_id: TaskId,
            parts: Set<PartId>,
        }>,
        parts: Map<PartId, struct PeerPartState {
            strat: enum PeerPartStrategy {
                Pending(TaskId),
                Merkle(struct MerkleStrat {
                    #![derive(PartialEq, Eq)]
                    latest_cursor: CursorIndex,
                }),
                Cursor(struct CursorStrat {
                    #![derive(PartialEq, Eq)]
                    latest_cursor: CursorIndex,
                    since_member: CursorIndex,
                    since_obj: CursorIndex,
                }),
            }
        }>
    }
}

impl PeerState {
    fn cursors_for_peer_replay_worker_parts<'a>(
        &self,
        parts: impl std::iter::Iterator<Item = &'a PartId>,
    ) -> Map<PartId, PeerPartCursors> {
        parts
            .map(|&part_id| {
                match self.parts.get(&part_id).expect(ERROR_IMPOSSIBLE).strat {
                    PeerPartStrategy::Pending(_) => unreachable!(),
                    PeerPartStrategy::Merkle(MerkleStrat { latest_cursor, .. }) => (
                        part_id,
                        PeerPartCursors {
                            // on merkle, start the replay since the latest
                            // on a focus on live events
                            member_cursor: latest_cursor,
                            obj_cursor: latest_cursor,
                        },
                    ),
                    PeerPartStrategy::Cursor(CursorStrat {
                        since_member,
                        since_obj,
                        ..
                    }) => (
                        part_id,
                        PeerPartCursors {
                            member_cursor: since_member,
                            obj_cursor: since_obj,
                        },
                    ),
                }
            })
            .collect()
    }
}

#[derive(Default)]
pub struct BigSyncMachine {
    all_seen_peer: Set<PeerId>,
    peers: Map<PeerId, PeerState>,

    cursor_machine: cursor::CursorSyncMachine,
    cursors_cmd_buf: Vec<cursor::CursorMachineCommand>,

    tasks: Tasks,
}

impl BigSyncMachine {
    pub fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, Task> {
        self.tasks.drain_spawn_queue()
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, u64> {
        self.tasks.drain_stop_queue()
    }

    pub async fn handle_evt<K: FutureForm, S: PartitionStore<K>>(
        &mut self,
        evt: BigSyncEvent,
        part_store: &S,
    ) {
        match evt {
            BigSyncEvent::SetPeer(evt) => self.handle_set_peer_evt(evt),
            BigSyncEvent::RemovePeer(evt) => self.handle_remove_peer_evt(evt),
            BigSyncEvent::SyncCompleted(evt) => {
                self.tasks.stop_task(evt.task_id);
                let _state = self
                    .tasks
                    .all
                    .remove(&evt.task_id)
                    .expect(ERROR_UNRECONIZED);
                self.handle_sync_completed(evt, part_store).await;
            }
            BigSyncEvent::SyncFailed(evt) => {
                self.tasks.stop_task(evt.task_id);
                let state = self
                    .tasks
                    .all
                    .remove(&evt.task_id)
                    .expect(ERROR_UNRECONIZED);
                self.handle_sync_failed(evt, state.retry);
            }
        }
    }

    pub fn handle_tick(&mut self, now: Instant) {
        self.tasks.enqueue_due_tasks(now);
    }

    pub async fn handle_msg<K: FutureForm, S: PartitionStore<K>>(
        &mut self,
        msg: BigSyncMsg,
        part_store: &S,
    ) {
        match msg {
            BigSyncMsg::MachineTaskResult(MachineTaskResult { task_id, deets }) => {
                self.tasks.stop_task(task_id);
                let state = self.tasks.all.remove(&task_id).expect(ERROR_UNRECONIZED);
                match deets {
                    TaskResultDeets::SetPeerStrategy(evt) => {
                        self.handle_set_peer_strat(task_id, state.retry, evt)
                    }
                }
            }
            BigSyncMsg::MachineTaskError(MachineTaskError { task_id, deets }) => {
                self.tasks.stop_task(task_id);
                let state = self.tasks.all.remove(&task_id).expect(ERROR_UNRECONIZED);
                match deets {
                    MachineTaskErrDeets::DecidePeerStrategyError(err) => {
                        self.handle_decide_peer_strat_err(task_id, state.retry, err)
                    }
                    MachineTaskErrDeets::PeerReplayWorkerError(err) => {
                        self.handle_peer_replay_worker_err(task_id, state.retry, err)
                    }
                }
            }
            BigSyncMsg::PeerReplayWorker(msg) => {
                self.handle_peer_replay_worker_msg(msg, part_store).await
            }
        }
    }

    async fn handle_peer_replay_worker_msg<K: FutureForm, S: PartitionStore<K>>(
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
                PeerPartStrategy::Merkle(_strat) => todo!(),
                PeerPartStrategy::Cursor(_strat) => cursors_strat = true,
            }
        }
        if cursors_strat {
            self.cursor_machine
                .on_subscription_evt(msg.peer_id, msg.evt, part_store, &mut self.cursors_cmd_buf)
                .await;
        }
        self.dispatch_cursor_cmds();
    }

    fn dispatch_cursor_cmds(&mut self) {
        while let Some(command) = self.cursors_cmd_buf.pop() {
            self.dispatch_cursor_cmd(command);
        }
    }

    fn dispatch_cursor_cmd(&mut self, cmd: cursor::CursorMachineCommand) {
        let task: SyncTask = cmd.into();
        let Some(peer_state) = self.peers.get_mut(&task.peer_id) else {
            assert!(self.all_seen_peer.contains(&task.peer_id), "fishy");
            return;
        };
        if let Some(worker) = peer_state.sync_workers.get_mut(&task.obj_id) {
            worker.task = task.clone();
            let _ = self
                .tasks
                .replace_task(worker.task_id, TaskDeets::SyncTask(task));
            return;
        }
        let task_id = self.tasks.spawn_task(TaskDeets::SyncTask(task.clone()));
        peer_state
            .sync_workers
            .insert(task.obj_id, SyncWorkerState { task_id, task });
    }

    fn handle_sync_failed(&mut self, evt: SyncFailedEvent, retry: Retry) {
        warn!(
            peer_id = ?evt.peer_id,
            obj_id = ?evt.obj_id,
            task_id = evt.task_id,
            err = ?evt.err,
            "sync task failed"
        );
        let task = {
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
            worker.task.clone()
        };

        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        if let Some(worker) = peer_state.sync_workers.get_mut(&evt.obj_id) {
            let task_id = self.tasks.spawn_delayed_task(
                TaskDeets::SyncTask(task),
                retry,
                Duration::from_secs(2),
            );
            worker.task_id = task_id;
        }
    }

    async fn handle_sync_completed<K: FutureForm, S: PartitionStore<K>>(
        &mut self,
        evt: SyncCompletedEvent,
        part_store: &S,
    ) {
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

        let Some(peer_state) = self.peers.get_mut(&evt.peer_id) else {
            return;
        };
        peer_state.sync_workers.remove(&evt.obj_id);
        let mut cursor_out = Vec::new();
        self.cursor_machine
            .on_obj_sync_completed(evt.completion, part_store, &mut cursor_out)
            .await
            .expect(ERROR_IMPOSSIBLE);
        self.cursors_cmd_buf.extend(cursor_out);
        self.dispatch_cursor_cmds();
    }

    fn handle_decide_peer_strat_err(
        &mut self,
        task_id: TaskId,
        retry: Retry,
        DecidePeerStrategyError { peer_id, deets }: DecidePeerStrategyError,
    ) {
        match deets {
            GenericTaskError::MpscSend(_) => unreachable!(),
            GenericTaskError::Rpc(_) | GenericTaskError::MpscRecv(_) => {
                // noop
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
                PeerPartStrategy::Merkle(_) | PeerPartStrategy::Cursor(_) => {}
            };
        }
        if !parts_retry.is_empty() {
            let deets =
                TaskDeets::MachineTask(MachineTask::DecidePeerStrategy(DecidePeerStrategyTask {
                    peer_id,
                    parts: parts_retry.clone(),
                }));
            let decide_task = self
                .tasks
                .spawn_delayed_task(deets, retry, Duration::from_secs(2));
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
            PeerReplayWorkerErrorDeets::SubError(rpc::SubPartsError::UnkownParts {
                unkown_parts,
            }) => {
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
        let deets = TaskDeets::MachineTask(MachineTask::PeerReplay(PeerReplayTask {
            peer_id,
            parts: peer_state.cursors_for_peer_replay_worker_parts(worker.parts.iter()),
        }));
        let replay_task = self
            .tasks
            .spawn_delayed_task(deets, retry, Duration::from_secs(2));
        peer_state.replay_worker = Some(PeerReplayWorkerState {
            task_id: replay_task,
            parts: worker.parts.clone(),
        });
    }

    fn handle_set_peer_evt(&mut self, SetPeerEvent { peer_id, parts }: SetPeerEvent) {
        // clear out everything, avoid reuising any old state
        // treating SetPeer as a refresh peer cmd in a way
        self.handle_remove_peer_evt(RemovePeerEvent { peer_id });
        self.all_seen_peer.insert(peer_id);
        let deets =
            TaskDeets::MachineTask(MachineTask::DecidePeerStrategy(DecidePeerStrategyTask {
                peer_id,
                parts: parts.clone(),
            }));
        let decide_task = self.tasks.spawn_task(deets);
        self.peers.insert(
            peer_id,
            PeerState {
                fully_synced_parts: default(),
                sync_workers: default(),
                replay_worker: default(),
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
                self.tasks.stop_task(worker.task_id);
            }
            let mut clear_cursor_machine_state = false;
            for (_old_part_id, state) in old.parts {
                match state.strat {
                    PeerPartStrategy::Pending(_) => {}
                    PeerPartStrategy::Merkle(_strat) => todo!(),
                    PeerPartStrategy::Cursor(_strat) => {
                        clear_cursor_machine_state = true;
                    }
                }
            }
            if clear_cursor_machine_state {
                self.cursor_machine.clear_peer(peer_id);
            }
            if let Some(replay_worker) = old.replay_worker {
                self.tasks.stop_task(replay_worker.task_id);
            }
        }
    }

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
        let mut parts_retry = Set::new();

        for (part_id, decision) in part_strats {
            let old = peer_state.parts.remove(&part_id);
            let strat = match decision {
                PeerPartStratDecision::Unkown => {
                    parts_retry.insert(part_id);
                    continue;
                }
                PeerPartStratDecision::Merkle(_strat) => todo!(),
                PeerPartStratDecision::Cursor(strat) => PeerPartStrategy::Cursor(strat),
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
        if !parts_retry.is_empty() {
            let deets =
                TaskDeets::MachineTask(MachineTask::DecidePeerStrategy(DecidePeerStrategyTask {
                    peer_id,
                    parts: parts_retry.clone(),
                }));
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
        {
            let replay_req_parts: Set<_> = peer_state
                .parts
                .iter()
                .filter_map(|(&part_id, state)| match state.strat {
                    PeerPartStrategy::Pending(_) => None,
                    PeerPartStrategy::Merkle(_) | PeerPartStrategy::Cursor(_) => Some(part_id),
                })
                .collect();
            let refresh = match &peer_state.replay_worker {
                Some(old_state) => {
                    let refresh = replay_req_parts != old_state.parts;
                    if refresh {
                        self.tasks.stop_task(old_state.task_id);
                    }
                    refresh
                }
                None => true,
            };
            if refresh {
                let deets = TaskDeets::MachineTask(MachineTask::PeerReplay(PeerReplayTask {
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
    }
}
