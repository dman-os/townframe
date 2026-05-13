mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use big_sync_core::{
    mpsc, BigSyncEvent, BigSyncMachine, BigSyncMsg, PartId, PeerId, SyncCompletion, SyncTask,
    SyncTaskDeets, TaskCtx, TaskId,
};

#[cfg(test)]
use big_sync_core::TaskCounts;

mod part_store;
mod rpc;
#[cfg(test)]
mod test;
mod trap;

use crate::interlude::*;

#[derive(Clone)]
pub struct BigSyncWorkerHandle {
    host_tx: tokio::sync::mpsc::Sender<BigSyncWorkerMsg>,
}

type SharedPartitionStore = Arc<dyn part_store::HostPartitionStore>;
type SharedPeerRpcClient = Arc<dyn rpc::HostBigRpcClient>;
type SharedRpcClients = Arc<std::sync::Mutex<HashMap<PeerId, SharedPeerRpcClient>>>;

#[derive(Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
pub enum BigSyncWorkerError {
    /// Unkown backend {backend_id} set for part {part_id}
    UnknownBackend {
        backend_id: BackendId,
        part_id: PartId,
    },
}

structstruck::strike! {
    enum BigSyncWorkerMsg {
        SetPeer {
            peer_id: PeerId,
            client: SharedPeerRpcClient,
            /// Partitions to sync from the peer
            parts: HashMap<PartId, BackendId>,
            resp: tokio::sync::oneshot::Sender<Result<(), BigSyncWorkerError>>
        },
        RemovePeer {
            peer_id: PeerId,
            resp: tokio::sync::oneshot::Sender<()>,
        },
        #[cfg(test)]
        Snapshot {
            resp: tokio::sync::oneshot::Sender<WorkerSnapshot>,
        },
    }
}

#[async_trait]
pub trait SyncBackend: Send + Sync + 'static {
    async fn run(&self, task: SyncTaskDeets) -> Res<Vec<SyncCompletion>>;
}

pub type BackendId = u64;

pub struct StopToken {
    pub cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSnapshot {
    pub peer_parts: HashMap<PeerId, HashMap<PartId, BackendId>>,
    pub task_counts: TaskCounts,
    pub active_machine_tasks: usize,
    pub active_sync_tasks: usize,
}

#[cfg(test)]
impl WorkerSnapshot {
    pub fn is_idle(&self) -> bool {
        self.task_counts.pending == 0
            && self.task_counts.sync_spawn_queue == 0
            && self.task_counts.machine_spawn_queue == 0
            && self.task_counts.stop_queue == 0
            && self.active_sync_tasks == 0
    }
}

impl StopToken {
    pub async fn stop(self) -> Result<(), utils_rs::WaitOnHandleError> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(
            self.join_handle,
            utils_rs::scale_timeout(Duration::from_secs(5)),
        )
        .await
    }
}

impl BigSyncWorkerHandle {
    pub async fn set_peer(
        &self,
        peer_id: PeerId,
        client: Arc<dyn rpc::HostBigRpcClient>,
        parts: HashMap<PartId, BackendId>,
    ) -> Res<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.host_tx
            .send(BigSyncWorkerMsg::SetPeer {
                peer_id,
                client,
                parts,
                resp: resp_tx,
            })
            .await
            .wrap_err(ERROR_CHANNEL)?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)??;
        Ok(())
    }

    pub async fn remove_peer(&self, peer_id: PeerId) -> Res<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.host_tx
            .send(BigSyncWorkerMsg::RemovePeer {
                peer_id,
                resp: resp_tx,
            })
            .await
            .wrap_err(ERROR_CHANNEL)?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn snapshot(&self) -> Res<WorkerSnapshot> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.host_tx
            .send(BigSyncWorkerMsg::Snapshot { resp: resp_tx })
            .await
            .wrap_err(ERROR_CHANNEL)?;
        Ok(resp_rx.await.wrap_err(ERROR_CHANNEL)?)
    }

    #[cfg(test)]
    pub async fn wait_for_idle(&self, timeout: Duration) -> Res<()> {
        let deadline = std::time::Instant::now() + timeout;
        let mut last_snapshot = None;
        loop {
            let snapshot = self.snapshot().await?;
            if snapshot.is_idle() {
                if last_snapshot.as_ref().is_some_and(|prev| prev == &snapshot) {
                    return Ok(());
                }
                last_snapshot = Some(snapshot);
            } else {
                last_snapshot = None;
            }

            if std::time::Instant::now() >= deadline {
                return Err(ferr!(
                    "timed out waiting for big_sync worker to become idle"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub fn spawn_big_sync_worker(
    part_store: SharedPartitionStore,
    sync_backends: HashMap<BackendId, Arc<dyn SyncBackend>>,
) -> Res<(BigSyncWorkerHandle, StopToken)> {
    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();

    let machine = big_sync_core::BigSyncMachine::default();
    let (host_tx, host_rx) = tokio::sync::mpsc::channel(64);
    let (sync_tx, sync_rx) = mpsc::bounded(64, "SyncWorkers".into(), "BigSyncMachine".into());
    let (task_tx, task_rx) = mpsc::bounded(64, "BigSync tasks".into(), "BigSyncMachine".into());
    let mut worker = BigSyncWorker {
        cancel_token: cancel_token.clone(),
        task_set,
        part_store,
        sync_backends,
        machine,

        host_rx,
        sync_tx,
        sync_rx,
        task_tx,
        task_rx,

        rpc_clients: default(),
        tasks: default(),
        sync_tasks: default(),
        peers: default(),
    };
    let fut = {
        let cancel_token = cancel_token.clone();
        async move {
            let shutdown = BigRedToken {
                err: default(),
                master_cancel: cancel_token.clone(),
            };
            let shutdown = Arc::new(shutdown);
            let maybe_res = cancel_token
                .run_until_cancelled(worker.machine_loop(Arc::clone(&shutdown)))
                .await;

            for (_id, task) in worker.tasks.drain() {
                task.cancel_token.cancel();
                task.handle
                    .join(Duration::from_secs(2))
                    .await
                    .wrap_err("error joining task")
                    .inspect_err(|err| error!(?err))
                    .ok();
            }
            let sync_tasks = std::mem::take(&mut worker.sync_tasks);
            for (_id, task) in sync_tasks {
                task.cancel_token.cancel();
                task.handle
                    .join(Duration::from_secs(2))
                    .await
                    .wrap_err("error joining task")
                    .inspect_err(|err| error!(?err))
                    .ok();
            }
            if let Some(Err(err)) = maybe_res {
                return Err(err);
            }
            if let Some(shutdown) = Arc::into_inner(shutdown) {
                if let Some(res) = shutdown.err.into_inner() {
                    return Err(res);
                }
            }
            Ok(())
        }
    };
    let join_handle = tokio::task::spawn(async move { fut.await.unwrap() }.in_current_span());

    Ok((
        BigSyncWorkerHandle { host_tx },
        StopToken {
            cancel_token,
            join_handle,
        },
    ))
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct BigRedToken {
    #[educe(Debug(ignore))]
    err: tokio::sync::OnceCell<eyre::Report>,
    #[educe(Debug(ignore))]
    master_cancel: CancellationToken,
}

impl BigRedToken {
    fn set_err(&self, err: eyre::Report) {
        let _ = self.err.set(err);
        self.master_cancel.cancel();
    }
}

struct BigSyncWorker {
    cancel_token: CancellationToken,

    task_set: utils_rs::AbortableJoinSet,
    sync_backends: HashMap<BackendId, Arc<dyn SyncBackend>>,

    host_rx: tokio::sync::mpsc::Receiver<BigSyncWorkerMsg>,
    machine: BigSyncMachine,

    sync_rx: mpsc::Receiver<BigSyncEvent>,
    sync_tx: mpsc::Sender<BigSyncEvent>,
    task_rx: mpsc::Receiver<BigSyncMsg>,
    task_tx: mpsc::Sender<BigSyncMsg>,

    tasks: HashMap<TaskId, TaskDeets>,
    peers: HashMap<PeerId, PeerState>,
    sync_tasks: HashMap<TaskId, ActiveSyncTaskDeets>,
    rpc_clients: SharedRpcClients,
    part_store: SharedPartitionStore,
}

struct PeerState {
    parts: HashMap<PartId, BackendId>,
}

struct TaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

struct ActiveSyncTaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

const MAX_ACTIVE_SYNC_TASKS: usize = 32;

enum LoopAction {
    Cont,
    Break,
}
impl BigSyncWorker {
    async fn machine_loop(&mut self, shutdown: Arc<BigRedToken>) -> Res<()> {
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        let (trap, mut err_rx) = trap::TaskTrap::new();
        loop {
            let part_store = trap::TrappedPartStore {
                trap: trap.clone(),
                inner: Arc::clone(&self.part_store),
            };
            let loop_action = tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    LoopAction::Break
                }
                res = err_rx.recv() => {
                    let err = res.expect(ERROR_IMPOSSIBLE);
                    return Err(err);
                },
                msg = self.task_rx.recv() => {
                    let msg = msg.expect(ERROR_CALLER);
                    run_until_cancelled_or_trapped(
                        &self.cancel_token,
                        &mut err_rx,
                        self.machine.handle_msg(msg, &part_store)
                    ).await?
                }
                evt = self.sync_rx.recv() => {
                    let evt = evt.expect(ERROR_CALLER);
                    run_until_cancelled_or_trapped(
                        &self.cancel_token,
                        &mut err_rx,
                        self.machine.handle_evt(evt, &part_store)
                    ).await?
                }
                cmd = self.host_rx.recv() => {
                    let msg = cmd.expect(ERROR_CALLER);
                    self.handle_msg(msg, &mut err_rx, &part_store).await?
                }
                _ = janitor_tick.tick() => {
                    self.machine.handle_tick(std::time::Instant::now());
                    LoopAction::Cont
                }
            };
            if matches!(loop_action, LoopAction::Break) {
                break;
            }

            self.batch_stop_tasks().await?;
            while let Some(task) = self.machine.pop_machine_spawn_queue() {
                self.spawn_machine_task(task, Arc::clone(&shutdown)).await?;
            }

            while self.sync_tasks.len() < MAX_ACTIVE_SYNC_TASKS {
                let Some(task) = self.machine.pop_sync_spawn_queue() else {
                    break;
                };
                self.spawn_sync_task(task).await?;
            }
        }
        Ok(())
    }

    async fn handle_msg(
        &mut self,
        msg: BigSyncWorkerMsg,
        err_rx: &mut tokio::sync::mpsc::Receiver<eyre::Report>,
        part_store: &trap::TrappedPartStore,
    ) -> Res<LoopAction> {
        let evt = match msg {
            BigSyncWorkerMsg::SetPeer {
                peer_id,
                client,
                parts,
                resp,
            } => {
                for (&part_id, &backend_id) in &parts {
                    if !self.sync_backends.contains_key(&backend_id) {
                        resp.send(Err(BigSyncWorkerError::UnknownBackend {
                            backend_id,
                            part_id,
                        }))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                        return Ok(LoopAction::Cont);
                    }
                }
                self.rpc_clients
                    .lock()
                    .expect(ERROR_MUTEX)
                    .insert(peer_id, client);
                self.peers.insert(
                    peer_id,
                    PeerState {
                        parts: parts.clone(),
                    },
                );
                let evt = BigSyncEvent::SetPeer(big_sync_core::SetPeerEvent {
                    peer_id: peer_id,
                    parts: parts.into_keys().collect(),
                });
                let _ = resp.send(Ok(()));
                evt
            }
            BigSyncWorkerMsg::RemovePeer { peer_id, resp } => {
                self.peers.remove(&peer_id);
                let evt = BigSyncEvent::RemovePeer(big_sync_core::RemovePeerEvent { peer_id });
                let _ = resp.send(());
                evt
            }
            #[cfg(test)]
            BigSyncWorkerMsg::Snapshot { resp } => {
                let snapshot = WorkerSnapshot {
                    peer_parts: self
                        .peers
                        .iter()
                        .map(|(&peer_id, peer_state)| (peer_id, peer_state.parts.clone()))
                        .collect(),
                    task_counts: self.machine.task_counts(),
                    active_machine_tasks: self.tasks.len(),
                    active_sync_tasks: self.sync_tasks.len(),
                };
                let _ = resp.send(snapshot);
                return Ok(LoopAction::Cont);
            }
        };
        run_until_cancelled_or_trapped(
            &self.cancel_token,
            err_rx,
            self.machine.handle_evt(evt, part_store),
        )
        .await
    }

    async fn batch_stop_tasks(&mut self) -> Res<()> {
        // NOTE: we exclusivley rely on the machine's
        // internal task tracking to clean up tasks
        for task_id in self.machine.drain_stop_queue() {
            if let Some(task) = self.tasks.remove(&task_id) {
                task.cancel_token.cancel();
                task.handle
                    .join(Duration::from_secs(2))
                    .await
                    .inspect_err(|err| error!(?err))
                    .ok();
                continue;
            }
            if let Some(task) = self.sync_tasks.remove(&task_id) {
                task.cancel_token.cancel();
                task.handle
                    .join(Duration::from_secs(2))
                    .await
                    .inspect_err(|err| error!(?err))
                    .ok();
            }
        }
        Ok(())
    }

    async fn spawn_machine_task(
        &mut self,
        task: big_sync_core::MachineTask,
        shutdown: Arc<BigRedToken>,
    ) -> Res<()> {
        let cancel_token = self.cancel_token.child_token();
        let task_id = task.id;
        let worker = MachineTaskWorker {
            task,
            part_store: Arc::clone(&self.part_store),
            rpc_clients: Arc::clone(&self.rpc_clients),
            bsm_tx: self.task_tx.clone(),
            cancel_token: self.cancel_token.child_token(),
            shutdown,
        };
        let handle = self
            .task_set
            .spawn(worker.run())
            .wrap_err(ERROR_CANCELLED)?;
        self.tasks.insert(
            task_id,
            TaskDeets {
                cancel_token,
                handle,
            },
        );
        Ok(())
    }

    async fn spawn_sync_task(&mut self, task: SyncTask) -> Res<()> {
        let backend = self
            .sync_backends
            .values()
            .next()
            .expect(ERROR_UNRECONIZED)
            .clone();
        let cancel_token = self.cancel_token.child_token();
        let task_id = task.id;
        let worker = SyncTaskWorker {
            task: task.clone(),
            backend,
            host_tx: self.sync_tx.clone(),
            cancel_token: self.cancel_token.child_token(),
        };
        let handle = self
            .task_set
            .spawn(worker.run())
            .wrap_err(ERROR_CANCELLED)?;
        self.sync_tasks.insert(
            task_id,
            ActiveSyncTaskDeets {
                cancel_token,
                handle,
            },
        );
        Ok(())
    }
}

struct MachineTaskWorker {
    task: big_sync_core::MachineTask,
    part_store: SharedPartitionStore,
    rpc_clients: SharedRpcClients,

    bsm_tx: mpsc::Sender<BigSyncMsg>,
    cancel_token: CancellationToken,

    shutdown: Arc<BigRedToken>,
}

impl MachineTaskWorker {
    async fn run(self) {
        let (trap, mut err_rx) = trap::TaskTrap::new();
        let part_store = trap::TrappedPartStore {
            trap: trap.clone(),
            inner: Arc::clone(&self.part_store),
        };
        let tcx = TaskCtx {
            task_id: self.task.id,
            main_tx: self.bsm_tx.clone(),
            part_store,
            rpc_clients: self
                .rpc_clients
                .lock()
                .expect(ERROR_MUTEX)
                .iter()
                .map(|(&peer_id, client)| {
                    (
                        peer_id,
                        trap::TrappedRpcClient {
                            trap: trap.clone(),
                            inner: Arc::clone(&client),
                        },
                    )
                })
                .collect(),
            _phantom: default(),
        };
        tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {
            },
            res = err_rx.recv() => {
                let err = res.expect(ERROR_IMPOSSIBLE);
                self.shutdown.set_err(err);
            },
            () = self.task.run(tcx) => {
            }
        };
    }
}

struct SyncTaskWorker {
    task: SyncTask,
    backend: Arc<dyn SyncBackend>,
    host_tx: mpsc::Sender<BigSyncEvent>,
    cancel_token: CancellationToken,
}

impl SyncTaskWorker {
    async fn run(self) {
        let res = tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {
                return;
            }
            res = self.backend.run(self.task.deets.clone()) => res,
        };
        match res {
            Ok(completions) => {
                for completion in completions {
                    let (peer_id, obj_id) = match &completion {
                        SyncCompletion::AddedMember { peer, obj_id, .. }
                        | SyncCompletion::ChangedObject { peer, obj_id }
                        | SyncCompletion::DeletedMember { peer, obj_id }
                        | SyncCompletion::Noop { peer, obj_id } => (*peer, *obj_id),
                    };
                    self.host_tx
                        .send(BigSyncEvent::SyncCompleted(
                            big_sync_core::SyncCompletedEvent {
                                task_id: self.task.id,
                                peer_id,
                                obj_id,
                                completion,
                            },
                        ))
                        .await
                        .expect(ERROR_CHANNEL);
                }
            }
            Err(err) => {
                self.host_tx
                    .send(BigSyncEvent::SyncFailed(big_sync_core::SyncFailedEvent {
                        task_id: self.task.id,
                        peer_id: self.task.deets.peer_id,
                        obj_id: self.task.deets.obj_id,
                        err,
                    }))
                    .await
                    .expect(ERROR_CHANNEL);
            }
        }
    }
}

async fn run_until_cancelled_or_trapped<E>(
    cancel_token: &CancellationToken,
    err_rx: &mut tokio::sync::mpsc::Receiver<E>,
    fut: impl std::future::Future<Output = ()>,
) -> Result<LoopAction, E> {
    tokio::select! {
        _ = cancel_token.cancelled() => Ok(LoopAction::Break),
        res = err_rx.recv() => {
            let err = res.expect(ERROR_IMPOSSIBLE);
            Err(err)
        },
        () = fut => Ok(LoopAction::Cont)
    }
}
