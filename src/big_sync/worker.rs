use std::collections::VecDeque;

use crate::interlude::*;

use crate::trap;
use crate::SyncBackend;

use big_sync_core::{
    mpsc, BigSyncEvent, BigSyncMachine, BigSyncMachineCommand, MachineTask, MachineTaskMsg, PartId,
    PeerId, SyncTask, SyncTaskCompletion, SyncTaskDeets, TaskCtx, TaskId,
};
use rand::{rngs::StdRng, SeedableRng};

#[cfg(any(test, feature = "test-support"))]
use big_sync_core::TaskCounts;

#[derive(Clone)]
pub struct BigSyncWorkerHandle {
    host_tx: tokio::sync::mpsc::Sender<BigSyncWorkerMsg>,
    stats_tx: tokio::sync::broadcast::Sender<big_sync_core::SyncStatEvent>,
}

type SharedPartitionStore = Arc<dyn crate::part_store::HostPartStore>;
type SharedPeerRpcClient = Arc<dyn crate::rpc::HostBigRpcClient>;
type SharedRpcClients = Arc<std::sync::Mutex<HashMap<PeerId, SharedPeerRpcClient>>>;

#[derive(Debug, thiserror::Error, displaydoc::Display, Serialize, Deserialize)]
pub enum BigSyncWorkerError {
    /// Unkown backend {backend_id} set for part {part_id}
    UnknownBackend {
        backend_id: BackendId,
        part_id: PartId,
    },
    /// Unknown peer {peer_id} in full sync waiter request
    UnknownPeer { peer_id: PeerId },
    /// Unknown part {part_id} for peer {peer_id} in full sync waiter request
    UnknownPart { peer_id: PeerId, part_id: PartId },
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
        WaitForFullSync {
            waiter_id: u64,
            peer_ids: std::collections::HashSet<PeerId>,
            part_ids: std::collections::HashSet<PartId>,
            resp: tokio::sync::oneshot::Sender<Result<(), BigSyncWorkerError>>,
        },
        #[cfg(any(test, feature = "test-support"))]
        ReapZombieTasks {
            timeout: Duration,
            resp: tokio::sync::oneshot::Sender<()>,
        },
        #[cfg(any(test, feature = "test-support"))]
        Snapshot {
            resp: tokio::sync::oneshot::Sender<WorkerSnapshot>,
        },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncTaskRunOutcome {
    Completion(SyncTaskCompletion),
    Stale,
}

pub type BackendId = Arc<str>;

pub struct StopToken {
    pub cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSnapshot {
    pub peer_parts: HashMap<PeerId, HashMap<PartId, BackendId>>,
    pub full_sync_waiters: HashMap<u64, Vec<(PeerId, PartId)>>,
    pub last_object_syncs: Vec<(PeerId, PartId, big_sync_core::ObjId, std::time::Instant)>,
    pub task_counts: TaskCounts,
    pub active_machine_tasks: usize,
    pub active_sync_tasks: usize,
    pub zombie_tasks: usize,
}

#[cfg(any(test, feature = "test-support"))]
impl WorkerSnapshot {
    pub fn is_idle(&self) -> bool {
        self.task_counts.pending == 0
            && self.task_counts.sync_spawn_queue == 0
            && self.task_counts.machine_spawn_queue == 0
            && self.task_counts.stop_queue == 0
            && self.active_sync_tasks == 0
            && self.zombie_tasks == 0
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
    pub fn subscribe_stats(
        &self,
    ) -> tokio::sync::broadcast::Receiver<big_sync_core::SyncStatEvent> {
        self.stats_tx.subscribe()
    }

    pub async fn set_peer(
        &self,
        peer_id: PeerId,
        client: Arc<dyn crate::rpc::HostBigRpcClient>,
        parts: HashMap<PartId, BackendId>,
    ) -> Res<()> {
        let part_count = parts.len();
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
        tracing::debug!(peer_id = %peer_id, part_count, "queue set peer");
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
        tracing::debug!(peer_id = %peer_id, "queue remove peer");
        resp_rx.await.wrap_err(ERROR_CHANNEL)?;
        Ok(())
    }

    pub async fn wait_for_full_sync(
        &self,
        peer_ids: impl IntoIterator<Item = PeerId>,
        part_ids: impl IntoIterator<Item = PartId>,
    ) -> Res<()> {
        let peer_ids: std::collections::HashSet<_> = peer_ids.into_iter().collect();
        let part_ids: std::collections::HashSet<_> = part_ids.into_iter().collect();
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.host_tx
            .send(BigSyncWorkerMsg::WaitForFullSync {
                waiter_id: rand::random(),
                peer_ids,
                part_ids,
                resp: resp_tx,
            })
            .await
            .wrap_err(ERROR_CHANNEL)?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)??;
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn drain_zombie_tasks(&self, timeout: Duration) -> Res<()> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.host_tx
            .send(BigSyncWorkerMsg::ReapZombieTasks {
                timeout,
                resp: resp_tx,
            })
            .await
            .wrap_err(ERROR_CHANNEL)?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn snapshot(&self) -> Res<WorkerSnapshot> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.host_tx
            .send(BigSyncWorkerMsg::Snapshot { resp: resp_tx })
            .await
            .wrap_err(ERROR_CHANNEL)?;
        resp_rx.await.wrap_err(ERROR_CHANNEL)
    }

    #[cfg(any(test, feature = "test-support"))]
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

const ABORT_DURATION_SECS: u64 = 2;

pub fn spawn_big_sync_worker(
    part_store: SharedPartitionStore,
    sync_backends: HashMap<BackendId, Arc<dyn SyncBackend>>,
) -> Res<(BigSyncWorkerHandle, StopToken)> {
    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();
    let (stats_tx, _) = tokio::sync::broadcast::channel(1024);

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

        machine_spawn_queue: default(),
        sync_spawn_queue: default(),

        host_rx,
        sync_tx,
        sync_rx,
        task_tx,
        task_rx,
        stats_tx: stats_tx.clone(),
        full_sync_waiters: default(),

        rpc_clients: default(),
        tasks: default(),
        sync_tasks: default(),
        zombie_tasks: default(),
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

            for (task_id, task) in std::mem::take(&mut worker.tasks) {
                task.cancel_token.cancel();
                let old = worker.zombie_tasks.insert(
                    task_id,
                    ZombieTaskDeets {
                        kind: ZombieTaskKind::Machine,
                        cancel_token: task.cancel_token,
                        handle: task.handle,
                    },
                );
                assert!(old.is_none(), "fishy");
            }
            for (task_id, task) in std::mem::take(&mut worker.sync_tasks) {
                task.cancel_token.cancel();
                let old = worker.zombie_tasks.insert(
                    task_id,
                    ZombieTaskDeets {
                        kind: ZombieTaskKind::Sync,
                        cancel_token: task.cancel_token,
                        handle: task.handle,
                    },
                );
                assert!(old.is_none(), "fishy");
            }
            worker
                .reap_zombie_tasks(Duration::from_secs(ABORT_DURATION_SECS))
                .await
                .inspect_err(|err| error!(%err))
                .ok();
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
        BigSyncWorkerHandle { host_tx, stats_tx },
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

    machine_spawn_queue: VecDeque<MachineTask>,
    sync_spawn_queue: VecDeque<SyncTask>,

    sync_rx: mpsc::Receiver<BigSyncEvent>,
    sync_tx: mpsc::Sender<BigSyncEvent>,
    task_rx: mpsc::Receiver<MachineTaskMsg>,
    task_tx: mpsc::Sender<MachineTaskMsg>,
    stats_tx: tokio::sync::broadcast::Sender<big_sync_core::SyncStatEvent>,
    full_sync_waiters: HashMap<u64, tokio::sync::oneshot::Sender<Result<(), BigSyncWorkerError>>>,

    tasks: HashMap<TaskId, TaskDeets>,
    peers: HashMap<PeerId, PeerState>,
    sync_tasks: HashMap<TaskId, ActiveSyncTaskDeets>,
    zombie_tasks: HashMap<TaskId, ZombieTaskDeets>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ZombieTaskKind {
    Machine,
    Sync,
}

struct ZombieTaskDeets {
    kind: ZombieTaskKind,
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

const MAX_ACTIVE_SYNC_TASKS: usize = 32;

impl BigSyncWorker {
    #[tracing::instrument(skip(self, shutdown))]
    async fn machine_loop(&mut self, shutdown: Arc<BigRedToken>) -> Res<()> {
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    break;
                }
                msg = self.task_rx.recv() => {
                    let Ok(msg) = msg else {
                        break;
                    };
                    self.machine.handle_task_msg(msg);
                }
                evt = self.sync_rx.recv() => {
                    let Ok(evt) = evt else {
                        break;
                    };
                    self.machine.handle_evt(evt);
                }
                msg = self.host_rx.recv() => {
                    let Some(msg) = msg else {
                        break;
                    };
                    self.handle_msg(msg).await?;
                }
                _ = janitor_tick.tick() => {
                    self.machine.handle_tick(std::time::Instant::now());
                }
            };
            while let Some((id, cmd)) = self.machine.get_cmd() {
                match cmd {
                    BigSyncMachineCommand::RemoveObjFromPart { obj_id, part_id } => {
                        self.part_store
                            .remove_obj_from_part(obj_id, part_id)
                            .await?;
                    }
                    BigSyncMachineCommand::AddObjToPart { obj_id, part_id } => {
                        self.part_store
                            .add_obj_to_parts(obj_id, vec![part_id])
                            .await?;
                    }
                    BigSyncMachineCommand::SetPartCursor {
                        peer_id,
                        part_id,
                        cursor,
                    } => {
                        self.part_store
                            .set_peer_part_cursor(peer_id, part_id, cursor)
                            .await?;
                    }
                }
                self.machine.handle_cmd_success(id);
            }

            self.batch_stop_tasks().await?;
            self.machine_spawn_queue
                .extend(self.machine.drain_machine_spawn_queue());
            while let Some(task) = self.machine_spawn_queue.pop_front() {
                self.spawn_machine_task(task, Arc::clone(&shutdown)).await?;
            }

            for task in self.machine.drain_sync_spawn_queue() {
                self.sync_spawn_queue.push_front(task);
            }
            while self.sync_tasks.len() < MAX_ACTIVE_SYNC_TASKS {
                let Some(task) = self.sync_spawn_queue.pop_front() else {
                    break;
                };
                self.spawn_sync_task(task).await?;
            }
            self.sweep_finished_zombies();
            for event in self.machine.drain_stat_evts() {
                if let big_sync_core::SyncStatEvent::FullSyncWaiterSatisfied { waiter_id } = event {
                    if let Some(resp) = self.full_sync_waiters.remove(&waiter_id) {
                        let _ = resp.send(Ok(()));
                    }
                }
                let _ = self.stats_tx.send(event);
            }
        }
        Ok(())
    }

    async fn handle_msg(&mut self, msg: BigSyncWorkerMsg) -> Res<()> {
        match msg {
            BigSyncWorkerMsg::SetPeer {
                peer_id,
                client,
                parts,
                resp,
            } => {
                for (&part_id, backend_id) in &parts {
                    if !self.sync_backends.contains_key(backend_id) {
                        resp.send(Err(BigSyncWorkerError::UnknownBackend {
                            backend_id: Arc::clone(backend_id),
                            part_id,
                        }))
                        .inspect_err(|_| warn!(ERROR_CALLER))
                        .ok();
                        return Ok(());
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
                let part_count = parts.len();
                let evt = BigSyncEvent::SetPeer(big_sync_core::SetPeerEvent {
                    peer_id,
                    parts: parts.into_keys().collect(),
                });
                resp.send(Ok(())).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                tracing::debug!(peer_id = %peer_id, part_count, "accept set peer");
                self.machine.handle_evt(evt);
            }
            BigSyncWorkerMsg::RemovePeer { peer_id, resp } => {
                self.peers.remove(&peer_id);
                let evt = BigSyncEvent::RemovePeer(big_sync_core::RemovePeerEvent { peer_id });
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
                tracing::debug!(peer_id = %peer_id, "accept remove peer");
                self.machine.handle_evt(evt);
            }
            BigSyncWorkerMsg::WaitForFullSync {
                waiter_id,
                peer_ids,
                part_ids,
                resp,
            } => {
                for peer_id in &peer_ids {
                    let Some(peer_state) = self.peers.get(peer_id) else {
                        resp.send(Err(BigSyncWorkerError::UnknownPeer { peer_id: *peer_id }))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                        return Ok(());
                    };
                    for part_id in &part_ids {
                        if !peer_state.parts.contains_key(part_id) {
                            resp.send(Err(BigSyncWorkerError::UnknownPart {
                                peer_id: *peer_id,
                                part_id: *part_id,
                            }))
                            .inspect_err(|_| warn!(ERROR_CALLER))
                            .ok();
                            return Ok(());
                        }
                    }
                }
                let old = self.full_sync_waiters.insert(waiter_id, resp);
                assert!(old.is_none(), "fishy");
                self.machine.handle_evt(BigSyncEvent::WaitForFullSync(
                    big_sync_core::WaitForFullSyncEvent {
                        waiter_id,
                        peer_ids,
                        part_ids,
                    },
                ));
            }
            #[cfg(any(test, feature = "test-support"))]
            BigSyncWorkerMsg::ReapZombieTasks { timeout, resp } => {
                tracing::debug!(
                    zombie_count = self.zombie_tasks.len(),
                    ?timeout,
                    "drain zombie request"
                );
                self.reap_zombie_tasks(timeout).await?;
                resp.send(()).inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            #[cfg(any(test, feature = "test-support"))]
            BigSyncWorkerMsg::Snapshot { resp } => {
                let snapshot = WorkerSnapshot {
                    peer_parts: self
                        .peers
                        .iter()
                        .map(|(&peer_id, peer_state)| (peer_id, peer_state.parts.clone()))
                        .collect(),
                    full_sync_waiters: self.machine.debug_full_sync_waiters(),
                    last_object_syncs: self.machine.debug_last_object_syncs(),
                    task_counts: self.machine.task_counts(),
                    active_machine_tasks: self.tasks.len(),
                    active_sync_tasks: self.sync_tasks.len(),
                    zombie_tasks: self.zombie_tasks.len(),
                };
                resp.send(snapshot)
                    .inspect_err(|_| warn!(ERROR_CALLER))
                    .ok();
            }
        }
        Ok(())
    }

    async fn batch_stop_tasks(&mut self) -> Res<()> {
        let task_ids: Vec<_> = self.machine.drain_stop_queue().collect();
        let stop_count = task_ids.len();
        if stop_count > 0 {
            tracing::debug!(stop_count, "draining stop queue");
        }
        for task_id in task_ids {
            if let Some(task) = self.tasks.remove(&task_id) {
                task.cancel_token.cancel();
                // task.handle
                //     .join(Duration::from_millis(2000))
                //     .await
                //     .inspect_err(|err| error!(%err))
                //     .ok();
                let old = self.zombie_tasks.insert(
                    task_id,
                    ZombieTaskDeets {
                        kind: ZombieTaskKind::Machine,
                        cancel_token: task.cancel_token,
                        handle: task.handle,
                    },
                );
                assert!(old.is_none(), "fishy");
            } else if let Some(task) = self.sync_tasks.remove(&task_id) {
                task.cancel_token.cancel();
                // task.handle
                //     .join(Duration::from_millis(2000))
                //     .await
                //     .inspect_err(|err| error!(%err))
                //     .ok();
                let old = self.zombie_tasks.insert(
                    task_id,
                    ZombieTaskDeets {
                        kind: ZombieTaskKind::Sync,
                        cancel_token: task.cancel_token,
                        handle: task.handle,
                    },
                );
                assert!(old.is_none(), "fishy");
            }
        }
        Ok(())
    }

    fn sweep_finished_zombies(&mut self) {
        for (task_id, task) in self
            .zombie_tasks
            .extract_if(|_task_id, task| task.handle.is_finished())
        {
            tracing::debug!(task_id, kind = ?task.kind, "sweeping finished zombie task");
        }
    }

    async fn reap_zombie_tasks(&mut self, timeout: Duration) -> Res<()> {
        self.sweep_finished_zombies();
        let zombie_count = self.zombie_tasks.len();
        if zombie_count > 0 {
            tracing::debug!(zombie_count, "aborting zombie tasks at drain timeout");
        }
        use futures_buffered::BufferedStreamExt;
        futures::stream::iter(self.zombie_tasks.drain().map(|(task_id, task)| async move {
            tracing::debug!(task_id, kind = ?task.kind, "draining zombie task");
            task.cancel_token.cancel();
            task.handle
                .join(timeout)
                .await
                .inspect_err(|err| error!(%err, "error reaping zombie task"))
                .ok();
        }))
        .buffered_unordered(16)
        .collect::<()>()
        .await;
        Ok(())
    }

    async fn spawn_machine_task(
        &mut self,
        task: big_sync_core::MachineTask,
        shutdown: Arc<BigRedToken>,
    ) -> Res<()> {
        let cancel_token = self.cancel_token.child_token();
        let task_id = task.id;
        tracing::debug!(task_id, "spawn machine task");
        let worker = MachineTaskWorker {
            task,
            part_store: Arc::clone(&self.part_store),
            rpc_clients: Arc::clone(&self.rpc_clients),
            bsm_tx: self.task_tx.clone(),
            cancel_token: cancel_token.clone(),
            rng: StdRng::from_os_rng(),
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
        let peer_state = self
            .peers
            .get(&task.deets.peer_id)
            .expect(ERROR_UNRECONIZED);
        let mut part_ids: Vec<PartId> = if task.part_hints.is_empty() {
            self.part_store.obj_parts(task.deets.obj_id).await?
        } else {
            task.part_hints.iter().copied().collect()
        };
        part_ids.sort_unstable();
        part_ids.dedup();
        assert!(
            !part_ids.is_empty(),
            "sync task for obj {:?} had no parts to resolve a backend",
            task.deets.obj_id
        );
        let mut backend_id = None;
        for part_id in &part_ids {
            let Some(part_backend_id) = peer_state.parts.get(part_id) else {
                panic!(
                    "sync task requested unknown part {part_id:?} for peer {}",
                    task.deets.peer_id
                );
            };
            match backend_id {
                None => backend_id = Some(Arc::clone(part_backend_id)),
                Some(ref existing) => assert_eq!(
                    existing, part_backend_id,
                    "sync task parts mapped to different backend ids for peer {} obj {:?}",
                    task.deets.peer_id, task.deets.obj_id
                ),
            }
        }
        let backend_id = backend_id.expect(ERROR_IMPOSSIBLE);
        let backend = Arc::clone(
            self.sync_backends
                .get(&backend_id)
                .unwrap_or_else(|| panic!("unknown backend {backend_id} for sync task")),
        );
        let cancel_token = self.cancel_token.child_token();
        let task_id = task.id;
        tracing::debug!(
            task_id,
            peer_id = %task.deets.peer_id,
            obj_id = %task.deets.obj_id,
            part_hint_count = part_ids.len(),
            "spawn sync task"
        );
        let worker = SyncTaskWorker {
            task: task.clone(),
            backend,
            host_tx: self.sync_tx.clone(),
            cancel_token: cancel_token.clone(),
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
    task: MachineTask,
    part_store: SharedPartitionStore,
    rpc_clients: SharedRpcClients,

    bsm_tx: mpsc::Sender<MachineTaskMsg>,
    cancel_token: CancellationToken,
    rng: StdRng,

    shutdown: Arc<BigRedToken>,
}

impl MachineTaskWorker {
    #[tracing::instrument(skip(self))]
    async fn run(self) {
        let _ = self
            .cancel_token
            .run_until_cancelled(async move {
                let (trap, mut err_rx) = trap::TaskTrap::new();
                let part_store = trap::TrappedPartStore {
                    trap: trap.clone(),
                    inner: Arc::clone(&self.part_store),
                };
                let tcx = TaskCtx {
                    task_id: self.task.id,
                    main_tx: self.bsm_tx.clone(),
                    part_store,
                    rng: self.rng,
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
                                    inner: Arc::clone(client),
                                },
                            )
                        })
                        .collect(),
                    _phantom: default(),
                };
                tokio::select! {
                    res = err_rx.recv() => {
                        let err = res.expect(ERROR_IMPOSSIBLE);
                        self.shutdown.set_err(err);
                    },
                    () = self.task.run(tcx) => {
                    }
                };
            })
            .await;
    }
}

struct SyncTaskWorker {
    task: SyncTask,
    backend: Arc<dyn SyncBackend>,
    host_tx: mpsc::Sender<BigSyncEvent>,
    cancel_token: CancellationToken,
}

impl SyncTaskWorker {
    #[tracing::instrument(
        skip(self),
        fields(
            task_id = self.task.id,
            peer_id = %self.task.deets.peer_id,
            obj_id = %self.task.deets.obj_id,
        )
    )]
    async fn run(self) {
        let fut = async move {
            let SyncTask {
                id: _task_id,
                part_hints: _part_hints,
                deets,
            } = self.task;
            let SyncTaskDeets {
                peer_id,
                obj_id,
                remote_payload,
            } = deets;
            let res = self.backend.sync_obj(peer_id, obj_id, remote_payload).await;
            match res {
                Ok(SyncTaskRunOutcome::Completion(completion)) => {
                    self.host_tx
                        .send(BigSyncEvent::SyncCompleted(
                            big_sync_core::SyncCompletedEvent {
                                task_id: _task_id,
                                peer_id,
                                completion,
                            },
                        ))
                        .await
                        .expect(ERROR_CHANNEL);
                }
                Ok(SyncTaskRunOutcome::Stale) => {
                    self.host_tx
                        .send(BigSyncEvent::SyncStale(big_sync_core::SyncStaleEvent {
                            task_id: _task_id,
                            peer_id,
                            obj_id,
                        }))
                        .await
                        .expect(ERROR_CHANNEL);
                }
                Err(err) => {
                    self.host_tx
                        .send(BigSyncEvent::SyncFailed(big_sync_core::SyncFailedEvent {
                            task_id: _task_id,
                            peer_id,
                            obj_id,
                            err,
                        }))
                        .await
                        .expect(ERROR_CHANNEL);
                }
            }
        };
        let _ = self.cancel_token.run_until_cancelled(fut).await;
    }
}

// enum LoopAction {
//     Cont,
//     Break,
// }
// async fn run_until_cancelled_or_trapped<E>(
//     cancel_token: &CancellationToken,
//     err_rx: &mut tokio::sync::mpsc::Receiver<E>,
//     fut: impl std::future::Future<Output = ()>,
// ) -> Result<LoopAction, E> {
//     tokio::select! {
//         _ = cancel_token.cancelled() => Ok(LoopAction::Break),
//         res = err_rx.recv() => {
//             let err = res.expect(ERROR_IMPOSSIBLE);
//             Err(err)
//         },
//         () = fut => Ok(LoopAction::Cont)
//     }
// }
