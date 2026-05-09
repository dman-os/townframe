mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use future_form::{FutureForm, Sendable};
use futures::future::BoxFuture;
use irpc::{channel::none::NoSender, rpc_requests, Client, WithChannels};
use serde::{Deserialize, Serialize};

use big_sync_core::{
    mpsc,
    part_store::{ObjPayload, PartitionStore, PeerPartCursors},
    rpc::BigSyncRpcClient,
    BigSyncEvent, BigSyncMachine, BigSyncMsg, ObjId, PartId, PeerId, SyncCompletion, SyncTask,
    SyncTaskDeets, TaskCtx, TaskId,
};

mod part_store;
mod rpc;

use crate::interlude::*;

pub struct StopToken {
    pub cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
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

pub type BigSyncWorkerHandle = Client<BigSyncWorkerProtocol>;
type SharedPartitionStore = Arc<dyn part_store::HostPartitionStore>;
type SharedPeerRpcClient = Arc<dyn rpc::HostBigRpcClient>;
type SharedRpcClients = Arc<std::sync::Mutex<HashMap<PeerId, SharedPeerRpcClient>>>;

#[rpc_requests(message = BigSyncWorkerMessage)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BigSyncWorkerProtocol {
    #[rpc(tx = NoSender)]
    SetPeer(SetPeerCommand),
    #[rpc(tx = NoSender)]
    RemovePeer(RemovePeerCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetPeerCommand {
    pub peer_id: PeerId,
    /// Partitions to sync from the peer
    pub parts: std::collections::HashSet<PartId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovePeerCommand {
    pub peer_id: PeerId,
}

pub async fn set_peer(client: &BigSyncWorkerHandle, cmd: SetPeerCommand) -> Res<()> {
    client.notify(cmd).await.wrap_err(ERROR_CHANNEL)?;
    Ok(())
}

pub async fn remove_peer(client: &BigSyncWorkerHandle, cmd: RemovePeerCommand) -> Res<()> {
    client.notify(cmd).await.wrap_err(ERROR_CHANNEL)?;
    Ok(())
}

pub fn spawn_big_sync_worker(
    part_store: SharedPartitionStore,
) -> Res<(BigSyncWorkerHandle, StopToken)> {
    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();

    let machine = big_sync_core::BigSyncMachine::default();
    let (host_tx, host_rx) = tokio::sync::mpsc::channel(64);
    let (sync_tx, sync_rx) = mpsc::bounded(64, "SyncWorkers".into(), "BigSyncMachine".into());
    let (task_tx, task_rx) = mpsc::bounded(64, "BigSync tasks".into(), "BigSyncMachine".into());
    let mut worker = BigSyncWorker {
        task_set,
        sync_backends: default(),
        part_store,
        rpc_clients: default(),
        cancel_token: cancel_token.clone(),
        host_rx,
        machine,
        tasks: default(),
        sync_tasks: default(),

        sync_tx,
        sync_rx,
        task_tx,
        task_rx,
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
            if let Some(res) = Arc::try_unwrap(shutdown)
                .expect(ERROR_IMPOSSIBLE)
                .err
                .take()
            {
                return Err(res);
            }
            Ok(())
        }
    };
    let join_handle = tokio::task::spawn(async move { fut.await.unwrap() }.in_current_span());

    Ok((
        Client::local(host_tx),
        StopToken {
            cancel_token,
            join_handle,
        },
    ))
}

#[async_trait]
pub trait SyncBackend: Send + Sync + 'static {
    async fn run(
        &self,
        task: SyncTaskDeets,
        part_store: SharedPartitionStore,
    ) -> Res<Vec<SyncCompletion>>;
}

struct SyncTaskWorker {
    task: SyncTask,
    backend: Arc<dyn SyncBackend>,
    part_store: SharedPartitionStore,
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
            res = self.backend.run(self.task.deets.clone(), Arc::clone(&self.part_store)) => res,
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

    tasks: HashMap<TaskId, TaskDeets>,
    sync_tasks: HashMap<TaskId, ActiveSyncTaskDeets>,
    sync_backends: HashMap<u64, Arc<dyn SyncBackend>>,
    rpc_clients: SharedRpcClients,
    part_store: SharedPartitionStore,

    host_rx: tokio::sync::mpsc::Receiver<BigSyncWorkerMessage>,
    machine: BigSyncMachine,

    sync_rx: mpsc::Receiver<BigSyncEvent>,
    sync_tx: mpsc::Sender<BigSyncEvent>,
    task_rx: mpsc::Receiver<BigSyncMsg>,
    task_tx: mpsc::Sender<BigSyncMsg>,
}

struct TaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

struct ActiveSyncTaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

#[derive(Clone)]
struct TaskTrap {
    tx: tokio::sync::mpsc::Sender<eyre::Report>,
}

enum Never {}

impl TaskTrap {
    fn new() -> (Self, tokio::sync::mpsc::Receiver<eyre::Report>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        (Self { tx }, rx)
    }

    async fn run_or_trap<F, O>(&self, fut: F) -> O
    where
        F: std::future::Future<Output = Res<O>>,
    {
        match fut.await {
            Ok(val) => val,
            Err(err) => {
                self.trap(err).await;
                unreachable!()
            }
        }
    }

    async fn trap(&self, err: eyre::Report) -> Never {
        self.tx.send(err).await.expect(ERROR_CHANNEL);
        std::future::pending::<()>().await;
        unreachable!()
    }
}

const MAX_ACTIVE_SYNC_TASKS: usize = 32;

impl BigSyncWorker {
    async fn machine_loop(&mut self, shutdown: Arc<BigRedToken>) -> Res<()> {
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        let (trap, mut err_rx) = TaskTrap::new();
        loop {
            let part_store = TrappedTaskStore {
                trap: trap.clone(),
                inner: Arc::clone(&self.part_store),
            };
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    break;
                }
                res = err_rx.recv() => {
                    let err = res.expect(ERROR_IMPOSSIBLE);
                    return Err(err);
                },
                msg = self.task_rx.recv() => {
                    let msg = msg.expect(ERROR_CALLER);
                    self.machine.handle_msg(msg, &part_store).await;
                }
                evt = self.sync_rx.recv() => {
                    let evt = evt.expect(ERROR_CALLER);
                    self.machine.handle_evt(evt, &part_store).await;
                }
                cmd = self.host_rx.recv() => {
                    let cmd = cmd.expect(ERROR_CALLER);
                    let evt = match cmd {
                        BigSyncWorkerMessage::SetPeer(msg) => {
                            let WithChannels { inner, .. } = msg;
                            BigSyncEvent::SetPeer(big_sync_core::SetPeerEvent {
                                peer_id: inner.peer_id,
                                parts: inner.parts.into_iter().collect(),
                            })
                        }
                        BigSyncWorkerMessage::RemovePeer(msg) => {
                            let WithChannels { inner, .. } = msg;
                            BigSyncEvent::RemovePeer(big_sync_core::RemovePeerEvent {
                                peer_id: inner.peer_id,
                            })
                        }
                    };
                    self.machine.handle_evt(evt, &part_store).await;
                }
                _ = janitor_tick.tick() => {
                    self.machine.handle_tick(std::time::Instant::now());
                }
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

    async fn batch_stop_tasks(&mut self) -> Res<()> {
        // NOTE: we exclusivley rely on the machine's
        // internal task tracking to clean up tasks
        for task_id in self.machine.drain_stop_queue() {
            if let Some(task) = self.tasks.remove(&task_id) {
                task.cancel_token.cancel();
                task.handle.join(Duration::from_secs(2)).await?;
                continue;
            }
            if let Some(task) = self.sync_tasks.remove(&task_id) {
                task.cancel_token.cancel();
                task.handle.join(Duration::from_secs(2)).await?;
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
            part_store: Arc::clone(&self.part_store),
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
        let (trap, mut err_rx) = TaskTrap::new();
        let part_store = TrappedTaskStore {
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
                        TrappedRpcClient {
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

struct TrappedTaskStore {
    trap: TaskTrap,
    inner: SharedPartitionStore,
}

impl PartitionStore<Sendable> for TrappedTaskStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        let fut = self.inner.member_count(part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        let fut = self.inner.obj_payload(obj_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> BoxFuture<'a, ()> {
        let fut = self.inner.upsert_obj(obj_id, payload.clone(), parts.into());
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        let fut = self.inner.obj_parts(obj_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
        let fut = self.inner.add_obj_to_parts(obj_id, parts.into());
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        let fut = self.inner.remove_obj_from_part(obj_id, part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> BoxFuture<'a, PeerPartCursors> {
        let fut = self.inner.get_peer_part_cursor(peer_id, part_id);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> BoxFuture<'a, ()> {
        let fut = self.inner.set_peer_part_cursor(peer_id, part_id, cursors);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }
}

struct TrappedRpcClient {
    trap: TaskTrap,
    inner: SharedPeerRpcClient,
}

impl BigSyncRpcClient<Sendable> for TrappedRpcClient {
    fn peer_summary<'a>(
        &'a self,
        req: big_sync_core::rpc::PeerSummaryRequest,
    ) -> BoxFuture<'a, big_sync_core::rpc::BigSyncRpcResult<big_sync_core::rpc::PeerSummaryResult>>
    {
        let fut = self.inner.peer_summary(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }

    fn sub_parts<'a>(
        &'a self,
        req: big_sync_core::rpc::SubPartsRequest,
    ) -> BoxFuture<
        'a,
        big_sync_core::rpc::BigSyncRpcResult<
            Result<mpsc::Receiver<big_sync_core::rpc::SubEvent>, big_sync_core::rpc::SubPartsError>,
        >,
    > {
        let fut = self.inner.sub_parts(req);
        Sendable::from_future(self.trap.run_or_trap(fut))
    }
}
