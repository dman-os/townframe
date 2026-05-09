mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use future_form::{FutureForm, Sendable};
// FIXME: FutureForm is expensive, can we just for everyone to be Send
// and get over it?
use async_trait::async_trait;
use futures::future::BoxFuture;
use irpc::{
    channel::none::NoSender,
    rpc_requests, Client, WithChannels,
};
use serde::{Deserialize, Serialize};

use big_sync_core::{
    part_store::{ObjPayload, PartitionStore, PeerPartCursors},
    rpc::{BigSyncRpcClient, BigSyncRpcResult},
    BigSyncEvent, BigSyncMachine, BigSyncMsg, ObjId, PartId, PeerId, SyncCompletion, SyncTask,
    TaskCtx, TaskId,
};
use utils_rs::prelude::tokio::sync::oneshot;

mod part_store;

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

pub async fn set_peer(
    client: &BigSyncWorkerHandle,
    cmd: SetPeerCommand,
) -> Res<()> {
    client.notify(cmd).await.wrap_err(ERROR_CHANNEL)?;
    Ok(())
}

pub async fn remove_peer(
    client: &BigSyncWorkerHandle,
    cmd: RemovePeerCommand,
) -> Res<()> {
    client.notify(cmd).await.wrap_err(ERROR_CHANNEL)?;
    Ok(())
}

pub fn spawn_big_sync_worker(
    part_store: Arc<part_store::SqlitePartStoreHandle>,
) -> Res<(BigSyncWorkerHandle, StopToken)> {
    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();
    let (task_tx, task_rx) =
        big_sync_core::mpsc::bounded(64, "BigSync tasks".into(), "BigSyncMachine".into());
    let (event_tx, event_rx) =
        big_sync_core::mpsc::bounded(64, "BigSync events".into(), "BigSyncMachine".into());

    let machine = big_sync_core::BigSyncMachine::default();
    let (host_tx, host_rx) = tokio::sync::mpsc::channel(64);
    let worker = BigSyncWorker {
        task_set,
        task_tx,
        task_rx,
        event_tx,
        event_rx,
        rpc_clients: default(),
        sync_backends: default(),
        part_store,
        cancel_token: cancel_token.clone(),
        host_rx,
        machine,
        tasks: default(),
        sync_tasks: default(),
    };
    let join_handle = tokio::task::spawn(
        {
            let cancen_token = cancel_token.clone();
            async move {
                if let Some(Err(err)) = cancen_token
                    .run_until_cancelled(worker.machine_loop())
                    .await
                {
                    panic!("error: {err:?}")
                }
            }
        }
        .in_current_span(),
    );

    Ok((
        Client::local(host_tx),
        StopToken {
            cancel_token,
            join_handle,
        },
    ))
}

struct BigSyncIrpcClient {}

#[async_trait]
pub trait SyncBackend: Send + Sync + 'static {
    async fn run(
        &self,
        task: SyncTask,
        part_store: Arc<part_store::SqlitePartStoreHandle>,
    ) -> Res<Vec<SyncCompletion>>;
}

struct SyncTaskWorker {
    task_id: TaskId,
    task: SyncTask,
    backend: Arc<dyn SyncBackend>,
    part_store: Arc<part_store::SqlitePartStoreHandle>,
    host_tx: big_sync_core::mpsc::Sender<BigSyncEvent>,
    cancel_token: CancellationToken,
}

impl SyncTaskWorker {
    async fn run(self) {
        let task = self.task;
        let res = tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {
                return;
            }
            res = self.backend.run(task.clone(), Arc::clone(&self.part_store)) => res,
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
                                task_id: self.task_id,
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
                        task_id: self.task_id,
                        peer_id: task.peer_id,
                        obj_id: task.obj_id,
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

    task_tx: big_sync_core::mpsc::Sender<BigSyncMsg>,
    task_rx: big_sync_core::mpsc::Receiver<BigSyncMsg>,
    event_tx: big_sync_core::mpsc::Sender<BigSyncEvent>,
    event_rx: big_sync_core::mpsc::Receiver<BigSyncEvent>,
    tasks: HashMap<TaskId, TaskDeets>,
    sync_tasks: HashMap<TaskId, SyncTaskDeets>,
    sync_backends: HashMap<u64, Arc<dyn SyncBackend>>,
    rpc_clients: HashMap<PeerId, Arc<BigSyncIrpcClient>>,
    part_store: Arc<part_store::SqlitePartStoreHandle>,

    host_rx: tokio::sync::mpsc::Receiver<BigSyncWorkerMessage>,
    machine: BigSyncMachine,
}

struct TaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

struct SyncTaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

const MAX_ACTIVE_SYNC_TASKS: usize = 32;

impl BigSyncWorker {
    async fn spawn_machine_task(
        &mut self,
        task_id: TaskId,
        deets: big_sync_core::MachineTask,
        shutdown: Arc<BigRedToken>,
    ) -> Res<()> {
        let cancel_token = self.cancel_token.child_token();
        let worker = MachineTaskWorker {
            task_id,
            task: deets,
            part_store: Arc::clone(&self.part_store),
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

    async fn spawn_sync_task(&mut self, task_id: TaskId, task: SyncTask) -> Res<()> {
        let backend = self
            .sync_backends
            .values()
            .next()
            .expect(ERROR_UNRECONIZED)
            .clone();
        let cancel_token = self.cancel_token.child_token();
        let worker = SyncTaskWorker {
            task_id,
            task: task.clone(),
            backend,
            part_store: Arc::clone(&self.part_store),
            host_tx: self.event_tx.clone(),
            cancel_token: self.cancel_token.child_token(),
        };
        let handle = self
            .task_set
            .spawn(worker.run())
            .wrap_err(ERROR_CANCELLED)?;
        self.sync_tasks.insert(
            task_id,
            SyncTaskDeets {
                cancel_token,
                handle,
            },
        );
        Ok(())
    }

    async fn machine_loop(mut self) -> Res<()> {
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));

        let shutdown = BigRedToken {
            err: default(),
            master_cancel: self.cancel_token.clone(),
        };
        let shutdown = Arc::new(shutdown);
        loop {
            let (ecx, err_rx) = Yielder::new();
            let part_store = EffectedTaskStore {
                ecx,
                inner: Arc::clone(&self.part_store),
            };
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    break;
                }
                res = err_rx => {
                    let err = res.expect(ERROR_IMPOSSIBLE);
                    shutdown.set_err(err);
                    break;
                },
                evt = self.event_rx.recv() => {
                    let evt = evt.expect(ERROR_CALLER);
                    self.machine.handle_evt(evt, &part_store).await;
                }
                cmd = self.host_rx.recv() => {
                    let cmd = cmd.expect(ERROR_CALLER);
                    match cmd {
                        BigSyncWorkerMessage::SetPeer(msg) => {
                            let WithChannels { inner, .. } = msg;
                            self.event_tx
                                .send(BigSyncEvent::SetPeer(big_sync_core::SetPeerEvent {
                                    peer_id: inner.peer_id,
                                    parts: inner.parts.into_iter().collect(),
                                }))
                                .await
                                .expect(ERROR_CHANNEL);
                        }
                        BigSyncWorkerMessage::RemovePeer(msg) => {
                            let WithChannels { inner, .. } = msg;
                            self.event_tx
                                .send(BigSyncEvent::RemovePeer(big_sync_core::RemovePeerEvent {
                                    peer_id: inner.peer_id,
                                }))
                                .await
                                .expect(ERROR_CHANNEL);
                        }
                    }
                }
                msg = self.task_rx.recv() => {
                    let msg = msg.expect(ERROR_CALLER);
                    self.machine.handle_msg(msg, &part_store).await;
                }
                _ = janitor_tick.tick() => {
                    self.machine.handle_tick(std::time::Instant::now());
                }
            }

            // NOTE: we exclusivley rely on the machine's
            // internal task tracking to clean up tasks
            for task_id in self.machine.drain_stop_queue() {
                if let Some(task) = self.tasks.remove(&task_id) {
                    task.cancel_token.cancel();
                    task.handle.join(Duration::from_secs(2)).await;
                    continue;
                }
                if let Some(task) = self.sync_tasks.remove(&task_id) {
                    task.cancel_token.cancel();
                    task.handle.join(Duration::from_secs(2)).await;
                }
            }

            while let Some(task) = self.machine.pop_machine_spawn_queue() {
                let task_id = task.id;
                match task.deets {
                    big_sync_core::TaskDeets::SyncTask(_) => unreachable!(),
                    big_sync_core::TaskDeets::MachineTask(deets) => {
                        self.spawn_machine_task(
                            task_id,
                            deets,
                            Arc::clone(&shutdown),
                        )
                        .await?;
                    }
                }
            }

            while self.sync_tasks.len() < MAX_ACTIVE_SYNC_TASKS {
                if self.machine.peek_sync_spawn_queue().is_none() {
                    break;
                }
                let task = self.machine.pop_sync_spawn_queue().expect(ERROR_IMPOSSIBLE);
                let task_id = task.id;
                match task.deets {
                    big_sync_core::TaskDeets::SyncTask(sync_task) => {
                        self.spawn_sync_task(task_id, sync_task).await?;
                    }
                    big_sync_core::TaskDeets::MachineTask(_) => unreachable!(),
                }
            }
        }

        for (_id, task) in self.tasks.drain() {
            task.cancel_token.cancel();
            task.handle.join(Duration::from_secs(2)).await;
        }
        let sync_tasks = std::mem::take(&mut self.sync_tasks);
        for (_id, task) in sync_tasks {
            task.cancel_token.cancel();
            task.handle.join(Duration::from_secs(2)).await;
        }
        if let Some(err) = Arc::try_unwrap(shutdown)
            .expect(ERROR_IMPOSSIBLE)
            .err
            .take()
        {
            Err(err)
        } else {
            Ok(())
        }
    }
}

struct MachineTaskWorker {
    task_id: TaskId,
    task: big_sync_core::MachineTask,
    part_store: Arc<part_store::SqlitePartStoreHandle>,

    bsm_tx: big_sync_core::mpsc::Sender<BigSyncMsg>,
    cancel_token: CancellationToken,

    shutdown: Arc<BigRedToken>,
}

impl MachineTaskWorker {
    async fn run(self) {
        let (ecx, err_rx) = Yielder::new();
        let part_store = EffectedTaskStore {
            ecx,
            inner: Arc::clone(&self.part_store),
        };
        let tcx: TaskCtx<_, _, EffectedBigSyncRpc> = TaskCtx {
            task_id: self.task_id,
            main_tx: self.bsm_tx.clone(),
            part_store,
            rpc_clients: default(),
            _phantom: default(),
        };
        tokio::select! {
            biased;
            () = self.cancel_token.cancelled() => {
            },
            res = err_rx => {
                let err = res.expect(ERROR_IMPOSSIBLE);
                self.shutdown.set_err(err);
            },
            () = self.task.run(tcx) => {
            }
        };
    }
}

struct Yielder<E> {
    err_tx: std::sync::Mutex<Option<oneshot::Sender<E>>>,
}

enum Never {}

impl<E> Yielder<E> {
    fn new() -> (Yielder<E>, oneshot::Receiver<E>) {
        let (err_tx, err_rx) = oneshot::channel();
        (
            Self {
                err_tx: std::sync::Mutex::new(Some(err_tx)),
            },
            err_rx,
        )
    }

    /// This never returns
    async fn err_out(&self, err: E) -> Never {
        self.err_tx
            .lock()
            .expect(ERROR_MUTEX)
            .take()
            .map(|chan| {
                chan.send(err)
                    .inspect_err(|_| warn!("EffectedTaskStore used after drop"));
            })
            .ok_or_eyre("EffectedTaskStore used after drop")
            .inspect_err(|err| warn!(?err));
        std::future::pending::<()>().await;
        unreachable!()
    }
}

struct EffectedTaskStore {
    ecx: Yielder<eyre::Report>,
    inner: Arc<part_store::SqlitePartStoreHandle>,
}

impl PartitionStore<Sendable> for EffectedTaskStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> BoxFuture<'a, PeerPartCursors> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursors: PeerPartCursors,
    ) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }
}

struct EffectedBigSyncRpc {
    ecx: Yielder<eyre::Report>,
}
impl BigSyncRpcClient<Sendable> for EffectedBigSyncRpc {
    fn peer_summary<'a>(
        &'a self,
        req: big_sync_core::rpc::PeerSummaryRequest,
    ) -> BoxFuture<'a, BigSyncRpcResult<big_sync_core::rpc::PeerSummaryResult>> {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }

    fn sub_parts<'a>(
        &'a self,
        req: big_sync_core::rpc::SubPartsRequest,
    ) -> BoxFuture<
        'a,
        BigSyncRpcResult<
            Result<
                big_sync_core::mpsc::Receiver<big_sync_core::rpc::SubEvent>,
                big_sync_core::rpc::SubPartsError,
            >,
        >,
    > {
        Sendable::from_future(async move {
            self.ecx.err_out(ferr!("TODO")).await;
            unreachable!()
        })
    }
}
