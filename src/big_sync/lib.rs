mod interlude {
    pub use utils_rs::prelude::*;

    pub use tokio_util::sync::CancellationToken;
}

use future_form::{FutureForm, Sendable};
// FIXME: FutureForm is expensive, can we just for everyone to be Send
// and get over it?
use futures::future::BoxFuture;

use big_sync_core::{
    part_store::{ObjPayload, PartitionStore, PeerPartCursors},
    rpc::{BigSyncRpcClient, BigSyncRpcResult},
    BigSyncCommand, BigSyncMachine, BigSyncMsg, ObjId, PartId, PeerId, TaskCtx, TaskId,
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

pub struct TokioBigSyncWorkerHandle {
    host_tx: big_sync_core::mpsc::Sender<BigSyncCommand>,
}

pub fn spawn_big_sync_worker(
    part_store: Arc<part_store::SqlitePartStoreHandle>,
) -> Res<(TokioBigSyncWorkerHandle, StopToken)> {
    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();

    let machine = big_sync_core::BigSyncMachine::default();
    let (host_tx, host_rx) =
        big_sync_core::mpsc::bounded(64, "BigSyncHost".into(), "BigSyncMachine".into());
    let worker = BigSyncWorker {
        task_set,
        rpc_clients: default(),
        part_store,
        cancel_token: cancel_token.clone(),
        host_rx,
        machine,
        tasks: default(),
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
        TokioBigSyncWorkerHandle { host_tx },
        StopToken {
            cancel_token,
            join_handle,
        },
    ))
}

struct BigSyncIrpcClient {}

#[derive(educe::Educe)]
#[educe(Debug)]
struct BigSyncShutdownToken {
    #[educe(Debug(ignore))]
    err: tokio::sync::OnceCell<eyre::Report>,
    #[educe(Debug(ignore))]
    master_cancel: CancellationToken,
}

impl BigSyncShutdownToken {
    fn set_err(&self, err: eyre::Report) {
        let _ = self.err.set(err);
        self.master_cancel.cancel();
    }
}

struct BigSyncWorker {
    cancel_token: CancellationToken,

    task_set: utils_rs::AbortableJoinSet,

    tasks: HashMap<TaskId, TaskDeets>,
    rpc_clients: HashMap<PeerId, Arc<BigSyncIrpcClient>>,
    part_store: Arc<part_store::SqlitePartStoreHandle>,

    host_rx: big_sync_core::mpsc::Receiver<BigSyncCommand>,
    machine: BigSyncMachine,
}

struct TaskDeets {
    cancel_token: CancellationToken,
    handle: utils_rs::TaskHandle,
}

impl BigSyncWorker {
    async fn machine_loop(mut self) -> Res<()> {
        let mut janitor_tick = tokio::time::interval(Duration::from_millis(500));
        let (task_tx, task_rx) =
            big_sync_core::mpsc::bounded(64, "BigSync tasks".into(), "BigSyncMachine".into());

        let shutdown = BigSyncShutdownToken {
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
                cmd = self.host_rx.recv() => {
                    let cmd = cmd.expect(ERROR_CALLER);
                    self.machine.handle_cmd(cmd);
                }
                msg = task_rx.recv() => {
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
                }
            }

            for task in self.machine.drain_spawn_queue() {
                let task_id = task.id;
                match task.deets {
                    big_sync_core::TaskDeets::SyncTask(_) => todo!(),
                    big_sync_core::TaskDeets::MachineTask(deets) => {
                        let cancel_token = self.cancel_token.child_token();
                        let worker = MachineTaskWorker {
                            task_id,
                            task: deets,
                            part_store: Arc::clone(&self.part_store),
                            bsm_tx: task_tx.clone(),
                            cancel_token: self.cancel_token.child_token(),
                            shutdown: Arc::clone(&shutdown),
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
                    }
                }
            }
        }

        for (_id, task) in self.tasks.drain() {
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

    shutdown: Arc<BigSyncShutdownToken>,
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
