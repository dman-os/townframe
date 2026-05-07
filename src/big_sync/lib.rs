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

pub struct TokioBigSyncMachineHandle {
    host_tx: big_sync_core::mpsc::Sender<BigSyncCommand>,
}

pub fn spawn_big_sync_machine(
    part_store: Arc<part_store::SqlitePartStoreHandle>,
) -> Res<(TokioBigSyncMachineHandle, StopToken)> {
    let cancel_token = CancellationToken::new();
    let task_set = utils_rs::AbortableJoinSet::new();

    let (host_tx, host_rx) =
        big_sync_core::mpsc::bounded(64, "BigSyncHost".into(), "BigSyncMachine".into());
    let (task_inbox_tx, task_inbox_rx) =
        big_sync_core::mpsc::bounded(64, "BigSync tasks".into(), "BigSyncMachine".into());

    let machine = big_sync_core::BigSyncMachine::default();
    let cx = TokioBigSyncCtx {
        host_rx,
        task_inbox_tx,
        task_inbox_rx,
        task_set,
        rpc_clients: default(),
        part_store,
        cancel_token: cancel_token.clone(),
        err: default(),
    };
    let cx = Arc::new(cx);
    let join_handle = tokio::task::spawn(
        {
            let cancen_token = cancel_token.clone();
            async move {
                if let Some(Err(err)) = cancen_token
                    .run_until_cancelled(main_loop(cx, machine))
                    .await
                {
                    panic!("error: {err:?}")
                }
            }
        }
        .in_current_span(),
    );

    Ok((
        TokioBigSyncMachineHandle { host_tx },
        StopToken {
            cancel_token,
            join_handle,
        },
    ))
}

struct BigSyncIrpcClient {}

struct TokioBigSyncCtx {
    cancel_token: CancellationToken,

    task_inbox_tx: big_sync_core::mpsc::Sender<BigSyncMsg>,
    task_inbox_rx: big_sync_core::mpsc::Receiver<BigSyncMsg>,
    host_rx: big_sync_core::mpsc::Receiver<BigSyncCommand>,
    task_set: utils_rs::AbortableJoinSet,

    rpc_clients: HashMap<PeerId, Arc<BigSyncIrpcClient>>,
    part_store: Arc<part_store::SqlitePartStoreHandle>,

    err: tokio::sync::OnceCell<eyre::Report>,
}

impl TokioBigSyncCtx {
    async fn err_out(&self, err: eyre::Report) -> Res<Never> {
        self.err.set(err);
        self.cancel_token.cancel();
        std::future::pending::<()>().await;
        unreachable!()
    }
}

async fn main_loop(cx: Arc<TokioBigSyncCtx>, mut machine: BigSyncMachine) -> Res<()> {
    let mut tasks = HashMap::new();
    loop {
        let (err_tx, err_rx) = oneshot::channel();
        let ecx = Yielder {
            err_tx: Some(err_tx).into(),
        };
        let part_store = EffectedTaskStore {
            ecx,
            inner: Arc::clone(&cx.part_store),
        };
        let res = tokio::select! {
            biased;
            res = err_rx => {
                let err = res.expect(ERROR_IMPOSSIBLE);
                cx.err_out(err).await?;
            }
            () = machine.run(&cx.host_rx, &cx.task_inbox_rx, &part_store) =>{
            }
        };

        for task in machine.tasks_mut().drain_spawn_queue() {
            let id = task.id;
            match task.deets {
                big_sync_core::TaskDeets::SyncTask(cursor_machine_command) => todo!(),
                big_sync_core::TaskDeets::MachineTask(deets) => {
                    let handle = cx
                        .task_set
                        .spawn(machine_task_loop(cx.clone(), id, deets))
                        .wrap_err(ERROR_CANCELLED)?;
                    tasks.insert(id, handle);
                }
            }
        }
        // TODO: stop queue
    }
}

async fn machine_task_loop(
    cx: Arc<TokioBigSyncCtx>,
    task_id: TaskId,
    task: big_sync_core::MachineTask,
) {
    let (err_tx, err_rx) = oneshot::channel();
    let ecx = Yielder {
        err_tx: Some(err_tx).into(),
    };
    let part_store = EffectedTaskStore {
        ecx,
        inner: Arc::clone(&cx.part_store),
    };
    let tcx: TaskCtx<_, _, EffectedBigSyncRpc> = TaskCtx {
        task_id: task_id,
        main_tx: cx.task_inbox_tx.clone(),
        part_store,
        rpc_clients: default(),
        _phantom: default(),
    };
    let res = tokio::select! {
        biased;
        res = cx.cancel_token.cancelled() => {
        },
        err = err_rx => {
        },
        () = task.run(tcx) => {
        }
    };
}

struct Yielder<E> {
    err_tx: std::sync::Mutex<Option<oneshot::Sender<E>>>,
}

enum Never {}

impl<E> Yielder<E> {
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
