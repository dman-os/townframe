use crate::interlude::*;

pub mod decide_peer_strat;
use decide_peer_strat::*;
mod set_peer_part_cursor;
use set_peer_part_cursor::*;

use crate::{mpsc, rpc::BigSyncRpcClient, scheduler::TaskId, store::PartStore};

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub enum MachineTaskMsg {
        MachineTaskResult (pub struct {
            pub task_id: TaskId,
            pub(crate) deets: pub(crate) enum TaskResultDeets {
                SetPeerPartCursor (()),
                SetPeerStrategy (SetPeerStrategy),
            },
        }),
        MachineTaskError (pub struct {
            pub task_id: TaskId,
            pub(crate) deets: pub(crate) enum MachineTaskErrDeets {
                DecidePeerStrategy(DecidePeerStrategyTaskError)
            },
        })
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct MachineTask {
        /// A task is a single threaded work that's enqueued
        /// by the sync machine and is supposed run concurrently
        /// to the main event loop
        pub id: TaskId,
        pub(crate) deets: pub(crate) enum MachineTaskDeets {
            #![derive(Clone)]
            DecidePeerStrategy(DecidePeerStrategyTask)
            SetPeerPartCursor(SetPeerPartCursorTask)
        }
    }
}
structstruck::strike! {
    #[derive(Clone)]
    #[structstruck::each[derive(Debug)]]
    pub struct SyncTask {
        pub id: TaskId,
        pub kind: SyncTaskKind,
        pub part_hints: Set<PartKey>,
        pub deets: struct SyncTaskDeets {
            #![derive(Clone)]

            pub peer_key: PeerKey,
            pub obj_key: ObjKey,
            pub remote_payload: Option<ObjPayload>,
        }
    }
}

/// What a spawned sync-pipeline task should do. `Sync` fetches/converges the
/// object contents with the peer; `RemoveFromParts` asks the backend to evict
/// the object's membership hints. Both replay through the same keyed
/// scheduling/coalescing machinery and retry transient failures identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncTaskKind {
    Sync,
    RemoveFromParts,
}

structstruck::strike! {
    #[derive(Clone)]
    pub struct SyncTaskSeed {
        pub kind: SyncTaskKind,
        pub part_hints: Set<PartKey>,
        pub deets: SyncTaskDeets,
    }
}

/// Test-support projection of `SchedulerCounts`, so the sync worker's
/// diagnostics do not have to name the scheduler type. Fields mirror it
/// one-for-one, including the single spawn queue, because the frame is single.
#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCounts {
    pub live: usize,
    pub delayed: usize,
    pub spawn_queue: usize,
    pub stop_queue: usize,
}

#[cfg(any(test, feature = "test-support"))]
impl TaskCounts {
    pub fn is_idle(&self) -> bool {
        self.live == 0 && self.delayed == 0 && self.spawn_queue == 0 && self.stop_queue == 0
    }
}

pub struct TaskCtx<K, PStore, Rpc, Rng> {
    pub task_id: TaskId,
    pub main_tx: mpsc::Sender<MachineTaskMsg>,
    pub rpc_clients: Map<PeerKey, Rpc>,
    pub part_store: PStore,
    pub rng: Rng,
    pub _phantom: std::marker::PhantomData<K>,
}

impl MachineTask {
    pub async fn run<K, PStore, Rpc, Rng>(self, mut cx: TaskCtx<K, PStore, Rpc, Rng>)
    where
        K: FutureForm,
        PStore: PartStore<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let res = match self.deets {
            MachineTaskDeets::DecidePeerStrategy(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::DecidePeerStrategy),
            MachineTaskDeets::SetPeerPartCursor(inner) => Ok(inner.run(&mut cx).await),
        };
        let msg = match res {
            Ok(deets) => MachineTaskMsg::MachineTaskResult(MachineTaskResult {
                task_id: cx.task_id,
                deets,
            }),
            Err(deets) => MachineTaskMsg::MachineTaskError(MachineTaskError {
                task_id: cx.task_id,
                deets,
            }),
        };
        cx.main_tx.send(msg).await.expect(ERROR_CHANNEL)
    }
}
