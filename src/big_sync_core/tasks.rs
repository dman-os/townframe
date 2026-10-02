use crate::interlude::*;

pub mod decide_peer_strat;
use decide_peer_strat::*;
pub mod replay_page;
use replay_page::{ReplayPageResult, ReplayPageTask, ReplayPageTaskError};
pub mod list_bucket;
use list_bucket::{ListBucketsResult, ListBucketsTask, ListBucketsTaskError};
pub mod leaf_buckets;
use leaf_buckets::{LeafBucketsResult, LeafBucketsTask, LeafBucketsTaskError};

use crate::{mpsc, part_store::PartStoreReadOnly, rpc::BigSyncRpcClient};
use std::time::Instant;

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub enum MachineTaskMsg {
        MachineTaskResult (pub struct {
            pub task_id: TaskId,
            pub(crate) deets: pub(crate) enum TaskResultDeets {
                SetPeerStrategy (SetPeerStrategy),
                ListBuckets (ListBucketsResult),
                LeafBuckets (LeafBucketsResult),
                ReplayPage (ReplayPageResult),
            },
        }),
        MachineTaskError (pub struct {
            pub task_id: TaskId,
            pub(crate) deets: pub(crate) enum MachineTaskErrDeets {
                DecidePeerStrategy(DecidePeerStrategyTaskError)
                ReplayPage(ReplayPageTaskError)
                ListBuckets(ListBucketsTaskError)
                LeafBuckets(LeafBucketsTaskError)
            },
        })
    }
}

pub type TaskId = u64;

structstruck::strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct MachineTask {
        /// A task is a single threaded work that's enqueued
        /// by the sync machine and is supposed run concurrently
        /// to the main event loop
        pub id: TaskId,
        pub(crate) deets: pub(crate) enum MachineTaskDeets {
            #![derive(Clone)]
            DecidePeerStrategy (DecidePeerStrategyTask)
            ReplayPage(ReplayPageTask)
            ListBuckets(ListBucketsTask)
            LeafBuckets(LeafBucketsTask)
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

            pub peer_id: PeerKey,
            pub obj_id: ObjKey,
            pub remote_payload: Option<crate::part_store::ObjPayload>,
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

/// The retry bookkeeping carried through the task frame.
///
/// There is one such type because there is one frame: the machine's handlers
/// only ever hand this value straight back to the scheduler that produced it.
/// A second structurally-identical type here would exist only to be converted.
pub use crate::scheduler::Retry;

impl Retry {
    /// A fresh retry state (first attempt, no backoff). Used when a removal
    /// failure arrives for a task that was already stopped (cancelled by a
    /// re-add): the cancelled path never consumes the retry, so a fresh
    /// value is only a placeholder.
    pub(crate) fn fresh(now: Instant) -> Self {
        Self {
            attempt_no: 1,
            backoff: Duration::ZERO,
            queued_at: now,
        }
    }
}

#[derive(Clone)]
pub enum TaskSeed {
    Machine(MachineTaskDeets),
    Sync(SyncTaskSeed),
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
        PStore: PartStoreReadOnly<K>,
        Rpc: BigSyncRpcClient<K>,
        Rng: rand::Rng,
    {
        let res = match self.deets {
            MachineTaskDeets::DecidePeerStrategy(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::DecidePeerStrategy),
            MachineTaskDeets::ReplayPage(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::ReplayPage),
            MachineTaskDeets::ListBuckets(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::ListBuckets),
            MachineTaskDeets::LeafBuckets(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::LeafBuckets),
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
