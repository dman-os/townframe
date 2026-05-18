use crate::interlude::*;

pub mod decide_peer_strat;
use decide_peer_strat::*;
pub mod peer_replay;
use peer_replay::{PeerReplayTask, PeerReplayWorkerError, PeerReplayWorkerMsg};
pub mod list_bucket;
use list_bucket::{ListBucketsResult, ListBucketsTask, ListBucketsTaskError};
pub mod leaf_buckets;
use leaf_buckets::{LeafBucketsResult, LeafBucketsTask, LeafBucketsTaskError};

use crate::{mpsc, part_store::PartStore, rpc::BigSyncRpcClient};
use std::time::Instant;

structstruck::strike! {
    pub enum MachineTaskMsg {
        MachineTaskResult (pub struct {
            pub task_id: TaskId,
            pub(crate) deets: pub(crate) enum TaskResultDeets {
                SetPeerStrategy (SetPeerStrategy),
                ListBuckets (ListBucketsResult),
                LeafBuckets (LeafBucketsResult),
            },
        }),
        MachineTaskError (pub struct {
            pub task_id: TaskId,
            pub(crate) deets: pub(crate) enum MachineTaskErrDeets {
                DecidePeerStrategyError(DecidePeerStrategyTaskError)
                PeerReplayWorkerError(PeerReplayWorkerError)
                ListBucketsError(ListBucketsTaskError)
                LeafBucketsError(LeafBucketsTaskError)
            },
        })
        PeerReplayWorker (PeerReplayWorkerMsg)
    }
}

pub type TaskId = u64;

structstruck::strike! {
    pub struct MachineTask {
        /// A task is a single threaded work that's enqueued
        /// by the sync machine and is supposed run concurrently
        /// to the main event loop
        pub id: TaskId,
        pub(crate) deets: pub(crate) enum MachineTaskDeets {
            DecidePeerStrategy (DecidePeerStrategyTask)
            PeerReplay(PeerReplayTask)
            ListBuckets(ListBucketsTask)
            LeafBuckets(LeafBucketsTask)
        }
    }
}
structstruck::strike! {
    #[derive(Clone)]
    pub struct SyncTask {
        pub id: TaskId,
        pub deets: struct SyncTaskDeets {
            #![derive(Clone)]

            pub peer_id: PeerId,
            pub obj_id: ObjId,
            pub part_hints: Set<PartId>,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Retry {
    pub attempt_no: usize,
    backoff: Duration,
    queued_at: Instant,
}

pub enum TaskSeed {
    Machine(MachineTaskDeets),
    Sync(SyncTaskDeets),
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskCounts {
    pub all: usize,
    pub pending: usize,
    pub sync_spawn_queue: usize,
    pub machine_spawn_queue: usize,
    pub stop_queue: usize,
}

#[cfg(any(test, feature = "test-support"))]
impl TaskCounts {
    pub fn is_idle(&self) -> bool {
        self.all == 0
            && self.pending == 0
            && self.sync_spawn_queue == 0
            && self.machine_spawn_queue == 0
            && self.stop_queue == 0
    }
}

structstruck::strike! {
    #[derive(Default)]
    pub struct Tasks {
        next_id: TaskId,
        all: Map<TaskId, pub struct TaskState {
            pub retry: Retry,
        }>,
        pending: Map<TaskId, (TaskSeed, Instant)>,
        sync_spawn_queue: VecDeque<SyncTask>,
        machine_spawn_queue: VecDeque<MachineTask>,
        stop_queue: Set<TaskId>,
    }
}

impl Tasks {
    #[cfg(any(test, feature = "test-support"))]
    pub fn task_counts(&self) -> TaskCounts {
        TaskCounts {
            all: self.all.len(),
            pending: self.pending.len(),
            sync_spawn_queue: self.sync_spawn_queue.len(),
            machine_spawn_queue: self.machine_spawn_queue.len(),
            stop_queue: self.stop_queue.len(),
        }
    }
    pub fn stop_task(&mut self, id: TaskId) -> Option<TaskState> {
        let old = self.all.remove(&id);
        if self.pending.remove(&id).is_some() {
            // we only remove the sttate if the
            // task isn't alive
            return old;
        }
        self.sync_spawn_queue.retain(|task| task.id != id);
        self.machine_spawn_queue.retain(|task| task.id != id);
        self.stop_queue.insert(id);
        old
    }

    pub fn spawn_task(&mut self, seed: TaskSeed) -> TaskId {
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
        match seed {
            TaskSeed::Sync(deets) => self.sync_spawn_queue.push_back(SyncTask { id, deets }),
            TaskSeed::Machine(deets) => self
                .machine_spawn_queue
                .push_back(MachineTask { id, deets }),
        }
        id
    }

    pub fn spawn_delayed_task(
        &mut self,
        seed: TaskSeed,
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
        self.pending.insert(id, (seed, due_at));
        id
    }

    pub fn enqueue_due_tasks(&mut self, now: Instant) {
        let due_task_ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(task_id, (_, due_at))| (*due_at <= now).then_some(*task_id))
            .collect();
        for id in due_task_ids {
            let Some((seed, _)) = self.pending.remove(&id) else {
                continue;
            };
            match seed {
                TaskSeed::Sync(deets) => self.sync_spawn_queue.push_back(SyncTask { id, deets }),
                TaskSeed::Machine(deets) => self
                    .machine_spawn_queue
                    .push_back(MachineTask { id, deets }),
            }
        }
    }

    pub fn pop_sync_spawn_queue(&mut self) -> Option<SyncTask> {
        self.sync_spawn_queue.pop_front()
    }

    pub fn pop_machine_spawn_queue(&mut self) -> Option<MachineTask> {
        self.machine_spawn_queue.pop_front()
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, u64> {
        self.stop_queue.drain()
    }
}

pub struct TaskCtx<K: FutureForm, PStore: PartStore<K>, Rpc: BigSyncRpcClient<K>, Rng: rand::Rng> {
    pub task_id: TaskId,
    pub main_tx: mpsc::Sender<MachineTaskMsg>,
    pub rpc_clients: Map<PeerId, Rpc>,
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
                .map_err(MachineTaskErrDeets::DecidePeerStrategyError),
            MachineTaskDeets::PeerReplay(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::PeerReplayWorkerError),
            MachineTaskDeets::ListBuckets(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::ListBucketsError),
            MachineTaskDeets::LeafBuckets(inner) => inner
                .run(&mut cx)
                .await
                .map_err(MachineTaskErrDeets::LeafBucketsError),
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
