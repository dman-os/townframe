use std::time::Instant;

use crate::interlude::*;

pub type TaskId = u64;

strike! {
    pub struct Scheduler<Seed> {
        next_task_id: TaskId,
        /// Every allocated-but-not-stopped task id with its retry bookkeeping.
        all: HashMap<TaskId, Retry>,
        spawn_queue: Vec<
            /// A task seed plus its id, drained by the driver for execution.
            pub struct TaskSeed<Seed> {
                #![derive(Debug)]
                pub id: TaskId,
                pub seed: Seed,
            }
        >,
        stop_queue: HashSet<TaskId>,
    }
}

impl<Seed> Default for Scheduler<Seed> {
    fn default() -> Self {
        Self {
            next_task_id: default(),
            all: default(),
            spawn_queue: default(),
            stop_queue: default(),
        }
    }
}

impl<Seed> Scheduler<Seed> {
    pub fn queue(&mut self, now: Instant, seed: Seed) -> TaskId {
        let id = self.next_task_id;
        self.next_task_id += 1;
        self.all.insert(
            id,
            Retry {
                attempt_no: 1,
                backoff: Duration::ZERO,
                queued_at: now,
            },
        );
        self.spawn_queue.push(TaskSeed { id, seed });
        id
    }

    pub fn stop(&mut self, id: TaskId) -> Option<Retry> {
        let retry = self.all.remove(&id)?;
        self.spawn_queue.retain(|task| task.id != id);
        self.stop_queue.insert(id);
        Some(retry)
    }

    pub fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, TaskSeed<Seed>> {
        self.spawn_queue.drain(..)
    }

    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, TaskId> {
        self.stop_queue.drain()
    }
}

pub struct Retry {
    attempt_no: usize,
    backoff: Duration,
    queued_at: Instant,
}

impl Retry {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            attempt_no: 1,
            backoff: Duration::ZERO,
            queued_at: now,
        }
    }
}
