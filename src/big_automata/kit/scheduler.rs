//! Task scheduling for the sans-io machines.
//!
//! The machine *queues* tasks; the driver drains the spawn queue and runs them, then reports
//! each one back as completed or failed. A failed task is retried after an exponential
//! backoff. Delayed tasks reach the spawn queue only when the driver calls [`Scheduler::tick`]
//! with a clock reading, so the scheduler never reads a clock itself.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::interlude::*;

pub mod keyed;

use super::Instant;

pub type TaskId = u64;

const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(60);

strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct Scheduler<Seed> {
        /// The cap on retry backoff. Zero falls back to [`DEFAULT_MAX_BACKOFF`].
        max_backoff: Duration,
        next_task_id: TaskId,
        /// Every task the machine still owns, whether queued, delayed or running, with its
        /// retry bookkeeping.
        live: BTreeMap<TaskId, pub struct Retry {
            #![derive(Clone, Copy, PartialEq, Eq)]
            pub attempt_no: usize,
            pub backoff: Duration,
            pub queued_at: Instant,
        }>,
        /// Tasks waiting out their backoff.
        delayed: BTreeMap<TaskId, struct DelayedTask<Seed> {
            seed: Seed,
            due_at: Instant,
        }>,
        /// `delayed` ordered by due time, so the next deadline is the first entry.
        due: BTreeSet<(Instant, TaskId)>,
        spawn_queue: Vec<
            /// A task seed plus its id, drained by the driver for execution.
            pub struct TaskSeed<Seed> {
                pub id: TaskId,
                pub seed: Seed,
            }
        >,
        /// Tasks the driver may be running that it must now abort.
        stop_queue: Vec<TaskId>,
    }
}

/// What [`Scheduler::stop`] retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stopped {
    pub retry: Retry,
    /// The driver was handed the task, so it may still be running: the task is in the stop
    /// queue and the driver confirms when it has ended. `false` for a task that never left
    /// the spawn queue or was waiting out a backoff: nothing runs and nothing is confirmed.
    pub running: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerCounts {
    pub live: usize,
    pub delayed: usize,
    pub spawn_queue: usize,
    pub stop_queue: usize,
}

impl<Seed> Default for Scheduler<Seed> {
    fn default() -> Self {
        Self {
            max_backoff: DEFAULT_MAX_BACKOFF,
            next_task_id: default(),
            live: default(),
            delayed: default(),
            due: default(),
            spawn_queue: default(),
            stop_queue: default(),
        }
    }
}

impl Retry {
    fn first(now: Instant) -> Self {
        Self {
            attempt_no: 1,
            backoff: Duration::ZERO,
            queued_at: now,
        }
    }
}

impl<Seed> Scheduler<Seed> {
    pub fn set_max_backoff(&mut self, max_backoff: Duration) {
        self.max_backoff = max_backoff;
    }

    pub fn counts(&self) -> SchedulerCounts {
        SchedulerCounts {
            live: self.live.len(),
            delayed: self.delayed.len(),
            spawn_queue: self.spawn_queue.len(),
            stop_queue: self.stop_queue.len(),
        }
    }

    /// Queue a task for the driver to spawn on its next drain.
    pub fn queue(&mut self, now: Instant, seed: Seed) -> TaskId {
        let id = self.next_id();
        self.live.insert(id, Retry::first(now));
        self.spawn_queue.push(TaskSeed { id, seed });
        id
    }

    /// The task finished. Returns `None` for a stale completion: the task was stopped while it
    /// ran.
    pub fn complete(&mut self, id: TaskId) -> Option<Retry> {
        let retry = self.live.remove(&id)?;
        assert!(
            !self.delayed.contains_key(&id) && !self.spawn_queue.iter().any(|task| task.id == id),
            "completion for task {id} the driver was never handed"
        );
        Some(retry)
    }

    /// Cancel a task. One still waiting out its backoff, or still in the spawn queue, simply
    /// vanishes; one the driver may be running enters the stop queue so the driver aborts it.
    /// Returns `None` if the task was already retired.
    ///
    /// [`Stopped::running`] tells the caller whether the task may still be running: the abort
    /// is only a request, and the driver confirms the task has ended some time after it, so a
    /// caller that must not start a replacement beside the old task waits for that (see
    /// [`keyed::KeyedScheduler`]).
    pub fn stop(&mut self, id: TaskId) -> Option<Stopped> {
        let retry = self.live.remove(&id)?;
        if let Some(delayed) = self.delayed.remove(&id) {
            self.due.remove(&(delayed.due_at, id));
            return Some(Stopped {
                retry,
                running: false,
            });
        }
        let queued_len = self.spawn_queue.len();
        self.spawn_queue.retain(|task| task.id != id);
        let running = self.spawn_queue.len() == queued_len;
        if running {
            self.stop_queue.push(id);
        }
        Some(Stopped { retry, running })
    }

    /// The task failed: retire it and queue `seed` again after a backoff. Returns the new task
    /// id, or `None` for a stale failure (the task was stopped while it ran), in which case
    /// the seed is dropped.
    ///
    /// The first retry waits `min_delay`; later retries double the previous backoff, floored
    /// by `min_delay`. Both are capped by the max backoff, and the cap wins over the floor: it
    /// is the embedder's pacing knob (daybook tests clamp it to 500ms so a route whose grant
    /// is still in flight is picked up quickly).
    pub fn retry(
        &mut self,
        id: TaskId,
        seed: Seed,
        min_delay: Duration,
        now: Instant,
    ) -> Option<TaskId> {
        let prev = self.complete(id)?;
        let max_backoff = if self.max_backoff.is_zero() {
            DEFAULT_MAX_BACKOFF
        } else {
            self.max_backoff
        };
        let backoff = if prev.backoff.is_zero() {
            min_delay.min(max_backoff)
        } else {
            prev.backoff
                .saturating_mul(2)
                .max(min_delay)
                .min(max_backoff)
        };
        let retry = Retry {
            attempt_no: prev.attempt_no + 1,
            backoff,
            queued_at: now,
        };
        let due_at = now + backoff;
        let next = self.next_id();
        self.live.insert(next, retry);
        self.delayed.insert(next, DelayedTask { seed, due_at });
        self.due.insert((due_at, next));
        Some(next)
    }

    /// The earliest deadline the driver should pass to [`Self::tick`]. `None` means nothing is
    /// waiting, so the driver can wait on events alone.
    pub fn next_due(&self) -> Option<Instant> {
        self.due.first().map(|(due_at, _)| *due_at)
    }

    /// Move every delayed task due at or before `now` onto the spawn queue, earliest first.
    pub fn tick(&mut self, now: Instant) {
        while let Some(&(due_at, id)) = self.due.first() {
            if due_at > now {
                break;
            }
            self.due.pop_first();
            let delayed = self.delayed.remove(&id).expect(ERROR_IMPOSSIBLE);
            self.spawn_queue.push(TaskSeed {
                id,
                seed: delayed.seed,
            });
        }
    }

    /// Hand queued tasks to the driver, in queue order.
    pub fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, TaskSeed<Seed>> {
        self.spawn_queue.drain(..)
    }

    /// Put back drained tasks the driver had no budget to run; they stay first in line.
    pub fn requeue_spawned(&mut self, tasks: impl IntoIterator<Item = TaskSeed<Seed>>) {
        let rest = std::mem::take(&mut self.spawn_queue);
        self.spawn_queue.extend(tasks);
        self.spawn_queue.extend(rest);
    }

    /// Hand stopped task ids to the driver so it can abort their running work.
    pub fn drain_stop_queue(&mut self) -> std::vec::Drain<'_, TaskId> {
        self.stop_queue.drain(..)
    }

    /// The retry bookkeeping of a live task.
    pub fn retry_of(&self, id: TaskId) -> Option<Retry> {
        self.live.get(&id).copied()
    }

    fn next_id(&mut self) -> TaskId {
        let id = self.next_task_id;
        self.next_task_id += 1;
        id
    }
}

#[cfg(test)]
mod tests;
