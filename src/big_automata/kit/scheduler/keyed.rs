//! Keyed task replacement over [`Scheduler`].
//!
//! A key has at most one current task. Replacing it stops the old task and queues the new one,
//! optionally merging the old task's unfinished seed into the replacement. A key can also be
//! *parked*: its seed is kept with no task until an external [`KeyedScheduler::wake`] queues it
//! again.
//!
//! # Stopped before replaced
//!
//! Stopping a task is a request: the driver aborts it and the aborted future may keep running
//! until its next poll (blocking work and non-cancel-safe sections run on). Starting the
//! replacement at once would run two tasks for one key and put more tasks in flight than the
//! caller's slot count allows. So stopping a task the driver may be running (one handed out by
//! [`KeyedScheduler::drain_spawn_queue`] and not yet reported) puts its key in the *stopping*
//! state:
//!
//! 1. The old task enters the stop queue, and the key stays busy: [`KeyedScheduler::replace_with`]
//!    queues nothing and returns `None`, merging the new seed into the one waiting for the key.
//!    A task that never left the spawn queue, or was waiting out a backoff, runs nowhere and
//!    is replaced immediately as before.
//! 2. The driver reports [`KeyedScheduler::stopped`] once for every task it drained from the stop
//!    queue, when that task is no longer running. Only then is the waiting replacement queued
//!    (and parked work parked, a cancelled key freed).
//! 3. Whatever the stopping task reports meanwhile (`complete`, `fail`) is stale and changes
//!    nothing: the result belongs to a task its key gave up on, and the key frees on `stopped`
//!    alone.
//!
//! Every other report from the driver is checked against the key's current task, so a report from
//! a task that was replaced or cancelled is stale and does nothing.

use std::collections::BTreeMap;

use crate::interlude::*;

use super::Instant;
use super::Retry;
use super::Scheduler;
use super::SchedulerCounts;
use super::TaskId;
use super::TaskSeed;

/// What a key holds. A key with no entry is idle.
#[derive(Debug)]
enum Slot<Seed> {
    /// Work held with no task until [`KeyedScheduler::wake`].
    Parked(Seed),
    /// The key's current task and its seed, for a retry or a merge.
    Active { task: TaskId, seed: Seed },
    /// The driver may still be running `task`, which the key gave up on. The key is busy until
    /// the driver reports it ended.
    Stopping { task: TaskId, then: Then<Seed> },
}

/// What a stopping key becomes once its old task has ended.
#[derive(Debug)]
enum Then<Seed> {
    Idle,
    Queue(Seed),
    Park(Seed),
}

#[derive(Debug)]
pub struct KeyedScheduler<Key, Seed> {
    tasks: Scheduler<Seed>,
    slots: BTreeMap<Key, Slot<Seed>>,
    /// The keys of current (`Active`) tasks.
    key_by_task: BTreeMap<TaskId, Key>,
    /// The keys of tasks the driver has yet to report ended.
    stopping_by_task: BTreeMap<TaskId, Key>,
}

impl<Key, Seed> Default for KeyedScheduler<Key, Seed> {
    fn default() -> Self {
        Self {
            tasks: default(),
            slots: default(),
            key_by_task: default(),
            stopping_by_task: default(),
        }
    }
}

impl<Key, Seed> KeyedScheduler<Key, Seed>
where
    Key: Ord + Clone,
    Seed: Clone,
{
    pub fn set_max_backoff(&mut self, max_backoff: Duration) {
        self.tasks.set_max_backoff(max_backoff);
    }

    /// Replace the key's current or parked work with `seed`, discarding the old seed. See
    /// [`Self::replace_with`] for the return value.
    pub fn replace(&mut self, now: Instant, key: Key, seed: Seed) -> Option<TaskId> {
        self.replace_with(now, key, seed, |_old, new| new)
    }

    /// Replace the key's current or parked work, merging the old seed into the new one.
    ///
    /// Returns the replacement's task, or `None` when the key's old task may still be running:
    /// the replacement then waits, with the merged seed, until [`Self::stopped`] reports the old
    /// task ended (see the module docs). A later replace of a waiting key merges into the seed
    /// that waits.
    pub fn replace_with(
        &mut self,
        now: Instant,
        key: Key,
        seed: Seed,
        merge: impl FnOnce(Seed, Seed) -> Seed,
    ) -> Option<TaskId> {
        match self.slots.remove(&key) {
            None => Some(self.start(now, key, seed)),
            Some(Slot::Parked(old)) => Some(self.start(now, key, merge(old, seed))),
            Some(Slot::Active { task, seed: old }) => {
                match self.stop_current(task, &key, Then::Queue(merge(old, seed))) {
                    None => None,
                    Some(Then::Queue(seed)) => Some(self.start(now, key, seed)),
                    Some(_) => unreachable!("{ERROR_IMPOSSIBLE}"),
                }
            }
            Some(Slot::Stopping { task, then }) => {
                let seed = match then {
                    Then::Idle => seed,
                    Then::Queue(old) | Then::Park(old) => merge(old, seed),
                };
                self.slots.insert(
                    key,
                    Slot::Stopping {
                        task,
                        then: Then::Queue(seed),
                    },
                );
                None
            }
        }
    }

    /// Drop the key's current task and any parked or waiting work. Returns the task the driver
    /// was asked to stop, if there was one; its late report is stale, and the key stays busy
    /// until [`Self::stopped`] if the task may be running. A key already stopping returns
    /// `None`: its task was returned by the call that stopped it.
    pub fn cancel(&mut self, key: &Key) -> Option<TaskId> {
        match self.slots.remove(key)? {
            Slot::Parked(_) => None,
            Slot::Active { task, .. } => {
                // Nothing waits behind a cancel, so there is nothing to start if the task
                // ran nowhere.
                self.stop_current(task, key, Then::Idle);
                Some(task)
            }
            Slot::Stopping { task, .. } => {
                self.slots.insert(
                    key.clone(),
                    Slot::Stopping {
                        task,
                        then: Then::Idle,
                    },
                );
                None
            }
        }
    }

    /// The driver reports that a task it drained from the stop queue is no longer running.
    /// Frees its key: the waiting replacement is queued and its task returned, parked work is
    /// parked, and a cancelled key goes idle. Whatever the task reported meanwhile was stale.
    ///
    /// Panics for a task that is not stopping: the driver reports each stopped task exactly
    /// once.
    pub fn stopped(&mut self, now: Instant, task: TaskId) -> Option<TaskId> {
        let key = self
            .stopping_by_task
            .remove(&task)
            .expect("stopped for a task that is not stopping");
        let Some(Slot::Stopping { task: held, then }) = self.slots.remove(&key) else {
            unreachable!("{ERROR_IMPOSSIBLE}")
        };
        assert_eq!(held, task, "a key's stopping task was not the reported one");
        match then {
            Then::Idle => None,
            Then::Queue(seed) => Some(self.start(now, key, seed)),
            Then::Park(seed) => {
                self.slots.insert(key, Slot::Parked(seed));
                None
            }
        }
    }

    /// The task finished. Retires its key and hands back the key and its seed, or `None` for a
    /// stale report (including one from a stopping task).
    pub fn complete(&mut self, task: TaskId) -> Option<(Key, Seed)> {
        let key = self.key_by_task.remove(&task)?;
        let Some(Slot::Active {
            task: current,
            seed,
        }) = self.slots.remove(&key)
        else {
            unreachable!("{ERROR_IMPOSSIBLE}")
        };
        assert_eq!(current, task, "a keyed task was not its key's current task");
        self.tasks.complete(task).expect(ERROR_IMPOSSIBLE);
        Some((key, seed))
    }

    /// The task failed: retry the key's seed after a backoff, keeping the key. Returns the new
    /// task, or `None` for a stale report (including one from a stopping task). A later
    /// `replace` for the key replaces the pending retry.
    pub fn fail(&mut self, task: TaskId, min_delay: Duration, now: Instant) -> Option<TaskId> {
        let key = self.key_by_task.remove(&task)?;
        let Some(Slot::Active {
            task: current,
            seed,
        }) = self.slots.get_mut(&key)
        else {
            unreachable!("{ERROR_IMPOSSIBLE}")
        };
        assert_eq!(
            *current, task,
            "a keyed task was not its key's current task"
        );
        let retried = self
            .tasks
            .retry(task, seed.clone(), min_delay, now)
            .expect(ERROR_IMPOSSIBLE);
        *current = retried;
        self.key_by_task.insert(retried, key);
        Some(retried)
    }

    /// Hold `seed` for an idle key until [`Self::wake`]. A key that is stopping, with nothing
    /// waiting, parks once its task has ended.
    pub fn park(&mut self, key: Key, seed: Seed) {
        match self.slots.get_mut(&key) {
            None => {
                self.slots.insert(key, Slot::Parked(seed));
            }
            Some(Slot::Stopping {
                then: then @ Then::Idle,
                ..
            }) => *then = Then::Park(seed),
            Some(Slot::Active { .. }) => panic!("cannot park a key with a current task"),
            Some(_) => panic!("key already has parked work"),
        }
    }

    /// Queue a parked key's seed. Returns the new task, or `None` if the key has a current task
    /// or nothing parked. A stopping key's parked work is queued once its task has ended.
    pub fn wake(&mut self, now: Instant, key: &Key) -> Option<TaskId> {
        match self.slots.get_mut(key)? {
            Slot::Parked(_) => {
                let Some(Slot::Parked(seed)) = self.slots.remove(key) else {
                    unreachable!("{ERROR_IMPOSSIBLE}")
                };
                Some(self.start(now, key.clone(), seed))
            }
            Slot::Stopping { then, .. } => {
                if let Then::Park(seed) = then {
                    *then = Then::Queue(seed.clone());
                }
                None
            }
            Slot::Active { .. } => None,
        }
    }

    pub fn active_task(&self, key: &Key) -> Option<TaskId> {
        match self.slots.get(key)? {
            Slot::Active { task, .. } => Some(*task),
            Slot::Parked(_) | Slot::Stopping { .. } => None,
        }
    }

    /// The task the driver has yet to report ended, if the key is stopping.
    pub fn stopping_task(&self, key: &Key) -> Option<TaskId> {
        match self.slots.get(key)? {
            Slot::Stopping { task, .. } => Some(*task),
            Slot::Parked(_) | Slot::Active { .. } => None,
        }
    }

    /// Tasks the driver has yet to report ended. They count against any slot budget: they may
    /// still be running.
    pub fn stopping(&self) -> usize {
        self.stopping_by_task.len()
    }

    /// Whether the key holds parked work, waiting for a wake (or, on a stopping key, for the
    /// task to end).
    pub fn is_parked(&self, key: &Key) -> bool {
        matches!(
            self.slots.get(key),
            Some(
                Slot::Parked(_)
                    | Slot::Stopping {
                        then: Then::Park(_),
                        ..
                    }
            )
        )
    }

    /// The key a current task belongs to. A stopping task has no key to report for.
    pub fn key_of(&self, task: TaskId) -> Option<&Key> {
        self.key_by_task.get(&task)
    }

    pub fn retry_of(&self, task: TaskId) -> Option<Retry> {
        self.tasks.retry_of(task)
    }

    pub fn counts(&self) -> SchedulerCounts {
        self.tasks.counts()
    }

    pub fn next_due(&self) -> Option<Instant> {
        self.tasks.next_due()
    }

    pub fn tick(&mut self, now: Instant) {
        self.tasks.tick(now);
    }

    pub fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, TaskSeed<Seed>> {
        self.tasks.drain_spawn_queue()
    }

    /// Tasks the driver must abort. It reports each one back through [`Self::stopped`] once it
    /// has ended.
    pub fn drain_stop_queue(&mut self) -> std::vec::Drain<'_, TaskId> {
        self.tasks.drain_stop_queue()
    }

    /// Queue a task for `key` and make it the key's current one.
    fn start(&mut self, now: Instant, key: Key, seed: Seed) -> TaskId {
        let task = self.tasks.queue(now, seed.clone());
        let old = self.key_by_task.insert(task, key.clone());
        assert!(old.is_none(), "scheduler task id was reused");
        let old = self.slots.insert(key, Slot::Active { task, seed });
        assert!(old.is_none(), "key already had work");
        task
    }

    /// Stop the key's current `task`, whose slot the caller already took out, leaving `then`
    /// waiting for it. If the task runs nowhere the key is left idle and `then` comes back for
    /// the caller to carry out at once.
    fn stop_current(&mut self, task: TaskId, key: &Key, then: Then<Seed>) -> Option<Then<Seed>> {
        self.key_by_task.remove(&task);
        let stopped = self.tasks.stop(task).expect(ERROR_IMPOSSIBLE);
        if !stopped.running {
            return Some(then);
        }
        self.stopping_by_task.insert(task, key.clone());
        self.slots
            .insert(key.clone(), Slot::Stopping { task, then });
        None
    }
}

#[cfg(test)]
mod tests;
