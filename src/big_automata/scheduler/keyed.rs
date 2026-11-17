//! Keyed task replacement over [`Scheduler`].
//!
//! A key has at most one current task. Replacing it stops the old task (the driver aborts it
//! if it was running) and queues the new one, optionally merging the old task's unfinished
//! seed into the replacement. A key can also be *parked*: its seed is kept with no task until
//! an external [`KeyedScheduler::wake`] queues it again.
//!
//! Every report from the driver (`complete`, `fail`) is checked against the key's current
//! task, so a report from a task that was replaced or cancelled is stale and does nothing.

use std::collections::BTreeMap;

use crate::interlude::*;

use super::{Instant, Retry, Scheduler, SchedulerCounts, TaskId, TaskSeed};

strike! {
    #[structstruck::each[derive(Debug)]]
    pub struct KeyedScheduler<Key, Seed> {
        tasks: Scheduler<Seed>,
        active_by_key: BTreeMap<Key, TaskId>,
        key_by_task: BTreeMap<TaskId, Key>,
        /// The seed of each key's current task, or of its parked work when it has no task.
        seed_by_key: BTreeMap<Key, Seed>,
    }
}

impl<Key, Seed> Default for KeyedScheduler<Key, Seed> {
    fn default() -> Self {
        Self {
            tasks: default(),
            active_by_key: default(),
            key_by_task: default(),
            seed_by_key: default(),
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

    /// Replace the key's current or parked work with `seed`, discarding the old seed.
    pub fn replace(&mut self, now: Instant, key: Key, seed: Seed) -> TaskId {
        self.replace_with(now, key, seed, |_old, new| new)
    }

    /// Replace the key's current or parked work, merging the old seed into the new one. The old
    /// task is stopped before the replacement is queued.
    pub fn replace_with(
        &mut self,
        now: Instant,
        key: Key,
        seed: Seed,
        merge: impl FnOnce(Seed, Seed) -> Seed,
    ) -> TaskId {
        let seed = match self.seed_by_key.remove(&key) {
            Some(old) => merge(old, seed),
            None => seed,
        };
        if let Some(old_task) = self.active_by_key.remove(&key) {
            self.key_by_task.remove(&old_task);
            self.tasks.stop(old_task).expect(ERROR_IMPOSSIBLE);
        }
        let task = self.tasks.queue(now, seed.clone());
        self.track(key, task, seed);
        task
    }

    /// Drop the key's current task and any parked work. Returns the stopped task, if there was
    /// one; its late report is stale.
    pub fn cancel(&mut self, key: &Key) -> Option<TaskId> {
        self.seed_by_key.remove(key);
        let task = self.active_by_key.remove(key)?;
        self.key_by_task.remove(&task);
        self.tasks.stop(task).expect(ERROR_IMPOSSIBLE);
        Some(task)
    }

    /// The task finished. Retires its key and hands back the key and its seed, or `None` for a
    /// stale report.
    pub fn complete(&mut self, task: TaskId) -> Option<(Key, Seed)> {
        let key = self.key_by_task.remove(&task)?;
        let current = self.active_by_key.remove(&key);
        assert_eq!(current, Some(task), "a keyed task was not its key's current task");
        self.tasks.complete(task).expect(ERROR_IMPOSSIBLE);
        let seed = self.seed_by_key.remove(&key).expect(ERROR_IMPOSSIBLE);
        Some((key, seed))
    }

    /// The task failed: retry the key's seed after a backoff, keeping the key. Returns the new
    /// task, or `None` for a stale report. A later `replace` for the key replaces the pending
    /// retry.
    pub fn fail(&mut self, task: TaskId, min_delay: Duration, now: Instant) -> Option<TaskId> {
        let key = self.key_by_task.remove(&task)?;
        let seed = self.seed_by_key.get(&key).expect(ERROR_IMPOSSIBLE).clone();
        let retried = self
            .tasks
            .retry(task, seed, min_delay, now)
            .expect(ERROR_IMPOSSIBLE);
        let old = self.active_by_key.insert(key.clone(), retried);
        assert_eq!(old, Some(task), "a keyed task was not its key's current task");
        self.key_by_task.insert(retried, key);
        Some(retried)
    }

    /// Hold `seed` for an idle key until [`Self::wake`].
    pub fn park(&mut self, key: Key, seed: Seed) {
        assert!(
            !self.active_by_key.contains_key(&key),
            "cannot park a key with a current task"
        );
        let old = self.seed_by_key.insert(key, seed);
        assert!(old.is_none(), "key already has parked work");
    }

    /// Queue a parked key's seed. Returns the new task, or `None` if the key has a current task
    /// or nothing parked.
    pub fn wake(&mut self, now: Instant, key: &Key) -> Option<TaskId> {
        if self.active_by_key.contains_key(key) {
            return None;
        }
        let seed = self.seed_by_key.remove(key)?;
        let task = self.tasks.queue(now, seed.clone());
        self.track(key.clone(), task, seed);
        Some(task)
    }

    pub fn active_task(&self, key: &Key) -> Option<TaskId> {
        self.active_by_key.get(key).copied()
    }

    pub fn is_parked(&self, key: &Key) -> bool {
        !self.active_by_key.contains_key(key) && self.seed_by_key.contains_key(key)
    }

    /// The key a live task belongs to.
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

    pub fn requeue_spawned(&mut self, tasks: impl IntoIterator<Item = TaskSeed<Seed>>) {
        self.tasks.requeue_spawned(tasks);
    }

    pub fn drain_stop_queue(&mut self) -> std::vec::Drain<'_, TaskId> {
        self.tasks.drain_stop_queue()
    }

    fn track(&mut self, key: Key, task: TaskId, seed: Seed) {
        let old = self.active_by_key.insert(key.clone(), task);
        assert!(old.is_none(), "key already had a current task");
        let old = self.key_by_task.insert(task, key.clone());
        assert!(old.is_none(), "scheduler task id was reused");
        self.seed_by_key.insert(key, seed);
    }
}

#[cfg(test)]
mod tests;
