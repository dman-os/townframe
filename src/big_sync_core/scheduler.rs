//! Generic task scheduler extracted from [`crate::tasks::Tasks`].
//!
//! Same lifecycle semantics as the existing sync machine's task table:
//!
//! - Tasks are spawned via a spawn queue that the driver drains (spawn is a
//!   *decision*, execution stays on the driver side).
//! - A failed/stopped task either disappears (if it never left the pending
//!   backoff queue) or enters the stop queue so the driver can cancel its
//!   running future. See `Tasks::stop_task`.
//! - Retries re-enter a delayed queue with exponential backoff doubling,
//!   floored by `min_delay`, capped by `max_backoff` (with the same 1-minute
//!   fallback when the cap is configured to zero). See
//!   `Tasks::spawn_delayed_task` — the formula below is verbatim.
//! - Due tasks move from the delayed queue to the spawn queue only when an
//!   external tick supplies a clock reading (`tick(now)`), keeping the core
//!   sans-io and deterministic under test.
//!
//! Deviations from `Tasks`, both forced by genericity and documented here:
//! - The concrete `TaskSeed::{Sync, Machine}` split becomes a single
//!   user-supplied `Seed` type; the dual spawn queues existed only because
//!   `Tasks` knew those two concrete kinds. Drivers route seeds to their own
//!   executors after draining.
//! - `Instant::now()` calls inside `Tasks::spawn_task/spawn_delayed_task`
//!   become explicit `now: Instant` parameters (sans-io / determinism).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

pub type TaskId = u64;

/// Retry bookkeeping attached to every live task. Mirrors `tasks::Retry`.
#[derive(Debug, Clone, Copy)]
pub struct Retry {
    pub attempt_no: usize,
    pub backoff: Duration,
    pub queued_at: Instant,
}

/// A task seed plus its id, drained by the driver for execution.
#[derive(Debug, Clone)]
pub struct SpawnedTask<Seed> {
    pub id: TaskId,
    pub seed: Seed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerCounts {
    pub live: usize,
    pub delayed: usize,
    pub spawn_queue: usize,
    pub stop_queue: usize,
}

/// The scheduler proper. Pure state + pure transitions; the clock is always
/// supplied by the caller.
#[derive(Debug)]
pub struct Scheduler<Seed> {
    max_backoff: Duration,
    next_id: TaskId,
    /// Every allocated-but-not-stopped task id with its retry bookkeeping.
    live: HashMap<TaskId, Retry>,
    /// Tasks waiting out their backoff: id -> (seed, due_at).
    delayed: BTreeMap<TaskId, (Seed, Instant)>,
    spawn_queue: Vec<SpawnedTask<Seed>>,
    stop_queue: HashSet<TaskId>,
    /// Ids that were legitimately stopped and may still be respawned once.
    /// Consumed by `respawn_delayed`, which refuses to resurrect a task that
    /// never went through `stop`.
    stopped: HashSet<TaskId>,
}

impl<Seed: Clone> Default for Scheduler<Seed> {
    fn default() -> Self {
        Self {
            max_backoff: Duration::from_secs(60),
            next_id: 0,
            live: Default::default(),
            delayed: Default::default(),
            spawn_queue: Default::default(),
            stop_queue: Default::default(),
            stopped: Default::default(),
        }
    }
}

impl<Seed: Clone> Scheduler<Seed> {
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

    /// Spawn immediately. Mirrors `Tasks::spawn_task` (clock injected).
    pub fn spawn(&mut self, now: Instant, seed: Seed) -> TaskId {
        let id = self.next_id;
        self.next_id += 1;
        self.live.insert(
            id,
            Retry {
                attempt_no: 1,
                backoff: Duration::ZERO,
                queued_at: now,
            },
        );
        self.spawn_queue.push(SpawnedTask { id, seed });
        id
    }

    /// Stop/cancel a task. If it was still waiting out backoff it vanishes
    /// without touching the stop queue (nothing is running to cancel);
    /// otherwise it is dropped from the spawn queue if queued there and its
    /// id enters the stop queue so the driver cancels the running work.
    /// Mirrors `Tasks::stop_task` exactly.
    pub fn stop(&mut self, id: TaskId) -> Option<Retry> {
        // A stop always licenses one later respawn, whichever branch takes.
        self.stopped.insert(id);
        let old = self.live.remove(&id);
        if self.delayed.remove(&id).is_some() {
            return old;
        }
        self.spawn_queue.retain(|task| task.id != id);
        self.stop_queue.insert(id);
        old
    }

    /// Re-seed a failed task after a delay, computing the next backoff step.
    /// The formula is verbatim from `Tasks::spawn_delayed_task`: first retry
    /// uses `min_delay` (capped), later retries double the previous backoff
    /// (floored by `min_delay`, capped by `max_backoff`; a zero cap falls
    /// back to one minute).
    ///
    /// Contract, made unrepresentable-to-violate: `prev_id` must have been
    /// [`Self::stop`]ped first — the real machine always pairs them
    /// (`handle_evt(SyncFailed)` runs `stop_task(task_id)` before respawning
    /// with the saved retry). Respawning a task that was never stopped is an
    /// invariant violation and panics. Each stop licenses exactly one
    /// respawn; a second respawn from the same stop panics too.
    pub fn respawn_delayed(
        &mut self,
        prev_id: TaskId,
        seed: Seed,
        prev_retry: Retry,
        min_delay: Duration,
        now: Instant,
    ) -> TaskId {
        assert!(
            self.stopped.remove(&prev_id),
            "respawn_delayed for task {prev_id} without a prior stop"
        );
        let max_backoff = if self.max_backoff.is_zero() {
            Duration::from_secs(60)
        } else {
            self.max_backoff
        };
        let backoff = if prev_retry.backoff.is_zero() {
            min_delay.min(max_backoff)
        } else {
            prev_retry
                .backoff
                .saturating_mul(2)
                .max(min_delay)
                .min(max_backoff)
        };
        let retry = Retry {
            attempt_no: prev_retry.attempt_no + 1,
            queued_at: now,
            backoff,
        };
        let due_at = retry.queued_at + retry.backoff;

        let id = self.next_id;
        self.next_id += 1;
        self.live.insert(id, retry);
        self.delayed.insert(id, (seed, due_at));
        id
    }

    /// Move due delayed tasks onto the spawn queue. Mirrors
    /// `Tasks::enqueue_due_tasks`.
    pub fn tick(&mut self, now: Instant) {
        let due_ids: Vec<_> = self
            .delayed
            .iter()
            .filter_map(|(id, (_, due_at))| (*due_at <= now).then_some(*id))
            .collect();
        for id in due_ids {
            let Some((seed, _)) = self.delayed.remove(&id) else {
                continue;
            };
            self.spawn_queue.push(SpawnedTask { id, seed });
        }
    }

    /// Hand spawned tasks to the driver.
    pub fn drain_spawn_queue(&mut self) -> std::vec::Drain<'_, SpawnedTask<Seed>> {
        self.spawn_queue.drain(..)
    }

    /// Hand cancelled task ids to the driver so it can abort running futures.
    pub fn drain_stop_queue(&mut self) -> std::collections::hash_set::Drain<'_, TaskId> {
        self.stop_queue.drain()
    }

    /// The retry bookkeeping of a still-live task, for feeding back into
    /// `respawn_delayed` on failure.
    pub fn retry_of(&self, id: TaskId) -> Option<Retry> {
        self.live.get(&id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    enum Seed {
        Sync(u64),
        Diff,
    }

    fn t(secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(secs)
    }

    #[test]
    fn spawn_then_drain_then_stop_enters_stop_queue() {
        let now = t(0);
        let mut s = Scheduler::<Seed>::default();
        let id = s.spawn(now, Seed::Sync(1));
        assert_eq!(s.counts().spawn_queue, 1);
        s.stop(id);
        // stopped before the driver drained it: gone from spawn queue,
        // present in stop queue for cancellation bookkeeping.
        assert_eq!(s.counts().spawn_queue, 0);
        assert_eq!(s.counts().stop_queue, 1);
        let stopped: Vec<_> = s.drain_stop_queue().collect();
        assert_eq!(stopped, vec![id]);
    }

    #[test]
    fn failed_running_task_stops_then_respawns_with_backoff() {
        let now = t(0);
        let mut s = Scheduler::<Seed>::default();
        let id = s.spawn(now, Seed::Diff);
        s.drain_spawn_queue(); // driver running it
        let retry = s.retry_of(id).expect("live");

        // Driver reports failure: the machine stops the old task (the stop
        // queue tells the driver to forget it) and re-seeds after backoff —
        // the exact pairing in BigSyncMachine::handle_evt(SyncFailed).
        s.stop(id);
        assert_eq!(s.counts().stop_queue, 1);
        let id2 = s.respawn_delayed(id, Seed::Diff, retry, Duration::from_secs(5), now);
        assert_eq!(s.counts().delayed, 1);
        assert_eq!(s.retry_of(id2).unwrap().backoff, Duration::from_secs(5));

        // Stopping a task still waiting out backoff vanishes without touching
        // the stop queue: nothing is running to cancel (`Tasks::stop_task`).
        s.stop(id2);
        assert_eq!(s.counts().delayed, 0);
        assert_eq!(s.counts().stop_queue, 1); // unchanged — only the first id
    }

    #[test]
    fn backoff_doubles_with_floor_and_cap() {
        let t0 = t(0);
        let mut s = Scheduler::<Seed>::default();
        s.set_max_backoff(Duration::from_secs(30));
        let id = s.spawn(t0, Seed::Sync(1));
        s.drain_spawn_queue();

        // Every respawn is preceded by stop(), mirroring
        // handle_evt(SyncFailed): stop_task then respawn with saved retry.
        let retry1 = s.retry_of(id).unwrap();
        s.stop(id);
        let id2 = s.respawn_delayed(id, Seed::Sync(1), retry1, Duration::from_secs(3), t0);
        // first retry: min_delay capped by max_backoff
        assert_eq!(s.retry_of(id2).unwrap().backoff, Duration::from_secs(3));

        s.tick(t0 + Duration::from_secs(3));
        let drained: Vec<_> = s.drain_spawn_queue().collect();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].id, id2);

        // second retry doubles: 6s
        let retry2 = s.retry_of(id2).unwrap();
        s.stop(id2);
        let id3 = s.respawn_delayed(id2, Seed::Sync(1), retry2, Duration::from_secs(3), t0);
        assert_eq!(s.retry_of(id3).unwrap().backoff, Duration::from_secs(6));

        // ... doubling continues: 24s ...
        let retry3 = s.retry_of(id3).unwrap();
        s.stop(id3);
        let id4 = s.respawn_delayed(id3, Seed::Sync(1), retry3, Duration::from_secs(3), t0);
        let retry4 = s.retry_of(id4).unwrap();
        s.stop(id4);
        let id5 = s.respawn_delayed(id4, Seed::Sync(1), retry4, Duration::from_secs(3), t0);
        assert_eq!(s.retry_of(id5).unwrap().backoff, Duration::from_secs(24));

        // ... until it hits the cap: 30s
        let retry5 = s.retry_of(id5).unwrap();
        s.stop(id5);
        let id6 = s.respawn_delayed(id5, Seed::Sync(1), retry5, Duration::from_secs(3), t0);
        assert_eq!(s.retry_of(id6).unwrap().backoff, Duration::from_secs(30));

        // not due yet: nothing spawns
        s.tick(t0 + Duration::from_secs(29));
        assert_eq!(s.counts().spawn_queue, 0);
        s.tick(t0 + Duration::from_secs(30));
        assert_eq!(s.counts().spawn_queue, 1);
    }

    #[test]
    fn zero_max_backoff_falls_back_to_one_minute_cap() {
        let t0 = t(0);
        let mut s = Scheduler::<Seed>::default();
        s.set_max_backoff(Duration::ZERO);
        let id = s.spawn(t0, Seed::Diff);
        s.drain_spawn_queue();
        let retry = s.retry_of(id).unwrap();
        s.stop(id);
        let id2 = s.respawn_delayed(id, Seed::Diff, retry, Duration::from_secs(120), t0);
        assert_eq!(s.retry_of(id2).unwrap().backoff, Duration::from_secs(60));
    }

    #[test]
    #[should_panic(expected = "without a prior stop")]
    fn respawning_a_task_that_was_never_stopped_panics() {
        let t0 = t(0);
        let mut s = Scheduler::<Seed>::default();
        let id = s.spawn(t0, Seed::Diff);
        s.drain_spawn_queue();
        let retry = s.retry_of(id).expect("live");
        // No stop(): the predecessor is still live — resurrection must be
        // impossible.
        s.respawn_delayed(id, Seed::Diff, retry, Duration::from_secs(1), t0);
    }

    #[test]
    #[should_panic(expected = "without a prior stop")]
    fn one_stop_licenses_exactly_one_respawn() {
        let t0 = t(0);
        let mut s = Scheduler::<Seed>::default();
        let id = s.spawn(t0, Seed::Diff);
        s.drain_spawn_queue();
        let retry = s.retry_of(id).expect("live");
        s.stop(id);
        let _id2 = s.respawn_delayed(id, Seed::Diff, retry, Duration::from_secs(1), t0);
        // The single stop was already consumed by the first respawn.
        s.respawn_delayed(id, Seed::Diff, retry, Duration::from_secs(1), t0);
    }
}
