//! Tokio execution for keyed commands produced by the frontier worker.
//!
//! The worker owns logical keys, source dependencies, generations, and merging.
//! This type owns physical task handles, cancellation, delayed retries, and
//! externally parked work.

use crate::interlude::*;
use crate::scheduler::{KeyedScheduler, Retry, SpawnedTask, TaskId};
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::time::{Duration, Instant};

type TaskFuture<O> = Pin<Box<dyn Future<Output = Res<O>> + Send + 'static>>;

#[derive(Debug)]
pub struct TokioTaskCompletion<C, O> {
    pub task_id: TaskId,
    pub command: C,
    pub result: Res<O>,
    /// Retry bookkeeping captured before the scheduler retires this task.
    /// A caller may feed it to [`TokioKeyedScheduler::retry`].
    pub retry: Retry,
}

/// Physical execution of keyed commands with bounded concurrency.
///
/// `C` is treated as an already-merged command. This scheduler deliberately
/// does not merge it; that is the concurrent walker's responsibility because
/// only the walker knows which source revisions the command covers.
pub struct TokioKeyedScheduler<K, C, O>
where
    K: Eq + Hash + Copy + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    O: Send + 'static,
{
    scheduler: KeyedScheduler<K, C>,
    task_set: utils_rs::AbortableJoinSet,
    handles: HashMap<TaskId, utils_rs::TaskHandle>,
    /// Futures for logical tasks waiting in the scheduler's ready queue.
    ///
    /// A keyed task can be admitted logically while the physical task budget
    /// is full.  Keep its future here until `spawn_queued` has capacity rather
    /// than treating the queued task as running.
    ready_futures: HashMap<TaskId, TaskFuture<O>>,
    /// Futures for tasks waiting out a failure backoff.  These are moved into
    /// `ready_futures` implicitly when their scheduler entries become due.
    retry_futures: HashMap<TaskId, TaskFuture<O>>,
    completion_tx: tokio::sync::mpsc::Sender<TokioTaskCompletion<C, O>>,
    completion_rx: tokio::sync::mpsc::Receiver<TokioTaskCompletion<C, O>>,
    max_concurrent: usize,
}

impl<K, C, O> TokioKeyedScheduler<K, C, O>
where
    K: Eq + Hash + Copy + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    O: Send + 'static,
{
    pub fn new(max_concurrent: usize) -> Self {
        assert!(max_concurrent > 0, "task budget must be non-zero");
        let (completion_tx, completion_rx) = tokio::sync::mpsc::channel(max_concurrent);
        Self {
            scheduler: KeyedScheduler::default(),
            task_set: utils_rs::AbortableJoinSet::new(),
            handles: HashMap::new(),
            ready_futures: HashMap::new(),
            retry_futures: HashMap::new(),
            completion_tx,
            completion_rx,
            max_concurrent,
        }
    }

    pub fn active_count(&self) -> usize {
        self.handles.len()
    }

    pub fn has_capacity_for(&self, key: K) -> bool {
        self.scheduler.active_task(key).is_some() || self.active_count() < self.max_concurrent
    }

    /// Replace the logical task for `key` and return its task id.
    ///
    /// The caller supplies an already-merged command and its future.  The
    /// logical task is admitted to the ready queue even when the physical
    /// budget is full; `spawn_queued` starts it once capacity is available.
    pub fn replace<F>(&mut self, key: K, command: C, future: F) -> Res<TaskId>
    where
        F: Future<Output = Res<O>> + Send + 'static,
    {
        let old_task = self.scheduler.active_task(key);
        let task_id = self.scheduler.replace(Instant::now(), key, command);
        if let Some(old_task) = old_task {
            self.ready_futures.remove(&old_task);
            // A replaced delayed retry no longer owns its future.
            self.retry_futures.remove(&old_task);
        }
        self.abort_stopped();
        let old = self.ready_futures.insert(task_id, Box::pin(future));
        assert!(old.is_none(), "ready task id was reused");
        self.spawn_queued()?;
        Ok(task_id)
    }

    /// Cancel the current task for `key`, including a delayed retry.
    pub fn cancel(&mut self, key: K) {
        let old_task = self.scheduler.active_task(key);
        if self.scheduler.cancel(key).is_some() {
            if let Some(old_task) = old_task {
                self.ready_futures.remove(&old_task);
                self.retry_futures.remove(&old_task);
            }
            self.abort_stopped();
            // Cancellation may free the only physical slot.  Admit queued
            // work now instead of waiting for an unrelated completion or
            // timer event.
            self.spawn_queued()
                .expect("queued keyed task admission failed after cancellation");
        }
    }

    /// Park a completed task until an external signal makes its dependencies
    /// available again. Its command is retained by the scheduler.
    pub fn park(&mut self, key: K, command: C) {
        self.scheduler.park(key, command);
    }

    /// Wake parked work and put it in the ready queue.
    ///
    /// Waking is a logical state transition, not permission to bypass the
    /// physical execution budget.  The retained future stays queued until
    /// `spawn_queued` observes capacity, so an external notification cannot
    /// be lost merely because another batch is currently running.
    pub fn wake<F>(&mut self, key: K, future: F) -> Res<bool>
    where
        F: Future<Output = Res<O>> + Send + 'static,
    {
        if !self.scheduler.wake(Instant::now(), key) {
            return Ok(false);
        }
        let task_id = self
            .scheduler
            .active_task(key)
            .expect("woken key must have an active task");
        let old = self.ready_futures.insert(task_id, Box::pin(future));
        assert!(old.is_none(), "ready task id was reused");
        self.spawn_queued()?;
        Ok(true)
    }

    /// Schedule a failed completion for a backoff retry. The future is held
    /// without polling until [`Self::tick`] observes its due time.
    pub fn retry<F>(
        &mut self,
        key: K,
        command: C,
        retry: Retry,
        min_delay: Duration,
        future: F,
    ) -> Res<TaskId>
    where
        F: Future<Output = Res<O>> + Send + 'static,
    {
        let task_id = self.scheduler.retry_delayed(
            Instant::now(),
            key,
            command,
            retry,
            min_delay,
        );
        let old = self.retry_futures.insert(task_id, Box::pin(future));
        assert!(old.is_none(), "retry task id was reused");
        self.abort_stopped();
        Ok(task_id)
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        // A ready queue is only actionable while there is physical capacity.
        // Returning `now` while the budget is full would make every machine's
        // timer arm spin without making progress.
        if self.active_count() < self.max_concurrent {
            if self.scheduler.spawn_queue_len() > 0 {
                return Some(Instant::now());
            }
            self.scheduler.next_due()
        } else {
            None
        }
    }

    /// Advance delayed retries and spawn every task that became due or can now
    /// fit in the physical budget.
    pub fn tick(&mut self, now: Instant) -> Res<()> {
        self.scheduler.tick(now);
        self.spawn_queued()
    }

    pub async fn next_completion(&mut self) -> Res<TokioTaskCompletion<C, O>> {
        loop {
            let completion = self
                .completion_rx
                .recv()
                .await
                .ok_or_else(|| ferr!("keyed task completion channel closed"))?;
            let Some(_handle) = self.handles.remove(&completion.task_id) else {
                // A cancelled/replaced task raced with the completion channel.
                continue;
            };
            let Some(retry) = self.scheduler.retry_of(completion.task_id) else {
                // A stale completion can have lost its logical task already.
                continue;
            };
            if self.scheduler.complete(completion.task_id) {
                // `complete` enqueues a stop entry for the finished task; drain
                // it here so completion-only paths do not leak the stop queue.
                self.abort_stopped();
                // Completion is also the admission point for work that was
                // ready but waiting for the physical budget.
                self.spawn_queued()?;
                return Ok(TokioTaskCompletion { retry, ..completion });
            }
        }
    }

    fn spawn_queued(&mut self) -> Res<()> {
        let queued: Vec<_> = self.scheduler.drain_spawn_queue().collect();
        let mut remaining = Vec::new();
        for task in queued {
            if self.handles.len() >= self.max_concurrent {
                remaining.push(task);
                continue;
            }
            let future = self
                .ready_futures
                .remove(&task.id)
                .or_else(|| self.retry_futures.remove(&task.id))
                .expect("every queued task must have a future");
            self.spawn(task, future)?;
        }
        self.scheduler.requeue_spawned(remaining);
        Ok(())
    }

    fn spawn(&mut self, task: SpawnedTask<C>, future: TaskFuture<O>) -> Res<()> {
        let task_id = task.id;
        let completion_tx = self.completion_tx.clone();
        let command = task.seed;
        let handle = self.task_set.spawn(async move {
            let result = future.await;
            // Closing the receiver means the owning worker is shutting down;
            // the task has no consumer left to report to.
            let _ = completion_tx
                .send(TokioTaskCompletion {
                    task_id,
                    command,
                    result,
                    // Filled by `next_completion` from scheduler state.
                    retry: Retry {
                        attempt_no: 0,
                        backoff: Duration::ZERO,
                        queued_at: Instant::now(),
                    },
                })
                .await;
        })?;
        self.handles.insert(task_id, handle);
        Ok(())
    }

    fn abort_stopped(&mut self) {
        for task_id in self.scheduler.drain_stop_queue() {
            if let Some(handle) = self.handles.remove(&task_id) {
                handle.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn waking_parked_work_waits_for_physical_capacity() {
        let mut tasks = TokioKeyedScheduler::<u64, u64, u32>::new(1);
        tasks.replace(1, 1, std::future::pending::<Res<u32>>()).unwrap();
        tasks.park(2, 2);

        assert!(tasks.wake(2, async { Ok(2u32) }).unwrap());
        assert_eq!(tasks.active_count(), 1);
        assert_eq!(tasks.next_deadline(), None);

        tasks.cancel(1);
        tokio::task::yield_now().await;
        let completion = tasks.next_completion().await.unwrap();
        assert_eq!(completion.command, 2);
        assert_eq!(completion.result.unwrap(), 2);
    }
}
