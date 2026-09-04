//! Tokio execution for keyed commands produced by the frontier worker.
//!
//! The worker owns logical keys, source dependencies, generations, and merging.
//! This type only owns physical task handles, cancellation, and the execution
//! budget. Replacing a key cancels the old physical task before the replacement
//! is spawned.

use crate::interlude::*;
use crate::scheduler::{KeyedScheduler, SpawnedTask, TaskId};
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::time::Instant;

#[derive(Debug)]
pub struct TokioTaskCompletion<C, O> {
    pub task_id: TaskId,
    pub command: C,
    pub result: Res<O>,
}

/// Physical execution of keyed commands with bounded concurrency.
///
/// `C` is treated as an already-merged command.  This scheduler deliberately
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

    /// Replace the physical task for `key` and return its task id.
    ///
    /// The caller supplies an already-merged command and its future.  The
    /// replacement path drains cancellation before spawning the new task.
    pub fn replace<F>(&mut self, key: K, command: C, future: F) -> Res<TaskId>
    where
        F: Future<Output = Res<O>> + Send + 'static,
    {
        if !self.has_capacity_for(key) {
            return Err(ferr!("keyed task budget exhausted"));
        }
        let task_id = self.scheduler.replace(Instant::now(), key, command);
        self.abort_stopped();
        let spawned = self
            .scheduler
            .drain_spawn_queue()
            .next()
            .expect("keyed scheduler replacement must enqueue one task");
        assert_eq!(spawned.id, task_id);
        if let Err(error) = self.spawn(spawned, future) {
            // Spawn failure must not leave a phantom active key with no
            // physical task behind it.
            self.scheduler.cancel(key);
            self.abort_stopped();
            return Err(error);
        }
        Ok(task_id)
    }
    pub fn cancel(&mut self, key: K) {
        if self.scheduler.cancel(key).is_some() {
            self.abort_stopped();
        }
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
            if self.scheduler.complete(completion.task_id) {
                // `complete` enqueues a stop entry for the finished task;
                // drain it here so completion-only paths don't leak the
                // stop queue on quiet workers.
                self.abort_stopped();
                return Ok(completion);
            }
        }
    }
    fn spawn<F>(&mut self, task: SpawnedTask<C>, future: F) -> Res<()>
    where
        F: Future<Output = Res<O>> + Send + 'static,
    {
        let task_id = task.id;
        let completion_tx = self.completion_tx.clone();
        let command = task.seed;
        let handle = self.task_set.spawn(async move {
            let result = future.await;
            // Closing the receiver means the owning worker is shutting down;
            // the task has no consumer left to report to. This is the
            // cancellation path, not a lost completion during normal running.
            if completion_tx
                .send(TokioTaskCompletion {
                    task_id,
                    command,
                    result,
                })
                .await
                .is_err()
            {
                // Receiver closed: the owning worker is shutting down.
            }
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
