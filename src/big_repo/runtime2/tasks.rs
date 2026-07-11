//! Runtime-owned background tasks.
//!
//! Runtime2 does not implement an executor or task scheduler. It depends on
//! [`TaskRuntime`] to create independently-stoppable [`TaskSet`]s, then lets
//! the platform implementation delegate to its native task machinery.
//!
//! The native implementation in this module is deliberately small: it wraps
//! [`utils_rs::AbortableJoinSet`]. A wasm implementation can use
//! `wasm_bindgen_futures::spawn_local` behind the same interface without
//! imposing `Send` or Tokio types on the actors.

use std::{sync::Arc, time::Duration};

use future_form::{FutureForm, Sendable};
use futures::{FutureExt, future::Abortable, stream::AbortHandle};

/// Platform capability for creating independent task ownership scopes.
///
/// A scope is a unit of structured concurrency. Runtime2 creates separate
/// scopes for the hub loop and its children so shutdown can stop children
/// before awaiting the hub.
pub trait TaskRuntime<F: FutureForm>: Clone + 'static {
    type Tasks: TaskSet<F>;

    /// Create an empty, independently-stoppable task set.
    fn task_set(&self) -> Self::Tasks;
}

/// A set of owned background tasks.
///
/// Implementations are responsible for driving spawned futures, retaining the
/// runtime's join handles, and awaiting task termination in [`stop`](Self::stop).
/// Runtime2 only retains the returned [`AbortHandle`] when an individual task
/// (such as a doc-worker) must be cancelled before its enclosing set stops.
pub trait TaskSet<F: FutureForm>: Clone + 'static {
    /// Spawn a task owned by this set.
    ///
    /// Unexpected errors are programming failures: implementations must make
    /// them observable from [`stop`](Self::stop), rather than log and detach.
    fn spawn(
        &self,
        task: F::Future<'static, eyre::Result<()>>,
    ) -> eyre::Result<AbortHandle>;

    /// Abort every task in the set without waiting for termination.
    fn abort(&self);

    /// Stop accepting work and await every task already owned by this set.
    ///
    /// Implementations may abort remaining tasks when `timeout` elapses, but
    /// must return the timeout/failure rather than silently succeeding.
    fn stop(&self, timeout: Duration) -> F::Future<'_, eyre::Result<()>>;
}

/// Native Tokio task runtime.
///
/// Tokio is confined to this backend through [`utils_rs::AbortableJoinSet`];
/// runtime2 actors depend only on [`TaskRuntime`] / [`TaskSet`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioTaskRuntime;

/// Native task ownership scope backed by [`utils_rs::AbortableJoinSet`].
#[derive(Debug, Clone)]
pub struct TokioTaskSet {
    inner: Arc<utils_rs::AbortableJoinSet>,
}

impl TaskRuntime<Sendable> for TokioTaskRuntime {
    type Tasks = TokioTaskSet;

    fn task_set(&self) -> Self::Tasks {
        TokioTaskSet {
            inner: Arc::new(utils_rs::AbortableJoinSet::new()),
        }
    }
}

impl TaskSet<Sendable> for TokioTaskSet {
    fn spawn(
        &self,
        task: <Sendable as FutureForm>::Future<'static, eyre::Result<()>>,
    ) -> eyre::Result<AbortHandle> {
        let (abort, registration) = AbortHandle::new_pair();
        self.inner
            .spawn(async move {
                // Cancellation of one task is expected during doc eviction.
                // An unexpected task error panics so AbortableJoinSet observes
                // a JoinError and propagates it from stop().
                if let Ok(result) = Abortable::new(task, registration).await {
                    result.unwrap();
                }
            })
            .map_err(|error| eyre::eyre!("task set is not accepting work: {error}"))?;
        Ok(abort)
    }

    fn abort(&self) {
        self.inner.abort();
    }

    fn stop(&self, timeout: Duration) -> <Sendable as FutureForm>::Future<'_, eyre::Result<()>> {
        async move {
            self.inner
                .stop(timeout)
                .await
                .map_err(|error| eyre::eyre!("failed stopping runtime task set: {error}"))
        }
        .boxed()
    }
}
