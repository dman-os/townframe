//! Waiting on spawned tasks, and sets of them that can be aborted together.

use crate::interlude::*;

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum WaitOnHandleError {
    /// task error: {0:?}
    JoinError(#[from] tokio::task::JoinError),
    /// task was aborted due to timeout
    Timeout(#[from] tokio::time::error::Elapsed),
}

pub async fn wait_on_handle_with_timeout<T>(
    mut join_handle: tokio::task::JoinHandle<T>,
    timeout: Duration,
) -> Result<T, WaitOnHandleError> {
    match tokio::time::timeout(timeout, &mut join_handle).await {
        Ok(res) => Ok(res?),
        Err(err) => {
            join_handle.abort();
            Err(err.into())
        }
    }
}

pub async fn wait_on_handle<T>(
    join_handle: tokio::task::JoinHandle<T>,
) -> Result<T, tokio::task::JoinError> {
    join_handle.await
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum AbortableTaskError {
    /// task died before finish
    FoundDead,
    /// task set aborted
    Aborted,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum AbortableJoinSetError {
    /// task set aborted
    Aborted,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum AbortableJoinSetStopError {
    /// task set aborted
    Aborted,
    /// task error: {0:?}
    JoinError(#[from] tokio::task::JoinError),
    /// task set was aborted due to timeout
    Timeout(#[from] tokio::time::error::Elapsed),
}

#[derive(Debug)]
pub struct TaskHandle {
    abort: tokio::task::AbortHandle,
    done_rx: tokio::sync::oneshot::Receiver<()>,
}

impl TaskHandle {
    pub fn abort(&self) {
        self.abort.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }

    pub async fn join(self, timeout: Duration) -> Result<(), AbortableTaskError> {
        match tokio::time::timeout(timeout, self.done_rx).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => Err(AbortableTaskError::FoundDead),
            Err(_) => {
                self.abort.abort();
                Err(AbortableTaskError::Aborted)
            }
        }
    }
}

#[derive(Debug)]
pub struct AbortableJoinSet {
    inner: std::sync::Mutex<Option<tokio::task::JoinSet<()>>>,
}

impl Default for AbortableJoinSet {
    fn default() -> Self {
        Self::new()
    }
}

impl AbortableJoinSet {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(Some(tokio::task::JoinSet::new())),
        }
    }

    pub fn len(&self) -> usize {
        let guard = self.inner.lock().expect(ERROR_MUTEX);
        guard.as_ref().map_or(0, |set| set.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[track_caller]
    pub fn spawn<F>(&self, fut: F) -> Result<TaskHandle, AbortableJoinSetError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let caller = std::panic::Location::caller();
        let caller_file = caller.file();
        let caller_line = caller.line();
        let span = ::tracing::Span::current();
        let mut guard = self.inner.lock().expect(ERROR_MUTEX);
        let Some(join_set) = guard.as_mut() else {
            return Err(AbortableJoinSetError::Aborted);
        };
        // A long-lived runtime may spawn many short jobs before shutdown.
        // Reap completed tasks opportunistically so JoinSet does not retain
        // every result until `stop()`. A failed task is an invariant break and
        // must surface immediately rather than remain hidden until shutdown.
        while let Some(result) = join_set.try_join_next() {
            if let Err(err) = result
                && !err.is_cancelled()
            {
                std::panic::resume_unwind(err.into_panic());
            }
        }
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let abort = join_set.spawn(tracing_futures::Instrument::instrument(
            async move {
                fut.await;
                done_tx.send(()).ok();
            },
            ::tracing::debug_span!(parent: &span, "task", file = caller_file, line = caller_line),
        ));
        Ok(TaskHandle { abort, done_rx })
    }

    /// Abort every task currently owned by the set.
    ///
    /// The set remains available to [`stop`](Self::stop), which can then join
    /// the aborted tasks and surface any unexpected join failures. Keeping
    /// abort and join as separate operations lets owners enforce reverse-order
    /// shutdown without abandoning task cleanup.
    pub fn abort(&self) {
        let mut guard = self.inner.lock().expect(ERROR_MUTEX);
        let Some(join_set) = guard.as_mut() else {
            return;
        };
        join_set.abort_all();
    }

    pub async fn stop(&self, timeout: Duration) -> Result<(), AbortableJoinSetStopError> {
        let Some(mut join_set) = self.inner.lock().expect(ERROR_MUTEX).take() else {
            return Err(AbortableJoinSetStopError::Aborted);
        };
        match tokio::time::timeout(timeout, async {
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(_) => {}
                    Err(err) if err.is_cancelled() => {}
                    Err(err) => return Err(err),
                }
            }
            Ok::<(), tokio::task::JoinError>(())
        })
        .await
        {
            Ok(out) => out.map_err(Into::into),
            Err(err) => {
                join_set.abort_all();
                Err(err.into())
            }
        }
    }
}
