//! One shutdown idiom for every long-lived component.
//!
//! A component boots into `(Arc<Self>, StopToken)`. Its background tasks are
//! spawned through the token's tracker and watch its cancellation token.
//! [`StopToken::stop`] cancels, closes the tracker, and *joins*: it never
//! aborts, so a task that ignores cancellation is a liveness bug that shows up
//! as a hung stop rather than as silently lost work. Dropping a token does
//! nothing.
//!
//! Tokens compose in construction order reversed: a service that owns a
//! projector and is itself fronted by an IRPC adapter stops the adapter first,
//! then the service, so no task is ever cancelled while something that depends
//! on it is still running.

use futures::FutureExt;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};
use tracing::instrument::WithSubscriber;

#[derive(Debug, Clone)]
struct Stage {
    cancel: CancellationToken,
    tracker: TaskTracker,
    closed: Arc<Mutex<bool>>,
    failed: Arc<AtomicBool>,
}

/// Cancels and joins the background work of one or more components, in order.
#[derive(Debug, Default)]
pub struct StopToken {
    stages: Vec<Stage>,
}

impl StopToken {
    /// A token for one component. The returned [`Spawner`] is what the
    /// component keeps to start tasks and observe its own cancellation.
    pub fn new() -> (Self, Spawner) {
        let stage = Stage {
            cancel: CancellationToken::new(),
            tracker: TaskTracker::new(),
            closed: Arc::new(Mutex::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
        };
        let spawner = Spawner {
            cancel: stage.cancel.clone(),
            tracker: stage.tracker.clone(),
            closed: Arc::clone(&stage.closed),
            failed: Arc::clone(&stage.failed),
        };
        (
            Self {
                stages: vec![stage],
            },
            spawner,
        )
    }

    /// Stops `self` first, then `inner`. Use it to express "the adapter stops
    /// before the service it fronts".
    pub fn then(mut self, inner: StopToken) -> Self {
        self.stages.extend(inner.stages);
        self
    }

    /// Cancel, close, and join every stage in order.
    pub async fn stop(self) {
        for stage in self.stages {
            *stage.closed.lock().unwrap() = true;
            stage.cancel.cancel();
            stage.tracker.close();
            stage.tracker.wait().await;
            assert!(
                !stage.failed.load(Ordering::Acquire),
                "component task panicked"
            );
        }
    }
}

/// The component-side half of a [`StopToken`].
#[derive(Debug, Clone)]
pub struct Spawner {
    cancel: CancellationToken,
    tracker: TaskTracker,
    closed: Arc<Mutex<bool>>,
    failed: Arc<AtomicBool>,
}

impl Spawner {
    pub fn spawn<F>(&self, future: F) -> Option<tokio::task::JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let token = self.track()?;
        let failed = Arc::clone(&self.failed);
        Some(tokio::spawn(
            async move {
                let _token = token;
                match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(output) => output,
                    Err(panic) => {
                        failed.store(true, Ordering::Release);
                        std::panic::resume_unwind(panic)
                    }
                }
            }
            .with_current_subscriber(),
        ))
    }

    pub(crate) fn track(&self) -> Option<TaskTrackerToken> {
        let closed = self.closed.lock().unwrap();
        if *closed {
            None
        } else {
            Some(self.tracker.token())
        }
    }

    /// Resolves once [`StopToken::stop`] has been called for this component.
    pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Number of tracked tasks that have not finished.
    pub fn live_tasks(&self) -> usize {
        self.tracker.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stop_cancels_then_joins_in_declared_order() {
        let (inner_stop, inner) = StopToken::new();
        let (outer_stop, outer) = StopToken::new();
        let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let inner_order = std::sync::Arc::clone(&order);
        let inner_spawner = inner.clone();
        let _inner_task = inner.spawn(async move {
            inner_spawner.cancelled().await;
            inner_order.lock().unwrap().push("inner");
        });
        let outer_order = std::sync::Arc::clone(&order);
        let outer_spawner = outer.clone();
        let _outer_task = outer.spawn(async move {
            outer_spawner.cancelled().await;
            outer_order.lock().unwrap().push("outer");
        });

        outer_stop.then(inner_stop).stop().await;
        assert_eq!(*order.lock().unwrap(), vec!["outer", "inner"]);
        assert_eq!(inner.live_tasks(), 0);
        assert_eq!(outer.live_tasks(), 0);
    }

    #[tokio::test]
    async fn stop_joins_inline_work_and_closes_admission() {
        let (stop, spawner) = StopToken::new();
        let inline = spawner.track().unwrap();
        let mut stopping = Box::pin(stop.stop());
        assert!(futures::poll!(&mut stopping).is_pending());
        assert!(spawner.is_cancelled());
        assert!(spawner.track().is_none());
        assert!(spawner.spawn(async { panic!("late work ran") }).is_none());
        drop(inline);
        stopping.await;
        assert_eq!(spawner.live_tasks(), 0);
    }

    #[tokio::test]
    async fn stop_reports_background_panics() {
        let (stop, spawner) = StopToken::new();
        let task = spawner
            .spawn(async { panic!("background failure") })
            .unwrap();
        assert!(task.await.unwrap_err().is_panic());
        assert!(AssertUnwindSafe(stop.stop()).catch_unwind().await.is_err());
    }
}
