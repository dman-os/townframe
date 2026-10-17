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

use std::future::Future;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Debug, Clone)]
struct Stage {
    cancel: CancellationToken,
    tracker: TaskTracker,
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
        };
        let spawner = Spawner {
            cancel: stage.cancel.clone(),
            tracker: stage.tracker.clone(),
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
            stage.cancel.cancel();
            stage.tracker.close();
            stage.tracker.wait().await;
        }
    }
}

/// The component-side half of a [`StopToken`].
#[derive(Debug, Clone)]
pub struct Spawner {
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl Spawner {
    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tracker.spawn(future)
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
        inner.spawn(async move {
            inner_spawner.cancelled().await;
            inner_order.lock().unwrap().push("inner");
        });
        let outer_order = std::sync::Arc::clone(&order);
        let outer_spawner = outer.clone();
        outer.spawn(async move {
            outer_spawner.cancelled().await;
            outer_order.lock().unwrap().push("outer");
        });

        outer_stop.then(inner_stop).stop().await;
        assert_eq!(*order.lock().unwrap(), vec!["outer", "inner"]);
        assert_eq!(inner.live_tasks(), 0);
        assert_eq!(outer.live_tasks(), 0);
    }
}
