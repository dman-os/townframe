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

use crate::interlude::*;
use std::{sync::Arc, time::Duration};

use future_form::{FutureForm, Sendable};
use futures::{
    FutureExt,
    future::{AbortHandle, Abortable},
};

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
    fn spawn(&self, task: F::Future<'static, eyre::Result<()>>) -> eyre::Result<AbortHandle>;

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

/// Native timer backend. Tokio remains confined to platform adapters; actor
/// code depends only on [`Timer`](crate::runtime2::Timer).
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioTimer;

impl crate::runtime2::Timer<Sendable> for TokioTimer {
    fn sleep(&self, duration: Duration) -> <Sendable as FutureForm>::Future<'static, ()> {
        tokio::time::sleep(duration).boxed()
    }
}

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
            match self.inner.stop(timeout).await {
                Ok(()) => Ok(()),
                Err(utils_rs::AbortableJoinSetStopError::JoinError(error))
                    if error.is_cancelled() =>
                {
                    Ok(())
                }
                Err(error) => Err(eyre::eyre!("failed stopping runtime task set: {error}")),
            }
        }
        .boxed()
    }
}

/// Deterministic time for runtime2 machine tests.
///
/// One object serves as both [`Clock`](crate::runtime2::Clock) and
/// [`Timer`](crate::runtime2::Timer) because the two must agree: every machine
/// loop reads `instant()` to work out how long to wait and then sleeps that
/// duration, so a clock that moved without waking sleepers (or a timer that
/// woke them without moving the clock) would let a loop spin or hang. That is
/// why [`advance`](ManualTime::advance) moves the clock and releases exactly
/// the sleepers whose deadline it passed, and why production passes one timer
/// and one clock out of `native.rs` to every actor on the node.
///
/// With this, "advance 250 ms, then tick" is an assertion instead of a
/// wall-clock wait: a test parks a machine, waits for the sleep to arm, moves
/// time, and asserts the state transition the loop was waiting for.
#[cfg(test)]
pub(crate) mod manual_time {
    use super::Sendable;
    use futures::FutureExt;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::sync::{broadcast, watch};

    /// A clock and timer whose time only moves when a test moves it.
    #[derive(Debug)]
    pub(crate) struct ManualTime {
        base: Instant,
        /// Advancements since `base`. Shared rather than copied so `instant()`
        /// and every armed sleeper read the same value.
        elapsed: watch::Sender<Duration>,
        /// One event per [`Timer::sleep`](crate::runtime2::Timer::sleep) call.
        /// A test awaits these to learn that a machine reached its timer arm,
        /// which is otherwise unobservable from outside the loop.
        arms: broadcast::Sender<()>,
    }

    impl ManualTime {
        pub(crate) fn new() -> Arc<Self> {
            let (elapsed, _) = watch::channel(Duration::ZERO);
            let (arms, _) = broadcast::channel(1024);
            Arc::new(Self {
                base: Instant::now(),
                elapsed,
                arms,
            })
        }

        /// Move time forward, releasing every sleeper whose deadline it passes.
        /// Sleepers with a later deadline stay parked; they re-read the value on
        /// each change, so a partial advance cannot release them early.
        pub(crate) fn advance(&self, duration: Duration) {
            let now = *self.elapsed.borrow() + duration;
            // `send_replace` notifies receivers without requiring any to exist, so
            // `new()` does not have to keep a receiver alive for time to move.
            let _previous: Duration = self.elapsed.send_replace(now);
        }

        /// Resolve once `count` sleeps have been armed. Machine loops poll a
        /// sleep they armed on an earlier iteration, so a test that wants to
        /// observe the *first* wait awaits one arming.
        pub(crate) async fn wait_until_armed(&self, count: usize) {
            let mut arms = self.arms.subscribe();
            let mut seen = 0usize;
            while seen < count {
                match arms.recv().await {
                    Ok(()) => seen += 1,
                    // A lagging subscriber missed at least as many armings as the
                    // channel holds, which already satisfies the count.
                    Err(broadcast::error::RecvError::Lagged(_)) => seen = count,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }

    impl crate::runtime2::Clock for ManualTime {
        fn instant(&self) -> Instant {
            self.base + *self.elapsed.borrow()
        }
    }

    impl crate::runtime2::Timer<Sendable> for ManualTime {
        fn sleep(
            &self,
            duration: Duration,
        ) -> <Sendable as super::FutureForm>::Future<'static, ()> {
            // Reading the deadline at call time keeps the wait identical to
            // `sleep_until(instant + duration)` on a real clock: the machine
            // loops pass `deadline.saturating_duration_since(now)`, so a deadline
            // the clock has already passed arrives here as `Duration::ZERO` and
            // must resolve without an advance (the wall-clock version returned
            // immediately too).
            let deadline = *self.elapsed.borrow() + duration;
            // An arming is a notification, not a message: no receiver has to exist for
            // time to be observable, so a failed send is not an error (see
            // `wait_until_armed`, which treats a lagging subscriber as satisfied).
            let _receivers: Result<usize, broadcast::error::SendError<()>> = self.arms.send(());
            let mut elapsed = self.elapsed.subscribe();
            async move {
                loop {
                    if *elapsed.borrow_and_update() >= deadline {
                        return;
                    }
                    if elapsed.changed().await.is_err() {
                        return;
                    }
                }
            }
            .boxed()
        }
    }

    mod tests {
        use super::ManualTime;
        use crate::runtime2::{Clock, Timer};
        use future_form::Sendable;
        use std::time::Duration;

        #[test]
        fn advancing_the_manual_clock_moves_its_instant() {
            let manual = ManualTime::new();
            let start = Clock::instant(&*manual);
            manual.advance(Duration::from_millis(750));
            assert_eq!(
                Clock::instant(&*manual).duration_since(start),
                Duration::from_millis(750),
                "the clock and the sleepers must share one time source"
            );
        }

        #[tokio::test]
        async fn a_sleep_waits_for_its_deadline_and_not_for_wall_clock() {
            let manual = ManualTime::new();
            let sleeper = {
                let manual = std::sync::Arc::clone(&manual);
                tokio::spawn(async move {
                    Timer::<Sendable>::sleep(&*manual, Duration::from_millis(500)).await;
                })
            };
            manual.wait_until_armed(1).await;
            assert!(
                !sleeper.is_finished(),
                "a 500 ms sleep must not resolve before time moves"
            );

            // A partial advance must not release it: the sleeper is woken by any
            // change and re-reads the value, so only a passing deadline frees it.
            manual.advance(Duration::from_millis(499));
            tokio::task::yield_now().await;
            assert!(
                !sleeper.is_finished(),
                "a 499 ms advance must not release a 500 ms sleep"
            );

            manual.advance(Duration::from_millis(1));
            sleeper.await.expect("the sleeper resolves at its deadline");
        }

        /// The equivalence case the machine loops depend on: they pass
        /// `deadline.saturating_duration_since(now)`, which is zero when the
        /// scheduler deadline has already passed, and a zero wait must complete
        /// without an advance rather than defer a tick.
        #[tokio::test]
        async fn a_zero_duration_sleep_resolves_without_advancing_time() {
            let manual = ManualTime::new();
            Timer::<Sendable>::sleep(&*manual, Duration::ZERO).await;
            assert_eq!(
                Clock::instant(&*manual),
                Clock::instant(&*manual),
                "a zero wait must not move the clock"
            );
        }
    }
}
