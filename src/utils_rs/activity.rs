//! A node's activity registry (ADR 052, node activity and idle exit). It lives here, below
//! every crate that takes holds (`big_repo`, `big_sync`, `big_blobs`, `big_nodule`, the
//! dispatcher), and `big_node` creates the one instance at boot and hands it to each service.
//!
//! A subsystem that starts work the node should finish before it stops by itself takes a
//! [`Hold`] and drops it when the work is done. A hold is a guard, so an early return, a `?` or a
//! panic cannot leak one. Standing loops (pollers, janitors, listeners) never take one for being
//! alive, only for each unit of finite work they start; work that cannot progress (changes for an
//! unreachable peer) holds nothing, or a node offline would never become idle.
//!
//! The node is idle when it has had no hold for a whole idle period, continuously: any hold taken
//! restarts the clock. A server that exits when idle awaits [`Activity::idle_for`], and `day node
//! status` shows [`Activity::snapshot`]. The registry is plain counters and labels, with no engine
//! types, so every host can use it.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// What a hold is for (ADR 052 §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HoldKind {
    /// A connection is open.
    Client,
    /// A dispatch call is in flight.
    Call,
    /// Local changes are unacknowledged by a reachable peer or relay, or a pull is in progress.
    Sync,
    /// A blob pin or transfer is in flight.
    Blob,
    /// A checkpoint, compaction or frontier write is running.
    Upkeep,
    /// A cartridge handler or routine, or a micro's call, is running.
    Guest,
}

/// The node's activity: the holds currently taken, and when the last one was released.
#[derive(Clone)]
pub struct Activity {
    state: Arc<Mutex<State>>,
    /// Bumped on every hold and release, so a waiter learns that the holds changed.
    changed: Arc<watch::Sender<u64>>,
}

struct State {
    holds: BTreeMap<u64, Taken>,
    next: u64,
    /// When the node last had no holds: since boot, or since the last hold was released.
    idle_since: Instant,
}

struct Taken {
    kind: HoldKind,
    label: Cow<'static, str>,
    since: Instant,
}

/// Work in progress: the node is not idle while one is alive. Dropping it releases the hold.
#[must_use = "a hold is released when dropped"]
pub struct Hold {
    activity: Activity,
    id: u64,
}

/// One hold, as [`Activity::snapshot`] shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoldView {
    pub kind: HoldKind,
    pub label: String,
    /// How long it has been held.
    pub held_for: Duration,
}

/// The registry at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The current holds, oldest first.
    pub holds: Vec<HoldView>,
    /// How long the node has had no hold; `None` while one is held.
    pub idle_for: Option<Duration>,
}

impl Default for Activity {
    fn default() -> Self {
        Self::new()
    }
}

impl Activity {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                holds: BTreeMap::new(),
                next: 0,
                idle_since: Instant::now(),
            })),
            changed: Arc::new(watch::Sender::new(0)),
        }
    }

    /// Takes a hold of `kind`. A label known at compile time costs no allocation.
    pub fn hold(&self, kind: HoldKind, label: impl Into<Cow<'static, str>>) -> Hold {
        let id = {
            let mut state = self.state.lock().unwrap();
            let id = state.next;
            state.next += 1;
            state.holds.insert(
                id,
                Taken {
                    kind,
                    label: label.into(),
                    since: Instant::now(),
                },
            );
            id
        };
        self.changed.send_modify(|generation| *generation += 1);
        Hold {
            activity: self.clone(),
            id,
        }
    }

    /// Resolves once the node has had no hold for `period`, continuously. A hold taken while it
    /// waits restarts the period from that hold's release.
    pub async fn idle_for(&self, period: Duration) {
        // Subscribed before the state is read, so a change after the read is seen as changed.
        let mut changes = self.changed.subscribe();
        loop {
            let idle_since = {
                let state = self.state.lock().unwrap();
                state.holds.is_empty().then_some(state.idle_since)
            };
            match idle_since {
                None => changes
                    .changed()
                    .await
                    .expect("the registry owns its sender for as long as it is waited on"),
                Some(since) => {
                    tokio::select! {
                        () = tokio::time::sleep_until(since + period) => return,
                        changed = changes.changed() => changed
                            .expect("the registry owns its sender for as long as it is waited on"),
                    }
                }
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let now = Instant::now();
        let state = self.state.lock().unwrap();
        let mut holds = state
            .holds
            .values()
            .map(|taken| HoldView {
                kind: taken.kind,
                label: taken.label.clone().into_owned(),
                held_for: now - taken.since,
            })
            .collect::<Vec<_>>();
        holds.sort_by_key(|hold| std::cmp::Reverse(hold.held_for));
        Snapshot {
            idle_for: holds.is_empty().then(|| now - state.idle_since),
            holds,
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        {
            let mut state = self.activity.state.lock().unwrap();
            state
                .holds
                .remove(&self.id)
                .expect("a hold is released once, by its own drop");
            if state.holds.is_empty() {
                state.idle_since = Instant::now();
            }
        }
        self.activity
            .changed
            .send_modify(|generation| *generation += 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: Duration = Duration::from_secs(600);

    /// Whether `idle_for(PERIOD)` has resolved after the clock moves by `by`.
    async fn resolved_after(waiter: &mut tokio::task::JoinHandle<()>, by: Duration) -> bool {
        tokio::time::advance(by).await;
        tokio::task::yield_now().await;
        waiter.is_finished()
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_node_with_no_holds_is_idle_after_the_period() {
        let activity = Activity::new();
        let waiting = activity.clone();
        let mut waiter = tokio::spawn(async move { waiting.idle_for(PERIOD).await });
        assert!(!resolved_after(&mut waiter, PERIOD - Duration::from_secs(1)).await);
        assert!(resolved_after(&mut waiter, Duration::from_secs(1)).await);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_hold_keeps_the_node_busy_and_its_release_restarts_the_period() {
        let activity = Activity::new();
        let waiting = activity.clone();
        let mut waiter = tokio::spawn(async move { waiting.idle_for(PERIOD).await });
        tokio::time::advance(PERIOD / 2).await;
        let hold = activity.hold(HoldKind::Sync, "changes for relay r");
        // Held across what would have been the end of the period.
        assert!(!resolved_after(&mut waiter, PERIOD).await);
        let snapshot = activity.snapshot();
        assert_eq!(snapshot.idle_for, None);
        assert_eq!(snapshot.holds.len(), 1);
        assert_eq!(snapshot.holds[0].kind, HoldKind::Sync);
        assert_eq!(snapshot.holds[0].label, "changes for relay r");
        assert_eq!(snapshot.holds[0].held_for, PERIOD);
        drop(hold);
        // The period starts again at the release, not at boot.
        assert!(!resolved_after(&mut waiter, PERIOD - Duration::from_secs(1)).await);
        assert_eq!(
            activity.snapshot().idle_for,
            Some(PERIOD - Duration::from_secs(1))
        );
        assert!(resolved_after(&mut waiter, Duration::from_secs(1)).await);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn idle_waits_for_the_last_of_several_holds() {
        let activity = Activity::new();
        let first = activity.hold(HoldKind::Client, "socket");
        let second = activity.hold(HoldKind::Call, String::from("readDatums"));
        let waiting = activity.clone();
        let mut waiter = tokio::spawn(async move { waiting.idle_for(PERIOD).await });
        drop(first);
        assert!(!resolved_after(&mut waiter, PERIOD).await);
        drop(second);
        assert!(resolved_after(&mut waiter, PERIOD).await);
    }

    /// A hold dropped while a panic unwinds is released like any other.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_panicking_holder_releases_its_hold() {
        let activity = Activity::new();
        let holder = activity.clone();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _hold = holder.hold(HoldKind::Guest, "handler");
            panic!("the handler failed");
        }));
        assert!(panicked.is_err());
        assert_eq!(activity.snapshot().holds, Vec::new());
    }
}
