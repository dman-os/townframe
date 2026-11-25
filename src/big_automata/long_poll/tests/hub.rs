//! The driver on a paused clock: parking, waking, lingering, timing out and cancelling through
//! the real [`LongPollHub`] and [`Waiting`], with the runtime's own timer.

use std::pin::pin;

use super::*;
use crate::Clock;

fn hub() -> LongPollHub<u8> {
    let clock: Clock = Arc::new(|| tokio::time::Instant::now().into_std());
    LongPollHub::new(clock)
}

fn deadline(hub: &LongPollHub<u8>, ms: u64) -> Instant {
    hub.now() + Duration::from_millis(ms)
}

fn immediate() -> Linger {
    Linger::NONE
}

/// Polls `waiting` once, on its own task's behalf: `None` while it is pending.
async fn poll_once(waiting: &mut (impl Future<Output = Outcome> + Unpin)) -> Option<Outcome> {
    match futures::poll!(waiting) {
        std::task::Poll::Ready(outcome) => Some(outcome),
        std::task::Poll::Pending => None,
    }
}

#[tokio::test(start_paused = true)]
async fn law_12_a_commit_between_park_and_first_poll_is_not_lost() {
    let hub = hub();
    let waiting = hub.park(&[1], 0, deadline(&hub, 10_000), immediate());
    hub.committed(1, [(1, 1)]);
    assert_eq!(
        waiting.wait(&RuntimeTimer).await,
        Outcome::Ready { through_rev: 1 }
    );
    assert_eq!(hub.parked(), 0);
}

#[tokio::test(start_paused = true)]
async fn law_13_dropping_leaves_nothing_parked() {
    let hub = hub();
    // Dropped before it is ever polled.
    drop(hub.park(&[1], 0, deadline(&hub, 10_000), immediate()));
    assert_eq!(hub.parked(), 0);
    // Dropped while waiting.
    let mut waiting = Box::pin(
        hub.park(&[1], 0, deadline(&hub, 10_000), immediate())
            .wait(&RuntimeTimer),
    );
    assert_eq!(poll_once(&mut waiting).await, None);
    assert_eq!(hub.parked(), 1);
    drop(waiting);
    assert_eq!(hub.parked(), 0);
    // Dropped after it ended, unread.
    let ended = hub.park(&[1], 0, deadline(&hub, 10_000), immediate());
    hub.committed(1, [(1, 1)]);
    assert!(ended.outcome().is_some());
    drop(ended);
    assert_eq!(hub.parked(), 0);
    // A commit finds nobody.
    let touched = hub.touched();
    hub.committed(2, [(1, 1)]);
    assert_eq!(hub.touched(), touched);
}

#[tokio::test(start_paused = true)]
async fn law_14_a_commit_wakes_exactly_the_waiters_it_concerns() {
    let hub = hub();
    let mut first = pin!(
        hub.park(&[1], 0, deadline(&hub, 10_000), immediate())
            .wait(&RuntimeTimer)
    );
    let mut second = pin!(
        hub.park(&[2], 0, deadline(&hub, 10_000), immediate())
            .wait(&RuntimeTimer)
    );
    let mut third = pin!(
        hub.park(&[2, 3], 0, deadline(&hub, 10_000), immediate())
            .wait(&RuntimeTimer)
    );
    assert_eq!(poll_once(&mut first).await, None);
    assert_eq!(poll_once(&mut second).await, None);
    assert_eq!(poll_once(&mut third).await, None);
    let touched = hub.touched();
    hub.committed(1, [(2, 1)]);
    assert_eq!(hub.touched() - touched, 2);
    assert_eq!(
        poll_once(&mut second).await,
        Some(Outcome::Ready { through_rev: 1 })
    );
    assert_eq!(
        poll_once(&mut third).await,
        Some(Outcome::Ready { through_rev: 1 })
    );
    assert_eq!(poll_once(&mut first).await, None);
    assert_eq!(hub.parked(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_poll_lingers_then_answers_as_one() {
    let hub = hub();
    let mut waiting = pin!(
        hub.park(&[1], 0, deadline(&hub, 10_000), linger(100, 1_000))
            .wait(&RuntimeTimer)
    );
    assert_eq!(poll_once(&mut waiting).await, None);
    hub.committed(1, [(1, 1)]);
    tokio::time::sleep(Duration::from_millis(60)).await;
    hub.committed(2, [(1, 1)]);
    assert_eq!(
        poll_once(&mut waiting).await,
        None,
        "still inside the window"
    );
    hub.committed(3, [(1, 1)]);
    // 60ms in: the window ends 100ms after the first commit, not after the last.
    tokio::time::sleep(Duration::from_millis(41)).await;
    assert_eq!(
        poll_once(&mut waiting).await,
        Some(Outcome::Ready { through_rev: 3 })
    );
}

#[tokio::test(start_paused = true)]
async fn a_quiet_poll_times_out_at_its_deadline() {
    let hub = hub();
    let mut waiting = pin!(
        hub.park(&[1], 0, deadline(&hub, 500), immediate())
            .wait(&RuntimeTimer)
    );
    assert_eq!(poll_once(&mut waiting).await, None);
    hub.committed(1, [(2, 1)]);
    tokio::time::sleep(Duration::from_millis(499)).await;
    assert_eq!(poll_once(&mut waiting).await, None);
    assert_eq!(waiting.await, Outcome::TimedOut);
    assert_eq!(hub.parked(), 0);
}

/// A timer whose sleeps are over as soon as they start, as a simulation's is: the poll ends at
/// its deadline whatever the clock says.
struct Expired;

impl Timer for Expired {
    fn sleep(&self, _: Duration) -> impl Future<Output = ()> + Send + use<> {
        std::future::ready(())
    }
}

#[tokio::test(start_paused = true)]
async fn an_expired_timer_ends_the_poll_at_its_deadline() {
    let hub = hub();
    let other = hub.park(&[1], 0, deadline(&hub, 9_000), immediate());
    let waiting = hub.park(&[1], 0, deadline(&hub, 500), immediate());
    assert_eq!(waiting.wait(&Expired).await, Outcome::TimedOut);
    assert_eq!(hub.parked(), 1, "a later deadline is not ended with it");
    drop(other);
}
