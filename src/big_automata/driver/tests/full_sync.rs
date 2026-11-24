//! The layer above: `wait_for_full_sync` over the facts of several drivers.

use futures::FutureExt;

use super::rig::*;
use crate::driver::Fact;
use crate::driver::Facts;
use crate::driver::Removed;
use crate::interlude::*;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;

/// Two peers on one hub, each serving part 1.
async fn two_peers() -> (Facts, Rig, Rig) {
    let facts = Facts::new();
    let a = Rig::with(facts.clone(), 8, |_| {});
    let b = Rig::with(facts.clone(), 9, |_| {});
    for rig in [&a, &b] {
        rig.peer.publish(1, &[1], 10).await;
        rig.want(&[1]).await;
    }
    // both pairs are facts before any wait looks for them
    a.fact(1, Fact::Unsynced).await;
    b.fact(1, Fact::Unsynced).await;
    (facts, a, b)
}

fn pairs() -> [(PeerKey, PartKey); 2] {
    [(principal(8), part(1)), (principal(9), part(1))]
}

/// The wait is the conjunction: one peer synced is not enough, and it resolves once the last
/// is.
#[tokio::test(start_paused = true)]
async fn full_sync_waits_for_every_asked_pair() {
    let (facts, a, b) = two_peers().await;
    b.peer.set_down(true);
    a.connect();
    b.connect();
    let mut wait = std::pin::pin!(facts.wait_for_full_sync(pairs()));
    a.synced(1).await;
    n0_future::time::sleep(Duration::from_secs(60)).await;
    assert!(wait.as_mut().now_or_never().is_none(), "one peer is down");
    b.peer.set_down(false);
    assert_eq!(within(wait).await, Ok(()));
}

/// The conjunction holds at one instant: a pair that went `Unsynced` after it was seen
/// `Synced` is waited for again.
#[tokio::test(start_paused = true)]
async fn full_sync_needs_the_pairs_synced_together() {
    let (facts, a, b) = two_peers().await;
    a.connect();
    a.synced(1).await;
    let mut wait = std::pin::pin!(facts.wait_for_full_sync(pairs()));
    assert!(wait.as_mut().now_or_never().is_none());
    a.driver.disconnect();
    a.fact(1, Fact::Unsynced).await;
    b.connect();
    b.synced(1).await;
    assert!(
        wait.as_mut().now_or_never().is_none(),
        "peer a is not synced now"
    );
    a.connect();
    assert_eq!(within(wait).await, Ok(()));
}

/// A pair whose part is removed resolves the waiter with that fact.
#[tokio::test(start_paused = true)]
async fn full_sync_reports_a_removed_part() {
    let (facts, a, b) = two_peers().await;
    b.peer.set_down(true);
    a.connect();
    b.connect();
    let mut wait = std::pin::pin!(facts.wait_for_full_sync(pairs()));
    assert!(wait.as_mut().now_or_never().is_none());
    b.unwant(&[1]).await;
    assert_eq!(
        within(wait).await,
        Err(Removed {
            peer: principal(9),
            part: part(1)
        })
    );
}

/// A pair whose peer goes away (its driver ends) resolves the waiter with that fact.
#[tokio::test(start_paused = true)]
async fn full_sync_reports_a_peer_that_goes_away() {
    let (facts, a, b) = two_peers().await;
    b.peer.set_down(true);
    a.connect();
    b.connect();
    let mut wait = std::pin::pin!(facts.wait_for_full_sync(pairs()));
    assert!(wait.as_mut().now_or_never().is_none());
    drop(b.driver);
    assert_eq!(
        within(wait).await,
        Err(Removed {
            peer: principal(9),
            part: part(1)
        })
    );
}

/// A pair that is not wanted at all, and no pairs at all, resolve at once.
#[tokio::test(start_paused = true)]
async fn full_sync_does_not_wait_on_unwanted_pairs() {
    let (facts, _a, _b) = two_peers().await;
    assert_eq!(facts.wait_for_full_sync([]).now_or_never(), Some(Ok(())));
    assert_eq!(
        facts
            .wait_for_full_sync([(principal(8), part(5))])
            .now_or_never(),
        Some(Err(Removed {
            peer: principal(8),
            part: part(5)
        }))
    );
}
