//! Laws 1 to 5: the loop, the calls, the timers and the cursor writes.

use super::rig::*;
use crate::driver::DriverConfig;
use crate::driver::Fact;
use crate::driver::Facts;
use crate::driver::PeerCursors;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::memory::MemoryPeerCursors;
use crate::interlude::*;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::laws::set;
use crate::part_store::memory::MemoryPartStore;

/// Law 1: route changes, outcomes and timers all reach the machine through the one loop, so a
/// storm of them leaves exactly the final routes wanted (the machine's own fact recomputation
/// asserts after every flush in test builds).
#[tokio::test(start_paused = true)]
async fn law_1_inputs_land_one_at_a_time_through_the_loop() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1, 2], 10).await;
    rig.peer.publish(2, &[3], 20).await;
    rig.peer.publish(3, &[4], 30).await;
    rig.connect();
    for _ in 0..8 {
        rig.want(&[1, 2, 3]).await;
        tokio::task::yield_now().await;
        rig.unwant(&[2]).await;
        rig.want(&[2]).await;
        rig.unwant(&[1, 3]).await;
        tokio::task::yield_now().await;
        rig.want(&[1, 3]).await;
        rig.unwant(&[2]).await;
    }
    // a connection lost mid-storm: the rest of the churn lands on a fresh machine
    rig.driver.disconnect();
    rig.connect();
    rig.want(&[2]).await;
    rig.unwant(&[2]).await;
    rig.synced(1).await;
    rig.synced(3).await;
    rig.assert_converged(1, 4).await;
    rig.assert_converged(3, 4).await;
    let mut stream = rig.facts.subscribe();
    let mut seen = vec![];
    for _ in 0..2 {
        let event = stream.next().await;
        seen.push((event.part, event.fact));
    }
    assert_eq!(
        seen,
        [(part(1), Fact::Synced), (part(3), Fact::Synced)],
        "part 2 was removed and is not a fact"
    );
}

/// Law 2: with nothing failing, every object is recorded once and fetched once, and every call
/// ends.
#[tokio::test(start_paused = true)]
async fn law_2_every_command_runs_once_and_ends() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1, 2, 3], 10).await;
    rig.want(&[1]).await;
    rig.connect();
    rig.synced(1).await;
    for at in 1..=3 {
        for kind in [Kind::Record, Kind::Fetch] {
            let key = obj(at);
            assert_eq!(
                rig.backend.count(&BackendEvent::Started(kind, key.clone())),
                1
            );
            assert_eq!(
                rig.backend
                    .count(&BackendEvent::Finished(kind, key.clone())),
                1
            );
            assert_eq!(rig.backend.count(&BackendEvent::Dropped(kind, key)), 0);
        }
    }
    rig.assert_converged(1, 3).await;
}

/// Law 2: a `Cancel` drops the call's future and the machine is still told, so the slot frees
/// and the follow-up job runs. Removing part 1 contradicts the fetch of an object that is also
/// in part 2: the fetch is cancelled and part 2 still gets the object.
#[tokio::test(start_paused = true)]
async fn law_2_a_cancel_drops_the_call_and_still_reports() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    rig.peer.publish(2, &[1], 10).await;
    rig.backend.hold_fetches(true);
    rig.want(&[1, 2]).await;
    rig.connect();
    rig.backend.running(Kind::Fetch, &obj(1)).await;
    rig.unwant(&[1]).await;
    until(|| {
        rig.backend
            .count(&BackendEvent::Dropped(Kind::Fetch, obj(1)))
            == 1
    })
    .await;
    rig.backend.hold_fetches(false);
    rig.synced(2).await;
    rig.assert_converged(2, 1).await;
}

/// Law 3: the driver adds no concurrency of its own, so the machine's bounds hold at the
/// backend: at most `fetch_cap` fetches and `window` objects with a call at once.
#[tokio::test(start_paused = true)]
async fn law_3_the_machines_bounds_are_the_drivers() {
    let rig = Rig::with(Facts::new(), LOCAL_PEER, |config| {
        config.window = 3;
        config.fetch_cap = 1;
    });
    let objs: Vec<u8> = (1..=12).collect();
    rig.peer.publish(1, &objs, 10).await;
    rig.want(&[1]).await;
    rig.connect();
    rig.synced(1).await;
    assert_eq!(rig.backend.peak(Kind::Fetch), 1);
    assert!(rig.backend.peak_objs() <= 3, "{}", rig.backend.peak_objs());
    rig.assert_converged(1, 12).await;
}

/// Law 4: the loop wakes at the machine's `next_due`: a failed record is retried after the
/// machine's backoff with no other input to wake the loop, and not before.
#[tokio::test(start_paused = true)]
async fn law_4_the_loop_wakes_at_the_machines_deadline() {
    let run = |failing: usize| async move {
        let rig = Rig::new();
        rig.backend.fail_records(failing);
        rig.peer.publish(1, &[1], 10).await;
        let start = tokio::time::Instant::now();
        rig.want(&[1]).await;
        rig.connect();
        rig.synced(1).await;
        rig.assert_converged(1, 1).await;
        (start.elapsed(), rig.config.min_backoff)
    };
    let (clean, backoff) = run(0).await;
    assert!(clean < backoff, "{clean:?} without a failure");
    let (retried, _) = run(1).await;
    assert!(
        retried >= backoff,
        "{retried:?} with one failure: retried too soon"
    );
}

/// A table whose first writes are the slowest, so writes that overlapped would land out of
/// order.
struct SlowFirst {
    table: MemoryPeerCursors,
    calls: std::sync::atomic::AtomicU64,
}

impl PeerCursors for SlowFirst {
    async fn read(&self, peer: &PeerKey, backend: &str, parts: &[PartKey]) -> Res<Vec<u64>> {
        self.table.read(peer, backend, parts).await
    }

    async fn write(&self, peer: &PeerKey, backend: &str, writes: &[(PartKey, u64)]) -> Res<()> {
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        n0_future::time::sleep(Duration::from_secs(8u64.saturating_sub(call))).await;
        self.table.write(peer, backend, writes).await
    }
}

/// Law 5: each write lands before the next is issued and a part's cursor never goes back,
/// even when the first writes are the slowest.
#[tokio::test(start_paused = true)]
async fn law_5_cursor_writes_land_in_order_and_never_go_back() {
    let table = MemoryPeerCursors::default();
    let peer = ScriptedPeer::new();
    let local = MemoryPartStore::default();
    let backend = ScriptedBackend::new(local.clone(), peer.store().clone());
    let config = DriverConfig::new(principal(LOCAL_PEER), "memory");
    let driver = PeerDriver::spawn(
        config.clone(),
        clock(),
        Facts::new(),
        backend,
        SlowFirst {
            table: table.clone(),
            calls: default(),
        },
        local,
    );
    peer.publish(1, &[1], 1).await;
    driver
        .update_routes(RouteUpdate {
            added_parts: vec![part(1)],
            ..default()
        })
        .await;
    driver.connect(peer.clone());
    let mut head = 0;
    for value in 2..=6u8 {
        until(|| !table.written().is_empty()).await;
        head = peer
            .commit(vec![set(&[(1, value)])])
            .await
            .expect("a change");
        until(|| table.get(&config.peer, "memory", &part(1)) == head).await;
    }
    let written: Vec<u64> = table
        .written()
        .into_iter()
        .map(|(_, cursor)| cursor)
        .collect();
    assert!(written.len() >= 2, "{written:?}");
    assert!(written.is_sorted(), "a cursor went back: {written:?}");
    assert_eq!(*written.last().unwrap(), head);
}
