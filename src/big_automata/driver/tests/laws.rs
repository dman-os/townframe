//! Laws 1 to 5: the loop, the calls, the timers and the cursor writes.

use super::rig::*;
use crate::driver::DriverConfig;
use crate::driver::Fact;
use crate::driver::Facts;
use crate::driver::ObjJob;
use crate::driver::PeerCursors;
use crate::driver::PeerDriver;
use crate::driver::RouteUpdate;
use crate::driver::Settle;
use crate::driver::SyncBackend;
use crate::driver::memory::LwwPayload;
use crate::driver::memory::MemoryBackend;
use crate::driver::memory::MemoryPeerCursors;
use crate::interlude::*;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;
use crate::sync::PeerCursor;

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
/// backend: at most `fetch_cap` fetches and `window` objects with a call at once. A replay
/// page is never split inside a revision, so the window may be exceeded by the rows of one
/// revision less one: the backlog is committed two objects to a revision, so up to 4.
#[tokio::test(start_paused = true)]
async fn law_3_the_machines_bounds_are_the_drivers() {
    let rig = Rig::with(Facts::new(), LOCAL_PEER, |config| {
        config.window = 3;
        config.fetch_cap = 1;
    });
    let objs: Vec<u8> = (1..=12).collect();
    for pair in objs.chunks(2) {
        rig.peer.publish(1, pair, 10).await;
    }
    rig.want(&[1]).await;
    rig.connect();
    rig.synced(1).await;
    assert_eq!(rig.backend.peak(Kind::Fetch), 1);
    assert!(rig.backend.peak_objs() <= 4, "{}", rig.backend.peak_objs());
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
    // The held `Live` round would wake the loop only after its 15s hold: a retry earlier than
    // that was woken by the machine's own deadline.
    assert!(
        retried < Duration::from_secs(10),
        "{retried:?} with one failure: the loop slept past the deadline"
    );
}

/// A table whose first writes are the slowest, so writes that overlapped would land out of
/// order.
struct SlowFirst {
    table: MemoryPeerCursors,
    calls: std::sync::atomic::AtomicU64,
}

impl PeerCursors for SlowFirst {
    async fn read(
        &self,
        peer: &PeerKey,
        backend: &str,
        parts: &[PartKey],
    ) -> Res<Vec<Option<PeerCursor>>> {
        self.table.read(peer, backend, parts).await
    }

    async fn write(
        &self,
        peer: &PeerKey,
        backend: &str,
        writes: &[(PartKey, PeerCursor)],
    ) -> Res<()> {
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
    peer.follow(&local);
    let config = DriverConfig::new(principal(LOCAL_PEER), "memory");
    let (driver, driver_loop) = PeerDriver::start(
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
    tokio::spawn(driver_loop);
    peer.publish(1, &[1], 1).await;
    driver
        .update_routes(RouteUpdate {
            added_parts: vec![part(1)],
            ..default()
        })
        .await;
    driver.connect(peer.clone());
    let mut head = 0;
    // Writes of the part arrive faster than the table lands them (the first take 8s, the next
    // 7s, and so on), so writes that were not awaited in turn would overlap and be overtaken.
    for value in 2..=6u8 {
        head = peer
            .commit(vec![LwwPayload::set(&[(1, value)])])
            .await
            .expect("a change");
        n0_future::time::sleep(Duration::from_millis(500)).await;
    }
    until(|| {
        table
            .get(&config.peer, "memory", &part(1))
            .is_some_and(|cursor| cursor.rev == head)
    })
    .await;
    let written: Vec<u64> = table
        .written()
        .into_iter()
        .map(|(_, cursor)| cursor.rev)
        .collect();
    assert!(written.len() >= 2, "{written:?}");
    assert!(written.is_sorted(), "a cursor went back: {written:?}");
    assert_eq!(*written.last().unwrap(), head);
}

/// A backend whose futures are not `Send`: it holds an `Rc` across an await, as a browser
/// backend holding a JS handle would.
struct LocalBackend(MemoryBackend);

impl SyncBackend for LocalBackend {
    async fn record(&self, job: ObjJob) -> Res<Settle> {
        let held = std::rc::Rc::new(());
        tokio::task::yield_now().await;
        let settle = self.0.record(job).await;
        drop(held);
        settle
    }

    async fn fetch(&self, job: ObjJob) -> Res<()> {
        let held = std::rc::Rc::new(());
        tokio::task::yield_now().await;
        let fetched = self.0.fetch(job).await;
        drop(held);
        fetched
    }
}

/// The traits put no `Send` bound on the embedder's futures and the driver hands its loop over
/// unspawned, so a backend that is not `Send` runs the loop on a local executor and reaches
/// `Synced` with the payload landed (the browser's situation: `spawn_local`).
#[tokio::test(start_paused = true)]
async fn law_1_the_loop_runs_locally_over_a_backend_that_is_not_send() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let peer = ScriptedPeer::new();
            let local = MemoryPartStore::default();
            let facts = Facts::new();
            let config = DriverConfig::new(principal(LOCAL_PEER), "memory");
            peer.follow(&local);
            let (driver, driver_loop) = PeerDriver::start(
                config.clone(),
                clock(),
                facts.clone(),
                LocalBackend(MemoryBackend::new(
                    PeerKey::new(&[1]),
                    local.clone(),
                    peer.store().clone(),
                )),
                MemoryPeerCursors::default(),
                local.clone(),
            );
            tokio::task::spawn_local(driver_loop);
            peer.publish(1, &[1], 10).await;
            driver
                .update_routes(RouteUpdate {
                    added_parts: vec![part(1)],
                    ..default()
                })
                .await;
            driver.connect(peer.clone());
            let synced = facts.wait_for_full_sync([(config.peer.clone(), part(1))]);
            assert_eq!(within(synced).await, Ok(()));
            let members = Rig::members(&local, 1, 1).await;
            assert_eq!(members.len(), 1, "the object landed: {members:?}");
        })
        .await;
}

/// The memory table obeys the table laws every [`PeerCursors`] implementation runs.
#[tokio::test]
async fn the_memory_cursor_table_obeys_the_table_laws() -> Res<()> {
    crate::driver::table_laws::peer_cursor_table_laws(&MemoryPeerCursors::default()).await
}
