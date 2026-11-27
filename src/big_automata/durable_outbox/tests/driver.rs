//! The driver's laws end to end: the real loop over a memory table and a scripted backend, on
//! tokio's paused clock, so every wait is virtual time.

use std::sync::Arc;

use tokio::task::JoinHandle;

use super::world::Fate;
use super::world::World;
use super::world::WorldBackend;
use super::*;
use crate::kit::Clock;
use crate::okv::OkvStore;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::RevSource;

pub(super) const CONSUMER: &str = "outbox";

pub(super) fn clock() -> Clock {
    Arc::new(|| tokio::time::Instant::now().into_std())
}

/// A virtual-time bound on a wait, so a stalled driver fails the test instead of hanging it.
pub(super) async fn within<T>(wait: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3600), wait)
        .await
        .expect("the driver stalled: the wait did not end in an hour of virtual time")
}

pub(super) async fn until(mut cond: impl FnMut() -> bool) {
    within(async {
        while !cond() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

pub(super) fn config(max_fetches: usize) -> DriverConfig {
    DriverConfig {
        outbox: Config {
            max_fetches,
            max_in_flight: max_fetches + 2,
            min_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(800),
            aging: Duration::from_secs(60),
        },
        consumer: CONSUMER.to_owned(),
        page_entries: std::num::NonZeroUsize::new(4).unwrap(),
        prune_every: 1,
    }
}

/// A driver running over a world; a crash drops its loop where it stands.
pub(super) struct Run {
    pub world: World,
    config: DriverConfig,
    pub handle: OutboxHandle,
    pub task: JoinHandle<()>,
    crashed: bool,
}

impl Run {
    pub(super) fn start(world: &World, config: DriverConfig) -> Self {
        let (handle, task) = Self::spawn(world, &config);
        Self {
            world: world.clone(),
            config,
            handle,
            task,
            crashed: false,
        }
    }

    fn spawn(world: &World, config: &DriverConfig) -> (OutboxHandle, JoinHandle<()>) {
        let (handle, run) = OutboxHandle::start(
            config.clone(),
            clock(),
            world.wants.store().clone(),
            WorldBackend(world.clone()),
        );
        // every peer that is up is announced to a new driver, as an embedder would
        for peer_n in world.with(|shared| shared.up.clone()) {
            handle.peer_connected(peer(peer_n));
        }
        (handle, tokio::spawn(run))
    }

    /// The process dies: the loop and every future it owns are dropped at their await points.
    pub(super) async fn crash(&mut self) {
        if std::mem::replace(&mut self.crashed, true) {
            return;
        }
        self.task.abort();
        // a loop that had already panicked is a bug the crash must not hide
        if let Err(error) = (&mut self.task).await
            && error.is_panic()
        {
            std::panic::resume_unwind(error.into_panic());
        }
    }

    pub(super) async fn restart(&mut self) {
        self.crash().await;
        (self.handle, self.task) = Self::spawn(&self.world, &self.config);
        self.crashed = false;
    }

    pub(super) fn connect(&self, p: u8) {
        self.world.with(|shared| shared.up.insert(p));
        self.handle.peer_connected(peer(p));
    }

    pub(super) fn disconnect(&self, p: u8) {
        self.world.with(|shared| shared.up.remove(&p));
        self.handle.peer_disconnected(peer(p));
    }

    pub(super) async fn consumer_cursor(&self) -> Option<u64> {
        self.world
            .wants
            .store()
            .consumers()
            .await
            .unwrap()
            .get(CONSUMER)
            .copied()
    }

    /// The table's rows, tombstones included.
    pub(super) async fn rows(&self) -> usize {
        let store = self.world.wants.store();
        let latest = store.latest_rev().await.unwrap();
        if latest == 0 {
            return 0;
        }
        let mut page = Page::default();
        let limits = Limits {
            max_entries: std::num::NonZeroUsize::MAX,
            max_bytes: None,
        };
        store
            .read(&crate::okv::Selector::All, 0, latest, limits, &mut page)
            .await
            .unwrap();
        page.entries.len()
    }
}

fn world(cap: usize, shared_store: bool, fates: &[Fate]) -> World {
    let world = World::new(cap, shared_store);
    world.with(|shared| shared.fates = fates.to_vec());
    world
}

pub(super) fn quiet(world: &World) -> bool {
    world.with(|shared| shared.model.is_empty() && shared.in_flight.is_empty())
}

#[tokio::test(start_paused = true)]
async fn law_4_the_stream_is_polled_while_the_budget_is_full() {
    let world = world(1, false, &[Fate::Hang]);
    let run = Run::start(&world, config(1));
    run.connect(1);
    for n in 1..=5 {
        world.record(n, 1, 0, 1, 0);
    }
    let latest = world.wants.store().latest_rev().await.unwrap();
    until(|| world.with(|shared| shared.started.len() == 1)).await;
    // the one slot is held by a fetch that never ends, and the stream still took every row
    within(async {
        while run.consumer_cursor().await != Some(latest) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(world.with(|shared| shared.started.len()), 1);
    assert!(world.violations().is_empty());
}

#[tokio::test(start_paused = true)]
async fn law_5_a_superseding_want_runs_after_the_old_fetch_is_gone() {
    let world = world(2, false, &[Fate::Hang, Fate::Ok(10)]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    until(|| world.with(|shared| shared.started.len() == 1)).await;
    world.record(1, 1, 0, 2, 0);
    until(|| quiet(&world)).await;
    let started = world.with(|shared| shared.started.clone());
    assert_eq!(started.len(), 2);
    assert_ne!(started[0].2, started[1].2, "the new version");
    assert_eq!(world.with(|shared| shared.durable[&1].clone()), [2].into());
    assert_eq!(world.violations(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn law_6_an_equal_identity_overwrite_does_not_restart_the_fetch() {
    let world = world(2, false, &[Fate::Hang]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    run.connect(2);
    world.record(1, 1, 0, 1, 0);
    until(|| world.with(|shared| shared.started.len() == 1)).await;
    for priority in 1..4 {
        world.record(1, 2, priority, 1, 0);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    assert_eq!(world.with(|shared| shared.started.len()), 1);
    assert_eq!(world.with(|shared| shared.in_flight.len()), 1);
}

#[tokio::test(start_paused = true)]
async fn law_8_the_cap_bounds_live_fetches() {
    let world = world(2, false, &[Fate::Ok(500)]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    for n in 1..=6 {
        world.record(n, 1, 0, 1, 0);
    }
    until(|| quiet(&world)).await;
    assert_eq!(world.violations(), Vec::<String>::new());
    assert_eq!(world.with(|shared| shared.started.len()), 6);
}

#[tokio::test(start_paused = true)]
async fn law_9_a_want_parks_until_a_provider_connects() {
    let world = world(2, false, &[]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 2, 0, 1, 0);
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(world.with(|shared| shared.started.len()), 0, "no spinning");
    run.connect(2);
    until(|| quiet(&world)).await;
    assert_eq!(world.with(|shared| shared.started[0].1), 2);
}

#[tokio::test(start_paused = true)]
async fn law_9_a_disconnect_cancels_and_the_want_goes_to_another_provider() {
    let world = world(2, false, &[Fate::Hang, Fate::Ok(10)]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    run.connect(2);
    world.record(1, 1, 0, 1, 0);
    world.record(1, 2, 0, 1, 0);
    until(|| world.with(|shared| shared.started.len() == 1)).await;
    assert_eq!(world.with(|shared| shared.started[0].1), 1);
    run.disconnect(1);
    until(|| quiet(&world)).await;
    let started = world.with(|shared| shared.started.clone());
    assert_eq!(started.iter().map(|s| s.1).collect::<Vec<_>>(), vec![1, 2]);
}

#[tokio::test(start_paused = true)]
async fn law_10_a_failed_fetch_is_retried_after_a_backoff() {
    let world = world(2, false, &[Fate::Fail(10), Fate::Ok(10)]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    let before = tokio::time::Instant::now();
    world.record(1, 1, 0, 1, 0);
    until(|| quiet(&world)).await;
    assert_eq!(world.with(|shared| shared.started.len()), 2);
    assert!(
        before.elapsed() >= Duration::from_millis(100),
        "it waited its backoff"
    );
}

#[tokio::test(start_paused = true)]
async fn law_11_a_withdrawal_cancels_the_fetch() {
    let world = world(2, false, &[Fate::Hang]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    until(|| world.with(|shared| shared.in_flight.len() == 1)).await;
    world.withdraw(1);
    until(|| world.with(|shared| shared.in_flight.is_empty())).await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
        world.with(|shared| shared.started.len()),
        1,
        "nothing restarted it"
    );
}

#[tokio::test(start_paused = true)]
async fn law_12_the_driver_lands_after_the_content_is_durable() {
    let world = world(2, false, &[Fate::Ok(10)]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    until(|| quiet(&world)).await;
    assert_eq!(world.with(|shared| shared.landings.clone().len()), 1);
    assert_eq!(world.violations(), Vec::<String>::new());
    world.check_table();
}

#[tokio::test(start_paused = true)]
async fn law_12_a_backend_that_shares_the_store_lands_in_the_fetch() {
    let world = world(2, true, &[Fate::Ok(10)]);
    let run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    until(|| quiet(&world)).await;
    assert_eq!(world.with(|shared| shared.landings.clone()).len(), 1);
    assert_eq!(world.violations(), Vec::<String>::new());
}

/// A crash between the content and the landing is one more fetch of the same version.
#[tokio::test(start_paused = true)]
async fn law_12_a_crash_between_fetch_and_landing_fetches_once_more() {
    let world = world(2, false, &[Fate::Ok(10)]);
    let notify = Arc::new(tokio::sync::Notify::new());
    world.with(|shared| shared.crash_after_content = Some(Arc::clone(&notify)));
    let mut run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    within(notify.notified()).await;
    run.crash().await;
    world.with(|shared| shared.crash_after_content = None);
    assert_eq!(
        world.with(|shared| shared.landings.len()),
        0,
        "the want is still there"
    );
    world.check_table();

    run.restart().await;
    until(|| quiet(&world)).await;
    let started = world.with(|shared| shared.started.clone());
    assert_eq!(started.len(), 2);
    assert_eq!(started[0].2, started[1].2, "the same version again");
    assert_eq!(world.violations(), Vec::<String>::new());
}

/// A want withdrawn while it lands finds its landing run to the end and answer `false`.
#[tokio::test(start_paused = true)]
async fn law_13_a_landing_is_not_cancelled() {
    let world = world(2, false, &[Fate::Ok(10)]);
    world.with(|shared| shared.land_delay = Duration::from_secs(1));
    let run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    until(|| world.with(|shared| shared.durable.contains_key(&1))).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    world.withdraw(1);
    tokio::time::sleep(Duration::from_secs(5)).await;
    let landings = world.with(|shared| shared.landings.clone());
    assert_eq!(landings.len(), 1, "the landing ran to its end");
    assert!(!landings[0].2, "and found the row gone");
    assert_eq!(world.violations(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn law_14_the_outbox_reports_its_cursor_and_the_tombstones_are_pruned() {
    let world = world(2, false, &[Fate::Ok(10)]);
    let run = Run::start(&world, config(2));
    within(async {
        while run.consumer_cursor().await.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(run.consumer_cursor().await, Some(0), "registered at start");
    run.connect(1);
    for n in 1..=3 {
        world.record(n, 1, 0, 1, 0);
    }
    until(|| quiet(&world)).await;
    let latest = world.wants.store().latest_rev().await.unwrap();
    within(async {
        while run.consumer_cursor().await != Some(latest) || run.rows().await != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        run.rows().await,
        0,
        "three landings left no tombstone behind"
    );
}

#[tokio::test(start_paused = true)]
async fn law_15_a_store_error_in_a_landing_crashes_the_loop() {
    let world = world(2, false, &[Fate::Ok(10)]);
    world.with(|shared| shared.fail_landings = true);
    let mut run = Run::start(&world, config(2));
    run.connect(1);
    world.record(1, 1, 0, 1, 0);
    let err = within(&mut run.task).await.expect_err("the loop panics");
    assert!(err.is_panic());
}
