//! The simulation: the real driver over a memory table and a scripted backend, on a paused
//! clock, driven by a script of writes, connections, crashes and sleeps, with the checks of
//! [`World`] after every step and a quiet drain at the end.
//!
//! # What is modelled
//!
//! - **Writes**: records (a fresh want, a new identity superseding one, another provider or
//!   priority) and withdrawals, each in one transaction with the backend's own write. The
//!   world's naive model of the table is written by the same calls, independently of the
//!   outbox's own record logic, and the table is compared with it after every step.
//! - **Fetches**: scripted by a cycle of fates, some failing, some slow; against a peer that is
//!   down they fail. The backend's content store survives crashes. A script picks whether the
//!   content shares the table's transaction (law 12) or lands after, and how slow a landing is.
//! - **Peers** connect and leave while fetches are running.
//! - **Crashes** drop the loop where it stands, with fetches and landings in flight, and start
//!   a new one over the same table and content: after a plain `Crash` (between a record and its
//!   fetch, or mid-fetch) and after `CrashAfterContent`, which stops the next fetch right after
//!   its content is durable and before its landing.
//!
//! # Checks
//!
//! After every step: the table's rows equal the model's; no violation was recorded (one fetch
//! per object, at most `C` live fetches, every landing agrees with the model about the version
//! it deleted, and no want landed before its content was durable).
//!
//! After a quiet drain (every peer up, fetches reliable, landings fast): every want not
//! withdrawn landed, which is the proof no crash lost one; the table's tombstones are pruned
//! once the outbox's cursor reaches the latest rev; and a fresh driver over the drained
//! table fetches nothing.

use proptest::prelude::*;

use super::driver::Run;
use super::driver::config;
use super::driver::quiet;
use super::driver::until;
use super::driver::within;
use super::world::Fate;
use super::world::World;
use super::*;

#[derive(Debug, Clone)]
enum SimOp {
    Record {
        obj: u8,
        provider: u8,
        priority: u8,
        identity: u8,
    },
    Withdraw(u8),
    Connect(u8),
    Disconnect(u8),
    /// Milliseconds of virtual time.
    Sleep(u16),
    Crash,
    CrashAfterContent,
}

#[derive(Debug, Clone)]
struct Script {
    shared_store: bool,
    cap: usize,
    land_delay_ms: u16,
    fates: Vec<Fate>,
    ops: Vec<SimOp>,
}

fn any_fate() -> impl Strategy<Value = Fate> {
    prop_oneof![
        3 => (0u64..2_000).prop_map(Fate::Ok),
        1 => (0u64..500).prop_map(Fate::Fail),
    ]
}

fn any_sim_op() -> impl Strategy<Value = SimOp> {
    prop_oneof![
        6 => (0u8..4, 1u8..4, 0u8..3, 0u8..3).prop_map(|(obj, provider, priority, identity)| {
            SimOp::Record { obj, provider, priority, identity }
        }),
        2 => (0u8..4).prop_map(SimOp::Withdraw),
        3 => (1u8..4).prop_map(SimOp::Connect),
        2 => (1u8..4).prop_map(SimOp::Disconnect),
        6 => (0u16..3_000).prop_map(SimOp::Sleep),
        1 => Just(SimOp::Crash),
        1 => Just(SimOp::CrashAfterContent),
    ]
}

fn any_script() -> impl Strategy<Value = Script> {
    (
        any::<bool>(),
        1usize..4,
        prop_oneof![Just(0u16), 1u16..1_500],
        prop::collection::vec(any_fate(), 1..6),
        prop::collection::vec(any_sim_op(), 1..40),
    )
        .prop_map(|(shared_store, cap, land_delay_ms, fates, ops)| Script {
            shared_store,
            cap,
            land_delay_ms,
            fates,
            ops,
        })
}

fn check(world: &World) {
    world.check_table();
    assert_eq!(world.violations(), Vec::<String>::new());
}

/// Stop the next fetch right after its content is durable, and crash there.
async fn crash_after_content(run: &mut Run) {
    let armed = Arc::new(tokio::sync::Notify::new());
    run.world
        .with(|shared| shared.crash_after_content = Some(Arc::clone(&armed)));
    // no fetch may come: the crash then is a plain one
    let _ = tokio::time::timeout(Duration::from_secs(10), armed.notified()).await;
    run.world.with(|shared| shared.crash_after_content = None);
    run.restart().await;
}

async fn drain(run: &mut Run) {
    let world = run.world.clone();
    world.with(|shared| {
        shared.reliable = true;
        shared.fail_landings = false;
        shared.land_delay = Duration::ZERO;
    });
    for peer_n in 1..=3 {
        run.connect(peer_n);
    }
    until(|| quiet(&world)).await;
    check(&world);

    let latest = crate::rev::RevSource::latest_rev(world.wants.store())
        .await
        .unwrap();
    within(async {
        while run.consumer_cursor().await != Some(latest) || run.rows().await != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    let started = world.with(|shared| shared.started.len());
    run.restart().await;
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(
        world.with(|shared| shared.started.len()),
        started,
        "a drained table fetches nothing"
    );
    check(&world);
}

fn run_sim(script: Script) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async move {
        let world = World::new(script.cap, script.shared_store);
        world.with(|shared| {
            shared.fates = script.fates;
            shared.land_delay = Duration::from_millis(script.land_delay_ms.into());
        });
        let mut run = Run::start(&world, config(script.cap));
        let base = tokio::time::Instant::now();
        for op in script.ops {
            match op {
                SimOp::Record {
                    obj,
                    provider,
                    priority,
                    identity,
                } => {
                    let now_ms = base.elapsed().as_millis() as u64;
                    world.record(obj, provider, priority, identity, now_ms);
                }
                SimOp::Withdraw(n) => world.withdraw(n),
                SimOp::Connect(p) => run.connect(p),
                SimOp::Disconnect(p) => run.disconnect(p),
                SimOp::Sleep(ms) => tokio::time::sleep(Duration::from_millis(ms.into())).await,
                SimOp::Crash => run.restart().await,
                SimOp::CrashAfterContent => crash_after_content(&mut run).await,
            }
            check(&world);
        }
        drain(&mut run).await;
    });
}

proptest! {
    #[test]
    fn the_outbox_survives_the_script(script in any_script()) {
        run_sim(script);
    }
}

fn record(obj: u8, provider: u8, identity: u8) -> SimOp {
    SimOp::Record {
        obj,
        provider,
        priority: 0,
        identity,
    }
}

fn script(shared_store: bool, ops: Vec<SimOp>) -> Script {
    Script {
        shared_store,
        cap: 2,
        land_delay_ms: 0,
        fates: vec![Fate::Ok(300), Fate::Fail(50)],
        ops,
    }
}

/// A supersession and a withdrawal racing slow fetches, on both landing paths.
#[test]
fn supersessions_and_withdrawals_race_fetches() {
    for shared_store in [false, true] {
        run_sim(script(
            shared_store,
            vec![
                SimOp::Connect(1),
                record(0, 1, 0),
                record(1, 1, 0),
                SimOp::Sleep(100),
                record(0, 1, 1),
                SimOp::Withdraw(1),
                SimOp::Sleep(100),
                record(1, 1, 2),
                record(0, 1, 2),
                SimOp::Sleep(2_000),
            ],
        ));
    }
}

/// Peers leaving mid-fetch send the want to another provider, and one leaving for good parks it
/// until it is back.
#[test]
fn providers_come_and_go() {
    run_sim(script(
        false,
        vec![
            SimOp::Connect(1),
            SimOp::Connect(2),
            record(0, 1, 0),
            record(0, 2, 0),
            record(1, 3, 0),
            SimOp::Sleep(100),
            SimOp::Disconnect(1),
            SimOp::Sleep(1_000),
            SimOp::Connect(3),
            SimOp::Disconnect(2),
            SimOp::Sleep(1_000),
        ],
    ));
}

/// A crash between a record and its fetch, and one between a fetch and its landing, lose
/// nothing.
#[test]
fn crashes_between_record_fetch_and_landing_lose_no_want() {
    run_sim(script(
        false,
        vec![
            SimOp::Connect(1),
            record(0, 1, 0),
            SimOp::Crash,
            record(1, 1, 0),
            SimOp::CrashAfterContent,
            record(2, 1, 0),
            SimOp::Sleep(150),
            SimOp::Crash,
            SimOp::Sleep(500),
        ],
    ));
}
