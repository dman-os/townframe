//! The outbox's configuration swept against a backend with injected latency, on tokio's paused
//! clock: a fetch takes `FETCH` of virtual time and a landing `LAND`, so the makespan and the
//! per-priority latencies are deterministic functions of `C`, `M` and `aging`, the same on
//! every machine, and a table of them can be read as tuning advice.
//!
//! `BIG_AUTOMATA_BENCH_WANTS=2000` sets the number of wants (default 400). Run with
//! `--no-capture` to see the table.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use tokio::time::Instant as Clock;

use super::driver::clock;
use super::*;
use crate::bench::record_line;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;

type Table = MemoryOkv<Vec<u8>, Vec<u8>>;

/// What a fetch costs the backend, in virtual time.
const FETCH: Duration = Duration::from_millis(100);
/// What a landing costs the backend, in virtual time.
const LAND: Duration = Duration::from_millis(5);
/// Distinct priorities the wants cycle through.
const PRIORITIES: usize = 5;
/// The wants arrive 10 ms apart on the writer's clock, so aging has ages to weigh.
const ARRIVAL_GAP_MS: u64 = 10;

fn wants_count() -> usize {
    std::env::var("BIG_AUTOMATA_BENCH_WANTS")
        .map(|wants| wants.parse().expect("the count is a number"))
        .unwrap_or(400)
}

fn obj_of(row: usize) -> ObjKey {
    ObjKey::new(&u32::try_from(row).expect("rows fit u32").to_be_bytes())
}

fn priority_of(obj: &ObjKey) -> usize {
    u32::from_be_bytes(obj.as_bytes().try_into().expect("four bytes")) as usize % PRIORITIES
}

struct SimBackend {
    db: MemoryDb,
    wants: WantsTable<Table>,
    start: Clock,
    landed: Arc<AtomicUsize>,
    done: Arc<tokio::sync::Notify>,
    /// Each landing: the want's priority and the virtual time it landed at.
    landings: Arc<Mutex<Vec<(usize, Duration)>>>,
    total: usize,
}

impl OutboxBackend for SimBackend {
    async fn fetch(&self, _job: &Job) -> Res<Fetched> {
        n0_future::time::sleep(FETCH).await;
        Ok(Fetched::Durable)
    }

    async fn land(&self, obj: &ObjKey, version: Version) -> Res<bool> {
        n0_future::time::sleep(LAND).await;
        let mut tx = self.db.begin();
        let deleted = self.wants.land(&mut tx, obj, version).await?;
        tx.commit().expect("one landing at a time");
        self.landings
            .lock()
            .expect("landings lock")
            .push((priority_of(obj), self.start.elapsed()));
        if self.landed.fetch_add(1, Ordering::SeqCst) + 1 == self.total {
            self.done.notify_one();
        }
        Ok(deleted)
    }
}

struct Outcome {
    config: Config,
    makespan: Duration,
    /// Median landing time of the highest and of the lowest priority.
    high: Duration,
    low: Duration,
}

async fn run(wants_n: usize, outbox: Config) -> Outcome {
    let db = MemoryDb::default();
    let table: Table = db.table();
    let wants = WantsTable::new(table.clone());
    let provider = peer(1);
    for chunk in (0..wants_n).collect::<Vec<_>>().chunks(100) {
        let mut tx = db.begin();
        for &row in chunk {
            let advert = Advert::new(
                provider.clone(),
                u8::try_from(priority_of(&obj_of(row))).expect("small"),
                ByteKey::new(&row.to_be_bytes()),
            );
            wants
                .record(&mut tx, &obj_of(row), advert, row as u64 * ARRIVAL_GAP_MS)
                .await
                .expect("record");
        }
        tx.commit().expect("commit");
    }
    let done = Arc::new(tokio::sync::Notify::new());
    let landings = Arc::new(Mutex::new(vec![]));
    let start = Clock::now();
    let backend = SimBackend {
        db,
        wants,
        start,
        landed: default(),
        done: Arc::clone(&done),
        landings: Arc::clone(&landings),
        total: wants_n,
    };
    let mut config = DriverConfig::new("bench");
    config.outbox = outbox.clone();
    let (handle, driver_loop) = OutboxHandle::start(config, clock(), table, backend);
    let task = tokio::spawn(driver_loop);
    handle.peer_connected(provider);
    done.notified().await;
    let makespan = start.elapsed();
    task.abort();
    let landings = landings.lock().expect("landings lock");
    let median = |priority: usize| {
        let mut times: Vec<_> = landings
            .iter()
            .filter(|(landed, _)| *landed == priority)
            .map(|(_, at)| *at)
            .collect();
        times.sort();
        times[times.len() / 2]
    };
    Outcome {
        config: outbox,
        makespan,
        high: median(PRIORITIES - 1),
        low: median(0),
    }
}

fn config(max_fetches: usize, max_in_flight: usize, aging: Duration) -> Config {
    Config {
        max_fetches,
        max_in_flight,
        min_backoff: Duration::from_secs(2),
        max_backoff: Duration::from_secs(60),
        aging,
    }
}

fn aging_label(aging: Duration) -> String {
    if aging == Duration::MAX {
        "strict".to_owned()
    } else {
        format!("{}ms", aging.as_millis())
    }
}

fn print_table(wants_n: usize, outcomes: &[Outcome]) {
    println!(
        "{:>4} {:>4} {:>8} {:>10} {:>12} {:>12} {:>12} {:>12}",
        "C", "M", "aging", "makespan_s", "wants/s", "high_p50_s", "low_p50_s", "fetch_ms"
    );
    for outcome in outcomes {
        println!(
            "{:>4} {:>4} {:>8} {:>10.3} {:>12.1} {:>12.3} {:>12.3} {:>12}",
            outcome.config.max_fetches,
            outcome.config.max_in_flight,
            aging_label(outcome.config.aging),
            outcome.makespan.as_secs_f64(),
            wants_n as f64 / outcome.makespan.as_secs_f64(),
            outcome.high.as_secs_f64(),
            outcome.low.as_secs_f64(),
            FETCH.as_millis(),
        );
        let params = [
            ("n", wants_n.to_string()),
            ("C", outcome.config.max_fetches.to_string()),
            ("M", outcome.config.max_in_flight.to_string()),
            ("aging", aging_label(outcome.config.aging)),
            ("fetch_ms", FETCH.as_millis().to_string()),
        ];
        for (metric, value) in [
            ("sim_time", outcome.makespan.as_secs_f64()),
            ("sim_time_high_p50", outcome.high.as_secs_f64()),
            ("sim_time_low_p50", outcome.low.as_secs_f64()),
        ] {
            println!(
                "{}",
                record_line("big_automata::outbox_sweep", metric, "s", value, &params)
            );
        }
    }
}

/// `C` against `M` at the default aging, then aging at one `C` and `M`.
#[tokio::test(start_paused = true)]
async fn outbox_configuration_sweep() {
    let wants_n = wants_count();
    let default_aging = Duration::from_secs(60);
    let mut outcomes = vec![];
    for max_fetches in [1, 4, 16, 64] {
        for factor in [1, 2, 4] {
            outcomes.push(
                run(
                    wants_n,
                    config(max_fetches, max_fetches * factor, default_aging),
                )
                .await,
            );
        }
    }
    for aging in [
        Duration::MAX,
        Duration::from_secs(5),
        Duration::from_millis(500),
    ] {
        outcomes.push(run(wants_n, config(4, 16, aging)).await);
    }
    print_table(wants_n, &outcomes);

    // The grid, `M = C`, `2C`, `4C` per `C`: more fetches in flight finish sooner.
    let makespan = |max_fetches: usize, factor: usize| {
        outcomes
            .iter()
            .find(|outcome| {
                outcome.config.max_fetches == max_fetches
                    && outcome.config.max_in_flight == max_fetches * factor
                    && outcome.config.aging == default_aging
            })
            .expect("the grid holds it")
            .makespan
    };
    assert!(makespan(4, 1) < makespan(1, 1));
    assert!(makespan(16, 1) < makespan(4, 1));
    // `M = C` leaves no room to land beside a full set of fetches, so it cannot beat more room.
    assert!(makespan(4, 4) <= makespan(4, 1));
    // strict priority serves the top priority first; the aging sweep keeps the same work
    let strict = outcomes
        .iter()
        .find(|outcome| outcome.config.aging == Duration::MAX)
        .expect("the sweep holds it");
    assert!(
        strict.high < strict.low,
        "strict priority favours the top priority"
    );
}
