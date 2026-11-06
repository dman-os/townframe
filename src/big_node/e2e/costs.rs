//! What the node's operations cost (ADR 032 section 9, ledger D25): wall time, heap allocated and
//! heap peak per operation, plus the storage a note adds, for one scripted workload driven
//! through the service clients like a user.
//!
//! Two kinds of claim live here, following `big_sync`'s `BandRun`: work is *counted* and asserted
//! on, wall time is *measured* and only reported. Allocations are the counted work. They are a
//! process-wide count of everything the node did for an operation (its actors, its background
//! workers, SQLite's Rust callers), they do not depend on the machine or its load, and a cost that
//! grows with a data size grows the count with it. So a growth claim is a ratio of counts between
//! two sizes, asserted in a debug build in the default run, and fails deterministically when an
//! operation turns linear.
//!
//! The cost table is the documentation side. [`long_test_cost_table`] runs every operation at a small size
//! in the default run, so the script cannot rot. For the numbers that matter, run it in release
//! with output shown and a size of your choosing:
//!
//! ```text
//! BIG_NODE_COSTS_NOTES=200 cargo nextest run --release -p big_node --no-capture -E 'test(/costs::/)'
//! ```
//!
//! and read the `COST` lines. Medians are reported per operation; `max` exposes a slow outlier or
//! a cost that grows with the drawer.
//!
//! Mechanism choices, each against its precedent:
//! - Heap: a counting [`GlobalAlloc`] over [`System`], the technique of dhat-rs's `HeapStats`
//!   (testing mode) and divan's `AllocProfiler`. dhat-rs itself is not used: its allocator takes a
//!   global mutex per allocation while profiling, allows one profiler per process and has no
//!   per-interval peak, and its per-site backtraces are not what a cost table reads. It remains
//!   the tool for attributing an allocation to a site, on demand. divan's profiler counts per
//!   thread, which misses the node's work: it runs on other tasks' threads. The counters here are
//!   process-wide and only move while a test turned them on, so other tests in this binary pay one
//!   relaxed load per allocation. Counts are only meaningful one test per process, which is how
//!   nextest runs.
//! - Heap is the Rust heap only. SQLite's page cache and other C allocations go to libc directly;
//!   resident set size (`VmRSS`) is reported beside it as the coarse whole-process figure.
//! - Wall time: neither criterion nor divan. Both time a closure repeated until the statistics
//!   settle, which assumes iterations are alike; these operations change the state they run on
//!   (a note makes the drawer bigger), and the drawer size is the variable of interest. The
//!   workload is a script instead, and each operation's samples are reported by position, as the
//!   scale test's tenths were. Both are also separate bench targets, which cannot reach this
//!   `cfg(test)` harness.

use super::blob_discovery::collection;
use super::blob_discovery::invite;
use super::harness::Cluster;
use super::harness::Topology;
use super::listeners::MAX_RESPONSE;
use super::listeners::post;
use super::listeners::tcp_client;
use super::*;
use big_cabinet_core::drawer::DrawerAccess as Access;
use service_utils_rs::Operation;
use service_utils_rs::XrpcClient;
use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;

#[global_allocator]
static HEAP: CountingAllocator = CountingAllocator;

/// Off until a test in this module turns it on, so the rest of the binary is not slowed by
/// contended counters.
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
/// Bytes allocated and not yet freed since counting started. Blocks allocated before it started
/// and freed after drive it below zero, so only differences of it mean anything.
static LIVE: AtomicI64 = AtomicI64::new(0);
/// The highest [`LIVE`] since [`measure`] last reset it to the then-current value.
static PEAK: AtomicI64 = AtomicI64::new(0);


// INSTR: count-weighted allocation-site sampler (BIG_NODE_COSTS_SAMPLE=<stride>): every stride-th allocation of any size
// records a backtrace, tagged by INSTR_PHASE (1 = early window, 2 = late window, 0 = off). A thread-local guard stops reentry.
static INSTR_PHASE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0); // INSTR
static INSTR_ACC: AtomicU64 = AtomicU64::new(0); // INSTR
static INSTR_STRIDE: AtomicU64 = AtomicU64::new(64); // INSTR
static INSTR_SAMPLES: std::sync::Mutex<Vec<(u8, [usize; 40])>> = std::sync::Mutex::new(Vec::new()); // INSTR
std::thread_local! { static INSTR_GUARD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; } // INSTR
unsafe extern "C" { // INSTR
    fn _Unwind_Backtrace(trace: extern "C" fn(*mut std::ffi::c_void, *mut std::ffi::c_void) -> i32, arg: *mut std::ffi::c_void) -> i32; // INSTR
    fn _Unwind_GetIP(context: *mut std::ffi::c_void) -> usize; // INSTR
} // INSTR
extern "C" fn instr_frame(context: *mut std::ffi::c_void, arg: *mut std::ffi::c_void) -> i32 { // INSTR
    // SAFETY: arg is the (array, depth) pair instr_sample passed; context is live for the callback. // INSTR
    let (ips, depth) = unsafe { &mut *(arg as *mut ([usize; 40], usize)) }; // INSTR
    if *depth == 40 { return 5; } // INSTR
    ips[*depth] = unsafe { _Unwind_GetIP(context) }; // INSTR
    *depth += 1; // INSTR
    0 // INSTR
} // INSTR
fn instr_sample(_size: usize) { // INSTR
    let phase = INSTR_PHASE.load(Relaxed); // INSTR
    if phase == 0 { return; } // INSTR
    if !INSTR_ACC.fetch_add(1, Relaxed).is_multiple_of(INSTR_STRIDE.load(Relaxed)) { return; } // INSTR
    if INSTR_GUARD.try_with(|guard| guard.replace(true)).unwrap_or(true) { return; } // INSTR
    // Raw libgcc unwind: no std backtrace lock, so a thread holding the ld.so loader lock cannot deadlock against us. // INSTR
    let mut frames = ([0usize; 40], 0usize); // INSTR
    unsafe { _Unwind_Backtrace(instr_frame, &mut frames as *mut _ as *mut std::ffi::c_void) }; // INSTR
    if let Ok(mut samples) = INSTR_SAMPLES.try_lock() { samples.push((phase, frames.0)); } // INSTR
    INSTR_GUARD.try_with(|guard| guard.set(false)).ok(); // INSTR (fails only during TLS teardown)
} // INSTR
fn instr_dump() { // INSTR
    INSTR_PHASE.store(0, Relaxed); // INSTR
    let samples = std::mem::take(&mut *INSTR_SAMPLES.lock().unwrap()); // INSTR
    let stride = INSTR_STRIDE.load(Relaxed); // INSTR
    eprintln!("INSTR alloc-site samples {} stride {stride} exe {:?}", samples.len(), std::env::current_exe().unwrap()); // INSTR
    for line in std::fs::read_to_string("/proc/self/maps").unwrap().lines().filter(|line| line.contains("big_node-")).take(1) { eprintln!("INSTR maps {line}"); } // INSTR
    for (phase, ips) in samples { // INSTR
        let ips: Vec<String> = ips.into_iter().take_while(|ip| *ip != 0).map(|ip| format!("{ip:x}")).collect(); // INSTR
        eprintln!("INSTR sample {phase} {}", ips.join(" ")); // INSTR
    } // INSTR
} // INSTR

/// [`System`], counting: a `realloc` is one allocation of its new size, as dhat-rs counts it.
struct CountingAllocator;

impl CountingAllocator {
    fn count(live_delta: i64, allocated: usize) {
        if !COUNTING.load(Relaxed) {
            return;
        }
        instr_sample(allocated); // INSTR
        ALLOCATIONS.fetch_add(1, Relaxed);
        ALLOCATED.fetch_add(allocated as u64, Relaxed);
        let live = LIVE.fetch_add(live_delta, Relaxed) + live_delta;
        PEAK.fetch_max(live, Relaxed);
    }
}

// SAFETY: every call is forwarded unchanged to `System`; the counting touches only atomics and
// never allocates.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() {
            Self::count(layout.size() as i64, layout.size());
        }
        block
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc_zeroed(layout) };
        if !block.is_null() {
            Self::count(layout.size() as i64, layout.size());
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        unsafe { System.dealloc(block, layout) };
        if COUNTING.load(Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, Relaxed);
        }
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(block, layout, new_size) };
        if !moved.is_null() {
            Self::count(new_size as i64 - layout.size() as i64, new_size);
        }
        moved
    }
}

/// What one operation cost. `peak` is the highest the heap rose above where it stood when the
/// operation started; the counts include whatever the node's background workers did meanwhile,
/// which for an operation that wakes them is part of its cost.
#[derive(Clone, Copy)]
struct Usage {
    wall: Duration,
    allocations: u64,
    allocated: u64,
    peak: u64,
}

/// The work is boxed: the script awaits many large node futures in one test body, which in a
/// debug build would otherwise all be laid out on the test thread's stack and overflow it.
async fn measure<T>(work: impl Future<Output = T>) -> (T, Usage) {
    let live = LIVE.load(Relaxed);
    PEAK.store(live, Relaxed);
    let allocations = ALLOCATIONS.load(Relaxed);
    let allocated = ALLOCATED.load(Relaxed);
    let start = Instant::now();
    let output = Box::pin(work).await;
    let usage = Usage {
        wall: start.elapsed(),
        allocations: ALLOCATIONS.load(Relaxed) - allocations,
        allocated: ALLOCATED.load(Relaxed) - allocated,
        peak: u64::try_from(PEAK.load(Relaxed) - live).unwrap_or(0),
    };
    (output, usage)
}

/// Resident set size of the whole process, in bytes.
fn resident() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .unwrap();
    let kib: u64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
    kib * 1024
}

/// Bytes under `path`, recursively.
fn disk(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                disk(&entry.path())
            } else {
                entry.metadata().unwrap().len()
            }
        })
        .sum()
}

/// A document's size three ways: its Automerge change count, its compacted `save()` and what the
/// repo stores for it (the encrypted sedimentree blobs a peer pulls).
async fn document(node: &BigNode, doc: &DocId) -> (usize, usize, usize) {
    let big_repo::DocLookup::Ready(handle) = node.repo.get_doc(doc).await.unwrap() else {
        panic!("{doc} is not materialized");
    };
    let (changes, saved) = handle
        .with_document_read(|doc| (doc.get_changes(&[]).len(), doc.save().len()))
        .await;
    let stored = node
        .repo
        .inspect_stored_doc_blobs(doc.clone())
        .await
        .unwrap()
        .iter()
        .map(Vec::len)
        .sum();
    (changes, saved, stored)
}

/// Samples per operation, in the order they were taken, and free-form facts, printed as
/// `COST`-prefixed lines a reader can grep out of the captured output.
#[derive(Default)]
struct Table {
    rows: Vec<(String, Vec<Usage>)>,
    facts: Vec<String>,
}

impl Table {
    fn record(&mut self, op: &str, usage: Usage) {
        match self.rows.iter_mut().find(|(name, _)| name == op) {
            Some((_, samples)) => samples.push(usage),
            None => self.rows.push((op.to_owned(), vec![usage])),
        }
    }

    async fn run<T>(&mut self, op: &str, work: impl Future<Output = T>) -> T {
        let (output, usage) = measure(work).await;
        self.record(op, usage);
        output
    }

    fn fact(&mut self, fact: String) {
        eprintln!("COST fact: {fact}");
        self.facts.push(fact);
    }

    fn print(&self) {
        fn median<T: Ord + Copy>(mut values: Vec<T>) -> T {
            values.sort_unstable();
            values[values.len() / 2]
        }
        let kib = |bytes: u64| bytes as f64 / 1024.0;
        eprintln!(
            "COST | op | n | wall p50 | wall max | allocations p50 | allocated p50 (KiB) | peak p50 (KiB) |"
        );
        eprintln!("COST |---|---|---|---|---|---|---|");
        for (op, samples) in &self.rows {
            let wall = median(samples.iter().map(|usage| usage.wall).collect());
            let max = samples.iter().map(|usage| usage.wall).max().unwrap();
            let allocations = median(samples.iter().map(|usage| usage.allocations).collect());
            let allocated = median(samples.iter().map(|usage| usage.allocated).collect());
            let peak = median(samples.iter().map(|usage| usage.peak).collect());
            eprintln!(
                "COST | {op} | {} | {wall:.2?} | {max:.2?} | {allocations} | {:.0} | {:.0} |",
                samples.len(),
                kib(allocated),
                kib(peak),
            );
        }
        for fact in &self.facts {
            eprintln!("COST fact: {fact}");
        }
    }
}

fn mib(bytes: i64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// How many notes the cost table writes and replicates. `BIG_NODE_COSTS_NOTES` overrides it; the
/// default keeps the debug run short (creation is linear per note today, see
/// [`a_note_costs_no_more_to_create_in_a_fuller_drawer`]).
fn notes() -> usize {
    std::env::var("BIG_NODE_COSTS_NOTES").map_or(10, |count| count.parse().unwrap())
}

fn text(ordinal: usize) -> String {
    format!("note {ordinal}")
}

/// The pinned quadratic (ledger N6): placing a document in a drawer writes a roster entry, and a
/// roster write costs more the more entries the drawer has, so creating notes is quadratic in the
/// drawer's size. The counted allocations of the 6th–10th and of the 26th–30th note into one
/// drawer must stay within a quarter of each other. Flat with noise passes; a per-note cost linear
/// in the drawer fails, since the measured slope (about 9k allocations per roster entry, two
/// roster writes per note, over about 170k for a note in an empty drawer) puts the ratio near 1.7.
/// The sizes are small because creating thirty notes takes about forty seconds in a debug build.
#[tokio::test(flavor = "multi_thread")]
async fn a_note_costs_no_more_to_create_in_a_fuller_drawer() {
    const NOTES: usize = 30;
    const WINDOW: usize = 5;
    let mut cluster = Cluster::boot(&["alice"]).await;
    let drawer = cluster.share_drawer(0, &[]).await;
    if let Ok(stride) = std::env::var("BIG_NODE_COSTS_SAMPLE") { INSTR_STRIDE.store(stride.parse().unwrap(), Relaxed); } // INSTR
    COUNTING.store(true, Relaxed);
    let mut created = Vec::new();
    // INSTR: process CPU (utime+stime, all threads, dead ones included) in clock ticks of 10 ms.
    let instr_cpu = || -> u64 { let stat = std::fs::read_to_string("/proc/self/stat").unwrap(); let fields: Vec<&str> = stat.rsplit_once(')').unwrap().1.split_whitespace().collect(); fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap() }; // INSTR
    *sqlite_rs::INSTR_SQL_CALLERS.lock().unwrap() = None; // INSTR
    for ordinal in 0..NOTES {
        let instr_cpu_before = instr_cpu(); // INSTR
        INSTR_PHASE.store(if std::env::var("BIG_NODE_COSTS_SAMPLE").is_err() { 0 } else if (5..10).contains(&ordinal) { 1 } else if (25..30).contains(&ordinal) { 2 } else { 0 }, Relaxed); // INSTR
        let instr_sql_before: Vec<u64> = sqlite_rs::INSTR_SQL.iter().map(|count| count.load(Relaxed)).collect(); // INSTR
        tracing::info!(ordinal, "INSTR create note begin"); // INSTR
        let (_, usage) = measure(cluster.create_note(0, &drawer, &text(ordinal))).await;
        let instr_sql: Vec<u64> = sqlite_rs::INSTR_SQL.iter().zip(&instr_sql_before).map(|(count, before)| count.load(Relaxed) - before).collect(); // INSTR
        eprintln!("INSTR create note {ordinal}: wall {:.1?} cpu {} ms allocations {} sql write_leases {} read_leases {} write_gate_wait {:.1}ms jobs {} job_busy {:.1}ms", usage.wall, (instr_cpu() - instr_cpu_before) * 10, usage.allocations, instr_sql[0], instr_sql[1], instr_sql[2] as f64 / 1e6, instr_sql[3], instr_sql[4] as f64 / 1e6); // INSTR
        created.push(usage);
    }
    { let mut instr_callers: Vec<(String, u64)> = sqlite_rs::INSTR_SQL_CALLERS.lock().unwrap().take().unwrap_or_default().into_iter().collect(); instr_callers.sort_by_key(|(_, count)| std::cmp::Reverse(*count)); for (caller, count) in instr_callers.iter().take(40) { eprintln!("INSTR sql caller x16 {count}: {caller}"); } } // INSTR
    INSTR_PHASE.store(0, Relaxed); // INSTR
    if std::env::var("BIG_NODE_COSTS_SAMPLE").is_ok() { instr_dump(); } // INSTR
    COUNTING.store(false, Relaxed);
    let window = |end: usize| {
        let mut window: Vec<_> = created[end - WINDOW..end]
            .iter()
            .map(|usage| (usage.allocations, usage.wall))
            .collect();
        window.sort_unstable();
        window[WINDOW / 2]
    };
    let (small, small_wall) = window(2 * WINDOW);
    let (large, large_wall) = window(NOTES);
    eprintln!(
        "COST create note: {small} allocations ({small_wall:.1?}) at drawer size {}, {large} \
         ({large_wall:.1?}) at drawer size {}",
        2 * WINDOW,
        NOTES,
    );
    cluster.stop_all().await;
    assert!(
        large * 4 <= small * 5,
        "creating a note allocates {large} times at drawer size {NOTES} against {small} at size \
         {}: its cost grows with the drawer",
        2 * WINDOW,
    );
}

/// Every operation the node offers a user, run in order on two nodes, then dispatch over each
/// path on a third. See the module docs for how to read and how to scale it.
#[tokio::test(flavor = "multi_thread")]
async fn long_test_cost_table() {
    let count = notes();
    let mut table = Table::default();
    COUNTING.store(true, Relaxed);

    let (live, rss) = (LIVE.load(Relaxed), resident());
    let mut cluster = table
        .run(
            "boot two fresh durable nodes",
            Cluster::boot(&["alice", "bob"]),
        )
        .await;
    table.fact(format!(
        "fresh node: {} Rust heap, {} resident, each (two booted, idle)",
        mib((LIVE.load(Relaxed) - live) / 2),
        mib((resident() - rss) as i64 / 2),
    ));

    for ordinal in 0..2 {
        let drawer = table
            .run(
                "create drawer",
                collection(cluster.node(0), &format!("costs-{ordinal}")),
            )
            .await;
        table
            .run(
                "invite a peer to a drawer",
                invite(cluster.node(0), cluster.node(1), &drawer, Access::Write),
            )
            .await;
    }
    let drawer = table
        .run(
            "share drawer (create, blob inventory, invite, both remember)",
            cluster.share_drawer(0, &[1]),
        )
        .await;

    let home = disk(cluster.home(0));
    let tenth = (count / 10).max(1);
    let mut notes = Vec::with_capacity(count);
    for ordinal in 0..count {
        let (note, usage) = measure(cluster.create_note(0, &drawer, &text(ordinal))).await;
        table.record("create note", usage);
        if ordinal < tenth {
            table.record("create note, first tenth of the drawer", usage);
        }
        if ordinal >= count - tenth {
            table.record("create note, last tenth of the drawer", usage);
        }
        notes.push(note);
    }
    table.fact(format!(
        "node directory grew {} bytes per note",
        (disk(cluster.home(0)) - home) / count as u64,
    ));
    let (changes, saved, stored) = document(cluster.node(0), &notes[0]).await;
    table.fact(format!(
        "a note: {changes} Automerge changes, {saved} bytes saved, {stored} bytes stored"
    ));
    let (changes, saved, stored) = document(cluster.node(0), &drawer.id).await;
    table.fact(format!(
        "the drawer after {count} notes: {changes} changes, {saved} bytes saved ({} per note), \
         {stored} bytes stored",
        saved / count
    ));

    for note in notes.iter().take(10) {
        table
            .run(
                "write note (one datum)",
                cluster.update_note(0, note, "main", "rewritten"),
            )
            .await;
        table
            .run("read note", async {
                cluster.read_note(0, note).await.unwrap()
            })
            .await;
    }

    let bytes = vec![7_u8; 64 * 1024];
    let mut blobs = Vec::new();
    for _ in 0..3 {
        blobs.push(
            table
                .run(
                    "put 64 KiB blob and publish it in the drawer",
                    cluster.add_blob(0, &drawer, &bytes),
                )
                .await,
        );
    }
    for blob in &blobs {
        let read = table
            .run("read 64 KiB blob", cluster.read_blob(0, *blob))
            .await;
        assert_eq!(read.as_deref(), Some(bytes.as_slice()));
    }

    let (live, rss) = (LIVE.load(Relaxed), resident());
    tracing::info!("INSTR phase connect begin"); // INSTR
    table
        .run("connect two nodes", cluster.connect(Topology::Pair))
        .await;
    tracing::info!("INSTR phase connect end"); // INSTR
    // Read in order from the first note Bob could not yet read, so each wake re-reads only what is
    // still outstanding; each tenth is a sample, so a rate that degrades with the count shows.
    let next = std::cell::Cell::new(0);
    let lap = std::cell::Cell::new(Duration::ZERO);
    let tenths = std::cell::RefCell::new(Vec::new());
    let start = Instant::now();
    let (_, usage) = measure(cluster.until(async |cluster| {
        while next.get() < count {
            let ordinal = next.get();
            let Some(note) = cluster.read_note(1, &notes[ordinal]).await else {
                return false;
            };
            let expected = if ordinal < 10 {
                "rewritten".to_owned()
            } else {
                text(ordinal)
            };
            assert_eq!(note.text("main"), Some(expected));
            next.set(ordinal + 1);
            if (ordinal + 1).is_multiple_of(tenth) {
                let elapsed = start.elapsed();
                tenths.borrow_mut().push(elapsed - lap.replace(elapsed));
            }
        }
        true
    }))
    .await;
    table.record(&format!("replicate {count} notes after connecting"), usage);
    table.fact(format!(
        "replication, each successive tenth ({tenth} notes): {:.2?}",
        tenths.into_inner()
    ));
    table.fact(format!(
        "after replicating: Rust heap {} and resident {} above before connecting",
        mib(LIVE.load(Relaxed) - live),
        mib(resident() as i64 - rss as i64),
    ));
    let received = table
        .run(
            "replicate a 64 KiB blob (mirror the inventory)",
            cluster.blob_arrives(1, &drawer, blobs[0]),
        )
        .await;
    assert_eq!(received, bytes);

    tracing::info!("INSTR phase replicate end"); // INSTR
    eprintln!("INSTR dump for connect+replicate"); // INSTR
    table.run("stop node", cluster.stop(1)).await;
    tracing::info!("INSTR phase reboot begin"); // INSTR
    table
        .run(
            &format!("reboot node holding {count} notes"),
            cluster.restart(1),
        )
        .await;
    tracing::info!("INSTR phase reboot end"); // INSTR
    cluster.stop_all().await;

    // D19: many nodes per process. What each one adds, booted side by side.
    const MANY: usize = 3;
    let (live, rss) = (LIVE.load(Relaxed), resident());
    let mut many = table
        .run(
            "boot three fresh durable nodes in one process",
            Cluster::boot(&["n0", "n1", "n2"]),
        )
        .await;
    table.fact(format!(
        "{MANY} nodes in one process: {} Rust heap, {} resident, per node",
        mib((LIVE.load(Relaxed) - live) / MANY as i64),
        mib((resident() as i64 - rss as i64) / MANY as i64),
    ));
    many.stop_all().await;

    dispatch_costs(&mut table).await;
    COUNTING.store(false, Relaxed);
    table.print();
}

/// One `readDatums` per path through the node's router: the in-process dispatch client, the Unix
/// socket and TCP. The difference between the rows is the transport; the operation is the same.
async fn dispatch_costs(table: &mut Table) {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let mut config = config(
        secrets().await,
        runtime.ephemeral(&[]).unwrap(),
        big_repo::Storage::memory(&runtime).unwrap(),
    );
    config.listeners = Listeners {
        unix_socket: Some(socket.clone()),
        ..super::listeners::tcp()
    };
    let (node, stop) = BigNode::boot(config).await.unwrap();
    let note = create(
        &node,
        "costs-dispatch",
        json!({"$type": NOTE, "value": "x"}),
    )
    .await;
    let input = serde_json::to_value(ReadDatums {
        capability: capability(
            &node,
            DocScope {
                read: BTreeSet::from([note.doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await,
        doc_id: note.doc_id.clone(),
        at: At::Current,
        datums: None,
        optimistic: false,
    })
    .unwrap();
    let operator = Authorization::identity(node.authority.root);
    let unix = XrpcClient::unix(&socket, MAX_RESPONSE).unwrap();
    let tcp = tcp_client(&node);
    for _ in 0..20 {
        table
            .run("dispatch readDatums in-process", async {
                node.dispatch
                    .client
                    .call_with(
                        RequestContext::root(),
                        operator.metadata(),
                        big_dispatch_core::Invoke {
                            operation: ReadDatums::XRPC.nsid.to_owned(),
                            input: input.clone(),
                            context: Default::default(),
                        },
                    )
                    .await
                    .unwrap()
            })
            .await;
        for (op, client, authorization) in [
            ("dispatch readDatums over UDS", &unix, None),
            ("dispatch readDatums over TCP", &tcp, Some(operator)),
        ] {
            let (status, body) = table
                .run(
                    op,
                    post(client, ReadDatums::XRPC.nsid, authorization, input.clone()),
                )
                .await;
            assert_eq!(status, 200, "{body}");
        }
    }
    shutdown(node, stop).await;
}
