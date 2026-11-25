//! Wall-clock benches (divan) of the same workloads as `instr`, for exploration: how long
//! things take on the machine in front of you, in release. Never gated, on any machine: shared
//! runners are too noisy and a laptop is busy. Run: `x/bench.ts run big_automata:wall --release`.
//!
//! divan over criterion: far fewer dependencies (no plotting, no rayon), and attribute-based
//! benches that read like the workloads they time.

// divan times with the OS clock; on wasm the target is empty but builds (the browser run is
// `x/bench.ts run big_automata_web:web`).
#![cfg_attr(target_family = "wasm", no_main)]
#![cfg(not(target_family = "wasm"))]

mod workloads;

use std::sync::Arc;

use big_automata::bench::sync_once;
use divan::Bencher;

fn main() {
    divan::main();
}

#[divan::bench(args = [10_000, 100_000])]
fn okv_write(bencher: Bencher, rows: usize) {
    bencher
        .with_inputs(|| workloads::empty_table(rows))
        .bench_values(workloads::okv_write);
}

#[divan::bench(args = [10_000, 100_000])]
fn okv_page_the_log(bencher: Bencher, rows: usize) {
    bencher
        .with_inputs(|| workloads::filled_table(rows))
        .bench_values(workloads::okv_page_the_log);
}

#[divan::bench(args = [10_000, 100_000])]
fn rev_stream_replay(bencher: Bencher, rows: usize) {
    bencher
        .with_inputs(|| workloads::filled_table(rows))
        .bench_values(workloads::rev_stream_replay);
}

#[divan::bench(args = [10_000, 100_000])]
fn part_store_page_the_log(bencher: Bencher, members: usize) {
    bencher
        .with_inputs(|| workloads::filled_part(members))
        .bench_values(workloads::part_store_page_the_log);
}

#[divan::bench(args = [1_638, 16_380])]
fn replay_replace_every_target(bencher: Bencher, targets: usize) {
    bencher
        .with_inputs(|| workloads::opened(targets))
        .bench_values(workloads::replay_replace_every_target);
}

#[divan::bench(args = [1_000, 10_000])]
fn outbox_machine_fetches_every_want(bencher: Bencher, rows: usize) {
    bencher
        .with_inputs(|| workloads::wants(rows))
        .bench_values(workloads::outbox_machine_fetches_every_want);
}

/// Two in-memory nodes, N objects, the real driver; the clock is the paused one, so only the
/// work is timed.
#[divan::bench(args = [1_000, 10_000])]
fn whole_sync(bencher: Bencher, objects: usize) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime");
    bencher.bench_local(|| {
        runtime.block_on(sync_once(
            objects,
            std::time::Duration::ZERO,
            |_| {},
            Arc::new(|| tokio::time::Instant::now().into_std()),
        ))
    });
}
