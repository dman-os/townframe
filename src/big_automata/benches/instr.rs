//! Instruction-count benches (Gungraun, over callgrind): the same operation at `n` and `10n`
//! per layer. Instructions are deterministic on one machine and toolchain, so CI gates on them
//! (`x/bench.ts gate`), and a quadratic path shows as a tenfold step becoming a hundredfold.
//! Setup runs outside the measured region. Run: `x/bench.ts run big_automata:instr`.

// Gungraun needs valgrind, which a browser does not have: on wasm the target is empty but builds.
#![cfg_attr(target_family = "wasm", no_main)]
#![cfg(not(target_family = "wasm"))]

mod workloads;

use std::hint::black_box;

use big_automata::durable_outbox::Outbox;
use big_automata::okv::memory::MemoryDb;
use big_automata::part_store::memory::MemoryPartStore;
use big_automata::replay::server::ReplayServer;
use gungraun::library_benchmark;
use gungraun::library_benchmark_group;
use gungraun::main;
use workloads::Table;

#[library_benchmark]
#[bench::n1k(args = (1_000), setup = workloads::empty_table)]
#[bench::n10k(args = (10_000), setup = workloads::empty_table)]
fn okv_write(state: (usize, (MemoryDb, Table))) -> usize {
    black_box(workloads::okv_write(state))
}

#[library_benchmark]
#[bench::n1k(args = (1_000), setup = workloads::filled_table)]
#[bench::n10k(args = (10_000), setup = workloads::filled_table)]
fn okv_page_the_log(state: (MemoryDb, Table)) -> u64 {
    black_box(workloads::okv_page_the_log(state))
}

#[library_benchmark]
#[bench::n1k(args = (1_000), setup = workloads::filled_table)]
#[bench::n10k(args = (10_000), setup = workloads::filled_table)]
fn rev_stream_replay(state: (MemoryDb, Table)) -> u64 {
    black_box(workloads::rev_stream_replay(state))
}

#[library_benchmark]
#[bench::n1k(args = (1_000), setup = workloads::filled_part)]
#[bench::n10k(args = (10_000), setup = workloads::filled_part)]
fn part_store_page_the_log(store: MemoryPartStore) -> u64 {
    black_box(workloads::part_store_page_the_log(store))
}

#[library_benchmark]
#[bench::n1638(args = (1_638), setup = workloads::opened)]
#[bench::n16380(args = (16_380), setup = workloads::opened)]
fn replay_replace_every_target(state: (usize, ReplayServer)) -> u64 {
    black_box(workloads::replay_replace_every_target(state))
}

#[library_benchmark]
#[bench::n1k(args = (1_000), setup = workloads::wants)]
#[bench::n10k(args = (10_000), setup = workloads::wants)]
fn outbox_machine_fetches_every_want(state: (usize, Outbox)) -> usize {
    black_box(workloads::outbox_machine_fetches_every_want(state))
}

library_benchmark_group!(
    name = layers;
    benchmarks =
        okv_write,
        okv_page_the_log,
        rev_stream_replay,
        part_store_page_the_log,
        replay_replace_every_target,
        outbox_machine_fetches_every_want
);

main!(library_benchmark_groups = layers);
