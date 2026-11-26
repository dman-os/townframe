//! Instruction-count benches (Gungraun, over callgrind): the same operation at `n` and `10n`.
//! Instructions are deterministic on one machine and toolchain, so a quadratic path shows as a
//! tenfold step becoming a hundredfold. Setup runs outside the measured region, but under
//! valgrind, so sizes stay modest. Run: `x/bench.ts run keyline_sqlite:instr`.

// Gungraun needs valgrind, which a browser does not have: on wasm the target is empty but builds.
#![cfg_attr(target_family = "wasm", no_main)]
#![cfg(not(target_family = "wasm"))]

use std::hint::black_box;

use gungraun::library_benchmark;
use gungraun::library_benchmark_group;
use gungraun::main;
use keyline_sqlite::workloads;
use keyline_sqlite::workloads::Fixture;
use keyline_sqlite::workloads::Store;
use sqlite_rs::Database;

#[library_benchmark]
#[bench::n200(args = (200), setup = workloads::to_insert)]
#[bench::n2k(args = (2_000), setup = workloads::to_insert)]
fn insert_batch(state: (Store, Vec<keyline::certificate::VerifiedCertificate<()>>)) -> u64 {
    black_box(workloads::insert(state))
}

#[library_benchmark]
#[bench::n200(args = (200), setup = workloads::inserted)]
#[bench::n2k(args = (2_000), setup = workloads::inserted)]
fn members_after_insert(state: (Store, Fixture)) -> usize {
    black_box(workloads::members_after_insert(state))
}

#[library_benchmark]
#[bench::n200(args = (200), setup = workloads::inserted)]
#[bench::n2k(args = (2_000), setup = workloads::inserted)]
fn point_read(state: (Store, Fixture)) -> usize {
    black_box(workloads::point_read(state))
}

#[library_benchmark]
#[bench::n100(args = (100), setup = workloads::stored)]
#[bench::n1k(args = (1_000), setup = workloads::stored)]
fn cold_members(state: (Database, keyhive_crypto::identifier::Identifier)) -> usize {
    black_box(workloads::cold_members(state))
}

library_benchmark_group!(
    name = authority;
    benchmarks = insert_batch, members_after_insert, point_read, cold_members
);

main!(library_benchmark_groups = authority);
