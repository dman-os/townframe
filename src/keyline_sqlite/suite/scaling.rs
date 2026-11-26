//! Scaling checks: each runs an operation at `n` and `10n` and asserts that what it allocated
//! grew about tenfold (or not at all, for a point read), not a hundredfold. They catch an
//! accidental quadratic path and gate on **counted allocations**, which are the same on every
//! machine. Counting is per thread: it sees this crate's work (encoding rows, decoding and
//! verifying them, the evaluation), not what SQLite's worker thread allocates.
//!
//! Allocation counts miss a loop that allocates nothing: the instruction bench
//! (`benches/instr.rs`) covers those. Both run the workloads in `workloads.rs`.

use crate::workloads;
use utils_rs::testing::alloc_count::Counted;
use utils_rs::testing::alloc_count::CountingAllocator;
use utils_rs::testing::alloc_count::measure;

#[global_allocator]
static HEAP: CountingAllocator = CountingAllocator;

const GROWTH: usize = 10;
/// A linear path grows `GROWTH` times and a quadratic one `GROWTH` squared; the slack covers a
/// vector doubling or a hash table a size up, not a second pass per row.
const MAX_ALLOCATIONS_RATIO: f64 = 14.0;
const MAX_BYTES_RATIO: f64 = 25.0;

fn record_line(bench: &str, metric: &str, unit: &str, value: f64, n: usize) -> String {
    format!(
        "BENCH_RECORD {}",
        serde_json::json!({
            "bench": bench,
            "metric": metric,
            "unit": unit,
            "value": value,
            "params": { "n": n.to_string() },
        })
    )
}

/// Run `measured` at `small` and `10 * small`, print the records, and return the two tallies.
fn run(what: &str, small: usize, measured: impl Fn(usize) -> Counted) -> (Counted, Counted) {
    // The first run pays one-time initialisation (statics, the runtime's first connection), which
    // is not the operation's cost and would swamp the smaller size.
    measured(small);
    let low = measured(small);
    let high = measured(small * GROWTH);
    for (n, counted) in [(small, low), (small * GROWTH, high)] {
        let bench = format!("keyline_sqlite::scaling::{what}");
        println!(
            "{}\n{}",
            record_line(
                &bench,
                "allocations",
                "count",
                counted.allocations as f64,
                n
            ),
            record_line(&bench, "alloc_bytes", "bytes", counted.bytes as f64, n),
        );
    }
    println!(
        "SCALING {what}: n={small} allocations {} bytes {} -> n={} allocations {} bytes {}",
        low.allocations,
        low.bytes,
        small * GROWTH,
        high.allocations,
        high.bytes
    );
    (low, high)
}

fn ratio(low: u64, high: u64) -> f64 {
    high as f64 / low.max(1) as f64
}

fn assert_linear(what: &str, small: usize, measured: impl Fn(usize) -> Counted) {
    let (low, high) = run(what, small, measured);
    let (allocations, bytes) = (
        ratio(low.allocations, high.allocations),
        ratio(low.bytes, high.bytes),
    );
    assert!(
        allocations <= MAX_ALLOCATIONS_RATIO && bytes <= MAX_BYTES_RATIO,
        "{what} grew superlinearly: x{allocations:.2} allocations and x{bytes:.2} bytes for x{GROWTH} the input"
    );
}

#[test]
fn insert_is_linear_in_the_batch() {
    assert_linear("insert", 300, |n| {
        let state = workloads::to_insert(n);
        measure(|| workloads::insert(state)).1
    });
}

/// The main query: the first `members` after an insert, which evaluates both strata.
#[test]
fn members_after_insert_is_linear_in_the_set() {
    assert_linear("members_after_insert", 300, |n| {
        let state = workloads::inserted(n);
        measure(|| workloads::members_after_insert(state)).1
    });
}

/// Attaching to a database and asking: every row decoded and verified, then the evaluation.
#[test]
fn cold_members_is_linear_in_the_set() {
    assert_linear("cold_members", 100, |n| {
        let state = workloads::stored(n);
        measure(|| workloads::cold_members(state)).1
    });
}

/// An indexed point read costs the same whatever the set holds.
#[test]
fn a_point_read_does_not_grow_with_the_set() {
    let (low, high) = run("point_read", 300, |n| {
        let state = workloads::inserted(n);
        measure(|| workloads::point_read(state)).1
    });
    // The answer is one row at both sizes, so the allocations are the same to within noise.
    let grew = ratio(low.allocations, high.allocations);
    assert!(
        grew <= 1.5,
        "a point read grew x{grew:.2} for x{GROWTH} the set"
    );
}
