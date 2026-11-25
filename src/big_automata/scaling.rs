//! Scaling checks: each runs an operation at `n` and `10n` and asserts that what it
//! allocated grew about tenfold, not a hundredfold. They exist to catch an accidental quadratic
//! path (the replay server's reuse check once was one) and gate CI on **counted allocations**,
//! which are the same on every machine, never on wall time.
//!
//! Allocation counts see only paths that allocate. A quadratic loop that allocates nothing is
//! caught by the instruction-count benches in `benches/instr.rs` (see the benchmarking skill),
//! which the CI gate compares across commits.
//!
//! The counting allocator is process-wide for this test binary but counts only the thread
//! inside a [`measure`](utils_rs::testing::alloc_count::measure), so tests may share a process.

use utils_rs::testing::alloc_count::Counted;
use utils_rs::testing::alloc_count::CountingAllocator;

mod okv;
mod part_store;
mod replay;
mod rev;
mod scheduler;

#[global_allocator]
static HEAP: CountingAllocator = CountingAllocator;

/// The growth between the two sizes.
const GROWTH: usize = 10;
/// A linear path grows `GROWTH` times and a quadratic one `GROWTH²`. The slack above linear
/// covers a tree getting a level deeper and a vector's doubling, not a second pass per row.
const MAX_ALLOCATIONS_RATIO: f64 = 14.0;
/// Bytes get more slack: one hash table or vector growing past a power of two jumps up to
/// sixteenfold in a single allocation, which is a step and not a trend.
const MAX_BYTES_RATIO: f64 = 25.0;

/// Run `measured` at `small` and at ten times it, and assert neither allocations nor bytes
/// grew by more than [`MAX_ALLOCATIONS_RATIO`] and [`MAX_BYTES_RATIO`]. `measured` builds its own input and returns only what the
/// operation under test allocated (setup is outside the [`measure`](utils_rs::testing::alloc_count::measure)).
pub(crate) fn assert_linear(what: &str, small: usize, mut measured: impl FnMut(usize) -> Counted) {
    let low = measured(small);
    let high = measured(small * GROWTH);
    for (n, counted) in [(small, low), (small * GROWTH, high)] {
        let bench = format!("big_automata::scaling::{}", what.replace(' ', "_"));
        let params = [("n", n.to_string())];
        println!(
            "{}\n{}",
            crate::bench::record_line(
                &bench,
                "allocations",
                "count",
                counted.allocations as f64,
                &params
            ),
            crate::bench::record_line(
                &bench,
                "alloc_bytes",
                "bytes",
                counted.bytes as f64,
                &params
            ),
        );
    }
    let ratio = |low: u64, high: u64| high as f64 / low.max(1) as f64;
    let allocations = ratio(low.allocations, high.allocations);
    let bytes = ratio(low.bytes, high.bytes);
    println!(
        "SCALING {what}: n={small} allocations {} bytes {} -> n={} allocations {} bytes {} (x{allocations:.2} allocations, x{bytes:.2} bytes)",
        low.allocations,
        low.bytes,
        small * GROWTH,
        high.allocations,
        high.bytes,
    );
    assert!(
        allocations <= MAX_ALLOCATIONS_RATIO && bytes <= MAX_BYTES_RATIO,
        "{what} grew superlinearly: x{allocations:.2} allocations and x{bytes:.2} bytes for \
         x{GROWTH} the input (limits x{MAX_ALLOCATIONS_RATIO} and x{MAX_BYTES_RATIO})"
    );
}
