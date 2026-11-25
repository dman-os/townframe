//! Whole sync on the simulated clock: two in-memory nodes, N objects, the real driver. The
//! virtual time, the rounds and the wire bytes are deterministic, so they are reported as
//! records and a few are asserted; the real wall time is reported beside them for exploration
//! only.
//!
//! `BIG_AUTOMATA_BENCH_SIZES=1000,10000,100000` sets the sizes (default: one small size, so
//! the test stays quick in debug builds). Run with `--no-capture` to see the table.

use super::rig::clock;
use crate::bench::SyncReport;
use crate::bench::sync_once;
use crate::interlude::*;

fn sizes() -> Vec<usize> {
    std::env::var("BIG_AUTOMATA_BENCH_SIZES")
        .map(|sizes| {
            sizes
                .split(',')
                .map(|size| size.trim().parse().expect("a size is a number"))
                .collect()
        })
        .unwrap_or_else(|_| vec![1_000])
}

fn print_table(reports: &[SyncReport]) {
    println!(
        "{:>8} {:>8} {:>12} {:>10} {:>8} {:>8} {:>12} {:>12} {:>10}",
        "objects",
        "rtt_ms",
        "sim_time_s",
        "wall_ms",
        "calls",
        "rounds",
        "bytes_up",
        "bytes_down",
        "B/object"
    );
    for report in reports {
        println!(
            "{:>8} {:>8} {:>12.3} {:>10.1} {:>8} {:>8} {:>12} {:>12} {:>10.1}",
            report.objects,
            report.rtt.as_millis(),
            report.elapsed.as_secs_f64(),
            report.wall.as_secs_f64() * 1e3,
            report.calls,
            report.rounds,
            report.bytes_up,
            report.bytes_down,
            (report.bytes_up + report.bytes_down) as f64 / report.objects as f64,
        );
    }
}

#[tokio::test(start_paused = true)]
async fn whole_sync_costs_by_size_and_round_trip() {
    let mut reports = vec![];
    for objects in sizes() {
        for rtt in [Duration::ZERO, Duration::from_millis(50)] {
            let report = sync_once(objects, rtt, |_| {}, clock()).await;
            for line in report.record_lines("big_automata::whole_sync") {
                println!("{line}");
            }
            reports.push(report);
        }
    }
    print_table(&reports);
    for pair in reports.chunks(2) {
        let [fast, slow] = pair else { unreachable!() };
        assert_eq!(
            (fast.calls, fast.rounds, fast.bytes_up, fast.bytes_down),
            (slow.calls, slow.rounds, slow.bytes_up, slow.bytes_down),
            "the round trip changes time, not the exchange"
        );
        assert!(
            slow.elapsed > fast.elapsed,
            "a round trip per call must show on the simulated clock"
        );
    }
}

/// A part of more than three leaf pages (1024 entries each) syncs: its second page's
/// `next_after` sorts below the first's by key, and a walk that judged progress by key failed
/// the page for ever (found at 3500 objects; 3000 fit in two pages and never met it). Past
/// 16384 objects the walk leafs one level down, and 30k objects spread over many buckets.
#[tokio::test(start_paused = true)]
async fn whole_sync_of_a_part_beyond_three_pages_completes() {
    for objects in [3_500, 4_000, 10_000, 30_000] {
        let report = n0_future::time::timeout(
            Duration::from_secs(3600),
            sync_once(objects, Duration::ZERO, |_| {}, clock()),
        )
        .await
        .unwrap_or_else(|_| panic!("{objects} objects sync within an hour of virtual time"));
        assert_eq!(report.objects, objects);
    }
}
