//! The simulation's cases: the proptest, determinism, and the bugs planted on purpose.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::Config as RunnerConfig;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;

use super::ops::op;
use super::ops::run;
use super::*;

/// `count` ops lists from a fixed seed, the same on every run.
fn cases(seed: u8, count: usize, longest: usize) -> Vec<Vec<Op>> {
    let mut runner = TestRunner::new_with_rng(
        RunnerConfig::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &[seed; 32]),
    );
    let strategy = proptest::collection::vec(op(), 40..longest);
    (0..count)
        .map(|_| strategy.new_tree(&mut runner).expect("a case").current())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]

    #[test]
    fn the_machines_agree_with_the_world_through_faults_and_disorder(
        ops in proptest::collection::vec(op(), 0..200),
        seed in any::<u64>(),
    ) {
        run(ops, seed, None);
    }
}

/// One fixed case, run with its place named, so a failure can be replayed.
fn run_named(seed: u8, at: usize, ops: Vec<Op>, plant: Option<Plant>) {
    let count = ops.len();
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| run(ops, u64::from(seed), plant))) {
        let message = panic_message(payload.as_ref());
        panic!("seed {seed} case {at} ({count} ops): {message}");
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_default()
}

/// What the simulation observes after every op, then after the drain.
fn trace(ops: &[Op], seed: u64) -> Vec<String> {
    let mut sim = Sim::new(seed, None);
    let mut out = vec![];
    for op in ops {
        sim.step(op.clone());
        out.push(format!("{op:?} => {}", sim.snapshot()));
    }
    sim.quiesce();
    out.push(format!("quiesced => {}", sim.snapshot()));
    out
}

/// Law zero: a seed replays. Without it a failure the simulation finds cannot be reproduced.
#[test]
fn the_same_seed_replays_the_same_trace() {
    for seed in 0..16u8 {
        for (at, ops) in cases(seed, 8, 200).iter().enumerate() {
            let first = trace(ops, u64::from(seed));
            let second = trace(ops, u64::from(seed));
            if let Some(step) = first.iter().zip(&second).position(|(a, b)| a != b) {
                panic!(
                    "seed {seed} case {at} diverged at step {step}\n first: {}\nsecond: {}",
                    first[step], second[step]
                );
            }
            assert_eq!(first.len(), second.len(), "seed {seed} case {at}");
        }
    }
}

/// Runs the fixed cases with `plant` until one fails, and answers its message.
fn first_failure(plant: Plant) -> Option<String> {
    for seed in 0..16u8 {
        for (at, ops) in cases(seed, 24, 200).into_iter().enumerate() {
            let result = catch_unwind(AssertUnwindSafe(|| run(ops, u64::from(seed), Some(plant))));
            if let Err(payload) = result {
                return Some(format!(
                    "seed {seed} case {at}: {}",
                    panic_message(payload.as_ref())
                ));
            }
        }
    }
    None
}

/// The fixed cases pass on the real machine, so a failure below is the planted bug's.
/// `SIM_CASE=seed:case` runs one of them alone (with `SIM_TRACE=1` to see its commands).
#[test]
fn the_fixed_cases_pass_without_a_planted_bug() {
    let only = std::env::var("SIM_CASE").ok().map(|case| {
        let (seed, at) = case.split_once(':').expect("SIM_CASE=seed:case");
        (
            seed.parse::<u8>().expect("a seed"),
            at.parse::<usize>().expect("a case"),
        )
    });
    for seed in 0..16u8 {
        for (at, ops) in cases(seed, 24, 200).into_iter().enumerate() {
            if only.is_none_or(|only| only == (seed, at)) {
                run_named(seed, at, ops, None);
            }
        }
    }
}

fn caught(plant: Plant, law: &str) {
    let message = first_failure(plant).unwrap_or_else(|| panic!("{plant:?} was not caught"));
    assert!(
        message.contains(law),
        "{plant:?} was caught, but not by {law}: {message}"
    );
}

#[test]
fn planted_read_served_while_a_write_is_in_flight_is_caught() {
    caught(Plant::ServeReadsDuringWrite, "law 1");
}

#[test]
fn planted_lifo_queue_is_caught() {
    caught(Plant::LifoQueue, "submission order");
}

#[test]
fn planted_frontier_ahead_of_applied_is_caught() {
    caught(Plant::FrontierAhead, "law 5");
}

#[test]
fn planted_plan_that_does_not_hold_is_caught() {
    caught(Plant::PlanDoesNotHold, "law 4");
}

#[test]
fn planted_hydration_of_an_unreadable_document_is_caught() {
    caught(Plant::HydrateUnreadable, "law 8");
}

#[test]
fn planted_publication_during_a_write_is_caught() {
    caught(Plant::PublishDuringWrite, "law 1");
}

#[test]
fn planted_write_while_publishing_is_caught() {
    caught(Plant::WriteWhilePublishing, "frontier");
}

#[test]
fn planted_notification_during_a_write_is_caught() {
    caught(Plant::NotifyDuringWrite, "law 1");
}

#[test]
fn planted_frontier_row_by_the_current_revision_is_caught() {
    caught(Plant::FrontierByCurrentRev, "law 5");
}

/// The compare-and-set on a frontier row is needed: ordering the writes keeps rows in order,
/// but a store without it writes a row that claims remote content the machine never applied.
#[test]
fn without_the_compare_and_set_a_frontier_row_claims_unapplied_content() {
    let mut caught = None;
    'cases: for seed in 0..16u8 {
        for (at, ops) in cases(seed, 24, 200).into_iter().enumerate() {
            let result = catch_unwind(AssertUnwindSafe(|| {
                let mut sim = Sim::new(u64::from(seed), None);
                sim.world.cas = false;
                for op in ops {
                    sim.step(op);
                }
                sim.quiesce();
            }));
            if let Err(payload) = result {
                caught = Some((seed, at, panic_message(payload.as_ref())));
                break 'cases;
            }
        }
    }
    let (seed, at, message) = caught.expect("a store without the compare-and-set was not caught");
    assert!(
        message.contains("frontier"),
        "seed {seed} case {at} failed, but not on the frontier: {message}"
    );
}

#[test]
fn planted_reuse_of_the_actor_after_a_failed_commit_is_caught() {
    caught(Plant::ReuseActor, "SeqNumber");
}

#[test]
fn planted_apply_on_stale_is_caught() {
    caught(Plant::ApplyOnStale, "law");
}

#[test]
fn planted_trust_in_the_landed_revision_is_caught() {
    caught(Plant::TrustLandedRev, "frontier");
}
