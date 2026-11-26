//! The simulation's cases: the proptest, determinism, and the bugs planted on purpose.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use proptest::strategy::ValueTree;
use proptest::test_runner::Config as RunnerConfig;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;

use super::super::Plant;
use super::*;

/// `count` op lists from a fixed seed, the same on every run.
fn cases(seed: u8, count: usize) -> Vec<Vec<Op>> {
    let mut runner = TestRunner::new_with_rng(
        RunnerConfig::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &[seed; 32]),
    );
    let strategy = proptest::collection::vec(op(), 40..160);
    (0..count)
        .map(|_| strategy.new_tree(&mut runner).expect("a case").current())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn the_machines_agree_with_the_world_through_faults_and_clock_jumps(
        ops in proptest::collection::vec(op(), 0..200),
    ) {
        run(&ops, None);
    }
}

/// A fixed seed replays: the same cases give the same outcome.
#[test]
fn a_seed_replays() {
    let trace = |ops: &[Op]| {
        let mut sim = Sim::new(None);
        let mut seen = vec![];
        for op in ops {
            sim.step(op);
            seen.push(format!("{:?} {:?}", sim.net_open, sim.leases));
        }
        sim.quiesce();
        seen
    };
    for ops in cases(7, 8) {
        assert_eq!(trace(&ops), trace(&ops));
    }
}

/// The planted bug is caught by some case of a fixed seed.
fn caught(plant: Plant) {
    let caught = (0..4u8)
        .flat_map(|seed| cases(seed, 64))
        .any(|ops| catch_unwind(AssertUnwindSafe(|| run(&ops, Some(plant)))).is_err());
    assert!(caught, "{plant:?} was not caught");
}

#[test]
fn planted_routing_to_every_peer_is_caught() {
    caught(Plant::RouteEverywhere);
}

#[test]
fn planted_eager_shared_sessions_are_caught() {
    caught(Plant::EagerShared);
}

#[test]
fn planted_ignored_revoke_is_caught() {
    caught(Plant::IgnoreRevoke);
}

#[test]
fn planted_sessions_that_never_close_are_caught() {
    caught(Plant::NeverClose);
}

#[test]
fn planted_ignored_caps_are_caught() {
    caught(Plant::NoCaps);
}

#[test]
fn without_a_plant_the_same_cases_pass() {
    for ops in (0..4u8).flat_map(|seed| cases(seed, 64)) {
        run(&ops, None);
    }
}
