//! Law zero of the simulation: a seed replays. Without it a failure the simulation finds cannot
//! be reproduced.

use super::ops::Op;
use super::ops::op;
use super::*;

use proptest::strategy::ValueTree;
use proptest::test_runner::Config;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;

/// What the driver observes of the machine after a step, as text.
fn snapshot(sim: &Sim, start: Instant) -> String {
    format!(
        "rounds={:?} restarts={:?} mirror={:?} parts={:?} objs={:?} net={} \
         session={} now={:?} due={:?}",
        sim.rounds,
        sim.restarts,
        sim.mirror,
        sim.wanted_parts,
        sim.wanted_objs,
        sim.net.len(),
        sim.session,
        sim.now - start,
        sim.sync
            .next_due()
            .map(|due| due.saturating_duration_since(start)),
    )
}

/// One case's trace: the snapshot after every op, then after the drain.
fn trace(ops: &[Op], seed: u64) -> Vec<String> {
    let mut sim = Sim::new(Shape::small(), seed);
    let start = sim.now;
    let mut out = vec![];
    for op in ops {
        sim.step(op.clone());
        out.push(format!("{op:?} => {}", snapshot(&sim, start)));
    }
    sim.step(Op::Poison(0));
    sim.quiesce();
    out.push(format!("quiesced => {}", snapshot(&sim, start)));
    out
}

fn cases(seed: u8, count: usize) -> Vec<Vec<Op>> {
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &[seed; 32]),
    );
    let strat = proptest::collection::vec(op(), 100..300);
    (0..count)
        .map(|_| strat.new_tree(&mut runner).unwrap().current())
        .collect()
}

#[test]
fn the_same_seed_replays_the_same_trace() {
    for seed in 0..32u8 {
        for (at, ops) in cases(seed, 12).iter().enumerate() {
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
