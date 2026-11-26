//! The simulation's ops, cases and planted bugs.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use proptest::strategy::ValueTree;
use proptest::test_runner::Config as RunnerConfig;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;

use super::*;

#[derive(Debug, Clone)]
pub(in crate::coordinator) enum Op {
    /// A plan over the documents of `mask` (never empty), each part drawn from `kinds`.
    Submit {
        mask: u8,
        kinds: u16,
    },
    /// Finish a call in flight; `mode` as in [`Sim::complete`], mostly ok.
    Complete {
        pick: u8,
        mode: u8,
    },
    LandZombie(u8),
    RemoteWrite {
        doc: u8,
        keyed: bool,
    },
    Keys {
        doc: u8,
    },
    Access {
        doc: u8,
        readable: bool,
    },
    Deliver(u8),
    Advance(u8),
    Tick,
    Crash {
        landed: u8,
    },
}

impl Sim {
    pub fn step(&mut self, op: Op) {
        if self.trace {
            eprintln!("{op:?}");
        }
        let doc = |doc: u8| usize::from(doc) % DOCS;
        match op {
            Op::Submit { mask, kinds } => {
                let mask = usize::from(mask) % ((1 << DOCS) - 1) + 1;
                let count = (0..DOCS).filter(|d| mask >> d & 1 == 1).count();
                let parts: Vec<Part> = (0..count)
                    .map(|at| match (kinds >> (3 * at)) & 7 {
                        0..=3 => Part::Write,
                        4 | 5 => Part::Ingest,
                        6 => Part::Noop,
                        _ => Part::Bad,
                    })
                    .collect();
                self.submit(mask, &parts);
            }
            Op::Complete { pick, mode } => {
                let mode = match mode % 16 {
                    0 => 1,
                    1 => 2,
                    2 => 3,
                    _ => 0,
                };
                self.complete(usize::from(pick), mode);
            }
            Op::LandZombie(pick) => self.land_zombie(usize::from(pick)),
            Op::RemoteWrite { doc: d, keyed } => self.remote_write(doc(d), keyed),
            Op::Keys { doc: d } => self.grant_keys(doc(d)),
            Op::Access { doc: d, readable } => self.set_readable(doc(d), readable),
            Op::Deliver(pick) => self.deliver(usize::from(pick)),
            Op::Advance(by) => self.advance(Duration::from_millis(250 * u64::from(by))),
            Op::Tick => self.tick(),
            Op::Crash { landed } => self.crash(usize::from(landed)),
        }
        self.check();
    }
}

pub(in crate::coordinator) fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        10 => (any::<u8>(), any::<u16>()).prop_map(|(mask, kinds)| Op::Submit { mask, kinds }),
        30 => (any::<u8>(), any::<u8>()).prop_map(|(pick, mode)| Op::Complete { pick, mode }),
        2 => any::<u8>().prop_map(Op::LandZombie),
        4 => (any::<u8>(), any::<bool>()).prop_map(|(doc, keyed)| Op::RemoteWrite { doc, keyed }),
        2 => any::<u8>().prop_map(|doc| Op::Keys { doc }),
        1 => (any::<u8>(), any::<bool>()).prop_map(|(doc, readable)| Op::Access { doc, readable }),
        8 => any::<u8>().prop_map(Op::Deliver),
        2 => any::<u8>().prop_map(Op::Advance),
        2 => Just(Op::Tick),
        1 => any::<u8>().prop_map(|landed| Op::Crash { landed }),
    ]
}

/// Run `ops`, drain, then crash, restart and drain again.
pub(in crate::coordinator) fn run(ops: Vec<Op>, seed: u64, plant: Option<Plant>) {
    let mut sim = Sim::new(seed, plant);
    for op in ops {
        sim.step(op);
    }
    sim.quiesce();
    sim.crash(0);
    sim.quiesce();
}

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
    fn the_coordinator_and_its_documents_agree_with_the_world_through_faults_and_disorder(
        ops in proptest::collection::vec(op(), 0..200),
        seed in any::<u64>(),
    ) {
        run(ops, seed, None);
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
        .unwrap_or_default()
}

/// One fixed case, named so a failure can be replayed with `SIM_CASE=seed:case`.
fn run_named(seed: u8, at: usize, ops: Vec<Op>, plant: Option<Plant>) {
    let count = ops.len();
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| run(ops, u64::from(seed), plant))) {
        panic!(
            "seed {seed} case {at} ({count} ops): {}",
            panic_message(payload.as_ref())
        );
    }
}

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

/// Law zero: a seed replays.
#[test]
fn the_same_seed_replays_the_same_trace() {
    for seed in 0..12u8 {
        for (at, ops) in cases(seed, 6, 160).iter().enumerate() {
            let first = trace(ops, u64::from(seed));
            let second = trace(ops, u64::from(seed));
            if let Some(step) = first.iter().zip(&second).position(|(a, b)| a != b) {
                panic!(
                    "seed {seed} case {at} diverged at step {step}\n first: {}\nsecond: {}",
                    first[step], second[step]
                );
            }
        }
    }
}

/// The fixed cases pass on the real machines. `SIM_CASE=seed:case` runs one alone.
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
    for seed in 0..16u8 {
        for (at, ops) in cases(seed, 24, 200).into_iter().enumerate() {
            let result = catch_unwind(AssertUnwindSafe(|| run(ops, u64::from(seed), Some(plant))));
            if let Err(payload) = result {
                let message = panic_message(payload.as_ref());
                assert!(
                    message.contains(law),
                    "{plant:?} was caught, but not by {law}: seed {seed} case {at}: {message}"
                );
                return;
            }
        }
    }
    panic!("{plant:?} was not caught");
}

#[test]
fn planted_overlapping_admission_is_caught() {
    caught(Plant::AdmitOverlapping, "law 1");
}

#[test]
fn planted_overtaking_is_caught() {
    caught(Plant::Overtake, "law 4");
}

#[test]
fn planted_commit_despite_a_failed_encryption_is_caught() {
    caught(Plant::CommitDespiteFailure, "law 2");
}

#[test]
fn planted_forgotten_abort_is_caught() {
    caught(Plant::ForgetToAbort, "law");
}

#[test]
fn planted_forgotten_settlement_is_caught() {
    caught(Plant::ForgetToSettle, "law 6");
}
