//! The simulation's ops, cases and planted bugs.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use proptest::strategy::ValueTree;
use proptest::test_runner::Config as RunnerConfig;
use proptest::test_runner::RngAlgorithm;
use proptest::test_runner::TestRng;
use proptest::test_runner::TestRunner;

use proptest::prelude::*;

use super::*;

#[derive(Debug, Clone)]
pub(in crate::keyhive_boundary) enum Op {
    Encrypt(u8),
    /// Finish a call in flight; `mode` as in [`Sim::complete`], mostly ok.
    Complete {
        pick: u8,
        mode: u8,
    },
    Settle {
        pick: u8,
        mode: u8,
    },
    /// A membership change (`slot` 255) or an ingest from the slot's session.
    Locked(u8),
    Connect(u8),
    Disconnect(u8),
    Standing {
        slot: u8,
        class: u8,
        on: bool,
    },
    Prekeys(u8),
    Evict(u8),
    Decrypt(u8),
    Advance(u8),
    Tick,
    /// A jump of the clock past every debounce.
    Jump,
}

impl Sim {
    pub fn step(&mut self, op: Op) {
        if self.trace {
            eprintln!("{op:?}");
        }
        let slot = |slot: u8| usize::from(slot) % SLOTS;
        match op {
            Op::Encrypt(doc) => {
                self.encrypt(u64::from(doc) % (DOCS + 1));
            }
            Op::Complete { pick, mode } => {
                let mode = match mode % 64 {
                    0..=5 => 1,
                    6 => 2,
                    _ => 0,
                };
                self.complete(usize::from(pick), mode);
            }
            Op::Settle { pick, mode } => self.settle(usize::from(pick), usize::from(mode)),
            Op::Locked(from) => {
                self.locked((from != 255).then(|| slot(from)));
            }
            Op::Connect(s) => {
                self.connect(slot(s));
            }
            Op::Disconnect(s) => self.disconnect(slot(s)),
            Op::Standing { slot: s, class, on } => self.set_standing(slot(s), u64::from(class), on),
            Op::Prekeys(left) => self.prekeys(usize::from(left % 12)),
            Op::Evict(doc) => self.evict(u64::from(doc)),
            Op::Decrypt(doc) => {
                self.decrypt(u64::from(doc) % (DOCS + 1));
            }
            Op::Advance(by) => self.advance(Duration::from_millis(40 * u64::from(by))),
            Op::Tick => self.tick(),
            Op::Jump => self.advance(Duration::from_secs(30)),
        }
    }
}

pub(in crate::keyhive_boundary) fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        10 => any::<u8>().prop_map(Op::Encrypt),
        30 => (any::<u8>(), any::<u8>()).prop_map(|(pick, mode)| Op::Complete { pick, mode }),
        12 => (any::<u8>(), any::<u8>()).prop_map(|(pick, mode)| Op::Settle { pick, mode }),
        6 => prop_oneof![Just(255u8), 0u8..3].prop_map(Op::Locked),
        4 => any::<u8>().prop_map(Op::Connect),
        2 => any::<u8>().prop_map(Op::Disconnect),
        3 => (any::<u8>(), any::<u8>(), any::<bool>())
            .prop_map(|(slot, class, on)| Op::Standing { slot, class, on }),
        2 => any::<u8>().prop_map(Op::Prekeys),
        2 => any::<u8>().prop_map(Op::Evict),
        6 => any::<u8>().prop_map(Op::Decrypt),
        4 => any::<u8>().prop_map(Op::Advance),
        3 => Just(Op::Tick),
        1 => Just(Op::Jump),
    ]
}

/// Run `ops`, then drain.
pub(in crate::keyhive_boundary) fn run(ops: Vec<Op>, seed: u64, plant: Option<Plant>) {
    let mut sim = Sim::new(seed, plant);
    sim.consume = true;
    for op in ops {
        sim.step(op);
    }
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
    fn the_boundary_obeys_its_laws_through_failures_disorder_and_peers_coming_and_going(
        ops in proptest::collection::vec(op(), 0..250),
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

fn trace(ops: &[Op], seed: u64) -> Vec<String> {
    let mut sim = Sim::new(seed, None);
    sim.consume = true;
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
        for (at, ops) in cases(seed, 6, 200).iter().enumerate() {
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

/// The fixed cases pass on the real machine. `SIM_CASE=seed:case` runs one alone.
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
        for (at, ops) in cases(seed, 24, 250).into_iter().enumerate() {
            if only.is_none_or(|only| only == (seed, at)) {
                let count = ops.len();
                if let Err(payload) =
                    catch_unwind(AssertUnwindSafe(|| run(ops, u64::from(seed), None)))
                {
                    panic!(
                        "seed {seed} case {at} ({count} ops): {}",
                        panic_message(payload.as_ref())
                    );
                }
            }
        }
    }
}

fn caught(plant: Plant, law: &str) {
    for seed in 0..16u8 {
        for (at, ops) in cases(seed, 24, 250).into_iter().enumerate() {
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
fn planted_apply_of_a_dropped_preparation_is_caught() {
    caught(Plant::ApplyDropped, "law 1");
}

#[test]
fn planted_encryption_during_a_locked_operation_is_caught() {
    caught(Plant::EncryptDuringLocked, "law 3");
}

#[test]
fn planted_lock_with_an_encryption_in_flight_is_caught() {
    caught(Plant::LockWithEncryptionInFlight, "law 3");
}

#[test]
fn planted_echo_to_the_source_is_caught() {
    caught(Plant::EchoToSource, "law 4");
}

#[test]
fn planted_notification_of_an_ineligible_session_is_caught() {
    caught(Plant::NotifyIneligible, "law 4");
}

#[test]
fn planted_double_notification_is_caught() {
    caught(Plant::NotifyTwice, "law 4");
}

#[test]
fn planted_skipped_reopen_is_caught() {
    caught(Plant::SkipReopen, "law 2");
}

#[test]
fn planted_skipped_hydration_is_caught() {
    caught(Plant::SkipHydrate, "law 5");
}

#[test]
fn planted_skipped_refill_is_caught() {
    caught(Plant::SkipRefill, "law 6");
}

#[test]
fn planted_commands_after_a_trap_are_caught() {
    caught(Plant::ContinueAfterTrap, "law 7");
}

#[test]
fn planted_decryption_without_a_hydration_is_caught() {
    caught(Plant::DecryptWithoutHydrate, "law 5");
}

#[test]
fn planted_refill_that_is_never_retried_is_caught() {
    caught(Plant::NoRefillRetry, "law 8");
}
