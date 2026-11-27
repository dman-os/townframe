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

/// `count` op lists from a fixed seed, the same on every run.
fn cases(seed: u8, count: usize) -> Vec<Vec<Op>> {
    let mut runner = TestRunner::new_with_rng(
        RunnerConfig::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &[seed; 32]),
    );
    let strategy = proptest::collection::vec(op(), 60..220);
    (0..count)
        .map(|_| strategy.new_tree(&mut runner).expect("a case").current())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    #[test]
    fn the_hub_and_the_machines_agree_with_the_world_through_faults_and_disorder(
        ops in proptest::collection::vec(op(), 0..260),
    ) {
        run(&ops, None);
    }
}

/// A fixed seed replays: the same cases give the same observations.
#[test]
fn a_seed_replays() {
    let trace = |ops: &[Op]| {
        let mut sim = Sim::new(None);
        let mut seen = vec![];
        for op in ops {
            sim.step(op);
            seen.push(format!(
                "{:?} {:?} {}",
                sim.machines.keys().collect::<Vec<_>>(),
                sim.routes,
                sim.saved_cursor
            ));
        }
        sim.quiesce();
        seen
    };
    for ops in cases(7, 8) {
        assert_eq!(trace(&ops), trace(&ops));
    }
}

#[test]
fn without_a_plant_the_same_cases_pass() {
    for ops in (0..4u8).flat_map(|seed| cases(seed, 48)) {
        run(&ops, None);
    }
}

/// The planted bug is caught by a case of a fixed seed, or by the script that reaches it
/// directly, for the bugs that need a particular state of the world.
fn caught(plant: Plant, script: &[Op]) {
    if !script.is_empty() {
        assert!(
            catch_unwind(AssertUnwindSafe(|| run(script, Some(plant)))).is_err(),
            "{plant:?} was not caught by its script"
        );
        // and the script is innocent without the plant
        run(script, None);
        return;
    }
    let caught = (0..6u8)
        .flat_map(|seed| cases(seed, 64))
        .any(|ops| catch_unwind(AssertUnwindSafe(|| run(&ops, Some(plant)))).is_err());
    assert!(caught, "{plant:?} was not caught");
}

macro_rules! planted {
    ($($name:ident: $plant:ident $(= $script:expr)?,)*) => {$(
        #[test]
        fn $name() {
            #[allow(unused_mut, unused_assignments)]
            let mut script: Vec<Op> = vec![];
            $(script = $script;)?
            caught(Plant::$plant, &script);
        }
    )*};
}

planted! {
    planted_stop_while_wanted_is_caught: StopBusy,
    planted_second_machine_for_a_running_document_is_caught: DoubleStart,
    planted_start_on_a_keyhive_epoch_is_caught: StartOnEpoch,
    planted_parked_documents_that_never_wake_are_caught: NoWake,
    planted_dropped_catch_up_is_caught: DropStale,
    planted_catch_up_past_its_limit_is_caught: LimitIgnored,
    planted_cursor_past_unsettled_work_is_caught: CursorAhead,
    planted_ignored_window_is_caught: WindowIgnored,
    planted_lost_waiters_are_caught: LoseWaiters,
    // a round in flight when the Hub is told to drain
    planted_early_drain_is_caught: DrainEarly = vec![
        Op::Connect(0),
        Op::Dial(0, true),
        Op::Want(0, true),
        Op::Drain,
    ],
    // a routed document whose standing is then all revoked
    planted_ignored_revoke_is_caught: IgnoreRevoke = vec![
        Op::LocalStanding(0, 0, true),
        Op::PeerStanding(0, 0, true),
        Op::Connect(0),
        Op::InterestOn(0),
        Op::Dial(0, true),
        Op::PeerStanding(0, 0, false),
    ],
    planted_reused_actor_is_caught: SameActor,
    // a plan that arrives while the stop request for its document is out
    planted_serving_a_stopping_document_is_caught: ServeWhileStopping = {
        let mut ops = vec![Op::Open(0), Op::Complete(0, false)];
        ops.extend((0..4).map(|_| Op::Reports(0)));
        ops.push(Op::CloseHandle(0, 0));
        ops.extend((0..4).map(|_| Op::Reports(0)));
        ops.push(Op::Advance(3));
        ops.extend((0..4).map(|_| Op::Reports(0)));
        ops.push(Op::Plan(1));
        ops
    },
}
