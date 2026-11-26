//! One named test per law of the [parent module](super), each a short script of the
//! simulation's rig. The rig checks every law after every step and at every answer as well, so
//! a script also fails if it breaks a law it was not written for.

use std::collections::BTreeSet;

use super::sim::Part::Bad;
use super::sim::Part::Ingest;
use super::sim::Part::Noop;
use super::sim::Part::Write;
use super::sim::Sim;
use super::*;

fn keys(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(ToString::to_string).collect()
}

/// A rig with every Document hydrated and nothing in flight.
fn ready() -> Sim {
    let mut sim = Sim::new(1, None);
    sim.settle_documents();
    assert!(sim.kinds().is_empty());
    sim
}

/// Run a plan whose Documents have all prepared to its end, every call ok.
fn finish(sim: &mut Sim) {
    while sim.kinds().contains(&"encrypt") {
        assert!(sim.complete_kind("encrypt", 0));
    }
    while sim.complete_kind("commit", 0) {}
}

#[test]
fn law_1_documents_in_flight_are_disjoint() {
    let mut sim = ready();
    let first = sim.submit(0b011, &[Write, Write]);
    let second = sim.submit(0b110, &[Write, Write]);
    assert_eq!(
        (sim.running(), sim.queued()),
        (1, 1),
        "the second shares document 1"
    );
    finish(&mut sim);
    assert!(sim.is_answered(first));
    assert_eq!(
        (sim.running(), sim.queued()),
        (1, 0),
        "and starts when the first is over"
    );
    finish(&mut sim);
    assert!(sim.is_answered(second));
    assert_eq!(sim.world.visible(1), keys(&["l1", "l2"]));
}

#[test]
fn law_4_plans_that_share_a_document_start_in_submission_order() {
    let mut sim = ready();
    // document 1 is busy, so the plan on {0, 1} is blocked
    sim.submit(0b010, &[Write]);
    sim.submit(0b011, &[Write, Write]);
    // a plan on {0} shares document 0 with the blocked plan ahead of it: it waits, though
    // document 0 is free
    sim.submit(0b001, &[Write]);
    assert_eq!((sim.running(), sim.queued()), (1, 2));
    finish(&mut sim);
    assert_eq!(
        (sim.running(), sim.queued()),
        (1, 1),
        "the blocked plan first"
    );
    finish(&mut sim);
    assert_eq!((sim.running(), sim.queued()), (1, 0));
    finish(&mut sim);
}

#[test]
fn a_plan_that_shares_nothing_overtakes_a_blocked_plan() {
    let mut sim = ready();
    sim.submit(0b010, &[Write]); // B, running
    sim.submit(0b011, &[Write, Write]); // {A, B}: blocked on B
    sim.submit(0b100, &[Write]); // {C}: shares nothing with the plans ahead
    assert_eq!(
        (sim.running(), sim.queued()),
        (2, 1),
        "{{C}} runs past the blocked {{A, B}}"
    );
    finish(&mut sim);
    finish(&mut sim);
}

#[test]
fn the_window_bounds_the_plans_in_flight() {
    let mut sim = Sim::with(1, None, Config { max_plans: 1 });
    sim.settle_documents();
    sim.submit(0b001, &[Write]);
    sim.submit(0b010, &[Write]);
    assert_eq!(
        (sim.running(), sim.queued()),
        (1, 1),
        "disjoint, but the window is full"
    );
    finish(&mut sim);
    finish(&mut sim);
    assert_eq!(sim.running(), 0);
}

#[test]
fn law_2_a_failed_encryption_commits_nothing() {
    let mut sim = ready();
    let plan = sim.submit(0b011, &[Write, Write]);
    assert_eq!(sim.kinds(), ["encrypt", "encrypt"]);
    assert!(sim.complete_kind("encrypt", 1));
    assert_eq!(sim.reply(plan), &Err(PlanError::Failed));
    // the other encryption is still out: it is cancelled, and settled when it answers
    assert!(sim.complete_kind("encrypt", 0));
    assert!(sim.kinds().is_empty());
    sim.settle_documents();
    assert_eq!(sim.world.visible(0), keys(&[]));
    assert_eq!(sim.world.visible(1), keys(&[]));
    sim.quiesce();
}

#[test]
fn law_2_a_failed_commit_commits_nothing_and_a_lost_answer_commits_everything() {
    let mut sim = ready();
    let failed = sim.submit(0b011, &[Write, Write]);
    finish_encrypting(&mut sim);
    assert!(sim.complete_kind("commit", 1));
    assert_eq!(sim.reply(failed), &Err(PlanError::Failed));
    assert_eq!(sim.world.visible(0), keys(&[]));
    sim.quiesce();
    // a commit that ran but whose answer was lost: the caller is told it failed, and the world
    // has all of it
    let lost = sim.submit(0b011, &[Write, Write]);
    finish_encrypting(&mut sim);
    assert!(sim.complete_kind("commit", 2));
    assert_eq!(sim.reply(lost), &Err(PlanError::Failed));
    assert_eq!(sim.world.visible(0), keys(&["l2"]));
    assert_eq!(sim.world.visible(1), keys(&["l2"]));
    sim.quiesce();
}

fn finish_encrypting(sim: &mut Sim) {
    while sim.kinds().contains(&"encrypt") {
        assert!(sim.complete_kind("encrypt", 0));
    }
    assert_eq!(sim.kinds(), ["commit"], "one commit for the whole plan");
}

#[test]
fn law_3_every_prepared_document_is_released_once_on_a_commit() {
    let mut sim = ready();
    // document 2 changes nothing: it is released without a commit row
    let plan = sim.submit(0b111, &[Write, Noop, Write]);
    assert_eq!(sim.kinds(), ["encrypt", "encrypt"]);
    finish(&mut sim);
    let Ok(outs) = sim.reply(plan) else {
        panic!("committed");
    };
    assert_eq!(outs.len(), 3);
    assert_eq!(sim.world.visible(0), keys(&["l1"]));
    assert_eq!(sim.world.visible(1), keys(&[]));
    assert_eq!(sim.world.visible(2), keys(&["l1"]));
}

#[test]
fn law_5_a_refused_prepare_aborts_the_whole_plan() {
    let mut sim = ready();
    // document 1's closure fails: it refuses, and document 0 is released
    let plan = sim.submit(0b011, &[Write, Bad]);
    let Err(PlanError::Refused { doc, why }) = sim.reply(plan) else {
        panic!("refused");
    };
    assert_eq!(*doc, 1);
    assert!(matches!(why, Rejected::Op(_)));
    assert!(sim.kinds().is_empty(), "nothing to encrypt or commit");
    assert_eq!(sim.world.visible(0), keys(&[]));
    // the next plan on document 0 is not held up (it takes a new document and a new actor)
    let next = sim.submit(0b001, &[Write]);
    sim.settle_documents();
    finish(&mut sim);
    assert!(sim.reply(next).is_ok());
}

#[test]
fn law_5_a_parked_document_refuses() {
    let mut sim = Sim::new(1, None);
    sim.remote_write(2, false);
    sim.deliver_all();
    sim.settle_documents();
    let plan = sim.submit(0b110, &[Write, Write]);
    let Err(PlanError::Refused { doc, why }) = sim.reply(plan) else {
        panic!("refused");
    };
    assert_eq!((*doc, why), (2, &Rejected::Parked));
    assert_eq!(sim.world.visible(1), keys(&[]));
    sim.quiesce();
}

#[test]
fn law_5_an_unreadable_document_refuses() {
    let mut sim = ready();
    sim.set_readable(1, false);
    sim.deliver_all();
    let plan = sim.submit(0b011, &[Write, Write]);
    let Err(PlanError::Refused { doc, why }) = sim.reply(plan) else {
        panic!("refused");
    };
    assert_eq!((*doc, why), (1, &Rejected::Unreadable));
    sim.quiesce();
}

#[test]
fn externally_authored_changes_for_n_documents_commit_in_one_plan() {
    let mut sim = ready();
    let plan = sim.submit(0b111, &[Ingest, Ingest, Ingest]);
    assert_eq!(sim.kinds().len(), 3);
    finish(&mut sim);
    assert!(sim.reply(plan).is_ok());
    for doc in 0..3 {
        assert_eq!(sim.world.visible(doc), keys(&["e1"]));
    }
    // and a plan that mixes an ingest with a local write
    let mixed = sim.submit(0b011, &[Ingest, Write]);
    finish(&mut sim);
    assert!(sim.reply(mixed).is_ok());
    sim.quiesce();
}

#[test]
fn a_plan_that_changes_nothing_commits_nothing() {
    let mut sim = ready();
    let plan = sim.submit(0b011, &[Noop, Noop]);
    assert!(sim.is_answered(plan));
    assert!(
        sim.kinds().is_empty(),
        "no encryption, no commit, no actor to replace"
    );
    assert!(sim.reply(plan).is_ok());
}
