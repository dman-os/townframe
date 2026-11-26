//! One named test per law of the [parent module](super), each a short script of the simulation's
//! rig. The rig's oracle checks every law at every command, so a script also fails if it breaks
//! a law it was not written for.

use std::time::Duration;

use super::sim::Sim;
use super::*;

/// Run a membership change (or an ingest from `slot`) to its commit, every call ok.
fn run_locked(sim: &mut Sim, slot: Option<usize>) -> Ticket {
    let ticket = sim.locked(slot);
    for kind in ["lock", "begin", "run", "commit"] {
        assert!(sim.complete_kind(kind, 0), "{kind} is in flight");
    }
    ticket
}

/// An encryption of `doc` to the point where it is prepared and waits for its commit.
fn prepared(sim: &mut Sim, doc: u64) {
    sim.encrypt(doc);
    sim.complete_all("hydrate");
    assert!(sim.complete_kind("encrypt", 0));
}

/// Let the debounce run out and answer every eligibility query.
fn notify_round(sim: &mut Sim) {
    sim.advance(Duration::from_secs(2));
    sim.complete_all("eligible");
}

#[test]
fn law_1_a_preparation_is_applied_only_when_its_commit_landed() {
    let mut sim = Sim::new(1, None);
    prepared(&mut sim, 0);
    // the commit failed: it is dropped, and nothing is applied
    sim.settle(0, 1);
    assert!(sim.kinds().is_empty());
    assert!(sim.log.iter().any(|c| c.starts_with("DropEncrypt")));
    assert!(!sim.log.iter().any(|c| c.starts_with("ApplyEncrypt")));
    // the commit landed but the answer was lost: dropped too
    prepared(&mut sim, 0);
    sim.settle(0, 2);
    assert!(sim.kinds().is_empty());
    // the commit landed: applied
    prepared(&mut sim, 0);
    sim.settle(0, 0);
    assert_eq!(sim.kinds(), ["apply"]);
    sim.complete_all("apply");
    sim.quiesce();
}

#[test]
fn a_refused_encryption_is_answered_failed_and_holds_nothing() {
    let mut sim = Sim::new(1, None);
    sim.encrypt(0);
    sim.complete_all("hydrate");
    assert!(sim.complete_kind("encrypt", 1));
    assert!(sim.log.iter().any(|c| c.contains("Failed")));
    assert_eq!(sim.prepared_count(), 0);
    sim.quiesce();
}

#[test]
fn encryptions_of_different_documents_run_at_once() {
    let mut sim = Sim::new(1, None);
    sim.encrypt(0);
    sim.encrypt(1);
    sim.encrypt(3);
    assert_eq!(sim.kinds(), ["hydrate", "hydrate"], "documents 0 and 1");
    sim.complete_all("hydrate");
    assert_eq!(sim.kinds(), ["encrypt", "encrypt", "encrypt"]);
    sim.complete_all("encrypt");
    for _ in 0..3 {
        sim.settle(0, 0);
    }
    sim.quiesce();
}

#[test]
fn law_2_a_locked_operation_commits_all_its_rows() {
    let mut sim = Sim::new(1, None);
    let ticket = run_locked(&mut sim, None);
    assert_eq!(sim.reply(ticket), Some(&Ok(())));
    assert!(
        sim.log.iter().position(|c| c.starts_with("AcquireLock"))
            < sim.log.iter().position(|c| c.starts_with("Begin")),
        "the lock is taken before the transaction"
    );
    sim.quiesce();
}

#[test]
fn law_2_a_refused_run_rolls_back_and_reopens_before_anything_else() {
    let mut sim = Sim::new(1, None);
    prepared(&mut sim, 0);
    sim.settle(0, 0);
    sim.complete_all("apply");
    assert_eq!(sim.m.docs.len(), 1, "document 0 is loaded");
    let ticket = sim.locked(None);
    for kind in ["lock", "begin"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert!(sim.complete_kind("run", 1));
    assert_eq!(sim.kinds(), ["rollback"]);
    // an encryption asked for meanwhile waits: no keyhive call before the reopen
    sim.encrypt(0);
    assert_eq!(sim.kinds(), ["rollback"]);
    assert!(sim.complete_kind("rollback", 0));
    assert_eq!(sim.kinds(), ["reopen"]);
    assert!(sim.complete_kind("reopen", 0));
    assert_eq!(sim.reply(ticket), Some(&Err(Refused)));
    // the new instance holds nothing, so the document is hydrated again
    assert_eq!(sim.kinds(), ["hydrate"]);
    sim.quiesce();
}

#[test]
fn law_2_a_refused_commit_also_rolls_back_and_reopens() {
    let mut sim = Sim::new(1, None);
    let ticket = sim.locked(None);
    for kind in ["lock", "begin", "run"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert!(sim.complete_kind("commit", 1));
    assert!(sim.complete_kind("rollback", 0));
    assert!(sim.complete_kind("reopen", 0));
    assert_eq!(sim.reply(ticket), Some(&Err(Refused)));
    sim.quiesce();
}

#[test]
fn law_3_the_lock_waits_for_encryptions_and_blocks_new_ones() {
    let mut sim = Sim::new(1, None);
    prepared(&mut sim, 0);
    let ticket = sim.locked(None);
    assert!(
        sim.kinds().is_empty(),
        "a preparation still holds the shared lock"
    );
    // a new encryption queues behind the locked operation
    sim.encrypt(1);
    assert!(sim.kinds().is_empty());
    sim.settle(0, 0);
    sim.complete_all("apply");
    assert_eq!(sim.kinds(), ["lock"]);
    for kind in ["lock", "begin", "run", "commit"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert_eq!(sim.reply(ticket), Some(&Ok(())));
    assert_eq!(
        sim.kinds().len(),
        1,
        "the held encryption starts after the release"
    );
    sim.quiesce();
}

#[test]
fn law_3_locked_operations_run_one_at_a_time_in_order() {
    let mut sim = Sim::new(1, None);
    let first = sim.locked(None);
    let second = sim.locked(None);
    assert_eq!(sim.kinds(), ["lock"]);
    for kind in ["lock", "begin", "run", "commit"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert_eq!(sim.reply(first), Some(&Ok(())));
    assert_eq!(sim.reply(second), None);
    assert_eq!(sim.kinds(), ["lock"]);
    sim.quiesce();
    assert_eq!(sim.reply(second), Some(&Ok(())));
}

#[test]
fn law_4_only_eligible_sessions_are_told() {
    let mut sim = Sim::new(1, None);
    let (a, b) = (sim.connect(0).unwrap(), sim.connect(1).unwrap());
    let event = sim.fresh_event();
    // Keyline: session b may not see this class of event
    sim.set_standing(1, event % sim::CLASSES, false);
    sim.report_next(vec![(event, None)]);
    run_locked(&mut sim, None);
    notify_round(&mut sim);
    assert_eq!(sim.notified(a), [event]);
    assert!(sim.notified(b).is_empty());
    sim.quiesce();
}

#[test]
fn law_4_an_event_never_goes_back_to_its_source() {
    let mut sim = Sim::new(1, None);
    let (a, b) = (sim.connect(0).unwrap(), sim.connect(1).unwrap());
    let event = sim.fresh_event();
    sim.report_next(vec![(event, Some(a))]);
    run_locked(&mut sim, Some(0));
    notify_round(&mut sim);
    assert!(sim.notified(a).is_empty());
    assert_eq!(sim.notified(b), [event]);
    sim.quiesce();
}

#[test]
fn law_4_a_session_that_lost_standing_before_the_answer_is_not_told() {
    let mut sim = Sim::new(1, None);
    let a = sim.connect(0).unwrap();
    let event = sim.fresh_event();
    sim.report_next(vec![(event, None)]);
    run_locked(&mut sim, None);
    // asked at send time, not at commit time
    sim.set_standing(0, event % sim::CLASSES, false);
    notify_round(&mut sim);
    assert!(sim.notified(a).is_empty());
    sim.quiesce();
}

#[test]
fn law_4_each_event_reaches_a_session_once() {
    let mut sim = Sim::new(1, None);
    let a = sim.connect(0).unwrap();
    let event = sim.fresh_event();
    sim.report_next(vec![(event, None)]);
    run_locked(&mut sim, None);
    notify_round(&mut sim);
    notify_round(&mut sim);
    assert_eq!(sim.notified(a), [event]);
    assert!(sim.m.is_idle());
}

#[test]
fn law_4_a_reconnect_is_a_new_session_and_a_closed_one_hears_nothing() {
    let mut sim = Sim::new(1, None);
    let a = sim.connect(0).unwrap();
    let event = sim.fresh_event();
    sim.report_next(vec![(event, None)]);
    run_locked(&mut sim, None);
    sim.advance(Duration::from_secs(2));
    assert_eq!(sim.kinds(), ["eligible"]);
    // closing cancels the query; whatever it answers is ignored
    sim.disconnect(0);
    sim.complete_all("eligible");
    assert!(sim.notified(a).is_empty());
    let again = sim.connect(0).unwrap();
    assert_ne!(a, again);
    assert!(
        sim.m.is_idle(),
        "events before the connection are not replayed"
    );
}

#[test]
fn law_4_events_inside_the_debounce_share_one_query() {
    let mut sim = Sim::new(1, None);
    let a = sim.connect(0).unwrap();
    let (e1, e2) = (sim.fresh_event(), sim.fresh_event());
    sim.report_next(vec![(e1, None)]);
    run_locked(&mut sim, None);
    sim.advance(Duration::from_millis(50));
    sim.report_next(vec![(e2, None)]);
    run_locked(&mut sim, None);
    sim.advance(Duration::from_millis(50));
    assert!(
        sim.kinds().is_empty(),
        "the debounce restarted with the second event"
    );
    sim.advance(Duration::from_millis(100));
    assert_eq!(sim.kinds(), ["eligible"]);
    sim.complete_all("eligible");
    assert_eq!(sim.notified(a), [e1, e2]);
}

#[test]
fn law_4_a_steady_stream_is_still_asked_about_within_the_maximum_delay() {
    let mut sim = Sim::new(1, None);
    sim.connect(0).unwrap();
    let max = Config::default().max_delay;
    for _ in 0..12 {
        let event = sim.fresh_event();
        sim.report_next(vec![(event, None)]);
        run_locked(&mut sim, None);
        sim.advance(Duration::from_millis(90));
    }
    assert!(
        sim.kinds().contains(&"eligible"),
        "a stream that never pauses for {max:?} is asked about anyway"
    );
    sim.quiesce();
}

#[test]
fn an_encryptions_events_are_told_once_it_is_applied() {
    // a rotation's CGKA op rides the commit of the change
    let mut told = false;
    for seed in 0..64u64 {
        let mut sim = Sim::new(seed, None);
        let a = sim.connect(0).unwrap();
        prepared(&mut sim, 0);
        sim.settle(0, 0);
        sim.complete_all("apply");
        notify_round(&mut sim);
        told |= !sim.notified(a).is_empty();
        sim.quiesce();
    }
    assert!(told, "some seed's encryption carried an event");
}

#[test]
fn law_5_a_document_is_hydrated_before_it_is_used_and_again_after_an_eviction() {
    let mut sim = Sim::new(1, None);
    sim.encrypt(0);
    assert_eq!(sim.kinds(), ["hydrate"]);
    sim.complete_all("hydrate");
    assert_eq!(sim.kinds(), ["encrypt"]);
    sim.complete_all("encrypt");
    // the hub saw another process move the version: evicted once nothing prepared refers to it
    sim.evict(0);
    assert!(!sim.log.iter().any(|c| c.starts_with("Evict")));
    sim.settle(0, 0);
    sim.complete_all("apply");
    assert!(sim.log.iter().any(|c| c.starts_with("Evict")));
    sim.encrypt(0);
    assert_eq!(sim.kinds(), ["hydrate"]);
    sim.quiesce();
}

#[test]
fn law_5_an_eviction_during_a_hydration_hydrates_again() {
    let mut sim = Sim::new(1, None);
    sim.encrypt(0);
    sim.evict(0);
    assert_eq!(sim.kinds(), ["hydrate"]);
    assert!(sim.complete_kind("hydrate", 0));
    // what it read may be stale: evicted, and loaded afresh for the request
    assert!(sim.log.iter().any(|c| c.starts_with("Evict")));
    assert_eq!(sim.kinds(), ["hydrate"]);
    sim.quiesce();
}

#[test]
fn law_6_low_prekeys_are_refilled_by_a_locked_operation_of_their_own() {
    let mut sim = Sim::new(1, None);
    sim.prekeys(3);
    assert_eq!(sim.kinds(), ["lock"]);
    for kind in ["lock", "begin"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert!(
        sim.log
            .iter()
            .any(|c| c.starts_with("Run") && c.contains("refill: true")),
        "the run refills"
    );
    sim.quiesce();
}

#[test]
fn law_6_a_queued_operation_carries_the_refill() {
    let mut sim = Sim::new(1, None);
    let ticket = sim.locked(None);
    sim.prekeys(2);
    run_locked_tail(&mut sim);
    assert_eq!(sim.reply(ticket), Some(&Ok(())));
    assert!(
        sim.kinds().is_empty(),
        "no operation of its own: the queued one restored the floor"
    );
    sim.quiesce();
}

/// The steps after the lock was requested.
fn run_locked_tail(sim: &mut Sim) {
    for kind in ["lock", "begin", "run", "commit"] {
        assert!(sim.complete_kind(kind, 0));
    }
}

#[test]
fn law_6_a_refused_refill_does_not_loop() {
    let mut sim = Sim::new(1, None);
    sim.prekeys(1);
    for kind in ["lock", "begin"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert!(sim.complete_kind("run", 1));
    assert!(sim.complete_kind("rollback", 0));
    assert!(sim.complete_kind("reopen", 0));
    assert!(sim.kinds().is_empty(), "nothing retries on its own");
    // the next operation carries it
    run_locked(&mut sim, None);
    sim.quiesce();
}

#[test]
fn law_7_a_store_failure_traps_and_nothing_follows() {
    let mut sim = Sim::new(1, None);
    sim.connect(0).unwrap();
    sim.encrypt(0);
    sim.encrypt(1);
    assert!(sim.complete_kind("hydrate", 2));
    assert!(sim.m.is_trapped());
    assert!(sim.log.last().unwrap().starts_with("Trap"));
    let issued = sim.log.len();
    sim.complete_all("hydrate");
    sim.locked(None);
    sim.encrypt(2);
    sim.advance(Duration::from_secs(5));
    assert_eq!(sim.log.len(), issued, "no command follows a trap");
    assert_eq!(sim.m.next_due(), None);
    sim.quiesce();
}

#[test]
fn law_7_a_store_failure_inside_a_locked_operation_traps_without_a_rollback() {
    let mut sim = Sim::new(1, None);
    sim.locked(None);
    for kind in ["lock", "begin"] {
        assert!(sim.complete_kind(kind, 0));
    }
    assert!(sim.complete_kind("run", 2));
    assert!(sim.log.last().unwrap().starts_with("Trap"));
    assert!(!sim.log.iter().any(|c| c.starts_with("Rollback")));
}
