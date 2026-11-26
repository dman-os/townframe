//! One named test per law of the [parent module](crate::document), each a short script of the
//! simulation's rig checked against the oracle world. The rig checks every law after every step
//! as well, so a script also fails if it breaks a law it was not written for.

use std::collections::BTreeSet;

use super::super::sim::Sim;
use super::super::sim::WriteKind;

fn keys(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(ToString::to_string).collect()
}

/// A rig with document 0 open and live: hydrated, nothing in flight.
fn live() -> Sim {
    let mut sim = Sim::new(1, None);
    sim.open(0);
    assert!(sim.complete_kind(0, "hydrate", false));
    sim.deliver_all();
    sim
}

#[test]
fn law_1_readers_see_only_committed_state() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    // the change is made, out for encryption, and nobody may look at the document
    assert_eq!(sim.in_flight(0), ["encrypt"]);
    sim.read(0);
    assert_eq!(sim.unanswered(), 2, "the read waits for the write");
    assert!(sim.complete_kind(0, "encrypt", false));
    assert_eq!(sim.in_flight(0), ["commit"]);
    assert_eq!(sim.unanswered(), 2, "and still waits: nothing is committed");
    // the commit lands: the write is answered, then the read, which sees it
    assert!(sim.complete_kind(0, "commit", false));
    assert_eq!(sim.unanswered(), 0);
    assert_eq!(sim.live(0), Some(keys(&["l1"])));
    assert_eq!(sim.world.visible(0), keys(&["l1"]));
}

/// A failed commit leaves readers seeing only committed state: the live document is rebuilt
/// from committed state, and the reads that waited are served from that.
#[test]
fn law_1_a_failed_commit_leaves_readers_with_committed_state_only() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    sim.read(0);
    assert!(sim.complete_kind(0, "commit", true));
    // the document with the uncommitted change is gone, and the read waits for a new one
    assert_eq!(sim.live(0), None);
    // a commit that failed may have landed: a fresh actor first
    assert!(sim.complete_kind(0, "actor", false));
    assert_eq!(sim.in_flight(0), ["hydrate"]);
    assert_eq!(
        sim.unanswered(),
        1,
        "the write is answered, the read is not"
    );
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(
        sim.unanswered(),
        0,
        "the rig checked the read saw no uncommitted key"
    );
    assert_eq!(sim.live(0), Some(keys(&[])));
    assert_eq!(sim.world.visible(0), keys(&[]));
    // a later write is made on the rebuilt document and lands
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    assert_eq!(sim.live(0), Some(keys(&["l2"])));
}

/// A commit that landed but was reported failed: the new document shows it, and the next write
/// takes a fresh actor.
#[test]
fn law_2_a_commit_that_landed_but_was_reported_failed() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_undecided(0, true));
    assert_eq!(sim.live(0), None);
    assert_eq!(
        sim.in_flight(0),
        ["actor"],
        "waits for a fresh actor before it hydrates"
    );
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "actor", false));
    assert!(sim.complete_kind(0, "hydrate", false));
    // the write that was reported failed is there: it landed. The next one is being made on it.
    assert_eq!(sim.world.visible(0), keys(&["l1"]));
    assert_eq!(sim.live(0), Some(keys(&["l1", "l2"])));
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    assert_eq!(sim.world.visible(0), keys(&["l1", "l2"]));
}

/// A commit reported failed that only runs later: the next write was made after a hydration
/// that did not have it. Under the old actor the two would share a sequence number; under the
/// fresh one they sit side by side.
#[test]
fn law_2_a_commit_that_was_reported_failed_runs_later() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_undecided(0, false));
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "actor", false));
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(
        sim.live(0),
        Some(keys(&["l2"])),
        "the first has not run yet"
    );
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    sim.land_zombies();
    sim.quiesce();
    assert_eq!(sim.world.visible(0), keys(&["l1", "l2"]));
    assert_eq!(sim.live(0), Some(keys(&["l1", "l2"])));
}

#[test]
fn law_1_a_failed_encryption_is_forgotten_too() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", true));
    assert_eq!(sim.live(0), None);
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(sim.live(0), Some(keys(&[])));
}

#[test]
fn law_1_an_aborted_plan_is_forgotten() {
    let mut sim = live();
    sim.plan_prepare_writes(0b01);
    assert_eq!(sim.finishable().len(), 1);
    sim.plan_finish(0, false);
    assert_eq!(
        sim.live(0),
        None,
        "the document with the plan's change is gone"
    );
    assert!(sim.complete_kind(0, "actor", false));
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(sim.live(0), Some(keys(&[])));
    assert_eq!(sim.world.visible(0), keys(&[]));
    sim.plan_prepare_writes(0b01);
    sim.plan_finish(0, true);
    assert_eq!(sim.live(0), Some(keys(&["l2"])), "a committed plan is kept");
}

/// An external author's change goes through the same path as a write: held until committed,
/// forgotten if the commit fails, and a repeat is a no-op.
#[test]
fn law_1_an_ingested_change_is_visible_only_once_committed() {
    let mut sim = live();
    sim.ingest(0, false);
    sim.read(0);
    assert_eq!(sim.in_flight(0), ["encrypt"]);
    assert_eq!(sim.unanswered(), 2);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", true));
    assert_eq!(sim.live(0), None);
    assert!(sim.complete_kind(0, "actor", false));
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(sim.live(0), Some(keys(&[])));
    // sent again, it lands
    sim.ingest(0, true);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    assert_eq!(sim.live(0), Some(keys(&["e1"])));
    // and sent a third time it changes nothing: no call at all
    sim.ingest(0, true);
    assert!(sim.in_flight(0).is_empty());
    assert_eq!(sim.unanswered(), 0);
}

/// automerge is a CRDT: remote changes are applied while a write is in flight. If the write
/// then fails, the new document has the durable remote change and not the local one.
#[test]
fn law_1_remote_changes_are_applied_during_a_write_and_survive_its_failure() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    sim.read(0);
    sim.remote_write(0, true);
    sim.deliver_all();
    assert_eq!(sim.in_flight(0), ["encrypt", "decrypt"]);
    assert!(sim.complete_kind(0, "decrypt", false));
    assert_eq!(
        sim.live(0),
        Some(keys(&["l1", "r1"])),
        "applied alongside the write"
    );
    assert_eq!(sim.unanswered(), 2, "the read still waits");
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", true));
    assert!(sim.complete_kind(0, "actor", false));
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(sim.live(0), Some(keys(&["r1"])));
    assert_eq!(sim.unanswered(), 0);
}

/// The frontier's rows are written one at a time: a write waits for the publication in flight.
#[test]
fn law_5_a_write_waits_for_the_publication_in_flight() {
    let mut sim = live();
    sim.remote_write(0, true);
    sim.deliver_all();
    assert!(sim.complete_kind(0, "decrypt", false));
    assert_eq!(sim.in_flight(0), ["publish"]);
    sim.write(0, WriteKind::Normal);
    assert_eq!(sim.in_flight(0), ["publish"], "the write waits");
    assert!(sim.complete_kind(0, "publish", false));
    assert_eq!(sim.in_flight(0), ["encrypt"]);
    sim.quiesce();
    assert_eq!(sim.world.frontier_rev(0), sim.world.tree_rev(0));
}

#[test]
fn law_2_per_document_order() {
    let mut sim = live();
    for _ in 0..4 {
        sim.write(0, WriteKind::Normal);
    }
    // one at a time: only the first has been built
    assert_eq!(sim.in_flight(0), ["encrypt"]);
    for _ in 0..4 {
        assert!(sim.complete_kind(0, "encrypt", false));
        assert!(sim.complete_kind(0, "commit", false));
    }
    // each change was built on the one before it, so they landed in submission order
    let landed = &sim.world.docs[0].changes;
    assert_eq!(landed.len(), 4);
    for pair in landed.windows(2) {
        assert!(pair[1].change.deps().contains(&pair[0].change.hash()));
    }
    assert_eq!(sim.live(0), Some(keys(&["l1", "l2", "l3", "l4"])));
}

#[test]
fn law_3_read_your_writes() {
    let mut sim = live();
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    // the rig remembers the write was acknowledged, and checks that this read sees it
    sim.read(0);
    // and so does a read after the machine is gone and rebuilt
    sim.crash(0);
    sim.open(0);
    assert!(sim.complete_kind(0, "hydrate", false));
    sim.read(0);
    assert_eq!(sim.live(0), Some(keys(&["l1"])));
}

#[test]
fn law_4_plans_hold_the_document() {
    let mut sim = live();
    sim.plan_prepare_writes(0b01);
    assert_eq!(sim.describe(0).split(' ').nth(1), Some("held"));
    // a write and a remote change arrive while the plan holds the document
    sim.write(0, WriteKind::Normal);
    sim.remote_write(0, true);
    sim.deliver_all();
    // remote content is applied alongside the hold, but no write starts
    assert_eq!(
        sim.in_flight(0),
        ["decrypt"],
        "nothing else starts while held"
    );
    sim.plan_finish(0, true);
    // the held write resumes, after the plan's change
    sim.quiesce();
    assert_eq!(sim.live(0), Some(keys(&["l1", "l2", "r1"])));
}

#[test]
fn law_5_frontier() {
    let mut sim = live();
    // a local write publishes its frontier in its own commit: no publish call
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    assert_eq!(sim.world.frontier_rev(0), sim.world.tree_rev(0));
    sim.deliver_all();
    assert!(sim.in_flight(0).is_empty());
    // remote content publishes once applied, and never before
    sim.remote_write(0, true);
    assert_eq!(sim.world.frontier_rev(0), sim.world.tree_rev(0) - 1);
    sim.deliver_all();
    assert_eq!(sim.in_flight(0), ["decrypt"]);
    assert!(sim.complete_kind(0, "decrypt", false));
    assert_eq!(sim.in_flight(0), ["publish"]);
    assert!(sim.complete_kind(0, "publish", false));
    assert_eq!(sim.world.frontier_rev(0), sim.world.tree_rev(0));
    // content that landed between the machine's last look and its own commit is not claimed
    sim.write(0, WriteKind::Normal);
    sim.remote_write(0, true);
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    assert!(
        sim.world.frontier_rev(0) < sim.world.tree_rev(0),
        "the commit's frontier row is refused: the tree held content the machine had not applied"
    );
    sim.quiesce();
    assert_eq!(sim.world.frontier_rev(0), sim.world.tree_rev(0));
}

/// Found by the simulation: a frontier that was current for what was applied, in a document the
/// tree had moved ahead of, was left current when the live copy was dropped.
#[test]
fn law_5_a_dropped_document_that_was_behind_has_a_stale_frontier() {
    let mut sim = live();
    sim.remote_write(0, true);
    sim.deliver_all();
    assert!(sim.complete_kind(0, "decrypt", false));
    assert_eq!(sim.in_flight(0), ["publish"]);
    // the tree moves on while the frontier is being published
    sim.remote_write(0, true);
    sim.deliver_all();
    assert!(sim.complete_kind(0, "publish", false));
    // access is lost: the in-flight read is cancelled and the live copy dropped
    sim.set_readable(0, false);
    sim.deliver_all();
    assert_eq!(sim.describe(0).split(' ').next(), Some("cold"));
    sim.set_readable(0, true);
    sim.quiesce();
    assert_eq!(sim.world.frontier_rev(0), sim.world.tree_rev(0));
}

/// Found by the Write coordinator's simulation: a commit that finds content in the tree it has
/// not applied raised `known_rev` past it, and a document blocked on keys stayed blocked, with
/// no later feed row to make it ask again.
#[test]
fn law_5_a_commit_that_finds_unapplied_content_fetches_it_even_when_blocked() {
    let mut sim = live();
    sim.remote_write(0, false);
    sim.deliver_all();
    assert!(sim.complete_kind(0, "decrypt", false));
    assert!(sim.describe(0).contains("blocked"), "{}", sim.describe(0));
    sim.write(0, WriteKind::Normal);
    assert!(sim.complete_kind(0, "encrypt", false));
    // another node's change lands before this write's commit, and its feed row is slow
    sim.remote_write(0, true);
    assert!(sim.complete_kind(0, "commit", false));
    sim.deliver_all();
    sim.quiesce();
    // (the new change builds on the one that has no key, so it is not visible either; but the
    // machine has read the tree through it)
    assert_eq!(sim.live(0), Some(keys(&["l1"])));
    assert!(
        sim.describe(0).starts_with("live@3/blocked"),
        "{}",
        sim.describe(0)
    );
}

#[test]
fn law_6_parked_means_blocked() {
    let mut sim = Sim::new(1, None);
    sim.remote_write(0, false);
    sim.deliver_all();
    sim.open(0);
    assert!(sim.complete_kind(0, "hydrate", false));
    assert!(sim.describe(0).starts_with("parked"), "{}", sim.describe(0));
    // reads and writes wait while parked
    sim.read(0);
    assert!(sim.in_flight(0).is_empty());
    // the keys arrive: the epoch move makes it try again
    sim.grant_keys(0);
    sim.deliver_all();
    assert_eq!(sim.in_flight(0), ["hydrate"]);
    sim.quiesce();
    assert_eq!(sim.live(0), Some(keys(&["r1"])));
}

#[test]
fn law_7_eviction() {
    let mut sim = Sim::new(1, None);
    sim.open(0);
    assert!(!sim.evictable(0), "a handle");
    sim.drop_handle(0, 0);
    assert!(!sim.evictable(0), "hydrating");
    assert!(sim.complete_kind(0, "hydrate", false));
    assert!(sim.evictable(0));
    sim.interest(0, true);
    assert!(!sim.evictable(0), "an interest");
    sim.interest(0, false);
    sim.write(0, WriteKind::Normal);
    sim.drop_handle(0, 0);
    assert!(!sim.evictable(0), "a write in flight");
    assert!(sim.complete_kind(0, "encrypt", false));
    assert!(sim.complete_kind(0, "commit", false));
    sim.deliver_all();
    assert!(sim.evictable(0));
    // evicting loses nothing: a new machine finds the write
    sim.evict(0);
    sim.open(0);
    assert!(sim.complete_kind(0, "hydrate", false));
    assert_eq!(sim.live(0), Some(keys(&["l1"])));
    // a document with remote content to catch up is not evictable
    sim.drop_handle(0, 0);
    sim.remote_write(0, true);
    sim.deliver_all();
    assert!(!sim.evictable(0), "behind");
}

#[test]
fn law_8_unreadable_documents_are_never_hydrated() {
    let mut sim = Sim::new(1, None);
    sim.set_readable(0, false);
    sim.deliver_all();
    sim.open(0);
    sim.read(0);
    assert!(sim.in_flight(0).is_empty(), "no hydration");
    // told it is unreadable, the machine refuses what waits, and hydrates when it is told
    // otherwise
    sim.set_readable(0, true);
    sim.deliver_all();
    assert_eq!(sim.in_flight(0), ["hydrate"]);
    // losing access while hydrating cancels the read
    sim.set_readable(0, false);
    sim.deliver_all();
    sim.quiesce();
    assert_eq!(sim.describe(0).split(' ').next(), Some("cold"));
}

#[test]
fn a_failed_call_backs_off_and_retries() {
    let mut sim = Sim::new(1, None);
    sim.open(0);
    assert!(sim.complete_kind(0, "hydrate", true));
    assert!(sim.in_flight(0).is_empty(), "waiting out the backoff");
    sim.tick();
    assert_eq!(sim.in_flight(0), ["hydrate"]);
}

#[test]
fn a_stale_handle_is_refused() {
    let mut sim = live();
    sim.drop_handle(0, 0);
    sim.evict(0);
    sim.open(0);
    // the rig checks that the reply is `StaleHandle` and that nothing was written
    sim.write(0, WriteKind::Stale);
    sim.write(0, WriteKind::Bad);
    sim.quiesce();
}
