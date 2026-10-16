//! Behavioural tests for the sans-I/O router machine.
//!
//! These drive the real transitions — election projection, registration and
//! recovery, offers, declines, cancellation, and stale-session fencing — and
//! assert the emitted actions a driver would perform. No mocks echo anything
//! back; the adapter is a real (if trivial) classification source.

use super::super::test_util::*;
use super::*;
use crate::tasks::{
    ActiveAttemptSummary, AllocationId, AttemptState, Capacity, DeclineReason, OpaqueReason,
    PoolAttemptId, PoolClaimId, PoolTaskId, Preference, RouterSessionId,
    TaskDeclaration, TaskPoolId,
};

/// Nothing is ever proven obsolete. Used where the test is about scheduling,
/// not about domain settlement.
struct NothingObsolete;

impl RouterDomainAdapter for NothingObsolete {
    fn is_proven_obsolete(&self, _declaration: &TaskDeclaration) -> bool {
        false
    }
}

/// Reports proven obsolescence exactly for the listed obligations, standing in
/// for a domain holding durable settlement evidence.
struct ObsoleteTasks(HashSet<PoolTaskId>);

impl RouterDomainAdapter for ObsoleteTasks {
    fn is_proven_obsolete(&self, declaration: &TaskDeclaration) -> bool {
        self.0.contains(&declaration.task_id)
    }
}

fn protocols(versions: &[u16]) -> Vec<SchedulingProtocolVersion> {
    versions.iter().copied().map(SchedulingProtocolVersion).collect()
}

fn machine(node_seed: u8, supported: &[u16]) -> RouterMachine<SequentialIds> {
    RouterMachine::new(
        RouterConfig {
            pool: pool(),
            node: node(node_seed),
            incarnation: incarnation(node_seed),
            supported_protocols: protocols(supported),
        },
        ids(),
    )
}

fn claim(
    candidate_seed: u8,
    generation: u64,
    protocol: u16,
    claim_id: u128,
) -> RouterClaim {
    RouterClaim {
        pool: pool(),
        candidate: node(candidate_seed),
        incarnation: incarnation(candidate_seed),
        writer_seq: 1,
        generation,
        claim_id: PoolClaimId::new(claim_id),
        scheduling_protocol: SchedulingProtocolVersion(protocol),
        observed: BTreeMap::new(),
    }
}

fn feed<Adapter: RouterDomainAdapter>(
    machine: &mut RouterMachine<SequentialIds>,
    event: RouterEvent,
    adapter: &Adapter,
) -> Vec<RouterAction> {
    let mut out = Vec::new();
    machine.on_event(event, adapter, &mut out);
    out
}

/// Feed the persisted-claim acknowledgement the driver would send after its
/// durable write and `router_settle`.
fn confirm_claims(
    machine: &mut RouterMachine<SequentialIds>,
    actions: &[RouterAction],
    adapter: &NothingObsolete,
) {
    for action in actions {
        if let RouterAction::PersistClaim { claim } = action {
            let mut out = Vec::new();
            machine.on_event(
                RouterEvent::ClaimPersisted {
                    generation: claim.generation,
                    claim_id: claim.claim_id,
                },
                adapter,
                &mut out,
            );
        }
    }
}

fn register(
    machine: &mut RouterMachine<SequentialIds>,
    seed: u8,
    protocol: u16,
    capabilities: &[&str],
    capacity: u32,
) -> Vec<RouterAction> {
    register_session(
        machine,
        session(seed),
        protocol,
        capabilities,
        capacity,
        Vec::new(),
    )
}

fn register_session(
    machine: &mut RouterMachine<SequentialIds>,
    session: SessionKey,
    protocol: u16,
    capabilities: &[&str],
    capacity: u32,
    active_attempts: Vec<ActiveAttemptSummary>,
) -> Vec<RouterAction> {
    feed(
        machine,
        RouterEvent::RegisterExecutor {
            session,
            capabilities: CapabilitySummary::from_labels(
                capabilities.iter().copied(),
            ),
            capacity: Capacity::new(capacity),
            protocol: SchedulingProtocolVersion(protocol),
            active_attempts,
        },
        &NothingObsolete,
    )
}

fn session(seed: u8) -> SessionKey {
    SessionKey {
        node: node(seed),
        incarnation: incarnation(seed),
        session: RouterSessionId::new(u128::from(seed)),
    }
}

/// A new connection from the same node and incarnation — the case node plus
/// incarnation cannot distinguish on its own.
fn reconnect(seed: u8) -> SessionKey {
    SessionKey {
        node: node(seed),
        incarnation: incarnation(seed),
        session: RouterSessionId::new(1000 + u128::from(seed)),
    }
}

/// Start from a machine that has taken leadership over an empty slot.
fn leader(supported: &[u16]) -> RouterMachine<SequentialIds> {
    let adapter = NothingObsolete;
    let mut machine = machine(9, supported);
    let mut out = Vec::new();
    machine.on_event(RouterEvent::ConsiderTakeover, &adapter, &mut out);
    confirm_claims(&mut machine, &out, &adapter);
    assert!(machine.is_routing(), "leader fixture must hold leadership");
    machine
}

#[test]
fn capacity_counts_origin_attempts_and_offers_without_double_counting_matches() {
    let mut machine = leader(&[1]);
    register_session(&mut machine, session(3), 1, &["gpu"], 2, vec![
        ActiveAttemptSummary {
            task_id: derived_task("origin"),
            attempt_id: PoolAttemptId::new(30),
            allocation: None,
        },
    ]);
    let actions = feed(&mut machine, RouterEvent::TaskObserved {
        ticket: Box::new(ticket("G1")),
    }, &NothingObsolete);
    let [RouterAction::SendOffer { allocation_id, .. }] = actions.as_slice() else {
        panic!("one slot must remain available: {actions:?}");
    };
    let allocation_id = *allocation_id;
    let blocked = feed(&mut machine, RouterEvent::TaskObserved {
        ticket: Box::new(ticket("G2")),
    }, &NothingObsolete);
    assert!(blocked.is_empty(), "origin plus offer exhausts capacity: {blocked:?}");

    // Reconciliation drops the finished origin and reports the offered attempt.
    // Its matching allocation and attempt are one obligation, freeing one slot.
    let resumed = register_session(&mut machine, session(3), 1, &["gpu"], 2, vec![
        ActiveAttemptSummary {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(31),
            allocation: Some(allocation_id),
        },
    ]);
    assert!(matches!(resumed.as_slice(), [RouterAction::SendOffer { task_id, .. }]
        if *task_id == derived_task("G2")));
}

#[test]
fn restart_claim_advances_the_retained_own_writer_lane() {
    let mut machine = machine(9, &[1]);
    let mut retained = claim(9, 5, 1, 77);
    retained.writer_seq = 42;
    feed(&mut machine, RouterEvent::SlotMerged { claims: vec![retained] }, &NothingObsolete);
    let actions = feed(&mut machine, RouterEvent::ConsiderTakeover, &NothingObsolete);
    let [RouterAction::PersistClaim { claim }] = actions.as_slice() else {
        panic!("restart must publish a fresh lane: {actions:?}");
    };
    assert_eq!(claim.writer_seq, 43);
    assert_eq!(claim.generation, 6);
    assert!(!machine.is_routing());
    confirm_claims(&mut machine, &actions, &NothingObsolete);
    assert!(machine.is_routing());
}

#[test]
fn pending_offer_is_distinct_from_origin_but_accepted_running_event_overlaps() {
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 2);
    let offer = feed(&mut machine, RouterEvent::TaskObserved {
        ticket: Box::new(ticket("G1")),
    }, &NothingObsolete);
    let [RouterAction::SendOffer { allocation_id, .. }] = offer.as_slice() else {
        panic!("expected first offer: {offer:?}");
    };
    let allocation_id = *allocation_id;
    register_session(&mut machine, session(3), 1, &["gpu"], 2, vec![
        ActiveAttemptSummary {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(31),
            allocation: None,
        },
    ]);
    let blocked = feed(&mut machine, RouterEvent::TaskObserved {
        ticket: Box::new(ticket("G2")),
    }, &NothingObsolete);
    assert!(blocked.is_empty(), "origin and unaccepted offer owe separate slots");
    let started = feed(&mut machine, RouterEvent::OfferAccepted {
        session: session(3), allocation_id,
        attempt_id: PoolAttemptId::new(31),
    }, &NothingObsolete);
    assert!(matches!(started.as_slice(), [RouterAction::StartAttempt { .. }]));
    feed(&mut machine, RouterEvent::AttemptChanged {
        session: session(3),
        task_id: derived_task("G1"),
        attempt_id: PoolAttemptId::new(31),
        state: AttemptState::Running,
    }, &NothingObsolete);
    let resumed = feed(&mut machine, RouterEvent::RoutingTick, &NothingObsolete);
    assert!(matches!(resumed.as_slice(), [RouterAction::SendOffer { task_id, .. }]
        if *task_id == derived_task("G2")));
}

#[test]
fn failed_claim_allows_retry_and_fences_delayed_persistence_results() {
    let mut machine = machine(9, &[1]);
    feed(&mut machine, RouterEvent::ConsiderTakeover, &NothingObsolete);
    let failed = machine.own_claim().unwrap().clone();
    feed(&mut machine, RouterEvent::HeartbeatObserved {
        claim: failed.liveness_key(),
    }, &NothingObsolete);
    feed(&mut machine, RouterEvent::ClaimPersistenceFailed {
        generation: failed.generation, claim_id: failed.claim_id,
    }, &NothingObsolete);
    assert!(!machine.is_routing());
    assert!(machine.own_claim().is_none());
    feed(&mut machine, RouterEvent::ClaimPersisted {
        generation: failed.generation, claim_id: failed.claim_id,
    }, &NothingObsolete);
    assert!(!machine.is_routing(), "a late acknowledgment must not revive a failed claim");

    let retry = feed(&mut machine, RouterEvent::ConsiderTakeover, &NothingObsolete);
    let [RouterAction::PersistClaim { claim }] = retry.as_slice() else {
        panic!("failed self-liveness must not block retry: {retry:?}");
    };
    let pending = claim.clone();
    assert!(pending.generation > failed.generation);
    assert!(pending.writer_seq > failed.writer_seq);
    feed(&mut machine, RouterEvent::ClaimPersisted {
        generation: failed.generation, claim_id: failed.claim_id,
    }, &NothingObsolete);
    feed(&mut machine, RouterEvent::ClaimPersistenceFailed {
        generation: failed.generation, claim_id: failed.claim_id,
    }, &NothingObsolete);
    assert_eq!(machine.own_claim(), Some(&pending));
    assert!(!machine.is_routing(), "stale success cannot confirm the retry");
    confirm_claims(&mut machine, &retry, &NothingObsolete);
    assert!(machine.is_routing());
    feed(&mut machine, RouterEvent::ClaimPersistenceFailed {
        generation: pending.generation, claim_id: pending.claim_id,
    }, &NothingObsolete);
    assert!(machine.is_routing(), "a delayed failure cannot undo a confirmed write");
}

#[test]
fn takeover_after_losing_confirmed_leadership_requires_a_new_write() {
    let mut machine = leader(&[1]);
    let remote = claim(2, 2, 1, 100);
    feed(&mut machine, RouterEvent::SlotMerged { claims: vec![remote.clone()] }, &NothingObsolete);
    feed(&mut machine, RouterEvent::HeartbeatObserved { claim: remote.liveness_key() }, &NothingObsolete);
    assert!(!machine.is_routing());
    feed(&mut machine, RouterEvent::HeartbeatExpired { claim: remote.liveness_key() }, &NothingObsolete);
    let actions = feed(&mut machine, RouterEvent::ConsiderTakeover, &NothingObsolete);
    assert!(matches!(actions.as_slice(), [RouterAction::PersistClaim { .. }]));
    feed(&mut machine, RouterEvent::SlotMerged { claims: vec![] }, &NothingObsolete);
    assert!(!machine.is_routing(), "the previous claim's acknowledgment must not carry over");
    confirm_claims(&mut machine, &actions, &NothingObsolete);
    assert!(machine.is_routing());
}

/// A live winner keeps a candidate from claiming: takeover waits for the
/// heartbeat to expire rather than starting a second election.
#[test]
fn takeover_waits_for_a_live_winner_and_proceeds_once_it_expires() {
    let adapter = NothingObsolete;
    let mut machine = machine(9, &[1]);
    let remote = claim(1, 1, 1, 100);

    feed(
        &mut machine,
        RouterEvent::SlotMerged {
            claims: vec![remote.clone()],
        },
        &adapter,
    );
    feed(
        &mut machine,
        RouterEvent::HeartbeatObserved {
            claim: remote.liveness_key(),
        },
        &adapter,
    );

    let waiting = feed(&mut machine, RouterEvent::ConsiderTakeover, &adapter);
    assert!(
        waiting.is_empty(),
        "a live winner must not be pre-empted: {waiting:?}"
    );
    assert!(!machine.is_routing());

    feed(
        &mut machine,
        RouterEvent::HeartbeatExpired {
            claim: remote.liveness_key(),
        },
        &adapter,
    );
    let takeover = feed(&mut machine, RouterEvent::ConsiderTakeover, &adapter);
    let RouterAction::PersistClaim { claim } = &takeover[0] else {
        panic!("expected a claim after the winner expired: {takeover:?}");
    };
    assert_eq!(claim.generation, 2, "takeover claims the next generation");

    confirm_claims(&mut machine, &takeover, &adapter);
    assert!(machine.is_routing(), "the fresh claim becomes the projection");
}

/// One shared election per pool: among live claims at the greatest generation
/// the newer scheduling protocol is preferred, and the older node stands down
/// instead of forming a version-specific cohort. A dead newer claim does not
/// keep the older node down.
#[test]
fn newer_live_scheduling_protocol_wins_and_a_dead_one_stops_blocking() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    let own_generation = machine.own_claim().expect("own claim").generation;

    let newer = claim(2, own_generation, 2, 200);
    feed(
        &mut machine,
        RouterEvent::SlotMerged {
            claims: vec![newer.clone()],
        },
        &adapter,
    );
    feed(
        &mut machine,
        RouterEvent::HeartbeatObserved {
            claim: newer.liveness_key(),
        },
        &adapter,
    );
    assert!(
        !machine.is_routing(),
        "a live newer-protocol claim must defeat the older node"
    );

    feed(
        &mut machine,
        RouterEvent::HeartbeatExpired {
            claim: newer.liveness_key(),
        },
        &adapter,
    );
    assert!(
        machine.is_routing(),
        "a dead newer-version historical claim must not block the live older node"
    );
}

/// Leadership is not granted until the driver confirms the durable claim write:
/// a heartbeat observed in between must not start routing.
#[test]
fn leadership_requires_the_confirmed_claim_write() {
    let adapter = NothingObsolete;
    let mut machine = machine(9, &[1]);
    let mut out = Vec::new();
    machine.on_event(RouterEvent::ConsiderTakeover, &adapter, &mut out);
    let own = machine.own_claim().expect("claim").liveness_key();
    assert!(!machine.is_routing(), "an unconfirmed claim is not leadership");

    // A heartbeat for this machine's own claim arrives before the write confirms.
    feed(
        &mut machine,
        RouterEvent::HeartbeatObserved { claim: own },
        &adapter,
    );
    assert!(
        !machine.is_routing(),
        "an unconfirmed claim must not route, even when its heartbeat is live"
    );

    // Only the confirmation makes it the router.
    let generation = machine.own_claim().expect("claim").generation;
    let claim_id = machine.own_claim().expect("claim").claim_id;
    feed(
        &mut machine,
        RouterEvent::ClaimPersisted {
            generation,
            claim_id,
        },
        &adapter,
    );
    assert!(machine.is_routing(), "the confirmed claim takes leadership");
}

/// A session that cannot speak the selected router's protocol is rejected and
/// registers nothing; it waits rather than participating in a second election.
#[test]
fn incompatible_session_is_rejected_rather_than_forming_a_cohort() {
    let mut machine = leader(&[1]);
    let actions = register(&mut machine, 4, 2, &[], 1);
    assert_eq!(
        actions,
        vec![RouterAction::RejectSession {
            session: session(4),
            reason: DeclineReason::UnsupportedProtocol,
        }]
    );

    // A compatible session on the same node is accepted and can be offered work.
    let accepted = register(&mut machine, 4, 1, &[], 1);
    assert!(accepted.is_empty(), "a compatible session registers quietly");
}

/// A pending ticket is offered to a registered, capable executor, and the offer
/// is identical whether the executor registered before or after the ticket was
/// observed.
#[test]
fn offer_is_identical_in_both_arrival_orderings() {
    let adapter = NothingObsolete;
    let declaration = declaration("G1");

    let mut executor_first = leader(&[1]);
    register(&mut executor_first, 3, 1, &["gpu"], 1);
    let offer_after = feed(
        &mut executor_first,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );

    let mut ticket_first = leader(&[1]);
    let pending = feed(
        &mut ticket_first,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    assert!(pending.is_empty(), "nothing to offer before a session exists");
    let offer_before = register(&mut ticket_first, 3, 1, &["gpu"], 1);

    assert_eq!(offer_after, offer_before);
    assert_eq!(
        offer_after,
        vec![RouterAction::SendOffer {
            session: session(3),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration),
        }]
    );
}

/// An executor that cannot satisfy the task's capabilities is not offered the
/// task, but a capable one still is.
#[test]
fn incapable_sessions_are_not_offered() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["web"], 1);
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    assert!(actions.is_empty(), "an incapable executor must not be offered work");

    let actions = register(&mut machine, 5, 1, &["gpu"], 1);
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], RouterAction::SendOffer { .. }));
}

/// Registration recovery: a surviving executor introduces an origin fast-path
/// attempt, and the router must not offer that task again after takeover.
#[test]
fn registration_with_an_active_origin_attempt_prevents_duplicate_offers() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register_session(
        &mut machine,
        session(3),
        1,
        &["gpu"],
        2,
        vec![ActiveAttemptSummary {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(7),
            allocation: None,
        }],
    );
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    assert!(
        actions.is_empty(),
        "a task already running under an origin attempt must not be re-offered: {actions:?}"
    );
}

/// Capacity bounds concurrent offers per session.
#[test]
fn capacity_bounds_concurrent_offers() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);

    let first = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    assert_eq!(first.len(), 1);

    let second = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G2")),
        },
        &adapter,
    );
    assert!(
        second.is_empty(),
        "a full session must not receive another concurrent offer: {second:?}"
    );
}

/// Placement `Only` restricts offerings to the named node.
#[test]
fn placement_only_restricts_offer_recipients() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    register(&mut machine, 5, 1, &["gpu"], 1);

    let mut only = ticket("G1");
    only.declaration.declaration.placement = Preference::Only(node(5));
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(only),
        },
        &adapter,
    );
    assert_eq!(
        actions,
        vec![RouterAction::SendOffer {
            session: session(5),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
            declaration: {
                let mut declaration = declaration("G1");
                declaration.placement = Preference::Only(node(5));
                Box::new(declaration)
            },
        }]
    );
}

/// Accepting an offer is what authorizes the start, and only for the session it
/// was offered to.
#[test]
fn acceptance_starts_the_offered_allocation() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    let offers = register(&mut machine, 3, 1, &["gpu"], 1);
    let RouterAction::SendOffer {
        allocation_id, ..
    } = offers[0]
    else {
        panic!("expected an offer");
    };

    let actions = feed(
        &mut machine,
        RouterEvent::OfferAccepted {
            session: session(3),
            allocation_id,
            attempt_id: PoolAttemptId::new(1),
        },
        &adapter,
    );
    assert_eq!(
        actions,
        vec![RouterAction::StartAttempt {
            session: session(3),
            allocation_id,
            task_id: derived_task("G1"),
        }]
    );
}

/// A router that has lost the election does not authorize starts: a late
/// acceptance for its own offer is ignored, because the winner now owns
/// allocation.
#[test]
fn a_lost_router_does_not_authorize_starts() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    let offers = register(&mut machine, 3, 1, &["gpu"], 1);
    let RouterAction::SendOffer {
        allocation_id, ..
    } = offers[0]
    else {
        panic!("expected an offer");
    };
    let own_generation = machine.own_claim().expect("own claim").generation;

    // A live newer-protocol claim at the same generation defeats this router.
    let newer = claim(2, own_generation, 2, 400);
    feed(
        &mut machine,
        RouterEvent::SlotMerged {
            claims: vec![newer.clone()],
        },
        &adapter,
    );
    feed(
        &mut machine,
        RouterEvent::HeartbeatObserved {
            claim: newer.liveness_key(),
        },
        &adapter,
    );
    assert!(!machine.is_routing());

    let actions = feed(
        &mut machine,
        RouterEvent::OfferAccepted {
            session: session(3),
            allocation_id,
            attempt_id: PoolAttemptId::new(1),
        },
        &adapter,
    );
    assert!(
        actions.is_empty(),
        "a router that lost the election must not start the task: {actions:?}"
    );
}

/// A decision for a session that no longer serves the router is ignored: after a
/// reconnect, the old allocation cannot start a task the new session owns.
#[test]
fn stale_session_responses_are_fenced() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    let first_offer = register(&mut machine, 3, 1, &["gpu"], 1);
    let RouterAction::SendOffer {
        allocation_id: old_allocation,
        ..
    } = first_offer[0]
    else {
        panic!("expected an offer");
    };

    // Reconnect: the old connection dies and the same node *and incarnation*
    // opens a new connection. Node plus incarnation cannot tell these apart, so
    // the session id is what fences the old connection's messages.
    feed(
        &mut machine,
        RouterEvent::SessionClosed {
            session: session(3),
        },
        &adapter,
    );
    let new_session = reconnect(3);
    let out = register_session(&mut machine, new_session, 1, &["gpu"], 1, Vec::new());
    let RouterAction::SendOffer {
        allocation_id: new_allocation,
        ..
    } = out[0]
    else {
        panic!("expected a re-offer to the new session");
    };
    assert_ne!(old_allocation, new_allocation);

    // A late acceptance from the dead session must not start anything.
    let actions = feed(
        &mut machine,
        RouterEvent::OfferAccepted {
            session: session(3),
            allocation_id: old_allocation,
            attempt_id: PoolAttemptId::new(1),
        },
        &adapter,
    );
    assert!(
        actions.is_empty(),
        "a dead session's acceptance must be ignored: {actions:?}"
    );

    // The live session's acceptance still starts the task.
    let actions = feed(
        &mut machine,
        RouterEvent::OfferAccepted {
            session: new_session,
            allocation_id: new_allocation,
            attempt_id: PoolAttemptId::new(2),
        },
        &adapter,
    );
    assert_eq!(
        actions,
        vec![RouterAction::StartAttempt {
            session: new_session,
            allocation_id: new_allocation,
            task_id: derived_task("G1"),
        }]
    );
}

/// A `NotReady` decline blocks only that executor and keeps the task pending; the
/// task remains offerable to a different executor.
#[test]
fn not_ready_decline_blocks_one_executor_and_keeps_the_task_pending() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    register(&mut machine, 5, 1, &["gpu"], 1);
    let offers = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    let RouterAction::SendOffer {
        session: offered_session,
        allocation_id,
        ..
    } = offers[0]
    else {
        panic!("expected an offer");
    };
    let other_session = if offered_session == session(3) {
        session(5)
    } else {
        session(3)
    };

    let after_not_ready = feed(
        &mut machine,
        RouterEvent::OfferDeclined {
            session: offered_session,
            allocation_id,
            reason: DeclineReason::NotReady,
        },
        &adapter,
    );
    assert_eq!(
        after_not_ready,
        vec![RouterAction::SendOffer {
            session: other_session,
            allocation_id: AllocationId::new(2),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
        }],
        "a NotReady decline must leave the task pending for another executor"
    );

    // The blocked executor becomes eligible again on a readiness wakeup, while
    // the executor that just got the offer is still holding it.
    let wake = feed(
        &mut machine,
        RouterEvent::ReadinessWakeup {
            session: offered_session,
            task_id: derived_task("G1"),
        },
        &adapter,
    );
    assert!(
        wake.is_empty(),
        "the task is already allocated, so a wakeup must not double-offer: {wake:?}"
    );
}

/// Ticket removal cancels a live allocation and stops further offers.
#[test]
fn removal_cancels_live_attempts() {
    let declaration = declaration("G1");

    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &NothingObsolete,
    );

    let actions = feed(
        &mut machine,
        RouterEvent::TaskRemoved {
            task_id: declaration.task_id,
        },
        &NothingObsolete,
    );
    assert_eq!(
        actions,
        vec![RouterAction::CancelAttempt {
            session: session(3),
            task_id: declaration.task_id,
            reason: OpaqueReason::from_label("task no longer scheduled"),
        }]
    );

    // The removed task is no longer pending, so a later tick offers nothing.
    let actions = feed(&mut machine, RouterEvent::RoutingTick, &NothingObsolete);
    assert!(actions.is_empty(), "a removed ticket must not be re-offered");
}

/// The router does not offer a ticket its own adapter reports as provably
/// obsolete, and it does offer one that is merely not locally materializable
/// (the adapter says nothing about it).
#[test]
fn proven_obsolescence_stops_offers_but_local_unreadiness_does_not() {
    let declaration = declaration("G1");
    let obsolete = ObsoleteTasks(HashSet::from([declaration.task_id]));

    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &obsolete,
    );
    assert!(
        actions.is_empty(),
        "proven obsolescence must stop the offer: {actions:?}"
    );

    // The same router with no proven obsolescence offers the task, because a
    // remote executor may hold the inputs this node lacks.
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &NothingObsolete,
    );
    assert_eq!(actions.len(), 1);
    assert!(matches!(actions[0], RouterAction::SendOffer { .. }));
}

/// A ticket that already carries a terminal fact is never offered; if it was
/// allocated, the attempt is cancelled.
#[test]
fn terminal_ticket_is_not_offered() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);

    let mut terminal = ticket("G1");
    terminal
        .terminal
        .insert(node(1), terminal_lane(node(1), 1, success_fact(4, "report://z")));
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(terminal),
        },
        &adapter,
    );
    assert!(actions.is_empty(), "a terminal ticket must not be offered");
}

/// A slot merge from another pool is rejected rather than mixed into this
/// pool's election.
#[test]
fn foreign_pool_claims_are_rejected() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    let mut foreign = claim(2, 5, 1, 300);
    foreign.pool = TaskPoolId::from_label("agent-background");

    let actions = feed(
        &mut machine,
        RouterEvent::SlotMerged {
            claims: vec![foreign.clone()],
        },
        &adapter,
    );
    assert_eq!(
        actions,
        vec![RouterAction::RejectClaim {
            collision: Box::new(ClaimCollision::ForeignPool {
                pool: TaskPoolId::from_label("agent-background"),
            }),
        }]
    );
    assert!(
        machine.is_routing(),
        "a foreign claim must not displace this pool's projection"
    );
}

/// Attempt progress updates are tracked per session and per task, and a closed
/// session's updates are dropped.
#[test]
fn attempt_changes_track_running_work_and_release_it_on_finish() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);

    feed(
        &mut machine,
        RouterEvent::AttemptChanged {
            session: session(3),
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            state: AttemptState::Running,
        },
        &adapter,
    );
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    assert!(
        actions.is_empty(),
        "a task reported running by its executor must not be offered: {actions:?}"
    );

    let actions = feed(
        &mut machine,
        RouterEvent::AttemptChanged {
            session: session(3),
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            state: AttemptState::Failed,
        },
        &adapter,
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, RouterAction::SendOffer { .. })),
        "a failed attempt frees the task immediately: {actions:?}"
    );
}

/// A finish report for a superseded attempt does not clear the attempt the
/// router currently tracks, so capacity is not freed while the live attempt runs.
#[test]
fn a_stale_finish_does_not_clear_the_live_attempt() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(
        &mut machine,
        RouterEvent::AttemptChanged {
            session: session(3),
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(7),
            state: AttemptState::Running,
        },
        &adapter,
    );

    let actions = feed(
        &mut machine,
        RouterEvent::AttemptChanged {
            session: session(3),
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            state: AttemptState::Succeeded,
        },
        &adapter,
    );
    assert!(
        actions.is_empty(),
        "a stale finish must not free the live attempt: {actions:?}"
    );

    // The live attempt is still tracked, so the task is not offered.
    let actions = feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    assert!(actions.is_empty(), "the live attempt still holds the task");
}

/// A remote `Obsolete` decline is that executor's view, not router-local proof,
/// so the task stays pending and is offered to another executor.
#[test]
fn a_remote_obsolete_decline_does_not_retire_the_task() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    // One capable executor only, so the allocation is deterministic.
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(
        &mut machine,
        RouterEvent::TaskObserved {
            ticket: Box::new(ticket("G1")),
        },
        &adapter,
    );
    let offers = feed(&mut machine, RouterEvent::RoutingTick, &adapter);
    assert!(offers.is_empty(), "the task is already allocated");

    let actions = feed(
        &mut machine,
        RouterEvent::OfferDeclined {
            session: session(3),
            allocation_id: AllocationId::new(1),
            reason: DeclineReason::Obsolete,
        },
        &adapter,
    );
    // The only other executor is the same one, which is now blocked for this
    // task, so nothing is immediately re-offered...
    assert!(actions.is_empty(), "no other executor is available: {actions:?}");

    // ...but a different executor that registers later is still offered the task,
    // proving the remote decline removed nothing.
    let actions = register(&mut machine, 5, 1, &["gpu"], 1);
    assert_eq!(
        actions,
        vec![RouterAction::SendOffer {
            session: session(5),
            allocation_id: AllocationId::new(2),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
        }],
        "a remote obsolete decline must leave the obligation schedulable"
    );
}

#[test]
fn unavailable_retries_only_after_authoritative_capacity_change() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) }, &adapter);
    assert!(feed(&mut machine, RouterEvent::OfferDeclined {
        session: session(3), allocation_id: AllocationId::new(1), reason: DeclineReason::Unavailable,
    }, &adapter).is_empty());
    assert!(feed(&mut machine, RouterEvent::RoutingTick, &adapter).is_empty());
    assert!(register(&mut machine, 3, 1, &["gpu"], 1).is_empty());
    assert!(feed(&mut machine, RouterEvent::ReadinessWakeup {
        session: session(3), task_id: derived_task("G1"),
    }, &adapter).is_empty());
    let actions = register(&mut machine, 3, 1, &["gpu"], 2);
    assert_eq!(actions, vec![RouterAction::SendOffer {
        session: session(3), allocation_id: AllocationId::new(2),
        task_id: derived_task("G1"), declaration: Box::new(declaration("G1")),
    }]);
}

#[test]
fn closed_session_wakeup_cannot_unblock_replacement() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) }, &adapter);
    feed(&mut machine, RouterEvent::OfferDeclined {
        session: session(3), allocation_id: AllocationId::new(1), reason: DeclineReason::NotReady,
    }, &adapter);
    feed(&mut machine, RouterEvent::SessionClosed { session: session(3) }, &adapter);
    register_session(&mut machine, reconnect(3), 1, &["gpu"], 1, vec![]);
    feed(&mut machine, RouterEvent::OfferDeclined {
        session: reconnect(3), allocation_id: AllocationId::new(2), reason: DeclineReason::NotReady,
    }, &adapter);
    assert!(feed(&mut machine, RouterEvent::ReadinessWakeup {
        session: session(3), task_id: derived_task("G1"),
    }, &adapter).is_empty());
    assert_eq!(feed(&mut machine, RouterEvent::ReadinessWakeup {
        session: reconnect(3), task_id: derived_task("G1"),
    }, &adapter), vec![RouterAction::SendOffer {
        session: reconnect(3), allocation_id: AllocationId::new(3),
        task_id: derived_task("G1"), declaration: Box::new(declaration("G1")),
    }]);
}

#[test]
fn exact_attempt_release_recovers_capacity_but_not_readiness_blocks() {
    for reason in [DeclineReason::Unavailable, DeclineReason::NotReady, DeclineReason::CoordinationIncomplete] {
        let adapter = NothingObsolete;
        let mut machine = leader(&[1]);
        register_session(&mut machine, session(3), 1, &["gpu"], 2, vec![
            ActiveAttemptSummary { task_id: derived_task("origin"),
                attempt_id: PoolAttemptId::new(30), allocation: None },
        ]);
        feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) }, &adapter);
        feed(&mut machine, RouterEvent::OfferDeclined {
            session: session(3), allocation_id: AllocationId::new(1), reason,
        }, &adapter);
        assert!(feed(&mut machine, RouterEvent::AttemptChanged {
            session: session(3), task_id: derived_task("origin"),
            attempt_id: PoolAttemptId::new(29), state: AttemptState::Failed,
        }, &adapter).is_empty());
        let released = feed(&mut machine, RouterEvent::AttemptChanged {
            session: session(3), task_id: derived_task("origin"),
            attempt_id: PoolAttemptId::new(30), state: AttemptState::Failed,
        }, &adapter);
        if reason == DeclineReason::Unavailable {
            assert_eq!(released, vec![RouterAction::SendOffer {
                session: session(3), allocation_id: AllocationId::new(2),
                task_id: derived_task("G1"), declaration: Box::new(declaration("G1")),
            }]);
        } else {
            assert!(released.is_empty());
        }
    }
}

#[test]
fn readiness_wakeup_releases_only_senders_task() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) }, &adapter);
    feed(&mut machine, RouterEvent::OfferDeclined {
        session: session(3), allocation_id: AllocationId::new(1), reason: DeclineReason::NotReady,
    }, &adapter);
    register(&mut machine, 5, 1, &["gpu"], 1);
    feed(&mut machine, RouterEvent::OfferDeclined {
        session: session(5), allocation_id: AllocationId::new(2), reason: DeclineReason::CoordinationIncomplete,
    }, &adapter);
    assert!(feed(&mut machine, RouterEvent::ReadinessWakeup {
        session: session(3), task_id: derived_task("unrelated"),
    }, &adapter).is_empty());
    assert_eq!(feed(&mut machine, RouterEvent::ReadinessWakeup {
        session: session(5), task_id: derived_task("G1"),
    }, &adapter), vec![RouterAction::SendOffer {
        session: session(5), allocation_id: AllocationId::new(3),
        task_id: derived_task("G1"), declaration: Box::new(declaration("G1")),
    }]);
}

#[test]
fn cancellation_deduplicates_allocated_and_registered_ownership() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) }, &adapter);
    feed(&mut machine, RouterEvent::OfferAccepted {
        session: session(3), allocation_id: AllocationId::new(1), attempt_id: PoolAttemptId::new(1),
    }, &adapter);
    feed(&mut machine, RouterEvent::AttemptChanged {
        session: session(3), task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
        state: AttemptState::Running,
    }, &adapter);
    assert_eq!(feed(&mut machine, RouterEvent::TaskRemoved { task_id: derived_task("G1") },
        &adapter), vec![RouterAction::CancelAttempt {
            session: session(3), task_id: derived_task("G1"),
            reason: OpaqueReason::from_label("task no longer scheduled"),
        }]);
    assert!(feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &adapter).is_empty());
    assert_eq!(feed(&mut machine, RouterEvent::AttemptChanged {
        session: session(3), task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
        state: AttemptState::Cancelled,
    }, &adapter), vec![RouterAction::SendOffer {
        session: session(3), allocation_id: AllocationId::new(2),
        task_id: derived_task("G2"), declaration: Box::new(declaration("G2")),
    }]);
}

#[test]
fn delayed_decline_cannot_release_an_accepted_attempt() {
    let adapter = NothingObsolete;
    let mut machine = leader(&[1]);
    register(&mut machine, 3, 1, &["gpu"], 1);
    feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) }, &adapter);
    feed(&mut machine, RouterEvent::OfferAccepted {
        session: session(3), allocation_id: AllocationId::new(1), attempt_id: PoolAttemptId::new(1),
    }, &adapter);
    assert!(feed(&mut machine, RouterEvent::OfferDeclined {
        session: session(3), allocation_id: AllocationId::new(1), reason: DeclineReason::Unavailable,
    }, &adapter).is_empty());
    feed(&mut machine, RouterEvent::TaskRemoved { task_id: derived_task("G1") }, &adapter);
    assert!(feed(&mut machine, RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &adapter).is_empty());
    assert!(feed(&mut machine, RouterEvent::AttemptChanged {
        session: session(3), task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(99),
        state: AttemptState::Cancelled,
    }, &adapter).is_empty(), "unreported accepted ownership still fences the exact attempt id");
    assert_eq!(feed(&mut machine, RouterEvent::AttemptChanged {
        session: session(3), task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
        state: AttemptState::Cancelled,
    }, &adapter), vec![RouterAction::SendOffer {
        session: session(3), allocation_id: AllocationId::new(2),
        task_id: derived_task("G2"), declaration: Box::new(declaration("G2")),
    }]);
}
