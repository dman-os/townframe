//! Behavioural tests for the sans-I/O executor machine.
//!
//! The contracts under test are the ones a driver depends on: persist-before-
//! start ordering, cancellation winning that ordering, persistence-failure
//! handling, origin fast-path durability, disconnect/reconnect re-announcement,
//! stale-session and stale-attempt fencing, and executor-authoritative readiness.

use super::super::test_util::*;
use super::*;
use crate::tasks::{
    AllocationId, AttemptState, Capacity, DeclineReason, DomainCoordinationRef, OpaqueReason,
    OpaqueResultRef, PoolAttemptId, PoolClaimId, RouterClaim, RouterSessionId,
    TaskClassification, TaskPoolId,
};

/// A domain adapter whose readiness and obsolescence are set explicitly,
/// standing in for a real per-node project/triage adapter without mocking the
/// machine.
struct FakeDomain {
    not_ready: HashSet<PoolTaskId>,
    obsolete: HashSet<PoolTaskId>,
}

impl FakeDomain {
    fn ready() -> Self {
        Self {
            not_ready: HashSet::new(),
            obsolete: HashSet::new(),
        }
    }
}

impl WorkerDomainAdapter for FakeDomain {
    fn classify(&mut self, ticket: &TaskTicket) -> TaskClassification {
        if self.obsolete.contains(&ticket.task_id) {
            return TaskClassification::Obsolete;
        }
        if self.not_ready.contains(&ticket.task_id) {
            return TaskClassification::NotReady(ReadinessWatch::new(
                DomainCoordinationRef::from_label("photo-7"),
            ));
        }
        TaskClassification::Runnable(crate::tasks::ResolvedInvocation {
            args: ticket.declaration.declaration.input.clone(),
        })
    }
}

fn worker_machine(capacity: u32, supported: &[u16]) -> PoolWorkerMachine<SequentialIds> {
    PoolWorkerMachine::new(
        WorkerConfig {
            pool: pool(),
            node: node(3),
            incarnation: incarnation(3),
            capabilities: CapabilitySummary::from_labels(["gpu"]),
            capacity: Capacity::new(capacity),
            supported_protocols: supported
                .iter()
                .copied()
                .map(SchedulingProtocolVersion)
                .collect(),
        },
        ids(),
    )
}

fn router_claim(protocol: u16) -> RouterClaim {
    RouterClaim {
        pool: pool(),
        candidate: node(1),
        incarnation: incarnation(1),
        writer_seq: 1,
        generation: 1,
        claim_id: PoolClaimId::new(1),
        scheduling_protocol: SchedulingProtocolVersion(protocol),
        observed: BTreeMap::new(),
    }
}

fn session() -> SessionKey {
    SessionKey {
        node: node(1),
        incarnation: incarnation(1),
        session: RouterSessionId::new(1),
    }
}

/// A different connection from the same router node and incarnation.
fn other_session() -> SessionKey {
    SessionKey {
        node: node(1),
        incarnation: incarnation(1),
        session: RouterSessionId::new(2),
    }
}

fn feed<Adapter: WorkerDomainAdapter>(
    machine: &mut PoolWorkerMachine<SequentialIds>,
    event: WorkerEvent,
    adapter: &mut Adapter,
) -> Vec<WorkerAction> {
    let mut out = Vec::new();
    machine.on_event(event, adapter, &mut out);
    out
}

/// Discovers a router and returns the registration the driver would send.
fn connect(
    machine: &mut PoolWorkerMachine<SequentialIds>,
    adapter: &mut FakeDomain,
) -> WorkerRegistration {
    let actions = feed(
        machine,
        WorkerEvent::RouteDiscovered {
            claim: Box::new(router_claim(1)),
            session: session(),
        },
        adapter,
    );
    let WorkerAction::RegisterSession { registration, .. } = &actions[0] else {
        panic!("expected a registration action");
    };
    registration.clone()
}

/// Offer `G1` on the live session and return the emitted actions.
fn offer_g1(
    machine: &mut PoolWorkerMachine<SequentialIds>,
    adapter: &mut FakeDomain,
) -> Vec<WorkerAction> {
    feed(
        machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
            ticket: Some(Box::new(ticket("G1"))),
        },
        adapter,
    )
}

/// A terminal ticket observed repeatedly is handed to the domain once, not once
/// per replication.
#[test]
fn repeated_terminal_observation_is_handed_over_once() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    let mut terminal = ticket("G1");
    terminal.terminal.insert(
        node(2),
        terminal_lane(node(2), 1, success_fact(8, "report://done")),
    );
    let first = feed(
        &mut machine,
        WorkerEvent::TicketObserved {
            ticket: Box::new(terminal.clone()),
        },
        &mut adapter,
    );
    assert_eq!(first.len(), 1, "the first observation reports the fact");
    let second = feed(
        &mut machine,
        WorkerEvent::TicketObserved {
            ticket: Box::new(terminal),
        },
        &mut adapter,
    );
    assert!(
        second.is_empty(),
        "a repeated observation must not report the fact again: {second:?}"
    );
}

#[test]
fn observed_cancellation_then_success_delivers_the_monotonic_update_once() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    let mut cancelled = ticket("G1");
    cancelled.terminal.insert(node(2), terminal_lane(node(2), 1, cancelled_fact("cancel")));
    let first = feed(&mut machine, WorkerEvent::TicketObserved {
        ticket: Box::new(cancelled.clone()),
    }, &mut adapter);
    assert!(matches!(first.as_slice(), [WorkerAction::DomainTerminalFact {
        summary: TerminalSummary::Cancelled { .. }, ..
    }]));
    let mut success = ticket("G1");
    success.terminal.insert(node(2), terminal_lane(node(2), 2, success_fact(8, "report://done")));
    let mut merged = cancelled.clone();
    merged.merge(&success).unwrap();
    let update = feed(&mut machine, WorkerEvent::TicketObserved {
        ticket: Box::new(merged.clone()),
    }, &mut adapter);
    assert!(matches!(update.as_slice(), [WorkerAction::DomainTerminalFact {
        summary: TerminalSummary::Succeeded { attempt_id, .. }, ..
    }] if *attempt_id == PoolAttemptId::new(8)));
    for observed in [merged, cancelled] {
        let replay = feed(&mut machine, WorkerEvent::TicketObserved {
            ticket: Box::new(observed),
        }, &mut adapter);
        assert!(replay.is_empty(), "duplicate success or stale cancellation is inert");
    }
}

#[test]
fn remote_success_advances_a_pending_or_accepted_local_cancellation() {
    for accepted in [false, true] {
        let mut adapter = FakeDomain::ready();
        let mut machine = worker_machine(1, &[1]);
        connect(&mut machine, &mut adapter);
        ack_running_attempt(&mut machine, &mut adapter);
        feed(&mut machine, WorkerEvent::AttemptFinished {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            fact: cancelled_fact("local cancel"),
        }, &mut adapter);
        if accepted {
            feed(&mut machine, WorkerEvent::TerminalFactAccepted {
                task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
            }, &mut adapter);
        }
        let mut success = ticket("G1");
        success.terminal.insert(node(2), terminal_lane(node(2), 2, success_fact(8, "report://done")));
        let update = feed(&mut machine, WorkerEvent::TicketObserved {
            ticket: Box::new(success.clone()),
        }, &mut adapter);
        assert_eq!(update.iter().filter(|action| matches!(action,
            WorkerAction::DomainTerminalFact { summary: TerminalSummary::Succeeded { .. }, .. }
        )).count(), 1, "success must advance local cancellation, accepted={accepted}");
        feed(&mut machine, WorkerEvent::TerminalFactAccepted {
            task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
        }, &mut adapter);
        let replay = feed(&mut machine, WorkerEvent::TicketObserved {
            ticket: Box::new(success),
        }, &mut adapter);
        assert!(!replay.iter().any(|action| matches!(action, WorkerAction::DomainTerminalFact { .. })));
    }
}

#[test]
fn stale_router_loss_preserves_the_new_sessions_allocation() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    feed(&mut machine, WorkerEvent::RouteDiscovered {
        claim: Box::new(router_claim(1)), session: other_session(),
    }, &mut adapter);
    feed(&mut machine, WorkerEvent::OfferReceived {
        session: other_session(),
        allocation_id: AllocationId::new(9),
        task_id: derived_task("G1"),
        declaration: Box::new(declaration("G1")),
        ticket: Some(Box::new(ticket("G1"))),
    }, &mut adapter);
    feed(&mut machine, WorkerEvent::RouterLost { session: session() }, &mut adapter);
    assert_eq!(machine.current_session(), Some(other_session()));
    let persisted = feed(&mut machine, WorkerEvent::AttemptPersisted {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
    assert!(persisted.is_empty(), "stale router loss must not turn the offer into an origin start");
    let start = feed(&mut machine, WorkerEvent::StartReceived {
        session: other_session(), allocation_id: AllocationId::new(9), task_id: derived_task("G1"),
    }, &mut adapter);
    assert!(start.iter().any(|action| matches!(action, WorkerAction::StartDispatch { .. })));
}

#[test]
fn incompatible_elected_router_stands_down_the_old_session_but_keeps_running_work() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(2, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);
    feed(&mut machine, WorkerEvent::RouteDiscovered {
        claim: Box::new(router_claim(2)), session: other_session(),
    }, &mut adapter);
    assert_eq!(machine.current_session(), None);
    assert_eq!(machine.running_attempts(), 1);
    let refused = feed(&mut machine, WorkerEvent::OfferReceived {
        session: session(),
        allocation_id: AllocationId::new(9),
        task_id: derived_task("G2"),
        declaration: Box::new(declaration("G2")),
        ticket: Some(Box::new(ticket("G2"))),
    }, &mut adapter);
    assert!(matches!(refused.as_slice(), [WorkerAction::DeclineOffer {
        reason: DeclineReason::StaleSession, ..
    }]));
    let stale_start = feed(&mut machine, WorkerEvent::StartReceived {
        session: session(), allocation_id: AllocationId::new(1), task_id: derived_task("G1"),
    }, &mut adapter);
    assert!(stale_start.is_empty());
    let registration = connect(&mut machine, &mut adapter);
    assert_eq!(registration.active_attempts, vec![ActiveAttemptSummary {
        task_id: derived_task("G1"),
        attempt_id: PoolAttemptId::new(1),
        allocation: None,
    }]);
}

#[test]
fn registration_uses_the_compatible_discovered_router_protocol() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1, 2]);
    let registration = connect(&mut machine, &mut adapter);
    assert_eq!(registration.protocol, SchedulingProtocolVersion(1));
    assert_eq!(machine.current_session(), Some(session()));
}

#[test]
fn local_failure_releases_only_its_exact_attempt_without_settling_the_ticket() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);
    let failed = feed(&mut machine, WorkerEvent::AttemptFailed {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
    assert_eq!(failed, vec![WorkerAction::ReportAttemptChanged {
        session: session(),
        task_id: derived_task("G1"),
        attempt_id: PoolAttemptId::new(1),
        state: AttemptState::Failed,
    }]);
    assert_eq!(machine.running_attempts(), 0);
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), None);
    let offered = offer_g1(&mut machine, &mut adapter);
    assert!(offered.iter().any(|action| matches!(action, WorkerAction::AcceptOffer { .. })));
    let current = machine.attempt_for_task(&derived_task("G1")).unwrap();
    assert_ne!(current, PoolAttemptId::new(1));
    let stale = feed(&mut machine, WorkerEvent::AttemptFailed {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
    assert!(stale.is_empty());
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), Some(current));
}

#[test]
fn retired_unstarted_offers_release_capacity_and_reject_delayed_io_or_start() {
    for incompatible in [false, true] {
        let mut adapter = FakeDomain::ready();
        let mut machine = worker_machine(1, &[1]);
        connect(&mut machine, &mut adapter);
        offer_g1(&mut machine, &mut adapter);
        if incompatible {
            feed(&mut machine, WorkerEvent::RouteDiscovered {
                claim: Box::new(router_claim(2)), session: other_session(),
            }, &mut adapter);
        } else {
            feed(&mut machine, WorkerEvent::RouterLost { session: session() }, &mut adapter);
        }
        assert_eq!(machine.attempt_for_task(&derived_task("G1")), Some(PoolAttemptId::new(1)));
        let ack = feed(&mut machine, WorkerEvent::AttemptPersisted {
            task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
        }, &mut adapter);
        let start = feed(&mut machine, WorkerEvent::StartReceived {
            session: session(), allocation_id: AllocationId::new(1), task_id: derived_task("G1"),
        }, &mut adapter);
        assert!(matches!(ack.as_slice(), [WorkerAction::CancelAttempt { .. }]));
        assert!(start.is_empty(), "retired offer cannot regain authorization");
        assert_eq!(machine.attempt_for_task(&derived_task("G1")), None);
        connect(&mut machine, &mut adapter);
        let reuse = offer_g1(&mut machine, &mut adapter);
        assert!(reuse.iter().any(|action| matches!(action, WorkerAction::AcceptOffer { .. })));
    }
}

#[test]
fn retired_offer_write_failure_releases_capacity_without_a_terminal_fact() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    offer_g1(&mut machine, &mut adapter);
    feed(&mut machine, WorkerEvent::RouterLost { session: session() }, &mut adapter);
    let failure = feed(&mut machine, WorkerEvent::AttemptPersistFailed {
        task_id: derived_task("G1"),
        attempt_id: PoolAttemptId::new(1),
        reason: OpaqueReason::from_label("write failed"),
    }, &mut adapter);
    assert!(matches!(failure.as_slice(), [WorkerAction::CancelAttempt { .. }]));
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), None);
    connect(&mut machine, &mut adapter);
    let reuse = offer_g1(&mut machine, &mut adapter);
    assert!(reuse.iter().any(|action| matches!(action, WorkerAction::AcceptOffer { .. })));
}

#[test]
fn retirement_releases_a_persisted_offer_that_was_never_authorized_to_start() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    offer_g1(&mut machine, &mut adapter);
    let ack = feed(&mut machine, WorkerEvent::AttemptPersisted {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
    assert!(ack.is_empty(), "a durable offer still requires start authorization");
    let retired = feed(&mut machine, WorkerEvent::RouterLost { session: session() }, &mut adapter);
    assert!(matches!(retired.as_slice(), [WorkerAction::CancelAttempt { .. }]));
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), None);
    connect(&mut machine, &mut adapter);
    let reuse = offer_g1(&mut machine, &mut adapter);
    assert!(reuse.iter().any(|action| matches!(action, WorkerAction::AcceptOffer { .. })));
}

#[test]
fn router_loss_preserves_already_authorized_inflight_work() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    let write = offer_g1(&mut machine, &mut adapter);
    assert!(write.iter().any(|action| matches!(action, WorkerAction::PersistAttempt { .. })));
    let waiting = feed(&mut machine, WorkerEvent::StartReceived {
        session: session(), allocation_id: AllocationId::new(1), task_id: derived_task("G1"),
    }, &mut adapter);
    assert!(waiting.is_empty(), "authorization must not emit a duplicate persistence write");
    feed(&mut machine, WorkerEvent::RouterLost { session: session() }, &mut adapter);
    let ack = feed(&mut machine, WorkerEvent::AttemptPersisted {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
    assert!(matches!(ack.as_slice(), [WorkerAction::StartDispatch { .. }]));
    assert_eq!(machine.running_attempts(), 1);
}

/// A local attempt's terminal fact and the later replication of that same fact
/// are one obligation result: the domain must not be handed both.
#[test]
fn locally_reported_terminal_is_not_handed_over_again_from_replication() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);

    let local = feed(
        &mut machine,
        WorkerEvent::AttemptFinished {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            fact: success_fact(1, "report://local"),
        },
        &mut adapter,
    );
    assert!(
        local
            .iter()
            .any(|action| matches!(action, WorkerAction::DomainTerminalFact { .. })),
        "the local completion reports its fact to the domain"
    );
    feed(
        &mut machine,
        WorkerEvent::TerminalFactAccepted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        &mut adapter,
    );

    // The same success replicates back through BigSync as a terminal ticket.
    let mut replicated = ticket("G1");
    replicated.terminal.insert(
        node(3),
        terminal_lane(node(3), 1, success_fact(1, "report://local")),
    );
    let actions = feed(
        &mut machine,
        WorkerEvent::TicketObserved {
            ticket: Box::new(replicated),
        },
        &mut adapter,
    );
    assert!(
        !actions
            .iter()
            .any(|action| matches!(action, WorkerAction::DomainTerminalFact { .. })),
        "the same obligation result must not be handed to the domain twice: {actions:?}"
    );
}

/// A reconnect that arrives without the old session being closed first still
/// fences the previous connection's allocation, so the new router sees a running
/// attempt rather than a grant it never issued.
#[test]
fn reconnect_without_close_clears_the_previous_allocation() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);

    // A different connection from the same router node and incarnation arrives
    // while the first is still "live".
    let replacement = other_session();
    let mut out = Vec::new();
    machine.on_event(
        WorkerEvent::RouteDiscovered {
            claim: Box::new(router_claim(1)),
            session: replacement,
        },
        &mut adapter,
        &mut out,
    );
    let WorkerAction::RegisterSession { registration, .. } = &out[0] else {
        panic!("expected a registration action");
    };
    assert_eq!(machine.current_session(), Some(replacement));
    assert_eq!(
        registration.active_attempts,
        vec![ActiveAttemptSummary {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            allocation: None,
        }],
        "the replacement router must see a running attempt, not a stale grant"
    );
}

/// Drive an offer to a running attempt and answer the domain acknowledgement, so
/// the fixture ends with one running attempt for `G1`.
fn ack_running_attempt(machine: &mut PoolWorkerMachine<SequentialIds>, adapter: &mut FakeDomain) {
    offer_g1(machine, adapter);
    feed(
        machine,
        WorkerEvent::StartReceived {
            session: session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
        },
        adapter,
    );
    let actions = feed(
        machine,
        WorkerEvent::AttemptPersisted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        adapter,
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, WorkerAction::StartDispatch { .. })),
        "fixture attempt must start: {actions:?}"
    );
}

/// An accepted offer writes a durable attempt before dispatch starts, and the
/// start only follows the driver's acknowledgement.
#[test]
fn accepted_offer_persists_before_it_starts() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    let declaration = declaration("G1");
    let actions = offer_g1(&mut machine, &mut adapter);
    assert_eq!(
        actions,
        vec![
            WorkerAction::AcceptOffer {
                session: session(),
                allocation_id: AllocationId::new(1),
                attempt_id: PoolAttemptId::new(1),
            },
            WorkerAction::PersistAttempt {
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                allocation: Some(AllocationId::new(1)),
                declaration: Box::new(declaration.clone()),
                invocation: crate::tasks::ResolvedInvocation { args: declaration.input.clone() },
                declaration_digest: declaration.canonical_digest(),
            },
        ],
        "accepting must request a durable attempt without starting dispatch"
    );

    // The router's start arrives while the durable write is still outstanding.
    let actions = feed(
        &mut machine,
        WorkerEvent::StartReceived {
            session: session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
        },
        &mut adapter,
    );
    assert!(
        actions.is_empty(),
        "dispatch must not start before the persistence acknowledgement: {actions:?}"
    );

    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptPersisted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![
            WorkerAction::ReportAttemptChanged {
                session: session(),
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                state: AttemptState::Running,
            },
            WorkerAction::StartDispatch {
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                invocation: crate::tasks::ResolvedInvocation { args: declaration.input.clone() },
                declaration_digest: declaration.canonical_digest(),
                declaration: Box::new(declaration),
            },
        ],
        "the acknowledgement must release exactly one start"
    );
    assert_eq!(machine.running_attempts(), 1);
}

/// A persistence failure releases the attempt and reports it; dispatch never
/// starts, and a later acknowledgement cannot start it either.
#[test]
fn persistence_failure_prevents_the_start_and_frees_the_slot() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    let declaration = declaration("G1");
    offer_g1(&mut machine, &mut adapter);

    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptPersistFailed {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            reason: OpaqueReason::from_label("disk full"),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![
            WorkerAction::ReportAttemptChanged {
                session: session(),
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                state: AttemptState::Failed,
            },
            WorkerAction::CancelAttempt {
                task_id: derived_task("G1"),
                attempt_id: Some(PoolAttemptId::new(1)),
                reason: OpaqueReason::from_label("disk full"),
            },
        ],
        "a failed durable write must report failure and never start"
    );
    assert_eq!(machine.running_attempts(), 0);

    // A late acknowledgement for the released attempt is inert.
    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptPersisted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        &mut adapter,
    );
    assert!(
        actions.is_empty(),
        "an acknowledgement for a released attempt must not start it: {actions:?}"
    );

    // Capacity is free again, so the same offer can be accepted afresh.
    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(2),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, WorkerAction::AcceptOffer { .. })),
        "a failed attempt must not consume the node's capacity: {actions:?}"
    );
}

/// A cancellation that arrives before the persistence acknowledgement must
/// suppress the start: no dispatch ever runs, and the router is told the attempt
/// was cancelled.
#[test]
fn cancellation_before_acknowledgement_prevents_the_start() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    offer_g1(&mut machine, &mut adapter);
    feed(
        &mut machine,
        WorkerEvent::StartReceived {
            session: session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
        },
        &mut adapter,
    );

    let actions = feed(
        &mut machine,
        WorkerEvent::CancelReceived {
            session: session(),
            task_id: derived_task("G1"),
            reason: OpaqueReason::from_label("superseded"),
        },
        &mut adapter,
    );
    assert!(
        actions.is_empty(),
        "cancellation before ack emits nothing yet"
    );

    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptPersisted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        &mut adapter,
    );
    assert!(
        actions
            .iter()
            .all(|action| !matches!(action, WorkerAction::StartDispatch { .. })),
        "no dispatch may start after a pre-acknowledgement cancellation: {actions:?}"
    );
    assert_eq!(
        actions,
        vec![
            WorkerAction::ReportAttemptChanged {
                session: session(),
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                state: AttemptState::Cancelled,
            },
            WorkerAction::CancelAttempt {
                task_id: derived_task("G1"),
                attempt_id: Some(PoolAttemptId::new(1)),
                reason: OpaqueReason::from_label("superseded"),
            },
        ]
    );
    assert_eq!(machine.running_attempts(), 0);
}

/// A cancellation after dispatch started cancels the running attempt; it does
/// not start anything new.
#[test]
fn cancellation_after_start_cancels_the_running_attempt() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);

    let actions = feed(
        &mut machine,
        WorkerEvent::CancelReceived {
            session: session(),
            task_id: derived_task("G1"),
            reason: OpaqueReason::from_label("superseded"),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::CancelAttempt {
            task_id: derived_task("G1"),
            attempt_id: Some(PoolAttemptId::new(1)),
            reason: OpaqueReason::from_label("superseded"),
        }]
    );
}

/// A stale completion from an attempt this node already replaced must not settle
/// or delete the live attempt. This is why the attempt id is compared before any
/// removal.
#[test]
fn stale_completion_does_not_settle_the_live_attempt() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(2, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);

    // A completion naming the *previous* attempt id.
    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptFinished {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(99),
            fact: success_fact(99, "report://stale"),
        },
        &mut adapter,
    );
    assert!(
        actions.is_empty(),
        "a stale completion must emit nothing: {actions:?}"
    );
    assert_eq!(
        machine.attempt_for_task(&derived_task("G1")),
        Some(PoolAttemptId::new(1)),
        "the live attempt must survive a stale completion"
    );
    assert_eq!(machine.running_attempts(), 1);

    // The live attempt still finishes normally.
    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptFinished {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            fact: success_fact(1, "report://final"),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![
            WorkerAction::DomainTerminalFact {
                declaration: Box::new(declaration("G1")),
                attempt_id: Some(PoolAttemptId::new(1)),
                summary: TerminalSummary::Succeeded {
                    attempt_id: PoolAttemptId::new(1),
                    result_ref: Some(OpaqueResultRef::from_label(
                        "report://final"
                    )),
                },
            },
        ]
    );

    // Only the domain's acknowledgement settles the obligation locally.
    let actions = feed(
        &mut machine,
        WorkerEvent::TerminalFactAccepted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        &mut adapter,
    );
    assert_eq!(actions, vec![WorkerAction::ReportAttemptChanged {
        session: session(), task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
        state: AttemptState::Succeeded,
    }]);
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), None);

    // A stale reintroduction of the same pending obligation is now declined as
    // obsolete, because local settlement is durable knowledge.
    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(3),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(3),
            reason: DeclineReason::Obsolete,
        }]
    );
}

/// A terminal ticket is handed to the domain, which is the only place a result
/// is incorporated; a stale pending copy then declines as obsolete.
#[test]
fn observed_terminal_ticket_is_handed_to_the_domain() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    let mut terminal = ticket("G1");
    terminal.terminal.insert(
        node(2),
        terminal_lane(node(2), 1, success_fact(8, "report://done")),
    );
    let actions = feed(
        &mut machine,
        WorkerEvent::TicketObserved {
            ticket: Box::new(terminal),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DomainTerminalFact {
            declaration: Box::new(declaration("G1")),
            attempt_id: None,
            summary: TerminalSummary::Succeeded {
                attempt_id: PoolAttemptId::new(8),
                result_ref: Some(OpaqueResultRef::from_label("report://done")),
            },
        }],
        "a merged remote success must reach the domain exactly once"
    );

    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(1),
            reason: DeclineReason::Obsolete,
        }]
    );
}

/// The origin fast path uses the same durable attempt path and the same local
/// validation: persist before start, and the router is told about the attempt.
#[test]
fn origin_fast_path_persists_before_starting_and_reports_to_the_router() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    let declaration = declaration("G1");
    let actions = feed(
        &mut machine,
        WorkerEvent::OriginAttemptStart {
            declaration: Box::new(declaration.clone()),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::PersistAttempt {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            allocation: None,
            invocation: crate::tasks::ResolvedInvocation { args: declaration.input.clone() },
            declaration_digest: declaration.canonical_digest(),
            declaration: Box::new(declaration.clone()),
        }],
        "the origin path must write a durable attempt before starting"
    );

    let actions = feed(
        &mut machine,
        WorkerEvent::AttemptPersisted {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![
            WorkerAction::ReportAttemptChanged {
                session: session(),
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                state: AttemptState::Running,
            },
            WorkerAction::StartDispatch {
                task_id: derived_task("G1"),
                attempt_id: PoolAttemptId::new(1),
                invocation: crate::tasks::ResolvedInvocation { args: declaration.input.clone() },
                declaration_digest: declaration.canonical_digest(),
                declaration: Box::new(declaration),
            },
        ]
    );
}

/// The origin fast path does not bypass readiness, capacity, or local
/// settlement: each of those suppresses the start.
#[test]
fn origin_fast_path_respects_readiness_capacity_and_settlement() {
    let mut adapter = FakeDomain::ready();
    adapter.not_ready.insert(derived_task("G1"));
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    let actions = feed(
        &mut machine,
        WorkerEvent::OriginAttemptStart {
            declaration: Box::new(declaration("G1")),
        },
        &mut adapter,
    );
    assert_eq!(actions, vec![WorkerAction::WatchReadiness {
        task_id: derived_task("G1"),
        watch: ReadinessWatch::new(DomainCoordinationRef::from_label("photo-7")),
    }]);

    // Make it ready and let the capacity be consumed, then a second origin start
    // is suppressed.
    adapter.not_ready.clear();
    ack_running_attempt(&mut machine, &mut adapter);
    let actions = feed(
        &mut machine,
        WorkerEvent::OriginAttemptStart {
            declaration: Box::new(declaration("G2")),
        },
        &mut adapter,
    );
    assert!(
        actions.is_empty(),
        "an origin start above capacity must not run: {actions:?}"
    );

    // After the first attempt's terminal fact is acknowledged, a settled
    // obligation is suppressed too.
    let mut settled_machine = worker_machine(1, &[1]);
    let mut settled_adapter = FakeDomain::ready();
    settled_adapter.obsolete.insert(derived_task("G3"));
    connect(&mut settled_machine, &mut settled_adapter);
    let actions = feed(
        &mut settled_machine,
        WorkerEvent::OriginAttemptStart {
            declaration: Box::new(declaration("G3")),
        },
        &mut settled_adapter,
    );
    assert!(
        actions.is_empty(),
        "an origin start for a proven-obsolete obligation must not run: {actions:?}"
    );
}

/// Reconnecting to a new router incarnation re-announces the live attempts so
/// the new router does not re-offer work already running here.
#[test]
fn reconnect_reannounces_live_attempts_to_the_new_router() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    let first = connect(&mut machine, &mut adapter);
    assert!(first.active_attempts.is_empty());
    ack_running_attempt(&mut machine, &mut adapter);

    let actions = feed(
        &mut machine,
        WorkerEvent::RouterLost {
            session: session(),
        },
        &mut adapter,
    );
    assert!(actions.is_empty());
    assert_eq!(machine.current_session(), None);

    // A replacement router reconnects; registration carries the bounded attempt
    // set from local durable state.
    let replacement = other_session();
    let mut out = Vec::new();
    machine.on_event(
        WorkerEvent::RouteDiscovered {
            claim: Box::new(RouterClaim {
                incarnation: NodeIncarnationId::new(77),
                ..router_claim(1)
            }),
            session: replacement,
        },
        &mut adapter,
        &mut out,
    );
    let WorkerAction::RegisterSession { registration, .. } = &out[0] else {
        panic!("expected a registration action");
    };
    assert_eq!(machine.current_session(), Some(replacement));
    assert_eq!(
        registration.active_attempts,
        vec![ActiveAttemptSummary {
            task_id: derived_task("G1"),
            attempt_id: PoolAttemptId::new(1),
            allocation: None,
        }],
        "a reconnected executor must introduce its live attempts"
    );
}

/// Offers, starts, and cancellations from a session this worker no longer serves
/// are fenced, so a message from a dead connection cannot start or cancel work.
#[test]
fn messages_from_a_stale_session_are_fenced() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: other_session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: other_session(),
            allocation_id: AllocationId::new(1),
            reason: DeclineReason::StaleSession,
        }]
    );

    ack_running_attempt(&mut machine, &mut adapter);
    let actions = feed(
        &mut machine,
        WorkerEvent::CancelReceived {
            session: other_session(),
            task_id: derived_task("G1"),
            reason: OpaqueReason::from_label("stale cancel"),
        },
        &mut adapter,
    );
    assert!(
        actions.is_empty(),
        "a stale session must not cancel live work: {actions:?}"
    );
    assert_eq!(machine.running_attempts(), 1);
}

/// An offer whose declaration does not match the ticket it names, or belongs to
/// another pool, is declined as invalid.
#[test]
fn mismatched_offer_is_declined_invalid() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    // The declaration names a different task than the offer.
    let mut mismatched = declaration("G1");
    mismatched.task_id = derived_task("G2");
    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(1),
            task_id: derived_task("G1"),
            declaration: Box::new(mismatched),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(1),
            reason: DeclineReason::Invalid,
        }]
    );

    // The declaration belongs to a different pool.
    let mut foreign = declaration("G1");
    foreign.pool = TaskPoolId::from_label("agent-background");
    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(2),
            task_id: derived_task("G1"),
            declaration: Box::new(foreign),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(2),
            reason: DeclineReason::Invalid,
        }]
    );
}

/// Executor readiness is authoritative: a `NotReady` task is declined with the
/// domain's watch, and a missing ticket declines as `CoordinationIncomplete`
/// rather than pretending obsolescence.
#[test]
fn readiness_and_missing_tickets_are_declined_distinctly() {
    let mut adapter = FakeDomain::ready();
    adapter.not_ready.insert(derived_task("G1"));
    let mut machine = worker_machine(2, &[1]);
    connect(&mut machine, &mut adapter);

    let actions = offer_g1(&mut machine, &mut adapter);
    assert_eq!(
        actions,
        vec![
            WorkerAction::WatchReadiness {
                task_id: derived_task("G1"),
                watch: ReadinessWatch::new(DomainCoordinationRef::from_label("photo-7")),
            },
            WorkerAction::DeclineOffer {
                session: session(),
                allocation_id: AllocationId::new(1),
                reason: DeclineReason::NotReady,
            },
        ]
    );

    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(2),
            task_id: derived_task("G2"),
            declaration: Box::new(declaration("G2")),
            ticket: None,
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(2),
            reason: DeclineReason::CoordinationIncomplete,
        }],
        "an unmaterialized ticket is coordination-incomplete, not obsolete"
    );
    assert_eq!(feed(&mut machine, WorkerEvent::TicketObserved {
        ticket: Box::new(ticket("G2")),
    }, &mut adapter), vec![WorkerAction::ReportReadinessWakeup {
        session: session(), task_id: derived_task("G2"),
    }]);
    assert_eq!(machine.running_attempts(), 0);
}

/// Capacity is reserved at acceptance, so outstanding offers that have not yet
/// started still count against the bound.
#[test]
fn capacity_counts_outstanding_attempts_not_only_running_ones() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    connect(&mut machine, &mut adapter);

    // Accept an offer but never acknowledge persistence: the attempt is
    // outstanding, not running.
    offer_g1(&mut machine, &mut adapter);
    assert_eq!(machine.running_attempts(), 0);

    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(9),
            task_id: derived_task("G2"),
            declaration: Box::new(declaration("G2")),
            ticket: Some(Box::new(ticket("G2"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(9),
            reason: DeclineReason::Unavailable,
        }],
        "an accepted-but-unstarted attempt must still hold capacity"
    );
}

/// A router speaking an unsupported scheduling protocol is not accepted; the
/// worker stays idle and waits for a compatible router.
#[test]
fn unsupported_router_protocol_is_not_served() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(1, &[1]);
    let actions = feed(
        &mut machine,
        WorkerEvent::RouteDiscovered {
            claim: Box::new(router_claim(2)),
            session: session(),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::UnsupportedRouterProtocol {
            session: session(),
            claim_protocol: SchedulingProtocolVersion(2),
        }]
    );
    assert_eq!(machine.current_session(), None);
}

/// The same offer arriving twice (a duplicate delivery) does not start two
/// attempts.
#[test]
fn duplicate_offer_for_a_running_task_is_declined() {
    let mut adapter = FakeDomain::ready();
    let mut machine = worker_machine(2, &[1]);
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);

    let actions = feed(
        &mut machine,
        WorkerEvent::OfferReceived {
            session: session(),
            allocation_id: AllocationId::new(5),
            task_id: derived_task("G1"),
            declaration: Box::new(declaration("G1")),
            ticket: Some(Box::new(ticket("G1"))),
        },
        &mut adapter,
    );
    assert_eq!(
        actions,
        vec![WorkerAction::DeclineOffer {
            session: session(),
            allocation_id: AllocationId::new(5),
            reason: DeclineReason::Unavailable,
        }]
    );
    assert_eq!(machine.running_attempts(), 1);
}

#[test]
fn origin_requires_local_placement_and_capabilities() {
    for (placement, capabilities, allowed) in [
        (crate::tasks::Preference::Only(node(8)), CapabilitySummary::from_labels(["gpu"]), false),
        (crate::tasks::Preference::Only(node(3)), CapabilitySummary::from_labels(["cpu"]), false),
        (crate::tasks::Preference::Only(node(3)), CapabilitySummary::from_labels(["gpu"]), true),
        (crate::tasks::Preference::PreferOrigin(node(8)), CapabilitySummary::from_labels(["gpu"]), true),
    ] {
        let mut machine = worker_machine(1, &[1]);
        let mut adapter = FakeDomain::ready();
        let mut declaration = declaration("G1");
        declaration.placement = placement;
        declaration.capabilities = capabilities;
        let actions = feed(&mut machine, WorkerEvent::OriginAttemptStart {
            declaration: Box::new(declaration),
        }, &mut adapter);
        assert_eq!(actions.iter().any(|action| matches!(action, WorkerAction::PersistAttempt { .. })), allowed);
        assert!(!actions.iter().any(|action| matches!(action, WorkerAction::StartDispatch { .. })));
    }
}

#[test]
fn readiness_watch_deduplicates_and_wakes_current_connection_once() {
    let mut machine = worker_machine(1, &[1]);
    let mut adapter = FakeDomain::ready();
    adapter.not_ready.insert(derived_task("G1"));
    connect(&mut machine, &mut adapter);
    let observe = || WorkerEvent::TicketObserved { ticket: Box::new(ticket("G1")) };
    assert_eq!(feed(&mut machine, observe(), &mut adapter), vec![WorkerAction::WatchReadiness {
        task_id: derived_task("G1"),
        watch: ReadinessWatch::new(DomainCoordinationRef::from_label("photo-7")),
    }]);
    assert!(feed(&mut machine, observe(), &mut adapter).is_empty());
    feed(&mut machine, WorkerEvent::RouteDiscovered {
        claim: Box::new(router_claim(1)), session: other_session(),
    }, &mut adapter);
    assert_eq!(feed(&mut machine, WorkerEvent::ReadinessWakeup { task_id: derived_task("G1") },
        &mut adapter), vec![WorkerAction::ReportReadinessWakeup {
            session: other_session(), task_id: derived_task("G1"),
        }]);
    assert!(feed(&mut machine, WorkerEvent::ReadinessWakeup { task_id: derived_task("G1") },
        &mut adapter).is_empty());
}

#[test]
fn readiness_descriptor_replacement_and_disconnected_progress_are_local() {
    struct Inputs(&'static str);
    impl WorkerDomainAdapter for Inputs {
        fn classify(&mut self, _: &TaskTicket) -> TaskClassification {
            TaskClassification::NotReady(ReadinessWatch::new(DomainCoordinationRef::from_label(self.0)))
        }
    }
    let mut machine = worker_machine(1, &[1]);
    let mut adapter = Inputs("first");
    let observe = || WorkerEvent::TicketObserved { ticket: Box::new(ticket("G1")) };
    feed(&mut machine, observe(), &mut adapter);
    adapter.0 = "replacement";
    assert_eq!(feed(&mut machine, observe(), &mut adapter), vec![WorkerAction::WatchReadiness {
        task_id: derived_task("G1"),
        watch: ReadinessWatch::new(DomainCoordinationRef::from_label("replacement")),
    }]);
    assert!(feed(&mut machine, WorkerEvent::ReadinessWakeup { task_id: derived_task("G1") },
        &mut adapter).is_empty());
    assert!(feed(&mut machine, WorkerEvent::ReadinessWakeup { task_id: derived_task("G1") },
        &mut adapter).is_empty());
    assert!(feed(&mut machine, WorkerEvent::RouteDiscovered {
        claim: Box::new(router_claim(1)), session: session(),
    }, &mut adapter).iter().any(|action| matches!(action, WorkerAction::RegisterSession { .. })));
}

#[test]
#[should_panic(expected = "terminal acknowledgement requires a finished local attempt")]
fn premature_matching_terminal_ack_is_an_invariant_violation() {
    let mut machine = worker_machine(1, &[1]);
    let mut adapter = FakeDomain::ready();
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);
    feed(&mut machine, WorkerEvent::TerminalFactAccepted {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
}

#[test]
fn stale_terminal_ack_preserves_replacement_attempt() {
    let mut machine = worker_machine(1, &[1]);
    let mut adapter = FakeDomain::ready();
    connect(&mut machine, &mut adapter);
    ack_running_attempt(&mut machine, &mut adapter);
    feed(&mut machine, WorkerEvent::AttemptFailed {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter);
    offer_g1(&mut machine, &mut adapter);
    assert!(feed(&mut machine, WorkerEvent::TerminalFactAccepted {
        task_id: derived_task("G1"), attempt_id: PoolAttemptId::new(1),
    }, &mut adapter).is_empty());
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), Some(PoolAttemptId::new(2)));
    let declined = feed(&mut machine, WorkerEvent::OfferReceived {
        session: session(), allocation_id: AllocationId::new(10), task_id: derived_task("G2"),
        declaration: Box::new(declaration("G2")), ticket: Some(Box::new(ticket("G2"))),
    }, &mut adapter);
    assert_eq!(declined, vec![WorkerAction::DeclineOffer {
        session: session(), allocation_id: AllocationId::new(10), reason: DeclineReason::Unavailable,
    }]);
}

#[test]
#[should_panic(expected = "local task declaration violates a producer invariant")]
fn invalid_authoritative_origin_fails_at_the_local_producer_boundary() {
    let mut machine = worker_machine(1, &[1]);
    let mut adapter = FakeDomain::ready();
    let mut invalid = declaration("G1");
    invalid.effect_policy = crate::tasks::EffectPolicy::AuthoritativePlacement;
    invalid.placement = crate::tasks::Preference::AnyNode;
    feed(&mut machine, WorkerEvent::OriginAttemptStart { declaration: Box::new(invalid) }, &mut adapter);
}

#[test]
fn invalid_remote_authoritative_offer_cannot_consume_capacity() {
    let mut machine = worker_machine(1, &[1]);
    let mut adapter = FakeDomain::ready();
    connect(&mut machine, &mut adapter);
    let mut invalid = ticket("G1");
    invalid.declaration.declaration.effect_policy = crate::tasks::EffectPolicy::AuthoritativePlacement;
    invalid.declaration.declaration.placement = crate::tasks::Preference::AnyNode;
    let actions = feed(&mut machine, WorkerEvent::OfferReceived {
        session: session(), allocation_id: AllocationId::new(9), task_id: invalid.task_id,
        declaration: Box::new(invalid.declaration.declaration.clone()), ticket: Some(Box::new(invalid)),
    }, &mut adapter);
    assert_eq!(actions, vec![WorkerAction::DeclineOffer {
        session: session(), allocation_id: AllocationId::new(9), reason: DeclineReason::Invalid,
    }]);
    assert!(machine.attempt_for_task(&derived_task("G1")).is_none());
    offer_g1(&mut machine, &mut adapter);
    assert_eq!(machine.attempt_for_task(&derived_task("G1")), Some(PoolAttemptId::new(1)));
}
