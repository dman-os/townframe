//! Driver-loop smoke test across both machines.
//!
//! Unit modules cover individual transitions. This driver composes registration,
//! offers, readiness subscriptions, reoffers, and the durable-start handshake
//! through the two public action/event APIs. Storage acknowledgements and local
//! execution are in-memory effects here, not transport/storage integration.

use std::collections::{HashMap, HashSet};

use super::test_util::*;
use super::*;

/// What a driver records while it runs the machines.
#[derive(Default)]
struct DriverLog {
    /// Every dispatch the worker was told to start, in order.
    started: Vec<(PoolTaskId, PoolAttemptId)>,
    /// Every terminal fact the worker's domain was told about, in order.
    incorporated: Vec<(PoolTaskId, TerminalSummary)>,
    accepted: usize,
    watches: HashMap<PoolTaskId, ReadinessWatch>,
    router_actions: Vec<RouterAction>,
    cancelled: Vec<(PoolTaskId, Option<PoolAttemptId>)>,
    hold_persistence: bool,
    pending_persistence: Vec<(PoolTaskId, PoolAttemptId)>,
    durable_invocations: HashMap<(PoolTaskId, PoolAttemptId), ([u8; 32], ResolvedInvocation)>,
    computed: Vec<(PoolTaskId, PoolAttemptId, u32)>,
}

#[derive(Default)]
struct TestDomain {
    /// Tasks the local adapter declares ready.
    ready: HashSet<PoolTaskId>,
    multiplier: u32,
}

impl WorkerDomainAdapter for TestDomain {
    fn classify(&mut self, ticket: &TaskTicket) -> TaskClassification {
        if self.ready.contains(&ticket.task_id) {
            let input = ticket.declaration.declaration.input.iter().map(|byte| u32::from(*byte)).sum::<u32>();
            let mut args = input.to_be_bytes().to_vec();
            args.extend_from_slice(&self.multiplier.to_be_bytes());
            TaskClassification::Runnable(ResolvedInvocation { args })
        } else {
            TaskClassification::NotReady(ReadinessWatch::new(
                DomainCoordinationRef::from_label("local-input"),
            ))
        }
    }
}

/// The router adapter the driver hands to the router.
struct NothingObsolete;

impl RouterDomainAdapter for NothingObsolete {
    fn is_proven_obsolete(&self, _declaration: &TaskDeclaration) -> bool {
        false
    }
}

/// A driver that translates actions into events and records what it was asked to
/// do. Nothing else, because everything else is the machines' job.
struct Driver {
    router: RouterMachine<SequentialIds>,
    worker: PoolWorkerMachine<SequentialIds>,
    domain: TestDomain,
    log: DriverLog,
    tickets: HashMap<PoolTaskId, TaskTicket>,
}

/// Run one worker action to completion against the worker and its domain,
/// pushing any actions it generates onto `queue`.
///
/// Durability here is instantaneous: a real driver writes to storage (and
/// reports the same acknowledgement), but the handshake is identical.
fn run_worker_action(
    worker: &mut PoolWorkerMachine<SequentialIds>,
    router: &mut RouterMachine<SequentialIds>,
    domain: &mut TestDomain,
    log: &mut DriverLog,
    action: WorkerAction,
    queue: &mut Vec<WorkerAction>,
) {
    match action {
        WorkerAction::PersistAttempt {
            task_id,
            attempt_id,
            invocation,
            declaration_digest,
            ..
        } => {
            log.durable_invocations.insert((task_id, attempt_id), (declaration_digest, invocation));
            if log.hold_persistence {
                log.pending_persistence.push((task_id, attempt_id));
            } else {
                worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id }, domain, queue);
            }
        }
        WorkerAction::StartDispatch {
            task_id,
            attempt_id,
            invocation,
            declaration_digest,
            ..
        } => {
            let (durable_digest, _) = log.durable_invocations.get(&(task_id, attempt_id))
                .expect("dispatch requires an already durable attempt");
            assert_eq!(*durable_digest, declaration_digest);
            let args: [u8; 8] = invocation.args.try_into().expect("domain-owned arithmetic arguments");
            let input = u32::from_be_bytes(args[..4].try_into().unwrap());
            let multiplier = u32::from_be_bytes(args[4..].try_into().unwrap());
            log.computed.push((task_id, attempt_id, input * multiplier));
            log.started.push((task_id, attempt_id));
        }
        WorkerAction::DomainTerminalFact {
            declaration,
            summary,
            attempt_id,
        } => {
            let task_id = declaration.task_id;
            log.incorporated.push((task_id, summary));
            if let Some(attempt_id) = attempt_id {
                worker.on_event(WorkerEvent::TerminalFactAccepted { task_id, attempt_id },
                    domain, queue);
            }
        }
        WorkerAction::RegisterSession { session, registration } => {
            let mut actions = Vec::new();
            router.on_event(RouterEvent::RegisterExecutor {
                session, capabilities: registration.capabilities,
                capacity: registration.capacity, protocol: registration.protocol,
                active_attempts: registration.active_attempts,
            }, &NothingObsolete, &mut actions);
            log.router_actions.extend(actions);
        }
        WorkerAction::WatchReadiness { task_id, watch } => {
            log.watches.insert(task_id, watch);
        }
        WorkerAction::AcceptOffer { session, allocation_id, attempt_id } => {
            log.accepted += 1;
            let mut actions = Vec::new();
            router.on_event(RouterEvent::OfferAccepted { session, allocation_id, attempt_id },
                &NothingObsolete, &mut actions);
            for action in actions {
                if let RouterAction::StartAttempt { session, allocation_id, task_id } = action {
                    worker.on_event(WorkerEvent::StartReceived { session, allocation_id, task_id },
                        domain, queue);
                }
            }
        }
        WorkerAction::DeclineOffer { session, allocation_id, reason } => {
            let mut actions = Vec::new();
            router.on_event(RouterEvent::OfferDeclined { session, allocation_id, reason },
                &NothingObsolete, &mut actions);
            log.router_actions.extend(actions);
        }
        WorkerAction::ReportAttemptChanged { session, task_id, attempt_id, state } => {
            router.on_event(RouterEvent::AttemptChanged { session, task_id, attempt_id, state },
                &NothingObsolete, &mut log.router_actions);
        }
        WorkerAction::ReportReadinessWakeup { .. } => panic!("deliver wakeups through Driver"),
        WorkerAction::CancelAttempt { task_id, attempt_id, .. } => {
            log.cancelled.push((task_id, attempt_id));
        }
        WorkerAction::UnsupportedRouterProtocol { .. } => {}
    }
}

/// Drain a worker action queue, feeding generated actions back in.
fn drain_worker(
    worker: &mut PoolWorkerMachine<SequentialIds>,
    router: &mut RouterMachine<SequentialIds>,
    domain: &mut TestDomain,
    log: &mut DriverLog,
    actions: Vec<WorkerAction>,
) {
    let mut queue = std::collections::VecDeque::from(actions);
    while let Some(action) = queue.pop_front() {
        let mut generated = Vec::new();
        run_worker_action(worker, router, domain, log, action, &mut generated);
        queue.extend(generated);
    }
}

impl Driver {
    fn new(ready: &[PoolTaskId]) -> Self {
        Self {
            router: RouterMachine::new(
                RouterConfig {
                    pool: pool(),
                    node: node(9),
                    incarnation: incarnation(9),
                    supported_protocols: vec![SchedulingProtocolVersion(1)],
                },
                ids(),
            ),
            worker: PoolWorkerMachine::new(
                WorkerConfig {
                    pool: pool(),
                    node: node(3),
                    incarnation: incarnation(3),
                    capabilities: CapabilitySummary::from_labels(["gpu"]),
                    capacity: Capacity::new(1),
                    supported_protocols: vec![SchedulingProtocolVersion(1)],
                },
                ids(),
            ),
            domain: TestDomain {
                ready: ready.iter().copied().collect(),
                multiplier: 2,
            },
            log: DriverLog::default(),
            tickets: HashMap::new(),
        }
    }

    fn router_session(&self) -> SessionKey {
        SessionKey {
            node: node(3),
            incarnation: incarnation(3),
            session: RouterSessionId::new(1),
        }
    }

    /// Take router leadership: claim, then confirm the durable write.
    fn take_leadership(&mut self) {
        let mut out = Vec::new();
        self.router
            .on_event(RouterEvent::ConsiderTakeover, &NothingObsolete, &mut out);
        let confirmed: Vec<RouterClaim> = out
            .into_iter()
            .filter_map(|action| match action {
                RouterAction::PersistClaim { claim } => Some(claim),
                _ => None,
            })
            .collect();
        for claim in confirmed {
            self.router.on_event(
                RouterEvent::ClaimPersisted {
                    generation: claim.generation,
                    claim_id: claim.claim_id,
                },
                &NothingObsolete,
                &mut Vec::new(),
            );
        }
        assert!(self.router.is_routing(), "driver fixture must hold leadership");
    }

    /// Offer `ticket` through the router to the worker and run the full
    /// acceptance, authorization, and persistence handshake; returns acceptance.
    fn route(&mut self, ticket: &TaskTicket) -> bool {
        let session = self.router_session();

        // The worker discovers the router before any offer can arrive.
        let claim = self.router.own_claim().expect("own claim").clone();
        let mut discovered = Vec::new();
        self.worker.on_event(
            WorkerEvent::RouteDiscovered {
                claim: Box::new(claim),
                session,
            },
            &mut self.domain,
            &mut discovered,
        );
        drain_worker(
            &mut self.worker,
            &mut self.router,
            &mut self.domain,
            &mut self.log,
            discovered,
        );

        self.tickets.insert(ticket.task_id, ticket.clone());
        let before = self.log.accepted;
        let mut actions = Vec::new();
        self.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket.clone()) },
            &NothingObsolete, &mut actions);
        self.deliver_offers(actions);
        self.log.accepted != before
    }

    fn deliver_offers(&mut self, actions: Vec<RouterAction>) {
        let mut pending = std::collections::VecDeque::from(actions);
        pending.extend(std::mem::take(&mut self.log.router_actions));
        while let Some(action) = pending.pop_front() {
            let event = match action {
                RouterAction::SendOffer { session, allocation_id, task_id, declaration } => {
                    WorkerEvent::OfferReceived {
                        session, allocation_id, task_id, declaration,
                        ticket: self.tickets.get(&task_id).cloned().map(Box::new),
                    }
                }
                RouterAction::CancelAttempt { session, task_id, reason } => {
                    WorkerEvent::CancelReceived { session, task_id, reason }
                }
                _ => continue,
            };
            let mut out = Vec::new();
            self.worker.on_event(event, &mut self.domain, &mut out);
            drain_worker(&mut self.worker, &mut self.router, &mut self.domain, &mut self.log, out);
            pending.extend(std::mem::take(&mut self.log.router_actions));
        }
    }

    fn materialize(&mut self, task_id: PoolTaskId) {
        self.log.watches.remove(&task_id).expect("installed input subscription");
        self.domain.ready.insert(task_id);
        let mut out = Vec::new();
        self.worker.on_event(WorkerEvent::ReadinessWakeup { task_id }, &mut self.domain, &mut out);
        for action in out {
            let WorkerAction::ReportReadinessWakeup { session, task_id } = action else {
                panic!("input progress must not dispatch directly");
            };
            let mut offers = Vec::new();
            self.router.on_event(RouterEvent::ReadinessWakeup { session, task_id },
                &NothingObsolete, &mut offers);
            self.deliver_offers(offers);
        }
    }
}

/// The router-allocated path runs one durable attempt through the public API, and
/// only a node whose local adapter is ready starts anything.
#[test]
fn router_allocated_attempt_runs_through_the_public_api() {
    let mut driver = Driver::new(&[derived_task("G1")]);
    driver.take_leadership();

    let accepted = driver.route(&ticket("G1"));
    assert!(accepted, "a ready worker must accept the routed offer");
    assert_eq!(
        driver.log.started,
        vec![(derived_task("G1"), PoolAttemptId::new(1))],
        "exactly one dispatch must start, after persistence"
    );
}

/// A worker whose local adapter is not ready declines the offer and starts
/// nothing.
#[test]
fn not_ready_worker_starts_nothing() {
    let mut driver = Driver::new(&[]);
    driver.take_leadership();

    let accepted = driver.route(&ticket("G1"));
    assert!(!accepted, "an unready worker must decline");
    assert!(driver.log.started.is_empty(), "nothing may start");
}

#[test]
fn materialized_input_reoffers_and_starts_once_after_persistence() {
    let task_id = derived_task("G1");
    let mut driver = Driver::new(&[]);
    driver.take_leadership();
    assert!(!driver.route(&ticket("G1")));
    assert!(driver.log.started.is_empty());
    driver.materialize(task_id);
    assert_eq!(driver.log.started, vec![(task_id, PoolAttemptId::new(1))]);
    let mut out = Vec::new();
    driver.worker.on_event(WorkerEvent::ReadinessWakeup { task_id }, &mut driver.domain, &mut out);
    assert!(out.is_empty());
}

#[test]
fn delayed_old_readiness_notification_does_not_unblock_replacement() {
    let task_id = derived_task("G1");
    let mut driver = Driver::new(&[]);
    driver.take_leadership();
    driver.route(&ticket("G1"));
    let old = driver.router_session();
    let mut wake = Vec::new();
    driver.domain.ready.insert(task_id);
    driver.worker.on_event(WorkerEvent::ReadinessWakeup { task_id }, &mut driver.domain, &mut wake);
    let WorkerAction::ReportReadinessWakeup { session, task_id } = wake.remove(0) else {
        panic!("expected a real worker progress notification");
    };
    driver.router.on_event(RouterEvent::SessionClosed { session: old }, &NothingObsolete, &mut Vec::new());
    driver.worker.on_event(WorkerEvent::RouterLost { session: old }, &mut driver.domain, &mut Vec::new());
    driver.domain.ready.remove(&task_id);
    let replacement = SessionKey { session: RouterSessionId::new(2), ..old };
    let mut registration = Vec::new();
    driver.worker.on_event(WorkerEvent::RouteDiscovered {
        claim: Box::new(driver.router.own_claim().unwrap().clone()), session: replacement,
    }, &mut driver.domain, &mut registration);
    let WorkerAction::RegisterSession { session: current, registration } = registration.remove(0) else {
        panic!("expected replacement registration");
    };
    let mut offers = Vec::new();
    driver.router.on_event(RouterEvent::RegisterExecutor {
        session: current, capabilities: registration.capabilities, capacity: registration.capacity,
        protocol: registration.protocol, active_attempts: registration.active_attempts,
    }, &NothingObsolete, &mut offers);
    driver.deliver_offers(offers);
    let mut stale = Vec::new();
    driver.router.on_event(RouterEvent::ReadinessWakeup { session, task_id }, &NothingObsolete, &mut stale);
    driver.deliver_offers(stale);
    assert!(driver.log.started.is_empty());
    driver.materialize(task_id);
    assert_eq!(driver.log.started, vec![(task_id, PoolAttemptId::new(1))]);
}

#[test]
fn incorporation_ack_releases_capacity_before_next_offer() {
    let first = derived_task("G1");
    let second = derived_task("G2");
    let mut driver = Driver::new(&[first, second]);
    driver.take_leadership();
    assert!(driver.route(&ticket("G1")));
    let first_attempt = driver.worker.attempt_for_task(&first).unwrap();
    let mut finishing = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptFinished {
        task_id: first, attempt_id: first_attempt, fact: success_fact(1, "done"),
    }, &mut driver.domain, &mut finishing);
    assert!(!finishing.iter().any(|action| matches!(action,
        WorkerAction::ReportAttemptChanged { state: AttemptState::Succeeded | AttemptState::Cancelled, .. }
    )), "terminal capacity hint must wait for domain incorporation and actual release");
    assert!(matches!(finishing.as_slice(), [WorkerAction::DomainTerminalFact { .. }]));
    driver.tickets.insert(second, ticket("G2"));
    let mut offers = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &NothingObsolete, &mut offers);
    driver.deliver_offers(offers);
    assert_eq!(driver.log.started, vec![(first, first_attempt)]);
    let mut released = Vec::new();
    driver.worker.on_event(WorkerEvent::TerminalFactAccepted {
        task_id: first, attempt_id: first_attempt,
    }, &mut driver.domain, &mut released);
    assert!(driver.worker.attempt_for_task(&first).is_none());
    for action in released {
        let WorkerAction::ReportAttemptChanged { session, task_id, attempt_id, state } = action else {
            panic!("release reports the exact finished attempt");
        };
        let mut offers = Vec::new();
        driver.router.on_event(RouterEvent::AttemptChanged { session, task_id, attempt_id, state },
            &NothingObsolete, &mut offers);
        driver.deliver_offers(offers);
    }
    assert_eq!(driver.log.started, vec![(first, first_attempt), (second, PoolAttemptId::new(2))]);
    let mut stale = Vec::new();
    driver.worker.on_event(WorkerEvent::TerminalFactAccepted {
        task_id: second, attempt_id: first_attempt,
    }, &mut driver.domain, &mut stale);
    assert!(stale.is_empty());
    assert_eq!(driver.worker.attempt_for_task(&second), Some(PoolAttemptId::new(2)));
}

#[test]
fn accepted_unstarted_attempt_release_is_identity_fenced() {
    for cancel in [false, true] {
        let task_id = derived_task("G1");
        let mut driver = Driver::new(&[task_id]);
        driver.take_leadership();
        let session = driver.router_session();
        let mut registration = Vec::new();
        driver.worker.on_event(WorkerEvent::RouteDiscovered {
            claim: Box::new(driver.router.own_claim().unwrap().clone()), session,
        }, &mut driver.domain, &mut registration);
        drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, registration);
        driver.tickets.insert(task_id, ticket("G1"));
        let mut offers = Vec::new();
        driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) },
            &NothingObsolete, &mut offers);
        let RouterAction::SendOffer { allocation_id, declaration, .. } = offers.remove(0) else { panic!() };
        let mut accepted = Vec::new();
        driver.worker.on_event(WorkerEvent::OfferReceived {
            session, allocation_id, task_id, declaration, ticket: Some(Box::new(ticket("G1"))),
        }, &mut driver.domain, &mut accepted);
        assert!(matches!(accepted.remove(0), WorkerAction::AcceptOffer { .. }));
        let WorkerAction::PersistAttempt { attempt_id, .. } = accepted.remove(0) else { panic!() };
        let mut starts = Vec::new();
        driver.router.on_event(RouterEvent::OfferAccepted { session, allocation_id, attempt_id },
            &NothingObsolete, &mut starts);
        assert!(matches!(starts.as_slice(), [RouterAction::StartAttempt { .. }]));
        let mut released = Vec::new();
        if cancel {
            driver.worker.on_event(WorkerEvent::CancelReceived {
                session, task_id, reason: OpaqueReason::from_label("cancel before ack"),
            }, &mut driver.domain, &mut released);
            driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id },
                &mut driver.domain, &mut released);
        } else {
            driver.worker.on_event(WorkerEvent::AttemptPersistFailed {
                task_id, attempt_id, reason: OpaqueReason::from_label("write failed"),
            }, &mut driver.domain, &mut released);
        }
        assert!(!released.iter().any(|action| matches!(action, WorkerAction::StartDispatch { .. })));
        let mut reoffers = Vec::new();
        for action in released {
            if let WorkerAction::ReportAttemptChanged { session, task_id, attempt_id, state } = action {
                driver.router.on_event(RouterEvent::AttemptChanged { session, task_id, attempt_id, state },
                    &NothingObsolete, &mut reoffers);
            }
        }
        assert!(matches!(reoffers.as_slice(), [RouterAction::SendOffer { .. }]),
            "exact accepted attempt release must free its allocation, cancel={cancel}");
        driver.deliver_offers(reoffers);
        assert_eq!(driver.log.started, vec![(task_id, PoolAttemptId::new(2))]);
        let mut stale = Vec::new();
        driver.router.on_event(RouterEvent::AttemptChanged {
            session, task_id, attempt_id, state: AttemptState::Failed,
        }, &NothingObsolete, &mut stale);
        assert!(stale.is_empty());
        driver.router.on_event(RouterEvent::RoutingTick, &NothingObsolete, &mut stale);
        assert!(stale.is_empty(), "stale failure must not make the live task schedulable");
    }
}

#[test]
fn router_cancellation_before_held_persistence_ack_never_starts() {
    let task_id = derived_task("G1");
    let next = derived_task("G2");
    let mut driver = Driver::new(&[task_id, next]);
    driver.take_leadership();
    driver.log.hold_persistence = true;
    assert!(driver.route(&ticket("G1")));
    assert!(driver.log.started.is_empty());
    let (task_id, attempt_id) = driver.log.pending_persistence.remove(0);
    let mut cancellations = Vec::new();
    driver.router.on_event(RouterEvent::TaskRemoved { task_id }, &NothingObsolete, &mut cancellations);
    driver.deliver_offers(cancellations);
    assert!(driver.log.started.is_empty());
    driver.tickets.insert(next, ticket("G2"));
    let mut offers = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &NothingObsolete, &mut offers);
    assert!(offers.is_empty(), "cancellation cannot advertise capacity before the worker releases it");
    driver.deliver_offers(offers);
    driver.log.hold_persistence = false;
    let mut acknowledged = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id },
        &mut driver.domain, &mut acknowledged);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain,
        &mut driver.log, acknowledged);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.started, vec![(next, PoolAttemptId::new(2))]);
    assert_eq!(driver.log.cancelled, vec![(task_id, Some(attempt_id))]);
    assert!(driver.worker.attempt_for_task(&task_id).is_none());
    let mut stale = Vec::new();
    driver.router.on_event(RouterEvent::AttemptChanged {
        session: driver.router_session(), task_id, attempt_id, state: AttemptState::Cancelled,
    }, &NothingObsolete, &mut stale);
    assert!(stale.is_empty());
}

#[test]
fn remote_cancellation_incorporation_does_not_release_running_local_attempt() {
    let task_id = derived_task("G1");
    let next = derived_task("G2");
    let mut driver = Driver::new(&[task_id, next]);
    driver.take_leadership();
    driver.route(&ticket("G1"));
    let attempt_id = driver.worker.attempt_for_task(&task_id).unwrap();
    let mut remote = ticket("G1");
    remote.terminal.insert(node(8), terminal_lane(node(8), 1, cancelled_fact("remote cancellation")));
    let mut actions = Vec::new();
    driver.worker.on_event(WorkerEvent::TicketObserved { ticket: Box::new(remote) },
        &mut driver.domain, &mut actions);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, actions);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.cancelled, vec![(task_id, Some(attempt_id))]);
    assert_eq!(driver.worker.attempt_for_task(&task_id), Some(attempt_id));
    assert_eq!(driver.worker.running_attempts(), 1);
    driver.tickets.insert(next, ticket("G2"));
    let mut offers = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &NothingObsolete, &mut offers);
    driver.deliver_offers(offers);
    assert_eq!(driver.log.started, vec![(task_id, attempt_id)]);
    let mut finished = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptFinished {
        task_id, attempt_id, fact: cancelled_fact("remote cancellation"),
    }, &mut driver.domain, &mut finished);
    assert!(driver.worker.attempt_for_task(&task_id).is_none());
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, finished);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.started, vec![(task_id, attempt_id), (next, PoolAttemptId::new(2))]);
    assert_eq!(driver.log.incorporated, vec![(task_id, TerminalSummary::Cancelled {
        reason: OpaqueReason::from_label("remote cancellation"),
    })]);
}

#[test]
fn delayed_acceptance_of_cancelled_offer_binds_release_without_authorizing_start() {
    let task_id = derived_task("G1");
    let next = derived_task("G2");
    let mut driver = Driver::new(&[task_id, next]);
    driver.take_leadership();
    let session = driver.router_session();
    let mut registration = Vec::new();
    driver.worker.on_event(WorkerEvent::RouteDiscovered {
        claim: Box::new(driver.router.own_claim().unwrap().clone()), session,
    }, &mut driver.domain, &mut registration);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, registration);
    let mut offers = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) },
        &NothingObsolete, &mut offers);
    let RouterAction::SendOffer { allocation_id, declaration, .. } = offers.remove(0) else { panic!() };
    let mut accepted = Vec::new();
    driver.worker.on_event(WorkerEvent::OfferReceived {
        session, allocation_id, task_id, declaration, ticket: Some(Box::new(ticket("G1"))),
    }, &mut driver.domain, &mut accepted);
    let WorkerAction::AcceptOffer { session, allocation_id, attempt_id } = accepted.remove(0) else { panic!() };
    assert!(matches!(accepted.as_slice(), [WorkerAction::PersistAttempt { .. }]));
    let mut cancelled = Vec::new();
    driver.router.on_event(RouterEvent::TaskRemoved { task_id }, &NothingObsolete, &mut cancelled);
    driver.deliver_offers(cancelled);
    let mut delayed = Vec::new();
    driver.router.on_event(RouterEvent::OfferAccepted { session, allocation_id, attempt_id },
        &NothingObsolete, &mut delayed);
    assert!(matches!(delayed.as_slice(), [RouterAction::CancelAttempt { .. }]),
        "delayed acceptance must preserve cancellation, not authorize dispatch");
    driver.deliver_offers(delayed);
    driver.tickets.insert(next, ticket("G2"));
    let mut pending = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &NothingObsolete, &mut pending);
    assert!(pending.is_empty());
    let mut released = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id },
        &mut driver.domain, &mut released);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, released);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.started, vec![(next, PoolAttemptId::new(2))]);
    assert_eq!(driver.log.cancelled, vec![(task_id, Some(attempt_id))]);
    let mut stale = Vec::new();
    driver.router.on_event(RouterEvent::AttemptChanged {
        session, task_id, attempt_id, state: AttemptState::Cancelled,
    }, &NothingObsolete, &mut stale);
    assert!(stale.is_empty());
}

#[test]
fn registered_origin_cancellation_retains_capacity_until_exact_local_release() {
    let task_id = derived_task("G1");
    let next = derived_task("G2");
    let mut driver = Driver::new(&[task_id, next]);
    driver.take_leadership();
    let mut origin = Vec::new();
    driver.worker.on_event(WorkerEvent::OriginAttemptStart {
        declaration: Box::new(declaration("G1")),
    }, &mut driver.domain, &mut origin);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, origin);
    let attempt_id = driver.worker.attempt_for_task(&task_id).unwrap();
    assert!(!driver.route(&ticket("G2")), "registration announces the running origin's capacity");
    let mut cancellation = Vec::new();
    driver.router.on_event(RouterEvent::TaskRemoved { task_id }, &NothingObsolete, &mut cancellation);
    driver.deliver_offers(cancellation);
    assert_eq!(driver.log.cancelled, vec![(task_id, Some(attempt_id))],
        "task removal must cancel registered origin work, not only allocations");
    assert_eq!(driver.worker.attempt_for_task(&task_id), Some(attempt_id));
    let mut stale = Vec::new();
    driver.router.on_event(RouterEvent::AttemptChanged {
        session: driver.router_session(), task_id, attempt_id: PoolAttemptId::new(99),
        state: AttemptState::Cancelled,
    }, &NothingObsolete, &mut stale);
    assert!(stale.is_empty());
    driver.router.on_event(RouterEvent::RoutingTick, &NothingObsolete, &mut stale);
    assert!(stale.is_empty(), "cancellation request/stale receipt cannot release origin capacity");
    let mut finished = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptFinished {
        task_id, attempt_id, fact: cancelled_fact("task no longer scheduled"),
    }, &mut driver.domain, &mut finished);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, finished);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.started, vec![(task_id, attempt_id), (next, PoolAttemptId::new(2))]);
    assert!(driver.worker.attempt_for_task(&task_id).is_none());
}

#[cfg_attr(test, test)]
pub(crate) fn resolved_computation_survives_domain_changes_and_authorized_reconnect() {
    for origin in [false, true] {
        let task_id = derived_task("G1");
        let mut driver = Driver::new(&[task_id]);
        driver.take_leadership();
        driver.log.hold_persistence = true;
        if origin {
            let mut out = Vec::new();
            driver.worker.on_event(WorkerEvent::OriginAttemptStart {
                declaration: Box::new(declaration("G1")),
            }, &mut driver.domain, &mut out);
            drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, out);
        }
        driver.route(&ticket("G1"));
        let (task_id, attempt_id) = driver.log.pending_persistence.remove(0);
        driver.domain.multiplier = 3;
        let mut observed = Vec::new();
        driver.worker.on_event(WorkerEvent::TicketObserved { ticket: Box::new(ticket("G1")) },
            &mut driver.domain, &mut observed);
        drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, observed);
        let old = driver.router_session();
        driver.router.on_event(RouterEvent::SessionClosed { session: old }, &NothingObsolete, &mut Vec::new());
        let mut registration = Vec::new();
        driver.worker.on_event(WorkerEvent::RouteDiscovered {
            claim: Box::new(driver.router.own_claim().unwrap().clone()),
            session: SessionKey { session: RouterSessionId::new(2), ..old },
        }, &mut driver.domain, &mut registration);
        drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, registration);
        driver.deliver_offers(Vec::new());
        assert!(driver.log.computed.is_empty());
        driver.log.hold_persistence = false;
        let mut acknowledged = Vec::new();
        driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id },
            &mut driver.domain, &mut acknowledged);
        drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, acknowledged);
        driver.deliver_offers(Vec::new());
        assert_eq!(driver.log.computed, vec![(task_id, attempt_id, 240)]);
        let mut retry = Vec::new();
        driver.worker.on_event(WorkerEvent::AttemptFailed { task_id, attempt_id },
            &mut driver.domain, &mut retry);
        drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, retry);
        driver.deliver_offers(Vec::new());
        assert_eq!(driver.log.computed, vec![(task_id, attempt_id, 240), (task_id, PoolAttemptId::new(2), 360)]);
    }
}

#[cfg_attr(test, test)]
pub(crate) fn cancelled_resolution_never_executes_and_stale_ack_cannot_start_replacement() {
    let task_id = derived_task("G1");
    let mut driver = Driver::new(&[task_id]);
    driver.take_leadership();
    driver.log.hold_persistence = true;
    driver.route(&ticket("G1"));
    let (_, old_attempt) = driver.log.pending_persistence.remove(0);
    let mut cancelled = Vec::new();
    driver.router.on_event(RouterEvent::TaskRemoved { task_id }, &NothingObsolete, &mut cancelled);
    driver.deliver_offers(cancelled);
    let mut acknowledged = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id: old_attempt },
        &mut driver.domain, &mut acknowledged);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, acknowledged);
    driver.deliver_offers(Vec::new());
    assert!(driver.log.computed.is_empty());
    driver.domain.multiplier = 3;
    let mut offered = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G1")) },
        &NothingObsolete, &mut offered);
    driver.deliver_offers(offered);
    let (_, replacement) = driver.log.pending_persistence.remove(0);
    let mut stale = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id: old_attempt },
        &mut driver.domain, &mut stale);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, stale);
    assert!(driver.log.computed.is_empty());
    assert_eq!(driver.worker.attempt_for_task(&task_id), Some(replacement));
    let mut actual = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptPersisted { task_id, attempt_id: replacement },
        &mut driver.domain, &mut actual);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, actual);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.computed, vec![(task_id, replacement, 360)]);
}

#[cfg_attr(test, test)]
pub(crate) fn malformed_remote_terminal_observation_cannot_cancel_valid_running_work() {
    let task_id = derived_task("G1");
    let next = derived_task("G2");
    let mut driver = Driver::new(&[task_id, next]);
    driver.take_leadership();
    driver.route(&ticket("G1"));
    let attempt_id = driver.worker.attempt_for_task(&task_id).unwrap();
    let mut malformed = ticket("G1");
    malformed.declaration.declaration.effect_policy = EffectPolicy::AuthoritativePlacement;
    malformed.declaration.declaration.placement = Preference::AnyNode;
    malformed.terminal.insert(node(8), terminal_lane(node(8), 1, cancelled_fact("invalid authority")));
    let mut actions = Vec::new();
    driver.worker.on_event(WorkerEvent::TicketObserved { ticket: Box::new(malformed.clone()) },
        &mut driver.domain, &mut actions);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, actions);
    let mut remote = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(malformed) },
        &NothingObsolete, &mut remote);
    driver.deliver_offers(remote);
    assert!(driver.log.cancelled.is_empty());
    assert!(driver.log.incorporated.is_empty());
    assert_eq!(driver.worker.attempt_for_task(&task_id), Some(attempt_id));
    driver.tickets.insert(next, ticket("G2"));
    let mut pending = Vec::new();
    driver.router.on_event(RouterEvent::TaskObserved { ticket: Box::new(ticket("G2")) },
        &NothingObsolete, &mut pending);
    driver.deliver_offers(pending);
    assert_eq!(driver.log.started, vec![(task_id, attempt_id)]);
    let mut finished = Vec::new();
    driver.worker.on_event(WorkerEvent::AttemptFinished { task_id, attempt_id, fact: success_fact(1, "valid") },
        &mut driver.domain, &mut finished);
    drain_worker(&mut driver.worker, &mut driver.router, &mut driver.domain, &mut driver.log, finished);
    driver.deliver_offers(Vec::new());
    assert_eq!(driver.log.started, vec![(task_id, attempt_id), (next, PoolAttemptId::new(2))]);
}
