//! Sans-I/O executor machine: registration, offer acceptance, and durable
//! attempt start (ADR 011 §1, §7, §10).
//!
//! The worker never executes anything itself — it decides *whether* and *when*
//! to hand an obligation to the driver's local dispatch, and it enforces the
//! orderings that make duplication and cancellation safe across a crash or a
//! partition:
//!
//! * **Persist before start.** An accepted offer emits a durable attempt write
//!   ([`WorkerAction::PersistAttempt`]) and only starts dispatch after the driver
//!   acknowledges it ([`WorkerEvent::AttemptPersisted`]). A crash between accept
//!   and acknowledgement therefore leaves either no attempt or a durable attempt
//!   that restart reconciliation can see — never a running dispatch the router
//!   believes was never allocated. The acknowledgement is explicit because
//!   durability is I/O: the machine cannot itself persist anything.
//! * **Cancellation wins the ordering.** A cancellation that arrives before the
//!   persistence acknowledgement suppresses the start entirely: the machine
//!   records the pending cancellation and, on acknowledgement, reports the
//!   attempt cancelled instead of starting it.
//! * **Every offer is the same path.** Origin fast-path starts use
//!   `PersistAttempt` exactly as router-allocated ones do, and origin starts are
//!   subject to the same readiness, capacity, and settled checks — the fast path
//!   skips the router round trip, not local validation.
//!
//! The executor is also the authoritative readiness decision (ADR 011 §10): the
//! router may cache a classification for scheduling efficiency, but only this
//! node's domain adapter can decide that the exact inputs are materializable.

use crate::interlude::*;

use super::model::{
    ActiveAttemptSummary, AllocationId, AttemptState, CapabilitySummary, Capacity, CoordinationIds,
    DeclineReason, NodeIncarnationId, NodePubkey, OpaqueReason, PoolAttemptId, PoolTaskId,
    ReadinessWatch, ResolvedInvocation, SchedulingProtocolVersion, TaskClassification,
    TaskDeclaration, TaskPoolId, TaskTicket, TerminalFact, TerminalSummary,
};
use super::router::{RouterClaim, SessionKey};

/// Exact asynchronous classification fence. Results require the same pending
/// evidence, declaration and session/allocation, not a cached readiness guess.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ClassificationToken {
    pub request: u64,
    pub task_id: PoolTaskId,
    pub declaration_digest: [u8; 32],
    pub ticket_version: [u8; 32],
    pub session: Option<SessionKey>,
    pub allocation: Option<AllocationId>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DispatchToken {
    pub task_id: PoolTaskId,
    pub attempt_id: PoolAttemptId,
    pub declaration_digest: [u8; 32],
    pub allocation: Option<AllocationId>,
    pub session: Option<SessionKey>,
}

/// What one node tells a router on registration (ADR 011 §7).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub node: NodePubkey,
    pub incarnation: NodeIncarnationId,
    pub capabilities: CapabilitySummary,
    pub capacity: Capacity,
    pub protocol: SchedulingProtocolVersion,
    pub active_attempts: Vec<ActiveAttemptSummary>,
}

/// Worker inputs.
#[derive(Debug)]
pub enum WorkerEvent {
    Classified {
        token: ClassificationToken,
        classification: TaskClassification,
    },
    /// Actual authorized runtime admission, not preparation or queued I/O.
    DispatchStarted { token: DispatchToken },
    /// The driver discovered a router heartbeat for `claim` and established a
    /// session to it. Reconnection is just another discovery.
    RouteDiscovered {
        claim: Box<RouterClaim>,
        session: SessionKey,
    },
    /// The session to the current router died.
    RouterLost { session: SessionKey },
    /// An offer arrived on the live session. The driver fetched the ticket named
    /// by the declaration; it may be absent if synchronization has not landed it.
    OfferReceived {
        session: SessionKey,
        allocation_id: AllocationId,
        task_id: PoolTaskId,
        declaration: Box<TaskDeclaration>,
        ticket: Option<Box<TaskTicket>>,
    },
    /// The router told this executor to start an accepted allocation.
    StartReceived {
        session: SessionKey,
        allocation_id: AllocationId,
        task_id: PoolTaskId,
    },
    /// Best-effort cancellation for a task.
    CancelReceived {
        session: SessionKey,
        task_id: PoolTaskId,
        reason: OpaqueReason,
    },
    /// The driver durably persisted an attempt, so dispatch may now start.
    AttemptPersisted {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
    },
    /// The driver could not durably persist an attempt, so dispatch must not
    /// start and the attempt is released.
    AttemptPersistFailed {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
        reason: OpaqueReason,
    },
    /// The local attempt reached a terminal state.
    AttemptFinished {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
        fact: TerminalFact,
    },
    /// Local execution failed without settling the global task. Release only
    /// this exact attempt; the router may allocate the still-pending obligation.
    AttemptFailed {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
    },
    /// The domain incorporated a terminal fact. The exact finishing record is
    /// removed before reporting terminal state to the router, so that capacity
    /// release cannot outrun domain incorporation on independent I/O channels.
    TerminalFactAccepted {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
    },
    /// A ticket became visible locally (BigSync projection).
    TicketObserved { ticket: Box<TaskTicket> },
    /// Local input or coordination progress permits a reoffer. This hint does
    /// not authorize dispatch; the next offer must be classified again.
    ReadinessWakeup { task_id: PoolTaskId },
    /// Start an attempt from the origin fast path: no router round trip, but the
    /// same durable attempt path and the same local validation.
    OriginAttemptStart { ticket: Box<TaskTicket> },
}

/// Worker outputs. Every I/O the driver must perform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerAction {
    ClassifyTask {
        token: ClassificationToken,
        ticket: Box<TaskTicket>,
    },
    /// Open a routing session and register the current bounded attempt set.
    RegisterSession {
        session: SessionKey,
        registration: WorkerRegistration,
    },
    /// Accept an offer.
    AcceptOffer {
        session: SessionKey,
        allocation_id: AllocationId,
        attempt_id: PoolAttemptId,
    },
    /// Decline an offer with the authoritative local reason.
    DeclineOffer {
        session: SessionKey,
        allocation_id: AllocationId,
        reason: DeclineReason,
    },
    /// Durably write the attempt *before* any dispatch starts. The driver reports
    /// completion with [`WorkerEvent::AttemptPersisted`] or
    /// [`WorkerEvent::AttemptPersistFailed`].
    PersistAttempt {
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
        allocation: Option<AllocationId>,
        declaration: Box<TaskDeclaration>,
        invocation: ResolvedInvocation,
        declaration_digest: [u8; 32],
    },
    /// Hand a terminal fact to the domain for incorporation. Only `Some` names
    /// a local finishing attempt awaiting `TerminalFactAccepted`; observed remote
    /// facts carry `None` and do not authorize release of any local attempt.
    DomainTerminalFact {
        declaration: Box<TaskDeclaration>,
        summary: TerminalSummary,
        attempt_id: Option<PoolAttemptId>,
    },
    /// Begin local dispatch. Emitted only after `PersistAttempt` is acknowledged.
    StartDispatch {
        token: DispatchToken,
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
        declaration: Box<TaskDeclaration>,
        invocation: ResolvedInvocation,
        declaration_digest: [u8; 32],
    },
    /// Ask local dispatch to cancel a running or persisted attempt.
    CancelAttempt {
        task_id: PoolTaskId,
        attempt_id: Option<PoolAttemptId>,
        reason: OpaqueReason,
    },
    /// Report an attempt transition to the router.
    ReportAttemptChanged {
        session: SessionKey,
        task_id: PoolTaskId,
        attempt_id: PoolAttemptId,
        state: AttemptState,
    },
    /// Install/replace a one-shot task-keyed local input subscription. The
    /// driver retains it across routing reconnects and reports `ReadinessWakeup`.
    WatchReadiness {
        task_id: PoolTaskId,
        watch: ReadinessWatch,
    },
    /// A local input changed; the router may reoffer, but must not infer readiness.
    ReportReadinessWakeup {
        session: SessionKey,
        task_id: PoolTaskId,
    },
    /// The router's scheduling protocol is outside this node's supported set.
    /// The worker stays idle and waits; it must not form a competing election.
    UnsupportedRouterProtocol {
        session: SessionKey,
        claim_protocol: SchedulingProtocolVersion,
    },
}

/// Machine configuration fixed at construction.
pub struct WorkerConfig {
    pub pool: TaskPoolId,
    pub node: NodePubkey,
    pub incarnation: NodeIncarnationId,
    pub capabilities: CapabilitySummary,
    pub capacity: Capacity,
    /// Scheduling protocols this node can speak. An offer from a router outside
    /// this set is declined and the node waits for a compatible router.
    pub supported_protocols: Vec<SchedulingProtocolVersion>,
}

/// Where an attempt is in the persist/start handshake.
#[derive(Clone, PartialEq, Eq, Debug)]
struct AttemptRecord {
    task_id: PoolTaskId,
    attempt_id: PoolAttemptId,
    declaration: Box<TaskDeclaration>,
    /// Retained until persistence is acknowledged, then moved into dispatch.
    invocation: Option<ResolvedInvocation>,
    declaration_digest: [u8; 32],
    allocation: Option<AllocationId>,
    /// Whether `PersistAttempt` has been emitted. Separate from
    /// `persisted` (the driver's acknowledgement) so a `StartReceived` arriving
    /// before the acknowledgement does not emit a second durable write.
    persist_emitted: bool,
    persisted: bool,
    start_requested: bool,
    start_emitted: Option<DispatchToken>,
    running: bool,
    /// Cancellation received before the persistence acknowledgement. Its presence
    /// suppresses the start when the acknowledgement lands, and cancels the
    /// attempt when a failure lands.
    cancel_before_ack: Option<OpaqueReason>,
    /// A terminal fact whose domain incorporation has not been acknowledged.
    /// Retained so a stale completion cannot be confused with a live one.
    finishing: Option<TerminalFact>,
}

impl AttemptRecord {
    fn is_running(&self) -> bool {
        self.running
    }
}

/// The sans-I/O per-pool executor.
pub struct PoolWorkerMachine<IdSource> {
    config: WorkerConfig,
    ids: IdSource,
    /// The router session this worker currently serves, if any.
    current: Option<SessionKey>,
    /// Attempts by allocation id (router-allocated) or task (origin fast path).
    attempts_by_allocation: BTreeMap<AllocationId, AttemptRecord>,
    origin_attempts: BTreeMap<PoolTaskId, AttemptRecord>,
    /// Tasks whose local classification is `NotReady`/`CoordinationIncomplete`.
    blocked_ready: BTreeMap<PoolTaskId, Option<ReadinessWatch>>,
    /// Local durable knowledge that an obligation is settled. Retained so a
    /// stale remote ticket reintroduced by an old replica is classified obsolete
    /// rather than executed.
    settled: BTreeSet<PoolTaskId>,
    /// The latest summary handed to the domain, whether pending or accepted.
    /// Repeated views are inert, but monotonic cancellation-to-success updates
    /// must still reach the domain even after local settlement.
    handed_terminal: BTreeMap<PoolTaskId, TerminalSummary>,
    classification_sequence: u64,
    classifications: BTreeMap<PoolTaskId, (ClassificationToken, WorkerEvent)>,
}

impl<IdSource: CoordinationIds> PoolWorkerMachine<IdSource> {
    #[must_use]
    pub fn new(config: WorkerConfig, ids: IdSource) -> Self {
        Self {
            config,
            ids,
            current: None,
            attempts_by_allocation: BTreeMap::new(),
            origin_attempts: BTreeMap::new(),
            blocked_ready: BTreeMap::new(),
            settled: BTreeSet::new(),
            handed_terminal: BTreeMap::new(),
            classification_sequence: 0,
            classifications: BTreeMap::new(),
        }
    }

    /// Reattach an already queued durable dispatch before opening any routing
    /// session. This is not recovery of a prepared offer: the driver must prove
    /// that the exact attempt was authorized and has an existing workflow job.
    /// Its captured invocation remains in that job, so recovery neither
    /// reclassifies inputs nor emits another persistence/start action. Old
    /// connection allocations are discarded, but the attempt identity survives.
    pub fn restore_running_attempt(
        &mut self,
        attempt_id: PoolAttemptId,
        declaration: Box<TaskDeclaration>,
    ) {
        self.restore_attempt(attempt_id, declaration, None, true);
    }

    fn restore_attempt(
        &mut self,
        attempt_id: PoolAttemptId,
        declaration: Box<TaskDeclaration>,
        invocation: Option<ResolvedInvocation>,
        running: bool,
    ) {
        assert!(
            self.current.is_none(),
            "restore attempts before routing registration"
        );
        assert_eq!(
            declaration.pool, self.config.pool,
            "recovered attempt belongs to another pool"
        );
        assert!(
            declaration.validate_local().is_valid(),
            "invalid recovered declaration"
        );
        assert!(
            !matches!(declaration.placement, super::model::Preference::Only(node) if node != self.config.node),
            "recovered authoritative attempt belongs to another executor"
        );
        let task_id = declaration.task_id;
        assert!(
            !self.settled.contains(&task_id),
            "recovered attempt is already settled"
        );
        assert!(
            self.attempt_for_task(&task_id).is_none(),
            "duplicate recovered task attempt"
        );
        assert!(
            self.origin_attempts
                .values()
                .all(|record| record.attempt_id != attempt_id),
            "recovered attempt identity reused for another task"
        );
        let declaration_digest = declaration.canonical_digest();
        self.origin_attempts.insert(
            task_id,
            AttemptRecord {
                task_id,
                attempt_id,
                declaration,
                invocation,
                declaration_digest,
                allocation: None,
                persist_emitted: true,
                persisted: true,
                start_requested: true,
                start_emitted: None,
                running,
                cancel_before_ack: None,
                finishing: None,
            },
        );
    }

    /// Reconcile a durable preparation with no journal-proven queued job. The
    /// driver's real adapter rechecks execution policy before starting it.
    pub fn restore_prepared_attempt(
        &mut self,
        attempt_id: PoolAttemptId,
        declaration: Box<TaskDeclaration>,
        invocation: ResolvedInvocation,
    ) {
        self.restore_attempt(attempt_id, declaration, Some(invocation), false);
    }

    /// The router session this worker currently serves.
    #[must_use]
    pub fn current_session(&self) -> Option<SessionKey> {
        self.current
    }

    /// Lets the driver drop owned work as soon as its exact receipt is obsolete.
    pub(crate) fn classification_pending(&self, token: &ClassificationToken) -> bool {
        self.classifications
            .get(&token.task_id)
            .is_some_and(|(pending, _)| pending == token)
    }

    /// Attempts currently running locally.
    #[must_use]
    pub fn running_attempts(&self) -> usize {
        self.attempts_by_allocation
            .values()
            .chain(self.origin_attempts.values())
            .filter(|attempt| attempt.is_running())
            .count()
    }

    /// The attempt id for a task, if this node has an attempt record.
    #[must_use]
    pub fn attempt_for_task(&self, task_id: &PoolTaskId) -> Option<PoolAttemptId> {
        self.attempts_by_allocation
            .values()
            .chain(self.origin_attempts.values())
            .find(|attempt| attempt.task_id == *task_id)
            .map(|attempt| attempt.attempt_id)
    }

    fn record_for_task_mut(&mut self, task_id: &PoolTaskId) -> Option<&mut AttemptRecord> {
        if let Some(allocation_id) = self
            .attempts_by_allocation
            .iter()
            .find(|(_, record)| record.task_id == *task_id)
            .map(|(allocation_id, _)| *allocation_id)
        {
            return self.attempts_by_allocation.get_mut(&allocation_id);
        }
        self.origin_attempts.get_mut(task_id)
    }

    /// Retire one live routing agreement without abandoning authorized work.
    /// Offers never authorized to start are released; an in-flight write keeps
    /// its cancellation fence until its eventual acknowledgment or failure.
    fn retire_allocations(&mut self, out: &mut Vec<WorkerAction>) {
        for (_, mut record) in std::mem::take(&mut self.attempts_by_allocation) {
            record.allocation = None;
            if !record.start_requested && !record.running {
                let reason = OpaqueReason::from_label("router session retired before start");
                if record.persisted {
                    out.push(WorkerAction::CancelAttempt {
                        task_id: record.task_id,
                        attempt_id: Some(record.attempt_id),
                        reason,
                    });
                    continue;
                }
                if !record.persist_emitted {
                    continue;
                }
                record.cancel_before_ack = Some(reason);
            }
            self.origin_attempts.insert(record.task_id, record);
        }
    }

    fn remove_record_for_task(&mut self, task_id: &PoolTaskId) -> Option<AttemptRecord> {
        if let Some(allocation_id) = self
            .attempts_by_allocation
            .iter()
            .find(|(_, record)| record.task_id == *task_id)
            .map(|(allocation_id, _)| *allocation_id)
        {
            return self.attempts_by_allocation.remove(&allocation_id);
        }
        self.origin_attempts.remove(task_id)
    }

    /// The bounded active-attempt set reported on registration. Only attempts the
    /// *executor* is responsible for are announced, which is what stops a new
    /// router from re-offering work a surviving executor already runs.
    fn active_attempt_summaries(&self) -> Vec<ActiveAttemptSummary> {
        self.attempts_by_allocation
            .values()
            .chain(self.origin_attempts.values())
            .filter(|attempt| attempt.persist_emitted)
            .map(|attempt| ActiveAttemptSummary {
                task_id: attempt.task_id,
                attempt_id: attempt.attempt_id,
                allocation: attempt.allocation,
            })
            .collect()
    }

    fn registration(&self, protocol: SchedulingProtocolVersion) -> WorkerRegistration {
        WorkerRegistration {
            node: self.config.node,
            incarnation: self.config.incarnation,
            capabilities: self.config.capabilities.clone(),
            capacity: self.config.capacity,
            protocol,
            active_attempts: self.active_attempt_summaries(),
        }
    }

    /// Handle one event, pushing bounded actions into `out`.
    pub fn on_event(&mut self, event: WorkerEvent, out: &mut Vec<WorkerAction>) {
        let event = match event {
            WorkerEvent::OriginAttemptStart { ticket } if ticket.is_terminal() => {
                WorkerEvent::TicketObserved { ticket }
            }
            WorkerEvent::Classified {
                token,
                classification,
            } => {
                let Some((expected, _)) = self.classifications.get(&token.task_id) else {
                    return;
                };
                if *expected != token {
                    return;
                }
                let (_, original) = self
                    .classifications
                    .remove(&token.task_id)
                    .expect(ERROR_IMPOSSIBLE);
                if token.session.is_some() && token.session != self.current {
                    return;
                }
                self.apply_event(original, Some(classification), out);
                return;
            }
            event => event,
        };
        let request = match &event {
            WorkerEvent::OfferReceived {
                session,
                allocation_id,
                ticket: Some(ticket),
                ..
            } if self.current == Some(*session) => Some((
                ticket.as_ref().clone(),
                Some(*session),
                Some(*allocation_id),
            )),
            WorkerEvent::TicketObserved { ticket } if !ticket.is_terminal() => {
                Some((ticket.as_ref().clone(), None, None))
            }
            WorkerEvent::OriginAttemptStart { ticket } => {
                Some((ticket.as_ref().clone(), None, None))
            }
            _ => None,
        };
        if let WorkerEvent::RouterLost { session } = &event {
            self.classifications
                .retain(|_, (token, _)| token.session != Some(*session));
        }
        if let WorkerEvent::CancelReceived {
            task_id, session, ..
        } = &event
            && self.current == Some(*session)
        {
            self.classifications.remove(task_id);
        }
        if let WorkerEvent::TicketObserved { ticket } = &event
            && ticket.is_terminal()
        {
            self.classifications.remove(&ticket.task_id);
        }
        if let Some((ticket, session, allocation)) = request {
            let task_id = ticket.task_id;
            self.classification_sequence = self
                .classification_sequence
                .checked_add(1)
                .expect(ERROR_IMPOSSIBLE);
            let token = ClassificationToken {
                request: self.classification_sequence,
                task_id,
                declaration_digest: ticket.declaration.declaration.canonical_digest(),
                ticket_version: ticket.version_digest(),
                session,
                allocation,
            };
            if let Some((old, _)) = self.classifications.insert(task_id, (token.clone(), event))
                && let (Some(session), Some(allocation_id)) = (old.session, old.allocation)
            {
                out.push(WorkerAction::DeclineOffer {
                    session,
                    allocation_id,
                    reason: DeclineReason::Unavailable,
                });
            }
            out.push(WorkerAction::ClassifyTask {
                token,
                ticket: Box::new(ticket),
            });
            return;
        }
        self.apply_event(event, None, out);
    }

    fn apply_event(
        &mut self,
        event: WorkerEvent,
        classification: Option<TaskClassification>,
        out: &mut Vec<WorkerAction>,
    ) {
        match event {
            WorkerEvent::Classified { .. } => {
                unreachable!("classification results are consumed at ingress")
            }
            WorkerEvent::DispatchStarted { token } => {
                let Some(record) = self.record_for_task_mut(&token.task_id) else {
                    return;
                };
                if record.start_emitted.as_ref() != Some(&token)
                    || record.cancel_before_ack.is_some()
                    || record.finishing.is_some()
                    || record.running
                {
                    return;
                }
                record.running = true;
                if let Some(session) = self.current {
                    out.push(WorkerAction::ReportAttemptChanged {
                        session,
                        task_id: token.task_id,
                        attempt_id: token.attempt_id,
                        state: AttemptState::Running,
                    });
                }
            }
            WorkerEvent::RouteDiscovered { claim, session } => {
                // SessionKey names this executor, not the remote router. The
                // authenticated RPC boundary binds reports to that same identity.
                assert_eq!(
                    session.node, self.config.node,
                    "routing session names another executor"
                );
                assert_eq!(
                    session.incarnation, self.config.incarnation,
                    "routing session names another executor incarnation"
                );
                if !self
                    .config
                    .supported_protocols
                    .contains(&claim.scheduling_protocol)
                {
                    self.current = None;
                    self.retire_allocations(out);
                    out.push(WorkerAction::UnsupportedRouterProtocol {
                        session,
                        claim_protocol: claim.scheduling_protocol,
                    });
                    return;
                }
                // A new router incarnation supersedes any previous session;
                // attempts are not abandoned, they are re-announced.
                if self.current != Some(session) {
                    self.retire_allocations(out);
                }
                self.current = Some(session);
                out.push(WorkerAction::RegisterSession {
                    session,
                    registration: self.registration(claim.scheduling_protocol),
                });
            }
            WorkerEvent::RouterLost { session } => {
                if self.current != Some(session) {
                    return;
                }
                self.current = None;
                self.retire_allocations(out);
            }
            WorkerEvent::OfferReceived {
                session,
                allocation_id,
                task_id,
                declaration,
                ticket,
            } => {
                self.on_offer(
                    session,
                    allocation_id,
                    task_id,
                    declaration,
                    ticket,
                    classification,
                    out,
                );
            }
            WorkerEvent::StartReceived {
                session,
                allocation_id,
                task_id,
            } => {
                if self.current != Some(session) {
                    return;
                }
                let Some(record) = self.attempts_by_allocation.get_mut(&allocation_id) else {
                    return;
                };
                if record.task_id != task_id {
                    return;
                }
                record.start_requested = true;
                self.persist_and_maybe_start(allocation_id, out);
            }
            WorkerEvent::CancelReceived {
                session,
                task_id,
                reason,
            } => {
                if self.current != Some(session) {
                    return;
                }
                self.cancel_task(&task_id, reason, out);
            }
            WorkerEvent::AttemptPersisted {
                task_id,
                attempt_id,
            } => {
                self.on_attempt_persisted(&task_id, attempt_id, out);
            }
            WorkerEvent::AttemptPersistFailed {
                task_id,
                attempt_id,
                reason,
            } => {
                // Durability was not achieved, so the attempt must not run. The
                // router is told so it can allocate the task elsewhere. A stale
                // failure for an attempt this node already replaced is ignored,
                // because removing by task id would delete the live attempt.
                let matching = self
                    .record_for_task_mut(&task_id)
                    .is_some_and(|record| record.attempt_id == attempt_id);
                if !matching {
                    return;
                }
                self.remove_record_for_task(&task_id);
                if let Some(session) = self.current {
                    out.push(WorkerAction::ReportAttemptChanged {
                        session,
                        task_id,
                        attempt_id,
                        state: AttemptState::Failed,
                    });
                }
                out.push(WorkerAction::CancelAttempt {
                    task_id,
                    attempt_id: Some(attempt_id),
                    reason,
                });
            }
            WorkerEvent::AttemptFinished {
                task_id,
                attempt_id,
                fact,
            } => {
                self.on_attempt_finished(&task_id, attempt_id, fact, out);
            }
            WorkerEvent::AttemptFailed {
                task_id,
                attempt_id,
            } => {
                let current = self.record_for_task_mut(&task_id).is_some_and(|record| {
                    record.attempt_id == attempt_id && record.finishing.is_none()
                });
                if !current {
                    return;
                }
                self.remove_record_for_task(&task_id);
                if let Some(session) = self.current {
                    out.push(WorkerAction::ReportAttemptChanged {
                        session,
                        task_id,
                        attempt_id,
                        state: AttemptState::Failed,
                    });
                }
            }
            WorkerEvent::TerminalFactAccepted {
                task_id,
                attempt_id,
            } => {
                // The domain incorporated the fact, so local settlement is now
                // durable knowledge and stale reintroductions are inert.
                let matching = self
                    .record_for_task_mut(&task_id)
                    .is_some_and(|record| record.attempt_id == attempt_id);
                if !matching {
                    return;
                }
                let succeeded = self
                    .record_for_task_mut(&task_id)
                    .and_then(|record| record.finishing.as_ref())
                    .expect("terminal acknowledgement requires a finished local attempt")
                    .is_success();
                if succeeded {
                    self.settled.insert(task_id);
                }
                self.remove_record_for_task(&task_id);
                if let Some(session) = self.current {
                    out.push(WorkerAction::ReportAttemptChanged {
                        session,
                        task_id,
                        attempt_id,
                        state: if succeeded {
                            AttemptState::Succeeded
                        } else {
                            AttemptState::Cancelled
                        },
                    });
                }
            }
            WorkerEvent::TicketObserved { ticket } => {
                self.on_ticket_observed(&ticket, classification, out);
            }
            WorkerEvent::ReadinessWakeup { task_id } => {
                self.wake_readiness(task_id, out);
            }
            WorkerEvent::OriginAttemptStart { ticket } => {
                self.on_origin_start(
                    Box::new(ticket.declaration.declaration),
                    classification.expect(ERROR_IMPOSSIBLE),
                    out,
                );
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "event fields stay at the call site rather than in a synthetic struct"
    )]
    fn on_offer(
        &mut self,
        session: SessionKey,
        allocation_id: AllocationId,
        task_id: PoolTaskId,
        declaration: Box<TaskDeclaration>,
        ticket: Option<Box<TaskTicket>>,
        classification: Option<TaskClassification>,
        out: &mut Vec<WorkerAction>,
    ) {
        if self.current != Some(session) {
            out.push(WorkerAction::DeclineOffer {
                session,
                allocation_id,
                reason: DeclineReason::StaleSession,
            });
            return;
        }
        if self.attempt_for_task(&task_id).is_some() {
            out.push(WorkerAction::DeclineOffer {
                session,
                allocation_id,
                reason: DeclineReason::Unavailable,
            });
            return;
        }
        if self.outstanding_attempts() >= self.config.capacity.max_concurrent_attempts {
            out.push(WorkerAction::DeclineOffer {
                session,
                allocation_id,
                reason: DeclineReason::Unavailable,
            });
            return;
        }
        // The offer's declaration must match the ticket it names and belong to
        // this pool; otherwise a hostile or stale router could make this node run
        // an obligation it never synchronized.
        if declaration.task_id != task_id
            || declaration.pool != self.config.pool
            || !declaration.validate_local().is_valid()
            || ticket.as_ref().is_some_and(|ticket| {
                ticket.task_id != task_id || ticket.declaration.declaration != *declaration
            })
        {
            out.push(WorkerAction::DeclineOffer {
                session,
                allocation_id,
                reason: DeclineReason::Invalid,
            });
            return;
        }

        // The executor performs the final classification. A missing ticket is
        // `CoordinationIncomplete`, not obsolescence.
        let classification = match &ticket {
            None => TaskClassification::CoordinationIncomplete,
            Some(_) => {
                if self.settled.contains(&task_id) {
                    TaskClassification::Obsolete
                } else {
                    classification.expect(ERROR_IMPOSSIBLE)
                }
            }
        };
        match classification {
            TaskClassification::Runnable(invocation) => {
                let attempt_id = self.ids.next_attempt_id();
                self.attempts_by_allocation.insert(
                    allocation_id,
                    AttemptRecord {
                        task_id,
                        attempt_id,
                        declaration_digest: declaration.canonical_digest(),
                        invocation: Some(invocation),
                        declaration,
                        allocation: Some(allocation_id),
                        persist_emitted: false,
                        persisted: false,
                        start_requested: false,
                        start_emitted: None,
                        running: false,
                        cancel_before_ack: None,
                        finishing: None,
                    },
                );
                out.push(WorkerAction::AcceptOffer {
                    session,
                    allocation_id,
                    attempt_id,
                });
                self.persist_and_maybe_start(allocation_id, out);
            }
            TaskClassification::NotReady(watch) => {
                self.wait_for_readiness(task_id, Some(watch), out);
                out.push(WorkerAction::DeclineOffer {
                    session,
                    allocation_id,
                    reason: DeclineReason::NotReady,
                });
            }
            TaskClassification::CoordinationIncomplete => {
                self.wait_for_readiness(task_id, None, out);
                out.push(WorkerAction::DeclineOffer {
                    session,
                    allocation_id,
                    reason: DeclineReason::CoordinationIncomplete,
                });
            }
            TaskClassification::Obsolete => {
                self.settled.insert(task_id);
                out.push(WorkerAction::DeclineOffer {
                    session,
                    allocation_id,
                    reason: DeclineReason::Obsolete,
                });
            }
            TaskClassification::Invalid => {
                out.push(WorkerAction::DeclineOffer {
                    session,
                    allocation_id,
                    reason: DeclineReason::Invalid,
                });
            }
        }
    }

    fn on_origin_start(
        &mut self,
        declaration: Box<TaskDeclaration>,
        classification: TaskClassification,
        out: &mut Vec<WorkerAction>,
    ) {
        let violation = declaration.validate_local();
        assert!(
            violation.is_valid(),
            "local task declaration violates a producer invariant: {violation}"
        );
        let task_id = declaration.task_id;
        if self.attempt_for_task(&task_id).is_some() {
            return;
        }
        if self.settled.contains(&task_id) {
            return;
        }
        if self.outstanding_attempts() >= self.config.capacity.max_concurrent_attempts {
            return;
        }
        if matches!(declaration.placement, super::model::Preference::Only(node) if node != self.config.node)
            || !declaration
                .capabilities
                .is_subset_of(&self.config.capabilities)
        {
            return;
        }
        let invocation = match classification {
            TaskClassification::Runnable(invocation) => invocation,
            TaskClassification::NotReady(watch) => {
                self.wait_for_readiness(task_id, Some(watch), out);
                return;
            }
            TaskClassification::CoordinationIncomplete => {
                self.wait_for_readiness(task_id, None, out);
                return;
            }
            TaskClassification::Invalid => return,
            TaskClassification::Obsolete => {
                self.settled.insert(task_id);
                return;
            }
        };

        let attempt_id = self.ids.next_attempt_id();
        self.origin_attempts.insert(
            task_id,
            AttemptRecord {
                task_id,
                attempt_id,
                declaration_digest: declaration.canonical_digest(),
                invocation: Some(invocation),
                declaration,
                allocation: None,
                persist_emitted: false,
                persisted: false,
                start_requested: true,
                start_emitted: None,
                running: false,
                cancel_before_ack: None,
                finishing: None,
            },
        );
        let record = self
            .origin_attempts
            .get_mut(&task_id)
            .expect(ERROR_IMPOSSIBLE);
        record.persist_emitted = true;
        out.push(WorkerAction::PersistAttempt {
            task_id,
            attempt_id,
            allocation: None,
            declaration: record.declaration.clone(),
            invocation: record.invocation.as_ref().expect(ERROR_IMPOSSIBLE).clone(),
            declaration_digest: record.declaration_digest,
        });
    }

    fn on_attempt_persisted(
        &mut self,
        task_id: &PoolTaskId,
        attempt_id: PoolAttemptId,
        out: &mut Vec<WorkerAction>,
    ) {
        let allocation_key = self
            .attempts_by_allocation
            .iter()
            .find(|(_, record)| record.task_id == *task_id && record.attempt_id == attempt_id)
            .map(|(allocation_id, _)| *allocation_id);

        if let Some(allocation_id) = allocation_key {
            let record = self
                .attempts_by_allocation
                .get_mut(&allocation_id)
                .expect(ERROR_IMPOSSIBLE);
            record.persisted = true;
            if let Some(reason) = record.cancel_before_ack.clone() {
                // Keep the reservation until the actual domain cancellation
                // settles; persistence is not a cancellation receipt.
                out.push(WorkerAction::CancelAttempt {
                    task_id: *task_id,
                    attempt_id: Some(attempt_id),
                    reason,
                });
                return;
            }
            self.persist_and_maybe_start(allocation_id, out);
            return;
        }

        // Origin fast-path attempt. Only acknowledge the *matching* attempt id;
        // a persisted report for a superseded attempt must not start the live one.
        let Some(record) = self.origin_attempts.get_mut(task_id) else {
            return;
        };
        if record.attempt_id != attempt_id {
            return;
        }
        record.persisted = true;
        if let Some(reason) = record.cancel_before_ack.clone() {
            let attempt_id = record.attempt_id;
            out.push(WorkerAction::CancelAttempt {
                task_id: *task_id,
                attempt_id: Some(attempt_id),
                reason,
            });
            return;
        }
        self.start_origin(task_id, out);
    }

    fn on_attempt_finished(
        &mut self,
        task_id: &PoolTaskId,
        attempt_id: PoolAttemptId,
        fact: TerminalFact,
        out: &mut Vec<WorkerAction>,
    ) {
        // Compare the attempt id *before* removing anything: a stale completion
        // from an attempt this node has already replaced must not delete or
        // settle the live attempt.
        let Some(record) = self.record_for_task_mut(task_id) else {
            return;
        };
        if record.attempt_id != attempt_id {
            return;
        }
        if record.finishing.is_some() {
            // Already reported; awaiting domain incorporation.
            return;
        }

        let state = if fact.is_success() {
            AttemptState::Succeeded
        } else {
            AttemptState::Cancelled
        };
        // The domain owns durable result incorporation. Retain capacity until
        // its acknowledgement; a terminal router report would otherwise permit
        // new offers while this finishing record still occupies the worker.
        let declaration = match self.record_for_task_mut(task_id) {
            Some(record) => {
                record.finishing = Some(fact.clone());
                record.declaration.clone()
            }
            None => return,
        };
        let summary = match fact {
            TerminalFact::Succeeded {
                attempt_id,
                result_ref,
            } => TerminalSummary::Succeeded {
                attempt_id,
                result_ref,
            },
            TerminalFact::Cancelled { reason } => TerminalSummary::Cancelled { reason },
        };
        let already_delivered = self.handed_terminal.get(task_id).is_some_and(|previous| {
            previous == &summary
                || matches!(
                    (previous, &summary),
                    (
                        TerminalSummary::Succeeded { .. },
                        TerminalSummary::Cancelled { .. }
                    )
                )
        });
        if already_delivered && self.settled.contains(task_id) {
            self.remove_record_for_task(task_id);
            if let Some(session) = self.current {
                out.push(WorkerAction::ReportAttemptChanged {
                    session,
                    task_id: *task_id,
                    attempt_id,
                    state,
                });
            }
            return;
        }
        self.handed_terminal.insert(*task_id, summary.clone());
        out.push(WorkerAction::DomainTerminalFact {
            declaration,
            summary,
            attempt_id: Some(attempt_id),
        });
    }

    fn on_ticket_observed(
        &mut self,
        ticket: &TaskTicket,
        classification: Option<TaskClassification>,
        out: &mut Vec<WorkerAction>,
    ) {
        if !ticket.declaration.declaration.validate_local().is_valid() {
            return;
        }
        // Settlement prevents re-execution, not propagation of a newly merged
        // terminal result. Inspect the summary first so success can advance an
        // already-observed cancellation.
        if let Some(summary) = ticket.terminal_summary() {
            self.settled.insert(ticket.task_id);
            let deliver = self
                .handed_terminal
                .get(&ticket.task_id)
                .is_none_or(|previous| {
                    previous != &summary
                        && !matches!(
                            (previous, &summary),
                            (
                                TerminalSummary::Succeeded { .. },
                                TerminalSummary::Cancelled { .. }
                            )
                        )
                });
            if deliver {
                self.handed_terminal.insert(ticket.task_id, summary.clone());
                out.push(WorkerAction::DomainTerminalFact {
                    declaration: Box::new(ticket.declaration.declaration.clone()),
                    summary,
                    attempt_id: None,
                });
            }
            self.cancel_task(
                &ticket.task_id,
                OpaqueReason::from_label("obligation settled"),
                out,
            );
            return;
        }
        if self.settled.contains(&ticket.task_id) {
            self.cancel_task(
                &ticket.task_id,
                OpaqueReason::from_label("obligation settled"),
                out,
            );
            return;
        }
        match classification.expect(ERROR_IMPOSSIBLE) {
            TaskClassification::Obsolete => {
                self.settled.insert(ticket.task_id);
                self.cancel_task(
                    &ticket.task_id,
                    OpaqueReason::from_label("obligation obsolete"),
                    out,
                );
            }
            TaskClassification::NotReady(watch) => {
                self.wait_for_readiness(ticket.task_id, Some(watch), out);
            }
            TaskClassification::Runnable(_) => {
                self.wake_readiness(ticket.task_id, out);
            }
            TaskClassification::CoordinationIncomplete => {
                self.wait_for_readiness(ticket.task_id, None, out);
            }
            TaskClassification::Invalid => {}
        }
    }

    fn wait_for_readiness(
        &mut self,
        task_id: PoolTaskId,
        watch: Option<ReadinessWatch>,
        out: &mut Vec<WorkerAction>,
    ) {
        if self.blocked_ready.get(&task_id) == Some(&watch) {
            return;
        }
        if let Some(watch) = &watch {
            out.push(WorkerAction::WatchReadiness {
                task_id,
                watch: watch.clone(),
            });
        }
        self.blocked_ready.insert(task_id, watch);
    }

    fn wake_readiness(&mut self, task_id: PoolTaskId, out: &mut Vec<WorkerAction>) {
        if self.blocked_ready.remove(&task_id).is_some()
            && let Some(session) = self.current
        {
            out.push(WorkerAction::ReportReadinessWakeup { session, task_id });
        }
    }

    /// Emit `PersistAttempt` for an accepted allocation that has not been
    /// persisted yet, and start dispatch if the acknowledgement already landed.
    fn persist_and_maybe_start(
        &mut self,
        allocation_id: AllocationId,
        out: &mut Vec<WorkerAction>,
    ) {
        let Some(record) = self.attempts_by_allocation.get_mut(&allocation_id) else {
            return;
        };
        if !record.persist_emitted {
            record.persist_emitted = true;
            out.push(WorkerAction::PersistAttempt {
                task_id: record.task_id,
                attempt_id: record.attempt_id,
                allocation: Some(allocation_id),
                declaration: record.declaration.clone(),
                invocation: record.invocation.as_ref().expect(ERROR_IMPOSSIBLE).clone(),
                declaration_digest: record.declaration_digest,
            });
            return;
        }
        // Still waiting on the driver's acknowledgement: dispatch must not start.
        if !record.persisted {
            return;
        }
        if record.running || record.start_emitted.is_some() || !record.start_requested {
            return;
        }
        let token = DispatchToken {
            task_id: record.task_id,
            attempt_id: record.attempt_id,
            declaration_digest: record.declaration_digest,
            allocation: record.allocation,
            session: self.current,
        };
        record.start_emitted = Some(token.clone());
        out.push(WorkerAction::StartDispatch {
            token,
            task_id: record.task_id,
            attempt_id: record.attempt_id,
            declaration: record.declaration.clone(),
            invocation: record.invocation.take().expect(ERROR_IMPOSSIBLE),
            declaration_digest: record.declaration_digest,
        });
    }

    fn start_origin(&mut self, task_id: &PoolTaskId, out: &mut Vec<WorkerAction>) {
        let Some(record) = self.origin_attempts.get_mut(task_id) else {
            return;
        };
        if record.running
            || record.start_emitted.is_some()
            || !record.persisted
            || !record.start_requested
        {
            return;
        }
        let token = DispatchToken {
            task_id: record.task_id,
            attempt_id: record.attempt_id,
            declaration_digest: record.declaration_digest,
            allocation: record.allocation,
            session: self.current,
        };
        record.start_emitted = Some(token.clone());
        out.push(WorkerAction::StartDispatch {
            token,
            task_id: record.task_id,
            attempt_id: record.attempt_id,
            declaration: record.declaration.clone(),
            invocation: record.invocation.take().expect(ERROR_IMPOSSIBLE),
            declaration_digest: record.declaration_digest,
        });
    }

    fn cancel_task(
        &mut self,
        task_id: &PoolTaskId,
        reason: OpaqueReason,
        out: &mut Vec<WorkerAction>,
    ) {
        let Some(record) = self.record_for_task_mut(task_id) else {
            return;
        };
        if !record.persisted {
            // Cancellation before the persistence acknowledgement: record it so
            // the acknowledgement suppresses the start.
            record.cancel_before_ack = Some(reason);
            return;
        }
        record.start_emitted = None;
        record.cancel_before_ack = Some(reason.clone());
        out.push(WorkerAction::CancelAttempt {
            task_id: *task_id,
            attempt_id: Some(record.attempt_id),
            reason,
        });
    }

    /// Attempts this node owes, whether or not they have started. Capacity is
    /// reserved at acceptance so outstanding offers cannot over-commit.
    fn outstanding_attempts(&self) -> u32 {
        let count = self.attempts_by_allocation.len() + self.origin_attempts.len();
        u32::try_from(count).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests;
