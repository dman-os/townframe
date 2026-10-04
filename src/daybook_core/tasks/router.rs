//! Sans-I/O router machine: per-pool election, registration, offers, and live
//! allocation (ADR 011 §5–§7, §10).
//!
//! The machine is a pure reducer. It emits bounded [`RouterAction`]s that a
//! role-specific driver applies — persistent slot writes, heartbeat publication,
//! point-to-point RPC, and cancellation — and outcomes come back as events, so
//! takeover timing, protocol preference, and offer/start ordering are
//! reproducible in tests.
//!
//! Two facts this file exists to encode:
//!
//! * **One shared election per pool, no compatibility cohorts.** Claims carry a
//!   live [`SchedulingProtocolVersion`] independent of persisted payload
//!   schemas. The projection prefers the newer live eligible version, and
//!   liveness is a *local monotonic observation* fed by the driver
//!   ([`RouterEvent::HeartbeatExpired`]) — never a signed eternal lease. A dead
//!   historical newer-version claim therefore stops blocking takeover.
//! * **Router-side obsolescence is proven obsolescence.** The router consults
//!   [`RouterDomainAdapter`] only to find durable evidence that an obligation is
//!   settled; absence of local inputs is not proof of global obsolescence, so it
//!   never vetoes a remote ready executor.

use crate::interlude::*;

use super::model::{
    AllocationId, CapabilitySummary, Capacity, CoordinationIds, DeclineReason, NodeIncarnationId,
    NodePubkey, OpaqueReason, PoolClaimId, PoolTaskId, RouterSessionId,
    SchedulingProtocolVersion, TaskDeclaration, TaskPoolId, TaskTicket,
};

/// Durable evidence the router consults before offering a ticket.
///
/// The router half of the domain-neutral adapter. It answers exactly one
/// question — "does the domain hold evidence proving this obligation is settled
/// or no longer desired?" — because that is the only domain fact a router may
/// act on. `false` means *no proof*, not "runnable": readiness belongs to the
/// executor that will actually run the task.
pub trait RouterDomainAdapter {
    fn is_proven_obsolete(&self, declaration: &TaskDeclaration) -> bool;
}

/// A router candidate's signed claim in the pool's election slot (ADR 011 §5).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RouterClaim {
    pub pool: TaskPoolId,
    pub candidate: NodePubkey,
    pub incarnation: NodeIncarnationId,
    pub writer_seq: u64,
    pub generation: u64,
    pub claim_id: PoolClaimId,
    /// Live scheduling/RPC compatibility advertised by this claim. Persisted
    /// payload schemas are separate contracts and are not negotiated here.
    pub scheduling_protocol: SchedulingProtocolVersion,
    /// Causal observation of other candidates' generations at claim time.
    pub observed: BTreeMap<NodePubkey, u64>,
}

impl RouterClaim {
    /// Deterministic rank among claims at the same generation:
    /// `BLAKE3(pool_id || generation || candidate || claim_id)`.
    ///
    /// The lowest rank wins, so the projection is a pure function of slot
    /// contents and every participant computes the same winner.
    #[must_use]
    pub fn rank(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"daybook/router-rank/v1");
        let pool = self.pool.as_str().as_bytes();
        hasher.update(&(pool.len() as u64).to_be_bytes());
        hasher.update(pool);
        hasher.update(&self.generation.to_be_bytes());
        hasher.update(&self.candidate.to_bytes32());
        hasher.update(&self.claim_id.to_u128().to_be_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Identity of this claim generation in the live liveness view.
    #[must_use]
    pub fn liveness_key(&self) -> ClaimLivenessKey {
        ClaimLivenessKey {
            candidate: self.candidate,
            generation: self.generation,
            claim_id: self.claim_id,
        }
    }
}

/// Liveness key of one claim generation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ClaimLivenessKey {
    pub candidate: NodePubkey,
    pub generation: u64,
    pub claim_id: PoolClaimId,
}

/// Why a merged remote claim lane was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClaimCollision {
    #[error("router candidate {candidate} equivocated at writer sequence {writer_seq}")]
    Equivocated {
        candidate: NodePubkey,
        writer_seq: u64,
    },
    #[error("router claim names a different pool than this machine's")]
    ForeignPool { pool: TaskPoolId },
    #[error("router claim generation {generation} moved backwards from {existing}")]
    RegressedGeneration { generation: u64, existing: u64 },
}

/// The election slot: one latest claim lane per candidate (ADR 011 §5).
///
/// Merging by greatest `writer_seq` per candidate; omitting a known lane never
/// deletes it. Equal sequence with unequal bytes is equivocation.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct RouterSlotState {
    lanes: BTreeMap<NodePubkey, RouterClaim>,
}

impl RouterSlotState {
    #[must_use]
    pub fn lanes(&self) -> &BTreeMap<NodePubkey, RouterClaim> {
        &self.lanes
    }

    #[must_use]
    pub fn get(&self, candidate: &NodePubkey) -> Option<&RouterClaim> {
        self.lanes.get(candidate)
    }

    /// Merge one remote lane. Returns whether anything changed.
    pub fn merge_claim(&mut self, claim: &RouterClaim) -> Result<bool, ClaimCollision> {
        match self.lanes.get(&claim.candidate) {
            None => {
                self.lanes.insert(claim.candidate, claim.clone());
                Ok(true)
            }
            Some(existing) if claim.writer_seq > existing.writer_seq => {
                // A writer's lane only moves forward; a claim may not retire a
                // generation it already superseded.
                if claim.generation < existing.generation {
                    return Err(ClaimCollision::RegressedGeneration {
                        generation: claim.generation,
                        existing: existing.generation,
                    });
                }
                self.lanes.insert(claim.candidate, claim.clone());
                Ok(true)
            }
            Some(existing) if claim.writer_seq < existing.writer_seq => Ok(false),
            Some(existing) => {
                if existing != claim {
                    return Err(ClaimCollision::Equivocated {
                        candidate: claim.candidate,
                        writer_seq: claim.writer_seq,
                    });
                }
                Ok(false)
            }
        }
    }

    #[must_use]
    pub fn max_generation(&self) -> u64 {
        self.lanes
            .values()
            .map(|claim| claim.generation)
            .max()
            .unwrap_or_default()
    }
}

/// Identity of one live executor session: the node, its incarnation, and the
/// session id minted for this particular connection.
///
/// All three matter. Node plus incarnation alone cannot tell two connections
/// from the same process apart, so a reconnect would be indistinguishable from
/// the connection it replaced and a late message from the old one would be
/// accepted.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SessionKey {
    pub node: NodePubkey,
    pub incarnation: NodeIncarnationId,
    pub session: RouterSessionId,
}

/// One registered executor session. Internal to the machine: drivers see
/// [`SessionKey`] in events and actions, not the registration record.
#[derive(Clone, PartialEq, Eq, Debug)]
struct RouterSession {
    pub key: SessionKey,
    pub capabilities: CapabilitySummary,
    pub capacity: Capacity,
    pub protocol: SchedulingProtocolVersion,
}

/// One attempt an executor reports as already running, as the router tracks it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct ActiveAttemptRef {
    pub attempt_id: super::model::PoolAttemptId,
    pub allocation: Option<AllocationId>,
}

/// Internal session record: the registration plus the executor's live attempts.
#[derive(Clone, PartialEq, Eq, Debug)]
struct RegisteredSession {
    session: RouterSession,
    active_attempts: HashMap<PoolTaskId, ActiveAttemptRef>,
}

/// Router inputs. Clocks, transport, storage, and identity minting all arrive
/// from the driver.
#[derive(Debug)]
pub enum RouterEvent {
    /// A remote slot merge arrived from BigSync.
    SlotMerged { claims: Vec<RouterClaim> },
    /// A router heartbeat was observed for `claim`, so it is live.
    HeartbeatObserved { claim: ClaimLivenessKey },
    /// The driver's local monotonic clock says `claim`'s heartbeat has been
    /// absent for `router_takeover_after`. Liveness is a local observation, not
    /// a signed lease, so this is what eventually retires even a newer-version
    /// historical claim.
    HeartbeatExpired { claim: ClaimLivenessKey },
    /// The driver's takeover timer fired; the machine decides whether to claim.
    ConsiderTakeover,
    /// The driver durably persisted the claim (and waited `router_settle`),
    /// confirming the claim is visible in the slot.
    ClaimPersisted { generation: u64, claim_id: PoolClaimId },
    /// The claim write failed. Fence the result by the claim's exact identity;
    /// the driver may later request takeover again, but failure grants no role.
    ClaimPersistenceFailed { generation: u64, claim_id: PoolClaimId },
    /// The driver's heartbeat cadence fired.
    HeartbeatDue,
    /// An executor registered (or re-registered) a session. The connection's
    /// session id is a transport fact supplied by the driver, not minted here:
    /// only the driver knows which connection this is.
    RegisterExecutor {
        session: SessionKey,
        capabilities: CapabilitySummary,
        capacity: Capacity,
        protocol: SchedulingProtocolVersion,
        active_attempts: Vec<super::model::ActiveAttemptSummary>,
    },
    /// A session's transport died or the executor explicitly closed.
    SessionClosed { session: SessionKey },
    /// A ticket became visible in the pool's active part.
    TaskObserved { ticket: Box<TaskTicket> },
    /// A ticket left the active part.
    TaskRemoved { task_id: PoolTaskId },
    /// The executor accepted an offer.
    OfferAccepted {
        session: SessionKey,
        allocation_id: AllocationId,
        attempt_id: super::model::PoolAttemptId,
    },
    /// The executor declined an offer.
    OfferDeclined {
        session: SessionKey,
        allocation_id: AllocationId,
        reason: DeclineReason,
    },
    /// An attempt's state changed on the executor.
    AttemptChanged {
        session: SessionKey,
        task_id: PoolTaskId,
        attempt_id: super::model::PoolAttemptId,
        state: super::model::AttemptState,
    },
    /// An exact executor session observed local progress, permitting a reoffer.
    ReadinessWakeup { session: SessionKey, task_id: PoolTaskId },
    /// A scheduling pass was requested. Task observation and wakeups also
    /// trigger one, so this is only for periodic retry cadence.
    RoutingTick,
}

/// Router outputs. The driver owns all I/O these actions require.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterAction {
    /// Durably write this claim into the pool's router-slot object, then wait
    /// `router_settle` before reporting [`RouterEvent::ClaimPersisted`], or report
    /// [`RouterEvent::ClaimPersistenceFailed`] if that write fails.
    PersistClaim { claim: RouterClaim },
    /// Publish a router heartbeat on the pool's BigEphemeral topic.
    PublishHeartbeat { claim: RouterClaim },
    /// Refuse a session that cannot speak a protocol this router supports.
    RejectSession {
        session: SessionKey,
        reason: DeclineReason,
    },
    /// Send a point-to-point offer.
    SendOffer {
        session: SessionKey,
        allocation_id: AllocationId,
        task_id: PoolTaskId,
        declaration: Box<TaskDeclaration>,
    },
    /// Tell an executor to begin a task it accepted. The executor still persists
    /// its attempt before starting.
    StartAttempt {
        session: SessionKey,
        allocation_id: AllocationId,
        task_id: PoolTaskId,
    },
    /// Best-effort cancellation of a running or offered attempt.
    CancelAttempt {
        session: SessionKey,
        task_id: PoolTaskId,
        reason: OpaqueReason,
    },
    /// A remote claim lane was rejected rather than merged.
    RejectClaim { collision: Box<ClaimCollision> },
}

/// Live allocation state, held only here and on the executor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AllocationPhase {
    Offered,
    Accepted(super::model::PoolAttemptId),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Allocation {
    session: SessionKey,
    task_id: PoolTaskId,
    phase: AllocationPhase,
    /// Cancellation is an outstanding effect, not evidence of released capacity.
    cancel_requested: bool,
}

/// Router machine configuration fixed at construction.
pub struct RouterConfig {
    pub pool: TaskPoolId,
    pub node: NodePubkey,
    pub incarnation: NodeIncarnationId,
    /// Scheduling protocols this node can speak. A claim advertising a version
    /// outside this set is ineligible *for this node*, which is how an older
    /// participant waits instead of forming a competing cohort.
    pub supported_protocols: Vec<SchedulingProtocolVersion>,
}

/// The sans-I/O per-pool router.
pub struct RouterMachine<IdSource> {
    config: RouterConfig,
    ids: IdSource,
    slot: RouterSlotState,
    /// Claims whose heartbeat has been observed and not yet expired. Local
    /// monotonic knowledge; the driver feeds both transitions.
    live_claims: BTreeSet<ClaimLivenessKey>,
    own_claim: Option<RouterClaim>,
    /// Whether the driver durably persisted `own_claim`. Leadership requires
    /// both a projected win *and* a visible claim: routing before the write is
    /// confirmed could advertise a router no peer can see, so slots and heartbeats
    /// that arrive in between must not grant leadership.
    own_claim_persisted: bool,
    routing: bool,
    sessions: BTreeMap<SessionKey, RegisteredSession>,
    allocations: BTreeMap<AllocationId, Allocation>,
    /// Pending tickets by task, awaiting a viable offer.
    pending: BTreeMap<PoolTaskId, Box<TaskTicket>>,
    /// Executor-local refusals, released only by the corresponding readiness
    /// or authoritative capacity change. Other executors remain eligible.
    blocked_offers: BTreeMap<(SessionKey, PoolTaskId), DeclineReason>,
}

impl<IdSource: CoordinationIds> RouterMachine<IdSource> {
    #[must_use]
    pub fn new(config: RouterConfig, ids: IdSource) -> Self {
        Self {
            config,
            ids,
            slot: RouterSlotState::default(),
            live_claims: BTreeSet::new(),
            own_claim: None,
            own_claim_persisted: false,
            routing: false,
            sessions: BTreeMap::new(),
            allocations: BTreeMap::new(),
            pending: BTreeMap::new(),
            blocked_offers: BTreeMap::new(),
        }
    }

    /// Whether this machine currently holds router leadership.
    #[must_use]
    pub fn is_routing(&self) -> bool {
        self.routing
    }

    /// The claim this machine published, if any.
    #[must_use]
    pub fn own_claim(&self) -> Option<&RouterClaim> {
        self.own_claim.as_ref()
    }

    /// The currently projected winner over all claims the machine knows.
    ///
    /// The lowest-ranked live claim at the greatest generation, with version
    /// preference applied to live candidacy: among live claims the greatest
    /// [`SchedulingProtocolVersion`] is chosen first and rank breaks ties within
    /// that version. A dead claim never wins over a live one, which is what stops
    /// a historical newer-version claim from permanently blocking takeover.
    #[must_use]
    pub fn projected_winner(&self) -> Option<&RouterClaim> {
        let max_generation = self.slot.max_generation();
        let claims: Vec<&RouterClaim> = self
            .slot
            .lanes()
            .values()
            .filter(|claim| claim.generation == max_generation)
            .collect();
        if claims.is_empty() {
            return None;
        }

        let newest_live_protocol = claims
            .iter()
            .filter(|claim| self.is_live(&claim.liveness_key()))
            .map(|claim| claim.scheduling_protocol)
            .max();
        match newest_live_protocol {
            Some(newest) => claims
                .into_iter()
                .filter(|claim| {
                    claim.scheduling_protocol >= newest && self.is_live(&claim.liveness_key())
                })
                .min_by_key(|claim| claim.rank()),
            // No live claim at the greatest generation: every claim here is
            // dead, so rank alone reports the deterministic stale winner for
            // observation. Leadership still requires liveness.
            None => claims.into_iter().min_by_key(|claim| claim.rank()),
        }
    }

    fn is_live(&self, key: &ClaimLivenessKey) -> bool {
        if let Some(own) = &self.own_claim
            && own.liveness_key() == *key
        {
            return true;
        }
        self.live_claims.contains(key)
    }

    /// Handle one event, pushing bounded actions into `out`.
    pub fn on_event<Adapter: RouterDomainAdapter>(
        &mut self,
        event: RouterEvent,
        adapter: &Adapter,
        out: &mut Vec<RouterAction>,
    ) {
        match event {
            RouterEvent::SlotMerged { claims } => {
                for claim in &claims {
                    if claim.pool != self.config.pool {
                        out.push(RouterAction::RejectClaim {
                            collision: Box::new(ClaimCollision::ForeignPool {
                                pool: claim.pool.clone(),
                            }),
                        });
                        continue;
                    }
                    if let Err(collision) = self.slot.merge_claim(claim) {
                        out.push(RouterAction::RejectClaim {
                            collision: Box::new(collision),
                        });
                    }
                }
                self.reconsider_leadership();
            }
            RouterEvent::HeartbeatObserved { claim } => {
                self.live_claims.insert(claim);
                // A newer live claim can defeat ours: stop routing promptly
                // rather than continuing after the slot says otherwise.
                self.reconsider_leadership();
            }
            RouterEvent::HeartbeatExpired { claim } => {
                self.live_claims.remove(&claim);
                self.reconsider_leadership();
            }
            RouterEvent::ConsiderTakeover => {
                self.consider_takeover(out);
            }
            RouterEvent::ClaimPersisted {
                generation,
                claim_id,
            } => {
                let confirmed = self.own_claim.as_ref().is_some_and(|claim| {
                    claim.generation == generation && claim.claim_id == claim_id
                });
                if confirmed {
                    self.own_claim_persisted = true;
                    self.reconsider_leadership();
                }
            }
            RouterEvent::ClaimPersistenceFailed { generation, claim_id } => {
                if !self.own_claim_persisted
                    && self.own_claim.as_ref().is_some_and(|claim| {
                        claim.generation == generation && claim.claim_id == claim_id
                    })
                {
                    let failed = self.own_claim.take().expect(ERROR_IMPOSSIBLE);
                    self.live_claims.remove(&failed.liveness_key());
                    self.routing = false;
                    // Retain its lane: retries must advance writer sequence and
                    // generation even if a failed write became visible remotely.
                }
            }
            RouterEvent::HeartbeatDue => {
                if self.routing
                    && let Some(claim) = &self.own_claim
                {
                    out.push(RouterAction::PublishHeartbeat {
                        claim: claim.clone(),
                    });
                }
            }
            RouterEvent::RegisterExecutor {
                session,
                capabilities,
                capacity,
                protocol,
                active_attempts,
            } => {
                if !self.config.supported_protocols.contains(&protocol) {
                    out.push(RouterAction::RejectSession {
                        session,
                        reason: DeclineReason::UnsupportedProtocol,
                    });
                    return;
                }
                let attempt_map = active_attempts
                    .into_iter()
                    .map(|attempt| {
                        (
                            attempt.task_id,
                            ActiveAttemptRef {
                                attempt_id: attempt.attempt_id,
                                allocation: attempt.allocation,
                            },
                        )
                    })
                    .collect();
                if self.sessions.get(&session).is_some_and(|registered| {
                    registered.session.capacity != capacity
                        || registered.active_attempts != attempt_map
                }) {
                    self.blocked_offers.retain(|(blocked_session, _), reason| {
                        *blocked_session != session || *reason != DeclineReason::Unavailable
                    });
                }
                self.sessions.insert(
                    session,
                    RegisteredSession {
                        session: RouterSession {
                            key: session,
                            capabilities,
                            capacity,
                            protocol,
                        },
                        active_attempts: attempt_map,
                    },
                );
                // A newly registered session can make already-pending tickets
                // runnable, so scheduling is considered immediately rather than
                // waiting for the next ticket or tick.
                self.try_allocate(adapter, out);
            }
            RouterEvent::SessionClosed { session } => {
                self.sessions.remove(&session);
                // An offer/acceptance that dies with its session is not durable
                // progress: the pending BigSync ticket becomes allocatable again.
                let stale: Vec<AllocationId> = self
                    .allocations
                    .iter()
                    .filter(|(_, allocation)| allocation.session == session)
                    .map(|(allocation_id, _)| *allocation_id)
                    .collect();
                for allocation_id in stale {
                    self.allocations.remove(&allocation_id);
                }
                self.blocked_offers
                    .retain(|(blocked_session, _), _| *blocked_session != session);
                self.try_allocate(adapter, out);
            }
            RouterEvent::TaskObserved { ticket } => {
                self.observe_ticket(&ticket, adapter, out);
            }
            RouterEvent::TaskRemoved { task_id } => {
                self.pending.remove(&task_id);
                self.cancel_task(&task_id, out);
            }
            RouterEvent::OfferAccepted {
                session,
                allocation_id,
                attempt_id,
            } => {
                // A router that has lost the election must not authorize new
                // starts: the winner owns allocation now, and this machine's
                // offer is a leftover the executor should not act on.
                if !self.routing {
                    return;
                }
                let Some(allocation) = self.allocations.get(&allocation_id).copied() else {
                    return;
                };
                if allocation.session != session {
                    return;
                }
                if matches!(allocation.phase, AllocationPhase::Accepted(existing) if existing != attempt_id) {
                    return;
                }
                self.allocations.insert(
                    allocation_id,
                    Allocation {
                        phase: AllocationPhase::Accepted(attempt_id),
                        ..allocation
                    },
                );
                if allocation.cancel_requested {
                    out.push(RouterAction::CancelAttempt {
                        session, task_id: allocation.task_id,
                        reason: OpaqueReason::from_label("task no longer scheduled"),
                    });
                } else {
                    out.push(RouterAction::StartAttempt {
                        session, allocation_id, task_id: allocation.task_id,
                    });
                }
            }
            RouterEvent::OfferDeclined {
                session,
                allocation_id,
                reason,
            } => {
                let Some(allocation) = self.allocations.get(&allocation_id).copied() else {
                    return;
                };
                if allocation.session != session || allocation.phase != AllocationPhase::Offered {
                    // An accepted attempt owns capacity until its exact outcome;
                    // a delayed decline cannot undo that binding.
                    return;
                }
                self.allocations.remove(&allocation_id);
                match reason {
                    DeclineReason::NotReady
                    | DeclineReason::CoordinationIncomplete
                    | DeclineReason::Unavailable
                    | DeclineReason::Obsolete
                    | DeclineReason::Invalid => {
                        // All of these are *this executor's* answer, not proof
                        // about the obligation: `NotReady`/`CoordinationIncomplete`
                        // are local unreadiness, and even `Obsolete`/`Invalid` come
                        // from one node's view. The router only acts on its own
                        // adapter's proven obsolescence, so the task stays pending
                        // for another executor.
                        self.blocked_offers.insert((session, allocation.task_id), reason);
                    }
                    // The executor is gone or unusable; nothing to block.
                    DeclineReason::StaleSession | DeclineReason::UnsupportedProtocol => {}
                }
                self.try_allocate(adapter, out);
            }
            RouterEvent::AttemptChanged {
                session,
                task_id,
                attempt_id,
                state,
            } => {
                let tracked = self
                    .sessions
                    .get(&session)
                    .and_then(|registered| registered.active_attempts.get(&task_id))
                    .map(|active| active.attempt_id);
                match state {
                    super::model::AttemptState::Running => {
                        if tracked.is_some_and(|tracked| tracked != attempt_id) {
                            // A different attempt is already recorded; a stale
                            // running report must not overwrite it.
                            return;
                        }
                        if let Some(registered) = self.sessions.get_mut(&session) {
                            registered.active_attempts.insert(task_id, ActiveAttemptRef {
                                attempt_id,
                                allocation: None,
                            });
                        }
                    }
                    _ => {
                        // A pre-running accepted attempt owns its allocation even
                        // before a Running report. Both ownership paths require
                        // the exact attempt id; stale outcomes cannot release it.
                        let owns_allocation = self.allocations.values().any(|allocation| {
                            allocation.session == session && allocation.task_id == task_id
                                && allocation.phase == AllocationPhase::Accepted(attempt_id)
                        });
                        if tracked != Some(attempt_id) && !owns_allocation {
                            return;
                        }
                        if tracked == Some(attempt_id)
                            && let Some(registered) = self.sessions.get_mut(&session)
                        {
                            registered.active_attempts.remove(&task_id);
                        }
                        // Release the allocation this attempt held, so the pending
                        // task can be scheduled again.
                        self.allocations.retain(|_, allocation| {
                            !(allocation.task_id == task_id && allocation.session == session
                                && allocation.phase == AllocationPhase::Accepted(attempt_id))
                        });
                        self.blocked_offers.retain(|(blocked_session, _), reason| {
                            *blocked_session != session || *reason != DeclineReason::Unavailable
                        });
                        self.try_allocate(adapter, out);
                    }
                }
            }
            RouterEvent::ReadinessWakeup { session, task_id } => {
                if !self.sessions.contains_key(&session) {
                    return;
                }
                if self.blocked_offers.get(&(session, task_id)).is_some_and(|reason| {
                    matches!(reason, DeclineReason::NotReady | DeclineReason::CoordinationIncomplete)
                }) {
                    self.blocked_offers.remove(&(session, task_id));
                }
                self.try_allocate(adapter, out);
            }
            RouterEvent::RoutingTick => {
                self.try_allocate(adapter, out);
            }
        }
    }

    fn observe_ticket<Adapter: RouterDomainAdapter>(
        &mut self,
        ticket: &TaskTicket,
        adapter: &Adapter,
        out: &mut Vec<RouterAction>,
    ) {
        if !ticket.declaration.declaration.validate_local().is_valid() {
            return;
        }
        // A terminal ticket is no longer runnable.
        if ticket.is_terminal() {
            self.pending.remove(&ticket.task_id);
            self.cancel_task(&ticket.task_id, out);
            return;
        }
        // Proven obsolescence retires a ticket; local unreadiness does not,
        // because a remote executor may hold the inputs this router lacks.
        if adapter.is_proven_obsolete(&ticket.declaration.declaration) {
            self.pending.remove(&ticket.task_id);
            self.cancel_task(&ticket.task_id, out);
            return;
        }
        self.pending
            .insert(ticket.task_id, Box::new(ticket.clone()));
        self.try_allocate(adapter, out);
    }

    fn cancel_task(&mut self, task_id: &PoolTaskId, out: &mut Vec<RouterAction>) {
        // Keep ownership until the worker's exact release or session closure.
        // An offered allocation also retains intent: a delayed acceptance binds
        // its attempt identity, but can only receive cancellation, never Start.
        for allocation in self.allocations.values_mut().filter(|allocation| allocation.task_id == *task_id) {
            allocation.cancel_requested = true;
            out.push(RouterAction::CancelAttempt {
                session: allocation.session,
                task_id: *task_id,
                reason: OpaqueReason::from_label("task no longer scheduled"),
            });
        }
        // Registered origin work has no allocation, but the task cancellation
        // still reaches its owner. Keep the reported attempt until exact release.
        for (session, registered) in &self.sessions {
            if registered.active_attempts.contains_key(task_id)
                && !self.allocations.values().any(|allocation| {
                    allocation.session == *session && allocation.task_id == *task_id
                })
            {
                out.push(RouterAction::CancelAttempt {
                    session: *session,
                    task_id: *task_id,
                    reason: OpaqueReason::from_label("task no longer scheduled"),
                });
            }
        }
    }

    fn consider_takeover(&mut self, out: &mut Vec<RouterAction>) {
        if self.routing {
            return;
        }
        // A claim may only be published after the currently projected claim's
        // heartbeat has been absent for the driver's takeover interval. The
        // driver expires liveness before asking, so a live winner here means this
        // node must wait rather than create a second election.
        if let Some(winner) = self.projected_winner()
            && self.is_live(&winner.liveness_key())
        {
            return;
        }
        let generation = self.slot.max_generation() + 1;
        let claim = RouterClaim {
            pool: self.config.pool.clone(),
            candidate: self.config.node,
            incarnation: self.config.incarnation,
            writer_seq: self.next_writer_seq(),
            generation,
            claim_id: self.ids.next_claim_id(),
            scheduling_protocol: self.highest_supported_protocol(),
            observed: self
                .slot
                .lanes()
                .iter()
                .map(|(candidate, claim)| (*candidate, claim.generation))
                .collect(),
        };
        self.own_claim = Some(claim.clone());
        self.own_claim_persisted = false;
        // The machine's own claim is visible to itself immediately: the driver's
        // durable write is what makes it visible to peers, and leadership is
        // confirmed only after `ClaimPersisted`.
        match self.slot.merge_claim(&claim) {
            Ok(_) => out.push(RouterAction::PersistClaim { claim }),
            // A fresh claim at `max_generation + 1` cannot collide with its own
            // slot; reaching here means local slot state disagrees with the claim
            // just built, so report it rather than publishing a claim the
            // projection does not contain.
            Err(collision) => out.push(RouterAction::RejectClaim {
                collision: Box::new(collision),
            }),
        }
    }

    fn next_writer_seq(&self) -> u64 {
        let retained = self.slot.get(&self.config.node).map(|claim| claim.writer_seq);
        let pending = self.own_claim.as_ref().map(|claim| claim.writer_seq);
        retained.into_iter().chain(pending).max().unwrap_or(0)
            .checked_add(1).expect("router writer sequence exhausted")
    }

    fn highest_supported_protocol(&self) -> SchedulingProtocolVersion {
        self.config
            .supported_protocols
            .iter()
            .copied()
            .max()
            .unwrap_or(SchedulingProtocolVersion(0))
    }

    /// Recompute leadership after the slot or liveness view changed.
    ///
    /// Leadership requires *both* that this machine's claim is the projected
    /// winner and that the driver confirmed the durable write. A heartbeat or slot
    /// merge that arrives between [`RouterEvent::ConsiderTakeover`] and
    /// [`RouterEvent::ClaimPersisted`] therefore cannot grant leadership: the claim
    /// is not yet visible to peers, so routing on it would advertise a router no
    /// other node can see.
    fn reconsider_leadership(&mut self) {
        let Some(own) = self.own_claim.clone() else {
            self.routing = false;
            return;
        };
        self.routing = self.own_claim_persisted
            && self
                .projected_winner()
                .is_some_and(|winner| winner == &own);
    }

    fn try_allocate<Adapter: RouterDomainAdapter>(
        &mut self,
        adapter: &Adapter,
        out: &mut Vec<RouterAction>,
    ) {
        if !self.routing {
            return;
        }
        let task_ids: Vec<PoolTaskId> = self.pending.keys().copied().collect();
        for task_id in task_ids {
            // An outstanding offer or accepted allocation already claims this
            // task; it is not offered again until it settles.
            if self
                .allocations
                .values()
                .any(|allocation| allocation.task_id == task_id)
            {
                continue;
            }
            let Some(ticket) = self.pending.get(&task_id) else {
                continue;
            };
            if ticket.is_terminal() || adapter.is_proven_obsolete(&ticket.declaration.declaration)
            {
                self.pending.remove(&task_id);
                continue;
            }
            let declaration = &ticket.declaration.declaration;

            let mut candidates: Vec<(SessionKey, &RegisteredSession)> = self
                .sessions
                .iter()
                .map(|(key, session)| (*key, session))
                .collect();

            let preferred = match &declaration.placement {
                super::model::Preference::PreferOrigin(node)
                | super::model::Preference::Only(node) => Some(*node),
                super::model::Preference::AnyNode => None,
            };
            if let super::model::Preference::Only(only_node) = &declaration.placement {
                candidates.retain(|(key, _)| key.node == *only_node);
            }
            candidates.sort_by(|left, right| {
                let left_preferred = preferred.is_some_and(|node| node == left.0.node);
                let right_preferred = preferred.is_some_and(|node| node == right.0.node);
                right_preferred
                    .cmp(&left_preferred)
                    .then_with(|| left.0.node.cmp(&right.0.node))
                    .then_with(|| left.0.session.cmp(&right.0.session))
            });

            for (key, registered) in candidates {
                if !declaration
                    .capabilities
                    .is_subset_of(&registered.session.capabilities)
                {
                    continue;
                }
                // A task is offered only where no *registered session* reports it
                // already running. Checking only the candidate would let another
                // executor be handed the same work while a surviving origin
                // attempt holds it — the duplicate a takeover is supposed to
                // avoid.
                if self.sessions.values().any(|session| {
                    session.active_attempts.contains_key(&task_id)
                }) {
                    break;
                }
                // Outstanding allocations count against capacity: an accepted
                // attempt that has not yet reported is still work this executor
                // owes, so treating only *running* attempts as the bound would
                // let a session be over-committed.
                if self.outstanding_for(key) >= registered.session.capacity.max_concurrent_attempts {
                    continue;
                }
                if self.blocked_offers.contains_key(&(key, task_id)) {
                    continue;
                }
                let allocation_id = self.ids.next_allocation_id();
                self.allocations.insert(
                    allocation_id,
                    Allocation {
                        session: key,
                        task_id,
                        phase: AllocationPhase::Offered,
                        cancel_requested: false,
                    },
                );
                out.push(RouterAction::SendOffer {
                    session: key,
                    allocation_id,
                    task_id,
                    declaration: Box::new(declaration.clone()),
                });
                break;
            }
        }
    }

    /// How many attempts one session owes the router: offers outstanding,
    /// accepted allocations, and attempts it has itself reported running.
    fn outstanding_for(&self, session: SessionKey) -> u32 {
        let registered = self.sessions.get(&session);
        let reported = registered.map_or(0, |session| session.active_attempts.len());
        let unreported_allocations = self.allocations.iter()
            .filter(|(id, allocation)| {
                if allocation.session != session {
                    return false;
                }
                // Registration carries allocation identity; live Running reports
                // carry attempt identity. Only the accepted attempt overlaps,
                // never a different origin attempt for the same task.
                !registered.and_then(|session| session.active_attempts.get(&allocation.task_id))
                    .is_some_and(|attempt| match attempt.allocation {
                        Some(reported) => reported == **id,
                        None => allocation.phase == AllocationPhase::Accepted(attempt.attempt_id),
                    })
            })
            .count();
        u32::try_from(reported + unreported_allocations).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests;
