//! Generic distributed task-pool coordination (ADR 011).
//!
//! This module is the domain-neutral half of distributed work allocation: pool
//! identity, deterministic task identity, canonical task declarations with
//! separately authenticated publisher evidence, monotonic terminal facts, router
//! election, live executor sessions/allocations, and the executor's durable-
//! attempt handshake. The reusable [`big_sync_core::encrypted_register`] owns
//! signed latest-per-writer evidence. BigSync transport, encryption, durable
//! publication and triage projection remain driver-owned.
//!
//! Three rules shape the split:
//!
//! * **Sans-I/O transitions.** [`RouterMachine`] and [`PoolWorkerMachine`] take
//!   input events and return bounded actions.
//!   Clocks, identity minting, storage, transport, and dispatch I/O belong to
//!   the driver, which feeds the resulting outcomes back as events — including
//!   the durability acknowledgements (`ClaimPersisted`, `AttemptPersisted`,
//!   `TerminalFactAccepted`). That is what lets a test drive takeover, stale
//!   sessions, and persist-before-start ordering deterministically.
//! * **Domain neutrality.** The generic layer knows *that* an obligation is
//!   runnable, not *why*. A per-node domain adapter answers
//!   [`TaskClassification`] (executor side) or explicit asynchronous
//!   obsolescence proof results (router side).
//! * **Identity separation.** A task domain derives [`PoolTaskId`]
//!   deterministically from its own coordination slot and work generation, so
//!   two isolated nodes agreeing on an obligation agree on its id.
//!   [`PoolTaskId`] is deliberately a distinct persisted identity from
//!   [`big_sync_core::TaskId`], which is a scheduler-local work item.
//!
//! Persisted schemas and live protocols version independently: see
//! [`TASK_TICKET_PAYLOAD_SCHEMA`], [`ROUTER_SLOT_PAYLOAD_SCHEMA`], and
//! [`SchedulingProtocolVersion`]. A wire-protocol bump never migrates retained
//! data, and an unknown persisted version is unsupported data, not evidence of
//! cancellation.

pub mod driver;
#[cfg(test)]
mod e2e;
mod model;
pub mod pool;
pub mod router;
pub mod rpc;
pub mod storage;
pub mod store;
#[cfg(test)]
mod test_util;
pub mod transport;
pub mod worker;

pub use model::{
    ActiveAttemptSummary, AllocationId, AttemptState, CapabilitySummary, Capacity,
    CollisionRejected, CoordinationIds, CoordinationSlotKey, DeclarationViolation, DeclineReason,
    DomainCoordinationRef, DomainId, EffectPolicy, HandlerRef, MergeDisposition, NodeIncarnationId,
    NodePubkey, OpaqueReason, OpaqueResultRef, PoolAttemptId, PoolClaimId, PoolTaskId,
    PoolTaskIdInputs, Preference, PublisherEvidence, ROUTER_SLOT_PAYLOAD_SCHEMA, ReadinessWatch,
    ResolvedInvocation, ResultRetention, RouterSessionId, SchedulingProtocolVersion,
    SignedTaskDeclaration, SignedTerminalFact, TASK_TICKET_PAYLOAD_SCHEMA, TaskClassification,
    TaskDeclaration, TaskPoolId, TaskTicket, TerminalFact, TerminalSummary, WorkGeneration,
    derive_pool_task_id,
};
pub use pool::{
    PoolBindingLookup, PoolDescriptorSnapshot, PoolDiscovery, PoolMetadataRejection, PoolReference,
    PoolRegistrationError, PoolRepo, PoolWatch, RetentionClass, RoutingDefaults, RpcTransport,
    TASK_POOL_DESCRIPTOR_PROTOCOL, TaskPoolDescriptor,
};
pub use router::{
    ClaimCollision, ClaimLivenessKey, ObsolescenceToken, RouterAction, RouterClaim, RouterConfig,
    RouterEvent, RouterMachine, RouterSlotState, SessionKey,
};
pub use worker::{
    ClassificationToken, PoolWorkerMachine, WorkerAction, WorkerConfig, WorkerEvent,
    WorkerRegistration,
};
