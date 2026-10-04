//! Shared fixtures for the `tasks` behavioural tests.
//!
//! These build real declarations and tickets rather than mocks: a test that
//! passes here exercises the same merge, classification, and handshake code a
//! driver would.

use super::model::{
    AllocationId, CapabilitySummary, CoordinationIds, CoordinationSlotKey, DomainCoordinationRef,
    DomainId, EffectPolicy, HandlerRef, NodeIncarnationId, NodePubkey, PoolClaimId, PoolTaskId,
    PoolTaskIdInputs, Preference,
    PublisherEvidence, ResultRetention, SignedTerminalFact, TaskDeclaration, TaskPoolId,
    TaskTicket, TerminalFact, WorkGeneration, derive_pool_task_id,
};
use super::model::{OpaqueReason, OpaqueResultRef, PoolAttemptId};

pub fn node(seed: u8) -> NodePubkey {
    NodePubkey::new([seed; 32])
}

pub fn pool() -> TaskPoolId {
    TaskPoolId::from_label("image-description")
}

pub fn domain() -> DomainId {
    DomainId::from_label("triage")
}

/// A deterministic id for the given generation label, exactly as a task domain
/// would derive it.
pub fn derived_task(generation: &str) -> PoolTaskId {
    derive_pool_task_id(PoolTaskIdInputs {
        domain: &domain(),
        pool: &pool(),
        slot: &CoordinationSlotKey::from_label("photo-7/describe-image"),
        generation: &WorkGeneration::from_label(generation),
    })
}

/// A canonical declaration for one generation.
pub fn declaration(generation: &str) -> TaskDeclaration {
    TaskDeclaration {
        task_id: derived_task(generation),
        pool: pool(),
        domain: domain(),
        producer: node(1),
        handler: HandlerRef::from_label("describe-image"),
        input: generation.as_bytes().to_vec(),
        capabilities: CapabilitySummary::from_labels(["gpu"]),
        coordination_ref: Some(DomainCoordinationRef::from_label("photo-7")),
        placement: Preference::PreferOrigin(node(1)),
        effect_policy: EffectPolicy::Idempotent,
        not_before_secs: None,
        not_after_secs: None,
        result_retention: ResultRetention::ExternalSettlement(
            DomainCoordinationRef::from_label("photo-7"),
        ),
    }
}

/// Publisher evidence as a given node would publish it. `envelope` differs per
/// publisher so tests prove evidence is not confused with canonical meaning.
pub fn evidence(publisher: NodePubkey, envelope_seed: u8) -> PublisherEvidence {
    PublisherEvidence {
        publisher,
        envelope: vec![envelope_seed; 8],
    }
}

pub fn ticket(generation: &str) -> TaskTicket {
    TaskTicket::new(declaration(generation), evidence(node(1), 1))
}

pub fn success_fact(attempt_id: u128, result: &str) -> TerminalFact {
    TerminalFact::Succeeded {
        attempt_id: PoolAttemptId::new(attempt_id),
        result_ref: Some(OpaqueResultRef::from_label(result)),
    }
}

pub fn cancelled_fact(reason: &str) -> TerminalFact {
    TerminalFact::Cancelled {
        reason: OpaqueReason::from_label(reason),
    }
}

pub fn terminal_lane(writer: NodePubkey, writer_seq: u64, fact: TerminalFact) -> SignedTerminalFact {
    SignedTerminalFact {
        writer,
        writer_seq,
        fact,
    }
}

pub fn incarnation(seed: u8) -> NodeIncarnationId {
    NodeIncarnationId::new(u128::from(seed))
}

/// A deterministic [`CoordinationIds`] source: one counter per identity kind.
///
/// Test-only counters keep identities predictable across reducer scenarios.
#[derive(Default)]
pub(crate) struct SequentialIds {
    claims: u128,
    allocations: u128,
    attempts: u128,
}

impl CoordinationIds for SequentialIds {
    fn next_claim_id(&mut self) -> PoolClaimId {
        self.claims += 1;
        PoolClaimId::new(self.claims)
    }

    fn next_allocation_id(&mut self) -> AllocationId {
        self.allocations += 1;
        AllocationId::new(self.allocations)
    }

    fn next_attempt_id(&mut self) -> PoolAttemptId {
        self.attempts += 1;
        PoolAttemptId::new(self.attempts)
    }
}

/// A tiny [`CoordinationIds`] wrapper for tests that need to inspect minted ids.
pub fn ids() -> SequentialIds {
    SequentialIds::default()
}
