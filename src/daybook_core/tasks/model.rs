//! Domain-neutral protocol model for task pools (ADR 011 §2, §5, §8, §10, §11).
//!
//! Pure data plus validation/merge rules. This module never touches storage,
//! transport, clocks, or threads: a driver applies the returned dispositions,
//! and all time and random identity enter through explicit arguments.
//!
//! The persisted-schema constants here and the live scheduling protocol version
//! ([`SchedulingProtocolVersion`]) are deliberately separate contracts. A live
//! ALPN bump does not migrate retained task tickets or router slots, and an
//! unknown retained schema version is unsupported data rather than proof a task
//! was cancelled or may be pruned.

use crate::interlude::*;

use blake3::Hasher;

/// Persisted schema of a [`TaskTicket`] payload. Bumped independently of any
/// live RPC/scheduling protocol version.
pub const TASK_TICKET_PAYLOAD_SCHEMA: u16 = 1;

/// Persisted schema of a router slot payload. Bumped independently of any live
/// RPC/scheduling protocol version.
pub const ROUTER_SLOT_PAYLOAD_SCHEMA: u16 = 1;

fn update_len_prefixed(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// A stable identity whose pre-image is a human-chosen label.
///
/// Identities in this protocol are opaque to the generic layer: they are used as
/// map keys and digest inputs, so what a caller puts in them only has to be
/// stable and distinct. The wrapper keeps them distinct types so a router id can
/// never be passed where a task id is meant.
macro_rules! label_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
        pub struct $name(CHeapStr);

        impl $name {
            /// Build from a caller-chosen label. The label is copied into the
            /// identity, so a borrowed string is accepted without demanding a
            /// `'static` lifetime.
            #[must_use]
            pub fn from_label(label: impl Into<String>) -> Self {
                Self(CHeapStr::new(label.into()))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

label_newtype!(
    /// Stable logical identity of one task pool.
    ///
    /// A pool is a scheduling and retention class, not a FIFO queue (ADR 011 §2),
    /// and the identity is stable across physical part rotation so task ids and
    /// router lanes survive a part generation change.
    TaskPoolId
);
label_newtype!(
    /// Stable identity of a (document, branch, processor) coordination slot held
    /// by a task domain (ADR 010, terminology).
    CoordinationSlotKey
);
label_newtype!(
    /// Exact source version + processor generation + effective configuration
    /// generation processed by one execution obligation (ADR 010).
    WorkGeneration
);
label_newtype!(
    /// Domain namespacing of a derived task id.
    DomainId
);
label_newtype!(
    /// Which routine an executor should invoke, interpreted by the domain.
    HandlerRef
);
label_newtype!(
    /// Where a domain's durable settlement record for a task lives, when one
    /// exists. It is compared for identity only, never ordered numerically
    /// against any other cursor.
    DomainCoordinationRef
);
label_newtype!(
    /// A domain-defined cancellation reason. Opaque by design: reasons are
    /// policy, not protocol.
    OpaqueReason
);
label_newtype!(
    /// A domain-defined handle to a produced result. The task layer carries it
    /// but never dereferences it.
    OpaqueResultRef
);

/// A Daybook node's public identity. It names a node; it does not address it
/// (ADR 011 §6), so it is never used as a transport endpoint.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodePubkey([u8; 32]);

impl NodePubkey {
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn to_bytes32(&self) -> [u8; 32] {
        self.0
    }

    /// A key no node is expected to have, for tests and stand-in fixtures.
    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

impl std::fmt::Display for NodePubkey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&utils_rs::hash::encode_base58_multibase(self.0))
    }
}

/// One run of a node process. A restart mints a fresh incarnation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeIncarnationId(u128);

impl NodeIncarnationId {
    #[must_use]
    pub fn new(raw: u128) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn to_u128(&self) -> u128 {
        self.0
    }

    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

/// One live router connection.
///
/// A session id is minted per connection, not per node incarnation: a node that
/// reconnects to the same router, or reconnects after a transport blip, produces
/// a new session, so messages addressed to the previous connection are fenced
/// even though node and incarnation are unchanged. The driver that owns the
/// connection mints it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct RouterSessionId(u128);

impl RouterSessionId {
    #[must_use]
    pub fn new(raw: u128) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn to_u128(&self) -> u128 {
        self.0
    }

    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

/// Identity of one actual local execution of a task (ADR 010 terminology).
///
/// Duplicate attempts may exist across partitions; a terminal fact names the
/// attempt that produced it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PoolAttemptId(u128);

impl PoolAttemptId {
    #[must_use]
    pub fn new(raw: u128) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn to_u128(&self) -> u128 {
        self.0
    }

    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

/// A router↔executor live agreement to attempt one task (ADR 011 §7).
///
/// Allocations are ephemeral — they live only in router/executor memory and
/// local dispatch state — so their identity is minted rather than derived.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct AllocationId(u128);

impl AllocationId {
    #[must_use]
    pub fn new(raw: u128) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn to_u128(&self) -> u128 {
        self.0
    }

    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

/// Random identity of one router claim, breaking rank ties between candidates
/// publishing at the same generation (ADR 011 §5).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PoolClaimId(u128);

impl PoolClaimId {
    #[must_use]
    pub fn new(raw: u128) -> Self {
        Self(raw)
    }

    #[must_use]
    pub fn to_u128(&self) -> u128 {
        self.0
    }

    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

/// A deterministic distributed obligation identity, distinct from
/// [`big_sync_core::TaskId`], which is a scheduler-local work item.
///
/// Derived by the task domain from its own slot and generation, so two isolated
/// nodes deriving the same obligation agree on the id without synchronizing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PoolTaskId([u8; 32]);

impl PoolTaskId {
    #[must_use]
    pub fn new(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    #[must_use]
    pub fn to_bytes32(&self) -> [u8; 32] {
        self.0
    }

    /// A digest no derived obligation is expected to have, for independently
    /// submitted one-shot work.
    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }
}

impl std::fmt::Display for PoolTaskId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&utils_rs::hash::encode_base58_multibase(self.0))
    }
}

/// The domain inputs a deterministic [`PoolTaskId`] is derived from.
pub struct PoolTaskIdInputs<'a> {
    pub domain: &'a DomainId,
    pub pool: &'a TaskPoolId,
    pub slot: &'a CoordinationSlotKey,
    pub generation: &'a WorkGeneration,
}

/// Derive the deterministic obligation id for one processor work generation.
///
/// Domain-separated and length-prefixed so no two distinct input tuples share a
/// digest, and stable forever: replicas that agree on the inputs agree on the
/// id, which is what deduplicates independently derived obligations.
#[must_use]
pub fn derive_pool_task_id(inputs: PoolTaskIdInputs<'_>) -> PoolTaskId {
    let mut hasher = Hasher::new();
    hasher.update(b"daybook/pool-task-id/v1");
    update_len_prefixed(&mut hasher, inputs.domain.as_str().as_bytes());
    update_len_prefixed(&mut hasher, inputs.pool.as_str().as_bytes());
    update_len_prefixed(&mut hasher, inputs.slot.as_str().as_bytes());
    update_len_prefixed(&mut hasher, inputs.generation.as_str().as_bytes());
    PoolTaskId(*hasher.finalize().as_bytes())
}

/// The capabilities an executor must have for a task to be offerable to it.
///
/// Coarse scheduling hints, not the authoritative readiness decision: the
/// selected executor still classifies the ticket itself (ADR 011 §3).
#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct CapabilitySummary(HashSet<CHeapStr>);

impl CapabilitySummary {
    #[must_use]
    pub fn empty() -> Self {
        Self(HashSet::new())
    }

    #[must_use]
    pub fn from_labels(labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(
            labels
                .into_iter()
                .map(|label| CHeapStr::new(label.into()))
                .collect(),
        )
    }

    #[must_use]
    pub fn contains(&self, label: &str) -> bool {
        self.0.contains(label)
    }

    #[must_use]
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn update_digest(&self, hasher: &mut Hasher) {
        let mut labels: Vec<&str> = self.0.iter().map(CHeapStr::as_str).collect();
        labels.sort_unstable();
        for label in labels {
            update_len_prefixed(hasher, label.as_bytes());
        }
    }
}

/// Where a task is allowed to execute (ADR 011 §8, §10).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Preference {
    /// Origin prefers to run its own work, but any eligible executor may.
    PreferOrigin(NodePubkey),
    /// Explicitly pinned to one node's authority.
    Only(NodePubkey),
    /// No placement preference; schedule wherever eligible.
    AnyNode,
}

impl Preference {
    fn update_digest(&self, hasher: &mut Hasher) {
        match self {
            Self::PreferOrigin(node) => {
                hasher.update(b"prefer-origin");
                hasher.update(&node.to_bytes32());
            }
            Self::Only(node) => {
                hasher.update(b"only");
                hasher.update(&node.to_bytes32());
            }
            Self::AnyNode => {
                hasher.update(b"any");
            }
        }
    }
}

/// Whether and how a domain tolerates duplicate execution of this task
/// (ADR 011 scenario 13).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum EffectPolicy {
    /// Domain state is safe under at-least-once execution.
    Idempotent,
    /// The domain derives an external idempotency key from the task id.
    ExternalIdempotencyKey,
    /// Execution is placed on one explicitly authoritative node.
    AuthoritativePlacement,
    /// The domain has accepted that duplicates may occur.
    AcceptsDuplicates,
}

impl EffectPolicy {
    fn update_digest(self, hasher: &mut Hasher) {
        let tag: &[u8] = match self {
            Self::Idempotent => b"idempotent",
            Self::ExternalIdempotencyKey => b"external-idempotency-key",
            Self::AuthoritativePlacement => b"authoritative-placement",
            Self::AcceptsDuplicates => b"accepts-duplicates",
        };
        hasher.update(tag);
    }
}

/// How a successful result is retained once the ticket leaves active scheduling
/// (ADR 011 §11).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ResultRetention {
    /// A durable domain record already proves settlement, so the task data may
    /// be removed after the domain incorporates it.
    ExternalSettlement(DomainCoordinationRef),
    /// The ticket itself retains settlement evidence. `None` retains it
    /// indefinitely; a finite horizon cannot precede the execution deadline.
    TicketAuthoritative { retain_until: Option<i64> },
}

impl ResultRetention {
    fn update_digest(&self, hasher: &mut Hasher) {
        match self {
            Self::ExternalSettlement(coordination_ref) => {
                hasher.update(b"external-settlement");
                update_len_prefixed(hasher, coordination_ref.as_str().as_bytes());
            }
            Self::TicketAuthoritative { retain_until } => {
                hasher.update(b"ticket-authoritative");
                match retain_until {
                    Some(until) => {
                        hasher.update(b"retain-until");
                        hasher.update(&until.to_be_bytes());
                    }
                    None => {
                        hasher.update(b"retain-indefinitely");
                    }
                }
            }
        }
    }
}

/// The canonical, immutable execution meaning of one task (ADR 011 §8).
///
/// Two authorized publishers asserting the *same* deterministic obligation may
/// produce different bytes — different envelope, different signature, different
/// non-semantic hints — but they must not produce different *meaning*. This type
/// is that meaning and nothing else: no publisher identity, no ciphertext, no
/// signature, no local scheduling hints. Its [`PartialEq`] is the equality
/// collision validation uses.
///
/// `task_id` is present so a declaration cannot be detached from the id it
/// claims. `producer` is the node that authored the triggering work (triage's
/// *origin*, ADR 010 terminology): it is canonical because origin preference and
/// authoritative placement are properties of the obligation, while *which node
/// republished* the evidence is not.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TaskDeclaration {
    pub task_id: PoolTaskId,
    pub pool: TaskPoolId,
    pub domain: DomainId,
    pub producer: NodePubkey,
    pub handler: HandlerRef,
    /// Canonical domain-owned input bytes; interpretation belongs to the handler.
    pub input: Vec<u8>,
    pub capabilities: CapabilitySummary,
    pub coordination_ref: Option<DomainCoordinationRef>,
    pub placement: Preference,
    pub effect_policy: EffectPolicy,
    pub not_before_secs: Option<i64>,
    pub not_after_secs: Option<i64>,
    pub result_retention: ResultRetention,
}

impl TaskDeclaration {
    /// Domain-separated digest of the canonical execution meaning.
    ///
    /// Covers exactly the fields of [`Self`] and nothing else: publisher
    /// identity, envelopes, signatures, and local scheduling hints are not
    /// inputs, so independently equivalent publishers canonicalize to one digest
    /// while unequal execution meaning cannot.
    #[must_use]
    pub fn canonical_digest(&self) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(b"daybook/task-declaration/v1");
        hasher.update(&self.task_id.to_bytes32());
        update_len_prefixed(&mut hasher, self.pool.as_str().as_bytes());
        update_len_prefixed(&mut hasher, self.domain.as_str().as_bytes());
        hasher.update(&self.producer.to_bytes32());
        update_len_prefixed(&mut hasher, self.handler.as_str().as_bytes());
        update_len_prefixed(&mut hasher, &self.input);
        self.capabilities.update_digest(&mut hasher);
        match &self.coordination_ref {
            Some(coordination_ref) => {
                hasher.update(b"coordination-ref");
                update_len_prefixed(&mut hasher, coordination_ref.as_str().as_bytes());
            }
            None => {
                hasher.update(b"no-coordination-ref");
            }
        }
        self.placement.update_digest(&mut hasher);
        self.effect_policy.update_digest(&mut hasher);
        match self.not_before_secs {
            Some(secs) => {
                hasher.update(b"not-before");
                hasher.update(&secs.to_be_bytes());
            }
            None => {
                hasher.update(b"no-not-before");
            }
        }
        match self.not_after_secs {
            Some(secs) => {
                hasher.update(b"not-after");
                hasher.update(&secs.to_be_bytes());
            }
            None => {
                hasher.update(b"no-not-after");
            }
        }
        self.result_retention.update_digest(&mut hasher);
        *hasher.finalize().as_bytes()
    }

    /// Validate a *local* producer's declaration under the invariant that one
    /// task id means one obligation. A local violation is a programming error
    /// and the caller fails loudly, unlike a rejected remote payload.
    #[must_use]
    pub fn validate_local(&self) -> DeclarationViolation {
        if let (Some(not_before), Some(not_after)) = (self.not_before_secs, self.not_after_secs)
            && not_after < not_before
        {
            return DeclarationViolation::WindowInverted {
                not_before_secs: not_before,
                not_after_secs: not_after,
            };
        }

        if let ResultRetention::TicketAuthoritative { retain_until: Some(retain_until) } = self.result_retention {
            let Some(not_after_secs) = self.not_after_secs else {
                return DeclarationViolation::MissingExecutionDeadline;
            };
            if not_after_secs > retain_until {
                return DeclarationViolation::RetentionPrecedesDeadline {
                    not_after_secs, retain_until,
                };
            }
        }
        if self.effect_policy == EffectPolicy::AuthoritativePlacement
            && !matches!(self.placement, Preference::Only(_))
        {
            return DeclarationViolation::AuthoritativePlacementRequiresOnly;
        }

        DeclarationViolation::Valid
    }
}

/// Outcome of [`TaskDeclaration::validate_local`], as a value so a caller can
/// branch exhaustively.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclarationViolation {
    Valid,
    WindowInverted {
        not_before_secs: i64,
        not_after_secs: i64,
    },
    MissingExecutionDeadline,
    RetentionPrecedesDeadline { not_after_secs: i64, retain_until: i64 },
    AuthoritativePlacementRequiresOnly,
}

impl DeclarationViolation {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

impl std::fmt::Display for DeclarationViolation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Valid => formatter.write_str("valid"),
            Self::WindowInverted {
                not_before_secs,
                not_after_secs,
            } => write!(
                formatter,
                "task window is inverted: not_before {not_before_secs} > not_after {not_after_secs}"
            ),
            Self::MissingExecutionDeadline => formatter
                .write_str("finite ticket-authoritative retention requires an execution deadline"),
            Self::RetentionPrecedesDeadline { not_after_secs, retain_until } => write!(
                formatter, "retention horizon {retain_until} precedes execution deadline {not_after_secs}"
            ),
            Self::AuthoritativePlacementRequiresOnly => formatter
                .write_str("authoritative effect placement requires an Only node"),
        }
    }
}

/// Separately authenticated evidence that one publisher asserts a declaration
/// (ADR 011 §8; design note "Declaration equivalence").
///
/// Publisher identity, envelope, and signature are evidence *about* a
/// declaration, not part of it. Merging evidence never changes meaning.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PublisherEvidence {
    pub publisher: NodePubkey,
    /// Authenticated envelope bytes as published (ciphertext, framing, or
    /// signature). Opaque to this layer.
    pub envelope: Vec<u8>,
}

/// A signed declaration: canonical meaning plus the evidence of its publishers.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SignedTaskDeclaration {
    pub declaration: TaskDeclaration,
    /// Authenticated publisher evidence, deduplicated by publisher identity.
    pub publishers: Vec<PublisherEvidence>,
}

/// A terminal fact, preserving concurrent authenticated facts per writer
/// (ADR 011 §8).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TerminalFact {
    Succeeded {
        attempt_id: PoolAttemptId,
        result_ref: Option<OpaqueResultRef>,
    },
    Cancelled {
        reason: OpaqueReason,
    },
}

impl TerminalFact {
    /// Whether this fact reports success. Success is the fact a domain must be
    /// able to observe after any merge.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded { .. })
    }
}

/// One writer's slot in the terminal-fact lanes.
///
/// Success dominates cancellation regardless of sequence; within either outcome
/// merging takes the greater writer sequence. Equal sequence with unequal facts
/// is equivocation and is rejected before mutation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SignedTerminalFact {
    pub writer: NodePubkey,
    pub writer_seq: u64,
    pub fact: TerminalFact,
}

/// Compact terminal summary surfaced to callers that only need the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalSummary {
    Succeeded {
        attempt_id: PoolAttemptId,
        result_ref: Option<OpaqueResultRef>,
    },
    Cancelled {
        reason: OpaqueReason,
    },
}

/// Why a remote payload was rejected. Malformed or conflicting remote input is
/// an error to report, not a reason to crash the node; a *local* producer
/// violating the deterministic-id invariant is a panic instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CollisionRejected {
    #[error("task {task_id} has an invalid declaration: {violation}")]
    InvalidDeclaration { task_id: PoolTaskId, violation: DeclarationViolation },
    /// Two unequal canonical declarations claimed the same task id. Publishers
    /// are not "desired revision siblings"; replacement is a new task id.
    #[error("task {task_id} has two unequal declarations under one identity")]
    UnequalDeclaration {
        task_id: PoolTaskId,
        existing: Box<TaskDeclaration>,
        incoming: Box<TaskDeclaration>,
    },
    /// Two writers published different facts at the same sequence number.
    #[error("task {task_id} equivocated at writer sequence {writer_seq}")]
    EquivocatedTerminal {
        task_id: PoolTaskId,
        writer: NodePubkey,
        writer_seq: u64,
    },
    /// The payload declared an unsupported persisted schema version. Unknown
    /// retained versions are unsupported data, never proof of cancellation.
    #[error("task {task_id} carries unsupported payload schema {schema}")]
    UnsupportedSchema { task_id: PoolTaskId, schema: u16 },
    /// The payload's schema differed from the local one.
    #[error("task ticket payload schema {schema} does not match local schema")]
    SchemaMismatch { schema: u16 },
    /// Two tickets claimed different task ids but were merged as one.
    #[error("task ticket {existing} was merged with a ticket for {incoming}")]
    MismatchedIdentity {
        existing: PoolTaskId,
        incoming: PoolTaskId,
    },
}

/// Whether a merge changed local state, which is what decides whether the merged
/// payload is advertised back to the sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeDisposition {
    Changed,
    Unchanged,
}

/// A task ticket: immutable declaration plus mergeable publisher evidence and
/// terminal facts (ADR 011 §8).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TaskTicket {
    pub schema: u16,
    pub task_id: PoolTaskId,
    pub declaration: SignedTaskDeclaration,
    /// Per-writer terminal lanes. A valid success or cancellation stops ordinary
    /// scheduling; concurrent facts from different writers all survive a merge.
    pub terminal: HashMap<NodePubkey, SignedTerminalFact>,
}

impl TaskTicket {
    /// A fresh ticket around one publisher's canonical declaration.
    #[must_use]
    pub fn new(declaration: TaskDeclaration, evidence: PublisherEvidence) -> Self {
        let task_id = declaration.task_id;
        Self {
            schema: TASK_TICKET_PAYLOAD_SCHEMA,
            task_id,
            declaration: SignedTaskDeclaration {
                declaration,
                publishers: vec![evidence],
            },
            terminal: HashMap::new(),
        }
    }

    /// Build a ticket for a *locally produced* obligation, failing loudly when the
    /// declaration violates the local invariants.
    ///
    /// This is the producer boundary where the deterministic-id and retention
    /// invariants are the producer's own responsibility: a violation here is a
    /// programming error in this process, not malformed remote input, so it
    /// panics rather than returning a rejected payload (see
    /// [`TaskDeclaration::validate_local`] and [`CollisionRejected`] for the
    /// remote path).
    #[must_use]
    pub fn declare_local(declaration: TaskDeclaration, evidence: PublisherEvidence) -> Self {
        let violation = declaration.validate_local();
        assert!(
            violation.is_valid(),
            "local task declaration violates a producer invariant: {violation}",
        );
        Self::new(declaration, evidence)
    }

    /// The task's terminal summary, if any lane reports one.
    ///
    /// Success is monotonic and dominates: once any authenticated writer reports
    /// success the summary is `Succeeded` no matter how many cancellations merge
    /// afterwards, because a cancellation that loses the local finalization
    /// ordering must not claim to have undone published effects (ADR 011 §8).
    /// The choice among concurrent successes and among concurrent cancellations
    /// is a pure function of lane contents — greatest `(attempt, result)` and
    /// lowest writer key respectively — so every replica reading the same lanes
    /// reports the same summary rather than one that depends on map iteration
    /// order.
    #[must_use]
    pub fn terminal_summary(&self) -> Option<TerminalSummary> {
        let mut success: Option<(PoolAttemptId, Option<OpaqueResultRef>)> = None;
        let mut cancellation: Option<&SignedTerminalFact> = None;
        for (writer, lane) in &self.terminal {
            match &lane.fact {
                TerminalFact::Succeeded {
                    attempt_id,
                    result_ref,
                } => {
                    let candidate = (*attempt_id, result_ref.clone());
                    if success.as_ref().is_none_or(|existing| candidate > *existing) {
                        success = Some(candidate);
                    }
                }
                TerminalFact::Cancelled { .. } => {
                    // Lanes are keyed by writer, so the writer keys being
                    // compared always differ and this is a total order.
                    if cancellation.is_none_or(|existing| writer < &existing.writer) {
                        cancellation = Some(lane);
                    }
                }
            }
        }
        if let Some((attempt_id, result_ref)) = success {
            return Some(TerminalSummary::Succeeded {
                attempt_id,
                result_ref,
            });
        }
        cancellation.and_then(|lane| match &lane.fact {
            TerminalFact::Cancelled { reason } => Some(TerminalSummary::Cancelled {
                reason: reason.clone(),
            }),
            TerminalFact::Succeeded { .. } => None,
        })
    }

    /// Whether any lane reports success.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(
            self.terminal_summary(),
            Some(TerminalSummary::Succeeded { .. })
        )
    }

    /// Whether any lane reports a terminal fact at all.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        !self.terminal.is_empty()
    }

    /// Merge a remote ticket into this one.
    ///
    /// Commutative, associative, and idempotent for valid writers:
    ///
    /// * Declaration equality is canonical-meaning equality; unequal meaning,
    ///   mismatched identity, and unknown schemas are rejected **before** any
    ///   local state changes, so a malformed remote payload cannot leave a
    ///   partially ingested ticket behind.
    /// * Publisher evidence is a set keyed by *evidence content*, so the merged
    ///   evidence is the same set regardless of merge order; the vector is
    ///   canonicalized so two replicas holding the same evidence compare equal.
    /// * Terminal lanes keep every concurrent writer's fact. Success dominates
    ///   cancellation, and the greater writer sequence wins among successes or
    ///   among cancellations. Thus later success results converge without a
    ///   cancellation claiming to undo a published effect (ADR 011 §8).
    pub fn merge(&mut self, remote: &Self) -> Result<MergeDisposition, CollisionRejected> {
        if remote.schema != self.schema {
            return Err(CollisionRejected::SchemaMismatch {
                schema: remote.schema,
            });
        }
        if self.schema != TASK_TICKET_PAYLOAD_SCHEMA {
            return Err(CollisionRejected::UnsupportedSchema {
                task_id: self.task_id,
                schema: self.schema,
            });
        }
        if remote.task_id != self.task_id {
            return Err(CollisionRejected::MismatchedIdentity {
                existing: self.task_id,
                incoming: remote.task_id,
            });
        }
        let violation = remote.declaration.declaration.validate_local();
        if !violation.is_valid() {
            return Err(CollisionRejected::InvalidDeclaration {
                task_id: remote.task_id, violation,
            });
        }
        if remote.declaration.declaration != self.declaration.declaration {
            return Err(CollisionRejected::UnequalDeclaration {
                task_id: self.task_id,
                existing: Box::new(self.declaration.declaration.clone()),
                incoming: Box::new(remote.declaration.declaration.clone()),
            });
        }

        // Validation pass: a ticket carrying an equivocated lane is rejected whole,
        // before any mutation, so no lane from a malformed payload is retained.
        for (writer, incoming) in &remote.terminal {
            if let Some(existing) = self.terminal.get(writer)
                && incoming.writer_seq == existing.writer_seq
                && existing.fact != incoming.fact
            {
                return Err(CollisionRejected::EquivocatedTerminal {
                    task_id: self.task_id,
                    writer: *writer,
                    writer_seq: incoming.writer_seq,
                });
            }
        }

        let mut changed = false;

        // Evidence is a set of records, not of publishers: one publisher may
        // legitimately present more than one envelope, and which records are
        // present must not depend on merge order.
        for evidence in &remote.declaration.publishers {
            if !self.declaration.publishers.contains(evidence) {
                self.declaration.publishers.push(evidence.clone());
                changed = true;
            }
        }
        if changed {
            self.declaration.publishers.sort_by(|left, right| {
                left.publisher
                    .cmp(&right.publisher)
                    .then_with(|| left.envelope.cmp(&right.envelope))
            });
        }

        for (writer, incoming) in &remote.terminal {
            match self.terminal.get(writer) {
                None => {
                    self.terminal.insert(*writer, incoming.clone());
                    changed = true;
                }
                Some(existing) => {
                    let replace = match (&existing.fact, &incoming.fact) {
                        // Success is monotonic per writer: once a writer reports it,
                        // its later cancellation does not displace it.
                        (TerminalFact::Succeeded { .. }, TerminalFact::Succeeded { .. }) => {
                            incoming.writer_seq > existing.writer_seq
                        }
                        (TerminalFact::Succeeded { .. }, TerminalFact::Cancelled { .. }) => false,
                        (_, TerminalFact::Succeeded { .. }) => true,
                        _ => incoming.writer_seq > existing.writer_seq,
                    };
                    if replace {
                        self.terminal.insert(*writer, incoming.clone());
                        changed = true;
                    }
                }
            }
        }

        Ok(if changed {
            MergeDisposition::Changed
        } else {
            MergeDisposition::Unchanged
        })
    }
}

/// Owned execution arguments resolved by the local domain. Their encoding is
/// opaque to coordination and remains fixed for one durably persisted attempt.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ResolvedInvocation {
    pub args: Vec<u8>,
}

/// A domain's classification of a ticket (ADR 011 §10).
///
/// The executor performs the final classification; the router may cache one for
/// scheduling efficiency but must re-ask before offering.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TaskClassification {
    /// Ready to run, with the domain's execution arguments already resolved.
    Runnable(ResolvedInvocation),
    /// Locally unable to run yet, with the input change that could change that.
    /// `NotReady` is *local* unreadiness and is never proof of global
    /// obsolescence (ADR 010, classification contract).
    NotReady(ReadinessWatch),
    /// Durable domain evidence proves the obligation is satisfied or no longer
    /// desired. An authoritative domain statement, not a local guess.
    Obsolete,
    /// The domain's relevant replay boundary has not been reached, so newly
    /// arrived task state cannot yet be judged.
    CoordinationIncomplete,
    /// The declaration itself is invalid for this domain.
    Invalid,
}

/// What local input change would make a [`TaskClassification::NotReady`] task
/// runnable. The domain adapter emits a readiness wakeup when it materializes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ReadinessWatch(DomainCoordinationRef);

impl ReadinessWatch {
    #[must_use]
    pub fn new(coordination_ref: DomainCoordinationRef) -> Self {
        Self(coordination_ref)
    }

    #[must_use]
    pub fn coordination_ref(&self) -> &DomainCoordinationRef {
        &self.0
    }
}

/// The live scheduling/RPC compatibility version a router claim advertises
/// (ADR 011 §6, "RPC compatibility and rolling upgrades").
///
/// Deliberately *not* a persisted payload schema: claims carry it so one shared
/// election can prefer a newer live routing version without forming
/// version-specific cohorts, and bumping it migrates no retained data.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct SchedulingProtocolVersion(pub u16);

impl std::fmt::Display for SchedulingProtocolVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "scheduling/v{}", self.0)
    }
}

/// Coarse executor capacity.
///
/// Capacity is a scheduling hint and a liveness guard, never durable protocol
/// state: losing it only means a task is offered elsewhere or reallocated after
/// a session is re-established.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Capacity {
    pub max_concurrent_attempts: u32,
}

impl Capacity {
    #[must_use]
    pub fn new(max_concurrent_attempts: u32) -> Self {
        Self {
            max_concurrent_attempts,
        }
    }
}

/// What one node's session tells a router about an attempt it already runs
/// (ADR 011 §7).
///
/// Normal registration state, not a snapshot protocol: after a takeover a
/// surviving executor introduces its own bounded current set so the new router
/// does not re-offer work that is already running.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ActiveAttemptSummary {
    pub task_id: PoolTaskId,
    pub attempt_id: PoolAttemptId,
    /// `None` when the attempt was started by the origin fast path rather than
    /// by a router allocation.
    pub allocation: Option<AllocationId>,
}

/// Why an executor declined.
///
/// Executor readiness is authoritative for its own local inputs, so `NotReady`
/// must never be read by the router as proof of global obsolescence.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DeclineReason {
    /// Local input has not materialized. The domain records a readiness watch.
    NotReady,
    /// Durable domain evidence says the obligation is settled or undesired.
    Obsolete,
    /// The domain's replay boundary has not been reached.
    CoordinationIncomplete,
    /// The declaration is invalid for this domain, or the offer did not match
    /// the ticket it named.
    Invalid,
    /// This executor cannot speak the router's scheduling protocol.
    UnsupportedProtocol,
    /// The offer named a router session this executor no longer serves.
    StaleSession,
    /// At capacity, or already running this task.
    Unavailable,
}

/// Progress an executor reports for one attempt. Detailed retry and failure
/// policy stays in local dispatch state; only these coarse states cross the live
/// session.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AttemptState {
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

/// Random identity minting, injected so the machines stay deterministic in tests
/// and never reach for a global RNG.
pub trait CoordinationIds {
    fn next_claim_id(&mut self) -> PoolClaimId;
    fn next_allocation_id(&mut self) -> AllocationId;
    fn next_attempt_id(&mut self) -> PoolAttemptId;
}


#[cfg(test)]
mod tests;
