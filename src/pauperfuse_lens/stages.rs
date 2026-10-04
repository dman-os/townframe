//! The five stage traits (ADR 012 §10) and the work/view types that cross the
//! stage boundaries. Concrete APIs stay advisory by ADR — these fix the seams
//! only: interest, proposal, preparation, production, and diff view, driven by
//! the selector that lives outside the lens (design §1.1).

use daybook_types::doc::{DocId, FacetKey, FacetRaw};
use std::collections::HashMap;

use crate::identity::LensIdentity;
use crate::outcome::UninterpretedReason;
use crate::proposal::{OutputKind, Proposal, Subject};
use crate::recipe::{DepAtHeads, Recipe};
use crate::signals::{Constraint, Signal};

/// Stage-boundary failure model (ADR 012 §5/§6, design §7): the execution
/// outcomes are distinct and none triggers automatic fallback. Recognition
/// *never* fails here — a declined interpretation is a
/// [`LensDecision::Declined`] with its per-claim reason, considered against
/// other applicable lenses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LensFailure {
    /// The lens was selected and rejects the content: a facet shape or bytes
    /// outside the declared representation (ADR 012 §6: a lens/preparation
    /// error, surfaced; nothing is silently reinterpreted).
    #[error("lens preparation rejected: {0}")]
    Preparation(String),
    /// A declared input was missing or unavailable at the recipe's recorded
    /// heads (execution state, distinct from a claim's uninterpreted outcome).
    #[error("lens input unavailable: {0}")]
    Unavailable(String),
    /// The access surface itself failed (storage/runtime), beneath the lens's
    /// own preparation.
    #[error("lens runtime failure: {0}")]
    Runtime(String),
    /// The recorded recipe/plan does not belong to this lens (provenance
    /// mismatch); rendering with another codec is never silent (ADR 012 §4).
    #[error("lens recipe mismatch: {0}")]
    Recipe(String),
}

/// Stage 1 — interest (ADR 012 §3). Synchronously evaluable against gathered
/// evidence; the coordinator shares each requested signal's acquisition work
/// across interested lenses and reads nothing no candidate requested.
pub trait LensInterest {
    fn identity(&self) -> &LensIdentity;
    /// Signals this lens must have gathered to be considered (demand-driven).
    fn signals(&self) -> &[Signal];
    /// Structural content constraints beyond raw signals; evaluated at the
    /// proposal stage, where every declination names its exact reason.
    fn constraints(&self) -> &[Constraint];
}

/// Recognition context the coordinator assembled once from shared signal
/// work: the claim under evaluation, its document, and the raw facet values
/// at the evaluated heads.
pub struct RecognitionContext<'a> {
    pub subject: &'a Subject,
    pub document: &'a DocId,
    /// The document's facet values at the evaluated head set.
    pub facets: &'a HashMap<FacetKey, FacetRaw>,
}

/// Stage 2 — proposal (ADR 012 §3). A decision per claim: `Proposed` enters
/// selection; `Declined` is a recognition rejection with a per-claim visible
/// reason (ADR 012 §5) — "not this format" is a recognition result, and other
/// applicable lenses, including text/blob fallback, are considered; there is
/// no retry loop until something succeeds.
pub trait LensProposal {
    fn propose(&self, ctx: &RecognitionContext<'_>) -> LensDecision;
}

/// The proposal-stage decision: `Proposed` carries the complete declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LensDecision {
    Proposed(Proposal),
    Declined(UninterpretedReason),
}

/// Stage 3 — preparation (ADR 012 §6, §10). Parses/validates edits or
/// discovers the complete output plan; returns buffered operations, never
/// published writes. All selected lenses must prepare before anything stages.
#[async_trait::async_trait]
pub trait LensPrepare {
    /// The ingest half: file bytes at a bound output become candidate document
    /// operations against the recorded recipe and facet bases (ADR 012 §6:
    /// not `ingest(&Delta)` with no base context). Zero prepared operations =
    /// unchanged representation (round-trip stability, §8).
    async fn prepare_ingest(&self, work: &IngestWork<'_>) -> Result<PreparedDocOps, LensFailure>;

    /// The render half: resolve/validate the complete output plan a recipe
    /// produces (paths/kinds declared before visible publication, ADR 012 §9).
    async fn prepare_project(&self, work: &ProjectWork<'_>) -> Result<PreparedPlan, LensFailure>;
}

/// Ingest work: the changed bytes plus everything that makes them an edit of
/// a known representation.
pub struct IngestWork<'a> {
    /// The recipe recorded for the bound output (ADR 012 §6).
    pub recipe: &'a Recipe,
    /// The observed file bytes.
    pub bytes: &'a [u8],
    /// The bound output path, matching the selection record spelling.
    pub path: &'a str,
    /// The recorded facet value at the render heads (base context); `None`
    /// when no render state is recorded yet (a fresh import binding).
    pub base: Option<&'a FacetRaw>,
}

/// Project work: the recipe and the recorded output path the render
/// materializes (the selection's per-output record; v1 plans are exactly one
/// file per recipe, discovered later where a format demands it).
pub struct ProjectWork<'a> {
    pub recipe: &'a Recipe,
    /// The output path recorded by the selection (claim → per-output record).
    pub path: &'a str,
}

/// One buffered document operation, prepared but unpublished (ADR 012 §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetOp {
    pub document: DocId,
    pub facet: FacetKey,
    pub value: FacetRaw,
}

/// Prepared facet operations for one ingest work item. A target outside the
/// proposal's declared Owned facet inputs is a preparation error (ADR 012 §3/
/// §7: only declared destinations may be written; see
/// [`PreparedDocOps::require_declared_destinations`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDocOps {
    pub ops: Vec<FacetOp>,
}

impl PreparedDocOps {
    /// Whether the prepared edit is a no-op (round-trip stability).
    pub fn is_no_op(&self) -> bool {
        self.ops.is_empty()
    }

    /// The prepared operations may only land on the proposal's declared
    /// destinations; anything else is a preparation failure naming the
    /// undeclared destination.
    pub fn require_declared_destinations(&self, proposal: &Proposal) -> Result<(), LensFailure> {
        for op in &self.ops {
            if !proposal
                .owned_facets()
                .any(|(doc, facet)| doc == &op.document && facet == &op.facet)
            {
                return Err(LensFailure::Preparation(format!(
                    "prepared operation writes undeclared destination {} facet {}",
                    op.document, op.facet
                )));
            }
        }
        Ok(())
    }
}

/// One entry of a complete output plan (ADR 012 §9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanOutput {
    pub path: String,
    pub kind: OutputKind,
}

/// A complete output plan: the entry set is decided before visible
/// publication; dynamically discovered paths never stream into the live
/// checkout (ADR 012 §3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPlan {
    pub outputs: Vec<PlanOutput>,
}

/// Facet access the production stage may use to read its declared inputs at a
/// recipe's recorded heads (ADR 012 §8). Object-safe so a lens registry can
/// stay `dyn`; async-trait boxes the future.
#[async_trait::async_trait]
pub trait FacetAccess: Send + Sync {
    /// The facet value at exactly these heads on the access surface; `None`
    /// when the facet is absent there — an availability condition the lens
    /// surfaces, never empty bytes.
    async fn facet_at_heads(&self, dependency: &DepAtHeads) -> Result<Option<FacetRaw>, FacetAccessError>;
}

/// Why facet access failed (ADR 012 §6: availability stays distinct from
/// runtime failure).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FacetAccessError {
    #[error("facet document unavailable: {0}")]
    Unavailable(String),
    #[error("facet access failed: {0}")]
    Runtime(String),
}

/// Stage 4 — production (ADR 012 §8): serves output bytes at a specific
/// recipe/version, lazily and streamably where possible; the recipe's
/// execution compatibility decides how it may be produced (never re-rendered
/// with another codec). v1 returns the byte vector.
#[async_trait::async_trait]
pub trait LensProduce {
    async fn produce(
        &self,
        recipe: &Recipe,
        access: &dyn FacetAccess,
    ) -> Result<Vec<u8>, LensFailure>;
}

/// A diff view (ADR 012 §10: explains logical/rendered differences for status
/// and ordinary branch resolution). Entries are human-readable, in a defined
/// order; empty = the compared states are logically identical.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DifferenceView {
    pub entries: Vec<String>,
}

impl DifferenceView {
    pub fn is_logical_noop(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Stage 5 — diff view. `before`/`after` are the interpreted facet values at
/// the compared head sets (`None` = absent); a missing target is an absence
/// entry, never empty content. Never inserts conflict markers into files.
pub trait LensDiff {
    fn describe_difference(
        &self,
        before: Option<&FacetRaw>,
        after: Option<&FacetRaw>,
    ) -> Result<DifferenceView, LensFailure>;
}

/// One lens object implementing every stage with its shared state (design
/// §1.1): the traits are the seams; splitting into five independent objects
/// would be a later refactor.
pub trait Lens: LensInterest + LensProposal + LensPrepare + LensProduce + LensDiff + Send + Sync {}
impl<T> Lens for T where
    T: LensInterest + LensProposal + LensPrepare + LensProduce + LensDiff + Send + Sync
{
}

/// The registered lenses of one interpretation surface, in declared order.
/// v1 holds the built-in lenses; plug manifests (ADR 007) drive installation
/// later.
pub struct LensRegistry {
    lenses: Vec<Box<dyn Lens>>,
}

impl LensRegistry {
    pub fn new(lenses: Vec<Box<dyn Lens>>) -> Self {
        Self { lenses }
    }

    pub fn lenses(&self) -> &[Box<dyn Lens>] {
        &self.lenses
    }
}