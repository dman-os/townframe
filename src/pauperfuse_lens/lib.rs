//! The Daybook lens contract (ADR 012): the stage traits, the data contracts
//! lenses declare, and the stateless selector that lives outside the lens.
//!
//! A lens interprets Daybook facets as file representations and file edits as
//! document operations. The pipeline (ADR 012 §1) runs interest → proposal →
//! preparation → production → diff view; the selector drives the lenses in that
//! order but is deliberately *not* part of any lens. Selection recomputes
//! deterministically from current state, signals, configured versions, and
//! overrides; it never executes a fallback loop until a lens succeeds (ADR 012
//! §5).

pub mod identity;
pub mod signals;
pub mod proposal;
pub mod recipe;
pub mod stages;
pub mod outcome;
pub mod selector;
pub mod provenance;

pub use identity::{LensIdentity, LensVersion};
pub use outcome::{ClaimOutcome, ProjectedClaim, TargetStateClass, TargetStubState, Uninterpreted, UninterpretedReason, combine_declinations};
pub use proposal::{
    DocumentAccess, FacetRole, LensCategory, LensInput, OutputKind, OutputSlot, Proposal,
    Specificity, Subject,
};
pub use recipe::{DepAtHeads, ExecCompat, Recipe};
pub use selector::{
    Authority, ExplicitChoice, LensDefault, Selection, SelectionConfig, SelectionError,
    SelectionReason, SubjectSelection, effective_authority, select,
};
pub use stages::FacetAccess;
pub use provenance::{ByteEvidence, OutputProvenance, SelectionProvenance};
pub use signals::{ClaimScope, Constraint, Signal, SignalSet};
pub use stages::{
    DifferenceView, FacetAccessError, FacetOp, IngestWork, Lens, LensDecision,
    LensFailure, LensDiff, LensInterest, LensPrepare, LensProduce, LensProposal, LensRegistry,
    PlanOutput, PreparedDocOps, PreparedPlan, ProjectWork, RecognitionContext,
};