//! Per-claim outcome vocabulary and the Q5 resolution: claims the projector
//! cannot interpret are *visible outcomes*, never hard errors. The
//! target-missing states — absent facet, `Pending` stub, `Blob` facet — are
//! **solved-visible**: classified, settled outcomes rather than pending
//! failures, and never a silent zero-byte stub either (ADR 012 §1/§5).

use daybook_types::doc::WellKnownFacet;

use crate::proposal::{Proposal, Subject};
use crate::recipe::Recipe;

/// How a stub/blob target state presents: the interpreter classes can account
/// for the claim, but the addressed facet is not renderable bytes today (blob
/// retrieval is later backend-strategy work, ADR 013).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TargetStubState {
    /// The facet is a `Pending` stub.
    Pending,
    /// The facet is a `Blob` payload.
    Blob,
}

impl core::fmt::Display for TargetStubState {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            TargetStubState::Pending => "pending stub",
            TargetStubState::Blob => "blob facet",
        })
    }
}

/// One reference target's state class, used to classify claims (and, for
/// selective claims, each target) into visible outcomes. Solved-visible
/// classes return even though nothing renderable exists: the state is
/// *accounted*, which is precisely what keeps a stub/blob from inventing a
/// fake zero-length file in the checkout (design §7's failure model).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStateClass {
    /// Solved-visible: the addressed facet is absent at the evaluated heads.
    Missing,
    /// Solved-visible: a stub/blob facet state.
    StubOrBlob(TargetStubState),
    /// Present and representable in principle, but no installed lens proposes
    /// for this claim class yet (e.g. selective claims).
    RepresentableNoLens,
    /// Present, but not a shape any interpreter represents.
    NotRepresentable,
    /// Present with a foreign/unparseable facet value (not a known facet shape).
    UnknownShape,
}

impl TargetStateClass {
    /// Classifies one facet value (by well-known facet shape) for outcome
    /// reporting. `None` = absent at the evaluated heads.
    pub fn of_value(value: Option<&serde_json::Value>) -> Self {
        let Some(value) = value else {
            return Self::Missing;
        };
        let Ok(facet) = serde_json::from_value::<WellKnownFacet>(value.clone()) else {
            return Self::UnknownShape;
        };
        match facet {
            WellKnownFacet::Blob(_) => Self::StubOrBlob(TargetStubState::Blob),
            WellKnownFacet::Pending { .. } => Self::StubOrBlob(TargetStubState::Pending),
            // Facets a future interpreter could represent through this claim.
            WellKnownFacet::Note(_)
            | WellKnownFacet::ImageMetadata(_)
            | WellKnownFacet::OcrResult(_)
            | WellKnownFacet::Embedding(_) => Self::RepresentableNoLens,
            WellKnownFacet::Dmeta(_)
            | WellKnownFacet::RefGeneric(_)
            | WellKnownFacet::LabelGeneric(_)
            | WellKnownFacet::TitleGeneric(_)
            | WellKnownFacet::PathGeneric(_)
            | WellKnownFacet::Body(_)
            | WellKnownFacet::BlobPin(_)
            | WellKnownFacet::PlugsConfig(_)
            | WellKnownFacet::PlugManifest(_)
            | WellKnownFacet::Branch(_)
            | WellKnownFacet::Branches(_) => Self::NotRepresentable,
        }
    }
}

impl core::fmt::Display for TargetStateClass {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            TargetStateClass::Missing => write!(formatter, "missing"),
            TargetStateClass::StubOrBlob(stub) => write!(formatter, "{stub}"),
            TargetStateClass::RepresentableNoLens => {
                write!(
                    formatter,
                    "representable (no selective-claim lens installed)"
                )
            }
            TargetStateClass::NotRepresentable => write!(formatter, "not representable"),
            TargetStateClass::UnknownShape => write!(formatter, "unknown shape"),
        }
    }
}

/// Why a claim produced no projection. Non-renderable claims are outcomes, not
/// errors; they are reported per claim and per target (Q5, resolved).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UninterpretedReason {
    /// No installed lens proposes for this claim's shape (v1: selective dpath
    /// claims); the per-target states are named in the detail.
    NoLens(String),
    /// Solved-visible (Q5): the facet the claim addresses is absent at the
    /// evaluated heads. The claim stays a resolved classification, never a
    /// pending error.
    TargetMissing(String),
    /// Solved-visible (Q5): the addressed facet is a stub (Pending) or Blob
    /// state the interpreters cannot render; blob retrieval is later backend
    /// strategy work (ADR 013).
    TargetStubOrBlob {
        target: String,
        stub: TargetStubState,
    },
    /// The target exists but is not the shape any interpreter represents.
    TargetShape(String),
    /// The claim's declared Body/reference resolution is outside the
    /// interpreters' declared classes (cross-document, pinned, encoded).
    ReferenceUnsupported(String),
    /// The claim's shape does not match any interpreter's class.
    ClaimShape(String),
    /// The claim's facet key is a malformed dpath label (FDR 001 §4: reported,
    /// never silently dropped).
    ClaimMalformed(String),
    /// The claim's output path structure is not a valid materializable path.
    OutputPath(String),
}

impl core::fmt::Display for UninterpretedReason {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            UninterpretedReason::NoLens(detail) => write!(formatter, "no lens: {detail}"),
            UninterpretedReason::TargetMissing(target) => {
                write!(formatter, "target missing: {target}")
            }
            UninterpretedReason::TargetStubOrBlob { target, stub } => {
                write!(formatter, "{stub} target: {target}")
            }
            UninterpretedReason::TargetShape(detail) => write!(formatter, "target shape: {detail}"),
            UninterpretedReason::ReferenceUnsupported(detail) => {
                write!(formatter, "reference unsupported: {detail}")
            }
            UninterpretedReason::ClaimShape(detail) => write!(formatter, "claim shape: {detail}"),
            UninterpretedReason::ClaimMalformed(detail) => {
                write!(formatter, "malformed dpath claim: {detail}")
            }
            UninterpretedReason::OutputPath(detail) => write!(formatter, "output path: {detail}"),
        }
    }
}

impl UninterpretedReason {
    /// Whether this is a solved-visible classification (Q5): settled, no retry
    /// expectation, no blocking state.
    pub fn solved_visible(&self) -> bool {
        matches!(
            self,
            UninterpretedReason::TargetMissing(_) | UninterpretedReason::TargetStubOrBlob { .. }
        )
    }

    /// Ordering for combining several lens declinations into one claim's
    /// visible outcome: the most informative per-target classifications first,
    /// so a stub/blob state is never buried under a generic class note.
    fn informative_rank(&self) -> u8 {
        match self {
            Self::TargetStubOrBlob { .. } => 0,
            Self::TargetMissing(_) => 1,
            Self::TargetShape(_) => 2,
            Self::ReferenceUnsupported(_) => 3,
            Self::OutputPath(_) => 4,
            Self::ClaimShape(_) => 5,
            Self::NoLens(_) => 6,
            Self::ClaimMalformed(_) => 7,
        }
    }
}

/// A claim's non-projection outcome: one visible, settled reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uninterpreted {
    /// The claim's subject; `None` when the claim key itself is malformed.
    pub subject: Option<Subject>,
    pub reason: UninterpretedReason,
}

impl core::fmt::Display for Uninterpreted {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.subject {
            Some(subject) => write!(
                formatter,
                "claim {}: uninterpreted — {}",
                subject, self.reason
            ),
            None => write!(formatter, "uninterpreted — {}", self.reason),
        }
    }
}

/// The result of interpreting one claim under the lens contract: a selected
/// proposal with its recipe, or a visible uninterpreted outcome (Q5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedClaim {
    pub subject: Subject,
    pub proposal: Proposal,
    /// The production recipe at the evaluated heads (selection provenance,
    /// design §4).
    pub recipe: Recipe,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// Boxed so the common `Uninterpreted` status path (a small enum) does not
    /// pay the proposal/recipe payload on every `ClaimOutcome` copy.
    Projected(Box<ProjectedClaim>),
    Uninterpreted(Uninterpreted),
}

impl ClaimOutcome {
    pub fn subject(&self) -> Option<&Subject> {
        match self {
            ClaimOutcome::Projected(projected) => Some(&projected.subject),
            ClaimOutcome::Uninterpreted(uninterpreted) => uninterpreted.subject.as_ref(),
        }
    }

    pub fn uninterpreted(&self) -> Option<&Uninterpreted> {
        match self {
            ClaimOutcome::Uninterpreted(uninterpreted) => Some(uninterpreted),
            _ => None,
        }
    }
}

impl core::fmt::Display for ClaimOutcome {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ClaimOutcome::Projected(projected) => write!(
                formatter,
                "claim {}: projected by {}",
                projected.subject, projected.proposal.lens
            ),
            ClaimOutcome::Uninterpreted(uninterpreted) => uninterpreted.fmt(formatter),
        }
    }
}

/// Combines several declined interpretations of one claim into the single
/// most informative classification, chosen by the per-variant priority above
/// so a stub/blob state is never buried under a generic class note.
pub fn combine_declinations(declinations: &[UninterpretedReason]) -> Option<UninterpretedReason> {
    let strongest = declinations
        .iter()
        .max_by_key(|reason| core::cmp::Reverse(reason.informative_rank()));
    strongest.cloned()
}
