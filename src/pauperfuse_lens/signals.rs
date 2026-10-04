//! Recognition signals and structural constraints (ADR 012 §2–§3).
//!
//! A lens declares what it needs to be *considered*; the coordinator gathers
//! each shared signal once across interested lenses and never reads evidence
//! (magic bytes, MIME, sizes) that no candidate requested.

use daybook_types::doc::{MimeType, WellKnownFacetTag};

/// The scope of a dpath claim, from the claim's facet value (FDR 001 §2):
/// an empty target list is the whole-document default scope, a non-empty one
/// is a selective claim naming its materializing facets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClaimScope {
    /// The claim materializes every user-visible facet of the document.
    WholeDocument,
    /// The claim addresses its listed targets.
    Selective,
}

/// One recognition signal (ADR 012 §2). Document-side signals key on claims
/// and facets; file-side signals key on the filesystem evidence ingestion
/// surfaces (path/extension, MIME, magic/header bytes, size).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Signal {
    /// A dpath claim with the declared scope.
    Claim { scope: ClaimScope },
    /// A well-known facet tag exists on the interpreted document.
    FacetTag { tag: WellKnownFacetTag },
    /// File-side: the exact extension as spelled on disk.
    Extension(String),
    /// File-side: MIME evidence.
    MimeType(MimeType),
    /// File-side: the file starts with these bytes.
    MagicPrefix(Vec<u8>),
    /// File-side: an accepted byte-size range. Size is a cheap exclusion
    /// signal (ADR 012 §2): zero-length or oversized inputs may be declined
    /// before parsing.
    SizeRange { min_bytes: u64, max_bytes: Option<u64> },
}

impl Signal {
    /// Whether this signal key matches a declaration, so the coordinator can
    /// count a gathered signal as shared demand.
    pub fn same_evidence(&self, other: &Signal) -> bool {
        matches!(
            (self, other),
            (Signal::Claim { .. }, Signal::Claim { .. })
                | (Signal::FacetTag { .. }, Signal::FacetTag { .. })
                | (Signal::Extension(_), Signal::Extension(_))
                | (Signal::MimeType(_), Signal::MimeType(_))
                | (Signal::MagicPrefix(_), Signal::MagicPrefix(_))
                | (Signal::SizeRange { .. }, Signal::SizeRange { .. })
        )
    }
}

/// What a lens needs to be considered (ADR 012 §3 "Interest declaration").
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SignalSet {
    pub signals: Vec<Signal>,
}

impl SignalSet {
    pub fn new(signals: impl IntoIterator<Item = Signal>) -> Self {
        Self {
            signals: signals.into_iter().collect(),
        }
    }
}

/// Structural constraints beyond raw signals (ADR 012 §3). Interest stays
/// synchronously readable evidence: constraints are *declared* here and
/// evaluated at the proposal stage, whose declinations carry the exact reason
/// per claim (ADR 012 §5). A broken constraint is never an execution failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Constraint {
    /// The claim's Body must resolve to a single, unpinned, same-document
    /// facet reference whose value is a Note with an accepted MIME type
    /// (`None` = representable shape, any MIME).
    BodySelectsNote { mime_types: Vec<MimeType> },
    /// The claim's Body must resolve to a single, unpinned, same-document
    /// facet reference whose value is a Blob (the raw byte representation).
    /// Blob MIME is display-only in v1: no mime-based interpretation.
    BodySelectsBlob,
}