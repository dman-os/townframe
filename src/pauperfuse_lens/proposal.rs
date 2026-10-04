//! The proposal contract (ADR 012 §3): what a lens that applies to a claim
//! must declare about inputs, ownership, interpretation category, specificity,
//! and output structure — before selection, and before any visible
//! publication.

use daybook_types::doc::{DocId, FacetKey};
use daybook_types::dpath::Dpath;

use crate::identity::LensIdentity;

/// Selection granularity for this contract's subjects: one dpath claim of one
/// document (v1). Workspace/multi-file proposals own several subjects later;
/// selection already groups and disjoint-wins by subject.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Subject {
    /// A dpath claim at its exact label. Whole-document or selective scope
    /// lives in the claim's facet value, not here.
    Dpath(Dpath),
}

impl core::fmt::Display for Subject {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Subject::Dpath(dpath) => write!(formatter, "{dpath}"),
        }
    }
}

/// How the interpreted document may be written by a selected lens invocation
/// (ADR 012 §7). The effective access is the *weakest* across every declared
/// input, including recognition-only context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocumentAccess {
    /// The checkout owns/opens this document for staging: declared
    /// destinations may receive facet operations on ingest.
    Owner,
    /// Read-only context (e.g. a cross-document reference): participates in
    /// the weakest-authority computation but is never writably owned. A
    /// reference does not confer ownership of its holder's facets.
    ReadOnlyContext,
}

/// The interpretation categories the selector orders (ADR 012 §4), strongest
/// first by declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LensCategory {
    /// Reserved for the selector: an explicit user override ranks above every
    /// interpretation. A lens that self-declares it is a proposal-integrity
    /// error — a proposal cannot know user configuration.
    ExplicitChoice,
    /// A recognized workspace/multi-file interpretation.
    WorkspaceCompound,
    /// A recognized structured single-format interpretation.
    IndividualFormat,
    /// Raw text / blob fallback.
    BasicFallback,
}

/// The lens's declared specificity rank within its category: a proposal that
/// matched more of its declared contextual shape outranks one that matched
/// less (design §1.3). Declared by the lens, never computed by the selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Specificity(pub u8);

/// Stable output slot of a proposal's complete output structure (ADR 012 §9):
/// an internal name, the materialization kind, and the claimed path (exact
/// checkout naming happens there, collisions and platform limits resolved by
/// the coordinator, never by rewriting claim identities).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSlot {
    /// Stable internal slot id (e.g. `"body"`, `"sidecar"`).
    pub slot: String,
    pub kind: OutputKind,
    /// The claimed relative path structure, as a slash-separated key.
    pub path: String,
}

/// Kinds of materialization (ADR 012 §9: N files, directories with content).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputKind {
    File,
    Directory,
}

/// One declared facet dependency with the role that decides both authority
/// treatment and recipe placement (design §1.2: owned editable outputs
/// distinguished from read-only context).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LensInput {
    /// A named document under this interpretation. The document access is the
    /// per-invocation authorization surface; facet values inside it follow the
    /// facet role.
    Document { document: DocId, access: DocumentAccess },
    /// A facet dependency: `Owned` inputs are the write destinations on ingest
    /// and the affected production dependencies; `Context` inputs are
    /// read-only and recorded as recipe context (ADR 012 §3, §8).
    Facet { document: DocId, facet: FacetKey, role: FacetRole },
}

/// How a facet participates (ADR 012 §3/§8). Facet-level spelling of ADR 012
/// §3's "owned editable outputs / read-only context" distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FacetRole {
    /// The lens writes this facet's value on ingest; it is an affected
    /// (production) dependency.
    Owned,
    /// Read-only to shape the interpretation; recorded as recipe context.
    Context,
}

/// A complete proposal declaration. Required fields, not options (design
/// §1.2); anything a proposal cannot declare is an interface finding (ADR 012
/// §11), not a hidden assumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub lens: LensIdentity,
    /// The claim this proposal interprets (v1: exactly one).
    pub subject: Subject,
    /// Declared inputs incl. recognition-only context; each Owned facet is a
    /// write destination (ADR 012 §3).
    pub inputs: Vec<LensInput>,
    pub category: LensCategory,
    pub specificity: Specificity,
    /// Stable output slots: kind + internal name + claimed path structure.
    pub outputs: Vec<OutputSlot>,
}

impl Proposal {
    /// The Owned facet dependencies: the write destinations on ingest.
    pub fn owned_facets(&self) -> impl Iterator<Item = (&DocId, &FacetKey)> {
        self.inputs.iter().filter_map(|input| match input {
            LensInput::Facet { document, facet, role: FacetRole::Owned } => Some((document, facet)),
            _ => None,
        })
    }
}
