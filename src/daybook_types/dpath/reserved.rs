//! Reserved materialization surfaces (FDR 001 §5).

use crate::doc::DocId;

/// The reserved materialization surface holding dpathless documents (FDR 001 §5).
pub const BY_ID_SURFACE_NAME: &str = "by-id";

/// A surface the materializer owns, which is not a dpath claim (FDR 001 §5).
///
/// Reserved surfaces win their literal real name; dpath claimants of the same
/// name spill into the `.d` suffix rules (so a claim on `/by-id/x` materializes
/// at `/by-id.d/x`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedSurface {
    pub name: String,
    pub kind: ReservedKind,
}

/// What a reserved surface holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservedKind {
    /// `/by-id`: dpathless documents, materialized as full JSON reprs.
    /// Documents that have dpaths do **not** appear here (FDR 001 §5).
    ById { doc_ids: Vec<DocId> },
    /// A checkout metadata directory (`.dtree`, `.dnode`): contents are not
    /// dpath claims, so the derived tree only holds the directory itself.
    MetadataDir,
}

impl ReservedSurface {
    /// The `/by-id` surface over the given dpathless documents.
    pub fn by_id(doc_ids: impl IntoIterator<Item = DocId>) -> Self {
        Self {
            name: BY_ID_SURFACE_NAME.to_string(),
            kind: ReservedKind::ById {
                doc_ids: doc_ids.into_iter().collect(),
            },
        }
    }

    /// A checkout metadata directory (FDR 002 owns the actual names).
    pub fn metadata_dir(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: ReservedKind::MetadataDir,
        }
    }
}
