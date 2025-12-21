//! Automerge module
//! 
//! This module provides automerge-compatible types with Hydrate/Reconcile derives
//! and From/Into implementations for converting between root and automerge types.

pub use crate::gen::automerge::*;

use autosurgeon::{Hydrate, Reconcile};
use serde::{Deserialize, Serialize};
use struct_patch::Patch;
use time::OffsetDateTime;

use crate::gen::automerge::doc::*;

/// Document type for automerge - manually written (excluded from generation)
#[derive(Debug, Clone, Hydrate, Reconcile, Patch, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[patch(attribute(derive(Debug, Default)))]
#[cfg_attr(feature = "uniffi", patch(attribute(derive(uniffi::Record))))]
pub struct Doc {
    pub id: DocId,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub created_at: OffsetDateTime,
    #[serde(with = "utils_rs::codecs::sane_iso8601")]
    #[autosurgeon(with = "utils_rs::am::codecs::date")]
    pub updated_at: OffsetDateTime,
    pub content: DocContent,
    pub props: Vec<DocProp>,
}

// From/Into impls between root and automerge types
// Note: When automerge feature is enabled, crate::Doc is automerge::Doc, so no conversion needed
// Conversions to/from wit are handled in wit.rs conversions module
