//! Daybook types crate
//!
//! This crate provides type definitions for daybook with feature-gated support
//! for automerge, uniffi, and wit bindings.

mod interlude {
    pub use serde::{Deserialize, Serialize};

    #[cfg(feature = "automerge")]
    pub use autosurgeon::{Hydrate, Reconcile};

    pub use utils_rs::prelude::*;
}

// Internal generated modules - not exported directly
mod gen;

// Feature modules - these are the public API
// Each module re-exports both manual and generated types, hiding generation details

/// Document types module - root types for general use
///
/// This module contains manually written types (Doc, DocPatch, etc.) and
/// re-exports all generated root types, hiding the generation details.
pub mod doc;

#[cfg(feature = "automerge")]
/// Automerge types module - for hydrate/reconcile boundaries
///
/// This module contains manually written automerge types and
/// re-exports all generated automerge types, hiding the generation details.
pub mod automerge;

#[cfg(feature = "wit")]
/// WIT types module - for WebAssembly Interface Types
///
/// This module contains manually written WIT types and
/// re-exports all generated WIT types, hiding the generation details.
pub mod wit;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

use crate::interlude::*;

uniffi::custom_type!(OffsetDateTime, i64, {
    remote,
    lower: |dt| dt.unix_timestamp(),
    try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
        .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
});

uniffi::custom_type!(Uuid, Vec<u8>, {
    remote,
    lower: |uuid| uuid.as_bytes().to_vec(),
    try_lift: |bytes: Vec<u8>| {
        Uuid::from_slice(&bytes)
            .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
    }
});

use crate::doc::ChangeHashSet;
#[cfg(feature = "uniffi")]
uniffi::custom_type!(ChangeHashSet, Vec<String>, {
    remote,
    lower: |hash| utils_rs::am::serialize_commit_heads(&hash.0),
    try_lift: |strings: Vec<String>| {
        Ok(ChangeHashSet(utils_rs::am::parse_commit_heads(&strings).to_anyhow()?))
    }
});
