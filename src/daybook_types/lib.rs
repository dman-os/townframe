//! Daybook types crate
//! 
//! This crate provides type definitions for daybook with feature-gated support
//! for automerge, uniffi, and wit bindings.

mod interlude {
    pub use serde::{Deserialize, Serialize};
    
    #[cfg(feature = "automerge")]
    pub use autosurgeon::{Hydrate, Reconcile};
    
    #[cfg(feature = "automerge")]
    pub use time::OffsetDateTime;
    
    #[cfg(feature = "wit")]
    pub use api_utils_rs::wit::townframe::api_utils::utils::Datetime;

    pub use utils_rs::prelude::*;
}

pub mod gen;

#[cfg(feature = "automerge")]
pub mod automerge;

#[cfg(feature = "wit")]
pub mod wit;

// Re-export generated types from gen module
pub use gen::*;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(feature = "uniffi")]
mod uniffi_custom_types {
    use time::OffsetDateTime;
    
    uniffi::custom_type!(OffsetDateTime, i64, {
        remote,
        lower: |dt| dt.unix_timestamp(),
        try_lift: |int| OffsetDateTime::from_unix_timestamp(int)
            .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
    });
}

#[cfg(feature = "uniffi")]
mod uniffi_uuid {
    use uuid::Uuid;
    
    uniffi::custom_type!(Uuid, Vec<u8>, {
        remote,
        lower: |uuid| uuid.as_bytes().to_vec(),
        try_lift: |bytes: Vec<u8>| {
            Uuid::from_slice(&bytes)
                .map_err(|err| uniffi::deps::anyhow::anyhow!(err))
        }
    });
}

// Re-export types based on enabled features
// When automerge feature is enabled, use automerge types, otherwise use root types
#[cfg(feature = "automerge")]
mod _types {
    pub use crate::gen::automerge::doc::{
        DocContent, DocContentKind, DocId, DocProp, DocPropKind,
    };
    pub use crate::automerge::Doc;
}

#[cfg(not(feature = "automerge"))]
mod _types {
    use crate::interlude::*;
    
    pub use crate::gen::root::doc::{
        DocContent, DocContentKind, DocId, DocProp, DocPropKind,
    };
    
    /// Document type - manually written (excluded from generation)
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
    pub struct Doc {
        pub id: DocId,
        #[serde(with = "utils_rs::codecs::sane_iso8601")]
        pub created_at: time::OffsetDateTime,
        #[serde(with = "utils_rs::codecs::sane_iso8601")]
        pub updated_at: time::OffsetDateTime,
        pub content: DocContent,
        pub props: Vec<DocProp>,
    }
}

pub use _types::*;

// Re-export gen::doc module for backward compatibility (for DocPatch access)
pub mod doc {
    #[cfg(feature = "automerge")]
    pub use crate::gen::automerge::doc::*;
    #[cfg(not(feature = "automerge"))]
    pub use crate::gen::root::doc::*;
    
    // Re-export DocPatch from automerge module when feature is enabled
    // struct_patch generates DocPatch in the same module as Doc
    #[cfg(feature = "automerge")]
    pub use crate::automerge::DocPatch;
}

