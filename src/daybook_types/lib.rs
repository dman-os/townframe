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

// Re-export automerge::Doc for easier access when automerge feature is enabled
#[cfg(feature = "automerge")]
pub use crate::automerge::Doc as AutomergeDoc;

#[cfg(feature = "wit")]
pub mod wit;

// Re-export root types (always available, with serde/uniffi)
pub use gen::root::*;

// When wit feature is enabled, re-export WIT types at crate root for wit-bindgen compatibility
// This allows wit-bindgen generated code to use daybook_types::DocProp etc. and get WIT types
#[cfg(feature = "wit")]
pub use gen::wit::doc::{DocContent, DocProp, DocPropKind, ImageMeta, DocBlob};

// Re-export root doc types (when wit is not enabled, or for types not overridden by WIT)
#[cfg(not(feature = "wit"))]
pub use gen::root::doc::*;

// Document types module
pub mod doc;

// Re-export Doc and DocPatch from doc module for convenience
pub use doc::{Doc, DocPatch};

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


