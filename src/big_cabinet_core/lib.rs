// The declaration macro names this crate by path so that it resolves from any user of it; this
// alias is what makes the same expansion resolve inside the crate's own tests.
extern crate self as big_cabinet_core;

pub mod dmeta;
pub mod reference;
pub use reference::{
    DocumentId, FACET_REFERENCE_SCHEME, FacetKey, FacetReference, FacetTag, ReferenceDocumentId,
    ReferenceError, SELF_DOCUMENT,
};

#[cfg(feature = "schemars")]
pub mod facet;

#[cfg(test)]
mod panproto_schema_store_poc;
