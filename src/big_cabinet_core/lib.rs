// The declaration macro names this crate by path so that it resolves from any user of it; this
// alias is what makes the same expansion resolve inside the crate's own tests.
extern crate self as big_cabinet_core;

pub mod authority;
pub mod datum;
pub mod dmeta;
pub mod doc;
pub mod drawer;
pub mod reference;
pub mod schema_compat;

pub use reference::DATUM_REFERENCE_SCHEME;
pub use reference::DatumKey;
pub use reference::DatumReference;
pub use reference::DatumTag;
pub use reference::DocId;
pub use reference::ReferenceDocId;
pub use reference::ReferenceError;
pub use reference::SELF_DOC;

#[cfg(test)]
mod panproto_schema_store_poc;
#[cfg(test)]
mod protocol_wire;
