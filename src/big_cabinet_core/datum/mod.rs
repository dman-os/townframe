//! Datum declaration: what a declaration states, the conventions on top of the schema, and how a
//! value is stored in a document.
//!
//! Schemas are JSON Schema 2020-12, derived by `schemars` from a Rust type. Three things JSON Schema
//! leaves open are ours — what happens to a member a declaration does not name, where a union's
//! discriminant sits, and how a value is stored — and each is a sidecar keyword, which the
//! specification reads as an annotation and a conforming validator ignores.
//!
//! The `datum` macro in the `macros` crate does the refusing, because refusing means reading the
//! item's attributes and a derive cannot do that.

mod grammar;
pub mod operation;
mod value;

#[cfg(feature = "schemars")]
mod codec;
#[cfg(feature = "schemars")]
mod input;
#[cfg(feature = "schemars")]
pub mod lexicon;
#[cfg(feature = "schemars")]
pub use input::LexiconAdmissionError;

#[cfg(feature = "schemars")]
mod universe;

#[cfg(all(test, feature = "schemars"))]
mod e2e;

/// The vocabulary a wasm guest declares a type with lives in `datum_core`; everything the
/// declaration macro emits against resolves through this path as well.
pub use datum_core::*;
pub use grammar::GrammarError;
pub use grammar::parse_ref;
pub use value::validate_object_schema;
pub use value::validate_object_value;

#[cfg(feature = "schemars")]
pub use codec::Bytes;
#[cfg(feature = "schemars")]
pub use codec::Codec;
#[cfg(feature = "schemars")]
pub use codec::CodecError;
#[cfg(feature = "schemars")]
pub use codec::CompiledSchema;
#[cfg(feature = "schemars")]
pub use codec::Counter;
#[cfg(feature = "schemars")]
pub use codec::DeclaredReference;
#[cfg(feature = "schemars")]
pub use codec::Float32;
#[cfg(feature = "schemars")]
pub use codec::Float64;
#[cfg(feature = "schemars")]
pub use codec::KEYWORD;
#[cfg(feature = "schemars")]
pub use codec::Stored;
#[cfg(feature = "schemars")]
pub use codec::Text;
#[cfg(feature = "schemars")]
pub use codec::codec_schema;
#[cfg(feature = "schemars")]
pub use codec::declared_references;
#[cfg(feature = "schemars")]
pub use codec::hydrate_value;
#[cfg(feature = "schemars")]
pub use codec::reconcile_json_atomic;
#[cfg(feature = "schemars")]
pub use universe::Bundle;
#[cfg(feature = "schemars")]
pub use universe::DatumDecl;
#[cfg(feature = "schemars")]
pub use universe::Declaration;
#[cfg(feature = "schemars")]
pub use universe::Lexicon;
#[cfg(feature = "schemars")]
pub use universe::LexiconError;
#[cfg(feature = "schemars")]
pub use universe::TypeDecl;
#[cfg(feature = "schemars")]
pub use universe::Universe;
#[cfg(feature = "schemars")]
pub use universe::UniverseError;
