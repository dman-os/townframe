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

mod member;
mod shape;
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
mod wire;

#[cfg(all(test, feature = "schemars"))]
mod e2e;

/// For the `datum` macro, which joins a const authority to a member's name at compile time.
#[doc(hidden)]
pub use const_format;
pub use member::DatumMember;
pub use member::is_nsid;
pub use member::str_eq;
pub use shape::DatumShape;
pub use shape::Openness;
pub use shape::Shape;
pub use shape::Tagging;
pub use value::Extra;
pub use wire::KnownString;
pub use wire::Member;
pub use wire::UnionLayout;
pub use wire::UnknownMember;
pub use wire::deserialize_closed_str;
pub use wire::deserialize_known_str;
pub use wire::deserialize_union;
pub use wire::serialize_member;

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
pub use shape::ShapeError;
#[cfg(feature = "schemars")]
pub use shape::ShapeFlag;
#[cfg(feature = "schemars")]
pub use shape::check_shape;
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
#[cfg(feature = "schemars")]
pub use wire::MemberSchema;
#[cfg(feature = "schemars")]
pub use wire::string_schema;
#[cfg(feature = "schemars")]
pub use wire::union_schema;
