//! Facet declaration: what a declaration states, the conventions on top of the schema, and how a
//! value is stored in a document.
//!
//! Schemas are JSON Schema 2020-12, derived by `schemars` from a Rust type. Three things JSON Schema
//! leaves open are ours — what happens to a member a declaration does not name, where a union's
//! discriminant sits, and how a value is stored — and each is a sidecar keyword, which the
//! specification reads as an annotation and a conforming validator ignores.
//!
//! The `facet` macro in the `macros` crate does the refusing, because refusing means reading the
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

pub use member::{FacetMember, str_eq};
pub use shape::{FacetShape, Openness, Shape, Tagging};
pub use value::Extra;
pub use wire::{
    KnownString, Member, UnionLayout, UnknownMember, deserialize_known_str, deserialize_union,
    serialize_member,
};

#[cfg(feature = "schemars")]
pub use codec::{
    Bytes, Codec, CodecError, CompiledSchema, Counter, DeclaredReference, Float32, Float64,
    KEYWORD, Stored, Text, codec_schema, declared_references, hydrate_value, reconcile_json_atomic,
};
#[cfg(feature = "schemars")]
pub use shape::{ShapeError, ShapeFlag, check_shape};
#[cfg(feature = "schemars")]
pub use universe::{
    Bundle, Declaration, FacetDecl, Lexicon, LexiconError, TypeDecl, Universe, UniverseError,
};
#[cfg(feature = "schemars")]
pub use wire::{MemberSchema, open_string_schema, union_schema};
