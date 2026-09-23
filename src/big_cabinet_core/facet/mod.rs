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
mod universe;
mod value;

#[cfg(feature = "schemars")]
mod codec;
mod wire;

#[cfg(all(test, feature = "schemars"))]
mod e2e;

pub use member::{FacetMember, str_eq};
pub use shape::{FacetShape, Openness, Shape, Tagging};
pub use universe::{Bundle, Declaration, FacetDecl, TypeDecl, Universe, UniverseError};
pub use value::Extra;

#[cfg(feature = "schemars")]
pub use codec::{
    Bytes, Codec, CodecError, Counter, Float32, Float64, KEYWORD, Stored, Text, codec_schema,
    hydrate_value,
};
#[cfg(feature = "schemars")]
pub use shape::{ShapeError, ShapeFlag, check_shape};
pub use wire::{
    KnownString, Member, MemberSchema, UnknownMember, deserialize_known_str, deserialize_union,
    open_string_schema, serialize_member, union_schema,
};
