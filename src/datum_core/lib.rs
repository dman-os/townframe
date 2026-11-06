//! The vocabulary every datum declaration states itself in: a member's id, the shape of a
//! declared type, the overflow map of an open object, and the wire helpers an open union and an
//! open string vocabulary serialize through.
//!
//! This is the part of the datum discipline a wasm guest needs in order to declare a type. The
//! validators, the storage codec and the Lexicon projection need `jiff`, `jsonschema` and a
//! document store, so they stay in `big_cabinet_core`, which re-exports everything here under
//! `big_cabinet_core::datum`. The `datum` macro in the `macros` crate emits against this API
//! through the path it is given with `runtime = …`.

mod member;
mod shape;
mod value;
mod wire;

/// For the `datum` macro, which joins a const authority to a member's name at compile time.
#[doc(hidden)]
pub use const_format;
pub use member::DatumMember;
pub use member::is_nsid;
pub use member::is_nsid_authority;
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
pub use shape::ShapeError;
#[cfg(feature = "schemars")]
pub use shape::ShapeFlag;
#[cfg(feature = "schemars")]
pub use shape::check_shape;
#[cfg(feature = "schemars")]
pub use wire::MemberSchema;
#[cfg(feature = "schemars")]
pub use wire::string_schema;
#[cfg(feature = "schemars")]
pub use wire::union_schema;
