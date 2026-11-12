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
mod nsid;
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
pub use nsid::InvalidNsid;
pub use nsid::Nsid;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;

    const AUTHORITY: &str = "org.example.runtime";

    /// The declaration macro emits against whichever module `runtime =` names, so a crate that
    /// links `datum_core` alone (a wasm guest) can declare a type without `big_cabinet_core`.
    #[macros::datum(runtime = crate, authority = AUTHORITY)]
    #[derive(Serialize, Deserialize)]
    struct Declared {
        label: String,
        #[serde(flatten)]
        extra: Extra,
    }

    #[test]
    fn the_runtime_argument_routes_the_emitted_items() {
        assert_eq!(
            <Declared as DatumMember>::ID,
            "org.example.runtime.declared"
        );
        assert_eq!(
            <Declared as DatumShape>::SHAPE,
            Shape::Object {
                openness: Openness::Open
            }
        );
    }
}
