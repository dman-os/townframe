//! The id of a declared type.

/// A type that can be a union member.
///
/// The id is the `$type` value on the wire and the tag a datum key carries, in reverse domain
/// notation: `org.example.daybook.note`. `DatumTag::from` resolves the same string to the
/// well-known tag it names, so one id serves both and a tag is never written twice.
///
/// Emitted by `#[datum(id = "…")]` or `#[datum(id_prefix = "…")]`; not written by hand.
pub trait DatumMember {
    const ID: &'static str;
}

/// Byte comparison usable in a const, for the assert that keeps a serde tag equal to a member id.
pub const fn str_eq(one: &str, two: &str) -> bool {
    let (one, two) = (one.as_bytes(), two.as_bytes());
    if one.len() != two.len() {
        return false;
    }
    let mut index = 0;
    while index < one.len() {
        if one[index] != two[index] {
            return false;
        }
        index += 1;
    }
    true
}
