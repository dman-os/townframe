//! The id of a declared type.

/// A type that can be a union member.
///
/// The id is the `$type` value on the wire and the tag a datum key carries, in reverse domain
/// notation: `org.example.daybook.note`. `DatumTag::from` resolves the same string to the
/// well-known tag it names, so one id serves both and a tag is never written twice.
///
/// Emitted by `#[datum(authority = …)]`, with an optional `name = "…"`; not written by hand.
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

/// Whether `id` is an AT Proto NSID: a reversed-domain authority of two or more labels, then one
/// name segment of ASCII letters and digits that starts with a letter.
///
/// Const, because the `datum` macro asserts it of every member id where the type is declared; the
/// admission of a carried Lexicon checks its ids by the same rule.
pub const fn is_nsid(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.len() > 317 {
        return false;
    }
    let mut dot = bytes.len();
    while dot > 0 && bytes[dot - 1] != b'.' {
        dot -= 1;
    }
    if dot == 0 {
        return false;
    }
    let (authority, name) = bytes.split_at(dot - 1);
    is_authority(authority) && is_name(name.split_at(1).1)
}

const fn is_authority(authority: &[u8]) -> bool {
    if authority.is_empty() || authority.len() > 253 || !authority[0].is_ascii_alphabetic() {
        return false;
    }
    let (mut start, mut index, mut labels) = (0, 0, 0);
    while index <= authority.len() {
        if index == authority.len() || authority[index] == b'.' {
            let len = index - start;
            if len == 0 || len > 63 || authority[start] == b'-' || authority[index - 1] == b'-' {
                return false;
            }
            labels += 1;
            start = index + 1;
        } else if !(authority[index].is_ascii_alphanumeric() || authority[index] == b'-') {
            return false;
        }
        index += 1;
    }
    labels >= 2
}

const fn is_name(name: &[u8]) -> bool {
    if name.is_empty() || name.len() > 63 || !name[0].is_ascii_alphabetic() {
        return false;
    }
    let mut index = 1;
    while index < name.len() {
        if !name[index].is_ascii_alphanumeric() {
            return false;
        }
        index += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::is_nsid;

    #[test]
    fn an_nsid_is_an_authority_of_two_labels_and_one_name() {
        for valid in [
            "org.example.note",
            "org.example.daybook.drawer.selfDoc",
            "com.atproto.lexicon.schema",
            "a-b.c1.n2",
        ] {
            assert!(is_nsid(valid), "{valid}");
        }
        for invalid in [
            "",
            "note",
            "example.note",
            "org..note",
            "org.example.",
            ".org.example.note",
            "1org.example.note",
            "org.-example.note",
            "org.example-.note",
            "org.example.1note",
            "org.example.self-doc",
            "org.example.self_doc",
            "org.ex ample.note",
        ] {
            assert!(!is_nsid(invalid), "{invalid}");
        }
        let label = "a".repeat(64);
        assert!(!is_nsid(&format!("org.{label}.note")));
        assert!(!is_nsid(&format!("org.example.{label}")));
    }
}
