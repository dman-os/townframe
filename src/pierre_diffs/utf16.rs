//! UTF-16 offset policy.
//!
//! Original to this crate (no upstream counterpart).
//!
//! # Policy
//!
//! Upstream is JavaScript, so every offset it exposes is a count of UTF-16 code
//! units (`string.length`, `String.prototype.slice`, regex `match.index`). The
//! ones that cross a boundary visible to the rest of the system are
//! `DecorationItem` spans (`spanStart`/`spanLength`), token `lineCharStart`/
//! `lineCharEnd`, `FileDiffMetadata.{addition,deletion}CharacterCount` and
//! `Hunk` line counts. This port keeps those quantities **in UTF-16 code
//! units** so that JSON produced by this crate is identical to Pierre's and the
//! differential oracle can compare it without translation.
//!
//! Text itself is stored as Rust `String` (UTF-8). Code that slices text works
//! in byte offsets internally and converts at the edge with the helpers below.
//! Two consequences are deliberate:
//!
//! * A UTF-16 offset that lands between the two halves of a surrogate pair has
//!   no byte offset; [`byte_offset`] returns `None` and callers treat that as
//!   an invariant violation.
//! * Strings with lone surrogates are not representable. Upstream inputs are
//!   file contents and patches decoded from UTF-8, which cannot contain them,
//!   and oracle corpora avoid them.
//!
//! The diff layer (`diff.rs`, on `similar`) tokenises `&str` by `char`, so
//! change values carry no offsets; only the
//! *consumers* of change objects (word-diff decorations, S5) count UTF-16.

/// `string.length` in JavaScript: the number of UTF-16 code units.
pub fn len(text: &str) -> usize {
    text.chars().map(char::len_utf16).sum()
}

/// Converts a UTF-16 offset into `text` to a byte offset. Returns `None` when
/// the offset is past the end or splits a surrogate pair.
pub fn byte_offset(text: &str, utf16_offset: usize) -> Option<usize> {
    let mut units = 0;
    for (byte_index, ch) in text.char_indices() {
        if units == utf16_offset {
            return Some(byte_index);
        }
        if units > utf16_offset {
            return None;
        }
        units += ch.len_utf16();
    }
    (units == utf16_offset).then_some(text.len())
}

/// Converts a byte offset (which must lie on a char boundary) to UTF-16 units.
pub fn utf16_offset(text: &str, byte_offset: usize) -> usize {
    len(&text[..byte_offset])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lengths_count_code_units() {
        assert_eq!(len(""), 0);
        assert_eq!(len("abc"), 3);
        assert_eq!(len("é"), 1);
        assert_eq!(len("😀"), 2);
        assert_eq!(len("a😀b"), 4);
    }

    #[test]
    fn offsets_round_trip_and_reject_surrogate_splits() {
        let text = "a😀é";
        assert_eq!(byte_offset(text, 0), Some(0));
        assert_eq!(byte_offset(text, 1), Some(1));
        assert_eq!(byte_offset(text, 2), None);
        assert_eq!(byte_offset(text, 3), Some(5));
        assert_eq!(byte_offset(text, 4), Some(text.len()));
        assert_eq!(byte_offset(text, 5), None);
        assert_eq!(utf16_offset(text, 5), 3);
        assert_eq!(utf16_offset(text, text.len()), 4);
    }
}
