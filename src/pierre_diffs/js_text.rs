//! JavaScript text semantics the port must reproduce exactly.
//!
//! Original to this crate (no upstream counterpart). Upstream relies on the
//! engine's definitions of `\s`, `String.prototype.trim` and "line terminator";
//! Rust's `char::is_whitespace`, `str::trim` and `str::lines` differ (Rust
//! counts U+0085 and excludes U+FEFF). Every ported function that upstream
//! writes as `/\s/`, `.trim()` or `^`/`$` under the `m` flag goes through
//! these helpers.

/// ECMAScript `\s` (and the `WhiteSpace` + `LineTerminator` set used by
/// `String.prototype.trim`): TAB, VT, FF, SP, NBSP, ZWNBSP (U+FEFF), the Zs
/// category (U+1680, U+2000..=U+200A, U+202F, U+205F, U+3000), LF, CR, LS and
/// PS. U+180E and U+0085 are not whitespace in current ECMAScript.
pub fn is_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\t' | '\u{000b}' | '\u{000c}' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
                | '\n'
                | '\r'
    )
}

/// ECMAScript `LineTerminator`: LF, CR, LS, PS. This is what `.` excludes and
/// what `^`/`$` anchor on under the `m` flag.
pub fn is_line_terminator(ch: char) -> bool {
    matches!(ch, '\n' | '\r' | '\u{2028}' | '\u{2029}')
}

/// `String.prototype.trim`.
pub fn trim(text: &str) -> &str {
    text.trim_matches(is_whitespace)
}

/// `String.prototype.trimStart`.
pub fn trim_start(text: &str) -> &str {
    text.trim_start_matches(is_whitespace)
}

/// `String.prototype.trimEnd`.
pub fn trim_end(text: &str) -> &str {
    text.trim_end_matches(is_whitespace)
}

/// `/\s/.test(text)`: the unanchored test, true when any character is
/// whitespace.
pub fn contains_whitespace(text: &str) -> bool {
    text.chars().any(is_whitespace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitespace_set_matches_ecmascript() {
        let inside = [
            '\t', '\n', '\u{b}', '\u{c}', '\r', ' ', '\u{a0}', '\u{1680}', '\u{2000}', '\u{200a}',
            '\u{2028}', '\u{2029}', '\u{202f}', '\u{205f}', '\u{3000}', '\u{feff}',
        ];
        for ch in inside {
            assert!(is_whitespace(ch), "{ch:?}");
        }
        // Rust treats U+0085 as whitespace, ECMAScript does not; U+180E and
        // U+200B are not whitespace in either.
        for ch in ['\u{85}', '\u{180e}', '\u{200b}', 'a', '0', '\u{2010}'] {
            assert!(!is_whitespace(ch), "{ch:?}");
        }
    }

    #[test]
    fn trim_uses_ecmascript_whitespace() {
        assert_eq!(trim("\u{feff} a \u{85}\u{a0}"), "a \u{85}");
        assert_eq!(trim_start("\u{2028} a "), "a ");
        assert_eq!(trim_end(" a\u{3000}"), " a");
    }
}
