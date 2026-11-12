// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/parseQuotedDiffFileName.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `TextDecoder('utf-8')` is `String::from_utf8_lossy` (both replace
// invalid sequences with U+FFFD by the WHATWG maximal-subpart rule);
// `rawLength` is in UTF-16 code units, as JavaScript's string indexes are
// (see `crate::utf16`).

use crate::utf16;

/// A decoded quoted file name and the length of the raw token it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotedDiffFileName {
    pub file_name: String,
    /// Length of the quoted token in UTF-16 code units.
    pub raw_length: usize,
}

fn named_escape(ch: char) -> Option<char> {
    match ch {
        '"' => Some('"'),
        '\\' => Some('\\'),
        'a' => Some('\x07'),
        'b' => Some('\x08'),
        't' => Some('\t'),
        'n' => Some('\n'),
        'v' => Some('\x0b'),
        'f' => Some('\x0c'),
        'r' => Some('\r'),
        _ => None,
    }
}

/// Reads one quoted Git/jsdiff filename and reports where the token ends.
/// Adjacent octal escapes encode UTF-8 bytes, so they must be decoded together.
pub fn parse_quoted_diff_file_name(input: &str) -> Option<QuotedDiffFileName> {
    let chars: Vec<char> = input.chars().collect();
    if chars.first() != Some(&'"') {
        return None;
    }

    let mut file_name = String::new();
    let mut index = 1;
    while index < chars.len() {
        let ch = chars[index];
        if ch == '"' {
            let raw: String = chars[..=index].iter().collect();
            return Some(QuotedDiffFileName {
                file_name,
                raw_length: utf16::len(&raw),
            });
        }
        if ch != '\\' {
            file_name.push(ch);
            index += 1;
            continue;
        }

        if let Some(escaped) = chars.get(index + 1).copied().and_then(named_escape) {
            file_name.push(escaped);
            index += 2;
            continue;
        }

        let mut bytes: Vec<u8> = Vec::new();
        loop {
            bytes.push(read_octal_byte(&chars, index + 1)?);
            index += 4;
            let continues = chars.get(index) == Some(&'\\')
                && chars
                    .get(index + 1)
                    .is_some_and(|next| ('0'..='7').contains(next));
            if !continues {
                break;
            }
        }
        file_name.push_str(&String::from_utf8_lossy(&bytes));
    }
    None
}

// Requires three octal digits in the byte range so malformed escapes cannot
// be truncated or wrapped when copied into a byte array.
fn read_octal_byte(chars: &[char], index: usize) -> Option<u8> {
    let digit = |offset: usize| chars.get(index + offset).and_then(|ch| ch.to_digit(8));
    let (first, second, third) = (digit(0)?, digit(1)?, digit(2)?);
    // `to_digit(8)` already limits each digit to 0..=7; the first must be 0..=3.
    (first <= 3).then(|| u8::try_from(first * 64 + second * 8 + third).expect("at most 255"))
}
