// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getLineEndingType.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `'CRLF' | 'CR' | 'LF' | 'none'` string union is an enum with the same serde strings.

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LineEndingType {
    #[serde(rename = "CRLF")]
    Crlf,
    #[serde(rename = "CR")]
    Cr,
    #[serde(rename = "LF")]
    Lf,
    #[serde(rename = "none")]
    None,
}

pub fn get_line_ending_type(content: &str) -> LineEndingType {
    // Windows
    if content.contains("\r\n") {
        return LineEndingType::Crlf;
    }
    // Old Mac
    if content.contains('\r') {
        return LineEndingType::Cr;
    }
    // Unix/Linux/Modern Mac
    if content.contains('\n') {
        return LineEndingType::Lf;
    }
    // No line endings found
    LineEndingType::None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_the_first_matching_ending() {
        assert_eq!(get_line_ending_type("a\r\nb\n"), LineEndingType::Crlf);
        assert_eq!(get_line_ending_type("a\rb"), LineEndingType::Cr);
        assert_eq!(get_line_ending_type("a\nb"), LineEndingType::Lf);
        assert_eq!(get_line_ending_type("ab"), LineEndingType::None);
    }
}
