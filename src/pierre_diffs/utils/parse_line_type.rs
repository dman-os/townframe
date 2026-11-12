// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/parseLineType.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `console.error` plus `undefined` result for an invalid first character is `None`; `Exclude<HunkLineType, 'expanded'>` is its own enum.

/// `Exclude<HunkLineType, 'expanded'>`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedLineType {
    Context,
    Metadata,
    Addition,
    Deletion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLine {
    pub line: String,
    pub line_type: ParsedLineType,
}

pub fn parse_line_type(line: &str) -> Option<ParsedLine> {
    let first_char = line.chars().next()?;
    let line_type = match first_char {
        ' ' => ParsedLineType::Context,
        '\\' => ParsedLineType::Metadata,
        '+' => ParsedLineType::Addition,
        '-' => ParsedLineType::Deletion,
        _ => return None,
    };
    let processed_line = &line[first_char.len_utf8()..];
    Some(ParsedLine {
        // NOTE(amadeus): If the line is empty, we should make it a
        // newline to force shiki to highlight the row. This should
        // only really ever apply as the last line of a hunk that was most likely
        // processed via a string and not a file since patch files will include a
        // newline here by default
        line: if processed_line.is_empty() {
            String::from("\n")
        } else {
            processed_line.to_owned()
        },
        line_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_by_first_character_and_forces_a_newline_for_empty_lines() {
        assert_eq!(
            parse_line_type("+x\n"),
            Some(ParsedLine {
                line: "x\n".into(),
                line_type: ParsedLineType::Addition
            })
        );
        assert_eq!(
            parse_line_type(" ").map(|parsed| parsed.line),
            Some("\n".into())
        );
        assert_eq!(
            parse_line_type("\\ No newline").map(|parsed| parsed.line_type),
            Some(ParsedLineType::Metadata)
        );
        assert_eq!(parse_line_type("x"), None);
        assert_eq!(parse_line_type(""), None);
    }
}
