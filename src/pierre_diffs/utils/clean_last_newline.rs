// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/cleanLastNewline.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language (`&str` slice instead of a copy).

/// Drops one trailing `\n`, and a `\r` before it.
pub fn clean_last_newline(contents: &str) -> &str {
    match contents.strip_suffix('\n') {
        Some(without_newline) => without_newline
            .strip_suffix('\r')
            .unwrap_or(without_newline),
        None => contents,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_one_line_ending() {
        assert_eq!(clean_last_newline("a\r\n"), "a");
        assert_eq!(clean_last_newline("a\n\n"), "a\n");
        assert_eq!(clean_last_newline("a\r"), "a\r");
        assert_eq!(clean_last_newline(""), "");
    }
}
