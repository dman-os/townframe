// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/splitFileContents.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: uses `scanners::split_with_newlines` for the `/(?<=\\n)/` split.

use crate::scanners::split_with_newlines;

/// Splits file contents into lines using the same logic as diff parsing.
/// Preserves trailing newlines on each line.
pub fn split_file_contents(contents: &str) -> Vec<&str> {
    if contents.is_empty() {
        Vec::new()
    } else {
        split_with_newlines(contents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_contents_have_no_lines() {
        assert!(split_file_contents("").is_empty());
        assert_eq!(split_file_contents("a\nb"), ["a\n", "b"]);
    }
}
