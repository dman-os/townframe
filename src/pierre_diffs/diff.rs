// Replaces jsdiff 9.0.0 (`diff` on npm) in the @pierre/diffs 1.6.4 port. Pierre
// calls exactly four entry points of it: `diffLines`, `diffWordsWithSpace`,
// `diffChars` (all returning `ChangeObject[]`) and `createTwoFilesPatch`
// (`parseDiffFromFile.ts`, `renderDiffWithHighlighter.ts`). This module keeps
// that call shape and data flow so Pierre's callers port with the same
// architecture, but the diffing itself is the `similar` crate (Myers).
// Credit: shape follows jsdiff's `ChangeObject` and `createTwoFilesPatch`
// (BSD-3-Clause, Kevin Decker); no jsdiff code is copied.
// Divergences (recorded in PARITY.md): hunk boundaries and word/char runs may
// differ from jsdiff's where several minimal diffs exist; word tokens are
// `similar`'s (maximal non-whitespace runs and whitespace runs) rather than
// jsdiff's word/punctuation/whitespace-merging tokenizer; jsdiff's per-call
// options (`ignoreCase`, `ignoreWhitespace`, `oneChangePerToken`,
// `maxEditLength`, ...) are not offered because Pierre passes none of them
// except `context`/`headerOptions` through `parseDiffOptions`.

use serde::Deserialize;
use serde::Serialize;
use similar::Algorithm;
use similar::ChangeTag;
use similar::DiffTag;
use similar::TextDiff;
use similar::capture_diff_slices;
use similar::group_diff_ops;

use crate::scanners::split_with_newlines;

/// One run of a diff result, with jsdiff's `ChangeObject` field names and JSON
/// shape: `count` tokens (lines, words or chars) that were added, removed or
/// kept, with their concatenated text in `value`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChangeObject {
    pub count: usize,
    pub added: bool,
    pub removed: bool,
    pub value: String,
}

fn change_objects<'text>(diff: &TextDiff<'text, 'text, str>) -> Vec<ChangeObject> {
    let mut runs: Vec<ChangeObject> = Vec::new();
    for change in diff.iter_all_changes() {
        let added = change.tag() == ChangeTag::Insert;
        let removed = change.tag() == ChangeTag::Delete;
        match runs.last_mut() {
            Some(last) if last.added == added && last.removed == removed => {
                last.count += 1;
                last.value.push_str(change.value());
            }
            _ => runs.push(ChangeObject {
                count: 1,
                added,
                removed,
                value: change.value().to_owned(),
            }),
        }
    }
    runs
}

/// Line tokens as jsdiff cuts them: split after `\n` only, terminators
/// attached. (`similar`'s own line splitter also breaks at a lone `\r`, which
/// jsdiff and Pierre's parsers do not, so lines are tokenised here and handed to
/// `similar` as slices.)
fn line_tokens(text: &str) -> Vec<&str> {
    if text.is_empty() {
        Vec::new()
    } else {
        split_with_newlines(text)
    }
}

/// `diffLines`: line tokens keep their terminator.
pub fn diff_lines(old_string: &str, new_string: &str) -> Vec<ChangeObject> {
    let old_lines = line_tokens(old_string);
    let new_lines = line_tokens(new_string);
    let ops = capture_diff_slices(Algorithm::Myers, &old_lines, &new_lines);
    let mut runs: Vec<ChangeObject> = Vec::new();
    for op in &ops {
        let (tag, old_range, new_range) = op.as_tag_tuple();
        let pieces: [(bool, bool, &[&str]); 2] = match tag {
            DiffTag::Equal => [(false, false, &old_lines[old_range]), (false, false, &[])],
            DiffTag::Delete => [(false, true, &old_lines[old_range]), (false, false, &[])],
            DiffTag::Insert => [(true, false, &new_lines[new_range]), (false, false, &[])],
            DiffTag::Replace => [
                (false, true, &old_lines[old_range]),
                (true, false, &new_lines[new_range]),
            ],
        };
        for (added, removed, tokens) in pieces {
            if tokens.is_empty() {
                continue;
            }
            let value = tokens.concat();
            match runs.last_mut() {
                Some(last) if last.added == added && last.removed == removed => {
                    last.count += tokens.len();
                    last.value.push_str(&value);
                }
                _ => runs.push(ChangeObject {
                    count: tokens.len(),
                    added,
                    removed,
                    value,
                }),
            }
        }
    }
    runs
}

/// `diffWordsWithSpace`: word and whitespace tokens, whitespace significant.
pub fn diff_words_with_space(old_string: &str, new_string: &str) -> Vec<ChangeObject> {
    change_objects(
        &TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_words(old_string, new_string),
    )
}

/// `diffChars`: one token per `char`.
pub fn diff_chars(old_string: &str, new_string: &str) -> Vec<ChangeObject> {
    change_objects(
        &TextDiff::configure()
            .algorithm(Algorithm::Myers)
            .diff_chars(old_string, new_string),
    )
}

/// Which header lines the patch carries (jsdiff's `headerOptions`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeaderOptions {
    pub include_index: bool,
    pub include_underline: bool,
    pub include_file_headers: bool,
}

/// `INCLUDE_HEADERS`, the default.
pub const INCLUDE_HEADERS: HeaderOptions = HeaderOptions {
    include_index: true,
    include_underline: true,
    include_file_headers: true,
};

/// The subset of jsdiff's patch options Pierre can reach through
/// `ParseDiffOptions`: `context` (default 4) and `headerOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PatchOptions {
    pub context: Option<usize>,
    pub header_options: Option<HeaderOptions>,
}

/// One side of a patch: the name, contents and optional header (the text after
/// the tab on the `---`/`+++` lines).
#[derive(Debug, Clone, Copy)]
pub struct PatchSide<'text> {
    pub file_name: &'text str,
    pub contents: &'text str,
    pub header: Option<&'text str>,
}

/// C-style quoting of file names that git and GNU diff would quote; names that
/// need none are returned as is.
fn quote_file_name_if_needed(name: &str) -> String {
    if name
        .chars()
        .all(|ch| ('\x20'..='\x7e').contains(&ch) && ch != '"' && ch != '\\')
    {
        return name.to_owned();
    }
    let mut quoted = String::from("\"");
    for byte in name.bytes() {
        match byte {
            0x07 => quoted.push_str("\\a"),
            0x08 => quoted.push_str("\\b"),
            0x09 => quoted.push_str("\\t"),
            0x0a => quoted.push_str("\\n"),
            0x0b => quoted.push_str("\\v"),
            0x0c => quoted.push_str("\\f"),
            0x0d => quoted.push_str("\\r"),
            b'"' => quoted.push_str("\\\""),
            b'\\' => quoted.push_str("\\\\"),
            0x20..=0x7e => quoted.push(char::from(byte)),
            _ => quoted.push_str(&format!("\\{byte:03o}")),
        }
    }
    quoted.push('"');
    quoted
}

fn hunk_range(start: usize, end: usize) -> String {
    // Unified diff quirk: an empty range begins at the line before it.
    let length = end - start;
    let first = if length == 0 { start } else { start + 1 };
    format!("{first},{length}")
}

/// `createTwoFilesPatch`: a unified diff with git-style quoted names and
/// jsdiff's header lines. Unlike `similar`'s own formatter, hunk headers
/// always carry both counts (`@@ -1,1 +1,2 @@`), as jsdiff's do.
pub fn create_two_files_patch(
    old_side: PatchSide<'_>,
    new_side: PatchSide<'_>,
    options: PatchOptions,
) -> String {
    let header_options = options.header_options.unwrap_or(INCLUDE_HEADERS);
    let mut lines: Vec<String> = Vec::new();
    if header_options.include_index && old_side.file_name == new_side.file_name {
        lines.push(format!("Index: {}", old_side.file_name));
    }
    if header_options.include_underline {
        lines.push("=".repeat(67));
    }
    if header_options.include_file_headers {
        let suffix = |header: Option<&str>| match header {
            Some(header) if !header.is_empty() => format!("\t{header}"),
            _ => String::new(),
        };
        lines.push(format!(
            "--- {}{}",
            quote_file_name_if_needed(old_side.file_name),
            suffix(old_side.header)
        ));
        lines.push(format!(
            "+++ {}{}",
            quote_file_name_if_needed(new_side.file_name),
            suffix(new_side.header)
        ));
    }

    let old_lines = line_tokens(old_side.contents);
    let new_lines = line_tokens(new_side.contents);
    let ops = capture_diff_slices(Algorithm::Myers, &old_lines, &new_lines);
    for hunk in group_diff_ops(ops, options.context.unwrap_or(4)) {
        let (first, last) = (hunk[0], hunk[hunk.len() - 1]);
        lines.push(format!(
            "@@ -{} +{} @@",
            hunk_range(first.old_range().start, last.old_range().end),
            hunk_range(first.new_range().start, last.new_range().end)
        ));
        for op in &hunk {
            let (tag, old_range, new_range) = op.as_tag_tuple();
            let sides: [(char, &[&str]); 2] = match tag {
                DiffTag::Equal => [(' ', &old_lines[old_range]), ('+', &[])],
                DiffTag::Delete => [('-', &old_lines[old_range]), ('+', &[])],
                DiffTag::Insert => [('+', &new_lines[new_range]), ('+', &[])],
                DiffTag::Replace => [('-', &old_lines[old_range]), ('+', &new_lines[new_range])],
            };
            for (sign, tokens) in sides {
                for token in tokens {
                    lines.push(format!(
                        "{sign}{}",
                        token.strip_suffix('\n').unwrap_or(token)
                    ));
                    if !token.ends_with('\n') {
                        lines.push(String::from("\\ No newline at end of file"));
                    }
                }
            }
        }
    }
    let mut text = lines.join("\n");
    text.push('\n');
    text
}

#[cfg(test)]
mod oracle_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn side<'text>(name: &'text str, contents: &'text str) -> PatchSide<'text> {
        PatchSide {
            file_name: name,
            contents,
            header: None,
        }
    }

    fn run(count: usize, added: bool, removed: bool, value: &str) -> ChangeObject {
        ChangeObject {
            count,
            added,
            removed,
            value: value.to_owned(),
        }
    }

    #[test]
    fn line_runs_carry_counts_and_terminators() {
        assert_eq!(
            diff_lines("a\nb\nc\n", "a\nx\ny\nc\n"),
            [
                run(1, false, false, "a\n"),
                run(1, false, true, "b\n"),
                run(2, true, false, "x\ny\n"),
                run(1, false, false, "c\n"),
            ]
        );
    }

    #[test]
    fn patch_has_jsdiff_headers_and_counts() {
        let patch = create_two_files_patch(
            side("a.txt", "one\ntwo\n"),
            side("a.txt", "one\n2\n"),
            PatchOptions::default(),
        );
        assert_eq!(
            patch,
            "Index: a.txt\n===================================================================\n--- a.txt\n+++ a.txt\n@@ -1,2 +1,2 @@\n one\n-two\n+2\n"
        );
    }

    #[test]
    fn missing_newline_is_marked_and_empty_ranges_use_the_previous_line() {
        let patch = create_two_files_patch(
            side("a", ""),
            side("b", "x"),
            PatchOptions {
                context: Some(0),
                header_options: Some(HeaderOptions {
                    include_index: false,
                    include_underline: false,
                    include_file_headers: false,
                }),
            },
        );
        assert_eq!(patch, "@@ -0,0 +1,1 @@\n+x\n\\ No newline at end of file\n");
    }

    #[test]
    fn names_needing_quotes_are_c_quoted() {
        assert_eq!(quote_file_name_if_needed("plain/a.txt"), "plain/a.txt");
        assert_eq!(
            quote_file_name_if_needed("a\"é\t"),
            "\"a\\\"\\303\\251\\t\""
        );
    }
}
