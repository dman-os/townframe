// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): the twelve regular expressions
// of packages/diffs/src/constants.ts, as hand-written scanners.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: regexes become functions over `&str`. Positions are byte offsets
// (see `crate::utf16` for the offset policy; the oracle tests convert to
// UTF-16 units before comparing). Matches borrow from the input.
//
// JavaScript semantics reproduced here, all pinned by the oracle goldens:
// * Under the `m` flag `^` matches at index 0 and after any of the four line
//   terminators LF, CR, LS (U+2028) and PS (U+2029) -- so after a lone CR, and
//   between the CR and LF of a CRLF pair -- and `$` before any of them or at
//   the end. `.` excludes the same four characters.
// * `\s` is ECMAScript whitespace, not Rust's (`crate::js_text`).
// * `String.prototype.split` with a zero-width regex never splits at index 0
//   or at the end of the string, and never yields an empty first piece.
// * Without the `m` flag `^`/`$` anchor only at the string ends.
// * `\d` is ASCII `[0-9]` (no `u` flag), `/i` folds ASCII only.
//
// Not reproduced: the exported `g`-flag regexes (`GIT_DIFF_FILE_BREAK_REGEX`
// and friends) are stateful under `RegExp.prototype.test` because `lastIndex`
// persists between calls. Upstream calls `.test` on them in `_processPatch`
// and `_processFile`; every blob those calls see starts at a `diff --git`
// boundary (index 0), where the zero-width match leaves `lastIndex` at 0, so
// the statefulness is unobservable for split input. A direct caller of
// `processFile` with leading junk before `diff --git` could observe it;
// `has_git_diff_file_break` is the stateless equivalent.

use crate::js_text::is_line_terminator;
use crate::js_text::is_whitespace;
use crate::js_text::trim_start;

/// Splits `text` before every line start (index > 0, < len) at which
/// `matches_at` holds for the remaining text. This is `text.split(/(?=^X)/m)`
/// for a lookahead `X`.
fn split_before_line_starts(text: &str, matches_at: impl Fn(&str) -> bool) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut piece_start = 0;
    for (index, ch) in text.char_indices() {
        if !is_line_terminator(ch) {
            continue;
        }
        let line_start = index + ch.len_utf8();
        if line_start < text.len() && matches_at(&text[line_start..]) {
            pieces.push(&text[piece_start..line_start]);
            piece_start = line_start;
        }
    }
    pieces.push(&text[piece_start..]);
    pieces
}

/// Byte offsets of every `^` position under the `m` flag: 0 and each index
/// right after a line terminator (the end of the string included).
fn line_starts(text: &str) -> impl Iterator<Item = usize> + '_ {
    std::iter::once(0).chain(
        text.char_indices()
            .filter(|&(_, ch)| is_line_terminator(ch))
            .map(|(index, ch)| index + ch.len_utf8()),
    )
}

fn starts_with_unified_diff_file_header(rest: &str) -> bool {
    // `---\s+\S`
    rest.strip_prefix("---").is_some_and(|after| {
        let trimmed = trim_start(after);
        trimmed.len() < after.len() && !trimmed.is_empty()
    })
}

fn starts_with_commit_metadata(rest: &str) -> bool {
    // `From [a-f0-9]+ .+$` under `m`: the hex run is followed by a space, and
    // `.+$` needs at least one non-terminator character after that space.
    let Some(after_from) = rest.strip_prefix("From ") else {
        return false;
    };
    let hex_len = after_from
        .bytes()
        .take_while(|byte| matches!(byte, b'a'..=b'f' | b'0'..=b'9'))
        .count();
    if hex_len == 0 {
        return false;
    }
    after_from[hex_len..]
        .strip_prefix(' ')
        .and_then(|tail| tail.chars().next())
        .is_some_and(|first| !is_line_terminator(first))
}

/// `data.split(COMMIT_METADATA_SPLIT)` (`/(?=^From [a-f0-9]+ .+$)/m`).
pub fn split_commit_metadata(text: &str) -> Vec<&str> {
    split_before_line_starts(text, starts_with_commit_metadata)
}

/// `contents.split(GIT_DIFF_FILE_BREAK_REGEX)` (`/(?=^diff --git)/gm`).
pub fn split_git_diff_files(text: &str) -> Vec<&str> {
    split_before_line_starts(text, |rest| rest.starts_with("diff --git"))
}

/// `GIT_DIFF_FILE_BREAK_REGEX.test(text)` starting from `lastIndex` 0.
pub fn has_git_diff_file_break(text: &str) -> bool {
    line_starts(text).any(|start| text[start..].starts_with("diff --git"))
}

/// `contents.split(UNIFIED_DIFF_FILE_BREAK_REGEX)` (`/(?=^---\s+\S)/gm`).
pub fn split_unified_diff_files(text: &str) -> Vec<&str> {
    split_before_line_starts(text, starts_with_unified_diff_file_header)
}

/// `contents.split(FILE_CONTEXT_BLOB)` (`/(?=^@@ )/gm`).
pub fn split_file_context_blobs(text: &str) -> Vec<&str> {
    split_before_line_starts(text, |rest| rest.starts_with("@@ "))
}

/// `contents.split(SPLIT_WITH_NEWLINES)` (`/(?<=\n)/`): pieces keep their
/// trailing `\n`. Note `"".split(..)` is `[""]`, which is why upstream's
/// `splitFileContents` guards the empty string.
pub fn split_with_newlines(text: &str) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut piece_start = 0;
    for (index, ch) in text.char_indices() {
        let line_start = index + ch.len_utf8();
        if ch == '\n' && line_start < text.len() {
            pieces.push(&text[piece_start..line_start]);
            piece_start = line_start;
        }
    }
    pieces.push(&text[piece_start..]);
    pieces
}

/// A `HUNK_HEADER` match. Group numbers are those of
/// `/^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?: (.*))?/m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HunkHeaderMatch<'text> {
    /// Byte offset of the match in the searched text.
    pub index: usize,
    /// Byte length of the whole match (`match[0]`).
    pub length: usize,
    /// Group 1.
    pub old_start: &'text str,
    /// Group 2, absent when the header is `-N` with no count.
    pub old_count: Option<&'text str>,
    /// Group 3.
    pub new_start: &'text str,
    /// Group 4.
    pub new_count: Option<&'text str>,
    /// Group 5: the text after `@@ `, up to the end of the line.
    pub context: Option<&'text str>,
}

fn take_digits(text: &str) -> Option<(&str, &str)> {
    let len = text.bytes().take_while(u8::is_ascii_digit).count();
    (len > 0).then(|| text.split_at(len))
}

/// Digits, then an optional `,digits`.
fn take_range(text: &str) -> Option<(&str, Option<&str>, &str)> {
    let (start, rest) = take_digits(text)?;
    match rest.strip_prefix(',') {
        Some(after_comma) => match take_digits(after_comma) {
            Some((count, rest)) => Some((start, Some(count), rest)),
            // `(?:,(\d+))?` is skipped, leaving the comma where the caller
            // expects a space, so the overall match fails.
            None => Some((start, None, rest)),
        },
        None => Some((start, None, rest)),
    }
}

fn hunk_header_at(rest: &str) -> Option<HunkHeaderMatch<'_>> {
    let after_prefix = rest.strip_prefix("@@ -")?;
    let (old_start, old_count, after_old) = take_range(after_prefix)?;
    let after_plus = after_old.strip_prefix(" +")?;
    let (new_start, new_count, after_new) = take_range(after_plus)?;
    let after_marker = after_new.strip_prefix(" @@")?;
    let (context, tail) = match after_marker.strip_prefix(' ') {
        Some(after_space) => {
            // `(.*)` runs to the next line terminator.
            let end = after_space
                .char_indices()
                .find(|&(_, ch)| is_line_terminator(ch))
                .map_or(after_space.len(), |(index, _)| index);
            (Some(&after_space[..end]), &after_space[end..])
        }
        None => (None, after_marker),
    };
    Some(HunkHeaderMatch {
        index: 0,
        length: rest.len() - tail.len(),
        old_start,
        old_count,
        new_start,
        new_count,
        context,
    })
}

/// `text.match(HUNK_HEADER)`: the first hunk header at any line start of
/// `text` (the `m` flag), not only at the start of the string.
pub fn match_hunk_header(text: &str) -> Option<HunkHeaderMatch<'_>> {
    line_starts(text).find_map(|start| {
        hunk_header_at(&text[start..]).map(|found| HunkHeaderMatch {
            index: start,
            ..found
        })
    })
}

/// A `FILENAME_HEADER_REGEX` match: `/^(---|\+\+\+)\s+([^\t\r\n]+)/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilenameHeaderMatch<'text> {
    pub length: usize,
    /// Group 1: `---` or `+++`.
    pub marker: &'text str,
    /// Group 2.
    pub name: &'text str,
}

/// `text.match(FILENAME_HEADER_REGEX)` (anchored at the start of `text`; no
/// `m` flag).
///
/// `\s+` backtracks: for `"---   "` the name is the last space, because the
/// greedy whitespace run leaves nothing for `[^\t\r\n]+` and gives one
/// character back.
pub fn match_filename_header(text: &str) -> Option<FilenameHeaderMatch<'_>> {
    let marker = ["---", "+++"]
        .into_iter()
        .find(|marker| text.starts_with(marker))?;
    let after_marker = &text[marker.len()..];
    let whitespace_ends: Vec<usize> = after_marker
        .char_indices()
        .take_while(|&(_, ch)| is_whitespace(ch))
        .map(|(index, ch)| index + ch.len_utf8())
        .collect();
    // Longest whitespace run first, then give characters back one at a time.
    for &name_start in whitespace_ends.iter().rev() {
        let rest = &after_marker[name_start..];
        let name_len = rest.find(['\t', '\r', '\n']).unwrap_or(rest.len());
        if name_len > 0 {
            return Some(FilenameHeaderMatch {
                length: marker.len() + name_start + name_len,
                marker: &text[..marker.len()],
                name: &rest[..name_len],
            });
        }
    }
    None
}

/// An `INDEX_LINE_METADATA` match: `/^index ([0-9a-f]+)\.\.([0-9a-f]+)(?: (\d+))?$/i`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexLineMatch<'text> {
    pub old_id: &'text str,
    pub new_id: &'text str,
    /// Group 3, the file mode, when present.
    pub mode: Option<&'text str>,
}

/// `text.match(INDEX_LINE_METADATA)`: matches the whole string.
pub fn match_index_line(text: &str) -> Option<IndexLineMatch<'_>> {
    // `/i` folds ASCII only here (no `u` flag): "INDEX " matches.
    let head = text.get(..6)?;
    if !head.eq_ignore_ascii_case("index ") {
        return None;
    }
    let after_head = &text[6..];
    let hex_len = |rest: &str| rest.bytes().take_while(u8::is_ascii_hexdigit).count();
    let old_len = hex_len(after_head);
    if old_len == 0 {
        return None;
    }
    let after_old = after_head[old_len..].strip_prefix("..")?;
    let new_len = hex_len(after_old);
    if new_len == 0 {
        return None;
    }
    let tail = &after_old[new_len..];
    let mode = if tail.is_empty() {
        None
    } else {
        let digits = tail.strip_prefix(' ')?;
        let (digits, rest) = take_digits(digits)?;
        if !rest.is_empty() {
            return None;
        }
        Some(digits)
    };
    Some(IndexLineMatch {
        old_id: &after_head[..old_len],
        new_id: &after_old[..new_len],
        mode,
    })
}

/// `/^<marker>{7,}(?:\s.*)?$/` without the `m` flag: seven or more markers,
/// then the end of the text or one whitespace character followed by a single
/// line (no further terminators).
fn is_conflict_marker_with_label(text: &str, marker: char) -> bool {
    let marker_len = text.chars().take_while(|&ch| ch == marker).count();
    if marker_len < 7 {
        return false;
    }
    let tail = &text[marker_len * marker.len_utf8()..];
    let mut tail_chars = tail.chars();
    match tail_chars.next() {
        None => true,
        Some(first) => is_whitespace(first) && tail_chars.all(|ch| !is_line_terminator(ch)),
    }
}

/// `MERGE_CONFLICT_START_MARKER_REGEX.test(line)` (`/^<{7,}(?:\s.*)?$/`).
pub fn is_merge_conflict_start_marker(line: &str) -> bool {
    is_conflict_marker_with_label(line, '<')
}

/// `MERGE_CONFLICT_BASE_MARKER_REGEX.test(line)` (`/^\|{7,}(?:\s.*)?$/`).
pub fn is_merge_conflict_base_marker(line: &str) -> bool {
    is_conflict_marker_with_label(line, '|')
}

/// `MERGE_CONFLICT_SEPARATOR_MARKER_REGEX.test(line)` (`/^={7,}$/`).
pub fn is_merge_conflict_separator_marker(line: &str) -> bool {
    line.len() >= 7 && line.bytes().all(|byte| byte == b'=')
}

/// `MERGE_CONFLICT_END_MARKER_REGEX.test(line)` (`/^>{7,}(?:\s.*)?$/`).
pub fn is_merge_conflict_end_marker(line: &str) -> bool {
    is_conflict_marker_with_label(line, '>')
}

#[cfg(test)]
mod oracle_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_with_newlines_keeps_terminators() {
        assert_eq!(split_with_newlines("a\nb\n"), ["a\n", "b\n"]);
        assert_eq!(split_with_newlines("a\nb"), ["a\n", "b"]);
        assert_eq!(split_with_newlines("\n\n"), ["\n", "\n"]);
        assert_eq!(split_with_newlines(""), [""]);
    }

    #[test]
    fn lookahead_splits_never_split_at_zero_or_end() {
        assert_eq!(
            split_git_diff_files("diff --git a b\n"),
            ["diff --git a b\n"]
        );
        assert_eq!(
            split_git_diff_files("intro\ndiff --git a\ndiff --git b"),
            ["intro\n", "diff --git a\n", "diff --git b"]
        );
        assert_eq!(
            split_git_diff_files("x\r\u{2028}diff --git"),
            ["x\r\u{2028}", "diff --git"]
        );
    }

    #[test]
    fn unified_split_requires_whitespace_then_a_non_space() {
        // `---\n\ny` qualifies (`\s+` spans the newlines), a trailing `--- \n` does not.
        assert_eq!(
            split_unified_diff_files("a\n--- x\n---\n\ny\n--- \n"),
            ["a\n", "--- x\n", "---\n\ny\n--- \n"]
        );
    }

    #[test]
    fn commit_metadata_needs_hex_id_and_a_subject() {
        assert_eq!(
            split_commit_metadata("From abc123 Mon\nbody\nFrom deadbeef x\nmore"),
            ["From abc123 Mon\nbody\n", "From deadbeef x\nmore"]
        );
        // No subject text after the id: not a boundary.
        assert_eq!(
            split_commit_metadata("a\nFrom abc \nb"),
            ["a\nFrom abc \nb"]
        );
        // Uppercase hex is not accepted.
        assert_eq!(
            split_commit_metadata("a\nFrom ABC x\nb"),
            ["a\nFrom ABC x\nb"]
        );
    }

    #[test]
    fn hunk_header_groups() {
        let found = match_hunk_header("@@ -1,2 +30 @@ fn main()").expect("matches");
        assert_eq!(found.index, 0);
        assert_eq!(found.old_start, "1");
        assert_eq!(found.old_count, Some("2"));
        assert_eq!(found.new_start, "30");
        assert_eq!(found.new_count, None);
        assert_eq!(found.context, Some("fn main()"));
        assert_eq!(found.length, "@@ -1,2 +30 @@ fn main()".len());

        let later = match_hunk_header("x\n@@ -1 +1 @@\n").expect("m flag matches later lines");
        assert_eq!((later.index, later.context, later.length), (2, None, 11));
        assert_eq!(match_hunk_header("@@ -1,x +1 @@"), None);
        assert_eq!(match_hunk_header("  @@ -1 +1 @@"), None);
    }

    #[test]
    fn filename_header_backtracks_whitespace() {
        let found = match_filename_header("--- a/file.txt\t2020").expect("matches");
        assert_eq!(
            (found.marker, found.name, found.length),
            ("---", "a/file.txt", 14)
        );
        let found = match_filename_header("---   ").expect("one space given back");
        assert_eq!(found.name, " ");
        assert_eq!(match_filename_header("---\t"), None);
        assert_eq!(match_filename_header("---x"), None);
        assert_eq!(
            match_filename_header("+++\u{a0}b").map(|found| found.name),
            Some("b")
        );
    }

    #[test]
    fn index_line_is_case_insensitive_and_anchored() {
        assert_eq!(
            match_index_line("INDEX ab12..CD34 100644"),
            Some(IndexLineMatch {
                old_id: "ab12",
                new_id: "CD34",
                mode: Some("100644")
            })
        );
        assert_eq!(
            match_index_line("index ab..cd").map(|found| found.mode),
            Some(None)
        );
        assert_eq!(match_index_line("index ab..cd 12x"), None);
        assert_eq!(match_index_line("index ab..cd\n"), None);
        assert_eq!(match_index_line("index ..cd"), None);
    }

    #[test]
    fn merge_conflict_markers() {
        assert!(is_merge_conflict_start_marker("<<<<<<<"));
        assert!(is_merge_conflict_start_marker("<<<<<<< HEAD"));
        assert!(is_merge_conflict_start_marker("<<<<<<<\n"));
        assert!(!is_merge_conflict_start_marker("<<<<<<<\n\nx"));
        assert!(!is_merge_conflict_start_marker("<<<<<<"));
        assert!(!is_merge_conflict_start_marker("<<<<<<<x"));
        assert!(is_merge_conflict_base_marker("||||||| base"));
        assert!(is_merge_conflict_separator_marker("======="));
        assert!(!is_merge_conflict_separator_marker("======= "));
        assert!(is_merge_conflict_end_marker(">>>>>>>\u{a0}x"));
    }
}
