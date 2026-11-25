// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/parsePatchFiles.ts
// (the patch/file/hunk splitting and hunk-header helpers).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: split into a child module of `parse_patch_files` only to stay inside
// the crate's module item budget; the code is otherwise the upstream helpers.

pub(super) struct ParsedHunkHeader {
    pub(super) addition_count: usize,
    pub(super) addition_start: usize,
    pub(super) deletion_count: usize,
    pub(super) deletion_start: usize,
    pub(super) hunk_context: Option<String>,
}

pub(super) fn split_git_diff_files(contents: &str) -> Vec<&str> {
    split_at_line_prefix(contents, "diff --git")
}

pub(super) fn split_unified_diff_files(contents: &str) -> Vec<&str> {
    if contents.is_empty() {
        return vec![""];
    }

    let mut parts: Vec<&str> = Vec::new();
    let mut part_start_index = 0;
    let mut line_start_index = 0;
    let mut remaining_deletion_lines = 0usize;
    let mut remaining_addition_lines = 0usize;
    let mut has_opened_unified_file = false;

    while line_start_index < contents.len() {
        let next_line_start_index = get_next_line_start_index(contents, line_start_index);
        if remaining_deletion_lines == 0 && remaining_addition_lines == 0 {
            if is_unified_diff_file_header_at(contents, line_start_index) {
                if line_start_index > part_start_index {
                    parts.push(&contents[part_start_index..line_start_index]);
                }
                part_start_index = line_start_index;
                has_opened_unified_file = true;
                line_start_index = get_next_line_start_index(contents, next_line_start_index);
                continue;
            }

            if has_opened_unified_file
                && contents[line_start_index..].starts_with("@@ -")
                && let Some(file_header) =
                    parse_hunk_header(&contents[line_start_index..next_line_start_index])
            {
                remaining_deletion_lines = file_header.deletion_count;
                remaining_addition_lines = file_header.addition_count;
            }
            line_start_index = next_line_start_index;
            continue;
        }

        match contents.as_bytes()[line_start_index] {
            b'\\' => {}
            b' ' => {
                remaining_deletion_lines = remaining_deletion_lines.saturating_sub(1);
                remaining_addition_lines = remaining_addition_lines.saturating_sub(1);
            }
            b'-' => remaining_deletion_lines = remaining_deletion_lines.saturating_sub(1),
            b'+' => remaining_addition_lines = remaining_addition_lines.saturating_sub(1),
            _ => {}
        }
        line_start_index = next_line_start_index;
    }

    parts.push(&contents[part_start_index..]);
    parts
}

pub(super) fn starts_with_unified_diff_file_header(contents: &str) -> bool {
    is_unified_diff_file_header_at(contents, 0)
}

pub(super) fn is_unified_diff_file_header_at(contents: &str, line_start_index: usize) -> bool {
    let next_line_start_index = get_next_line_start_index(contents, line_start_index);
    is_unified_diff_header_line_at(contents, line_start_index, "---")
        && is_unified_diff_header_line_at(contents, next_line_start_index, "+++")
}

pub(super) fn is_unified_diff_header_line_at(
    contents: &str,
    line_start_index: usize,
    prefix: &str,
) -> bool {
    let Some(rest) = contents.get(line_start_index..) else {
        return false;
    };
    let Some(after_prefix) = rest.strip_prefix(prefix) else {
        return false;
    };
    let mut chars = after_prefix.chars();
    if !matches!(chars.next(), Some(' ' | '\t')) {
        return false;
    }
    for ch in chars {
        if ch == '\n' || ch == '\r' {
            break;
        }
        if ch != ' ' && ch != '\t' {
            return true;
        }
    }
    false
}

pub(super) fn get_next_line_start_index(contents: &str, line_start_index: usize) -> usize {
    contents[line_start_index..]
        .find('\n')
        .map_or(contents.len(), |offset| line_start_index + offset + 1)
}

pub(super) fn parse_hunk_header(line: &str) -> Option<ParsedHunkHeader> {
    let rest = line.strip_prefix("@@ -")?;
    let (deletion_start, rest) = read_positive_integer(rest)?;
    let (deletion_count, rest) = match rest.strip_prefix(',') {
        Some(after_comma) => read_positive_integer(after_comma)?,
        None => (1, rest),
    };
    let rest = rest.strip_prefix(" +")?;
    let (addition_start, rest) = read_positive_integer(rest)?;
    let (addition_count, rest) = match rest.strip_prefix(',') {
        Some(after_comma) => read_positive_integer(after_comma)?,
        None => (1, rest),
    };
    let after_marker = rest.strip_prefix(" @@")?;
    let hunk_context = after_marker.strip_prefix(' ').map(trim_line_end);
    Some(ParsedHunkHeader {
        addition_count,
        addition_start,
        deletion_count,
        deletion_start,
        hunk_context,
    })
}

/// Reads ASCII digits; returns the value and the rest. `None` when there are
/// no digits (or the number overflows `usize`).
pub(super) fn read_positive_integer(value: &str) -> Option<(usize, &str)> {
    let digits = value.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let parsed = value[..digits].parse::<usize>().ok()?;
    Some((parsed, &value[digits..]))
}

pub(super) fn trim_line_end(value: &str) -> String {
    value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(value)
        .to_owned()
}

pub(super) fn split_at_line_prefix<'text>(contents: &'text str, prefix: &str) -> Vec<&'text str> {
    if contents.is_empty() {
        return vec![""];
    }

    let newline_prefix = format!("\n{prefix}");
    let first_boundary_index = if contents.starts_with(prefix) {
        Some(0)
    } else {
        find_line_prefix_index(contents, &newline_prefix, 0)
    };
    let Some(first_boundary_index) = first_boundary_index else {
        return vec![contents];
    };

    let mut parts = Vec::new();
    if first_boundary_index > 0 {
        parts.push(&contents[..first_boundary_index]);
    }

    let mut start_index = first_boundary_index;
    while let Some(next_boundary_index) =
        find_line_prefix_index(contents, &newline_prefix, start_index + 1)
    {
        parts.push(&contents[start_index..next_boundary_index]);
        start_index = next_boundary_index;
    }
    parts.push(&contents[start_index..]);
    parts
}

pub(super) fn find_line_prefix_index(
    contents: &str,
    newline_prefix: &str,
    from_index: usize,
) -> Option<usize> {
    contents
        .get(from_index..)?
        .find(newline_prefix)
        .map(|offset| from_index + offset + 1)
}
