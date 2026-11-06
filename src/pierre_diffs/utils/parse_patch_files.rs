// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/parsePatchFiles.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: patch text is external input, so failures are values. `throwOnError`
// keeps its meaning: with it set, the first problem is returned as
// `Err(ParsePatchError)`; without it, every spot where upstream calls
// `console.error` and recovers instead pushes a message onto
// `Parsed::warnings` and recovery proceeds identically. `detachString` and
// `releaseStringDetachBuffer` (V8 substring-retention workarounds) are dropped.
// The `GIT_DIFF_HEADER_FILENAMES` regex is `scanners::match_git_diff_header_filenames`;
// the lookaround regexes are the S1 scanners. Character counts are UTF-16 code
// units (`crate::utf16`). Integers in hunk headers that overflow `usize` make
// the header invalid where JavaScript would round to a double.

use crate::js_text;
use crate::scanners::has_git_diff_file_break;
use crate::scanners::match_filename_header;
use crate::scanners::match_git_diff_header_filenames;
use crate::scanners::match_index_line;
use crate::scanners::split_commit_metadata;
use crate::scanners::split_with_newlines;
use crate::types::patch::ChangeContent;
use crate::types::patch::ChangeTypes;
use crate::types::patch::ContextContent;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::types::patch::ParsePatchOptions;
use crate::types::patch::ParsedPatch;
use crate::utf16;
use crate::utils::clean_last_newline::clean_last_newline;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_end_boundary;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;
use crate::utils::parse_line_type::ParsedLineType;
use crate::utils::parse_quoted_diff_file_name::parse_quoted_diff_file_name;
use crate::utils::realign_change_content::realign_change_content_by_similarity;

use self::hunk_split::ParsedHunkHeader;
use self::hunk_split::parse_hunk_header;
use self::hunk_split::split_at_line_prefix;
use self::hunk_split::split_git_diff_files;
use self::hunk_split::split_unified_diff_files;
use self::hunk_split::starts_with_unified_diff_file_header;

/// The error `throwOnError` turns upstream's recoverable problems into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsePatchError {
    pub message: String,
}

impl std::fmt::Display for ParsePatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ParsePatchError {}

/// A parse result plus the recoverable problems upstream reports through
/// `console.error` (empty when `throwOnError` is set, since the first problem
/// is then an error instead).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed<Value> {
    pub value: Value,
    pub warnings: Vec<String>,
}

fn error(message: &str) -> ParsePatchError {
    ParsePatchError {
        message: message.to_owned(),
    }
}

pub fn process_patch(
    data: &str,
    options: &ParsePatchOptions,
) -> Result<Parsed<ParsedPatch>, ParsePatchError> {
    let mut warnings = Vec::new();
    let value = process_patch_inner(data, options, None, &mut warnings)?;
    Ok(Parsed { value, warnings })
}

fn process_patch_inner(
    data: &str,
    options: &ParsePatchOptions,
    patch_index: Option<usize>,
    warnings: &mut Vec<String>,
) -> Result<ParsedPatch, ParsePatchError> {
    let throw_on_error = options.throw_on_error.unwrap_or(false);
    let is_git_diff = is_git_diff_patch(data);
    let raw_files = if is_git_diff {
        split_git_diff_files(data)
    } else {
        split_unified_diff_files(data)
    };
    let cache_key_scope = if options.disable_line_matching == Some(true) {
        "patch-file-raw"
    } else {
        "patch-file"
    };
    let mut patch_metadata: Option<String> = None;
    let mut files: Vec<FileDiffMetadata> = Vec::new();
    for file_or_patch_metadata in raw_files {
        let is_metadata = if is_git_diff {
            !has_git_diff_file_break(file_or_patch_metadata)
        } else {
            !starts_with_unified_diff_file_header(file_or_patch_metadata)
        };
        if is_metadata {
            // If we get in here, it's most likely the introductory metadata from the
            // patch, or something is fucked with the diff format
            if patch_metadata.is_none() {
                patch_metadata = Some(file_or_patch_metadata.to_owned());
            } else if throw_on_error {
                return Err(error("parsePatchContent: unknown file blob"));
            } else {
                warnings.push(format!(
                    "parsePatchContent: unknown file blob: {file_or_patch_metadata}"
                ));
            }
            continue;
        }
        let cache_key = options.cache_key_prefix.as_deref().map(|prefix| {
            let index = files.len().to_string();
            match patch_index {
                None => compose_cache_key(cache_key_scope, &[prefix, &index]),
                Some(patch_index) => {
                    compose_cache_key(cache_key_scope, &[prefix, &patch_index.to_string(), &index])
                }
            }
        });
        let current_file = process_file_inner(
            file_or_patch_metadata,
            ProcessFileOptions {
                cache_key,
                is_git_diff: Some(is_git_diff),
                disable_line_matching: options.disable_line_matching,
                throw_on_error: Some(throw_on_error),
                ..ProcessFileOptions::default()
            },
            warnings,
        )?;
        if let Some(current_file) = current_file {
            files.push(current_file);
        }
    }
    Ok(ParsedPatch {
        patch_metadata,
        files,
    })
}

#[derive(Debug, Clone, Default)]
pub struct ProcessFileOptions<'files> {
    pub disable_line_matching: Option<bool>,
    pub cache_key: Option<String>,
    pub is_git_diff: Option<bool>,
    pub old_file: Option<&'files FileContents>,
    pub new_file: Option<&'files FileContents>,
    pub throw_on_error: Option<bool>,
}

pub fn process_file(
    file_diff_string: &str,
    options: ProcessFileOptions<'_>,
) -> Result<Parsed<Option<FileDiffMetadata>>, ParsePatchError> {
    let mut warnings = Vec::new();
    let value = process_file_inner(file_diff_string, options, &mut warnings)?;
    Ok(Parsed { value, warnings })
}

/// Indexes that run across the hunks of one file.
struct FileCursor {
    deletion_line_index: usize,
    addition_line_index: usize,
}

fn process_file_inner(
    file_diff_string: &str,
    options: ProcessFileOptions<'_>,
    warnings: &mut Vec<String>,
) -> Result<Option<FileDiffMetadata>, ParsePatchError> {
    let ProcessFileOptions {
        cache_key,
        disable_line_matching,
        is_git_diff,
        old_file,
        new_file,
        throw_on_error,
    } = options;
    let disable_line_matching = disable_line_matching.unwrap_or(false);
    let is_git_diff = is_git_diff.unwrap_or_else(|| has_git_diff_file_break(file_diff_string));
    let throw_on_error = throw_on_error.unwrap_or(false);

    let mut last_hunk_end = 0;
    let hunks = split_at_line_prefix(file_diff_string, "@@ ");
    let mut current_file: Option<FileDiffMetadata> = None;
    let is_partial = old_file.is_none() || new_file.is_none();
    let mut cursor = FileCursor {
        deletion_line_index: 0,
        addition_line_index: 0,
    };
    for hunk in hunks {
        let mut lines = split_with_newlines(hunk);
        let Some(&first_line) = lines.first() else {
            // `split_with_newlines` always yields a piece, so this mirrors an
            // unreachable upstream branch.
            if throw_on_error {
                return Err(error("parsePatchContent: invalid hunk"));
            }
            warnings.push(format!("parsePatchContent: invalid hunk {hunk}"));
            continue;
        };
        let file_header = parse_hunk_header(first_line);
        // Setup currentFile, this should be the first iteration of our hunks, and
        // technically not a hunk
        let (Some(file_header), Some(current_file)) = (file_header, current_file.as_mut()) else {
            if current_file.is_some() {
                if throw_on_error {
                    return Err(error("parsePatchContent: Invalid hunk"));
                }
                warnings.push(format!("parsePatchContent: Invalid hunk {hunk}"));
                continue;
            }
            let mut new_current =
                new_file_metadata(is_partial, old_file, new_file, cache_key.clone());
            process_file_header_lines(
                &lines,
                &mut new_current,
                is_git_diff,
                throw_on_error,
                warnings,
            )?;
            current_file = Some(new_current);
            continue;
        };

        // Strip trailing bare newlines (format-patch separators between commits)
        // if needed
        while lines
            .last()
            .is_some_and(|last| matches!(*last, "\n" | "\r" | "\r\n" | ""))
        {
            lines.pop();
        }
        if !is_partial {
            cursor.deletion_line_index = file_header.deletion_start.saturating_sub(1);
            cursor.addition_line_index = file_header.addition_start.saturating_sub(1);
        }

        let mut hunk_data = new_hunk(&file_header, first_line, &cursor);
        let parsed_counts = parse_hunk_lines(
            &lines,
            &mut hunk_data,
            current_file,
            &mut cursor,
            is_partial,
            throw_on_error,
            warnings,
        )?;
        repair_hunk_counts_if_needed(
            &mut hunk_data,
            parsed_counts,
            first_line,
            is_partial,
            throw_on_error,
            warnings,
        )?;
        push_hunk_geometry(current_file, hunk_data, &mut last_hunk_end);
    }
    let Some(mut current_file) = current_file else {
        return Ok(None);
    };
    if throw_on_error && is_partial && !is_git_diff && current_file.hunks.is_empty() {
        return Err(error("parsePatchContent: unified file has no hunks"));
    }
    finalize_file(
        &mut current_file,
        is_git_diff,
        is_partial,
        old_file,
        new_file,
        disable_line_matching,
    );
    Ok(Some(current_file))
}

fn new_hunk(file_header: &ParsedHunkHeader, first_line: &str, cursor: &FileCursor) -> Hunk {
    Hunk {
        collapsed_before: 0,

        split_line_count: 0,
        split_line_start: 0,

        unified_line_count: 0,
        unified_line_start: 0,

        addition_count: file_header.addition_count,
        addition_start: file_header.addition_start,
        addition_lines: 0,

        deletion_count: file_header.deletion_count,
        deletion_start: file_header.deletion_start,
        deletion_lines: 0,

        deletion_line_index: cursor.deletion_line_index,
        addition_line_index: cursor.addition_line_index,

        hunk_content: Vec::new(),
        hunk_context: file_header.hunk_context.clone(),
        hunk_specs: Some(first_line.to_owned()),

        no_eofcr_additions: false,
        no_eofcr_deletions: false,
    }
}

/// Processes each body line of one hunk (`lines[0]` is the `@@` line),
/// filling `hunk_data.hunk_content` and, for partial diffs, the file's
/// line arrays.
fn parse_hunk_lines(
    lines: &[&str],
    hunk_data: &mut Hunk,
    current_file: &mut FileDiffMetadata,
    cursor: &mut FileCursor,
    is_partial: bool,
    throw_on_error: bool,
    warnings: &mut Vec<String>,
) -> Result<(usize, usize), ParsePatchError> {
    let mut current_content: Option<usize> = None;
    let mut last_line_type: Option<ParsedLineType> = None;
    let mut parsed_addition_lines = 0;
    let mut parsed_deletion_lines = 0;
    for raw_line in lines.iter().skip(1).copied() {
        if parsed_addition_lines >= hunk_data.addition_count
            && parsed_deletion_lines >= hunk_data.deletion_count
            && !raw_line.starts_with('\\')
        {
            let is_unexpected_body_line =
                is_hunk_body_line(raw_line) && !is_format_patch_version_separator(raw_line);
            if !is_unexpected_body_line {
                break;
            }
            if throw_on_error {
                return Err(error(
                    "parsePatchContent: hunk has more lines than expected",
                ));
            }
        }

        let first_char = raw_line.chars().next();
        // If we can't properly process the line, well, lets just try to salvage
        // things and continue... It's possible an AI generated diff might have
        // some stray blank lines or something in there
        let Some(line_type) = first_char.and_then(parse_raw_line_type) else {
            if throw_on_error {
                return Err(error("parsePatchContent: invalid hunk line"));
            }
            warnings.push(format!(
                "parseLineType: Invalid firstChar: \"{}\", full line: \"{raw_line}\"",
                first_char.map_or_else(|| String::from("undefined"), String::from)
            ));
            warnings.push(format!("processFile: invalid rawLine: {raw_line}"));
            continue;
        };

        match line_type {
            ParsedLineType::Addition => {
                if throw_on_error && parsed_addition_lines >= hunk_data.addition_count {
                    return Err(error("parsePatchContent: hunk has too many addition lines"));
                }
                let line = get_parsed_line_content(raw_line);
                let index = change_group(hunk_data, &mut current_content, cursor);
                cursor.addition_line_index += 1;
                parsed_addition_lines += 1;
                if is_partial {
                    current_file.addition_character_count += utf16::len(&line);
                    current_file.addition_lines.push(line);
                }
                if let HunkContent::Change(change) = &mut hunk_data.hunk_content[index] {
                    change.additions += 1;
                }
                hunk_data.addition_lines += 1;
                last_line_type = Some(ParsedLineType::Addition);
            }
            ParsedLineType::Deletion => {
                if throw_on_error && parsed_deletion_lines >= hunk_data.deletion_count {
                    return Err(error("parsePatchContent: hunk has too many deletion lines"));
                }
                let line = get_parsed_line_content(raw_line);
                let index = change_group(hunk_data, &mut current_content, cursor);
                cursor.deletion_line_index += 1;
                parsed_deletion_lines += 1;
                if is_partial {
                    current_file.deletion_character_count += utf16::len(&line);
                    current_file.deletion_lines.push(line);
                }
                if let HunkContent::Change(change) = &mut hunk_data.hunk_content[index] {
                    change.deletions += 1;
                }
                hunk_data.deletion_lines += 1;
                last_line_type = Some(ParsedLineType::Deletion);
            }
            ParsedLineType::Context => {
                if throw_on_error
                    && (parsed_deletion_lines >= hunk_data.deletion_count
                        || parsed_addition_lines >= hunk_data.addition_count)
                {
                    return Err(error("parsePatchContent: hunk has too many context lines"));
                }
                let line = get_parsed_line_content(raw_line);
                let index = context_group(hunk_data, &mut current_content, cursor);
                cursor.addition_line_index += 1;
                cursor.deletion_line_index += 1;
                parsed_addition_lines += 1;
                parsed_deletion_lines += 1;
                if is_partial {
                    current_file.deletion_character_count += utf16::len(&line);
                    current_file.addition_character_count += utf16::len(&line);
                    current_file.deletion_lines.push(line.clone());
                    current_file.addition_lines.push(line);
                }
                if let HunkContent::Context(context) = &mut hunk_data.hunk_content[index] {
                    context.lines += 1;
                }
                last_line_type = Some(ParsedLineType::Context);
            }
            ParsedLineType::Metadata => {
                let Some(index) = current_content else {
                    continue;
                };
                apply_no_newline_marker(hunk_data, index, last_line_type, current_file, is_partial);
            }
        }
    }
    Ok((parsed_deletion_lines, parsed_addition_lines))
}

/// Returns the index of the current `context` group, opening a new one when
/// the previous group is not a context group.
fn context_group(
    hunk_data: &mut Hunk,
    current_content: &mut Option<usize>,
    cursor: &FileCursor,
) -> usize {
    if let Some(index) = *current_content
        && matches!(hunk_data.hunk_content[index], HunkContent::Context(_))
    {
        return index;
    }
    hunk_data
        .hunk_content
        .push(HunkContent::Context(ContextContent {
            lines: 0,
            addition_line_index: cursor.addition_line_index,
            deletion_line_index: cursor.deletion_line_index,
        }));
    let index = hunk_data.hunk_content.len() - 1;
    *current_content = Some(index);
    index
}

/// A `\ No newline at end of file` marker after `last_line_type`.
fn apply_no_newline_marker(
    hunk_data: &mut Hunk,
    content_index: usize,
    last_line_type: Option<ParsedLineType>,
    current_file: &mut FileDiffMetadata,
    is_partial: bool,
) {
    if matches!(
        hunk_data.hunk_content[content_index],
        HunkContent::Context(_)
    ) {
        hunk_data.no_eofcr_additions = true;
        hunk_data.no_eofcr_deletions = true;
    } else if last_line_type == Some(ParsedLineType::Deletion) {
        hunk_data.no_eofcr_deletions = true;
    } else if last_line_type == Some(ParsedLineType::Addition) {
        hunk_data.no_eofcr_additions = true;
    }
    // If we're dealing with partial content from a diff, we need to strip
    // newlines manually from the content
    if is_partial
        && matches!(
            last_line_type,
            Some(ParsedLineType::Addition | ParsedLineType::Context)
        )
    {
        strip_last_line_newline(
            &mut current_file.addition_lines,
            &mut current_file.addition_character_count,
        );
    }
    if is_partial
        && matches!(
            last_line_type,
            Some(ParsedLineType::Deletion | ParsedLineType::Context)
        )
    {
        strip_last_line_newline(
            &mut current_file.deletion_lines,
            &mut current_file.deletion_character_count,
        );
    }
}

/// When the parsed line counts differ from the header's, errors in strict mode
/// and otherwise repairs the hunk's start/count fields (and, for hydrated
/// diffs, its line indexes) to the parsed counts.
fn repair_hunk_counts_if_needed(
    hunk_data: &mut Hunk,
    (parsed_deletion_lines, parsed_addition_lines): (usize, usize),
    first_line: &str,
    is_partial: bool,
    throw_on_error: bool,
    warnings: &mut Vec<String>,
) -> Result<(), ParsePatchError> {
    if parsed_addition_lines == hunk_data.addition_count
        && parsed_deletion_lines == hunk_data.deletion_count
    {
        return Ok(());
    }
    if throw_on_error {
        return Err(error("parsePatchContent: hunk line count mismatch"));
    }
    warnings.push(format!(
        "parsePatchContent: hunk line count mismatch: \"{}\", declared old/new {}/{}, parsed old/new {parsed_deletion_lines}/{parsed_addition_lines}",
        first_line.trim_end(),
        hunk_data.deletion_count,
        hunk_data.addition_count,
    ));
    // Re-encode each original boundary using the repaired count. Zero-count
    // ranges use the boundary itself as their start; positive ranges use +1.
    let repaired_addition_start =
        get_hunk_side_start_boundary(hunk_data.addition_start, hunk_data.addition_count)
            + usize::from(parsed_addition_lines != 0);
    let repaired_deletion_start =
        get_hunk_side_start_boundary(hunk_data.deletion_start, hunk_data.deletion_count)
            + usize::from(parsed_deletion_lines != 0);

    // Hydrated hunks index into full-file line arrays, so their indexes must
    // move with repaired starts. Partial hunks index patch-built arrays.
    if !is_partial {
        let addition_start_delta =
            repaired_addition_start as isize - hunk_data.addition_start as isize;
        let deletion_start_delta =
            repaired_deletion_start as isize - hunk_data.deletion_start as isize;
        hunk_data.addition_line_index = hunk_data
            .addition_line_index
            .saturating_add_signed(addition_start_delta);
        hunk_data.deletion_line_index = hunk_data
            .deletion_line_index
            .saturating_add_signed(deletion_start_delta);
        for content in &mut hunk_data.hunk_content {
            match content {
                HunkContent::Context(context) => {
                    context.addition_line_index = context
                        .addition_line_index
                        .saturating_add_signed(addition_start_delta);
                    context.deletion_line_index = context
                        .deletion_line_index
                        .saturating_add_signed(deletion_start_delta);
                }
                HunkContent::Change(change) => {
                    change.addition_line_index = change
                        .addition_line_index
                        .saturating_add_signed(addition_start_delta);
                    change.deletion_line_index = change
                        .deletion_line_index
                        .saturating_add_signed(deletion_start_delta);
                }
            }
        }
    }

    hunk_data.addition_start = repaired_addition_start;
    hunk_data.deletion_start = repaired_deletion_start;
    hunk_data.addition_count = parsed_addition_lines;
    hunk_data.deletion_count = parsed_deletion_lines;
    Ok(())
}

/// Computes the hunk's split/unified geometry, chains it onto the file's
/// running totals and appends it.
fn push_hunk_geometry(
    current_file: &mut FileDiffMetadata,
    mut hunk_data: Hunk,
    last_hunk_end: &mut usize,
) {
    hunk_data.collapsed_before =
        get_hunk_side_start_boundary(hunk_data.addition_start, hunk_data.addition_count)
            .saturating_sub(*last_hunk_end);
    *last_hunk_end = get_hunk_side_end_boundary(hunk_data.addition_start, hunk_data.addition_count);
    for content in &hunk_data.hunk_content {
        match content {
            HunkContent::Context(context) => {
                hunk_data.split_line_count += context.lines;
                hunk_data.unified_line_count += context.lines;
            }
            HunkContent::Change(change) => {
                hunk_data.split_line_count += change.additions.max(change.deletions);
                hunk_data.unified_line_count += change.deletions + change.additions;
            }
        }
    }
    hunk_data.split_line_start = current_file.split_line_count + hunk_data.collapsed_before;
    hunk_data.unified_line_start = current_file.unified_line_count + hunk_data.collapsed_before;

    current_file.split_line_count += hunk_data.collapsed_before + hunk_data.split_line_count;
    current_file.unified_line_count += hunk_data.collapsed_before + hunk_data.unified_line_count;
    current_file.hunks.push(hunk_data);
}

/// Everything after the last hunk: trailing collapsed lines, the change type
/// of non-git patches, and similarity realignment.
fn finalize_file(
    current_file: &mut FileDiffMetadata,
    is_git_diff: bool,
    is_partial: bool,
    old_file: Option<&FileContents>,
    new_file: Option<&FileContents>,
    disable_line_matching: bool,
) {
    // Account for collapsed lines after the final hunk and increment the
    // split/unified counts properly
    if let Some(last_hunk) = current_file.hunks.last()
        && !is_partial
        && !current_file.addition_lines.is_empty()
        && !current_file.deletion_lines.is_empty()
    {
        let last_hunk_end =
            get_hunk_side_end_boundary(last_hunk.addition_start, last_hunk.addition_count);
        let total_file_lines = current_file.addition_lines.len();
        let collapsed_after = total_file_lines.saturating_sub(last_hunk_end);
        current_file.split_line_count += collapsed_after;
        current_file.unified_line_count += collapsed_after;
    }

    // If this isn't a git diff style patch, then we'll need to sus out some
    // additional metadata manually
    if !is_git_diff {
        let old_empty = old_file.is_none_or(|file| file.contents.is_empty());
        let new_empty = new_file.is_none_or(|file| file.contents.is_empty());
        if current_file
            .prev_name
            .as_ref()
            .is_some_and(|prev_name| &current_file.name != prev_name)
        {
            current_file.change_type = if current_file.hunks.is_empty() {
                ChangeTypes::RenamePure
            } else {
                ChangeTypes::RenameChanged
            };
        }
        // Sort of a hack for detecting deleted/added files...
        else if old_empty && new_file.is_some() && !new_empty {
            current_file.change_type = ChangeTypes::New;
        } else if old_file.is_some() && !old_empty && new_empty {
            current_file.change_type = ChangeTypes::Deleted;
        }
    }
    if !matches!(
        current_file.change_type,
        ChangeTypes::RenamePure | ChangeTypes::RenameChanged
    ) {
        current_file.prev_name = None;
    }
    if !disable_line_matching {
        // Partial diffs hold every line referenced by a change block, so they can
        // use the same similarity alignment as complete diffs.
        realign_change_content_by_similarity(current_file);
    }
}

fn new_file_metadata(
    is_partial: bool,
    old_file: Option<&FileContents>,
    new_file: Option<&FileContents>,
    cache_key: Option<String>,
) -> FileDiffMetadata {
    let mut addition_lines: Vec<String> = Vec::new();
    let mut deletion_lines: Vec<String> = Vec::new();
    let mut addition_character_count = 0;
    let mut deletion_character_count = 0;
    if let (false, Some(old_file), Some(new_file)) = (is_partial, old_file, new_file) {
        addition_lines = owned_lines(&new_file.contents);
        deletion_lines = owned_lines(&old_file.contents);
        addition_character_count = utf16::len(&new_file.contents);
        deletion_character_count = utf16::len(&old_file.contents);
        // If either file is technically empty, then we should empty the
        // arrays respectively
        if addition_lines.len() == 1 && new_file.contents.is_empty() {
            addition_lines.clear();
        }
        if deletion_lines.len() == 1 && old_file.contents.is_empty() {
            deletion_lines.clear();
        }
    }
    FileDiffMetadata {
        name: String::new(),
        prev_name: None,
        lang: None,
        new_object_id: None,
        prev_object_id: None,
        mode: None,
        prev_mode: None,
        change_type: ChangeTypes::Change,
        hunks: Vec::new(),
        split_line_count: 0,
        unified_line_count: 0,
        is_partial,
        deletion_lines,
        addition_lines,
        deletion_character_count,
        addition_character_count,
        edit_session_dirty: None,
        cache_key,
    }
}

/// The file's header lines (everything before the first `@@ ` hunk).
fn process_file_header_lines(
    lines: &[&str],
    current_file: &mut FileDiffMetadata,
    is_git_diff: bool,
    throw_on_error: bool,
    warnings: &mut Vec<String>,
) -> Result<(), ParsePatchError> {
    for line in lines.iter().copied() {
        if line.starts_with("diff --git") {
            let Some((raw_prev_name, raw_name)) =
                match_git_diff_header_filenames(js_text::trim(line))
            else {
                if throw_on_error {
                    return Err(error("parsePatchContent: invalid git diff header"));
                }
                warnings.push(format!("parsePatchContent: invalid git diff header {line}"));
                continue;
            };
            let prev_name = decode_diff_file_name(raw_prev_name, true);
            let name = decode_diff_file_name(raw_name, true);
            if prev_name != name {
                current_file.prev_name = Some(prev_name);
            }
            current_file.name = name;
            continue;
        }

        let filename_match = if line.starts_with("---") || line.starts_with("+++") {
            match_filename_header(line)
        } else {
            None
        };
        if let Some(filename_match) = filename_match {
            let file_name = decode_diff_file_name(filename_match.name, is_git_diff);
            if filename_match.marker == "---" && file_name != "/dev/null" {
                current_file.prev_name = Some(file_name.clone());
                current_file.name = file_name;
            } else if filename_match.marker == "+++" && file_name != "/dev/null" {
                current_file.name = file_name;
            }
        }
        // Git diffs have a bunch of additional metadata we can pull from
        else if is_git_diff {
            if let Some(rest) = line.strip_prefix("new mode ") {
                current_file.mode = Some(js_text::trim(rest).to_owned());
            }
            if let Some(rest) = line.strip_prefix("old mode ") {
                current_file.prev_mode = Some(js_text::trim(rest).to_owned());
            }
            if let Some(rest) = line.strip_prefix("new file mode") {
                current_file.change_type = ChangeTypes::New;
                current_file.mode = Some(js_text::trim(rest).to_owned());
            }
            if let Some(rest) = line.strip_prefix("deleted file mode") {
                current_file.change_type = ChangeTypes::Deleted;
                current_file.mode = Some(js_text::trim(rest).to_owned());
            }
            if line.starts_with("similarity index") {
                current_file.change_type = if line.starts_with("similarity index 100%") {
                    ChangeTypes::RenamePure
                } else {
                    ChangeTypes::RenameChanged
                };
            }
            if line.starts_with("index ")
                && let Some(index_match) = match_index_line(js_text::trim(line))
            {
                current_file.prev_object_id = Some(index_match.old_id.to_owned());
                current_file.new_object_id = Some(index_match.new_id.to_owned());
                if let Some(mode) = index_match.mode {
                    current_file.mode = Some(mode.to_owned());
                }
            }
            // Pure renames and copies have no --- / +++ headers. These paths
            // are already relative to the repository, without transport prefixes.
            if let Some(rest) = line
                .strip_prefix("rename from ")
                .or_else(|| line.strip_prefix("copy from "))
            {
                current_file.prev_name = Some(decode_diff_file_name(rest, false));
            }
            if let Some(rest) = line
                .strip_prefix("rename to ")
                .or_else(|| line.strip_prefix("copy to "))
            {
                current_file.name = decode_diff_file_name(rest, false);
            }
        }
    }
    Ok(())
}

fn owned_lines(contents: &str) -> Vec<String> {
    split_with_newlines(contents)
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// Returns the index of the current `change` group, opening a new one when the
/// previous group is not a change group.
fn change_group(
    hunk_data: &mut Hunk,
    current_content: &mut Option<usize>,
    cursor: &FileCursor,
) -> usize {
    if let Some(index) = *current_content
        && matches!(hunk_data.hunk_content[index], HunkContent::Change(_))
    {
        return index;
    }
    hunk_data
        .hunk_content
        .push(HunkContent::Change(ChangeContent {
            additions: 0,
            deletions: 0,
            addition_line_index: cursor.addition_line_index,
            deletion_line_index: cursor.deletion_line_index,
        }));
    let index = hunk_data.hunk_content.len() - 1;
    *current_content = Some(index);
    index
}

fn strip_last_line_newline(lines: &mut [String], character_count: &mut usize) {
    if let Some(line) = lines.last_mut() {
        let cleaned = clean_last_newline(line).to_owned();
        *character_count -= utf16::len(line) - utf16::len(&cleaned);
        *line = cleaned;
    }
}

/// Parses a patch file string into an array of parsed patches.
///
/// `data` supports multi-commit patches; see [`ParsePatchOptions`].
pub fn parse_patch_files(
    data: &str,
    options: &ParsePatchOptions,
) -> Result<Parsed<Vec<ParsedPatch>>, ParsePatchError> {
    // NOTE(amadeus): This function is pretty forgiving in that it can accept a
    // patch file that includes commit metdata, multiple commits, or not
    let mut warnings = Vec::new();
    let mut patches: Vec<ParsedPatch> = Vec::new();
    let raw_patches = if has_commit_metadata_boundary(data) {
        split_commit_metadata(data)
    } else {
        vec![data]
    };
    for patch in raw_patches {
        let patch_index = options.cache_key_prefix.as_ref().map(|_| patches.len());
        patches.push(process_patch_inner(
            patch,
            options,
            patch_index,
            &mut warnings,
        )?);
    }
    Ok(Parsed {
        value: patches,
        warnings,
    })
}

// Decode only quoted tokens, preserving whitespace inside them. Git's a/ and
// b/ prefixes are part of the quoted token and must be removed after decoding.
fn decode_diff_file_name(value: &str, strip_git_prefix: bool) -> String {
    let raw_name = js_text::trim(value);
    let name = if raw_name.starts_with('"') {
        parse_quoted_diff_file_name(raw_name)
            .map_or_else(|| raw_name.to_owned(), |parsed| parsed.file_name)
    } else {
        raw_name.to_owned()
    };
    if strip_git_prefix && (name.starts_with("a/") || name.starts_with("b/")) {
        name[2..].to_owned()
    } else {
        name
    }
}

fn has_commit_metadata_boundary(data: &str) -> bool {
    data.starts_with("From ") || data.contains("\nFrom ")
}

fn is_hunk_body_line(line: &str) -> bool {
    matches!(line.chars().next(), Some('+' | '-' | ' '))
}

fn is_format_patch_version_separator(line: &str) -> bool {
    line.strip_prefix("--").is_some_and(|rest| {
        rest.chars()
            .all(|ch| matches!(ch, ' ' | '\t' | '\n' | '\r'))
    })
}

fn is_git_diff_patch(data: &str) -> bool {
    data.starts_with("diff --git") || data.contains("\ndiff --git")
}

fn parse_raw_line_type(first_char: char) -> Option<ParsedLineType> {
    match first_char {
        ' ' => Some(ParsedLineType::Context),
        '\\' => Some(ParsedLineType::Metadata),
        '+' => Some(ParsedLineType::Addition),
        '-' => Some(ParsedLineType::Deletion),
        _ => None,
    }
}

fn get_parsed_line_content(raw_line: &str) -> String {
    let mut chars = raw_line.chars();
    chars.next();
    let processed_line = chars.as_str();
    if processed_line.is_empty() {
        String::from("\n")
    } else {
        processed_line.to_owned()
    }
}

mod hunk_split;
