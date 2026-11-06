// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/hydratePartialDiff.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the `type: 'clone' | 'merge'` parameter is two functions
// (`hydrate_partial_diff_clone`, `hydrate_partial_diff_merge`) over one shared
// body. Upstream's `throw`s (the diff is not partial, the loaded files do not
// fit the diff's change type) are panics: the loader handed back files that
// contradict the diff it was asked to hydrate, which is a bug of the caller,
// not bad external text. Character counts are UTF-16 code units.

use crate::types::patch::ChangeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffLoadedFiles;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::types::patch::ParseDiffOptions;
use crate::types::patch::signed;
use crate::utf16;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_end_boundary;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;
use crate::utils::split_file_contents::split_file_contents;

struct HydratedHunksResult {
    hunks: Vec<Hunk>,
    split_line_count: usize,
    unified_line_count: usize,
}

/// Hydrates a partial diff with full file line arrays, preserving its hunks.
/// Pass the options used to parse the patch so derived cache keys distinguish
/// line-matching modes. Returns a hydrated copy and leaves `file_diff` alone.
pub fn hydrate_partial_diff_clone(
    file_diff: &FileDiffMetadata,
    files: &FileDiffLoadedFiles,
    parse_diff_options: &ParseDiffOptions,
) -> FileDiffMetadata {
    let mut target = file_diff.clone();
    hydrate_partial_diff_merge(&mut target, files, parse_diff_options);
    target
}

/// As [`hydrate_partial_diff_clone`], upgrading `file_diff` in place.
pub fn hydrate_partial_diff_merge(
    target_file_diff: &mut FileDiffMetadata,
    files: &FileDiffLoadedFiles,
    parse_diff_options: &ParseDiffOptions,
) {
    assert!(
        target_file_diff.is_partial,
        "hydratePartialDiff: fileDiff must be partial"
    );

    match target_file_diff.change_type {
        ChangeTypes::Change | ChangeTypes::RenameChanged => {
            let old_file = require_old_file(target_file_diff, files);
            let new_file = require_new_file(target_file_diff, files);
            hydrate_two_sided_file_diff(target_file_diff, old_file, new_file, parse_diff_options);
        }
        ChangeTypes::RenamePure => {
            let new_file = require_new_file(target_file_diff, files);
            require_missing_old_file(target_file_diff, files);
            let lines: Vec<String> = split_file_contents(&new_file.contents)
                .into_iter()
                .map(str::to_owned)
                .collect();
            target_file_diff.is_partial = false;
            target_file_diff.deletion_lines = lines.clone();
            target_file_diff.addition_lines = lines;
            target_file_diff.deletion_character_count = utf16::len(&new_file.contents);
            target_file_diff.addition_character_count = utf16::len(&new_file.contents);
            set_hydrated_cache_key(target_file_diff, None, Some(new_file), parse_diff_options);
        }
        ChangeTypes::New | ChangeTypes::Deleted => panic!(
            "hydratePartialDiff: {} diffs cannot be hydrated from loaded files",
            change_type_name(target_file_diff.change_type)
        ),
    }
}

fn change_type_name(change_type: ChangeTypes) -> &'static str {
    match change_type {
        ChangeTypes::Change => "change",
        ChangeTypes::RenamePure => "rename-pure",
        ChangeTypes::RenameChanged => "rename-changed",
        ChangeTypes::New => "new",
        ChangeTypes::Deleted => "deleted",
    }
}

fn hydrate_two_sided_file_diff(
    file_diff: &mut FileDiffMetadata,
    old_file: &FileContents,
    new_file: &FileContents,
    parse_diff_options: &ParseDiffOptions,
) {
    let deletion_lines: Vec<String> = split_file_contents(&old_file.contents)
        .into_iter()
        .map(str::to_owned)
        .collect();
    let addition_lines: Vec<String> = split_file_contents(&new_file.contents)
        .into_iter()
        .map(str::to_owned)
        .collect();
    let HydratedHunksResult {
        hunks,
        split_line_count,
        unified_line_count,
    } = hydrate_hunks(&file_diff.hunks, addition_lines.len());

    file_diff.hunks = hunks;
    file_diff.split_line_count = split_line_count;
    file_diff.unified_line_count = unified_line_count;
    file_diff.is_partial = false;
    file_diff.deletion_lines = deletion_lines;
    file_diff.addition_lines = addition_lines;
    file_diff.deletion_character_count = utf16::len(&old_file.contents);
    file_diff.addition_character_count = utf16::len(&new_file.contents);
    set_hydrated_cache_key(
        file_diff,
        Some(old_file),
        Some(new_file),
        parse_diff_options,
    );
}

fn hydrate_hunks(hunks: &[Hunk], total_addition_lines: usize) -> HydratedHunksResult {
    let mut split_line_count = 0;
    let mut unified_line_count = 0;
    let mut last_hunk_addition_end = 0;

    let mut hydrated_hunks: Vec<Hunk> = Vec::new();

    for hunk in hunks {
        let addition_line_index = signed(hunk.addition_start) - 1;
        let deletion_line_index = signed(hunk.deletion_start) - 1;
        let mut content_addition_line_index = addition_line_index;
        let mut content_deletion_line_index = deletion_line_index;
        let mut hunk_addition_lines = 0;
        let mut hunk_deletion_lines = 0;
        let mut hunk_split_line_count = 0;
        let mut hunk_unified_line_count = 0;
        let mut hunk_content: Vec<HunkContent> = Vec::new();

        for content in &hunk.hunk_content {
            match content {
                HunkContent::Context(context) => {
                    let mut context = context.clone();
                    context.addition_line_index = content_addition_line_index;
                    context.deletion_line_index = content_deletion_line_index;
                    content_addition_line_index += signed(context.lines);
                    content_deletion_line_index += signed(context.lines);
                    hunk_split_line_count += context.lines;
                    hunk_unified_line_count += context.lines;
                    hunk_content.push(HunkContent::Context(context));
                }
                HunkContent::Change(change) => {
                    let mut change = change.clone();
                    change.addition_line_index = content_addition_line_index;
                    change.deletion_line_index = content_deletion_line_index;
                    content_addition_line_index += signed(change.additions);
                    content_deletion_line_index += signed(change.deletions);
                    hunk_addition_lines += change.additions;
                    hunk_deletion_lines += change.deletions;
                    hunk_split_line_count += change.additions.max(change.deletions);
                    hunk_unified_line_count += change.additions + change.deletions;
                    hunk_content.push(HunkContent::Change(change));
                }
            }
        }

        let collapsed_before =
            get_hunk_side_start_boundary(hunk.addition_start, hunk.addition_count)
                .saturating_sub(last_hunk_addition_end);
        hydrated_hunks.push(Hunk {
            collapsed_before,
            addition_line_index,
            deletion_line_index,
            addition_lines: hunk_addition_lines,
            deletion_lines: hunk_deletion_lines,
            hunk_content,
            split_line_start: split_line_count + collapsed_before,
            unified_line_start: unified_line_count + collapsed_before,
            split_line_count: hunk_split_line_count,
            unified_line_count: hunk_unified_line_count,
            ..hunk.clone()
        });

        split_line_count += collapsed_before + hunk_split_line_count;
        unified_line_count += collapsed_before + hunk_unified_line_count;
        last_hunk_addition_end =
            get_hunk_side_end_boundary(hunk.addition_start, hunk.addition_count);
    }

    if let Some(last_hunk) = hydrated_hunks.last() {
        let last_hunk_end =
            get_hunk_side_end_boundary(last_hunk.addition_start, last_hunk.addition_count);
        let collapsed_after = total_addition_lines.saturating_sub(last_hunk_end);
        split_line_count += collapsed_after;
        unified_line_count += collapsed_after;
    }

    HydratedHunksResult {
        hunks: hydrated_hunks,
        split_line_count,
        unified_line_count,
    }
}

fn require_old_file<'files>(
    file_diff: &FileDiffMetadata,
    files: &'files FileDiffLoadedFiles,
) -> &'files FileContents {
    match files {
        FileDiffLoadedFiles::Changed { old_file, .. } => old_file,
        FileDiffLoadedFiles::PureRenamed { .. } => panic!(
            "hydratePartialDiff: {} diff for {} requires oldFile",
            change_type_name(file_diff.change_type),
            file_diff.name
        ),
    }
}

fn require_new_file<'files>(
    _file_diff: &FileDiffMetadata,
    files: &'files FileDiffLoadedFiles,
) -> &'files FileContents {
    match files {
        FileDiffLoadedFiles::Changed { new_file, .. }
        | FileDiffLoadedFiles::PureRenamed { new_file } => new_file,
    }
}

fn require_missing_old_file(file_diff: &FileDiffMetadata, files: &FileDiffLoadedFiles) {
    assert!(
        matches!(files, FileDiffLoadedFiles::PureRenamed { .. }),
        "hydratePartialDiff: {} diff for {} requires oldFile to be null",
        change_type_name(file_diff.change_type),
        file_diff.name
    );
}

fn get_hydrated_cache_key(
    file_diff: &FileDiffMetadata,
    old_file: Option<&FileContents>,
    new_file: Option<&FileContents>,
    parse_diff_options: &ParseDiffOptions,
) -> Option<String> {
    if let Some(cache_key) = &file_diff.cache_key {
        return Some(format!("{cache_key}:hydrated"));
    }
    get_loaded_file_cache_key(old_file, new_file, parse_diff_options)
}

fn set_hydrated_cache_key(
    file_diff: &mut FileDiffMetadata,
    old_file: Option<&FileContents>,
    new_file: Option<&FileContents>,
    parse_diff_options: &ParseDiffOptions,
) {
    file_diff.cache_key = get_hydrated_cache_key(file_diff, old_file, new_file, parse_diff_options);
}

fn get_loaded_file_cache_key(
    old_file: Option<&FileContents>,
    new_file: Option<&FileContents>,
    parse_diff_options: &ParseDiffOptions,
) -> Option<String> {
    if let (Some(old_file), Some(new_file)) = (old_file, new_file) {
        return match (&old_file.cache_key, &new_file.cache_key) {
            (Some(old_cache_key), Some(new_cache_key)) => Some(compose_cache_key(
                if parse_diff_options.disable_line_matching == Some(true) {
                    "hydrated-files-raw"
                } else {
                    "hydrated-files"
                },
                &[old_cache_key, new_cache_key],
            )),
            _ => None,
        };
    }
    old_file
        .and_then(|file| file.cache_key.clone())
        .or_else(|| new_file.and_then(|file| file.cache_key.clone()))
}
