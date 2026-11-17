// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/parseDiffFromFile.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: jsdiff's `createTwoFilesPatch` is `crate::diff::create_two_files_patch`
// (similar-backed, so hunk boundaries can differ from jsdiff's; see PARITY.md).
// "You must pass oldFile, newFile, or both" is unrepresentable: the inputs are
// a `DiffFileInput`, which always has at least one side. Errors are values
// (`ParsePatchError`) and recoverable problems are `Parsed::warnings`.

use crate::diff::PatchSide;
use crate::diff::create_two_files_patch;
use crate::types::patch::ChangeTypes;
use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParseDiffOptions;
use crate::utils::compose_cache_key::compose_cache_key;
use crate::utils::parse_patch_files::ParsePatchError;
use crate::utils::parse_patch_files::Parsed;
use crate::utils::parse_patch_files::ProcessFileOptions;
use crate::utils::parse_patch_files::process_file;

const MISSING_FILE_NAME: &str = "/dev/null";

fn create_missing_file() -> FileContents {
    FileContents {
        name: String::from(MISSING_FILE_NAME),
        contents: String::new(),
        lang: None,
        header: None,
        cache_key: None,
    }
}

/// Parses a diff from two file contents objects.
///
/// If both `oldFile` and `newFile` have a `cacheKey`, the resulting diff gets a
/// collision-safe key derived from both values and the line-matching mode.
pub fn parse_diff_from_file(
    input: &DiffFileInput,
    options: &ParseDiffOptions,
    throw_on_error: bool,
) -> Result<Parsed<FileDiffMetadata>, ParsePatchError> {
    let (old_file, new_file): (Option<&FileContents>, Option<&FileContents>) = match input {
        DiffFileInput::Both { old_file, new_file } => (Some(old_file), Some(new_file)),
        DiffFileInput::Added { new_file } => (None, Some(new_file)),
        DiffFileInput::Deleted { old_file } => (Some(old_file), None),
    };
    let disable_line_matching = options.disable_line_matching;

    let missing_old = create_missing_file();
    let missing_new = create_missing_file();
    let resolved_old_file = old_file.unwrap_or(&missing_old);
    let resolved_new_file = new_file.unwrap_or(&missing_new);
    let patch = create_two_files_patch(
        PatchSide {
            file_name: &resolved_old_file.name,
            contents: &resolved_old_file.contents,
            header: resolved_old_file.header.as_deref(),
        },
        PatchSide {
            file_name: &resolved_new_file.name,
            contents: &resolved_new_file.contents,
            header: resolved_new_file.header.as_deref(),
        },
        options.create_patch,
    );

    let cache_key = match (
        old_file.and_then(|file| file.cache_key.as_deref()),
        new_file.and_then(|file| file.cache_key.as_deref()),
    ) {
        (Some(old_cache_key), Some(new_cache_key)) => Some(compose_cache_key(
            if disable_line_matching == Some(true) {
                "diff-raw"
            } else {
                "diff"
            },
            &[old_cache_key, new_cache_key],
        )),
        _ => None,
    };
    let Parsed {
        value: file_data,
        warnings,
    } = process_file(
        &patch,
        ProcessFileOptions {
            cache_key,
            old_file: Some(resolved_old_file),
            new_file: Some(resolved_new_file),
            disable_line_matching,
            throw_on_error: Some(throw_on_error),
            is_git_diff: None,
        },
    )?;
    let Some(mut file_data) = file_data else {
        return Err(ParsePatchError {
            message: String::from(
                "parseDiffFrom: FileInvalid diff -- probably need to fix something -- if the files are the same maybe?",
            ),
        });
    };
    match input {
        DiffFileInput::Added { .. } => {
            file_data.change_type = ChangeTypes::New;
            file_data.prev_name = None;
        }
        DiffFileInput::Deleted { .. } => {
            file_data.change_type = ChangeTypes::Deleted;
            file_data.prev_name = None;
        }
        DiffFileInput::Both { .. } => {}
    }
    // If we've been provided an override for language in the newFile, let's pass
    // it through to FileDiffMetadata.
    let language = match input {
        DiffFileInput::Both { new_file, .. } | DiffFileInput::Added { new_file } => {
            new_file.lang.as_ref()
        }
        DiffFileInput::Deleted { old_file } => old_file.lang.as_ref(),
    };
    if let Some(language) = language {
        file_data.lang = Some(language.clone());
    }
    Ok(Parsed {
        value: file_data,
        warnings,
    })
}
