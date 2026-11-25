// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getSingularPatch.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: the two `throw`s are `ParsePatchError`s (the patch is external
// input); the `console.error` dumps of the parsed value are dropped.

use crate::types::patch::FileDiffMetadata;
use crate::types::patch::ParsePatchOptions;
use crate::utils::parse_patch_files::ParsePatchError;
use crate::utils::parse_patch_files::parse_patch_files;

pub fn get_singular_patch(patch: &str) -> Result<FileDiffMetadata, ParsePatchError> {
    let mut parsed_patches = parse_patch_files(patch, &ParsePatchOptions::default())?.value;
    if parsed_patches.len() != 1 {
        return Err(ParsePatchError {
            message: String::from(
                "PatchDiff: Provided patch must include only 1 patch, with 1 diff",
            ),
        });
    }
    let mut files = parsed_patches.remove(0).files;
    if files.len() != 1 {
        return Err(ParsePatchError {
            message: String::from("FileDiff: Provided patch must contain exactly 1 file diff"),
        });
    }
    Ok(files.remove(0))
}
