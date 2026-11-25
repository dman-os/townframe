// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/cloneFileDiffMetadata.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `FileDiffMetadata` and `Hunk` derive `Clone`, which is already the deep copy upstream writes by hand; these wrappers keep the upstream names and call sites.

use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;

pub fn clone_hunks(hunks: &[Hunk]) -> Vec<Hunk> {
    hunks.to_vec()
}

pub fn clone_file_diff_metadata(file_diff: &FileDiffMetadata) -> FileDiffMetadata {
    file_diff.clone()
}
