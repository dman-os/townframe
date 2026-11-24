// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/isDiffPlainText.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::types::patch::FileDiffMetadata;
use crate::utils::get_filetype_from_file_name::get_filetype_from_file_name;

pub fn is_diff_plain_text(diff: &FileDiffMetadata) -> bool {
    let computed_lang = diff
        .lang
        .clone()
        .unwrap_or_else(|| get_filetype_from_file_name(&diff.name));
    let computed_previous_lang = diff.lang.clone().unwrap_or_else(|| {
        diff.prev_name
            .as_deref()
            .map_or_else(|| String::from("text"), get_filetype_from_file_name)
    });
    computed_lang == "text" && computed_previous_lang == "text"
}
