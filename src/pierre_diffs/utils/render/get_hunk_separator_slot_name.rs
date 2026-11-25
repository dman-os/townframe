// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getHunkSeparatorSlotName.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::types::lines::CodeColumnType;

pub fn get_hunk_separator_slot_name(column_type: CodeColumnType, hunk_index: usize) -> String {
    let name = match column_type {
        CodeColumnType::Unified => "unified",
        CodeColumnType::Additions => "additions",
        CodeColumnType::Deletions => "deletions",
    };
    format!("hunk-separator-{name}-{hunk_index}")
}
