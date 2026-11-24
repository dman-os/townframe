// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/areFilesEqual.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `FileContents | undefined` is `Option<&FileContents>`; `fileA?.x === fileB?.x` compares the `undefined` of an absent side with an absent field, exactly as `Option` comparison does.

use crate::types::patch::FileContents;

pub fn are_files_equal(file_a: Option<&FileContents>, file_b: Option<&FileContents>) -> bool {
    file_a.and_then(|file| file.cache_key.as_deref())
        == file_b.and_then(|file| file.cache_key.as_deref())
        && file_a.map(|file| file.contents.as_str()) == file_b.map(|file| file.contents.as_str())
        && file_a.map(|file| file.name.as_str()) == file_b.map(|file| file.name.as_str())
        && file_a.and_then(|file| file.lang.as_deref())
            == file_b.and_then(|file| file.lang.as_deref())
}
