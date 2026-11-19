// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getDiffFileInput.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `oldFile?: FileContents | null` distinguishes `undefined` (not passed) from `null` (intentionally missing): `Option<Option<FileContents>>`, `None` being `undefined`. The two `throw`s are panics (a caller passing neither side, or omitting one, is a programming error), with upstream's messages.

use crate::types::patch::DiffFileInput;
use crate::types::patch::FileContents;

pub fn get_diff_file_input(
    old_file: Option<Option<FileContents>>,
    new_file: Option<Option<FileContents>>,
    context: &str,
) -> Option<DiffFileInput> {
    let (old_file, new_file) = match (old_file, new_file) {
        (None, None) => return None,
        (Some(old_file), Some(new_file)) => (old_file, new_file),
        (None, Some(_)) | (Some(_), None) => {
            panic!("{context}: Pass null for an intentionally missing oldFile or newFile side")
        }
    };
    match (old_file, new_file) {
        (None, None) => panic!("{context}: You must pass oldFile, newFile, or both"),
        (None, Some(new_file)) => Some(DiffFileInput::Added { new_file }),
        (Some(old_file), None) => Some(DiffFileInput::Deleted { old_file }),
        (Some(old_file), Some(new_file)) => Some(DiffFileInput::Both { old_file, new_file }),
    }
}
