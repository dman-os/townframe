// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getIconForType.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::sprite::SvgSpriteName;
use crate::types::patch::ChangeTypes;

/// `ChangeTypes | 'file'`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IconType {
    File,
    Change(ChangeTypes),
}

pub fn get_icon_for_type(icon_type: IconType) -> SvgSpriteName {
    match icon_type {
        IconType::File => SvgSpriteName::FileCode,
        IconType::Change(ChangeTypes::Change) => SvgSpriteName::SymbolModified,
        IconType::Change(ChangeTypes::New) => SvgSpriteName::SymbolAdded,
        IconType::Change(ChangeTypes::Deleted) => SvgSpriteName::SymbolDeleted,
        IconType::Change(ChangeTypes::RenamePure | ChangeTypes::RenameChanged) => {
            SvgSpriteName::SymbolMoved
        }
    }
}
