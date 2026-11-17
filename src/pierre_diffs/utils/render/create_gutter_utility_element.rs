// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createGutterUtilityElement.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language (`createIconElement` defaults are 16x16).

use crate::hast::Element;
use crate::hast::Properties;
use crate::sprite::SvgSpriteName;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::hast_utils::create_icon_element;

pub fn create_gutter_utility_element() -> Element {
    create_hast_element(
        "button",
        vec![
            create_icon_element(
                SvgSpriteName::Plus,
                16,
                16,
                &Properties::new().with("data-icon", ""),
            )
            .into(),
        ],
        Properties::new()
            .with("data-utility-button", "")
            .with("type", "button"),
    )
}
