// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createContentColumn.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::Properties;
use crate::utils::render::hast_utils::create_hast_element;

pub fn create_content_column(children: Vec<ElementContent>, row_count: usize) -> Element {
    create_hast_element(
        "div",
        children,
        Properties::new()
            .with("data-content", "")
            .with("style", format!("grid-row: span {row_count}")),
    )
}
