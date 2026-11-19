// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createNoNewlineElement.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::hast::Element;
use crate::hast::Properties;
use crate::types::lines::LineTypes;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::hast_utils::create_text_node_element;

pub fn create_no_newline_element(line_type: LineTypes) -> Element {
    create_hast_element(
        "div",
        vec![
            create_hast_element(
                "span",
                vec![create_text_node_element("No newline at end of file").into()],
                Properties::new(),
            )
            .into(),
        ],
        Properties::new()
            .with("data-no-newline", "")
            .with("data-line-type", line_type.as_str())
            .with("data-column-content", ""),
    )
}
