// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createAnnotationElement.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language.

use crate::hast::Element;
use crate::hast::Properties;
use crate::types::lines::AnnotationSpan;
use crate::utils::render::hast_utils::create_hast_element;

pub fn create_annotation_element(span: &AnnotationSpan) -> Element {
    create_hast_element(
        "div",
        vec![
            create_hast_element(
                "div",
                span.annotations
                    .iter()
                    .map(|slot_id| {
                        create_hast_element(
                            "slot",
                            Vec::new(),
                            Properties::new().with("name", slot_id.as_str()),
                        )
                        .into()
                    })
                    .collect(),
                Properties::new().with("data-annotation-content", ""),
            )
            .into(),
        ],
        Properties::new().with(
            "data-line-annotation",
            format!("{},{}", span.hunk_index, span.line_index),
        ),
    )
}
