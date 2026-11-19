// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/getOrCreateCodeNode.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: nodes are hast `Element`s: `code` is an optional existing element, the created one is returned, and `pre?.appendChild(code)` is the caller placing the returned element among the pre's children (an `Element` cannot be appended to a parent it does not own). `columnType` is `CodeColumnType`.

use crate::hast::Element;
use crate::hast::Properties;
use crate::types::lines::CodeColumnType;

#[derive(Debug, Clone, Default)]
pub struct CreateCodeNodeProps {
    pub code: Option<Element>,
    pub column_type: Option<CodeColumnType>,
    pub row_span: Option<usize>,
    pub container_size: bool,
}

pub fn get_or_create_code_node(props: CreateCodeNodeProps) -> Element {
    let CreateCodeNodeProps {
        code,
        column_type,
        row_span,
        container_size,
    } = props;
    let mut code = code.unwrap_or_else(|| {
        let mut properties = Properties::new();
        properties.set("data-code", "");
        if let Some(column_type) = column_type {
            properties.set(
                match column_type {
                    CodeColumnType::Additions => "data-additions",
                    CodeColumnType::Deletions => "data-deletions",
                    CodeColumnType::Unified => "data-unified",
                },
                "",
            );
        }
        Element::new("code", properties, Vec::new())
    });
    if let Some(row_span) = row_span {
        code.properties
            .set_style_property("grid-row", &format!("span {row_span}"));
    } else {
        code.properties.remove_style_property("grid-row");
    }
    if container_size {
        code.properties.set("data-container-size", "");
    } else {
        code.properties.remove("data-container-size");
    }
    code
}
