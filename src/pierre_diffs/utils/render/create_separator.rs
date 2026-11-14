// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/createSeparator.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: none beyond the language (hast nodes are `crate::hast` types).

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::hast::Properties;
use crate::sprite::SvgSpriteName;
use crate::types::lines::ExpansionDirections;
use crate::types::options::HunkSeparators;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::hast_utils::create_icon_element;
use crate::utils::render::hast_utils::create_text_node_element;

#[derive(Debug, Clone, Default)]
pub struct CreateSeparatorProps {
    pub separator_type: Option<HunkSeparators>,
    pub content: Option<String>,
    pub expand_index: Option<usize>,
    pub chunked: bool,
    pub slot_name: Option<String>,
    pub is_first_hunk: bool,
    pub is_last_hunk: bool,
}

fn flag(present: bool) -> PropValue {
    if present {
        PropValue::Str(String::new())
    } else {
        PropValue::Undefined
    }
}

fn create_expand_button(direction: ExpansionDirections) -> Element {
    create_hast_element(
        "div",
        vec![
            create_icon_element(
                if direction == ExpansionDirections::Both {
                    SvgSpriteName::ExpandAll
                } else {
                    SvgSpriteName::Expand
                },
                16,
                16,
                &Properties::new().with("data-icon", ""),
            )
            .into(),
        ],
        Properties::new()
            .with("role", "button")
            .with("data-expand-button", "")
            .with(
                "data-expand-both",
                flag(direction == ExpansionDirections::Both),
            )
            .with("data-expand-up", flag(direction == ExpansionDirections::Up))
            .with(
                "data-expand-down",
                flag(direction == ExpansionDirections::Down),
            ),
    )
}

fn separator_type_name(separator_type: HunkSeparators) -> &'static str {
    match separator_type {
        HunkSeparators::Simple => "simple",
        HunkSeparators::Metadata => "metadata",
        HunkSeparators::LineInfo => "line-info",
        HunkSeparators::LineInfoBasic => "line-info-basic",
        HunkSeparators::Custom => "custom",
    }
}

pub fn create_separator(props: CreateSeparatorProps) -> Element {
    let CreateSeparatorProps {
        separator_type,
        content,
        expand_index,
        chunked,
        slot_name,
        is_first_hunk,
        is_last_hunk,
    } = props;
    let separator_type = separator_type.expect("createSeparator: type is required");
    let mut button_count = 0;
    let mut children: Vec<ElementContent> = Vec::new();
    if separator_type == HunkSeparators::Metadata
        && let Some(content) = &content
    {
        children.push(
            create_hast_element(
                "div",
                vec![create_text_node_element(content).into()],
                Properties::new().with("data-separator-wrapper", ""),
            )
            .into(),
        );
    }
    if matches!(
        separator_type,
        HunkSeparators::LineInfo | HunkSeparators::LineInfoBasic
    ) && let Some(content) = &content
    {
        let mut content_children: Vec<ElementContent> = Vec::new();
        if expand_index.is_some() {
            if !chunked {
                content_children.push(
                    create_expand_button(if !is_first_hunk && !is_last_hunk {
                        ExpansionDirections::Both
                    } else if is_first_hunk {
                        ExpansionDirections::Down
                    } else {
                        ExpansionDirections::Up
                    })
                    .into(),
                );
                button_count += 1;
            } else {
                if !is_first_hunk {
                    content_children.push(create_expand_button(ExpansionDirections::Up).into());
                    button_count += 1;
                }
                if !is_last_hunk {
                    content_children.push(create_expand_button(ExpansionDirections::Down).into());
                    button_count += 1;
                }
            }
        }
        content_children.push(
            create_hast_element(
                "div",
                vec![
                    create_hast_element(
                        "span",
                        vec![create_text_node_element(content).into()],
                        Properties::new().with("data-unmodified-lines", ""),
                    )
                    .into(),
                ],
                Properties::new().with("data-separator-content", ""),
            )
            .into(),
        );
        if chunked && expand_index.is_some() {
            content_children.push(
                create_hast_element(
                    "div",
                    vec![create_text_node_element("Expand all").into()],
                    Properties::new()
                        .with("role", "button")
                        .with("data-expand-button", "")
                        .with("data-expand-all-button", ""),
                )
                .into(),
            );
        }
        children.push(
            create_hast_element(
                "div",
                content_children,
                Properties::new()
                    .with("data-separator-wrapper", "")
                    .with("data-separator-multi-button", flag(button_count > 1)),
            )
            .into(),
        );
    }
    if separator_type == HunkSeparators::Custom
        && let Some(slot_name) = &slot_name
    {
        children.push(
            create_hast_element(
                "slot",
                Vec::new(),
                Properties::new().with("name", slot_name.as_str()),
            )
            .into(),
        );
    }
    let separator_name = if children.is_empty() {
        "simple"
    } else {
        separator_type_name(separator_type)
    };
    create_hast_element(
        "div",
        children,
        Properties::new()
            .with("data-separator", separator_name)
            .with("data-expand-index", expand_index)
            .with("data-separator-first", flag(is_first_hunk))
            .with("data-separator-last", flag(is_last_hunk)),
    )
}
