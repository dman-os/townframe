// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/hast_utils.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: hast nodes are `crate::hast` types; `createGutterItem`'s line type is a string (a `LineTypes` or one of `buffer`/`separator`/`annotation`); `createGutterGap`'s buffer type is `GutterBufferType`.

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::hast::Properties;
use crate::hast::Text;
use crate::sprite::SvgSpriteName;
use crate::types::lines::LineTypes;

pub fn create_text_node_element(value: &str) -> Text {
    Text::new(value)
}

pub fn create_hast_element(
    tag_name: &str,
    children: Vec<ElementContent>,
    properties: Properties,
) -> Element {
    Element::new(tag_name, properties, children)
}

pub fn create_icon_element(
    name: SvgSpriteName,
    width: i64,
    height: i64,
    properties: &Properties,
) -> Element {
    let mut icon_properties = Properties::new()
        .with("width", width)
        .with("height", height)
        .with("viewBox", "0 0 16 16");
    icon_properties.extend(properties);
    create_hast_element(
        "svg",
        vec![
            create_hast_element(
                "use",
                Vec::new(),
                Properties::new().with(
                    "href",
                    format!("#{}", name.as_str().trim_start_matches('#')),
                ),
            )
            .into(),
        ],
        icon_properties,
    )
}

/// Finds the first `code` element following first children, as upstream does.
pub fn find_code_element(node: &Element) -> Option<&Element> {
    let mut first_child = node.children.first();
    while let Some(child) = first_child {
        match child {
            ElementContent::Element(element) if element.tag_name == "code" => return Some(element),
            ElementContent::Element(element) => first_child = element.children.first(),
            ElementContent::Text(_) => return None,
        }
    }
    None
}

pub fn create_gutter_wrapper(children: Vec<ElementContent>) -> Element {
    create_hast_element("div", children, Properties::new().with("data-gutter", ""))
}

pub fn create_gutter_item(
    line_type: &str,
    line_number: usize,
    line_index: &str,
    properties: &Properties,
) -> Element {
    let mut item_properties = Properties::new()
        .with("data-line-type", line_type)
        .with("data-column-number", line_number)
        .with("data-line-index", line_index);
    item_properties.extend(properties);
    create_hast_element(
        "div",
        vec![
            create_hast_element(
                "span",
                vec![create_text_node_element(&line_number.to_string()).into()],
                Properties::new().with("data-line-number-content", ""),
            )
            .into(),
        ],
        item_properties,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GutterBufferType {
    Annotation,
    Buffer,
    Metadata,
}

pub fn create_gutter_gap(
    line_type: Option<LineTypes>,
    buffer_type: GutterBufferType,
    size: usize,
) -> Element {
    let (name, style) = match buffer_type {
        GutterBufferType::Annotation => ("annotation", format!("grid-row: span {size};")),
        GutterBufferType::Buffer => (
            "buffer",
            format!("grid-row: span {size};min-height:calc({size} * 1lh);"),
        ),
        GutterBufferType::Metadata => (
            "metadata",
            format!("grid-row: span {size};min-height:calc({size} * 1lh);"),
        ),
    };
    create_hast_element(
        "div",
        Vec::new(),
        Properties::new()
            .with("data-gutter-buffer", name)
            .with("data-buffer-size", size)
            .with(
                "data-line-type",
                if buffer_type == GutterBufferType::Annotation {
                    PropValue::Undefined
                } else {
                    line_type.map(LineTypes::as_str).into()
                },
            )
            .with("style", style),
    )
}
