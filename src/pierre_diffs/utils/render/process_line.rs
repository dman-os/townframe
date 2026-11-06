// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/processLine.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `state.lineInfo` is the array form only (the function form serves lazily built rows, which this port does not have). A missing entry is an invariant violation and panics, as the upstream `throw` does.

use crate::hast::Element;
use crate::hast::PropValue;
use crate::types::lines::LineIndex;
use crate::types::lines::LineInfo;
use crate::utils::render::hast_utils::create_text_node_element;

/// `SharedRenderState`, array form.
pub type SharedRenderState<'state> = &'state [Option<LineInfo>];

pub fn process_line(mut node: Element, line: usize, state: SharedRenderState<'_>) -> Element {
    let line_info = state
        .get(line - 1)
        .and_then(Option::as_ref)
        .unwrap_or_else(|| panic!("processLine: line {line}, contains no state.lineInfo"));
    // We need to convert the current line to a div but keep all the decorations
    // that may be applied
    "div".clone_into(&mut node.tag_name);
    node.properties.set("data-line", line_info.line_number);
    node.properties
        .set("data-alt-line", line_info.alt_line_number);
    node.properties
        .set("data-line-type", line_info.line_type.as_str());
    node.properties.set(
        "data-line-index",
        match line_info.line_index {
            LineIndex::Single(index) => PropValue::from(index),
            LineIndex::Pair(first, second) => PropValue::Str(format!("{first},{second}")),
        },
    );

    // NOTE(amadeus): We need to push newline characters into empty rows or else
    // copy/pasta will have issues
    if node.children.is_empty() {
        node.children.push(create_text_node_element("\n").into());
    }

    node
}
