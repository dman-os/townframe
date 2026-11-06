// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/test/renderTokenLines.test.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: Upstream serialises the hast to HTML and queries it through JSDOM; here the same questions (text content, which element carries a `data-range`, who is the parent) are asked of the hast tree directly, so `hastToHtml` and JSDOM are not involved. The third upstream test drives `useTokenTransformer` (editor token transformer, `data-char` tokens), which is not ported; see PARITY.md.

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::highlighter_types::ThemedToken;
use crate::utils::parse_diff_decorations::DecorationItem;
use crate::utils::parse_diff_decorations::DecorationPosition;
use crate::utils::render::render_token_lines::RenderTokenLinesOptions;
use crate::utils::render::render_token_lines::render_token_lines;

fn decoration(name: &str, start: usize, end: usize) -> DecorationItem {
    DecorationItem {
        start: DecorationPosition {
            line: 0,
            character: start,
        },
        end: DecorationPosition {
            line: 0,
            character: end,
        },
        properties: [(String::from("data-range"), name.to_owned())]
            .into_iter()
            .collect(),
        always_wrap: false,
    }
}

fn tokens(contents: &[&str]) -> Vec<ThemedToken> {
    let mut offset = 0;
    contents
        .iter()
        .map(|content| {
            let token = ThemedToken {
                content: (*content).to_owned(),
                offset,
                ..ThemedToken::default()
            };
            offset += content.len();
            token
        })
        .collect()
}

fn text(nodes: &[ElementContent]) -> String {
    nodes.iter().map(ElementContent::text_content).collect()
}

/// An element with its parent's `data-range` (if the parent carries one).
type Found<'tree> = (&'tree Element, Option<String>);

/// Every element with `data-range == name`, depth first.
fn find_ranges<'tree>(
    nodes: &'tree [ElementContent],
    name: &str,
    parent: Option<&'tree Element>,
    found: &mut Vec<Found<'tree>>,
) {
    for node in nodes {
        let Some(element) = node.as_element() else {
            continue;
        };
        if element.properties.get("data-range") == Some(&PropValue::Str(name.to_owned())) {
            let parent_range =
                parent.and_then(|parent| match parent.properties.get("data-range") {
                    Some(PropValue::Str(value)) => Some(value.clone()),
                    _ => None,
                });
            found.push((element, parent_range));
        }
        find_ranges(&element.children, name, Some(element), found);
    }
}

fn find<'tree>(nodes: &'tree [ElementContent], name: &str) -> Vec<Found<'tree>> {
    let mut found = Vec::new();
    find_ranges(nodes, name, None, &mut found);
    found
}

fn render(contents: &[&str], decorations: &[DecorationItem]) -> Vec<ElementContent> {
    render_token_lines(
        &[tokens(contents)],
        RenderTokenLinesOptions {
            decorations,
            ..RenderTokenLinesOptions::default()
        },
    )
}

fn children_text(element: &Element) -> String {
    text(&element.children)
}

#[test]
fn crossing_decorations_preserve_their_text_across_token_boundaries() {
    let nodes = render(
        &["ab", "cd", "ef", "gh"],
        &[decoration("a", 1, 5), decoration("b", 3, 7)],
    );
    assert_eq!(text(&nodes), "abcdefgh");
    let a = find(&nodes, "a");
    assert_eq!(children_text(a[0].0), "bcde");
    let b = find(&nodes, "b");
    let b_text: Vec<String> = b
        .iter()
        .map(|(element, _)| children_text(element))
        .collect();
    assert_eq!(b_text, ["de", "fg"]);
    assert_eq!(b[0].1.as_deref(), Some("a"));
}

#[test]
fn markers_join_the_following_range_at_shared_boundaries_and_the_last_range_at_eof() {
    let nodes = render(
        &["ab", "cd", "ef"],
        &[
            decoration("first", 0, 3),
            decoration("second", 3, 6),
            decoration("middle-marker", 3, 3),
            decoration("end-marker", 6, 6),
        ],
    );
    let first = find(&nodes, "first");
    assert_eq!(children_text(first[0].0), "abc");
    let second = find(&nodes, "second");
    assert_eq!(second.len(), 1);
    assert_eq!(children_text(second[0].0), "def");
    let middle = find(&second[0].0.children, "middle-marker");
    assert_eq!(children_text(middle[0].0), "");
    let end = find(&second[0].0.children, "end-marker");
    assert_eq!(children_text(end[0].0), "");
}
