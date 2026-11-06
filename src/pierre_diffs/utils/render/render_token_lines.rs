// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/utils/renderTokenLines.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: not ported, with the reason: `useTokenTransformer` and
// `wrapTokenFragments` (editor caret mapping; edit mode is out of scope),
// `lazyLineAST` (setDeferredArrayItem; rows are built eagerly),
// `cacheHtmlStyles` (a WeakMap keyed by object identity), and numeric
// (absolute-offset) decoration positions with `lineOffsets` (nothing in the
// diff path produces them). The decoration wrapper tree is built with an
// owned stack of open wrapper elements that are attached to their parent when
// closed; upstream appends each wrapper to its parent when created, but
// nothing else is appended to the parent while a wrapper is open, so the
// resulting child order is identical. Columns are UTF-16 code units and are
// converted to byte offsets only to slice token text.

use std::collections::HashMap;

use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::Properties;
use crate::highlighter_types::ThemedToken;
use crate::js_text;
use crate::utf16;
use crate::utils::parse_diff_decorations::DecorationItem;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::hast_utils::create_text_node_element;
use crate::utils::render::process_line::SharedRenderState;
use crate::utils::render::process_line::process_line;
use crate::utils::render::token_style::token_style;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergeWhitespaces {
    Never,
    #[default]
    Always,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RenderTokenLinesOptions<'state, 'decorations> {
    pub state: Option<SharedRenderState<'state>>,
    pub merge_whitespaces: MergeWhitespaces,
    pub decorations: &'decorations [DecorationItem],
}

// from/to are columns within this line; order preserves outer-to-inner nesting.
struct LineDecorationRange<'decoration> {
    decoration: &'decoration DecorationItem,
    order: usize,
    from: usize,
    to: usize,
}

struct DecorationBoundary {
    position: usize,
    range: usize,
    start: bool,
}

/// Converts highlighted tokens to one HAST element per line for custom renderers.
/// Callers provide any line separators and enclosing pre/code elements.
pub fn render_token_lines(
    lines: &[Vec<ThemedToken>],
    options: RenderTokenLinesOptions<'_, '_>,
) -> Vec<ElementContent> {
    let RenderTokenLinesOptions {
        state,
        merge_whitespaces,
        decorations,
    } = options;
    let mut decorations_by_line: HashMap<usize, Vec<&DecorationItem>> = HashMap::new();
    for decoration in decorations {
        let Some(last_line) = lines.len().checked_sub(1) else {
            continue;
        };
        let last_line = decoration.end.line.min(last_line);
        for line in decoration.start.line..=last_line {
            decorations_by_line
                .entry(line)
                .or_default()
                .push(decoration);
        }
    }
    lines
        .iter()
        .enumerate()
        .map(|(line_index, tokens)| {
            render_line(
                tokens,
                line_index,
                state,
                merge_whitespaces,
                decorations_by_line.remove(&line_index),
            )
        })
        .collect()
}

fn render_line(
    tokens: &[ThemedToken],
    line_index: usize,
    state: Option<SharedRenderState<'_>>,
    merge_whitespaces: MergeWhitespaces,
    spans: Option<Vec<&DecorationItem>>,
) -> ElementContent {
    let normalized = if merge_whitespaces == MergeWhitespaces::Never {
        None
    } else {
        merge_whitespace_tokens(tokens)
    };
    let tokens = normalized.as_deref().unwrap_or(tokens);
    let mut line = create_hast_element(
        if state.is_none() { "span" } else { "div" },
        Vec::new(),
        if state.is_none() {
            Properties::new().with("class", "line")
        } else {
            Properties::new()
        },
    );
    match spans {
        None => {
            append_tokens(&mut line, tokens);
        }
        Some(spans) => {
            append_decorated_tokens(&mut line, tokens, spans, line_index);
        }
    }
    match state {
        None => line.into(),
        Some(state) => process_line(line, line_index + 1, state).into(),
    }
}

/// Folds whitespace-only tokens into the following token (undecorated
/// whitespace only); `None` when no token changed.
fn merge_whitespace_tokens(tokens: &[ThemedToken]) -> Option<Vec<ThemedToken>> {
    let mut normalized: Option<Vec<ThemedToken>> = None;
    let mut pending_whitespace = String::new();
    let mut whitespace_offset = 0;
    for (index, source) in tokens.iter().enumerate() {
        let mut token = source.clone();
        let decorated_whitespace = token.font_style.unwrap_or(0) & 12 != 0;
        if !decorated_whitespace
            && !token.content.is_empty()
            && token.content.chars().all(js_text::is_whitespace)
            && index + 1 < tokens.len()
        {
            normalized.get_or_insert_with(|| tokens[..index].to_vec());
            if pending_whitespace.is_empty() {
                whitespace_offset = token.offset;
            }
            pending_whitespace.push_str(&token.content);
            continue;
        }
        if !pending_whitespace.is_empty()
            && let Some(normalized) = normalized.as_mut()
        {
            if decorated_whitespace {
                normalized.push(ThemedToken {
                    content: std::mem::take(&mut pending_whitespace),
                    offset: whitespace_offset,
                    ..ThemedToken::default()
                });
            } else {
                token.content = format!(
                    "{}{}",
                    std::mem::take(&mut pending_whitespace),
                    token.content
                );
                token.offset = whitespace_offset;
            }
        }
        if let Some(normalized) = normalized.as_mut() {
            normalized.push(token);
        }
    }
    normalized
}

// Returns the line length in UTF-16 code units.
fn append_tokens(line: &mut Element, tokens: &[ThemedToken]) -> usize {
    let mut column = 0;
    for token in tokens {
        if token.content.is_empty() {
            continue;
        }
        line.children
            .push(create_token_span(token, &token_style(token), &token.content).into());
        column += utf16::len(&token.content);
    }
    column
}

fn create_token_span(token: &ThemedToken, style: &str, text: &str) -> Element {
    let mut properties = Properties::new();
    if let Some(html_attrs) = &token.html_attrs {
        for (name, value) in html_attrs {
            properties.set(name, value.as_str());
        }
    }
    if !style.is_empty() {
        properties.set("style", style);
    }
    create_hast_element(
        "span",
        vec![create_text_node_element(text).into()],
        properties,
    )
}

/// Open decoration wrappers over the line being built.
struct DecoratedLine<'decoration> {
    line: Element,
    /// `(range index, wrapper)`, outermost first.
    stack: Vec<(usize, Element)>,
    ranges: Vec<LineDecorationRange<'decoration>>,
    boundaries: Vec<DecorationBoundary>,
    boundary_index: usize,
    /// Range indexes in outer-to-inner `order`.
    active: Vec<usize>,
    line_length: usize,
}

impl DecoratedLine<'_> {
    fn push_to_parent(&mut self, node: Element) {
        match self.stack.last_mut() {
            Some((_, wrapper)) => wrapper.children.push(node.into()),
            None => self.line.children.push(node.into()),
        }
    }

    /// Closes wrappers above `depth`, attaching each to its parent.
    fn truncate_stack(&mut self, depth: usize) {
        while self.stack.len() > depth {
            let (_, wrapper) = self.stack.pop().expect("length checked");
            self.push_to_parent(wrapper);
        }
    }

    // Keep the same outer-to-inner order even when decorations cross each other.
    fn update_active(&mut self, range: usize, add: bool) {
        let order = self.ranges[range].order;
        let low = self
            .active
            .partition_point(|&other| self.ranges[other].order < order);
        if add {
            self.active.insert(low, range);
        } else if self.active.get(low) == Some(&range) {
            self.active.remove(low);
        }
    }

    // Reuse wrappers across tokens so a continuous range stays in one element.
    fn sync_parent(&mut self) {
        let mut depth = 0;
        for position in 0..self.active.len() {
            let range = self.active[position];
            if self.stack.get(depth).map(|(open, _)| *open) != Some(range) {
                self.truncate_stack(depth);
                let wrapper = create_hast_element(
                    "span",
                    Vec::new(),
                    properties_of(self.ranges[range].decoration),
                );
                self.stack.push((range, wrapper));
            }
            depth += 1;
        }
        self.truncate_stack(depth);
    }

    // Markers belong inside ranges starting here, except at the line end,
    // where they belong inside ranges ending here.
    fn advance_boundary(&mut self, position: usize) {
        let first = self.boundary_index;
        while self
            .boundaries
            .get(self.boundary_index)
            .is_some_and(|boundary| boundary.position == position)
        {
            self.boundary_index += 1;
        }
        let mut has_marker = false;
        for index in first..self.boundary_index {
            let (range, start) = (self.boundaries[index].range, self.boundaries[index].start);
            let (from, to) = (self.ranges[range].from, self.ranges[range].to);
            if !start && from != to && position < self.line_length {
                self.update_active(range, false);
            }
        }
        for index in first..self.boundary_index {
            let (range, start) = (self.boundaries[index].range, self.boundaries[index].start);
            let (from, to) = (self.ranges[range].from, self.ranges[range].to);
            if start
                && from <= position
                && to >= position
                && (to > position || from == position || position == self.line_length)
            {
                self.update_active(range, true);
                if from == to {
                    has_marker = true;
                }
            }
        }
        if has_marker {
            self.sync_parent();
            for index in first..self.boundary_index {
                let (range, start) = (self.boundaries[index].range, self.boundaries[index].start);
                if start && self.ranges[range].from == self.ranges[range].to {
                    self.update_active(range, false);
                }
            }
        }
        if position < self.line_length {
            self.sync_parent();
        }
    }

    fn next_boundary_position(&self) -> Option<usize> {
        self.boundaries
            .get(self.boundary_index)
            .map(|boundary| boundary.position)
    }
}

fn properties_of(decoration: &DecorationItem) -> Properties {
    let mut properties = Properties::new();
    for (name, value) in &decoration.properties {
        properties.set(name, value.as_str());
    }
    properties
}

// Visit sorted boundaries once to avoid scanning every decoration per token.
// Emit empty ranges before text at the same position.
fn append_decorated_tokens(
    line: &mut Element,
    tokens: &[ThemedToken],
    mut spans: Vec<&DecorationItem>,
    line_index: usize,
) -> usize {
    // Open outer ranges first, regardless of their order in the input.
    if spans.len() > 1 {
        spans.sort_by(|first, second| {
            first
                .start
                .line
                .cmp(&second.start.line)
                .then(first.start.character.cmp(&second.start.character))
                .then(second.end.line.cmp(&first.end.line))
                .then(second.end.character.cmp(&first.end.character))
        });
    }
    let line_length: usize = tokens.iter().map(|token| utf16::len(&token.content)).sum();
    let mut ranges: Vec<LineDecorationRange<'_>> = Vec::new();
    let mut boundaries: Vec<DecorationBoundary> = Vec::new();
    for (order, decoration) in spans.iter().enumerate() {
        let from = if decoration.start.line == line_index {
            decoration.start.character
        } else {
            0
        };
        let to = if decoration.end.line == line_index {
            decoration.end.character
        } else {
            line_length
        };
        boundaries.push(DecorationBoundary {
            position: from.min(line_length),
            range: order,
            start: true,
        });
        boundaries.push(DecorationBoundary {
            position: to.min(line_length),
            range: order,
            start: false,
        });
        ranges.push(LineDecorationRange {
            decoration,
            order,
            from,
            to,
        });
    }
    boundaries.sort_by_key(|boundary| boundary.position);
    let mut decorated = DecoratedLine {
        line: std::mem::replace(
            line,
            create_hast_element("span", Vec::new(), Properties::new()),
        ),
        stack: Vec::new(),
        ranges,
        boundaries,
        boundary_index: 0,
        active: Vec::new(),
        line_length,
    };

    let mut column = 0;
    for token in tokens {
        if token.content.is_empty() {
            continue;
        }
        let start = column;
        let end = start + utf16::len(&token.content);
        let style = token_style(token);
        while column < end {
            if decorated.next_boundary_position() == Some(column) {
                decorated.advance_boundary(column);
            }
            let to = end.min(decorated.next_boundary_position().unwrap_or(end));
            let text = if column == start && to == end {
                token.content.as_str()
            } else {
                let from_byte = utf16::byte_offset(&token.content, column - start)
                    .expect("decoration boundaries fall on character boundaries");
                let to_byte = utf16::byte_offset(&token.content, to - start)
                    .expect("decoration boundaries fall on character boundaries");
                &token.content[from_byte..to_byte]
            };
            decorated.push_to_parent(create_token_span(token, &style, text));
            column = to;
        }
    }
    if decorated.next_boundary_position() == Some(column) {
        decorated.advance_boundary(column);
    }
    decorated.truncate_stack(0);
    *line = decorated.line;
    column
}
