// Part of the S12a-2b port of FileDiff.ts (see ../file_diff.rs for the header):
// applying a render to the code columns: `applyHunksToDOM`,
// `replaceCodeColumns`, `applyCodeColumnsInPlace`, `applyPartialRender`,
// `insertPartialHTML`, `renderPartialColumn`, `mergeBuffersIfNecessary`,
// `applyRowSpan`, `trimColumnRows`, `trimColumns`, `getBufferSize`,
// `updateBufferSize`, `getColumnPair` and `getCodeColumns`. A view over the
// instance (see `file_diff_view!`).
// Changes (besides the ones in the main header):
// * HTML strings (`innerHTML = renderPartialHTML(ast)`, `insertAdjacentHTML`)
//   are the AST's nodes created in the document; `afterbegin` inserts before the
//   first child and `beforeend` appends. Rows and children a column drops are
//   retired (see `Nodes::drop_node`).
// * `dataset.x` reads are `data-x` attribute reads; `'x' in dataset` is
//   attribute presence.
// * `getOrCreateCodeNode` runs over a document node: a new node is built from
//   the util's element, an existing one has the util applied to its property bag.
// * `ColumnElements | [ColumnElements?, ColumnElements?]` is `ColumnSet`; the
//   `Array.isArray` mismatches that throw upstream cannot be built. `trimColumns`
//   returns the removed row count (upstream's `-1` failure, which it never
//   returns, is a panic at the throw sites).
// * `shouldGuardRebuildScroll`/`guardWebKitScrollDuringRebuild` are editor and
//   Safari code and are dropped.
// * `console.error({ gutterElement, contentElement })` before a throw is the
//   panic message.

use std::rc::Rc;

use super::*;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::types::lines::CodeColumnType;
use crate::utils::dom::get_or_create_code_node::CreateCodeNodeProps;
use crate::utils::dom::get_or_create_code_node::get_or_create_code_node;

file_diff_view!(pub(crate) Columns);

#[derive(Clone, Copy)]
struct ColumnPair {
    gutter: NodeKey,
    content: NodeKey,
}

/// `ColumnElements | [ColumnElements | undefined, ColumnElements | undefined]`
#[derive(Clone, Copy)]
enum ColumnSet {
    Unified(ColumnPair),
    Split(Option<ColumnPair>, Option<ColumnPair>),
}

#[derive(Clone, Copy, PartialEq)]
enum InsertPosition {
    AfterBegin,
    BeforeEnd,
}

/// `getElementChildren`: the children of an element node, taken out of it.
fn element_children(node: Option<&mut ElementContent>) -> Option<Vec<ElementContent>> {
    match node? {
        ElementContent::Element(element) => Some(std::mem::take(&mut element.children)),
        ElementContent::Text(_) => None,
    }
}

/// `Number.parseInt(value, 10)`, `NaN` being `None`.
fn parse_int(value: &str) -> Option<i64> {
    let value = value.trim_start();
    let digits_start = usize::from(value.starts_with(['+', '-']));
    let end = value[digits_start..]
        .find(|character: char| !character.is_ascii_digit())
        .map_or(value.len(), |position| position + digits_start);
    value[..end].parse().ok()
}

impl<Host, Rendered, LAnnotation> Columns<'_, Host, Rendered, LAnnotation>
where
    Host: FileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    fn nodes_from(&self, contents: &[ElementContent]) -> Vec<NodeKey> {
        contents
            .iter()
            .map(|content| match content {
                ElementContent::Element(element) => self.document().create_from_hast(element),
                ElementContent::Text(text) => self.document().create_text(&text.value),
            })
            .collect()
    }

    fn has_attribute(&self, node: NodeKey, name: &str) -> bool {
        self.document().get_attribute(node, name).is_some()
    }

    fn attribute_is(&self, node: NodeKey, name: &str, value: &str) -> bool {
        self.document().get_attribute(node, name).as_deref() == Some(value)
    }

    fn get_or_create_code_node(
        &self,
        code: Option<NodeKey>,
        column_type: CodeColumnType,
        row_span: Option<usize>,
        container_size: bool,
    ) -> NodeKey {
        let props = |code: Option<hast::Element>| CreateCodeNodeProps {
            code,
            column_type: Some(column_type),
            row_span,
            container_size,
        };
        match code {
            None => self
                .document()
                .create_from_hast(&get_or_create_code_node(props(None))),
            Some(code) => {
                self.document().edit_properties(code, |properties| {
                    let element =
                        hast::Element::new("code", std::mem::take(properties), Vec::new());
                    *properties = get_or_create_code_node(props(Some(element))).properties;
                });
                code
            }
        }
    }

    pub(super) fn apply_hunks_to_dom(&self, pre: NodeKey, result: &HunksRenderResult) {
        self.replace_code_columns(pre, result);
    }

    // Renders a code column's AST into an existing elements without replacing
    // the gutter and content parents. Identity matters in edit mode: the content
    // element is the focused contenteditable, and replacing it ends the
    // browser's editing session — focus tears down and restores, and iOS
    // answers every session restart with an animated caret reveal.
    //
    // Returns false when the element has no column pair yet (initial render,
    // or a diff-style switch built a fresh element); the caller then assigns
    // the full innerHTML.
    fn apply_code_columns_in_place(
        &self,
        code: NodeKey,
        mut ast: Vec<ElementContent>,
        row_count: usize,
    ) -> bool {
        let Some(columns) = self.get_column_pair(Some(code)) else {
            return false;
        };
        let gutter_children = element_children(ast.get_mut(0));
        let content_children = element_children(ast.get_mut(1));
        let (Some(gutter_children), Some(content_children)) = (gutter_children, content_children)
        else {
            return false;
        };
        let nodes = Nodes(self.0);
        let gutter_nodes = self.nodes_from(&gutter_children);
        nodes.replace_children_retiring(columns.gutter, &gutter_nodes);
        let content_nodes = self.nodes_from(&content_children);
        nodes.replace_children_retiring(columns.content, &content_nodes);
        if Some(row_count) != self.state.borrow().last_row_count {
            let span = format!("span {row_count}");
            self.document()
                .set_style_property(columns.gutter, "grid-row", &span);
            self.document()
                .set_style_property(columns.content, "grid-row", &span);
        }
        true
    }

    /// One column's `getOrCreateCodeNode` plus content: in place when the
    /// column pair exists, else the code node's children are the AST.
    fn fill_column(
        &self,
        code: Option<NodeKey>,
        column_type: CodeColumnType,
        row_span: Option<usize>,
        container_size: bool,
        ast: Vec<ElementContent>,
        row_count: usize,
    ) -> NodeKey {
        let code = self.get_or_create_code_node(code, column_type, row_span, container_size);
        if !self.apply_code_columns_in_place(code, ast.clone(), row_count) {
            let nodes = self.nodes_from(&ast);
            Nodes(self.0).replace_children_retiring(code, &nodes);
        }
        code
    }

    fn replace_code_columns(&self, pre: NodeKey, result: &HunksRenderResult) {
        let (overflow, container_size) = {
            let options = self.options.borrow();
            (
                options.base.base.overflow.unwrap_or(Overflow::Scroll),
                matches!(
                    options.hunk_separators,
                    None | Some(FileDiffHunkSeparators::LineInfo)
                ),
            )
        };
        let row_span = (overflow == Overflow::Wrap).then_some(result.row_count);
        let nodes = Nodes(self.0);
        nodes.apply_pre_node_attributes(pre, result);

        let mut should_replace = false;
        // Create code elements and insert HTML content
        let mut code_elements: Vec<NodeKey> = Vec::new();
        let (unified_ast, deletions_ast, additions_ast) = {
            let renderer = self.hunks_renderer.borrow();
            (
                renderer.render_code_ast(CodeColumnType::Unified, result),
                renderer.render_code_ast(CodeColumnType::Deletions, result),
                renderer.render_code_ast(CodeColumnType::Additions, result),
            )
        };
        let (code_unified, code_deletions, code_additions) = {
            let state = self.state.borrow();
            (
                state.code_unified,
                state.code_deletions,
                state.code_additions,
            )
        };
        if let Some(unified_ast) = unified_ast {
            should_replace =
                code_unified.is_none() || code_additions.is_some() || code_deletions.is_some();

            // Clean up addition/deletion elements if necessary
            {
                let mut state = self.state.borrow_mut();
                state.code_deletions = None;
                state.code_additions = None;
            }
            nodes.drop_optional(code_deletions);
            nodes.drop_optional(code_additions);

            let unified = self.fill_column(
                code_unified,
                CodeColumnType::Unified,
                row_span,
                container_size,
                unified_ast,
                result.row_count,
            );
            self.state.borrow_mut().code_unified = Some(unified);
            code_elements.push(unified);
        } else if deletions_ast.is_some() || additions_ast.is_some() {
            if let Some(deletions_ast) = deletions_ast {
                should_replace = code_deletions.is_none() || code_unified.is_some();

                // Clean up unified column if necessary
                let unified = self.state.borrow_mut().code_unified.take();
                nodes.drop_optional(unified);

                let deletions = self.fill_column(
                    code_deletions,
                    CodeColumnType::Deletions,
                    row_span,
                    container_size,
                    deletions_ast,
                    result.row_count,
                );
                self.state.borrow_mut().code_deletions = Some(deletions);
                code_elements.push(deletions);
            } else {
                // If we have no deletion column, lets clean it up if it exists
                let deletions = self.state.borrow_mut().code_deletions.take();
                nodes.drop_optional(deletions);
            }

            if let Some(additions_ast) = additions_ast {
                should_replace =
                    should_replace || code_additions.is_none() || code_unified.is_some();

                // Clean up unified column if necessary
                let unified = self.state.borrow_mut().code_unified.take();
                nodes.drop_optional(unified);

                let additions = self.fill_column(
                    code_additions,
                    CodeColumnType::Additions,
                    row_span,
                    container_size,
                    additions_ast,
                    result.row_count,
                );
                self.state.borrow_mut().code_additions = Some(additions);
                code_elements.push(additions);
            } else {
                // If we have no addition column, lets clean it up if it exists
                let additions = self.state.borrow_mut().code_additions.take();
                nodes.drop_optional(additions);
            }
        } else {
            // if we get in here, there's no content to render, so lets just clean
            // everything up
            let columns = {
                let mut state = self.state.borrow_mut();
                [
                    state.code_unified.take(),
                    state.code_deletions.take(),
                    state.code_additions.take(),
                ]
            };
            for column in columns {
                nodes.drop_optional(column);
            }
        }

        if code_elements.is_empty() {
            // `pre.textContent = ''`
            nodes.replace_children_retiring(pre, &[]);
        } else if should_replace {
            nodes.replace_children_retiring(pre, &code_elements);
        }

        self.state.borrow_mut().last_row_count = Some(result.row_count);
    }

    pub(super) fn apply_partial_render(
        &self,
        file_diff: &Rc<FileDiffMetadata>,
        previous_render_range: Option<&RenderRange>,
        render_range: Option<&RenderRange>,
    ) -> bool {
        let diff_style = self
            .options
            .borrow()
            .base
            .diff_style
            .unwrap_or(DiffStyle::Split);
        let (pre, code_unified, code_additions, code_deletions, last_row_count) = {
            let state = self.state.borrow();
            (
                state.pre,
                state.code_unified,
                state.code_additions,
                state.code_deletions,
                state.last_row_count,
            )
        };
        let (Some(_), Some(previous_render_range), Some(render_range), Some(last_row_count)) = (
            pre,
            // We must have a current and previous render range to do a partial render
            previous_render_range,
            render_range,
            last_row_count,
        ) else {
            return false;
        };
        // Neither render range may be infinite
        if !previous_render_range.total_lines.is_finite() || !render_range.total_lines.is_finite() {
            return false;
        }
        let Some(code_elements) =
            self.get_code_columns(diff_style, code_unified, code_deletions, code_additions)
        else {
            return false;
        };

        let previous_start = previous_render_range.starting_line;
        let next_start = render_range.starting_line;
        let previous_end = previous_start + previous_render_range.total_lines;
        let next_end = next_start + render_range.total_lines;

        let overlap_start = previous_start.max(next_start);
        let overlap_end = previous_end.min(next_end);
        if overlap_end <= overlap_start {
            return false;
        }

        let trim_start = (overlap_start - previous_start).max(0.0);
        let trim_end = (previous_end - overlap_end).max(0.0);

        let trim_result = self.trim_columns(
            code_elements,
            [trim_start, trim_end],
            [previous_start, overlap_start, overlap_end],
        );
        assert!(
            last_row_count >= trim_result,
            "FileDiff.applyPartialRender: trimmed beyond DOM row count"
        );

        let mut row_count = last_row_count - trim_result;
        let render_chunk = |starting_line: f64, total_lines: f64| -> Option<HunksRenderResult> {
            if total_lines <= 0.0 {
                return None;
            }
            Some(self.hunks_renderer.borrow().render_diff(
                &*self.highlighter,
                file_diff,
                &RenderRange {
                    starting_line,
                    total_lines,
                    buffer_before: 0.0,
                    buffer_after: 0.0,
                },
            ))
        };

        let prepend_result = render_chunk(next_start, (overlap_start - next_start).max(0.0));
        if prepend_result.is_none() && next_start < overlap_start {
            return false;
        }

        let append_result = render_chunk(overlap_end, (next_end - overlap_end).max(0.0));
        if append_result.is_none() && next_end > overlap_end {
            return false;
        }

        let mut apply_chunk = |result: Option<HunksRenderResult>, position: InsertPosition| {
            let Some(result) = result else {
                return;
            };
            self.insert_partial_html(code_elements, &result, position);
            row_count += result.row_count;
            Lifecycle(self.0).adopt_render_data(Rc::clone(file_diff));
        };
        apply_chunk(prepend_result, InsertPosition::AfterBegin);
        apply_chunk(append_result, InsertPosition::BeforeEnd);

        if last_row_count != row_count {
            self.apply_row_span(code_elements, row_count);
            self.state.borrow_mut().last_row_count = Some(row_count);
        }

        true
    }

    fn insert_partial_html(
        &self,
        columns: ColumnSet,
        result: &HunksRenderResult,
        insert_position: InsertPosition,
    ) {
        let (unified_ast, deletions_ast, additions_ast) = {
            let renderer = self.hunks_renderer.borrow();
            (
                renderer.render_code_ast(CodeColumnType::Unified, result),
                renderer.render_code_ast(CodeColumnType::Deletions, result),
                renderer.render_code_ast(CodeColumnType::Additions, result),
            )
        };
        match columns {
            ColumnSet::Unified(column) => {
                self.render_partial_column(Some(column), unified_ast, insert_position);
            }
            ColumnSet::Split(deletions, additions) => {
                self.render_partial_column(deletions, deletions_ast, insert_position);
                self.render_partial_column(additions, additions_ast, insert_position);
            }
        }
    }

    fn render_partial_column(
        &self,
        column: Option<ColumnPair>,
        ast: Option<Vec<ElementContent>>,
        insert_position: InsertPosition,
    ) {
        let (Some(column), Some(mut ast)) = (column, ast) else {
            return;
        };
        let gutter_children = element_children(ast.get_mut(0));
        let content_children = element_children(ast.get_mut(1));
        let (Some(mut gutter_children), Some(mut content_children)) =
            (gutter_children, content_children)
        else {
            panic!("FileDiff.insertPartialHTML: Unexpected AST structure");
        };
        let buffer_size = |element: Option<&ElementContent>| match element {
            Some(ElementContent::Element(element)) => {
                match element.properties.get("data-buffer-size") {
                    Some(PropValue::Int(size)) => Some(*size as f64),
                    Some(PropValue::Float(size)) => Some(*size),
                    _ => None,
                }
            }
            _ => None,
        };
        let last_child = |node: NodeKey| self.document().children(node).last().copied();
        if insert_position == InsertPosition::BeforeEnd
            && let Some(size) = buffer_size(content_children.first())
        {
            self.merge_buffers_if_necessary(
                size,
                last_child(column.content),
                last_child(column.gutter),
                &mut gutter_children,
                &mut content_children,
                true,
            );
        }
        if insert_position == InsertPosition::AfterBegin
            && let Some(size) = buffer_size(content_children.last())
        {
            self.merge_buffers_if_necessary(
                size,
                self.document().children(column.content).first().copied(),
                self.document().children(column.gutter).first().copied(),
                &mut gutter_children,
                &mut content_children,
                false,
            );
        }

        for (parent, children) in [
            (column.gutter, &gutter_children),
            (column.content, &content_children),
        ] {
            let nodes = self.nodes_from(children);
            let first = self.document().child_nodes(parent).first().copied();
            for node in nodes {
                match insert_position {
                    InsertPosition::AfterBegin => {
                        self.document().insert_before(parent, node, first)
                    }
                    InsertPosition::BeforeEnd => self.document().append_child(parent, node),
                }
            }
        }
    }

    fn merge_buffers_if_necessary(
        &self,
        adjustment_size: f64,
        content_element: Option<NodeKey>,
        gutter_element: Option<NodeKey>,
        gutter_children: &mut Vec<ElementContent>,
        content_children: &mut Vec<ElementContent>,
        from_start: bool,
    ) {
        let (Some(content_element), Some(gutter_element)) = (content_element, gutter_element)
        else {
            return;
        };
        let Some(current_size) = self.get_buffer_size(content_element) else {
            return;
        };
        if from_start {
            gutter_children.remove(0);
            content_children.remove(0);
        } else {
            gutter_children.pop();
            content_children.pop();
        }
        let size = current_size as f64 + adjustment_size;
        self.update_buffer_size(content_element, size);
        self.update_buffer_size(gutter_element, size);
    }

    fn apply_row_span(&self, columns: ColumnSet, row_count: usize) {
        let apply_span = |column: Option<ColumnPair>| {
            let Some(column) = column else {
                return;
            };
            let span = format!("span {row_count}");
            self.document()
                .set_style_property(column.gutter, "grid-row", &span);
            self.document()
                .set_style_property(column.content, "grid-row", &span);
        };
        match columns {
            ColumnSet::Unified(column) => apply_span(Some(column)),
            ColumnSet::Split(deletions, additions) => {
                apply_span(deletions);
                apply_span(additions);
            }
        }
    }

    fn drop_row(&self, gutter_element: NodeKey, content_element: NodeKey) {
        let nodes = Nodes(self.0);
        nodes.drop_node(gutter_element);
        nodes.drop_node(content_element);
    }

    fn trim_column_rows(
        &self,
        columns: Option<ColumnPair>,
        mut pre_trim_count: i64,
        post_trim_start: i64,
    ) -> usize {
        let mut visible_line_index: i64 = 0;
        let mut row_count = 0;
        let mut row_index = 0;
        let mut pending_metadata_trim = false;
        let has_post_trim = post_trim_start >= 0;

        let Some(columns) = columns else {
            return 0;
        };
        let content_children = self.document().children(columns.content);
        let gutter_children = self.document().children(columns.gutter);
        assert!(
            content_children.len() == gutter_children.len(),
            "FileDiff.trimColumnRows: columns do not match"
        );

        while row_index < content_children.len() {
            if pre_trim_count <= 0 && !has_post_trim && !pending_metadata_trim {
                break;
            }
            let gutter_element = gutter_children[row_index];
            let content_element = content_children[row_index];
            row_index += 1;
            let should_trim = |pre_trim_count: i64, visible_line_index: i64| {
                pre_trim_count > 0 || (has_post_trim && visible_line_index >= post_trim_start)
            };
            let is_annotation_row =
                self.attribute_is(gutter_element, "data-gutter-buffer", "annotation")
                    && self.has_attribute(content_element, "data-line-annotation");
            let is_metadata_row =
                self.attribute_is(gutter_element, "data-gutter-buffer", "metadata")
                    && self.has_attribute(content_element, "data-no-newline");

            if pending_metadata_trim {
                pending_metadata_trim = false;
                if is_annotation_row || is_metadata_row {
                    self.drop_row(gutter_element, content_element);
                    row_count += 1;
                    continue;
                }
            }

            // If we found a line element, lets trim it if necessary
            if self.has_attribute(gutter_element, "data-line-index")
                && self.has_attribute(content_element, "data-line-index")
            {
                if should_trim(pre_trim_count, visible_line_index) {
                    self.drop_row(gutter_element, content_element);
                    if pre_trim_count > 0 {
                        pre_trim_count -= 1;
                        if pre_trim_count == 0 {
                            pending_metadata_trim = true;
                        }
                    }
                    row_count += 1;
                }
                visible_line_index += 1;
                continue;
            }

            // Separators should be removed, but don't count towards line indices
            if self.has_attribute(gutter_element, "data-separator")
                && self.has_attribute(content_element, "data-separator")
            {
                if should_trim(pre_trim_count, visible_line_index) {
                    self.drop_row(gutter_element, content_element);
                    row_count += 1;
                }
                continue;
            }

            // Annotations should be removed, but don't count towards line indices
            if is_annotation_row || is_metadata_row {
                if should_trim(pre_trim_count, visible_line_index) {
                    self.drop_row(gutter_element, content_element);
                    row_count += 1;
                }
                continue;
            }

            if self.attribute_is(gutter_element, "data-gutter-buffer", "buffer")
                && self.has_attribute(content_element, "data-content-buffer")
            {
                let total_rows = self
                    .get_buffer_size(content_element)
                    .expect("FileDiff.trimColumnRows: invalid element");
                if pre_trim_count > 0 {
                    let rows_to_remove = pre_trim_count.min(total_rows);
                    let new_size = total_rows - rows_to_remove;
                    if new_size > 0 {
                        self.update_buffer_size(gutter_element, new_size as f64);
                        self.update_buffer_size(content_element, new_size as f64);
                        row_count += rows_to_remove as usize;
                    } else {
                        self.drop_row(gutter_element, content_element);
                        row_count += total_rows as usize;
                    }
                    pre_trim_count -= rows_to_remove;
                    if pre_trim_count == 0 && new_size == 0 {
                        pending_metadata_trim = true;
                    }
                }
                // If we are in a post clip era...
                else if has_post_trim {
                    let buffer_start = visible_line_index;
                    let buffer_end = visible_line_index + total_rows - 1;
                    if post_trim_start <= buffer_start {
                        self.drop_row(gutter_element, content_element);
                        row_count += total_rows as usize;
                    } else if post_trim_start <= buffer_end {
                        let rows_to_remove = buffer_end - post_trim_start + 1;
                        let new_size = total_rows - rows_to_remove;
                        self.update_buffer_size(gutter_element, new_size as f64);
                        self.update_buffer_size(content_element, new_size as f64);
                        row_count += rows_to_remove as usize;
                    }
                }
                visible_line_index += total_rows;
                continue;
            }

            panic!("FileDiff.trimColumnRows: unknown row elements");
        }

        row_count
    }

    fn trim_columns(
        &self,
        columns: ColumnSet,
        [trim_start, trim_end]: [f64; 2],
        [previous_start, overlap_start, overlap_end]: [f64; 3],
    ) -> usize {
        let pre_trim_count = (overlap_start - previous_start).max(0.0) as i64;
        let post_trim_start = (overlap_end - previous_start) as i64;
        assert!(
            post_trim_start >= 0,
            "FileDiff.trimColumns: overlap ends before previous"
        );
        let should_trim_start = trim_start > 0.0;
        let should_trim_end = trim_end > 0.0;
        if !should_trim_start && !should_trim_end {
            return 0;
        }
        let effective_pre_trim_count = if should_trim_start { pre_trim_count } else { 0 };
        let effective_post_trim_start = if should_trim_end { post_trim_start } else { -1 };

        match columns {
            ColumnSet::Unified(column) => self.trim_column_rows(
                Some(column),
                effective_pre_trim_count,
                effective_post_trim_start,
            ),
            ColumnSet::Split(deletions, additions) => {
                let deletions_trim = self.trim_column_rows(
                    deletions,
                    effective_pre_trim_count,
                    effective_post_trim_start,
                );
                let additions_trim = self.trim_column_rows(
                    additions,
                    effective_pre_trim_count,
                    effective_post_trim_start,
                );
                // We should avoid the trim validation if we are split but
                // there's only one side
                assert!(
                    !(deletions.is_some()
                        && additions.is_some()
                        && deletions_trim != additions_trim),
                    "FileDiff.trimColumns: split columns out of sync"
                );
                if deletions.is_some() {
                    deletions_trim
                } else {
                    additions_trim
                }
            }
        }
    }

    fn get_buffer_size(&self, element: NodeKey) -> Option<i64> {
        parse_int(&self.document().get_attribute(element, "data-buffer-size")?)
    }

    fn update_buffer_size(&self, element: NodeKey, size: f64) {
        self.document()
            .set_attribute(element, "data-buffer-size", &size.to_string());
        self.document()
            .set_style_property(element, "grid-row", &format!("span {size}"));
        self.document()
            .set_style_property(element, "min-height", &format!("calc({size} * 1lh)"));
    }

    fn get_column_pair(&self, code: Option<NodeKey>) -> Option<ColumnPair> {
        let children = self.document().children(code?);
        let (gutter, content) = (*children.first()?, *children.get(1)?);
        if !self.has_attribute(gutter, "data-gutter")
            || !self.has_attribute(content, "data-content")
        {
            return None;
        }
        Some(ColumnPair { gutter, content })
    }

    fn get_code_columns(
        &self,
        diff_style: DiffStyle,
        code_unified: Option<NodeKey>,
        code_deletions: Option<NodeKey>,
        code_additions: Option<NodeKey>,
    ) -> Option<ColumnSet> {
        if diff_style == DiffStyle::Unified {
            self.get_column_pair(code_unified).map(ColumnSet::Unified)
        } else {
            let deletions = self.get_column_pair(code_deletions);
            let additions = self.get_column_pair(code_additions);
            (deletions.is_some() || additions.is_some())
                .then_some(ColumnSet::Split(deletions, additions))
        }
    }
}
