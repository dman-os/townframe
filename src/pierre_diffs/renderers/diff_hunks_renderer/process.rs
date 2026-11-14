// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): the `processDiffResult` method and the
// module-level helpers after the class in packages/diffs/src/renderers/DiffHunksRenderer.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: `processDiffResult`'s closure state (`context`, `pendingSplitContext`,
// the `pushSeparators`/`pushGutterLineNumber` closures) are fields and methods of
// `ProcessContext`/`RowProcessor` so the 560-line method is split along its own
// seams (unified row, split row, no-newline rows, separators). Line elements are
// moved out of the `ThemedDiffResult`. `getModifiedLinesString` uses the English
// plural rule directly (`1` is singular, everything else plural).

use std::collections::HashMap;

use super::DiffHunksRenderer;
use super::HunksRenderResult;
use super::InjectedRow;
use super::RenderedLineContext;
use super::SplitInjectedRow;
use super::SplitLineDecorationProps;
use super::UnifiedLineDecorationProps;
use super::can_hydrate_collapsed_context;
use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::Properties;
use crate::types::layout::RenderRange;
use crate::types::lines::AnnotationSpan;
use crate::types::lines::CodeColumnType;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::HunkData;
use crate::types::lines::HunkDataExpandable;
use crate::types::lines::LineTypes;
use crate::types::options::DiffStyle;
use crate::types::options::HunkSeparators;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::utils::get_total_line_count_from_hunks::get_total_line_count_from_hunks;
use crate::utils::iterate_over_diff::DiffLineCallbackProps;
use crate::utils::iterate_over_diff::DiffLineType;
use crate::utils::iterate_over_diff::IterateOverDiffProps;
use crate::utils::iterate_over_diff::IterationDiffStyle;
use crate::utils::iterate_over_diff::iterate_over_diff;
use crate::utils::render::create_empty_row_buffer::create_empty_row_buffer;
use crate::utils::render::create_no_newline_element::create_no_newline_element;
use crate::utils::render::create_separator::CreateSeparatorProps;
use crate::utils::render::create_separator::create_separator;
use crate::utils::render::get_hunk_separator_slot_name::get_hunk_separator_slot_name;
use crate::utils::render::hast_utils::GutterBufferType;
use crate::utils::render::hast_utils::create_gutter_gap;
use crate::utils::render::hast_utils::create_gutter_item;
use crate::utils::render::includes_file_annotations::FILE_ANNOTATION_HUNK_INDEX;
use crate::utils::render::includes_file_annotations::FILE_ANNOTATION_LINE_INDEX;
use crate::utils::render::includes_file_annotations::get_file_annotations;
use crate::utils::render::includes_file_annotations::should_render_file_annotations;
use crate::utils::render::render_diff_with_highlighter::ThemedDiffResult;
use crate::utils::virtual_diff_layout::get_trailing_context_range_size;

/// A `RenderRange` line count: a non-negative integer held in a JS number.
pub(super) fn line_count(value: f64) -> usize {
    assert!(
        value >= 0.0 && value.fract() == 0.0 && value <= u32::MAX as f64,
        "RenderRange line count must be a non-negative integer, got {value}"
    );
    value as usize
}

/// `collapsedLines: number | 'unknown'`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CollapsedLines {
    Count(usize),
    Unknown,
}

#[derive(Debug, Clone, Copy)]
struct PushSeparatorProps<'hunk> {
    hunk_index: usize,
    collapsed_lines: CollapsedLines,
    range_size: usize,
    hunk_specs: Option<&'hunk str>,
    is_first_hunk: bool,
    is_last_hunk: bool,
    is_expandable: bool,
}

/// `ProcessContext` and `PendingSplitContext` of upstream, in one value.
struct ProcessContext {
    diff_style: DiffStyle,
    hunk_separators: HunkSeparators,
    expansion_line_count: usize,
    row_count: usize,
    unified_content: Vec<ElementContent>,
    deletions_content: Vec<ElementContent>,
    additions_content: Vec<ElementContent>,
    unified_gutter: Vec<ElementContent>,
    deletions_gutter: Vec<ElementContent>,
    additions_gutter: Vec<ElementContent>,
    hunk_data: Vec<HunkData>,
    // PendingSplitContext: a one-sided run of rows whose other column needs a
    // single buffer row of the same height once the run ends.
    pending_size: usize,
    pending_side: Option<AnnotationSide>,
}

impl ProcessContext {
    fn push_to_gutter(&mut self, column: CodeColumnType, element: Element) {
        match column {
            CodeColumnType::Unified => self.unified_gutter.push(element.into()),
            CodeColumnType::Deletions => self.deletions_gutter.push(element.into()),
            CodeColumnType::Additions => self.additions_gutter.push(element.into()),
        }
    }

    fn content_mut(&mut self, column: CodeColumnType) -> &mut Vec<ElementContent> {
        match column {
            CodeColumnType::Unified => &mut self.unified_content,
            CodeColumnType::Deletions => &mut self.deletions_content,
            CodeColumnType::Additions => &mut self.additions_content,
        }
    }

    fn pending_increment(&mut self) {
        self.pending_size += 1;
    }

    fn flush_pending(&mut self) {
        if self.diff_style == DiffStyle::Unified {
            return;
        }
        let Some(side) = self.pending_side.filter(|_| self.pending_size > 0) else {
            self.pending_side = None;
            self.pending_size = 0;
            return;
        };
        let column = match side {
            AnnotationSide::Additions => CodeColumnType::Additions,
            AnnotationSide::Deletions => CodeColumnType::Deletions,
        };
        self.push_to_gutter(
            column,
            create_gutter_gap(None, GutterBufferType::Buffer, self.pending_size),
        );
        let buffer = create_empty_row_buffer(self.pending_size);
        self.content_mut(column).push(buffer.into());
        self.pending_size = 0;
        self.pending_side = None;
    }

    fn increment_row_count(&mut self, count: usize) {
        self.row_count += count;
    }

    fn push_gutter_line_number(
        &mut self,
        column: CodeColumnType,
        line_type: LineTypes,
        line_number: usize,
        line_index: &str,
        gutter_properties: Option<&Properties>,
    ) {
        let empty = Properties::new();
        self.push_to_gutter(
            column,
            create_gutter_item(
                line_type.as_str(),
                line_number,
                line_index,
                gutter_properties.unwrap_or(&empty),
            ),
        );
    }

    fn push_unified_injected_rows(&mut self, rows: Vec<InjectedRow>) {
        for row in rows {
            self.unified_content.push(row.content.into());
            self.push_to_gutter(CodeColumnType::Unified, row.gutter);
            self.increment_row_count(1);
        }
    }

    fn push_split_injected_rows(&mut self, rows: Vec<SplitInjectedRow>) {
        for SplitInjectedRow { deletion, addition } in rows {
            if deletion.is_none() && addition.is_none() {
                continue;
            }
            let missing_side = match (&deletion, &addition) {
                (Some(_), Some(_)) => None,
                (None, _) => Some(AnnotationSide::Deletions),
                (_, None) => Some(AnnotationSide::Additions),
            };
            if missing_side.is_none() || self.pending_side != missing_side {
                self.flush_pending();
            }
            if let Some(deletion) = deletion {
                self.deletions_content.push(deletion.content.into());
                self.push_to_gutter(CodeColumnType::Deletions, deletion.gutter);
            }
            if let Some(addition) = addition {
                self.additions_content.push(addition.content.into());
                self.push_to_gutter(CodeColumnType::Additions, addition.gutter);
            }
            if missing_side.is_some() {
                self.pending_side = missing_side;
                self.pending_increment();
            }
            self.increment_row_count(1);
        }
    }

    fn push_separator(&mut self, column: CodeColumnType, props: PushSeparatorProps<'_>) {
        let PushSeparatorProps {
            hunk_index,
            collapsed_lines,
            range_size,
            hunk_specs,
            is_first_hunk,
            is_last_hunk,
            is_expandable,
        } = props;
        if let CollapsedLines::Count(count) = collapsed_lines
            && count == 0
        {
            return;
        }
        // The gutter and the content column each get their own separator node.
        let (gutter_separator, content_separator) = match self.hunk_separators {
            HunkSeparators::Metadata => {
                let Some(hunk_specs) = hunk_specs else {
                    return;
                };
                let create = || {
                    create_separator(CreateSeparatorProps {
                        separator_type: Some(HunkSeparators::Metadata),
                        content: Some(hunk_specs.to_owned()),
                        is_first_hunk,
                        is_last_hunk,
                        ..CreateSeparatorProps::default()
                    })
                };
                (create(), create())
            }
            HunkSeparators::Simple => {
                if hunk_index == 0 {
                    return;
                }
                let create = || {
                    create_separator(CreateSeparatorProps {
                        separator_type: Some(HunkSeparators::Simple),
                        is_first_hunk,
                        is_last_hunk: false,
                        ..CreateSeparatorProps::default()
                    })
                };
                (create(), create())
            }
            separator_type => {
                let slot_name = get_hunk_separator_slot_name(column, hunk_index);
                let chunked = range_size > self.expansion_line_count;
                let expand_index = is_expandable.then_some(hunk_index);
                let content = match collapsed_lines {
                    CollapsedLines::Count(count) => get_modified_lines_string(count),
                    CollapsedLines::Unknown => {
                        String::from("More unchanged context may be available")
                    }
                };
                let create = || {
                    create_separator(CreateSeparatorProps {
                        separator_type: Some(separator_type),
                        content: Some(content.clone()),
                        expand_index,
                        chunked,
                        slot_name: Some(slot_name.clone()),
                        is_first_hunk,
                        is_last_hunk,
                    })
                };
                let separators = (create(), create());
                self.hunk_data.push(HunkData {
                    slot_name,
                    hunk_index,
                    lines: match collapsed_lines {
                        CollapsedLines::Count(count) => count,
                        CollapsedLines::Unknown => 0,
                    },
                    line_count_known: matches!(collapsed_lines, CollapsedLines::Count(_)),
                    column_type: column,
                    expandable: is_expandable.then_some(HunkDataExpandable {
                        up: !is_first_hunk,
                        down: !is_last_hunk,
                        chunked,
                    }),
                });
                separators
            }
        };
        self.push_to_gutter(column, gutter_separator);
        self.content_mut(column).push(content_separator.into());
        if column != CodeColumnType::Additions {
            self.increment_row_count(1);
        }
    }
}

// Uses the English plural rule: "one" is exactly 1, so 0 reads as "0 unmodified lines".
fn get_modified_lines_string(lines: usize) -> String {
    let suffix = if lines == 1 { "" } else { "s" };
    format!("{lines} unmodified line{suffix}")
}

/// `withContentProperties`: `{ ...lineNode.properties, ...contentProperties }`.
fn with_content_properties(
    line_node: Option<ElementContent>,
    content_properties: Option<&Properties>,
) -> Option<ElementContent> {
    match (line_node, content_properties) {
        (Some(ElementContent::Element(mut element)), Some(properties)) => {
            element.properties.extend(properties);
            Some(ElementContent::Element(element))
        }
        (line_node, _) => line_node,
    }
}

/// `PushLineWithAnnotation`
struct PushLineWithAnnotation {
    diff_style: DiffStyle,
    row_type: DiffLineType,
    deletion_line: Option<ElementContent>,
    addition_line: Option<ElementContent>,
    unified_span: Option<AnnotationSpan>,
    deletion_span: Option<AnnotationSpan>,
    addition_span: Option<AnnotationSpan>,
}

/// The type of an annotation row's gutter gap:
/// `type === 'change' ? change : type`.
fn annotation_line_type(row_type: DiffLineType, change: LineTypes) -> LineTypes {
    match row_type {
        DiffLineType::Change => change,
        DiffLineType::Context => LineTypes::Context,
        DiffLineType::ContextExpanded => LineTypes::ContextExpanded,
    }
}

impl<LAnnotation> DiffHunksRenderer<LAnnotation> {
    pub fn process_diff_result(
        &self,
        file_diff: &FileDiffMetadata,
        render_range: &RenderRange,
        themed: ThemedDiffResult,
    ) -> HunksRenderResult {
        let ThemedDiffResult {
            code,
            theme_styles,
            base_theme_type,
        } = themed;
        let defaults = self.options_with_defaults().base;
        let unified = defaults.diff_style == DiffStyle::Unified;
        let can_hydrate_context =
            can_hydrate_collapsed_context(file_diff, self.options.has_diff_file_loader);
        let mut processor = RowProcessor {
            renderer: self,
            file_diff,
            addition_lines: code.addition_lines,
            deletion_lines: code.deletion_lines,
            context: ProcessContext {
                diff_style: defaults.diff_style,
                hunk_separators: defaults.hunk_separators,
                expansion_line_count: defaults.expansion_line_count,
                row_count: 0,
                unified_content: Vec::new(),
                deletions_content: Vec::new(),
                additions_content: Vec::new(),
                unified_gutter: Vec::new(),
                deletions_gutter: Vec::new(),
                additions_gutter: Vec::new(),
                hunk_data: Vec::new(),
                pending_size: 0,
                pending_side: None,
            },
            trailing_range_size: get_trailing_context_range_size(
                file_diff,
                "DiffHunksRenderer.processDiffResult",
            ),
            can_hydrate_context,
            is_expandable_diff: !file_diff.is_partial || can_hydrate_context,
        };

        processor.push_file_level_annotations(render_range);

        let expanded_hunks = if defaults.expand_unchanged {
            ExpandedHunks::All
        } else {
            ExpandedHunks::borrowed(&self.expanded_hunks)
        };
        iterate_over_diff(
            IterateOverDiffProps {
                diff: file_diff,
                diff_style: match defaults.diff_style {
                    DiffStyle::Unified => IterationDiffStyle::Unified,
                    DiffStyle::Split => IterationDiffStyle::Split,
                },
                starting_line: line_count(render_range.starting_line),
                total_lines: render_range
                    .total_lines
                    .is_finite()
                    .then(|| line_count(render_range.total_lines)),
                expanded_hunks: Some(&expanded_hunks),
                collapsed_context_threshold: defaults.collapsed_context_threshold,
            },
            |props| processor.process_row(props),
        );

        let RowProcessor { mut context, .. } = processor;
        if defaults.diff_style == DiffStyle::Split {
            context.flush_pending();
        }

        let total_lines = get_total_line_count_from_hunks(&file_diff.hunks)
            .max(file_diff.addition_lines.len())
            .max(file_diff.deletion_lines.len());
        let has_buffer = render_range.buffer_before > 0.0 || render_range.buffer_after > 0.0;
        // Determine which ASTs to include based on diff style and file type
        use crate::types::patch::ChangeTypes;
        let include_additions = !unified && file_diff.change_type != ChangeTypes::Deleted;
        let include_deletions = !unified && file_diff.change_type != ChangeTypes::New;
        let has_content = context.row_count > 0 || has_buffer;
        let additions_content =
            (include_additions && has_content).then_some(context.additions_content);
        let deletions_content =
            (include_deletions && has_content).then_some(context.deletions_content);
        let unified_content = (unified && has_content).then_some(context.unified_content);
        let pre_node = self.create_pre_element(
            deletions_content.is_some() && additions_content.is_some(),
            total_lines,
            None,
        );
        HunksRenderResult {
            unified_gutter_ast: (unified && has_content).then_some(context.unified_gutter),
            unified_content_ast: unified_content,
            deletions_gutter_ast: (include_deletions && has_content)
                .then_some(context.deletions_gutter),
            deletions_content_ast: deletions_content,
            additions_gutter_ast: (include_additions && has_content)
                .then_some(context.additions_gutter),
            additions_content_ast: additions_content,
            hunk_data: context.hunk_data,
            pre_node,
            theme_styles,
            base_theme_type,
            header_element: (!defaults.disable_file_header).then(|| self.render_header(file_diff)),
            total_lines,
            row_count: context.row_count,
            buffer_before: render_range.buffer_before,
            buffer_after: render_range.buffer_after,
            // FIXME upstream: the css is not produced here yet.
            css: String::new(),
        }
    }

    /// `getAnnotations('unified', ...)`.
    fn unified_annotations(
        &self,
        deletion_line_number: Option<usize>,
        addition_line_number: Option<usize>,
        hunk_index: i64,
        line_index: i64,
    ) -> Option<AnnotationSpan> {
        let mut annotations = Vec::new();
        for (line_number, map) in [
            (deletion_line_number, &self.deletion_annotations),
            (addition_line_number, &self.addition_annotations),
        ] {
            if let Some(line_number) = line_number {
                annotations.extend(self.annotation_names(map.get(&line_number)));
            }
        }
        (!annotations.is_empty()).then_some(AnnotationSpan {
            hunk_index,
            line_index,
            annotations,
        })
    }

    /// `getAnnotations('split', ...)`: `(deletionSpan, additionSpan)`.
    fn split_annotations(
        &self,
        deletion_line_number: Option<usize>,
        addition_line_number: Option<usize>,
        hunk_index: i64,
        line_index: i64,
    ) -> Option<(AnnotationSpan, AnnotationSpan)> {
        let span = |line_number: Option<usize>,
                    map: &HashMap<
            usize,
            Vec<crate::types::view::DiffLineAnnotation<LAnnotation>>,
        >| {
            AnnotationSpan {
                hunk_index,
                line_index,
                annotations: line_number
                    .map(|line_number| self.annotation_names(map.get(&line_number)))
                    .unwrap_or_default(),
            }
        };
        let deletion_span = span(deletion_line_number, &self.deletion_annotations);
        let addition_span = span(addition_line_number, &self.addition_annotations);
        (!addition_span.annotations.is_empty() || !deletion_span.annotations.is_empty())
            .then_some((deletion_span, addition_span))
    }

    fn annotation_names(
        &self,
        annotations: Option<&Vec<crate::types::view::DiffLineAnnotation<LAnnotation>>>,
    ) -> Vec<String> {
        annotations
            .map(|annotations| {
                annotations
                    .iter()
                    .map(|annotation| (self.annotation_slot_name)(annotation))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The per-render state `processDiffResult`'s callback closes over.
struct RowProcessor<'renderer, 'diff, LAnnotation> {
    renderer: &'renderer DiffHunksRenderer<LAnnotation>,
    file_diff: &'diff FileDiffMetadata,
    addition_lines: Vec<Option<ElementContent>>,
    deletion_lines: Vec<Option<ElementContent>>,
    context: ProcessContext,
    trailing_range_size: usize,
    can_hydrate_context: bool,
    is_expandable_diff: bool,
}

impl<'diff, LAnnotation> RowProcessor<'_, 'diff, LAnnotation> {
    /// `pushLineWithAnnotation`
    fn push_line_with_annotation(&mut self, props: PushLineWithAnnotation) {
        let PushLineWithAnnotation {
            diff_style,
            row_type,
            deletion_line,
            addition_line,
            unified_span,
            deletion_span,
            addition_span,
        } = props;
        let hooks = &self.renderer.hooks;
        let context = &mut self.context;
        let mut has_annotation_row = false;
        if diff_style == DiffStyle::Unified {
            if let Some(line) = addition_line.or(deletion_line.clone()) {
                context.unified_content.push(line);
            }
            if let Some(span) = unified_span {
                let line_type = annotation_line_type(
                    row_type,
                    if deletion_line.is_some() {
                        LineTypes::ChangeDeletion
                    } else {
                        LineTypes::ChangeAddition
                    },
                );
                context
                    .unified_content
                    .push(hooks.create_annotation_element(&span).into());
                context.push_to_gutter(
                    CodeColumnType::Unified,
                    create_gutter_gap(Some(line_type), GutterBufferType::Annotation, 1),
                );
                has_annotation_row = true;
            }
        } else {
            let has_deletion = deletion_line.is_some();
            let has_addition = addition_line.is_some();
            if let Some(line) = deletion_line {
                context.deletions_content.push(line);
            }
            if let Some(line) = addition_line {
                context.additions_content.push(line);
            }
            if let Some(span) = deletion_span {
                let line_type = annotation_line_type(
                    row_type,
                    if has_deletion {
                        LineTypes::ChangeDeletion
                    } else {
                        LineTypes::Context
                    },
                );
                context
                    .deletions_content
                    .push(hooks.create_annotation_element(&span).into());
                context.push_to_gutter(
                    CodeColumnType::Deletions,
                    create_gutter_gap(Some(line_type), GutterBufferType::Annotation, 1),
                );
                has_annotation_row = true;
            }
            if let Some(span) = addition_span {
                let line_type = annotation_line_type(
                    row_type,
                    if has_addition {
                        LineTypes::ChangeAddition
                    } else {
                        LineTypes::Context
                    },
                );
                context
                    .additions_content
                    .push(hooks.create_annotation_element(&span).into());
                context.push_to_gutter(
                    CodeColumnType::Additions,
                    create_gutter_gap(Some(line_type), GutterBufferType::Annotation, 1),
                );
                has_annotation_row = true;
            }
        }
        if has_annotation_row {
            context.increment_row_count(1);
        }
    }

    /// `pushFileLevelAnnotations`
    fn push_file_level_annotations(&mut self, render_range: &RenderRange) {
        if !should_render_file_annotations(render_range) {
            return;
        }
        use crate::types::patch::ChangeTypes;
        let renderer = self.renderer;
        let deletion_names = if self.file_diff.change_type != ChangeTypes::New {
            renderer.annotation_names(get_file_annotations(&renderer.deletion_annotations))
        } else {
            Vec::new()
        };
        let addition_names = if self.file_diff.change_type != ChangeTypes::Deleted {
            renderer.annotation_names(get_file_annotations(&renderer.addition_annotations))
        } else {
            Vec::new()
        };
        if deletion_names.is_empty() && addition_names.is_empty() {
            return;
        }
        let span = |annotations| AnnotationSpan {
            hunk_index: FILE_ANNOTATION_HUNK_INDEX,
            line_index: FILE_ANNOTATION_LINE_INDEX,
            annotations,
        };
        let mut props = PushLineWithAnnotation {
            diff_style: self.context.diff_style,
            row_type: DiffLineType::Context,
            deletion_line: None,
            addition_line: None,
            unified_span: None,
            deletion_span: None,
            addition_span: None,
        };
        if self.context.diff_style == DiffStyle::Unified {
            props.unified_span = Some(span([deletion_names, addition_names].concat()));
        } else {
            props.deletion_span = Some(span(deletion_names));
            props.addition_span = Some(span(addition_names));
        }
        self.push_line_with_annotation(props);
    }

    fn push_separators(&mut self, props: PushSeparatorProps<'_>) {
        self.context.flush_pending();
        if self.context.diff_style == DiffStyle::Unified {
            self.context.push_separator(CodeColumnType::Unified, props);
        } else {
            self.context
                .push_separator(CodeColumnType::Deletions, props);
            self.context
                .push_separator(CodeColumnType::Additions, props);
        }
    }

    /// The `iterateOverDiff` callback of `processDiffResult`.
    fn process_row(&mut self, props: DiffLineCallbackProps<'diff>) -> bool {
        let DiffLineCallbackProps {
            hunk_index,
            hunk,
            collapsed_before,
            collapsed_after,
            line_type,
            deletion_line,
            addition_line,
        } = props;
        let diff_style = self.context.diff_style;
        let unified = diff_style == DiffStyle::Unified;
        let split_line_index = deletion_line
            .or(addition_line)
            .expect("a row has a side")
            .split_line_index;
        let unified_line_index = addition_line
            .or(deletion_line)
            .expect("a row has a side")
            .unified_line_index;

        if !unified && line_type != DiffLineType::Change {
            self.context.flush_pending();
        }

        if collapsed_before > 0 {
            self.push_separators(PushSeparatorProps {
                hunk_index,
                collapsed_lines: CollapsedLines::Count(collapsed_before),
                range_size: hunk.map_or(0, |hunk| hunk.collapsed_before),
                hunk_specs: hunk.and_then(|hunk| hunk.hunk_specs.as_deref()),
                is_first_hunk: hunk_index == 0,
                is_last_hunk: false,
                is_expandable: self.is_expandable_diff,
            });
        }

        let line_index = if unified {
            unified_line_index
        } else {
            split_line_index
        };
        let rendered = RenderedLineContext {
            row_type: line_type,
            hunk_index,
            line_index,
            unified_line_index,
            split_line_index,
            deletion_line,
            addition_line,
        };
        if unified {
            self.push_unified_row(&rendered);
        } else {
            self.push_split_row(&rendered);
        }

        let is_final_split_hunk_row = !unified
            && hunk.is_some_and(|hunk| {
                split_line_index + 1 == hunk.split_line_start + hunk.split_line_count
            });
        let is_final_hunk_row = hunk_index + 1 == self.file_diff.hunks.len()
            && hunk.is_some_and(|hunk| {
                if unified {
                    unified_line_index + 1 == hunk.unified_line_start + hunk.unified_line_count
                } else {
                    split_line_index + 1 == hunk.split_line_start + hunk.split_line_count
                }
            });
        let no_eofcr_deletion = deletion_line.is_some_and(|line| line.no_eofcr)
            || (is_final_split_hunk_row && hunk.is_some_and(|hunk| hunk.no_eofcr_deletions));
        let no_eofcr_addition = addition_line.is_some_and(|line| line.no_eofcr)
            || (is_final_split_hunk_row && hunk.is_some_and(|hunk| hunk.no_eofcr_additions));
        if no_eofcr_addition || no_eofcr_deletion {
            self.push_no_eofcr_rows(line_type, no_eofcr_deletion, no_eofcr_addition);
        }

        let hunk_separators = self.context.hunk_separators;
        if hunk_separators != HunkSeparators::Simple
            && hunk_separators != HunkSeparators::Metadata
            && (collapsed_after > 0 || (is_final_hunk_row && self.can_hydrate_context))
        {
            self.push_separators(PushSeparatorProps {
                hunk_index: if line_type == DiffLineType::ContextExpanded {
                    hunk_index
                } else {
                    hunk_index + 1
                },
                collapsed_lines: if is_final_hunk_row && self.can_hydrate_context {
                    CollapsedLines::Unknown
                } else {
                    CollapsedLines::Count(collapsed_after)
                },
                range_size: self.trailing_range_size,
                hunk_specs: None,
                is_first_hunk: false,
                is_last_hunk: true,
                is_expandable: self.is_expandable_diff,
            });
        }
        self.context.increment_row_count(1);
        false
    }

    fn take_lines(
        &mut self,
        deletion_line: Option<crate::utils::iterate_over_diff::DiffLineMetadata>,
        addition_line: Option<crate::utils::iterate_over_diff::DiffLineMetadata>,
    ) -> (Option<ElementContent>, Option<ElementContent>) {
        let deletion = deletion_line.and_then(|line| self.deletion_lines[line.line_index].take());
        let addition = addition_line.and_then(|line| self.addition_lines[line.line_index].take());
        if deletion.is_none() && addition.is_none() {
            panic!(
                "DiffHunksRenderer.processDiffResult: deletionLine and additionLine are null, something is wrong (file {})",
                self.file_diff.name
            );
        }
        (deletion, addition)
    }

    fn push_unified_row(&mut self, rendered: &RenderedLineContext) {
        let RenderedLineContext {
            row_type,
            hunk_index,
            line_index,
            unified_line_index,
            split_line_index,
            deletion_line,
            addition_line,
        } = *rendered;
        let hooks = &self.renderer.hooks;
        let injected = hooks
            .unified_injected_rows_for_line(rendered)
            .unwrap_or_default();
        if let Some(rows) = injected.before {
            self.context.push_unified_injected_rows(rows);
        }
        let (deletion_content, addition_content) = self.take_lines(deletion_line, addition_line);
        let line_type = match row_type {
            DiffLineType::Change if addition_line.is_some() => LineTypes::ChangeAddition,
            DiffLineType::Change => LineTypes::ChangeDeletion,
            DiffLineType::Context => LineTypes::Context,
            DiffLineType::ContextExpanded => LineTypes::ContextExpanded,
        };
        let decoration = hooks.unified_line_decoration(&UnifiedLineDecorationProps {
            row_type,
            line_type,
            addition_line_index: addition_line.map(|line| line.line_index),
            deletion_line_index: deletion_line.map(|line| line.line_index),
        });
        self.context.push_gutter_line_number(
            CodeColumnType::Unified,
            decoration.gutter_line_type,
            addition_line
                .or(deletion_line)
                .expect("a row has a side")
                .line_number,
            &format!("{unified_line_index},{split_line_index}"),
            decoration.gutter_properties.as_ref(),
        );
        // The content of the addition wins; a deletion's is only kept for the
        // annotation row's line type.
        let (deletion_content, addition_content) = if addition_content.is_some() {
            (
                deletion_content,
                with_content_properties(addition_content, decoration.content_properties.as_ref()),
            )
        } else {
            (
                with_content_properties(deletion_content, decoration.content_properties.as_ref()),
                None,
            )
        };
        let unified_span = self.renderer.unified_annotations(
            deletion_line.map(|line| line.line_number),
            addition_line.map(|line| line.line_number),
            i64::try_from(hunk_index).expect("hunk index fits"),
            i64::try_from(line_index).expect("line index fits"),
        );
        self.push_line_with_annotation(PushLineWithAnnotation {
            diff_style: DiffStyle::Unified,
            row_type,
            deletion_line: deletion_content,
            addition_line: addition_content,
            unified_span,
            deletion_span: None,
            addition_span: None,
        });
        if let Some(rows) = injected.after {
            self.context.push_unified_injected_rows(rows);
        }
    }

    fn push_split_row(&mut self, rendered: &RenderedLineContext) {
        let RenderedLineContext {
            row_type,
            hunk_index,
            line_index,
            split_line_index,
            deletion_line,
            addition_line,
            ..
        } = *rendered;
        let hooks = &self.renderer.hooks;
        let injected = hooks
            .split_injected_rows_for_line(rendered)
            .unwrap_or_default();
        if let Some(rows) = injected.before {
            self.context.push_split_injected_rows(rows);
        }
        let (deletion_content, addition_content) = self.take_lines(deletion_line, addition_line);
        let deletion_decoration = hooks.split_line_decoration(&SplitLineDecorationProps {
            side: AnnotationSide::Deletions,
            row_type,
            line_index: deletion_line.map(|line| line.line_index),
        });
        let addition_decoration = hooks.split_line_decoration(&SplitLineDecorationProps {
            side: AnnotationSide::Additions,
            row_type,
            line_index: addition_line.map(|line| line.line_index),
        });

        let missing_side = match row_type {
            DiffLineType::Change if addition_content.is_none() => Some(AnnotationSide::Additions),
            DiffLineType::Change if deletion_content.is_none() => Some(AnnotationSide::Deletions),
            _ => None,
        };
        if let Some(missing_side) = missing_side {
            if self
                .context
                .pending_side
                .is_some_and(|side| side != missing_side)
            {
                self.context.flush_pending();
            }
            self.context.pending_side = Some(missing_side);
            self.context.pending_increment();
        } else if row_type == DiffLineType::Change {
            // A change row with both sides fills the column a pending
            // one-sided buffer was holding open (an insert/delete block
            // directly followed by a paired block, from similarity
            // realignment); flush first so the buffer lands above this row.
            self.context.flush_pending();
        }

        let annotation_spans = self.renderer.split_annotations(
            deletion_line.map(|line| line.line_number),
            addition_line.map(|line| line.line_number),
            i64::try_from(hunk_index).expect("hunk index fits"),
            i64::try_from(line_index).expect("line index fits"),
        );
        if annotation_spans.is_some() && self.context.pending_size > 0 {
            self.context.flush_pending();
        }

        let mut deletion_content = deletion_content;
        if let Some(line) = deletion_line {
            deletion_content = with_content_properties(
                deletion_content,
                deletion_decoration.content_properties.as_ref(),
            );
            self.context.push_gutter_line_number(
                CodeColumnType::Deletions,
                deletion_decoration.gutter_line_type,
                line.line_number,
                &format!("{},{split_line_index}", line.unified_line_index),
                deletion_decoration.gutter_properties.as_ref(),
            );
        }
        let mut addition_content = addition_content;
        if let Some(line) = addition_line {
            addition_content = with_content_properties(
                addition_content,
                addition_decoration.content_properties.as_ref(),
            );
            self.context.push_gutter_line_number(
                CodeColumnType::Additions,
                addition_decoration.gutter_line_type,
                line.line_number,
                &format!("{},{split_line_index}", line.unified_line_index),
                addition_decoration.gutter_properties.as_ref(),
            );
        }
        let (deletion_span, addition_span) = match annotation_spans {
            Some((deletion_span, addition_span)) => (Some(deletion_span), Some(addition_span)),
            None => (None, None),
        };
        self.push_line_with_annotation(PushLineWithAnnotation {
            diff_style: DiffStyle::Split,
            row_type,
            deletion_line: deletion_content,
            addition_line: addition_content,
            unified_span: None,
            deletion_span,
            addition_span,
        });
        if let Some(rows) = injected.after {
            self.context.push_split_injected_rows(rows);
        }
    }

    /// The "\ No newline at end of file" rows of one source row.
    fn push_no_eofcr_rows(
        &mut self,
        row_type: DiffLineType,
        no_eofcr_deletion: bool,
        no_eofcr_addition: bool,
    ) {
        let unified = self.context.diff_style == DiffStyle::Unified;
        let context = &mut self.context;
        if !unified {
            context.flush_pending();
        }
        let no_eof_type = |change: LineTypes| match row_type {
            DiffLineType::Context => LineTypes::Context,
            DiffLineType::ContextExpanded => LineTypes::ContextExpanded,
            DiffLineType::Change => change,
        };
        if no_eofcr_deletion {
            let no_eof_type = no_eof_type(LineTypes::ChangeDeletion);
            if unified {
                context
                    .unified_content
                    .push(create_no_newline_element(no_eof_type).into());
                context.push_to_gutter(
                    CodeColumnType::Unified,
                    create_gutter_gap(Some(no_eof_type), GutterBufferType::Metadata, 1),
                );
            } else {
                context
                    .deletions_content
                    .push(create_no_newline_element(no_eof_type).into());
                context.push_to_gutter(
                    CodeColumnType::Deletions,
                    create_gutter_gap(Some(no_eof_type), GutterBufferType::Metadata, 1),
                );
                if !no_eofcr_addition {
                    context.push_to_gutter(
                        CodeColumnType::Additions,
                        create_gutter_gap(None, GutterBufferType::Buffer, 1),
                    );
                    context
                        .additions_content
                        .push(create_empty_row_buffer(1).into());
                }
            }
        }
        if no_eofcr_addition && !(unified && no_eofcr_deletion && row_type != DiffLineType::Change)
        {
            let no_eof_type = no_eof_type(LineTypes::ChangeAddition);
            if unified {
                context
                    .unified_content
                    .push(create_no_newline_element(no_eof_type).into());
                context.push_to_gutter(
                    CodeColumnType::Unified,
                    create_gutter_gap(Some(no_eof_type), GutterBufferType::Metadata, 1),
                );
            } else {
                context
                    .additions_content
                    .push(create_no_newline_element(no_eof_type).into());
                context.push_to_gutter(
                    CodeColumnType::Additions,
                    create_gutter_gap(Some(no_eof_type), GutterBufferType::Metadata, 1),
                );
                if !no_eofcr_deletion {
                    context.push_to_gutter(
                        CodeColumnType::Deletions,
                        create_gutter_gap(None, GutterBufferType::Buffer, 1),
                    );
                    context
                        .deletions_content
                        .push(create_empty_row_buffer(1).into());
                }
            }
        }
        context.increment_row_count(1);
    }
}
