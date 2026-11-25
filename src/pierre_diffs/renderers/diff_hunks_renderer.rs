// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/renderers/DiffHunksRenderer.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// - Synchronous and stateless about diffs. Upstream keeps the latest diff, a
//   `renderCache`, a pending background highlight result and a worker pool and
//   reconciles them across renders; here `render_diff` takes the diff and a
//   highlighter and returns the result, so `renderCache`, `pendingHighlightResult`,
//   `hydrate`, `asyncRender`/`asyncHighlight`, `onHighlightSuccess`/`onHighlightError`,
//   `onRenderUpdate`, `initializeHighlighter`, `canRenderDiff`, `getDiffForNextRender`,
//   `getEffectiveCodeOptions`, `getMatchingWorkerResultCache`, the worker manager and
//   `clearRenderCache` are dropped (PARITY.md lists each).
// - The edit session is out of scope: `beginEditSession`, `endEditSession`,
//   `editorRenderReady`, `refreshHighlightedResult`, `updateRenderCache`,
//   `applyDocumentChange`, `setExpandedHunksMap`, the session-region helpers and
//   `isRenderCacheDirty` (always false without a session) are dropped.
// - `renderFullHTML`/`renderPartialHTML` are dropped with `hastToHtml`.
// - `lazyLineAST` is replaced by the worker's windowed plain-text render: a
//   plain-text diff rendered through a finite `RenderRange` highlights only the
//   window (see `render_diff`).
// - `HunksRenderResult.fileDiff` is not returned: the caller owns the diff.
//   `process_diff_result` consumes the `ThemedDiffResult` and moves the line
//   elements into the result instead of sharing them with a cache.
// - The `protected` extension points (`getUnifiedLineDecoration`,
//   `getSplitLineDecoration`, `createAnnotationElement`,
//   `getUnifiedInjectedRowsForLine`, `getSplitInjectedRowsForLine`) are the
//   [`DiffHunksRendererHooks`] trait; the `annotationSlotName` constructor
//   argument is a closure.
// - `loadDiffFiles != null` is the `has_diff_file_loader` option.

mod process;

use std::collections::HashMap;

use serde::Serialize;

use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::constants::DEFAULT_EXPANDED_REGION;
use crate::constants::DEFAULT_RENDER_RANGE;
use crate::constants::DEFAULT_TOKENIZE_MAX_LENGTH;
use crate::constants::default_themes;
use crate::hast::Element;
use crate::hast::ElementContent;
use crate::hast::PropValue;
use crate::hast::Properties;
use crate::highlighter_types::DiffsHighlighter;
use crate::types::layout::RenderRange;
use crate::types::lines::AnnotationSpan;
use crate::types::lines::CodeColumnType;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::ExpansionDirections;
use crate::types::lines::ForceDiffPlainTextOptions;
use crate::types::lines::HunkData;
use crate::types::lines::HunkExpansionRegion;
use crate::types::lines::LineTypes;
use crate::types::options::BaseDiffOptions;
use crate::types::options::BaseDiffOptionsWithDefaults;
use crate::types::options::BaseThemeType;
use crate::types::options::CustomPreProperties;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::FileHeaderRenderMode;
use crate::types::options::HunkSeparators;
use crate::types::options::LineDiffTypes;
use crate::types::options::Overflow;
use crate::types::options::PrePropertiesConfig;
use crate::types::options::PreType;
use crate::types::options::RenderDiffOptions;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;
use crate::utils::iterate_over_diff::DiffLineMetadata;
use crate::utils::iterate_over_diff::DiffLineType;
use crate::utils::predicates::is_diff_plain_text::is_diff_plain_text;
use crate::utils::render::create_annotation_element::create_annotation_element;
use crate::utils::render::create_content_column::create_content_column;
use crate::utils::render::create_file_header_element::CreateFileHeaderElementProps;
use crate::utils::render::create_file_header_element::FileOrDiff;
use crate::utils::render::create_file_header_element::create_file_header_element;
use crate::utils::render::create_pre_element::create_pre_element;
use crate::utils::render::get_line_annotation_name::AnnotationName;
use crate::utils::render::get_line_annotation_name::get_line_annotation_name;
use crate::utils::render::hast_utils::create_gutter_wrapper;
use crate::utils::render::hast_utils::create_hast_element;
use crate::utils::render::render_diff_with_highlighter::render_diff_with_highlighter;

/// `DiffHunksRendererOptions`: the base diff options plus the header mode.
/// `Default` is upstream's constructor default `{ theme: DEFAULT_THEMES }`.
#[derive(Debug, Clone, PartialEq)]
pub struct DiffHunksRendererOptions {
    pub base: BaseDiffOptions,
    pub header_render_mode: Option<FileHeaderRenderMode>,
    /// Stands for `loadDiffFiles != null`; the loader itself (a callback)
    /// belongs to the consuming app.
    pub has_diff_file_loader: bool,
}

impl Default for DiffHunksRendererOptions {
    fn default() -> Self {
        let mut base = BaseDiffOptions::default();
        base.base.theme = Some(ThemeOption::Themes(default_themes()));
        Self {
            base,
            header_render_mode: None,
            has_diff_file_loader: false,
        }
    }
}

/// `DiffHunksRendererOptionsWithDefaults`. Upstream omits `themeType` here; the
/// resolved value is carried along unused.
#[derive(Debug, Clone, PartialEq)]
pub struct DiffHunksRendererOptionsWithDefaults {
    pub base: BaseDiffOptionsWithDefaults,
    pub header_render_mode: FileHeaderRenderMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnifiedLineDecorationProps {
    pub row_type: DiffLineType,
    pub line_type: LineTypes,
    pub addition_line_index: Option<usize>,
    pub deletion_line_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitLineDecorationProps {
    pub side: AnnotationSide,
    pub row_type: DiffLineType,
    pub line_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LineDecoration {
    pub gutter_line_type: LineTypes,
    pub gutter_properties: Option<Properties>,
    pub content_properties: Option<Properties>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RenderedLineContext {
    pub row_type: DiffLineType,
    pub hunk_index: usize,
    pub line_index: usize,
    pub unified_line_index: usize,
    pub split_line_index: usize,
    pub deletion_line: Option<DiffLineMetadata>,
    pub addition_line: Option<DiffLineMetadata>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InjectedRow {
    pub content: Element,
    pub gutter: Element,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SplitInjectedRow {
    pub deletion: Option<InjectedRow>,
    pub addition: Option<InjectedRow>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct UnifiedInjectedRowPlacement {
    pub before: Option<Vec<InjectedRow>>,
    pub after: Option<Vec<InjectedRow>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SplitInjectedRowPlacement {
    pub before: Option<Vec<SplitInjectedRow>>,
    pub after: Option<Vec<SplitInjectedRow>>,
}

/// The `protected` members upstream's subclasses override. Every method has
/// upstream's default behaviour.
pub trait DiffHunksRendererHooks {
    fn unified_line_decoration(&self, props: &UnifiedLineDecorationProps) -> LineDecoration {
        LineDecoration {
            gutter_line_type: props.line_type,
            gutter_properties: None,
            content_properties: Some(
                Properties::new().with("data-line-type", props.line_type.as_str()),
            ),
        }
    }

    fn split_line_decoration(&self, props: &SplitLineDecorationProps) -> LineDecoration {
        let line_type = match props.row_type {
            DiffLineType::Change => match props.side {
                AnnotationSide::Deletions => LineTypes::ChangeDeletion,
                AnnotationSide::Additions => LineTypes::ChangeAddition,
            },
            DiffLineType::Context => LineTypes::Context,
            DiffLineType::ContextExpanded => LineTypes::ContextExpanded,
        };
        LineDecoration {
            gutter_line_type: line_type,
            gutter_properties: None,
            content_properties: Some(Properties::new().with("data-line-type", line_type.as_str())),
        }
    }

    fn create_annotation_element(&self, span: &AnnotationSpan) -> Element {
        create_annotation_element(span)
    }

    /// Extra rows rendered before/after the current line in unified style.
    fn unified_injected_rows_for_line(
        &self,
        _context: &RenderedLineContext,
    ) -> Option<UnifiedInjectedRowPlacement> {
        None
    }

    /// Extra rows per side rendered before/after the current line in split style.
    fn split_injected_rows_for_line(
        &self,
        _context: &RenderedLineContext,
    ) -> Option<SplitInjectedRowPlacement> {
        None
    }
}

/// Upstream's behaviour with no subclass.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultHooks;

impl DiffHunksRendererHooks for DefaultHooks {}

/// `HunksRenderResult` (without `fileDiff`, see the file header).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HunksRenderResult {
    #[serde(rename = "unifiedGutterAST", skip_serializing_if = "Option::is_none")]
    pub unified_gutter_ast: Option<Vec<ElementContent>>,
    #[serde(rename = "unifiedContentAST", skip_serializing_if = "Option::is_none")]
    pub unified_content_ast: Option<Vec<ElementContent>>,
    #[serde(rename = "deletionsGutterAST", skip_serializing_if = "Option::is_none")]
    pub deletions_gutter_ast: Option<Vec<ElementContent>>,
    #[serde(
        rename = "deletionsContentAST",
        skip_serializing_if = "Option::is_none"
    )]
    pub deletions_content_ast: Option<Vec<ElementContent>>,
    #[serde(rename = "additionsGutterAST", skip_serializing_if = "Option::is_none")]
    pub additions_gutter_ast: Option<Vec<ElementContent>>,
    #[serde(
        rename = "additionsContentAST",
        skip_serializing_if = "Option::is_none"
    )]
    pub additions_content_ast: Option<Vec<ElementContent>>,
    pub hunk_data: Vec<HunkData>,
    pub css: String,
    pub pre_node: Element,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_element: Option<Element>,
    pub total_lines: usize,
    pub theme_styles: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_theme_type: Option<BaseThemeType>,
    pub row_count: usize,
    pub buffer_before: f64,
    pub buffer_after: f64,
}

/// `annotationSlotName`: the slot a line annotation is rendered into.
pub type AnnotationSlotName<LAnnotation> = Box<dyn Fn(&DiffLineAnnotation<LAnnotation>) -> String>;

pub struct DiffHunksRenderer<LAnnotation> {
    pub options: DiffHunksRendererOptions,
    expanded_hunks: HashMap<usize, HunkExpansionRegion>,
    deletion_annotations: HashMap<usize, Vec<DiffLineAnnotation<LAnnotation>>>,
    addition_annotations: HashMap<usize, Vec<DiffLineAnnotation<LAnnotation>>>,
    annotation_slot_name: AnnotationSlotName<LAnnotation>,
    hooks: Box<dyn DiffHunksRendererHooks>,
}

impl<LAnnotation> DiffHunksRenderer<LAnnotation> {
    /// `new DiffHunksRenderer(options)`: default slot names, default hooks.
    pub fn new(options: DiffHunksRendererOptions) -> Self {
        Self::with_hooks(
            options,
            Box::new(|annotation| {
                get_line_annotation_name(AnnotationName::DiffLine {
                    side: annotation.side,
                    line_number: annotation.line_number,
                })
            }),
            Box::new(DefaultHooks),
        )
    }

    pub fn with_hooks(
        options: DiffHunksRendererOptions,
        annotation_slot_name: AnnotationSlotName<LAnnotation>,
        hooks: Box<dyn DiffHunksRendererHooks>,
    ) -> Self {
        Self {
            options,
            expanded_hunks: HashMap::new(),
            deletion_annotations: HashMap::new(),
            addition_annotations: HashMap::new(),
            annotation_slot_name,
            hooks,
        }
    }

    /// `recycle`: forgets the annotations of the previous use. (Upstream also
    /// drops the diff and the render cache and cleans up the worker's tasks;
    /// the Rust renderer holds none of them.)
    pub fn recycle(&mut self) {
        self.addition_annotations.clear();
        self.deletion_annotations.clear();
    }

    /// `cleanUp`: `recycle` and forgets the expanded hunks. (The edit session
    /// and the worker are not ported.)
    pub fn clean_up(&mut self) {
        self.recycle();
        self.expanded_hunks.clear();
    }

    pub fn set_options(&mut self, options: DiffHunksRendererOptions) {
        self.options = options;
    }

    pub fn expand_hunk(
        &mut self,
        index: usize,
        direction: ExpansionDirections,
        expansion_line_count: Option<usize>,
    ) {
        let expansion_line_count =
            expansion_line_count.unwrap_or(self.options_with_defaults().base.expansion_line_count);
        let mut region = self
            .expanded_hunks
            .get(&index)
            .copied()
            .unwrap_or(HunkExpansionRegion {
                from_start: 0,
                from_end: 0,
            });
        if matches!(
            direction,
            ExpansionDirections::Up | ExpansionDirections::Both
        ) {
            region.from_start += expansion_line_count;
        }
        if matches!(
            direction,
            ExpansionDirections::Down | ExpansionDirections::Both
        ) {
            region.from_end += expansion_line_count;
        }
        self.expanded_hunks.insert(index, region);
    }

    pub fn get_expanded_hunk(&self, hunk_index: usize) -> HunkExpansionRegion {
        self.expanded_hunks
            .get(&hunk_index)
            .copied()
            .unwrap_or(DEFAULT_EXPANDED_REGION)
    }

    pub fn get_expanded_hunks_map(&self) -> &HashMap<usize, HunkExpansionRegion> {
        &self.expanded_hunks
    }

    pub fn set_line_annotations(&mut self, line_annotations: Vec<DiffLineAnnotation<LAnnotation>>) {
        self.addition_annotations.clear();
        self.deletion_annotations.clear();
        for annotation in line_annotations {
            let map = match annotation.side {
                AnnotationSide::Deletions => &mut self.deletion_annotations,
                AnnotationSide::Additions => &mut self.addition_annotations,
            };
            map.entry(annotation.line_number)
                .or_default()
                .push(annotation);
        }
    }

    /// `getOptionsWithDefaults`.
    pub fn options_with_defaults(&self) -> DiffHunksRendererOptionsWithDefaults {
        let base = &self.options.base;
        let code = &base.base;
        DiffHunksRendererOptionsWithDefaults {
            base: BaseDiffOptionsWithDefaults {
                theme: code
                    .theme
                    .clone()
                    .unwrap_or_else(|| ThemeOption::Themes(default_themes())),
                disable_line_numbers: code.disable_line_numbers.unwrap_or(false),
                overflow: code.overflow.unwrap_or(Overflow::Scroll),
                theme_type: code.theme_type.unwrap_or(ThemeTypes::System),
                collapsed: code.collapsed.unwrap_or(false),
                disable_file_header: code.disable_file_header.unwrap_or(false),
                disable_virtualization_buffers: code
                    .disable_virtualization_buffers
                    .unwrap_or(false),
                sticky_header: code.sticky_header.unwrap_or(false),
                use_css_classes: code.use_css_classes.unwrap_or(false),
                use_token_transformer: code.use_token_transformer.unwrap_or(false),
                tokenize_max_line_length: code.tokenize_max_line_length.unwrap_or(1000),
                tokenize_max_length: code
                    .tokenize_max_length
                    .unwrap_or(DEFAULT_TOKENIZE_MAX_LENGTH),
                diff_style: base.diff_style.unwrap_or(DiffStyle::Split),
                diff_indicators: base.diff_indicators.unwrap_or(DiffIndicators::Bars),
                disable_background: base.disable_background.unwrap_or(false),
                hunk_separators: base.hunk_separators.unwrap_or(HunkSeparators::LineInfo),
                expand_unchanged: base.expand_unchanged.unwrap_or(false),
                collapsed_context_threshold: base
                    .collapsed_context_threshold
                    .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD),
                line_diff_type: base.line_diff_type.unwrap_or(LineDiffTypes::WordAlt),
                max_line_diff_length: base.max_line_diff_length.unwrap_or(1000),
                expansion_line_count: base.expansion_line_count.unwrap_or(100),
            },
            header_render_mode: self
                .options
                .header_render_mode
                .unwrap_or(FileHeaderRenderMode::Default),
        }
    }

    /// `getRenderOptions(diff).options`.
    fn render_options(&self) -> RenderDiffOptions {
        let defaults = self.options_with_defaults().base;
        RenderDiffOptions {
            theme: defaults.theme,
            use_token_transformer: self.options.base.base.use_token_transformer == Some(true),
            tokenize_max_line_length: defaults.tokenize_max_line_length,
            line_diff_type: defaults.line_diff_type,
            max_line_diff_length: defaults.max_line_diff_length,
        }
    }

    /// `renderDiff`: highlights `diff` and turns the result into the rendered
    /// columns.
    ///
    /// A diff that is empty, plain text or larger than `tokenizeMaxLength` is
    /// rendered as plain text; with a finite `render_range` only that window is
    /// highlighted (the worker's `getPlainDiffAST`, which replaces upstream's
    /// `lazyLineAST`). Anything else is highlighted whole, since tokenization
    /// needs the preceding lines' lexical state.
    pub fn render_diff(
        &self,
        highlighter: &dyn DiffsHighlighter,
        diff: &FileDiffMetadata,
        render_range: &RenderRange,
    ) -> HunksRenderResult {
        let defaults = self.options_with_defaults().base;
        let has_content = !diff.addition_lines.is_empty() || !diff.deletion_lines.is_empty();
        let force_plain_text = !has_content
            || is_diff_plain_text(diff)
            || is_diff_massive(diff, defaults.tokenize_max_length);
        let is_default_range = *render_range == DEFAULT_RENDER_RANGE;
        let plain_text = if force_plain_text {
            ForceDiffPlainTextOptions {
                force_plain_text: true,
                lazy_line_ast: None,
                starting_line: Some(process::line_count(render_range.starting_line)),
                total_lines: render_range
                    .total_lines
                    .is_finite()
                    .then(|| process::line_count(render_range.total_lines)),
                // If we aren't using a windowed render, then we need to render everything
                expanded_hunks: Some(if is_default_range || defaults.expand_unchanged {
                    ExpandedHunks::All
                } else {
                    ExpandedHunks::borrowed(&self.expanded_hunks)
                }),
                collapsed_context_threshold: Some(defaults.collapsed_context_threshold),
            }
        } else {
            ForceDiffPlainTextOptions {
                force_plain_text: false,
                lazy_line_ast: None,
                starting_line: None,
                total_lines: None,
                expanded_hunks: None,
                collapsed_context_threshold: Some(defaults.collapsed_context_threshold),
            }
        };
        let themed =
            render_diff_with_highlighter(diff, highlighter, &self.render_options(), &plain_text);
        self.process_diff_result(diff, render_range, themed)
    }

    /// `createPreElement`.
    pub fn create_pre_element(
        &self,
        split: bool,
        total_lines: usize,
        custom_properties: Option<CustomPreProperties>,
    ) -> Element {
        let defaults = self.options_with_defaults().base;
        create_pre_element(&PrePropertiesConfig {
            diff_indicators: defaults.diff_indicators,
            disable_background: defaults.disable_background,
            disable_line_numbers: defaults.disable_line_numbers,
            overflow: defaults.overflow,
            pre_type: PreType::Diff,
            split,
            total_lines,
            custom_properties,
        })
    }

    /// `renderCodeAST`.
    pub fn render_code_ast(
        &self,
        column: CodeColumnType,
        result: &HunksRenderResult,
    ) -> Option<Vec<ElementContent>> {
        let (gutter_ast, content_ast) = match column {
            CodeColumnType::Unified => (&result.unified_gutter_ast, &result.unified_content_ast),
            CodeColumnType::Deletions => {
                (&result.deletions_gutter_ast, &result.deletions_content_ast)
            }
            CodeColumnType::Additions => {
                (&result.additions_gutter_ast, &result.additions_content_ast)
            }
        };
        let (gutter_ast, content_ast) = (gutter_ast.as_ref()?, content_ast.as_ref()?);
        let mut gutter = create_gutter_wrapper(gutter_ast.clone());
        gutter
            .properties
            .set("style", format!("grid-row: span {}", result.row_count));
        let content_column = create_content_column(content_ast.clone(), result.row_count);
        Some(vec![gutter.into(), content_column.into()])
    }

    /// `renderFullAST`: the `pre` with every column's `code` element, appended
    /// after `children`.
    pub fn render_full_ast(
        &self,
        result: &HunksRenderResult,
        mut children: Vec<ElementContent>,
    ) -> Element {
        let container_size =
            self.options_with_defaults().base.hunk_separators == HunkSeparators::LineInfo;
        let code_element = |ast: Vec<ElementContent>, column_attribute: &str| {
            create_hast_element(
                "code",
                ast,
                Properties::new()
                    .with("data-code", "")
                    .with(
                        "data-container-size",
                        if container_size {
                            PropValue::Str(String::new())
                        } else {
                            PropValue::Undefined
                        },
                    )
                    .with(column_attribute, ""),
            )
        };
        let mut pre = result.pre_node.clone();
        if let Some(unified) = self.render_code_ast(CodeColumnType::Unified, result) {
            children.push(code_element(unified, "data-unified").into());
            pre.children = children;
            return pre;
        }
        if let Some(deletions) = self.render_code_ast(CodeColumnType::Deletions, result) {
            children.push(code_element(deletions, "data-deletions").into());
        }
        if let Some(additions) = self.render_code_ast(CodeColumnType::Additions, result) {
            children.push(code_element(additions, "data-additions").into());
        }
        pre.children = children;
        pre
    }

    fn render_header(&self, diff: &FileDiffMetadata) -> Element {
        let defaults = self.options_with_defaults();
        create_file_header_element(CreateFileHeaderElementProps {
            file_or_diff: FileOrDiff::Diff(diff),
            mode: defaults.header_render_mode,
            sticky_header: defaults.base.sticky_header,
        })
    }
}

fn is_diff_massive(diff: &FileDiffMetadata, tokenize_max_length: usize) -> bool {
    diff.addition_lines.len().max(diff.deletion_lines.len()) > tokenize_max_length
}

fn can_hydrate_collapsed_context(file_diff: &FileDiffMetadata, has_file_loader: bool) -> bool {
    use crate::types::patch::ChangeTypes;
    file_diff.is_partial
        && has_file_loader
        && matches!(
            file_diff.change_type,
            ChangeTypes::Change | ChangeTypes::RenameChanged
        )
}
