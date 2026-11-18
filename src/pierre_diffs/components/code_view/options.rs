// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C2: `CodeViewOptions` and the option keys, lines 71-640; `shouldClearPool`,
// `hasItemLayoutOptionChanged`, `hasCodeViewDiffEstimateOptionChanged`,
// 4466-4524; `computeMetricsCache`, 953-970).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * The TypeScript option machinery (`CODE_VIEW_DIFF_OPTION_KEYS`, the
//   `Pick<FileDiffOptions, ...>` pass-through, the callback-type conditionals
//   that append the item context) is a struct: `CodeViewOptions` embeds the
//   pass-through keys as a `FileDiffOptions` (`diff`) and adds the
//   CodeView-only fields. The keys `CodeViewDiffOptionKeys` leaves out are
//   mapped, not passed through, so `diff` must not set them: `stickyHeader`
//   (CodeView's `stickyHeaders`), `collapsed` (the item's) and
//   `hunkSeparators` (CodeView's own field, which cannot be `custom`:
//   `CodeViewHunkSeparators`). The callbacks (`CodeViewSharedCallbackKeys`,
//   `CodeViewSelectionCallbackKeys`) are `callbacks`, typed as the `FileDiff`
//   callback plus the trailing item context (`CodeViewOptionCallback`), so
//   `diff` must not set those either. `validate` panics on a violation (an
//   invariant of the caller, not input).
// * The item context handed to a callback is a snapshot
//   (`CodeViewDiffItemContext`), not the live record: no borrow of CodeView's
//   state may be held across a user callback. It omits `instance`, which
//   would parameterise every option type over the host (parity/divergences.psv).
// * Object-valued options (`layout`, `itemMetrics`, `smoothScrollSettings`)
//   compare by value in `CodeViewOptions::is_equal` (`areOptionsEqual`
//   compares them by reference upstream): an equal-valued fresh object does
//   not bump `renderOptionsRevision` here (parity/divergences.psv).
// * Dropped: edit mode (`getEditStateKey`, `createEditor`, `onItemEdit*`,
//   `onEditComplete`) and `__devOnlyValidateItemHeights`; `onToken*` callbacks
//   have no `InteractionManager` counterpart (the port omits token
//   interactions). `renderCodeViewHeader`/`Footer` return `Rendered` (the slot
//   content of `slot.rs`) instead of an `HTMLElement`.
// * `onSelectedLinesChange`'s `CodeViewLineSelection | null` is
//   `Option<&CodeViewLineSelection>`.

use std::rc::Rc;

use super::state::CodeViewLineSelection;
use crate::components::file_diff::FileDiffHunkSeparators;
use crate::components::file_diff::FileDiffOptions;
use crate::constants::DEFAULT_CODE_VIEW_FILE_METRICS;
use crate::constants::DEFAULT_CODE_VIEW_LAYOUT;
use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::constants::DEFAULT_SMOOTH_SCROLL_SETTINGS;
use crate::constants::default_themes;
use crate::document::NodeKey;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::GetHoveredLineResult;
use crate::managers::interaction_manager::LineEventProps;
use crate::types::layout::CodeViewLayout;
use crate::types::layout::SmoothScrollSettings;
use crate::types::layout::VirtualFileMetrics;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::options::PostRenderPhase;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::CodeViewDiffItem;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectedLineRange;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use crate::utils::predicates::are_options_equal::are_options_equal;
use crate::utils::predicates::are_themes_equal::are_themes_equal;

/// `Exclude<HunkSeparators, 'custom'>`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodeViewHunkSeparators {
    Simple,
    Metadata,
    LineInfo,
    LineInfoBasic,
}

impl<Rendered> From<CodeViewHunkSeparators> for FileDiffHunkSeparators<Rendered> {
    fn from(separators: CodeViewHunkSeparators) -> Self {
        match separators {
            CodeViewHunkSeparators::Simple => Self::Simple,
            CodeViewHunkSeparators::Metadata => Self::Metadata,
            CodeViewHunkSeparators::LineInfo => Self::LineInfo,
            CodeViewHunkSeparators::LineInfoBasic => Self::LineInfoBasic,
        }
    }
}

/// `CodeViewDiffItemContext`: the item record a callback was made for, as the
/// callback runs. A snapshot (see the header); `instance` is omitted.
#[derive(Debug, Clone, PartialEq)]
pub struct CodeViewDiffItemContext<LAnnotation> {
    /// Latest item snapshot for this record. Controlled updates can replace it.
    pub item: CodeViewDiffItem<LAnnotation>,
    /// Current index of this record in the ordered items array.
    pub index: usize,
    /// Absolute top offset of this item inside the scroll content.
    pub top: f64,
    /// Total measured height reserved for this item.
    pub height: f64,
    /// Root `<diffs-container>` node currently mounted for this item, only
    /// exists when rendered.
    pub element: Option<NodeKey>,
    /// Last controlled version observed for this record.
    pub version: Option<usize>,
    /// Last CodeView option revision this item rendered with.
    pub rendered_options_revision: i64,
}

/// `CodeViewOptionCallback` for a `FileDiff` callback taking one argument by
/// reference (`A`): the same arguments plus the item context as the trailing
/// one. Compared by identity, as callbacks are in `areOptionsEqual`.
pub type ItemCallback<LAnnotation, A, R = ()> =
    PtrEq<dyn Fn(&A, &CodeViewDiffItemContext<LAnnotation>) -> R>;

/// `CodeViewOptionCallback` for a `FileDiff` callback taking two arguments by
/// reference.
pub type ItemCallback2<LAnnotation, A, B, R = ()> =
    PtrEq<dyn Fn(&A, &B, &CodeViewDiffItemContext<LAnnotation>) -> R>;

/// `CodeViewOptionCallback` for the four selection callbacks
/// (`onLineSelected`, `onLineSelectionStart`, `onLineSelectionChange`,
/// `onLineSelectionEnd`): the range, `None` when cleared, plus the item
/// context.
pub type ItemSelectionCallback<LAnnotation> =
    PtrEq<dyn Fn(Option<&SelectedLineRange>, &CodeViewDiffItemContext<LAnnotation>)>;

/// `CodeViewOptionCallback` for `renderGutterUtility`. Written out rather than
/// an `ItemCallback`: as a generic argument the `dyn Fn` would be `'static`,
/// which the hovered-line getter the instance hands over is not.
pub type ItemGutterUtilityCallback<LAnnotation, Rendered> = PtrEq<
    dyn Fn(
        &dyn Fn() -> Option<GetHoveredLineResult>,
        &CodeViewDiffItemContext<LAnnotation>,
    ) -> Option<Rendered>,
>;

/// The `FileDiff` callbacks CodeView wraps per item
/// (`CodeViewSharedCallbackKeys` and `CodeViewSelectionCallbackKeys`), typed
/// with the item context. Where the `FileDiff` callback takes its one
/// argument by value (`onGutterUtilityClick`'s range, `onPostRender`'s phase)
/// this one takes it by reference, which is what keeps `ItemCallback` and
/// `ItemCallback2` the only shapes besides the selection one.
pub struct CodeViewCallbacks<Rendered, LAnnotation> {
    pub render_custom_header: Option<ItemCallback<LAnnotation, FileDiffMetadata, Option<Rendered>>>,
    pub render_header_prefix: Option<ItemCallback<LAnnotation, FileDiffMetadata, Option<Rendered>>>,
    pub render_header_filename_suffix:
        Option<ItemCallback<LAnnotation, FileDiffMetadata, Option<Rendered>>>,
    pub render_header_metadata:
        Option<ItemCallback<LAnnotation, FileDiffMetadata, Option<Rendered>>>,
    pub render_annotation: Option<
        ItemCallback2<
            LAnnotation,
            DiffLineAnnotation<LAnnotation>,
            FileDiffMetadata,
            Option<Rendered>,
        >,
    >,
    pub render_gutter_utility: Option<ItemGutterUtilityCallback<LAnnotation, Rendered>>,
    pub on_post_render: Option<ItemCallback2<LAnnotation, NodeKey, PostRenderPhase>>,
    pub on_gutter_utility_click: Option<ItemCallback<LAnnotation, SelectedLineRange>>,
    pub on_line_click: Option<ItemCallback<LAnnotation, LineEventProps<NodeKey>>>,
    pub on_line_number_click: Option<ItemCallback<LAnnotation, LineEventProps<NodeKey>>>,
    pub on_line_enter: Option<ItemCallback<LAnnotation, LineEventProps<NodeKey>>>,
    pub on_line_leave: Option<ItemCallback<LAnnotation, LineEventProps<NodeKey>>>,
    pub on_line_selected: Option<ItemSelectionCallback<LAnnotation>>,
    pub on_line_selection_start: Option<ItemSelectionCallback<LAnnotation>>,
    pub on_line_selection_change: Option<ItemSelectionCallback<LAnnotation>>,
    pub on_line_selection_end: Option<ItemSelectionCallback<LAnnotation>>,
}

impl<Rendered, LAnnotation> Default for CodeViewCallbacks<Rendered, LAnnotation> {
    fn default() -> Self {
        Self {
            render_custom_header: None,
            render_header_prefix: None,
            render_header_filename_suffix: None,
            render_header_metadata: None,
            render_annotation: None,
            render_gutter_utility: None,
            on_post_render: None,
            on_gutter_utility_click: None,
            on_line_click: None,
            on_line_number_click: None,
            on_line_enter: None,
            on_line_leave: None,
            on_line_selected: None,
            on_line_selection_start: None,
            on_line_selection_change: None,
            on_line_selection_end: None,
        }
    }
}

impl<Rendered, LAnnotation> Clone for CodeViewCallbacks<Rendered, LAnnotation> {
    fn clone(&self) -> Self {
        Self {
            render_custom_header: self.render_custom_header.clone(),
            render_header_prefix: self.render_header_prefix.clone(),
            render_header_filename_suffix: self.render_header_filename_suffix.clone(),
            render_header_metadata: self.render_header_metadata.clone(),
            render_annotation: self.render_annotation.clone(),
            render_gutter_utility: self.render_gutter_utility.clone(),
            on_post_render: self.on_post_render.clone(),
            on_gutter_utility_click: self.on_gutter_utility_click.clone(),
            on_line_click: self.on_line_click.clone(),
            on_line_number_click: self.on_line_number_click.clone(),
            on_line_enter: self.on_line_enter.clone(),
            on_line_leave: self.on_line_leave.clone(),
            on_line_selected: self.on_line_selected.clone(),
            on_line_selection_start: self.on_line_selection_start.clone(),
            on_line_selection_change: self.on_line_selection_change.clone(),
            on_line_selection_end: self.on_line_selection_end.clone(),
        }
    }
}

impl<Rendered, LAnnotation> PartialEq for CodeViewCallbacks<Rendered, LAnnotation> {
    /// Every callback by identity.
    fn eq(&self, other: &Self) -> bool {
        self.render_custom_header == other.render_custom_header
            && self.render_header_prefix == other.render_header_prefix
            && self.render_header_filename_suffix == other.render_header_filename_suffix
            && self.render_header_metadata == other.render_header_metadata
            && self.render_annotation == other.render_annotation
            && self.render_gutter_utility == other.render_gutter_utility
            && self.on_post_render == other.on_post_render
            && self.on_gutter_utility_click == other.on_gutter_utility_click
            && self.on_line_click == other.on_line_click
            && self.on_line_number_click == other.on_line_number_click
            && self.on_line_enter == other.on_line_enter
            && self.on_line_leave == other.on_line_leave
            && self.on_line_selected == other.on_line_selected
            && self.on_line_selection_start == other.on_line_selection_start
            && self.on_line_selection_change == other.on_line_selection_change
            && self.on_line_selection_end == other.on_line_selection_end
    }
}

/// `renderCodeViewHeader` / `renderCodeViewFooter`: compared by identity (the
/// host is re-populated when the reference changes).
pub type RenderCodeViewSlotCallback<Rendered> = PtrEq<dyn Fn() -> Option<Rendered>>;

/// `onSelectedLinesChange`
pub type SelectedLinesChangeCallback = Rc<dyn Fn(Option<&CodeViewLineSelection>)>;

/// `CodeViewOptions<LAnnotation, Caret>`
pub struct CodeViewOptions<Rendered, LAnnotation> {
    /// The keys CodeView passes through to its items, see the header for
    /// which fields of it must stay unset.
    pub diff: FileDiffOptions<NodeKey, Rendered, LAnnotation>,
    /// The callbacks CodeView wraps per item, see [`CodeViewCallbacks`].
    pub callbacks: CodeViewCallbacks<Rendered, LAnnotation>,
    pub hunk_separators: Option<CodeViewHunkSeparators>,
    pub item_metrics: Option<PartialVirtualFileMetrics>,
    pub pointer_events_on_scroll: Option<bool>,
    pub smooth_scroll_settings: Option<SmoothScrollSettings>,
    pub sticky_headers: Option<bool>,
    pub on_selected_lines_change: Option<SelectedLinesChangeCallback>,
    pub layout: Option<CodeViewLayout>,
    /// Render a non-virtualized element at the very start of the scroll
    /// content, before the first item.
    pub render_code_view_header: Option<RenderCodeViewSlotCallback<Rendered>>,
    /// Render a non-virtualized element at the very end of the scroll content.
    pub render_code_view_footer: Option<RenderCodeViewSlotCallback<Rendered>>,
}

impl<Rendered, LAnnotation> Default for CodeViewOptions<Rendered, LAnnotation> {
    /// The constructor default `{ theme: DEFAULT_THEMES }`.
    fn default() -> Self {
        Self {
            diff: FileDiffOptions::default(),
            callbacks: CodeViewCallbacks::default(),
            hunk_separators: None,
            item_metrics: None,
            pointer_events_on_scroll: None,
            smooth_scroll_settings: None,
            sticky_headers: None,
            on_selected_lines_change: None,
            layout: None,
            render_code_view_header: None,
            render_code_view_footer: None,
        }
    }
}

impl<Rendered, LAnnotation> Clone for CodeViewOptions<Rendered, LAnnotation> {
    fn clone(&self) -> Self {
        Self {
            diff: self.diff.clone(),
            callbacks: self.callbacks.clone(),
            hunk_separators: self.hunk_separators,
            item_metrics: self.item_metrics,
            pointer_events_on_scroll: self.pointer_events_on_scroll,
            smooth_scroll_settings: self.smooth_scroll_settings,
            sticky_headers: self.sticky_headers,
            on_selected_lines_change: self.on_selected_lines_change.clone(),
            layout: self.layout,
            render_code_view_header: self.render_code_view_header.clone(),
            render_code_view_footer: self.render_code_view_footer.clone(),
        }
    }
}

impl<Rendered, LAnnotation> CodeViewOptions<Rendered, LAnnotation> {
    /// Panics when `diff` sets a key CodeView maps itself or a callback it
    /// wraps per item.
    pub fn validate(&self) {
        assert!(
            self.diff.base.base.sticky_header.is_none()
                && self.diff.base.base.collapsed.is_none()
                && self.diff.hunk_separators.is_none(),
            "CodeViewOptions.diff must not set stickyHeader, collapsed or hunkSeparators: use sticky_headers, the item's collapsed and hunk_separators"
        );
        let diff = &self.diff;
        let interaction = &diff.interaction;
        assert!(
            diff.render_custom_header.is_none()
                && diff.render_header_prefix.is_none()
                && diff.render_header_filename_suffix.is_none()
                && diff.render_header_metadata.is_none()
                && diff.render_annotation.is_none()
                && diff.render_gutter_utility.is_none()
                && diff.on_post_render.is_none()
                && interaction.on_gutter_utility_click.is_none()
                && interaction.on_line_click.is_none()
                && interaction.on_line_number_click.is_none()
                && interaction.on_line_enter.is_none()
                && interaction.on_line_leave.is_none()
                && interaction.on_line_selected.is_none()
                && interaction.on_line_selection_start.is_none()
                && interaction.on_line_selection_change.is_none()
                && interaction.on_line_selection_end.is_none(),
            "CodeViewOptions.diff must not set the callbacks CodeView wraps per item: use CodeViewOptions.callbacks, which receive the item context"
        );
    }

    /// `getLayout()`'s read: `this.options.layout ?? DEFAULT_CODE_VIEW_LAYOUT`.
    pub fn layout_or_default(&self) -> CodeViewLayout {
        self.layout.unwrap_or(DEFAULT_CODE_VIEW_LAYOUT)
    }

    /// `getSmoothScrollSettings()`
    pub fn smooth_scroll_settings_or_default(&self) -> SmoothScrollSettings {
        self.smooth_scroll_settings
            .unwrap_or(DEFAULT_SMOOTH_SCROLL_SETTINGS)
    }

    /// `shouldDisablePointerEvents()`
    pub fn should_disable_pointer_events(&self) -> bool {
        self.pointer_events_on_scroll != Some(true)
    }

    /// `areOptionsEqual(optionsA, optionsB)` over the whole object: the
    /// pass-through keys as `areOptionsEqual`, every other field by value
    /// (callbacks by identity), see the header.
    pub fn is_equal(&self, other: &Self) -> bool {
        let Self {
            diff,
            callbacks,
            hunk_separators,
            item_metrics,
            pointer_events_on_scroll,
            smooth_scroll_settings,
            sticky_headers,
            on_selected_lines_change,
            layout,
            render_code_view_header,
            render_code_view_footer,
        } = self;
        are_options_equal(Some(diff), Some(&other.diff))
            && *callbacks == other.callbacks
            && *hunk_separators == other.hunk_separators
            && *item_metrics == other.item_metrics
            && *pointer_events_on_scroll == other.pointer_events_on_scroll
            && *smooth_scroll_settings == other.smooth_scroll_settings
            && *sticky_headers == other.sticky_headers
            && match (on_selected_lines_change, &other.on_selected_lines_change) {
                (Some(first), Some(second)) => {
                    std::ptr::addr_eq(Rc::as_ptr(first), Rc::as_ptr(second))
                }
                (None, None) => true,
                _ => false,
            }
            && *layout == other.layout
            && *render_code_view_header == other.render_code_view_header
            && *render_code_view_footer == other.render_code_view_footer
    }

    fn theme(&self) -> ThemeOption {
        self.diff
            .base
            .base
            .theme
            .clone()
            .unwrap_or_else(|| ThemeOption::Themes(default_themes()))
    }
}

/// `computeMetricsCache`: `itemMetrics` over `DEFAULT_CODE_VIEW_FILE_METRICS`
/// (the three optional metrics have no default).
pub fn compute_item_metrics(item_metrics: Option<PartialVirtualFileMetrics>) -> VirtualFileMetrics {
    let item_metrics = item_metrics.unwrap_or_default();
    VirtualFileMetrics {
        hunk_line_count: item_metrics
            .hunk_line_count
            .unwrap_or(DEFAULT_CODE_VIEW_FILE_METRICS.hunk_line_count),
        line_height: item_metrics
            .line_height
            .unwrap_or(DEFAULT_CODE_VIEW_FILE_METRICS.line_height),
        diff_header_height: item_metrics
            .diff_header_height
            .unwrap_or(DEFAULT_CODE_VIEW_FILE_METRICS.diff_header_height),
        hunk_separator_height: item_metrics.hunk_separator_height,
        spacing: item_metrics
            .spacing
            .unwrap_or(DEFAULT_CODE_VIEW_FILE_METRICS.spacing),
        padding_top: item_metrics.padding_top,
        padding_bottom: item_metrics.padding_bottom,
    }
}

/// `shouldClearPool`: the options a pooled shell's theme and CSS assets depend on.
pub fn should_clear_pool<Rendered, LAnnotation>(
    previous: &CodeViewOptions<Rendered, LAnnotation>,
    next: &CodeViewOptions<Rendered, LAnnotation>,
) -> bool {
    !are_themes_equal(Some(&previous.theme()), Some(&next.theme()))
        || previous
            .diff
            .base
            .base
            .theme_type
            .unwrap_or(ThemeTypes::System)
            != next.diff.base.base.theme_type.unwrap_or(ThemeTypes::System)
        || previous.diff.base.base.unsafe_css != next.diff.base.base.unsafe_css
}

/// `hasItemLayoutOptionChanged`
pub fn has_item_layout_option_changed<Rendered, LAnnotation>(
    previous: &CodeViewOptions<Rendered, LAnnotation>,
    next: &CodeViewOptions<Rendered, LAnnotation>,
) -> bool {
    let (previous_base, next_base) = (&previous.diff.base, &next.diff.base);
    previous_base.base.overflow.unwrap_or(Overflow::Scroll)
        != next_base.base.overflow.unwrap_or(Overflow::Scroll)
        || previous_base.base.disable_line_numbers.unwrap_or(false)
            != next_base.base.disable_line_numbers.unwrap_or(false)
        || previous_base.base.disable_file_header.unwrap_or(false)
            != next_base.base.disable_file_header.unwrap_or(false)
        || previous_base.base.unsafe_css != next_base.base.unsafe_css
        || previous_base.diff_style.unwrap_or(DiffStyle::Split)
            != next_base.diff_style.unwrap_or(DiffStyle::Split)
        || previous_base
            .diff_indicators
            .unwrap_or(DiffIndicators::Bars)
            != next_base.diff_indicators.unwrap_or(DiffIndicators::Bars)
        || has_code_view_hunk_separators_changed(previous, next)
        || previous_base.expand_unchanged.unwrap_or(false)
            != next_base.expand_unchanged.unwrap_or(false)
        || previous_base
            .collapsed_context_threshold
            .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
            != next_base
                .collapsed_context_threshold
                .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
}

/// `hasCodeViewDiffEstimateOptionChanged`
pub fn has_code_view_diff_estimate_option_changed<Rendered, LAnnotation>(
    previous: &CodeViewOptions<Rendered, LAnnotation>,
    next: &CodeViewOptions<Rendered, LAnnotation>,
) -> bool {
    let (previous_base, next_base) = (&previous.diff.base, &next.diff.base);
    previous_base.base.disable_file_header.unwrap_or(false)
        != next_base.base.disable_file_header.unwrap_or(false)
        || has_code_view_hunk_separators_changed(previous, next)
        || previous_base.expand_unchanged.unwrap_or(false)
            != next_base.expand_unchanged.unwrap_or(false)
        || previous_base
            .collapsed_context_threshold
            .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
            != next_base
                .collapsed_context_threshold
                .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
}

/// `(previousOptions.hunkSeparators ?? 'line-info') !== (nextOptions.hunkSeparators ?? 'line-info')`
fn has_code_view_hunk_separators_changed<Rendered, LAnnotation>(
    previous: &CodeViewOptions<Rendered, LAnnotation>,
    next: &CodeViewOptions<Rendered, LAnnotation>,
) -> bool {
    previous
        .hunk_separators
        .unwrap_or(CodeViewHunkSeparators::LineInfo)
        != next
            .hunk_separators
            .unwrap_or(CodeViewHunkSeparators::LineInfo)
}
