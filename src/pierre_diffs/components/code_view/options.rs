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
//   pass-through keys and callbacks as a `FileDiffOptions` (`diff`) and adds
//   the CodeView-only fields. The keys `CodeViewDiffOptionKeys` leaves out are
//   mapped, not passed through, so `diff` must not set them: `stickyHeader`
//   (CodeView's `stickyHeaders`), `collapsed` (the item's) and
//   `hunkSeparators` (CodeView's own field, which cannot be `custom`:
//   `CodeViewHunkSeparators`). `validate` panics on a violation (an invariant
//   of the caller, not input).
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
use crate::types::layout::CodeViewLayout;
use crate::types::layout::SmoothScrollSettings;
use crate::types::layout::VirtualFileMetrics;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::options::ThemeOption;
use crate::types::options::ThemeTypes;
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

/// `renderCodeViewHeader` / `renderCodeViewFooter`: compared by identity (the
/// host is re-populated when the reference changes).
pub type RenderCodeViewSlotCallback<Rendered> = PtrEq<dyn Fn() -> Option<Rendered>>;

/// `onSelectedLinesChange`
pub type SelectedLinesChangeCallback = Rc<dyn Fn(Option<&CodeViewLineSelection>)>;

/// `CodeViewOptions<LAnnotation, Caret>`
pub struct CodeViewOptions<Rendered, LAnnotation> {
    /// The keys and callbacks CodeView passes through to its items, see the
    /// header for which fields of it must stay unset.
    pub diff: FileDiffOptions<NodeKey, Rendered, LAnnotation>,
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
    /// Panics when `diff` sets a key CodeView maps itself.
    pub fn validate(&self) {
        assert!(
            self.diff.base.base.sticky_header.is_none()
                && self.diff.base.base.collapsed.is_none()
                && self.diff.hunk_separators.is_none(),
            "CodeViewOptions.diff must not set stickyHeader, collapsed or hunkSeparators: use sticky_headers, the item's collapsed and hunk_separators"
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
