// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/FileDiff.ts.
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: PARTIAL port, the options seam (S12a-1): `FileDiffOptions`, the
// theme and renderer-option derivations, the interaction wiring rule,
// `getLineIndexForDiff` and the option-dependent predicates of the class. The
// class body (render, hydrate, annotations, expansion, cleanUp, the DOM
// helpers) follows in later parts; see parity/closure_notes.md.
// * `FileDiffOptions<Element, Rendered, LAnnotation>`: `extends BaseDiffOptions,
//   InteractionManagerBaseOptions<'diff'>` is composition (`base`,
//   `interaction`). The callbacks upstream types as returning
//   `HTMLElement | string | number | null | undefined` return the host's
//   `Rendered` (a hast element, a Dioxus node...). Callbacks are `PtrEq`, so the
//   derived `PartialEq` compares them by identity as `areOptionsEqual` does.
//   `hunkSeparators` replaces `BaseDiffOptions.hunkSeparators`
//   (`Exclude<HunkSeparators, 'custom'> | function`); the function's second
//   parameter (the FileDiff instance) is not passed. `loadDiffFiles` is the flag
//   `has_diff_file_loader` (as in `DiffHunksRendererOptions`).
// * Dropped: `onEditChange`, `onEditComplete` and the `Caret` type parameter
//   (edit mode), the `workerManager` branch of `getTheme` (worker pool),
//   token callbacks (DEFER, see InteractionManager), `renderGutterUtility`'s
//   and `renderAnnotation`'s DOM return types (they return `Rendered`),
//   `onPostRender`'s `instance` argument.
// * `getLineIndexForDiff`: upstream's method takes the diff the caller picked
//   (`getDiffForLineIndex`); here it is a free function over
//   `Option<&FileDiffMetadata>`. Subtractions saturate at zero where
//   JavaScript's `Math.max(.., 0)` clamps.

use crate::constants::default_themes;
use crate::js_collections::PtrEq;
use crate::managers::interaction_manager::GetHoveredLineResult;
use crate::managers::interaction_manager::GetLineIndexUtility;
use crate::managers::interaction_manager::HunkExpandCallback;
use crate::managers::interaction_manager::InteractionManagerBaseOptions;
use crate::managers::interaction_manager::InteractionManagerOptions;
use crate::managers::interaction_manager::InteractionPluckOptions;
use crate::managers::interaction_manager::pluck_interaction_options;
use crate::types::lines::HunkData;
use crate::types::options::BaseDiffOptions;
use crate::types::options::HunkSeparators;
use crate::types::options::PostRenderPhase;
use crate::types::options::ThemeOption;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::HunkContent;
use crate::types::view::DiffLineAnnotation;
use crate::types::view::SelectionSide;
use crate::utils::get_hunk_side_boundaries::get_hunk_side_start_boundary;

pub type RenderHunkSeparatorCallback<Rendered> = PtrEq<dyn Fn(&HunkData) -> Option<Rendered>>;

/// `Exclude<HunkSeparators, 'custom'> | ((hunk, instance) => ...)`
pub enum FileDiffHunkSeparators<Rendered> {
    Simple,
    Metadata,
    LineInfo,
    LineInfoBasic,
    /// Deprecated upstream.
    Custom(RenderHunkSeparatorCallback<Rendered>),
}

impl<Rendered> Clone for FileDiffHunkSeparators<Rendered> {
    fn clone(&self) -> Self {
        match self {
            Self::Simple => Self::Simple,
            Self::Metadata => Self::Metadata,
            Self::LineInfo => Self::LineInfo,
            Self::LineInfoBasic => Self::LineInfoBasic,
            Self::Custom(callback) => Self::Custom(callback.clone()),
        }
    }
}

impl<Rendered> PartialEq for FileDiffHunkSeparators<Rendered> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Simple, Self::Simple)
            | (Self::Metadata, Self::Metadata)
            | (Self::LineInfo, Self::LineInfo)
            | (Self::LineInfoBasic, Self::LineInfoBasic) => true,
            (Self::Custom(first), Self::Custom(second)) => first == second,
            _ => false,
        }
    }
}

pub type RenderHeaderCallback<Rendered> = PtrEq<dyn Fn(&FileDiffMetadata) -> Option<Rendered>>;
pub type RenderAnnotationCallback<Rendered, LAnnotation> =
    PtrEq<dyn Fn(&DiffLineAnnotation<LAnnotation>, &FileDiffMetadata) -> Option<Rendered>>;
pub type RenderGutterUtilityCallback<Rendered> =
    PtrEq<dyn Fn(&dyn Fn() -> Option<GetHoveredLineResult>) -> Option<Rendered>>;
pub type OnPostRenderCallback<Element> = PtrEq<dyn Fn(&Element, PostRenderPhase)>;

/// `FileDiffOptions<LAnnotation, Caret>`
pub struct FileDiffOptions<Element, Rendered, LAnnotation> {
    /// `Omit<BaseDiffOptions, 'hunkSeparators'>`: `base.hunk_separators` is
    /// unused, see `hunk_separators`.
    pub base: BaseDiffOptions,
    pub interaction: InteractionManagerBaseOptions<Element>,
    pub hunk_separators: Option<FileDiffHunkSeparators<Rendered>>,
    pub render_header_prefix: Option<RenderHeaderCallback<Rendered>>,
    pub render_header_filename_suffix: Option<RenderHeaderCallback<Rendered>>,
    pub render_header_metadata: Option<RenderHeaderCallback<Rendered>>,
    pub render_custom_header: Option<RenderHeaderCallback<Rendered>>,
    /// When true, errors during rendering are rethrown instead of being caught
    /// and displayed in the DOM.
    pub disable_error_handling: Option<bool>,
    pub render_annotation: Option<RenderAnnotationCallback<Rendered, LAnnotation>>,
    pub render_gutter_utility: Option<RenderGutterUtilityCallback<Rendered>>,
    pub on_post_render: Option<OnPostRenderCallback<Element>>,
    /// `loadDiffFiles != null`
    pub has_diff_file_loader: bool,
}

impl<Element, Rendered, LAnnotation> Default for FileDiffOptions<Element, Rendered, LAnnotation> {
    /// The constructor default `{ theme: DEFAULT_THEMES }`.
    fn default() -> Self {
        let mut base = BaseDiffOptions::default();
        base.base.theme = Some(ThemeOption::Themes(default_themes()));
        Self {
            base,
            interaction: InteractionManagerBaseOptions::default(),
            hunk_separators: None,
            render_header_prefix: None,
            render_header_filename_suffix: None,
            render_header_metadata: None,
            render_custom_header: None,
            disable_error_handling: None,
            render_annotation: None,
            render_gutter_utility: None,
            on_post_render: None,
            has_diff_file_loader: false,
        }
    }
}

impl<Element, Rendered, LAnnotation> Clone for FileDiffOptions<Element, Rendered, LAnnotation> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            interaction: self.interaction.clone(),
            hunk_separators: self.hunk_separators.clone(),
            render_header_prefix: self.render_header_prefix.clone(),
            render_header_filename_suffix: self.render_header_filename_suffix.clone(),
            render_header_metadata: self.render_header_metadata.clone(),
            render_custom_header: self.render_custom_header.clone(),
            disable_error_handling: self.disable_error_handling,
            render_annotation: self.render_annotation.clone(),
            render_gutter_utility: self.render_gutter_utility.clone(),
            on_post_render: self.on_post_render.clone(),
            has_diff_file_loader: self.has_diff_file_loader,
        }
    }
}

impl<Element, Rendered, LAnnotation> PartialEq for FileDiffOptions<Element, Rendered, LAnnotation> {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base
            && self.interaction == other.interaction
            && self.hunk_separators == other.hunk_separators
            && self.render_header_prefix == other.render_header_prefix
            && self.render_header_filename_suffix == other.render_header_filename_suffix
            && self.render_header_metadata == other.render_header_metadata
            && self.render_custom_header == other.render_custom_header
            && self.disable_error_handling == other.disable_error_handling
            && self.render_annotation == other.render_annotation
            && self.render_gutter_utility == other.render_gutter_utility
            && self.on_post_render == other.on_post_render
            && self.has_diff_file_loader == other.has_diff_file_loader
    }
}

impl<Element, Rendered, LAnnotation> FileDiffOptions<Element, Rendered, LAnnotation> {
    /// `getTheme` without the worker pool: `options.theme ?? DEFAULT_THEMES`.
    pub fn get_theme(&self) -> ThemeOption {
        self.base
            .base
            .theme
            .clone()
            .unwrap_or_else(|| ThemeOption::Themes(default_themes()))
    }

    /// Whether `hunkSeparators` makes the separators expandable:
    /// `typeof hunkSeparators === 'function' || (hunkSeparators ?? 'line-info')
    /// === 'line-info' || hunkSeparators === 'line-info-basic'`, the condition
    /// under which the constructor and `syncInteractionOptions` hand
    /// `handleExpandHunk` to `pluckInteractionOptions`.
    pub fn expands_hunks_on_click(&self) -> bool {
        matches!(
            self.hunk_separators,
            None | Some(
                FileDiffHunkSeparators::Custom(_)
                    | FileDiffHunkSeparators::LineInfo
                    | FileDiffHunkSeparators::LineInfoBasic
            )
        )
    }

    /// `pluckInteractionOptions(this.options, expands ? this.handleExpandHunk :
    /// undefined, this.getLineIndex)`; the two callbacks belong to the instance.
    pub fn interaction_options(
        &self,
        handle_expand_hunk: Option<HunkExpandCallback>,
        get_line_index: Option<GetLineIndexUtility>,
    ) -> InteractionManagerOptions<Element> {
        pluck_interaction_options(
            InteractionPluckOptions {
                base: self.interaction.clone(),
                has_render_gutter_utility: self.render_gutter_utility.is_some(),
            },
            handle_expand_hunk.filter(|_| self.expands_hunks_on_click()),
            get_line_index,
        )
    }

    /// `canPartiallyRender`
    pub fn can_partially_render(
        &self,
        force_render: bool,
        annotations_changed: bool,
        did_content_change: bool,
    ) -> bool {
        !(force_render
            || annotations_changed
            || did_content_change
            || matches!(
                self.hunk_separators,
                Some(FileDiffHunkSeparators::Custom(_))
            ))
    }

    /// `shouldApplyColumnVariables`; `pre_has_merge_conflict` is
    /// `pre.hasAttribute('data-has-merge-conflict')` (merge conflicts are
    /// DEFER, so a host without them passes false).
    pub fn should_apply_column_variables(
        &self,
        overflow: crate::types::options::Overflow,
        line_annotation_count: usize,
        pre_has_merge_conflict: bool,
    ) -> bool {
        if matches!(
            self.hunk_separators,
            Some(FileDiffHunkSeparators::Custom(_))
        ) {
            return true;
        }
        overflow == crate::types::options::Overflow::Scroll
            && (line_annotation_count > 0 || pre_has_merge_conflict)
    }

    /// The `HunkSeparators` the renderer sees (`'custom'` for a function).
    pub fn renderer_hunk_separators(&self) -> Option<HunkSeparators> {
        self.hunk_separators
            .as_ref()
            .map(|separators| match separators {
                FileDiffHunkSeparators::Simple => HunkSeparators::Simple,
                FileDiffHunkSeparators::Metadata => HunkSeparators::Metadata,
                FileDiffHunkSeparators::LineInfo => HunkSeparators::LineInfo,
                FileDiffHunkSeparators::LineInfoBasic => HunkSeparators::LineInfoBasic,
                FileDiffHunkSeparators::Custom(_) => HunkSeparators::Custom,
            })
    }
}

// Resolve source lines against the same diff that produced the rendered
// rows. During an asynchronous replacement, that can be the previous diff.
pub fn get_line_index_for_diff(
    file_diff: Option<&FileDiffMetadata>,
    line_number: usize,
    side: SelectionSide,
) -> Option<(usize, usize)> {
    let file_diff = file_diff?;
    let last_hunk = file_diff.hunks.last();
    let mut target_unified_index: Option<usize> = None;
    let mut target_split_index: Option<usize> = None;
    for hunk in &file_diff.hunks {
        let (hunk_start, hunk_count) = if side == SelectionSide::Deletions {
            (hunk.deletion_start, hunk.deletion_count)
        } else {
            (hunk.addition_start, hunk.addition_count)
        };
        let mut current_line_number = get_hunk_side_start_boundary(hunk_start, hunk_count) + 1;
        let mut split_index = hunk.split_line_start;
        let mut unified_index = hunk.unified_line_start;

        // If we've selected a line between or before a hunk,
        // we should grab its index here
        if line_number < current_line_number {
            let difference = current_line_number - line_number;
            target_unified_index = Some(unified_index.saturating_sub(difference));
            target_split_index = Some(split_index.saturating_sub(difference));
            break;
        }

        if line_number >= current_line_number + hunk_count {
            if last_hunk.is_some_and(|last| std::ptr::eq(last, hunk)) {
                let difference = line_number - (current_line_number + hunk_count);
                target_unified_index = Some(unified_index + hunk.unified_line_count + difference);
                target_split_index = Some(split_index + hunk.split_line_count + difference);
                break;
            }
            continue;
        }

        for content in &hunk.hunk_content {
            match content {
                HunkContent::Context(context) => {
                    if line_number < current_line_number + context.lines {
                        let difference = line_number - current_line_number;
                        target_split_index = Some(split_index + difference);
                        target_unified_index = Some(unified_index + difference);
                        break;
                    }
                    current_line_number += context.lines;
                    split_index += context.lines;
                    unified_index += context.lines;
                }
                HunkContent::Change(change) => {
                    let side_count = if side == SelectionSide::Deletions {
                        change.deletions
                    } else {
                        change.additions
                    };
                    if line_number < current_line_number + side_count {
                        let index_difference = line_number - current_line_number;
                        target_unified_index = Some(
                            unified_index
                                + if side == SelectionSide::Additions {
                                    change.deletions
                                } else {
                                    0
                                }
                                + index_difference,
                        );
                        target_split_index = Some(split_index + index_difference);
                        break;
                    }
                    current_line_number += side_count;
                    split_index += change.deletions.max(change.additions);
                    unified_index += change.deletions + change.additions;
                }
            }
        }

        break;
    }

    Some((target_unified_index?, target_split_index?))
}
