// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/VirtualizedFileDiff.ts
// (S12b-1: the layout model, upstream lines 76-360 and 1263-1395, 1592-1965 and 2200-2451).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes: upstream's layout methods are methods of the class and read
// `this.options`, `this.hunksRenderer.getExpandedHunksMap()`, `getLayoutDiff()`
// (`pendingRender?.diff ?? getRenderedDiff()`) and the virtualizer's
// `config.resizeDebugging`. Here they are methods of `DiffLayout`, the
// instance's layout state (`top`, `height`, `metrics`, `cache`, `layoutDirty`),
// and what the class reads through `this` arrives as a `LayoutContext` argument
// that the instance builds from its options, its renderer and its pending
// render; `fileDiff: FileDiffMetadata | undefined = this.getLayoutDiff()`
// default parameters are `Option<&FileDiffMetadata>` falling back to
// `LayoutContext::layout_diff` (JavaScript's default applies to an explicit
// `undefined` as well). The instance fields the layout methods write outside the
// layout (`renderRange = undefined` in `invalidateDerivedLayoutCache`) are
// returned as a flag the caller applies (`#[must_use]`): `reset_layout_cache`,
// `invalidate_derived_layout_cache` and `set_metrics` return whether the render
// range must be cleared. Not ported (see dropped.psv): the advanced (CodeView)
// mode, so `isSimpleMode()` is always true and `forceSimpleRecompute` alone
// decides the recompute in `resetLayoutCache`; the ghost text rows
// (`cache.ghostTextRows*`, `applyGhostTextRows`) and `this.editor`;
// `currentCollapsed` (read only by `updateCodeViewLayout`); `validateComputedHeight`
// (a `console.log` of the container's measured height) so a resize-debugging run
// recomputes as upstream does and logs nothing; `getNumericScrollAnchor` (read
// only by `CodeView`, see dropped.psv); and `measureFileAnnotationHeight`, a DOM
// read that ports with the measurement (S12b-2). Line indexes are `i64`
// (upstream's numbers: `startLineIndex` is computed by subtraction and unread
// when its block is empty), row counts are `usize`. `processRows` is a closure
// over `top` and `renderedLineIndex` upstream; here it is `CheckpointBuilder`'s
// method over the same two variables so that `approximateLayoutCheckpoints` stays
// inside the function budget, and the per-hunk body of its loop is
// `CheckpointBuilder::process_hunk`. `getLayoutCheckpointBeforeTop`'s
// `renderedLineIndex % hunkLineCount === 0` is `checked_rem`: a zero
// `hunkLineCount` is `NaN` in JavaScript and never matches, and here yields no
// checkpoint too. `getTrailingExpandedRegion(hunkIndex: hunks.length - 1)` of a
// diff without hunks (index -1, which finds no trailing context) is skipped. The
// `Math.max(hunk.collapsedBefore, 0)` clamp is the type (`usize`). The
// `fileDiff.hunks[hunkIndex] == null` throw is unreachable by construction.
// `getHunkMetadataOffsets`' unified `rowCount - unifiedCount` panics when the
// last change block does not fit its hunk's rows (a malformed hunk).
// `areOptionalFilesEqual` is `are_files_equal` (which already treats an absent
// file as equal only to an absent file) behind upstream's name.

use std::collections::HashMap;

use crate::components::file_diff::FileDiffHunkSeparators;
use crate::components::file_diff::FileDiffOptions;
use crate::constants::DEFAULT_COLLAPSED_CONTEXT_THRESHOLD;
use crate::types::layout::VirtualFileMetrics;
use crate::types::lines::ExpandedHunks;
use crate::types::options::DiffIndicators;
use crate::types::options::DiffStyle;
use crate::types::options::HunkSeparators;
use crate::types::options::Overflow;
use crate::types::patch::ChangeTypes;
use crate::types::patch::FileContents;
use crate::types::patch::FileDiffMetadata;
use crate::types::patch::Hunk;
use crate::types::patch::HunkContent;
use crate::types::view::AnnotationSide;
use crate::types::view::DiffLineAnnotation;
use crate::utils::compute_estimated_diff_heights::ComputeEstimatedDiffHeightsOptions;
use crate::utils::compute_estimated_diff_heights::compute_estimated_diff_heights;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use crate::utils::compute_virtual_file_metrics::compute_virtual_file_metrics;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_height;
use crate::utils::compute_virtual_file_metrics::get_virtual_file_header_region;
use crate::utils::predicates::are_files_equal::are_files_equal;
use crate::utils::render::includes_file_annotations::FILE_ANNOTATION_LINE_NUMBER;
use crate::utils::render::includes_file_annotations::includes_file_annotations;
use crate::utils::virtual_diff_layout::GetExpandedRegionProps;
use crate::utils::virtual_diff_layout::GetTrailingExpandedRegionProps;
use crate::utils::virtual_diff_layout::HunkSeparatorBaseProps;
use crate::utils::virtual_diff_layout::get_expanded_region;
use crate::utils::virtual_diff_layout::get_leading_hunk_separator_layout;
use crate::utils::virtual_diff_layout::get_trailing_expanded_region;
use crate::utils::virtual_diff_layout::get_trailing_hunk_separator_layout;

pub const VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL: usize = 3_000;

/// `DiffLayoutCheckpoint`
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiffLayoutCheckpoint {
    pub rendered_line_index: usize,
    pub line_index: i64,
    pub top: f64,
}

/// `DiffLayoutCache` (without the ghost text rows, see the header).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DiffLayoutCache {
    /// Sparse map: view-specific line index -> measured height delta from the
    /// baseline line height. Only stores lines that differ from the estimate.
    pub height_deltas: HashMap<i64, f64>,
    /// Aggregate delta from estimated height, including source row deltas and the
    /// zero-baseline file annotation row height.
    pub measured_height_delta_total: f64,
    /// Baseline estimated heights for the active diff content. These are preserved
    /// across style/collapse toggles and cleared only when estimate inputs change.
    pub estimated_split_height: Option<f64>,
    pub estimated_unified_height: Option<f64>,
    /// Sparse measured positions used to resume deep geometry scans near a target
    /// diff line, rendered row, or scroll offset instead of replaying layout from
    /// the first hunk.
    pub checkpoints: Vec<DiffLayoutCheckpoint>,
    /// Total renderable diff rows for the current diff style and expansion state.
    pub total_lines: usize,
    /// Measured height for the file annotation row. Starts at 0 so
    /// unmeasured annotations behave like all other unmeasured annotations.
    pub file_annotation_height: f64,
}

/// `ResetLayoutCacheOptions`, with upstream's defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResetLayoutCacheOptions {
    pub force_simple_recompute: bool,
    pub include_estimated_heights: bool,
    pub reset_render_range: bool,
}

impl Default for ResetLayoutCacheOptions {
    fn default() -> Self {
        Self {
            force_simple_recompute: false,
            include_estimated_heights: false,
            reset_render_range: true,
        }
    }
}

/// The options the layout reads from `this.options`, defaults applied.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LayoutOptions {
    pub disable_file_header: bool,
    pub collapsed: bool,
    pub expand_unchanged: bool,
    pub collapsed_context_threshold: usize,
    pub diff_style: DiffStyle,
    /// `getHunkSeparatorType()`
    pub hunk_separators: HunkSeparators,
    /// `this.options.loadDiffFiles != null`
    pub has_file_loader: bool,
}

impl LayoutOptions {
    pub fn of<Element, Rendered, LAnnotation>(
        options: &FileDiffOptions<Element, Rendered, LAnnotation>,
    ) -> Self {
        Self {
            disable_file_header: options.base.base.disable_file_header.unwrap_or(false),
            collapsed: options.base.base.collapsed.unwrap_or(false),
            expand_unchanged: options.base.expand_unchanged.unwrap_or(false),
            collapsed_context_threshold: options
                .base
                .collapsed_context_threshold
                .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD),
            diff_style: options.base.diff_style.unwrap_or(DiffStyle::Split),
            hunk_separators: get_option_hunk_separator_type(options.hunk_separators.as_ref()),
            has_file_loader: options.has_diff_file_loader,
        }
    }
}

/// What the layout methods read through `this` upstream.
#[derive(Debug, Clone, Copy)]
pub struct LayoutContext<'ctx> {
    pub options: LayoutOptions,
    /// `this.hunksRenderer.getExpandedHunksMap()`, as `ExpandedHunks::Regions`.
    pub expanded_hunks: &'ctx ExpandedHunks,
    /// `getLayoutDiff()`: `pendingRender?.diff ?? getRenderedDiff()`.
    pub layout_diff: Option<&'ctx FileDiffMetadata>,
    /// `this.getSimpleVirtualizer()?.config.resizeDebugging ?? false`
    pub resize_debugging: bool,
}

impl LayoutContext<'_> {
    /// `expandUnchanged ? true : this.hunksRenderer.getExpandedHunksMap()`
    fn effective_expanded_hunks(&self) -> &ExpandedHunks {
        if self.options.expand_unchanged {
            &ExpandedHunks::All
        } else {
            self.expanded_hunks
        }
    }
}

/// The layout state of a `VirtualizedFileDiff`: its position, its estimated and
/// measured height, and the cache the height is derived from.
#[derive(Debug, Clone)]
pub struct DiffLayout {
    pub top: Option<f64>,
    pub height: f64,
    pub metrics: VirtualFileMetrics,
    pub cache: DiffLayoutCache,
    pub layout_dirty: bool,
}

impl DiffLayout {
    /// The constructor's `this.metrics = computeVirtualFileMetrics(metrics)` and
    /// the field initialisers.
    pub fn new(metrics: PartialVirtualFileMetrics) -> Self {
        Self {
            top: None,
            height: 0.0,
            metrics: compute_virtual_file_metrics(metrics),
            cache: DiffLayoutCache::default(),
            layout_dirty: true,
        }
    }

    /// `setMetrics`. Returns whether the render range must be cleared.
    #[must_use]
    pub fn set_metrics(
        &mut self,
        metrics: PartialVirtualFileMetrics,
        force: bool,
        ctx: &LayoutContext<'_>,
    ) -> bool {
        let next_metrics = compute_virtual_file_metrics(metrics);
        if !force && self.metrics == next_metrics {
            return false;
        }

        self.metrics = next_metrics;
        self.reset_layout_cache(
            ResetLayoutCacheOptions {
                include_estimated_heights: true,
                ..ResetLayoutCacheOptions::default()
            },
            ctx,
        )
    }

    pub fn set_file_annotation_height(&mut self, next_height: f64) -> bool {
        let previous_height = self.cache.file_annotation_height;
        if next_height == previous_height {
            return false;
        }

        self.cache.file_annotation_height = next_height;
        self.cache.measured_height_delta_total += next_height - previous_height;
        true
    }

    /// Get the height for a line, using cached value if available.
    /// If not cached and has_metadata_line is true, adds lineHeight for the metadata.
    pub fn get_line_height(&self, line_index: i64, has_metadata_line: bool) -> f64 {
        self.get_estimated_line_height(has_metadata_line)
            + self
                .cache
                .height_deltas
                .get(&line_index)
                .copied()
                .unwrap_or(0.0)
    }

    pub fn get_estimated_line_height(&self, has_metadata_line: bool) -> f64 {
        let multiplier = if has_metadata_line { 2.0 } else { 1.0 };
        self.metrics.line_height * multiplier
    }

    /// `resetLayoutCache`. Returns whether the render range must be cleared.
    #[must_use]
    pub fn reset_layout_cache(
        &mut self,
        options: ResetLayoutCacheOptions,
        ctx: &LayoutContext<'_>,
    ) -> bool {
        let ResetLayoutCacheOptions {
            force_simple_recompute,
            include_estimated_heights,
            reset_render_range,
        } = options;
        self.layout_dirty = true;
        self.cache.file_annotation_height = 0.0;
        self.cache.height_deltas.clear();
        self.cache.measured_height_delta_total = 0.0;
        let clear_render_range =
            self.invalidate_derived_layout_cache(include_estimated_heights, reset_render_range);
        // NOTE(amadeus): In CodeView we intentionally batch computes to all happen
        // at the same time, so we shouldn't trigger this there.
        if force_simple_recompute {
            self.compute_approximate_size(false, None, ctx);
        }
        clear_render_range
    }

    /// `invalidateDerivedLayoutCache`. Returns whether the render range must be
    /// cleared (`this.renderRange != null && resetRenderRange`: the caller knows
    /// whether there is one).
    #[must_use]
    pub fn invalidate_derived_layout_cache(
        &mut self,
        include_estimated_heights: bool,
        reset_render_range: bool,
    ) -> bool {
        self.layout_dirty = true;
        self.cache.checkpoints.clear();
        self.cache.total_lines = 0;
        if include_estimated_heights {
            self.cache.estimated_split_height = None;
            self.cache.estimated_unified_height = None;
        }
        reset_render_range
    }

    // Compute the approximate size from the cached baseline estimate plus any
    // measured height deltas observed in rendered rows.
    // The reason we refer to this as `approximate size` is because heights my
    // dynamically change for a number of reasons so we can never be fully sure
    // if the height is 100% accurate
    pub fn compute_approximate_size(
        &mut self,
        force: bool,
        file_diff: Option<&FileDiffMetadata>,
        ctx: &LayoutContext<'_>,
    ) {
        let file_diff = file_diff.or(ctx.layout_diff);
        let should_validate_size = ctx.resize_debugging;
        if !force && !self.layout_dirty && !should_validate_size {
            return;
        }

        self.height = 0.0;
        self.cache.checkpoints.clear();
        self.cache.total_lines = 0;
        let Some(file_diff) = file_diff else {
            self.layout_dirty = false;
            return;
        };

        if ctx.options.collapsed {
            self.height +=
                get_virtual_file_header_height(&self.metrics, ctx.options.disable_file_header);
            self.layout_dirty = false;
            return;
        }

        self.height = self.get_active_estimated_height(Some(file_diff), ctx)
            + self.cache.measured_height_delta_total;
        self.layout_dirty = false;
    }

    pub fn get_active_estimated_height(
        &mut self,
        file_diff: Option<&FileDiffMetadata>,
        ctx: &LayoutContext<'_>,
    ) -> f64 {
        self.ensure_estimated_diff_heights(file_diff, ctx);
        let estimated_height = match ctx.options.diff_style {
            DiffStyle::Split => self.cache.estimated_split_height,
            DiffStyle::Unified => self.cache.estimated_unified_height,
        };
        estimated_height
            .expect("VirtualizedFileDiff.getActiveEstimatedHeight: missing estimated height")
    }

    pub fn ensure_estimated_diff_heights(
        &mut self,
        file_diff: Option<&FileDiffMetadata>,
        ctx: &LayoutContext<'_>,
    ) {
        let Some(file_diff) = file_diff.or(ctx.layout_diff) else {
            self.cache.estimated_split_height = None;
            self.cache.estimated_unified_height = None;
            return;
        };
        if self.cache.estimated_split_height.is_some()
            && self.cache.estimated_unified_height.is_some()
        {
            return;
        }

        let options = &ctx.options;
        let estimated = compute_estimated_diff_heights(ComputeEstimatedDiffHeightsOptions {
            file_diff,
            metrics: &self.metrics,
            disable_file_header: options.disable_file_header,
            hunk_separators: options.hunk_separators,
            expand_unchanged: options.expand_unchanged,
            expanded_hunks: Some(ctx.expanded_hunks),
            collapsed_context_threshold: options.collapsed_context_threshold,
            can_hydrate_partial_diff: can_hydrate_collapsed_context(
                file_diff,
                options.has_file_loader,
            ),
        });
        self.cache.estimated_split_height = Some(estimated.split_height);
        self.cache.estimated_unified_height = Some(estimated.unified_height);
    }

    /// `approximateLayoutCheckpoints`
    pub fn approximate_layout_checkpoints(
        &mut self,
        file_diff: &FileDiffMetadata,
        ctx: &LayoutContext<'_>,
    ) {
        if (!self.layout_dirty && !self.cache.checkpoints.is_empty())
            || file_diff.hunks.is_empty()
            || ctx.options.collapsed
        {
            return;
        }

        let top = get_virtual_file_header_region(&self.metrics, ctx.options.disable_file_header)
            + self.cache.file_annotation_height;
        let mut builder = CheckpointBuilder {
            metrics: &self.metrics,
            checkpoints: &mut self.cache.checkpoints,
            height_delta_prefix: create_height_delta_prefix(&self.cache.height_deltas),
            top,
            rendered_line_index: 0,
        };
        for hunk_index in 0..file_diff.hunks.len() {
            builder.process_hunk(file_diff, hunk_index, ctx);
        }
        self.cache.total_lines = builder.rendered_line_index;
    }

    // Find the nearest sparse layout checkpoint at or before an active
    // diff-style line index. Diff checkpoints also store the dense rendered-row
    // index, so deep line-position lookups can resume iteration from that
    // rendered row and replay only the nearby layout work instead of walking
    // from the first hunk.
    pub fn get_layout_checkpoint_before_line_index(
        &self,
        line_index: i64,
    ) -> Option<DiffLayoutCheckpoint> {
        if line_index <= 0 || self.cache.checkpoints.is_empty() {
            return None;
        }

        let mut low = 0;
        let mut high = self.cache.checkpoints.len() as isize - 1;
        let mut result = None;

        while low <= high {
            let mid = (low + high) >> 1;
            let checkpoint = self.cache.checkpoints[mid as usize];
            if checkpoint.line_index <= line_index {
                result = Some(checkpoint);
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        result
    }

    // Find the nearest sparse layout checkpoint at or before a scroll offset.
    // Render-range scans start from this checkpoint so variable-height diffs
    // only replay nearby rows. When `hunk_line_count` is provided, step backward
    // to a rendered hunk boundary so buffer calculations can reuse absolute hunk
    // offsets safely.
    pub fn get_layout_checkpoint_before_top(
        &self,
        top: f64,
        hunk_line_count: Option<usize>,
    ) -> Option<DiffLayoutCheckpoint> {
        let checkpoints = &self.cache.checkpoints;
        let mut low = 0;
        let mut high = checkpoints.len() as isize - 1;
        let mut result_index: isize = -1;

        while low <= high {
            let mid = (low + high) >> 1;
            if checkpoints[mid as usize].top <= top {
                result_index = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        let Some(hunk_line_count) = hunk_line_count else {
            return usize::try_from(result_index)
                .ok()
                .map(|index| checkpoints[index]);
        };

        let mut index = result_index;
        while index >= 0 {
            let checkpoint = checkpoints[index as usize];
            if checkpoint.rendered_line_index.checked_rem(hunk_line_count) == Some(0) {
                return Some(checkpoint);
            }
            index -= 1;
        }

        None
    }

    // Row total used to clamp render-range scrolling. Sparse layout checkpoints can
    // still hold a smaller pre-edit count until they are rebuilt, so always take
    // the max against the live diff metadata (including additionLines.length).
    pub fn get_layout_line_count(
        &self,
        file_diff: &FileDiffMetadata,
        diff_style: DiffStyle,
        ctx: &LayoutContext<'_>,
    ) -> usize {
        let expanded_line_count = get_expanded_line_count(file_diff, diff_style, ctx);
        let metadata_line_count = match diff_style {
            DiffStyle::Split => file_diff.split_line_count,
            DiffStyle::Unified => file_diff.unified_line_count,
        };
        expanded_line_count
            .max(metadata_line_count)
            .max(file_diff.addition_lines.len())
            .max(file_diff.deletion_lines.len())
            .max(self.cache.total_lines)
    }
}

/// `hasFileAnnotations`; `line_annotations` is `getLayoutAnnotations()`.
pub fn has_file_annotations<Metadata>(
    file_diff: &FileDiffMetadata,
    line_annotations: &[DiffLineAnnotation<Metadata>],
) -> bool {
    if !includes_file_annotations(
        line_annotations
            .iter()
            .map(|annotation| annotation.line_number),
    ) {
        return false;
    }
    line_annotations.iter().any(|annotation| {
        if annotation.line_number != FILE_ANNOTATION_LINE_NUMBER {
            return false;
        }
        // Lets ensure for singled sided diffs that the sides match
        match file_diff.change_type {
            ChangeTypes::New => annotation.side == AnnotationSide::Additions,
            ChangeTypes::Deleted => annotation.side == AnnotationSide::Deletions,
            _ => true,
        }
    })
}

pub fn get_expanded_line_count(
    file_diff: &FileDiffMetadata,
    diff_style: DiffStyle,
    ctx: &LayoutContext<'_>,
) -> usize {
    let mut count = 0;
    if file_diff.is_partial {
        for hunk in &file_diff.hunks {
            count += match diff_style {
                DiffStyle::Split => hunk.split_line_count,
                DiffStyle::Unified => hunk.unified_line_count,
            };
        }
        return count;
    }

    let collapsed_context_threshold = ctx.options.collapsed_context_threshold;
    let expanded_hunks = ctx.effective_expanded_hunks();

    for (hunk_index, hunk) in file_diff.hunks.iter().enumerate() {
        let hunk_count = match diff_style {
            DiffStyle::Split => hunk.split_line_count,
            DiffStyle::Unified => hunk.unified_line_count,
        };
        count += hunk_count;
        let collapsed_before = hunk.collapsed_before;
        let region = get_expanded_region(GetExpandedRegionProps {
            is_partial: file_diff.is_partial,
            range_size: collapsed_before,
            expanded_hunks: Some(expanded_hunks),
            hunk_index,
            collapsed_context_threshold,
        });
        if collapsed_before > 0 {
            count += if region.render_all {
                collapsed_before
            } else {
                region.from_start + region.from_end
            };
        }
    }

    let trailing_region = file_diff.hunks.len().checked_sub(1).and_then(|hunk_index| {
        get_trailing_expanded_region(GetTrailingExpandedRegionProps {
            file_diff,
            hunk_index,
            expanded_hunks: Some(expanded_hunks),
            collapsed_context_threshold,
            error_prefix: "VirtualizedFileDiff",
        })
    });
    if let Some(trailing_region) = trailing_region {
        count += trailing_region.from_start + trailing_region.from_end;
    }

    count
}

/// `processRows`' two mutable variables (`top`, `renderedLineIndex`) and what it
/// reads, so the closure's body and the loop around it are methods.
struct CheckpointBuilder<'layout> {
    metrics: &'layout VirtualFileMetrics,
    checkpoints: &'layout mut Vec<DiffLayoutCheckpoint>,
    height_delta_prefix: HeightDeltaPrefix,
    top: f64,
    rendered_line_index: usize,
}

/// `processRows`' destructured parameter, with its defaults.
#[derive(Default)]
struct Rows {
    row_count: usize,
    start_line_index: i64,
    pre_separator_height: f64,
    post_separator_height: f64,
    metadata_offsets: Vec<usize>,
}

impl CheckpointBuilder<'_> {
    fn process_rows(&mut self, rows: Rows) {
        let Rows {
            row_count,
            start_line_index,
            pre_separator_height,
            post_separator_height,
            metadata_offsets,
        } = rows;
        if row_count == 0 {
            return;
        }

        let line_height = self.metrics.line_height;
        let block_start = self.rendered_line_index;
        let block_end = self.rendered_line_index + row_count;
        let mut next_checkpoint = get_next_checkpoint_index(block_start);
        while next_checkpoint < block_end {
            let offset = next_checkpoint - block_start;
            let checkpoint_top = self.top
                + (if offset > 0 {
                    pre_separator_height
                } else {
                    0.0
                })
                + offset as f64 * line_height
                + count_metadata_offsets_before(&metadata_offsets, offset) as f64 * line_height
                + sum_height_deltas(
                    &self.height_delta_prefix,
                    start_line_index,
                    start_line_index + offset as i64,
                );
            self.checkpoints.push(DiffLayoutCheckpoint {
                rendered_line_index: next_checkpoint,
                line_index: start_line_index + offset as i64,
                top: checkpoint_top,
            });
            next_checkpoint += VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL;
        }

        self.top += pre_separator_height
            + row_count as f64 * line_height
            + metadata_offsets.len() as f64 * line_height
            + sum_height_deltas(
                &self.height_delta_prefix,
                start_line_index,
                start_line_index + row_count as i64,
            )
            + post_separator_height;
        self.rendered_line_index = block_end;
    }

    /// The body of upstream's `for (hunkIndex ...)` loop.
    fn process_hunk(
        &mut self,
        file_diff: &FileDiffMetadata,
        hunk_index: usize,
        ctx: &LayoutContext<'_>,
    ) {
        let LayoutOptions {
            collapsed_context_threshold,
            diff_style,
            hunk_separators,
            has_file_loader,
            ..
        } = ctx.options;
        let hunk = &file_diff.hunks[hunk_index];
        let final_hunk_index = file_diff.hunks.len() - 1;
        let expanded_hunks = Some(ctx.effective_expanded_hunks());
        let separator_props = HunkSeparatorBaseProps {
            separator_type: hunk_separators,
            metrics: self.metrics,
        };
        let hunk_line_start = match diff_style {
            DiffStyle::Split => hunk.split_line_start,
            DiffStyle::Unified => hunk.unified_line_start,
        } as i64;

        let leading_region = get_expanded_region(GetExpandedRegionProps {
            is_partial: file_diff.is_partial,
            range_size: hunk.collapsed_before,
            expanded_hunks,
            hunk_index,
            collapsed_context_threshold,
        });
        let leading_separator_height = if leading_region.collapsed_lines > 0 {
            get_leading_hunk_separator_layout(
                separator_props,
                hunk_index,
                hunk.hunk_specs.as_deref(),
            )
            .map_or(0.0, |layout| layout.total_height)
        } else {
            0.0
        };

        self.process_rows(Rows {
            row_count: leading_region.from_start,
            start_line_index: hunk_line_start - leading_region.range_size as i64,
            ..Rows::default()
        });

        let mut pending_leading_separator_height = leading_separator_height;
        self.process_rows(Rows {
            row_count: leading_region.from_end,
            start_line_index: hunk_line_start - leading_region.from_end as i64,
            pre_separator_height: pending_leading_separator_height,
            ..Rows::default()
        });
        if leading_region.from_end > 0 {
            pending_leading_separator_height = 0.0;
        }

        let trailing_region = if hunk_index == final_hunk_index {
            get_trailing_expanded_region(GetTrailingExpandedRegionProps {
                file_diff,
                hunk_index,
                expanded_hunks,
                collapsed_context_threshold,
                error_prefix: "VirtualizedFileDiff",
            })
        } else {
            None
        };
        let trailing_separator_height = if trailing_region
            .is_some_and(|trailing_region| trailing_region.collapsed_lines > 0)
            || (hunk_index == final_hunk_index
                && can_hydrate_collapsed_context(file_diff, has_file_loader))
        {
            get_trailing_hunk_separator_layout(separator_props)
                .map_or(0.0, |layout| layout.total_height)
        } else {
            0.0
        };
        let trailing_expanded_count = trailing_region.map_or(0, |trailing_region| {
            trailing_region.from_start + trailing_region.from_end
        });

        let (hunk_body_row_count, hunk_body_start_line_index) = match diff_style {
            DiffStyle::Split => (hunk.split_line_count, hunk.split_line_start),
            DiffStyle::Unified => (hunk.unified_line_count, hunk.unified_line_start),
        };
        self.process_rows(Rows {
            row_count: hunk_body_row_count,
            start_line_index: hunk_body_start_line_index as i64,
            pre_separator_height: pending_leading_separator_height,
            post_separator_height: if trailing_expanded_count == 0 {
                trailing_separator_height
            } else {
                0.0
            },
            metadata_offsets: get_hunk_metadata_offsets(diff_style, hunk, hunk_body_row_count),
        });

        if trailing_region.is_some() && trailing_expanded_count > 0 {
            self.process_rows(Rows {
                row_count: trailing_expanded_count,
                start_line_index: (hunk_body_start_line_index + hunk_body_row_count) as i64,
                post_separator_height: trailing_separator_height,
                ..Rows::default()
            });
        }
    }
}

struct HeightDeltaPrefix {
    line_indexes: Vec<i64>,
    prefix_totals: Vec<f64>,
}

fn create_height_delta_prefix(height_deltas: &HashMap<i64, f64>) -> HeightDeltaPrefix {
    let mut entries: Vec<(i64, f64)> = height_deltas
        .iter()
        .map(|(line_index, delta)| (*line_index, *delta))
        .collect();
    entries.sort_by_key(|(line_index, _)| *line_index);
    let mut line_indexes = Vec::with_capacity(entries.len());
    let mut prefix_totals = Vec::with_capacity(entries.len() + 1);
    prefix_totals.push(0.0);
    let mut total = 0.0;
    for (line_index, delta) in entries {
        line_indexes.push(line_index);
        total += delta;
        prefix_totals.push(total);
    }
    HeightDeltaPrefix {
        line_indexes,
        prefix_totals,
    }
}

fn sum_height_deltas(
    prefix: &HeightDeltaPrefix,
    start_line_index: i64,
    end_line_index: i64,
) -> f64 {
    if start_line_index >= end_line_index || prefix.line_indexes.is_empty() {
        return 0.0;
    }
    let start = lower_bound(&prefix.line_indexes, start_line_index);
    let end = lower_bound(&prefix.line_indexes, end_line_index);
    prefix.prefix_totals[end] - prefix.prefix_totals[start]
}

/// First index whose value is not below `target`.
fn lower_bound(values: &[i64], target: i64) -> usize {
    values.partition_point(|value| *value < target)
}

fn get_next_checkpoint_index(rendered_line_index: usize) -> usize {
    rendered_line_index.div_ceil(VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL)
        * VIRTUALIZED_FILE_DIFF_LAYOUT_CHECKPOINT_INTERVAL
}

fn count_metadata_offsets_before(metadata_offsets: &[usize], offset: usize) -> usize {
    metadata_offsets
        .iter()
        .filter(|metadata_offset| **metadata_offset < offset)
        .count()
}

fn get_hunk_metadata_offsets(diff_style: DiffStyle, hunk: &Hunk, row_count: usize) -> Vec<usize> {
    if row_count == 0 || (!hunk.no_eofcr_additions && !hunk.no_eofcr_deletions) {
        return Vec::new();
    }

    let Some(last_content) = hunk.hunk_content.last() else {
        return Vec::new();
    };

    let last_change = match last_content {
        HunkContent::Context(_) => return vec![row_count - 1],
        HunkContent::Change(change) => change,
    };

    let split_count = last_change.deletions.max(last_change.additions);
    let unified_count = last_change.deletions + last_change.additions;
    if diff_style == DiffStyle::Split {
        return if split_count > 0 && (hunk.no_eofcr_additions || hunk.no_eofcr_deletions) {
            vec![row_count - 1]
        } else {
            Vec::new()
        };
    }

    let mut offsets = Vec::new();
    let content_start_offset = row_count
        .checked_sub(unified_count)
        .expect("the last change block lies inside its hunk's rows");
    if last_change.deletions > 0 && hunk.no_eofcr_deletions {
        offsets.push(content_start_offset + last_change.deletions - 1);
    }
    if last_change.additions > 0 && hunk.no_eofcr_additions {
        offsets.push(row_count - 1);
    }
    offsets
}

/// `hasDiffLayoutOptionChanged`
pub fn has_diff_layout_option_changed<Element, Rendered, LAnnotation>(
    previous_options: &FileDiffOptions<Element, Rendered, LAnnotation>,
    next_options: &FileDiffOptions<Element, Rendered, LAnnotation>,
) -> bool {
    let (previous, next) = (&previous_options.base, &next_options.base);
    previous.diff_style.unwrap_or(DiffStyle::Split) != next.diff_style.unwrap_or(DiffStyle::Split)
        || previous.base.overflow.unwrap_or(Overflow::Scroll)
            != next.base.overflow.unwrap_or(Overflow::Scroll)
        || previous.base.collapsed.unwrap_or(false) != next.base.collapsed.unwrap_or(false)
        || previous.base.disable_line_numbers.unwrap_or(false)
            != next.base.disable_line_numbers.unwrap_or(false)
        || previous.base.disable_file_header.unwrap_or(false)
            != next.base.disable_file_header.unwrap_or(false)
        || previous.diff_indicators.unwrap_or(DiffIndicators::Bars)
            != next.diff_indicators.unwrap_or(DiffIndicators::Bars)
        || has_hunk_separators_changed(previous_options, next_options)
        || previous_options.has_diff_file_loader != next_options.has_diff_file_loader
        || previous.expand_unchanged.unwrap_or(false) != next.expand_unchanged.unwrap_or(false)
        || previous
            .collapsed_context_threshold
            .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
            != next
                .collapsed_context_threshold
                .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
        || previous.base.unsafe_css != next.base.unsafe_css
}

/// `hasDiffEstimateOptionChanged`
pub fn has_diff_estimate_option_changed<Element, Rendered, LAnnotation>(
    previous_options: &FileDiffOptions<Element, Rendered, LAnnotation>,
    next_options: &FileDiffOptions<Element, Rendered, LAnnotation>,
) -> bool {
    let (previous, next) = (&previous_options.base, &next_options.base);
    previous.base.disable_file_header.unwrap_or(false)
        != next.base.disable_file_header.unwrap_or(false)
        || has_hunk_separators_changed(previous_options, next_options)
        || previous_options.has_diff_file_loader != next_options.has_diff_file_loader
        || previous.expand_unchanged.unwrap_or(false) != next.expand_unchanged.unwrap_or(false)
        || previous
            .collapsed_context_threshold
            .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
            != next
                .collapsed_context_threshold
                .unwrap_or(DEFAULT_COLLAPSED_CONTEXT_THRESHOLD)
}

/// `(previousOptions.hunkSeparators ?? 'line-info') !== (nextOptions.hunkSeparators ?? 'line-info')`:
/// a function separator is equal only to the same function.
fn has_hunk_separators_changed<Element, Rendered, LAnnotation>(
    previous_options: &FileDiffOptions<Element, Rendered, LAnnotation>,
    next_options: &FileDiffOptions<Element, Rendered, LAnnotation>,
) -> bool {
    let line_info = FileDiffHunkSeparators::LineInfo;
    previous_options
        .hunk_separators
        .as_ref()
        .unwrap_or(&line_info)
        != next_options.hunk_separators.as_ref().unwrap_or(&line_info)
}

/// `canHydrateCollapsedContext`
pub fn can_hydrate_collapsed_context(file_diff: &FileDiffMetadata, has_file_loader: bool) -> bool {
    file_diff.is_partial
        && has_file_loader
        && matches!(
            file_diff.change_type,
            ChangeTypes::Change | ChangeTypes::RenameChanged
        )
}

/// `getOptionHunkSeparatorType`: a function is `'custom'`.
pub fn get_option_hunk_separator_type<Rendered>(
    hunk_separators: Option<&FileDiffHunkSeparators<Rendered>>,
) -> HunkSeparators {
    match hunk_separators {
        Some(FileDiffHunkSeparators::Custom(_)) => HunkSeparators::Custom,
        Some(FileDiffHunkSeparators::Simple) => HunkSeparators::Simple,
        Some(FileDiffHunkSeparators::Metadata) => HunkSeparators::Metadata,
        Some(FileDiffHunkSeparators::LineInfoBasic) => HunkSeparators::LineInfoBasic,
        Some(FileDiffHunkSeparators::LineInfo) | None => HunkSeparators::LineInfo,
    }
}

/// `areOptionalFilesEqual`
pub fn are_optional_files_equal(
    file_a: Option<&FileContents>,
    file_b: Option<&FileContents>,
) -> bool {
    are_files_equal(file_a, file_b)
}

// Extracts the view-specific line index from the data-line-index attribute.
// Format is "unifiedIndex,splitIndex"
pub fn parse_line_index(line_index_attr: &str, diff_style: DiffStyle) -> i64 {
    let (unified_index, split_index) = line_index_attr
        .split_once(',')
        .expect("data-line-index is \"unifiedIndex,splitIndex\"");
    let index = match diff_style {
        DiffStyle::Split => split_index,
        DiffStyle::Unified => unified_index,
    };
    index.parse().expect("data-line-index holds integers")
}
