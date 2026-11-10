//! The instance around `DiffLayout` for the tests: what upstream's layout
//! methods read through `this` (the options, the renderer's expanded hunks, the
//! pending render's diff, the line annotations, the render range) and the layout
//! halves of the class methods that call them (`setOptions`, `setMetrics`, the
//! simple-mode `expandHunk`, `updateCodeViewLayout`), as S12b-3's
//! `VirtualizedFileDiff` will compose them. Test-only, original to this crate.

use std::rc::Rc;

use super::layout::DiffLayout;
use super::layout::LayoutContext;
use super::layout::LayoutOptions;
use super::layout::ResetLayoutCacheOptions;
use super::layout::has_diff_estimate_option_changed;
use super::layout::has_diff_layout_option_changed;
use crate::components::file_diff::FileDiffOptions;
use crate::document::NodeKey;
use crate::renderers::diff_hunks_renderer::DiffHunksRenderer;
use crate::renderers::diff_hunks_renderer::DiffHunksRendererOptions;
use crate::types::lines::ExpandedHunks;
use crate::types::lines::ExpansionDirections;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::DiffLineAnnotation;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use crate::utils::predicates::are_diff_targets_equal::are_diff_targets_equal;
use crate::utils::predicates::are_options_equal::are_options_equal;

pub(super) type Options = FileDiffOptions<NodeKey, u8, ()>;

pub(super) struct LayoutInstance {
    pub(super) options: Options,
    pub(super) layout: DiffLayout,
    /// `this.hunksRenderer`, for its expanded hunks.
    pub(super) renderer: DiffHunksRenderer<()>,
    /// `getLayoutDiff()`: `this.pendingRender?.diff` (no instance here renders).
    pub(super) layout_diff: Option<Rc<FileDiffMetadata>>,
    /// `this.lineAnnotations`
    pub(super) annotations: Vec<DiffLineAnnotation<u8>>,
    /// `this.renderRange != null`
    pub(super) render_range_set: bool,
    /// `this.getSimpleVirtualizer()?.config.resizeDebugging`
    pub(super) resize_debugging: bool,
}

impl LayoutInstance {
    pub(super) fn new(options: Options, metrics: PartialVirtualFileMetrics) -> Self {
        Self {
            options,
            layout: DiffLayout::new(metrics),
            renderer: DiffHunksRenderer::new(DiffHunksRendererOptions::default()),
            layout_diff: None,
            annotations: Vec::new(),
            render_range_set: false,
            resize_debugging: false,
        }
    }

    /// Runs `body` with the context upstream would read through `this`.
    pub(super) fn with_context<Output>(
        &mut self,
        body: impl FnOnce(&mut DiffLayout, &LayoutContext<'_>) -> Output,
    ) -> Output {
        let expanded_hunks = ExpandedHunks::borrowed(self.renderer.get_expanded_hunks_map());
        let context = LayoutContext {
            options: LayoutOptions::of(&self.options),
            expanded_hunks: &expanded_hunks,
            layout_diff: self.layout_diff.as_deref(),
            resize_debugging: self.resize_debugging,
        };
        body(&mut self.layout, &context)
    }

    /// `this.renderRange = undefined` when a layout method asked for it.
    pub(super) fn apply_render_range_reset(&mut self, clear_render_range: bool) {
        if clear_render_range {
            self.render_range_set = false;
        }
    }

    pub(super) fn reset_layout_cache(&mut self, options: ResetLayoutCacheOptions) {
        let clear =
            self.with_context(|layout, context| layout.reset_layout_cache(options, context));
        self.apply_render_range_reset(clear);
    }

    pub(super) fn compute_approximate_size(&mut self) {
        self.with_context(|layout, context| layout.compute_approximate_size(false, None, context));
    }

    pub(super) fn set_metrics(&mut self, metrics: PartialVirtualFileMetrics, force: bool) {
        let clear =
            self.with_context(|layout, context| layout.set_metrics(metrics, force, context));
        self.apply_render_range_reset(clear);
    }

    /// `VirtualizedFileDiff.setOptions`, minus `FileDiff.setOptions`'s own work
    /// (S12b-3) and the virtualizer notification.
    pub(super) fn set_options(&mut self, next: Options) {
        let options_changed = !are_options_equal(Some(&self.options), Some(&next));
        let layout_changed =
            options_changed && has_diff_layout_option_changed(&self.options, &next);
        let estimate_changed =
            layout_changed && has_diff_estimate_option_changed(&self.options, &next);
        self.options = next;
        if layout_changed {
            self.reset_layout_cache(ResetLayoutCacheOptions {
                force_simple_recompute: true,
                include_estimated_heights: estimate_changed,
                reset_render_range: true,
            });
        }
    }

    /// The simple-mode `expandHunk`, minus `loadFilesIfNecessary` and the
    /// virtualizer notification (S12b-3).
    pub(super) fn expand_hunk(
        &mut self,
        hunk_index: usize,
        direction: ExpansionDirections,
        expansion_line_count_override: Option<usize>,
    ) {
        self.renderer
            .expand_hunk(hunk_index, direction, expansion_line_count_override);
        self.reset_layout_cache(ResetLayoutCacheOptions {
            include_estimated_heights: true,
            ..ResetLayoutCacheOptions::default()
        });
        self.compute_approximate_size();
    }

    /// The layout half of `updateCodeViewLayout(fileDiff, top)` for an instance
    /// with no pending hydration or expansion (`updatePendingRender`'s
    /// `layoutDiffChanged` and the cache resets that follow it).
    pub(super) fn update_layout(&mut self, file_diff: &Rc<FileDiffMetadata>, top: f64) -> f64 {
        let layout_diff_changed =
            !are_diff_targets_equal(self.layout_diff.as_deref(), Some(file_diff));
        self.layout_diff = Some(Rc::clone(file_diff));
        if layout_diff_changed {
            self.reset_layout_cache(ResetLayoutCacheOptions {
                include_estimated_heights: true,
                ..ResetLayoutCacheOptions::default()
            });
        }
        self.layout.top = Some(top);
        let layout_diff = Some(&**file_diff);
        self.with_context(|layout, context| {
            layout.compute_approximate_size(false, layout_diff, context);
        });
        self.layout.height
    }
}
