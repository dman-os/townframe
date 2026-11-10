// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/VirtualizedFileDiff.ts
// (S12b-2: window and measurement; upstream lines 413-560 (`reconcileHeights`,
// `onRender`), 561-650 (`updateCodeViewLayout`), 1151-1164 (`setVisibility`),
// 1195-1262's layout accessors, 1307-1318 (`getLayoutDiff`,
// `getLayoutAnnotations`), 1551-1581 (`updatePendingRender`,
// `syncVirtualizedTop`), 1600-1608 (`getVirtualizedTop`), 195-208
// (`syncLineAnnotations`) and 2230-2252 (`measureFileAnnotationHeight`)).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * The class extends `FileDiff`; the port holds the `FileDiff` it extends
//   (`file_diff`, built by the constructor with the `FileDiffHooks` of
//   `overrides.rs`, which hold a `Weak` back-reference to this instance) and
//   reads the inherited fields through `FileDiff::virtualized()` and the public
//   accessors. The instance is an `Rc` (the virtualizer holds it as an
//   `InstanceRef`, a `SubscribedInstance`, and the hooks call back into it).
//   The layout state is `DiffLayout` (see `layout.rs`) behind a `RefCell`; the
//   virtualizer is `Virtualizer<Host>` (an `Rc`), `getSimpleVirtualizer()` is
//   always that virtualizer (the advanced mode is dropped, so `isSimpleMode()`
//   is true and `isAdvancedMode()` false everywhere).
// * The overrides (`render`, `rerender`, `setOptions`, `setThemeType`,
//   `setLineAnnotations`, `cleanUp`, `expandHunk`, `finalizeRender`,
//   `handleFilesLoaded`, `onRender`, `setVisibility`) are in `overrides.rs`.
// * `measureFileAnnotationHeight(codeGroups)` is a method (`measure_file_annotation_height`)
//   reading each element's height through the host (`VirtualizerHost::element_bounding_height`,
//   which `reconcileHeights`' `getBoundingClientRect().height` needs as well, so no
//   new host method is needed). DOM reads go through the instance's `Document`:
//   `codeGroup.children[1]` is the second element child, `children` of the
//   content is walked in place by raw position (`Document::element_child_from`,
//   no copy and no arena borrow across the host call), `dataset.lineIndex` /
//   `'lineAnnotation' in dataset` / `'noNewline' in dataset` are the
//   `data-line-index`, `data-line-annotation` and `data-no-newline` attributes,
//   `nextElementSibling` is the next step of that same walk, and every element
//   is an `HTMLElement` (`instanceof HTMLElement` checks are always true).
//   `FILE_ANNOTATION_DOM_KEY` is built once per measurement.
// * `updateCodeViewLayout` keeps its upstream name (the DOM-free way to put a
//   diff into the instance); it drops CodeView's channels: the `reset`
//   argument (`PendingCodeViewLayoutReset`), and the `pendingHydratedDiff` and
//   `pendingExpansions` arms, whose fields upstream writes only in the advanced
//   mode (`handleFilesLoaded` and `expandHunk` stage them for CodeView's layout
//   pass), so they are never set in the mode that remains. `updateInputDiff`
//   and `getRenderedDiff` take and return `Rc`s (`FileDiff`'s identity model);
//   `Object.assign` on the incoming diff goes with the hydration arm.
// * Ghost text rows and the editor are dropped (dropped.psv): `reconcileHeights`
//   starts with `hasHeightChange = false` where upstream folds in
//   `applyGhostTextRows`, so the placeholder branch's recompute and
//   `setPlaceholderHeight` are unreachable and its `if (hasHeightChange)` is
//   omitted; the per-row ghost terms are zero. `isResizeDebuggingEnabled()` is
//   `virtualizer.config.resize_debugging`.
// * `getDiffForNextRender` is the identity here (see file_diff.rs), so
//   `updatePendingRender`'s `pendingRenderDiff` is the working diff.
// * Layout methods that upstream pairs with `this.renderRange = undefined`
//   return a flag (`layout.rs`); the instance applies it through
//   `FileDiff::virtualized().set_render_range(None)`.

use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use super::layout::DiffLayout;
use super::layout::LayoutContext;
use super::layout::LayoutOptions;
use super::layout::ResetLayoutCacheOptions;
use super::layout::parse_line_index;
use super::overrides::OverrideHooks;
use super::window::LinePosition;
use crate::components::file_diff::FileDiff;
use crate::components::file_diff::FileDiffOptions;
use crate::components::file_diff::LineAnnotations;
use crate::components::file_diff::RenderedContent;
use crate::components::file_diff::VirtualizedFileDiffHost;
use crate::components::virtualizer::InstanceRef;
use crate::components::virtualizer::Virtualizer;
use crate::components::virtualizer::VirtualizerHost;
use crate::document::Document;
use crate::document::NodeKey;
use crate::highlighter_types::DiffsHighlighter;
use crate::managers::resize_manager::ResizeObserverHub;
use crate::managers::universal_rendering_manager::UniversalRenderingManager;
use crate::types::layout::RenderRange;
use crate::types::layout::RenderWindow;
use crate::types::lines::ExpandedHunks;
use crate::types::options::DiffStyle;
use crate::types::options::Overflow;
use crate::types::patch::FileDiffMetadata;
use crate::types::view::SelectionSide;
use crate::utils::compute_virtual_file_metrics::PartialVirtualFileMetrics;
use crate::utils::predicates::are_diff_targets_equal::are_diff_targets_equal;
use crate::utils::render::includes_file_annotations::file_annotation_dom_key;
use crate::utils::render::includes_file_annotations::should_render_file_annotations;

/// `PendingRender`: the diff the layout was calculated from, even when only a
/// placeholder is rendered.
struct PendingRender {
    /// Diff requested for the next render.
    working_diff: Rc<FileDiffMetadata>,
    /// Diff selected by the renderer.
    diff: Rc<FileDiffMetadata>,
}

/// The fields of the class that are not the layout.
struct InstanceState {
    layout: DiffLayout,
    is_visible: bool,
    current_collapsed: Option<bool>,
    pending_render: Option<PendingRender>,
    is_setup: bool,
}

/// What `updatePendingRender` returns.
pub struct PendingRenderUpdate {
    pub pending_render_diff: Rc<FileDiffMetadata>,
    pub layout_diff_changed: bool,
    pub rendered_diff_changed: bool,
    pub annotations_changed: bool,
}

pub struct VirtualizedFileDiff<
    Host: VirtualizedFileDiffHost,
    Rendered: 'static,
    LAnnotation: 'static,
> {
    weak_self: Weak<Self>,
    file_diff: Rc<FileDiff<Host, Rendered, LAnnotation>>,
    virtualizer: Rc<Virtualizer<Host>>,
    host: Rc<Host>,
    state: RefCell<InstanceState>,
}

impl<Host, Rendered, LAnnotation> VirtualizedFileDiff<Host, Rendered, LAnnotation>
where
    Host: VirtualizedFileDiffHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `new VirtualizedFileDiff(options, virtualizer, metrics, undefined,
    /// requestOwnerRender)`: the `FileDiff` it extends is built with this
    /// instance's hooks. The `Rc` is cyclic through the hooks' `Weak` only.
    #[expect(
        clippy::too_many_arguments,
        reason = "upstream's constructor and `FileDiff`'s take the same collaborators"
    )]
    pub fn new(
        host: Rc<Host>,
        resize_observer_hub: Rc<ResizeObserverHub<Host>>,
        render_queue: UniversalRenderingManager<Host>,
        highlighter: Rc<dyn DiffsHighlighter>,
        options: FileDiffOptions<NodeKey, Rendered, LAnnotation>,
        virtualizer: Rc<Virtualizer<Host>>,
        metrics: PartialVirtualFileMetrics,
        request_owner_render: Option<Rc<dyn Fn()>>,
    ) -> Rc<Self> {
        Rc::new_cyclic(|weak: &Weak<Self>| {
            let hooks = Rc::new(OverrideHooks {
                instance: Weak::clone(weak),
            });
            Self {
                weak_self: Weak::clone(weak),
                file_diff: FileDiff::new(
                    Rc::clone(&host),
                    resize_observer_hub,
                    render_queue,
                    highlighter,
                    hooks,
                    options,
                    request_owner_render,
                ),
                virtualizer,
                host,
                state: RefCell::new(InstanceState {
                    layout: DiffLayout::new(metrics),
                    is_visible: false,
                    current_collapsed: None,
                    pending_render: None,
                    is_setup: false,
                }),
            }
        })
    }

    /// `this` as the virtualizer holds it.
    pub(super) fn instance_ref(&self) -> InstanceRef {
        self.weak_self
            .upgrade()
            .expect("the instance is alive while it is called")
    }

    /// The `FileDiff` this instance extends.
    pub fn file_diff(&self) -> &Rc<FileDiff<Host, Rendered, LAnnotation>> {
        &self.file_diff
    }

    pub(super) fn virtualizer(&self) -> &Rc<Virtualizer<Host>> {
        &self.virtualizer
    }

    pub(super) fn is_setup(&self) -> bool {
        self.state.borrow().is_setup
    }

    pub(super) fn set_is_setup(&self, is_setup: bool) {
        self.state.borrow_mut().is_setup = is_setup;
    }

    pub(super) fn set_is_visible(&self, is_visible: bool) {
        self.state.borrow_mut().is_visible = is_visible;
    }

    /// `this.top ??= this.getVirtualizedTop()`
    pub(super) fn ensure_top(&self) {
        if self.state.borrow().layout.top.is_none() {
            self.sync_virtualized_top();
        }
    }

    /// `this.pendingRender?.diff`
    pub(super) fn pending_render_diff(&self) -> Option<Rc<FileDiffMetadata>> {
        self.state
            .borrow()
            .pending_render
            .as_ref()
            .map(|pending| Rc::clone(&pending.diff))
    }

    /// `this.pendingRender = undefined`
    pub(super) fn clear_pending_render(&self) {
        self.state.borrow_mut().pending_render = None;
    }

    /// The hydrated diff replaces `expected` in the pending render, as it does
    /// in the instance's other fields (see `overrides.rs`).
    pub(super) fn swap_pending_render_diff(
        &self,
        expected: &Rc<FileDiffMetadata>,
        hydrated: &Rc<FileDiffMetadata>,
    ) {
        if let Some(pending) = self.state.borrow_mut().pending_render.as_mut() {
            for slot in [&mut pending.working_diff, &mut pending.diff] {
                if Rc::ptr_eq(slot, expected) {
                    *slot = Rc::clone(hydrated);
                }
            }
        }
    }

    /// The layout state (`top`, `height`, the cache).
    pub fn layout(&self) -> std::cell::Ref<'_, DiffLayout> {
        std::cell::Ref::map(self.state.borrow(), |state| &state.layout)
    }

    /// `setMetrics`
    pub fn set_metrics(&self, metrics: PartialVirtualFileMetrics, force: bool) {
        let layout_diff = self.get_layout_diff();
        let clear = self.with_layout(layout_diff.as_deref(), |layout, ctx| {
            layout.set_metrics(metrics, force, ctx)
        });
        self.apply_render_range_reset(clear);
    }

    /// Test-only: the diffs of `this.pendingRender`.
    #[cfg(test)]
    pub fn pending_render_diffs(&self) -> Option<(Rc<FileDiffMetadata>, Rc<FileDiffMetadata>)> {
        self.state
            .borrow()
            .pending_render
            .as_ref()
            .map(|pending| (Rc::clone(&pending.working_diff), Rc::clone(&pending.diff)))
    }

    /// `this.height`
    pub fn get_virtualized_height(&self) -> f64 {
        self.state.borrow().layout.height
    }

    /// `this.top`
    pub fn top(&self) -> Option<f64> {
        self.state.borrow().layout.top
    }

    pub fn is_visible(&self) -> bool {
        self.state.borrow().is_visible
    }

    /// Runs `body` over the layout with the context upstream reads through
    /// `this` (the renderer's expanded hunks are shared, not copied).
    /// `layout_diff` is `getLayoutDiff()`; hold it across the call.
    pub fn with_layout<Output>(
        &self,
        layout_diff: Option<&FileDiffMetadata>,
        body: impl FnOnce(&mut DiffLayout, &LayoutContext<'_>) -> Output,
    ) -> Output {
        let options = LayoutOptions::of(&self.file_diff.options());
        let renderer = self.file_diff.hunks_renderer();
        let expanded_hunks = ExpandedHunks::borrowed(renderer.get_expanded_hunks_map());
        let ctx = LayoutContext {
            options,
            expanded_hunks: &expanded_hunks,
            layout_diff,
            resize_debugging: self.virtualizer.config.resize_debugging,
        };
        body(&mut self.state.borrow_mut().layout, &ctx)
    }

    /// The `Document` the instance renders into (the host answers one for all
    /// of its traits).
    fn document(&self) -> &Document {
        VirtualizerHost::document(&*self.host)
    }

    /// `this.renderRange = undefined` when a layout method asked for it.
    fn apply_render_range_reset(&self, clear_render_range: bool) {
        if clear_render_range {
            self.file_diff.virtualized().set_render_range(None);
        }
    }

    // CodeView calculates its next layout before updating the DOM. This keeps every
    // layout calculation within that frame tied to the same diff.
    pub fn get_layout_diff(&self) -> Option<Rc<FileDiffMetadata>> {
        let pending = self
            .state
            .borrow()
            .pending_render
            .as_ref()
            .map(|pending| Rc::clone(&pending.diff));
        pending.or_else(|| self.file_diff.get_rendered_diff())
    }

    // Annotations paired with the content layout is prepared for.
    pub fn get_layout_annotations(&self) -> LineAnnotations<LAnnotation> {
        let pending_diff = self
            .state
            .borrow()
            .pending_render
            .as_ref()
            .map(|pending| Rc::clone(&pending.diff));
        match pending_diff {
            Some(diff) => self.file_diff.get_annotations_for_diff(&diff),
            None => self.file_diff.line_annotations(),
        }
    }

    fn get_virtualized_top(&self) -> f64 {
        match self.file_diff.virtualized().file_container() {
            Some(file_container) => self
                .virtualizer
                .get_offset_in_scroll_container(&file_container),
            None => 0.0,
        }
    }

    pub fn sync_virtualized_top(&self) {
        let top = self.get_virtualized_top();
        self.state.borrow_mut().layout.top = Some(top);
    }

    /// `resetLayoutCache`
    pub fn reset_layout_cache(&self, options: ResetLayoutCacheOptions) {
        let layout_diff = self.get_layout_diff();
        let clear = self.with_layout(layout_diff.as_deref(), |layout, ctx| {
            layout.reset_layout_cache(options, ctx)
        });
        self.apply_render_range_reset(clear);
    }

    /// `invalidateDerivedLayoutCache(includeEstimatedHeights)`
    pub fn invalidate_derived_layout_cache(&self, include_estimated_heights: bool) {
        let clear = self
            .state
            .borrow_mut()
            .layout
            .invalidate_derived_layout_cache(include_estimated_heights, true);
        self.apply_render_range_reset(clear);
    }

    /// `computeApproximateSize(force, fileDiff)`
    pub fn compute_approximate_size(&self, force: bool, file_diff: Option<&FileDiffMetadata>) {
        let layout_diff = self.get_layout_diff();
        self.with_layout(layout_diff.as_deref(), |layout, ctx| {
            layout.compute_approximate_size(force, file_diff, ctx);
        });
    }

    // Keep measured row heights as estimates when annotations change; rendering
    // will replace them with the updated heights without discarding other rows.
    pub fn sync_line_annotations(
        &self,
        line_annotations: Option<&LineAnnotations<LAnnotation>>,
    ) -> bool {
        let Some(line_annotations) = line_annotations else {
            return false;
        };
        if !self.file_diff.is_new_annotations(line_annotations)
            || (line_annotations.is_empty() && self.file_diff.get_latest_annotations().is_empty())
        {
            return false;
        }
        self.file_diff
            .set_line_annotations(Rc::clone(line_annotations));
        true
    }

    pub fn update_pending_render(
        &self,
        next_file_diff: &Rc<FileDiffMetadata>,
        line_annotations: Option<&LineAnnotations<LAnnotation>>,
    ) -> PendingRenderUpdate {
        let working_diff = self
            .file_diff
            .get_working_diff(Some(Rc::clone(next_file_diff)))
            .unwrap_or_else(|| Rc::clone(next_file_diff));
        let previous_rendered_diff = self.file_diff.get_rendered_diff();
        let previous_layout_diff = self.get_layout_diff();
        // `hunksRenderer.getDiffForNextRender(workingDiff)`: the identity here.
        let pending_render_diff = Rc::clone(&working_diff);

        self.state.borrow_mut().pending_render = Some(PendingRender {
            working_diff,
            diff: Rc::clone(&pending_render_diff),
        });

        let annotations_changed = self.sync_line_annotations(line_annotations);
        PendingRenderUpdate {
            annotations_changed,
            layout_diff_changed: !are_diff_targets_equal(
                previous_layout_diff.as_deref(),
                Some(&pending_render_diff),
            ),
            rendered_diff_changed: !are_diff_targets_equal(
                previous_rendered_diff.as_deref(),
                Some(&pending_render_diff),
            ),
            pending_render_diff,
        }
    }

    /// `this.pendingRender.workingDiff === (this.getWorkingDiff(nextFileDiff) ?? nextFileDiff)`
    /// and its `diff`: what `render` reads before deciding to call
    /// `updatePendingRender`.
    pub fn pending_render_for(
        &self,
        next_file_diff: &Rc<FileDiffMetadata>,
    ) -> Option<Rc<FileDiffMetadata>> {
        let working_diff = self
            .file_diff
            .get_working_diff(Some(Rc::clone(next_file_diff)))
            .unwrap_or_else(|| Rc::clone(next_file_diff));
        self.state
            .borrow()
            .pending_render
            .as_ref()
            .filter(|pending| Rc::ptr_eq(&pending.working_diff, &working_diff))
            .map(|pending| Rc::clone(&pending.diff))
    }

    /// `updateCodeViewLayout(fileDiff, top, undefined, lineAnnotations)`, see the header.
    pub fn update_code_view_layout(
        &self,
        file_diff: &Rc<FileDiffMetadata>,
        top: f64,
        line_annotations: Option<&LineAnnotations<LAnnotation>>,
    ) -> f64 {
        let collapsed = LayoutOptions::of(&self.file_diff.options()).collapsed;
        let mut reset_layout_cache = false;

        let diff_changed = self
            .file_diff
            .virtualized()
            .update_input_diff(Rc::clone(file_diff), line_annotations.map(Rc::clone));
        let update = self.update_pending_render(file_diff, line_annotations);
        // `!this.forceRenderOverride && (...)`: setting it again is a no-op.
        if diff_changed || update.rendered_diff_changed || update.annotations_changed {
            self.file_diff.virtualized().force_render_override();
        }

        if update.layout_diff_changed {
            reset_layout_cache = true;
        }
        let reset_estimated_heights = update.layout_diff_changed;
        if self.state.borrow().current_collapsed != Some(collapsed) {
            self.state.borrow_mut().current_collapsed = Some(collapsed);
            reset_layout_cache = true;
        }
        if reset_layout_cache {
            self.reset_layout_cache(ResetLayoutCacheOptions {
                include_estimated_heights: reset_estimated_heights,
                ..ResetLayoutCacheOptions::default()
            });
        } else if reset_estimated_heights {
            self.invalidate_derived_layout_cache(true);
        }

        self.state.borrow_mut().layout.top = Some(top);
        self.compute_approximate_size(false, Some(&update.pending_render_diff));
        self.get_virtualized_height()
    }

    /// `getLinePosition`
    pub fn get_line_position(
        &self,
        line_number: usize,
        side: SelectionSide,
    ) -> Option<LinePosition> {
        let layout_diff = self.get_layout_diff();
        self.with_layout(layout_diff.as_deref(), |layout, ctx| {
            layout.get_line_position(line_number, side, ctx)
        })
    }

    /// `computeRenderRangeFromWindow`
    pub fn compute_render_range_from_window(
        &self,
        file_diff: &FileDiffMetadata,
        file_top: f64,
        window: RenderWindow,
    ) -> RenderRange {
        let layout_diff = self.get_layout_diff();
        let line_annotations = self.get_layout_annotations();
        self.with_layout(layout_diff.as_deref(), |layout, ctx| {
            layout.compute_render_range_from_window(
                file_diff,
                file_top,
                window,
                &line_annotations,
                ctx,
            )
        })
    }

    /// `reconcileHeights`: fold the heights the DOM reports for the rendered rows
    /// into the layout cache; `true` if any height changed.
    pub fn reconcile_heights(&self) -> bool {
        let virtualized = self.file_diff.virtualized();
        let mut has_height_change = false;
        let overflow = self
            .file_diff
            .options()
            .base
            .base
            .overflow
            .unwrap_or(Overflow::Scroll);
        // A placeholder may have a prepared layout before any content has
        // rendered.
        let file_diff = if virtualized.place_holder().is_some() {
            self.get_layout_diff()
        } else {
            self.file_diff.get_rendered_diff()
        };
        let (Some(_), Some(_)) = (virtualized.file_container(), file_diff) else {
            let mut state = self.state.borrow_mut();
            if state.layout.height != 0.0 {
                has_height_change = true;
            }
            state.layout.height = 0.0;
            return has_height_change;
        };
        self.sync_virtualized_top();
        let measure_all_rows = overflow != Overflow::Scroll
            || !self.file_diff.line_annotations().is_empty()
            || self.virtualizer.config.resize_debugging;
        // After the last annotation is removed, keep measuring rendered rows
        // until their previously cached heights have been corrected.
        {
            let state = self.state.borrow();
            let cache = &state.layout.cache;
            if virtualized.place_holder().is_some()
                || (!measure_all_rows
                    && cache.height_deltas.is_empty()
                    && cache.file_annotation_height == 0.0)
            {
                return has_height_change;
            }
        }
        let diff_style = LayoutOptions::of(&self.file_diff.options()).diff_style;
        let code_groups = virtualized.code_groups();
        let code_groups = match diff_style {
            DiffStyle::Split => [code_groups.deletions, code_groups.additions],
            DiffStyle::Unified => [code_groups.unified, None],
        };

        // Keep offscreen file annotation measurements consistent with the buffers
        // computed for this render. A rendered top row can confirm their removal.
        if virtualized
            .render_range()
            .is_some_and(|render_range| should_render_file_annotations(&render_range))
        {
            let file_annotation_height = self.measure_file_annotation_height(&code_groups);
            let next_file_annotation_height = file_annotation_height.unwrap_or(0.0);
            if self
                .state
                .borrow_mut()
                .layout
                .set_file_annotation_height(next_file_annotation_height)
            {
                has_height_change = true;
            }
        }

        for code_group in code_groups.into_iter().flatten() {
            has_height_change |=
                self.reconcile_code_group(code_group, diff_style, measure_all_rows);
        }

        if has_height_change || self.virtualizer.config.resize_debugging {
            self.compute_approximate_size(true, None);
        }
        has_height_change
    }

    /// `codeGroup.children[1]`.
    fn second_element_child(&self, code_group: NodeKey) -> Option<NodeKey> {
        let document = self.document();
        let (first, _) = document.element_child_from(code_group, 0)?;
        document
            .element_child_from(code_group, first + 1)
            .map(|(_, key)| key)
    }

    /// The body of `reconcileHeights`' loop over `codeGroups`, after the
    /// `children[1]` check; `true` if any row's delta changed.
    fn reconcile_code_group(
        &self,
        code_group: NodeKey,
        diff_style: DiffStyle,
        measure_all_rows: bool,
    ) -> bool {
        let document = self.document();
        let Some(content) = self.second_element_child(code_group) else {
            return false;
        };
        let mut has_height_change = false;
        // Walks the live children by raw position; `cursor` is both the following
        // line and the line's `nextElementSibling`, and no arena borrow is held
        // across the host call.
        let mut cursor = document.element_child_from(content, 0);
        while let Some((position, line)) = cursor {
            cursor = document.element_child_from(content, position + 1);
            let Some(line_index_attr) = document.get_attribute(line, "data-line-index") else {
                continue;
            };

            let line_index = parse_line_index(&line_index_attr, diff_style);
            let mut state = self.state.borrow_mut();
            let layout = &mut state.layout;
            let previous_delta = layout
                .cache
                .height_deltas
                .get(&line_index)
                .copied()
                .unwrap_or(0.0);
            // With no annotations or wrapping, only stale custom measurements need
            // DOM reads.
            if !measure_all_rows && previous_delta == 0.0 {
                continue;
            }
            let mut measured_height = self.host.element_bounding_height(&line);
            let mut has_metadata = false;
            // Annotations or noNewline metadata increase the size of the their
            // attached line
            if let Some((_, sibling)) = cursor
                && (document.has_attribute(sibling, "data-line-annotation")
                    || document.has_attribute(sibling, "data-no-newline"))
            {
                if document.has_attribute(sibling, "data-no-newline") {
                    has_metadata = true;
                }
                measured_height += self.host.element_bounding_height(&sibling);
            }
            let estimated_height = layout.get_estimated_line_height(has_metadata);
            let next_delta = measured_height - estimated_height;

            if next_delta == previous_delta {
                continue;
            }

            has_height_change = true;
            layout.cache.measured_height_delta_total += next_delta - previous_delta;
            if next_delta == 0.0 {
                layout.cache.height_deltas.remove(&line_index);
            } else {
                layout.cache.height_deltas.insert(line_index, next_delta);
            }
        }
        has_height_change
    }

    /// `measureFileAnnotationHeight`: the tallest file annotation row among the
    /// code groups' content, `None` if there is none.
    fn measure_file_annotation_height(&self, code_groups: &[Option<NodeKey>; 2]) -> Option<f64> {
        let document = self.document();
        let dom_key = file_annotation_dom_key();
        let mut height: Option<f64> = None;
        for code_group in code_groups.iter().copied().flatten() {
            let Some(content) = self.second_element_child(code_group) else {
                continue;
            };
            let mut cursor = document.element_child_from(content, 0);
            while let Some((position, child)) = cursor {
                cursor = document.element_child_from(content, position + 1);
                if document
                    .get_attribute(child, "data-line-annotation")
                    .as_deref()
                    != Some(dom_key.as_str())
                {
                    continue;
                }
                height = Some(
                    height
                        .unwrap_or(0.0)
                        .max(self.host.element_bounding_height(&child)),
                );
            }
        }
        height
    }
}
