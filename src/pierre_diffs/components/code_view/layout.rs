// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C2: layout and selection: `applySelectedLines`, `syncSelection`,
// `markLayoutDirtyFromIndex`, `markItemLayoutDirty`, `recomputeLayout`,
// `prepareItemInstance`, `findFirstVisibleIndex`, `findLastVisibleIndex`,
// `resetRenderState`, `setOptions`, `capturePendingLayoutAnchor`,
// `canSkipRenderForAppend`).
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `prepareItemInstance` is a method (it reads the viewer's options, pushed to
//   the instance before the layout, see `item_options.rs`) and
//   `updateCodeViewLayout` returns a `CodeViewLayoutUpdate`: when a staged
//   hydration was consumed, upstream's `Object.assign(fileDiff, nextDiff)`
//   mutated the item's diff in place, so the item keeps the returned hydrated
//   `Rc` instead (parity/divergences.psv).
// * `item.item.annotations ?? []` is a fresh empty `Rc<Vec>` when absent, as a
//   fresh `[]` is upstream.
// * `capturePendingLayoutAnchor` returns while there is no root, which is every
//   call before C4's `setup`; past it, it is `scroll.rs`'s `getScrollAnchor`.
// * `findFirstVisibleIndex`/`findLastVisibleIndex` panic on an invalid index as
//   upstream throws.

use std::collections::HashMap;
use std::rc::Rc;

use super::CodeView;
use super::CodeViewHost;
use super::options::CodeViewOptions;
use super::options::has_code_view_diff_estimate_option_changed;
use super::options::has_item_layout_option_changed;
use super::options::should_clear_pool;
use super::state::CodeViewLineSelection;
use super::state::ItemKind;
use super::state::ItemRef;
use super::state::RenderState;
use crate::components::file_diff::LineAnnotations;
use crate::components::file_diff::RenderedContent;
use crate::managers::interaction_manager::SelectionWriteOptions;
use crate::types::layout::PendingCodeViewLayoutReset;
use crate::utils::predicates::are_selections_equal::are_selections_equal;

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `applySelectedLines(selection, options)`
    pub(crate) fn apply_selected_lines(
        &self,
        selection: Option<CodeViewLineSelection>,
        options: SelectionWriteOptions,
    ) {
        let previous = self.state.borrow().selected_lines.clone();
        match (&selection, &previous) {
            (None, None) => return,
            (Some(selection), Some(previous))
                if previous.id == selection.id
                    && are_selections_equal(Some(&previous.range), Some(&selection.range)) =>
            {
                return;
            }
            _ => {}
        }

        // If we are selecting a new element and had a previous selection, null
        // out the current selection, otherwise if it's a selection on the same
        // item the next selection will take care of that for us
        if let Some(previous) = &previous
            && selection.as_ref().map(|selection| &selection.id) != Some(&previous.id)
            && let Some(item) = self.item_by_id(&previous.id)
        {
            let instance = Rc::clone(item.borrow().instance());
            instance.file_diff().set_selected_lines(
                None,
                SelectionWriteOptions {
                    notify: Some(false),
                    ..SelectionWriteOptions::default()
                },
            );
        }

        self.state.borrow_mut().selected_lines = selection.clone();
        // `idToItem.get(selection?.id ?? '')`
        let selected_item =
            self.item_by_id(selection.as_ref().map_or("", |selection| &selection.id));
        if let Some(item) = selected_item {
            let instance = Rc::clone(item.borrow().instance());
            instance
                .file_diff()
                .set_selected_lines(selection.map(|selection| selection.range), options);
        }
    }

    /// `syncSelection()`
    pub(crate) fn sync_selection(&self) {
        let Some(selected) = self.state.borrow().selected_lines.clone() else {
            return;
        };
        let Some(item) = self.item_by_id(&selected.id) else {
            self.state.borrow_mut().selected_lines = None;
            let on_change = self.state.borrow().options.on_selected_lines_change.clone();
            if let Some(on_change) = on_change {
                on_change(None);
            }
            return;
        };
        let instance = Rc::clone(item.borrow().instance());
        instance.file_diff().set_selected_lines(
            Some(selected.range),
            SelectionWriteOptions {
                notify: Some(false),
                ..SelectionWriteOptions::default()
            },
        );
    }

    /// `setSelectedLines(selection, options)`
    pub fn set_selected_lines(
        &self,
        selection: Option<CodeViewLineSelection>,
        options: SelectionWriteOptions,
    ) {
        self.apply_selected_lines(selection, options);
    }

    /// `clearSelectedLines(options)`
    pub fn clear_selected_lines(&self, options: SelectionWriteOptions) {
        self.apply_selected_lines(None, options);
    }

    pub(crate) fn item_by_id(&self, id: &str) -> Option<ItemRef<Host, Rendered, LAnnotation>> {
        self.state.borrow().id_to_item.get(id).cloned()
    }

    /// Track the earliest index whose measured layout may now be stale. Later
    /// render passes relayout from this point forward so we do not have to
    /// rebuild positions for the whole list after every change.
    /// `markLayoutDirtyFromIndex(index)`
    pub(crate) fn mark_layout_dirty_from_index(&self, index: usize) {
        let mut state = self.state.borrow_mut();
        state.layout_dirty_index = Some(
            state
                .layout_dirty_index
                .map_or(index, |dirty| dirty.min(index)),
        );
    }

    /// Mark the earliest affected item as layout-dirty after an imperative
    /// change. Each record carries its current array index so this stays O(1).
    /// `markItemLayoutDirty(item)`
    pub(crate) fn mark_item_layout_dirty(&self, item: &ItemRef<Host, Rendered, LAnnotation>) {
        let index = item.borrow().index;
        let registered = self
            .state
            .borrow()
            .items
            .get(index)
            .is_some_and(|current| Rc::ptr_eq(current, item));
        assert!(
            registered,
            "CodeView.markItemLayoutDirty: unknown item id \"{}\"",
            item.borrow().id()
        );
        self.mark_layout_dirty_from_index(index);
    }

    /// `resetRenderState()`
    pub(crate) fn reset_render_state(&self) {
        self.state.borrow_mut().render_state = RenderState::RESET;
    }

    /// Hand the instance the options CodeView would answer for this item
    /// right now, see `item_options.rs`.
    pub(crate) fn push_item_options(&self, item: &ItemRef<Host, Rendered, LAnnotation>) {
        let (instance, options) = {
            let mut item = item.borrow_mut();
            let ItemKind::Diff(diff) = &mut item.kind else {
                unreachable!("file items cannot exist (VirtualizedFile is DEFER)");
            };
            let options =
                self.build_diff_options(&diff.item, &diff.options_state, &mut diff.callbacks);
            (Rc::clone(&diff.instance), options)
        };
        instance.set_options_from_code_view(options);
    }

    /// `item.instance.updateCodeViewLayout(item.item.fileDiff, top, reset,
    /// item.item.annotations ?? [])`, returning the height.
    fn layout_item(
        &self,
        item: &ItemRef<Host, Rendered, LAnnotation>,
        top: f64,
        reset: Option<PendingCodeViewLayoutReset>,
    ) -> f64 {
        self.push_item_options(item);
        let (instance, file_diff, annotations) = {
            let item = item.borrow();
            let ItemKind::Diff(diff) = &item.kind else {
                unreachable!("file items cannot exist (VirtualizedFile is DEFER)");
            };
            let annotations: LineAnnotations<LAnnotation> = diff
                .item
                .annotations
                .clone()
                .unwrap_or_else(|| Rc::new(Vec::new()));
            (
                Rc::clone(&diff.instance),
                Rc::clone(&diff.item.file_diff),
                annotations,
            )
        };
        let update = instance.update_code_view_layout(&file_diff, top, reset, Some(&annotations));
        if let Some(hydrated_diff) = update.hydrated_diff
            && let ItemKind::Diff(diff) = &mut item.borrow_mut().kind
        {
            diff.item.file_diff = hydrated_diff;
        }
        update.height
    }

    /// `prepareItemInstance(item)`
    pub(crate) fn prepare_item_instance(&self, item: &ItemRef<Host, Rendered, LAnnotation>) -> f64 {
        self.push_item_options(item);
        let (instance, top) = {
            let item = item.borrow();
            (Rc::clone(item.instance()), item.top)
        };
        instance.clean_up(true);
        self.layout_item(item, top, None)
    }

    /// Recompute measured tops and heights starting from the earliest dirty
    /// item. Earlier items keep their existing layout, while everything from
    /// `start_index` onward is remeasured so downstream positions and total
    /// scroll height stay consistent after inserts, removals, or versioned item
    /// updates. `recomputeLayout(startIndex, reset)`
    pub fn recompute_layout(&self, start_index: usize, reset: Option<PendingCodeViewLayoutReset>) {
        let items = self.state.borrow().items.clone();
        if items.is_empty() {
            self.state.borrow_mut().scroll_height = 0.0;
            return;
        }

        let layout = self.get_layout();
        let mut running_top = 0.0;
        if start_index > 0 {
            let previous_item = items
                .get(start_index - 1)
                .expect("CodeView.recomputeLayout: invalid dirty index")
                .borrow();
            running_top = previous_item.top + previous_item.height + layout.gap;
        }

        for index in start_index..items.len() {
            let item = &items[index];
            item.borrow_mut().top = running_top;
            let height = self.layout_item(item, running_top, reset);
            item.borrow_mut().height = height;
            running_top += height;
            if index < items.len() - 1 {
                running_top += layout.gap;
            }
        }

        let mut state = self.state.borrow_mut();
        if running_top != state.scroll_height {
            state.scroll_dirty = true;
        }
        state.scroll_height = running_top;
    }

    /// Find the first item whose bottom edge crosses into the viewport window.
    /// `findFirstVisibleIndex(top)`
    pub fn find_first_visible_index(&self, top: f64) -> usize {
        let state = self.state.borrow();
        let (mut low, mut high) = (0_i64, state.items.len() as i64 - 1);
        let mut result = state.items.len();
        while low <= high {
            let mid = (low + high) >> 1;
            let item = state.items[mid as usize].borrow();
            if item.top + item.height > top {
                result = mid as usize;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        result
    }

    /// Find the last item whose top edge is still within the viewport window,
    /// or `None` (upstream's `-1`). `findLastVisibleIndex(bottom)`
    pub fn find_last_visible_index(&self, bottom: f64) -> Option<usize> {
        let state = self.state.borrow();
        let (mut low, mut high) = (0_i64, state.items.len() as i64 - 1);
        let mut result = None;
        while low <= high {
            let mid = (low + high) >> 1;
            if state.items[mid as usize].borrow().top <= bottom {
                result = Some(mid as usize);
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        result
    }

    /// `capturePendingLayoutAnchor(nextItems = this.idToItem)`
    pub fn capture_pending_layout_anchor(
        &self,
        next_items: Option<&HashMap<String, ItemRef<Host, Rendered, LAnnotation>>>,
    ) {
        {
            let state = self.state.borrow();
            if state.root.is_none()
                || state.items.is_empty()
                || state.pending_scroll_target.is_some()
            {
                return;
            }
        }
        let anchor = self.get_scroll_anchor(self.get_scroll_top(), next_items);
        self.state.borrow_mut().pending_layout_anchor = anchor;
    }

    /// `canSkipRenderForAppend(appendedTop)`
    pub(crate) fn can_skip_render_for_append(&self, appended_top: f64) -> bool {
        let state = self.state.borrow();
        state.container.is_some()
            && state.render_state.first_index != -1
            && state.pending_scroll_target.is_none()
            && state.scroll_animation.is_none()
            && state.layout_dirty_index.is_none()
            && appended_top > state.window_specs.bottom
    }

    /// `setOptions(options)`
    pub fn set_options(&self, options: Option<CodeViewOptions<Rendered, LAnnotation>>) {
        let Some(options) = options else { return };
        options.validate();

        self.capture_pending_layout_anchor(None);
        let previous_layout = self.get_layout();
        let (previous_options, previous_item_metrics) = {
            let state = self.state.borrow();
            (state.options.clone(), state.item_metrics_cache)
        };

        if should_clear_pool(&previous_options, &options) {
            self.invalidate_element_pool();
        }

        let next_item_metrics = super::options::compute_item_metrics(options.item_metrics);
        {
            let mut state = self.state.borrow_mut();
            state.options = options.clone();
            state.item_metrics_cache = next_item_metrics;
        }
        // Upstream's per-item options read the viewer's options live, so every
        // instance sees the new ones from here on: the anchor the next render
        // captures asks the instances for line positions under them, before the
        // layout is recomputed.
        let items = self.state.borrow().items.clone();
        for item in &items {
            self.push_item_options(item);
        }
        let item_metrics_changed = previous_item_metrics != next_item_metrics;
        let layout_changed = previous_layout != self.get_layout();
        if layout_changed {
            self.sync_layout();
        }

        let item_layout_changed =
            item_metrics_changed || has_item_layout_option_changed(&previous_options, &options);
        if item_layout_changed {
            let mut state = self.state.borrow_mut();
            let previous_reset = state.pending_layout_reset;
            state.pending_layout_reset = Some(PendingCodeViewLayoutReset {
                metrics: if item_metrics_changed {
                    Some(next_item_metrics)
                } else {
                    previous_reset.and_then(|reset| reset.metrics)
                },
                reset_file_layout_cache: true,
                reset_diff_layout_cache: true,
                include_estimated_diff_heights: previous_reset
                    .is_some_and(|reset| reset.include_estimated_diff_heights)
                    || item_metrics_changed
                    || has_code_view_diff_estimate_option_changed(&previous_options, &options),
            });
        }

        if layout_changed || item_layout_changed {
            self.mark_layout_dirty_from_index(0);
            self.state.borrow_mut().scroll_dirty = true;
        }

        if !previous_options.is_equal(&options) {
            self.state.borrow_mut().render_options_revision += 1;
        }

        // Render when there are items, OR when the header/footer presence
        // changed: an otherwise-empty CodeView still needs a render to
        // mount/unmount its hosts.
        let header_footer_changed = previous_options.render_code_view_header
            != options.render_code_view_header
            || previous_options.render_code_view_footer != options.render_code_view_footer;
        if !self.state.borrow().items.is_empty() || header_footer_changed {
            self.render(false);
        }
    }
}
