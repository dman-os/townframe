// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C5: the render pass. `scrollTo` and `resolveEffectiveScrollBehavior`'s
// caller, 1576-1617; `render`, 1895-1904; `computeRenderRangeAndEmit`,
// 3465-3745; `flushManagers`, 3747-3753; `getFitPerfectlyOverscroll`,
// 4440-4443; and the module function `renderItem`, 4553-4571.)
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `performance.now()` (the default timestamp of `computeRenderRangeAndEmit`
//   and the start of a smooth scroll's animation) is `CodeViewHost::now`. The
//   frame timestamps the render queue hands over are the same clock.
// * `CodeView.__STOP`, `isReady()` (the worker pool and highlighter readiness
//   gate), `attachItemEditor`, `flushSlotCoordinator`,
//   `validateStickyContainerHeight` and `primeScrollTarget` are DROP
//   (dropped.psv): the highlights backend is synchronous, edit mode and the
//   React slot coordinator are not ported, and the validators read live layout.
// * A `Set` of updated items iterates in insertion order and `flushManagers`
//   observes that order, so the pass keeps the items in a `Vec` for the flush
//   and in an `UpdatedItems` set (by allocation) for `reconcileRenderedItems`.
// * The per-item options the instance reads lazily upstream are pushed to it
//   (`push_item_options`) when the item's `renderedOptionsRevision` differs
//   from the viewer's, which is when upstream's getters would answer
//   differently (closure notes, design point 2).
// * `scrollAnchor === this.pendingLayoutAnchor` compares identity upstream:
//   `getScrollAnchor` answers the pending anchor itself whenever its item is
//   live and a computed anchor is a new object, so the pass compares the
//   captured anchor with the field by value (a computed anchor never carries
//   the id of the pending anchor's skipped item).

use std::rc::Rc;

use super::CodeView;
use super::CodeViewHost;
use super::shell::UpdatedItems;
use super::shell::item_key;
use super::state::ItemKind;
use super::state::ItemRef;
use super::state::ScrollToAnimation;
use crate::components::file_diff::FileDiffRenderProps;
use crate::components::file_diff::LineAnnotations;
use crate::components::file_diff::RenderedContent;
use crate::document::NodeKey;
use crate::types::view::CodeViewScrollBehavior;
use crate::types::view::CodeViewScrollTarget;
use crate::utils::create_window_from_scroll_position::WindowFromScrollPositionProps;
use crate::utils::create_window_from_scroll_position::create_window_from_scroll_position;

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `scrollTo(target)`: resolve the target now, then let the next render
    /// frame move the scroll (instantly, or one spring step per frame).
    pub fn scroll_to(&self, target: &CodeViewScrollTarget) {
        if self.state.borrow().root.is_none() {
            return;
        }

        let Some(pending_target) = self.normalize_scroll_target(target) else {
            return;
        };

        let Some(destination) = self.resolve_scroll_target_top(&pending_target) else {
            return;
        };

        let behavior = self.resolve_effective_scroll_behavior(&pending_target, destination);
        if behavior == CodeViewScrollBehavior::Smooth {
            // Use ??= so if we have an animation in progress it will be smoothly
            // transitioned into the new target and not reset
            if self.state.borrow().scroll_animation.is_none() {
                let position = self.get_scroll_top();
                self.state.borrow_mut().scroll_animation = Some(ScrollToAnimation {
                    position,
                    velocity: 0.0,
                    // Since we kick off a render to requestAnimationFrame, by
                    // initializing lastTimestamp as performance.now() it means
                    // we can begin animating on the next render call and not
                    // wait a frame to get frame time
                    last_timestamp: self.host.now(),
                });
            }
        } else {
            self.state.borrow_mut().scroll_animation = None;
        }

        // We'll attempt to scroll to this new target on the next render frame
        self.suspend_scroll_interactions();
        {
            let mut state = self.state.borrow_mut();
            state.pending_layout_anchor = None;
            state.pending_scroll_target = Some(pending_target);
        }
        self.render(false);
    }

    /// `render(immediate)`: queue `computeRenderRangeAndEmit` on the next frame,
    /// or run it now (cancelling the queued one).
    pub fn render(&self, immediate: bool) {
        #[cfg(test)]
        self.render_calls.borrow_mut().push(immediate);
        if immediate {
            self.render_queue
                .dequeue_render(&self.compute_render_range_and_emit);
            self.compute_render_range_and_emit(None);
        } else {
            self.render_queue
                .queue_render(&self.compute_render_range_and_emit);
        }
    }

    /// `renderItem(item, fileContainer, forceRender)`
    fn render_item(
        &self,
        item: &ItemRef<Host, Rendered, LAnnotation>,
        file_container: Option<NodeKey>,
        force_render: bool,
    ) -> bool {
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
        instance.render(FileDiffRenderProps {
            defer_managers: true,
            file_container,
            file_diff: Some(file_diff),
            force_render,
            line_annotations: Some(annotations),
            ..FileDiffRenderProps::default()
        })
    }

    /// Render an item the pass mounted or kept, and record it as updated when
    /// its render changed anything.
    fn render_mounted_item(
        &self,
        item: &ItemRef<Host, Rendered, LAnnotation>,
        file_container: Option<NodeKey>,
        updated_items: &mut Vec<ItemRef<Host, Rendered, LAnnotation>>,
    ) {
        let render_options_revision = self.state.borrow().render_options_revision;
        let revision_differs = item.borrow().rendered_options_revision != render_options_revision;
        if revision_differs {
            self.push_item_options(item);
        }
        // A mounted item re-renders when the options moved on since it last did
        let force_render = file_container.is_none() && revision_differs;
        if self.render_item(item, file_container, force_render) {
            item.borrow_mut().rendered_options_revision = render_options_revision;
            updated_items.push(Rc::clone(item));
        }
    }

    /// `flushManagers(updatedItems)`
    fn flush_managers(&self, updated_items: &[ItemRef<Host, Rendered, LAnnotation>]) {
        for item in updated_items {
            let instance = Rc::clone(item.borrow().instance());
            instance.file_diff().flush_managers();
        }
    }

    /// `getFitPerfectlyOverscroll()`: we actually need a bit of overscroll even
    /// when attempting to fit perfectly because we round to the nearest
    /// container and we may need to render the gaps before and after a
    /// perfectly fit element to include the spacing between. We do this by
    /// adding the gap and header height above and below the viewport.
    fn get_fit_perfectly_overscroll(&self) -> f64 {
        self.get_layout().gap + self.state.borrow().item_metrics_cache.diff_header_height
    }

    /// `computeRenderRangeAndEmit(timestamp = performance.now())`: one render
    /// frame. Lays out what is dirty, resolves where the scroll should be this
    /// frame (anchors, a pending target, the spring), mounts and unmounts the
    /// items of the render window, reconciles the measured heights and
    /// positions the sticky slab.
    pub(crate) fn compute_render_range_and_emit(&self, timestamp: Option<f64>) {
        if self.state.borrow().container.is_none() {
            return;
        }
        let timestamp = timestamp.unwrap_or_else(|| self.host.now());

        // Read the current viewport and logical scroll position before making
        // DOM mutations, then capture an anchor that can survive layout
        // recalculation.
        let viewport_height = self.get_height();
        let initial_scroll_top = self.get_scroll_top();
        let mut scroll_top_after_layout = initial_scroll_top;
        // Typically a pendingLayoutAnchor will be created from a setOptions
        // call, that will force us to attempt to fit to a new scroll position to
        // not allow the viewport to jump around on us. This can also be
        // triggered later on if a particular item marks itself as dirty
        let mut compute_scroll_correction = self.state.borrow().pending_layout_anchor.is_some();
        // We need to grab the anchor before we re-compute any layout updates, or
        // else we'll get invalid anchor reference data. If we have a
        // pendingLayoutAnchor it will just grab that for us instead of
        // attempting to compute one
        let mut scroll_anchor = self.get_scroll_anchor(scroll_top_after_layout, None);

        // If any item marked itself as dirty, we should re-compute everything
        // after it and then force a new scroll top correction if we aren't
        // already
        let dirty = {
            let mut state = self.state.borrow_mut();
            state
                .layout_dirty_index
                .take()
                .map(|index| (index, state.pending_layout_reset.take()))
        };
        if let Some((index, reset)) = dirty {
            self.recompute_layout(index, reset);
            compute_scroll_correction = true;
        }

        // If layout shifted, resolve the logical scrollTop that keeps the
        // captured anchor in the same viewport position.
        if compute_scroll_correction
            && let Some(anchor) = &scroll_anchor
            && let Some(anchored_scroll_top_after_layout) = self.resolve_anchored_scroll_top(anchor)
        {
            let layout_anchor_delta = anchored_scroll_top_after_layout - scroll_top_after_layout;
            scroll_top_after_layout = anchored_scroll_top_after_layout;
            if let Some(animation) = &mut self.state.borrow_mut().scroll_animation {
                // If we have a delta measurement adjustment, we have to pass
                // that change onto the scroll animation to ensure the animation
                // remains stable, later on
                animation.position += layout_anchor_delta;
            }
        }
        // Recomputing layout can shrink the scroll range, for example when
        // items collapse, so clamp current scroll position and update DOM so
        // scroll changes are valid before deriving the render window for this
        // frame.
        if compute_scroll_correction {
            scroll_top_after_layout = self.clamp_scroll_top(scroll_top_after_layout);
            self.sync_container_height();
        }

        // Resolve the logical scrollTop this render frame should target. The
        // paged root scrollTop is derived later only if the scaffold needs to
        // move.
        let target_scroll_top =
            self.compute_target_scroll_top_for_frame(scroll_top_after_layout, timestamp);

        // When performing very large scroll jumps, we should attempt to render
        // the bare minimum to ensure we can paint quickly. We'll queue up
        // another render at the end to fill things out on the next tick. If we
        // had to correct layout-adjusted scroll state then we should not
        // fitPerfectly because there's a good chance we'll be re-rendering the
        // same elements again
        let fit_perfectly = !compute_scroll_correction && {
            let rendered_scroll_top = self.state.borrow().render_state.scroll_top;
            rendered_scroll_top == -1.0
                || (target_scroll_top - rendered_scroll_top).abs()
                    > viewport_height + self.config.overscroll_size * 2.0
        };

        // If we are doing a `fitPerfectly` render it means we are rendering
        // completely new content which means no need to scroll fix anything
        if fit_perfectly {
            scroll_anchor = None;
        }

        // Compute the projected logical window, then synchronize the paged
        // scroll scaffold before mutating rendered items.
        let header_height = self.state.borrow().header.height;
        let window_specs = create_window_from_scroll_position(WindowFromScrollPositionProps {
            // The window is in item-space (0 = first item's top); subtract the
            // header so a tall header can't desync which items fall inside the
            // render window.
            scroll_top: target_scroll_top - header_height,
            height: viewport_height,
            scroll_height: self.get_scroll_height(),
            fit_perfectly,
            fit_perfectly_overscroll: self.get_fit_perfectly_overscroll(),
            overscroll_size: self.config.overscroll_size,
        });
        self.state.borrow_mut().window_specs = window_specs;
        let mut synced_scroll_top = initial_scroll_top;
        if (self.state.borrow().pending_scroll_target.is_some()
            && target_scroll_top != synced_scroll_top)
            || self.needs_scroll_page_update(target_scroll_top)
        {
            // Apply programmatic scrolls and user-driven page rebases before
            // DOM mutations so the browser reconciles the render against the
            // right paged scroll position.
            self.apply_scroll_fix(target_scroll_top, synced_scroll_top, window_specs);
            synced_scroll_top = target_scroll_top;
        }

        // Reconcile the currently mounted DOM against the new projected render
        // window, cleaning up any elements that are no longer visible.
        let (top, bottom) = (window_specs.top, window_specs.bottom);
        let (first_index, last_index) = {
            let state = self.state.borrow();
            (
                state.render_state.first_index,
                state.render_state.last_index,
            )
        };
        if first_index >= 0 {
            for index in first_index..=last_index {
                let item = self
                    .state
                    .borrow()
                    .items
                    .get(index as usize)
                    .cloned()
                    .unwrap_or_else(|| {
                        panic!("CodeView.computeRenderRangeAndEmit: No item at index: {index}")
                    });
                let (item_top, item_height) = {
                    let item = item.borrow();
                    (item.top, item.height)
                };
                let is_visible = item_top > top - item_height && item_top <= bottom;
                // If not visible, we should unmount it and clean it up
                if !is_visible {
                    self.release_rendered_item(&item);
                }
            }
        }

        // Mount/unmount/re-populate the header/footer hosts in the same
        // DOM-mutation window as the items, after the scroll anchor was
        // captured above and before the post-render anchor resolve below.
        let hosts_changed = self.reconcile_header_footer_hosts();

        let mut prev_element: Option<NodeKey> = None;
        let mut updated_items: Vec<ItemRef<Host, Rendered, LAnnotation>> = Vec::new();
        let starting_index = self.find_first_visible_index(top) as i64;
        let last_rendered_index = self
            .find_last_visible_index(bottom)
            .map_or(-1, |index| index as i64);

        for item_index in starting_index..=last_rendered_index {
            let item = self
                .state
                .borrow()
                .items
                .get(item_index as usize)
                .cloned()
                .expect("CodeView.computeRenderRangeAndEmit: missing item");
            let element = item.borrow().element;
            match element {
                // If the item isn't rendered yet, we need to create a wrapper
                // element for it and render it
                None => {
                    let element = self.acquire_element();
                    item.borrow_mut().element = Some(element);
                    self.sync_rendered_item_order(element, prev_element);
                    let instance = Rc::clone(item.borrow().instance());
                    instance.file_diff().virtualized().virtualized_setup();
                    self.render_mounted_item(&item, Some(element), &mut updated_items);
                    prev_element = Some(element);
                }
                // Otherwise kick off a render as necessary
                Some(element) => {
                    self.sync_rendered_item_order(element, prev_element);
                    self.render_mounted_item(&item, None, &mut updated_items);
                    prev_element = Some(element);
                }
            }
        }

        {
            let mut state = self.state.borrow_mut();
            state.render_state.first_index = if starting_index <= last_rendered_index {
                starting_index
            } else {
                -1
            };
            state.render_state.last_index = last_rendered_index;
        }

        self.flush_managers(&updated_items);
        // Read phase: measure a freshly mounted or re-populated host now so its
        // getBoundingClientRect batches into the same reflow as the item height
        // reads.
        if hosts_changed {
            self.measure_mounted_hosts();
        }
        let updated_keys: UpdatedItems = updated_items.iter().map(item_key).collect();
        self.reconcile_rendered_items(Some(&updated_keys));
        self.sync_container_height();
        self.update_sticky_positioning();

        // Now that the dom has been flushed and we've computed our updated
        // item/line metrics, we should attempt to resolve any scroll anchors
        // and scroll animation data. We have already applied the desired scroll
        // position before rendering, so the only scroll changes should be to
        // scrollFix from lines that did not match their computed state.
        //
        // - No pending scrollTo target -> Only attempt to scrollFix if there's
        //   a mismatch
        // - Instant pending scrollTo target -> Resolve target anchor position
        //   and apply any necessary scroll fixes
        // - Smooth pending scrollTo target -> Apply necessary scrollFix if
        //   necessary and rebase/update the outstanding spring animation
        //   values.
        let anchored_scroll_top_after_render = scroll_anchor
            .as_ref()
            .and_then(|anchor| self.resolve_anchored_scroll_top(anchor));
        {
            let mut state = self.state.borrow_mut();
            if scroll_anchor == state.pending_layout_anchor {
                state.pending_layout_anchor = None;
            }
        }
        // The amount of computed layout shift from the render
        let post_render_anchor_delta = anchored_scroll_top_after_render
            .map_or(0.0, |anchored| anchored - scroll_top_after_layout);

        let post_render_scroll_top;
        let mut should_check_pending_target_settled = false;
        if self.state.borrow().pending_scroll_target.is_some() {
            match self.advance_scroll_animation(timestamp, post_render_anchor_delta) {
                Some(pending_target_scroll_top) => {
                    post_render_scroll_top = pending_target_scroll_top;
                    should_check_pending_target_settled = true;
                }
                // If something bad happened with our pending scroll target,
                // then we'd fall back here. Unlikely to happen in practice, but
                // we need to reset the scrollTop if so
                None => post_render_scroll_top = scroll_top_after_layout,
            }
        } else {
            post_render_scroll_top = anchored_scroll_top_after_render.unwrap_or(target_scroll_top);
        }
        // If the new intended scroll position has changed, we should apply that
        // now to bring everything in line
        if post_render_scroll_top != synced_scroll_top {
            self.apply_scroll_fix(post_render_scroll_top, synced_scroll_top, window_specs);
            synced_scroll_top = post_render_scroll_top;
        }
        if should_check_pending_target_settled {
            let pending = self.state.borrow().pending_scroll_target.clone();
            if let Some(pending) = pending
                && self.is_pending_target_settled(&pending)
            {
                let mut state = self.state.borrow_mut();
                state.pending_scroll_target = None;
                state.scroll_animation = None;
            }
        }
        let rounded_synced_scroll_top = self.round_to_device_pixel(synced_scroll_top);
        self.state.borrow_mut().render_state.scroll_top = rounded_synced_scroll_top;

        // The post-render scroll-correction block above can call applyScrollFix
        // -> syncPagedScrollScaffolding -> applyStickyPositioning, which
        // recomputes renderState.stickyHeight from getStickyBounds(windowSpecs)
        // for the corrected scroll position. The rendered DOM slice was
        // committed earlier in this frame for the pre-correction window and is
        // not re-rendered here, so that windowSpecs-based value can diverge from
        // the committed slice by the scroll-correction delta. Recompute sticky
        // positioning from the committed renderRange (no-arg path) so
        // renderState.stickyHeight matches the slice actually in the DOM.
        self.update_sticky_positioning();

        self.fix_container_focus();

        // If we are hitting a fitPerfectly heuristic, we should queue up another
        // render to fill out content. If we are performing a scroll animation
        // we'll need another render to continue.
        if fit_perfectly || self.state.borrow().scroll_animation.is_some() {
            self.render(false);
        }
    }
}
