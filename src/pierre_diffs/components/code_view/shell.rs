// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C4: the DOM shell. `suspendScrollInteractions`, `restoreScrollInteractions`,
// `reconcileHeaderFooterHosts`, `reconcileHost`, `setHostHeight`,
// `measureMountedHosts`, `setup`, `reset`, `cleanUp`, `cleanAllRenderedItems`,
// `syncContainerHeight`, `getStickyBounds`, `applyStickyPositioning`,
// `syncPagedScrollScaffolding`, `applyScrollFix`, `reconcileRenderedItems`,
// `updateStickyPositioning`, `handleScroll`, `clearPendingScroll`,
// `handleResize`, `subscribeToScroll`, `notifyScroll`, and
// `createCodeViewHeaderFooterHostElement` from
// packages/diffs/src/utils/createCodeViewHeaderFooterHostElement.ts.)
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * The class's DOM (`root`, `container`, `stickyContainer`, `stickyOffset`, the
//   hosts) is `Document` nodes; structure and style writes go through the
//   `Document`. The writes that happen on every scrolled frame (the slab's
//   spacer height, `top` and `bottom`, and the container height) go through
//   three narrow `CodeViewHost` methods (`write_slab_spacer`,
//   `write_slab_sticky`, `write_container_height`) whose defaults are the
//   `Document` writes, so a host that renders the slab on its own path
//   overrides those three and nothing else.
// * Browser events arrive as calls: `handle_scroll`, `clear_pending_scroll`
//   (one for `wheel`, `touchstart`, `pointerdown` and `keydown`, none of which
//   upstream's handler reads) and `handle_resize(entries)`. The host registers
//   the listeners and the one `ResizeObserver` (`add_shell_listeners`,
//   `create_shell_observer`, `shell_observe`).
// * `setTimeout(restoreScrollInteractions)` is `ScrollSyncHost::set_timeout`; the
//   closure holds a `Weak` and does nothing for a viewer that is gone.
// * `Math.random()` is `CodeViewHost::random` (Q9). `MOBILE_SAFARI`, a module
//   constant upstream, is `CodeView::mobile_safari`, computed from the host's
//   `PlatformInfo` in `new`.
// * `validateRenderedItemHeight` (a dev-only `console.error`),
//   `window.__INSTANCE`, `window.__TOGGLE` and `CodeView.__STOP` are not ported
//   (dropped.psv). `reset`'s editor and slot-coordinator arms are DROP.
// * `slab_nodes` and `scroll_sync_manager_of` are not upstream (C6): a host that
//   renders the slab on its own path has to know which nodes it is
//   (`SlabNodes`), and a host with one scroll listener registry has to find
//   the item whose `ScrollSyncManager` a column's scroll event belongs to.
// * Arena discipline: a node the shell is done with (an unpooled item shell, a
//   torn-down host, the container after `cleanUp`) is `discard`ed; the
//   `Document` frees by owner.
// * `getBoundingClientRect().height` of a host is `element_bounding_height`;
//   `document.activeElement`/`shadowRoot.activeElement`/`root.focus` are
//   `active_element`/`shadow_active_element`/`focus_root`.

use std::collections::HashSet;
use std::rc::Rc;

use super::CodeView;
use super::CodeViewHost;
use super::options::RenderCodeViewSlotCallback;
use super::state::EMPTY_WINDOW_SPECS;
use super::state::ItemRef;
use crate::components::file_diff::RenderedContent;
use crate::components::virtualizer::ScrollBehavior;
use crate::components::virtualizer::VirtualizerHost;
use crate::constants::CODE_VIEW_FOOTER_ATTRIBUTE;
use crate::constants::CODE_VIEW_HEADER_ATTRIBUTE;
use crate::document::NodeKey;
use crate::managers::resize_manager::ResizeObserverEntry;
use crate::managers::scroll_sync_manager::ScrollSyncManager;
use crate::types::layout::RenderWindow;
use crate::types::layout::VirtualWindowSpecs;

const DEFAULT_SCROLL_INTERACTION_RESTORE_DELAY_MS: u32 = 120;
const SUB_PIXEL_TOLERANCE: f64 = 1.0;
const SCROLLING_CODE_OVERFLOW_FIX_VARIABLE: &str = "--diffs-overflow-override";

/// The container, the slab's spacer (`stickyOffset`) and the slab (`stickyContainer`) of the
/// viewer's DOM: `root > [container > [spacer, slab > shell*]]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlabNodes {
    pub container: NodeKey,
    pub spacer: NodeKey,
    pub slab: NodeKey,
}

/// `'header' | 'footer'`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HostKind {
    Header,
    Footer,
}

/// `StickyBounds`
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct StickyBounds {
    pub sticky_top: f64,
    pub sticky_bottom: f64,
}

/// The `Set<CodeViewContextItem>` of items a render updated, by allocation.
pub(crate) type UpdatedItems = HashSet<usize>;

/// An item's identity in an [`UpdatedItems`] set.
pub(crate) fn item_key<Host, Rendered, LAnnotation>(
    item: &ItemRef<Host, Rendered, LAnnotation>,
) -> usize
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    Rc::as_ptr(item).cast::<()>() as usize
}

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    pub(super) fn document(&self) -> &crate::document::Document {
        VirtualizerHost::document(&*self.host)
    }

    /// `shouldDisablePointerEvents()`
    fn should_disable_pointer_events(&self) -> bool {
        self.state.borrow().options.pointer_events_on_scroll != Some(true)
    }

    /// `clearScrollInteractionTimer()`
    fn clear_scroll_interaction_timer(&self) {
        let timer = self.state.borrow_mut().scroll_interaction_fix_timer.take();
        if let Some(timer) = timer {
            self.host.clear_timeout(timer);
        }
    }

    /// `suspendScrollInteractions()`
    pub(crate) fn suspend_scroll_interactions(&self) {
        self.clear_scroll_interaction_timer();

        if self.should_disable_pointer_events() && !self.state.borrow().pointer_events_disabled {
            self.document()
                .set_style_property(self.sticky_container, "pointer-events", "none");
            self.state.borrow_mut().pointer_events_disabled = true;
        }

        // This is a really important fix for mobile safari; under aggressive
        // scroll conditions we'll eventually crash/reload the page. It appears
        // to be caused by the fact that the code wrapper elements are
        // horizontally scrollable, so while aggressively scrolling, we disable
        // scrolling. We don't want to apply this fix to good browsers since in
        // those cases it can fuck with layout in ways that aren't appropriate
        if self.mobile_safari && !self.state.borrow().code_overflow_fix {
            self.document().set_style_property(
                self.sticky_container,
                SCROLLING_CODE_OVERFLOW_FIX_VARIABLE,
                "hidden",
            );
            self.state.borrow_mut().code_overflow_fix = true;
        }

        let weak = std::rc::Weak::clone(&self.weak_self);
        let timer = self.host.set_timeout(
            Box::new(move || {
                // A viewer that was dropped has nothing left to restore.
                if let Some(view) = weak.upgrade() {
                    view.restore_scroll_interactions();
                }
            }),
            DEFAULT_SCROLL_INTERACTION_RESTORE_DELAY_MS,
        );
        self.state.borrow_mut().scroll_interaction_fix_timer = Some(timer);
    }

    /// `restoreScrollInteractions()`
    pub(crate) fn restore_scroll_interactions(&self) {
        self.clear_scroll_interaction_timer();

        if self.state.borrow().pointer_events_disabled {
            self.document()
                .remove_style_property(self.sticky_container, "pointer-events");
            self.state.borrow_mut().pointer_events_disabled = false;
        }

        if self.state.borrow().code_overflow_fix {
            self.document().set_style_property(
                self.sticky_container,
                SCROLLING_CODE_OVERFLOW_FIX_VARIABLE,
                "auto",
            );
            self.state.borrow_mut().code_overflow_fix = false;
        }
    }

    /// `createCodeViewHeaderFooterHostElement(type, container, resizeObserver)`:
    /// create, style, position, and observe a header/footer host element.
    /// `flow-root` gives it a block formatting context so the caller's content
    /// margins stay inside it, keeping measurement / ResizeObserver heights
    /// accurate.
    fn create_header_footer_host_element(&self, kind: HostKind, container: NodeKey) -> NodeKey {
        let document = self.document();
        let element = document.create_element("div");
        document.set_style_property(element, "display", "flow-root");
        match kind {
            HostKind::Header => {
                document.set_attribute(element, CODE_VIEW_HEADER_ATTRIBUTE, "");
                document.before(container, element);
            }
            HostKind::Footer => {
                document.set_attribute(element, CODE_VIEW_FOOTER_ATTRIBUTE, "");
                document.after(container, element);
            }
        }
        if self.state.borrow().resize_observing {
            self.host.shell_observe(&element);
        }
        element
    }

    /// `reconcileHeaderFooterHosts()`: mount/unmount/re-populate the header and
    /// footer hosts to match the current `renderCodeViewHeader`/`Footer`
    /// options. Returns true when a host's content changed (a fresh mount or a
    /// swapped callback) so the render cycle takes a synchronous height
    /// measurement in the following read phase.
    pub(crate) fn reconcile_header_footer_hosts(&self) -> bool {
        let header_changed = self.reconcile_host(HostKind::Header);
        let footer_changed = self.reconcile_host(HostKind::Footer);
        header_changed || footer_changed
    }

    /// `reconcileHost(type)`: create + position + populate a host when its
    /// callback first appears, re-populate it when the callback reference
    /// changes, and tear it down when the callback is removed. Otherwise the
    /// mounted host is left untouched (its content is owned by the caller).
    /// Returns whether its content changed.
    fn reconcile_host(&self, kind: HostKind) -> bool {
        let (root, container, render, element, mounted_render) = {
            let state = self.state.borrow();
            let (render, host) = match kind {
                HostKind::Header => (&state.options.render_code_view_header, &state.header),
                HostKind::Footer => (&state.options.render_code_view_footer, &state.footer),
            };
            (
                state.root,
                state.container,
                render.clone(),
                host.element,
                host.render.clone(),
            )
        };
        let (Some(_), Some(container)) = (root, container) else {
            return false;
        };
        let document = self.document();

        // Callback removed → the host should not exist; tear it down and reset
        // height.
        let Some(render) = render else {
            let Some(element) = element else {
                return false;
            };
            if self.state.borrow().resize_observing {
                self.host.shell_unobserve(&element);
            }
            document.discard(element);
            {
                let mut state = self.state.borrow_mut();
                let host = self.host_mut(&mut state, kind);
                host.element = None;
                host.render = None;
            }
            self.set_host_height(kind, 0.0);
            return false;
        };

        // Same callback on a mounted host → its content is already current (the
        // common per-frame path).
        if element.is_some() && mounted_render.as_ref() == Some(&render) {
            return false;
        }

        // Callback added or swapped → ensure the host exists, then repopulate
        // it from the callback's latest output. A returned node replaces the
        // content; a nullish return empties the host. (Upstream leaves the
        // light DOM of a container-managed host alone; the Dioxus view is not
        // component-managed, Q5.)
        let element =
            element.unwrap_or_else(|| self.create_header_footer_host_element(kind, container));
        {
            let mut state = self.state.borrow_mut();
            self.host_mut(&mut state, kind).element = Some(element);
        }
        let content = render();
        let previous = document.child_nodes(element);
        match content {
            Some(content) => {
                let node = content.to_node(document);
                document.replace_children(element, &[node]);
            }
            // `element.children.length > 0`: a text node left by an earlier
            // callback is not an element child, so a nullish return leaves it
            // (upstream's behaviour, kept).
            None if !document.children(element).is_empty() => {
                document.replace_children(element, &[]);
            }
            None => return self.finish_host(kind, render),
        }
        for node in previous {
            document.discard(node);
        }
        self.finish_host(kind, render)
    }

    /// `host.render = render` and the content-changed result of `reconcileHost`.
    fn finish_host(&self, kind: HostKind, render: RenderCodeViewSlotCallback<Rendered>) -> bool {
        self.host_mut(&mut self.state.borrow_mut(), kind).render = Some(render);
        true
    }

    fn host_mut<'a>(
        &self,
        state: &'a mut super::State<Host, Rendered, LAnnotation>,
        kind: HostKind,
    ) -> &'a mut super::state::HeaderFooterHost<Rendered> {
        match kind {
            HostKind::Header => &mut state.header,
            HostKind::Footer => &mut state.footer,
        }
    }

    /// `setHostHeight(host, height)`: store a host's measured height, flagging
    /// the scroll state dirty when it actually changed so the surrounding
    /// render cycle re-derives the scroll range and re-anchors (the header
    /// offset shifts every item's position).
    fn set_host_height(&self, kind: HostKind, height: f64) {
        let mut state = self.state.borrow_mut();
        let host = self.host_mut(&mut state, kind);
        if host.height == height {
            return;
        }
        host.height = height;
        state.scroll_dirty = true;
    }

    /// `measureMountedHosts()`: read the mounted hosts' heights. Called only in
    /// the render cycle's read phase on a change frame.
    pub(crate) fn measure_mounted_hosts(&self) {
        for kind in [HostKind::Header, HostKind::Footer] {
            let element = {
                let state = self.state.borrow();
                match kind {
                    HostKind::Header => state.header.element,
                    HostKind::Footer => state.footer.element,
                }
            };
            if let Some(element) = element {
                self.set_host_height(kind, self.host.element_bounding_height(&element));
            }
        }
    }

    /// `setup(root)`
    pub fn setup(&self, root: NodeKey) {
        assert!(
            self.state.borrow().root.is_none(),
            "CodeView.setup: already setup"
        );
        let document = self.document();
        self.state.borrow_mut().root = Some(root);
        document.set_style_property(root, "overflow-anchor", "none");
        if !document.has_attribute(root, "tabindex") {
            document.set_attribute(root, "tabindex", "-1");
        }
        let container = *self
            .state
            .borrow_mut()
            .container
            .get_or_insert_with(|| document.create_element("div"));
        // NOTE(amadeus): We can't put `size` in here or it breaks Firefox's
        // sticky headers
        document.set_style_property(container, "contain", "layout style");
        self.sync_layout();
        document.append_child(container, self.sticky_offset);
        document.append_child(container, self.sticky_container);
        document.append_child(root, container);
        {
            let mut state = self.state.borrow_mut();
            state.scroll_dirty = true;
            state.height_dirty = true;
            state.resize_observing = true;
        }
        self.host.create_shell_observer();
        self.host.shell_observe(&self.sticky_container);
        // Any user-driven scroll intent cancels an in-flight programmatic
        // scroll. pointerdown catches scrollbar drag (the scrollbar belongs to
        // root); wheel / touchstart cover trackpad + touch scroll; keydown
        // covers arrow keys, PgUp/PgDn, Home/End on a focused scroll container.
        self.host.add_shell_listeners(&root);
        self.host.shell_observe(&root);
        self.render(true);
    }

    /// `cleanAllRenderedItems()`
    fn clean_all_rendered_items(&self) {
        let (first_index, last_index) = {
            let state = self.state.borrow();
            (
                state.render_state.first_index,
                state.render_state.last_index,
            )
        };
        if first_index == -1 {
            return;
        }
        for index in first_index..=last_index {
            let item = self
                .state
                .borrow()
                .items
                .get(index as usize)
                .cloned()
                .unwrap_or_else(|| {
                    panic!("CodeView.cleanAllRenderedItems: Item does not exist at index: {index}")
                });
            self.release_rendered_item(&item);
        }
    }

    /// `reset()`
    pub fn reset(&self) {
        self.render_queue
            .dequeue_render(&self.compute_render_range_and_emit);
        self.restore_scroll_interactions();
        self.clean_all_rendered_items();
        let discarded_items = {
            let mut state = self.state.borrow_mut();
            state.selected_lines = None;
            state.id_to_item.clear();
            state.instance_to_item.clear();
            std::mem::take(&mut state.items)
        };
        for item in discarded_items {
            let instance = Rc::clone(item.borrow().instance());
            instance.clean_up(false);
        }
        let document = self.document();
        {
            let mut state = self.state.borrow_mut();
            state.layout_dirty_index = None;
            state.pending_layout_reset = None;
        }
        // `stickyContainer.textContent = ''`
        for child in document.child_nodes(self.sticky_container) {
            document.discard(child);
        }
        self.host.write_slab_spacer(self.sticky_offset, None);
        let container = self.state.borrow().container;
        if let Some(container) = container {
            document.remove_style_property(container, "height");
        }
        {
            let mut state = self.state.borrow_mut();
            state.container_height = -1.0;
            state.window_specs = EMPTY_WINDOW_SPECS;
            state.pending_layout_anchor = None;
            state.should_fix_container_focus = false;
            state.height = 0.0;
            state.scroll_top = 0.0;
            state.scroll_page_offset = 0.0;
            state.scroll_height = 0.0;
            state.scroll_dirty = true;
            state.height_dirty = true;
        }
        self.reset_render_state();
    }

    /// `cleanUp()`
    pub fn clean_up(&self) {
        self.reset();
        self.clear_element_pool();
        self.restore_scroll_interactions();
        let document = self.document();
        let (root, container, was_observing) = {
            let mut state = self.state.borrow_mut();
            let was_observing = std::mem::take(&mut state.resize_observing);
            (state.root, state.container, was_observing)
        };
        // `this.resizeObserver?.disconnect()`
        if was_observing {
            self.host.shell_disconnect();
        }
        if let Some(root) = root {
            self.host.remove_shell_listeners(&root);
            document.remove_style_property(root, "overflow-anchor");
        }
        document.remove(self.sticky_offset);
        document.remove(self.sticky_container);
        for child in document.child_nodes(self.sticky_container) {
            document.discard(child);
        }
        if let Some(container) = container {
            // `container.remove()`; nothing else lives in it now, and `setup`
            // makes a new one.
            document.discard(container);
        }
        for kind in [HostKind::Header, HostKind::Footer] {
            let element = {
                let mut state = self.state.borrow_mut();
                let host = self.host_mut(&mut state, kind);
                host.render = None;
                host.height = 0.0;
                host.element.take()
            };
            if let Some(element) = element {
                document.discard(element);
            }
        }
        let mut state = self.state.borrow_mut();
        state.root = None;
        state.container = None;
    }

    /// The three nodes the slab's per-frame writes address (`write_container_height`,
    /// `write_slab_spacer`, `write_slab_sticky`), which a host that renders them on their own
    /// path needs to know before the first pass. `None` after `clean_up`.
    pub fn slab_nodes(&self) -> Option<SlabNodes> {
        Some(SlabNodes {
            container: self.state.borrow().container?,
            spacer: self.sticky_offset,
            slab: self.sticky_container,
        })
    }

    /// The scroll sync manager of the rendered item whose shell holds `element`. Every item's
    /// `FileDiff` owns a manager and registers its split columns' listeners through the one
    /// host, which cannot tell the managers apart: it asks which item a scroll event's node
    /// belongs to.
    pub fn scroll_sync_manager_of(
        &self,
        element: NodeKey,
    ) -> Option<Rc<ScrollSyncManager<Host>>> {
        let (first_index, last_index) = {
            let state = self.state.borrow();
            (
                state.render_state.first_index,
                state.render_state.last_index,
            )
        };
        if first_index == -1 {
            return None;
        }
        let document = self.document();
        (first_index..=last_index).find_map(|index| {
            let item = self.state.borrow().items.get(index as usize).cloned()?;
            let item = item.borrow();
            let shell = item.element?;
            document
                .is_attached(element, shell)
                .then(|| Rc::clone(item.instance().file_diff().scroll_sync_manager()))
        })
    }

    /// `subscribeToScroll(listener)`: the returned closure unsubscribes.
    pub fn subscribe_to_scroll(
        &self,
        listener: super::ScrollListener<Host, Rendered, LAnnotation>,
    ) -> Box<dyn FnOnce()> {
        let id = {
            let mut state = self.state.borrow_mut();
            state.next_scroll_listener_id += 1;
            let id = state.next_scroll_listener_id;
            state.scroll_listeners.push((id, listener));
            id
        };
        let weak = std::rc::Weak::clone(&self.weak_self);
        Box::new(move || {
            if let Some(view) = weak.upgrade() {
                view.state
                    .borrow_mut()
                    .scroll_listeners
                    .retain(|(listener_id, _)| *listener_id != id);
            }
        })
    }

    /// `notifyScroll()`
    fn notify_scroll(&self) {
        // Avoid DOM thrash of checking scroll position if we don't need it
        if self.state.borrow().scroll_listeners.is_empty() {
            return;
        }
        let scroll_top = self.get_scroll_top();
        let listeners: Vec<_> = self
            .state
            .borrow()
            .scroll_listeners
            .iter()
            .map(|(_, listener)| Rc::clone(listener))
            .collect();
        for listener in listeners {
            listener(scroll_top, self);
        }
    }

    /// `syncContainerHeight()`
    pub(crate) fn sync_container_height(&self) {
        let paged_scroll_height = self.get_paged_scroll_height();
        let (container, container_height) = {
            let state = self.state.borrow();
            (state.container, state.container_height)
        };
        let Some(container) = container else {
            return;
        };
        if container_height == paged_scroll_height {
            return;
        }
        self.host
            .write_container_height(container, paged_scroll_height);
        self.state.borrow_mut().container_height = paged_scroll_height;
    }

    /// `getStickyBounds(windowSpecs?)`
    pub(crate) fn get_sticky_bounds(
        &self,
        window_specs: Option<VirtualWindowSpecs>,
    ) -> Option<StickyBounds> {
        let (first_index, last_index) = match window_specs {
            Some(window_specs) => (
                self.find_first_visible_index(window_specs.top) as i64,
                self.find_last_visible_index(window_specs.bottom)
                    .map_or(-1, |index| index as i64),
            ),
            None => {
                let state = self.state.borrow();
                (
                    state.render_state.first_index,
                    state.render_state.last_index,
                )
            }
        };
        if first_index == -1 || last_index == -1 || first_index > last_index {
            return None;
        }
        let (first, last) = {
            let state = self.state.borrow();
            (
                state.items.get(first_index as usize).cloned(),
                state.items.get(last_index as usize).cloned(),
            )
        };
        let render_window = window_specs.map(|window_specs| RenderWindow {
            top: window_specs.top,
            bottom: window_specs.bottom,
        });
        let first_sticky_specs = first?
            .borrow()
            .instance()
            .get_advanced_sticky_specs(render_window)?;
        let last_sticky_specs = last?
            .borrow()
            .instance()
            .get_advanced_sticky_specs(render_window)?;
        Some(StickyBounds {
            sticky_top: self.get_paged_layout_top(first_sticky_specs.top_offset.max(0.0)),
            sticky_bottom: self
                .get_paged_layout_top(last_sticky_specs.top_offset + last_sticky_specs.height),
        })
    }

    /// `applyStickyPositioning({ stickyTop, stickyBottom })`: the slab's writes,
    /// on every scrolled frame the slab moves.
    pub(crate) fn apply_sticky_positioning(&self, bounds: StickyBounds) {
        let StickyBounds {
            sticky_top,
            sticky_bottom,
        } = bounds;
        let height = self.get_height();
        let item_metrics = self.state.borrow().item_metrics_cache;
        let sticky_container_height = sticky_bottom - sticky_top;

        {
            let mut state = self.state.borrow_mut();
            state.render_state.sticky_height = sticky_container_height;
            state.render_state.sticky_top = sticky_top;
            state.render_state.sticky_bottom = sticky_bottom;
        }

        self.host
            .write_slab_spacer(self.sticky_offset, Some(sticky_top));
        // NOTE(amadeus): Wee polish lad -- when dragging the scrollbar up or
        // down quickly, this prevents the laggy scroll view from lining up with
        // the numbers exactly
        let random_offset = -(self.host.random() * item_metrics.line_height).trunc();
        let sticky_jitter = -(sticky_container_height + random_offset).max(0.0) + height;
        self.host.write_slab_sticky(
            self.sticky_container,
            sticky_jitter,
            sticky_jitter + item_metrics.diff_header_height,
        );
    }

    /// `syncPagedScrollScaffolding(windowSpecs)`
    pub(crate) fn sync_paged_scroll_scaffolding(&self, window_specs: VirtualWindowSpecs) {
        self.sync_container_height();
        match self.get_sticky_bounds(Some(window_specs)) {
            None => self.host.write_slab_spacer(
                self.sticky_offset,
                Some(self.get_paged_layout_top(window_specs.top)),
            ),
            Some(sticky_bounds) => self.apply_sticky_positioning(sticky_bounds),
        }
    }

    /// `applyScrollFix(targetScrollTop, syncedScrollTop, windowSpecs)`
    pub(crate) fn apply_scroll_fix(
        &self,
        target_scroll_top: f64,
        synced_scroll_top: f64,
        window_specs: VirtualWindowSpecs,
    ) {
        let Some(root) = self.state.borrow().root else {
            return;
        };
        let rounded_target_scroll_top =
            self.round_to_device_pixel(self.clamp_scroll_top(target_scroll_top));
        let rounded_synced_scroll_top = self.round_to_device_pixel(synced_scroll_top);

        let previous_page_offset = self.state.borrow().scroll_page_offset;
        let synced_paged_scroll_top = self.round_to_device_pixel(
            self.clamp_paged_scroll_top(rounded_synced_scroll_top - previous_page_offset),
        );
        let super::scroll::PagedScrollPosition {
            paged_scroll_top,
            scroll_page_offset,
        } = self.resolve_paged_scroll_position(rounded_target_scroll_top);
        let target_paged_scroll_top = paged_scroll_top;

        let rebase_changed = previous_page_offset != scroll_page_offset;
        if rounded_target_scroll_top == self.state.borrow().render_state.scroll_top
            && rounded_target_scroll_top == rounded_synced_scroll_top
            && target_paged_scroll_top == synced_paged_scroll_top
            && !rebase_changed
        {
            return;
        }
        self.suspend_scroll_interactions();
        if target_paged_scroll_top != synced_paged_scroll_top || rebase_changed {
            self.state.borrow_mut().scroll_page_offset = scroll_page_offset;
            self.sync_paged_scroll_scaffolding(window_specs);
        }
        if target_paged_scroll_top != synced_paged_scroll_top {
            self.host
                .element_scroll_to(&root, target_paged_scroll_top, ScrollBehavior::Instant);
        }
        // Keep cached scroll state in sync with writes we performed ourselves,
        // so later reads do not need to touch layout just to discover the same
        // value.
        let mut state = self.state.borrow_mut();
        state.render_state.scroll_top = rounded_target_scroll_top;
        state.scroll_top = rounded_target_scroll_top;
        state.scroll_dirty = false;
    }

    /// `reconcileRenderedItems(updatedItems?)`
    pub(crate) fn reconcile_rendered_items(&self, updated_items: Option<&UpdatedItems>) {
        let (first_index, last_index) = {
            let state = self.state.borrow();
            (
                state.render_state.first_index,
                state.render_state.last_index,
            )
        };
        if first_index == -1 {
            return;
        }

        let gap = self.get_layout().gap;
        let items_len = self.state.borrow().items.len();
        let mut current_top = -1.0_f64;
        let mut height_changed = false;
        // Iterate through the rendered items to reconcile height. If a height
        // has changed, we'll have to iterate all the way till the end to update
        // all appropriate heights
        for index in first_index as usize..items_len {
            // If we've incurred no height changes and ended, we can abort
            if !height_changed && index as i64 > last_index {
                break;
            }
            let item = self
                .state
                .borrow()
                .items
                .get(index)
                .cloned()
                .expect("CodeView.reconcileRenderedItems: Invalid item");
            let instance = Rc::clone(item.borrow().instance());
            let item_top = item.borrow().top;
            if current_top == -1.0 {
                current_top = item_top;
            } else if item_top != current_top {
                item.borrow_mut().top = current_top;
                instance.sync_virtualized_top();
                height_changed = true;
            }
            // If updatedInstances provided, only reconcile those. If not
            // provided (resize path), reconcile all rendered items.
            let reconcile = match updated_items {
                None => index as i64 <= last_index,
                Some(updated_items) => updated_items.contains(&item_key(&item)),
            };
            if reconcile && instance.reconcile_heights() {
                height_changed = true;
                item.borrow_mut().height = instance.get_virtualized_height();
            }
            current_top += instance.get_virtualized_height();
            if index < items_len - 1 {
                current_top += gap;
            }
        }

        if height_changed {
            let mut state = self.state.borrow_mut();
            state.scroll_dirty = true;
            state.scroll_height = current_top;
        }
    }

    /// `updateStickyPositioning()`
    pub(crate) fn update_sticky_positioning(&self) {
        let Some(sticky_bounds) = self.get_sticky_bounds(None) else {
            // No rendered slice means no sticky scaffold: clear the spacer so an
            // emptied viewer sheds the offset height captured from the last
            // rendered layout instead of keeping phantom space in the container.
            if self.state.borrow().render_state.first_index == -1 {
                self.host.write_slab_spacer(self.sticky_offset, None);
            }
            return;
        };
        let StickyBounds {
            sticky_top,
            sticky_bottom,
        } = sticky_bounds;
        let sticky_container_height = sticky_bottom - sticky_top;

        {
            let state = self.state.borrow();
            if sticky_container_height == state.render_state.sticky_height
                && sticky_top == state.render_state.sticky_top
                && sticky_bottom == state.render_state.sticky_bottom
            {
                return;
            }
        }

        self.apply_sticky_positioning(sticky_bounds);
    }

    /// `handleScroll`: the root's `scroll` event.
    pub fn handle_scroll(&self) {
        self.suspend_scroll_interactions();
        self.state.borrow_mut().scroll_dirty = true;
        self.notify_scroll();
        self.render(false);
    }

    /// `clearPendingScroll`: abort any in-flight programmatic scroll when the
    /// user takes over (`wheel`, `touchstart`, `pointerdown`, `keydown` on the
    /// root).
    pub fn clear_pending_scroll(&self) {
        let mut state = self.state.borrow_mut();
        state.pending_scroll_target = None;
        state.pending_layout_anchor = None;
        state.scroll_animation = None;
    }

    /// The anchored fix shared by `handleResize`'s sticky container and host
    /// branches: restore the anchor's viewport offset after a layout shift
    /// and settle the pending target.
    fn fix_scroll_for_resize(
        &self,
        current_scroll_top: f64,
        anchor: Option<super::state::ScrollAnchor>,
    ) {
        let anchored_scroll_top = anchor
            .as_ref()
            .and_then(|anchor| self.resolve_anchored_scroll_top(anchor));
        if let Some(anchored_scroll_top) = anchored_scroll_top {
            let resize_anchor_delta = anchored_scroll_top - current_scroll_top;
            let window_specs = self.state.borrow().window_specs;
            self.apply_scroll_fix(anchored_scroll_top, current_scroll_top, window_specs);
            if let Some(animation) = &mut self.state.borrow_mut().scroll_animation {
                // if we had to apply a scroll fix then we should make sure to
                // match the scroll fix delta to the scrollAnimation position to
                // ensure the animation continues smoothly as if the scroll fix
                // never happened
                animation.position += resize_anchor_delta;
            }
        }
        let pending = self.state.borrow().pending_scroll_target.clone();
        if let Some(pending) = pending
            && self.is_pending_target_settled(&pending)
        {
            let mut state = self.state.borrow_mut();
            state.pending_scroll_target = None;
            state.scroll_animation = None;
        }
    }

    /// `handleResize(entries)`
    pub fn handle_resize(&self, entries: &[ResizeObserverEntry<NodeKey>]) {
        let mut should_render = false;
        for entry in entries {
            let (header_element, footer_element, sticky_height, header_height, footer_height) = {
                let state = self.state.borrow();
                (
                    state.header.element,
                    state.footer.element,
                    state.render_state.sticky_height,
                    state.header.height,
                    state.footer.height,
                )
            };
            // If the sticky container resizes (could be from a render, which it
            // will probably ignore) or if an annotation or line wrap triggers a
            // resize
            if entry.target == self.sticky_container {
                let block_size = entry.border_box_block_size;
                if (block_size - sticky_height).abs() >= SUB_PIXEL_TOLERANCE {
                    // If content resizes above the viewport, we want to be sure
                    // that it doesn't cause things to jump within the viewport
                    let current_scroll_top = self.get_scroll_top();
                    let anchor = self.get_scroll_anchor(current_scroll_top, None);

                    self.reconcile_rendered_items(None);
                    self.update_sticky_positioning();

                    self.fix_scroll_for_resize(current_scroll_top, anchor);
                    should_render = true;
                }
            }
            // A header/footer host resized after mount (async content, fonts, a
            // portal filling in). Re-measure and, for a header — which lives
            // above the items — re-anchor so the content under the user's eyes
            // doesn't jump, mirroring the sticky container branch above. Items
            // are untouched by a host resize, so we skip
            // reconcileRenderedItems/updateStickyPositioning; the trailing
            // render() reconciles the range and render window.
            else if Some(entry.target) == header_element || Some(entry.target) == footer_element {
                let (kind, host_height) = if Some(entry.target) == header_element {
                    (HostKind::Header, header_height)
                } else {
                    (HostKind::Footer, footer_height)
                };
                let block_size = entry.border_box_block_size;
                if block_size != host_height {
                    // Capture the anchor with the OLD offset, apply the new
                    // height, then resolve with the NEW offset so the delta
                    // cancels the layout shift. A footer only changes the
                    // scroll range, so its anchor resolves to no change (or a
                    // clamp when it shrinks below the current scroll).
                    let current_scroll_top = self.get_scroll_top();
                    let anchor = self.get_scroll_anchor(current_scroll_top, None);
                    self.set_host_height(kind, block_size);
                    self.fix_scroll_for_resize(current_scroll_top, anchor);
                    should_render = true;
                }
            }
            // Root element resize (element-mode only)
            else {
                let mut state = self.state.borrow_mut();
                state.scroll_dirty = true;
                state.height_dirty = true;
                should_render = true;
            }
        }

        // If the DOM changed in an unexpected way, we should kick off a
        // synchronous render immediately because it will ensure no visual
        // jitter if we need to scroll fix
        if should_render {
            self.render(true);
        }
    }
}
