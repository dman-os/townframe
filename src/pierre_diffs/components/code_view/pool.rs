// Ported from @pierre/diffs 1.6.4 (tag diffs-v1.6.4, commit
// 75e00805b44e193c0fb235c1bd9d7fadfa06acf6): packages/diffs/src/components/CodeView.ts
// (C4: the element pool. `getElementPoolLimit`, `acquireElement`,
// `releaseRenderedItem`, `renderedItemOwnsFocus`, `fixContainerFocus`,
// `cleanElement`, `queueElementForPool`, `clearElementPool`,
// `invalidateElementPool`, the generation tracker, `isPooledShadowChild` and
// `syncRenderedItemOrder`.)
// Copyright 2025 Pierre Computer Company, Apache-2.0; see THIRD_PARTY_NOTICES.md.
// Changes:
// * `pendingElementPool`, `promotePendingPooledElements`, `isElementClean`,
//   `getElementPoolSize`'s second term and `isComponentManaged`'s doubling of
//   the pool limit exist only for React's portal cleanup; the Dioxus view is
//   not component-managed (Q5, dropped.psv), so a released shell is always
//   clean and goes straight to the pool.
// * `elementPoolTracker` (a `WeakMap`) is a `HashMap<NodeKey, u64>`, pruned
//   when a shell is discarded. A shell the pool declines (over the limit, a
//   stale generation), the pool cleared by `invalidateElementPool` or
//   `clearElementPool`, and the light DOM `cleanElement` empties are
//   `discard`ed: the `Document` frees by owner, not by garbage collection.
// * `cleanElement` keeps the shared shell assets (`isPooledShadowChild`: the
//   sprite and the core, theme and unsafe CSS `style` elements). They are the
//   container's, not the instance's: `FileDiff::clean_up` leaves them in the
//   shadow root and the next instance adopts them (Q7, lifecycle.rs).
// * `isPooledShadowChild`'s `instanceof SVGElement` is the tag test.

use std::rc::Rc;

use super::CodeView;
use super::CodeViewHost;
use super::state::ItemRef;
use crate::components::file_diff::RenderedContent;
use crate::constants::CORE_CSS_ATTRIBUTE;
use crate::constants::DIFFS_TAG_NAME;
use crate::constants::THEME_CSS_ATTRIBUTE;
use crate::constants::UNSAFE_CSS_ATTRIBUTE;
use crate::document::Document;
use crate::document::NodeKey;

impl<Host, Rendered, LAnnotation> CodeView<Host, Rendered, LAnnotation>
where
    Host: CodeViewHost,
    Rendered: RenderedContent + 'static,
    LAnnotation: Clone + PartialEq + 'static,
{
    /// `getElementPoolLimit()`: roughly the number of collapsed files that fill
    /// the viewport and its overscroll, at least eight.
    pub(crate) fn get_element_pool_limit(&self) -> usize {
        let viewport_size = self.get_height() + self.config.overscroll_size * 2.0;
        let diff_header_height = self.state.borrow().item_metrics_cache.diff_header_height;
        (8.0_f64).max((viewport_size / diff_header_height.max(10.0)).ceil() + 1.0) as usize
    }

    /// `acquireElement()`
    pub(crate) fn acquire_element(&self) -> NodeKey {
        let document = self.document();
        let element = loop {
            let pooled = self.state.borrow_mut().element_pool.pop();
            match pooled {
                Some(element) if !self.is_element_pool_generation_current(element) => {
                    self.discard_shell(element);
                }
                Some(element) => break element,
                None => break document.create_element(DIFFS_TAG_NAME),
            }
        };
        self.mark_element_pool_generation_current(element);
        element
    }

    /// `releaseRenderedItem(item)`
    pub(crate) fn release_rendered_item(&self, item: &ItemRef<Host, Rendered, LAnnotation>) {
        let element = item.borrow().element;
        if let Some(element) = element
            && self.rendered_item_owns_focus(element)
        {
            self.state.borrow_mut().should_fix_container_focus = true;
        }

        let instance = Rc::clone(item.borrow().instance());
        instance.clean_up(true);
        item.borrow_mut().element = None;
        let Some(element) = element else {
            return;
        };

        self.document().remove(element);
        self.clean_element(element);
        self.queue_element_for_pool(element);
    }

    /// `renderedItemOwnsFocus(element)`
    fn rendered_item_owns_focus(&self, element: NodeKey) -> bool {
        let owns_light_focus = self
            .host
            .active_element()
            .is_some_and(|active| self.document().is_attached(active, element));
        owns_light_focus || self.host.shadow_active_element(&element).is_some()
    }

    /// `fixContainerFocus()`
    pub(crate) fn fix_container_focus(&self) {
        let (should_fix, root) = {
            let mut state = self.state.borrow_mut();
            (
                std::mem::take(&mut state.should_fix_container_focus),
                state.root,
            )
        };
        if should_fix && let Some(root) = root {
            self.host.focus_root(&root);
        }
    }

    /// `cleanElement(element)`: strip item-specific DOM while keeping the
    /// expensive shared shell assets that are valid for every item in this
    /// CodeView until shared options change.
    fn clean_element(&self, element: NodeKey) {
        let document = self.document();
        if let Some(shadow_root) = document.shadow_root(element) {
            for child in document.children(shadow_root) {
                if !is_pooled_shadow_child(document, child) {
                    document.discard(child);
                }
            }
        }

        // `element.replaceChildren()`
        for child in document.child_nodes(element) {
            document.discard(child);
        }
    }

    /// `queueElementForPool(element)`
    fn queue_element_for_pool(&self, element: NodeKey) {
        let pool_limit = self.get_element_pool_limit();
        if !self.is_element_pool_generation_current(element)
            || self.state.borrow().element_pool.len() >= pool_limit
        {
            self.discard_shell(element);
            return;
        }
        self.state.borrow_mut().element_pool.push(element);
    }

    /// A shell the pool will not keep.
    fn discard_shell(&self, element: NodeKey) {
        self.state
            .borrow_mut()
            .element_pool_tracker
            .remove(&element);
        self.document().discard(element);
    }

    /// `clearElementPool()`
    pub(crate) fn clear_element_pool(&self) {
        let pooled = std::mem::take(&mut self.state.borrow_mut().element_pool);
        for element in pooled {
            self.discard_shell(element);
        }
    }

    /// `invalidateElementPool()`: pooled shells made under the old options are
    /// stale.
    pub(crate) fn invalidate_element_pool(&self) {
        self.state.borrow_mut().element_pool_version += 1;
        self.clear_element_pool();
    }

    fn mark_element_pool_generation_current(&self, element: NodeKey) {
        let mut state = self.state.borrow_mut();
        let version = state.element_pool_version;
        state.element_pool_tracker.insert(element, version);
    }

    fn is_element_pool_generation_current(&self, element: NodeKey) -> bool {
        let state = self.state.borrow();
        state.element_pool_tracker.get(&element) == Some(&state.element_pool_version)
    }

    /// `syncRenderedItemOrder(container, element, prevElement)`: keep the
    /// rendered DOM order aligned with the current record order even when we
    /// reuse existing elements. Reused items may already be mounted elsewhere in
    /// the sticky container, so this moves them into the correct sibling
    /// position before rendering updates.
    pub(crate) fn sync_rendered_item_order(&self, element: NodeKey, prev_element: Option<NodeKey>) {
        let document = self.document();
        match prev_element {
            None => {
                if document.first_child(self.sticky_container) != Some(element) {
                    document.prepend(self.sticky_container, element);
                }
            }
            Some(prev_element) => {
                if document.next_sibling(prev_element) != Some(element) {
                    document.after(prev_element, element);
                }
            }
        }
    }
}

/// `isPooledShadowChild(child)`
fn is_pooled_shadow_child(document: &Document, child: NodeKey) -> bool {
    let tag = document.tag_name(child);
    if tag.eq_ignore_ascii_case("svg") {
        return true;
    }
    tag.eq_ignore_ascii_case("style")
        && [
            CORE_CSS_ATTRIBUTE,
            THEME_CSS_ATTRIBUTE,
            UNSAFE_CSS_ATTRIBUTE,
        ]
        .iter()
        .any(|attribute| document.has_attribute(child, attribute))
}
