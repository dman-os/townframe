//! The listeners the renderer attaches to the nodes the host registered, and where their events go.
//!
//! Dioxus delivers an event as platform data converted to the typed payload (`ResizeData`,
//! `VisibleData`, `ScrollData`, `MountedData`). Every handler does the same three things in the
//! same order: record what the payload says in the host's cache, tell the port's manager that
//! asked to be told (so its synchronous reads already see the new value), and flush the frame
//! queue, which is the end of the dispatch (see `host.rs`).

use std::rc::Rc;

use dioxus::core::AttributeValue;
use dioxus::html::MountedData;
use dioxus::html::PlatformEventData;
use dioxus::html::ResizeData;
use dioxus::html::ScrollData;
use dioxus::html::VisibleData;
use dioxus::prelude::*;
use pierre_diffs::components::virtualizer::InstanceRef;
use pierre_diffs::components::virtualizer::IntersectionEntry;
use pierre_diffs::components::virtualizer::ResizeEntry;
use pierre_diffs::components::virtualizer::Virtualizer;
use pierre_diffs::document::NodeKey;
use pierre_diffs::managers::resize_manager::ResizeObserverEntry;
use pierre_diffs::managers::resize_manager::ResizeObserverHub;
use pierre_diffs::managers::scroll_sync_manager::ScrollSide;
use pierre_diffs::managers::scroll_sync_manager::ScrollSyncManager;

use crate::geometry::ScrollSample;
use crate::host::DioxusHost;

/// Where the events of one view go: its host cache and the managers that observe through it.
pub(crate) struct Dispatch {
    pub(crate) host: Rc<DioxusHost>,
    pub(crate) hub: Rc<ResizeObserverHub<DioxusHost>>,
    pub(crate) virtualizer: Rc<Virtualizer<DioxusHost>>,
    pub(crate) scroll_sync: Rc<ScrollSyncManager<DioxusHost>>,
    pub(crate) instance: InstanceRef,
}

impl Dispatch {
    pub(crate) fn mounted(&self, node: NodeKey, mounted: MountedData) {
        self.host.record_mounted(node, Rc::new(mounted));
        self.host.flush_frames();
    }

    /// `ResizeObserver`'s callback: `border_height` is `borderBoxSize.blockSize` and
    /// `content_width` is `contentBoxSize.inlineSize`.
    pub(crate) fn resized(&self, node: NodeKey, border_height: f64, content_width: f64) {
        let previous = self.host.record_size(node, border_height);
        self.hub
            .handle_shared_resize_entries(&[ResizeObserverEntry {
                target: node,
                border_box_block_size: border_height,
                content_box_inline_size: content_width,
            }]);
        self.virtualizer.handle_container_resize(&[ResizeEntry {
            target: node,
            block_size: border_height,
        }]);
        // Upstream sees a row's height change at the next pass its own render triggers. Here
        // the layout that changed it arrives after that pass, so a row that grew or first
        // appeared asks for the pass that reads it (a no-op unless it has something to reconcile).
        if previous != Some(border_height) && node != self.host.container() {
            self.virtualizer.request_height_reconcile(&self.instance);
        }
        self.host.flush_frames();
    }

    /// `IntersectionObserver`'s callback.
    pub(crate) fn visible(&self, node: NodeKey, is_intersecting: bool) {
        self.virtualizer
            .handle_intersection_change(&[IntersectionEntry {
                target: node,
                is_intersecting,
            }]);
        self.host.flush_frames();
    }

    pub(crate) fn scrolled(&self, node: NodeKey, sample: ScrollSample) {
        self.host.record_scroll(node, sample);
        match self.host.scroll_side(node) {
            Some(None) => self.virtualizer.handle_element_scroll(),
            Some(Some(ScrollSide::Deletions)) => self.scroll_sync.handle_deletions_scroll(),
            Some(Some(ScrollSide::Additions)) => self.scroll_sync.handle_additions_scroll(),
            // The listener was removed while the event was in flight.
            None => {}
        }
        self.host.flush_frames();
    }

    /// The listener attributes of `node`, for the renderer to add to the element.
    pub(crate) fn listeners(self: &Rc<Self>, node: NodeKey) -> Vec<Attribute> {
        let mut attributes = Vec::new();
        if self.host.wants_resize(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onresize", move |data: ResizeData| {
                // Horizontal writing mode: the block size is the height.
                let border = data
                    .get_border_box_size()
                    .expect("the platform reports a resized element's border box");
                let content = data
                    .get_content_box_size()
                    .expect("the platform reports a resized element's content box");
                dispatch.resized(node, border.height, content.width);
            }));
        }
        if self.host.wants_visible(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onvisible", move |data: VisibleData| {
                let intersecting = data
                    .is_intersecting()
                    .expect("the platform reports whether a watched element intersects");
                dispatch.visible(node, intersecting);
            }));
        }
        if self.host.wants_scroll(node) {
            let dispatch = Rc::clone(self);
            attributes.push(listener("onscroll", move |data: ScrollData| {
                dispatch.scrolled(
                    node,
                    ScrollSample {
                        top: data.scroll_top(),
                        left: data.scroll_left(),
                        scroll_height: f64::from(data.scroll_height()),
                        client_height: f64::from(data.client_height()),
                    },
                );
            }));
            // Scroll commands go through `MountedData`, which exists only from `onmounted`: a
            // node that is scrolled is a node that gets it.
            let dispatch = Rc::clone(self);
            attributes.push(listener("onmounted", move |data: MountedData| {
                dispatch.mounted(node, data);
            }));
        }
        attributes
    }
}

/// An event attribute over the platform's data; what `rsx!` builds for `on<event>: handler`.
fn listener<Data>(name: &'static str, mut handler: impl FnMut(Data) + 'static) -> Attribute
where
    Data: for<'a> From<&'a PlatformEventData> + 'static,
{
    Attribute::new(
        name,
        AttributeValue::listener(move |event: Event<PlatformEventData>| {
            handler(Data::from(&*event.data()));
        }),
        None,
        false,
    )
}
