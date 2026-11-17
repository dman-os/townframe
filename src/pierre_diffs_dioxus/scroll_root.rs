//! `ScrollRoot`: the scroller an embedder owns, for a `CodeViewView` that lays out in the page's
//! own scroll container instead of a scroller of its own.
//!
//! A child component cannot attach listeners to an ancestor, so the embedder forwards the
//! ancestor's `onmounted`, `onscroll` and `onresize` into this handle and passes the handle to
//! the viewer (`ScrollMode::Embedder`). The viewer reads the handle as it reads its own root:
//!
//! * The scroll position and the viewport height are the scroller's. The viewer sits somewhere
//!   inside the scroller's content, so its own scroll position is the scroller's less the
//!   distance from the top of the content to the top of the viewer (`content offset`). That
//!   distance is measured with client rects (the viewer's, the scroller's, and the scroller's
//!   scroll offset) when the viewer mounts and whenever the scroller or the viewer is resized:
//!   no event reports it. Content that changes size above the viewer without resizing either
//!   leaves it stale until the next such event. A renderer without client rects leaves it at 0,
//!   which is right when the viewer is the first thing in the scroller.
//! * Scroll writes (a reveal, the viewer's scroll fixes) go to the scroller through its
//!   `MountedData`, shifted by the same offset.
//!
//! No element between the scroller and the viewer may be a scroll container (`overflow` other
//! than `visible` or `clip`): the viewer's sticky slab would stick to that one instead. Set `overflow-anchor: none` on the scroller too: the viewer
//! sets it on its own root, which excludes its subtree from anchor selection, but a scroller
//! that anchors to a sibling above it would shift the position the viewer's scroll fix is about
//! to correct.

use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;

use dioxus::html::MountedData;
use dioxus::html::MountedError;
use dioxus::html::ResizeData;
use dioxus::html::ScrollData;
use dioxus::prelude::*;

use crate::geometry::ScrollSample;

/// What the handle tells the viewer linked to it; the viewer reads the new values from the handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootNotice {
    /// The scroller mounted: scroll writes can go to it.
    Mounted,
    Scrolled,
    Resized,
    /// The viewer's place in the scroller's content moved.
    Moved,
}

pub(crate) type Link = Rc<dyn Fn(RootNotice)>;

#[derive(Default)]
struct Shared {
    scroller: RefCell<Option<Rc<MountedData>>>,
    viewer: RefCell<Option<Rc<MountedData>>>,
    sample: Cell<Option<ScrollSample>>,
    /// The scroller's border box: block size, inline size.
    size: Cell<Option<(f64, f64)>>,
    offset: Cell<f64>,
    /// The viewer reading this handle, if one is mounted.
    link: RefCell<Option<Link>>,
}

/// The embedder's scroller, as a handle the viewer reads. Clone it freely: clones are the same
/// handle. Make one with [`use_scroll_root`], forward the scroller's `onmounted`, `onscroll` and
/// `onresize` into it and pass it as `ScrollMode::Embedder`.
///
/// The scroller can be the document itself: mark the element that wraps the page with
/// `"data-dioxus-scroll-proxy": "document"` (the Dioxus fork's interpreter then reports the
/// document's scroll and the viewport's size as that element's `onscroll` and `onresize`, and
/// drives the document through its `MountedData`) and give it no scroll of its own. Put
/// `overflow-anchor: none` on `html`. A nested scroller gets no momentum scrolling in some
/// engines (WebKitGTK), the document does.
///
/// ```ignore
/// let root = use_scroll_root();
/// rsx! {
///     div {
///         style: "height:100vh;overflow-y:auto;overflow-anchor:none",
///         onmounted: { let root = root.clone(); move |event| root.onmounted(event) },
///         onscroll: { let root = root.clone(); move |event| root.onscroll(event) },
///         onresize: { let root = root.clone(); move |event| root.onresize(event) },
///         CodeViewView::<()> { scroll: ScrollMode::Embedder(root.clone()), /* ... */ }
///     }
/// }
/// ```
#[derive(Clone, Default)]
pub struct ScrollRoot(Rc<Shared>);

impl PartialEq for ScrollRoot {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

/// A handle that lives as long as the calling component.
pub fn use_scroll_root() -> ScrollRoot {
    use_hook(ScrollRoot::default)
}

impl ScrollRoot {
    /// Forward the scroller's `onmounted`.
    pub fn onmounted(&self, event: Event<MountedData>) {
        self.mounted(event.data());
    }

    /// Forward the scroller's `onscroll`.
    pub fn onscroll(&self, event: Event<ScrollData>) {
        let data = event.data();
        self.scrolled(ScrollSample {
            top: data.scroll_top(),
            left: data.scroll_left(),
            scroll_height: f64::from(data.scroll_height()),
            client_height: f64::from(data.client_height()),
        });
    }

    /// Forward the scroller's `onresize`.
    pub fn onresize(&self, event: Event<ResizeData>) {
        let data = event.data();
        let border = data
            .get_border_box_size()
            .expect("the platform reports a resized element's border box");
        let content = data
            .get_content_box_size()
            .expect("the platform reports a resized element's content box");
        self.resized(border.height, content.width);
        self.measure();
    }

    pub(crate) fn mounted(&self, scroller: Rc<MountedData>) {
        *self.0.scroller.borrow_mut() = Some(scroller);
        self.notify(RootNotice::Mounted);
        self.measure();
    }

    pub(crate) fn scrolled(&self, sample: ScrollSample) {
        self.0.sample.set(Some(sample));
        self.notify(RootNotice::Scrolled);
    }

    pub(crate) fn resized(&self, height: f64, width: f64) {
        self.0.size.set(Some((height, width)));
        self.notify(RootNotice::Resized);
    }

    /// The viewer's own root mounted: its place in the scroller's content can be measured.
    pub(crate) fn viewer_mounted(&self, viewer: Rc<MountedData>) {
        *self.0.viewer.borrow_mut() = Some(viewer);
        // A scroller that reported before this viewer linked reports nothing more until it
        // changes (the embedder replaced the viewer, not the scroller), and the viewer's first
        // pass waits for those reports: tell it what the handle knows. A scroller that has not
        // reported yet is reported to the viewer as it does.
        if self.0.scroller.borrow().is_some() {
            self.notify(RootNotice::Mounted);
        }
        if self.size().is_some() {
            self.notify(RootNotice::Resized);
        }
        if self.sample().is_some() {
            self.notify(RootNotice::Scrolled);
        }
        self.measure();
    }

    pub(crate) fn scroller(&self) -> Option<Rc<MountedData>> {
        self.0.scroller.borrow().clone()
    }

    pub(crate) fn sample(&self) -> Option<ScrollSample> {
        self.0.sample.get()
    }

    pub(crate) fn size(&self) -> Option<(f64, f64)> {
        self.0.size.get()
    }

    /// The distance from the top of the scroller's content to the top of the viewer.
    pub(crate) fn offset(&self) -> f64 {
        self.0.offset.get()
    }

    /// The scroller's sample as the viewer's own root would report it: its scroll position is
    /// the scroller's less the viewer's offset (negative while the viewer's top is below the
    /// fold, which the viewer clamps).
    pub(crate) fn viewer_sample(&self, sample: ScrollSample) -> ScrollSample {
        ScrollSample {
            top: sample.top - self.offset(),
            ..sample
        }
    }

    /// A viewer starts reading this handle. The handle may have outlived another viewer (the
    /// embedder replaced it while the scroller went on), so what it knew of that viewer's root is
    /// dropped: the new viewer's root measures itself when it mounts.
    pub(crate) fn link(&self, link: Link) {
        *self.0.viewer.borrow_mut() = None;
        self.0.offset.set(0.0);
        *self.0.link.borrow_mut() = Some(link);
    }

    /// A viewer stops reading this handle. A replaced component is dropped after its replacement
    /// mounted, so the handle may already belong to the new viewer: only the viewer's own link
    /// is removed.
    pub(crate) fn unlink(&self, link: &Link) {
        let mut current = self.0.link.borrow_mut();
        if current
            .as_ref()
            .is_some_and(|current| std::ptr::addr_eq(Rc::as_ptr(current), Rc::as_ptr(link)))
        {
            *current = None;
        }
    }

    /// The `Rc` is cloned out first: the viewer's reaction may link or unlink.
    fn notify(&self, notice: RootNotice) {
        let link = self.0.link.borrow().clone();
        if let Some(link) = link {
            link(notice);
        }
    }

    /// Measures the viewer's offset in the scroller's content and tells the viewer if it moved.
    /// Called from an event handler or a render, where `spawn` has a runtime.
    pub(crate) fn measure(&self) {
        let (Some(scroller), Some(viewer)) = (self.scroller(), self.0.viewer.borrow().clone())
        else {
            return;
        };
        let root = self.clone();
        spawn(async move {
            let rects = async {
                let scroller_rect = scroller.get_client_rect().await?;
                let viewer_rect = viewer.get_client_rect().await?;
                let scrolled = scroller.get_scroll_offset().await?;
                Ok::<_, MountedError>(viewer_rect.origin.y - scroller_rect.origin.y + scrolled.y)
            };
            let offset = match rects.await {
                Ok(offset) => offset,
                // A renderer without client rects can only scroll a viewer at the top of the
                // content, which is the offset 0 it already has.
                Err(MountedError::NotSupported) => return,
                Err(error) => panic!("the platform measures a mounted element: {error}"),
            };
            if (offset - root.offset()).abs() >= 0.5 {
                root.0.offset.set(offset);
                root.notify(RootNotice::Moved);
            }
        });
    }
}
