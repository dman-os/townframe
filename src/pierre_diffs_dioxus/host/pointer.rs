//! The pointer half of the host: which nodes render pointer handlers, the drag's document-level
//! listener flag, and `element_from_point` over the cached row geometry. It is a child of
//! `host.rs` because it reads the host's private caches.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dioxus::html::MountedError;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;
use pierre_diffs::managers::interaction_manager::HitTest;
use pierre_diffs::managers::interaction_manager::InteractionHost;

use super::DioxusHost;
use crate::geometry::HitFrame;
use crate::geometry::is_margin_anchor;

/// The pointer handlers a node renders with.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PointerListeners {
    /// `click`: a hunk's expand controls and the `pre`.
    pub(crate) click: bool,
    /// `pointerdown` and `pointermove`: the nodes the manager resolves rows from.
    pub(crate) press_and_move: bool,
    /// `pointerleave`: the `pre` only, because the event does not bubble.
    pub(crate) leave: bool,
    /// `pointermove`, `pointerup` and `pointercancel` for a drag, on the view's top node.
    pub(crate) document: bool,
}

/// The nodes a hunk's expansion is clicked on.
const EXPAND_ATTRIBUTES: [&str; 2] = ["data-expand-button", "data-unmodified-lines"];

/// The attributes that make a node something the manager resolves a row target from: the ones
/// `resolvePointerTarget`, the selection rows and the gutter utility path read.
const ROW_TARGET_ATTRIBUTES: [&str; 6] = [
    "data-line",
    "data-column-number",
    "data-line-annotation",
    "data-gutter-buffer",
    "data-utility-button",
    "data-gutter-utility-slot",
];

#[derive(Default)]
pub(crate) struct PointerState {
    /// The `pre` the `InteractionManager` listens on, while it listens.
    pre: Cell<Option<NodeKey>>,
    /// Whether the view's options give the pointer anything to do on a row (hover, selection,
    /// the gutter utility). Hunk expansion alone leaves it false: the manager listens for the
    /// expand clicks (upstream attaches its listeners whenever separators expand), but a view
    /// that only expands has no use for a move or press on every line.
    rows: Cell<bool>,
    /// Whether a drag asked for the document-level pointer listeners.
    document_listeners: Cell<bool>,
    /// What `element_from_point` measures from; present only while a drag has one.
    frame: RefCell<Option<HitFrame>>,
}

impl PointerState {
    /// Whether a drag can start on a row: the manager listens and the options give the pointer
    /// something to do on rows.
    pub(crate) fn acts_on_rows(&self) -> bool {
        self.pre.get().is_some() && self.rows.get()
    }

    /// Sets whether pointers act on rows; the view calls it with its props.
    pub(crate) fn set_rows(&self, rows: bool) {
        self.rows.set(rows);
    }

    /// Whether a drag asked for the document-level listeners, which the handlers read.
    pub(crate) fn document_listeners_active(&self) -> bool {
        self.document_listeners.get()
    }

    /// Which pointer handlers `node` renders with; none unless the manager listens.
    pub(crate) fn listeners(
        &self,
        document: &Document,
        top: NodeKey,
        node: NodeKey,
    ) -> PointerListeners {
        let Some(pre) = self.pre.get() else {
            return PointerListeners::default();
        };
        let rows = self.rows.get();
        let is_pre = node == pre;
        let expands = EXPAND_ATTRIBUTES
            .iter()
            .any(|name| document.has_attribute(node, name));
        PointerListeners {
            click: is_pre || expands,
            press_and_move: rows
                && (is_pre
                    || expands
                    || ROW_TARGET_ATTRIBUTES
                        .iter()
                        .any(|name| document.has_attribute(node, name))
                    // Content slotted into the gutter utility or an annotation row is a light
                    // child of the host that renders inside the shadow tree's slot.
                    || document.get_attribute(node, "slot").is_some_and(|slot| {
                        slot == "gutter-utility-slot" || slot.starts_with("annotation-")
                    })),
            leave: rows && is_pre,
            document: rows && node == top,
        }
    }

    /// The nodes the hit-test frame is measured from need `MountedData`: the `pre`, the content
    /// columns and the rows whose margin is measured (`is_margin_anchor`), while the pointer acts
    /// on rows.
    pub(crate) fn wants_mounted(&self, document: &Document, node: NodeKey) -> bool {
        self.rows.get()
            && self.pre.get().is_some_and(|pre| {
                node == pre
                    || document.has_attribute(node, "data-content")
                    || is_margin_anchor(document, node)
            })
    }
}

/// Measures the hit-test frame (see `HitFrame`). The client rects are async commands, so the
/// frame lands a task later; until then `element_from_point` answers `Unavailable` and the
/// manager follows the event path.
fn measure_frame(host: &Rc<DioxusHost>) {
    let Some(pre) = host.pointer.pre.get() else {
        return;
    };
    let columns = host.document.query_by_attribute(pre, "data-content", None);
    let Some(content) = columns.first().copied() else {
        return;
    };
    // The nodes measured: the `pre`, the first column, and every margin anchor of every column.
    let measured: Vec<NodeKey> = [pre, content]
        .into_iter()
        .chain(columns.iter().flat_map(|column| {
            host.document
                .child_nodes(column)
                .into_iter()
                .filter(|row| is_margin_anchor(&host.document, *row))
        }))
        .collect();
    let mounted = host.mounted.borrow();
    let Some(handles) = measured
        .iter()
        .map(|node| mounted.get(node).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        // Not mounted yet: the drag follows the event path.
        return;
    };
    drop(mounted);
    let host = Rc::clone(host);
    dioxus::prelude::spawn(async move {
        let mut tops = HashMap::new();
        let mut pre_rect = None;
        let mut content_rect = None;
        for (node, handle) in measured.iter().zip(&handles) {
            let rect = match handle.get_client_rect().await {
                Ok(rect) => rect,
                // A renderer without client rects has no hit-test, which the manager is built to
                // handle.
                Err(MountedError::NotSupported) => return,
                Err(error) => panic!("the platform measures a mounted element: {error}"),
            };
            if *node == pre {
                pre_rect = Some(rect);
            } else if *node == content {
                content_rect = Some(rect);
            }
            // Only the margin anchors' tops are read back (`Geometry::margins`).
            tops.insert(*node, rect.origin.y);
        }
        let (pre_rect, content_rect) = (
            pre_rect.expect("the pre was measured"),
            content_rect.expect("the column was measured"),
        );
        // The drag may have ended while the rects were in flight.
        if !host.pointer.document_listeners.get() || !host.document.exists(content) {
            return;
        }
        let geometry = host.geometry.borrow();
        let scroll_top = host.scroll_top();
        let margins = geometry.margins(&host.document, pre, content_rect.origin.y, &tops);
        *host.pointer.frame.borrow_mut() = Some(HitFrame {
            pre_left: pre_rect.origin.x,
            pre_width: pre_rect.size.width,
            content,
            content_client_top: content_rect.origin.y,
            content_offset: geometry.top_in_content(&host.document, host.container, content),
            scroll_top,
            margins,
        });
    });
}

impl InteractionHost for DioxusHost {
    fn document(&self) -> &Document {
        &self.document
    }

    /// Answered from the cached row geometry, not the engine (see `Geometry::element_at`).
    /// `Unavailable` is the port's "callers fall back to the event path": the answer while no
    /// frame is measured (no drag, the rects still in flight, a host that is not
    /// measuring rows, a renderer without client rects). A frame whose column the document dropped is
    /// stale in the same way.
    fn element_from_point(&self, x: f64, y: f64) -> HitTest<NodeKey> {
        let frame = self.pointer.frame.borrow();
        let (Some(frame), Some(pre)) = (frame.as_ref(), self.pointer.pre.get()) else {
            return HitTest::Unavailable;
        };
        if !self.document.exists(frame.content) {
            return HitTest::Unavailable;
        }
        let geometry = self.geometry.borrow();
        let scroll_top = self.scroll_top();
        match geometry.element_at(
            &self.document,
            self.container,
            pre,
            frame,
            scroll_top,
            (x, y),
        ) {
            Some(row) => HitTest::Element(row),
            None => HitTest::NoElement,
        }
    }

    /// The renderer attaches the handlers to the nodes that name `pre` as their listening root
    /// (`PointerState::listeners`): registering is recording the `pre`.
    fn add_pre_listeners(&self, pre: NodeKey) {
        self.pointer.pre.set(Some(pre));
    }

    fn remove_pre_listeners(&self, _pre: NodeKey) {
        self.pointer.pre.set(None);
    }

    /// The flag the handlers read (see the module docs of `host.rs`). The hit-test frame is
    /// measured now, because the drag that asked for these listeners is the only thing that
    /// hit-tests.
    fn add_document_listeners(&self) {
        self.pointer.document_listeners.set(true);
        *self.pointer.frame.borrow_mut() = None;
        measure_frame(
            &self
                .weak_self
                .upgrade()
                .expect("the host is alive while it is called"),
        );
    }

    fn remove_document_listeners(&self) {
        self.pointer.document_listeners.set(false);
        *self.pointer.frame.borrow_mut() = None;
    }
}
