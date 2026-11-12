//! Content from the embedder at a point of the diff (annotations; later header slots and the
//! gutter utility).
//!
//! `FileDiff` places what its render callbacks return by asking the callback's `Rendered` value
//! for a node (`RenderedContent::to_node`). The document must never hold Dioxus elements (the
//! ported logic sees ordinary nodes), so `SlotContent::to_node` creates a placeholder element
//! and records, in the view's `SlotTable`, the closure that makes the Dioxus `Element` for it.
//! The renderer swaps the placeholder for that element when it flattens the document. The table
//! stores a closure, not an element, so the content is made again on every Dioxus render and
//! reads the caller's signals there, instead of freezing what the callback saw when `FileDiff`
//! called it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use dioxus::prelude::*;
use pierre_diffs::components::file_diff::RenderedContent;
use pierre_diffs::document::Document;
use pierre_diffs::document::NodeKey;

type MakeElement = Rc<dyn Fn() -> Element>;

/// Placeholder node to content, shared by the view and every `SlotContent` it hands out.
#[derive(Default)]
pub(crate) struct SlotTable(RefCell<HashMap<NodeKey, MakeElement>>);

impl SlotTable {
    pub(crate) fn content(&self, placeholder: NodeKey) -> Option<Element> {
        let make = self.0.borrow().get(&placeholder).cloned()?;
        Some(make())
    }

    /// Forgets placeholders `FileDiff` discarded; a discarded key fails `exists`, so the table
    /// is bounded by the live placeholders.
    pub(crate) fn prune(&self, document: &Document) {
        self.0.borrow_mut().retain(|node, _| document.exists(*node));
    }
}

/// What the view's render callbacks return to `FileDiff`: a handle on content to be placed.
#[derive(Clone)]
pub struct SlotContent {
    make: MakeElement,
    table: Rc<SlotTable>,
}

impl SlotContent {
    pub(crate) fn new(table: &Rc<SlotTable>, make: impl Fn() -> Element + 'static) -> Self {
        Self {
            make: Rc::new(make),
            table: Rc::clone(table),
        }
    }
}

impl RenderedContent for SlotContent {
    fn to_node(&self, document: &Document) -> NodeKey {
        let placeholder = document.create_element("div");
        document.set_attribute(placeholder, "data-dioxus-slot-content", "");
        self.table
            .0
            .borrow_mut()
            .insert(placeholder, Rc::clone(&self.make));
        placeholder
    }
}
