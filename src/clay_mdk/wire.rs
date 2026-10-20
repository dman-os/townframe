//! Dioxus `WriteMutations` → typed wire batch.
//!
//! A guest framework adapter renders into this writer and ships the batch through
//! `surface.apply`. The mapping is one-to-one with Dioxus Core's `WriteMutations`, so the host
//! can replay the batch into any `WriteMutations` of its own (a recording test backend or the
//! Blitz-backed native-dom writer). No closure, component, or reactive state crosses here.
use crate::presentation::{AttributeValue, Mutation, NodeId};
use dioxus_core::{BorrowedAttributeValue, ElementId, WriteMutations};

/// Records one batch of edits. `take` drains it; allocations are kept for the next render.
#[derive(Default)]
pub struct WireMutations<'a> {
    batch: Vec<Mutation<'a>>,
}

impl<'a> WireMutations<'a> {
    pub fn mount_component(&mut self, component: &'a crate::presentation::ComponentRef) {
        self.batch.push(Mutation::MountComponent(component));
    }

    /// The edits written since the last `take`, in order.
    pub fn take(&mut self) -> Vec<Mutation<'a>> {
        std::mem::take(&mut self.batch)
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }
}

fn node_id(id: ElementId) -> NodeId {
    // Element ids are surface-local, so narrowing to the wire width is a capacity limit,
    // not a semantic translation.
    u32::try_from(id.raw()).expect("element id exceeds the wire width")
}

fn count(m: usize) -> u32 {
    u32::try_from(m).expect("stack count exceeds the wire width")
}

impl WriteMutations for WireMutations<'_> {
    fn push_id(&mut self, id: ElementId) {
        self.batch.push(Mutation::PushId(node_id(id)));
    }

    fn set_id(&mut self, id: ElementId) {
        self.batch.push(Mutation::SetId(node_id(id)));
    }

    fn child(&mut self, index: usize) {
        self.batch.push(Mutation::Child(count(index)));
    }

    fn pop(&mut self) {
        self.batch.push(Mutation::Pop);
    }

    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        self.batch.push(Mutation::CreateElement((
            tag.to_owned(),
            ns.map(str::to_owned),
        )));
    }

    fn create_text(&mut self, value: &str) {
        self.batch.push(Mutation::CreateText(value.to_owned()));
    }

    fn clone(&mut self) {
        self.batch.push(Mutation::CloneNode);
    }

    fn append_children(&mut self, m: usize) {
        self.batch.push(Mutation::AppendChildren(count(m)));
    }

    fn replace_with(&mut self, m: usize) {
        self.batch.push(Mutation::ReplaceWith(count(m)));
    }

    fn insert_after(&mut self, m: usize) {
        self.batch.push(Mutation::InsertAfter(count(m)));
    }

    fn insert_before(&mut self, m: usize) {
        self.batch.push(Mutation::InsertBefore(count(m)));
    }

    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        let value = match value {
            BorrowedAttributeValue::Text(text) => AttributeValue::Text(text.to_owned()),
            BorrowedAttributeValue::Float(float) => AttributeValue::Float(float),
            BorrowedAttributeValue::Int(int) => AttributeValue::Int(int),
            BorrowedAttributeValue::Bool(bool) => AttributeValue::Boolean(bool),
            BorrowedAttributeValue::None => AttributeValue::None,
            // A process-local Rust object cannot cross an address space. Reaching this is an
            // adapter bug: a guest must not hand the host a native attribute value.
            BorrowedAttributeValue::Any(_) => {
                panic!("`Any` attribute `{name}` cannot cross the presentation boundary")
            }
        };
        self.batch.push(Mutation::SetAttribute((
            name.to_owned(),
            ns.map(str::to_owned),
            value,
        )));
    }

    fn set_text(&mut self, value: &str) {
        self.batch.push(Mutation::SetText(value.to_owned()));
    }

    fn add_event_listener(&mut self, name: &str) {
        self.batch.push(Mutation::AddEventListener(name.to_owned()));
    }

    fn remove_event_listener(&mut self, name: &str) {
        self.batch
            .push(Mutation::RemoveEventListener(name.to_owned()));
    }

    fn remove(&mut self) {
        self.batch.push(Mutation::Remove);
    }
}
