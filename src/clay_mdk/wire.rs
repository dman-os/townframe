//! Dioxus `WriteMutations` → typed wire batch.
//!
//! A guest framework adapter renders into this writer and ships the batch through
//! `surface.apply`. The mapping is one-to-one with Dioxus Core's `WriteMutations`, so the host
//! can replay the batch into any `WriteMutations` of its own (a recording test backend or the
//! Blitz-backed native-dom writer). No closure, component, or reactive state crosses here.
use crate::presentation::AttributeValue;
use crate::presentation::Mutation;
use crate::presentation::NodeId;
use dioxus_core::BorrowedAttributeValue;
use dioxus_core::ElementId;
use dioxus_core::WriteMutations;

/// The name of the event the host answers an element read with. It must equal
/// `clay_surface::ELEMENT_REPLY`, which this guest-side crate cannot depend on.
pub const ELEMENT_REPLY: &str = "element-reply";

/// The attribute a slot element carries (an integer slot id). Dioxus has no way to write a
/// `mount-component` mutation, so a slot is an element whose creation ends with this attribute
/// and [`WireMutations`] turns the attribute write into the mutation (see
/// [`WireMutations::take_resolving`]).
pub const SLOT_ATTRIBUTE: &str = "data-clay-slot";

/// One recorded edit: a finished wire mutation, or a slot whose component reference is only
/// looked up when the batch is taken.
enum Edit<'a> {
    Wire(Mutation<'a>),
    Slot(u64),
}

/// Records one batch of edits. `take` drains it; allocations are kept for the next render.
#[derive(Default)]
pub struct WireMutations<'a> {
    batch: Vec<Edit<'a>>,
}

impl<'a> WireMutations<'a> {
    pub fn mount_component(&mut self, component: &'a crate::presentation::ComponentRef) {
        self.batch
            .push(Edit::Wire(Mutation::MountComponent(component)));
    }

    /// The edits written since the last `take`, in order. Panics when a slot was written: a
    /// slot's component is only known to [`Self::take_resolving`].
    pub fn take(&mut self) -> Vec<Mutation<'a>> {
        self.batch
            .drain(..)
            .map(|edit| match edit {
                Edit::Wire(mutation) => mutation,
                Edit::Slot(id) => panic!("slot {id} written to a batch with no slot registry"),
            })
            .collect()
    }

    /// The edits written since the last `take`, in order, each slot expanded to a mounted
    /// component appended to the slot element: the element is on top of the stack when its
    /// attribute is written, so `mount-component` pushes the component above it and
    /// `append-children` leaves the element on top again, which is the stack the surrounding
    /// edits expect. `component` names the held reference of each slot id; an unknown id is a
    /// bug in the adapter that registered it.
    pub fn take_resolving<'b>(
        &mut self,
        component: impl Fn(u64) -> &'b crate::presentation::ComponentRef,
    ) -> Vec<Mutation<'b>>
    where
        'a: 'b,
    {
        let mut batch = Vec::with_capacity(self.batch.len());
        for edit in self.batch.drain(..) {
            match edit {
                Edit::Wire(mutation) => batch.push(mutation),
                Edit::Slot(id) => {
                    batch.push(Mutation::MountComponent(component(id)));
                    batch.push(Mutation::AppendChildren(1));
                }
            }
        }
        batch
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    fn push(&mut self, mutation: Mutation<'a>) {
        self.batch.push(Edit::Wire(mutation));
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
        self.push(Mutation::PushId(node_id(id)));
    }

    fn set_id(&mut self, id: ElementId) {
        self.push(Mutation::SetId(node_id(id)));
    }

    fn child(&mut self, index: usize) {
        self.push(Mutation::Child(count(index)));
    }

    fn pop(&mut self) {
        self.push(Mutation::Pop);
    }

    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        self.push(Mutation::CreateElement((
            tag.to_owned(),
            ns.map(str::to_owned),
        )));
    }

    fn create_text(&mut self, value: &str) {
        self.push(Mutation::CreateText(value.to_owned()));
    }

    fn clone(&mut self) {
        self.push(Mutation::CloneNode);
    }

    fn append_children(&mut self, m: usize) {
        self.push(Mutation::AppendChildren(count(m)));
    }

    fn replace_with(&mut self, m: usize) {
        self.push(Mutation::ReplaceWith(count(m)));
    }

    fn insert_after(&mut self, m: usize) {
        self.push(Mutation::InsertAfter(count(m)));
    }

    fn insert_before(&mut self, m: usize) {
        self.push(Mutation::InsertBefore(count(m)));
    }

    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        if name == SLOT_ATTRIBUTE {
            // Written once, while the slot element is created: it is keyed by its slot id, so
            // another component is another element and Dioxus never rewrites a live one (that
            // would leave the old component mounted beside the new one).
            let BorrowedAttributeValue::Int(id) = value else {
                panic!("`{SLOT_ATTRIBUTE}` is an integer slot id");
            };
            self.batch.push(Edit::Slot(
                u64::try_from(id).expect("slot ids are unsigned"),
            ));
            return;
        }
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
        self.push(Mutation::SetAttribute((
            name.to_owned(),
            ns.map(str::to_owned),
            value,
        )));
    }

    fn set_text(&mut self, value: &str) {
        self.push(Mutation::SetText(value.to_owned()));
    }

    fn add_event_listener(&mut self, name: &str) {
        self.push(Mutation::AddEventListener(name.to_owned()));
    }

    fn remove_event_listener(&mut self, name: &str) {
        self.push(Mutation::RemoveEventListener(name.to_owned()));
    }

    fn remove(&mut self) {
        self.push(Mutation::Remove);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::presentation::AttributeValue;

    fn one(write: impl FnOnce(&mut WireMutations<'static>)) -> Mutation<'static> {
        let mut edits = WireMutations::default();
        write(&mut edits);
        let batch = edits.take();
        // `take` drains: the recorder is empty for the next render.
        assert!(edits.is_empty());
        assert_eq!(batch.len(), 1);
        batch.into_iter().next().unwrap()
    }

    // The wire codec is the guest↔host presentation contract: every `WriteMutations` op must
    // keep mapping to exactly its declared wire mutation, in the declared width.
    #[test]
    fn every_op_maps_to_its_wire_mutation() {
        assert!(matches!(
            one(|edits| edits.push_id(ElementId::from_raw(7))),
            Mutation::PushId(7)
        ));
        assert!(matches!(
            one(|edits| edits.set_id(ElementId::from_raw(9))),
            Mutation::SetId(9)
        ));
        assert!(matches!(one(|edits| edits.child(2)), Mutation::Child(2)));
        assert!(matches!(one(|edits| edits.pop()), Mutation::Pop));
        assert!(matches!(
            one(|edits| edits.create_element("div", Some("ns"))),
            Mutation::CreateElement((tag, ns)) if tag == "div" && ns.as_deref() == Some("ns")
        ));
        assert!(matches!(
            one(|edits| edits.create_element("span", None)),
            Mutation::CreateElement((tag, ns)) if tag == "span" && ns.is_none()
        ));
        assert!(matches!(
            one(|edits| edits.create_text("hello")),
            Mutation::CreateText(value) if value == "hello"
        ));
        assert!(matches!(one(|edits| edits.clone()), Mutation::CloneNode));
        assert!(matches!(
            one(|edits| edits.append_children(3)),
            Mutation::AppendChildren(3)
        ));
        assert!(matches!(
            one(|edits| edits.replace_with(1)),
            Mutation::ReplaceWith(1)
        ));
        assert!(matches!(
            one(|edits| edits.insert_after(4)),
            Mutation::InsertAfter(4)
        ));
        assert!(matches!(
            one(|edits| edits.insert_before(5)),
            Mutation::InsertBefore(5)
        ));
        assert!(matches!(
            one(|edits| edits.set_attribute("class", None, BorrowedAttributeValue::Text("tile"))),
            Mutation::SetAttribute((name, ns, AttributeValue::Text(value)))
                if name == "class" && ns.is_none() && value == "tile"
        ));
        assert!(matches!(
            one(|edits| edits.set_attribute("ratio", Some("ns"), BorrowedAttributeValue::Float(0.5))),
            Mutation::SetAttribute((name, ns, AttributeValue::Float(value)))
                if name == "ratio" && ns.as_deref() == Some("ns") && value == 0.5
        ));
        assert!(matches!(
            one(|edits| edits.set_attribute("width", None, BorrowedAttributeValue::Int(-3))),
            Mutation::SetAttribute((name, ns, AttributeValue::Int(value)))
                if name == "width" && ns.is_none() && value == -3
        ));
        assert!(matches!(
            one(|edits| edits.set_attribute("hidden", None, BorrowedAttributeValue::Bool(true))),
            Mutation::SetAttribute((name, ns, AttributeValue::Boolean(value)))
                if name == "hidden" && ns.is_none() && value
        ));
        assert!(matches!(
            one(|edits| edits.set_attribute("data-x", None, BorrowedAttributeValue::None)),
            Mutation::SetAttribute((name, ns, AttributeValue::None))
                if name == "data-x" && ns.is_none()
        ));
        assert!(matches!(
            one(|edits| edits.set_text("bye")),
            Mutation::SetText(value) if value == "bye"
        ));
        assert!(matches!(
            one(|edits| edits.add_event_listener("click")),
            Mutation::AddEventListener(name) if name == "click"
        ));
        assert!(matches!(
            one(|edits| edits.remove_event_listener("keydown")),
            Mutation::RemoveEventListener(name) if name == "keydown"
        ));
        assert!(matches!(one(|edits| edits.remove()), Mutation::Remove));
    }

    // A component reference is a host resource a native test cannot mint, so the expansion
    // into `mount-component` is covered end to end by the `clay_mdk` wasm tests; here, that the
    // attribute is recorded as a slot at its position rather than as an attribute.
    #[test]
    fn a_slot_attribute_is_recorded_as_a_slot_not_an_attribute() {
        let mut edits = WireMutations::default();
        edits.push_id(ElementId::from_raw(3));
        edits.set_attribute(SLOT_ATTRIBUTE, None, BorrowedAttributeValue::Int(7));
        edits.pop();
        assert!(matches!(
            edits.batch.as_slice(),
            [
                Edit::Wire(Mutation::PushId(3)),
                Edit::Slot(7),
                Edit::Wire(Mutation::Pop)
            ]
        ));
    }

    #[test]
    #[should_panic(expected = "slot 7 written to a batch with no slot registry")]
    fn taking_a_slot_without_a_registry_is_an_adapter_bug() {
        let mut edits = WireMutations::default();
        edits.set_attribute(SLOT_ATTRIBUTE, None, BorrowedAttributeValue::Int(7));
        edits.take();
    }

    #[test]
    fn batch_preserves_write_order_and_take_drains() {
        let mut edits = WireMutations::default();
        assert!(edits.is_empty());
        edits.push_id(ElementId::from_raw(1));
        edits.create_text("a");
        edits.append_children(1);
        edits.pop();
        let batch = edits.take();
        assert!(edits.is_empty());
        assert!(matches!(
            batch.as_slice(),
            [
                Mutation::PushId(1),
                Mutation::CreateText(_),
                Mutation::AppendChildren(1),
                Mutation::Pop,
            ]
        ));
        assert!(edits.take().is_empty());
    }

    #[test]
    #[should_panic(expected = "`Any` attribute `class` cannot cross the presentation boundary")]
    fn an_any_attribute_cannot_cross_the_boundary() {
        let any: std::rc::Rc<dyn dioxus_core::AnyValue> = std::rc::Rc::new(5u32);
        one(|edits| {
            edits.set_attribute("class", None, BorrowedAttributeValue::Any(&any));
        });
    }

    #[test]
    #[should_panic(expected = "element id exceeds the wire width")]
    fn an_element_id_beyond_the_wire_width_is_an_adapter_bug() {
        one(|edits| edits.push_id(ElementId::from_raw(u32::MAX as usize + 1)));
    }

    #[test]
    #[should_panic(expected = "stack count exceeds the wire width")]
    fn a_stack_count_beyond_the_wire_width_is_an_adapter_bug() {
        one(|edits| edits.append_children(u32::MAX as usize + 1));
    }
}
