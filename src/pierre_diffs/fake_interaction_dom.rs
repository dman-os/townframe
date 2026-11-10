//! A scripted fake of the rendered hast model for `InteractionManager`:
//! an arena of nodes with parents, children, attributes and slots, the
//! structural reads and writes the manager asks of its host, a table-driven
//! `elementFromPoint`, and a recording trace. Shared by the oracle replay and
//! the ported upstream tests. Test-only, original to this crate.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::fake_dom::FakeFrames;
use crate::hast;
use crate::hast::PropValue;
use crate::managers::interaction_manager::HitTest;
use crate::managers::interaction_manager::InteractionHost;
use crate::managers::universal_rendering_manager::AnimationFrames;
use crate::managers::universal_rendering_manager::FrameId;

#[derive(Deserialize, Clone)]
pub struct TreeNode {
    pub tag: String,
    #[serde(default)]
    pub attrs: Vec<(String, String)>,
    #[serde(default)]
    pub children: Vec<TreeNode>,
    pub name: String,
}

pub struct DomNode {
    pub label: Option<String>,
    pub tag: String,
    pub attrs: Vec<(String, String)>,
    pub parent: Option<usize>,
    pub children: Vec<usize>,
    pub assigned_slot: Option<usize>,
    pub internal: bool,
}

#[derive(Default)]
pub struct FakeDom {
    pub nodes: RefCell<Vec<DomNode>>,
    pub by_name: RefCell<HashMap<String, usize>>,
    pub hits: RefCell<HashMap<String, Option<usize>>>,
    pub hit_test_available: Cell<bool>,
    pub trace: RefCell<Vec<Value>>,
    pub recording: Cell<bool>,
    pub pre_listener: Cell<bool>,
    pub document_listener: Cell<bool>,
    pub frames: FakeFrames,
}

impl FakeDom {
    pub fn build(&self, node: &TreeNode, parent: Option<usize>) -> usize {
        let id = self.push(DomNode {
            label: Some(node.name.clone()),
            tag: node.tag.to_uppercase(),
            attrs: node.attrs.clone(),
            parent,
            children: Vec::new(),
            assigned_slot: None,
            internal: false,
        });
        self.by_name.borrow_mut().insert(node.name.clone(), id);
        if let Some(parent) = parent {
            self.nodes.borrow_mut()[parent].children.push(id);
        }
        for child in &node.children {
            self.build(child, Some(id));
        }
        id
    }

    pub fn push(&self, node: DomNode) -> usize {
        let mut nodes = self.nodes.borrow_mut();
        nodes.push(node);
        nodes.len() - 1
    }

    // `$button`, `$container`, `$slot`: nodes the manager created, by role.
    pub fn id(&self, name: &str) -> usize {
        if name.starts_with('$') {
            let count = self.nodes.borrow().len();
            return (0..count)
                .find(|id| self.name_of(*id) == name)
                .expect("a node with that role exists");
        }
        self.by_name.borrow()[name]
    }

    pub fn emit(&self, entry: Value) {
        if self.recording.get() {
            self.trace.borrow_mut().push(entry);
        }
    }

    pub fn name_of(&self, id: usize) -> String {
        let nodes = self.nodes.borrow();
        let node = &nodes[id];
        if let Some(label) = &node.label {
            return label.clone();
        }
        let has = |name: &str| node.attrs.iter().any(|(key, _)| key == name);
        if has("data-gutter-utility-slot") {
            "$container".to_owned()
        } else if node.tag == "SLOT" {
            "$slot".to_owned()
        } else if node.tag == "BUTTON" {
            "$button".to_owned()
        } else {
            "$temp".to_owned()
        }
    }

    pub fn detach(&self, id: usize) {
        let parent = self.nodes.borrow()[id].parent;
        if let Some(parent) = parent {
            self.nodes.borrow_mut()[parent]
                .children
                .retain(|child| *child != id);
            self.nodes.borrow_mut()[id].parent = None;
        }
    }

    pub fn build_from_hast(&self, element: &hast::Element) -> usize {
        let attrs = element
            .properties
            .defined()
            .map(|(key, value)| {
                let text = match value {
                    PropValue::Str(text) => text.clone(),
                    PropValue::Int(number) => number.to_string(),
                    PropValue::Float(number) => number.to_string(),
                    PropValue::Bool(_) | PropValue::Undefined => String::new(),
                };
                (key.to_owned(), text)
            })
            .collect();
        let id = self.push(DomNode {
            label: None,
            tag: element.tag_name.to_uppercase(),
            attrs,
            parent: None,
            children: Vec::new(),
            assigned_slot: None,
            internal: true,
        });
        for child in &element.children {
            if let hast::ElementContent::Element(child) = child {
                let child_id = self.build_from_hast(child);
                self.nodes.borrow_mut()[child_id].parent = Some(id);
                self.nodes.borrow_mut()[id].children.push(child_id);
            }
        }
        id
    }

    pub fn descendants(&self, root: usize, found: &mut Vec<usize>) {
        let children = self.nodes.borrow()[root].children.clone();
        for child in children {
            found.push(child);
            self.descendants(child, found);
        }
    }
}

impl AnimationFrames for FakeDom {
    fn request_animation_frame(&self, callback: Box<dyn FnOnce(f64)>) -> FrameId {
        let id = self.frames.request_animation_frame(callback);
        self.emit(json!(["raf.request", id]));
        id
    }

    fn cancel_animation_frame(&self, frame_id: FrameId) {
        self.frames.cancel_animation_frame(frame_id);
        self.emit(json!(["raf.cancel", frame_id]));
    }
}

impl InteractionHost for FakeDom {
    type Element = usize;

    fn parent(&self, element: &usize) -> Option<usize> {
        self.nodes.borrow()[*element].parent
    }

    fn children(&self, element: &usize) -> Vec<usize> {
        self.nodes.borrow()[*element].children.clone()
    }

    fn previous_element_sibling(&self, element: &usize) -> Option<usize> {
        let parent = self.parent(element)?;
        let nodes = self.nodes.borrow();
        let siblings = &nodes[parent].children;
        let position = siblings.iter().position(|id| id == element)?;
        position.checked_sub(1).map(|previous| siblings[previous])
    }

    fn next_sibling_element(&self, element: &usize) -> Option<usize> {
        let parent = self.parent(element)?;
        let nodes = self.nodes.borrow();
        let siblings = &nodes[parent].children;
        let position = siblings.iter().position(|id| id == element)?;
        siblings.get(position + 1).copied()
    }

    fn tag_name(&self, element: &usize) -> String {
        self.nodes.borrow()[*element].tag.clone()
    }

    fn get_attribute(&self, element: &usize, name: &str) -> Option<String> {
        self.nodes.borrow()[*element]
            .attrs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    }

    fn set_attribute(&self, element: &usize, name: &str, value: &str) {
        {
            let mut nodes = self.nodes.borrow_mut();
            let node = &mut nodes[*element];
            match node.attrs.iter_mut().find(|(key, _)| key == name) {
                Some((_, slot)) => *slot = value.to_owned(),
                None => node.attrs.push((name.to_owned(), value.to_owned())),
            }
        }
        if !self.nodes.borrow()[*element].internal {
            self.emit(json!(["attr.set", self.name_of(*element), name, value]));
        }
    }

    fn remove_attribute(&self, element: &usize, name: &str) {
        self.nodes.borrow_mut()[*element]
            .attrs
            .retain(|(key, _)| key != name);
        if !self.nodes.borrow()[*element].internal {
            self.emit(json!(["attr.remove", self.name_of(*element), name]));
        }
    }

    fn assigned_slot(&self, element: &usize) -> Option<usize> {
        self.nodes.borrow()[*element].assigned_slot
    }

    fn query_by_attribute(&self, root: &usize, name: &str, value: Option<&str>) -> Vec<usize> {
        let mut all = Vec::new();
        self.descendants(*root, &mut all);
        all.into_iter()
            .filter(|id| match self.get_attribute(id, name) {
                None => false,
                Some(actual) => value.is_none_or(|expected| actual == expected),
            })
            .collect()
    }

    fn element_from_point(&self, x: f64, y: f64) -> HitTest<usize> {
        if !self.hit_test_available.get() {
            return HitTest::Unavailable;
        }
        match self.hits.borrow().get(&format!("{x},{y}")) {
            Some(Some(id)) => HitTest::Element(*id),
            _ => HitTest::NoElement,
        }
    }

    fn create_element(&self, tag: &str) -> usize {
        self.push(DomNode {
            label: None,
            tag: tag.to_uppercase(),
            attrs: Vec::new(),
            parent: None,
            children: Vec::new(),
            assigned_slot: None,
            internal: false,
        })
    }

    fn create_element_from_hast(&self, element: &hast::Element) -> usize {
        self.build_from_hast(element)
    }

    fn append_child(&self, parent: &usize, child: &usize) {
        self.detach(*child);
        self.nodes.borrow_mut()[*parent].children.push(*child);
        self.nodes.borrow_mut()[*child].parent = Some(*parent);
        self.emit(json!([
            "append",
            self.name_of(*parent),
            self.name_of(*child)
        ]));
    }

    fn remove_node(&self, node: &usize) {
        let parent = self.parent(node);
        self.detach(*node);
        if let Some(parent) = parent
            && !self.nodes.borrow()[parent].internal
        {
            self.emit(json!(["remove", self.name_of(*node)]));
        }
    }

    fn replace_children(&self, parent: &usize, children: &[usize]) {
        let old = self.nodes.borrow()[*parent].children.clone();
        for old_child in old {
            self.nodes.borrow_mut()[old_child].parent = None;
        }
        self.nodes.borrow_mut()[*parent].children.clear();
        for child in children {
            self.detach(*child);
            self.nodes.borrow_mut()[*parent].children.push(*child);
            self.nodes.borrow_mut()[*child].parent = Some(*parent);
        }
        let names: Vec<String> = children.iter().map(|child| self.name_of(*child)).collect();
        self.emit(json!(["replaceChildren", self.name_of(*parent), names]));
    }

    fn add_pre_listeners(&self, pre: &usize) {
        for kind in ["click", "pointerdown", "pointermove", "pointerleave"] {
            self.emit(json!(["pre.addEventListener", self.name_of(*pre), kind]));
        }
        self.pre_listener.set(true);
    }

    fn remove_pre_listeners(&self, pre: &usize) {
        for kind in ["click", "pointerdown", "pointermove", "pointerleave"] {
            self.emit(json!(["pre.removeEventListener", self.name_of(*pre), kind]));
        }
        self.pre_listener.set(false);
    }

    fn add_document_listeners(&self) {
        for kind in ["pointermove", "pointerup", "pointercancel"] {
            self.emit(json!(["document.addEventListener", kind]));
        }
        self.document_listener.set(true);
    }

    fn remove_document_listeners(&self) {
        for kind in ["pointermove", "pointerup", "pointercancel"] {
            self.emit(json!(["document.removeEventListener", kind]));
        }
        self.document_listener.set(false);
    }
}
