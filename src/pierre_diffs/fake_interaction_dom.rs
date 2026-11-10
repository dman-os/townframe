//! A scripted fake platform for `InteractionManager` over the port's
//! `Document`: named nodes built from a scenario tree, a table-driven
//! `elementFromPoint`, listener registration, and a recording trace that
//! includes the document's mutations (through its observer). Shared by the
//! oracle replay and the ported upstream tests. Test-only, original to this
//! crate.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde::Deserialize;
use serde_json::Value;
use serde_json::json;

use crate::document::Document;
use crate::document::Mutation;
use crate::document::NodeKey;
use crate::fake_dom::FakeFrames;
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

#[derive(Default)]
pub struct Shared {
    pub labels: RefCell<HashMap<NodeKey, String>>,
    pub trace: RefCell<Vec<Value>>,
    pub recording: Cell<bool>,
}

impl Shared {
    pub fn emit(&self, entry: Value) {
        if self.recording.get() {
            self.trace.borrow_mut().push(entry);
        }
    }

    // The scenario's name, or the role of a node the manager created.
    pub fn name_of(&self, doc: &Document, node: NodeKey) -> String {
        if let Some(label) = self.labels.borrow().get(&node) {
            return label.clone();
        }
        if doc
            .get_attribute(node, "data-gutter-utility-slot")
            .is_some()
        {
            "$container".to_owned()
        } else if doc.tag_name(node) == "SLOT" {
            "$slot".to_owned()
        } else if doc.tag_name(node) == "BUTTON" {
            "$button".to_owned()
        } else {
            "$temp".to_owned()
        }
    }
}

pub struct FakeDom {
    pub doc: Document,
    pub shared: Rc<Shared>,
    pub by_name: RefCell<HashMap<String, NodeKey>>,
    pub hits: RefCell<HashMap<String, Option<NodeKey>>>,
    pub hit_test_available: Cell<bool>,
    pub pre_listener: Cell<bool>,
    pub document_listener: Cell<bool>,
    pub frames: FakeFrames,
}

impl Default for FakeDom {
    fn default() -> Self {
        let doc = Document::new();
        let shared = Rc::new(Shared::default());
        let observed = Rc::clone(&shared);
        doc.set_observer(Some(Rc::new(move |doc: &Document, mutation: &Mutation| {
            let name = |node: NodeKey| observed.name_of(doc, node);
            match mutation {
                Mutation::SetAttribute {
                    node,
                    name: attribute,
                    value,
                } => {
                    observed.emit(json!(["attr.set", name(*node), attribute, value]));
                }
                Mutation::RemoveAttribute {
                    node,
                    name: attribute,
                } => {
                    observed.emit(json!(["attr.remove", name(*node), attribute]));
                }
                Mutation::Append { parent, child } => {
                    observed.emit(json!(["append", name(*parent), name(*child)]));
                }
                Mutation::Remove { node } => observed.emit(json!(["remove", name(*node)])),
                Mutation::ReplaceChildren { parent, children } => {
                    let names: Vec<String> = children.iter().map(|child| name(*child)).collect();
                    observed.emit(json!(["replaceChildren", name(*parent), names]));
                }
                Mutation::SetStyleProperty { .. }
                | Mutation::RemoveStyleProperty { .. }
                | Mutation::InsertBefore { .. } => {}
            }
        })));
        Self {
            doc,
            shared,
            by_name: RefCell::default(),
            hits: RefCell::default(),
            hit_test_available: Cell::new(false),
            pre_listener: Cell::new(false),
            document_listener: Cell::new(false),
            frames: FakeFrames::default(),
        }
    }
}

impl FakeDom {
    pub fn build(&self, node: &TreeNode, parent: Option<NodeKey>) -> NodeKey {
        let key = self.doc.create_element(&node.tag);
        for (name, value) in &node.attrs {
            self.doc.set_attribute(key, name, value);
        }
        self.shared
            .labels
            .borrow_mut()
            .insert(key, node.name.clone());
        self.by_name.borrow_mut().insert(node.name.clone(), key);
        if let Some(parent) = parent {
            self.doc.append_child(parent, key);
        }
        for child in &node.children {
            self.build(child, Some(key));
        }
        key
    }

    // `$button`, `$container`, `$slot`: nodes the manager created, found under
    // the scenario's roots by their role attribute.
    pub fn id(&self, name: &str) -> NodeKey {
        if name.starts_with('$') {
            let (attribute, value) = match name {
                "$container" => ("data-gutter-utility-slot", None),
                "$button" => ("data-utility-button", None),
                "$slot" => ("name", Some("gutter-utility-slot")),
                other => panic!("unknown role {other}"),
            };
            let roots: Vec<NodeKey> = self.by_name.borrow().values().copied().collect();
            return roots
                .into_iter()
                .flat_map(|root| self.doc.query_by_attribute(root, attribute, value))
                .next()
                .expect("a node with that role exists");
        }
        self.by_name.borrow()[name]
    }

    pub fn emit(&self, entry: Value) {
        self.shared.emit(entry);
    }

    pub fn name_of(&self, node: NodeKey) -> String {
        self.shared.name_of(&self.doc, node)
    }

    pub fn recording(&self) -> &Cell<bool> {
        &self.shared.recording
    }

    pub fn trace(&self) -> Vec<Value> {
        self.shared.trace.borrow().clone()
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
    fn document(&self) -> &Document {
        &self.doc
    }

    fn element_from_point(&self, x: f64, y: f64) -> HitTest<NodeKey> {
        if !self.hit_test_available.get() {
            return HitTest::Unavailable;
        }
        match self.hits.borrow().get(&format!("{x},{y}")) {
            Some(Some(key)) => HitTest::Element(*key),
            _ => HitTest::NoElement,
        }
    }

    fn add_pre_listeners(&self, pre: NodeKey) {
        for kind in ["click", "pointerdown", "pointermove", "pointerleave"] {
            self.emit(json!(["pre.addEventListener", self.name_of(pre), kind]));
        }
        self.pre_listener.set(true);
    }

    fn remove_pre_listeners(&self, pre: NodeKey) {
        for kind in ["click", "pointerdown", "pointermove", "pointerleave"] {
            self.emit(json!(["pre.removeEventListener", self.name_of(pre), kind]));
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
