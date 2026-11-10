//! The port's DOM: an arena of element and text nodes with stable keys.
//!
//! Original to this crate (no upstream counterpart). Upstream's classes read
//! and write a live DOM; here the rendered tree is a `Document`, FileDiff's
//! render edits it, the managers read it through their host traits, and a
//! platform layer (Dioxus) renders it. The tree holds `hast::Properties`, so
//! hast output is converted at the boundary (`create_from_hast`: hast in, keyed
//! nodes out); it is not a second hast.
//!
//! Keys are arena indexes and are never reused while the document lives:
//! removing a node detaches it (its subtree keeps its shape) but does not free
//! the slot, so a key held by a manager never names a different node later.
//! Methods take `&self` (interior mutability) because the managers re-enter
//! their hosts from callbacks, as upstream's classes re-enter the DOM.
//! Attribute values are strings; `Properties` keeps insertion order.

use std::borrow::Borrow;
use std::cell::RefCell;
use std::rc::Rc;

use crate::hast;
use crate::hast::PropValue;
use crate::hast::Properties;

pub type NodeKey = usize;

enum NodeData {
    Element { tag: String, properties: Properties },
    Text(String),
}

struct Node {
    data: NodeData,
    parent: Option<NodeKey>,
    children: Vec<NodeKey>,
}

/// An edit of the tree, reported to the observer after it is applied.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    SetAttribute {
        node: NodeKey,
        name: String,
        value: String,
    },
    RemoveAttribute {
        node: NodeKey,
        name: String,
    },
    SetStyleProperty {
        node: NodeKey,
        name: String,
        value: String,
    },
    RemoveStyleProperty {
        node: NodeKey,
        name: String,
    },
    Append {
        parent: NodeKey,
        child: NodeKey,
    },
    InsertBefore {
        parent: NodeKey,
        child: NodeKey,
        reference: Option<NodeKey>,
    },
    /// Only reported for a node that was attached.
    Remove {
        node: NodeKey,
    },
    ReplaceChildren {
        parent: NodeKey,
        children: Vec<NodeKey>,
    },
}

type Observer = Rc<dyn Fn(&Document, &Mutation)>;

#[derive(Default)]
pub struct Document {
    nodes: RefCell<Vec<Node>>,
    observer: RefCell<Option<Observer>>,
}

fn attribute_text(value: &PropValue) -> Option<String> {
    match value {
        PropValue::Undefined => None,
        PropValue::Str(text) => Some(text.clone()),
        PropValue::Int(number) => Some(number.to_string()),
        PropValue::Float(number) => Some(number.to_string()),
        PropValue::Bool(true) => Some(String::new()),
        PropValue::Bool(false) => None,
    }
}

impl Document {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reports every edit made through the public mutators (not the building of
    /// a detached subtree) after it is applied; the platform layer marks its
    /// component dirty from it. At most one observer.
    pub fn set_observer(&self, observer: Option<Observer>) {
        *self.observer.borrow_mut() = observer;
    }

    fn notify(&self, mutation: &Mutation) {
        let observer = self.observer.borrow().clone();
        if let Some(observer) = observer {
            observer(self, mutation);
        }
    }

    fn push(&self, data: NodeData) -> NodeKey {
        let mut nodes = self.nodes.borrow_mut();
        nodes.push(Node {
            data,
            parent: None,
            children: Vec::new(),
        });
        nodes.len() - 1
    }

    /// A detached element (`document.createElement`).
    pub fn create_element(&self, tag: &str) -> NodeKey {
        self.push(NodeData::Element {
            tag: tag.to_owned(),
            properties: Properties::new(),
        })
    }

    /// Whether `node` names a node of this document.
    pub fn exists(&self, node: NodeKey) -> bool {
        node < self.nodes.borrow().len()
    }

    pub fn create_text(&self, text: &str) -> NodeKey {
        self.push(NodeData::Text(text.to_owned()))
    }

    /// The detached subtree a hast element describes.
    pub fn create_from_hast(&self, element: &hast::Element) -> NodeKey {
        let key = self.push(NodeData::Element {
            tag: element.tag_name.clone(),
            properties: element.properties.clone(),
        });
        for child in &element.children {
            let child_key = match child {
                hast::ElementContent::Element(element) => self.create_from_hast(element),
                hast::ElementContent::Text(text) => self.create_text(&text.value),
            };
            self.attach(key, child_key);
        }
        key
    }

    /// `parent.appendChild(child)`: moves `child` if it is attached elsewhere.
    pub fn append_child(&self, parent: impl Borrow<NodeKey>, child: impl Borrow<NodeKey>) {
        let parent = *parent.borrow();
        let child = *child.borrow();
        self.attach(parent, child);
        self.notify(&Mutation::Append { parent, child });
    }

    fn attach(&self, parent: NodeKey, child: NodeKey) {
        self.detach(child);
        let mut nodes = self.nodes.borrow_mut();
        nodes[parent].children.push(child);
        nodes[child].parent = Some(parent);
    }

    /// `parent.insertBefore(child, reference)`; `None` appends.
    pub fn insert_before(
        &self,
        parent: impl Borrow<NodeKey>,
        child: impl Borrow<NodeKey>,
        reference: Option<NodeKey>,
    ) {
        let parent = *parent.borrow();
        let child = *child.borrow();
        self.detach(child);
        let mut nodes = self.nodes.borrow_mut();
        let position = reference
            .and_then(|reference| {
                nodes[parent]
                    .children
                    .iter()
                    .position(|key| *key == reference)
            })
            .unwrap_or(nodes[parent].children.len());
        nodes[parent].children.insert(position, child);
        nodes[child].parent = Some(parent);
        drop(nodes);
        self.notify(&Mutation::InsertBefore {
            parent,
            child,
            reference,
        });
    }

    /// `node.remove()`: detaches the node from its parent.
    pub fn remove(&self, node: impl Borrow<NodeKey>) {
        let node = *node.borrow();
        if self.parent(node).is_some() {
            self.detach(node);
            self.notify(&Mutation::Remove { node });
        }
    }

    fn detach(&self, node: NodeKey) {
        let mut nodes = self.nodes.borrow_mut();
        if let Some(parent) = nodes[node].parent.take() {
            nodes[parent].children.retain(|key| *key != node);
        }
    }

    /// `parent.replaceChildren(...children)`: the old children are detached.
    pub fn replace_children(&self, parent: impl Borrow<NodeKey>, children: &[NodeKey]) {
        let parent = *parent.borrow();
        let old = std::mem::take(&mut self.nodes.borrow_mut()[parent].children);
        for key in old {
            self.nodes.borrow_mut()[key].parent = None;
        }
        for child in children {
            self.attach(parent, *child);
        }
        self.notify(&Mutation::ReplaceChildren {
            parent,
            children: children.to_vec(),
        });
    }

    pub fn parent(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        self.nodes.borrow()[node].parent
    }

    /// `element.children`: element children only.
    pub fn children(&self, node: impl Borrow<NodeKey>) -> Vec<NodeKey> {
        let node = *node.borrow();
        let nodes = self.nodes.borrow();
        nodes[node]
            .children
            .iter()
            .copied()
            .filter(|key| matches!(nodes[*key].data, NodeData::Element { .. }))
            .collect()
    }

    /// `element.previousElementSibling`
    pub fn previous_element_sibling(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        let nodes = self.nodes.borrow();
        let siblings = &nodes[nodes[node].parent?].children;
        let position = siblings.iter().position(|key| *key == node)?;
        siblings[..position]
            .iter()
            .rev()
            .copied()
            .find(|key| matches!(nodes[*key].data, NodeData::Element { .. }))
    }

    /// `element.nextSibling` when that node is an element (a text node between
    /// them makes it `None`, as `nextSibling instanceof HTMLElement` is false).
    pub fn next_sibling_element(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        let nodes = self.nodes.borrow();
        let siblings = &nodes[nodes[node].parent?].children;
        let position = siblings.iter().position(|key| *key == node)?;
        let next = *siblings.get(position + 1)?;
        matches!(nodes[next].data, NodeData::Element { .. }).then_some(next)
    }

    /// `element.tagName`: upper case, as the DOM reports HTML elements.
    pub fn tag_name(&self, node: impl Borrow<NodeKey>) -> String {
        let node = *node.borrow();
        match &self.nodes.borrow()[node].data {
            NodeData::Element { tag, .. } => tag.to_uppercase(),
            NodeData::Text(_) => String::new(),
        }
    }

    /// `node.textContent`
    pub fn text_content(&self, node: impl Borrow<NodeKey>) -> String {
        let node = *node.borrow();
        let nodes = self.nodes.borrow();
        let mut out = String::new();
        let mut stack = vec![node];
        while let Some(key) = stack.pop() {
            match &nodes[key].data {
                NodeData::Text(text) => out.push_str(text),
                NodeData::Element { .. } => stack.extend(nodes[key].children.iter().rev()),
            }
        }
        out
    }

    pub fn get_attribute(&self, node: impl Borrow<NodeKey>, name: &str) -> Option<String> {
        let node = *node.borrow();
        match &self.nodes.borrow()[node].data {
            NodeData::Element { properties, .. } => properties.get(name).and_then(attribute_text),
            NodeData::Text(_) => None,
        }
    }

    pub fn set_attribute(&self, node: impl Borrow<NodeKey>, name: &str, value: &str) {
        let node = *node.borrow();
        self.with_properties(node, |properties| properties.set(name, value));
        self.notify(&Mutation::SetAttribute {
            node,
            name: name.to_owned(),
            value: value.to_owned(),
        });
    }

    pub fn remove_attribute(&self, node: impl Borrow<NodeKey>, name: &str) {
        let node = *node.borrow();
        self.with_properties(node, |properties| properties.remove(name));
        self.notify(&Mutation::RemoveAttribute {
            node,
            name: name.to_owned(),
        });
    }

    /// `element.style.setProperty(name, value)`
    pub fn set_style_property(&self, node: impl Borrow<NodeKey>, name: &str, value: &str) {
        let node = *node.borrow();
        self.with_properties(node, |properties| {
            properties.set_style_property(name, value)
        });
        self.notify(&Mutation::SetStyleProperty {
            node,
            name: name.to_owned(),
            value: value.to_owned(),
        });
    }

    /// `element.style.removeProperty(name)`
    pub fn remove_style_property(&self, node: impl Borrow<NodeKey>, name: &str) {
        let node = *node.borrow();
        self.with_properties(node, |properties| properties.remove_style_property(name));
        self.notify(&Mutation::RemoveStyleProperty {
            node,
            name: name.to_owned(),
        });
    }

    fn with_properties(&self, node: NodeKey, edit: impl FnOnce(&mut Properties)) {
        if let NodeData::Element { properties, .. } = &mut self.nodes.borrow_mut()[node].data {
            edit(properties);
        }
    }

    /// Descendants of `root` (not `root`), in document order, having attribute
    /// `name` (with `value` when given): `[name]` / `[name="value"]`.
    pub fn query_by_attribute(
        &self,
        root: impl Borrow<NodeKey>,
        name: &str,
        value: Option<&str>,
    ) -> Vec<NodeKey> {
        let root = *root.borrow();
        let mut found = Vec::new();
        let mut stack: Vec<NodeKey> = self.nodes.borrow()[root]
            .children
            .iter()
            .rev()
            .copied()
            .collect();
        while let Some(key) = stack.pop() {
            if self
                .get_attribute(key, name)
                .is_some_and(|actual| value.is_none_or(|expected| actual == expected))
            {
                found.push(key);
            }
            stack.extend(self.nodes.borrow()[key].children.iter().rev());
        }
        found
    }

    /// `element.closest(selector)` for a predicate: the node itself, then its
    /// ancestors.
    pub fn closest(
        &self,
        node: impl Borrow<NodeKey>,
        matches: impl Fn(NodeKey) -> bool,
    ) -> Option<NodeKey> {
        let node = *node.borrow();
        let mut current = Some(node);
        while let Some(key) = current {
            if matches(key) {
                return Some(key);
            }
            current = self.parent(key);
        }
        None
    }

    /// `element.assignedSlot`: the `<slot>` element of the document whose
    /// `name` equals the node's `slot` attribute (the first created, if
    /// several). The renderer places annotation content where the slot sits
    /// upstream, so the slot is found by name in the model, whichever tree it
    /// hangs in.
    pub fn assigned_slot(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        let slot_name = self.get_attribute(node, "slot")?;
        let count = self.nodes.borrow().len();
        (0..count).find(|key| {
            self.tag_name(*key) == "SLOT"
                && self.get_attribute(*key, "name").as_deref() == Some(&slot_name)
        })
    }
}
