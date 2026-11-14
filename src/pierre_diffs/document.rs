//! The port's DOM: an arena of element and text nodes with generational keys.
//!
//! Original to this crate (no upstream counterpart). Upstream's classes read
//! and write a live DOM; here the rendered tree is a `Document`, FileDiff's
//! render edits it, the managers read it through their host traits, and a
//! platform layer (Dioxus) renders it. The tree holds `hast::Properties`, so
//! hast output is converted at the boundary (`create_from_hast`: hast in, keyed
//! nodes out); it is not a second hast.
//!
//! A [`NodeKey`] is a slot index plus the slot's generation. [`Document::remove`]
//! only detaches a node: a detached node stays usable, because the managers
//! hold keys of nodes that have left the tree until their next setup cleans
//! them up, as upstream's classes hold detached DOM nodes. The owner of a
//! subtree it is done with calls [`Document::discard`], which detaches and
//! frees the whole subtree: its slots go on a free list and their generation is
//! bumped, so the arena stays bounded under a virtualized window that creates
//! and drops rows forever, and a stale key fails `exists` and panics on use (an
//! invariant break) instead of naming whatever reused the slot.
//!
//! An element can have a shadow root (`attach_shadow`): a root node whose parent
//! is the host (parent walks cross the boundary, as `composedPath` does) but
//! which is not among the host's `children`. The platform layer flattens it,
//! rendering the shadow tree with the host's slotted children at its slots
//! (`assigned_slot`).
//!
//! Methods take `&self` (interior mutability) because the managers re-enter
//! their hosts from callbacks, as upstream's classes re-enter the DOM.
//! Attribute values are strings; `Properties` keeps insertion order.

use std::borrow::Borrow;
use std::cell::RefCell;
use std::rc::Rc;

use crate::hast;
use crate::hast::PropValue;
use crate::hast::Properties;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeKey {
    index: u32,
    generation: u32,
}

enum NodeData {
    Element { tag: String, properties: Properties },
    Text(String),
}

struct Node {
    data: NodeData,
    parent: Option<NodeKey>,
    children: Vec<NodeKey>,
    /// `element.shadowRoot`: a root node whose parent is this host but which is
    /// not one of its `children`.
    shadow_root: Option<NodeKey>,
}

struct Slot {
    generation: u32,
    node: Option<Node>,
}

#[derive(Default)]
struct Arena {
    slots: Vec<Slot>,
    free: Vec<u32>,
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
    /// The node's whole property bag was edited (`edit_properties`).
    EditProperties {
        node: NodeKey,
    },
}

type Observer = Rc<dyn Fn(&Document, &Mutation)>;

#[derive(Default)]
pub struct Document {
    arena: RefCell<Arena>,
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

impl Arena {
    fn node(&self, key: NodeKey) -> &Node {
        let slot = &self.slots[key.index as usize];
        assert!(slot.generation == key.generation, "stale NodeKey {key:?}");
        slot.node.as_ref().expect("a live slot holds a node")
    }

    fn node_mut(&mut self, key: NodeKey) -> &mut Node {
        let slot = &mut self.slots[key.index as usize];
        assert!(slot.generation == key.generation, "stale NodeKey {key:?}");
        slot.node.as_mut().expect("a live slot holds a node")
    }

    fn is_element(&self, key: NodeKey) -> bool {
        matches!(self.node(key).data, NodeData::Element { .. })
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
        let mut arena = self.arena.borrow_mut();
        let node = Node {
            data,
            parent: None,
            children: Vec::new(),
            shadow_root: None,
        };
        if let Some(index) = arena.free.pop() {
            let slot = &mut arena.slots[index as usize];
            slot.node = Some(node);
            NodeKey {
                index,
                generation: slot.generation,
            }
        } else {
            arena.slots.push(Slot {
                generation: 0,
                node: Some(node),
            });
            NodeKey {
                index: u32::try_from(arena.slots.len() - 1).expect("fewer than 2^32 nodes"),
                generation: 0,
            }
        }
    }

    /// Whether `node` names a live node of this document (false for a key whose
    /// slot was freed, even if the slot has been reused since).
    pub fn exists(&self, node: NodeKey) -> bool {
        self.arena
            .borrow()
            .slots
            .get(node.index as usize)
            .is_some_and(|slot| slot.generation == node.generation && slot.node.is_some())
    }

    /// The number of slots ever allocated (live or free); bounded under churn
    /// because freed slots are reused.
    pub fn capacity(&self) -> usize {
        self.arena.borrow().slots.len()
    }

    /// A detached element (`document.createElement`).
    pub fn create_element(&self, tag: &str) -> NodeKey {
        self.push(NodeData::Element {
            tag: tag.to_owned(),
            properties: Properties::new(),
        })
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

    /// `host.shadowRoot`
    pub fn shadow_root(&self, host: impl Borrow<NodeKey>) -> Option<NodeKey> {
        self.arena.borrow().node(*host.borrow()).shadow_root
    }

    /// `host.attachShadow({ mode: 'open' })`: the new root's parent is `host`
    /// (parent walks cross the boundary, as `composedPath` does) but it is not
    /// among `host`'s `children`. A host has at most one root.
    pub fn attach_shadow(&self, host: impl Borrow<NodeKey>) -> NodeKey {
        let host = *host.borrow();
        assert!(
            self.shadow_root(host).is_none(),
            "attach_shadow: the host already has a shadow root"
        );
        let root = self.create_element("#shadow-root");
        let mut arena = self.arena.borrow_mut();
        arena.node_mut(root).parent = Some(host);
        arena.node_mut(host).shadow_root = Some(root);
        root
    }

    /// `parent.appendChild(child)`: moves `child` if it is attached elsewhere.
    pub fn append_child(&self, parent: impl Borrow<NodeKey>, child: impl Borrow<NodeKey>) {
        let (parent, child) = (*parent.borrow(), *child.borrow());
        self.attach(parent, child);
        self.notify(&Mutation::Append { parent, child });
    }

    fn attach(&self, parent: NodeKey, child: NodeKey) {
        self.detach(child);
        let mut arena = self.arena.borrow_mut();
        arena.node_mut(parent).children.push(child);
        arena.node_mut(child).parent = Some(parent);
    }

    /// `parent.insertBefore(child, reference)`; `None` appends.
    pub fn insert_before(
        &self,
        parent: impl Borrow<NodeKey>,
        child: impl Borrow<NodeKey>,
        reference: Option<NodeKey>,
    ) {
        let (parent, child) = (*parent.borrow(), *child.borrow());
        self.detach(child);
        let mut arena = self.arena.borrow_mut();
        let children = &arena.node(parent).children;
        let position = reference
            .and_then(|reference| children.iter().position(|key| *key == reference))
            .unwrap_or(children.len());
        arena.node_mut(parent).children.insert(position, child);
        arena.node_mut(child).parent = Some(parent);
        drop(arena);
        self.notify(&Mutation::InsertBefore {
            parent,
            child,
            reference,
        });
    }

    /// `node.remove()`: detaches the node from its parent; the node and its
    /// subtree stay usable (see the module docs).
    pub fn remove(&self, node: impl Borrow<NodeKey>) {
        let node = *node.borrow();
        if self.parent(node).is_some() {
            self.detach(node);
            self.notify(&Mutation::Remove { node });
        }
    }

    /// Detaches `node` and frees its whole subtree. The owner calls this for
    /// nodes it is done with, once no manager still needs them for cleanup.
    pub fn discard(&self, node: impl Borrow<NodeKey>) {
        let node = *node.borrow();
        self.remove(node);
        let mut arena = self.arena.borrow_mut();
        let mut stack = vec![node];
        while let Some(key) = stack.pop() {
            let node = arena.node_mut(key);
            let children = std::mem::take(&mut node.children);
            let shadow_root = node.shadow_root.take();
            stack.extend(children);
            stack.extend(shadow_root);
            let slot = &mut arena.slots[key.index as usize];
            slot.node = None;
            slot.generation += 1;
            arena.free.push(key.index);
        }
    }

    fn detach(&self, node: NodeKey) {
        let mut arena = self.arena.borrow_mut();
        if let Some(parent) = arena.node_mut(node).parent.take() {
            arena.node_mut(parent).children.retain(|key| *key != node);
        }
    }

    /// `parent.replaceChildren(...children)`: the old children are detached.
    pub fn replace_children(&self, parent: impl Borrow<NodeKey>, children: &[NodeKey]) {
        let parent = *parent.borrow();
        let old = std::mem::take(&mut self.arena.borrow_mut().node_mut(parent).children);
        {
            let mut arena = self.arena.borrow_mut();
            for key in old {
                arena.node_mut(key).parent = None;
            }
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
        self.arena.borrow().node(*node.borrow()).parent
    }

    /// Whether `node` is reachable from `root` by parents (itself included).
    pub fn is_attached(&self, node: impl Borrow<NodeKey>, root: impl Borrow<NodeKey>) -> bool {
        let root = *root.borrow();
        let mut current = Some(*node.borrow());
        while let Some(key) = current {
            if key == root {
                return true;
            }
            current = self.parent(key);
        }
        false
    }

    /// `node.childNodes`: every child, text nodes included.
    pub fn child_nodes(&self, node: impl Borrow<NodeKey>) -> Vec<NodeKey> {
        self.arena.borrow().node(*node.borrow()).children.clone()
    }

    /// Several attribute and style writes at once: `edit` sees the node's
    /// property bag (for utilities that edit a `Properties`).
    pub fn edit_properties(&self, node: impl Borrow<NodeKey>, edit: impl FnOnce(&mut Properties)) {
        let node = *node.borrow();
        self.with_properties(node, edit);
        self.notify(&Mutation::EditProperties { node });
    }

    /// `element.attributes`, in order, as `(name, value)` strings.
    pub fn attributes(&self, node: impl Borrow<NodeKey>) -> Vec<(String, String)> {
        match &self.arena.borrow().node(*node.borrow()).data {
            NodeData::Element { properties, .. } => properties
                .defined()
                .filter_map(|(name, value)| Some((name.to_owned(), attribute_text(value)?)))
                .collect(),
            NodeData::Text(_) => Vec::new(),
        }
    }

    /// `element.children`: element children only.
    pub fn children(&self, node: impl Borrow<NodeKey>) -> Vec<NodeKey> {
        let arena = self.arena.borrow();
        arena
            .node(*node.borrow())
            .children
            .iter()
            .copied()
            .filter(|key| arena.is_element(*key))
            .collect()
    }

    /// The first element child of `parent` at or after raw child position
    /// `start`, with its raw position: a cursor over `parent.children` that
    /// copies nothing and holds no borrow between calls, so a caller can walk
    /// the live collection and call out to the host between steps (resume at
    /// `position + 1`). It also answers `nextElementSibling` for the child at
    /// `position` without `next_element_sibling`'s scan for the node's own slot.
    pub fn element_child_from(
        &self,
        parent: impl Borrow<NodeKey>,
        start: usize,
    ) -> Option<(usize, NodeKey)> {
        let arena = self.arena.borrow();
        let children = &arena.node(*parent.borrow()).children;
        children
            .get(start..)?
            .iter()
            .enumerate()
            .find(|(_, key)| arena.is_element(**key))
            .map(|(offset, key)| (start + offset, *key))
    }

    /// `element.previousElementSibling`
    pub fn previous_element_sibling(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        let arena = self.arena.borrow();
        let siblings = &arena.node(arena.node(node).parent?).children;
        let position = siblings.iter().position(|key| *key == node)?;
        siblings[..position]
            .iter()
            .rev()
            .copied()
            .find(|key| arena.is_element(*key))
    }

    /// `element.nextSibling` when that node is an element (a text node between
    /// them makes it `None`, as `nextSibling instanceof HTMLElement` is false).
    pub fn next_sibling_element(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        let arena = self.arena.borrow();
        let siblings = &arena.node(arena.node(node).parent?).children;
        let position = siblings.iter().position(|key| *key == node)?;
        let next = *siblings.get(position + 1)?;
        arena.is_element(next).then_some(next)
    }

    /// `element.nextElementSibling`: the next sibling that is an element, text
    /// nodes skipped.
    pub fn next_element_sibling(&self, node: impl Borrow<NodeKey>) -> Option<NodeKey> {
        let node = *node.borrow();
        let arena = self.arena.borrow();
        let siblings = &arena.node(arena.node(node).parent?).children;
        let position = siblings.iter().position(|key| *key == node)?;
        siblings[position + 1..]
            .iter()
            .copied()
            .find(|key| arena.is_element(*key))
    }

    /// `element.hasAttribute(name)` (also `'name' in element.dataset`).
    pub fn has_attribute(&self, node: impl Borrow<NodeKey>, name: &str) -> bool {
        match &self.arena.borrow().node(*node.borrow()).data {
            NodeData::Element { properties, .. } => {
                properties.get(name).and_then(attribute_text).is_some()
            }
            NodeData::Text(_) => false,
        }
    }

    /// `element.tagName`: upper case, as the DOM reports HTML elements.
    pub fn tag_name(&self, node: impl Borrow<NodeKey>) -> String {
        match &self.arena.borrow().node(*node.borrow()).data {
            NodeData::Element { tag, .. } => tag.to_uppercase(),
            NodeData::Text(_) => String::new(),
        }
    }

    /// `node.textContent`
    pub fn text_content(&self, node: impl Borrow<NodeKey>) -> String {
        let arena = self.arena.borrow();
        let mut out = String::new();
        let mut stack = vec![*node.borrow()];
        while let Some(key) = stack.pop() {
            match &arena.node(key).data {
                NodeData::Text(text) => out.push_str(text),
                NodeData::Element { .. } => stack.extend(arena.node(key).children.iter().rev()),
            }
        }
        out
    }

    pub fn get_attribute(&self, node: impl Borrow<NodeKey>, name: &str) -> Option<String> {
        match &self.arena.borrow().node(*node.borrow()).data {
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
        if let NodeData::Element { properties, .. } =
            &mut self.arena.borrow_mut().node_mut(node).data
        {
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
        let mut found = Vec::new();
        let mut stack: Vec<NodeKey> = self
            .arena
            .borrow()
            .node(*root.borrow())
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
            stack.extend(self.arena.borrow().node(key).children.iter().rev());
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
        let mut current = Some(*node.borrow());
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
        let slot_name = self.get_attribute(node, "slot")?;
        let keys: Vec<NodeKey> = {
            let arena = self.arena.borrow();
            arena
                .slots
                .iter()
                .enumerate()
                .filter(|(_, slot)| slot.node.is_some())
                .map(|(index, slot)| NodeKey {
                    index: u32::try_from(index).expect("fewer than 2^32 nodes"),
                    generation: slot.generation,
                })
                .collect()
        };
        keys.into_iter().find(|key| {
            self.tag_name(key) == "SLOT"
                && self.get_attribute(key, "name").as_deref() == Some(&slot_name)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A virtualized window creates and drops rows forever; discarding what
    // leaves the window keeps the arena at the window's size.
    #[test]
    fn churning_a_window_of_rows_keeps_the_arena_bounded() {
        let document = Document::new();
        let content = document.create_element("div");
        let mut window: Vec<NodeKey> = Vec::new();
        for step in 0..2_000 {
            for _ in 0..50 {
                let row = document.create_element("div");
                document.set_attribute(row, "data-line", &step.to_string());
                let text = document.create_text("x");
                document.append_child(row, text);
                document.append_child(content, row);
                window.push(row);
            }
            if window.len() > 200 {
                for row in window.drain(..50) {
                    document.discard(row);
                }
            }
        }
        assert_eq!(document.children(content).len(), 200);
        // 200 rows of two nodes plus the container, plus one slice of slack.
        assert!(
            document.capacity() <= 1 + 2 * 250,
            "{}",
            document.capacity()
        );
    }

    #[test]
    fn a_shadow_root_is_parented_to_its_host_but_is_not_a_child() {
        let document = Document::new();
        let host = document.create_element("diffs-container");
        let root = document.attach_shadow(host);
        let pre = document.create_element("pre");
        document.append_child(root, pre);
        assert_eq!(document.shadow_root(host), Some(root));
        assert!(document.children(host).is_empty());
        assert_eq!(document.parent(root), Some(host));
        assert!(document.is_attached(pre, host));
        assert_eq!(document.children(root), vec![pre]);
        document.discard(host);
        assert!(!document.exists(root));
        assert!(!document.exists(pre));
    }

    #[test]
    #[should_panic(expected = "stale NodeKey")]
    fn a_stale_key_panics_instead_of_naming_the_slot_reuser() {
        let document = Document::new();
        let first = document.create_element("div");
        document.discard(first);
        let reuser = document.create_element("span");
        assert!(!document.exists(first));
        assert!(document.exists(reuser));
        assert_eq!(document.capacity(), 1);
        document.tag_name(first);
    }

    #[test]
    fn remove_detaches_but_keeps_the_subtree_usable() {
        let document = Document::new();
        let parent = document.create_element("div");
        let child = document.create_element("p");
        document.append_child(parent, child);
        document.remove(child);
        assert!(document.exists(child));
        assert_eq!(document.parent(child), None);
        assert!(!document.is_attached(child, parent));
        document.append_child(parent, child);
        assert!(document.is_attached(child, parent));
    }
}
