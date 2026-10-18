//! Engine-neutral presentation wire, `WriteMutations` replay, and retained surface tree.
//!
//! The WIT `presentation` contract ships ordered batches of [`Mutation`] operations that mirror
//! `dioxus_core::WriteMutations` one-to-one. A renderer backend is any `WriteMutations`
//! implementation plus event translation:
//! - Direct Blitz documents replay batches into `dioxus_native_dom::DioxusState::writer`.
//! - Host-composed surfaces replay batches into [`SurfaceTree`], which renders into the host
//!   Dioxus `VirtualDom` through [`crate::native::ComponentMount`] without `ElementId` collisions
//!   and withdraws its subtree when its [`crate::host::Mount`] or [`crate::host::Instance`] closes.

use dioxus::prelude::*;
use dioxus_core::{BorrowedAttributeValue, ElementId, ListenerCallback, WriteMutations};
use dioxus_html::PlatformEventData;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    rc::Rc,
    sync::{LazyLock, Mutex},
};

pub type NodeId = u32;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PresentationError {
    #[error("presentation surface or mount point has been revoked")]
    Revoked,
    #[error("unsupported presentation operation: {0}")]
    Unsupported(String),
    #[error("invalid presentation mutation batch: {0}")]
    InvalidBatch(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    Text(String),
    Float(f64),
    Int(i64),
    Boolean(bool),
    None,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mutation {
    PushId(NodeId),
    SetId(NodeId),
    Child(u32),
    Pop,
    CreateElement((String, Option<String>)),
    CreateText(String),
    CloneNode,
    AppendChildren(u32),
    ReplaceWith(u32),
    InsertAfter(u32),
    InsertBefore(u32),
    SetAttribute((String, Option<String>, AttributeValue)),
    SetText(String),
    AddEventListener(String),
    RemoveEventListener(String),
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub revision: u64,
    pub element: NodeId,
    pub name: String,
    pub bubbles: bool,
    pub data_json: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventResponse {
    pub prevent_default: bool,
    pub stop_propagation: bool,
}

pub type SurfaceEventHandler = Rc<dyn Fn(Event) -> EventResponse>;

/// Replays one wire batch into any `dioxus_core::WriteMutations` writer in order.
pub fn replay(batch: &[Mutation], to: &mut impl WriteMutations) {
    let element = |id: NodeId| ElementId::from_raw(id as usize);
    for mutation in batch {
        match mutation {
            Mutation::PushId(id) => to.push_id(element(*id)),
            Mutation::SetId(id) => to.set_id(element(*id)),
            Mutation::Child(index) => to.child(*index as usize),
            Mutation::Pop => to.pop(),
            Mutation::CreateElement((tag, ns)) => to.create_element(tag, ns.as_deref()),
            Mutation::CreateText(value) => to.create_text(value),
            Mutation::CloneNode => to.clone(),
            Mutation::AppendChildren(m) => to.append_children(*m as usize),
            Mutation::ReplaceWith(m) => to.replace_with(*m as usize),
            Mutation::InsertAfter(m) => to.insert_after(*m as usize),
            Mutation::InsertBefore(m) => to.insert_before(*m as usize),
            Mutation::SetAttribute((name, ns, value)) => {
                let value = match value {
                    AttributeValue::Text(text) => BorrowedAttributeValue::Text(text),
                    AttributeValue::Float(float) => BorrowedAttributeValue::Float(*float),
                    AttributeValue::Int(int) => BorrowedAttributeValue::Int(*int),
                    AttributeValue::Boolean(bool) => BorrowedAttributeValue::Bool(*bool),
                    AttributeValue::None => BorrowedAttributeValue::None,
                };
                to.set_attribute(name, ns.as_deref(), value);
            }
            Mutation::SetText(value) => to.set_text(value),
            Mutation::AddEventListener(name) => to.add_event_listener(name),
            Mutation::RemoveEventListener(name) => to.remove_event_listener(name),
            Mutation::Remove => to.remove(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SurfaceNodeId(usize);

impl SurfaceNodeId {
    const ROOT: Self = Self(0);
}

#[derive(Clone, Copy, Debug)]
struct StackEntry {
    node: SurfaceNodeId,
    element_id: Option<ElementId>,
}

#[derive(Clone, Debug, PartialEq)]
enum SurfaceNodeKind {
    Root,
    Element {
        tag: String,
        namespace: Option<String>,
        attributes: BTreeMap<(String, Option<String>), AttributeValue>,
        listeners: BTreeSet<String>,
        element_id: Option<ElementId>,
    },
    Text(String),
}

#[derive(Clone, Debug, PartialEq)]
struct SurfaceNode {
    kind: SurfaceNodeKind,
    parent: Option<SurfaceNodeId>,
    children: Vec<SurfaceNodeId>,
}

/// Retained logical tree for one mounted surface.
///
/// Replaying wire batches into [`SurfaceTree`] executes the `WriteMutations` stack machine against
/// an arena of surface-local nodes so that [`SurfaceTree::render`] can project the subtree into a
/// host `VirtualDom` while translating host events back to the guest's surface-local `ElementId`s.
#[derive(Clone, Debug)]
pub struct SurfaceTree {
    nodes: Vec<Option<SurfaceNode>>,
    stack: Vec<StackEntry>,
    element_to_node: Vec<Option<SurfaceNodeId>>,
    revision: u64,
}

impl Default for SurfaceTree {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for SurfaceTree {
    fn eq(&self, other: &Self) -> bool {
        self.revision == other.revision && self.nodes == other.nodes
    }
}

impl SurfaceTree {
    pub fn new() -> Self {
        Self {
            nodes: vec![Some(SurfaceNode {
                kind: SurfaceNodeKind::Root,
                parent: None,
                children: Vec::new(),
            })],
            stack: vec![StackEntry {
                node: SurfaceNodeId::ROOT,
                element_id: Some(ElementId::ROOT),
            }],
            element_to_node: vec![Some(SurfaceNodeId::ROOT)],
            revision: 0,
        }
    }

    pub fn apply(&mut self, batch: &[Mutation]) -> u64 {
        replay(batch, self);
        self.revision += 1;
        self.revision
    }

    pub fn clear(&mut self) {
        let revision = self.revision + 1;
        *self = Self::new();
        self.revision = revision;
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn is_empty(&self) -> bool {
        self.node(SurfaceNodeId::ROOT).children.is_empty()
    }

    pub fn text_content(&self) -> String {
        let mut out = String::new();
        self.collect_text(SurfaceNodeId::ROOT, &mut out);
        out
    }

    fn collect_text(&self, id: SurfaceNodeId, out: &mut String) {
        let node = self.node(id);
        if let SurfaceNodeKind::Text(text) = &node.kind {
            out.push_str(text);
        }
        for &child in &node.children {
            self.collect_text(child, out);
        }
    }

    fn alloc_node(&mut self, kind: SurfaceNodeKind) -> SurfaceNodeId {
        let id = SurfaceNodeId(self.nodes.len());
        self.nodes.push(Some(SurfaceNode {
            kind,
            parent: None,
            children: Vec::new(),
        }));
        id
    }

    fn node(&self, id: SurfaceNodeId) -> &SurfaceNode {
        self.nodes
            .get(id.0)
            .and_then(Option::as_ref)
            .unwrap_or_else(|| panic!("surface node {} does not exist", id.0))
    }

    fn node_mut(&mut self, id: SurfaceNodeId) -> &mut SurfaceNode {
        self.nodes
            .get_mut(id.0)
            .and_then(Option::as_mut)
            .unwrap_or_else(|| panic!("surface node {} does not exist", id.0))
    }

    fn lookup(&self, id: ElementId) -> SurfaceNodeId {
        self.element_to_node
            .get(id.raw())
            .copied()
            .flatten()
            .unwrap_or_else(|| panic!("surface asked for unknown ElementId {}", id.raw()))
    }

    fn set_mapping(&mut self, id: ElementId, node: SurfaceNodeId) {
        let index = id.raw();
        if self.element_to_node.len() <= index {
            self.element_to_node.resize(index + 1, None);
        }
        self.element_to_node[index] = Some(node);
        if let SurfaceNodeKind::Element { element_id, .. } = &mut self.node_mut(node).kind {
            *element_id = Some(id);
        }
    }

    fn top(&self) -> StackEntry {
        *self
            .stack
            .last()
            .expect("surface stack unexpectedly empty")
    }

    fn pop_entry(&mut self) -> StackEntry {
        self.stack
            .pop()
            .expect("surface stack unexpectedly empty")
    }

    fn pop_nodes(&mut self, m: usize) -> Vec<SurfaceNodeId> {
        let split = self.stack.len() - m;
        self.stack
            .split_off(split)
            .into_iter()
            .map(|entry| entry.node)
            .collect()
    }

    fn detach_from_parent(&mut self, child: SurfaceNodeId) {
        if let Some(parent) = self.node_mut(child).parent.take()
            && let Some(parent_node) = self.nodes.get_mut(parent.0).and_then(Option::as_mut)
        {
            parent_node.children.retain(|&existing| existing != child);
        }
    }

    fn drop_subtree(&mut self, root: SurfaceNodeId) {
        let Some(node) = self.nodes.get_mut(root.0).and_then(Option::take) else {
            return;
        };
        if let SurfaceNodeKind::Element {
            element_id: Some(id),
            ..
        } = node.kind
            && let Some(slot) = self.element_to_node.get_mut(id.raw())
            && *slot == Some(root)
        {
            *slot = None;
        }
        for child in node.children {
            self.drop_subtree(child);
        }
    }

    fn deep_clone(&mut self, source: SurfaceNodeId) -> SurfaceNodeId {
        let (kind, children) = {
            let node = self.node(source);
            let kind = match &node.kind {
                SurfaceNodeKind::Root => panic!("cannot clone surface root"),
                SurfaceNodeKind::Element {
                    tag,
                    namespace,
                    attributes,
                    ..
                } => SurfaceNodeKind::Element {
                    tag: tag.clone(),
                    namespace: namespace.clone(),
                    attributes: attributes.clone(),
                    listeners: BTreeSet::new(),
                    element_id: None,
                },
                SurfaceNodeKind::Text(text) => SurfaceNodeKind::Text(text.clone()),
            };
            (kind, node.children.clone())
        };
        let cloned = self.alloc_node(kind);
        let cloned_children: Vec<SurfaceNodeId> =
            children.into_iter().map(|c| self.deep_clone(c)).collect();
        for &child in &cloned_children {
            self.node_mut(child).parent = Some(cloned);
        }
        self.node_mut(cloned).children = cloned_children;
        cloned
    }

    pub fn render(&self, dispatch: &SurfaceEventHandler) -> Element {
        let root_children = &self.node(SurfaceNodeId::ROOT).children;
        rsx! {
            for &child in root_children {
                {self.render_node(child, dispatch)}
            }
        }
    }

    fn render_node(&self, id: SurfaceNodeId, dispatch: &SurfaceEventHandler) -> Element {
        let node = self.node(id);
        match &node.kind {
            SurfaceNodeKind::Root => unreachable!("root is never a child node"),
            SurfaceNodeKind::Text(text) => rsx! { "{text}" },
            SurfaceNodeKind::Element {
                tag,
                namespace,
                attributes,
                listeners,
                element_id,
            } => {
                let mut attrs = Vec::with_capacity(attributes.len() + listeners.len());
                for ((name, ns), value) in attributes {
                    let attr_name = intern_str(name);
                    let attr_ns = ns.as_deref().map(intern_str);
                    let volatile = attr_name == "value" || attr_name == "checked";
                    let attr_value = match value {
                        AttributeValue::Text(text) => dioxus_core::AttributeValue::Text(text.clone()),
                        AttributeValue::Float(float) => dioxus_core::AttributeValue::Float(*float),
                        AttributeValue::Int(int) => dioxus_core::AttributeValue::Int(*int),
                        AttributeValue::Boolean(bool) => dioxus_core::AttributeValue::Bool(*bool),
                        AttributeValue::None => dioxus_core::AttributeValue::None,
                    };
                    attrs.push(dioxus_core::Attribute::new(
                        attr_name, attr_value, attr_ns, volatile,
                    ));
                }
                if let Some(guest_element) = *element_id {
                    let guest_node_id =
                        NodeId::try_from(guest_element.raw()).expect("element id fits NodeId");
                    for listener_name in listeners {
                        if let Some(attr) = surface_listener_attribute(
                            listener_name,
                            self.revision,
                            guest_node_id,
                            dispatch.clone(),
                        ) {
                            attrs.push(attr);
                        }
                    }
                }
                let children = node.children.iter().map(|&child| self.render_node(child, dispatch));
                let key = id.0;
                render_element_tag(tag, namespace.as_deref(), key, attrs, children)
            }
        }
    }
}

impl WriteMutations for SurfaceTree {
    fn push_id(&mut self, id: ElementId) {
        let node = self.lookup(id);
        self.stack.push(StackEntry {
            node,
            element_id: Some(id),
        });
    }

    fn set_id(&mut self, id: ElementId) {
        let node = self.top().node;
        self.set_mapping(id, node);
        if let Some(top) = self.stack.last_mut() {
            top.element_id = Some(id);
        }
    }

    fn child(&mut self, index: usize) {
        let parent = self.top().node;
        let child = self.node(parent).children[index];
        let element_id = match &self.node(child).kind {
            SurfaceNodeKind::Element { element_id, .. } => *element_id,
            _ => None,
        };
        *self.stack.last_mut().expect("surface stack unexpectedly empty") = StackEntry {
            node: child,
            element_id,
        };
    }

    fn pop(&mut self) {
        self.pop_entry();
    }

    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        let node = self.alloc_node(SurfaceNodeKind::Element {
            tag: tag.to_owned(),
            namespace: ns.map(str::to_owned),
            attributes: BTreeMap::new(),
            listeners: BTreeSet::new(),
            element_id: None,
        });
        self.stack.push(StackEntry {
            node,
            element_id: None,
        });
    }

    fn create_text(&mut self, value: &str) {
        let node = self.alloc_node(SurfaceNodeKind::Text(value.to_owned()));
        self.stack.push(StackEntry {
            node,
            element_id: None,
        });
    }

    fn clone(&mut self) {
        let source = self.top().node;
        let cloned = self.deep_clone(source);
        *self.stack.last_mut().expect("surface stack unexpectedly empty") = StackEntry {
            node: cloned,
            element_id: None,
        };
    }

    fn append_children(&mut self, m: usize) {
        let children = self.pop_nodes(m);
        let parent = self.top().node;
        for &child in &children {
            self.detach_from_parent(child);
            self.node_mut(child).parent = Some(parent);
        }
        self.node_mut(parent).children.extend(children);
    }

    fn replace_with(&mut self, m: usize) {
        let replacements = self.pop_nodes(m);
        let target = self.pop_entry();
        for &node in &replacements {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(target.node)
            .parent
            .expect("replaced surface node must have a parent");
        for &node in &replacements {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == target.node)
            .expect("target node must be in parent's children");
        siblings.splice(pos..=pos, replacements);
        self.drop_subtree(target.node);
    }

    fn insert_after(&mut self, m: usize) {
        let nodes = self.pop_nodes(m);
        let anchor = self.top().node;
        for &node in &nodes {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(anchor)
            .parent
            .expect("anchor surface node must have a parent");
        for &node in &nodes {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == anchor)
            .expect("anchor node must be in parent's children");
        siblings.splice(pos + 1..pos + 1, nodes);
    }

    fn insert_before(&mut self, m: usize) {
        let nodes = self.pop_nodes(m);
        let anchor = self.top().node;
        for &node in &nodes {
            self.detach_from_parent(node);
        }
        let parent = self
            .node(anchor)
            .parent
            .expect("anchor surface node must have a parent");
        for &node in &nodes {
            self.node_mut(node).parent = Some(parent);
        }
        let siblings = &mut self.node_mut(parent).children;
        let pos = siblings
            .iter()
            .position(|&existing| existing == anchor)
            .expect("anchor node must be in parent's children");
        siblings.splice(pos..pos, nodes);
    }

    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        let node = self.top().node;
        let SurfaceNodeKind::Element { attributes, .. } = &mut self.node_mut(node).kind else {
            panic!("set_attribute called on non-element surface node");
        };
        let key = (name.to_owned(), ns.map(str::to_owned));
        match value {
            BorrowedAttributeValue::None | BorrowedAttributeValue::Bool(false) => {
                attributes.remove(&key);
            }
            BorrowedAttributeValue::Text(text) => {
                attributes.insert(key, AttributeValue::Text(text.to_owned()));
            }
            BorrowedAttributeValue::Float(float) => {
                attributes.insert(key, AttributeValue::Float(float));
            }
            BorrowedAttributeValue::Int(int) => {
                attributes.insert(key, AttributeValue::Int(int));
            }
            BorrowedAttributeValue::Bool(true) => {
                attributes.insert(key, AttributeValue::Boolean(true));
            }
            BorrowedAttributeValue::Any(_) => {
                panic!("`Any` attribute `{name}` cannot cross the presentation boundary")
            }
        }
    }

    fn set_text(&mut self, value: &str) {
        let node = self.top().node;
        match &mut self.node_mut(node).kind {
            SurfaceNodeKind::Text(text) => {
                *text = value.to_owned();
            }
            SurfaceNodeKind::Element { .. } => {
                let children = self.node(node).children.clone();
                for child in children {
                    self.drop_subtree(child);
                }
                let text_node = self.alloc_node(SurfaceNodeKind::Text(value.to_owned()));
                self.node_mut(text_node).parent = Some(node);
                self.node_mut(node).children = vec![text_node];
            }
            SurfaceNodeKind::Root => panic!("set_text called on surface root"),
        }
    }

    fn add_event_listener(&mut self, name: &str) {
        let entry = self.top();
        let element_id = entry
            .element_id
            .or_else(|| match &self.node(entry.node).kind {
                SurfaceNodeKind::Element { element_id, .. } => *element_id,
                _ => None,
            })
            .expect("event listener target must be mapped to an ElementId");
        self.set_mapping(element_id, entry.node);
        let SurfaceNodeKind::Element { listeners, .. } = &mut self.node_mut(entry.node).kind else {
            panic!("add_event_listener called on non-element surface node");
        };
        listeners.insert(name.to_owned());
    }

    fn remove_event_listener(&mut self, name: &str) {
        let node = self.top().node;
        if let SurfaceNodeKind::Element { listeners, .. } = &mut self.node_mut(node).kind {
            listeners.remove(name);
        }
    }

    fn remove(&mut self) {
        let entry = self.pop_entry();
        self.detach_from_parent(entry.node);
        self.drop_subtree(entry.node);
    }
}

fn intern_str(value: &str) -> &'static str {
    static INTERNED: LazyLock<Mutex<HashSet<&'static str>>> =
        LazyLock::new(|| Mutex::new(HashSet::new()));
    let mut set = INTERNED.lock().unwrap();
    if let Some(&existing) = set.get(value) {
        return existing;
    }
    let leaked: &'static str = Box::leak(value.to_owned().into_boxed_str());
    set.insert(leaked);
    leaked
}

macro_rules! expand_surface_event_listener {
    (
        enum Event {
            $(
                #[convert = $converter:ident]
                #[events = [
                    $(
                        $( #[$attr:meta] )*
                        $name:ident => $raw:ident,
                    )*
                ]]
                $(#[raw = [$($raw_only:ident),* $(,)?]])?
                $group:ident($data:ident),
            )*
        }
    ) => {
        fn surface_listener_attribute(
            raw_name: &str,
            revision: u64,
            element: NodeId,
            dispatch: SurfaceEventHandler,
        ) -> Option<dioxus_core::Attribute> {
            match raw_name {
                $(
                    $(
                        stringify!($raw) => {
                            let event_name = stringify!($raw);
                            let listener = ListenerCallback::new(
                                move |evt: dioxus_core::Event<PlatformEventData>| {
                                    let platform_rc = evt.data();
                                    let data_json = expand_surface_event_listener!(
                                        @serialize $group, $data, platform_rc.as_ref()
                                    );
                                    let response = dispatch(Event {
                                        revision,
                                        element,
                                        name: event_name.to_owned(),
                                        bubbles: evt.propagates(),
                                        data_json,
                                    });
                                    if response.prevent_default {
                                        evt.prevent_default();
                                    }
                                    if response.stop_propagation {
                                        evt.stop_propagation();
                                    }
                                },
                            );
                            return Some(dioxus_core::Attribute::new(
                                stringify!($name),
                                dioxus_core::AttributeValue::Listener(listener.erase()),
                                None,
                                false,
                            ));
                        }
                    )*
                )*
                _ => None,
            }
        }
    };
    (@serialize Mounted, $data:ident, $platform:expr) => {{
        let _ = $platform;
        "null".to_owned()
    }};
    (@serialize $group:ident, $data:ident, $platform:expr) => {{
        pastey::paste! {
            let platform: &PlatformEventData = $platform;
            if let Some(serialized) = platform.downcast::<dioxus_html::[<Serialized $data>]>() {
                serde_json::to_string(serialized).expect("event data serializes to JSON")
            } else {
                let converted = dioxus_html::$data::from(platform);
                serde_json::to_string(&converted).expect("event data serializes to JSON")
            }
        }
    }};
}

dioxus_html::with_html_event_groups!(expand_surface_event_listener);

fn render_element_tag(
    tag: &str,
    _namespace: Option<&str>,
    key: usize,
    attrs: Vec<dioxus_core::Attribute>,
    children: impl Iterator<Item = Element>,
) -> Element {
    macro_rules! match_tags {
        ($( $tag_ident:ident ),* $(,)?) => {
            match tag {
                $(
                    stringify!($tag_ident) => rsx! {
                        $tag_ident {
                            key: "{key}",
                            ..attrs,
                            {children}
                        }
                    },
                )*
                _ => rsx! {
                    div {
                        key: "{key}",
                        ..attrs,
                        {children}
                    }
                },
            }
        };
    }
    match_tags!(
        div, section, article, aside, header, footer, main, nav, h1, h2, h3, h4, h5, h6, p, span,
        pre, code, blockquote, strong, em, small, b, i, u, s, mark, kbd, sub, sup, br, hr, ul, ol,
        li, dl, dt, dd, table, thead, tbody, tfoot, tr, th, td, caption, form, label, input,
        textarea, button, select, option, optgroup, fieldset, legend, details, summary, dialog, a,
        img, figure, figcaption, progress, meter, output, time, data, svg, g, path, circle, rect,
        line, polyline, polygon, defs
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        host::Host,
        native::{ComponentMount, NativeComponents, SurfaceSession},
    };
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedMouseData};
    use std::cell::Cell;

    fn counter_initial_batch() -> Vec<Mutation> {
        vec![
            Mutation::PushId(0),
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetAttribute(("class".into(), None, AttributeValue::Text("btn".into()))),
            Mutation::CreateText("0".into()),
            Mutation::AppendChildren(1),
            Mutation::SetId(1),
            Mutation::AddEventListener("click".into()),
            Mutation::PushId(1),
            Mutation::Child(0),
            Mutation::SetId(2),
            Mutation::Pop,
            Mutation::AppendChildren(1),
            Mutation::Pop,
        ]
    }

    fn counter_text_update_batch(value: &str) -> Vec<Mutation> {
        vec![
            Mutation::PushId(2),
            Mutation::SetText(value.into()),
            Mutation::Pop,
        ]
    }

    #[test]
    fn surface_tree_replays_batches_and_clears_on_withdrawal() {
        let mut tree = SurfaceTree::new();
        assert!(tree.is_empty());
        assert_eq!(tree.apply(&counter_initial_batch()), 1);
        assert!(!tree.is_empty());
        assert_eq!(tree.text_content(), "0");

        assert_eq!(tree.apply(&counter_text_update_batch("1")), 2);
        assert_eq!(tree.text_content(), "1");

        tree.clear();
        assert!(tree.is_empty());
        assert_eq!(tree.text_content(), "");
        assert_eq!(tree.revision(), 3);
    }

    #[test]
    fn surface_mount_renders_in_host_vdom_dispatches_reentrantly_and_revokes_on_instance_close() {
        let host = Host::new();
        let components = NativeComponents::new(&host);
        let viewer = host.start("viewer");
        let micro = host.start("micro");

        let dropped = Rc::new(Cell::new(false));
        let dropped_flag = dropped.clone();
        let writer_slot: Rc< std::cell::RefCell<Option<crate::native::SurfaceWriter>> > =
            Rc::default();
        let writer_slot_capture = writer_slot.clone();

        let reference = components
            .publish_surface(&micro, move |_mount, writer| {
                writer.apply(&counter_initial_batch())?;
                *writer_slot_capture.borrow_mut() = Some(writer.clone());
                let clicks = Cell::new(0_u64);
                let flag = dropped_flag.clone();
                Ok(SurfaceSession::with_drop(
                    move |event| {
                        assert_eq!(event.name, "click");
                        assert_eq!(event.element, 1);
                        let next = clicks.get() + 1;
                        clicks.set(next);
                        // Re-entrant apply inside event dispatch must not conflict with SurfaceTree borrow.
                        writer
                            .apply(&counter_text_update_batch(&next.to_string()))
                            .unwrap();
                        EventResponse::default()
                    },
                    move || flag.set(true),
                ))
            })
            .unwrap();

        #[derive(Clone, PartialEq)]
        struct HostProps {
            components: NativeComponents,
            viewer: crate::host::Instance,
            reference: crate::host::ComponentRef,
        }

        fn host_app(props: HostProps) -> Element {
            rsx! {
                section {
                    ComponentMount {
                        components: props.components,
                        owner: props.viewer,
                        reference: props.reference,
                    }
                }
            }
        }

        let mut host_dom = VirtualDom::new_with_props(
            host_app,
            HostProps {
                components,
                viewer,
                reference,
            },
        );
        let mut host_tree = SurfaceTree::new();
        host_dom.rebuild(&mut host_tree);
        assert_eq!(host_tree.text_content(), "0");

        // Find the host VirtualDom's ElementId for the mounted `<button>` (carrying the click listener,
        // not the detached template prototype).
        let host_button_id = host_tree
            .nodes
            .iter()
            .flatten()
            .find_map(|node| match &node.kind {
                SurfaceNodeKind::Element {
                    tag,
                    listeners,
                    element_id,
                    ..
                } if tag == "button" && listeners.contains("click") => *element_id,
                _ => None,
            })
            .expect("host tree rendered the surface button");

        let click = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        host_dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            click,
            host_button_id,
        );
        host_dom.render_immediate(&mut host_tree);
        assert_eq!(host_tree.text_content(), "1");

        // Closing the micro's Instance revokes the surface writer immediately.
        micro.close();
        let writer = writer_slot.borrow().clone().unwrap();
        assert_eq!(
            writer.apply(&counter_text_update_batch("2")),
            Err(PresentationError::Revoked)
        );
        drop(host_dom);
        assert!(dropped.get());
    }
}
