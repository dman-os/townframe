//! The behaviour-test harness every kit component copies (`KIT-070`).
//!
//! A real `VirtualDom` renders into a small retained DOM ([`Page`]) that implements
//! `WriteMutations` and nothing else, so a test exercises exactly what a renderer would: the
//! mutation stream, attributes, listeners and event dispatch back through the runtime. No
//! renderer crate is involved, which keeps the kit's dependency rule (Dioxus only) intact and
//! makes the tests run identically for every target the kit builds for.
//!
//! Tests find elements the way an assistive technology does: [`Page::by_role`] matches an ARIA
//! role (explicit or implied by the tag) and an accessible name, and skips `aria-hidden`
//! subtrees. A test that has to reach for a tag or a class instead has found a missing role or
//! name, which is a component bug.
use dioxus::prelude::*;
use dioxus_core::BorrowedAttributeValue;
use dioxus_core::ElementId;
use dioxus_core::Event;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_core::WriteMutations;
use dioxus_html::Code;
use dioxus_html::EventData;
use dioxus_html::Key;
use dioxus_html::Location;
use dioxus_html::Modifiers;
use dioxus_html::SerializedKeyboardData;
use dioxus_html::SerializedMouseData;
use dioxus_html::SerializedPointInteraction;
use dioxus_html::SerializedPointerData;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// A node of the retained DOM, by index into [`Dom::nodes`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Node(usize);

enum Kind {
    Root,
    Element {
        tag: String,
        /// Plain attributes under their name, namespaced ones (`style`) as `ns:name`.
        attrs: BTreeMap<String, String>,
        listeners: BTreeSet<String>,
    },
    Text(String),
}

struct Entry {
    kind: Kind,
    parent: Option<usize>,
    children: Vec<usize>,
    element_id: Option<ElementId>,
}

/// The retained DOM that the `VirtualDom` writes into.
struct Dom {
    nodes: Vec<Entry>,
    by_element_id: BTreeMap<usize, usize>,
    stack: Vec<usize>,
}

impl Dom {
    fn new() -> Self {
        let root = Entry {
            kind: Kind::Root,
            parent: None,
            children: vec![],
            element_id: Some(ElementId::ROOT),
        };
        Self {
            nodes: vec![root],
            by_element_id: BTreeMap::from([(ElementId::ROOT.raw(), 0)]),
            stack: vec![0],
        }
    }

    fn alloc(&mut self, kind: Kind) -> usize {
        self.nodes.push(Entry {
            kind,
            parent: None,
            children: vec![],
            element_id: None,
        });
        self.nodes.len() - 1
    }

    fn top(&self) -> usize {
        *self.stack.last().expect("mutation stack is never empty")
    }

    fn pop_many(&mut self, count: usize) -> Vec<usize> {
        let at = self.stack.len() - count;
        self.stack.split_off(at)
    }

    fn detach(&mut self, node: usize) {
        if let Some(parent) = self.nodes[node].parent.take() {
            self.nodes[parent].children.retain(|child| *child != node);
        }
    }

    /// Insert `nodes` into the parent of `anchor` at `anchor`'s index plus `offset`.
    fn insert_around(&mut self, anchor: usize, nodes: Vec<usize>, offset: usize) {
        for node in &nodes {
            self.detach(*node);
        }
        let parent = self.nodes[anchor].parent.expect("anchor has a parent");
        let at = self.nodes[parent]
            .children
            .iter()
            .position(|child| *child == anchor)
            .expect("anchor is its parent's child")
            + offset;
        for node in &nodes {
            self.nodes[*node].parent = Some(parent);
        }
        self.nodes[parent].children.splice(at..at, nodes);
    }

    fn deep_clone(&mut self, source: usize) -> usize {
        let kind = match &self.nodes[source].kind {
            Kind::Root => panic!("the root cannot be cloned"),
            Kind::Element { tag, attrs, .. } => Kind::Element {
                tag: tag.clone(),
                attrs: attrs.clone(),
                listeners: BTreeSet::new(),
            },
            Kind::Text(text) => Kind::Text(text.clone()),
        };
        let copy = self.alloc(kind);
        for child in self.nodes[source].children.clone() {
            let child_copy = self.deep_clone(child);
            self.nodes[child_copy].parent = Some(copy);
            self.nodes[copy].children.push(child_copy);
        }
        copy
    }
}

impl WriteMutations for Dom {
    fn push_id(&mut self, id: ElementId) {
        self.stack.push(self.by_element_id[&id.raw()]);
    }

    fn set_id(&mut self, id: ElementId) {
        let top = self.top();
        self.nodes[top].element_id = Some(id);
        self.by_element_id.insert(id.raw(), top);
    }

    fn child(&mut self, index: usize) {
        let top = self.top();
        *self.stack.last_mut().unwrap() = self.nodes[top].children[index];
    }

    fn pop(&mut self) {
        self.stack.pop();
    }

    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        assert!(ns.is_none() || ns == Some("http://www.w3.org/2000/svg"));
        let node = self.alloc(Kind::Element {
            tag: tag.to_owned(),
            attrs: BTreeMap::new(),
            listeners: BTreeSet::new(),
        });
        self.stack.push(node);
    }

    fn create_text(&mut self, value: &str) {
        let node = self.alloc(Kind::Text(value.to_owned()));
        self.stack.push(node);
    }

    fn clone(&mut self) {
        let copy = self.deep_clone(self.top());
        *self.stack.last_mut().unwrap() = copy;
    }

    fn append_children(&mut self, m: usize) {
        let children = self.pop_many(m);
        let parent = self.top();
        for child in &children {
            self.detach(*child);
            self.nodes[*child].parent = Some(parent);
        }
        self.nodes[parent].children.extend(children);
    }

    fn replace_with(&mut self, m: usize) {
        let replacements = self.pop_many(m);
        let target = self.stack.pop().expect("a replace target");
        self.insert_around(target, replacements, 0);
        self.detach(target);
    }

    fn insert_after(&mut self, m: usize) {
        let nodes = self.pop_many(m);
        let anchor = self.top();
        self.insert_around(anchor, nodes, 1);
    }

    fn insert_before(&mut self, m: usize) {
        let nodes = self.pop_many(m);
        let anchor = self.top();
        self.insert_around(anchor, nodes, 0);
    }

    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        let top = self.top();
        let Kind::Element { attrs, .. } = &mut self.nodes[top].kind else {
            panic!("set_attribute on a non-element");
        };
        let key = ns.map_or_else(|| name.to_owned(), |ns| format!("{ns}:{name}"));
        match value {
            BorrowedAttributeValue::None | BorrowedAttributeValue::Bool(false) => {
                attrs.remove(&key);
            }
            BorrowedAttributeValue::Text(text) => {
                attrs.insert(key, text.to_owned());
            }
            BorrowedAttributeValue::Float(float) => {
                attrs.insert(key, float.to_string());
            }
            BorrowedAttributeValue::Int(int) => {
                attrs.insert(key, int.to_string());
            }
            BorrowedAttributeValue::Bool(true) => {
                attrs.insert(key, "true".to_owned());
            }
            BorrowedAttributeValue::Any(_) => panic!("`Any` attribute `{name}` has no DOM form"),
        }
    }

    fn set_text(&mut self, value: &str) {
        let top = self.top();
        match &mut self.nodes[top].kind {
            Kind::Text(text) => *text = value.to_owned(),
            _ => {
                let text = self.alloc(Kind::Text(value.to_owned()));
                self.nodes[text].parent = Some(top);
                self.nodes[top].children = vec![text];
            }
        }
    }

    fn add_event_listener(&mut self, name: &str) {
        let top = self.top();
        let Kind::Element { listeners, .. } = &mut self.nodes[top].kind else {
            panic!("event listener on a non-element");
        };
        listeners.insert(name.to_owned());
    }

    fn remove_event_listener(&mut self, name: &str) {
        let top = self.top();
        if let Kind::Element { listeners, .. } = &mut self.nodes[top].kind {
            listeners.remove(name);
        }
    }

    fn remove(&mut self) {
        let node = self.stack.pop().expect("a node to remove");
        self.detach(node);
    }
}

/// A rendered component and the DOM it wrote.
pub(crate) struct Page {
    vdom: VirtualDom,
    dom: Dom,
}

impl Page {
    /// Render `app` with root props (a test's recorder, usually an `Rc<RefCell<Vec<_>>>`).
    pub(crate) fn with_props<P: Clone + 'static, M: 'static>(
        app: impl dioxus_core::ComponentFunction<P, M>,
        props: P,
    ) -> Self {
        Self::from_vdom(VirtualDom::new_with_props(app, props))
    }

    fn from_vdom(mut vdom: VirtualDom) -> Self {
        // Event payloads are the serialized forms; without a converter `event.data()` panics.
        dioxus_html::set_event_converter(Box::new(dioxus_html::SerializedHtmlEventConverter));
        let mut dom = Dom::new();
        vdom.rebuild(&mut dom);
        Self { vdom, dom }
    }

    /// Flush pending state changes into the DOM, as the renderer's event loop would.
    fn settle(&mut self) {
        self.vdom.render_immediate(&mut self.dom);
    }

    fn element_id(&self, node: Node) -> ElementId {
        self.dom.nodes[node.0]
            .element_id
            .unwrap_or_else(|| panic!("{} has no listener, so no ElementId", self.describe(node)))
    }

    fn describe(&self, node: Node) -> String {
        match &self.dom.nodes[node.0].kind {
            Kind::Element { tag, attrs, .. } => format!("<{tag} {attrs:?}>"),
            Kind::Text(text) => format!("text {text:?}"),
            Kind::Root => "root".into(),
        }
    }

    fn walk(&self, node: usize, accessible_only: bool, out: &mut Vec<Node>) {
        if let Kind::Element { attrs, .. } = &self.dom.nodes[node].kind {
            if accessible_only
                && (attrs.get("aria-hidden").is_some_and(|v| v == "true")
                    || attrs.contains_key("hidden"))
            {
                return;
            }
            out.push(Node(node));
        }
        for child in &self.dom.nodes[node].children {
            self.walk(*child, accessible_only, out);
        }
    }

    /// The text of a subtree.
    pub(crate) fn text(&self, node: Node) -> String {
        fn collect(page: &Page, node: usize, out: &mut String) {
            if let Kind::Text(text) = &page.dom.nodes[node].kind {
                out.push_str(text);
            }
            for child in &page.dom.nodes[node].children {
                collect(page, *child, out);
            }
        }
        let mut out = String::new();
        collect(self, node.0, &mut out);
        out
    }

    /// An attribute (namespaced ones are keyed `ns:name`).
    pub(crate) fn attr(&self, node: Node, name: &str) -> Option<String> {
        match &self.dom.nodes[node.0].kind {
            Kind::Element { attrs, .. } => attrs.get(name).cloned(),
            _ => None,
        }
    }

    /// A property of the inline `style` attribute.
    pub(crate) fn style(&self, node: Node, property: &str) -> Option<String> {
        self.attr(node, "style")?
            .split(';')
            .find_map(|declaration| {
                let (name, value) = declaration.split_once(':')?;
                (name.trim() == property).then(|| value.trim().to_owned())
            })
    }

    fn role_of(&self, node: Node) -> Option<String> {
        let Kind::Element { tag, attrs, .. } = &self.dom.nodes[node.0].kind else {
            return None;
        };
        if let Some(role) = attrs.get("role") {
            return Some(role.clone());
        }
        match tag.as_str() {
            "button" => Some("button"),
            "a" if attrs.contains_key("href") => Some("link"),
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => Some("heading"),
            "nav" => Some("navigation"),
            _ => None,
        }
        .map(str::to_owned)
    }

    fn name_of(&self, node: Node) -> String {
        match self.attr(node, "aria-label") {
            Some(label) => label,
            None => self.text(node),
        }
    }

    /// Every accessible element with `role` whose accessible name is `name`.
    pub(crate) fn all_by_role(&self, role: &str, name: &str) -> Vec<Node> {
        let mut all = vec![];
        self.walk(0, true, &mut all);
        all.into_iter()
            .filter(|node| {
                self.role_of(*node).as_deref() == Some(role) && self.name_of(*node) == name
            })
            .collect()
    }

    /// The one accessible element with `role` named `name`; zero or several is a failure.
    pub(crate) fn by_role(&self, role: &str, name: &str) -> Node {
        let found = self.all_by_role(role, name);
        match found[..] {
            [only] => only,
            _ => panic!(
                "expected exactly one {role} named {name:?}, found {} in:\n{}",
                found.len(),
                self.outline()
            ),
        }
    }

    /// Whether an accessible element with `role` named `name` exists.
    pub(crate) fn has_role(&self, role: &str, name: &str) -> bool {
        !self.all_by_role(role, name).is_empty()
    }

    /// The accessibility outline of the page: one line per accessible element with a role.
    pub(crate) fn outline(&self) -> String {
        let mut all = vec![];
        self.walk(0, true, &mut all);
        all.into_iter()
            .filter_map(|node| {
                self.role_of(node)
                    .map(|role| format!("{role} {:?}\n", self.name_of(node)))
            })
            .collect()
    }

    fn dispatch(&mut self, node: Node, name: &str, data: EventData) {
        let element = self.element_id(node);
        self.vdom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            name,
            Event::new(data.into_any(), true),
            element,
        );
        self.settle();
    }

    /// A primary-button click.
    pub(crate) fn click(&mut self, node: Node) {
        self.dispatch(
            node,
            "click",
            EventData::Mouse(SerializedMouseData::default()),
        );
    }

    /// A pointer event (`pointerdown`, `pointermove`, `pointerup`, `pointerleave`,
    /// `pointercancel`) at document x `page_x`. The page coordinate is the only one the kit reads
    /// during a gesture (Blitz skill: the target changes mid-gesture, the page space does not).
    pub(crate) fn pointer(&mut self, node: Node, name: &str, page_x: f64) {
        let point = SerializedPointInteraction {
            page_x,
            client_x: page_x,
            ..Default::default()
        };
        let mut json = serde_json::to_value(point).expect("point serializes");
        json.as_object_mut().unwrap().extend(
            serde_json::json!({
                "pointer_id": 1, "width": 1.0, "height": 1.0, "pressure": 0.5,
                "tangential_pressure": 0.0, "tilt_x": 0, "tilt_y": 0, "twist": 0,
                "pointer_type": "mouse", "is_primary": true,
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let data: SerializedPointerData = serde_json::from_value(json).expect("pointer data");
        self.dispatch(node, name, EventData::Pointer(data));
    }

    /// A `keydown` of `key`.
    pub(crate) fn key(&mut self, node: Node, key: Key) {
        let data = SerializedKeyboardData::new(
            key,
            Code::Unidentified,
            Location::Standard,
            false,
            Modifiers::empty(),
            false,
        );
        self.dispatch(node, "keydown", EventData::Keyboard(data));
    }
}

impl Page {
    /// The parent element of `node`.
    pub(crate) fn parent(&self, node: Node) -> Node {
        Node(self.dom.nodes[node.0].parent.expect("node has a parent"))
    }
}
