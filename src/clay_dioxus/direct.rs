//! X4 spike (docs/scratch/pierre-spikes.md, "X4 receivers"): receivers that apply a guest's
//! presentation batches straight to the real DOM under a host-owned container element, with no
//! retained [`clay_surface::SurfaceTree`] and no host `VirtualDom` diff of the guest's subtree.
//!
//! Two backends run the same Dioxus mutation stack machine (`SurfaceTree`'s `WriteMutations`
//! impl) on live DOM nodes:
//! - [`Kind::Js`]: the decoded batch is flattened into one `u32` op array plus one string blob
//!   and handed to `direct.js` in a single call (the sledgehammer shape);
//! - [`Kind::Rust`]: wasm walks the decoded batch and calls web-sys per op.
//!
//! Selected per surface by `window.__clayDirect = "js" | "rust"` for owners whose name contains
//! `pierre_spike_scroll`. Browser builds only.
use clay_host::ComponentRef;
use clay_surface::ElementCommand;
use clay_surface::ElementRead;
use clay_surface::Event;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::ScrollBehavior;
use clay_surface::SurfaceEventHandler;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Js,
    Rust,
}

#[wasm_bindgen(module = "/direct.js")]
extern "C" {
    type Receiver;
    #[wasm_bindgen(constructor)]
    fn new(dispatch: &js_sys::Function, mouse_json: &str) -> Receiver;
    #[wasm_bindgen(method)]
    fn apply(this: &Receiver, ops: &[u32], strings: &str) -> f64;
    #[wasm_bindgen(method)]
    fn attach(this: &Receiver, container: &str);
    #[wasm_bindgen(method)]
    fn clear(this: &Receiver);
    #[wasm_bindgen(method)]
    fn command(this: &Receiver, id: u32, kind: u32, a: f64, b: f64, smooth: bool) -> String;

    #[wasm_bindgen(js_name = newRoot)]
    fn new_root() -> web_sys::Node;
    #[wasm_bindgen(js_name = attachRoot)]
    fn attach_root(container: &str, root: &web_sys::Node);
    #[wasm_bindgen(js_name = runCommand)]
    fn run_command(el: &web_sys::Node, kind: u32, a: f64, b: f64, smooth: bool) -> String;
    #[wasm_bindgen(js_name = eventJson)]
    fn event_json(e: &web_sys::Event, name: &str, mouse_json: &str) -> String;
    #[wasm_bindgen(js_name = listenerOptions)]
    fn listener_options(name: &str) -> JsValue;
    #[wasm_bindgen(js_name = queueMicrotaskFn)]
    fn queue_microtask(f: &js_sys::Function);
}

type Dispatch = Rc<RefCell<Option<SurfaceEventHandler>>>;

/// Deliver one DOM event to the bound surface handler; the result is a bit mask
/// (1 prevent-default, 2 stop-propagation) the receiver applies to the DOM event.
fn deliver(dispatch: &Dispatch, element: u32, name: &str, bubbles: bool, data_json: String, revision: u64) -> u32 {
    // Cloned out so no borrow is held while the guest runs (it re-enters `apply`).
    let handler = dispatch
        .borrow()
        .clone()
        .expect("a DOM event reached a direct receiver before its surface was bound");
    let response = handler(Event {
        revision,
        element,
        name: name.to_owned(),
        bubbles,
        data_json,
    });
    u32::from(response.prevent_default) | (u32::from(response.stop_propagation) << 1)
}

struct JsBackend {
    receiver: Receiver,
    _dispatch_closure: Closure<dyn Fn(u32, String, bool, String, f64) -> u32>,
    ops: RefCell<Vec<u32>>,
    strings: RefCell<String>,
}

const NONE: u32 = u32::MAX;

impl JsBackend {
    fn new(dispatch: Dispatch, mouse_json: &str) -> Self {
        let closure: Closure<dyn Fn(u32, String, bool, String, f64) -> u32> =
            Closure::new(move |id, name: String, bubbles, json, revision| {
                deliver(&dispatch, id, &name, bubbles, json, revision as u64)
            });
        Self {
            receiver: Receiver::new(closure.as_ref().unchecked_ref(), mouse_json),
            _dispatch_closure: closure,
            ops: RefCell::default(),
            strings: RefCell::default(),
        }
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> u64 {
        let mut out = Encoder {
            ops: self.ops.take(),
            strings: self.strings.take(),
            utf16_len: 0,
        };
        out.ops.clear();
        out.strings.clear();
        let encoded = super::timing::now();
        for mutation in edits {
            out.mutation(mutation);
        }
        super::timing::record("direct-encode", encoded, edits.len() as f64, out.ops.len() as f64);
        let started = super::timing::now();
        let revision = self.receiver.apply(&out.ops, &out.strings);
        super::timing::record("direct-js", started, edits.len() as f64, 0.0);
        *self.ops.borrow_mut() = out.ops;
        *self.strings.borrow_mut() = out.strings;
        revision as u64
    }
}

/// The flat form `direct.js` reads: opcodes and ids in `ops`, strings as (offset, length) pairs
/// into `strings`, in UTF-16 units because the JS side slices the decoded blob with `substring`.
struct Encoder {
    ops: Vec<u32>,
    strings: String,
    utf16_len: u32,
}

impl Encoder {
    fn put(&mut self, text: &str) {
        let len = if text.is_ascii() { text.len() } else { text.encode_utf16().count() } as u32;
        self.ops.extend([self.utf16_len, len]);
        self.strings.push_str(text);
        self.utf16_len += len;
    }

    fn put_opt(&mut self, text: Option<&str>) {
        match text {
            Some(text) => self.put(text),
            None => self.ops.extend([0, NONE]),
        }
    }

    fn mutation(&mut self, mutation: &Mutation<ComponentRef>) {
        use clay_surface::AttributeValue as V;
        match mutation {
            Mutation::PushId(id) => self.ops.extend([0, *id]),
            Mutation::SetId(id) => self.ops.extend([1, *id]),
            Mutation::Child(index) => self.ops.extend([2, *index]),
            Mutation::Pop => self.ops.push(3),
            Mutation::CreateElement((tag, ns)) => {
                self.ops.push(4);
                self.put(tag);
                self.put_opt(ns.as_deref());
            }
            Mutation::CreateText(text) => {
                self.ops.push(5);
                self.put(text);
            }
            Mutation::MountComponent(_) => {
                panic!("the direct receiver prototype has no component slots")
            }
            Mutation::CloneNode => self.ops.push(6),
            Mutation::AppendChildren(m) => self.ops.extend([7, *m]),
            Mutation::ReplaceWith(m) => self.ops.extend([8, *m]),
            Mutation::InsertAfter(m) => self.ops.extend([9, *m]),
            Mutation::InsertBefore(m) => self.ops.extend([10, *m]),
            Mutation::SetAttribute((name, ns, value)) => {
                self.ops.push(11);
                self.put(name);
                self.put_opt(ns.as_deref());
                match value {
                    V::Text(text) => {
                        self.ops.push(0);
                        self.put(text);
                    }
                    V::Float(float) => {
                        self.ops.push(0);
                        self.put(&float.to_string());
                    }
                    V::Int(int) => {
                        self.ops.push(0);
                        self.put(&int.to_string());
                    }
                    V::Boolean(true) => self.ops.push(1),
                    V::Boolean(false) | V::None => self.ops.push(2),
                }
            }
            Mutation::SetText(text) => {
                self.ops.push(12);
                self.put(text);
            }
            Mutation::AddEventListener(name) => {
                self.ops.push(13);
                self.put(name);
            }
            Mutation::RemoveEventListener(name) => {
                self.ops.push(14);
                self.put(name);
            }
            Mutation::Remove => self.ops.push(15),
        }
    }
}

struct RustListener {
    node: web_sys::Node,
    name: String,
    closure: Closure<dyn FnMut(web_sys::Event)>,
}

struct RustBackend {
    document: web_sys::Document,
    root: web_sys::Node,
    nodes: Vec<Option<web_sys::Node>>,
    stack: Vec<(web_sys::Node, Option<u32>)>,
    listeners: BTreeMap<(u32, String), RustListener>,
    pending_mounted: Vec<u32>,
    attached: bool,
    revision: u64,
    dispatch: Dispatch,
    mouse_json: Rc<str>,
}

impl RustBackend {
    fn new(dispatch: Dispatch, mouse_json: &str) -> Self {
        let root = new_root();
        Self {
            document: web_sys::window().expect("a window").document().expect("a document"),
            nodes: vec![Some(root.clone())],
            stack: vec![(root.clone(), Some(0))],
            root,
            listeners: BTreeMap::new(),
            pending_mounted: Vec::new(),
            attached: false,
            revision: 0,
            dispatch,
            mouse_json: mouse_json.into(),
        }
    }

    fn node(&self, id: u32) -> web_sys::Node {
        self.nodes[id as usize].clone().expect("push-id names a live node")
    }

    fn top(&self) -> &web_sys::Node {
        &self.stack.last().expect("stack is never empty").0
    }

    fn pop_n(&mut self, m: u32) -> Vec<web_sys::Node> {
        let at = self.stack.len() - m as usize;
        self.stack.split_off(at).into_iter().map(|(node, _)| node).collect()
    }

    fn fragment(&self, kids: Vec<web_sys::Node>) -> web_sys::Node {
        let fragment: web_sys::Node = self.document.create_document_fragment().into();
        for kid in kids {
            fragment.append_child(&kid).unwrap();
        }
        fragment
    }

    fn apply(&mut self, edits: &[Mutation<ComponentRef>]) -> u64 {
        use clay_surface::AttributeValue as V;
        for mutation in edits {
            match mutation {
                Mutation::PushId(id) => {
                    let node = self.node(*id);
                    self.stack.push((node, Some(*id)));
                }
                Mutation::SetId(id) => {
                    let top = self.stack.last_mut().expect("stack is never empty");
                    top.1 = Some(*id);
                    let node = top.0.clone();
                    if self.nodes.len() <= *id as usize {
                        self.nodes.resize(*id as usize + 1, None);
                    }
                    self.nodes[*id as usize] = Some(node);
                }
                Mutation::Child(index) => {
                    let child = self.top().child_nodes().item(*index).expect("child index in range");
                    *self.stack.last_mut().expect("stack is never empty") = (child, None);
                }
                Mutation::Pop => {
                    self.stack.pop();
                }
                Mutation::CreateElement((tag, ns)) => {
                    let element = match ns {
                        None => self.document.create_element(tag).unwrap(),
                        Some(ns) => self.document.create_element_ns(Some(ns), tag).unwrap(),
                    };
                    self.stack.push((element.into(), None));
                }
                Mutation::CreateText(text) => {
                    self.stack.push((self.document.create_text_node(text).into(), None));
                }
                Mutation::MountComponent(_) => {
                    panic!("the direct receiver prototype has no component slots")
                }
                Mutation::CloneNode => {
                    let cloned = self.top().clone_node_with_deep(true).unwrap();
                    *self.stack.last_mut().expect("stack is never empty") = (cloned, None);
                }
                Mutation::AppendChildren(m) => {
                    let kids = self.pop_n(*m);
                    let parent = self.top();
                    for kid in &kids {
                        parent.append_child(kid).unwrap();
                    }
                }
                Mutation::ReplaceWith(m) => {
                    let kids = self.pop_n(*m);
                    let (target, _) = self.stack.pop().expect("replace target");
                    let replacement = if kids.len() == 1 { kids[0].clone() } else { self.fragment(kids) };
                    let parent = target.parent_node().expect("replaced node has a parent");
                    parent.replace_child(&replacement, &target).unwrap();
                }
                Mutation::InsertAfter(m) => {
                    let kids = self.pop_n(*m);
                    let anchor = self.top();
                    let insert = if kids.len() == 1 { kids[0].clone() } else { self.fragment(kids) };
                    let parent = anchor.parent_node().expect("anchor has a parent");
                    parent.insert_before(&insert, anchor.next_sibling().as_ref()).unwrap();
                }
                Mutation::InsertBefore(m) => {
                    let kids = self.pop_n(*m);
                    let anchor = self.top();
                    let insert = if kids.len() == 1 { kids[0].clone() } else { self.fragment(kids) };
                    let parent = anchor.parent_node().expect("anchor has a parent");
                    parent.insert_before(&insert, Some(anchor)).unwrap();
                }
                Mutation::SetAttribute((name, ns, value)) => {
                    // `unchecked_ref`: the stack machine already knows it is an element here,
                    // so no `instanceof` per op.
                    let element: &web_sys::Element = self.top().unchecked_ref();
                    match (ns.as_deref(), value) {
                        (Some("style"), V::None | V::Boolean(false)) => {
                            let style = element.unchecked_ref::<web_sys::HtmlElement>().style();
                            style.remove_property(name).unwrap();
                        }
                        (Some("style"), value) => {
                            let style = element.unchecked_ref::<web_sys::HtmlElement>().style();
                            style.set_property(name, &attr_text(value)).unwrap();
                        }
                        (Some(ns), V::None | V::Boolean(false)) => element.remove_attribute_ns(Some(ns), name).unwrap(),
                        (Some(ns), value) => element.set_attribute_ns(Some(ns), name, &attr_text(value)).unwrap(),
                        (None, value) if matches!(name.as_str(), "value" | "checked" | "selected") => {
                            let on = !matches!(value, V::None | V::Boolean(false));
                            let text = if on { attr_text(value) } else { String::new() };
                            let key = JsValue::from_str(name);
                            let set = if name == "value" { JsValue::from_str(&text) } else { JsValue::from_bool(on) };
                            js_sys::Reflect::set(element, &key, &set).unwrap();
                        }
                        (None, V::None | V::Boolean(false)) => element.remove_attribute(name).unwrap(),
                        (None, value) => element.set_attribute(name, &attr_text(value)).unwrap(),
                    }
                }
                Mutation::SetText(text) => {
                    self.top().set_text_content(Some(text));
                }
                Mutation::AddEventListener(name) => {
                    let (node, id) = self.stack.last().expect("stack is never empty").clone();
                    let id = id.expect("event listener target has no element id");
                    if name == "mounted" {
                        self.pending_mounted.push(id);
                        continue;
                    }
                    let dispatch = self.dispatch.clone();
                    let mouse = self.mouse_json.clone();
                    let event_name = name.clone();
                    let closure: Closure<dyn FnMut(web_sys::Event)> = Closure::new(move |event: web_sys::Event| {
                        let json = event_json(&event, &event_name, &mouse);
                        let mask = deliver(&dispatch, id, &event_name, event.bubbles(), json, 0);
                        if mask & 1 != 0 {
                            event.prevent_default();
                        }
                        if mask & 2 != 0 {
                            event.stop_propagation();
                        }
                    });
                    let target: &web_sys::EventTarget = node.unchecked_ref();
                    let options = listener_options(name);
                    let function: &js_sys::Function = closure.as_ref().unchecked_ref();
                    let passive = js_sys::Reflect::get(&options, &"passive".into()).is_ok_and(|v| v.is_truthy());
                    let init = web_sys::AddEventListenerOptions::new();
                    init.set_passive(passive);
                    target
                        .add_event_listener_with_callback_and_add_event_listener_options(name, function, &init)
                        .unwrap();
                    self.listeners.insert(
                        (id, name.clone()),
                        RustListener {
                            node,
                            name: name.clone(),
                            closure,
                        },
                    );
                }
                Mutation::RemoveEventListener(name) => {
                    let id = self.stack.last().expect("stack is never empty").1.expect("listener target has an id");
                    if let Some(listener) = self.listeners.remove(&(id, name.clone())) {
                        let target: &web_sys::EventTarget = listener.node.unchecked_ref();
                        target
                            .remove_event_listener_with_callback(&listener.name, listener.closure.as_ref().unchecked_ref())
                            .unwrap();
                    }
                }
                Mutation::Remove => {
                    let (node, _) = self.stack.pop().expect("remove target");
                    if let Some(parent) = node.parent_node() {
                        parent.remove_child(&node).unwrap();
                    }
                }
            }
        }
        self.revision += 1;
        self.revision
    }
}

fn attr_text(value: &clay_surface::AttributeValue) -> String {
    use clay_surface::AttributeValue as V;
    match value {
        V::Text(text) => text.clone(),
        V::Float(float) => float.to_string(),
        V::Int(int) => int.to_string(),
        V::Boolean(_) | V::None => String::new(),
    }
}

enum Backend {
    Js(JsBackend),
    Rust(RefCell<RustBackend>),
}

struct Slot {
    backend: Backend,
    dispatch: Dispatch,
}

thread_local! {
    static SLOTS: RefCell<BTreeMap<u32, Rc<Slot>>> = RefCell::default();
    static NEXT: Cell<u32> = const { Cell::new(1) };
}

fn slot(id: u32) -> Rc<Slot> {
    SLOTS.with(|slots| slots.borrow().get(&id).expect("a live direct receiver").clone())
}

/// Which receiver, if any, this owner's surface asks for.
pub(crate) fn requested(owner: &str) -> Option<Kind> {
    if !owner.contains("pierre_spike_scroll") {
        return None;
    }
    let window = web_sys::window().expect("a window");
    match js_sys::Reflect::get(&window, &"__clayDirect".into()).expect("window is readable").as_string().as_deref() {
        Some("js") => Some(Kind::Js),
        Some("rust") => Some(Kind::Rust),
        _ => None,
    }
}

pub(crate) fn create(kind: Kind) -> u32 {
    let mouse_json = serde_json::to_string(&dioxus_html::SerializedMouseData::default()).expect("mouse data serializes");
    let dispatch: Dispatch = Rc::default();
    let backend = match kind {
        Kind::Js => Backend::Js(JsBackend::new(dispatch.clone(), &mouse_json)),
        Kind::Rust => Backend::Rust(RefCell::new(RustBackend::new(dispatch.clone(), &mouse_json))),
    };
    let id = NEXT.with(|next| next.replace(next.get() + 1));
    SLOTS.with(|slots| slots.borrow_mut().insert(id, Rc::new(Slot { backend, dispatch })));
    id
}

pub(crate) fn container_id(id: u32) -> String {
    format!("clay-direct-{id}")
}

/// Bind the surface's event handler: DOM events reach the guest through it.
pub(crate) fn bind(id: u32, handler: SurfaceEventHandler) {
    *slot(id).dispatch.borrow_mut() = Some(handler);
}

/// Put the receiver's root under the host-rendered container and deliver the `mounted` events
/// queued so far.
pub(crate) fn attach(id: u32) {
    let slot = slot(id);
    match &slot.backend {
        Backend::Js(js) => js.receiver.attach(&container_id(id)),
        Backend::Rust(rust) => {
            let (root, pending) = {
                let mut rust = rust.borrow_mut();
                rust.attached = true;
                (rust.root.clone(), std::mem::take(&mut rust.pending_mounted))
            };
            attach_root(&container_id(id), &root);
            flush_mounted(&slot, pending);
        }
    }
}

fn flush_mounted(slot: &Slot, ids: Vec<u32>) {
    for element in ids {
        deliver(&slot.dispatch, element, "mounted", false, "null".into(), 0);
    }
}

pub(crate) fn apply(id: u32, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
    let slot = slot(id);
    let revision = match &slot.backend {
        Backend::Js(js) => js.apply(edits),
        Backend::Rust(rust) => {
            let started = super::timing::now();
            let (revision, pending) = {
                let mut rust = rust.borrow_mut();
                let revision = rust.apply(edits);
                let pending = if rust.attached { std::mem::take(&mut rust.pending_mounted) } else { Vec::new() };
                (revision, pending)
            };
            super::timing::record("direct-rust", started, edits.len() as f64, 0.0);
            if !pending.is_empty() {
                let slot = slot.clone();
                let task = Closure::once_into_js(move || flush_mounted(&slot, pending));
                queue_microtask(task.unchecked_ref());
            }
            revision
        }
    };
    Ok(revision)
}

pub(crate) fn command(id: u32, element: u32, command: ElementCommand) {
    let slot = slot(id);
    let (kind, a, b, smooth, request) = match command {
        ElementCommand::Scroll(to) => (0, to.left, to.top, to.behavior == ScrollBehavior::Smooth, 0),
        ElementCommand::ScrollBy(by) => (1, by.left, by.top, by.behavior == ScrollBehavior::Smooth, 0),
        ElementCommand::SetFocus(focus) => (2, f64::from(u8::from(focus)), 0.0, false, 0),
        ElementCommand::Read { request, what } => (
            match what {
                ElementRead::ClientRect => 3,
                ElementRead::ScrollOffset => 4,
                ElementRead::ScrollSize => 5,
            },
            0.0,
            0.0,
            false,
            request,
        ),
    };
    let value = match &slot.backend {
        Backend::Js(js) => js.receiver.command(element, kind, a, b, smooth),
        Backend::Rust(rust) => {
            let node = rust.borrow().node(element);
            run_command(&node, kind, a, b, smooth)
        }
    };
    if request != 0 {
        // The guest is mid-call: answer once it returns.
        let task = Closure::once_into_js(move || {
            deliver(
                &slot.dispatch,
                element,
                clay_surface::ELEMENT_REPLY,
                false,
                format!("{{\"request\":{request},\"value\":{value}}}"),
                0,
            );
        });
        queue_microtask(task.unchecked_ref());
    }
}

/// Clear the receiver's DOM and forget it: the surface was withdrawn.
pub(crate) fn withdraw(id: u32) {
    let Some(slot) = SLOTS.with(|slots| slots.borrow_mut().remove(&id)) else {
        return;
    };
    match &slot.backend {
        Backend::Js(js) => js.receiver.clear(),
        Backend::Rust(rust) => {
            let mut rust = rust.borrow_mut();
            while let Some(child) = rust.root.first_child() {
                rust.root.remove_child(&child).unwrap();
            }
            rust.listeners.clear();
            rust.nodes.truncate(1);
        }
    }
}
