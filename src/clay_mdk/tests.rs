//! Recording renderer probe: exercises the actual component ABI, not the Blitz renderer.
//!
//! The host replays each typed batch into a Rust `WriteMutations`. That is the same path the
//! Blitz-backed native-dom writer takes, so what this test proves about ordering and content holds
//! for the real renderer; only tree semantics differ.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use wasmtime::component::{Component, HasSelf, Linker, Resource, ResourceTable};
use wasmtime::{Engine, Store};

#[path = "../micro_counter/fixture.rs"]
mod counter_fixture;
use counter_fixture::counter_component;

mod host_bindings {
    wasmtime::component::bindgen!({
        path: "../clay_guest_session/wit",
        world: "micro-client",
        imports: { default: trappable },
        with: {
            "townframe:dayframe/presentation.surface": super::Surface,
            "townframe:dayframe/presentation.mount-point": super::Placement,
            "townframe:dayframe/presentation.component-ref": super::ComponentRef,
        },
    });
}

use blitz_dom::{BaseDocument, DocumentConfig, LocalName, NodeId, QualName, ns};
use clay_surface::{AttributeValue, Mutation};
use dioxus_core::{BorrowedAttributeValue, ElementId, WriteMutations};
use dioxus_native_dom::DioxusState;
use host_bindings::exports::townframe::dayframe::presentation_events::Event;
use host_bindings::townframe::dayframe::{
    documents,
    presentation::{self, *},
};

/// Replays one wire batch into any host `WriteMutations`, in order.
fn replay(batch: &[presentation::Mutation], to: &mut impl WriteMutations) {
    use presentation::{AttributeValue, Mutation};
    let element = |id: u32| ElementId::from_raw(id as usize);
    for mutation in batch {
        match mutation {
            Mutation::PushId(id) => to.push_id(element(*id)),
            Mutation::SetId(id) => to.set_id(element(*id)),
            Mutation::Child(index) => to.child(*index as usize),
            Mutation::Pop => to.pop(),
            Mutation::CreateElement((tag, ns)) => to.create_element(tag, ns.as_deref()),
            Mutation::CreateText(value) => to.create_text(value),
            Mutation::MountComponent(_) => unreachable!("counter fixture has no component grants"),
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

/// A host writer that records the replayed operations as host-side `Mutation`s. The surface's
/// view of the tree is exactly what the replay saw, independent of how the guest encoded it.
#[derive(Default)]
struct RecordingWriter(Vec<Mutation>);

impl WriteMutations for RecordingWriter {
    fn push_id(&mut self, id: ElementId) {
        self.0.push(Mutation::PushId(id.raw() as u32));
    }
    fn set_id(&mut self, id: ElementId) {
        self.0.push(Mutation::SetId(id.raw() as u32));
    }
    fn child(&mut self, index: usize) {
        self.0.push(Mutation::Child(index as u32));
    }
    fn pop(&mut self) {
        self.0.push(Mutation::Pop);
    }
    fn create_element(&mut self, tag: &str, ns: Option<&str>) {
        self.0
            .push(Mutation::CreateElement((tag.into(), ns.map(Into::into))));
    }
    fn create_text(&mut self, value: &str) {
        self.0.push(Mutation::CreateText(value.into()));
    }
    fn clone(&mut self) {
        self.0.push(Mutation::CloneNode);
    }
    fn append_children(&mut self, m: usize) {
        self.0.push(Mutation::AppendChildren(m as u32));
    }
    fn replace_with(&mut self, m: usize) {
        self.0.push(Mutation::ReplaceWith(m as u32));
    }
    fn insert_after(&mut self, m: usize) {
        self.0.push(Mutation::InsertAfter(m as u32));
    }
    fn insert_before(&mut self, m: usize) {
        self.0.push(Mutation::InsertBefore(m as u32));
    }
    fn set_attribute(&mut self, name: &str, ns: Option<&str>, value: BorrowedAttributeValue<'_>) {
        let value = match value {
            BorrowedAttributeValue::Text(text) => AttributeValue::Text(text.into()),
            BorrowedAttributeValue::Float(float) => AttributeValue::Float(float),
            BorrowedAttributeValue::Int(int) => AttributeValue::Int(int),
            BorrowedAttributeValue::Bool(bool) => AttributeValue::Boolean(bool),
            BorrowedAttributeValue::None => AttributeValue::None,
            BorrowedAttributeValue::Any(_) => unreachable!("`Any` never crosses the wire"),
        };
        self.0.push(Mutation::SetAttribute((
            name.into(),
            ns.map(Into::into),
            value,
        )));
    }
    fn set_text(&mut self, value: &str) {
        self.0.push(Mutation::SetText(value.into()));
    }
    fn add_event_listener(&mut self, name: &str) {
        self.0.push(Mutation::AddEventListener(name.into()));
    }
    fn remove_event_listener(&mut self, name: &str) {
        self.0.push(Mutation::RemoveEventListener(name.into()));
    }
    fn remove(&mut self) {
        self.0.push(Mutation::Remove);
    }
}

/// Where a surface's edits land. One host, two backends: the recording backend keeps wire batches
/// for assertions, the Blitz backend replays them into a real retained tree through native-dom's
/// writer. A webview backend would replay the same batches into a real DOM; the batch format is the
/// contract, so a backend is a `WriteMutations` implementation plus event translation.
enum Backend {
    Recording(Vec<Vec<Mutation>>),
    /// Committed revisions. The Blitz tree state itself lives in [`TestHost::states`]: a
    /// `DioxusState` holds `Rc` attribute values, so it is not `Send` and cannot live in Wasmtime's
    /// resource table.
    Blitz {
        revisions: u64,
    },
}

#[derive(Clone)]
pub struct Surface {
    alive: Arc<AtomicBool>,
    backend: Arc<Mutex<Backend>>,
    identity: u64,
}

impl Surface {
    fn batches(&self) -> Vec<Vec<Mutation>> {
        match &*self.backend.lock().unwrap() {
            Backend::Recording(batches) => batches.clone(),
            Backend::Blitz { .. } => {
                panic!("a Blitz surface commits into a real tree; it records nothing")
            }
        }
    }
}

/// A granted placement. `root` is the real document node the surface renders into; a placement
/// without one is synthetic and its surface records batches instead.
pub struct Placement {
    alive: Arc<AtomicBool>,
    root: Option<NodeId>,
}

impl Placement {
    fn synthetic(alive: Arc<AtomicBool>) -> Self {
        Self { alive, root: None }
    }
}

struct TestHost {
    table: ResourceTable,
    doc: Option<BaseDocument>,
    /// Per-surface Blitz tree state, keyed by the surface resource's rep.
    states: BTreeMap<u64, DioxusState>,
    routes: BTreeMap<u64, Vec<u32>>,
    next_surface: u64,
}

impl TestHost {
    fn new() -> Self {
        Self {
            table: ResourceTable::new(),
            doc: None,
            states: BTreeMap::new(),
            routes: BTreeMap::new(),
            next_surface: 0,
        }
    }

    fn start(
        store: &mut Store<Self>,
        component: &Component,
        linker: &Linker<Self>,
    ) -> wasmtime::Result<host_bindings::MicroClient> {
        let guest = host_bindings::MicroClient::instantiate(&mut *store, component, linker)?;
        guest
            .townframe_dayframe_micro_lifecycle()
            .call_activate(store, &[])?
            .map_err(wasmtime::Error::msg)?;
        Ok(guest)
    }

    /// Grants a placement rooted at a real node in this host's document, creating the document on
    /// first use so recording-only hosts never pay for one.
    fn grant_blitz_placement(&mut self) -> Resource<Placement> {
        let doc = self
            .doc
            .get_or_insert_with(|| BaseDocument::new(DocumentConfig::default()));
        let mut mutator = doc.mutate();
        let root = mutator.create_element(
            QualName {
                prefix: None,
                ns: ns!(html),
                local: LocalName::from("div"),
            },
            vec![],
        );
        let document_root = mutator.doc.root_node().id;
        mutator.append_children(document_root, &[root]);
        drop(mutator);
        self.table
            .push(Placement {
                alive: Arc::new(AtomicBool::new(true)),
                root: Some(root),
            })
            .expect("resource table is unbounded")
    }

    /// The ids of the counter's buttons in a granted placement's real subtree.
    fn buttons(&self, placement: &Resource<Placement>) -> Vec<NodeId> {
        let root = self
            .table
            .get(placement)
            .expect("placement is live")
            .root
            .expect("placement is Blitz-backed");
        let doc = self
            .doc
            .as_ref()
            .expect("a Blitz placement implies a document");
        doc.query_selector_all_in(root, "button")
            .expect("selector is valid")
            .into_vec()
    }

    /// The counter's visible text, read structurally out of the real document rather than parsed
    /// back out of serialized HTML.
    fn counter_text(&self, placement: &Resource<Placement>) -> String {
        let doc = self.doc.as_ref().expect("document exists");
        let buttons = self.buttons(placement);
        assert_eq!(buttons.len(), 1, "the counter renders exactly one button");
        doc.get_node(buttons[0])
            .expect("button is in the document")
            .text_content()
    }

    /// Whether the counter's button carries a registered click listener. Blitz records listeners
    /// as attributes, so this proves `add-event-listener` reached the real tree.
    fn has_click_listener(&self, placement: &Resource<Placement>) -> bool {
        let buttons = self.buttons(placement);
        assert_eq!(buttons.len(), 1, "the counter renders exactly one button");
        self.doc
            .as_ref()
            .expect("document exists")
            .get_node(buttons[0])
            .expect("button is in the document")
            .attr(LocalName::from("click"))
            .is_some()
    }

    /// Resolves a click targeting `target` in the Blitz document to `(surface_rep, Event)` by
    /// walking ancestors for `data-dioxus-id` and matching the surface whose `DioxusState` owns
    /// that `NodeId`. Releases all borrows of `BaseDocument` before returning so the caller can
    /// invoke the guest (which re-entrantly calls `surface.apply` and mutates `BaseDocument`).
    fn resolve_click(&self, target: NodeId) -> Option<(u32, Event)> {
        use dioxus_html::{
            EventData, Modifiers, SerializedMouseData,
            geometry::{ClientPoint, Coordinates, ElementPoint, PagePoint, ScreenPoint},
            input_data::{MouseButton, decode_mouse_button_set},
        };

        let doc = self.doc.as_ref()?;
        let mut current = Some(target);
        let (rep, node_id, element_id) =
            loop {
                let node_id = current?;
                let node = doc.get_node(node_id)?;
                if let Some(raw_id) = node
                    .attr(LocalName::from("data-dioxus-id"))
                    .and_then(|value| value.parse::<usize>().ok())
                {
                    let element_id = ElementId::from_raw(raw_id);
                    if let Some((&rep, _)) = self.states.iter().find(|(_, state)| {
                        state.try_element_to_node_id(element_id) == Some(node_id)
                    }) {
                        break (rep, node_id, element_id);
                    }
                }
                current = node.parent;
            };

        let rep = *self.routes.get(&rep)?.first()?;
        let surface = self.table.get(&Resource::<Surface>::new_borrow(rep)).ok()?;
        let revision = match &*surface.backend.lock().unwrap() {
            Backend::Blitz { revisions } => *revisions,
            Backend::Recording(_) => return None,
        };
        let node = doc.get_node(node_id)?;
        let blitz = node.synthetic_click_event_data(Modifiers::empty());
        let mouse = SerializedMouseData::new(
            Some(MouseButton::Primary),
            decode_mouse_button_set(blitz.buttons.bits() as u16),
            Coordinates::new(
                ScreenPoint::new(blitz.screen_x() as f64, blitz.screen_y() as f64),
                ClientPoint::new(blitz.client_x() as f64, blitz.client_y() as f64),
                ElementPoint::new(blitz.element_x() as f64, blitz.element_y() as f64),
                PagePoint::new(blitz.page_x() as f64, blitz.page_y() as f64),
            ),
            blitz.mods,
        );
        let data_json = serde_json::to_string(&EventData::Mouse(mouse)).unwrap();
        Some((
            rep,
            Event {
                revision,
                element: element_id.raw().try_into().unwrap(),
                name: "click".into(),
                bubbles: true,
                data_json,
            },
        ))
    }
}

impl documents::Host for TestHost {}

impl documents::HostDocumentAccess for TestHost {
    fn watch(
        &mut self,
        _access: Resource<documents::DocumentAccess>,
        _selection: documents::WatchSelection,
    ) -> wasmtime::Result<Result<Resource<documents::DocumentWatch>, documents::DocumentError>>
    {
        Ok(Err(documents::DocumentError::Denied))
    }

    fn mutate(
        &mut self,
        _access: Resource<documents::DocumentAccess>,
        _route: documents::FacetRoute,
        _mutation: documents::FacetMutation,
    ) -> wasmtime::Result<Result<Resource<documents::Operation>, documents::DocumentError>> {
        Ok(Err(documents::DocumentError::Denied))
    }

    fn drop(&mut self, access: Resource<documents::DocumentAccess>) -> wasmtime::Result<()> {
        self.table.delete(access)?;
        Ok(())
    }
}

impl documents::HostDocumentWatch for TestHost {
    fn current(
        &mut self,
        _watch: Resource<documents::DocumentWatch>,
    ) -> wasmtime::Result<Result<documents::DocumentSnapshot, documents::DocumentError>> {
        Ok(Err(documents::DocumentError::Denied))
    }

    fn select(
        &mut self,
        _watch: Resource<documents::DocumentWatch>,
        _selection: documents::WatchSelection,
    ) -> wasmtime::Result<Result<(), documents::DocumentError>> {
        Ok(Err(documents::DocumentError::Denied))
    }

    fn refresh(
        &mut self,
        _watch: Resource<documents::DocumentWatch>,
    ) -> wasmtime::Result<Result<(), documents::DocumentError>> {
        Ok(Err(documents::DocumentError::Denied))
    }

    fn drop(&mut self, watch: Resource<documents::DocumentWatch>) -> wasmtime::Result<()> {
        self.table.delete(watch)?;
        Ok(())
    }
}

impl documents::HostOperation for TestHost {
    fn current(
        &mut self,
        _operation: Resource<documents::Operation>,
    ) -> wasmtime::Result<Result<documents::OperationReceipt, documents::DocumentError>> {
        Ok(Err(documents::DocumentError::Denied))
    }

    fn drop(&mut self, operation: Resource<documents::Operation>) -> wasmtime::Result<()> {
        self.table.delete(operation)?;
        Ok(())
    }
}

pub struct ComponentRef;

impl presentation::Host for TestHost {}

impl presentation::HostComponentRef for TestHost {
    fn drop(&mut self, component: Resource<ComponentRef>) -> wasmtime::Result<()> {
        self.table.delete(component)?;
        Ok(())
    }
}
impl presentation::HostMountPoint for TestHost {
    fn open(
        &mut self,
        placement: Resource<Placement>,
    ) -> wasmtime::Result<Result<Resource<Surface>, PresentationError>> {
        let granted = self.table.get(&placement)?;
        if !granted.alive.load(Ordering::Acquire) {
            return Ok(Err(PresentationError::Revoked));
        }
        let alive = granted.alive.clone();
        let root = granted.root;
        let backend = match root {
            Some(_) => Backend::Blitz { revisions: 0 },
            None => Backend::Recording(Vec::new()),
        };
        self.next_surface = self.next_surface.checked_add(1).unwrap();
        let identity = self.next_surface;
        let surface = self.table.push(Surface {
            alive,
            backend: Arc::new(Mutex::new(backend)),
            identity,
        })?;
        assert!(self.routes.insert(identity, vec![surface.rep()]).is_none());
        if let Some(root) = root {
            let previous = self.states.insert(identity, DioxusState::create(root));
            assert!(
                previous.is_none(),
                "surface identity reused while still mounted"
            );
        }
        Ok(Ok(surface))
    }
    fn drop(&mut self, placement: Resource<Placement>) -> wasmtime::Result<()> {
        self.table.delete(placement)?;
        Ok(())
    }
}
impl presentation::HostSurface for TestHost {
    fn apply(
        &mut self,
        surface: Resource<Surface>,
        edits: Vec<presentation::Mutation>,
    ) -> wasmtime::Result<Result<u64, PresentationError>> {
        let Self {
            table, doc, states, ..
        } = self;
        let surface = table.get_mut(&surface)?;
        if !surface.alive.load(Ordering::Acquire) {
            return Ok(Err(PresentationError::Revoked));
        }
        match &mut *surface.backend.lock().unwrap() {
            Backend::Recording(batches) => {
                let mut writer = RecordingWriter::default();
                replay(&edits, &mut writer);
                batches.push(writer.0);
                Ok(Ok(batches.len().try_into().unwrap()))
            }
            Backend::Blitz { revisions } => {
                let state = states
                    .get_mut(&surface.identity)
                    .expect("a Blitz surface always has tree state");
                let doc = doc.as_mut().expect("a Blitz placement implies a document");
                let mut writer = state.writer(doc);
                replay(&edits, &mut writer);
                *revisions += 1;
                Ok(Ok(*revisions))
            }
        }
    }
    fn retain(
        &mut self,
        surface: Resource<Surface>,
    ) -> wasmtime::Result<Result<Resource<Surface>, PresentationError>> {
        let surface = self.table.get(&surface)?;
        if !surface.alive.load(Ordering::Acquire) {
            return Ok(Err(PresentationError::Revoked));
        }
        let retained = surface.clone();
        let identity = surface.identity;
        let retained = self.table.push(retained)?;
        self.routes.get_mut(&identity).unwrap().push(retained.rep());
        Ok(Ok(retained))
    }

    fn identity(&mut self, surface: Resource<Surface>) -> wasmtime::Result<u64> {
        Ok(self.table.get(&surface)?.identity)
    }

    fn drop(&mut self, surface: Resource<Surface>) -> wasmtime::Result<()> {
        let rep = surface.rep();
        let surface = self.table.delete(surface)?;
        let routes = self.routes.get_mut(&surface.identity).unwrap();
        routes.retain(|routed| *routed != rep);
        if Arc::strong_count(&surface.backend) > 1 {
            return Ok(());
        }
        assert!(routes.is_empty());
        self.routes.remove(&surface.identity);
        if let Some(state) = self.states.remove(&surface.identity)
            && let Some(doc) = self.doc.as_mut()
        {
            let root = state.element_to_node_id(ElementId::ROOT);
            let mut mutator = doc.mutate();
            for child in mutator.child_ids(root) {
                mutator.remove_and_drop_node(child);
            }
        }
        Ok(())
    }
}

fn click_event(revision: u64, element: u32) -> Event {
    let html = dioxus_html::HtmlEvent {
        element: dioxus_core::ElementId::from_raw(element as usize),
        name: "click".into(),
        bubbles: true,
        data: dioxus_html::EventData::Mouse(dioxus_html::SerializedMouseData::default()),
    };
    Event {
        revision,
        element: html.element.raw().try_into().unwrap(),
        name: html.name,
        bubbles: html.bubbles,
        data_json: serde_json::to_string(&html.data).unwrap(),
    }
}

fn sdk_text_edits(dom: &mut crate::GuestDom, target: dioxus_core::RenderTargetId) -> Vec<String> {
    dom.edits
        .get_mut(&target)
        .unwrap()
        .take()
        .into_iter()
        .filter_map(|edit| match edit {
            crate::presentation::Mutation::CreateText(text)
            | crate::presentation::Mutation::SetText(text) => Some(text),
            _ => None,
        })
        .collect()
}

fn sdk_click(dom: &mut crate::GuestDom, target: dioxus_core::RenderTargetId) {
    let event = dioxus_core::Event::new(
        dioxus_html::EventData::Mouse(dioxus_html::SerializedMouseData::default()).into_any(),
        true,
    );
    dom.dom
        .runtime()
        .handle_event_for_target(target, "click", event, ElementId::from_raw(1));
    dom.render().unwrap();
}

#[test]
fn sdk_one_root_shares_state_across_targets_and_survives_all_mounts_closing() {
    use dioxus::prelude::*;
    use std::{cell::Cell, rc::Rc};

    fn counter(initializations: Rc<Cell<usize>>) -> Element {
        use_hook(move || initializations.set(initializations.get() + 1));
        let mut count = use_signal(|| 0_u64);
        rsx! { button { onclick: move |_| count += 1, "{count}" } }
    }

    let initializations = Rc::new(Cell::new(0));
    let mut dom = crate::GuestDom::new_with_props(counter, initializations.clone());
    assert_eq!(initializations.get(), 1);
    let first = dom.mount_target(100);
    dom.render().unwrap();
    assert_eq!(sdk_text_edits(&mut dom, first), ["0"]);
    let second = dom.mount_target(101);
    dom.render().unwrap();
    assert!(sdk_text_edits(&mut dom, first).is_empty());
    assert_eq!(sdk_text_edits(&mut dom, second), ["0"]);

    sdk_click(&mut dom, second);
    assert_eq!(sdk_text_edits(&mut dom, first), ["1"]);
    assert_eq!(sdk_text_edits(&mut dom, second), ["1"]);
    dom.unmount_target(100);
    assert!(sdk_text_edits(&mut dom, second).is_empty());
    assert!(!dom.edits.contains_key(&first));
    let third = dom.mount_target(102);
    dom.render().unwrap();
    assert_eq!(sdk_text_edits(&mut dom, third), ["1"]);
    sdk_click(&mut dom, third);
    assert_eq!(sdk_text_edits(&mut dom, second), ["2"]);
    assert_eq!(sdk_text_edits(&mut dom, third), ["2"]);

    dom.unmount_target(101);
    dom.unmount_target(102);
    assert!(dom.edits.is_empty());
    for identity in 103..111 {
        let target = dom.mount_target(identity);
        dom.render().unwrap();
        assert_eq!(sdk_text_edits(&mut dom, target), ["2"]);
        dom.unmount_target(identity);
        assert!(dom.edits.is_empty());
    }
    assert_eq!(initializations.get(), 1);
}

#[test]
fn sdk_colliding_element_ids_dispatch_only_to_the_selected_target_scope() {
    use dioxus::prelude::*;

    #[component]
    fn LocalCounter() -> Element {
        let mut count = use_signal(|| 0_u64);
        rsx! { button { onclick: move |_| count += 1, "{count}" } }
    }

    fn app() -> Element {
        rsx! { LocalCounter {} }
    }

    let mut dom = crate::GuestDom::new(app);
    let first = dom.mount_target(1);
    let second = dom.mount_target(2);
    dom.render().unwrap();
    assert_eq!(sdk_text_edits(&mut dom, first), ["0"]);
    assert_eq!(sdk_text_edits(&mut dom, second), ["0"]);
    sdk_click(&mut dom, second);
    assert!(sdk_text_edits(&mut dom, first).is_empty());
    assert_eq!(sdk_text_edits(&mut dom, second), ["1"]);
    sdk_click(&mut dom, first);
    assert_eq!(sdk_text_edits(&mut dom, first), ["1"]);
    assert!(sdk_text_edits(&mut dom, second).is_empty());
    dom.unmount_target(1);
    sdk_click(&mut dom, second);
    assert_eq!(sdk_text_edits(&mut dom, second), ["2"]);
    dom.unmount_target(2);
}

#[test]
fn sdk_external_updates_render_into_live_mounts_and_later_mounts() {
    use dioxus::prelude::*;
    use std::{cell::Cell, rc::Rc};

    fn app(external: Rc<Cell<Option<Signal<u64>>>>) -> Element {
        let count = use_signal(|| 0_u64);
        external.set(Some(count));
        rsx! { button { "{count}" } }
    }

    let external = Rc::new(Cell::new(None));
    let mut dom = crate::GuestDom::new_with_props(app, external.clone());
    let mut count = external.get().unwrap();
    count.set(17);
    dom.render().unwrap();
    let first = dom.mount_target(10);
    dom.render().unwrap();
    assert_eq!(sdk_text_edits(&mut dom, first), ["17"]);
    let second = dom.mount_target(11);
    dom.render().unwrap();
    assert!(sdk_text_edits(&mut dom, first).is_empty());
    assert_eq!(sdk_text_edits(&mut dom, second), ["17"]);
    count.set(18);
    dom.render().unwrap();
    assert_eq!(sdk_text_edits(&mut dom, first), ["18"]);
    assert_eq!(sdk_text_edits(&mut dom, second), ["18"]);
    dom.unmount_target(10);
    count.set(19);
    dom.render().unwrap();
    assert_eq!(sdk_text_edits(&mut dom, second), ["19"]);
    assert!(!dom.edits.contains_key(&first));
    dom.unmount_target(11);
}

#[test]
fn sdk_retained_surface_shares_identity_tree_and_revocation() -> wasmtime::Result<()> {
    use presentation::Mutation;
    let mut host = TestHost::new();
    let placement = host.grant_blitz_placement();
    let original =
        presentation::HostMountPoint::open(&mut host, Resource::new_borrow(placement.rep()))?
            .unwrap();
    let retained =
        presentation::HostSurface::retain(&mut host, Resource::new_borrow(original.rep()))?
            .unwrap();
    assert_ne!(original.rep(), retained.rep());
    let identity =
        presentation::HostSurface::identity(&mut host, Resource::new_borrow(original.rep()))?;
    assert_eq!(
        presentation::HostSurface::identity(&mut host, Resource::new_borrow(retained.rep()))?,
        identity,
    );
    presentation::HostSurface::drop(&mut host, original)?;
    presentation::HostSurface::apply(
        &mut host,
        Resource::new_borrow(retained.rep()),
        vec![
            Mutation::PushId(0),
            Mutation::CreateElement(("button".into(), None)),
            Mutation::SetId(1),
            Mutation::CreateText("kept".into()),
            Mutation::AppendChildren(1),
            Mutation::AppendChildren(1),
        ],
    )?
    .unwrap();
    assert_eq!(host.counter_text(&placement), "kept");
    host.table
        .get(&placement)?
        .alive
        .store(false, Ordering::Release);
    assert!(matches!(
        presentation::HostSurface::retain(&mut host, Resource::new_borrow(retained.rep()))?,
        Err(PresentationError::Revoked),
    ));
    assert!(matches!(
        presentation::HostSurface::apply(&mut host, Resource::new_borrow(retained.rep()), vec![])?,
        Err(PresentationError::Revoked),
    ));
    presentation::HostSurface::drop(&mut host, retained)?;
    assert!(host.buttons(&placement).is_empty());
    assert!(host.states.is_empty());
    assert!(host.routes.is_empty());
    let next_placement = host.grant_blitz_placement();
    let next =
        presentation::HostMountPoint::open(&mut host, Resource::new_borrow(next_placement.rep()))?
            .unwrap();
    assert_ne!(
        presentation::HostSurface::identity(&mut host, Resource::new_borrow(next.rep()))?,
        identity,
    );
    presentation::HostSurface::drop(&mut host, next)?;
    Ok(())
}

#[test]
fn event_envelope_roundtrips_through_dioxus_without_a_second_payload_schema() {
    let event = click_event(1, 7);
    let json = serde_json::json!({"element": event.element, "name": event.name,
        "bubbles": event.bubbles, "data": serde_json::from_str::<serde_json::Value>(&event.data_json).unwrap()});
    let html: dioxus_html::HtmlEvent = serde_json::from_value(json).unwrap();
    assert_eq!(
        html,
        dioxus_html::HtmlEvent {
            element: dioxus_core::ElementId::from_raw(7),
            name: "click".into(),
            bubbles: true,
            data: dioxus_html::EventData::Mouse(dioxus_html::SerializedMouseData::default()),
        }
    );
}

fn assert_batches(actual: &[Vec<Mutation>], expected: &[Vec<Mutation>]) {
    assert_eq!(actual, expected);
}

fn counter_click_element(batch: &[Mutation]) -> u32 {
    let mut tree = clay_surface::SurfaceTree::new();
    tree.apply(batch).unwrap();
    tree.listener_id("button", "click").unwrap().raw() as u32
}

/// The same counter rendered natively through a host `WriteMutations`. If the guest's batches
/// match this, the wire preserved Dioxus's edit stream exactly across the address space.
fn expected_counter_batches(event_count: u64) -> Vec<Vec<Mutation>> {
    use dioxus::prelude::*;
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedHtmlEventConverter, SerializedMouseData};

    fn counter() -> Element {
        let mut count = use_signal(|| Some(0_u64));
        let current = count();
        rsx! {
            div { class: "flex flex-col gap-4",
                button {
                    class: "rounded-md border border-border px-4 py-2 focus-visible:outline",
                    "aria-label": "Increment Wasm counter",
                    disabled: current.is_none(),
                    onclick: move |_| count.set(Some(count().unwrap() + 1)),
                    if let Some(count) = current { "{count}" } else { "Loading counter" }
                }
                {Option::<VNode>::None}
            }
        }
    }

    dioxus_html::set_event_converter(Box::new(SerializedHtmlEventConverter));
    let mut dom = VirtualDom::new(counter);
    let mut batches = Vec::with_capacity(event_count as usize + 1);
    let mut writer = RecordingWriter::default();
    dom.rebuild(&mut writer);
    batches.push(std::mem::take(&mut writer.0));
    let element = ElementId::from_raw(counter_click_element(&batches[0]) as usize);
    for _ in 0..event_count {
        let event = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        dom.runtime()
            .handle_event_for_target(RenderTargetId::ROOT, "click", event, element);
        dom.render_immediate(&mut writer);
        batches.push(std::mem::take(&mut writer.0));
    }
    batches
}

#[test]
fn initial_batch_is_a_real_tree_not_an_opaque_blob() {
    let batches = expected_counter_batches(1);
    // The initial build creates a button carrying a click listener with a text child.
    assert!(
        batches[0]
            .iter()
            .any(|m| matches!(m, Mutation::CreateElement((tag, _)) if tag == "button"))
    );
    assert!(
        batches[0]
            .iter()
            .any(|m| matches!(m, Mutation::AddEventListener(name) if name == "click"))
    );
    assert!(
        batches[0]
            .iter()
            .any(|m| matches!(m, Mutation::CreateText(text) if text == "0"))
    );
    // A click only updates the text node; it does not rebuild the tree.
    assert_eq!(
        batches[1]
            .iter()
            .filter(|m| matches!(m, Mutation::CreateElement(_)))
            .count(),
        0
    );
    assert!(
        batches[1]
            .iter()
            .any(|m| matches!(m, Mutation::SetText(text) if text == "1"))
    );
}

#[test]
fn shared_store_probe_pins_cross_incarnation_failure_after_guest_trap() -> wasmtime::Result<()> {
    let engine = Engine::default();
    let component = Component::new(&engine, counter_component())?;
    let mut linker = Linker::new(&engine);
    host_bindings::MicroClient::add_to_linker::<_, HasSelf<TestHost>>(&mut linker, |host| host)?;
    let mut store = Store::new(&engine, TestHost::new());
    let first = TestHost::start(&mut store, &component, &linker)?;
    let events = first.townframe_dayframe_presentation_events();
    let denied = store
        .data_mut()
        .table
        .push(Placement::synthetic(Arc::new(AtomicBool::new(false))))?;
    assert!(matches!(
        events.call_mount(&mut store, Resource::new_borrow(denied.rep()), &[])?,
        Err(PresentationError::Revoked)
    ));
    let first_lifetime = Arc::new(AtomicBool::new(true));
    let placement = store
        .data_mut()
        .table
        .push(Placement::synthetic(first_lifetime.clone()))?;
    let surface = events
        .call_mount(&mut store, Resource::new_borrow(placement.rep()), &[])?
        .unwrap();
    let expected_batches = expected_counter_batches(4);
    let element = counter_click_element(&expected_batches[0]);
    let click = |revision| click_event(revision, element);
    for revision in 1..=2 {
        let response = events.call_dispatch(
            &mut store,
            Resource::new_borrow(surface.rep()),
            &click(revision),
        )?;
        assert!(!response.prevent_default && !response.stop_propagation);
    }
    assert_batches(
        &store.data().table.get(&surface)?.batches(),
        &expected_batches[..3],
    );

    // A second instance of the same compiled component has a separate counter/address space.
    let second = TestHost::start(&mut store, &component, &linker)?;
    let second_events = second.townframe_dayframe_presentation_events();
    let second_placement = store
        .data_mut()
        .table
        .push(Placement::synthetic(Arc::new(AtomicBool::new(true))))?;
    let second_surface = second_events
        .call_mount(
            &mut store,
            Resource::new_borrow(second_placement.rep()),
            &[],
        )?
        .unwrap();
    second_events.call_dispatch(
        &mut store,
        Resource::new_borrow(second_surface.rep()),
        &click(1),
    )?;
    assert_batches(
        &store.data().table.get(&second_surface)?.batches(),
        &expected_batches[..2],
    );
    events.call_dispatch(&mut store, Resource::new_borrow(surface.rep()), &click(3))?;
    let expected = &expected_batches[..4];
    assert_batches(&store.data().table.get(&surface)?.batches(), expected);

    let mut malformed = click(4);
    malformed.data_json = "invalid JSON".into();
    let error = events
        .call_dispatch(&mut store, Resource::new_borrow(surface.rep()), &malformed)
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::UnreachableCodeReached)
    );
    assert_batches(&store.data().table.get(&surface)?.batches(), expected);
    let error = second_events
        .call_dispatch(
            &mut store,
            Resource::new_borrow(second_surface.rep()),
            &click(2),
        )
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::CannotEnterComponent)
    );
    assert_batches(
        &store.data().table.get(&second_surface)?.batches(),
        &expected_batches[..2],
    );
    Ok(())
}

#[test]
fn separate_stores_isolate_guest_traps() -> wasmtime::Result<()> {
    let engine = Engine::default();
    let component = Component::new(&engine, counter_component())?;
    let mut linker = Linker::new(&engine);
    host_bindings::MicroClient::add_to_linker::<_, HasSelf<TestHost>>(&mut linker, |host| host)?;
    let create = || -> wasmtime::Result<_> {
        let mut store = Store::new(&engine, TestHost::new());
        let guest = TestHost::start(&mut store, &component, &linker)?;
        let alive = Arc::new(AtomicBool::new(true));
        let placement = store
            .data_mut()
            .table
            .push(Placement::synthetic(alive.clone()))?;
        let surface = guest
            .townframe_dayframe_presentation_events()
            .call_mount(&mut store, Resource::new_borrow(placement.rep()), &[])?
            .unwrap();
        Ok((store, guest, surface))
    };
    let (mut first_store, first_guest, first_surface) = create()?;
    let (mut second_store, second_guest, second_surface) = create()?;
    let expected_batches = expected_counter_batches(2);
    let element = counter_click_element(&expected_batches[0]);
    let click = |revision| click_event(revision, element);
    let first_events = first_guest.townframe_dayframe_presentation_events();
    let second_events = second_guest.townframe_dayframe_presentation_events();
    first_events.call_dispatch(
        &mut first_store,
        Resource::new_borrow(first_surface.rep()),
        &click(1),
    )?;
    second_events.call_dispatch(
        &mut second_store,
        Resource::new_borrow(second_surface.rep()),
        &click(1),
    )?;
    // No `?` inside this chain: an early return would hold the store borrow across the dispatch
    // calls that follow.
    let first_before_close = first_store
        .data()
        .table
        .get(&first_surface)
        .expect("surface is live")
        .batches()
        .to_vec();
    let second_after_first_click = second_store.data().table.get(&second_surface)?.batches();
    assert_batches(&first_before_close, &second_after_first_click);
    let mut malformed = click(2);
    malformed.data_json = "invalid JSON".into();
    let error = first_events
        .call_dispatch(
            &mut first_store,
            Resource::new_borrow(first_surface.rep()),
            &malformed,
        )
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::UnreachableCodeReached)
    );
    second_events.call_dispatch(
        &mut second_store,
        Resource::new_borrow(second_surface.rep()),
        &click(2),
    )?;
    assert_batches(
        &first_store.data().table.get(&first_surface)?.batches(),
        &first_before_close,
    );
    assert_batches(
        &second_store.data().table.get(&second_surface)?.batches(),
        &expected_batches[..3],
    );
    Ok(())
}

/// A host wired to the real Blitz backend, with the counter component ready to instantiate.
struct BlitzRig {
    store: Store<TestHost>,
    component: Component,
    linker: Linker<TestHost>,
}

impl BlitzRig {
    fn new() -> wasmtime::Result<Self> {
        let engine = Engine::default();
        let component = Component::new(&engine, counter_component())?;
        let mut linker = Linker::new(&engine);
        host_bindings::MicroClient::add_to_linker::<_, HasSelf<TestHost>>(&mut linker, |host| {
            host
        })?;
        Ok(Self {
            store: Store::new(&engine, TestHost::new()),
            component,
            linker,
        })
    }

    fn spawn(&mut self) -> wasmtime::Result<host_bindings::MicroClient> {
        TestHost::start(&mut self.store, &self.component, &self.linker)
    }

    fn grant(&mut self) -> Resource<Placement> {
        self.store.data_mut().grant_blitz_placement()
    }

    fn counter_text(&self, placement: &Resource<Placement>) -> String {
        self.store.data().counter_text(placement)
    }

    fn has_click_listener(&self, placement: &Resource<Placement>) -> bool {
        self.store.data().has_click_listener(placement)
    }

    fn buttons(&self, placement: &Resource<Placement>) -> Vec<NodeId> {
        self.store.data().buttons(placement)
    }

    /// Dispatches a click on `target` in the Blitz document by resolving the owning surface and
    /// `data-dioxus-id` element, releasing the document borrow, and invoking the guest's
    /// `presentation-events.dispatch` (which re-entrantly applies mutations into the document).
    fn click(
        &mut self,
        guest: &host_bindings::MicroClient,
        target: NodeId,
    ) -> wasmtime::Result<
        host_bindings::exports::townframe::dayframe::presentation_events::EventResponse,
    > {
        let (rep, event) = self
            .store
            .data()
            .resolve_click(target)
            .expect("target node must belong to a mounted Blitz surface with a listener");
        guest
            .townframe_dayframe_presentation_events()
            .call_dispatch(&mut self.store, Resource::new_borrow(rep), &event)
    }
}

#[test]
fn blitz_backend_renders_a_guest_component_into_a_real_tree() -> wasmtime::Result<()> {
    let mut rig = BlitzRig::new()?;
    let guest = rig.spawn()?;
    let events = guest.townframe_dayframe_presentation_events();
    let placement = rig.grant();
    let _surface = events
        .call_mount(&mut rig.store, Resource::new_borrow(placement.rep()), &[])?
        .unwrap();

    // The initial batch built a real retained subtree with a registered listener, not a blob.
    assert_eq!(rig.counter_text(&placement), "0");
    assert!(rig.has_click_listener(&placement));
    let button = rig.buttons(&placement)[0];

    // Real Blitz DOM clicks on the button node update that same node in place across re-entrant apply.
    for expected in ["1", "2"] {
        let response = rig.click(&guest, button)?;
        assert!(!response.prevent_default && !response.stop_propagation);
        assert_eq!(rig.counter_text(&placement), expected);
        assert_eq!(rig.buttons(&placement), vec![button]);
    }
    Ok(())
}

#[test]
fn two_surfaces_in_one_document_keep_independent_subtrees() -> wasmtime::Result<()> {
    let mut rig = BlitzRig::new()?;
    let first = rig.spawn()?;
    let second = rig.spawn()?;
    let first_events = first.townframe_dayframe_presentation_events();
    let second_events = second.townframe_dayframe_presentation_events();

    let first_placement = rig.grant();
    let second_placement = rig.grant();
    let _first_surface = first_events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[],
        )?
        .unwrap();
    let _second_surface = second_events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(second_placement.rep()),
            &[],
        )?
        .unwrap();

    assert_eq!(rig.counter_text(&first_placement), "0");
    assert_eq!(rig.counter_text(&second_placement), "0");

    let first_button = rig.buttons(&first_placement)[0];
    let second_button = rig.buttons(&second_placement)[0];
    assert_ne!(first_button, second_button);

    // Both guests address their button as element 1 (`data-dioxus-id="1"`). Routing by Blitz NodeId
    // and per-surface `DioxusState` keeps them apart.
    for _ in 0..2 {
        rig.click(&first, first_button)?;
    }
    rig.click(&second, second_button)?;

    assert_eq!(rig.counter_text(&first_placement), "2");
    assert_eq!(rig.counter_text(&second_placement), "1");
    Ok(())
}

#[test]
fn wasm_one_incarnation_shares_state_across_live_surfaces_and_remounts() -> wasmtime::Result<()> {
    let mut rig = BlitzRig::new()?;
    let guest = rig.spawn()?;
    let events = guest.townframe_dayframe_presentation_events();
    let first_placement = rig.grant();
    let second_placement = rig.grant();
    let first = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[],
        )?
        .unwrap();
    let second = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(second_placement.rep()),
            &[],
        )?
        .unwrap();
    let first_identity = presentation::HostSurface::identity(
        rig.store.data_mut(),
        Resource::new_borrow(first.rep()),
    )?;
    let second_identity = presentation::HostSurface::identity(
        rig.store.data_mut(),
        Resource::new_borrow(second.rep()),
    )?;
    assert_ne!(first_identity, second_identity);
    assert_eq!(rig.store.data().routes[&first_identity].len(), 2);
    assert_eq!(rig.store.data().routes[&second_identity].len(), 2);
    let first_button = rig.buttons(&first_placement)[0];
    let second_button = rig.buttons(&second_placement)[0];
    rig.click(&guest, first_button)?;
    assert_eq!(rig.counter_text(&first_placement), "1");
    assert_eq!(rig.counter_text(&second_placement), "1");
    rig.click(&guest, second_button)?;
    assert_eq!(rig.counter_text(&first_placement), "2");
    assert_eq!(rig.counter_text(&second_placement), "2");
    events.call_unmount(&mut rig.store, Resource::new_borrow(first.rep()))?;
    assert_eq!(rig.store.data().routes[&first_identity].len(), 1);
    presentation::HostSurface::drop(rig.store.data_mut(), first)?;
    assert!(rig.buttons(&first_placement).is_empty());
    assert_eq!(rig.store.data().states.len(), 1);
    assert!(!rig.store.data().routes.contains_key(&first_identity));
    rig.click(&guest, second_button)?;
    assert_eq!(rig.counter_text(&second_placement), "3");
    assert_eq!(rig.buttons(&second_placement), [second_button]);

    let remounted = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[],
        )?
        .unwrap();
    let remounted_identity = presentation::HostSurface::identity(
        rig.store.data_mut(),
        Resource::new_borrow(remounted.rep()),
    )?;
    assert_ne!(remounted_identity, first_identity);
    assert_ne!(remounted_identity, second_identity);
    assert_eq!(rig.counter_text(&first_placement), "3");
    let remounted_button = rig.buttons(&first_placement)[0];
    rig.click(&guest, remounted_button)?;
    assert_eq!(rig.counter_text(&first_placement), "4");
    assert_eq!(rig.counter_text(&second_placement), "4");
    events.call_unmount(&mut rig.store, Resource::new_borrow(second.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), second)?;
    events.call_unmount(&mut rig.store, Resource::new_borrow(remounted.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), remounted)?;
    assert!(rig.store.data().states.is_empty());
    assert!(rig.store.data().routes.is_empty());

    let last = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[],
        )?
        .unwrap();
    assert_eq!(rig.counter_text(&first_placement), "4");
    let last_button = rig.buttons(&first_placement)[0];
    rig.click(&guest, last_button)?;
    assert_eq!(rig.counter_text(&first_placement), "5");
    events.call_unmount(&mut rig.store, Resource::new_borrow(last.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), last)?;
    assert!(rig.store.data().states.is_empty());
    assert!(rig.store.data().routes.is_empty());
    Ok(())
}

#[test]
fn wasm_revoked_target_does_not_trap_or_invalidate_live_siblings() -> wasmtime::Result<()> {
    let mut rig = BlitzRig::new()?;
    let guest = rig.spawn()?;
    let events = guest.townframe_dayframe_presentation_events();
    let first_placement = rig.grant();
    let second_placement = rig.grant();
    let first = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[],
        )?
        .unwrap();
    let second = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(second_placement.rep()),
            &[],
        )?
        .unwrap();
    let first_identity = presentation::HostSurface::identity(
        rig.store.data_mut(),
        Resource::new_borrow(first.rep()),
    )?;
    rig.store
        .data()
        .table
        .get(&first_placement)?
        .alive
        .store(false, Ordering::Release);
    let button = rig.buttons(&second_placement)[0];
    rig.click(&guest, button)?;
    assert_eq!(rig.counter_text(&second_placement), "1");
    assert_eq!(rig.counter_text(&first_placement), "0");
    assert_eq!(rig.store.data().routes[&first_identity].len(), 1);
    rig.click(&guest, button)?;
    assert_eq!(rig.counter_text(&second_placement), "2");
    events.call_unmount(&mut rig.store, Resource::new_borrow(first.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), first)?;
    assert!(rig.buttons(&first_placement).is_empty());
    assert!(matches!(
        events.call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[]
        )?,
        Err(PresentationError::Revoked),
    ));
    let replacement_placement = rig.grant();
    let replacement = events
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(replacement_placement.rep()),
            &[],
        )?
        .unwrap();
    assert_eq!(rig.counter_text(&replacement_placement), "2");
    let replacement_button = rig.buttons(&replacement_placement)[0];
    rig.click(&guest, replacement_button)?;
    assert_eq!(rig.counter_text(&replacement_placement), "3");
    assert_eq!(rig.counter_text(&second_placement), "3");
    events.call_unmount(&mut rig.store, Resource::new_borrow(second.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), second)?;
    events.call_unmount(&mut rig.store, Resource::new_borrow(replacement.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), replacement)?;
    assert!(rig.store.data().states.is_empty());
    assert!(rig.store.data().routes.is_empty());
    Ok(())
}

#[test]
fn dropping_a_blitz_surface_withdraws_its_subtree_without_disturbing_siblings()
-> wasmtime::Result<()> {
    let mut rig = BlitzRig::new()?;
    let first = rig.spawn()?;
    let second = rig.spawn()?;

    let first_placement = rig.grant();
    let second_placement = rig.grant();
    let first_surface = first
        .townframe_dayframe_presentation_events()
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(first_placement.rep()),
            &[],
        )?
        .unwrap();
    let _second_surface = second
        .townframe_dayframe_presentation_events()
        .call_mount(
            &mut rig.store,
            Resource::new_borrow(second_placement.rep()),
            &[],
        )?
        .unwrap();

    let first_button = rig.buttons(&first_placement)[0];
    let first_text_child = rig
        .store
        .data()
        .doc
        .as_ref()
        .unwrap()
        .get_node(first_button)
        .unwrap()
        .children[0];
    // Clicking the child text node inside the button bubbles up to the button's `data-dioxus-id`.
    rig.click(&first, first_text_child)?;
    assert_eq!(rig.counter_text(&first_placement), "1");

    // Dropping the first surface removes its rendered nodes from the Blitz document while leaving
    // the sibling surface intact and interactive.
    first
        .townframe_dayframe_presentation_events()
        .call_unmount(&mut rig.store, Resource::new_borrow(first_surface.rep()))?;
    presentation::HostSurface::drop(rig.store.data_mut(), first_surface)?;
    assert!(rig.buttons(&first_placement).is_empty());
    assert_eq!(rig.counter_text(&second_placement), "0");

    let second_button = rig.buttons(&second_placement)[0];
    rig.click(&second, second_button)?;
    assert_eq!(rig.counter_text(&second_placement), "1");
    Ok(())
}
