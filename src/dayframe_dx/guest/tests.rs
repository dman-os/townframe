//! Recording renderer probe: exercises the actual component ABI, not the Blitz renderer.
//!
//! The host replays each typed batch into a Rust `WriteMutations`. That is the same path the
//! Blitz-backed native-dom writer takes, so what this test proves about ordering and content holds
//! for the real renderer; only tree semantics differ.
use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use wasmtime::component::{Component, HasSelf, Linker, Resource, ResourceTable};
use wasmtime::{Engine, Store};

mod host_bindings {
    wasmtime::component::bindgen!({
        path: "../app/wit",
        world: "presentation-client",
        imports: { default: trappable },
        additional_derives: [PartialEq],
        with: {
            "townframe:dayframe/presentation.surface": super::Surface,
            "townframe:dayframe/presentation.mount-point": super::Placement,
        },
    });
}

use blitz_dom::{BaseDocument, DocumentConfig, LocalName, NodeId, QualName, ns};
use dioxus_core::{BorrowedAttributeValue, ElementId, WriteMutations};
use dioxus_native_dom::DioxusState;
use host_bindings::exports::townframe::dayframe::presentation_events::Event;
use host_bindings::townframe::dayframe::presentation::{self, *};

/// Replays one wire batch into any host `WriteMutations`, in order.
fn replay(batch: &[Mutation], to: &mut impl WriteMutations) {
    let element = |id: u32| ElementId::from_raw(id as usize);
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

pub struct Surface {
    alive: Arc<AtomicBool>,
    backend: Backend,
}

impl Surface {
    fn batches(&self) -> &[Vec<Mutation>] {
        match &self.backend {
            Backend::Recording(batches) => batches,
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
    states: BTreeMap<u32, DioxusState>,
}

impl TestHost {
    fn new() -> Self {
        Self {
            table: ResourceTable::new(),
            doc: None,
            states: BTreeMap::new(),
        }
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
            EventData, SerializedMouseData,
            geometry::{ClientPoint, Coordinates, ElementPoint, PagePoint, ScreenPoint},
            input_data::{MouseButton, decode_mouse_button_set},
             Modifiers,
        };

        let doc = self.doc.as_ref()?;
        let mut current = Some(target);
        let (rep, node_id, element_id) = loop {
            let node_id = current?;
            let node = doc.get_node(node_id)?;
            if let Some(raw_id) = node
                .attr(LocalName::from("data-dioxus-id"))
                .and_then(|value| value.parse::<usize>().ok())
            {
                let element_id = ElementId::from_raw(raw_id);
                if let Some((&rep, _)) = self
                    .states
                    .iter()
                    .find(|(_, state)| state.try_element_to_node_id(element_id) == Some(node_id))
                {
                    break (rep, node_id, element_id);
                }
            }
            current = node.parent;
        };

        let surface = self.table.get(&Resource::<Surface>::new_borrow(rep)).ok()?;
        let revision = match surface.backend {
            Backend::Blitz { revisions } => revisions,
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

impl presentation::Host for TestHost {}
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
        let surface = self.table.push(Surface { alive, backend })?;
        if let Some(root) = root {
            let previous = self.states.insert(surface.rep(), DioxusState::create(root));
            assert!(previous.is_none(), "surface rep reused while still mounted");
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
        edits: Vec<Mutation>,
    ) -> wasmtime::Result<Result<u64, PresentationError>> {
        let Self { table, doc, states } = self;
        let rep = surface.rep();
        let surface = table.get_mut(&surface)?;
        if !surface.alive.load(Ordering::Acquire) {
            return Ok(Err(PresentationError::Revoked));
        }
        match &mut surface.backend {
            Backend::Recording(batches) => {
                let mut writer = RecordingWriter::default();
                replay(&edits, &mut writer);
                batches.push(writer.0);
                Ok(Ok(batches.len().try_into().unwrap()))
            }
            Backend::Blitz { revisions } => {
                let state = states
                    .get_mut(&rep)
                    .expect("a Blitz surface always has tree state");
                let doc = doc.as_mut().expect("a Blitz placement implies a document");
                let mut writer = state.writer(doc);
                replay(&edits, &mut writer);
                *revisions += 1;
                Ok(Ok(*revisions))
            }
        }
    }
    fn drop(&mut self, surface: Resource<Surface>) -> wasmtime::Result<()> {
        if let Some(state) = self.states.remove(&surface.rep())
            && let Some(doc) = self.doc.as_mut()
        {
            let root = state.element_to_node_id(ElementId::ROOT);
            let mut mutator = doc.mutate();
            for child in mutator.child_ids(root) {
                mutator.remove_and_drop_node(child);
            }
        }
        self.table.delete(surface)?;
        Ok(())
    }
}

fn counter_component() -> Vec<u8> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let output = Command::new("cargo")
        .current_dir(&directory)
        .args([
            "build",
            "-p",
            "dayframe_guest",
            "--example",
            "counter",
            "--target",
            "wasm32-unknown-unknown",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "guest build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    // Ask Cargo for its configured target directory; never choose a separate target directory.
    let metadata = Command::new("cargo")
        .current_dir(&directory)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .unwrap();
    assert!(
        metadata.status.success(),
        "{}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    let metadata: serde_json::Value = serde_json::from_slice(&metadata.stdout).unwrap();
    let core = PathBuf::from(metadata["target_directory"].as_str().unwrap())
        .join("wasm32-unknown-unknown/debug/examples/counter.wasm");
    let output = Command::new("wasm-tools")
        .args(["component", "new"])
        .arg(core)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "component encoding failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn click_event(revision: u64) -> Event {
    let html = dioxus_html::HtmlEvent {
        element: dioxus_core::ElementId::from_raw(1),
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

#[test]
fn event_envelope_roundtrips_through_dioxus_without_a_second_payload_schema() {
    let event = click_event(1);
    let json = serde_json::json!({"element": event.element, "name": event.name,
        "bubbles": event.bubbles, "data": serde_json::from_str::<serde_json::Value>(&event.data_json).unwrap()});
    let html: dioxus_html::HtmlEvent = serde_json::from_value(json).unwrap();
    assert_eq!(
        html,
        dioxus_html::HtmlEvent {
            element: dioxus_core::ElementId::from_raw(1),
            name: "click".into(),
            bubbles: true,
            data: dioxus_html::EventData::Mouse(dioxus_html::SerializedMouseData::default()),
        }
    );
}

/// The same counter rendered natively through a host `WriteMutations`. If the guest's batches
/// match this, the wire preserved Dioxus's edit stream exactly across the address space.
fn expected_counter_batches(event_count: u64) -> Vec<Vec<Mutation>> {
    use dioxus::prelude::*;
    use dioxus_core::{RenderTargetId, VirtualDom};
    use dioxus_html::{EventData, SerializedHtmlEventConverter, SerializedMouseData};

    fn counter() -> Element {
        let mut count = use_signal(|| 0_u64);
        rsx! {
            button {
                onclick: move |_| count += 1,
                "{count}"
            }
        }
    }

    dioxus_html::set_event_converter(Box::new(SerializedHtmlEventConverter));
    let mut dom = VirtualDom::new(counter);
    let mut batches = Vec::with_capacity(event_count as usize + 1);
    let mut writer = RecordingWriter::default();
    dom.rebuild(&mut writer);
    batches.push(std::mem::take(&mut writer.0));
    for _ in 0..event_count {
        let event = dioxus_core::Event::new(
            EventData::Mouse(SerializedMouseData::default()).into_any(),
            true,
        );
        dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            event,
            ElementId::from_raw(1),
        );
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
    presentation::add_to_linker::<_, HasSelf<TestHost>>(&mut linker, |host| host)?;
    let mut store = Store::new(&engine, TestHost::new());
    let first = host_bindings::PresentationClient::instantiate(&mut store, &component, &linker)?;
    let events = first.townframe_dayframe_presentation_events();
    let denied = store
        .data_mut()
        .table
        .push(Placement::synthetic(Arc::new(AtomicBool::new(false))))?;
    assert!(matches!(
        events.call_mount(&mut store, Resource::new_borrow(denied.rep()))?,
        Err(PresentationError::Revoked)
    ));
    let first_lifetime = Arc::new(AtomicBool::new(true));
    let placement = store
        .data_mut()
        .table
        .push(Placement::synthetic(first_lifetime.clone()))?;
    let surface = events
        .call_mount(&mut store, Resource::new_borrow(placement.rep()))?
        .unwrap();
    let expected_batches = expected_counter_batches(4);
    let click = click_event;
    for revision in 1..=2 {
        let response = events.call_dispatch(
            &mut store,
            Resource::new_borrow(surface.rep()),
            &click(revision),
        )?;
        assert!(!response.prevent_default && !response.stop_propagation);
    }
    assert_eq!(
        store.data().table.get(&surface)?.batches(),
        expected_batches[..3].to_vec()
    );

    // A second instance of the same compiled component has a separate counter/address space.
    let second = host_bindings::PresentationClient::instantiate(&mut store, &component, &linker)?;
    let second_events = second.townframe_dayframe_presentation_events();
    let second_placement = store
        .data_mut()
        .table
        .push(Placement::synthetic(Arc::new(AtomicBool::new(true))))?;
    let second_surface = second_events
        .call_mount(&mut store, Resource::new_borrow(second_placement.rep()))?
        .unwrap();
    second_events.call_dispatch(
        &mut store,
        Resource::new_borrow(second_surface.rep()),
        &click(1),
    )?;
    assert_eq!(
        store.data().table.get(&second_surface)?.batches(),
        expected_batches[..2].to_vec()
    );
    events.call_dispatch(&mut store, Resource::new_borrow(surface.rep()), &click(3))?;
    let expected = expected_batches[..4].to_vec();
    assert_eq!(store.data().table.get(&surface)?.batches(), expected);

    // Revocation is enforced at the imported operation, not trusted to the guest's event handler.
    first_lifetime.store(false, Ordering::Release);
    let error = events
        .call_dispatch(&mut store, Resource::new_borrow(surface.rep()), &click(4))
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<wasmtime::Trap>(),
        Some(&wasmtime::Trap::UnreachableCodeReached)
    );
    assert_eq!(store.data().table.get(&surface)?.batches(), expected);
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
    assert_eq!(
        store.data().table.get(&second_surface)?.batches(),
        expected_batches[..2].to_vec()
    );
    Ok(())
}

#[test]
fn separate_stores_isolate_guest_traps_and_revoke_only_their_own_surface() -> wasmtime::Result<()> {
    let engine = Engine::default();
    let component = Component::new(&engine, counter_component())?;
    let mut linker = Linker::new(&engine);
    presentation::add_to_linker::<_, HasSelf<TestHost>>(&mut linker, |host| host)?;
    let create = || -> wasmtime::Result<_> {
        let mut store = Store::new(&engine, TestHost::new());
        let guest =
            host_bindings::PresentationClient::instantiate(&mut store, &component, &linker)?;
        let alive = Arc::new(AtomicBool::new(true));
        let placement = store
            .data_mut()
            .table
            .push(Placement::synthetic(alive.clone()))?;
        let surface = guest
            .townframe_dayframe_presentation_events()
            .call_mount(&mut store, Resource::new_borrow(placement.rep()))?
            .unwrap();
        Ok((store, guest, surface, alive))
    };
    let (mut first_store, first_guest, first_surface, first_lifetime) = create()?;
    let (mut second_store, second_guest, second_surface, _) = create()?;
    let expected_batches = expected_counter_batches(2);
    let click = click_event;
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
    assert_eq!(
        first_before_close,
        second_store.data().table.get(&second_surface)?.batches()
    );
    first_lifetime.store(false, Ordering::Release);
    let error = first_events
        .call_dispatch(
            &mut first_store,
            Resource::new_borrow(first_surface.rep()),
            &click(2),
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
    assert_eq!(
        first_store.data().table.get(&first_surface)?.batches(),
        first_before_close
    );
    assert_eq!(
        second_store.data().table.get(&second_surface)?.batches(),
        expected_batches[..3].to_vec()
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
        presentation::add_to_linker::<_, HasSelf<TestHost>>(&mut linker, |host| host)?;
        Ok(Self {
            store: Store::new(&engine, TestHost::new()),
            component,
            linker,
        })
    }

    /// Starts another incarnation of the counter. Each owns its virtual DOM, so two surfaces in one
    /// document need two instances.
    fn spawn(&mut self) -> wasmtime::Result<host_bindings::PresentationClient> {
        host_bindings::PresentationClient::instantiate(
            &mut self.store,
            &self.component,
            &self.linker,
        )
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
        guest: &host_bindings::PresentationClient,
        target: NodeId,
    ) -> wasmtime::Result<host_bindings::exports::townframe::dayframe::presentation_events::EventResponse>
    {
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
        .call_mount(&mut rig.store, Resource::new_borrow(placement.rep()))?
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
        .call_mount(&mut rig.store, Resource::new_borrow(first_placement.rep()))?
        .unwrap();
    let _second_surface = second_events
        .call_mount(&mut rig.store, Resource::new_borrow(second_placement.rep()))?
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
fn dropping_a_blitz_surface_withdraws_its_subtree_without_disturbing_siblings()
-> wasmtime::Result<()> {
    let mut rig = BlitzRig::new()?;
    let first = rig.spawn()?;
    let second = rig.spawn()?;

    let first_placement = rig.grant();
    let second_placement = rig.grant();
    let first_surface = first
        .townframe_dayframe_presentation_events()
        .call_mount(&mut rig.store, Resource::new_borrow(first_placement.rep()))?
        .unwrap();
    let _second_surface = second
        .townframe_dayframe_presentation_events()
        .call_mount(&mut rig.store, Resource::new_borrow(second_placement.rep()))?
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
    presentation::HostSurface::drop(rig.store.data_mut(), first_surface)?;
    assert!(rig.buttons(&first_placement).is_empty());
    assert_eq!(rig.counter_text(&second_placement), "0");

    let second_button = rig.buttons(&second_placement)[0];
    rig.click(&second, second_button)?;
    assert_eq!(rig.counter_text(&second_placement), "1");
    Ok(())
}
