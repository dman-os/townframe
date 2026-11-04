use crate::composition::desktop_runtime;
use crate::composition::Application;
use crate::composition::CompositionRuntime;
use blitz_dom::ns;
use blitz_dom::BaseDocument;
use blitz_dom::DocumentConfig;
use blitz_dom::LocalName;
use blitz_dom::NodeId;
use blitz_dom::QualName;
use clay_host::MicroId;
use clay_host::MicroState;
use dioxus::prelude::*;
use dioxus_core::ElementId;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_html::geometry::ClientPoint;
use dioxus_html::geometry::Coordinates;
use dioxus_html::geometry::ElementPoint;
use dioxus_html::geometry::PagePoint;
use dioxus_html::geometry::ScreenPoint;
use dioxus_html::input_data::decode_mouse_button_set;
use dioxus_html::input_data::MouseButton;
use dioxus_html::EventData;
use dioxus_html::Modifiers;
use dioxus_html::SerializedHtmlEventConverter;
use dioxus_html::SerializedMouseData;
use dioxus_native_dom::DioxusState;

struct HeadlessDom {
    dom: VirtualDom,
    document: BaseDocument,
    state: DioxusState,
    root: NodeId,
}

impl HeadlessDom {
    fn new<P: Clone + 'static>(component: fn(P) -> Element, props: P) -> Self {
        dioxus_html::set_event_converter(Box::new(SerializedHtmlEventConverter));
        let mut document = BaseDocument::new(DocumentConfig::default());
        let root = {
            let mut mutator = document.mutate();
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
            root
        };
        let mut state = DioxusState::create(root);
        let mut dom = VirtualDom::new_with_props(component, props);
        dom.rebuild(&mut state.writer(&mut document));
        Self {
            dom,
            document,
            state,
            root,
        }
    }

    fn connected_descendant(&self, target: NodeId) -> bool {
        if target == self.root {
            return false;
        }
        let mut current = target;
        let mut within_mount = false;
        loop {
            within_mount |= current == self.root;
            if current == self.document.root_node().id {
                return within_mount;
            }
            let Some(parent) = self.document.get_node(current).and_then(|node| node.parent) else {
                return false;
            };
            if !self
                .document
                .get_node(parent)
                .expect("parent node exists")
                .children
                .contains(&current)
            {
                return false;
            }
            current = parent;
        }
    }

    fn find(&self, selector: &str) -> Option<NodeId> {
        let matches: Vec<_> = self
            .document
            .query_selector_all_in(self.root, selector)
            .expect("valid DOM selector")
            .into_vec()
            .into_iter()
            .filter(|&node| self.connected_descendant(node))
            .collect();
        assert!(
            matches.len() <= 1,
            "selector {selector:?} matched multiple connected descendants: {matches:?}"
        );
        matches.first().copied()
    }

    fn one(&self, selector: &str) -> NodeId {
        self.find(selector)
            .unwrap_or_else(|| panic!("selector {selector:?} matched no connected descendant"))
    }

    fn text(&self, selector: &str) -> String {
        self.document
            .get_node(self.one(selector))
            .expect("selected node exists")
            .text_content()
    }

    fn attribute(&self, selector: &str, name: &str) -> Option<String> {
        self.document
            .get_node(self.one(selector))
            .expect("selected node exists")
            .attr(LocalName::from(name))
            .map(str::to_owned)
    }

    fn click(&mut self, selector: &str) {
        eprintln!("DOM click: {selector}");
        let target = self.one(selector);
        self.click_node(target);
    }

    fn click_node(&mut self, target: NodeId) {
        assert!(
            self.connected_descendant(target),
            "click target must be a connected descendant of the mounted application"
        );
        let (element, mouse) = {
            let mut current = target;
            let (element, blitz) = loop {
                let node = self
                    .document
                    .get_node(current)
                    .expect("target ancestor exists");
                if let Some(raw) = node.attr(LocalName::from("data-dioxus-id")) {
                    let element = ElementId::from_raw(raw.parse().expect("valid host Dioxus id"));
                    assert_eq!(
                        self.state.try_element_to_node_id(element),
                        Some(current),
                        "DOM target must reverse-map to the current host Dioxus node"
                    );
                    break (element, node.synthetic_click_event_data(Modifiers::empty()));
                }
                assert_ne!(
                    current, self.root,
                    "click target has no host Dioxus mapping"
                );
                current = node.parent.expect("connected ancestor has a parent");
            };
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
            (element, mouse)
        };
        self.dom.runtime().handle_event_for_target(
            RenderTargetId::ROOT,
            "click",
            dioxus_core::Event::new(EventData::Mouse(mouse).into_any(), true),
            element,
        );
    }

    async fn wait_until(&mut self, checkpoint: &str, ready: impl Fn(&Self) -> bool) {
        eprintln!("composed DOM checkpoint: {checkpoint}");
        loop {
            self.dom
                .render_immediate(&mut self.state.writer(&mut self.document));
            if ready(self) {
                return;
            }
            self.dom.wait_for_work().await;
        }
    }
}

const SETTINGS: &str = "section[aria-label=\"Settings\"]";
const SAMPLE: &str = "button[aria-label=\"Open Sample micro settings\"]";
const COUNTER: &str = "button[aria-label=\"Open Counter (Wasm) settings\"]";
const COUNTER_VALUE: &str = "button[aria-label=\"Increment Wasm counter\"]";

struct Shell {
    runtime: CompositionRuntime,
    ui: HeadlessDom,
    counter: MicroId,
}

impl Shell {
    async fn start() -> Self {
        fn composed(runtime: CompositionRuntime) -> Element {
            let runtime = use_signal(move || runtime);
            rsx! { Application { runtime } }
        }

        let runtime = desktop_runtime(std::sync::Arc::new(clay_store::MemoryStore::new()))
            .await
            .unwrap();
        // A discovered package is admitted disabled; these scenarios exercise the counter, so
        // enable it the way a user does, through its install datum. The reconciler applies the
        // datum when the shell starts serving.
        // Before serving, nothing drives the persister, so the acknowledgement is dropped.
        drop(runtime.set_enabled(&MicroId::new(micro_counter_manifest::ID), true));
        let mut ui = HeadlessDom::new(composed, runtime.clone());
        ui.wait_until("Settings sections listed", |ui| {
            ui.find(SAMPLE).is_some() && ui.find(COUNTER).is_some()
        })
        .await;
        Self {
            runtime,
            ui,
            counter: MicroId::new(micro_counter_manifest::ID),
        }
    }
}

// Opening the counter's Settings section is what starts its Wasm guest: the section is a
// manifest-declared row whose presentation is a lazy endpoint (ADR 025 §7).
#[tokio::test(flavor = "current_thread")]
async fn opening_the_counter_section_starts_the_guest_and_leaving_it_idles_the_guest() {
    let Shell {
        runtime,
        mut ui,
        counter,
    } = Shell::start().await;
    // Sections sort by micro id, so the counter's opens first; select the sample's to start
    // with the counter's section unmounted and its guest idle.
    ui.click(SAMPLE);
    ui.wait_until("sample section selected, counter idle", |ui| {
        ui.attribute(SAMPLE, "aria-pressed").as_deref() == Some("true")
            && runtime.state(&counter) == MicroState::Idle
    })
    .await;
    assert_eq!(
        runtime.state(&MicroId::new(micro_sample::ID)),
        MicroState::Active
    );

    ui.click(COUNTER);
    ui.wait_until("counter section mounted", |ui| {
        ui.attribute(COUNTER, "aria-pressed").as_deref() == Some("true")
            && ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);
    assert_eq!(
        ui.text("section[aria-label=\"Settings\"] h3"),
        "Counter (Wasm)"
    );
    assert_eq!(ui.text(COUNTER_VALUE), "0");

    // A click on the button's label child reaches the guest exactly once.
    let button = ui.one(COUNTER_VALUE);
    let child = *ui
        .document
        .get_node(button)
        .unwrap()
        .children
        .first()
        .expect("counter button has a label child");
    ui.click_node(child);
    ui.wait_until("child-targeted host click reached guest once", |ui| {
        ui.text(COUNTER_VALUE) == "1"
    })
    .await;
    assert_eq!(ui.one(COUNTER_VALUE), button);
    ui.click(COUNTER_VALUE);
    ui.wait_until("button-targeted host click reached guest once", |ui| {
        ui.text(COUNTER_VALUE) == "2"
    })
    .await;

    ui.click(SAMPLE);
    ui.wait_until("switched away from the counter section", |ui| {
        ui.attribute(SAMPLE, "aria-pressed").as_deref() == Some("true")
            && ui.find(COUNTER_VALUE).is_none()
    })
    .await;
    ui.wait_until("nothing holds the counter, so it idles", |_| {
        runtime.state(&counter) == MicroState::Idle
    })
    .await;

    ui.click(COUNTER);
    ui.wait_until("counter section mounted again", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);
    let before = ui.text(COUNTER_VALUE).parse::<u64>().unwrap();
    ui.click(COUNTER_VALUE);
    ui.wait_until("the restarted guest is interactive", |ui| {
        ui.text(COUNTER_VALUE) == (before + 1).to_string()
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn external_document_changes_reach_the_counter_guest() {
    let Shell {
        runtime, mut ui, ..
    } = Shell::start().await;
    ui.click(COUNTER);
    ui.wait_until("counter section mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    runtime.documents().mock_documents().external_change(
        clay_docs::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: micro_counter_manifest::CONFIG_TAG.into(),
            key_id: micro_counter_manifest::CONFIG_KEY.into(),
        },
        Some(serde_json::json!(41)),
    );
    ui.wait_until("external document update reached the guest mirror", |ui| {
        ui.text(COUNTER_VALUE) == "41"
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn disabling_the_counter_withdraws_its_section_until_it_is_enabled() {
    let Shell {
        runtime,
        mut ui,
        counter,
    } = Shell::start().await;
    ui.click(COUNTER);
    ui.wait_until("counter section mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;

    ui.click("button[aria-label=\"Close Settings\"]");
    ui.wait_until("Settings closed", |ui| {
        ui.find(SETTINGS).is_none() && ui.find("button[aria-label=\"Open Settings\"]").is_some()
    })
    .await;
    ui.click("button[aria-label=\"Open Settings\"]");
    ui.wait_until("Settings reopened", |ui| {
        ui.find(SAMPLE).is_some() && ui.find(COUNTER).is_some()
    })
    .await;

    ui.click("button[aria-label=\"Disable dayframe.counter micro\"]");
    ui.wait_until("disabled counter withdrew its rendered section", |ui| {
        ui.find(COUNTER_VALUE).is_none()
            && ui
                .find("button[aria-label=\"Enable dayframe.counter micro\"]")
                .is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Disabled);
    // The manifest row outlives the micro's incarnations: the section is still listed.
    assert!(ui.find(COUNTER).is_some());
    assert_eq!(
        runtime.state(&MicroId::new(micro_sample::ID)),
        MicroState::Active
    );

    ui.click("button[aria-label=\"Enable dayframe.counter micro\"]");
    ui.wait_until("reenabled counter can be disabled again", |ui| {
        ui.find("button[aria-label=\"Disable dayframe.counter micro\"]")
            .is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Idle);
    // Reopening the section starts a fresh incarnation.
    ui.click(SAMPLE);
    ui.wait_until("switched to the sample section", |ui| {
        ui.attribute(SAMPLE, "aria-pressed").as_deref() == Some("true")
    })
    .await;
    ui.click(COUNTER);
    ui.wait_until("new counter incarnation mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);
    let before = ui.text(COUNTER_VALUE).parse::<u64>().unwrap();
    ui.click(COUNTER_VALUE);
    ui.wait_until("new incarnation is interactive", |ui| {
        ui.text(COUNTER_VALUE) == (before + 1).to_string()
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn the_sample_section_edits_its_configuration() {
    let Shell { mut ui, .. } = Shell::start().await;
    ui.click(SAMPLE);
    ui.wait_until("sample section mounted", |ui| {
        ui.find("button[aria-label=\"Disable sample configuration flag\"]")
            .is_some()
    })
    .await;
    ui.click("button[aria-label=\"Disable sample configuration flag\"]");
    ui.wait_until("sample flag toggled", |ui| {
        ui.find("button[aria-label=\"Enable sample configuration flag\"]")
            .is_some()
    })
    .await;
}
