use crate::composition::Application;
use crate::composition::DesktopRuntime;
use blitz_dom::BaseDocument;
use blitz_dom::DocumentConfig;
use blitz_dom::LocalName;
use blitz_dom::NodeId;
use blitz_dom::QualName;
use blitz_dom::ns;
use clay_dioxus::NativeComponents;
use clay_host::AccessError;
use clay_mdk::wasm::WasmMicro;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::composition::InstanceState;
use dioxus::prelude::*;
use dioxus_core::ElementId;
use dioxus_core::RenderTargetId;
use dioxus_core::VirtualDom;
use dioxus_html::EventData;
use dioxus_html::Modifiers;
use dioxus_html::SerializedHtmlEventConverter;
use dioxus_html::SerializedMouseData;
use dioxus_html::geometry::ClientPoint;
use dioxus_html::geometry::Coordinates;
use dioxus_html::geometry::ElementPoint;
use dioxus_html::geometry::PagePoint;
use dioxus_html::geometry::ScreenPoint;
use dioxus_html::input_data::MouseButton;
use dioxus_html::input_data::decode_mouse_button_set;
use dioxus_native_dom::DioxusState;
use micro_sample::SampleMicro;
use micro_settings::SettingsMicro;

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
const COUNTER_STATUS: &str = "section[aria-label=\"Settings\"] h3 + div p[role=\"status\"]";

type DesktopApplication = ApplicationRuntime<NativeComponents>;

struct SettingsFixture {
    application: DesktopApplication,
    ui: HeadlessDom,
    mock: clay_docs::MockDocuments,
    counter_facet: clay_docs::Facet<u64>,
    counter_route: clay_docs::FacetRoute,
    settings: SettingsMicro,
    sample: SampleMicro,
    first_counter: WasmMicro,
}

impl SettingsFixture {
    async fn start() -> Self {
        fn composed(runtime: DesktopRuntime) -> Element {
            let runtime = use_signal(move || runtime);
            rsx! { Application { runtime } }
        }

        let runtime = DesktopRuntime::new();
        let application = runtime.application.clone();
        let mock = application.mock_documents();
        let counter_facet = application
            .grant_config(
                &application.root(),
                "counter",
                vec![daybook_types::manifest::RoutineFacetAccess {
                    tag: micro_counter_manifest::CONFIG_TAG.into(),
                    key_id: Some(micro_counter_manifest::CONFIG_KEY.into()),
                    read: true,
                    ..Default::default()
                }],
            )
            .unwrap()
            .remove("counter")
            .unwrap()
            .facet::<u64>(
                micro_counter_manifest::CONFIG_TAG.into(),
                micro_counter_manifest::CONFIG_KEY.into(),
            )
            .unwrap();
        let counter_route = clay_docs::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: micro_counter_manifest::CONFIG_TAG.into(),
            key_id: micro_counter_manifest::CONFIG_KEY.into(),
        };
        let mut ui = HeadlessDom::new(composed, runtime);
        ui.wait_until("Settings contributions mounted", |ui| {
            ui.find(SAMPLE).is_some() && ui.find(COUNTER).is_some()
        })
        .await;
        let settings =
            clay_mdk::native::output::<SettingsMicro, _>(&application, "settings").unwrap();
        let sample = clay_mdk::native::output::<SampleMicro, _>(&application, "sample").unwrap();
        let first_counter = application.output::<WasmMicro>("counter").unwrap();
        Self {
            application,
            ui,
            mock,
            counter_facet,
            counter_route,
            settings,
            sample,
            first_counter,
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn composed_settings_wasm_dom_flow() {
    let SettingsFixture {
        application,
        mut ui,
        mock,
        counter_facet,
        counter_route,
        first_counter,
        ..
    } = SettingsFixture::start().await;

    ui.click(COUNTER);
    ui.wait_until("Wasm section selected", |ui| {
        assert_eq!(application.host().check(&first_counter.instance), Ok(()));
        ui.attribute(COUNTER, "aria-pressed").as_deref() == Some("true")
            && ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(
        ui.text("section[aria-label=\"Settings\"] h3"),
        "Counter (Wasm)"
    );
    assert_eq!(ui.text(COUNTER_VALUE), "0");

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
    let pending = counter_facet.watch.current().unwrap();
    assert_eq!(counter_facet.value(&pending), Some(1));
    assert_eq!(pending.pending.len(), 1);
    mock.settle(false);
    ui.wait_until(
        "rejected Wasm write rolled back and delivered its receipt",
        |ui| ui.text(COUNTER_VALUE) == "0" && ui.text(COUNTER_STATUS).contains("RolledBack"),
    )
    .await;

    ui.click(COUNTER_VALUE);
    ui.wait_until("button-targeted host click reached guest once", |ui| {
        ui.text(COUNTER_VALUE) == "1"
    })
    .await;
    mock.settle(true);
    ui.wait_until("accepted Wasm write delivered its receipt", |ui| {
        ui.text(COUNTER_STATUS).contains("Accepted")
    })
    .await;
    assert!(counter_facet.watch.current().unwrap().pending.is_empty());
    mock.external_change(counter_route.clone(), Some(serde_json::json!(41)));
    ui.wait_until("external document update reached the guest mirror", |ui| {
        ui.text(COUNTER_VALUE) == "41"
    })
    .await;
    ui.click(COUNTER_VALUE);
    ui.wait_until("uncertain write was initially optimistic", |ui| {
        ui.text(COUNTER_VALUE) == "42"
    })
    .await;
    let id = *counter_facet
        .watch
        .current()
        .unwrap()
        .pending
        .first()
        .unwrap();
    mock.complete(
        id,
        Err(clay_docs::OperationError::Transport {
            message: "response was lost".into(),
            outcome_unknown: true,
        }),
    );
    ui.wait_until("transport uncertainty survived the WIT receipt", |ui| {
        assert_eq!(application.host().check(&first_counter.instance), Ok(()));
        let value = ui.text(COUNTER_VALUE);
        let status = ui.text(COUNTER_STATUS);
        value == "41" && status.contains("Transport") && status.contains("outcome-unknown: true")
    })
    .await;
    ui.click("button[aria-label=\"Refresh Wasm counter\"]");
    assert_eq!(ui.text(COUNTER_VALUE), "41");
    mock.external_change(counter_route, Some(serde_json::json!(2)));
    ui.wait_until("refreshed guest remains subscribed", |ui| {
        ui.text(COUNTER_VALUE) == "2"
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn composed_settings_wasm_lifecycle_flow() {
    let SettingsFixture {
        application,
        mut ui,
        mock,
        counter_facet,
        counter_route,
        settings,
        sample,
        first_counter,
    } = SettingsFixture::start().await;
    let reports = application.reports();

    ui.click(COUNTER);
    ui.wait_until("Wasm section mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    let button = ui.one(COUNTER_VALUE);
    ui.click(SAMPLE);
    ui.wait_until("switched away from Wasm section", |ui| {
        ui.attribute(SAMPLE, "aria-pressed").as_deref() == Some("true")
            && ui.find(COUNTER_VALUE).is_none()
    })
    .await;
    assert!(!ui.connected_descendant(button));
    ui.click(COUNTER);
    ui.wait_until("Wasm section remounted after switching back", |ui| {
        assert_eq!(
            application.host().check(&first_counter.instance),
            Ok(()),
            "switching sections must not trap or revoke the counter incarnation"
        );
        ui.attribute(COUNTER, "aria-pressed").as_deref() == Some("true")
            && ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    let before = ui.text(COUNTER_VALUE).parse::<u64>().unwrap();
    ui.click(COUNTER_VALUE);
    ui.wait_until("remounted section remains interactive", |ui| {
        ui.text(COUNTER_VALUE) == (before + 1).to_string()
    })
    .await;

    ui.click("button[aria-label=\"Close Settings\"]");
    ui.wait_until("Settings closed", |ui| {
        ui.find(SETTINGS).is_none() && ui.find("button[aria-label=\"Open Settings\"]").is_some()
    })
    .await;
    mock.settle(true);
    mock.external_change(counter_route, Some(serde_json::json!(17)));
    ui.click("button[aria-label=\"Open Settings\"]");
    ui.wait_until("Settings reopened", |ui| {
        ui.find(SAMPLE).is_some() && ui.find(COUNTER).is_some()
    })
    .await;
    ui.click(COUNTER);
    ui.wait_until("Wasm section remounted after reopening Settings", |ui| {
        assert_eq!(
            application.host().check(&first_counter.instance),
            Ok(()),
            "reopening Settings must not trap or revoke the counter incarnation"
        );
        ui.find(COUNTER_VALUE).is_some() && ui.text(COUNTER_VALUE) == "17"
    })
    .await;
    let before = ui.text(COUNTER_VALUE).parse::<u64>().unwrap();
    ui.click(COUNTER_VALUE);
    ui.wait_until("reopened Settings remains interactive", |ui| {
        ui.text(COUNTER_VALUE) == (before + 1).to_string()
    })
    .await;

    mock.settle(true);
    ui.wait_until(
        "accepted edits persisted before disabling the guest",
        |ui| {
            counter_facet.watch.current().unwrap().pending.is_empty()
                && ui.text(COUNTER_STATUS).contains("Accepted")
        },
    )
    .await;
    let persisted_count = before + 1;
    assert_eq!(
        counter_facet.value(&counter_facet.watch.current().unwrap()),
        Some(persisted_count)
    );

    ui.click("button[aria-label=\"Disable counter micro\"]");
    ui.wait_until(
        "disabled counter withdrew its rendered contribution",
        |ui| {
            ui.find(COUNTER).is_none()
                && ui.find(COUNTER_VALUE).is_none()
                && reports.borrow().as_ref().unwrap().get("counter")
                    == Some(&InstanceState::Disabled)
        },
    )
    .await;
    assert_eq!(
        application.host().check(&first_counter.instance),
        Err(AccessError::Revoked)
    );
    assert_eq!(application.host().check(&sample.instance), Ok(()));
    assert_eq!(application.host().check(&settings.instance), Ok(()));
    assert_eq!(
        ui.attribute(SAMPLE, "aria-pressed").as_deref(),
        Some("true")
    );

    ui.click("button[aria-label=\"Enable counter micro\"]");
    ui.wait_until("reenabled counter republished its contribution", |ui| {
        ui.find(COUNTER).is_some()
            && reports.borrow().as_ref().unwrap().get("counter") == Some(&InstanceState::Active)
    })
    .await;
    let second_counter = application.output::<WasmMicro>("counter").unwrap();
    assert!(second_counter.instance != first_counter.instance);
    ui.click(COUNTER);
    ui.wait_until("new Wasm incarnation mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(ui.text(COUNTER_VALUE), persisted_count.to_string());
    ui.click(COUNTER_VALUE);
    ui.wait_until("new Wasm incarnation is interactive", |ui| {
        ui.text(COUNTER_VALUE) == (persisted_count + 1).to_string()
    })
    .await;

    second_counter.exhaust_fuel();
    ui.click(COUNTER_VALUE);
    ui.wait_until("guest trap withdrew only the failed contribution", |ui| {
        ui.find(COUNTER).is_none()
            && ui.find(COUNTER_VALUE).is_none()
            && reports.borrow().as_ref().unwrap().get("counter") == Some(&InstanceState::Stopped)
    })
    .await;
    assert_eq!(
        application.host().check(&second_counter.instance),
        Err(AccessError::Revoked)
    );
    assert_eq!(application.host().check(&sample.instance), Ok(()));
    assert_eq!(application.host().check(&settings.instance), Ok(()));
    assert_eq!(
        ui.attribute(SAMPLE, "aria-pressed").as_deref(),
        Some("true")
    );
    assert_eq!(settings.sections.snapshot().unwrap().len(), 1);
    assert_eq!(counter_facet.watch.current().unwrap().pending.len(), 1);
    mock.settle(true);
    assert_eq!(
        counter_facet.value(&counter_facet.watch.current().unwrap()),
        Some(persisted_count + 1)
    );

    ui.click("button[aria-label=\"Disable sample configuration flag\"]");
    ui.wait_until(
        "native sibling remains interactive after guest trap",
        |ui| {
            ui.find("button[aria-label=\"Enable sample configuration flag\"]")
                .is_some()
        },
    )
    .await;
    assert_eq!(application.host().check(&sample.instance), Ok(()));
    assert_eq!(application.host().check(&settings.instance), Ok(()));
}
