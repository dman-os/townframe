use crate::composition::desktop_runtime;
use crate::composition::Application;
use crate::composition::CompositionRuntime;
use blitz_dom::ns;
use blitz_dom::BaseDocument;
use blitz_dom::DocumentConfig;
use blitz_dom::LocalName;
use blitz_dom::NodeId;
use blitz_dom::QualName;
use clay_host::MicroState;
use clay_mdk::MicroId;
use dayframe_composition::HostInputs;
use dayframe_composition::Insets;
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

mod layout;

/// Host widths on either side of the compact breakpoint.
const COMPACT_PX: f64 = 400.0;
pub(crate) const WIDE_PX: f64 = 1200.0;
const WINDOW_HEIGHT_PX: f64 = 800.0;

/// What a launcher hands `Application`: the runtime and its window's width. `host` is filled
/// with the inputs `Application` was given, so a scenario can press the platform back.
#[derive(Clone)]
pub(crate) struct Launch {
    runtime: CompositionRuntime,
    width: f64,
    host: std::rc::Rc<std::cell::RefCell<Option<HostInputs>>>,
}

impl Launch {
    pub(crate) fn new(runtime: CompositionRuntime, width: f64) -> Self {
        Self {
            runtime,
            width,
            host: Default::default(),
        }
    }
}

/// `Application` as a launcher mounts it, in a window `launch.width` logical px wide.
pub(crate) fn composed(launch: Launch) -> Element {
    let runtime = use_signal(|| launch.runtime.clone());
    let width = use_signal(|| launch.width);
    let height = use_signal(|| WINDOW_HEIGHT_PX);
    let insets = use_signal(Insets::default);
    let platform_back = use_signal(|| 0_u64);
    let can_back = use_signal(|| false);
    let host = HostInputs {
        width: width.into(),
        height: height.into(),
        insets: insets.into(),
        platform_back,
        can_back,
        keyboard: false,
        start: None,
    };
    *launch.host.borrow_mut() = Some(host.clone());
    rsx! { Application { runtime, host } }
}

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

const HOME: &str = "[role=\"region\"][aria-label=\"Home\"]";
const OPEN_SETTINGS: &str = "button[aria-label=\"Open Settings\"]";
const SETTINGS: &str = "[role=\"region\"][aria-label=\"Settings\"]";
const BACK: &str = "button[aria-label=\"Back\"]";
/// On a compact host the list stays mounted (covered) under the details, so each carries a back
/// button of its own; the covered one is out of assistive technology's reach, not out of the DOM.
const DETAILS_BACK: &str =
    "[role=\"region\"][aria-label=\"Settings details\"] button[aria-label=\"Back\"]";
const SAMPLE: &str = "button[aria-label=\"Open Sample micro settings\"]";
const COUNTER: &str = "button[aria-label=\"Open Counter (Wasm) settings\"]";
const COUNTER_VALUE: &str = "button[aria-label=\"Increment Wasm counter\"]";
const TOOLBAR_INCREMENT: &str = "[role=\"toolbar\"] button[aria-label=\"Increment\"]";

/// The platform back gesture, as a launcher reports it.
fn press_platform_back(host: &std::cell::RefCell<Option<HostInputs>>) {
    let mut presses = host
        .borrow()
        .as_ref()
        .expect("the application mounted")
        .platform_back;
    presses += 1;
}

struct Shell {
    runtime: CompositionRuntime,
    ui: HeadlessDom,
    counter: MicroId,
    host: std::rc::Rc<std::cell::RefCell<Option<HostInputs>>>,
}

impl Shell {
    /// Boot the desktop composition in a window `width` px wide, showing home.
    async fn start(width: f64) -> Self {
        let runtime = desktop_runtime(std::sync::Arc::new(clay_store::MemoryStore::new()))
            .await
            .unwrap();
        // A discovered package is admitted disabled; these scenarios exercise the counter, so
        // enable it the way a user does, through its install datum. The reconciler applies the
        // datum when the shell starts serving.
        // Before serving, nothing drives the persister, so the acknowledgement is dropped.
        drop(runtime.set_enabled(&MicroId::local(micro_counter_manifest::ID), true));
        let launch = Launch::new(runtime.clone(), width);
        let host = std::rc::Rc::clone(&launch.host);
        let mut ui = HeadlessDom::new(composed, launch);
        ui.wait_until("home lists Settings", |ui| {
            ui.find(HOME).is_some() && ui.find(OPEN_SETTINGS).is_some()
        })
        .await;
        Self {
            runtime,
            ui,
            counter: MicroId::local(micro_counter_manifest::ID),
            host,
        }
    }

    /// Boot at a wide window and open Settings from home.
    async fn start_in_settings() -> Self {
        let mut shell = Self::start(WIDE_PX).await;
        shell.open_settings().await;
        shell
    }

    /// Press Settings on home and wait for its sections to be listed.
    async fn open_settings(&mut self) {
        self.ui.click(OPEN_SETTINGS);
        self.ui
            .wait_until("Settings sections listed", |ui| {
                ui.find(SAMPLE).is_some() && ui.find(COUNTER).is_some()
            })
            .await;
    }
}

// Home lists Settings from its declared row and opening it is what starts it; going back from
// it returns to home and releases it. The whole walk at a compact width, where the section's
// details come over the list: Home, Settings, a section, back to the list, back to home.
#[tokio::test(flavor = "current_thread")]
async fn home_to_settings_to_a_section_and_back_at_a_compact_width() {
    let settings = MicroId::local(micro_settings_manifest::ID);
    let shell = Shell::start(COMPACT_PX).await;
    let host = std::rc::Rc::clone(&shell.host);
    let Shell {
        runtime,
        mut ui,
        counter,
        ..
    } = shell;
    assert_eq!(runtime.state(&settings), MicroState::Idle);
    ui.click(OPEN_SETTINGS);
    ui.wait_until("Settings list over home", |ui| {
        ui.find(COUNTER).is_some() && ui.find(HOME).is_none() && ui.find(SETTINGS).is_some()
    })
    .await;
    assert_eq!(runtime.state(&settings), MicroState::Active);

    ui.click(COUNTER);
    ui.wait_until("the counter section is the details", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);
    assert_eq!(
        ui.text("section[aria-label=\"Counter (Wasm)\"] h2"),
        "Counter (Wasm)"
    );

    // Back dismisses the details: the guest is released and the list is still there, mounted
    // beneath, so Settings never stopped.
    ui.click(DETAILS_BACK);
    ui.wait_until("details gone, list showing", |ui| {
        ui.find(COUNTER_VALUE).is_none() && ui.find(COUNTER).is_some()
    })
    .await;
    ui.wait_until("nothing holds the counter, so it idles", |_| {
        runtime.state(&counter) == MicroState::Idle
    })
    .await;
    assert_eq!(runtime.state(&settings), MicroState::Active);

    // The platform's back gesture lands on the same stack as the button.
    press_platform_back(&host);
    ui.wait_until("home again", |ui| ui.find(HOME).is_some())
        .await;
    ui.wait_until("leaving Settings released it", |_| {
        runtime.state(&settings) == MicroState::Idle
    })
    .await;
    assert!(ui.find(BACK).is_none(), "home is the base of the stack");
}

// The same walk in a window wide enough for the list and the details side by side: opening a
// section does not hide the list, so the sections stay listed and another can replace the first.
#[tokio::test(flavor = "current_thread")]
async fn home_to_settings_to_a_section_and_back_at_a_wide_width() {
    let Shell {
        runtime,
        mut ui,
        counter,
        ..
    } = Shell::start(WIDE_PX).await;
    ui.click(OPEN_SETTINGS);
    ui.wait_until("Settings listed beside the sidebar", |ui| {
        ui.find(COUNTER).is_some() && ui.find("button[aria-label=\"Show Home\"]").is_some()
    })
    .await;
    ui.click(COUNTER);
    ui.wait_until("the counter section beside the list", |ui| {
        ui.find(COUNTER_VALUE).is_some() && ui.find(SAMPLE).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);

    // Another row replaces the details, and one back leaves the details altogether.
    ui.click(SAMPLE);
    ui.wait_until("the sample section replaced the counter's", |ui| {
        ui.find(COUNTER_VALUE).is_none()
            && ui
                .find("button[aria-label=\"Disable sample configuration flag\"]")
                .is_some()
    })
    .await;
    ui.click(BACK);
    ui.wait_until("details gone", |ui| {
        ui.find("button[aria-label=\"Disable sample configuration flag\"]")
            .is_none()
            && ui.find(COUNTER).is_some()
    })
    .await;

    // The sidebar goes home without back.
    ui.click("button[aria-label=\"Show Home\"]");
    ui.wait_until("home", |ui| ui.find(HOME).is_some()).await;
}

// Opening the counter's Settings section is what starts its Wasm guest: the section is a
// manifest-declared row whose presentation is a lazy endpoint (ADR 031 §7).
#[tokio::test(flavor = "current_thread")]
async fn opening_the_counter_section_starts_the_guest_and_leaving_it_idles_the_guest() {
    let Shell {
        runtime,
        mut ui,
        counter,
        ..
    } = Shell::start_in_settings().await;
    // Open the sample's section first, with the counter's section unmounted and its guest idle.
    ui.click(SAMPLE);
    ui.wait_until("sample section opened, counter idle", |ui| {
        ui.find("button[aria-label=\"Disable sample configuration flag\"]")
            .is_some()
            && runtime.state(&counter) == MicroState::Idle
    })
    .await;
    assert_eq!(
        runtime.state(&MicroId::local(micro_sample::ID)),
        MicroState::Active
    );

    ui.click(COUNTER);
    ui.wait_until("counter section mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);
    assert_eq!(
        ui.text("section[aria-label=\"Counter (Wasm)\"] h2"),
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
        ui.find(COUNTER_VALUE).is_none()
            && ui
                .find("button[aria-label=\"Disable sample configuration flag\"]")
                .is_some()
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
    } = Shell::start_in_settings().await;
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
        ..
    } = Shell::start_in_settings().await;
    ui.click(COUNTER);
    ui.wait_until("counter section mounted", |ui| {
        ui.find(COUNTER_VALUE).is_some()
    })
    .await;

    // The lifecycle section would replace the counter's in the details position, so the micro
    // is disabled through the install datum its toggle writes.
    drop(runtime.set_enabled(&counter, false));
    ui.wait_until("disabled counter withdrew its rendered section", |ui| {
        ui.find(COUNTER_VALUE).is_none()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Disabled);
    // The manifest row outlives the micro's incarnations: the section is still listed.
    assert!(ui.find(COUNTER).is_some());
    assert_eq!(
        runtime.state(&MicroId::local(micro_sample::ID)),
        MicroState::Active
    );

    // The section stayed open while its micro was disabled, and its slot re-redeems when the
    // supervisor records the micro enabled: a fresh incarnation mounts without reopening.
    drop(runtime.set_enabled(&counter, true));
    ui.wait_until("reenabled counter section recovered", |ui| {
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

// The lifecycle controls are a Settings section: its toggle writes the install datum, the
// supervisor follows, and its status line reads the micro's state.
#[tokio::test(flavor = "current_thread")]
async fn the_lifecycle_section_enables_and_disables_a_micro() {
    let Shell {
        runtime,
        mut ui,
        counter,
        ..
    } = Shell::start_in_settings().await;
    ui.click("button[aria-label=\"Open Micro lifecycle settings\"]");
    let disable = "button[aria-label=\"Disable dayframe.counter micro\"]";
    let enable = "button[aria-label=\"Enable dayframe.counter micro\"]";
    ui.wait_until("the counter is listed enabled", |ui| ui.find(disable).is_some())
        .await;
    ui.click(disable);
    ui.wait_until("the counter is disabled", |ui| ui.find(enable).is_some())
        .await;
    assert_eq!(runtime.state(&counter), MicroState::Disabled);
    ui.click(enable);
    ui.wait_until("the counter is enabled again", |ui| {
        ui.find(disable).is_some()
    })
    .await;
    // The row of the opened section is the current one in the list.
    assert_eq!(
        ui.attribute(
            "button[aria-label=\"Open Micro lifecycle settings\"]",
            "aria-current"
        )
        .as_deref(),
        Some("true")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn the_sample_section_edits_its_configuration() {
    let Shell { mut ui, .. } = Shell::start_in_settings().await;
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

// The toolbar's Increment button is a manifest-declared row contributed by the counter package,
// attached to a lazy endpoint to the guest's `increment` call. It renders, pinned outside the
// shell's panels, with the Wasm guest stopped; pressing it starts the guest on Wasmtime for the
// length of the call, and the increment is visible once the counter's section is opened.
#[tokio::test(flavor = "current_thread")]
async fn the_toolbar_button_increments_a_stopped_wasm_counter() {
    let Shell {
        runtime,
        mut ui,
        counter,
        ..
    } = Shell::start(WIDE_PX).await;
    // Home is showing, so nothing mounts the counter's section and the guest idles; the pinned
    // toolbar is on screen.
    ui.wait_until("the toolbar lists the counter's button", |ui| {
        ui.find(TOOLBAR_INCREMENT).is_some()
    })
    .await;
    // Rendering the button read its lazy reference without starting the guest.
    assert_eq!(runtime.state(&counter), MicroState::Idle);

    ui.click(TOOLBAR_INCREMENT);
    ui.wait_until("the press ran and the guest idled again", |ui| {
        ui.attribute(TOOLBAR_INCREMENT, "disabled").is_none()
            && runtime.state(&counter) == MicroState::Idle
    })
    .await;

    ui.click(OPEN_SETTINGS);
    ui.wait_until("Settings listed", |ui| ui.find(COUNTER).is_some())
        .await;
    ui.click(COUNTER);
    ui.wait_until("the counter shows the toolbar's increment", |ui| {
        ui.find(COUNTER_VALUE).is_some() && ui.text(COUNTER_VALUE) == "1"
    })
    .await;
}

// Pins a defect in blitz-dom 0.3.0-beta.4 (`DocumentMutator::process_added_subtree` records every
// added form-associated element in `form_nodes`; `remove_and_drop_node` never forgets them; `flush`
// then calls `reset_form_owner(id)`, which indexes the node slab with an id that was already freed,
// `form.rs:38`). A renderer that mounts a button and unmounts it within one mutation batch hits it,
// which Dioxus does whenever a render creates a subtree that the same diff then replaces.
#[test]
#[should_panic(expected = "invalid SlotMap key")]
fn blitz_flush_resets_the_form_owner_of_a_button_dropped_in_the_same_batch() {
    let mut document = BaseDocument::new(DocumentConfig::default());
    let mut mutator = document.mutate();
    let button = mutator.create_element(
        QualName {
            prefix: None,
            ns: ns!(html),
            local: LocalName::from("button"),
        },
        vec![],
    );
    let document_root = mutator.doc.root_node().id;
    mutator.append_children(document_root, &[button]);
    mutator.remove_and_drop_node(button);
    // The mutator's drop flushes.
    drop(mutator);
}
