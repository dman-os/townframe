//! The Wasm layout on Wasmtime (ADR 031 §13), through the real `Application`: the layout guest
//! arranges the panels the counter (a Wasm guest) and the sample (a native built-in) contribute
//! through their manifests. It replaces the old pushed-component `slots` scenario: the layout
//! mounts other micros' presentations through capabilities it redeems itself, so what that
//! scenario proved (interactive children, one child's withdrawal not touching the others) is
//! proven here on the capability path.
use super::composed;
use super::HeadlessDom;
use super::Launch;
use crate::composition::desktop_runtime;
use crate::composition::CompositionRuntime;
use clay_docs::FacetRoute;
use clay_host::MicroState;
use clay_mdk::MicroId;

const COUNTER_PANEL: &str = "section[aria-label=\"Counter (Wasm) panel\"]";
const SAMPLE_PANEL: &str = "section[aria-label=\"Sample micro panel\"]";
const COUNTER_INCREMENT: &str =
    "section[aria-label=\"Counter (Wasm) panel\"] button[aria-label=\"Increment Wasm counter\"]";
const SAMPLE_FLAG: &str =
    "section[aria-label=\"Sample micro panel\"] button[aria-label$=\"sample configuration flag\"]";
const SHOW_COUNTER: &str = "button[aria-label=\"Show Counter (Wasm) panel\"]";
const HIDE_COUNTER: &str = "button[aria-label=\"Hide Counter (Wasm) panel\"]";
const SHOW_SAMPLE: &str = "button[aria-label=\"Show Sample micro panel\"]";
const ENABLE_COUNTER: &str = "button[aria-label=\"Enable dayframe.counter micro\"]";
const DISABLE_COUNTER: &str = "button[aria-label=\"Disable dayframe.counter micro\"]";

fn arrangement_route() -> FacetRoute {
    FacetRoute {
        document: format!("{}-config", micro_layout_manifest::CONFIG_NAME),
        branch: "main".into(),
        tag: micro_layout_manifest::CONFIG_TAG.into(),
        key_id: micro_layout_manifest::CONFIG_KEY.into(),
    }
}

/// The stored arrangement, as the layout's document holds it.
fn stored_arrangement(runtime: &CompositionRuntime) -> Option<serde_json::Value> {
    runtime
        .documents()
        .mock_documents()
        .get(&arrangement_route())
}

struct Layout {
    runtime: CompositionRuntime,
    ui: HeadlessDom,
    counter: MicroId,
}

impl Layout {
    /// Boot the shell with the layout enabled and `visible` as the stored arrangement. Home is
    /// showing, so Settings' counter section is not mounted (it would be a second mount of the
    /// counter's view, and a second holder of the guest). The counter package stays disabled,
    /// as discovered.
    async fn start(visible: &[&str]) -> Self {
        let runtime = desktop_runtime(std::sync::Arc::new(clay_store::MemoryStore::new()))
            .await
            .unwrap();
        // Before serving, nothing drives the persister, so the acknowledgement is dropped.
        drop(runtime.set_enabled(&MicroId::local(micro_layout_manifest::ID), true));
        runtime.documents().mock_documents().seed(
            arrangement_route(),
            serde_json::json!({ "visible": visible }),
        );
        let mut ui = HeadlessDom::new(composed, Launch::new(runtime.clone(), super::WIDE_PX));
        // The toggles list the installed panels, which the guest reads from its mirror of the
        // collection: they appear once the layout is running and bootstrapped.
        ui.wait_until("the layout lists the installed panels", |ui| {
            ui.find(SHOW_SAMPLE).is_some() || ui.find(SAMPLE_PANEL).is_some()
        })
        .await;
        Self {
            runtime,
            ui,
            counter: MicroId::local(micro_counter_manifest::ID),
        }
    }
}

// Showing a panel is what starts its provider: a panel that is installed but not shown is only a
// row, which the layout reads without redeeming it.
//
// The clock is paused, so it only advances when every task is parked on a timer: the layout
// guest's collection feed is always parked on its long poll (30 s), and a guest task that is
// spawned by a synchronous entry (a panel's slot created while the layout renders inside
// `mount`) once waited for that poll to expire before its first poll. Here that would be 30
// virtual seconds, which the final assertion rejects deterministically.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_hidden_panel_never_activates_its_provider_until_it_is_shown() {
    let started = tokio::time::Instant::now();
    let Layout {
        runtime,
        mut ui,
        counter,
    } = Layout::start(&["dayframe.sample"]).await;
    ui.wait_until("the sample panel is mounted", |ui| {
        ui.find(SAMPLE_FLAG).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Disabled);
    ui.click(ENABLE_COUNTER);
    ui.wait_until("the counter is enabled and its panel is offered", |ui| {
        ui.find(DISABLE_COUNTER).is_some() && ui.find(SHOW_COUNTER).is_some()
    })
    .await;
    ui.wait_until("an enabled counter nothing holds is idle", |_| {
        runtime.state(&counter) == MicroState::Idle
    })
    .await;
    // The layout is mounted with its toggle for the counter on screen, so a panel redeemed
    // eagerly would hold the guest `Active` by now.
    assert_eq!(runtime.state(&counter), MicroState::Idle);
    assert!(ui.find(COUNTER_PANEL).is_none());

    ui.click(SHOW_COUNTER);
    ui.wait_until("the counter panel mounted", |ui| {
        ui.find(COUNTER_INCREMENT).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);
    assert_eq!(
        stored_arrangement(&runtime),
        Some(serde_json::json!({ "visible": ["dayframe.sample", "dayframe.counter"] }))
    );

    ui.click(HIDE_COUNTER);
    ui.wait_until("the counter panel withdrawn and its guest released", |ui| {
        ui.find(COUNTER_PANEL).is_none() && runtime.state(&counter) == MicroState::Idle
    })
    .await;
    assert_eq!(
        stored_arrangement(&runtime),
        Some(serde_json::json!({ "visible": ["dayframe.sample"] }))
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "a layout step waited for a timer: {:?}",
        started.elapsed()
    );
}

// An arrangement outlives what it names: an id nothing is registered under, and an id whose
// provider is unavailable, each show a placeholder and neither edits the stored arrangement.
#[tokio::test(flavor = "current_thread")]
async fn an_unregistered_or_unavailable_panel_is_a_placeholder_and_the_arrangement_is_untouched() {
    let visible = ["dayframe.counter", "ghost"];
    let Layout {
        runtime, mut ui, ..
    } = Layout::start(&visible).await;
    let stored = serde_json::json!({ "visible": visible });
    ui.wait_until("both placeholders shown", |ui| {
        ui.find("section[aria-label=\"ghost panel\"] p[role=\"status\"]")
            .is_some()
            && ui
                .find("section[aria-label=\"Counter (Wasm) panel\"] p[role=\"alert\"]")
                .is_some()
    })
    .await;
    assert_eq!(
        ui.text("section[aria-label=\"ghost panel\"] p[role=\"status\"]"),
        "Panel ghost is not installed."
    );
    assert!(ui
        .text("section[aria-label=\"Counter (Wasm) panel\"] p[role=\"alert\"]")
        .contains("unavailable"));
    assert_eq!(stored_arrangement(&runtime), Some(stored.clone()));

    // Enabling the provider mounts its panel where the placeholder was; the unregistered id
    // stays a placeholder.
    ui.click(ENABLE_COUNTER);
    ui.wait_until("the counter's panel replaced its placeholder", |ui| {
        ui.find(COUNTER_INCREMENT).is_some()
    })
    .await;
    assert!(ui
        .find("section[aria-label=\"ghost panel\"] p[role=\"status\"]")
        .is_some());
    assert_eq!(stored_arrangement(&runtime), Some(stored));
}

// A mounted panel is a live presentation of its provider, and one provider leaving takes only
// its own panel away.
#[tokio::test(flavor = "current_thread")]
async fn a_mounted_panel_is_interactive_and_disabling_one_provider_leaves_the_others_mounted() {
    let Layout {
        runtime,
        mut ui,
        counter,
    } = Layout::start(&["dayframe.counter", "dayframe.sample"]).await;
    ui.click(ENABLE_COUNTER);
    ui.wait_until("both panels mounted", |ui| {
        ui.find(COUNTER_INCREMENT).is_some() && ui.find(SAMPLE_FLAG).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Active);

    let before = ui.text(COUNTER_INCREMENT).parse::<u64>().unwrap();
    ui.click(COUNTER_INCREMENT);
    ui.wait_until("a click in the panel reached the counter guest", |ui| {
        ui.text(COUNTER_INCREMENT) == (before + 1).to_string()
    })
    .await;

    ui.click(DISABLE_COUNTER);
    ui.wait_until("the counter's panel is a placeholder", |ui| {
        ui.find(COUNTER_INCREMENT).is_none()
            && ui
                .find("section[aria-label=\"Counter (Wasm) panel\"] p[role=\"alert\"]")
                .is_some()
    })
    .await;
    // The sample's panel stayed mounted and is still interactive.
    assert_eq!(
        ui.attribute(SAMPLE_FLAG, "aria-label").as_deref(),
        Some("Disable sample configuration flag")
    );
    ui.click(SAMPLE_FLAG);
    ui.wait_until("the sample's panel handled a click", |ui| {
        ui.attribute(SAMPLE_FLAG, "aria-label").as_deref()
            == Some("Enable sample configuration flag")
    })
    .await;

    ui.click(ENABLE_COUNTER);
    ui.wait_until("the counter's panel is back on a new incarnation", |ui| {
        ui.find(COUNTER_INCREMENT).is_some()
    })
    .await;
    assert_eq!(ui.text(COUNTER_INCREMENT), (before + 1).to_string());
}
