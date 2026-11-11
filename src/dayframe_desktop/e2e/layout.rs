//! The Wasm layout on Wasmtime (ADR 031 §13), through the real `Application`: the layout guest
//! arranges the panels the counter (a Wasm guest) and the sample (a native built-in) contribute
//! through their manifests. It replaces the old pushed-component `slots` scenario: the layout
//! mounts other micros' presentations through capabilities it redeems itself, so what that
//! scenario proved (interactive children, one child's withdrawal not touching the others) is
//! proven here on the capability path.
use super::composed;
use super::press_platform_back;
use super::HeadlessDom;
use super::Launch;
use crate::composition::assemble;
use crate::composition::desktop_runtime;
use crate::composition::CompositionRuntime;
use clay_docs::FacetRoute;
use clay_host::MicroState;
use clay_mdk::MicroId;
use dayframe_composition::HostInputs;

const COUNTER_PANEL: &str = "section[aria-label=\"Counter (Wasm) panel\"]";
const SAMPLE_PANEL: &str = "section[aria-label=\"Sample micro panel\"]";
const COUNTER_INCREMENT: &str =
    "section[aria-label=\"Counter (Wasm) panel\"] button[aria-label=\"Increment Wasm counter\"]";
const SAMPLE_FLAG: &str =
    "section[aria-label=\"Sample micro panel\"] button[aria-label$=\"sample configuration flag\"]";
const SHOW_COUNTER: &str = "button[aria-label=\"Show Counter (Wasm) panel\"]";
const HIDE_COUNTER: &str = "button[aria-label=\"Hide Counter (Wasm) panel\"]";
const HIDE_SAMPLE: &str = "button[aria-label=\"Hide Sample micro panel\"]";
const SHOW_SAMPLE: &str = "button[aria-label=\"Show Sample micro panel\"]";
const OPEN_LAYOUT: &str = "button[aria-label=\"Open Layout\"]";

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

/// What the lifecycle section's toggle does (it is not on screen beside the layout, which fills
/// the main position): write the counter's install datum, which the reconciler applies.
fn enable_counter(runtime: &CompositionRuntime, enabled: bool) {
    // The acknowledgement resolves once the datum is stored; the scenarios wait on the UI.
    drop(runtime.set_enabled(&MicroId::local(micro_counter_manifest::ID), enabled));
}

struct Layout {
    runtime: CompositionRuntime,
    ui: HeadlessDom,
    counter: MicroId,
    host: std::rc::Rc<std::cell::RefCell<Option<HostInputs>>>,
}

impl Layout {
    /// Boot the shell with the layout enabled and `visible` as the stored arrangement, and open
    /// the Layout panel from home. Settings' counter section is not mounted (it would be a second
    /// mount of the counter's view, and a second holder of the guest). The counter package stays
    /// disabled, as discovered.
    async fn start(visible: &[&str]) -> Self {
        let runtime = desktop_runtime(std::sync::Arc::new(clay_store::MemoryStore::new()))
            .await
            .unwrap();
        Self::start_on(runtime, visible).await
    }

    /// [`Self::start`] over a runtime the caller assembled.
    async fn start_on(runtime: CompositionRuntime, visible: &[&str]) -> Self {
        // Before serving, nothing drives the persister, so the acknowledgement is dropped.
        drop(runtime.set_enabled(&MicroId::local(micro_layout_manifest::ID), true));
        runtime.documents().mock_documents().seed(
            arrangement_route(),
            serde_json::json!({ "visible": visible }),
        );
        let launch = Launch::new(runtime.clone(), super::WIDE_PX);
        let host = launch.host.clone();
        let mut ui = HeadlessDom::new(composed, launch);
        ui.wait_until("home offers the layout", |ui| {
            ui.find(OPEN_LAYOUT).is_some()
        })
        .await;
        ui.click(OPEN_LAYOUT);
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
            host,
        }
    }
}

/// The desktop composition with its packages on `wcmp` over Wasmi, the engine the browser
/// build runs (over the page's own `WebAssembly` instead of Wasmi).
async fn wasmi_runtime() -> CompositionRuntime {
    assemble(
        std::sync::Arc::new(clay_store::MemoryStore::new()),
        |resources, id, manifest, bindings, bytes| {
            clay_mdk::host::wcmp_factory(resources, id, manifest, bindings, move || {
                let bytes = bytes.clone();
                async move {
                    let engine = wcmp::Engine::with_backend(wcmp_wasm_core_wasmi::Wasmi::new())
                        .map_err(|error| error.to_string())?;
                    clay_rt_wcmp::WcmpPackage::load(engine, &bytes).await
                }
            })
        },
    )
    .await
    .unwrap()
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
        ..
    } = Layout::start(&["dayframe.sample"]).await;
    ui.wait_until("the sample panel is mounted", |ui| {
        ui.find(SAMPLE_FLAG).is_some()
    })
    .await;
    assert_eq!(runtime.state(&counter), MicroState::Disabled);
    enable_counter(&runtime, true);
    ui.wait_until("the counter is enabled and its panel is offered", |ui| {
        ui.find(SHOW_COUNTER).is_some()
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

// The same walk on `wcmp` (Wasmi here, the page's `WebAssembly` in the browser), leaving the
// layout and opening it again. A task the guest spawns from a synchronous entry (a panel's slot
// created while the layout renders inside `mount`) is only polled once the engine's driver
// ticks the store, and nothing but the entry itself knew it had to: each of the two mounts
// waited out the feed's long poll (30 virtual seconds) before its panels appeared. The clock is
// paused, so it advances only when every task is parked on a timer.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn returning_to_the_wcmp_layout_mounts_its_panels_without_waiting_for_a_timer() {
    let started = tokio::time::Instant::now();
    let Layout {
        runtime,
        mut ui,
        counter,
        host,
    } = Layout::start_on(
        wasmi_runtime().await,
        &["dayframe.sample", "dayframe.counter"],
    )
    .await;
    enable_counter(&runtime, true);
    ui.wait_until("the first incarnation mounted both panels", |ui| {
        ui.find(SAMPLE_FLAG).is_some() && ui.find(COUNTER_INCREMENT).is_some()
    })
    .await;
    press_platform_back(&host);
    ui.wait_until("home again with the layout's guest released", |ui| {
        ui.find(OPEN_LAYOUT).is_some() && runtime.state(&counter) == MicroState::Idle
    })
    .await;
    ui.click(OPEN_LAYOUT);
    ui.wait_until("the new incarnation mounted both panels", |ui| {
        ui.find(SAMPLE_FLAG).is_some() && ui.find(COUNTER_INCREMENT).is_some()
    })
    .await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "returning to the layout waited for a timer: {:?}",
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
    enable_counter(&runtime, true);
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
        ..
    } = Layout::start(&["dayframe.counter", "dayframe.sample"]).await;
    enable_counter(&runtime, true);
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

    enable_counter(&runtime, false);
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

    enable_counter(&runtime, true);
    ui.wait_until("the counter's panel is back on a new incarnation", |ui| {
        ui.find(COUNTER_INCREMENT).is_some()
    })
    .await;
    assert_eq!(ui.text(COUNTER_INCREMENT), (before + 1).to_string());
}

// Hiding a panel withdraws its section and releases its provider, which closes the provider when
// nothing else holds it. That must leave the layout, which does not depend on the sample's
// lifetime, mounted and interactive: the panel can be shown and hidden again, round after round.
// Found by the Bombadil exploration of the browser build (`src/dayframe_web/bombadil/shell.ts`):
// the layout's whole region became "Component withdrawn." when the sample's panel was hidden,
// because a guest task of the layout was suspended in the middle of its callback when the host
// delivered a document change, and the task hook gave both the same stack (`clay_mdk`'s
// `task_stacks`). Whether the two overlap depends on how the host's tasks interleave, so each
// round is another chance to see it; the unit tests of `task_stacks` pin the hook's side.
#[tokio::test(flavor = "current_thread")]
async fn hiding_a_native_panel_leaves_the_layout_mounted() {
    let Layout { mut ui, .. } = Layout::start_on(wasmi_runtime().await, &["dayframe.sample"]).await;
    ui.wait_until("the sample's panel is mounted", |ui| {
        ui.find(SAMPLE_FLAG).is_some()
    })
    .await;
    for _round in 0..6 {
        ui.click(HIDE_SAMPLE);
        ui.wait_until("the sample's panel is gone", |ui| {
            ui.find(SAMPLE_PANEL).is_none()
        })
        .await;
        ui.wait_until("the layout offers the panel again", |ui| {
            ui.find(SHOW_SAMPLE).is_some()
        })
        .await;
        ui.click(SHOW_SAMPLE);
        ui.wait_until("the sample's panel is mounted again", |ui| {
            ui.find(SAMPLE_FLAG).is_some()
        })
        .await;
    }
}

// The lab (a Wasm guest) mounts as a layout panel and lists its experiments; opening the mock
// review and walking its journeys down to a file renders the diff (`FileDiffView`, an ordinary
// Dioxus component) with the file's annotation thread inside the micro. This is the stress test
// of the guest surface: every tag, attribute and stylesheet rule the diff emits crosses it.
#[tokio::test(flavor = "current_thread")]
async fn the_lab_panel_lists_its_experiments_and_the_mock_review_renders_a_diff() {
    const PANEL: &str = "section[aria-label=\"Lab panel\"]";
    let Layout {
        runtime, mut ui, ..
    } = Layout::start(&["dayframe.lab"]).await;
    drop(runtime.set_enabled(&MicroId::local(micro_lab_manifest::ID), true));
    let inside = |selector: &str| format!("{PANEL} {selector}");
    let open_review = inside("button[aria-label=\"Open Mock review\"]");
    ui.wait_until("the lab lists its experiments", |ui| {
        ui.find(&open_review).is_some()
    })
    .await;
    ui.click(&open_review);
    let open_change = inside("button[aria-label=\"Open change Cap the retry backoff\"]");
    ui.wait_until("the mock review lists its changes", |ui| {
        ui.find(&open_change).is_some()
    })
    .await;
    ui.click(&open_change);
    let open_file = inside("button[aria-label=\"Open file src/retry.rs\"]");
    ui.wait_until("the overview lists the change's files", |ui| {
        ui.find(&open_file).is_some()
    })
    .await;
    assert!(
        ui.text(&open_file).contains("+5 -2"),
        "{}",
        ui.text(&open_file)
    );
    ui.click(&open_file);
    let open_thread = inside("button[aria-label=\"Open thread on line 6\"]");
    // A refused batch faults the guest and withdraws its panel, so fail on the fault (which
    // names the refusal) instead of waiting for a thread that can no longer appear.
    let lab = MicroId::local(micro_lab_manifest::ID);
    ui.wait_until("the diff renders with its annotation thread", |ui| {
        if let MicroState::Faulted(fault) = runtime.state(&lab) {
            panic!("the lab faulted rendering the diff: {fault}");
        }
        ui.find(&open_thread).is_some()
    })
    .await;
    ui.click(&open_thread);
    ui.wait_until("the thread opens", |ui| {
        ui.find(&inside("ol[aria-label=\"Comments\"]")).is_some()
    })
    .await;
}
