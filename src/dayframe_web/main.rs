//! Web composition shell for Dayframe (ADR 018 §7): engine-neutral composition plus the
//! `wcmp` counter guest, fetched as a dx asset during activation (never `include_bytes!`).
use clay_dioxus::NativeComponents;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroFactory;
use dioxus::prelude::*;
use ui::ThemeMode;
use ui::ThemeRoot;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

/// The counter component artifact the composition fetches at activation. Placed next to the
/// host module by `x/dayframe-web.ts` (the `DAYFRAME_COUNTER_COMPONENT` path from
/// `x/check-dayframe.ts --prepare-only`); a guest change does not rebuild the host.
const COUNTER_COMPONENT_URL: &str = "counter.component.wasm";

async fn fetch_component_bytes(url: &str) -> Result<Vec<u8>, String> {
    let window = web_sys::window().expect("the shell runs in a window");
    let response = JsFuture::from(window.fetch_with_str(url))
        .await
        .map_err(|error| format!("fetching {url}: {error:?}"))?
        .dyn_into::<web_sys::Response>()
        .expect("fetch resolves to a Response");
    if !response.ok() {
        return Err(format!("fetching {url}: HTTP {}", response.status()));
    }
    let buffer = JsFuture::from(
        response
            .array_buffer()
            .map_err(|error| format!("reading {url}: {error:?}"))?,
    )
    .await
    .map_err(|error| format!("reading {url}: {error:?}"))?;
    Ok(js_sys::Uint8Array::new(&buffer).to_vec())
}

/// Compile the fetched counter component on the browser's `WebAssembly` engine through `wcmp`.
/// Re-activation after disable/enable re-runs this factory; each incarnation re-fetches because
/// `WcmpPackage` is not `Clone` and only lives for that activation.
async fn counter_package() -> clay_rt_wcmp::WcmpPackage {
    let bytes = fetch_component_bytes(COUNTER_COMPONENT_URL)
        .await
        .expect("counter component must be fetchable");
    let backend = wcmp_wasm_core_web::Web::new();
    let engine = wcmp::Engine::with_backend(backend).expect("browser WebAssembly engine");
    clay_rt_wcmp::WcmpPackage::load(engine, &bytes)
        .await
        .expect("fetched counter component must be valid")
}

/// Engine providers this shell links into the shared composition: the counter on `wcmp`.
pub fn guest_providers(application: &ApplicationRuntime<NativeComponents>) -> Vec<MicroFactory> {
    vec![clay_mdk::wcmp::provider(
        application,
        micro_counter_manifest::manifest(),
        counter_package,
    )]
}

pub fn web_runtime() -> dayframe_composition::CompositionRuntime {
    dayframe_composition::CompositionRuntime::new(guest_providers)
}

fn main() {
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let runtime = use_signal(web_runtime);
    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                dayframe_composition::Application { runtime }
            }
        }
    }
}
