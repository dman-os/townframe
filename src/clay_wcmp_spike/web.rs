//! The browser page of the spike: fetch the counter component, activate it on `wcmp` over the
//! browser's `WebAssembly` engine, and mount its surface through `clay_dioxus` on `dioxus-web`.
//!
//! `x/spike-wcmp-web.ts` serves this module next to `counter.component.wasm` and drives it from
//! Playwright. The page reports progress through `#spike-status`: `loading`, then `ready` once the
//! surface is mounted, or `failed: <reason>` (with `role="alert"`) if fetching, compiling, or
//! activating failed. A guest call that suspends panics in [`crate::first_poll`], which the driver
//! sees as a page error.
use crate::WcmpPackage;
use crate::WcmpRuntime;
use clay_dioxus::ComponentMount;
use clay_dioxus::NativeComponents;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::Instance;
use clay_surface::SurfaceSink;
use dioxus::prelude::*;
use std::sync::Arc;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_futures::JsFuture;

const COMPONENT_URL: &str = "counter.component.wasm";

#[wasm_bindgen(start)]
pub fn start() {
    dioxus::launch(App);
}

#[derive(Clone)]
struct Mounted {
    components: NativeComponents,
    viewer: Instance,
    reference: ComponentRef,
}

async fn fetch_component() -> Result<Vec<u8>, String> {
    let window = web_sys::window().expect("the spike runs in a window");
    let response = JsFuture::from(window.fetch_with_str(COMPONENT_URL))
        .await
        .map_err(|error| format!("fetching {COMPONENT_URL}: {error:?}"))?
        .dyn_into::<web_sys::Response>()
        .expect("fetch resolves to a Response");
    if !response.ok() {
        return Err(format!(
            "fetching {COMPONENT_URL}: HTTP {}",
            response.status()
        ));
    }
    let buffer = JsFuture::from(
        response
            .array_buffer()
            .map_err(|error| format!("reading {COMPONENT_URL}: {error:?}"))?,
    )
    .await
    .map_err(|error| format!("reading {COMPONENT_URL}: {error:?}"))?;
    Ok(js_sys::Uint8Array::new(&buffer).to_vec())
}

async fn mount_counter(host: Host, components: NativeComponents) -> Result<Mounted, String> {
    let bytes = fetch_component().await?;
    let package = WcmpPackage::load(&bytes)
        .await
        .map_err(|error| format!("compiling the component: {error}"))?;
    let owner = host.start("counter");
    let runtime = WcmpRuntime::activate(&host, owner.clone(), &package).await?;
    let reference = components
        .publish_surface(&owner, move |_mount, writer| {
            runtime.mount(Arc::new(writer) as Arc<dyn SurfaceSink<ComponentRef>>, &[])
        })
        .map_err(|error| format!("publishing the counter surface: {error:?}"))?;
    Ok(Mounted {
        components,
        viewer: host.start("viewer"),
        reference,
    })
}

#[component]
fn App() -> Element {
    let host = use_hook(Host::new);
    let components = use_hook(|| NativeComponents::new(&host));
    clay_dioxus::use_component_recovery(components.clone());
    let mounted = use_resource(move || mount_counter(host.clone(), components.clone()));
    let mounted = mounted.read().clone();
    match mounted {
        None => rsx! { p { id: "spike-status", "loading" } },
        Some(Err(error)) => rsx! {
            p { id: "spike-status", role: "alert", "failed: {error}" }
        },
        Some(Ok(mounted)) => rsx! {
            p { id: "spike-status", "ready" }
            div { id: "spike-surface",
                ComponentMount {
                    components: mounted.components,
                    owner: mounted.viewer,
                    reference: mounted.reference,
                }
            }
        },
    }
}
