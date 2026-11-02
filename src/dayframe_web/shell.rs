//! Web composition shell for Dayframe (ADR 018 §7): native built-ins linked by this
//! shell plus every package in `packages/index.json`, written by `x/dayframe-web.ts` from
//! each component's statically-read manifest. A package's bytes are fetched only when an
//! instance activates, and their digest is verified against the index before the browser
//! engine (`wcmp`) ever sees them.
use clay_micro_core::micros::MicroId;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use dayframe_composition::CompositionRuntime;
use dayframe_composition::InstalledMicro;
use dioxus::prelude::*;
use serde::Deserialize;
use std::collections::BTreeMap;
use ui::ThemeMode;
use ui::ThemeRoot;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

/// One entry of `packages/index.json`: where the component lives, the SHA-256 the shell
/// must verify after fetching, and the manifest read statically at build time.
#[derive(Clone, Deserialize)]
struct PackageIndexEntry {
    location: String,
    digest: String,
    manifest: clay_mdk::MicroManifest,
}

async fn fetch_bytes(url: &str) -> Result<Vec<u8>, String> {
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

/// Fetch and compile one indexed package on the browser's `WebAssembly` engine through
/// the shared `clay_rt_wcmp` seam. The fetched bytes must hash to the indexed digest and
/// declare the indexed manifest; a mismatch is this micro's activation failure, reported
/// in its status row while the rest of the shell keeps running.
async fn indexed_package(entry: PackageIndexEntry) -> Result<clay_rt_wcmp::WcmpPackage, String> {
    let bytes = fetch_bytes(&entry.location).await?;
    let package = clay_mdk::package::Package::from_bytes(bytes)?;
    if package.digest() != entry.digest {
        return Err(format!(
            "package at {} declares digest {} but the index pins {}",
            entry.location,
            package.digest(),
            entry.digest
        ));
    }
    if package.manifest() != &entry.manifest {
        return Err(format!(
            "package at {} declares a manifest that differs from the index",
            entry.location
        ));
    }
    clay_rt_wcmp_web::load(package.bytes()).await
}

fn locked_settings() -> BTreeMap<String, ConfiguredMicro> {
    BTreeMap::from([(
        "settings".into(),
        ConfiguredMicro {
            provider: MicroId::new(micro_settings_manifest::ID),
            enablement: Enablement::Enabled,
            eager: true,
            bindings: BTreeMap::new(),
        },
    )])
}

fn native_micros() -> Vec<InstalledMicro> {
    vec![
        InstalledMicro::new(micro_settings_manifest::manifest(), |application| {
            clay_mdk::native::provider(
                application,
                micro_settings_manifest::manifest(),
                micro_settings::activate,
            )
        }),
        InstalledMicro::new(micro_sample::manifest(), |application| {
            clay_mdk::native::provider(
                application,
                micro_sample::manifest(),
                micro_sample::activate,
            )
        }),
    ]
}

/// Build the runtime once the package index arrives. An index that cannot be fetched or
/// parsed is one installation error in the lifecycle panel; the native built-ins still run.
async fn web_runtime() -> CompositionRuntime {
    let mut installed = native_micros();
    let mut errors = Vec::new();
    match fetch_bytes("packages/index.json").await {
        Ok(bytes) => match serde_json::from_slice::<Vec<PackageIndexEntry>>(&bytes) {
            Ok(entries) => {
                for entry in entries {
                    let manifest = entry.manifest.clone();
                    installed.push(InstalledMicro::new(manifest.clone(), move |application| {
                        let entry = entry.clone();
                        clay_mdk::wcmp::provider(application, manifest.clone(), move || {
                            indexed_package(entry.clone())
                        })
                    }));
                }
            }
            Err(error) => errors.push(format!("package index is not an index: {error}")),
        },
        Err(error) => errors.push(error),
    }
    let mut runtime = CompositionRuntime::new(installed, locked_settings());
    runtime.installation_errors = errors;
    micro_sample::seed_mock_defaults(&runtime.application.documents());
    // Demo seed for the counter package `x/dayframe-web.ts` installs: start its config
    // at zero so the guest renders its value instead of the absent-config notice.
    runtime.application.mock_documents().seed(
        clay_docs::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: micro_counter_manifest::CONFIG_TAG.into(),
            key_id: micro_counter_manifest::CONFIG_KEY.into(),
        },
        serde_json::json!(0),
    );
    runtime
}

pub fn main() {
    console_error_panic_hook::set_once();
    dioxus::launch(App);
}

#[component]
fn ReadyApp(runtime: Signal<Option<CompositionRuntime>>) -> Element {
    let initial = runtime
        .read()
        .clone()
        .expect("ReadyApp mounts only with a built runtime");
    let runtime = use_signal(|| initial);
    rsx! { dayframe_composition::Application { runtime } }
}

#[component]
fn App() -> Element {
    let mut runtime = use_signal(|| None::<CompositionRuntime>);
    use_future(move || async move {
        let built = web_runtime().await;
        runtime.set(Some(built));
    });
    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                if runtime.read().is_some() {
                    ReadyApp { runtime }
                } else {
                    p { role: "status", "Loading Dayframe packages…" }
                }
            }
        }
    }
}
