//! Web composition shell for Dayframe (ADR 018 §7): native built-ins linked by this
//! shell plus every package in `packages/index.json`, written by `x/dayframe-web.ts` from
//! each component's statically-read manifest. A package's bytes are fetched only when an
//! instance activates, and their digest is verified against the index before the browser
//! engine (`wcmp`) ever sees them.
use clay_mdk::host::factory;
use dayframe_composition::CompositionRuntime;
use dayframe_composition::InstalledMicro;
use dioxus::prelude::*;
use serde::Deserialize;
use ui::ThemeMode;
use ui::ThemeRoot;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

/// One entry of `packages/index.json`: where the component lives, the BLAKE3 digest the shell
/// must verify after fetching, and the package's sidecar JSON (identity, authority, manifest).
/// STAND-IN until `big_plugs` delivers manifests.
#[derive(Clone, Deserialize)]
struct PackageIndexEntry {
    location: String,
    digest: String,
    sidecar: clay_mdk::Sidecar,
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
/// the shared `clay_rt_wcmp` seam. The fetched bytes must hash to the indexed digest; a
/// mismatch is this micro's activation failure, reported in its status row while the rest
/// of the shell keeps running.
async fn indexed_package(entry: PackageIndexEntry) -> Result<clay_rt_wcmp::WcmpPackage, String> {
    let bytes = fetch_bytes(&entry.location).await?;
    let package = clay_mdk::package::Package::new(clay_mdk::LOCAL_NAMESPACE, entry.sidecar, bytes)?;
    if package.digest() != entry.digest {
        return Err(format!(
            "package at {} hashes to {} but the index pins {}",
            entry.location,
            package.digest(),
            entry.digest
        ));
    }
    clay_rt_wcmp_web::load(package.bytes()).await
}

/// The shell's native built-ins. Settings is the one root: eager, and locked because the shell
/// is unusable without it. Toolbar is on-demand: its pinned root row starts it when the shell
/// mounts it. Sample is eager so its section is written at boot. Collection access
/// is resolved by the composition from each manifest's requests and configuration, not
/// hand-written here.
fn native_micros() -> Vec<InstalledMicro> {
    vec![
        InstalledMicro {
            eager: true,
            locked: true,
            ..InstalledMicro::new(
                clay_mdk::MicroId::local(micro_settings_manifest::ID),
                micro_settings_manifest::sidecar(),
                |id, resources, bindings| {
                    factory(
                        resources,
                        id,
                        micro_settings_manifest::manifest(),
                        bindings,
                        micro_settings::activate,
                    )
                },
            )
        },
        InstalledMicro {
            enabled_on_admission: true,
            ..InstalledMicro::new(
                clay_mdk::MicroId::local(micro_toolbar_manifest::ID),
                micro_toolbar_manifest::sidecar(),
                |id, resources, bindings| {
                    factory(
                        resources,
                        id,
                        micro_toolbar_manifest::manifest(),
                        bindings,
                        micro_toolbar::activate,
                    )
                },
            )
        },
        InstalledMicro {
            eager: true,
            enabled_on_admission: true,
            ..InstalledMicro::new(
                clay_mdk::MicroId::local(micro_sample::ID),
                micro_sample::sidecar(),
                |id, resources, bindings| {
                    factory(
                        resources,
                        id,
                        micro_sample::manifest(),
                        bindings,
                        micro_sample::activate,
                    )
                },
            )
        },
    ]
}

/// Build the runtime once the package index arrives, over the browser's IndexedDB so manifest
/// rows persist across reloads. An index that cannot be fetched or parsed is one installation
/// error in the lifecycle panel; the native built-ins still run. A store that cannot open or
/// load (blocked or full browser storage) is an error the caller shows instead of a shell.
async fn web_runtime() -> Result<CompositionRuntime, String> {
    let store = clay_store::IndexedDbStore::open("dayframe")
        .await
        .map_err(|error| format!("cannot open browser storage: {error}"))?;
    let mut installed = native_micros();
    let mut errors = Vec::new();
    match fetch_bytes("packages/index.json").await {
        Ok(bytes) => match serde_json::from_slice::<Vec<PackageIndexEntry>>(&bytes) {
            Ok(entries) => {
                for entry in entries {
                    // Stand-in namespace until the node's DID installs packages (see
                    // `clay_mdk::LOCAL_NAMESPACE`); a name that is not a plug name is this
                    // entry's installation error.
                    let id = match clay_mdk::MicroId::installed(
                        clay_mdk::LOCAL_NAMESPACE,
                        &entry.sidecar.id,
                    ) {
                        Ok(id) => id,
                        Err(error) => {
                            errors.push(format!("{}: {error}", entry.location));
                            continue;
                        }
                    };
                    let sidecar = entry.sidecar.clone();
                    installed.push(InstalledMicro::new(
                        id,
                        sidecar.clone(),
                        move |id, resources, bindings| {
                            clay_mdk::host::wcmp_factory(
                                resources,
                                id,
                                sidecar.manifest,
                                bindings,
                                move || indexed_package(entry.clone()),
                            )
                        },
                    ));
                }
            }
            Err(error) => errors.push(format!("package index is not an index: {error}")),
        },
        Err(error) => errors.push(error),
    }
    let mut runtime = CompositionRuntime::new(std::sync::Arc::new(store), installed)
        .await
        .map_err(|error| format!("cannot load browser storage: {error}"))?;
    runtime.installation_errors.extend(errors);
    micro_sample::seed_mock_defaults(runtime.documents());
    // Demo seed for the counter package `x/dayframe-web.ts` installs: start its config
    // at zero so the guest renders its value instead of the absent-config notice.
    runtime.documents().mock_documents().seed(
        clay_docs::FacetRoute {
            document: "counter-config".into(),
            branch: "main".into(),
            tag: micro_counter_manifest::CONFIG_TAG.into(),
            key_id: micro_counter_manifest::CONFIG_KEY.into(),
        },
        serde_json::json!(0),
    );
    Ok(runtime)
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
    let mut failure = use_signal(|| None::<String>);
    use_future(move || async move {
        match web_runtime().await {
            Ok(built) => runtime.set(Some(built)),
            Err(error) => failure.set(Some(error)),
        }
    });
    rsx! {
        ThemeRoot {
            family: "material".to_string(),
            mode: ThemeMode::Dark,
            main {
                class: "min-h-screen box-border bg-background p-6 font-sans text-foreground",
                if let Some(error) = failure() {
                    p { role: "alert", "Dayframe cannot start: {error}" }
                } else if runtime.read().is_some() {
                    ReadyApp { runtime }
                } else {
                    p { role: "status", "Loading Dayframe packages…" }
                }
            }
        }
    }
}
