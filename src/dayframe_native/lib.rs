//! Native-engine half of the Dayframe composition root, shared by the desktop and mobile
//! shells: the linked built-in micros, the Wasmtime package factory, the SQLite store and the
//! demo seed (ADR 018 §7). A shell supplies only what differs per platform: where the store
//! file lives and where package bytes come from (a scanned directory on desktop, the APK's
//! assets on Android).
use clay_mdk::host::factory;
use clay_store::OrderedStore;
use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;
use dayframe_composition::InstalledMicro;
use dioxus::prelude::*;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use ui::ThemeMode;
use ui::ThemeRoot;

/// One package as the shell found it: the `<name>.manifest.json` sidecar text and the guest
/// component bytes it describes.
// STAND-IN until `big_plugs` delivers manifests.
pub struct PackageFiles {
    /// Where the shell found it, for installation error messages.
    pub origin: String,
    pub manifest_json: Vec<u8>,
    pub component: Vec<u8>,
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

/// Open the durable store at `path`, creating a missing parent directory. Failing here is an
/// external error (an unwritable disk, a corrupt file) that the launcher shows instead of a
/// shell.
pub async fn open_store(path: &Path) -> Result<clay_store::SqliteStore, String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    // A `Database` keeps its runtime (and so the sqlite workers) alive.
    let database = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default()).database(
        path,
        &[],
        "_dayframe_shell_unused",
    );
    clay_store::SqliteStore::open(database)
        .await
        .map_err(|error| format!("cannot open the store at {}: {error}", path.display()))
}

/// Parse each package and make it a Wasmtime-backed catalog entry. An unparseable package is
/// reported per entry and never stops the shell.
fn package_micros(packages: Vec<PackageFiles>) -> (Vec<InstalledMicro>, Vec<String>) {
    let mut installed = Vec::new();
    let mut errors = Vec::new();
    for files in packages {
        let parsed = clay_mdk::package::Package::from_json(
            clay_mdk::LOCAL_NAMESPACE,
            &files.manifest_json,
            files.component,
        );
        match parsed {
            Ok(package) => {
                let sidecar = package.sidecar().clone();
                let bytes = package.bytes().to_vec();
                installed.push(InstalledMicro::new(
                    package.id().clone(),
                    sidecar.clone(),
                    move |id, resources, bindings| {
                        clay_mdk::host::wasm_factory(
                            resources,
                            id,
                            sidecar.manifest,
                            bindings,
                            move || {
                                let bytes = bytes.clone();
                                async move { clay_mdk::host::WasmPackage::from_bytes(bytes).await }
                            },
                        )
                    },
                ));
            }
            Err(error) => errors.push(format!("{}: {error}", files.origin)),
        }
    }
    (installed, errors)
}

/// Assemble the native composition over `store` with `packages` installed beside the linked
/// built-ins. `package_errors` are the shell's own discovery failures (an unreadable
/// directory, a missing asset), reported beside the parse failures. Async because durable
/// collections load before they report bootstrap, which parks on a disk-backed store.
pub async fn native_runtime<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
    packages: Vec<PackageFiles>,
    package_errors: Vec<String>,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    let mut installed = native_micros();
    let (packages, errors) = package_micros(packages);
    installed.extend(packages);
    let mut runtime = CompositionRuntime::new(store, installed).await?;
    runtime.installation_errors.extend(package_errors);
    runtime.installation_errors.extend(errors);
    micro_sample::seed_mock_defaults(runtime.documents());
    // Demo seed for the counter package: start its config at zero so the guest renders its
    // value instead of the absent-config notice.
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

/// Builds the runtime for [`Shell`]; the shell supplies the store location and package source.
pub type RuntimeLoader =
    Callback<(), Pin<Box<dyn Future<Output = Result<CompositionRuntime, String>>>>>;

/// A [`RuntimeLoader`] from an async constructor. Must be called inside a component, as a
/// `Callback` is owned by the current scope.
pub fn runtime_loader<Fut>(build: impl Fn() -> Fut + 'static) -> RuntimeLoader
where
    Fut: Future<Output = Result<CompositionRuntime, String>> + 'static,
{
    Callback::new(move |()| -> Pin<Box<dyn Future<Output = Result<CompositionRuntime, String>>>> {
        Box::pin(build())
    })
}

/// The launch screen shared by the native shells: loads the runtime once, shows a failure
/// instead of a shell when the store or packages cannot be loaded, and mounts the
/// [`Application`] when it is ready.
#[component]
pub fn Shell(load: RuntimeLoader) -> Element {
    let mut runtime = use_signal(|| None::<CompositionRuntime>);
    let mut failure = use_signal(|| None::<String>);
    use_future(move || async move {
        match load.call(()).await {
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
                    p { role: "status", "Loading Dayframe…" }
                }
            }
        }
    }
}

/// Mounts the shell once the runtime exists; `Application` takes the runtime as a signal.
#[component]
fn ReadyApp(runtime: Signal<Option<CompositionRuntime>>) -> Element {
    let initial = runtime
        .read()
        .clone()
        .expect("ReadyApp mounts only with a built runtime");
    let runtime = use_signal(|| initial);
    rsx! { Application { runtime } }
}
