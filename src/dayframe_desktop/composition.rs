//! Desktop composition root: native built-ins linked by this shell plus every package in
//! the package directory, read statically (manifest and digest from the component bytes,
//! never by running the guest) and activated on Wasmtime (ADR 018 §7).
pub use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;

use clay_mdk::host::factory;
use clay_store::OrderedStore;
use dayframe_composition::InstalledMicro;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// The shell's native built-ins. The shell micro is the frame: eager, and locked because nothing
/// is usable without it. Settings is an on-demand panel: home lists its declared row, and
/// opening it starts it. Toolbar is on-demand: its pinned root row starts it when the frame
/// mounts it. Sample is eager so its section is written at boot. Collection access is resolved
/// by the composition from each manifest's requests and configuration, not hand-written here.
fn native_micros() -> Vec<InstalledMicro> {
    let shell = clay_mdk::MicroId::local(micro_shell_manifest::ID);
    vec![
        InstalledMicro {
            eager: true,
            locked: true,
            ..InstalledMicro::new(
                shell.clone(),
                micro_shell_manifest::sidecar(),
                |id, resources, bindings| {
                    factory(
                        resources,
                        id,
                        micro_shell_manifest::manifest(),
                        bindings,
                        micro_shell::activate,
                    )
                },
            )
        },
        InstalledMicro {
            enabled_on_admission: true,
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
            .satisfied_by(&shell)
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

/// The app's data directory: `$XDG_DATA_HOME/dayframe` (falling back to `~/.local/share`).
fn data_directory() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share"))
        })?;
    Some(base.join("dayframe"))
}

/// The directory scanned for package files: `DAYFRAME_PACKAGE_DIR`, else
/// `<data directory>/packages`.
fn package_directory() -> Option<PathBuf> {
    if let Some(directory) = std::env::var_os("DAYFRAME_PACKAGE_DIR") {
        return Some(PathBuf::from(directory));
    }
    Some(data_directory()?.join("packages"))
}

/// Open the durable store under the shell's collections: the SQLite file named by
/// `DAYFRAME_STORE_PATH`, else `<data directory>/store.sqlite`. A missing parent directory is
/// created. Failing here is an external error (no home directory, unwritable disk, a corrupt
/// file) that the launcher shows instead of a shell.
pub async fn open_store() -> Result<clay_store::SqliteStore, String> {
    let path = match std::env::var_os("DAYFRAME_STORE_PATH") {
        Some(path) => PathBuf::from(path),
        None => data_directory()
            .ok_or("no DAYFRAME_STORE_PATH, XDG_DATA_HOME or HOME to place the store under")?
            .join("store.sqlite"),
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create {}: {error}", parent.display()))?;
    }
    // A `Database` keeps its runtime (and so the sqlite workers) alive.
    let database = sqlite_rs::SqliteRuntime::new(sqlite_rs::RuntimeConfig::default()).database(
        &path,
        &[],
        "_dayframe_shell_unused",
    );
    clay_store::SqliteStore::open(database)
        .await
        .map_err(|error| format!("cannot open the store at {}: {error}", path.display()))
}

/// `<name>` of a `<name>.manifest.json` path.
fn manifest_package_name(path: &Path) -> Option<&str> {
    path.file_name()?.to_str()?.strip_suffix(".manifest.json")
}

/// Read every package in the package directory. A missing directory installs nothing; an
/// unreadable or unpaired entry is reported per entry and never stops the shell.
///
/// STAND-IN until `big_plugs` delivers manifests: a package is the sibling files
/// `<name>.manifest.json` and `<name>.component.wasm`, discovered by the manifest.
fn package_micros() -> (Vec<InstalledMicro>, Vec<String>) {
    let Some(directory) = package_directory() else {
        return (Vec::new(), Vec::new());
    };
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(_) if !directory.exists() => return (Vec::new(), Vec::new()),
        Err(error) => {
            return (
                Vec::new(),
                vec![format!(
                    "cannot scan package directory {}: {error}",
                    directory.display()
                )],
            );
        }
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file() && manifest_package_name(path).is_some())
        .collect();
    paths.sort();
    let mut installed = Vec::new();
    let mut errors = Vec::new();
    for path in paths {
        let component = path.with_file_name(format!(
            "{}.component.wasm",
            manifest_package_name(&path).expect("filtered to manifest files")
        ));
        let read = |path: &Path| {
            std::fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))
        };
        let package = read(&path).and_then(|json| {
            let bytes = read(&component)?;
            clay_mdk::package::Package::from_json(clay_mdk::LOCAL_NAMESPACE, &json, bytes)
                .map_err(|error| format!("{}: {error}", path.display()))
        });
        match package {
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
            Err(error) => errors.push(error),
        }
    }
    (installed, errors)
}

/// Assemble the desktop composition over `store`. Async because durable collections load
/// before they report bootstrap, which parks on a disk-backed store.
pub async fn desktop_runtime<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    let mut installed = native_micros();
    let (packages, errors) = package_micros();
    installed.extend(packages);
    let mut runtime = CompositionRuntime::new(store, installed).await?;
    runtime.installation_errors.extend(errors);
    micro_sample::seed_mock_defaults(runtime.documents());
    // Demo seed for the counter package the check script installs: start its config at
    // zero so the guest renders its value instead of the absent-config notice.
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

#[cfg(test)]
mod tests;
