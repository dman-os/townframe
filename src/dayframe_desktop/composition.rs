//! Desktop composition root: native built-ins linked by this shell plus every package in
//! the package directory, read statically (manifest and digest from the component bytes,
//! never by running the guest) and activated on Wasmtime (ADR 018 §7).
pub use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;

use clay_mdk::native::factory;
use clay_mdk::native::CollectionBindings;
use dayframe_composition::InstalledMicro;
use micro_settings_manifest::SECTIONS;
use std::path::PathBuf;

/// The shell's native built-ins. Settings is the one root: eager, and locked because the shell
/// is unusable without it. Sample is eager so its section is written at boot. What each may
/// read or write is this catalog's decision, not the micro's.
fn native_micros() -> Vec<InstalledMicro> {
    vec![
        InstalledMicro {
            eager: true,
            locked: true,
            ..InstalledMicro::new(micro_settings_manifest::manifest(), |resources| {
                factory(
                    resources,
                    micro_settings_manifest::manifest(),
                    CollectionBindings {
                        read: vec![SECTIONS.into()],
                        write: vec![],
                    },
                    micro_settings::activate,
                )
            })
        },
        InstalledMicro {
            eager: true,
            ..InstalledMicro::new(micro_sample::manifest(), |resources| {
                factory(
                    resources,
                    micro_sample::manifest(),
                    CollectionBindings {
                        read: vec![],
                        write: vec![SECTIONS.into()],
                    },
                    micro_sample::activate,
                )
            })
        },
    ]
}

/// The directory scanned for package files: `DAYFRAME_PACKAGE_DIR`, else
/// `$XDG_DATA_HOME/dayframe/packages` (falling back to `~/.local/share`).
fn package_directory() -> Option<PathBuf> {
    if let Some(directory) = std::env::var_os("DAYFRAME_PACKAGE_DIR") {
        return Some(PathBuf::from(directory));
    }
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share"))
        })?;
    Some(base.join("dayframe").join("packages"))
}

/// Read every package in the package directory. A missing directory installs nothing; an
/// unreadable or manifest-less entry is reported per entry and never stops the shell.
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
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    let mut installed = Vec::new();
    let mut errors = Vec::new();
    for path in paths {
        let package = std::fs::read(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))
            .and_then(|bytes| {
                clay_mdk::package::Package::from_bytes(bytes)
                    .map_err(|error| format!("{}: {error}", path.display()))
            });
        match package {
            Ok(package) => {
                let manifest = package.manifest().clone();
                let bytes = package.bytes().to_vec();
                installed.push(InstalledMicro::new(manifest.clone(), move |resources| {
                    clay_mdk::wasm::factory(
                        resources,
                        manifest,
                        CollectionBindings::default(),
                        move || {
                            let bytes = bytes.clone();
                            async move { clay_mdk::wasm::WasmPackage::from_bytes(bytes).await }
                        },
                    )
                }));
            }
            Err(error) => errors.push(error),
        }
    }
    (installed, errors)
}

/// Assemble the desktop composition. The constructor is async because durable collections load
/// before they report bootstrap; with the in-memory store it never parks, so blocking is safe.
pub fn desktop_runtime() -> CompositionRuntime {
    let mut installed = native_micros();
    let (packages, errors) = package_micros();
    installed.extend(packages);
    let mut runtime = futures::executor::block_on(CompositionRuntime::new(installed));
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
    runtime
}

#[cfg(test)]
mod tests;
