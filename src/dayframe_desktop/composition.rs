//! Desktop composition root: native built-ins linked by this shell plus every package in
//! the package directory, read statically (manifest and digest from the component bytes,
//! never by running the guest) and activated on Wasmtime (ADR 018 §7).
pub use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;

use clay_micro_core::micros::MicroId;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use dayframe_composition::InstalledMicro;
use std::collections::BTreeMap;
use std::path::PathBuf;

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
                installed.push(InstalledMicro::new(manifest.clone(), move |application| {
                    let bytes = bytes.clone();
                    clay_mdk::wasm::provider(application, manifest.clone(), move || {
                        let bytes = bytes.clone();
                        async move { clay_mdk::wasm::WasmPackage::from_bytes(bytes).await }
                    })
                }));
            }
            Err(error) => errors.push(error),
        }
    }
    (installed, errors)
}

pub fn desktop_runtime() -> CompositionRuntime {
    let mut installed = native_micros();
    let (packages, errors) = package_micros();
    installed.extend(packages);
    let mut runtime = CompositionRuntime::new(installed, locked_settings());
    runtime.installation_errors = errors;
    micro_sample::seed_mock_defaults(&runtime.application.documents());
    // Demo seed for the counter package the check script installs: start its config at
    // zero so the guest renders its value instead of the absent-config notice.
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

#[cfg(test)]
mod counter_tests;

#[cfg(test)]
mod tests;
