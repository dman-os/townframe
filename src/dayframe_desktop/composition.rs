//! Desktop composition root: native built-ins linked by this shell plus every package in
//! the package directory, read statically (manifest and digest from the component bytes,
//! never by running the guest) and activated on Wasmtime (ADR 018 §7).
pub use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;

use clay_store::OrderedStore;
use dayframe_native::PackageFiles;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

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

/// Open the durable store: the SQLite file named by `DAYFRAME_STORE_PATH`, else
/// `<data directory>/store.sqlite`.
pub async fn open_store() -> Result<clay_store::SqliteStore, String> {
    let path = match std::env::var_os("DAYFRAME_STORE_PATH") {
        Some(path) => PathBuf::from(path),
        None => data_directory()
            .ok_or("no DAYFRAME_STORE_PATH, XDG_DATA_HOME or HOME to place the store under")?
            .join("store.sqlite"),
    };
    dayframe_native::open_store(&path).await
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
fn scan_packages() -> (Vec<PackageFiles>, Vec<String>) {
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
    let mut packages = Vec::new();
    let mut errors = Vec::new();
    for path in paths {
        let component = path.with_file_name(format!(
            "{}.component.wasm",
            manifest_package_name(&path).expect("filtered to manifest files")
        ));
        let read = |path: &Path| {
            std::fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))
        };
        match read(&path).and_then(|manifest_json| {
            Ok(PackageFiles {
                origin: path.display().to_string(),
                manifest_json,
                component: read(&component)?,
            })
        }) {
            Ok(files) => packages.push(files),
            Err(error) => errors.push(error),
        }
    }
    (packages, errors)
}

/// Assemble the desktop composition over `store`, its packages on Wasmtime.
pub async fn desktop_runtime<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    assemble(store, dayframe_native::wasmtime_engine).await
}

/// The composition with its packages run by `engine`: the engine the packages run on is the
/// composition root's choice (see `dayframe_native::PackageEngine`).
pub(crate) async fn assemble<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
    engine: impl dayframe_native::PackageEngine,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    let (packages, errors) = scan_packages();
    dayframe_native::native_runtime(store, packages, errors, engine).await
}

#[cfg(test)]
mod tests;
