//! Desktop composition root: native built-ins linked by this shell plus every package in
//! the package directory, read statically (manifest and digest from the component bytes,
//! never by running the guest) and activated on Wasmtime (ADR 018 §7).
pub use daytxt_core::CompositionRuntime;

use clay_store::OrderedStore;
use daytxt_native::PackageFiles;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

/// The app's data directory: `$XDG_DATA_HOME/daytxt` (falling back to `~/.local/share`).
fn data_directory() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share"))
        })?;
    Some(base.join("daytxt"))
}

/// The directory scanned for package files: `DAYTXT_PACKAGE_DIR`, else
/// `<data directory>/packages`.
fn package_directory() -> Option<PathBuf> {
    if let Some(directory) = std::env::var_os("DAYTXT_PACKAGE_DIR") {
        return Some(PathBuf::from(directory));
    }
    Some(data_directory()?.join("packages"))
}

/// Open the durable store: the SQLite file named by `DAYTXT_STORE_PATH`, else
/// `<data directory>/store.sqlite`.
pub async fn open_store() -> Result<clay_store::SqliteStore, String> {
    let path = match std::env::var_os("DAYTXT_STORE_PATH") {
        Some(path) => PathBuf::from(path),
        None => data_directory()
            .ok_or("no DAYTXT_STORE_PATH, XDG_DATA_HOME or HOME to place the store under")?
            .join("store.sqlite"),
    };
    daytxt_native::open_store(&path).await
}

/// `<name>` of a `<name>.manifest.json` path.
fn manifest_package_name(path: &Path) -> Option<&str> {
    path.file_name()?.to_str()?.strip_suffix(".manifest.json")
}

/// Read every package in the package directory. A missing directory installs nothing; an
/// unreadable or unpaired entry is reported per entry and never stops the shell.
///
/// STAND-IN until `big_cartridges` delivers manifests: a package is the sibling files
/// `<name>.manifest.json` and `<name>.component.wasm`, discovered by the manifest.
pub(crate) fn scan_packages() -> (Vec<PackageFiles>, Vec<String>) {
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

/// The file in the package directory holding intxt's nodule handler component, staged beside
/// the packages by `x/daytxt.ts` (`prepareDaytxtPackages`).
const INTXT_GUEST_FILE: &str = "intxt.guest.wasm";

/// Assemble the desktop composition over `store`, its packages on Wasmtime, with no node: no
/// micro has a way to call one. Tests build this; the app uses [`hosted_runtime`].
#[cfg(test)]
pub async fn desktop_runtime<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    let (packages, errors) = scan_packages();
    assemble(store, daytxt_native::wasmtime_engine, packages, errors).await
}

/// [`desktop_runtime`] on the desktop's node: the one `day` uses (`DAY_NODE`, else the platform
/// data directory, resolved as `day` does), served on its socket so `day` reaches it.
pub async fn hosted_runtime<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
) -> Result<CompositionRuntime, String> {
    let root = match std::env::var_os("DAY_NODE") {
        Some(root) => PathBuf::from(root),
        None => day_node::NodeDir::default_root()
            .map_err(|error| format!("cannot place the node: {error:#}"))?,
    };
    // The operator's install files and pairing tickets sit in the app's data directory.
    let data_directory =
        data_directory().ok_or("no XDG_DATA_HOME or HOME to place the app's files under")?;
    std::fs::create_dir_all(&data_directory)
        .map_err(|error| format!("cannot create {}: {error}", data_directory.display()))?;
    let guest_path = package_directory()
        .ok_or("no package directory to read the intxt guest from")?
        .join(INTXT_GUEST_FILE);
    let intxt_guest = std::fs::read(&guest_path)
        .map_err(|error| format!("cannot read {}: {error}", guest_path.display()))?;
    let (packages, errors) = scan_packages();
    daytxt_native::hosted_runtime(
        store,
        packages,
        errors,
        daytxt_native::wasmtime_engine,
        daytxt_native::NodeHost {
            root,
            serve: true,
            pairing_directories: vec![data_directory],
            intxt_guest,
        },
    )
    .await
}

/// The composition of `packages` run by `engine`: the engine the packages run on is the
/// composition root's choice (see `daytxt_native::PackageEngine`). No micro has a way to call a
/// node.
#[cfg(test)]
pub(crate) async fn assemble<St: OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
    engine: impl daytxt_native::PackageEngine,
    packages: Vec<PackageFiles>,
    errors: Vec<String>,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    daytxt_native::native_runtime(store, packages, errors, engine, &|_| None, &|_| None).await
}

#[cfg(test)]
mod tests;
