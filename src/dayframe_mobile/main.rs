//! Android composition root: the shared native composition (`dayframe_native`) with the store
//! under the app's internal data directory and the Wasm packages read from the APK's assets.
//!
//! The crate only means something on Android; a host build exists so `cargo clippy` and the
//! guardrails see the manifest, and its `main` refuses to run.

#[cfg(not(target_os = "android"))]
fn main() {
    panic!("dayframe_mobile runs on Android only; use dayframe_desktop on a host");
}

/// Android entry. The `main` below is never called: `android-activity` loads the cdylib and
/// calls `android_main`, which hands the activity to Blitz's winit before launching.
#[cfg(target_os = "android")]
fn main() {}

#[cfg(target_os = "android")]
mod android {
    use dayframe_native::PackageFiles;
    use dayframe_native::Shell;
    use dioxus::prelude::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::OnceLock;

    /// `AndroidApp::internal_data_path()`, captured in `android_main`. App-private storage that
    /// needs no permission and is removed with the app.
    static DATA_DIRECTORY: OnceLock<PathBuf> = OnceLock::new();

    /// The packages `x/dayframe.ts android` stages into `packages/` before the build. `dx`
    /// copies a folder asset into the APK's `assets/` keeping the file names, which is the only
    /// way to put bytes in the APK: it bundles what `asset!` names. Android offers no
    /// directory listing through the resolver, so the names are listed here.
    static PACKAGES: Asset = asset!("/packages");
    const PACKAGE_NAMES: [&str; 2] = ["counter", "layout"];

    #[no_mangle]
    fn android_main(app: dioxus_native::AndroidApp) {
        DATA_DIRECTORY
            .set(
                app.internal_data_path()
                    .expect("the activity has an internal data path"),
            )
            .expect("android_main runs once");
        dioxus_native::set_android_app(app);
        dioxus::launch(App);
    }

    async fn read_package(name: &str) -> Result<PackageFiles, String> {
        let read = |suffix: &'static str| async move {
            let path = format!("{PACKAGES}/{name}.{suffix}");
            dioxus::asset_resolver::read_asset_bytes(&path)
                .await
                .map_err(|error| format!("cannot read bundled asset {path}: {error}"))
        };
        Ok(PackageFiles {
            origin: format!("bundled package {name}"),
            manifest_json: read("manifest.json").await?,
            component: read("component.wasm").await?,
        })
    }

    async fn load() -> Result<dayframe_native::CompositionRuntime, String> {
        let store_path = DATA_DIRECTORY
            .get()
            .expect("android_main sets the data directory before launching")
            .join("store.sqlite");
        let store = dayframe_native::open_store(&store_path).await?;
        let mut packages = Vec::new();
        let mut errors = Vec::new();
        for name in PACKAGE_NAMES {
            match read_package(name).await {
                Ok(files) => packages.push(files),
                Err(error) => errors.push(error),
            }
        }
        dayframe_native::native_runtime(Arc::new(store), packages, errors)
            .await
            .map_err(|error| format!("cannot load the store: {error}"))
    }

    #[component]
    fn App() -> Element {
        rsx! {
            Shell { load: dayframe_native::runtime_loader(load) }
        }
    }
}
