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
    use jni::objects::JObject;
    use jni::objects::JValue;
    use jni::JNIEnv;
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
        dayframe_native::native_runtime(
            Arc::new(store),
            packages,
            errors,
            dayframe_native::wasmtime_engine,
        )
        .await
        .map_err(|error| format!("cannot load the store: {error}"))
    }

    /// The system bars and display cutout over the window, in logical px. With `targetSdk` 36
    /// the activity draws edge to edge, and winit-android's `safe_area()` is zero, so the
    /// shell has to read the insets from the decor view itself. `getRootWindowInsets` is null
    /// until the view is attached to a window; that reads as no insets, and the next surface
    /// resize reads again.
    fn system_insets(scale: f64) -> dayframe_composition::Insets {
        let app = dioxus_native::current_android_app();
        // SAFETY: `vm_as_ptr` is the process's `JavaVM`, valid for the process's lifetime.
        let vm = unsafe { jni::JavaVM::from_raw(app.vm_as_ptr().cast()) }
            .expect("android-activity hands over a valid JavaVM");
        let mut env = vm
            .attach_current_thread()
            .expect("the window thread can attach to the JVM");
        // SAFETY: `activity_as_ptr` is a global reference android-activity keeps alive for the
        // activity's lifetime; `JObject` does not own or delete it.
        let activity = unsafe { JObject::from_raw(app.activity_as_ptr().cast()) };
        fn call<'local>(
            env: &mut JNIEnv<'local>,
            object: &JObject,
            name: &str,
            signature: &str,
        ) -> JObject<'local> {
            env.call_method(object, name, signature, &[])
                .and_then(|value| value.l())
                .expect("a framework accessor on the activity's window")
        }
        let window = call(&mut env, &activity, "getWindow", "()Landroid/view/Window;");
        let decor = call(&mut env, &window, "getDecorView", "()Landroid/view/View;");
        let insets = call(
            &mut env,
            &decor,
            "getRootWindowInsets",
            "()Landroid/view/WindowInsets;",
        );
        if insets.is_null() {
            return dayframe_composition::Insets::default();
        }
        let sdk = env
            .get_static_field("android/os/Build$VERSION", "SDK_INT", "I")
            .and_then(|value| value.i())
            .expect("Build.VERSION.SDK_INT");
        let int = |env: &mut JNIEnv, object: &JObject, name: &str| {
            env.call_method(object, name, "()I", &[])
                .and_then(|value| value.i())
                .expect("an int inset accessor") as f64
                / scale
        };
        if sdk >= 30 {
            // `WindowInsets.Type.systemBars() | displayCutout()`: what the app must not draw under.
            let mask = |env: &mut JNIEnv, name: &str| {
                env.call_static_method("android/view/WindowInsets$Type", name, "()I", &[])
                    .and_then(|value| value.i())
                    .expect("a WindowInsets.Type mask")
            };
            let mask = mask(&mut env, "systemBars") | mask(&mut env, "displayCutout");
            let bars = env
                .call_method(
                    &insets,
                    "getInsets",
                    "(I)Landroid/graphics/Insets;",
                    &[JValue::Int(mask)],
                )
                .and_then(|value| value.l())
                .expect("WindowInsets.getInsets");
            let mut side = |name: &str| {
                env.get_field(&bars, name, "I")
                    .and_then(|value| value.i())
                    .expect("an android.graphics.Insets field") as f64
                    / scale
            };
            dayframe_composition::Insets {
                top: side("top"),
                right: side("right"),
                bottom: side("bottom"),
                left: side("left"),
            }
        } else {
            // API 28-29 have only the deprecated accessors; they report the same bars and cutout.
            dayframe_composition::Insets {
                top: int(&mut env, &insets, "getSystemWindowInsetTop"),
                right: int(&mut env, &insets, "getSystemWindowInsetRight"),
                bottom: int(&mut env, &insets, "getSystemWindowInsetBottom"),
                left: int(&mut env, &insets, "getSystemWindowInsetLeft"),
            }
        }
    }

    #[component]
    fn App() -> Element {
        rsx! {
            Shell {
                load: dayframe_native::runtime_loader(load),
                insets: Some(Callback::new(system_insets)),
            }
        }
    }
}
