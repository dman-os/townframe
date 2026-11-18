//! Android composition root: the shared native composition (`dayframe_native`) with the store
//! under the app's internal data directory and the Wasm packages read from the APK's assets, and
//! the app's own in-process node (`dayframe_node`) that its Wasm micros reach by `dispatch.local`.
//!
//! The crate only means something on Android; a host build exists so `cargo clippy` and the
//! guardrails see the manifest, and its `main` refuses to run.

#[cfg(not(target_os = "android"))]
fn main() {
    panic!("dayframe_mobile runs on Android only; use dayframe_desktop on a host");
}

/// Android entry: `dioxus-desktop`'s `start_app` (which `WryActivity` calls) runs `main` on its
/// own thread once the activity exists.
#[cfg(target_os = "android")]
fn main() {
    android::main();
}

#[cfg(target_os = "android")]
mod android {
    use dayframe_native::PackageFiles;
    use dayframe_native::Shell;
    use dayframe_native::WryApplication;
    use dioxus::prelude::*;
    use jni::JNIEnv;
    use jni::objects::JObject;
    use jni::objects::JValue;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use tokio::sync::OnceCell;

    /// The activity's `getFilesDir()`, captured in [`main`]. App-private storage that needs no
    /// permission and is removed with the app.
    static DATA_DIRECTORY: OnceLock<PathBuf> = OnceLock::new();

    /// The activity's `getExternalFilesDir(null)` (`/sdcard/Android/data/<package>/files`), where
    /// `adb push` and `adb pull` reach without a debuggable build: the operator's side of
    /// pairing. `None` when the device has no external storage.
    static EXTERNAL_DIRECTORY: OnceLock<Option<PathBuf>> = OnceLock::new();

    /// The file in the external (or, failing that, internal) data directory that lists the
    /// endpoint tickets of the nodes this phone dials, one per line.
    const BOOTSTRAP_FILE: &str = "bootstrap.txt";

    /// The file this phone's node writes its own endpoint ticket to at boot, for the operator to
    /// put in the other node's bootstrap list.
    const ADDRESS_FILE: &str = "node-address.txt";

    /// The phone's node, booted once for the process. Android has no second process to hold a
    /// node, so the app is the holder, and it lives as long as the process does: there is no
    /// activity-destroy hook that could stop it in order, and the OS reclaims the process.
    static NODE: OnceCell<dayframe_node::NodeComposition> = OnceCell::const_new();

    /// The packages `x/dayframe.ts android` stages into `packages/` before the build. `dx`
    /// copies a folder asset into the APK's `assets/` keeping the file names, which is the only
    /// way to put bytes in the APK: it bundles what `asset!` names. Android offers no
    /// directory listing through the resolver, so the names are listed here.
    static PACKAGES: Asset = asset!("/packages");
    const PACKAGE_NAMES: [&str; 3] = ["counter", "layout", "review"];

    /// intxt's nodule handler component, staged beside the packages (`x/dayframe.ts android`).
    const INTXT_GUEST_FILE: &str = "intxt.guest.wasm";

    pub(super) fn main() {
        let (data_directory, external) =
            dioxus::prelude::manganis::android::with_activity(|env, activity| {
                Some((
                    file_path(env, activity, "getFilesDir", "()Ljava/io/File;", &[])
                        .expect("an activity has an internal files directory"),
                    file_path(
                        env,
                        activity,
                        "getExternalFilesDir",
                        "(Ljava/lang/String;)Ljava/io/File;",
                        &[JValue::Object(&JObject::null())],
                    ),
                ))
            })
            .expect("dioxus-desktop sets the activity before it runs main");
        DATA_DIRECTORY
            .set(data_directory.clone())
            .expect("main runs once per process");
        EXTERNAL_DIRECTORY
            .set(external)
            .expect("main runs once per process");
        // Android has no `$HOME` or XDG cache for Wasmtime to find, so without this every process
        // start recompiles every guest (6.5 s for a debug counter on a ZTE 8050). The artifacts
        // are regenerable, so they sit under the internal data dir and go with the app.
        dayframe_native::set_component_cache_directory(data_directory.join("component-cache"));
        dioxus::LaunchBuilder::mobile().launch(App);
    }

    /// The absolute path of the `java.io.File` that `method` on the activity returns, or `None`
    /// when it returns null (no external storage).
    fn file_path(
        env: &mut JNIEnv,
        activity: &JObject,
        method: &str,
        signature: &str,
        arguments: &[JValue],
    ) -> Option<PathBuf> {
        let file = env
            .call_method(activity, method, signature, arguments)
            .and_then(|value| value.l())
            .expect("a Context file-directory accessor");
        if file.is_null() {
            return None;
        }
        let path = env
            .call_method(&file, "getAbsolutePath", "()Ljava/lang/String;", &[])
            .and_then(|value| value.l())
            .expect("File.getAbsolutePath");
        let path: String = env.get_string(&path.into()).expect("a Java string").into();
        Some(PathBuf::from(path))
    }

    /// `MainActivity.requestSystemBack`, which the activity's `OnBackInvokedCallback` calls on the
    /// UI thread: whether the shell took the back gesture. `WryActivity` loads `libmain.so` with
    /// `System.loadLibrary`, so the JVM binds the method to this export by its name.
    #[no_mangle]
    pub extern "system" fn Java_dev_dioxus_main_MainActivity_requestSystemBack(
        _env: JNIEnv,
        _activity: JObject,
    ) -> jni::sys::jboolean {
        dayframe_native::SYSTEM_BACK.request().into()
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

    /// The directories the operator's files are read from, external first: `adb push` reaches the
    /// external one without a debuggable build, the internal one is the fallback.
    fn pairing_directories(data_directory: &std::path::Path) -> Vec<&std::path::Path> {
        EXTERNAL_DIRECTORY
            .get()
            .and_then(Option::as_deref)
            .into_iter()
            .chain([data_directory])
            .collect()
    }

    /// The text of the install file `name` (see `dayframe_node::MicroInstall::interim`): the
    /// first of the pairing directories that has it. Missing everywhere is no file, which installs
    /// the micro unconfigured; any other read failure is the operator's to see.
    fn read_install_file(
        data_directory: &std::path::Path,
        name: &str,
    ) -> std::io::Result<Option<Vec<u8>>> {
        for directory in pairing_directories(data_directory) {
            match std::fs::read(directory.join(name)) {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    /// Boots the phone's node under `<data>/node` with one actor per package micro, dialing the
    /// tickets in the pairing files and publishing its own ticket beside them. A node that
    /// cannot boot is a failure the launcher shows: the micros' node access is part of what the
    /// app is.
    async fn boot_node(
        data_directory: &std::path::Path,
        installs: Vec<dayframe_node::MicroInstall>,
    ) -> Result<dayframe_node::NodeComposition, String> {
        let pairing_directories = pairing_directories(data_directory);
        let mut bootstrap = Vec::new();
        for directory in &pairing_directories {
            bootstrap.extend(
                dayframe_node::read_tickets(&directory.join(BOOTSTRAP_FILE))
                    .map_err(|error| format!("cannot read the bootstrap tickets: {error:#}"))?,
            );
        }
        let node = dayframe_node::NodeComposition::boot(dayframe_node::NodeConfig {
            root: data_directory.join("node"),
            bootstrap,
            // The node learns the datum vocabulary of intxt (the PR and intxt datums the review
            // micro reads) from intxt's cartridge, whose Lexicons it admits when the cartridge is
            // installed below. The review config datum is Clay's, not the node's, and needs no
            // schema here.
            schemas: Arc::new(
                big_cabinet::SchemaRepo::open(std::iter::empty())
                    .expect("no registrations is a valid schema repo"),
            ),
            installs,
        })
        .await
        .map_err(|error| format!("cannot boot the node: {error:#}"))?;
        // intxt's handler component ships in the APK beside the packages. The install is
        // idempotent by cartridge version, so every boot runs it; the first one writes.
        let guest_path = format!("{PACKAGES}/{INTXT_GUEST_FILE}");
        let guest = dioxus::asset_resolver::read_asset_bytes(&guest_path)
            .await
            .map_err(|error| format!("cannot read bundled asset {guest_path}: {error}"))?;
        node.install_intxt(&guest)
            .await
            .map_err(|error| format!("cannot install intxt on the node: {error}"))?;
        let address = node.address();
        for directory in pairing_directories {
            let path = directory.join(ADDRESS_FILE);
            std::fs::write(&path, &address)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        }
        Ok(node)
    }

    async fn load() -> Result<dayframe_native::CompositionRuntime, String> {
        let data_directory = DATA_DIRECTORY
            .get()
            .expect("main sets the data directory before launching");
        let store = dayframe_native::open_store(&data_directory.join("store.sqlite")).await?;
        let mut packages = Vec::new();
        let mut errors = Vec::new();
        for name in PACKAGE_NAMES {
            match read_package(name).await {
                Ok(files) => packages.push(files),
                Err(error) => errors.push(error),
            }
        }
        let installs = dayframe_node::MicroInstall::interim(
            dayframe_native::package_micro_ids(&packages),
            |name| read_install_file(data_directory, name),
        )
        .map_err(|error| format!("cannot read the install files: {error:#}"))?;
        let node = NODE
            .get_or_try_init(|| boot_node(data_directory, installs))
            .await?;
        dayframe_native::native_runtime(
            Arc::new(store),
            packages,
            errors,
            dayframe_native::wasmtime_engine,
            &|micro| node.egress(micro),
            &|micro| node.config_datum(micro).cloned(),
        )
        .await
        .map_err(|error| format!("cannot load the store: {error}"))
    }

    /// The WebView's size in CSS px and the display's CSS px per physical px
    /// (`DisplayMetrics.density`). The WebView fills the activity's content view, which sits below
    /// the status bar when the activity is not drawn edge to edge, so the content view is the size
    /// to lay out in; tao's window reports the whole window and a scale factor of 1.0.
    fn webview_viewport(_: ()) -> dayframe_native::Viewport {
        dioxus::prelude::manganis::android::with_activity(|env, activity| {
            let resources = env
                .call_method(
                    activity,
                    "getResources",
                    "()Landroid/content/res/Resources;",
                    &[],
                )
                .and_then(|value| value.l())
                .expect("Context.getResources");
            let metrics = env
                .call_method(
                    &resources,
                    "getDisplayMetrics",
                    "()Landroid/util/DisplayMetrics;",
                    &[],
                )
                .and_then(|value| value.l())
                .expect("Resources.getDisplayMetrics");
            let scale = f64::from(
                env.get_field(&metrics, "density", "F")
                    .and_then(|value| value.f())
                    .expect("DisplayMetrics.density"),
            );
            let content = content_view(env, activity);
            let mut dimension = |name: &str| {
                f64::from(
                    env.call_method(&content, name, "()I", &[])
                        .and_then(|value| value.i())
                        .expect("a View dimension"),
                ) / scale
            };
            Some(dayframe_native::Viewport {
                width: dimension("getWidth"),
                height: dimension("getHeight"),
                scale,
            })
        })
        .expect("dioxus-desktop keeps the current activity set")
    }

    /// `android.R.id.content`, the view the WebView fills.
    fn content_view<'local>(env: &mut JNIEnv<'local>, activity: &JObject) -> JObject<'local> {
        const CONTENT_ID: i32 = 0x0102_0002;
        env.call_method(
            activity,
            "findViewById",
            "(I)Landroid/view/View;",
            &[JValue::Int(CONTENT_ID)],
        )
        .and_then(|value| value.l())
        .expect("Activity.findViewById")
    }

    /// The system bars and display cutout over the webview's content, in CSS px. The bars are
    /// read from the decor view (`getRootWindowInsets`, null until the view is attached, which
    /// reads as no insets until the next resize), and only the part of each that overlaps the
    /// activity's content view counts: an activity drawn edge to edge has its content under the
    /// bars, while one that is not (Android 14 and older default) already sits between them, and
    /// insetting it again would leave a band of empty chrome.
    fn system_insets(scale: f64) -> dayframe_composition::Insets {
        dioxus::prelude::manganis::android::with_activity(|env, activity| {
            Some(read_insets(env, activity, scale))
        })
        .unwrap_or_default()
    }

    fn read_insets(
        env: &mut JNIEnv,
        activity: &JObject,
        scale: f64,
    ) -> dayframe_composition::Insets {
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
        let int = |env: &mut JNIEnv, object: &JObject, name: &str| {
            env.call_method(object, name, "()I", &[])
                .and_then(|value| value.i())
                .expect("an int accessor")
        };
        let window = call(env, activity, "getWindow", "()Landroid/view/Window;");
        let decor = call(env, &window, "getDecorView", "()Landroid/view/View;");
        let insets = call(
            env,
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
        // [top, right, bottom, left] in physical px, over the whole window.
        let bars = if sdk >= 30 {
            // `WindowInsets.Type.systemBars() | displayCutout()`: what the app must not draw under.
            let mask = |env: &mut JNIEnv, name: &str| {
                env.call_static_method("android/view/WindowInsets$Type", name, "()I", &[])
                    .and_then(|value| value.i())
                    .expect("a WindowInsets.Type mask")
            };
            let mask = mask(env, "systemBars") | mask(env, "displayCutout");
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
                    .expect("an android.graphics.Insets field")
            };
            [side("top"), side("right"), side("bottom"), side("left")]
        } else {
            // API 28-29 have only the deprecated accessors; they report the same bars and cutout.
            [
                int(env, &insets, "getSystemWindowInsetTop"),
                int(env, &insets, "getSystemWindowInsetRight"),
                int(env, &insets, "getSystemWindowInsetBottom"),
                int(env, &insets, "getSystemWindowInsetLeft"),
            ]
        };
        // Where the content view (the WebView) sits in the window.
        let content = content_view(env, activity);
        let location = env.new_int_array(2).expect("a two-int array");
        env.call_method(
            &content,
            "getLocationInWindow",
            "([I)V",
            &[JValue::Object(&location)],
        )
        .expect("View.getLocationInWindow");
        let mut origin = [0_i32; 2];
        env.get_int_array_region(&location, 0, &mut origin)
            .expect("reading the location array");
        let [left, top] = origin;
        let right = int(env, &decor, "getWidth") - left - int(env, &content, "getWidth");
        let bottom = int(env, &decor, "getHeight") - top - int(env, &content, "getHeight");
        let [bar_top, bar_right, bar_bottom, bar_left] = bars;
        let overlap = |bar: i32, offset: i32| f64::from((bar - offset).max(0)) / scale;
        dayframe_composition::Insets {
            top: overlap(bar_top, top),
            right: overlap(bar_right, right),
            bottom: overlap(bar_bottom, bottom),
            left: overlap(bar_left, left),
        }
    }

    #[component]
    fn App() -> Element {
        rsx! {
            Shell {
                load: dayframe_native::runtime_loader(load),
                ready: Callback::new(|runtime| {
                    rsx! {
                        WryApplication {
                            runtime,
                            insets: Some(Callback::new(system_insets)),
                            viewport: Some(Callback::new(webview_viewport)),
                        }
                    }
                }),
            }
        }
    }
}
