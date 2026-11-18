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

/// Android entry. The `main` below is never called: `android-activity` loads the cdylib and
/// calls `android_main`, which hands the activity to Blitz's winit before launching.
#[cfg(target_os = "android")]
fn main() {}

#[cfg(target_os = "android")]
mod android {
    use dayframe_native::BlitzApplication;
    use dayframe_native::PackageFiles;
    use dayframe_native::Shell;
    use dioxus::prelude::*;
    use jni::objects::JObject;
    use jni::objects::JValue;
    use jni::JNIEnv;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use tokio::sync::OnceCell;

    /// `AndroidApp::internal_data_path()`, captured in `android_main`. App-private storage that
    /// needs no permission and is removed with the app.
    static DATA_DIRECTORY: OnceLock<PathBuf> = OnceLock::new();

    /// `AndroidApp::external_data_path()` (`/sdcard/Android/data/<package>/files`), where
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
    static NODE: OnceCell<Booted> = OnceCell::const_new();

    /// The booted node and the micros its cartridges contribute, as the shell loads them: each is
    /// a package whose component came out of the node's catalog, with the handles the node minted
    /// for it from its `uses` and the cartridge's grants.
    struct Booted {
        node: dayframe_node::NodeComposition,
        cartridge_packages: Vec<PackageFiles>,
        /// Cartridge micros that could not be loaded, for the shell's installation errors.
        cartridge_errors: Vec<String>,
    }

    /// The packages `x/dayframe.ts android` stages into `packages/` before the build. `dx`
    /// copies a folder asset into the APK's `assets/` keeping the file names, which is the only
    /// way to put bytes in the APK: it bundles what `asset!` names. Android offers no
    /// directory listing through the resolver, so the names are listed here.
    static PACKAGES: Asset = asset!("/packages");
    const PACKAGE_NAMES: [&str; 3] = ["counter", "layout", "review"];

    /// intxt's nodule handler component, staged beside the packages (`x/dayframe.ts android`).
    const INTXT_GUEST_FILE: &str = "intxt.guest.wasm";

    #[no_mangle]
    fn android_main(app: dioxus_native::AndroidApp) {
        let data_directory = app
            .internal_data_path()
            .expect("the activity has an internal data path");
        DATA_DIRECTORY
            .set(data_directory.clone())
            .expect("android_main runs once");
        EXTERNAL_DIRECTORY
            .set(app.external_data_path())
            .expect("android_main runs once");
        // Android has no `$HOME` or XDG cache for Wasmtime to find, so without this every process
        // start recompiles every guest (6.5 s for a debug counter on a ZTE 8050). The artifacts
        // are regenerable, so they sit under the internal data dir and go with the app.
        dayframe_native::set_component_cache_directory(data_directory.join("component-cache"));
        register_back_callback(&app);
        dioxus_native::set_android_app(app);
        dioxus::launch(App);
    }

    /// `MainActivity.requestSystemBack`, which the activity's `OnBackInvokedCallback` calls on the
    /// UI thread: whether the shell took the back gesture.
    extern "system" fn request_system_back(_env: JNIEnv, _activity: JObject) -> jni::sys::jboolean {
        dayframe_native::SYSTEM_BACK.request().into()
    }

    /// Bind `MainActivity.requestSystemBack` to [`request_system_back`]. NativeActivity loads
    /// `libmain.so` through `dlopen` rather than `System.loadLibrary`, so the JVM's by-name
    /// symbol lookup never finds the export and the method has to be registered explicitly.
    fn register_back_callback(app: &dioxus_native::AndroidApp) {
        // SAFETY: see `system_insets`.
        let vm = unsafe { jni::JavaVM::from_raw(app.vm_as_ptr().cast()) }
            .expect("android-activity hands over a valid JavaVM");
        let mut env = vm
            .attach_current_thread()
            .expect("android_main can attach to the JVM");
        let activity = unsafe { JObject::from_raw(app.activity_as_ptr().cast()) };
        let class = env
            .get_object_class(&activity)
            .expect("the activity has a class");
        env.register_native_methods(
            &class,
            &[jni::NativeMethod {
                name: "requestSystemBack".into(),
                sig: "()Z".into(),
                fn_ptr: request_system_back as *mut std::ffi::c_void,
            }],
        )
        .expect("MainActivity declares requestSystemBack");
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

    /// Boots the phone's node under `<data>/node`, dialing the tickets in the pairing files and
    /// publishing its own ticket beside them, installs intxt on it, and loads the micros the
    /// node's cartridges contribute. `bundled` are the ids of the packages in the APK: a micro a
    /// cartridge contributes replaces the bundled package of the same name, and one that is in no
    /// cartridge gets its node access from the interim install table. A node that cannot boot is
    /// a failure the launcher shows: the micros' node access is part of what the app is.
    async fn boot_node(
        data_directory: &std::path::Path,
        bundled: Vec<String>,
    ) -> Result<Booted, String> {
        let pairing_directories = pairing_directories(data_directory);
        let mut bootstrap = Vec::new();
        for directory in &pairing_directories {
            bootstrap.extend(
                dayframe_node::read_tickets(&directory.join(BOOTSTRAP_FILE))
                    .map_err(|error| format!("cannot read the bootstrap tickets: {error:#}"))?,
            );
        }
        let mut node = dayframe_node::NodeComposition::boot(dayframe_node::NodeConfig {
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
            installs: Vec::new(),
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

        let mut cartridge_packages = Vec::new();
        let mut cartridge_errors = Vec::new();
        for loaded in node
            .sync_cartridge_micros()
            .await
            .map_err(|error| format!("cannot load the cartridges' micros: {error:#}"))?
        {
            match loaded {
                Ok(micro) => cartridge_packages.push(PackageFiles {
                    origin: format!("cartridge {}", micro.micro),
                    manifest_json: micro.manifest_json,
                    component: micro.component,
                }),
                Err(error) => cartridge_errors.push(format!("{error:#}")),
            }
        }
        // INTERIM: a bundled micro that no cartridge replaces is installed from the table in
        // `dayframe_node`, until it is published as a cartridge.
        let uncovered = bundled
            .into_iter()
            .filter(|id| {
                !cartridge_packages
                    .iter()
                    .filter_map(dayframe_native::package_micro_id)
                    .any(|loaded| plug_name(&loaded) == plug_name(id))
            })
            .collect::<Vec<_>>();
        let installs = dayframe_node::MicroInstall::interim(uncovered, |name| {
            read_install_file(data_directory, name)
        })
        .map_err(|error| format!("cannot read the install files: {error:#}"))?;
        for install in installs {
            node.install_micro(install)
                .await
                .map_err(|error| format!("cannot install a micro on the node: {error:#}"))?;
        }

        let address = node.address();
        for directory in pairing_directories {
            let path = directory.join(ADDRESS_FILE);
            std::fs::write(&path, &address)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        }
        Ok(Booted {
            node,
            cartridge_packages,
            cartridge_errors,
        })
    }

    /// The `<name>` of a micro id `<did>/<name>`: a cartridge's micro is installed under its
    /// publisher's DID, a bundled package under the local namespace, and the name is what they
    /// share.
    fn plug_name(id: &str) -> &str {
        id.split_once('/')
            .unwrap_or_else(|| panic!("micro id {id:?} is not `<did>/<name>`"))
            .1
    }

    async fn load() -> Result<dayframe_native::CompositionRuntime, String> {
        let data_directory = DATA_DIRECTORY
            .get()
            .expect("android_main sets the data directory before launching");
        let store = dayframe_native::open_store(&data_directory.join("store.sqlite")).await?;
        let mut bundled = Vec::new();
        let mut errors = Vec::new();
        for name in PACKAGE_NAMES {
            match read_package(name).await {
                Ok(files) => bundled.push(files),
                Err(error) => errors.push(error),
            }
        }
        let booted = NODE
            .get_or_try_init(|| {
                boot_node(
                    data_directory,
                    bundled
                        .iter()
                        .filter_map(dayframe_native::package_micro_id)
                        .collect(),
                )
            })
            .await?;
        // A cartridge's micro replaces the bundled package of the same name.
        let replaced = |files: &PackageFiles| {
            dayframe_native::package_micro_id(files).is_some_and(|id| {
                booted
                    .cartridge_packages
                    .iter()
                    .filter_map(dayframe_native::package_micro_id)
                    .any(|loaded| plug_name(&loaded) == plug_name(&id))
            })
        };
        let mut packages: Vec<PackageFiles> = bundled
            .into_iter()
            .filter(|files| !replaced(files))
            .collect();
        packages.extend(booted.cartridge_packages.iter().cloned());
        errors.extend(booted.cartridge_errors.iter().cloned());
        let node = &booted.node;
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
                ready: Callback::new(|runtime| {
                    rsx! { BlitzApplication { runtime, insets: Some(Callback::new(system_insets)) } }
                }),
            }
        }
    }
}
