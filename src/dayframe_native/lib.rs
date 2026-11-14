//! Native-engine half of the Dayframe composition root, shared by the desktop and mobile
//! shells: the linked built-in micros, the Wasmtime package factory, the SQLite store and the
//! demo seed (ADR 018 §7). A shell supplies only what differs per platform: where the store
//! file lives and where package bytes come from (a scanned directory on desktop, the APK's
//! assets on Android).
use clay_host::Factory;
use clay_mdk::host::factory;
use clay_mdk::host::CapabilityPublication;
use clay_mdk::host::CollectionBindings;
use clay_mdk::host::Resources;
use clay_store::OrderedStore;
use dayframe_composition::Application;
pub use dayframe_composition::CompositionRuntime;
#[cfg(feature = "blitz")]
use dayframe_composition::HostInputs;
#[cfg(feature = "blitz")]
use dayframe_composition::Insets;
use dayframe_composition::InstalledMicro;
use dioxus::prelude::*;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
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

/// The shell's native built-ins. The shell micro is the frame: eager, and locked because nothing
/// is usable without it. Settings is an on-demand panel: home lists its declared row, and
/// opening it starts it. Toolbar is on-demand: its sidebar panel row starts it when the sidebar
/// mounts it. Sample is eager so its section is written at boot. The lifecycle controls are the
/// composition's own Settings section. Collection access is resolved
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
            .satisfied_by(&shell, &micro_shell_manifest::sidecar())
            .expect("the shell publishes what Settings requires")
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
        dayframe_composition::lifecycle::micro(),
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

/// Turns a package's identity, manifest and bytes into a supervisor factory: the engine the
/// packages run on is the composition root's choice (Wasmtime natively, Wasmi in tests).
pub trait PackageEngine:
    Fn(
        Resources,
        clay_mdk::MicroId,
        clay_mdk::MicroManifest,
        CollectionBindings,
        Vec<u8>,
    ) -> Rc<dyn Factory<CapabilityPublication>>
    + Clone
    + 'static
{
}

impl<F> PackageEngine for F where
    F: Fn(
            Resources,
            clay_mdk::MicroId,
            clay_mdk::MicroManifest,
            CollectionBindings,
            Vec<u8>,
        ) -> Rc<dyn Factory<CapabilityPublication>>
        + Clone
        + 'static
{
}

/// Where Wasmtime keeps compiled guests; see `clay_rt_wasmtime::set_component_cache_directory`.
pub use clay_mdk::host::set_component_cache_directory;

/// The Wasmtime [`PackageEngine`], what both native shells run packages on.
pub fn wasmtime_engine(
    resources: Resources,
    id: clay_mdk::MicroId,
    manifest: clay_mdk::MicroManifest,
    bindings: CollectionBindings,
    bytes: Vec<u8>,
) -> Rc<dyn Factory<CapabilityPublication>> {
    clay_mdk::host::wasm_factory(resources, id, manifest, bindings, move || {
        let bytes = bytes.clone();
        async move { clay_mdk::host::WasmPackage::from_bytes(bytes).await }
    })
}

/// Parse each package and make it a catalog entry run by `engine`. An unparseable package is
/// reported per entry and never stops the shell.
fn package_micros(
    packages: Vec<PackageFiles>,
    engine: impl PackageEngine,
) -> (Vec<InstalledMicro>, Vec<String>) {
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
                let engine = engine.clone();
                let micro = InstalledMicro::new(
                    package.id().clone(),
                    sidecar.clone(),
                    move |id, resources, bindings| {
                        engine(resources, id, sidecar.manifest, bindings, bytes)
                    },
                )
                // A package may require the shell's publications (the layout's app bar
                // button); the shell is the only provider a package can name today.
                .satisfied_by(
                    &clay_mdk::MicroId::local(micro_shell_manifest::ID),
                    &micro_shell_manifest::sidecar(),
                );
                match micro {
                    Ok(micro) => installed.push(micro),
                    Err(error) => errors.push(format!("{}: {error}", files.origin)),
                }
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
    engine: impl PackageEngine,
) -> Result<CompositionRuntime, clay_store::StoreError> {
    let mut installed = native_micros();
    let (packages, errors) = package_micros(packages, engine);
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
    Callback::new(
        move |()| -> Pin<Box<dyn Future<Output = Result<CompositionRuntime, String>>>> {
            Box::pin(build())
        },
    )
}

/// The Android system back gesture, as the activity reports it. With `targetSdk` 36 the
/// platform's predictive back never delivers `KEYCODE_BACK` to the native activity: the system
/// either invokes an `OnBackInvokedCallback` the activity registered or, with none, closes the
/// app. `MainActivity` registers one and asks [`SystemBack::request`] from the UI thread
/// whether the shell will take the gesture; when it will not (the shell is on its home), the
/// activity moves the task to the background, which is what the system would have done.
pub struct SystemBack {
    requested: tokio::sync::Notify,
    can_back: std::sync::atomic::AtomicBool,
}

impl SystemBack {
    /// Claim the gesture for the shell if it has somewhere to go back to; whether it did.
    pub fn request(&self) -> bool {
        let claimed = self.can_back.load(std::sync::atomic::Ordering::Acquire);
        if claimed {
            self.requested.notify_one();
        }
        claimed
    }
}

/// The one system back of the process, read by the shell's `HostInputs` and driven by the
/// activity (see [`SystemBack`]).
pub static SYSTEM_BACK: SystemBack = SystemBack {
    requested: tokio::sync::Notify::const_new(),
    can_back: std::sync::atomic::AtomicBool::new(false),
};

/// Reads the system chrome currently covering the window, in logical px, given the window's
/// scale factor. Only a shell whose window has such chrome (Android) supplies one.
#[cfg(feature = "blitz")]
pub type InsetsSource = Callback<f64, Insets>;

/// What a shell mounts once the runtime exists: it reads the renderer's window events into
/// `HostInputs` and renders `Application`. Desktop picks per renderer (Wry or Blitz); Android
/// mounts [`BlitzApplication`].
pub type ReadyView = Callback<Signal<Option<CompositionRuntime>>, Element>;

/// The launch screen shared by the native shells: loads the runtime once, shows a failure
/// instead of a shell when the store or packages cannot be loaded, and mounts `ready` when the
/// runtime exists.
#[component]
pub fn Shell(load: RuntimeLoader, ready: ReadyView) -> Element {
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
                class: "bg-background font-sans text-foreground",
                if let Some(error) = failure() {
                    p { role: "alert", "Dayframe cannot start: {error}" }
                } else if runtime.read().is_some() {
                    {ready.call(runtime)}
                } else {
                    p { role: "status", "Loading Dayframe…" }
                }
            }
        }
    }
}

/// Mounts the shell under Blitz once the runtime exists; `Application` takes the runtime as a
/// signal. `insets` is the system chrome source of a window that has some (Android).
#[cfg(feature = "blitz")]
#[component]
pub fn BlitzApplication(
    runtime: Signal<Option<CompositionRuntime>>,
    insets: Option<InsetsSource>,
) -> Element {
    let initial = runtime
        .read()
        .clone()
        .expect("BlitzApplication mounts only with a built runtime");
    let runtime = use_signal(|| initial);
    let host = use_host_inputs(insets);
    rsx! { Application { runtime, host } }
}

/// What the window tells the shell: its logical size (the shell never measures the DOM, and
/// Blitz panics if it tries to from a handler), and the platform back gesture. Android's
/// `KEYCODE_BACK` arrives from winit as `BrowserBack`; a mouse's back button is the desktop
/// equivalent. Neither is a key Blitz delivers to a focused element, so they are taken from the
/// window event stream.
#[cfg(feature = "blitz")]
fn use_host_inputs(insets_source: Option<InsetsSource>) -> HostInputs {
    use dioxus_native::winit::event::ButtonSource;
    use dioxus_native::winit::event::ElementState;
    use dioxus_native::winit::event::MouseButton;
    use dioxus_native::winit::event::WindowEvent;
    use dioxus_native::winit::keyboard::Key;
    use dioxus_native::winit::keyboard::NamedKey;
    use dioxus_native::winit::window::Window;
    let window: Arc<dyn Window> = use_hook(consume_context);
    let logical_size = |window: &Arc<dyn Window>| {
        let size = window.surface_size();
        let scale = window.scale_factor();
        (size.width as f64 / scale, size.height as f64 / scale)
    };
    let initial = logical_size(&window);
    let mut width = use_signal(|| initial.0);
    let mut height = use_signal(|| initial.1);
    let mut platform_back = use_signal(|| 0_u64);
    let can_back = use_signal(|| false);
    // A window without an `InsetsSource` (desktop) has no system chrome over its surface. With
    // one, the insets are re-read whenever the surface changes: winit-android reports neither
    // `InsetsChanged` nor `ContentRectChanged` (both are `warn!("TODO")`), and a rotation, a
    // bar appearing or a cutout all change the surface's size.
    let read_insets = move |window: &Arc<dyn Window>| {
        insets_source.map_or_else(Insets::default, |source| source.call(window.scale_factor()))
    };
    let mut insets = use_signal({
        let window = window.clone();
        move || read_insets(&window)
    });
    #[cfg(target_os = "android")]
    {
        use_future(move || async move {
            loop {
                SYSTEM_BACK.requested.notified().await;
                platform_back += 1;
            }
        });
        use_effect(move || {
            SYSTEM_BACK
                .can_back
                .store(can_back(), std::sync::atomic::Ordering::Release);
        });
    }
    dioxus_native::use_window_event(move |event, _| match event {
        WindowEvent::SurfaceResized(_) | WindowEvent::ScaleFactorChanged { .. } => {
            let (w, h) = logical_size(&window);
            width.set(w);
            height.set(h);
            insets.set(read_insets(&window));
        }
        WindowEvent::KeyboardInput { event, .. }
            if event.state == ElementState::Pressed
                && !event.repeat
                && event.logical_key == Key::Named(NamedKey::BrowserBack) =>
        {
            platform_back += 1;
        }
        WindowEvent::PointerButton {
            button: ButtonSource::Mouse(MouseButton::Back),
            state: ElementState::Pressed,
            ..
        } => platform_back += 1,
        _ => {}
    });
    HostInputs {
        width: width.into(),
        height: height.into(),
        insets: insets.into(),
        platform_back,
        can_back,
        // Blitz has no keyboard focus (`tabindex` panics it) and no pointer capture.
        renderer: dayframe_composition::Renderer::Blitz,
        // The development seed: see `HostInputs::start`.
        start: std::env::var("DAYFRAME_SHELL_OPEN").ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn system_back_is_claimed_only_while_the_shell_can_go_back() {
        let back = SystemBack {
            requested: tokio::sync::Notify::const_new(),
            can_back: std::sync::atomic::AtomicBool::new(false),
        };
        assert!(
            !back.request(),
            "the shell on its home leaves back to the system"
        );
        back.can_back.store(true, Ordering::Release);
        assert!(back.request());
        // The permit is stored, so a request made before the shell's task waits is not lost.
        back.requested.notified().await;
    }
}
