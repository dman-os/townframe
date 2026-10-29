//! Wasmtime provider assembly: the engine-backed twin of [`crate::native::provider`]. A wasm guest
//! renders into a wire surface instead of a host-linked view, so this backend is renderer-bound —
//! it publishes through the Dioxus adapter's surface registry — while its authoring vocabulary
//! stays the MDK's: the guest's declared contributions are routed through the same [`Context`] a
//! native activation gets, and the controller it publishes is identified by [`ComponentRef`],
//! never by a host type.
use crate::ComponentRef;
use crate::manifest::MicroManifest;
use crate::native::Context;
use crate::native::compose_descriptor;
use crate::native::routine_facets;
use clay_dioxus::NativeComponents;
use clay_host::Instance;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::ActivationError;
use clay_micro_core::micros::MicroFactory;
use clay_micro_core::micros::Provision as ComposeProvision;
pub use clay_rt_wasmtime::WasmPackage;
use clay_rt_wasmtime::WasmRuntime;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// The cached package for one prepared guest component. The host process compiles a component
/// once and reuses the linker across incarnations, so composition code caches per guest name.
/// `x/check-dayframe.ts` prepares the artifact and exports its path. Compilation happens
/// outside the cache lock so concurrent activations do not serialize on it.
pub async fn component_package(name: &'static str) -> WasmPackage {
    static CACHE: std::sync::Mutex<BTreeMap<&'static str, WasmPackage>> =
        std::sync::Mutex::new(BTreeMap::new());
    if let Some(package) = CACHE.lock().unwrap().get(name) {
        return package.clone();
    }
    let package = WasmPackage::from_env(name)
        .await
        .expect("prepared component must be valid");
    CACHE
        .lock()
        .unwrap()
        .entry(name)
        .or_insert_with(|| package.clone())
        .clone()
}

/// The published controller of an activated wasm micro: its incarnation identity, the surface
/// component that guest renders into, and a test-only fuel control. Runtime and store ownership
/// remain in the driver future and surface factory, not in this provision.
#[derive(Clone)]
pub struct WasmMicro {
    pub instance: Instance,
    // Test control knob only, not the runtime: a shared fuel budget whose `0` makes the
    // incarnation's next guest entry trap. The runtime and store themselves live only in the
    // driver future and the surface factory closure (ADR 018 §4).
    fuel: Arc<AtomicU64>,
    detail: ComponentRef,
}

impl PartialEq for WasmMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl ComposeProvision for WasmMicro {
    const ID: &'static str = "wasm.controller";
}

impl WasmMicro {
    /// Activate a guest package for one incarnation. The engine checks the manifest's component
    /// grants against the host the renderer registry serves, the guest's driver future becomes
    /// incarnation-owned work, and the surface it renders into is published as a component of
    /// this incarnation — so withdrawing the incarnation withdraws the guest's presentation.
    pub(crate) async fn activate(
        context: &Context<NativeComponents>,
        package: &WasmPackage,
    ) -> Result<Self, ActivationError> {
        let instance = context.instance();
        let (runtime, driver) = WasmRuntime::activate(
            context.registry().host(),
            instance.clone(),
            package,
            context.configs().clone(),
        )
        .await
        .map_err(|error| match error {
            clay_rt_wasmtime::ActivationError::Access(error) => ActivationError::Access(error),
            clay_rt_wasmtime::ActivationError::Initialization(message) => {
                ActivationError::Initialization(message)
            }
        })?;
        let fuel = runtime.fuel_budget();
        context.own_future(driver)?;
        let mounted = runtime.clone();
        let detail = context
            .registry()
            .publish_surface(&instance, move |_mount, writer| {
                mounted.mount(Arc::new(writer), &[])
            })
            .map(ComponentRef::from_host)?;
        Ok(Self {
            instance,
            fuel,
            detail,
        })
    }

    /// The component this guest renders into. Transport data for collection entries and mounts;
    /// [`ComponentRef::host`] unwraps it for a host-side mount.
    pub fn surface(&self) -> ComponentRef {
        self.detail
    }

    /// Trip the guest's fuel limit. The engine traps on its next instruction, which closes the
    /// incarnation and withdraws everything it owns; test-only.
    pub fn exhaust_fuel(&self) {
        self.fuel.store(0, Ordering::Relaxed);
    }
}

/// Turn a wasm micro (manifest + package factory) into a composition provider. The package
/// is compiled inside the async activation, not at composition-construction time. The
/// composition root calls this; micro crates never build providers themselves. After the guest
/// activates, each contribution its manifest declares is routed to the contract it names with
/// the guest's published surface (see [`crate::native::Contributable`]); a failure rolls the
/// incarnation back.
pub fn provider<Fut>(
    application: &ApplicationRuntime<NativeComponents>,
    manifest: MicroManifest,
    package: impl Fn() -> Fut + 'static,
) -> MicroFactory
where
    Fut: Future<Output = WasmPackage> + 'static,
{
    let descriptor = compose_descriptor(&manifest);
    assert!(
        descriptor.provisions.iter().any(|provision| {
            provision.id == WasmMicro::ID && provision.version == WasmMicro::VERSION
        }),
        "Wasm manifest must declare its controller provision"
    );
    let config = manifest.config.clone();
    let contributions = Rc::new(manifest.contributions);
    let registry = application.renderer();
    let roots = application.root_mounts();
    let package = Rc::new(package);
    MicroFactory::new_async(descriptor, move |activation| {
        let config = config.clone();
        let contributions = contributions.clone();
        let registry = registry.clone();
        let roots = roots.clone();
        let package = package.clone();
        async move {
            let package = package().await;
            let configs = match &config {
                Some(grant) => {
                    activation.grant_config(&grant.name, routine_facets(&grant.facets))?
                }
                None => BTreeMap::new(),
            };
            let context = Context::new(activation, registry, roots, configs);
            let micro = WasmMicro::activate(&context, &package).await?;
            context.contribute(&contributions, micro.surface())?;
            context.activation().publish(micro);
            Ok(())
        }
    })
}

#[cfg(test)]
mod tests;
