//! Wasmtime guest assembly: the engine-backed twin of [`crate::native::factory`]. A wasm guest
//! renders into a wire surface instead of a host-linked view, so this backend is bound to the
//! Dioxus adapter's surface registry; its authoring vocabulary stays the MDK's. The shared
//! incarnation logic lives in [`crate::engine`]; this module only says how Wasmtime
//! initializes, mounts, and closes a guest.
use crate::engine::Engine;
use crate::engine::PackageSource;
use crate::engine::Started;
use crate::manifest::MicroManifest;
use crate::native::CollectionBindings;
use crate::native::Resources;
use clay_guest_session::InitialContext;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_host::ComponentRef;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Instance;
use clay_host::LocalFuture;
pub use clay_rt_wasmtime::WasmPackage;
use clay_rt_wasmtime::WasmRuntime;
use clay_surface::PresentationError;
use clay_surface::SurfaceSession;
use clay_surface::SurfaceSink;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

/// The cached package for one prepared guest component. The host process compiles a component
/// once and reuses the linker across incarnations, so composition code caches per guest name.
/// `x/check-dayframe.ts` prepares the artifact and exports its path. Compilation happens
/// outside the cache lock so concurrent activations do not serialize on it. A missing or
/// invalid artifact is an initialization failure the supervisor records as `Faulted`, not a
/// host panic: the shell stays up and reports it.
pub async fn component_package(name: &'static str) -> Result<WasmPackage, String> {
    static CACHE: std::sync::Mutex<BTreeMap<&'static str, WasmPackage>> =
        std::sync::Mutex::new(BTreeMap::new());
    if let Some(package) = CACHE.lock().unwrap().get(name) {
        return Ok(package.clone());
    }
    let package = WasmPackage::from_env(name).await?;
    Ok(CACHE
        .lock()
        .unwrap()
        .entry(name)
        .or_insert_with(|| package.clone())
        .clone())
}

struct Wasmtime;

impl Engine for Wasmtime {
    type Package = WasmPackage;
    type Runtime = WasmRuntime;

    fn initialize(
        host: Host,
        instance: Instance,
        package: WasmPackage,
        context: InitialContext,
    ) -> Started<Self> {
        Box::pin(async move {
            let (runtime, driver) = WasmRuntime::initialize(&host, instance, &package, context)
                .await
                .map_err(|error| error.to_string())?;
            let driver: LocalFuture<'static, ()> = Box::pin(driver);
            Ok((runtime, driver))
        })
    }

    fn publications(runtime: &WasmRuntime) -> BTreeMap<String, CapabilityPublication> {
        runtime.publications()
    }

    fn mount(
        runtime: &WasmRuntime,
        publication: &str,
        writer: Arc<dyn SurfaceSink<ComponentRef>>,
    ) -> Result<SurfaceSession, PresentationError> {
        runtime.mount(publication, writer, &[])
    }

    fn close(runtime: WasmRuntime) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move { runtime.close().await })
    }
}

/// Turn a wasm micro (manifest + package source) into a supervisor factory. The package is
/// compiled inside the async initialization, not at composition-construction time, and a
/// source error (fetch, compile, missing artifact) is an initialization failure the supervisor
/// retains as `Faulted` until retried, never a shell panic. The composition root calls this;
/// micro crates never build factories themselves.
pub fn factory<Fut>(
    resources: Resources,
    manifest: MicroManifest,
    bindings: CollectionBindings,
    package: impl Fn() -> Fut + 'static,
) -> Rc<dyn Factory<CapabilityPublication>>
where
    Fut: Future<Output = Result<WasmPackage, String>> + 'static,
{
    let package: PackageSource<Wasmtime> = Rc::new(move || Box::pin(package()));
    crate::engine::factory::<Wasmtime>(resources, manifest, bindings, package)
}

#[cfg(test)]
mod tests;
