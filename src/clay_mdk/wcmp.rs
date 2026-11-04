//! `wcmp` guest assembly: the browser engine's twin of [`crate::wasm::factory`]. A guest
//! renders into a wire surface; the same authoring vocabulary and the shared incarnation logic
//! in [`crate::engine`] apply, with the package source supplying a `clay_rt_wcmp` package
//! compiled for the active backend (`wcmp-wasm-core-web` in the browser). There is no
//! instruction-fuel control (ADR 018 §5).
use crate::engine::Engine;
use crate::engine::PackageSource;
use crate::engine::Started;
use crate::manifest::MicroId;
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
pub use clay_rt_wcmp::WcmpPackage;
use clay_rt_wcmp::WcmpRuntime;
use clay_surface::PresentationError;
use clay_surface::SurfaceSession;
use clay_surface::SurfaceSink;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

struct Wcmp;

impl Engine for Wcmp {
    type Package = WcmpPackage;
    type Runtime = WcmpRuntime;

    fn initialize(
        host: Host,
        instance: Instance,
        package: WcmpPackage,
        context: InitialContext,
    ) -> Started<Self> {
        Box::pin(async move {
            let (runtime, driver) = WcmpRuntime::initialize(&host, instance, &package, context)
                .await
                .map_err(|error| error.to_string())?;
            let driver: LocalFuture<'static, ()> = Box::pin(driver);
            Ok((runtime, driver))
        })
    }

    fn publications(runtime: &WcmpRuntime) -> BTreeMap<String, CapabilityPublication> {
        runtime.publications()
    }

    fn mount(
        runtime: &WcmpRuntime,
        publication: &str,
        writer: Arc<dyn SurfaceSink<ComponentRef>>,
    ) -> Result<SurfaceSession, PresentationError> {
        runtime.mount(publication, writer)
    }

    fn close(runtime: WcmpRuntime) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move { runtime.close().await })
    }
}

/// Turn a `wcmp` micro (identity + manifest + package source) into a supervisor factory. The package is
/// compiled inside the async initialization, not at composition-construction time, and a
/// source error (fetch, compile, missing artifact) is an initialization failure the supervisor
/// retains as `Faulted` until retried, never a shell panic. The composition root calls this;
/// micro crates never build factories themselves.
pub fn factory<Fut>(
    resources: Resources,
    micro: MicroId,
    manifest: MicroManifest,
    bindings: CollectionBindings,
    package: impl Fn() -> Fut + 'static,
) -> Rc<dyn Factory<CapabilityPublication>>
where
    Fut: Future<Output = Result<WcmpPackage, String>> + 'static,
{
    let package: PackageSource<Wcmp> = Rc::new(move || Box::pin(package()));
    crate::engine::factory::<Wcmp>(resources, micro, manifest, bindings, package)
}
