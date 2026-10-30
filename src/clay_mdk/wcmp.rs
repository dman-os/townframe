//! `wcmp` provider assembly: the browser engine's twin of [`crate::wasm::provider`]. Same
//! authoring vocabulary — the guest's declared contributions route through [`Context`] — with
//! the package factory supplying a `clay_rt_wcmp` package compiled for the active backend
//! (`wcmp-wasm-core-web` in the browser). The published provision id is `wasm.controller`, so a
//! manifest written for Wasmtime composes here unchanged; there is no instruction-fuel control
//! (ADR 018 §5).
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
use clay_rt_wcmp::ActivationError as WcmpActivationError;
use clay_rt_wcmp::WcmpPackage;
use clay_rt_wcmp::WcmpRuntime;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

/// The published controller of an activated `wcmp` micro: incarnation identity and the surface
/// component the guest renders into. Runtime and store ownership stay in the driver future and
/// the surface factory, matching the Wasmtime twin (ADR 018 §4).
#[derive(Clone)]
pub struct WcmpMicro {
    pub instance: Instance,
    detail: ComponentRef,
}

impl PartialEq for WcmpMicro {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl ComposeProvision for WcmpMicro {
    const ID: &'static str = "wasm.controller";
}

impl WcmpMicro {
    /// Activate a `wcmp` package for one incarnation. The engine checks grants against the host
    /// the renderer registry serves; the driver future is incarnation-owned work; the surface is
    /// a component of this incarnation, so withdrawing the incarnation withdraws the guest.
    pub(crate) async fn activate(
        context: &Context<NativeComponents>,
        package: &WcmpPackage,
    ) -> Result<Self, ActivationError> {
        let instance = context.instance();
        let (runtime, driver) = WcmpRuntime::activate(
            context.registry().host(),
            instance.clone(),
            package,
            context.configs().clone(),
        )
        .await
        .map_err(|error| match error {
            WcmpActivationError::Access(error) => ActivationError::Access(error),
            WcmpActivationError::Initialization(message) => {
                ActivationError::Initialization(message)
            }
        })?;
        context.own_future(driver)?;
        let mounted = runtime;
        let detail = context
            .registry()
            .publish_surface(&instance, move |_mount, writer| {
                mounted.mount(Arc::new(writer), &[])
            })
            .map(ComponentRef::from_host)?;
        Ok(Self { instance, detail })
    }

    /// The component this guest renders into.
    pub fn surface(&self) -> ComponentRef {
        self.detail
    }
}

/// Turn a `wcmp` micro (manifest + package factory) into a composition provider. The package is
/// fetched/compiled inside the async activation, never at composition-construction time; a
/// factory error (fetch, HTTP status, compile) is an activation failure the reconciler retains
/// until configuration changes — never a shell panic.
pub fn provider<Fut>(
    application: &ApplicationRuntime<NativeComponents>,
    manifest: MicroManifest,
    package: impl Fn() -> Fut + 'static,
) -> MicroFactory
where
    Fut: Future<Output = Result<WcmpPackage, String>> + 'static,
{
    let descriptor = compose_descriptor(&manifest);
    assert!(
        descriptor.provisions.iter().any(|provision| {
            provision.id == WcmpMicro::ID && provision.version == WcmpMicro::VERSION
        }),
        "Wcmp manifest must declare its wasm.controller provision"
    );
    let config = manifest.config.clone();
    // The same declarations are both routed on activation (below) and readable from the static
    // registry while this micro is idle, which is what lets a consumer list its rows without
    // booting their contributors (ADR 014 §Composition and lifecycle).
    let registry_rows = manifest
        .contributions
        .iter()
        .map(|contribution| clay_micro_core::micros::MicroContribution {
            provision: contribution.provision.clone(),
            payload: contribution.payload.clone(),
        })
        .collect();
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
            let package = package().await.map_err(ActivationError::Initialization)?;
            let configs = match &config {
                Some(grant) => {
                    activation.grant_config(&grant.name, routine_facets(&grant.facets))?
                }
                None => BTreeMap::new(),
            };
            let context = Context::new(activation, registry, roots, configs);
            let micro = WcmpMicro::activate(&context, &package).await?;
            context.contribute(&contributions, micro.surface())?;
            context.activation().publish(micro);
            Ok(())
        }
    })
    .with_contributions(registry_rows)
}
