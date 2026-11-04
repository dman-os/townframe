//! Guest incarnations on any engine: one supervisor [`Factory`] generic over the engine
//! adapter. [`crate::wasm`] and [`crate::wcmp`] only say how their runtime initializes, mounts
//! and closes; granting the context, binding presentations to the renderer, and the
//! incarnation's close order are the same for both (ADR 030 §§5–6).
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;

use clay_guest_session::InitialContext;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_guest_session::capabilities::PublicationKind;
use clay_host::Activator;
use clay_host::ComponentRef;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Initialized;
use clay_host::Instance;
use clay_host::LocalFuture;
use clay_host::MicroId;
use clay_host::Root;
use clay_surface::PresentationError;
use clay_surface::SurfaceSession;
use clay_surface::SurfaceSink;
use tokio::task::JoinHandle;

use crate::manifest::MicroManifest;
use crate::native::CollectionBindings;
use crate::native::Resources;
use crate::native::declared_publications;
use crate::native::grant_configs;

/// The outcome of starting one incarnation: its runtime and the driver future that runs it.
pub(crate) type Started<E> =
    LocalFuture<'static, Result<(<E as Engine>::Runtime, LocalFuture<'static, ()>), String>>;

/// How one engine instantiates and drives a guest incarnation.
pub(crate) trait Engine: 'static {
    type Package: 'static;
    type Runtime: Clone + 'static;

    /// Instantiate `package` for `instance` and run its `initialize` with `context`. The
    /// returned driver future runs until the incarnation ends.
    fn initialize(
        host: Host,
        instance: Instance,
        package: Self::Package,
        context: InitialContext,
    ) -> Started<Self>;

    /// What `initialize` returned, by name.
    fn publications(runtime: &Self::Runtime) -> BTreeMap<String, CapabilityPublication>;

    /// Mount the presentation published as `publication` into `writer`.
    fn mount(
        runtime: &Self::Runtime,
        publication: &str,
        writer: Arc<dyn SurfaceSink<ComponentRef>>,
    ) -> Result<SurfaceSession, PresentationError>;

    /// Run the guest's `instance-root.close` and release its store.
    fn close(runtime: Self::Runtime) -> LocalFuture<'static, Result<(), String>>;
}

/// Fetches or compiles the package for one incarnation. Fallible: a missing or invalid
/// artifact is an initialization failure the supervisor records, never a host panic.
pub(crate) type PackageSource<E> = Rc<dyn Fn() -> PackageFuture<E>>;

type PackageFuture<E> = LocalFuture<'static, Result<<E as Engine>::Package, String>>;

struct GuestFactory<E: Engine> {
    resources: Resources,
    manifest: MicroManifest,
    declared: BTreeMap<String, DeclaredPublication>,
    bindings: CollectionBindings,
    package: PackageSource<E>,
}

pub(crate) fn factory<E: Engine>(
    resources: Resources,
    manifest: MicroManifest,
    bindings: CollectionBindings,
    package: PackageSource<E>,
) -> Rc<dyn Factory<CapabilityPublication>> {
    let declared = declared_publications(&manifest);
    Rc::new(GuestFactory::<E> {
        resources,
        manifest,
        declared,
        bindings,
        package,
    })
}

struct GuestRoot<E: Engine> {
    runtime: E::Runtime,
    driver: JoinHandle<()>,
}

impl<E: Engine> Root for GuestRoot<E> {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let Self { runtime, driver } = *self;
            let closed = E::close(runtime).await;
            // The driver returns once the incarnation's store is gone. A panic in it is a
            // programming error raised here.
            if let Err(error) = driver.await {
                std::panic::resume_unwind(error.into_panic());
            }
            closed
        })
    }
}

impl<E: Engine> Factory<CapabilityPublication> for GuestFactory<E> {
    fn initialize(
        &self,
        host: Host,
        instance: Instance,
        requirements: BTreeMap<String, CapabilityPublication>,
        activator: Activator<CapabilityPublication>,
    ) -> LocalFuture<'static, Result<Initialized<CapabilityPublication>, String>> {
        let resources = self.resources.clone();
        let manifest = self.manifest.clone();
        let declared = self.declared.clone();
        let bindings = self.bindings.snapshot();
        let package = Rc::clone(&self.package);
        Box::pin(async move {
            let package = package().await?;
            let micro = MicroId::new(manifest.id.as_str());
            // Guests see their collections by the manifest's binding names, never by id.
            let collections = bindings
                .iter()
                .map(|(binding, bound)| {
                    resources
                        .collections
                        .collection(&bound.id)
                        .map(|registrations| (binding.clone(), registrations.projection()))
                        .ok_or_else(|| format!("collection {} is not declared", bound.id))
                })
                .collect::<Result<_, _>>()?;
            let registrations = bindings
                .iter()
                .filter(|(_, bound)| bound.write)
                .map(|(binding, bound)| {
                    resources
                        .collections
                        .grant(&bound.id, &micro)
                        .map(|grant| (binding.clone(), grant))
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<_, _>>()?;
            let context = InitialContext {
                configs: grant_configs(&resources.documents, &host, &instance, &manifest)?,
                collections,
                registrations,
                requirements,
                publications: declared,
                micro: Some(micro),
                activator: Some(activator),
                ..InitialContext::default()
            };
            let (runtime, driver) = E::initialize(host, instance.clone(), package, context).await?;
            let driver = tokio::task::spawn_local(driver);
            let publications = E::publications(&runtime);
            for (name, publication) in &publications {
                if publication.kind() != PublicationKind::Presentation {
                    continue;
                }
                let component = publication
                    .component()
                    .expect("a presentation publication has a component");
                let mounted = runtime.clone();
                let name = name.clone();
                resources
                    .components
                    .attach_surface(&instance, component, move |_mount, writer| {
                        E::mount(&mounted, &name, Arc::new(writer))
                    });
            }
            Ok(Initialized {
                root: Box::new(GuestRoot::<E> { runtime, driver }),
                publications,
            })
        })
    }
}
