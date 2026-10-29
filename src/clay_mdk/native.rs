//! The in-process backend: one authoring API over the host's composition machinery. Native
//! micros depend on this module, never on `clay_micro_core`, a renderer crate, or the host kernel
//! directly (ADR 015 §3 dependency direction, §Boundary correction). The MDK adds ergonomics,
//! never authority: every operation below still runs through the host's checked services.
//!
//! Nothing here names a renderer. A [`Context`] is generic over the host's [`RendererRegistry`],
//! so the same micro composes under whatever registry the composition root installed; publishing
//! a component hands the registry a factory producing *its* element type. Presentation that does
//! need a concrete renderer — the view-side slot component and the collection hook — lives in
//! [`crate::view`].

pub use clay_collection::Batch;
pub use clay_collection::Query;
pub use clay_collection::ReadOnly;
pub use clay_collection::RowChange;
pub use clay_collection::View;
pub use clay_docs::DocumentRef;
pub use clay_docs::DocumentService;
pub use clay_docs::DocumentSnapshot;
pub use clay_docs::DocumentWatch;
pub use clay_docs::Facet;
pub use clay_docs::FacetRoute;
pub use clay_docs::FacetValue;
pub use clay_docs::Freshness;
pub use clay_docs::Operation;
pub use clay_docs::OperationReceipt;
pub use clay_host::AccessError;
pub use clay_host::CollectionReader;
pub use clay_host::CollectionSubscription;
pub use clay_host::CollectionWriter;
pub use clay_host::Entry;
pub use clay_host::Instance;
pub use clay_host::RendererRegistry;

use crate::ComponentRef;
use crate::manifest::FacetAccess;
use crate::manifest::MicroManifest;
use crate::manifest::Provision;
use clay_host::HostedCollection;
use clay_host::RootMount;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroActivation;
use clay_micro_core::micros::MicroDescriptor;
use clay_micro_core::micros::MicroFactory;
use clay_micro_core::micros::MicroId as ComposeMicroId;
use clay_micro_core::micros::MicroProvision as ComposeMicroProvision;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A contract that accepts contributions from micros that cannot write its collection
/// themselves — a wasm guest, whose manifest names this contract and carries the payload as data
/// (see [`crate::Contribution`]). The contract crate implements this next to its [`Provision`];
/// the micro providing it publishes with [`Context::publish_contributable`], and the backend
/// routes each declared contribution here by contract id once the contributor has activated.
pub trait Contributable: Provision {
    /// Land one contribution. `contributor` is the activated incarnation making it, `surface`
    /// the component it published; the payload's shape is the contract's own vocabulary. A
    /// payload that violates the contract is a defect in the declaring micro's manifest — reject
    /// it with [`AccessError::Denied`] so the activation rolls back, rather than panicking.
    fn contribute(
        &self,
        contributor: &Instance,
        payload: &serde_json::Value,
        surface: ComponentRef,
    ) -> Result<(), AccessError>;
}

#[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
type Acceptor = Arc<
    dyn Fn(&Instance, &serde_json::Value, ComponentRef) -> Result<(), AccessError> + Send + Sync,
>;

/// What the MDK stores in the composition registry for one published provision: the contract
/// value, and the acceptor when the contract is [`Contributable`]. Stored type-erased under the
/// contract's id so a backend that only has the contract's *name* — a manifest's contribution
/// target — can still reach the acceptor; [`Context::require`] and [`output`] downcast the value.
struct Envelope {
    value: Arc<dyn Any + Send + Sync>,
    /// Contribution acceptor, only routed by an engine adapter (`wasmtime`/`wcmp`) or exercised
    /// by this crate's own tests. Engine-less hosts publish the contract value alone.
    #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
    accept: Option<Acceptor>,
}

impl Envelope {
    fn open<T: Provision>(erased: Arc<dyn Any + Send + Sync>) -> T {
        erased
            .downcast_ref::<Self>()
            .expect("MDK contract published outside the MDK")
            .value
            .downcast_ref::<T>()
            .expect("contract identity mapped to two native types")
            .clone()
    }
}

/// A host-owned contribution collection provided by the activating incarnation (ADR 015 §6).
/// Writers touch only their own entries; readers get snapshots; the host withdraws everything
/// when the provider or a contributor closes.
#[derive(Clone)]
pub struct Collection<T: Clone + Send + Sync + 'static> {
    collection: HostedCollection<T>,
}

impl<T: Clone + Send + Sync + 'static> Collection<T> {
    pub fn from_host(collection: HostedCollection<T>) -> Self {
        Self { collection }
    }

    pub fn host(&self) -> &HostedCollection<T> {
        &self.collection
    }

    pub fn reader(&self, consumer: &Instance) -> CollectionReader<T> {
        self.collection.reader(consumer)
    }

    pub fn writer(&self, contributor: &Instance) -> Result<CollectionWriter<T>, AccessError> {
        self.collection.writer(contributor)
    }
}

/// The value surface one native activation may use: granted configuration documents, declared
/// requirements resolved to typed provisions, this incarnation's collections, component
/// publication, root mounting, publications, and owned-future registration. It never hands out
/// the host or the registry (ADR 015 §7); the provider adapter below constructs it.
pub struct Context<R: RendererRegistry> {
    activation: MicroActivation,
    registry: R,
    roots: HostedCollection<RootMount>,
    configs: BTreeMap<String, DocumentRef>,
}

impl<R: RendererRegistry> Context<R> {
    /// Assemble one activation's value surface. Only this crate's provider adapters call it: a
    /// micro receives a context, it never builds one.
    pub(crate) fn new(
        activation: MicroActivation,
        registry: R,
        roots: HostedCollection<RootMount>,
        configs: BTreeMap<String, DocumentRef>,
    ) -> Self {
        Self {
            activation,
            registry,
            roots,
            configs,
        }
    }

    /// The composition activation this context mediates. Backend adapters (the wasm provider)
    /// reach through it to stage controller publications; micro code does not.
    #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
    pub(crate) fn activation(&self) -> &MicroActivation {
        &self.activation
    }

    /// The renderer registry this activation publishes into. Backend adapters need it for
    /// renderer-specific publication shapes; micro code publishes through
    /// [`Context::publish_component`] instead.
    #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
    pub(crate) fn registry(&self) -> &R {
        &self.registry
    }

    /// The configuration documents granted for this activation, keyed by manifest binding name.
    #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
    pub(crate) fn configs(&self) -> &BTreeMap<String, DocumentRef> {
        &self.configs
    }

    /// This activation's incarnation identity. Plain data; operations validate it host-side.
    pub fn instance(&self) -> Instance {
        self.activation.instance()
    }

    /// A configuration document granted from the manifest's declared ACL. `None` means the
    /// manifest declared no such binding; grants are validated before activation runs.
    pub fn config(&self, name: &str) -> Option<&DocumentRef> {
        self.configs.get(name)
    }

    /// A live provision for a declared requirement. Declarations are not authority: this
    /// succeeds only while a compatible published provision exists.
    pub fn require<T: Provision>(&self) -> Result<T, AccessError> {
        self.activation
            .require_erased(T::ID, T::VERSION)
            .map(Envelope::open::<T>)
    }

    /// Stage a provision for commit when this activation succeeds. A failed or dropped
    /// activation rolls back every staged publication.
    pub fn publish<T: Provision>(&self, value: T) {
        #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
        let envelope = Envelope {
            value: Arc::new(value),
            accept: None,
        };
        #[cfg(not(any(test, feature = "wasmtime", feature = "wcmp")))]
        let envelope = Envelope {
            value: Arc::new(value),
        };
        self.activation
            .publish_erased(T::ID, T::VERSION, Arc::new(envelope));
    }

    /// [`Context::publish`] for a contract that accepts declared contributions; the backend
    /// routes a contributor's manifest entries to this value's [`Contributable::contribute`].
    /// Without an engine adapter linked there is no backend to route them, so the contract
    /// value is published alone (the trait bound still documents the contract's shape).
    pub fn publish_contributable<T: Contributable>(&self, value: T) {
        #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
        let envelope = {
            let acceptor = value.clone();
            Envelope {
                value: Arc::new(value),
                accept: Some(Arc::new(move |contributor, payload, surface| {
                    acceptor.contribute(contributor, payload, surface)
                })),
            }
        };
        #[cfg(not(any(test, feature = "wasmtime", feature = "wcmp")))]
        let envelope = Envelope {
            value: Arc::new(value),
        };
        self.activation
            .publish_erased(T::ID, T::VERSION, Arc::new(envelope));
    }

    /// Land this incarnation's declared contributions (a backend step, run after the micro has
    /// activated and published `surface`). Each names a declared requirement; the bound
    /// provider must have published it as contributable.
    #[cfg(any(test, feature = "wasmtime", feature = "wcmp"))]
    pub(crate) fn contribute(
        &self,
        contributions: &[crate::Contribution],
        surface: ComponentRef,
    ) -> Result<(), AccessError> {
        let instance = self.instance();
        for contribution in contributions {
            let version = self
                .activation
                .descriptor()
                .requirements
                .iter()
                .find(|requirement| requirement.id == contribution.provision)
                .expect("manifest contributes to a contract it does not require")
                .version;
            let target = self
                .activation
                .require_erased(&contribution.provision, version)?;
            let accept = target
                .downcast_ref::<Envelope>()
                .expect("MDK contract published outside the MDK")
                .accept
                .as_ref()
                .unwrap_or_else(|| {
                    panic!(
                        "contract {} accepts no contributions",
                        contribution.provision
                    )
                });
            accept(&instance, &contribution.payload, surface)?;
        }
        Ok(())
    }

    /// A contribution collection provided by this incarnation.
    pub fn collection<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> Result<Collection<T>, AccessError> {
        self.activation.collection().map(Collection::from_host)
    }

    /// Publish a render factory as a component of this incarnation. The factory produces the
    /// installed registry's element type, so a micro with a native view constrains its context
    /// to a renderer whose element it writes. The returned reference is transport data for
    /// collection entries and mounts; rendering resolves the factory host-side.
    pub fn publish_component(
        &self,
        render: impl Fn() -> R::Render + 'static,
    ) -> Result<ComponentRef, AccessError> {
        let instance = self.activation.instance();
        self.registry
            .publish(&instance, render)
            .map(ComponentRef::from_host)
    }

    /// Insert a root mount contribution for this incarnation. The host withdraws it when the
    /// incarnation closes.
    pub fn mount_root(
        &self,
        title: impl Into<String>,
        content: ComponentRef,
    ) -> Result<(), AccessError> {
        let instance = self.activation.instance();
        let writer = self.roots.writer(&instance)?;
        writer.insert(RootMount {
            title: title.into(),
            content: content.host(),
        })?;
        Ok(())
    }

    /// Register incarnation-owned local work (D13). The composition driver polls it alongside
    /// reconciliation and drops it when this instance closes.
    pub fn own_future(
        &self,
        future: impl Future<Output = ()> + 'static,
    ) -> Result<(), AccessError> {
        self.activation.own_future(future)
    }
}

/// Turn a native micro (manifest + activate function) into a composition provider. The
/// composition root calls this; micro crates never build providers themselves. `activate` is
/// generic over the registry the runtime carries, which is what keeps this module — and every
/// micro authored against it — renderer-agnostic.
pub fn provider<R: RendererRegistry + 'static>(
    application: &ApplicationRuntime<R>,
    manifest: MicroManifest,
    activate: impl Fn(&Context<R>) -> Result<(), AccessError> + 'static,
) -> MicroFactory {
    let registry = application.renderer();
    let roots = application.root_mounts();
    let descriptor = compose_descriptor(&manifest);
    let config = manifest.config.clone();
    let activate = std::rc::Rc::new(activate);
    MicroFactory::new(descriptor, move |activation| {
        let configs = match &config {
            Some(grant) => activation.grant_config(&grant.name, routine_facets(&grant.facets))?,
            None => BTreeMap::new(),
        };
        let context = Context::new(activation, registry.clone(), roots.clone(), configs);
        activate(&context)
    })
}

/// Host-side retrieval of an MDK-published provision (composition roots and tests).
pub fn output<T: Provision, R: RendererRegistry>(
    application: &ApplicationRuntime<R>,
    instance: &str,
) -> Result<T, AccessError> {
    application
        .output_erased(instance, T::ID, T::VERSION)
        .map(Envelope::open::<T>)
}

pub(crate) fn compose_descriptor(manifest: &MicroManifest) -> MicroDescriptor {
    MicroDescriptor {
        id: ComposeMicroId::new(manifest.id.as_str()),
        version: manifest.version,
        provisions: compose_provisions(&manifest.provisions),
        requirements: compose_provisions(&manifest.requirements),
    }
}

fn compose_provisions(references: &[crate::manifest::ProvisionRef]) -> Vec<ComposeMicroProvision> {
    references
        .iter()
        .map(|reference| ComposeMicroProvision {
            id: reference.id.clone(),
            version: reference.version,
        })
        .collect()
}

pub(crate) fn routine_facets(
    facets: &[FacetAccess],
) -> Vec<daybook_types::manifest::RoutineFacetAccess> {
    facets
        .iter()
        .map(|facet| daybook_types::manifest::RoutineFacetAccess {
            owner_plug_id: facet.owner_plug_id.clone(),
            tag: facet.tag.clone().into(),
            key_id: facet.key_id.clone(),
            read: facet.read,
            write: facet.write,
            create: facet.create,
            delete: facet.delete,
        })
        .collect()
}

#[cfg(test)]
mod tests;
