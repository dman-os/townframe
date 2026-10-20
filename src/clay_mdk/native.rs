//! The in-process backend: one authoring API over the host's composition machinery. Native
//! micros depend on this module, never on `clay_compose`, `clay_dioxus`, or the host kernel
//! directly (ADR 015 §3 dependency direction, §Boundary correction). The MDK adds ergonomics,
//! never authority: every operation below still runs through the host's checked services.

pub use clay_data::{
    DocumentRef, DocumentService, DocumentSnapshot, DocumentWatch, Facet, FacetRoute, Operation,
    OperationReceipt,
};
pub use clay_host::{AccessError, CollectionReader, CollectionWriter, Entry, Instance};

#[cfg(feature = "wasmtime")]
use crate::manifest::ConfigGrant;
use crate::{
    ComponentRef,
    manifest::{FacetAccess, MicroManifest, Provision},
};
#[cfg(feature = "wasmtime")]
use clay_compose::micros::{
    ConfigManifest as ComposeConfigManifest, MicroManifest as ComposeMicroManifest,
};
use clay_compose::micros::{
    MicroActivation, MicroDescriptor, MicroFactory, MicroId as ComposeMicroId,
    MicroProvision as ComposeMicroProvision, Provision as ComposeProvision,
};
use clay_dioxus::{ComponentMount, NativeComponents, application::ApplicationRuntime};
use clay_host::{HostedCollection, RootMount};
use dioxus::prelude::*;
use std::collections::BTreeMap;

/// Adapter bridge between the MDK's contract vocabulary and the composition registry's typed
/// provision storage. Host-side retrieval of an MDK-published provision goes through
/// [`output`]; the wrapper never crosses into micro code.
#[derive(Clone)]
pub struct Provided<T>(pub T);

impl<T: Provision> ComposeProvision for Provided<T> {
    const ID: &'static str = T::ID;
    const VERSION: u32 = T::VERSION;
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
pub struct Context {
    activation: MicroActivation,
    native: NativeComponents,
    roots: HostedCollection<RootMount>,
    configs: BTreeMap<String, DocumentRef>,
}

impl Context {
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
            .require::<Provided<T>>()
            .map(|value| value.0)
    }

    /// Stage a provision for commit when this activation succeeds. A failed or dropped
    /// activation rolls back every staged publication.
    pub fn publish<T: Provision>(&self, value: T) {
        self.activation.publish(Provided(value));
    }

    /// A contribution collection provided by this incarnation.
    pub fn collection<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> Result<Collection<T>, AccessError> {
        self.activation.collection().map(Collection::from_host)
    }

    /// Publish a Dioxus render factory as a component of this incarnation. The returned
    /// reference is transport data for collection entries and mounts; rendering resolves the
    /// factory host-side.
    pub fn publish_component(
        &self,
        render: impl Fn() -> Element + 'static,
    ) -> Result<ComponentRef, AccessError> {
        let instance = self.activation.instance();
        self.native
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

/// The view-side counterpart of [`Context::publish_component`]: mounts a published component
/// into the host tree, resolving the renderer registry from Dioxus context. Backend-owned so
/// micro views never name a renderer crate; the WIT backend gains the matching slot component
/// with the collections waist.
#[derive(Props, Clone, PartialEq)]
pub struct ComponentSlotProps {
    pub owner: Instance,
    pub reference: ComponentRef,
}

#[expect(non_snake_case, reason = "Dioxus component naming")]
pub fn ComponentSlot(props: ComponentSlotProps) -> Element {
    let reference = props.reference.host();
    rsx! { ComponentMount { owner: props.owner, reference } }
}

/// Turn a native micro (manifest + activate function) into a composition provider. The
/// composition root calls this; micro crates never build providers themselves.
pub fn provider(
    application: &ApplicationRuntime,
    manifest: MicroManifest,
    activate: impl Fn(&Context) -> Result<(), AccessError> + 'static,
) -> MicroFactory {
    let native = application.native();
    let roots = application.root_mounts();
    let descriptor = compose_descriptor(&manifest);
    let config = manifest.config.clone();
    let activate = std::rc::Rc::new(activate);
    MicroFactory::new(descriptor, move |activation| {
        let configs = match &config {
            Some(grant) => activation.grant_config(&grant.name, routine_facets(&grant.facets))?,
            None => BTreeMap::new(),
        };
        let context = Context {
            activation,
            native: native.clone(),
            roots: roots.clone(),
            configs,
        };
        activate(&context)
    })
}

/// Turn a wasm micro (manifest + precompiled package) into a composition provider. Mirrors the
/// renderer adapter's generic wasm provider but keeps micro-facing types in the MDK vocabulary:
/// the contribution callback receives an authoring [`Context`], not the registry's activation.
#[cfg(feature = "wasmtime")]
pub fn wasm_provider<C: 'static>(
    application: &ApplicationRuntime,
    manifest: MicroManifest<C>,
    package: clay_dioxus::wasmtime::WasmPackage,
    contribute: impl Fn(&Context, &C, ComponentRef) -> Result<(), AccessError> + 'static,
) -> MicroFactory {
    let native = application.native();
    let roots = application.root_mounts();
    let compose_manifest = ComposeMicroManifest {
        descriptor: compose_descriptor(&manifest),
        config: manifest.config.as_ref().map(compose_config),
        contributions: manifest.contributions,
    };
    clay_dioxus::wasmtime::provider(
        compose_manifest,
        package,
        native.clone(),
        move |activation, contribution, detail| {
            let context = Context {
                activation: activation.clone(),
                native: native.clone(),
                roots: roots.clone(),
                configs: BTreeMap::new(),
            };
            contribute(&context, contribution, ComponentRef::from_host(detail))
        },
    )
}

/// Host-side retrieval of an MDK-published provision (composition roots and tests).
pub fn output<T: Provision>(
    application: &ApplicationRuntime,
    instance: &str,
) -> Result<T, AccessError> {
    application
        .output::<Provided<T>>(instance)
        .map(|provided| provided.0)
}

fn compose_descriptor<C>(manifest: &MicroManifest<C>) -> MicroDescriptor {
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

#[cfg(feature = "wasmtime")]
fn compose_config(grant: &ConfigGrant) -> ComposeConfigManifest {
    ComposeConfigManifest {
        name: grant.name.clone(),
        facets: routine_facets(&grant.facets),
    }
}

fn routine_facets(facets: &[FacetAccess]) -> Vec<daybook_types::manifest::RoutineFacetAccess> {
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
