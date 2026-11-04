//! The in-process backend: one authoring API over the host's session machinery. Native micros
//! depend on this module, never on a renderer crate or the host kernel directly (ADR 015 §3
//! dependency direction). The MDK adds ergonomics, never authority: every operation below
//! still runs through the same host sessions a guest's engine drives, so a native micro and a
//! guest are interchangeable to every other micro (ADR 025 §5).
//!
//! A native micro is one [`factory`]: a manifest plus an `activate` function. The supervisor
//! runs it as an incarnation like any guest: `activate` stages publications through the
//! [`Context`], and what it staged is admitted against the manifest's declared publications
//! and handed back by name (ADR 025 §8). Nothing here names a renderer: a [`Context`] is
//! generic over the host's [`RendererRegistry`].
//!
//! Incarnation-owned work (the collection mirrors, [`Context::own_future`]) runs as local
//! tasks, so a supervisor driving native micros runs inside a `tokio::task::LocalSet`.

mod collections;

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
pub use clay_guest_session::capabilities::CallError;
pub use clay_guest_session::capabilities::CallTarget;
pub use clay_guest_session::capabilities::CapabilityFailure;
pub use clay_guest_session::capabilities::CapabilityPublication;
pub use clay_guest_session::capabilities::CapabilityRef;
pub use clay_guest_session::capabilities::ContractIdentity;
pub use clay_guest_session::capabilities::PresentationComponent;
pub use clay_guest_session::capabilities::PublicationEndpoint;
pub use clay_guest_session::capabilities::RowAttachment;
pub use clay_guest_session::collections::WriteFailure;
pub use clay_host::AccessError;
pub use clay_host::CollectionRegistry;
pub use clay_host::Instance;
pub use clay_host::RendererRegistry;
pub use collections::Attachment;
pub use collections::CollectionWatch;
pub use collections::Registration;
pub use collections::Row;

use crate::ComponentRef;
use crate::manifest::ContractRef;
use crate::manifest::FacetAccess;
use crate::manifest::MicroManifest;
use crate::manifest::PublicationKind;
use clay_guest_session::capabilities::CapabilityLimits;
use clay_guest_session::capabilities::CapabilitySession;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_guest_session::capabilities::PublishedCapability;
use clay_guest_session::capabilities::admit_publications;
use clay_guest_session::collections::BytesRegistrations;
use clay_guest_session::collections::CollectionLimits;
use clay_guest_session::collections::CollectionSession;
use clay_host::Activator;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Initialized;
use clay_host::LocalFuture;
use clay_host::MicroId;
use clay_host::Root;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use tokio::task::JoinHandle;

/// What a composition root hands every native micro it links: the renderer registry its
/// components publish into, the document service its configuration is granted from, and the
/// declared collections it may bind.
#[derive(Clone)]
pub struct Resources<R: RendererRegistry> {
    pub registry: R,
    pub documents: DocumentService,
    pub collections: CollectionRegistry<BytesRegistrations>,
}

/// The declared collections one micro is configured to use, by collection id. Configuration
/// decides these, not the micro (ADR 025 §11); a collection in `write` is also readable.
#[derive(Clone, Debug, Default)]
pub struct CollectionBindings {
    pub read: Vec<String>,
    pub write: Vec<String>,
}

/// Local tasks an incarnation owns. Dropping them aborts them, so a failed initialization
/// leaves no work behind; an orderly close aborts and joins them.
#[derive(Default)]
struct Tasks(Vec<JoinHandle<()>>);

impl Tasks {
    async fn close(mut self) {
        for task in &self.0 {
            task.abort();
        }
        for task in std::mem::take(&mut self.0) {
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                // A task that panicked is a programming error, surfaced at the join.
                Err(error) => std::panic::resume_unwind(error.into_panic()),
            }
        }
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// The value surface one native activation may use: granted configuration documents, resolved
/// requirements, declared collections, component and call publication, and owned-task
/// registration. It never hands out the host or the registry (ADR 015 §7); [`factory`]
/// constructs it.
pub struct Context<R: RendererRegistry> {
    instance: Instance,
    micro: MicroId,
    registry: R,
    configs: BTreeMap<String, DocumentRef>,
    requirements: BTreeMap<String, CapabilityRef>,
    resources: Resources<R>,
    bindings: CollectionBindings,
    capabilities: CapabilitySession,
    sessions: CollectionSession,
    published: RefCell<Vec<(String, PublishedCapability)>>,
    tasks: RefCell<Tasks>,
}

impl<R: RendererRegistry> Context<R> {
    /// This activation's incarnation identity. Plain data; operations validate it host-side.
    pub fn instance(&self) -> Instance {
        self.instance.clone()
    }

    /// A configuration document granted from the manifest's declared ACL. `None` means the
    /// manifest declared no such binding; grants are validated before activation runs.
    pub fn config(&self, name: &str) -> Option<&DocumentRef> {
        self.configs.get(name)
    }

    /// The reference that satisfies requirement `binding`, or `None` when configuration bound
    /// nothing to it. Holding it keeps the provider incarnation live.
    pub fn require(&self, binding: &str) -> Option<&CapabilityRef> {
        self.requirements.get(binding)
    }

    /// Stage a presentation publication named `name` whose component renders `render`. The
    /// name must be one the manifest declares as a presentation; admission checks that once
    /// `activate` returns, and nothing is reachable by other micros before then.
    pub fn publish_component(
        &self,
        name: &str,
        render: impl Fn() -> R::Render + 'static,
    ) -> Result<ComponentRef, AccessError> {
        let component = self.registry.publish(&self.instance, render)?;
        self.stage(self.capabilities.publish_component(name, component))?;
        Ok(ComponentRef::from_host(component))
    }

    /// Stage a call publication named `name` served by `target` under `contract`.
    pub fn publish_call(
        &self,
        name: &str,
        contract: ContractIdentity,
        target: std::sync::Arc<dyn CallTarget>,
    ) -> Result<(), AccessError> {
        self.stage(self.capabilities.publish_call(name, contract, target))?;
        Ok(())
    }

    fn stage(
        &self,
        published: Result<PublishedCapability, CapabilityFailure>,
    ) -> Result<CapabilityPublication, AccessError> {
        let published = published.map_err(|failure| match failure {
            CapabilityFailure::Revoked => AccessError::Revoked,
            CapabilityFailure::ContractMismatch
            | CapabilityFailure::LimitExceeded
            | CapabilityFailure::Unavailable => AccessError::Denied,
        })?;
        let publication = published.publication();
        self.published
            .borrow_mut()
            .push((publication.name().to_owned(), published));
        Ok(publication)
    }

    /// Mirror a collection this micro is configured to read. The feed task is incarnation-owned.
    pub fn collection<K: DeserializeOwned + 'static, V: DeserializeOwned + 'static>(
        &self,
        id: &str,
    ) -> Result<CollectionWatch<K, V>, AccessError> {
        if !self
            .bindings
            .read
            .iter()
            .chain(&self.bindings.write)
            .any(|bound| bound == id)
        {
            return Err(AccessError::Denied);
        }
        let projection = self
            .resources
            .collections
            .collection(id)
            .ok_or(AccessError::Denied)?
            .projection();
        let reader = self.sessions.grant(projection)?;
        let (watch, feed) = CollectionWatch::start(reader, self.capabilities.clone());
        self.own_future(feed)?;
        Ok(watch)
    }

    /// Write authority over a collection this micro is configured to write, together with its
    /// mirror. The writer's rows belong to this incarnation and are retracted when it closes.
    pub fn registration<
        K: Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static,
    >(
        &self,
        id: &str,
    ) -> Result<Registration<K, V>, AccessError> {
        if !self.bindings.write.iter().any(|bound| bound == id) {
            return Err(AccessError::Denied);
        }
        let grant = self
            .resources
            .collections
            .grant(id, &self.micro)
            .map_err(|_| AccessError::Denied)?;
        let writer = self.sessions.grant_writer(grant.into())?;
        Ok(Registration::new(writer, self.collection(id)?))
    }

    /// Register incarnation-owned local work. It is dropped when this instance closes; a
    /// panic in it is raised at the incarnation's close.
    pub fn own_future(
        &self,
        future: impl Future<Output = ()> + 'static,
    ) -> Result<(), AccessError> {
        self.tasks
            .borrow_mut()
            .0
            .push(tokio::task::spawn_local(future));
        Ok(())
    }
}

type Activate<R> = Rc<dyn Fn(&Context<R>) -> Result<(), AccessError>>;

struct NativeFactory<R: RendererRegistry> {
    resources: Resources<R>,
    manifest: MicroManifest,
    declared: BTreeMap<String, DeclaredPublication>,
    bindings: CollectionBindings,
    activate: Activate<R>,
}

/// Turn a native micro (manifest + activate function) into a supervisor factory. The
/// composition root calls this; micro crates never build factories themselves.
pub fn factory<R: RendererRegistry + 'static>(
    resources: Resources<R>,
    manifest: MicroManifest,
    bindings: CollectionBindings,
    activate: impl Fn(&Context<R>) -> Result<(), AccessError> + 'static,
) -> Rc<dyn Factory<CapabilityPublication>> {
    let declared = declared_publications(&manifest);
    Rc::new(NativeFactory {
        resources,
        manifest,
        declared,
        bindings,
        activate: Rc::new(activate),
    })
}

/// Owns an incarnation's publication leases and tasks until the supervisor closes it.
struct NativeRoot {
    leases: BTreeMap<String, PublishedCapability>,
    tasks: Tasks,
}

impl Root for NativeRoot {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let Self { leases, tasks } = *self;
            tasks.close().await;
            // The leases drop with the root; the supervisor revokes the publications it
            // handed out before it tears the incarnation down.
            drop(leases);
            Ok(())
        })
    }
}

impl<R: RendererRegistry + 'static> Factory<CapabilityPublication> for NativeFactory<R> {
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
        let bindings = self.bindings.clone();
        let activate = Rc::clone(&self.activate);
        Box::pin(async move {
            let capabilities =
                CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
            capabilities.attach_activator(activator);
            let sessions =
                CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
            let configs = grant_configs(&resources.documents, &host, &instance, &manifest)?;
            let requirements = requirements
                .iter()
                .map(|(binding, publication)| {
                    capabilities
                        .mint(publication)
                        .map(|reference| (binding.clone(), reference))
                        .map_err(|failure| format!("requirement {binding}: {failure:?}"))
                })
                .collect::<Result<_, _>>()?;
            let context = Context {
                instance,
                micro: MicroId::new(manifest.id.as_str()),
                registry: resources.registry.clone(),
                configs,
                requirements,
                resources,
                bindings,
                capabilities,
                sessions,
                published: RefCell::default(),
                tasks: RefCell::default(),
            };
            activate(&context).map_err(|error| format!("activation refused: {error}"))?;
            let Context {
                published, tasks, ..
            } = context;
            let leases = admit_publications(&declared, published.into_inner())?;
            let publications = leases
                .iter()
                .map(|(name, lease)| (name.clone(), lease.publication()))
                .collect();
            Ok(Initialized {
                root: Box::new(NativeRoot {
                    leases,
                    tasks: tasks.into_inner(),
                }),
                publications,
            })
        })
    }
}

/// Grant the configuration documents a manifest requests, keyed by binding (ADR 015 §7). The
/// manifest's ACL is validated by the caller's admission; the document service enforces the
/// resolved routes.
pub(crate) fn grant_configs(
    documents: &DocumentService,
    host: &Host,
    instance: &Instance,
    manifest: &MicroManifest,
) -> Result<BTreeMap<String, DocumentRef>, String> {
    let mut configs = BTreeMap::new();
    for request in &manifest.configuration_requests {
        configs.extend(
            documents
                .grant_config(
                    host,
                    instance,
                    &request.binding,
                    routine_facets(&request.facets),
                )
                .map_err(|error| format!("config {}: {error}", request.binding))?,
        );
    }
    Ok(configs)
}

/// The publications a manifest declares, in the session's terms. A kind this build does not
/// know is not declared here, so an incarnation that returns it fails admission.
pub(crate) fn declared_publications(
    manifest: &MicroManifest,
) -> BTreeMap<String, DeclaredPublication> {
    manifest
        .publications
        .iter()
        .filter_map(|publication| {
            let (kind, contract) = match (&publication.kind, &publication.contract) {
                (PublicationKind::Presentation, _) => (
                    clay_guest_session::capabilities::PublicationKind::Presentation,
                    ContractIdentity::presentation_component(),
                ),
                (PublicationKind::Call, Some(contract)) => (
                    clay_guest_session::capabilities::PublicationKind::Call,
                    contract_identity(contract),
                ),
                (PublicationKind::Call, None) => {
                    unreachable!("a validated manifest gives every call a contract")
                }
                (PublicationKind::Other(_), _) => return None,
            };
            Some((
                publication.name.clone(),
                DeclaredPublication { kind, contract },
            ))
        })
        .collect()
}

/// A manifest contract id `namespace.name` in the session's terms: the last dotted segment is
/// the name, the rest the namespace, and the integer version is the major of `(major, 0, 0)`.
fn contract_identity(contract: &ContractRef) -> ContractIdentity {
    let (namespace, name) = contract
        .id
        .rsplit_once('.')
        .unwrap_or(("", contract.id.as_str()));
    ContractIdentity::new(namespace, name, (contract.version, 0, 0))
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
