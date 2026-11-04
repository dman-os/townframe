//! The in-process backend: the host-session machinery behind [`crate::Context`]. Native micros
//! depend on the MDK, never on a renderer crate or the host kernel directly (ADR 015 §3
//! dependency direction). Every operation still runs through the same host sessions a guest's
//! engine drives, so a native micro and a guest are interchangeable to every other micro
//! (ADR 025 §5).
//!
//! A native micro is one [`factory`]: a manifest plus an async `activate` function. The
//! supervisor runs it as an incarnation like any guest: `activate` stages publications through
//! the [`Context`], and what it staged is admitted against the manifest's declared publications
//! and handed back by name (ADR 025 §8).
//!
//! Incarnation-owned work (the collection mirrors, [`Context::spawn`]) runs as local tasks, so
//! a supervisor driving native micros runs inside a `tokio::task::LocalSet`.

mod collections;
mod declared;

pub use clay_docs::DocumentService;
pub use clay_host::CollectionRegistry;
pub use collections::Attachment;
pub use collections::Collection;
pub use collections::Registration;
pub use collections::Row;
pub use declared::DeclaredRows;
pub use declared::declared_rows;
pub use declared::manifest_scope;
pub use declared::runtime_scope;

use clay_dioxus::NativeComponents;
use clay_docs::DocumentRef;
use clay_guest_session::capabilities::CapabilityFailure;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_host::Instance;

use crate::Capability;
use crate::CollectionAccess;
use crate::Config;
use crate::Context;
use crate::Error;
use crate::manifest::ContractIdentity;
use crate::manifest::FacetAccess;
use crate::manifest::MicroManifest;
use crate::manifest::PublicationDeclaration;
use crate::manifest::PublicationKind;
use clay_guest_session::capabilities::CapabilityLimits;
use clay_guest_session::capabilities::CapabilitySession;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_guest_session::capabilities::PublishedCapability;
use clay_guest_session::capabilities::admit_publications;
use clay_guest_session::collections::BytesRegistrations;
use clay_guest_session::collections::CollectionLimits;
use clay_guest_session::collections::CollectionReader;
use clay_guest_session::collections::CollectionSession;
use clay_guest_session::collections::RegistrationWriter;
use clay_host::Activator;
use clay_host::Factory;
use clay_host::Host;
use clay_host::Initialized;
use clay_host::LocalFuture;
use clay_host::MicroId;
use clay_host::Root;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use tokio::task::JoinHandle;

/// What a composition root hands every micro it links: the component registry its
/// presentations publish through (native components render into it, and a guest engine attaches
/// its wire surfaces to it), the document service its configuration is granted from, and the
/// declared collections it may bind. The registry is the concrete Dioxus one: it is the only
/// renderer there is, and a guest's surface can only attach to a registry that owns surfaces.
#[derive(Clone)]
pub struct Resources {
    pub components: NativeComponents,
    pub documents: DocumentService,
    pub collections: CollectionRegistry<BytesRegistrations>,
}

impl Resources {
    pub fn new(
        components: NativeComponents,
        documents: DocumentService,
        collections: CollectionRegistry<BytesRegistrations>,
    ) -> Self {
        Self {
            components,
            documents,
            collections,
        }
    }
}

/// One local collection binding resolved to a declared collection (ADR 025 §11).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundCollection {
    /// The declared collection's id.
    pub id: String,
    /// Whether the micro may also register rows; a writable binding is always readable.
    pub write: bool,
}

impl BoundCollection {
    pub fn read(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            write: false,
        }
    }

    pub fn write(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            write: true,
        }
    }
}

/// What configuration resolved each of one micro's collection requests to, by the micro's
/// local binding name; a binding absent from the map is unbound. Configuration decides these,
/// not the micro (ADR 025 §11). It is a shared handle: the composition replaces the resolution
/// when the micro's binding datum changes, and the factory snapshots it at each `initialize`,
/// so an incarnation sees exactly the resolution current when it started and never changes
/// under it (there is no hot rebinding).
#[derive(Clone, Debug, Default)]
pub struct CollectionBindings(Rc<RefCell<BTreeMap<String, BoundCollection>>>);

impl CollectionBindings {
    pub fn new(bound: impl IntoIterator<Item = (String, BoundCollection)>) -> Self {
        Self(Rc::new(RefCell::new(bound.into_iter().collect())))
    }

    /// Replace the resolution; incarnations already running keep the one they started with.
    pub fn replace(&self, bound: BTreeMap<String, BoundCollection>) {
        *self.0.borrow_mut() = bound;
    }

    pub fn snapshot(&self) -> BTreeMap<String, BoundCollection> {
        self.0.borrow().clone()
    }
}

/// The declared registration collection a shell reads its roots from: micros register a
/// [`RootRow`] (usually as a manifest-declared row) and the shell redeems the presentation each
/// one attaches.
pub const SHELL_ROOTS: &str = "shell.roots";

/// One `shell.roots` row, keyed by the registering micro's id: a title and the root
/// presentation (an attachment of the row).
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RootRow {
    pub title: String,
    pub content: Attachment,
}

/// Local tasks an incarnation owns. Dropping them aborts them, so a failed initialization
/// leaves no work behind; an orderly close aborts and joins them.
#[derive(Default)]
pub(crate) struct Tasks(Vec<JoinHandle<()>>);

impl Tasks {
    pub(crate) fn push(&mut self, task: JoinHandle<()>) {
        self.0.push(task);
    }

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

/// One resolved collection request: the reader (and, for a write request, the writer) was
/// granted before `activate` ran, so the accessors cannot fail.
pub(crate) struct CollectionSlot {
    pub(crate) reader: CollectionReader,
    pub(crate) writer: Option<Rc<RegistrationWriter>>,
}

/// The in-process backend's state behind one [`Context`]: everything `activate` may use, granted
/// before it runs. Built by [`factory`].
pub(crate) struct Activation {
    pub(crate) instance: Instance,
    pub(crate) components: NativeComponents,
    pub(crate) configs: BTreeMap<String, Config>,
    /// Every requirement the manifest declares; an optional one nothing resolved holds `None`.
    pub(crate) requirements: BTreeMap<String, Option<Capability>>,
    /// Every collection request the manifest declares; one configuration left unbound is `None`.
    pub(crate) collections: BTreeMap<String, Option<CollectionSlot>>,
    pub(crate) declared: BTreeMap<String, DeclaredPublication>,
    pub(crate) capabilities: CapabilitySession,
    /// Staged by `activate`; `None` once admission took them, after which publishing is a
    /// programming error.
    pub(crate) published: RefCell<Option<Vec<(String, PublishedCapability)>>>,
    pub(crate) tasks: Rc<RefCell<Tasks>>,
}

impl Activation {
    pub(crate) fn collection_slot(&self, binding: &str) -> Option<&CollectionSlot> {
        self.collections
            .get(binding)
            .unwrap_or_else(|| panic!("the manifest declares no collection request {binding:?}"))
            .as_ref()
    }

    pub(crate) fn declared_call(
        &self,
        name: &str,
    ) -> clay_guest_session::capabilities::ContractIdentity {
        match self.declared.get(name) {
            Some(DeclaredPublication {
                kind: clay_guest_session::capabilities::PublicationKind::Call,
                contract,
            }) => contract.clone(),
            _ => panic!("the manifest declares no call publication {name:?}"),
        }
    }

    pub(crate) fn stage(
        &self,
        published: Result<PublishedCapability, CapabilityFailure>,
    ) -> Result<CapabilityPublication, Error> {
        let published = published?;
        let publication = published.publication();
        self.published
            .borrow_mut()
            .as_mut()
            .expect("publications are staged only while activate runs")
            .push((publication.name().to_owned(), published));
        Ok(publication)
    }
}

type Activate = Rc<dyn Fn(Context) -> LocalFuture<'static, Result<(), Error>>>;

struct NativeFactory {
    resources: Resources,
    manifest: MicroManifest,
    declared: BTreeMap<String, DeclaredPublication>,
    bindings: CollectionBindings,
    activate: Activate,
}

/// Turn a native micro (manifest + `activate` function) into a supervisor factory. The
/// composition root calls this; micro crates never build factories themselves.
pub fn factory<Fut: Future<Output = Result<(), Error>> + 'static>(
    resources: Resources,
    manifest: MicroManifest,
    bindings: CollectionBindings,
    activate: impl Fn(Context) -> Fut + 'static,
) -> Rc<dyn Factory<CapabilityPublication>> {
    let declared = declared_publications(&manifest);
    Rc::new(NativeFactory {
        resources,
        manifest,
        declared,
        bindings,
        activate: Rc::new(move |context| Box::pin(activate(context))),
    })
}

/// Owns an incarnation's publication leases and tasks until the supervisor closes it.
struct NativeRoot {
    leases: BTreeMap<String, PublishedCapability>,
    tasks: Rc<RefCell<Tasks>>,
}

impl Root for NativeRoot {
    fn close(self: Box<Self>) -> LocalFuture<'static, Result<(), String>> {
        Box::pin(async move {
            let Self { leases, tasks } = *self;
            // Taken out of the shared cell: the tasks themselves may still hold the context.
            let tasks = std::mem::take(&mut *tasks.borrow_mut());
            tasks.close().await;
            // The leases drop with the root; the supervisor revokes the publications it
            // handed out before it tears the incarnation down.
            drop(leases);
            Ok(())
        })
    }
}

impl Factory<CapabilityPublication> for NativeFactory {
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
        let activate = Rc::clone(&self.activate);
        Box::pin(async move {
            let micro = MicroId::new(manifest.id.as_str());
            let capabilities =
                CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
            capabilities.attach_activator(activator);
            let sessions =
                CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
            let configs = grant_configs(&resources.documents, &host, &instance, &manifest)?
                .into_iter()
                .map(|(binding, document)| (binding, Config::new(document)))
                .collect();
            let requirements = manifest
                .requires
                .iter()
                .map(|required| {
                    let resolved = requirements
                        .get(&required.binding)
                        .map(|publication| {
                            capabilities
                                .mint(publication)
                                .map(Capability::new)
                                .map_err(|failure| {
                                    format!("requirement {}: {failure:?}", required.binding)
                                })
                        })
                        .transpose()?;
                    Ok((required.binding.clone(), resolved))
                })
                .collect::<Result<_, String>>()?;
            // Grant every resolved collection request up front so the accessors cannot fail
            // mid-activation. A bound collection that is not declared is a configuration
            // fault, reported as a failed initialization like the guest engine's.
            let collections = manifest
                .collection_requests
                .iter()
                .map(|request| {
                    let slot = bindings
                        .get(&request.binding)
                        .map(|bound| {
                            let reader = resources
                                .collections
                                .collection(&bound.id)
                                .ok_or_else(|| format!("collection {} is not declared", bound.id))
                                .and_then(|collection| {
                                    sessions
                                        .grant(collection.projection())
                                        .map_err(|error| error.to_string())
                                })?;
                            let writer = (bound.write && request.access == CollectionAccess::Write)
                                .then(|| {
                                    resources
                                        .collections
                                        .grant(&bound.id, &micro)
                                        .map_err(|error| error.to_string())
                                        .and_then(|grant| {
                                            sessions
                                                .grant_writer(grant)
                                                .map_err(|error| error.to_string())
                                        })
                                        .map(Rc::new)
                                })
                                .transpose()?;
                            Ok::<_, String>(CollectionSlot { reader, writer })
                        })
                        .transpose()?;
                    Ok((request.binding.clone(), slot))
                })
                .collect::<Result<_, String>>()?;
            let context = Context(Rc::new(Activation {
                instance,
                components: resources.components.clone(),
                configs,
                requirements,
                collections,
                declared: declared.clone(),
                capabilities,
                published: RefCell::new(Some(Vec::new())),
                tasks: Rc::default(),
            }));
            activate(context.clone())
                .await
                .map_err(|error| format!("activation refused: {error}"))?;
            let published = context
                .0
                .published
                .borrow_mut()
                .take()
                .expect("admission is the only taker of staged publications");
            let leases = admit_publications(&declared, published)?;
            let publications = leases
                .iter()
                .map(|(name, lease)| (name.clone(), lease.publication()))
                .collect();
            Ok(Initialized {
                root: Box::new(NativeRoot {
                    leases,
                    tasks: Rc::clone(&context.0.tasks),
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
            let (kind, contract) = admitted_publication(publication)?;
            Some((
                publication.name.clone(),
                DeclaredPublication { kind, contract },
            ))
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

/// The kind and contract the capability bank admits `publication` under, or `None` for a kind
/// this build does not serve. Every Presentation shares the one presentation-component
/// contract, so a contract alone tells a redeemer which kind to expect. Expects a validated
/// manifest, which gives every Call a contract.
pub(crate) fn admitted_publication(
    publication: &PublicationDeclaration,
) -> Option<(
    clay_guest_session::capabilities::PublicationKind,
    clay_guest_session::capabilities::ContractIdentity,
)> {
    use clay_guest_session::capabilities::PublicationKind as Kind;
    match (&publication.kind, &publication.contract) {
        (PublicationKind::Presentation, _) => Some((
            Kind::Presentation,
            clay_guest_session::capabilities::ContractIdentity::presentation_component(),
        )),
        (PublicationKind::Call, Some(contract)) => Some((Kind::Call, contract.admitted())),
        (PublicationKind::Call, None) => {
            unreachable!("a validated manifest gives every call a contract")
        }
        (PublicationKind::Other(_), _) => None,
    }
}

impl ContractIdentity {
    /// The capability bank's form of this manifest contract; the fields carry over unchanged.
    pub(crate) fn admitted(&self) -> clay_guest_session::capabilities::ContractIdentity {
        clay_guest_session::capabilities::ContractIdentity::new(
            self.namespace.clone(),
            self.name.clone(),
            self.version,
        )
    }
}
