//! The in-process backend: the host-session machinery behind [`crate::Context`]. Native micros
//! depend on the MDK, never on a renderer crate or the host kernel directly (ADR 015 §3
//! dependency direction). Every operation still runs through the same host sessions a guest's
//! engine drives, so a native micro and a guest are interchangeable to every other micro
//! (ADR 031 §5).
//!
//! A native micro is one [`factory`]: a manifest plus an async `activate` function. The
//! supervisor runs it as an incarnation like any guest: `activate` stages publications through
//! the [`Context`], and what it staged is admitted against the manifest's declared publications
//! and handed back by name (ADR 031 §8).
//!
//! Incarnation-owned work (the collection mirrors, [`Context::spawn`]) runs as local tasks, so
//! a supervisor driving native micros runs inside a `tokio::task::LocalSet`.

mod declared;

pub use clay_docs::DocumentService;
pub use clay_host::CollectionRegistry;
pub use declared::DeclaredRows;
pub use declared::declared_rows;
pub use declared::manifest_scope;
pub use declared::runtime_scope;

use clay_collection::Nsid;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_dioxus::NativeComponents;
use clay_docs::DocumentRef;
use clay_guest_session::capabilities::CallError;
use clay_guest_session::capabilities::CapabilityFailure;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_guest_session::capabilities::GuestCall;
use clay_guest_session::capabilities::GuestCallTarget;
use clay_guest_session::collections::ReadFailure;
use clay_host::Instance;
use clay_host::RendererRegistry;

use crate::Access;
use crate::Attachment;
use crate::Capability;
use crate::Collection;
use crate::Config;
use crate::Context;
use crate::Error;
use crate::context::Backend;
use crate::context::RawHandler;
use crate::context::Shared;
use crate::manifest::FacetAccess;
use crate::manifest::MicroManifest;
use crate::manifest::PublicationDeclaration;
use crate::manifest::PublicationKind;
use crate::rows::Feed;
use crate::rows::Minter;
use crate::rows::Stored;
use crate::rows::stored_page;
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
use dioxus_core::Element;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;

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

/// One local collection binding resolved to a declared collection (ADR 031 §11).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundCollection {
    /// The declared collection's id.
    pub id: Nsid,
    /// Whether the micro may also register rows; a writable binding is always readable.
    pub write: bool,
}

impl BoundCollection {
    pub fn read(id: Nsid) -> Self {
        Self { id, write: false }
    }

    pub fn write(id: Nsid) -> Self {
        Self { id, write: true }
    }
}

/// What configuration resolved each of one micro's collection requests to, by the micro's
/// local binding name; a binding absent from the map is unbound. Configuration decides these,
/// not the micro (ADR 031 §11). It is a shared handle: the composition replaces the resolution
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
pub const SHELL_ROOTS: &str = "org.example.clay.shell.roots";

/// One [`SHELL_ROOTS`] row, keyed by the registering micro's id: a title and the root
/// presentation (an attachment of the row). A `pinned` root stays on screen however the shell
/// folds its other roots (Settings can be closed; a toolbar cannot); an absent field is `false`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RootRow {
    pub title: String,
    pub content: Attachment,
    #[serde(default)]
    pub pinned: bool,
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

/// The in-process backend's state behind one [`Context`]: everything `activate` may use that
/// is not shared with the guest backend, granted before it runs. Built by [`factory`].
pub(crate) struct Activation {
    pub(crate) instance: Instance,
    pub(crate) components: NativeComponents,
    /// Every collection request the manifest declares; one configuration left unbound is `None`.
    collections: BTreeMap<String, Option<CollectionSlot>>,
    declared: BTreeMap<String, DeclaredPublication>,
    pub(crate) capabilities: CapabilitySession,
    /// Staged by `activate`; `None` once admission took them, after which publishing is a
    /// programming error.
    published: RefCell<Option<Vec<(String, PublishedCapability)>>>,
    tasks: Rc<RefCell<Tasks>>,
}

impl Activation {
    fn collection_slot(&self, binding: &str) -> Option<&CollectionSlot> {
        self.collections
            .get(binding)
            .unwrap_or_else(|| panic!("the manifest declares no collection request {binding:?}"))
            .as_ref()
    }

    pub(crate) fn collection_reader(&self, binding: &str) -> Option<CollectionReader> {
        Some(self.collection_slot(binding)?.reader.clone())
    }

    /// The writer of the collection bound to `binding`, or `None` when it is unbound. Panics
    /// when the manifest's request for it is read-only.
    pub(crate) fn registration_writer(&self, binding: &str) -> Option<Rc<RegistrationWriter>> {
        let slot = self.collection_slot(binding)?;
        Some(
            slot.writer
                .clone()
                .unwrap_or_else(|| panic!("collection request {binding:?} is read-only")),
        )
    }

    fn declared_call(&self, name: &str) -> Nsid {
        match self.declared.get(name) {
            Some(DeclaredPublication {
                kind: clay_guest_session::capabilities::PublicationKind::Call,
                contract,
            }) => contract.clone(),
            _ => panic!("the manifest declares no call publication {name:?}"),
        }
    }

    fn stage(
        &self,
        published: Result<PublishedCapability, CapabilityFailure>,
    ) -> Result<(), Error> {
        let published = published?;
        let publication = published.publication();
        self.published
            .borrow_mut()
            .as_mut()
            .expect("publications are staged only while activate runs")
            .push((publication.name().to_owned(), published));
        Ok(())
    }

    pub(crate) fn publish_component(
        &self,
        name: &str,
        render: Rc<dyn Fn() -> Element>,
    ) -> Result<(), Error> {
        let component = self.components.publish(&self.instance, move || render())?;
        self.stage(self.capabilities.publish_component(name, component))
    }

    /// Stage the call publication `name`. The publication's target must be `Send + Sync` and the
    /// handler is neither, so the target only queues the request; the loop below runs handlers
    /// on the local executor, exactly as an engine's driver serves a guest's
    /// `micro-lifecycle.call`.
    pub(crate) fn publish_call(
        this: &Rc<Self>,
        name: &str,
        handler: RawHandler,
    ) -> Result<(), Error> {
        let contract = this.declared_call(name);
        let (queue, mut inbox) = mpsc::unbounded_channel::<GuestCall>();
        let target = Arc::new(GuestCallTarget::new(name.to_owned(), queue));
        this.spawn(Box::pin(async move {
            let mut running = JoinSet::new();
            loop {
                tokio::select! {
                    call = inbox.recv() => {
                        let Some(GuestCall { request, reply, .. }) = call else {
                            return;
                        };
                        let handler = Rc::clone(&handler);
                        running.spawn_local(async move {
                            let outcome = handler(request)
                                .await
                                .map_err(|error| CallError::Provider(error.to_string()));
                            // A caller that stopped waiting dropped its receiver, which is how a
                            // cancelled call is told apart from a served one: nothing to answer.
                            reply.send(outcome).ok();
                        });
                    }
                    Some(finished) = running.join_next(), if !running.is_empty() => {
                        if let Err(error) = finished {
                            // A panicked handler is a programming error, raised at the close.
                            std::panic::resume_unwind(error.into_panic());
                        }
                    }
                }
            }
        }));
        this.stage(this.capabilities.publish_call(name, contract, target))
    }

    /// Run `future` as incarnation-owned local work: dropped when the instance closes, with a
    /// panic raised at the close.
    pub(crate) fn spawn(&self, future: Pin<Box<dyn Future<Output = ()>>>) {
        self.tasks
            .borrow_mut()
            .push(tokio::task::spawn_local(future));
    }
}

/// Keep the in-process mirror of the collection `reader` names current: long-poll the
/// projection lease and fold each page into the [`Feed`]'s mirror. Runs until the incarnation
/// closes.
pub(crate) async fn feed(reader: CollectionReader, feed: Feed) {
    loop {
        // The empty selector is the whole collection.
        let lease = reader
            .acquire(Vec::new(), None)
            .expect("a granted reader acquires its collection");
        feed.mirror.borrow_mut().begin_lease(None);
        loop {
            let ticket = feed.mirror.borrow_mut().begin_read();
            // No timeout: a caught-up lease parks until a change or revocation.
            let read = lease
                .read(
                    ticket.committed().cloned(),
                    ReadLimits::default(),
                    std::future::pending(),
                )
                .await;
            match read {
                Ok(page) => {
                    assert!(
                        feed.mirror
                            .borrow_mut()
                            .apply(&ticket, &stored_page(page, Stored::InProcess))
                    );
                    feed.applied();
                }
                Err(ReadFailure::Read(ReadError::ResetRequired)) => {
                    feed.mirror.borrow_mut().clear();
                    feed.applied();
                    break;
                }
                // The incarnation closed: nothing is left to mirror.
                Err(ReadFailure::Read(ReadError::Revoked)) => return,
                Err(ReadFailure::Read(ReadError::SourceUnavailable)) => {
                    unimplemented!("registration sources are always available")
                }
                Err(failure @ (ReadFailure::Busy | ReadFailure::PageLimit)) => {
                    panic!("mirror read failed: {failure:?}")
                }
            }
        }
    }
}

impl<K: serde::de::DeserializeOwned, V: serde::de::DeserializeOwned> Collection<K, V> {
    /// Mirror `collection` for a participant that is not a micro, such as a shell: `instance`
    /// is the participant's own instance and `activator` is what its rows' lazy references
    /// activate through. Returns the mirror and the feed loop, which the caller must drive
    /// until `instance` closes.
    pub fn observe(
        host: &Host,
        instance: &Instance,
        activator: Activator<CapabilityPublication>,
        collection: &BytesRegistrations,
    ) -> Result<(Self, impl Future<Output = ()> + 'static + use<K, V>), Error> {
        let capabilities =
            CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
        capabilities.attach_activator(activator);
        let sessions =
            CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
        let reader = sessions.grant(collection.projection())?;
        Ok(Self::start(reader, capabilities))
    }

    /// Start mirroring `reader`. Returns the mirror and the loop that feeds it, which the
    /// caller owns and must drive until the incarnation closes.
    pub(crate) fn start(
        reader: CollectionReader,
        capabilities: CapabilitySession,
    ) -> (Self, impl Future<Output = ()> + 'static + use<K, V>) {
        let (collection, state) = Self::new(Minter::InProcess(capabilities));
        (collection, feed(reader, state))
    }
}

type Activate = Rc<dyn Fn(Context) -> LocalFuture<'static, Result<(), Error>>>;

struct NativeFactory {
    resources: Resources,
    micro: MicroId,
    manifest: MicroManifest,
    declared: BTreeMap<String, DeclaredPublication>,
    bindings: CollectionBindings,
    activate: Activate,
}

/// The supervisor's opaque name for an installed micro is its id text.
impl From<&crate::MicroId> for MicroId {
    fn from(micro: &crate::MicroId) -> Self {
        Self::new(micro.as_str())
    }
}

/// Turn a native micro (identity + manifest + `activate` function) into a supervisor factory.
/// The identity is the package envelope's, not the manifest's. The composition root calls this;
/// micro crates never build factories themselves.
pub fn factory<Fut: Future<Output = Result<(), Error>> + 'static>(
    resources: Resources,
    micro: crate::MicroId,
    manifest: MicroManifest,
    bindings: CollectionBindings,
    activate: impl Fn(Context) -> Fut + 'static,
) -> Rc<dyn Factory<CapabilityPublication>> {
    let declared = declared_publications(&manifest);
    Rc::new(NativeFactory {
        resources,
        micro: (&micro).into(),
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
        let micro = self.micro.clone();
        let manifest = self.manifest.clone();
        let declared = self.declared.clone();
        let bindings = self.bindings.snapshot();
        let activate = Rc::clone(&self.activate);
        Box::pin(async move {
            let capabilities =
                CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
            capabilities.attach_activator(activator);
            capabilities.attach_own(&micro, &declared);
            let sessions =
                CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
            let configs = grant_configs(&resources.documents, &host, &instance, &manifest)?
                .into_iter()
                .map(|(binding, document)| (binding, Config::in_process(document)))
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
                                .map(Capability::in_process)
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
                            let writer = (bound.write && request.access == Some(Access::Write))
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
            let activation = Rc::new(Activation {
                instance,
                components: resources.components.clone(),
                collections,
                declared: declared.clone(),
                capabilities,
                published: RefCell::new(Some(Vec::new())),
                tasks: Rc::default(),
            });
            let context = Context(Rc::new(Shared {
                configs,
                requirements,
                backend: Backend::InProcess(Rc::clone(&activation)),
            }));
            activate(context)
                .await
                .map_err(|error| format!("activation refused: {error}"))?;
            let published = activation
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
                    tasks: Rc::clone(&activation.tasks),
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
        let document = documents
            .grant_config(
                host,
                instance,
                &request.binding,
                routine_facets(&request.facets),
            )
            .map_err(|error| format!("config {}: {error}", request.binding))?;
        configs.insert(request.binding.clone(), document);
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
            // A manifest request never names the plug that owns the document (ADR 028 §4).
            owner_plug_id: None,
            tag: facet.tag.as_str().into(),
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
) -> Option<(clay_guest_session::capabilities::PublicationKind, Nsid)> {
    use clay_guest_session::capabilities::PublicationKind as Kind;
    match (&publication.kind, &publication.contract) {
        (PublicationKind::Presentation, _) => Some((
            Kind::Presentation,
            clay_guest_session::capabilities::presentation_component(),
        )),
        (PublicationKind::Call, Some(contract)) => Some((Kind::Call, contract.clone())),
        (PublicationKind::Call, None) => {
            unreachable!("a validated manifest gives every call a contract")
        }
        (PublicationKind::Other(_), _) => None,
    }
}

// The authoring vocabulary's view of the host's results. Each is a runtime condition an author
// may handle (see [`Error`]).

impl From<clay_host::AccessError> for Error {
    fn from(error: clay_host::AccessError) -> Self {
        match error {
            clay_host::AccessError::Denied => Self::Denied,
            clay_host::AccessError::Revoked => Self::Revoked,
        }
    }
}

impl From<clay_docs::DocumentError> for Error {
    fn from(error: clay_docs::DocumentError) -> Self {
        match error {
            clay_docs::DocumentError::Access(error) => error.into(),
            clay_docs::DocumentError::SubscriptionLagged => Self::Lagged,
        }
    }
}

impl From<CapabilityFailure> for Error {
    fn from(failure: CapabilityFailure) -> Self {
        match failure {
            CapabilityFailure::Revoked => Self::Revoked,
            CapabilityFailure::Unavailable => Self::Unavailable,
            CapabilityFailure::ContractMismatch => Self::Denied,
            CapabilityFailure::LimitExceeded => Self::Limit,
        }
    }
}

impl From<clay_guest_session::collections::WriteFailure> for Error {
    fn from(failure: clay_guest_session::collections::WriteFailure) -> Self {
        use clay_guest_session::collections::WriteFailure;
        match failure {
            WriteFailure::Conflict { current } => Self::Conflict(current),
            WriteFailure::Revoked => Self::Revoked,
            WriteFailure::RowLimit | WriteFailure::CapabilityLimit => Self::Limit,
            WriteFailure::KeyTooLarge | WriteFailure::ValueTooLarge => {
                Self::Rejected("row exceeds the collection's size limits".into())
            }
            WriteFailure::InvalidAttachment => {
                Self::Rejected("row names an attachment it does not carry".into())
            }
        }
    }
}

impl From<CallError> for Error {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Capability(failure) => failure.into(),
            CallError::Provider(message) => Self::Rejected(message),
        }
    }
}

impl From<clay_guest_session::capabilities::RefStatus> for crate::RefStatus {
    fn from(status: clay_guest_session::capabilities::RefStatus) -> Self {
        use clay_guest_session::capabilities::RefStatus;
        match status {
            RefStatus::Live => Self::Live,
            RefStatus::Dormant => Self::Dormant,
            RefStatus::Disabled => Self::Disabled,
            RefStatus::Faulted => Self::Faulted,
            RefStatus::Unavailable => Self::Unavailable,
            RefStatus::Revoked => Self::Revoked,
        }
    }
}

impl From<crate::RefStatus> for clay_guest_session::capabilities::RefStatus {
    fn from(status: crate::RefStatus) -> Self {
        use crate::RefStatus;
        match status {
            RefStatus::Live => Self::Live,
            RefStatus::Dormant => Self::Dormant,
            RefStatus::Disabled => Self::Disabled,
            RefStatus::Faulted => Self::Faulted,
            RefStatus::Unavailable => Self::Unavailable,
            RefStatus::Revoked => Self::Revoked,
        }
    }
}

impl From<clay_docs::OperationReceipt> for crate::OperationReceipt {
    fn from(receipt: clay_docs::OperationReceipt) -> Self {
        use clay_docs::OperationError;
        use clay_docs::OperationStatus;
        Self {
            id: receipt.id,
            revision: receipt.revision,
            status: match receipt.status {
                OperationStatus::LocallyApplied => crate::OperationStatus::LocallyApplied,
                OperationStatus::Accepted => crate::OperationStatus::Accepted,
                OperationStatus::RolledBack(OperationError::Rejected { message }) => {
                    crate::OperationStatus::RolledBack(crate::OperationError::Rejected { message })
                }
                OperationStatus::RolledBack(OperationError::Transport {
                    message,
                    outcome_unknown,
                }) => crate::OperationStatus::RolledBack(crate::OperationError::Transport {
                    message,
                    outcome_unknown,
                }),
            },
        }
    }
}
