//! The micro authoring surface (ADR 015 §5): one [`Context`] a micro's `activate` receives, one
//! [`Error`], and the typed handles it hands out. Authors write
//! `async fn activate(cx: Context) -> Result<(), Error>` and nothing in it names a backend.
//!
//! The MDK adds ergonomics, never authority: every operation below runs through the host
//! sessions, whether the micro is linked in-process (feature `in_process`, built by
//! [`crate::host::factory`]) or runs as a wasm guest (feature `guest`, exported by
//! [`crate::export_micro!`]). A micro is interchangeable to every other micro either way
//! (ADR 025 §5). Each public method here is written once and delegates to the backend's arm of
//! one private enum, so the two backends cannot drift in signature; a build that enables both
//! features holds either in a [`Context`] and every handle it mints stays with its backend.
//!
//! Handles are plain values a component captures in its `render` closure: there is no ambient
//! context to look up. Components subscribe through the hooks in [`crate::hooks`].
use crate::Collection;
use crate::FacetState;
use crate::Registration;
use crate::rows::Minter;
use crate::rows::Writer;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

pub use clay_collection::Stamp;
#[cfg(feature = "in_process")]
pub use clay_host::Instance;

/// Why an authoring operation failed. Every variant is a runtime condition an author may
/// handle or propagate; programming errors (an undeclared binding, a row that violates its
/// contract) crash instead.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("capability denied")]
    Denied,
    /// The incarnation, or what the handle names, has ended.
    #[error("capability revoked")]
    Revoked,
    /// A lazy reference's provider is disabled, faulted, or does not publish the name.
    #[error("provider unavailable")]
    Unavailable,
    #[error("a limit of the incarnation was reached")]
    Limit,
    /// The watch fell behind the host's stream; reread the current state.
    #[error("subscription lagged")]
    Lagged,
    /// A value did not decode as the contract's type.
    #[error("invalid value: {0}")]
    InvalidValue(String),
    /// A compare-and-set write lost to the row at this stamp (`None`: the key is absent).
    #[error("write conflicts with the row at {0:?}")]
    Conflict(Option<Stamp>),
    /// The provider of a call, or a registration's admission, rejected the request.
    #[error("rejected: {0}")]
    Rejected(String),
}

/// What a reference reports without activating anything (ADR 024 §9 `ref-status`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefStatus {
    Live,
    Dormant,
    Disabled,
    Faulted,
    Unavailable,
    Revoked,
}

/// A local write's acknowledgement state (ADR 015 §4).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationStatus {
    LocallyApplied,
    Accepted,
    RolledBack(OperationError),
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OperationError {
    #[error("operation rejected: {message}")]
    Rejected { message: String },
    #[error("transport failure (outcome unknown: {outcome_unknown}): {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationReceipt {
    pub id: u64,
    pub revision: u64,
    pub status: OperationStatus,
}

/// A reference another micro's publication minted for this one: a resolved requirement or a
/// row attachment. Holding it does not keep the provider running unless it is redeemed (a slot
/// mounting a presentation) or retained; [`Self::status`] never activates anything.
pub struct Capability(CapabilityInner);

enum CapabilityInner {
    #[cfg(feature = "in_process")]
    InProcess(clay_guest_session::capabilities::CapabilityRef),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::capability_bank::CapabilityRef>),
}

impl Capability {
    #[cfg(feature = "in_process")]
    pub(crate) fn in_process(reference: clay_guest_session::capabilities::CapabilityRef) -> Self {
        Self(CapabilityInner::InProcess(reference))
    }

    #[cfg(feature = "guest")]
    pub(crate) fn guest(reference: Rc<crate::capability_bank::CapabilityRef>) -> Self {
        Self(CapabilityInner::Guest(reference))
    }

    pub fn status(&self) -> RefStatus {
        match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => reference.status().into(),
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => reference.status().into(),
        }
    }

    /// One call of the provider's call publication, CBOR-encoded both ways. The provider is
    /// held strongly for the duration of the call only.
    pub async fn call<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: &Req,
    ) -> Result<Resp, Error> {
        let request = encode(request);
        let response = match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => reference.call(request).await?,
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => reference.call(request).await?,
        };
        ciborium::from_reader(response.as_slice())
            .map_err(|error| Error::InvalidValue(error.to_string()))
    }

    /// Redeem the provider's presentation, activating the provider if the reference is lazy.
    /// The returned proxy holds the provider live for as long as it is held, so a slot keeps it
    /// for exactly the life of what it mounts (ADR 025 §9). In-process only: a guest has no way
    /// to mount a redeemed component yet.
    #[cfg(feature = "in_process")]
    pub async fn redeem_presentation(
        &self,
    ) -> Result<clay_guest_session::capabilities::PresentationComponent, Error> {
        match &self.0 {
            CapabilityInner::InProcess(reference) => Ok(reference.redeem_presentation().await?),
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(_) => unreachable!("guest references are not redeemable yet"),
        }
    }
}

impl Clone for Capability {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => CapabilityInner::InProcess(reference.share()),
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => CapabilityInner::Guest(Rc::clone(reference)),
        })
    }
}

/// Two references are equal when they store back as the same attachment (in-process) or are the
/// same held reference (guest), so a re-rendered row that still names the same target does not
/// remount its slot.
impl PartialEq for Capability {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            #[cfg(feature = "in_process")]
            (CapabilityInner::InProcess(left), CapabilityInner::InProcess(right)) => {
                left.attachment() == right.attachment()
            }
            #[cfg(feature = "guest")]
            (CapabilityInner::Guest(left), CapabilityInner::Guest(right)) => {
                Rc::ptr_eq(left, right)
            }
            #[cfg(all(feature = "in_process", feature = "guest"))]
            _ => false,
        }
    }
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("CBOR encoding of an in-memory value");
    bytes
}

/// One of this micro's own publications, staged by [`Context::publish_component`] or
/// [`Context::publish_call`], which a row can attach (see [`Registration::put`]).
#[derive(Clone)]
pub struct Publication(PublicationInner);

#[derive(Clone)]
enum PublicationInner {
    #[cfg(feature = "in_process")]
    InProcess(clay_guest_session::capabilities::CapabilityPublication),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::guest::Staged>),
}

impl Publication {
    #[cfg(feature = "in_process")]
    pub(crate) fn in_process(
        publication: clay_guest_session::capabilities::CapabilityPublication,
    ) -> Self {
        Self(PublicationInner::InProcess(publication))
    }

    #[cfg(feature = "guest")]
    pub(crate) fn guest(staged: Rc<crate::guest::Staged>) -> Self {
        Self(PublicationInner::Guest(staged))
    }
}

/// What one row of a registration attaches to its value (indexed by [`crate::Attachment`]):
/// one of this micro's own publications.
#[derive(Clone)]
pub enum RowAttachment {
    Bound(Publication),
}

impl RowAttachment {
    #[cfg(feature = "in_process")]
    pub(crate) fn into_in_process(self) -> clay_guest_session::capabilities::RowAttachment {
        let Self::Bound(Publication(publication)) = self;
        match publication {
            PublicationInner::InProcess(publication) => {
                clay_guest_session::capabilities::RowAttachment::Bound(publication)
            }
            #[cfg(feature = "guest")]
            PublicationInner::Guest(_) => unreachable!("a publication belongs to its own backend"),
        }
    }

    /// A fresh reference to the attached publication, moved into one write.
    #[cfg(feature = "guest")]
    pub(crate) fn into_guest(self) -> Result<crate::capability_bank::CapabilityRef, Error> {
        let Self::Bound(Publication(publication)) = self;
        match publication {
            PublicationInner::Guest(staged) => staged.reference(),
            #[cfg(feature = "in_process")]
            PublicationInner::InProcess(_) => {
                unreachable!("a publication belongs to its own backend")
            }
        }
    }
}

/// A configuration document granted from the manifest's declared ACL.
#[derive(Clone)]
pub struct Config(ConfigInner);

#[derive(Clone)]
enum ConfigInner {
    #[cfg(feature = "in_process")]
    InProcess(clay_docs::DocumentRef),
    #[cfg(feature = "guest")]
    Guest(crate::mirror::ConfigDocument),
}

impl Config {
    #[cfg(feature = "in_process")]
    pub(crate) fn in_process(document: clay_docs::DocumentRef) -> Self {
        Self(ConfigInner::InProcess(document))
    }

    #[cfg(feature = "guest")]
    pub(crate) fn guest(document: crate::mirror::ConfigDocument) -> Self {
        Self(ConfigInner::Guest(document))
    }

    /// The typed facet `tag`/`key_id` of this document, watched for the incarnation's life.
    pub fn facet<T: DeserializeOwned + Serialize>(
        &self,
        tag: &str,
        key_id: &str,
    ) -> Result<Facet<T>, Error> {
        Ok(Facet(match &self.0 {
            #[cfg(feature = "in_process")]
            ConfigInner::InProcess(document) => {
                FacetInner::InProcess(document.facet(tag.into(), key_id.into())?)
            }
            #[cfg(feature = "guest")]
            ConfigInner::Guest(document) => FacetInner::Guest(document.facet(tag, key_id)?),
        }))
    }
}

/// One configuration facet's decoded state at a revision of its document watch.
#[derive(Clone, Debug, PartialEq)]
pub struct FacetSnapshot<T> {
    /// `Loading` (not hydrated yet) is distinct from `Absent` (known absent after hydration).
    pub state: FacetState<T>,
    /// A targeted refresh is in flight; the state still shows the retained local value.
    pub refreshing: bool,
    pub revision: u64,
    /// Local writes not yet acknowledged.
    pub pending: usize,
}

/// A typed configuration facet: reads see local writes optimistically, writes return the
/// [`Operation`] that tracks their acknowledgement.
pub struct Facet<T>(FacetInner<T>);

enum FacetInner<T> {
    #[cfg(feature = "in_process")]
    InProcess(clay_docs::Facet<T>),
    #[cfg(feature = "guest")]
    Guest(crate::mirror::Facet<T>),
}

impl<T: Clone> Clone for Facet<T> {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            #[cfg(feature = "in_process")]
            FacetInner::InProcess(facet) => FacetInner::InProcess(facet.clone()),
            #[cfg(feature = "guest")]
            FacetInner::Guest(facet) => FacetInner::Guest(facet.clone()),
        })
    }
}

/// Two facets are equal when they watch through the same lease, so a re-rendered component
/// handed the same facet is not re-rendered for it.
impl<T> PartialEq for Facet<T> {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            #[cfg(feature = "in_process")]
            (FacetInner::InProcess(left), FacetInner::InProcess(right)) => left == right,
            #[cfg(feature = "guest")]
            (FacetInner::Guest(left), FacetInner::Guest(right)) => left == right,
            #[cfg(all(feature = "in_process", feature = "guest"))]
            _ => false,
        }
    }
}

impl<T: Clone + DeserializeOwned + Serialize> Facet<T> {
    pub fn current(&self) -> Result<FacetSnapshot<T>, Error> {
        match &self.0 {
            #[cfg(feature = "in_process")]
            FacetInner::InProcess(facet) => Ok(in_process_snapshot(facet, &facet.watch.current()?)),
            #[cfg(feature = "guest")]
            FacetInner::Guest(facet) => crate::guest::snapshot(facet),
        }
    }

    /// Submit `value`. The write is visible to reads at once and its [`Operation`] tracks the
    /// acknowledgement. A guest additionally re-reads its mirror from the host so the local
    /// value shows (the host sends no event for the guest's own write); an `Err` from that
    /// re-read describes the mirror, the write itself having been submitted.
    pub fn write(&self, value: T) -> Result<Operation, Error> {
        Ok(Operation(match &self.0 {
            #[cfg(feature = "in_process")]
            FacetInner::InProcess(facet) => OperationInner::InProcess(facet.write(value)?),
            #[cfg(feature = "guest")]
            FacetInner::Guest(facet) => {
                let operation = facet.write(&value)?;
                facet.resync()?;
                OperationInner::Guest(operation)
            }
        }))
    }

    /// Request a targeted refresh. Returns the post-request view; a pending refresh shows as
    /// [`FacetSnapshot::refreshing`], never as a missing value.
    pub fn refresh(&self) -> Result<FacetSnapshot<T>, Error> {
        match &self.0 {
            #[cfg(feature = "in_process")]
            FacetInner::InProcess(facet) => Ok(in_process_snapshot(facet, &facet.watch.refresh()?)),
            #[cfg(feature = "guest")]
            FacetInner::Guest(facet) => {
                facet.refresh()?;
                crate::guest::snapshot(facet)
            }
        }
    }

    /// The stream of this facet's changes, for a hook to turn into re-renders.
    #[cfg(any(feature = "view", feature = "guest"))]
    pub(crate) fn changes(&self) -> Result<crate::hooks::Changes, Error> {
        Ok(match &self.0 {
            #[cfg(feature = "in_process")]
            FacetInner::InProcess(facet) => {
                crate::hooks::Changes::Document(facet.watch.subscribe()?)
            }
            #[cfg(feature = "guest")]
            FacetInner::Guest(_) => crate::hooks::Changes::Pushed,
        })
    }
}

#[cfg(feature = "in_process")]
fn in_process_snapshot<T: Clone + DeserializeOwned + Serialize>(
    facet: &clay_docs::Facet<T>,
    snapshot: &clay_docs::DocumentSnapshot,
) -> FacetSnapshot<T> {
    use clay_docs::FacetValue;
    FacetSnapshot {
        state: match facet.state(snapshot) {
            FacetValue::Loading => FacetState::Loading,
            FacetValue::Absent => FacetState::Absent,
            FacetValue::Present(value) => FacetState::Present(value),
        },
        refreshing: snapshot.freshness == clay_docs::Freshness::Refreshing,
        revision: snapshot.revision,
        pending: snapshot.pending.len(),
    }
}

/// A submitted write: its acknowledgement as the host reports it.
#[derive(Clone)]
pub struct Operation(OperationInner);

#[derive(Clone)]
enum OperationInner {
    #[cfg(feature = "in_process")]
    InProcess(clay_docs::Operation),
    #[cfg(feature = "guest")]
    Guest(crate::mirror::MirrorOperation),
}

impl Operation {
    pub fn current(&self) -> OperationReceipt {
        match &self.0 {
            #[cfg(feature = "in_process")]
            OperationInner::InProcess(operation) => operation.current().into(),
            #[cfg(feature = "guest")]
            OperationInner::Guest(operation) => operation.current().into(),
        }
    }

    /// The receipt that follows `after_revision`: `Ok(None)` once the operation settled with
    /// nothing newer, [`Error::Lagged`] when `after_revision` is ahead of what was delivered.
    pub async fn next(&self, after_revision: u64) -> Result<Option<OperationReceipt>, Error> {
        match &self.0 {
            #[cfg(feature = "in_process")]
            OperationInner::InProcess(operation) => Ok(operation
                .next(after_revision)
                .await?
                .map(OperationReceipt::from)),
            #[cfg(feature = "guest")]
            OperationInner::Guest(operation) => Ok(operation
                .next(after_revision)
                .await?
                .map(OperationReceipt::from)),
        }
    }
}

type LocalFuture<T> = Pin<Box<dyn Future<Output = T>>>;

/// A call handler over raw request and response bytes: the typed handler of
/// [`Context::publish_call`] wrapped once for the backend. An `Err` reaches the caller as the
/// provider's rejection.
pub(crate) type RawHandler = Rc<dyn Fn(Vec<u8>) -> LocalFuture<Result<Vec<u8>, Error>>>;

/// The value surface one activation may use: its granted configuration, resolved requirements
/// and collection bindings, publication, and owned tasks. It never hands out the host or a
/// renderer (ADR 015 §7). Cheap to clone; clones share the one activation.
#[derive(Clone)]
pub struct Context(pub(crate) Rc<Shared>);

/// What every backend's activation grants the same way, and the backend's own state.
pub(crate) struct Shared {
    pub(crate) configs: BTreeMap<String, Config>,
    /// Every requirement the manifest declares; an optional one nothing resolved holds `None`.
    pub(crate) requirements: BTreeMap<String, Option<Capability>>,
    pub(crate) backend: Backend,
}

pub(crate) enum Backend {
    #[cfg(feature = "in_process")]
    InProcess(Rc<crate::native::Activation>),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::guest::Runtime>),
}

impl Backend {
    /// A mirror of the collection bound to `binding` with the feed that keeps it current.
    fn collection<K: DeserializeOwned, V: DeserializeOwned>(
        &self,
        binding: &str,
    ) -> Option<(Collection<K, V>, LocalFuture<()>)> {
        match self {
            #[cfg(feature = "in_process")]
            Self::InProcess(activation) => {
                let reader = activation.collection_reader(binding)?;
                let (collection, feed) =
                    Collection::new(Minter::InProcess(activation.capabilities.clone()));
                Some((collection, Box::pin(crate::native::feed(reader, feed))))
            }
            #[cfg(feature = "guest")]
            Self::Guest(runtime) => {
                let reader = runtime.collection_reader(binding)?;
                let (collection, feed) = Collection::new(Minter::Guest);
                Some((collection, Box::pin(crate::guest::feed(reader, feed))))
            }
        }
    }

    fn writer(&self, binding: &str) -> Option<Writer> {
        match self {
            #[cfg(feature = "in_process")]
            Self::InProcess(activation) => activation
                .registration_writer(binding)
                .map(Writer::InProcess),
            #[cfg(feature = "guest")]
            Self::Guest(runtime) => runtime.registration_writer(binding).map(Writer::Guest),
        }
    }

    fn publish_component(
        &self,
        name: &str,
        render: Rc<dyn Fn() -> dioxus_core::Element>,
    ) -> Result<Publication, Error> {
        match self {
            #[cfg(feature = "in_process")]
            Self::InProcess(activation) => activation.publish_component(name, render),
            #[cfg(feature = "guest")]
            Self::Guest(runtime) => runtime.publish_component(name, render),
        }
    }

    fn publish_call(&self, name: &str, handler: RawHandler) -> Result<Publication, Error> {
        match self {
            #[cfg(feature = "in_process")]
            Self::InProcess(activation) => {
                crate::native::Activation::publish_call(activation, name, handler)
            }
            #[cfg(feature = "guest")]
            Self::Guest(runtime) => runtime.publish_call(name, handler),
        }
    }

    fn spawn(&self, future: LocalFuture<()>) {
        match self {
            #[cfg(feature = "in_process")]
            Self::InProcess(activation) => activation.spawn(future),
            #[cfg(feature = "guest")]
            Self::Guest(runtime) => runtime.spawn(future),
        }
    }
}

impl Context {
    /// This activation's incarnation identity. Plain data; operations validate it host-side.
    /// In-process only: a guest never names its own instance.
    #[cfg(feature = "in_process")]
    pub fn instance(&self) -> Instance {
        match &self.0.backend {
            Backend::InProcess(activation) => activation.instance.clone(),
            #[cfg(feature = "guest")]
            Backend::Guest(_) => unreachable!("a guest context has no instance"),
        }
    }

    /// The configuration document granted under `name`, or `None` when the manifest declared
    /// no such binding or the composition activated the micro standalone.
    pub fn config(&self, name: &str) -> Option<&Config> {
        self.0.configs.get(name)
    }

    /// The reference that satisfies requirement `binding`, or `None` when configuration bound
    /// nothing to an optional one. Holding it keeps the provider incarnation live.
    ///
    /// Panics when the manifest declares no such requirement.
    pub fn require(&self, binding: &str) -> Option<&Capability> {
        self.0
            .requirements
            .get(binding)
            .unwrap_or_else(|| panic!("the manifest declares no requirement {binding:?}"))
            .as_ref()
    }

    /// Mirror the collection bound to `binding`, or `None` when configuration left an optional
    /// request unbound. The feed task is incarnation-owned.
    ///
    /// Panics when the manifest declares no collection request `binding`, or the key or value
    /// type does not match the collection's contract (rows crash on decode).
    pub fn collection<K: DeserializeOwned + 'static, V: DeserializeOwned + 'static>(
        &self,
        binding: &str,
    ) -> Option<Collection<K, V>> {
        let (collection, feed) = self.0.backend.collection(binding)?;
        self.spawn(feed);
        Some(collection)
    }

    /// Write authority over the collection bound to `binding`, together with its mirror, or
    /// `None` when configuration left it unbound. The writer's rows belong to this incarnation
    /// and are retracted when it closes.
    ///
    /// Panics when the manifest's request for `binding` is read-only.
    pub fn registration<
        K: Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static,
    >(
        &self,
        binding: &str,
    ) -> Option<Registration<K, V>> {
        let writer = self.0.backend.writer(binding)?;
        Some(Registration::new(
            Rc::new(writer),
            self.collection(binding)?,
        ))
    }

    /// Stage the presentation publication `name` whose component renders `render`. The name
    /// must be one the manifest declares as a presentation; admission checks that once
    /// `activate` returns, and nothing is reachable by other micros before then. Any number of
    /// presentations may be published, one per declared name.
    ///
    /// Returns the staged publication, which a row can attach (see [`Registration::put`]).
    pub fn publish_component(
        &self,
        name: &str,
        render: impl Fn() -> dioxus_core::Element + 'static,
    ) -> Result<Publication, Error> {
        self.0.backend.publish_component(name, Rc::new(render))
    }

    /// Stage the call publication `name`, served by `handler` under the contract the manifest
    /// declares for it. Requests and responses are CBOR; a request that does not decode as
    /// `Req`, or a handler `Err`, reaches the caller as the provider's rejection. Calls run
    /// concurrently on this incarnation's executor and are dropped when it closes.
    ///
    /// Panics when the manifest declares no call `name`.
    pub fn publish_call<Req, Resp, Handler, Fut>(
        &self,
        name: &str,
        handler: Handler,
    ) -> Result<Publication, Error>
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        Handler: Fn(Req) -> Fut + 'static,
        Fut: Future<Output = Result<Resp, Error>> + 'static,
    {
        let handler = Rc::new(handler);
        self.0.backend.publish_call(
            name,
            Rc::new(move |request: Vec<u8>| {
                let handler = Rc::clone(&handler);
                Box::pin(async move {
                    let request =
                        ciborium::from_reader::<Req, _>(request.as_slice()).map_err(|error| {
                            Error::InvalidValue(format!("request does not decode: {error}"))
                        })?;
                    Ok(encode(&handler(request).await?))
                })
            }),
        )
    }

    /// Run `future` as incarnation-owned local work: it is dropped when this instance closes,
    /// and a panic in it is raised at the close (in-process) or traps the guest.
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.0.backend.spawn(Box::pin(future));
    }
}
