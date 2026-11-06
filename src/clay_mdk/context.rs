//! The micro authoring surface (ADR 015 §5): one [`Context`] a micro's `activate` receives, one
//! [`Error`], and the typed handles it hands out. Authors write
//! `async fn activate(cx: Context) -> Result<(), Error>` and nothing in it names a backend.
//!
//! The MDK adds ergonomics, never authority: every operation below runs through the host
//! sessions, whether the micro is linked in-process (feature `in_process`, built by
//! [`crate::host::factory`]) or runs as a wasm guest (feature `guest`, exported by
//! [`crate::export_micro!`]). A micro is interchangeable to every other micro either way
//! (ADR 031 §5). Each public method here is written once and delegates to the backend's arm of
//! one private enum, so the two backends cannot drift in signature; a build that enables both
//! features holds either in a [`Context`] and every handle it mints stays with its backend.
//!
//! Handles are plain values a component captures in its `render` closure: there is no ambient
//! context to look up. Components subscribe through the hooks in [`crate::hooks`].
use crate::Collection;
use crate::CollectionDecl;
use crate::ComponentDecl;
use crate::ConfigDecl;
use crate::FacetState;
use crate::Registration;
use crate::manifest::Access;
use crate::manifest::Request;
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

/// What a reference reports without activating anything (ADR 030 §9 `ref-status`).
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

    /// Whether this reference belongs to a wasm guest rather than an in-process micro.
    #[cfg(feature = "guest")]
    pub(crate) fn is_guest(&self) -> bool {
        match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(_) => false,
            CapabilityInner::Guest(_) => true,
        }
    }

    /// What tells two guest references apart for a component key: the held resource's address.
    #[cfg(feature = "guest")]
    pub(crate) fn identity(&self) -> usize {
        match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(_) => unreachable!("only guest references have an address"),
            CapabilityInner::Guest(reference) => Rc::as_ptr(reference) as usize,
        }
    }

    pub fn status(&self) -> RefStatus {
        match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => reference.status().into(),
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => reference.status().into(),
        }
    }

    /// Completes once [`Self::status`] differs from `seen` (at once if it already does), so a
    /// holder that saw a failure waits for its provider to change (a disabled micro enabled, an
    /// unconfigured one configured) instead of retrying. Woken by the supervisor's status
    /// ticks, never by polling. A reference to a live publication has no restart to follow and
    /// never completes.
    pub async fn status_changed(&self, seen: RefStatus) {
        match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => reference.status_changed(seen.into()).await,
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => reference.status_changed(seen.into()).await,
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
        decode(&response)
    }

    /// Retain the provider's call publication: the provider stays live (a lazy one is started
    /// if none is) for as long as the returned [`CallBinding`] is held, so any number of calls
    /// through it share one incarnation, and dropping it lets the provider go idle. This is how
    /// a micro keeps a provider up while it is likely to be called again (a focused editor and
    /// its completion providers), where [`Self::call`] holds it for one call only.
    pub async fn retain_call(&self) -> Result<CallBinding, Error> {
        Ok(CallBinding(match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => {
                CallBindingInner::InProcess(reference.retain_call().await?)
            }
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => {
                CallBindingInner::Guest(reference.retain_call().await?)
            }
        }))
    }

    /// Redeem the provider's presentation, activating the provider if the reference is lazy.
    /// The returned proxy holds the provider live for as long as it is held, so a slot keeps it
    /// for exactly the life of what it mounts (ADR 031 §9).
    pub async fn redeem_presentation(&self) -> Result<PresentationComponent, Error> {
        Ok(PresentationComponent(match &self.0 {
            #[cfg(feature = "in_process")]
            CapabilityInner::InProcess(reference) => {
                PresentationInner::InProcess(reference.redeem_presentation().await?)
            }
            #[cfg(feature = "guest")]
            CapabilityInner::Guest(reference) => {
                PresentationInner::Guest(Rc::new(reference.redeem_presentation().await?))
            }
        }))
    }
}

/// A retained call (see [`Capability::retain_call`]): the provider stays live while this is
/// held, and dropping it releases the hold. Once the provider's incarnation ends, calls through
/// it report [`Error::Revoked`] rather than starting another.
pub struct CallBinding(CallBindingInner);

enum CallBindingInner {
    #[cfg(feature = "in_process")]
    InProcess(clay_guest_session::capabilities::CallBinding),
    #[cfg(feature = "guest")]
    Guest(crate::capability_bank::CallBinding),
}

impl CallBinding {
    /// One call of the retained publication, CBOR-encoded both ways.
    pub async fn call<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: &Req,
    ) -> Result<Resp, Error> {
        let request = encode(request);
        let response = match &self.0 {
            #[cfg(feature = "in_process")]
            CallBindingInner::InProcess(binding) => binding.call(request).await?,
            #[cfg(feature = "guest")]
            CallBindingInner::Guest(binding) => binding.call(request).await?,
        };
        decode(&response)
    }
}

/// A redeemed presentation: the provider stays live while this is held. Mounted by the
/// [`crate::hooks::PresentationSlot`] component.
pub struct PresentationComponent(PresentationInner);

enum PresentationInner {
    #[cfg(feature = "in_process")]
    InProcess(clay_guest_session::capabilities::PresentationComponent),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::capability_bank::PresentationComponent>),
}

/// What a slot mounts for a redeemed presentation: the component reference the provider's
/// publication renders as. In-process the host reference is mounted by the installed renderer
/// on behalf of the micro that redeemed it; in a guest it is the `component-ref` resource, which
/// the guest runtime sends to the host with `mount-component`.
#[derive(Clone, PartialEq)]
pub struct Mount(pub(crate) MountInner);

#[derive(Clone)]
pub(crate) enum MountInner {
    #[cfg(feature = "in_process")]
    InProcess(Instance, clay_host::ComponentRef),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::presentation::ComponentRef>),
}

impl PartialEq for MountInner {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            #[cfg(feature = "in_process")]
            (Self::InProcess(left, left_ref), Self::InProcess(right, right_ref)) => {
                left == right && left_ref == right_ref
            }
            #[cfg(feature = "guest")]
            (Self::Guest(left), Self::Guest(right)) => Rc::ptr_eq(left, right),
            #[cfg(all(feature = "in_process", feature = "guest"))]
            _ => false,
        }
    }
}

impl PresentationComponent {
    /// The component to mount, or why there is none (the provider's incarnation ended, or the
    /// incarnation holds as many component references as it may).
    pub fn component(&self) -> Result<Mount, Error> {
        Ok(Mount(match &self.0 {
            #[cfg(feature = "in_process")]
            PresentationInner::InProcess(proxy) => {
                MountInner::InProcess(proxy.owner().clone(), proxy.component()?)
            }
            #[cfg(feature = "guest")]
            PresentationInner::Guest(proxy) => MountInner::Guest(Rc::new(proxy.component()?)),
        }))
    }

    /// Completes once the provider incarnation this was redeemed from has ended, after which
    /// [`Self::component`] fails and a slot redeems again. Woken by the supervisor's status
    /// ticks; a proxy of a live publication has no restart to follow and never completes.
    pub async fn revoked(&self) {
        match &self.0 {
            #[cfg(feature = "in_process")]
            PresentationInner::InProcess(proxy) => proxy.revocation().wait().await,
            #[cfg(feature = "guest")]
            PresentationInner::Guest(proxy) => proxy.revoked().await,
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

fn decode<T: DeserializeOwned>(response: &[u8]) -> Result<T, Error> {
    ciborium::from_reader(response).map_err(|error| Error::InvalidValue(error.to_string()))
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("CBOR encoding of an in-memory value");
    bytes
}

/// What one row of a registration attaches to its value (indexed by [`crate::Attachment`]):
/// one of this micro's own publications, named as the manifest declares it.
///
/// A durable row persists endpoints, never references (ADR 031 §7), so the host turns the name
/// into the endpoint `(micro, publication, contract)` when the row is written. That needs no
/// live publication, so a row may be written at any time: from `activate`, from a spawned task
/// or from a call handler, and a reader that redeems it activates the micro as for any lazy
/// endpoint. A name the manifest does not declare fails the write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowAttachment(String);

impl RowAttachment {
    /// The presentation `P` this micro publishes.
    pub fn own<P: ComponentDecl>() -> Self {
        Self(P::NAME.into())
    }

    /// One of this micro's publications named by data instead of by declaration: a call
    /// publication (calls stay named by string until they are declared by their `Method`
    /// request type), or a name the caller does not control.
    pub fn by_name(publication: impl Into<String>) -> Self {
        Self(publication.into())
    }

    #[cfg(feature = "guest")]
    pub(crate) fn into_name(self) -> String {
        self.0
    }

    /// The endpoint the host stores for this attachment.
    #[cfg(feature = "in_process")]
    pub(crate) fn into_in_process(
        self,
        capabilities: &clay_guest_session::capabilities::CapabilitySession,
    ) -> Result<clay_guest_session::capabilities::RowAttachment, Error> {
        capabilities
            .own_endpoint(&self.0)
            .ok_or_else(|| clay_guest_session::collections::WriteFailure::InvalidAttachment.into())
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
    pub(crate) fn facet<T: DeserializeOwned + Serialize>(
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
    /// The manifest's collection requests, which resolve a declared collection type to the
    /// binding name this micro requested it under.
    pub(crate) requests: Vec<Request>,
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
    ) -> Result<(), Error> {
        match self {
            #[cfg(feature = "in_process")]
            Self::InProcess(activation) => activation.publish_component(name, render),
            #[cfg(feature = "guest")]
            Self::Guest(runtime) => runtime.publish_component(name, render),
        }
    }

    fn publish_call(&self, name: &str, handler: RawHandler) -> Result<(), Error> {
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

    /// The facet `C` declares, watched for the incarnation's life, or `None` when the
    /// composition activated the micro standalone and granted no document under `C::BINDING`.
    ///
    /// Panics when the manifest requests no configuration under `C::BINDING`.
    pub fn config<C: ConfigDecl>(&self) -> Result<Option<Facet<C::Value>>, Error> {
        self.0
            .configs
            .get(C::BINDING)
            .map(|config| config.facet(C::NSID, C::KEY))
            .transpose()
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

    /// The binding this micro requested collection `C` under with `access`. A read lookup
    /// falls back to a write request, because writing implies reading.
    ///
    /// Panics when the manifest has no such request, or has several (a micro that needs two
    /// bindings to one contract with the same access cannot name them by type).
    fn binding<C: CollectionDecl>(&self, access: Access) -> &str {
        let matching = |access: &Access| {
            let mut requests = self.0.requests.iter().filter(|request| {
                request.contract.as_str() == C::NSID && request.access.as_ref() == Some(access)
            });
            let first = requests.next();
            assert!(
                requests.next().is_none(),
                "the manifest requests {} {access:?} under several bindings",
                C::NSID
            );
            first
        };
        let request = matching(&access)
            .or_else(|| {
                (access == Access::Read)
                    .then(|| matching(&Access::Write))
                    .flatten()
            })
            .unwrap_or_else(|| {
                panic!(
                    "the manifest declares no {access:?} request for collection {}",
                    C::NSID
                )
            });
        &request.binding
    }

    /// Mirror collection `C`, bound under the binding this micro's manifest requested it
    /// under, or `None` when configuration left an optional request unbound. The feed task is
    /// incarnation-owned.
    ///
    /// Panics when the manifest declares no collection request for `C`.
    pub fn collection<C: CollectionDecl>(&self) -> Option<Collection<C::Key, C::Value>> {
        self.mirror(self.binding::<C>(Access::Read))
    }

    fn mirror<K: DeserializeOwned + 'static, V: DeserializeOwned + 'static>(
        &self,
        binding: &str,
    ) -> Option<Collection<K, V>> {
        let (collection, feed) = self.0.backend.collection(binding)?;
        self.spawn(feed);
        Some(collection)
    }

    /// Write authority over collection `C`, together with its mirror, or `None` when
    /// configuration left it unbound. The writer's rows belong to this incarnation and are
    /// retracted when it closes.
    ///
    /// Panics when the manifest requests no write access to `C`.
    pub fn registration<C: CollectionDecl>(&self) -> Option<Registration<C::Key, C::Value>> {
        let binding = self.binding::<C>(Access::Write);
        let writer = self.0.backend.writer(binding)?;
        Some(Registration::new(Rc::new(writer), self.mirror(binding)?))
    }

    /// Stage the presentation publication `P` whose component renders `render`. `P::NAME` must
    /// be one the manifest declares as a presentation; admission checks that once `activate`
    /// returns, and nothing is reachable by other micros before then. Any number of
    /// presentations may be published, one per declared name.
    pub fn publish<P: ComponentDecl>(
        &self,
        render: impl Fn() -> dioxus_core::Element + 'static,
    ) -> Result<(), Error> {
        self.0.backend.publish_component(P::NAME, Rc::new(render))
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
    ) -> Result<(), Error>
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
