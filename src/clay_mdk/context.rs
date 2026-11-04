//! The micro authoring surface (ADR 015 §5): one [`Context`] a micro's `activate` receives, one
//! [`Error`], and the typed handles it hands out. Authors write
//! `async fn activate(cx: Context) -> Result<(), Error>` and nothing in it names a backend.
//!
//! The MDK adds ergonomics, never authority: every operation below runs through the same host
//! sessions a guest's engine drives, so a native micro and a guest are interchangeable to every
//! other micro (ADR 025 §5). [`Context`] holds the backend's activation state privately; today
//! that is the in-process one built by [`crate::host::factory`].
//!
//! Handles are plain values a component captures in its `render` closure: there is no ambient
//! context to look up. Components subscribe through the hooks in [`crate::hooks`].
use crate::native::Activation;
use clay_docs::DocumentRef;
use clay_docs::Freshness;
use clay_guest_session::capabilities::CallError;
use clay_guest_session::capabilities::CapabilityFailure;
use clay_guest_session::capabilities::CapabilityRef;
use clay_guest_session::capabilities::GuestCall;
use clay_guest_session::capabilities::GuestCallTarget;
use clay_guest_session::collections::WriteFailure;
use dioxus_core::Element;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub use crate::native::Attachment;
pub use crate::native::Collection;
pub use crate::native::Registration;
pub use crate::native::Row;
pub use clay_collection::Stamp;
pub use clay_docs::Operation;
pub use clay_docs::OperationReceipt;
pub use clay_guest_session::capabilities::CapabilityPublication as Publication;
use clay_guest_session::capabilities::PresentationComponent;
pub use clay_guest_session::capabilities::RefStatus;
pub use clay_guest_session::capabilities::RowAttachment;
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

impl From<clay_host::AccessError> for Error {
    fn from(error: clay_host::AccessError) -> Self {
        match error {
            clay_host::AccessError::Denied => Self::Denied,
            clay_host::AccessError::Revoked => Self::Revoked,
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

impl From<WriteFailure> for Error {
    fn from(failure: WriteFailure) -> Self {
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

/// A recipient-local reference to another micro's publication: a row attachment or a resolved
/// requirement. Holding it does not keep the provider running unless it is redeemed (a slot
/// mounting a presentation) or retained; [`Self::status`] never activates anything.
pub struct Capability(CapabilityRef);

impl Capability {
    pub(crate) fn new(reference: CapabilityRef) -> Self {
        Self(reference)
    }

    pub fn status(&self) -> RefStatus {
        self.0.status()
    }

    /// Redeem the provider's presentation, activating the provider if the reference is lazy.
    /// The returned proxy holds the provider live for as long as it is held, so a slot keeps it
    /// for exactly the life of what it mounts (ADR 025 §9).
    pub async fn redeem_presentation(&self) -> Result<PresentationComponent, Error> {
        Ok(self.0.redeem_presentation().await?)
    }

    /// One call of the provider's call publication, CBOR-encoded both ways. The provider is
    /// held strongly for the duration of the call only.
    pub async fn call<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: &Req,
    ) -> Result<Resp, Error> {
        let response = self.0.call(encode(request)).await?;
        ciborium::from_reader(response.as_slice())
            .map_err(|error| Error::InvalidValue(error.to_string()))
    }
}

impl Clone for Capability {
    fn clone(&self) -> Self {
        Self(self.0.share())
    }
}

/// Two references are equal when they store back as the same attachment, so a re-rendered row
/// that still names the same target does not remount its slot.
impl PartialEq for Capability {
    fn eq(&self, other: &Self) -> bool {
        self.0.attachment() == other.0.attachment()
    }
}

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("CBOR encoding of an in-memory value");
    bytes
}

/// A configuration document granted from the manifest's declared ACL.
#[derive(Clone)]
pub struct Config(DocumentRef);

impl Config {
    pub(crate) fn new(document: DocumentRef) -> Self {
        Self(document)
    }

    /// The typed facet `tag`/`key_id` of this document, watched for the incarnation's life.
    pub fn facet<T: DeserializeOwned + Serialize>(
        &self,
        tag: &str,
        key_id: &str,
    ) -> Result<Facet<T>, Error> {
        Ok(Facet(self.0.facet(tag.into(), key_id.into())?))
    }
}

/// One configuration facet's decoded state at a revision of its document watch.
#[derive(Clone, Debug, PartialEq)]
pub struct FacetSnapshot<T> {
    /// `Loading` (not hydrated yet) is distinct from `Absent` (known absent after hydration).
    pub state: crate::FacetState<T>,
    /// A targeted refresh is in flight; the state still shows the retained local value.
    pub refreshing: bool,
    pub revision: u64,
    /// Local writes not yet acknowledged.
    pub pending: usize,
}

/// A typed configuration facet: reads see local writes optimistically, writes return the
/// [`Operation`] that tracks their acknowledgement.
#[derive(Clone)]
pub struct Facet<T>(clay_docs::Facet<T>);

/// Two facets are equal when they watch through the same lease, so a re-rendered component
/// handed the same facet is not re-rendered for it.
impl<T> PartialEq for Facet<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Clone + DeserializeOwned + Serialize> Facet<T> {
    pub fn current(&self) -> Result<FacetSnapshot<T>, Error> {
        Ok(self.snapshot(&self.0.watch.current()?))
    }

    pub fn write(&self, value: T) -> Result<Operation, Error> {
        Ok(self.0.write(value)?)
    }

    /// Request a targeted refresh. Returns the post-request view; a pending refresh shows as
    /// [`FacetSnapshot::refreshing`], never as a missing value.
    pub fn refresh(&self) -> Result<FacetSnapshot<T>, Error> {
        Ok(self.snapshot(&self.0.watch.refresh()?))
    }

    /// A receiver that ticks on every change of the facet's document watch.
    pub(crate) fn changes(
        &self,
    ) -> Result<tokio::sync::watch::Receiver<clay_docs::DocumentSnapshot>, Error> {
        Ok(self.0.watch.subscribe()?)
    }

    fn snapshot(&self, snapshot: &clay_docs::DocumentSnapshot) -> FacetSnapshot<T> {
        use clay_docs::FacetValue;
        FacetSnapshot {
            state: match self.0.state(snapshot) {
                FacetValue::Loading => crate::FacetState::Loading,
                FacetValue::Absent => crate::FacetState::Absent,
                FacetValue::Present(value) => crate::FacetState::Present(value),
            },
            refreshing: snapshot.freshness == Freshness::Refreshing,
            revision: snapshot.revision,
            pending: snapshot.pending.len(),
        }
    }
}

/// The value surface one activation may use: its granted configuration, resolved requirements
/// and collection bindings, publication, and owned tasks. It never hands out the host or a
/// renderer (ADR 015 §7). Cheap to clone; clones share the one activation.
#[derive(Clone)]
pub struct Context(pub(crate) Rc<Activation>);

impl Context {
    /// This activation's incarnation identity. Plain data; operations validate it host-side.
    pub fn instance(&self) -> Instance {
        self.0.instance.clone()
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
        let slot = self.0.collection_slot(binding)?;
        let (collection, feed) =
            Collection::start(slot.reader.clone(), self.0.capabilities.clone());
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
        let slot = self.0.collection_slot(binding)?;
        let writer = slot
            .writer
            .clone()
            .unwrap_or_else(|| panic!("collection request {binding:?} is read-only"));
        Some(Registration::new(writer, self.collection(binding)?))
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
        render: impl Fn() -> Element + 'static,
    ) -> Result<Publication, Error> {
        let component = (self.0.publisher)(&self.0.instance, Rc::new(render))?;
        self.0
            .stage(self.0.capabilities.publish_component(name, component))
    }

    /// Stage the call publication `name`, served by `handler` under the contract the manifest
    /// declares for it. Requests and responses are CBOR; a request that does not decode as
    /// `Req`, or a handler `Err`, reaches the caller as the provider's rejection. Calls run
    /// concurrently on this incarnation's local executor and are dropped when it closes.
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
        let contract = self.0.declared_call(name);
        // The publication's target must be `Send + Sync` and the handler is neither, so the
        // target only queues the request; the loop below runs handlers on the local executor,
        // exactly as an engine's driver serves a guest's `micro-lifecycle.call`.
        let (queue, mut inbox) = mpsc::unbounded_channel::<GuestCall>();
        let target = Arc::new(GuestCallTarget::new(name.to_owned(), queue));
        let handler = Rc::new(handler);
        self.spawn(async move {
            let mut running = JoinSet::new();
            loop {
                tokio::select! {
                    call = inbox.recv() => {
                        let Some(GuestCall { request, reply, .. }) = call else {
                            return;
                        };
                        let handler = Rc::clone(&handler);
                        running.spawn_local(async move {
                            let outcome = match ciborium::from_reader::<Req, _>(request.as_slice()) {
                                Ok(request) => handler(request)
                                    .await
                                    .map(|response| encode(&response))
                                    .map_err(|error| CallError::Provider(error.to_string())),
                                Err(error) => Err(CallError::Provider(format!(
                                    "request does not decode: {error}"
                                ))),
                            };
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
        });
        self.0
            .stage(self.0.capabilities.publish_call(name, contract, target))
    }

    /// Run `future` as incarnation-owned local work: it is dropped when this instance closes,
    /// and a panic in it is raised at the close.
    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.0
            .tasks
            .borrow_mut()
            .push(tokio::task::spawn_local(future));
    }
}
