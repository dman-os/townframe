//! Host capability bank (ADR 023, ADR 024 §9, ADR 025 §§5-6): named publications of two kinds
//! (Call and Presentation), recipient-local references, and typed redemption. Authority stays
//! host-side; a [`CapabilityPublication`] stored in a collection value is only an `Arc` to the
//! host publication record, never a serializable ID, and a [`CapabilityRef`] is minted per
//! recipient incarnation.
//!
//! A reference is bound or lazy. A bound reference targets a live publication and never
//! activates anything: it is `Revoked` once its incarnation closes. A lazy reference names a
//! micro's publication; using it asks the supervisor (through an [`Activator`]) to start or
//! join that micro and holds the resulting [`Lease`] for as long as the use is strong: for the
//! duration of one `call`, and for the lifetime of a retained [`CallBinding`] or
//! [`PresentationComponent`]. Requests and responses are opaque bytes (CBOR by convention).

use clay_host::AccessError;
use clay_host::Activator;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::Instance;
use clay_host::Lease;
use clay_host::Lifecycle;
use clay_host::MicroId;
use clay_host::MicroState;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractIdentity {
    pub namespace: &'static str,
    pub name: &'static str,
    pub version: (u32, u32, u32),
}

pub const PRESENTATION_COMPONENT_CONTRACT: ContractIdentity = ContractIdentity {
    namespace: "townframe:dayframe",
    name: "presentation-component",
    version: (0, 3, 0),
};

pub const CALL_CONTRACT: ContractIdentity = ContractIdentity {
    namespace: "townframe:dayframe",
    name: "call",
    version: (0, 3, 0),
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityFailure {
    ContractMismatch,
    Revoked,
    LimitExceeded,
    /// A lazy reference's provider could not be activated, or does not publish the name.
    Unavailable,
}

/// Why a call failed: the reference was unusable, or the provider's handler rejected it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallError {
    Capability(CapabilityFailure),
    Provider(String),
}

impl From<CapabilityFailure> for CallError {
    fn from(failure: CapabilityFailure) -> Self {
        Self::Capability(failure)
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationKind {
    Call,
    Presentation,
}

/// Runs a Call publication's handler inside the provider incarnation. The engine supplies
/// one that reaches the guest's exported `call`; it must be `Send` because it lives in the
/// publication record that recipients' engine state holds.
pub trait CallTarget: Send + Sync {
    fn call(&self, request: Vec<u8>) -> BoxFuture<'static, Result<Vec<u8>, String>>;
}

/// A weak, name-only reference to a micro's publication (ADR 025 §6), as read from a durable
/// row. `contract` is what the row promises; it is checked before anything is activated.
pub struct LazyEndpoint {
    pub micro: MicroId,
    pub publication: String,
    pub contract: ContractIdentity,
    pub activator: Activator<CapabilityPublication>,
}

#[derive(Clone, Copy, Debug)]
pub struct CapabilityLimits {
    pub publications: usize,
    pub references: usize,
    pub proxies: usize,
}

impl Default for CapabilityLimits {
    fn default() -> Self {
        Self {
            publications: 64,
            references: 1024,
            proxies: 1024,
        }
    }
}

enum Behavior {
    Call(Arc<dyn CallTarget>),
    Presentation(ComponentRef),
}

struct PublicationInner {
    id: u64,
    name: String,
    contract: ContractIdentity,
    behavior: Behavior,
    host: Host,
    /// The publishing incarnation. Until it commits initialization the publication is staged
    /// (ADR 024 §5): only the owner itself may redeem it.
    owner: Instance,
    revoked: AtomicBool,
}

impl PublicationInner {
    fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }
    fn live(&self, recipient: &Instance) -> Result<&Behavior, CapabilityFailure> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(CapabilityFailure::Revoked);
        }
        if *recipient != self.owner
            && !matches!(
                self.host.lifecycle(&self.owner),
                Lifecycle::Active | Lifecycle::Closing
            )
        {
            return Err(CapabilityFailure::Revoked);
        }
        if let Behavior::Presentation(component) = &self.behavior {
            self.host
                .check_component(*component)
                .map_err(|_| CapabilityFailure::Revoked)?;
        }
        Ok(&self.behavior)
    }
}

/// Cloneable handle to a host publication, stored in collection values.
/// Cloning conveys no new authority: redemption still requires a
/// recipient-local [`CapabilityRef`] minted for a live incarnation.
#[derive(Clone)]
pub struct CapabilityPublication {
    inner: Arc<PublicationInner>,
}

impl std::fmt::Debug for CapabilityPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilityPublication")
            .field("id", &self.inner.id)
            .field("name", &self.inner.name)
            .field("contract", &self.inner.contract)
            .finish_non_exhaustive()
    }
}

impl PartialEq for CapabilityPublication {
    fn eq(&self, other: &Self) -> bool {
        self.inner.id == other.inner.id
    }
}
impl Eq for CapabilityPublication {}

impl CapabilityPublication {
    pub fn id(&self) -> u64 {
        self.inner.id
    }
    pub fn contract(&self) -> &ContractIdentity {
        &self.inner.contract
    }
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    pub fn kind(&self) -> PublicationKind {
        match self.inner.behavior {
            Behavior::Call(_) => PublicationKind::Call,
            Behavior::Presentation(_) => PublicationKind::Presentation,
        }
    }
    pub fn is_revoked(&self) -> bool {
        self.inner.revoked.load(Ordering::Acquire)
            || matches!(&self.inner.behavior, Behavior::Presentation(component)
                if self.inner.host.check_component(*component).is_err())
    }
}

/// Owned publication lease. Dropping (or provider-incarnation closure)
/// revokes the publication and every proxy derived from it; the lease
/// captures its own publication record, so revoking a stale lease can
/// never revoke a replacement provider's publication.
pub struct PublishedCapability {
    publication: CapabilityPublication,
    session: Arc<SessionState>,
}

impl PublishedCapability {
    pub fn publication(&self) -> CapabilityPublication {
        self.publication.clone()
    }
    pub fn reference(&self) -> Result<CapabilityRef, CapabilityFailure> {
        self.session.mint(Target::Bound(self.publication.clone()))
    }
    pub fn revoke(&self) {
        self.publication.inner.revoke();
    }
}

impl Drop for PublishedCapability {
    fn drop(&mut self) {
        self.publication.inner.revoke();
        let mut counts = self.session.counts.lock().unwrap();
        counts.publications = counts.publications.checked_sub(1).unwrap();
    }
}

struct SessionState {
    host: Host,
    instance: Instance,
    limits: CapabilityLimits,
    counts: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
    publications: usize,
    references: usize,
    proxies: usize,
}

impl SessionState {
    fn check(&self) -> Result<(), CapabilityFailure> {
        self.host
            .check(&self.instance)
            .map_err(|_| CapabilityFailure::Revoked)
    }
    fn mint(self: &Arc<Self>, target: Target) -> Result<CapabilityRef, CapabilityFailure> {
        self.check()?;
        let mut counts = self.counts.lock().unwrap();
        if counts.references >= self.limits.references {
            return Err(CapabilityFailure::LimitExceeded);
        }
        counts.references += 1;
        drop(counts);
        Ok(CapabilityRef {
            target,
            recipient: Arc::clone(self),
        })
    }

    /// Count one retained proxy against the recipient's limit.
    fn take_proxy(self: &Arc<Self>) -> Result<(), CapabilityFailure> {
        let mut counts = self.counts.lock().unwrap();
        if counts.proxies >= self.limits.proxies {
            return Err(CapabilityFailure::LimitExceeded);
        }
        counts.proxies += 1;
        Ok(())
    }
}

/// Per-incarnation bank session: publishes as this instance, and mints /
/// redeems references for this instance as recipient.
#[derive(Clone)]
pub struct CapabilitySession {
    state: Arc<SessionState>,
}

static NEXT_PUBLICATION: AtomicU64 = AtomicU64::new(1);

impl CapabilitySession {
    pub fn new(host: Host, instance: Instance, limits: CapabilityLimits) -> Self {
        Self {
            state: Arc::new(SessionState {
                host,
                instance,
                limits,
                counts: Mutex::new(Counts::default()),
            }),
        }
    }

    pub fn publication_count(&self) -> usize {
        self.state.counts.lock().unwrap().publications
    }
    pub fn reference_count(&self) -> usize {
        self.state.counts.lock().unwrap().references
    }
    pub fn proxy_count(&self) -> usize {
        self.state.counts.lock().unwrap().proxies
    }

    /// Publish a fresh presentation component, named `name`, owned by this incarnation.
    pub fn publish_presentation(
        &self,
        name: &str,
    ) -> Result<PublishedCapability, CapabilityFailure> {
        let component = self
            .state
            .host
            .publish_component(&self.state.instance)
            .map_err(|_| CapabilityFailure::Revoked)?;
        self.publish_component(name, component)
    }

    /// Deposit an existing component reference, named `name`, under the presentation
    /// contract. The component must belong to this session's host.
    pub fn publish_component(
        &self,
        name: &str,
        component: ComponentRef,
    ) -> Result<PublishedCapability, CapabilityFailure> {
        self.publish(
            name,
            PRESENTATION_COMPONENT_CONTRACT,
            Behavior::Presentation(component),
        )
    }

    /// Publish a call named `name` that `target` serves inside this incarnation.
    pub fn publish_call(
        &self,
        name: &str,
        target: Arc<dyn CallTarget>,
    ) -> Result<PublishedCapability, CapabilityFailure> {
        self.publish(name, CALL_CONTRACT, Behavior::Call(target))
    }

    fn publish(
        &self,
        name: &str,
        contract: ContractIdentity,
        behavior: Behavior,
    ) -> Result<PublishedCapability, CapabilityFailure> {
        self.state.check()?;
        if let Behavior::Presentation(component) = &behavior {
            self.state
                .host
                .check_component(*component)
                .map_err(|error| match error {
                    AccessError::Denied => CapabilityFailure::ContractMismatch,
                    AccessError::Revoked => CapabilityFailure::Revoked,
                })?;
        }
        {
            let mut counts = self.state.counts.lock().unwrap();
            if counts.publications >= self.state.limits.publications {
                return Err(CapabilityFailure::LimitExceeded);
            }
            counts.publications += 1;
        }
        let publication = CapabilityPublication {
            inner: Arc::new(PublicationInner {
                id: NEXT_PUBLICATION.fetch_add(1, Ordering::Relaxed),
                name: name.to_owned(),
                contract,
                behavior,
                host: self.state.host.clone(),
                owner: self.state.instance.clone(),
                revoked: AtomicBool::new(false),
            }),
        };
        // Provider-incarnation closure revokes exactly this publication.
        let inner = Arc::clone(&publication.inner);
        let registered = self
            .state
            .host
            .own(&self.state.instance, Box::new(move || inner.revoke()));
        if let Err(error) = registered {
            let mut counts = self.state.counts.lock().unwrap();
            counts.publications -= 1;
            return Err(match error {
                AccessError::Revoked => CapabilityFailure::Revoked,
                AccessError::Denied => CapabilityFailure::ContractMismatch,
            });
        }
        Ok(PublishedCapability {
            publication,
            session: Arc::clone(&self.state),
        })
    }

    /// Mint a recipient-local reference to the live `publication` for this session's
    /// incarnation. Using it never activates anything.
    pub fn mint(
        &self,
        publication: &CapabilityPublication,
    ) -> Result<CapabilityRef, CapabilityFailure> {
        self.state.mint(Target::Bound(publication.clone()))
    }

    /// Mint a recipient-local reference that activates `endpoint`'s provider on use.
    pub fn mint_lazy(&self, endpoint: LazyEndpoint) -> Result<CapabilityRef, CapabilityFailure> {
        self.state.mint(Target::Lazy(endpoint))
    }

    pub fn same_recipient(&self, reference: &CapabilityRef) -> bool {
        Arc::ptr_eq(&self.state, &reference.recipient)
    }
}

enum Target {
    Bound(CapabilityPublication),
    Lazy(LazyEndpoint),
}

/// Recipient-local, unforgeable reference. Guests hold this only as a
/// WIT resource; copying collection CBOR cannot fabricate one.
pub struct CapabilityRef {
    target: Target,
    recipient: Arc<SessionState>,
}

impl CapabilityRef {
    pub fn recipient_scope(&self) -> u64 {
        self.recipient.instance.scope_id().0
    }

    /// The live publication this reference is bound to. A lazy reference has none: only a
    /// bound reference can be attached to a collection value.
    pub fn publication(&self) -> Option<CapabilityPublication> {
        match &self.target {
            Target::Bound(publication) => Some(publication.clone()),
            Target::Lazy(_) => None,
        }
    }

    /// What the reference can report without activating anything.
    pub fn status(&self) -> RefStatus {
        if self.recipient.check().is_err() {
            return RefStatus::Revoked;
        }
        match &self.target {
            Target::Bound(publication) if publication.is_revoked() => RefStatus::Revoked,
            Target::Bound(_) => RefStatus::Live,
            Target::Lazy(endpoint) => match endpoint.activator.status(&endpoint.micro) {
                None | Some(MicroState::Blocked(_)) => RefStatus::Unavailable,
                Some(MicroState::Active) => RefStatus::Live,
                Some(MicroState::Idle | MicroState::Initializing | MicroState::Closing) => {
                    RefStatus::Dormant
                }
                Some(MicroState::Disabled) => RefStatus::Disabled,
                Some(MicroState::Faulted(_)) => RefStatus::Faulted,
            },
        }
    }

    /// One call. A lazy reference holds its lease only until the response arrives.
    pub async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, CallError> {
        let (publication, _lease) = self.resolve(&CALL_CONTRACT).await?;
        publication.invoke(&self.recipient.instance, request).await
    }

    /// A retained call binding: strong while held, inert once its publication closes.
    pub async fn retain_call(&self) -> Result<CallBinding, CapabilityFailure> {
        let (publication, lease) = self.resolve(&CALL_CONTRACT).await?;
        publication.inner.live(&self.recipient.instance)?;
        self.recipient.take_proxy()?;
        Ok(CallBinding {
            publication,
            recipient: Arc::clone(&self.recipient),
            _lease: lease,
        })
    }

    /// A retained presentation proxy: strong while held, inert once its publication closes.
    pub async fn redeem_presentation(&self) -> Result<PresentationComponent, CapabilityFailure> {
        let (publication, lease) = self.resolve(&PRESENTATION_COMPONENT_CONTRACT).await?;
        publication.inner.live(&self.recipient.instance)?;
        self.recipient.take_proxy()?;
        Ok(PresentationComponent {
            publication,
            recipient: Arc::clone(&self.recipient),
            _lease: lease,
        })
    }

    /// The publication this use targets, with the lease that keeps a lazy one's provider
    /// live. The contract is checked before any activation, then again against what the
    /// provider actually published.
    async fn resolve(
        &self,
        contract: &ContractIdentity,
    ) -> Result<(CapabilityPublication, Option<Lease<CapabilityPublication>>), CapabilityFailure>
    {
        self.recipient.check()?;
        match &self.target {
            Target::Bound(publication) if publication.contract() != contract => {
                Err(CapabilityFailure::ContractMismatch)
            }
            Target::Bound(publication) => Ok((publication.clone(), None)),
            Target::Lazy(endpoint) if endpoint.contract != *contract => {
                Err(CapabilityFailure::ContractMismatch)
            }
            Target::Lazy(endpoint) => {
                let lease = endpoint
                    .activator
                    .acquire(&endpoint.micro, &endpoint.publication)
                    .await
                    .map_err(|_| CapabilityFailure::Unavailable)?;
                // The recipient may have closed while its provider was starting.
                self.recipient.check()?;
                if lease.contract() != contract {
                    return Err(CapabilityFailure::ContractMismatch);
                }
                Ok(((*lease).clone(), Some(lease)))
            }
        }
    }
}

impl CapabilityPublication {
    async fn invoke(&self, recipient: &Instance, request: Vec<u8>) -> Result<Vec<u8>, CallError> {
        let Behavior::Call(target) = self.inner.live(recipient)? else {
            return Err(CapabilityFailure::ContractMismatch.into());
        };
        target.call(request).await.map_err(CallError::Provider)
    }
}

impl Drop for CapabilityRef {
    fn drop(&mut self) {
        let mut counts = self.recipient.counts.lock().unwrap();
        counts.references = counts.references.checked_sub(1).unwrap();
    }
}

/// A retained call proxy. The consumer owns it and drops it normally; provider closure
/// makes it inert (`call` reports `Revoked`) without removing it from its owner.
pub struct CallBinding {
    publication: CapabilityPublication,
    recipient: Arc<SessionState>,
    _lease: Option<Lease<CapabilityPublication>>,
}

impl CallBinding {
    pub async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, CallError> {
        self.recipient.check()?;
        self.publication
            .invoke(&self.recipient.instance, request)
            .await
    }
    pub fn is_revoked(&self) -> bool {
        self.publication.is_revoked()
    }
}

impl Drop for CallBinding {
    fn drop(&mut self) {
        let mut counts = self.recipient.counts.lock().unwrap();
        counts.proxies = counts.proxies.checked_sub(1).unwrap();
    }
}

/// Typed consumer proxy. The consumer owns this handle and drops it
/// normally; provider closure makes it inert (`component()` reports
/// `Revoked`) without removing it from its owner.
pub struct PresentationComponent {
    publication: CapabilityPublication,
    recipient: Arc<SessionState>,
    _lease: Option<Lease<CapabilityPublication>>,
}

impl PresentationComponent {
    pub fn component(&self) -> Result<ComponentRef, CapabilityFailure> {
        self.recipient.check()?;
        match self.publication.inner.live(&self.recipient.instance)? {
            Behavior::Presentation(component) => Ok(*component),
            Behavior::Call(_) => unreachable!("redeemed under the presentation contract"),
        }
    }
    pub fn is_revoked(&self) -> bool {
        self.publication.is_revoked()
    }
}

impl Drop for PresentationComponent {
    fn drop(&mut self) {
        let mut counts = self.recipient.counts.lock().unwrap();
        counts.proxies = counts.proxies.checked_sub(1).unwrap();
    }
}

#[cfg(test)]
mod lazy_tests;
#[cfg(test)]
mod tests;
