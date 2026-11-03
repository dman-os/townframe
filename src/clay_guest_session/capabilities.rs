//! Host capability bank (ADR 023): typed publication, recipient-local
//! references, and typed redemption. Authority stays host-side; a
//! [`CapabilityPublication`] stored in a collection value is only an
//! `Arc` to the host publication record, never a serializable ID, and a
//! [`CapabilityRef`] is minted per recipient incarnation.

use clay_host::AccessError;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::Instance;
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityFailure {
    ContractMismatch,
    Revoked,
    LimitExceeded,
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

struct PublicationInner {
    id: u64,
    contract: ContractIdentity,
    component: ComponentRef,
    host: Host,
    revoked: AtomicBool,
}

impl PublicationInner {
    fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }
    fn live_component(&self) -> Result<ComponentRef, CapabilityFailure> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(CapabilityFailure::Revoked);
        }
        self.host
            .check_component(self.component)
            .map_err(|_| CapabilityFailure::Revoked)?;
        Ok(self.component)
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
    pub fn is_revoked(&self) -> bool {
        self.inner.revoked.load(Ordering::Acquire)
            || self
                .inner
                .host
                .check_component(self.inner.component)
                .is_err()
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
        self.session.mint(&self.publication)
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
    fn mint(
        self: &Arc<Self>,
        publication: &CapabilityPublication,
    ) -> Result<CapabilityRef, CapabilityFailure> {
        self.check()?;
        let mut counts = self.counts.lock().unwrap();
        if counts.references >= self.limits.references {
            return Err(CapabilityFailure::LimitExceeded);
        }
        counts.references += 1;
        drop(counts);
        Ok(CapabilityRef {
            publication: publication.clone(),
            recipient: Arc::clone(self),
        })
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

    /// Publish a fresh presentation component owned by this incarnation.
    pub fn publish_presentation_component(&self) -> Result<PublishedCapability, CapabilityFailure> {
        let component = self
            .state
            .host
            .publish_component(&self.state.instance)
            .map_err(|_| CapabilityFailure::Revoked)?;
        self.publish_component(component)
    }

    /// Deposit an existing component reference under the presentation
    /// contract. The component must belong to this session's host.
    pub fn publish_component(
        &self,
        component: ComponentRef,
    ) -> Result<PublishedCapability, CapabilityFailure> {
        self.state.check()?;
        self.state
            .host
            .check_component(component)
            .map_err(|error| match error {
                AccessError::Denied => CapabilityFailure::ContractMismatch,
                AccessError::Revoked => CapabilityFailure::Revoked,
            })?;
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
                contract: PRESENTATION_COMPONENT_CONTRACT,
                component,
                host: self.state.host.clone(),
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

    /// Mint a recipient-local reference to `publication` for this
    /// session's incarnation.
    pub fn mint(
        &self,
        publication: &CapabilityPublication,
    ) -> Result<CapabilityRef, CapabilityFailure> {
        self.state.mint(publication)
    }

    pub fn same_recipient(&self, reference: &CapabilityRef) -> bool {
        Arc::ptr_eq(&self.state, &reference.recipient)
    }
}

/// Recipient-local, unforgeable reference. Guests hold this only as a
/// WIT resource; copying collection CBOR cannot fabricate one.
pub struct CapabilityRef {
    publication: CapabilityPublication,
    recipient: Arc<SessionState>,
}

impl CapabilityRef {
    pub fn publication_id(&self) -> u64 {
        self.publication.id()
    }
    pub fn recipient_scope(&self) -> u64 {
        self.recipient.instance.scope_id().0
    }
    pub fn contract(&self) -> &ContractIdentity {
        self.publication.contract()
    }
    pub fn publication(&self) -> CapabilityPublication {
        self.publication.clone()
    }

    pub fn redeem(
        &self,
        contract: &ContractIdentity,
    ) -> Result<PresentationComponent, CapabilityFailure> {
        self.recipient.check()?;
        if self.publication.contract() != contract {
            return Err(CapabilityFailure::ContractMismatch);
        }
        self.publication.inner.live_component()?;
        let mut counts = self.recipient.counts.lock().unwrap();
        if counts.proxies >= self.recipient.limits.proxies {
            return Err(CapabilityFailure::LimitExceeded);
        }
        counts.proxies += 1;
        drop(counts);
        Ok(PresentationComponent {
            publication: self.publication.clone(),
            recipient: Arc::clone(&self.recipient),
        })
    }

    pub fn as_presentation_component(&self) -> Result<PresentationComponent, CapabilityFailure> {
        self.redeem(&PRESENTATION_COMPONENT_CONTRACT)
    }
}

impl Drop for CapabilityRef {
    fn drop(&mut self) {
        let mut counts = self.recipient.counts.lock().unwrap();
        counts.references = counts.references.checked_sub(1).unwrap();
    }
}

/// Typed consumer proxy. The consumer owns this handle and drops it
/// normally; provider closure makes it inert (`component()` reports
/// `Revoked`) without removing it from its owner.
pub struct PresentationComponent {
    publication: CapabilityPublication,
    recipient: Arc<SessionState>,
}

impl PresentationComponent {
    pub fn component(&self) -> Result<ComponentRef, CapabilityFailure> {
        self.recipient.check()?;
        self.publication.inner.live_component()
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
mod tests {
    use super::*;

    fn bank(limits: CapabilityLimits) -> (Host, CapabilitySession, Instance) {
        let host = Host::new();
        let instance = host.start("provider");
        (
            host.clone(),
            CapabilitySession::new(host, instance.clone(), limits),
            instance,
        )
    }

    const ACTION_CONTRACT: ContractIdentity = ContractIdentity {
        namespace: "townframe:dayframe",
        name: "action",
        version: (0, 3, 0),
    };

    #[test]
    fn two_recipients_get_distinct_refs_and_redeem_independently() {
        let (host, provider, _instance) = bank(CapabilityLimits::default());
        let lease = provider.publish_presentation_component().unwrap();
        let publication = lease.publication();
        let consumer_a =
            CapabilitySession::new(host.clone(), host.start("a"), CapabilityLimits::default());
        let consumer_b =
            CapabilitySession::new(host.clone(), host.start("b"), CapabilityLimits::default());
        let ref_a = consumer_a.mint(&publication).unwrap();
        let ref_b = consumer_b.mint(&publication).unwrap();
        assert_eq!(ref_a.publication_id(), ref_b.publication_id());
        assert_ne!(ref_a.recipient_scope(), ref_b.recipient_scope());
        let proxy_a = ref_a.as_presentation_component().unwrap();
        let proxy_b = ref_b.as_presentation_component().unwrap();
        assert_eq!(proxy_a.component().unwrap(), proxy_b.component().unwrap());
        assert_eq!(consumer_a.proxy_count(), 1);
        drop(proxy_a);
        assert_eq!(consumer_a.proxy_count(), 0);
        assert!(proxy_b.component().is_ok());
    }

    #[test]
    fn wrong_contract_redemption_fails_without_exposing_the_resource() {
        let (_host, provider, _instance) = bank(CapabilityLimits::default());
        let lease = provider.publish_presentation_component().unwrap();
        let reference = lease.reference().unwrap();
        assert_eq!(
            reference.redeem(&ACTION_CONTRACT).err(),
            Some(CapabilityFailure::ContractMismatch)
        );
        assert_eq!(provider.proxy_count(), 0);
        assert!(reference.as_presentation_component().is_ok());
    }

    #[test]
    fn lease_drop_and_provider_closure_make_retained_proxies_inert_but_droppable() {
        let (host, provider, instance) = bank(CapabilityLimits::default());
        let consumer = CapabilitySession::new(
            host.clone(),
            host.start("consumer"),
            CapabilityLimits::default(),
        );
        let lease = provider.publish_presentation_component().unwrap();
        let proxy = consumer
            .mint(&lease.publication())
            .unwrap()
            .as_presentation_component()
            .unwrap();
        assert!(proxy.component().is_ok());
        drop(lease);
        assert_eq!(proxy.component().err(), Some(CapabilityFailure::Revoked));
        assert!(proxy.is_revoked());
        drop(proxy);
        assert_eq!(consumer.proxy_count(), 0);

        let lease = provider.publish_presentation_component().unwrap();
        let proxy = consumer
            .mint(&lease.publication())
            .unwrap()
            .as_presentation_component()
            .unwrap();
        host.close_instance(&instance).unwrap();
        assert_eq!(proxy.component().err(), Some(CapabilityFailure::Revoked));
        drop(proxy);
        drop(lease);
    }

    #[test]
    fn stale_cleanup_cannot_revoke_a_replacement_publication() {
        let (host, provider, instance) = bank(CapabilityLimits::default());
        let consumer = CapabilitySession::new(
            host.clone(),
            host.start("consumer"),
            CapabilityLimits::default(),
        );
        let stale = provider.publish_presentation_component().unwrap();
        let stale_proxy = consumer
            .mint(&stale.publication())
            .unwrap()
            .as_presentation_component()
            .unwrap();
        host.close_instance(&instance).unwrap();
        let replacement_instance = host.start("provider-replacement");
        let replacement = CapabilitySession::new(
            host.clone(),
            replacement_instance,
            CapabilityLimits::default(),
        );
        let fresh = replacement.publish_presentation_component().unwrap();
        // Stale lease cleanup happens only now, after the replacement exists.
        drop(stale);
        assert_eq!(
            stale_proxy.component().err(),
            Some(CapabilityFailure::Revoked)
        );
        let fresh_proxy = consumer
            .mint(&fresh.publication())
            .unwrap()
            .as_presentation_component()
            .unwrap();
        assert!(fresh_proxy.component().is_ok());
    }

    #[test]
    fn limits_release_capacity_on_drop() {
        let limits = CapabilityLimits {
            publications: 1,
            references: 1,
            proxies: 1,
        };
        let (_host, provider, _instance) = bank(limits);
        let lease = provider.publish_presentation_component().unwrap();
        assert_eq!(
            provider.publish_presentation_component().err(),
            Some(CapabilityFailure::LimitExceeded)
        );
        let reference = lease.reference().unwrap();
        assert_eq!(
            lease.reference().err(),
            Some(CapabilityFailure::LimitExceeded)
        );
        let proxy = reference.as_presentation_component().unwrap();
        assert_eq!(
            reference.as_presentation_component().err(),
            Some(CapabilityFailure::LimitExceeded)
        );
        drop(proxy);
        assert!(reference.as_presentation_component().is_ok());
        drop(reference);
        assert_eq!(provider.reference_count(), 0);
        assert!(lease.reference().is_ok());
        drop(lease);
        assert_eq!(provider.publication_count(), 0);
        assert!(provider.publish_presentation_component().is_ok());
    }
}
