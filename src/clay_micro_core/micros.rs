//! Transient ownership and provider-dispatch primitives for micro instances and integrations.
//!
//! The registry is deliberately a narrow host lifecycle boundary, not an ambient capability
//! resolver or interface-enumeration service. Boot reconciliation will own root scopes; a granted
//! instantiation facility creates children under explicit owners. Ambient owners are explicit
//! application-lived scopes, not detached work or implicit persistence.

pub mod composition;
mod provisions;
pub use provisions::Provision;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use clay_host::AccessError;
use clay_host::Host;
use clay_host::HostedCollection;
use clay_host::Instance;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use serde::Deserialize;
use serde::Serialize;

use tokio::sync::watch;

use self::composition::ConfiguredMicro;

type LocalFuture = Pin<Box<dyn Future<Output = ()>>>;

struct OwnedFuture {
    host: Host,
    owner: Instance,
    future: LocalFuture,
}

struct FutureOwner {
    host: Host,
    instance: Instance,
    slot: Weak<RefCell<Option<LocalFuture>>>,
}

/// Incarnation-owned futures, scheduled by a `FuturesUnordered` so a wake polls only the woken
/// future. Each future sits in a slot shared with `owners`: the running entry polls through the
/// slot, and `reap` empties the slot of a closed owner so the future and everything it captured
/// are dropped immediately, even while nobody is driving the set.
struct OwnedFutures {
    incoming: tokio::sync::mpsc::UnboundedReceiver<OwnedFuture>,
    running: FuturesUnordered<LocalFuture>,
    owners: Vec<FutureOwner>,
}

impl OwnedFutures {
    fn admit(&mut self, owned: OwnedFuture) {
        let slot = Rc::new(RefCell::new(Some(owned.future)));
        self.owners.push(FutureOwner {
            host: owned.host,
            instance: owned.owner,
            slot: Rc::downgrade(&slot),
        });
        self.running
            .push(Box::pin(std::future::poll_fn(move |context| {
                let mut slot = slot.borrow_mut();
                let Some(future) = slot.as_mut() else {
                    return Poll::Ready(());
                };
                let poll = future.as_mut().poll(context);
                if poll.is_ready() {
                    *slot = None;
                }
                poll
            })));
    }
}

pub struct FutureDriver<'a> {
    storage: &'a RefCell<Option<OwnedFutures>>,
    futures: Option<OwnedFutures>,
}

impl<'a> FutureDriver<'a> {
    fn take(storage: &'a RefCell<Option<OwnedFutures>>) -> Option<Self> {
        let futures = storage.borrow_mut().take()?;
        Some(Self {
            storage,
            futures: Some(futures),
        })
    }

    /// Admits newly registered futures and drops those whose owner has closed, without polling.
    pub fn reap(&mut self) {
        let futures = self.futures.as_mut().unwrap();
        while let Ok(owned) = futures.incoming.try_recv() {
            futures.admit(owned);
        }
        futures.owners.retain(|owner| {
            let Some(slot) = owner.slot.upgrade() else {
                return false;
            };
            if owner.host.check(&owner.instance).is_ok() {
                return true;
            }
            let future = slot.borrow_mut().take();
            drop(future);
            false
        });
    }

    /// Never completes. No `tokio::spawn`: owned futures are `!Send`, so the composition driver
    /// polls them on the caller's thread (D13), keeping this crate executor-free and wasm32-safe.
    pub async fn run(&mut self) {
        let futures = self.futures.as_mut().unwrap();
        loop {
            let owned = tokio::select! {
                owned = futures.incoming.recv() => owned.expect("registry owns its future sender"),
                Some(()) = futures.running.next(), if !futures.running.is_empty() => continue,
            };
            futures.admit(owned);
        }
    }
}

impl Drop for FutureDriver<'_> {
    fn drop(&mut self) {
        self.reap();
        assert!(
            self.storage
                .borrow_mut()
                .replace(self.futures.take().unwrap())
                .is_none()
        );
    }
}
/// Stable provider identity used by configuration and composition persistence.
#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct MicroId(String);

impl MicroId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MicroProvision {
    pub id: String,
    pub version: u32,
}

/// The host-visible declaration for one implementation provider. Provisions describe contribution
/// contracts; they are metadata, not authority grants. This is the composition core's whole view
/// of a micro: the authoring-side manifest (identity, ACL, contributions) belongs to the MDK, and
/// its adapter lowers it into this descriptor.
#[derive(Clone, Debug, PartialEq)]
pub struct MicroDescriptor {
    pub id: MicroId,
    pub version: u32,
    pub provisions: Vec<MicroProvision>,
    pub requirements: Vec<MicroProvision>,
}

/// One static declaration. `provision` names a declared requirement of the contributor;
/// `payload` is the acceptor's semantic data. Deliberately free of MDK types: this crate is the
/// lifecycle boundary, and declarations are plain wire data.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MicroContribution {
    pub provision: String,
    pub payload: serde_json::Value,
}

/// One static declaration resolved to the configured instance that offers it. This is the row a
/// consumer micro reads without activating the contributor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Declaration {
    pub instance: String,
    pub contract: String,
    pub payload: serde_json::Value,
}

/// The composition facts a micro may read and change about itself. The host owns routing — every
/// edge crosses it because address spaces are separate — but the micro owns *which* declared rows
/// it accepts, which is how a toolbar can list buttons without the host knowing what a toolbar is
/// (ADR 014 §Composition and lifecycle).
///
/// Every operation is checked against the incarnation that received this handle: once that
/// incarnation closes, a retained clone (a UI hook, a spawned task) gets `Revoked` instead of
/// reading or writing on behalf of a successor.
#[derive(Clone)]
pub struct CompositionBroker {
    host: Host,
    instance: Instance,
    name: String,
    providers: Rc<RefCell<BTreeMap<MicroId, Rc<dyn MicroProvider>>>>,
    desired: Rc<RefCell<BTreeMap<String, ConfiguredMicro>>>,
    policy: Rc<RefCell<self::composition::CompositionPolicy>>,
    changes: tokio::sync::watch::Sender<()>,
}

pub struct DeclarationWatch {
    broker: CompositionBroker,
    contract: String,
    changes: watch::Receiver<()>,
}

impl DeclarationWatch {
    pub fn current(&mut self) -> Result<Vec<Declaration>, AccessError> {
        self.changes.borrow_and_update();
        self.broker.declarations(&self.contract)
    }

    pub async fn next(&mut self) -> Result<Vec<Declaration>, AccessError> {
        tokio::select! {
            biased;
            _ = self.broker.host.await_close(&self.broker.instance) => Err(AccessError::Revoked),
            result = self.changes.changed() => {
                result.map_err(|_| AccessError::Revoked)?;
                self.current()
            }
        }
    }
}

impl CompositionBroker {
    pub fn watch(&self, contract: &str) -> Result<DeclarationWatch, AccessError> {
        self.host.check(&self.instance)?;
        Ok(DeclarationWatch {
            broker: self.clone(),
            contract: contract.to_owned(),
            changes: self.changes.subscribe(),
        })
    }
    /// Every declared contribution to `contract` addressed to this instance — the contributor's
    /// configured binding for that contract names it — in configured-instance order. Payloads are
    /// the contributor's semantic data; nothing here activates the contributor.
    pub fn declarations(&self, contract: &str) -> Result<Vec<Declaration>, AccessError> {
        self.host.check(&self.instance)?;
        let providers = self.providers.borrow();
        let registry: BTreeMap<_, _> = providers
            .iter()
            .map(|(id, provider)| (id.clone(), provider.contributions()))
            .collect();
        Ok(self
            .desired
            .borrow()
            .iter()
            .filter(|(_, entry)| {
                composition::contributes_to(&registry, entry, contract, &self.name)
            })
            .filter_map(|(instance, entry)| {
                registry[&entry.provider]
                    .iter()
                    .find(|contribution| contribution.provision == contract)
                    .map(|contribution| Declaration {
                        instance: instance.clone(),
                        contract: contract.to_owned(),
                        payload: contribution.payload.clone(),
                    })
            })
            .collect())
    }

    /// The rows this instance currently accepts: contributor instance -> contract.
    pub fn accepted(&self) -> Result<BTreeMap<String, String>, AccessError> {
        self.host.check(&self.instance)?;
        Ok(self
            .policy
            .borrow()
            .demand
            .get(&self.name)
            .cloned()
            .unwrap_or_default())
    }

    /// Replace this instance's accepted rows. Accepting a declared row keeps its contributor
    /// live; withdrawing the last one evicts it. Rows for undeclared or unaddressed contributions
    /// are denied immediately; stale accepted rows cease to demand removed or rebound contributors.
    pub fn accept(&self, rows: BTreeMap<String, String>) -> Result<(), AccessError> {
        self.host.check(&self.instance)?;
        let registry = self
            .providers
            .borrow()
            .iter()
            .map(|(id, provider)| (id.clone(), provider.contributions()))
            .collect();
        let desired = self.desired.borrow();
        if rows.iter().any(|(contributor, contract)| {
            desired.get(contributor).is_none_or(|entry| {
                !composition::contributes_to(&registry, entry, contract, &self.name)
            })
        }) {
            return Err(AccessError::Denied);
        }
        {
            let mut policy = self.policy.borrow_mut();
            if policy.demand.get(&self.name).cloned().unwrap_or_default() == rows {
                return Ok(());
            }
            if rows.is_empty() {
                policy.demand.remove(&self.name);
            } else {
                policy.demand.insert(self.name.clone(), rows);
            }
        }
        // Same wake the shell's root edits use: the driver re-evaluates liveness.
        self.changes.send_replace(());
        Ok(())
    }

    /// Accept every declared contribution to `contracts` addressed to this instance from now on,
    /// including rows declared after this call. A standing rule, so a shell micro lists a growing
    /// registry without being re-activated for each new contributor.
    pub fn accept_all(
        &self,
        contracts: impl IntoIterator<Item = String>,
    ) -> Result<(), AccessError> {
        self.host.check(&self.instance)?;
        let contracts: std::collections::BTreeSet<String> = contracts.into_iter().collect();
        {
            let mut policy = self.policy.borrow_mut();
            if policy
                .accept_all
                .get(&self.name)
                .cloned()
                .unwrap_or_default()
                == contracts
            {
                return Ok(());
            }
            if contracts.is_empty() {
                policy.accept_all.remove(&self.name);
            } else {
                policy.accept_all.insert(self.name.clone(), contracts);
            }
        }
        self.changes.send_replace(());
        Ok(())
    }
}

/// Provision values staged by an activation before commit. Glossary: ADR 015 §Vocabulary.
type PendingProvisions = BTreeMap<String, Arc<dyn std::any::Any + Send + Sync>>;

/// Trusted native activation adapter. Feature code receives the bound capabilities it constructs,
/// not this surface. Dependencies are resolved only through configured bindings.
/// The value carries exactly what one activation may use: its instance token, the resolved
/// declared requirements, a staging area for publications, configuration grants, collections
/// provided by this incarnation, and the owned-future registration point. It never hands out the
/// [`Host`]: every facility below is checked against this incarnation (ADR 015 §7).
#[derive(Clone)]
pub struct MicroActivation {
    host: Host,
    instance: Instance,
    descriptor: MicroDescriptor,
    requirements: BTreeMap<String, provisions::Published>,
    pending: Arc<Mutex<Option<PendingProvisions>>>,
    documents: clay_docs::DocumentService,
    owned_futures: tokio::sync::mpsc::UnboundedSender<OwnedFuture>,
    broker: CompositionBroker,
}

impl MicroActivation {
    /// This activation's composition broker: static declarations it may read, and the accepted
    /// rows it may change for its own instance.
    pub fn broker(&self) -> &CompositionBroker {
        &self.broker
    }

    pub fn instance(&self) -> Instance {
        self.instance.clone()
    }

    /// The declaration this activation runs under: what it may require and publish.
    pub fn descriptor(&self) -> &MicroDescriptor {
        &self.descriptor
    }

    /// A host-owned contribution collection provided by this incarnation. Its readers and every
    /// contributor's entries are withdrawn when this incarnation closes.
    pub fn collection<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> Result<HostedCollection<T>, AccessError> {
        self.host.check(&self.instance)?;
        Ok(HostedCollection::new(&self.host, &self.instance))
    }

    /// Grant this activation's approved configuration facets (ADR 015 §7): the manifest ACL is
    /// validated by the caller's adapter; the document service enforces the resolved routes.
    pub fn grant_config(
        &self,
        invoking_plug: &str,
        acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    ) -> Result<BTreeMap<String, clay_docs::DocumentRef>, AccessError> {
        self.documents
            .grant_config(&self.host, &self.instance, invoking_plug, acl)
    }

    pub fn own_future(
        &self,
        future: impl Future<Output = ()> + 'static,
    ) -> Result<(), AccessError> {
        self.host.check(&self.instance)?;
        let instance = self.instance.clone();
        let host = self.host.clone();
        self.owned_futures
            .send(OwnedFuture {
                host: host.clone(),
                owner: instance.clone(),
                future: Box::pin(async move {
                    tokio::select! {
                        biased;
                        _ = host.await_close(&instance) => {},
                        () = future => {},
                    }
                }),
            })
            .map_err(|_| AccessError::Revoked)
    }

    /// A declared requirement resolved by contract id and version rather than Rust type — for an
    /// adapter whose caller only has the contract's name (a manifest's contribution target).
    /// Declarations are checked the same way; the value is whatever the provider stored.
    pub fn require_erased(
        &self,
        id: &str,
        version: u32,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, AccessError> {
        assert!(
            self.descriptor
                .requirements
                .iter()
                .any(|requirement| requirement.id == id && requirement.version == version),
            "adapter requested an undeclared requirement"
        );
        self.host.check(&self.instance)?;
        self.requirements
            .get(id)
            .ok_or(AccessError::Denied)?
            .get_erased(version)
    }

    pub fn require<T: Provision>(&self) -> Result<T, AccessError> {
        Ok(self
            .require_erased(T::ID, T::VERSION)?
            .downcast_ref::<T>()
            .expect("contract identity mapped to two native types")
            .clone())
    }

    /// Stage a provision under a contract named at runtime; the typed [`Self::publish`] is sugar
    /// over this. The contract must be declared, and each contract is published once.
    pub fn publish_erased(
        &self,
        id: &str,
        version: u32,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) {
        self.host
            .check(&self.instance)
            .expect("publication from a closed activation");
        assert!(
            self.descriptor
                .provisions
                .iter()
                .any(|provision| provision.id == id && provision.version == version),
            "adapter published an undeclared provision"
        );
        assert!(
            self.pending
                .lock()
                .unwrap()
                .as_mut()
                .expect("publication after activation completed")
                .insert(id.into(), value)
                .is_none(),
            "adapter published a provision twice"
        );
    }

    pub fn publish<T: Provision>(&self, value: T) {
        self.publish_erased(T::ID, T::VERSION, Arc::new(value));
    }
}

/// A dynamically dispatched provider (ADR 015 §7). One asynchronous activation contract:
/// provider implementations remain free to use their own typed activation context, and a
/// synchronous provider is the degenerate case wrapped by [`MicroFactory`]. The host only owns
/// the common instance/lifecycle boundary.
pub trait MicroProvider {
    fn descriptor(&self) -> MicroDescriptor;
    /// Static declaration registry rows, readable without an instance (ADR 014 §Composition).
    fn contributions(&self) -> Vec<MicroContribution> {
        Vec::new()
    }
    fn activate<'a>(
        &'a self,
        context: MicroActivation,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActivationError>> + 'a>>;
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error("micro initialization failed: {0}")]
    Initialization(String),
}

type ActivateFn =
    Rc<dyn Fn(MicroActivation) -> Pin<Box<dyn Future<Output = Result<(), ActivationError>>>>>;

/// Closure-backed [`MicroProvider`]. `new` takes the common native activation that binds
/// capabilities and publishes outputs without awaiting; `new_async` takes one that awaits. Either
/// runs inside the composition driver's pending-activation poll, and dropping or failing it rolls
/// back all owned effects and staged provisions.
pub struct MicroFactory {
    descriptor: MicroDescriptor,
    contributions: Vec<MicroContribution>,
    activate: ActivateFn,
}

impl MicroFactory {
    pub fn new(
        descriptor: MicroDescriptor,
        activate: impl Fn(MicroActivation) -> Result<(), AccessError> + 'static,
    ) -> Self {
        Self::new_async(descriptor, move |context| {
            std::future::ready(activate(context).map_err(ActivationError::from))
        })
    }

    pub fn new_async<F, Fut>(descriptor: MicroDescriptor, activate: F) -> Self
    where
        F: Fn(MicroActivation) -> Fut + 'static,
        Fut: Future<Output = Result<(), ActivationError>> + 'static,
    {
        Self {
            descriptor,
            contributions: Vec::new(),
            activate: Rc::new(move |context| Box::pin(activate(context))),
        }
    }

    /// Attach the manifest's declared contributions. They are registry data, not activation
    /// effects: a shell can enumerate them while this micro is idle.
    pub fn with_contributions(mut self, contributions: Vec<MicroContribution>) -> Self {
        self.contributions = contributions;
        self
    }
}

impl MicroProvider for MicroFactory {
    fn descriptor(&self) -> MicroDescriptor {
        self.descriptor.clone()
    }

    fn contributions(&self) -> Vec<MicroContribution> {
        self.contributions.clone()
    }

    fn activate<'a>(
        &'a self,
        context: MicroActivation,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActivationError>> + 'a>> {
        (self.activate)(context)
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum MicroRegistryError {
    #[error("micro provider is already registered: {0}")]
    DuplicateProvider(String),
    #[error("micro provider is not registered: {0}")]
    UnknownProvider(String),
    #[error("invalid dependency binding for contract {0}")]
    InvalidBinding(String),
    #[error("provider did not publish required provision {0}")]
    MissingProvision(String),
    #[error("micro initialization failed: {0}")]
    Initialization(String),
    #[error(transparent)]
    Access(#[from] AccessError),
}

impl From<ActivationError> for MicroRegistryError {
    fn from(error: ActivationError) -> Self {
        match error {
            ActivationError::Access(error) => Self::Access(error),
            ActivationError::Initialization(message) => Self::Initialization(message),
        }
    }
}

/// Host-facing dynamic dispatch for provider implementations.
///
/// Registration is intentionally separate from activation. A configured instance can be started,
/// stopped, and started again without replacing the provider implementation. The registry does not
/// discover arbitrary interfaces: activation names bindings for the provider's declared requirements.
pub struct MicroRegistry {
    providers: Rc<RefCell<BTreeMap<MicroId, Rc<dyn MicroProvider>>>>,
    desired: Rc<RefCell<BTreeMap<String, ConfiguredMicro>>>,
    policy: Rc<RefCell<self::composition::CompositionPolicy>>,
    live: provisions::Provisions,
    changes: watch::Sender<()>,
    documents: clay_docs::DocumentService,
    owned_futures: tokio::sync::mpsc::UnboundedSender<OwnedFuture>,
    future_driver: Rc<RefCell<Option<OwnedFutures>>>,
}

impl Default for MicroRegistry {
    /// Registry for document-less tests: the private service never receives a grant.
    fn default() -> Self {
        Self::new(clay_docs::DocumentService::new(32))
    }
}

impl MicroRegistry {
    pub fn new(documents: clay_docs::DocumentService) -> Self {
        let (owned_futures, incoming) = tokio::sync::mpsc::unbounded_channel();
        Self {
            providers: Rc::default(),
            desired: Rc::default(),
            policy: Rc::default(),
            live: provisions::Provisions::default(),
            changes: watch::channel(()).0,
            documents,
            owned_futures,
            future_driver: Rc::new(RefCell::new(Some(OwnedFutures {
                incoming,
                running: FuturesUnordered::new(),
                owners: Vec::new(),
            }))),
        }
    }
}

impl Clone for MicroRegistry {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            desired: self.desired.clone(),
            policy: self.policy.clone(),
            live: self.live.clone(),
            changes: self.changes.clone(),
            documents: self.documents.clone(),
            owned_futures: self.owned_futures.clone(),
            future_driver: self.future_driver.clone(),
        }
    }
}

struct PreparedActivation {
    provider: Rc<dyn MicroProvider>,
    descriptor: MicroDescriptor,
    requirements: BTreeMap<String, provisions::Published>,
}

pub(super) struct PendingActivation {
    pub instance: Instance,
    pub ended: watch::Receiver<Option<clay_host::CloseReason>>,
    host: Host,
    closed: LocalFuture,
    future: Pin<Box<dyn Future<Output = Result<(), ActivationError>>>>,
    descriptor: MicroDescriptor,
    pending: Arc<Mutex<Option<PendingProvisions>>>,
    live: provisions::Provisions,
    future_driver: Rc<RefCell<Option<OwnedFutures>>>,
    committed: bool,
}

impl PendingActivation {
    pub fn poll(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ActivationError>> {
        let ended = self.ended.borrow().is_some();
        if ended || self.closed.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(AccessError::Revoked.into()));
        }
        if self.host.check(&self.instance).is_err() {
            return Poll::Pending;
        }
        self.future.as_mut().poll(context)
    }

    pub fn commit(&mut self) -> Result<Instance, MicroRegistryError> {
        self.host.check(&self.instance)?;
        let values = self.pending.lock().unwrap().take().unwrap();
        if let Some(id) = unpublished(&self.descriptor, &values) {
            return Err(MicroRegistryError::MissingProvision(id.to_owned()));
        }
        self.live
            .commit(&self.host, &self.instance, values, &self.descriptor)?;
        self.host.check(&self.instance)?;
        self.committed = true;
        Ok(self.instance.clone())
    }
}

impl Drop for PendingActivation {
    fn drop(&mut self) {
        self.pending.lock().unwrap().take();
        if !self.committed {
            self.host.close_instance(&self.instance).unwrap();
            drop(FutureDriver::take(&self.future_driver));
        }
    }
}

fn unpublished<'a>(descriptor: &'a MicroDescriptor, values: &PendingProvisions) -> Option<&'a str> {
    descriptor
        .provisions
        .iter()
        .find(|provision| !values.contains_key(&provision.id))
        .map(|provision| provision.id.as_str())
}

impl MicroRegistry {
    pub fn future_driver(&self) -> FutureDriver<'_> {
        FutureDriver::take(&self.future_driver).expect("registry futures already driven")
    }

    pub async fn drive_owned_futures(&self) {
        self.future_driver().run().await;
    }

    pub fn changes(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }

    /// Privileged inspection of one explicitly named live provision; not a micro service lookup.
    pub fn output<T: Provision>(&self, instance: &str) -> Result<T, AccessError> {
        self.live.get::<T>(instance)
    }

    /// [`Self::output`] for a contract named at runtime.
    pub fn output_erased(
        &self,
        instance: &str,
        contract: &str,
        version: u32,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, AccessError> {
        self.live.get_erased(instance, contract, version)
    }

    pub fn register(
        &mut self,
        provider: impl MicroProvider + 'static,
    ) -> Result<(), MicroRegistryError> {
        let descriptor = provider.descriptor();
        let mut providers = self.providers.borrow_mut();
        if providers.contains_key(&descriptor.id) {
            return Err(MicroRegistryError::DuplicateProvider(
                descriptor.id.as_str().to_owned(),
            ));
        }
        assert!(providers.insert(descriptor.id, Rc::new(provider)).is_none());
        drop(providers);
        self.changes.send_replace(());
        Ok(())
    }

    /// Shared liveness authority. The application sets roots; activations read and change only
    /// their own accepted rows through [`CompositionBroker`]; the reconciler drops rows whose
    /// incarnation ended.
    pub(crate) fn policy(&self) -> Rc<RefCell<self::composition::CompositionPolicy>> {
        self.policy.clone()
    }

    /// Drop every acceptance whose consumer has no incarnation. Called by the reconciler once per
    /// pass with the set of instances that are active or activating.
    pub(crate) fn retain_acceptance(&self, has_incarnation: impl Fn(&str) -> bool) {
        let mut policy = self.policy.borrow_mut();
        policy.demand.retain(|name, _| has_incarnation(name));
        policy.accept_all.retain(|name, _| has_incarnation(name));
    }

    /// Publish the desired configuration so activations can resolve a declaration to the
    /// configured instance that offers it and to the consumer it is bound to.
    pub(crate) fn project_desired(&self, desired: &BTreeMap<String, ConfiguredMicro>) {
        if *self.desired.borrow() != *desired {
            *self.desired.borrow_mut() = desired.clone();
            self.changes.send_replace(());
        }
    }

    /// Static declaration registry, keyed by provider id. No activation required.
    pub fn contribution_registry(&self) -> BTreeMap<MicroId, Vec<MicroContribution>> {
        self.providers
            .borrow()
            .iter()
            .map(|(id, provider)| (id.clone(), provider.contributions()))
            .collect()
    }

    pub fn descriptors(&self) -> impl Iterator<Item = MicroDescriptor> {
        // Release the catalog borrow before invoking provider activation code during reconciliation.
        self.providers
            .borrow()
            .values()
            .map(|provider| provider.descriptor())
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn prepare_activation(
        &self,
        provider: &MicroId,
        bindings: &BTreeMap<String, String>,
    ) -> Result<PreparedActivation, MicroRegistryError> {
        let provider_impl = self
            .providers
            .borrow()
            .get(provider)
            .cloned()
            .ok_or_else(|| MicroRegistryError::UnknownProvider(provider.as_str().to_owned()))?;
        let descriptor = provider_impl.descriptor();
        assert_eq!(
            descriptor
                .requirements
                .iter()
                .map(|requirement| &requirement.id)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            descriptor.requirements.len(),
            "duplicate requirement declaration"
        );
        for id in bindings.keys() {
            if !descriptor
                .requirements
                .iter()
                .any(|requirement| &requirement.id == id)
            {
                return Err(MicroRegistryError::InvalidBinding(id.clone()));
            }
        }
        let mut requirements = BTreeMap::new();
        for requirement in &descriptor.requirements {
            let supplier = bindings
                .get(&requirement.id)
                .ok_or_else(|| MicroRegistryError::InvalidBinding(requirement.id.clone()))?;
            let provision = self.live.resolve(supplier, &requirement.id)?;
            if provision.version != requirement.version {
                return Err(MicroRegistryError::InvalidBinding(requirement.id.clone()));
            }
            requirements.insert(requirement.id.clone(), provision);
        }
        Ok(PreparedActivation {
            provider: provider_impl,
            descriptor,
            requirements,
        })
    }

    pub(super) fn begin(
        &self,
        host: &Host,
        provider: &MicroId,
        instance_name: String,
        bindings: &BTreeMap<String, String>,
    ) -> Result<PendingActivation, MicroRegistryError> {
        let PreparedActivation {
            provider,
            descriptor,
            requirements,
        } = self.prepare_activation(provider, bindings)?;
        let instance = host.try_start(instance_name.clone())?;
        let ended = host.observe_instance(&instance)?;
        let pending = Arc::new(Mutex::new(Some(BTreeMap::new())));
        let mut activation = PendingActivation {
            instance: instance.clone(),
            ended,
            host: host.clone(),
            closed: {
                let host = host.clone();
                let instance = instance.clone();
                Box::pin(async move { host.await_close(&instance).await })
            },
            future: Box::pin(std::future::pending()),
            descriptor: descriptor.clone(),
            pending: pending.clone(),
            live: self.live.clone(),
            future_driver: self.future_driver.clone(),
            committed: false,
        };
        for provision in requirements.values() {
            provision.host.check(&provision.owner)?;
            host.bind_dependency(&instance, &provision.owner)?;
        }
        let context = MicroActivation {
            host: host.clone(),
            instance: instance.clone(),
            descriptor,
            requirements,
            pending,
            documents: self.documents.clone(),
            owned_futures: self.owned_futures.clone(),
            broker: CompositionBroker {
                host: host.clone(),
                instance: instance.clone(),
                name: instance_name,
                providers: self.providers.clone(),
                desired: self.desired.clone(),
                policy: self.policy.clone(),
                changes: self.changes.clone(),
            },
        };
        activation.future = Box::pin(async move { provider.activate(context).await });
        Ok(activation)
    }

    /// Activates one configured instance outside the composition driver. Closing the instance or
    /// dropping this future before it resolves rolls back every owned effect and staged provision.
    pub async fn activate(
        &self,
        host: &Host,
        provider: &MicroId,
        instance_name: impl Into<String>,
        bindings: BTreeMap<String, String>,
    ) -> Result<Instance, MicroRegistryError> {
        let mut activation = self.begin(host, provider, instance_name.into(), &bindings)?;
        std::future::poll_fn(|context| activation.poll(context)).await?;
        activation.commit()
    }
}

#[cfg(test)]
mod tests;
