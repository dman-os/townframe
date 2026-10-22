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

use tokio::sync::watch;

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MicroDescriptor {
    pub id: MicroId,
    pub version: u32,
    pub provisions: Vec<MicroProvision>,
    pub requirements: Vec<MicroProvision>,
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
}

impl MicroActivation {
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
            activate: Rc::new(move |context| Box::pin(activate(context))),
        }
    }
}

impl MicroProvider for MicroFactory {
    fn descriptor(&self) -> MicroDescriptor {
        self.descriptor.clone()
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
        let instance = host.try_start(instance_name)?;
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
