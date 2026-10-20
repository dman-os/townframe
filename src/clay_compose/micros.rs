//! Transient ownership and provider-dispatch primitives for micro instances and integrations.
//!
//! The registry is deliberately a narrow host lifecycle boundary, not an ambient capability
//! resolver or interface-enumeration service. Boot reconciliation will own root scopes; a granted
//! instantiation facility creates children under explicit owners. Ambient owners are explicit
//! application-lived scopes, not detached work or implicit persistence.

pub mod composition;
mod provisions;
pub use provisions::Provision;

use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    sync::{Arc, Mutex},
    task::Poll,
};

use clay_host::{AccessError, Host, HostedCollection, Instance};
use futures::stream::{FuturesUnordered, StreamExt};

use tokio::sync::watch;

type LocalFuture = Pin<Box<dyn Future<Output = ()>>>;

struct OwnedFuture {
    owner: Instance,
    future: LocalFuture,
}

/// Incarnation-owned futures, scheduled by a `FuturesUnordered` so a wake polls only the woken
/// future. Each future sits in a slot shared with `owners`: the running entry polls through the
/// slot, and `reap` empties the slot of a closed owner so the future and everything it captured
/// are dropped immediately, even while nobody is driving the set.
struct OwnedFutures {
    incoming: tokio::sync::mpsc::UnboundedReceiver<OwnedFuture>,
    running: FuturesUnordered<LocalFuture>,
    owners: Vec<(Instance, Weak<RefCell<Option<LocalFuture>>>)>,
}

impl OwnedFutures {
    fn admit(&mut self, owned: OwnedFuture) {
        let slot = Rc::new(RefCell::new(Some(owned.future)));
        self.owners.push((owned.owner, Rc::downgrade(&slot)));
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
        futures.owners.retain(|(owner, slot)| {
            let Some(slot) = slot.upgrade() else {
                return false;
            };
            if owner.check().is_ok() {
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
/// contracts; they are metadata, not authority grants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MicroDescriptor {
    pub id: MicroId,
    pub version: u32,
    pub provisions: Vec<MicroProvision>,
    pub requirements: Vec<MicroProvision>,
}

#[derive(Clone)]
pub struct MicroManifest<C> {
    pub descriptor: MicroDescriptor,
    pub config: Option<ConfigManifest>,
    pub contributions: Vec<C>,
}

#[derive(Clone)]
pub struct ConfigManifest {
    pub name: String,
    pub facets: Vec<daybook_types::manifest::RoutineFacetAccess>,
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
    documents: clay_data::DocumentService,
    owned_futures: tokio::sync::mpsc::UnboundedSender<OwnedFuture>,
}

impl MicroActivation {
    pub fn instance(&self) -> Instance {
        self.instance.clone()
    }

    /// A host-owned contribution collection provided by this incarnation. Its readers and every
    /// contributor's entries are withdrawn when this incarnation closes.
    pub fn collection<T: Clone + Send + Sync + 'static>(
        &self,
    ) -> Result<HostedCollection<T>, AccessError> {
        self.instance.check()?;
        Ok(HostedCollection::new(&self.host, &self.instance))
    }

    /// Grant this activation's approved configuration facets (ADR 015 §7): the manifest ACL is
    /// validated by the caller's adapter; the document service enforces the resolved routes.
    pub fn grant_config(
        &self,
        invoking_plug: &str,
        acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    ) -> Result<BTreeMap<String, clay_data::DocumentRef>, AccessError> {
        self.documents
            .grant_config(&self.host, &self.instance, invoking_plug, acl)
    }

    pub fn own_future(
        &self,
        future: impl Future<Output = ()> + 'static,
    ) -> Result<(), AccessError> {
        self.instance.check()?;
        let instance = self.instance.clone();
        self.owned_futures
            .send(OwnedFuture {
                owner: instance.clone(),
                future: Box::pin(async move {
                    tokio::select! {
                        biased;
                        _ = instance.closed() => {},
                        () = future => {},
                    }
                }),
            })
            .map_err(|_| AccessError::Revoked)
    }

    pub fn require<T: Provision>(&self) -> Result<T, AccessError> {
        assert!(
            self.descriptor
                .requirements
                .iter()
                .any(|requirement| requirement.id == T::ID && requirement.version == T::VERSION),
            "adapter requested an undeclared requirement"
        );
        self.instance.check()?;
        self.requirements
            .get(T::ID)
            .ok_or(AccessError::Denied)?
            .get::<T>()
    }

    pub fn publish<T: Provision>(&self, value: T) {
        self.instance
            .check()
            .expect("publication from a closed activation");
        assert!(
            self.descriptor
                .provisions
                .iter()
                .any(|provision| provision.id == T::ID && provision.version == T::VERSION),
            "adapter published an undeclared provision"
        );
        assert!(
            self.pending
                .lock()
                .unwrap()
                .as_mut()
                .expect("publication after activation completed")
                .insert(T::ID.into(), Arc::new(value))
                .is_none(),
            "adapter published a provision twice"
        );
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
    documents: clay_data::DocumentService,
    owned_futures: tokio::sync::mpsc::UnboundedSender<OwnedFuture>,
    future_driver: Rc<RefCell<Option<OwnedFutures>>>,
}

impl Default for MicroRegistry {
    /// Registry for document-less tests: the private service never receives a grant.
    fn default() -> Self {
        Self::new(clay_data::DocumentService::new(32))
    }
}

impl MicroRegistry {
    pub fn new(documents: clay_data::DocumentService) -> Self {
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
    host: Host,
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
        self.future.as_mut().poll(context)
    }

    pub fn commit(mut self) -> Result<Instance, MicroRegistryError> {
        self.instance.check()?;
        let values = self.pending.lock().unwrap().take().unwrap();
        if let Some(id) = unpublished(&self.descriptor, &values) {
            return Err(MicroRegistryError::MissingProvision(id.to_owned()));
        }
        self.live
            .commit(&self.host, &self.instance, values, &self.descriptor)?;
        self.instance.check()?;
        self.committed = true;
        Ok(self.instance.clone())
    }
}

impl Drop for PendingActivation {
    fn drop(&mut self) {
        self.pending.lock().unwrap().take();
        if !self.committed {
            self.instance.close();
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
        let instance = host.activate(instance_name, Ok)?;
        let pending = Arc::new(Mutex::new(Some(BTreeMap::new())));
        let mut activation = PendingActivation {
            instance: instance.clone(),
            host: host.clone(),
            future: Box::pin(std::future::pending()),
            descriptor: descriptor.clone(),
            pending: pending.clone(),
            live: self.live.clone(),
            future_driver: self.future_driver.clone(),
            committed: false,
        };
        for provision in requirements.values() {
            provision.owner.check()?;
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
        activation.future = Box::pin(async move {
            tokio::select! {
                biased;
                _ = instance.closed() => Err(AccessError::Revoked.into()),
                result = async move { provider.activate(context).await } => result,
            }
        });
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
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_dispatches_a_provider_through_host_lifecycle() {
        let host = Host::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let provider_id = MicroId::new("dayframe.test");
        let mut registry = MicroRegistry::default();
        let seen_by_provider = seen.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: provider_id.clone(),
                    version: 1,
                    provisions: Vec::new(),
                    requirements: Vec::new(),
                },
                move |context| {
                    seen_by_provider
                        .lock()
                        .unwrap()
                        .push(context.instance().name.clone());
                    Ok(())
                },
            ))
            .unwrap();
        let second_id = MicroId::new("dayframe.other");
        let second_seen = seen.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: second_id.clone(),
                    version: 1,
                    provisions: Vec::new(),
                    requirements: Vec::new(),
                },
                move |context| {
                    second_seen
                        .lock()
                        .unwrap()
                        .push(format!("other:{}", context.instance().name));
                    Ok(())
                },
            ))
            .unwrap();
        let other = registry
            .activate(&host, &second_id, "other-test", BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(other.name, "other-test");
        other.close();

        let instance = registry
            .activate(&host, &provider_id, "configured-test", BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["other:other-test", "configured-test"]
        );
        assert_eq!(instance.name, "configured-test");
        instance.close();
        assert_eq!(instance.check(), Err(AccessError::Revoked));
        host.close();
    }

    #[tokio::test]
    async fn closed_host_rejects_registry_activation_without_calling_provider() {
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let id = MicroId::new("closed-host");
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: id.clone(),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![],
                },
                |_| panic!("closed host must reject activation before invoking provider"),
            ))
            .unwrap();
        host.close();
        assert!(matches!(
            registry
                .activate(&host, &id, "closed", BTreeMap::new())
                .await,
            Err(MicroRegistryError::Access(AccessError::Revoked))
        ));
    }

    #[tokio::test]
    async fn closure_before_first_poll_does_not_invoke_async_provider_factory() {
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let id = MicroId::new("closed-before-poll");
        registry
            .register(MicroFactory::new_async(
                MicroDescriptor {
                    id: id.clone(),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![],
                },
                |_| -> std::future::Ready<Result<(), ActivationError>> {
                    panic!("closed activation must not invoke provider factory")
                },
            ))
            .unwrap();
        let mut activation = registry
            .begin(&host, &id, "closed".into(), &BTreeMap::new())
            .unwrap();
        activation.instance.close();
        assert_eq!(
            std::future::poll_fn(|context| activation.poll(context)).await,
            Err(ActivationError::Access(AccessError::Revoked))
        );
        host.close();
    }

    #[tokio::test]
    async fn registry_driver_polls_local_work_without_application_or_surface() {
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let id = MicroId::new("local-work");
        let seen = Rc::new(std::cell::Cell::new(0));
        let observed = seen.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: id.clone(),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![],
                },
                move |activation| {
                    let observed = observed.clone();
                    activation.own_future(async move { observed.set(observed.get() + 1) })
                },
            ))
            .unwrap();
        let instance = registry
            .activate(&host, &id, "local", BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(seen.get(), 0);
        let mut driver = Box::pin(registry.drive_owned_futures());
        std::future::poll_fn(|context| {
            assert!(driver.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(seen.get(), 1);
        std::future::poll_fn(|context| {
            assert!(driver.as_mut().poll(context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(seen.get(), 1);
        instance.close();
        drop(driver);
        host.close();
    }

    #[tokio::test]
    async fn registry_failure_drops_queued_work_without_polling_a_driver() {
        struct Dropped(Rc<std::cell::Cell<bool>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let id = MicroId::new("queued-failure");
        let dropped = Rc::new(std::cell::Cell::new(false));
        let observed = dropped.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: id.clone(),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![],
                },
                move |activation| {
                    let guard = Dropped(observed.clone());
                    activation.own_future(async move {
                        let _guard = guard;
                        panic!("failed activation's queued work must not be polled");
                    })?;
                    Err(AccessError::Denied)
                },
            ))
            .unwrap();
        assert!(matches!(
            registry
                .activate(&host, &id, "failed", BTreeMap::new())
                .await,
            Err(MicroRegistryError::Access(AccessError::Denied))
        ));
        assert!(dropped.get());
        host.close();
    }

    #[tokio::test]
    async fn closing_an_owner_drops_its_pending_future_on_reap_without_polling() {
        struct Dropped(Rc<std::cell::Cell<bool>>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let id = MicroId::new("closing-owner");
        let dropped = Rc::new(std::cell::Cell::new(false));
        let observed = dropped.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: id.clone(),
                    version: 1,
                    requirements: vec![],
                    provisions: vec![],
                },
                move |activation| {
                    let guard = Dropped(observed.clone());
                    activation.own_future(async move {
                        let _guard = guard;
                        std::future::pending::<()>().await;
                    })
                },
            ))
            .unwrap();
        let first = registry
            .activate(&host, &id, "first", BTreeMap::new())
            .await
            .unwrap();
        let second = registry
            .activate(&host, &id, "second", BTreeMap::new())
            .await
            .unwrap();
        let mut driver = registry.future_driver();
        driver.reap();
        assert!(!dropped.get());
        first.close();
        driver.reap();
        assert!(dropped.get());
        assert_eq!(driver.futures.as_ref().unwrap().owners.len(), 1);
        drop(driver);
        second.close();
        host.close();
    }

    #[tokio::test]
    async fn async_micro_factory_activates_and_rolls_back_on_error() {
        let host = Host::new();
        let mut registry = MicroRegistry::default();
        let ok_id = MicroId::new("dayframe.async.ok");
        let fail_id = MicroId::new("dayframe.async.fail");
        let leaked = Arc::new(Mutex::new(None));
        let capture = leaked.clone();
        let provider_host = host.clone();
        registry
            .register(MicroFactory::new_async(
                MicroDescriptor {
                    id: ok_id.clone(),
                    version: 1,
                    provisions: vec![],
                    requirements: vec![],
                },
                |context| async move {
                    tokio::task::yield_now().await;
                    context.instance().check()?;
                    Ok(())
                },
            ))
            .unwrap();
        registry
            .register(MicroFactory::new_async(
                MicroDescriptor {
                    id: fail_id.clone(),
                    version: 1,
                    provisions: vec![],
                    requirements: vec![],
                },
                move |context| {
                    let capture = capture.clone();
                    let host = provider_host.clone();
                    async move {
                        tokio::task::yield_now().await;
                        let component = host.publish_component(&context.instance())?;
                        *capture.lock().unwrap() = Some((context.instance(), component));
                        Err(AccessError::Denied.into())
                    }
                },
            ))
            .unwrap();
        let instance = registry
            .activate(&host, &ok_id, "async-ok", BTreeMap::new())
            .await
            .unwrap();
        assert!(instance.check().is_ok());
        assert!(matches!(
            registry
                .activate(&host, &fail_id, "async-fail", BTreeMap::new())
                .await,
            Err(MicroRegistryError::Access(AccessError::Denied))
        ));
        let (failed_instance, _) = leaked.lock().unwrap().take().unwrap();
        assert_eq!(failed_instance.check(), Err(AccessError::Revoked));
        host.close();
    }

    #[tokio::test]
    async fn failed_provider_activation_rolls_back_published_effects() {
        let host = Host::new();
        let root = host.start("root");
        let provider_id = MicroId::new("dayframe.failing");
        let published = Arc::new(Mutex::new(None));
        let published_by_provider = published.clone();
        let mut registry = MicroRegistry::default();
        let provider_host = host.clone();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: provider_id.clone(),
                    version: 1,
                    provisions: Vec::new(),
                    requirements: Vec::new(),
                },
                move |context| {
                    let component = provider_host.publish_component(&context.instance())?;
                    *published_by_provider.lock().unwrap() = Some((context.instance(), component));
                    Err(AccessError::Denied)
                },
            ))
            .unwrap();

        assert!(matches!(
            registry
                .activate(&host, &provider_id, "failing", BTreeMap::new())
                .await,
            Err(MicroRegistryError::Access(AccessError::Denied))
        ));
        let (instance, component) = published.lock().unwrap().take().unwrap();
        assert_eq!(instance.check(), Err(AccessError::Revoked));
        assert!(matches!(
            host.mount(&root, component),
            Err(AccessError::Revoked)
        ));
        host.close();
    }
}
