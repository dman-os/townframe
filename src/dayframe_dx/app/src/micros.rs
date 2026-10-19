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
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::host::{AccessError, Host, Instance};

use tokio::sync::watch;

struct OwnedFuture {
    owner: Instance,
    future: Pin<Box<dyn Future<Output = ()>>>,
}

struct OwnedFutures {
    incoming: tokio::sync::mpsc::UnboundedReceiver<OwnedFuture>,
    active: Vec<OwnedFuture>,
}

pub(crate) struct FutureDriver<'a> {
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

    pub fn reap(&mut self) {
        let futures = self.futures.as_mut().unwrap();
        while let Ok(future) = futures.incoming.try_recv() {
            futures.active.push(future);
        }
        futures.active.retain(|future| future.owner.check().is_ok());
    }

    pub async fn run(&mut self) {
        let futures = self.futures.as_mut().unwrap();
        std::future::poll_fn(|context| {
            while let std::task::Poll::Ready(future) = futures.incoming.poll_recv(context) {
                futures
                    .active
                    .push(future.expect("registry owns its future sender"));
            }
            futures.active.retain_mut(|future| {
                future.owner.check().is_ok() && future.future.as_mut().poll(context).is_pending()
            });
            std::task::Poll::<()>::Pending
        })
        .await;
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

type PendingProvisions = BTreeMap<String, Arc<dyn std::any::Any + Send + Sync>>;

/// Trusted native activation adapter. Feature code receives the bound capabilities it constructs,
/// not this host-authority surface. Dependencies are resolved only through configured bindings.
#[derive(Clone)]
pub struct MicroActivation {
    host: Host,
    instance: Instance,
    descriptor: MicroDescriptor,
    requirements: BTreeMap<String, provisions::Published>,
    pending: Arc<Mutex<Option<PendingProvisions>>>,
    owned_futures: tokio::sync::mpsc::UnboundedSender<OwnedFuture>,
}

impl MicroActivation {
    pub fn host(&self) -> Host {
        self.host.clone()
    }
    pub fn instance(&self) -> Instance {
        self.instance.clone()
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

/// A dynamically dispatched provider. Provider implementations remain free to use their own
/// typed activation context; the host only owns the common instance/lifecycle boundary.
pub trait MicroProvider {
    fn descriptor(&self) -> MicroDescriptor;
    fn activate(&self, context: MicroActivation) -> Result<(), AccessError>;
    fn activate_async<'a>(
        &'a self,
        context: MicroActivation,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActivationError>> + 'a>> {
        Box::pin(async move { self.activate(context).map_err(Into::into) })
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ActivationError {
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error("micro initialization failed: {0}")]
    Initialization(String),
}

/// Native implementation adapter: declares requirements, binds capabilities, and publishes outputs.
pub struct MicroFactory {
    descriptor: MicroDescriptor,
    activate: Rc<dyn Fn(MicroActivation) -> Result<(), AccessError>>,
}

impl MicroFactory {
    pub fn new(
        descriptor: MicroDescriptor,
        activate: impl Fn(MicroActivation) -> Result<(), AccessError> + 'static,
    ) -> Self {
        Self {
            descriptor,
            activate: Rc::new(activate),
        }
    }
}

impl MicroProvider for MicroFactory {
    fn descriptor(&self) -> MicroDescriptor {
        self.descriptor.clone()
    }

    fn activate(&self, context: MicroActivation) -> Result<(), AccessError> {
        (self.activate)(context)
    }
}

type AsyncActivateFn =
    Rc<dyn Fn(MicroActivation) -> Pin<Box<dyn Future<Output = Result<(), ActivationError>>>>>;

/// Asynchronous provider adapter whose activation runs inside [`Host::activate_async`].
/// Dropping or failing the activation future rolls back all owned effects and provisions.
pub struct AsyncMicroFactory {
    descriptor: MicroDescriptor,
    activate: AsyncActivateFn,
}

impl AsyncMicroFactory {
    pub fn new<F, Fut>(descriptor: MicroDescriptor, activate: F) -> Self
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

impl MicroProvider for AsyncMicroFactory {
    fn descriptor(&self) -> MicroDescriptor {
        self.descriptor.clone()
    }

    fn activate(&self, _context: MicroActivation) -> Result<(), AccessError> {
        Err(AccessError::Denied)
    }

    fn activate_async<'a>(
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
    owned_futures: tokio::sync::mpsc::UnboundedSender<OwnedFuture>,
    future_driver: Rc<RefCell<Option<OwnedFutures>>>,
}

impl Default for MicroRegistry {
    fn default() -> Self {
        let (owned_futures, incoming) = tokio::sync::mpsc::unbounded_channel();
        Self {
            providers: Rc::default(),
            live: provisions::Provisions::default(),
            changes: watch::channel(()).0,
            owned_futures,
            future_driver: Rc::new(RefCell::new(Some(OwnedFutures {
                incoming,
                active: Vec::new(),
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
        self.live.commit(
            self.instance.host(),
            &self.instance,
            values,
            &self.descriptor,
        )?;
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
    pub(crate) fn future_driver(&self) -> FutureDriver<'_> {
        FutureDriver::take(&self.future_driver).expect("registry futures already driven")
    }

    pub async fn drive_owned_futures(&self) {
        self.future_driver().run().await;
    }

    pub(crate) fn changes(&self) -> watch::Receiver<()> {
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

    fn stage(
        &self,
        host: &Host,
        prepared: PreparedActivation,
        instance_name: String,
    ) -> Result<(PendingActivation, Rc<dyn MicroProvider>, MicroActivation), MicroRegistryError>
    {
        let PreparedActivation {
            provider,
            descriptor,
            requirements,
        } = prepared;
        let instance = host.activate(instance_name, Ok)?;
        let pending = Arc::new(Mutex::new(Some(BTreeMap::new())));
        let activation = PendingActivation {
            instance: instance.clone(),
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
            instance,
            descriptor,
            requirements,
            pending,
            owned_futures: self.owned_futures.clone(),
        };
        Ok((activation, provider, context))
    }

    pub(super) fn begin(
        &self,
        host: &Host,
        provider: &MicroId,
        instance_name: String,
        bindings: &BTreeMap<String, String>,
    ) -> Result<PendingActivation, MicroRegistryError> {
        let prepared = self.prepare_activation(provider, bindings)?;
        let (mut activation, provider, context) = self.stage(host, prepared, instance_name)?;
        let instance = activation.instance.clone();
        activation.future = Box::pin(async move {
            tokio::select! {
                biased;
                _ = instance.closed() => Err(AccessError::Revoked.into()),
                result = async { provider.activate_async(context).await } => result,
            }
        });
        Ok(activation)
    }

    pub fn activate(
        &self,
        host: &Host,
        provider: &MicroId,
        instance_name: impl Into<String>,
        bindings: BTreeMap<String, String>,
    ) -> Result<Instance, MicroRegistryError> {
        let prepared = self.prepare_activation(provider, &bindings)?;
        let (activation, provider, context) = self.stage(host, prepared, instance_name.into())?;
        provider.activate(context)?;
        activation.commit()
    }

    pub async fn activate_async(
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

    #[test]
    fn registry_dispatches_a_provider_through_host_lifecycle() {
        let host = Host::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let provider_id = MicroId::new("dayframe.test");
        let mut registry = MicroRegistry::default();
        let host_for_provider = host.clone();
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
                    assert!(context.host() == host_for_provider);
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
            .unwrap();
        assert_eq!(other.name, "other-test");
        other.close();

        let instance = registry
            .activate(&host, &provider_id, "configured-test", BTreeMap::new())
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
            registry.activate(&host, &id, "closed", BTreeMap::new()),
            Err(MicroRegistryError::Access(AccessError::Revoked))
        ));
        assert!(matches!(
            registry
                .activate_async(&host, &id, "closed", BTreeMap::new())
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
            .register(AsyncMicroFactory::new(
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

    #[test]
    fn registry_failure_drops_queued_work_without_polling_a_driver() {
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
            registry.activate(&host, &id, "failed", BTreeMap::new()),
            Err(MicroRegistryError::Access(AccessError::Denied))
        ));
        assert!(dropped.get());
        host.close();
    }

    #[test]
    fn failed_provider_activation_rolls_back_published_effects() {
        let host = Host::new();
        let root = host.start("root");
        let provider_id = MicroId::new("dayframe.failing");
        let published = Arc::new(Mutex::new(None));
        let published_by_provider = published.clone();
        let mut registry = MicroRegistry::default();
        registry
            .register(MicroFactory::new(
                MicroDescriptor {
                    id: provider_id.clone(),
                    version: 1,
                    provisions: Vec::new(),
                    requirements: Vec::new(),
                },
                move |context| {
                    let component = context.host().publish_component(&context.instance())?;
                    *published_by_provider.lock().unwrap() = Some((context.instance(), component));
                    Err(AccessError::Denied)
                },
            ))
            .unwrap();

        assert!(matches!(
            registry.activate(&host, &provider_id, "failing", BTreeMap::new()),
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
