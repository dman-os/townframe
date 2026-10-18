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

type Recovery = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// An explicit owner of transient effects, with asynchronous, reverse-order recovery.
///
/// Cloning shares ownership; dropping a handle does not dispose the scope. The lifecycle owner
/// must await [`Self::close`] before destroying facilities its effects need for recovery. Once
/// closure starts, it continues even if the requesting future is cancelled.
#[derive(Clone)]
pub struct Scope {
    inner: Arc<ScopeInner>,
}

struct ScopeInner {
    effects: Mutex<Option<Vec<Recovery>>>,
    completed: watch::Sender<bool>,
}

/// An effect was not installed because its owner has begun closing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("micro scope is closing")]
pub struct ScopeClosed;

impl Default for Scope {
    fn default() -> Self {
        Self::new()
    }
}

impl Scope {
    /// Creates an explicitly owned root lifetime. This operation confers no capabilities.
    pub fn new() -> Self {
        let (completed, _) = watch::channel(false);
        Self {
            inner: Arc::new(ScopeInner {
                effects: Mutex::new(Some(Vec::new())),
                completed,
            }),
        }
    }

    /// Installs an effect and adopts its recovery in one critical section.
    ///
    /// If closing has begun, `install` is not called. Installation must be synchronous,
    /// non-reentrant, and short; it must not call this scope or await external work. The returned
    /// future performs recovery only when polled during closure. An installation panic is an
    /// invariant failure, not recoverable partial activation. Fallible/async activation needs a
    /// separate protocol above this primitive.
    pub fn effect<F, R>(&self, install: F) -> Result<(), ScopeClosed>
    where
        F: FnOnce() -> R,
        R: Future<Output = ()> + Send + 'static,
    {
        let mut effects = self.inner.effects.lock().unwrap();
        let effects = effects.as_mut().ok_or(ScopeClosed)?;
        effects.push(Box::pin(install()));
        Ok(())
    }

    /// Creates an owned child. Parent closure awaits child recovery at this position in its
    /// reverse-order recovery sequence. A child may also close early without affecting siblings.
    pub fn child(&self) -> Result<Self, ScopeClosed> {
        let child = Self::new();
        let recovery = child.clone();
        self.effect(|| async move { recovery.close().await })?;
        Ok(child)
    }

    /// Begins closure once and waits for all recoveries, including children, to finish.
    ///
    /// Calls may overlap. Recovery runs in an owned task so cancelling one waiter cannot strand
    /// a half-closed scope. Recovery failure is a programming invariant: a panic propagates from
    /// the task rather than being logged or swallowed. Provider/dependent ordering is not inferred
    /// here; the future dependency runtime must close consumers before destroying providers.
    pub async fn close(&self) {
        let mut completed = self.inner.completed.subscribe();
        {
            let mut effects = self.inner.effects.lock().unwrap();
            if let Some(effects) = effects.take() {
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    for recovery in effects.into_iter().rev() {
                        recovery.await;
                    }
                    inner.completed.send_replace(true);
                });
            }
        }
        completed.wait_for(|done| *done).await.unwrap();
    }
}
/// Stable provider identity used by configuration and composition persistence.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
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
    bindings: BTreeMap<String, String>,
    live: provisions::Provisions,
    pending: Arc<Mutex<Option<PendingProvisions>>>,
}

impl MicroActivation {
    pub fn host(&self) -> Host {
        self.host.clone()
    }
    pub fn instance(&self) -> Instance {
        self.instance.clone()
    }

    pub fn require<T: Provision>(&self) -> Result<T, AccessError> {
        assert!(
            self.descriptor
                .requirements
                .iter()
                .any(|requirement| requirement.id == T::ID && requirement.version == T::VERSION),
            "adapter requested an undeclared requirement"
        );
        let supplier = self.bindings.get(T::ID).ok_or(AccessError::Denied)?;
        self.live.get::<T>(supplier)
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
    ) -> Pin<Box<dyn Future<Output = Result<(), AccessError>> + 'a>> {
        Box::pin(async move { self.activate(context) })
    }
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
    Rc<dyn Fn(MicroActivation) -> Pin<Box<dyn Future<Output = Result<(), AccessError>>>>>;

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
        Fut: Future<Output = Result<(), AccessError>> + 'static,
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
    ) -> Pin<Box<dyn Future<Output = Result<(), AccessError>> + 'a>> {
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
    #[error(transparent)]
    Access(#[from] AccessError),
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
}

impl Default for MicroRegistry {
    fn default() -> Self {
        Self {
            providers: Rc::default(),
            live: provisions::Provisions::default(),
            changes: watch::channel(()).0,
        }
    }
}

impl Clone for MicroRegistry {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            live: self.live.clone(),
            changes: self.changes.clone(),
        }
    }
}

struct PreparedActivation {
    provider: Rc<dyn MicroProvider>,
    descriptor: MicroDescriptor,
    dependencies: Vec<Instance>,
}

fn unpublished<'a>(descriptor: &'a MicroDescriptor, values: &PendingProvisions) -> Option<&'a str> {
    descriptor
        .provisions
        .iter()
        .find(|provision| !values.contains_key(&provision.id))
        .map(|provision| provision.id.as_str())
}

impl MicroRegistry {
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
        let mut dependencies = Vec::new();
        for requirement in &descriptor.requirements {
            let supplier = bindings
                .get(&requirement.id)
                .ok_or_else(|| MicroRegistryError::InvalidBinding(requirement.id.clone()))?;
            let (owner, version) = self.live.provider(supplier, &requirement.id)?;
            if version != requirement.version {
                return Err(MicroRegistryError::InvalidBinding(requirement.id.clone()));
            }
            dependencies.push(owner);
        }
        Ok(PreparedActivation {
            provider: provider_impl,
            descriptor,
            dependencies,
        })
    }

    pub fn activate(
        &self,
        host: &Host,
        provider: &MicroId,
        instance_name: impl Into<String>,
        bindings: BTreeMap<String, String>,
    ) -> Result<Instance, MicroRegistryError> {
        let PreparedActivation {
            provider: provider_impl,
            descriptor,
            dependencies,
        } = self.prepare_activation(provider, &bindings)?;
        let activation_host = host.clone();
        let live = self.live.clone();
        let missing = Arc::new(Mutex::new(None));
        let missing_for_activation = missing.clone();
        let activated = host.activate(instance_name, move |instance| {
            for supplier in &dependencies {
                activation_host.bind_dependency(&instance, supplier);
            }
            let pending = Arc::new(Mutex::new(Some(BTreeMap::new())));
            provider_impl.activate(MicroActivation {
                host: activation_host.clone(),
                instance: instance.clone(),
                descriptor: descriptor.clone(),
                bindings,
                live: live.clone(),
                pending: pending.clone(),
            })?;
            let values = pending.lock().unwrap().take().unwrap();
            if let Some(id) = unpublished(&descriptor, &values) {
                *missing_for_activation.lock().unwrap() = Some(id.to_owned());
                return Err(AccessError::Denied);
            }
            live.commit(&activation_host, &instance, values, &descriptor)?;
            Ok(instance)
        });
        match activated {
            Ok(instance) => Ok(instance),
            Err(error) => Err(missing.lock().unwrap().take().map_or_else(
                || MicroRegistryError::Access(error),
                MicroRegistryError::MissingProvision,
            )),
        }
    }

    pub async fn activate_async(
        &self,
        host: &Host,
        provider: &MicroId,
        instance_name: impl Into<String>,
        bindings: BTreeMap<String, String>,
    ) -> Result<Instance, MicroRegistryError> {
        let PreparedActivation {
            provider: provider_impl,
            descriptor,
            dependencies,
        } = self.prepare_activation(provider, &bindings)?;
        let activation_host = host.clone();
        let live = self.live.clone();
        let missing = Arc::new(Mutex::new(None));
        let missing_for_activation = missing.clone();
        let activated = host
            .activate_async(instance_name, move |instance| async move {
                for supplier in &dependencies {
                    activation_host.bind_dependency(&instance, supplier);
                }
                let pending = Arc::new(Mutex::new(Some(BTreeMap::new())));
                provider_impl
                    .activate_async(MicroActivation {
                        host: activation_host.clone(),
                        instance: instance.clone(),
                        descriptor: descriptor.clone(),
                        bindings,
                        live: live.clone(),
                        pending: pending.clone(),
                    })
                    .await?;
                let values = pending.lock().unwrap().take().unwrap();
                if let Some(id) = unpublished(&descriptor, &values) {
                    *missing_for_activation.lock().unwrap() = Some(id.to_owned());
                    return Err(AccessError::Denied);
                }
                live.commit(&activation_host, &instance, values, &descriptor)?;
                Ok(instance)
            })
            .await;
        match activated {
            Ok(instance) => Ok(instance),
            Err(error) => Err(missing.lock().unwrap().take().map_or_else(
                || MicroRegistryError::Access(error),
                MicroRegistryError::MissingProvision,
            )),
        }
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
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn children_recover_in_reverse_order_without_affecting_siblings() {
        let root = Scope::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let first = root.child().unwrap();
        let second = root.child().unwrap();
        for (scope, label) in [(&first, "first"), (&second, "second")] {
            let log = log.clone();
            scope
                .effect(|| async move { log.lock().unwrap().push(label) })
                .unwrap();
        }
        second.close().await;
        assert_eq!(*log.lock().unwrap(), vec!["second"]);
        first.effect(|| async {}).unwrap();
        root.close().await;
        assert_eq!(*log.lock().unwrap(), vec!["second", "first"]);
        root.close().await;
        assert_eq!(*log.lock().unwrap(), vec!["second", "first"]);
    }

    #[tokio::test]
    async fn cancelling_close_waiter_does_not_cancel_recovery() {
        let scope = Scope::new();
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        scope
            .effect(|| async move {
                started_tx.send(()).unwrap();
                finish_rx.await.unwrap();
            })
            .unwrap();
        let waiter_scope = scope.clone();
        let waiter = tokio::spawn(async move { waiter_scope.close().await });
        started_rx.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        // Rejected installations must not execute their side effects.
        assert_eq!(
            scope.effect(|| -> std::future::Ready<()> { panic!("must not install") }),
            Err(ScopeClosed)
        );
        assert!(matches!(scope.child(), Err(ScopeClosed)));
        finish_tx.send(()).unwrap();
        let ((), ()) = tokio::join!(scope.close(), scope.close());
    }

    #[tokio::test]
    async fn recovery_is_lifo_and_effect_installation_is_immediate() {
        let scope = Scope::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        for label in ["one", "two"] {
            let log = log.clone();
            scope
                .effect(|| {
                    log.lock().unwrap().push(("install", label));
                    async move {
                        log.lock().unwrap().push(("recover", label));
                    }
                })
                .unwrap();
        }
        assert_eq!(
            *log.lock().unwrap(),
            vec![("install", "one"), ("install", "two")]
        );
        scope.close().await;
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                ("install", "one"),
                ("install", "two"),
                ("recover", "two"),
                ("recover", "one"),
            ]
        );
    }
}
