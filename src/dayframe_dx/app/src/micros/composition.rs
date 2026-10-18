//! Reconcile a projection of owner configuration with live micro incarnations.
//! This module does not persist configuration or grant data permissions. Those remain separate
//! host facilities. Bindings determine dependency order; registration order is irrelevant.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{MicroDescriptor, MicroId, MicroRegistry, MicroRegistryError};
use crate::{
    documents::{DocumentSnapshot, Facet},
    host::{AccessError, CloseReason, Host, Instance},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Enablement {
    Enabled,
    Disabled,
}

/// Desired boot instance. The map key is its stable configured identity; an activation creates a
/// fresh runtime identity. A binding names the supplier instance for one declared contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConfiguredMicro {
    pub provider: MicroId,
    pub enablement: Enablement,
    pub bindings: BTreeMap<String, String>,
}

/// Observable result of a reconciliation, not a promise that a declaration is a live provision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstanceState {
    Disabled,
    /// The incarnation was explicitly closed outside configuration reconciliation. It stays stopped
    /// until its configuration changes; dependency withdrawal alone does not enter this state.
    Stopped,
    UnavailableProvider(MicroId),
    Blocked {
        requirements: Vec<String>,
    },
    Active,
    Failed(MicroRegistryError),
}

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum ConfigurationError {
    #[error("missing binding for {instance}: {contract}")]
    MissingBinding { instance: String, contract: String },
    #[error("undeclared binding for {instance}: {contract}")]
    UnexpectedBinding { instance: String, contract: String },
    #[error("incompatible supplier for {instance}: {contract}")]
    IncompatibleBinding { instance: String, contract: String },
    #[error("dependency cycle prevents ordering instances {0:?}")]
    Cycle(Vec<String>),
    #[error("configured instance is locked and cannot be modified: {0}")]
    LockedInstance(String),
    #[error("unknown configured instance: {0}")]
    UnknownInstance(String),
    #[error(transparent)]
    Access(#[from] AccessError),
}

/// Atomic current composition plus change notification.
///
/// Nothing below this trait knows whether the desired composition comes from static locked-down
/// linkage, a host document facet, or an in-memory test mock.
pub trait ConfigurationSource {
    fn current(&self) -> Result<BTreeMap<String, ConfiguredMicro>, AccessError>;
    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError>;
    fn is_locked(&self, _instance: &str) -> bool {
        false
    }
}

pub struct ConfigurationWatch {
    inner: ConfigurationWatchInner,
}

struct FacetWatch {
    facet: Facet<BTreeMap<String, ConfiguredMicro>>,
    locked: BTreeMap<String, ConfiguredMicro>,
    updates: watch::Receiver<DocumentSnapshot>,
}

enum ConfigurationWatchInner {
    Static(BTreeMap<String, ConfiguredMicro>),
    Watch(watch::Receiver<BTreeMap<String, ConfiguredMicro>>),
    Facet(Box<FacetWatch>),
}

impl ConfigurationWatch {
    pub fn from_static(entries: BTreeMap<String, ConfiguredMicro>) -> Self {
        Self {
            inner: ConfigurationWatchInner::Static(entries),
        }
    }

    pub fn from_watch(receiver: watch::Receiver<BTreeMap<String, ConfiguredMicro>>) -> Self {
        Self {
            inner: ConfigurationWatchInner::Watch(receiver),
        }
    }

    pub fn current(&mut self) -> Result<BTreeMap<String, ConfiguredMicro>, AccessError> {
        match &mut self.inner {
            ConfigurationWatchInner::Static(entries) => Ok(entries.clone()),
            ConfigurationWatchInner::Watch(receiver) => Ok(receiver.borrow_and_update().clone()),
            ConfigurationWatchInner::Facet(watch) => {
                let snapshot = watch.updates.borrow_and_update();
                if snapshot.revoked {
                    return Err(AccessError::Revoked);
                }
                let mut desired = watch.facet.value(&snapshot).unwrap_or_default();
                for (name, config) in &watch.locked {
                    desired.insert(name.clone(), config.clone());
                }
                Ok(desired)
            }
        }
    }

    pub async fn changed(&mut self) -> Result<(), AccessError> {
        match &mut self.inner {
            ConfigurationWatchInner::Static(_) => std::future::pending().await,
            ConfigurationWatchInner::Watch(receiver) => {
                receiver.changed().await.map_err(|_| AccessError::Revoked)
            }
            ConfigurationWatchInner::Facet(watch) => {
                watch
                    .updates
                    .changed()
                    .await
                    .map_err(|_| AccessError::Revoked)?;
                if watch.updates.borrow().revoked {
                    return Err(AccessError::Revoked);
                }
                Ok(())
            }
        }
    }
}

/// Static configuration source for locked-down builds. Every entry is locked and immutable.
#[derive(Clone, Debug, Default)]
pub struct StaticConfiguration {
    entries: BTreeMap<String, ConfiguredMicro>,
}

impl StaticConfiguration {
    pub fn new(entries: BTreeMap<String, ConfiguredMicro>) -> Self {
        Self { entries }
    }
}

impl ConfigurationSource for StaticConfiguration {
    fn current(&self) -> Result<BTreeMap<String, ConfiguredMicro>, AccessError> {
        Ok(self.entries.clone())
    }

    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        Ok(ConfigurationWatch::from_static(self.entries.clone()))
    }

    fn is_locked(&self, instance: &str) -> bool {
        self.entries.contains_key(instance)
    }
}

/// In-memory configuration source for tests and non-document harnesses, supporting locked entries.
#[derive(Clone)]
pub struct MockConfiguration {
    locked: Arc<BTreeMap<String, ConfiguredMicro>>,
    sender: Arc<watch::Sender<BTreeMap<String, ConfiguredMicro>>>,
}

impl Default for MockConfiguration {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}

impl MockConfiguration {
    pub fn new(initial: BTreeMap<String, ConfiguredMicro>) -> Self {
        Self::with_locked(BTreeMap::new(), initial)
    }

    pub fn with_locked(
        locked: BTreeMap<String, ConfiguredMicro>,
        mut dynamic: BTreeMap<String, ConfiguredMicro>,
    ) -> Self {
        for (name, config) in &locked {
            dynamic.insert(name.clone(), config.clone());
        }
        let (sender, _) = watch::channel(dynamic);
        Self {
            locked: Arc::new(locked),
            sender: Arc::new(sender),
        }
    }

    pub fn set(
        &self,
        desired: BTreeMap<String, ConfiguredMicro>,
    ) -> Result<(), ConfigurationError> {
        for (name, locked_config) in self.locked.iter() {
            if desired.get(name) != Some(locked_config) {
                return Err(ConfigurationError::LockedInstance(name.clone()));
            }
        }
        self.sender.send_if_modified(|current| {
            if *current == desired {
                return false;
            }
            *current = desired;
            true
        });
        Ok(())
    }

    pub fn set_enablement(
        &self,
        instance: &str,
        enablement: Enablement,
    ) -> Result<(), ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let mut next = self.sender.borrow().clone();
        let entry = next
            .get_mut(instance)
            .ok_or_else(|| ConfigurationError::UnknownInstance(instance.to_owned()))?;
        entry.enablement = enablement;
        self.set(next)
    }

    pub fn rebind(
        &self,
        instance: &str,
        bindings: BTreeMap<String, String>,
    ) -> Result<(), ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let mut next = self.sender.borrow().clone();
        let entry = next
            .get_mut(instance)
            .ok_or_else(|| ConfigurationError::UnknownInstance(instance.to_owned()))?;
        entry.bindings = bindings;
        self.set(next)
    }
}

impl ConfigurationSource for MockConfiguration {
    fn current(&self) -> Result<BTreeMap<String, ConfiguredMicro>, AccessError> {
        Ok(self.sender.borrow().clone())
    }

    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        Ok(ConfigurationWatch::from_watch(self.sender.subscribe()))
    }

    fn is_locked(&self, instance: &str) -> bool {
        self.locked.contains_key(instance)
    }
}

/// Facet-backed configuration source for malleable apps. Locked boot entries cannot be disabled,
/// rebound, or removed by local or external facet mutations.
#[derive(Clone)]
pub struct FacetConfiguration {
    facet: Facet<BTreeMap<String, ConfiguredMicro>>,
    locked: BTreeMap<String, ConfiguredMicro>,
}

impl FacetConfiguration {
    pub fn new(
        facet: Facet<BTreeMap<String, ConfiguredMicro>>,
        locked: BTreeMap<String, ConfiguredMicro>,
    ) -> Self {
        Self { facet, locked }
    }

    pub fn facet(&self) -> &Facet<BTreeMap<String, ConfiguredMicro>> {
        &self.facet
    }

    pub fn set_enablement(
        &self,
        instance: &str,
        enablement: Enablement,
    ) -> Result<u64, ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let mut next = self.current()?;
        let entry = next
            .get_mut(instance)
            .ok_or_else(|| ConfigurationError::UnknownInstance(instance.to_owned()))?;
        entry.enablement = enablement;
        for locked_key in self.locked.keys() {
            next.remove(locked_key);
        }
        Ok(self.facet.write(next)?)
    }

    pub fn rebind(
        &self,
        instance: &str,
        bindings: BTreeMap<String, String>,
    ) -> Result<u64, ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let mut next = self.current()?;
        let entry = next
            .get_mut(instance)
            .ok_or_else(|| ConfigurationError::UnknownInstance(instance.to_owned()))?;
        entry.bindings = bindings;
        for locked_key in self.locked.keys() {
            next.remove(locked_key);
        }
        Ok(self.facet.write(next)?)
    }
}

impl ConfigurationSource for FacetConfiguration {
    fn current(&self) -> Result<BTreeMap<String, ConfiguredMicro>, AccessError> {
        let snapshot = self.facet.watch.current()?;
        if snapshot.revoked {
            return Err(AccessError::Revoked);
        }
        let mut desired = self.facet.value(&snapshot).unwrap_or_default();
        for (name, config) in &self.locked {
            desired.insert(name.clone(), config.clone());
        }
        Ok(desired)
    }

    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        let updates = self.facet.watch.subscribe()?;
        Ok(ConfigurationWatch {
            inner: ConfigurationWatchInner::Facet(Box::new(FacetWatch {
                facet: self.facet.clone(),
                locked: self.locked.clone(),
                updates,
            })),
        })
    }

    fn is_locked(&self, instance: &str) -> bool {
        self.locked.contains_key(instance)
    }
}

struct Active {
    configuration: ConfiguredMicro,
    instance: Instance,
}

struct Failed {
    configuration: ConfiguredMicro,
    dependencies: Vec<Instance>,
    error: MicroRegistryError,
}

#[derive(Default)]
pub(crate) struct Composition {
    active: BTreeMap<String, Active>,
    failed: BTreeMap<String, Failed>,
    stopped: BTreeMap<String, ConfiguredMicro>,
    order: Vec<String>,
}

impl Composition {
    pub async fn reconcile(
        &mut self,
        host: &Host,
        registry: &MicroRegistry,
        desired: &BTreeMap<String, ConfiguredMicro>,
    ) -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
        let definitions: BTreeMap<_, _> = registry
            .descriptors()
            .map(|descriptor| (descriptor.id.clone(), descriptor))
            .collect();
        // Validate before withdrawing anything: a malformed edit must not destroy a working graph.
        let order = activation_order(&definitions, desired)?;
        for name in self.order.iter().rev() {
            let active = self.active.get(name).unwrap();
            if desired.get(name) != Some(&active.configuration) {
                active.instance.close();
            }
        }
        for (name, active) in &self.active {
            if active.instance.close_reason() == Some(CloseReason::Requested)
                && desired.get(name) == Some(&active.configuration)
            {
                // Do not turn a self-stop or administrative close into an automatic restart loop.
                self.stopped
                    .insert(name.clone(), active.configuration.clone());
            }
        }
        self.stopped
            .retain(|name, configuration| desired.get(name) == Some(configuration));
        self.active
            .retain(|_, active| active.instance.check().is_ok());
        self.failed
            .retain(|name, failed| desired.get(name) == Some(&failed.configuration));
        let mut states: BTreeMap<_, _> = desired
            .iter()
            .filter(|(_, configuration)| configuration.enablement == Enablement::Disabled)
            .map(|(name, _)| (name.clone(), InstanceState::Disabled))
            .collect();
        for name in &order {
            let configuration = &desired[name];
            if !definitions.contains_key(&configuration.provider) {
                states.insert(
                    name.clone(),
                    InstanceState::UnavailableProvider(configuration.provider.clone()),
                );
                continue;
            }
            if self.stopped.contains_key(name) {
                states.insert(name.clone(), InstanceState::Stopped);
                continue;
            }
            let missing: Vec<_> = configuration
                .bindings
                .iter()
                .filter(|(_, supplier)| !self.active.contains_key(*supplier))
                .map(|(contract, _)| contract.clone())
                .collect();
            if !missing.is_empty() {
                states.insert(
                    name.clone(),
                    InstanceState::Blocked {
                        requirements: missing,
                    },
                );
                continue;
            }
            if self.active.contains_key(name) {
                states.insert(name.clone(), InstanceState::Active);
                continue;
            }
            let dependencies: Vec<_> = configuration
                .bindings
                .values()
                .map(|supplier| self.active[supplier].instance.clone())
                .collect();
            // Failure is retained until its inputs change. Unrelated configuration edits do not
            // repeatedly run a failing activation; disable/re-enable explicitly permits another try.
            if let Some(failed) = self.failed.get(name)
                && failed.dependencies == dependencies
            {
                states.insert(name.clone(), InstanceState::Failed(failed.error.clone()));
                continue;
            }
            match registry
                .activate_async(
                    host,
                    &configuration.provider,
                    name.clone(),
                    configuration.bindings.clone(),
                )
                .await
            {
                Ok(instance) => {
                    self.failed.remove(name);
                    assert!(
                        self.active
                            .insert(
                                name.clone(),
                                Active {
                                    configuration: configuration.clone(),
                                    instance,
                                }
                            )
                            .is_none()
                    );
                    states.insert(name.clone(), InstanceState::Active);
                }
                Err(error) => {
                    states.insert(name.clone(), InstanceState::Failed(error.clone()));
                    self.failed.insert(
                        name.clone(),
                        Failed {
                            configuration: configuration.clone(),
                            dependencies,
                            error,
                        },
                    );
                }
            }
        }
        self.order = order
            .into_iter()
            .filter(|name| self.active.contains_key(name))
            .collect();
        Ok(states)
    }
}

fn activation_order(
    definitions: &BTreeMap<MicroId, MicroDescriptor>,
    desired: &BTreeMap<String, ConfiguredMicro>,
) -> Result<Vec<String>, ConfigurationError> {
    let mut dependencies = BTreeMap::new();
    for (name, configuration) in desired {
        if configuration.enablement == Enablement::Disabled {
            continue;
        }
        let mut required = BTreeSet::new();
        if let Some(definition) = definitions.get(&configuration.provider) {
            for contract in configuration.bindings.keys() {
                if !definition
                    .requirements
                    .iter()
                    .any(|requirement| &requirement.id == contract)
                {
                    return Err(ConfigurationError::UnexpectedBinding {
                        instance: name.clone(),
                        contract: contract.clone(),
                    });
                }
            }
            for requirement in &definition.requirements {
                let supplier_name =
                    configuration.bindings.get(&requirement.id).ok_or_else(|| {
                        ConfigurationError::MissingBinding {
                            instance: name.clone(),
                            contract: requirement.id.clone(),
                        }
                    })?;
                if let Some(supplier) = desired.get(supplier_name) {
                    if let Some(supplier_definition) = definitions.get(&supplier.provider)
                        && !supplier_definition.provisions.iter().any(|provision| {
                            provision.id == requirement.id
                                && provision.version == requirement.version
                        })
                    {
                        return Err(ConfigurationError::IncompatibleBinding {
                            instance: name.clone(),
                            contract: requirement.id.clone(),
                        });
                    }
                    if supplier.enablement == Enablement::Enabled {
                        required.insert(supplier_name.clone());
                    }
                }
            }
        }
        dependencies.insert(name.clone(), required);
    }
    let mut order = Vec::with_capacity(dependencies.len());
    while !dependencies.is_empty() {
        let ready: Vec<_> = dependencies
            .iter()
            .filter(|(_, required)| required.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        if ready.is_empty() {
            return Err(ConfigurationError::Cycle(
                dependencies.into_keys().collect(),
            ));
        }
        for name in ready {
            dependencies.remove(&name).unwrap();
            for required in dependencies.values_mut() {
                required.remove(&name);
            }
            order.push(name);
        }
    }
    Ok(order)
}

#[cfg(test)]
mod tests;
