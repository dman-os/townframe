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
use clay_data::{DocumentSnapshot, Facet, Operation};
use clay_host::{AccessError, CloseReason, Host, Instance};

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
    Activating,
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
    #[error("configuration is not hydrated")]
    Loading,
    #[error("configuration request was superseded")]
    Superseded,
    #[error(transparent)]
    Access(#[from] AccessError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationSnapshot {
    Loading,
    Ready(BTreeMap<String, ConfiguredMicro>),
}

/// Atomic current composition plus change notification.
///
/// Nothing below this trait knows whether the desired composition comes from static locked-down
/// linkage, a host document facet, or an in-memory test mock.
pub trait ConfigurationSource {
    fn current(&self) -> Result<ConfigurationSnapshot, AccessError>;
    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError>;
    fn fixed(&self) -> Option<&BTreeMap<String, ConfiguredMicro>> {
        None
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

    pub fn current(&mut self) -> Result<ConfigurationSnapshot, AccessError> {
        match &mut self.inner {
            ConfigurationWatchInner::Static(entries) => {
                Ok(ConfigurationSnapshot::Ready(entries.clone()))
            }
            ConfigurationWatchInner::Watch(receiver) => Ok(ConfigurationSnapshot::Ready(
                receiver.borrow_and_update().clone(),
            )),
            ConfigurationWatchInner::Facet(watch) => {
                let snapshot = watch.updates.borrow_and_update();
                facet_configuration(&watch.facet, &watch.locked, &snapshot)
            }
        }
    }

    pub fn has_changed(&self) -> Result<bool, AccessError> {
        match &self.inner {
            ConfigurationWatchInner::Static(_) => Ok(false),
            ConfigurationWatchInner::Watch(receiver) => {
                receiver.has_changed().map_err(|_| AccessError::Revoked)
            }
            ConfigurationWatchInner::Facet(watch) => watch
                .updates
                .has_changed()
                .map_err(|_| AccessError::Revoked),
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
    fn current(&self) -> Result<ConfigurationSnapshot, AccessError> {
        Ok(ConfigurationSnapshot::Ready(self.entries.clone()))
    }

    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        Ok(ConfigurationWatch::from_static(self.entries.clone()))
    }

    fn fixed(&self) -> Option<&BTreeMap<String, ConfiguredMicro>> {
        Some(&self.entries)
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
        let mut result = Ok(());
        self.sender.send_if_modified(|current| {
            let Some(entry) = current.get_mut(instance) else {
                result = Err(ConfigurationError::UnknownInstance(instance.to_owned()));
                return false;
            };
            if entry.enablement == enablement {
                return false;
            }
            entry.enablement = enablement;
            true
        });
        result
    }

    pub fn rebind(
        &self,
        instance: &str,
        bindings: BTreeMap<String, String>,
    ) -> Result<(), ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let mut result = Ok(());
        self.sender.send_if_modified(|current| {
            let Some(entry) = current.get_mut(instance) else {
                result = Err(ConfigurationError::UnknownInstance(instance.to_owned()));
                return false;
            };
            if entry.bindings == bindings {
                return false;
            }
            entry.bindings = bindings;
            true
        });
        result
    }
}

impl ConfigurationSource for MockConfiguration {
    fn current(&self) -> Result<ConfigurationSnapshot, AccessError> {
        Ok(ConfigurationSnapshot::Ready(self.sender.borrow().clone()))
    }

    fn subscribe(&self) -> Result<ConfigurationWatch, AccessError> {
        Ok(ConfigurationWatch::from_watch(self.sender.subscribe()))
    }

    fn fixed(&self) -> Option<&BTreeMap<String, ConfiguredMicro>> {
        Some(&self.locked)
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
    ) -> Result<Operation, ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let ConfigurationSnapshot::Ready(mut next) = self.current()? else {
            return Err(ConfigurationError::Loading);
        };
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
    ) -> Result<Operation, ConfigurationError> {
        if self.locked.contains_key(instance) {
            return Err(ConfigurationError::LockedInstance(instance.to_owned()));
        }
        let ConfigurationSnapshot::Ready(mut next) = self.current()? else {
            return Err(ConfigurationError::Loading);
        };
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
    fn current(&self) -> Result<ConfigurationSnapshot, AccessError> {
        facet_configuration(&self.facet, &self.locked, &self.facet.watch.current()?)
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

    fn fixed(&self) -> Option<&BTreeMap<String, ConfiguredMicro>> {
        Some(&self.locked)
    }
}

fn facet_configuration(
    facet: &Facet<BTreeMap<String, ConfiguredMicro>>,
    locked: &BTreeMap<String, ConfiguredMicro>,
    snapshot: &DocumentSnapshot,
) -> Result<ConfigurationSnapshot, AccessError> {
    if snapshot.revoked {
        return Err(AccessError::Revoked);
    }
    if snapshot
        .facets
        .values()
        .any(|value| matches!(value, clay_data::FacetValue::Loading))
    {
        return Ok(ConfigurationSnapshot::Loading);
    }
    let mut desired = facet.value(snapshot).unwrap_or_default();
    for (name, config) in locked {
        desired.insert(name.clone(), config.clone());
    }
    Ok(ConfigurationSnapshot::Ready(desired))
}

struct Active {
    configuration: ConfiguredMicro,
    host: Host,
    instance: Instance,
    ended: watch::Receiver<Option<CloseReason>>,
}

struct Failed {
    configuration: ConfiguredMicro,
    dependencies: Vec<Instance>,
    error: MicroRegistryError,
}

struct Pending {
    configuration: ConfiguredMicro,
    dependencies: Vec<Instance>,
    activation: super::PendingActivation,
    result: Option<Result<(), super::ActivationError>>,
}

struct Closing {
    stop_if_unchanged: Option<ConfiguredMicro>,
    ended: watch::Receiver<Option<CloseReason>>,
    closed: super::LocalFuture,
}

#[derive(Default)]
pub struct Composition {
    active: BTreeMap<String, Active>,
    failed: BTreeMap<String, Failed>,
    stopped: BTreeMap<String, ConfiguredMicro>,
    pending: BTreeMap<String, Pending>,
    closing: BTreeMap<String, Closing>,
}

impl Composition {
    pub fn validate(
        registry: &MicroRegistry,
        desired: &BTreeMap<String, ConfiguredMicro>,
    ) -> Result<(), ConfigurationError> {
        let definitions = registry
            .descriptors()
            .map(|descriptor| (descriptor.id.clone(), descriptor))
            .collect();
        activation_order(&definitions, desired).map(|_| ())
    }

    fn retire(
        &mut self,
        name: String,
        stop_if_unchanged: Option<ConfiguredMicro>,
        ended: watch::Receiver<Option<CloseReason>>,
    ) {
        let mut observer = ended.clone();
        assert!(
            self.closing
                .insert(
                    name,
                    Closing {
                        stop_if_unchanged,
                        ended,
                        closed: Box::pin(async move {
                            observer.wait_for(Option::is_some).await.unwrap();
                        }),
                    }
                )
                .is_none()
        );
    }

    pub fn cancel_pending(&mut self) {
        for (name, pending) in std::mem::take(&mut self.pending) {
            self.retire(name, None, pending.activation.ended.clone());
            drop(pending);
        }
    }

    pub fn is_settled(&self) -> bool {
        self.pending.is_empty() && self.closing.is_empty()
    }

    pub fn update(
        &mut self,
        host: &Host,
        registry: &MicroRegistry,
        desired: &BTreeMap<String, ConfiguredMicro>,
    ) -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
        let definitions: BTreeMap<_, _> = registry
            .descriptors()
            .map(|descriptor| (descriptor.id.clone(), descriptor))
            .collect();
        let order = activation_order(&definitions, desired)?;
        let retiring = self
            .active
            .extract_if(.., |name, active| {
                desired.get(name) != Some(&active.configuration)
                    || active.host.check(&active.instance).is_err()
            })
            .collect::<Vec<_>>();
        for (name, active) in retiring {
            let changed = desired.get(&name) != Some(&active.configuration);
            self.retire(
                name,
                (!changed).then_some(active.configuration),
                active.ended,
            );
            if changed {
                active.host.close_instance(&active.instance).unwrap();
            }
        }
        let cancelled = self
            .pending
            .extract_if(.., |name, pending| {
                desired.get(name) != Some(&pending.configuration)
            })
            .collect::<Vec<_>>();
        for (name, pending) in cancelled {
            self.retire(name, None, pending.activation.ended.clone());
            drop(pending);
        }
        self.closing.retain(|name, closing| {
            let reason = *closing.ended.borrow();
            if reason == Some(CloseReason::Requested)
                && let Some(configuration) = &closing.stop_if_unchanged
                && desired.get(name) == Some(configuration)
            {
                self.stopped.insert(name.clone(), configuration.clone());
            }
            reason.is_none()
        });
        let mut retain = |name: &String,
                          configuration: &ConfiguredMicro,
                          ended: &watch::Receiver<Option<CloseReason>>| {
            let reason = *ended.borrow();
            if reason == Some(CloseReason::Requested) && desired.get(name) == Some(configuration) {
                self.stopped.insert(name.clone(), configuration.clone());
            }
            reason.is_none()
        };
        self.active
            .retain(|name, active| retain(name, &active.configuration, &active.ended));
        self.pending.retain(|name, pending| {
            retain(name, &pending.configuration, &pending.activation.ended)
        });
        self.stopped
            .retain(|name, configuration| desired.get(name) == Some(configuration));
        self.failed
            .retain(|name, failed| desired.get(name) == Some(&failed.configuration));
        let mut states: BTreeMap<_, _> = desired
            .iter()
            .filter(|(_, config)| config.enablement == Enablement::Disabled)
            .map(|(name, _)| (name.clone(), InstanceState::Disabled))
            .collect();
        for name in order {
            let configuration = &desired[&name];
            let state = if !definitions.contains_key(&configuration.provider) {
                InstanceState::UnavailableProvider(configuration.provider.clone())
            } else if self.stopped.contains_key(&name) {
                InstanceState::Stopped
            } else if self.active.contains_key(&name) {
                InstanceState::Active
            } else {
                let missing: Vec<_> = configuration
                    .bindings
                    .iter()
                    .filter(|(_, supplier)| !self.active.contains_key(*supplier))
                    .map(|(contract, _)| contract.clone())
                    .collect();
                if !missing.is_empty() {
                    InstanceState::Blocked {
                        requirements: missing,
                    }
                } else if self.pending.contains_key(&name) {
                    InstanceState::Activating
                } else {
                    let dependencies: Vec<_> = configuration
                        .bindings
                        .values()
                        .map(|supplier| self.active[supplier].instance.clone())
                        .collect();
                    if let Some(failed) = self.failed.get(&name)
                        && failed.dependencies == dependencies
                    {
                        InstanceState::Failed(failed.error.clone())
                    } else if self.closing.contains_key(&name) {
                        InstanceState::Activating
                    } else {
                        match registry.begin(
                            host,
                            &configuration.provider,
                            name.clone(),
                            &configuration.bindings,
                        ) {
                            Ok(activation) => {
                                assert!(
                                    self.pending
                                        .insert(
                                            name.clone(),
                                            Pending {
                                                configuration: configuration.clone(),
                                                dependencies,
                                                activation,
                                                result: None
                                            }
                                        )
                                        .is_none()
                                );
                                InstanceState::Activating
                            }
                            Err(error) => {
                                self.failed.insert(
                                    name.clone(),
                                    Failed {
                                        configuration: configuration.clone(),
                                        dependencies,
                                        error: error.clone(),
                                    },
                                );
                                InstanceState::Failed(error)
                            }
                        }
                    }
                }
            };
            states.insert(name, state);
        }
        Ok(states)
    }

    pub async fn next(&mut self) -> String {
        std::future::poll_fn(|context| {
            for (name, closing) in &mut self.closing {
                let ended = closing.ended.borrow().is_some();
                if ended || closing.closed.as_mut().poll(context).is_ready() {
                    return std::task::Poll::Ready(name.clone());
                }
            }
            for (name, pending) in &mut self.pending {
                if pending.result.is_none()
                    && let std::task::Poll::Ready(result) = pending.activation.poll(context)
                {
                    pending.result = Some(result);
                }
                let ended = pending.activation.ended.borrow().is_some();
                if pending.result.is_some()
                    && (ended
                        || pending
                            .activation
                            .host
                            .check(&pending.activation.instance)
                            .is_ok()
                        || pending.activation.closed.as_mut().poll(context).is_ready())
                {
                    return std::task::Poll::Ready(name.clone());
                }
            }
            std::task::Poll::Pending
        })
        .await
    }

    pub fn complete(&mut self, name: String, desired: &BTreeMap<String, ConfiguredMicro>) {
        if let Some(closing) = self.closing.remove(&name) {
            let reason = closing
                .ended
                .borrow()
                .expect("closure completed without a terminal reason");
            if reason == CloseReason::Requested
                && let Some(configuration) = closing.stop_if_unchanged
                && desired.get(&name) == Some(&configuration)
            {
                self.stopped.insert(name, configuration);
            }
            return;
        }
        let mut pending = self.pending.remove(&name).unwrap();
        let result = pending.result.take().unwrap();
        if desired.get(&name) != Some(&pending.configuration) {
            self.retire(name, None, pending.activation.ended.clone());
            return;
        }
        let reason = *pending.activation.ended.borrow();
        if let Some(reason) = reason {
            if reason == CloseReason::Requested {
                self.stopped.insert(name, pending.configuration);
            }
            return;
        }
        if pending
            .activation
            .host
            .check(&pending.activation.instance)
            .is_err()
        {
            pending.result = Some(result);
            assert!(self.pending.insert(name, pending).is_none());
            return;
        }
        let dependencies: Option<Vec<_>> = pending
            .configuration
            .bindings
            .values()
            .map(|supplier| {
                self.active
                    .get(supplier)
                    .map(|active| active.instance.clone())
            })
            .collect();
        if dependencies.as_ref() != Some(&pending.dependencies)
            || pending
                .dependencies
                .iter()
                .any(|instance| pending.activation.host.check(instance).is_err())
        {
            self.retire(name, None, pending.activation.ended.clone());
            return;
        }
        let host = pending.activation.host.clone();
        let ended = pending.activation.ended.clone();
        match result
            .map_err(MicroRegistryError::from)
            .and_then(|()| pending.activation.commit())
        {
            Ok(instance) => {
                self.failed.remove(&name);
                assert!(
                    self.active
                        .insert(
                            name,
                            Active {
                                configuration: pending.configuration,
                                host,
                                ended,
                                instance
                            }
                        )
                        .is_none()
                );
            }
            Err(MicroRegistryError::Access(AccessError::Revoked))
                if pending
                    .activation
                    .host
                    .check(&pending.activation.instance)
                    .is_err() =>
            {
                pending.result = Some(Err(AccessError::Revoked.into()));
                assert!(self.pending.insert(name, pending).is_none());
            }
            Err(error) => {
                self.retire(name.clone(), None, ended);
                self.failed.insert(
                    name,
                    Failed {
                        configuration: pending.configuration,
                        dependencies: pending.dependencies,
                        error,
                    },
                );
            }
        }
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
