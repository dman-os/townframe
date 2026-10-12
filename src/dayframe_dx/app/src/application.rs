//! Host-backed application runtime. Platform composition supplies configured micros.
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

use crate::{
    host::{CollectionReader, Host, HostStatus, HostedCollection, Instance, RootMount},
    micros::composition::{Composition, ConfigurationError, ConfiguredMicro, InstanceState},
    micros::{MicroDescriptor, MicroProvider, MicroRegistry, MicroRegistryError},
    native::NativeComponents,
};

/// The platform-neutral composition boundary for a Dayframe application.
///
/// It owns host facilities and a provider registry, not any concrete micro implementation.
/// Configured activations bind declared requirements to named live providers.
#[derive(Clone)]
pub struct ApplicationRuntime {
    host: Host,
    root: Instance,
    roots: CollectionReader<RootMount>,
    native: NativeComponents,
    root_mounts: HostedCollection<RootMount>,
    providers: MicroRegistry,
    composition: Rc<RefCell<Composition>>,
    desired: tokio::sync::watch::Sender<BTreeMap<String, ConfiguredMicro>>,
    reports:
        tokio::sync::watch::Sender<Result<BTreeMap<String, InstanceState>, ConfigurationError>>,
    observing: Rc<Cell<bool>>,
}

impl Default for ApplicationRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationRuntime {
    pub fn new() -> Self {
        let host = Host::new();
        let native = NativeComponents::new(&host);
        let root = host.start("desktop-root");
        let root_mounts = HostedCollection::<RootMount>::new(&host, &root);

        Self {
            roots: root_mounts.reader(&root),
            host,
            root,
            native,
            root_mounts,
            providers: MicroRegistry::default(),
            composition: Rc::default(),
            desired: tokio::sync::watch::channel(BTreeMap::new()).0,
            reports: tokio::sync::watch::channel(Ok(BTreeMap::new())).0,
            observing: Rc::default(),
        }
    }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    pub fn root(&self) -> Instance {
        self.root.clone()
    }

    pub fn roots(&self) -> CollectionReader<RootMount> {
        self.roots.clone()
    }

    pub fn native(&self) -> NativeComponents {
        self.native.clone()
    }

    pub fn root_mounts(&self) -> HostedCollection<RootMount> {
        self.root_mounts.clone()
    }

    pub fn output<T: crate::micros::Provision>(
        &self,
        instance: &str,
    ) -> Result<T, crate::host::AccessError> {
        self.providers.output(instance)
    }

    pub fn register_provider(
        &mut self,
        provider: impl MicroProvider + 'static,
    ) -> Result<(), MicroRegistryError> {
        self.providers.register(provider)
    }

    pub fn provider_descriptors(&self) -> Vec<MicroDescriptor> {
        self.providers.descriptors().collect()
    }

    /// Apply a desired boot configuration projection. Malformed edits leave the live graph intact;
    /// absent implementations/dependencies and activation failures are explicit per-instance states.
    pub fn configure(
        &self,
        desired: &BTreeMap<String, ConfiguredMicro>,
    ) -> Result<BTreeMap<String, InstanceState>, ConfigurationError> {
        let states =
            self.composition
                .borrow_mut()
                .reconcile(&self.host, &self.providers, desired)?;
        self.desired.send_if_modified(|current| {
            if current == desired {
                return false;
            }
            *current = desired.clone();
            true
        });
        self.reports
            .send_modify(|report| *report = Ok(states.clone()));
        Ok(states)
    }

    /// Atomic current report plus future updates. Invalid configuration discovered when a provider
    /// becomes available is reported here without tearing down the last working graph.
    pub fn reports(
        &self,
    ) -> tokio::sync::watch::Receiver<Result<BTreeMap<String, InstanceState>, ConfigurationError>>
    {
        self.reports.subscribe()
    }

    /// Run one application-owned local observer. The caller owns this future and cancels it on
    /// unmount; host shutdown also ends it. Subscribe before taking the first snapshot so provider
    /// registration or closure between configuration application and first poll cannot be missed.
    pub async fn observe(&self) {
        struct Guard(Rc<Cell<bool>>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        assert!(
            !self.observing.replace(true),
            "application already has a lifecycle observer"
        );
        let _guard = Guard(self.observing.clone());
        let mut host_changes = self.host.changes();
        let mut provider_changes = self.providers.changes();
        let mut desired_changes = self.desired.subscribe();
        loop {
            let status = *host_changes.borrow_and_update();
            match status {
                HostStatus::Closed => {
                    let states = desired_changes
                        .borrow()
                        .iter()
                        .map(|(name, configured)| {
                            let state = match configured.enablement {
                                crate::micros::composition::Enablement::Enabled => {
                                    InstanceState::Stopped
                                }
                                crate::micros::composition::Enablement::Disabled => {
                                    InstanceState::Disabled
                                }
                            };
                            (name.clone(), state)
                        })
                        .collect();
                    self.reports.send_modify(|report| *report = Ok(states));
                    return;
                }
                HostStatus::Closing => {
                    host_changes.changed().await.unwrap();
                    continue;
                }
                HostStatus::Running => {}
            }
            let desired = desired_changes.borrow_and_update().clone();
            let report =
                self.composition
                    .borrow_mut()
                    .reconcile(&self.host, &self.providers, &desired);
            self.reports.send_modify(|current| *current = report);
            tokio::select! {
                result = host_changes.changed() => result.unwrap(),
                result = provider_changes.changed() => result.unwrap(),
                result = desired_changes.changed() => result.unwrap(),
            }
        }
    }
}

#[cfg(test)]
mod tests;
