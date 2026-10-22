//! Host-backed application runtime. Platform composition supplies configured micros.
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::micros::MicroDescriptor;
use crate::micros::MicroProvider;
use crate::micros::MicroRegistry;
use crate::micros::MicroRegistryError;
use crate::micros::composition::Composition;
use crate::micros::composition::ConfigurationError;
use crate::micros::composition::ConfigurationSnapshot;
use crate::micros::composition::ConfigurationSource;
use crate::micros::composition::ConfigurationWatch;
use crate::micros::composition::ConfiguredMicro;
use crate::micros::composition::Enablement;
use crate::micros::composition::InstanceState;
use clay_host::CollectionReader;
use clay_host::Host;
use clay_host::HostStatus;
use clay_host::HostedCollection;
use clay_host::Instance;
use clay_host::RendererRegistry;
use clay_host::RootMount;

type Report = Result<BTreeMap<String, InstanceState>, ConfigurationError>;

struct Source {
    epoch: u64,
    implementation: Rc<dyn ConfigurationSource>,
    watch: Option<ConfigurationWatch>,
    failed: bool,
}

#[derive(Default)]
struct Configuration {
    revision: u64,
    desired: Rc<BTreeMap<String, ConfiguredMicro>>,
    source: Option<Source>,
    loading: bool,
    error: Option<ConfigurationError>,
    settled: Option<(u64, Report)>,
}

struct Driver<'a, R: RendererRegistry> {
    application: &'a ApplicationRuntime<R>,
    composition: Composition,
    source_watch: Option<(u64, ConfigurationWatch)>,
    cancel_on_drop: bool,
}

impl<R: RendererRegistry> Drop for Driver<'_, R> {
    fn drop(&mut self) {
        if self.cancel_on_drop {
            let _futures = self.application.providers.future_driver();
            self.composition.cancel_pending();
        }
        if let Some((epoch, watch)) = self.source_watch.take()
            && let Some(source) = self.application.configuration.borrow_mut().source.as_mut()
            && source.epoch == epoch
            && !source.failed
        {
            source.watch = Some(watch);
        }
        assert!(
            self.application
                .composition
                .borrow_mut()
                .replace(std::mem::take(&mut self.composition))
                .is_none()
        );
        self.application.events.send_replace(());
    }
}

/// The platform-neutral composition boundary for a Dayframe application.
///
/// It owns host facilities and a provider registry, not any concrete micro implementation.
/// Configured activations bind declared requirements to named live providers.
#[derive(Clone)]
pub struct ApplicationRuntime<R: RendererRegistry> {
    host: Host,
    root: Instance,
    roots: CollectionReader<RootMount>,
    renderer: R,
    root_mounts: HostedCollection<RootMount>,
    providers: MicroRegistry,
    documents: clay_docs::DocumentService,
    composition: Rc<RefCell<Option<Composition>>>,
    configuration: Rc<RefCell<Configuration>>,
    events: tokio::sync::watch::Sender<()>,
    reports: tokio::sync::watch::Sender<Report>,
    observing: Rc<Cell<bool>>,
}

impl<R: RendererRegistry> Default for ApplicationRuntime<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: RendererRegistry> ApplicationRuntime<R> {
    pub fn new() -> Self {
        let host = Host::new();
        let documents = clay_docs::DocumentService::new(/* cache capacity */ 32);
        let renderer = R::for_host(&host);
        let root = host.start("desktop-root");
        let root_mounts = HostedCollection::<RootMount>::new(&host, &root);
        Self {
            roots: root_mounts.reader(&root),
            host,
            root,
            renderer,
            root_mounts,
            providers: MicroRegistry::new(documents.clone()),
            documents,
            composition: Rc::new(RefCell::new(Some(Composition::default()))),
            configuration: Rc::default(),
            events: tokio::sync::watch::channel(()).0,
            reports: tokio::sync::watch::channel(Ok(BTreeMap::new())).0,
            observing: Rc::default(),
        }
    }

    pub fn host(&self) -> Host {
        self.host.clone()
    }

    pub fn documents(&self) -> clay_docs::DocumentService {
        self.documents.clone()
    }

    /// Privileged mock source controls for tests and harnesses; never exposed to activations.
    pub fn mock_documents(&self) -> clay_docs::MockDocuments {
        self.documents.mock_documents()
    }

    /// Composition-root authority: grant an instance its approved configuration facets.
    pub fn grant_config(
        &self,
        instance: &Instance,
        invoking_plug: &str,
        acl: Vec<daybook_types::manifest::RoutineFacetAccess>,
    ) -> Result<std::collections::BTreeMap<String, clay_docs::DocumentRef>, clay_host::AccessError>
    {
        self.documents
            .grant_config(&self.host, instance, invoking_plug, acl)
    }

    pub fn root(&self) -> Instance {
        self.root.clone()
    }

    pub fn roots(&self) -> CollectionReader<RootMount> {
        self.roots.clone()
    }

    pub fn renderer(&self) -> R {
        self.renderer.clone()
    }

    pub fn root_mounts(&self) -> HostedCollection<RootMount> {
        self.root_mounts.clone()
    }

    pub fn output<T: crate::micros::Provision>(
        &self,
        instance: &str,
    ) -> Result<T, clay_host::AccessError> {
        self.providers.output(instance)
    }

    /// [`Self::output`] for a contract named at runtime.
    pub fn output_erased(
        &self,
        instance: &str,
        contract: &str,
        version: u32,
    ) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, clay_host::AccessError> {
        self.providers.output_erased(instance, contract, version)
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

    /// Install a [`ConfigurationSource`] without reconciling. The observer applies it.
    pub fn install_configuration_source(&self, source: impl ConfigurationSource + 'static) {
        let source: Rc<dyn ConfigurationSource> = Rc::new(source);
        let subscription = source.subscribe();
        let snapshot = match &subscription {
            Ok(watch) => source.current().and_then(|snapshot| {
                watch.has_changed()?;
                Ok(snapshot)
            }),
            Err(error) => Err(*error),
        };
        let mut configuration = self.configuration.borrow_mut();
        configuration.revision += 1;
        configuration.settled = None;
        configuration.error = None;
        let epoch = configuration.revision;
        configuration.source = Some(Source {
            epoch,
            implementation: source,
            watch: subscription.ok(),
            failed: false,
        });
        drop(configuration);
        self.accept_snapshot(snapshot, true);
    }

    /// Install a [`ConfigurationSource`] and reconcile its initial desired composition.
    pub async fn set_configuration_source(
        &self,
        source: impl ConfigurationSource + 'static,
    ) -> Report {
        self.install_configuration_source(source);
        let revision = self.configuration.borrow().revision;
        self.wait_revision(revision).await
    }

    /// Reconcile the current snapshot from the installed [`ConfigurationSource`].
    pub async fn reconcile_configuration(&self) -> Report {
        let source = self
            .configuration
            .borrow()
            .source
            .as_ref()
            .map(|source| (source.implementation.clone(), source.failed));
        if let Some((source, failed)) = source {
            {
                let mut configuration = self.configuration.borrow_mut();
                configuration.revision += 1;
                configuration.settled = None;
            }
            if !failed {
                self.accept_snapshot(source.current(), true);
            }
            self.events.send_replace(());
            let revision = self.configuration.borrow().revision;
            self.wait_revision(revision).await
        } else {
            let desired = self.configuration.borrow().desired.clone();
            self.configure(&desired).await
        }
    }

    pub fn is_locked(&self, instance: &str) -> bool {
        let source = self
            .configuration
            .borrow()
            .source
            .as_ref()
            .map(|source| source.implementation.clone());
        source.is_some_and(|source| {
            source
                .fixed()
                .is_some_and(|fixed| fixed.contains_key(instance))
        })
    }

    fn validate(
        &self,
        desired: &BTreeMap<String, ConfiguredMicro>,
    ) -> Result<(), ConfigurationError> {
        let source = self
            .configuration
            .borrow()
            .source
            .as_ref()
            .map(|source| source.implementation.clone());
        if let Some(source) = source
            && let Some(fixed) = source.fixed()
        {
            for (name, entry) in fixed {
                if desired.get(name) != Some(entry) {
                    return Err(ConfigurationError::LockedInstance(name.clone()));
                }
            }
        }
        Composition::validate(&self.providers, desired)
    }

    fn accept_snapshot(
        &self,
        snapshot: Result<ConfigurationSnapshot, clay_host::AccessError>,
        initial: bool,
    ) {
        let snapshot = snapshot
            .map_err(ConfigurationError::from)
            .and_then(|snapshot| {
                if let ConfigurationSnapshot::Ready(desired) = &snapshot {
                    self.validate(desired)?;
                }
                Ok(snapshot)
            });
        let mut configuration = self.configuration.borrow_mut();
        if !initial {
            let unchanged = match &snapshot {
                Ok(ConfigurationSnapshot::Ready(desired)) => {
                    !configuration.loading
                        && configuration.error.is_none()
                        && *configuration.desired == *desired
                }
                Ok(ConfigurationSnapshot::Loading) => configuration.loading,
                Err(error) => configuration.error.as_ref() == Some(error),
            };
            if unchanged {
                return;
            }
            if !configuration.loading {
                configuration.revision += 1;
            }
            configuration.settled = None;
        }
        match snapshot {
            Ok(ConfigurationSnapshot::Loading) => {
                configuration.loading = true;
                configuration.error = None;
            }
            Ok(ConfigurationSnapshot::Ready(desired)) => {
                configuration.desired = Rc::new(desired);
                configuration.loading = false;
                configuration.error = None;
            }
            Err(error) => {
                if matches!(error, ConfigurationError::Access(_)) {
                    configuration.source.as_mut().unwrap().failed = true;
                }
                configuration.loading = false;
                configuration.error = Some(error);
            }
        }
        drop(configuration);
        self.events.send_replace(());
    }

    /// Apply a desired boot configuration projection. Malformed edits leave the live graph intact;
    /// absent implementations/dependencies and activation failures are explicit per-instance states.
    pub async fn configure(&self, desired: &BTreeMap<String, ConfiguredMicro>) -> Report {
        let source_error = {
            let configuration = self.configuration.borrow();
            configuration
                .source
                .as_ref()
                .filter(|source| source.failed)
                .and(configuration.error.clone())
        };
        if let Some(error) = source_error {
            return Err(error);
        }
        let validation = self.validate(desired);
        let revision = {
            let mut configuration = self.configuration.borrow_mut();
            configuration.revision += 1;
            configuration.settled = None;
            configuration.error = validation.err();
            if configuration.error.is_none() {
                configuration.desired = Rc::new(desired.clone());
                configuration.loading = false;
            }
            configuration.revision
        };
        self.events.send_replace(());
        self.wait_revision(revision).await
    }

    fn driver(&self) -> Option<Driver<'_, R>> {
        let composition = self.composition.borrow_mut().take()?;
        Some(Driver {
            application: self,
            composition,
            source_watch: None,
            cancel_on_drop: true,
        })
    }

    async fn wait_revision(&self, revision: u64) -> Report {
        let mut events = self.events.subscribe();
        loop {
            events.borrow_and_update();
            {
                let configuration = self.configuration.borrow();
                if configuration.revision != revision {
                    return Err(ConfigurationError::Superseded);
                }
                if let Some((settled_revision, report)) = &configuration.settled
                    && *settled_revision == revision
                {
                    return report.clone();
                }
            }
            if let Some(mut driver) = self.driver() {
                driver.run(Some(revision)).await;
                driver.cancel_on_drop = false;
            } else {
                events.changed().await.unwrap();
            }
        }
    }

    /// Atomic current report plus future updates. Invalid configuration discovered when a provider
    /// becomes available is reported here without tearing down the last working graph.
    pub fn reports(&self) -> tokio::sync::watch::Receiver<Report> {
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
        let mut events = self.events.subscribe();
        loop {
            events.borrow_and_update();
            if let Some(mut driver) = self.driver() {
                driver.run(None).await;
                driver.cancel_on_drop = false;
                return;
            }
            events.changed().await.unwrap();
        }
    }
}

impl<R: RendererRegistry> Driver<'_, R> {
    fn source_updates(&mut self) {
        let mut configuration = self.application.configuration.borrow_mut();
        let epoch = configuration.source.as_ref().map(|source| source.epoch);
        if self.source_watch.as_ref().map(|(epoch, _)| *epoch) != epoch {
            self.source_watch = configuration
                .source
                .as_mut()
                .and_then(|source| source.watch.take().map(|watch| (source.epoch, watch)));
        }
        if configuration
            .source
            .as_ref()
            .is_some_and(|source| source.failed)
        {
            self.source_watch = None;
        }
        drop(configuration);
        if let Some((_, watch)) = self.source_watch.as_mut() {
            let snapshot = match watch.has_changed() {
                Ok(false) => return,
                Ok(true) => watch.current(),
                Err(error) => Err(error),
            };
            self.application.accept_snapshot(snapshot, false);
            if self
                .application
                .configuration
                .borrow()
                .source
                .as_ref()
                .unwrap()
                .failed
            {
                self.source_watch = None;
            }
        }
    }

    fn publish(&self, revision: u64, report: Report, settled: bool) {
        let mut configuration = self.application.configuration.borrow_mut();
        assert_eq!(configuration.revision, revision);
        if settled {
            configuration.settled = Some((revision, report.clone()));
        }
        drop(configuration);
        self.application
            .reports
            .send_modify(|current| *current = report);
        if settled {
            self.application.events.send_replace(());
        }
    }

    async fn run(&mut self, target: Option<u64>) {
        let application = self.application;
        let mut futures = application.providers.future_driver();
        let mut host_changes = self.application.host.changes();
        let mut provider_changes = self.application.providers.changes();
        let mut events = self.application.events.subscribe();
        let mut completion = None;
        loop {
            events.borrow_and_update();
            self.source_updates();
            futures.reap();
            let (revision, desired, error, loading) = {
                let configuration = self.application.configuration.borrow();
                (
                    configuration.revision,
                    configuration.desired.clone(),
                    configuration.error.clone(),
                    configuration.loading,
                )
            };
            if target.is_some_and(|target| target != revision) {
                return;
            }
            let status = *host_changes.borrow_and_update();
            match status {
                HostStatus::Closed => {
                    self.composition.cancel_pending();
                    let states = desired
                        .iter()
                        .map(|(name, config)| {
                            (
                                name.clone(),
                                match config.enablement {
                                    Enablement::Enabled => InstanceState::Stopped,
                                    Enablement::Disabled => InstanceState::Disabled,
                                },
                            )
                        })
                        .collect();
                    self.publish(revision, Ok(states), true);
                    return;
                }
                HostStatus::Closing => {
                    self.composition.cancel_pending();
                    futures.reap();
                    host_changes.changed().await.unwrap();
                    continue;
                }
                HostStatus::Running => {}
            }
            if let Some(name) = completion.take() {
                self.composition.complete(name, &desired);
            }
            let states = self.composition.update(
                &self.application.host,
                &self.application.providers,
                &desired,
            );
            let report = match error {
                Some(error) => Err(error),
                None if loading => Err(ConfigurationError::Loading),
                None => states,
            };
            let settled = report
                .as_ref()
                .is_err_and(|error| *error != ConfigurationError::Loading)
                || (!loading && self.composition.is_settled());
            self.publish(revision, report, settled);
            if target.is_some() && settled {
                return;
            }
            events.borrow_and_update();
            tokio::select! {
                biased;
                result = host_changes.changed() => result.unwrap(),
                result = events.changed() => result.unwrap(),
                result = provider_changes.changed() => result.unwrap(),
                result = async {
                    match self.source_watch.as_mut() {
                        Some((_, watch)) => watch.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(error) = result {
                        self.application.accept_snapshot(Err(error), false);
                        self.source_watch = None;
                    } else if let Some((_, watch)) = self.source_watch.as_mut() {
                        let snapshot = watch.current();
                        self.application.accept_snapshot(snapshot, false);
                    }
                }
                () = futures.run() => unreachable!(),
                ready = self.composition.next() => completion = Some(ready),
            }
        }
    }
}

#[cfg(test)]
mod tests;
