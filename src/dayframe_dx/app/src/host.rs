//! Privileged composition host. Micros receive bound handles, not this constructor surface.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

use crate::documents::DocumentService;

static NEXT_HOST: AtomicU64 = AtomicU64::new(1);
type Cleanup = Box<dyn FnOnce() + Send>;

/// Why an incarnation ended. Dependency withdrawal is not a request to disable the consumer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    Requested,
    DependencyWithdrawn,
    HostShutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HostStatus {
    Running,
    Closing,
    Closed,
}

struct InstanceRecord {
    effects: Vec<Cleanup>,
    ended: watch::Sender<Option<CloseReason>>,
}
struct ActivationGuard {
    instance: Instance,
    committed: bool,
}
impl Drop for ActivationGuard {
    fn drop(&mut self) {
        if !self.committed {
            self.instance.close();
        }
    }
}

/// Basic root presentation vocabulary, independent of feature-specific layout grammar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootMount {
    pub title: String,
    pub content: ComponentRef,
}

#[derive(Clone)]
pub struct Host {
    pub(crate) inner: Arc<Mutex<State>>,
    pub(crate) documents: DocumentService,
    identity: u64,
}

pub(crate) struct State {
    next: u64,
    instances: BTreeMap<u64, InstanceRecord>,
    dependencies: BTreeMap<u64, BTreeSet<u64>>,
    closing: BTreeSet<u64>,
    components: BTreeMap<ComponentRef, u64>,
    live_components: watch::Sender<BTreeSet<ComponentRef>>,
    mounts: BTreeMap<u64, MountRecord>,
    closed: bool,
    changes: watch::Sender<HostStatus>,
}

struct MountRecord {
    owner: u64,
    supplier: u64,
    live: watch::Sender<bool>,
}

#[derive(Clone)]
pub struct Instance {
    host: Host,
    id: u64,
    pub name: String,
    ended: watch::Receiver<Option<CloseReason>>,
}

/// Opaque transport value; only the host can issue a live component reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComponentRef {
    host: u64,
    id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("capability denied")]
    Denied,
    #[error("capability revoked")]
    Revoked,
}

pub struct Mount {
    host: Host,
    id: u64,
    live: watch::Receiver<bool>,
}

impl PartialEq for Host {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
impl PartialEq for Instance {
    fn eq(&self, other: &Self) -> bool {
        self.host == other.host && self.id == other.id
    }
}
impl Default for Host {
    fn default() -> Self {
        Self::new()
    }
}

impl Host {
    pub fn new() -> Self {
        Self {
            identity: NEXT_HOST.fetch_add(1, Ordering::Relaxed),
            inner: Arc::new(Mutex::new(State {
                next: 0,
                instances: BTreeMap::new(),
                dependencies: BTreeMap::new(),
                closing: BTreeSet::new(),
                components: BTreeMap::new(),
                live_components: watch::channel(BTreeSet::new()).0,
                mounts: BTreeMap::new(),
                closed: false,
                changes: watch::channel(HostStatus::Running).0,
            })),
            documents: DocumentService::new(/*cache_capacity*/ 32),
        }
    }

    pub fn start(&self, name: impl Into<String>) -> Instance {
        let mut state = self.inner.lock().unwrap();
        assert!(!state.closed, "cannot start instances on a closed host");
        state.next += 1;
        let id = state.next;
        let (ended, lifetime) = watch::channel(None);
        assert!(
            state
                .instances
                .insert(
                    id,
                    InstanceRecord {
                        effects: Vec::new(),
                        ended
                    }
                )
                .is_none()
        );
        state.changes.send_replace(HostStatus::Running);
        Instance {
            host: self.clone(),
            id,
            name: name.into(),
            ended: lifetime,
        }
    }

    /// Activation is transactional with respect to owned effects. Failed mandatory publication
    /// closes partial effects; the host does not retain a half-activated instance.
    pub fn activate<T>(
        &self,
        name: impl Into<String>,
        activate: impl FnOnce(Instance) -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
        let mut guard = ActivationGuard {
            instance: self.start(name),
            committed: false,
        };
        let result = activate(guard.instance.clone())?;
        guard.committed = true;
        Ok(result)
    }

    /// Dropping a cancelled activation future closes its installed effects as well.
    pub async fn activate_async<T, F: std::future::Future<Output = Result<T, AccessError>>>(
        &self,
        name: impl Into<String>,
        activate: impl FnOnce(Instance) -> F,
    ) -> Result<T, AccessError> {
        let mut guard = ActivationGuard {
            instance: self.start(name),
            committed: false,
        };
        let result = activate(guard.instance.clone()).await?;
        guard.committed = true;
        Ok(result)
    }

    pub fn publish_component(&self, owner: &Instance) -> Result<ComponentRef, AccessError> {
        self.check(owner)?;
        let mut state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&owner.id) {
            return Err(AccessError::Revoked);
        }
        state.next += 1;
        let reference = ComponentRef {
            host: self.identity,
            id: state.next,
        };
        assert!(state.components.insert(reference, owner.id).is_none());
        state
            .live_components
            .send_replace(state.components.keys().copied().collect());
        Ok(reference)
    }

    pub(crate) fn component_lifetimes(&self) -> watch::Receiver<BTreeSet<ComponentRef>> {
        self.inner.lock().unwrap().live_components.subscribe()
    }

    pub fn mount(&self, owner: &Instance, component: ComponentRef) -> Result<Mount, AccessError> {
        self.check(owner)?;
        if component.host != self.identity {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&owner.id) {
            return Err(AccessError::Revoked);
        }
        let supplier = *state
            .components
            .get(&component)
            .ok_or(AccessError::Revoked)?;
        let (live, receiver) = watch::channel(true);
        state.next += 1;
        let id = state.next;
        assert!(
            state
                .mounts
                .insert(
                    id,
                    MountRecord {
                        owner: owner.id,
                        supplier,
                        live
                    }
                )
                .is_none()
        );
        Ok(Mount {
            host: self.clone(),
            id,
            live: receiver,
        })
    }

    pub(crate) fn check(&self, instance: &Instance) -> Result<(), AccessError> {
        if self != &instance.host {
            return Err(AccessError::Denied);
        }
        instance.check()
    }

    pub(crate) fn own(&self, instance: &Instance, cleanup: Cleanup) -> Result<(), AccessError> {
        if self != &instance.host {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        state
            .instances
            .get_mut(&instance.id)
            .ok_or(AccessError::Revoked)?
            .effects
            .push(cleanup);
        Ok(())
    }

    pub(crate) fn changes(&self) -> watch::Receiver<HostStatus> {
        self.inner.lock().unwrap().changes.subscribe()
    }

    pub fn close(&self) {
        let ids = {
            let mut state = self.inner.lock().unwrap();
            if state.closed {
                return;
            }
            state.closed = true;
            state.changes.send_replace(HostStatus::Closing);
            state.instances.keys().rev().copied().collect::<Vec<_>>()
        };
        for id in ids {
            self.close_instance_inner(id, CloseReason::HostShutdown);
        }
        self.inner
            .lock()
            .unwrap()
            .changes
            .send_replace(HostStatus::Closed);
    }

    pub(crate) fn bind_dependency(&self, consumer: &Instance, provider: &Instance) {
        self.check(consumer).unwrap();
        self.check(provider).unwrap();
        if consumer.id == provider.id {
            return;
        }
        let mut state = self.inner.lock().unwrap();
        let mut frontier = vec![provider.id];
        let mut seen = BTreeSet::new();
        while let Some(id) = frontier.pop() {
            assert_ne!(id, consumer.id, "cyclic micro dependency");
            if seen.insert(id)
                && let Some(dependencies) = state.dependencies.get(&id)
            {
                frontier.extend(dependencies);
            }
        }
        state
            .dependencies
            .entry(consumer.id)
            .or_default()
            .insert(provider.id);
    }

    fn close_instance(&self, id: u64) {
        if self.close_instance_inner(id, CloseReason::Requested) {
            let state = self.inner.lock().unwrap();
            if !state.closed {
                // Notify only after the entire dependent subtree and its cleanup have completed.
                state.changes.send_replace(HostStatus::Running);
            }
        }
    }

    fn close_instance_inner(&self, id: u64, reason: CloseReason) -> bool {
        let dependents = {
            let mut state = self.inner.lock().unwrap();
            if !state.instances.contains_key(&id) || !state.closing.insert(id) {
                return false;
            }
            state
                .dependencies
                .iter()
                .filter(|(_, providers)| providers.contains(&id))
                .map(|(consumer, _)| *consumer)
                .collect::<Vec<_>>()
        };
        // Committed bindings remain available for consumer recovery until provider teardown.
        for dependent in dependents {
            let dependent_reason = match reason {
                CloseReason::Requested | CloseReason::DependencyWithdrawn => {
                    CloseReason::DependencyWithdrawn
                }
                CloseReason::HostShutdown => CloseReason::HostShutdown,
            };
            self.close_instance_inner(dependent, dependent_reason);
        }
        let effects = {
            let mut state = self.inner.lock().unwrap();
            let Some(effects) = state.instances.remove(&id) else {
                return false;
            };
            // Withdraw mounts before releasing the supplier's component/document effects.
            state.mounts.retain(|_, mount| {
                if mount.owner == id || mount.supplier == id {
                    mount.live.send_replace(false);
                    false
                } else {
                    true
                }
            });
            state.components.retain(|_, owner| *owner != id);
            state
                .live_components
                .send_replace(state.components.keys().copied().collect());
            state.dependencies.remove(&id);
            for providers in state.dependencies.values_mut() {
                providers.remove(&id);
            }
            effects
        };
        for cleanup in effects.effects.into_iter().rev() {
            cleanup();
        }
        effects.ended.send_replace(Some(reason));
        self.inner.lock().unwrap().closing.remove(&id);
        true
    }
}

impl Instance {
    /// Terminal reason is published after this incarnation's recovery has completed.
    pub fn close_reason(&self) -> Option<CloseReason> {
        *self.ended.borrow()
    }

    pub fn check(&self) -> Result<(), AccessError> {
        if self
            .host
            .inner
            .lock()
            .unwrap()
            .instances
            .contains_key(&self.id)
        {
            Ok(())
        } else {
            Err(AccessError::Revoked)
        }
    }
    pub fn close(&self) {
        self.host.close_instance(self.id);
    }
}

impl Mount {
    pub fn is_live(&self) -> bool {
        *self.live.borrow()
    }
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.live.clone()
    }
}
impl Drop for Mount {
    fn drop(&mut self) {
        if let Some(record) = self.host.inner.lock().unwrap().mounts.remove(&self.id) {
            record.live.send_replace(false);
        }
    }
}

/// A collection's read and insertion handles carry distinct authority. The host constructs the
/// backing store; a domain API (such as Settings) defines the entry type, not the host.
pub struct HostedCollection<T: Clone + Send + Sync + 'static> {
    host: Host,
    provider: Instance,
    state: Arc<Mutex<CollectionState<T>>>,
}
struct CollectionState<T> {
    next: u64,
    entries: BTreeMap<u64, T>,
    readers: BTreeMap<u64, watch::Sender<BTreeMap<u64, T>>>,
}
impl<T: Clone> CollectionState<T> {
    fn publish(&self) {
        for reader in self.readers.values() {
            reader.send_replace(self.entries.clone());
        }
    }
}
#[derive(Clone)]
pub struct CollectionReader<T: Clone + Send + Sync + 'static> {
    collection: HostedCollection<T>,
    consumer: Instance,
    updates: watch::Receiver<BTreeMap<u64, T>>,
}
#[derive(Clone)]
pub struct CollectionWriter<T: Clone + Send + Sync + 'static> {
    collection: HostedCollection<T>,
    contributor: Instance,
}
#[derive(Clone)]
pub struct Entry<T: Clone + Send + Sync + 'static> {
    writer: CollectionWriter<T>,
    id: u64,
}
impl<T: Clone + Send + Sync + 'static> Clone for HostedCollection<T> {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            provider: self.provider.clone(),
            state: self.state.clone(),
        }
    }
}
impl<T: Clone + Send + Sync + 'static> HostedCollection<T> {
    pub fn new(host: &Host, provider: &Instance) -> Self {
        host.check(provider).unwrap();
        let state = Arc::new(Mutex::new(CollectionState {
            next: 0,
            entries: BTreeMap::new(),
            readers: BTreeMap::new(),
        }));
        let cleanup = state.clone();
        host.own(
            provider,
            Box::new(move || {
                let mut state = cleanup.lock().unwrap();
                state.entries.clear();
                for (_, reader) in std::mem::take(&mut state.readers) {
                    reader.send_replace(BTreeMap::new());
                }
            }),
        )
        .unwrap();
        Self {
            host: host.clone(),
            provider: provider.clone(),
            state,
        }
    }
    pub fn reader(&self, consumer: &Instance) -> CollectionReader<T> {
        self.host.check(consumer).unwrap();
        self.host.bind_dependency(consumer, &self.provider);
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        let id = state.next;
        let (sender, updates) = watch::channel(state.entries.clone());
        let cleanup = self.state.clone();
        self.host
            .own(
                consumer,
                Box::new(move || {
                    if let Some(sender) = cleanup.lock().unwrap().readers.remove(&id) {
                        sender.send_replace(BTreeMap::new());
                    }
                }),
            )
            .unwrap();
        state.readers.insert(id, sender);
        CollectionReader {
            collection: self.clone(),
            consumer: consumer.clone(),
            updates,
        }
    }
    pub fn writer(&self, contributor: &Instance) -> CollectionWriter<T> {
        self.host.check(contributor).unwrap();
        self.host.bind_dependency(contributor, &self.provider);
        CollectionWriter {
            collection: self.clone(),
            contributor: contributor.clone(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> CollectionReader<T> {
    pub fn snapshot(&self) -> Result<BTreeMap<u64, T>, AccessError> {
        self.consumer.check()?;
        self.collection.provider.check()?;
        Ok(self.collection.state.lock().unwrap().entries.clone())
    }
    pub fn subscribe(&self) -> Result<watch::Receiver<BTreeMap<u64, T>>, AccessError> {
        self.consumer.check()?;
        self.collection.provider.check()?;
        Ok(self.updates.clone())
    }
}
impl<T: Clone + Send + Sync + 'static> CollectionWriter<T> {
    pub fn insert(&self, value: T) -> Result<Entry<T>, AccessError> {
        self.contributor.check()?;
        self.collection.provider.check()?;
        let mut state = self.collection.state.lock().unwrap();
        state.next += 1;
        let id = state.next;
        let cleanup = self.collection.state.clone();
        self.collection.host.own(
            &self.contributor,
            Box::new(move || {
                let mut state = cleanup.lock().unwrap();
                state.entries.remove(&id);
                state.publish();
            }),
        )?;
        assert!(state.entries.insert(id, value).is_none());
        state.publish();
        Ok(Entry {
            writer: self.clone(),
            id,
        })
    }
}
impl<T: Clone + Send + Sync + 'static> Entry<T> {
    pub fn update(&self, value: T) -> Result<(), AccessError> {
        self.writer.contributor.check()?;
        self.writer.collection.provider.check()?;
        let mut state = self.writer.collection.state.lock().unwrap();
        let entry = state
            .entries
            .get_mut(&self.id)
            .ok_or(AccessError::Revoked)?;
        *entry = value;
        state.publish();
        Ok(())
    }
    pub fn remove(&self) -> Result<(), AccessError> {
        self.writer.contributor.check()?;
        self.writer.collection.provider.check()?;
        let mut state = self.writer.collection.state.lock().unwrap();
        state.entries.remove(&self.id).ok_or(AccessError::Revoked)?;
        state.publish();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_entries_and_readers_follow_their_owned_lifetimes() {
        let host = Host::new();
        let provider = host.start("provider");
        let consumer = host.start("consumer");
        let first_contributor = host.start("first-contributor");
        let second_contributor = host.start("second-contributor");
        let collection = HostedCollection::<String>::new(&host, &provider);
        let reader = collection.reader(&consumer);
        let first = collection
            .writer(&first_contributor)
            .insert("first".into())
            .unwrap();
        let second = collection
            .writer(&second_contributor)
            .insert("second".into())
            .unwrap();

        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .values()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        first_contributor.close();
        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .values()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["second"]
        );
        assert_eq!(
            first.update("not retained".into()),
            Err(AccessError::Revoked)
        );
        assert!(second.update("updated".into()).is_ok());
        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .get(&second.id)
                .map(String::as_str),
            Some("updated")
        );

        provider.close();
        assert_eq!(reader.snapshot(), Err(AccessError::Revoked));
        assert_eq!(
            second.update("not retained".into()),
            Err(AccessError::Revoked)
        );
        host.close();
    }
    #[tokio::test]
    async fn cancelling_async_activation_withdraws_owned_effects() {
        use std::{future::Future, task::Poll};

        let host = Host::new();
        let root = host.start("root");
        let host_for_activation = host.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let mut activation = Box::pin(host.activate_async("async", move |instance| async move {
            let component = host_for_activation.publish_component(&instance)?;
            if ready.send((instance.clone(), component)).is_err() {
                panic!("activation waiter dropped before publication");
            }
            std::future::pending::<Result<(), AccessError>>().await
        }));
        std::future::poll_fn(|context| {
            assert!(activation.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let (instance, component) = match started.await {
            Ok(value) => value,
            Err(_) => panic!("activation exited before publishing its component"),
        };
        drop(activation);
        assert_eq!(instance.check(), Err(AccessError::Revoked));
        assert!(matches!(
            host.mount(&root, component),
            Err(AccessError::Revoked)
        ));
        host.close();
    }

    #[test]
    fn closure_notifications_follow_the_entire_dependent_recovery() {
        let host = Host::new();
        let provider = host.start("provider");
        let consumer = host.start("consumer");
        host.bind_dependency(&consumer, &provider);
        let mut changes = host.changes();
        for instance in [&provider, &consumer] {
            let observer = changes.clone();
            host.own(
                instance,
                Box::new(move || {
                    assert!(
                        !observer.has_changed().unwrap(),
                        "closure notification overtook recovery"
                    );
                }),
            )
            .unwrap();
        }
        provider.close();
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        assert_eq!(provider.close_reason(), Some(CloseReason::Requested));
        assert_eq!(
            consumer.close_reason(),
            Some(CloseReason::DependencyWithdrawn)
        );
        provider.close();
        assert!(
            !changes.has_changed().unwrap(),
            "duplicate close emitted activity"
        );
        host.close();
        assert_eq!(*changes.borrow(), HostStatus::Closed);
    }

    #[test]
    fn host_shutdown_is_not_reported_complete_before_recovery() {
        let host = Host::new();
        let instance = host.start("owned");
        let changes = host.changes();
        let during_cleanup = changes.clone();
        host.own(
            &instance,
            Box::new(move || {
                assert_eq!(*during_cleanup.borrow(), HostStatus::Closing);
            }),
        )
        .unwrap();
        host.close();
        assert_eq!(*changes.borrow(), HostStatus::Closed);
        assert_eq!(instance.close_reason(), Some(CloseReason::HostShutdown));
    }
}
