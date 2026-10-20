//! Privileged composition host. Micros receive bound handles, not this constructor surface.
//!
//! This crate is the authority kernel only: lifecycle, tokens, contribution collections,
//! component references, and mount records. Component (wasm) hosting lives in the engine
//! crates and document data lives in `clay_data`; neither is this crate's concern
//! (ADR 015 §3, §6).
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

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
pub enum HostStatus {
    Running,
    Closing,
    Closed,
}

struct InstanceRecord {
    effects: Vec<Cleanup>,
    ended: watch::Sender<Option<CloseReason>>,
}

/// Non-blocking close ordering (ADR 015 §6). Exactly one close operation drives the
/// close/recovery cascade at a time; a nested `close` only marks the host closing and defers
/// its shutdown cascade to the active driver, which adopts it once its own cascade completes.
/// This preserves "a close in progress finishes its dependents before a later shutdown closes
/// their providers" without OS-thread blocking, so the host stays wasm32-safe (R7).
struct Recovery {
    active: bool,
    closed: bool,
    pending: BTreeSet<u64>,
}
struct ActivationGuard {
    host: Host,
    instance: Instance,
    committed: bool,
}
impl Drop for ActivationGuard {
    fn drop(&mut self) {
        if !self.committed {
            self.host.close_instance(&self.instance).unwrap();
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
    identity: u64,
}

pub(crate) struct State {
    next: u64,
    instances: BTreeMap<u64, InstanceRecord>,
    ending: BTreeMap<u64, (watch::Sender<Option<CloseReason>>, CloseReason)>,
    dependencies: BTreeMap<u64, BTreeSet<u64>>,
    closing: BTreeSet<u64>,
    components: BTreeMap<ComponentRef, u64>,
    live_components: watch::Sender<BTreeSet<ComponentRef>>,
    mounts: BTreeMap<u64, MountRecord>,
    recovery: Recovery,
    changes: watch::Sender<HostStatus>,
}

struct MountRecord {
    owner: u64,
    supplier: u64,
    live: watch::Sender<bool>,
}

#[derive(Clone, Debug)]
pub struct Instance {
    host: u64,
    id: u64,
    pub name: String,
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
                ending: BTreeMap::new(),
                dependencies: BTreeMap::new(),
                closing: BTreeSet::new(),
                components: BTreeMap::new(),
                live_components: watch::channel(BTreeSet::new()).0,
                mounts: BTreeMap::new(),
                recovery: Recovery {
                    active: false,
                    closed: false,
                    pending: BTreeSet::new(),
                },
                changes: watch::channel(HostStatus::Running).0,
            })),
        }
    }

    pub fn start(&self, name: impl Into<String>) -> Instance {
        self.try_start(name)
            .expect("cannot start instances on a closed host")
    }

    pub fn try_start(&self, name: impl Into<String>) -> Result<Instance, AccessError> {
        let mut state = self.inner.lock().unwrap();
        if state.recovery.closed {
            return Err(AccessError::Revoked);
        }
        state.next += 1;
        let id = state.next;
        let (ended, _) = watch::channel(None);
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
        Ok(Instance {
            host: self.identity,
            id,
            name: name.into(),
        })
    }

    /// Activation is transactional with respect to owned effects. Failed mandatory publication
    /// closes partial effects; the host does not retain a half-activated instance.
    pub fn activate<T>(
        &self,
        name: impl Into<String>,
        activate: impl FnOnce(Instance) -> Result<T, AccessError>,
    ) -> Result<T, AccessError> {
        let mut guard = ActivationGuard {
            host: self.clone(),
            instance: self.try_start(name)?,
            committed: false,
        };
        let result = activate(guard.instance.clone())?;
        self.check(&guard.instance)?;
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
            host: self.clone(),
            instance: self.try_start(name)?,
            committed: false,
        };
        let instance = guard.instance.clone();
        let result = tokio::select! {
            biased;
            _ = self.await_close(&instance) => return Err(AccessError::Revoked),
            result = async { activate(instance.clone()).await } => result?,
        };
        self.check(&instance)?;
        guard.committed = true;
        Ok(result)
    }

    pub fn publish_component(&self, owner: &Instance) -> Result<ComponentRef, AccessError> {
        self.check(owner)?;
        let mut state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&owner.id) || state.closing.contains(&owner.id) {
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

    pub fn check_component(&self, reference: ComponentRef) -> Result<(), AccessError> {
        if reference.host != self.identity {
            return Err(AccessError::Denied);
        }
        let state = self.inner.lock().unwrap();
        let supplier = state
            .components
            .get(&reference)
            .ok_or(AccessError::Revoked)?;
        if state.closing.contains(supplier) {
            return Err(AccessError::Revoked);
        }
        Ok(())
    }

    pub fn component_lifetimes(&self) -> watch::Receiver<BTreeSet<ComponentRef>> {
        self.inner.lock().unwrap().live_components.subscribe()
    }

    pub fn mount(&self, owner: &Instance, component: ComponentRef) -> Result<Mount, AccessError> {
        self.check(owner)?;
        if component.host != self.identity {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&owner.id) || state.closing.contains(&owner.id) {
            return Err(AccessError::Revoked);
        }
        let supplier = *state
            .components
            .get(&component)
            .ok_or(AccessError::Revoked)?;
        if state.closing.contains(&supplier) {
            return Err(AccessError::Revoked);
        }
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

    pub fn check(&self, instance: &Instance) -> Result<(), AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&instance.id) || state.closing.contains(&instance.id) {
            return Err(AccessError::Revoked);
        }
        Ok(())
    }

    pub fn observe_instance(
        &self,
        instance: &Instance,
    ) -> Result<watch::Receiver<Option<CloseReason>>, AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let state = self.inner.lock().unwrap();
        if state.closing.contains(&instance.id) {
            return Err(AccessError::Revoked);
        }
        Ok(state
            .instances
            .get(&instance.id)
            .ok_or(AccessError::Revoked)?
            .ended
            .subscribe())
    }

    pub async fn await_close(&self, instance: &Instance) {
        assert_eq!(
            self.identity, instance.host,
            "instance belongs to another host"
        );
        let mut ended = {
            let state = self.inner.lock().unwrap();
            match state.instances.get(&instance.id) {
                Some(record) => record.ended.subscribe(),
                None => match state.ending.get(&instance.id) {
                    Some((ended, _)) => ended.subscribe(),
                    None => return,
                },
            }
        };
        ended.wait_for(Option::is_some).await.unwrap();
    }

    pub fn own(&self, instance: &Instance, cleanup: Cleanup) -> Result<(), AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if state.closing.contains(&instance.id) {
            return Err(AccessError::Revoked);
        }
        state
            .instances
            .get_mut(&instance.id)
            .ok_or(AccessError::Revoked)?
            .effects
            .push(cleanup);
        Ok(())
    }

    pub fn changes(&self) -> watch::Receiver<HostStatus> {
        self.inner.lock().unwrap().changes.subscribe()
    }

    pub fn close(&self) {
        // Only the active close driver runs cascades. If another close is already in progress
        // on this thread, this call merely marks the host closing; the active driver adopts the
        // shutdown cascade when its own cascade completes (see `run_closes`).
        {
            let mut state = self.inner.lock().unwrap();
            if state.recovery.closed {
                return;
            }
            state.recovery.closed = true;
            state.changes.send_replace(HostStatus::Closing);
        }
        self.run_closes();
    }

    pub fn close_instance(&self, instance: &Instance) -> Result<(), AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        {
            let mut state = self.inner.lock().unwrap();
            if !state.instances.contains_key(&instance.id) || state.closing.contains(&instance.id) {
                return Ok(());
            }
            state.closing.insert(instance.id);
            state.recovery.pending.insert(instance.id);
        }
        self.run_closes();
        Ok(())
    }

    /// Drives close cascades until quiescent. Only one driver is active at a time; a close
    /// initiated from inside cleanup registers in `closing` and returns, and the active driver
    /// drains it. Requested closes drain first (inner-first, reverse start order); an adopted
    /// shutdown then cascades over what remains, so a close in progress finishes its dependents
    /// before the shutdown closes their providers.
    fn run_closes(&self) {
        {
            let mut state = self.inner.lock().unwrap();
            if state.recovery.active {
                return;
            }
            state.recovery.active = true;
        }
        loop {
            // Drain every registered requested close first (inner-first). A close registered
            // from inside cleanup lands in `pending` and is driven here, so the operation that
            // started it always reaches quiescence before returning.
            let requested = {
                let mut state = self.inner.lock().unwrap();
                std::mem::take(&mut state.recovery.pending)
            };
            for id in requested {
                self.close_instance_inner(id, CloseReason::Requested);
            }
            // An adopted shutdown cascades over whatever remains.
            if self.inner.lock().unwrap().recovery.closed {
                let ids = self
                    .inner
                    .lock()
                    .unwrap()
                    .instances
                    .keys()
                    .rev()
                    .copied()
                    .collect::<Vec<_>>();
                for id in ids {
                    self.close_instance_inner(id, CloseReason::HostShutdown);
                }
            }
            let mut state = self.inner.lock().unwrap();
            if state.recovery.pending.is_empty()
                && (!state.recovery.closed || state.instances.is_empty())
            {
                for (_, (ended, reason)) in std::mem::take(&mut state.ending) {
                    ended.send_replace(Some(reason));
                }
                state.recovery.active = false;
                if state.recovery.closed {
                    assert!(state.closing.is_empty());
                    state.changes.send_replace(HostStatus::Closed);
                } else {
                    state.changes.send_replace(HostStatus::Running);
                }
                break;
            }
        }
    }

    pub fn bind_dependency(
        &self,
        consumer: &Instance,
        provider: &Instance,
    ) -> Result<(), AccessError> {
        if consumer.host != self.identity || provider.host != self.identity {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&consumer.id)
            || !state.instances.contains_key(&provider.id)
            || state.closing.contains(&consumer.id)
            || state.closing.contains(&provider.id)
        {
            return Err(AccessError::Revoked);
        }
        if consumer.id == provider.id {
            return Ok(());
        }
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
        Ok(())
    }

    fn close_instance_inner(&self, id: u64, reason: CloseReason) -> bool {
        let dependents = {
            let mut state = self.inner.lock().unwrap();
            if !state.instances.contains_key(&id) {
                state.closing.remove(&id);
                return false;
            }
            state.closing.insert(id);
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
            assert!(state.ending.insert(id, (effects.ended, reason)).is_none());
            effects.effects
        };
        for cleanup in effects.into_iter().rev() {
            cleanup();
        }
        let mut state = self.inner.lock().unwrap();
        state.dependencies.remove(&id);
        for providers in state.dependencies.values_mut() {
            providers.remove(&id);
        }
        state.closing.remove(&id);
        true
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

/// Host-owned transient contribution collection (ADR 015 §4): owned entries with
/// registration/disposal semantics, not optimistic persistence or source cursors. Entries are
/// plain clone-cheap data (R4) so the same collection shape can cross the wire for guests.
/// A collection's read and insertion handles carry distinct authority. The host constructs the
/// backing store; a domain API (such as Settings) defines the entry type, not the host.
pub struct HostedCollection<T: Clone + Send + Sync + 'static> {
    host: Host,
    provider: Instance,
    state: Arc<Mutex<CollectionState<T>>>,
}
struct CollectionState<T> {
    next: u64,
    next_generation: u64,
    entries: BTreeMap<u64, (u64, T)>,
    readers: BTreeMap<u64, watch::Sender<BTreeMap<u64, T>>>,
}
impl<T: Clone> CollectionState<T> {
    fn snapshot(&self) -> BTreeMap<u64, T> {
        self.entries
            .iter()
            .map(|(id, (_, value))| (*id, value.clone()))
            .collect()
    }
    fn publish(&self) {
        for reader in self.readers.values() {
            reader.send_replace(self.snapshot());
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
    generation: u64,
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
            next_generation: 1,
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
        self.host.bind_dependency(consumer, &self.provider).unwrap();
        let mut state = self.state.lock().unwrap();
        state.next += 1;
        let id = state.next;
        let (sender, updates) = watch::channel(state.snapshot());
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
    pub fn writer(&self, contributor: &Instance) -> Result<CollectionWriter<T>, AccessError> {
        self.host.bind_dependency(contributor, &self.provider)?;
        Ok(CollectionWriter {
            collection: self.clone(),
            contributor: contributor.clone(),
        })
    }
}

impl<T: Clone + Send + Sync + 'static> CollectionReader<T> {
    pub fn snapshot(&self) -> Result<BTreeMap<u64, T>, AccessError> {
        self.collection.host.check(&self.consumer)?;
        self.collection.host.check(&self.collection.provider)?;
        Ok(self.collection.state.lock().unwrap().snapshot())
    }
    pub fn subscribe(&self) -> Result<watch::Receiver<BTreeMap<u64, T>>, AccessError> {
        self.collection.host.check(&self.consumer)?;
        self.collection.host.check(&self.collection.provider)?;
        Ok(self.updates.clone())
    }
}
impl<T: Clone + Send + Sync + 'static> CollectionWriter<T> {
    pub fn insert(&self, value: T) -> Result<Entry<T>, AccessError> {
        self.collection.host.check(&self.contributor)?;
        self.collection.host.check(&self.collection.provider)?;
        let mut state = self.collection.state.lock().unwrap();
        state.next += 1;
        let id = state.next;
        state.next_generation += 1;
        let generation = state.next_generation;
        let cleanup = self.collection.state.clone();
        // Cleanup removes only this exact (id, generation) incarnation, so a stale cleanup
        // cannot withdraw a replacement entry that later reuses the id slot.
        self.collection.host.own(
            &self.contributor,
            Box::new(move || {
                let mut state = cleanup.lock().unwrap();
                if state
                    .entries
                    .get(&id)
                    .is_some_and(|(current, _)| *current == generation)
                {
                    state.entries.remove(&id);
                    state.publish();
                }
            }),
        )?;
        assert!(state.entries.insert(id, (generation, value)).is_none());
        state.publish();
        Ok(Entry {
            writer: self.clone(),
            id,
            generation,
        })
    }
}
impl<T: Clone + Send + Sync + 'static> Entry<T> {
    /// The collection-scoped id of this entry.
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn update(&self, value: T) -> Result<(), AccessError> {
        self.writer
            .collection
            .host
            .check(&self.writer.contributor)?;
        self.writer
            .collection
            .host
            .check(&self.writer.collection.provider)?;
        let mut state = self.writer.collection.state.lock().unwrap();
        match state.entries.get_mut(&self.id) {
            Some((generation, entry)) if *generation == self.generation => {
                *entry = value;
                state.publish();
                Ok(())
            }
            _ => Err(AccessError::Revoked),
        }
    }

    /// Withdraw this entry. A lease is once-only in spirit but idempotent in effect (ADR 015
    /// §6): removing an entry that is already gone — withdrawn here, revoked by owner closure,
    /// or never present — is not an error, because an entry's removal is always the desired
    /// end state. Removal while the writer's contributor or the provider is closed is also
    /// allowed: cleanup must not require live authority. Only this exact (id, generation)
    /// incarnation is withdrawn; a stale lease never removes a replacement.
    pub fn remove(self) {
        let mut state = self.writer.collection.state.lock().unwrap();
        if state
            .entries
            .get(&self.id)
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            state.entries.remove(&self.id);
            state.publish();
        }
    }

    /// Withdraw without consuming, for shared handles. Same idempotent semantics as `remove`.
    pub fn release(&self) {
        let mut state = self.writer.collection.state.lock().unwrap();
        if state
            .entries
            .get(&self.id)
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            state.entries.remove(&self.id);
            state.publish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_values_do_not_retain_host_or_owned_services() {
        let host = Host::new();
        let state = Arc::downgrade(&host.inner);
        let instance = host.start("retained");
        let retained = instance.clone();
        let service = Arc::new(());
        let service_lifetime = Arc::downgrade(&service);
        host.own(&instance, Box::new(move || drop(service)))
            .unwrap();
        assert_eq!(Arc::strong_count(&host.inner), 1);
        drop(host);
        assert!(state.upgrade().is_none());
        assert!(service_lifetime.upgrade().is_none());
        assert_eq!(instance, retained);
        assert_eq!(retained.name, "retained");
    }

    #[test]
    fn foreign_instance_with_colliding_id_is_denied() {
        let host = Host::new();
        let foreign_host = Host::new();
        let local = host.start("same-name");
        let foreign = foreign_host.start("same-name");
        assert_eq!(local.id, foreign.id);
        assert_ne!(local, foreign);
        assert_eq!(host.check(&foreign), Err(AccessError::Denied));
        assert_eq!(host.close_instance(&foreign), Err(AccessError::Denied));
        assert!(matches!(
            host.observe_instance(&foreign),
            Err(AccessError::Denied)
        ));
        assert_eq!(host.check(&local), Ok(()));
        assert_eq!(foreign_host.check(&foreign), Ok(()));
        host.close();
        assert_eq!(host.close_instance(&local), Ok(()));
        assert_eq!(host.close_instance(&foreign), Err(AccessError::Denied));
        foreign_host.close();
    }

    #[tokio::test]
    #[should_panic(expected = "instance belongs to another host")]
    async fn awaiting_a_foreign_instance_is_an_invariant_failure() {
        let host = Host::new();
        let foreign_host = Host::new();
        let instance = foreign_host.start("foreign");
        foreign_host.close();
        host.await_close(&instance).await;
    }

    #[tokio::test]
    async fn stale_identity_cannot_revoke_replacement_and_terminal_storage_is_drained() {
        let host = Host::new();
        let stale = host.start("same-name");
        let terminal = host.observe_instance(&stale).unwrap();
        host.close_instance(&stale).unwrap();
        assert_eq!(*terminal.borrow(), Some(CloseReason::Requested));
        assert!(matches!(
            host.observe_instance(&stale),
            Err(AccessError::Revoked)
        ));
        for _ in 0..128 {
            let replacement = host.start("same-name");
            let ended = host.observe_instance(&replacement).unwrap();
            assert_ne!(stale, replacement);
            host.close_instance(&stale).unwrap();
            host.await_close(&stale).await;
            assert_eq!(host.check(&stale), Err(AccessError::Revoked));
            assert_eq!(host.check(&replacement), Ok(()));
            assert_eq!(*ended.borrow(), None);
            host.close_instance(&replacement).unwrap();
            assert_eq!(*ended.borrow(), Some(CloseReason::Requested));
            let state = host.inner.lock().unwrap();
            assert!(state.instances.is_empty());
            assert!(state.ending.is_empty());
            assert!(state.closing.is_empty());
            assert!(state.dependencies.is_empty());
            assert!(state.recovery.pending.is_empty());
        }
        host.close();
        assert_eq!(host.try_start("closed"), Err(AccessError::Revoked));
    }

    #[tokio::test]
    async fn early_and_late_waiters_wait_for_terminal_cleanup() {
        use std::{future::Future, task::Poll};

        let host = Host::new();
        let instance = host.start("owned");
        let mut terminal = host.observe_instance(&instance).unwrap();
        let mut early = Box::pin(host.await_close(&instance));
        std::future::poll_fn(|context| {
            assert!(early.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        host.own(
            &instance,
            Box::new(move || {
                entered.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        )
        .unwrap();
        let closing_host = host.clone();
        let closing_instance = instance.clone();
        let closing = std::thread::spawn(move || {
            closing_host.close_instance(&closing_instance).unwrap();
        });
        entered_rx.await.unwrap();
        assert_eq!(host.check(&instance), Err(AccessError::Revoked));
        assert!(matches!(
            host.observe_instance(&instance),
            Err(AccessError::Revoked)
        ));
        assert_eq!(*terminal.borrow(), None);
        let mut late = Box::pin(host.await_close(&instance));
        std::future::poll_fn(|context| {
            assert!(early.as_mut().poll(context).is_pending());
            assert!(late.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        early.await;
        late.await;
        terminal.changed().await.unwrap();
        assert_eq!(*terminal.borrow(), Some(CloseReason::Requested));
        closing.join().unwrap();
        host.await_close(&instance).await;
        assert!(host.inner.lock().unwrap().ending.is_empty());
        host.close();
    }

    #[test]
    fn collection_writer_binding_rejects_foreign_and_closed_authority() {
        let host = Host::new();
        let provider = host.start("provider");
        let contributor = host.start("contributor");
        let collection = HostedCollection::<String>::new(&host, &provider);
        let foreign_host = Host::new();
        let foreign = foreign_host.start("foreign");
        assert!(matches!(
            collection.writer(&foreign),
            Err(AccessError::Denied)
        ));
        let writer = collection.writer(&contributor).unwrap();
        let entry = writer.insert("live".into()).unwrap();
        let closed = host.start("closed");
        host.close_instance(&closed).unwrap();
        assert!(matches!(
            collection.writer(&closed),
            Err(AccessError::Revoked)
        ));
        host.close_instance(&provider).unwrap();
        assert_eq!(host.check(&contributor), Err(AccessError::Revoked));
        assert_eq!(entry.update("stale".into()), Err(AccessError::Revoked));
        let replacement = host.start("replacement");
        assert!(matches!(
            collection.writer(&replacement),
            Err(AccessError::Revoked)
        ));
        assert_eq!(host.check(&replacement), Ok(()));
        assert!(host.inner.lock().unwrap().dependencies.is_empty());
        host.close();
        foreign_host.close();
    }

    #[test]
    fn collection_writer_binding_rejects_queued_closes() {
        let host = Host::new();
        let blocker = host.start("blocker");
        let provider = host.start("provider");
        let contributor = host.start("contributor");
        let collection = HostedCollection::<String>::new(&host, &provider);
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        host.own(
            &blocker,
            Box::new(move || {
                entered.send(()).unwrap();
                release_rx.recv().unwrap();
            }),
        )
        .unwrap();
        let closing_host = host.clone();
        let closing = std::thread::spawn(move || {
            closing_host.close_instance(&blocker).unwrap();
        });
        entered_rx.recv().unwrap();
        host.close_instance(&contributor).unwrap();
        let contributor_rejected =
            matches!(collection.writer(&contributor), Err(AccessError::Revoked));
        host.close_instance(&provider).unwrap();
        let live = host.start("live");
        let provider_rejected = matches!(collection.writer(&live), Err(AccessError::Revoked));
        release.send(()).unwrap();
        closing.join().unwrap();
        assert!(contributor_rejected);
        assert!(provider_rejected);
        assert_eq!(host.check(&live), Ok(()));
        assert!(host.inner.lock().unwrap().dependencies.is_empty());
        host.close();
    }

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
            .unwrap()
            .insert("first".into())
            .unwrap();
        let second = collection
            .writer(&second_contributor)
            .unwrap()
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
        host.close_instance(&first_contributor).unwrap();
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

        host.close_instance(&provider).unwrap();
        assert_eq!(reader.snapshot(), Err(AccessError::Revoked));
        assert_eq!(
            second.update("not retained".into()),
            Err(AccessError::Revoked)
        );
        host.close();
    }

    #[test]
    fn withdrawing_an_entry_is_idempotent_and_a_stale_lease_cannot_remove_a_replacement() {
        let host = Host::new();
        let provider = host.start("provider");
        let contributor = host.start("contributor");
        let collection = HostedCollection::<String>::new(&host, &provider);
        let reader = collection.reader(&contributor);
        let writer = collection.writer(&contributor).unwrap();
        let entry = writer.insert("live".into()).unwrap();
        entry.release();
        // Releasing again — and a stale handle to the same id — must not error and must not
        // withdraw the replacement entry the slot is reused for.
        entry.release();
        writer.insert("replacement".into()).unwrap();
        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .values()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["replacement"]
        );
        // Ids are not reused (a reader's registration consumed one), but the stale handle's
        // generation no longer matches any live entry, so it cannot withdraw the replacement.
        entry.release();
        assert_eq!(
            reader
                .snapshot()
                .unwrap()
                .values()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["replacement"]
        );
        assert_eq!(entry.update("stale".into()), Err(AccessError::Revoked));
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
        assert_eq!(host.check(&instance), Err(AccessError::Revoked));
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
        host.bind_dependency(&consumer, &provider).unwrap();
        let provider_ended = host.observe_instance(&provider).unwrap();
        let consumer_ended = host.observe_instance(&consumer).unwrap();
        let mut changes = host.changes();
        let sibling = host.start("sibling");
        host.bind_dependency(&sibling, &provider).unwrap();
        let sibling_ended = host.observe_instance(&sibling).unwrap();
        changes.borrow_and_update();
        for instance in [&provider, &consumer, &sibling] {
            let observer = changes.clone();
            let terminals = [
                provider_ended.clone(),
                consumer_ended.clone(),
                sibling_ended.clone(),
            ];
            host.own(
                instance,
                Box::new(move || {
                    assert!(
                        !observer.has_changed().unwrap(),
                        "closure notification overtook recovery"
                    );
                    for terminal in terminals {
                        assert_eq!(*terminal.borrow(), None);
                    }
                }),
            )
            .unwrap();
        }
        host.close_instance(&provider).unwrap();
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        assert_eq!(*provider_ended.borrow(), Some(CloseReason::Requested));
        assert_eq!(
            *consumer_ended.borrow(),
            Some(CloseReason::DependencyWithdrawn)
        );
        host.close_instance(&provider).unwrap();
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
        let ended = host.observe_instance(&instance).unwrap();
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
        assert_eq!(*ended.borrow(), Some(CloseReason::HostShutdown));
    }

    #[test]
    fn a_close_started_inside_cleanup_recovers_dependents_before_their_provider() {
        // A host shutdown re-entered from an instance's own cleanup must still close the
        // consumer first, keep the provider live for the consumer's cleanup, and report
        // Closed only after the whole cascade finished — all without OS-thread blocking.
        let host = Host::new();
        let provider = host.start("provider");
        let consumer = host.start("consumer");
        host.bind_dependency(&consumer, &provider).unwrap();
        let provider_ended = host.observe_instance(&provider).unwrap();
        let consumer_ended = host.observe_instance(&consumer).unwrap();
        let changes = host.changes();
        let provider_for_cleanup = provider.clone();
        let host_in_cleanup = host.clone();
        let recovered = Arc::new(Mutex::new(Vec::new()));
        let consumer_recovered = recovered.clone();
        host.own(
            &consumer,
            Box::new(move || {
                host_in_cleanup
                    .check(&provider_for_cleanup)
                    .expect("provider must outlive consumer cleanup");
                host_in_cleanup.close();
                consumer_recovered.lock().unwrap().push("consumer");
            }),
        )
        .unwrap();
        let provider_recovered = recovered.clone();
        let consumer_terminal = consumer_ended.clone();
        host.own(
            &provider,
            Box::new(move || {
                assert_eq!(*consumer_terminal.borrow(), None);
                provider_recovered.lock().unwrap().push("provider");
            }),
        )
        .unwrap();
        host.close_instance(&consumer).unwrap();
        assert_eq!(*recovered.lock().unwrap(), ["consumer", "provider"]);
        assert_eq!(*changes.borrow(), HostStatus::Closed);
        assert_eq!(*consumer_ended.borrow(), Some(CloseReason::Requested));
        assert_eq!(*provider_ended.borrow(), Some(CloseReason::HostShutdown));
    }

    #[test]
    fn reentrant_shutdown_does_not_publish_closed_before_current_cleanup() {
        let host = Host::new();
        let instance = host.start("owned");
        let ended = host.observe_instance(&instance).unwrap();
        let changes = host.changes();
        let observer = changes.clone();
        let reentrant = host.clone();
        host.own(
            &instance,
            Box::new(move || {
                reentrant.close();
                assert_eq!(*observer.borrow(), HostStatus::Closing);
            }),
        )
        .unwrap();
        host.close_instance(&instance).unwrap();
        assert_eq!(*changes.borrow(), HostStatus::Closed);
        assert_eq!(*ended.borrow(), Some(CloseReason::Requested));
    }

    #[tokio::test]
    async fn closing_an_instance_cancels_its_pending_activation() {
        use std::{future::Future, task::Poll};

        let host = Host::new();
        let (ready, started) = tokio::sync::oneshot::channel();
        let mut activation = Box::pin(host.activate_async("pending", |instance| async move {
            assert!(ready.send(instance).is_ok());
            std::future::pending::<Result<(), AccessError>>().await
        }));
        std::future::poll_fn(|context| {
            assert!(activation.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        host.close_instance(&started.await.unwrap()).unwrap();
        assert_eq!(activation.await, Err(AccessError::Revoked));
        host.close();
    }
}
