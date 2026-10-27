//! Instance lifecycle: hosts, incarnations, mounts, capability checks, and cleanup ordering.
//!
//! This is the authority kernel. Contribution collections live in [`crate::collection`] and
//! depend on this module, never the reverse.
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::watch;

use clay_collection::ScopeId;

// ── Types: public lifecycle vocabulary, then host-internal records ───────────

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

#[derive(Clone, Debug)]
pub struct Instance {
    host: u64,
    pub(crate) id: u64,
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

/// The renderer-specific component registry that the platform-neutral application runtime holds
/// and hands to consumers.
///
/// The host kernel knows nothing about any concrete renderer; the composition layer supplies one
/// through this seam so the same runtime drives native (Dioxus) and headless/test renderers. An
/// implementor is built from a [`Host`] and cloned into the runtime and everything it exposes.
///
/// Publishing goes through here rather than through [`Host::publish_component`] because a
/// component's render factory is a renderer value: the host issues the capability-checked
/// [`ComponentRef`], the registry retains the factory that reference resolves to.
pub trait RendererRegistry: Clone {
    /// What a published render factory produces — an element type only the renderer names.
    type Render: 'static;

    fn for_host(host: &Host) -> Self;

    /// Publish `render` as a component owned by `owner`. The implementor must route the
    /// capability check through [`Host::publish_component`] so revocation stays authoritative.
    fn publish<F>(&self, owner: &Instance, render: F) -> Result<ComponentRef, AccessError>
    where
        F: Fn() -> Self::Render + 'static;
}

pub(crate) struct InstanceRecord {
    effects: Vec<Cleanup>,
    ended: watch::Sender<Option<CloseReason>>,
}

pub(crate) struct State {
    pub(crate) next: u64,
    pub(crate) instances: BTreeMap<u64, InstanceRecord>,
    pub(crate) ending: BTreeMap<u64, (watch::Sender<Option<CloseReason>>, CloseReason)>,
    pub(crate) dependencies: BTreeMap<u64, BTreeSet<u64>>,
    pub(crate) closing: BTreeSet<u64>,
    pub(crate) components: BTreeMap<ComponentRef, u64>,
    pub(crate) live_components: watch::Sender<BTreeSet<ComponentRef>>,
    pub(crate) mounts: BTreeMap<u64, MountRecord>,
    pub(crate) recovery: Recovery,
    pub(crate) changes: watch::Sender<HostStatus>,
}

pub(crate) struct MountRecord {
    owner: u64,
    supplier: u64,
    live: watch::Sender<bool>,
}

/// Non-blocking close ordering (ADR 015 §6). Exactly one close operation drives the
/// close/recovery cascade at a time; a nested `close` only marks the host closing and defers
/// its shutdown cascade to the active driver, which adopts it once its own cascade completes.
/// This preserves "a close in progress finishes its dependents before a later shutdown closes
/// their providers" without OS-thread blocking, so the host stays wasm32-safe (R7).
pub(crate) struct Recovery {
    active: bool,
    closed: bool,
    pub(crate) pending: BTreeSet<u64>,
}

// ── Public API: constructors, then the host lifecycle ────────────────────────

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

impl Instance {
    pub fn scope_id(&self) -> ScopeId {
        ScopeId(self.id)
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

impl Drop for Mount {
    fn drop(&mut self) {
        if let Some(record) = self.host.inner.lock().unwrap().mounts.remove(&self.id) {
            record.live.send_replace(false);
        }
    }
}

// ── Internals: host identity counter, effect cleanup handles ─────────────────

static NEXT_HOST: AtomicU64 = AtomicU64::new(1);
type Cleanup = Box<dyn FnOnce() + Send>;

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
