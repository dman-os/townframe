//! Instance lifecycle: hosts, incarnations, mounts, capability checks, and cleanup ordering.
//!
//! This is the authority kernel. Contribution collections live in [`crate::collection`] and
//! depend on this module, never the reverse.
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
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

/// Where an incarnation is in its lifecycle (ADR 030 §§5–6). Only `Active` and `Closing`
/// incarnations are visible to other micros: everything published while `Created` or
/// `Initializing` stays staged until initialization commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    Created,
    Initializing,
    Active,
    /// Outward visibility has been withdrawn; the incarnation's own authority and its
    /// dependencies stay usable so cleanup (`instance-root.close()`) can run.
    Closing,
    Closed,
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
    /// Owns the registered render factory: dropping it unregisters the factory and drops what
    /// it captured. The publishing incarnation holds it until its close.
    type Guard: 'static;

    fn publish<F>(
        &self,
        owner: &Instance,
        render: F,
    ) -> Result<(ComponentRef, Self::Guard), AccessError>
    where
        F: Fn() -> Self::Render + 'static;
}

/// A strong runtime edge `consumer -> provider` (ADR 030 §2): while it exists the provider
/// incarnation cannot be closed for lack of demand, and closing the provider closes the
/// consumer first. Dropping it removes exactly this edge; edges are keyed by incarnation ids,
/// which are never reused, so a stale binding cannot touch a replacement's edges.
#[must_use = "dropping a binding releases the provider immediately"]
pub struct Binding {
    state: Weak<Mutex<State>>,
    consumer: u64,
    provider: u64,
}

pub(crate) struct InstanceRecord {
    effects: Vec<Cleanup>,
    ended: watch::Sender<Option<CloseReason>>,
    lifecycle: watch::Sender<Lifecycle>,
    /// Called with `Active` when initialization commits and with `Closing` when outward
    /// visibility is withdrawn. Hooks run outside the host lock, so a racing commit and close
    /// may deliver `Active` after `Closing`; `Closing` is terminal and hooks must treat a late
    /// `Active` as a no-op.
    outward: Vec<OutwardHook>,
}

pub(crate) struct Ending {
    ended: watch::Sender<Option<CloseReason>>,
    lifecycle: watch::Sender<Lifecycle>,
    reason: CloseReason,
}

pub(crate) struct State {
    pub(crate) next: u64,
    pub(crate) instances: BTreeMap<u64, InstanceRecord>,
    pub(crate) ending: BTreeMap<u64, Ending>,
    /// consumer -> provider -> number of live [`Binding`]s for that edge.
    pub(crate) dependencies: BTreeMap<u64, BTreeMap<u64, usize>>,
    /// Incarnations whose owned effects are being torn down. Distinct from
    /// [`Lifecycle::Closing`], which only withdraws outward visibility.
    pub(crate) tearing_down: BTreeSet<u64>,
    pub(crate) components: BTreeMap<ComponentRef, u64>,
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
                tearing_down: BTreeSet::new(),
                components: BTreeMap::new(),
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

    /// Start an incarnation that is immediately `Active`, for hosts that have no staged
    /// initialization (native compositions, tests).
    pub fn try_start(&self, name: impl Into<String>) -> Result<Instance, AccessError> {
        self.insert(name.into(), Lifecycle::Active)
    }

    /// Create a `Created` incarnation. It owns effects and may acquire inward capabilities,
    /// but nothing it publishes is outwardly visible until [`Self::commit_initialization`].
    pub fn create(&self, name: impl Into<String>) -> Result<Instance, AccessError> {
        self.insert(name.into(), Lifecycle::Created)
    }

    fn insert(&self, name: String, lifecycle: Lifecycle) -> Result<Instance, AccessError> {
        let mut state = self.inner.lock().unwrap();
        if state.recovery.closed {
            return Err(AccessError::Revoked);
        }
        state.next += 1;
        let id = state.next;
        let record = InstanceRecord {
            effects: Vec::new(),
            ended: watch::channel(None).0,
            lifecycle: watch::channel(lifecycle).0,
            outward: Vec::new(),
        };
        assert!(state.instances.insert(id, record).is_none());
        state.changes.send_replace(HostStatus::Running);
        Ok(Instance {
            host: self.identity,
            id,
            name,
        })
    }

    /// Run one staged initialization (ADR 030 §5): create the incarnation, run `initialize`,
    /// and commit it to `Active` only if that succeeds. On error, on [`Self::close_instance`]
    /// while pending, or when the returned future is dropped, the incarnation is closed and
    /// everything it owned or staged unwinds without ever having been visible.
    pub async fn initialize<T, E, F>(
        &self,
        name: impl Into<String>,
        initialize: impl FnOnce(Instance) -> F,
    ) -> Result<(Instance, T), E>
    where
        E: From<AccessError>,
        F: std::future::Future<Output = Result<T, E>>,
    {
        let mut guard = ActivationGuard {
            host: self.clone(),
            instance: self.create(name)?,
            committed: false,
        };
        let instance = guard.instance.clone();
        self.begin_initialization(&instance)?;
        let result = tokio::select! {
            biased;
            _ = self.await_close(&instance) => return Err(AccessError::Revoked.into()),
            result = initialize(instance.clone()) => result?,
        };
        self.commit_initialization(&instance)?;
        guard.committed = true;
        Ok((instance, result))
    }

    pub fn begin_initialization(&self, instance: &Instance) -> Result<(), AccessError> {
        self.transition(instance, |current| {
            assert_eq!(
                current,
                Lifecycle::Created,
                "initialization begins exactly once, from Created"
            );
            Lifecycle::Initializing
        })
        .map(drop)
    }

    /// Make the incarnation `Active` and atomically expose what it staged: every outward hook
    /// observes `Active` before this returns.
    pub fn commit_initialization(&self, instance: &Instance) -> Result<(), AccessError> {
        let hooks = self.transition(instance, |current| {
            assert_ne!(current, Lifecycle::Active, "initialization committed twice");
            Lifecycle::Active
        })?;
        for hook in hooks {
            hook(Lifecycle::Active);
        }
        Ok(())
    }

    /// Move a live, not-yet-closing incarnation to the state `next` picks from its current
    /// one, returning the outward hooks to notify.
    fn transition(
        &self,
        instance: &Instance,
        next: impl FnOnce(Lifecycle) -> Lifecycle,
    ) -> Result<Vec<OutwardHook>, AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let state = self.inner.lock().unwrap();
        if state.tearing_down.contains(&instance.id) {
            return Err(AccessError::Revoked);
        }
        let record = state
            .instances
            .get(&instance.id)
            .ok_or(AccessError::Revoked)?;
        let current = *record.lifecycle.borrow();
        if current == Lifecycle::Closing {
            return Err(AccessError::Revoked);
        }
        record.lifecycle.send_replace(next(current));
        Ok(record.outward.clone())
    }

    /// Withdraw outward visibility (ADR 030 §6 step 1): the incarnation becomes `Closing`,
    /// its outward hooks observe `Closing`, and new publications and bindings to it are
    /// refused. Its own authority, owned effects, and dependencies stay intact until
    /// [`Self::close_instance`]. Returns `false` when the incarnation is already closing or
    /// gone, so exactly one caller drives each close.
    pub fn begin_close(&self, instance: &Instance) -> Result<bool, AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let hooks = {
            let mut state = self.inner.lock().unwrap();
            if state.tearing_down.contains(&instance.id) {
                return Ok(false);
            }
            let Some(record) = state.instances.get_mut(&instance.id) else {
                return Ok(false);
            };
            match record.withdraw() {
                Some(hooks) => hooks,
                None => return Ok(false),
            }
        };
        for hook in hooks {
            hook(Lifecycle::Closing);
        }
        Ok(true)
    }

    /// Register a hook told when the incarnation's staged outward effects become visible
    /// (`Active`) and when they must be withdrawn (`Closing`). Returns the lifecycle at
    /// registration: when that is already `Active` the hook will not be called with it, so
    /// the caller publishes immediately.
    pub fn on_outward(
        &self,
        instance: &Instance,
        hook: impl Fn(Lifecycle) + Send + Sync + 'static,
    ) -> Result<Lifecycle, AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if state.tearing_down.contains(&instance.id) {
            return Err(AccessError::Revoked);
        }
        let record = state
            .instances
            .get_mut(&instance.id)
            .ok_or(AccessError::Revoked)?;
        let current = *record.lifecycle.borrow();
        if current == Lifecycle::Closing {
            return Err(AccessError::Revoked);
        }
        record.outward.push(Arc::new(hook));
        Ok(current)
    }

    /// The incarnation's lifecycle. A closed (or never-issued) incarnation reports `Closed`;
    /// one whose teardown has run but whose close cascade has not quiesced reports `Closing`.
    pub fn lifecycle(&self, instance: &Instance) -> Lifecycle {
        assert_eq!(
            self.identity, instance.host,
            "instance belongs to another host"
        );
        let state = self.inner.lock().unwrap();
        match state.instances.get(&instance.id) {
            Some(record) => *record.lifecycle.borrow(),
            None if state.ending.contains_key(&instance.id) => Lifecycle::Closing,
            None => Lifecycle::Closed,
        }
    }

    pub fn observe_lifecycle(
        &self,
        instance: &Instance,
    ) -> Result<watch::Receiver<Lifecycle>, AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let state = self.inner.lock().unwrap();
        match state.instances.get(&instance.id) {
            Some(record) => Ok(record.lifecycle.subscribe()),
            None => state
                .ending
                .get(&instance.id)
                .map(|ending| ending.lifecycle.subscribe())
                .ok_or(AccessError::Revoked),
        }
    }

    /// Publish a component owned by `owner`. Refused once `owner` is `Closing`: a closing
    /// incarnation cannot add outward resources. A component published while `owner` is still
    /// initializing is issued, but other micros only learn of it through publications that
    /// stay staged until `owner` commits.
    pub fn publish_component(&self, owner: &Instance) -> Result<ComponentRef, AccessError> {
        if self.identity != owner.host {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if state.tearing_down.contains(&owner.id)
            || state
                .instances
                .get(&owner.id)
                .is_none_or(|record| *record.lifecycle.borrow() == Lifecycle::Closing)
        {
            return Err(AccessError::Revoked);
        }
        state.next += 1;
        let reference = ComponentRef {
            host: self.identity,
            id: state.next,
        };
        assert!(state.components.insert(reference, owner.id).is_none());
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
        if state.tearing_down.contains(supplier) {
            return Err(AccessError::Revoked);
        }
        Ok(())
    }

    pub fn mount(&self, owner: &Instance, component: ComponentRef) -> Result<Mount, AccessError> {
        self.check(owner)?;
        if component.host != self.identity {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        if !state.instances.contains_key(&owner.id) || state.tearing_down.contains(&owner.id) {
            return Err(AccessError::Revoked);
        }
        let supplier = *state
            .components
            .get(&component)
            .ok_or(AccessError::Revoked)?;
        if state.tearing_down.contains(&supplier) {
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
        if !state.instances.contains_key(&instance.id) || state.tearing_down.contains(&instance.id)
        {
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
        if state.tearing_down.contains(&instance.id) {
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
                    Some(ending) => ending.ended.subscribe(),
                    None => return,
                },
            }
        };
        ended.wait_for(Option::is_some).await.unwrap();
    }

    /// Attach an effect to the incarnation; it runs during teardown in reverse attachment
    /// order. A refused effect is dropped after the host lock is released, so an effect whose
    /// drop re-enters the host (a [`Binding`]) is safe to offer.
    pub fn own(&self, instance: &Instance, cleanup: Cleanup) -> Result<(), AccessError> {
        if self.identity != instance.host {
            return Err(AccessError::Denied);
        }
        let mut state = self.inner.lock().unwrap();
        let tearing_down = state.tearing_down.contains(&instance.id);
        match state.instances.get_mut(&instance.id) {
            Some(record) if !tearing_down => {
                record.effects.push(cleanup);
                Ok(())
            }
            _ => {
                drop(state);
                drop(cleanup);
                Err(AccessError::Revoked)
            }
        }
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
            if !state.instances.contains_key(&instance.id)
                || state.tearing_down.contains(&instance.id)
            {
                return Ok(());
            }
            state.tearing_down.insert(instance.id);
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
                for ending in std::mem::take(&mut state.ending).into_values() {
                    ending.lifecycle.send_replace(Lifecycle::Closed);
                    ending.ended.send_replace(Some(ending.reason));
                }
                state.recovery.active = false;
                if state.recovery.closed {
                    assert!(state.tearing_down.is_empty());
                    state.changes.send_replace(HostStatus::Closed);
                } else {
                    state.changes.send_replace(HostStatus::Running);
                }
                break;
            }
        }
    }

    /// Create a strong edge `consumer -> provider` (ADR 030 §2). Refused when either side is
    /// closing or gone: a closing incarnation accepts no new strong acquisitions. Binding an
    /// incarnation to itself or closing a cycle is an invariant failure — mandatory
    /// configured cycles are rejected before anything is started.
    pub fn bind(&self, consumer: &Instance, provider: &Instance) -> Result<Binding, AccessError> {
        if consumer.host != self.identity || provider.host != self.identity {
            return Err(AccessError::Denied);
        }
        assert_ne!(consumer.id, provider.id, "a micro cannot bind itself");
        let mut state = self.inner.lock().unwrap();
        for id in [consumer.id, provider.id] {
            if state.tearing_down.contains(&id)
                || state
                    .instances
                    .get(&id)
                    .is_none_or(|record| *record.lifecycle.borrow() == Lifecycle::Closing)
            {
                return Err(AccessError::Revoked);
            }
        }
        let mut frontier = vec![provider.id];
        let mut seen = BTreeSet::new();
        while let Some(id) = frontier.pop() {
            if id == consumer.id {
                // Release the lock first so unwinding bindings can still remove their edges.
                drop(state);
                panic!("cyclic micro dependency");
            }
            if seen.insert(id)
                && let Some(dependencies) = state.dependencies.get(&id)
            {
                frontier.extend(dependencies.keys());
            }
        }
        *state
            .dependencies
            .entry(consumer.id)
            .or_default()
            .entry(provider.id)
            .or_default() += 1;
        Ok(Binding {
            state: Arc::downgrade(&self.inner),
            consumer: consumer.id,
            provider: provider.id,
        })
    }

    /// Whether any live binding still retains `provider`.
    pub fn is_bound(&self, provider: &Instance) -> bool {
        assert_eq!(
            self.identity, provider.host,
            "instance belongs to another host"
        );
        self.inner
            .lock()
            .unwrap()
            .dependencies
            .values()
            .any(|providers| providers.contains_key(&provider.id))
    }

    fn close_instance_inner(&self, id: u64, reason: CloseReason) -> bool {
        let (hooks, dependents) = {
            let mut state = self.inner.lock().unwrap();
            let Some(record) = state.instances.get_mut(&id) else {
                state.tearing_down.remove(&id);
                return false;
            };
            let hooks = record.withdraw().unwrap_or_default();
            state.tearing_down.insert(id);
            let dependents = state
                .dependencies
                .iter()
                .filter(|(_, providers)| providers.contains_key(&id))
                .map(|(consumer, _)| *consumer)
                .collect::<Vec<_>>();
            (hooks, dependents)
        };
        // Outward visibility goes first, before any dependent or owned effect is torn down.
        for hook in hooks {
            hook(Lifecycle::Closing);
        }
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
            let Some(record) = state.instances.remove(&id) else {
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
            let ending = Ending {
                ended: record.ended,
                lifecycle: record.lifecycle,
                reason,
            };
            assert!(state.ending.insert(id, ending).is_none());
            record.effects
        };
        for cleanup in effects.into_iter().rev() {
            cleanup();
        }
        let mut state = self.inner.lock().unwrap();
        state.dependencies.remove(&id);
        state.dependencies.retain(|_, providers| {
            providers.remove(&id);
            !providers.is_empty()
        });
        state.tearing_down.remove(&id);
        true
    }
}

impl InstanceRecord {
    /// Enter `Closing`, handing back the outward hooks to notify; `None` if already closing.
    fn withdraw(&mut self) -> Option<Vec<OutwardHook>> {
        if *self.lifecycle.borrow() == Lifecycle::Closing {
            return None;
        }
        self.lifecycle.send_replace(Lifecycle::Closing);
        Some(std::mem::take(&mut self.outward))
    }
}

impl Drop for Binding {
    fn drop(&mut self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut state = state.lock().unwrap();
        // Teardown of either endpoint already removed the edge; nothing left to release.
        let Some(providers) = state.dependencies.get_mut(&self.consumer) else {
            return;
        };
        let Some(count) = providers.get_mut(&self.provider) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            providers.remove(&self.provider);
            if providers.is_empty() {
                state.dependencies.remove(&self.consumer);
            }
        }
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
type OutwardHook = Arc<dyn Fn(Lifecycle) + Send + Sync>;

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
