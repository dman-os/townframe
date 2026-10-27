//! Bounded admission of transported requests and their cancellation.
//!
//! Two scopes exist and must not be confused:
//!
//! * A [`CancellationSignal`] is request-scoped. Direct calls share it by
//!   clone; a transported request carries only its key and binds to the
//!   receiver's canonical state when admitted.
//! * [`Admission`] is receiver-scoped. It bounds concurrently admitted unary
//!   handlers and stream pumps, accounts for them in [`TaskActivity`], and
//!   keeps the registry that lets a caller cancel work it has already handed
//!   over.
//!
//! The ledger that joins the two is capacity-bounded: a lease that is no
//! longer in the live map is terminal forever, so terminal history never
//! accumulates.

use crate::context::ContextError;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

/// The scoped key carried by a cancellation command. The token distinguishes
/// separately admitted requests that happen to reuse the same request UUID.
/// A caller must create a fresh token for each transport request; reusing the
/// same full key for another in-flight generation is invalid, so this API does
/// not promise to disambiguate that replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CancellationKey {
    pub request_id: Uuid,
    pub token: Uuid,
}

/// A transport-owned admission registry. Its live map is bounded by the
/// explicitly configured scope capacity; an issued ordinal absent from the map
/// is terminal forever, so terminal history does not accumulate.
#[derive(Clone)]
pub struct CancellationTransport {
    admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct AdmissionLease {
    epoch: Uuid,
    ordinal: u64,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum WaitHookPoint {
    AfterFlagCheck,
    BeforeWait,
}

#[cfg(test)]
struct WaitGate {
    ready: tokio::sync::oneshot::Sender<()>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
struct CancellationWaitHook {
    point: WaitHookPoint,
    gates: std::sync::Mutex<std::collections::VecDeque<WaitGate>>,
}

#[cfg(test)]
impl CancellationWaitHook {
    async fn pause(&self, point: WaitHookPoint) {
        let gate = if self.point == point {
            self.gates.lock().expect("wait hook mutex").pop_front()
        } else {
            None
        };
        if let Some(gate) = gate {
            gate.ready.send(()).expect("waiter test remains open");
            gate.release.notified().await;
        }
    }
}

#[cfg(test)]
struct OrdinaryAdmissionHook {
    first: AtomicBool,
    ready: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

#[cfg(test)]
impl OrdinaryAdmissionHook {
    fn pause_first(&self) {
        if !self.first.swap(true, Ordering::AcqRel) {
            self.ready
                .lock()
                .expect("ordinary hook mutex")
                .take()
                .expect("ordinary hook ready sender")
                .send(())
                .expect("ordinary hook test remains open");
            self.release
                .lock()
                .expect("ordinary hook mutex")
                .take()
                .expect("ordinary hook release receiver")
                .recv()
                .expect("ordinary hook release remains open");
        }
    }
}

struct AdmissionEntry {
    key: CancellationKey,
    state: Arc<CancellationState>,
    claimed: bool,
}

struct AdmissionLedger {
    epoch: Uuid,
    next_ordinal: u64,
    scope_capacity: usize,
    live: HashMap<u64, AdmissionEntry>,
}

impl AdmissionLedger {
    fn new(scope_capacity: usize) -> Self {
        assert!(
            scope_capacity > 0,
            "admission scope capacity must be positive"
        );
        Self {
            epoch: Uuid::now_v7(),
            next_ordinal: 0,
            scope_capacity,
            live: HashMap::new(),
        }
    }

    fn issue(
        &mut self,
        key: CancellationKey,
        state: Arc<CancellationState>,
    ) -> Result<AdmissionLease, ContextError> {
        if self.live.len() >= self.scope_capacity {
            return Err(ContextError::CancellationCapacityExceeded {
                limit: self.scope_capacity,
            });
        }
        let ordinal = self.next_ordinal;
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .expect("admission ordinal exhausted");
        let lease = AdmissionLease {
            epoch: self.epoch,
            ordinal,
        };
        let previous = self.live.insert(
            ordinal,
            AdmissionEntry {
                key,
                state,
                claimed: false,
            },
        );
        assert!(previous.is_none(), "admission ordinal was issued twice");
        Ok(lease)
    }

    fn claim(
        &mut self,
        lease: AdmissionLease,
        key: CancellationKey,
    ) -> Result<Arc<CancellationState>, ContextError> {
        if lease.epoch != self.epoch || lease.ordinal >= self.next_ordinal {
            return Err(ContextError::Cancelled);
        }
        let Some(entry) = self.live.get_mut(&lease.ordinal) else {
            return Err(ContextError::Cancelled);
        };
        if entry.key != key {
            return Err(ContextError::Cancelled);
        }
        if entry.claimed {
            return Err(ContextError::CancellationAlreadyAdmitted);
        }
        entry.claimed = true;
        Ok(Arc::clone(&entry.state))
    }

    fn retire(&mut self, lease: AdmissionLease, state: &Arc<CancellationState>) {
        if lease.epoch != self.epoch {
            return;
        }
        if let Some(entry) = self.live.get(&lease.ordinal)
            && Arc::ptr_eq(&entry.state, state)
        {
            self.live.remove(&lease.ordinal);
        }
    }
}

/// Receiver-owned handle for the same bounded admission registry used by its
/// transport. It carries no cancellation command queue: cancellation is an
/// atomic state transition on the canonical request state.
pub struct CancellationReceiver {
    admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
}

pub fn cancellation_channel(
    scope_capacity: usize,
) -> (CancellationTransport, CancellationReceiver) {
    let admissions = Arc::new(std::sync::Mutex::new(AdmissionLedger::new(scope_capacity)));
    (
        CancellationTransport {
            admissions: Arc::clone(&admissions),
        },
        CancellationReceiver { admissions },
    )
}

impl CancellationTransport {
    fn admissions(&self) -> Arc<std::sync::Mutex<AdmissionLedger>> {
        Arc::clone(&self.admissions)
    }
}

struct CancellationState {
    cancelled: AtomicBool,
    transport_started: AtomicBool,
    transport_active: AtomicBool,
    transport_bound: AtomicBool,
    lease: std::sync::Mutex<Option<AdmissionLease>>,
    canonical: std::sync::Mutex<Option<Arc<CancellationState>>>,
    #[cfg(test)]
    wait_hook: std::sync::Mutex<Option<Arc<CancellationWaitHook>>>,
    changed: tokio::sync::Notify,
}

/// A request-scoped cancellation signal shared by direct calls. Serialized
/// requests preserve the UUID and already-cancelled state; transports use
/// [`CancellationTransport`] for cancellation after admission.
#[derive(Clone)]
pub struct CancellationSignal {
    id: Uuid,
    token: Uuid,
    state: Arc<CancellationState>,
    transport: Option<CancellationTransport>,
}

impl CancellationSignal {
    fn new_with(id: Uuid, token: Uuid, state: Arc<CancellationState>) -> Self {
        Self {
            id,
            token,
            state,
            transport: None,
        }
    }

    pub fn with_transport(transport: CancellationTransport) -> Self {
        Self::with_transport_for(
            Uuid::now_v7(),
            Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                transport_started: AtomicBool::new(false),
                transport_active: AtomicBool::new(false),
                transport_bound: AtomicBool::new(false),
                lease: std::sync::Mutex::new(None),
                canonical: std::sync::Mutex::new(None),
                #[cfg(test)]
                wait_hook: std::sync::Mutex::new(None),
                changed: tokio::sync::Notify::new(),
            }),
            transport,
        )
    }

    fn with_transport_for(
        id: Uuid,
        state: Arc<CancellationState>,
        transport: CancellationTransport,
    ) -> Self {
        state.transport_bound.store(true, Ordering::Release);
        Self {
            id,
            token: Uuid::now_v7(),
            state,
            transport: Some(transport),
        }
    }

    /// The same request state, bound to a caller-owned transport with a fresh
    /// per-request token.
    pub fn bound_to(&self, transport: CancellationTransport) -> Self {
        Self::with_transport_for(self.id, Arc::clone(&self.state), transport)
    }

    pub(crate) fn key(&self) -> CancellationKey {
        CancellationKey {
            request_id: self.id,
            token: self.token,
        }
    }

    pub fn cancel(&self) {
        self.cancel_local();
    }

    /// Starts the caller-owned lifetime of one transport request. The bounded
    /// transport registry returns a typed capacity error rather than creating
    /// untracked ownership. A nontransport signal returns `Ok(None)`.
    pub fn begin_transport_request(
        &self,
    ) -> Result<Option<CancellationRequestGuard>, ContextError> {
        let Some(transport) = &self.transport else {
            return Ok(None);
        };
        assert!(
            self.state
                .transport_started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "a cancellation token cannot be reused for another transport request"
        );
        let lease = transport
            .admissions
            .lock()
            .expect("admission registry mutex")
            .issue(self.key(), Arc::clone(&self.state))?;
        *self.state.lease.lock().expect("cancellation state mutex") = Some(lease);
        self.state.transport_active.store(true, Ordering::Release);
        Ok(Some(CancellationRequestGuard {
            lease,
            state: Arc::clone(&self.state),
            admissions: transport.admissions(),
        }))
    }

    fn cancel_local(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    fn bind_canonical(&self, canonical: Arc<CancellationState>) -> Result<(), ContextError> {
        if Arc::ptr_eq(&self.state, &canonical) {
            return Ok(());
        }
        if canonical
            .canonical
            .lock()
            .expect("canonical state mutex")
            .is_some()
        {
            return Err(ContextError::CancellationAlreadyAdmitted);
        }
        let mut bound = self.state.canonical.lock().expect("canonical state mutex");
        if let Some(existing) = &*bound {
            if Arc::ptr_eq(existing, &canonical) {
                return Ok(());
            }
            return Err(ContextError::CancellationAlreadyAdmitted);
        }
        *bound = Some(canonical);
        drop(bound);
        self.state.changed.notify_waiters();
        Ok(())
    }

    fn canonical(&self) -> Option<Arc<CancellationState>> {
        self.state
            .canonical
            .lock()
            .expect("canonical state mutex")
            .clone()
    }

    #[cfg(test)]
    fn set_wait_hook(&self, hook: Arc<CancellationWaitHook>) {
        *self.state.wait_hook.lock().expect("wait hook mutex") = Some(hook);
    }

    #[cfg(test)]
    async fn pause_wait_hook(&self, point: WaitHookPoint) {
        let hook = self
            .state
            .wait_hook
            .lock()
            .expect("wait hook mutex")
            .clone();
        if let Some(hook) = hook {
            hook.pause(point).await;
        }
    }

    pub async fn cancelled(&self) {
        loop {
            let notified = self.state.changed.notified();
            let canonical = self.canonical();
            if let Some(canonical) = canonical {
                let canonical_notified = canonical.changed.notified();
                if self.is_cancelled() {
                    return;
                }
                #[cfg(test)]
                self.pause_wait_hook(WaitHookPoint::AfterFlagCheck).await;
                #[cfg(test)]
                self.pause_wait_hook(WaitHookPoint::BeforeWait).await;
                tokio::select! {
                    _ = notified => {},
                    _ = canonical_notified => {},
                }
            } else {
                if self.is_cancelled() {
                    return;
                }
                #[cfg(test)]
                self.pause_wait_hook(WaitHookPoint::AfterFlagCheck).await;
                #[cfg(test)]
                self.pause_wait_hook(WaitHookPoint::BeforeWait).await;
                notified.await;
            }
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
            || self
                .canonical()
                .is_some_and(|canonical| canonical.cancelled.load(Ordering::Acquire))
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

/// Terminal ownership fence for a caller-owned transport request.
///
/// Keep this guard alive until the request reaches a terminal result or is
/// deliberately dropped. [`crate::Client`] stores it inside the stream it
/// returns for an IRPC subscription so cancellation remains active through
/// EOF, error, or subscription drop.
pub struct CancellationRequestGuard {
    lease: AdmissionLease,
    state: Arc<CancellationState>,
    admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
}

impl Drop for CancellationRequestGuard {
    fn drop(&mut self) {
        self.state.transport_active.store(false, Ordering::Release);
        self.admissions
            .lock()
            .expect("admission registry mutex")
            .retire(self.lease, &self.state);
    }
}

impl Default for CancellationSignal {
    fn default() -> Self {
        Self::new_with(
            Uuid::now_v7(),
            Uuid::now_v7(),
            Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                transport_started: AtomicBool::new(false),
                transport_active: AtomicBool::new(false),
                transport_bound: AtomicBool::new(false),
                lease: std::sync::Mutex::new(None),
                canonical: std::sync::Mutex::new(None),
                #[cfg(test)]
                wait_hook: std::sync::Mutex::new(None),
                changed: tokio::sync::Notify::new(),
            }),
        )
    }
}

impl Serialize for CancellationSignal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CancellationSignal", 5)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("token", &self.token)?;
        state.serialize_field("cancelled", &self.is_cancelled())?;
        state.serialize_field(
            "transport_bound",
            &self.state.transport_bound.load(Ordering::Acquire),
        )?;
        state.serialize_field(
            "admission",
            &*self.state.lease.lock().expect("cancellation state mutex"),
        )?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for CancellationSignal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            id: Uuid,
            token: Uuid,
            cancelled: bool,
            transport_bound: bool,
            admission: Option<AdmissionLease>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Ok(Self::new_with(
            wire.id,
            wire.token,
            Arc::new(CancellationState {
                cancelled: AtomicBool::new(wire.cancelled),
                transport_started: AtomicBool::new(false),
                transport_active: AtomicBool::new(false),
                transport_bound: AtomicBool::new(wire.transport_bound),
                lease: std::sync::Mutex::new(wire.admission),
                canonical: std::sync::Mutex::new(None),
                #[cfg(test)]
                wait_hook: std::sync::Mutex::new(None),
                changed: tokio::sync::Notify::new(),
            }),
        ))
    }
}

impl fmt::Debug for CancellationSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationSignal")
            .field("id", &self.id)
            .field("token", &self.token)
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl PartialEq for CancellationSignal {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key() && self.is_cancelled() == other.is_cancelled()
    }
}

impl Eq for CancellationSignal {}

/// Shared admission counters kept by the IRPC adapter. Guards decrement their
/// counter on every exit path.
#[derive(Default)]
pub struct TaskActivity {
    admitted: std::sync::atomic::AtomicUsize,
    peak_admitted: std::sync::atomic::AtomicUsize,
    spawned: std::sync::atomic::AtomicUsize,
    peak_spawned: std::sync::atomic::AtomicUsize,
    active: std::sync::atomic::AtomicUsize,
    peak_active: std::sync::atomic::AtomicUsize,
    stream_pumps: std::sync::atomic::AtomicUsize,
    peak_stream_pumps: std::sync::atomic::AtomicUsize,
    changed: tokio::sync::Notify,
}

pub struct TaskGuard {
    activity: std::sync::Arc<TaskActivity>,
    counter: TaskCounter,
}

#[derive(Clone, Copy)]
enum TaskCounter {
    Admitted,
    Spawned,
    Active,
    StreamPumps,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        let counter = match self.counter {
            TaskCounter::Admitted => &self.activity.admitted,
            TaskCounter::Spawned => &self.activity.spawned,
            TaskCounter::Active => &self.activity.active,
            TaskCounter::StreamPumps => &self.activity.stream_pumps,
        };
        counter.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.activity.changed.notify_one();
    }
}

impl TaskActivity {
    fn guard(activity: std::sync::Arc<Self>, kind: TaskCounter) -> TaskGuard {
        let (counter, peak) = match kind {
            TaskCounter::Admitted => (&activity.admitted, &activity.peak_admitted),
            TaskCounter::Spawned => (&activity.spawned, &activity.peak_spawned),
            TaskCounter::Active => (&activity.active, &activity.peak_active),
            TaskCounter::StreamPumps => (&activity.stream_pumps, &activity.peak_stream_pumps),
        };
        let count = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        peak.fetch_max(count, std::sync::atomic::Ordering::SeqCst);
        activity.changed.notify_one();
        TaskGuard {
            activity,
            counter: kind,
        }
    }

    pub fn admitted(self: &std::sync::Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::Admitted)
    }

    pub fn spawned(self: &std::sync::Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::Spawned)
    }

    pub fn active(self: &std::sync::Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::Active)
    }

    pub fn stream_pump(self: &std::sync::Arc<Self>) -> TaskGuard {
        Self::guard(Arc::clone(self), TaskCounter::StreamPumps)
    }

    pub fn admitted_count(&self) -> usize {
        self.admitted.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn spawned_count(&self) -> usize {
        self.spawned.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn active_count(&self) -> usize {
        self.active.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn peak_count(&self) -> usize {
        self.peak_active.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn stream_pump_count(&self) -> usize {
        self.stream_pumps.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn peak_stream_pump_count(&self) -> usize {
        self.peak_stream_pumps
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn wait_for_change(&self) {
        self.changed.notified().await;
    }
}

/// Receiver-side admission for transported requests. It owns the unary and
/// stream permits, the activity counters, and the registry of admitted
/// cancellation scopes; the adapter that spawns the work owns the tasks.
///
/// Cancellation admission is backed by the transport-owned live lease map.
/// Registrations held here are bounded separately by the unary and stream
/// permits; producer/queued ownership is bounded by the configured scope
/// capacity supplied to [`cancellation_channel`].
struct CancellationRegistry {
    admitted: HashMap<CancellationKey, CancellationSignal>,
}

pub struct Admission {
    unary_permits: std::sync::Arc<tokio::sync::Semaphore>,
    stream_permits: std::sync::Arc<tokio::sync::Semaphore>,
    activity: std::sync::Arc<TaskActivity>,
    cancellations: std::sync::Arc<std::sync::Mutex<CancellationRegistry>>,
    admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
    #[cfg(test)]
    ordinary_admission_hook: Arc<std::sync::Mutex<Option<Arc<OrdinaryAdmissionHook>>>>,
    max_handlers: usize,
    max_stream_pumps: usize,
}

/// Removes one admitted cancellation scope when its child exits.
///
/// This retains only registry state, not the admission, so it cannot form an
/// ownership cycle back to the adapter that spawned the child.
pub struct CancellationRegistration {
    registry: std::sync::Arc<std::sync::Mutex<CancellationRegistry>>,
    key: CancellationKey,
    state: Arc<CancellationState>,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().expect("owned cancellation mutex");
        if registry
            .admitted
            .get(&self.key)
            .is_some_and(|signal| Arc::ptr_eq(&signal.state, &self.state))
        {
            registry.admitted.remove(&self.key);
        }
    }
}

impl Admission {
    pub fn new(max_handlers: usize, max_stream_pumps: usize) -> std::sync::Arc<Self> {
        let scope_capacity = max_handlers.saturating_add(max_stream_pumps).max(1);
        Self::with_admissions(
            max_handlers,
            max_stream_pumps,
            Arc::new(std::sync::Mutex::new(AdmissionLedger::new(scope_capacity))),
        )
    }

    fn with_admissions(
        max_handlers: usize,
        max_stream_pumps: usize,
        admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
    ) -> std::sync::Arc<Self> {
        assert!(max_handlers > 0 && max_stream_pumps > 0);
        std::sync::Arc::new(Self {
            unary_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(max_handlers)),
            stream_permits: std::sync::Arc::new(tokio::sync::Semaphore::new(max_stream_pumps)),
            activity: std::sync::Arc::new(TaskActivity::default()),
            cancellations: std::sync::Arc::new(std::sync::Mutex::new(CancellationRegistry {
                admitted: HashMap::new(),
            })),
            admissions,
            #[cfg(test)]
            ordinary_admission_hook: Arc::new(std::sync::Mutex::new(None)),
            max_handlers,
            max_stream_pumps,
        })
    }

    /// Creates an admission using the bounded registry owned by its
    /// cancellation receiver. No watcher task or command queue is needed:
    /// registration binds receiver state directly to the canonical producer
    /// state.
    pub fn with_cancellation(
        max_handlers: usize,
        max_stream_pumps: usize,
        receiver: CancellationReceiver,
    ) -> std::sync::Arc<Self> {
        Self::with_admissions(max_handlers, max_stream_pumps, receiver.admissions)
    }

    #[cfg(test)]
    fn set_ordinary_admission_hook(&self, hook: Arc<OrdinaryAdmissionHook>) {
        *self
            .ordinary_admission_hook
            .lock()
            .expect("ordinary admission hook mutex") = Some(hook);
    }

    #[cfg(test)]
    fn pause_ordinary_admission(&self) {
        let hook = self
            .ordinary_admission_hook
            .lock()
            .expect("ordinary admission hook mutex")
            .clone();
        if let Some(hook) = hook {
            hook.pause_first();
        }
    }

    #[doc(hidden)]
    pub fn register_cancellation(
        self: &std::sync::Arc<Self>,
        signal: CancellationSignal,
    ) -> Result<CancellationRegistration, ContextError> {
        let key = signal.key();
        let transport_bound = signal.state.transport_bound.load(Ordering::Acquire);
        let mut registry = self.cancellations.lock().expect("owned cancellation mutex");
        if registry.admitted.contains_key(&key) {
            signal.cancel_local();
            return Err(ContextError::CancellationAlreadyAdmitted);
        }
        #[cfg(test)]
        if !transport_bound {
            self.pause_ordinary_admission();
        }

        if transport_bound {
            let lease = match signal
                .state
                .lease
                .lock()
                .expect("cancellation state mutex")
                .as_ref()
                .copied()
            {
                Some(lease) => lease,
                None => {
                    signal.cancel_local();
                    return Err(ContextError::Cancelled);
                }
            };
            let canonical = match self
                .admissions
                .lock()
                .expect("admission registry mutex")
                .claim(lease, key)
            {
                Ok(canonical) => canonical,
                Err(error) => {
                    signal.cancel_local();
                    return Err(error);
                }
            };
            if let Err(error) = signal.bind_canonical(canonical) {
                signal.cancel_local();
                return Err(error);
            }
        } else if signal
            .state
            .lease
            .lock()
            .expect("cancellation state mutex")
            .is_some()
        {
            signal.cancel_local();
            return Err(ContextError::Cancelled);
        }

        registry.admitted.insert(key, signal.clone());
        Ok(CancellationRegistration {
            registry: std::sync::Arc::clone(&self.cancellations),
            key,
            state: signal.state,
        })
    }

    pub async fn acquire_unary(&self) -> tokio::sync::OwnedSemaphorePermit {
        std::sync::Arc::clone(&self.unary_permits)
            .acquire_owned()
            .await
            .expect("owned IRPC unary semaphore lives")
    }

    pub fn try_acquire_stream(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        std::sync::Arc::clone(&self.stream_permits)
            .try_acquire_owned()
            .ok()
    }

    pub fn activity(&self) -> std::sync::Arc<TaskActivity> {
        std::sync::Arc::clone(&self.activity)
    }

    pub fn max_handlers(&self) -> usize {
        self.max_handlers
    }

    pub fn max_stream_pumps(&self) -> usize {
        self.max_stream_pumps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::RequestContext;

    #[tokio::test]
    async fn terminal_lease_rejects_delayed_serialized_admission_after_cancel_and_drop() {
        let (transport, receiver) = cancellation_channel(4);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let producer = RequestContext::root().with_cancellation_transport(transport);
        let _request = producer
            .cancellation
            .begin_transport_request()
            .expect("begin transport scope")
            .expect("transport scope");
        let wire = serde_json::to_vec(&producer).expect("request context serializes");
        producer.cancel();
        drop(_request);

        let delayed = serde_json::from_slice::<RequestContext>(&wire)
            .expect("delayed request context deserializes");
        assert!(
            adapter
                .register_cancellation(delayed.cancellation.clone())
                .is_err()
        );
        assert!(delayed.cancellation.is_cancelled());

        let replay = serde_json::from_slice::<RequestContext>(&wire)
            .expect("replayed request context deserializes");
        assert!(adapter.register_cancellation(replay.cancellation).is_err());
    }

    #[tokio::test]
    async fn alternating_delayed_admissions_remain_bounded() {
        let (transport, receiver) = cancellation_channel(2);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let oldest = RequestContext::root().with_cancellation_transport(transport.clone());
        let _oldest_guard = oldest
            .cancellation
            .begin_transport_request()
            .expect("begin oldest scope")
            .expect("oldest transport scope");

        for _ in 0..128 {
            let producer = RequestContext::root().with_cancellation_transport(transport.clone());
            let guard = producer
                .cancellation
                .begin_transport_request()
                .expect("begin later scope")
                .expect("later transport scope");
            let wire = serde_json::to_vec(&producer).expect("later context serializes");
            drop(guard);
            let delayed = serde_json::from_slice::<RequestContext>(&wire)
                .expect("later context deserializes");
            assert!(adapter.register_cancellation(delayed.cancellation).is_err());
            assert_eq!(
                adapter
                    .admissions
                    .lock()
                    .expect("admission registry mutex")
                    .live
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn three_queued_cancellation_scopes_do_not_panic() {
        let (transport, receiver) = cancellation_channel(4);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let mut queued = Vec::new();
        for _ in 0..3 {
            let producer = RequestContext::root().with_cancellation_transport(transport.clone());
            let guard = producer
                .cancellation
                .begin_transport_request()
                .expect("begin queued scope")
                .expect("queued transport scope");
            producer.cancel();
            let wire = serde_json::to_vec(&producer).expect("queued context serializes");
            queued.push((wire, guard));
        }
        for (wire, guard) in queued {
            let delayed = serde_json::from_slice::<RequestContext>(&wire)
                .expect("queued context deserializes");
            let registration = adapter
                .register_cancellation(delayed.cancellation.clone())
                .expect("queued scope admission");
            assert!(delayed.cancellation.is_cancelled());
            drop(registration);
            drop(guard);
        }

        let (transport, _receiver) = cancellation_channel(1);
        let first = CancellationSignal::with_transport(transport.clone());
        let first_guard = first
            .begin_transport_request()
            .expect("first scope result")
            .expect("first scope");
        let second = CancellationSignal::with_transport(transport);
        assert!(matches!(
            second.begin_transport_request(),
            Err(ContextError::CancellationCapacityExceeded { limit: 1 })
        ));
        drop(first_guard);
    }

    #[test]
    fn dropped_scopes_do_not_make_later_cancel_panic() {
        let (transport, _receiver) = cancellation_channel(1);
        for _ in 0..64 {
            let signal = CancellationSignal::with_transport(transport.clone());
            let guard = signal
                .begin_transport_request()
                .expect("scope result")
                .expect("transport scope");
            drop(guard);
        }
        let signal = CancellationSignal::with_transport(transport);
        let guard = signal
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        signal.cancel();
        drop(guard);
    }

    #[tokio::test]
    async fn lease_validation_fails_closed_but_nontransport_contexts_remain_valid() {
        let (transport, receiver) = cancellation_channel(4);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let producer = RequestContext::root().with_cancellation_transport(transport);
        let _guard = producer
            .cancellation
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        let wire = serde_json::to_value(&producer).expect("context serializes");

        for mutation in [
            ("epoch", serde_json::json!(Uuid::now_v7().to_string())),
            ("ordinal", serde_json::json!(u64::MAX)),
        ] {
            let mut value = wire.clone();
            value["cancellation"]["admission"][mutation.0] = mutation.1;
            let malformed = serde_json::from_value::<RequestContext>(value)
                .expect("mutated context deserializes");
            assert!(
                adapter
                    .register_cancellation(malformed.cancellation)
                    .is_err()
            );
        }

        let mut missing = wire.clone();
        missing["cancellation"]["admission"] = serde_json::Value::Null;
        let missing = serde_json::from_value::<RequestContext>(missing)
            .expect("missing lease context deserializes");
        assert!(adapter.register_cancellation(missing.cancellation).is_err());

        let ordinary = RequestContext::root();
        let ordinary_registration = adapter
            .register_cancellation(ordinary.cancellation)
            .expect("ordinary nontransport registration");
        drop(ordinary_registration);
    }

    #[tokio::test]
    async fn same_state_and_serde_detached_admissions_share_canonical_cancellation() {
        let (transport, receiver) = cancellation_channel(4);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let producer = RequestContext::root().with_cancellation_transport(transport.clone());
        let _guard = producer
            .cancellation
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");

        let same_state = adapter
            .register_cancellation(producer.cancellation.clone())
            .expect("same-state registration");
        producer.cancel();
        assert!(producer.cancellation.is_cancelled());
        drop(same_state);
        drop(_guard);

        let producer = RequestContext::root().with_cancellation_transport(transport);
        let guard = producer
            .cancellation
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        let wire = serde_json::to_vec(&producer).expect("producer serializes");
        let detached =
            serde_json::from_slice::<RequestContext>(&wire).expect("detached context deserializes");
        let registration = adapter
            .register_cancellation(detached.cancellation.clone())
            .expect("detached registration");
        producer.cancel();
        assert!(detached.cancellation.is_cancelled());
        drop(registration);
        drop(guard);
    }

    #[tokio::test]
    async fn producer_drop_does_not_cancel_admitted_work() {
        let (transport, receiver) = cancellation_channel(2);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let producer = RequestContext::root().with_cancellation_transport(transport);
        let guard = producer
            .cancellation
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        let wire = serde_json::to_vec(&producer).expect("producer serializes");
        let detached =
            serde_json::from_slice::<RequestContext>(&wire).expect("detached context deserializes");
        let registration = adapter
            .register_cancellation(detached.cancellation.clone())
            .expect("admitted work");
        let producer_state = Arc::downgrade(&producer.cancellation.state);
        drop(guard);
        drop(producer);
        assert!(
            producer_state.upgrade().is_some(),
            "admission retains the canonical producer state after producer drop"
        );
        assert!(!detached.cancellation.is_cancelled());
        drop(registration);
        drop(detached);
        assert!(
            producer_state.upgrade().is_none(),
            "canonical state retires after registration and detached signal drop"
        );
    }

    #[test]
    fn ordinary_duplicate_registration_is_atomic() {
        let adapter = Admission::new(2, 1);
        let context = RequestContext::root();
        let wire = serde_json::to_vec(&context).expect("ordinary context serializes");
        let first = serde_json::from_slice::<RequestContext>(&wire)
            .expect("first ordinary context deserializes")
            .cancellation;
        let second = serde_json::from_slice::<RequestContext>(&wire)
            .expect("second ordinary context deserializes")
            .cancellation;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        adapter.set_ordinary_admission_hook(Arc::new(OrdinaryAdmissionHook {
            first: AtomicBool::new(false),
            ready: std::sync::Mutex::new(Some(ready_tx)),
            release: std::sync::Mutex::new(Some(release_rx)),
        }));

        let first_adapter = Arc::clone(&adapter);
        let first_task = std::thread::spawn(move || first_adapter.register_cancellation(first));
        ready_rx
            .recv()
            .expect("first registration reached post-decision checkpoint");

        let (contender_started_tx, contender_started_rx) = std::sync::mpsc::channel();
        let second_adapter = Arc::clone(&adapter);
        let second_task = std::thread::spawn(move || {
            contender_started_tx
                .send(())
                .expect("ordinary contender remains open");
            second_adapter.register_cancellation(second)
        });
        contender_started_rx
            .recv()
            .expect("ordinary contender started");
        assert!(
            adapter.cancellations.try_lock().is_err(),
            "the post-decision checkpoint must retain the registry mutex"
        );

        release_tx.send(()).expect("first registration release");
        let first_result = first_task.join().expect("first registration thread");
        let second_result = second_task.join().expect("second registration thread");
        assert!(first_result.is_ok(), "first ordinary admission wins");
        assert!(
            matches!(
                second_result,
                Err(ContextError::CancellationAlreadyAdmitted)
            ),
            "second ordinary admission must lose to the first"
        );
    }

    #[tokio::test]
    async fn cancellation_waiter_registers_before_final_flag_check() {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let hook = Arc::new(CancellationWaitHook {
            point: WaitHookPoint::AfterFlagCheck,
            gates: std::sync::Mutex::new(std::collections::VecDeque::from([WaitGate {
                ready: ready_tx,
                release: Arc::clone(&release),
            }])),
        });
        let signal = CancellationSignal::default();
        signal.set_wait_hook(hook);
        let mut waiter = Box::pin(signal.cancelled());
        let waker = futures::task::noop_waker_ref();
        let mut context = std::task::Context::from_waker(waker);
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
        ready_rx.await.expect("waiter reached synchronization seam");
        signal.cancel();
        release.notify_one();
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Ready(())
        ));

        let (transport, receiver) = cancellation_channel(2);
        let producer = CancellationSignal::with_transport(transport);
        let producer_guard = producer
            .begin_transport_request()
            .expect("canonical transport admission")
            .expect("canonical transport scope");
        let wire = serde_json::to_vec(&producer).expect("canonical signal serializes");
        let detached = serde_json::from_slice::<CancellationSignal>(&wire)
            .expect("canonical signal deserializes");
        let adapter = Admission::with_cancellation(2, 1, receiver);
        let registration = adapter
            .register_cancellation(detached.clone())
            .expect("canonical signal admission");
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        detached.set_wait_hook(Arc::new(CancellationWaitHook {
            point: WaitHookPoint::AfterFlagCheck,
            gates: std::sync::Mutex::new(std::collections::VecDeque::from([WaitGate {
                ready: ready_tx,
                release: Arc::clone(&release),
            }])),
        }));
        let mut waiter = Box::pin(detached.cancelled());
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
        ready_rx
            .await
            .expect("canonical waiter reached synchronization seam");
        producer.cancel();
        release.notify_one();
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Ready(())
        ));
        drop(registration);
        drop(producer_guard);
    }

    #[tokio::test]
    async fn binding_wakes_a_detached_waiter_already_waiting_locally() {
        let (transport, receiver) = cancellation_channel(2);
        let producer = CancellationSignal::with_transport(transport);
        let producer_guard = producer
            .begin_transport_request()
            .expect("transport admission")
            .expect("transport signal");
        let wire = serde_json::to_vec(&producer).expect("transport signal serializes");
        let detached = serde_json::from_slice::<CancellationSignal>(&wire)
            .expect("detached signal deserializes");
        let adapter = Admission::with_cancellation(2, 1, receiver);

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let release = Arc::new(tokio::sync::Notify::new());
        detached.set_wait_hook(Arc::new(CancellationWaitHook {
            point: WaitHookPoint::BeforeWait,
            gates: std::sync::Mutex::new(std::collections::VecDeque::from([WaitGate {
                ready: ready_tx,
                release: Arc::clone(&release),
            }])),
        }));
        let waiter_signal = detached.clone();
        let mut waiter = Box::pin(waiter_signal.cancelled());
        let waker = futures::task::noop_waker_ref();
        let mut context = std::task::Context::from_waker(waker);
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Pending
        ));
        ready_rx.await.expect("detached waiter reached local wait");
        release.notify_one();
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Pending
        ));

        let registration = adapter
            .register_cancellation(detached)
            .expect("detached admission binds");
        assert!(!producer.is_cancelled());
        producer.cancel();
        assert!(matches!(
            std::future::Future::poll(waiter.as_mut(), &mut context),
            std::task::Poll::Ready(())
        ));
        drop(registration);
        drop(producer_guard);
    }

    #[tokio::test]
    async fn cancellation_before_admission_is_consumed_by_matching_registration() {
        let (transport, receiver) = cancellation_channel(4);
        let adapter = Admission::with_cancellation(1, 1, receiver);
        let signal = CancellationSignal::with_transport(transport);
        let _request = signal
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        signal.cancel();

        let registration = adapter
            .register_cancellation(signal.clone())
            .expect("cancellation registration");
        assert!(signal.is_cancelled());
        drop(registration);
    }

    #[tokio::test]
    async fn completed_transport_scope_ignores_late_cancellation() {
        let (transport, _receiver) = cancellation_channel(1);
        let signal = CancellationSignal::with_transport(transport);
        let request = signal
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        drop(request);
        signal.cancel();
        assert!(signal.is_cancelled());
    }

    #[tokio::test]
    async fn unmatched_cancellation_does_not_exhaust_registry_capacity() {
        let (transport, receiver) = cancellation_channel(1);
        let adapter = Admission::with_cancellation(1, 1, receiver);

        for _ in 0..4 {
            let signal = CancellationSignal::with_transport(transport.clone());
            let guard = signal
                .begin_transport_request()
                .expect("scope result")
                .expect("transport scope");
            signal.cancel();
            let registration = adapter
                .register_cancellation(signal)
                .expect("cancellation registration");
            drop(registration);
            drop(guard);
        }
    }
}
