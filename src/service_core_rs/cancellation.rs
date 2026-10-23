use crate::ContextError;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CancellationKey {
    pub request_id: Uuid,
    pub token: Uuid,
}

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

pub struct CancellationReceiver {
    admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
    cancellations: Arc<std::sync::Mutex<HashMap<CancellationKey, CancellationSignal>>>,
    #[cfg(test)]
    ordinary_admission_hook: std::sync::Mutex<Option<Arc<OrdinaryAdmissionHook>>>,
}

pub fn cancellation_channel(
    scope_capacity: usize,
) -> (CancellationTransport, CancellationReceiver) {
    let admissions = Arc::new(std::sync::Mutex::new(AdmissionLedger::new(scope_capacity)));
    (
        CancellationTransport {
            admissions: Arc::clone(&admissions),
        },
        CancellationReceiver {
            admissions,
            cancellations: Arc::new(std::sync::Mutex::new(HashMap::new())),
            #[cfg(test)]
            ordinary_admission_hook: std::sync::Mutex::new(None),
        },
    )
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
        let mut signal = Self::default();
        signal.state.transport_bound.store(true, Ordering::Release);
        signal.transport = Some(transport);
        signal
    }

    pub fn bound_to(&self, transport: CancellationTransport) -> Self {
        self.state.transport_bound.store(true, Ordering::Release);
        Self {
            id: self.id,
            token: Uuid::now_v7(),
            state: Arc::clone(&self.state),
            transport: Some(transport),
        }
    }

    fn key(&self) -> CancellationKey {
        CancellationKey {
            request_id: self.id,
            token: self.token,
        }
    }

    pub fn cancel(&self) {
        self.cancel_local();
    }

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
            admissions: Arc::clone(&transport.admissions),
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
                tokio::select! { _ = notified => {}, _ = canonical_notified => {}, }
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

pub struct CancellationRegistration {
    registry: Arc<std::sync::Mutex<HashMap<CancellationKey, CancellationSignal>>>,
    key: CancellationKey,
    state: Arc<CancellationState>,
}

impl Drop for CancellationRegistration {
    fn drop(&mut self) {
        let mut registry = self.registry.lock().expect("owned cancellation mutex");
        if registry
            .get(&self.key)
            .is_some_and(|signal| Arc::ptr_eq(&signal.state, &self.state))
        {
            registry.remove(&self.key);
        }
    }
}

impl CancellationReceiver {
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

    pub fn register_cancellation(
        &self,
        signal: CancellationSignal,
    ) -> Result<CancellationRegistration, ContextError> {
        let key = signal.key();
        let transport_bound = signal.state.transport_bound.load(Ordering::Acquire);
        let mut registry = self.cancellations.lock().expect("owned cancellation mutex");
        if registry.contains_key(&key) {
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
        let previous = registry.insert(key, signal.clone());
        assert!(previous.is_none(), "cancellation key was admitted twice");
        Ok(CancellationRegistration {
            registry: Arc::clone(&self.cancellations),
            key,
            state: signal.state,
        })
    }
}

#[cfg(test)]
mod tests;
