//! Shared runtime vocabulary for typed services whose canonical interface is
//! XRPC over HTTP, with direct and IRPC transports as local optimizations.
//!
//! Concrete services declare their operations and own their domain types. This
//! module contains only transport-neutral request context and XRPC metadata.
//!
//! ## External generator contract
//!
//! `declare_typed_service!` emits the typed client, XRPC descriptors, and the
//! native IRPC protocol from one operation declaration. Framework-owned paths
//! use `$crate` and library reexports, so renaming this package (for example to
//! `renamed_utils`) does not change generated paths. The `serde_module` name in
//! each declaration must be unique when multiple declarations share a module;
//! it scopes the serde crate override used by generated derives.
//!
//! The full IRPC expansion also requires these direct consumer dependencies:
//!
//! ```toml
//! [dependencies]
//! renamed_utils = { package = "utils_rs", version = "0.1", default-features = false, features = ["native", "downloader"] }
//! serde = { version = "1", features = ["derive"] }
//! irpc = "0.17"
//! tracing = "0.1"
//! ```
//!
//! `irpc 0.17`'s upstream `rpc_requests` proc macro emits absolute
//! `::irpc`, `::serde`, and `::tracing` paths and provides no crate-path
//! override, so those three crates are part of the supported full-generator
//! consumer contract. They are required by the upstream expansion, not by
//! domain handlers; other application dependencies remain application-specific
//! and are not implied by this declaration. Async adapter shutdown joins owned
//! children and observes failures; [`OwnedIrpcAdapter`]'s `Drop` only requests
//! abort because synchronous `Drop` cannot join. HTTP/Iroh transport wiring and
//! independent-clock deadline semantics remain separate integration gates; the
//! native UUID cancellation path is scoped to each adapter's admitted work.

use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[doc(hidden)]
pub use irpc as __irpc;
#[doc(hidden)]
pub use serde as __serde;
#[doc(hidden)]
pub use tokio as __tokio;
#[doc(hidden)]
pub use tracing as __tracing;

pub mod sample;

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

    fn key(&self) -> CancellationKey {
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
/// deliberately dropped. The generated IRPC stream client stores it in the
/// returned subscription so stream cancellation remains active through EOF,
/// error, or subscription drop.
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

/// Errors found before a service handler is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ContextError {
    #[error("request context is missing a request id")]
    MissingRequestId,
    #[error("request context is missing a trace id")]
    MissingTraceId,
    #[error("request was cancelled")]
    Cancelled,
    #[error("request cancellation key is already admitted")]
    CancellationAlreadyAdmitted,
    #[error("request cancellation scope capacity exhausted (limit {limit})")]
    CancellationCapacityExceeded { limit: usize },
    #[error("request exceeded its time budget")]
    DeadlineExceeded,
}

/// Errors at the typed client boundary keep transport failure distinct from a
/// semantic operation error. A procedure transport failure is retry-unsafe
/// because the server may have committed before its response was lost.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError<E> {
    #[error("transport failed: {message}")]
    Transport {
        message: String,
        outcome_unknown: bool,
    },
    #[error("operation failed: {0}")]
    Operation(E),
}

impl<E> ClientError<E> {
    pub fn transport(error: impl fmt::Display, outcome_unknown: bool) -> Self {
        Self::Transport {
            message: error.to_string(),
            outcome_unknown,
        }
    }
}

/// Transport metadata carried beside domain parameters. HTTP adapters map these
/// fields to headers; native callers carry the same typed values without putting
/// them into domain JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpMetadata {
    pub authorization: Option<String>,
    pub idempotency_key: Option<String>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    pub timeout: Option<Duration>,
}

/// Propagated state for a native request. This is the small native equivalent
/// of the HTTP metadata currently shared by every transport. `time_budget` is
/// the serializable transport value; `deadline` is rebuilt by a receiver when
/// it accepts the envelope locally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub trace_id: Uuid,
    pub span_id: Uuid,
    pub parent_span_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub time_budget: Option<Duration>,
    pub cancellation: CancellationSignal,
    /// Absolute wall-clock deadline carried across serialized boundaries.
    /// Receiver-local `Instant` state is rebuilt from this value.
    deadline_unix_ms: Option<u64>,
    #[serde(skip)]
    deadline: Option<Instant>,
}

impl RequestContext {
    pub fn root() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            trace_id: Uuid::now_v7(),
            span_id: Uuid::now_v7(),
            parent_span_id: None,
            command_id: None,
            time_budget: None,
            cancellation: CancellationSignal::default(),
            deadline_unix_ms: None,
            deadline: None,
        }
    }

    pub fn child(&self) -> Self {
        Self {
            request_id: self.request_id,
            trace_id: self.trace_id,
            span_id: Uuid::now_v7(),
            parent_span_id: Some(self.span_id),
            command_id: self.command_id,
            time_budget: self.time_budget,
            cancellation: self.cancellation.clone(),
            deadline_unix_ms: self.deadline_unix_ms,
            deadline: self.deadline,
        }
    }

    pub fn with_command_id(mut self, command_id: Uuid) -> Self {
        self.command_id = Some(command_id);
        self
    }

    pub fn with_time_budget(mut self, time_budget: Duration) -> Self {
        self.time_budget = Some(time_budget);
        self.deadline = Some(Instant::now() + time_budget);
        self.deadline_unix_ms =
            Some(wall_clock_millis().saturating_add(time_budget.as_millis() as u64));
        self
    }

    /// Rebuilds receiver-local deadline state without resetting elapsed budget.
    pub fn for_receiver(mut self) -> Self {
        self.deadline = self.deadline_unix_ms.map(|deadline| {
            let remaining = deadline.saturating_sub(wall_clock_millis());
            Instant::now() + Duration::from_millis(remaining)
        });
        self
    }

    /// Attaches a caller-owned cancellation transport. Generated IRPC clients
    /// open and close the request scope around their call; a caller that
    /// serializes a context separately must hold
    /// [`CancellationSignal::begin_transport_request`] until that request's
    /// terminal result or drop.
    pub fn with_cancellation_transport(mut self, transport: CancellationTransport) -> Self {
        self.cancellation = CancellationSignal::with_transport_for(
            self.cancellation.id,
            Arc::clone(&self.cancellation.state),
            transport,
        );
        self
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn validate(&self) -> Result<(), ContextError> {
        if self.request_id.is_nil() {
            return Err(ContextError::MissingRequestId);
        }
        if self.trace_id.is_nil() {
            return Err(ContextError::MissingTraceId);
        }
        if self.cancellation.is_cancelled() {
            return Err(ContextError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
            || self
                .deadline_unix_ms
                .is_some_and(|deadline| wall_clock_millis() >= deadline)
        {
            return Err(ContextError::DeadlineExceeded);
        }
        Ok(())
    }
}

fn wall_clock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_millis() as u64
}

/// Native request envelope used by direct and IRPC transports. An HTTP adapter
/// maps request metadata into `context` and the Lexicon input into `params`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request<P> {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub params: P,
}

impl<P> Request<P> {
    pub fn new(context: RequestContext, params: P) -> Self {
        Self {
            context,
            metadata: HttpMetadata::default(),
            params,
        }
    }

    pub fn with_metadata(context: RequestContext, metadata: HttpMetadata, params: P) -> Self {
        Self {
            context,
            metadata,
            params,
        }
    }

    /// Rebuilds receiver-local deadline state after a transport boundary.
    pub fn for_receiver(mut self) -> Self {
        self.context = self.context.for_receiver();
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum XrpcKind {
    Query,
    Procedure,
    Subscription,
}

impl XrpcKind {
    pub const fn transport_outcome_unknown(self) -> bool {
        matches!(self, Self::Procedure | Self::Subscription)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMethod {
    Get,
    Post,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpInput {
    Query,
    JsonBody,
}

/// Canonical external identity and HTTP/schema mapping for an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct XrpcOperation {
    pub nsid: &'static str,
    pub kind: XrpcKind,
    pub method: HttpMethod,
    pub input: HttpInput,
    pub output_schema: &'static str,
    pub error_schema: &'static str,
}

/// Transport-independent HTTP request produced from a typed XRPC call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub context: RequestContext,
    pub metadata: HttpMetadata,
    pub operation: XrpcOperation,
    pub query: Option<serde_json::Value>,
    pub body: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
#[error("XRPC transport failed: {message}")]
pub struct XrpcTransportError {
    pub message: String,
}

/// Shared typed dispatch seam implemented by each concrete service.
pub trait UnaryServiceDispatch<Operation, Params, Output, Error>: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<Params>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Output, Error>> + Send + 'a>>;
}

/// Shared typed dispatch seam for subscription implementations. The generated
/// transport layer owns transport-specific receivers; services only provide the
/// validated domain subscription returned by this hook.
pub trait StreamServiceDispatch<Operation, Params, Subscription, Error>: Send + Sync {
    fn dispatch<'a>(
        &'a self,
        operation: Operation,
        request: Request<Params>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Subscription, Error>> + Send + 'a>>;
}

/// Shared admission counters used by generated IRPC dispatch. Guards decrement
/// their counter on every exit path, while completed handles can be reaped.
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

/// Shared owned lifecycle for generated IRPC adapters. It owns admission
/// permits, instrumentation, and every spawned child handle so consumers only
/// provide their domain service and operation rows.
///
/// Cancellation admission is backed by the transport-owned live lease map.
/// Adapter-held registrations are bounded separately by the unary and stream
/// permits; producer/queued ownership is bounded by the configured scope
/// capacity supplied to `cancellation_channel`.
struct CancellationRegistry {
    admitted: HashMap<CancellationKey, CancellationSignal>,
}

pub struct OwnedIrpcAdapter {
    unary_permits: std::sync::Arc<tokio::sync::Semaphore>,
    stream_permits: std::sync::Arc<tokio::sync::Semaphore>,
    activity: std::sync::Arc<TaskActivity>,
    child_tasks: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    cancellations: std::sync::Arc<std::sync::Mutex<CancellationRegistry>>,
    admissions: Arc<std::sync::Mutex<AdmissionLedger>>,
    #[cfg(test)]
    ordinary_admission_hook: Arc<std::sync::Mutex<Option<Arc<OrdinaryAdmissionHook>>>>,
    max_handlers: usize,
    max_stream_pumps: usize,
}

/// Removes one admitted cancellation scope when its generated child exits.
///
/// This retains only registry state, not the adapter. Generated children are
/// retained by the adapter's join-handle list, so this registration cannot form
/// an ownership cycle back to the adapter.
#[doc(hidden)]
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

impl OwnedIrpcAdapter {
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
            child_tasks: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
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

    /// Creates an adapter using the bounded registry owned by its cancellation
    /// receiver. No watcher task or command queue is needed: registration binds
    /// receiver state directly to the canonical producer state.
    pub fn new_with_cancellation(
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

    pub fn register_child(&self, task: tokio::task::JoinHandle<()>) {
        self.child_tasks
            .lock()
            .expect("owned IRPC child-task mutex")
            .push(task);
    }

    pub fn child_count(&self) -> usize {
        self.child_tasks
            .lock()
            .expect("owned IRPC child-task mutex")
            .len()
    }

    /// Reaps finished children between admissions and surfaces unexpected
    /// panics immediately. Finished handles that race this scan are reaped on
    /// the next admission or during shutdown.
    pub async fn reap_completed(&self) {
        let completed = {
            let mut tasks = self
                .child_tasks
                .lock()
                .expect("owned IRPC child-task mutex");
            let mut completed = Vec::new();
            let mut remaining = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                if task.is_finished() {
                    completed.push(task);
                } else {
                    remaining.push(task);
                }
            }
            *tasks = remaining;
            completed
        };
        for task in completed {
            task.await.expect("owned IRPC child task must not panic");
        }
    }

    /// Aborts unfinished children, joins all owned handles, and surfaces any
    /// non-cancellation child failure. This is the quiescing lifecycle path;
    /// dropping the adapter is intentionally weaker because `Drop` is not
    /// asynchronous.
    pub async fn shutdown_children(&self) {
        self.reap_completed().await;
        let tasks = std::mem::take(
            &mut *self
                .child_tasks
                .lock()
                .expect("owned IRPC child-task mutex"),
        );
        for task in tasks {
            task.abort();
            match task.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => panic!("owned IRPC child task failed: {error}"),
            }
        }
    }
}

impl Drop for OwnedIrpcAdapter {
    /// Requests cancellation without claiming that children have finished.
    /// Use [`OwnedIrpcAdapter::shutdown_children`] when joining is required.
    fn drop(&mut self) {
        let tasks = self
            .child_tasks
            .lock()
            .expect("owned IRPC child-task mutex");
        for task in tasks.iter() {
            task.abort();
        }
    }
}

/// The canonical operation declaration.  It is deliberately declarative for
/// this reference: the same entries generate the XRPC descriptors, the typed
/// client methods, and the optimized IRPC message/client.
#[macro_export]
macro_rules! declare_typed_service {
    (
        service = $service:ty;
        operation = $operation:ident;
        descriptors = $descriptors:ident;
        error = $error:ty;
        client = $client:ident;
        transport = $transport:ident;
        irpc = $irpc:ident;
        irpc_message = $irpc_message:ident;
        serde_module = $serde_module:ident;
        subscription = $subscription:ident;
        direct_subscription = $direct_subscription:ty;
        subscription_transport = $subscription_transport:ident;
        executor = $executor:path;
        request_encoder = $encoder:path;
        response_decoder = $decoder:path;
        stream_decoder = $stream_decoder:path;
        event_window = $event_window:expr;
        dispatch = $dispatch:ident;
        dispatch_loop = $dispatch_loop:ident;
        stream_capacity_error = $capacity_error:path;
        unary {
            $(
                $variant:ident => $method:ident / $metadata_method:ident(
                    $params:ty
                ) -> $output:ty = $kind:ident, $http:ident, $input:ident,
                    $nsid:literal, $output_schema:literal, $error_schema:literal;
            )+
        }
        stream {
            $(
                $stream_variant:ident => $stream_method:ident / $stream_metadata_method:ident(
                    $stream_params:ty
                ) -> $stream_item:ty = $stream_kind:ident, $stream_http:ident, $stream_input:ident,
                    $stream_nsid:literal, $stream_output_schema:literal, $stream_error_schema:literal;
            )+
        }
    ) => {
        $crate::declare_service_operations! {
            operation = $operation;
            descriptors = $descriptors;
            unary {
                $(
                    $variant = $kind, $http, $input,
                        $nsid, $output_schema, $error_schema;
                )+
            }
            stream {
                $(
                    $stream_variant = $stream_kind, $stream_http, $stream_input,
                        $stream_nsid, $stream_output_schema, $stream_error_schema;
                )+
            }
        }

        fn operation_descriptor(operation: $operation) -> $crate::service::XrpcOperation {
            $descriptors[operation as usize]
        }

        mod $serde_module {
            use super::*;
            #[allow(unused_imports)]
            use $crate::service::__serde as serde;
            #[allow(unused_imports)]
            use $crate::service::__irpc as irpc;
            #[allow(unused_imports)]
            use $crate::service::__tokio as tokio;
            #[allow(unused_imports)]
            use $crate::service::__tracing as tracing;

            #[ $crate::service::__irpc::rpc_requests(message = $irpc_message)]
            #[derive(Debug, serde::Serialize, serde::Deserialize)]
            #[serde(crate = "serde")]
            pub enum $irpc {
                $(
                    #[rpc(tx = $crate::service::__irpc::channel::oneshot::Sender<Result<$output, $error>>)]
                    $variant($crate::service::Request<$params>),
                )+
                $(
                    #[rpc(tx = $crate::service::__irpc::channel::mpsc::Sender<Result<$stream_item, $error>>)]
                    $stream_variant($crate::service::Request<$stream_params>),
                )+
            }
        }

        pub use $serde_module::$irpc;
        pub use $serde_module::$irpc_message;

        pub enum $transport {
            Direct(std::sync::Arc<$service>),
            Irpc($crate::service::__irpc::Client<$irpc>),
            Xrpc(std::sync::Arc<dyn $executor>),
        }

        impl Clone for $transport {
            fn clone(&self) -> Self {
                match self {
                    Self::Direct(service) => Self::Direct(std::sync::Arc::clone(service)),
                    Self::Irpc(client) => Self::Irpc(client.clone()),
                    Self::Xrpc(executor) => Self::Xrpc(std::sync::Arc::clone(executor)),
                }
            }
        }

        pub struct $client {
            transport: $transport,
        }

        impl $client {
            pub fn direct(service: std::sync::Arc<$service>) -> Self {
                Self { transport: $transport::Direct(service) }
            }

            pub fn irpc(client:  $crate::service::__irpc::Client<$irpc>) -> Self {
                Self { transport: $transport::Irpc(client) }
            }

            pub fn xrpc(executor: std::sync::Arc<dyn $executor>) -> Self {
                Self { transport: $transport::Xrpc(executor) }
            }

            $(
                pub async fn $method(
                    &self,
                    context: $crate::service::RequestContext,
                    params: $params,
                ) -> Result<$output, $crate::service::ClientError<$error>> {
                    self.$metadata_method($crate::service::Request::new(context, params)).await
                }

                pub async fn $metadata_method(
                    &self,
                    request: $crate::service::Request<$params>,
                ) -> Result<$output, $crate::service::ClientError<$error>> {
                    match &self.transport {
                        $transport::Direct(service) => {
                            request.context.validate().map_err(|error| {
                                $crate::service::ClientError::Operation(
                                    <$error as From<$crate::service::ContextError>>::from(error),
                                )
                            })?;
                            <$service as $crate::service::UnaryServiceDispatch<
                                $operation, $params, $output, $error,
                            >>::dispatch(service.as_ref(), $operation::$variant, request)
                                .await
                                .map_err($crate::service::ClientError::Operation)
                        },
                        $transport::Irpc(client) => {
                            let operation = operation_descriptor($operation::$variant);
                            let _cancellation = request
                                .context
                                .cancellation
                                .begin_transport_request()
                                .map_err(|error| {
                                    $crate::service::ClientError::Operation(
                                        <$error as From<$crate::service::ContextError>>::from(error),
                                    )
                                })?;
                            client
                                .rpc(request)
                                .await
                                .map_err(|error| {
                                    $crate::service::ClientError::transport(
                                        error,
                                        operation.kind.transport_outcome_unknown(),
                                    )
                                })?
                                .map_err($crate::service::ClientError::Operation)
                        },
                        $transport::Xrpc(executor) => {
                            let operation = operation_descriptor($operation::$variant);
                            request.context.validate().map_err(|error| {
                                $crate::service::ClientError::Operation(
                                    <$error as From<$crate::service::ContextError>>::from(error),
                                )
                            })?;
                            let http = $encoder(operation, &request)?;
                            let response = executor
                                .execute(http)
                                .await
                                .map_err(|error| {
                                    $crate::service::ClientError::transport(
                                        error,
                                        operation.kind.transport_outcome_unknown(),
                                    )
                                })?;
                            $decoder(response)
                        }
                    }
                }
            )+

            $(
                pub async fn $stream_method(
                    &self,
                    context: $crate::service::RequestContext,
                    params: $stream_params,
                ) -> Result<$subscription, $crate::service::ClientError<$error>> {
                    self.$stream_metadata_method($crate::service::Request::new(context, params)).await
                }

                pub async fn $stream_metadata_method(
                    &self,
                    request: $crate::service::Request<$stream_params>,
                ) -> Result<$subscription, $crate::service::ClientError<$error>> {
                    match &self.transport {
                        $transport::Direct(service) => {
                            request.context.validate().map_err(|error| {
                                $crate::service::ClientError::Operation(
                                    <$error as From<$crate::service::ContextError>>::from(error),
                                )
                            })?;
                            let direct = <$service as $crate::service::StreamServiceDispatch<
                                $operation, $stream_params, $direct_subscription, $error,
                            >>::dispatch(service.as_ref(), $operation::$stream_variant, request)
                                .await
                                .map_err($crate::service::ClientError::Operation)?;
                            Ok($subscription {
                                inner: $subscription_transport::Direct(direct),
                            })
                        },
                        $transport::Irpc(client) => {
                            let operation = operation_descriptor($operation::$stream_variant);
                            let _cancellation = request
                                .context
                                .cancellation
                                .begin_transport_request()
                                .map_err(|error| {
                                    $crate::service::ClientError::Operation(
                                        <$error as From<$crate::service::ContextError>>::from(error),
                                    )
                                })?;
                            client
                                .server_streaming(request, $event_window)
                                .await
                                .map(|receiver| $subscription {
                                    inner: $subscription_transport::Irpc {
                                        receiver,
                                        cancellation: _cancellation,
                                    },
                                })
                                .map_err(|error| {
                                    $crate::service::ClientError::transport(
                                        error,
                                        operation.kind.transport_outcome_unknown(),
                                    )
                                })
                        },
                        $transport::Xrpc(executor) => {
                            let operation = operation_descriptor($operation::$stream_variant);
                            request.context.validate().map_err(|error| {
                                $crate::service::ClientError::Operation(
                                    <$error as From<$crate::service::ContextError>>::from(error),
                                )
                            })?;
                            let http = $encoder(operation, &request)?;
                            let response = executor
                                .execute(http)
                                .await
                                .map_err(|error| {
                                    $crate::service::ClientError::transport(
                                        error,
                                        operation.kind.transport_outcome_unknown(),
                                    )
                                })?;
                            let stream = $stream_decoder(response)?;
                            Ok($subscription {
                                inner: $subscription_transport::Xrpc(stream),
                            })
                        }
                    }
                }
            )+
        }

        async fn $dispatch(
            service: std::sync::Arc<$service>,
            adapter: std::sync::Arc<$crate::service::OwnedIrpcAdapter>,
            message: $irpc_message,
        ) {
            adapter.reap_completed().await;
            let is_stream = match &message {
                $($irpc_message::$stream_variant(_) => true,)+
                _ => false,
            };
            if is_stream {
                let Some(stream_permit) = adapter.try_acquire_stream() else {
                    match message {
                        $( $irpc_message::$stream_variant(request) => {
                            let  $crate::service::__irpc::WithChannels { tx, .. } = request;
                            if tx.send(Err($capacity_error(adapter.max_stream_pumps()))).await.is_err() {
                                return;
                            }
                        } )+
                        _ => unreachable!(),
                    }
                    return;
                };
                let stream = adapter.activity().stream_pump();
                let cancellation = match &message {
                    $($irpc_message::$stream_variant(request) => request.inner.context.cancellation.clone(),)+
                    $($irpc_message::$variant(request) => request.inner.context.cancellation.clone(),)+
                };
                let registration = match adapter.register_cancellation(cancellation) {
                    Ok(registration) => registration,
                    Err(error) => {
                        match message {
                            $( $irpc_message::$stream_variant(request) => {
                                let $crate::service::__irpc::WithChannels { tx, .. } = request;
                                if tx
                                    .send(Err(
                                        <$error as From<$crate::service::ContextError>>::from(error),
                                    ))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            } )+
                            _ => unreachable!(),
                        }
                        return;
                    }
                };
                let task = $crate::service::__tokio::spawn(async move {
                    let _stream_permit = stream_permit;
                    let _stream = stream;
                    let _registration = registration;
                    match message {
                        $( $irpc_message::$stream_variant(request) => {
                            let  $crate::service::__irpc::WithChannels { inner, tx, .. } = request;
                            let inner = inner.for_receiver();
                            let cancellation = inner.context.cancellation.clone();
                            let result = match inner.context.validate() {
                                Ok(()) => $crate::service::__tokio::select! {
                                    result = <$service as $crate::service::StreamServiceDispatch<
                                        $operation, $stream_params, $direct_subscription, $error,
                                    >>::dispatch(service.as_ref(), $operation::$stream_variant, inner) => result,
                                    _ = cancellation.cancelled() => Err(
                                        <$error as From<$crate::service::ContextError>>::from(
                                            $crate::service::ContextError::Cancelled,
                                        )
                                    ),
                                },
                                Err(error) => Err(<$error as From<$crate::service::ContextError>>::from(error)),
                            };
                            let mut subscription = match result {
                                Ok(subscription) => subscription,
                                Err(error) => {
                                    if tx.send(Err(error)).await.is_err() {
                                        return;
                                    }
                                    return;
                                }
                            };
                            loop {
                                let item = $crate::service::__tokio::select! {
                                    result = subscription.recv() => match result {
                                        Ok(Some(event)) => Ok(event),
                                        Ok(None) => break,
                                        Err(error) => Err(error),
                                    },
                                    _ = cancellation.cancelled() => Err(
                                        <$error as From<$crate::service::ContextError>>::from(
                                            $crate::service::ContextError::Cancelled,
                                        )
                                    ),
                                };
                                let terminal = item.is_err();
                                if tx.send(item).await.is_err() || terminal { break; }
                            }
                        } )+
                        _ => unreachable!(),
                    }
                });
                adapter.register_child(task);
                return;
            }

            // Admission happens before spawning, so a bounded input queue cannot
            // be drained into an unbounded set of parked Tokio tasks.
            let permit = adapter.acquire_unary().await;
            let cancellation = match &message {
                $($irpc_message::$variant(request) => request.inner.context.cancellation.clone(),)+
                $($irpc_message::$stream_variant(request) => request.inner.context.cancellation.clone(),)+
            };
            let registration = match adapter.register_cancellation(cancellation) {
                Ok(registration) => registration,
                Err(error) => {
                    match message {
                        $( $irpc_message::$variant(request) => {
                            let $crate::service::__irpc::WithChannels { tx, .. } = request;
                            if tx
                                .send(Err(
                                    <$error as From<$crate::service::ContextError>>::from(error),
                                ))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        } )+
                        _ => unreachable!(),
                    }
                    return;
                }
            };
            let activity = adapter.activity();
            let admitted = activity.admitted();
            let spawned = activity.spawned();
            let task = $crate::service::__tokio::spawn(async move {
                let _permit = permit;
                let _admitted = admitted;
                let _spawned = spawned;
                let _registration = registration;
                let _active = activity.active();
                match message {
                    $( $irpc_message::$variant(request) => {
                        let  $crate::service::__irpc::WithChannels { inner, tx, .. } = request;
                        let inner = inner.for_receiver();
                        let cancellation = inner.context.cancellation.clone();
                        let result = match inner.context.validate() {
                            Ok(()) => $crate::service::__tokio::select! {
                                result = <$service as $crate::service::UnaryServiceDispatch<
                                    $operation, $params, $output, $error,
                                >>::dispatch(service.as_ref(), $operation::$variant, inner) => result,
                                _ = cancellation.cancelled() => Err(
                                    <$error as From<$crate::service::ContextError>>::from(
                                        $crate::service::ContextError::Cancelled,
                                    )
                                ),
                            },
                            Err(error) => Err(<$error as From<$crate::service::ContextError>>::from(error)),
                        };
                        if tx.send(result).await.is_err() {
                            return;
                        }
                    } )+
                    _ => unreachable!(),
                }
            });
            adapter.register_child(task);
        }

        pub async fn $dispatch_loop(
            service: std::sync::Arc<$service>,
            adapter: std::sync::Arc<$crate::service::OwnedIrpcAdapter>,
            mut receiver: $crate::service::__tokio::sync::mpsc::Receiver<$irpc_message>,
        ) {
            while let Some(message) = receiver.recv().await {
                $dispatch(std::sync::Arc::clone(&service), std::sync::Arc::clone(&adapter), message).await;
            }
            adapter.shutdown_children().await;
        }
    };
}

/// Declares the operation vocabulary shared by native, IRPC, and XRPC service
/// adapters. Concrete services add their typed handlers and domain errors, but
/// operation identity and HTTP/schema metadata are declared once here.
#[macro_export]
macro_rules! declare_service_operations {
    (
        operation = $operation:ident;
        descriptors = $descriptors:ident;
        unary {
            $(
                $variant:ident = $kind:ident, $http:ident, $input:ident,
                $nsid:literal, $output_schema:literal, $error_schema:literal;
            )+
        }
        stream {
            $(
                $stream_variant:ident = $stream_kind:ident, $stream_http:ident, $stream_input:ident,
                $stream_nsid:literal, $stream_output_schema:literal, $stream_error_schema:literal;
            )+
        }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $operation {
            $( $variant, )+
            $( $stream_variant, )+
        }

        pub const $descriptors: &[$crate::service::XrpcOperation] = &[
            $( $crate::service::XrpcOperation {
                nsid: $nsid,
                kind: $crate::service::XrpcKind::$kind,
                method: $crate::service::HttpMethod::$http,
                input: $crate::service::HttpInput::$input,
                output_schema: $output_schema,
                error_schema: $error_schema,
            }, )+
            $( $crate::service::XrpcOperation {
                nsid: $stream_nsid,
                kind: $crate::service::XrpcKind::$stream_kind,
                method: $crate::service::HttpMethod::$stream_http,
                input: $crate::service::HttpInput::$stream_input,
                output_schema: $stream_output_schema,
                error_schema: $stream_error_schema,
            }, )+
        ];
    };
}

#[cfg(test)]
mod operation_declaration_tests {
    use super::*;
    use serde::{Deserialize, Serialize, de::DeserializeOwned};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn terminal_lease_rejects_delayed_serialized_admission_after_cancel_and_drop() {
        let (transport, receiver) = cancellation_channel(4);
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new(2, 1);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(2, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(2, 1, receiver);

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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
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
        let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);

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

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeQueryParams {
        value: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeProcedureParams {
        value: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeOutput {
        value: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeEvent {
        value: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
    pub enum ProbeError {
        #[error("probe context failed: {0}")]
        Context(#[from] ContextError),
        #[error("probe failed")]
        Failed,
        #[error("probe subscription capacity exhausted (limit {0})")]
        Capacity(usize),
    }

    pub struct ProbeService {
        seen_metadata: Arc<Mutex<Vec<HttpMetadata>>>,
        domain_calls: Arc<AtomicUsize>,
        pending_polled: Arc<AtomicUsize>,
        pending_dropped: Arc<AtomicUsize>,
    }

    impl ProbeService {
        fn new() -> Self {
            Self {
                seen_metadata: Arc::new(Mutex::new(Vec::new())),
                domain_calls: Arc::new(AtomicUsize::new(0)),
                pending_polled: Arc::new(AtomicUsize::new(0)),
                pending_dropped: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn seen_metadata(&self) -> Vec<HttpMetadata> {
            self.seen_metadata
                .lock()
                .expect("probe metadata mutex")
                .clone()
        }
    }

    struct PendingProbe {
        polled: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    impl std::future::Future for PendingProbe {
        type Output = Result<ProbeOutput, ProbeError>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.get_mut().polled.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Pending
        }
    }

    impl Drop for PendingProbe {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl UnaryServiceDispatch<ProbeOperation, ProbeQueryParams, ProbeOutput, ProbeError>
        for ProbeService
    {
        fn dispatch<'a>(
            &'a self,
            _operation: ProbeOperation,
            request: Request<ProbeQueryParams>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ProbeOutput, ProbeError>> + Send + 'a>,
        > {
            self.seen_metadata
                .lock()
                .expect("probe metadata mutex")
                .push(request.metadata.clone());
            self.domain_calls.fetch_add(1, Ordering::SeqCst);
            if request.params.value == "cancel-query" {
                return Box::pin(PendingProbe {
                    polled: Arc::clone(&self.pending_polled),
                    dropped: Arc::clone(&self.pending_dropped),
                });
            }
            Box::pin(async move {
                Ok(ProbeOutput {
                    value: request.params.value,
                })
            })
        }
    }

    impl UnaryServiceDispatch<ProbeOperation, ProbeProcedureParams, ProbeOutput, ProbeError>
        for ProbeService
    {
        fn dispatch<'a>(
            &'a self,
            _operation: ProbeOperation,
            request: Request<ProbeProcedureParams>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ProbeOutput, ProbeError>> + Send + 'a>,
        > {
            self.seen_metadata
                .lock()
                .expect("probe metadata mutex")
                .push(request.metadata.clone());
            self.domain_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                Ok(ProbeOutput {
                    value: format!("procedure:{}", request.params.value),
                })
            })
        }
    }

    pub struct ProbeDirectSubscription {
        _sender: Option<mpsc::Sender<Result<ProbeEvent, ProbeError>>>,
        receiver: mpsc::Receiver<Result<ProbeEvent, ProbeError>>,
    }

    impl ProbeDirectSubscription {
        async fn recv(&mut self) -> Result<Option<ProbeEvent>, ProbeError> {
            self.receiver.recv().await.transpose()
        }
    }

    impl
        StreamServiceDispatch<
            ProbeOperation,
            ProbeSubscriptionParams,
            ProbeDirectSubscription,
            ProbeError,
        > for ProbeService
    {
        fn dispatch<'a>(
            &'a self,
            _operation: ProbeOperation,
            request: Request<ProbeSubscriptionParams>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<ProbeDirectSubscription, ProbeError>>
                    + Send
                    + 'a,
            >,
        > {
            self.seen_metadata
                .lock()
                .expect("probe metadata mutex")
                .push(request.metadata.clone());
            self.domain_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                let (sender, receiver) = mpsc::channel(4);
                let sender = match request.params.after {
                    100 => {
                        sender
                            .send(Ok(ProbeEvent {
                                value: "eof-event".into(),
                            }))
                            .await
                            .expect("probe EOF receiver lives");
                        None
                    }
                    101 => {
                        sender
                            .send(Err(ProbeError::Failed))
                            .await
                            .expect("probe error receiver lives");
                        None
                    }
                    _ => {
                        sender
                            .send(Ok(ProbeEvent {
                                value: format!("event:{}", request.params.after),
                            }))
                            .await
                            .expect("probe subscription receiver lives");
                        Some(sender)
                    }
                };
                Ok(ProbeDirectSubscription {
                    _sender: sender,
                    receiver,
                })
            })
        }
    }

    #[derive(Debug)]
    pub struct ProbeResponse {
        status: u16,
        body: Option<serde_json::Value>,
        stream: Option<mpsc::Receiver<Result<ProbeEvent, ProbeError>>>,
    }

    #[async_trait::async_trait]
    pub trait ProbeExecutor: Send + Sync {
        async fn execute(&self, request: HttpRequest) -> Result<ProbeResponse, XrpcTransportError>;
    }

    pub struct ProbeFakeExecutor {
        seen_metadata: Arc<Mutex<Vec<HttpMetadata>>>,
    }

    impl ProbeFakeExecutor {
        fn new() -> Self {
            Self {
                seen_metadata: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProbeExecutor for ProbeFakeExecutor {
        async fn execute(&self, request: HttpRequest) -> Result<ProbeResponse, XrpcTransportError> {
            self.seen_metadata
                .lock()
                .expect("probe executor metadata mutex")
                .push(request.metadata.clone());
            if request.operation.kind == XrpcKind::Subscription {
                let (sender, receiver) = mpsc::channel(1);
                sender
                    .send(Ok(ProbeEvent {
                        value: "xrpc-event".into(),
                    }))
                    .await
                    .expect("probe stream receiver lives");
                return Ok(ProbeResponse {
                    status: 200,
                    body: None,
                    stream: Some(receiver),
                });
            }
            Ok(ProbeResponse {
                status: 200,
                body: request.body.or(request.query),
                stream: None,
            })
        }
    }

    fn probe_encode<P: Serialize>(
        operation: XrpcOperation,
        request: &Request<P>,
    ) -> Result<HttpRequest, ClientError<ProbeError>> {
        let params = serde_json::to_value(&request.params)
            .map_err(|error| ClientError::transport(error, false))?;
        Ok(HttpRequest {
            context: request.context.clone(),
            metadata: request.metadata.clone(),
            operation,
            query: matches!(operation.input, HttpInput::Query).then_some(params.clone()),
            body: matches!(operation.input, HttpInput::JsonBody).then_some(params),
        })
    }

    fn probe_decode<T: DeserializeOwned>(
        response: ProbeResponse,
    ) -> Result<T, ClientError<ProbeError>> {
        if response.status >= 400 {
            return Err(ClientError::Operation(ProbeError::Failed));
        }
        serde_json::from_value(
            response
                .body
                .ok_or_else(|| ClientError::transport("missing probe response", false))?,
        )
        .map_err(|error| ClientError::transport(error, false))
    }

    fn probe_stream_decode(
        response: ProbeResponse,
    ) -> Result<mpsc::Receiver<Result<ProbeEvent, ProbeError>>, ClientError<ProbeError>> {
        if response.status >= 400 {
            return Err(ClientError::Operation(ProbeError::Failed));
        }
        response
            .stream
            .ok_or_else(|| ClientError::transport("missing probe stream", true))
    }

    enum ProbeSubscriptionTransport {
        Direct(ProbeDirectSubscription),
        Irpc {
            receiver: irpc::channel::mpsc::Receiver<Result<ProbeEvent, ProbeError>>,
            cancellation: Option<CancellationRequestGuard>,
        },
        Xrpc(mpsc::Receiver<Result<ProbeEvent, ProbeError>>),
    }

    pub struct ProbeSubscription {
        inner: ProbeSubscriptionTransport,
    }

    impl ProbeSubscription {
        async fn recv(&mut self) -> Result<Option<ProbeEvent>, ClientError<ProbeError>> {
            match &mut self.inner {
                ProbeSubscriptionTransport::Direct(subscription) => {
                    subscription.recv().await.map_err(ClientError::Operation)
                }
                ProbeSubscriptionTransport::Irpc {
                    receiver,
                    cancellation: _cancellation,
                } => receiver
                    .recv()
                    .await
                    .map_err(|error| ClientError::transport(error, true))?
                    .transpose()
                    .map_err(ClientError::Operation),
                ProbeSubscriptionTransport::Xrpc(receiver) => receiver
                    .recv()
                    .await
                    .transpose()
                    .map_err(ClientError::Operation),
            }
        }
    }

    fn probe_capacity(limit: usize) -> ProbeError {
        ProbeError::Capacity(limit)
    }

    declare_typed_service! {
        service = ProbeService;
        operation = ProbeOperation;
        descriptors = PROBE_OPERATIONS;
        error = ProbeError;
        client = ProbeClient;
        transport = ProbeTransport;
        irpc = ProbeIrpc;
        irpc_message = ProbeIrpcMessage;
        serde_module = probe_typed_serde;
        subscription = ProbeSubscription;
        direct_subscription = ProbeDirectSubscription;
        subscription_transport = ProbeSubscriptionTransport;
        executor = ProbeExecutor;
        request_encoder = probe_encode;
        response_decoder = probe_decode;
        stream_decoder = probe_stream_decode;
        event_window = 2;
        dispatch = dispatch_probe_irpc;
        dispatch_loop = dispatch_probe_irpc_loop;
        stream_capacity_error = probe_capacity;
        unary {
            Ping => ping / ping_with_metadata(ProbeQueryParams) -> ProbeOutput = Query, Get, Query,
                "org.example.probe.ping", "ProbeOutput", "ProbeError";
            Mutate => mutate / mutate_with_metadata(ProbeProcedureParams) -> ProbeOutput = Procedure, Post, JsonBody,
                "org.example.probe.mutate", "ProbeOutput", "ProbeError";
        }
        stream {
            Events => events / events_with_metadata(ProbeSubscriptionParams) -> ProbeEvent = Subscription, Get, Query,
                "org.example.probe.events", "ProbeEvent", "ProbeError";
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ProbeSubscriptionParams {
        after: u64,
    }

    #[test]
    fn a_second_consumer_uses_shared_typed_generation() {
        assert_eq!(PROBE_OPERATIONS.len(), 3);
        assert_eq!(PROBE_OPERATIONS[0].kind, XrpcKind::Query);
        assert_eq!(PROBE_OPERATIONS[1].kind, XrpcKind::Procedure);
        assert_eq!(PROBE_OPERATIONS[2].kind, XrpcKind::Subscription);
    }

    async fn wait_for_stream_cleanup(adapter: &Arc<OwnedIrpcAdapter>) {
        while adapter.activity.stream_pump_count() != 0
            || !adapter
                .cancellations
                .lock()
                .expect("stream cancellation registry mutex")
                .admitted
                .is_empty()
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            adapter.stream_permits.available_permits(),
            adapter.max_stream_pumps,
            "stream child releases its permit"
        );
    }

    #[tokio::test]
    async fn generated_stream_terminal_paths_release_server_and_transport_ownership() {
        // A normal EOF releases the server child, while the subscription still
        // owns the producer's transport guard until it is dropped.
        {
            let (transport, receiver) = cancellation_channel(4);
            let (sender, incoming) = mpsc::channel(4);
            let client = ProbeClient::irpc(irpc::Client::local(sender));
            let service = Arc::new(ProbeService::new());
            let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
            let activity = adapter.activity();
            let dispatcher = tokio::spawn(dispatch_probe_irpc_loop(
                Arc::clone(&service),
                Arc::clone(&adapter),
                incoming,
            ));
            let context = RequestContext::root().with_cancellation_transport(transport.clone());
            let weak_state = Arc::downgrade(&context.cancellation.state);
            let mut subscription = client
                .events(context.clone(), ProbeSubscriptionParams { after: 100 })
                .await
                .expect("normal-EOF subscription");
            assert_eq!(
                subscription
                    .recv()
                    .await
                    .expect("normal-EOF event")
                    .expect("normal-EOF item")
                    .value,
                "eof-event"
            );
            assert!(subscription.recv().await.expect("normal EOF").is_none());
            wait_for_stream_cleanup(&adapter).await;
            assert_eq!(activity.stream_pump_count(), 0);
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("normal-EOF admission mutex")
                    .live
                    .len(),
                1,
                "subscription guard remains live after server EOF"
            );
            drop(subscription);
            drop(context);
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("normal-EOF admission mutex")
                    .live
                    .len(),
                0,
                "normal EOF subscription drop retires its transport scope"
            );
            assert!(weak_state.upgrade().is_none());
            drop(client);
            dispatcher.await.expect("normal-EOF dispatcher");
            adapter.shutdown_children().await;
        }

        // A stream item error is terminal and must release the same server
        // ownership before the client subscription is dropped.
        {
            let (transport, receiver) = cancellation_channel(4);
            let (sender, incoming) = mpsc::channel(4);
            let client = ProbeClient::irpc(irpc::Client::local(sender));
            let service = Arc::new(ProbeService::new());
            let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
            let dispatcher = tokio::spawn(dispatch_probe_irpc_loop(
                Arc::clone(&service),
                Arc::clone(&adapter),
                incoming,
            ));
            let context = RequestContext::root().with_cancellation_transport(transport.clone());
            let mut subscription = client
                .events(context.clone(), ProbeSubscriptionParams { after: 101 })
                .await
                .expect("error subscription");
            assert!(matches!(
                subscription.recv().await,
                Err(ClientError::Operation(ProbeError::Failed))
            ));
            wait_for_stream_cleanup(&adapter).await;
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("error admission mutex")
                    .live
                    .len(),
                1,
                "terminal error does not silently consume the client guard"
            );
            drop(subscription);
            drop(context);
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("error admission mutex")
                    .live
                    .len(),
                0
            );
            drop(client);
            dispatcher.await.expect("error dispatcher");
            adapter.shutdown_children().await;
        }

        // Explicit cancellation is distinct from guard retirement: cancellation
        // wakes the generated child, then dropping the subscription retires the
        // producer scope without rolling back any admitted state.
        {
            let (transport, receiver) = cancellation_channel(4);
            let (sender, incoming) = mpsc::channel(4);
            let client = ProbeClient::irpc(irpc::Client::local(sender));
            let service = Arc::new(ProbeService::new());
            let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
            let dispatcher = tokio::spawn(dispatch_probe_irpc_loop(
                Arc::clone(&service),
                Arc::clone(&adapter),
                incoming,
            ));
            let context = RequestContext::root().with_cancellation_transport(transport.clone());
            let mut subscription = client
                .events(context.clone(), ProbeSubscriptionParams { after: 102 })
                .await
                .expect("cancellable subscription");
            assert!(
                subscription
                    .recv()
                    .await
                    .expect("cancellable event")
                    .is_some()
            );
            drop(subscription);
            assert!(
                !context.cancellation.is_cancelled(),
                "subscription drop retires its guard without explicit cancellation"
            );
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("drop admission mutex")
                    .live
                    .len(),
                0,
                "subscription drop retires the producer scope"
            );
            context.cancel();
            wait_for_stream_cleanup(&adapter).await;
            assert!(context.cancellation.is_cancelled());
            drop(context);
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("drop admission mutex")
                    .live
                    .len(),
                0,
                "explicit cancellation plus subscription drop retires transport scope"
            );
            drop(client);
            dispatcher.await.expect("drop dispatcher");
            adapter.shutdown_children().await;
        }

        // Joined adapter shutdown must release a still-running generated child;
        // the client guard remains independent until its subscription is dropped.
        {
            let (transport, receiver) = cancellation_channel(4);
            let (sender, incoming) = mpsc::channel(4);
            let client = ProbeClient::irpc(irpc::Client::local(sender));
            let service = Arc::new(ProbeService::new());
            let adapter = OwnedIrpcAdapter::new_with_cancellation(1, 1, receiver);
            let activity = adapter.activity();
            let dispatcher = tokio::spawn(dispatch_probe_irpc_loop(
                Arc::clone(&service),
                Arc::clone(&adapter),
                incoming,
            ));
            let context = RequestContext::root().with_cancellation_transport(transport.clone());
            let mut subscription = client
                .events(context.clone(), ProbeSubscriptionParams { after: 103 })
                .await
                .expect("shutdown subscription");
            assert!(subscription.recv().await.expect("shutdown event").is_some());
            adapter.shutdown_children().await;
            assert_eq!(activity.stream_pump_count(), 0);
            assert_eq!(adapter.stream_permits.available_permits(), 1);
            assert_eq!(
                adapter
                    .cancellations
                    .lock()
                    .expect("shutdown cancellation registry mutex")
                    .admitted
                    .len(),
                0,
                "joined shutdown drops server registration"
            );
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("shutdown admission mutex")
                    .live
                    .len(),
                1,
                "joined shutdown does not retire the client guard"
            );
            drop(subscription);
            drop(context);
            assert_eq!(
                transport
                    .admissions()
                    .lock()
                    .expect("shutdown admission mutex")
                    .live
                    .len(),
                0
            );
            drop(client);
            dispatcher.await.expect("shutdown dispatcher");
        }
    }

    #[tokio::test]
    async fn dropping_native_probe_adapter_aborts_pending_serialized_handler() {
        let (cancel_transport, cancel_receiver) = cancellation_channel(8);
        let (sender, receiver) = mpsc::channel(4);
        let client = ProbeClient::irpc(irpc::Client::local(sender));
        let service = Arc::new(ProbeService::new());
        let owned = OwnedIrpcAdapter::new_with_cancellation(1, 1, cancel_receiver);
        let weak_owned = Arc::downgrade(&owned);
        let activity = owned.activity();
        let unary_permits = Arc::clone(&owned.unary_permits);
        let dispatcher = tokio::spawn(dispatch_probe_irpc_loop(
            Arc::clone(&service),
            Arc::clone(&owned),
            receiver,
        ));

        let producer_context = RequestContext::root().with_cancellation_transport(cancel_transport);
        let _producer_guard = producer_context
            .cancellation
            .begin_transport_request()
            .expect("producer scope result")
            .expect("producer transport scope");
        let wire = serde_json::to_vec(&producer_context).expect("pending context serializes");
        let receiver_context =
            serde_json::from_slice::<RequestContext>(&wire).expect("pending context deserializes");
        let pending_call = tokio::spawn(async move {
            client
                .ping(
                    receiver_context,
                    ProbeQueryParams {
                        value: "cancel-query".into(),
                    },
                )
                .await
        });

        while service.pending_polled.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(service.domain_calls.load(Ordering::SeqCst), 1);
        assert_eq!(unary_permits.available_permits(), 0);
        assert_eq!(activity.admitted_count(), 1);
        assert_eq!(activity.spawned_count(), 1);
        assert_eq!(activity.active_count(), 1);

        // The generated dispatcher captures an adapter Arc, so stop it before
        // the external adapter is dropped. The
        // pending generated child remains in the adapter's owned handles and
        // must be aborted by Drop itself.
        dispatcher.abort();
        dispatcher
            .await
            .expect_err("dispatcher was explicitly aborted for adapter-drop test");
        drop(owned);

        while service.pending_dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        while activity.admitted_count() != 0
            || activity.spawned_count() != 0
            || activity.active_count() != 0
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(service.pending_dropped.load(Ordering::SeqCst), 1);
        assert_eq!(unary_permits.available_permits(), 1);
        assert!(weak_owned.upgrade().is_none());
        assert!(pending_call.await.expect("pending call task").is_err());
    }

    #[tokio::test]
    async fn second_consumer_uses_generated_direct_irpc_and_fake_xrpc_clients() {
        let direct_service = Arc::new(ProbeService::new());
        let direct = ProbeClient::direct(Arc::clone(&direct_service));
        assert_eq!(
            direct
                .ping(
                    RequestContext::root(),
                    ProbeQueryParams {
                        value: "direct".into()
                    }
                )
                .await
                .expect("direct query")
                .value,
            "direct"
        );
        assert_eq!(
            direct
                .mutate(
                    RequestContext::root(),
                    ProbeProcedureParams {
                        value: "direct".into()
                    }
                )
                .await
                .expect("direct procedure")
                .value,
            "procedure:direct"
        );
        assert_eq!(
            direct
                .events(RequestContext::root(), ProbeSubscriptionParams { after: 1 })
                .await
                .expect("direct subscription")
                .recv()
                .await
                .expect("direct event")
                .expect("event")
                .value,
            "event:1"
        );
        let direct_metadata = HttpMetadata {
            authorization: Some("direct-auth".into()),
            idempotency_key: Some("direct-idempotency".into()),
            traceparent: Some("direct-traceparent".into()),
            tracestate: Some("direct-tracestate".into()),
            timeout: Some(Duration::from_secs(3)),
        };
        direct
            .ping_with_metadata(Request::with_metadata(
                RequestContext::root(),
                direct_metadata.clone(),
                ProbeQueryParams {
                    value: "metadata".into(),
                },
            ))
            .await
            .expect("direct metadata");
        assert_eq!(
            direct_service.seen_metadata().last(),
            Some(&direct_metadata)
        );

        let (sender, receiver) = mpsc::channel(4);
        let irpc_client = ProbeClient::irpc(irpc::Client::local(sender));
        let service = Arc::new(ProbeService::new());
        let owned = OwnedIrpcAdapter::new(4, 2);
        let task = tokio::spawn(dispatch_probe_irpc_loop(
            Arc::clone(&service),
            Arc::clone(&owned),
            receiver,
        ));
        assert_eq!(
            irpc_client
                .ping(
                    RequestContext::root(),
                    ProbeQueryParams {
                        value: "irpc".into()
                    }
                )
                .await
                .expect("IRPC query")
                .value,
            "irpc"
        );
        assert_eq!(
            irpc_client
                .mutate(
                    RequestContext::root(),
                    ProbeProcedureParams {
                        value: "irpc".into()
                    }
                )
                .await
                .expect("IRPC procedure")
                .value,
            "procedure:irpc"
        );
        let cancelled = RequestContext::root();
        cancelled.cancel();
        assert!(matches!(
            irpc_client
                .ping(
                    cancelled,
                    ProbeQueryParams {
                        value: "cancelled".into()
                    }
                )
                .await,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::Cancelled
            )))
        ));
        let mut subscription = irpc_client
            .events(RequestContext::root(), ProbeSubscriptionParams { after: 2 })
            .await
            .expect("IRPC subscription");
        assert_eq!(
            subscription
                .recv()
                .await
                .expect("IRPC event")
                .expect("event")
                .value,
            "event:2"
        );
        let irpc_metadata = HttpMetadata {
            authorization: Some("irpc-auth".into()),
            idempotency_key: Some("irpc-idempotency".into()),
            traceparent: Some("irpc-traceparent".into()),
            tracestate: Some("irpc-tracestate".into()),
            timeout: Some(Duration::from_secs(4)),
        };
        irpc_client
            .ping_with_metadata(Request::with_metadata(
                RequestContext::root(),
                irpc_metadata.clone(),
                ProbeQueryParams {
                    value: "metadata".into(),
                },
            ))
            .await
            .expect("IRPC metadata");
        assert_eq!(service.seen_metadata().last(), Some(&irpc_metadata));
        let calls_before_expiry = service.domain_calls.load(Ordering::SeqCst);
        let expired = RequestContext::root().with_time_budget(Duration::ZERO);
        assert!(matches!(
            irpc_client
                .mutate(
                    expired,
                    ProbeProcedureParams {
                        value: "expired".into()
                    }
                )
                .await,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::DeadlineExceeded
            )))
        ));
        assert_eq!(
            service.domain_calls.load(Ordering::SeqCst),
            calls_before_expiry
        );
        drop(irpc_client);
        task.await.expect("IRPC dispatcher");
        owned.shutdown_children().await;

        let (cancel_transport, cancel_receiver) = cancellation_channel(8);
        let (sender, receiver) = mpsc::channel(4);
        let cancellable_client = Arc::new(ProbeClient::irpc(irpc::Client::local(sender)));
        let cancellable_service = Arc::new(ProbeService::new());
        let cancellable_owned = OwnedIrpcAdapter::new_with_cancellation(1, 1, cancel_receiver);
        let cancellable_task = tokio::spawn(dispatch_probe_irpc_loop(
            Arc::clone(&cancellable_service),
            Arc::clone(&cancellable_owned),
            receiver,
        ));

        let pre_cancelled_calls = cancellable_service.domain_calls.load(Ordering::SeqCst);
        let pre_cancelled = RequestContext::root();
        pre_cancelled.cancel();
        assert!(matches!(
            cancellable_client
                .ping(
                    pre_cancelled,
                    ProbeQueryParams {
                        value: "pre-cancelled".into()
                    }
                )
                .await,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::Cancelled
            )))
        ));
        assert_eq!(
            cancellable_service.domain_calls.load(Ordering::SeqCst),
            pre_cancelled_calls
        );

        let pending_context =
            RequestContext::root().with_cancellation_transport(cancel_transport.clone());
        let pending_canceller = pending_context.clone();
        let _pending_request = pending_context
            .cancellation
            .begin_transport_request()
            .expect("pending transport scope");
        let pending_wire =
            serde_json::to_vec(&pending_context).expect("pending context serializes");
        let pending_receiver_context = serde_json::from_slice::<RequestContext>(&pending_wire)
            .expect("pending context deserializes");
        let pending_call = tokio::spawn({
            let client = Arc::clone(&cancellable_client);
            async move {
                client
                    .ping(
                        pending_receiver_context,
                        ProbeQueryParams {
                            value: "cancel-query".into(),
                        },
                    )
                    .await
            }
        });
        while cancellable_service.domain_calls.load(Ordering::SeqCst) <= pre_cancelled_calls {
            tokio::task::yield_now().await;
        }
        pending_canceller.cancel();
        assert!(matches!(
            pending_call.await.expect("pending cancellation task"),
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::Cancelled
            )))
        ));
        while cancellable_owned.activity().active_count() != 0
            || cancellable_owned.activity().admitted_count() != 0
            || cancellable_owned.activity().spawned_count() != 0
        {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            cancellable_client
                .ping(
                    RequestContext::root(),
                    ProbeQueryParams {
                        value: "capacity-recovered".into()
                    },
                )
                .await
                .expect("capacity recovers after cancellation")
                .value,
            "capacity-recovered"
        );

        let stream_context =
            RequestContext::root().with_cancellation_transport(cancel_transport.clone());
        let stream_canceller = stream_context.clone();
        let mut stream = cancellable_client
            .events(stream_context.clone(), ProbeSubscriptionParams { after: 9 })
            .await
            .expect("cancellable subscription");
        assert_eq!(
            stream
                .recv()
                .await
                .expect("first stream event")
                .expect("event")
                .value,
            "event:9"
        );
        assert!(
            stream_context
                .cancellation
                .state
                .transport_active
                .load(Ordering::Acquire),
            "generated stream retains cancellation scope after establishment"
        );
        stream_canceller.cancel();
        let stream_cancel_result = stream.recv().await;
        assert!(matches!(
            stream_cancel_result,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::Cancelled
            )))
        ));
        while cancellable_owned.activity().stream_pump_count() != 0 {
            tokio::task::yield_now().await;
        }

        let wire = serde_json::to_vec(&RequestContext::root()).expect("context serializes");
        let first = serde_json::from_slice::<RequestContext>(&wire)
            .expect("context deserializes")
            .with_cancellation_transport(cancel_transport.clone());
        let late_canceller = first.clone();
        cancellable_client
            .ping(
                first,
                ProbeQueryParams {
                    value: "late-cancel-first".into(),
                },
            )
            .await
            .expect("first reused-id request");
        late_canceller.cancel();
        tokio::task::yield_now().await;
        let second = serde_json::from_slice::<RequestContext>(&wire)
            .expect("context deserializes again")
            .with_cancellation_transport(cancel_transport.clone());
        assert_eq!(
            cancellable_client
                .ping(
                    second,
                    ProbeQueryParams {
                        value: "reused-id-second".into()
                    }
                )
                .await
                .expect("late cancellation cannot cancel reused id")
                .value,
            "reused-id-second"
        );

        drop(cancellable_client);
        cancellable_task.await.expect("cancellable dispatcher");
        cancellable_owned.shutdown_children().await;

        let xrpc_executor = Arc::new(ProbeFakeExecutor::new());
        let xrpc_seen = Arc::clone(&xrpc_executor.seen_metadata);
        let xrpc_transport: Arc<dyn ProbeExecutor> = xrpc_executor;
        let xrpc = ProbeClient::xrpc(xrpc_transport);
        assert_eq!(
            xrpc.ping(
                RequestContext::root(),
                ProbeQueryParams {
                    value: "xrpc".into()
                }
            )
            .await
            .expect("XRPC query")
            .value,
            "xrpc"
        );
        assert_eq!(
            xrpc.mutate(
                RequestContext::root(),
                ProbeProcedureParams {
                    value: "xrpc".into()
                }
            )
            .await
            .expect("XRPC procedure")
            .value,
            "xrpc"
        );
        let mut subscription = xrpc
            .events(RequestContext::root(), ProbeSubscriptionParams { after: 3 })
            .await
            .expect("XRPC subscription");
        assert_eq!(
            subscription
                .recv()
                .await
                .expect("XRPC event")
                .expect("event")
                .value,
            "xrpc-event"
        );
        let xrpc_metadata = HttpMetadata {
            authorization: Some("xrpc-auth".into()),
            idempotency_key: Some("xrpc-idempotency".into()),
            traceparent: Some("xrpc-traceparent".into()),
            tracestate: Some("xrpc-tracestate".into()),
            timeout: Some(Duration::from_secs(5)),
        };
        xrpc.ping_with_metadata(Request::with_metadata(
            RequestContext::root(),
            xrpc_metadata.clone(),
            ProbeQueryParams {
                value: "metadata".into(),
            },
        ))
        .await
        .expect("XRPC metadata");
        assert_eq!(
            xrpc_seen.lock().expect("probe metadata mutex").last(),
            Some(&xrpc_metadata)
        );
    }

    #[tokio::test]
    async fn full_generator_validates_context_metadata_and_receiver_expiry() {
        let direct = ProbeClient::direct(Arc::new(ProbeService::new()));
        let invalid = RequestContext {
            request_id: Uuid::nil(),
            ..RequestContext::root()
        };
        assert!(matches!(
            direct
                .ping(invalid, ProbeQueryParams { value: "x".into() })
                .await,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::MissingRequestId
            )))
        ));
        let metadata = Request::with_metadata(
            RequestContext::root(),
            HttpMetadata {
                authorization: Some("bearer".into()),
                ..Default::default()
            },
            ProbeProcedureParams { value: "m".into() },
        );
        assert_eq!(
            direct
                .mutate_with_metadata(metadata)
                .await
                .expect("metadata procedure")
                .value,
            "procedure:m"
        );
        let expired = RequestContext::root().with_time_budget(Duration::ZERO);
        assert!(matches!(
            direct
                .mutate(expired, ProbeProcedureParams { value: "x".into() })
                .await,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::DeadlineExceeded
            )))
        ));
        let xrpc = ProbeClient::xrpc(Arc::new(ProbeFakeExecutor::new()));
        let invalid = RequestContext {
            request_id: Uuid::nil(),
            ..RequestContext::root()
        };
        assert!(matches!(
            xrpc.ping(invalid, ProbeQueryParams { value: "x".into() })
                .await,
            Err(ClientError::Operation(ProbeError::Context(
                ContextError::MissingRequestId
            )))
        ));
    }

    #[tokio::test]
    async fn generated_procedure_transport_loss_is_outcome_unknown() {
        let (sender, mut receiver) = mpsc::channel(1);
        let client = ProbeClient::irpc(irpc::Client::local(sender));
        let task = tokio::spawn(async move {
            drop(receiver.recv().await);
        });
        let result = client
            .mutate(
                RequestContext::root(),
                ProbeProcedureParams {
                    value: "lost".into(),
                },
            )
            .await;
        task.await.expect("drop request");
        assert!(matches!(
            result,
            Err(ClientError::Transport {
                outcome_unknown: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn shared_owned_adapter_reaps_completed_children() {
        let adapter = OwnedIrpcAdapter::new(1, 1);
        adapter.register_child(tokio::spawn(async {}));
        tokio::task::yield_now().await;
        adapter.reap_completed().await;
        assert_eq!(adapter.child_count(), 0);
    }

    #[tokio::test]
    async fn shared_owned_adapter_shutdown_joins_pending_children_and_releases_guards() {
        let adapter = OwnedIrpcAdapter::new(1, 1);
        let activity = adapter.activity();
        let child_activity = Arc::clone(&activity);
        adapter.register_child(tokio::spawn(async move {
            let _admitted = child_activity.admitted();
            let _spawned = child_activity.spawned();
            std::future::pending::<()>().await;
        }));
        tokio::task::yield_now().await;
        assert_eq!(activity.admitted_count(), 1);
        assert_eq!(activity.spawned_count(), 1);
        assert_eq!(adapter.child_count(), 1);

        adapter.shutdown_children().await;

        assert_eq!(activity.admitted_count(), 0);
        assert_eq!(activity.spawned_count(), 0);
        assert_eq!(adapter.child_count(), 0);
    }

    #[test]
    fn context_carries_command_budget_and_cancellation() {
        let command_id = Uuid::now_v7();
        let context = RequestContext::root()
            .with_command_id(command_id)
            .with_time_budget(Duration::from_secs(1));
        let child = context.child();
        assert_eq!(child.command_id, Some(command_id));
        assert_eq!(child.time_budget, context.time_budget);
        assert!(child.validate().is_ok());

        child.cancel();
        assert_eq!(child.validate(), Err(ContextError::Cancelled));
        assert_eq!(context.validate(), Err(ContextError::Cancelled));

        let expired = RequestContext::root().with_time_budget(Duration::ZERO);
        assert_eq!(expired.validate(), Err(ContextError::DeadlineExceeded));
    }

    #[tokio::test]
    async fn cancellation_uses_transport_owned_state() {
        let (transport, _receiver) = cancellation_channel(8);
        let context = RequestContext::root().with_cancellation_transport(transport);
        let _request = context
            .cancellation
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        context.cancel();
        assert_eq!(context.validate(), Err(ContextError::Cancelled));
    }

    #[test]
    fn serialized_context_preserves_elapsed_budget_and_cancellation() {
        let context = RequestContext::root().with_time_budget(Duration::from_millis(5));
        let wire = serde_json::to_vec(&context).expect("context serializes");
        std::thread::sleep(Duration::from_millis(10));
        let received: RequestContext = serde_json::from_slice(&wire).expect("context deserializes");
        assert_eq!(
            received.for_receiver().validate(),
            Err(ContextError::DeadlineExceeded)
        );

        let context = RequestContext::root();
        context.cancel();
        let wire = serde_json::to_vec(&context).expect("context serializes");
        let received: RequestContext = serde_json::from_slice(&wire).expect("context deserializes");
        assert_eq!(received.validate(), Err(ContextError::Cancelled));
    }
}
