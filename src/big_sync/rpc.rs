use crate::interlude::*;

use crate::part_store::{HostPartStore, ReadTarget, validate_replay_targets};

use big_sync_core::PeerKey;
use big_sync_core::rpc::{
    BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PeerSummaryError, PeerSummaryRequest, PeerSummaryResult,
    ReplayPage, ReplayPageRequest, ReplaySessionId, ReplaySubscriptionPage,
    ReplaySubscriptionRequest, ReplaySubscriptionResponse, ReplaySubscriptionTarget, RpcError,
    TargetVerdict,
};
use irpc::{WithChannels, channel, rpc_requests};
use tokio::sync::mpsc;

pub const BIG_SYNC_RPC_ALPN: &[u8] = b"townframe/big-sync/0";

/// The longest a page request is held while its target has nothing to send.
///
/// The caller asks for a hold and this caps it, so a caller cannot park a request
/// for as long as it likes. It paces the caught-up case only: a page that has
/// events answers at once, and the caller re-issues either way. A protocol knob,
/// not a measured threshold.
const MAX_PAGE_HOLD: Duration = Duration::from_secs(15);

/// The largest page a caller may ask for.
///
/// `limit` is untrusted wire input, so it is capped where it arrives: without a
/// cap a peer can ask for `u32::MAX` and make the responder buffer a whole part
/// in one reply, which defeats paging-as-flow-control. The cap is the asking
/// side's own page size (1024 events, the measured value behind
/// `ReplayPageTask::LIMIT`), so a well-behaved caller is never refused; raising
/// that page size means raising this one. A request at or above the cap gets the
/// cap rather than an error. A protocol knob, not a measured threshold.
const MAX_PAGE_LIMIT: u32 = 1024;

/// Apply [`MAX_PAGE_LIMIT`] to an untrusted page request.
fn page_limit(requested: u32) -> u32 {
    requested.min(MAX_PAGE_LIMIT)
}

/// The largest page a caller may ask a bucket endpoint for.
///
/// `limit_hint` is untrusted wire input, exactly as `ReplayPageRequest::limit` is,
/// so it is capped where it arrives: without a cap a peer can ask for `u32::MAX`
/// and make the responder buffer a whole part's bucket summaries, or a whole
/// bucket's entries, in one reply. The cap is the asking side's own leaf page size
/// (`BucketMachine::LEAF_BUCKET_LIMIT_HINT` = 1024, the same number as
/// [`MAX_PAGE_LIMIT`]); that side asks for 128 changed buckets
/// (`BucketMachine::GET_BUCKET_LIMIT_HINT` = 8 × `BuckId::ARITY`), so a well-behaved
/// caller is never refused. A request at or above the cap gets the cap rather than
/// an error.
///
/// The cap bounds the hint only, never the answer: both stores add `BuckId::ARITY`
/// of headroom on top of the hint for the last bucket's changed siblings, and that
/// arithmetic runs after this cap, so the sibling-group guarantee survives a capped
/// request. A protocol knob, not a measured threshold.
const MAX_BUCKET_LIMIT: u32 = 1024;

/// Apply [`MAX_BUCKET_LIMIT`] to an untrusted bucket page hint.
fn bucket_limit(requested: u32) -> u32 {
    requested.min(MAX_BUCKET_LIMIT)
}

/// How many rpc requests may be handled at once.
///
/// A page request can hold for up to `MAX_PAGE_HOLD`, so a held request must not block
/// the loop that dispatches the next one: handling requests inline serializes every peer
/// behind one parked page request, and hides the stop token behind it too. The cap keeps
/// that concurrency bounded rather than letting a peer grow the handler set without
/// limit. A protocol knob, not a measured threshold.
const MAX_INFLIGHT_RPC_HANDLERS: usize = 64;

/// A request stamped with the storage scope it targets.
///
/// The `scope_key` string is the stable cross-peer scope identifier (the
/// integer `scope_id` is AUTOINCREMENT-local to each database and must never
/// go on the wire). The server routes each request to the scope-bound store
/// registered under this key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScopedRequest<T> {
    pub scope_key: Arc<str>,
    pub inner: T,
}

/// Wire-level BigSync RPC client: every request carries its scope.
///
/// Scope-bound callers wrap a [`WireBigSyncRpcClient`] in a [`ScopedRpcClient`]
/// so the machine-side [`HostBigRpcClient`] interface stays scope-agnostic.
#[async_trait]
pub trait WireBigSyncRpcClient: Send + Sync {
    async fn peer_summary(
        &self,
        req: ScopedRequest<PeerSummaryRequest>,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, PeerSummaryError>>>;

    async fn replay_page(
        &self,
        req: ScopedRequest<ReplayPageRequest>,
    ) -> Res<BigSyncRpcResult<ReplayPage>>;
    async fn replay_subscription(
        &self,
        req: ScopedRequest<ReplaySubscriptionRequest>,
    ) -> Res<BigSyncRpcResult<ReplaySubscriptionResponse>>;

    async fn get_changed_buckets(
        &self,
        req: ScopedRequest<GetChangedBucketsRequest>,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>>;

    async fn leaf_buckets(
        &self,
        req: ScopedRequest<LeafBucketsRequest>,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>>;
}

/// Scope-stamping adapter: implements the scope-agnostic [`HostBigRpcClient`]
/// by attaching this worker's `scope_key` to every outgoing request.
#[derive(Clone)]
pub struct ScopedRpcClient {
    pub scope_key: Arc<str>,
    pub inner: Arc<dyn WireBigSyncRpcClient>,
}

#[async_trait]
impl HostBigRpcClient for ScopedRpcClient {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, PeerSummaryError>>> {
        self.inner
            .peer_summary(ScopedRequest {
                scope_key: Arc::clone(&self.scope_key),
                inner: req,
            })
            .await
    }

    async fn replay_page(&self, req: ReplayPageRequest) -> Res<BigSyncRpcResult<ReplayPage>> {
        self.inner
            .replay_page(ScopedRequest {
                scope_key: Arc::clone(&self.scope_key),
                inner: req,
            })
            .await
    }
    async fn replay_subscription(
        &self,
        req: ReplaySubscriptionRequest,
    ) -> Res<BigSyncRpcResult<ReplaySubscriptionResponse>> {
        self.inner
            .replay_subscription(ScopedRequest {
                scope_key: Arc::clone(&self.scope_key),
                inner: req,
            })
            .await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
        self.inner
            .get_changed_buckets(ScopedRequest {
                scope_key: Arc::clone(&self.scope_key),
                inner: req,
            })
            .await
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
        self.inner
            .leaf_buckets(ScopedRequest {
                scope_key: Arc::clone(&self.scope_key),
                inner: req,
            })
            .await
    }
}

#[async_trait]
pub trait HostBigRpcClient: Send + Sync {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, PeerSummaryError>>>;

    async fn replay_page(&self, req: ReplayPageRequest) -> Res<BigSyncRpcResult<ReplayPage>>;
    async fn replay_subscription(
        &self,
        req: ReplaySubscriptionRequest,
    ) -> Res<BigSyncRpcResult<ReplaySubscriptionResponse>>;

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>>;

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>>;
}

#[rpc_requests(message = BigSyncRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum BigSyncIrpc {
    #[rpc(tx = channel::oneshot::Sender<Result<PeerSummaryResult, PeerSummaryError>>)]
    PeerSummary(ScopedRequest<PeerSummaryRequest>),
    #[rpc(tx = channel::oneshot::Sender<Result<ReplayPage, RpcError>>)]
    ReplayPage(ScopedRequest<ReplayPageRequest>),
    #[rpc(tx = channel::oneshot::Sender<Result<ReplaySubscriptionResponse, RpcError>>)]
    ReplaySubscription(ScopedRequest<ReplaySubscriptionRequest>),
    #[rpc(tx = channel::oneshot::Sender<Result<Vec<BucketSummary>, ListPartsError>>)]
    GetChangedBuckets(ScopedRequest<GetChangedBucketsRequest>),
    #[rpc(tx = channel::oneshot::Sender<Result<LeafBucketResult, LeafBucketsError>>)]
    LeafBuckets(ScopedRequest<LeafBucketsRequest>),
}
impl BigSyncRpcClient {
    pub fn over_iroh(endpoint: iroh::Endpoint, endpoint_addr: iroh::EndpointAddr) -> Self {
        Self::over_iroh_with_alpn(endpoint, endpoint_addr, BIG_SYNC_RPC_ALPN)
    }

    pub fn over_iroh_with_alpn(
        endpoint: iroh::Endpoint,
        endpoint_addr: iroh::EndpointAddr,
        alpn: &'static [u8],
    ) -> Self {
        Self {
            client: irpc_iroh::client::<BigSyncIrpc>(endpoint, endpoint_addr, alpn),
        }
    }
}
#[derive(Clone)]
pub struct BigSyncRpcHandle {
    client: irpc::Client<BigSyncIrpc>,
    protocol_handler: BigSyncRpcProtocolHandler,
    /// The worker's in-flight page registrations, which the worker itself owns. Held here only for
    /// tests: `spawn_big_sync_rpc` is the only constructor, and nothing outside the responder can
    /// observe a release by `supersede`.
    #[cfg(any(test, feature = "test-support"))]
    replay_cancels: Arc<std::sync::Mutex<ReplayCancellationRegistry>>,
}

/// One page request the responder is holding, and whether it has been asked to stop waiting.
///
/// A request retires its registration before it builds its reply, so an entry that exists is a
/// request that has not answered yet, and `cancelled` is set from the moment a `supersede` (or a
/// later request reusing the id) asks it to release. The bit is observable only while the request
/// is still in flight.
#[cfg(any(test, feature = "test-support"))]
pub struct ReplayRequestRegistration {
    pub peer: PeerKey,
    pub scope_key: Arc<str>,
    pub session_id: ReplaySessionId,
    pub request_id: big_sync_core::rpc::ReplayRequestId,
    pub cancelled: bool,
}

impl BigSyncRpcHandle {
    /// The responder's in-flight page requests, each with whether it has been asked to release.
    ///
    /// A client cannot tell a page released by a `supersede` from one that waited out its own hold:
    /// both answer with the same page and the same verdicts. This is the responder's own
    /// registration — the map `register_replay_request` writes and `forget_replay_request` removes —
    /// so a test can assert the release it caused rather than infer it from how long the answer
    /// took.
    #[cfg(any(test, feature = "test-support"))]
    pub fn replay_request_registrations(&self) -> Vec<ReplayRequestRegistration> {
        let cancels = self
            .replay_cancels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut registrations: Vec<ReplayRequestRegistration> = cancels
            .iter()
            .flat_map(|((peer, scope_key, session_id), per_scope)| {
                per_scope
                    .iter()
                    .map(move |(request_id, cancel)| ReplayRequestRegistration {
                        peer: peer.clone(),
                        scope_key: Arc::clone(scope_key),
                        session_id: *session_id,
                        request_id: *request_id,
                        cancelled: cancel.is_cancelled(),
                    })
            })
            .collect();
        registrations
            .sort_by_key(|registration| (registration.session_id.0, registration.request_id.0));
        registrations
    }

    pub fn local_sender(&self) -> irpc::LocalSender<BigSyncIrpc> {
        self.client.as_local().expect(ERROR_IMPOSSIBLE)
    }

    pub fn protocol_handler(&self) -> BigSyncRpcProtocolHandler {
        self.protocol_handler.clone()
    }

    /// A client that reaches this worker in the same process, with no socket.
    ///
    /// The caller's identity is supplied here rather than read off a connection, because a peer in
    /// this process has no connection to authenticate it. The messages travel the same queue the iroh
    /// protocol handler feeds, so the worker cannot tell the two apart and applies every rule it
    /// applies to a remote caller.
    pub fn in_memory_client(&self, caller: PeerKey) -> BigSyncRpcClient {
        const QUEUE: usize = 1024;
        let (tx, mut rx) = mpsc::channel(QUEUE);
        let authenticated = self.protocol_handler.tx.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if authenticated.send((caller.clone(), msg)).await.is_err() {
                    // The worker is gone. The caller learns that from its own request, not here.
                    break;
                }
            }
        });
        BigSyncRpcClient {
            client: irpc::Client::<BigSyncIrpc>::local(tx),
        }
    }
}

#[derive(Clone)]
pub struct BigSyncRpcProtocolHandler {
    tx: mpsc::Sender<(PeerKey, BigSyncRpcMessage)>,
}

impl std::fmt::Debug for BigSyncRpcProtocolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BigSyncRpcProtocolHandler")
            .finish_non_exhaustive()
    }
}

impl iroh::protocol::ProtocolHandler for BigSyncRpcProtocolHandler {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let peer_id = PeerKey::new(*conn.remote_id().as_bytes());
        loop {
            let msg = match irpc_iroh::read_request::<BigSyncIrpc>(&conn).await {
                Ok(Some(msg)) => msg,
                Ok(None) => break,
                Err(err) => {
                    warn!(?err, "error reading big sync rpc request");
                    break;
                }
            };
            if self.tx.send((peer_id.clone(), msg)).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

pub struct BigSyncRpcStopToken {
    cancel_token: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl BigSyncRpcStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5))
            .await
            .wrap_err("failed stopping big sync rpc")
    }
}

pub async fn spawn_big_sync_rpc(
    stores: HashMap<Arc<str>, Arc<dyn HostPartStore>>,
) -> Res<(BigSyncRpcHandle, BigSyncRpcStopToken)> {
    let (rpc_tx, mut rpc_rx) = mpsc::channel(1024);
    let (authenticated_tx, mut authenticated_rx) = mpsc::channel(1024);
    let client = irpc::Client::<BigSyncIrpc>::local(rpc_tx);

    let cancel_token = CancellationToken::new();
    let replay_cancels: Arc<std::sync::Mutex<ReplayCancellationRegistry>> = default();
    let fut = {
        let cancel_token = cancel_token.clone();
        let worker = Arc::new(BigSyncRpcWorker {
            stores,
            replay_cancels: Arc::clone(&replay_cancels),
            replay_subscriptions: default(),
        });
        let permits = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_RPC_HANDLERS));
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = rpc_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        spawn_rpc_handler(&worker, &permits, msg, None);
                    }
                    authenticated = authenticated_rx.recv() => {
                        let Some((peer_id, msg)) = authenticated else {
                            break;
                        };
                        spawn_rpc_handler(&worker, &permits, msg, Some(peer_id));
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });

    Ok((
        BigSyncRpcHandle {
            client,
            protocol_handler: BigSyncRpcProtocolHandler {
                tx: authenticated_tx,
            },
            #[cfg(any(test, feature = "test-support"))]
            replay_cancels,
        },
        BigSyncRpcStopToken {
            cancel_token,
            join_handle,
        },
    ))
}

/// Handle one request off the dispatch loop, under the inflight cap.
///
/// A panic inside a handler is not swallowed: the process-wide panic handler takes the
/// whole process down, which is the intended behaviour for an invariant break here.
fn spawn_rpc_handler(
    worker: &Arc<BigSyncRpcWorker>,
    permits: &Arc<tokio::sync::Semaphore>,
    msg: BigSyncRpcMessage,
    authenticated_peer: Option<PeerKey>,
) {
    let worker = Arc::clone(worker);
    let permits = Arc::clone(permits);
    tokio::spawn(async move {
        let _permit = permits
            .acquire_owned()
            .await
            .expect("rpc handler permits are never closed");
        worker.handle_rpc_message(msg, authenticated_peer).await;
    });
}

#[derive(Clone)]
pub struct BigSyncRpcClient {
    client: irpc::Client<BigSyncIrpc>,
}

#[async_trait]
impl WireBigSyncRpcClient for BigSyncRpcClient {
    async fn peer_summary(
        &self,
        req: ScopedRequest<PeerSummaryRequest>,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, PeerSummaryError>>> {
        let response = match self.client.rpc(req).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "big sync peer_summary rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };
        Ok(Ok(response))
    }

    async fn replay_page(
        &self,
        req: ScopedRequest<ReplayPageRequest>,
    ) -> Res<BigSyncRpcResult<ReplayPage>> {
        let response = match self.client.rpc(req).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "big sync replay_page rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };
        Ok(response)
    }
    async fn replay_subscription(
        &self,
        req: ScopedRequest<ReplaySubscriptionRequest>,
    ) -> Res<BigSyncRpcResult<ReplaySubscriptionResponse>> {
        let response = match self.client.rpc(req).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "big sync replay_subscription rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };
        Ok(response)
    }

    async fn get_changed_buckets(
        &self,
        req: ScopedRequest<GetChangedBucketsRequest>,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>> {
        let response = match self.client.rpc(req).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "big sync get_changed_buckets rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };
        Ok(Ok(response))
    }

    async fn leaf_buckets(
        &self,
        req: ScopedRequest<LeafBucketsRequest>,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>> {
        let response = match self.client.rpc(req).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "big sync leaf_buckets rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };
        Ok(Ok(response))
    }
}
const MAX_REPLAY_SUBSCRIPTIONS_PER_PEER: usize = 8;
const MAX_REPLAY_SUBSCRIPTION_TARGETS: usize = 16_384;
const MAX_REPLAY_SUBSCRIPTION_TARGET_BYTES: usize = 2 * 1024 * 1024;
const REPLAY_SUBSCRIPTION_TTL: Duration = Duration::from_secs(10 * 60);

/// The `ReadTarget` a subscription target's authorization is asked through.
fn replay_target_read_target(target: &big_sync_core::rpc::ReplaySubscriptionTarget) -> ReadTarget {
    match target {
        big_sync_core::rpc::ReplaySubscriptionTarget::Part { part_id } => {
            ReadTarget::Part(part_id.clone())
        }
        big_sync_core::rpc::ReplaySubscriptionTarget::Object { obj_id } => {
            ReadTarget::Object(obj_id.clone())
        }
    }
}

/// Whether the scope has the target at all: the existence half of admitting an addition.
///
/// A part is asked through `summarize_parts`, which is what the page read itself asks to
/// decide a target's `UnknownPart` verdict, so an absent part and a hidden one are one
/// answer here too. An object target has no unknown-object verdict to be answered with, and
/// the store answers a read of an object it does not hold as an empty drained page rather
/// than as an unknown target, so authorization is the whole admission question for it.
async fn replay_target_exists(
    store: &dyn HostPartStore,
    target: &big_sync_core::rpc::ReplaySubscriptionTarget,
) -> Res<bool> {
    match target {
        big_sync_core::rpc::ReplaySubscriptionTarget::Part { part_id } => Ok(store
            .summarize_parts(HashSet::from([part_id.clone()]))
            .await?
            .is_ok()),
        big_sync_core::rpc::ReplaySubscriptionTarget::Object { .. } => Ok(true),
    }
}

fn replay_event_matches_target(
    event: &big_sync_core::rpc::PartEvent,
    target: &big_sync_core::rpc::ReplaySubscriptionTarget,
) -> bool {
    match (event, target) {
        (
            big_sync_core::rpc::PartEvent::Changed(changed),
            big_sync_core::rpc::ReplaySubscriptionTarget::Part { part_id },
        ) => changed
            .part_ids
            .iter()
            .any(|candidate| candidate == part_id),
        (
            big_sync_core::rpc::PartEvent::Removed(removed),
            big_sync_core::rpc::ReplaySubscriptionTarget::Part { part_id },
        ) => &removed.part_id == part_id,
        (
            big_sync_core::rpc::PartEvent::Changed(changed),
            big_sync_core::rpc::ReplaySubscriptionTarget::Object { obj_id },
        ) => &changed.obj_id == obj_id,
        (
            big_sync_core::rpc::PartEvent::Removed(removed),
            big_sync_core::rpc::ReplaySubscriptionTarget::Object { obj_id },
        ) => &removed.obj_id == obj_id,
    }
}

/// One `Update` as it arrived: the subscription it names and the changes it carries.
struct ReplaySubscriptionUpdate {
    session_id: ReplaySessionId,
    subscription_id: big_sync_core::rpc::ReplaySubscriptionId,
    generation: u64,
    additions: Vec<big_sync_core::rpc::ReplaySubscriptionTargetEntry>,
    removals: Vec<big_sync_core::rpc::ReplayTargetId>,
}

/// One applied reconfiguration: the request's generation, with the additions the store's own
/// admission accepted separated from the ones it refused.
struct AppliedReplayUpdate {
    subscription_id: big_sync_core::rpc::ReplaySubscriptionId,
    generation: u64,
    additions: Vec<big_sync_core::rpc::ReplaySubscriptionTargetEntry>,
    removals: Vec<big_sync_core::rpc::ReplayTargetId>,
    rejected: Vec<(big_sync_core::rpc::ReplayTargetId, TargetVerdict)>,
}

#[derive(Clone)]
struct ReplaySubscriptionState {
    generation: u64,
    targets:
        HashMap<big_sync_core::rpc::ReplayTargetId, big_sync_core::rpc::ReplaySubscriptionTarget>,
    /// The entries the generation above refused. A repeat of that generation is answered with
    /// the same verdicts: the caller's retry of an update whose answer was lost must not be
    /// told that its refused entries landed.
    rejected: Vec<(big_sync_core::rpc::ReplayTargetId, TargetVerdict)>,
    changed: Arc<tokio::sync::Notify>,
    last_touched: std::time::Instant,
}

type ReplaySessionKey = (PeerKey, Arc<str>, ReplaySessionId);
type ReplaySubscriptionRegistry = HashMap<
    ReplaySessionKey,
    HashMap<big_sync_core::rpc::ReplaySubscriptionId, ReplaySubscriptionState>,
>;
type ReplayCancellationRegistry =
    HashMap<ReplaySessionKey, HashMap<big_sync_core::rpc::ReplayRequestId, Arc<CancellationToken>>>;

struct BigSyncRpcWorker {
    stores: HashMap<Arc<str>, Arc<dyn HostPartStore>>,
    /// The in-flight page requests of each peer and storage scope, so a request that supersedes one can drop the
    /// older one if it is still waiting. An entry lives only as long as the request it names —
    /// inserted when the request starts, removed when it answers — so a peer can only ever name
    /// its own in-flight requests within that scope, and nothing outlives the request it belongs to.
    ///
    /// Behind an `Arc` because `spawn_big_sync_rpc` keeps a second handle on it: a client cannot
    /// see the difference between a page released by a `supersede` and one that waited out its own
    /// hold, so the responder's own registration is the only place that release is observable.
    replay_cancels: Arc<std::sync::Mutex<ReplayCancellationRegistry>>,
    replay_subscriptions: std::sync::Mutex<ReplaySubscriptionRegistry>,
}

impl BigSyncRpcWorker {
    /// The in-flight registrations, taken without ever propagating a poisoned lock.
    ///
    /// A request id arrives from a remote peer, so nothing this registry does may take down the
    /// dispatch loop: a panic raised elsewhere while the lock was held would otherwise turn one
    /// bad request into every later request failing.
    fn replay_cancels(&self) -> std::sync::MutexGuard<'_, ReplayCancellationRegistry> {
        self.replay_cancels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn sweep_replay_subscriptions(&self) {
        let now = std::time::Instant::now();
        let mut registry = self
            .replay_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.retain(|_, subscriptions| {
            subscriptions.retain(|_, subscription| {
                now.duration_since(subscription.last_touched) < REPLAY_SUBSCRIPTION_TTL
            });
            !subscriptions.is_empty()
        });
    }

    /// Apply one reconfiguration to a subscription, opening it when the id is unknown.
    ///
    /// Opening is folded into the update (ADR 012 decision 9): the same call carries the
    /// additions, so opening gets the same per-entry outcomes as any later change. Removals
    /// are applied before additions, so one request may remove a target and re-add the same
    /// part under a fresh id, and a removal of an id the subscription does not hold is a
    /// no-op: a client that lost the answer to its own update has to be able to remove an
    /// entry it cannot know the responder took.
    ///
    /// An addition is inserted only when the scope has the target and the asker may read it;
    /// anything else comes back in `rejected`, which is what lets the client retry exactly
    /// those entries. Both refusal kinds are one outcome for the client, because absent access
    /// rows cannot distinguish a revocation from a grant that has not landed yet.
    async fn update_replay_subscription(
        &self,
        store: &dyn HostPartStore,
        subscriber: &PeerKey,
        scope_key: &Arc<str>,
        update: ReplaySubscriptionUpdate,
    ) -> Result<ReplaySubscriptionResponse, RpcError> {
        let ReplaySubscriptionUpdate {
            session_id,
            subscription_id,
            generation,
            additions,
            removals,
        } = update;
        self.sweep_replay_subscriptions();
        if additions.len() + removals.len() > MAX_REPLAY_SUBSCRIPTION_TARGETS {
            return Err(RpcError::SubscriptionLimit);
        }
        let encoded = additions
            .iter()
            .map(|entry| replay_target_wire_bytes(&entry.target))
            .sum::<usize>();
        if encoded > MAX_REPLAY_SUBSCRIPTION_TARGET_BYTES {
            return Err(RpcError::SubscriptionLimit);
        }
        // The ids are the caller's own, so two of them in one request is a caller bug rather
        // than a target this responder has to adjudicate.
        let mut removal_ids = std::collections::HashSet::new();
        for id in &removals {
            if !removal_ids.insert(*id) {
                return Err(RpcError::InvalidRequest(
                    "duplicate replay target removal".into(),
                ));
            }
        }
        let mut addition_ids = std::collections::HashSet::new();
        for entry in &additions {
            if !addition_ids.insert(entry.id) {
                return Err(RpcError::InvalidRequest(
                    "duplicate replay target addition".into(),
                ));
            }
        }
        let key = (subscriber.clone(), Arc::clone(scope_key), session_id);
        // The standing is read before the store reads below, and the apply re-checks it: an
        // update a newer one overtook in the meantime is answered as not applied rather than
        // clobbering the newer generation's target set.
        match self.replay_subscription_standing(&key, subscription_id) {
            // Only a fresh client state opens a subscription. An update ahead of an unknown id
            // belongs to a handle this responder has forgotten (its TTL, or a restart), and
            // answering as unknown is what makes the client re-state its whole target set
            // instead of opening with a delta that silently misses the entries it thinks the
            // responder holds.
            None if generation != 0 => return Err(RpcError::UnknownSubscription),
            None => {}
            Some((current, rejected)) if generation <= current => {
                return Ok(ReplaySubscriptionResponse::Updated {
                    generation: current,
                    rejected,
                });
            }
            Some(_) => {}
        }
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for entry in additions {
            let exists = replay_target_exists(store, &entry.target)
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = ?error,
                        "replay subscription addition existence check failed"
                    );
                    RpcError::Internal
                })?;
            let verdict = if !exists {
                Some(TargetVerdict::UnknownPart)
            } else if store
                .read_denied(replay_target_read_target(&entry.target), subscriber.clone())
                .await
                .map_err(|error| {
                    tracing::error!(
                        error = ?error,
                        "replay subscription addition access check failed"
                    );
                    RpcError::Internal
                })?
            {
                Some(TargetVerdict::Unauthorized)
            } else {
                None
            };
            match verdict {
                Some(verdict) => {
                    // A refusal is how a peer learns it may not have what it asked for, so the
                    // reason belongs in the log where an operator can see the rate.
                    tracing::debug!(
                        ?verdict,
                        %subscriber,
                        target = ?entry.target,
                        "replay subscription addition refused"
                    );
                    rejected.push((entry.id, verdict));
                }
                None => accepted.push(entry),
            }
        }
        self.apply_replay_update(
            &key,
            AppliedReplayUpdate {
                subscription_id,
                generation,
                additions: accepted,
                removals,
                rejected,
            },
        )
    }

    /// The generation a subscription holds and the entries that generation refused, or `None`
    /// when this responder does not hold the id.
    fn replay_subscription_standing(
        &self,
        key: &ReplaySessionKey,
        subscription_id: big_sync_core::rpc::ReplaySubscriptionId,
    ) -> Option<(
        u64,
        Vec<(big_sync_core::rpc::ReplayTargetId, TargetVerdict)>,
    )> {
        let registry = self
            .replay_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registry.get(key).and_then(|subscriptions| {
            subscriptions
                .get(&subscription_id)
                .map(|state| (state.generation, state.rejected.clone()))
        })
    }

    /// Write one applied update, opening the subscription when the id is unknown.
    fn apply_replay_update(
        &self,
        key: &ReplaySessionKey,
        applied: AppliedReplayUpdate,
    ) -> Result<ReplaySubscriptionResponse, RpcError> {
        let AppliedReplayUpdate {
            subscription_id,
            generation,
            additions,
            removals,
            rejected,
        } = applied;
        let mut registry = self
            .replay_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (peer, _, _) = key;
        let peer_subscription_count = registry
            .iter()
            .filter(|((registered_peer, _, _), _)| registered_peer == peer)
            .map(|(_, subscriptions)| subscriptions.len())
            .sum::<usize>();
        let subscriptions = registry.entry(key.clone()).or_default();
        if !subscriptions.contains_key(&subscription_id)
            && peer_subscription_count >= MAX_REPLAY_SUBSCRIPTIONS_PER_PEER
        {
            return Err(RpcError::SubscriptionLimit);
        }
        let Some(state) = subscriptions.get_mut(&subscription_id) else {
            // This update opens the subscription, so its additions are the whole target set
            // and there is nothing to reconcile them with.
            let targets: HashMap<_, _> = additions
                .into_iter()
                .map(|entry| (entry.id, entry.target))
                .collect();
            subscriptions.insert(
                subscription_id,
                ReplaySubscriptionState {
                    generation,
                    targets,
                    rejected: rejected.clone(),
                    changed: Arc::new(tokio::sync::Notify::new()),
                    last_touched: std::time::Instant::now(),
                },
            );
            return Ok(ReplaySubscriptionResponse::Updated {
                generation,
                rejected,
            });
        };
        if generation <= state.generation {
            // A newer update overtook this one while the store was being read.
            return Ok(ReplaySubscriptionResponse::Updated {
                generation: state.generation,
                rejected: state.rejected.clone(),
            });
        }
        let current_target_bytes = state
            .targets
            .values()
            .map(replay_target_wire_bytes)
            .sum::<usize>();
        let removed_target_bytes = removals
            .iter()
            .filter_map(|id| state.targets.get(id))
            .map(replay_target_wire_bytes)
            .sum::<usize>();
        let added_target_bytes = additions
            .iter()
            .map(|entry| replay_target_wire_bytes(&entry.target))
            .sum::<usize>();
        if current_target_bytes
            .saturating_sub(removed_target_bytes)
            .saturating_add(added_target_bytes)
            > MAX_REPLAY_SUBSCRIPTION_TARGET_BYTES
        {
            return Err(RpcError::SubscriptionLimit);
        }
        let new_target_count = state
            .targets
            .len()
            .saturating_sub(
                removals
                    .iter()
                    .filter(|id| state.targets.contains_key(*id))
                    .count(),
            )
            .saturating_add(
                additions
                    .iter()
                    .filter(|entry| !state.targets.contains_key(&entry.id))
                    .count(),
            );
        if new_target_count > MAX_REPLAY_SUBSCRIPTION_TARGETS {
            return Err(RpcError::SubscriptionLimit);
        }
        // Removals before additions: one request may remove a target and re-add the same
        // part under a fresh id, and an id the subscription does not hold is a no-op here.
        for id in removals {
            state.targets.remove(&id);
        }
        for entry in additions {
            state.targets.insert(entry.id, entry.target);
        }
        state.generation = generation;
        state.rejected = rejected.clone();
        state.last_touched = std::time::Instant::now();
        state.changed.notify_waiters();
        Ok(ReplaySubscriptionResponse::Updated {
            generation,
            rejected,
        })
    }

    fn close_replay_subscription(
        &self,
        peer: &PeerKey,
        scope_key: &Arc<str>,
        session_id: ReplaySessionId,
        subscription_id: big_sync_core::rpc::ReplaySubscriptionId,
    ) -> Result<ReplaySubscriptionResponse, RpcError> {
        self.sweep_replay_subscriptions();
        let key = (peer.clone(), Arc::clone(scope_key), session_id);
        let mut registry = self
            .replay_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(subscriptions) = registry.get_mut(&key) else {
            return Err(RpcError::UnknownSubscription);
        };
        let Some(subscription) = subscriptions.remove(&subscription_id) else {
            return Err(RpcError::UnknownSubscription);
        };
        subscription.changed.notify_waiters();
        if subscriptions.is_empty() {
            registry.remove(&key);
        }
        Ok(ReplaySubscriptionResponse::Closed)
    }

    fn replay_subscription_snapshot(
        &self,
        peer: &PeerKey,
        scope_key: &Arc<str>,
        session_id: ReplaySessionId,
        subscription_id: big_sync_core::rpc::ReplaySubscriptionId,
    ) -> Result<ReplaySubscriptionState, RpcError> {
        self.sweep_replay_subscriptions();
        let key = (peer.clone(), Arc::clone(scope_key), session_id);
        let mut registry = self
            .replay_subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let subscriptions = registry
            .get_mut(&key)
            .ok_or(RpcError::UnknownSubscription)?;
        let state = subscriptions
            .get_mut(&subscription_id)
            .ok_or(RpcError::UnknownSubscription)?;
        state.last_touched = std::time::Instant::now();
        Ok(state.clone())
    }
    async fn handle_replay_subscription(
        &self,
        scope_key: Arc<str>,
        subscriber: PeerKey,
        request: ReplaySubscriptionRequest,
    ) -> Result<ReplaySubscriptionResponse, RpcError> {
        match request {
            ReplaySubscriptionRequest::Update {
                session_id,
                subscription_id,
                generation,
                additions,
                removals,
            } => {
                let Some(store) = self.stores.get(&scope_key) else {
                    return Err(RpcError::InvalidRequest("unknown storage scope".into()));
                };
                self.update_replay_subscription(
                    store.as_ref(),
                    &subscriber,
                    &scope_key,
                    ReplaySubscriptionUpdate {
                        session_id,
                        subscription_id,
                        generation,
                        additions,
                        removals,
                    },
                )
                .await
            }
            ReplaySubscriptionRequest::Close {
                session_id,
                subscription_id,
            } => {
                self.close_replay_subscription(&subscriber, &scope_key, session_id, subscription_id)
            }
            ReplaySubscriptionRequest::Next {
                session_id,
                subscription_id,
                request_id,
                supersede,
                targets,
                limit,
                hold_ms,
            } => {
                let Some(store) = self.stores.get(&scope_key) else {
                    return Err(RpcError::InvalidRequest("unknown storage scope".into()));
                };
                let snapshot = self.replay_subscription_snapshot(
                    &subscriber,
                    &scope_key,
                    session_id,
                    subscription_id,
                )?;
                let requested_ids: std::collections::HashSet<_> =
                    targets.iter().map(|(id, _)| *id).collect();
                if requested_ids.len() != targets.len() {
                    return Err(RpcError::InvalidRequest(
                        "duplicate replay target id".into(),
                    ));
                }
                let requested = targets
                    .into_iter()
                    .map(|(id, cursor)| {
                        snapshot
                            .targets
                            .get(&id)
                            .map(|target| target.with_cursor(cursor))
                            .ok_or(RpcError::InvalidRequest("unknown replay target id".into()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                validate_replay_targets(&requested)
                    .map_err(|error| RpcError::InvalidRequest(error.to_string()))?;
                let cancel = self.register_replay_request(
                    &subscriber,
                    &scope_key,
                    session_id,
                    request_id,
                    CancellationToken::new(),
                );
                if let Some(superseded) = supersede {
                    self.cancel_replay_request(&subscriber, &scope_key, session_id, superseded);
                }
                let replay_started = std::time::Instant::now();
                tracing::debug!(
                    peer = %subscriber,
                    ?request_id,
                    target_count = requested.len(),
                    hold_ms,
                    "replay subscription page begin",
                );
                let result = store
                    .replay_page_round_with_update(
                        ReplayPageRequest {
                            session_id,
                            request_id,
                            supersede,
                            targets: requested,
                            limit: page_limit(limit),
                            hold_ms,
                        },
                        subscriber.clone(),
                        Duration::from_millis(u64::from(hold_ms)).min(MAX_PAGE_HOLD),
                        cancel.as_ref().clone(),
                        Some(snapshot.changed),
                    )
                    .await;
                match &result {
                    Ok(page) => {
                        let drained_count = page
                            .targets
                            .iter()
                            .filter(|(_, verdict)| {
                                matches!(verdict, TargetVerdict::Events { drained: true, .. })
                            })
                            .count();
                        tracing::debug!(
                            peer = %subscriber,
                            ?request_id,
                            elapsed_ms = replay_started.elapsed().as_millis(),
                            event_count = page.events.len(),
                            target_count = page.targets.len(),
                            drained_count,
                            "replay subscription page store response",
                        );
                    }
                    Err(error) => tracing::debug!(
                        peer = %subscriber,
                        ?request_id,
                        elapsed_ms = replay_started.elapsed().as_millis(),
                        ?error,
                        "replay subscription page store error",
                    ),
                }
                self.forget_replay_request(
                    &subscriber,
                    &scope_key,
                    session_id,
                    request_id,
                    &cancel,
                );
                let page = result.map_err(|error| {
                    tracing::error!(error = ?error, "replay subscription page failed");
                    RpcError::Internal
                })?;
                let latest = self.replay_subscription_snapshot(
                    &subscriber,
                    &scope_key,
                    session_id,
                    subscription_id,
                )?;
                let reverse: HashMap<_, _> = latest
                    .targets
                    .iter()
                    .map(|(id, target)| (target.clone(), *id))
                    .collect();
                let events = page
                    .events
                    .into_iter()
                    .filter(|event| {
                        latest
                            .targets
                            .values()
                            .any(|target| replay_event_matches_target(event, target))
                    })
                    .collect();
                let target_verdicts = page
                    .targets
                    .into_iter()
                    .filter_map(|(target, verdict)| {
                        let id = reverse.get(&ReplaySubscriptionTarget::from(&target))?;
                        requested_ids.contains(id).then_some((*id, verdict))
                    })
                    .collect();
                Ok(ReplaySubscriptionResponse::Page(ReplaySubscriptionPage {
                    page: ReplayPage {
                        events,
                        targets: Vec::new(),
                    },
                    targets: target_verdicts,
                }))
            }
        }
    }

    /// Register a request's own cancellation for as long as it is in flight.
    ///
    /// A reused id is a normal event on a peer-supplied id, not an invariant to crash on: the
    /// newer request is the live one, so the older registration for that id is cancelled and
    /// replaced — which is the same thing a `supersede` names.
    fn register_replay_request(
        &self,
        peer: &PeerKey,
        scope_key: &Arc<str>,
        session_id: ReplaySessionId,
        request_id: big_sync_core::rpc::ReplayRequestId,
        cancel: CancellationToken,
    ) -> Arc<CancellationToken> {
        let cancel = Arc::new(cancel);
        let mut cancels = self.replay_cancels();
        let key = (peer.clone(), Arc::clone(scope_key), session_id);
        let per_scope = cancels.entry(key).or_default();
        if let Some(previous) = per_scope.insert(request_id, Arc::clone(&cancel)) {
            previous.cancel();
        }
        cancel
    }

    /// Drop a request's registration once it has answered, but only if this request still owns it.
    fn forget_replay_request(
        &self,
        peer: &PeerKey,
        scope_key: &Arc<str>,
        session_id: ReplaySessionId,
        request_id: big_sync_core::rpc::ReplayRequestId,
        registration: &Arc<CancellationToken>,
    ) {
        let mut cancels = self.replay_cancels();
        let key = (peer.clone(), Arc::clone(scope_key), session_id);
        if let Some(per_scope) = cancels.get_mut(&key) {
            let owned = per_scope
                .get(&request_id)
                .is_some_and(|current| Arc::ptr_eq(current, registration));
            if owned {
                per_scope.remove(&request_id);
                if per_scope.is_empty() {
                    cancels.remove(&key);
                }
            }
        }
    }

    /// Ask one in-flight request of this peer to stop waiting.
    ///
    /// Best effort by construction: a request whose read already produced rows ships them
    /// regardless, and one that is waiting exits at its next check point. An id this peer does
    /// not have in flight is a no-op.
    fn cancel_replay_request(
        &self,
        peer: &PeerKey,
        scope_key: &Arc<str>,
        session_id: ReplaySessionId,
        request_id: big_sync_core::rpc::ReplayRequestId,
    ) {
        let key = (peer.clone(), Arc::clone(scope_key), session_id);
        let cancel = {
            let cancels = self.replay_cancels();
            cancels
                .get(&key)
                .and_then(|per_scope| per_scope.get(&request_id))
                .cloned()
        };
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
    }
}

/// Ceiling on the parts set one `PeerSummary` request may name. Remote input;
/// see the refusal in the message handler.
const MAX_SUMMARY_PARTS: usize = 64;

impl BigSyncRpcWorker {
    #[tracing::instrument(
        skip(self, msg),
        fields(
            peer_id = tracing::field::Empty,
            // The responder half of the wire protocol, so an OpenTelemetry export has to
            // map this span to a server span.
            otel.kind = "server",
        )
    )]
    async fn handle_rpc_message(
        &self,
        msg: BigSyncRpcMessage,
        authenticated_peer: Option<PeerKey>,
    ) {
        // The caller is authenticated by the transport, so it is only known here, and a
        // span field cannot be added after creation: it is declared empty above and
        // filled in before any arm can log. An unauthenticated call leaves it unset,
        // which is itself the finding.
        if let Some(peer_id) = &authenticated_peer {
            tracing::Span::current().record("peer_id", tracing::field::display(peer_id));
        }
        match msg {
            BigSyncRpcMessage::PeerSummary(req) => {
                let WithChannels { inner, tx, .. } = req;
                // The parts set is remote input; the handler answers per part.
                // A peer that names thousands of parts makes this handler run
                // store work for every one of them; the answer set is bounded
                // instead (a summary request names a peer's newly pending
                // parts - the number of told inventories - so this ceiling is
                // orders of magnitude above any honest request).
                //
                // Over the ceiling the refusal is the whole request, and it is answered
                // as an error: an empty answer map would read to the asker as an idle peer.
                if inner.inner.parts.len() > MAX_SUMMARY_PARTS {
                    let requested = inner.inner.parts.len();
                    warn!(
                        requested,
                        cap = MAX_SUMMARY_PARTS,
                        "peer_summary names too many parts; refusing all"
                    );
                    tx.send(Err(PeerSummaryError::TooManyParts {
                        requested,
                        cap: MAX_SUMMARY_PARTS,
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                }
                let Some(store) = self.stores.get(&inner.scope_key) else {
                    warn!(scope_key = %inner.scope_key, "peer_summary for unknown scope");
                    tx.send(Ok(PeerSummaryResult {
                        parts: HashMap::new(),
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                // An unauthenticated caller is refused on every arm: this surface has
                // no local caller to serve, and a local in-process caller reaches the
                // store directly. An empty answer refuses every named part.
                let Some(asker) = authenticated_peer else {
                    warn!(scope_key = %inner.scope_key, "rejecting unauthenticated peer_summary request");
                    tx.send(Ok(PeerSummaryResult {
                        parts: HashMap::new(),
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                let out = {
                    // The count is a fact about the asker, so the responder counts it
                    // against the cursor the asker advertised.
                    let PeerSummaryRequest {
                        parts,
                        asker_part_cursors,
                    } = inner.inner;
                    // The answer is per part. A part the asker may not read is
                    // simply absent from the answer map - denied and missing fold
                    // together in what the map omits - but a part's absence must
                    // never swallow a readable part's summary riding the same
                    // request: a granted part starved beside an ungranted one is
                    // the batch-refusal defect this per-part shape exists to close.
                    // The store's own `summarize_parts` contract still folds an
                    // unknown part into an error, so each part is summarized on its
                    // own; a summary request names a peer's newly pending parts (the
                    // number of told inventories), not an unbounded set.
                    let mut summaries = HashMap::new();
                    for part_id in parts {
                        if store
                            .read_denied(ReadTarget::Part(part_id.clone()), asker.clone())
                            .await
                            .expect(ERROR_IMPOSSIBLE)
                        {
                            continue;
                        }
                        if let Ok(mut parts) = store
                            .summarize_parts(HashSet::from([part_id.clone()]))
                            .await
                            .expect(ERROR_IMPOSSIBLE)
                        {
                            let Some(summary) = parts.remove(&part_id) else {
                                continue;
                            };
                            let since = asker_part_cursors.get(&part_id).copied().unwrap_or(0);
                            let dirty = store
                                .part_dirty_count(part_id.clone(), Some(asker.clone()), since)
                                .await
                                .expect(ERROR_IMPOSSIBLE);
                            summaries.insert(part_id, summary.into_strat_summaries(dirty));
                        }
                        // The store reports an unknown part by name - that part is
                        // just absent from the answer; this part passed the
                        // readability pre-pass so the race is unknown-parts only.
                    }
                    PeerSummaryResult { parts: summaries }
                };
                tx.send(Ok(out))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            BigSyncRpcMessage::ReplaySubscription(req) => {
                let WithChannels { inner, tx, .. } = req;
                let Some(subscriber) = authenticated_peer else {
                    tx.send(Err(RpcError::Unauthorized))
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return;
                };
                let out = self
                    .handle_replay_subscription(inner.scope_key, subscriber, inner.inner)
                    .await;
                tx.send(out)
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            BigSyncRpcMessage::ReplayPage(req) => {
                let WithChannels { inner, tx, .. } = req;
                let ReplayPageRequest {
                    session_id,
                    request_id,
                    supersede,
                    targets,
                    limit,
                    hold_ms,
                } = inner.inner;
                if let Err(err) = validate_replay_targets(&targets) {
                    tx.send(Err(RpcError::InvalidRequest(err.to_string())))
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return;
                }
                // An unauthenticated caller is not a special case of "nothing to send": every
                // target it named is unauthorized, and the page says so per target.
                let Some(subscriber) = authenticated_peer else {
                    warn!("rejecting unauthenticated replay_page request");
                    let targets = targets
                        .into_iter()
                        .map(|target| (target, TargetVerdict::Unauthorized))
                        .collect();
                    tx.send(Ok(ReplayPage {
                        events: Vec::new(),
                        targets,
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                let Some(store) = self.stores.get(&inner.scope_key) else {
                    warn!(scope_key = %inner.scope_key, "replay_page for unknown scope");
                    let targets = targets
                        .into_iter()
                        .map(|target| (target, TargetVerdict::UnknownPart))
                        .collect();
                    tx.send(Ok(ReplayPage {
                        events: Vec::new(),
                        targets,
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                // This request's own cancellation, registered for the lifetime of the call so
                // that a request superseding it reaches this request's task. The responder
                // only ever drops a request that is still waiting: one whose read already
                // produced rows ships them.
                let cancel = self.register_replay_request(
                    &subscriber,
                    &inner.scope_key,
                    session_id,
                    request_id,
                    CancellationToken::new(),
                );
                if let Some(superseded) = supersede {
                    self.cancel_replay_request(
                        &subscriber,
                        &inner.scope_key,
                        session_id,
                        superseded,
                    );
                }
                let out = match store
                    .replay_page_round(
                        ReplayPageRequest {
                            session_id,
                            request_id,
                            supersede,
                            targets,
                            limit: page_limit(limit),
                            hold_ms,
                        },
                        subscriber.clone(),
                        Duration::from_millis(u64::from(hold_ms)).min(MAX_PAGE_HOLD),
                        cancel.as_ref().clone(),
                    )
                    .await
                {
                    Ok(page) => Ok(page),
                    Err(err) => {
                        tracing::error!(error = ?err, "replay page failed");
                        Err(RpcError::Internal)
                    }
                };
                self.forget_replay_request(
                    &subscriber,
                    &inner.scope_key,
                    session_id,
                    request_id,
                    &cancel,
                );
                // The response channel closes when the caller drops the request — the
                // ordinary outcome for a page the caller superseded or gave up on, which is
                // what `ERROR_CALLER` names. That is a signal about the caller's lifecycle,
                // not this task's, so a failed send is reported and dropped rather than
                // panicking here: a peer that drops a request must not be able to take the
                // process down. Every response send in this dispatcher is written this way.
                tx.send(out)
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            BigSyncRpcMessage::GetChangedBuckets(req) => {
                let WithChannels { inner, tx, .. } = req;
                let Some(store) = self.stores.get(&inner.scope_key) else {
                    warn!(scope_key = %inner.scope_key, "get_changed_buckets for unknown scope");
                    tx.send(Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![],
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                // As in `ReplayPage`: an unauthenticated caller is unauthorized, and
                // says so in the shape a refused part has.
                let Some(subscriber) = authenticated_peer else {
                    warn!(scope_key = %inner.scope_key, "rejecting unauthenticated get_changed_buckets request");
                    tx.send(Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![inner.inner.part_id.clone()],
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                let mut request = inner.inner;
                request.limit_hint = bucket_limit(request.limit_hint);
                let out = store
                    .get_changed_buckets(request, subscriber)
                    .await
                    .unwrap();
                tx.send(out)
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            BigSyncRpcMessage::LeafBuckets(req) => {
                let WithChannels { inner, tx, .. } = req;
                let Some(store) = self.stores.get(&inner.scope_key) else {
                    warn!(scope_key = %inner.scope_key, "leaf_buckets for unknown scope");
                    tx.send(Err(LeafBucketsError::UnkownPart))
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return;
                };
                let Some(subscriber) = authenticated_peer else {
                    warn!(scope_key = %inner.scope_key, "rejecting unauthenticated leaf_buckets request");
                    tx.send(Err(LeafBucketsError::UnkownPart))
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return;
                };
                let mut request = inner.inner;
                request.limit_hint = bucket_limit(request.limit_hint);
                let out = store.leaf_buckets(request, subscriber).await.unwrap();
                tx.send(out)
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
        }
    }
}

fn replay_target_wire_bytes(target: &big_sync_core::rpc::ReplaySubscriptionTarget) -> usize {
    16 + match target {
        big_sync_core::rpc::ReplaySubscriptionTarget::Part { part_id } => part_id.as_bytes().len(),
        big_sync_core::rpc::ReplaySubscriptionTarget::Object { obj_id } => obj_id.as_bytes().len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::part_store::HostPartStore;
    use crate::part_store::memory::MemoryPartStore;
    use big_sync_core::rpc::{
        LeafBucketRequest, PartEvent, ReplayPageRequest, ReplayRequestId, ReplaySessionId,
        ReplaySubscriptionId, ReplaySubscriptionResponse, ReplaySubscriptionTarget,
        ReplaySubscriptionTargetEntry, ReplayTargetId, RpcError, SubscriptionTarget,
    };
    use big_sync_core::{BuckId, ByteKey, FingerprintSeed, ObjKey, PartKey};
    use iroh::protocol::Router;
    use keyhive_core::access::Access;
    use std::net::Ipv4Addr;

    /// The responder's registration for one request, as `BigSyncRpcHandle` exposes it.
    ///
    /// A registration exists only while its request is in flight: the responder inserts it at the
    /// top of the request's handler and removes it before the request's reply is built. So `None`
    /// is a request that has already answered, and `Some { cancelled: true }` is one a `supersede`
    /// has asked to release.
    fn registration_for(
        handle: &BigSyncRpcHandle,
        peer: &PeerKey,
        session_id: ReplaySessionId,
        request_id: ReplayRequestId,
    ) -> Option<ReplayRequestRegistration> {
        handle
            .replay_request_registrations()
            .into_iter()
            .find(|registration| {
                &registration.peer == peer
                    && registration.session_id == session_id
                    && registration.request_id == request_id
            })
    }

    /// A reused request id is peer-supplied input, so it must not be able to stop the dispatcher:
    /// the newer request takes the id and the older registration is the one that stops waiting.
    #[test]
    fn a_reused_request_id_replaces_its_registration_instead_of_panicking() {
        let worker = BigSyncRpcWorker {
            stores: HashMap::new(),
            replay_cancels: Default::default(),
            replay_subscriptions: Default::default(),
        };
        let peer = PeerKey::new([7u8; 32]);
        let scope: Arc<str> = Arc::from("scope");
        let older = CancellationToken::new();
        let newer = CancellationToken::new();

        let older_registration = worker.register_replay_request(
            &peer,
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(3),
            older.clone(),
        );
        let newer_registration = worker.register_replay_request(
            &peer,
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(3),
            newer.clone(),
        );

        assert!(
            older.is_cancelled(),
            "the older registration for a reused id is the one that stops waiting"
        );
        assert!(
            !newer.is_cancelled(),
            "the newer request keeps its own cancellation live"
        );

        // The old request can finish after the replacement was installed, but must not erase it.
        worker.forget_replay_request(
            &peer,
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(3),
            &older_registration,
        );
        worker.cancel_replay_request(&peer, &scope, ReplaySessionId(0), ReplayRequestId(3));
        assert!(
            newer.is_cancelled(),
            "the old request cannot erase the newer registration"
        );

        // Forgetting the current registration and touching an unknown id are no-ops afterward.
        worker.forget_replay_request(
            &peer,
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(3),
            &newer_registration,
        );
        worker.forget_replay_request(
            &peer,
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(9),
            &older_registration,
        );
        let other_scope: Arc<str> = Arc::from("other-scope");
        let first_scope_request = CancellationToken::new();
        let second_scope_request = CancellationToken::new();
        worker.register_replay_request(
            &peer,
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(4),
            first_scope_request.clone(),
        );
        worker.register_replay_request(
            &peer,
            &other_scope,
            ReplaySessionId(0),
            ReplayRequestId(4),
            second_scope_request.clone(),
        );
        let other_session_request = CancellationToken::new();
        worker.register_replay_request(
            &peer,
            &scope,
            ReplaySessionId(1),
            ReplayRequestId(4),
            other_session_request.clone(),
        );
        worker.cancel_replay_request(&peer, &scope, ReplaySessionId(0), ReplayRequestId(4));
        assert!(first_scope_request.is_cancelled());
        assert!(!second_scope_request.is_cancelled());
        assert!(!other_session_request.is_cancelled());
    }

    /// A panic taken while the registry was held must not turn one bad request into every later
    /// request failing: the lock is poisoned here on purpose, which is the only way to reach it.
    #[test]
    fn a_poisoned_cancel_registry_still_serves_requests() {
        let worker = Arc::new(BigSyncRpcWorker {
            stores: HashMap::new(),
            replay_cancels: Default::default(),
            replay_subscriptions: Default::default(),
        });
        let poisoner = Arc::clone(&worker);
        let scope: Arc<str> = Arc::from("scope");
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _held = poisoner.replay_cancels();
            panic!("poison the registry");
        }));
        assert!(
            poisoned.is_err(),
            "the test's own panic is what leaves the registry poisoned"
        );

        let _registration = worker.register_replay_request(
            &PeerKey::new([8u8; 32]),
            &scope,
            ReplaySessionId(0),
            ReplayRequestId(1),
            CancellationToken::new(),
        );
    }

    /// The one `Update` call opens a subscription the responder does not know, applies
    /// removals before additions — so one request may remove a target and re-add the same part
    /// under a fresh id (ADR 012 decision 9) — and a removal of an id it does not hold is a
    /// no-op rather than a refusal, because a retried update cannot know what the responder took.
    #[tokio::test]
    async fn replay_subscription_update_opens_and_replaces_targets() -> Res<()> {
        let worker = BigSyncRpcWorker {
            stores: HashMap::new(),
            replay_cancels: Default::default(),
            replay_subscriptions: Default::default(),
        };
        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new());
        seed_test_store(&store, part_id.clone()).await?;
        let peer = PeerKey::new([9u8; 32]);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;
        let store: Arc<dyn HostPartStore> = store;
        let scope: Arc<str> = Arc::from("scope");
        let subscription_id = ReplaySubscriptionId(11);
        let first_id = ReplayTargetId(3);
        let target = ReplaySubscriptionTarget::Part {
            part_id: part_id.clone(),
        };

        // The first update against an unknown id opens the subscription.
        assert_eq!(
            worker
                .update_replay_subscription(
                    store.as_ref(),
                    &peer,
                    &scope,
                    ReplaySubscriptionUpdate {
                        session_id: ReplaySessionId(0),
                        subscription_id,
                        generation: 0,
                        additions: vec![ReplaySubscriptionTargetEntry {
                            id: first_id,
                            target: target.clone(),
                        }],
                        removals: Vec::new(),
                    },
                )
                .await,
            Ok(ReplaySubscriptionResponse::Updated {
                generation: 0,
                rejected: Vec::new(),
            }),
            "the first update opens the subscription"
        );

        // Removing the same part and re-adding it under a fresh id is one request.
        let fresh_id = ReplayTargetId(4);
        assert_eq!(
            worker
                .update_replay_subscription(
                    store.as_ref(),
                    &peer,
                    &scope,
                    ReplaySubscriptionUpdate {
                        session_id: ReplaySessionId(0),
                        subscription_id,
                        generation: 1,
                        additions: vec![ReplaySubscriptionTargetEntry {
                            id: fresh_id,
                            target: target.clone(),
                        }],
                        removals: vec![first_id],
                    },
                )
                .await,
            Ok(ReplaySubscriptionResponse::Updated {
                generation: 1,
                rejected: Vec::new(),
            }),
        );
        let snapshot = worker
            .replay_subscription_snapshot(&peer, &scope, ReplaySessionId(0), subscription_id)
            .expect("subscription remains after target removal");
        assert_eq!(snapshot.generation, 1);
        assert!(!snapshot.targets.contains_key(&first_id));
        assert!(snapshot.targets.contains_key(&fresh_id));

        // An id the subscription does not hold is a no-op to remove: the client re-sends its
        // batch until an answer acknowledges it, and cannot know which ids the responder took.
        assert_eq!(
            worker
                .update_replay_subscription(
                    store.as_ref(),
                    &peer,
                    &scope,
                    ReplaySubscriptionUpdate {
                        session_id: ReplaySessionId(0),
                        subscription_id,
                        generation: 2,
                        additions: Vec::new(),
                        removals: vec![first_id, ReplayTargetId(99)],
                    },
                )
                .await,
            Ok(ReplaySubscriptionResponse::Updated {
                generation: 2,
                rejected: Vec::new(),
            }),
            "an unknown removal id is not a refusal"
        );

        // Another session's subscription under the same id is its own.
        assert_eq!(
            worker
                .update_replay_subscription(
                    store.as_ref(),
                    &peer,
                    &scope,
                    ReplaySubscriptionUpdate {
                        session_id: ReplaySessionId(1),
                        subscription_id,
                        generation: 0,
                        additions: vec![ReplaySubscriptionTargetEntry {
                            id: first_id,
                            target: target.clone(),
                        }],
                        removals: Vec::new(),
                    },
                )
                .await,
            Ok(ReplaySubscriptionResponse::Updated {
                generation: 0,
                rejected: Vec::new(),
            }),
        );
        let other_session = worker
            .replay_subscription_snapshot(&peer, &scope, ReplaySessionId(1), subscription_id)
            .expect("second session is independent");
        assert!(other_session.targets.contains_key(&first_id));
        let snapshot = worker
            .replay_subscription_snapshot(&peer, &scope, ReplaySessionId(0), subscription_id)
            .expect("changing one session does not remove the other");
        assert!(snapshot.targets.contains_key(&fresh_id));
        Ok(())
    }

    /// An update reports partial success: a target the scope does not have, or one the asker
    /// may not read, comes back as a refused entry and is not inserted, while the entries that
    /// are acceptable land in the same request (ADR 012 decision 9).
    #[tokio::test]
    async fn replay_subscription_update_refuses_unservable_additions() -> Res<()> {
        let worker = BigSyncRpcWorker {
            stores: HashMap::new(),
            replay_cancels: Default::default(),
            replay_subscriptions: Default::default(),
        };
        let part_id = test_part();
        let hidden_part = PartKey::new(b"a-part-this-asker-may-not-read");
        let absent_part = PartKey::new(b"a-part-this-scope-does-not-have");
        let store = Arc::new(MemoryPartStore::new());
        seed_test_store(&store, part_id.clone()).await?;
        store.ensure_part(hidden_part.clone()).await?;
        let peer = PeerKey::new([9u8; 32]);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;
        let store: Arc<dyn HostPartStore> = store;
        let scope: Arc<str> = Arc::from("scope");
        let subscription_id = ReplaySubscriptionId(11);
        let readable = ReplayTargetId(1);
        let unreadable = ReplayTargetId(2);
        let unknown = ReplayTargetId(3);

        let response = worker
            .update_replay_subscription(
                store.as_ref(),
                &peer,
                &scope,
                ReplaySubscriptionUpdate {
                    session_id: ReplaySessionId(0),
                    subscription_id,
                    generation: 0,
                    additions: vec![
                        ReplaySubscriptionTargetEntry {
                            id: readable,
                            target: ReplaySubscriptionTarget::Part {
                                part_id: part_id.clone(),
                            },
                        },
                        ReplaySubscriptionTargetEntry {
                            id: unreadable,
                            target: ReplaySubscriptionTarget::Part {
                                part_id: hidden_part,
                            },
                        },
                        ReplaySubscriptionTargetEntry {
                            id: unknown,
                            target: ReplaySubscriptionTarget::Part {
                                part_id: absent_part,
                            },
                        },
                    ],
                    removals: Vec::new(),
                },
            )
            .await
            .expect("a refused entry is a partial success, not a request error");
        let ReplaySubscriptionResponse::Updated {
            generation,
            rejected,
        } = response
        else {
            panic!("an update is answered with an update answer");
        };
        assert_eq!(generation, 0);
        assert_eq!(
            rejected,
            vec![
                (unreadable, TargetVerdict::Unauthorized),
                (unknown, TargetVerdict::UnknownPart),
            ],
            "the refused entries come back with their reason"
        );
        let snapshot = worker
            .replay_subscription_snapshot(&peer, &scope, ReplaySessionId(0), subscription_id)
            .expect("opened subscription");
        assert!(snapshot.targets.contains_key(&readable));
        assert!(
            !snapshot.targets.contains_key(&unreadable) && !snapshot.targets.contains_key(&unknown),
            "a refused entry is not inserted"
        );

        // A repeat of the applied generation is not applied again: the answer names that
        // generation and the verdicts it was given. A resend whose first answer was lost therefore
        // keeps its refusals, and is never told that a refused entry landed.
        let repeat = worker
            .update_replay_subscription(
                store.as_ref(),
                &peer,
                &scope,
                ReplaySubscriptionUpdate {
                    session_id: ReplaySessionId(0),
                    subscription_id,
                    generation: 0,
                    additions: vec![ReplaySubscriptionTargetEntry {
                        id: unreadable,
                        target: ReplaySubscriptionTarget::Part {
                            part_id: PartKey::new(b"a-part-this-asker-may-not-read"),
                        },
                    }],
                    removals: Vec::new(),
                },
            )
            .await
            .expect("a repeated generation is answered rather than refused");
        let ReplaySubscriptionResponse::Updated { rejected, .. } = repeat else {
            panic!("an update is answered with an update answer");
        };
        assert_eq!(
            rejected,
            vec![
                (unreadable, TargetVerdict::Unauthorized),
                (unknown, TargetVerdict::UnknownPart),
            ],
            "the answer to a repeat is the applied generation's own verdicts"
        );
        Ok(())
    }

    /// An update that is no newer than the generation the responder holds is not applied, and
    /// the answer names the generation that was held: the client sends past it rather than
    /// clobbering a newer target set with an older one.
    #[tokio::test]
    async fn replay_subscription_update_does_not_apply_an_older_generation() -> Res<()> {
        let worker = BigSyncRpcWorker {
            stores: HashMap::new(),
            replay_cancels: Default::default(),
            replay_subscriptions: Default::default(),
        };
        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new());
        seed_test_store(&store, part_id.clone()).await?;
        let peer = PeerKey::new([9u8; 32]);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;
        let store: Arc<dyn HostPartStore> = store;
        let scope: Arc<str> = Arc::from("scope");
        let subscription_id = ReplaySubscriptionId(7);
        let entry = |id| ReplaySubscriptionTargetEntry {
            id,
            target: ReplaySubscriptionTarget::Part {
                part_id: part_id.clone(),
            },
        };
        for generation in [0, 5] {
            worker
                .update_replay_subscription(
                    store.as_ref(),
                    &peer,
                    &scope,
                    ReplaySubscriptionUpdate {
                        session_id: ReplaySessionId(0),
                        subscription_id,
                        generation,
                        additions: vec![entry(ReplayTargetId(generation as u32))],
                        removals: Vec::new(),
                    },
                )
                .await?;
        }
        let response = worker
            .update_replay_subscription(
                store.as_ref(),
                &peer,
                &scope,
                ReplaySubscriptionUpdate {
                    session_id: ReplaySessionId(0),
                    subscription_id,
                    generation: 3,
                    additions: vec![entry(ReplayTargetId(3))],
                    removals: Vec::new(),
                },
            )
            .await?;
        assert_eq!(
            response,
            ReplaySubscriptionResponse::Updated {
                generation: 5,
                rejected: Vec::new(),
            },
            "an older generation is not applied, and the answer says which one was held"
        );
        let snapshot = worker
            .replay_subscription_snapshot(&peer, &scope, ReplaySessionId(0), subscription_id)
            .expect("opened subscription");
        assert!(
            !snapshot.targets.contains_key(&ReplayTargetId(3)),
            "the overtaken update's entries are not in the target set"
        );
        Ok(())
    }

    /// A responder that has forgotten the handle answers `UnknownSubscription` to an update
    /// that is not a fresh open, which is how a swept or restarted responder makes the client
    /// re-state its whole target set instead of opening with a delta (ADR 012 decision 9).
    #[tokio::test]
    async fn replay_subscription_update_of_an_unknown_handle_is_refused() -> Res<()> {
        let worker = BigSyncRpcWorker {
            stores: HashMap::new(),
            replay_cancels: Default::default(),
            replay_subscriptions: Default::default(),
        };
        let store = Arc::new(MemoryPartStore::new());
        let store: Arc<dyn HostPartStore> = store;
        let peer = PeerKey::new([9u8; 32]);
        let scope: Arc<str> = Arc::from("scope");
        assert_eq!(
            worker
                .update_replay_subscription(
                    store.as_ref(),
                    &peer,
                    &scope,
                    ReplaySubscriptionUpdate {
                        session_id: ReplaySessionId(0),
                        subscription_id: ReplaySubscriptionId(4),
                        generation: 9,
                        additions: Vec::new(),
                        removals: Vec::new(),
                    },
                )
                .await,
            Err(RpcError::UnknownSubscription),
        );
        Ok(())
    }

    #[test]
    fn an_untrusted_page_limit_is_capped() {
        // A zero limit is a zero-event page and must pass through the cap
        // untouched: the cap is a ceiling, not a floor.
        assert_eq!(page_limit(0), 0);
        assert_eq!(page_limit(1), 1);
        assert_eq!(page_limit(MAX_PAGE_LIMIT), MAX_PAGE_LIMIT);
        assert_eq!(
            page_limit(MAX_PAGE_LIMIT.saturating_add(1)),
            MAX_PAGE_LIMIT,
            "one above the cap asks for the cap; it is not an error"
        );
        assert_eq!(page_limit(u32::MAX), MAX_PAGE_LIMIT);
        // The cap may only lower a request, never raise one: a `max` here would
        // turn a caller's own page size into an amplification.
        for requested in [0, 1, 2, MAX_PAGE_LIMIT - 1, MAX_PAGE_LIMIT, u32::MAX] {
            assert!(
                page_limit(requested) <= requested,
                "capping {requested} must not raise it"
            );
        }
    }

    #[test]
    fn an_untrusted_bucket_limit_is_capped() {
        // As for a page: the cap is a ceiling, not a floor, so a hint of zero stays
        // "no preference" and is decided by the store.
        assert_eq!(bucket_limit(0), 0);
        assert_eq!(bucket_limit(1), 1);
        assert_eq!(bucket_limit(MAX_BUCKET_LIMIT), MAX_BUCKET_LIMIT);
        assert_eq!(
            bucket_limit(MAX_BUCKET_LIMIT.saturating_add(1)),
            MAX_BUCKET_LIMIT,
            "one above the cap asks for the cap; it is not an error"
        );
        assert_eq!(bucket_limit(u32::MAX), MAX_BUCKET_LIMIT);
        for requested in [0, 1, 2, MAX_BUCKET_LIMIT - 1, MAX_BUCKET_LIMIT, u32::MAX] {
            assert!(
                bucket_limit(requested) <= requested,
                "capping {requested} must not raise it"
            );
        }
        // A well-behaved caller asks for less than the cap on both endpoints —
        // `GET_BUCKET_LIMIT_HINT` (8 buckets per `BuckId::ARITY` siblings) and
        // `LEAF_BUCKET_LIMIT_HINT` — so no correct caller is ever refused.
        let asking_side_changed_hint = 8 * u32::from(BuckId::ARITY);
        assert_eq!(
            bucket_limit(asking_side_changed_hint),
            asking_side_changed_hint
        );
        assert_eq!(bucket_limit(1024), 1024);
    }

    fn test_part() -> PartKey {
        PartKey(ByteKey::new([
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
        ]))
    }

    fn test_obj(byte: u8) -> ObjKey {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        ObjKey(ByteKey::new(bytes))
    }

    async fn seed_test_store(store: &MemoryPartStore, part_id: PartKey) -> Res<()> {
        store.ensure_part(part_id.clone()).await?;

        let live_obj = test_obj(1);
        let dead_obj = test_obj(2);
        let payload_live = serde_json::json!({"kind":"live","value":1});
        let payload_dead = serde_json::json!({"kind":"dead","value":2});

        store
            .set_obj_payload(live_obj.clone(), payload_live.clone())
            .await?;
        store
            .add_obj_to_parts(live_obj, vec![part_id.clone()])
            .await?;
        store
            .set_obj_payload(dead_obj.clone(), payload_dead.clone())
            .await?;
        store
            .add_obj_to_parts(dead_obj.clone(), vec![part_id.clone()])
            .await?;
        store.remove_obj_from_part(dead_obj, part_id).await?;
        Ok(())
    }

    /// A connected client endpoint for this test, bound locally.
    async fn test_endpoint() -> Res<iroh::Endpoint> {
        Ok(iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind_addr((Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?)
    }

    /// Which client one caller of the peer-facing read arms uses.
    ///
    /// A connected peer is authenticated by its connection. The unauthenticated caller
    /// is `spawn_big_sync_rpc`'s local channel, which stamps no peer key at all.
    #[derive(Clone, Copy)]
    enum Caller<'a> {
        Peer(&'a BigSyncRpcClient),
        Unauthenticated(&'a irpc::Client<BigSyncIrpc>),
    }

    /// What one caller is entitled to on every arm.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Entitlement {
        /// The asker holds Read on the part.
        Permitted,
        /// The asker holds nothing, or the surface did not authenticate it at all.
        Refused,
    }

    async fn summary_answer(
        caller: Caller<'_>,
        req: ScopedRequest<PeerSummaryRequest>,
    ) -> Res<Result<PeerSummaryResult, PeerSummaryError>> {
        Ok(match caller {
            Caller::Peer(client) => client.peer_summary(req).await??,
            Caller::Unauthenticated(client) => client.rpc(req).await?,
        })
    }

    async fn changed_buckets_answer(
        caller: Caller<'_>,
        req: ScopedRequest<GetChangedBucketsRequest>,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        Ok(match caller {
            Caller::Peer(client) => client.get_changed_buckets(req).await??,
            Caller::Unauthenticated(client) => client.rpc(req).await?,
        })
    }

    async fn leaf_buckets_answer(
        caller: Caller<'_>,
        req: ScopedRequest<LeafBucketsRequest>,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        Ok(match caller {
            Caller::Peer(client) => client.leaf_buckets(req).await??,
            Caller::Unauthenticated(client) => client.rpc(req).await?,
        })
    }

    async fn replay_page_answer(
        caller: Caller<'_>,
        req: ScopedRequest<ReplayPageRequest>,
    ) -> Res<ReplayPage> {
        Ok(match caller {
            Caller::Peer(client) => client.replay_page(req).await??,
            Caller::Unauthenticated(client) => client.rpc(req).await??,
        })
    }

    /// A permitted asker gets the part's own summary; a refused one gets an
    /// empty answer map (the part's absence is the refusal) — the per-part
    /// answer that keeps the part's neighbors' summaries on the wire — and
    /// denied stays indistinguishable from unknown.
    async fn assert_peer_summary_arm(
        caller: Caller<'_>,
        caller_label: &str,
        entitlement: Entitlement,
        req: ScopedRequest<PeerSummaryRequest>,
        expected: &PeerSummaryResult,
    ) -> Res<()> {
        let answer = summary_answer(caller, req).await?.expect(
            "these arms name a legal part count, so a refusal here is per part, not TooManyParts",
        );
        match (entitlement, answer) {
            (Entitlement::Permitted, answer) => assert_eq!(
                &answer, expected,
                "{caller_label}: a permitted asker gets the store's own summary"
            ),
            (Entitlement::Refused, answer) => {
                // Refusal is an empty answer map: a part the response does not
                // name is refused by its absence.
                assert!(
                    answer.parts.is_empty(),
                    "{caller_label}: a refusal answers no part summaries"
                );
            }
        }
        Ok(())
    }

    /// A permitted asker gets the store's own bucket page; a refused one gets the part
    /// named as unknown rather than an empty page.
    async fn assert_changed_buckets_arm(
        caller: Caller<'_>,
        caller_label: &str,
        entitlement: Entitlement,
        part_id: &PartKey,
        req: ScopedRequest<GetChangedBucketsRequest>,
        expected: &[BucketSummary],
    ) -> Res<()> {
        let answer = changed_buckets_answer(caller, req).await?;
        match (entitlement, answer) {
            (Entitlement::Permitted, Ok(answer)) => {
                assert_eq!(
                    answer, expected,
                    "{caller_label}: a permitted asker gets the store's own bucket walk"
                );
                assert!(
                    !answer.is_empty(),
                    "{caller_label}: the permitted walk is not an empty page"
                );
            }
            (Entitlement::Refused, Err(ListPartsError::UnkownParts { unkown_parts })) => {
                assert_eq!(
                    unkown_parts,
                    vec![part_id.clone()],
                    "{caller_label}: the refusal names the part it will not answer for"
                );
            }
            (entitlement, answer) => {
                panic!("{caller_label}: {entitlement:?} get_changed_buckets answered {answer:?}")
            }
        }
        Ok(())
    }

    /// As above for the leaf walk, whose refusal is its own variant.
    async fn assert_leaf_buckets_arm(
        caller: Caller<'_>,
        caller_label: &str,
        entitlement: Entitlement,
        req: ScopedRequest<LeafBucketsRequest>,
        expected: &LeafBucketResult,
    ) -> Res<()> {
        let answer = leaf_buckets_answer(caller, req).await?;
        match (entitlement, answer) {
            (Entitlement::Permitted, Ok(answer)) => {
                assert_eq!(
                    answer, *expected,
                    "{caller_label}: a permitted asker gets the store's own leaf page"
                );
                let page = answer
                    .bucks
                    .get(&BuckId::ROOT)
                    .expect("the requested bucket has a page");
                assert!(
                    !page.entries.is_empty(),
                    "{caller_label}: the permitted page carries the bucket's entries"
                );
            }
            (Entitlement::Refused, Err(LeafBucketsError::UnkownPart)) => {}
            (entitlement, answer) => {
                panic!("{caller_label}: {entitlement:?} leaf_buckets answered {answer:?}")
            }
        }
        Ok(())
    }

    /// The page arm refuses with `Unauthorized`, which is not an empty page: an empty
    /// page is the caught-up answer, and a caller must not have to infer a denial from
    /// it.
    ///
    /// A permitted page is re-issued while it comes back empty, because the
    /// subscription a page drains is produced by a spawned task: a first page can be
    /// answered before that task has delivered anything, and re-issuing from the
    /// returned cursor is how a caller is meant to resume.
    async fn assert_replay_page_arm(
        caller: Caller<'_>,
        caller_label: &str,
        entitlement: Entitlement,
        req: ScopedRequest<ReplayPageRequest>,
        expected_events: &[PartEvent],
    ) -> Res<()> {
        const ATTEMPTS: u8 = 8;
        let mut outcome = replay_page_answer(caller, req.clone()).await?;
        match entitlement {
            // The verdict is per target, so a refusal is the answer for the target this
            // round named, not a shape of the whole page.
            Entitlement::Refused => match outcome.verdict(&req.inner.targets[0]) {
                Some(TargetVerdict::Unauthorized) => Ok(()),
                other => panic!("{caller_label}: a refused page answered {other:?}"),
            },
            Entitlement::Permitted => {
                for _ in 0..ATTEMPTS {
                    if outcome.events == expected_events {
                        return Ok(());
                    }
                    outcome = replay_page_answer(caller, req.clone()).await?;
                }
                panic!("{caller_label}: no page carried the granted events");
            }
        }
    }

    /// A request naming more parts than the responder will answer is refused whole, by
    /// name.
    ///
    /// An empty answer map would be indistinguishable from an idle peer: the asker would
    /// mark every part unknown and re-ask, which is the starvation the per-part answer
    /// exists to prevent, only silent. The ceiling itself still answers - what is refused
    /// is the request that exceeds it.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_over_ceiling_summary_request_is_refused_by_name() -> Res<()> {
        let store: Arc<dyn HostPartStore> = Arc::new(MemoryPartStore::new());
        let (rpc_handle, _rpc_stop) =
            spawn_big_sync_rpc(HashMap::from([(Arc::from("test-scope"), store)])).await?;

        // The refusal happens before any part is looked up, so none of these parts has to
        // exist in the store. The ceiling is 64, so one byte indexes them.
        let part_at = |i: usize| {
            let mut bytes = [0u8; 32];
            bytes[0] = i as u8;
            PartKey(ByteKey::new(bytes))
        };
        let request_naming = |count: usize| ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: PeerSummaryRequest {
                parts: (0..count).map(part_at).collect(),
                asker_part_cursors: HashMap::new(),
            },
        };

        let at_ceiling = rpc_handle
            .client
            .rpc(request_naming(MAX_SUMMARY_PARTS))
            .await?;
        assert!(
            at_ceiling.is_ok(),
            "a request at the ceiling is answered, not refused whole: {at_ceiling:?}"
        );

        let over_ceiling = rpc_handle
            .client
            .rpc(request_naming(MAX_SUMMARY_PARTS + 1))
            .await?;
        assert_eq!(
            over_ceiling,
            Err(PeerSummaryError::TooManyParts {
                requested: MAX_SUMMARY_PARTS + 1,
                cap: MAX_SUMMARY_PARTS,
            }),
            "an over-ceiling request names the count that tripped the ceiling"
        );
        Ok(())
    }

    /// The peer-facing read arms are authorized in one place, and this table is what
    /// keeps them unified: every arm answers a permitted peer with its own read, and
    /// answers an unpermitted peer and an unauthenticated caller with its refusal.
    /// A refusal is never an empty or partial answer, because an omission still
    /// confirms that the part exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_read_arm_refuses_what_the_asker_may_not_read() -> Res<()> {
        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new());
        seed_test_store(&store, part_id.clone()).await?;

        let rpc_store: Arc<dyn HostPartStore> = Arc::<MemoryPartStore>::clone(&store) as _;
        let (rpc_handle, rpc_stop) =
            spawn_big_sync_rpc(HashMap::from([(Arc::from("test-scope"), rpc_store)])).await?;

        let server_endpoint = test_endpoint().await?;
        let router = Router::builder(server_endpoint.clone())
            .accept(BIG_SYNC_RPC_ALPN, rpc_handle.protocol_handler())
            .spawn();
        let server_addr = router.endpoint().addr();
        assert!(
            !server_addr.addrs.is_empty(),
            "server endpoint address should expose at least one transport address"
        );

        // Two connected peers: the granted one is the permitted asker, the other is
        // authenticated and holds nothing. The responder learns a peer's key from its
        // connection, so the granted key is read off that peer's own endpoint.
        let granted_endpoint = test_endpoint().await?;
        let granted_peer = PeerKey::new(*granted_endpoint.id().as_bytes());
        let granted = BigSyncRpcClient::over_iroh(granted_endpoint, server_addr.clone());
        let refused_endpoint = test_endpoint().await?;
        let refused_peer = PeerKey::new(*refused_endpoint.id().as_bytes());
        let refused = BigSyncRpcClient::over_iroh(refused_endpoint, server_addr.clone());
        assert_ne!(granted_peer, refused_peer, "two endpoints, two identities");
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(granted_peer.clone(), Access::Read)]),
            )
            .await?;

        let summary_request = || ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: PeerSummaryRequest {
                parts: [part_id.clone()].into_iter().collect(),
                asker_part_cursors: HashMap::from([(part_id.clone(), 0)]),
            },
        };
        let changed_request = || ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: GetChangedBucketsRequest {
                part_id: part_id.clone(),
                offset: BuckId::ROOT,
                to_level: BuckId::MAX_LEVEL,
                since: 0,
                limit_hint: 16,
            },
        };
        let leaf_request = || ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: LeafBucketsRequest {
                part_id: part_id.clone(),
                since: 0,
                buckets: vec![LeafBucketRequest {
                    buck_id: BuckId::ROOT,
                    after: None,
                }],
                seed: FingerprintSeed::new(0xaaaa_bbbb, 0xcccc_dddd),
                limit_hint: 16,
            },
        };
        let page_target = SubscriptionTarget::Part {
            part_id: part_id.clone(),
            cursor: 0,
        };
        let page_request = || ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: ReplayPageRequest {
                session_id: ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(0),
                supersede: None,
                targets: vec![page_target.clone()],
                limit: 16,
                hold_ms: 50,
            },
        };

        // What the permitted asker is entitled to, taken from the store itself: what
        // the arms are checked against is the store's own answer, not a second
        // implementation of it in the test.
        let expected_summary = {
            let mut summaries = HashMap::new();
            for (part_id, summary) in store
                .summarize_parts([part_id.clone()].into_iter().collect())
                .await?
                .expect("a granted part summarizes")
            {
                let dirty = store
                    .part_dirty_count(part_id.clone(), Some(granted_peer.clone()), 0)
                    .await?;
                summaries.insert(part_id, summary.into_strat_summaries(dirty));
            }
            // A granted part is never refused, and the store contract guarantees
            // coverage of every granted part asked.
            PeerSummaryResult { parts: summaries }
        };
        let expected_changed = store
            .get_changed_buckets(changed_request().inner, granted_peer.clone())
            .await?
            .expect("a granted part answers a bucket walk");
        assert!(
            !expected_changed.is_empty(),
            "the seeded part has changed buckets to compare against"
        );
        let expected_leaf = store
            .leaf_buckets(leaf_request().inner, granted_peer.clone())
            .await?
            .expect("a granted part answers a leaf walk");
        let expected_page = store
            .replay_page_round(
                ReplayPageRequest {
                    session_id: ReplaySessionId(0),
                    request_id: big_sync_core::rpc::ReplayRequestId(0),
                    supersede: None,
                    targets: vec![page_target.clone()],
                    limit: 16,
                    hold_ms: 250,
                },
                granted_peer.clone(),
                Duration::from_millis(250),
                CancellationToken::new(),
            )
            .await?;
        assert!(
            !expected_page.events.is_empty(),
            "the granted page carries the seeded events"
        );
        assert!(
            matches!(
                expected_page.verdict(&page_target),
                Some(TargetVerdict::Events { .. })
            ),
            "the granted page carries an events verdict"
        );

        // The table: one row per caller, and every arm is asserted for that caller.
        // A new arm belongs in every row, which is what makes "unified" a property of
        // this test rather than of a review.
        let callers = [
            (
                "granted peer",
                Caller::Peer(&granted),
                Entitlement::Permitted,
            ),
            (
                "authenticated peer without access",
                Caller::Peer(&refused),
                Entitlement::Refused,
            ),
            (
                "unauthenticated caller",
                Caller::Unauthenticated(&rpc_handle.client),
                Entitlement::Refused,
            ),
        ];
        for (caller_label, caller, entitlement) in callers {
            assert_peer_summary_arm(
                caller,
                caller_label,
                entitlement,
                summary_request(),
                &expected_summary,
            )
            .await?;
            assert_changed_buckets_arm(
                caller,
                caller_label,
                entitlement,
                &part_id,
                changed_request(),
                &expected_changed,
            )
            .await?;
            assert_leaf_buckets_arm(
                caller,
                caller_label,
                entitlement,
                leaf_request(),
                &expected_leaf,
            )
            .await?;
            assert_replay_page_arm(
                caller,
                caller_label,
                entitlement,
                page_request(),
                &expected_page.events,
            )
            .await?;
        }

        rpc_stop.stop().await?;
        router.shutdown().await?;
        server_endpoint.close().await;
        Ok(())
    }

    /// A held page must not wedge the dispatch loop. The handlers are spawned for
    /// exactly this reason (see `MAX_INFLIGHT_RPC_HANDLERS`), so a request parked on
    /// its hold cannot serialize the peers behind it. A client that gives up mid-hold
    /// leaves the responder holding; the next request must still be answered instead
    /// of waiting out the cancelled hold.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_held_page_request_does_not_wedge_the_dispatch_loop() -> Res<()> {
        use keyhive_core::access::Access;

        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new());
        // The part exists and is granted, but holds nothing: a page for it can only
        // leave on its hold.
        store.ensure_part(part_id.clone()).await?;

        let rpc_store: Arc<dyn HostPartStore> = Arc::<MemoryPartStore>::clone(&store) as _;
        let (rpc_handle, rpc_stop) =
            spawn_big_sync_rpc(HashMap::from([(Arc::from("test-scope"), rpc_store)])).await?;

        let server_endpoint = test_endpoint().await?;
        let router = Router::builder(server_endpoint.clone())
            .accept(BIG_SYNC_RPC_ALPN, rpc_handle.protocol_handler())
            .spawn();
        let server_addr = router.endpoint().addr();

        let granted_endpoint = test_endpoint().await?;
        let granted_peer = PeerKey::new(*granted_endpoint.id().as_bytes());
        let granted = BigSyncRpcClient::over_iroh(granted_endpoint, server_addr);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(granted_peer.clone(), Access::Read)]),
            )
            .await?;

        let target = SubscriptionTarget::Part {
            part_id: part_id.clone(),
            cursor: 0,
        };
        let page_request = |request_id: u64, supersede: Option<u64>, hold_ms: u32| ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: ReplayPageRequest {
                session_id: ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(request_id),
                supersede: supersede.map(big_sync_core::rpc::ReplayRequestId),
                targets: vec![target.clone()],
                limit: 16,
                hold_ms,
            },
        };

        // Parked, not answered: nothing is waiting on the part, so the responder holds
        // the request until its hold elapses. The request runs as its own task rather than
        // as a future this test polls by hand: `irpc-iroh` holds the client's shared
        // connection lock for the whole of `open_bi`, so a request future left unpolled
        // while it is inside that call keeps the lock, and the next request then waits on
        // the lock instead of on the responder — the test would be measuring the client's
        // dial rather than the dispatch loop. The join handle keeps the release below
        // observable rather than taken on trust.
        // The hold is `MAX_PAGE_HOLD`, the most a caller can ask for: nothing below can release
        // the parked request by its own hold expiring, so every release this test observes is one
        // it caused. The same bound is why asserting that the request is still parked cannot hang
        // the suite — its own task answers within 15s whatever the test does.
        let park_hold_ms = u32::try_from(MAX_PAGE_HOLD.as_millis())
            .expect("MAX_PAGE_HOLD is representable in the wire's hold_ms field");
        let parked_request = page_request(0, None, park_hold_ms);
        let parked_client = granted.clone();
        let held = tokio::spawn(async move { parked_client.replay_page(parked_request).await });

        // The held request is still parked on the responder. A fresh page is answered
        // anyway: a loop that handled pages inline would have to finish the parked page
        // first, so this page could not be answered until the parked one's hold elapsed.
        // That is asserted as the state it leaves behind — the parked request is still
        // parked — rather than as the round trip's duration, which would fail on a loaded
        // machine and pass on an inline loop that happened to answer quickly. A page that
        // answered early would have finished the join handle by now, and nothing has asked
        // the responder to release this one yet. The fresh page deliberately does not name
        // the parked request, so nothing but the next block's supersede can release it.
        let fresh = granted.replay_page(page_request(1, None, 50)).await??;
        assert!(
            !held.is_finished(),
            "a parked page must not serialize the next request behind its hold"
        );
        assert!(
            !registration_for(
                &rpc_handle,
                &granted_peer,
                ReplaySessionId(0),
                ReplayRequestId(0)
            )
            .is_some_and(|registration| registration.cancelled),
            "nothing has asked the parked request to release yet"
        );
        assert!(
            fresh.events.is_empty(),
            "the part still has nothing to send"
        );
        assert!(
            matches!(fresh.verdict(&target), Some(TargetVerdict::Events { .. })),
            "the fresh page carries an events verdict"
        );

        // The supersede has to reach the parked request's own task, which answers instead
        // of waiting out the rest of its hold. A responder registers a request's
        // cancellation at the top of its handler, so a successor that arrives before that
        // point names an id the responder does not hold yet and is a documented no-op —
        // hence the retry with a fresh id.
        //
        // The retry is driven by the responder's own registration and exits on it rather than on
        // elapsed time: an entry that is present is a request that has not answered yet, an entry
        // that was observed and is now gone is a request that answered, and a cancelled entry is
        // one a supersede released. The cancelled bit is only observable while the request is
        // still in flight, because answering retires the registration, so the loop accepts either
        // form. The previous form waited a fixed interval and asserted the elapsed time, which
        // fails on a loaded machine and passes on a supersede that never landed.
        const SUPERSEDE_ATTEMPTS: u32 = 64;
        let mut successor = 2u64;
        let mut attempts = 0;
        let mut observed_registration = false;
        loop {
            match registration_for(
                &rpc_handle,
                &granted_peer,
                ReplaySessionId(0),
                ReplayRequestId(0),
            ) {
                Some(registration) => {
                    observed_registration = true;
                    if registration.cancelled {
                        break;
                    }
                }
                // Registered above and gone now, so the parked request answered. Nothing else
                // removes a registration: its hold is `MAX_PAGE_HOLD`, it has not been asked to
                // release by anything but this test's carriers, and this loop exits as soon as one
                // of them lands rather than letting the hold run out. A hold-driven release would
                // need 15s of carrier round trips, and the attempt bound below runs out first
                // unless every one of them is unexpectedly slow.
                None if observed_registration => break,
                // Not registered yet: the responder spawns a request's handler independently of
                // this test, so the first carriers may arrive before the registration exists and
                // are the documented no-op.
                None => {}
            }
            assert!(
                attempts < SUPERSEDE_ATTEMPTS,
                "the parked request never registered, so no supersede could reach it"
            );
            attempts += 1;
            let carrier = granted
                .replay_page(page_request(successor, Some(0), 50))
                .await??;
            assert!(
                carrier.events.is_empty(),
                "the part still has nothing to send"
            );
            successor += 1;
        }
        let released = held
            .await
            .expect("the parked request task does not panic")??;
        assert!(
            released.events.is_empty(),
            "the released page carries nothing to send"
        );
        assert!(
            matches!(
                released.verdict(&target),
                Some(TargetVerdict::Events { .. })
            ),
            "the released page carries an events verdict"
        );

        // The registry is still usable after the release: a drain-only request answers.
        let self_superseded = granted
            .replay_page(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: ReplayPageRequest {
                    session_id: ReplaySessionId(0),
                    request_id: big_sync_core::rpc::ReplayRequestId(2),
                    supersede: Some(big_sync_core::rpc::ReplayRequestId(2)),
                    targets: vec![SubscriptionTarget::Part {
                        part_id: part_id.clone(),
                        cursor: 0,
                    }],
                    limit: 16,
                    hold_ms: 0,
                },
            })
            .await??;
        assert!(self_superseded.events.is_empty());
        assert!(matches!(
            self_superseded.verdict(&SubscriptionTarget::Part {
                part_id: part_id.clone(),
                cursor: 0,
            }),
            Some(TargetVerdict::Events {
                resume: 0,
                drained: false
            })
        ));

        let after = granted.replay_page(page_request(3, None, 0)).await??;
        assert!(
            after.events.is_empty(),
            "the dispatch loop still serves a request after a supersede"
        );
        assert!(
            matches!(after.verdict(&target), Some(TargetVerdict::Events { .. })),
            "the dispatch loop still serves a target verdict after a supersede"
        );

        rpc_stop.stop().await?;
        router.shutdown().await?;
        server_endpoint.close().await;
        Ok(())
    }

    /// A page carries its verdict explicitly, so the wire can tell "nothing further is
    /// waiting" from "nothing was drained": a quiet part whose replay half reported
    /// completion is `drained: true`, while a page that cannot drain anything (a zero
    /// limit) reports `drained: false` with the caller's own cursor. Both sides are
    /// pinned, plus that the event landing after a quiet page is still fetchable from
    /// the cursor the caller already holds, that a page stopping on its limit leaves
    /// backlog, and that a drain-only request never waits.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_page_that_carried_nothing_is_not_a_verdict_about_the_log() -> Res<()> {
        use keyhive_core::access::Access;

        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new());
        store.ensure_part(part_id.clone()).await?;

        let rpc_store: Arc<dyn HostPartStore> = Arc::<MemoryPartStore>::clone(&store) as _;
        let (rpc_handle, rpc_stop) =
            spawn_big_sync_rpc(HashMap::from([(Arc::from("test-scope"), rpc_store)])).await?;

        let server_endpoint = test_endpoint().await?;
        let router = Router::builder(server_endpoint.clone())
            .accept(BIG_SYNC_RPC_ALPN, rpc_handle.protocol_handler())
            .spawn();
        let server_addr = router.endpoint().addr();

        let granted_endpoint = test_endpoint().await?;
        let granted_peer = PeerKey::new(*granted_endpoint.id().as_bytes());
        let granted = BigSyncRpcClient::over_iroh(granted_endpoint, server_addr);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(granted_peer.clone(), Access::Read)]),
            )
            .await?;

        let target = SubscriptionTarget::Part {
            part_id: part_id.clone(),
            cursor: 0,
        };
        let page_request = |request_id: u64, hold_ms: u32, limit: u32| ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: ReplayPageRequest {
                session_id: ReplaySessionId(0),
                request_id: big_sync_core::rpc::ReplayRequestId(request_id),
                supersede: None,
                targets: vec![target.clone()],
                limit,
                hold_ms,
            },
        };
        let invalid = granted
            .replay_page(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: ReplayPageRequest {
                    session_id: ReplaySessionId(0),
                    request_id: big_sync_core::rpc::ReplayRequestId(9),
                    supersede: None,
                    targets: vec![
                        target.clone(),
                        SubscriptionTarget::Part {
                            part_id: part_id.clone(),
                            cursor: 1,
                        },
                    ],
                    limit: 16,
                    hold_ms: 0,
                },
            })
            .await?
            .expect_err("duplicate logical routes are invalid input");
        assert!(matches!(
            invalid,
            RpcError::InvalidRequest(reason) if reason.contains("logical route")
        ));

        // A page that carries nothing without the replay half reporting completion is not
        // a verdict: it answers `drained: false` and carries the caller's own cursor. A
        // zero limit reaches that branch without draining anything, which is the same
        // answer a page that runs out of its hold gives.
        let undrained = granted.replay_page(page_request(1, 50, 0)).await??;
        assert!(
            undrained.events.is_empty(),
            "a page that carries nothing has no events"
        );
        let Some(TargetVerdict::Events { resume, drained }) = undrained.verdict(&target) else {
            panic!("a bounded page must carry an events verdict");
        };
        assert!(
            !drained,
            "a page that carried nothing is not a caught-up verdict"
        );
        assert_eq!(*resume, 0, "and it resumes from the caller's own cursor");

        // Nothing has joined the part, so this page can only leave on its hold — and
        // this time the replay half has reported completion, which *is* the caught-up
        // verdict.
        let quiet = granted.replay_page(page_request(2, 50, 16)).await??;
        assert!(quiet.events.is_empty(), "a quiet part carries no events");
        let Some(TargetVerdict::Events { resume, drained }) = quiet.verdict(&target) else {
            panic!("a quiet page must carry an events verdict");
        };
        assert!(*drained, "a quiet part whose replay completed is caught up");
        assert_eq!(*resume, 0, "and it resumes from the caller's own cursor");

        // A zero hold must not enter the wait, and the client cannot see that from the answer:
        // a request that entered a wait with a zero hold returns the same page. What it can see
        // is the pairing — a long-hold request for the same scope is parked, and the zero-hold
        // request still answers. A zero hold that waited behind, or was held up by, a parked
        // request of its own scope could not answer here. The parked request's hold is
        // `MAX_PAGE_HOLD`, so it cannot be released by its own expiry within this block, and
        // `MAX_PAGE_HOLD` caps how long it can stay parked, so asserting it is still parked
        // cannot hang the test. The zero-hold answer must also have retired its registration:
        // the responder forgets a request before it builds the reply, so a registration left
        // behind would be a leak the next request of that id would inherit.
        let park_hold_ms = u32::try_from(MAX_PAGE_HOLD.as_millis())
            .expect("MAX_PAGE_HOLD is representable in the wire's hold_ms field");
        let parked_request = page_request(3, park_hold_ms, 16);
        let parked_client = granted.clone();
        let parked = tokio::spawn(async move { parked_client.replay_page(parked_request).await });

        let zero_hold = granted.replay_page(page_request(4, 0, 16)).await??;
        assert!(
            !parked.is_finished(),
            "a zero-hold page must not release or wait behind a parked request"
        );
        assert!(
            zero_hold.events.is_empty(),
            "a zero-hold page of a caught-up part carries nothing"
        );
        let Some(TargetVerdict::Events { drained, .. }) = zero_hold.verdict(&target) else {
            panic!("a zero-hold page must carry an events verdict");
        };
        assert!(
            *drained,
            "a zero-hold page still reports the caught-up verdict"
        );
        assert!(
            registration_for(
                &rpc_handle,
                &granted_peer,
                ReplaySessionId(0),
                ReplayRequestId(4)
            )
            .is_none(),
            "an answered request retires its registration"
        );
        // The parked request is released by this test dropping it, not by a supersede: its own
        // hold is the only thing left that will answer it, and the responder's handler for it
        // holds one of `MAX_INFLIGHT_RPC_HANDLERS` permits until then, which is not a limit this
        // test reaches. Dropping the client half keeps the test from waiting out that hold.
        parked.abort();

        // The event that lands after the quiet page is still the caller's to fetch, from
        // the cursor the caller already holds.
        seed_test_store(&store, part_id.clone()).await?;
        let late = granted.replay_page(page_request(5, 250, 16)).await??;
        assert!(
            !late.events.is_empty(),
            "the event written after the quiet page must still be fetchable",
        );
        let Some(TargetVerdict::Events { drained, .. }) = late.verdict(&target) else {
            panic!("a late page must carry an events verdict");
        };
        assert!(
            *drained,
            "the page ran to the end of the log, so it is caught up"
        );
        // The write's own touch has to be fetchable. The page may also carry the
        // object's previous state as a removal row: the reader reports removals the
        // old subscription suppressed, and that is a log fact, not a defect.
        assert!(
            late.events.iter().any(|event| matches!(
                event,
                PartEvent::Changed(changed) if changed.part_ids.contains(&part_id)
            )),
            "the late page carries the granted part's own event, got {late:?}"
        );

        // The other side of the verdict still exists where it means something: a
        // page that stops on its own limit is not caught up, because backlog
        // remains, and the caller asks again from the cursor it got back.
        let truncated = granted.replay_page(page_request(6, 250, 1)).await??;
        assert_eq!(
            truncated.events.len(),
            1,
            "a one-event page carries one event"
        );
        let Some(TargetVerdict::Events { drained, .. }) = truncated.verdict(&target) else {
            panic!("a truncated page must carry an events verdict");
        };
        assert!(!drained, "a page that stopped on its limit leaves backlog");

        // `hold_ms == 0` is a drain-only request: it answers out of the log and never enters the
        // wait. What is asserted is its answer, and — above, where a request of this scope is
        // parked — that it answers rather than waiting behind one. "Must not wait" itself has no
        // further state this client can observe: a request that entered a wait with a zero hold
        // returns the same page, so the elapsed-time form that used to stand at the end of this
        // block asserted a property of the machine rather than of the code, and was removed
        // rather than retuned. The drain-only path is otherwise pinned by the verdicts below
        // against the caller's own cursor and the reader's drained bit.
        let drain_only = granted.replay_page(page_request(7, 0, 16)).await??;
        assert!(
            !drain_only.events.is_empty(),
            "a drain-only page still carries the backlog"
        );
        let Some(TargetVerdict::Events { drained, .. }) = drain_only.verdict(&target) else {
            panic!("a drain-only page must carry an events verdict");
        };
        assert!(
            *drained,
            "a drain-only page still reports the reader's caught-up verdict"
        );

        rpc_stop.stop().await?;
        router.shutdown().await?;
        server_endpoint.close().await;
        Ok(())
    }
}
