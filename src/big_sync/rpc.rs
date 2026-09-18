use crate::interlude::*;

use crate::part_store::{HostPartStore, ReadTarget};

use big_sync_core::PeerKey;
use big_sync_core::rpc::{
    BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PeerSummaryRequest, PeerSummaryResult, ReplayPageOutcome,
    ReplayPageRequest,
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
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    async fn replay_page(
        &self,
        req: ScopedRequest<ReplayPageRequest>,
    ) -> Res<BigSyncRpcResult<ReplayPageOutcome>>;

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
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
        self.inner
            .peer_summary(ScopedRequest {
                scope_key: Arc::clone(&self.scope_key),
                inner: req,
            })
            .await
    }

    async fn replay_page(
        &self,
        req: ReplayPageRequest,
    ) -> Res<BigSyncRpcResult<ReplayPageOutcome>> {
        self.inner
            .replay_page(ScopedRequest {
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
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    async fn replay_page(&self, req: ReplayPageRequest)
    -> Res<BigSyncRpcResult<ReplayPageOutcome>>;

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
    #[rpc(tx = channel::oneshot::Sender<Result<PeerSummaryResult, ListPartsError>>)]
    PeerSummary(ScopedRequest<PeerSummaryRequest>),
    #[rpc(tx = channel::oneshot::Sender<ReplayPageOutcome>)]
    ReplayPage(ScopedRequest<ReplayPageRequest>),
    #[rpc(tx = channel::oneshot::Sender<Result<Vec<BucketSummary>, ListPartsError>>)]
    GetChangedBuckets(ScopedRequest<GetChangedBucketsRequest>),
    #[rpc(tx = channel::oneshot::Sender<Result<LeafBucketResult, LeafBucketsError>>)]
    LeafBuckets(ScopedRequest<LeafBucketsRequest>),
}
impl IrohBigSyncRpcClient {
    pub fn new(endpoint: iroh::Endpoint, endpoint_addr: iroh::EndpointAddr) -> Self {
        Self::new_with_alpn(endpoint, endpoint_addr, BIG_SYNC_RPC_ALPN)
    }

    pub fn new_with_alpn(
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
}

impl BigSyncRpcHandle {
    pub fn local_sender(&self) -> irpc::LocalSender<BigSyncIrpc> {
        self.client.as_local().expect(ERROR_IMPOSSIBLE)
    }

    pub fn protocol_handler(&self) -> BigSyncRpcProtocolHandler {
        self.protocol_handler.clone()
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
    let fut = {
        let cancel_token = cancel_token.clone();
        let worker = Arc::new(BigSyncRpcWorker { stores });
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
pub struct IrohBigSyncRpcClient {
    client: irpc::Client<BigSyncIrpc>,
}

#[async_trait]
impl WireBigSyncRpcClient for IrohBigSyncRpcClient {
    async fn peer_summary(
        &self,
        req: ScopedRequest<PeerSummaryRequest>,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>> {
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
    ) -> Res<BigSyncRpcResult<ReplayPageOutcome>> {
        let response = match self.client.rpc(req).await {
            Ok(response) => response,
            Err(err) => {
                warn!(?err, "big sync replay_page rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };
        Ok(Ok(response))
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

struct BigSyncRpcWorker {
    stores: HashMap<Arc<str>, Arc<dyn HostPartStore>>,
}

impl BigSyncRpcWorker {
    #[tracing::instrument(skip(self, msg))]
    async fn handle_rpc_message(
        &self,
        msg: BigSyncRpcMessage,
        authenticated_peer: Option<PeerKey>,
    ) {
        match msg {
            BigSyncRpcMessage::PeerSummary(req) => {
                let WithChannels { inner, tx, .. } = req;
                let Some(store) = self.stores.get(&inner.scope_key) else {
                    warn!(scope_key = %inner.scope_key, "peer_summary for unknown scope");
                    tx.send(Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![],
                    }))
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
                    return;
                };
                // An unauthenticated caller is refused on every arm: this surface has
                // no local caller to serve, and a local in-process caller reaches the
                // store directly. Refused as unknown rather than empty, so "not for
                // you" cannot be told apart from "no such part".
                let Some(asker) = authenticated_peer else {
                    warn!(scope_key = %inner.scope_key, "rejecting unauthenticated peer_summary request");
                    tx.send(Err(ListPartsError::UnkownParts {
                        unkown_parts: inner.inner.parts.into_iter().collect(),
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
                    // The access half, asked for every named part before anything is
                    // summarized: a part the asker may not read has to look exactly like
                    // one this scope does not know, so it is folded into the same
                    // `UnkownParts` answer rather than omitted from it.
                    let mut unreadable = Vec::new();
                    let mut readable = HashSet::new();
                    for part_id in parts {
                        if store
                            .read_denied(ReadTarget::Part(part_id.clone()), asker.clone())
                            .await
                            .unwrap()
                        {
                            unreadable.push(part_id);
                        } else {
                            readable.insert(part_id);
                        }
                    }
                    if !unreadable.is_empty() {
                        unreadable.sort_unstable();
                        Err(ListPartsError::UnkownParts {
                            unkown_parts: unreadable,
                        })
                    } else {
                        match store.summarize_parts(readable).await.unwrap() {
                            Ok(parts) => {
                                let mut summaries = HashMap::new();
                                for (part_id, summary) in parts {
                                    let since =
                                        asker_part_cursors.get(&part_id).copied().unwrap_or(0);
                                    let dirty = store
                                        .part_dirty_count(
                                            part_id.clone(),
                                            Some(asker.clone()),
                                            since,
                                        )
                                        .await
                                        .unwrap();
                                    summaries.insert(part_id, summary.into_strat_summaries(dirty));
                                }
                                Ok(PeerSummaryResult { parts: summaries })
                            }
                            Err(err) => Err(err),
                        }
                    }
                };
                tx.send(out)
                    .await
                    .inspect_err(|_| warn_loc!(ERROR_CALLER))
                    .ok();
            }
            BigSyncRpcMessage::ReplayPage(req) => {
                let WithChannels { inner, tx, .. } = req;
                // An unauthenticated caller is not a special case of "nothing to
                // send": it is unauthorized, and says so.
                let Some(subscriber) = authenticated_peer else {
                    warn!("rejecting unauthenticated replay_page request");
                    tx.send(ReplayPageOutcome::Unauthorized)
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return;
                };
                let Some(store) = self.stores.get(&inner.scope_key) else {
                    warn!(scope_key = %inner.scope_key, "replay_page for unknown scope");
                    tx.send(ReplayPageOutcome::UnknownPart)
                        .await
                        .inspect_err(|_| warn_loc!(ERROR_CALLER))
                        .ok();
                    return;
                };
                let out = store
                    .replay_page(
                        inner.inner.target,
                        page_limit(inner.inner.limit),
                        subscriber,
                        Duration::from_millis(u64::from(inner.inner.hold_ms)).min(MAX_PAGE_HOLD),
                    )
                    .await
                    .unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::part_store::HostPartStore;
    use crate::part_store::memory::MemoryPartStore;
    use big_sync_core::rpc::{LeafBucketRequest, PartEvent, ReplayPageRequest, SubscriptionTarget};
    use big_sync_core::{BuckId, ByteKey, FingerprintSeed, ObjKey, PartKey};
    use iroh::protocol::Router;
    use keyhive_core::access::Access;
    use std::net::Ipv4Addr;

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
        Peer(&'a IrohBigSyncRpcClient),
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
    ) -> Res<Result<PeerSummaryResult, ListPartsError>> {
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
    ) -> Res<ReplayPageOutcome> {
        Ok(match caller {
            Caller::Peer(client) => client.replay_page(req).await??,
            Caller::Unauthenticated(client) => client.rpc(req).await?,
        })
    }

    /// A permitted asker gets the part's own summary; a refused one gets the part named
    /// as unknown, which is the same answer an unknown part gets.
    async fn assert_peer_summary_arm(
        caller: Caller<'_>,
        caller_label: &str,
        entitlement: Entitlement,
        part_id: &PartKey,
        req: ScopedRequest<PeerSummaryRequest>,
        expected: &PeerSummaryResult,
    ) -> Res<()> {
        let answer = summary_answer(caller, req).await?;
        match (entitlement, answer) {
            (Entitlement::Permitted, Ok(answer)) => assert_eq!(
                &answer, expected,
                "{caller_label}: a permitted asker gets the store's own summary"
            ),
            (Entitlement::Refused, Err(ListPartsError::UnkownParts { unkown_parts })) => {
                assert_eq!(
                    unkown_parts,
                    vec![part_id.clone()],
                    "{caller_label}: the refusal names the part it will not answer for"
                );
            }
            (entitlement, answer) => {
                panic!("{caller_label}: {entitlement:?} peer_summary answered {answer:?}")
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
            Entitlement::Refused => match outcome {
                ReplayPageOutcome::Unauthorized => Ok(()),
                other => panic!("{caller_label}: a refused page answered {other:?}"),
            },
            Entitlement::Permitted => {
                for _ in 0..ATTEMPTS {
                    match outcome {
                        ReplayPageOutcome::Events(page) if page.events == expected_events => {
                            return Ok(());
                        }
                        ReplayPageOutcome::Events(_) => {
                            outcome = replay_page_answer(caller, req.clone()).await?;
                        }
                        other => panic!("{caller_label}: a permitted page answered {other:?}"),
                    }
                }
                panic!("{caller_label}: no page carried the granted events");
            }
        }
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
        let granted = IrohBigSyncRpcClient::new(granted_endpoint, server_addr.clone());
        let refused_endpoint = test_endpoint().await?;
        let refused_peer = PeerKey::new(*refused_endpoint.id().as_bytes());
        let refused = IrohBigSyncRpcClient::new(refused_endpoint, server_addr.clone());
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
                target: page_target.clone(),
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
            .replay_page(
                page_target.clone(),
                16,
                granted_peer.clone(),
                Duration::from_millis(250),
            )
            .await?;
        let ReplayPageOutcome::Events(expected_page) = expected_page else {
            panic!("a granted part answers a page, got {expected_page:?}");
        };
        assert!(
            !expected_page.events.is_empty(),
            "the granted page carries the seeded events"
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
                &part_id,
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
        let granted = IrohBigSyncRpcClient::new(granted_endpoint, server_addr);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(granted_peer.clone(), Access::Read)]),
            )
            .await?;

        let page_request = |hold_ms: u32| ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: ReplayPageRequest {
                target: SubscriptionTarget::Part {
                    part_id: part_id.clone(),
                    cursor: 0,
                },
                limit: 16,
                hold_ms,
            },
        };

        // Parked, not answered: nothing is waiting on the part, so the responder holds
        // the request until its hold elapses. The client then gives up on it.
        let held = granted.replay_page(page_request(5_000));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), held)
                .await
                .is_err(),
            "a page with nothing to send is held rather than answered early"
        );

        // The cancelled request is still parked on the responder. A fresh page is
        // answered anyway, and it is answered *before* the parked request's hold
        // elapses: a loop that handled pages inline would serialize behind the parked
        // one and only answer after its full 5s. The margin is wide enough that only
        // serialization can trip it.
        let started = std::time::Instant::now();
        let fresh = granted.replay_page(page_request(50)).await??;
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "a parked page must not serialize the next request behind its hold, waited {:?}",
            started.elapsed()
        );
        let ReplayPageOutcome::Events(page) = fresh else {
            panic!("a granted part answers a page, got {fresh:?}");
        };
        assert!(page.events.is_empty(), "the part still has nothing to send");

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
        let granted = IrohBigSyncRpcClient::new(granted_endpoint, server_addr);
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(granted_peer.clone(), Access::Read)]),
            )
            .await?;

        let page_request = |hold_ms: u32, limit: u32| ScopedRequest {
            scope_key: Arc::from("test-scope"),
            inner: ReplayPageRequest {
                target: SubscriptionTarget::Part {
                    part_id: part_id.clone(),
                    cursor: 0,
                },
                limit,
                hold_ms,
            },
        };

        // A page that carries nothing without the replay half reporting completion is not
        // a verdict: it answers `drained: false` and carries the caller's own cursor. A
        // zero limit reaches that branch without draining anything, which is the same
        // answer a page that runs out of its hold gives.
        let undrained = granted.replay_page(page_request(50, 0)).await??;
        let ReplayPageOutcome::Events(undrained) = undrained else {
            panic!("a granted part answers a page, got {undrained:?}");
        };
        assert!(
            undrained.events.is_empty(),
            "a page that carries nothing has no events"
        );
        assert!(
            !undrained.drained,
            "a page that carried nothing is not a caught-up verdict"
        );
        assert_eq!(
            undrained.resume, 0,
            "and it resumes from the caller's own cursor"
        );

        // Nothing has joined the part, so this page can only leave on its hold — and
        // this time the replay half has reported completion, which *is* the caught-up
        // verdict.
        let quiet = granted.replay_page(page_request(50, 16)).await??;
        let ReplayPageOutcome::Events(quiet) = quiet else {
            panic!("a granted part answers a page, got {quiet:?}");
        };
        assert!(quiet.events.is_empty(), "a quiet part carries no events");
        assert!(
            quiet.drained,
            "a quiet part whose replay completed is caught up"
        );
        assert_eq!(
            quiet.resume, 0,
            "and it resumes from the caller's own cursor"
        );

        // The event that lands after the quiet page is still the caller's to fetch, from
        // the cursor the caller already holds.
        seed_test_store(&store, part_id.clone()).await?;
        let late = granted.replay_page(page_request(250, 16)).await??;
        let ReplayPageOutcome::Events(late) = late else {
            panic!("a granted part answers a page, got {late:?}");
        };
        assert!(
            !late.events.is_empty(),
            "the event written after the quiet page must still be fetchable",
        );
        assert!(
            late.drained,
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
        let truncated = granted.replay_page(page_request(250, 1)).await??;
        let ReplayPageOutcome::Events(truncated) = truncated else {
            panic!("a granted part answers a page, got {truncated:?}");
        };
        assert_eq!(
            truncated.events.len(),
            1,
            "a one-event page carries one event"
        );
        assert!(
            !truncated.drained,
            "a page that stopped on its limit leaves backlog"
        );

        // `hold_ms == 0` is a drain-only request: it answers out of the log and
        // never enters the wait, so it cannot take its hold's worth of time.
        let started = std::time::Instant::now();
        let drain_only = granted.replay_page(page_request(0, 16)).await??;
        let ReplayPageOutcome::Events(drain_only) = drain_only else {
            panic!("a granted part answers a page, got {drain_only:?}");
        };
        assert!(
            !drain_only.events.is_empty(),
            "a drain-only page still carries the backlog"
        );
        assert!(
            drain_only.drained,
            "a drain-only page still reports the reader's caught-up verdict"
        );
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "hold_ms == 0 must not wait: it took {:?}",
            started.elapsed()
        );

        rpc_stop.stop().await?;
        router.shutdown().await?;
        server_endpoint.close().await;
        Ok(())
    }
}
