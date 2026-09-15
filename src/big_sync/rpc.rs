use crate::interlude::*;

use crate::part_store::HostPartStore;

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

    async fn replay_page(
        &self,
        req: ReplayPageRequest,
    ) -> Res<BigSyncRpcResult<ReplayPageOutcome>>;

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
        let mut worker = BigSyncRpcWorker { stores };
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = rpc_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        worker.handle_rpc_message(msg, None).await;
                    }
                    authenticated = authenticated_rx.recv() => {
                        let Some((peer_id, msg)) = authenticated else {
                            break;
                        };
                        worker.handle_rpc_message(msg, Some(peer_id)).await;
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
        &mut self,
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
                let out = {
                    // The count is a fact about the asker, so the responder counts it
                    // against the cursor the asker advertised. `authenticated_peer` is
                    // the asker's principal; `None` there is the local in-process
                    // caller, which access rows do not gate.
                    let PeerSummaryRequest {
                        parts,
                        asker_part_cursors,
                    } = inner.inner;
                    match store.summarize_parts(parts).await.unwrap() {
                        Ok(parts) => {
                            let mut summaries = HashMap::new();
                            for (part_id, summary) in parts {
                                let since =
                                    asker_part_cursors.get(&part_id).copied().unwrap_or(0);
                                let dirty = store
                                    .part_dirty_count(part_id.clone(), authenticated_peer.clone(), since)
                                    .await
                                    .unwrap();
                                summaries.insert(part_id, summary.into_strat_summaries(dirty));
                            }
                            Ok(PeerSummaryResult { parts: summaries })
                        }
                        Err(err) => Err(err),
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
                        inner.inner.limit,
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
                let out = store.get_changed_buckets(inner.inner).await.unwrap();
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
                let out = store.leaf_buckets(inner.inner).await.unwrap();
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
    use big_sync_core::{BuckId, ByteKey, FingerprintSeed, ObjKey, PartKey};
    use iroh::protocol::Router;
    use std::net::Ipv4Addr;

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
        store.add_obj_to_parts(live_obj, vec![part_id.clone()]).await?;
        store
            .set_obj_payload(dead_obj.clone(), payload_dead.clone())
            .await?;
        store.add_obj_to_parts(dead_obj.clone(), vec![part_id.clone()]).await?;
        store.remove_obj_from_part(dead_obj, part_id).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_iroh_rpc_roundtrip_matches_store() -> Res<()> {
        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new());
        seed_test_store(&store, part_id.clone()).await?;

        // The same expectation is asserted for the in-process call and the network
        // call. They agree because this store holds no access rows, so the access half
        // is 0 for the local principal and for the connecting peer alike, and the
        // member half does not depend on who is asking.
        let expected_peer_summary = {
            let mut summaries = HashMap::new();
            for (part_id, summary) in store
                .summarize_parts([part_id.clone()].into_iter().collect())
                .await?
                .unwrap()
            {
                let dirty = HostPartStore::part_dirty_count(store.as_ref(), part_id.clone(), None, 0).await?;
                summaries.insert(part_id, summary.into_strat_summaries(dirty));
            }
            PeerSummaryResult { parts: summaries }
        };
        let expected_changed_buckets = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id: part_id.clone(),
                offset: BuckId::ROOT,
                to_level: BuckId::MAX_LEVEL,
                since: 0,
                limit_hint: 16,
            })
            .await?
            .unwrap();
        let expected_leaf_buckets = store
            .leaf_buckets(LeafBucketsRequest {
                part_id: part_id.clone(),
                since: 0,
                buckets: vec![big_sync_core::rpc::LeafBucketRequest {
                    buck_id: BuckId::ROOT,
                    after: None,
                }],
                seed: FingerprintSeed::new(0xaaaa_bbbb, 0xcccc_dddd),
                limit_hint: 16,
            })
            .await?
            .unwrap();
        // The in-process call and the network call must answer the same page.
        // They agree because this store holds no access rows, so the member half
        // is the same for both and neither is denied.
        let page_target =
            big_sync_core::rpc::SubscriptionTarget::Part { part_id: part_id.clone(), cursor: 0 };
        let expected_page = store
            .replay_page(
                page_target.clone(),
                16,
                PeerKey::new([0u8; 32]),
                Duration::from_millis(250),
            )
            .await?;

        let rpc_store = Arc::<MemoryPartStore>::clone(&store);
        let rpc_store: Arc<dyn HostPartStore> = rpc_store;
        let (rpc_handle, rpc_stop) =
            spawn_big_sync_rpc(HashMap::from([(Arc::from("test-scope"), rpc_store)])).await?;
        let local_peer_summary: Result<PeerSummaryResult, ListPartsError> = rpc_handle
            .client
            .rpc(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: PeerSummaryRequest {
                    parts: [part_id.clone()].into_iter().collect(),
                    asker_part_cursors: HashMap::from([(part_id.clone(), 0)]),
                },
            })
            .await?;
        assert_eq!(local_peer_summary, Ok(expected_peer_summary.clone()));

        let server_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind_addr((Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let router = Router::builder(server_endpoint.clone())
            .accept(BIG_SYNC_RPC_ALPN, rpc_handle.protocol_handler())
            .spawn();
        let server_addr = router.endpoint().addr();
        assert!(
            !server_addr.addrs.is_empty(),
            "server endpoint address should expose at least one transport address"
        );
        let client_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind_addr((Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let client = IrohBigSyncRpcClient::new(client_endpoint, server_addr);

        let peer_summary = client
            .peer_summary(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: PeerSummaryRequest {
                    parts: [part_id.clone()].into_iter().collect(),
                    asker_part_cursors: HashMap::from([(part_id.clone(), 0)]),
                },
            })
            .await?;
        assert_eq!(peer_summary, Ok(Ok(expected_peer_summary)));

        let changed_buckets = client
            .get_changed_buckets(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: GetChangedBucketsRequest {
                    part_id: part_id.clone(),
                    offset: BuckId::ROOT,
                    to_level: BuckId::MAX_LEVEL,
                    since: 0,
                    limit_hint: 16,
                },
            })
            .await?;
        assert_eq!(changed_buckets, Ok(Ok(expected_changed_buckets)));

        let leaf_buckets = client
            .leaf_buckets(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: LeafBucketsRequest {
                    part_id,
                    since: 0,
                    buckets: vec![big_sync_core::rpc::LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after: None,
                    }],
                    seed: FingerprintSeed::new(0xaaaa_bbbb, 0xcccc_dddd),
                    limit_hint: 16,
                },
            })
            .await?;
        assert_eq!(leaf_buckets, Ok(Ok(expected_leaf_buckets)));

        let page = client
            .replay_page(ScopedRequest {
                scope_key: Arc::from("test-scope"),
                inner: big_sync_core::rpc::ReplayPageRequest {
                    hold_ms: 50,
                    target: page_target,
                    limit: 16,
                },
            })
            .await??;
        assert_eq!(page, expected_page);

        drop(client);
        rpc_stop.stop().await?;
        router.shutdown().await?;
        server_endpoint.close().await;
        Ok(())
    }
}
