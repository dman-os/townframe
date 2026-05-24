use crate::interlude::*;

use crate::part_store::HostPartStore;

use big_sync_core::rpc::{
    BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, ObjRemoved, ObjUpserted, PeerSummaryRequest,
    PeerSummaryResult, SubEvent, SubPartsRequest,
};
use irpc::{channel, rpc_requests, WithChannels};
use tokio::sync::mpsc;

pub const BIG_SYNC_RPC_ALPN: &[u8] = b"townframe/big-sync/0";

#[async_trait]
pub trait HostBigRpcClient: Send + Sync {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<big_sync_core::mpsc::Receiver<SubEvent>, ListPartsError>>>;

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
    PeerSummary(PeerSummaryRequest),
    #[rpc(tx = channel::mpsc::Sender<SubEventWire>)]
    SubParts(SubPartsRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<Vec<BucketSummary>, ListPartsError>>)]
    GetChangedBuckets(GetChangedBucketsRequest),
    #[rpc(tx = channel::oneshot::Sender<Result<LeafBucketResult, LeafBucketsError>>)]
    LeafBuckets(LeafBucketsRequest),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SubEventWire {
    Upserted {
        cursor: big_sync_core::part_store::CursorIndex,
        part_id: big_sync_core::PartId,
        obj_id: big_sync_core::ObjId,
        payload_json: String,
    },
    Deleted {
        cursor: big_sync_core::part_store::CursorIndex,
        part_id: big_sync_core::PartId,
        obj_id: big_sync_core::ObjId,
    },
    ReplayComplete,
}

impl From<SubEvent> for SubEventWire {
    fn from(value: SubEvent) -> Self {
        match value {
            SubEvent::Upserted(inner) => Self::Upserted {
                cursor: inner.cursor,
                part_id: inner.part_id,
                obj_id: inner.obj_id,
                payload_json: serde_json::to_string(&inner.payload).expect(ERROR_JSON),
            },
            SubEvent::Deleted(inner) => Self::Deleted {
                cursor: inner.cursor,
                part_id: inner.part_id,
                obj_id: inner.obj_id,
            },
            SubEvent::ReplayComplete => Self::ReplayComplete,
        }
    }
}

impl TryFrom<SubEventWire> for SubEvent {
    type Error = serde_json::Error;

    fn try_from(value: SubEventWire) -> Result<Self, Self::Error> {
        Ok(match value {
            SubEventWire::Upserted {
                cursor,
                part_id,
                obj_id,
                payload_json,
            } => Self::Upserted(ObjUpserted {
                cursor,
                part_id,
                obj_id,
                payload: serde_json::from_str(&payload_json)?,
            }),
            SubEventWire::Deleted {
                cursor,
                part_id,
                obj_id,
            } => Self::Deleted(ObjRemoved {
                cursor,
                part_id,
                obj_id,
            }),
            SubEventWire::ReplayComplete => Self::ReplayComplete,
        })
    }
}

#[derive(Clone)]
pub struct BigSyncRpcHandle {
    client: irpc::Client<BigSyncIrpc>,
}

impl BigSyncRpcHandle {
    pub fn local_sender(&self) -> irpc::LocalSender<BigSyncIrpc> {
        self.client.as_local().expect(ERROR_IMPOSSIBLE)
    }

    pub fn protocol_handler(&self) -> BigSyncRpcProtocolHandler {
        BigSyncRpcProtocolHandler {
            tx: self.local_sender(),
        }
    }
}

#[derive(Clone)]
pub struct BigSyncRpcProtocolHandler {
    tx: irpc::LocalSender<BigSyncIrpc>,
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
        loop {
            let msg = match irpc_iroh::read_request::<BigSyncIrpc>(&conn).await {
                Ok(Some(msg)) => msg,
                Ok(None) => break,
                Err(err) => {
                    warn!(?err, "error reading big sync rpc request");
                    break;
                }
            };
            if self.tx.send_raw(msg).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

pub struct BigSyncRpcStopToken {
    cancel_token: CancellationToken,
    subscription_tasks: Arc<utils_rs::AbortableJoinSet>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl BigSyncRpcStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.subscription_tasks
            .stop(Duration::from_secs(5))
            .await
            .wrap_err("failed stopping big sync rpc subscription forwarders")?;
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5))
            .await
            .wrap_err("failed stopping big sync rpc")
    }
}

pub async fn spawn_big_sync_rpc(
    store: Arc<dyn HostPartStore>,
) -> Res<(BigSyncRpcHandle, BigSyncRpcStopToken)> {
    let (rpc_tx, mut rpc_rx) = mpsc::channel(1024);
    let client = irpc::Client::<BigSyncIrpc>::local(rpc_tx);

    let cancel_token = CancellationToken::new();
    let subscription_tasks = Arc::new(utils_rs::AbortableJoinSet::new());
    let fut = {
        let cancel_token = cancel_token.clone();
        let subscription_tasks = Arc::clone(&subscription_tasks);
        let mut worker = BigSyncRpcWorker {
            store,
            cancel_token: cancel_token.clone(),
            subscription_tasks,
        };
        async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => break,
                    msg = rpc_rx.recv() => {
                        let Some(msg) = msg else {
                            break;
                        };
                        worker.handle_rpc_message(msg).await;
                    }
                }
            }
            eyre::Ok(())
        }
    };
    let join_handle = tokio::spawn(async { fut.await.unwrap() });

    Ok((
        BigSyncRpcHandle { client },
        BigSyncRpcStopToken {
            cancel_token,
            subscription_tasks,
            join_handle,
        },
    ))
}

#[derive(Clone)]
pub struct IrohBigSyncRpcClient {
    client: irpc::Client<BigSyncIrpc>,
}

impl IrohBigSyncRpcClient {
    pub fn new(endpoint: iroh::Endpoint, endpoint_addr: iroh::EndpointAddr) -> Self {
        Self {
            client: irpc_iroh::client::<BigSyncIrpc>(endpoint, endpoint_addr, BIG_SYNC_RPC_ALPN),
        }
    }
}

#[async_trait]
impl HostBigRpcClient for IrohBigSyncRpcClient {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
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

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<big_sync_core::mpsc::Receiver<SubEvent>, ListPartsError>>>
    {
        let part_ids: std::collections::HashSet<_> =
            req.parts.iter().map(|part| part.part_id).collect();
        match self
            .peer_summary(PeerSummaryRequest { parts: part_ids })
            .await?
        {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => return Ok(Ok(Err(err))),
            Err(err) => return Ok(Err(err)),
        }

        let remote_rx = match self.client.server_streaming(req, 1024).await {
            Ok(rx) => rx,
            Err(err) => {
                warn!(?err, "big sync sub_parts rpc transport failed");
                return Ok(Err(big_sync_core::rpc::RpcError::TransportError));
            }
        };

        let (local_tx, local_rx) = big_sync_core::mpsc::unbounded(
            "big-sync-iroh-rpc".into(),
            "big-sync-rpc-client".into(),
        );
        tokio::spawn(async move {
            let mut remote_rx = remote_rx;
            loop {
                match remote_rx.recv().await {
                    Ok(Some(evt)) => {
                        let evt = match SubEvent::try_from(evt) {
                            Ok(evt) => evt,
                            Err(err) => {
                                warn!(?err, "big sync sub_parts payload decode failed");
                                break;
                            }
                        };
                        if local_tx.send(evt).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        warn!(?err, "big sync sub_parts bridge failed");
                        break;
                    }
                }
            }
        });

        Ok(Ok(Ok(local_rx)))
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
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
        req: LeafBucketsRequest,
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
    store: Arc<dyn HostPartStore>,
    cancel_token: CancellationToken,
    subscription_tasks: Arc<utils_rs::AbortableJoinSet>,
}

impl BigSyncRpcWorker {
    #[tracing::instrument(skip(self, msg))]
    async fn handle_rpc_message(&mut self, msg: BigSyncRpcMessage) {
        match msg {
            BigSyncRpcMessage::PeerSummary(req) => {
                let WithChannels { inner, tx, .. } = req;
                let out = {
                    let parts = self.store.summarize_parts(inner.parts).await.unwrap();
                    parts.map(|parts| PeerSummaryResult {
                        parts,
                        deepest_bucket_level: big_sync_core::BuckId::MAX_LEVEL,
                    })
                };
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            BigSyncRpcMessage::SubParts(req) => {
                let WithChannels { inner, tx, .. } = req;
                let sub = self.store.subscribe(inner).await.unwrap();
                let Ok(sub) = sub else {
                    warn!("sub_parts request for unknown parts");
                    return;
                };
                let child_token = self.cancel_token.child_token();
                self.subscription_tasks
                    .spawn(async move {
                        let fut = async move {
                            loop {
                                tokio::select! {
                                    biased;
                                    _ = child_token.cancelled() => break,
                                    evt = sub.recv() => {
                                        let evt = match evt {
                                            Ok(evt) => evt,
                                            Err(_) => break,
                                        };
                                        if tx.send(SubEventWire::from(evt)).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            eyre::Ok(())
                        };
                        fut.await.unwrap();
                    })
                    .expect("failed spawning big sync rpc subscription forwarder");
            }
            BigSyncRpcMessage::GetChangedBuckets(req) => {
                let WithChannels { inner, tx, .. } = req;
                let out = self.store.get_changed_buckets(inner).await.unwrap();
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
            BigSyncRpcMessage::LeafBuckets(req) => {
                let WithChannels { inner, tx, .. } = req;
                let out = self.store.leaf_buckets(inner).await.unwrap();
                tx.send(out).await.inspect_err(|_| warn!(ERROR_CALLER)).ok();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::part_store::memory::MemoryPartStore;
    use crate::part_store::HostPartStore;
    use big_sync_core::rpc::SubEvent;
    use big_sync_core::{BuckId, Byte32Id, FingerprintSeed, ObjId, PartId, PeerId};
    use iroh::protocol::Router;
    use std::net::Ipv4Addr;

    fn test_peer() -> PeerId {
        PeerId::new([
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
        ])
    }

    fn test_part() -> PartId {
        PartId(Byte32Id::new([
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12, //
            32, 12, 54, 54, 65, 112, 213, 43, 12, 54, 123, 123, 54, 23, 68, 12,
        ]))
    }

    fn test_obj(byte: u8) -> ObjId {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        ObjId(Byte32Id::new(bytes))
    }

    async fn collect_sub_events(rx: big_sync_core::mpsc::Receiver<SubEvent>) -> Res<Vec<SubEvent>> {
        let mut out = Vec::new();
        loop {
            let evt = rx.recv().await?;
            let done = matches!(evt, SubEvent::ReplayComplete);
            out.push(evt);
            if done {
                break;
            }
        }
        Ok(out)
    }

    async fn seed_test_store(store: &MemoryPartStore, part_id: PartId) -> Res<()> {
        store.ensure_part(part_id).await?;

        let live_obj = test_obj(1);
        let dead_obj = test_obj(2);
        let payload_live = serde_json::json!({"kind":"live","value":1});
        let payload_dead = serde_json::json!({"kind":"dead","value":2});

        store
            .upsert_obj(live_obj, payload_live.clone(), vec![part_id], None)
            .await?;
        store
            .upsert_obj(dead_obj, payload_dead.clone(), vec![part_id], None)
            .await?;
        store.remove_obj_from_part(dead_obj, part_id, None).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn real_iroh_rpc_roundtrip_matches_store() -> Res<()> {
        let peer = test_peer();
        let part_id = test_part();
        let store = Arc::new(MemoryPartStore::new(peer));
        seed_test_store(&store, part_id).await?;

        let expected_peer_summary = PeerSummaryResult {
            parts: store
                .summarize_parts([part_id].into_iter().collect())
                .await?
                .unwrap(),
            deepest_bucket_level: big_sync_core::BuckId::MAX_LEVEL,
        };
        let expected_changed_buckets = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id,
                offset: BuckId::ROOT,
                since: 0,
                limit_hint: 16,
            })
            .await?
            .unwrap();
        let expected_leaf_buckets = store
            .leaf_buckets(LeafBucketsRequest {
                part_id,
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
        let expected_sub_events = collect_sub_events(
            store
                .subscribe(SubPartsRequest {
                    parts: vec![big_sync_core::rpc::PartStreamCursorRequest { part_id, cursor: 0 }],
                })
                .await?
                .unwrap(),
        )
        .await?;

        let rpc_store = Arc::<MemoryPartStore>::clone(&store);
        let rpc_store: Arc<dyn HostPartStore> = rpc_store;
        let (rpc_handle, rpc_stop) = spawn_big_sync_rpc(rpc_store).await?;
        let local_peer_summary: Result<PeerSummaryResult, ListPartsError> = rpc_handle
            .client
            .rpc(PeerSummaryRequest {
                parts: [part_id].into_iter().collect(),
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
            .peer_summary(PeerSummaryRequest {
                parts: [part_id].into_iter().collect(),
            })
            .await?;
        assert_eq!(peer_summary, Ok(Ok(expected_peer_summary)));

        let changed_buckets = client
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id,
                offset: BuckId::ROOT,
                since: 0,
                limit_hint: 16,
            })
            .await?;
        assert_eq!(changed_buckets, Ok(Ok(expected_changed_buckets)));

        let leaf_buckets = client
            .leaf_buckets(LeafBucketsRequest {
                part_id,
                since: 0,
                buckets: vec![big_sync_core::rpc::LeafBucketRequest {
                    buck_id: BuckId::ROOT,
                    after: None,
                }],
                seed: FingerprintSeed::new(0xaaaa_bbbb, 0xcccc_dddd),
                limit_hint: 16,
            })
            .await?;
        assert_eq!(leaf_buckets, Ok(Ok(expected_leaf_buckets)));

        let sub_events = client
            .sub_parts(SubPartsRequest {
                parts: vec![big_sync_core::rpc::PartStreamCursorRequest { part_id, cursor: 0 }],
            })
            .await???;
        let sub_events =
            tokio::time::timeout(Duration::from_secs(10), collect_sub_events(sub_events)).await??;
        assert_eq!(sub_events, expected_sub_events);

        drop(client);
        rpc_stop.stop().await?;
        router.shutdown().await?;
        server_endpoint.close().await;
        Ok(())
    }
}
