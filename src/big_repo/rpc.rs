use crate::{interlude::*, BigRepo};

use big_sync_core::PeerId;
use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc::{channel, rpc_requests, WithChannels};
use std::{collections::HashMap, sync::RwLock};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// ALPN for direct BigRepo-to-BigRepo RPCs.
///
/// This protocol is deliberately separate from Subduction. It is used for
/// direct node-to-node control signals that must not be relayed by a
/// Subduction handler.
pub const REPO_SYNC_ALPN: &[u8] = b"townframe/repo-sync/0";

/// A best-effort notification that the sender's local Keyhive state changed.
///
/// The notification carries no state and is not a consistency mechanism. The
/// receiver uses it only to start a normal Keyhive sync over the authenticated
/// BigRepo connection from which the peer is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyhiveChangedRpcEvent {
    /// The first event confirms that the subscription is installed. Later
    /// events are local-change invalidation hints.
    pub initial: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubscribeKeyhiveChangesRequest;

#[rpc_requests(message = RepoSyncRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RepoSyncRpc {
    /// Keep a direct stream open for local Keyhive invalidation notices.
    #[rpc(tx = channel::mpsc::Sender<KeyhiveChangedRpcEvent>)]
    SubscribeKeyhiveChanges(SubscribeKeyhiveChangesRequest),
}

#[derive(Debug, Default)]
struct RpcPeerMap {
    by_endpoint: HashMap<iroh::EndpointId, PeerId>,
    by_peer: HashMap<PeerId, iroh::EndpointId>,
}

impl RpcPeerMap {
    fn register(&mut self, endpoint_id: iroh::EndpointId, peer_id: PeerId) {
        if let Some(old_peer_id) = self.by_endpoint.insert(endpoint_id, peer_id) {
            self.by_peer.remove(&old_peer_id);
        }
        if let Some(old_endpoint_id) = self.by_peer.insert(peer_id, endpoint_id) {
            self.by_endpoint.remove(&old_endpoint_id);
        }
    }

    fn unregister(&mut self, peer_id: PeerId) {
        if let Some(endpoint_id) = self.by_peer.remove(&peer_id) {
            self.by_endpoint.remove(&endpoint_id);
        }
    }

    fn lookup(&self, endpoint_id: iroh::EndpointId) -> PeerId {
        self.by_endpoint
            .get(&endpoint_id)
            .copied()
            .unwrap_or_else(|| PeerId::new(*endpoint_id.as_bytes()))
    }
}

#[derive(Clone)]
pub struct BigRepoRpcHandle {
    rpc_tx: mpsc::Sender<(PeerId, RepoSyncRpcMessage)>,
    peer_map: Arc<RwLock<RpcPeerMap>>,
}

impl BigRepoRpcHandle {
    pub fn local_sender(&self) -> mpsc::Sender<(PeerId, RepoSyncRpcMessage)> {
        self.rpc_tx.clone()
    }

    /// Associate an authenticated Iroh endpoint with its BigRepo identity.
    ///
    /// The mapping is only an identity seam for RPC consumers; notification
    /// delivery itself does not perform authorization.
    pub fn register_peer(&self, endpoint_id: iroh::EndpointId, peer_id: PeerId) {
        self.peer_map
            .write()
            .expect(ERROR_MUTEX)
            .register(endpoint_id, peer_id);
    }

    /// Remove the identity mapping for a disconnected BigRepo peer.
    pub fn unregister_peer(&self, peer_id: PeerId) {
        self.peer_map
            .write()
            .expect(ERROR_MUTEX)
            .unregister(peer_id);
    }

    pub fn protocol_handler(&self) -> BigRepoRpcProtocolHandler {
        BigRepoRpcProtocolHandler {
            tx: self.rpc_tx.clone(),
            peer_map: Arc::clone(&self.peer_map),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BigRepoRpcProtocolHandler {
    tx: mpsc::Sender<(PeerId, RepoSyncRpcMessage)>,
    peer_map: Arc<RwLock<RpcPeerMap>>,
}

impl ProtocolHandler for BigRepoRpcProtocolHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let endpoint_id = conn.remote_id();
        let peer_id = self.peer_map.read().expect(ERROR_MUTEX).lookup(endpoint_id);
        loop {
            let msg = match irpc_iroh::read_request::<RepoSyncRpc>(&conn).await {
                Ok(Some(msg)) => msg,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%peer_id, ?error, "error reading BigRepo RPC request");
                    break;
                }
            };
            if self.tx.send((peer_id, msg)).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

pub struct BigRepoRpcStopToken {
    cancel_token: CancellationToken,
    subscription_tasks: Arc<utils_rs::AbortableJoinSet>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl BigRepoRpcStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.subscription_tasks
            .stop(Duration::from_secs(5))
            .await
            .wrap_err("failed stopping BigRepo RPC subscription forwarders")?;
        utils_rs::wait_on_handle_with_timeout(self.join_handle, Duration::from_secs(5))
            .await
            .wrap_err("failed stopping BigRepo RPC")
    }
}

pub async fn spawn_repo_rpc(
    big_repo: Arc<BigRepo>,
) -> Res<(BigRepoRpcHandle, BigRepoRpcStopToken)> {
    let (rpc_tx, mut rpc_rx) = mpsc::channel(1024);
    let peer_map = Arc::new(RwLock::new(RpcPeerMap::default()));
    let cancel_token = CancellationToken::new();
    let subscription_tasks = Arc::new(utils_rs::AbortableJoinSet::new());
    let worker_subscription_tasks = Arc::clone(&subscription_tasks);
    let worker_cancel_token = cancel_token.clone();

    let join_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = worker_cancel_token.cancelled() => break,
                msg = rpc_rx.recv() => {
                    let Some((peer_id, msg)) = msg else {
                        break;
                    };
                    handle_rpc_message(
                        Arc::clone(&big_repo),
                        worker_subscription_tasks.as_ref(),
                        &worker_cancel_token,
                        peer_id,
                        msg,
                    ).await;
                }
            }
        }
    });

    Ok((
        BigRepoRpcHandle { rpc_tx, peer_map },
        BigRepoRpcStopToken {
            cancel_token,
            subscription_tasks,
            join_handle,
        },
    ))
}

async fn handle_rpc_message(
    big_repo: Arc<BigRepo>,
    subscription_tasks: &utils_rs::AbortableJoinSet,
    cancel_token: &CancellationToken,
    peer_id: PeerId,
    msg: RepoSyncRpcMessage,
) {
    match msg {
        RepoSyncRpcMessage::SubscribeKeyhiveChanges(req) => {
            let WithChannels { tx, .. } = req;
            let mut changes = big_repo.subscribe_keyhive_changes();
            let cancel = cancel_token.child_token();
            let _task = subscription_tasks
                .spawn(async move {
                    if tx
                        .send(KeyhiveChangedRpcEvent { initial: true })
                        .await
                        .is_err()
                    {
                        return;
                    }
                    loop {
                        tokio::select! {
                            biased;
                            _ = cancel.cancelled() => break,
                            event = changes.recv() => {
                                match event {
                                    Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                        if tx
                                            .send(KeyhiveChangedRpcEvent { initial: false })
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                                }
                            }
                        }
                    }
                })
                .expect(ERROR_TOKIO);
            tracing::debug!(%peer_id, "started direct Keyhive change stream");
        }
    }
}

#[derive(Clone)]
pub struct IrohBigRepoRpcClient {
    client: irpc::Client<RepoSyncRpc>,
}

impl IrohBigRepoRpcClient {
    pub fn new(endpoint: iroh::Endpoint, endpoint_addr: iroh::EndpointAddr) -> Self {
        Self {
            client: irpc_iroh::client::<RepoSyncRpc>(endpoint, endpoint_addr, REPO_SYNC_ALPN),
        }
    }

    pub async fn subscribe_keyhive_changes(
        &self,
        capacity: usize,
    ) -> Res<irpc::channel::mpsc::Receiver<KeyhiveChangedRpcEvent>> {
        self.client
            .server_streaming(SubscribeKeyhiveChangesRequest, capacity)
            .await
            .wrap_err("failed subscribing to BigRepo Keyhive changes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, SharedPartStore, StorageConfig};
    use big_sync::MemoryPartStore;
    use iroh::protocol::Router;
    use std::net::Ipv4Addr;
    use tokio::time::timeout;

    #[test]
    fn rpc_peer_mapping_tracks_application_identity() {
        let endpoint_id = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let application_peer = PeerId::new([8; 32]);
        let mut map = RpcPeerMap::default();

        map.register(endpoint_id, application_peer);
        assert_eq!(map.lookup(endpoint_id), application_peer);

        map.unregister(application_peer);
        assert_eq!(
            map.lookup(endpoint_id),
            PeerId::new(*endpoint_id.as_bytes())
        );
    }

    async fn test_endpoint() -> Res<iroh::Endpoint> {
        Ok(iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?)
    }

    #[tokio::test]
    async fn keyhive_change_stream_delivers_ready_and_local_change() -> Res<()> {
        let (repo, repo_stop) = BigRepo::boot(Config {
            node_identity_seed: [41; 32],
            storage: StorageConfig::Memory,
            scope_key: Arc::from("rpc-ready"),
            hidden_parts: Default::default(),
        })
        .await?;
        let endpoint = test_endpoint().await?;
        let (rpc, rpc_stop) = spawn_repo_rpc(Arc::clone(&repo)).await?;
        let router = Router::builder(endpoint.clone())
            .accept(REPO_SYNC_ALPN, rpc.protocol_handler())
            .spawn();

        let client_endpoint = test_endpoint().await?;
        let client = IrohBigRepoRpcClient::new(client_endpoint.clone(), endpoint.addr());
        let mut events = client.subscribe_keyhive_changes(8).await?;
        let ready = timeout(Duration::from_secs(5), events.recv())
            .await
            .map_err(|_| ferr!("timed out waiting for RPC subscription readiness"))??
            .ok_or_eyre("RPC stream closed before readiness")?;
        assert!(ready.initial);

        repo.create_group_with_parents(Vec::new()).await?;
        let changed = timeout(Duration::from_secs(5), events.recv())
            .await
            .map_err(|_| ferr!("timed out waiting for Keyhive change RPC"))??
            .ok_or_eyre("RPC stream closed before Keyhive change")?;
        assert!(!changed.initial);

        drop(events);
        router.shutdown().await?;
        client_endpoint.close().await;
        endpoint.close().await;
        rpc_stop.stop().await?;
        repo_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn keyhive_change_stream_reconnects_after_client_disconnect() -> Res<()> {
        let (repo, repo_stop) = BigRepo::boot(Config {
            node_identity_seed: [42; 32],
            storage: StorageConfig::Memory,
            scope_key: Arc::from("rpc-reconnect"),
            hidden_parts: Default::default(),
        })
        .await?;
        let endpoint = test_endpoint().await?;
        let (rpc, rpc_stop) = spawn_repo_rpc(Arc::clone(&repo)).await?;
        let _router = Router::builder(endpoint.clone())
            .accept(REPO_SYNC_ALPN, rpc.protocol_handler())
            .spawn();

        // First subscription: consume initial=true
        let client_a = test_endpoint().await?;
        let sub_a = IrohBigRepoRpcClient::new(client_a.clone(), endpoint.addr());
        let mut events_a = sub_a.subscribe_keyhive_changes(8).await?;
        let ready = timeout(Duration::from_secs(5), events_a.recv())
            .await
            .map_err(|_| ferr!("timed out waiting for first readiness"))??
            .ok_or_eyre("first stream closed before readiness")?;
        assert!(ready.initial);

        // Disconnect the subscriber and drop the stream
        drop(events_a);
        client_a.close().await;

        // Second subscription from a fresh endpoint
        let client_b = test_endpoint().await?;
        let sub_b = IrohBigRepoRpcClient::new(client_b.clone(), endpoint.addr());
        let mut events_b = sub_b.subscribe_keyhive_changes(8).await?;
        let ready_b = timeout(Duration::from_secs(5), events_b.recv())
            .await
            .map_err(|_| ferr!("timed out waiting for second readiness"))??
            .ok_or_eyre("second stream closed before readiness")?;
        assert!(ready_b.initial);

        // Local Keyhive change should reach the second subscriber
        repo.create_group_with_parents(Vec::new()).await?;
        let changed = timeout(Duration::from_secs(5), events_b.recv())
            .await
            .map_err(|_| ferr!("timed out waiting for Keyhive change on second subscriber"))??
            .ok_or_eyre("second stream closed before Keyhive change")?;
        assert!(!changed.initial);

        drop(events_b);
        client_b.close().await;
        _router.shutdown().await?;
        endpoint.close().await;
        rpc_stop.stop().await?;
        repo_stop.stop().await?;
        Ok(())
    }
}
