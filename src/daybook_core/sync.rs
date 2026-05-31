use crate::interlude::*;

use std::str::FromStr;

use big_repo::BigRepo;
use big_sync::BackendId;
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    EndpointId,
};
use irpc::rpc::RemoteService;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::index::DocBlobsIndexRepo;
use crate::progress::ProgressRepo;
use crate::repo::RepoCtx;

mod bootstrap;
pub use bootstrap::*;
#[cfg(test)]
mod tests;

pub const SUBDUCTION_ALPN: &[u8] = b"subduction/0";
pub const IROH_CLONE_URL_SCHEME: &str = "db+iroh-clone";
pub const PARTITION_SYNC_ALPN: &[u8] = b"townframe/partition-sync/0";
pub const REPO_SYNC_ALPN: &[u8] = b"townframe/repo-sync/0";
pub const CLONE_PROVISION_ALPN: &[u8] = b"townframe/clone-provision/0";
pub const CORE_DOCS_PARTITION_ID: &str = "core.docs";
pub(crate) const BLOBS_BACKEND_ID: &str = "blobs";

pub type PeerKey = Arc<str>;

#[derive(Debug, Clone)]
struct SubductionProtocolHandler {
    big_repo: Arc<BigRepo>,
    incoming_conn_tx: mpsc::UnboundedSender<big_repo::BigRepoConnection>,
    end_signal_tx: mpsc::UnboundedSender<big_repo::ConnFinishSignal>,
}

impl ProtocolHandler for SubductionProtocolHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let conn = self
            .big_repo
            .accept_connection_iroh(conn, Some(self.end_signal_tx.clone()))
            .await
            .map_err(|err| AcceptError::from_boxed(err.into()))?;
        self.incoming_conn_tx.send(conn).ok();
        Ok(())
    }
}

#[derive(educe::Educe)]
#[educe(Debug)]
struct AuthenticatedIrohProtocol<S: RemoteService, K> {
    #[educe(Debug(ignore))]
    tx: mpsc::Sender<(K, S::Message)>,
    #[educe(Debug(ignore))]
    peer_key_fn: Arc<dyn Fn(iroh::EndpointId) -> K + Send + Sync>,
}

impl<S: RemoteService, K> Clone for AuthenticatedIrohProtocol<S, K> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            peer_key_fn: Arc::clone(&self.peer_key_fn),
        }
    }
}

impl<S, K> ProtocolHandler for AuthenticatedIrohProtocol<S, K>
where
    S: RemoteService + serde::de::DeserializeOwned + Send + 'static,
    K: Send + 'static + Clone,
{
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let peer_key = (self.peer_key_fn)(conn.remote_id());
        loop {
            let msg = match irpc_iroh::read_request::<S>(&conn).await {
                Ok(Some(msg)) => msg,
                Ok(None) => break,
                Err(err) => {
                    warn!(?err, "error reading request from authenticated connection");
                    break;
                }
            };
            if self.tx.send((peer_key.clone(), msg)).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

enum ActivePeerState {
    Connecting,
    Connected { peer_key: PeerKey },
}

pub struct IrohSyncRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    rcx: Arc<RepoCtx>,

    router: iroh::protocol::Router,

    config_repo: Arc<crate::config::ConfigRepo>,
    blobs_sync_backend: Arc<crate::blobs::sync::BlobSyncBackend>,
    _doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,

    conn_end_signal_tx: mpsc::UnboundedSender<big_repo::ConnFinishSignal>,
    active_peers: tokio::sync::RwLock<HashMap<PeerId, ActivePeerState>>,
    // sync_store: am_utils_rs::sync::store::SyncStoreHandle,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    big_sync_worker: big_sync::BigSyncWorkerHandle,
    _big_sync_rpc: big_sync::rpc::BigSyncRpcHandle,
    repo_sync_backend: Arc<big_repo::BigRepoSyncBackend>,
}

#[derive(Debug, Clone)]
pub enum IrohSyncEvent {
    IncomingConnection {
        peer_key: PeerKey,
    },
    OutgoingConnection {
        peer_key: PeerKey,
    },
    ConnectionClosed {
        peer_key: PeerKey,
        reason: String,
    },
    PeerFullySynced {
        peer_key: PeerKey,
        doc_count: usize,
    },
    PartitionFullySynced {
        peer_key: PeerKey,
        partition: String,
    },
    DocSyncedWithPeer {
        peer_key: PeerKey,
        doc_id: DocumentId,
    },
    BlobSynced {
        hash: String,
    },
    BlobDownloadStarted {
        hash: String,
    },
    BlobDownloadFinished {
        hash: String,
        success: bool,
    },
    BlobSyncBackoff {
        hash: String,
        delay: Duration,
        attempt_no: usize,
    },
    StalePeer {
        peer_key: PeerKey,
    },
}

pub struct IrohSyncRepoStopToken {
    cancel_token: CancellationToken,
    worker_handle: JoinHandle<()>,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    router: iroh::protocol::Router,
    // partition_sync_stop_token: am_utils_rs::sync::node::SyncNodeStopToken,
    big_repo_rpc_stop_token: big_repo::rpc::BigRepoRpcStopToken,
    big_sync_rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    big_sync_worker_stop: big_sync::StopToken,
    // partition_sync_store_stop_token: am_utils_rs::sync::store::SyncStoreStopToken,
}

impl IrohSyncRepoStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        let reconnect_handle = self.reconnect_task.lock().expect(ERROR_MUTEX).take();
        if let Some(handle) = reconnect_handle {
            utils_rs::wait_on_handle_with_timeout(
                handle,
                utils_rs::scale_timeout(Duration::from_secs(60)),
            )
            .await?;
        }
        // pre light the stop signal to the full worker
        // Worker shutdown drains active repo connections; each connection stop can wait up to 5s.
        utils_rs::wait_on_handle_with_timeout(
            self.worker_handle,
            utils_rs::scale_timeout(Duration::from_secs(10)),
        )
        .await?;
        self.big_sync_worker_stop.stop().await?;
        self.big_sync_rpc_stop.stop().await?;
        self.big_repo_rpc_stop_token.stop().await?;
        // NOTE: we only add timeouts for stop tokens that don't have internal
        // timeouts
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            self.router.shutdown(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout waiting for router shutdown"))??;
        Ok(())
    }
}

impl IrohSyncRepo {
    pub async fn boot(
        rcx: Arc<RepoCtx>,
        config_repo: Arc<crate::config::ConfigRepo>,
        blobs_repo: Arc<BlobsRepo>,
        doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
        progress_repo: Option<Arc<ProgressRepo>>,
    ) -> Res<(Arc<Self>, IrohSyncRepoStopToken)> {
        let endpoint_builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(rcx.iroh_secret_key.clone());
        #[cfg(test)]
        let endpoint_builder = endpoint_builder
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled);
        let endpoint = endpoint_builder.bind().await?;
        let blobs = blobs_repo.iroh_store();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let docs = iroh_docs::protocol::Docs::memory()
            .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
            .await
            .map_err(|err| ferr!("error booting iroh docs protocol: {err:?}"))?;
        let blobs_sync_backend = Arc::new(crate::blobs::sync::BlobSyncBackend::new(
            Arc::clone(&blobs_repo),
            Arc::clone(&rcx.part_store),
            endpoint.clone(),
        ));

        let cancel_token = CancellationToken::new();

        let (incoming_conn_tx, incoming_conn_rx) = mpsc::unbounded_channel();
        let (conn_end_tx, conn_end_rx) = mpsc::unbounded_channel();
        let (clone_rpc_tx, clone_rpc_rx) = mpsc::channel(128);

        let (big_repo_rpc, repo_rpc_stop_token) =
            big_repo::rpc::spawn_repo_rpc(Arc::clone(&rcx.big_repo)).await?;

        let repo_sync_backend = Arc::new(
            big_repo::BigRepoSyncBackend::boot(Arc::downgrade(&rcx.big_repo), endpoint.clone())
                .await
                .wrap_err("failed booting big repo sync backend")?,
        );
        let blob_sync_backend: Arc<dyn big_sync::SyncBackend> =
            Arc::clone(&blobs_sync_backend) as _;
        let mut sync_backends = std::collections::HashMap::new();
        sync_backends.insert(BLOBS_BACKEND_ID.into(), blob_sync_backend);
        sync_backends.insert(
            big_repo::BigRepo::BACKEND_ID.into(),
            Arc::clone(&repo_sync_backend) as _,
        );
        let (big_sync_worker, big_sync_worker_stop) =
            big_sync::spawn_big_sync_worker(Arc::clone(&rcx.part_store), sync_backends)?;

        let (big_sync_rpc, big_sync_rpc_stop) =
            big_sync::rpc::spawn_big_sync_rpc(Arc::clone(&rcx.part_store)).await?;

        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                SUBDUCTION_ALPN,
                SubductionProtocolHandler {
                    big_repo: Arc::clone(&rcx.big_repo),
                    incoming_conn_tx,
                    end_signal_tx: conn_end_tx.clone(),
                },
            )
            .accept(
                big_sync::rpc::BIG_SYNC_RPC_ALPN,
                irpc_iroh::IrohProtocol::<big_sync::rpc::BigSyncIrpc>::with_sender(
                    big_sync_rpc.local_sender(),
                ),
            )
            .accept(
                big_repo::rpc::REPO_SYNC_ALPN,
                AuthenticatedIrohProtocol::<big_repo::rpc::RepoSyncRpc, PeerId> {
                    tx: big_repo_rpc.local_sender(),
                    peer_key_fn: Arc::new(|endpoint_id| PeerId::new(*endpoint_id.as_bytes())),
                },
            )
            .accept(
                CLONE_PROVISION_ALPN,
                // NOTE: we don't use 0Rtt since CloneProvisionRpc requests are not idempotetnt
                // safe
                irpc_iroh::IrohProtocol::<bootstrap::CloneProvisionRpc>::with_sender(
                    clone_rpc_tx.clone(),
                ),
            )
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs, None),
            )
            .accept(iroh_docs::ALPN, docs.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        config_repo
            .ensure_local_sync_device(router.endpoint().id(), &rcx.local_device_name)
            .await?;

        let big_sync_rx = big_sync_worker.subscribe_stats();

        let reconnect_task = default();
        let repo = Arc::new(Self {
            rcx,
            router: router.clone(),
            config_repo,
            blobs_sync_backend,
            _doc_blobs_index_repo: doc_blobs_index_repo,
            progress_repo,
            cancel_token: cancel_token.clone(),
            registry: crate::repos::ListenersRegistry::new(),
            active_peers: default(),
            conn_end_signal_tx: conn_end_tx,
            reconnect_task: Arc::clone(&reconnect_task),
            big_sync_worker,
            repo_sync_backend,
            _big_sync_rpc: big_sync_rpc, // active_endpoint_ids: tokio::sync::RwLock::new(HashMap::new()),
        });
        #[cfg(test)]
        bootstrap::register_test_clone_rpc_sender(router.endpoint().id(), clone_rpc_tx.clone())
            .await;

        #[cfg(test)]
        let router_for_shutdown = router.clone();
        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            async move {
                let loop_res = repo
                    .machine_loop(big_sync_rx, clone_rpc_rx, incoming_conn_rx, conn_end_rx)
                    .await;
                #[cfg(test)]
                bootstrap::unregister_test_clone_rpc_sender(router_for_shutdown.endpoint().id())
                    .await;
                loop_res.unwrap();
            }
            .instrument(tracing::info_span!("IrohSyncRepo listen task"))
        });

        Ok((
            repo,
            IrohSyncRepoStopToken {
                cancel_token,
                worker_handle,
                reconnect_task,
                router,
                big_repo_rpc_stop_token: repo_rpc_stop_token,
                big_sync_rpc_stop,
                big_sync_worker_stop,
            },
        ))
    }

    fn ensure_repo_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is shutting down")
        }
        Ok(())
    }

    fn peer_partition_ids(&self, _peer_key: &str) -> HashMap<PartId, BackendId> {
        let repo_backend_id = big_repo::BigRepo::BACKEND_ID.into();
        let blob_backend_id = BLOBS_BACKEND_ID.into();
        [
            (
                crate::part_id_from_label(CORE_DOCS_PARTITION_ID),
                Arc::clone(&repo_backend_id),
            ),
            (
                crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(
                    &self.rcx.doc_drawer.document_id(),
                ),
                Arc::clone(&repo_backend_id),
            ),
            // (
            //     crate::part_id_from_label(crate::rt::PROCESSOR_RUNLOG_PARTITION_ID),
            //     repo_backend_id,
            // ),
            (
                crate::part_id_from_label(crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID),
                Arc::clone(&blob_backend_id),
            ),
            (
                crate::part_id_from_label(crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID),
                blob_backend_id,
            ),
        ]
        .into()
    }

    async fn spawn_connect_known_devices_once(self: &Arc<Self>, trigger: &'static str) {
        let Ok(mut reconnect_task) = self.reconnect_task.try_lock() else {
            // if locked, someone else has already qued an reconnect task or
            // or we're shutting down
            return;
        };
        if let Some(existing) = reconnect_task.as_ref() {
            if !existing.is_finished() {
                return;
            }
        }
        // NOTE: we just drop the old handle since we're using
        // a mutex which we shouldn't hold across await points
        // if let Some(done) = reconnect_task.take() {
        //     let _ = done.await;
        // }
        let repo = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let _ = repo
                .cancel_token
                .clone()
                .run_until_cancelled(async move {
                    if let Err(err) = repo.connect_known_devices_once().await {
                        if !repo.cancel_token.is_cancelled() {
                            warn!(?err, trigger, "known-device reconnect failed");
                        }
                    }
                })
                .await;
        });
        *reconnect_task = Some(handle);
    }

    async fn machine_loop(
        self: &Arc<Self>,
        mut big_sync_rx: tokio::sync::broadcast::Receiver<big_sync_core::SyncStatEvent>,
        mut clone_rpc_rx: mpsc::Receiver<bootstrap::CloneProvisionRpcMessage>,
        mut incoming_conn_rx: mpsc::UnboundedReceiver<big_repo::BigRepoConnection>,
        mut conn_end_rx: mpsc::UnboundedReceiver<big_repo::ConnFinishSignal>,
    ) -> Res<()> {
        use crate::repos::Repo;

        let mut config_listener = self
            .config_repo
            .subscribe(crate::repos::SubscribeOpts { capacity: 64 });
        let mut reconnect_tick = tokio::time::interval(Duration::from_secs(15));
        reconnect_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        self.spawn_connect_known_devices_once("initial").await;

        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    debug!("cancel token lit");
                    break;
                }
                val = clone_rpc_rx.recv() => {
                    let msg = val.ok_or_eyre("clone rpc is down")?;
                    use irpc::WithChannels;
                    match msg {
                        bootstrap::CloneProvisionRpcMessage::ResolveCloneInfo(req) => {
                            let WithChannels { inner, tx, .. } = req;
                            let out = self.handle_resolve_clone_info(inner.req).await;
                            tx.send(out.map_err(|err| format!("{err:#}")))
                                .await
                                .inspect_err(|_| warn!(ERROR_CALLER))
                                .ok();
                        }
                        bootstrap::CloneProvisionRpcMessage::RequestCloneProvision(req) => {
                            let WithChannels { inner, tx, .. } = req;
                            let out = self.handle_request_clone_provision(inner.req).await;
                            tx.send(out.map_err(|err| format!("{err:#}")))
                                .await
                                .inspect_err(|_| warn!(ERROR_CALLER))
                                .ok();
                        }
                    }
                }
                val = big_sync_rx.recv() => {
                    match val {
                        Ok(event) => {
                            self.handle_big_sync_evt(event).await?;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(?skipped, "sync observer lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            if !self.cancel_token.is_cancelled() {
                                error!("full sync worer is down");
                            }
                            break;
                        }
                    }
                }
                val = incoming_conn_rx.recv() => {
                    let conn = val.ok_or_eyre("iroh protcol is down")?;
                    self.handle_incoming_big_repo_conn(conn).await?;
                }
                val = conn_end_rx.recv() => {
                    let signal = val.expect("impossible actually");
                    self.handle_big_repo_conn_end(signal).await?;
                }
                _ = reconnect_tick.tick() => {
                    self.spawn_connect_known_devices_once("periodic").await;
                }
                val = config_listener.recv_async() => {
                    match val {
                        Ok(event) => {
                            if matches!(
                                &*event,
                                crate::config::ConfigEvent::SyncDevicesChanged { .. }
                            ) {
                                self.spawn_connect_known_devices_once("config-change").await;
                            }
                        }
                        Err(crate::repos::RecvError::Closed) => {
                            warn!("config listener closed; re-subscribing");
                            config_listener = self
                                .config_repo
                                .subscribe(crate::repos::SubscribeOpts { capacity: 64 });
                        }
                        Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                            warn!(dropped_count, "config listener dropped events");
                        }
                    }
                }
            }
        }
        // cleanup
        let active_peers = self
            .active_peers
            .read()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for peer_id in active_peers {
            self.repo_sync_backend.unregister_remote_peer(peer_id);
            self.blobs_sync_backend.unregister_remote_peer(peer_id);
            self.big_sync_worker.remove_peer(peer_id).await.ok();
        }
        self.active_peers.write().await.clear();
        eyre::Ok(())
    }
    async fn handle_incoming_big_repo_conn(&self, conn: big_repo::BigRepoConnection) -> Res<()> {
        {
            let mut active_peers = self.active_peers.write().await;
            if active_peers.contains_key(&conn.peer_id) {
                panic!("curiosity trap: duplicate incoming connection? how did we get here?");
            }
            active_peers.insert(conn.peer_id, ActivePeerState::Connecting);
        }
        let peer_id = conn.peer_id;
        let res = async {
            let peer_key = daybook_types::doc::format_peer_key(conn.peer_id.as_bytes());
            let events = [IrohSyncEvent::IncomingConnection {
                peer_key: Arc::clone(&peer_key),
            }];
            let partition_ids = self.peer_partition_ids(&peer_key);
            let endpoint = self.router.endpoint().clone();
            let remote_info = endpoint
                .remote_info(
                    EndpointId::from_bytes(conn.peer_id.as_bytes()).expect(ERROR_IMPOSSIBLE),
                )
                .await
                .ok_or_eyre("unable to get remote info for incoming conn")?;
            let addr = iroh::EndpointAddr::from_parts(
                remote_info.id(),
                remote_info.into_addrs().map(|info| info.into_addr()),
            );
            let big_sync_rpc_client =
                big_sync::rpc::IrohBigSyncRpcClient::new(endpoint, addr.clone());
            let big_sync_rpc_client = Arc::new(big_sync_rpc_client);

            self.blobs_sync_backend
                .register_remote_peer(conn.peer_id, addr.clone());
            self.repo_sync_backend
                .register_remote_peer(conn.peer_id, addr.clone());
            self.big_sync_worker
                .set_peer(conn.peer_id, big_sync_rpc_client, partition_ids)
                .await?;

            let old = self
                .active_peers
                .write()
                .await
                .insert(peer_id, ActivePeerState::Connected { peer_key });
            assert!(matches!(old, Some(ActivePeerState::Connecting)), "fishy");

            self.registry.notify(events);
            eyre::Ok(())
        }
        .await;
        if res.is_err() {
            let old = self.active_peers.write().await.remove(&peer_id);
            assert!(matches!(old, Some(ActivePeerState::Connecting)), "fishy")
        }

        Ok(())
    }

    async fn handle_big_repo_conn_end(
        self: &Arc<Self>,
        signal: big_repo::ConnFinishSignal,
    ) -> Res<()> {
        self.repo_sync_backend
            .unregister_remote_peer(signal.peer_id);
        self.blobs_sync_backend
            .unregister_remote_peer(signal.peer_id);
        self.big_sync_worker.remove_peer(signal.peer_id).await?;
        let Some(ActivePeerState::Connected { peer_key }) =
            self.active_peers.write().await.remove(&signal.peer_id)
        else {
            eyre::bail!("unkown connection disconnected");
        };
        let events = [IrohSyncEvent::ConnectionClosed {
            peer_key,
            reason: signal
                .err
                .map(|err| format!("conn error: {err}"))
                .unwrap_or_else(|| "natural disconnect".into()),
        }];

        self.registry.notify(events);
        if self.cancel_token.is_cancelled() {
            return Ok(());
        }
        self.spawn_connect_known_devices_once("connection-close")
            .await;
        Ok(())
    }

    async fn handle_resolve_clone_info(
        &self,
        req: bootstrap::CloneInfoRequest,
    ) -> Res<bootstrap::CloneInfoResponse> {
        let _ = req;
        Ok(bootstrap::CloneInfoResponse {
            repo_name: self.rcx.repo_name.clone(),
            device_name: Some(self.rcx.local_device_name.clone()),
        })
    }

    async fn handle_request_clone_provision(
        &self,
        req: bootstrap::RequestCloneProvisionReq,
    ) -> Res<bootstrap::CloneProvisionResponse> {
        let endpoint_id = iroh::PublicKey::from_str(&req.requester_endpoint_id)
            .wrap_err("invalid requester_endpoint_id in clone provision request")?;
        let _requester_peer_key = daybook_types::doc::format_peer_key(endpoint_id.as_bytes());
        // self.sync_store.allow_peer(requester_peer_key).await?;
        let endpoint_addr = self.router.endpoint().addr();
        let device_name = req
            .requested_device_name
            .unwrap_or_else(|| format!("clone-{}", endpoint_id));
        Ok(bootstrap::CloneProvisionResponse {
            endpoint_addr,
            repo_id: self.rcx.repo_id.clone(),
            repo_name: self.rcx.repo_name.clone(),
            app_doc_id: self.rcx.doc_app.document_id().to_string(),
            drawer_doc_id: self.rcx.doc_drawer.document_id().to_string(),
            device_name: Some(device_name),
        })
    }

    /// Allow a peer to connect to this node by their endpoint ID.
    /// Useful for test setups where nodes need to connect in a mesh topology.
    #[cfg(test)]
    pub async fn allow_peer_by_endpoint_id(&self, endpoint_id: EndpointId) -> Res<()> {
        let _peer_key = daybook_types::doc::format_peer_key(endpoint_id.as_bytes());
        // self.sync_store.allow_peer(peer_key).await
        Ok(())
    }

    async fn reserve_endpoint_connection(&self, peer_id: PeerId) -> bool {
        let mut active_peers = self.active_peers.write().await;
        if active_peers.contains_key(&peer_id) {
            return false;
        }
        active_peers.insert(peer_id, ActivePeerState::Connecting);
        true
    }

    async fn handle_big_sync_evt(&self, evt: big_sync_core::SyncStatEvent) -> Res<()> {
        match evt {
            big_sync_core::SyncStatEvent::ObjectSynced { peer_id, obj_id } => {
                self.registry.notify([IrohSyncEvent::DocSyncedWithPeer {
                    peer_key: daybook_types::doc::format_peer_key(peer_id.as_bytes()),
                    doc_id: obj_id,
                }]);
            }
            big_sync_core::SyncStatEvent::PeerPartFullySynced { peer_id, part_id } => {
                self.registry.notify([IrohSyncEvent::PartitionFullySynced {
                    peer_key: daybook_types::doc::format_peer_key(peer_id.as_bytes()),
                    partition: part_id.to_string(),
                }]);
            }
            big_sync_core::SyncStatEvent::PeerPartStale { .. } => {}
            big_sync_core::SyncStatEvent::PartFullySynced { .. } => {}
            big_sync_core::SyncStatEvent::PartStale { .. } => {}
            big_sync_core::SyncStatEvent::PeerFullySynced { .. } => {}
            big_sync_core::SyncStatEvent::PeerStale { peer_id } => {
                self.registry.notify([IrohSyncEvent::StalePeer {
                    peer_key: daybook_types::doc::format_peer_key(peer_id.as_bytes()),
                }]);
            }
            big_sync_core::SyncStatEvent::FullSyncWaiterSatisfied { .. } => {}
        }
        Ok(())
    }

    pub async fn connect_endpoint_addr(&self, endpoint_addr: iroh::EndpointAddr) -> Res<()> {
        self.ensure_repo_live()?;

        if endpoint_addr.id == self.router.endpoint().id() {
            eyre::bail!("connecting to ourself is not supported");
        }
        let endpoint_id = endpoint_addr.id;
        let peer_id = PeerId::new(*endpoint_id.as_bytes());

        let endpoint = self.router.endpoint().clone();

        if !self.reserve_endpoint_connection(peer_id).await {
            return Ok(());
        }
        let res = async {
            let peer_key = daybook_types::doc::format_peer_key(endpoint_id.as_bytes());
            let events = [IrohSyncEvent::OutgoingConnection {
                peer_key: Arc::clone(&peer_key),
            }];

            let partition_ids = self.peer_partition_ids(&peer_key);
            let conn = self
                .rcx
                .big_repo
                .open_connection_iroh(
                    self.router.endpoint().clone(),
                    endpoint_addr.clone(),
                    peer_id,
                    Some(self.conn_end_signal_tx.clone()),
                )
                .await?;
            let big_sync_rpc_client =
                big_sync::rpc::IrohBigSyncRpcClient::new(endpoint, endpoint_addr.clone());
            let big_sync_rpc_client = Arc::new(big_sync_rpc_client);

            self.blobs_sync_backend
                .register_remote_peer(conn.peer_id, endpoint_addr.clone());
            self.repo_sync_backend
                .register_remote_peer(conn.peer_id, endpoint_addr.clone());
            self.big_sync_worker
                .set_peer(conn.peer_id, big_sync_rpc_client, partition_ids)
                .await?;

            let old = self
                .active_peers
                .write()
                .await
                .insert(peer_id, ActivePeerState::Connected { peer_key });
            assert!(matches!(old, Some(ActivePeerState::Connecting)), "fishy");
            self.registry.notify(events);
            eyre::Ok(())
        }
        .await;
        if res.is_err() {
            let old = self.active_peers.write().await.remove(&peer_id);
            assert!(matches!(old, Some(ActivePeerState::Connecting)), "fishy")
        }

        Ok(())
    }

    pub async fn connect_url(&self, source_url: &str) -> Res<iroh::EndpointAddr> {
        self.ensure_repo_live()?;
        let endpoint_addr = bootstrap::parse_clone_endpoint_addr(source_url)?;
        self.connect_endpoint_addr(endpoint_addr.clone()).await?;
        Ok(endpoint_addr)
    }

    pub async fn connect_known_devices_once(&self) -> Res<()> {
        self.ensure_repo_live()?;
        #[cfg(not(test))]
        {
            let devices = self.config_repo.list_known_sync_devices().await?;
            let local_endpoint_id = self.router.endpoint().id();
            for device in devices {
                if device.endpoint_id == local_endpoint_id {
                    continue;
                }
                if let Err(err) = self
                    .connect_endpoint_addr(iroh::EndpointAddr::new(device.endpoint_id))
                    .await
                {
                    warn!(
                        ?err,
                        endpoint_id = %device.endpoint_id,
                        "failed reconnect attempt for known sync device"
                    );
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn wait_for_full_sync(
        &self,
        peer_ids: &[PeerId],
        required_partitions: &[PartId],
        timeout: Duration,
    ) -> Res<()> {
        self.ensure_repo_live()?;
        let Some(_progress_repo) = self.progress_repo.clone() else {
            eyre::bail!("wait_for_full_sync requires a progress-enabled IrohSyncRepo");
        };
        if peer_ids.is_empty() {
            return Ok(());
        }
        let timeout_outcome = tokio::time::timeout(timeout, async {
            self.big_sync_worker
                .wait_for_full_sync(
                    peer_ids.iter().copied(),
                    required_partitions.iter().copied(),
                )
                .await?;
            eyre::Ok(())
        })
        .await;
        match timeout_outcome {
            Ok(out) => out?,
            Err(_) => {
                eyre::bail!("timed out waiting for full sync");
            }
        }
        Ok(())
    }

    pub async fn wait_until_peers_sync(&self, peer_ids: &[PeerId], timeout: Duration) -> Res<()> {
        let parts = self.peer_partition_ids("").into_keys().collect::<Vec<_>>();
        self.wait_for_full_sync(peer_ids, &parts, timeout).await
    }
}

impl crate::repos::Repo for IrohSyncRepo {
    type Event = IrohSyncEvent;

    fn registry(&self) -> &Arc<crate::repos::ListenersRegistry> {
        &self.registry
    }

    fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}
