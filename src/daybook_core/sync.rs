use crate::interlude::*;

use am_utils_rs::repo::PeerId;
use am_utils_rs::sync::protocol::{PartitionId, PeerKey};
use iroh::EndpointId;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::index::DocBlobsIndexRepo;
use crate::progress::ProgressRepo;
use crate::repo::RepoCtx;

mod bootstrap;
mod full;
pub use bootstrap::*;
pub const SUBDUCTION_ALPN: &[u8] = b"subduction/0";
pub const IROH_CLONE_URL_SCHEME: &str = "db+iroh-clone";
pub const PARTITION_SYNC_ALPN: &[u8] = b"townframe/partition-sync/0";
pub const REPO_SYNC_ALPN: &[u8] = b"townframe/repo-sync/0";
pub const CLONE_PROVISION_ALPN: &[u8] = b"townframe/clone-provision/0";
pub const CORE_DOCS_PARTITION_ID: &str = "core.docs";

#[derive(Debug, Clone)]
struct SubductionProtocolHandler {
    big_repo: Arc<am_utils_rs::repo::BigRepo>,
    incoming_conn_tx: tokio::sync::mpsc::UnboundedSender<am_utils_rs::repo::BigRepoConnection>,
    end_signal_tx: tokio::sync::mpsc::UnboundedSender<am_utils_rs::repo::ConnFinishSignal>,
}

impl iroh::protocol::ProtocolHandler for SubductionProtocolHandler {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let conn = self
            .big_repo
            .accept_connection_iroh(conn, Some(self.end_signal_tx.clone()))
            .await
            .map_err(|err| iroh::protocol::AcceptError::from_boxed(err.into()))?;
        self.incoming_conn_tx.send(conn).ok();
        Ok(())
    }
}

struct AuthenticatedIrohProtocol<S: irpc::rpc::RemoteService> {
    tx: tokio::sync::mpsc::Sender<(am_utils_rs::sync::protocol::PeerKey, S::Message)>,
    peer_key_fn:
        Arc<dyn Fn(iroh::EndpointId) -> am_utils_rs::sync::protocol::PeerKey + Send + Sync>,
}

impl<S: irpc::rpc::RemoteService> std::fmt::Debug for AuthenticatedIrohProtocol<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticatedIrohProtocol")
            .field("tx", &self.tx)
            .finish_non_exhaustive()
    }
}

impl<S: irpc::rpc::RemoteService> Clone for AuthenticatedIrohProtocol<S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            peer_key_fn: Arc::clone(&self.peer_key_fn),
        }
    }
}

impl<S: irpc::rpc::RemoteService + serde::de::DeserializeOwned + Send + 'static>
    iroh::protocol::ProtocolHandler for AuthenticatedIrohProtocol<S>
{
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
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
    Connected {
        peer_key: am_utils_rs::sync::protocol::PeerKey,
    },
}

pub struct IrohSyncRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    rcx: Arc<RepoCtx>,

    router: iroh::protocol::Router,

    config_repo: Arc<crate::config::ConfigRepo>,
    _doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,

    conn_end_signal_tx: tokio::sync::mpsc::UnboundedSender<am_utils_rs::repo::ConnFinishSignal>,
    active_peers: tokio::sync::RwLock<HashMap<PeerId, ActivePeerState>>,
    full_sync_handle: full::WorkerHandle,
    sync_store: am_utils_rs::sync::store::SyncStoreHandle,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
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
        partition: String,
        hash: String,
    },
    BlobDownloadFinished {
        partition: String,
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
    full_stop_token: full::StopToken,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    router: iroh::protocol::Router,
    partition_sync_stop_token: am_utils_rs::sync::node::SyncNodeStopToken,
    repo_rpc_stop_token: am_utils_rs::repo::rpc::RepoRpcStopToken,
    partition_sync_store_stop_token: am_utils_rs::sync::store::SyncStoreStopToken,
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
        self.full_stop_token.stop().await?;
        // NOTE: we only add timeouts for stop tokens that don't have internal
        // timeouts
        tokio::time::timeout(
            utils_rs::scale_timeout(Duration::from_secs(10)),
            self.router.shutdown(),
        )
        .await
        .map_err(|_| eyre::eyre!("timeout for waiting router shutdown"))??;

        self.repo_rpc_stop_token.stop().await?;
        self.partition_sync_stop_token.stop().await?;

        self.partition_sync_store_stop_token.stop().await?;
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
        let endpoint_builder = iroh::Endpoint::builder().secret_key(rcx.iroh_secret_key.clone());
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

        let cancel_token = CancellationToken::new();

        let (incoming_conn_tx, incoming_conn_rx) = tokio::sync::mpsc::unbounded_channel();
        let (conn_end_tx, conn_end_rx) = tokio::sync::mpsc::unbounded_channel();
        let (clone_rpc_tx, clone_rpc_rx) = tokio::sync::mpsc::channel(128);

        let (partition_sync_store, partition_sync_store_stop_token) =
            am_utils_rs::sync::store::spawn_sync_store(rcx.sql.db_pool.clone()).await?;
        let (partition_sync_node, partition_sync_stop_token) =
            am_utils_rs::sync::node::spawn_sync_node(
                Arc::clone(&rcx.partition_store),
                partition_sync_store.clone(),
                Arc::new(am_utils_rs::sync::AllowAllPartitionAccessPolicy),
            )
            .await?;
        let partition_sync_node = Arc::new(partition_sync_node);
        let (repo_rpc, repo_rpc_stop_token) = am_utils_rs::repo::rpc::spawn_repo_rpc(
            Arc::clone(&rcx.big_repo),
            partition_sync_store.clone(),
            Arc::new(am_utils_rs::sync::AllowAllPartitionAccessPolicy),
        )
        .await?;

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
                PARTITION_SYNC_ALPN,
                AuthenticatedIrohProtocol::<am_utils_rs::sync::protocol::PartitionSyncRpc> {
                    tx: partition_sync_node.local_sender(),
                    peer_key_fn: Arc::new(|endpoint_id| {
                        daybook_types::doc::format_peer_key(endpoint_id.as_bytes())
                    }),
                },
            )
            .accept(
                REPO_SYNC_ALPN,
                AuthenticatedIrohProtocol::<am_utils_rs::repo::rpc::RepoSyncRpc> {
                    tx: repo_rpc.local_sender(),
                    peer_key_fn: Arc::new(|endpoint_id| {
                        daybook_types::doc::format_peer_key(endpoint_id.as_bytes())
                    }),
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

        let (mut full_sync_handle, full_stop_token) = full::spawn_full_sync_worker(
            Arc::clone(&rcx.big_repo),
            Arc::clone(&rcx.local_peer_key),
            Arc::clone(&blobs_repo),
            progress_repo.clone(),
            partition_sync_store.clone(),
            endpoint.clone(),
        )
        .await?;
        let full_sync_rx = full_sync_handle.events_rx.take().expect("impossible");

        let reconnect_task = default();
        let repo = Arc::new(Self {
            rcx,
            router: router.clone(),
            config_repo,
            _doc_blobs_index_repo: doc_blobs_index_repo,
            progress_repo,
            cancel_token: cancel_token.clone(),
            registry: crate::repos::ListenersRegistry::new(),
            active_peers: default(),
            conn_end_signal_tx: conn_end_tx,
            full_sync_handle,
            sync_store: partition_sync_store.clone(),
            reconnect_task: Arc::clone(&reconnect_task),
            // bootstrap_docs: tokio::sync::Mutex::new(Vec::new()),
            // active_endpoint_ids: tokio::sync::RwLock::new(HashMap::new()),
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
                    .machine_loop(full_sync_rx, clone_rpc_rx, incoming_conn_rx, conn_end_rx)
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
                full_stop_token,
                worker_handle,
                reconnect_task,
                router,
                partition_sync_stop_token,
                repo_rpc_stop_token,
                partition_sync_store_stop_token,
            },
        ))
    }

    fn ensure_repo_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is shutting down")
        }
        Ok(())
    }

    fn peer_partition_ids(&self, _peer_key: &str) -> HashSet<PartitionId> {
        [
            CORE_DOCS_PARTITION_ID.into(),
            crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(
                self.rcx.doc_drawer.document_id(),
            ),
            crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID.into(),
            crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.into(),
            crate::rt::PROCESSOR_RUNLOG_PARTITION_ID.into(),
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
            let _ = repo.cancel_token.clone().run_until_cancelled(async move {
                if let Err(err) = repo.connect_known_devices_once().await {
                    if !repo.cancel_token.is_cancelled() {
                        warn!(?err, trigger, "known-device reconnect failed");
                    }
                }
            });
        });
        *reconnect_task = Some(handle);
    }

    async fn machine_loop(
        self: &Arc<Self>,
        mut full_sync_rx: tokio::sync::broadcast::Receiver<full::FullSyncEvent>,
        mut clone_rpc_rx: tokio::sync::mpsc::Receiver<bootstrap::CloneProvisionRpcMessage>,
        mut incoming_conn_rx: tokio::sync::mpsc::UnboundedReceiver<
            am_utils_rs::repo::BigRepoConnection,
        >,
        mut conn_end_rx: tokio::sync::mpsc::UnboundedReceiver<am_utils_rs::repo::ConnFinishSignal>,
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
                val = full_sync_rx.recv() => {
                    match val {
                        Ok(event) => {
                            self.handle_full_sync_evt(event).await?;
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
            self.full_sync_handle.del_connection(peer_id).await.ok();
        }
        self.active_peers.write().await.clear();
        eyre::Ok(())
    }
    async fn handle_incoming_big_repo_conn(
        &self,
        conn: am_utils_rs::repo::BigRepoConnection,
    ) -> Res<()> {
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
            let remote_info = self
                .router
                .endpoint()
                .remote_info(conn.peer_id.into())
                .await
                .ok_or_eyre("unable to get remote info for incoming conn")?;
            let addr = iroh::EndpointAddr::from_parts(
                remote_info.id(),
                remote_info.into_addrs().map(|info| info.into_addr()),
            );
            self.full_sync_handle
                .set_connection(conn, addr, Arc::clone(&peer_key), partition_ids)
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
        signal: am_utils_rs::repo::ConnFinishSignal,
    ) -> Res<()> {
        self.full_sync_handle.del_connection(signal.peer_id).await?;
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
        let requester_peer_key = daybook_types::doc::format_peer_key(endpoint_id.as_bytes());
        self.sync_store.allow_peer(requester_peer_key).await?;
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
    pub async fn allow_peer_by_endpoint_id(&self, endpoint_id: EndpointId) -> Res<()> {
        let peer_key = daybook_types::doc::format_peer_key(endpoint_id.as_bytes());
        self.sync_store.allow_peer(peer_key).await
    }

    async fn reserve_endpoint_connection(&self, peer_id: PeerId) -> bool {
        let mut active_peers = self.active_peers.write().await;
        if active_peers.contains_key(&peer_id) {
            return false;
        }
        active_peers.insert(peer_id, ActivePeerState::Connecting);
        true
    }

    async fn handle_full_sync_evt(&self, evt: full::FullSyncEvent) -> Res<()> {
        match evt {
            full::FullSyncEvent::PeerFullSynced {
                peer_key,
                doc_count,
            } => self.registry.notify([IrohSyncEvent::PeerFullySynced {
                doc_count,
                peer_key,
            }]),
            full::FullSyncEvent::PartitionFullSynced {
                peer_key,
                partition,
            } => self.registry.notify([IrohSyncEvent::PartitionFullySynced {
                peer_key,
                partition: partition.as_tag_value().to_string(),
            }]),
            full::FullSyncEvent::DocSyncedWithPeer { peer_key, doc_id } => self
                .registry
                .notify([IrohSyncEvent::DocSyncedWithPeer { peer_key, doc_id }]),
            full::FullSyncEvent::BlobSynced { hash } => {
                self.registry.notify([IrohSyncEvent::BlobSynced { hash }])
            }
            full::FullSyncEvent::BlobDownloadStarted { partition, hash } => {
                self.registry.notify([IrohSyncEvent::BlobDownloadStarted {
                    partition: partition.as_tag_value().to_string(),
                    hash,
                }])
            }
            full::FullSyncEvent::BlobDownloadFinished {
                partition,
                hash,
                success,
            } => self.registry.notify([IrohSyncEvent::BlobDownloadFinished {
                partition: partition.as_tag_value().to_string(),
                hash,
                success,
            }]),
            full::FullSyncEvent::BlobSyncBackoff {
                hash,
                delay,
                attempt_no,
            } => self.registry.notify([IrohSyncEvent::BlobSyncBackoff {
                hash,
                delay,
                attempt_no,
            }]),
            full::FullSyncEvent::StalePeer { peer_key } => self
                .registry
                .notify([IrohSyncEvent::StalePeer { peer_key }]),
        }
        Ok(())
    }

    pub async fn connect_endpoint_addr(&self, endpoint_addr: iroh::EndpointAddr) -> Res<()> {
        self.ensure_repo_live()?;

        if endpoint_addr.id == self.router.endpoint().id() {
            eyre::bail!("connecting to ourself is not supported");
        }
        let endpoint_id = endpoint_addr.id;
        let peer_id = endpoint_id.into();

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
            self.full_sync_handle
                .set_connection(conn, endpoint_addr, Arc::clone(&peer_key), partition_ids)
                .await?;
            let old = self
                .active_peers
                .write()
                .await
                .insert(peer_id, ActivePeerState::Connected { peer_key: peer_key });
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

    pub async fn wait_for_full_sync(
        &self,
        endpoint_ids: &[EndpointId],
        required_partitions: &[am_utils_rs::sync::protocol::PartitionId],
        timeout: Duration,
    ) -> Res<()> {
        self.ensure_repo_live()?;
        let Some(progress_repo) = self.progress_repo.clone() else {
            eyre::bail!("wait_for_full_sync requires a progress-enabled IrohSyncRepo");
        };
        let target_peers: HashSet<EndpointId> = endpoint_ids.iter().cloned().collect();
        if target_peers.is_empty() {
            return Ok(());
        }
        let required_partitions: HashSet<full::PartitionKey> = required_partitions
            .iter()
            .cloned()
            .map(full::PartitionKey::from_partition_id)
            .collect();
        let peer_ids: Vec<am_utils_rs::ids::PeerId32> =
            endpoint_ids.iter().cloned().map(Into::into).collect();
        let initial_snapshot = self
            .full_sync_handle
            .get_peer_sync_snapshot(peer_ids.clone())
            .await?;
        debug!(
            target_peers = ?target_peers,
            required_partitions = ?required_partitions,
            initial_snapshot = ?initial_snapshot,
            "wait_for_full_sync initial state"
        );
        let timeout_outcome = tokio::time::timeout(timeout, async {
            self.full_sync_handle
                .wait_for_peers_fully_synced(peer_ids.clone(), required_partitions.clone())
                .await?;
            eyre::Ok(())
        })
        .await;
        match timeout_outcome {
            Ok(out) => out?,
            Err(_) => {
                let latest_snapshot = self
                    .full_sync_handle
                    .get_peer_sync_snapshot(peer_ids.clone())
                    .await
                    .map(|snapshot| format!("{snapshot:?}"))
                    .unwrap_or_else(|err| format!("snapshot_error={err:?}"));
                let remaining = self
                    .full_sync_handle
                    .get_peer_sync_snapshot(peer_ids.clone())
                    .await
                    .map(|snapshot| {
                        target_peers
                            .iter()
                            .filter(|endpoint_id| {
                                let peer_id: am_utils_rs::ids::PeerId32 = (**endpoint_id).into();
                                !snapshot.get(&peer_id).is_some_and(|peer| {
                                    required_partitions.iter().all(|partition| {
                                        peer.fully_synced_partitions.contains(partition)
                                    })
                                })
                            })
                            .copied()
                            .collect::<HashSet<_>>()
                    })
                    .unwrap_or_default();
                let connected_peers = self
                    .active_peers
                    .read()
                    .await
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                let tasks = progress_repo.list_by_tag_prefix("sync/full/").await;
                let diag = match tasks {
                    Ok(tasks) => {
                        let active = tasks
                            .iter()
                            .filter(|task| task.state == crate::progress::ProgressTaskState::Active)
                            .count();
                        let ids = tasks
                            .iter()
                            .map(|task| task.id.clone())
                            .take(16)
                            .collect::<Vec<_>>();
                        format!(
                            "full_sync_tasks_total={}; full_sync_tasks_active={active}; sample_task_ids={ids:?}; latest_snapshot={latest_snapshot}; connected_peers={connected_peers:?}",
                            tasks.len()
                        )
                    }
                    Err(err) => format!(
                        "failed_listing_full_sync_tasks={err:?}; latest_snapshot={latest_snapshot}; connected_peers={connected_peers:?}"
                    ),
                };
                eyre::bail!("timed out waiting for full sync: remaining={remaining:?}; {diag}");
            }
        }
        Ok(())
    }

    pub async fn wait_until_peers_sync(&self, endpoint_ids: &[EndpointId]) -> Res<()> {
        let required_partitions = self.peer_partition_ids("").into_iter().collect::<Vec<_>>();
        self.wait_for_full_sync(endpoint_ids, &required_partitions, Duration::from_secs(30))
            .await
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

#[cfg(test)]
mod tests {
    use super::*;
    mod ladder;
    mod stress;

    use crate::blobs::BlobsRepo;
    use crate::drawer::DrawerRepo;
    use crate::index::DocBlobsIndexRepo;
    use crate::local_state::SqliteLocalStateRepo;
    use crate::plugs::PlugsRepo;
    use crate::progress::ProgressRepo;
    use crate::repo::{RepoCtx, RepoOpenOptions};
    use crate::repos::{Repo, SubscribeOpts};
    use daybook_types::doc::{
        AddDocArgs, DocId, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag,
    };
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    struct SyncTestNode {
        ctx: Arc<RepoCtx>,
        blobs_repo: Arc<BlobsRepo>,
        drawer: Arc<DrawerRepo>,
        progress_repo: Arc<ProgressRepo>,
        progress_stop: crate::repos::RepoStopToken,
        drawer_stop: crate::repos::RepoStopToken,
        _plugs_repo: Arc<PlugsRepo>,
        plugs_stop: crate::repos::RepoStopToken,
        config_stop: crate::repos::RepoStopToken,
        doc_blobs_index_stop: crate::repos::RepoStopToken,
        sqlite_local_state_stop: crate::repos::RepoStopToken,
        doc_blobs_bridge_cancel: CancellationToken,
        doc_blobs_bridge_handle: Option<JoinHandle<()>>,
        sync_repo: Arc<IrohSyncRepo>,
        sync_stop: IrohSyncRepoStopToken,
    }

    impl SyncTestNode {
        async fn stop(self) -> Res<()> {
            let SyncTestNode {
                ctx,
                blobs_repo: _blobs_repo,
                drawer: _drawer,
                progress_repo: _progress_repo,
                progress_stop,
                drawer_stop,
                _plugs_repo,
                plugs_stop,
                config_stop,
                doc_blobs_index_stop,
                sqlite_local_state_stop,
                doc_blobs_bridge_cancel,
                doc_blobs_bridge_handle,
                sync_repo,
                sync_stop,
            } = self;
            // NOTE: do early cancellation before waiting on actual stops.
            sync_stop.cancel_token.cancel();
            doc_blobs_bridge_cancel.cancel();
            doc_blobs_index_stop.cancel_token.cancel();
            sqlite_local_state_stop.cancel_token.cancel();
            config_stop.cancel_token.cancel();
            drawer_stop.cancel_token.cancel();
            plugs_stop.cancel_token.cancel();

            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(30)),
                sync_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting sync stop"))??;
            drop(sync_repo);
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                progress_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting progress stop"))??;
            if let Some(handle) = doc_blobs_bridge_handle {
                tokio::time::timeout(
                    utils_rs::scale_timeout(Duration::from_secs(5)),
                    utils_rs::wait_on_handle_with_timeout(
                        handle,
                        utils_rs::scale_timeout(Duration::from_secs(2)),
                    ),
                )
                .await
                .map_err(|_| eyre::eyre!("timeout waiting doc blobs bridge join"))??;
            }
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                doc_blobs_index_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting doc blobs index stop"))??;
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                sqlite_local_state_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting sqlite local state stop"))??;
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                config_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting config stop"))??;
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                drawer_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting drawer stop"))??;
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                plugs_stop.stop(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting plugs stop"))??;
            tokio::time::timeout(
                utils_rs::scale_timeout(Duration::from_secs(10)),
                ctx.shutdown(),
            )
            .await
            .map_err(|_| eyre::eyre!("timeout waiting ctx shutdown"))??;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_sync_between_copied_repos() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let mut created_doc_ids = Vec::new();
        for _ in 0..3 {
            let new_doc_id = node_a
                .drawer
                .add(daybook_types::doc::AddDocArgs {
                    branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                    facets: default(),
                    user_path: Some(daybook_types::doc::UserPathBuf::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
            created_doc_ids.push(new_doc_id);
        }

        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20))
            .await?;
        for doc_id in &created_doc_ids {
            wait_for_doc_presence_with_activity(&node_b, doc_id, Duration::from_secs(60)).await?;
        }

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(
            ids_a, ids_b,
            "replica doc sets are not equal after full sync"
        );

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_live_sync_bidirectional_after_clone() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20))
            .await?;

        let doc_on_a = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;

        let doc_on_b = node_b
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_b.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_a, &doc_on_b, Duration::from_secs(60)).await?;

        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(20)).await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(ids_a, ids_b, "live sync did not converge to equal doc sets");

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_live_sync_propagates_repeated_doc_updates() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let device_name = "test-device".to_string();
        let rtx = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions {},
            device_name.clone(),
            device_name,
        )
        .await?;
        rtx.shutdown().await?;

        let seed_node = open_sync_node(&repo_a_path).await?;
        let ticket = seed_node.sync_repo.get_clone_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&ticket, &repo_b_path).await?;
        seed_node.stop().await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20))
            .await?;

        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;

        for idx in 0..6 {
            let branch = daybook_types::doc::BranchPathBuf::from("main");
            let Some((_doc, heads)) = node_a.drawer.get_with_heads(&doc_id, &branch, None).await?
            else {
                eyre::bail!("missing source doc after initial sync: {doc_id}");
            };
            let mut facets_set = std::collections::HashMap::new();
            facets_set.insert(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                FacetRaw::from(WellKnownFacet::TitleGeneric(format!("repeat-{idx}"))),
            );
            node_a
                .drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.clone(),
                        facets_set,
                        facets_remove: vec![],
                        user_path: Some(daybook_types::doc::UserPathBuf::from(
                            node_a.ctx.local_user_path.clone(),
                        )),
                    },
                    &branch,
                    Some(heads),
                )
                .await?;
        }

        wait_for_doc_head_parity(
            &node_a,
            &node_b,
            &doc_id,
            &daybook_types::doc::BranchPathBuf::from("main"),
            Duration::from_secs(30),
        )
        .await?;

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cloned_repo_registers_core_docs_partition_on_open() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let device_name = "test-device".to_string();
        let rtx = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions {},
            device_name.clone(),
            device_name,
        )
        .await?;
        rtx.shutdown().await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let created_doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_a, &created_doc_id, Duration::from_secs(30))
            .await?;
        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;
        node_a.stop().await?;

        let node_b = open_sync_node(&repo_b_path).await?;
        let partitions = node_b
            .ctx
            .partition_store
            .list_partitions()
            .await?
            .partitions;
        let core_partition = partitions
            .iter()
            .find(|summary| &summary.partition_id[..] == CORE_DOCS_PARTITION_ID);
        assert!(
            core_partition.is_some(),
            "cloned repo should register core docs partition on open: {partitions:?}"
        );
        let core_partition = core_partition.expect("checked above");
        assert!(
            core_partition.member_count >= 2,
            "core docs partition should include drawer/app docs after sync boot: {core_partition:?}"
        );

        node_b.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_ticket_in_tests_omits_relay_addresses() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_path = temp_root.path().join("repo-a");
        tokio::fs::create_dir_all(&repo_path).await?;

        let device_name = "test-device".to_string();
        let rtx = RepoCtx::init(
            &repo_path,
            RepoOpenOptions {},
            device_name.clone(),
            device_name,
        )
        .await?;
        rtx.shutdown().await?;

        let node = open_sync_node(&repo_path).await?;
        let ticket = node.sync_repo.get_clone_ticket_url().await?;
        let info = crate::sync::resolve_clone_info_from_url(&ticket).await?;
        assert!(!info.repo_name.is_empty());
        node.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_clone_sync_batch_100_docs_with_blobs() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let mut args_batch = Vec::new();
        for idx in 0..100usize {
            let payload = format!("blob-payload-{idx:03}").into_bytes();
            let hash = node_a.blobs_repo.put(&payload).await?;
            args_batch.push(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: payload.len() as u64,
                        digest: hash.clone(),
                        inline: None,
                        urls: Some(vec![format!("db+blob:///{hash}")]),
                    })),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            });
        }
        let created = node_a.drawer.batch_add(args_batch).await?;
        assert_eq!(created.len(), 100);

        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(20))
            .await?;
        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(
            ids_a, ids_b,
            "doc sets are not equal after 100-doc clone sync"
        );

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_blob_sync_validates_bytes() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let mut blob_payloads = Vec::new();
        let mut args_batch = Vec::new();
        for idx in 0..8usize {
            let payload = format!("blob-bytes-validation-{idx:03}").into_bytes();
            let hash = node_a.blobs_repo.put(&payload).await?;
            blob_payloads.push((hash.clone(), payload));
            args_batch.push(AddDocArgs {
                branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Blob),
                    FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: blob_payloads.last().expect("just pushed").1.len() as u64,
                        digest: hash.clone(),
                        inline: None,
                        urls: Some(vec![format!("db+blob:///{hash}")]),
                    })),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            });
        }
        node_a.drawer.batch_add(args_batch).await?;

        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(60))
            .await?;

        for (hash, expected) in &blob_payloads {
            let got =
                wait_for_blob_bytes(&node_b.blobs_repo, hash, Duration::from_secs(60)).await?;
            assert_eq!(
                got, *expected,
                "blob content mismatch after sync for hash={hash}"
            );
        }

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_sync_after_bootstrap_clone_converges() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let device_name = "test-device".to_string();
        let rtx = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions {},
            device_name.clone(),
            device_name,
        )
        .await?;
        rtx.shutdown().await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let mut created_doc_ids = Vec::new();
        for _ in 0..8 {
            let new_doc_id = node_a
                .drawer
                .add(daybook_types::doc::AddDocArgs {
                    branch_path: daybook_types::doc::BranchPathBuf::from("main"),
                    facets: default(),
                    user_path: Some(daybook_types::doc::UserPathBuf::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
            created_doc_ids.push(new_doc_id);
        }

        let sync_url = node_a.sync_repo.get_clone_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;

        let node_b = open_sync_node(&repo_b_path).await?;
        let endpoint_addr = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_addr.id, Duration::from_secs(30))
            .await?;

        for doc_id in &created_doc_ids {
            wait_for_doc_presence_with_activity(&node_b, doc_id, Duration::from_secs(60)).await?;
        }

        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(30)).await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(
            ids_a, ids_b,
            "sync after bootstrap clone did not converge to equal doc sets"
        );

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    async fn init_and_copy_repo_pair(
        repo_a_path: &std::path::Path,
        repo_b_path: &std::path::Path,
    ) -> Res<()> {
        tokio::fs::create_dir_all(repo_a_path).await?;
        let device_name = "test-device".to_string();
        let rtx = RepoCtx::init(
            repo_a_path,
            RepoOpenOptions {},
            device_name.clone(),
            device_name,
        )
        .await?;
        let source_repo_id = rtx.repo_id.clone();
        let source_app_doc_id = rtx.doc_app.document_id().clone();
        let source_drawer_doc_id = rtx.doc_drawer.document_id().clone();
        rtx.shutdown().await?;

        let seed_node = open_sync_node(repo_a_path).await?;
        let result = async {
            let ticket = seed_node.sync_repo.get_clone_ticket_url().await?;
            bootstrap_clone_repo_from_url_for_tests(&ticket, repo_b_path).await?;

            let ctx = RepoCtx::open(repo_b_path, RepoOpenOptions {}, "test-device".into()).await?;
            if ctx.repo_id != source_repo_id {
                eyre::bail!(
                    "init repo_id mismatch after clone (source={}, cloned={})",
                    source_repo_id,
                    ctx.repo_id
                );
            }
            if ctx.doc_app.document_id() != &source_app_doc_id {
                eyre::bail!(
                    "init app doc mismatch after clone (source={}, cloned={})",
                    source_app_doc_id,
                    ctx.doc_app.document_id()
                );
            }
            if ctx.doc_drawer.document_id() != &source_drawer_doc_id {
                eyre::bail!(
                    "init drawer doc mismatch after clone (source={}, cloned={})",
                    source_drawer_doc_id,
                    ctx.doc_drawer.document_id()
                );
            }
            ctx.shutdown().await
        }
        .await;
        seed_node.stop().await?;
        result
    }

    async fn bootstrap_clone_repo_from_url_for_tests(
        source_url: &str,
        destination: &std::path::Path,
    ) -> Res<()> {
        crate::sync::clone_repo_init_from_url(
            source_url,
            destination,
            crate::sync::CloneRepoInitOptions {
                timeout: Duration::from_secs(30),
            },
        )
        .await?;
        Ok(())
    }

    async fn open_sync_node(repo_root: &std::path::Path) -> Res<SyncTestNode> {
        let rtx = RepoCtx::open(repo_root, RepoOpenOptions {}, "test-device".into()).await?;
        let blobs_repo = BlobsRepo::new(
            rtx.layout.blobs_root.clone(),
            rtx.local_user_path.clone(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                Arc::clone(&rtx.partition_store),
            )),
        )
        .await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            Arc::clone(&rtx.big_repo),
            Arc::clone(&blobs_repo),
            rtx.doc_app.document_id().clone(),
            daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
        )
        .await?;
        let (drawer_repo, drawer_stop) = DrawerRepo::load(
            Arc::clone(&rtx.big_repo),
            Arc::clone(&rtx.partition_store),
            rtx.doc_drawer.document_id().clone(),
            daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
            rtx.sql.clone(),
            rtx.layout.repo_root.join("local_state"),
            Arc::new(surelock::mutex::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(surelock::mutex::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            Some(Arc::clone(&plugs_repo)),
        )
        .await?;
        let (config_repo, config_stop) = crate::config::ConfigRepo::load(
            Arc::clone(&rtx.big_repo),
            rtx.doc_app.document_id().clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPathBuf::from(rtx.local_user_path.clone()),
            rtx.sql.clone(),
        )
        .await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            SqliteLocalStateRepo::boot(rtx.layout.repo_root.join("local_state")).await?;
        let (doc_blobs_index_repo, doc_blobs_index_stop) = DocBlobsIndexRepo::boot(
            Arc::clone(&drawer_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&sqlite_local_state_repo),
        )
        .await?;
        let (doc_blobs_bridge_cancel, doc_blobs_bridge_handle) =
            spawn_doc_blobs_index_bridge_for_tests(
                Arc::clone(&drawer_repo),
                Arc::clone(&doc_blobs_index_repo),
            );
        let (progress_repo, progress_stop) = ProgressRepo::boot(rtx.sql.clone()).await?;
        let (sync_repo, sync_stop) = IrohSyncRepo::boot(
            Arc::clone(&rtx),
            Arc::clone(&config_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&doc_blobs_index_repo),
            Some(Arc::clone(&progress_repo)),
        )
        .await?;

        Ok(SyncTestNode {
            ctx: rtx,
            blobs_repo,
            drawer: drawer_repo,
            progress_repo,
            progress_stop,
            drawer_stop,
            _plugs_repo: plugs_repo,
            plugs_stop,
            config_stop,
            doc_blobs_index_stop,
            sqlite_local_state_stop,
            doc_blobs_bridge_cancel,
            doc_blobs_bridge_handle: Some(doc_blobs_bridge_handle),
            sync_repo,
            sync_stop,
        })
    }

    fn spawn_doc_blobs_index_bridge_for_tests(
        drawer_repo: Arc<DrawerRepo>,
        doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    ) -> (CancellationToken, JoinHandle<()>) {
        let drawer_listener = drawer_repo.subscribe(SubscribeOpts::new(16_384));
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_for_task.cancelled() => break,
                    evt = drawer_listener.recv_async() => {
                        match evt {
                            Ok(evt) => match evt.as_ref() {
                                crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                                    doc_blobs_index_repo.enqueue_delete(id.clone()).unwrap_or_log();
                                }
                                crate::drawer::DrawerEvent::DocAdded { id, entry, .. } => {
                                    for (branch_name, heads) in &entry.branches {
                                        doc_blobs_index_repo
                                            .enqueue_upsert(
                                                id.clone(),
                                                daybook_types::doc::BranchPathBuf::from(
                                                    branch_name.as_str(),
                                                ),
                                                heads.clone(),
                                            )
                                            .unwrap_or_log();
                                    }
                                }
                                crate::drawer::DrawerEvent::DocUpdated { id, entry, .. } => {
                                    let retained_branches: Vec<daybook_types::doc::BranchPathBuf> = entry
                                        .branches
                                        .keys()
                                        .map(|branch_name| {
                                            daybook_types::doc::BranchPathBuf::from(branch_name.as_str())
                                        })
                                        .collect();
                                    doc_blobs_index_repo
                                        .enqueue_delete_branches_not_in(
                                            id.clone(),
                                            retained_branches,
                                        )
                                        .unwrap_or_log();
                                    for (branch_name, heads) in &entry.branches {
                                        doc_blobs_index_repo
                                            .enqueue_upsert(
                                                id.clone(),
                                                daybook_types::doc::BranchPathBuf::from(
                                                    branch_name.as_str(),
                                                ),
                                                heads.clone(),
                                            )
                                            .unwrap_or_log();
                                    }
                                }
                            },
                            Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                                panic!("doc blobs bridge dropped {dropped_count} drawer events");
                            }
                            Err(crate::repos::RecvError::Closed) => break,
                        }
                    }
                }
            }
        });
        (cancel, handle)
    }

    async fn list_doc_ids(drawer: &DrawerRepo) -> Res<HashSet<String>> {
        let (_, ids) = drawer.list_just_ids().await?;
        Ok(ids.into_iter().collect())
    }

    #[tracing::instrument(skip_all)]
    async fn wait_for_doc_presence_with_activity(
        node: &SyncTestNode,
        doc_id: &DocId,
        absolute_timeout: Duration,
    ) -> Res<()> {
        let last_activity = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
        let last_activity_for_wait = Arc::clone(&last_activity);
        let drawer_listener = node.drawer.subscribe(SubscribeOpts::new(1024));
        let sync_listener = node.sync_repo.subscribe(SubscribeOpts::new(2048));
        let progress_listener = node.progress_repo.subscribe(SubscribeOpts::new(4096));
        let mut loop_count = 0u64;
        tokio::time::timeout(absolute_timeout, async {
            loop {
                loop_count += 1;
                let found = node
                    .drawer
                    .get_doc_with_facets_at_branch(doc_id, daybook_types::doc::BranchPath::new("main"), None)
                    .await?
                    .is_some();
                if found {
                    break;
                }
                tokio::select! {
                    val = drawer_listener.recv_lossy_async() => {
                        let evt = val.map_err(|_| eyre::eyre!("drawer listener closed while waiting for doc presence"))?;
                        match evt.as_ref() {
                            crate::drawer::DrawerEvent::DocAdded { id, .. }
                            | crate::drawer::DrawerEvent::DocUpdated { id, .. }
                            | crate::drawer::DrawerEvent::DocDeleted { id, .. } if id == doc_id => {
                                *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                            }
                            crate::drawer::DrawerEvent::DocAdded { .. }
                            | crate::drawer::DrawerEvent::DocUpdated { .. }
                            | crate::drawer::DrawerEvent::DocDeleted { .. } => {
                                *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                            }
                        }
                    }
                    val = sync_listener.recv_async() => {
                        match val {
                            Ok(_) => {
                                *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                            }
                            Err(crate::repos::RecvError::Closed) => eyre::bail!("sync listener closed while waiting for doc presence"),
                            Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                                eyre::bail!("sync listener dropped events while waiting for doc presence: dropped_count={dropped_count}");
                            }
                        }
                    }
                    val = progress_listener.recv_async() => {
                        match val {
                            Ok(_) => {
                                *last_activity_for_wait.lock().expect(ERROR_MUTEX) = std::time::Instant::now();
                            }
                            Err(crate::repos::RecvError::Closed) => eyre::bail!("progress listener closed while waiting for doc presence"),
                            Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                                eyre::bail!("progress listener dropped events while waiting for doc presence: dropped_count={dropped_count}");
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(150)) => {}
                }
            }
            eyre::Ok(())
        })
        .await
        .map_err(|_| {
            let since_last_activity = std::time::Instant::now()
                .saturating_duration_since(*last_activity.lock().expect(ERROR_MUTEX));
            eyre::eyre!(
                "timed out waiting for document presence: doc_id={doc_id} absolute_timeout={:?} (last_activity_ago={:?})",
                absolute_timeout,
                since_last_activity,
            )
        })??;
        Ok(())
    }

    async fn wait_for_sync_convergence(
        source: &SyncTestNode,
        target: &SyncTestNode,
        endpoint_id: EndpointId,
        timeout: Duration,
    ) -> Res<()> {
        let required_partitions = source
            .sync_repo
            .peer_partition_ids("")
            .into_iter()
            .collect::<Vec<_>>();
        tokio::try_join!(
            target.sync_repo.wait_for_full_sync(
                std::slice::from_ref(&endpoint_id),
                &required_partitions,
                timeout,
            ),
            wait_for_doc_set_parity(&source.drawer, &target.drawer, timeout),
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_full_sync_succeeds_after_event_was_already_emitted() -> Res<()> {
        utils_rs::testing::setup_tracing_once();

        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
        let endpoint_addr_ba = node_b.sync_repo.connect_url(&ticket_a).await?;

        wait_for_sync_convergence(
            &node_a,
            &node_b,
            endpoint_addr_ba.id,
            Duration::from_secs(20),
        )
        .await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let required_partitions = node_b
            .sync_repo
            .peer_partition_ids("")
            .into_iter()
            .collect::<Vec<_>>();
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&endpoint_addr_ba.id),
                &required_partitions,
                Duration::from_secs(5),
            )
            .await?;

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    async fn wait_for_doc_set_parity(
        left: &DrawerRepo,
        right: &DrawerRepo,
        timeout: Duration,
    ) -> Res<()> {
        let mut last_left = HashSet::<String>::new();
        let mut last_right = HashSet::<String>::new();
        let timeout_outcome = tokio::time::timeout(timeout, async {
            let mut last_heartbeat = std::time::Instant::now();
            loop {
                let lset = list_doc_ids(left).await?;
                let rset = list_doc_ids(right).await?;
                last_left = lset.clone();
                last_right = rset.clone();
                if lset == rset {
                    debug!(count = lset.len(), "drawer doc-set parity reached");
                    break;
                }
                let now = std::time::Instant::now();
                if now.duration_since(last_heartbeat) >= Duration::from_secs(2) {
                    last_heartbeat = now;
                    let missing_on_right =
                        lset.difference(&rset).take(8).cloned().collect::<Vec<_>>();
                    let missing_on_left =
                        rset.difference(&lset).take(8).cloned().collect::<Vec<_>>();
                    debug!(
                        left_count = lset.len(),
                        right_count = rset.len(),
                        missing_on_right = ?missing_on_right,
                        missing_on_left = ?missing_on_left,
                        "waiting for drawer doc-set parity"
                    );
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            eyre::Ok(())
        })
        .await;
        match timeout_outcome {
            Ok(out) => out?,
            Err(_) => {
                let missing_on_right = last_left
                    .difference(&last_right)
                    .take(12)
                    .cloned()
                    .collect::<Vec<_>>();
                let missing_on_left = last_right
                    .difference(&last_left)
                    .take(12)
                    .cloned()
                    .collect::<Vec<_>>();
                eyre::bail!(
                    "timed out waiting for drawer doc-set parity: left_count={} right_count={} missing_on_right={missing_on_right:?} missing_on_left={missing_on_left:?}",
                    last_left.len(),
                    last_right.len()
                );
            }
        }
        Ok(())
    }

    async fn wait_for_doc_head_parity(
        left: &SyncTestNode,
        right: &SyncTestNode,
        doc_id: &String,
        branch: &daybook_types::doc::BranchPath,
        timeout: Duration,
    ) -> Res<()> {
        let mut last_left = None::<Vec<String>>;
        let mut last_right = None::<Vec<String>>;
        tokio::time::timeout(timeout, async {
            loop {
                let left_heads = left
                    .drawer
                    .get_with_heads(doc_id, branch, None)
                    .await?
                    .map(|(_, heads)| {
                        let mut out = heads.iter().map(ToString::to_string).collect::<Vec<_>>();
                        out.sort_unstable();
                        out
                    })
                    .ok_or_else(|| eyre::eyre!("left missing doc heads for {doc_id}"))?;
                let right_heads = right
                    .drawer
                    .get_with_heads(doc_id, branch, None)
                    .await?
                    .map(|(_, heads)| {
                        let mut out = heads.iter().map(ToString::to_string).collect::<Vec<_>>();
                        out.sort_unstable();
                        out
                    })
                    .ok_or_else(|| eyre::eyre!("right missing doc heads for {doc_id}"))?;
                last_left = Some(left_heads);
                last_right = Some(right_heads);
                if last_left == last_right {
                    break eyre::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| {
            eyre::eyre!(
                "timed out waiting for doc head parity: doc_id={} branch={} left={:?} right={:?}",
                doc_id,
                branch,
                last_left,
                last_right
            )
        })??;
        Ok(())
    }

    async fn wait_for_blob_bytes(
        blobs_repo: &BlobsRepo,
        hash: &str,
        timeout: Duration,
    ) -> Res<Vec<u8>> {
        tokio::time::timeout(timeout, async {
            loop {
                let path = match blobs_repo.get_path(hash).await {
                    Ok(path) => path,
                    Err(err) => {
                        let msg = err.to_string();
                        if msg.contains("Blob not found:")
                            || msg.contains("Referenced blob source missing for hash")
                        {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            continue;
                        }
                        return Err(err);
                    }
                };
                if tokio::fs::try_exists(&path).await? {
                    return tokio::fs::read(path).await.map_err(Into::into);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for blob bytes: {hash}"))?
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_blob_bytes_retries_until_blob_arrives() -> Res<()> {
        utils_rs::testing::setup_tracing_once();

        let temp_root = tempfile::tempdir()?;
        let blobs_repo = BlobsRepo::new(
            temp_root.path().join("blobs"),
            "/u/stress-test/dev-local".into(),
            Arc::new(crate::blobs::NoopPartitionMembershipWriter),
        )
        .await?;
        let payload = b"delayed-blob-arrival".to_vec();
        let expected_hash = utils_rs::hash::blake3_hash_bytes(&payload);

        let repo_bg = Arc::clone(&blobs_repo);
        let payload_bg = payload.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            repo_bg.put(&payload_bg).await.expect("put should succeed");
        });

        let got = wait_for_blob_bytes(&blobs_repo, &expected_hash, Duration::from_secs(5)).await?;
        assert_eq!(got, payload);

        blobs_repo.shutdown().await?;
        Ok(())
    }
}
