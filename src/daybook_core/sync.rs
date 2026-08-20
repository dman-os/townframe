use crate::interlude::*;

use std::str::FromStr;

use big_repo::BigRepo;
use big_sync::BackendId;
use iroh::{
    EndpointId,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
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
pub const REPO_SYNC_ALPN: &[u8] = big_repo::rpc::REPO_SYNC_ALPN;
pub const CLONE_PROVISION_ALPN: &[u8] = b"townframe/clone-provision/0";
pub(crate) const BLOBS_BACKEND_ID: &str = "blobs";

pub type PeerKey = Arc<str>;

#[derive(Debug, Clone)]
struct SubductionProtocolHandler {
    big_repo: Arc<BigRepo>,
    endpoint: iroh::Endpoint,
    incoming_conn_tx: mpsc::UnboundedSender<big_repo::BigRepoConnection>,
    end_signal_tx: mpsc::UnboundedSender<big_repo::ConnFinishSignal>,
}

impl ProtocolHandler for SubductionProtocolHandler {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let conn = self
            .big_repo
            .accept_connection_iroh(
                conn,
                self.endpoint.clone(),
                Some(self.end_signal_tx.clone()),
            )
            .await
            .map_err(|err| AcceptError::from_boxed(err.into()))?;
        tracing::debug!(peer_id = %conn.peer_id, "subduction conn accepted");
        self.incoming_conn_tx.send(conn).ok();
        Ok(())
    }
}

#[derive(Debug)]
enum ActivePeerState {
    Connecting {
        // The registering connection's end flag, once it exists. `None` only
        // while an outbound dial reserves the slot before the connection has
        // been opened (no end signal can exist before then).
        closed: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    },
    Connected {
        peer_key: PeerKey,
        // The connection's end flag (shared with the runtime's watcher),
        // used to identify which connection a `ConnFinishSignal` belongs to.
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    },
}

pub struct IrohSyncRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    rcx: Arc<RepoCtx>,
    authority: crate::authority::RepoAuthority,

    router: iroh::protocol::Router,
    address_lookup: iroh::address_lookup::MemoryLookup,

    config_repo: Arc<crate::config::ConfigRepo>,
    blobs_sync_backend: Arc<crate::blobs::sync::BlobSyncBackend>,
    _doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,

    conn_end_signal_tx: mpsc::UnboundedSender<big_repo::ConnFinishSignal>,
    active_peers: tokio::sync::RwLock<HashMap<PeerId, ActivePeerState>>,
    // sync_store: am_utils_rs::sync::store::SyncStoreHandle,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    big_sync_worker: big_sync::BigSyncWorkerHandle,
    blob_sync_worker: big_sync::BigSyncWorkerHandle,
    big_repo_rpc: big_repo::rpc::BigRepoRpcHandle,
    _big_sync_rpc: big_sync::rpc::BigSyncRpcHandle,
    _blob_sync_rpc: big_sync::rpc::BigSyncRpcHandle,
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
    blob_sync_rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    blob_sync_worker_stop: big_sync::StopToken,
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
        self.big_sync_worker_stop.stop().await?;
        self.big_sync_rpc_stop.stop().await?;
        self.blob_sync_worker_stop.stop().await?;
        self.blob_sync_rpc_stop.stop().await?;
        self.big_repo_rpc_stop_token.stop().await?;
        // Worker shutdown drains active repo connections; each connection stop can wait up to 5s.
        utils_rs::wait_on_handle_with_timeout(
            self.worker_handle,
            utils_rs::scale_timeout(Duration::from_secs(30)),
        )
        .await?;
        let endpoint = self.router.endpoint().clone();
        endpoint.close().await;
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
        let address_lookup = iroh::address_lookup::MemoryLookup::default();
        let endpoint_builder = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .secret_key(rcx.iroh_secret_key.clone())
            .address_lookup(address_lookup.clone());
        #[cfg(test)]
        let endpoint_builder = endpoint_builder
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled);
        let endpoint = endpoint_builder.bind().await?;
        let blobs = blobs_repo.iroh_store();
        let blobs_sync_backend = Arc::new(crate::blobs::sync::BlobSyncBackend::new(
            Arc::clone(&blobs_repo),
            Arc::clone(&rcx.blob_part_store),
            endpoint.clone(),
            address_lookup.clone(),
        ));
        blobs_repo.set_sync_backend((*blobs_sync_backend).clone());

        let cancel_token = CancellationToken::new();
        let authority = crate::authority::ensure(&rcx.big_repo, &rcx.sql, None).await?;

        let (incoming_conn_tx, incoming_conn_rx) = mpsc::unbounded_channel();
        let (conn_end_tx, conn_end_rx) = mpsc::unbounded_channel();
        let (clone_rpc_tx, clone_rpc_rx) = mpsc::channel(128);

        let (big_repo_rpc, repo_rpc_stop_token) =
            big_repo::rpc::spawn_repo_rpc(Arc::clone(&rcx.big_repo)).await?;

        let repo_sync_backend = Arc::new(
            big_repo::BigRepoSyncBackend::boot(Arc::downgrade(&rcx.big_repo))
                .await
                .wrap_err("failed booting big repo sync backend")?,
        );
        let blob_sync_backend: Arc<dyn big_sync::SyncBackend> =
            Arc::clone(&blobs_sync_backend) as _;
        let mut doc_sync_backends = std::collections::HashMap::new();
        doc_sync_backends.insert(
            big_repo::BigRepo::BACKEND_ID.into(),
            Arc::clone(&repo_sync_backend) as _,
        );
        let max_task_backoff = rcx.options.sync_max_task_backoff;

        let (big_sync_worker, big_sync_worker_stop) = big_sync::spawn_big_sync_worker_with_options(
            Arc::clone(&rcx.part_store),
            doc_sync_backends,
            "daybook-docs",
            max_task_backoff,
        )?;

        let mut blob_sync_backends = std::collections::HashMap::new();
        blob_sync_backends.insert(BLOBS_BACKEND_ID.into(), blob_sync_backend);
        let (blob_sync_worker, blob_sync_worker_stop) =
            big_sync::spawn_big_sync_worker_with_options(
                Arc::clone(&rcx.blob_part_store),
                blob_sync_backends,
                "daybook-blobs",
                max_task_backoff,
            )?;

        let (big_sync_rpc, big_sync_rpc_stop) =
            big_sync::rpc::spawn_big_sync_rpc(Arc::clone(&rcx.part_store)).await?;
        let (blob_sync_rpc, blob_sync_rpc_stop) =
            big_sync::rpc::spawn_big_sync_rpc(Arc::clone(&rcx.blob_part_store)).await?;
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                SUBDUCTION_ALPN,
                SubductionProtocolHandler {
                    big_repo: Arc::clone(&rcx.big_repo),
                    endpoint: endpoint.clone(),
                    incoming_conn_tx,
                    end_signal_tx: conn_end_tx.clone(),
                },
            )
            .accept(
                big_sync::rpc::BIG_SYNC_RPC_ALPN,
                big_sync_rpc.protocol_handler(),
            )
            .accept(
                big_sync::rpc::BIG_SYNC_BLOB_RPC_ALPN,
                blob_sync_rpc.protocol_handler(),
            )
            .accept(
                big_repo::rpc::REPO_SYNC_ALPN,
                big_repo_rpc.protocol_handler(),
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
            .spawn();

        config_repo
            .ensure_local_sync_device(router.endpoint().id(), &rcx.local_device_name)
            .await?;

        let big_sync_rx = big_sync_worker.subscribe_stats();
        let reconnect_task = default();
        let repo = Arc::new(Self {
            rcx,
            authority,
            router: router.clone(),
            address_lookup,
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
            blob_sync_worker,
            big_repo_rpc: big_repo_rpc.clone(),
            _big_sync_rpc: big_sync_rpc,
            _blob_sync_rpc: blob_sync_rpc,
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
                blob_sync_rpc_stop,
                blob_sync_worker_stop,
            },
        ))
    }

    fn ensure_repo_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is shutting down");
        }
        Ok(())
    }
}

impl IrohSyncRepo {
    #[inline]
    pub fn is_blob_part(&self, part_id: PartId) -> bool {
        let core_blob = crate::blobs::blob_inventory_part_id(&self.rcx.core_inventory_doc_id);
        let docs_blob = crate::blobs::blob_inventory_part_id(&self.rcx.docs_inventory_doc_id);
        part_id == core_blob || part_id == docs_blob
    }

    fn peer_partition_ids(
        &self,
        _peer_key: &str,
        include_blob_parts: bool,
    ) -> HashMap<PartId, BackendId> {
        let repo_backend_id = big_repo::BigRepo::BACKEND_ID.into();
        let mut parts = HashMap::from([
            (
                self.authority.core_docs_part_id(),
                Arc::clone(&repo_backend_id),
            ),
            (
                self.authority.content_docs_part_id(),
                Arc::clone(&repo_backend_id),
            ),
            (
                self.authority.default_drawer_part_id(),
                Arc::clone(&repo_backend_id),
            ),
            (
                self.authority.blob_inventories_part_id(),
                Arc::clone(&repo_backend_id),
            ),
        ]);
        if include_blob_parts {
            let blob_backend_id = BLOBS_BACKEND_ID.into();
            parts.insert(
                crate::blobs::blob_inventory_part_id(&self.rcx.core_inventory_doc_id),
                Arc::clone(&blob_backend_id),
            );
            parts.insert(
                crate::blobs::blob_inventory_part_id(&self.rcx.docs_inventory_doc_id),
                blob_backend_id,
            );
        }
        parts
    }

    fn split_partitions(
        parts: HashMap<PartId, BackendId>,
    ) -> (HashMap<PartId, BackendId>, HashMap<PartId, BackendId>) {
        let blob_backend = BLOBS_BACKEND_ID.into();
        let mut doc = HashMap::new();
        let mut blob = HashMap::new();
        for (part, backend) in parts {
            if backend == blob_backend {
                blob.insert(part, backend);
            } else {
                doc.insert(part, backend);
            }
        }
        (doc, blob)
    }

    async fn spawn_connect_known_devices_once(self: &Arc<Self>, trigger: &'static str) {
        let Ok(mut reconnect_task) = self.reconnect_task.try_lock() else {
            // if locked, someone else has already qued an reconnect task or
            // or we're shutting down
            return;
        };
        if let Some(existing) = reconnect_task.as_ref()
            && !existing.is_finished()
        {
            return;
        }
        // NOTE: we just drop the old handle since we're using
        // a mutex which we shouldn't hold across await points
        // if let Some(done) = reconnect_task.take() {
        //     let _ = done.await;
        // }
        let repo = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let _cancelled = repo
                .cancel_token
                .clone()
                .run_until_cancelled(async move {
                    if let Err(err) = repo.connect_known_devices_once().await
                        && !repo.cancel_token.is_cancelled()
                    {
                        warn!(?err, trigger, "known-device reconnect failed");
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
                            let WithChannels { tx, .. } = req;
                            tx.send(Ok(bootstrap::CloneInfoResponse {
                                repo_name: self.rcx.repo_name.clone(),
                                device_name: Some(self.rcx.local_device_name.clone()),
                                }))
                                .await
                                .inspect_err(|_| warn_loc!(ERROR_CALLER))
                                .ok();
                        }
                        bootstrap::CloneProvisionRpcMessage::RequestCloneProvision(req) => {
                            let WithChannels { inner, tx, .. } = req;
                            let out = self.handle_request_clone_provision(inner.req).await;
                            tx.send(out.map_err(|err| format!("{err:#}")))
                                .await
                                .inspect_err(|_| warn_loc!(ERROR_CALLER))
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
                    // Per-connection error isolation: an incoming connection
                    // handler failure (provision sync cancelled by a closed
                    // connection, keyhive round failure, ...) drops that
                    // connection — the handler has already cleaned up its
                    // registration and re-registered a clone for re-provision
                    // — but must not take down the whole sync machine. The
                    // peer's reconnect machinery re-establishes the
                    // connection and re-runs the flow.
                    if let Err(error) = self.handle_incoming_big_repo_conn(conn).await {
                        warn!(?error, "incoming big_repo connection failed; dropping connection");
                    }
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
            self.big_repo_rpc.unregister_peer(peer_id);
            self.big_sync_worker.remove_peer(peer_id).await.ok();
        }
        self.active_peers.write().await.clear();
        eyre::Ok(())
    }
    async fn handle_incoming_big_repo_conn(&self, conn: big_repo::BigRepoConnection) -> Res<()> {
        tracing::debug!(
            peer_id = %conn.peer_id,
            closed = conn.is_closed(),
            "handle_incoming: start"
        );
        // peer id — the clone bootstrap persists the clone identity, so the
        // clone-phase connection and the peer's runtime connection present
        // the same id. Tear down the old registration first; the old
        // connection's own end signal is identified as stale by its end flag
        // and ignored. A registration already in flight (`Connecting`) is
        // left alone rather than raced.
        let replaced = {
            let mut active_peers = self.active_peers.write().await;
            match active_peers.get(&conn.peer_id) {
                Some(ActivePeerState::Connected { .. }) => {
                    active_peers.remove(&conn.peer_id);
                    true
                }
                Some(ActivePeerState::Connecting { .. }) => {
                    warn!(
                        peer_id = %conn.peer_id,
                        "ignoring duplicate incoming connection during setup"
                    );
                    return Ok(());
                }
                None => false,
            }
        };
        if replaced {
            warn!(
                peer_id = %conn.peer_id,
                "new connection replacing existing registration (re-establishment)"
            );
            self.teardown_peer_registration(conn.peer_id).await;
        }
        {
            let mut active_peers = self.active_peers.write().await;
            active_peers.insert(
                conn.peer_id,
                ActivePeerState::Connecting {
                    closed: Some(conn.closed_flag()),
                },
            );
        }
        let peer_id = conn.peer_id;
        let res = async {
            let peer_key = daybook_types::doc::format_peer_key(conn.peer_id.as_bytes());
            let events = [IrohSyncEvent::IncomingConnection {
                peer_key: Arc::clone(&peer_key),
            }];
            let endpoint = self.router.endpoint().clone();
            let remote_info = endpoint
                .remote_info(
                    EndpointId::from_bytes(conn.peer_id.as_bytes()).expect(ERROR_IMPOSSIBLE),
                )
                .await
                .ok_or_eyre("unable to get remote info for incoming conn")?;
            let remote_endpoint_id = remote_info.id();
            let addr = iroh::EndpointAddr::from_parts(
                remote_endpoint_id,
                remote_info.into_addrs().map(|info| info.into_addr()),
            );
            self.address_lookup.add_endpoint_info(addr.clone());
            self.blobs_sync_backend
                .register_peer_addr(conn.peer_id, addr.clone());
            self.big_repo_rpc.register_peer(remote_endpoint_id, peer_id);
            let doc_rpc_client =
                big_sync::rpc::IrohBigSyncRpcClient::new(endpoint.clone(), addr.clone());
            let blob_rpc_client = big_sync::rpc::IrohBigSyncRpcClient::new_with_alpn(
                endpoint,
                addr.clone(),
                big_sync::rpc::BIG_SYNC_BLOB_RPC_ALPN,
            );
            let doc_rpc_client = Arc::new(doc_rpc_client);
            let blob_rpc_client = Arc::new(blob_rpc_client);

            let partition_ids = self.peer_partition_ids(&peer_key, true);
            let (doc_parts, blob_parts) = Self::split_partitions(partition_ids);
            self.big_sync_worker
                .set_peer(conn.peer_id, doc_rpc_client, doc_parts, HashMap::new())
                .await?;
            if !blob_parts.is_empty() {
                self.blob_sync_worker
                    .set_peer(conn.peer_id, blob_rpc_client, blob_parts, HashMap::new())
                    .await?;
            }

            let old = self.active_peers.write().await.insert(
                peer_id,
                ActivePeerState::Connected {
                    peer_key,
                    closed: conn.closed_flag(),
                },
            );
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );

            self.registry.notify(events);
            eyre::Ok(())
        }
        .await;
        if let Err(error) = &res {
            error!(%peer_id, ?error, "incoming BigRepo connection setup failed");
            self.big_repo_rpc.unregister_peer(peer_id);
            let old = self.active_peers.write().await.remove(&peer_id);
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );
        }

        res
    }

    async fn handle_big_repo_conn_end(
        self: &Arc<Self>,
        signal: big_repo::ConnFinishSignal,
    ) -> Res<()> {
        // The runtime emits a signal for every tracked connection end. A
        // replaced (superseded) connection's end must not tear down the
        // replacement: only act when the signal's end flag matches the peer's
        // current registration.
        let is_current = {
            let active_peers = self.active_peers.read().await;
            match active_peers.get(&signal.peer_id) {
                Some(ActivePeerState::Connected { closed, .. }) => {
                    std::sync::Arc::ptr_eq(closed, &signal.closed)
                }
                Some(ActivePeerState::Connecting {
                    closed: Some(closed),
                }) => std::sync::Arc::ptr_eq(closed, &signal.closed),
                // A dial reservation with no connection yet cannot own this
                // end signal. The dialing task still owns the slot.
                Some(ActivePeerState::Connecting { closed: None }) => false,
                None => false,
            }
        };
        if !is_current {
            debug!(
                peer_id = %signal.peer_id,
                current = false,
                error = ?signal.err,
                "connection end for replaced or unknown connection; ignoring"
            );
            return Ok(());
        }
        info!(
            peer_id = %signal.peer_id,
            current = true,
            error = ?signal.err,
            "current connection ended; tearing down peer registration"
        );
        self.teardown_peer_registration(signal.peer_id).await;
        let removed = self.active_peers.write().await.remove(&signal.peer_id);
        let peer_key = match removed {
            Some(ActivePeerState::Connected { peer_key, .. }) => Some(peer_key),
            Some(ActivePeerState::Connecting { .. }) => None,
            None => {
                debug!(peer_id = %signal.peer_id, "connection end for unknown peer");
                return Ok(());
            }
        };
        if let Some(peer_key) = peer_key {
            let events = [IrohSyncEvent::ConnectionClosed {
                peer_key,
                reason: signal
                    .err
                    .map(|err| format!("conn error: {err}"))
                    .unwrap_or_else(|| "natural disconnect".into()),
            }];

            self.registry.notify(events);
        }
        if self.cancel_token.is_cancelled() {
            return Ok(());
        }
        self.spawn_connect_known_devices_once("connection-close")
            .await;
        Ok(())
    }

    /// Tear down a peer's registration without touching `active_peers` (the
    /// caller manages that). Idempotent per peer.
    async fn teardown_peer_registration(&self, peer_id: PeerId) {
        self.big_repo_rpc.unregister_peer(peer_id);
        self.blobs_sync_backend.unregister_peer_addr(peer_id);
        self.big_sync_worker.remove_peer(peer_id).await.ok();
        self.blob_sync_worker.remove_peer(peer_id).await.ok();
    }

    async fn handle_request_clone_provision(
        &self,
        req: bootstrap::RequestCloneProvisionReq,
    ) -> Res<bootstrap::CloneProvisionResponse> {
        let endpoint_id = iroh::PublicKey::from_str(&req.requester_endpoint_id)
            .wrap_err("invalid requester_endpoint_id in clone provision request")?;
        eyre::ensure!(
            req.requester_contact_card.id().to_bytes() == *endpoint_id.as_bytes(),
            "clone endpoint identity does not match its Keyhive contact card"
        );
        let requester = self
            .rcx
            .big_repo
            .receive_keyhive_contact_card(&req.requester_contact_card)
            .await?;
        self.rcx
            .big_repo
            .add_admin_member_to_group(requester, &self.authority.repo_agents)
            .await?;
        let endpoint_addr = self.endpoint_addr();
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
            repo_agents_group: self.authority.ids().repo_agents,
            core_docs_group: self.authority.ids().core_docs,
            content_docs_group: self.authority.ids().content_docs,
            default_drawer_group: self.authority.ids().default_drawer,
            blob_inventories_group: self.authority.ids().blob_inventories,
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
        active_peers.insert(peer_id, ActivePeerState::Connecting { closed: None });
        true
    }

    async fn handle_big_sync_evt(&self, evt: big_sync_core::SyncStatEvent) -> Res<()> {
        match evt {
            big_sync_core::SyncStatEvent::ObjectSynced { peer_id, obj_id } => {
                info!(
                    local_peer_id = %self.router.endpoint().id(),
                    %peer_id,
                    %obj_id,
                    "BigSync object synced"
                );
                self.registry.notify([IrohSyncEvent::DocSyncedWithPeer {
                    peer_key: daybook_types::doc::format_peer_key(peer_id.as_bytes()),
                    doc_id: obj_id,
                }]);
            }
            big_sync_core::SyncStatEvent::PeerPartFullySynced { peer_id, part_id } => {
                info!(
                    local_peer_id = %self.router.endpoint().id(),
                    %peer_id,
                    %part_id,
                    "BigSync peer partition fully synced"
                );
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

            let partition_ids = self.peer_partition_ids(&peer_key, true);
            let (doc_parts, blob_parts) = Self::split_partitions(partition_ids);
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
            let doc_rpc_client =
                big_sync::rpc::IrohBigSyncRpcClient::new(endpoint.clone(), endpoint_addr.clone());
            let blob_rpc_client = big_sync::rpc::IrohBigSyncRpcClient::new_with_alpn(
                endpoint,
                endpoint_addr.clone(),
                big_sync::rpc::BIG_SYNC_BLOB_RPC_ALPN,
            );
            let doc_rpc_client = Arc::new(doc_rpc_client);
            let blob_rpc_client = Arc::new(blob_rpc_client);

            self.address_lookup.add_endpoint_info(endpoint_addr.clone());
            self.blobs_sync_backend
                .register_peer_addr(conn.peer_id, endpoint_addr.clone());
            self.big_repo_rpc.register_peer(endpoint_id, conn.peer_id);
            self.big_sync_worker
                .set_peer(
                    conn.peer_id,
                    Arc::clone(&doc_rpc_client) as Arc<dyn big_sync::rpc::HostBigRpcClient>,
                    doc_parts,
                    HashMap::new(),
                )
                .await?;
            if !blob_parts.is_empty() {
                self.blob_sync_worker
                    .set_peer(
                        conn.peer_id,
                        Arc::clone(&blob_rpc_client) as Arc<dyn big_sync::rpc::HostBigRpcClient>,
                        blob_parts,
                        HashMap::new(),
                    )
                    .await?;
            }
            info!(
                local_peer_id = %self.router.endpoint().id(),
                %peer_id,
                "outgoing connection registered; starting keyhive subscription"
            );

            let old = self.active_peers.write().await.insert(
                peer_id,
                ActivePeerState::Connected {
                    peer_key,
                    closed: conn.closed_flag(),
                },
            );
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );
            info!(
                local_peer_id = %self.router.endpoint().id(),
                %peer_id,
                "outgoing connection fully registered"
            );
            self.registry.notify(events);
            eyre::Ok(())
        }
        .await;
        if res.is_err() {
            self.big_repo_rpc.unregister_peer(peer_id);
            let old = self.active_peers.write().await.remove(&peer_id);
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            )
        }

        Ok(())
    }

    pub async fn connect_url(&self, source_url: &str) -> Res<iroh::EndpointAddr> {
        self.ensure_repo_live()?;
        let endpoint_addr = bootstrap::parse_clone_endpoint_addr(source_url)?;
        self.connect_endpoint_addr(endpoint_addr.clone()).await?;
        Ok(endpoint_addr)
    }

    pub async fn ensure_local_blob_from_active_peers(
        &self,
        blob_id: crate::blobs::BlobId,
    ) -> Res<()> {
        let peers = self
            .active_peers
            .read()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for peer_id in peers {
            if let Err(err) = self
                .blobs_sync_backend
                .ensure_local_blob(peer_id, blob_id)
                .await
            {
                tracing::warn!(%peer_id, %blob_id, ?err, "failed to download missing blob from active peer");
            } else {
                return Ok(());
            }
        }
        eyre::bail!("unable to download missing blob {blob_id} from any active peer");
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
        timeout: Option<Duration>,
    ) -> Res<()> {
        self.ensure_repo_live()?;
        let Some(_progress_repo) = self.progress_repo.clone() else {
            eyre::bail!("wait_for_full_sync requires a progress-enabled IrohSyncRepo");
        };
        if peer_ids.is_empty() {
            return Ok(());
        }
        let (blob_parts, doc_parts): (Vec<_>, Vec<_>) = required_partitions
            .iter()
            .partition(|part| self.is_blob_part(**part));
        let wait_fut = async {
            let doc_wait = self
                .big_sync_worker
                .wait_for_full_sync(peer_ids.iter().copied(), doc_parts.iter().copied());
            let blob_wait = self
                .blob_sync_worker
                .wait_for_full_sync(peer_ids.iter().copied(), blob_parts.iter().copied());
            tokio::try_join!(doc_wait, blob_wait)?;
            eyre::Ok(())
        };
        if let Some(timeout) = timeout {
            let timeout = utils_rs::scale_timeout(timeout);
            tokio::time::timeout(timeout, wait_fut)
                .await
                .wrap_err("timeout waiting for full_sync")??;
        } else {
            wait_fut.await?;
        }
        Ok(())
    }

    /// Test-support fence for the fixed point of BigSync and BigRepo local
    /// work. Unlike one `wait_for_full_sync`, this includes new part events
    /// generated while applying the preceding sync round.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn wait_for_network_rest(
        &self,
        peer_ids: &[PeerId],
        required_partitions: &[PartId],
    ) -> Res<()> {
        self.ensure_repo_live()?;
        let (blob_parts, doc_parts): (Vec<_>, Vec<_>) = required_partitions
            .iter()
            .copied()
            .partition(|part| self.is_blob_part(*part));
        let targets = [
            big_sync::test_support::NetworkRestTarget {
                worker: self.big_sync_worker.clone(),
                store: Arc::clone(&self.rcx.part_store) as Arc<dyn big_sync::HostPartStore>,
                peer_ids: peer_ids.to_vec(),
                part_ids: doc_parts,
            },
            big_sync::test_support::NetworkRestTarget {
                worker: self.blob_sync_worker.clone(),
                store: Arc::clone(&self.rcx.blob_part_store) as Arc<dyn big_sync::HostPartStore>,
                peer_ids: peer_ids.to_vec(),
                part_ids: blob_parts,
            },
        ];
        big_sync::test_support::wait_for_network_rest(&targets, || {
            self.rcx.big_repo.wait_for_quiescence(None)
        })
        .await
    }

    pub async fn wait_until_peers_sync(
        &self,
        peer_ids: &[PeerId],
        timeout: Option<Duration>,
    ) -> Res<()> {
        let parts = self
            .peer_partition_ids("", true)
            .into_keys()
            .collect::<Vec<_>>();
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
