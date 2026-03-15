use crate::interlude::*;

use am_utils_rs::sync::protocol::PartitionId;
use iroh::EndpointId;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::index::DocBlobsIndexRepo;
use crate::progress::ProgressRepo;
use crate::repo::RepoCtx;

mod bootstrap;
mod full;
pub use bootstrap::*;

pub const IROH_DOC_URL_SCHEME: &str = "db+iroh-doc";
pub const PARTITION_SYNC_ALPN: &[u8] = b"townframe/partition-sync/0";

enum ActivePeerState {
    Connecting,
    Connected(am_utils_rs::repo::RepoConnection),
}

pub struct IrohSyncRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    rcx: Arc<RepoCtx>,

    router: iroh::protocol::Router,

    config_repo: Arc<crate::config::ConfigRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,

    iroh_docs: iroh_docs::api::DocsApi,
    iroh_blobs: iroh_blobs::api::Store,

    conn_end_signal_tx: tokio::sync::mpsc::UnboundedSender<am_utils_rs::repo::ConnFinishSignal>,
    active_samod_peers: tokio::sync::RwLock<HashMap<EndpointId, ActivePeerState>>,
    full_sync_handle: full::WorkerHandle,
    // bootstrap_docs: tokio::sync::Mutex<Vec<iroh_docs::api::Doc>>,
    // active_endpoint_ids: tokio::sync::RwLock<HashMap<String, ()>>,
}

#[derive(Debug, Clone)]
pub enum IrohSyncEvent {
    IncomingConnection {
        endpoint_id: EndpointId,
        conn_id: samod::ConnectionId,
        peer_id: Arc<str>,
    },
    OutgoingConnection {
        endpoint_id: EndpointId,
        conn_id: samod::ConnectionId,
        peer_id: Arc<str>,
    },
    ConnectionClosed {
        endpoint_id: EndpointId,
        reason: String,
    },
    PeerFullySynced {
        endpoint_id: EndpointId,
        doc_count: usize,
    },
    DocSyncedWithPeer {
        endpoint_id: EndpointId,
        doc_id: DocumentId,
    },
    BlobSynced {
        hash: String,
        endpoint_id: Option<EndpointId>,
    },
    BlobDownloadStarted {
        endpoint_id: EndpointId,
        partition: String,
        hash: String,
    },
    BlobDownloadFinished {
        endpoint_id: EndpointId,
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
        endpoint_id: EndpointId,
    },
}

pub struct IrohSyncRepoStopToken {
    cancel_token: CancellationToken,
    worker_handle: JoinHandle<()>,
    router: iroh::protocol::Router,
    partition_sync_stop_token: am_utils_rs::sync::node::SyncNodeStopToken,
    partition_sync_store_stop_token: am_utils_rs::sync::store::SyncStoreStopToken,
    full_stop_token: full::StopToken,
}

impl IrohSyncRepoStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.full_stop_token.stop().await?;
        // NOTE: we only add timeouts for stop tokens that don't have internal
        // timeouts
        tokio::time::timeout(Duration::from_secs(10), self.router.shutdown())
            .await
            .map_err(|_| eyre::eyre!("timeout for waiting router shutdown"))??;
        self.partition_sync_stop_token.stop().await?;

        self.partition_sync_store_stop_token.stop().await?;
        utils_rs::wait_on_handle_with_timeout(self.worker_handle, Duration::from_secs(2)).await?;
        Ok(())
    }
}

impl IrohSyncRepo {
    async fn ensure_local_drawer_partition_seeded(rcx: &RepoCtx) -> Res<()> {
        let partition_id = crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(
            rcx.doc_drawer.document_id(),
        );
        let drawer_doc_id = rcx.doc_drawer.document_id().to_string();
        let app_doc_id = rcx.doc_app.document_id().to_string();
        rcx.big_repo
            .add_doc_to_partition(&partition_id, &drawer_doc_id)
            .await?;
        rcx.big_repo
            .add_doc_to_partition(&partition_id, &app_doc_id)
            .await?;
        Ok(())
    }

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
            .relay_mode(iroh::RelayMode::Disabled)
            .clear_address_lookup();
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
        let (partition_sync_store, partition_sync_store_stop_token) =
            am_utils_rs::sync::store::spawn_sync_store(rcx.big_repo.state_pool().clone()).await?;
        let (partition_sync_node, partition_sync_stop_token) =
            am_utils_rs::sync::node::spawn_sync_node(
                Arc::clone(&rcx.big_repo),
                partition_sync_store.clone(),
                Arc::new(am_utils_rs::sync::AllowAllPartitionAccessPolicy),
            )
            .await?;
        let partition_sync_node = Arc::new(partition_sync_node);

        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                am_utils_rs::BigRepo::SYNC_ALPN,
                am_utils_rs::repo::iroh::IrohRepoProtocol {
                    cancel_token: default(),
                    big_repo: Arc::clone(&rcx.big_repo),
                    conn_tx: incoming_conn_tx,
                    end_signal_tx: conn_end_tx.clone(),
                },
            )
            .accept(
                PARTITION_SYNC_ALPN,
                irpc_iroh::IrohProtocol::<am_utils_rs::sync::protocol::PartitionSyncRpc>::with_sender(
                    partition_sync_node.local_sender(),
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
        Self::ensure_local_drawer_partition_seeded(&rcx).await?;

        let (mut full_sync_handle, full_stop_token) = full::start_full_sync_worker(
            Arc::clone(&rcx),
            Arc::clone(&blobs_repo),
            Arc::clone(&doc_blobs_index_repo),
            progress_repo.clone(),
            Arc::clone(&partition_sync_node),
            partition_sync_store.clone(),
            endpoint.clone(),
        )
        .await?;
        let full_sync_rx = full_sync_handle.events_rx.take().expect("impossible");

        let repo = Arc::new(Self {
            rcx,
            router: router.clone(),
            config_repo,
            doc_blobs_index_repo,
            progress_repo,
            iroh_docs: docs.api().clone(),
            iroh_blobs: blobs,
            cancel_token: cancel_token.clone(),
            registry: crate::repos::ListenersRegistry::new(),
            active_samod_peers: default(),
            conn_end_signal_tx: conn_end_tx,
            full_sync_handle,
            // bootstrap_docs: tokio::sync::Mutex::new(Vec::new()),
            // active_endpoint_ids: tokio::sync::RwLock::new(HashMap::new()),
        });

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            async move {
                repo.machine_loop(full_sync_rx, incoming_conn_rx, conn_end_rx)
                    .await
                    .unwrap();
            }
            .instrument(tracing::info_span!("IrohSyncRepo listen task"))
        });

        Ok((
            repo,
            IrohSyncRepoStopToken {
                cancel_token,
                worker_handle,
                router,
                partition_sync_stop_token,
                partition_sync_store_stop_token,
                full_stop_token,
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
            crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(
                self.rcx.doc_drawer.document_id(),
            ),
        ]
        .into()
    }

    async fn machine_loop(
        &self,
        mut full_sync_rx: tokio::sync::broadcast::Receiver<full::FullSyncEvent>,
        mut incoming_conn_rx: tokio::sync::mpsc::UnboundedReceiver<
            am_utils_rs::repo::RepoConnection,
        >,
        mut conn_end_rx: tokio::sync::mpsc::UnboundedReceiver<am_utils_rs::repo::ConnFinishSignal>,
    ) -> Res<()> {
        loop {
            tokio::select! {
                biased;
                _ = self.cancel_token.cancelled() => {
                    debug!("cancel token lit");
                    break;
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
                    self.handle_incoming_am_conn(conn).await?;
                }
                val = conn_end_rx.recv() => {
                    let signal = val.expect("impossible actually");
                    self.handle_conn_end(signal).await?;
                }
            }
        }
        // cleanup
        {
            let mut active_samod_peers =
                std::mem::replace(&mut *self.active_samod_peers.write().await, default());
            use futures_buffered::BufferedStreamExt;
            futures::stream::iter(active_samod_peers.drain().filter_map(
                |(_endpoint_id, state)| match state {
                    ActivePeerState::Connected(conn) => Some(conn.stop()),
                    ActivePeerState::Connecting => None,
                },
            ))
            .buffered_unordered(16)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Res<Vec<_>>>()?;
            // FIXME: why was the futures_buffered based code replaced here?
            // for (endpoint_id, state) in active_samod_peers.drain() {
            //     if let ActivePeerState::Connected(conn) = state {
            //         if let Err(err) = conn.stop().await {
            //             warn!(
            //                 ?endpoint_id,
            //                 ?err,
            //                 "error stopping peer connection during cleanup"
            //             );
            //         }
            //     }
            // }
        }
        eyre::Ok(())
    }

    async fn handle_incoming_am_conn(&self, conn: am_utils_rs::repo::RepoConnection) -> Res<()> {
        let endpoint_id = conn
            .endpoint_id
            .expect("incoming iroh connection missing endpoint_id");
        let peer_key = conn.peer_id.to_string();

        {
            let mut active_samod_peers = self.active_samod_peers.write().await;
            if active_samod_peers.contains_key(&endpoint_id) {
                drop(active_samod_peers);
                conn.stop().await?;
                return Ok(());
            }
            active_samod_peers.insert(endpoint_id, ActivePeerState::Connecting);
        }

        let events = [IrohSyncEvent::IncomingConnection {
            endpoint_id,
            peer_id: Arc::<str>::clone(&conn.peer_id),
            conn_id: conn.id,
        }];
        let partition_ids = self.peer_partition_ids(&peer_key);

        if let Err(err) = self
            .full_sync_handle
            .set_connection(endpoint_id, conn.id, peer_key, partition_ids)
            .await
        {
            let old = self.active_samod_peers.write().await.remove(&endpoint_id);
            assert!(old.is_some(), "fishy");
            return Err(err);
        }

        let old = self
            .active_samod_peers
            .write()
            .await
            .insert(endpoint_id, ActivePeerState::Connected(conn));
        assert!(matches!(old, Some(ActivePeerState::Connecting)), "fishy");

        self.registry.notify(events);

        Ok(())
    }

    async fn handle_conn_end(&self, signal: am_utils_rs::repo::ConnFinishSignal) -> Res<()> {
        let Some(endpoint_id) = self.active_samod_peers.read().await.iter().find_map(
            |(endpoint_id, state)| match state {
                ActivePeerState::Connected(conn) if conn.id == signal.conn_id => Some(*endpoint_id),
                ActivePeerState::Connected(_) | ActivePeerState::Connecting => None,
            },
        ) else {
            debug!(
                conn_id = ?signal.conn_id,
                peer_id = %signal.peer_id,
                reason = %signal.reason,
                "ignoring finish signal for unknown connection"
            );
            return Ok(());
        };

        let events = [IrohSyncEvent::ConnectionClosed {
            endpoint_id,
            reason: signal.reason,
        }];

        self.full_sync_handle.del_connection(endpoint_id).await?;

        let old = self.active_samod_peers.write().await.remove(&endpoint_id);
        assert!(matches!(old, Some(ActivePeerState::Connected(_))), "fishy");

        self.registry.notify(events);
        Ok(())
    }

    async fn reserve_endpoint_connection(&self, endpoint_id: EndpointId) -> bool {
        let mut active_samod_peers = self.active_samod_peers.write().await;
        if active_samod_peers.contains_key(&endpoint_id) {
            return false;
        }
        active_samod_peers.insert(endpoint_id, ActivePeerState::Connecting);
        true
    }

    async fn clear_endpoint_if_connecting(&self, endpoint_id: EndpointId) {
        let mut active_samod_peers = self.active_samod_peers.write().await;
        if matches!(
            active_samod_peers.get(&endpoint_id),
            Some(ActivePeerState::Connecting)
        ) {
            active_samod_peers.remove(&endpoint_id);
        }
    }

    async fn finalize_outgoing_connection(
        &self,
        endpoint_id: EndpointId,
        peer_key: am_utils_rs::sync::protocol::PeerKey,
        conn: am_utils_rs::repo::RepoConnection,
    ) -> Res<()> {
        let conn_id = conn.id;
        let partition_ids = self.peer_partition_ids(&peer_key);
        if let Err(err) = self
            .full_sync_handle
            .set_connection(endpoint_id, conn_id, peer_key, partition_ids)
            .await
        {
            self.clear_endpoint_if_connecting(endpoint_id).await;
            return Err(err);
        }

        let old = self
            .active_samod_peers
            .write()
            .await
            .insert(endpoint_id, ActivePeerState::Connected(conn));
        assert!(matches!(old, Some(ActivePeerState::Connecting)), "fishy");
        Ok(())
    }

    async fn handle_full_sync_evt(&self, evt: full::FullSyncEvent) -> Res<()> {
        match evt {
            full::FullSyncEvent::PeerFullSynced {
                endpoint_id,
                doc_count,
            } => self.registry.notify([IrohSyncEvent::PeerFullySynced {
                endpoint_id,
                doc_count,
            }]),
            full::FullSyncEvent::DocSyncedWithPeer {
                endpoint_id,
                doc_id,
            } => self.registry.notify([IrohSyncEvent::DocSyncedWithPeer {
                endpoint_id,
                doc_id,
            }]),
            full::FullSyncEvent::BlobSynced { hash, endpoint_id } => self
                .registry
                .notify([IrohSyncEvent::BlobSynced { hash, endpoint_id }]),
            full::FullSyncEvent::BlobDownloadStarted {
                endpoint_id,
                partition,
                hash,
            } => self.registry.notify([IrohSyncEvent::BlobDownloadStarted {
                endpoint_id,
                partition: partition.as_tag_value().to_string(),
                hash,
            }]),
            full::FullSyncEvent::BlobDownloadFinished {
                endpoint_id,
                partition,
                hash,
                success,
            } => self.registry.notify([IrohSyncEvent::BlobDownloadFinished {
                endpoint_id,
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
            full::FullSyncEvent::StalePeer { endpoint_id } => self
                .registry
                .notify([IrohSyncEvent::StalePeer { endpoint_id }]),
        }
        Ok(())
    }

    pub async fn connect_endpoint_addr(&self, endpoint_addr: iroh::EndpointAddr) -> Res<()> {
        self.ensure_repo_live()?;

        if endpoint_addr.id == self.router.endpoint().id() {
            eyre::bail!("connecting to ourself is not supported");
        }
        let endpoint_id = endpoint_addr.id;

        if !self.reserve_endpoint_connection(endpoint_id).await {
            return Ok(());
        }

        let conn = match self
            .rcx
            .big_repo
            .spawn_connection_iroh(
                self.router.endpoint(),
                endpoint_addr,
                Some(self.conn_end_signal_tx.clone()),
            )
            .await
        {
            Ok(conn) => conn,
            Err(err) => {
                self.clear_endpoint_if_connecting(endpoint_id).await;
                return Err(err);
            }
        };
        let peer_id = conn.peer_info.peer_id.as_str().into();
        let conn_id = conn.id;
        let peer_key = conn.peer_id.to_string();
        let events = [IrohSyncEvent::OutgoingConnection {
            endpoint_id,
            peer_id,
            conn_id,
        }];

        self.finalize_outgoing_connection(endpoint_id, peer_key, conn)
            .await?;
        self.registry.notify(events);
        Ok(())
    }

    pub async fn connect_url(&self, iroh_doc_url: &str) -> Res<bootstrap::SyncBootstrapState> {
        self.ensure_repo_live()?;

        let bootstrap =
            bootstrap::resolve_bootstrap_with_docs(&self.iroh_docs, &self.iroh_blobs, iroh_doc_url)
                .await?;

        if bootstrap.repo_id != self.rcx.repo_id {
            eyre::bail!(
                "bootstrap repo_id mismatch (local={}, remote={})",
                self.rcx.repo_id,
                bootstrap.repo_id
            );
        }
        self.connect_endpoint_addr(bootstrap.endpoint_addr.clone())
            .await?;
        Ok(bootstrap)
    }

    pub async fn connect_known_devices_once(&self) -> Res<()> {
        self.ensure_repo_live()?;

        let devices = self.config_repo.list_known_sync_devices().await?;
        let local_endpoint_id = self.router.endpoint().id();
        for device in devices {
            if device.endpoint_id == local_endpoint_id {
                continue;
            }
            self.connect_endpoint_addr(iroh::EndpointAddr::new(device.endpoint_id))
                .await?;
        }
        Ok(())
    }

    pub async fn wait_for_full_sync(
        &self,
        endpoint_ids: &[EndpointId],
        timeout: Duration,
    ) -> Res<()> {
        use crate::repos::Repo;

        self.ensure_repo_live()?;
        let Some(progress_repo) = self.progress_repo.clone() else {
            eyre::bail!("wait_for_full_sync requires a progress-enabled IrohSyncRepo");
        };
        let target_peers: HashSet<EndpointId> = endpoint_ids.iter().cloned().collect();
        if target_peers.is_empty() {
            return Ok(());
        }
        let initial_snapshot = self
            .full_sync_handle
            .get_peer_sync_snapshot(endpoint_ids)
            .await?;
        let mut remaining = target_peers
            .iter()
            .filter(|endpoint_id| {
                !initial_snapshot
                    .get(*endpoint_id)
                    .is_some_and(|snapshot| snapshot.emitted_full_synced)
            })
            .copied()
            .collect::<HashSet<_>>();
        debug!(
            target_peers = ?target_peers,
            initial_snapshot = ?initial_snapshot,
            remaining = ?remaining,
            "wait_for_full_sync initial state"
        );
        let listener = self
            .registry
            .subscribe::<IrohSyncEvent>(crate::repos::SubscribeOpts { capacity: 1024 });
        let doc_blobs_listener = self
            .doc_blobs_index_repo
            .subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
        let timeout_outcome = tokio::time::timeout(timeout, async {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            let listener = listener;
            let doc_blobs_listener = doc_blobs_listener;
            let mut quiet_since: Option<std::time::Instant> = None;
            let mut heartbeat_at = std::time::Instant::now();
            loop {
                let now = std::time::Instant::now();
                if remaining.is_empty() {
                    match quiet_since {
                        None => quiet_since = Some(now),
                        Some(since) if now.duration_since(since) >= Duration::from_millis(3000) => {
                            break;
                        }
                        Some(_) => {}
                    }
                } else {
                    quiet_since = None;
                }
                tokio::select! {
                    val = listener.recv_async() => {
                        let event = val.map_err(|err| match err {
                            crate::repos::RecvError::Closed => eyre::eyre!("sync listener closed"),
                            crate::repos::RecvError::Dropped { dropped_count } => {
                                eyre::eyre!("sync listener dropped events: dropped_count={dropped_count}")
                            }
                        })?;
                        match event.as_ref() {
                            IrohSyncEvent::PeerFullySynced { endpoint_id, .. } => {
                                remaining.remove(endpoint_id);
                                quiet_since = None;
                            }
                            IrohSyncEvent::StalePeer { endpoint_id } => {
                                if target_peers.contains(endpoint_id) {
                                    remaining.insert(*endpoint_id);
                                    quiet_since = None;
                                }
                            }
                            IrohSyncEvent::BlobDownloadStarted { .. } => {
                                quiet_since = None;
                            }
                            IrohSyncEvent::BlobDownloadFinished { .. } => {
                                quiet_since = None;
                            }
                            _ => {}
                        }
                    }
                    val = doc_blobs_listener.recv_async() => {
                        match val {
                            Ok(_) => {
                                quiet_since = None;
                            }
                            Err(crate::repos::RecvError::Closed) => {
                                eyre::bail!("doc blobs index listener closed");
                            }
                            Err(crate::repos::RecvError::Dropped { dropped_count }) => {
                                eyre::bail!("doc blobs index listener dropped events: dropped_count={dropped_count}");
                            }
                        }
                    }
                    _ = tick.tick() => {
                        if !remaining.is_empty()
                            && now.duration_since(heartbeat_at) >= Duration::from_secs(2)
                        {
                            heartbeat_at = now;
                            let latest_snapshot = self
                                .full_sync_handle
                                .get_peer_sync_snapshot(endpoint_ids)
                                .await?;
                            for endpoint_id in &target_peers {
                                let emitted_full_synced = latest_snapshot
                                    .get(endpoint_id)
                                    .is_some_and(|snapshot| snapshot.emitted_full_synced);
                                if emitted_full_synced {
                                    remaining.remove(endpoint_id);
                                }
                            }
                            let tasks = progress_repo
                                .list_by_tag_prefix("sync/full/")
                                .await
                                .map(|tasks| {
                                    let active = tasks
                                        .iter()
                                        .filter(|task| task.state == crate::progress::ProgressTaskState::Active)
                                        .count();
                                    let ids = tasks.iter().map(|task| task.id.clone()).take(12).collect::<Vec<_>>();
                                    (tasks.len(), active, ids)
                                });
                            match tasks {
                                Ok((task_count, active_count, ids)) => {
                                    debug!(
                                        remaining = ?remaining,
                                        latest_snapshot = ?latest_snapshot,
                                        task_count,
                                        active_count,
                                        task_ids = ?ids,
                                        "wait_for_full_sync heartbeat"
                                    );
                                }
                                Err(err) => {
                                    warn!(?err, remaining = ?remaining, "wait_for_full_sync heartbeat failed listing progress tasks");
                                }
                            }
                        }
                    }
                }
            }
            eyre::Ok(())
        })
        .await;
        match timeout_outcome {
            Ok(out) => out?,
            Err(_) => {
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
                        format!("full_sync_tasks_total={}; full_sync_tasks_active={active}; sample_task_ids={ids:?}", tasks.len())
                    }
                    Err(err) => format!("failed_listing_full_sync_tasks={err:?}"),
                };
                eyre::bail!("timed out waiting for full sync: remaining={remaining:?}; {diag}");
            }
        }
        Ok(())
    }

    pub async fn wait_until_peers_sync(&self, endpoint_ids: &[EndpointId]) -> Res<()> {
        self.wait_for_full_sync(endpoint_ids, Duration::from_secs(30))
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
    mod stress;

    use crate::blobs::BlobsRepo;
    use crate::drawer::DrawerRepo;
    use crate::index::DocBlobsIndexRepo;
    use crate::local_state::SqliteLocalStateRepo;
    use crate::plugs::PlugsRepo;
    use crate::progress::ProgressRepo;
    use crate::repo::{RepoCtx, RepoOpenOptions};
    use crate::repos::{Repo, SubscribeOpts};
    use daybook_types::doc::{AddDocArgs, FacetKey, FacetRaw, WellKnownFacet, WellKnownFacetTag};
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;

    struct SyncTestNode {
        ctx: Arc<RepoCtx>,
        blobs_repo: Arc<BlobsRepo>,
        drawer: Arc<DrawerRepo>,
        progress_repo: Arc<ProgressRepo>,
        drawer_stop: crate::repos::RepoStopToken,
        _plugs_repo: Arc<PlugsRepo>,
        plugs_stop: crate::repos::RepoStopToken,
        config_stop: crate::repos::RepoStopToken,
        doc_blobs_index_stop: crate::index::DocBlobsIndexStopToken,
        sqlite_local_state_stop: crate::repos::RepoStopToken,
        doc_blobs_bridge_cancel: CancellationToken,
        doc_blobs_bridge_handle: Option<JoinHandle<()>>,
        sync_repo: Arc<IrohSyncRepo>,
        sync_stop: IrohSyncRepoStopToken,
    }

    impl SyncTestNode {
        async fn stop(mut self) -> Res<()> {
            tokio::time::timeout(Duration::from_secs(10), self.sync_stop.stop())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting sync stop"))??;
            self.doc_blobs_bridge_cancel.cancel();
            if let Some(handle) = self.doc_blobs_bridge_handle.take() {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    utils_rs::wait_on_handle_with_timeout(handle, Duration::from_secs(2)),
                )
                .await
                .map_err(|_| eyre::eyre!("timeout waiting doc blobs bridge join"))??;
            }
            tokio::time::timeout(Duration::from_secs(10), self.drawer_stop.stop())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting drawer stop"))??;
            tokio::time::timeout(Duration::from_secs(10), self.plugs_stop.stop())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting plugs stop"))??;
            tokio::time::timeout(Duration::from_secs(10), self.config_stop.stop())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting config stop"))??;
            tokio::time::timeout(Duration::from_secs(10), self.doc_blobs_index_stop.stop())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting doc blobs index stop"))??;
            tokio::time::timeout(Duration::from_secs(10), self.sqlite_local_state_stop.stop())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting sqlite local state stop"))??;
            tokio::time::timeout(Duration::from_secs(10), self.ctx.shutdown())
                .await
                .map_err(|_| eyre::eyre!("timeout waiting ctx shutdown"))??;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_sync_between_copied_repos() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
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
                    branch_path: daybook_types::doc::BranchPath::from("main"),
                    facets: default(),
                    user_path: Some(daybook_types::doc::UserPath::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
            created_doc_ids.push(new_doc_id);
        }

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap.endpoint_id,
            Duration::from_secs(20),
        )
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
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap.endpoint_id,
            Duration::from_secs(20),
        )
        .await?;

        let doc_on_a = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPath::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;

        let doc_on_b = node_b
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPath::from(
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
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let rtx = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions {
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        rtx.shutdown().await?;
        drop(rtx);

        let seed_node = open_sync_node(&repo_a_path).await?;
        let ticket = seed_node.sync_repo.get_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&ticket, &repo_b_path).await?;
        seed_node.stop().await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap.endpoint_id,
            Duration::from_secs(20),
        )
        .await?;

        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPath::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;

        for idx in 0..6 {
            let branch = daybook_types::doc::BranchPath::from("main");
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
                        user_path: Some(daybook_types::doc::UserPath::from(
                            node_a.ctx.local_user_path.clone(),
                        )),
                    },
                    branch.clone(),
                    Some(heads),
                )
                .await?;
        }

        wait_for_doc_head_parity(
            &node_a,
            &node_b,
            &doc_id,
            &daybook_types::doc::BranchPath::from("main"),
            Duration::from_secs(30),
        )
        .await?;

        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cloned_repo_registers_drawer_replicated_partition_on_open() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let rtx = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions {
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        rtx.shutdown().await?;
        drop(rtx);

        let node_a = open_sync_node(&repo_a_path).await?;
        let created_doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: daybook_types::doc::BranchPath::from("main"),
                facets: default(),
                user_path: Some(daybook_types::doc::UserPath::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        wait_for_doc_presence_with_activity(&node_a, &created_doc_id, Duration::from_secs(30))
            .await?;
        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;
        node_a.stop().await?;

        let node_b = open_sync_node(&repo_b_path).await?;
        let partitions = node_b
            .ctx
            .big_repo
            .list_partitions_for_peer(&"peer-partition-visibility".into())
            .await?;
        let drawer_partition_id = crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(
            &node_b.ctx.doc_drawer.document_id().clone(),
        );
        let drawer_partition = partitions
            .iter()
            .find(|summary| summary.partition_id == drawer_partition_id);
        assert!(
            drawer_partition.is_some(),
            "cloned repo should register drawer replicated partition on open: {partitions:?}"
        );
        let drawer_partition = drawer_partition.expect("checked above");
        assert!(
            drawer_partition.member_count >= 2,
            "drawer replicated partition should include drawer/app docs after sync boot: {drawer_partition:?}"
        );

        node_b.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bootstrap_ticket_in_tests_omits_relay_addresses() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        let temp_root = tempfile::tempdir()?;
        let repo_path = temp_root.path().join("repo-a");
        tokio::fs::create_dir_all(&repo_path).await?;

        let rtx = RepoCtx::init(
            &repo_path,
            RepoOpenOptions {
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        rtx.shutdown().await?;
        drop(rtx);

        let node = open_sync_node(&repo_path).await?;
        let ticket = node.sync_repo.get_ticket_url().await?;
        let bootstrap = crate::sync::resolve_bootstrap_from_url(&ticket).await?;
        let addr_debug = format!("{:?}", bootstrap.endpoint_addr);
        assert!(
            !addr_debug.contains("Relay("),
            "test bootstrap endpoint should not advertise relay addresses: {addr_debug}"
        );
        node.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_clone_sync_batch_100_docs_with_blobs() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
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
                branch_path: daybook_types::doc::BranchPath::from("main"),
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
                user_path: Some(daybook_types::doc::UserPath::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            });
        }
        let created = node_a.drawer.batch_add(args_batch).await?;
        assert_eq!(created.len(), 100);

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap.endpoint_id,
            Duration::from_secs(120),
        )
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
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
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
                branch_path: daybook_types::doc::BranchPath::from("main"),
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
                user_path: Some(daybook_types::doc::UserPath::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            });
        }
        node_a.drawer.batch_add(args_batch).await?;

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap.endpoint_id,
            Duration::from_secs(60),
        )
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
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");

        tokio::fs::create_dir_all(&repo_a_path).await?;
        let rtx = RepoCtx::init(
            &repo_a_path,
            RepoOpenOptions {
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        rtx.shutdown().await?;
        drop(rtx);

        let node_a = open_sync_node(&repo_a_path).await?;
        let mut created_doc_ids = Vec::new();
        for _ in 0..8 {
            let new_doc_id = node_a
                .drawer
                .add(daybook_types::doc::AddDocArgs {
                    branch_path: daybook_types::doc::BranchPath::from("main"),
                    facets: default(),
                    user_path: Some(daybook_types::doc::UserPath::from(
                        node_a.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
            created_doc_ids.push(new_doc_id);
        }

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        bootstrap_clone_repo_from_url_for_tests(&sync_url, &repo_b_path).await?;

        let node_b = open_sync_node(&repo_b_path).await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap.endpoint_id,
            Duration::from_secs(30),
        )
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

    fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) -> Res<()> {
        std::fs::create_dir_all(dst)
            .wrap_err_with(|| format!("failed creating copy destination {}", dst.display()))?;
        for entry in std::fs::read_dir(src)
            .wrap_err_with(|| format!("failed reading source directory {}", src.display()))?
        {
            let entry = entry
                .wrap_err_with(|| format!("failed reading directory entry in {}", src.display()))?;
            let file_type = entry.file_type().wrap_err_with(|| {
                format!(
                    "failed getting file type for source entry {}",
                    entry.path().display()
                )
            })?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_all(&src_path, &dst_path).wrap_err_with(|| {
                    format!(
                        "failed recursively copying directory {} -> {}",
                        src_path.display(),
                        dst_path.display()
                    )
                })?;
            } else if file_type.is_file() {
                match std::fs::copy(&src_path, &dst_path) {
                    Ok(_) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        // Source files such as sqlite WAL/SHM can disappear between read_dir and copy.
                        continue;
                    }
                    Err(err) => {
                        return Err(err).wrap_err_with(|| {
                            format!(
                                "failed copying file {} -> {}",
                                src_path.display(),
                                dst_path.display()
                            )
                        });
                    }
                }
            }
        }
        Ok(())
    }

    async fn init_and_copy_repo_pair(
        repo_a_path: &std::path::Path,
        repo_b_path: &std::path::Path,
    ) -> Res<()> {
        tokio::fs::create_dir_all(repo_a_path).await?;
        let rtx = RepoCtx::init(
            repo_a_path,
            RepoOpenOptions {
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        let source_repo_id = rtx.repo_id.clone();
        rtx.shutdown().await?;
        drop(rtx);

        copy_dir_all(repo_a_path, repo_b_path)?;
        let repo_b_sql = crate::app::SqlCtx::new(&format!(
            "sqlite://{}",
            repo_b_path.join("sqlite.db").display()
        ))
        .await?;
        crate::app::globals::set_repo_id(&repo_b_sql.db_pool, &source_repo_id).await?;
        let repo_b_repo_id = crate::app::globals::get_repo_id(&repo_b_sql.db_pool)
            .await?
            .ok_or_eyre("repo_b repo_id missing after copy")?;
        if repo_b_repo_id != source_repo_id {
            eyre::bail!(
                "copied repo_id mismatch (source={}, copied={})",
                source_repo_id,
                repo_b_repo_id
            );
        }
        crate::secrets::force_set_fallback_secret_for_tests(
            &repo_b_sql.db_pool,
            &source_repo_id,
            &iroh::SecretKey::generate(&mut rand::rng()),
        )
        .await?;
        Ok(())
    }

    async fn bootstrap_clone_repo_from_url_for_tests(
        source_url: &str,
        destination: &std::path::Path,
    ) -> Res<()> {
        if destination.exists() {
            let mut read_dir = tokio::fs::read_dir(destination).await?;
            if read_dir.next_entry().await?.is_some() {
                eyre::bail!(
                    "bootstrap clone destination must be empty: {}",
                    destination.display()
                );
            }
        } else {
            tokio::fs::create_dir_all(destination).await?;
        }

        let bootstrap = crate::sync::resolve_bootstrap_from_url(source_url).await?;
        let sqlite_path = destination.join("sqlite.db");
        let sql = crate::app::SqlCtx::new(&format!("sqlite://{}", sqlite_path.display())).await?;
        crate::app::globals::set_repo_id(&sql.db_pool, &bootstrap.repo_id).await?;
        let identity =
            crate::secrets::SecretRepo::load_or_init_identity(&sql.db_pool, &bootstrap.repo_id)
                .await?;
        let _repo_user_id = crate::repo::get_or_init_repo_user_id(&sql.db_pool).await?;

        let (big_repo, big_repo_stop) = am_utils_rs::BigRepo::boot(am_utils_rs::repo::Config {
            storage: am_utils_rs::repo::StorageConfig::Disk {
                path: destination.join("samod"),
                big_repo_sqlite_url: None,
            },
            peer_id: format!("/{}/{}", bootstrap.repo_id, identity.iroh_public_key),
        })
        .await?;
        crate::sync::connect_and_pull_required_docs_once(
            &big_repo,
            identity.iroh_secret_key.clone(),
            &bootstrap,
            Duration::from_secs(30),
        )
        .await?;
        crate::app::globals::set_init_state(
            &sql.db_pool,
            &crate::app::globals::InitState::Created {
                doc_id_app: bootstrap.app_doc_id,
                doc_id_drawer: bootstrap.drawer_doc_id,
            },
        )
        .await?;
        crate::repo::mark_repo_initialized(destination).await?;
        big_repo_stop.stop().await?;
        Ok(())
    }

    async fn open_sync_node(repo_root: &std::path::Path) -> Res<SyncTestNode> {
        let rtx = Arc::new(
            RepoCtx::open(
                repo_root,
                RepoOpenOptions {
                    ws_connector_url: None,
                },
                "test-device".into(),
            )
            .await?,
        );
        let blobs_repo =
            BlobsRepo::new(rtx.layout.blobs_root.clone(), rtx.local_user_path.clone()).await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            Arc::clone(&rtx.big_repo),
            Arc::clone(&blobs_repo),
            rtx.doc_app.document_id().clone(),
            daybook_types::doc::UserPath::from(rtx.local_user_path.clone()),
        )
        .await?;
        let (drawer_repo, drawer_stop) = DrawerRepo::load(
            Arc::clone(&rtx.big_repo),
            rtx.doc_drawer.document_id().clone(),
            daybook_types::doc::UserPath::from(rtx.local_user_path.clone()),
            rtx.layout.repo_root.join("local_state"),
            Arc::new(std::sync::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            Arc::new(std::sync::Mutex::new(
                crate::drawer::lru::KeyedLruPool::new(1000),
            )),
            Some(Arc::clone(&plugs_repo)),
        )
        .await?;
        let (config_repo, config_stop) = crate::config::ConfigRepo::load(
            Arc::clone(&rtx.big_repo),
            rtx.doc_app.document_id().clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPath::from(rtx.local_user_path.clone()),
            rtx.sql.db_pool.clone(),
        )
        .await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            SqliteLocalStateRepo::boot(rtx.layout.repo_root.join("local_state")).await?;
        let (doc_blobs_index_repo, doc_blobs_index_stop) = DocBlobsIndexRepo::boot(
            Arc::clone(&drawer_repo),
            Arc::clone(&sqlite_local_state_repo),
        )
        .await?;
        let (doc_blobs_bridge_cancel, doc_blobs_bridge_handle) =
            spawn_doc_blobs_index_bridge_for_tests(
                Arc::clone(&drawer_repo),
                Arc::clone(&doc_blobs_index_repo),
            );
        let progress_repo = ProgressRepo::boot(rtx.sql.db_pool.clone()).await?;
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
        let drawer_listener = drawer_repo.subscribe(SubscribeOpts::new(1024));
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel_for_task.cancelled() => break,
                    evt = drawer_listener.recv_lossy_async() => {
                        let Ok(evt) = evt else {
                            break;
                        };
                        match evt.as_ref() {
                            crate::drawer::DrawerEvent::DocDeleted { id, .. } => {
                                doc_blobs_index_repo.enqueue_delete(id.clone()).unwrap_or_log();
                            }
                            crate::drawer::DrawerEvent::DocAdded { id, entry, .. } => {
                                if let Some(heads) = entry.branches.get("main") {
                                    doc_blobs_index_repo.enqueue_upsert(id.clone(), heads.clone()).unwrap_or_log();
                                }
                            }
                            crate::drawer::DrawerEvent::DocUpdated { id, entry, .. } => {
                                if let Some(heads) = entry.branches.get("main") {
                                    doc_blobs_index_repo.enqueue_upsert(id.clone(), heads.clone()).unwrap_or_log();
                                }
                            }
                            crate::drawer::DrawerEvent::ListChanged { .. } => {}
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

    async fn wait_for_doc_presence_with_activity(
        node: &SyncTestNode,
        doc_id: &str,
        absolute_timeout: Duration,
    ) -> Res<()> {
        let last_activity = Arc::new(std::sync::Mutex::new(std::time::Instant::now()));
        let last_activity_for_wait = Arc::clone(&last_activity);
        let drawer_listener = node.drawer.subscribe(SubscribeOpts::new(1024));
        let sync_listener = node.sync_repo.subscribe(SubscribeOpts::new(2048));
        let progress_listener = node.progress_repo.subscribe(SubscribeOpts::new(4096));
        tokio::time::timeout(absolute_timeout, async {
            loop {
                let found = node
                    .drawer
                    .list_just_ids()
                    .await
                    .map(|(_, ids)| ids.iter().any(|id| id == doc_id))
                    .unwrap_or(false);
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
                            crate::drawer::DrawerEvent::ListChanged { .. }
                            | crate::drawer::DrawerEvent::DocAdded { .. }
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
        tokio::try_join!(
            target
                .sync_repo
                .wait_for_full_sync(std::slice::from_ref(&endpoint_id), timeout),
            wait_for_doc_set_parity(&source.drawer, &target.drawer, timeout),
        )?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wait_for_full_sync_succeeds_after_event_was_already_emitted() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");

        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let ticket_a = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap_ba = node_b.sync_repo.connect_url(&ticket_a).await?;

        wait_for_sync_convergence(
            &node_a,
            &node_b,
            bootstrap_ba.endpoint_id,
            Duration::from_secs(20),
        )
        .await?;

        tokio::time::sleep(Duration::from_secs(1)).await;

        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap_ba.endpoint_id),
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
                last_left = Some(left_heads.clone());
                last_right = Some(right_heads.clone());
                if left_heads == right_heads {
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
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");

        let temp_root = tempfile::tempdir()?;
        let blobs_repo = BlobsRepo::new(
            temp_root.path().join("blobs"),
            "/u/stress-test/dev-local".to_string(),
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
