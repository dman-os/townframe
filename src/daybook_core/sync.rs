use crate::interlude::*;

use iroh::EndpointId;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::blobs::BlobsRepo;
use crate::drawer::DrawerRepo;
use crate::index::DocBlobsIndexRepo;
use crate::progress::ProgressRepo;
use crate::repo::RepoCtx;

mod bootstrap;
mod full;
pub use bootstrap::*;

pub const IROH_DOC_URL_SCHEME: &str = "db+iroh-doc";

enum ActivePeerState {
    Connecting,
    Connected(am_utils_rs::RepoConnection),
}

pub struct IrohSyncRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    rcx: Arc<RepoCtx>,

    router: iroh::protocol::Router,

    config_repo: Arc<crate::config::ConfigRepo>,
    drawer_repo: Arc<DrawerRepo>,
    blobs_repo: Arc<BlobsRepo>,
    doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
    progress_repo: Option<Arc<ProgressRepo>>,

    iroh_docs: iroh_docs::api::DocsApi,
    iroh_blobs: iroh_blobs::api::Store,

    conn_end_signal_tx: tokio::sync::mpsc::UnboundedSender<am_utils_rs::ConnFinishSignal>,
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
        reason: samod::ConnFinishedReason,
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
    full_stop_token: full::StopToken,
}

impl IrohSyncRepoStopToken {
    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        self.router.shutdown().await?;
        self.full_stop_token.stop().await?;
        utils_rs::wait_on_handle_with_timeout(self.worker_handle, Duration::from_secs(2)).await?;
        Ok(())
    }
}

impl IrohSyncRepo {
    async fn collect_expected_blob_hashes_from_drawer(&self) -> Res<HashSet<String>> {
        use daybook_types::doc::{FacetKey, WellKnownFacet, WellKnownFacetTag};

        let blob_facet_key = FacetKey::from(WellKnownFacetTag::Blob);
        let mut expected = HashSet::new();
        for item in self.drawer_repo.list().await? {
            let Some(main_heads) = item.branches.get("main") else {
                continue;
            };
            let Some(doc) = self
                .drawer_repo
                .get_doc_with_facets_at_heads(
                    &item.doc_id,
                    main_heads,
                    Some(vec![blob_facet_key.clone()]),
                )
                .await?
            else {
                continue;
            };
            for facet_raw in doc.facets.values() {
                let facet = WellKnownFacet::from_json(facet_raw.clone(), WellKnownFacetTag::Blob)?;
                let WellKnownFacet::Blob(blob) = facet else {
                    continue;
                };
                let Some(urls) = blob.urls else {
                    continue;
                };
                for url in urls {
                    if let Some(hash) = parse_db_blob_hash(&url) {
                        expected.insert(hash);
                    }
                }
            }
        }
        Ok(expected)
    }

    pub async fn boot(
        rcx: Arc<RepoCtx>,
        drawer_repo: Arc<DrawerRepo>,
        config_repo: Arc<crate::config::ConfigRepo>,
        blobs_repo: Arc<BlobsRepo>,
        doc_blobs_index_repo: Arc<DocBlobsIndexRepo>,
        progress_repo: Option<Arc<ProgressRepo>>,
    ) -> Res<(Arc<Self>, IrohSyncRepoStopToken)> {
        let endpoint = iroh::Endpoint::builder()
            .secret_key(rcx.iroh_secret_key.clone())
            .bind()
            .await?;
        let blobs = blobs_repo.iroh_store();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let docs = iroh_docs::protocol::Docs::memory()
            .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
            .await
            .map_err(|err| ferr!("error booting iroh docs protocol: {err:?}"))?;

        let (incoming_conn_tx, incoming_conn_rx) = tokio::sync::mpsc::unbounded_channel();
        let (conn_end_tx, conn_end_rx) = tokio::sync::mpsc::unbounded_channel();

        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                AmCtx::SYNC_ALPN,
                am_utils_rs::iroh::IrohRepoProtocol {
                    cancel_token: default(),
                    acx: rcx.acx.clone(),
                    conn_tx: incoming_conn_tx,
                    end_signal_tx: conn_end_tx.clone(),
                },
            )
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs, None),
            )
            .accept(iroh_docs::ALPN, docs.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        let cancel_token = CancellationToken::new();
        config_repo
            .ensure_local_sync_device(router.endpoint().id(), &rcx.local_device_name)
            .await?;

        let (mut full_sync_handle, full_stop_token) = full::start_full_sync_worker(
            Arc::clone(&rcx),
            Arc::clone(&drawer_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&doc_blobs_index_repo),
            progress_repo.clone(),
            endpoint.clone(),
            cancel_token.child_token(),
        )
        .await?;
        let full_sync_rx = full_sync_handle.events_rx.take().expect("impossible");

        let repo = Arc::new(Self {
            rcx,
            router: router.clone(),
            config_repo,
            drawer_repo,
            blobs_repo,
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

    async fn machine_loop(
        &self,
        mut full_sync_rx: tokio::sync::broadcast::Receiver<full::FullSyncEvent>,
        mut incoming_conn_rx: tokio::sync::mpsc::UnboundedReceiver<am_utils_rs::RepoConnection>,
        mut conn_end_rx: tokio::sync::mpsc::UnboundedReceiver<am_utils_rs::ConnFinishSignal>,
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
                            error!("full sync worer is down");
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
        }
        eyre::Ok(())
    }

    async fn handle_incoming_am_conn(&self, conn: am_utils_rs::RepoConnection) -> Res<()> {
        let endpoint_id = conn
            .endpoint_id
            .expect("incoming iroh connection missing endpoint_id");

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

        if let Err(err) = self
            .full_sync_handle
            .set_connection(endpoint_id, conn.id)
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

    async fn handle_conn_end(&self, signal: am_utils_rs::ConnFinishSignal) -> Res<()> {
        let endpoint_id = self
            .active_samod_peers
            .read()
            .await
            .iter()
            .find_map(|(endpoint_id, state)| match state {
                ActivePeerState::Connected(conn) if conn.id == signal.conn_id => Some(*endpoint_id),
                ActivePeerState::Connected(_) | ActivePeerState::Connecting => None,
            })
            .expect("connection finished for unknown conn_id");

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
        conn: am_utils_rs::RepoConnection,
    ) -> Res<()> {
        let conn_id = conn.id;
        if let Err(err) = self
            .full_sync_handle
            .set_connection(endpoint_id, conn_id)
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
            .acx
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
        let events = [IrohSyncEvent::OutgoingConnection {
            endpoint_id,
            peer_id,
            conn_id,
        }];

        self.finalize_outgoing_connection(endpoint_id, conn).await?;
        self.registry.notify(events);
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
        if self.progress_repo.is_none() {
            eyre::bail!("wait_for_full_sync requires a progress-enabled IrohSyncRepo");
        }
        let target_peers: HashSet<EndpointId> = endpoint_ids.iter().cloned().collect();
        let mut remaining = target_peers.clone();
        if target_peers.is_empty() {
            return Ok(());
        }
        let listener = self
            .registry
            .subscribe::<IrohSyncEvent>(crate::repos::SubscribeOpts { capacity: 1024 });
        let doc_blobs_listener = self
            .doc_blobs_index_repo
            .subscribe(crate::repos::SubscribeOpts { capacity: 1024 });
        tokio::time::timeout(timeout, async {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            let listener = listener;
            let doc_blobs_listener = doc_blobs_listener;
            let mut active_downloads: HashSet<(EndpointId, String, String)> = HashSet::new();
            let mut quiet_since: Option<std::time::Instant> = None;
            loop {
                let targeted_active = active_downloads
                    .iter()
                    .filter(|(peer_id, _, _)| target_peers.contains(peer_id))
                    .count();
                if remaining.is_empty() && targeted_active == 0 {
                    let mut missing_blob = false;
                    let expected_hashes = self.collect_expected_blob_hashes_from_drawer().await?;
                    for hash in expected_hashes {
                        if !self.blobs_repo.has_hash(&hash).await? {
                            missing_blob = true;
                            break;
                        }
                    }
                    if missing_blob {
                        quiet_since = None;
                        continue;
                    }
                    let now = std::time::Instant::now();
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
                            }
                            IrohSyncEvent::StalePeer { endpoint_id } => {
                                if target_peers.contains(endpoint_id) {
                                    remaining.insert(*endpoint_id);
                                    quiet_since = None;
                                }
                            }
                            IrohSyncEvent::BlobDownloadStarted {
                                endpoint_id,
                                partition,
                                hash,
                                ..
                            } => {
                                if target_peers.contains(endpoint_id) {
                                    active_downloads
                                        .insert((*endpoint_id, partition.clone(), hash.clone()));
                                    quiet_since = None;
                                }
                            }
                            IrohSyncEvent::BlobDownloadFinished {
                                endpoint_id,
                                partition,
                                hash,
                                ..
                            } => {
                                active_downloads
                                    .remove(&(*endpoint_id, partition.clone(), hash.clone()));
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
                    _ = tick.tick() => {}
                }
            }
            eyre::Ok(())
        })
        .await
        .map_err(|_| {
            eyre::eyre!(
                "timed out waiting for full sync: remaining={remaining:?}"
            )
        })??;
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

fn parse_db_blob_hash(raw_url: &str) -> Option<String> {
    let parsed = url::Url::parse(raw_url).ok()?;
    if parsed.scheme() != crate::blobs::BLOB_SCHEME {
        return None;
    }
    if parsed.host_str().is_some() {
        return None;
    }
    let hash = parsed.path().trim_start_matches('/');
    if hash.is_empty() {
        return None;
    }
    Some(hash.to_string())
}

// fn parse_endpoint_id_base58(raw: &str) -> Res<iroh::EndpointId> {
//     let raw = utils_rs::hash::decode_base58_multibase(raw)
//         .wrap_err("peer_id endpoint id is not base58")?;
//     if raw.len() != 32 {
//         eyre::bail!("peer id decoded base58 is not len 32");
//     }
//     let mut buf = [0_u8; 32];
//     buf.copy_from_slice(&raw);
//     Ok(iroh::EndpointId::from_bytes(&buf)?)
// }

#[cfg(test)]
mod tests {
    use super::*;

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
            self.sync_stop.stop().await?;
            self.doc_blobs_bridge_cancel.cancel();
            if let Some(handle) = self.doc_blobs_bridge_handle.take() {
                utils_rs::wait_on_handle_with_timeout(handle, Duration::from_secs(2)).await?;
            }
            self.drawer_stop.stop().await?;
            self.plugs_stop.stop().await?;
            self.config_stop.stop().await?;
            self.doc_blobs_index_stop.stop().await?;
            self.sqlite_local_state_stop.stop().await?;
            if let Some(stop) = self.ctx.acx_stop.lock().await.take() {
                stop.stop().await?;
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_sync_between_copied_repos() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        info!("test=iroh_sync_between_copied_repos stage=setup");

        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        info!("test=iroh_sync_between_copied_repos stage=init_copy_repos");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        info!("test=iroh_sync_between_copied_repos stage=open_nodes");
        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        info!("test=iroh_sync_between_copied_repos stage=create_docs count=3");
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

        info!("test=iroh_sync_between_copied_repos stage=start_bootstrap");
        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;

        info!("test=iroh_sync_between_copied_repos stage=wait_full_sync");
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
                Duration::from_secs(20),
            )
            .await?;
        info!("test=iroh_sync_between_copied_repos stage=verify_doc_presence");
        for doc_id in &created_doc_ids {
            wait_for_doc_presence(&node_b.drawer, doc_id, Duration::from_secs(10)).await?;
        }

        info!("test=iroh_sync_between_copied_repos stage=verify_parity");
        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(20)).await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(
            ids_a, ids_b,
            "replica doc sets are not equal after full sync"
        );

        info!("test=iroh_sync_between_copied_repos stage=shutdown");
        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_live_sync_bidirectional_after_clone() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        info!("test=iroh_live_sync_bidirectional_after_clone stage=setup");

        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        info!("test=iroh_live_sync_bidirectional_after_clone stage=init_copy_repos");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        info!("test=iroh_live_sync_bidirectional_after_clone stage=open_nodes");
        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        info!("test=iroh_live_sync_bidirectional_after_clone stage=start_bootstrap");
        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        info!("test=iroh_live_sync_bidirectional_after_clone stage=wait_full_sync");
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
                Duration::from_secs(20),
            )
            .await?;

        info!("test=iroh_live_sync_bidirectional_after_clone stage=create_doc_on_a");
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
        wait_for_doc_presence(&node_b.drawer, &doc_on_a, Duration::from_secs(15)).await?;

        info!("test=iroh_live_sync_bidirectional_after_clone stage=create_doc_on_b");
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
        wait_for_doc_presence(&node_a.drawer, &doc_on_b, Duration::from_secs(15)).await?;

        info!("test=iroh_live_sync_bidirectional_after_clone stage=verify_parity");
        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(20)).await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(ids_a, ids_b, "live sync did not converge to equal doc sets");

        info!("test=iroh_live_sync_bidirectional_after_clone stage=shutdown");
        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_clone_sync_batch_100_docs_with_blobs() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=setup");

        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=init_copy_repos");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=open_nodes");
        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        let mut args_batch = Vec::new();
        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=create_docs count=100");
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
        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=docs_created count=100");

        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=start_bootstrap");
        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=wait_full_sync");
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
                Duration::from_secs(120),
            )
            .await?;

        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=verify_parity");
        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(120)).await?;
        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(
            ids_a, ids_b,
            "doc sets are not equal after 100-doc clone sync"
        );

        info!("test=iroh_clone_sync_batch_100_docs_with_blobs stage=shutdown");
        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn iroh_blob_sync_validates_bytes() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        std::env::set_var("DAYB_DISABLE_KEYRING", "1");
        info!("test=iroh_blob_sync_validates_bytes stage=setup");

        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        info!("test=iroh_blob_sync_validates_bytes stage=init_copy_repos");
        init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

        info!("test=iroh_blob_sync_validates_bytes stage=open_nodes");
        let node_a = open_sync_node(&repo_a_path).await?;
        let node_b = open_sync_node(&repo_b_path).await?;

        info!("test=iroh_blob_sync_validates_bytes stage=create_docs_with_blobs count=8");
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

        info!("test=iroh_blob_sync_validates_bytes stage=start_bootstrap");
        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        info!("test=iroh_blob_sync_validates_bytes stage=wait_full_sync");
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
                Duration::from_secs(60),
            )
            .await?;

        info!("test=iroh_blob_sync_validates_bytes stage=verify_blob_bytes count=8");
        for (hash, expected) in &blob_payloads {
            let got = tokio::fs::read(node_b.blobs_repo.get_path(hash).await?).await?;
            assert_eq!(
                got, *expected,
                "blob content mismatch after sync for hash={hash}"
            );
        }

        info!("test=iroh_blob_sync_validates_bytes stage=shutdown");
        node_b.stop().await?;
        node_a.stop().await?;
        Ok(())
    }

    fn copy_dir_all(src: &std::path::Path, dst: &std::path::Path) -> Res<()> {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_all(&src_path, &dst_path)?;
            } else if file_type.is_file() {
                if let Err(err) = std::fs::copy(&src_path, &dst_path) {
                    if err.kind() != std::io::ErrorKind::NotFound {
                        return Err(err.into());
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
        info!(
            "stage=init_and_copy_repo_pair repo_a={} repo_b={}",
            repo_a_path.display(),
            repo_b_path.display()
        );
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
        info!("stage=init_and_copy_repo_pair source_repo_id={source_repo_id}");
        if let Some(stop) = rtx.acx_stop.lock().await.take() {
            stop.stop().await?;
        }
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
        info!("stage=init_and_copy_repo_pair copied_repo_id_verified");
        crate::secrets::force_set_fallback_secret_for_tests(
            &repo_b_sql.db_pool,
            &source_repo_id,
            &iroh::SecretKey::generate(&mut rand::rng()),
        )
        .await?;
        info!("stage=init_and_copy_repo_pair done");
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
            rtx.acx.clone(),
            Arc::clone(&blobs_repo),
            rtx.doc_app.document_id().clone(),
            rtx.local_actor_id.clone(),
        )
        .await?;
        let (drawer_repo, drawer_stop) = DrawerRepo::load(
            rtx.acx.clone(),
            rtx.doc_drawer.document_id().clone(),
            rtx.local_actor_id.clone(),
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
            rtx.acx.clone(),
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
        let (sync_repo, sync_stop) = IrohSyncRepo::boot(
            Arc::clone(&rtx),
            Arc::clone(&drawer_repo),
            Arc::clone(&config_repo),
            Arc::clone(&blobs_repo),
            Arc::clone(&doc_blobs_index_repo),
            Some(ProgressRepo::boot(rtx.sql.db_pool.clone()).await?),
        )
        .await?;

        Ok(SyncTestNode {
            ctx: rtx,
            blobs_repo,
            drawer: drawer_repo,
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

    async fn wait_for_doc_presence(
        drawer: &DrawerRepo,
        doc_id: &str,
        timeout: Duration,
    ) -> Res<()> {
        tokio::time::timeout(timeout, async {
            loop {
                let found = drawer
                    .list_just_ids()
                    .await
                    .map(|(_, ids)| ids.iter().any(|id| id == doc_id))
                    .unwrap_or(false);
                if found {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for document presence: {doc_id}"))?;
        Ok(())
    }

    async fn wait_for_doc_set_parity(
        left: &DrawerRepo,
        right: &DrawerRepo,
        timeout: Duration,
    ) -> Res<()> {
        tokio::time::timeout(timeout, async {
            loop {
                let l = list_doc_ids(left).await;
                let r = list_doc_ids(right).await;
                if let (Ok(lset), Ok(rset)) = (l, r) {
                    if lset == rset {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for drawer doc-set parity"))?;
        Ok(())
    }
}
