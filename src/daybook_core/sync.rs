use crate::interlude::*;

use iroh::EndpointId;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::drawer::DrawerRepo;
use crate::repo::RepoCtx;

mod bootstrap;
// mod full;
mod full2;
pub use bootstrap::*;

pub const IROH_DOC_URL_SCHEME: &str = "db+iroh-doc";

pub struct IrohSyncRepo {
    pub registry: Arc<crate::repos::ListenersRegistry>,
    cancel_token: CancellationToken,
    rcx: Arc<RepoCtx>,

    router: iroh::protocol::Router,

    config_repo: Arc<crate::config::ConfigRepo>,

    iroh_docs: iroh_docs::api::DocsApi,
    iroh_blobs: iroh_blobs::api::Store,

    conn_end_signal_tx: tokio::sync::mpsc::UnboundedSender<am_utils_rs::ConnFinishSignal>,
    active_samod_peers: tokio::sync::RwLock<HashMap<EndpointId, (am_utils_rs::RepoConnection,)>>,
    full_sync_handle: full2::WorkerHandle,
    // bootstrap_docs: tokio::sync::Mutex<Vec<iroh_docs::api::Doc>>,
    // active_endpoint_ids: tokio::sync::RwLock<HashMap<String, ()>>,
}

#[derive(Debug, Clone)]
pub enum IrohSyncEvent {
    IncomingConnetion {
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
    DocSyncUpdates {
        updates: Arc<[am_utils_rs::peers::DocPeerSyncUpdate]>,
    },
    PeerFullySynced {
        endpoint_id: EndpointId,
        doc_count: usize,
    },
    DocSyncedWithPeer {
        endpoint_id: EndpointId,
        doc_id: DocumentId,
    },
    StalePeer {
        endpoint_id: EndpointId,
    },
}

pub struct IrohSyncRepoStopToken {
    cancel_token: CancellationToken,
    worker_handle: JoinHandle<()>,
    router: iroh::protocol::Router,
    full_stop_token: full2::StopToken,
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
    pub async fn boot(
        rcx: Arc<RepoCtx>,
        drawer_repo: Arc<DrawerRepo>,
        config_repo: Arc<crate::config::ConfigRepo>,
    ) -> Res<(Arc<Self>, IrohSyncRepoStopToken)> {
        let endpoint = iroh::Endpoint::builder()
            .secret_key(rcx.iroh_secret_key.clone())
            .bind()
            .await?;
        let blobs = (*iroh_blobs::store::mem::MemStore::new()).clone();
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

        let (mut full_sync_handle, full_stop_token) =
            full2::start_full_sync_worker(rcx.clone(), drawer_repo, cancel_token.child_token())
                .await?;
        let full_sync_rx = full_sync_handle.events_rx.take().expect("impossible");

        let repo = Arc::new(Self {
            rcx,
            router: router.clone(),
            config_repo,
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
        mut full_sync_rx: tokio::sync::broadcast::Receiver<full2::FullSyncEvent>,
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
            futures::stream::iter(
                active_samod_peers
                    .drain()
                    .map(|(_endpoint_id, (conn,))| conn.stop()),
            )
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
            .clone()
            .expect("incoming iroh connection missing endpoint_id");

        let events = [IrohSyncEvent::IncomingConnetion {
            endpoint_id: endpoint_id.clone(),
            peer_id: conn.peer_id.clone(),
            conn_id: conn.id,
        }];

        self.full_sync_handle
            .set_connection(endpoint_id.clone(), conn.id)
            .await?;

        self.active_samod_peers
            .write()
            .await
            .insert(endpoint_id, (conn,));

        self.registry.notify(events);

        Ok(())
    }

    async fn handle_conn_end(&self, signal: am_utils_rs::ConnFinishSignal) -> Res<()> {
        let endpoint_id = self
            .active_samod_peers
            .read()
            .await
            .iter()
            .find_map(|(endpoint_id, (conn,))| {
                info!(?endpoint_id, "XXX");
                if conn.id == signal.conn_id {
                    Some(endpoint_id.clone())
                } else {
                    None
                }
            })
            .expect("connection finished for unknown conn_id");

        let events = [IrohSyncEvent::ConnectionClosed {
            endpoint_id: endpoint_id.clone(),
            reason: signal.reason,
        }];

        self.full_sync_handle
            .del_connection(endpoint_id.clone())
            .await?;

        let old = self.active_samod_peers.write().await.remove(&endpoint_id);
        assert!(old.is_some(), "fishy");

        self.registry.notify(events);
        Ok(())
    }

    async fn handle_full_sync_evt(&self, evt: full2::FullSyncEvent) -> Res<()> {
        match evt {
            full2::FullSyncEvent::PeerFullSynced {
                endpoint_id,
                doc_count,
            } => self.registry.notify([IrohSyncEvent::PeerFullySynced {
                endpoint_id,
                doc_count,
            }]),
            full2::FullSyncEvent::DocSyncedWithPeer {
                endpoint_id,
                doc_id,
            } => self.registry.notify([IrohSyncEvent::DocSyncedWithPeer {
                endpoint_id,
                doc_id,
            }]),
            full2::FullSyncEvent::StalePeer { endpoint_id } => self
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

    pub async fn connect_endpoint_addr(&self, endpoint_addr: iroh::EndpointAddr) -> Res<()> {
        self.ensure_repo_live()?;

        if self
            .active_samod_peers
            .read()
            .await
            .contains_key(&endpoint_addr.id)
        {
            return Ok(());
        }

        if endpoint_addr.id == self.router.endpoint().id() {
            eyre::bail!("connecting to ourself is not supported");
        }
        let endpoint_id = endpoint_addr.id.clone();
        let conn = self
            .rcx
            .acx
            .spawn_connection_iroh(
                self.router.endpoint(),
                endpoint_addr,
                Some(self.conn_end_signal_tx.clone()),
            )
            .await?;
        let peer_id = conn.peer_info.peer_id.as_str().into();
        let events = [IrohSyncEvent::OutgoingConnection {
            endpoint_id: endpoint_id.clone(),
            peer_id,
            conn_id: conn.id,
        }];

        self.full_sync_handle
            .set_connection(endpoint_id.clone(), conn.id)
            .await?;

        self.active_samod_peers
            .write()
            .await
            .insert(endpoint_id, (conn,));
        self.registry.notify(events);
        Ok(())
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
        self.ensure_repo_live()?;
        let mut remaining: HashSet<EndpointId> = endpoint_ids.iter().cloned().collect();
        if remaining.is_empty() {
            return Ok(());
        }
        for endpoint_id in &remaining {
            self.full_sync_handle
                .add_full_sync_peer(endpoint_id.clone())
                .await?;
        }
        let listener = self
            .registry
            .subscribe::<IrohSyncEvent>(crate::repos::SubscribeOpts { capacity: 1024 });
        tokio::time::timeout(timeout, async {
            while !remaining.is_empty() {
                let event = listener.recv_async().await.map_err(|err| match err {
                    crate::repos::RecvError::Closed => eyre::eyre!("sync listener closed"),
                    crate::repos::RecvError::Dropped { dropped_count } => {
                        eyre::eyre!("sync listener dropped events: dropped_count={dropped_count}")
                    }
                })?;
                if let IrohSyncEvent::PeerFullySynced { endpoint_id, .. } = event.as_ref() {
                    remaining.remove(endpoint_id);
                }
            }
            eyre::Ok(())
        })
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for full sync: remaining={remaining:?}"))??;
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
    use crate::plugs::PlugsRepo;
    use crate::repo::{RepoCtx, RepoOpenOptions};

    struct SyncTestNode {
        ctx: Arc<RepoCtx>,
        drawer: Arc<DrawerRepo>,
        drawer_stop: crate::repos::RepoStopToken,
        _plugs_repo: Arc<PlugsRepo>,
        plugs_stop: crate::repos::RepoStopToken,
        config_stop: crate::repos::RepoStopToken,
        sync_repo: Arc<IrohSyncRepo>,
        sync_stop: IrohSyncRepoStopToken,
    }

    impl SyncTestNode {
        async fn stop(self) -> Res<()> {
            self.sync_stop.stop().await?;
            self.drawer_stop.stop().await?;
            self.plugs_stop.stop().await?;
            self.config_stop.stop().await?;
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

        let gcx = crate::app::GlobalCtx::new().await?;
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        tokio::fs::create_dir_all(&repo_a_path).await?;

        let rtx = RepoCtx::open(
            &gcx,
            &repo_a_path,
            RepoOpenOptions {
                ensure_initialized: true,
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        if let Some(stop) = rtx.acx_stop.lock().await.take() {
            stop.stop().await?;
        }
        drop(rtx);

        copy_dir_all(&repo_a_path, &repo_b_path)?;
        let repo_b_sql = crate::app::SqlCtx::new(&format!(
            "sqlite://{}",
            repo_b_path.join("sqlite.db").display()
        ))
        .await?;
        let repo_id = crate::app::globals::get_or_init_repo_id(&repo_b_sql.db_pool).await?;
        crate::secrets::force_set_fallback_secret_for_tests(
            &repo_b_sql.db_pool,
            &repo_id,
            &iroh::SecretKey::generate(&mut rand::rng()),
        )
        .await?;

        let node_a = open_sync_node(&gcx, &repo_a_path).await?;
        let node_b = open_sync_node(&gcx, &repo_b_path).await?;

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

        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
                Duration::from_secs(20),
            )
            .await?;
        for doc_id in &created_doc_ids {
            wait_for_doc_presence(&node_b.drawer, doc_id, Duration::from_secs(10)).await?;
        }

        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(20)).await?;

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

        let gcx = crate::app::GlobalCtx::new().await?;
        let temp_root = tempfile::tempdir()?;
        let repo_a_path = temp_root.path().join("repo-a");
        let repo_b_path = temp_root.path().join("repo-b");
        tokio::fs::create_dir_all(&repo_a_path).await?;

        let rtx = RepoCtx::open(
            &gcx,
            &repo_a_path,
            RepoOpenOptions {
                ensure_initialized: true,
                ws_connector_url: None,
            },
            "test-device".into(),
        )
        .await?;
        if let Some(stop) = rtx.acx_stop.lock().await.take() {
            stop.stop().await?;
        }
        drop(rtx);

        copy_dir_all(&repo_a_path, &repo_b_path)?;
        let repo_b_sql = crate::app::SqlCtx::new(&format!(
            "sqlite://{}",
            repo_b_path.join("sqlite.db").display()
        ))
        .await?;
        let repo_id = crate::app::globals::get_or_init_repo_id(&repo_b_sql.db_pool).await?;
        crate::secrets::force_set_fallback_secret_for_tests(
            &repo_b_sql.db_pool,
            &repo_id,
            &iroh::SecretKey::generate(&mut rand::rng()),
        )
        .await?;

        let node_a = open_sync_node(&gcx, &repo_a_path).await?;
        let node_b = open_sync_node(&gcx, &repo_b_path).await?;

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        node_b
            .sync_repo
            .wait_for_full_sync(
                std::slice::from_ref(&bootstrap.endpoint_id),
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
        wait_for_doc_presence(&node_b.drawer, &doc_on_a, Duration::from_secs(15)).await?;

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

        wait_for_doc_set_parity(&node_a.drawer, &node_b.drawer, Duration::from_secs(20)).await?;

        let ids_a = list_doc_ids(&node_a.drawer).await?;
        let ids_b = list_doc_ids(&node_b.drawer).await?;
        assert_eq!(ids_a, ids_b, "live sync did not converge to equal doc sets");

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

    async fn open_sync_node(
        gcx: &crate::app::GlobalCtx,
        repo_root: &std::path::Path,
    ) -> Res<SyncTestNode> {
        let rtx = Arc::new(
            RepoCtx::open(
                gcx,
                repo_root,
                RepoOpenOptions {
                    ensure_initialized: false,
                    ws_connector_url: None,
                },
                "test-device".into(),
            )
            .await?,
        );

        let blobs_repo = BlobsRepo::new(rtx.layout.blobs_root.clone()).await?;
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
        let (sync_repo, sync_stop) = IrohSyncRepo::boot(
            Arc::clone(&rtx),
            Arc::clone(&drawer_repo),
            Arc::clone(&config_repo),
        )
        .await?;

        Ok(SyncTestNode {
            ctx: rtx,
            drawer: drawer_repo,
            drawer_stop,
            _plugs_repo: plugs_repo,
            plugs_stop,
            config_stop,
            sync_repo,
            sync_stop,
        })
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
