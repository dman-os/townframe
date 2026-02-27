use std::str::FromStr;

use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::store::Query;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::interlude::*;

mod full;

pub const IROH_DOC_URL_SCHEME: &str = "db+iroh-doc";
const BOOTSTRAP_KEY_REPO_ID: &[u8] = b"repo_id";
const BOOTSTRAP_KEY_APP_DOC_ID: &[u8] = b"app_doc_id";
const BOOTSTRAP_KEY_DRAWER_DOC_ID: &[u8] = b"drawer_doc_id";
const BOOTSTRAP_KEY_DEVICE_NAME: &[u8] = b"device_name";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncBootstrapState {
    pub endpoint_addr: iroh::EndpointAddr,
    pub endpoint_id: String,
    pub repo_id: String,
    pub app_doc_id: DocumentId,
    pub drawer_doc_id: DocumentId,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone)]
pub enum IrohSyncEvent {
    IncomingConnetion {
        peer_info: samod::PeerInfo,
    },
    DocSyncUpdates {
        updates: Arc<[am_utils_rs::peers::DocPeerSyncUpdate]>,
    },
}

pub struct IrohSyncRepo {
    acx: AmCtx,
    router: iroh::protocol::Router,
    config_repo: Arc<crate::config::ConfigRepo>,
    observer: Arc<am_utils_rs::peers::PeerSyncObserverHandle>,
    iroh_docs: iroh_docs::api::DocsApi,
    iroh_blobs: iroh_blobs::api::Store,
    bootstrap_docs: tokio::sync::Mutex<Vec<iroh_docs::api::Doc>>,
    cancel_token: CancellationToken,
    local_repo_id: String,
    app_doc_id: DocumentId,
    drawer_doc_id: DocumentId,
    local_device_name: String,
    active_endpoint_ids: tokio::sync::RwLock<HashMap<String, ()>>,
    pub registry: Arc<crate::repos::ListenersRegistry>,
}

pub struct IrohSyncRepoStopToken {
    cancel_token: CancellationToken,
    worker_handle: Option<JoinHandle<()>>,
    observer_stop: Option<am_utils_rs::peers::PeerSyncObserverStopToken>,
    router: iroh::protocol::Router,
}

impl IrohSyncRepoStopToken {
    pub async fn stop(mut self) -> Res<()> {
        self.cancel_token.cancel();
        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }
        if let Some(stop) = self.observer_stop.take() {
            stop.stop().await?;
        }
        self.router.shutdown().await?;
        Ok(())
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

impl IrohSyncRepo {
    pub async fn boot(
        acx: AmCtx,
        config_repo: Arc<crate::config::ConfigRepo>,
        sec_key: iroh::SecretKey,
        app_doc_id: DocumentId,
        drawer_doc_id: DocumentId,
        local_device_name: String,
    ) -> Res<(Arc<Self>, IrohSyncRepoStopToken)> {
        let endpoint = iroh::Endpoint::builder().secret_key(sec_key).bind().await?;
        let blobs = (*iroh_blobs::store::mem::MemStore::new()).clone();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let docs = iroh_docs::protocol::Docs::memory()
            .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
            .await
            .map_err(|err| ferr!("error booting iroh docs protocol: {err:?}"))?;

        let (incoming_conn_tx, mut incoming_conn_rx) = tokio::sync::mpsc::unbounded_channel();
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                AmCtx::SYNC_ALPN,
                am_utils_rs::iroh::IrohRepoProtocol {
                    acx: acx.clone(),
                    conn_tx: incoming_conn_tx,
                },
            )
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs, None),
            )
            .accept(iroh_docs::ALPN, docs.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        let (observer, observer_stop) = acx.spawn_peer_sync_observer();
        let main_cancel_token = CancellationToken::new();
        let local_repo_id = config_repo.get_repo_id().await?;
        let endpoint_id = router.endpoint().id().to_string();
        config_repo
            .ensure_local_sync_device(endpoint_id, &local_device_name)
            .await?;

        let repo = Arc::new(Self {
            acx,
            router: router.clone(),
            config_repo,
            observer: Arc::clone(&observer),
            iroh_docs: docs.api().clone(),
            iroh_blobs: blobs,
            bootstrap_docs: tokio::sync::Mutex::new(Vec::new()),
            cancel_token: main_cancel_token.child_token(),
            local_repo_id,
            app_doc_id,
            drawer_doc_id,
            local_device_name,
            active_endpoint_ids: tokio::sync::RwLock::new(HashMap::new()),
            registry: crate::repos::ListenersRegistry::new(),
        });

        let worker_handle = tokio::spawn({
            let repo = Arc::clone(&repo);
            let cancel_token = main_cancel_token.clone();
            async move {
                let mut updates_rx = repo.observer.subscribe();
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_token.cancelled() => {
                            break;
                        }
                        recv = incoming_conn_rx.recv() => {
                            let conn = recv.expect(ERROR_CHANNEL);
                            let endpoint_id = endpoint_id_from_peer_id(&conn.peer_info.peer_id)
                                .expect("peer id must include endpoint id suffix");
                            repo.mark_endpoint_connected(endpoint_id).await;
                            repo.registry.notify([IrohSyncEvent::IncomingConnetion {
                                peer_info: conn.peer_info
                            }]);
                        }
                        recv = updates_rx.recv() => {
                            match recv {
                                Ok(updates) => {
                                    repo.refresh_active_endpoint_ids_from_snapshot().await.unwrap();
                                    repo.registry.notify([IrohSyncEvent::DocSyncUpdates { updates }]);
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!(?skipped, "sync observer lagged");
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok((
            repo,
            IrohSyncRepoStopToken {
                cancel_token: main_cancel_token,
                worker_handle: Some(worker_handle),
                observer_stop: Some(observer_stop),
                router,
            },
        ))
    }

    pub async fn get_ticket_url(&self) -> Res<String> {
        let author = self
            .iroh_docs
            .author_default()
            .await
            .map_err(|err| ferr!("error getting default docs author: {err:?}"))?;
        let doc = self
            .iroh_docs
            .create()
            .await
            .map_err(|err| ferr!("error creating bootstrap doc: {err:?}"))?;
        doc.set_bytes(
            author,
            BOOTSTRAP_KEY_REPO_ID.to_vec(),
            self.local_repo_id.as_bytes().to_vec(),
        )
        .await
        .map_err(|err| ferr!("error writing repo_id bootstrap key: {err:?}"))?;
        doc.set_bytes(
            author,
            BOOTSTRAP_KEY_APP_DOC_ID.to_vec(),
            self.app_doc_id.to_string().as_bytes().to_vec(),
        )
        .await
        .map_err(|err| ferr!("error writing app_doc_id bootstrap key: {err:?}"))?;
        doc.set_bytes(
            author,
            BOOTSTRAP_KEY_DRAWER_DOC_ID.to_vec(),
            self.drawer_doc_id.to_string().as_bytes().to_vec(),
        )
        .await
        .map_err(|err| ferr!("error writing drawer_doc_id bootstrap key: {err:?}"))?;
        doc.set_bytes(
            author,
            BOOTSTRAP_KEY_DEVICE_NAME.to_vec(),
            self.local_device_name.as_bytes().to_vec(),
        )
        .await
        .map_err(|err| ferr!("error writing device_name bootstrap key: {err:?}"))?;
        let ticket = doc
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|err| ferr!("error sharing bootstrap doc: {err:?}"))?;
        doc.start_sync(vec![])
            .await
            .map_err(|err| ferr!("error starting bootstrap doc sync: {err:?}"))?;
        self.bootstrap_docs.lock().await.push(doc);
        Ok(format_iroh_doc_ticket_url(&ticket))
    }

    pub async fn connect_url(&self, iroh_doc_url: &str) -> Res<SyncBootstrapState> {
        let bootstrap =
            resolve_bootstrap_with_docs(&self.iroh_docs, &self.iroh_blobs, iroh_doc_url).await?;
        self.ensure_repo_allowed(&bootstrap).await?;
        self.connect_endpoint_addr(bootstrap.endpoint_addr.clone())
            .await?;
        Ok(bootstrap)
    }

    pub async fn connect_endpoint_addr(&self, endpoint_addr: iroh::EndpointAddr) -> Res<()> {
        if endpoint_addr.id == self.router.endpoint().id() {
            eyre::bail!("Connecting to ourself is not supported");
        }
        let endpoint_id = endpoint_addr.id.to_string();
        let _conn = self
            .acx
            .spawn_connection_iroh(self.router.endpoint(), endpoint_addr)
            .await?;
        self.mark_endpoint_connected(endpoint_id).await;
        Ok(())
    }

    pub async fn connect_known_devices_once(&self) -> Res<()> {
        let devices = self.config_repo.list_known_sync_devices().await?;
        let snapshot = self.observer.snapshot().await?;
        let local_endpoint_id = self.router.endpoint().id().to_string();
        for device in devices {
            if device.endpoint_id == local_endpoint_id {
                continue;
            }
            if is_endpoint_already_connected(&snapshot, &device.endpoint_id) {
                continue;
            }
            let endpoint_id = iroh::PublicKey::from_str(&device.endpoint_id)
                .wrap_err_with(|| format!("invalid endpoint id {}", device.endpoint_id))?;
            self.connect_endpoint_addr(iroh::EndpointAddr::new(endpoint_id))
                .await?;
        }
        Ok(())
    }

    pub async fn full_sync_peers(
        &self,
        drawer_repo: Arc<crate::drawer::DrawerRepo>,
        endpoint_ids: &[String],
    ) -> Res<(full::FullSyncHandle, full::FullSyncStopToken)> {
        let target_ids = endpoint_ids.iter().cloned().collect::<HashSet<_>>();
        if target_ids.is_empty() {
            eyre::bail!("at least one endpoint id must be provided for full_sync_peers");
        }

        self.refresh_active_endpoint_ids_from_snapshot().await?;
        self.ensure_endpoints_active(&target_ids).await?;

        let snapshot = self.observer.snapshot().await?;
        let updates_rx = self.observer.subscribe();
        Ok(full::spawn_full_sync_worker(
            drawer_repo,
            self.acx.clone(),
            snapshot,
            updates_rx,
            target_ids,
        ))
    }

    async fn mark_endpoint_connected(&self, endpoint_id: String) {
        self.active_endpoint_ids
            .write()
            .await
            .insert(endpoint_id, ());
    }

    async fn refresh_active_endpoint_ids_from_snapshot(&self) -> Res<()> {
        let snapshot = self.observer.snapshot().await?;
        let mut next = HashMap::new();
        for connection in snapshot.connections {
            let samod::ConnectionState::Connected { their_peer_id } = connection.state else {
                continue;
            };
            let endpoint_id = endpoint_id_from_peer_id(&their_peer_id)
                .expect("peer id must include endpoint id suffix");
            next.insert(endpoint_id, ());
        }
        *self.active_endpoint_ids.write().await = next;
        Ok(())
    }

    async fn ensure_endpoints_active(&self, endpoint_ids: &HashSet<String>) -> Res<()> {
        let active = self.active_endpoint_ids.read().await;
        let mut missing = endpoint_ids
            .iter()
            .filter(|endpoint_id| !active.contains_key(*endpoint_id))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            missing.sort();
            eyre::bail!(
                "requested endpoints are not connected: {}",
                missing.join(", ")
            );
        }
        Ok(())
    }

    async fn ensure_repo_allowed(&self, bootstrap: &SyncBootstrapState) -> Res<()> {
        if bootstrap.repo_id != self.local_repo_id {
            eyre::bail!(
                "bootstrap repo_id mismatch (local={}, remote={})",
                self.local_repo_id,
                bootstrap.repo_id
            );
        }
        Ok(())
    }
}

pub fn format_iroh_doc_ticket_url(ticket: &iroh_docs::DocTicket) -> String {
    format!("{IROH_DOC_URL_SCHEME}:{ticket}")
}

pub fn parse_iroh_doc_ticket_url(input: &str) -> Res<iroh_docs::DocTicket> {
    let payload = input
        .strip_prefix(&format!("{IROH_DOC_URL_SCHEME}:"))
        .ok_or_eyre("invalid sync url scheme, expected db+iroh-doc:<ticket>")?;
    iroh_docs::DocTicket::from_str(payload).wrap_err("invalid iroh docs ticket")
}

pub async fn resolve_bootstrap_from_url(iroh_doc_url: &str) -> Res<SyncBootstrapState> {
    let session = TempDocsSession::boot(None).await?;
    let out = resolve_bootstrap_with_docs(&session.docs, &session.blobs, iroh_doc_url).await;
    session.shutdown().await?;
    out
}

pub async fn pull_required_docs_once(
    acx: &AmCtx,
    app_doc_id: &DocumentId,
    drawer_doc_id: &DocumentId,
    timeout: std::time::Duration,
) -> Res<()> {
    let app_doc_id = app_doc_id.clone();
    let drawer_doc_id = drawer_doc_id.clone();
    tokio::time::timeout(timeout, async move {
        loop {
            let app = acx.find_doc(&app_doc_id).await?;
            let drawer = acx.find_doc(&drawer_doc_id).await?;
            if app.is_some() && drawer.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        Ok::<(), eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("timed out waiting for remote docs during clone"))??;

    Ok(())
}

async fn resolve_bootstrap_with_docs(
    docs: &iroh_docs::api::DocsApi,
    blobs: &iroh_blobs::api::Store,
    iroh_doc_url: &str,
) -> Res<SyncBootstrapState> {
    let ticket = parse_iroh_doc_ticket_url(iroh_doc_url)?;
    let endpoint_addr = ticket
        .nodes
        .first()
        .cloned()
        .ok_or_eyre("iroh docs ticket is missing endpoint addresses")?;
    let doc = docs
        .import(ticket.clone())
        .await
        .map_err(|err| ferr!("error importing bootstrap doc ticket: {err:?}"))?;
    doc.start_sync(ticket.nodes.clone())
        .await
        .map_err(|err| ferr!("error starting bootstrap doc sync: {err:?}"))?;

    let timeout_at = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let repo_id = read_bootstrap_key(&doc, blobs, BOOTSTRAP_KEY_REPO_ID).await?;
        let app_doc_id = read_bootstrap_key(&doc, blobs, BOOTSTRAP_KEY_APP_DOC_ID).await?;
        let drawer_doc_id = read_bootstrap_key(&doc, blobs, BOOTSTRAP_KEY_DRAWER_DOC_ID).await?;
        let device_name = read_bootstrap_key(&doc, blobs, BOOTSTRAP_KEY_DEVICE_NAME).await?;
        if let (Some(repo_id), Some(app_doc_id), Some(drawer_doc_id)) =
            (repo_id, app_doc_id, drawer_doc_id)
        {
            let app_doc_id =
                DocumentId::from_str(&app_doc_id).wrap_err("invalid app_doc_id in bootstrap")?;
            let drawer_doc_id = DocumentId::from_str(&drawer_doc_id)
                .wrap_err("invalid drawer_doc_id in bootstrap")?;
            let endpoint_id = endpoint_addr.id.to_string();
            // FIXME: let's warn on error at least
            let _ = doc.leave().await;
            return Ok(SyncBootstrapState {
                endpoint_addr,
                endpoint_id,
                repo_id,
                app_doc_id,
                drawer_doc_id,
                device_name,
            });
        }
        if tokio::time::Instant::now() >= timeout_at {
            eyre::bail!("timed out waiting for bootstrap state from iroh docs");
        }
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }
}

async fn read_bootstrap_key(
    doc: &iroh_docs::api::Doc,
    blobs: &iroh_blobs::api::Store,
    key: &[u8],
) -> Res<Option<String>> {
    let Some(entry) = doc
        .get_one(Query::key_exact(key))
        .await
        .map_err(|err| ferr!("error reading bootstrap key: {err:?}"))?
    else {
        return Ok(None);
    };
    let bytes = match blobs.get_bytes(entry.content_hash()).await {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    Ok(Some(
        std::str::from_utf8(&bytes)
            .wrap_err("bootstrap key has invalid utf8 value")?
            .to_string(),
    ))
}

fn is_endpoint_already_connected(
    snapshot: &am_utils_rs::peers::PeerSyncSnapshot,
    endpoint_id: &str,
) -> bool {
    snapshot.connections.iter().any(|connection| {
        let samod::ConnectionState::Connected { their_peer_id } = &connection.state else {
            return false;
        };
        endpoint_id_from_peer_id(their_peer_id)
            .as_ref()
            .map(|suffix| suffix == endpoint_id)
            .unwrap_or(false)
    })
}

fn endpoint_id_from_peer_id(peer_id: &samod::PeerId) -> Option<String> {
    peer_id.to_string().rsplit('/').next().map(str::to_string)
}

struct TempDocsSession {
    router: iroh::protocol::Router,
    docs: iroh_docs::api::DocsApi,
    blobs: iroh_blobs::api::Store,
}

impl TempDocsSession {
    async fn boot(secret_key: Option<iroh::SecretKey>) -> Res<Self> {
        let mut endpoint_builder = iroh::Endpoint::builder();
        if let Some(secret_key) = secret_key {
            endpoint_builder = endpoint_builder.secret_key(secret_key);
        }
        let endpoint = endpoint_builder.bind().await?;
        let blobs = (*iroh_blobs::store::mem::MemStore::new()).clone();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
        let docs = iroh_docs::protocol::Docs::memory()
            .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
            .await
            .map_err(|err| ferr!("error booting temporary docs protocol: {err:?}"))?;
        let router = iroh::protocol::Router::builder(endpoint)
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs, None),
            )
            .accept(iroh_docs::ALPN, docs.clone())
            .accept(iroh_gossip::ALPN, gossip)
            .spawn();
        Ok(Self {
            router,
            docs: docs.api().clone(),
            blobs,
        })
    }

    async fn shutdown(self) -> Res<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

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

        let init_ctx = RepoCtx::open(
            &gcx,
            &repo_a_path,
            RepoOpenOptions {
                ensure_initialized: true,
                ws_connector_url: None,
            },
        )
        .await?;
        if let Some(stop) = init_ctx.acx_stop.lock().await.take() {
            stop.stop().await?;
        }
        drop(init_ctx);

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

        let sync_url = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap = node_b.sync_repo.connect_url(&sync_url).await?;
        let mut full_sync = node_b
            .sync_repo
            .full_sync_peers(
                Arc::clone(&node_b.drawer),
                std::slice::from_ref(&bootstrap.endpoint_id),
            )
            .await?;

        full_sync.0.wait_until_done().await?;
        full_sync.1.stop().await?;

        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            loop {
                let found = node_b
                    .drawer
                    .list()
                    .await
                    .map(|docs| docs.into_iter().any(|doc| doc.doc_id == new_doc_id))
                    .unwrap_or(false);
                if found {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        })
        .await
        .map_err(|_| eyre::eyre!("timed out waiting for replicated document"))?;

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
        let ctx = Arc::new(
            RepoCtx::open(
                gcx,
                repo_root,
                RepoOpenOptions {
                    ensure_initialized: false,
                    ws_connector_url: None,
                },
            )
            .await?,
        );

        let blobs_repo = BlobsRepo::new(ctx.layout.blobs_root.clone()).await?;
        let (plugs_repo, plugs_stop) = PlugsRepo::load(
            ctx.acx.clone(),
            Arc::clone(&blobs_repo),
            ctx.doc_app.document_id().clone(),
            ctx.local_actor_id.clone(),
        )
        .await?;
        let (drawer_repo, drawer_stop) = DrawerRepo::load(
            ctx.acx.clone(),
            ctx.doc_drawer.document_id().clone(),
            ctx.local_actor_id.clone(),
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
            ctx.acx.clone(),
            ctx.doc_app.document_id().clone(),
            Arc::clone(&plugs_repo),
            daybook_types::doc::UserPath::from(ctx.local_user_path.clone()),
            ctx.sql.db_pool.clone(),
        )
        .await?;
        let (sync_repo, sync_stop) = IrohSyncRepo::boot(
            ctx.acx.clone(),
            Arc::clone(&config_repo),
            ctx.iroh_secret_key.clone(),
            ctx.doc_app.document_id().clone(),
            ctx.doc_drawer.document_id().clone(),
            "sync-test".to_string(),
        )
        .await?;

        Ok(SyncTestNode {
            ctx,
            drawer: drawer_repo,
            drawer_stop,
            _plugs_repo: plugs_repo,
            plugs_stop,
            config_stop,
            sync_repo,
            sync_stop,
        })
    }
}
