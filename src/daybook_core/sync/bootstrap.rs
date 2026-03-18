use crate::interlude::*;

use super::{IrohSyncRepo, IROH_DOC_URL_SCHEME};

use std::str::FromStr;

use iroh_docs::api::protocol::{AddrInfoOptions, ShareMode};
use iroh_docs::store::Query;

use iroh::EndpointId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncBootstrapState {
    pub endpoint_addr: iroh::EndpointAddr,
    pub endpoint_id: EndpointId,
    pub repo_id: String,
    pub repo_name: String,
    pub app_doc_id: DocumentId,
    pub drawer_doc_id: DocumentId,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CloneRepoInitOptions {
    pub timeout: std::time::Duration,
}

impl Default for CloneRepoInitOptions {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CloneRepoInitResult {
    pub repo_path: std::path::PathBuf,
    pub bootstrap: SyncBootstrapState,
}

const BOOTSTRAP_KEY_REPO_ID: &[u8] = b"repo_id";
const BOOTSTRAP_KEY_REPO_NAME: &[u8] = b"repo_name";
const BOOTSTRAP_KEY_APP_DOC_ID: &[u8] = b"app_doc_id";
const BOOTSTRAP_KEY_DRAWER_DOC_ID: &[u8] = b"drawer_doc_id";
const BOOTSTRAP_KEY_DEVICE_NAME: &[u8] = b"device_name";

impl IrohSyncRepo {
    pub async fn get_ticket_url(&self) -> Res<String> {
        self.ensure_repo_live()?;
        let doc = self
            .iroh_docs
            .create()
            .await
            .map_err(|err| ferr!("error creating bootstrap doc: {err:?}"))?;
        {
            let author = self
                .iroh_docs
                .author_default()
                .await
                .map_err(|err| ferr!("error getting default docs author: {err:?}"))?;
            doc.set_bytes(
                author,
                BOOTSTRAP_KEY_REPO_ID.to_vec(),
                self.rcx.repo_id.as_bytes().to_vec(),
            )
            .await
            .map_err(|err| ferr!("error writing repo_id bootstrap key: {err:?}"))?;
            let repo_name = self
                .rcx
                .layout
                .repo_root
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| self.rcx.repo_id.clone());
            doc.set_bytes(
                author,
                BOOTSTRAP_KEY_REPO_NAME.to_vec(),
                repo_name.as_bytes().to_vec(),
            )
            .await
            .map_err(|err| ferr!("error writing repo_name bootstrap key: {err:?}"))?;
            doc.set_bytes(
                author,
                BOOTSTRAP_KEY_APP_DOC_ID.to_vec(),
                self.rcx
                    .doc_app
                    .document_id()
                    .to_string()
                    .as_bytes()
                    .to_vec(),
            )
            .await
            .map_err(|err| ferr!("error writing app_doc_id bootstrap key: {err:?}"))?;
            doc.set_bytes(
                author,
                BOOTSTRAP_KEY_DRAWER_DOC_ID.to_vec(),
                self.rcx
                    .doc_drawer
                    .document_id()
                    .to_string()
                    .as_bytes()
                    .to_vec(),
            )
            .await
            .map_err(|err| ferr!("error writing drawer_doc_id bootstrap key: {err:?}"))?;
            doc.set_bytes(
                author,
                BOOTSTRAP_KEY_DEVICE_NAME.to_vec(),
                self.rcx.local_device_name.as_bytes().to_vec(),
            )
            .await
            .map_err(|err| ferr!("error writing device_name bootstrap key: {err:?}"))?;
        }
        let ticket = doc
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|err| ferr!("error sharing bootstrap doc: {err:?}"))?;
        doc.start_sync(vec![])
            .await
            .map_err(|err| ferr!("error starting bootstrap doc sync: {err:?}"))?;
        Ok(format!("{IROH_DOC_URL_SCHEME}:{ticket}"))
    }
}

/// NOTE: this uses a fresh secret key for the connection
#[tracing::instrument(skip(iroh_doc_url))]
pub async fn resolve_bootstrap_from_url(iroh_doc_url: &str) -> Res<SyncBootstrapState> {
    let session = TempDocsSession::boot(None).await?;
    let out = resolve_bootstrap_with_docs(&session.docs, &session.blobs, iroh_doc_url).await;
    session.shutdown().await?;
    return out;

    struct TempDocsSession {
        router: iroh::protocol::Router,
        docs: iroh_docs::api::DocsApi,
        blobs: iroh_blobs::api::Store,
    }

    impl TempDocsSession {
        async fn boot(secret_key: Option<iroh::SecretKey>) -> Res<Self> {
            let endpoint_builder = match secret_key {
                Some(secret_key) => iroh::Endpoint::builder().secret_key(secret_key),
                None => iroh::Endpoint::builder(),
            };
            #[cfg(test)]
            let endpoint_builder = endpoint_builder
                .relay_mode(iroh::RelayMode::Disabled)
                .clear_address_lookup();
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
}

#[tracing::instrument(skip(docs, blobs, iroh_doc_url))]
pub(super) async fn resolve_bootstrap_with_docs(
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
        let repo_name = read_bootstrap_key(&doc, blobs, BOOTSTRAP_KEY_REPO_NAME).await?;
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
            let endpoint_id = endpoint_addr.id;
            doc.leave().await.to_eyre()?;
            return Ok(SyncBootstrapState {
                endpoint_addr,
                endpoint_id,
                repo_id,
                repo_name: repo_name.unwrap_or_default(),
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

#[tracing::instrument(skip(big_repo, iroh_secret_key, bootstrap, timeout))]
pub async fn connect_and_pull_required_docs_once(
    big_repo: &SharedBigRepo,
    iroh_secret_key: iroh::SecretKey,
    bootstrap: &SyncBootstrapState,
    timeout: std::time::Duration,
) -> Res<()> {
    let endpoint_builder = iroh::Endpoint::builder().secret_key(iroh_secret_key);
    #[cfg(test)]
    let endpoint_builder = endpoint_builder
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_address_lookup();
    let endpoint = endpoint_builder.bind().await?;
    let conn = big_repo
        .spawn_connection_iroh(&endpoint, bootstrap.endpoint_addr.clone(), None)
        .await?;

    let pull_res = pull_required_docs_once(
        big_repo,
        &bootstrap.app_doc_id,
        &bootstrap.drawer_doc_id,
        timeout,
    )
    .await;
    let stop_res = conn.stop().await;

    pull_res?;
    stop_res?;
    endpoint.close().await;
    Ok(())
}

#[tracing::instrument(skip(source_url, destination, options))]
pub async fn clone_repo_init_from_url(
    source_url: &str,
    destination: &std::path::Path,
    options: CloneRepoInitOptions,
) -> Res<CloneRepoInitResult> {
    let destination = std::path::absolute(destination)?;
    if destination.exists() {
        let mut read_dir = tokio::fs::read_dir(&destination).await?;
        if read_dir.next_entry().await?.is_some() {
            eyre::bail!(
                "clone destination must be empty or non-existent: {}",
                destination.display()
            );
        }
    } else {
        tokio::fs::create_dir_all(&destination).await?;
    }

    let bootstrap = resolve_bootstrap_from_url(source_url).await?;
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
    connect_and_pull_required_docs_once(
        &big_repo,
        identity.iroh_secret_key.clone(),
        &bootstrap,
        options.timeout,
    )
    .await?;
    crate::app::globals::set_init_state(
        &sql.db_pool,
        &crate::app::globals::InitState::Created {
            doc_id_app: bootstrap.app_doc_id.clone(),
            doc_id_drawer: bootstrap.drawer_doc_id.clone(),
        },
    )
    .await?;
    crate::repo::mark_repo_initialized(&destination).await?;
    big_repo_stop.stop().await?;
    Ok(CloneRepoInitResult {
        repo_path: destination,
        bootstrap,
    })
}

async fn pull_required_docs_once(
    big_repo: &SharedBigRepo,
    app_doc_id: &DocumentId,
    drawer_doc_id: &DocumentId,
    timeout: std::time::Duration,
) -> Res<()> {
    let app_doc_id = app_doc_id.clone();
    let drawer_doc_id = drawer_doc_id.clone();
    tokio::time::timeout(timeout, async move {
        loop {
            let app = big_repo.find_doc_handle(&app_doc_id).await?;
            let drawer = big_repo.find_doc_handle(&drawer_doc_id).await?;
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

async fn read_bootstrap_key(
    doc: &iroh_docs::api::Doc,
    blobs: &iroh_blobs::api::Store,
    key: &[u8],
) -> Res<Option<String>> {
    let Some(entry) = doc
        .get_one(Query::key_exact(key))
        .await
        .to_eyre()
        .wrap_err("error reading bootstrap key")?
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

fn parse_iroh_doc_ticket_url(input: &str) -> Res<iroh_docs::DocTicket> {
    let payload = input
        .strip_prefix(&format!("{IROH_DOC_URL_SCHEME}:"))
        .ok_or_eyre("invalid sync url scheme, expected db+iroh-doc:<ticket>")?;
    iroh_docs::DocTicket::from_str(payload).wrap_err("invalid iroh docs ticket")
}
