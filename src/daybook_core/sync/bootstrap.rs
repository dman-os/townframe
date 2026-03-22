use crate::interlude::*;

use super::{IrohSyncRepo, CLONE_PROVISION_ALPN, IROH_CLONE_URL_SCHEME};

use std::str::FromStr;

use irpc::{channel, rpc_requests};
use iroh::EndpointId;
use iroh_tickets::endpoint::EndpointTicket;
#[cfg(test)]
use tokio::sync::RwLock;

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloneProvisionRequest {
    pub requested_device_name: Option<String>,
    pub provision: bool,
    pub requester_endpoint_id: Option<String>,
    pub requester_peer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloneProvisionResponse {
    pub endpoint_addr: iroh::EndpointAddr,
    pub repo_id: String,
    pub repo_name: String,
    pub app_doc_id: String,
    pub drawer_doc_id: String,
    pub device_name: Option<String>,
    pub issued_iroh_secret_key_hex: Option<String>,
    pub issued_iroh_public_key: Option<String>,
    pub issued_peer_key: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RequestCloneProvisionRpcReq {
    pub req: CloneProvisionRequest,
}

#[rpc_requests(message = CloneProvisionRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum CloneProvisionRpc {
    #[rpc(tx = channel::oneshot::Sender<Result<CloneProvisionResponse, String>>)]
    RequestCloneProvision(RequestCloneProvisionRpcReq),
}

impl IrohSyncRepo {
    pub async fn get_ticket_url(&self) -> Res<String> {
        self.ensure_repo_live()?;
        let endpoint_addr = self.current_endpoint_addr_for_clone().await;
        let endpoint_ticket = EndpointTicket::from(endpoint_addr).to_string();
        Ok(format!("{IROH_CLONE_URL_SCHEME}:{endpoint_ticket}"))
    }
}

impl IrohSyncRepo {
    async fn current_endpoint_addr_for_clone(&self) -> iroh::EndpointAddr {
        let mut endpoint_addr = self.router.endpoint().addr();
        if endpoint_addr.addrs.is_empty() {
            let started = std::time::Instant::now();
            while endpoint_addr.addrs.is_empty() && started.elapsed() < Duration::from_secs(2) {
                tokio::time::sleep(Duration::from_millis(50)).await;
                endpoint_addr = self.router.endpoint().addr();
            }
        }
        endpoint_addr
    }

    pub async fn current_bootstrap_state(&self) -> SyncBootstrapState {
        let repo_name = self
            .rcx
            .layout
            .repo_root
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| self.rcx.repo_id.clone());
        let endpoint_addr = self.current_endpoint_addr_for_clone().await;
        let endpoint_id = endpoint_addr.id;
        SyncBootstrapState {
            endpoint_addr,
            endpoint_id,
            repo_id: self.rcx.repo_id.clone(),
            repo_name,
            app_doc_id: self.rcx.doc_app.document_id().clone(),
            drawer_doc_id: self.rcx.doc_drawer.document_id().clone(),
            device_name: Some(self.rcx.local_device_name.clone()),
        }
    }
}

impl CloneProvisionResponse {
    pub fn to_bootstrap_state(&self) -> Res<SyncBootstrapState> {
        let endpoint_id = self.endpoint_addr.id;
        Ok(SyncBootstrapState {
            endpoint_addr: self.endpoint_addr.clone(),
            endpoint_id,
            repo_id: self.repo_id.clone(),
            repo_name: self.repo_name.clone(),
            app_doc_id: DocumentId::from_str(&self.app_doc_id)
                .wrap_err("invalid app_doc_id in clone response")?,
            drawer_doc_id: DocumentId::from_str(&self.drawer_doc_id)
                .wrap_err("invalid drawer_doc_id in clone response")?,
            device_name: self.device_name.clone(),
        })
    }
}

pub async fn request_clone_provision_via_rpc(
    source_url: &str,
    request: CloneProvisionRequest,
) -> Res<CloneProvisionResponse> {
    let endpoint_addr = parse_clone_endpoint_addr(source_url)?;
    #[cfg(test)]
    if let Some(local_sender) = lookup_test_clone_rpc_sender(endpoint_addr.id).await {
        let client = irpc::Client::<CloneProvisionRpc>::local(local_sender);
        let response = client
            .rpc(RequestCloneProvisionRpcReq { req: request })
            .await
            .wrap_err("clone provision rpc transport failed (in-memory)")?
            .map_err(|err| eyre::eyre!("clone provision rpc failed: {err}"))?;
        return Ok(response);
    }
    let endpoint = iroh::Endpoint::builder().bind().await?;
    let client = irpc_iroh::client::<CloneProvisionRpc>(
        endpoint.clone(),
        endpoint_addr,
        CLONE_PROVISION_ALPN,
    );
    let response = client
        .rpc(RequestCloneProvisionRpcReq { req: request })
        .await
        .wrap_err("clone provision rpc transport failed")?
        .map_err(|err| eyre::eyre!("clone provision rpc failed: {err}"))?;
    endpoint.close().await;
    Ok(response)
}

#[tracing::instrument(skip(source_url))]
pub async fn resolve_bootstrap_from_url(source_url: &str) -> Res<SyncBootstrapState> {
    request_clone_provision_via_rpc(
        source_url,
        CloneProvisionRequest {
            requested_device_name: None,
            provision: false,
            requester_endpoint_id: None,
            requester_peer_key: None,
        },
    )
    .await?
    .to_bootstrap_state()
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

    let provision = request_clone_provision_via_rpc(
        source_url,
        CloneProvisionRequest {
            requested_device_name: Some(format!(
                "clone-{}",
                std::env::consts::ARCH
            )),
            provision: true,
            requester_endpoint_id: None,
            requester_peer_key: None,
        },
    )
    .await?;
    let bootstrap = provision.to_bootstrap_state()?;
    let sqlite_path = destination.join("sqlite.db");
    let sql = crate::app::SqlCtx::new(&format!("sqlite://{}", sqlite_path.display())).await?;
    crate::app::globals::set_repo_id(&sql.db_pool, &bootstrap.repo_id).await?;
    let issued_secret_hex = provision
        .issued_iroh_secret_key_hex
        .ok_or_eyre("clone provision response missing issued_iroh_secret_key_hex")?;
    let issued_public_key = provision
        .issued_iroh_public_key
        .ok_or_eyre("clone provision response missing issued_iroh_public_key")?;
    let identity = crate::secrets::SecretRepo::set_identity_from_secret_hex(
        &sql.db_pool,
        &bootstrap.repo_id,
        &issued_secret_hex,
    )
    .await?;
    if identity.iroh_public_key.to_string() != issued_public_key {
        eyre::bail!("provisioned public key mismatch while cloning");
    }
    let _repo_user_id = crate::repo::get_or_init_repo_user_id(&sql.db_pool).await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS sync_allowed_peers(
            peer_key TEXT PRIMARY KEY,
            endpoint_id TEXT NULL
        )
        "#,
    )
    .execute(&sql.db_pool)
    .await?;
    let source_peer_key = format!("/{}/{}", bootstrap.repo_id, bootstrap.endpoint_id);
    sqlx::query(
        "INSERT INTO sync_allowed_peers(peer_key, endpoint_id) VALUES(?, ?) ON CONFLICT(peer_key) DO UPDATE SET endpoint_id = excluded.endpoint_id",
    )
    .bind(source_peer_key)
    .bind(bootstrap.endpoint_id.to_string())
    .execute(&sql.db_pool)
    .await?;
    let mut sync_config = crate::app::globals::get_sync_config(&sql.db_pool).await?;
    if !sync_config
        .known_devices
        .iter()
        .any(|entry| entry.endpoint_id == bootstrap.endpoint_id)
    {
        sync_config
            .known_devices
            .push(crate::app::globals::SyncDeviceEntry {
                endpoint_id: bootstrap.endpoint_id,
                name: bootstrap
                    .device_name
                    .clone()
                    .unwrap_or_else(|| bootstrap.endpoint_id.to_string()),
                added_at: jiff::Timestamp::now(),
                last_connected_at: None,
            });
        crate::app::globals::set_sync_config(&sql.db_pool, &sync_config).await?;
    }

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

fn parse_clone_endpoint_addr(input: &str) -> Res<iroh::EndpointAddr> {
    let payload = input
        .strip_prefix(&format!("{IROH_CLONE_URL_SCHEME}:"))
        .ok_or_eyre("invalid clone url scheme, expected db+iroh-clone:<endpoint-ticket>")?;
    let endpoint_ticket = EndpointTicket::from_str(payload)
        .wrap_err("invalid endpoint ticket payload in clone url")?;
    Ok(endpoint_ticket.into())
}

#[cfg(test)]
static TEST_CLONE_RPC_REGISTRY: LazyLock<RwLock<HashMap<EndpointId, tokio::sync::mpsc::Sender<CloneProvisionRpcMessage>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[cfg(test)]
pub async fn register_test_clone_rpc_sender(
    endpoint_id: EndpointId,
    sender: tokio::sync::mpsc::Sender<CloneProvisionRpcMessage>,
) {
    TEST_CLONE_RPC_REGISTRY.write().await.insert(endpoint_id, sender);
}

#[cfg(test)]
pub async fn unregister_test_clone_rpc_sender(endpoint_id: EndpointId) {
    TEST_CLONE_RPC_REGISTRY.write().await.remove(&endpoint_id);
}

#[cfg(test)]
async fn lookup_test_clone_rpc_sender(
    endpoint_id: EndpointId,
) -> Option<tokio::sync::mpsc::Sender<CloneProvisionRpcMessage>> {
    TEST_CLONE_RPC_REGISTRY
        .read()
        .await
        .get(&endpoint_id)
        .cloned()
}
