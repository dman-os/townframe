use crate::interlude::*;

use super::{
    IrohSyncRepo, CLONE_PROVISION_ALPN, CORE_DOCS_PARTITION_ID, IROH_CLONE_URL_SCHEME,
};

use std::str::FromStr;

use iroh::EndpointId;
use iroh_tickets::endpoint::EndpointTicket;
use irpc::{channel, rpc_requests};
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

// ---------------------------------------------------------------------------
// Resolve clone info (lightweight metadata, no authentication required)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloneInfoRequest {
    pub requested_device_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloneInfoResponse {
    pub repo_name: String,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ResolveCloneInfoRpcReq {
    pub req: CloneInfoRequest,
}

// ---------------------------------------------------------------------------
// Request clone provision (full bootstrap + peer authorization)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestCloneProvisionReq {
    pub requested_device_name: Option<String>,
    pub requester_endpoint_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloneProvisionResponse {
    pub endpoint_addr: iroh::EndpointAddr,
    pub repo_id: String,
    pub repo_name: String,
    pub app_doc_id: String,
    pub drawer_doc_id: String,
    pub device_name: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RequestCloneProvisionRpcReq {
    pub req: RequestCloneProvisionReq,
}

#[rpc_requests(message = CloneProvisionRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum CloneProvisionRpc {
    #[rpc(tx = channel::oneshot::Sender<Result<CloneInfoResponse, String>>)]
    ResolveCloneInfo(ResolveCloneInfoRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<CloneProvisionResponse, String>>)]
    RequestCloneProvision(RequestCloneProvisionRpcReq),
}

impl IrohSyncRepo {
    pub async fn get_clone_ticket_url(&self) -> Res<String> {
        self.ensure_repo_live()?;
        let endpoint_addr = self.router.endpoint().addr();
        let endpoint_ticket = EndpointTicket::from(endpoint_addr).to_string();
        Ok(format!("{IROH_CLONE_URL_SCHEME}:{endpoint_ticket}"))
    }
    pub fn endpoint_addr(&self) -> iroh::EndpointAddr {
        self.router.endpoint().addr()
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

// ---------------------------------------------------------------------------
// Client-side RPC helpers
// ---------------------------------------------------------------------------

#[tracing::instrument(skip(source_url))]
pub async fn resolve_clone_info_from_url(source_url: &str) -> Res<CloneInfoResponse> {
    let endpoint_addr = parse_clone_endpoint_addr(source_url)?;
    let req = ResolveCloneInfoRpcReq {
        req: CloneInfoRequest {
            requested_device_name: None,
        },
    };
    #[cfg(test)]
    if let Some(local_sender) = lookup_test_clone_rpc_sender(endpoint_addr.id).await {
        let client = irpc::Client::<CloneProvisionRpc>::local(local_sender);
        let response = client
            .rpc(req)
            .await
            .wrap_err("clone info rpc transport failed (in-memory)")?
            .map_err(|err| eyre::eyre!("clone info rpc failed: {err}"))?;
        return Ok(response);
    }
    let endpoint = iroh::Endpoint::builder().bind().await?;
    let client = irpc_iroh::client::<CloneProvisionRpc>(
        endpoint.clone(),
        endpoint_addr,
        CLONE_PROVISION_ALPN,
    );
    let response_res = client.rpc(req).await;
    endpoint.close().await;
    let response = response_res
        .wrap_err("clone info rpc transport failed")?
        .map_err(|err| eyre::eyre!("clone info rpc failed: {err}"))?;
    Ok(response)
}

#[tracing::instrument(skip(source_url))]
pub async fn request_clone_provision_from_url(
    source_url: &str,
    req: RequestCloneProvisionReq,
) -> Res<CloneProvisionResponse> {
    let endpoint_addr = parse_clone_endpoint_addr(source_url)?;
    let req = RequestCloneProvisionRpcReq { req };
    #[cfg(test)]
    if let Some(local_sender) = lookup_test_clone_rpc_sender(endpoint_addr.id).await {
        let client = irpc::Client::<CloneProvisionRpc>::local(local_sender);
        let response = client
            .rpc(req)
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
    let response_res = client.rpc(req).await;
    endpoint.close().await;
    let response = response_res
        .wrap_err("clone provision rpc transport failed")?
        .map_err(|err| eyre::eyre!("clone provision rpc failed: {err}"))?;
    Ok(response)
}

#[tracing::instrument(skip(big_repo, blobs_repo, local_peer_key, iroh_secret_key, bootstrap, timeout, sql))]
pub async fn connect_and_pull_required_partitions_once(
    big_repo: &SharedBigRepo,
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    local_peer_key: &str,
    iroh_secret_key: iroh::SecretKey,
    sql: &crate::app::SqlCtx,
    bootstrap: &SyncBootstrapState,
    timeout: std::time::Duration,
) -> Res<()> {
    let endpoint_builder = iroh::Endpoint::builder().secret_key(iroh_secret_key);
    #[cfg(test)]
    let endpoint_builder = endpoint_builder
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .relay_mode(iroh::RelayMode::Disabled);
    let endpoint = endpoint_builder.bind().await?;
    let result: Res<()> = pull_required_partitions_via_full_sync_worker(
        big_repo,
        blobs_repo,
        local_peer_key,
        &endpoint,
        sql,
        bootstrap,
        timeout,
    )
    .await;
    endpoint.close().await;
    result
}

async fn pull_required_partitions_via_full_sync_worker(
    big_repo: &SharedBigRepo,
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    local_peer_key: &str,
    endpoint: &iroh::Endpoint,
    sql: &crate::app::SqlCtx,
    bootstrap: &SyncBootstrapState,
    timeout: std::time::Duration,
) -> Res<()> {
    let _ = local_peer_key;
    let source_peer_key = daybook_types::doc::format_peer_key(bootstrap.endpoint_id.as_bytes());
    let peer_id: am_utils_rs::repo::PeerId = bootstrap.endpoint_id.into();

    let (sync_store, sync_store_stop) =
        am_utils_rs::sync::store::spawn_sync_store(sql.db_pool.clone()).await?;
    sync_store.allow_peer(source_peer_key.clone()).await?;

    let (sync_node_handle, sync_node_stop) = am_utils_rs::sync::node::spawn_sync_node(
        big_repo.partition_store().clone(),
        sync_store.clone(),
        Arc::new(am_utils_rs::sync::AllowAllPartitionAccessPolicy),
    )
    .await?;

    let _sync_router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            super::PARTITION_SYNC_ALPN,
            super::AuthenticatedIrohProtocol::<am_utils_rs::sync::protocol::PartitionSyncRpc> {
                tx: sync_node_handle.local_sender(),
                peer_key_fn: Arc::new(|endpoint_id| {
                    daybook_types::doc::format_peer_key(endpoint_id.as_bytes())
                }),
            },
        )
        .spawn();

    // Ensure blob scope partitions exist locally so bootstrap_blob_scope_memberships
    // doesn't fail on an empty partition store.
    big_repo
        .partition_store()
        .ensure_partition(&CORE_DOCS_PARTITION_ID.to_string())
        .await?;
    big_repo
        .partition_store()
        .ensure_partition(
            &crate::drawer::DrawerRepo::replicated_partition_id_for_drawer(
                &bootstrap.drawer_doc_id,
            ),
        )
        .await?;
    big_repo
        .partition_store()
        .ensure_partition(&crate::rt::PROCESSOR_RUNLOG_PARTITION_ID.to_string())
        .await?;
    big_repo
        .partition_store()
        .ensure_partition(&crate::blobs::BLOB_SCOPE_DOCS_PARTITION_ID.to_string())
        .await?;
    big_repo
        .partition_store()
        .ensure_partition(&crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.to_string())
        .await?;

    let (worker_handle, worker_stop) = super::full::spawn_full_sync_worker(
        Arc::clone(big_repo),
        source_peer_key.clone().into(),
        Arc::clone(blobs_repo),
        None,
        sync_store,
        endpoint.clone(),
    )
    .await?;

    let partitions: HashSet<am_utils_rs::sync::protocol::PartitionId> = [
        CORE_DOCS_PARTITION_ID.to_string(),
        crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.to_string(),
    ]
    .into_iter()
    .collect();

    let connection = big_repo
        .open_connection_iroh(
            endpoint.clone(),
            bootstrap.endpoint_addr.clone(),
            peer_id,
            None,
        )
        .await?;

    worker_handle
        .set_connection(
            connection,
            bootstrap.endpoint_addr.clone(),
            source_peer_key.clone(),
            partitions.clone(),
        )
        .await?;

    let timeout_result = tokio::time::timeout(timeout, async {
        let required_partitions = partitions
            .iter()
            .cloned()
            .map(super::full::PartitionKey::from_partition_id)
            .collect::<HashSet<_>>();
        worker_handle
            .wait_for_peers_fully_synced(vec![peer_id], required_partitions)
            .await
    })
    .await;

    worker_stop.cancel_token.cancel();

    let _ = sync_node_stop.stop().await;
    let _ = sync_store_stop.stop().await;

    match timeout_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(err),
        Err(_) => {
            eyre::bail!("timed out waiting for required partitions during clone");
        }
    }

    let app_present = big_repo.get_doc(&bootstrap.app_doc_id).await?.is_some();
    let drawer_present = big_repo.get_doc(&bootstrap.drawer_doc_id).await?.is_some();
    if !app_present || !drawer_present {
        eyre::bail!(
            "required core docs missing after clone sync (app_present={app_present}, drawer_present={drawer_present})"
        );
    }

    Ok(())
}

// FIXME: move parts of this into a RepoCtx method
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
    }
    let parent = destination
        .parent()
        .ok_or_eyre("clone destination missing parent directory")?;
    tokio::fs::create_dir_all(parent).await?;
    let staging = next_clone_staging_dir(parent)?;
    tokio::fs::create_dir_all(&staging).await?;

    let cloned = async {
        let secret_repo = crate::secrets::SecretRepo::boot().await?;

        // Generate identity locally — secret keys never leave the device.
        let local_secret = iroh::SecretKey::generate(&mut rand::rng());
        let local_public = local_secret.public();
        let provision = request_clone_provision_from_url(
            source_url,
            RequestCloneProvisionReq {
                requested_device_name: Some(format!("clone-{}", std::env::consts::ARCH)),
                requester_endpoint_id: local_public.to_string(),
            },
        )
        .await?;
        let bootstrap = provision.to_bootstrap_state()?;

        let local_peer_key = daybook_types::doc::format_peer_key(local_public.as_bytes());

        let sqlite_path = staging.join("sqlite.db");
        let sql = crate::app::SqlCtx::new(crate::app::SqlConfig {
            database_url: format!("sqlite://{}", sqlite_path.display()),
        })
        .await?;
        crate::repo::globals::set_string_global(&sql, "global.repo_id", &bootstrap.repo_id).await?;
        crate::repo::globals::set_string_global(&sql, "global.repo_name", &bootstrap.repo_name)
            .await?;
        let checkout_id = {
            let id = Uuid::new_v4();
            let id = utils_rs::hash::encode_base58_multibase(id);
            format!("dcheckout_{id}")
        };
        crate::repo::globals::set_string_global(&sql, "global.checkout_id", &checkout_id).await?;
        let user_id = format!(
            "{}{}",
            daybook_types::doc::user_path::USER_ID_PREFIX,
            Uuid::new_v4().bs58()
        );
        crate::repo::globals::set_string_global(&sql, "global.user_id", &user_id).await?;

        let identity = secret_repo
            .set_identity(&checkout_id, local_secret.clone())
            .await?;

        let pkey_bs58 = utils_rs::hash::encode_base58_multibase(local_public.as_bytes());
        let device_id = format!(
            "{}{}",
            daybook_types::doc::user_path::DEVICE_ID_PREFIX,
            pkey_bs58
        );
        let local_user_path = daybook_types::doc::UserPathBuf::new()
            .join("/")
            .join(user_id)
            .join(device_id);
        let mut sync_config = crate::repo::globals::get_sync_config(&sql).await?;
        if !sync_config
            .known_devices
            .iter()
            .any(|entry| entry.endpoint_id == bootstrap.endpoint_id)
        {
            sync_config
                .known_devices
                .push(crate::repo::globals::SyncDeviceEntry {
                    endpoint_id: bootstrap.endpoint_id,
                    name: bootstrap
                        .device_name
                        .clone()
                        .unwrap_or_else(|| bootstrap.endpoint_id.to_string()),
                    added_at: jiff::Timestamp::now(),
                    last_connected_at: None,
                });
            crate::repo::globals::set_sync_config(&sql, &sync_config).await?;
        }

        let (big_repo, big_repo_stop) = am_utils_rs::BigRepo::boot(am_utils_rs::repo::Config {
            storage: am_utils_rs::repo::StorageConfig::Disk {
                path: staging.join("samod"),
            },
            peer_id: identity.iroh_public_key.into(),
            secret_key_bytes: identity.iroh_secret_key.to_bytes(),
        })
        .await?;
        let blobs_repo = crate::blobs::BlobsRepo::new(
            staging.join("blobs"),
            "clone-bootstrap".into(),
            Arc::new(crate::blobs::PartitionStoreMembershipWriter::new(
                big_repo.partition_store(),
            )),
        )
        .await?;

        connect_and_pull_required_partitions_once(
            &big_repo,
            &blobs_repo,
            &local_peer_key,
            local_secret.clone(),
            &sql,
            &bootstrap,
            options.timeout,
        )
        .await?;

        blobs_repo.shutdown().await?;

        crate::repo::globals::set_init_state(
            &sql,
            &crate::repo::globals::InitState::Created {
                doc_id_app: bootstrap.app_doc_id.clone(),
                doc_id_drawer: bootstrap.drawer_doc_id.clone(),
            },
        )
        .await?;

        crate::repo::finish_clone_init(&big_repo, &sql, local_user_path, staging.join("blobs"))
            .await?;
        crate::repo::mark_repo_initialized(&staging).await?;

        big_repo_stop.stop().await?;
        Ok::<SyncBootstrapState, eyre::Report>(bootstrap)
    }
    .await;

    let bootstrap = match cloned {
        Ok(bootstrap) => bootstrap,
        Err(err) => {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            return Err(err);
        }
    };

    if destination.exists() {
        let mut read_dir = tokio::fs::read_dir(&destination).await?;
        if read_dir.next_entry().await?.is_some() {
            let _ = tokio::fs::remove_dir_all(&staging).await;
            eyre::bail!(
                "clone destination became non-empty during clone: {}",
                destination.display()
            );
        }
        tokio::fs::remove_dir(&destination).await?;
    }
    tokio::fs::rename(&staging, &destination).await?;

    Ok(CloneRepoInitResult {
        repo_path: destination,
        bootstrap,
    })
}

fn next_clone_staging_dir(parent: &std::path::Path) -> Res<std::path::PathBuf> {
    for _ in 0..16usize {
        let candidate = parent.join(format!(".daybook-clone-staging-{}", Uuid::new_v4()));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    eyre::bail!(
        "failed allocating clone staging directory under {}",
        parent.display()
    );
}

pub fn parse_clone_endpoint_addr(input: &str) -> Res<iroh::EndpointAddr> {
    let payload = if let Some(stripped) = input.strip_prefix(&format!("{IROH_CLONE_URL_SCHEME}:")) {
        stripped
    } else {
        input
    };
    let endpoint_ticket = EndpointTicket::from_str(payload)
        .wrap_err("invalid endpoint ticket payload in clone url")?;
    Ok(endpoint_ticket.into())
}

#[cfg(test)]
static TEST_CLONE_RPC_REGISTRY: LazyLock<
    RwLock<HashMap<EndpointId, tokio::sync::mpsc::Sender<CloneProvisionRpcMessage>>>,
> = LazyLock::new(|| RwLock::new(HashMap::new()));

#[cfg(test)]
pub async fn register_test_clone_rpc_sender(
    endpoint_id: EndpointId,
    sender: tokio::sync::mpsc::Sender<CloneProvisionRpcMessage>,
) {
    TEST_CLONE_RPC_REGISTRY
        .write()
        .await
        .insert(endpoint_id, sender);
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
