use crate::interlude::*;

use super::{
    IrohSyncRepo, CLONE_PROVISION_ALPN, CORE_DOCS_PARTITION_ID, IROH_CLONE_URL_SCHEME,
    PARTITION_SYNC_ALPN, REPO_SYNC_ALPN,
};

use std::str::FromStr;

use futures::StreamExt;
use iroh::EndpointId;
use iroh_blobs::api::downloader::DownloadProgressItem;
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

#[tracing::instrument(skip(big_repo, blobs_repo, iroh_secret_key, bootstrap, timeout))]
pub async fn connect_and_pull_required_partitions_once(
    big_repo: &SharedBigRepo,
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    local_peer_key: &str,
    iroh_secret_key: iroh::SecretKey,
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
    let result: Res<()> = pull_required_partitions_once(
        big_repo,
        blobs_repo,
        local_peer_key,
        &endpoint,
        bootstrap,
        timeout,
    )
    .await;
    endpoint.close().await;
    result
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

        let local_peer_key =
            daybook_types::doc::format_peer_key(&bootstrap.repo_id, local_public.as_bytes());

        let sqlite_path = staging.join("sqlite.db");
        let sql = crate::app::SqlCtx::new(crate::app::SqlConfig {
            database_url: format!("sqlite://{}", sqlite_path.display()),
        })
        .await?;
        crate::repo::globals::set_string_global(&sql, "global.repo_id", &bootstrap.repo_id).await?;
        crate::repo::globals::set_string_global(&sql, "global.repo_name", &bootstrap.repo_name)
            .await?;
        let user_id = format!(
            "{}{}",
            daybook_types::doc::user_path::USER_ID_PREFIX,
            Uuid::new_v4().bs58()
        );
        crate::repo::globals::set_string_global(&sql, "global.user_id", &user_id).await?;

        let identity =
            crate::secrets::SecretRepo::set_identity(&bootstrap.repo_id, local_secret.clone())
                .await?;
        if identity.iroh_public_key.to_string() != local_public.to_string() {
            eyre::bail!("provisioned public key mismatch while cloning");
        }

        let pkey_bs58 = utils_rs::hash::encode_base58_multibase(local_public.as_bytes());
        let device_id = format!(
            "{}{}",
            daybook_types::doc::user_path::DEVICE_ID_PREFIX,
            pkey_bs58
        );
        let local_user_path = format!("/{user_id}/{device_id}");
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
        })
        .await?;
        let source_peer_key = format!("/{}/{}", bootstrap.repo_id, bootstrap.endpoint_id);
        let (sync_store, sync_store_stop) =
            am_utils_rs::sync::store::spawn_sync_store(sql.db_pool.clone()).await?;
        let allow_res = sync_store.allow_peer(source_peer_key.into()).await;
        let stop_res = sync_store_stop.stop().await;
        allow_res?;
        stop_res?;

        let blobs_repo = crate::blobs::BlobsRepo::new(
            staging.join("blobs"),
            "clone-bootstrap".to_string(),
            Arc::new(crate::blobs::NoopPartitionMembershipWriter),
        )
        .await?;

        connect_and_pull_required_partitions_once(
            &big_repo,
            &blobs_repo,
            &local_peer_key,
            local_secret.clone(),
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

        crate::repo::finish_clone_init(
            &big_repo,
            &sql,
            local_user_path,
            staging.join("blobs"),
        )
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

async fn pull_required_partitions_once(
    big_repo: &SharedBigRepo,
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    local_peer_key: &str,
    endpoint: &iroh::Endpoint,
    bootstrap: &SyncBootstrapState,
    timeout: std::time::Duration,
) -> Res<()> {
    tokio::time::timeout(timeout, async move {
        let partition_rpc = irpc_iroh::client::<am_utils_rs::sync::protocol::PartitionSyncRpc>(
            endpoint.clone(),
            bootstrap.endpoint_addr.clone(),
            PARTITION_SYNC_ALPN,
        );
        let partition_list = partition_rpc
            .rpc(am_utils_rs::sync::protocol::ListPartitionsRpcReq {
                peer: local_peer_key.into(),
            })
            .await
            .wrap_err("list required partitions rpc failed")?
            .map_err(|err| eyre::eyre!("list required partitions rpc failed: {err:?}"))?;
        let available_partitions: HashSet<String> = partition_list
            .partitions
            .into_iter()
            .map(|summary| summary.partition_id)
            .collect();
        for required in [
            CORE_DOCS_PARTITION_ID.to_string(),
            crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID.to_string(),
        ] {
            if !available_partitions.contains(&required) {
                eyre::bail!("required partition missing on clone source: {required}");
            }
        }

        let core_docs = list_current_partition_members(
            &partition_rpc,
            local_peer_key,
            CORE_DOCS_PARTITION_ID,
        )
        .await?;
        let app_doc_id = bootstrap.app_doc_id.to_string();
        let drawer_doc_id = bootstrap.drawer_doc_id.to_string();
        if !core_docs.contains(&app_doc_id) || !core_docs.contains(&drawer_doc_id) {
            eyre::bail!(
                "required core docs missing from partition {CORE_DOCS_PARTITION_ID} (app_present={}, drawer_present={})",
                core_docs.contains(&app_doc_id),
                core_docs.contains(&drawer_doc_id)
            );
        }

        let repo_rpc = irpc_iroh::client::<am_utils_rs::repo::rpc::RepoSyncRpc>(
            endpoint.clone(),
            bootstrap.endpoint_addr.clone(),
            REPO_SYNC_ALPN,
        );
        let full_docs = repo_rpc
            .rpc(am_utils_rs::repo::rpc::GetDocsFullRpcReq {
                peer: local_peer_key.into(),
                req: am_utils_rs::repo::rpc::GetDocsFullRequest {
                    doc_ids: vec![app_doc_id.clone(), drawer_doc_id.clone()],
                },
            })
            .await
            .wrap_err("GetDocsFull rpc failed during clone bootstrap")?
            .map_err(|err| eyre::eyre!("GetDocsFull rejected during clone bootstrap: {err:?}"))?;

        for full_doc in full_docs.docs {
            let parsed = DocumentId::from_str(&full_doc.doc_id).wrap_err_with(|| {
                format!(
                    "invalid core doc id in bootstrap payload: {}",
                    full_doc.doc_id
                )
            })?;
            if big_repo.get_doc(&parsed).await?.is_some() {
                continue;
            }
            let loaded = automerge::Automerge::load(&full_doc.automerge_save).map_err(|err| {
                eyre::eyre!(
                    "invalid automerge payload for core doc {} during clone bootstrap: {err}",
                    full_doc.doc_id
                )
            })?;
            big_repo.put_doc(parsed, loaded).await?;
        }

        let mut attempts = 0usize;
        loop {
            attempts += 1;
            let app = big_repo.get_doc(&bootstrap.app_doc_id).await?.is_some();
            let drawer = big_repo.get_doc(&bootstrap.drawer_doc_id).await?.is_some();
            if app && drawer {
                break;
            }
            if attempts.is_multiple_of(10) {
                debug!(
                    attempts,
                    app_doc_id = %bootstrap.app_doc_id,
                    drawer_doc_id = %bootstrap.drawer_doc_id,
                    "still waiting for required docs after clone bootstrap pull attempt"
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }

        let plug_blob_hashes = list_current_partition_members(
            &partition_rpc,
            local_peer_key,
            crate::blobs::BLOB_SCOPE_PLUGS_PARTITION_ID,
        )
        .await?;
        for hash in plug_blob_hashes {
            ensure_blob_hash_present(blobs_repo, endpoint, bootstrap.endpoint_id, &hash).await?;
        }

        Ok::<(), eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("timed out waiting for required partitions during clone"))??;

    Ok(())
}

async fn list_current_partition_members(
    partition_rpc: &irpc::Client<am_utils_rs::sync::protocol::PartitionSyncRpc>,
    local_peer_key: &str,
    partition_id: &str,
) -> Res<HashSet<String>> {
    let mut current_members = HashSet::new();
    let mut since = None;
    loop {
        let page = partition_rpc
            .rpc(
                am_utils_rs::sync::protocol::GetPartitionMemberEventsRpcReq {
                    peer: local_peer_key.into(),
                    req: am_utils_rs::sync::protocol::GetPartitionMemberEventsRequest {
                        partitions: vec![am_utils_rs::sync::protocol::PartitionCursorRequest {
                            partition_id: partition_id.to_string(),
                            since,
                        }],
                        limit: am_utils_rs::sync::protocol::DEFAULT_EVENT_PAGE_LIMIT,
                    },
                },
            )
            .await
            .wrap_err_with(|| {
                format!("partition member replay rpc failed for partition {partition_id}")
            })?
            .map_err(|err| {
                eyre::eyre!(
                    "partition member replay rpc failed for partition {partition_id}: {err:?}"
                )
            })?;
        for event in page.events {
            match event.deets {
                am_utils_rs::sync::protocol::PartitionMemberEventDeets::MemberUpsert {
                    item_id,
                    ..
                } => {
                    current_members.insert(item_id);
                }
                am_utils_rs::sync::protocol::PartitionMemberEventDeets::MemberRemoved {
                    item_id,
                    ..
                } => {
                    current_members.remove(&item_id);
                }
            }
        }
        let cursor = page
            .cursors
            .into_iter()
            .find(|cursor| cursor.partition_id == partition_id)
            .ok_or_eyre("partition cursor page missing during clone bootstrap")?;
        if !cursor.has_more {
            break;
        }
        since = Some(
            cursor
                .next_cursor
                .ok_or_eyre("partition replay cursor missing next_cursor")?,
        );
    }
    Ok(current_members)
}

async fn ensure_blob_hash_present(
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    endpoint: &iroh::Endpoint,
    endpoint_id: EndpointId,
    hash: &str,
) -> Res<()> {
    if blobs_repo.has_hash(hash).await? {
        return Ok(());
    }
    let iroh_hash = crate::blobs::daybook_hash_to_iroh_hash(hash)?;
    if blobs_repo.iroh_store().blobs().has(iroh_hash).await? {
        blobs_repo.put_from_store(hash).await?;
        return Ok(());
    }
    let downloader = blobs_repo.iroh_store().downloader(endpoint);
    let progress = downloader.download(iroh_hash, vec![endpoint_id]);
    let mut stream = progress
        .stream()
        .await
        .map_err(|err| eyre::eyre!("failed opening blob download stream for {hash}: {err:?}"))?;
    let mut saw_error = false;
    let mut last_error: Option<String> = None;
    while let Some(item) = stream.next().await {
        match item {
            DownloadProgressItem::DownloadError => {
                eyre::bail!("blob download reported error for hash {hash}: download error");
            }
            DownloadProgressItem::Error(err) => {
                eyre::bail!("blob download reported error for hash {hash}: {err:?}");
            }
            item @ DownloadProgressItem::ProviderFailed { .. } => {
                saw_error = true;
                last_error = Some(format!("{item:?}"));
            }
            DownloadProgressItem::TryProvider { .. }
            | DownloadProgressItem::Progress(_)
            | DownloadProgressItem::PartComplete { .. } => {}
        }
    }
    let blob_present = blobs_repo.iroh_store().blobs().has(iroh_hash).await?;
    if !blob_present {
        if saw_error {
            if let Some(details) = last_error {
                eyre::bail!("blob download reported error for hash {hash}: {details}");
            }
            eyre::bail!("blob download reported error for hash {hash}");
        }
        eyre::bail!("blob not found in iroh store after download for hash {hash}");
    }
    blobs_repo.put_from_store(hash).await?;
    if !blobs_repo.has_hash(hash).await? {
        eyre::bail!("blob materialization failed for hash {hash}");
    }
    Ok(())
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
