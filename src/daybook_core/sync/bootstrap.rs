use crate::interlude::*;

use super::{CLONE_PROVISION_ALPN, IROH_CLONE_URL_SCHEME, IrohSyncRepo};

use std::str::FromStr;

use big_repo::SharedPartStore;
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
    /// ADR 007 §2: the repo config doc (third core doc).
    pub config_doc_id: Option<DocumentId>,
    pub device_name: Option<String>,
    pub(crate) authority_ids: crate::authority::RepoAuthorityIds,
}

#[derive(Debug, Clone)]
pub struct CloneRepoInitOptions {
    pub timeout: std::time::Duration,
    pub repo_options: crate::repo::RepoOpenOptions,
}

impl Default for CloneRepoInitOptions {
    fn default() -> Self {
        Self {
            timeout: std::time::Duration::from_secs(30),
            repo_options: crate::repo::RepoOpenOptions::default(),
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
    pub requester_contact_card: big_repo::keyhive_core::contact_card::ContactCard,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloneProvisionResponse {
    pub endpoint_addr: iroh::EndpointAddr,
    pub repo_id: String,
    pub repo_name: String,
    pub app_doc_id: String,
    pub drawer_doc_id: String,
    /// ADR 007 §2: the repo config doc (third core doc). Serde-default so
    /// older peers (pre-config-doc) still deserialize.
    #[serde(default)]
    pub config_doc_id: Option<String>,
    pub device_name: Option<String>,
    pub repo_agents_group: [u8; 32],
    pub core_docs_group: [u8; 32],
    pub content_docs_group: [u8; 32],
    pub default_drawer_group: [u8; 32],
    pub blob_inventories_group: [u8; 32],
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
        let ep = self.router.endpoint();
        let addrs: Vec<iroh::TransportAddr> = ep
            .bound_sockets()
            .into_iter()
            .map(iroh::TransportAddr::Ip)
            .collect();
        iroh::EndpointAddr::from_parts(ep.id(), addrs)
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
            config_doc_id: self
                .config_doc_id
                .as_deref()
                .map(DocumentId::from_str)
                .transpose()
                .wrap_err("invalid config_doc_id in clone response")?,
            device_name: self.device_name.clone(),
            authority_ids: crate::authority::RepoAuthorityIds {
                repo_agents: self.repo_agents_group,
                core_docs: self.core_docs_group,
                content_docs: self.content_docs_group,
                default_drawer: self.default_drawer_group,
                blob_inventories: self.blob_inventories_group,
            },
        })
    }
}

// ---------------------------------------------------------------------------
// Client-side RPC helpers
// ---------------------------------------------------------------------------

async fn bind_bootstrap_endpoint(secret_key: Option<iroh::SecretKey>) -> Res<iroh::Endpoint> {
    let address_lookup = iroh::address_lookup::MemoryLookup::default();
    let mut builder =
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal).address_lookup(address_lookup);
    if let Some(key) = secret_key {
        builder = builder.secret_key(key);
    }
    #[cfg(test)]
    {
        builder = builder
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled);
    }
    let endpoint = builder.bind().await?;
    Ok(endpoint)
}

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
    let endpoint = bind_bootstrap_endpoint(None).await?;
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
    let endpoint = bind_bootstrap_endpoint(None).await?;
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

#[tracing::instrument(skip(
    big_repo,
    blobs_repo,
    partition_store,
    blob_part_store,
    iroh_secret_key,
    bootstrap,
    options
))]
pub async fn connect_and_pull_required_partitions_once(
    big_repo: &SharedBigRepo,
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    partition_store: &SharedPartStore,
    blob_part_store: &SharedPartStore,
    iroh_secret_key: iroh::SecretKey,
    bootstrap: &SyncBootstrapState,
    options: &CloneRepoInitOptions,
) -> Res<()> {
    let timeout = utils_rs::scale_timeout(options.timeout);
    let address_lookup = iroh::address_lookup::MemoryLookup::default();
    let endpoint = bind_bootstrap_endpoint(Some(iroh_secret_key)).await?;
    let result: Res<()> = pull_required_partitions_via_big_sync_worker(
        big_repo,
        blobs_repo,
        partition_store,
        blob_part_store,
        &endpoint,
        address_lookup,
        bootstrap,
        timeout,
        options.repo_options.sync_max_task_backoff,
    )
    .await;
    endpoint.close().await;
    result
}

/// Pulls initial required core partitions from the remote peer during clone bootstrap.
///
/// # Architectural Constraints & Anti-Regression Rules
/// DO NOT ADD HACKS OR WORKAROUNDS TO THIS FUNCTION.
/// Specifically:
/// - **NO Polling/Sleep Loops**: `wait_for_full_sync` and `wait_for_keyhive_reconciliation`
///   are deterministic, event-driven async futures. Polling loops mask underlying event-delivery failures.
/// - **NO Arbitrary Retries**: Reliance must be placed strictly on the event-driven futures provided
///   by `big_sync` and `big_repo`.
///
/// # Core Operational Sequence
/// 1. **Keyhive Sub & Reconciliation**: Subscribes to remote keyhive updates via `subscribe_keyhive_changes`
///    and waits for `wait_for_keyhive_reconciliation`. This guarantees all encryption keys and identity
///    credentials needed for the core document spaces are present locally.
/// 2. **Peer Registration**: Registers the peer with `big_sync_worker.set_peer` for the required
///    bootstrap partitions (`core_docs_partition_id`, `content_docs_partition_id`, `drawer_partition_id`).
/// 3. **Partition Reconciliation**: Invokes `big_sync_worker.wait_for_full_sync` on the required
///    partitions. `big_sync` compares partition summaries, fetches missing objects, and ensures
///    full convergence.
/// 4. **Bootstrap Document Materialization**: Calls `sync_doc_with_peer` specifically for `app_doc_id`
///    and `drawer_doc_id` to ensure the core application and drawer handles are ready for `finish_clone_init`.
#[tracing::instrument(skip_all)]
#[expect(clippy::too_many_arguments)]
async fn pull_required_partitions_via_big_sync_worker(
    big_repo: &SharedBigRepo,
    blobs_repo: &Arc<crate::blobs::BlobsRepo>,
    partition_store: &SharedPartStore,
    blob_part_store: &SharedPartStore,
    endpoint: &iroh::Endpoint,
    address_lookup: iroh::address_lookup::MemoryLookup,
    bootstrap: &SyncBootstrapState,
    timeout: std::time::Duration,
    max_task_backoff: Option<std::time::Duration>,
) -> Res<()> {
    let core_docs_partition_id = big_repo::group_part_id(bootstrap.authority_ids.core_docs);
    let content_docs_partition_id = big_repo::group_part_id(bootstrap.authority_ids.content_docs);
    let drawer_partition_id = big_repo::group_part_id(bootstrap.authority_ids.default_drawer);
    let blob_sync_backend = Arc::new(crate::blobs::sync::BlobSyncBackend::new(
        Arc::clone(blobs_repo),
        Arc::clone(blob_part_store),
        endpoint.clone(),
        address_lookup,
    ));
    let mut doc_sync_backends = HashMap::new();
    let repo_backend_id = big_repo::BigRepo::BACKEND_ID.into();
    let repo_sync_backend = Arc::new(
        big_repo::BigRepoSyncBackend::boot(Arc::downgrade(big_repo))
            .await
            .wrap_err("failed booting big repo sync backend")?,
    );
    doc_sync_backends.insert(
        Arc::clone(&repo_backend_id),
        Arc::clone(&repo_sync_backend) as _,
    );
    let mut blob_sync_backends = HashMap::new();
    blob_sync_backends.insert(
        super::BLOBS_BACKEND_ID.into(),
        Arc::clone(&blob_sync_backend) as _,
    );
    let (big_sync_worker, big_sync_worker_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(partition_store),
        doc_sync_backends,
        "daybook-docs",
        max_task_backoff,
        Arc::from("daybook-core"),
    )?;
    let (blob_sync_worker, blob_sync_worker_stop) = big_sync::spawn_big_sync_worker_with_options(
        Arc::clone(blob_part_store),
        blob_sync_backends,
        "daybook-blobs",
        max_task_backoff,
        Arc::from("daybook-blobs"),
    )?;
    let (big_sync_rpc, big_sync_rpc_stop) =
        big_sync::rpc::spawn_big_sync_rpc(std::collections::HashMap::from([
            (Arc::from("daybook-core"), Arc::clone(partition_store) as _),
            (Arc::from("daybook-blobs"), Arc::clone(blob_part_store) as _),
        ]))
        .await?;
    let (repo_rpc, repo_rpc_stop_token) =
        big_repo::rpc::spawn_repo_rpc(Arc::clone(big_repo)).await?;

    let _router = iroh::protocol::Router::builder(endpoint.clone())
        .accept(
            big_sync::rpc::BIG_SYNC_RPC_ALPN,
            big_sync_rpc.protocol_handler(),
        )
        .accept(big_repo::rpc::REPO_SYNC_ALPN, repo_rpc.protocol_handler())
        .spawn();

    let peer_id = PeerId::new(*bootstrap.endpoint_id.as_bytes());
    let conn = big_repo
        .open_connection_iroh(
            endpoint.clone(),
            bootstrap.endpoint_addr.clone(),
            peer_id,
            None,
        )
        .await?;
    repo_rpc.register_peer(bootstrap.endpoint_id, conn.peer_id);
    let remote_repo_rpc =
        big_repo::rpc::IrohBigRepoRpcClient::new(endpoint.clone(), bootstrap.endpoint_addr.clone());
    let mut keyhive_changes =
        tokio::time::timeout(timeout, remote_repo_rpc.subscribe_keyhive_changes(8))
            .await
            .map_err(|_| ferr!("timed out installing clone Keyhive subscription"))??;
    let ready = tokio::time::timeout(timeout, keyhive_changes.recv())
        .await
        .map_err(|_| ferr!("timed out waiting for clone Keyhive subscription readiness"))??
        .ok_or_else(|| ferr!("clone Keyhive subscription closed before readiness"))?;
    if !ready.initial {
        eyre::bail!("clone Keyhive subscription did not send its readiness event");
    }
    tokio::time::timeout(timeout, big_repo.sync_keyhive_with_peer(peer_id))
        .await
        .map_err(|_| eyre::eyre!("timed out syncing keyhive during clone"))??;
    let big_sync_rpc_client =
        big_sync::rpc::IrohBigSyncRpcClient::new(endpoint.clone(), bootstrap.endpoint_addr.clone());
    let big_sync_rpc_client: Arc<dyn big_sync::rpc::WireBigSyncRpcClient> =
        Arc::new(big_sync_rpc_client);

    let initial_partitions: HashMap<PartId, big_sync::BackendId> = [
        (core_docs_partition_id, Arc::clone(&repo_backend_id)),
        (content_docs_partition_id, Arc::clone(&repo_backend_id)),
        (drawer_partition_id, Arc::clone(&repo_backend_id)),
        // booted and loaded the blob stores; they are not part of the initial clone
        // barrier.
    ]
    .into_iter()
    .collect();

    big_sync_worker
        .set_peer(
            peer_id,
            Arc::clone(&big_sync_rpc_client),
            initial_partitions.clone(),
            HashMap::new(),
        )
        .await?;
    // The blob worker is registered with the peer so blob-scope sync requests
    // from the seed are served during the clone connection. Blob inventory
    // parts are populated lazily after the repo finishes booting, so there are
    // no parts to register here; the seed's blob worker probes our blob scope
    // via the RPC registry and sees an empty store until then.
    blob_sync_worker
        .set_peer(peer_id, big_sync_rpc_client, HashMap::new(), HashMap::new())
        .await?;

    let timeout_result = tokio::time::timeout(timeout, async {
        // Only wait on the partitions that are guaranteed to exist during clone bootstrap.
        // Blob-scope partitions are populated lazily as blobs/plugs appear after the repo
        // finishes booting, so requiring them here would make clone bootstrap race normal
        // repo initialization.
        let required_partitions = [
            core_docs_partition_id,
            content_docs_partition_id,
            drawer_partition_id,
        ];
        big_sync_worker
            .wait_for_full_sync(vec![peer_id], required_partitions)
            .await?;

        let mut bootstrap_docs = vec![
            ("app", bootstrap.app_doc_id),
            ("drawer", bootstrap.drawer_doc_id),
        ];
        if let Some(config_doc_id) = bootstrap.config_doc_id {
            bootstrap_docs.push(("config", config_doc_id));
        }
        for (role, doc_id) in bootstrap_docs {
            tracing::info!(%doc_id, role, "clone bootstrap document sync begin");
            big_repo
                .sync_doc_with_peer(doc_id, peer_id)
                .await
                .wrap_err_with(|| {
                    format!("clone bootstrap failed syncing {role} document {doc_id}")
                })?;
            tracing::info!(%doc_id, role, "clone bootstrap document sync complete");
        }
        Ok(())
    })
    .await;

    match timeout_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(err),
        Err(_) => {
            eyre::bail!("timed out waiting for required partitions during clone");
        }
    }

    big_sync_rpc_stop.stop().await?;
    repo_rpc_stop_token.stop().await?;
    big_sync_worker_stop.stop().await?;
    blob_sync_worker_stop.stop().await?;

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
    let layout = crate::repo::RepoLayout {
        repo_root: destination.clone(),
        big_repo_root: destination.join("big_repo"),
        sqlite_path: destination.join("sqlite.db"),
        blobs_root: destination.join("blobs"),
        marker_path: destination.join("db.repo.txt"),
        lock_path: destination.join("repo.lock"),
    };
    let lock_guard = crate::repo::RepoLockGuard::acquire(staging.join("repo.lock")).await?;

    let cloned = async {
        let secret_repo = crate::secrets::SecretRepo::boot().await?;

        // Generate identity locally — secret keys never leave the device.
        let local_secret = iroh::SecretKey::generate();
        let local_public = local_secret.public();
        let sqlite_path = staging.join("sqlite.db");
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::file(sqlite_path)).await?;
        let (sqlite_local_state_repo, sqlite_local_state_stop) =
            crate::local_state::SqliteLocalStateRepo::boot(staging.join("local_state")).await?;
        let checkout_id = {
            let id = Uuid::new_v4();
            let id = utils_rs::hash::encode_base58_multibase(id);
            format!("dcheckout_{id}")
        };
        let identity = secret_repo
            .set_identity(&checkout_id, local_secret.clone())
            .await?;
        let (big_repo, big_repo_stop) = big_repo::BigRepo::boot(big_repo::Config {
            node_identity_seed: identity.iroh_secret_key.to_bytes(),
            storage: big_repo::StorageConfig::Disk {
                path: staging.join("big_repo"),
            },
            scope_key: Arc::from("daybook-core"),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await?;
        let provision = request_clone_provision_from_url(
            source_url,
            RequestCloneProvisionReq {
                requested_device_name: Some(format!("clone-{}", std::env::consts::ARCH)),
                requester_endpoint_id: local_public.to_string(),
                requester_contact_card: big_repo.local_keyhive_contact_card(),
            },
        )
        .await?;
        let bootstrap = provision.to_bootstrap_state()?;

        let local_peer_key = daybook_types::doc::format_peer_key(local_public.as_bytes());

        crate::repo::globals::set_string_global(&sql, "global.repo_id", &bootstrap.repo_id).await?;
        crate::authority::persist_ids(&sql, bootstrap.authority_ids).await?;
        crate::repo::globals::set_string_global(&sql, "global.repo_name", &bootstrap.repo_name)
            .await?;
        crate::repo::globals::set_string_global(&sql, "global.checkout_id", &checkout_id).await?;
        let user_id = format!(
            "{}{}",
            daybook_types::doc::user_path::USER_ID_PREFIX,
            Uuid::new_v4().bs58()
        );
        crate::repo::globals::set_string_global(&sql, "global.user_id", &user_id).await?;

        let pkey_bs58 = utils_rs::hash::encode_base58_multibase(local_public.as_bytes());
        let device_id = format!(
            "{}{}",
            daybook_types::doc::user_path::DEVICE_ID_PREFIX,
            pkey_bs58
        );
        let local_device_name = bootstrap
            .device_name
            .clone()
            .unwrap_or_else(|| format!("clone-{}", std::env::consts::ARCH));
        let local_user_path = daybook_types::doc::UserPathBuf::new()
            .join("/")
            .join(user_id)
            .join(device_id);
        let local_actor_id = daybook_types::doc::user_path::to_actor_id(
            &daybook_types::doc::UserPathBuf::from(local_user_path.clone()),
        );
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
                    agent_peer_id: None,
                    name: bootstrap
                        .device_name
                        .clone()
                        .unwrap_or_else(|| bootstrap.endpoint_id.to_string()),
                    added_at: jiff::Timestamp::now(),
                    last_connected_at: None,
                });
            crate::repo::globals::set_sync_config(&sql, &sync_config).await?;
        }

        let part_store = big_repo.shared_part_store();
        let blob_part_store = crate::repo::open_blob_part_store(big_repo.sql_ctx()).await?;
        let blobs_repo =
            crate::blobs::BlobsRepo::new(staging.join("blobs"), "clone-bootstrap".into()).await?;

        ensure_bootstrap_local_partitions(&part_store, &bootstrap).await?;

        connect_and_pull_required_partitions_once(
            &big_repo,
            &blobs_repo,
            &part_store,
            &blob_part_store,
            local_secret.clone(),
            &bootstrap,
            &options,
        )
        .await?;

        blobs_repo.shutdown().await?;

        crate::repo::globals::set_init_state(
            &sql,
            &crate::repo::globals::InitState::Created {
                doc_id_app: bootstrap.app_doc_id,
                doc_id_drawer: bootstrap.drawer_doc_id,
                doc_id_config: bootstrap.config_doc_id,
                core_inventory_doc_id: None,
                docs_inventory_doc_id: None,
            },
        )
        .await?;

        let rcx = crate::repo::finish_clone_init(crate::repo::RepoCtxParts {
            layout,
            lock_guard,
            options: options.repo_options.clone(),
            sql: sql.clone(),
            sqlite_local_state_repo,
            sqlite_local_state_stop: std::sync::Mutex::new(Some(sqlite_local_state_stop)),
            part_store: Arc::clone(&part_store),
            blob_part_store: Arc::clone(&blob_part_store),
            frontier_part_store: big_repo.frontier_part_store(),
            big_repo: Arc::clone(&big_repo),
            big_repo_stop: std::sync::Mutex::new(Some(big_repo_stop)),
            local_peer_key,
            local_actor_id,
            local_user_path,
            local_device_name,
            repo_id: bootstrap.repo_id.clone(),
            checkout_id,
            repo_name: bootstrap.repo_name.clone(),
            iroh_public_key: identity.iroh_public_key.to_string(),
            iroh_secret_key: identity.iroh_secret_key,
            secret_repo,
        })
        .await?;
        crate::repo::mark_repo_initialized(&staging).await?;

        rcx.shutdown().await?;
        Ok::<SyncBootstrapState, eyre::Report>(bootstrap)
    }
    .await;

    let bootstrap = match cloned {
        Ok(bootstrap) => bootstrap,
        Err(err) => {
            tokio::fs::remove_dir_all(&staging)
                .await
                .inspect_err(|err| error!("error deleting temp dir: {err}"))
                .ok();
            return Err(err);
        }
    };

    if destination.exists() {
        let mut read_dir = tokio::fs::read_dir(&destination).await?;
        if read_dir.next_entry().await?.is_some() {
            tokio::fs::remove_dir_all(&staging)
                .await
                .inspect_err(|err| error!("error deleting temp dir: {err}"))
                .ok();
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

async fn ensure_bootstrap_local_partitions(
    partition_store: &SharedPartStore,
    bootstrap: &SyncBootstrapState,
) -> Res<()> {
    let part_ids = [
        big_repo::group_part_id(bootstrap.authority_ids.core_docs),
        big_repo::group_part_id(bootstrap.authority_ids.content_docs),
        big_repo::group_part_id(bootstrap.authority_ids.default_drawer),
    ];
    for part_id in part_ids {
        partition_store.ensure_part(part_id).await?;
    }
    Ok(())
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
