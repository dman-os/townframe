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

pub type PeerId = Arc<str>;

#[derive(Debug, Clone)]
struct SubductionProtocolHandler {
    big_repo: Arc<BigRepo>,
    endpoint: iroh::Endpoint,
    incoming_conn_tx: mpsc::UnboundedSender<big_repo::BigRepoConnection>,
    end_signal_tx: mpsc::UnboundedSender<big_repo::ConnFinishSignal>,
}

impl ProtocolHandler for SubductionProtocolHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            worker = "subduction-accept",
            otel.kind = "server",
            peer_id = tracing::field::Empty,
        ),
    )]
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
        // The peer is only known once the connection is accepted.
        tracing::Span::current().record("peer_id", tracing::field::display(&conn.peer_id));
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
        peer_key: PeerId,
        // The connection's end flag (shared with the runtime's watcher),
        // used to identify which connection a `ConnFinishSignal` belongs to.
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct ProcessorActivation {
    epoch: uuid::Uuid,
    pool: crate::tasks::TaskPoolId,
    capabilities: crate::tasks::CapabilitySummary,
    capacity: crate::tasks::Capacity,
    timing: crate::tasks::driver::SchedulingTiming,
}

struct ProcessorAdoption {
    epoch: uuid::Uuid,
    runtime: std::sync::Weak<crate::rt::Rt>,
    cancel: CancellationToken,
    handle: JoinHandle<()>,
    ready:
        tokio::sync::watch::Receiver<Option<crate::rt::triage::domain::ProcessorConfigCandidate>>,
}

type ProcessorAdoptions = Arc<tokio::sync::Mutex<HashMap<String, ProcessorAdoption>>>;

struct TaskPeerRoute {
    client: Arc<dyn big_sync::rpc::WireBigSyncRpcClient>,
    address: iroh::EndpointAddr,
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
    progress_repo: Option<Arc<ProgressRepo>>,

    conn_end_signal_tx: mpsc::UnboundedSender<big_repo::ConnFinishSignal>,
    active_peers: tokio::sync::RwLock<HashMap<PeerKey, ActivePeerState>>,
    peer_registration_changed: tokio::sync::Notify,
    // sync_store: am_utils_rs::sync::store::SyncStoreHandle,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    big_sync_worker: big_sync::BigSyncWorkerHandle,
    blob_sync_worker: big_sync::BigSyncWorkerHandle,
    task_sync_worker: big_sync::BigSyncWorkerHandle,
    task_transport: Arc<crate::tasks::transport::TaskSyncBackend>,
    task_driver: Arc<crate::tasks::driver::NativeTaskDriver>,
    task_route_lock: Arc<tokio::sync::Mutex<()>>,
    task_peer_clients: std::sync::Mutex<HashMap<PeerKey, TaskPeerRoute>>,
    task_control_wake: tokio::sync::watch::Sender<()>,
    installed_peer_routes: std::sync::Mutex<HashMap<PeerKey, [HashMap<PartKey, BackendId>; 3]>>,
    processor_adoptions: ProcessorAdoptions,
    #[cfg(test)]
    task_permission_progress: tokio::sync::watch::Sender<u64>,
    big_repo_rpc: big_repo::rpc::BigRepoRpcHandle,
    _big_sync_rpc: big_sync::rpc::BigSyncRpcHandle,
}

#[derive(Debug, Clone)]
pub enum IrohSyncEvent {
    IncomingConnection {
        peer_key: PeerId,
    },
    OutgoingConnection {
        peer_key: PeerId,
    },
    ConnectionClosed {
        peer_key: PeerId,
        reason: String,
    },
    PeerFullySynced {
        peer_key: PeerId,
        doc_count: usize,
    },
    PartitionFullySynced {
        peer_key: PeerId,
        partition: String,
    },
    DocSyncedWithPeer {
        peer_key: PeerId,
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
        peer_key: PeerId,
    },
}

/// Records the order in which [`IrohSyncRepoStopToken::stop`] tears its children
/// down.
///
/// A stop-order inversion is invisible in any single run — every child here
/// stops successfully in any order, and the damage is a rare teardown race — so
/// the ordering the code relies on is recorded rather than left to a comment.
/// The comment on `blob_inventory_permission_stop` states the rule: the
/// inventory writer serves the store the blob worker and the RPC server serve
/// from, so it stops after both are down.
///
/// Zero-sized without `cfg(test)`: an unarmed recorder drops every record and
/// the token pays nothing for it.
#[derive(Clone, Default)]
pub(crate) struct ShutdownOrder {
    #[cfg(test)]
    recorded: Option<Arc<std::sync::Mutex<Vec<&'static str>>>>,
}

impl ShutdownOrder {
    /// A recorder that keeps what it is given. Armed in test builds only.
    fn for_build() -> Self {
        #[cfg(test)]
        {
            Self {
                recorded: Some(Arc::new(std::sync::Mutex::new(Vec::new()))),
            }
        }
        #[cfg(not(test))]
        {
            Self::default()
        }
    }

    /// Record one child's teardown. A no-op without a recorder.
    #[cfg_attr(not(test), allow(unused_variables))]
    fn record(&self, child: &'static str) {
        #[cfg(test)]
        if let Some(recorded) = &self.recorded {
            recorded.lock().expect(ERROR_MUTEX).push(child);
        }
    }

    /// The children recorded so far, in teardown order.
    #[cfg(test)]
    pub(crate) fn recorded(&self) -> Vec<&'static str> {
        self.recorded
            .as_ref()
            .expect("a recorder armed for the test build")
            .lock()
            .expect(ERROR_MUTEX)
            .clone()
    }
}

pub struct IrohSyncRepoStopToken {
    cancel_token: CancellationToken,
    worker_handle: JoinHandle<()>,
    reconnect_task: Arc<std::sync::Mutex<Option<JoinHandle<()>>>>,
    router: iroh::protocol::Router,
    task_sync_worker_stop: big_sync::StopToken,
    // partition_sync_stop_token: am_utils_rs::sync::node::SyncNodeStopToken,
    big_repo_rpc_stop_token: big_repo::rpc::BigRepoRpcStopToken,
    big_sync_rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    big_sync_worker_stop: big_sync::StopToken,
    blob_sync_worker_stop: big_sync::StopToken,
    /// The blob-inventory access-row writer. It writes into the store the blob worker and the
    /// RPC server serve from, so it stops after both are down.
    blob_inventory_permission_stop: crate::repos::RepoStopToken,
    /// Task authority rows remain maintained while the common worker/RPC serve.
    task_permission_stop: crate::repos::RepoStopToken,
    /// Control exchange may await a remote peer; it never blocks permission maintenance.
    task_control_stop: crate::repos::RepoStopToken,
    task_route_lock: Arc<tokio::sync::Mutex<()>>,
    task_driver: Arc<crate::tasks::driver::NativeTaskDriver>,
    processor_adoptions: ProcessorAdoptions,
    /// The order the children above were stopped in. Zero-sized outside tests.
    shutdown_order: ShutdownOrder,
    // partition_sync_store_stop_token: am_utils_rs::sync::store::SyncStoreStopToken,
}

impl IrohSyncRepoStopToken {
    /// The teardown record this token writes as it stops its children.
    #[cfg(test)]
    pub(crate) fn shutdown_order(&self) -> ShutdownOrder {
        self.shutdown_order.clone()
    }

    pub async fn stop(self) -> Res<()> {
        self.cancel_token.cancel();
        let adoptions = std::mem::take(&mut *self.processor_adoptions.lock().await);
        for (_, adoption) in adoptions {
            adoption.cancel.cancel();
            adoption.handle.await?;
        }
        let reconnect_handle = self.reconnect_task.lock().expect(ERROR_MUTEX).take();
        if let Some(handle) = reconnect_handle {
            utils_rs::wait_on_handle_with_timeout(
                handle,
                utils_rs::scale_timeout(Duration::from_secs(60)),
            )
            .await?;
        }
        self.task_control_stop.stop().await?;
        self.shutdown_order.record("task_control_stop");
        // Stop actual scheduling consumers/streams before their native identity,
        // retained store, ephemeral and endpoint dependencies.
        self.task_driver.stop().await?;
        self.shutdown_order.record("task_driver_stop");
        // Quiesce route mutation before stopping its worker dependency. The
        // authority task is independently cancellable even while waiting here.
        let quiesced_routes = self.task_route_lock.lock().await;
        // pre light the stop signal to the full worker
        self.big_sync_worker_stop.stop().await?;
        self.shutdown_order.record("big_sync_worker_stop");
        self.task_sync_worker_stop.stop().await?;
        self.shutdown_order.record("task_sync_worker_stop");
        self.big_sync_rpc_stop.stop().await?;
        self.shutdown_order.record("big_sync_rpc_stop");
        self.blob_sync_worker_stop.stop().await?;
        self.shutdown_order.record("blob_sync_worker_stop");
        self.big_repo_rpc_stop_token.stop().await?;
        self.shutdown_order.record("big_repo_rpc_stop_token");
        self.task_permission_stop.stop().await?;
        self.shutdown_order.record("task_permission_stop");
        drop(quiesced_routes);
        // The inventory writer serves the store the blob worker and the RPC
        // server serve from, so its stop is issued once both are down — and the
        // record after it is what makes that order assertable.
        self.blob_inventory_permission_stop.stop().await?;
        self.shutdown_order.record("blob_inventory_permission_stop");
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

impl RepoCtx {
    async fn processor_activation(&self, processor: &str) -> Res<Option<ProcessorActivation>> {
        let value = crate::repo::globals::get_string_global(
            &self.sql,
            &format!("local.processor-activation/{processor}"),
        )
        .await?;
        value
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    pub(crate) async fn clear_processor_activation(&self, processor: &str) -> Res<()> {
        let mut tokens = self.processor_activation_tokens.lock().await;
        sqlx::query("DELETE FROM kvstore WHERE key = ?")
            .bind(format!("local.processor-activation/{processor}"))
            .execute(&self.sql.write_pool)
            .await?;
        if let Some(cancel) = tokens.remove(processor) {
            cancel.cancel();
        }
        Ok(())
    }

    pub(crate) async fn register_processor_runtime(&self, rt: &Arc<crate::rt::Rt>) -> Res<()> {
        let mut owners = self.processor_runtime_owners.lock().await;
        owners.0 = Arc::downgrade(rt);
        if let Some(sync) = owners.1.upgrade()
            && !sync.cancel_token.is_cancelled()
        {
            sync.restore_processor_activations(rt).await?;
        }
        Ok(())
    }

    async fn register_processor_transport(&self, sync: &Arc<IrohSyncRepo>) -> Res<()> {
        let mut owners = self.processor_runtime_owners.lock().await;
        owners.1 = Arc::downgrade(sync);
        if let Some(rt) = owners.0.upgrade()
            && !rt.cancel_token.is_cancelled()
        {
            sync.restore_processor_activations(&rt).await?;
        }
        Ok(())
    }
}

impl IrohSyncRepo {
    #[tracing::instrument(level = "debug", skip_all, err(Debug), fields(worker = "iroh-sync"))]
    pub async fn boot(
        rcx: Arc<RepoCtx>,
        config_repo: Arc<crate::config::ConfigRepo>,
        blobs_repo: Arc<BlobsRepo>,
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
        // The blob-inventory parts are served to peers through `big_sync_rpc` below, and a part
        // with no access rows refuses everyone (ADR 013 decision A1/A2). The writer is spawned
        // here, at the boundary that serves those parts, rather than in `Rt`: a process that
        // syncs without an `Rt` (the clone path, and any headless sync) would otherwise serve
        // parts whose rows were never written, which reads to a peer as an unknown part and
        // blocks `wait_for_full_sync` forever.
        let blob_inventory_permission_stop = crate::blobs::spawn_blob_inventory_permission_writer(
            Arc::clone(&rcx.blob_part_store),
            Arc::clone(&rcx.big_repo),
            vec![
                rcx.core_inventory_doc_id.clone(),
                rcx.docs_inventory_doc_id.clone(),
            ],
            cancel_token.clone(),
        )
        .await?;
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
        let task_transport =
            Arc::new(crate::tasks::transport::TaskSyncBackend::boot(Arc::clone(&rcx)).await?);
        let (task_driver, scheduling_requests) = crate::tasks::driver::NativeTaskDriver::boot(
            Arc::clone(&rcx.big_repo),
            Arc::clone(&task_transport),
            endpoint.clone(),
            big_repo_rpc.clone(),
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
            // Bucket-diff, explicitly: this embedder used to opt out because the machine
            // stalled in its offline-reopen path; `bucket_band_reconciles_after_offline_reopen`
            // in big_repo now covers that path, so the band is on.
            Some(big_sync::SyncMode::Bucket),
            Arc::from("daybook-core"),
        )?;

        let mut blob_sync_backends = std::collections::HashMap::new();
        blob_sync_backends.insert(BLOBS_BACKEND_ID.into(), blob_sync_backend);
        let (blob_sync_worker, blob_sync_worker_stop) =
            big_sync::spawn_big_sync_worker_with_options(
                Arc::clone(&rcx.blob_part_store),
                blob_sync_backends,
                crate::repo::BLOB_SCOPE_KEY,
                max_task_backoff,
                // Bucket-diff, explicitly: see the note on the docs worker above.
                Some(big_sync::SyncMode::Bucket),
                Arc::from(crate::repo::BLOB_SCOPE_KEY),
            )?;
        let (task_sync_worker, task_sync_worker_stop) =
            big_sync::spawn_big_sync_worker_with_options(
                task_transport.shared_store(),
                HashMap::from([(
                    crate::tasks::transport::TASK_BACKEND_ID.into(),
                    Arc::clone(&task_transport) as Arc<dyn big_sync::SyncBackend>,
                )]),
                crate::tasks::transport::TASK_SCOPE_KEY,
                max_task_backoff,
                Some(big_sync::SyncMode::Bucket),
                Arc::from(crate::tasks::transport::TASK_SCOPE_KEY),
            )?;

        let (big_sync_rpc, big_sync_rpc_stop) =
            big_sync::rpc::spawn_big_sync_rpc(std::collections::HashMap::from([
                (Arc::from("daybook-core"), Arc::clone(&rcx.part_store) as _),
                (
                    Arc::from(crate::repo::BLOB_SCOPE_KEY),
                    Arc::clone(&rcx.blob_part_store) as _,
                ),
                (
                    Arc::from(crate::tasks::transport::TASK_SCOPE_KEY),
                    task_transport.shared_store() as _,
                ),
            ]))
            .await?;
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
            .accept(
                crate::tasks::rpc::TASK_COORDINATION_ALPN,
                crate::tasks::rpc::TaskCoordinationProtocolHandler::new(
                    big_repo_rpc.clone(),
                    scheduling_requests,
                ),
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
            progress_repo,
            cancel_token: cancel_token.clone(),
            registry: crate::repos::ListenersRegistry::new(),
            active_peers: default(),
            peer_registration_changed: default(),
            conn_end_signal_tx: conn_end_tx,
            reconnect_task: Arc::clone(&reconnect_task),
            big_sync_worker,
            blob_sync_worker,
            task_sync_worker,
            task_transport,
            task_driver,
            task_route_lock: Arc::new(tokio::sync::Mutex::new(())),
            task_peer_clients: Default::default(),
            task_control_wake: tokio::sync::watch::channel(()).0,
            installed_peer_routes: Default::default(),
            processor_adoptions: Default::default(),
            #[cfg(test)]
            task_permission_progress: tokio::sync::watch::channel(0).0,
            big_repo_rpc: big_repo_rpc.clone(),
            _big_sync_rpc: big_sync_rpc,
        });
        repo.task_transport.set_native_owner(&repo);
        let task_permission_stop = repo.spawn_task_permission_writer().await?;
        let task_control_stop = repo.spawn_task_control_exchange();
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
        let task_route_lock = Arc::clone(&repo.task_route_lock);
        let task_driver = Arc::clone(&repo.task_driver);
        let processor_adoptions = Arc::clone(&repo.processor_adoptions);

        let stop = IrohSyncRepoStopToken {
            cancel_token,
            worker_handle,
            reconnect_task,
            router,
            big_repo_rpc_stop_token: repo_rpc_stop_token,
            big_sync_rpc_stop,
            big_sync_worker_stop,
            blob_sync_worker_stop,
            task_sync_worker_stop,
            blob_inventory_permission_stop,
            task_permission_stop,
            task_control_stop,
            task_route_lock,
            task_driver,
            processor_adoptions,
            shutdown_order: ShutdownOrder::for_build(),
        };
        if let Err(error) = repo.rcx.register_processor_transport(&repo).await {
            stop.stop()
                .await
                .wrap_err("tearing down failed processor activation restore")?;
            return Err(error);
        }
        Ok((repo, stop))
    }

    fn ensure_repo_live(&self) -> Res<()> {
        if self.cancel_token.is_cancelled() {
            eyre::bail!("repo is shutting down");
        }
        Ok(())
    }
}

impl IrohSyncRepo {
    /// Attach an explicitly provisioned pool to the native daybook-tasks worker
    /// and currently connected peers. Repeated opens share the retained owner.
    /// An opaque relay must receive this metadata from its provisioner; this
    /// operation neither discovers private descriptors nor grants payload Read.
    pub async fn attach_task_pool(
        &self,
        snapshot: crate::tasks::pool::PoolDescriptorSnapshot,
    ) -> Res<Arc<crate::tasks::store::TaskStore>> {
        let _routes = self.task_route_lock.lock().await;
        self.ensure_repo_live()?;
        let tasks = self.task_transport.attach(snapshot, None).await?;
        self.refresh_task_peer_routes().await?;
        Ok(tasks)
    }

    /// Fetch authenticated current transport control from a native peer. This
    /// requires explicit pool/group sponsorship, not Read or executor participation.
    pub async fn discover_task_pool(
        &self,
        peer: iroh::EndpointAddr,
        pool: crate::tasks::TaskPoolId,
        group: [u8; 32],
    ) -> Res<Arc<crate::tasks::store::TaskStore>> {
        use crate::tasks::rpc::{
            ControlRejection, CurrentPoolControlRequest, TASK_COORDINATION_ALPN,
            TaskCoordinationRpc,
        };
        self.ensure_repo_live()?;
        let client = irpc_iroh::client::<TaskCoordinationRpc>(
            self.router.endpoint().clone(),
            peer,
            TASK_COORDINATION_ALPN,
        );
        let control = match client
            .rpc(CurrentPoolControlRequest {
                pool: pool.clone(),
                offered: None,
            })
            .await?
        {
            Ok(control) => control,
            Err(ControlRejection::Pending) => {
                return Err(big_repo::CoordinationError::Pending.into());
            }
            Err(ControlRejection::Unauthorized) => {
                return Err(big_repo::CoordinationError::Unauthorized.into());
            }
            Err(ControlRejection::Invalid(reason)) => {
                eyre::bail!("pool control rejected: {reason}")
            }
        };
        self.adopt_current_pool_control(&pool, group, control).await
    }

    /// Recover durably admitted control without contacting its publisher. This
    /// rechecks current native sponsorship and the original author's Edit access.
    pub async fn resume_task_pool(
        &self,
        pool: crate::tasks::TaskPoolId,
        group: [u8; 32],
    ) -> Res<Arc<crate::tasks::store::TaskStore>> {
        self.ensure_repo_live()?;
        let control = self
            .task_transport
            .cached_current_control(&pool)
            .await?
            .ok_or_eyre("no durably admitted current pool control")?;
        self.adopt_current_pool_control(&pool, group, control).await
    }

    async fn adopt_current_pool_control(
        &self,
        pool: &crate::tasks::TaskPoolId,
        group: [u8; 32],
        control: crate::tasks::pool::PoolRotationControl,
    ) -> Res<Arc<crate::tasks::store::TaskStore>> {
        let mut snapshot = control.admit(&self.rcx.big_repo, pool, group).await?;
        let _routes = self.task_route_lock.lock().await;
        self.ensure_repo_live()?;
        self.task_transport
            .retain_control_sources(&mut snapshot)
            .await?;
        let tasks = self.task_transport.attach(snapshot, Some(&control)).await?;
        self.refresh_task_peer_routes().await?;
        Ok(tasks)
    }

    /// Attach provisioner-supplied processor-slot metadata without enabling execution.
    /// This also supports opaque relays: native Relay is rechecked locally, and
    /// neither private domain discovery nor payload-key materialization is required.
    pub async fn attach_processor_domain(
        &self,
        snapshot: &crate::rt::triage::domain::ProcessorDomainSnapshot,
    ) -> Res<()> {
        self.ensure_repo_live()?;
        let snapshot = snapshot.transport(&self.rcx.big_repo).await?;
        let _routes = self.task_route_lock.lock().await;
        self.ensure_repo_live()?;
        self.task_transport.attach_processor(snapshot, None).await?;
        self.refresh_task_peer_routes().await?;
        Ok(())
    }

    /// Fetch bounded authenticated slot control for explicit processor/group sponsorship.
    /// This neither reads shared execution configuration nor enables the processor.
    pub async fn discover_processor_domain(
        &self,
        peer: iroh::EndpointAddr,
        processor: &str,
        group: [u8; 32],
    ) -> Res<()> {
        use crate::tasks::rpc::{
            ControlRejection, CurrentProcessorControlRequest, TASK_COORDINATION_ALPN,
            TaskCoordinationRpc,
        };
        self.ensure_repo_live()?;
        let client = irpc_iroh::client::<TaskCoordinationRpc>(
            self.router.endpoint().clone(),
            peer,
            TASK_COORDINATION_ALPN,
        );
        let control = match client
            .rpc(CurrentProcessorControlRequest {
                processor: processor.into(),
                offered: None,
            })
            .await?
        {
            Ok(control) => control,
            Err(ControlRejection::Pending) => {
                return Err(big_repo::CoordinationError::Pending.into());
            }
            Err(ControlRejection::Unauthorized) => {
                return Err(big_repo::CoordinationError::Unauthorized.into());
            }
            Err(ControlRejection::Invalid(reason)) => {
                eyre::bail!("processor control rejected: {reason}")
            }
        };
        self.adopt_current_processor_control(processor, group, control)
            .await
    }

    /// Resume authenticated slot transport without metadata Read or a publisher dial.
    pub async fn resume_processor_domain(&self, processor: &str, group: [u8; 32]) -> Res<()> {
        self.ensure_repo_live()?;
        let control = self
            .task_transport
            .cached_processor_control(processor)
            .await?
            .ok_or_eyre("no durably admitted current processor control")?;
        self.adopt_current_processor_control(processor, group, control)
            .await
    }

    pub(crate) async fn fetch_register_source_control(
        &self,
        peer: &PeerKey,
        scope: &[u8],
        document: &big_repo::DocumentId,
        key_ref: &url::Url,
    ) -> Res<crate::tasks::RegisterSourceControl> {
        use crate::tasks::rpc::{
            ControlRejection, RegisterSourceControlRequest, TASK_COORDINATION_ALPN,
            TaskCoordinationRpc,
        };
        self.ensure_repo_live()?;
        let address = self
            .task_peer_clients
            .lock()
            .expect(ERROR_MUTEX)
            .get(peer)
            .map(|route| route.address.clone())
            .ok_or_eyre("register source peer disconnected")?;
        if self.big_repo_rpc.peer_for_endpoint(address.id) != Some(peer.clone()) {
            return Err(big_repo::CoordinationError::Unauthorized.into());
        }
        let client = irpc_iroh::client::<TaskCoordinationRpc>(
            self.router.endpoint().clone(),
            address.clone(),
            TASK_COORDINATION_ALPN,
        );
        let response = tokio::select! {
            _ = self.cancel_token.cancelled() => eyre::bail!("native source control stopped"),
            response = client.rpc(RegisterSourceControlRequest { register_scope: scope.to_vec(), authority_document: document.clone(), key_ref: key_ref.clone() }) => response?,
        };
        if self.big_repo_rpc.peer_for_endpoint(address.id) != Some(peer.clone()) {
            return Err(big_repo::CoordinationError::Unauthorized.into());
        }
        match response {
            Ok(control) => Ok(control),
            Err(ControlRejection::Pending) => Err(big_repo::CoordinationError::Pending.into()),
            Err(ControlRejection::Unauthorized) => {
                Err(big_repo::CoordinationError::Unauthorized.into())
            }
            Err(ControlRejection::Invalid(reason)) => {
                eyre::bail!("register source control rejected: {reason}")
            }
        }
    }

    async fn adopt_current_processor_control(
        &self,
        processor: &str,
        group: [u8; 32],
        control: crate::rt::triage::domain::ProcessorRotationControl,
    ) -> Res<()> {
        let snapshot = control
            .admit(&self.rcx.big_repo, processor, group, None)
            .await?;
        let _routes = self.task_route_lock.lock().await;
        self.ensure_repo_live()?;
        self.task_transport
            .attach_processor(snapshot, Some(&control))
            .await?;
        self.refresh_task_peer_routes().await?;
        Ok(())
    }

    /// Enable distributed execution only after management supplies both native
    /// authority loci. Descriptor attachment by itself never starts processors.
    #[expect(
        clippy::too_many_arguments,
        reason = "Explicit native bindings and scheduling policy must be supplied together"
    )]
    pub async fn attach_distributed_processor(
        self: &Arc<Self>,
        rt: Arc<crate::rt::Rt>,
        processor_full_id: String,
        reference: crate::rt::triage::domain::ProcessorDomainReference,
        pool: crate::tasks::PoolDescriptorSnapshot,
        capabilities: crate::tasks::CapabilitySummary,
        capacity: crate::tasks::Capacity,
        timing: crate::tasks::driver::SchedulingTiming,
    ) -> Res<bool> {
        let repo = crate::rt::triage::domain::ProcessorDomainRepo::new(
            Arc::clone(&self.rcx.big_repo),
            self.rcx.local_actor_id.clone(),
        );
        let selected = repo
            .publish_candidate(
                &self.rcx.doc_config.document_id(),
                &processor_full_id,
                crate::rt::triage::domain::ProcessorConfigCandidate {
                    generation: pool.descriptor.rotation_generation,
                    reference: reference.clone(),
                },
            )
            .await?;
        let intent = self
            .rcx
            .processor_activation(&processor_full_id)
            .await?
            .unwrap_or(ProcessorActivation {
                epoch: uuid::Uuid::new_v4(),
                pool: pool.descriptor.pool_id.clone(),
                capabilities: capabilities.clone(),
                capacity,
                timing,
            });
        eyre::ensure!(
            intent.pool == pool.descriptor.pool_id
                && intent.capabilities == capabilities
                && intent.capacity == capacity
                && intent.timing.heartbeat_interval == timing.heartbeat_interval
                && intent.timing.router_settle == timing.router_settle
                && intent.timing.router_takeover_after == timing.router_takeover_after,
            "live processor activation policy changed; detach before replacing it"
        );
        crate::repo::globals::upsert_string_global(
            &self.rcx.sql,
            &format!("local.processor-activation/{processor_full_id}"),
            &serde_json::to_string(&intent)?,
        )
        .await?;
        let activation_cancel = self
            .start_processor_adoption(&rt, &processor_full_id, intent.clone())
            .await?;
        loop {
            match self
                .apply_distributed_processor(
                    &rt,
                    &processor_full_id,
                    &reference,
                    &pool,
                    &capabilities,
                    capacity,
                    timing,
                    &selected,
                    intent.epoch,
                    &activation_cancel,
                )
                .await
            {
                Err(error)
                    if matches!(
                        error.downcast_ref::<big_repo::CoordinationError>(),
                        Some(big_repo::CoordinationError::Pending)
                    ) =>
                {
                    // The activation intent already has an owned watcher. A native
                    // generation race is not rejection or an activation acknowledgment;
                    // resample using the authority reader's prescribed idle bound.
                    tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY)
                        .await;
                }
                result => return result,
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Explicit local participation and selected native bindings"
    )]
    async fn apply_distributed_processor(
        &self,
        rt: &Arc<crate::rt::Rt>,
        processor_full_id: &str,
        reference: &crate::rt::triage::domain::ProcessorDomainReference,
        pool: &crate::tasks::PoolDescriptorSnapshot,
        capabilities: &crate::tasks::CapabilitySummary,
        capacity: crate::tasks::Capacity,
        timing: crate::tasks::driver::SchedulingTiming,
        selected: &crate::rt::triage::domain::ProcessorConfigCandidate,
        activation_epoch: uuid::Uuid,
        activation_cancel: &CancellationToken,
    ) -> Res<bool> {
        let repo = crate::rt::triage::domain::ProcessorDomainRepo::new(
            Arc::clone(&self.rcx.big_repo),
            self.rcx.local_actor_id.clone(),
        );
        let (plug, processor) = processor_full_id
            .rsplit_once('/')
            .ok_or_eyre("processor identifier has no plug boundary")?;
        let locally_enabled = rt.plugs_repo.enabled_ref(plug).await?.is_some();
        if !locally_enabled {
            sqlx::query("DELETE FROM kvstore WHERE key = ? AND json_extract(value, '$.epoch') = ?")
                .bind(format!("local.processor-activation/{processor_full_id}"))
                .bind(activation_epoch.to_string())
                .execute(&self.rcx.sql.write_pool)
                .await?;
            self.stop_distributed_processor(rt, processor_full_id)
                .await?;
            return Ok(false);
        }
        if selected.reference != *reference
            || selected.generation != pool.descriptor.rotation_generation
        {
            self.stop_distributed_processor(rt, processor_full_id)
                .await?;
            return Ok(false);
        }
        let replacing = rt
            .distributed_processors
            .read()
            .await
            .get(processor_full_id)
            .is_some_and(|existing| {
                existing.domain != *reference
                    || existing.pool.reference != pool.reference
                    || existing.pool.descriptor != pool.descriptor
            });
        if replacing {
            self.stop_distributed_processor(rt, processor_full_id)
                .await?;
        }
        let domain = match repo.load(reference, processor_full_id).await? {
            crate::rt::triage::domain::ProcessorDomainLoad::Ready(snapshot) => snapshot,
            crate::rt::triage::domain::ProcessorDomainLoad::Pending {
                authority_pending: true,
                ..
            } => return Err(big_repo::CoordinationError::Pending.into()),
            crate::rt::triage::domain::ProcessorDomainLoad::Pending { .. } => return Ok(false),
            crate::rt::triage::domain::ProcessorDomainLoad::Rejected(error) => {
                return Err(error.into());
            }
        };
        let Some((_, shared_definition)) = rt
            .plugs_repo
            .read_manifest_at_ref(&domain.configuration.manifest)
            .await?
        else {
            return Ok(false);
        };
        eyre::ensure!(
            shared_definition.id() == plug
                && shared_definition
                    .processors
                    .get(processor)
                    .is_some_and(|definition| matches!(
                        definition.coordination,
                        daybook_types::manifest::ProcessorCoordination::Distributed(_)
                    )),
            "shared processor definition does not permit distributed execution"
        );
        eyre::ensure!(
            domain.pool == pool.reference,
            "processor domain names another task pool"
        );
        if repo
            .selected_candidate(&self.rcx.doc_config.document_id(), processor_full_id)
            .await?
            .as_ref()
            != Some(selected)
        {
            self.stop_distributed_processor(rt, processor_full_id)
                .await?;
            return Ok(false);
        }
        let mut processors = rt.distributed_processors.write().await;
        if rt.cancel_token.is_cancelled() || activation_cancel.is_cancelled() {
            return Ok(false);
        }
        if let Some(existing) = processors.get(processor_full_id) {
            eyre::ensure!(
                existing.domain == *reference
                    && existing.pool.reference == pool.reference
                    && existing.pool.descriptor == pool.descriptor,
                "processor attachment changed; explicit migration is required"
            );
            let changed =
                *existing.installed_domain_heads.lock().expect(ERROR_MUTEX) != domain.heads;
            if changed {
                self.attach_processor_domain(&domain).await?;
                *existing.installed_domain_heads.lock().expect(ERROR_MUTEX) = domain.heads;
            }
            // Current native authority was sampled above. An unchanged binding
            // must not replace its installed audience with an empty projection
            // and reopen the transport publication transaction on every wake.
            return Ok(true);
        }
        eyre::ensure!(
            processors
                .values()
                .all(|existing| existing.pool.descriptor.pool_id != pool.descriptor.pool_id),
            "a task pool is already bound to another processor adapter"
        );
        self.attach_processor_domain(&domain).await?;
        let adapter = Arc::new(
            crate::rt::task_adapter::CapturedRoutineAdapter::for_processor(
                Arc::clone(rt),
                pool.clone(),
                processor_full_id.to_owned(),
                reference.clone(),
                self.attach_task_pool(pool.clone()).await?,
            ),
        );
        let driver = self
            .attach_scheduling_pool(
                pool.clone(),
                adapter,
                capabilities.clone(),
                capacity,
                timing,
            )
            .await?;
        // Serialize attachment ownership through actor creation, so concurrent
        // management calls cannot bind one pool to different domain adapters.
        processors.insert(
            processor_full_id.to_owned(),
            Arc::new(crate::rt::triage::DistributedProcessor {
                plug_id: shared_definition.id(),
                domain: reference.clone(),
                installed_domain_heads: std::sync::Mutex::new(domain.heads),
                driver,
                pool: pool.clone(),
            }),
        );
        drop(processors);
        rt.triage_worker.wake_coordination();
        Ok(true)
    }

    /// Explicit recovery/rotation proposal; configuration arrival never creates one.
    /// The existing local participation owner adopts the deterministic selected candidate.
    pub async fn rotate_distributed_processor(
        self: &Arc<Self>,
        rt: &crate::rt::Rt,
        processor: &str,
    ) -> Res<crate::rt::triage::domain::ProcessorConfigCandidate> {
        let (reference, predecessor_pool) = {
            let active = rt.distributed_processors.read().await;
            let active = active
                .get(processor)
                .ok_or_eyre("processor is not explicitly attached")?;
            (active.domain.clone(), active.pool.clone())
        };
        let domains = crate::rt::triage::domain::ProcessorDomainRepo::new(
            Arc::clone(&self.rcx.big_repo),
            self.rcx.local_actor_id.clone(),
        );
        let predecessor = match domains.load(&reference, processor).await? {
            crate::rt::triage::domain::ProcessorDomainLoad::Ready(snapshot) => snapshot,
            crate::rt::triage::domain::ProcessorDomainLoad::Pending { .. } => {
                return Err(big_repo::CoordinationError::Pending.into());
            }
            crate::rt::triage::domain::ProcessorDomainLoad::Rejected(error) => {
                return Err(error.into());
            }
        };
        let pools = crate::tasks::pool::PoolRepo::new(
            Arc::clone(&self.rcx.big_repo),
            self.rcx.doc_config.document_id(),
            self.rcx.local_actor_id.clone(),
        );
        let successor = pools.propose_successor(&predecessor_pool).await?;
        let reference = crate::rt::triage::domain::ProcessorDomainReference {
            document: successor.reference.document.clone(),
            authority_group: successor.reference.authority_group,
        };
        domains
            .provision_successor(
                &predecessor.recovery_metadata(),
                &reference,
                successor.reference,
                successor.descriptor.publication_key,
            )
            .await?;
        domains
            .publish_candidate(
                &self.rcx.doc_config.document_id(),
                processor,
                crate::rt::triage::domain::ProcessorConfigCandidate {
                    generation: successor.descriptor.rotation_generation,
                    reference,
                },
            )
            .await
    }

    /// Await the actual scheduling owner after an explicit attachment returned pending.
    /// A different ready winner or explicit detach does not acknowledge this reference.
    pub async fn wait_for_processor_adoption(
        &self,
        processor: &str,
        reference: &crate::rt::triage::domain::ProcessorDomainReference,
    ) -> Res<bool> {
        let (runtime, cancel, mut ready) = {
            let adoptions = self.processor_adoptions.lock().await;
            let adoption = adoptions
                .get(processor)
                .ok_or_eyre("processor has no explicit adoption intent")?;
            (
                std::sync::Weak::clone(&adoption.runtime),
                adoption.cancel.clone(),
                adoption.ready.clone(),
            )
        };
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let acknowledged = ready.borrow_and_update().clone();
            if let Some(candidate) = acknowledged {
                let Some(runtime) = runtime.upgrade() else {
                    return Ok(false);
                };
                let domains = crate::rt::triage::domain::ProcessorDomainRepo::new(
                    Arc::clone(&self.rcx.big_repo),
                    self.rcx.local_actor_id.clone(),
                );
                let selected = domains
                    .selected_candidate(&self.rcx.doc_config.document_id(), processor)
                    .await?;
                let processors = runtime.distributed_processors.read().await;
                if selected.as_ref() == Some(&candidate)
                    && processors.get(processor).is_some_and(|owner| {
                        owner.domain == candidate.reference
                            && owner.pool.reference.rotation_generation == candidate.generation
                    })
                {
                    return Ok(&candidate.reference == reference);
                }
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(false),
                changed = ready.changed() => changed.map_err(|_| eyre::eyre!("processor adoption owner closed"))?,
            }
        }
    }

    /// Domain-owned disablement. Retained ciphertext and task obligations do not
    /// reattach the processor; a later enablement requires explicit attachment.
    pub async fn detach_distributed_processor(
        &self,
        rt: &crate::rt::Rt,
        processor_full_id: &str,
    ) -> Res<()> {
        self.rcx
            .clear_processor_activation(processor_full_id)
            .await?;
        let adoption = self
            .processor_adoptions
            .lock()
            .await
            .remove(processor_full_id);
        if let Some(adoption) = adoption {
            adoption.cancel.cancel();
            adoption.handle.await?;
        }
        self.stop_distributed_processor(rt, processor_full_id).await
    }

    async fn restore_processor_activations(self: &Arc<Self>, rt: &Arc<crate::rt::Rt>) -> Res<()> {
        // The primary-key range reads only local participation, never shared policy.
        let rows = sqlx::query_as::<_, (String, String)>(
            "SELECT key
                  , value
               FROM kvstore
              WHERE key >= 'local.processor-activation/'
                AND key < 'local.processor-activation0'
              ORDER BY key",
        )
        .fetch_all(&self.rcx.sql.read_pool)
        .await?;
        for (key, value) in rows {
            let processor = key
                .strip_prefix("local.processor-activation/")
                .expect(ERROR_IMPOSSIBLE);
            let intent: ProcessorActivation = serde_json::from_str(&value)?;
            let (plug, _) = processor
                .rsplit_once('/')
                .ok_or_eyre("processor identifier has no plug boundary")?;
            if rt.plugs_repo.enabled_ref(plug).await?.is_none() {
                self.rcx.clear_processor_activation(processor).await?;
                continue;
            }
            self.start_processor_adoption(rt, processor, intent).await?;
        }
        Ok(())
    }

    async fn start_processor_adoption(
        self: &Arc<Self>,
        rt: &Arc<crate::rt::Rt>,
        processor: &str,
        intent: ProcessorActivation,
    ) -> Res<CancellationToken> {
        let mut adoptions = self.processor_adoptions.lock().await;
        let mut tokens = self.rcx.processor_activation_tokens.lock().await;
        if !self
            .rcx
            .processor_activation(processor)
            .await?
            .is_some_and(|current| current.epoch == intent.epoch)
        {
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            return Ok(cancelled);
        }
        let ProcessorActivation {
            epoch,
            pool: pool_id,
            capabilities,
            capacity,
            timing,
        } = intent;
        if let Some(adoption) = adoptions.get(processor)
            && adoption.epoch == epoch
            && !adoption.cancel.is_cancelled()
        {
            return Ok(adoption.cancel.clone());
        }
        if let Some(previous) = adoptions.remove(processor) {
            previous.cancel.cancel();
            previous.handle.await?;
        }
        // Subscribe before the first selection read, including unavailable target arrival.
        let (local_ticket, mut local) = self
            .rcx
            .big_repo
            .subscribe_local_listener(big_repo::BigRepoLocalFilter { doc_id: None })
            .await?;
        let (domain_ticket, mut authority) = self
            .rcx
            .big_repo
            .subscribe_domain_listener(big_repo::BigRepoDomainFilter)
            .await?;
        let mut activation = rt.triage_worker.activation_updates();
        let cancel = self.cancel_token.child_token();
        tokens.insert(processor.to_owned(), cancel.clone());
        let stopped = cancel.clone();
        let owner = Arc::downgrade(self);
        let runtime = Arc::downgrade(rt);
        let runtime_stopped = rt.cancel_token.clone();
        let processor_id = processor.to_owned();
        let (ready, readiness) = tokio::sync::watch::channel(None);
        let handle = tokio::spawn(async move {
            let _tickets = (local_ticket, domain_ticket);
            let work = async {
                loop {
                    let Some(owner) = owner.upgrade() else {
                        break;
                    };
                    let Some(runtime) = runtime.upgrade() else {
                        break;
                    };
                    let (plug, _) = processor_id.rsplit_once('/').expect(ERROR_IMPOSSIBLE);
                    if runtime.plugs_repo.enabled_ref(plug).await?.is_none() {
                        owner.rcx.clear_processor_activation(&processor_id).await?;
                        break;
                    }
                    let domains = crate::rt::triage::domain::ProcessorDomainRepo::new(
                        Arc::clone(&owner.rcx.big_repo),
                        owner.rcx.local_actor_id.clone(),
                    );
                    let config = owner.rcx.doc_config.document_id();
                    // A wake invalidates the earlier ownership receipt before resampling.
                    // Even re-adopting the same reference must wake a waiter that saw
                    // its actor disappear between the receipt and the owner query.
                    ready.send_if_modified(|current| current.take().is_some());
                    let selected = domains.selected_candidate(&config, &processor_id).await?;
                    let mut pending_authority = false;
                    let mut adopted = None;
                    let mut shared_documents = HashSet::new();
                    let candidates = domains
                        .configuration_candidates(&config, &processor_id)
                        .await?;
                    if let Some(selected) = &selected {
                        let changed = runtime
                            .distributed_processors
                            .read()
                            .await
                            .get(&processor_id)
                            .is_some_and(|active| {
                                active.domain != selected.reference
                                    || active.pool.reference.rotation_generation
                                        != selected.generation
                            });
                        if changed {
                            owner
                                .stop_distributed_processor(&runtime, &processor_id)
                                .await?;
                        }
                        match domains.load(&selected.reference, &processor_id).await? {
                            crate::rt::triage::domain::ProcessorDomainLoad::Pending {
                                authority_pending,
                                ..
                            } => {
                                pending_authority |= authority_pending;
                            }
                            crate::rt::triage::domain::ProcessorDomainLoad::Rejected(
                                crate::rt::triage::domain::ProcessorDomainRejection::Unauthorized,
                            ) => {
                                owner
                                    .stop_distributed_processor(&runtime, &processor_id)
                                    .await?;
                                stopped.cancel();
                                break;
                            }
                            crate::rt::triage::domain::ProcessorDomainLoad::Rejected(error) => {
                                return Err(error.into());
                            }
                            crate::rt::triage::domain::ProcessorDomainLoad::Ready(domain) => {
                                for document in domain.configuration.config_documents.values() {
                                    shared_documents.insert(document.parse::<DocumentId>()?);
                                }
                                shared_documents.insert(
                                    crate::plugs::PlugsRepo::parse_enabled_ref(
                                        &domain.configuration.manifest,
                                    )?
                                    .doc_id
                                    .parse::<DocumentId>()?,
                                );
                                let pools = crate::tasks::pool::PoolRepo::new(
                                    Arc::clone(&owner.rcx.big_repo),
                                    config.clone(),
                                    owner.rcx.local_actor_id.clone(),
                                );
                                match pools
                                    .load_descriptor(
                                        &domain.pool,
                                        &pool_id,
                                        domain.pool.authority_group,
                                    )
                                    .await?
                                {
                                    crate::tasks::pool::PoolDiscovery::Ready(pool) => {
                                        match owner
                                            .apply_distributed_processor(
                                                &runtime,
                                                &processor_id,
                                                &selected.reference,
                                                &pool,
                                                &capabilities,
                                                capacity,
                                                timing,
                                                selected,
                                                epoch,
                                                &stopped,
                                            )
                                            .await
                                        {
                                            Ok(true) => {
                                                adopted = Some(selected);
                                            }
                                            Ok(false) => {}
                                            Err(error) => match error
                                                .downcast_ref::<big_repo::CoordinationError>()
                                            {
                                                Some(big_repo::CoordinationError::Pending) => {
                                                    // Native generation races do not always emit a complete
                                                    // authority edge; retain the native reader's idle recheck.
                                                    pending_authority = true;
                                                }
                                                Some(big_repo::CoordinationError::Unauthorized) => {
                                                    owner
                                                        .stop_distributed_processor(
                                                            &runtime,
                                                            &processor_id,
                                                        )
                                                        .await?;
                                                    stopped.cancel();
                                                    break;
                                                }
                                                _ => return Err(error),
                                            },
                                        }
                                    }
                                    crate::tasks::pool::PoolDiscovery::Pending {
                                        authority_pending,
                                        ..
                                    } => {
                                        pending_authority |= authority_pending;
                                    }
                                    crate::tasks::pool::PoolDiscovery::Rejected(error) => {
                                        return Err(error.into());
                                    }
                                    _ => eyre::bail!(
                                        "selected processor configuration has no unique pool descriptor"
                                    ),
                                }
                            }
                        }
                    }
                    let active = runtime
                        .distributed_processors
                        .read()
                        .await
                        .get(&processor_id)
                        .map(|active| {
                            (
                                active.domain.clone(),
                                active.pool.reference.rotation_generation,
                            )
                        });
                    if let Some((active_reference, active_generation)) = active {
                        let pools = crate::tasks::pool::PoolRepo::new(
                            Arc::clone(&owner.rcx.big_repo),
                            config.clone(),
                            owner.rcx.local_actor_id.clone(),
                        );
                        for candidate in &candidates {
                            if candidate.reference == active_reference
                                || candidate.generation > active_generation
                            {
                                continue;
                            }
                            let source = match domains.load(&candidate.reference, &processor_id).await? {
                                crate::rt::triage::domain::ProcessorDomainLoad::Ready(source) => source,
                                crate::rt::triage::domain::ProcessorDomainLoad::Pending { authority_pending, .. } => {
                                    pending_authority |= authority_pending;
                                    continue;
                                }
                                crate::rt::triage::domain::ProcessorDomainLoad::Rejected(
                                    crate::rt::triage::domain::ProcessorDomainRejection::Unauthorized,
                                ) => continue,
                                crate::rt::triage::domain::ProcessorDomainLoad::Rejected(error) => return Err(error.into()),
                            };
                            eyre::ensure!(
                                source.pool.rotation_generation == candidate.generation,
                                "source candidate generation disagrees with native descriptor"
                            );
                            let source_pool = match pools
                                .load_descriptor(
                                    &source.pool,
                                    &pool_id,
                                    source.pool.authority_group,
                                )
                                .await?
                            {
                                crate::tasks::pool::PoolDiscovery::Ready(pool) => pool,
                                crate::tasks::pool::PoolDiscovery::Pending {
                                    authority_pending,
                                    ..
                                } => {
                                    pending_authority |= authority_pending;
                                    continue;
                                }
                                crate::tasks::pool::PoolDiscovery::Rejected(error) => {
                                    return Err(error.into());
                                }
                                _ => eyre::bail!(
                                    "source processor configuration has no unique pool descriptor"
                                ),
                            };
                            let admit = async {
                                owner.attach_task_pool(source_pool).await?;
                                owner
                                    .rcx
                                    .attach_processor_slot_store(
                                        &source.transport(&owner.rcx.big_repo).await?,
                                        None,
                                    )
                                    .await?;
                                eyre::Ok(())
                            }
                            .await;
                            if let Err(error) = admit {
                                match error.downcast_ref::<big_repo::CoordinationError>() {
                                    Some(big_repo::CoordinationError::Pending) => {
                                        pending_authority = true
                                    }
                                    Some(big_repo::CoordinationError::Unauthorized) => {}
                                    _ => return Err(error),
                                }
                            }
                        }
                    }
                    ready.send_if_modified(|current| {
                        if current.as_ref() == adopted {
                            false
                        } else {
                            *current = adopted.cloned();
                            true
                        }
                    });
                    drop(runtime);
                    drop(owner);
                    loop {
                        let relevant = tokio::select! {
                            biased;
                            _ = stopped.cancelled() => return eyre::Ok(()),
                            changed = activation.changed() => {
                                changed.map_err(|_| eyre::eyre!("processor activation listener closed"))?;
                                true
                            }
                            _ = tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY), if pending_authority => true,
                            changes = local.recv() => changes.ok_or_eyre("processor materialization listener closed")?
                                .iter().any(|change| {
                                    let document = match change {
                                        big_repo::BigRepoLocalNotification::DocCreated { doc_id, .. }
                                        | big_repo::BigRepoLocalNotification::DocImported { doc_id, .. }
                                        | big_repo::BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. }
                                        | big_repo::BigRepoLocalNotification::DocMaterializationPending { doc_id }
                                        | big_repo::BigRepoLocalNotification::DocMaterializationReady { doc_id, .. } => doc_id,
                                    };
                                    document == &config || shared_documents.contains(document)
                                        || candidates.iter().any(|candidate| &candidate.reference.document == document)
                                }),
                            changes = authority.recv() => {
                                changes.ok_or_eyre("processor authority listener closed")?;
                                true
                            }
                        };
                        if relevant {
                            break;
                        }
                    }
                }
                eyre::Ok(())
            };
            tokio::select! {
                biased;
                _ = stopped.cancelled() => {},
                _ = runtime_stopped.cancelled() => {},
                result = work => result.unwrap(),
            }
            stopped.cancel();
        });
        adoptions.insert(
            processor.to_owned(),
            ProcessorAdoption {
                epoch,
                runtime: Arc::downgrade(rt),
                cancel: cancel.clone(),
                handle,
                ready: readiness,
            },
        );
        Ok(cancel)
    }

    async fn stop_distributed_processor(
        &self,
        rt: &crate::rt::Rt,
        processor_full_id: &str,
    ) -> Res<()> {
        let mut processors = rt.distributed_processors.write().await;
        if let Some(processor) = processors.remove(processor_full_id) {
            processor.driver.stop().await?;
        }
        drop(processors);
        rt.triage_worker.wake_coordination();
        Ok(())
    }

    /// Start one real scheduling actor on an explicitly provisioned pool.
    /// Relay/Read storage attachment alone never grants scheduling authority.
    pub async fn attach_scheduling_pool(
        &self,
        snapshot: crate::tasks::PoolDescriptorSnapshot,
        adapter: Arc<dyn crate::tasks::driver::TaskExecutionAdapter>,
        capabilities: crate::tasks::CapabilitySummary,
        capacity: crate::tasks::Capacity,
        timing: crate::tasks::driver::SchedulingTiming,
    ) -> Res<crate::tasks::driver::PoolDriverHandle> {
        self.attach_task_pool(snapshot.clone()).await?;
        self.task_driver
            .attach(snapshot, adapter, capabilities, capacity, timing)
            .await
    }

    pub fn task_store(
        &self,
        pool: &crate::tasks::TaskPoolId,
    ) -> Option<Arc<crate::tasks::store::TaskStore>> {
        self.task_transport.get(pool)
    }

    // Caller holds task_route_lock: attachment, authority updates and connection
    // establishment cannot overwrite one another with an older route snapshot.
    async fn refresh_task_peer_routes(&self) -> Res<()> {
        let clients = self
            .task_peer_clients
            .lock()
            .expect(ERROR_MUTEX)
            .iter()
            .map(|(peer, route)| (peer.clone(), Arc::clone(&route.client)))
            .collect::<Vec<_>>();
        for (peer, client) in clients {
            let parts = self.task_transport.routes(&peer);
            let (documents, blobs) =
                Self::split_partitions(self.peer_partition_ids(&peer, true).await?);
            let routes = [parts, documents, blobs];
            let changed = self
                .installed_peer_routes
                .lock()
                .expect(ERROR_MUTEX)
                .get(&peer)
                .map(|installed| {
                    std::array::from_fn::<_, 3, _>(|index| installed[index] != routes[index])
                })
                .unwrap_or([true; 3]);
            // SetPeer restarts pending strategy requests. Native authority wakes
            // must not cancel a request merely because its route is unchanged.
            for (index, worker) in [
                &self.task_sync_worker,
                &self.big_sync_worker,
                &self.blob_sync_worker,
            ]
            .into_iter()
            .enumerate()
            {
                if changed[index] {
                    worker
                        .set_peer(
                            peer.clone(),
                            Arc::clone(&client),
                            routes[index].clone(),
                            HashMap::new(),
                        )
                        .await?;
                }
            }
            self.installed_peer_routes
                .lock()
                .expect(ERROR_MUTEX)
                .insert(peer, routes);
        }
        Ok(())
    }

    fn spawn_task_control_exchange(self: &Arc<Self>) -> crate::repos::RepoStopToken {
        use crate::tasks::rpc::{
            ControlRejection, CurrentPoolControlRequest, CurrentProcessorControlRequest,
            TASK_COORDINATION_ALPN, TaskCoordinationRpc,
        };
        let mut wake = self.task_control_wake.subscribe();
        let weak = Arc::downgrade(self);
        let cancel_token = CancellationToken::new();
        let stopped = cancel_token.clone();
        let task = async move {
            let run = async {
                loop {
                    wake.borrow_and_update();
                    let Some(repo) = weak.upgrade() else {
                        return eyre::Ok(());
                    };
                    let peers = repo
                        .task_peer_clients
                        .lock()
                        .expect(ERROR_MUTEX)
                        .iter()
                        .map(|(peer, route)| (peer.clone(), route.address.clone()))
                        .collect::<Vec<_>>();
                    let pools = repo.task_transport.sponsored_pools();
                    let processors = repo.task_transport.sponsored_processors();
                    let mut pending = false;
                    for (peer, address) in peers {
                        if repo.big_repo_rpc.peer_for_endpoint(address.id) != Some(peer.clone()) {
                            continue;
                        }
                        let client = irpc_iroh::client::<TaskCoordinationRpc>(
                            repo.router.endpoint().clone(),
                            address.clone(),
                            TASK_COORDINATION_ALPN,
                        );
                        for pool in &pools {
                            let offered =
                                match repo.task_transport.current_control(pool, &peer).await {
                                    Ok(control) => Some(control),
                                    Err(big_repo::CoordinationError::Pending) => {
                                        pending = true;
                                        continue;
                                    }
                                    // A provisioned Relay may have no certificate
                                    // yet. It can still request one without signing.
                                    Err(big_repo::CoordinationError::Unauthorized) => None,
                                    Err(error) => return Err(error.into()),
                                };
                            let response = match client
                                .rpc(CurrentPoolControlRequest {
                                    pool: pool.clone(),
                                    offered,
                                })
                                .await
                            {
                                Ok(response) => response,
                                // Reconnection wakes exchange after network failure;
                                // a transport error does not create a retry timer.
                                Err(error) => {
                                    tracing::warn!(
                                        ?peer,
                                        ?pool,
                                        ?error,
                                        "pool control exchange transport failed"
                                    );
                                    continue;
                                }
                            };
                            let control = match response {
                                Ok(control) => control,
                                Err(ControlRejection::Pending) => {
                                    pending = true;
                                    continue;
                                }
                                Err(ControlRejection::Unauthorized) => continue,
                                // An advertisement never creates an unsolicited pool.
                                Err(ControlRejection::Invalid(reason)) => {
                                    tracing::debug!(?peer, ?pool, %reason, "pool control exchange declined");
                                    continue;
                                }
                            };
                            if repo.big_repo_rpc.peer_for_endpoint(address.id) != Some(peer.clone())
                            {
                                continue;
                            }
                            match repo
                                .task_transport
                                .accept_current_control(pool, &peer, &control)
                                .await
                            {
                                Ok(_) => {}
                                Err(big_repo::CoordinationError::Pending) => pending = true,
                                Err(big_repo::CoordinationError::Unauthorized) => {}
                                Err(big_repo::CoordinationError::Invalid(reason)) => {
                                    tracing::warn!(?peer, ?pool, %reason, "invalid offered pool control")
                                }
                                Err(error) => return Err(error.into()),
                            }
                        }
                        for processor in &processors {
                            let offered = match repo
                                .task_transport
                                .current_processor_control(processor, &peer)
                                .await
                            {
                                Ok(control) => Some(control),
                                Err(big_repo::CoordinationError::Pending) => {
                                    pending = true;
                                    continue;
                                }
                                Err(big_repo::CoordinationError::Unauthorized) => None,
                                Err(error) => return Err(error.into()),
                            };
                            let response = match client
                                .rpc(CurrentProcessorControlRequest {
                                    processor: processor.clone(),
                                    offered,
                                })
                                .await
                            {
                                Ok(response) => response,
                                Err(error) => {
                                    tracing::warn!(?peer, %processor, ?error, "processor control exchange transport failed");
                                    continue;
                                }
                            };
                            let control = match response {
                                Ok(control) => control,
                                Err(ControlRejection::Pending) => {
                                    pending = true;
                                    continue;
                                }
                                Err(ControlRejection::Unauthorized) => continue,
                                Err(ControlRejection::Invalid(reason)) => {
                                    tracing::debug!(?peer, %processor, %reason, "processor control exchange declined");
                                    continue;
                                }
                            };
                            if repo.big_repo_rpc.peer_for_endpoint(address.id) != Some(peer.clone())
                            {
                                continue;
                            }
                            match repo
                                .task_transport
                                .accept_processor_control(processor, &peer, &control)
                                .await
                            {
                                Ok(_) => {}
                                Err(big_repo::CoordinationError::Pending) => pending = true,
                                Err(big_repo::CoordinationError::Unauthorized) => {}
                                Err(big_repo::CoordinationError::Invalid(reason)) => {
                                    tracing::warn!(?peer, %processor, %reason, "invalid offered processor control")
                                }
                                Err(error) => return Err(error.into()),
                            }
                        }
                    }
                    drop(repo);
                    tokio::select! {
                        // The root owns the only sender. Its drop ends this
                        // child even when facade release precedes explicit stop.
                        changed = wake.changed() => if changed.is_err() { return eyre::Ok(()); },
                        _ = tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY), if pending => {},
                    }
                }
            };
            tokio::select! {
                _ = stopped.cancelled() => {},
                result = run => result.unwrap(),
            }
        };
        let worker_handle =
            tokio::spawn(task.instrument(tracing::info_span!("task current control exchange")));
        crate::repos::RepoStopToken {
            cancel_token,
            worker_handle: Some(worker_handle),
        }
    }

    async fn spawn_task_permission_writer(self: &Arc<Self>) -> Res<crate::repos::RepoStopToken> {
        use big_sync::DeltaWalkerStateRepo as _;
        use big_sync_core::concurrent_delta_walker::{ConcurrentDeltaRead, ConcurrentDeltaWalker};
        use big_sync_core::delta_walker_state::DeltaWalkerStateTransaction as _;
        use big_sync_core::revisioned_store::RevisionedStore as _;
        // Subscribe before any sampling so native completion hints cannot fall
        // between a Pending result and its wait. Hints never supply grants.
        let (domain_ticket, mut domain_rx) = self
            .rcx
            .big_repo
            .subscribe_domain_listener(big_repo::BigRepoDomainFilter)
            .await?;
        let (local_ticket, mut local_rx) = self
            .rcx
            .big_repo
            .subscribe_local_listener(big_repo::BigRepoLocalFilter { doc_id: None })
            .await?;
        let sql = self.rcx.big_repo.sql_ctx();
        let state = big_sync::SqliteDeltaWalkerStateRepo::new(
            sql.read_pool.clone(),
            sql.write_pool.clone(),
            "@daybook/core/task-pool-permissions",
            "keyhive-access",
        )
        .await?;
        let stream = big_repo::KeyhiveAccessRevisionStore::new(&self.rcx.big_repo);
        let durable = state.progress().await?.upstream_revision;
        let resume = durable.max(stream.archived_through().await?);
        if resume > durable {
            // Only explicitly attached pools are served. Boot cleared persisted
            // task permission rows, and attachment seeds current native authority.
            let mut tx = state.begin().await?;
            tx.advance_from(durable, resume).await?;
            tx.commit().await?;
        }
        stream.note_retention(&state).await?;
        let reader = stream
            .open(
                big_repo::KeyhiveAccessSelector {
                    watch: big_repo::AccessSubjectSet::All,
                    memory: state.clone(),
                },
                resume,
            )
            .await?;
        let mut walker: ConcurrentDeltaWalker<
            '_,
            big_repo::KeyhiveAccessRevisionStore<big_sync::SqliteDeltaWalkerStateRepo>,
            _,
            _,
        > = ConcurrentDeltaWalker::open(reader, state, |entry: &big_repo::KeyhiveAccessDelta| {
            entry.subject
        })
        .await?;
        let weak = Arc::downgrade(self);
        let cancel_token = CancellationToken::new();
        let worker_cancel = cancel_token.clone();
        let worker_handle = tokio::spawn(async move {
            let run = async {
                let _registrations = (domain_ticket, local_ticket);
                let mut pending_entries = Vec::new();
                loop {
                    let Some(repo) = weak.upgrade() else {
                        return eyre::Ok(());
                    };
                    let pending = repo.task_transport.refresh_pending();
                    let mut refresh_all = false;
                    tokio::select! {
                        batch = walker.next(std::num::NonZeroUsize::new(32).expect(ERROR_IMPOSSIBLE)),
                            if pending_entries.is_empty() => {
                            match batch? {
                                ConcurrentDeltaRead::ReplayComplete { .. } => continue,
                                ConcurrentDeltaRead::Entries { entries, .. } => {
                                    pending_entries.extend(entries);
                                    refresh_all = true;
                                }
                            }
                        }
                        _ = repo.task_transport.refresh_notified() => {}
                        notification = domain_rx.recv() => {
                            notification.expect(ERROR_CHANNEL);
                            if !pending { continue; }
                        }
                        notification = local_rx.recv() => {
                            notification.expect(ERROR_CHANNEL);
                            if !pending { continue; }
                        }
                        // Native access reads themselves use this idle bound
                        // while proof application is Pending: there is no
                        // complete notification for every generation race.
                        _ = tokio::time::sleep(big_repo::CoordinationAuthority::PENDING_RECHECK_DELAY),
                            if pending => {}
                    }
                    let _routes = repo.task_route_lock.lock().await;
                    repo.task_transport.refresh_authority(!refresh_all).await?;
                    // Also repairs cancellation after an attachment committed
                    // rows but before its caller installed native peer routes.
                    if !repo.cancel_token.is_cancelled() {
                        repo.refresh_task_peer_routes().await?;
                        repo.task_control_wake.send_replace(());
                    }
                    // Fail-closed denial during Pending is not a completed
                    // projection. Retain the exact batch until fresh sampling
                    // succeeds; cancellation leaves its durable cursor unacked.
                    if !repo.task_transport.refresh_pending() {
                        for delta in pending_entries.drain(..) {
                            walker.ack(delta.key, delta.cursor).await?;
                        }
                        #[cfg(test)]
                        repo.task_permission_progress
                            .send_replace(walker.durable_revision());
                    }
                }
            };
            // Cancellation drops an uncommitted row/cursor update or a route-lock
            // wait. Already committed permissions are idempotent on durable replay.
            tokio::select! {
                _ = worker_cancel.cancelled() => {}
                result = run => result.unwrap(),
            }
        });
        Ok(crate::repos::RepoStopToken {
            cancel_token,
            worker_handle: Some(worker_handle),
        })
    }

    #[cfg(test)]
    pub(crate) fn task_permission_progress(&self) -> tokio::sync::watch::Receiver<u64> {
        self.task_permission_progress.subscribe()
    }

    #[cfg(test)]
    pub(crate) fn task_backend(&self) -> Arc<crate::tasks::transport::TaskSyncBackend> {
        Arc::clone(&self.task_transport)
    }

    #[cfg(test)]
    pub(crate) fn native_rpc_client(
        &self,
        peer: iroh::EndpointAddr,
    ) -> big_sync::rpc::BigSyncRpcClient {
        big_sync::rpc::BigSyncRpcClient::over_iroh(self.router.endpoint().clone(), peer)
    }

    #[cfg(test)]
    pub(crate) fn task_sync_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<big_sync_core::SyncStatEvent> {
        self.task_sync_worker.subscribe_stats()
    }

    #[inline]
    pub fn is_blob_part(&self, part_id: &PartKey) -> bool {
        let core_blob = crate::blobs::blob_inventory_part_id(&self.rcx.core_inventory_doc_id);
        let docs_blob = crate::blobs::blob_inventory_part_id(&self.rcx.docs_inventory_doc_id);
        part_id == &core_blob || part_id == &docs_blob
    }

    async fn peer_partition_ids(
        &self,
        peer: &PeerKey,
        include_blob_parts: bool,
    ) -> Res<HashMap<PartKey, BackendId>> {
        use big_repo::keyhive_core::principal::identifier::Identifier;
        let principal = Identifier::from(ed25519_dalek::VerifyingKey::from_bytes(
            &peer.to_bytes32()?,
        )?);
        let repo_backend_id: BackendId = big_repo::BigRepo::BACKEND_ID.into();
        let mut parts = HashMap::new();
        // Independent collaborators share individual documents, not these
        // private inventories. Route hints follow native membership; the RPC
        // responder still performs the authoritative access check on every read.
        for (group, part) in [
            (
                &self.authority.core_docs,
                self.authority.core_docs_part_id(),
            ),
            (
                &self.authority.content_docs,
                self.authority.content_docs_part_id(),
            ),
            (
                &self.authority.default_drawer,
                self.authority.default_drawer_part_id(),
            ),
            (
                &self.authority.blob_inventories,
                self.authority.blob_inventories_part_id(),
            ),
        ] {
            if self
                .rcx
                .big_repo
                .keyhive()
                .agent_access_on(&principal, Identifier::from(group.id()))
                .await
                .is_some()
            {
                parts.insert(part, Arc::clone(&repo_backend_id));
            }
        }
        if include_blob_parts {
            let blob_backend_id: BackendId = BLOBS_BACKEND_ID.into();
            for document in [
                &self.rcx.core_inventory_doc_id,
                &self.rcx.docs_inventory_doc_id,
            ] {
                let subject = Identifier::from(ed25519_dalek::VerifyingKey::from_bytes(
                    &document.to_bytes32()?,
                )?);
                if self
                    .rcx
                    .big_repo
                    .keyhive()
                    .agent_access_on(&principal, subject)
                    .await
                    .is_some()
                {
                    parts.insert(
                        crate::blobs::blob_inventory_part_id(document),
                        Arc::clone(&blob_backend_id),
                    );
                }
            }
        }
        Ok(parts)
    }

    fn split_partitions(
        parts: HashMap<PartKey, BackendId>,
    ) -> (HashMap<PartKey, BackendId>, HashMap<PartKey, BackendId>) {
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

    #[tracing::instrument(level = "debug", skip_all, fields(worker = "sync-reconnect"))]
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
        // The sweep is a sibling of whatever asked for it (the machine loop's
        // periodic tick, a connection teardown), not a child of it: its span
        // stays a root and is linked causally instead of nested.
        let reconnect_span = tracing::debug_span!(
            parent: None,
            "sync_reconnect",
            worker = "sync-reconnect",
        );
        reconnect_span.follows_from(tracing::Span::current());
        let reconnect_fut = async move {
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
        };
        let handle = tokio::spawn(reconnect_fut.instrument(reconnect_span));
        *reconnect_task = Some(handle);
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug), fields(worker = "iroh-sync"))]
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
            .cloned()
            .collect::<Vec<_>>();
        for peer_id in active_peers {
            self.teardown_peer_registration(peer_id).await;
        }
        self.active_peers.write().await.clear();
        self.peer_registration_changed.notify_waiters();
        eyre::Ok(())
    }
    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(peer_id = %conn.peer_id, otel.kind = "server"),
    )]
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
            let replaced = match active_peers.get(&conn.peer_id) {
                Some(ActivePeerState::Connected { .. }) => true,
                Some(ActivePeerState::Connecting { .. }) => {
                    warn!(
                        peer_id = %conn.peer_id,
                        "ignoring duplicate incoming connection during setup"
                    );
                    return Ok(());
                }
                None => false,
            };
            // Keep the replacement reserved while old worker registrations drain.
            active_peers.insert(
                conn.peer_id.clone(),
                ActivePeerState::Connecting {
                    closed: Some(conn.closed_flag()),
                },
            );
            replaced
        };
        if replaced {
            warn!(
                peer_id = %conn.peer_id,
                "new connection replacing existing registration (re-establishment)"
            );
            self.teardown_peer_registration(conn.peer_id.clone()).await;
        }
        let peer_id = conn.peer_id.clone();
        let res = async {
            let peer_key = daybook_types::doc::format_peer_key(&conn.peer_id.to_bytes32()?);
            let events = [IrohSyncEvent::IncomingConnection {
                peer_key: Arc::clone(&peer_key),
            }];
            let endpoint = self.router.endpoint().clone();
            let remote_info = endpoint
                .remote_info(
                    EndpointId::from_bytes(&conn.peer_id.to_bytes32()?).expect(ERROR_IMPOSSIBLE),
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
                .register_peer_addr(peer_id.clone(), addr.clone());
            self.big_repo_rpc
                .register_peer(remote_endpoint_id, peer_id.clone());
            let doc_rpc_client =
                big_sync::rpc::BigSyncRpcClient::over_iroh(endpoint.clone(), addr.clone());
            let blob_rpc_client =
                big_sync::rpc::BigSyncRpcClient::over_iroh(endpoint, addr.clone());
            let doc_rpc_client = Arc::new(doc_rpc_client);
            let blob_rpc_client = Arc::new(blob_rpc_client);

            let _routes = self.task_route_lock.lock().await;
            let partition_ids = self.peer_partition_ids(&peer_id, true).await?;
            let (doc_parts, blob_parts) = Self::split_partitions(partition_ids);
            let task_client: Arc<dyn big_sync::rpc::WireBigSyncRpcClient> =
                Arc::clone(&doc_rpc_client) as _;
            self.task_peer_clients.lock().expect(ERROR_MUTEX).insert(
                peer_id.clone(),
                TaskPeerRoute {
                    client: Arc::clone(&task_client),
                    address: addr.clone(),
                },
            );
            self.installed_peer_routes
                .lock()
                .expect(ERROR_MUTEX)
                .remove(&peer_id);
            self.task_sync_worker
                .set_peer(
                    peer_id.clone(),
                    task_client,
                    self.task_transport.routes(&peer_id),
                    HashMap::new(),
                )
                .await?;
            self.big_sync_worker
                .set_peer(peer_id.clone(), doc_rpc_client, doc_parts, HashMap::new())
                .await?;
            if !blob_parts.is_empty() {
                self.blob_sync_worker
                    .set_peer(peer_id.clone(), blob_rpc_client, blob_parts, HashMap::new())
                    .await?;
            }

            let old = self.active_peers.write().await.insert(
                peer_id.clone(),
                ActivePeerState::Connected {
                    peer_key,
                    closed: conn.closed_flag(),
                },
            );
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );
            self.peer_registration_changed.notify_waiters();
            self.task_control_wake.send_replace(());

            self.registry.notify(events);
            eyre::Ok(())
        }
        .await;
        if let Err(error) = &res {
            error!(%peer_id, ?error, "incoming BigRepo connection setup failed");
            self.teardown_peer_registration(peer_id.clone()).await;
            let old = self.active_peers.write().await.remove(&peer_id);
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );
            self.peer_registration_changed.notify_waiters();
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
        self.teardown_peer_registration(signal.peer_id.clone())
            .await;
        let removed = self.active_peers.write().await.remove(&signal.peer_id);
        self.peer_registration_changed.notify_waiters();
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
    async fn teardown_peer_registration(&self, peer_id: PeerKey) {
        let _routes = self.task_route_lock.lock().await;
        self.task_peer_clients
            .lock()
            .expect(ERROR_MUTEX)
            .remove(&peer_id);
        self.installed_peer_routes
            .lock()
            .expect(ERROR_MUTEX)
            .remove(&peer_id);
        self.big_repo_rpc.unregister_peer(peer_id.clone());
        self.blobs_sync_backend
            .unregister_peer_addr(peer_id.clone());
        self.big_sync_worker.remove_peer(peer_id.clone()).await.ok();
        if !self.cancel_token.is_cancelled() {
            self.task_sync_worker
                .remove_peer(peer_id.clone())
                .await
                .expect(ERROR_CHANNEL);
        }
        self.blob_sync_worker.remove_peer(peer_id).await.ok();
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(peer_id = %req.requester_endpoint_id, otel.kind = "server"),
    )]
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
            config_doc_id: Some(self.rcx.doc_config.document_id().to_string()),
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

    async fn reserve_endpoint_connection(&self, peer_id: PeerKey) -> Res<bool> {
        loop {
            self.ensure_repo_live()?;
            // Register before inspecting state so setup completion cannot be lost.
            // A reservation is not a usable connection; callers acknowledge only
            // the fully installed worker/RPC registration or reserve their own dial.
            let changed = self.peer_registration_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut active_peers = self.active_peers.write().await;
                match active_peers.get(&peer_id) {
                    Some(ActivePeerState::Connected { closed, .. })
                        if !closed.load(std::sync::atomic::Ordering::Acquire) =>
                    {
                        return Ok(false);
                    }
                    Some(_) => {}
                    None => {
                        active_peers.insert(peer_id, ActivePeerState::Connecting { closed: None });
                        return Ok(true);
                    }
                }
            }
            tokio::select! {
                _ = self.cancel_token.cancelled() => eyre::bail!("repository shut down during connection setup"),
                _ = &mut changed => {}
            }
        }
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
                    peer_key: daybook_types::doc::format_peer_key(&peer_id.to_bytes32()?),
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
                    peer_key: daybook_types::doc::format_peer_key(&peer_id.to_bytes32()?),
                    partition: part_id.to_string(),
                }]);
            }
            big_sync_core::SyncStatEvent::PeerPartUnanswered { peer_id, part_id } => {
                // Report-only: the part keeps full sync blocked until the
                // embedder drops it from the subscription set, and the machine
                // re-asks on its own timer, so there is no work to do here
                // beyond recording the fact.
                info!(
                    local_peer_id = %self.router.endpoint().id(),
                    %peer_id,
                    %part_id,
                    "BigSync peer partition unanswered"
                );
            }
            big_sync_core::SyncStatEvent::PeerPartStale { .. } => {}
            big_sync_core::SyncStatEvent::PartFullySynced { .. } => {}
            big_sync_core::SyncStatEvent::PartStale { .. } => {}
            big_sync_core::SyncStatEvent::PeerFullySynced { .. } => {}
            big_sync_core::SyncStatEvent::PeerStale { peer_id } => {
                self.registry.notify([IrohSyncEvent::StalePeer {
                    peer_key: daybook_types::doc::format_peer_key(&peer_id.to_bytes32()?),
                }]);
            }
            big_sync_core::SyncStatEvent::FullSyncWaiterSatisfied { .. } => {}
        }
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        err(Debug),
        fields(peer_id = %endpoint_addr.id, otel.kind = "client"),
    )]
    pub async fn connect_endpoint_addr(&self, endpoint_addr: iroh::EndpointAddr) -> Res<()> {
        self.ensure_repo_live()?;

        if endpoint_addr.id == self.router.endpoint().id() {
            eyre::bail!("connecting to ourself is not supported");
        }
        let endpoint_id = endpoint_addr.id;
        let peer_id = PeerKey::new(*endpoint_id.as_bytes());

        let endpoint = self.router.endpoint().clone();

        self.address_lookup.add_endpoint_info(endpoint_addr.clone());
        if !self.reserve_endpoint_connection(peer_id.clone()).await? {
            return Ok(());
        }
        let res = async {
            let peer_key = daybook_types::doc::format_peer_key(endpoint_id.as_bytes());
            let events = [IrohSyncEvent::OutgoingConnection {
                peer_key: Arc::clone(&peer_key),
            }];

            let conn = self
                .rcx
                .big_repo
                .open_connection_iroh(
                    self.router.endpoint().clone(),
                    endpoint_addr.clone(),
                    peer_id.clone(),
                    Some(self.conn_end_signal_tx.clone()),
                )
                .await?;
            let doc_rpc_client =
                big_sync::rpc::BigSyncRpcClient::over_iroh(endpoint.clone(), endpoint_addr.clone());
            let blob_rpc_client =
                big_sync::rpc::BigSyncRpcClient::over_iroh(endpoint, endpoint_addr.clone());
            let doc_rpc_client = Arc::new(doc_rpc_client);
            let blob_rpc_client = Arc::new(blob_rpc_client);

            self.blobs_sync_backend
                .register_peer_addr(conn.peer_id.clone(), endpoint_addr.clone());
            self.big_repo_rpc
                .register_peer(endpoint_id, conn.peer_id.clone());
            let _routes = self.task_route_lock.lock().await;
            let partition_ids = self.peer_partition_ids(&conn.peer_id, true).await?;
            let (doc_parts, blob_parts) = Self::split_partitions(partition_ids);
            let task_client: Arc<dyn big_sync::rpc::WireBigSyncRpcClient> =
                Arc::clone(&doc_rpc_client) as _;
            self.task_peer_clients.lock().expect(ERROR_MUTEX).insert(
                conn.peer_id.clone(),
                TaskPeerRoute {
                    client: Arc::clone(&task_client),
                    address: endpoint_addr.clone(),
                },
            );
            self.installed_peer_routes
                .lock()
                .expect(ERROR_MUTEX)
                .remove(&conn.peer_id);
            self.task_sync_worker
                .set_peer(
                    conn.peer_id.clone(),
                    task_client,
                    self.task_transport.routes(&conn.peer_id),
                    HashMap::new(),
                )
                .await?;
            self.big_sync_worker
                .set_peer(
                    conn.peer_id.clone(),
                    Arc::clone(&doc_rpc_client) as Arc<dyn big_sync::rpc::WireBigSyncRpcClient>,
                    doc_parts,
                    HashMap::new(),
                )
                .await?;
            if !blob_parts.is_empty() {
                self.blob_sync_worker
                    .set_peer(
                        conn.peer_id.clone(),
                        Arc::clone(&blob_rpc_client)
                            as Arc<dyn big_sync::rpc::WireBigSyncRpcClient>,
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
                peer_id.clone(),
                ActivePeerState::Connected {
                    peer_key,
                    closed: conn.closed_flag(),
                },
            );
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );
            self.peer_registration_changed.notify_waiters();
            self.task_control_wake.send_replace(());
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
            self.teardown_peer_registration(peer_id.clone()).await;
            let old = self.active_peers.write().await.remove(&peer_id);
            assert!(
                matches!(old, Some(ActivePeerState::Connecting { .. })),
                "fishy"
            );
            self.peer_registration_changed.notify_waiters();
        }

        res
    }

    #[tracing::instrument(level = "debug", skip_all, err(Debug), fields(otel.kind = "client"))]
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
            .cloned()
            .collect::<Vec<_>>();
        for peer_id in peers {
            if let Err(err) = self
                .blobs_sync_backend
                .ensure_local_blob(peer_id.clone(), blob_id.clone())
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
        peer_ids: &[PeerKey],
        required_partitions: &[PartKey],
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
            .cloned()
            .partition(|part| self.is_blob_part(part));
        let wait_fut = async {
            let doc_wait = self
                .big_sync_worker
                .wait_for_full_sync(peer_ids.iter().cloned(), doc_parts.iter().cloned());
            let blob_wait = self
                .blob_sync_worker
                .wait_for_full_sync(peer_ids.iter().cloned(), blob_parts.iter().cloned());
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
        peer_ids: &[PeerKey],
        required_partitions: &[PartKey],
    ) -> Res<()> {
        self.ensure_repo_live()?;
        let (blob_parts, doc_parts): (Vec<_>, Vec<_>) = required_partitions
            .iter()
            .cloned()
            .partition(|part| self.is_blob_part(part));
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
        peer_ids: &[PeerKey],
        timeout: Option<Duration>,
    ) -> Res<()> {
        for peer in peer_ids {
            let parts = self
                .peer_partition_ids(peer, true)
                .await?
                .into_keys()
                .collect::<Vec<_>>();
            self.wait_for_full_sync(std::slice::from_ref(peer), &parts, timeout)
                .await?;
        }
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
