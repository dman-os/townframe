use crate::test::{StressBigSyncRpcClient, boot_part_store};
use crate::{
    BigRepo, BigRepoConnection, BigRepoStopToken, Config, DocumentId, PeerId, SharedPartStore,
    StorageConfig,
};
use big_sync::stress_support;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, timeout};

pub(crate) struct Node {
    pub(crate) repo: Arc<BigRepo>,
    pub(crate) store: SharedPartStore,
    worker: big_sync::BigSyncWorkerHandle,
    big_sync_stop: big_sync::StopToken,
    repo_stop: BigRepoStopToken,
    endpoint: iroh::Endpoint,
    _router: iroh::protocol::Router,
    accepted: Arc<Mutex<Option<BigRepoConnection>>>,
    accepts: Arc<Notify>,
}

#[derive(Clone, Debug)]
struct AcceptHandler {
    repo: Arc<BigRepo>,
    accepted: Arc<Mutex<Option<BigRepoConnection>>>,
    accepts: Arc<Notify>,
}

impl iroh::protocol::ProtocolHandler for AcceptHandler {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(conn, None)
            .await
            .map_err(|err| iroh::protocol::AcceptError::from_boxed(err.into()))?;
        *self.accepted.lock().await = Some(connection);
        self.accepts.notify_waiters();
        Ok(())
    }
}

impl Node {
    pub(crate) async fn boot(seed: u8) -> crate::Res<Self> {
        let (host, initial_stop) = boot_part_store("sqlite::memory:").await?;
        let part_init_obj = big_sync_core::ObjId(big_sync_core::Byte32Id::new(
            [255_u8.wrapping_sub(seed); 32],
        ));
        host.store
            .set_obj_payload(
                part_init_obj,
                serde_json::json!({ "heads": Vec::<String>::new() }),
            )
            .await?;
        host.store
            .remove_obj_from_part(part_init_obj, stress_support::test_part())
            .await?;
        initial_stop.stop().await?;

        let (repo, repo_stop) = BigRepo::boot(
            Config {
                node_identity_seed: [seed; 32],
                storage: StorageConfig::Memory,
            },
            Arc::clone(&host.store),
        )
        .await?;

        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await?;
        let accepted = Arc::new(Mutex::new(None));
        let accepts = Arc::new(Notify::new());
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                AcceptHandler {
                    repo: Arc::clone(&repo),
                    accepted: Arc::clone(&accepted),
                    accepts: Arc::clone(&accepts),
                },
            )
            .spawn();

        let sync_backend = Arc::new(crate::BigRepoSyncBackend::boot(Arc::downgrade(&repo)).await?);
        let mut backends = HashMap::new();
        backends.insert(BigRepo::BACKEND_ID.into(), sync_backend as _);
        let (worker, big_sync_stop) =
            big_sync::spawn_big_sync_worker(Arc::clone(&host.store), backends)?;
        Ok(Self {
            repo,
            store: Arc::clone(&host.store),
            worker,
            big_sync_stop,
            repo_stop,
            endpoint,
            _router: router,
            accepted,
            accepts,
        })
    }

    pub(crate) fn peer_id(&self) -> PeerId {
        self.repo.local_peer_id()
    }

    pub(crate) async fn connect(&self, remote: &Self) -> crate::Res<BigRepoConnection> {
        let connection = self
            .repo
            .open_connection_iroh(
                self.endpoint.clone(),
                remote.endpoint.addr(),
                remote.peer_id(),
                None,
            )
            .await?;
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part| (part, BigRepo::BACKEND_ID.into()))
            .collect();
        self.worker
            .set_peer(
                remote.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&remote.store),
                }),
                parts,
            )
            .await?;
        let parts = stress_support::test_parts()
            .into_iter()
            .map(|part| (part, BigRepo::BACKEND_ID.into()))
            .collect();
        remote
            .worker
            .set_peer(
                self.peer_id(),
                Arc::new(StressBigSyncRpcClient {
                    target_part_store: Arc::clone(&self.store),
                }),
                parts,
            )
            .await?;
        Ok(connection)
    }

    pub(crate) async fn accepted_connection(&self) -> BigRepoConnection {
        timeout(Duration::from_secs(10), async {
            loop {
                if let Some(connection) = self.accepted.lock().await.take() {
                    return connection;
                }
                self.accepts.notified().await;
            }
        })
        .await
        .expect("timed out waiting for accepted connection")
    }

    pub(crate) async fn stop(self) -> crate::Res<()> {
        self.endpoint.close().await;
        self.repo_stop.stop().await?;
        self.big_sync_stop.stop().await?;
        Ok(())
    }
}

pub(crate) async fn assert_materialized_parity(
    left: &Node,
    right: &Node,
    doc_id: DocumentId,
) -> crate::Res<()> {
    let left_state = left.repo.doc_head_state(doc_id).await?;
    let right_state = right.repo.doc_head_state(doc_id).await?;
    assert_eq!(left_state.sedimentree_heads, right_state.sedimentree_heads);
    assert_eq!(
        left_state.materialized_heads,
        right_state.materialized_heads
    );
    Ok(())
}
