//! An iroh endpoint, a big_sync worker, and the repo/sync RPC services around one BigRepo: the
//! two-peer wiring an embedder does, for end-to-end tests of what crosses between nodes.

use big_repo::{BigRepo, BigRepoConnection, BigRepoSyncBackend, SharedBigRepo};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct Accept {
    repo: SharedBigRepo,
    endpoint: iroh::Endpoint,
    accepted: tokio::sync::mpsc::UnboundedSender<BigRepoConnection>,
}

impl iroh::protocol::ProtocolHandler for Accept {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(connection, self.endpoint.clone(), None)
            .await
            .map_err(|error| iroh::protocol::AcceptError::from_boxed(error.into()))?;
        self.accepted.send(connection).unwrap();
        Ok(())
    }
}

pub(crate) struct Network {
    pub(crate) endpoint: iroh::Endpoint,
    _router: iroh::protocol::Router,
    pub(crate) worker: big_sync::BigSyncWorkerHandle,
    worker_stop: big_sync::StopToken,
    repo_rpc_stop: big_repo::rpc::BigRepoRpcStopToken,
    sync_rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    _sync_rpc: big_sync::rpc::BigSyncRpcHandle,
    pub(crate) accepted: tokio::sync::mpsc::UnboundedReceiver<BigRepoConnection>,
}

impl Network {
    pub(crate) async fn boot(repo: &SharedBigRepo, seed: u8) -> Self {
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let (accepted, accepted_rx) = tokio::sync::mpsc::unbounded_channel();
        let (repo_rpc, repo_rpc_stop) = big_repo::rpc::spawn_repo_rpc(Arc::clone(repo))
            .await
            .unwrap();
        let store = repo.shared_part_store();
        let (sync_rpc, sync_rpc_stop) = big_sync::rpc::spawn_big_sync_rpc(HashMap::from([(
            Arc::from("cabinet-peer"),
            Arc::clone(&store),
        )]))
        .await
        .unwrap();
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                Accept {
                    repo: Arc::clone(repo),
                    endpoint: endpoint.clone(),
                    accepted,
                },
            )
            .accept(big_repo::rpc::REPO_SYNC_ALPN, repo_rpc.protocol_handler())
            .accept(
                big_sync::rpc::BIG_SYNC_RPC_ALPN,
                sync_rpc.protocol_handler(),
            )
            .spawn();
        let backend = Arc::new(
            BigRepoSyncBackend::boot(Arc::downgrade(repo))
                .await
                .unwrap(),
        );
        let (worker, worker_stop) = big_sync::spawn_big_sync_worker_with_options(
            store,
            HashMap::from([(BigRepo::BACKEND_ID.into(), backend as _)]),
            "cabinet-peer",
            None,
            Some(big_sync::SyncMode::Bucket),
            Arc::from("cabinet-peer"),
        )
        .unwrap();
        Self {
            endpoint,
            _router: router,
            worker,
            worker_stop,
            repo_rpc_stop,
            sync_rpc_stop,
            _sync_rpc: sync_rpc,
            accepted: accepted_rx,
        }
    }

    pub(crate) async fn stop(self) {
        self.worker_stop.stop().await.unwrap();
        self.endpoint.close().await;
        self.repo_rpc_stop.stop().await.unwrap();
        self.sync_rpc_stop.stop().await.unwrap();
    }
}

/// Join two nodes: `source` opens the connection, `puller` accepts it and takes `source` as the
/// peer its worker pulls from. Answers with the connections on the source and puller sides.
pub(crate) async fn link(
    source: (&SharedBigRepo, &Network),
    puller: (&SharedBigRepo, &mut Network),
) -> (BigRepoConnection, BigRepoConnection) {
    let (source_repo, source_net) = source;
    let (puller_repo, puller_net) = puller;
    let connection = source_repo
        .open_connection_iroh(
            source_net.endpoint.clone(),
            puller_net.endpoint.addr(),
            puller_repo.local_peer_id(),
            None,
        )
        .await
        .unwrap();
    let accepted = puller_net.accepted.recv().await.unwrap();
    puller_net
        .worker
        .set_peer(
            source_repo.local_peer_id(),
            Arc::new(big_sync::rpc::BigSyncRpcClient::over_iroh(
                puller_net.endpoint.clone(),
                source_net.endpoint.addr(),
            )),
            HashMap::new(),
            HashMap::new(),
        )
        .await
        .unwrap();
    (connection, accepted)
}
