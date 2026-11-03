use super::*;
use big_repo::BigRepoConnection;
use big_repo::BigRepoSyncBackend;
use std::collections::HashMap;
use std::collections::HashSet;

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

#[derive(Debug)]
struct ServeBlobs(iroh_blobs::BlobsProtocol);

impl iroh::protocol::ProtocolHandler for ServeBlobs {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        iroh::protocol::ProtocolHandler::accept(&self.0, connection).await
    }
}

pub(super) struct Network {
    pub endpoint: iroh::Endpoint,
    router: iroh::protocol::Router,
    docs: big_sync::BigSyncWorkerHandle,
    blobs: big_sync::BigSyncWorkerHandle,
    backend: Arc<big_blobs::sync::BlobSyncBackend>,
    stops: Vec<service_utils_rs::StopToken>,
    doc_stop: big_sync::StopToken,
    blob_stop: big_sync::StopToken,
    repo_rpc: big_repo::rpc::BigRepoRpcHandle,
    _sync_rpc: big_sync::rpc::BigSyncRpcHandle,
    repo_rpc_stop: big_repo::rpc::BigRepoRpcStopToken,
    sync_rpc_stop: big_sync::rpc::BigSyncRpcStopToken,
    peers: tokio::sync::watch::Sender<HashSet<big_sync_core::PeerKey>>,
    accepted: tokio::sync::mpsc::UnboundedReceiver<BigRepoConnection>,
}

impl Network {
    pub async fn boot(node: &BigNode, seed: u8) -> Self {
        let lookup = iroh::address_lookup::MemoryLookup::new();
        let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .address_lookup(lookup.clone())
            .clear_ip_transports()
            .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .secret_key(iroh::SecretKey::from_bytes(&[seed; 32]))
            .relay_mode(iroh::RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let backend = Arc::new(big_blobs::sync::BlobSyncBackend::new(
            node.blob_workers.mirrors.clone(),
            endpoint.clone(),
            lookup,
        ));
        let (accepted, accepted_rx) = tokio::sync::mpsc::unbounded_channel();
        let (repo_rpc, repo_rpc_stop) = big_repo::rpc::spawn_repo_rpc(Arc::clone(&node.repo))
            .await
            .unwrap();
        let (sync_rpc, sync_rpc_stop) = big_sync::rpc::spawn_big_sync_rpc(HashMap::from([
            (Arc::from("discovery-docs"), node.repo.shared_part_store()),
            (
                Arc::from("discovery-blobs"),
                Arc::clone(&node.blob_workers.parts),
            ),
        ]))
        .await
        .unwrap();
        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(
                subduction_iroh::ALPN,
                Accept {
                    repo: Arc::clone(&node.repo),
                    endpoint: endpoint.clone(),
                    accepted,
                },
            )
            .accept(big_repo::rpc::REPO_SYNC_ALPN, repo_rpc.protocol_handler())
            .accept(
                big_sync::rpc::BIG_SYNC_RPC_ALPN,
                sync_rpc.protocol_handler(),
            )
            .accept(
                iroh_blobs::ALPN,
                ServeBlobs(iroh_blobs::BlobsProtocol::new(
                    node.blobs.service.store(),
                    None,
                )),
            )
            .spawn();
        let repo_backend = Arc::new(
            BigRepoSyncBackend::boot(Arc::downgrade(&node.repo))
                .await
                .unwrap(),
        );
        let (docs, doc_stop) = big_sync::spawn_big_sync_worker_with_options(
            node.repo.shared_part_store(),
            HashMap::from([(BigRepo::BACKEND_ID.into(), repo_backend as _)]),
            "discovery-docs",
            None,
            Some(big_sync::SyncMode::Bucket),
            Arc::from("discovery-docs"),
        )
        .unwrap();
        let (blobs, blob_stop) = big_sync::spawn_big_sync_worker_with_options(
            Arc::clone(&node.blob_workers.parts),
            HashMap::from([(Arc::from("blob-inventory"), Arc::clone(&backend) as _)]),
            "discovery-blobs",
            None,
            Some(big_sync::SyncMode::CursorOnly),
            Arc::from("discovery-blobs"),
        )
        .unwrap();
        let (peers, peer_rx) = tokio::sync::watch::channel(HashSet::new());
        let (stop, tasks) = service_utils_rs::StopToken::new();
        let cancel = tasks.clone();
        let demand = node.repo.drive_sync_demand(&docs, peer_rx);
        tasks.spawn(async move {
            tokio::select! { biased; _ = cancel.cancelled() => (), result = demand => result.unwrap(), }
        }).unwrap();
        let mirrors = node.blob_workers.mirrors.clone();
        let source = blobs.subscription_source();
        let cancel = tasks.clone();
        tasks.spawn(async move {
            tokio::select! { biased; _ = cancel.cancelled() => (), result = mirrors.drive_subscriptions(source, Arc::from("blob-inventory")) => result.unwrap(), }
        }).unwrap();
        Self {
            endpoint,
            router,
            docs,
            blobs,
            backend,
            stops: vec![stop],
            doc_stop,
            blob_stop,
            repo_rpc,
            _sync_rpc: sync_rpc,
            repo_rpc_stop,
            sync_rpc_stop,
            peers,
            accepted: accepted_rx,
        }
    }

    pub async fn link(
        owner: (&BigNode, &Self),
        reader: (&BigNode, &mut Self),
    ) -> (BigRepoConnection, BigRepoConnection) {
        let (owner, owner_net) = owner;
        let (reader, reader_net) = reader;
        owner_net
            .repo_rpc
            .register_peer(reader_net.endpoint.id(), reader.repo.local_peer_id());
        reader_net
            .repo_rpc
            .register_peer(owner_net.endpoint.id(), owner.repo.local_peer_id());
        let connection = owner
            .repo
            .open_connection_iroh(
                owner_net.endpoint.clone(),
                reader_net.endpoint.addr(),
                reader.repo.local_peer_id(),
                None,
            )
            .await
            .unwrap();
        let accepted = reader_net.accepted.recv().await.unwrap();
        for worker in [&reader_net.docs, &reader_net.blobs] {
            worker
                .set_peer(
                    owner.repo.local_peer_id(),
                    Arc::new(big_sync::rpc::BigSyncRpcClient::over_iroh(
                        reader_net.endpoint.clone(),
                        owner_net.endpoint.addr(),
                    )),
                    HashMap::new(),
                    HashMap::new(),
                )
                .await
                .unwrap();
        }
        reader_net
            .backend
            .register_peer_addr(owner.repo.local_peer_id(), owner_net.endpoint.addr());
        reader_net
            .peers
            .send_replace(HashSet::from([owner.repo.local_peer_id()]));
        (connection, accepted)
    }

    pub async fn stop(mut self) {
        for stop in self.stops.drain(..) {
            stop.stop().await;
        }
        self.blob_stop.stop().await.unwrap();
        self.doc_stop.stop().await.unwrap();
        self.router.shutdown().await.unwrap();
        self.repo_rpc_stop.stop().await.unwrap();
        self.sync_rpc_stop.stop().await.unwrap();
        self.endpoint.close().await;
    }
}
