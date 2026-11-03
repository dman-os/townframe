use super::tests::Harness;
use super::*;
use big_repo::BigRepoConnection;
use big_repo::SharedBigRepo;
use big_repo::keyhive_core::access::Access;
use big_sync::SyncBackend;
use big_sync::rpc::spawn_big_sync_rpc;
use big_sync_core::PartKey;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use iroh::protocol::Router;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::TempTag;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug)]
struct AcceptRepo {
    repo: SharedBigRepo,
    endpoint: iroh::Endpoint,
    accepted: mpsc::UnboundedSender<BigRepoConnection>,
}

impl ProtocolHandler for AcceptRepo {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let connection = self
            .repo
            .accept_connection_iroh(connection, self.endpoint.clone(), None)
            .await
            .map_err(|error| AcceptError::from_boxed(error.into()))?;
        self.accepted.send(connection).unwrap();
        Ok(())
    }
}

fn repo_router(harness: &Harness) -> (Router, mpsc::UnboundedReceiver<BigRepoConnection>) {
    let (accepted, receiver) = mpsc::unbounded_channel();
    let router = Router::builder(harness.endpoint.clone())
        .accept(
            subduction_iroh::ALPN,
            AcceptRepo {
                repo: Arc::clone(&harness.mirrors.projection.repo),
                endpoint: harness.endpoint.clone(),
                accepted,
            },
        )
        .spawn();
    (router, receiver)
}

struct SourceNode {
    router: Router,
    _tag: TempTag,
    rpc: big_sync::rpc::BigSyncRpcHandle,
    rpc_stop: big_sync::rpc::BigSyncRpcStopToken,

}

async fn source_node(
    bytes: &[u8],
    parts: big_repo::SharedPartStore,
    part: PartKey,
    peer: big_sync_core::PeerKey,
) -> Res<SourceNode> {
    parts.ensure_part(part.clone()).await?;
    parts
        .set_part_members(part.clone(), HashMap::from([(peer, Access::Read)]))
        .await?;
    let digest = BlobId::new(*Hash::new(bytes).as_bytes());
    parts
        .set_obj_payload(
            digest.into(),
            serde_json::json!({"lengthOctets": bytes.len()}),
        )
        .await?;
    parts.add_obj_to_parts(digest.into(), vec![part]).await?;

    let (store, _) = iroh_blobs::store::mem::MemStore::new_with_virtuals(Default::default());
    let store = Store::from(store);
    let tag = store.blobs().add_bytes(bytes.to_vec()).temp_tag().await?;
    let (source_rpc, rpc_stop) =
        spawn_big_sync_rpc(HashMap::from([(Arc::from("inventory-transfers"), Arc::clone(&parts))])).await?;
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?;
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .accept(
            big_sync::rpc::BIG_SYNC_RPC_ALPN,
            source_rpc.protocol_handler(),
        )
        .spawn();
    Ok(SourceNode {
        router,
        _tag: tag,
        rpc: source_rpc,
        rpc_stop,

    })

}

async fn transfer_inventory_bytes(owner: &str, revoke: bool, restart: bool) -> Res<()> {
    let seed = match (owner, revoke, restart) {
        ("ciphertext", false, false) => 101,
        ("plaintext", false, false) => 111,
        ("ciphertext", true, false) => 121,
        ("ciphertext", false, true) => 131,
        _ => unreachable!("unsupported acceptance fixture variant"),
    };
    let bytes = vec![61; 200_000];
    let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
    let middle = Harness::new_network_with_seed(seed).await?;
    let inventory = middle.inventory().await?;
    let receiver = Harness::new_network_with_seed(seed + 1).await?;
    let unrelated_inventory = receiver.inventory().await?;
    let (middle_router, mut middle_accepted) = repo_router(&middle);
    let (receiver_router, _receiver_accepted) = repo_router(&receiver);

    let receiver_connection = receiver
        .mirrors
        .projection
        .repo
        .open_connection_iroh(
            receiver.endpoint.clone(),
            middle.endpoint.addr(),
            middle.mirrors.projection.repo.local_peer_id(),
            None,
        )
        .await?;
    let middle_connection = middle_accepted.recv().await.unwrap();
    let receiver_agent = middle
        .mirrors
        .projection
        .repo
        .receive_keyhive_contact_card(
            &receiver
                .mirrors
                .projection
                .repo
                .local_keyhive_contact_card(),
        )
        .await?;
    middle
        .mirrors
        .projection
        .repo
        .grant_doc_access(inventory.clone(), receiver_agent.clone(), Access::Read)
        .await?;
    middle_connection.sync_keyhive_with_peer().await?;
    receiver_connection.sync_keyhive_with_peer().await?;
    middle
        .mirrors
        .projection
        .repo
        .wait_for_keyhive_reconciliation()
        .await?;
    receiver
        .mirrors
        .projection
        .repo
        .wait_for_keyhive_reconciliation()
        .await?;
    assert!(receiver.mirrors.projection.readable(&inventory).await?);

    let part = crate::workers::inventory_part(&inventory);
    let owner_parts: big_repo::SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
    let middle_peer = big_sync_core::PeerKey::new(*middle.endpoint.id().as_bytes());
    let receiver_peer = big_sync_core::PeerKey::new(*receiver.endpoint.id().as_bytes());
    let source = source_node(&bytes, owner_parts, part.clone(), middle_peer.clone()).await?;
    let source_peer = big_sync_core::PeerKey::new(*source.router.endpoint().id().as_bytes());

    middle
        .mirrors
        .select_inventory(inventory.clone(), owner.to_owned())
        .await?;
    receiver.mirrors.admission.register(&inventory).await?;
    receiver
        .mirrors
        .select_inventory(inventory.clone(), owner.to_owned())
        .await?;
    middle.pin(&inventory, digest, bytes.len() as u64).await?;
    receiver.pin(&inventory, digest, bytes.len() as u64).await?;
    receiver
        .pin(&unrelated_inventory, digest, bytes.len() as u64)
        .await?;
    middle
        .backend
        .register_peer_addr(source_peer.clone(), source.router.endpoint().addr());

    let source_rpc = source.rpc.in_memory_client(middle_peer.clone());
    let (middle_worker, middle_stop) = big_sync::spawn_big_sync_worker(
        Arc::clone(&middle.mirrors.projection.parts),
        HashMap::from([(
            Arc::from("blob-inventory"),
            Arc::new(middle.backend.clone()) as Arc<dyn SyncBackend>,
        )]),
        "acceptance-middle",
        Arc::from("inventory-transfers"),
    )?;
    middle_worker
        .set_peer(
            source_peer.clone(),
            Arc::new(source_rpc),
            HashMap::from([(part.clone(), Arc::from("blob-inventory"))]),
            HashMap::new(),
        )
        .await?;
    middle.wait_membership(&inventory, digest, true).await?;
    assert_eq!(
        middle
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(Hash::from(digest.to_bytes32()))
            .await?
            .as_ref(),
        bytes.as_slice()
    );

    middle
        .mirrors
        .projection
        .parts
        .set_part_members(
            part.clone(),
            HashMap::from([(receiver_peer.clone(), Access::Read)]),
        )
        .await?;
    let (middle_rpc, middle_rpc_stop) = spawn_big_sync_rpc(HashMap::from([(
        Arc::from("inventory-transfers"),
        Arc::clone(&middle.mirrors.projection.parts),
    )]))
    .await?;
    let middle_blob_router = Router::builder(middle.endpoint.clone())
        .accept(
            iroh_blobs::ALPN,
            BlobsProtocol::new(middle.mirrors.projection.service.store(), None),
        )
        .accept(
            big_sync::rpc::BIG_SYNC_RPC_ALPN,
            middle_rpc.protocol_handler(),
        )
        .spawn();
    receiver.backend.register_peer_addr(
        big_sync_core::PeerKey::new(*middle.endpoint.id().as_bytes()),
        middle.endpoint.addr(),
    );
    let receiver_rpc = middle_rpc.in_memory_client(receiver_peer.clone());
    let (receiver_worker, mut receiver_stop) = big_sync::spawn_big_sync_worker(
        Arc::clone(&receiver.mirrors.projection.parts),
        HashMap::from([(
            Arc::from("blob-inventory"),
            Arc::new(receiver.backend.clone()) as Arc<dyn SyncBackend>,
        )]),
        "acceptance-receiver",
        Arc::from("inventory-transfers"),
    )?;
    receiver_worker
        .set_peer(
            big_sync_core::PeerKey::new(*middle.endpoint.id().as_bytes()),
            Arc::new(receiver_rpc),
            HashMap::from([(part.clone(), Arc::from("blob-inventory"))]),
            HashMap::new(),
        )
        .await?;
    receiver.wait_membership(&inventory, digest, true).await?;
    assert_eq!(
        receiver
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(Hash::from(digest.to_bytes32()))
            .await?
            .as_ref(),
        bytes.as_slice()
    );
    assert!(
        !receiver
            .mirrors
            .projection
            .parts
            .obj_parts(digest.into())
            .await?
            .contains(&crate::workers::inventory_part(&unrelated_inventory))
    );

    if restart {
        receiver_stop.stop().await?;
        let (receiver_worker, new_receiver_stop) = big_sync::spawn_big_sync_worker(
            Arc::clone(&receiver.mirrors.projection.parts),
            HashMap::from([(
                Arc::from("blob-inventory"),
                Arc::new(receiver.backend.clone()) as Arc<dyn SyncBackend>,
            )]),
            "acceptance-receiver-restart",
            Arc::from("inventory-transfers"),
        )?;
        receiver_worker
            .set_peer(
                big_sync_core::PeerKey::new(*middle.endpoint.id().as_bytes()),
                Arc::new(middle_rpc.in_memory_client(receiver_peer.clone())),
                HashMap::from([(
                    crate::workers::inventory_part(&inventory),
                    Arc::from("blob-inventory"),
                )]),
                HashMap::new(),
            )
            .await?;
        receiver_stop = new_receiver_stop;
        assert_eq!(
            receiver
                .mirrors
                .projection
                .service
                .store()
                .blobs()
                .get_bytes(Hash::from(digest.to_bytes32()))
                .await?
                .as_ref(),
            bytes.as_slice()
        );
    }

    if revoke {
        middle
            .mirrors
            .projection
            .repo
            .revoke_doc_access(inventory.clone(), receiver_agent)
            .await?;
        middle_connection.sync_keyhive_with_peer().await?;
        receiver_connection.sync_keyhive_with_peer().await?;
        middle
            .mirrors
            .projection
            .repo
            .wait_for_keyhive_reconciliation()
            .await?;
        receiver
            .mirrors
            .projection
            .repo
            .wait_for_keyhive_reconciliation()
            .await?;
        assert!(!receiver.mirrors.projection.readable(&inventory).await?);
        let retained = receiver
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(Hash::from(digest.to_bytes32()))
            .await?;
        assert_eq!(retained.as_ref(), bytes.as_slice());

        let later_bytes = vec![73; 180_000];
        let later_digest = BlobId::new(*Hash::new(&later_bytes).as_bytes());
        receiver
            .pin(&inventory, later_digest, later_bytes.len() as u64)
            .await?;

        let later_source_parts: big_repo::SharedPartStore =
            Arc::new(big_sync::MemoryPartStore::new());
        let later_source =
            source_node(
                &later_bytes,
                Arc::clone(&later_source_parts),
                part.clone(),
                middle_peer.clone(),
            )
            .await?;

        let later_source_peer =
            big_sync_core::PeerKey::new(*later_source.router.endpoint().id().as_bytes());
        middle
            .backend
            .register_peer_addr(later_source_peer.clone(), later_source.router.endpoint().addr());
        middle_worker
            .set_peer(
                later_source_peer,
                Arc::new(later_source.rpc.in_memory_client(middle_peer.clone())),
                HashMap::from([(part.clone(), Arc::from("blob-inventory"))]),
                HashMap::new(),
            )
            .await?;

        assert!(!receiver
            .mirrors
            .projection
            .parts
            .obj_parts(later_digest.into())
            .await?
            .contains(&crate::workers::inventory_part(&inventory)));
        assert!(receiver
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(Hash::from(later_digest.to_bytes32()))
            .await
            .is_err());
        later_source.rpc_stop.stop().await?;
        later_source.router.shutdown().await?;
    }

    receiver_stop.stop().await?;
    middle_stop.stop().await?;
    middle_rpc_stop.stop().await?;
    source.rpc_stop.stop().await?;
    source.router.shutdown().await?;
    drop(receiver_connection);
    drop(middle_connection);
    middle.shutdown().await?;
    receiver.shutdown().await?;
    receiver_router.shutdown().await?;
    middle_router.shutdown().await?;
    middle_blob_router.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_transfer_inventory_bytes_without_direct_child_invitation() -> Res<()> {
    transfer_inventory_bytes("ciphertext", false, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_transfer_plaintext_inventory_bytes_after_read_plus_grant() -> Res<()> {
    transfer_inventory_bytes("plaintext", false, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_revoke_inventory_access_after_byte_transfer() -> Res<()> {
    transfer_inventory_bytes("ciphertext", true, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_restart_receiver_preserves_transferred_bytes() -> Res<()> {
    transfer_inventory_bytes("ciphertext", false, true).await
}
