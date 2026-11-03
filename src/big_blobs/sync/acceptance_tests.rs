use super::tests::Harness;
use super::*;
use big_repo::keyhive_core::access::Access;
use big_sync::SyncBackend;
use big_sync::rpc::spawn_big_sync_rpc;
use big_sync_core::PartKey;
use iroh::protocol::Router;
use iroh_blobs::BlobsProtocol;
use iroh_blobs::Hash;
use iroh_blobs::api::Store;
use iroh_blobs::api::TempTag;
use std::collections::HashMap;

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
        spawn_big_sync_rpc(HashMap::from([(Arc::from("inventory-transfers"), parts)])).await?;
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

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_transfer_inventory_bytes_without_direct_child_invitation() -> Res<()> {
    let bytes = vec![61; 200_000];
    let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
    let inventory = big_cabinet_core::DocId::new([91; 32]);
    let part = crate::workers::inventory_part(&inventory);
    let owner_parts: big_repo::SharedPartStore = Arc::new(big_sync::MemoryPartStore::new());
    let middle = Harness::new_network().await?;
    let receiver = Harness::new_network().await?;
    let middle_peer = big_sync_core::PeerKey::new(*middle.endpoint.id().as_bytes());
    let receiver_peer = big_sync_core::PeerKey::new(*receiver.endpoint.id().as_bytes());
    let source = source_node(&bytes, owner_parts, part.clone(), middle_peer.clone()).await?;
    let source_peer = big_sync_core::PeerKey::new(*source.router.endpoint().id().as_bytes());

    middle.mirrors.admission.register(&inventory).await?;
    receiver.mirrors.admission.register(&inventory).await?;
    middle
        .mirrors
        .select_inventory(inventory.clone(), "ciphertext".into())
        .await?;
    receiver
        .mirrors
        .select_inventory(inventory.clone(), "ciphertext".into())
        .await?;
    middle.pin(&inventory, digest, bytes.len() as u64).await?;
    receiver.pin(&inventory, digest, bytes.len() as u64).await?;
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
            source_peer,
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
        .set_part_members(part.clone(), HashMap::from([(receiver_peer.clone(), Access::Read)]))
        .await?;
    let (middle_rpc, middle_rpc_stop) = spawn_big_sync_rpc(HashMap::from([(
        Arc::from("inventory-transfers"),
        Arc::clone(&middle.mirrors.projection.parts),
    )]))
    .await?;
    let middle_router = Router::builder(middle.endpoint.clone())
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
    let (receiver_worker, receiver_stop) = big_sync::spawn_big_sync_worker(
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
            HashMap::from([(part, Arc::from("blob-inventory"))]),
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

    receiver_stop.stop().await?;
    middle_stop.stop().await?;
    middle_router.shutdown().await?;
    middle_rpc_stop.stop().await?;
    source.rpc_stop.stop().await?;
    source.router.shutdown().await?;
    middle.shutdown().await?;
    receiver.shutdown().await?;
    Ok(())
}
