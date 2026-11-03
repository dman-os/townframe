use super::tests::Harness;
use super::*;
use bao_tree::ChunkRanges;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use iroh::protocol::Router;
use iroh_blobs::Hash;
use iroh_blobs::protocol::GetRequest;
use iroh_blobs::protocol::Request;
use tokio::sync::Notify;

#[derive(Debug)]
struct Response {
    hash: Hash,
    bytes: Bytes,
    started: Arc<Notify>,
    finish: Arc<Notify>,
}

impl ProtocolHandler for Response {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let (request, _) = Request::read_async(&mut recv).await?;
        assert_eq!(request, Request::Get(GetRequest::blob(self.hash)));
        let last = self.bytes.len() - 1;
        tokio::io::AsyncWriteExt::write_all(&mut send, &self.bytes[..last]).await?;
        self.started.notify_one();
        self.finish.notified().await;
        tokio::io::AsyncWriteExt::write_all(&mut send, &self.bytes[last..]).await?;
        send.finish()?;
        connection.closed().await;
        Ok(())
    }
}

async fn source(bytes: &[u8], corrupt: bool) -> Res<(Router, Arc<Notify>, Arc<Notify>)> {
    let outboard = PreOrderMemOutboard::create(bytes, iroh_blobs::store::IROH_BLOCK_SIZE);
    let mut encoded = (bytes.len() as u64).to_le_bytes().to_vec();
    bao_tree::io::sync::encode_ranges_validated(
        bytes,
        &outboard,
        &ChunkRanges::all(),
        &mut encoded,
    )?;
    if corrupt {
        *encoded.last_mut().unwrap() ^= 1;
    }
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?;
    let router = Router::builder(endpoint)
        .accept(
            iroh_blobs::ALPN,
            Response {
                hash: Hash::from(outboard.root),
                bytes: Bytes::from(encoded),
                started: Arc::clone(&started),
                finish: Arc::clone(&finish),
            },
        )
        .spawn();
    Ok((router, started, finish))
}

#[tokio::test]
async fn pin_first_and_object_first_download_only_after_same_inventory_admission() -> Res<()> {
    for pin_first in [true, false] {
        let harness = Harness::new_with_seed(101).await?;
        let inventory = harness.inventory().await?;
        let bytes =
            crate::encrypt::encrypt_bytes(&crate::encrypt::MasterKey::random(), &vec![7; 200_000]);
        let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
        let (router, started, finish) = source(&bytes, false).await?;
        harness
            .backend
            .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
        harness
            .mirrors
            .select_inventory(inventory.clone(), "root".into())
            .await?;
        if pin_first {
            harness.pin(&inventory, digest, bytes.len() as u64).await?;
        } else {
            harness.observe(&inventory, digest, 10, true).await?;
        }
        assert!(
            !harness
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .fetch_eligible()
        );
        assert!(
            harness
                .mirrors
                .projection
                .service
                .stat(&digest)
                .await?
                .is_none()
        );
        if pin_first {
            harness.observe(&inventory, digest, 10, true).await?;
        } else {
            harness.pin(&inventory, digest, bytes.len() as u64).await?;
        }
        started.notified().await;
        assert!(
            harness
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .fetching
        );
        assert!(
            harness
                .mirrors
                .projection
                .parts
                .obj_parts(digest.into())
                .await?
                .is_empty()
        );
        assert!(
            harness
                .mirrors
                .projection
                .service
                .stat(&digest)
                .await?
                .is_none()
        );
        finish.notify_one();
        harness.wait_membership(&inventory, digest, true).await?;
        assert_eq!(
            harness
                .mirrors
                .projection
                .service
                .store()
                .blobs()
                .get_bytes(Hash::from(digest.to_bytes32()))
                .await?
                .as_ref(),
            bytes
        );
        harness
            .mirrors
            .deselect_inventory(inventory.clone(), "root".into())
            .await?;
        harness.wait_membership(&inventory, digest, false).await?;
        assert!(
            harness
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .retains_bytes()
        );
        router.shutdown().await?;
        harness.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn late_transfer_cannot_publish_after_deselection_pin_removal_or_source_retraction() -> Res<()>
{
    for transition in ["deselect", "unpin", "retract"] {
        let harness = Harness::new().await?;
        let inventory = harness.inventory().await?;
        let bytes = vec![8; 200_000];
        let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
        let (router, started, finish) = source(&bytes, false).await?;
        harness
            .backend
            .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
        harness
            .mirrors
            .select_inventory(inventory.clone(), "root".into())
            .await?;
        harness.pin(&inventory, digest, bytes.len() as u64).await?;
        harness.observe(&inventory, digest, 10, true).await?;
        started.notified().await;
        match transition {
            "deselect" => {
                harness
                    .mirrors
                    .deselect_inventory(inventory.clone(), "root".into())
                    .await?
            }
            "unpin" => {
                harness
                    .mirrors
                    .admission
                    .pins(&inventory, 2, &BTreeMap::new(), true)
                    .await?
            }
            "retract" => harness.observe(&inventory, digest, 20, false).await?,
            _ => unreachable!(),
        }
        let mut completed = harness.mirrors.completions.subscribe();
        completed.borrow_and_update();
        finish.notify_one();
        completed.changed().await?;
        assert_eq!(
            harness
                .mirrors
                .projection
                .service
                .ownership
                .contains(&digest)
                .await?,
            transition != "retract"
        );
        assert!(
            harness
                .mirrors
                .projection
                .service
                .stat(&digest)
                .await?
                .is_some()
        );
        assert!(
            !harness
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .fetching
        );
        assert!(
            !harness
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .advertisable()
        );
        assert!(
            harness
                .mirrors
                .projection
                .parts
                .obj_parts(digest.into())
                .await?
                .is_empty()
        );
        router.shutdown().await?;
        harness.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn both_arrival_orders_download_after_reopen_and_keep_bytes_across_a_second_reopen() -> Res<()>
{
    for pin_first in [true, false] {
        let directory = tempfile::tempdir()?;
        let bytes = vec![11; 200_000];
        let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
        let harness = Harness::reopen(directory.path()).await?;
        let inventory = harness.inventory().await?;
        harness
            .mirrors
            .select_inventory(inventory.clone(), "root".into())
            .await?;
        if pin_first {
            harness.pin(&inventory, digest, bytes.len() as u64).await?;
        } else {
            harness.observe(&inventory, digest, 10, true).await?;
        }
        assert!(
            !harness
                .mirrors
                .admission
                .state(&inventory, digest)
                .await?
                .unwrap()
                .fetch_eligible()
        );
        harness.shutdown().await?;

        let harness = Harness::reopen(directory.path()).await?;
        let state = harness
            .mirrors
            .admission
            .state(&inventory, digest)
            .await?
            .unwrap();
        assert!(state.selected);
        assert_eq!(state.pinned, pin_first);
        assert_eq!(state.observed, !pin_first);
        assert!(
            harness
                .mirrors
                .projection
                .service
                .stat(&digest)
                .await?
                .is_none()
        );
        let (router, started, finish) = source(&bytes, false).await?;
        harness
            .backend
            .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
        if pin_first {
            harness.observe(&inventory, digest, 10, true).await?;
        } else {
            harness.pin(&inventory, digest, bytes.len() as u64).await?;
        }
        started.notified().await;
        assert!(
            harness
                .mirrors
                .projection
                .parts
                .obj_parts(digest.into())
                .await?
                .is_empty()
        );
        finish.notify_one();
        harness.wait_membership(&inventory, digest, true).await?;
        harness.shutdown().await?;
        router.shutdown().await?;

        let harness = Harness::reopen(directory.path()).await?;
        harness.wait_membership(&inventory, digest, true).await?;
        assert_eq!(
            harness
                .mirrors
                .projection
                .service
                .store()
                .blobs()
                .get_bytes(Hash::from(digest.to_bytes32()))
                .await?
                .as_ref(),
            bytes
        );
        assert!(
            harness
                .mirrors
                .projection
                .service
                .ownership
                .contains(&digest)
                .await?
        );
        harness.observe(&inventory, digest, 20, false).await?;
        harness.wait_membership(&inventory, digest, false).await?;
        assert!(
            !harness
                .mirrors
                .projection
                .service
                .ownership
                .contains(&digest)
                .await?
        );
        harness.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn interrupted_transfer_reopens_as_recoverable_work_not_completed_bytes() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let bytes = vec![12; 200_000];
    let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
    let harness = Harness::reopen(directory.path()).await?;
    let inventory = harness.inventory().await?;
    harness
        .mirrors
        .select_inventory(inventory.clone(), "root".into())
        .await?;
    harness.pin(&inventory, digest, bytes.len() as u64).await?;
    let (router, started, _) = source(&bytes, false).await?;
    harness
        .backend
        .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
    harness.observe(&inventory, digest, 10, true).await?;
    started.notified().await;
    assert!(
        harness
            .mirrors
            .admission
            .state(&inventory, digest)
            .await?
            .unwrap()
            .fetching
    );
    harness.shutdown().await?;
    router.shutdown().await?;

    let harness = Harness::reopen(directory.path()).await?;
    let state = harness
        .mirrors
        .admission
        .state(&inventory, digest)
        .await?
        .unwrap();
    assert!(!state.fetching);
    assert!(!state.bytes);
    assert!(!state.advertisable());
    assert!(state.fetch_eligible());
    let (router, started, finish) = source(&bytes, false).await?;
    harness
        .backend
        .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
    started.notified().await;
    finish.notify_one();
    harness.wait_membership(&inventory, digest, true).await?;
    assert_eq!(
        harness
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(Hash::from(digest.to_bytes32()))
            .await?
            .as_ref(),
        bytes
    );
    harness.shutdown().await?;
    router.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn virtual_mirror_keeps_plaintext_and_provider_until_its_last_observation_is_removed()
-> Res<()> {
    let harness = Harness::new().await?;
    let inventory = harness.inventory().await?;
    let service = Arc::clone(&harness.mirrors.projection.service);
    let bytes = b"a mirrored virtual representation retains its physical dependency";
    let (ciphertext, plaintext) = crate::encrypt::add_encrypted(
        service.store(),
        &service.provider,
        &service.roots,
        &crate::encrypt::MasterKey::random(),
        crate::encrypt::EncodingParams::default(),
        bytes,
    )
    .await?;
    let digest = BlobId::new(*ciphertext.as_bytes());
    let plaintext_id = BlobId::new(*plaintext.as_bytes());
    service
        .ownership
        .claim("document:virtual-fixture", &[digest])
        .await?;
    service.ownership.resolve_pair(&digest).await?;
    let info = service.stat(&digest).await?.unwrap();
    harness
        .mirrors
        .select_inventory(inventory.clone(), "root".into())
        .await?;
    harness.pin(&inventory, digest, info.length_octets).await?;
    harness.observe(&inventory, digest, 10, true).await?;
    harness.wait_membership(&inventory, digest, true).await?;
    assert!(
        service
            .store()
            .blobs()
            .sync_reader(ciphertext)
            .await?
            .is_none()
    );
    service
        .ownership
        .release("document:virtual-fixture")
        .await?;
    assert!(service.ownership.contains(&digest).await?);
    assert!(service.ownership.contains(&plaintext_id).await?);
    assert!(service.stat(&digest).await?.is_some());
    assert_eq!(
        service.store().blobs().get_bytes(plaintext).await?.as_ref(),
        bytes
    );
    harness
        .mirrors
        .deselect_inventory(inventory.clone(), "root".into())
        .await?;
    harness.wait_membership(&inventory, digest, false).await?;
    assert!(service.ownership.contains(&digest).await?);
    harness.observe(&inventory, digest, 20, false).await?;
    let mut progress = harness.mirrors.progress.subscribe();
    loop {
        progress.borrow_and_update();
        if !service.ownership.contains(&digest).await? {
            break;
        }
        progress.changed().await?;
    }
    let settled = harness
        .mirrors
        .admission
        .state(&inventory, digest)
        .await?
        .unwrap();
    assert!(!settled.observed && !settled.advertisable());
    assert!(!service.ownership.contains(&plaintext_id).await?);
    assert!(service.stat(&digest).await?.is_none());
    assert!(
        service
            .store()
            .tags()
            .get(format!("ct:{ciphertext}"))
            .await?
            .is_none()
    );
    assert!(
        service
            .store()
            .tags()
            .get(format!("pt:{ciphertext}"))
            .await?
            .is_none()
    );
    harness.shutdown().await
}

#[tokio::test]
async fn completion_releases_capacity_for_the_ninth_eligible_transfer() -> Res<()> {
    let harness = Harness::new().await?;
    let mut transfers = Vec::new();
    for index in 0..9 {
        let inventory = harness.inventory().await?;
        let bytes = vec![index + 20; 200_000];
        let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
        let (router, started, finish) = source(&bytes, false).await?;
        harness
            .backend
            .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
        harness
            .mirrors
            .select_inventory(inventory.clone(), "root".into())
            .await?;
        harness.pin(&inventory, digest, bytes.len() as u64).await?;
        harness.observe(&inventory, digest, 10, true).await?;
        if index < 8 {
            started.notified().await;
        }
        transfers.push((inventory, digest, router, started, finish));
    }
    let (inventory, digest, _, _, _) = &transfers[8];
    let state = harness
        .mirrors
        .admission
        .state(inventory, *digest)
        .await?
        .unwrap();
    assert!(state.fetch_eligible());
    assert!(!state.fetching);
    transfers[0].4.notify_one();
    transfers[8].3.notified().await;
    for (_, _, _, _, finish) in &transfers {
        finish.notify_one();
    }
    for (inventory, digest, router, _, _) in transfers {
        harness.wait_membership(&inventory, digest, true).await?;
        assert!(
            harness
                .mirrors
                .projection
                .service
                .ownership
                .contains(&digest)
                .await?
        );
        router.shutdown().await?;
    }
    harness.shutdown().await
}

#[tokio::test]
async fn identical_remote_bytes_do_not_cross_unlock_inventory_pairs() -> Res<()> {
    let harness = Harness::new().await?;
    let first = harness.inventory().await?;
    let second = harness.inventory().await?;
    let bytes = vec![41; 200_000];
    let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
    let (router, started, finish) = source(&bytes, false).await?;
    harness
        .backend
        .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
    for inventory in [&first, &second] {
        harness
            .mirrors
            .select_inventory(inventory.clone(), "root".into())
            .await?;
    }
    harness.observe(&first, digest, 10, true).await?;
    harness.pin(&second, digest, bytes.len() as u64).await?;
    for inventory in [&first, &second] {
        let state = harness
            .mirrors
            .admission
            .state(inventory, digest)
            .await?
            .unwrap();
        assert!(!state.fetch_eligible());
        assert!(!state.fetching);
    }
    harness.pin(&first, digest, bytes.len() as u64).await?;
    started.notified().await;
    finish.notify_one();
    harness.wait_membership(&first, digest, true).await?;
    assert_eq!(
        harness
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(Hash::from(digest.to_bytes32()))
            .await?
            .as_ref(),
        bytes
    );
    let second_state = harness
        .mirrors
        .admission
        .state(&second, digest)
        .await?
        .unwrap();
    assert!(!second_state.observed);
    assert!(!second_state.advertisable());
    assert_eq!(
        harness
            .mirrors
            .projection
            .parts
            .obj_parts(digest.into())
            .await?,
        vec![crate::workers::inventory_part(&first)]
    );
    harness.observe(&second, digest, 10, true).await?;
    harness.wait_membership(&second, digest, true).await?;
    harness.observe(&first, digest, 20, false).await?;
    harness.wait_membership(&first, digest, false).await?;
    assert_eq!(
        harness
            .mirrors
            .projection
            .parts
            .obj_parts(digest.into())
            .await?,
        vec![crate::workers::inventory_part(&second)]
    );
    assert!(
        harness
            .mirrors
            .projection
            .service
            .ownership
            .contains(&digest)
            .await?
    );
    router.shutdown().await?;
    harness.shutdown().await
}

#[tokio::test]
async fn corrupt_download_parks_without_advertising_or_acknowledging_bytes() -> Res<()> {
    let harness = Harness::new().await?;
    let inventory = harness.inventory().await?;
    let bytes = vec![9; 200_000];
    let digest = BlobId::new(*Hash::new(&bytes).as_bytes());
    let (router, started, finish) = source(&bytes, true).await?;
    harness
        .backend
        .register_peer_addr(PeerKey::new([103; 32]), router.endpoint().addr());
    harness
        .mirrors
        .select_inventory(inventory.clone(), "root".into())
        .await?;
    harness.pin(&inventory, digest, bytes.len() as u64).await?;
    harness.observe(&inventory, digest, 10, true).await?;
    started.notified().await;
    let attempt = harness
        .mirrors
        .admission
        .state(&inventory, digest)
        .await?
        .unwrap()
        .generation;
    finish.notify_one();
    let mut progress = harness.mirrors.progress.subscribe();
    loop {
        progress.borrow_and_update();
        let state = harness
            .mirrors
            .admission
            .state(&inventory, digest)
            .await?
            .unwrap();
        if !state.fetching && state.generation > attempt {
            assert!(!state.bytes);
            assert!(!state.advertisable());
            break;
        }
        progress.changed().await?;
    }
    assert!(
        harness
            .mirrors
            .projection
            .parts
            .obj_parts(digest.into())
            .await?
            .is_empty()
    );
    assert!(
        harness
            .mirrors
            .projection
            .service
            .stat(&digest)
            .await?
            .is_none()
    );
    router.shutdown().await?;
    harness.shutdown().await
}
