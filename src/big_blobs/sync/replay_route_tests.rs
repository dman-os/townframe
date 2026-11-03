use super::tests::Harness;
use super::*;
use big_repo::keyhive_core::access::Access;
use big_sync::receive::ReceiveProgress;
use big_sync::receive::ReceiveRoute;
use big_sync::receive::ReplayReceipt;
use big_sync::receive::ReplayReceiver;
use big_sync::rpc::BIG_SYNC_RPC_ALPN;
use big_sync::rpc::BigSyncRpcClient;
use big_sync::rpc::spawn_big_sync_rpc;
use big_sync_core::rpc::ObjChanged;
use big_sync_core::rpc::ObjRemovedFromPart;
use big_sync_core::rpc::PartEvent;
use big_sync_core::rpc::ReplaySubscriptionTarget;
use big_sync_core::rpc::TargetVerdict;
use iroh::protocol::AcceptError;
use iroh::protocol::ProtocolHandler;
use iroh::protocol::Router;
use sqlite_rs::SqliteRuntime;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

type Delivery = (ReplayReceipt, oneshot::Sender<()>);

#[derive(Debug)]
struct ServiceOwnedBlobs(iroh_blobs::BlobsProtocol);

impl ProtocolHandler for ServiceOwnedBlobs {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        self.0.accept(connection).await
    }
}

struct ReplayProbe {
    inner: BlobSyncBackend,
    admitted: mpsc::UnboundedSender<Delivery>,
    content: mpsc::UnboundedSender<(PeerKey, ObjKey)>,
}

#[async_trait]
impl ReplayReceiver for ReplayProbe {
    async fn receive_replay(
        &self,
        receipt: &ReplayReceipt,
        targets: &[ReplaySubscriptionTarget],
    ) -> Res<()> {
        assert_eq!(
            targets,
            receipt
                .replay
                .routes
                .iter()
                .map(|route| route.target.clone())
                .collect::<Vec<_>>()
        );
        self.inner.receive_replay(receipt, targets).await?;
        if !receipt.replay.page.events.is_empty() {
            let (release, wait) = oneshot::channel();
            self.admitted
                .send((receipt.clone(), release))
                .expect("test observes every replay admission");
            wait.await.expect("test releases replay acknowledgment");
        }
        Ok(())
    }
}

#[async_trait]
impl SyncBackend for ReplayProbe {
    fn replay_receiver(&self) -> Option<&dyn ReplayReceiver> {
        self.inner.replay_receiver()?;
        Some(self)
    }

    async fn sync_obj(
        &self,
        peer: PeerKey,
        object: ObjKey,
        parts: big_sync_core::PartObservations,
        payload: Option<ObjPayload>,
    ) -> Res<SyncTaskRunOutcome> {
        assert!(
            parts.is_empty(),
            "replay owns all blob membership observations"
        );
        let outcome = self
            .inner
            .sync_obj(peer.clone(), object.clone(), parts, payload)
            .await?;
        self.content
            .send((peer, object))
            .expect("test observes content-only callbacks");
        Ok(outcome)
    }

    async fn remove_obj_from_parts(
        &self,
        _peer: PeerKey,
        _object: ObjKey,
        _parts: big_sync_core::PartObservations,
    ) -> Res<()> {
        panic!("blob removals must arrive through durable replay admission")
    }
}

fn backends(
    harness: &Harness,
    admitted: mpsc::UnboundedSender<Delivery>,
    content: mpsc::UnboundedSender<(PeerKey, ObjKey)>,
) -> HashMap<big_sync::BackendId, Arc<dyn SyncBackend>> {
    HashMap::from([(
        Arc::from("blob-inventory"),
        Arc::new(ReplayProbe {
            inner: harness.backend.clone(),
            admitted,
            content,
        }) as Arc<dyn SyncBackend>,
    )])
}

async fn assert_pending(
    harness: &Harness,
    receipt: &ReplayReceipt,
    route: &ReceiveRoute,
    predecessor: u64,
    revision: u64,
) -> Res<()> {
    let journal = harness.mirrors.projection.parts.receive_journal().unwrap();
    assert_eq!(journal.pending().await?, Some(receipt.clone()));
    assert_eq!(receipt.replay.routes, vec![route.clone()]);
    assert_eq!(
        receipt.replay.requested,
        vec![route.target.with_cursor(predecessor)]
    );
    assert_eq!(
        receipt.replay.page.targets,
        vec![(
            route.target.with_cursor(revision),
            TargetVerdict::Events {
                resume: revision,
                drained: true,
            }
        )]
    );
    assert_eq!(
        journal
            .progress(receipt.replay.peer_id.clone(), route.clone())
            .await?,
        ReceiveProgress {
            received: revision,
            applied: predecessor,
        }
    );
    Ok(())
}

async fn source_observations(
    path: &std::path::Path,
    inventory: &big_cabinet_core::DocId,
    digest: BlobId,
    peer: &PeerKey,
) -> Res<Vec<(u64, bool, String)>> {
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.database(path.join("blob-state.sqlite"), &[], "fixture_migrations");
    Ok(sql
        .query(
            "SELECT revision
              , present
              , payload
           FROM big_blobs_source_observations
          WHERE inventory_id = ? AND digest = ? AND peer_id = ?",
            (
                inventory.to_string(),
                digest.to_string(),
                peer.as_bytes().to_vec(),
            ),
        )
        .await?
        .all()?)
}

async fn prepare_receiver(
    receiver: &Harness,
    inventory: &big_cabinet_core::DocId,
    digest: BlobId,
    length: u64,
) -> Res<()> {
    receiver
        .mirrors
        .select_inventory(inventory.clone(), "replay-root".into())
        .await?;
    receiver.pin(inventory, digest, length).await?;
    assert_eq!(
        receiver
            .mirrors
            .admission
            .inventory(&crate::workers::inventory_part(inventory))
            .await?,
        Some(inventory.clone())
    );
    assert!(
        !receiver
            .mirrors
            .admission
            .state(inventory, digest)
            .await?
            .unwrap()
            .observed
    );
    assert!(
        receiver
            .mirrors
            .projection
            .service
            .stat(&digest)
            .await?
            .is_none()
    );
    Ok(())
}

async fn assert_downloaded(
    receiver: &Harness,
    inventory: &big_cabinet_core::DocId,
    bytes: &[u8],
) -> Res<()> {
    let hash = iroh_blobs::Hash::new(bytes);
    let digest = BlobId::new(*hash.as_bytes());
    receiver.wait_membership(inventory, digest, true).await?;
    assert!(
        receiver
            .mirrors
            .admission
            .state(inventory, digest)
            .await?
            .unwrap()
            .advertisable()
    );
    assert_eq!(
        receiver
            .mirrors
            .projection
            .service
            .store()
            .blobs()
            .get_bytes(hash)
            .await?
            .as_ref(),
        bytes
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn network_replay_admits_before_ack_and_reopens_pending_removal() -> Res<()> {
    let source_directory = tempfile::tempdir()?;
    let receiver_directory = tempfile::tempdir()?;
    let source = Harness::reopen(source_directory.path()).await?;
    let receiver = Harness::reopen(receiver_directory.path()).await?;
    let source_peer = PeerKey::new(*source.endpoint.id().as_bytes());
    let receiver_peer = PeerKey::new(*receiver.endpoint.id().as_bytes());
    assert_ne!(source_peer, receiver_peer);
    let inventory = receiver.inventory().await?;
    let part = crate::workers::inventory_part(&inventory);
    let route = ReceiveRoute {
        backend: "blob-inventory".into(),
        target: ReplaySubscriptionTarget::Part {
            part_id: part.clone(),
        },
    };
    let bytes = vec![117; 200_000];
    let hash = iroh_blobs::Hash::new(&bytes);
    let digest = BlobId::new(*hash.as_bytes());
    let payload = serde_json::json!({"lengthOctets": bytes.len()});
    let source_guard = source
        .mirrors
        .projection
        .service
        .store()
        .blobs()
        .add_bytes(bytes.clone())
        .temp_tag()
        .await?;
    let source_parts = Arc::clone(&source.mirrors.projection.parts);
    source_parts.ensure_part(part.clone()).await?;
    source_parts
        .set_part_members(part.clone(), HashMap::from([(receiver_peer, Access::Read)]))
        .await?;
    source_parts
        .set_obj_payload(digest.into(), payload.clone())
        .await?;
    source_parts
        .add_obj_to_parts(digest.into(), vec![part.clone()])
        .await?;
    let added_revision = source_parts.latest_revision().await?;
    assert!(added_revision > 0);
    let (source_worker, source_worker_stop) = big_sync::spawn_big_sync_worker(
        Arc::clone(&source_parts),
        HashMap::from([(
            Arc::from("blob-inventory"),
            Arc::new(source.backend.clone()) as Arc<dyn SyncBackend>,
        )]),
        "blob-replay-source",
        Arc::from("inventory-transfers"),
    )?;
    let (rpc, rpc_stop) = spawn_big_sync_rpc(HashMap::from([(
        Arc::from("inventory-transfers"),
        Arc::clone(&source_parts),
    )]))
    .await?;
    let router = Router::builder(source.endpoint.clone())
        .accept(BIG_SYNC_RPC_ALPN, rpc.protocol_handler())
        .accept(
            iroh_blobs::ALPN,
            ServiceOwnedBlobs(iroh_blobs::BlobsProtocol::new(
                source.mirrors.projection.service.store(),
                None,
            )),
        )
        .spawn();
    prepare_receiver(&receiver, &inventory, digest, bytes.len() as u64).await?;
    receiver
        .backend
        .register_peer_addr(source_peer.clone(), router.endpoint().addr());
    let (admitted, mut deliveries) = mpsc::unbounded_channel();
    let (content, mut content_calls) = mpsc::unbounded_channel();
    let (worker, worker_stop) = big_sync::spawn_big_sync_worker(
        Arc::clone(&receiver.mirrors.projection.parts),
        backends(&receiver, admitted.clone(), content.clone()),
        "blob-replay-receiver",
        Arc::from("inventory-transfers"),
    )?;
    worker
        .set_peer(
            source_peer.clone(),
            Arc::new(BigSyncRpcClient::over_iroh(
                receiver.endpoint.clone(),
                router.endpoint().addr(),
            )),
            HashMap::from([(part.clone(), Arc::from("blob-inventory"))]),
            HashMap::new(),
        )
        .await?;
    let (added, release_added) = deliveries.recv().await.unwrap();
    assert_eq!(added.replay.peer_id, source_peer);
    assert_pending(&receiver, &added, &route, 0, added_revision).await?;
    assert_eq!(
        added.replay.page.events,
        vec![PartEvent::Changed(ObjChanged {
            cursor: added_revision,
            part_ids: vec![part.clone()],
            obj_id: digest.into(),
            payload: payload.clone(),
        })]
    );
    assert_eq!(
        source_observations(receiver_directory.path(), &inventory, digest, &source_peer).await?,
        vec![(added_revision, true, serde_json::to_string(&payload)?)]
    );
    assert!(content_calls.try_recv().is_err());
    assert_downloaded(&receiver, &inventory, &bytes).await?;
    assert_pending(&receiver, &added, &route, 0, added_revision).await?;
    release_added.send(()).unwrap();
    assert_eq!(
        content_calls.recv().await,
        Some((source_peer.clone(), digest.into()))
    );
    assert_eq!(
        receiver
            .mirrors
            .projection
            .parts
            .receive_journal()
            .unwrap()
            .progress(source_peer.clone(), route.clone())
            .await?,
        ReceiveProgress {
            received: added_revision,
            applied: added_revision
        }
    );
    source_parts
        .remove_obj_from_part(digest.into(), part.clone())
        .await?;
    let removed_revision = source_parts.latest_revision().await?;
    assert!(removed_revision > added_revision);
    let (removed, interrupted_release) = deliveries.recv().await.unwrap();
    assert_pending(
        &receiver,
        &removed,
        &route,
        added_revision,
        removed_revision,
    )
    .await?;
    assert_eq!(
        removed.replay.page.events,
        vec![PartEvent::Removed(ObjRemovedFromPart {
            cursor: removed_revision,
            part_id: part.clone(),
            obj_id: digest.into(),
        })]
    );
    let expected_observation = vec![(removed_revision, false, serde_json::to_string(&payload)?)];
    assert_eq!(
        source_observations(receiver_directory.path(), &inventory, digest, &source_peer).await?,
        expected_observation
    );
    receiver.wait_membership(&inventory, digest, false).await?;
    worker_stop.stop().await?;
    assert!(interrupted_release.is_closed());
    drop(interrupted_release);
    drop(worker);
    assert_pending(
        &receiver,
        &removed,
        &route,
        added_revision,
        removed_revision,
    )
    .await?;
    receiver.shutdown().await?;

    let receiver = Harness::reopen(receiver_directory.path()).await?;
    assert_pending(
        &receiver,
        &removed,
        &route,
        added_revision,
        removed_revision,
    )
    .await?;
    let (worker, worker_stop) = big_sync::spawn_big_sync_worker(
        Arc::clone(&receiver.mirrors.projection.parts),
        backends(&receiver, admitted, content),
        "blob-replay-reopened",
        Arc::from("inventory-transfers"),
    )?;
    let (redelivered, release_reopened) = deliveries.recv().await.unwrap();
    assert_eq!(redelivered, removed);
    assert_pending(
        &receiver,
        &redelivered,
        &route,
        added_revision,
        removed_revision,
    )
    .await?;
    assert_eq!(
        source_observations(receiver_directory.path(), &inventory, digest, &source_peer).await?,
        expected_observation
    );
    release_reopened.send(()).unwrap();
    worker
        .set_peer(
            source_peer.clone(),
            Arc::new(BigSyncRpcClient::over_iroh(
                receiver.endpoint.clone(),
                router.endpoint().addr(),
            )),
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
    let journal = receiver.mirrors.projection.parts.receive_journal().unwrap();
    assert!(journal.pending().await?.is_none());
    assert_eq!(
        journal.progress(source_peer.clone(), route).await?,
        ReceiveProgress {
            received: removed_revision,
            applied: removed_revision
        }
    );
    assert!(
        !receiver
            .mirrors
            .admission
            .state(&inventory, digest)
            .await?
            .unwrap()
            .observed
    );
    receiver.wait_membership(&inventory, digest, false).await?;
    assert!(content_calls.try_recv().is_err());
    worker_stop.stop().await?;
    drop(worker);
    receiver.shutdown().await?;
    source_worker_stop.stop().await?;
    drop(source_worker);
    router.shutdown().await?;
    rpc_stop.stop().await?;
    drop(source_parts);
    drop(source_guard);
    source.shutdown().await
}
