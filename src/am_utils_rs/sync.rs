pub mod node;
pub mod peer;
pub mod protocol;
pub mod store;

pub trait PartitionAccessPolicy: Send + Sync + 'static {
    fn can_access_partition(
        &self,
        peer: &protocol::PeerKey,
        partition_id: &protocol::PartitionId,
    ) -> bool;
}

pub struct AllowAllPartitionAccessPolicy;

impl PartitionAccessPolicy for AllowAllPartitionAccessPolicy {
    fn can_access_partition(
        &self,
        _peer: &protocol::PeerKey,
        _partition_id: &protocol::PartitionId,
    ) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::interlude::*;

    use crate::sync::{node::*, peer::*, protocol::*, store::*, *};

    use crate::repo::{BigRepo, BigRepoConfig};
    use automerge::transaction::Transactable;
    use samod::DocumentId;
    use std::collections::HashSet;
    use std::collections::{BTreeMap, HashMap as StdHashMap};
    use std::str::FromStr;
    use std::sync::LazyLock;
    use tokio::sync::mpsc;

    async fn boot_big_repo(peer_seed: &str) -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string(format!("big-{peer_seed}")))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        let dir = std::env::temp_dir().join("am_utils_sync_tests");
        std::fs::create_dir_all(&dir)?;
        let sqlite_path = dir.join(format!("{}-{}.sqlite", peer_seed, uuid::Uuid::new_v4()));
        let sqlite_url = format!(
            "sqlite://{}",
            sqlite_path
                .to_str()
                .ok_or_else(|| eyre::eyre!("invalid sqlite path"))?
        );
        BigRepo::boot_with_repo(repo, BigRepoConfig::new(sqlite_url)).await
    }

    fn sync_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: LazyLock<tokio::sync::Mutex<()>> =
            LazyLock::new(|| tokio::sync::Mutex::new(()));
        &LOCK
    }

    fn empty_payload() -> serde_json::Value {
        serde_json::json!({})
    }

    #[derive(Clone, Copy)]
    enum TestAckMode {
        Auto,
        ManualDoc,
    }

    #[derive(Debug, Clone)]
    struct TestDocSyncRequest {
        doc_id: String,
        cursor: u64,
    }

    enum AckSlotState {
        Pending,
        Ready,
    }

    struct TestSamodHarness {
        doc_sync_rx: mpsc::Receiver<TestDocSyncRequest>,
        join_handle: tokio::task::JoinHandle<()>,
    }

    impl TestSamodHarness {
        async fn stop(self) {
            self.join_handle.abort();
            let _ = self.join_handle.await;
        }
    }

    fn spawn_test_samod_harness(
        src: Arc<BigRepo>,
        dst: Arc<BigRepo>,
        mut samod_rx: mpsc::Receiver<SamodSyncRequest>,
        samod_ack_tx: mpsc::Sender<SamodSyncAck>,
        ack_mode: TestAckMode,
    ) -> TestSamodHarness {
        let (doc_sync_tx, doc_sync_rx) = mpsc::channel::<TestDocSyncRequest>(512);
        let join_handle = tokio::spawn(async move {
            let mut ack_slots: StdHashMap<PartitionId, BTreeMap<u64, AckSlotState>> = default();
            'harness: while let Some(req) = samod_rx.recv().await {
                match req {
                    SamodSyncRequest::PartitionMemberEvent { event, .. } => {
                        match &event.deets {
                            PartitionMemberEventDeets::MemberUpsert { item_id, payload } => {
                                let payload = serde_json::from_str::<serde_json::Value>(payload)
                                    .expect("member upsert payload should be valid json");
                                dst.partition_store()
                                    .add_member(&event.partition_id, item_id, &payload)
                                    .await
                                    .unwrap();
                            }
                            PartitionMemberEventDeets::MemberRemoved { item_id, payload } => {
                                let payload = serde_json::from_str::<serde_json::Value>(payload)
                                    .expect("member removed payload should be valid json");
                                dst.partition_store()
                                    .remove_member(&event.partition_id, item_id, &payload)
                                    .await
                                    .unwrap();
                            }
                        }
                        if samod_ack_tx
                            .send(SamodSyncAck::MemberCursorAdvanced {
                                partition_id: event.partition_id,
                                cursor: event.cursor,
                            })
                            .await
                            .is_err()
                        {
                            break 'harness;
                        }
                    }
                    SamodSyncRequest::PartitionDocEvent { event, .. } => {
                        let resolved;
                        match &event.deets {
                            PartitionDocEventDeets::ItemChanged { item_id, .. } => {
                                let parsed = match DocumentId::from_str(item_id) {
                                    Ok(parsed) => parsed,
                                    Err(_) => {
                                        let slots = ack_slots
                                            .entry(event.partition_id.clone())
                                            .or_default();
                                        slots.entry(event.cursor).or_insert(AckSlotState::Pending);
                                        continue;
                                    }
                                };
                                let local_has = dst.local_contains_document(&parsed).await.unwrap();
                                if local_has {
                                    if doc_sync_tx
                                        .send(TestDocSyncRequest {
                                            doc_id: item_id.clone(),
                                            cursor: event.cursor,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        break 'harness;
                                    }
                                    resolved = matches!(ack_mode, TestAckMode::Auto);
                                } else {
                                    let exported =
                                        src.samod_repo().local_export(parsed.clone()).await;
                                    resolved = match exported {
                                        Ok(exported) => {
                                            dst.import_doc(parsed, exported).await.unwrap();
                                            true
                                        }
                                        Err(_) => false,
                                    };
                                }
                            }
                            PartitionDocEventDeets::ItemDeleted { item_id, .. } => {
                                let _ = dst
                                    .partition_store()
                                    .remove_member(&event.partition_id, item_id, &empty_payload())
                                    .await;
                                resolved = true;
                            }
                        }
                        let slots = ack_slots.entry(event.partition_id.clone()).or_default();
                        match slots.entry(event.cursor) {
                            std::collections::btree_map::Entry::Vacant(v) => {
                                v.insert(if resolved {
                                    AckSlotState::Ready
                                } else {
                                    AckSlotState::Pending
                                });
                            }
                            std::collections::btree_map::Entry::Occupied(mut o) => {
                                if resolved {
                                    o.insert(AckSlotState::Ready);
                                }
                            }
                        }
                        loop {
                            let Some((&cursor, state)) = slots.first_key_value() else {
                                break;
                            };
                            if !matches!(state, AckSlotState::Ready) {
                                break;
                            }
                            slots.pop_first();
                            if samod_ack_tx
                                .send(SamodSyncAck::CursorAdvanced {
                                    partition_id: event.partition_id.clone(),
                                    cursor,
                                })
                                .await
                                .is_err()
                            {
                                break 'harness;
                            }
                        }
                    }
                    SamodSyncRequest::RequestDocSync {
                        partition_id,
                        doc_id,
                        cursor,
                        ..
                    } => {
                        if doc_sync_tx
                            .send(TestDocSyncRequest {
                                doc_id: doc_id.clone(),
                                cursor,
                            })
                            .await
                            .is_err()
                        {
                            break 'harness;
                        }
                        if matches!(ack_mode, TestAckMode::Auto)
                            && samod_ack_tx
                                .send(SamodSyncAck::CursorAdvanced {
                                    partition_id,
                                    cursor,
                                })
                                .await
                                .is_err()
                        {
                            break 'harness;
                        }
                    }
                    SamodSyncRequest::ImportDoc {
                        partition_id,
                        doc_id,
                        cursor,
                        ..
                    } => {
                        let resolved = if let Ok(parsed) = DocumentId::from_str(&doc_id) {
                            match src.samod_repo().local_export(parsed.clone()).await {
                                Ok(exported) => {
                                    dst.import_doc(parsed, exported).await.unwrap();
                                    true
                                }
                                Err(_) => false,
                            }
                        } else {
                            false
                        };
                        if resolved
                            && samod_ack_tx
                                .send(SamodSyncAck::CursorAdvanced {
                                    partition_id,
                                    cursor,
                                })
                                .await
                                .is_err()
                        {
                            break 'harness;
                        }
                    }
                    SamodSyncRequest::DocDeleted {
                        partition_id,
                        doc_id,
                        cursor,
                        ..
                    } => {
                        let _ = dst
                            .partition_store()
                            .remove_member(&partition_id, &doc_id, &empty_payload())
                            .await;
                        if samod_ack_tx
                            .send(SamodSyncAck::CursorAdvanced {
                                partition_id,
                                cursor,
                            })
                            .await
                            .is_err()
                        {
                            break 'harness;
                        }
                    }
                }
            }
        });
        TestSamodHarness {
            doc_sync_rx,
            join_handle,
        }
    }

    #[tokio::test]
    async fn sync_smoke_bootstrap_and_resume() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-1".into();
        let src = boot_big_repo("src-peer").await?;
        let dst = boot_big_repo("dst-peer").await?;

        let mut source_doc_ids = Vec::new();
        for idx in 0..20_usize {
            let handle = src.create_doc(automerge::Automerge::new()).await?;
            handle
                .with_document_local(|doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "seq", idx as i64)
                        .expect("failed writing seq");
                    tx.commit();
                })
                .await?;
            source_doc_ids.push(handle.document_id().to_string());
        }
        for doc_id in source_doc_ids.iter().take(10) {
            src.partition_store()
                .add_member(&part_id, doc_id, &empty_payload())
                .await?;
        }

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-b".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx.clone(),
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let mut harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx,
            TestAckMode::ManualDoc,
        );

        wait_until(
            Duration::from_secs(5),
            || async {
                let member_count = dst.partition_member_count(&part_id).await?;
                Ok(member_count == 10)
            },
            "timed out waiting for initial member sync",
        )
        .await?;

        for doc_id in source_doc_ids.iter().take(5) {
            let parsed = DocumentId::from_str(doc_id)
                .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
            let handle = src
                .find_doc(&parsed)
                .await?
                .ok_or_eyre("source doc missing during mutation")?;
            handle
                .with_document(|doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "mutated", true)
                        .expect("failed writing mutate flag");
                    tx.commit();
                })
                .await?;
        }
        for doc_id in source_doc_ids.iter().skip(10).take(5) {
            src.partition_store()
                .add_member(&part_id, doc_id, &empty_payload())
                .await?;
        }

        wait_until(
            Duration::from_secs(5),
            || async {
                let final_count = dst.partition_member_count(&part_id).await?;
                Ok(final_count == 15)
            },
            "timed out waiting for second member sync",
        )
        .await?;

        let mutated: HashSet<String> = source_doc_ids.iter().take(5).cloned().collect();
        wait_for_doc_sync_request(
            &mut harness.doc_sync_rx,
            Duration::from_secs(5),
            |req| mutated.contains(&req.doc_id),
            "timed out waiting for samod RequestDocSync for mutated docs",
        )
        .await?;

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_bootstrap_existing_local_doc_uses_samod_request() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-existing".into();
        let src = boot_big_repo("src-existing").await?;
        let dst = boot_big_repo("dst-existing").await?;

        let handle = src.create_doc(automerge::Automerge::new()).await?;
        handle
            .with_document_local(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "seed", "v")
                    .expect("failed writing seed");
                tx.commit();
            })
            .await?;
        let doc_id = handle.document_id().to_string();
        src.partition_store()
            .add_member(&part_id, &doc_id, &empty_payload())
            .await?;

        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc(parsed.clone(), exported).await?;
        assert_eq!(
            dst.partition_member_count(&part_id).await?,
            0,
            "test precondition failed: destination membership should be empty"
        );

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-existing".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key,
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let mut harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx,
            TestAckMode::Auto,
        );

        wait_until(
            Duration::from_secs(5),
            || async {
                let member_count = dst.partition_member_count(&part_id).await?;
                Ok(member_count == 1)
            },
            "timed out waiting for membership sync",
        )
        .await?;

        wait_for_doc_sync_request(
            &mut harness.doc_sync_rx,
            Duration::from_secs(5),
            |req| req.doc_id == doc_id,
            "timed out waiting for samod RequestDocSync for existing local doc",
        )
        .await?;

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_doc_cursor_advances_only_after_samod_ack() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-ack-gated".into();
        let src = boot_big_repo("src-ack-gated").await?;
        let dst = boot_big_repo("dst-ack-gated").await?;

        let handle = src.create_doc(automerge::Automerge::new()).await?;
        handle
            .with_document_local(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "seed", 1_i64)
                    .expect("failed writing seed");
                tx.commit();
            })
            .await?;
        let doc_id = handle.document_id().to_string();
        src.partition_store()
            .add_member(&part_id, &doc_id, &empty_payload())
            .await?;

        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc(parsed.clone(), exported).await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-ack-gated".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let mut harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx.clone(),
            TestAckMode::ManualDoc,
        );

        let req = wait_for_doc_sync_request(
            &mut harness.doc_sync_rx,
            Duration::from_secs(5),
            |req| req.doc_id == doc_id,
            "timed out waiting for samod RequestDocSync for ack-gated cursor test",
        )
        .await?;

        let req_cursor = req.cursor;

        let cursor_before_ack = dst_store
            .get_partition_cursor(peer_key.clone(), part_id.clone())
            .await?;
        assert!(
            cursor_before_ack.doc_cursor.is_none(),
            "doc cursor advanced before ack: {cursor_before_ack:?}"
        );

        samod_ack_tx
            .send(SamodSyncAck::DocSynced {
                partition_id: part_id.clone(),
                doc_id: doc_id.clone(),
                cursor: req_cursor,
            })
            .await
            .map_err(|err| ferr!("failed sending samod ack in test: {err}"))?;

        wait_until(
            Duration::from_secs(5),
            || async {
                let cursor = dst_store
                    .get_partition_cursor(peer_key.clone(), part_id.clone())
                    .await?;
                Ok(cursor.doc_cursor == Some(req_cursor))
            },
            "timed out waiting for doc cursor to advance after samod ack",
        )
        .await?;

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_ignores_stale_samod_ack_behind_persisted_doc_cursor() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-stale-ack".into();
        let src = boot_big_repo("src-stale-ack").await?;
        let dst = boot_big_repo("dst-stale-ack").await?;

        let handle = src.create_doc(automerge::Automerge::new()).await?;
        handle
            .with_document_local(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "seed", 1_i64)
                    .expect("failed writing seed");
                tx.commit();
            })
            .await?;
        let doc_id = handle.document_id().to_string();
        src.partition_store()
            .add_member(&part_id, &doc_id, &empty_payload())
            .await?;

        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc(parsed.clone(), exported).await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-stale-ack".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let mut harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx.clone(),
            TestAckMode::ManualDoc,
        );

        let req = wait_for_doc_sync_request(
            &mut harness.doc_sync_rx,
            Duration::from_secs(5),
            |req| req.doc_id == doc_id,
            "timed out waiting for samod RequestDocSync for stale-ack test",
        )
        .await?;
        let req_cursor = req.cursor;

        wait_until(
            Duration::from_secs(5),
            || async {
                let cursor = dst_store
                    .get_partition_cursor(peer_key.clone(), part_id.clone())
                    .await?;
                Ok(cursor.member_cursor.is_some())
            },
            "timed out waiting for member cursor before stale ack check",
        )
        .await?;

        dst_store
            .set_partition_cursor(
                peer_key.clone(),
                part_id.clone(),
                Some(req_cursor),
                Some(req_cursor + 10),
            )
            .await?;

        samod_ack_tx
            .send(SamodSyncAck::DocSynced {
                partition_id: part_id.clone(),
                doc_id: doc_id.clone(),
                cursor: req_cursor,
            })
            .await
            .map_err(|err| ferr!("failed sending stale samod ack in test: {err}"))?;

        tokio::time::sleep(Duration::from_millis(250)).await;
        let cursor = dst_store
            .get_partition_cursor(peer_key.clone(), part_id.clone())
            .await?;
        assert_eq!(
            cursor.doc_cursor,
            Some(req_cursor + 10),
            "stale ack must not regress persisted doc cursor"
        );

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_resets_stale_remote_peer_cursor_before_replay() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-reset-stale".into();
        let src = boot_big_repo("src-reset-stale").await?;
        let dst = boot_big_repo("dst-reset-stale").await?;

        let handle = src.create_doc(automerge::Automerge::new()).await?;
        handle
            .with_document_local(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "seed", 1_i64)
                    .expect("failed writing seed");
                tx.commit();
            })
            .await?;
        let doc_id = handle.document_id().to_string();
        src.partition_store()
            .add_member(&part_id, &doc_id, &empty_payload())
            .await?;
        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc(parsed, exported).await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-reset-stale".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        dst_store
            .set_partition_cursor(peer_key.clone(), part_id.clone(), Some(208), Some(208))
            .await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let mut harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx,
            TestAckMode::ManualDoc,
        );

        wait_until(
            Duration::from_secs(5),
            || async {
                let member_count = dst.partition_member_count(&part_id).await?;
                Ok(member_count == 1)
            },
            "timed out waiting for membership replay after stale cursor reset",
        )
        .await?;

        let req = wait_for_doc_sync_request(
            &mut harness.doc_sync_rx,
            Duration::from_secs(5),
            |req| req.doc_id == doc_id,
            "timed out waiting for doc replay after stale cursor reset",
        )
        .await?;

        let req_cursor = req.cursor;
        assert!(
            req_cursor < 208,
            "stale stored cursor was not invalidated before replay: req_cursor={req_cursor}"
        );

        wait_until(
            Duration::from_secs(5),
            || async {
                let cursor = dst_store
                    .get_partition_cursor(peer_key.clone(), part_id.clone())
                    .await?;
                Ok(cursor.member_cursor.is_some_and(|value| value < 208))
            },
            "timed out waiting for member cursor replay after stale reset",
        )
        .await?;

        let cursor = dst_store
            .get_partition_cursor(peer_key.clone(), part_id.clone())
            .await?;
        assert!(
            cursor.member_cursor.is_some_and(|value| value < 208),
            "member cursor should have been reset and replayed from remote frontier: {cursor:?}"
        );
        assert!(
            cursor.doc_cursor.is_none(),
            "doc cursor should remain ack-gated for existing local docs after stale reset replay: {cursor:?}"
        );

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_bootstrap_does_not_advance_doc_cursor_when_docs_unresolved() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-unresolved".into();
        let src = boot_big_repo("src-unresolved").await?;
        let dst = boot_big_repo("dst-unresolved").await?;

        sqlx::query(
            r#"
            INSERT INTO partition_item_state(partition_id, item_id, deleted, item_payload_json, latest_txid)
            VALUES(?, ?, 0, '{}', 1)
            ON CONFLICT(partition_id, item_id) DO UPDATE SET
                deleted = excluded.deleted,
                item_payload_json = excluded.item_payload_json,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(&part_id)
        .bind("doc_missing_remote_payload")
        .execute(src.state_pool())
        .await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-unresolved".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx,
            TestAckMode::Auto,
        );

        tokio::time::sleep(Duration::from_millis(700)).await;
        let cursor = dst_store
            .get_partition_cursor(peer_key.clone(), part_id.clone())
            .await?;
        assert!(
            cursor.doc_cursor.is_none(),
            "doc cursor must not advance when bootstrap page has unresolved docs: {cursor:?}"
        );

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_does_not_skip_failed_doc_cursor_when_later_doc_import_succeeds() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-blocked-cursor".into();
        let src = boot_big_repo("src-blocked-cursor").await?;
        let dst = boot_big_repo("dst-blocked-cursor").await?;

        sqlx::query(
            r#"
            INSERT INTO partition_item_state(partition_id, item_id, deleted, item_payload_json, latest_txid)
            VALUES(?, ?, 0, '{}', 1)
            ON CONFLICT(partition_id, item_id) DO UPDATE SET
                deleted = excluded.deleted,
                item_payload_json = excluded.item_payload_json,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(&part_id)
        .bind("definitely-not-a-samod-doc-id")
        .execute(src.state_pool())
        .await?;

        let handle = src.create_doc(automerge::Automerge::new()).await?;
        handle
            .with_document_local(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "seed", 9_i64)
                    .expect("failed writing seed");
                tx.commit();
            })
            .await?;
        let valid_doc_id = handle.document_id().to_string();
        src.partition_store()
            .add_member(&part_id, &valid_doc_id, &empty_payload())
            .await?;

        let valid_doc_id_parsed = DocumentId::from_str(&valid_doc_id)
            .map_err(|err| ferr!("invalid source doc id {valid_doc_id}: {err}"))?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-blocked-cursor".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx,
            TestAckMode::Auto,
        );

        wait_until(
            Duration::from_secs(5),
            || async { dst.local_contains_document(&valid_doc_id_parsed).await },
            "timed out waiting for later valid document import",
        )
        .await?;

        let cursor = dst_store
            .get_partition_cursor(peer_key.clone(), part_id.clone())
            .await?;
        assert!(
            cursor.doc_cursor.is_none(),
            "doc cursor must remain blocked when an earlier cursor failed: {cursor:?}"
        );

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_unresolved_partition_does_not_block_other_partition_cursor() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let p_bad: PartitionId = "sync-part-bad".into();
        let p_ok: PartitionId = "sync-part-ok".into();
        let src = boot_big_repo("src-partition-isolation").await?;
        let dst = boot_big_repo("dst-partition-isolation").await?;

        sqlx::query(
            r#"
            INSERT INTO partition_item_state(partition_id, item_id, deleted, item_payload_json, latest_txid)
            VALUES(?, ?, 0, '{}', 1)
            ON CONFLICT(partition_id, item_id) DO UPDATE SET
                deleted = excluded.deleted,
                item_payload_json = excluded.item_payload_json,
                latest_txid = excluded.latest_txid
            "#,
        )
        .bind(&p_bad)
        .bind("doc_missing_remote_payload")
        .execute(src.state_pool())
        .await?;

        let ok_handle = src.create_doc(automerge::Automerge::new()).await?;
        ok_handle
            .with_document_local(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "ok", true)
                    .expect("failed writing ok");
                tx.commit();
            })
            .await?;
        src.partition_store()
            .add_member(
                &p_ok,
                &ok_handle.document_id().to_string(),
                &empty_payload(),
            )
            .await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            src.partition_store(),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc, repo_rpc_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-partition-isolation".into();
        src_store.allow_peer(peer_key.clone(), None).await?;

        let (samod_tx, samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![p_bad.clone(), p_ok.clone()],
        })
        .await?;
        let harness = spawn_test_samod_harness(
            Arc::clone(&src),
            Arc::clone(&dst),
            samod_rx,
            samod_ack_tx,
            TestAckMode::Auto,
        );

        wait_until(
            Duration::from_secs(5),
            || async {
                let count = dst.partition_member_count(&p_ok).await?;
                let ok_cursor = dst_store
                    .get_partition_cursor(peer_key.clone(), p_ok.clone())
                    .await?;
                Ok(count == 1 && ok_cursor.doc_cursor.is_some())
            },
            "timed out waiting for healthy partition membership sync",
        )
        .await?;

        let bad_cursor = dst_store
            .get_partition_cursor(peer_key.clone(), p_bad.clone())
            .await?;
        let ok_cursor = dst_store
            .get_partition_cursor(peer_key.clone(), p_ok.clone())
            .await?;
        assert!(
            bad_cursor.doc_cursor.is_none(),
            "expected unresolved partition doc cursor to remain unset: {bad_cursor:?}"
        );
        assert!(
            ok_cursor.doc_cursor.is_some(),
            "expected healthy partition doc cursor to advance: {ok_cursor:?}"
        );

        harness.stop().await;
        worker_stop.stop().await?;
        repo_rpc_stop.stop().await?;
        node_stop.stop().await?;
        dst_store_stop.stop().await?;
        src_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_role_reversal_after_serve_only_does_not_skip_new_docs() -> Res<()> {
        let _guard = sync_test_lock().lock().await;
        let part_id: PartitionId = "sync-part-role-reversal".into();
        let repo_a = boot_big_repo("role-a").await?;
        let repo_b = boot_big_repo("role-b").await?;

        let mut a_doc_ids = Vec::new();
        for idx in 0..6_usize {
            let handle = repo_a.create_doc(automerge::Automerge::new()).await?;
            handle
                .with_document_local(|doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "seed_a", idx as i64)
                        .expect("failed writing seed_a");
                    tx.commit();
                })
                .await?;
            let doc_id = handle.document_id().to_string();
            repo_a
                .partition_store()
                .add_member(&part_id, &doc_id, &empty_payload())
                .await?;
            a_doc_ids.push(doc_id);
        }

        let (store_a, store_a_stop) = spawn_sync_store(repo_a.state_pool().clone()).await?;
        let (store_b, store_b_stop) = spawn_sync_store(repo_b.state_pool().clone()).await?;
        let (node_a, node_a_stop) = spawn_sync_node(
            repo_a.partition_store(),
            store_a.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (node_b, node_b_stop) = spawn_sync_node(
            repo_b.partition_store(),
            store_b.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc_a, repo_rpc_a_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&repo_a),
            store_a.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (_repo_rpc_b, repo_rpc_b_stop) = crate::repo::rpc::spawn_repo_rpc(
            Arc::clone(&repo_b),
            store_b.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;

        // Phase 1: B pulls from A. A only serves.
        let peer_a: PeerKey = "peer-a".into();
        let peer_b: PeerKey = "peer-b".into();
        store_a.allow_peer(peer_b.clone(), None).await?;

        let (samod_tx_b, samod_rx_b) = mpsc::channel(128);
        let (samod_ack_tx_b, samod_ack_rx_b) = mpsc::channel(128);
        let (_worker_b, worker_b_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_b.clone(),
            remote_peer: peer_a.clone(),
            rpc_client: node_a.rpc_client(),
            sync_store: store_b.clone(),
            samod_sync_tx: samod_tx_b,
            samod_ack_rx: samod_ack_rx_b,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let harness_b = spawn_test_samod_harness(
            Arc::clone(&repo_a),
            Arc::clone(&repo_b),
            samod_rx_b,
            samod_ack_tx_b,
            TestAckMode::Auto,
        );
        wait_until(
            Duration::from_secs(5),
            || async {
                let count = repo_b.partition_member_count(&part_id).await?;
                Ok(count == a_doc_ids.len() as i64)
            },
            "timed out waiting for phase-1 B<-A membership sync",
        )
        .await?;
        harness_b.stop().await;
        worker_b_stop.stop().await?;

        // Add new docs only on B after phase 1.
        let mut b_new_doc_ids = Vec::new();
        for idx in 0..4_usize {
            let handle = repo_b.create_doc(automerge::Automerge::new()).await?;
            handle
                .with_document_local(|doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "seed_b", idx as i64)
                        .expect("failed writing seed_b");
                    tx.commit();
                })
                .await?;
            let doc_id = handle.document_id().to_string();
            repo_b
                .partition_store()
                .add_member(&part_id, &doc_id, &empty_payload())
                .await?;
            b_new_doc_ids.push(doc_id);
        }

        // Phase 2: roles reversed, A pulls from B.
        store_b.allow_peer(peer_a.clone(), None).await?;
        let (samod_tx_a, samod_rx_a) = mpsc::channel(128);
        let (samod_ack_tx_a, samod_ack_rx_a) = mpsc::channel(128);
        let (_worker_a, worker_a_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_a.clone(),
            remote_peer: peer_b.clone(),
            rpc_client: node_b.rpc_client(),
            sync_store: store_a.clone(),
            samod_sync_tx: samod_tx_a,
            samod_ack_rx: samod_ack_rx_a,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        let harness_a = spawn_test_samod_harness(
            Arc::clone(&repo_b),
            Arc::clone(&repo_a),
            samod_rx_a,
            samod_ack_tx_a,
            TestAckMode::Auto,
        );

        wait_until(
            Duration::from_secs(5),
            || async {
                let count = repo_a.partition_member_count(&part_id).await?;
                Ok(count == (a_doc_ids.len() + b_new_doc_ids.len()) as i64)
            },
            "timed out waiting for phase-2 A<-B membership sync",
        )
        .await?;

        // Ensure role reversal imported B's new docs into A.
        for doc_id in &b_new_doc_ids {
            let parsed = DocumentId::from_str(doc_id)
                .map_err(|err| ferr!("invalid doc id '{doc_id}' in role reversal test: {err}"))?;
            wait_until(
                Duration::from_secs(5),
                || async { repo_a.local_contains_document(&parsed).await },
                &format!("repo_a missing role-reversed doc {doc_id}"),
            )
            .await?;
        }

        harness_a.stop().await;
        worker_a_stop.stop().await?;
        repo_rpc_b_stop.stop().await?;
        repo_rpc_a_stop.stop().await?;
        node_b_stop.stop().await?;
        node_a_stop.stop().await?;
        store_b_stop.stop().await?;
        store_a_stop.stop().await?;
        Ok(())
    }

    async fn wait_until<F, Fut>(
        timeout_dur: Duration,
        mut condition: F,
        timeout_msg: &str,
    ) -> Res<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Res<bool>>,
    {
        let deadline = tokio::time::Instant::now() + timeout_dur;
        loop {
            if condition().await? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                eyre::bail!("{timeout_msg}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_doc_sync_request<F>(
        rx: &mut mpsc::Receiver<TestDocSyncRequest>,
        timeout_dur: Duration,
        mut pred: F,
        timeout_msg: &str,
    ) -> Res<TestDocSyncRequest>
    where
        F: FnMut(&TestDocSyncRequest) -> bool,
    {
        let deadline = tokio::time::Instant::now() + timeout_dur;
        loop {
            while let Ok(req) = rx.try_recv() {
                if pred(&req) {
                    return Ok(req);
                }
            }
            if tokio::time::Instant::now() >= deadline {
                eyre::bail!("{timeout_msg}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
