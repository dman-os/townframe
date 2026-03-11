mod node;
mod peer;
mod protocol;
mod store;

pub use node::{spawn_sync_node, SyncNodeHandle, SyncNodeStopToken};
pub use peer::{
    spawn_peer_sync_worker, PeerSyncProgressEvent, PeerSyncWorkerEvent, PeerSyncWorkerHandle,
    PeerSyncWorkerStopToken, SamodSyncAck, SamodSyncRequest, SpawnPeerSyncWorkerArgs,
};
pub use protocol::{
    CursorIndex, FullDoc, GetDocsFullRequest, GetDocsFullResponse, GetPartitionDocEventsRequest,
    GetPartitionDocEventsResponse, GetPartitionMemberEventsRequest,
    GetPartitionMemberEventsResponse, ListPartitionsRequest, ListPartitionsResponse,
    PartitionCursorPage, PartitionCursorRequest, PartitionDocEvent, PartitionDocEventDeets,
    PartitionEvent, PartitionEventDeets, PartitionId, PartitionMemberEvent,
    PartitionMemberEventDeets, PartitionStreamCursorRequest, PartitionSummary, PartitionSyncError,
    PartitionSyncRpc, PeerKey, SubPartitionsRequest, SubscriptionItem, SubscriptionStreamKind,
    DEFAULT_DOC_BATCH_LIMIT, DEFAULT_EVENT_PAGE_LIMIT, DEFAULT_SUBSCRIPTION_CAPACITY,
    MAX_GET_DOCS_FULL_DOC_IDS,
};
pub use store::{spawn_sync_store, SyncStoreHandle, SyncStoreStopToken};

pub trait PartitionAccessPolicy: Send + Sync + 'static {
    fn can_access_partition(&self, peer: &PeerKey, partition_id: &PartitionId) -> bool;
}

pub struct AllowAllPartitionAccessPolicy;

impl PartitionAccessPolicy for AllowAllPartitionAccessPolicy {
    fn can_access_partition(&self, _peer: &PeerKey, _partition_id: &PartitionId) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::interlude::*;
    use crate::repo::{BigRepo, BigRepoConfig};
    use automerge::transaction::Transactable;
    use samod::DocumentId;
    use std::collections::HashSet;
    use std::str::FromStr;
    use tokio::sync::mpsc;

    async fn boot_big_repo(peer_seed: &str) -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string(format!("big-{peer_seed}")))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        BigRepo::boot_with_repo(repo, BigRepoConfig::new("sqlite::memory:".to_string())).await
    }

    #[tokio::test]
    async fn sync_smoke_bootstrap_and_resume() -> Res<()> {
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
            src.add_doc_to_partition(&part_id, doc_id).await?;
        }

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-b".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        let (samod_tx, mut samod_rx) = mpsc::channel(128);
        let (_samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            local_repo: Arc::clone(&dst),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx.clone(),
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;

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
            src.add_doc_to_partition(&part_id, doc_id).await?;
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
        wait_for_samod_request(
            &mut samod_rx,
            Duration::from_secs(5),
            |req| match req {
                SamodSyncRequest::RequestDocSync { doc_id, .. } => mutated.contains(doc_id),
                _ => false,
            },
            "timed out waiting for samod RequestDocSync for mutated docs",
        )
        .await?;

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_bootstrap_existing_local_doc_uses_samod_request() -> Res<()> {
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
        src.add_doc_to_partition(&part_id, &doc_id).await?;

        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc_fast(parsed.clone(), exported).await?;
        assert_eq!(
            dst.partition_member_count(&part_id).await?,
            0,
            "test precondition failed: destination membership should be empty"
        );

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-existing".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        let (samod_tx, mut samod_rx) = mpsc::channel(128);
        let (_samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key,
            rpc_client: node.rpc_client(),
            local_repo: Arc::clone(&dst),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;

        wait_until(
            Duration::from_secs(5),
            || async {
                let member_count = dst.partition_member_count(&part_id).await?;
                Ok(member_count == 1)
            },
            "timed out waiting for membership sync",
        )
        .await?;

        wait_for_samod_request(
            &mut samod_rx,
            Duration::from_secs(5),
            |req| match req {
                SamodSyncRequest::RequestDocSync {
                    doc_id: req_doc_id, ..
                } => req_doc_id == &doc_id,
                _ => false,
            },
            "timed out waiting for samod RequestDocSync for existing local doc",
        )
        .await?;

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_doc_cursor_advances_only_after_samod_ack() -> Res<()> {
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
        src.add_doc_to_partition(&part_id, &doc_id).await?;

        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc_fast(parsed.clone(), exported).await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-ack-gated".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        let (samod_tx, mut samod_rx) = mpsc::channel(128);
        let (samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            local_repo: Arc::clone(&dst),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;

        let req = wait_for_samod_request(
            &mut samod_rx,
            Duration::from_secs(5),
            |req| match req {
                SamodSyncRequest::RequestDocSync {
                    doc_id: req_doc_id, ..
                } => req_doc_id == &doc_id,
                _ => false,
            },
            "timed out waiting for samod RequestDocSync for ack-gated cursor test",
        )
        .await?;

        let req_cursor = match req {
            SamodSyncRequest::RequestDocSync { cursor, .. } => cursor,
            _ => unreachable!(),
        };

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

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_resets_stale_remote_peer_cursor_before_replay() -> Res<()> {
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
        src.add_doc_to_partition(&part_id, &doc_id).await?;
        let parsed = DocumentId::from_str(&doc_id)
            .map_err(|err| ferr!("invalid source doc id {doc_id}: {err}"))?;
        let exported = src
            .samod_repo()
            .local_export(parsed.clone())
            .await
            .map_err(|err| ferr!("failed exporting source doc: {err}"))?;
        let _ = dst.import_doc_fast(parsed, exported).await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-reset-stale".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        dst_store
            .set_partition_cursor(peer_key.clone(), part_id.clone(), Some(208), Some(208))
            .await?;

        let (samod_tx, mut samod_rx) = mpsc::channel(128);
        let (_samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            local_repo: Arc::clone(&dst),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;

        wait_until(
            Duration::from_secs(5),
            || async {
                let member_count = dst.partition_member_count(&part_id).await?;
                Ok(member_count == 1)
            },
            "timed out waiting for membership replay after stale cursor reset",
        )
        .await?;

        let req = wait_for_samod_request(
            &mut samod_rx,
            Duration::from_secs(5),
            |req| match req {
                SamodSyncRequest::RequestDocSync { doc_id: req_doc_id, .. } => req_doc_id == &doc_id,
                _ => false,
            },
            "timed out waiting for doc replay after stale cursor reset",
        )
        .await?;

        let req_cursor = match req {
            SamodSyncRequest::RequestDocSync { cursor, .. } => cursor,
            _ => unreachable!(),
        };
        assert!(
            req_cursor < 208,
            "stale stored cursor was not invalidated before replay: req_cursor={req_cursor}"
        );

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

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_bootstrap_does_not_advance_doc_cursor_when_docs_unresolved() -> Res<()> {
        let part_id: PartitionId = "sync-part-unresolved".into();
        let src = boot_big_repo("src-unresolved").await?;
        let dst = boot_big_repo("dst-unresolved").await?;

        sqlx::query(
            r#"
            INSERT INTO partition_doc_state(partition_id, doc_id, deleted, latest_txid)
            VALUES(?, ?, 0, 1)
            ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                deleted = excluded.deleted,
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
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-unresolved".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        let (samod_tx, _samod_rx) = mpsc::channel(128);
        let (_samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            local_repo: Arc::clone(&dst),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![part_id.clone()],
        })
        .await?;

        tokio::time::sleep(Duration::from_millis(700)).await;
        let cursor = dst_store
            .get_partition_cursor(peer_key.clone(), part_id.clone())
            .await?;
        assert!(
            cursor.doc_cursor.is_none(),
            "doc cursor must not advance when bootstrap page has unresolved docs: {cursor:?}"
        );

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_unresolved_partition_does_not_block_other_partition_cursor() -> Res<()> {
        let p_bad: PartitionId = "sync-part-bad".into();
        let p_ok: PartitionId = "sync-part-ok".into();
        let src = boot_big_repo("src-partition-isolation").await?;
        let dst = boot_big_repo("dst-partition-isolation").await?;

        sqlx::query(
            r#"
            INSERT INTO partition_doc_state(partition_id, doc_id, deleted, latest_txid)
            VALUES(?, ?, 0, 1)
            ON CONFLICT(partition_id, doc_id) DO UPDATE SET
                deleted = excluded.deleted,
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
        src.add_doc_to_partition(&p_ok, &ok_handle.document_id().to_string())
            .await?;

        let (src_store, src_store_stop) = spawn_sync_store(src.state_pool().clone()).await?;
        let (dst_store, dst_store_stop) = spawn_sync_store(dst.state_pool().clone()).await?;
        let (node, node_stop) = spawn_sync_node(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let peer_key: PeerKey = "peer-partition-isolation".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        let (samod_tx, _samod_rx) = mpsc::channel(128);
        let (_samod_ack_tx, samod_ack_rx) = mpsc::channel(128);
        let (_worker, worker_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_key.clone(),
            remote_peer: peer_key.clone(),
            rpc_client: node.rpc_client(),
            local_repo: Arc::clone(&dst),
            sync_store: dst_store.clone(),
            samod_sync_tx: samod_tx,
            samod_ack_rx,
            target_partitions: vec![p_bad.clone(), p_ok.clone()],
        })
        .await?;

        wait_until(
            Duration::from_secs(5),
            || async {
                let count = dst.partition_member_count(&p_ok).await?;
                Ok(count == 1)
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

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_role_reversal_after_serve_only_does_not_skip_new_docs() -> Res<()> {
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
            repo_a.add_doc_to_partition(&part_id, &doc_id).await?;
            a_doc_ids.push(doc_id);
        }

        let (store_a, store_a_stop) = spawn_sync_store(repo_a.state_pool().clone()).await?;
        let (store_b, store_b_stop) = spawn_sync_store(repo_b.state_pool().clone()).await?;
        let (node_a, node_a_stop) = spawn_sync_node(
            Arc::clone(&repo_a),
            store_a.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;
        let (node_b, node_b_stop) = spawn_sync_node(
            Arc::clone(&repo_b),
            store_b.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
        )
        .await?;

        // Phase 1: B pulls from A. A only serves.
        let peer_a: PeerKey = "peer-a".into();
        let peer_b: PeerKey = "peer-b".into();
        store_a.register_peer(peer_b.clone()).await?;
        node_a.register_local_peer(peer_b.clone()).await?;

        let (samod_tx_b, _samod_rx_b) = mpsc::channel(128);
        let (_samod_ack_tx_b, samod_ack_rx_b) = mpsc::channel(128);
        let (_worker_b, worker_b_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_b.clone(),
            remote_peer: peer_a.clone(),
            rpc_client: node_a.rpc_client(),
            local_repo: Arc::clone(&repo_b),
            sync_store: store_b.clone(),
            samod_sync_tx: samod_tx_b,
            samod_ack_rx: samod_ack_rx_b,
            target_partitions: vec![part_id.clone()],
        })
        .await?;
        wait_until(
            Duration::from_secs(5),
            || async {
                let count = repo_b.partition_member_count(&part_id).await?;
                Ok(count == a_doc_ids.len() as i64)
            },
            "timed out waiting for phase-1 B<-A membership sync",
        )
        .await?;
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
            repo_b.add_doc_to_partition(&part_id, &doc_id).await?;
            b_new_doc_ids.push(doc_id);
        }

        // Phase 2: roles reversed, A pulls from B.
        store_b.register_peer(peer_a.clone()).await?;
        node_b.register_local_peer(peer_a.clone()).await?;
        let (samod_tx_a, _samod_rx_a) = mpsc::channel(128);
        let (_samod_ack_tx_a, samod_ack_rx_a) = mpsc::channel(128);
        let (_worker_a, worker_a_stop) = spawn_peer_sync_worker(SpawnPeerSyncWorkerArgs {
            local_peer: peer_a.clone(),
            remote_peer: peer_b.clone(),
            rpc_client: node_b.rpc_client(),
            local_repo: Arc::clone(&repo_a),
            sync_store: store_a.clone(),
            samod_sync_tx: samod_tx_a,
            samod_ack_rx: samod_ack_rx_a,
            target_partitions: vec![part_id.clone()],
        })
        .await?;

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
            let has = repo_a.local_contains_document(&parsed).await?;
            assert!(has, "repo_a missing role-reversed doc {doc_id}");
        }

        worker_a_stop.stop().await?;
        node_a_stop.stop().await?;
        node_b_stop.stop().await?;
        store_a_stop.stop().await?;
        store_b_stop.stop().await?;
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

    async fn wait_for_samod_request<F>(
        rx: &mut mpsc::Receiver<SamodSyncRequest>,
        timeout_dur: Duration,
        mut pred: F,
        timeout_msg: &str,
    ) -> Res<SamodSyncRequest>
    where
        F: FnMut(&SamodSyncRequest) -> bool,
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
