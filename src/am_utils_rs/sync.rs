mod node;
mod peer;
mod protocol;
mod store;

pub use node::{spawn_sync_node, SyncNodeHandle, SyncNodeStopToken};
pub use peer::{
    spawn_peer_sync_worker, PeerSyncProgressEvent, PeerSyncWorkerEvent, PeerSyncWorkerHandle,
    PeerSyncWorkerStopToken, SamodSyncRequest,
};
pub use protocol::{
    cursor, FullDoc, GetDocsFullRequest, GetDocsFullResponse, GetPartitionDocEventsRequest,
    GetPartitionDocEventsResponse, GetPartitionMemberEventsRequest,
    GetPartitionMemberEventsResponse, ListPartitionsRequest, ListPartitionsResponse, OpaqueCursor,
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
    use std::str::FromStr;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

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

        let cancel = CancellationToken::new();
        let (src_store, src_store_stop) =
            spawn_sync_store(src.state_pool().clone(), cancel.child_token()).await?;
        let (dst_store, dst_store_stop) =
            spawn_sync_store(dst.state_pool().clone(), cancel.child_token()).await?;
        let (node, node_stop) = spawn_sync_node(
            Arc::clone(&src),
            src_store.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
            cancel.child_token(),
        )
        .await?;
        let peer_key: PeerKey = "peer-b".into();
        src_store.register_peer(peer_key.clone()).await?;
        node.register_local_peer(peer_key.clone()).await?;

        let (samod_tx, mut samod_rx) = mpsc::channel(128);
        let (worker, worker_stop) = spawn_peer_sync_worker(
            peer_key.clone(),
            node.rpc_client(),
            Arc::clone(&dst),
            dst_store.clone(),
            samod_tx.clone(),
            vec![part_id.clone()],
            cancel.child_token(),
        )
        .await?;
        timeout(Duration::from_secs(5), worker.start())
            .await
            .map_err(|_| ferr!("peer worker start timed out (phase 1)"))??;

        let member_count = dst.partition_member_count(&part_id).await?;
        assert_eq!(member_count, 10);

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

        timeout(Duration::from_secs(5), worker.start())
            .await
            .map_err(|_| ferr!("peer worker start timed out (phase 2)"))??;
        let final_count = dst.partition_member_count(&part_id).await?;
        assert_eq!(final_count, 15);

        let mut saw_doc_sync_request = false;
        while let Ok(req) = samod_rx.try_recv() {
            if matches!(req, SamodSyncRequest::RequestDocSync { .. }) {
                saw_doc_sync_request = true;
                break;
            }
        }
        assert!(saw_doc_sync_request, "expected at least one samod sync request");

        worker_stop.stop().await?;
        node_stop.stop().await?;
        src_store_stop.stop().await?;
        dst_store_stop.stop().await?;
        Ok(())
    }
}
