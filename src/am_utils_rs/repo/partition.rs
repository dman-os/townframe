use crate::interlude::*;

use std::collections::HashSet;
use std::str::FromStr;

use crate::repo::{BigRepo, DocumentId};
use crate::sync::protocol::*;

impl BigRepo {
    pub async fn partition_member_count(&self, part_id: &PartitionId) -> Res<i64> {
        self.partition_store.member_count(part_id).await
    }

    pub async fn is_member_present_in_partition_item_state(
        &self,
        partition_id: &PartitionId,
        member_id: &str,
    ) -> Res<bool> {
        self.partition_store
            .is_member_present_in_item_state(partition_id, member_id)
            .await
    }

    pub async fn list_partitions_for_peer(&self, peer: &PeerKey) -> Res<Vec<PartitionSummary>> {
        Ok(self
            .partition_store
            .list_partitions_for_peer(peer)
            .await?
            .partitions)
    }

    pub async fn get_partition_member_events_for_peer(
        &self,
        peer: &PeerKey,
        req: &GetPartitionMemberEventsRequest,
    ) -> Res<GetPartitionMemberEventsResponse> {
        self.partition_store
            .get_partition_member_events_for_peer(peer, req)
            .await
    }

    pub async fn get_partition_doc_events_for_peer(
        &self,
        peer: &PeerKey,
        req: &GetPartitionDocEventsRequest,
    ) -> Res<GetPartitionDocEventsResponse> {
        self.partition_store
            .get_partition_doc_events_for_peer(peer, req)
            .await
    }

    pub async fn subscribe_partition_events_for_peer(
        &self,
        peer: &PeerKey,
        reqs: &SubPartitionsRequest,
        capacity: usize,
    ) -> Res<tokio::sync::mpsc::Receiver<SubscriptionItem>> {
        self.partition_store
            .subscribe_partition_events_for_peer(peer, reqs, capacity)
            .await
    }

    pub async fn get_docs_full_in_partitions(
        &self,
        doc_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(PartitionSyncError::TooManyDocIds {
                requested: doc_ids.len(),
                max: MAX_GET_DOCS_FULL_DOC_IDS,
            }
            .into());
        }

        let mut dedup = HashSet::new();
        let requested_doc_ids: Vec<String> = doc_ids
            .iter()
            .filter(|doc_id| dedup.insert((*doc_id).clone()))
            .cloned()
            .collect();
        let denied_doc_id = self
            .find_first_inaccessible_doc_in_partitions(&requested_doc_ids, allowed_partitions)
            .await?;
        if let Some(denied) = denied_doc_id {
            return Err(PartitionSyncError::DocAccessDenied { doc_id: denied }.into());
        }

        use futures::StreamExt;
        use futures_buffered::BufferedStreamExt;
        let rows = futures::stream::iter(requested_doc_ids.into_iter().map(|doc_id| async move {
            let parsed = match DocumentId::from_str(&doc_id) {
                Ok(val) => val,
                Err(_) => return Ok(None),
            };
            let Some(automerge_save) = self.export_doc_save(&parsed).await? else {
                return Ok(None);
            };
            Ok(Some(FullDoc {
                doc_id,
                automerge_save,
            }))
        }))
        .buffered_unordered(16)
        .collect::<Vec<Res<Option<FullDoc>>>>()
        .await;

        let mut out = Vec::new();
        for row in rows {
            if let Some(doc) = row? {
                out.push(doc);
            }
        }
        Ok(out)
    }

    pub async fn is_doc_accessible_in_partitions(
        &self,
        doc_id: &str,
        allowed_partitions: &[PartitionId],
    ) -> Res<bool> {
        self.partition_store
            .is_item_present_in_membership_partitions(doc_id, allowed_partitions)
            .await
    }

    async fn find_first_inaccessible_doc_in_partitions(
        &self,
        doc_ids: &[String],
        allowed_partitions: &[PartitionId],
    ) -> Res<Option<String>> {
        self.partition_store
            .find_first_item_missing_membership_in_partitions(doc_ids, allowed_partitions)
            .await
    }
}
