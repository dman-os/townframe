use crate::interlude::*;

use std::collections::HashSet;
use std::str::FromStr;

use crate::repo::BigRepo;
use crate::sync::protocol::*;

impl BigRepo {
    pub(super) async fn record_doc_heads_change(
        &self,
        doc_id: &samod::DocumentId,
        heads: Vec<automerge::ChangeHash>,
    ) -> Res<()> {
        let item_payload = serde_json::json!({
            "heads": crate::serialize_commit_heads(&heads),
            "change_count_hint": 1_u64,
        });
        self.partition_store
            .record_member_item_change(&doc_id.to_string(), &item_payload)
            .await
    }

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
            let parsed = match samod::DocumentId::from_str(&doc_id) {
                Ok(val) => val,
                Err(_) => return Ok(None),
            };
            let doc = match self.repo.local_export(parsed).await {
                Ok(doc) => doc,
                Err(samod::LocalExportError::NotFound { .. }) => return Ok(None),
                Err(err) => {
                    return Err(eyre::Report::from(err).wrap_err("failed local-exporting doc"));
                }
            };
            Ok(Some(FullDoc {
                doc_id,
                automerge_save: doc.save(),
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::repo::{BigRepo, BigRepoConfig};
    use crate::sync::protocol::{
        GetPartitionDocEventsRequest, GetPartitionMemberEventsRequest, PartitionCursorRequest,
    };
    use automerge::transaction::Transactable;

    async fn boot_big_repo() -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string("bigrepo-test-peer".to_string()))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        BigRepo::boot_with_repo(repo, BigRepoConfig::new("sqlite::memory:".to_string())).await
    }

    fn empty_payload() -> serde_json::Value {
        serde_json::json!({})
    }

    #[tokio::test]
    async fn bigrepo_emits_partition_doc_events_on_doc_write() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
        let doc_id = handle.document_id().to_string();
        let partition_id = "p-main".into();

        big_repo
            .partition_store()
            .add_member(&partition_id, &doc_id, &empty_payload())
            .await?;
        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "k", "v")
                    .expect("failed setting test key");
                tx.commit();
            })
            .await?;

        let events = big_repo
            .get_partition_doc_events_for_peer(
                &"peer-a".into(),
                &GetPartitionDocEventsRequest {
                    partitions: vec![PartitionCursorRequest {
                        partition_id: partition_id.clone(),
                        since: None,
                    }],
                    limit: 1024,
                },
            )
            .await?;

        assert!(events
            .events
            .iter()
            .any(|evt| matches!(evt.deets, PartitionDocEventDeets::ItemChanged { .. })));
        assert!(events
            .cursors
            .iter()
            .any(|page| page.partition_id == partition_id));
        Ok(())
    }

    #[tokio::test]
    async fn bigrepo_member_snapshot_excludes_removed_docs() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
        let target_doc_id = handle.document_id().to_string();
        let partition_id = "p-remove".into();
        big_repo
            .partition_store()
            .add_member(&partition_id, &target_doc_id, &empty_payload())
            .await?;
        handle
            .with_document(|doc| {
                let mut tx = doc.transaction();
                tx.put(automerge::ROOT, "v", 1_i64)
                    .expect("failed setting test key");
                tx.commit();
            })
            .await?;
        big_repo
            .partition_store()
            .remove_member(&partition_id, &target_doc_id, &empty_payload())
            .await?;

        let snapshot = big_repo
            .get_partition_member_events_for_peer(
                &"peer-a".into(),
                &GetPartitionMemberEventsRequest {
                    partitions: vec![PartitionCursorRequest {
                        partition_id,
                        since: None,
                    }],
                    limit: 1024,
                },
            )
            .await?;
        assert!(
            !snapshot.events.iter().any(|event| {
                matches!(
                    event.deets,
                    PartitionMemberEventDeets::MemberUpsert { ref item_id, .. } if item_id == &target_doc_id
                )
            }),
            "removed doc should not remain in snapshot membership"
        );
        Ok(())
    }

    #[tokio::test]
    async fn remove_doc_without_doc_version_state_tombstones_partition_doc_state() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let partition_id = "p-remove-no-doc-version".to_string();
        let unknown_doc_id = "doc-no-version-state".to_string();

        big_repo
            .partition_store()
            .add_member(&partition_id, &unknown_doc_id, &empty_payload())
            .await?;
        assert!(
            big_repo
                .is_member_present_in_partition_item_state(&partition_id, &unknown_doc_id)
                .await?,
            "doc should be present in partition_doc_state after add"
        );

        big_repo
            .partition_store()
            .remove_member(&partition_id, &unknown_doc_id, &empty_payload())
            .await?;
        assert!(
            !big_repo
                .is_member_present_in_partition_item_state(&partition_id, &unknown_doc_id)
                .await?,
            "doc should be tombstoned in partition_doc_state even when doc_version_state is absent"
        );

        Ok(())
    }

    #[tokio::test]
    async fn bigrepo_member_snapshot_paginates_all_docs() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let partition_id: PartitionId = "p-snapshot-members".into();
        let mut expected = std::collections::HashSet::new();
        for _ in 0..7 {
            let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
            let doc_id = handle.document_id().to_string();
            big_repo
                .partition_store()
                .add_member(&partition_id, &doc_id, &empty_payload())
                .await?;
            expected.insert(doc_id);
        }

        let mut since = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = big_repo
                .get_partition_member_events_for_peer(
                    &"peer-a".into(),
                    &GetPartitionMemberEventsRequest {
                        partitions: vec![PartitionCursorRequest {
                            partition_id: partition_id.clone(),
                            since,
                        }],
                        limit: 3,
                    },
                )
                .await?;
            for evt in &page.events {
                if let PartitionMemberEventDeets::MemberUpsert { item_id, .. } = &evt.deets {
                    seen.insert(item_id.clone());
                }
            }
            let cursor = page
                .cursors
                .iter()
                .find(|item| item.partition_id == partition_id)
                .expect(ERROR_IMPOSSIBLE);
            since = cursor.next_cursor;
            if !cursor.has_more {
                break;
            }
        }

        assert_eq!(seen, expected, "snapshot paging dropped member docs");
        Ok(())
    }

    #[tokio::test]
    async fn bigrepo_doc_snapshot_paginates_all_docs() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let partition_id: PartitionId = "p-snapshot-docs".into();
        let mut expected = std::collections::HashSet::new();
        for i in 0..7_u64 {
            let handle = big_repo.create_doc(automerge::Automerge::new()).await?;
            handle
                .with_document(move |doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "idx", i)
                        .expect("failed setting test key");
                    tx.commit();
                })
                .await?;
            let doc_id = handle.document_id().to_string();
            big_repo
                .partition_store()
                .add_member(&partition_id, &doc_id, &empty_payload())
                .await?;
            expected.insert(doc_id);
        }

        let mut since = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = big_repo
                .get_partition_doc_events_for_peer(
                    &"peer-a".into(),
                    &GetPartitionDocEventsRequest {
                        partitions: vec![PartitionCursorRequest {
                            partition_id: partition_id.clone(),
                            since,
                        }],
                        limit: 3,
                    },
                )
                .await?;
            for evt in &page.events {
                if let PartitionDocEventDeets::ItemChanged { item_id, .. } = &evt.deets {
                    seen.insert(item_id.clone());
                }
            }
            let cursor = page
                .cursors
                .iter()
                .find(|item| item.partition_id == partition_id)
                .expect(ERROR_IMPOSSIBLE);
            since = cursor.next_cursor;
            if !cursor.has_more {
                break;
            }
        }

        assert_eq!(seen, expected, "snapshot paging dropped doc events");
        Ok(())
    }

    #[tokio::test]
    async fn get_docs_full_respects_allowed_partitions() -> Res<()> {
        let big_repo = boot_big_repo().await?;
        let _partition_events_rx = big_repo.subscribe_partition_events();
        let p1: PartitionId = "p-allowed".into();
        let p2: PartitionId = "p-denied".into();

        let d1 = big_repo.create_doc(automerge::Automerge::new()).await?;
        let d1_id = d1.document_id().to_string();
        big_repo
            .partition_store()
            .add_member(&p1, &d1_id, &empty_payload())
            .await?;

        let d2 = big_repo.create_doc(automerge::Automerge::new()).await?;
        let d2_id = d2.document_id().to_string();
        big_repo
            .partition_store()
            .add_member(&p2, &d2_id, &empty_payload())
            .await?;

        assert!(
            big_repo
                .is_doc_accessible_in_partitions(&d1_id, std::slice::from_ref(&p1))
                .await?
        );
        assert!(
            !big_repo
                .is_doc_accessible_in_partitions(&d2_id, std::slice::from_ref(&p1))
                .await?
        );

        let err = big_repo
            .get_docs_full_in_partitions(&[d1_id.clone(), d2_id.clone()], &[p1])
            .await
            .expect_err("request should be denied when one requested doc is inaccessible");
        assert!(
            err.to_string().contains("access denied for doc"),
            "expected access denied error, got: {err}"
        );
        Ok(())
    }
}
