use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::sqlite_big_repo_store::{GroupPartReconciliation, SqliteBigRepoStore};
use big_sync_core::{ObjId, PartId, PeerId};
use keyhive_core::event::static_event::StaticEvent;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const EVENT_BATCH_SIZE: u32 = 64;
const DOC_BATCH_SIZE: usize = 64;
const IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(25);

/// Crash-recoverable maintenance for Keyhive-derived policy and partitions.
///
/// The event log is only a durable dirty hint. Every reconciliation queries the
/// current Keyhive state, so replaying an event is harmless and pending events do
/// not create speculative policy or partition membership.
pub(crate) struct GroupPartWorker {
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    local_peer_id: PeerId,
    timer: Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
    evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
}

impl GroupPartWorker {
    pub(crate) fn new(
        store: SqliteBigRepoStore,
        keyhive: BigKeyhiveHandle,
        local_peer_id: PeerId,
        timer: Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
        evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    ) -> Self {
        Self {
            store,
            keyhive,
            local_peer_id,
            timer,
            evt_tx,
        }
    }

    pub(crate) async fn run(self) -> Res<()> {
        let mut announced_idle = false;
        loop {
            let cursor = self.store.keyhive_group_part_cursor().await?;
            let events = self
                .store
                .keyhive_events_after(cursor, EVENT_BATCH_SIZE)
                .await?;
            if events.is_empty() {
                if !announced_idle {
                    if self
                        .evt_tx
                        .send(crate::runtime2::Runtime2Evt::GroupPartWorkerAdvanced { cursor })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    announced_idle = true;
                }
                self.timer.sleep(IDLE_POLL).await;
                continue;
            }
            announced_idle = false;

            let event_cursor = events.last().expect("non-empty event batch").seq;
            let group_documents = self.keyhive.group_document_ids_by_id().await;
            let managed_group_parts: HashSet<PartId> =
                group_documents.keys().copied().map(group_part_id).collect();
            let local_principal = self.local_peer_id;
            let missed_history = events
                .first()
                .is_some_and(|event| event.seq > cursor.saturating_add(1));
            let docs: Vec<_> = if missed_history {
                tracing::warn!(
                    cursor,
                    first_retained_event = events.first().expect("non-empty event batch").seq,
                    "group-part event history was pruned; rebuilding current document state"
                );
                self.keyhive.document_ids().await
            } else {
                let mut docs = HashSet::new();
                for event in &events {
                    docs.extend(affected_documents(&event.bytes, &group_documents));
                }
                docs.into_iter().collect()
            };
            if docs.is_empty() {
                self.store
                    .reconcile_group_part_batch(&[], event_cursor, true)
                    .await?;
            } else {
                let batch_count = docs.len().div_ceil(DOC_BATCH_SIZE);
                for (batch_index, doc_batch) in docs.chunks(DOC_BATCH_SIZE).enumerate() {
                    let mut reconciliations = Vec::with_capacity(doc_batch.len());
                    for &doc in doc_batch {
                        reconciliations.push(
                            self.reconciliation_for(
                                doc,
                                &group_documents,
                                &managed_group_parts,
                                local_principal,
                            )
                            .await?,
                        );
                    }
                    self.store
                        .reconcile_group_part_batch(
                            &reconciliations,
                            event_cursor,
                            batch_index + 1 == batch_count,
                        )
                        .await?;
                }
            }
        }
    }

    async fn reconciliation_for(
        &self,
        doc: ObjId,
        group_documents: &HashMap<[u8; 32], std::collections::BTreeSet<ObjId>>,
        managed_group_parts: &HashSet<PartId>,
        local_principal: PeerId,
    ) -> Res<GroupPartReconciliation> {
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc.into_bytes())
            .map_err(|_| ferr!("document id is not a valid Ed25519 point"))?;
        let identifier = keyhive_core::principal::identifier::Identifier::from(verifying_key);
        let agents = self
            .keyhive
            .agents_for_membered(identifier)
            .await
            .into_iter()
            .map(|(principal, access)| (PeerId::new(principal), access))
            .collect::<HashMap<_, _>>();
        let desired_group_parts = group_documents
            .iter()
            .filter(|(_, documents)| documents.contains(&doc))
            .map(|(group_id, _)| group_part_id(*group_id))
            .collect();
        let desired_global = agents
            .get(&local_principal)
            .is_some_and(|access| access.is_fetcher());
        Ok(GroupPartReconciliation {
            doc,
            agents,
            managed_group_parts: managed_group_parts.clone(),
            desired_group_parts,
            desired_global,
        })
    }
}

fn group_part_id(group_id: [u8; 32]) -> PartId {
    let mut bytes = b"townframe/big-repo/group-part/sedimentree/v1".to_vec();
    bytes.extend_from_slice(&group_id);
    let raw = keyhive_crypto::digest::Digest::<Vec<u8>>::hash(&bytes).raw;
    PartId::new(raw.into())
}

fn affected_documents(
    bytes: &[u8],
    group_documents: &HashMap<[u8; 32], std::collections::BTreeSet<ObjId>>,
) -> Vec<ObjId> {
    let event: StaticEvent<Vec<u8>> =
        bincode::deserialize(bytes).expect("persisted Keyhive event must decode");
    let mut documents = Vec::new();
    match event {
        StaticEvent::CgkaOperation(operation) => {
            documents.push(ObjId::new(*operation.payload().doc_id().as_bytes()));
        }
        StaticEvent::Delegated(delegation) => {
            documents.extend(
                delegation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjId::new(id.to_bytes())),
            );
            if let Some(group_docs) = group_documents.get(delegation.issuer.as_bytes()) {
                documents.extend(group_docs.iter().copied());
            }
        }
        StaticEvent::Revoked(revocation) => {
            documents.extend(
                revocation
                    .payload()
                    .after_content
                    .keys()
                    .map(|id| ObjId::new(id.to_bytes())),
            );
            if let Some(group_docs) = group_documents.get(revocation.issuer.as_bytes()) {
                documents.extend(group_docs.iter().copied());
            }
        }
        StaticEvent::PrekeysExpanded(_) | StaticEvent::PrekeyRotated(_) => {}
    }
    documents
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_part_id_uses_sedimentree_namespace() {
        let actual = group_part_id([0; 32]);
        assert_eq!(
            actual.to_string(),
            "B1TtXt35pLe8AyPkUKgPLgbpFHckKjK3CHCQEytRFaLj"
        );
    }
}
