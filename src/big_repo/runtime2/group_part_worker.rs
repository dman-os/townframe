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
const GENERATION_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(10);
const GENERATION_DEBOUNCE_MAX_EXTENSIONS: usize = 4;

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
    /// Shared Keyhive state-generation counter (bumped by the hub).
    state_generation: Arc<std::sync::atomic::AtomicU64>,
    /// Highest generation this worker has fully reconciled.
    last_acked_generation: u64,
}

impl GroupPartWorker {
    pub(crate) fn new(
        store: SqliteBigRepoStore,
        keyhive: BigKeyhiveHandle,
        local_peer_id: PeerId,
        timer: Arc<dyn crate::runtime2::Timer<future_form::Sendable>>,
        evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        state_generation: Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            store,
            keyhive,
            local_peer_id,
            timer,
            evt_tx,
            state_generation,
            last_acked_generation: 0,
        }
    }

    pub(crate) async fn run(mut self) -> Res<()> {
        let mut announced_idle = false;
        loop {
            let cursor = self.store.keyhive_group_part_cursor().await?;
            let generation = self
                .state_generation
                .load(std::sync::atomic::Ordering::Relaxed);
            if generation > self.last_acked_generation {
                let generation = self.debounce_generation(generation).await;
                if !self.rebuild_for_generation(generation, cursor).await? {
                    return Ok(());
                }
                announced_idle = false;
                continue;
            }
            let events = self
                .store
                .keyhive_events_after(cursor, EVENT_BATCH_SIZE)
                .await?;
            if events.is_empty() {
                if !announced_idle {
                    if self
                        .evt_tx
                        .send(crate::runtime2::Runtime2Evt::GroupPartWorkerAdvanced {
                            generation: self.last_acked_generation,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                    announced_idle = true;
                }
                let notified = self.store.keyhive_event_notifier();
                tokio::select! {
                    _ = notified.notified() => {}
                    _ = self.timer.sleep(IDLE_POLL) => {}
                }
                continue;
            }
            tracing::debug!(
                cursor,
                event_count = events.len(),
                first_event = events.first().expect("non-empty event batch").seq,
                last_event = events.last().expect("non-empty event batch").seq,
                "group-part worker processing Keyhive events"
            );
            announced_idle = false;

            let event_cursor = events.last().expect("non-empty event batch").seq;
            let group_documents = self.keyhive.group_document_ids_by_id().await;
            let managed_group_parts: HashSet<PartId> =
                group_documents.keys().copied().map(group_part_id).collect();
            // Pre-create the group parts as we learn about groups (the group
            // is the access primitive). A group part row must exist for the
            // part to be advertiseable (`summarize_parts` succeeds) even
            // before any doc payload arrives — a pending want is pull access
            // and the route must be establishable for the first pull to
            // promote it. Source is ALL visible groups (keyhive restricts
            // visibility by definition), NOT just groups referenced by known
            // docs: membership precedes document payloads, and an empty
            // group must still advertise its part or peers answer the route
            // with UnkownParts and it never establishes. Runs before doc
            // reconciliation so member-before-group and group-before-member
            // orderings both settle.
            for group_id in self.keyhive.visible_group_ids().await {
                self.store
                    .ensure_part(group_part_id(group_id.to_bytes()))
                    .await?;
            }
            let local_principal = self.local_peer_id;
            let docs = self.keyhive.document_ids().await;
            tracing::debug!(
                cursor,
                event_cursor,
                document_count = docs.len(),
                ?docs,
                managed_group_part_count = managed_group_parts.len(),
                "group-part worker derived affected documents"
            );
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
                    tracing::debug!(
                        cursor,
                        event_cursor,
                        batch_index,
                        "group-part worker reconciled batch"
                    );
                }
            }
        }
    }

    /// Collapse a burst of Keyhive state-generation bumps into one rebuild.
    ///
    /// A quiet interval ends the debounce early. The extension cap guarantees
    /// that a continuous stream cannot postpone policy/partition projection
    /// indefinitely; changes arriving during the rebuild are picked up by the
    /// next loop.
    async fn debounce_generation(&self, mut generation: u64) -> u64 {
        for _ in 0..GENERATION_DEBOUNCE_MAX_EXTENSIONS {
            self.timer.sleep(GENERATION_DEBOUNCE).await;
            let latest = self
                .state_generation
                .load(std::sync::atomic::Ordering::Relaxed);
            if latest == generation {
                break;
            }
            generation = latest;
        }
        generation
    }

    /// Full rebuild driven by a Keyhive state-generation advance. The event
    /// log is only a durable dirty hint (B9): pending events can resolve with
    /// no new row, so the worker must re-derive the projection from current
    /// Keyhive state whenever the hub reports a state advance. Returns
    /// `false` when the event channel closed (runtime stopping).
    async fn rebuild_for_generation(&mut self, generation: u64, cursor: u64) -> Res<bool> {
        tracing::debug!(
            generation,
            cursor,
            "group-part worker full-rebuilding for Keyhive state generation"
        );
        let group_documents = self.keyhive.group_document_ids_by_id().await;
        let managed_group_parts: HashSet<PartId> =
            group_documents.keys().copied().map(group_part_id).collect();
        // Pre-create group part rows (see the event path) so parts are
        // advertiseable on rebuilds too. All visible groups, not just
        // groups referenced by known docs — an empty group must still
        // advertise its part.
        for group_id in self.keyhive.visible_group_ids().await {
            self.store
                .ensure_part(group_part_id(group_id.to_bytes()))
                .await?;
        }
        let local_principal = self.local_peer_id;
        let docs: Vec<_> = self.keyhive.document_ids().await;
        if docs.is_empty() {
            self.store
                .reconcile_group_part_batch(&[], cursor, true)
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
                        cursor,
                        batch_index + 1 == batch_count,
                    )
                    .await?;
            }
        }
        self.last_acked_generation = generation;
        if self
            .evt_tx
            .send(crate::runtime2::Runtime2Evt::GroupPartWorkerAdvanced { generation })
            .await
            .is_err()
        {
            return Ok(false);
        }
        Ok(true)
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
        let desired_group_parts: HashSet<PartId> = group_documents
            .iter()
            .filter(|(_, documents)| documents.contains(&doc))
            .map(|(group_id, _)| group_part_id(*group_id))
            .collect();
        let desired_global = agents
            .get(&local_principal)
            .is_some_and(|access| access.is_reader());
        tracing::debug!(
            ?doc,
            agent_count = agents.len(),
            local_access = ?agents.get(&local_principal),
            desired_global,
            desired_group_part_count = desired_group_parts.len(),
            "group-part worker computed document reconciliation"
        );
        Ok(GroupPartReconciliation {
            doc,
            agents,
            managed_group_parts: managed_group_parts.clone(),
            desired_group_parts,
            desired_global,
        })
    }
}

pub(crate) fn group_part_id(group_id: [u8; 32]) -> PartId {
    let mut bytes = b"townframe/big-repo/group-part/sedimentree/v1".to_vec();
    bytes.extend_from_slice(&group_id);
    let raw = keyhive_crypto::digest::Digest::<Vec<u8>>::hash(&bytes).raw;
    PartId::new(raw.into())
}

#[allow(dead_code)]
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
