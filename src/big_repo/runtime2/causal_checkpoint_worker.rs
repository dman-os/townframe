use crate::interlude::*;
use crate::keyhive::BigKeyhiveHandle;
use crate::sqlite_big_repo_store::SqliteBigRepoStore;
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::sync::Arc;

const IDLE_POLL: std::time::Duration = std::time::Duration::from_millis(25);

/// Crash-recoverable consumer of Keyhive events that closes every document
/// key transition with causal content coverage before advancing its own
/// durable cursor.
pub(crate) struct CausalCheckpointWorker {
    store: SqliteBigRepoStore,
    keyhive: BigKeyhiveHandle,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
}

impl CausalCheckpointWorker {
    pub(crate) fn new(
        store: SqliteBigRepoStore,
        keyhive: BigKeyhiveHandle,
        runtime: crate::runtime2::Runtime2Handle<Sendable>,
        timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    ) -> Self {
        Self {
            store,
            keyhive,
            runtime,
            timer,
        }
    }

    pub(crate) async fn run(self) -> Res<()> {
        // The cursor is authoritative for logged work. The startup audit also
        // covers databases created before this consumer existed, pruned event
        // history, and crashes after an Update was persisted but before its
        // covering ciphertext.
        while !self.attempt_all_documents().await? {
            if self.runtime.is_stopped() {
                return Ok(());
            }
            self.timer.sleep(IDLE_POLL).await;
        }

        loop {
            if self.runtime.is_stopped() {
                return Ok(());
            }
            let cursor = self.store.causal_checkpoint_cursor().await?;
            let events = self.store.keyhive_events_after(cursor, 1).await?;
            let Some(row) = events.first() else {
                self.timer.sleep(IDLE_POLL).await;
                continue;
            };

            if row.seq > cursor.saturating_add(1) && !self.attempt_all_documents().await? {
                self.timer.sleep(IDLE_POLL).await;
                continue;
            }

            let event: StaticEvent<Vec<u8>> =
                bincode::deserialize(&row.bytes).expect("persisted Keyhive event must decode");
            let complete = match event {
                StaticEvent::CgkaOperation(operation) => {
                    let doc_id = crate::DocumentId::new(*operation.payload().doc_id().as_bytes());
                    match self.runtime.ensure_causal_coverage(doc_id).await {
                        Ok(complete) => complete,
                        Err(_) if self.runtime.is_stopped() => return Ok(()),
                        Err(error) => return Err(error),
                    }
                }
                StaticEvent::Delegated(_)
                | StaticEvent::Revoked(_)
                | StaticEvent::PrekeysExpanded(_)
                | StaticEvent::PrekeyRotated(_) => true,
            };
            if complete {
                self.store.advance_causal_checkpoint_cursor(row.seq).await?;
            } else {
                self.timer.sleep(IDLE_POLL).await;
            }
        }
    }

    async fn attempt_all_documents(&self) -> Res<bool> {
        for doc_id in self.keyhive.document_ids().await {
            if self.runtime.is_stopped() {
                return Ok(false);
            }
            let doc_id = crate::DocumentId::new(*doc_id.as_bytes());
            let complete = match self.runtime.ensure_causal_coverage(doc_id).await {
                Ok(complete) => complete,
                Err(_) if self.runtime.is_stopped() => return Ok(false),
                Err(error) => return Err(error),
            };
            if !complete {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
