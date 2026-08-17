use crate::interlude::*;
use crate::sqlite_big_repo_store::SqliteBigRepoStore;
use big_sync::HostPartStore;
use big_sync_core::rpc::{SubEvent, SubPartsRequest, SubscriptionTarget};
use big_sync_core::{ObjId, PartId};
use future_form::Sendable;
use keyhive_core::event::static_event::StaticEvent;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const EVENT_BATCH_SIZE: u32 = 64;
const IDLE_POLL: Duration = Duration::from_millis(25);
const WATERMARK_TIMEOUT: Duration = Duration::from_millis(500);
const AUTOMERGE_OBJ_MASK: [u8; 32] = [0x5A; 32];

pub fn automerge_docs_part_id() -> PartId {
    PartId::new(*blake3::hash(b"big_repo:automerge_docs_partition").as_bytes())
}

pub fn automerge_doc_obj_id(doc_id: crate::DocumentId) -> ObjId {
    let mut bytes = *doc_id.as_bytes();
    for (byte, mask_byte) in bytes.iter_mut().zip(AUTOMERGE_OBJ_MASK.iter()) {
        *byte ^= *mask_byte;
    }
    ObjId(big_sync_core::Byte32Id::new(bytes))
}

pub fn automerge_obj_to_doc_id(obj_id: ObjId) -> crate::DocumentId {
    let mut bytes = *obj_id.as_bytes();
    for (byte, mask_byte) in bytes.iter_mut().zip(AUTOMERGE_OBJ_MASK.iter()) {
        *byte ^= *mask_byte;
    }
    crate::DocumentId::new(bytes)
}

/// Background worker that maintains the Automerge Frontier partition log by
/// tailing the Sedimentree partition log and Keyhive events, enforcing
/// materialization barriers before publishing decrypted Automerge heads.
pub(crate) struct AutomergeFrontierWorker {
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    _state_generation: Arc<std::sync::atomic::AtomicU64>,
    source_part_id: PartId,
    automerge_part_id: PartId,
}

impl AutomergeFrontierWorker {
    pub(crate) fn new(
        store: SqliteBigRepoStore,
        big_sync_store: Arc<dyn HostPartStore>,
        runtime: crate::runtime2::Runtime2Handle<Sendable>,
        timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
        evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        state_generation: Arc<std::sync::atomic::AtomicU64>,
        source_part_id: PartId,
    ) -> Self {
        Self {
            store,
            big_sync_store,
            runtime,
            timer,
            _evt_tx: evt_tx,
            _state_generation: state_generation,
            source_part_id,
            automerge_part_id: automerge_docs_part_id(),
        }
    }

    pub(crate) async fn run(self) -> Res<()> {
        let (mut part_cursor, mut keyhive_cursor) = self.store.automerge_frontier_cursors().await?;

        let part_listener = match self
            .big_sync_store
            .subscribe_local(SubPartsRequest {
                targets: HashSet::from([SubscriptionTarget::Part {
                    part_id: self.source_part_id,
                    cursor: part_cursor,
                }]),
            })
            .await?
        {
            Ok(listener) => listener,
            Err(err) => {
                eyre::bail!("failed subscribing to source partition: {err:?}");
            }
        };

        loop {
            if self.runtime.is_stopped() {
                return Ok(());
            }

            // ── 1. Process Keyhive Events ──────────────────────────────────
            let keyhive_events = self
                .store
                .keyhive_events_after(keyhive_cursor, EVENT_BATCH_SIZE)
                .await?;

            if !keyhive_events.is_empty() {
                for row in &keyhive_events {
                    if self.runtime.is_stopped() {
                        return Ok(());
                    }
                    let event: StaticEvent<Vec<u8>> = match bincode::deserialize(&row.bytes) {
                        Ok(ev) => ev,
                        Err(_) => continue,
                    };
                    if let StaticEvent::CgkaOperation(op) = event {
                        let doc_id = crate::DocumentId::new(*op.payload().doc_id().as_bytes());
                        if let Ok(crate::runtime2::types::DocLookup::Ready(bundle)) =
                            self.runtime.get_doc_handle(doc_id).await
                        {
                            let heads = surelock::key::lock_scope(|key| {
                                let (doc, _key) = key.lock(&bundle.doc);
                                doc.get_heads()
                            });
                            let heads_formatted = am_utils_rs::serialize_commit_heads(&heads);
                            let am_obj_id = automerge_doc_obj_id(doc_id);
                            let payload = serde_json::json!({ "heads": heads_formatted });
                            self.big_sync_store
                                .set_obj_payload(am_obj_id, payload)
                                .await?;
                            self.big_sync_store
                                .add_obj_to_parts(am_obj_id, vec![self.automerge_part_id])
                                .await?;
                        }
                    }
                    keyhive_cursor = row.seq;
                }
                self.store
                    .commit_automerge_frontier_cursors(part_cursor, keyhive_cursor)
                    .await?;
            }

            // ── 2. Process Partition Events ────────────────────────────────
            tokio::select! {
                biased;
                part_event = part_listener.recv() => {
                    let part_event = match part_event {
                        Ok(ev) => ev,
                        Err(_) => {
                            if self.runtime.is_stopped() {
                                return Ok(());
                            }
                            return Err(ferr!("AutomergeFrontierWorker partition listener closed"));
                        }
                    };

                    let (obj_id, new_cursor) = match part_event {
                        SubEvent::Added(inner) => (Some(inner.obj_id), inner.cursor),
                        SubEvent::Changed(inner) => (Some(inner.obj_id), inner.cursor),
                        SubEvent::Removed(inner) => (None, inner.cursor),
                        SubEvent::ObjectChanged(_) | SubEvent::ReplayComplete => (None, part_cursor),
                    };

                    if let Some(obj_id) = obj_id {
                        let doc_id = crate::DocumentId::new(*obj_id.as_bytes());
                        let watermark = self.store.get_sync_commit_watermark(doc_id, new_cursor).await?;
                        if let Ok(crate::runtime2::types::DocLookup::Ready(bundle)) =
                            self.runtime.get_doc_handle(doc_id).await
                        {
                            if let Some(target_row_id) = watermark {
                                drop(
                                    bundle
                                        .await_commit_watermark(target_row_id, WATERMARK_TIMEOUT)
                                        .await,
                                );
                            }
                            let heads = surelock::key::lock_scope(|key| {
                                let (doc, _key) = key.lock(&bundle.doc);
                                doc.get_heads()
                            });
                            let heads_formatted = am_utils_rs::serialize_commit_heads(&heads);
                            let am_obj_id = automerge_doc_obj_id(doc_id);
                            let payload = serde_json::json!({ "heads": heads_formatted });
                            self.big_sync_store
                                .set_obj_payload(am_obj_id, payload)
                                .await?;
                            self.big_sync_store
                                .add_obj_to_parts(am_obj_id, vec![self.automerge_part_id])
                                .await?;
                        }
                    }

                    part_cursor = new_cursor;
                    self.store
                        .commit_automerge_frontier_cursors(part_cursor, keyhive_cursor)
                        .await?;
                }
                _ = self.timer.sleep(IDLE_POLL) => {}
            }
        }
    }
}
