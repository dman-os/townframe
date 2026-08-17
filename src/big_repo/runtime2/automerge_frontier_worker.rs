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
/// tailing the Sedimentree partition log for configured source partitions and Keyhive events,
/// enforcing materialization barriers before publishing decrypted Automerge heads.
pub(crate) struct AutomergeFrontierWorker {
    store: SqliteBigRepoStore,
    big_sync_store: Arc<dyn HostPartStore>,
    runtime: crate::runtime2::Runtime2Handle<Sendable>,
    timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
    _evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
    _state_generation: Arc<std::sync::atomic::AtomicU64>,
    parts_rx: tokio::sync::mpsc::UnboundedReceiver<HashSet<PartId>>,
    initial_source_parts: HashSet<PartId>,
    automerge_part_id: PartId,
}

impl AutomergeFrontierWorker {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: SqliteBigRepoStore,
        big_sync_store: Arc<dyn HostPartStore>,
        runtime: crate::runtime2::Runtime2Handle<Sendable>,
        timer: Arc<dyn crate::runtime2::Timer<Sendable>>,
        evt_tx: async_channel::Sender<crate::runtime2::Runtime2Evt>,
        state_generation: Arc<std::sync::atomic::AtomicU64>,
        parts_rx: tokio::sync::mpsc::UnboundedReceiver<HashSet<PartId>>,
        initial_source_parts: HashSet<PartId>,
    ) -> Self {
        Self {
            store,
            big_sync_store,
            runtime,
            timer,
            _evt_tx: evt_tx,
            _state_generation: state_generation,
            parts_rx,
            initial_source_parts,
            automerge_part_id: automerge_docs_part_id(),
        }
    }

    async fn subscribe_to_parts(
        big_sync_store: &Arc<dyn HostPartStore>,
        store: &SqliteBigRepoStore,
        parts: &HashSet<PartId>,
    ) -> Res<Option<big_sync_core::mpsc::Receiver<SubEvent>>> {
        if parts.is_empty() {
            return Ok(None);
        }
        let mut targets = HashSet::new();
        for &part_id in parts {
            let cursor = store.automerge_part_cursor(part_id).await.unwrap_or(0);
            targets.insert(SubscriptionTarget::Part { part_id, cursor });
        }
        match big_sync_store
            .subscribe_local(SubPartsRequest { targets })
            .await?
        {
            Ok(listener) => Ok(Some(listener)),
            Err(err) => {
                eyre::bail!("failed subscribing to source partitions: {err:?}");
            }
        }
    }

    async fn process_materialized_doc(
        doc_id: crate::DocumentId,
        runtime: &crate::runtime2::Runtime2Handle<Sendable>,
        store: &SqliteBigRepoStore,
        big_sync_store: &Arc<dyn HostPartStore>,
        watermark_cursor: Option<u64>,
        automerge_part_id: PartId,
    ) -> Res<()> {
        let watermark = if let Some(cursor) = watermark_cursor {
            store.get_sync_commit_watermark(doc_id, cursor).await?
        } else {
            None
        };
        if let Ok(crate::runtime2::types::DocLookup::Ready(bundle)) =
            runtime.get_doc_handle(doc_id).await
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
            big_sync_store.set_obj_payload(am_obj_id, payload).await?;
            big_sync_store
                .add_obj_to_parts(am_obj_id, vec![automerge_part_id])
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn run(mut self) -> Res<()> {
        let mut watched_parts = self.initial_source_parts.clone();
        let mut keyhive_cursor = self.store.automerge_keyhive_cursor().await.unwrap_or(0);
        let mut part_listener =
            Self::subscribe_to_parts(&self.big_sync_store, &self.store, &watched_parts).await?;

        loop {
            if self.runtime.is_stopped() {
                return Ok(());
            }

            // ── 1. Process Keyhive Events ──────────────────────────────────
            if !watched_parts.is_empty() {
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
                            let doc_parts = self
                                .big_sync_store
                                .obj_parts(doc_id)
                                .await
                                .unwrap_or_default();
                            if doc_parts.iter().any(|part| watched_parts.contains(part)) {
                                Self::process_materialized_doc(
                                    doc_id,
                                    &self.runtime,
                                    &self.store,
                                    &self.big_sync_store,
                                    None,
                                    self.automerge_part_id,
                                )
                                .await?;
                            }
                        }
                        keyhive_cursor = row.seq;
                    }
                    self.store
                        .commit_automerge_keyhive_cursor(keyhive_cursor)
                        .await?;
                }
            }

            // ── 2. Process Dynamic Parts & Partition Events ───────────────
            tokio::select! {
                biased;
                Some(new_parts) = self.parts_rx.recv() => {
                    if watched_parts != new_parts {
                        watched_parts = new_parts;
                        part_listener = Self::subscribe_to_parts(&self.big_sync_store, &self.store, &watched_parts).await?;
                    }
                }
                part_event = async {
                    match &mut part_listener {
                        Some(listener) => listener.recv().await.ok(),
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(part_event) = part_event else {
                        if self.runtime.is_stopped() {
                            return Ok(());
                        }
                        return Err(ferr!("AutomergeFrontierWorker partition listener closed"));
                    };

                    match part_event {
                        SubEvent::Added(inner) => {
                            let doc_id = crate::DocumentId::new(*inner.obj_id.as_bytes());
                            Self::process_materialized_doc(
                                doc_id,
                                &self.runtime,
                                &self.store,
                                &self.big_sync_store,
                                Some(inner.cursor),
                                self.automerge_part_id,
                            )
                            .await?;
                            self.store
                                .commit_automerge_part_cursor(inner.part_id, inner.cursor)
                                .await?;
                        }
                        SubEvent::Changed(inner) => {
                            let doc_id = crate::DocumentId::new(*inner.obj_id.as_bytes());
                            Self::process_materialized_doc(
                                doc_id,
                                &self.runtime,
                                &self.store,
                                &self.big_sync_store,
                                Some(inner.cursor),
                                self.automerge_part_id,
                            )
                            .await?;
                            for &part_id in &inner.part_ids {
                                if watched_parts.contains(&part_id) {
                                    self.store
                                        .commit_automerge_part_cursor(part_id, inner.cursor)
                                        .await?;
                                }
                            }
                        }
                        SubEvent::Removed(inner) => {
                            self.store
                                .commit_automerge_part_cursor(inner.part_id, inner.cursor)
                                .await?;
                        }
                        SubEvent::ObjectChanged(_) | SubEvent::ReplayComplete => {}
                    }
                }
                _ = self.timer.sleep(IDLE_POLL) => {}
            }
        }
    }
}
