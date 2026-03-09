use crate::interlude::*;

mod node;
mod partition;
mod peer;
mod protocol;

use tokio::sync::mpsc;

pub use node::{spawn_sync_node, SyncNodeHandle, SyncNodeStopToken};
pub use partition::StaticPartitionSyncProvider;
pub use peer::{
    spawn_peer_sync_worker, PeerSyncWorkerHandle, PeerSyncWorkerStopToken, SamodSyncRequest,
};

pub type PartitionId = String;

pub type PeerKey = String;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionSummary {
    pub partition_id: PartitionId,
    pub latest_cursor: OpaqueCursor,
    pub member_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionEventKind {
    MemberUpsert {
        doc_id: String,
    },
    MemberRemoved {
        doc_id: String,
    },
    DocChanged {
        doc_id: String,
        heads: Vec<String>,
        change_count_hint: u64,
    },
    DocDeleted {
        doc_id: String,
        change_count_hint: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionEvent {
    pub cursor: OpaqueCursor,
    pub partition_id: PartitionId,
    pub kind: PartitionEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FullDoc {
    pub doc_id: String,
    pub automerge_save: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ListPartitionsRequest;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ListPartitionsResponse {
    pub partitions: Vec<PartitionSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionCursorRequest {
    pub partition_id: PartitionId,
    pub since: Option<OpaqueCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionEventsRequest {
    pub partitions: Vec<PartitionCursorRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionEventsResponse {
    pub events: Vec<PartitionEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetDocsFullRequest {
    pub doc_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetDocsFullResponse {
    pub docs: Vec<FullDoc>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubPartitionsRequest {
    pub partitions: Vec<PartitionCursorRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubscriptionItem {
    Event(PartitionEvent),
    SnapshotComplete,
}

#[derive(
    Debug,
    thiserror::Error,
    displaydoc::Display,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum PartitionSyncError {
    /// access denied for partition {partition_id:?}
    AccessDenied { partition_id: PartitionId },
    /// invalid cursor {cursor:?}
    InvalidCursor { cursor: OpaqueCursor },
    /// requested too many docs: requested={requested} max={max}
    TooManyDocIds { requested: usize, max: usize },
    /// unknown partition {partition_id:?}
    UnknownPartition { partition_id: PartitionId },
    /// access denied for doc {doc_id}
    DocAccessDenied { doc_id: String },
    /// internal error: {message}
    Internal { message: String },
}

impl PartitionSyncError {
    pub(crate) fn into_report(self) -> eyre::Report {
        ferr!("{self}")
    }
}

pub const MAX_GET_DOCS_FULL_DOC_IDS: usize = 256;
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1024;

pub trait PartitionAccessPolicy: Send + Sync + 'static {
    fn can_access_partition(&self, peer: &PeerKey, partition_id: &PartitionId) -> bool;
}

pub struct AllowAllPartitionAccessPolicy;

impl PartitionAccessPolicy for AllowAllPartitionAccessPolicy {
    fn can_access_partition(&self, _peer: &PeerKey, _partition_id: &PartitionId) -> bool {
        true
    }
}

pub struct PartitionSubscription {
    pub rx: mpsc::Receiver<SubscriptionItem>,
}

#[derive(Debug, Clone)]
pub enum PeerSyncProgressEvent {
    RequestStarted {
        op: &'static str,
    },
    RequestFinished {
        op: &'static str,
        success: bool,
        elapsed: Duration,
    },
    SubscriptionForwarded,
}

#[async_trait]
pub trait PartitionSyncProvider: Send + Sync + 'static {
    async fn list_partitions_for_peer(&self, peer: &PeerKey) -> Res<Vec<PartitionSummary>>;
    async fn get_partition_events(
        &self,
        peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
    ) -> Res<Vec<PartitionEvent>>;
    async fn get_docs_full(&self, peer: &PeerKey, doc_ids: &[String]) -> Res<Vec<FullDoc>>;
    async fn is_doc_accessible_for_peer(&self, peer: &PeerKey, doc_id: &str) -> Res<bool>;
    async fn subscribe(
        &self,
        peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
        capacity: usize,
    ) -> Res<PartitionSubscription>;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::repo::{BigRepo, BigRepoConfig};

    use automerge::transaction::Transactable;
    use samod::DocumentId;
    use std::collections::HashMap;
    use std::str::FromStr;

    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn get_partition_events_since_cursor_works() {
        let provider = StaticPartitionSyncProvider::new();
        let peer = "peer-a".into();

        let first = provider.upsert_member("p1".into(), "d1".into()).await;
        provider
            .emit_doc_changed("p1".into(), "d1".into(), vec!["h1".into()], 1)
            .await;
        provider.remove_member("p1".into(), "d2".into()).await;

        let response = provider
            .get_partition_events(
                &peer,
                &[PartitionCursorRequest {
                    partition_id: "p1".into(),
                    since: Some(first.cursor),
                }],
            )
            .await
            .unwrap();

        assert_eq!(response.len(), 2);
        assert!(matches!(
            response[0].kind,
            PartitionEventKind::DocChanged { .. }
        ));
        assert!(matches!(
            response[1].kind,
            PartitionEventKind::MemberRemoved { .. }
        ));
    }

    #[tokio::test]
    async fn subscribe_replays_then_streams_live() {
        let provider = StaticPartitionSyncProvider::new();
        let peer = "peer-a".into();
        provider.upsert_member("p1".into(), "d1".into()).await;

        let mut sub = provider
            .subscribe(
                &peer,
                &[PartitionCursorRequest {
                    partition_id: "p1".into(),
                    since: None,
                }],
                16,
            )
            .await
            .unwrap();

        let first = sub.rx.recv().await.unwrap();
        assert!(matches!(first, SubscriptionItem::Event(_)));
        let second = sub.rx.recv().await.unwrap();
        assert_eq!(second, SubscriptionItem::SnapshotComplete);

        provider
            .emit_doc_changed("p1".into(), "d1".into(), vec!["head2".into()], 2)
            .await;
        let live = sub.rx.recv().await.unwrap();
        assert!(matches!(
            live,
            SubscriptionItem::Event(PartitionEvent {
                kind: PartitionEventKind::DocChanged { .. },
                ..
            })
        ));
    }

    #[tokio::test]
    async fn snapshot_does_not_emit_docchanged_for_removed_member() {
        let provider = StaticPartitionSyncProvider::new();
        let peer = "peer-a".into();

        provider.upsert_member("p1".into(), "d1".into()).await;
        provider
            .emit_doc_changed("p1".into(), "d1".into(), vec!["h1".into()], 1)
            .await;
        provider.remove_member("p1".into(), "d1".into()).await;

        let snapshot = provider
            .get_partition_events(
                &peer,
                &[PartitionCursorRequest {
                    partition_id: "p1".into(),
                    since: None,
                }],
            )
            .await
            .unwrap();

        assert!(!snapshot.iter().any(|event| {
            matches!(
                event.kind,
                PartitionEventKind::MemberUpsert { ref doc_id } if doc_id == "d1"
            )
        }));
        assert!(!snapshot.iter().any(|event| {
            matches!(
                event.kind,
                PartitionEventKind::DocChanged { ref doc_id, .. } if doc_id == "d1"
            )
        }));
    }

    #[tokio::test]
    async fn peer_worker_uses_irpc_roundtrip() {
        let provider = Arc::new(StaticPartitionSyncProvider::new());
        provider.upsert_member("p1".into(), "d1".into()).await;
        provider
            .emit_doc_changed("p1".into(), "d1".into(), vec!["h-1".into()], 1)
            .await;
        provider.set_full_doc("d1".into(), vec![1, 2, 3]).await;

        let cancel = CancellationToken::new();
        let (node, node_stop) = spawn_sync_node(
            Arc::<StaticPartitionSyncProvider>::clone(&provider),
            Arc::new(AllowAllPartitionAccessPolicy),
            cancel.child_token(),
        )
        .await
        .unwrap();
        let peer: PeerKey = "peer-irpc".into();
        node.register_local_peer(peer.clone()).await.unwrap();

        let (_samod_tx, samod_rx) = mpsc::channel(4);
        let (peer_worker, peer_stop) = spawn_peer_sync_worker(
            peer.clone(),
            node.rpc_client(),
            samod_rx,
            cancel.child_token(),
        )
        .await
        .unwrap();

        let list = peer_worker.list_partitions().await.unwrap();
        assert_eq!(list.partitions.len(), 1);
        assert_eq!(&list.partitions[0].partition_id, "p1");

        let events = peer_worker
            .get_partition_events(GetPartitionEventsRequest {
                partitions: vec![PartitionCursorRequest {
                    partition_id: "p1".into(),
                    since: None,
                }],
            })
            .await
            .unwrap();
        assert!(events
            .events
            .iter()
            .any(|event| matches!(event.kind, PartitionEventKind::MemberUpsert { .. })));
        assert!(events
            .events
            .iter()
            .any(|event| matches!(event.kind, PartitionEventKind::DocChanged { .. })));

        let docs = peer_worker
            .get_docs_full(GetDocsFullRequest {
                doc_ids: vec!["d1".into()],
            })
            .await
            .unwrap();
        assert_eq!(docs.docs.len(), 1);
        assert_eq!(docs.docs[0].automerge_save, vec![1, 2, 3]);

        peer_stop.stop().await.unwrap();
        node_stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn peer_subscription_over_irpc_replays_and_goes_live() {
        let provider = Arc::new(StaticPartitionSyncProvider::new());
        provider.upsert_member("p1".into(), "d1".into()).await;

        let cancel = CancellationToken::new();
        let (node, node_stop) = spawn_sync_node(
            Arc::<StaticPartitionSyncProvider>::clone(&provider),
            Arc::new(AllowAllPartitionAccessPolicy),
            cancel.child_token(),
        )
        .await
        .unwrap();
        let peer: PeerKey = "peer-sub".into();
        node.register_local_peer(peer.clone()).await.unwrap();

        let (_samod_tx, samod_rx) = mpsc::channel(4);
        let (peer_worker, peer_stop) = spawn_peer_sync_worker(
            peer.clone(),
            node.rpc_client(),
            samod_rx,
            cancel.child_token(),
        )
        .await
        .unwrap();

        let mut sub = peer_worker
            .subscribe(SubPartitionsRequest {
                partitions: vec![PartitionCursorRequest {
                    partition_id: "p1".into(),
                    since: None,
                }],
            })
            .await
            .unwrap();

        let first = sub.rx.recv().await.unwrap();
        assert!(matches!(first, SubscriptionItem::Event(_)));
        let second = sub.rx.recv().await.unwrap();
        assert_eq!(second, SubscriptionItem::SnapshotComplete);

        provider
            .emit_doc_changed("p1".into(), "d1".into(), vec!["h-live".into()], 7)
            .await;
        let third = sub.rx.recv().await.unwrap();
        assert!(matches!(
            third,
            SubscriptionItem::Event(PartitionEvent {
                kind: PartitionEventKind::DocChanged { .. },
                ..
            })
        ));

        peer_stop.stop().await.unwrap();
        node_stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn peer_subscription_over_irpc_receives_mixed_live_event_kinds() {
        let provider = Arc::new(StaticPartitionSyncProvider::new());
        provider.upsert_member("p1".into(), "seed".into()).await;

        let cancel = CancellationToken::new();
        let (node, node_stop) = spawn_sync_node(
            Arc::<StaticPartitionSyncProvider>::clone(&provider),
            Arc::new(AllowAllPartitionAccessPolicy),
            cancel.child_token(),
        )
        .await
        .unwrap();
        let peer: PeerKey = "peer-mixed-live".into();
        node.register_local_peer(peer.clone()).await.unwrap();

        let (_samod_tx, samod_rx) = mpsc::channel(4);
        let (peer_worker, peer_stop) = spawn_peer_sync_worker(
            peer.clone(),
            node.rpc_client(),
            samod_rx,
            cancel.child_token(),
        )
        .await
        .unwrap();

        let mut sub = peer_worker
            .subscribe(SubPartitionsRequest {
                partitions: vec![PartitionCursorRequest {
                    partition_id: "p1".into(),
                    since: None,
                }],
            })
            .await
            .unwrap();

        // Drain initial replay until live phase starts.
        loop {
            let next = tokio::time::timeout(Duration::from_secs(2), sub.rx.recv())
                .await
                .expect("timed out waiting for replay item")
                .expect("subscription closed before snapshot complete");
            if next == SubscriptionItem::SnapshotComplete {
                break;
            }
        }

        provider
            .upsert_member("p1".into(), "doc-live-added".into())
            .await;
        provider.remove_member("p1".into(), "seed".into()).await;
        provider
            .emit_doc_changed(
                "p1".into(),
                "doc-live-added".into(),
                vec!["h-live-1".into()],
                11,
            )
            .await;
        provider
            .emit_doc_deleted("p1".into(), "doc-live-added".into(), 12)
            .await;

        let mut saw_member_upsert = false;
        let mut saw_member_removed = false;
        let mut saw_doc_changed = false;
        let mut saw_doc_deleted = false;
        let mut received = 0usize;
        while received < 4 {
            let next = tokio::time::timeout(Duration::from_secs(2), sub.rx.recv())
                .await
                .expect("timed out waiting for live event")
                .expect("subscription closed before live events");
            let SubscriptionItem::Event(event) = next else {
                continue;
            };
            received += 1;
            match event.kind {
                PartitionEventKind::MemberUpsert { .. } => saw_member_upsert = true,
                PartitionEventKind::MemberRemoved { .. } => saw_member_removed = true,
                PartitionEventKind::DocChanged { .. } => saw_doc_changed = true,
                PartitionEventKind::DocDeleted { .. } => saw_doc_deleted = true,
            }
        }

        assert!(saw_member_upsert);
        assert!(saw_member_removed);
        assert!(saw_doc_changed);
        assert!(saw_doc_deleted);

        peer_stop.stop().await.unwrap();
        node_stop.stop().await.unwrap();
    }

    #[tokio::test]
    async fn big_test_partition_sync_reconnect_flow() -> Res<()> {
        run_partition_reconnect_flow_test(40, 20, 10, 10, false).await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "heavy scenario; enable manually while iterating on sync perf"]
    async fn big_test_partition_sync_reconnect_flow_large() -> Res<()> {
        run_partition_reconnect_flow_test(2000, 1000, 500, 500, true).await?;
        Ok(())
    }

    async fn run_partition_reconnect_flow_test(
        total_docs: usize,
        initial_partition_size: usize,
        modify_count: usize,
        add_count: usize,
        print_progress: bool,
    ) -> Res<()> {
        let start_all = std::time::Instant::now();
        let part_id = "sync-part-1".into();
        let src = boot_big_repo("src-peer").await?;
        let dst = boot_big_repo("dst-peer").await?;

        let mut source_doc_ids = Vec::with_capacity(total_docs);
        for idx in 0..total_docs {
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

        for doc_id in source_doc_ids.iter().take(initial_partition_size) {
            src.add_doc_to_partition(&part_id, doc_id).await?;
        }

        let cancel = CancellationToken::new();
        let (node, node_stop) = spawn_sync_node(
            src.clone(),
            Arc::new(AllowAllPartitionAccessPolicy),
            cancel.child_token(),
        )
        .await?;
        let peer_key: PeerKey = "peer-b".into();
        node.register_local_peer(peer_key.clone()).await?;
        let (_samod_tx, samod_rx) = mpsc::channel(128);
        let (mut peer_worker, peer_stop) = spawn_peer_sync_worker(
            peer_key.clone(),
            node.rpc_client(),
            samod_rx,
            cancel.child_token(),
        )
        .await?;
        let mut progress_rx = peer_worker.events_rx.take().expect("progress rx missing");

        let mut cursor = None;
        let mut dst_doc_cache: HashMap<String, crate::repo::BigDocHandle> = HashMap::new();
        let sync_t0 = std::time::Instant::now();
        sync_partition_once(
            &peer_worker,
            &dst,
            &part_id,
            &mut cursor,
            &mut dst_doc_cache,
        )
        .await?;
        let first_sync_elapsed = sync_t0.elapsed();
        let member_count = dst.partition_member_count(&part_id).await?;
        if print_progress {
            drain_progress_events(&mut progress_rx).await;
            eprintln!(
                "[sync] first sync done in {first_sync_elapsed:?}, dst member count={member_count}",
            );
        }
        assert!(
            member_count >= 500.min(initial_partition_size as i64),
            "expected at least 500 members after first sync checkpoint, got {member_count}"
        );
        assert_eq!(member_count, initial_partition_size as i64);

        peer_stop.stop().await?;

        for doc_id in source_doc_ids.iter().take(modify_count) {
            let parsed = DocumentId::from_str(doc_id)
                .map_err(|err| ferr!("invalid doc id {doc_id}: {err}"))?;
            let handle = src
                .find_doc(&parsed)
                .await?
                .ok_or_eyre("source doc missing during modify phase")?;
            handle
                .with_document(|doc| {
                    let mut tx = doc.transaction();
                    tx.put(automerge::ROOT, "mutated", true)
                        .expect("failed writing mutate flag");
                    tx.commit();
                })
                .await?;
        }
        for idx in 0..add_count {
            let doc_idx = initial_partition_size + idx;
            let Some(doc_id) = source_doc_ids.get(doc_idx) else {
                break;
            };
            src.add_doc_to_partition(&part_id, doc_id).await?;
        }

        let (_samod_tx2, samod_rx2) = mpsc::channel(128);
        let (mut peer_worker2, peer_stop2) = spawn_peer_sync_worker(
            peer_key.clone(),
            node.rpc_client(),
            samod_rx2,
            cancel.child_token(),
        )
        .await?;
        let mut progress_rx2 = peer_worker2.events_rx.take().expect("progress rx missing");
        let sync_t1 = std::time::Instant::now();
        sync_partition_once(
            &peer_worker2,
            &dst,
            &part_id,
            &mut cursor,
            &mut dst_doc_cache,
        )
        .await?;
        let second_sync_elapsed = sync_t1.elapsed();

        let final_count = dst.partition_member_count(&part_id).await?;
        if print_progress {
            drain_progress_events(&mut progress_rx2).await;
            eprintln!(
                "[sync] second sync done in {second_sync_elapsed:?}, dst member count={final_count}",
            );
        }

        let expected = (initial_partition_size
            + add_count.min(total_docs.saturating_sub(initial_partition_size)))
            as i64;
        assert_eq!(final_count, expected);

        peer_stop2.stop().await?;
        node_stop.stop().await?;
        if print_progress {
            eprintln!("[sync] scenario finished in {:?}", start_all.elapsed());
        }
        Ok(())
    }

    async fn boot_big_repo(peer_seed: &str) -> Res<Arc<BigRepo>> {
        let repo = samod::Repo::build_tokio()
            .with_peer_id(samod::PeerId::from_string(format!("big-{peer_seed}")))
            .with_storage(samod::storage::InMemoryStorage::new())
            .load()
            .await;
        BigRepo::boot(repo, BigRepoConfig::new("sqlite::memory:".to_string())).await
    }

    async fn sync_partition_once(
        worker: &PeerSyncWorkerHandle,
        dst: &Arc<BigRepo>,
        part_id: &PartitionId,
        cursor: &mut Option<OpaqueCursor>,
        dst_doc_cache: &mut HashMap<String, crate::repo::BigDocHandle>,
    ) -> Res<()> {
        let events = worker
            .get_partition_events(GetPartitionEventsRequest {
                partitions: vec![PartitionCursorRequest {
                    partition_id: part_id.clone(),
                    since: cursor.clone(),
                }],
            })
            .await?;
        let mut to_fetch = Vec::<String>::new();
        for event in &events.events {
            if let Ok(txid) = cursor::to_txid(&event.cursor) {
                if cursor
                    .as_ref()
                    .and_then(|item| cursor::to_txid(item).ok())
                    .map(|cur| txid > cur)
                    .unwrap_or(true)
                {
                    *cursor = Some(event.cursor.clone());
                }
            }
            match &event.kind {
                PartitionEventKind::MemberUpsert { doc_id } => {
                    dst.add_doc_to_partition(part_id, doc_id).await?;
                }
                PartitionEventKind::MemberRemoved { doc_id } => {
                    dst.remove_doc_from_partition(part_id, doc_id).await?;
                }
                PartitionEventKind::DocChanged { doc_id, .. } => {
                    to_fetch.push(doc_id.clone());
                }
                PartitionEventKind::DocDeleted { .. } => {}
            }
        }
        to_fetch.sort();
        to_fetch.dedup();
        for chunk in to_fetch.chunks(128) {
            let docs = worker
                .get_docs_full(GetDocsFullRequest {
                    doc_ids: chunk.to_vec(),
                })
                .await?;
            for doc in docs.docs {
                let parsed = DocumentId::from_str(&doc.doc_id)
                    .map_err(|err| ferr!("invalid remote doc id '{}': {err}", doc.doc_id))?;
                let mut remote_doc = automerge::Automerge::load(&doc.automerge_save)
                    .map_err(|err| ferr!("failed loading remote automerge save: {err}"))?;
                if let Some(existing) = dst_doc_cache.get(&doc.doc_id).cloned() {
                    existing
                        .with_document_local(|local| {
                            local
                                .merge(&mut remote_doc)
                                .expect("failed merging remote doc into local");
                        })
                        .await?;
                } else {
                    let imported = dst.import_doc(parsed, remote_doc).await?;
                    dst_doc_cache.insert(doc.doc_id, imported);
                }
            }
        }
        Ok(())
    }

    async fn drain_progress_events(
        progress_rx: &mut tokio::sync::broadcast::Receiver<PeerSyncProgressEvent>,
    ) {
        loop {
            match progress_rx.try_recv() {
                Ok(event) => eprintln!("[sync-progress] {event:?}"),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(_) => break,
            }
        }
    }
}

pub type OpaqueCursor = String;

pub mod cursor {
    use super::*;

    pub fn from_txid(txid: u64) -> String {
        utils_rs::hash::encode_base58_multibase(txid.to_be_bytes())
    }

    pub fn to_txid(val: &str) -> Res<u64> {
        let raw = utils_rs::hash::decode_base58_multibase(val)
            .wrap_err_with(|| format!("invalid cursor encoding '{}'", val))?;
        let raw: [u8; 8] = raw
            .as_slice()
            .try_into()
            .map_err(|_| ferr!("invalid cursor byte length: expected 8 got {}", raw.len()))?;
        Ok(u64::from_be_bytes(raw))
    }

    #[test]
    fn cursor_roundtrip() {
        let raw = 42_u64;
        let enc = from_txid(raw);
        let dec = to_txid(&enc).unwrap();
        assert_eq!(raw, dec);
    }
}
