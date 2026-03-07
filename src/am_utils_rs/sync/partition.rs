use crate::interlude::*;

use crate::sync::{
    FullDoc, OpaqueCursor, PartitionCursorRequest, PartitionEvent, PartitionEventKind, PartitionId,
    PartitionSubscription, PartitionSummary, PartitionSyncError, PartitionSyncProvider, PeerKey,
    SubscriptionItem, DEFAULT_SUBSCRIPTION_CAPACITY, MAX_GET_DOCS_FULL_DOC_IDS,
};

use tokio::sync::{broadcast, mpsc, RwLock};

#[derive(Clone)]
pub struct StaticPartitionSyncProvider {
    state: Arc<RwLock<StaticState>>,
}

#[derive(Debug, Default, Clone)]
struct PartitionState {
    members: HashSet<String>,
    membership_log: Vec<MembershipLogEntry>,
    doc_log: Vec<DocLogEntry>,
    doc_state: HashMap<String, DocSnapshot>,
}

struct StaticState {
    next_txid: u64,
    partitions: HashMap<PartitionId, PartitionState>,
    docs_full: HashMap<String, Vec<u8>>,
    live_tx: broadcast::Sender<PartitionEvent>,
}

impl StaticState {
    fn alloc_txid(&mut self) -> u64 {
        let out = self.next_txid;
        self.next_txid = self.next_txid.saturating_add(1);
        out
    }

    fn partition_mut(&mut self, partition_id: &PartitionId) -> &mut PartitionState {
        self.partitions.entry(partition_id.clone()).or_default()
    }
}

impl Default for StaticPartitionSyncProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl StaticPartitionSyncProvider {
    pub fn new() -> Self {
        let (live_tx, _) = broadcast::channel(DEFAULT_SUBSCRIPTION_CAPACITY);
        Self {
            state: Arc::new(RwLock::new(StaticState {
                next_txid: 1,
                partitions: HashMap::new(),
                docs_full: HashMap::new(),
                live_tx,
            })),
        }
    }

    pub async fn set_full_doc(&self, doc_id: impl Into<String>, automerge_save: Vec<u8>) {
        let mut state = self.state.write().await;
        state.docs_full.insert(doc_id.into(), automerge_save);
    }

    pub async fn upsert_member(
        &self,
        partition_id: impl Into<String>,
        doc_id: impl Into<String>,
    ) -> PartitionEvent {
        let partition_id = PartitionId(partition_id.into());
        let doc_id = doc_id.into();
        let mut state = self.state.write().await;
        let txid = state.alloc_txid();
        let partition = state.partition_mut(&partition_id);
        partition.members.insert(doc_id.clone());
        partition.membership_log.push(MembershipLogEntry {
            txid,
            doc_id: doc_id.clone(),
            kind: MembershipKind::Upsert,
        });
        let out = PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::MemberUpsert { doc_id },
        };
        let _ = state.live_tx.send(out.clone());
        out
    }

    pub async fn remove_member(
        &self,
        partition_id: impl Into<String>,
        doc_id: impl Into<String>,
    ) -> PartitionEvent {
        let partition_id = PartitionId(partition_id.into());
        let doc_id = doc_id.into();
        let mut state = self.state.write().await;
        let txid = state.alloc_txid();
        let partition = state.partition_mut(&partition_id);
        partition.members.remove(&doc_id);
        partition.membership_log.push(MembershipLogEntry {
            txid,
            doc_id: doc_id.clone(),
            kind: MembershipKind::Removed,
        });
        let out = PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::MemberRemoved { doc_id },
        };
        let _ = state.live_tx.send(out.clone());
        out
    }

    pub async fn emit_doc_changed(
        &self,
        partition_id: impl Into<String>,
        doc_id: impl Into<String>,
        heads: Vec<String>,
        change_count_hint: u64,
    ) -> PartitionEvent {
        let partition_id = PartitionId(partition_id.into());
        let doc_id = doc_id.into();
        let mut state = self.state.write().await;
        let txid = state.alloc_txid();
        let partition = state.partition_mut(&partition_id);
        partition.doc_state.insert(
            doc_id.clone(),
            DocSnapshot {
                heads: heads.clone(),
                change_count_hint,
                deleted: false,
            },
        );
        partition.doc_log.push(DocLogEntry {
            txid,
            doc_id: doc_id.clone(),
            heads: heads.clone(),
            change_count_hint,
            kind: DocKind::Changed,
        });
        let out = PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::DocChanged {
                doc_id,
                heads,
                change_count_hint,
            },
        };
        let _ = state.live_tx.send(out.clone());
        out
    }

    pub async fn emit_doc_deleted(
        &self,
        partition_id: impl Into<String>,
        doc_id: impl Into<String>,
        change_count_hint: u64,
    ) -> PartitionEvent {
        let partition_id = PartitionId(partition_id.into());
        let doc_id = doc_id.into();
        let mut state = self.state.write().await;
        let txid = state.alloc_txid();
        let partition = state.partition_mut(&partition_id);
        partition.doc_state.insert(
            doc_id.clone(),
            DocSnapshot {
                heads: vec![],
                change_count_hint,
                deleted: true,
            },
        );
        partition.doc_log.push(DocLogEntry {
            txid,
            doc_id: doc_id.clone(),
            heads: vec![],
            change_count_hint,
            kind: DocKind::Deleted,
        });
        let out = PartitionEvent {
            cursor: OpaqueCursor::from_txid(txid),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::DocDeleted {
                doc_id,
                change_count_hint,
            },
        };
        let _ = state.live_tx.send(out.clone());
        out
    }
}

#[async_trait]
impl PartitionSyncProvider for StaticPartitionSyncProvider {
    async fn list_partitions_for_peer(&self, _peer: &PeerKey) -> Res<Vec<PartitionSummary>> {
        let state = self.state.read().await;
        let mut out = Vec::with_capacity(state.partitions.len());
        for (partition_id, partition) in &state.partitions {
            let latest = latest_partition_txid(partition);
            out.push(PartitionSummary {
                partition_id: partition_id.clone(),
                latest_cursor: OpaqueCursor::from_txid(latest),
                member_count: partition.members.len() as u64,
            });
        }
        out.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
        Ok(out)
    }

    async fn get_partition_events(
        &self,
        _peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
    ) -> Res<Vec<PartitionEvent>> {
        let state = self.state.read().await;
        let mut out = Vec::new();
        for req in reqs {
            let partition = state.partitions.get(&req.partition_id).ok_or_else(|| {
                PartitionSyncError::UnknownPartition {
                    partition_id: req.partition_id.clone(),
                }
                .into_report()
            })?;
            if let Some(since) = &req.since {
                let since_txid = since.to_txid().map_err(|_| {
                    PartitionSyncError::InvalidCursor {
                        cursor: since.clone(),
                    }
                    .into_report()
                })?;
                append_replay_events(partition, &req.partition_id, since_txid, &mut out);
            } else {
                append_snapshot_events(partition, &req.partition_id, &mut out);
            }
        }
        out.sort_by(cmp_partition_events);
        Ok(out)
    }

    async fn get_docs_full(&self, _peer: &PeerKey, doc_ids: &[String]) -> Res<Vec<FullDoc>> {
        if doc_ids.len() > MAX_GET_DOCS_FULL_DOC_IDS {
            return Err(PartitionSyncError::TooManyDocIds {
                requested: doc_ids.len(),
                max: MAX_GET_DOCS_FULL_DOC_IDS,
            }
            .into_report());
        }
        let state = self.state.read().await;
        let mut out = Vec::new();
        for doc_id in doc_ids {
            if let Some(bytes) = state.docs_full.get(doc_id) {
                out.push(FullDoc {
                    doc_id: doc_id.clone(),
                    automerge_save: bytes.clone(),
                });
            }
        }
        Ok(out)
    }

    async fn subscribe(
        &self,
        peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
        capacity: usize,
    ) -> Res<PartitionSubscription> {
        let mut live_rx = {
            let state = self.state.read().await;
            state.live_tx.subscribe()
        };
        let replay = self.get_partition_events(peer, reqs).await?;
        let high_watermark = replay
            .iter()
            .filter_map(|event| event.cursor.to_txid().ok())
            .max()
            .unwrap_or(0);
        let requested_partitions: HashSet<PartitionId> =
            reqs.iter().map(|part| part.partition_id.clone()).collect();
        let (tx, rx) = mpsc::channel(capacity.max(1));
        tokio::spawn(async move {
            for event in replay {
                if tx.send(SubscriptionItem::Event(event)).await.is_err() {
                    return;
                }
            }
            if tx.send(SubscriptionItem::SnapshotComplete).await.is_err() {
                return;
            }
            while let Ok(event) = live_rx.recv().await {
                if !requested_partitions.contains(&event.partition_id) {
                    continue;
                }
                let Ok(txid) = event.cursor.to_txid() else {
                    continue;
                };
                if txid <= high_watermark {
                    continue;
                }
                if tx.send(SubscriptionItem::Event(event)).await.is_err() {
                    return;
                }
            }
        });
        Ok(PartitionSubscription { rx })
    }
}

#[derive(Debug, Clone)]
struct MembershipLogEntry {
    txid: u64,
    doc_id: String,
    kind: MembershipKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MembershipKind {
    Upsert,
    Removed,
}

#[derive(Debug, Clone)]
struct DocLogEntry {
    txid: u64,
    doc_id: String,
    heads: Vec<String>,
    change_count_hint: u64,
    kind: DocKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DocKind {
    Changed,
    Deleted,
}

#[derive(Debug, Clone)]
struct DocSnapshot {
    heads: Vec<String>,
    change_count_hint: u64,
    deleted: bool,
}

fn latest_partition_txid(partition: &PartitionState) -> u64 {
    let mut out = 0;
    if let Some(last) = partition.membership_log.last() {
        out = out.max(last.txid);
    }
    if let Some(last) = partition.doc_log.last() {
        out = out.max(last.txid);
    }
    out
}

fn append_snapshot_events(
    partition: &PartitionState,
    partition_id: &PartitionId,
    out: &mut Vec<PartitionEvent>,
) {
    let snapshot_cursor = OpaqueCursor::from_txid(latest_partition_txid(partition));
    for doc_id in partition.members.iter().cloned() {
        out.push(PartitionEvent {
            cursor: snapshot_cursor.clone(),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::MemberUpsert { doc_id },
        });
    }
    for (doc_id, snapshot) in &partition.doc_state {
        if snapshot.deleted {
            continue;
        }
        out.push(PartitionEvent {
            cursor: snapshot_cursor.clone(),
            partition_id: partition_id.clone(),
            kind: PartitionEventKind::DocChanged {
                doc_id: doc_id.clone(),
                heads: snapshot.heads.clone(),
                change_count_hint: snapshot.change_count_hint,
            },
        });
    }
}

fn append_replay_events(
    partition: &PartitionState,
    partition_id: &PartitionId,
    since_txid: u64,
    out: &mut Vec<PartitionEvent>,
) {
    for entry in partition
        .membership_log
        .iter()
        .filter(|entry| entry.txid > since_txid)
    {
        let kind = match entry.kind {
            MembershipKind::Upsert => PartitionEventKind::MemberUpsert {
                doc_id: entry.doc_id.clone(),
            },
            MembershipKind::Removed => PartitionEventKind::MemberRemoved {
                doc_id: entry.doc_id.clone(),
            },
        };
        out.push(PartitionEvent {
            cursor: OpaqueCursor::from_txid(entry.txid),
            partition_id: partition_id.clone(),
            kind,
        });
    }
    for entry in partition
        .doc_log
        .iter()
        .filter(|entry| entry.txid > since_txid)
    {
        let kind = match entry.kind {
            DocKind::Changed => PartitionEventKind::DocChanged {
                doc_id: entry.doc_id.clone(),
                heads: entry.heads.clone(),
                change_count_hint: entry.change_count_hint,
            },
            DocKind::Deleted => PartitionEventKind::DocDeleted {
                doc_id: entry.doc_id.clone(),
                change_count_hint: entry.change_count_hint,
            },
        };
        out.push(PartitionEvent {
            cursor: OpaqueCursor::from_txid(entry.txid),
            partition_id: partition_id.clone(),
            kind,
        });
    }
}

fn cmp_partition_events(left: &PartitionEvent, right: &PartitionEvent) -> std::cmp::Ordering {
    let left_txid = left.cursor.to_txid().unwrap_or(0);
    let right_txid = right.cursor.to_txid().unwrap_or(0);
    left_txid
        .cmp(&right_txid)
        .then_with(|| left.partition_id.cmp(&right.partition_id))
        .then_with(|| event_kind_order(&left.kind).cmp(&event_kind_order(&right.kind)))
}

fn event_kind_order(kind: &PartitionEventKind) -> u8 {
    match kind {
        PartitionEventKind::MemberUpsert { .. } => 1,
        PartitionEventKind::MemberRemoved { .. } => 2,
        PartitionEventKind::DocChanged { .. } => 3,
        PartitionEventKind::DocDeleted { .. } => 4,
    }
}
