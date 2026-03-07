use crate::interlude::*;

mod node;
mod partition;
mod peer;

use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct OpaqueCursor(pub String);

impl OpaqueCursor {
    pub fn from_txid(txid: u64) -> Self {
        Self(utils_rs::hash::encode_base58_multibase(txid.to_be_bytes()))
    }

    pub fn to_txid(&self) -> Res<u64> {
        let raw = utils_rs::hash::decode_base58_multibase(&self.0)
            .wrap_err_with(|| format!("invalid cursor encoding '{}'", self.0))?;
        let raw: [u8; 8] = raw
            .as_slice()
            .try_into()
            .map_err(|_| ferr!("invalid cursor byte length: expected 8 got {}", raw.len()))?;
        Ok(u64::from_be_bytes(raw))
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct PartitionId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PeerKey(pub String);

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

#[derive(Debug, thiserror::Error, displaydoc::Display, Clone, PartialEq, Eq)]
pub enum PartitionSyncError {
    /// access denied for partition {partition_id:?}
    AccessDenied { partition_id: PartitionId },
    /// invalid cursor {cursor:?}
    InvalidCursor { cursor: OpaqueCursor },
    /// requested too many docs: requested={requested} max={max}
    TooManyDocIds { requested: usize, max: usize },
    /// unknown partition {partition_id:?}
    UnknownPartition { partition_id: PartitionId },
    /// internal error: {message}
    Internal { message: String },
}

impl PartitionSyncError {
    fn into_report(self) -> eyre::Report {
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

#[async_trait]
pub trait PartitionSyncProvider: Send + Sync + 'static {
    async fn list_partitions_for_peer(&self, peer: &PeerKey) -> Res<Vec<PartitionSummary>>;
    async fn get_partition_events(
        &self,
        peer: &PeerKey,
        reqs: &[PartitionCursorRequest],
    ) -> Res<Vec<PartitionEvent>>;
    async fn get_docs_full(&self, peer: &PeerKey, doc_ids: &[String]) -> Res<Vec<FullDoc>>;
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

    use partition::*;

    #[test]
    fn cursor_roundtrip() {
        let raw = 42_u64;
        let enc = OpaqueCursor::from_txid(raw);
        let dec = enc.to_txid().unwrap();
        assert_eq!(raw, dec);
    }

    #[tokio::test]
    async fn get_partition_events_since_cursor_works() {
        let provider = StaticPartitionSyncProvider::new();
        let peer = PeerKey("peer-a".into());

        let first = provider.upsert_member("p1", "d1").await;
        provider
            .emit_doc_changed("p1", "d1", vec!["h1".into()], 1)
            .await;
        provider.remove_member("p1", "d2").await;

        let response = provider
            .get_partition_events(
                &peer,
                &[PartitionCursorRequest {
                    partition_id: PartitionId("p1".into()),
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
        let peer = PeerKey("peer-a".into());
        provider.upsert_member("p1", "d1").await;

        let mut sub = provider
            .subscribe(
                &peer,
                &[PartitionCursorRequest {
                    partition_id: PartitionId("p1".into()),
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
            .emit_doc_changed("p1", "d1", vec!["head2".into()], 2)
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
}
