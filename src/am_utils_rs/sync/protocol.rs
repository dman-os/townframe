use crate::interlude::*;

use irpc::{channel, rpc_requests};

pub type PartitionId = String;
pub type PeerKey = String;
pub type OpaqueCursor = String;

pub const MAX_GET_DOCS_FULL_DOC_IDS: usize = 256;
pub const DEFAULT_EVENT_PAGE_LIMIT: u32 = 512;
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1024;
pub const DEFAULT_DOC_BATCH_LIMIT: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionSummary {
    pub partition_id: PartitionId,
    pub latest_cursor: OpaqueCursor,
    pub member_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionCursorRequest {
    pub partition_id: PartitionId,
    pub since: Option<OpaqueCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionStreamCursorRequest {
    pub partition_id: PartitionId,
    pub since_member: Option<OpaqueCursor>,
    pub since_doc: Option<OpaqueCursor>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionCursorPage {
    pub partition_id: PartitionId,
    pub next_cursor: Option<OpaqueCursor>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionMemberEvent {
    pub cursor: OpaqueCursor,
    pub partition_id: PartitionId,
    pub deets: PartitionMemberEventDeets,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionEvent {
    pub cursor: OpaqueCursor,
    pub partition_id: PartitionId,
    pub deets: PartitionEventDeets,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionEventDeets {
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
pub enum PartitionMemberEventDeets {
    MemberUpsert { doc_id: String },
    MemberRemoved { doc_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionDocEvent {
    pub cursor: OpaqueCursor,
    pub partition_id: PartitionId,
    pub deets: PartitionDocEventDeets,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionDocEventDeets {
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
pub struct GetPartitionMemberEventsRequest {
    pub partitions: Vec<PartitionCursorRequest>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionMemberEventsResponse {
    pub events: Vec<PartitionMemberEvent>,
    pub cursors: Vec<PartitionCursorPage>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionDocEventsRequest {
    pub partitions: Vec<PartitionCursorRequest>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionDocEventsResponse {
    pub events: Vec<PartitionDocEvent>,
    pub cursors: Vec<PartitionCursorPage>,
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
    pub partitions: Vec<PartitionStreamCursorRequest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubscriptionStreamKind {
    Member,
    Doc,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubscriptionItem {
    MemberEvent(PartitionMemberEvent),
    DocEvent(PartitionDocEvent),
    SnapshotComplete { stream: SubscriptionStreamKind },
    Lagged { dropped: u64 },
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ListPartitionsRpcReq {
    pub peer: PeerKey,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionMemberEventsRpcReq {
    pub peer: PeerKey,
    pub req: GetPartitionMemberEventsRequest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionDocEventsRpcReq {
    pub peer: PeerKey,
    pub req: GetPartitionDocEventsRequest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetDocsFullRpcReq {
    pub peer: PeerKey,
    pub req: GetDocsFullRequest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubPartitionsRpcReq {
    pub peer: PeerKey,
    pub req: SubPartitionsRequest,
}

#[rpc_requests(message = PartitionSyncRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PartitionSyncRpc {
    #[rpc(tx = channel::oneshot::Sender<Result<ListPartitionsResponse, PartitionSyncError>>)]
    ListPartitions(ListPartitionsRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<GetPartitionMemberEventsResponse, PartitionSyncError>>)]
    GetPartitionMemberEvents(GetPartitionMemberEventsRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<GetPartitionDocEventsResponse, PartitionSyncError>>)]
    GetPartitionDocEvents(GetPartitionDocEventsRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<GetDocsFullResponse, PartitionSyncError>>)]
    GetDocsFull(GetDocsFullRpcReq),
    #[rpc(tx = channel::mpsc::Sender<SubscriptionItem>)]
    SubPartitions(SubPartitionsRpcReq),
}

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
