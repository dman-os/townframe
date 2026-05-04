use crate::interlude::*;

use irpc::{channel, rpc_requests};

// FIXME: make these either a [u8; 32] or Arc<str>, they get cloned
// incredilbly frequently
pub type PartitionId = String;
pub type PeerKey = Arc<str>;
pub type CursorIndex = u64;

pub const DEFAULT_EVENT_PAGE_LIMIT: u32 = 512;
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionSummary {
    pub partition_id: PartitionId,
    pub latest_cursor: CursorIndex,
    pub member_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionCursorRequest {
    pub partition_id: PartitionId,
    pub since: Option<CursorIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionStreamCursorRequest {
    pub partition_id: PartitionId,
    pub since_member: Option<CursorIndex>,
    pub since_item: Option<CursorIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionCursorPage {
    pub partition_id: PartitionId,
    pub next_cursor: Option<CursorIndex>,
    // FIXME: do we need has_more where next_cursor being optional will suffice?
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionMemberEvent {
    pub cursor: CursorIndex,
    pub partition_id: PartitionId,
    pub deets: PartitionMemberEventDeets,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionEvent {
    pub cursor: CursorIndex,
    pub partition_id: PartitionId,
    pub deets: PartitionEventDeets,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionEventDeets {
    MemberUpsert {
        item_id: String,
    },
    MemberRemoved {
        item_id: String,
    },
    ItemChanged {
        item_id: String,
        payload: serde_json::Value,
    },
    ItemDeleted {
        item_id: String,
        payload: serde_json::Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionMemberEventDeets {
    MemberUpsert { item_id: String },
    MemberRemoved { item_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionItemEvent {
    pub cursor: CursorIndex,
    pub partition_id: PartitionId,
    pub deets: PartitionItemEventDeets,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PartitionItemEventDeets {
    ItemChanged { item_id: String, payload: String },
    ItemDeleted { item_id: String, payload: String },
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
pub struct GetPartitionItemEventsRequest {
    pub partitions: Vec<PartitionCursorRequest>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionItemEventsResponse {
    pub events: Vec<PartitionItemEvent>,
    pub cursors: Vec<PartitionCursorPage>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubPartitionsRequest {
    pub partitions: Vec<PartitionStreamCursorRequest>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum SubscriptionStreamKind {
    Member,
    Item,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SubscriptionItem {
    MemberEvent(PartitionMemberEvent),
    ItemEvent(PartitionItemEvent),
    ReplayComplete { stream: SubscriptionStreamKind },
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
    InvalidCursor { cursor: CursorIndex },
    /// unknown partition {partition_id:?}
    UnknownPartition { partition_id: PartitionId },
    /// internal error: {message}
    Internal { message: String },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ListPartitionsRpcReq;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionMemberEventsRpcReq {
    pub req: GetPartitionMemberEventsRequest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionItemEventsRpcReq {
    pub req: GetPartitionItemEventsRequest,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubPartitionsRpcReq {
    pub req: SubPartitionsRequest,
}

// NOTE: we're using 0rtt verson iroh irpc which is only
// safe when these requests are idempotent safe. amend this
// if adding new requests that aren't idempotent
#[rpc_requests(message = PartitionSyncRpcMessage)]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PartitionSyncRpc {
    #[rpc(tx = channel::oneshot::Sender<Result<ListPartitionsResponse, PartitionSyncError>>)]
    ListPartitions(ListPartitionsRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<GetPartitionMemberEventsResponse, PartitionSyncError>>)]
    GetPartitionMemberEvents(GetPartitionMemberEventsRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<GetPartitionItemEventsResponse, PartitionSyncError>>)]
    GetPartitionItemEvents(GetPartitionItemEventsRpcReq),
    #[rpc(tx = channel::mpsc::Sender<SubscriptionItem>)]
    SubPartitions(SubPartitionsRpcReq),
}
