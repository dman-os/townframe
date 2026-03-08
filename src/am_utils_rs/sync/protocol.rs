use crate::sync::{
    GetDocsFullRequest, GetDocsFullResponse, GetPartitionEventsRequest, GetPartitionEventsResponse,
    ListPartitionsResponse, PartitionSyncError, PeerKey, SubPartitionsRequest, SubscriptionItem,
};

use irpc::{channel, rpc_requests};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ListPartitionsRpcReq {
    pub peer: PeerKey,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetPartitionEventsRpcReq {
    pub peer: PeerKey,
    pub req: GetPartitionEventsRequest,
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
    #[rpc(tx = channel::oneshot::Sender<Result<GetPartitionEventsResponse, PartitionSyncError>>)]
    GetPartitionEvents(GetPartitionEventsRpcReq),
    #[rpc(tx = channel::oneshot::Sender<Result<GetDocsFullResponse, PartitionSyncError>>)]
    GetDocsFull(GetDocsFullRpcReq),
    #[rpc(tx = channel::mpsc::Sender<SubscriptionItem>)]
    SubPartitions(SubPartitionsRpcReq),
}
