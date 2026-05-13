use crate::interlude::*;

use big_sync_core::{
    mpsc::Receiver,
    rpc::{
        BigSyncRpcResult, ListPartsError, MerkleBucketsRequest, MerkleBucketsResponse,
        MerkleLeafItemsRequest, MerkleLeafItemsResponse, PeerSummaryRequest, PeerSummaryResult,
        SubEvent, SubPartsRequest,
    },
};

#[async_trait]
pub trait HostBigRpcClient: Send + Sync {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<Receiver<SubEvent>, ListPartsError>>>;

    async fn merkle_buckets(
        &self,
        req: MerkleBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<MerkleBucketsResponse, ListPartsError>>>;

    async fn merkle_leaf_items(
        &self,
        req: MerkleLeafItemsRequest,
    ) -> Res<BigSyncRpcResult<Result<MerkleLeafItemsResponse, ListPartsError>>>;
}
