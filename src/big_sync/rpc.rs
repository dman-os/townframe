use crate::interlude::*;

use big_sync_core::{
    mpsc::Receiver,
    rpc::{
        BigSyncRpcResult, BucketSummary, GetChangedBucketsRequest, LeafBucketsError,
        LeafBucketsRequest, LeafBucketResult, ListPartsError, PeerSummaryRequest,
        PeerSummaryResult, SubEvent, SubPartsRequest,
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

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>>;

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>>;
}
