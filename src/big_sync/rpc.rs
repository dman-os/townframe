use crate::interlude::*;

use big_sync_core::{
    mpsc::Receiver,
    rpc::{
        BigSyncRpcResult, PeerSummaryRequest, PeerSummaryResult, SubEvent, SubPartsError,
        SubPartsRequest,
    },
};

#[async_trait]
pub trait HostBigRpcClient: Send + Sync {
    async fn peer_summary(
        &self,
        req: PeerSummaryRequest,
    ) -> Res<BigSyncRpcResult<PeerSummaryResult>>;

    async fn sub_parts(
        &self,
        req: SubPartsRequest,
    ) -> Res<BigSyncRpcResult<Result<Receiver<SubEvent>, SubPartsError>>>;
}
