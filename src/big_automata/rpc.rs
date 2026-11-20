use crate::interlude::*;

pub mod bucket;
pub mod replay;
pub mod summary;

pub trait BigSyncRpcClient<K: FutureForm> {
    fn get_peer_part_summaries<'a>(
        &'a self,
        req: summary::PeerPartsSummaryRequest,
    ) -> K::Future<
        'a,
        BigSyncRpcResult<Result<summary::PeerPartsSummaryResponse, summary::PeerPartsSummaryError>>,
    >;

    fn update_replay_session<'a>(
        &'a self,
        req: replay::UpdateReplaySessionRequest,
    ) -> K::Future<'a, BigSyncRpcResult<replay::UpdateReplaySessionResponse>>;

    fn page_replay_session<'a>(
        &'a self,
        req: replay::PageReplaySessionRequest,
    ) -> K::Future<'a, BigSyncRpcResult<replay::PageReplaySessionRequest>>;

    fn close_replay_session<'a>(
        &'a self,
        req: replay::CloseReplaySession,
    ) -> K::Future<'a, BigSyncRpcResult<()>>;

    fn get_changed_buckets<'a>(
        &'a self,
        req: bucket::GetChangedBucketsRequest,
    ) -> K::Future<
        'a,
        BigSyncRpcResult<Result<bucket::GetChangedBucketsResponse, bucket::GetChangedBucketsError>>,
    >;

    /// FIXME: this this warning true?
    /// WARN: this doesn't limit the number of returned results
    /// It only accepts buckets at the level the requesting part advertises in
    /// its per-part [`PartStratSummary::Bucket`] summary.
    fn leaf_buckets<'a>(
        &'a self,
        req: bucket::LeafBucketsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<bucket::LeafBucketResult, bucket::LeafBucketsError>>>;
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum RpcError {
    /// TransportError
    TransportError,
    /// InvalidRequest {0}
    InvalidRequest(String),
    /// Unauthorized
    Unauthorized,
    /// UnknownSubscription
    UnknownSubscription,
    /// SubscriptionLimit
    SubscriptionLimit,
    /// Internal
    Internal,
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;
