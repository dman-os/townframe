use crate::interlude::*;

pub mod bucket;
pub mod summary;

pub trait BigSyncRpcClient {
    fn get_peer_part_summaries(
        &self,
        req: summary::PeerPartsSummaryRequest,
    ) -> impl Future<
        Output = BigSyncRpcResult<
            Result<summary::PeerPartsSummaryResponse, summary::PeerPartsSummaryError>,
        >,
    > + Send;

    // FIXME(port): the replay methods move onto the service stack binding; their types are in
    // `crate::replay::wire`.
    fn update_replay_session(
        &self,
        req: crate::replay::wire::UpdateSession,
    ) -> impl Future<
        Output = BigSyncRpcResult<
            Result<crate::replay::wire::SessionUpdated, crate::replay::wire::SessionError>,
        >,
    > + Send;

    fn page_replay_session(
        &self,
        req: crate::replay::wire::PageSession,
    ) -> impl Future<
        Output = BigSyncRpcResult<
            Result<crate::replay::wire::SessionPage, crate::replay::wire::SessionError>,
        >,
    > + Send;

    fn close_replay_session(
        &self,
        req: crate::replay::wire::CloseSession,
    ) -> impl Future<Output = BigSyncRpcResult<Result<(), crate::replay::wire::SessionError>>> + Send;

    fn get_changed_buckets(
        &self,
        req: bucket::GetChangedBucketsRequest,
    ) -> impl Future<
        Output = BigSyncRpcResult<
            Result<bucket::GetChangedBucketsResponse, bucket::GetChangedBucketsError>,
        >,
    > + Send;

    /// FIXME: this this warning true?
    /// WARN: this doesn't limit the number of returned results
    /// It only accepts buckets at the level the requesting part advertises in
    /// its per-part [`PartStratSummary::Bucket`] summary.
    fn leaf_buckets(
        &self,
        req: bucket::LeafBucketsRequest,
    ) -> impl Future<
        Output = BigSyncRpcResult<Result<bucket::LeafBucketsResponse, bucket::LeafBucketsError>>,
    > + Send;
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
