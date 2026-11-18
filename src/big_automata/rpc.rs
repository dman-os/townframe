use crate::interlude::*;

pub mod bucket;
pub mod summary;

pub trait BigSyncRpcClient<K: FutureForm> {
    fn get_peer_part_summaries<'a>(
        &'a self,
        req: summary::PeerPartsSummaryRequest,
    ) -> K::Future<
        'a,
        BigSyncRpcResult<Result<summary::PeerPartsSummaryResponse, summary::PeerPartsSummaryError>>,
    >;

    // FIXME(port): the replay methods move onto the service stack binding; their types are in
    // `crate::replay::wire`.
    fn update_replay_session<'a>(
        &'a self,
        req: crate::replay::wire::UpdateSession,
    ) -> K::Future<
        'a,
        BigSyncRpcResult<
            Result<crate::replay::wire::SessionUpdated, crate::replay::wire::SessionError>,
        >,
    >;

    fn page_replay_session<'a>(
        &'a self,
        req: crate::replay::wire::PageSession,
    ) -> K::Future<
        'a,
        BigSyncRpcResult<
            Result<crate::replay::wire::SessionPage, crate::replay::wire::SessionError>,
        >,
    >;

    fn close_replay_session<'a>(
        &'a self,
        req: crate::replay::wire::CloseSession,
    ) -> K::Future<'a, BigSyncRpcResult<Result<(), crate::replay::wire::SessionError>>>;

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
    ) -> K::Future<
        'a,
        BigSyncRpcResult<Result<bucket::LeafBucketsResponse, bucket::LeafBucketsError>>,
    >;
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
