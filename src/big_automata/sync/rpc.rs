//! The calls the per-peer machine makes besides the replay session: part summaries, bucket
//! listing and bucket leaves. Draft laws for the responder and its binding; the replay methods
//! below are already served by `ReplayProtocol` (`big_automata_iroh`).
//!
//! # Laws
//!
//! 1. **Answers are per principal, and refusal looks like absence.** Every answer is read for
//!    the asking peer from `PartStoreServe`: a part it may not read and a part the store does
//!    not know answer alike, so no answer confirms that a part exists.
//! 2. **One snapshot per answer.** Access is decided and rows are read under one snapshot, so a
//!    grant or revocation never lands between deciding and reading.
//! 3. **Summaries are per part.** The answer holds a `PartSummary` for each readable part
//!    asked and nothing for the rest. A request naming more than `MAX_SUMMARY_PARTS` parts is
//!    refused whole with `TooManyParts { cap }`; the asker chunks (PartLifecycle law 4).
//! 4. **Bucket listing** returns the part's buckets at the asked working level from `offset`, in
//!    order, changed after `since`, at most the limit; a refused part answers `UnknownPart`.
//! 5. **Leaves** return, per asked bucket, its member entries after `after`, at most the limit,
//!    each page echoing the `after` it answers (the walk drops a redelivered page by it).
//! 6. **Binding.** The three methods are a sibling protocol of `ReplayProtocol` on the same
//!    service stack and connection, so the replay protocol's spec and conformance stay as
//!    they are. The wire types are the store's (`PartSummary`, `BucketSummary`, leaf entries);
//!    the older summary types in `rpc/summary.rs` go.

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
