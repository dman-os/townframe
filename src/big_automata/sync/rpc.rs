//! The calls the per-peer machine makes besides the replay session: part summaries, bucket
//! listing and bucket leaves. The replay methods are served by `ReplayProtocol`; the three here
//! are the sibling [`SyncRpcProtocol`], answered by [`responder::RpcResponder`] and bound onto
//! the service stack by `big_automata_iroh`.
//!
//! # Laws
//!
//! 1. **Answers are per principal, and refusal looks like absence.** Every answer is read for
//!    the asking peer from `PartStoreServe`: a part it may not read and a part the store does
//!    not know answer alike, so no answer confirms that a part exists.
//! 2. **One snapshot per answer.** Access is decided and rows are read under one snapshot, so a
//!    grant or revocation never lands between deciding and reading: each answer is exactly one
//!    store call.
//! 3. **Summaries are per part.** The answer holds a `PartSummary` for each readable part
//!    asked and nothing for the rest. A request naming more than [`MAX_SUMMARY_PARTS`] parts is
//!    refused whole with `TooManyParts { cap }`, before the store is read; the asker chunks
//!    (PartLifecycle law 4). A request at the cap is answered.
//! 4. **Bucket listing** returns the part's non-empty buckets from `offset` through `to_level`,
//!    in bucket order, changed after `since`, `limit_hint` of them (at most
//!    [`MAX_LIST_BUCKETS`]) plus the remaining siblings of the last; a part the asker may not
//!    read answers `UnknownPart`, at any limit.
//! 5. **Leaves** return, per asked bucket, its member entries (live with their fingerprint under
//!    the request's seed, dead without) after `after`, at most the limit (the hint, at least 1
//!    and at most the responder's cap, which is at most [`MAX_LEAF_PAGE`]; a hint above the
//!    cap is capped, not refused), each page echoing the `after` it answers and carrying
//!    `next_after` exactly when the bucket has more (the walk drops a redelivered page by the
//!    echo). A bucket unchanged past `since` answers an empty page.
//!    A request naming more than [`MAX_LEAF_BUCKETS`] buckets is refused whole with
//!    `TooManyBuckets { cap }` before the store is read, and the asker never sends one.
//!    A request that is malformed whatever the store holds (a bucket id beyond the deepest level
//!    or not canonical, an `after` that is not in its bucket, a listing offset beyond the deepest
//!    level) is refused whole as `Invalid` before the store is read, which discloses nothing
//!    about any part.
//! 6. **Binding.** The three methods are a sibling protocol of `ReplayProtocol` on the same
//!    service stack and connection, so the replay protocol's spec and conformance stay as
//!    they are. The wire types are the store's (`PartSummary`, `BucketSummary`, leaf entries);
//!    the replay methods of [`BigSyncRpcClient`] delegate to the replay protocol's calls.
//!
//! # Testing
//!
//! Law tests against `MemoryPartStore` in [`responder`]'s tests, a differential proptest of the
//! responder against the part oracle over random stores, grants and requests, and one end to
//! end test per binding in `big_automata_iroh`.

use crate::interlude::*;

pub mod bucket;
pub mod responder;
pub mod summary;

/// Ceiling on the parts one summary request may name. Remote input: a peer that names
/// thousands of parts would make the responder do store work for each. Honest requests name a
/// peer's newly pending parts, orders of magnitude below it.
pub const MAX_SUMMARY_PARTS: usize = 64;

/// Ceiling on a bucket listing's `limit_hint`.
pub const MAX_LIST_BUCKETS: u32 = 1024;

/// Ceiling on the buckets one leaf request may name. Remote input: each bucket answers a page
/// of up to [`MAX_LEAF_PAGE`] entries, so a request is bounded by this times that. The old
/// `big_sync` responder had no such cap, only the page-size ceiling. The asker
/// (`BucketMachine`) splits a larger selection across requests.
pub const MAX_LEAF_BUCKETS: usize = 64;

/// Ceiling on the entries of one leaf page, whatever the hint.
pub const MAX_LEAF_PAGE: u32 = 1024;

// Reads, but procedures: their parameters are maps and lists, which a query string cannot carry.
service_core_rs::service_method!(summary::PartSummariesRequest => summary::PartSummariesResponse, summary::PartSummariesError, procedure("org.townframe.bigsync.rpc.partSummaries"));
service_core_rs::service_method!(bucket::GetChangedBucketsRequest => bucket::GetChangedBucketsResponse, bucket::BucketsError, procedure("org.townframe.bigsync.rpc.changedBuckets"));
service_core_rs::service_method!(bucket::LeafBucketsRequest => bucket::LeafBucketsResponse, bucket::BucketsError, procedure("org.townframe.bigsync.rpc.leafBuckets"));

service_core_rs::declare_service_protocol! {
    pub enum SyncRpcProtocol => SyncRpcMessage {
        unary {
            PartSummaries(summary::PartSummariesRequest),
            ChangedBuckets(bucket::GetChangedBucketsRequest),
            LeafBuckets(bucket::LeafBucketsRequest)
        }
        stream {}
    }
}

/// What the per-peer machine's driver calls on a peer: the three calls of [`SyncRpcProtocol`]
/// and the three of the replay protocol.
pub trait BigSyncRpcClient {
    fn part_summaries(
        &self,
        req: summary::PartSummariesRequest,
    ) -> impl Future<
        Output = BigSyncRpcResult<
            Result<summary::PartSummariesResponse, summary::PartSummariesError>,
        >,
    > + Send;

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
        Output = BigSyncRpcResult<Result<bucket::GetChangedBucketsResponse, bucket::BucketsError>>,
    > + Send;

    /// A page per asked bucket, at most the responder's cap of entries each: a hint above the
    /// cap is capped, so a walk follows `next_after` until a page finishes its bucket.
    fn leaf_buckets(
        &self,
        req: bucket::LeafBucketsRequest,
    ) -> impl Future<
        Output = BigSyncRpcResult<Result<bucket::LeafBucketsResponse, bucket::BucketsError>>,
    > + Send;
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum RpcError {
    /// TransportError {0}
    TransportError(String),
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
