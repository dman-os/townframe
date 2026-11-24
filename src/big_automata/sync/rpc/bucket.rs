//! The bucket calls: listing a part's changed buckets and paging a bucket's members.

use crate::interlude::*;

use crate::fingerprint::Fingerprint;
use crate::fingerprint::FingerprintSeed;
use crate::part_store::PayloadDigest;
use crate::part_store::bucket_summary::BucketSummary;
use crate::rev::Rev;

/// Changed buckets are returned for every level from [`Self::offset`].level()
/// through to [`Self::to_level`], in bucket order.
///
/// If a level is exhausted, the next depth is started from the first bucket of that level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetChangedBucketsRequest {
    pub part_key: PartKey,
    pub offset: BuckId,
    /// The deepest level the response may contain.
    pub to_level: BuckLevel,
    /// A mutation stamps every level's `changed_at`, filtering on
    /// the last replay cursor returns only the buckets whose ranges would differ.
    pub since: Rev,
    /// Soft limit on the number of buckets returned, capped at
    /// [`MAX_LIST_BUCKETS`](super::MAX_LIST_BUCKETS).
    /// Extra headroom of [`BuckId::ARITY`] is allowed to return
    /// all qualifying sibling buckets of the last one since that'd just be
    /// at most an extra 15 buckets.
    pub limit_hint: u32,
}

pub type GetChangedBucketsResponse = Vec<BucketSummary>;

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct LeafBucketsRequest {
        pub part_key: PartKey,
        /// A bucket unchanged past this cursor answers no entries.
        pub since: Rev,
        pub buckets: Vec<pub struct LeafBucketRequest {
            pub buck_id: BuckId,
            pub after: Option<ObjKey>,
        }>,
        /// Keys the entries' fingerprints; fresh per request so no collision is precomputed.
        pub seed: FingerprintSeed,
        /// Per bucket entry limit hint.
        /// None means no preference, and the impl returns the smallest useful page of one entry.
        /// The responder might have a hard cap but asking above the cap should just cap
        /// rather than error.
        pub limit_hint: Option<u32>,
    }
}

/// One page per requested bucket. The requester keeps the seed it sent, so the answer does not
/// echo it.
pub type LeafBucketsResponse = Map<BuckId, LeafBucketPage>;

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct LeafBucketPage {
        /// The `after` of the request this page answers, echoed so the walk can drop a page
        /// that answers a request it no longer has outstanding.
        pub after: Option<ObjKey>,
        pub entries: Vec<pub struct BucketObjPageEntry {
            pub obj_key: ObjKey,
            /// Over the object's payload digest, under the request's seed (see
            /// [`BucketObjPageEntry::fingerprint`]); `None` for a removed (dead) member, which has
            /// no content to compare.
            pub fp: Option<Fingerprint<(&'static str, ObjKey, PayloadDigest)>>,
        }>,
        /// Where the next page of this bucket starts; `None` when this page finishes it.
        pub next_after: Option<ObjKey>,
    }
}

impl BucketObjPageEntry {
    /// A leaf entry's fingerprint: the object and its payload digest under the request's seed.
    /// Both sides compute it from the digest stored at write time, so neither rereads payloads.
    pub fn fingerprint(
        seed: &FingerprintSeed,
        obj_key: &ObjKey,
        digest: PayloadDigest,
    ) -> Fingerprint<(&'static str, ObjKey, PayloadDigest)> {
        Fingerprint::new(seed, &("big-sync-obj-fp-v2", obj_key.clone(), digest))
    }
}

/// Why a bucket call was not answered. Shared by listing and leaves: both are per part.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum BucketsError {
    /// The part is unknown, or the requester may not read it: both answer alike, so a refusal
    /// does not confirm the part exists.
    UnknownPart,
    /// the request names more buckets than the responder answers at once; none was answered
    TooManyBuckets { cap: usize },
    /// the request is malformed: {0}
    Invalid(String),
    /// the request context failed: {0}
    Context(#[from] method_core::ContextError),
}
