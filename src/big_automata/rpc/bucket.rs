use crate::interlude::*;

use crate::fingerprint::{Fingerprint, FingerprintSeed};
use crate::ids::BuckId;
use crate::store::ObjPayload;
use crate::store::bucket::BucketSummary;

/// Changed buckets are returned for every level from [`Self::offset`].level()
/// through to [`Self::to_level`], in bucket order.
///
/// If a level is exhausted, the next depth is started from the first bucket of that level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetChangedBucketsRequest {
    pub part_id: PartKey,
    pub offset: BuckId,
    /// The deepest level the response may contain.
    pub to_level: BuckLevel,
    /// A mutation stamps every level's `changed_at`, filtering on
    /// the last replay cursor returns only the buckets whose ranges would differ.
    pub since: CursorIndex,
    /// Soft limit on the number of buckets returned.
    /// Extra headroom of [`BuckId::ARITY`] is allowed to return
    /// all qualifying sibling buckets of the last one since that'd just be
    /// at most an extra 15 buckets.
    pub limit_hint: u32,
}

pub type GetChangedBucketsResponse = Vec<BucketSummary>;

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum GetChangedBucketsError {
    /// UnkownPart
    UnkownPart,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct LeafBucketsRequest {
        pub part_id: PartKey,
        pub since: CursorIndex,
        pub buckets: Vec<pub struct LeafBucketRequest {
            pub buck_id: BuckId,
            pub after: Option<ObjKey>,
        }>,
        pub seed: FingerprintSeed,
        /// Per bucket entry entry limit hint.
        /// None means no preference, and the impl returns the smallest useful page of one entry.
        /// The responder might have a hard cap but asking above the cap should just cap
        /// rather than error.
        pub limit_hint: Option<u32>,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct LeafBucketResult {
        pub seed: FingerprintSeed,
        pub bucks: Map<
            BuckId,
            pub struct LeafBucketPage {
                pub entries: Vec<pub struct BucketObjPageEntry {
                    pub obj_id: ObjKey,
                    pub dead: bool,
                    pub fp: Fingerprint<(&'static str, ObjKey, ObjPayload)>,
                }>,
                pub next_after: Option<ObjKey>,
                pub done: bool,
            }
        >
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum LeafBucketsError {
    /// UnkownPart
    UnkownPart,
    /// Bucket level too shallow {buck_id:?}
    ShallowBucket { buck_id: BuckId },
}
