//! TODO: rate limiting

use crate::interlude::*;

use crate::fingerprint::{Fingerprint, FingerprintSeed};
use crate::mpsc::Receiver;
use crate::part_store::{CursorIndex, ObjPayload};

pub trait BigSyncRpcClient<K: FutureForm> {
    fn peer_summary<'a>(
        &'a self,
        req: PeerSummaryRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    fn sub_parts<'a>(
        &'a self,
        req: SubPartsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<Receiver<SubEvent>, ListPartsError>>>;

    /// Smart get_changed_buckets. It will dynamically adjust the levels to include
    /// according to change counts [`GetChangedBucketsRequest::since`].
    ///
    /// The idea being, if there are a lot of changes since the cursor, higher
    /// level bucket summaries can be useful since they allow fingerprint equal
    /// noops.
    fn get_changed_buckets<'a>(
        &'a self,
        req: GetChangedBucketsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<Vec<BucketSummary>, ListPartsError>>>;

    /// WARN: this doesn't limit the number of returned results
    /// It thus only accepts buckets that are of the level [`PeerSummaryResult::deepest_bucket_level`]
    fn leaf_buckets<'a>(
        &'a self,
        req: LeafBucketsRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<LeafBucketResult, LeafBucketsError>>>;
}

pub type BuckLevel = u8;
pub type BucketFp = (u64, u64);

pub struct GetChangedBucketsRequest {
    pub part_id: PartId,
    pub offset: BuckId,
    pub since: CursorIndex,
    /// RPC impls should return all changed
    /// sibling buckets of the last bucket before the limit
    /// in addition to the limit
    /// I.e. extra headroom of [`BuckId::ARITY`] is allowed.
    pub limit_hint: u32,
}

pub struct BucketSummary {
    pub id: BuckId,
    pub len: u32,
    pub live_count: u32,
    pub fp: BucketFp,
    pub changed_at: CursorIndex,
}

structstruck::strike! {
    #[derive(Debug)]
    pub struct LeafBucketsRequest {
        pub part_id: PartId,
        pub since: CursorIndex,
        pub buckets: Vec<BuckId>,
        pub seed: FingerprintSeed,
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum LeafBucketsError {
    /// UnkownPart
    UnkownPart,
    /// Bucket level too shallow {buck_id:?}
    ShallowBucket { buck_id: BuckId },
}

structstruck::strike! {
    pub struct LeafBucketResult {
        pub seed: FingerprintSeed,
        pub bucks: Map<
            BuckId,
            Vec<pub struct BucketObjPageEntry {
                pub obj_id: ObjId,
                pub dead: bool,
                pub fp: Fingerprint<(&'static str, ObjId, ObjPayload)>,
            }>
        >
    }
}

structstruck::strike! {
    pub struct PeerSummaryRequest {
        pub parts: Set<PartId>,
    }
}

structstruck::strike! {
    pub struct PeerSummaryResult {
        /// Only known partitions where the requestor has
        /// accessed are returned here. If an expected partition
        /// is missing, either access is denied or the peer doesn't
        /// know of the partitions yet. Request again with backoff
        /// in case peer learns of partitions from the current node
        pub parts: Map<
            PartId,
            pub struct PartSummary {
                pub latest_cursor: CursorIndex,
                pub member_count: u64,
            }
        >,
        pub deepest_bucket_level: BuckLevel
    }
}

structstruck::strike! {
    pub struct SubPartsRequest {
        pub parts: Vec<
            pub struct PartStreamCursorRequest {
                pub part_id: PartId,
                pub cursor: CursorIndex,
            }
        >,
    }
}

structstruck::strike! {
    pub struct PartPage {
        pub events: Vec<pub enum PartEvent {
            Upserted(pub struct ObjUpserted {
                #![derive(Debug, Clone, Serialize, Deserialize)]
                pub cursor: CursorIndex,
                pub part_id: PartId,
                pub obj_id: ObjId,
                pub payload: ObjPayload,
            }),
            Deleted(pub struct ObjRemoved {
                #![derive(Debug, Clone, Serialize, Deserialize)]
                pub cursor: CursorIndex,
                pub part_id: PartId,
                pub obj_id: ObjId,
            }),
        }>,
        pub next_cursor: Option<CursorIndex>,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, Serialize, Deserialize)]]
    pub enum SubEvent {
        Upserted(ObjUpserted),
        Deleted(ObjRemoved),
        ReplayComplete ,
    }
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum RpcError {
    /// TransportError
    TransportError,
}

#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub enum ListPartsError {
    /// UnkownParts {unkown_parts:?}
    UnkownParts { unkown_parts: Vec<PartId> },
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;
