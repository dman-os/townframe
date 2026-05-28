//! TODO: rate limiting

use serde::{Deserializer, Serializer};

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

pub const BUCKET_LIVE_FP_SEED: FingerprintSeed = FingerprintSeed::new(0x6c697665, 0x6275636b);
pub const BUCKET_DEAD_FP_SEED: FingerprintSeed = FingerprintSeed::new(0x64656164, 0x6275636b);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketMemberKind<'a> {
    Absent,
    Live(&'a ObjPayload),
    Dead,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BucketSummaryState {
    changed_at: CursorIndex,
    live_count: u32,
    dead_count: u32,
    live_fp: BucketFingerprint,
    dead_fp: BucketFingerprint,
}

impl BucketSummaryState {
    pub fn apply_transition(
        &mut self,
        buck_id: BuckId,
        obj_id: ObjId,
        cursor: CursorIndex,
        old: BucketMemberKind<'_>,
        new: BucketMemberKind<'_>,
    ) {
        self.changed_at = cursor;
        match old {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(payload) => self.remove_live(buck_id, obj_id, payload),
            BucketMemberKind::Dead => self.remove_dead(buck_id, obj_id),
        }
        match new {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(payload) => self.add_live(buck_id, obj_id, payload),
            BucketMemberKind::Dead => self.add_dead(buck_id, obj_id),
        }
    }

    pub fn summary(&self, id: BuckId) -> BucketSummary {
        BucketSummary {
            id,
            len: self.live_count + self.dead_count,
            live_count: self.live_count,
            fp: (self.live_fp.as_u64(), self.dead_fp.as_u64()),
            changed_at: self.changed_at,
        }
    }

    pub const fn changed_at(&self) -> CursorIndex {
        self.changed_at
    }

    fn add_live(&mut self, buck_id: BuckId, obj_id: ObjId, payload: &ObjPayload) {
        self.live_count = self.live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.live_fp.add(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
        );
    }

    fn remove_live(&mut self, buck_id: BuckId, obj_id: ObjId, payload: &ObjPayload) {
        assert!(self.live_count > 0, "fishy");
        self.live_count -= 1;
        self.live_fp.remove(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
        );
    }

    fn add_dead(&mut self, buck_id: BuckId, obj_id: ObjId) {
        self.dead_count = self.dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.dead_fp.add(
            &BUCKET_DEAD_FP_SEED,
            &("big-sync-bucket-dead-v1", buck_id, obj_id),
        );
    }

    fn remove_dead(&mut self, buck_id: BuckId, obj_id: ObjId) {
        assert!(self.dead_count > 0, "fishy");
        self.dead_count -= 1;
        self.dead_fp.remove(
            &BUCKET_DEAD_FP_SEED,
            &("big-sync-bucket-dead-v1", buck_id, obj_id),
        );
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct BucketFingerprint(u64);

impl BucketFingerprint {
    fn add<T: core::hash::Hash>(&mut self, seed: &FingerprintSeed, value: &T) {
        self.0 = self.0.wrapping_add(Fingerprint::new(seed, value).as_u64());
    }

    fn remove<T: core::hash::Hash>(&mut self, seed: &FingerprintSeed, value: &T) {
        self.0 = self.0.wrapping_sub(Fingerprint::new(seed, value).as_u64());
    }

    fn as_u64(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSummary {
    pub id: BuckId,
    pub len: u32,
    pub live_count: u32,
    pub fp: BucketFp,
    pub changed_at: CursorIndex,
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LeafBucketRequest {
        pub buck_id: BuckId,
        pub after: Option<ObjId>,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LeafBucketsRequest {
        pub part_id: PartId,
        pub since: CursorIndex,
        pub buckets: Vec<LeafBucketRequest>,
        pub seed: FingerprintSeed,
        pub limit_hint: u32,
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

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct LeafBucketResult {
        pub seed: FingerprintSeed,
        pub bucks: Map<
            BuckId,
            pub struct LeafBucketPage {
                pub entries: Vec<pub struct BucketObjPageEntry {
                    pub obj_id: ObjId,
                    pub dead: bool,
                    pub fp: Fingerprint<(&'static str, ObjId, ObjPayload)>,
                }>,
                pub next_after: Option<ObjId>,
                pub done: bool,
            }
        >
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct PeerSummaryRequest {
        pub parts: Set<PartId>,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
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
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
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
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    #[derive(Default)]
    pub struct PartPage {
        pub events: Vec<pub enum PartEvent {
            Changed(pub struct ObjChanged {
                pub cursor: CursorIndex,
                pub part_ids: Vec<PartId>,
                pub obj_id: ObjId,
                // NOTE: IRPC uses postcard encoding
                // which doesn't support serde_json::Value
                // types
                #[serde(
                    serialize_with = "value_as_string",
                    deserialize_with = "value_from_string"
                )]
                pub payload: ObjPayload,
            }),
            Added(pub struct ObjAddedToPart {
                pub cursor: CursorIndex,
                pub part_id: PartId,
                pub obj_id: ObjId,
                #[serde(
                    serialize_with = "option_value_as_string",
                    deserialize_with = "option_value_from_string"
                )]
                pub payload: Option<ObjPayload>,
            }),
            Removed(pub struct ObjRemovedFromPart {
                pub cursor: CursorIndex,
                pub part_id: PartId,
                pub obj_id: ObjId,
            }),
        }>,
        pub next_cursor: Option<CursorIndex>,
    }
}
fn value_as_string<S>(val: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&serde_json::to_string(val).map_err(serde::ser::Error::custom)?)
}

fn value_from_string<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: Deserializer<'de>,
{
    let str = String::deserialize(deserializer)?;
    serde_json::from_str(&str).map_err(serde::de::Error::custom)
}

fn option_value_as_string<S>(
    val: &Option<serde_json::Value>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match val {
        Some(val) => serializer
            .serialize_some(&serde_json::to_string(val).map_err(serde::ser::Error::custom)?),
        None => serializer.serialize_none(),
    }
}

fn option_value_from_string<'de, D>(deserializer: D) -> Result<Option<serde_json::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let str = Option::<String>::deserialize(deserializer)?;
    str.map(|str| serde_json::from_str(&str).map_err(serde::de::Error::custom))
        .transpose()
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub enum SubEvent {
        Changed(ObjChanged),
        Added(ObjAddedToPart),
        Removed(ObjRemovedFromPart),
        ReplayComplete ,
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum RpcError {
    /// TransportError
    TransportError,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum ListPartsError {
    /// UnkownParts {unkown_parts:?}
    UnkownParts { unkown_parts: Vec<PartId> },
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;
