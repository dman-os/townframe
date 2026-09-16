//! TODO: rate limiting

use serde::{Deserializer, Serializer};

use crate::interlude::*;

use crate::fingerprint::{Fingerprint, FingerprintSeed};
use crate::part_store::{CursorIndex, ObjPayload, PartDirtyCount};

pub trait BigSyncRpcClient<K: FutureForm> {
    fn peer_summary<'a>(
        &'a self,
        req: PeerSummaryRequest,
    ) -> K::Future<'a, BigSyncRpcResult<Result<PeerSummaryResult, ListPartsError>>>;

    /// One bounded, filtered replay page for a single target.
    ///
    /// Delivery is client-driven: the client names a target and a cursor, asks
    /// for a bounded number of events, and re-issues. The responder holds the
    /// request while there is nothing to send, so paging is the flow control.
    fn replay_page<'a>(
        &'a self,
        req: ReplayPageRequest,
    ) -> K::Future<'a, BigSyncRpcResult<ReplayPageOutcome>>;

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
    /// It only accepts buckets at the level the requesting part advertises in
    /// its per-part [`PartStratSummary::Bucket`] summary.
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
        obj_id: ObjKey,
        cursor: CursorIndex,
        old: BucketMemberKind<'_>,
        new: BucketMemberKind<'_>,
    ) {
        self.changed_at = cursor;
        match old {
            BucketMemberKind::Absent => {}
            BucketMemberKind::Live(payload) => self.remove_live(buck_id, obj_id.clone(), payload),
            BucketMemberKind::Dead => self.remove_dead(buck_id, obj_id.clone()),
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

    fn add_live(&mut self, buck_id: BuckId, obj_id: ObjKey, payload: &ObjPayload) {
        self.live_count = self.live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.live_fp.add(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
        );
    }

    fn remove_live(&mut self, buck_id: BuckId, obj_id: ObjKey, payload: &ObjPayload) {
        assert!(self.live_count > 0, "fishy");
        self.live_count -= 1;
        self.live_fp.remove(
            &BUCKET_LIVE_FP_SEED,
            &("big-sync-bucket-live-v1", buck_id, obj_id, payload),
        );
    }

    fn add_dead(&mut self, buck_id: BuckId, obj_id: ObjKey) {
        self.dead_count = self.dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        self.dead_fp.add(
            &BUCKET_DEAD_FP_SEED,
            &("big-sync-bucket-dead-v1", buck_id, obj_id),
        );
    }

    fn remove_dead(&mut self, buck_id: BuckId, obj_id: ObjKey) {
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
    pub part_id: PartKey,
    /// Resume cursor in bucket order. The response contains buckets with an id at or past
    /// it, at any level up to [`Self::to_level`].
    pub offset: BuckId,
    /// The deepest level the response may contain. Changed buckets are returned for every
    /// level from `offset.level()` through this one, in bucket order, so a walk that needs
    /// level N is a single scan instead of one exchange per level (ADR 012 decision 4,
    /// correction 3). Because a change stamps every ancestor's `changed_at`, filtering on
    /// the peer's cursor already narrows this to the buckets whose ranges differ.
    pub to_level: BuckLevel,
    pub since: CursorIndex,
    /// RPC impls should return all changed
    /// sibling buckets of the last bucket before the limit
    /// in addition to the limit
    /// I.e. extra headroom of [`BuckId::ARITY`] is allowed.
    pub limit_hint: u32,
}

/// Store-level part summary: the raw per-part facts a part store can report.
/// The RPC layer expands this into the per-strat [`PartStratSummary`] vec so
/// the decision side can pick a strat per part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartSummary {
    pub latest_cursor: CursorIndex,
    pub member_count: u64,
    /// The deepest bucket level this store has materialized for the part.
    pub deepest_bucket_level: BuckLevel,
}

impl PartSummary {
    /// Expand the raw store summary into the per-strat wire summaries the
    /// decision side consumes: cursor strat (latest cursor + the relevance the
    /// asker is behind on) + bucket strat (that part's deepest bucket level and
    /// member count).
    ///
    /// `dirty_count` comes from the responder, not from the store: it is a fact
    /// about the *asker*, counted against the cursor that asker advertised.
    pub fn into_strat_summaries(self, dirty_count: PartDirtyCount) -> Vec<PartStratSummary> {
        let mut summaries = vec![PartStratSummary::Cursor(CursorPartSummary {
            latest_cursor: self.latest_cursor,
            dirty_count,
        })];
        if self.deepest_bucket_level > 0 {
            summaries.push(PartStratSummary::Bucket(BucketPartSummary {
                deepest_bucket_level: self.deepest_bucket_level,
                member_count: self.member_count,
            }));
        }
        summaries
    }
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
        pub after: Option<ObjKey>,
    }
}

structstruck::strike! {
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct LeafBucketsRequest {
        pub part_id: PartKey,
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

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct PeerSummaryRequest {
        pub parts: Set<PartKey>,
        /// Per part, the cursor the ASKER holds for the peer it is asking: its
        /// position in that peer's stream. The responder counts relevance against
        /// this, because only the owner of the rows can evaluate them on its own
        /// cursor scale.
        ///
        /// A part the asker omits is read as `0`, which over-counts rather than
        /// under-counts: the bucket band is the heavier but safe direction, and the
        /// count is a hint either way.
        pub asker_part_cursors: Map<PartKey, CursorIndex>,
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub enum PartStratSummary {
        /// The peer can serve this part with the cursor strat; reports the
        /// latest cursor of the part and the relevance the ASKER is behind on.
        Cursor(pub struct CursorPartSummary {
            pub latest_cursor: CursorIndex,
            /// Counted by the responder against
            /// [`PeerSummaryRequest::asker_part_cursors`]. A band-selection hint,
            /// not a correctness input: a wrong value can only mis-select the band,
            /// and both bands converge.
            pub dirty_count: PartDirtyCount,
        }),
        /// The peer can serve this part with the bucket strat; reports that
        /// part's deepest materialized bucket level and member count.
        Bucket(pub struct BucketPartSummary {
            pub deepest_bucket_level: BuckLevel,
            pub member_count: u64,
        }),
    }
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub struct PeerSummaryResult {
        /// Only known partitions where the requestor has accessed are returned here.
        /// Each part reports the sync strats it supports; the decision side
        /// picks a strat per part (cursor diff or bucket working level), so
        /// different parts can be served by different strats.
        pub parts: Map<PartKey, Vec<PartStratSummary>>,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SubscriptionTarget {
    Part {
        part_id: PartKey,
        cursor: CursorIndex,
    },
    Object {
        obj_id: ObjKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubPartsRequest {
    /// The lowest global transaction cursor whose events should be replayed.
    pub lower_bound: CursorIndex,
    /// An immutable snapshot of the peer's complete subscription set.
    /// Reconfiguration establishes a replacement stream with a new replay barrier.
    pub targets: Set<SubscriptionTarget>,
}

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    #[derive(Default)]
    pub struct PartPage {
        pub events: Vec<pub enum PartEvent {
            Changed(pub struct ObjChanged {
                pub cursor: CursorIndex,
                pub part_ids: Vec<PartKey>,
                pub obj_id: ObjKey,
                // NOTE: IRPC uses postcard encoding
                // which doesn't support serde_json::Value
                // types
                #[serde(
                    serialize_with = "value_as_string",
                    deserialize_with = "value_from_string"
                )]
                pub payload: ObjPayload,
            }),
            Removed(pub struct ObjRemovedFromPart {
                pub cursor: CursorIndex,
                pub part_id: PartKey,
                pub obj_id: ObjKey,
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

structstruck::strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]]
    pub enum SubEvent {
        Changed(ObjChanged),
        Removed(ObjRemovedFromPart),
        ReplayComplete,
    }
}

impl From<PartEvent> for SubEvent {
    fn from(evt: PartEvent) -> Self {
        match evt {
            PartEvent::Changed(inner) => Self::Changed(inner),
            PartEvent::Removed(inner) => Self::Removed(inner),
        }
    }
}

/// A request for one bounded page of a single target's events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPageRequest {
    /// The one target being paged. A `Part` target carries the cursor to resume
    /// from; an `Object` target replays that object's derived part.
    pub target: SubscriptionTarget,
    /// Upper bound on how many events this page may carry.
    pub limit: u32,
    /// How long the responder may hold the request while the target has nothing
    /// to send. This is the caller's pacing choice, so a caller that has other
    /// work can ask for a short hold; the responder caps it.
    pub hold_ms: u32,
}

/// What a page request answered.
///
/// An empty page and a denied part are deliberately different answers: the
/// first means caught up, the second means the peer may no longer read the
/// part, which a caller must not have to infer from an empty page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayPageOutcome {
    /// Events after the target's cursor, filtered for the asking principal.
    ///
    /// `PartPage::next_cursor` is the resume point: `Some` means more is waiting,
    /// `None` means the responder's log is caught up as of the last event, which
    /// is how a caller learns that replay is complete.
    Events(PartPage),
    /// The target names a part this scope does not know.
    UnknownPart,
    /// The asking principal may not read the target's part.
    Unauthorized,
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
    UnkownParts { unkown_parts: Vec<PartKey> },
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;
