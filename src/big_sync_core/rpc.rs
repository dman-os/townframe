//! TODO: rate limiting

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    ) -> K::Future<'a, BigSyncRpcResult<ReplayPage>>;

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
        /// RPC impls should return at most this many entries per requested bucket.
        ///
        /// A hint rather than a bound: zero means no preference, and the impl returns
        /// the smallest useful page of one entry, because a page with no entries reads
        /// as `done` while entries remain. The responder caps it (as it caps
        /// [`ReplayPageRequest::limit`]), so asking above the cap gets the cap rather
        /// than an error.
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
        /// Where to resume this object's replay. An object route has no part of
        /// its own whose cursor could carry the position, and the store is the
        /// side that materializes the object's derived part, so the position
        /// travels with the route. Without it every page asks for the object's
        /// events from the start again.
        cursor: CursorIndex,
    },
}

/// What the responder answered about one requested target.
///
/// An empty target and a denied target are deliberately different answers: `Events` with
/// `drained` says the read of that target was exhausted (caught up to `resume`), and the
/// denial variants say why it was not served. Neither is inferred from an empty page, and
/// neither suppresses the other targets of the same page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetVerdict {
    /// Events after this target's cursor, filtered for the asking principal, plus the
    /// position this target resumes from.
    ///
    /// `resume` is always a position the caller can ask from again, and never a verdict:
    /// it is the last position whose events this page delivered in full — the caller's own
    /// cursor when the page delivered nothing, and the reader's own boundary when
    /// `drained` is set, because that read covered the range in full. `drained` is a
    /// per-page snapshot, never a durable "this replay is over": new events land past
    /// `resume`, so the caller holds and asks again rather than stopping. A page that
    /// filled on `limit` with rows still waiting is `drained: false`, so the caller re-asks
    /// immediately.
    Events { resume: CursorIndex, drained: bool },
    /// The target names a part this scope does not know.
    UnknownPart,
    /// The asking principal may not read the target's part.
    Unauthorized,
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
        /// Always a position the caller can ask from again, and never a verdict.
        ///
        /// It is the last position whose events this page has delivered in full: the
        /// caller's own cursor when it delivered nothing, otherwise advanced past the
        /// last scanned revision.
        pub resume: CursorIndex,
        /// Whether this part's read was exhausted within this page. A page that filled on
        /// `limit` with rows still waiting is `false`.
        pub drained: bool,
    }
}

/// One bounded page over a set of targets: what the responder answered about each.
///
/// This is the wire page. The storage-level [`PartPage`] is a different thing — one part's
/// events with one position, as `list_events` returns them — so the two are named for what
/// they carry rather than sharing a shape, and a page over a set never collapses one
/// target's position into another's.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReplayPage {
    /// The events this page carries, collapsed per object and ordered by position.
    pub events: Vec<PartEvent>,
    /// One verdict per requested target, in the request's order, each carrying the position
    /// that target resumes from.
    pub targets: Vec<(SubscriptionTarget, TargetVerdict)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayPageWire {
    parts: Vec<PartKey>,
    objects: Vec<ObjKey>,
    events: Vec<ReplayEventWire>,
    targets: Vec<(SubscriptionTarget, TargetVerdict)>,
}

#[derive(Debug, Serialize, Deserialize)]
enum ReplayEventWire {
    Changed(ReplayChangedWire),
    Removed(ReplayRemovedWire),
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayChangedWire {
    cursor: CursorIndex,
    object: u32,
    parts: Vec<u32>,
    #[serde(
        serialize_with = "value_as_string",
        deserialize_with = "value_from_string"
    )]
    payload: ObjPayload,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplayRemovedWire {
    cursor: CursorIndex,
    part: u32,
    object: u32,
}

fn intern_page_key<K>(
    key: &K,
    keys: &mut Vec<K>,
    indices: &mut std::collections::HashMap<K, u32>,
) -> u32
where
    K: Clone + Eq + std::hash::Hash,
{
    if let Some(index) = indices.get(key) {
        return *index;
    }
    let index = u32::try_from(keys.len()).expect("replay page key dictionary exceeds u32");
    keys.push(key.clone());
    indices.insert(key.clone(), index);
    index
}

impl ReplayPage {
    /// Conservative encoded page budget used by replay responders.
    pub const BYTE_BUDGET: usize = 64 * 1024;

    fn to_wire(&self) -> ReplayPageWire {
        let mut parts = Vec::new();
        let mut part_indices = std::collections::HashMap::new();
        let mut objects = Vec::new();
        let mut object_indices = std::collections::HashMap::new();
        let events = self
            .events
            .iter()
            .map(|event| match event {
                PartEvent::Changed(changed) => ReplayEventWire::Changed(ReplayChangedWire {
                    cursor: changed.cursor,
                    object: intern_page_key(&changed.obj_id, &mut objects, &mut object_indices),
                    parts: changed
                        .part_ids
                        .iter()
                        .map(|part| intern_page_key(part, &mut parts, &mut part_indices))
                        .collect(),
                    payload: changed.payload.clone(),
                }),
                PartEvent::Removed(removed) => ReplayEventWire::Removed(ReplayRemovedWire {
                    cursor: removed.cursor,
                    part: intern_page_key(&removed.part_id, &mut parts, &mut part_indices),
                    object: intern_page_key(&removed.obj_id, &mut objects, &mut object_indices),
                }),
            })
            .collect();
        ReplayPageWire {
            parts,
            objects,
            events,
            targets: self.targets.clone(),
        }
    }

    /// Size of the page in the postcard representation used by IRPC.
    pub fn encoded_size(&self) -> Result<usize, String> {
        postcard::to_allocvec(&self.to_wire())
            .map(|bytes| bytes.len())
            .map_err(|error| error.to_string())
    }
}

impl Serialize for ReplayPage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReplayPage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ReplayPageWire::deserialize(deserializer)?;
        let mut events = Vec::with_capacity(wire.events.len());
        for event in wire.events {
            match event {
                ReplayEventWire::Changed(changed) => {
                    let obj_id = wire
                        .objects
                        .get(usize::try_from(changed.object).map_err(|_| {
                            serde::de::Error::custom("replay object index does not fit usize")
                        })?)
                        .cloned()
                        .ok_or_else(|| {
                            serde::de::Error::custom("replay object index is out of bounds")
                        })?;
                    let part_ids = changed
                        .parts
                        .into_iter()
                        .map(|index| {
                            wire.parts
                                .get(usize::try_from(index).map_err(|_| {
                                    serde::de::Error::custom("replay part index does not fit usize")
                                })?)
                                .cloned()
                                .ok_or_else(|| {
                                    serde::de::Error::custom("replay part index is out of bounds")
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    events.push(PartEvent::Changed(ObjChanged {
                        cursor: changed.cursor,
                        part_ids,
                        obj_id,
                        payload: changed.payload,
                    }));
                }
                ReplayEventWire::Removed(removed) => {
                    let obj_id = wire
                        .objects
                        .get(usize::try_from(removed.object).map_err(|_| {
                            serde::de::Error::custom("replay object index does not fit usize")
                        })?)
                        .cloned()
                        .ok_or_else(|| {
                            serde::de::Error::custom("replay object index is out of bounds")
                        })?;
                    let part_id = wire
                        .parts
                        .get(usize::try_from(removed.part).map_err(|_| {
                            serde::de::Error::custom("replay part index does not fit usize")
                        })?)
                        .cloned()
                        .ok_or_else(|| {
                            serde::de::Error::custom("replay part index is out of bounds")
                        })?;
                    events.push(PartEvent::Removed(ObjRemovedFromPart {
                        cursor: removed.cursor,
                        part_id,
                        obj_id,
                    }));
                }
            }
        }
        Ok(Self {
            events,
            targets: wire.targets,
        })
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

impl ReplayPage {
    /// What the responder answered about , if this page named it.
    pub fn verdict(&self, target: &SubscriptionTarget) -> Option<&TargetVerdict> {
        self.targets
            .iter()
            .find(|(named, _)| named == target)
            .map(|(_, verdict)| verdict)
    }
}

impl PartEvent {
    /// The position this event was committed at.
    pub fn cursor(&self) -> CursorIndex {
        match self {
            Self::Changed(inner) => inner.cursor,
            Self::Removed(inner) => inner.cursor,
        }
    }
}

/// Identifies one in-flight page request on a connection.
///
/// The id is the caller's own: a peer names it in [`CancelReplayRequest`] to cancel
/// exactly that request, and the responder remembers nothing about it past the
/// request's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplayRequestId(pub u64);

/// A request for one bounded page over a set of targets.
///
/// The page lane is deliberately transport-agnostic: the same request can be
/// carried by a stream (WebSocket/NATS) or by an HTTP long-poll. The caller's
/// own pacing therefore travels in the request (`hold_ms`) instead of being a
/// server-side timeout, and a hold that expires is a normal answer (a drained
/// page), never an error — an HTTP responder answering 5xx for it would make
/// clients retry the whole request. On a push transport `hold_ms = 0` degenerates
/// cleanly to plain polling.
///
/// One request covers every target the caller wants in this round, so a set of
/// parts costs one request rather than one per part, and a target's own cursor
/// still travels with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayPageRequest {
    /// Identifies this request, so a later request can supersede it.
    pub request_id: ReplayRequestId,
    /// The in-flight request this one supersedes, if any.
    ///
    /// The responder cancels that request only if it is still waiting: a request whose read
    /// already produced rows ships them anyway, so superseding never destroys a page that
    /// was about to deliver. This travels in-band rather than as a separate message because
    /// the machine's rpc trait is generic over the future form, and a new required method on
    /// it would break every implementor outside this crate's reach.
    pub supersede: Option<ReplayRequestId>,
    /// The targets to page. A `Part` target carries the cursor to resume from; an
    /// `Object` target replays that object's derived part.
    pub targets: Vec<SubscriptionTarget>,
    /// Upper bound on how many events this page may carry, sliced across the targets
    /// so a target with a large backlog cannot starve a target with a small one.
    pub limit: u32,
    /// How long the responder may hold the request while every target has nothing
    /// to send. This is the caller's pacing choice, so a caller that has other
    /// work can ask for a short hold; the responder caps it. Zero means do not
    /// hold at all.
    pub hold_ms: u32,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum RpcError {
    /// TransportError
    TransportError,
    /// InvalidRequest {0}
    InvalidRequest(String),
    /// Internal
    Internal,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error, displaydoc::Display,
)]
pub enum ListPartsError {
    /// UnkownParts {unkown_parts:?}
    UnkownParts { unkown_parts: Vec<PartKey> },
}

pub type BigSyncRpcResult<T> = Result<T, RpcError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_page_compact_wire_round_trips_and_deduplicates_keys() {
        let obj = ObjKey::new(b"object-with-variable-length-key");
        let part = PartKey::new(b"part-with-variable-length-key");
        let page = ReplayPage {
            events: vec![
                PartEvent::Changed(ObjChanged {
                    cursor: 3,
                    part_ids: vec![part.clone()],
                    obj_id: obj.clone(),
                    payload: serde_json::json!({"value": 1}),
                }),
                PartEvent::Removed(ObjRemovedFromPart {
                    cursor: 4,
                    part_id: part,
                    obj_id: obj,
                }),
            ],
            targets: Vec::new(),
        };
        let wire = page.to_wire();
        assert_eq!(wire.objects.len(), 1);
        assert_eq!(wire.parts.len(), 1);
        let encoded = postcard::to_allocvec(&page).expect("encode replay page");
        let decoded: ReplayPage = postcard::from_bytes(&encoded).expect("decode replay page");
        assert_eq!(decoded, page);
    }

    #[test]
    fn replay_page_rejects_invalid_dictionary_indices() {
        let wire = ReplayPageWire {
            parts: Vec::new(),
            objects: vec![ObjKey::new(b"object")],
            events: vec![ReplayEventWire::Removed(ReplayRemovedWire {
                cursor: 1,
                part: 0,
                object: 1,
            })],
            targets: Vec::new(),
        };
        let encoded = postcard::to_allocvec(&wire).expect("encode invalid replay page");
        let error = postcard::from_bytes::<ReplayPage>(&encoded).expect_err("invalid index");
        assert!(!error.to_string().is_empty());
    }
}
