use crate::interlude::*;

use crate::{ScopeRef, ScopedIdResolver, ScopedObjRef, ScopedPartRef};
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketMemberKind, BucketObjPageEntry, BucketSummary, BucketSummaryState,
    GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, PartEvent, PartPage, PartSummary, SubEvent,
    SubPartsRequest,
};
use big_sync_core::{BuckId, Byte32Id, Fingerprint, ObjId, PartId, PeerId};
use tokio::sync::mpsc;

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

// pub mod sqlite;

pub(crate) fn obj_id_bounds_for_bucket(bucket_id: BuckId) -> (ObjId, Option<ObjId>) {
    let level = bucket_id.level();
    let prefix_bits = u32::from(level) * u32::from(BuckId::BITS_PER_LEVEL);
    debug_assert!(prefix_bits <= u32::from(u16::BITS));

    if prefix_bits == 0 {
        return (ObjId(Byte32Id::new([0; 32])), None);
    }

    let shift = u32::from(u16::BITS) - prefix_bits;
    let start_prefix = (u32::from(bucket_id.index())) << shift;
    let start = {
    let mut bytes = [0; 32];
    bytes[..2].copy_from_slice(&(start_prefix as u16).to_be_bytes());
    ObjId(Byte32Id::new(bytes))
    };
    if prefix_bits == u32::from(u16::BITS) || bucket_id.index() == u16::MAX {
        return (start, None);
    }
    let next_prefix = (u32::from(bucket_id.index()) + 1) << shift;
    if next_prefix > u32::from(u16::MAX) {
        return (start, None);
    }
    let end = Some({
        let mut bytes = [0; 32];
        bytes[..2].copy_from_slice(&(next_prefix as u16).to_be_bytes());
        ObjId(Byte32Id::new(bytes))
    });
    (start, end)
}

#[async_trait]
pub trait HostPartitionStore: Send + Sync {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>>;
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>>;
    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>>;
    async fn member_count(&self, part_id: PartId) -> Res<u64>;
    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>>;
    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary>;

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()>;

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>>;

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()>;
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()>;

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex>;

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()>;

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>>;

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::UnboundedReceiver<SubEvent>, ListPartsError>>;
}

#[cfg(any(test, feature = "test-support"))]
pub mod contract {
    use super::*;
    use big_sync_core::rpc::{
        BucketSummary, GetChangedBucketsRequest, LeafBucketRequest, LeafBucketsRequest,
        BUCKET_DEAD_FP_SEED, BUCKET_LIVE_FP_SEED,
    };
    use big_sync_core::{Fingerprint, FingerprintSeed};
    use std::collections::BTreeSet;

    pub async fn assert_scoped_obj_id_distribution<R>(resolver: &R, objs: &[ScopedObjRef]) -> Res<()>
    where
        R: ScopedIdResolver + Sync,
    {
        assert!(
            objs.len() >= 32,
            "need enough objects to exercise object-id distribution"
        );

        let mut obj_ids = Vec::with_capacity(objs.len());
        for obj in objs {
            let first = resolver.resolve_obj(obj).await?;
            let second = resolver.resolve_obj(obj).await?;
            assert_eq!(first, second, "resolve_obj must be stable for {obj:?}");
            obj_ids.push(first);
        }

        let unique_ids: BTreeSet<_> = obj_ids.iter().copied().collect();
        assert_eq!(
            unique_ids.len(),
            obj_ids.len(),
            "resolve_obj must not collapse distinct scoped objects onto the same obj id"
        );

        let unique_leaf_buckets: BTreeSet<_> = obj_ids
            .iter()
            .map(|obj_id| BuckId::from_obj_id(BuckId::MAX_LEVEL, obj_id))
            .collect();
        assert!(
            unique_leaf_buckets.len() >= 8,
            "object ids are too clustered across leaf buckets"
        );
        Ok(())
    }

    async fn expected_bucket_summary<S>(
        store: &S,
        live_ids: &BTreeSet<ObjId>,
        dead_ids: &BTreeSet<ObjId>,
    ) -> Res<BucketSummary>
    where
        S: HostPartitionStore + Sync,
    {
        let mut live_fp = 0u64;
        let mut dead_fp = 0u64;
        let mut live_count = 0u32;
        let mut dead_count = 0u32;

        let root = BuckId::ROOT;
        for obj_id in live_ids {
            let payload = store
                .obj_payload(*obj_id)
                .await?
                .expect("live object must have payload");
            live_fp = live_fp.wrapping_add(
                Fingerprint::new(
                    &BUCKET_LIVE_FP_SEED,
                    &("big-sync-bucket-live-v1", root, *obj_id, payload),
                )
                .as_u64(),
            );
            live_count = live_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        }
        for obj_id in dead_ids {
            assert!(
                !live_ids.contains(obj_id),
                "live and dead object sets must be disjoint"
            );
            assert!(
                store.obj_payload(*obj_id).await?.is_none(),
                "dead object must not have payload"
            );
            dead_fp = dead_fp.wrapping_add(
                Fingerprint::new(
                    &BUCKET_DEAD_FP_SEED,
                    &("big-sync-bucket-dead-v1", root, *obj_id),
                )
                .as_u64(),
            );
            dead_count = dead_count.checked_add(1).expect(ERROR_IMPOSSIBLE);
        }

        Ok(BucketSummary {
            id: root,
            len: live_count + dead_count,
            live_count,
            fp: (live_fp, dead_fp),
            changed_at: 0,
        })
    }

    pub async fn assert_root_bucket_summary<S>(
        store: &S,
        part_id: PartId,
        live_ids: &[ObjId],
        dead_ids: &[ObjId],
    ) -> Res<()>
    where
        S: HostPartitionStore + Sync,
    {
        assert_eq!(
            live_ids.len(),
            live_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "live object set contains duplicates"
        );
        assert_eq!(
            dead_ids.len(),
            dead_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "dead object set contains duplicates"
        );
        let live_ids: BTreeSet<_> = live_ids.iter().copied().collect();
        let dead_ids: BTreeSet<_> = dead_ids.iter().copied().collect();
        let expected = expected_bucket_summary(store, &live_ids, &dead_ids).await?;

        assert_eq!(
            store.member_count(part_id).await?,
            u64::from(expected.live_count)
        );

        let direct = store.get_bucket_summary(part_id, BuckId::ROOT).await?;
        assert_eq!(direct.id, BuckId::ROOT);
        assert_eq!(direct.len, expected.len);
        assert_eq!(direct.live_count, expected.live_count);
        assert_eq!(direct.fp, expected.fp);

        let changed = store
            .get_changed_buckets(GetChangedBucketsRequest {
                part_id,
                offset: BuckId::ROOT,
                since: 0,
                limit_hint: 1,
            })
            .await?;
        let changed = changed.expect(ERROR_IMPOSSIBLE);
        if expected.len == 0 {
            assert!(changed.is_empty());
        } else {
            assert_eq!(changed.len(), 1);
            assert_eq!(changed[0].id, BuckId::ROOT);
            assert_eq!(changed[0].len, expected.len);
            assert_eq!(changed[0].live_count, expected.live_count);
            assert_eq!(changed[0].fp, expected.fp);
            assert_eq!(changed[0].changed_at, direct.changed_at);
        }

        Ok(())
    }

    pub async fn assert_root_leaf_pagination<S>(
        store: &S,
        part_id: PartId,
        seed: FingerprintSeed,
        live_ids: &[ObjId],
        dead_ids: &[ObjId],
        limit_hint: u32,
    ) -> Res<()>
    where
        S: HostPartitionStore + Sync,
    {
        assert_eq!(
            live_ids.len(),
            live_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "live object set contains duplicates"
        );
        assert_eq!(
            dead_ids.len(),
            dead_ids.iter().copied().collect::<BTreeSet<_>>().len(),
            "dead object set contains duplicates"
        );
        let live_ids: BTreeSet<_> = live_ids.iter().copied().collect();
        let dead_ids: BTreeSet<_> = dead_ids.iter().copied().collect();
        assert!(
            live_ids.is_disjoint(&dead_ids),
            "live and dead object sets must be disjoint"
        );

        let expected: Vec<_> = live_ids.union(&dead_ids).copied().collect();
        let limit_hint = limit_hint.max(1);
        let mut seen = BTreeSet::new();
        let mut after = None;

        loop {
            let result = store
                .leaf_buckets(LeafBucketsRequest {
                    part_id,
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after,
                    }],
                    seed,
                    limit_hint,
                })
                .await?;
            let result = result.expect(ERROR_IMPOSSIBLE);
            assert_eq!(result.seed, seed);
            assert_eq!(result.bucks.len(), 1);

            let page = result.bucks.get(&BuckId::ROOT).expect(ERROR_IMPOSSIBLE);
            assert!(page
                .entries
                .windows(2)
                .all(|pair| pair[0].obj_id < pair[1].obj_id));
            assert!(page.entries.len() <= limit_hint as usize);

            if page.entries.is_empty() {
                assert!(page.done);
                assert!(page.next_after.is_none());
                break;
            }

            let last_obj_id = page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id;
            if page.done {
                assert!(page.next_after.is_none());
            } else {
                assert_eq!(page.next_after, Some(last_obj_id));
                assert_eq!(page.entries.len(), limit_hint as usize);
            }

            for entry in &page.entries {
                assert!(seen.insert(entry.obj_id), "duplicate leaf entry {}", entry.obj_id);
                assert_eq!(entry.dead, dead_ids.contains(&entry.obj_id));
                let expected_fp = if entry.dead {
                    Fingerprint::new(
                        &seed,
                        &("big-sync-obj-fp-v1", entry.obj_id, serde_json::Value::Null),
                    )
                } else {
                    let payload = store
                        .obj_payload(entry.obj_id)
                        .await?
                        .expect("live object must have payload");
                    Fingerprint::new(&seed, &("big-sync-obj-fp-v1", entry.obj_id, payload))
                };
                assert_eq!(entry.fp, expected_fp);
            }

            if page.done {
                break;
            }
            after = page.next_after;
        }

        assert_eq!(seen, expected.into_iter().collect());
        Ok(())
    }

    pub async fn assert_root_bucket_contract<S>(
        store: &S,
        part_id: PartId,
        seed: FingerprintSeed,
        live_ids: &[ObjId],
        dead_ids: &[ObjId],
        limit_hint: u32,
    ) -> Res<()>
    where
        S: HostPartitionStore + Sync,
    {
        assert_root_bucket_summary(store, part_id, live_ids, dead_ids).await?;
        assert_root_leaf_pagination(store, part_id, seed, live_ids, dead_ids, limit_hint).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ObjSyncStamp {
    pub(crate) seq: u64,
    pub(crate) origin: PeerId,
}

impl Default for ObjSyncStamp {
    fn default() -> Self {
        Self {
            seq: 0,
            origin: PeerId(Byte32Id::new([0; 32])),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncMutationOutcome {
    Applied,
    Stale,
}

structstruck::strike! {
    pub struct MemoryPartStore {
        pub(crate) owner_peer_id: PeerId,
        inner: Arc<surelock::mutex::Mutex<
            #[derive(Default)]
            struct MemoryPartStoreState {
                next_scope_id: u64,
                next_part_id: u64,
                next_obj_id: u64,
                scopes_by_name: HashMap<ScopeRef, u64>,
                scope_names_by_id: HashMap<u64, ScopeRef>,
                parts_by_scoped_ref: HashMap<(u64, Arc<str>), PartId>,
                scoped_refs_by_part: HashMap<PartId, (u64, Arc<str>)>,
                objs_by_scoped_ref: HashMap<(u64, Arc<str>), ObjId>,
                scoped_refs_by_obj: HashMap<ObjId, (u64, Arc<str>)>,
                global_cursor:
                    #[derive(Default)]
                    struct GlobalCursor {
                        counter: std::sync::atomic::AtomicU64,
                        event_to_part: BTreeMap<CursorIndex, PartId>,
                    },
                parts: HashMap<
                    PartId,
                    struct PartState {
                        #![derive(Default)]
                        latest_cursor: CursorIndex,

                        events: BTreeMap<CursorIndex, PartEvent>,

                        members: BTreeMap<
                            ObjId,
                            #[derive(Clone)]
                            struct PartMemberState {
                                payload: Option<ObjPayload>,
                                changed_at: CursorIndex,
                                removed_at: Option<CursorIndex>,
                            }
                        >,
                        bucket_stats: BTreeMap<BuckId, BucketSummaryState>,

                        bus: struct MemorySubsBus {
                            #![derive(Default)]
                            events_to_drop: Vec<CursorIndex>,
                            buf: Vec<PartEvent>,
                            subs_to_drop: Vec<usize>,
                            subs: Vec<mpsc::UnboundedSender<SubEvent>>
                        },
                    }
                >,
                objs: HashMap<
                    ObjId,
                    struct ObjDeets {
                        #![derive(Default)]
                        payload: Option<ObjPayload>,
                        parts: HashSet<PartId>,
                        sync_version: u64,
                        sync_stamp: ObjSyncStamp,
                    }
                >,
                tombstoned_objs: HashMap<ObjId, (CursorIndex, u64, ObjSyncStamp)>,
                peer_obj_payloads: HashMap<(PeerId, ObjId), Option<ObjPayload>>,
                peer_part_cursors: HashMap<(PeerId, PartId), CursorIndex>,
            }
        >>,
    }
}

impl Default for MemoryPartStore {
    fn default() -> Self {
        Self {
            owner_peer_id: zero_peer_id(),
            inner: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }
}

impl MemoryPartStore {
    pub fn with_owner(owner_peer_id: PeerId) -> Self {
        Self {
            owner_peer_id,
            inner: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }

    pub async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.parts.entry(part_id).or_default();
            Ok(())
        })
    }

    fn next_byte32_id(counter: &mut u64, domain: u8) -> Byte32Id {
        *counter = counter.checked_add(1).expect(ERROR_IMPOSSIBLE);
        let mut bytes = [0; 32];
        bytes[0] = domain;
        bytes[1..9].copy_from_slice(&counter.to_be_bytes());
        Byte32Id::new(bytes)
    }
}

fn zero_peer_id() -> PeerId {
    PeerId(Byte32Id::new([0; 32]))
}

#[cfg(test)]
pub(crate) fn test_scoped_obj_id(obj: &ScopedObjRef) -> ObjId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"big_sync_scoped_obj_id_v1");
    hasher.update(obj.scope.0.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(obj.obj.as_bytes());
    ObjId(Byte32Id::new(*hasher.finalize().as_bytes()))
}

impl MemoryPartStoreState {
    fn scope_id(&mut self, scope: &ScopeRef) -> u64 {
        if let Some(scope_id) = self.scopes_by_name.get(scope).copied() {
            return scope_id;
        }
        self.next_scope_id = self.next_scope_id.checked_add(1).expect(ERROR_IMPOSSIBLE);
        let scope_id = self.next_scope_id;
        let old = self.scopes_by_name.insert(scope.clone(), scope_id);
        assert!(old.is_none(), "fishy");
        let old = self.scope_names_by_id.insert(scope_id, scope.clone());
        assert!(old.is_none(), "fishy");
        scope_id
    }

    fn scope_ref(&self, scope_id: u64) -> ScopeRef {
        self.scope_names_by_id
            .get(&scope_id)
            .cloned()
            .expect(ERROR_IMPOSSIBLE)
    }

    fn bucket_items_for_path(
        &self,
        part_id: PartId,
        path: BuckId,
    ) -> Vec<(ObjId, CursorIndex, bool)> {
        let Some(part) = self.parts.get(&part_id) else {
            return Vec::new();
        };
        let (lower, upper) = obj_id_bounds_for_bucket(path);
        let mut items = Vec::new();
        match upper {
            Some(upper) => {
                for (&obj_id, member) in part.members.range(lower..upper) {
                    let cursor = member.removed_at.unwrap_or(member.changed_at);
                    items.push((obj_id, cursor, member.removed_at.is_some()));
                }
            }
            None => {
                for (&obj_id, member) in part.members.range(lower..) {
                    let cursor = member.removed_at.unwrap_or(member.changed_at);
                    items.push((obj_id, cursor, member.removed_at.is_some()));
                }
            }
        }
        items
    }

    fn bucket_summary(&self, part_id: PartId, path: BuckId) -> BucketSummary {
        self.parts
            .get(&part_id)
            .and_then(|part| part.bucket_stats.get(&path).cloned())
            .unwrap_or_default()
            .summary(path)
    }

    fn changed_bucket_summaries(
        &self,
        part_id: PartId,
        offset: BuckId,
        since: CursorIndex,
        limit_hint: u32,
    ) -> Result<Vec<BucketSummary>, ListPartsError> {
        let Some(part) = self.parts.get(&part_id) else {
            return Err(ListPartsError::UnkownParts {
                unkown_parts: vec![part_id],
            });
        };
        let mut buckets: Vec<_> = part
            .bucket_stats
            .range(offset..)
            .filter(|(buck_id, summary)| {
                buck_id.level() == offset.level() && summary.changed_at() > since
            })
            .map(|(&buck_id, summary)| summary.summary(buck_id))
            .collect();
        if buckets.is_empty() {
            return Ok(buckets);
        }
        if limit_hint == 0 {
            return Ok(Vec::new());
        }
        if buckets.len() > limit_hint as usize {
            let limit = limit_hint as usize;
            let last_parent = buckets[limit - 1].id.parent();
            let mut out = buckets.drain(..limit).collect::<Vec<_>>();
            while let Some(next) = buckets.first() {
                if next.id.parent() != last_parent {
                    break;
                }
                out.push(buckets.remove(0));
            }
            Ok(out)
        } else {
            Ok(buckets)
        }
    }
}

fn apply_bucket_transition(
    part: &mut PartState,
    obj_id: ObjId,
    cursor: CursorIndex,
    old: BucketMemberKind<'_>,
    new: BucketMemberKind<'_>,
) {
    for level in 0..=BuckId::MAX_LEVEL {
        let buck_id = BuckId::from_obj_id(level, &obj_id);
        let agg = part.bucket_stats.entry(buck_id).or_default();
        agg.apply_transition(buck_id, obj_id, cursor, old, new);
    }
}

fn obj_sync_version_locked(guard: &MemoryPartStoreState, obj_id: ObjId) -> u64 {
    if let Some(obj) = guard.objs.get(&obj_id) {
        return obj.sync_version;
    }
    guard
        .tombstoned_objs
        .get(&obj_id)
        .map(|(_, version, _)| *version)
        .unwrap_or_default()
}

fn obj_sync_stamp_locked(guard: &MemoryPartStoreState, obj_id: ObjId) -> ObjSyncStamp {
    if let Some(obj) = guard.objs.get(&obj_id) {
        return obj.sync_stamp.clone();
    }
    guard
        .tombstoned_objs
        .get(&obj_id)
        .map(|(_, _, stamp)| stamp.clone())
        .unwrap_or_default()
}

fn next_obj_sync_stamp_locked(
    guard: &MemoryPartStoreState,
    owner_peer_id: PeerId,
    obj_id: ObjId,
) -> ObjSyncStamp {
    let current = obj_sync_stamp_locked(guard, obj_id);
    ObjSyncStamp {
        seq: current.seq.saturating_add(1),
        origin: owner_peer_id,
    }
}

fn upsert_obj_locked(
    guard: &mut MemoryPartStoreState,
    owner_peer_id: PeerId,
    obj_id: ObjId,
    payload: ObjPayload,
    parts: Vec<PartId>,
    sync_version: Option<u64>,
    sync_stamp: Option<ObjSyncStamp>,
) {
    let event_payload = payload.clone();
    guard.tombstoned_objs.remove(&obj_id);
    let stamp =
        sync_stamp.unwrap_or_else(|| next_obj_sync_stamp_locked(guard, owner_peer_id, obj_id));
    let version = sync_version.unwrap_or(stamp.seq);

    let obj = guard.objs.entry(obj_id).or_default();
    obj.payload = Some(payload);
    obj.parts.extend(&parts);
    obj.sync_version = version;
    obj.sync_stamp = stamp.clone();

    for &part_id in &parts {
        let part = guard.parts.entry(part_id).or_default();
        let cursor = guard.global_cursor.get(part_id);
        let old_state = part.members.get(&obj_id).cloned();
        let old_kind = match old_state.as_ref() {
            Some(state) if state.removed_at.is_some() => BucketMemberKind::Dead,
            Some(state) => BucketMemberKind::Live(
                state
                    .payload
                    .as_ref()
                    .expect("live member must still have payload"),
            ),
            None => BucketMemberKind::Absent,
        };
        apply_bucket_transition(
            part,
            obj_id,
            cursor,
            old_kind,
            BucketMemberKind::Live(&event_payload),
        );
        if let Some(old) = part.members.get_mut(&obj_id) {
            if let Some(old_removed_at) = old.removed_at.take() {
                part.bus.remove_evt(old_removed_at);
            } else {
                part.bus.remove_evt(old.changed_at);
            }
            old.changed_at = cursor;
            old.payload = Some(event_payload.clone());
        } else {
            part.members.insert(
                obj_id,
                PartMemberState {
                    payload: Some(event_payload.clone()),
                    changed_at: cursor,
                    removed_at: None,
                },
            );
        }
        part.latest_cursor = cursor;
        part.bus
            .queue_evt(PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                cursor,
                part_id,
                obj_id,
                payload: event_payload.clone(),
            }));
        part.flush(&mut guard.global_cursor);
    }
}

fn remove_obj_from_part_locked(
    guard: &mut MemoryPartStoreState,
    owner_peer_id: PeerId,
    obj_id: ObjId,
    part_id: PartId,
    sync_version: Option<u64>,
    sync_stamp: Option<ObjSyncStamp>,
) {
    let stamp =
        sync_stamp.unwrap_or_else(|| next_obj_sync_stamp_locked(guard, owner_peer_id, obj_id));
    let version = sync_version.unwrap_or(stamp.seq);
    let part = guard.parts.entry(part_id).or_default();
    let Some(old_state) = part.members.get(&obj_id).cloned() else {
        return;
    };
    if old_state.removed_at.is_some() {
        return;
    }
    let cursor = guard.global_cursor.get(part_id);
    let old_payload = old_state
        .payload
        .as_ref()
        .expect("live member must still have payload");
    if let Some(old) = part.members.get_mut(&obj_id) {
        part.bus.remove_evt(old.changed_at);
        old.removed_at = Some(cursor);
        old.changed_at = cursor;
    }
    apply_bucket_transition(
        part,
        obj_id,
        cursor,
        BucketMemberKind::Live(&old_payload),
        BucketMemberKind::Dead,
    );
    part.latest_cursor = cursor;
    part.bus
        .queue_evt(PartEvent::Deleted(big_sync_core::rpc::ObjRemoved {
            cursor,
            part_id,
            obj_id,
        }));
    let remove_obj = if let Some(obj) = guard.objs.get_mut(&obj_id) {
        obj.parts.remove(&part_id);
        obj.sync_version = version;
        obj.sync_stamp = stamp.clone();
        obj.parts.is_empty()
    } else {
        false
    };
    if remove_obj {
        let cursor = part.latest_cursor;
        guard
            .tombstoned_objs
            .insert(obj_id, (cursor, version, stamp));
        guard.objs.remove(&obj_id);
    }

    part.flush(&mut guard.global_cursor);
}

#[async_trait]
impl ScopedIdResolver for MemoryPartStore {
    async fn resolve_part(&self, part: &ScopedPartRef) -> Res<PartId> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let scope_id = guard.scope_id(&part.scope);
            let key = (scope_id, Arc::<str>::clone(&part.part));
            if let Some(part_id) = guard.parts_by_scoped_ref.get(&key).copied() {
                guard.parts.entry(part_id).or_default();
                return Ok(part_id);
            }

            let part_id = PartId(Self::next_byte32_id(&mut guard.next_part_id, 1));
            let old = guard.parts_by_scoped_ref.insert(key.clone(), part_id);
            assert!(old.is_none(), "fishy");
            let old = guard.scoped_refs_by_part.insert(part_id, key);
            assert!(old.is_none(), "fishy");
            guard.parts.entry(part_id).or_default();
            Ok(part_id)
        })
    }

    async fn resolve_obj(&self, obj: &ScopedObjRef) -> Res<ObjId> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let scope_id = guard.scope_id(&obj.scope);
            let key = (scope_id, Arc::<str>::clone(&obj.obj));
            if let Some(obj_id) = guard.objs_by_scoped_ref.get(&key).copied() {
                return Ok(obj_id);
            }

            #[cfg(test)]
            let obj_id = test_scoped_obj_id(obj);
            #[cfg(not(test))]
            let obj_id = ObjId(Self::next_byte32_id(&mut guard.next_obj_id, 2));
            let old = guard.objs_by_scoped_ref.insert(key.clone(), obj_id);
            assert!(old.is_none(), "fishy");
            let old = guard.scoped_refs_by_obj.insert(obj_id, key);
            assert!(old.is_none(), "fishy");
            Ok(obj_id)
        })
    }

    async fn scoped_part(&self, part_id: PartId) -> Res<ScopedPartRef> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let (scope_id, part) = guard
                .scoped_refs_by_part
                .get(&part_id)
                .cloned()
                .ok_or_else(|| ferr!("part id {part_id} is not scoped in memory part store"))?;
            Ok(ScopedPartRef::new(guard.scope_ref(scope_id), part))
        })
    }

    async fn scoped_obj(&self, obj_id: ObjId) -> Res<ScopedObjRef> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let (scope_id, obj) = guard
                .scoped_refs_by_obj
                .get(&obj_id)
                .cloned()
                .ok_or_else(|| ferr!("obj id {obj_id} is not scoped in memory part store"))?;
            Ok(ScopedObjRef::new(guard.scope_ref(scope_id), obj))
        })
    }
}

impl PartState {
    fn flush(&mut self, global_cursor: &mut GlobalCursor) {
        for ii in self.bus.events_to_drop.drain(..) {
            self.events.remove(&ii);
            global_cursor.drop_cur(ii);
        }
        for evt in self.bus.buf.drain(..) {
            let (cursor, sub_evt) = match &evt {
                PartEvent::Upserted(inner) => (inner.cursor, SubEvent::Upserted(inner.clone())),
                PartEvent::Deleted(inner) => (inner.cursor, SubEvent::Deleted(inner.clone())),
            };
            self.events.insert(cursor, evt);
            for (ii, sub) in self.bus.subs.iter().enumerate() {
                if sub.send(sub_evt.clone()).is_err() {
                    self.bus.subs_to_drop.push(ii)
                }
            }
            self.bus.subs_to_drop.sort_unstable();
            self.bus.subs_to_drop.dedup();
            self.bus.subs_to_drop.reverse();
            for ii in self.bus.subs_to_drop.drain(..) {
                self.bus.subs.swap_remove(ii);
            }
        }
    }
}
impl MemorySubsBus {
    fn queue_evt(&mut self, evt: PartEvent) {
        self.buf.push(evt);
    }
    fn remove_evt(&mut self, idx: CursorIndex) {
        self.events_to_drop.push(idx);
    }
}
impl GlobalCursor {
    fn get(&mut self, part_id: PartId) -> CursorIndex {
        let ii = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.event_to_part.insert(ii, part_id);
        ii
    }

    fn drop_cur(&mut self, ii: CursorIndex) {
        self.event_to_part.remove(&ii);
    }
}

#[async_trait]
impl HostPartitionStore for MemoryPartStore {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out = HashMap::new();
            for part_id in parts {
                let Some(part) = guard.parts.get(&part_id) else {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
                out.insert(
                    part_id,
                    PartSummary {
                        latest_cursor: part.latest_cursor,
                        member_count: part
                            .members
                            .values()
                            .filter(|member| member.removed_at.is_none())
                            .count() as _,
                    },
                );
            }
            Ok(out)
        }))
    }
    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.bucket_summary(part_id, id)
        }))
    }
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.changed_bucket_summaries(req.part_id, req.offset, req.since, req.limit_hint)
        });
        tracing::debug!(
            part_id = %req.part_id,
            offset = ?req.offset,
            since = req.since,
            limit_hint = req.limit_hint,
            bucket_count = result.as_ref().map(|b| b.len()).unwrap_or(0),
            "memory store get changed buckets"
        );
        Ok(result)
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let Some(part) = guard.parts.get(&req.part_id) else {
                return Err(LeafBucketsError::UnkownPart);
            };
            let mut bucks = HashMap::new();
            for buck_req in req.buckets {
                let buck_id = buck_req.buck_id;
                let items = guard.bucket_items_for_path(req.part_id, buck_id);
                let start = match buck_req.after {
                    Some(after) => items
                        .iter()
                        .position(|(obj_id, _, _)| *obj_id > after)
                        .unwrap_or(items.len()),
                    None => 0,
                };
                let take = req.limit_hint.max(1) as usize;
                let end = (start + take).min(items.len());
                let done = end == items.len();
                let next_after = if done || start >= end {
                    None
                } else {
                    Some(items[end - 1].0)
                };
                let entries = items
                    .into_iter()
                    .skip(start)
                    .take(take)
                    .map(|(obj_id, _cursor, dead)| {
                        let fp = if dead {
                            Fingerprint::new(
                                &req.seed,
                                &("big-sync-obj-fp-v1", obj_id, serde_json::Value::Null),
                            )
                        } else {
                            let payload = part
                                .members
                                .get(&obj_id)
                                .and_then(|member| member.removed_at.is_none().then_some(()))
                                .and_then(|_| guard.objs.get(&obj_id))
                                .and_then(|obj| obj.payload.clone())
                                .expect(ERROR_IMPOSSIBLE);
                            Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
                        };
                        BucketObjPageEntry { obj_id, dead, fp }
                    })
                    .collect();
                bucks.insert(
                    buck_id,
                    LeafBucketPage {
                        entries,
                        next_after,
                        done,
                    },
                );
            }
            Ok(LeafBucketResult {
                seed: req.seed,
                bucks,
            })
        });
        tracing::debug!(
            part_id = %req.part_id,
            bucket_count = result.as_ref().map(|r| r.bucks.len()).unwrap_or(0),
            "memory store leaf buckets"
        );
        Ok(result)
    }
    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard
                .parts
                .get(&part_id)
                .map(|part| {
                    part.members
                        .values()
                        .filter(|member| member.removed_at.is_none())
                        .count() as u64
                })
                .unwrap_or(0))
        })
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.objs.get(&obj_id).and_then(|obj| obj.payload.clone()))
        })
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard
                .objs
                .get(&obj_id)
                .map(|deets| deets.parts.iter().copied().collect())
                .unwrap_or_default())
        })
    }

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_count = parts.len(), "memory store upsert obj");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            upsert_obj_locked(
                &mut *guard,
                self.owner_peer_id,
                obj_id,
                payload,
                parts,
                None,
                None,
            );
            Ok(())
        })
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_count = parts.len(), "memory store add obj to parts");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            let stamp = next_obj_sync_stamp_locked(guard, self.owner_peer_id, obj_id);
            let version = stamp.seq;
            guard.tombstoned_objs.remove(&obj_id);
            let obj = guard.objs.entry(obj_id).or_default();
            let payload = obj.payload.clone().expect(ERROR_IMPOSSIBLE);
            obj.sync_version = version;
            obj.sync_stamp = stamp;
            obj.parts.extend(&parts);
            for &part_id in &parts {
                let part = guard.parts.entry(part_id).or_default();
                let cursor = guard.global_cursor.get(part_id);
                let old_state = part.members.get(&obj_id).cloned();
                match old_state {
                    Some(state) if state.removed_at.is_none() => continue,
                    Some(state) => {
                        if let Some(old_removed_at) = state.removed_at {
                            part.bus.remove_evt(old_removed_at);
                        }
                        apply_bucket_transition(
                            part,
                            obj_id,
                            cursor,
                            BucketMemberKind::Dead,
                            BucketMemberKind::Live(&payload),
                        );
                        if let Some(old) = part.members.get_mut(&obj_id) {
                            old.changed_at = cursor;
                            old.removed_at = None;
                            old.payload = Some(payload.clone());
                        }
                    }
                    None => {
                        apply_bucket_transition(
                            part,
                            obj_id,
                            cursor,
                            BucketMemberKind::Absent,
                            BucketMemberKind::Live(&payload),
                        );
                        part.members.insert(
                            obj_id,
                            PartMemberState {
                                payload: Some(payload.clone()),
                                changed_at: cursor,
                                removed_at: None,
                            },
                        );
                    }
                }
                part.latest_cursor = cursor;
                part.bus
                    .queue_evt(PartEvent::Upserted(big_sync_core::rpc::ObjUpserted {
                        cursor,
                        part_id,
                        obj_id,
                        payload: payload.clone(),
                    }));
                part.flush(&mut guard.global_cursor);
            }
            Ok(())
        })
    }
    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_id = %part_id, "memory store remove obj from part");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            remove_obj_from_part_locked(
                &mut *guard,
                self.owner_peer_id,
                obj_id,
                part_id,
                None,
                None,
            );
            Ok(())
        })
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);

            Ok(guard
                .peer_part_cursors
                .get(&(peer_id, part_id))
                .cloned()
                .unwrap_or_default())
        })
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()> {
        tracing::debug!(peer_id = %peer_id, part_id = %part_id, cursor, "memory store set peer part cursor");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.peer_part_cursors.insert((peer_id, part_id), cursor);
            Ok(())
        })
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        let part_count = parts.len();
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out = HashMap::new();
            for part_id in parts {
                let mut next_cursor = None;
                let mut events = vec![];
                let Some(part) = guard.parts.get(&part_id) else {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
                for (&ii, evt) in part.events.range(cursor.saturating_add(1)..) {
                    if events.len() >= limit as usize {
                        next_cursor = Some(ii);
                        break;
                    }
                    events.push(match evt {
                        PartEvent::Upserted(inner) => PartEvent::Upserted(inner.clone()),
                        PartEvent::Deleted(inner) => PartEvent::Deleted(inner.clone()),
                    })
                }
                out.insert(
                    part_id,
                    PartPage {
                        events,
                        next_cursor,
                    },
                );
            }
            Ok(out)
        });
        tracing::debug!(
            part_count,
            cursor,
            limit,
            page_count = result.as_ref().map(|r| r.len()).unwrap_or(0),
            "memory store list events"
        );
        Ok(result)
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::UnboundedReceiver<SubEvent>, ListPartsError>> {
        tracing::debug!(part_count = reqs.parts.len(), "memory store subscribe");
        // make sure the parts exist first
        if let Err(err) = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            for req in &reqs.parts {
                let part_id = req.part_id;
                if !guard.parts.contains_key(&part_id) {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
            }
            Ok(())
        }) {
            return Ok(Err(err));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let state = Arc::clone(&self.inner);
        let fut = async move {
            let mut replay_done = false;
            let limit = 50;
            let mut events = Vec::with_capacity(limit);
            let parts: HashSet<_> = reqs.parts.iter().map(|req| req.part_id).collect();
            let mut cursor = reqs
                .parts
                .iter()
                .map(|req| req.cursor)
                .min()
                .unwrap_or(0)
                .saturating_add(1);
            while !replay_done {
                let mut next_cursor = cursor;
                surelock::key::lock_scope(|key| {
                    let (mut guard, _key) = key.lock(&state);
                    let guard = &mut *guard;

                    for (&ii, part_id) in guard.global_cursor.event_to_part.range(cursor..) {
                        if events.len() >= limit {
                            next_cursor = ii + 1;
                            break;
                        }
                        let part = guard.parts.get(part_id).expect(ERROR_IMPOSSIBLE);
                        let evt = part.events.get(&ii).expect(ERROR_UNRECONIZED);
                        if parts.contains(part_id) {
                            events.push(match evt {
                                PartEvent::Upserted(inner) => PartEvent::Upserted(inner.clone()),
                                PartEvent::Deleted(inner) => PartEvent::Deleted(inner.clone()),
                            })
                        }
                        next_cursor = ii + 1;
                    }
                });
                replay_done = events.is_empty();
                for evt in events.drain(..) {
                    if tx
                        .send(match evt {
                            PartEvent::Upserted(inner) => SubEvent::Upserted(inner.clone()),
                            PartEvent::Deleted(inner) => SubEvent::Deleted(inner.clone()),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                cursor = next_cursor;
            }
            if tx.send(SubEvent::ReplayComplete).is_err() {
                return;
            }
            surelock::key::lock_scope(|key| {
                let (mut guard, _key) = key.lock(&state);
                let guard = &mut *guard;

                for part_id in parts {
                    let part = guard.parts.get_mut(&part_id).expect(ERROR_IMPOSSIBLE);
                    part.bus.subs.push(tx.clone());
                }
            });
        };
        tokio::spawn(fut);
        Ok(Ok(rx))
    }
}

impl MemoryPartStore {
    pub async fn is_tombstoned(&self, obj_id: ObjId) -> Res<bool> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.tombstoned_objs.contains_key(&obj_id))
        })
    }

    pub async fn obj_sync_version(&self, obj_id: ObjId) -> Res<u64> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(obj_sync_version_locked(&guard, obj_id))
        })
    }

    pub(crate) async fn obj_sync_stamp(&self, obj_id: ObjId) -> Res<ObjSyncStamp> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(obj_sync_stamp_locked(&guard, obj_id))
        })
    }

    pub async fn get_peer_obj_payload(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
    ) -> Res<Option<Option<ObjPayload>>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.peer_obj_payloads.get(&(peer_id, obj_id)).cloned())
        })
    }

    pub async fn set_peer_obj_payload(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
        payload: Option<ObjPayload>,
    ) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.peer_obj_payloads.insert((peer_id, obj_id), payload);
            Ok(())
        })
    }

    #[tracing::instrument(
        skip(self, payload, parts),
        fields(obj_id = %obj_id, part_count = parts.len())
    )]
    pub(crate) async fn sync_upsert_obj(
        &self,
        obj_id: ObjId,
        payload: ObjPayload,
        parts: Vec<PartId>,
        expected_stamp: ObjSyncStamp,
        sync_version: u64,
        sync_stamp: ObjSyncStamp,
    ) -> Res<SyncMutationOutcome> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            if obj_sync_stamp_locked(guard, obj_id) != expected_stamp {
                tracing::trace!(
                    obj_id = %obj_id,
                    part_count = parts.len(),
                    expected_stamp = ?expected_stamp,
                    "sync upsert stale"
                );
                return Ok(SyncMutationOutcome::Stale);
            }
            upsert_obj_locked(
                guard,
                self.owner_peer_id,
                obj_id,
                payload,
                parts,
                Some(sync_version),
                Some(sync_stamp),
            );
            tracing::trace!(
                obj_id = %obj_id,
                part_count = guard
                    .objs
                    .get(&obj_id)
                    .map(|obj| obj.parts.len())
                    .unwrap_or_default(),
                expected_stamp = ?expected_stamp,
                "sync upsert applied"
            );
            Ok(SyncMutationOutcome::Applied)
        })
    }

    #[tracing::instrument(
        skip(self),
        fields(obj_id = %obj_id, part_id = %part_id)
    )]
    pub(crate) async fn sync_remove_obj_from_part(
        &self,
        obj_id: ObjId,
        part_id: PartId,
        expected_stamp: ObjSyncStamp,
        sync_version: u64,
        sync_stamp: ObjSyncStamp,
    ) -> Res<SyncMutationOutcome> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            if obj_sync_stamp_locked(guard, obj_id) != expected_stamp {
                tracing::trace!(
                    obj_id = %obj_id,
                    part_id = %part_id,
                    expected_stamp = ?expected_stamp,
                    "sync remove stale"
                );
                return Ok(SyncMutationOutcome::Stale);
            }
            let Some(part) = guard.parts.get(&part_id) else {
                tracing::trace!(
                    obj_id = %obj_id,
                    part_id = %part_id,
                    expected_stamp = ?expected_stamp,
                    "sync remove stale missing part"
                );
                return Ok(SyncMutationOutcome::Stale);
            };
            let Some(state) = part.members.get(&obj_id) else {
                tracing::trace!(
                    obj_id = %obj_id,
                    part_id = %part_id,
                    expected_stamp = ?expected_stamp,
                    "sync remove stale missing member"
                );
                return Ok(SyncMutationOutcome::Stale);
            };
            if state.removed_at.is_some() {
                tracing::trace!(
                    obj_id = %obj_id,
                    part_id = %part_id,
                    expected_stamp = ?expected_stamp,
                    "sync remove stale already removed"
                );
                return Ok(SyncMutationOutcome::Stale);
            }
            remove_obj_from_part_locked(
                guard,
                self.owner_peer_id,
                obj_id,
                part_id,
                Some(sync_version),
                Some(sync_stamp),
            );
            tracing::trace!(
                obj_id = %obj_id,
                part_id = %part_id,
                expected_stamp = ?expected_stamp,
                "sync remove applied"
            );
            Ok(SyncMutationOutcome::Applied)
        })
    }

    #[tracing::instrument(
        skip(self),
        fields(obj_id = %obj_id)
    )]
    pub(crate) async fn sync_tombstone_obj(
        &self,
        obj_id: ObjId,
        expected_stamp: ObjSyncStamp,
        sync_version: u64,
        sync_stamp: ObjSyncStamp,
    ) -> Res<SyncMutationOutcome> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            if obj_sync_stamp_locked(guard, obj_id) != expected_stamp {
                tracing::trace!(
                    obj_id = %obj_id,
                    expected_stamp = ?expected_stamp,
                    "sync tombstone stale"
                );
                return Ok(SyncMutationOutcome::Stale);
            }
            let Some(parts) = guard
                .objs
                .get(&obj_id)
                .map(|obj| obj.parts.iter().copied().collect::<Vec<_>>())
            else {
                if guard.tombstoned_objs.contains_key(&obj_id) {
                    return Ok(SyncMutationOutcome::Applied);
                }
                return Ok(SyncMutationOutcome::Stale);
            };
            for part_id in parts {
                remove_obj_from_part_locked(
                    guard,
                    self.owner_peer_id,
                    obj_id,
                    part_id,
                    Some(sync_version),
                    Some(sync_stamp.clone()),
                );
            }
            Ok(SyncMutationOutcome::Applied)
        })
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryPartStoreSnapshot {
    pub objs: BTreeMap<ObjId, MemoryObjSnapshot>,
    pub scoped_objs: BTreeMap<ScopedObjRef, MemoryScopedObjSnapshot>,
    pub peer_part_cursors: BTreeMap<(PeerId, PartId), CursorIndex>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryObjSnapshot {
    pub payload: Option<ObjPayload>,
    pub parts: BTreeSet<PartId>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryScopedObjSnapshot {
    pub payload: Option<ObjPayload>,
    pub parts: BTreeSet<ScopedPartRef>,
}

#[cfg(test)]
impl MemoryPartStore {
    pub(crate) async fn snapshot(&self) -> Res<MemoryPartStoreSnapshot> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let objs = guard
                .objs
                .iter()
                .map(|(&obj_id, obj)| {
                    (
                        obj_id,
                        MemoryObjSnapshot {
                            payload: obj.payload.clone(),
                            parts: obj.parts.iter().copied().collect(),
                        },
                    )
                })
                .collect();
            let scoped_objs = guard
                .objs
                .iter()
                .filter_map(|(&obj_id, obj)| {
                    let (scope_id, obj_name) = guard.scoped_refs_by_obj.get(&obj_id)?.clone();
                    let scope = guard.scope_ref(scope_id);
                    let parts = obj
                        .parts
                        .iter()
                        .filter_map(|part_id| {
                            let (scope_id, part_name) =
                                guard.scoped_refs_by_part.get(part_id)?.clone();
                            Some(ScopedPartRef::new(guard.scope_ref(scope_id), part_name))
                        })
                        .collect();
                    Some((
                        ScopedObjRef::new(scope, obj_name),
                        MemoryScopedObjSnapshot {
                            payload: obj.payload.clone(),
                            parts,
                        },
                    ))
                })
                .collect();
            Ok(MemoryPartStoreSnapshot {
                objs,
                scoped_objs,
                peer_part_cursors: guard.peer_part_cursors.clone().into_iter().collect(),
            })
        })
    }
}
