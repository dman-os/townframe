use crate::interlude::*;

use big_sync_core::keyed_frontier::{
    FrontierMutation, FrontierRead, FrontierReadLimits, FrontierRevision, KeyedFrontierReader,
    KeyedFrontierResult,
};
use big_sync_core::part_store::{CursorIndex, ObjPayload, PartDirtyCount};
use big_sync_core::rpc::{
    BucketMemberKind, BucketObjPageEntry, BucketSummary, BucketSummaryState,
    GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, ObjChanged, ObjRemovedFromPart, PartEvent, PartPage,
    PartSummary, SubEvent, SubPartsRequest,
};
use big_sync_core::{BuckId, Fingerprint, ObjKey, PartKey, PeerKey, mpsc};

use super::PartFrontierKey;
use super::{HostPartStore, PartScope, PartStoreStats, ReadTarget, bucket_index_bounds};
use super::{LEAF_PAGE_BYTE_BUDGET, leaf_entry_wire_bytes};
use crate::keyed_frontier::{
    MemoryKeyedFrontierSelector, MemoryKeyedFrontierSource, MemoryKeyedFrontierTable,
    MemoryKeyedFrontierView, open_memory_keyed_frontier,
};
#[cfg(test)]
use crate::test_support::{ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot};

use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;

structstruck::strike! {
    pub struct MemoryPartStore {
        inner: Arc<surelock::mutex::Mutex<
            #[derive(Default)]
            struct MemoryPartStoreScopeState {
                global_cursor:
                    #[derive(Default)]
                    struct GlobalCursor {
                        counter: std::sync::atomic::AtomicU64,
                    },
                parts: HashMap<
                    PartKey,
                    struct PartState {
                        #![derive(Default)]
                        latest_cursor: CursorIndex,
                        members: BTreeMap<
                            ObjKey,
                            #[derive(Clone)]
                            struct PartMemberState {
                                added_at: CursorIndex,
                                changed_at: CursorIndex,
                                removed_at: Option<CursorIndex>,
                                /// ADR 012 decision 1: the object's deepest bucket index.
                                /// Stored so a bucket's members are a range of *this*
                                /// rather than of the key, which stopped being possible.
                                buck_index: u16,
                            }
                        >,
                        bucket_stats: BTreeMap<BuckId, BucketSummaryState>,
                    }
                >,
                event_frontier: MemoryKeyedFrontierTable<PartFrontierKey, PartEvent>,
                bus: struct MemorySubsBus {
                    #![derive(Default)]
                    buf: Vec<PartEvent>,
                    /// Frontier keys whose *value* goes away without a `PartEvent` to say so:
                    /// `remove_obj_payload` drops an object's content, and the object lane's
                    /// frontier value is that content. A keyed frontier with no event to carry
                    /// needs this side channel; a `Delete` is a mutation like any other.
                    dropped_keys: Vec<PartFrontierKey>
                },
                objs: BTreeMap<
                    ObjKey,
                    struct ObjDeets {
                        #![derive(Default)]
                        payload: Option<ObjPayload>,
                        parts: HashSet<PartKey>,
                    }
                >,
                tombstoned_objs: HashMap<ObjKey, CursorIndex>,
                peer_part_cursors: HashMap<(PeerKey, PartKey), CursorIndex>,
                members: HashMap<
                    PartKey,
                    HashMap<
                        PeerKey,
                        #[derive(Clone)]
                        struct PartAccessState {
                            access: keyhive_core::access::Access,
                            changed_at: CursorIndex,
                        }
                    >
                >,
            }
        >>,
        hidden_parts: Arc<HashSet<PartKey>>,
    }
}

#[derive(Clone)]
struct MemoryPartEventSource {
    state: Arc<surelock::mutex::Mutex<MemoryPartStoreScopeState>>,
}

#[async_trait]
impl MemoryKeyedFrontierSource<PartFrontierKey, PartEvent> for MemoryPartEventSource {
    async fn view(
        &self,
    ) -> KeyedFrontierResult<MemoryKeyedFrontierView<PartFrontierKey, PartEvent>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.state);
            guard.event_frontier.view()
        }))
    }
}

#[derive(Clone)]
struct MemoryPartEventSelector {
    /// `Some(after)` selects every key in the scope (the `All` local scope),
    /// emitting revisions newer than `after` and ignoring the per-part and
    /// per-object bounds below.
    all: Option<CursorIndex>,
    part_cursors: HashMap<PartKey, CursorIndex>,
    objects: HashSet<ObjKey>,
    object_bounds: HashMap<ObjKey, CursorIndex>,
}

impl MemoryKeyedFrontierSelector<PartFrontierKey> for MemoryPartEventSelector {
    fn lower_bound(&self, key: &PartFrontierKey) -> Option<FrontierRevision> {
        if let Some(after) = self.all {
            return Some(after);
        }
        match key {
            PartFrontierKey::Object(obj_id) => self.object_bounds.get(obj_id).copied(),
            PartFrontierKey::Part { obj_id, part_id } => {
                match (
                    self.object_bounds.get(obj_id).copied(),
                    self.part_cursors.get(part_id).copied(),
                ) {
                    (Some(object_bound), Some(part_bound)) => Some(object_bound.min(part_bound)),
                    (Some(bound), None) | (None, Some(bound)) => Some(bound),
                    (None, None) => None,
                }
            }
        }
    }

    fn emit_empty_progress(&self) -> bool {
        true
    }

    fn initial_after(&self) -> FrontierRevision {
        if let Some(after) = self.all {
            return after;
        }
        self.part_cursors
            .values()
            .copied()
            .chain(self.object_bounds.values().copied())
            .min()
            .unwrap_or(0)
    }
}

impl Default for MemoryPartStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryPartStore {
    pub fn new() -> Self {
        Self::with_config(Default::default())
    }

    pub fn with_config(config: super::HostPartStoreConfig) -> Self {
        Self {
            inner: Arc::new(surelock::mutex::Mutex::new(default())),
            hidden_parts: Arc::new(config.hidden_parts),
        }
    }
}

impl MemoryPartStoreScopeState {
    fn bucket_items_for_path(
        &self,
        part_id: PartKey,
        path: BuckId,
    ) -> Vec<(ObjKey, CursorIndex, bool)> {
        let Some(part) = self.parts.get(&part_id) else {
            return Vec::new();
        };
        // The index is a hash of the object key, so a bucket's members are a range of the
        // stored index and there is no key range left to seek to in `members`. Scanning
        // keeps the obj_id order the page cursor needs; the sqlite stores put the same
        // index in a column and let the range predicate use it, which matters more there.
        let (lower, upper) = bucket_index_bounds(path);
        let mut items = Vec::new();
        for (obj_id, member) in &part.members {
            if member.buck_index < lower {
                continue;
            }
            if upper.is_some_and(|upper| member.buck_index >= upper) {
                continue;
            }
            let cursor = member.removed_at.unwrap_or(member.changed_at);
            items.push((obj_id.clone(), cursor, member.removed_at.is_some()));
        }
        items
    }

    fn bucket_summary(&self, part_id: PartKey, path: BuckId) -> BucketSummary {
        self.parts
            .get(&part_id)
            .and_then(|part| part.bucket_stats.get(&path).cloned())
            .unwrap_or_default()
            .summary(path)
    }

    fn changed_bucket_summaries(
        &self,
        part_id: PartKey,
        offset: BuckId,
        to_level: big_sync_core::rpc::BuckLevel,
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
                buck_id.level() <= to_level && summary.changed_at() > since
            })
            .map(|(&buck_id, summary)| summary.summary(buck_id))
            .collect();
        if buckets.is_empty() {
            return Ok(buckets);
        }
        // `limit_hint` is the response's page bound with `BuckId::ARITY` extra
        // siblings allowed (see `GetChangedBucketsRequest`), so a zero hint is a
        // zero-bucket page rather than an unspecified page size.
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

impl PartState {
    fn apply_bucket_transition(
        &mut self,
        obj_id: ObjKey,
        cursor: CursorIndex,
        old: BucketMemberKind<'_>,
        new: BucketMemberKind<'_>,
    ) {
        let deepest = BuckId::deepest_from_obj_key(&obj_id);
        for level in 0..=BuckId::MAX_LEVEL {
            let buck_id = deepest.to_level(level);
            let agg = self.bucket_stats.entry(buck_id).or_default();
            agg.apply_transition(buck_id, obj_id.clone(), cursor, old, new);
        }
    }
}
/// Whether `part` grants `principal` fetch access.
fn part_permits(
    members: &HashMap<PartKey, HashMap<PeerKey, PartAccessState>>,
    part_id: PartKey,
    principal: PeerKey,
) -> bool {
    members
        .get(&part_id)
        .and_then(|member_map| member_map.get(&principal))
        .is_some_and(|state| state.access.is_fetcher())
}

fn project_part_event(
    state: &MemoryPartStoreScopeState,
    event: PartEvent,
    selector: &MemoryPartEventSelector,
    subscriber: PeerKey,
) -> Option<SubEvent> {
    let obj_id = match &event {
        PartEvent::Changed(inner) => inner.obj_id.clone(),
        PartEvent::Removed(inner) => inner.obj_id.clone(),
    };
    // Candidate parts: what the event names, else the object's membership.
    let scope = match &event {
        PartEvent::Changed(inner) if !inner.part_ids.is_empty() => {
            PartScope::AnyOf(inner.part_ids.clone())
        }
        PartEvent::Changed(_) => PartScope::FromObject,
        PartEvent::Removed(inner) => PartScope::Part(inner.part_id.clone()),
    };
    // A concrete subscriber always filters.
    let Some(readable) = state.permitted_parts(scope, obj_id.clone(), Some(subscriber)) else {
        unreachable!("{}", ERROR_IMPOSSIBLE)
    };
    if readable.is_empty() {
        return None;
    }
    let selected = readable
        .into_iter()
        .filter(|part_id| selector.part_cursors.contains_key(part_id))
        .collect::<Vec<_>>();
    let object_selected = selector.objects.contains(&obj_id);
    match event {
        PartEvent::Changed(mut inner) if !selected.is_empty() => {
            inner.part_ids = selected;
            Some(SubEvent::Changed(inner))
        }
        PartEvent::Changed(inner) if object_selected => {
            Some(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor: inner.cursor,
                part_ids: Vec::new(),
                obj_id: inner.obj_id,
                payload: inner.payload,
            }))
        }
        PartEvent::Removed(inner) if !selected.is_empty() => Some(SubEvent::Removed(inner)),
        // An object reader that did not select the part has nothing to book for a membership
        // transition: a deletion is a part-lane fact, and a payload-less `Changed` would mean
        // *resolve the membership*, which the cursor machine books as a content delivery.
        _ => None,
    }
}

impl MemoryPartStoreScopeState {
    /// The subset of `scope`'s candidate parts that `principal` may read; `None` when
    /// unfiltered (local principal).
    fn permitted_parts(
        &self,
        scope: PartScope,
        obj_id: ObjKey,
        principal: Option<PeerKey>,
    ) -> Option<Vec<PartKey>> {
        let principal = principal?;
        let candidates: Vec<PartKey> = match scope {
            PartScope::Part(part_id) => vec![part_id],
            PartScope::AnyOf(part_ids) => part_ids,
            PartScope::FromObject => {
                let mut resolved = self
                    .objs
                    .get(&obj_id)
                    .map(|deets| deets.parts.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                resolved.sort_unstable();
                resolved
            }
        };
        Some(
            candidates
                .into_iter()
                .filter(|part_id| part_permits(&self.members, part_id.clone(), principal.clone()))
                .collect(),
        )
    }

    fn flush(&mut self) {
        let pending_events = std::mem::take(&mut self.bus.buf);
        let dropped_keys = std::mem::take(&mut self.bus.dropped_keys);
        if pending_events.is_empty() && dropped_keys.is_empty() {
            return;
        }
        let event_revision = pending_events
            .iter()
            .map(|event| match event {
                PartEvent::Changed(inner) => inner.cursor,
                PartEvent::Removed(inner) => inner.cursor,
            })
            .max();
        // A flush that only drops a key is still a mutation of the scope's state, and the
        // keyed frontier refuses a revision it has already applied.
        let revision = match event_revision {
            Some(revision) => {
                assert!(pending_events.iter().all(|event| match event {
                    PartEvent::Changed(inner) => inner.cursor == revision,
                    PartEvent::Removed(inner) => inner.cursor == revision,
                }));
                revision
            }
            None => self.global_cursor.next(),
        };
        let mut frontier_mutations = Vec::new();
        for key in dropped_keys {
            frontier_mutations.push(FrontierMutation::Delete { key });
        }
        for evt in pending_events {
            let (evt_parts, evt_obj_id) = match &evt {
                PartEvent::Changed(inner) => (inner.part_ids.clone(), inner.obj_id.clone()),
                PartEvent::Removed(inner) => (vec![inner.part_id.clone()], inner.obj_id.clone()),
            };
            if evt_parts.is_empty() {
                frontier_mutations.push(FrontierMutation::Put {
                    key: PartFrontierKey::Object(evt_obj_id),
                    value: evt.clone(),
                });
            } else {
                for part_id in &evt_parts {
                    let key = PartFrontierKey::Part {
                        obj_id: evt_obj_id.clone(),
                        part_id: part_id.clone(),
                    };
                    match &evt {
                        PartEvent::Removed(_) => {
                            frontier_mutations.push(FrontierMutation::Delete { key });
                        }
                        PartEvent::Changed(inner) => {
                            let mut inner = inner.clone();
                            inner.part_ids = vec![part_id.clone()];
                            frontier_mutations.push(FrontierMutation::Put {
                                key,
                                value: PartEvent::Changed(inner),
                            });
                        }
                    }
                }
            }
        }
        self.event_frontier
            .apply_at(revision, frontier_mutations)
            .expect("part events must advance the keyed frontier revision");
    }
}

impl MemorySubsBus {
    fn queue_evt(&mut self, evt: PartEvent) {
        self.buf.push(evt);
    }

    /// Drop a frontier key whose value goes away without a `PartEvent` that says so.
    fn queue_key_drop(&mut self, key: PartFrontierKey) {
        self.dropped_keys.push(key);
    }
}
impl GlobalCursor {
    /// The newest allocated revision. A read: it never allocates, so callers that only want
    /// to *observe* the cursor — reporting it, or stamping state that allocates no revision
    /// of its own — cannot advance the cursor space.
    fn peek(&self) -> CursorIndex {
        self.counter.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Allocate and return the next revision. Only write paths may call this.
    fn next(&self) -> CursorIndex {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1
    }
}

#[async_trait]
impl HostPartStore for MemoryPartStore {
    async fn latest_revision(&self) -> Res<CursorIndex> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.global_cursor.peek()
        }))
    }

    async fn summarize_parts(
        &self,
        parts: HashSet<PartKey>,
    ) -> Res<Result<HashMap<PartKey, PartSummary>, ListPartsError>> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out = HashMap::new();
            for part_id in parts {
                if self.hidden_parts.contains(&part_id) {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                }
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
                        deepest_bucket_level: BuckId::MAX_LEVEL,
                    },
                );
            }
            Ok(out)
        }))
    }
    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        // A part the subscriber may not read has to read as unknown, and this comes
        // before the walk itself so no part of the answer depends on the refusal.
        if self
            .read_denied(ReadTarget::Part(req.part_id.clone()), subscriber)
            .await?
        {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        // Hidden parts are invisible to remote part access, exactly as
        // `summarize_parts` and this store's own subscription filter treat them.
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.changed_bucket_summaries(
                req.part_id.clone(),
                req.offset,
                req.to_level,
                req.since,
                req.limit_hint,
            )
        });
        tracing::debug!(
            part_id = %req.part_id,
            offset = ?req.offset,
            since = req.since,
            limit_hint = req.limit_hint,
            bucket_count = result.as_ref().map(|buck| buck.len()).unwrap_or(0),
            "memory store get changed buckets"
        );
        Ok(result)
    }
    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        // As above: an unreadable part reads as unknown, never as an empty page.
        if self
            .read_denied(ReadTarget::Part(req.part_id.clone()), subscriber)
            .await?
        {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let Some(part) = guard.parts.get(&req.part_id) else {
                return Err(LeafBucketsError::UnkownPart);
            };
            let mut bucks = HashMap::new();
            for buck_req in req.buckets {
                let buck_id = buck_req.buck_id;
                if guard
                    .bucket_summary(req.part_id.clone(), buck_id)
                    .changed_at
                    <= req.since
                {
                    bucks.insert(
                        buck_id,
                        LeafBucketPage {
                            entries: Vec::new(),
                            next_after: None,
                            done: true,
                        },
                    );
                    continue;
                }
                let items = guard.bucket_items_for_path(req.part_id.clone(), buck_id);
                let start = match buck_req.after {
                    Some(after) => items
                        .iter()
                        .position(|(obj_id, _, _)| *obj_id > after)
                        .unwrap_or(items.len()),
                    None => 0,
                };
                // `LeafBucketsRequest::limit_hint` is a hint, not a bound: zero
                // means no preference, so the smallest useful page is one entry.
                let take = req.limit_hint.max(1) as usize;
                // The entry hint and the byte budget each bound the page, and whichever is
                // reached first ends it. The first entry is always taken, because a page with
                // no entries reads as `done` while entries remain, which would strand the tail.
                let mut bytes = 0usize;
                let mut taken = 0usize;
                for (obj_id, _cursor, _dead) in items.iter().skip(start).take(take) {
                    let entry_bytes = leaf_entry_wire_bytes(obj_id.as_bytes().len());
                    if taken > 0 && bytes + entry_bytes > LEAF_PAGE_BYTE_BUDGET {
                        break;
                    }
                    bytes += entry_bytes;
                    taken += 1;
                }
                let end = start + taken;
                let done = end == items.len();
                let next_after = if done || start >= end {
                    None
                } else {
                    Some(items[end - 1].0.clone())
                };
                let entries = items
                    .into_iter()
                    .skip(start)
                    .take(taken)
                    .map(|(obj_id, _cursor, dead)| {
                        let fp = if dead {
                            Fingerprint::new(
                                &req.seed,
                                &(
                                    "big-sync-obj-fp-v1",
                                    obj_id.clone(),
                                    serde_json::Value::Null,
                                ),
                            )
                        } else {
                            let payload = part
                                .members
                                .get(&obj_id)
                                .and_then(|member| member.removed_at.is_none().then_some(()))
                                .and_then(|_| guard.objs.get(&obj_id))
                                .and_then(|obj| obj.payload.clone())
                                .unwrap_or(serde_json::Value::Null);
                            Fingerprint::new(
                                &req.seed,
                                &("big-sync-obj-fp-v1", obj_id.clone(), payload),
                            )
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
            bucket_count = result.as_ref().map(|res| res.bucks.len()).unwrap_or(0),
            "memory store leaf buckets"
        );
        Ok(result)
    }

    async fn member_count(&self, part_id: PartKey) -> Res<u64> {
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

    async fn part_dirty_count(
        &self,
        part_id: PartKey,
        principal: Option<PeerKey>,
        since: CursorIndex,
    ) -> Res<PartDirtyCount> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            // A member carries the cursor of its last transition, which for a removed
            // member is the removal itself — the same value sqlite keeps in `txid`.
            let member_changes = guard
                .parts
                .get(&part_id)
                .map(|part| {
                    part.members
                        .values()
                        .filter(|member| member.removed_at.unwrap_or(member.changed_at) > since)
                        .count() as u64
                })
                .unwrap_or(0);
            // One row per (part, principal), so 0 or 1 in practice. A revocation
            // deletes that row, so a revocation does not count here. `None` is the
            // local principal, which access rows do not gate, so it has no access
            // half.
            let access_changes = principal
                .and_then(|principal| {
                    guard
                        .members
                        .get(&part_id)
                        .and_then(|member_map| member_map.get(&principal))
                        .map(|state| u64::from(state.changed_at > since))
                })
                .unwrap_or(0);
            Ok(PartDirtyCount {
                member_changes,
                access_changes,
            })
        })
    }
    async fn obj_payload(&self, obj_id: ObjKey) -> Res<Option<ObjPayload>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.objs.get(&obj_id).and_then(|obj| obj.payload.clone()))
        })
    }

    async fn get_bucket_summary(&self, part_id: PartKey, id: BuckId) -> Res<BucketSummary> {
        Ok(surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.bucket_summary(part_id, id)
        }))
    }

    async fn obj_parts(&self, obj_id: ObjKey) -> Res<Vec<PartKey>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard
                .objs
                .get(&obj_id)
                .map(|deets| deets.parts.iter().cloned().collect())
                .unwrap_or_default())
        })
    }

    async fn obj_exists(&self, obj_id: ObjKey) -> Res<bool> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            Ok(guard.objs.contains_key(&obj_id) || guard.tombstoned_objs.contains_key(&obj_id))
        })
    }

    async fn set_obj_payload(&self, obj_id: ObjKey, payload: ObjPayload) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            guard.tombstoned_objs.remove(&obj_id);
            let obj_state = guard.objs.entry(obj_id.clone()).or_default();
            let old_payload = obj_state.payload.replace(payload.clone());
            let desired_parts = obj_state.parts.clone();
            if desired_parts.is_empty() {
                let cursor = guard.global_cursor.next();
                guard
                    .bus
                    .queue_evt(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor,
                        part_ids: Vec::new(),
                        obj_id,
                        payload,
                    }));
                guard.flush();
                return Ok(());
            }
            let cursor = guard.global_cursor.next();
            if let Some(old_payload) = old_payload {
                for part_id in &desired_parts {
                    let part = guard.parts.get_mut(part_id).expect(ERROR_IMPOSSIBLE);
                    part.apply_bucket_transition(
                        obj_id.clone(),
                        cursor,
                        BucketMemberKind::Live(&old_payload),
                        BucketMemberKind::Live(&payload),
                    );
                    let part_obj_state = part.members.get_mut(&obj_id).expect(ERROR_IMPOSSIBLE);
                    part_obj_state.changed_at = cursor;
                    part.latest_cursor = cursor;
                }
                guard
                    .bus
                    .queue_evt(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor,
                        part_ids: desired_parts.into_iter().collect(),
                        obj_id,
                        payload,
                    }));
            } else {
                for part_id in desired_parts {
                    let part = guard.parts.entry(part_id.clone()).or_default();
                    part.apply_bucket_transition(
                        obj_id.clone(),
                        cursor,
                        BucketMemberKind::Absent,
                        BucketMemberKind::Live(&payload),
                    );
                    part.members.insert(
                        obj_id.clone(),
                        PartMemberState {
                            buck_index: BuckId::deepest_from_obj_key(&obj_id).index(),
                            added_at: cursor,
                            changed_at: cursor,
                            removed_at: None,
                        },
                    );
                    part.latest_cursor = cursor;
                    guard
                        .bus
                        .queue_evt(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                            cursor,
                            part_ids: vec![part_id.clone()],
                            obj_id: obj_id.clone(),
                            payload: payload.clone(),
                        }));
                }
            }
            guard.flush();
            Ok(())
        })
    }
    async fn add_obj_to_parts(&self, obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_count = parts.len(), "memory store add obj to parts");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            // `obj_id` is used for the whole loop below, so every by-value consumer gets
            // its own copy; only the map lookups borrow it.
            let obj_state = guard.objs.entry(obj_id.clone()).or_default();

            guard.tombstoned_objs.remove(&obj_id);
            let Some(payload) = obj_state.payload.clone() else {
                obj_state.parts.extend(parts);
                return Ok(());
            };
            obj_state.parts.extend(parts.iter().cloned());
            let cursor = guard.global_cursor.next();
            for part_id in &parts {
                let part = guard.parts.entry(part_id.clone()).or_default();
                let old_state = part.members.get(&obj_id).cloned();
                match old_state {
                    Some(state) if state.removed_at.is_none() => continue,
                    Some(_state) => {
                        part.apply_bucket_transition(
                            obj_id.clone(),
                            cursor,
                            BucketMemberKind::Dead,
                            BucketMemberKind::Live(&payload),
                        );
                        if let Some(old) = part.members.get_mut(&obj_id) {
                            // The absent-to-present transition re-stamps: a re-add after a
                            // removal is a new add, and the tombstone predicate is about when a
                            // row most recently became present.
                            old.added_at = cursor;
                            old.changed_at = cursor;
                            old.removed_at = None;
                        }
                    }
                    None => {
                        part.apply_bucket_transition(
                            obj_id.clone(),
                            cursor,
                            BucketMemberKind::Absent,
                            BucketMemberKind::Live(&payload),
                        );
                        part.members.insert(
                            obj_id.clone(),
                            PartMemberState {
                                buck_index: BuckId::deepest_from_obj_key(&obj_id).index(),
                                added_at: cursor,
                                changed_at: cursor,
                                removed_at: None,
                            },
                        );
                    }
                }
                part.latest_cursor = cursor;
                guard
                    .bus
                    .queue_evt(PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor,
                        part_ids: vec![part_id.clone()],
                        obj_id: obj_id.clone(),
                        payload: payload.clone(),
                    }));
            }
            guard.flush();
            Ok(())
        })
    }

    async fn remove_obj_from_part(&self, obj_id: ObjKey, part_id: PartKey) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, part_id = %part_id, "memory store remove obj from part");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;

            let Some(obj_state) = guard.objs.get_mut(&obj_id) else {
                return Ok(());
            };
            obj_state.parts.remove(&part_id);
            if obj_state.payload.is_none() {
                if obj_state.parts.is_empty() {
                    guard.objs.remove(&obj_id);
                }
                return Ok(());
            }

            let part = guard.parts.entry(part_id.clone()).or_default();
            let Some(old_state) = part.members.get(&obj_id).cloned() else {
                return Ok(());
            };
            if old_state.removed_at.is_some() {
                return Ok(());
            }
            let cursor = guard.global_cursor.next();
            let old_payload = obj_state
                .payload
                .as_ref()
                .expect("visible membership requires payload");
            if let Some(old) = part.members.get_mut(&obj_id) {
                old.removed_at = Some(cursor);
                old.changed_at = cursor;
            }
            part.apply_bucket_transition(
                obj_id.clone(),
                cursor,
                BucketMemberKind::Live(old_payload),
                BucketMemberKind::Dead,
            );
            part.latest_cursor = cursor;
            guard
                .bus
                .queue_evt(PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                    cursor,
                    part_id,
                    obj_id: obj_id.clone(),
                }));
            if obj_state.parts.is_empty() {
                // The object keeps its payload: a membership removal is not a content removal.
                // The entry stays, with the tombstone beside it, until `remove_obj_payload`
                // drops the content.
                let cursor = part.latest_cursor;
                guard.tombstoned_objs.insert(obj_id.clone(), cursor);
            }

            guard.flush();

            Ok(())
        })
    }

    /// Remove the object from every part it is in and drop its payload, in one transaction.
    ///
    /// The per-part half is `remove_obj_from_part`'s: the same bucket transition, member
    /// tombstone and frontier deletion, at one shared revision so the parts it leaves move
    /// together. That shared revision is also why the public per-part method cannot be called
    /// in a loop.
    async fn remove_obj_payload(&self, obj_id: ObjKey) -> Res<()> {
        tracing::debug!(obj_id = %obj_id, "memory store remove obj payload");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;

            let Some(obj_state) = guard.objs.get(&obj_id) else {
                // Unknown object: nothing to drop.
                return Ok(());
            };
            let Some(old_payload) = obj_state.payload.clone() else {
                // No payload, nothing to drop: a live membership implies one, so an object
                // without one has no membership this could clear either.
                return Ok(());
            };
            let parts: Vec<PartKey> = obj_state.parts.iter().cloned().collect();
            let cursor = if parts.is_empty() {
                None
            } else {
                Some(guard.global_cursor.next())
            };
            for part_id in parts {
                let cursor = cursor.expect(ERROR_IMPOSSIBLE);
                let part = guard.parts.entry(part_id.clone()).or_default();
                let Some(member) = part.members.get(&obj_id) else {
                    continue;
                };
                if member.removed_at.is_some() {
                    continue;
                }
                if let Some(member) = part.members.get_mut(&obj_id) {
                    member.removed_at = Some(cursor);
                    member.changed_at = cursor;
                }
                part.apply_bucket_transition(
                    obj_id.clone(),
                    cursor,
                    BucketMemberKind::Live(&old_payload),
                    BucketMemberKind::Dead,
                );
                part.latest_cursor = cursor;
                guard
                    .bus
                    .queue_evt(PartEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                        cursor,
                        part_id,
                        obj_id: obj_id.clone(),
                    }));
            }
            guard.objs.remove(&obj_id);
            let tombstone = cursor.unwrap_or_else(|| guard.global_cursor.peek());
            guard.tombstoned_objs.insert(obj_id.clone(), tombstone);
            // The object lane's frontier value *is* the content, so it goes with the payload:
            // the removals above are what tells a reader holding the content to drop it.
            if guard
                .event_frontier
                .get(&PartFrontierKey::Object(obj_id.clone()))
                .is_some()
            {
                guard
                    .bus
                    .queue_key_drop(PartFrontierKey::Object(obj_id.clone()));
            }

            guard.flush();

            Ok(())
        })
    }

    async fn partless_objects(&self, limit: u32, after: Option<ObjKey>) -> Res<Vec<ObjKey>> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let range = (
                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                std::ops::Bound::Unbounded,
            );
            Ok(guard
                .objs
                .range(range)
                .filter(|(_, obj)| obj.payload.is_some() && obj.parts.is_empty())
                .take(usize::try_from(limit).expect(ERROR_IMPOSSIBLE))
                .map(|(obj_id, _)| obj_id.clone())
                .collect())
        })
    }

    async fn part_store_stats(&self) -> Res<PartStoreStats> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut stats = PartStoreStats {
                payload_objects: 0,
                payload_bytes: 0,
                partless_objects: 0,
                live_rows: 0,
                dead_rows: 0,
            };
            for obj in guard.objs.values() {
                let Some(payload) = obj.payload.as_ref() else {
                    continue;
                };
                stats.payload_objects += 1;
                stats.payload_bytes +=
                    u64::try_from(serde_json::to_string(payload).wrap_err(ERROR_JSON)?.len())
                        .expect(ERROR_IMPOSSIBLE);
                if obj.parts.is_empty() {
                    stats.partless_objects += 1;
                }
            }
            for part in guard.parts.values() {
                for member in part.members.values() {
                    if member.removed_at.is_some() {
                        stats.dead_rows += 1;
                    } else {
                        stats.live_rows += 1;
                    }
                }
            }
            Ok(stats)
        })
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerKey, part_id: PartKey) -> Res<CursorIndex> {
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
        peer_id: PeerKey,
        part_id: PartKey,
        cursor: CursorIndex,
    ) -> Res<()> {
        tracing::debug!(peer_id = %peer_id, part_id = %part_id, cursor, "memory store set peer part cursor");
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let prev = guard
                .peer_part_cursors
                .get(&(peer_id.clone(), part_id.clone()))
                .copied()
                .unwrap_or_default();
            let cursor = prev.max(cursor);
            guard.peer_part_cursors.insert((peer_id, part_id), cursor);
            Ok(())
        })
    }

    async fn list_events(
        &self,
        parts: HashSet<PartKey>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>> {
        let part_count = parts.len();
        let result = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let mut out: HashMap<PartKey, PartPage> = default();
            for part_id in parts {
                let Some(part) = guard.parts.get(&part_id) else {
                    return Err(ListPartsError::UnkownParts {
                        unkown_parts: vec![part_id],
                    });
                };
                let mut candidates = part
                    .members
                    .iter()
                    .filter_map(|(obj_id, member)| {
                        let (event_cursor, event) = if let Some(removed_cursor) = member.removed_at
                        {
                            // `added_at <= cursor < removed_at`: a removal for a member this
                            // reader never saw as present carries no information for it, and
                            // excluding it here keeps it from consuming the page's limit.
                            if member.added_at > cursor {
                                return None;
                            }
                            (
                                removed_cursor,
                                PartEvent::Removed(ObjRemovedFromPart {
                                    cursor: removed_cursor,
                                    part_id: part_id.clone(),
                                    obj_id: obj_id.clone(),
                                }),
                            )
                        } else if member.changed_at > member.added_at {
                            let payload = guard
                                .objs
                                .get(obj_id)
                                .and_then(|obj| obj.payload.clone())
                                .unwrap_or(serde_json::Value::Null);
                            (
                                member.changed_at,
                                PartEvent::Changed(ObjChanged {
                                    cursor: member.changed_at,
                                    part_ids: vec![part_id.clone()],
                                    obj_id: obj_id.clone(),
                                    payload,
                                }),
                            )
                        } else {
                            let payload = guard
                                .objs
                                .get(obj_id)
                                .and_then(|obj| obj.payload.clone())
                                .unwrap_or(serde_json::Value::Null);
                            (
                                member.added_at,
                                PartEvent::Changed(ObjChanged {
                                    cursor: member.added_at,
                                    part_ids: vec![part_id.clone()],
                                    obj_id: obj_id.clone(),
                                    payload,
                                }),
                            )
                        };
                        (event_cursor > cursor).then_some((event_cursor, event))
                    })
                    .collect::<Vec<_>>();
                candidates.sort_by_key(|(event_cursor, _)| *event_cursor);
                // A snapshot read knows how many candidates it found, so the verdict
                // is "the page did not fill": a full page may still have a successor,
                // and only a page that came up short is caught up. A zero-limit page
                // carries nothing, so it may only claim caught-up when nothing at all
                // is waiting.
                let drained = if limit == 0 {
                    candidates.is_empty()
                } else {
                    candidates.len() < limit as usize
                };
                candidates.truncate(limit as usize);
                // Always a position to ask from again: past the last event this page
                // carried, or the caller's own cursor when it carried none.
                let resume = candidates
                    .last()
                    .map_or(cursor, |(event_cursor, _)| *event_cursor);
                out.insert(
                    part_id.clone(),
                    PartPage {
                        events: candidates.into_iter().map(|(_, event)| event).collect(),
                        resume,
                        drained,
                    },
                );
            }
            Ok(out)
        });
        tracing::debug!(
            part_count,
            cursor,
            limit,
            page_count = result.as_ref().map(|res| res.len()).unwrap_or(0),
            "memory store list events"
        );
        Ok(result)
    }

    async fn open_revision_reader(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn super::LocalPartRevisionReader>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;

        let part_cursors = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    Some((part_id.clone(), reqs.lower_bound.max(*cursor)))
                }
                SubscriptionTarget::Object { .. } => None,
            })
            .collect::<HashMap<_, _>>();
        let parts = part_cursors.keys().cloned().collect::<HashSet<_>>();
        let objects = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id, .. } => Some(obj_id.clone()),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect::<HashSet<_>>();
        let unknown_parts = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            part_cursors
                .keys()
                .filter(|part_id| !guard.parts.contains_key(part_id))
                .cloned()
                .collect::<Vec<_>>()
        });
        if !unknown_parts.is_empty() {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: unknown_parts,
            }));
        }
        let selector = MemoryPartEventSelector {
            all: None,
            part_cursors,
            objects: objects.clone(),
            object_bounds: reqs
                .targets
                .iter()
                .filter_map(|target| match target {
                    // An object target carries its own position, the same way a
                    // part target does: a set request can name targets whose
                    // replays have reached different points, so the floor cannot
                    // stand in for a target's own cursor.
                    SubscriptionTarget::Object { obj_id, cursor } => {
                        Some((obj_id.clone(), reqs.lower_bound.max(*cursor)))
                    }
                    SubscriptionTarget::Part { .. } => None,
                })
                .collect(),
        };
        let source: Arc<dyn MemoryKeyedFrontierSource<PartFrontierKey, PartEvent>> =
            Arc::new(MemoryPartEventSource {
                state: Arc::clone(&self.inner),
            });
        let reader = crate::keyed_frontier::open_memory_keyed_frontier(source, selector).await?;
        Ok(Ok(Box::new(super::PartRevisionReader::new(
            reader, objects, parts,
        ))))
    }

    async fn open_revision_reader_all(
        &self,
        after: CursorIndex,
    ) -> Res<Result<Box<dyn super::LocalPartRevisionReader>, ListPartsError>> {
        let selector = MemoryPartEventSelector {
            all: Some(after),
            part_cursors: HashMap::new(),
            objects: HashSet::new(),
            object_bounds: HashMap::new(),
        };
        let source: Arc<dyn MemoryKeyedFrontierSource<PartFrontierKey, PartEvent>> =
            Arc::new(MemoryPartEventSource {
                state: Arc::clone(&self.inner),
            });
        let reader = crate::keyed_frontier::open_memory_keyed_frontier(source, selector).await?;
        Ok(Ok(Box::new(super::PartRevisionReader::new_all(reader))))
    }

    async fn ensure_part(&self, part_id: PartKey) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            guard.parts.entry(part_id).or_default();
            Ok(())
        })
    }

    async fn set_part_members(
        &self,
        part: PartKey,
        agents: HashMap<PeerKey, keyhive_core::access::Access>,
    ) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            // Stamped the same way the sqlite store does it: one cursor per rewrite,
            // taken even when the rewrite clears the part, so both stores consume the
            // cursor sequence identically.
            let changed_at = guard.global_cursor.next();
            if agents.is_empty() {
                guard.members.remove(&part);
            } else {
                guard.members.insert(
                    part,
                    agents
                        .into_iter()
                        .map(|(principal, access)| {
                            (principal, PartAccessState { access, changed_at })
                        })
                        .collect(),
                );
            }
        });
        Ok(())
    }

    async fn add_part_member(
        &self,
        part: PartKey,
        member: PeerKey,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let changed_at = guard.global_cursor.next();
            guard
                .members
                .entry(part)
                .or_default()
                .insert(member, PartAccessState { access, changed_at });
        });
        Ok(())
    }

    async fn remove_part_member(&self, part: PartKey, member: PeerKey) -> Res<()> {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            if let Some(member_map) = guard.members.get_mut(&part) {
                member_map.remove(&member);
                if member_map.is_empty() {
                    guard.members.remove(&part);
                }
            }
        });
        Ok(())
    }

    async fn permitted_parts(
        &self,
        scope: PartScope,
        obj_id: ObjKey,
        principal: Option<PeerKey>,
    ) -> Res<Option<Vec<PartKey>>> {
        let permitted = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            guard.permitted_parts(scope, obj_id, principal)
        });
        Ok(permitted)
    }
}

// impl MemoryPartStore {
//     pub async fn is_tombstoned(&self, obj_id: ObjKey) -> Res<bool> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.tombstoned_objs.contains_key(&obj_id))
//         })
//     }
//
//     pub async fn obj_sync_version(&self, obj_id: ObjKey) -> Res<u64> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.obj_sync_version_locked(obj_id))
//         })
//     }
//
//     pub(crate) async fn obj_sync_stamp(&self, obj_id: ObjKey) -> Res<ObjSyncStamp> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.obj_sync_stamp_locked(obj_id))
//         })
//     }
//
//     async fn get_peer_obj_payload(
//         &self,
//         peer_id: PeerKey,
//         obj_id: ObjKey,
//     ) -> Res<Option<Option<ObjPayload>>> {
//         surelock::key::lock_scope(|key| {
//             let (guard, _key) = key.lock(&self.inner);
//             Ok(guard.peer_obj_payloads.get(&(peer_id, obj_id)).cloned())
//         })
//     }
//
//     async fn set_peer_obj_payload(
//         &self,
//         peer_id: PeerKey,
//         obj_id: ObjKey,
//         payload: Option<ObjPayload>,
//     ) -> Res<()> {
//         surelock::key::lock_scope(|key| {
//             let (mut guard, _key) = key.lock(&self.inner);
//             guard.peer_obj_payloads.insert((peer_id, obj_id), payload);
//             Ok(())
//         })
//     }
//
//     #[tracing::instrument(
//         skip(self, payload, parts),
//         fields(obj_id = %obj_id, part_count = parts.len())
//     )]
//     pub(crate) async fn sync_upsert_obj(
//         &self,
//         obj_id: ObjKey,
//         payload: ObjPayload,
//         parts: Vec<PartKey>,
//         expected_stamp: ObjSyncStamp,
//         sync_version: u64,
//         sync_stamp: ObjSyncStamp,
//     ) -> Res<SyncMutationOutcome> {
//         surelock::key::lock_scope(|key| {
//             let (mut guard, _key) = key.lock(&self.inner);
//             if guard.obj_sync_stamp_locked(obj_id) != expected_stamp {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_count = parts.len(),
//                     expected_stamp = ?expected_stamp,
//                     "sync upsert stale"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             }
//             guard.upsert_obj_locked(
//                 self.owner_peer_id,
//                 obj_id,
//                 payload,
//                 parts,
//                 Some(sync_version),
//                 Some(sync_stamp),
//             );
//             tracing::trace!(
//                 obj_id = %obj_id,
//                 part_count = guard
//                     .objs
//                     .get(&obj_id)
//                     .map(|obj| obj.parts.len())
//                     .unwrap_or_default(),
//                 expected_stamp = ?expected_stamp,
//                 "sync upsert applied"
//             );
//             Ok(SyncMutationOutcome::Applied)
//         })
//     }
//
//     #[tracing::instrument(
//     skip(self),
//     fields(obj_id = %obj_id, part_id = %part_id)
// )]
//     pub(crate) async fn sync_remove_obj_from_part(
//         &self,
//         obj_id: ObjKey,
//         part_id: PartKey,
//         expected_stamp: ObjSyncStamp,
//         sync_version: u64,
//         sync_stamp: ObjSyncStamp,
//     ) -> Res<SyncMutationOutcome> {
//         surelock::key::lock_scope(|key| {
//             let (mut guard, _key) = key.lock(&self.inner);
//             if guard.obj_sync_stamp_locked(obj_id) != expected_stamp {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             }
//             let Some(part) = guard.parts.get(&part_id) else {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale missing part"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             };
//             let Some(state) = part.members.get(&obj_id) else {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale missing member"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             };
//             if state.removed_at.is_some() {
//                 tracing::trace!(
//                     obj_id = %obj_id,
//                     part_id = %part_id,
//                     expected_stamp = ?expected_stamp,
//                     "sync remove stale already removed"
//                 );
//                 return Ok(SyncMutationOutcome::Stale);
//             }
//             guard.remove_obj_from_part_locked(
//                 self.owner_peer_id,
//                 obj_id,
//                 part_id,
//                 Some(sync_version),
//                 Some(sync_stamp),
//             );
//             tracing::trace!(
//                 obj_id = %obj_id,
//                 part_id = %part_id,
//                 expected_stamp = ?expected_stamp,
//                 "sync remove applied"
//             );
//             Ok(SyncMutationOutcome::Applied)
//         })
//     }
//
//     // #[tracing::instrument(
//     //     skip(self),
//     //     fields(obj_id = %obj_id)
//     // )]
//     // async fn sync_tombstone_obj(
//     //     &self,
//     //     obj_id: ObjKey,
//     //     expected_stamp: ObjSyncStamp,
//     //     sync_version: u64,
//     //     sync_stamp: ObjSyncStamp,
//     // ) -> Res<SyncMutationOutcome> {
//     //     surelock::key::lock_scope(|key| {
//     //         let (mut guard, _key) = key.lock(&self.inner);
//     //         if guard.obj_sync_stamp_locked(obj_id) != expected_stamp {
//     //             tracing::trace!(
//     //                 obj_id = %obj_id,
//     //                 expected_stamp = ?expected_stamp,
//     //                 "sync tombstone stale"
//     //             );
//     //             return Ok(SyncMutationOutcome::Stale);
//     //         }
//     //         let Some(parts) = guard
//     //             .objs
//     //             .get(&obj_id)
//     //             .map(|obj| obj.parts.iter().copied().collect::<Vec<_>>())
//     //         else {
//     //             if guard.tombstoned_objs.contains_key(&obj_id) {
//     //                 return Ok(SyncMutationOutcome::Applied);
//     //             }
//     //             return Ok(SyncMutationOutcome::Stale);
//     //         };
//     //         for part_id in parts {
//     //             guard.remove_obj_from_part_locked(
//     //                 self.owner_peer_id,
//     //                 obj_id,
//     //                 part_id,
//     //                 Some(sync_version),
//     //                 Some(sync_stamp.clone()),
//     //             );
//     //         }
//     //         Ok(SyncMutationOutcome::Applied)
//     //     })
//     // }
// }

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryPartStoreSnapshot {
    pub objs: BTreeMap<ObjKey, MemoryObjSnapshot>,
    pub peer_part_cursors: BTreeMap<(PeerKey, PartKey), CursorIndex>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemoryObjSnapshot {
    pub payload: Option<ObjPayload>,
    pub parts: BTreeSet<PartKey>,
}

#[cfg(test)]
impl MemoryPartStore {
    pub(crate) async fn snapshot(&self) -> Res<MemoryPartStoreSnapshot> {
        surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            let objs = guard
                .objs
                .iter()
                .map(|(obj_id, obj)| {
                    (
                        obj_id.clone(),
                        MemoryObjSnapshot {
                            payload: obj.payload.clone(),
                            parts: obj.parts.iter().cloned().collect(),
                        },
                    )
                })
                .collect();
            Ok(MemoryPartStoreSnapshot {
                objs,
                peer_part_cursors: guard.peer_part_cursors.clone().into_iter().collect(),
            })
        })
    }
}

#[cfg(test)]
impl From<MemoryPartStoreSnapshot> for ObservedStoreSnapshot {
    fn from(value: MemoryPartStoreSnapshot) -> Self {
        Self {
            objs: value
                .objs
                .into_iter()
                // Only live membership is observable: an object with no part is not remotely
                // deliverable, so a payload retained past its last part is policy state rather
                // than part of the store's observable contents. The sqlite store reads
                // membership for the same reason.
                .filter(|(_, snapshot)| !snapshot.parts.is_empty())
                .map(|(obj_id, snapshot)| {
                    (
                        obj_id,
                        ObservedObjSnapshot {
                            payload: snapshot.payload,
                            parts: snapshot.parts,
                        },
                    )
                })
                .collect(),
            peer_part_cursors: value.peer_part_cursors,
        }
    }
}

#[cfg(test)]
#[async_trait]
impl ObservedStore for MemoryPartStore {
    async fn observed_snapshot(&self) -> Res<ObservedStoreSnapshot> {
        Ok(self.snapshot().await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part_store::host_contract::{self, HostPartStoreContractHarness, PageEventStore};
    use big_sync_core::ByteKey;
    use std::{collections::HashSet, time::Duration};

    struct MemoryHostHarness {
        store: MemoryPartStore,
    }

    #[async_trait]
    impl HostPartStoreContractHarness for MemoryHostHarness {
        fn store(&self) -> &dyn HostPartStore {
            &self.store
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn memory_host_part_store_contract() -> Res<()> {
        let harness = MemoryHostHarness {
            store: MemoryPartStore::new(),
        };
        host_contract::assert_host_part_store_contract(&harness).await
    }

    /// The leaf page's byte budget, which the store applies beside the entry hint.
    #[tokio::test(flavor = "multi_thread")]
    async fn memory_leaf_page_byte_budget_contract() -> Res<()> {
        host_contract::assert_leaf_page_byte_budget_contract(&MemoryPartStore::new()).await
    }

    /// What the store shows a peer, which a part-less payload is not part of.
    #[tokio::test(flavor = "multi_thread")]
    async fn memory_observed_snapshot_excludes_partless_payload_contract() -> Res<()> {
        host_contract::assert_observed_snapshot_excludes_partless_payload(&MemoryPartStore::new())
            .await
    }

    /// A hidden part stays physically present but is invisible to remote part access,
    /// and that holds at the bucket endpoints too, not only in `summarize_parts` and
    /// this store's own subscription filter.
    #[tokio::test(flavor = "multi_thread")]
    async fn hidden_parts_are_invisible_to_bucket_walks() -> Res<()> {
        use big_sync_core::FingerprintSeed;
        use big_sync_core::rpc::{GetChangedBucketsRequest, LeafBucketRequest, LeafBucketsRequest};

        let hidden = PartKey(ByteKey::new([60u8; 32]));
        let visible = PartKey(ByteKey::new([61u8; 32]));
        let member = ObjKey(ByteKey::new([62u8; 32]));
        let store = MemoryPartStore::with_config(crate::part_store::HostPartStoreConfig {
            hidden_parts: HashSet::from([hidden.clone()]),
            ..Default::default()
        });
        // Both parts are granted to the subscriber, including the hidden one: the
        // refusal below has to be the hidden gate, not the access gate.
        let subscriber = crate::part_store::contract::grant_bucket_read(
            &store,
            [hidden.clone(), visible.clone()],
        )
        .await?;

        for part in [hidden.clone(), visible.clone()] {
            store.ensure_part(part.clone()).await?;
            store
                .set_obj_payload(member.clone(), serde_json::json!({ "tag": "member" }))
                .await?;
            store.add_obj_to_parts(member.clone(), vec![part]).await?;
        }

        // The control: the same walk answers for a part that is not hidden, so a
        // failure below is the gate and not an empty store.
        let buckets = store
            .get_changed_buckets(
                GetChangedBucketsRequest {
                    part_id: visible.clone(),
                    offset: BuckId::ROOT,
                    to_level: BuckId::MAX_LEVEL,
                    since: 0,
                    limit_hint: 16,
                },
                subscriber.clone(),
            )
            .await?
            .expect("a visible part answers a bucket walk");
        assert!(!buckets.is_empty(), "the control walk must see the part");

        match store
            .get_changed_buckets(
                GetChangedBucketsRequest {
                    part_id: hidden.clone(),
                    offset: BuckId::ROOT,
                    to_level: BuckId::MAX_LEVEL,
                    since: 0,
                    limit_hint: 16,
                },
                subscriber.clone(),
            )
            .await?
        {
            Err(ListPartsError::UnkownParts { unkown_parts }) => {
                assert_eq!(unkown_parts, vec![hidden.clone()]);
            }
            other => panic!("a hidden part must read as unknown, got {other:?}"),
        }

        match store
            .leaf_buckets(
                LeafBucketsRequest {
                    part_id: hidden.clone(),
                    since: 0,
                    buckets: vec![LeafBucketRequest {
                        buck_id: BuckId::ROOT,
                        after: None,
                    }],
                    seed: FingerprintSeed::new(0x5555_6666, 0x7777_8888),
                    limit_hint: 16,
                },
                subscriber,
            )
            .await?
        {
            Err(LeafBucketsError::UnkownPart) => {}
            other => panic!("a hidden part must read as unknown, got {other:?}"),
        }
        Ok(())
    }

    /// `hidden_parts` is applied to the page path as well as the bucket walk: a page for
    /// a hidden part reads as unknown, which is not an empty page and not a denial.
    ///
    /// The gate belongs to the peer-facing read — `replay_page` resolves the part
    /// through `summarize_parts` — so the trusted local reader below, which is the
    /// in-process path, still sees the part.
    #[tokio::test(flavor = "multi_thread")]
    async fn hidden_parts_are_invisible_to_the_page_path() -> Res<()> {
        use big_sync_core::rpc::{ReplayPageOutcome, SubscriptionTarget};

        let hidden = PartKey(ByteKey::new([70u8; 32]));
        let visible = PartKey(ByteKey::new([71u8; 32]));
        let member = ObjKey(ByteKey::new([72u8; 32]));
        let store = MemoryPartStore::with_config(crate::part_store::HostPartStoreConfig {
            hidden_parts: HashSet::from([hidden.clone()]),
            ..Default::default()
        });
        // Granted, the hidden part included: the refusal below has to be the hidden
        // gate and not the access gate.
        let subscriber = crate::part_store::contract::grant_bucket_read(
            &store,
            [hidden.clone(), visible.clone()],
        )
        .await?;
        for part in [hidden.clone(), visible.clone()] {
            store.ensure_part(part.clone()).await?;
            store
                .set_obj_payload(member.clone(), serde_json::json!({ "tag": "member" }))
                .await?;
            store.add_obj_to_parts(member.clone(), vec![part]).await?;
        }

        let target = |part_id: PartKey| SubscriptionTarget::Part { part_id, cursor: 0 };
        // The control: the same page reaches a part that is not hidden.
        let control = store
            .replay_page(
                target(visible),
                8,
                subscriber.clone(),
                Duration::from_millis(0),
            )
            .await?;
        assert!(
            !matches!(control, ReplayPageOutcome::UnknownPart),
            "the control page must reach a visible part, got {control:?}"
        );
        assert_eq!(
            store
                .replay_page(
                    target(hidden.clone()),
                    8,
                    subscriber,
                    Duration::from_millis(0),
                )
                .await?,
            ReplayPageOutcome::UnknownPart,
            "a hidden part must read as unknown on the page path"
        );

        // The in-process path is not gated: this is a peer-facing rule.
        let local = store
            .page_events_local(SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([target(hidden)]),
            })
            .await??;
        let mut saw_member = false;
        while !saw_member {
            let evt = tokio::time::timeout(Duration::from_secs(5), local.next())
                .await
                .expect("the local reader answers within the timeout")?;
            match evt {
                SubEvent::Changed(changed) if changed.obj_id == member => saw_member = true,
                SubEvent::ReplayComplete => break,
                _ => {}
            }
        }
        assert!(
            saw_member,
            "a trusted local reader still sees the hidden part"
        );
        Ok(())
    }

    /// This store filters subscriptions per recipient, so a page for a part the subscriber
    /// cannot read is denied rather than answered empty: an empty page is indistinguishable
    /// from being caught up, and the caller must not have to infer the difference.
    #[tokio::test(flavor = "multi_thread")]
    async fn memory_page_denied_for_unreadable_part() -> Res<()> {
        use big_sync_core::rpc::{ReplayPageOutcome, SubscriptionTarget};

        let store = MemoryPartStore::new();
        let part = PartKey(ByteKey::new([67u8; 32]));
        let obj_id = ObjKey(ByteKey::new([68u8; 32]));
        let member = PeerKey::new([69u8; 32]);
        let outsider = PeerKey::new([70u8; 32]);
        store.ensure_part(part.clone()).await?;
        store
            .set_obj_payload(obj_id.clone(), serde_json::json!({"value": 1}))
            .await?;
        store.add_obj_to_parts(obj_id, vec![part.clone()]).await?;
        store
            .set_part_members(
                part.clone(),
                HashMap::from([(member.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;

        let readable = store
            .replay_page(
                SubscriptionTarget::Part {
                    part_id: part.clone(),
                    cursor: 0,
                },
                8,
                member,
                Duration::from_millis(0),
            )
            .await?;
        assert!(
            matches!(readable, ReplayPageOutcome::Events(_)),
            "a granted member reads a page, got {readable:?}"
        );

        let denied = store
            .replay_page(
                SubscriptionTarget::Part {
                    part_id: part,
                    cursor: 0,
                },
                8,
                outsider,
                Duration::from_millis(0),
            )
            .await?;
        assert_eq!(
            denied,
            ReplayPageOutcome::Unauthorized,
            "a subscriber with no access row is denied, not reported as caught up"
        );
        Ok(())
    }

    /// `latest_revision` reports the newest allocated revision; it does not allocate one of
    /// its own, which the sqlite store's pure `SELECT` also states.
    #[tokio::test(flavor = "multi_thread")]
    async fn memory_latest_revision_is_idempotent() -> Res<()> {
        let store = MemoryPartStore::new();
        let first = store.latest_revision().await?;
        let second = store.latest_revision().await?;
        assert_eq!(
            first, second,
            "reading the latest revision must not allocate a revision"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn subscription_handoff_does_not_lose_immediate_mutation() -> Res<()> {
        let store = MemoryPartStore::new();
        let part = PartKey(ByteKey::new([61u8; 32]));
        let first = ObjKey(ByteKey::new([62u8; 32]));
        let second = ObjKey(ByteKey::new([63u8; 32]));
        let peer = PeerKey::new([64u8; 32]);

        store.ensure_part(part.clone()).await?;
        store
            .set_part_members(
                part.clone(),
                HashMap::from([(peer.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;
        for (obj, value) in [(first.clone(), "first"), (second.clone(), "second")] {
            store.set_obj_payload(obj, serde_json::json!(value)).await?;
        }
        store
            .add_obj_to_parts(first.clone(), vec![part.clone()])
            .await?;

        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: 0,
                    }]),
                },
                peer,
            )
            .await??;

        // This mutation is deliberately issued immediately after subscribe:
        // it may be observed by replay or by the pending-to-live handoff, but
        // it must not be lost in either case.
        store.add_obj_to_parts(second.clone(), vec![part]).await?;

        let mut seen = HashSet::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.next()).await??;
            match event {
                SubEvent::Changed(event) => {
                    seen.insert(event.obj_id);
                }
                SubEvent::ReplayComplete => break,
                SubEvent::Removed(_) => {}
            }
        }
        if !seen.contains(&second) {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.next()).await??;
            assert!(
                matches!(&event, SubEvent::Changed(event) if event.obj_id == second),
                "immediate mutation was not delivered after replay: {event:?}"
            );
        }
        assert!(seen.contains(&first), "replay lost the existing object");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn syncability_filter_drops_non_readable_events() -> Res<()> {
        let store = MemoryPartStore::new();
        let part = PartKey(ByteKey::new([1u8; 32]));
        let obj = ObjKey(ByteKey::new([2u8; 32]));
        let reader = PeerKey::new([3u8; 32]);
        let non_reader = PeerKey::new([4u8; 32]);

        store.ensure_part(part.clone()).await?;
        store
            .set_obj_payload(obj.clone(), serde_json::json!("content"))
            .await?;

        // Set doc members: only `reader` has Read access.
        let mut agents = HashMap::new();
        agents.insert(reader.clone(), keyhive_core::access::Access::Read);
        store.set_part_members(part.clone(), agents.clone()).await?;

        // Subscribe as reader — should receive the Added event.
        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: 0,
                    }]),
                },
                reader,
            )
            .await??;
        store.add_obj_to_parts(obj, vec![part.clone()]).await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match rx.next().await {
                    Ok(SubEvent::Changed(_)) => return Ok::<_, eyre::Report>(()),
                    Ok(SubEvent::ReplayComplete) => continue,
                    Ok(_) => continue,
                    Err(_) => return Err(ferr!("stream closed")),
                }
            }
        })
        .await??;

        // Subscribe as non-reader — should NOT receive the Added event.
        let rx2 = store
            .page_events(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: 0,
                    }]),
                },
                non_reader,
            )
            .await??;
        tokio::time::timeout(Duration::from_secs(2), async {
            match rx2.next().await {
                Ok(SubEvent::ReplayComplete) => Ok::<_, eyre::Report>(()),
                Ok(event) => Err(ferr!("denied replay leaked event: {event:?}")),
                Err(_) => Err(ferr!("denied subscriber closed during replay")),
            }
        })
        .await??;
        let second_obj = ObjKey(ByteKey::new([5u8; 32]));
        store.set_part_members(part.clone(), agents).await?;
        store
            .set_obj_payload(second_obj.clone(), serde_json::json!("content2"))
            .await?;
        store.add_obj_to_parts(second_obj, vec![part]).await?;
        match tokio::time::timeout(Duration::from_millis(200), rx2.next()).await {
            Err(_) => {}
            Ok(Ok(event)) => return Err(ferr!("denied live event leaked: {event:?}")),
            Ok(Err(_)) => return Err(ferr!("denied subscriber closed unexpectedly")),
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn syncability_filter_updates() -> Res<()> {
        let store = MemoryPartStore::new();
        let part = PartKey(ByteKey::new([10u8; 32]));
        let obj = ObjKey(ByteKey::new([20u8; 32]));
        let peer = PeerKey::new([30u8; 32]);

        store.ensure_part(part.clone()).await?;
        store
            .set_obj_payload(obj.clone(), serde_json::json!("initial"))
            .await?;

        // Initially peer has Read access.
        let mut agents = HashMap::new();
        agents.insert(peer.clone(), keyhive_core::access::Access::Read);
        store.set_part_members(part.clone(), agents).await?;

        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part.clone(),
                        cursor: 0,
                    }]),
                },
                peer,
            )
            .await??;
        store
            .add_obj_to_parts(obj.clone(), vec![part.clone()])
            .await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut saw_touch = false;
            let mut saw_replay_complete = false;
            loop {
                match rx.next().await {
                    Ok(SubEvent::Changed(_)) => saw_touch = true,
                    Ok(SubEvent::ReplayComplete) => saw_replay_complete = true,
                    Ok(event) => return Err(ferr!("unexpected authorized event: {event:?}")),
                    Err(_) => return Err(ferr!("authorized subscriber closed")),
                }
                if saw_touch && saw_replay_complete {
                    return Ok::<_, eyre::Report>(());
                }
            }
        })
        .await??;
        store
            .add_obj_to_parts(obj.clone(), vec![part.clone()])
            .await?;
        // Should receive Added event.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match rx.next().await {
                    Ok(SubEvent::Changed(_)) => return,
                    Ok(SubEvent::ReplayComplete) => continue,
                    Ok(_) => continue,
                    Err(_) => return,
                }
            }
        })
        .await
        .ok();

        // Now revoke access: set empty members.
        store.set_part_members(part, HashMap::new()).await?;
        store
            .set_obj_payload(obj, serde_json::json!("updated"))
            .await?;
        match tokio::time::timeout(Duration::from_millis(200), rx.next()).await {
            Err(_) => {}
            Ok(Ok(event)) => return Err(ferr!("revoked subscriber received event: {event:?}")),
            Ok(Err(_)) => return Err(ferr!("revoked subscriber closed unexpectedly")),
        }

        Ok(())
    }

    /// The primitive the dirty count rests on: local rows newer than a cursor are
    /// counted, with member and access changes reported separately so a caller can
    /// tell the two relevance sources apart. This semantics holds regardless of which
    /// side of a sync evaluates it against a cursor from its own stream.
    #[tokio::test(flavor = "multi_thread")]
    async fn part_dirty_count_separates_member_and_access_changes() -> Res<()> {
        let store = MemoryPartStore::new();
        let part = PartKey(ByteKey::new([40u8; 32]));
        let other_part = PartKey(ByteKey::new([41u8; 32]));
        let peer = PeerKey::new([42u8; 32]);
        let other_peer = PeerKey::new([43u8; 32]);
        let obj_a = ObjKey(ByteKey::new([44u8; 32]));
        let obj_b = ObjKey(ByteKey::new([45u8; 32]));
        let obj_c = ObjKey(ByteKey::new([46u8; 32]));

        store.ensure_part(part.clone()).await?;
        store.ensure_part(other_part.clone()).await?;
        assert_eq!(
            store
                .part_dirty_count(part.clone(), Some(peer.clone()), 0)
                .await?,
            PartDirtyCount::default(),
            "a part with neither members nor grants has no relevance to count"
        );

        // A member write moves the member number only.
        store
            .set_obj_payload(obj_a.clone(), serde_json::json!({"a": 1}))
            .await?;
        store.add_obj_to_parts(obj_a, vec![part.clone()]).await?;
        assert_eq!(
            store
                .part_dirty_count(part.clone(), Some(peer.clone()), 0)
                .await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 0,
            }
        );

        // A grant on this part for this peer adds the access number alongside it.
        store
            .set_part_members(
                part.clone(),
                HashMap::from([(peer.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;
        assert_eq!(
            store
                .part_dirty_count(part.clone(), Some(peer.clone()), 0)
                .await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 1,
            }
        );

        // The local principal is never gated by access rows, so it has no access half;
        // the member half does not depend on who is asking and still counts.
        assert_eq!(
            store.part_dirty_count(part.clone(), None, 0).await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 0,
            }
        );

        // A second member write moves only the member number.
        store
            .set_obj_payload(obj_b.clone(), serde_json::json!({"b": 1}))
            .await?;
        store.add_obj_to_parts(obj_b, vec![part.clone()]).await?;
        assert_eq!(
            store
                .part_dirty_count(part.clone(), Some(peer.clone()), 0)
                .await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            }
        );

        // Another principal's grant on this part is not this principal's relevance.
        store
            .add_part_member(part.clone(), other_peer, keyhive_core::access::Access::Read)
            .await?;
        assert_eq!(
            store
                .part_dirty_count(part.clone(), Some(peer.clone()), 0)
                .await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            },
            "another principal's grant must not be counted for this one"
        );

        // Another part's member write and grant are not this part's relevance.
        store
            .set_obj_payload(obj_c.clone(), serde_json::json!({"c": 1}))
            .await?;
        store
            .add_obj_to_parts(obj_c, vec![other_part.clone()])
            .await?;
        store
            .set_part_members(
                other_part,
                HashMap::from([(peer.clone(), keyhive_core::access::Access::Read)]),
            )
            .await?;
        assert_eq!(
            store
                .part_dirty_count(part.clone(), Some(peer.clone()), 0)
                .await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            },
            "another part's rows must not be counted here"
        );

        // The comparison is strictly newer-than, so nothing clears the store's own
        // current revision.
        let ceiling = store.latest_revision().await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), ceiling).await?,
            PartDirtyCount::default(),
            "no row can be newer than the store's current revision"
        );
        Ok(())
    }
}
