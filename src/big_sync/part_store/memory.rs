use crate::interlude::*;

use big_sync_core::keyed_frontier::{
    FrontierMutation, FrontierRead, FrontierReadLimits, FrontierRevision, KeyedFrontierReader,
    KeyedFrontierResult,
};
use big_sync_core::part_store::{CursorIndex, ObjPayload, PartDirtyCount};
use big_sync_core::rpc::{
    BucketMemberKind, BucketObjPageEntry, BucketSummary, BucketSummaryState,
    GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult, LeafBucketsError,
    LeafBucketsRequest, ListPartsError, ObjAddedToPart, ObjChanged, ObjRemovedFromPart, PartEvent,
    PartPage, PartSummary, SubEvent, SubPartsRequest, SubscriptionTarget,
};
use big_sync_core::{BuckId, Fingerprint, ObjKey, PartKey, PeerKey, mpsc};

use super::PartFrontierKey;
use super::{HostPartStore, PartScope, obj_id_bounds_for_bucket};
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
                            }
                        >,
                        bucket_stats: BTreeMap<BuckId, BucketSummaryState>,
                    }
                >,
                event_frontier: MemoryKeyedFrontierTable<PartFrontierKey, PartEvent>,
                bus: struct MemorySubsBus {
                    #![derive(Default)]
                    buf: Vec<PartEvent>
                },
                objs: HashMap<
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

impl MemoryPartStore {
    /// See the sqlite store's counterpart: derive and materialize the single-object part for
    /// each object a remote subscriber asked for, so that an explicit share on
    /// `o:{object_key}` has a membership row to be reached through.
    ///
    /// Quiet: no revision is consumed and no event is emitted, because subscribing is not a sync
    /// event. `peek` is the documented call for this: it exists for callers that only want to
    /// observe the cursor, or to stamp state that allocates no revision of its own — which is
    /// exactly a silent materialization — while `next` is reserved for write paths.
    ///
    /// Whether this store should instead emit an event for the materialized row, as the sqlite
    /// store does by recording it as a keyed-frontier `Changed`, is the undecided
    /// materialization-parity question. It is left open deliberately: an earlier attempt to
    /// consume a revision here was justified by an A/B against a test that was flaky for reasons
    /// unrelated to this code, so that measurement supported nothing.
    fn materialize_object_parts(&self, obj_ids: Vec<ObjKey>) {
        surelock::key::lock_scope(|key| {
            let (mut guard, _key) = key.lock(&self.inner);
            let guard = &mut *guard;
            let cursor = guard.global_cursor.peek();
            for obj_id in obj_ids {
                let part_id = obj_id.object_part_key();
                let payload = {
                    let obj_state = guard.objs.entry(obj_id).or_default();
                    obj_state.parts.insert(part_id);
                    obj_state.payload.clone()
                };
                let Some(payload) = payload else {
                    continue;
                };
                let part = guard.parts.entry(part_id).or_default();
                let old_state = part.members.get(&obj_id).cloned();
                let new_state = PartMemberState {
                    added_at: cursor,
                    changed_at: cursor,
                    removed_at: None,
                };
                match old_state {
                    Some(old) if old.removed_at.is_none() => continue,
                    Some(_) => part.apply_bucket_transition(
                        obj_id,
                        cursor,
                        BucketMemberKind::Dead,
                        BucketMemberKind::Live(&payload),
                    ),
                    None => part.apply_bucket_transition(
                        obj_id,
                        cursor,
                        BucketMemberKind::Absent,
                        BucketMemberKind::Live(&payload),
                    ),
                }
                part.members.insert(obj_id, new_state);
                part.latest_cursor = cursor;
            }
        });
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

impl PartState {
    fn apply_bucket_transition(
        &mut self,
        obj_id: ObjKey,
        cursor: CursorIndex,
        old: BucketMemberKind<'_>,
        new: BucketMemberKind<'_>,
    ) {
        for level in 0..=BuckId::MAX_LEVEL {
            let buck_id = BuckId::from_obj_key(level, &obj_id);
            let agg = self.bucket_stats.entry(buck_id).or_default();
            agg.apply_transition(buck_id, obj_id, cursor, old, new);
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
        PartEvent::Changed(inner) => inner.obj_id,
        PartEvent::Added(inner) => inner.obj_id,
        PartEvent::Removed(inner) => inner.obj_id,
    };
    // Candidate parts: what the event names, else the object's membership.
    let scope = match &event {
        PartEvent::Changed(inner) if !inner.part_ids.is_empty() => {
            PartScope::AnyOf(inner.part_ids.clone())
        }
        PartEvent::Changed(_) => PartScope::FromObject,
        PartEvent::Added(inner) => PartScope::Part(inner.part_id),
        PartEvent::Removed(inner) => PartScope::Part(inner.part_id),
    };
    // A concrete subscriber always filters.
    let Some(readable) = state.permitted_parts(scope, obj_id, Some(subscriber)) else {
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
        PartEvent::Added(inner) if !selected.is_empty() => Some(SubEvent::Added(inner)),
        PartEvent::Added(inner) if object_selected => {
            Some(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor: inner.cursor,
                part_ids: Vec::new(),
                obj_id: inner.obj_id,
                payload: inner.payload,
            }))
        }
        PartEvent::Removed(inner) if !selected.is_empty() => Some(SubEvent::Removed(inner)),
        PartEvent::Removed(inner) if object_selected => {
            Some(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor: inner.cursor,
                part_ids: Vec::new(),
                obj_id: inner.obj_id,
                payload: serde_json::Value::Null,
            }))
        }
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
                    .map(|deets| deets.parts.iter().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                resolved.sort_unstable();
                resolved
            }
        };
        Some(
            candidates
                .into_iter()
                .filter(|part_id| part_permits(&self.members, *part_id, principal))
                .collect(),
        )
    }

    fn flush(&mut self) {
        let pending_events = std::mem::take(&mut self.bus.buf);
        if pending_events.is_empty() {
            return;
        }
        let revision = pending_events
            .iter()
            .map(|event| match event {
                PartEvent::Changed(inner) => inner.cursor,
                PartEvent::Added(inner) => inner.cursor,
                PartEvent::Removed(inner) => inner.cursor,
            })
            .max()
            .expect(ERROR_IMPOSSIBLE);
        assert!(pending_events.iter().all(|event| match event {
            PartEvent::Changed(inner) => inner.cursor == revision,
            PartEvent::Added(inner) => inner.cursor == revision,
            PartEvent::Removed(inner) => inner.cursor == revision,
        }));
        let mut frontier_mutations = Vec::new();
        for evt in pending_events {
            let (evt_parts, evt_obj_id) = match &evt {
                PartEvent::Changed(inner) => (inner.part_ids.clone(), inner.obj_id),
                PartEvent::Added(inner) => (vec![inner.part_id], inner.obj_id),
                PartEvent::Removed(inner) => (vec![inner.part_id], inner.obj_id),
            };
            if evt_parts.is_empty() {
                frontier_mutations.push(FrontierMutation::Put {
                    key: PartFrontierKey::Object(evt_obj_id),
                    value: evt.clone(),
                });
            } else {
                for part_id in &evt_parts {
                    let key = PartFrontierKey::Part {
                        obj_id: evt_obj_id,
                        part_id: *part_id,
                    };
                    match &evt {
                        PartEvent::Removed(_) => {
                            frontier_mutations.push(FrontierMutation::Delete { key });
                        }
                        PartEvent::Changed(inner) => {
                            let mut inner = inner.clone();
                            inner.part_ids = vec![*part_id];
                            frontier_mutations.push(FrontierMutation::Put {
                                key,
                                value: PartEvent::Changed(inner),
                            });
                        }
                        PartEvent::Added(_) => {
                            frontier_mutations.push(FrontierMutation::Put {
                                key,
                                value: evt.clone(),
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
            bucket_count = result.as_ref().map(|buck| buck.len()).unwrap_or(0),
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
                if guard.bucket_summary(req.part_id, buck_id).changed_at <= req.since {
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
                                .unwrap_or(serde_json::Value::Null);
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
                .map(|deets| deets.parts.iter().copied().collect())
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
            let obj_state = guard.objs.entry(obj_id).or_default();
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
                for &part_id in &desired_parts {
                    let part = guard.parts.get_mut(&part_id).expect(ERROR_IMPOSSIBLE);
                    part.apply_bucket_transition(
                        obj_id,
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
                    let part = guard.parts.entry(part_id).or_default();
                    part.apply_bucket_transition(
                        obj_id,
                        cursor,
                        BucketMemberKind::Absent,
                        BucketMemberKind::Live(&payload),
                    );
                    part.members.insert(
                        obj_id,
                        PartMemberState {
                            added_at: cursor,
                            changed_at: cursor,
                            removed_at: None,
                        },
                    );
                    part.latest_cursor = cursor;
                    guard
                        .bus
                        .queue_evt(PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                            cursor,
                            part_id,
                            obj_id,
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
            let obj_state = guard.objs.entry(obj_id).or_default();

            guard.tombstoned_objs.remove(&obj_id);
            let Some(payload) = obj_state.payload.clone() else {
                obj_state.parts.extend(parts);
                return Ok(());
            };
            obj_state.parts.extend(&parts);
            let cursor = guard.global_cursor.next();
            for &part_id in &parts {
                let part = guard.parts.entry(part_id).or_default();
                let old_state = part.members.get(&obj_id).cloned();
                match old_state {
                    Some(state) if state.removed_at.is_none() => continue,
                    Some(_state) => {
                        part.apply_bucket_transition(
                            obj_id,
                            cursor,
                            BucketMemberKind::Dead,
                            BucketMemberKind::Live(&payload),
                        );
                        if let Some(old) = part.members.get_mut(&obj_id) {
                            old.changed_at = cursor;
                            old.removed_at = None;
                        }
                    }
                    None => {
                        part.apply_bucket_transition(
                            obj_id,
                            cursor,
                            BucketMemberKind::Absent,
                            BucketMemberKind::Live(&payload),
                        );
                        part.members.insert(
                            obj_id,
                            PartMemberState {
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
                    .queue_evt(PartEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                        cursor,
                        part_id,
                        obj_id,
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

            let part = guard.parts.entry(part_id).or_default();
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
                obj_id,
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
                    obj_id,
                }));
            if obj_state.parts.is_empty() {
                let cursor = part.latest_cursor;
                guard.tombstoned_objs.insert(obj_id, cursor);
                guard.objs.remove(&obj_id);
            }

            guard.flush();

            Ok(())
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
                .get(&(peer_id, part_id))
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
                    .filter_map(|(&obj_id, member)| {
                        let (event_cursor, event) = if let Some(removed_cursor) = member.removed_at
                        {
                            (
                                removed_cursor,
                                PartEvent::Removed(ObjRemovedFromPart {
                                    cursor: removed_cursor,
                                    part_id,
                                    obj_id,
                                }),
                            )
                        } else if member.changed_at > member.added_at {
                            let payload = guard
                                .objs
                                .get(&obj_id)
                                .and_then(|obj| obj.payload.clone())
                                .unwrap_or(serde_json::Value::Null);
                            (
                                member.changed_at,
                                PartEvent::Changed(ObjChanged {
                                    cursor: member.changed_at,
                                    part_ids: vec![part_id],
                                    obj_id,
                                    payload,
                                }),
                            )
                        } else {
                            let payload = guard
                                .objs
                                .get(&obj_id)
                                .and_then(|obj| obj.payload.clone())
                                .unwrap_or(serde_json::Value::Null);
                            (
                                member.added_at,
                                PartEvent::Added(ObjAddedToPart {
                                    cursor: member.added_at,
                                    part_id,
                                    obj_id,
                                    payload,
                                }),
                            )
                        };
                        (event_cursor > cursor).then_some((event_cursor, event))
                    })
                    .collect::<Vec<_>>();
                candidates.sort_by_key(|(event_cursor, _)| *event_cursor);
                let has_more = candidates.len() > limit as usize;
                candidates.truncate(limit as usize);
                let next_cursor = has_more
                    .then(|| candidates.last().map(|(event_cursor, _)| *event_cursor))
                    .flatten();
                out.insert(
                    part_id,
                    PartPage {
                        events: candidates.into_iter().map(|(_, event)| event).collect(),
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
            page_count = result.as_ref().map(|res| res.len()).unwrap_or(0),
            "memory store list events"
        );
        Ok(result)
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;

        let part_cursors: HashMap<PartKey, CursorIndex> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    Some((*part_id, reqs.lower_bound.max(*cursor)))
                }
                SubscriptionTarget::Object { .. } => None,
            })
            .collect();
        let objects: HashSet<ObjKey> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id } => Some(*obj_id),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect();
        if !objects.is_empty() {
            self.materialize_object_parts(objects.iter().copied().collect());
        }
        let unknown_parts = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            part_cursors
                .keys()
                .filter(|part_id| {
                    self.hidden_parts.contains(part_id) || !guard.parts.contains_key(part_id)
                })
                .copied()
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
            objects,
            object_bounds: reqs
                .targets
                .iter()
                .filter_map(|target| match target {
                    SubscriptionTarget::Object { obj_id } => Some((*obj_id, reqs.lower_bound)),
                    SubscriptionTarget::Part { .. } => None,
                })
                .collect(),
        };
        let source: Arc<dyn MemoryKeyedFrontierSource<PartFrontierKey, PartEvent>> =
            Arc::new(MemoryPartEventSource {
                state: Arc::clone(&self.inner),
            });
        let mut reader: Box<dyn KeyedFrontierReader<PartFrontierKey, PartEvent>> =
            open_memory_keyed_frontier(source, selector.clone()).await?;
        let state = Arc::clone(&self.inner);
        let (tx, rx) = mpsc::unbounded("MemoryPartStore".into(), "caller".into());
        tokio::spawn(async move {
            loop {
                match reader.next(FrontierReadLimits::default()).await.unwrap() {
                    FrontierRead::Entries { entries, .. } => {
                        let events = surelock::key::lock_scope(|key| {
                            let (guard, _key) = key.lock(&state);
                            let mut projected = Vec::new();
                            for entry in entries {
                                let event = match (entry.key, entry.value) {
                                    (PartFrontierKey::Object(_), None) => continue,
                                    (PartFrontierKey::Part { obj_id, part_id }, None) => {
                                        PartEvent::Removed(ObjRemovedFromPart {
                                            cursor: entry.revision,
                                            part_id,
                                            obj_id,
                                        })
                                    }
                                    (_, Some(event)) => event,
                                };
                                let Some(event) =
                                    project_part_event(&guard, event, &selector, subscriber)
                                else {
                                    continue;
                                };
                                if let SubEvent::Changed(changed) = &event
                                    && let Some(SubEvent::Changed(existing)) =
                                        projected.iter_mut().find(|candidate| {
                                            matches!(candidate, SubEvent::Changed(candidate)
                                                if candidate.cursor == changed.cursor
                                                    && candidate.obj_id == changed.obj_id)
                                        })
                                {
                                    for part_id in &changed.part_ids {
                                        if !existing.part_ids.contains(part_id) {
                                            existing.part_ids.push(*part_id);
                                        }
                                    }
                                    existing.payload = changed.payload.clone();
                                    continue;
                                }
                                projected.push(event);
                            }
                            projected
                        });
                        for event in events {
                            if tx.send(event).await.is_err() {
                                return;
                            }
                        }
                    }
                    FrontierRead::ReplayComplete { .. } => {
                        if tx.send(SubEvent::ReplayComplete).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Ok(rx))
    }

    async fn open_local_revision_reader(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn super::LocalPartRevisionReader>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;

        let part_cursors = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    Some((*part_id, reqs.lower_bound.max(*cursor)))
                }
                SubscriptionTarget::Object { .. } => None,
            })
            .collect::<HashMap<_, _>>();
        let parts = part_cursors.keys().copied().collect::<HashSet<_>>();
        let objects = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id } => Some(*obj_id),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect::<HashSet<_>>();
        let unknown_parts = surelock::key::lock_scope(|key| {
            let (guard, _key) = key.lock(&self.inner);
            part_cursors
                .keys()
                .filter(|part_id| !guard.parts.contains_key(part_id))
                .copied()
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
                    SubscriptionTarget::Object { obj_id } => Some((*obj_id, reqs.lower_bound)),
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

    async fn open_local_revision_reader_all(
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

    /// This store's subscriptions are filtered per recipient, so a page can be denied rather
    /// than silently empty.
    async fn page_denied(
        &self,
        target: &SubscriptionTarget,
        subscriber: PeerKey,
    ) -> Res<bool> {
        let (scope, obj_id) = match target {
            SubscriptionTarget::Part { part_id, .. } => {
                // `permitted_parts` reads the object only for a `FromObject` scope,
                // so a part is asked about directly.
                (PartScope::Part(*part_id), ObjKey::new([0u8; 32]))
            }
            SubscriptionTarget::Object { obj_id } => (PartScope::FromObject, *obj_id),
        };
        Ok(self
            .permitted_parts(scope, obj_id, Some(subscriber))
            .await?
            .is_some_and(|readable| readable.is_empty()))
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
    use crate::part_store::host_contract::{self, HostPartStoreContractHarness};
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

    /// The memory store's counterpart to the sqlite direct-share test: an explicit row on the
    /// derived object part is a direct share, and it resolves because subscribing materializes
    /// the object's single membership row.
    #[tokio::test(flavor = "multi_thread")]
    async fn memory_subscribe_object_part_direct_share_is_delivered_remotely() -> Res<()> {
        use big_sync_core::rpc::SubscriptionTarget;

        let store = MemoryPartStore::new();
        let obj_id = ObjKey(ByteKey::new([65u8; 32]));
        let peer = PeerKey::new([66u8; 32]);
        let payload = serde_json::json!({"value": 1});
        store.set_obj_payload(obj_id, payload.clone()).await?;
        store
            .set_part_members(
                obj_id.object_part_key(),
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([SubscriptionTarget::Object { obj_id }]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        match rx.recv().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.obj_id, obj_id);
                assert_eq!(changed.payload, payload);
            }
            event => panic!("expected the directly shared object, got {event:?}"),
        }
        assert_eq!(
            rx.recv().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
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
        store.ensure_part(part).await?;
        store
            .set_obj_payload(obj_id, serde_json::json!({"value": 1}))
            .await?;
        store.add_obj_to_parts(obj_id, vec![part]).await?;
        store
            .set_part_members(
                part,
                HashMap::from([(member, keyhive_core::access::Access::Read)]),
            )
            .await?;

        let readable = store
            .replay_page(
                SubscriptionTarget::Part {
                    part_id: part,
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

    /// Materializing an object part consumes no revision and emits no event: subscribing is not a
    /// sync event, so the row can only be stamped with a revision the store already had. This
    /// drives the object-target subscription that performs the materialization, so the coverage is
    /// that such a subscription completes without advancing the cursor space.
    #[tokio::test(flavor = "multi_thread")]
    async fn memory_materializing_object_part_is_quiet() -> Res<()> {
        use big_sync_core::rpc::SubscriptionTarget;

        let store = MemoryPartStore::new();
        let obj_id = ObjKey(ByteKey::new([71u8; 32]));
        let peer = PeerKey::new([72u8; 32]);
        store
            .set_obj_payload(obj_id, serde_json::json!({"value": 1}))
            .await?;
        store
            .set_part_members(
                obj_id.object_part_key(),
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await?;
        let before = store.latest_revision().await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([SubscriptionTarget::Object { obj_id }]),
                },
                peer,
            )
            .await??;
        loop {
            let event = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await??;
            if matches!(event, SubEvent::ReplayComplete) {
                break;
            }
        }

        let after = store.latest_revision().await?;
        assert_eq!(
            after, before,
            "materialization is quiet: subscribing consumes no revision, so a subscriber asking\
             for an object leaves the cursor space unchanged"
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

        store.ensure_part(part).await?;
        store
            .set_part_members(
                part,
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await?;
        for (obj, value) in [(first, "first"), (second, "second")] {
            store.set_obj_payload(obj, serde_json::json!(value)).await?;
        }
        store.add_obj_to_parts(first, vec![part]).await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                peer,
            )
            .await??;

        // This mutation is deliberately issued immediately after subscribe:
        // it may be observed by replay or by the pending-to-live handoff, but
        // it must not be lost in either case.
        store.add_obj_to_parts(second, vec![part]).await?;

        let mut seen = HashSet::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await??;
            match event {
                SubEvent::Added(event) => {
                    seen.insert(event.obj_id);
                }
                SubEvent::ReplayComplete => break,
                SubEvent::Changed(_) | SubEvent::Removed(_) => {}
            }
        }
        if !seen.contains(&second) {
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await??;
            assert!(
                matches!(&event, SubEvent::Added(event) if event.obj_id == second),
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

        store.ensure_part(part).await?;
        store
            .set_obj_payload(obj, serde_json::json!("content"))
            .await?;

        // Set doc members: only `reader` has Read access.
        let mut agents = HashMap::new();
        agents.insert(reader, keyhive_core::access::Access::Read);
        store.set_part_members(part, agents.clone()).await?;

        // Subscribe as reader — should receive the Added event.
        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                reader,
            )
            .await??;
        store.add_obj_to_parts(obj, vec![part]).await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match rx.recv().await {
                    Ok(SubEvent::Added(_)) => return Ok::<_, eyre::Report>(()),
                    Ok(SubEvent::ReplayComplete) => continue,
                    Ok(_) => continue,
                    Err(_) => return Err(ferr!("stream closed")),
                }
            }
        })
        .await??;

        // Subscribe as non-reader — should NOT receive the Added event.
        let rx2 = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                non_reader,
            )
            .await??;
        tokio::time::timeout(Duration::from_secs(2), async {
            match rx2.recv().await {
                Ok(SubEvent::ReplayComplete) => Ok::<_, eyre::Report>(()),
                Ok(SubEvent::Added(event)) => {
                    Err(ferr!("denied replay leaked Added event: {event:?}"))
                }
                Ok(event) => Err(ferr!("denied replay leaked event: {event:?}")),
                Err(_) => Err(ferr!("denied subscriber closed during replay")),
            }
        })
        .await??;
        let second_obj = ObjKey(ByteKey::new([5u8; 32]));
        store.set_part_members(part, agents).await?;
        store
            .set_obj_payload(second_obj, serde_json::json!("content2"))
            .await?;
        store.add_obj_to_parts(second_obj, vec![part]).await?;
        match tokio::time::timeout(Duration::from_millis(200), rx2.recv()).await {
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

        store.ensure_part(part).await?;
        store
            .set_obj_payload(obj, serde_json::json!("initial"))
            .await?;

        // Initially peer has Read access.
        let mut agents = HashMap::new();
        agents.insert(peer, keyhive_core::access::Access::Read);
        store.set_part_members(part, agents).await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Part {
                        part_id: part,
                        cursor: 0,
                    }]),
                },
                peer,
            )
            .await??;
        store.add_obj_to_parts(obj, vec![part]).await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut saw_added = false;
            let mut saw_replay_complete = false;
            loop {
                match rx.recv().await {
                    Ok(SubEvent::Added(_)) => saw_added = true,
                    Ok(SubEvent::ReplayComplete) => saw_replay_complete = true,
                    Ok(event) => return Err(ferr!("unexpected authorized event: {event:?}")),
                    Err(_) => return Err(ferr!("authorized subscriber closed")),
                }
                if saw_added && saw_replay_complete {
                    return Ok::<_, eyre::Report>(());
                }
            }
        })
        .await??;
        store.add_obj_to_parts(obj, vec![part]).await?;
        // Should receive Added event.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match rx.recv().await {
                    Ok(SubEvent::Added(_)) => return,
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
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
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

        store.ensure_part(part).await?;
        store.ensure_part(other_part).await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), 0).await?,
            PartDirtyCount::default(),
            "a part with neither members nor grants has no relevance to count"
        );

        // A member write moves the member number only.
        store
            .set_obj_payload(obj_a, serde_json::json!({"a": 1}))
            .await?;
        store.add_obj_to_parts(obj_a, vec![part]).await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), 0).await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 0,
            }
        );

        // A grant on this part for this peer adds the access number alongside it.
        store
            .set_part_members(
                part,
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), 0).await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 1,
            }
        );

        // The local principal is never gated by access rows, so it has no access half;
        // the member half does not depend on who is asking and still counts.
        assert_eq!(
            store.part_dirty_count(part, None, 0).await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 0,
            }
        );

        // A second member write moves only the member number.
        store
            .set_obj_payload(obj_b, serde_json::json!({"b": 1}))
            .await?;
        store.add_obj_to_parts(obj_b, vec![part]).await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), 0).await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            }
        );

        // Another principal's grant on this part is not this principal's relevance.
        store
            .add_part_member(part, other_peer, keyhive_core::access::Access::Read)
            .await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), 0).await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            },
            "another principal's grant must not be counted for this one"
        );

        // Another part's member write and grant are not this part's relevance.
        store
            .set_obj_payload(obj_c, serde_json::json!({"c": 1}))
            .await?;
        store.add_obj_to_parts(obj_c, vec![other_part]).await?;
        store
            .set_part_members(
                other_part,
                HashMap::from([(peer, keyhive_core::access::Access::Read)]),
            )
            .await?;
        assert_eq!(
            store.part_dirty_count(part, Some(peer), 0).await?,
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
