use super::HostPartStore;
use super::sqlite_core::{EVENT_ADDED, EVENT_CHANGED, EVENT_REMOVED};
use crate::interlude::*;
#[cfg(test)]
use crate::test_support::{ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot};

#[cfg(test)]
use big_sync_core::Byte32Id;
#[cfg(test)]
use big_sync_core::part_store::PartStoreReadOnly;
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, ObjAddedToPart, ObjChanged,
    ObjRemovedFromPart, PartEvent, PartPage, PartSummary, SubEvent, SubPartsRequest,
};
use big_sync_core::{BuckId, Fingerprint, ObjId, PartId, PeerId, mpsc};
#[cfg(test)]
use future_form::{FutureForm, Sendable};
#[cfg(test)]
use futures::future::BoxFuture;
use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;
#[cfg(test)]
use uuid::Uuid;

use super::sqlite_core::{PendingSubscription, SUB_FINALIZING, SUB_REPLAYING_CLEAN, encode_access};

struct SqliteSubscription {
    sender: mpsc::Sender<SubEvent>,
    principal: PeerId,
    pending: Arc<PendingSubscription>,
}

struct ReplayCandidate {
    txid: CursorIndex,
    obj_id: ObjId,
    part_id: Option<PartId>,
    event_type: i64,
    payload: ObjPayload,
}

#[derive(Default)]
struct SqliteSubscriptions {
    by_part: HashMap<PartId, HashSet<Uuid>>,
    parts_by_sub: HashMap<Uuid, HashSet<PartId>>,
    by_obj: HashMap<ObjId, HashSet<Uuid>>,
    objs_by_sub: HashMap<Uuid, HashSet<ObjId>>,
    pending: HashSet<Uuid>,
    live: HashSet<Uuid>,
    subs: HashMap<Uuid, Arc<SqliteSubscription>>,
}

impl SqliteSubscriptions {
    fn remove(&mut self, sub_id: Uuid) {
        self.pending.remove(&sub_id);
        self.live.remove(&sub_id);
        self.subs.remove(&sub_id);
        if let Some(parts) = self.parts_by_sub.remove(&sub_id) {
            for part_id in parts {
                if let Some(subs) = self.by_part.get_mut(&part_id) {
                    subs.remove(&sub_id);
                }
            }
        }
        if let Some(obj_ids) = self.objs_by_sub.remove(&sub_id) {
            for obj_id in obj_ids {
                if let Some(subs) = self.by_obj.get_mut(&obj_id) {
                    subs.remove(&sub_id);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SqlitePartStore {
    pub(crate) core: SqliteCore,
    bus: Arc<std::sync::RwLock<SqliteSubscriptions>>,
    hidden_parts: Arc<HashSet<PartId>>,
    live_debouncer: Arc<LiveDebouncer>,
}

/// Semantic target for debouncing state events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum DebounceTarget {
    Part(PartId, ObjId),
    Object(ObjId),
}

impl DebounceTarget {
    pub(crate) fn obj_id(&self) -> ObjId {
        match self {
            Self::Part(_, obj_id) | Self::Object(obj_id) => *obj_id,
        }
    }

    pub(crate) fn part_id(&self) -> Option<PartId> {
        match self {
            Self::Part(part_id, _) => Some(*part_id),
            Self::Object(_) => None,
        }
    }
}

fn reduce_sub_event(existing: &mut SubEvent, new: SubEvent) {
    match (std::mem::replace(existing, SubEvent::ReplayComplete), new) {
        (SubEvent::Added(mut existing_added), SubEvent::Added(new_added)) => {
            if new_added.cursor >= existing_added.cursor {
                existing_added.cursor = new_added.cursor;
                existing_added.payload = new_added.payload;
            }
            *existing = SubEvent::Added(existing_added);
        }
        (SubEvent::Added(mut existing_added), SubEvent::Changed(new_changed)) => {
            // Retain Added variant so receiver adds the object to the partition
            if new_changed.cursor >= existing_added.cursor {
                existing_added.cursor = new_changed.cursor;
                existing_added.payload = new_changed.payload;
            }
            *existing = SubEvent::Added(existing_added);
        }
        (SubEvent::Added(existing_added), SubEvent::Removed(mut new_removed)) => {
            new_removed.cursor = new_removed.cursor.max(existing_added.cursor);
            *existing = SubEvent::Removed(new_removed);
        }
        (SubEvent::Changed(mut existing_changed), SubEvent::Changed(new_changed)) => {
            for part_id in new_changed.part_ids {
                if !existing_changed.part_ids.contains(&part_id) {
                    existing_changed.part_ids.push(part_id);
                }
            }
            if new_changed.cursor >= existing_changed.cursor {
                existing_changed.cursor = new_changed.cursor;
                existing_changed.payload = new_changed.payload;
            }
            existing_changed.part_ids.sort_unstable();
            *existing = SubEvent::Changed(existing_changed);
        }
        (SubEvent::Changed(existing_changed), SubEvent::Removed(mut new_removed)) => {
            new_removed.cursor = new_removed.cursor.max(existing_changed.cursor);
            *existing = SubEvent::Removed(new_removed);
        }
        (SubEvent::Changed(existing_changed), SubEvent::Added(mut new_added)) => {
            if new_added.cursor < existing_changed.cursor {
                new_added.cursor = existing_changed.cursor;
            }
            *existing = SubEvent::Added(new_added);
        }
        (SubEvent::Removed(mut existing_removed), SubEvent::Removed(new_removed)) => {
            existing_removed.cursor = existing_removed.cursor.max(new_removed.cursor);
            *existing = SubEvent::Removed(existing_removed);
        }
        (SubEvent::Removed(mut existing_removed), SubEvent::Changed(new_changed)) => {
            // Stale Changed after Removed: retain Removed, advance cursor
            existing_removed.cursor = existing_removed.cursor.max(new_changed.cursor);
            *existing = SubEvent::Removed(existing_removed);
        }
        (SubEvent::Removed(existing_removed), SubEvent::Added(mut new_added)) => {
            if new_added.cursor < existing_removed.cursor {
                new_added.cursor = existing_removed.cursor;
            }
            *existing = SubEvent::Added(new_added);
        }
        (_old, new) => {
            *existing = new;
        }
    }
}

/// Per-store debouncer for live-subscriber state events.
///
/// Collapses bursts of state changes to the same (subscriber, target) into a
/// single delivery using semantic event accumulation.
struct LiveDebouncer {
    batcher: std::sync::Mutex<
        utils_rs::batching::KeyedBatcher<
            (Uuid, DebounceTarget),
            SubEvent,
            utils_rs::batching::DebouncePolicy,
        >,
    >,
    /// Owned separately so the flush task can wait on it without keeping the
    /// whole debouncer (and therefore the store's shutdown signal) alive.
    notify: Arc<tokio::sync::Notify>,
}

impl LiveDebouncer {
    fn new(policy: utils_rs::batching::DebouncePolicy) -> Arc<Self> {
        Arc::new(Self {
            batcher: std::sync::Mutex::new(utils_rs::batching::KeyedBatcher::new(
                policy,
                |_event: &SubEvent| 0,
                reduce_sub_event,
            )),
            notify: Arc::new(tokio::sync::Notify::new()),
        })
    }
}

/// Flush loop for [`LiveDebouncer`]. Runs until the store (the debouncer's
/// owner) is dropped; the store's own teardown never needs to wait on it.
async fn flush_live_debouncer(
    debouncer: std::sync::Weak<LiveDebouncer>,
    notify: Arc<tokio::sync::Notify>,
    bus: Arc<std::sync::RwLock<SqliteSubscriptions>>,
    core: SqliteCore,
) {
    // When the batcher is empty there is no deadline to sleep for; poll the
    // owner liveness at this interval so a dropped store stops the loop.
    const EMPTY_RECHECK: std::time::Duration = std::time::Duration::from_secs(5);
    loop {
        let deadline = {
            let Some(debouncer) = debouncer.upgrade() else {
                break;
            };
            debouncer.batcher.lock().expect(ERROR_MUTEX).next_deadline()
        };
        let sleep = match deadline {
            Some(deadline) => {
                futures::future::Either::Left(tokio::time::sleep_until(deadline.into()))
            }
            None => futures::future::Either::Right(tokio::time::sleep(EMPTY_RECHECK)),
        };
        tokio::select! {
            // A push only moves the trailing-edge deadline; delivery happens
            // when the (recomputed) deadline actually fires. This is what
            // makes the debounce a true trailing-edge collapse: a burst that
            // keeps pushing keeps pushing the deadline out, so the whole
            // burst — however long it spans — merges into one delivery.
            _ = notify.notified() => {}
            _ = sleep => {
                let due = {
                    let Some(debouncer) = debouncer.upgrade() else {
                        break;
                    };
                    debouncer
                        .batcher
                        .lock()
                        .expect(ERROR_MUTEX)
                        .take_due(std::time::Instant::now())
                };
                if !due.is_empty() {
                    deliver_due(&bus, &core, due).await;
                }
            }
        }
    }
}

/// Deliver a batch of due state events. The subscriber is re-resolved at flush
/// time (it may have disconnected), and the event is sent lossy (a full or
/// closed stream drops the subscriber, matching the immediate path).
async fn deliver_due(
    bus: &std::sync::RwLock<SqliteSubscriptions>,
    core: &SqliteCore,
    due: Vec<((Uuid, DebounceTarget), SubEvent)>,
) {
    let mut grouped: Vec<((Uuid, DebounceTarget), SubEvent)> = Vec::with_capacity(due.len());
    for ((sub_id, target), event) in due {
        let merge_idx = match &event {
            SubEvent::Changed(new_changed) => grouped.iter().position(
                |((candidate_sub_id, candidate_target), candidate_event)| {
                    *candidate_sub_id == sub_id
                        && *candidate_target == target
                        && matches!(
                            candidate_event,
                            SubEvent::Changed(existing)
                                if existing.obj_id == new_changed.obj_id
                                    && existing.cursor == new_changed.cursor
                        )
                },
            ),
            _ => None,
        };
        if let Some(index) = merge_idx {
            reduce_sub_event(&mut grouped[index].1, event);
        } else {
            grouped.push(((sub_id, target), event));
        }
    }
    let mut drop_subs = HashSet::new();
    let mut perm_cache: HashMap<(Option<PartId>, ObjId, PeerId), bool> = HashMap::new();
    for ((sub_id, target), event) in grouped {
        let (principal, sender) = {
            let bus = bus.read().expect(ERROR_MUTEX);
            let Some(sub) = bus.subs.get(&sub_id) else {
                continue;
            };
            (sub.principal, sub.sender.clone())
        };
        let key = (target.part_id(), target.obj_id(), principal);
        let permitted = if let Some(&cached) = perm_cache.get(&key) {
            cached
        } else {
            match event_permitted(core, key.0, key.1, Some(key.2)).await {
                Ok(is_permitted) => {
                    perm_cache.insert(key, is_permitted);
                    is_permitted
                }
                Err(err) => {
                    tracing::warn!(?err, %sub_id, "failed checking event permission during debounce flush");
                    continue;
                }
            }
        };
        if !permitted {
            continue;
        }
        if sender.try_send(event).is_err() {
            drop_subs.insert(sub_id);
        }
    }
    if !drop_subs.is_empty() {
        let mut bus = bus.write().expect(ERROR_MUTEX);
        for sub_id in drop_subs {
            bus.remove(sub_id);
        }
    }
}

use super::sqlite_core::MemberState;
use super::sqlite_core::SqliteCore;

/// Thin forwarding helpers so call sites inside SqlitePartStore's
/// HostPartStore impl continue to compile without changes.
impl SqlitePartStore {
    fn part_blob(id: PartId) -> Vec<u8> {
        SqliteCore::part_blob(id)
    }
    fn obj_blob(id: ObjId) -> Vec<u8> {
        SqliteCore::obj_blob(id)
    }
    fn peer_blob(id: PeerId) -> Vec<u8> {
        SqliteCore::peer_blob(id)
    }
    fn buck_i64(id: BuckId) -> i64 {
        SqliteCore::buck_i64(id)
    }
    fn buck_id(value: i64) -> BuckId {
        SqliteCore::buck_id(value)
    }
    fn u64_from_db(value: i64) -> u64 {
        SqliteCore::u64_from_db(value)
    }
    fn part_from_blob(blob: Vec<u8>) -> PartId {
        SqliteCore::part_from_blob(blob)
    }
    fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        SqliteCore::obj_from_blob(blob)
    }
    #[cfg(test)]
    fn peer_from_blob(blob: Vec<u8>) -> PeerId {
        SqliteCore::peer_from_blob(blob)
    }
    async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        SqliteCore::next_cursor(tx).await
    }
}

impl SqlitePartStore {
    pub async fn new(sql: SqlCtx, scope_key: impl Into<Arc<str>>, bucket_depth: u8) -> Res<Self> {
        Self::new_with_config(sql, scope_key, bucket_depth, Default::default()).await
    }

    pub async fn new_with_config(
        sql: SqlCtx,
        scope_key: impl Into<Arc<str>>,
        bucket_depth: u8,
        config: super::HostPartStoreConfig,
    ) -> Res<Self> {
        SqliteCore::init_schema(&sql.write_pool, bucket_depth).await?;
        let core = SqliteCore::new(sql, scope_key, bucket_depth).await?;
        let bus: Arc<std::sync::RwLock<SqliteSubscriptions>> =
            Arc::new(std::sync::RwLock::new(SqliteSubscriptions::default()));

        // Store-owned debounced delivery for live subscribers. The flush task
        // stops itself when the store (its owner) drops — no explicit stop
        // token needed.
        let live_debouncer = LiveDebouncer::new(utils_rs::batching::DebouncePolicy {
            quiet_window: config.debounce_quiet_window,
            max_latency: config.debounce_max_latency,
        });
        tokio::spawn(flush_live_debouncer(
            Arc::downgrade(&live_debouncer),
            Arc::clone(&live_debouncer.notify),
            Arc::clone(&bus),
            core.clone(),
        ));

        Ok(Self {
            core,
            bus,
            hidden_parts: Arc::new(config.hidden_parts),
            live_debouncer,
        })
    }
    async fn publish(&self, events: Vec<SubEvent>) {
        let mut dispatch = Vec::new();
        {
            let bus = self.bus.read().expect(ERROR_MUTEX);
            let mut recipients: HashMap<(Uuid, DebounceTarget), SubEvent> = HashMap::new();
            let push_recipient = |recipients: &mut HashMap<(Uuid, DebounceTarget), SubEvent>,
                                  sub_id: Uuid,
                                  target: DebounceTarget,
                                  event: SubEvent| {
                recipients
                    .entry((sub_id, target))
                    .and_modify(|existing| reduce_sub_event(existing, event.clone()))
                    .or_insert(event);
            };
            for event in events {
                match event {
                    SubEvent::Changed(inner) => {
                        let mut sub_ids = HashSet::new();
                        for part_id in &inner.part_ids {
                            if let Some(subs) = bus.by_part.get(part_id) {
                                sub_ids.extend(subs.iter().copied());
                            }
                        }
                        if let Some(subs) = bus.by_obj.get(&inner.obj_id) {
                            sub_ids.extend(subs.iter().copied());
                        }
                        for sub_id in sub_ids {
                            let object_match = bus
                                .objs_by_sub
                                .get(&sub_id)
                                .is_some_and(|objects| objects.contains(&inner.obj_id));
                            let mut part_ids = inner
                                .part_ids
                                .iter()
                                .copied()
                                .filter(|part_id| {
                                    bus.parts_by_sub
                                        .get(&sub_id)
                                        .is_some_and(|parts| parts.contains(part_id))
                                })
                                .collect::<Vec<_>>();
                            part_ids.sort_unstable();
                            if object_match {
                                let mut projected = inner.clone();
                                projected.part_ids = part_ids;
                                push_recipient(
                                    &mut recipients,
                                    sub_id,
                                    DebounceTarget::Object(inner.obj_id),
                                    SubEvent::Changed(projected),
                                );
                            } else if part_ids.len() == 1 {
                                let part_id = part_ids[0];
                                let mut projected = inner.clone();
                                projected.part_ids = part_ids;
                                push_recipient(
                                    &mut recipients,
                                    sub_id,
                                    DebounceTarget::Part(part_id, inner.obj_id),
                                    SubEvent::Changed(projected),
                                );
                            } else {
                                let merge_into_added = part_ids.iter().all(|part_id| {
                                    matches!(
                                        recipients.get(&(
                                            sub_id,
                                            DebounceTarget::Part(*part_id, inner.obj_id),
                                        )),
                                        Some(SubEvent::Added(_))
                                    )
                                });
                                if merge_into_added {
                                    for part_id in part_ids {
                                        let mut projected = inner.clone();
                                        projected.part_ids = vec![part_id];
                                        push_recipient(
                                            &mut recipients,
                                            sub_id,
                                            DebounceTarget::Part(part_id, inner.obj_id),
                                            SubEvent::Changed(projected),
                                        );
                                    }
                                } else {
                                    let mut projected = inner.clone();
                                    projected.part_ids = part_ids;
                                    push_recipient(
                                        &mut recipients,
                                        sub_id,
                                        DebounceTarget::Object(inner.obj_id),
                                        SubEvent::Changed(projected),
                                    );
                                }
                            }
                        }
                    }
                    SubEvent::Added(inner) => {
                        let part_subs =
                            bus.by_part.get(&inner.part_id).cloned().unwrap_or_default();
                        for sub_id in &part_subs {
                            push_recipient(
                                &mut recipients,
                                *sub_id,
                                DebounceTarget::Part(inner.part_id, inner.obj_id),
                                SubEvent::Added(inner.clone()),
                            );
                        }
                        if let Some(subs) = bus.by_obj.get(&inner.obj_id) {
                            for &sub_id in subs {
                                if !part_subs.contains(&sub_id) {
                                    push_recipient(
                                        &mut recipients,
                                        sub_id,
                                        DebounceTarget::Object(inner.obj_id),
                                        SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                                            cursor: inner.cursor,
                                            part_ids: Vec::new(),
                                            obj_id: inner.obj_id,
                                            payload: inner.payload.clone(),
                                        }),
                                    );
                                }
                            }
                        }
                    }
                    SubEvent::Removed(inner) => {
                        if let Some(subs) = bus.by_part.get(&inner.part_id) {
                            for &sub_id in subs {
                                push_recipient(
                                    &mut recipients,
                                    sub_id,
                                    DebounceTarget::Part(inner.part_id, inner.obj_id),
                                    SubEvent::Removed(inner.clone()),
                                );
                            }
                        }
                    }
                    SubEvent::ReplayComplete => {}
                }
            }
            for ((sub_id, target), event) in recipients {
                let Some(sub) = bus.subs.get(&sub_id) else {
                    continue;
                };
                if bus.pending.contains(&sub_id) {
                    if sub.pending.mark_dirty() {
                        dispatch.push((sub_id, target, event));
                    }
                    continue;
                }
                if bus.live.contains(&sub_id) {
                    dispatch.push((sub_id, target, event));
                }
            }
        }
        if !dispatch.is_empty() {
            let mut batcher = self.live_debouncer.batcher.lock().expect(ERROR_MUTEX);
            let now = std::time::Instant::now();
            for (sub_id, target, event) in dispatch {
                batcher.push(now, (sub_id, target), event);
            }
            self.live_debouncer.notify.notify_one();
        }
    }
}

impl SqlitePartStore {
    async fn replay_candidates(
        &self,
        parts: &HashSet<PartId>,
        objects: &HashSet<ObjId>,
        lower_bound: CursorIndex,
        exact_txid: Option<CursorIndex>,
        limit: Option<u32>,
    ) -> Res<Vec<ReplayCandidate>> {
        if parts.is_empty() && objects.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT m.txid, o.obj_id, p.part_id, m.event_type, o.payload_json
             FROM big_sync_members m
             JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
             LEFT JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        if let Some(txid) = exact_txid {
            query.push(" AND m.txid = ");
            query.push_bind(i64::try_from(txid).expect(ERROR_IMPOSSIBLE));
        } else {
            query.push(" AND m.txid > ");
            query.push_bind(i64::try_from(lower_bound).expect(ERROR_IMPOSSIBLE));
        }
        query.push(" AND (");
        if parts.is_empty() {
            query.push("0");
        } else {
            query
                .push("m.maybe_part_ref IN (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
            query.push_bind(self.core.scope_id);
            query.push(" AND part_id IN (");
            let mut separated = query.separated(", ");
            for part in parts {
                separated.push_bind(Self::part_blob(*part));
            }
            separated.push_unseparated("))");
        }
        query.push(" OR ");
        if objects.is_empty() {
            query.push("0");
        } else {
            query.push("m.obj_ref IN (SELECT obj_ref FROM big_sync_objs WHERE scope_id = ");
            query.push_bind(self.core.scope_id);
            query.push(" AND obj_id IN (");
            let mut separated = query.separated(", ");
            for obj in objects {
                separated.push_bind(Self::obj_blob(*obj));
            }
            separated.push_unseparated("))");
        }
        query.push(") ORDER BY m.txid, m.obj_ref, m.maybe_part_ref");
        if let Some(limit) = limit {
            query.push(" LIMIT ");
            query.push_bind(i64::from(limit));
        }
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                let payload = row
                    .try_get::<Option<String>, _>("payload_json")?
                    .as_deref()
                    .filter(|str| !str.is_empty())
                    .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                let part_id = row
                    .try_get::<Option<Vec<u8>>, _>("part_id")?
                    .map(Self::part_from_blob);
                Ok(ReplayCandidate {
                    txid: u64::try_from(row.try_get::<i64, _>("txid")?).expect(ERROR_IMPOSSIBLE),
                    obj_id: Self::obj_from_blob(row.try_get("obj_id")?),
                    part_id,
                    event_type: row.try_get("event_type")?,
                    payload,
                })
            })
            .collect()
    }
}

#[async_trait]
impl HostPartStore for SqlitePartStore {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>> {
        if parts.is_empty() {
            return Ok(Ok(HashMap::new()));
        }
        let mut hidden: Vec<_> = parts.intersection(&self.hidden_parts).copied().collect();
        if !hidden.is_empty() {
            hidden.sort_unstable();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: hidden,
            }));
        }

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT p.part_id, p.latest_cursor, COALESCE(b.live_count, 0) AS member_count
             FROM big_sync_parts p
             LEFT JOIN big_sync_buckets b
               ON b.scope_id = p.scope_id
              AND b.part_ref = p.part_ref
              AND b.level = 0
              AND b.buck_id = 0
             WHERE p.scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        query.push(" AND p.part_id IN (");
        let mut separated = query.separated(", ");
        for part_id in &parts {
            separated.push_bind(Self::part_blob(*part_id));
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;

        if rows.len() != parts.len() {
            let found: HashSet<PartId> = rows
                .iter()
                .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut missing: Vec<_> = parts.difference(&found).copied().collect();
            missing.sort();
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: missing,
            }));
        }

        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            let part_id = Self::part_from_blob(row.try_get("part_id")?);
            let latest_cursor: i64 = row.try_get("latest_cursor")?;
            let member_count: i64 = row.try_get("member_count")?;
            out.insert(
                part_id,
                PartSummary {
                    latest_cursor: u64::try_from(latest_cursor).expect(ERROR_IMPOSSIBLE),
                    member_count: u64::try_from(member_count).expect(ERROR_IMPOSSIBLE),
                    deepest_bucket_level: self.core.bucket_depth,
                },
            );
        }
        Ok(Ok(out))
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        if let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let mut out = HashMap::new();
        for part_id in parts {
            let requested_part = HashSet::from([part_id]);
            let candidates = self
                .replay_candidates(&requested_part, &HashSet::new(), cursor, None, Some(limit))
                .await?;
            let limit_usize = usize::try_from(limit).expect(ERROR_IMPOSSIBLE);
            let (candidates, next_cursor) = if limit_usize == 0 || candidates.len() < limit_usize {
                (candidates, None)
            } else {
                let cutoff = candidates.last().expect(ERROR_IMPOSSIBLE).txid;
                let mut page = candidates
                    .into_iter()
                    .filter(|candidate| candidate.txid < cutoff)
                    .collect::<Vec<_>>();
                let boundary = self
                    .replay_candidates(&requested_part, &HashSet::new(), cursor, Some(cutoff), None)
                    .await?;
                page.extend(boundary);
                let has_more = !self
                    .replay_candidates(&requested_part, &HashSet::new(), cutoff, None, Some(1))
                    .await?
                    .is_empty();
                (page, has_more.then_some(cutoff))
            };
            let events = candidates
                .iter()
                .map(|candidate| match candidate.event_type {
                    EVENT_ADDED => PartEvent::Added(ObjAddedToPart {
                        cursor: candidate.txid,
                        part_id,
                        obj_id: candidate.obj_id,
                        payload: candidate.payload.clone(),
                    }),
                    EVENT_REMOVED => PartEvent::Removed(ObjRemovedFromPart {
                        cursor: candidate.txid,
                        part_id,
                        obj_id: candidate.obj_id,
                    }),
                    _ => PartEvent::Changed(ObjChanged {
                        cursor: candidate.txid,
                        part_ids: vec![part_id],
                        obj_id: candidate.obj_id,
                        payload: candidate.payload.clone(),
                    }),
                })
                .collect();
            out.insert(
                part_id,
                PartPage {
                    events,
                    next_cursor,
                },
            );
        }
        Ok(Ok(out))
    }

    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        let member_count: Option<i64> = sqlx::query_scalar!(
            "SELECT live_count
             FROM big_sync_buckets
             WHERE scope_id = ?1 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?2
             ) AND level = 0 AND buck_id = 0",
            self.core.scope_id,
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        Ok(member_count
            .map(|member_count| u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let row = sqlx::query!(
            "SELECT payload_json
                 FROM big_sync_objs
                 WHERE scope_id = ?1 AND obj_id = ?2",
            self.core.scope_id,
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let payload: Option<String> = row.payload_json;
        payload
            .as_deref()
            .filter(|payload| !payload.is_empty())
            .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn set_obj_payload(&self, obj_id: ObjId, payload: ObjPayload) -> Res<()> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj_id).await?;
        let old_payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let live_parts = sqlx::query!(
            "SELECT m.maybe_part_ref, p.part_id
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2
               AND m.maybe_part_ref > 0 AND m.event_type != ?3",
            self.core.scope_id,
            obj_ref,
            EVENT_REMOVED
        )
        .fetch_all(&mut *tx)
        .await?;
        let pending_parts = sqlx::query!(
            "SELECT p.part_ref, p.part_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2",
            self.core.scope_id,
            obj_ref
        )
        .fetch_all(&mut *tx)
        .await?;
        let cursor = Self::next_cursor(&mut tx).await?;
        sqlx::query!(
            "UPDATE big_sync_objs SET payload_json = ?1 WHERE obj_ref = ?2",
            &payload_json,
            obj_ref
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
             VALUES (?1, ?2, 0, ?3, ?4)
             ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
                event_type = excluded.event_type, txid = excluded.txid",
            self.core.scope_id,
            obj_ref,
            EVENT_CHANGED,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&mut *tx)
        .await?;
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|str| !str.is_empty())
            .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        for part in &live_parts {
            let old_state = MemberState::Live(old_payload.clone());
            sqlx::query!(
                "UPDATE big_sync_members
                 SET event_type = ?1, txid = ?2
                 WHERE scope_id = ?3 AND obj_ref = ?4 AND maybe_part_ref = ?5",
                EVENT_CHANGED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.core.scope_id,
                obj_ref,
                part.maybe_part_ref
            )
            .execute(&mut *tx)
            .await?;
            self.core
                .apply_bucket_transition(
                    &mut tx,
                    Self::part_from_blob(part.part_id.clone()),
                    obj_id,
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?1)
                 WHERE part_ref = ?2 AND scope_id = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                part.maybe_part_ref,
                self.core.scope_id
            )
            .execute(&mut *tx)
            .await?;
        }
        let mut events = vec![SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: live_parts
                .iter()
                .map(|row| Self::part_from_blob(row.part_id.clone()))
                .collect(),
            obj_id,
            payload: payload.clone(),
        })];
        for part in pending_parts {
            let part_id = Self::part_from_blob(part.part_id);
            let part_ref = part.part_ref;
            sqlx::query!(
                "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
                    event_type = excluded.event_type, txid = excluded.txid",
                self.core.scope_id,
                obj_ref,
                part_ref,
                EVENT_ADDED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
            )
            .execute(&mut *tx)
            .await?;
            self.core
                .apply_bucket_transition(
                    &mut tx,
                    part_id,
                    obj_id,
                    cursor,
                    &MemberState::Absent,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "UPDATE big_sync_parts
                 SET latest_cursor = MAX(latest_cursor, ?1)
                 WHERE scope_id = ?2 AND part_ref = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE),
                self.core.scope_id,
                part_ref
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members
                 WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.core.scope_id,
                obj_ref,
                part_ref
            )
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        tx.commit().await?;
        self.publish(events).await;
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query!(
            "SELECT p.part_id
             FROM big_sync_members m
             JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             ) AND m.maybe_part_ref > 0 AND m.event_type != ?3
             UNION
             SELECT p.part_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = (
                 SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
             )
             ORDER BY part_id ASC",
            self.core.scope_id,
            Self::obj_blob(obj_id),
            EVENT_REMOVED
        )
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.part_id))
            .collect())
    }

    async fn obj_exists(&self, obj_id: ObjId) -> Res<bool> {
        let exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_objs
             WHERE scope_id = ?1 AND obj_id = ?2",
            self.core.scope_id,
            Self::obj_blob(obj_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        Ok(exists.is_some())
    }

    async fn get_bucket_summary(&self, part_id: PartId, id: BuckId) -> Res<BucketSummary> {
        self.core.bucket_summary_for_path(part_id, id).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.core.scope_id,
            Self::part_blob(req.part_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        let Some(_) = part_exists else {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        };

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT buck_id, level, changed_at, live_count, dead_count, live_fp, dead_fp
             FROM big_sync_buckets
             WHERE scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        query.push(" AND part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(self.core.scope_id);
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(")");
        query.push(" AND level = ");
        query.push_bind(i64::from(req.offset.level()));
        query.push(" AND buck_id >= ");
        query.push_bind(Self::buck_i64(req.offset));
        query.push(" AND changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" ORDER BY buck_id ASC LIMIT ");
        query.push_bind(i64::from(req.limit_hint) + i64::from(BuckId::ARITY));
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;

        if rows.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        let mut out = Vec::new();
        let mut last_parent = None;
        for row in rows {
            let bucket = BucketSummary {
                id: Self::buck_id(row.try_get::<i64, _>("buck_id")?),
                len: u32::try_from(
                    u64::try_from(row.try_get::<i64, _>("live_count")?).expect(ERROR_IMPOSSIBLE)
                        + u64::try_from(row.try_get::<i64, _>("dead_count")?)
                            .expect(ERROR_IMPOSSIBLE),
                )
                .expect(ERROR_IMPOSSIBLE),
                live_count: u32::try_from(row.try_get::<i64, _>("live_count")?)
                    .expect(ERROR_IMPOSSIBLE),
                fp: (
                    Self::u64_from_db(row.try_get::<i64, _>("live_fp")?),
                    Self::u64_from_db(row.try_get::<i64, _>("dead_fp")?),
                ),
                changed_at: u64::try_from(row.try_get::<i64, _>("changed_at")?)
                    .expect(ERROR_IMPOSSIBLE),
            };
            if out.len() < usize::try_from(req.limit_hint).expect(ERROR_IMPOSSIBLE) {
                out.push(bucket);
                continue;
            }
            let parent = bucket.id.parent();
            if last_parent.is_none() {
                last_parent = Some(out.last().expect(ERROR_IMPOSSIBLE).id.parent());
            }
            if Some(parent) != last_parent {
                break;
            }
            out.push(bucket);
        }
        Ok(Ok(out))
    }

    async fn leaf_buckets(
        &self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.core.scope_id,
            Self::part_blob(req.part_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        if part_exists.is_none() {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }

        if req.buckets.is_empty() {
            return Ok(Ok(LeafBucketResult {
                seed: req.seed,
                bucks: HashMap::new(),
            }));
        }

        struct LeafBucketPageBuilder {
            buck_id: BuckId,
            entries: Vec<BucketObjPageEntry>,
            total_count: u32,
        }

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "WITH requested(req_ord, buck_id, lower_id, upper_id, after_id) AS (",
        );
        for (req_ord, buck_req) in req.buckets.iter().enumerate() {
            let (lower_id, upper_id) = super::obj_id_bounds_for_bucket(buck_req.buck_id);
            if req_ord > 0 {
                query.push(" UNION ALL ");
            }
            query.push("SELECT ");
            query.push_bind(i64::try_from(req_ord).expect(ERROR_IMPOSSIBLE));
            query.push(" AS req_ord, ");
            query.push_bind(Self::buck_i64(buck_req.buck_id));
            query.push(" AS buck_id, ");
            query.push_bind(Self::obj_blob(lower_id));
            query.push(" AS lower_id, ");
            if let Some(upper_id) = upper_id {
                query.push_bind(Self::obj_blob(upper_id));
            } else {
                query.push("NULL");
            }
            query.push(" AS upper_id, ");
            if let Some(after) = buck_req.after {
                query.push_bind(Self::obj_blob(after));
            } else {
                query.push("NULL");
            }
            query.push(" AS after_id");
        }
        query.push(
            "), ranked AS (
                SELECT
                    r.req_ord,
                    r.buck_id,
                    o.obj_id,
                    m.event_type,
                    o.payload_json,
                    COUNT(*) OVER (PARTITION BY r.req_ord) AS total_count,
                    ROW_NUMBER() OVER (PARTITION BY r.req_ord ORDER BY o.obj_id ASC) AS row_num
                FROM requested r
                JOIN big_sync_members m
                  ON m.scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        query
            .push(" AND m.maybe_part_ref = (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(self.core.scope_id);
        query.push(" AND part_id = ");
        query.push_bind(Self::part_blob(req.part_id));
        query.push(") JOIN big_sync_buckets s ON s.scope_id = m.scope_id AND s.part_ref = m.maybe_part_ref AND s.buck_id = r.buck_id AND s.changed_at > ");
        query.push_bind(i64::try_from(req.since).expect(ERROR_IMPOSSIBLE));
        query.push(" AND o.obj_id >= r.lower_id");
        query.push(" AND (r.upper_id IS NULL OR o.obj_id < r.upper_id)");
        query.push(" AND (r.after_id IS NULL OR o.obj_id > r.after_id)");
        query.push(
            "
                JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
            )
            SELECT req_ord, buck_id, obj_id, event_type, payload_json, total_count
            FROM ranked
            WHERE row_num <= ",
        );
        query.push_bind(i64::from(req.limit_hint.max(1)));
        query.push(" ORDER BY req_ord, obj_id ASC");

        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;
        let mut pages: Vec<_> = req
            .buckets
            .iter()
            .map(|buck_req| LeafBucketPageBuilder {
                buck_id: buck_req.buck_id,
                entries: Vec::new(),
                total_count: 0,
            })
            .collect();
        for row in rows {
            let req_ord =
                usize::try_from(row.try_get::<i64, _>("req_ord")?).expect(ERROR_IMPOSSIBLE);
            let page = pages.get_mut(req_ord).expect(ERROR_IMPOSSIBLE);
            page.total_count =
                u32::try_from(row.try_get::<i64, _>("total_count")?).expect(ERROR_IMPOSSIBLE);
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let dead = row.try_get::<i64, _>("event_type")? == EVENT_REMOVED;
            let fp = if dead {
                Fingerprint::new(
                    &req.seed,
                    &("big-sync-obj-fp-v1", obj_id, serde_json::Value::Null),
                )
            } else {
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = payload_json
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(&payload_json).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id, payload))
            };
            page.entries.push(BucketObjPageEntry { obj_id, dead, fp });
        }
        let mut bucks = HashMap::with_capacity(pages.len());
        for page in pages {
            let done =
                u32::try_from(page.entries.len()).expect(ERROR_IMPOSSIBLE) == page.total_count;
            let next_after = if done || page.entries.is_empty() {
                None
            } else {
                Some(page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id)
            };
            bucks.insert(
                page.buck_id,
                LeafBucketPage {
                    entries: page.entries,
                    next_after,
                    done,
                },
            );
        }
        Ok(Ok(LeafBucketResult {
            seed: req.seed,
            bucks,
        }))
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj_id).await?;
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let Some(payload_json) = payload_json.filter(|str| !str.is_empty()) else {
            for part_id in parts {
                let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
                sqlx::query!(
                    "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, obj_ref, part_ref)
                     VALUES (?1, ?2, ?3)",
                    self.core.scope_id,
                    obj_ref,
                    part_ref
                )
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            return Ok(());
        };
        let payload: ObjPayload = serde_json::from_str(&payload_json).wrap_err(ERROR_JSON)?;
        let mut events = Vec::new();
        let cursor = Self::next_cursor(&mut tx).await?;
        for part_id in parts {
            let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
            let old_state = self
                .core
                .load_member_state(&mut tx, part_id, obj_id)
                .await?;
            if matches!(old_state, MemberState::Live(_)) {
                continue;
            }
            sqlx::query!(
                "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
                   event_type = excluded.event_type, txid = excluded.txid",
                self.core.scope_id,
                obj_ref,
                part_ref,
                EVENT_ADDED,
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
            )
            .execute(&mut *tx)
            .await?;
            self.core
                .apply_bucket_transition(
                    &mut tx,
                    part_id,
                    obj_id,
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?1) WHERE part_ref = ?2 AND scope_id = ?3",
                i64::try_from(cursor).expect(ERROR_IMPOSSIBLE), part_ref, self.core.scope_id
            ).execute(&mut *tx).await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.core.scope_id, obj_ref, part_ref
            ).execute(&mut *tx).await?;
            events.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor,
                part_id,
                obj_id,
                payload: payload.clone(),
            }));
        }
        tx.commit().await?;
        if !events.is_empty() {
            self.publish(events).await;
        }
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let Some(obj_ref) = self.core.find_obj_ref(obj_id).await? else {
            tx.commit().await?;
            return Ok(());
        };
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
        sqlx::query!(
            "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
            self.core.scope_id, obj_ref, part_ref
        ).execute(&mut *tx).await?;
        let old_state = self
            .core
            .load_member_state(&mut tx, part_id, obj_id)
            .await?;
        let MemberState::Live(old_payload) = old_state else {
            tx.commit().await?;
            return Ok(());
        };
        let cursor = Self::next_cursor(&mut tx).await?;
        sqlx::query!(
            "INSERT INTO big_sync_members(scope_id, obj_ref, maybe_part_ref, event_type, txid)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(obj_ref, maybe_part_ref) DO UPDATE SET
               event_type = excluded.event_type, txid = excluded.txid",
            self.core.scope_id,
            obj_ref,
            part_ref,
            EVENT_REMOVED,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&mut *tx)
        .await?;
        self.core
            .apply_bucket_transition(
                &mut tx,
                part_id,
                obj_id,
                cursor,
                &MemberState::Live(old_payload),
                &MemberState::Dead,
            )
            .await?;
        sqlx::query!(
            "UPDATE big_sync_parts SET latest_cursor = MAX(latest_cursor, ?1) WHERE scope_id = ?2 AND part_ref = ?3",
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE), self.core.scope_id, part_ref
        ).execute(&mut *tx).await?;
        let live_count: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE scope_id = ?1 AND obj_ref = ?2 AND maybe_part_ref > 0 AND event_type != ?3",
            self.core.scope_id,
            obj_ref,
            EVENT_REMOVED
        )
        .fetch_one(&mut *tx)
        .await?;
        if live_count == 0 {
            sqlx::query!(
                "UPDATE big_sync_objs SET payload_json = NULL WHERE obj_ref = ?1",
                obj_ref
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.publish(vec![SubEvent::Removed(
            big_sync_core::rpc::ObjRemovedFromPart {
                cursor,
                part_id,
                obj_id,
            },
        )])
        .await;
        Ok(())
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        let cursor: Option<i64> = sqlx::query_scalar!(
            "SELECT cursor
             FROM big_sync_peer_cursors
             WHERE scope_id = ?1 AND peer_id = ?2 AND part_ref = (
                 SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?3
             )",
            self.core.scope_id,
            Self::peer_blob(peer_id),
            Self::part_blob(part_id)
        )
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        Ok(cursor
            .map(|cursor| u64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .unwrap_or_default())
    }

    async fn set_peer_part_cursor(
        &self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> Res<()> {
        let mut tx = self.core.sql.write_pool.begin().await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part_id).await?;
        sqlx::query!(
            "INSERT INTO big_sync_peer_cursors(scope_id, peer_id, part_ref, cursor)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(scope_id, peer_id, part_ref) DO UPDATE SET cursor = MAX(cursor, excluded.cursor)",
            self.core.scope_id,
            Self::peer_blob(peer_id),
            part_ref,
            i64::try_from(cursor).expect(ERROR_IMPOSSIBLE)
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
        subscriber: PeerId,
    ) -> Res<Result<mpsc::Receiver<SubEvent>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;
        let parts: HashSet<PartId> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, .. } => Some(*part_id),
                SubscriptionTarget::Object { .. } => None,
            })
            .collect();
        let objects: HashSet<ObjId> = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id } => Some(*obj_id),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect();
        if let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let (tx, rx) = mpsc::unbounded("SqlitePartStore".into(), "caller".into());
        let sub_id = Uuid::new_v4();
        let sub = Arc::new(SqliteSubscription {
            sender: tx.clone(),
            principal: subscriber,
            pending: PendingSubscription::new(),
        });
        {
            let mut bus = self.bus.write().expect(ERROR_MUTEX);
            bus.pending.insert(sub_id);
            bus.subs.insert(sub_id, Arc::clone(&sub));
            bus.parts_by_sub.insert(sub_id, parts.clone());
            for part in &parts {
                bus.by_part.entry(*part).or_default().insert(sub_id);
            }
            bus.objs_by_sub.insert(sub_id, objects.clone());
            for obj in &objects {
                bus.by_obj.entry(*obj).or_default().insert(sub_id);
            }
        }
        let store = self.clone();
        tokio::spawn(async move {
            let mut cursor = reqs.lower_bound;
            loop {
                sub.pending
                    .state
                    .store(SUB_REPLAYING_CLEAN, std::sync::atomic::Ordering::Release);
                let mut candidates = store
                    .replay_candidates(&parts, &objects, cursor, None, Some(256))
                    .await
                    .expect(ERROR_IMPOSSIBLE);
                if candidates.is_empty() {
                    if sub.pending.begin_finalization() {
                        let mut bus = store.bus.write().expect(ERROR_MUTEX);
                        if sub.pending.state.load(std::sync::atomic::Ordering::Acquire)
                            != SUB_FINALIZING
                        {
                            continue;
                        }
                        if tx.try_send(SubEvent::ReplayComplete).is_err() {
                            bus.remove(sub_id);
                            return;
                        }
                        assert!(
                            sub.pending.become_ready(),
                            "subscription finalization state changed while bus was locked"
                        );
                        if bus.pending.remove(&sub_id) {
                            bus.live.insert(sub_id);
                        }
                        return;
                    }
                    continue;
                }
                let last_txid = candidates.last().expect(ERROR_IMPOSSIBLE).txid;
                let boundary = store
                    .replay_candidates(&parts, &objects, cursor, Some(last_txid), None)
                    .await
                    .expect(ERROR_IMPOSSIBLE);
                candidates.extend(boundary);
                candidates
                    .sort_by_key(|candidate| (candidate.txid, candidate.obj_id, candidate.part_id));
                candidates.dedup_by(|aa, bb| {
                    aa.txid == bb.txid
                        && aa.obj_id == bb.obj_id
                        && aa.part_id == bb.part_id
                        && aa.event_type == bb.event_type
                });
                let mut output: Vec<SubEvent> = Vec::new();
                let _has_object_target = !objects.is_empty();
                for candidate in candidates {
                    let part_match = candidate.part_id.filter(|part| parts.contains(part));
                    let object_match = objects.contains(&candidate.obj_id);
                    if !object_match && part_match.is_none() {
                        continue;
                    }
                    let permitted = HostPartStore::is_event_permitted(
                        &store,
                        part_match,
                        candidate.obj_id,
                        Some(subscriber),
                    )
                    .await
                    .expect(ERROR_IMPOSSIBLE);
                    if !permitted {
                        continue;
                    }
                    match candidate.event_type {
                        EVENT_CHANGED => {
                            let entry = output.iter_mut().find_map(|event| match event {
                                SubEvent::Changed(changed)
                                    if changed.cursor == candidate.txid
                                        && changed.obj_id == candidate.obj_id =>
                                {
                                    Some(changed)
                                }
                                _ => None,
                            });
                            if let Some(changed) = entry {
                                if let Some(part) = part_match
                                    && !changed.part_ids.contains(&part)
                                {
                                    changed.part_ids.push(part);
                                    changed.part_ids.sort_unstable();
                                }
                            } else {
                                output.push(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                                    cursor: candidate.txid,
                                    part_ids: part_match.into_iter().collect(),
                                    obj_id: candidate.obj_id,
                                    payload: candidate.payload,
                                }));
                            }
                        }
                        EVENT_ADDED if object_match && part_match.is_none() => {
                            output.push(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                                cursor: candidate.txid,
                                part_ids: Vec::new(),
                                obj_id: candidate.obj_id,
                                payload: candidate.payload,
                            }))
                        }
                        EVENT_ADDED if part_match.is_some() => {
                            output.push(SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                                cursor: candidate.txid,
                                part_id: part_match.expect(ERROR_IMPOSSIBLE),
                                obj_id: candidate.obj_id,
                                payload: candidate.payload,
                            }))
                        }
                        EVENT_REMOVED if part_match.is_some() => {
                            output.push(SubEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                                cursor: candidate.txid,
                                part_id: part_match.expect(ERROR_IMPOSSIBLE),
                                obj_id: candidate.obj_id,
                            }))
                        }
                        _ => {}
                    }
                }
                for event in output {
                    if tx.send(event).await.is_err() {
                        store.bus.write().expect(ERROR_MUTEX).remove(sub_id);
                        return;
                    }
                }
                cursor = last_txid;
            }
        });
        Ok(Ok(rx))
    }

    async fn ensure_part(&self, part_id: PartId) -> Res<()> {
        let mut tx = self.core.sql.write_pool.begin().await?;
        self.core.ensure_part_ref(&mut tx, part_id).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_obj_members(
        &self,
        obj: ObjId,
        agents: HashMap<PeerId, keyhive_core::access::Access>,
    ) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj).await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2",
            self.core.scope_id,
            obj_ref
        )
        .execute(&mut *tx)
        .await?;
        for (principal, access) in &agents {
            sqlx::query!(
                "INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level)
                 VALUES (?1, ?2, ?3, ?4)",
                self.core.scope_id,
                obj_ref,
                Self::peer_blob(*principal),
                encode_access(access)
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn add_obj_member(
        &self,
        obj: ObjId,
        member: PeerId,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let obj_ref = self.core.ensure_obj_ref(&mut tx, obj).await?;
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2 AND principal_id = ?3",
            self.core.scope_id, obj_ref, Self::peer_blob(member)
        ).execute(&mut *tx).await?;
        sqlx::query!(
            "INSERT INTO big_sync_syncable(scope_id, obj_ref, principal_id, access_level)
             VALUES (?1, ?2, ?3, ?4)",
            self.core.scope_id,
            obj_ref,
            Self::peer_blob(member),
            encode_access(&access)
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_obj_member(&self, obj: ObjId, member: PeerId) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let Some(obj_ref) = self.core.find_obj_ref(obj).await? else {
            tx.commit().await?;
            return Ok(());
        };
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND obj_ref = ?2 AND principal_id = ?3",
            self.core.scope_id, obj_ref, Self::peer_blob(member)
        ).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn is_event_permitted(
        &self,
        part_id: Option<PartId>,
        obj_id: ObjId,
        principal: Option<PeerId>,
    ) -> Res<bool> {
        event_permitted(&self.core, part_id, obj_id, principal).await
    }
}

/// Policy check for delivering an event to a subscriber. Shared by the
/// immediate publish path and the debounced flush task (which holds only the
/// core, not the whole store).
async fn event_permitted(
    core: &SqliteCore,
    part_id: Option<PartId>,
    obj_id: ObjId,
    principal: Option<PeerId>,
) -> Res<bool> {
    let Some(peer) = principal else {
        return Ok(true);
    };
    let peer_blob = SqliteCore::peer_blob(peer);
    let access_level: Option<i64> = sqlx::query_scalar!(
        "SELECT access_level
         FROM big_sync_syncable
         WHERE scope_id = ?1 AND obj_ref = (
             SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
         ) AND principal_id = ?3",
        core.scope_id,
        SqliteCore::obj_blob(obj_id),
        &peer_blob
    )
    .fetch_optional(&core.sql.read_pool)
    .await?;
    let permitted = access_level
        .map(|lvl| u8::try_from(lvl).expect(ERROR_IMPOSSIBLE))
        .map(super::sqlite_core::decode_access)
        .is_some_and(|access| access.is_fetcher());
    tracing::trace!(
        ?part_id,
        ?obj_id,
        ?principal,
        permitted,
        "policy event permission",
    );
    Ok(permitted)
}

#[cfg(test)]
impl PartStoreReadOnly<Sendable> for SqlitePartStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        Sendable::from_future(async move {
            HostPartStore::member_count(self, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        Sendable::from_future(async move {
            HostPartStore::obj_payload(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn get_bucket_summary<'a>(
        &'a self,
        part_id: PartId,
        id: BuckId,
    ) -> BoxFuture<'a, BucketSummary> {
        Sendable::from_future(async move {
            HostPartStore::get_bucket_summary(self, part_id, id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        Sendable::from_future(async move {
            HostPartStore::obj_parts(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
    ) -> BoxFuture<'a, CursorIndex> {
        Sendable::from_future(async move {
            HostPartStore::get_peer_part_cursor(self, peer_id, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }
}

#[cfg(test)]
impl big_sync_core::part_store::PartStore<Sendable> for SqlitePartStore {
    fn upsert_obj<'a>(&'a self, obj_id: ObjId, payload: &ObjPayload) -> BoxFuture<'a, ()> {
        let payload = payload.clone();
        Sendable::from_future(async move {
            HostPartStore::set_obj_payload(self, obj_id, payload)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartStore::add_obj_to_parts(self, obj_id, parts)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartStore::remove_obj_from_part(self, obj_id, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartStore::set_peer_part_cursor(self, peer_id, part_id, cursor)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }
}

#[cfg(test)]
#[async_trait]
impl ObservedStore for SqlitePartStore {
    async fn observed_snapshot(&self) -> Res<ObservedStoreSnapshot> {
        let rows = sqlx::query!(
            "SELECT objs.obj_id, parts.part_id, objs.payload_json
             FROM big_sync_members members
             JOIN big_sync_objs objs ON objs.obj_ref = members.obj_ref
             JOIN big_sync_parts parts ON parts.part_ref = members.maybe_part_ref
             WHERE members.scope_id = ?1 AND members.maybe_part_ref > 0
               AND members.event_type != ?2
             ORDER BY objs.obj_id ASC, parts.part_id ASC",
            self.core.scope_id,
            EVENT_REMOVED
        )
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        let mut objs = std::collections::BTreeMap::new();
        for row in rows {
            let obj_id = Self::obj_from_blob(row.obj_id);
            let part_id = Self::part_from_blob(row.part_id);
            let payload_json: Option<String> = row.payload_json;
            let payload = payload_json
                .as_deref()
                .filter(|payload| !payload.is_empty())
                .map(|payload| serde_json::from_str(payload).wrap_err(ERROR_JSON))
                .transpose()?;
            let entry = objs.entry(obj_id).or_insert_with(|| ObservedObjSnapshot {
                payload: payload.clone(),
                parts: std::collections::BTreeSet::new(),
            });
            if entry.payload.is_none() {
                entry.payload = payload;
            }
            entry.parts.insert(part_id);
        }
        let cursors = sqlx::query!(
            "SELECT peer_id, parts.part_id, cursor
             FROM big_sync_peer_cursors
             JOIN big_sync_parts parts ON parts.part_ref = big_sync_peer_cursors.part_ref
             WHERE big_sync_peer_cursors.scope_id = ?1",
            self.core.scope_id
        )
        .fetch_all(&self.core.sql.read_pool)
        .await?;
        let mut peer_part_cursors = std::collections::BTreeMap::new();
        for row in cursors {
            peer_part_cursors.insert(
                (
                    Self::peer_from_blob(row.peer_id),
                    Self::part_from_blob(row.part_id),
                ),
                u64::try_from(row.cursor).expect(ERROR_IMPOSSIBLE),
            );
        }
        Ok(ObservedStoreSnapshot {
            objs,
            peer_part_cursors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::part_store::host_contract::{self, HostPartStoreContractHarness};
    use big_sync_core::part_store::contract;

    async fn test_sql() -> Res<SqlCtx> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let sqlite_url = format!("sqlite://{}", db_path.display());
        SqlCtx::url(&sqlite_url).await
    }

    async fn test_store(scope_key: &str) -> Res<SqlitePartStore> {
        let sql = test_sql().await?;
        SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL).await
    }

    fn test_part_id(seed: u8) -> PartId {
        PartId(Byte32Id::new([seed; 32]))
    }

    fn test_obj_id(seed: u8) -> ObjId {
        ObjId(Byte32Id::new([seed; 32]))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_membership_semantics() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(1);
        let obj_id = test_obj_id(2);
        contract::assert_membership_semantics(&store, part_id, obj_id).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_add_obj_to_parts_is_idempotent() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(3);
        let obj_id = test_obj_id(4);
        contract::assert_add_obj_to_parts_is_idempotent(&store, part_id, obj_id).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_peer_cursor_roundtrip() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(5);
        contract::assert_peer_cursor_roundtrip(&store, PeerId(Byte32Id::new([42; 32])), part_id)
            .await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_root_bucket_contract() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(6);
        let seed = big_sync_core::FingerprintSeed::new(1, 2);

        let mut obj_ids = Vec::new();
        for ii in 0..5u8 {
            let obj_id = test_obj_id(10 + ii);
            HostPartStore::set_obj_payload(
                &store,
                obj_id,
                serde_json::json!({"phase": "present", "ii": ii}),
            )
            .await?;
            HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;
            obj_ids.push(obj_id);
        }

        crate::part_store::contract::assert_root_bucket_contract(
            &store,
            part_id,
            seed,
            &obj_ids,
            &[],
            2,
        )
        .await?;

        let removed_obj_id = obj_ids[1];
        HostPartStore::remove_obj_from_part(&store, removed_obj_id, part_id).await?;
        let live_ids: Vec<_> = obj_ids
            .iter()
            .copied()
            .filter(|obj_id| *obj_id != removed_obj_id)
            .collect();
        crate::part_store::contract::assert_root_bucket_contract(
            &store,
            part_id,
            seed,
            &live_ids,
            &[removed_obj_id],
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_list_events_derives_latest_effective_transition() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(7);
        let obj_id = test_obj_id(8);

        HostPartStore::set_obj_payload(&store, obj_id, serde_json::json!({"phase": "created"}))
            .await?;
        HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;
        HostPartStore::remove_obj_from_part(&store, obj_id, part_id).await?;

        let deleted_page = HostPartStore::list_events(&store, HashSet::from([part_id]), 0, 10)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        let deleted_events = &deleted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(deleted_events.len(), 1);
        let PartEvent::Removed(transition) = &deleted_events[0] else {
            panic!("expected removed event");
        };
        assert_eq!(transition.cursor, 3);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);

        HostPartStore::set_obj_payload(&store, obj_id, serde_json::json!({"phase": "recreated"}))
            .await?;
        HostPartStore::add_obj_to_parts(&store, obj_id, vec![part_id]).await?;

        let upserted_page = HostPartStore::list_events(&store, HashSet::from([part_id]), 0, 10)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        let upserted_events = &upserted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(upserted_events.len(), 1);
        let PartEvent::Added(second_added) = &upserted_events[0] else {
            panic!("expected second added event");
        };
        assert_eq!(second_added.cursor, 5);
        assert_eq!(second_added.part_id, part_id);
        assert_eq!(second_added.obj_id, obj_id);
        assert_eq!(
            second_added.payload,
            serde_json::json!({"phase": "recreated"})
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_scopes_are_isolated_by_scope_key() -> Res<()> {
        let store_a = test_store("big-sync-sqlite-test://repo").await?;
        let store_b = SqlitePartStore::new(
            store_a.core.sql.clone(),
            "big-sync-sqlite-test://other-repo",
            BuckId::MAX_LEVEL,
        )
        .await?;
        let part_id = test_part_id(9);
        let obj_id = test_obj_id(10);

        HostPartStore::set_obj_payload(&store_a, obj_id, serde_json::json!({"scope": "a"})).await?;
        HostPartStore::add_obj_to_parts(&store_a, obj_id, vec![part_id]).await?;
        HostPartStore::set_obj_payload(&store_b, obj_id, serde_json::json!({"scope": "b"})).await?;
        HostPartStore::add_obj_to_parts(&store_b, obj_id, vec![part_id]).await?;

        assert_eq!(
            HostPartStore::obj_payload(&store_a, obj_id).await?,
            Some(serde_json::json!({"scope": "a"}))
        );
        assert_eq!(
            HostPartStore::obj_payload(&store_b, obj_id).await?,
            Some(serde_json::json!({"scope": "b"}))
        );
        assert_eq!(HostPartStore::member_count(&store_a, part_id).await?, 1);
        assert_eq!(HostPartStore::member_count(&store_b, part_id).await?, 1);
        Ok(())
    }

    struct SqliteHostHarness {
        store: SqlitePartStore,
    }

    #[async_trait]
    impl HostPartStoreContractHarness for SqliteHostHarness {
        fn store(&self) -> &dyn HostPartStore {
            &self.store
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_host_part_store_contract() -> Res<()> {
        let harness = SqliteHostHarness {
            store: test_store("big-sync-sqlite-test://host-contract").await?,
        };
        host_contract::assert_host_part_store_contract(&harness).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_membership_cache_rehydrates_after_restart() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let sql = test_sql().await?;
        let scope_key = "big-sync-sqlite-test://membership-restart";

        // ---- first session ----
        let store1 = SqlitePartStore::new(sql.clone(), scope_key, BuckId::MAX_LEVEL).await?;
        let part = PartId(Byte32Id::new([201u8; 32]));
        let obj = ObjId(Byte32Id::new([202u8; 32]));
        let auth = PeerId::new([203u8; 32]);
        let denied = PeerId::new([204u8; 32]);

        store1.ensure_part(part).await?;
        store1
            .set_obj_payload(obj, serde_json::json!("first"))
            .await?;
        store1.add_obj_to_parts(obj, vec![part]).await?;

        // Persist membership.
        store1
            .set_obj_members(obj, std::collections::HashMap::from([(auth, Access::Read)]))
            .await?;

        // Helper to drain through ReplayComplete.
        async fn drain_through_replay(rx: &mpsc::Receiver<SubEvent>) -> Res<()> {
            loop {
                match timeout(Duration::from_secs(5), rx.recv()).await? {
                    Ok(SubEvent::ReplayComplete) => return Ok(()),
                    Ok(_) => continue,
                    Err(_) => {
                        eyre::bail!("sub channel closed during replay");
                    }
                }
            }
        }

        let sub = |peer: PeerId| {
            let store = &store1;
            let part = &part;
            async move {
                store
                    .subscribe(
                        SubPartsRequest {
                            lower_bound: 0,
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: *part,
                                    cursor: 0,
                                },
                            ]),
                        },
                        peer,
                    )
                    .await?
                    .map_err(eyre::Report::from)
            }
        };
        let auth_rx1 = sub(auth).await?;
        let denied_rx1 = sub(denied).await?;
        drain_through_replay(&auth_rx1).await?;
        drain_through_replay(&denied_rx1).await?;

        // Live mutation: authorized should receive, denied should not.
        store1
            .set_obj_payload(obj, serde_json::json!("second"))
            .await?;
        auth_rx1
            .recv()
            .await
            .expect("authorized must receive live event in first session");
        match timeout(
            utils_rs::scale_timeout(Duration::from_millis(500)),
            denied_rx1.recv(),
        )
        .await
        {
            Err(_elapsed) => {} /* expected */
            Ok(Ok(evt)) => {
                panic!("denied must not receive live event in first session; got {evt:?}");
            }
            Ok(Err(_)) => {
                panic!("denied channel closed unexpectedly in first session");
            }
        }

        // Drop store1 to simulate restart.
        drop(store1);

        // ---- second session on the same database and scope ----
        let store2 = SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL).await?;

        let sub2 = |peer: PeerId| {
            let store = &store2;
            let part = &part;
            async move {
                store
                    .subscribe(
                        SubPartsRequest {
                            lower_bound: 0,
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: *part,
                                    cursor: 0,
                                },
                            ]),
                        },
                        peer,
                    )
                    .await?
                    .map_err(eyre::Report::from)
            }
        };
        let auth_rx2 = sub2(auth).await?;
        let denied_rx2 = sub2(denied).await?;
        drain_through_replay(&auth_rx2).await?;
        drain_through_replay(&denied_rx2).await?;
        // Live mutation after restart.
        store2
            .set_obj_payload(obj, serde_json::json!("third"))
            .await?;
        auth_rx2
            .recv()
            .await
            .expect("authorized must receive live event after restart");
        // Denied must still be denied after restart (cache must be rehydrated).
        match timeout(
            utils_rs::scale_timeout(Duration::from_millis(500)),
            denied_rx2.recv(),
        )
        .await
        {
            Err(_elapsed) => {} /* expected: cache correctly rehydrated */
            Ok(Ok(evt)) => {
                panic!(
                    "denied subscriber received live event after restart; cache was NOT rehydrated, got {evt:?}"
                );
            }
            Ok(Err(_)) => {
                panic!("denied channel closed unexpectedly after restart");
            }
        }

        Ok(())
    }

    /// A burst of state changes to the same (subscriber, target) collapses
    /// into a single batcher entry carrying the newest payload.
    #[test]
    fn debounced_burst_merges_to_latest_wins() {
        let debouncer = LiveDebouncer::new(utils_rs::batching::DebouncePolicy {
            quiet_window: Duration::from_millis(50),
            max_latency: Duration::from_millis(500),
        });
        let key = (
            Uuid::new_v4(),
            DebounceTarget::Part(test_part_id(205), test_obj_id(206)),
        );
        let mut batcher = debouncer.batcher.lock().expect(ERROR_MUTEX);
        let now = std::time::Instant::now();
        for ii in 1..=10u8 {
            batcher.push(
                now,
                key,
                SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor: ii as u64,
                    part_ids: vec![test_part_id(205)],
                    obj_id: test_obj_id(206),
                    payload: serde_json::json!({"phase": ii}),
                }),
            );
        }
        assert_eq!(batcher.len(), 1, "burst must collapse to one entry");
        let due = batcher.take_due(now + Duration::from_secs(1));
        assert_eq!(due.len(), 1, "exactly one delivery");
        let (due_key, event) = &due[0];
        assert_eq!(due_key, &key);
        match event {
            SubEvent::Changed(inner) => {
                assert_eq!(inner.payload["phase"], serde_json::json!(10), "latest wins");
                assert_eq!(inner.cursor, 10);
            }
            other => panic!("expected Changed, got {other:?}"),
        }
        assert!(batcher.is_empty(), "delivery drains the batcher");
    }

    #[test]
    fn reduce_sub_event_added_then_changed_retains_added() {
        let part = test_part_id(205);
        let obj = test_obj_id(206);
        let mut evt = SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
            cursor: 1,
            part_id: part,
            obj_id: obj,
            payload: serde_json::json!({"v": 1}),
        });
        reduce_sub_event(
            &mut evt,
            SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor: 2,
                part_ids: vec![part],
                obj_id: obj,
                payload: serde_json::json!({"v": 2}),
            }),
        );
        match evt {
            SubEvent::Added(inner) => {
                assert_eq!(inner.cursor, 2);
                assert_eq!(inner.payload, serde_json::json!({"v": 2}));
            }
            other => panic!("expected Added to be retained, got {other:?}"),
        }
    }

    #[test]
    fn reduce_sub_event_different_partitions_remain_separate() {
        let debouncer = LiveDebouncer::new(utils_rs::batching::DebouncePolicy {
            quiet_window: Duration::from_millis(50),
            max_latency: Duration::from_millis(500),
        });
        let sub = Uuid::new_v4();
        let part1 = test_part_id(1);
        let part2 = test_part_id(2);
        let obj = test_obj_id(10);
        let mut batcher = debouncer.batcher.lock().expect(ERROR_MUTEX);
        let now = std::time::Instant::now();
        batcher.push(
            now,
            (sub, DebounceTarget::Part(part1, obj)),
            SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor: 1,
                part_id: part1,
                obj_id: obj,
                payload: serde_json::json!({"v": 1}),
            }),
        );
        batcher.push(
            now,
            (sub, DebounceTarget::Part(part2, obj)),
            SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                cursor: 2,
                part_id: part2,
                obj_id: obj,
                payload: serde_json::json!({"v": 2}),
            }),
        );
        assert_eq!(batcher.len(), 2, "different partitions must not collapse");
    }

    /// End-to-end: a live subscriber receives debounced delivery for object
    /// state changes and removals.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_state_event_debounced_delivery() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let store = test_store("big-sync-sqlite-test://debounce").await?;
        let part = PartId(Byte32Id::new([205u8; 32]));
        let obj = ObjId(Byte32Id::new([206u8; 32]));
        let peer = PeerId::new([207u8; 32]);
        store.ensure_part(part).await?;
        store
            .set_obj_payload(obj, serde_json::json!({"phase": 0}))
            .await?;
        store.add_obj_to_parts(obj, vec![part]).await?;
        store
            .set_obj_members(obj, std::collections::HashMap::from([(peer, Access::Read)]))
            .await?;

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
            .await?
            .map_err(eyre::Report::from)?;
        // Drain through ReplayComplete.
        loop {
            match timeout(Duration::from_secs(5), rx.recv()).await? {
                Ok(SubEvent::ReplayComplete) => break,
                Ok(_) => continue,
                Err(_) => {
                    eyre::bail!("sub channel closed during replay");
                }
            }
        }

        // A state change is delivered through the debouncer (flush task)
        // within the max-latency bound.
        store
            .set_obj_payload(obj, serde_json::json!({"phase": 1}))
            .await?;
        let first = timeout(utils_rs::scale_timeout(Duration::from_secs(5)), rx.recv())
            .await
            .expect("debounced delivery must arrive")
            .expect("channel must stay open");
        match first {
            SubEvent::Changed(inner) => {
                assert_eq!(inner.obj_id, obj);
                assert_eq!(inner.payload["phase"], serde_json::json!(1));
            }
            other => panic!("expected Changed, got {other:?}"),
        }

        // A removal is delivered through the subscriber channel.
        store.remove_obj_from_part(obj, part).await?;
        let removed = timeout(utils_rs::scale_timeout(Duration::from_secs(5)), rx.recv())
            .await
            .expect("Removed must be delivered")
            .expect("channel must stay open");
        assert!(matches!(removed, SubEvent::Removed(_)));

        Ok(())
    }

    /// End-to-end: Added followed by Changed before flush merges into Added
    /// with the latest payload and cursor.
    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_debounce_added_then_changed_retains_added() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let store = test_store("big-sync-sqlite-test://e2e-add-change").await?;
        let part = PartId(Byte32Id::new([210u8; 32]));
        let obj = ObjId(Byte32Id::new([211u8; 32]));
        let peer = PeerId::new([212u8; 32]);
        store.ensure_part(part).await?;
        store
            .set_obj_members(obj, std::collections::HashMap::from([(peer, Access::Read)]))
            .await?;

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
            .await?
            .map_err(eyre::Report::from)?;
        loop {
            match timeout(Duration::from_secs(5), rx.recv()).await? {
                Ok(SubEvent::ReplayComplete) => break,
                Ok(_) => continue,
                Err(_) => {
                    eyre::bail!("sub channel closed during replay");
                }
            }
        }

        // Publish Added followed by Changed within the debounce window
        store
            .publish(vec![
                SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                    cursor: 1,
                    part_id: part,
                    obj_id: obj,
                    payload: serde_json::json!({"v": 1}),
                }),
                SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor: 2,
                    part_ids: vec![part],
                    obj_id: obj,
                    payload: serde_json::json!({"v": 2}),
                }),
            ])
            .await;

        let evt = rx.recv().await.expect("channel stay open");

        match evt {
            SubEvent::Added(inner) => {
                assert_eq!(inner.part_id, part);
                assert_eq!(inner.obj_id, obj);
                assert_eq!(inner.payload, serde_json::json!({"v": 2}));
                assert_eq!(inner.cursor, 2);
            }
            other => panic!("expected Added with latest payload, got {other:?}"),
        }

        // No stray Changed should follow
        assert!(
            timeout(Duration::from_millis(150), rx.recv())
                .await
                .is_err(),
            "Added must not be followed by duplicate Changed",
        );

        Ok(())
    }

    /// End-to-end: Changed followed by Removed before flush delivers Removed
    /// and subsumes/cancels the pending Changed.
    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_debounce_changed_then_removed_delivers_removed() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let store = test_store("big-sync-sqlite-test://e2e-change-remove").await?;
        let part = PartId(Byte32Id::new([220u8; 32]));
        let obj = ObjId(Byte32Id::new([221u8; 32]));
        let peer = PeerId::new([222u8; 32]);
        store.ensure_part(part).await?;
        store
            .set_obj_members(obj, std::collections::HashMap::from([(peer, Access::Read)]))
            .await?;

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
            .await?
            .map_err(eyre::Report::from)?;
        loop {
            match timeout(Duration::from_secs(5), rx.recv()).await? {
                Ok(SubEvent::ReplayComplete) => break,
                Ok(_) => continue,
                Err(_) => {
                    eyre::bail!("sub channel closed during replay");
                }
            }
        }

        // Publish Changed followed by Removed within the debounce window
        store
            .publish(vec![
                SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor: 1,
                    part_ids: vec![part],
                    obj_id: obj,
                    payload: serde_json::json!({"v": 2}),
                }),
                SubEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                    cursor: 2,
                    part_id: part,
                    obj_id: obj,
                }),
            ])
            .await;

        let evt = rx.recv().await.expect("channel stay open");

        match evt {
            SubEvent::Removed(inner) => {
                assert_eq!(inner.part_id, part);
                assert_eq!(inner.obj_id, obj);
                assert_eq!(inner.cursor, 2);
            }
            other => panic!("expected Removed, got {other:?}"),
        }

        // Ensure no stale Changed arrives after Removed
        assert!(
            timeout(Duration::from_millis(150), rx.recv())
                .await
                .is_err(),
            "no stale Changed must arrive after Removed",
        );

        Ok(())
    }

    /// End-to-end: Removed followed by Changed retains Removed (stale Changed ignored).
    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_debounce_removed_then_changed_retains_removed() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let store = test_store("big-sync-sqlite-test://e2e-remove-change").await?;
        let part = PartId(Byte32Id::new([230u8; 32]));
        let obj = ObjId(Byte32Id::new([231u8; 32]));
        let peer = PeerId::new([232u8; 32]);
        store.ensure_part(part).await?;
        store
            .set_obj_members(obj, std::collections::HashMap::from([(peer, Access::Read)]))
            .await?;

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
            .await?
            .map_err(eyre::Report::from)?;
        loop {
            match timeout(Duration::from_secs(5), rx.recv()).await? {
                Ok(SubEvent::ReplayComplete) => break,
                Ok(_) => continue,
                Err(_) => {
                    eyre::bail!("sub channel closed during replay");
                }
            }
        }

        // Publish Removed followed by Changed within the debounce window
        store
            .publish(vec![
                SubEvent::Removed(big_sync_core::rpc::ObjRemovedFromPart {
                    cursor: 1,
                    part_id: part,
                    obj_id: obj,
                }),
                SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor: 2,
                    part_ids: vec![part],
                    obj_id: obj,
                    payload: serde_json::json!({"v": 2}),
                }),
            ])
            .await;

        let evt = rx.recv().await.expect("event must arrive");

        match evt {
            SubEvent::Removed(inner) => {
                assert_eq!(inner.part_id, part);
                assert_eq!(inner.obj_id, obj);
                assert_eq!(inner.cursor, 2);
            }
            other => panic!("expected Removed, got {other:?}"),
        }

        assert!(
            timeout(
                utils_rs::scale_timeout(Duration::from_millis(150)),
                rx.recv()
            )
            .await
            .is_err(),
            "no stale Changed after Removed",
        );

        Ok(())
    }

    /// End-to-end: One subscriber observing the same object through two partitions.
    #[tokio::test(flavor = "multi_thread")]
    async fn e2e_debounce_one_subscriber_two_partitions() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let store = test_store("big-sync-sqlite-test://e2e-multi-part").await?;
        let part1 = PartId(Byte32Id::new([240u8; 32]));
        let part2 = PartId(Byte32Id::new([241u8; 32]));
        let obj = ObjId(Byte32Id::new([242u8; 32]));
        let peer = PeerId::new([243u8; 32]);
        store.ensure_part(part1).await?;
        store.ensure_part(part2).await?;
        store
            .set_obj_members(obj, std::collections::HashMap::from([(peer, Access::Read)]))
            .await?;

        let rx = store
            .subscribe(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part1,
                            cursor: 0,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part2,
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        loop {
            match timeout(Duration::from_secs(5), rx.recv()).await? {
                Ok(SubEvent::ReplayComplete) => break,
                Ok(_) => continue,
                Err(_) => {
                    eyre::bail!("sub channel closed during replay");
                }
            }
        }

        // Publish Added for both partitions + Changed for both partitions before flush
        store
            .publish(vec![
                SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                    cursor: 1,
                    part_id: part1,
                    obj_id: obj,
                    payload: serde_json::json!({"v": 1}),
                }),
                SubEvent::Added(big_sync_core::rpc::ObjAddedToPart {
                    cursor: 2,
                    part_id: part2,
                    obj_id: obj,
                    payload: serde_json::json!({"v": 1}),
                }),
                SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                    cursor: 3,
                    part_ids: vec![part1, part2],
                    obj_id: obj,
                    payload: serde_json::json!({"v": 2}),
                }),
            ])
            .await;

        let mut seen_parts = HashSet::new();
        for _ in 0..2 {
            let evt = rx.recv().await.expect("event must arrive");
            match evt {
                SubEvent::Added(inner) => {
                    assert_eq!(inner.obj_id, obj);
                    assert_eq!(inner.payload, serde_json::json!({"v": 2}));
                    assert_eq!(inner.cursor, 3);
                    seen_parts.insert(inner.part_id);
                }
                other => panic!("expected Added for each partition, got {other:?}"),
            }
        }
        assert_eq!(seen_parts, HashSet::from([part1, part2]));

        // Now publish Changed for both partitions
        store
            .publish(vec![SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor: 4,
                part_ids: vec![part1, part2],
                obj_id: obj,
                payload: serde_json::json!({"v": 3}),
            })])
            .await;

        let evt = rx.recv().await.expect("event must arrive");
        match evt {
            SubEvent::Changed(inner) => {
                assert_eq!(inner.obj_id, obj);
                assert_eq!(inner.payload, serde_json::json!({"v": 3}));
                assert_eq!(inner.cursor, 4);
                assert_eq!(
                    inner.part_ids.into_iter().collect::<HashSet<_>>(),
                    HashSet::from([part1, part2]),
                );
            }
            other => panic!("expected one Changed event for both partitions, got {other:?}"),
        }
        assert!(
            timeout(
                utils_rs::scale_timeout(Duration::from_millis(150)),
                rx.recv()
            )
            .await
            .is_err(),
            "multi-part change must not emit duplicate logical events",
        );

        Ok(())
    }
}
