use super::HostPartStore;
use super::LocalPartRevisionReader;
use super::PartStoreStats;
use super::sqlite_core::EVENT_REMOVED;
use super::{PartFrontierKey, PartScope, ReadTarget, SqlitePartFrontier, SqlitePartSelector};
use crate::interlude::*;
use crate::keyed_frontier::open_sqlite_reader;
#[cfg(test)]
use crate::test_support::{ObservedObjSnapshot, ObservedStore, ObservedStoreSnapshot};

#[cfg(test)]
use big_sync_core::ByteKey;
use big_sync_core::keyed_frontier::{
    KeyedFrontier, KeyedFrontierTransaction,
};
#[cfg(test)]
use big_sync_core::part_store::PartStoreReadOnly;
use big_sync_core::part_store::{CursorIndex, ObjPayload, PartDirtyCount};
use big_sync_core::rpc::{
    BucketObjPageEntry, BucketSummary, GetChangedBucketsRequest, LeafBucketPage, LeafBucketResult,
    LeafBucketsError, LeafBucketsRequest, ListPartsError, ObjChanged, ObjRemovedFromPart,
    PartEvent, PartPage, PartSummary, SubEvent, SubPartsRequest,
};
use big_sync_core::{BuckId, Fingerprint, ObjKey, PartKey, PeerKey};
#[cfg(test)]
use future_form::{FutureForm, Sendable};
#[cfg(test)]
use futures::future::BoxFuture;
use sqlx::{QueryBuilder, Row};
use sqlx_utils_rs::SqlCtx;
use tokio::sync::Notify;
#[cfg(test)]
use uuid::Uuid;

use super::sqlite_core::encode_access;

struct ReplayCandidate {
    txid: CursorIndex,
    obj_id: ObjKey,
    event_type: i64,
    payload: ObjPayload,
}

#[derive(Clone)]
pub struct SqlitePartStore {
    pub(crate) core: SqliteCore,
    pub(crate) frontier: SqlitePartFrontier,
    hidden_parts: Arc<HashSet<PartKey>>,
}

/// Open a durable revision reader over a set of targets against an existing
/// BigSync SQLite schema. Callers that own a different store facade can supply
/// its read pool, scope, and commit wakeup; this is the same reader the
/// responder pages from, so the remote page and the local pull consumer read
/// one path rather than two.
pub async fn open_sqlite_revision_reader(
    read_pool: sqlx::SqlitePool,
    scope_id: i64,
    changed: Arc<Notify>,
    reqs: SubPartsRequest,
) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
    use big_sync_core::rpc::SubscriptionTarget;

    let objects = reqs
        .targets
        .iter()
        .filter_map(|target| match target {
            SubscriptionTarget::Object { obj_id, .. } => Some(obj_id.clone()),
            SubscriptionTarget::Part { .. } => None,
        })
        .collect::<HashSet<_>>();
    let parts = reqs
        .targets
        .iter()
        .filter_map(|target| match target {
            SubscriptionTarget::Part { part_id, .. } => Some(part_id.clone()),
            SubscriptionTarget::Object { .. } => None,
        })
        .collect::<HashSet<_>>();
    // The reader is the faithful seam: it hands back what the log holds, tombstones
    // included. Peer-facing policy lives in the responder, which knows the requested
    // cursor and the peer, and the rev-store adapters keep their tombstones.
    let mut selector = SqlitePartSelector::default();
    for target in reqs.targets {
        match target {
            SubscriptionTarget::Part { part_id, cursor } => {
                selector
                    .parts
                    .entry(part_id)
                    .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound.max(cursor)))
                    .or_insert(reqs.lower_bound.max(cursor));
            }
            SubscriptionTarget::Object { obj_id, cursor } => {
                // An object target carries its own position, the same way a part
                // target does: a set request can name targets whose replays have
                // reached different points, so the floor cannot stand in for a
                // target's own cursor.
                let target_bound = reqs.lower_bound.max(cursor);
                selector
                    .objects
                    .entry(obj_id)
                    .and_modify(|bound| *bound = (*bound).max(target_bound))
                    .or_insert(target_bound);
            }
        }
    }
    let frontier = SqlitePartFrontier::new(read_pool.clone(), read_pool, scope_id, changed);
    let reader = open_sqlite_reader(frontier, selector).await?;
    Ok(Ok(Box::new(super::PartRevisionReader::new(
        reader, objects, parts,
    ))))
}

/// Open a durable revision reader over every part and object in the scope,
/// including parts created after this call. Callers that own a different store
/// facade can supply its read pool, scope, and commit wakeup. `after` is the
/// replay lower bound (a part-store frontier revision).
pub async fn open_sqlite_revision_reader_all(
    read_pool: sqlx::SqlitePool,
    scope_id: i64,
    changed: Arc<Notify>,
    after: CursorIndex,
) -> Res<Result<Box<dyn LocalPartRevisionReader>, ListPartsError>> {
    let frontier = SqlitePartFrontier::new(read_pool.clone(), read_pool, scope_id, changed);
    let reader = open_sqlite_reader(
        frontier,
        SqlitePartSelector {
            all: Some(after),
            ..Default::default()
        },
    )
    .await?;
    Ok(Ok(Box::new(super::PartRevisionReader::new_all(reader))))
}

use super::sqlite_core::MemberState;
use super::sqlite_core::SqliteCore;

/// Thin forwarding helpers so call sites inside SqlitePartStore's
/// HostPartStore impl continue to compile without changes.
impl SqlitePartStore {
    fn part_blob(id: PartKey) -> Vec<u8> {
        SqliteCore::part_blob(id)
    }
    fn obj_blob(id: ObjKey) -> Vec<u8> {
        SqliteCore::obj_blob(id)
    }
    fn peer_blob(id: PeerKey) -> Vec<u8> {
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
    fn part_from_blob(blob: Vec<u8>) -> PartKey {
        SqliteCore::part_from_blob(blob)
    }
    fn obj_from_blob(blob: Vec<u8>) -> ObjKey {
        SqliteCore::obj_from_blob(blob)
    }
    #[cfg(test)]
    fn peer_from_blob(blob: Vec<u8>) -> PeerKey {
        SqliteCore::peer_from_blob(blob)
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
        let changed = Arc::new(tokio::sync::Notify::new());
        let frontier = SqlitePartFrontier::new(
            core.sql.read_pool.clone(),
            core.sql.write_pool.clone(),
            core.scope_id,
            changed,
        );

        Ok(Self {
            core,
            frontier,
            hidden_parts: Arc::new(config.hidden_parts),
        })
    }
}

impl SqlitePartStore {
    async fn replay_candidates(
        &self,
        parts: &HashSet<PartKey>,
        objects: &HashSet<ObjKey>,
        lower_bound: CursorIndex,
        exact_txid: Option<CursorIndex>,
        limit: Option<u32>,
    ) -> Res<Vec<ReplayCandidate>> {
        if parts.is_empty() && objects.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT m.txid, o.obj_id, m.event_type, o.payload_json
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
                separated.push_bind(Self::part_blob(part.clone()));
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
                separated.push_bind(Self::obj_blob(obj.clone()));
            }
            separated.push_unseparated("))");
        }
        query.push(")");
        // The `added_at` predicate (ADR 012 decision 9): a tombstone at cursor `T` belongs in a
        // reader's page exactly when `added_at <= cursor < T`. A removal for a member the reader
        // never saw carries no information for it, and excluding it here keeps it from consuming
        // the page's row budget. A zero stamp is a row from before the column and stays
        // selectable. `lower_bound` is the reader's cursor in all three shapes of this query.
        query.push(" AND (m.event_type != ");
        query.push_bind(EVENT_REMOVED);
        query.push(" OR m.added_at <= ");
        query.push_bind(i64::try_from(lower_bound).expect(ERROR_IMPOSSIBLE));
        query.push(")");
        query.push(" ORDER BY m.txid, m.obj_ref, m.maybe_part_ref");
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
                Ok(ReplayCandidate {
                    txid: u64::try_from(row.try_get::<i64, _>("txid")?).expect(ERROR_IMPOSSIBLE),
                    obj_id: Self::obj_from_blob(row.try_get("obj_id")?),
                    event_type: row.try_get("event_type")?,
                    payload,
                })
            })
            .collect()
    }
}

#[async_trait]
impl HostPartStore for SqlitePartStore {
    async fn latest_revision(&self) -> Res<CursorIndex> {
        let revision: i64 =
            sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                .fetch_one(&self.core.sql.read_pool)
                .await?;
        Ok(u64::try_from(revision)?)
    }

    async fn summarize_parts(
        &self,
        parts: HashSet<PartKey>,
    ) -> Res<Result<HashMap<PartKey, PartSummary>, ListPartsError>> {
        if parts.is_empty() {
            return Ok(Ok(HashMap::new()));
        }
        let mut hidden: Vec<_> = parts.intersection(&self.hidden_parts).cloned().collect();
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
            separated.push_bind(Self::part_blob(part_id.clone()));
        }
        separated.push_unseparated(")");
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;

        if rows.len() != parts.len() {
            let found: HashSet<PartKey> = rows
                .iter()
                .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
                .collect();
            let mut missing: Vec<_> = parts.difference(&found).cloned().collect();
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
        parts: HashSet<PartKey>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartKey, PartPage>, ListPartsError>> {
        if let Err(err) = self.summarize_parts(parts.clone()).await? {
            return Ok(Err(err));
        }
        let mut out = HashMap::new();
        for part_id in parts {
            let requested_part = HashSet::from([part_id.clone()]);
            let limit_usize = usize::try_from(limit).expect(ERROR_IMPOSSIBLE);
            let (drained, candidates) = if limit_usize == 0 {
                // A zero-length page carries nothing, and may only claim caught-up
                // when nothing is waiting: the page is not a statement about the log
                // unless it filled, or was asked for at least one event. Probe for one
                // waiting candidate so a caller that reads `drained` as "nothing further
                // is waiting" cannot strand the events it never received.
                let waiting = !self
                    .replay_candidates(&requested_part, &HashSet::new(), cursor, None, Some(1))
                    .await?
                    .is_empty();
                (!waiting, Vec::new())
            } else {
                let raw = self
                    .replay_candidates(&requested_part, &HashSet::new(), cursor, None, Some(limit))
                    .await?;
                // A snapshot read knows whether its fetch came up short, so the verdict
                // is "the page did not fill": a full page may still have a successor,
                // and only a page that came up short is caught up.
                if raw.len() < limit_usize {
                    (true, raw)
                } else {
                    // The last fetched txid is the page's boundary, and every event
                    // sharing it must travel together so a transaction is not split;
                    // the page therefore ends at that whole txid.
                    let cutoff = raw.last().expect(ERROR_IMPOSSIBLE).txid;
                    let mut page = raw
                        .into_iter()
                        .filter(|candidate| candidate.txid < cutoff)
                        .collect::<Vec<_>>();
                    let boundary = self
                        .replay_candidates(
                            &requested_part,
                            &HashSet::new(),
                            cursor,
                            Some(cutoff),
                            None,
                        )
                        .await?;
                    page.extend(boundary);
                    (false, page)
                }
            };
            // Always a position to ask from again: past the last event this page
            // carried, or the caller's own cursor when it carried none.
            let resume = candidates.last().map_or(cursor, |candidate| candidate.txid);
            let events = candidates
                .iter()
                .map(|candidate| match candidate.event_type {
                    EVENT_REMOVED => PartEvent::Removed(ObjRemovedFromPart {
                        cursor: candidate.txid,
                        part_id: part_id.clone(),
                        obj_id: candidate.obj_id.clone(),
                    }),
                    _ => PartEvent::Changed(ObjChanged {
                        cursor: candidate.txid,
                        part_ids: vec![part_id.clone()],
                        obj_id: candidate.obj_id.clone(),
                        payload: candidate.payload.clone(),
                    }),
                })
                .collect();
            out.insert(
                part_id,
                PartPage {
                    events,
                    resume,
                    drained,
                },
            );
        }
        Ok(Ok(out))
    }

    async fn member_count(&self, part_id: PartKey) -> Res<u64> {
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

    async fn part_dirty_count(
        &self,
        part_id: PartKey,
        principal: Option<PeerKey>,
        since: CursorIndex,
    ) -> Res<PartDirtyCount> {
        let Some(part_ref) = self.core.find_part_ref(part_id).await? else {
            // A part this scope has never seen has no members to be behind on and no
            // row authorizing the principal.
            return Ok(PartDirtyCount::default());
        };
        let since = i64::try_from(since).expect(ERROR_IMPOSSIBLE);
        // A member row carries the cursor of its last transition, removal included,
        // so a removal counts as a relevant change.
        let member_changes: i64 = sqlx::query_scalar!(
            "SELECT COUNT(*)
               FROM big_sync_members
              WHERE scope_id = ?1
                AND maybe_part_ref = ?2
                AND txid > ?3",
            self.core.scope_id,
            part_ref,
            since
        )
        .fetch_one(&self.core.sql.read_pool)
        .await?;
        // One row per (part, principal). A revocation deletes that row, so a
        // revocation does not count here. `None` is the local principal, which
        // access rows do not gate, so it has no access half.
        let access_changes: i64 = match principal {
            Some(principal) => {
                sqlx::query_scalar!(
                    "SELECT COUNT(*)
                       FROM big_sync_syncable
                      WHERE scope_id = ?1
                        AND part_ref = ?2
                        AND principal_id = ?3
                        AND changed_at > ?4",
                    self.core.scope_id,
                    part_ref,
                    Self::peer_blob(principal),
                    since
                )
                .fetch_one(&self.core.sql.read_pool)
                .await?
            }
            None => 0,
        };
        Ok(PartDirtyCount {
            member_changes: u64::try_from(member_changes).expect(ERROR_IMPOSSIBLE),
            access_changes: u64::try_from(access_changes).expect(ERROR_IMPOSSIBLE),
        })
    }

    async fn obj_payload(&self, obj_id: ObjKey) -> Res<Option<ObjPayload>> {
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

    async fn set_obj_payload(&self, obj_id: ObjKey, payload: ObjPayload) -> Res<()> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut frontier_tx = self.frontier.begin().await?;
        let cursor = frontier_tx.revision().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id.clone()).await?;
        let old_payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        sqlx::query!(
            "UPDATE big_sync_objs SET payload_json = ?1 WHERE obj_ref = ?2",
            payload_json,
            obj_ref
        )
        .execute(&mut **tx)
        .await?;
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
        .fetch_all(&mut **tx)
        .await?;
        let pending_parts = sqlx::query!(
            "SELECT p.part_ref, p.part_id
             FROM big_sync_pending_members m
             JOIN big_sync_parts p ON p.part_ref = m.part_ref
             WHERE m.scope_id = ?1 AND m.obj_ref = ?2",
            self.core.scope_id,
            obj_ref
        )
        .fetch_all(&mut **tx)
        .await?;
        let old_payload: ObjPayload = old_payload_json
            .as_deref()
            .filter(|str| !str.is_empty())
            .map(|str| serde_json::from_str(str).wrap_err(ERROR_JSON))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        for part in &live_parts {
            let old_state = MemberState::Live(old_payload.clone());
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    Self::part_from_blob(part.part_id.clone()),
                    obj_id.clone(),
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
        }
        let mut events = vec![SubEvent::Changed(big_sync_core::rpc::ObjChanged {
            cursor,
            part_ids: live_parts
                .iter()
                .map(|row| Self::part_from_blob(row.part_id.clone()))
                .collect(),
            obj_id: obj_id.clone(),
            payload: payload.clone(),
        })];
        for part in &pending_parts {
            let part_id = Self::part_from_blob(part.part_id.clone());
            let part_ref = part.part_ref;
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    part_id.clone(),
                    obj_id.clone(),
                    cursor,
                    &MemberState::Absent,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members
                 WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.core.scope_id,
                obj_ref,
                part_ref
            )
            .execute(&mut **tx)
            .await?;
            events.push(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor,
                part_ids: vec![part_id],
                obj_id: obj_id.clone(),
                payload: payload.clone(),
            }));
        }
        if live_parts.is_empty() {
            frontier_tx
                .put(
                    PartFrontierKey::Object(obj_id.clone()),
                    PartEvent::Changed(ObjChanged {
                        cursor,
                        part_ids: Vec::new(),
                        obj_id: obj_id.clone(),
                        payload: payload.clone(),
                    }),
                )
                .await?;
        }
        for part in &live_parts {
            frontier_tx
                .put(
                    PartFrontierKey::Part {
                        obj_id: obj_id.clone(),
                        part_id: Self::part_from_blob(part.part_id.clone()),
                    },
                    PartEvent::Changed(ObjChanged {
                        cursor,
                        part_ids: vec![Self::part_from_blob(part.part_id.clone())],
                        obj_id: obj_id.clone(),
                        payload: payload.clone(),
                    }),
                )
                .await?;
        }
        for part in &pending_parts {
            let part_id = Self::part_from_blob(part.part_id.clone());
            frontier_tx
                .put(
                    PartFrontierKey::Part {
                        obj_id: obj_id.clone(),
                        part_id: part_id.clone(),
                    },
                    PartEvent::Changed(big_sync_core::rpc::ObjChanged {
                        cursor,
                        part_ids: vec![part_id],
                        obj_id: obj_id.clone(),
                        payload: payload.clone(),
                    }),
                )
                .await?;
        }
        frontier_tx.commit().await?;
        Ok(())
    }

    async fn obj_part_added_at(
        &self,
        obj_id: ObjKey,
        part_id: PartKey,
    ) -> Res<Option<CursorIndex>> {
        // The member row is the record of the add. A row that is gone, or one that
        // predates the column, reports `None` — which delivers the tombstone rather than
        // dropping a removal on the strength of a record we do not have.
        let added_at: Option<i64> = sqlx::query_scalar(
            "SELECT m.added_at
               FROM big_sync_members m
              WHERE m.scope_id = ?1
                AND m.obj_ref = (
                    SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
                )
                AND m.maybe_part_ref = (
                    SELECT part_ref FROM big_sync_parts WHERE scope_id = ?1 AND part_id = ?3
                )",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id))
        .bind(Self::part_blob(part_id))
        .fetch_optional(&self.core.sql.read_pool)
        .await?;
        Ok(added_at.map(|value| u64::try_from(value).expect(ERROR_IMPOSSIBLE)))
    }

    async fn obj_parts(&self, obj_id: ObjKey) -> Res<Vec<PartKey>> {
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

    async fn obj_exists(&self, obj_id: ObjKey) -> Res<bool> {
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

    async fn get_bucket_summary(&self, part_id: PartKey, id: BuckId) -> Res<BucketSummary> {
        self.core.bucket_summary_for_path(part_id, id).await
    }

    async fn get_changed_buckets(
        &self,
        req: GetChangedBucketsRequest,
        subscriber: PeerKey,
    ) -> Res<Result<Vec<BucketSummary>, ListPartsError>> {
        // A part the subscriber may not read has to read as unknown, exactly as a
        // part this scope does not have does, so the refusal cannot be told apart
        // from one. Answered before any other work on the request.
        if self
            .read_denied(ReadTarget::Part(req.part_id.clone()), subscriber)
            .await?
        {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        // A hidden part stays physically present but is invisible to remote part
        // access. A bucket walk must answer for it exactly as `summarize_parts`
        // and the doc-scope store do, or the same RPC discloses the part's shape
        // in one scope and not in the other.
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(ListPartsError::UnkownParts {
                unkown_parts: vec![req.part_id],
            }));
        }
        // `limit_hint` is the response's page bound, with `BuckId::ARITY` extra
        // siblings allowed (see `GetChangedBucketsRequest`). A zero bound is a
        // zero-bucket page, not an unspecified one.
        if req.limit_hint == 0 {
            return Ok(Ok(Vec::new()));
        }
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.core.scope_id,
            Self::part_blob(req.part_id.clone())
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
        query.push(" AND level <= ");
        query.push_bind(i64::from(req.to_level));
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
        subscriber: PeerKey,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        // As above: an unreadable part reads as unknown, never as an empty page.
        if self
            .read_denied(ReadTarget::Part(req.part_id.clone()), subscriber)
            .await?
        {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        // Hidden parts are invisible to remote part access, as in every other
        // remote-facing read on this store.
        if self.hidden_parts.contains(&req.part_id) {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        let part_exists: Option<i64> = sqlx::query_scalar!(
            "SELECT 1
             FROM big_sync_parts
             WHERE scope_id = ?1 AND part_id = ?2",
            self.core.scope_id,
            Self::part_blob(req.part_id.clone())
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
            /// Encoded bytes the entries already cost, by `super::leaf_entry_wire_bytes`.
            bytes: usize,
            total_count: u32,
        }

        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "WITH requested(req_ord, buck_id, lower_index, upper_index, after_id) AS (",
        );
        for (req_ord, buck_req) in req.buckets.iter().enumerate() {
            let (lower_index, upper_index) = super::bucket_index_bounds(buck_req.buck_id);
            if req_ord > 0 {
                query.push(" UNION ALL ");
            }
            query.push("SELECT ");
            query.push_bind(i64::try_from(req_ord).expect(ERROR_IMPOSSIBLE));
            query.push(" AS req_ord, ");
            query.push_bind(Self::buck_i64(buck_req.buck_id));
            query.push(" AS buck_id, ");
            query.push_bind(i64::from(lower_index));
            query.push(" AS lower_index, ");
            if let Some(upper_index) = upper_index {
                query.push_bind(i64::from(upper_index));
            } else {
                query.push("NULL");
            }
            query.push(" AS upper_index, ");
            if let Some(after) = &buck_req.after {
                query.push_bind(Self::obj_blob(after.clone()));
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
        query.push(" AND o.buck_index >= r.lower_index");
        query.push(" AND (r.upper_index IS NULL OR o.buck_index < r.upper_index)");
        query.push(" AND (r.after_id IS NULL OR o.obj_id > r.after_id)");
        query.push(
            "
                JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
            )
            SELECT req_ord, buck_id, obj_id, event_type, payload_json, total_count
            FROM ranked
            WHERE row_num <= ",
        );
        // `LeafBucketsRequest::limit_hint` is a hint, not a bound: zero means no
        // preference, so the smallest useful page is one entry. The memory store
        // reads it the same way, and the shared contract harness pins that.
        query.push_bind(i64::from(req.limit_hint.max(1)));
        query.push(" ORDER BY req_ord, obj_id ASC");

        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;
        let mut pages: Vec<_> = req
            .buckets
            .iter()
            .map(|buck_req| LeafBucketPageBuilder {
                buck_id: buck_req.buck_id,
                entries: Vec::new(),
                bytes: 0,
                total_count: 0,
            })
            .collect();
        let mut budget_stopped: Option<usize> = None;
        for row in rows {
            let req_ord =
                usize::try_from(row.try_get::<i64, _>("req_ord")?).expect(ERROR_IMPOSSIBLE);
            // The budget ends the page for the bucket it binds on. Because the page is
            // keyset-paginated (`obj_id > after_id`), resuming past a skipped row would
            // strand it, so the page stops here and every later row of the *same* bucket is
            // dropped. Rows of the other buckets follow (the query orders by `req_ord`)
            // and must still be processed, so the stop is per-bucket rather than a flat
            // `break` of this shared result set.
            if budget_stopped == Some(req_ord) {
                continue;
            }
            let page = pages.get_mut(req_ord).expect(ERROR_IMPOSSIBLE);
            page.total_count =
                u32::try_from(row.try_get::<i64, _>("total_count")?).expect(ERROR_IMPOSSIBLE);
            let obj_id = Self::obj_from_blob(row.try_get("obj_id")?);
            let dead = row.try_get::<i64, _>("event_type")? == EVENT_REMOVED;
            // The page's cost is decided from the key alone — the fingerprint hashes the
            // payload without carrying it — so a row the page cannot hold costs no JSON parse.
            // The first entry is always taken, because a page with no entries reads as `done`
            // while entries remain, which would strand the tail.
            let entry_bytes = super::leaf_entry_wire_bytes(obj_id.as_bytes().len());
            if !page.entries.is_empty() && page.bytes + entry_bytes > super::LEAF_PAGE_BYTE_BUDGET {
                budget_stopped = Some(req_ord);
                continue;
            }
            page.bytes += entry_bytes;
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
                let payload_json: Option<String> = row.try_get("payload_json")?;
                let payload = payload_json
                    .filter(|payload_json| !payload_json.is_empty())
                    .map(|payload_json| serde_json::from_str(&payload_json).wrap_err(ERROR_JSON))
                    .transpose()?
                    .unwrap_or(serde_json::Value::Null);
                Fingerprint::new(&req.seed, &("big-sync-obj-fp-v1", obj_id.clone(), payload))
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
                Some(page.entries.last().expect(ERROR_IMPOSSIBLE).obj_id.clone())
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

    async fn add_obj_to_parts(&self, obj_id: ObjKey, parts: Vec<PartKey>) -> Res<()> {
        let mut frontier_tx = self.frontier.begin().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref = self.core.ensure_obj_ref(tx, obj_id.clone()).await?;
        let payload_json: Option<String> = sqlx::query_scalar!(
            "SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?1",
            obj_ref
        )
        .fetch_optional(&mut **tx)
        .await?
        .flatten();
        let Some(payload_json) = payload_json.filter(|str| !str.is_empty()) else {
            for part_id in parts {
                let part_ref = self.core.ensure_part_ref(&mut *tx, part_id).await?;
                sqlx::query!(
                    "INSERT OR IGNORE INTO big_sync_pending_members(scope_id, obj_ref, part_ref)
                     VALUES (?1, ?2, ?3)",
                    self.core.scope_id,
                    obj_ref,
                    part_ref
                )
                .execute(&mut **tx)
                .await?;
            }
            frontier_tx.commit().await?;
            return Ok(());
        };
        let payload: ObjPayload = serde_json::from_str(&payload_json).wrap_err(ERROR_JSON)?;
        let cursor = frontier_tx.revision().await?;
        let tx = frontier_tx.context_mut();
        let mut events = Vec::new();
        for part_id in parts {
            let part_ref = self.core.ensure_part_ref(&mut *tx, part_id.clone()).await?;
            let old_state = self
                .core
                .load_member_state(&mut *tx, part_id.clone(), obj_id.clone())
                .await?;
            if matches!(old_state, MemberState::Live(_)) {
                continue;
            }
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    part_id.clone(),
                    obj_id.clone(),
                    cursor,
                    &old_state,
                    &MemberState::Live(payload.clone()),
                )
                .await?;
            sqlx::query!(
                "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
                self.core.scope_id, obj_ref, part_ref
            ).execute(&mut **tx).await?;
            events.push(SubEvent::Changed(big_sync_core::rpc::ObjChanged {
                cursor,
                part_ids: vec![part_id],
                obj_id: obj_id.clone(),
                payload: payload.clone(),
            }));
        }
        for event in &events {
            let SubEvent::Changed(changed) = event else {
                continue;
            };
            for event_part in &changed.part_ids {
                // One frontier row per key, and the row's value is that part's touch.
                let mut narrowed = changed.clone();
                narrowed.part_ids = vec![event_part.clone()];
                frontier_tx
                    .put(
                        PartFrontierKey::Part {
                            obj_id: changed.obj_id.clone(),
                            part_id: event_part.clone(),
                        },
                        PartEvent::Changed(narrowed),
                    )
                    .await?;
            }
        }
        frontier_tx.commit().await?;
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjKey, part_id: PartKey) -> Res<()> {
        let mut frontier_tx = self.frontier.begin().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref: Option<i64> = sqlx::query_scalar!(
            "SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2",
            self.core.scope_id,
            Self::obj_blob(obj_id.clone())
        )
        .fetch_optional(&mut **tx)
        .await?;
        let Some(obj_ref) = obj_ref else {
            frontier_tx.commit().await?;
            return Ok(());
        };
        let part_ref = self.core.ensure_part_ref(&mut *tx, part_id.clone()).await?;
        sqlx::query!(
            "DELETE FROM big_sync_pending_members WHERE scope_id = ?1 AND obj_ref = ?2 AND part_ref = ?3",
            self.core.scope_id, obj_ref, part_ref
        ).execute(&mut **tx).await?;
        let old_state = self
            .core
            .load_member_state(&mut *tx, part_id.clone(), obj_id.clone())
            .await?;
        let MemberState::Live(old_payload) = old_state else {
            frontier_tx.commit().await?;
            return Ok(());
        };
        let cursor = frontier_tx.revision().await?;
        let tx = frontier_tx.context_mut();
        self.core
            .apply_bucket_transition(
                &mut *tx,
                part_id.clone(),
                obj_id.clone(),
                cursor,
                &MemberState::Live(old_payload),
                &MemberState::Dead,
            )
            .await?;
        // The object keeps its payload: a membership removal is not a content removal, and
        // dropping the content is `remove_obj_payload`'s job.
        frontier_tx
            .delete(PartFrontierKey::Part { obj_id, part_id })
            .await?;
        frontier_tx.commit().await?;
        Ok(())
    }

    /// Remove the object from every part it is in and drop its payload, in one transaction.
    ///
    /// The per-part half is `remove_obj_from_part`'s: the same bucket transition, tombstone row
    /// and frontier deletion, at one shared revision so the parts it leaves move together. That
    /// shared revision is also why the public per-part method cannot be called in a loop.
    async fn remove_obj_payload(&self, obj_id: ObjKey) -> Res<()> {
        let mut frontier_tx = self.frontier.begin().await?;
        let tx = frontier_tx.context_mut();
        let obj_ref: Option<i64> = sqlx::query_scalar(
            "SELECT obj_ref FROM big_sync_objs WHERE scope_id = ? AND obj_id = ?",
        )
        .bind(self.core.scope_id)
        .bind(Self::obj_blob(obj_id.clone()))
        .fetch_optional(&mut **tx)
        .await?;
        let Some(obj_ref) = obj_ref else {
            frontier_tx.commit().await?;
            return Ok(());
        };
        let payload_json: Option<String> =
            sqlx::query_scalar("SELECT payload_json FROM big_sync_objs WHERE obj_ref = ?")
                .bind(obj_ref)
                .fetch_one(&mut **tx)
                .await?;
        if payload_json.filter(|payload| !payload.is_empty()).is_none() {
            // No payload, nothing to drop. A live membership implies a payload, so an object
            // without one has no membership this could clear either.
            frontier_tx.commit().await?;
            return Ok(());
        }
        let live_parts = sqlx::query(
            "SELECT p.part_id
               FROM big_sync_members m
               JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
              WHERE m.scope_id = ? AND m.obj_ref = ? AND m.maybe_part_ref > 0
                AND m.event_type != ?",
        )
        .bind(self.core.scope_id)
        .bind(obj_ref)
        .bind(EVENT_REMOVED)
        .fetch_all(&mut **tx)
        .await?;
        let mut live_part_ids = Vec::with_capacity(live_parts.len());
        for row in live_parts {
            live_part_ids.push(Self::part_from_blob(row.try_get("part_id")?));
        }
        // One revision covers every removal: the parts move together, and the payload drop is
        // not a separate sync step.
        let cursor = if live_part_ids.is_empty() {
            None
        } else {
            Some(frontier_tx.revision().await?)
        };
        for part_id in live_part_ids {
            let cursor = cursor.expect(ERROR_IMPOSSIBLE);
            let tx = frontier_tx.context_mut();
            let old_state = self
                .core
                .load_member_state(&mut *tx, part_id.clone(), obj_id.clone())
                .await?;
            let MemberState::Live(old_payload) = old_state else {
                continue;
            };
            let tx = frontier_tx.context_mut();
            self.core
                .apply_bucket_transition(
                    &mut *tx,
                    part_id.clone(),
                    obj_id.clone(),
                    cursor,
                    &MemberState::Live(old_payload),
                    &MemberState::Dead,
                )
                .await?;
            frontier_tx
                .delete(PartFrontierKey::Part {
                    obj_id: obj_id.clone(),
                    part_id,
                })
                .await?;
        }
        // The object lane's row is content, and its payload is read out of `big_sync_objs`
        // rather than stored on the row, so a live row with nothing behind it is unreadable.
        // It goes with the payload; a reader that could hold the content drops it from the
        // removals above rather than from a notice. Checked before the payload is cleared,
        // because that read is what the row's value *is*.
        let object_key = PartFrontierKey::Object(obj_id);
        let has_object_lane_row = frontier_tx.get(&object_key).await?.is_some();
        let tx = frontier_tx.context_mut();
        sqlx::query("UPDATE big_sync_objs SET payload_json = NULL WHERE obj_ref = ?")
            .bind(obj_ref)
            .execute(&mut **tx)
            .await?;
        if has_object_lane_row {
            frontier_tx.delete(object_key).await?;
        }
        frontier_tx.commit().await?;
        Ok(())
    }

    async fn partless_objects(&self, limit: u32, after: Option<ObjKey>) -> Res<Vec<ObjKey>> {
        let mut query = QueryBuilder::<sqlx::Sqlite>::new(
            "SELECT o.obj_id
               FROM big_sync_objs o
              WHERE o.scope_id = ",
        );
        query.push_bind(self.core.scope_id);
        query.push(" AND o.payload_json IS NOT NULL AND o.payload_json != ''");
        if let Some(after) = after {
            query.push(" AND o.obj_id > ");
            query.push_bind(Self::obj_blob(after));
        }
        query.push(
            " AND NOT EXISTS (
                   SELECT 1 FROM big_sync_members m
                    WHERE m.scope_id = o.scope_id
                      AND m.obj_ref = o.obj_ref
                      AND m.maybe_part_ref > 0
                      AND m.event_type != ",
        );
        query.push_bind(EVENT_REMOVED);
        query.push(") ORDER BY o.obj_id ASC LIMIT ");
        query.push_bind(i64::from(limit));
        let rows = query.build().fetch_all(&self.core.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| Ok(Self::obj_from_blob(row.try_get("obj_id")?)))
            .collect()
    }

    async fn part_store_stats(&self) -> Res<PartStoreStats> {
        let row = sqlx::query(
            "SELECT
                 (SELECT COUNT(*) FROM big_sync_objs
                   WHERE scope_id = ?1 AND payload_json IS NOT NULL AND payload_json != '') AS payload_objects
               , (SELECT COALESCE(SUM(LENGTH(CAST(payload_json AS BLOB))), 0) FROM big_sync_objs
                   WHERE scope_id = ?1 AND payload_json IS NOT NULL AND payload_json != '') AS payload_bytes
               , (SELECT COUNT(*) FROM big_sync_objs o
                   WHERE o.scope_id = ?1 AND o.payload_json IS NOT NULL AND o.payload_json != ''
                     AND NOT EXISTS (
                         SELECT 1 FROM big_sync_members m
                          WHERE m.scope_id = o.scope_id
                            AND m.obj_ref = o.obj_ref
                            AND m.maybe_part_ref > 0
                            AND m.event_type != ?2)) AS partless_objects
               , (SELECT COUNT(*) FROM big_sync_members
                   WHERE scope_id = ?1 AND maybe_part_ref > 0 AND event_type != ?2) AS live_rows
               , (SELECT COUNT(*) FROM big_sync_members
                   WHERE scope_id = ?1 AND maybe_part_ref > 0 AND event_type = ?2) AS dead_rows",
        )
        .bind(self.core.scope_id)
        .bind(EVENT_REMOVED)
        .fetch_one(&self.core.sql.read_pool)
        .await?;
        Ok(PartStoreStats {
            payload_objects: u64::try_from(row.try_get::<i64, _>("payload_objects")?)
                .expect(ERROR_IMPOSSIBLE),
            payload_bytes: u64::try_from(row.try_get::<i64, _>("payload_bytes")?)
                .expect(ERROR_IMPOSSIBLE),
            partless_objects: u64::try_from(row.try_get::<i64, _>("partless_objects")?)
                .expect(ERROR_IMPOSSIBLE),
            live_rows: u64::try_from(row.try_get::<i64, _>("live_rows")?).expect(ERROR_IMPOSSIBLE),
            dead_rows: u64::try_from(row.try_get::<i64, _>("dead_rows")?).expect(ERROR_IMPOSSIBLE),
        })
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerKey, part_id: PartKey) -> Res<CursorIndex> {
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
        peer_id: PeerKey,
        part_id: PartKey,
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

    async fn open_revision_reader(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<Box<dyn super::LocalPartRevisionReader>, ListPartsError>> {
        use big_sync_core::rpc::SubscriptionTarget;

        let objects = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Object { obj_id, .. } => Some(obj_id.clone()),
                SubscriptionTarget::Part { .. } => None,
            })
            .collect::<HashSet<_>>();
        let parts = reqs
            .targets
            .iter()
            .filter_map(|target| match target {
                SubscriptionTarget::Part { part_id, .. } => Some(part_id.clone()),
                SubscriptionTarget::Object { .. } => None,
            })
            .collect::<HashSet<_>>();
        // The reader is the faithful seam: it hands back what the log holds, tombstones
        // included. Peer-facing policy lives in the responder, which knows the requested
        // cursor and the peer.
        let mut selector = SqlitePartSelector::default();
        for target in reqs.targets {
            match target {
                SubscriptionTarget::Part { part_id, cursor } => {
                    selector
                        .parts
                        .entry(part_id)
                        .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound.max(cursor)))
                        .or_insert(reqs.lower_bound.max(cursor));
                }
                SubscriptionTarget::Object { obj_id, .. } => {
                    selector
                        .objects
                        .entry(obj_id)
                        .and_modify(|bound| *bound = (*bound).max(reqs.lower_bound))
                        .or_insert(reqs.lower_bound);
                }
            }
        }
        let reader = open_sqlite_reader(self.frontier.clone(), selector).await?;
        Ok(Ok(Box::new(super::PartRevisionReader::new(
            reader, objects, parts,
        ))))
    }

    async fn open_revision_reader_all(
        &self,
        after: CursorIndex,
    ) -> Res<Result<Box<dyn super::LocalPartRevisionReader>, ListPartsError>> {
        let reader = open_sqlite_reader(
            self.frontier.clone(),
            SqlitePartSelector {
                all: Some(after),
                ..Default::default()
            },
        )
        .await?;
        Ok(Ok(Box::new(super::PartRevisionReader::new_all(reader))))
    }

    async fn ensure_part(&self, part_id: PartKey) -> Res<()> {
        let mut tx = self.core.sql.write_pool.begin().await?;
        self.core.ensure_part_ref(&mut tx, part_id).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_part_members(
        &self,
        part: PartKey,
        agents: HashMap<PeerKey, keyhive_core::access::Access>,
    ) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part).await?;
        let changed_at =
            i64::try_from(SqliteCore::next_cursor(&mut tx).await?).expect(ERROR_IMPOSSIBLE);
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND part_ref = ?2",
            self.core.scope_id,
            part_ref
        )
        .execute(&mut *tx)
        .await?;
        for (principal, access) in &agents {
            sqlx::query!(
                "INSERT INTO big_sync_syncable(scope_id, part_ref, principal_id, access_level, changed_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                self.core.scope_id,
                part_ref,
                Self::peer_blob(principal.clone()),
                encode_access(access),
                changed_at
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn add_part_member(
        &self,
        part: PartKey,
        member: PeerKey,
        access: keyhive_core::access::Access,
    ) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let part_ref = self.core.ensure_part_ref(&mut tx, part).await?;
        let changed_at =
            i64::try_from(SqliteCore::next_cursor(&mut tx).await?).expect(ERROR_IMPOSSIBLE);
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND part_ref = ?2 AND principal_id = ?3",
            self.core.scope_id, part_ref, Self::peer_blob(member.clone())
        ).execute(&mut *tx).await?;
        sqlx::query!(
            "INSERT INTO big_sync_syncable(scope_id, part_ref, principal_id, access_level, changed_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            self.core.scope_id,
            part_ref,
            Self::peer_blob(member),
            encode_access(&access),
            changed_at
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_part_member(&self, part: PartKey, member: PeerKey) -> Res<()> {
        let mut tx = self
            .core
            .sql
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await?;
        let Some(part_ref) = self.core.find_part_ref(part).await? else {
            tx.commit().await?;
            return Ok(());
        };
        sqlx::query!(
            "DELETE FROM big_sync_syncable WHERE scope_id = ?1 AND part_ref = ?2 AND principal_id = ?3",
            self.core.scope_id, part_ref, Self::peer_blob(member)
        ).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn permitted_parts(
        &self,
        scope: PartScope,
        obj_id: ObjKey,
        principal: Option<PeerKey>,
    ) -> Res<Option<Vec<PartKey>>> {
        permitted_parts(&self.core, scope, obj_id, principal).await
    }
}

/// Filter an outbound event's candidate parts down to the parts `principal` may read.
///
/// Access is granted per part, so this single operation is both the authorization
/// check and the non-exposure rule: a part id the principal cannot read is never
/// disclosed. `Ok(None)` means unfiltered (trusted local subscriber).
async fn permitted_parts(
    core: &SqliteCore,
    scope: PartScope,
    obj_id: ObjKey,
    principal: Option<PeerKey>,
) -> Res<Option<Vec<PartKey>>> {
    // `principal` is only borrowed here: it is still logged below, and the
    // non-exposure rule this function enforces is about its value, not ownership.
    let Some(peer) = &principal else {
        return Ok(None);
    };
    let peer_blob = SqliteCore::peer_blob(peer.clone());
    let candidates: Vec<PartKey> = match scope {
        // Resolve and filter in one query: the object's live parts that grant this
        // principal access. An event that named nothing usable still delivers when
        // the principal can read some part of it.
        PartScope::FromObject => {
            let rows = sqlx::query!(
                "SELECT p.part_id AS 'part_id: Vec<u8>', s.access_level
                 FROM big_sync_members m
                 JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
                 JOIN big_sync_syncable s ON s.part_ref = m.maybe_part_ref
                 WHERE m.scope_id = ?1
                   AND m.obj_ref = (
                       SELECT obj_ref FROM big_sync_objs WHERE scope_id = ?1 AND obj_id = ?2
                   )
                   AND m.maybe_part_ref > 0
                   AND m.event_type != 2
                   AND s.principal_id = ?3
                 ORDER BY p.part_id",
                core.scope_id,
                SqliteCore::obj_blob(obj_id.clone()),
                &peer_blob
            )
            .fetch_all(&core.sql.read_pool)
            .await?;
            let readable = rows
                .into_iter()
                .filter(|row| is_fetch_access(row.access_level))
                .map(|row| SqliteCore::part_from_blob(row.part_id))
                .collect::<Vec<_>>();
            tracing::trace!(
                ?obj_id,
                ?principal,
                part_count = readable.len(),
                "policy event permission",
            );
            return Ok(Some(readable));
        }
        PartScope::Part(part_id) => vec![part_id],
        PartScope::AnyOf(part_ids) => part_ids,
    };
    let mut readable = Vec::with_capacity(candidates.len());
    for part_id in candidates {
        let Some(part_ref) = core.find_part_ref(part_id.clone()).await? else {
            continue;
        };
        let access_level: Option<i64> = sqlx::query_scalar!(
            "SELECT access_level
             FROM big_sync_syncable
             WHERE scope_id = ?1 AND part_ref = ?2 AND principal_id = ?3",
            core.scope_id,
            part_ref,
            &peer_blob
        )
        .fetch_optional(&core.sql.read_pool)
        .await?;
        if access_level.is_some_and(is_fetch_access) {
            readable.push(part_id);
        }
    }
    tracing::trace!(
        ?obj_id,
        ?principal,
        part_count = readable.len(),
        "policy event permission",
    );
    Ok(Some(readable))
}

fn is_fetch_access(access_level: i64) -> bool {
    u8::try_from(access_level)
        .ok()
        .map(super::sqlite_core::decode_access)
        .is_some_and(|access| access.is_fetcher())
}

#[cfg(test)]
impl PartStoreReadOnly<Sendable> for SqlitePartStore {
    fn member_count<'a>(&'a self, part_id: PartKey) -> BoxFuture<'a, u64> {
        Sendable::from_future(async move {
            HostPartStore::member_count(self, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn part_dirty_count<'a>(
        &'a self,
        part_id: PartKey,
        principal: Option<PeerKey>,
        since: CursorIndex,
    ) -> BoxFuture<'a, PartDirtyCount> {
        Sendable::from_future(async move {
            HostPartStore::part_dirty_count(self, part_id, principal, since)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjKey) -> BoxFuture<'a, Option<ObjPayload>> {
        Sendable::from_future(async move {
            HostPartStore::obj_payload(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn get_bucket_summary<'a>(
        &'a self,
        part_id: PartKey,
        id: BuckId,
    ) -> BoxFuture<'a, BucketSummary> {
        Sendable::from_future(async move {
            HostPartStore::get_bucket_summary(self, part_id, id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjKey) -> BoxFuture<'a, Vec<PartKey>> {
        Sendable::from_future(async move {
            HostPartStore::obj_parts(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn get_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerKey,
        part_id: PartKey,
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
    fn upsert_obj<'a>(&'a self, obj_id: ObjKey, payload: &ObjPayload) -> BoxFuture<'a, ()> {
        let payload = payload.clone();
        Sendable::from_future(async move {
            HostPartStore::set_obj_payload(self, obj_id, payload)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjKey, parts: &[PartKey]) -> BoxFuture<'a, ()> {
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartStore::add_obj_to_parts(self, obj_id, parts)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjKey, part_id: PartKey) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartStore::remove_obj_from_part(self, obj_id, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE);
        })
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerKey,
        part_id: PartKey,
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
    use crate::part_store::host_contract::{self, HostPartStoreContractHarness, PageEventStore};
    use big_sync_core::keyed_frontier::KeyedFrontierTransaction;
    use big_sync_core::part_store::contract;
    use big_sync_core::rpc::SubscriptionTarget;
    use crate::part_store::ReplayPageOutcome;

    async fn test_sql() -> Res<SqlCtx> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let sqlite_url = format!("sqlite://{}", db_path.display());
        SqlCtx::url(&sqlite_url).await
    }

    async fn test_store(scope_key: &str) -> Res<SqlitePartStore> {
        let sql = test_sql().await?;
        SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL).await
    }

    /// A hidden part stays physically present but is invisible to remote part access
    /// (`HostPartStoreConfig::hidden_parts`), and that has to hold for the bucket
    /// endpoints too: `summarize_parts` and the doc-scope store both answer for such a
    /// part as if it were unknown, so a bucket walk that answered with its shape would
    /// disclose it in one scope and not the other.
    #[tokio::test(flavor = "multi_thread")]
    async fn hidden_parts_are_invisible_to_bucket_walks() -> Res<()> {
        use big_sync_core::FingerprintSeed;
        use big_sync_core::rpc::{LeafBucketRequest, LeafBucketsRequest};

        let hidden = test_part_id(60);
        let visible = test_part_id(61);
        let member = test_obj_id(62);
        let sql = test_sql().await?;
        let store = SqlitePartStore::new_with_config(
            sql,
            "hidden-parts",
            BuckId::MAX_LEVEL,
            crate::part_store::HostPartStoreConfig {
                hidden_parts: HashSet::from([hidden.clone()]),
                ..Default::default()
            },
        )
        .await?;

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
        let hidden = test_part_id(70);
        let visible = test_part_id(71);
        let member = test_obj_id(72);
        let sql = test_sql().await?;
        let store = SqlitePartStore::new_with_config(
            sql,
            "hidden-parts-page",
            BuckId::MAX_LEVEL,
            crate::part_store::HostPartStoreConfig {
                hidden_parts: HashSet::from([hidden.clone()]),
                ..Default::default()
            },
        )
        .await?;
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

    fn test_part_id(seed: u8) -> PartKey {
        PartKey(ByteKey::new([seed; 32]))
    }

    fn test_obj_id(seed: u8) -> ObjKey {
        ObjKey(ByteKey::new([seed; 32]))
    }

    async fn put_frontier_event(
        store: &SqlitePartStore,
        key: PartFrontierKey,
        event: PartEvent,
    ) -> Res<CursorIndex> {
        let mut tx = store.frontier.begin().await?;
        tx.put(key, event).await?;
        Ok(tx.commit().await?)
    }

    async fn delete_frontier_key(
        store: &SqlitePartStore,
        key: PartFrontierKey,
    ) -> Res<CursorIndex> {
        let mut tx = store.frontier.begin().await?;
        tx.delete(key).await?;
        Ok(tx.commit().await?)
    }

    /// The payload outlives the membership: this store used to null the payload when the live
    /// count reached zero, and `remove_obj_payload` is the only path that clears it now.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_membership_semantics() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://repo").await?;
        let part_id = test_part_id(1);
        let obj_id = test_obj_id(2);
        contract::assert_add_obj_to_parts_is_idempotent(&store, part_id.clone(), obj_id.clone())
            .await;
        host_contract::assert_payload_survives_membership_removal_contract(&SqliteHostHarness {
            store: store.clone(),
        })
        .await?;
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
        contract::assert_peer_cursor_roundtrip(&store, PeerKey(ByteKey::new([42; 32])), part_id)
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
                obj_id.clone(),
                serde_json::json!({"phase": "present", "ii": ii}),
            )
            .await?;
            HostPartStore::add_obj_to_parts(&store, obj_id.clone(), vec![part_id.clone()]).await?;
            obj_ids.push(obj_id);
        }

        crate::part_store::contract::assert_root_bucket_contract(
            &store,
            part_id.clone(),
            seed,
            &obj_ids,
            &[],
            2,
        )
        .await?;

        let removed_obj_id = obj_ids[1].clone();
        HostPartStore::remove_obj_from_part(&store, removed_obj_id.clone(), part_id.clone())
            .await?;
        let live_ids: Vec<_> = obj_ids
            .iter()
            .filter(|&obj_id| *obj_id != removed_obj_id)
            .cloned()
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

        HostPartStore::set_obj_payload(
            &store,
            obj_id.clone(),
            serde_json::json!({"phase": "created"}),
        )
        .await?;
        HostPartStore::add_obj_to_parts(&store, obj_id.clone(), vec![part_id.clone()]).await?;
        HostPartStore::remove_obj_from_part(&store, obj_id.clone(), part_id.clone()).await?;

        // The object joined the part at cursor 2 and the tombstone is at cursor 3, so a reader
        // at cursor 0 never saw the object in the part and is not handed the removal; a reader
        // that did see the add is.
        let nothing_at_zero =
            HostPartStore::list_events(&store, HashSet::from([part_id.clone()]), 0, 10)
                .await?
                .expect(ERROR_IMPOSSIBLE);
        assert!(
            nothing_at_zero
                .get(&part_id)
                .expect(ERROR_IMPOSSIBLE)
                .events
                .is_empty(),
            "a tombstone for a member this reader never saw is not delivered"
        );

        let deleted_page =
            HostPartStore::list_events(&store, HashSet::from([part_id.clone()]), 2, 10)
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

        HostPartStore::set_obj_payload(
            &store,
            obj_id.clone(),
            serde_json::json!({"phase": "recreated"}),
        )
        .await?;
        HostPartStore::add_obj_to_parts(&store, obj_id.clone(), vec![part_id.clone()]).await?;

        let upserted_page =
            HostPartStore::list_events(&store, HashSet::from([part_id.clone()]), 2, 10)
                .await?
                .expect(ERROR_IMPOSSIBLE);
        let upserted_events = &upserted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(upserted_events.len(), 1);
        let PartEvent::Changed(second_added) = &upserted_events[0] else {
            panic!("expected the second member write to arrive as a touch");
        };
        assert_eq!(second_added.cursor, 5);
        assert_eq!(second_added.part_ids, vec![part_id]);
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

        HostPartStore::set_obj_payload(&store_a, obj_id.clone(), serde_json::json!({"scope": "a"}))
            .await?;
        HostPartStore::add_obj_to_parts(&store_a, obj_id.clone(), vec![part_id.clone()]).await?;
        HostPartStore::set_obj_payload(&store_b, obj_id.clone(), serde_json::json!({"scope": "b"}))
            .await?;
        HostPartStore::add_obj_to_parts(&store_b, obj_id.clone(), vec![part_id.clone()]).await?;

        assert_eq!(
            HostPartStore::obj_payload(&store_a, obj_id.clone()).await?,
            Some(serde_json::json!({"scope": "a"}))
        );
        assert_eq!(
            HostPartStore::obj_payload(&store_b, obj_id).await?,
            Some(serde_json::json!({"scope": "b"}))
        );
        assert_eq!(
            HostPartStore::member_count(&store_a, part_id.clone()).await?,
            1
        );
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

    /// The leaf page's byte budget, which the store applies beside the entry hint.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_leaf_page_byte_budget_contract() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://leaf-byte-budget").await?;
        host_contract::assert_leaf_page_byte_budget_contract(&store).await
    }

    /// What the store shows a peer, which a part-less payload is not part of.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_observed_snapshot_excludes_partless_payload_contract() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://observed-partless").await?;
        host_contract::assert_observed_snapshot_excludes_partless_payload(&store).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_membership_cache_rehydrates_after_restart() -> Res<()> {
        use keyhive_core::access::Access;
        use tokio::time::{Duration, timeout};

        let sql = test_sql().await?;
        let scope_key = "big-sync-sqlite-test://membership-restart";

        // ---- first session ----
        let store1 = SqlitePartStore::new(sql.clone(), scope_key, BuckId::MAX_LEVEL).await?;
        let part = PartKey(ByteKey::new([201u8; 32]));
        let obj = ObjKey(ByteKey::new([202u8; 32]));
        let auth = PeerKey::new([203u8; 32]);
        let denied = PeerKey::new([204u8; 32]);

        store1.ensure_part(part.clone()).await?;
        store1
            .set_obj_payload(obj.clone(), serde_json::json!("first"))
            .await?;
        store1
            .add_obj_to_parts(obj.clone(), vec![part.clone()])
            .await?;

        // Persist membership.
        store1
            .set_part_members(
                part.clone(),
                std::collections::HashMap::from([(auth.clone(), Access::Read)]),
            )
            .await?;

        // Helper to drain through ReplayComplete.
        async fn drain_through_replay(stream: &host_contract::TestEventStream) -> Res<()> {
            loop {
                match timeout(Duration::from_secs(5), stream.next()).await? {
                    Ok(SubEvent::ReplayComplete) => return Ok(()),
                    Ok(_) => continue,
                    Err(_) => {
                        eyre::bail!("the reader stopped answering before its replay completed");
                    }
                }
            }
        }

        let sub = |peer: PeerKey| {
            let store = &store1;
            let part = &part;
            async move {
                store
                    .page_events(
                        SubPartsRequest {
                            lower_bound: 0,
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: part.clone(),
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
        let auth_rx1 = sub(auth.clone()).await?;
        drain_through_replay(&auth_rx1).await?;

        // Live mutation: the authorized peer receives it.
        store1
            .set_obj_payload(obj.clone(), serde_json::json!("second"))
            .await?;
        auth_rx1
            .next()
            .await
            .expect("authorized must receive live event in first session");
        // The denied peer's answer is the responder's refusal, not a filtered stream: the
        // reader is the store's unfiltered seam, so the denial is asserted where it is
        // enforced.
        assert!(
            matches!(
                store1
                    .replay_page(
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part.clone(),
                            cursor: 0,
                        },
                        16,
                        denied.clone(),
                        Duration::from_millis(50),
                    )
                    .await?,
                crate::part_store::ReplayPageOutcome::Unauthorized
            ),
            "a peer outside the part's members must be refused"
        );

        // Drop store1 to simulate restart.
        drop(store1);

        // ---- second session on the same database and scope ----
        let store2 = SqlitePartStore::new(sql, scope_key, BuckId::MAX_LEVEL).await?;

        let sub2 = |peer: PeerKey| {
            let store = &store2;
            let part = &part;
            async move {
                store
                    .page_events(
                        SubPartsRequest {
                            lower_bound: 0,
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: part.clone(),
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
        drain_through_replay(&auth_rx2).await?;
        // Live mutation after restart.
        store2
            .set_obj_payload(obj, serde_json::json!("third"))
            .await?;
        auth_rx2
            .next()
            .await
            .expect("authorized must receive live event after restart");
        // Denied must still be denied after restart: the refusal reads the members this
        // scope persisted, so it survives the store being recreated.
        assert!(
            matches!(
                store2
                    .replay_page(
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: part.clone(),
                            cursor: 0,
                        },
                        16,
                        denied.clone(),
                        Duration::from_millis(50),
                    )
                    .await?,
                crate::part_store::ReplayPageOutcome::Unauthorized
            ),
            "the refusal must survive a restart: it reads the persisted members"
        );

        Ok(())
    }

    /// The object lane is the local/unfiltered lane: a partless object still delivers on
    /// it. REMOTE object reads of a partless object are refused (see
    /// `sqlite_partless_object_is_refused_to_a_remote_peer`), because access is granted per
    /// part and this object is in no part.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_object_reader_delivers_a_partless_object_content() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://subscribe-object").await?;
        let obj_id = test_obj_id(210);
        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id.clone()),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 1}),
            }),
        )
        .await?;

        let rx = store
            .page_events_local(SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                    obj_id: obj_id.clone(),
                    cursor: 0,
                }]),
            })
            .await?
            .map_err(eyre::Report::from)?;
        match rx.next().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.cursor, 1);
                assert_eq!(changed.obj_id, obj_id);
                assert!(changed.part_ids.is_empty());
                assert_eq!(changed.payload, serde_json::json!({"value": 1}));
            }
            event => panic!("expected object Changed, got {event:?}"),
        }
        assert_eq!(
            rx.next().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    /// Access is granted per part, and a partless object is in no part, so it has no
    /// remote authorization until virtual parts land (ADR 012). Fail-closed: the
    /// responder refuses the target on both the replay and the live path, and the
    /// object's own event is unreadable to that peer, so neither a part id nor a payload
    /// can leak. The reader is the store's unfiltered seam, so the test asserts the
    /// answer where it is enforced.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_partless_object_is_refused_to_a_remote_peer() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://subscribe-partless-denied").await?;
        let obj_id = test_obj_id(225);
        let peer = PeerKey::new([226; 32]);
        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id.clone()),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 1}),
            }),
        )
        .await?;

        // The responder refuses the target: a partless object has no remote
        // authorization.
        let target = big_sync_core::rpc::SubscriptionTarget::Object {
            obj_id: obj_id.clone(),
            cursor: 0,
        };
        let refused = store
            .replay_page(target.clone(), 16, peer.clone(), Duration::from_millis(50))
            .await?;
        assert!(
            matches!(refused, crate::part_store::ReplayPageOutcome::Unauthorized),
            "a partless object must not be delivered to a remote subscriber; got {refused:?}"
        );
        let leaked = PartEvent::Changed(ObjChanged {
            cursor: 1,
            part_ids: Vec::new(),
            obj_id: obj_id.clone(),
            payload: serde_json::json!({"value": 1}),
        });
        assert!(
            !store.page_event_is_readable(&leaked, peer.clone()).await?,
            "the partless object's event must not be readable by a remote peer"
        );

        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id.clone()),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id,
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;
        // The live path answers the same way: a second change still cannot reach that
        // peer, because the refusal is the authorization answer and not a stream filter.
        let refused_again = store
            .replay_page(target, 16, peer, Duration::from_millis(50))
            .await?;
        assert!(
            matches!(
                refused_again,
                crate::part_store::ReplayPageOutcome::Unauthorized
            ),
            "a partless object stays refused on the live path; got {refused_again:?}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_part_targets_have_independent_lower_bounds() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-part-cursors").await?;
        let obj_id = test_obj_id(212);
        let first_part = test_part_id(213);
        let second_part = test_part_id(214);
        let peer = PeerKey::new([215; 32]);
        store.ensure_part(first_part.clone()).await?;
        store.ensure_part(second_part.clone()).await?;
        for part in [first_part.clone(), second_part.clone()] {
            store
                .set_part_members(part, HashMap::from([(peer.clone(), Access::Read)]))
                .await?;
        }
        // Access writes consume scope cursor values (ADR 012 decision 5), so the first
        // frontier edit does not start at 1: derive the cursors instead of hard-coding.
        let first_cursor = put_frontier_event(
            &store,
            PartFrontierKey::Part {
                obj_id: obj_id.clone(),
                part_id: first_part.clone(),
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![first_part.clone()],
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"part": 1}),
            }),
        )
        .await?;
        let second_cursor = put_frontier_event(
            &store,
            PartFrontierKey::Part {
                obj_id: obj_id.clone(),
                part_id: second_part.clone(),
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![second_part.clone()],
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"part": 2}),
            }),
        )
        .await?;

        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: first_part,
                            cursor: first_cursor,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: second_part.clone(),
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        match rx.next().await.expect("subscription channel stays open") {
            SubEvent::Changed(added) => {
                assert_eq!(added.cursor, second_cursor);
                assert_eq!(added.part_ids[0], second_part);
                assert_eq!(added.obj_id, obj_id);
            }
            event => panic!("expected only newer second-part Added, got {event:?}"),
        }
        assert_eq!(
            rx.next().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_coalesces_one_revision_across_parts() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-coalesce").await?;
        let obj_id = test_obj_id(216);
        let first_part = test_part_id(217);
        let second_part = test_part_id(218);
        let peer = PeerKey::new([219; 32]);
        store.ensure_part(first_part.clone()).await?;
        store.ensure_part(second_part.clone()).await?;
        for part in [first_part.clone(), second_part.clone()] {
            store
                .set_part_members(part, HashMap::from([(peer.clone(), Access::Read)]))
                .await?;
        }
        let mut tx = store.frontier.begin().await?;
        tx.put(
            PartFrontierKey::Part {
                obj_id: obj_id.clone(),
                part_id: first_part.clone(),
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![first_part.clone()],
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;
        tx.put(
            PartFrontierKey::Part {
                obj_id: obj_id.clone(),
                part_id: second_part.clone(),
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![second_part.clone()],
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;
        // One commit is one revision: a single cursor covers both parts (ADR 012 decision 8).
        let revision = tx.commit().await?;

        let rx = store
            .page_events(
                SubPartsRequest {
                    lower_bound: 0,
                    targets: HashSet::from([
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: first_part.clone(),
                            cursor: 0,
                        },
                        big_sync_core::rpc::SubscriptionTarget::Part {
                            part_id: second_part.clone(),
                            cursor: 0,
                        },
                    ]),
                },
                peer,
            )
            .await?
            .map_err(eyre::Report::from)?;
        match rx.next().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.cursor, revision);
                assert_eq!(changed.obj_id, obj_id);
                assert_eq!(
                    changed.part_ids.iter().cloned().collect::<HashSet<_>>(),
                    HashSet::from([first_part, second_part])
                );
            }
            event => panic!("expected coalesced Changed, got {event:?}"),
        }
        assert_eq!(
            rx.next().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        Ok(())
    }

    /// A part's tombstone reaches a reader that had seen the object in the part, and not one
    /// whose cursor predates the add (`added_at <= cursor < tombstone`).
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_subscribe_part_tombstone_is_removed() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://subscribe-tombstone").await?;
        let obj_id = test_obj_id(220);
        let part_id = test_part_id(221);
        let peer = PeerKey::new([222; 32]);
        store.ensure_part(part_id.clone()).await?;
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;
        let added_cursor = put_frontier_event(
            &store,
            PartFrontierKey::Part {
                obj_id: obj_id.clone(),
                part_id: part_id.clone(),
            },
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: vec![part_id.clone()],
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 1}),
            }),
        )
        .await?;
        let tombstone_cursor = delete_frontier_key(
            &store,
            PartFrontierKey::Part {
                obj_id: obj_id.clone(),
                part_id: part_id.clone(),
            },
        )
        .await?;

        let sub = |cursor| {
            let store = &store;
            let part_id = part_id.clone();
            let peer = peer.clone();
            async move {
                store
                    .page_events(
                        SubPartsRequest {
                            lower_bound: cursor,
                            targets: HashSet::from([
                                big_sync_core::rpc::SubscriptionTarget::Part {
                                    part_id: part_id.clone(),
                                    cursor,
                                },
                            ]),
                        },
                        peer,
                    )
                    .await
            }
        };

        // A faithful reader is handed the tombstone even when its cursor predates the add:
        // it replays the log, not a peer's replica. Peer policy is the responder's, asserted
        // below with the request's own cursor.
        let before = sub(0).await??;
        let mut before_events = Vec::new();
        loop {
            match before.next().await? {
                SubEvent::ReplayComplete => break,
                event => before_events.push(event),
            }
        }
        assert!(
            before_events
                .iter()
                .any(|event| matches!(event, SubEvent::Removed(_))),
            "a faithful reader is handed the tombstone; got {before_events:?}"
        );

        // A peer whose request started before the add is not told about the removal...
        let before_page = store
            .replay_page(
                big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_id.clone(),
                    cursor: 0,
                },
                8,
                peer.clone(),
                Duration::from_millis(50),
            )
            .await?;
        let crate::part_store::ReplayPageOutcome::Events(before_page) = before_page else {
            panic!("a readable part must be answered a page, got {before_page:?}");
        };
        assert!(
            before_page.events.is_empty(),
            "a peer whose request started before the add is not told about the removal, got {:?}",
            before_page.events
        );

        // ...and a peer whose request started at the add is.
        let after = sub(added_cursor).await??;
        assert_eq!(
            after.next().await.expect("the reader stays open"),
            SubEvent::Removed(ObjRemovedFromPart {
                cursor: tombstone_cursor,
                part_id: part_id.clone(),
                obj_id: obj_id.clone(),
            })
        );
        assert_eq!(
            after.next().await.expect("the reader stays open"),
            SubEvent::ReplayComplete
        );
        let at_add = store
            .replay_page(
                big_sync_core::rpc::SubscriptionTarget::Part {
                    part_id: part_id.clone(),
                    cursor: added_cursor,
                },
                8,
                peer,
                Duration::from_millis(50),
            )
            .await?;
        let crate::part_store::ReplayPageOutcome::Events(at_add) = at_add else {
            panic!("a readable part must be answered a page, got {at_add:?}");
        };
        assert!(
            at_add.events.iter().any(|event| matches!(
                event,
                PartEvent::Removed(removed) if removed.cursor == tombstone_cursor
            )),
            "a peer whose request started at the add is handed the removal, got {:?}",
            at_add.events
        );
        Ok(())
    }

    /// Object-lane replay boundary, on the local/unfiltered lane (see the remote-denial
    /// note on `sqlite_object_reader_delivers_a_partless_object_content`).
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_reader_replays_once_then_reads_after_the_boundary() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://subscribe-boundary").await?;
        let obj_id = test_obj_id(223);
        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id.clone()),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 1}),
            }),
        )
        .await?;

        let rx = store
            .page_events_local(SubPartsRequest {
                lower_bound: 0,
                targets: HashSet::from([big_sync_core::rpc::SubscriptionTarget::Object {
                    obj_id: obj_id.clone(),
                    cursor: 0,
                }]),
            })
            .await?
            .map_err(eyre::Report::from)?;
        assert!(matches!(
            rx.next().await.expect("subscription channel stays open"),
            SubEvent::Changed(ObjChanged { cursor: 1, .. })
        ));

        put_frontier_event(
            &store,
            PartFrontierKey::Object(obj_id.clone()),
            PartEvent::Changed(ObjChanged {
                cursor: 0,
                part_ids: Vec::new(),
                obj_id: obj_id.clone(),
                payload: serde_json::json!({"value": 2}),
            }),
        )
        .await?;

        assert_eq!(
            rx.next().await.expect("subscription channel stays open"),
            SubEvent::ReplayComplete
        );
        match rx.next().await.expect("subscription channel stays open") {
            SubEvent::Changed(changed) => {
                assert_eq!(changed.cursor, 2);
                assert_eq!(changed.obj_id, obj_id);
                assert_eq!(changed.payload, serde_json::json!({"value": 2}));
            }
            event => panic!("expected post-boundary Changed, got {event:?}"),
        }
        Ok(())
    }

    /// See the memory store's twin for why the primitive is asserted at the store
    /// level: local rows newer than a cursor are counted, with the two relevance
    /// sources reported separately.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_dirty_count_separates_member_and_access_changes() -> Res<()> {
        let store = test_store("big-sync-sqlite-test://part-dirty-count").await?;
        let part = test_part_id(60);
        let other_part = test_part_id(61);
        let peer = PeerKey::new([62; 32]);
        let other_peer = PeerKey::new([63; 32]);
        let obj_a = test_obj_id(64);
        let obj_b = test_obj_id(65);
        let obj_c = test_obj_id(66);
        let read = keyhive_core::access::Access::Read;

        store.ensure_part(part.clone()).await?;
        store.ensure_part(other_part.clone()).await?;
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part.clone(), Some(peer.clone()), 0).await?,
            PartDirtyCount::default(),
            "a part with neither members nor grants has no relevance to count"
        );

        // A member write moves the member number only.
        store
            .set_obj_payload(obj_a.clone(), serde_json::json!({"a": 1}))
            .await?;
        store.add_obj_to_parts(obj_a, vec![part.clone()]).await?;
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part.clone(), Some(peer.clone()), 0).await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 0,
            }
        );

        // A grant on this part for this peer adds the access number alongside it.
        store
            .set_part_members(part.clone(), HashMap::from([(peer.clone(), read)]))
            .await?;
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part.clone(), Some(peer.clone()), 0).await?,
            PartDirtyCount {
                member_changes: 1,
                access_changes: 1,
            }
        );

        // The local principal is never gated by access rows, so it has no access half;
        // the member half does not depend on who is asking and still counts.
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part.clone(), None, 0).await?,
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
            HostPartStore::part_dirty_count(&store, part.clone(), Some(peer.clone()), 0).await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            }
        );

        // Another principal's grant on this part is not this principal's relevance.
        store
            .add_part_member(part.clone(), other_peer, read)
            .await?;
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part.clone(), Some(peer.clone()), 0).await?,
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
            .set_part_members(other_part, HashMap::from([(peer.clone(), read)]))
            .await?;
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part.clone(), Some(peer.clone()), 0).await?,
            PartDirtyCount {
                member_changes: 2,
                access_changes: 1,
            },
            "another part's rows must not be counted here"
        );

        // The comparison is strictly newer-than, so nothing clears the store's own
        // current revision. Derived rather than a literal: the cursor domain here is
        // the sqlite integer range, so a `u64::MAX` ceiling is not a legal cursor.
        let ceiling = store.latest_revision().await?;
        assert_eq!(
            HostPartStore::part_dirty_count(&store, part, Some(peer), ceiling).await?,
            PartDirtyCount::default(),
            "no row can be newer than the store's current revision"
        );
        Ok(())
    }

    /// Replay until the page has something to report.
    ///
    /// A page that carried nothing is not a verdict about the log: the replay's
    /// hold can expire before the subscription has produced an event, and the
    /// wire contract says the caller re-asks from the same position. Polling
    /// here keeps a test independent of how long a hold happened to last
    /// instead of pinning it to a wall-clock window that load can close.
    async fn replay_page_with_events(
        store: &SqlitePartStore,
        target: SubscriptionTarget,
        limit: u32,
        peer: &PeerKey,
    ) -> Res<PartPage> {
        const PAGE_HOLD: Duration = Duration::from_millis(50);
        loop {
            let outcome = store
                .replay_page(target.clone(), limit, peer.clone(), PAGE_HOLD)
                .await?;
            let ReplayPageOutcome::Events(page) = outcome else {
                panic!("expected a page, got {outcome:?}");
            };
            if !page.events.is_empty() || page.drained {
                return Ok(page);
            }
        }
    }

    /// A page is bounded, and its cursor resumes without replaying or skipping.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_replay_page_is_bounded_and_resumes() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://replay-page-cursor").await?;
        let part_id = test_part_id(241);
        let peer = PeerKey::new([242; 32]);
        store.ensure_part(part_id.clone()).await?;
        store
            .set_part_members(
                part_id.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;
        let mut cursors = Vec::new();
        for seed in 0..5u8 {
            let obj_id = test_obj_id(seed);
            cursors.push(
                put_frontier_event(
                    &store,
                    PartFrontierKey::Part {
                        obj_id: obj_id.clone(),
                        part_id: part_id.clone(),
                    },
                    PartEvent::Changed(ObjChanged {
                        cursor: 0,
                        part_ids: vec![part_id.clone()],
                        obj_id,
                        payload: serde_json::json!({"seed": seed}),
                    }),
                )
                .await?,
            );
        }

        let page = replay_page_with_events(
            &store,
            SubscriptionTarget::Part {
                part_id: part_id.clone(),
                cursor: 0,
            },
            2,
            &peer,
        )
        .await?;
        assert_eq!(page.events.len(), 2, "a page obeys its limit");
        assert_eq!(
            page.resume, cursors[1],
            "the resume point is the last delivered event"
        );
        assert!(
            !page.drained,
            "a page that stopped at its limit is not a caught-up verdict"
        );

        // Resuming from the page's resume picks up exactly where it stopped.
        // Poll until the replay reports caught-up: a page that stopped short is
        // not a verdict either, and the caller re-asks from its `resume`.
        let mut remaining = Vec::new();
        let mut cursor = page.resume;
        loop {
            let outcome = store
                .replay_page(
                    SubscriptionTarget::Part {
                        part_id: part_id.clone(),
                        cursor,
                    },
                    10,
                    peer.clone(),
                    Duration::from_millis(50),
                )
                .await?;
            let ReplayPageOutcome::Events(page) = outcome else {
                panic!("expected a page, got {outcome:?}");
            };
            if page.events.is_empty() && !page.drained {
                continue;
            }
            remaining.extend(page.events);
            cursor = page.resume;
            if page.drained {
                assert_eq!(
                    cursor, cursors[4],
                    "and it resumes past its last delivered event"
                );
                break;
            }
        }
        assert_eq!(remaining.len(), 3, "the remaining events, no replay");
        Ok(())
    }

    /// The three answers a page can give are distinct, and an empty page says so
    /// instead of standing in for a denial.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_replay_page_reports_unknown_unauthorized_and_empty() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://replay-page-outcomes").await?;
        let granted_part = test_part_id(243);
        let ungranted_part = test_part_id(244);
        let peer = PeerKey::new([245; 32]);
        store.ensure_part(granted_part.clone()).await?;
        store.ensure_part(ungranted_part.clone()).await?;
        store
            .set_part_members(
                granted_part.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;

        let unknown = store
            .replay_page(
                SubscriptionTarget::Part {
                    part_id: test_part_id(246),
                    cursor: 0,
                },
                8,
                peer.clone(),
                Duration::from_millis(50),
            )
            .await?;
        assert_eq!(unknown, ReplayPageOutcome::UnknownPart);

        let denied = store
            .replay_page(
                SubscriptionTarget::Part {
                    part_id: ungranted_part,
                    cursor: 0,
                },
                8,
                peer.clone(),
                Duration::from_millis(50),
            )
            .await?;
        assert_eq!(
            denied,
            ReplayPageOutcome::Unauthorized,
            "a part with no access row is denied, not reported as empty"
        );

        // A granted part with nothing to send is an empty page, and the hold
        // bounds how long the responder waits for something to arrive.
        let empty = store
            .replay_page(
                SubscriptionTarget::Part {
                    part_id: granted_part,
                    cursor: 0,
                },
                8,
                peer,
                Duration::from_millis(50),
            )
            .await?;
        let ReplayPageOutcome::Events(page) = empty else {
            panic!("expected a page, got {empty:?}");
        };
        assert!(page.events.is_empty(), "no events to send");
        assert!(page.drained, "an exhausted replay is the caught-up answer");
        assert!(
            page.resume > 0,
            "a drained page's claim carries the boundary its read scanned in full, so the next \
             request starts past the range it already covered instead of re-scanning it (got {})",
            page.resume
        );
        Ok(())
    }

    /// Filtering survives the move to pages: a peer reads the parts it was
    /// granted, and only those.
    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_replay_page_filters_to_readable_parts() -> Res<()> {
        use keyhive_core::access::Access;

        let store = test_store("big-sync-sqlite-test://replay-page-filter").await?;
        let readable_part = test_part_id(247);
        let other_part = test_part_id(248);
        let peer = PeerKey::new([249; 32]);
        store.ensure_part(readable_part.clone()).await?;
        store.ensure_part(other_part.clone()).await?;
        store
            .set_part_members(
                readable_part.clone(),
                HashMap::from([(peer.clone(), Access::Read)]),
            )
            .await?;
        store
            .set_part_members(
                other_part.clone(),
                HashMap::from([(PeerKey::new([250; 32]), Access::Read)]),
            )
            .await?;
        let obj_id = test_obj_id(251);
        for part_id in [readable_part.clone(), other_part] {
            put_frontier_event(
                &store,
                PartFrontierKey::Part {
                    obj_id: obj_id.clone(),
                    part_id: part_id.clone(),
                },
                PartEvent::Changed(ObjChanged {
                    cursor: 0,
                    part_ids: vec![part_id],
                    obj_id: obj_id.clone(),
                    payload: serde_json::json!({"value": 1}),
                }),
            )
            .await?;
        }

        let page = replay_page_with_events(
            &store,
            SubscriptionTarget::Part {
                part_id: readable_part.clone(),
                cursor: 0,
            },
            8,
            &peer,
        )
        .await?;
        assert_eq!(
            page.events.len(),
            1,
            "the granted part's event is delivered"
        );
        assert!(
            matches!(
                page.events.first(),
                Some(PartEvent::Changed(changed)) if changed.part_ids == vec![readable_part]
            ),
            "and it carries only the granted part"
        );
        Ok(())
    }
}
