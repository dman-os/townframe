use super::HostPartitionStore;
use crate::interlude::*;
use crate::{ScopeRef, ScopedIdResolver, ScopedObjRef, ScopedPartRef};

use big_sync_core::merkle::{BucketId, MerkleBucketSummary, MerkleFingerprintSeed, MerkleLeafItem};
use big_sync_core::part_store::{CursorIndex, ObjPayload};
use big_sync_core::rpc::{
    ListPartsError, PartEvent, PartPage, PartSummary, PartTransition, SubEvent, SubPartsRequest,
};
use big_sync_core::{Byte32Id, ObjId, PartId, PeerId};
use future_form::{FutureForm, Sendable};
use futures::future::BoxFuture;
use sqlx::Row;
use tokio::sync::mpsc;
#[cfg(test)]
use uuid::Uuid;

#[derive(Clone)]
pub struct SqlitePartStore {
    read_pool: sqlx::SqlitePool,
    write_pool: sqlx::SqlitePool,
    bus: Arc<std::sync::Mutex<HashMap<PartId, Vec<mpsc::UnboundedSender<SubEvent>>>>>,
}

impl SqlitePartStore {
    pub async fn new(read_pool: sqlx::SqlitePool, write_pool: sqlx::SqlitePool) -> Res<Self> {
        init_schema(&write_pool).await?;
        Ok(Self {
            read_pool,
            write_pool,
            bus: default(),
        })
    }

    async fn next_id(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>, key: &str) -> Res<u64> {
        let value: i64 = sqlx::query_scalar(
            "UPDATE big_sync_meta SET value = value + 1 WHERE key = ?1 RETURNING value",
        )
        .bind(key)
        .fetch_one(&mut **tx)
        .await?;
        Ok(u64::try_from(value).expect(ERROR_IMPOSSIBLE))
    }

    async fn next_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Res<CursorIndex> {
        Self::next_id(tx, "global_cursor").await
    }

    fn generated_id(domain: u8, value: u64) -> Byte32Id {
        let mut bytes = [0; 32];
        bytes[0] = domain;
        bytes[1..9].copy_from_slice(&value.to_be_bytes());
        Byte32Id::new(bytes)
    }

    fn id_blob(id: Byte32Id) -> Vec<u8> {
        id.into_bytes().to_vec()
    }

    fn part_blob(id: PartId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    fn obj_blob(id: ObjId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    fn peer_blob(id: PeerId) -> Vec<u8> {
        Self::id_blob(id.0)
    }

    fn part_from_blob(blob: Vec<u8>) -> PartId {
        PartId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    fn obj_from_blob(blob: Vec<u8>) -> ObjId {
        ObjId(Byte32Id::new(blob.try_into().expect(ERROR_IMPOSSIBLE)))
    }

    async fn scope_id_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        scope: &ScopeRef,
    ) -> Res<i64> {
        let scope_url = scope.0.as_str();
        if let Some(scope_id) = sqlx::query_scalar::<_, i64>(
            "SELECT scope_id FROM big_sync_scopes WHERE scope_url = ?1",
        )
        .bind(scope_url)
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(scope_id);
        }

        sqlx::query("INSERT INTO big_sync_scopes(scope_url) VALUES (?1)")
            .bind(scope_url)
            .execute(&mut **tx)
            .await?;
        let scope_id: i64 =
            sqlx::query_scalar("SELECT scope_id FROM big_sync_scopes WHERE scope_url = ?1")
                .bind(scope_url)
                .fetch_one(&mut **tx)
                .await?;
        Ok(scope_id)
    }

    async fn scoped_part_from_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartId,
    ) -> Res<ScopedPartRef> {
        let row = sqlx::query(
            "SELECT scopes.scope_url, parts.scoped_part_id
             FROM big_sync_parts parts
             JOIN big_sync_scopes scopes ON scopes.scope_id = parts.scope_id
             WHERE parts.part_id = ?1",
        )
        .bind(Self::part_blob(part_id))
        .fetch_one(&mut **tx)
        .await?;
        let scope_url: String = row.try_get("scope_url")?;
        let scoped_part_id: String = row.try_get("scoped_part_id")?;
        Ok(ScopedPartRef::new(
            ScopeRef::new(scope_url.parse()?),
            Arc::<str>::from(scoped_part_id),
        ))
    }

    async fn scoped_obj_from_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj_id: ObjId,
    ) -> Res<ScopedObjRef> {
        let row = sqlx::query(
            "SELECT scopes.scope_url, objs.scoped_obj_id
             FROM big_sync_objs objs
             JOIN big_sync_scopes scopes ON scopes.scope_id = objs.scope_id
             WHERE objs.obj_id = ?1",
        )
        .bind(Self::obj_blob(obj_id))
        .fetch_one(&mut **tx)
        .await?;
        let scope_url: String = row.try_get("scope_url")?;
        let scoped_obj_id: String = row.try_get("scoped_obj_id")?;
        Ok(ScopedObjRef::new(
            ScopeRef::new(scope_url.parse()?),
            Arc::<str>::from(scoped_obj_id),
        ))
    }

    async fn resolve_part_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part: &ScopedPartRef,
    ) -> Res<PartId> {
        let scope_id = Self::scope_id_in_tx(tx, &part.scope).await?;
        if let Some(part_id) = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT part_id FROM big_sync_parts WHERE scope_id = ?1 AND scoped_part_id = ?2",
        )
        .bind(scope_id)
        .bind(part.part.as_ref())
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(Self::part_from_blob(part_id));
        }

        let id_value = Self::next_id(tx, "next_part_id").await?;
        let part_id = PartId(Self::generated_id(1, id_value));
        sqlx::query(
            "INSERT INTO big_sync_parts(part_id, scope_id, scoped_part_id, latest_cursor)
             VALUES (?1, ?2, ?3, 0)",
        )
        .bind(Self::part_blob(part_id))
        .bind(scope_id)
        .bind(part.part.as_ref())
        .execute(&mut **tx)
        .await?;
        Ok(part_id)
    }

    async fn resolve_obj_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        obj: &ScopedObjRef,
    ) -> Res<ObjId> {
        let scope_id = Self::scope_id_in_tx(tx, &obj.scope).await?;
        if let Some(obj_id) = sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT obj_id FROM big_sync_objs WHERE scope_id = ?1 AND scoped_obj_id = ?2",
        )
        .bind(scope_id)
        .bind(obj.obj.as_ref())
        .fetch_optional(&mut **tx)
        .await?
        {
            return Ok(Self::obj_from_blob(obj_id));
        }

        #[cfg(test)]
        let obj_id = crate::part_store::test_scoped_obj_id(obj);
        #[cfg(not(test))]
        let id_value = Self::next_id(tx, "next_obj_id").await?;
        #[cfg(not(test))]
        let obj_id = ObjId(Self::generated_id(2, id_value));
        sqlx::query(
            "INSERT INTO big_sync_objs(obj_id, scope_id, scoped_obj_id, payload_json)
             VALUES (?1, ?2, ?3, NULL)",
        )
        .bind(Self::obj_blob(obj_id))
        .bind(scope_id)
        .bind(obj.obj.as_ref())
        .execute(&mut **tx)
        .await?;
        Ok(obj_id)
    }

    async fn queue_transition(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        part_id: PartId,
        obj_id: ObjId,
    ) -> Res<PartTransition> {
        let cursor = Self::next_cursor(tx).await?;
        let cursor_i64 = i64::try_from(cursor).expect(ERROR_IMPOSSIBLE);
        sqlx::query("UPDATE big_sync_parts SET latest_cursor = ?1 WHERE part_id = ?2")
            .bind(cursor_i64)
            .bind(Self::part_blob(part_id))
            .execute(&mut **tx)
            .await?;
        Ok(PartTransition {
            cursor,
            part_id,
            obj_id,
        })
    }

    fn publish(&self, events: Vec<SubEvent>) {
        let mut bus = self.bus.lock().expect(ERROR_MUTEX);
        for event in events {
            let part_id = match &event {
                SubEvent::Upserted(transition) | SubEvent::Deleted(transition) => {
                    transition.part_id
                }
                SubEvent::ReplayComplete => continue,
            };
            let Some(subs) = bus.get_mut(&part_id) else {
                continue;
            };
            subs.retain(|sub| sub.send(event.clone()).is_ok());
        }
    }
}

async fn init_schema(pool: &sqlx::SqlitePool) -> Res<()> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value INTEGER NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;
    for key in ["global_cursor", "next_part_id", "next_obj_id"] {
        sqlx::query("INSERT OR IGNORE INTO big_sync_meta(key, value) VALUES (?1, 0)")
            .bind(key)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_scopes (
            scope_id INTEGER PRIMARY KEY AUTOINCREMENT,
            scope_url TEXT NOT NULL UNIQUE
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_parts (
            part_id BLOB PRIMARY KEY NOT NULL,
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            scoped_part_id TEXT NOT NULL,
            latest_cursor INTEGER NOT NULL DEFAULT 0,
            UNIQUE(scope_id, scoped_part_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_objs (
            obj_id BLOB PRIMARY KEY NOT NULL,
            scope_id INTEGER NOT NULL REFERENCES big_sync_scopes(scope_id),
            scoped_obj_id TEXT NOT NULL,
            payload_json TEXT,
            UNIQUE(scope_id, scoped_obj_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_members (
            part_id BLOB NOT NULL REFERENCES big_sync_parts(part_id),
            obj_id BLOB NOT NULL REFERENCES big_sync_objs(obj_id),
            changed_at INTEGER NOT NULL,
            removed_at INTEGER,
            latest_cursor INTEGER NOT NULL,
            PRIMARY KEY(part_id, obj_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    let member_columns = sqlx::query("PRAGMA table_info(big_sync_members)")
        .fetch_all(&mut *tx)
        .await?;
    let has_latest_cursor = member_columns
        .iter()
        .any(|row| row.try_get::<String, _>("name").expect(ERROR_IMPOSSIBLE) == "latest_cursor");
    if !has_latest_cursor {
        sqlx::query(
            "ALTER TABLE big_sync_members
             ADD COLUMN latest_cursor INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE big_sync_members
             SET latest_cursor = CASE
                WHEN removed_at IS NOT NULL AND removed_at >= changed_at THEN removed_at
                ELSE changed_at
             END",
        )
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS big_sync_members_part_latest_idx
         ON big_sync_members(part_id, latest_cursor)",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS big_sync_members_obj_idx
         ON big_sync_members(obj_id)",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("DROP TABLE IF EXISTS big_sync_events")
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS big_sync_peer_cursors (
            peer_id BLOB NOT NULL,
            part_id BLOB NOT NULL REFERENCES big_sync_parts(part_id),
            cursor INTEGER NOT NULL,
            PRIMARY KEY(peer_id, part_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

#[async_trait]
impl ScopedIdResolver for SqlitePartStore {
    async fn resolve_part(&self, part: &ScopedPartRef) -> Res<PartId> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let part_id = self.resolve_part_in_tx(&mut tx, part).await?;
        tx.commit().await?;
        Ok(part_id)
    }

    async fn resolve_obj(&self, obj: &ScopedObjRef) -> Res<ObjId> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let obj_id = self.resolve_obj_in_tx(&mut tx, obj).await?;
        tx.commit().await?;
        Ok(obj_id)
    }

    async fn scoped_part(&self, part_id: PartId) -> Res<ScopedPartRef> {
        let mut tx = self.read_pool.begin().await?;
        Self::scoped_part_from_tx(&mut tx, part_id).await
    }

    async fn scoped_obj(&self, obj_id: ObjId) -> Res<ScopedObjRef> {
        let mut tx = self.read_pool.begin().await?;
        Self::scoped_obj_from_tx(&mut tx, obj_id).await
    }
}

#[async_trait]
impl HostPartitionStore for SqlitePartStore {
    async fn summarize_parts(
        &self,
        parts: HashSet<PartId>,
    ) -> Res<Result<HashMap<PartId, PartSummary>, ListPartsError>> {
        let mut out = HashMap::new();
        for part_id in parts {
            let row = sqlx::query("SELECT latest_cursor FROM big_sync_parts WHERE part_id = ?1")
                .bind(Self::part_blob(part_id))
                .fetch_optional(&self.read_pool)
                .await?;
            let Some(row) = row else {
                return Ok(Err(ListPartsError::UnkownParts {
                    unkown_parts: vec![part_id],
                }));
            };
            let latest_cursor: i64 = row.try_get("latest_cursor")?;
            let member_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM big_sync_members
                 WHERE part_id = ?1 AND removed_at IS NULL",
            )
            .bind(Self::part_blob(part_id))
            .fetch_one(&self.read_pool)
            .await?;
            out.insert(
                part_id,
                PartSummary {
                    latest_cursor: u64::try_from(latest_cursor).expect(ERROR_IMPOSSIBLE),
                    member_count: u64::try_from(member_count).expect(ERROR_IMPOSSIBLE),
                },
            );
        }
        Ok(Ok(out))
    }

    async fn member_count(&self, part_id: PartId) -> Res<u64> {
        let member_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE part_id = ?1 AND removed_at IS NULL",
        )
        .bind(Self::part_blob(part_id))
        .fetch_one(&self.read_pool)
        .await?;
        Ok(u64::try_from(member_count).expect(ERROR_IMPOSSIBLE))
    }

    async fn obj_payload(&self, obj_id: ObjId) -> Res<Option<ObjPayload>> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload_json FROM big_sync_objs WHERE obj_id = ?1")
                .bind(Self::obj_blob(obj_id))
                .fetch_optional(&self.read_pool)
                .await?
                .flatten();
        payload
            .map(|payload| serde_json::from_str(&payload).wrap_err(ERROR_JSON))
            .transpose()
    }

    async fn upsert_obj(&self, obj_id: ObjId, payload: ObjPayload, parts: Vec<PartId>) -> Res<()> {
        let payload_json = serde_json::to_string(&payload).wrap_err(ERROR_JSON)?;
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("UPDATE big_sync_objs SET payload_json = ?1 WHERE obj_id = ?2")
            .bind(payload_json)
            .bind(Self::obj_blob(obj_id))
            .execute(&mut *tx)
            .await?;

        let mut events = Vec::new();
        for part_id in parts {
            let transition = Self::queue_transition(&mut tx, part_id, obj_id).await?;
            sqlx::query(
                "INSERT INTO big_sync_members(part_id, obj_id, changed_at, removed_at, latest_cursor)
                 VALUES (?1, ?2, ?3, NULL, ?3)
                 ON CONFLICT(part_id, obj_id) DO UPDATE SET
                    changed_at = excluded.changed_at,
                    removed_at = NULL,
                    latest_cursor = excluded.latest_cursor",
            )
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .bind(i64::try_from(transition.cursor).expect(ERROR_IMPOSSIBLE))
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Upserted(transition));
        }
        tx.commit().await?;
        self.publish(events);
        Ok(())
    }

    async fn obj_parts(&self, obj_id: ObjId) -> Res<Vec<PartId>> {
        let rows = sqlx::query(
            "SELECT part_id FROM big_sync_members
             WHERE obj_id = ?1 AND removed_at IS NULL",
        )
        .bind(Self::obj_blob(obj_id))
        .fetch_all(&self.read_pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| Self::part_from_blob(row.try_get("part_id").expect(ERROR_IMPOSSIBLE)))
            .collect())
    }

    async fn add_obj_to_parts(&self, obj_id: ObjId, parts: Vec<PartId>) -> Res<()> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let mut events = Vec::new();
        for part_id in parts {
            let current = sqlx::query(
                "SELECT removed_at FROM big_sync_members WHERE part_id = ?1 AND obj_id = ?2",
            )
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .fetch_optional(&mut *tx)
            .await?;
            if current
                .as_ref()
                .and_then(|row| {
                    row.try_get::<Option<i64>, _>("removed_at")
                        .expect(ERROR_JSON)
                })
                .is_none()
                && current.is_some()
            {
                continue;
            }
            let transition = Self::queue_transition(&mut tx, part_id, obj_id).await?;
            sqlx::query(
                "INSERT INTO big_sync_members(part_id, obj_id, changed_at, removed_at, latest_cursor)
                 VALUES (?1, ?2, ?3, NULL, ?3)
                 ON CONFLICT(part_id, obj_id) DO UPDATE SET
                    changed_at = excluded.changed_at,
                    removed_at = NULL,
                    latest_cursor = excluded.latest_cursor",
            )
            .bind(Self::part_blob(part_id))
            .bind(Self::obj_blob(obj_id))
            .bind(i64::try_from(transition.cursor).expect(ERROR_IMPOSSIBLE))
            .execute(&mut *tx)
            .await?;
            events.push(SubEvent::Upserted(transition));
        }
        tx.commit().await?;
        self.publish(events);
        Ok(())
    }

    async fn remove_obj_from_part(&self, obj_id: ObjId, part_id: PartId) -> Res<()> {
        let mut tx = self.write_pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = sqlx::query(
            "SELECT removed_at FROM big_sync_members WHERE part_id = ?1 AND obj_id = ?2",
        )
        .bind(Self::part_blob(part_id))
        .bind(Self::obj_blob(obj_id))
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = current else {
            tx.commit().await?;
            return Ok(());
        };
        let removed_at: Option<i64> = row.try_get("removed_at")?;
        if removed_at.is_some() {
            tx.commit().await?;
            return Ok(());
        }

        let transition = Self::queue_transition(&mut tx, part_id, obj_id).await?;
        sqlx::query(
            "UPDATE big_sync_members
             SET removed_at = ?1, latest_cursor = ?1
             WHERE part_id = ?2 AND obj_id = ?3",
        )
        .bind(i64::try_from(transition.cursor).expect(ERROR_IMPOSSIBLE))
        .bind(Self::part_blob(part_id))
        .bind(Self::obj_blob(obj_id))
        .execute(&mut *tx)
        .await?;

        let live_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM big_sync_members
             WHERE obj_id = ?1 AND removed_at IS NULL",
        )
        .bind(Self::obj_blob(obj_id))
        .fetch_one(&mut *tx)
        .await?;
        if live_count == 0 {
            sqlx::query("UPDATE big_sync_objs SET payload_json = NULL WHERE obj_id = ?1")
                .bind(Self::obj_blob(obj_id))
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        self.publish(vec![SubEvent::Deleted(transition)]);
        Ok(())
    }

    async fn get_peer_part_cursor(&self, peer_id: PeerId, part_id: PartId) -> Res<CursorIndex> {
        let cursor: Option<i64> = sqlx::query_scalar(
            "SELECT cursor FROM big_sync_peer_cursors WHERE peer_id = ?1 AND part_id = ?2",
        )
        .bind(Self::peer_blob(peer_id))
        .bind(Self::part_blob(part_id))
        .fetch_optional(&self.read_pool)
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
        sqlx::query(
            "INSERT INTO big_sync_peer_cursors(peer_id, part_id, cursor)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(peer_id, part_id) DO UPDATE SET cursor = excluded.cursor",
        )
        .bind(Self::peer_blob(peer_id))
        .bind(Self::part_blob(part_id))
        .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn list_events(
        &self,
        parts: HashSet<PartId>,
        cursor: CursorIndex,
        limit: u32,
    ) -> Res<Result<HashMap<PartId, PartPage>, ListPartsError>> {
        let summaries = self.summarize_parts(parts.clone()).await?;
        if let Err(err) = summaries {
            return Ok(Err(err));
        }

        let mut out = HashMap::new();
        for part_id in parts {
            let rows = sqlx::query(
                "SELECT obj_id, changed_at, removed_at, latest_cursor FROM big_sync_members
                 WHERE part_id = ?1 AND latest_cursor > ?2
                 ORDER BY latest_cursor ASC
                 LIMIT ?3",
            )
            .bind(Self::part_blob(part_id))
            .bind(i64::try_from(cursor).expect(ERROR_IMPOSSIBLE))
            .bind(i64::from(limit) + 1)
            .fetch_all(&self.read_pool)
            .await?;
            let mut next_cursor = None;
            let mut events = Vec::new();
            for row in rows {
                let row_cursor: i64 = row.try_get("latest_cursor")?;
                if events.len() >= usize::try_from(limit).expect(ERROR_IMPOSSIBLE) {
                    next_cursor = Some(u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE));
                    break;
                }
                let changed_at: i64 = row.try_get("changed_at")?;
                let removed_at: Option<i64> = row.try_get("removed_at")?;
                let transition = PartTransition {
                    cursor: u64::try_from(row_cursor).expect(ERROR_IMPOSSIBLE),
                    part_id,
                    obj_id: Self::obj_from_blob(row.try_get("obj_id")?),
                };
                let removed_after_change = removed_at
                    .is_some_and(|removed_at| removed_at >= changed_at && removed_at == row_cursor);
                events.push(if removed_after_change {
                    PartEvent::Deleted(transition)
                } else {
                    PartEvent::Upserted(transition)
                });
            }
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

    async fn subscribe(
        &self,
        reqs: SubPartsRequest,
    ) -> Res<Result<mpsc::UnboundedReceiver<SubEvent>, ListPartsError>> {
        let parts: HashSet<_> = reqs.parts.iter().map(|req| req.part_id).collect();
        let summaries = self.summarize_parts(parts.clone()).await?;
        if let Err(err) = summaries {
            return Ok(Err(err));
        }

        let cursor = reqs
            .parts
            .iter()
            .map(|req| req.cursor)
            .min()
            .unwrap_or_default();
        let (tx, rx) = mpsc::unbounded_channel();
        let page = self
            .list_events(parts.clone(), cursor, u32::MAX)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        for (_, part_page) in page {
            for event in part_page.events {
                let sub_event = match event {
                    PartEvent::Upserted(transition) => SubEvent::Upserted(transition),
                    PartEvent::Deleted(transition) => SubEvent::Deleted(transition),
                };
                if tx.send(sub_event).is_err() {
                    return Ok(Ok(rx));
                }
            }
        }
        if tx.send(SubEvent::ReplayComplete).is_err() {
            return Ok(Ok(rx));
        }

        let mut bus = self.bus.lock().expect(ERROR_MUTEX);
        for part_id in parts {
            bus.entry(part_id).or_default().push(tx.clone());
        }
        Ok(Ok(rx))
    }

    async fn merkle_bucket(&self, _part_id: PartId, _path: BucketId) -> Res<MerkleBucketSummary> {
        unimplemented!("sqlite merkle store is not implemented yet")
    }

    async fn merkle_child_buckets(
        &self,
        _part_id: PartId,
        _path: BucketId,
        _summary_budget: u16,
    ) -> Res<Vec<MerkleBucketSummary>> {
        unimplemented!("sqlite merkle store is not implemented yet")
    }

    async fn merkle_leaf_items(
        &self,
        _part_id: PartId,
        _path: BucketId,
        _seed: MerkleFingerprintSeed,
    ) -> Res<Vec<MerkleLeafItem>> {
        unimplemented!("sqlite merkle store is not implemented yet")
    }
}

impl big_sync_core::part_store::PartStore<Sendable> for SqlitePartStore {
    fn member_count<'a>(&'a self, part_id: PartId) -> BoxFuture<'a, u64> {
        Sendable::from_future(async move {
            HostPartitionStore::member_count(self, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_payload<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Option<ObjPayload>> {
        Sendable::from_future(async move {
            HostPartitionStore::obj_payload(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn upsert_obj<'a>(
        &'a self,
        obj_id: ObjId,
        payload: &ObjPayload,
        parts: &[PartId],
    ) -> BoxFuture<'a, ()> {
        let payload = payload.clone();
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartitionStore::upsert_obj(self, obj_id, payload, parts)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn obj_parts<'a>(&'a self, obj_id: ObjId) -> BoxFuture<'a, Vec<PartId>> {
        Sendable::from_future(async move {
            HostPartitionStore::obj_parts(self, obj_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn add_obj_to_parts<'a>(&'a self, obj_id: ObjId, parts: &[PartId]) -> BoxFuture<'a, ()> {
        let parts = parts.to_vec();
        Sendable::from_future(async move {
            HostPartitionStore::add_obj_to_parts(self, obj_id, parts)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn remove_obj_from_part<'a>(&'a self, obj_id: ObjId, part_id: PartId) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartitionStore::remove_obj_from_part(self, obj_id, part_id)
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
            HostPartitionStore::get_peer_part_cursor(self, peer_id, part_id)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn set_peer_part_cursor<'a>(
        &'a self,
        peer_id: PeerId,
        part_id: PartId,
        cursor: CursorIndex,
    ) -> BoxFuture<'a, ()> {
        Sendable::from_future(async move {
            HostPartitionStore::set_peer_part_cursor(self, peer_id, part_id, cursor)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn merkle_bucket<'a>(
        &'a self,
        part_id: PartId,
        path: &BucketId,
    ) -> BoxFuture<'a, MerkleBucketSummary> {
        let path = path.clone();
        Sendable::from_future(async move {
            HostPartitionStore::merkle_bucket(self, part_id, path)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn merkle_child_buckets<'a>(
        &'a self,
        part_id: PartId,
        path: &BucketId,
        summary_budget: u16,
    ) -> BoxFuture<'a, Vec<MerkleBucketSummary>> {
        let path = path.clone();
        Sendable::from_future(async move {
            HostPartitionStore::merkle_child_buckets(self, part_id, path, summary_budget)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }

    fn merkle_leaf_items<'a>(
        &'a self,
        part_id: PartId,
        path: &BucketId,
        seed: MerkleFingerprintSeed,
    ) -> BoxFuture<'a, Vec<MerkleLeafItem>> {
        let path = path.clone();
        Sendable::from_future(async move {
            HostPartitionStore::merkle_leaf_items(self, part_id, path, seed)
                .await
                .expect(ERROR_IMPOSSIBLE)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync_core::part_store::contract;
    use std::str::FromStr;

    async fn test_pool() -> Res<sqlx::SqlitePool> {
        let db_path = std::env::temp_dir().join(format!("big_sync-{}.sqlite", Uuid::new_v4()));
        let db_url = format!("sqlite://{}", db_path.display());
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&db_url)?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .locking_mode(sqlx::sqlite::SqliteLockingMode::Exclusive)
            .create_if_missing(true);
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await?;
        Ok(pool)
    }

    async fn test_store() -> Res<SqlitePartStore> {
        let pool = test_pool().await?;
        SqlitePartStore::new(pool.clone(), pool).await
    }

    fn scope() -> ScopeRef {
        ScopeRef::new(Url::parse("big-sync-sqlite-test://repo").expect(ERROR_IMPOSSIBLE))
    }

    fn alt_scope() -> ScopeRef {
        ScopeRef::new(Url::parse("big-sync-sqlite-test://other-repo").expect(ERROR_IMPOSSIBLE))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_membership_semantics() -> Res<()> {
        let store = test_store().await?;
        let part_id = store
            .resolve_part(&ScopedPartRef::new(scope(), "core.docs"))
            .await?;
        let obj_id = store
            .resolve_obj(&ScopedObjRef::new(scope(), "docs/root"))
            .await?;
        contract::assert_membership_semantics(&store, part_id, obj_id).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_add_obj_to_parts_is_idempotent() -> Res<()> {
        let store = test_store().await?;
        let part_id = store
            .resolve_part(&ScopedPartRef::new(scope(), "core.docs"))
            .await?;
        let obj_id = store
            .resolve_obj(&ScopedObjRef::new(scope(), "docs/root"))
            .await?;
        contract::assert_add_obj_to_parts_is_idempotent(&store, part_id, obj_id).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_contract_peer_cursor_roundtrip() -> Res<()> {
        let store = test_store().await?;
        let part_id = store
            .resolve_part(&ScopedPartRef::new(scope(), "core.docs"))
            .await?;
        contract::assert_peer_cursor_roundtrip(&store, PeerId(Byte32Id::new([42; 32])), part_id)
            .await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_part_store_root_bucket_contract() -> Res<()> {
        let store = test_store().await?;
        let part_id = store
            .resolve_part(&ScopedPartRef::new(scope(), "core.docs"))
            .await?;
        let seed = big_sync_core::FingerprintSeed::new(1, 2);

        let mut obj_ids = Vec::new();
        for ii in 0..5u8 {
            let obj = ScopedObjRef::new(scope(), format!("docs/{ii}"));
            let obj_id = store.resolve_obj(&obj).await?;
            HostPartitionStore::upsert_obj(
                &store,
                obj_id,
                serde_json::json!({"phase": "present", "ii": ii}),
                vec![part_id],
            )
            .await?;
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
        HostPartitionStore::remove_obj_from_part(&store, removed_obj_id, part_id).await?;
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
        let store = test_store().await?;
        let part_id = store
            .resolve_part(&ScopedPartRef::new(scope(), "core.docs"))
            .await?;
        let obj_id = store
            .resolve_obj(&ScopedObjRef::new(scope(), "docs/root"))
            .await?;

        HostPartitionStore::upsert_obj(
            &store,
            obj_id,
            serde_json::json!({"phase": "created"}),
            vec![part_id],
        )
        .await?;
        HostPartitionStore::remove_obj_from_part(&store, obj_id, part_id).await?;

        let deleted_page = HostPartitionStore::list_events(&store, HashSet::from([part_id]), 0, 10)
            .await?
            .expect(ERROR_IMPOSSIBLE);
        let deleted_events = &deleted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(deleted_events.len(), 1);
        let PartEvent::Deleted(transition) = &deleted_events[0] else {
            panic!("expected deleted event");
        };
        assert_eq!(transition.cursor, 2);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);

        HostPartitionStore::upsert_obj(
            &store,
            obj_id,
            serde_json::json!({"phase": "recreated"}),
            vec![part_id],
        )
        .await?;

        let upserted_page =
            HostPartitionStore::list_events(&store, HashSet::from([part_id]), 0, 10)
                .await?
                .expect(ERROR_IMPOSSIBLE);
        let upserted_events = &upserted_page.get(&part_id).expect(ERROR_IMPOSSIBLE).events;
        assert_eq!(upserted_events.len(), 1);
        let PartEvent::Upserted(transition) = &upserted_events[0] else {
            panic!("expected upserted event");
        };
        assert_eq!(transition.cursor, 3);
        assert_eq!(transition.part_id, part_id);
        assert_eq!(transition.obj_id, obj_id);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_scoped_refs_are_isolated_by_scope() -> Res<()> {
        let store = test_store().await?;
        let repo_part = ScopedPartRef::new(scope(), "core.docs");
        let other_repo_part = ScopedPartRef::new(alt_scope(), "core.docs");
        let repo_obj = ScopedObjRef::new(scope(), "docs/root");
        let other_repo_obj = ScopedObjRef::new(alt_scope(), "docs/root");

        let repo_part_id = store.resolve_part(&repo_part).await?;
        let other_repo_part_id = store.resolve_part(&other_repo_part).await?;
        let repo_obj_id = store.resolve_obj(&repo_obj).await?;
        let other_repo_obj_id = store.resolve_obj(&other_repo_obj).await?;

        assert_ne!(repo_part_id, other_repo_part_id);
        assert_ne!(repo_obj_id, other_repo_obj_id);
        assert_eq!(store.scoped_part(repo_part_id).await?, repo_part);
        assert_eq!(
            store.scoped_part(other_repo_part_id).await?,
            other_repo_part
        );
        assert_eq!(store.scoped_obj(repo_obj_id).await?, repo_obj);
        assert_eq!(store.scoped_obj(other_repo_obj_id).await?, other_repo_obj);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sqlite_migrates_old_member_cursor_shape() -> Res<()> {
        let pool = test_pool().await?;
        sqlx::query(
            "CREATE TABLE big_sync_members (
                part_id BLOB NOT NULL,
                obj_id BLOB NOT NULL,
                changed_at INTEGER NOT NULL,
                removed_at INTEGER,
                PRIMARY KEY(part_id, obj_id)
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE big_sync_events (
                cursor INTEGER PRIMARY KEY NOT NULL,
                part_id BLOB NOT NULL,
                event_kind TEXT NOT NULL,
                obj_id BLOB NOT NULL
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT INTO big_sync_members(part_id, obj_id, changed_at, removed_at)
             VALUES (?1, ?2, 3, 8)",
        )
        .bind(vec![1_u8; 32])
        .bind(vec![2_u8; 32])
        .execute(&pool)
        .await?;

        let _store = SqlitePartStore::new(pool.clone(), pool.clone()).await?;
        let latest_cursor: i64 = sqlx::query_scalar("SELECT latest_cursor FROM big_sync_members")
            .fetch_one(&pool)
            .await?;
        let old_event_table_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*)
             FROM sqlite_master
             WHERE type = 'table' AND name = 'big_sync_events'",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(latest_cursor, 8);
        assert_eq!(old_event_table_count, 0);
        Ok(())
    }
}
