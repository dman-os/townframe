#![allow(clippy::disallowed_names)]
use super::sqlite_read::{SqliteReadError, SqliteReadSource, open_sqlite_reader};
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierReadLimits, FrontierRevision, KeyedFrontier, KeyedFrontierError,
    KeyedFrontierReader, KeyedFrontierResult, KeyedFrontierTransaction, TransactionIsolation,
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Notify;
use utils_rs::prelude::async_trait;

pub trait SqliteFrontierCodec: Clone + Send + Sync + 'static {
    type Key: Ord + Clone + Send + Sync + 'static;
    type Value: Clone + Send + Sync + 'static;
    fn encode_key(&self, key: &Self::Key) -> Vec<u8>;
    fn decode_key(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Key, Box<dyn std::error::Error + Send + Sync>>;
    fn encode_value(&self, value: &Self::Value) -> Vec<u8>;
    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Value, Box<dyn std::error::Error + Send + Sync>>;
    fn namespace(&self, _key: &Self::Key) -> Option<Vec<u8>> {
        None
    }
}

#[derive(Debug, Clone)]
pub enum SqliteFrontierSelector<K> {
    All { after: FrontierRevision },
    Keys(BTreeMap<K, FrontierRevision>),
    Namespaces(BTreeMap<Vec<u8>, FrontierRevision>),
}

#[derive(Debug, Clone)]
pub(crate) struct GenericRow {
    key: Vec<u8>,
    revision: FrontierRevision,
    value: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct SqliteKeyedFrontier<C: SqliteFrontierCodec> {
    pub(crate) read_pool: SqlitePool,
    pub(crate) write_pool: SqlitePool,
    pub(crate) frontier_id: Arc<str>,
    pub(crate) changed: Arc<Notify>,
    pub(crate) codec: C,
}

impl<C: SqliteFrontierCodec> SqliteKeyedFrontier<C> {
    pub async fn new(
        read_pool: SqlitePool,
        write_pool: SqlitePool,
        frontier_id: impl Into<Arc<str>>,
        codec: C,
        changed: Arc<Notify>,
    ) -> KeyedFrontierResult<Self> {
        let frontier_id: Arc<str> = frontier_id.into();
        sqlx::query("CREATE TABLE IF NOT EXISTS generic_frontier_meta (frontier_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)")
            .execute(&write_pool).await.map_err(backend)?;
        sqlx::query(
            "INSERT OR IGNORE INTO generic_frontier_meta (frontier_id, revision) VALUES (?, 0)",
        )
        .bind(frontier_id.as_ref())
        .execute(&write_pool)
        .await
        .map_err(backend)?;
        sqlx::query("CREATE TABLE IF NOT EXISTS generic_frontier_entries (frontier_id TEXT NOT NULL, namespace BLOB, key BLOB NOT NULL, revision INTEGER NOT NULL, value BLOB, PRIMARY KEY(frontier_id, key))")
            .execute(&write_pool).await.map_err(backend)?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_generic_frontier_revision ON generic_frontier_entries (frontier_id, revision)")
            .execute(&write_pool).await.map_err(backend)?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_generic_frontier_namespace_revision ON generic_frontier_entries (frontier_id, namespace, revision)")
            .execute(&write_pool).await.map_err(backend)?;
        Ok(Self {
            read_pool,
            write_pool,
            frontier_id,
            changed,
            codec,
        })
    }

    pub async fn begin(&self) -> KeyedFrontierResult<SqliteKeyedFrontierTransaction<'_, C>> {
        let tx = self
            .write_pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(backend)?;
        Ok(self.begin_with_context(tx))
    }

    pub async fn latest_revision(&self) -> KeyedFrontierResult<FrontierRevision> {
        let revision: i64 =
            sqlx::query_scalar("SELECT revision FROM generic_frontier_meta WHERE frontier_id = ?")
                .bind(self.frontier_id.as_ref())
                .fetch_one(&self.read_pool)
                .await
                .map_err(backend)?;
        u64::try_from(revision).map_err(backend)
    }

    pub fn begin_with_context<'a>(
        &'a self,
        transaction: Transaction<'a, Sqlite>,
    ) -> SqliteKeyedFrontierTransaction<'a, C> {
        SqliteKeyedFrontierTransaction {
            transaction: Some(transaction),
            frontier_id: Arc::clone(&self.frontier_id),
            codec: self.codec.clone(),
            changed: Arc::clone(&self.changed),
            staged: BTreeMap::new(),
            reserved_revision: None,
        }
    }

    /// Apply a mutation batch inside a caller-owned SQLite transaction. This
    /// is the projection adapter used when a keyed frontier and another
    /// durable state machine must commit as one unit.
    pub async fn apply_in_context<'a>(
        &self,
        tx: &mut Transaction<'a, Sqlite>,
        mutations: impl IntoIterator<Item = (C::Key, Option<C::Value>)>,
    ) -> KeyedFrontierResult<FrontierRevision> {
        let mutations: Vec<_> = mutations.into_iter().collect();
        let frontier_id = Arc::clone(&self.frontier_id);
        let revision: i64 = sqlx::query_scalar(
            "UPDATE generic_frontier_meta SET revision = revision + 1 WHERE frontier_id = ? RETURNING revision",
        )
        .bind(frontier_id.as_ref())
        .fetch_one(&mut **tx)
        .await
        .map_err(backend)?;
        let revision = u64::try_from(revision).map_err(backend)?;
        for (key, value) in mutations {
            let namespace = self.codec.namespace(&key);
            let encoded_key = self.codec.encode_key(&key);
            let encoded_value = value.map(|value| self.codec.encode_value(&value));
            sqlx::query("INSERT INTO generic_frontier_entries (frontier_id, namespace, key, revision, value) VALUES (?, ?, ?, ?, ?) ON CONFLICT(frontier_id, key) DO UPDATE SET namespace = excluded.namespace, revision = excluded.revision, value = excluded.value")
                .bind(frontier_id.as_ref())
                .bind(namespace)
                .bind(encoded_key)
                .bind(i64::try_from(revision).map_err(backend)?)
                .bind(encoded_value)
                .execute(&mut **tx)
                .await
                .map_err(backend)?;
        }
        Ok(revision)
    }

    /// Notify readers after a caller-owned transaction has committed.
    pub fn notify_changed(&self) {
        self.changed.notify_waiters();
    }
}

impl<C: SqliteFrontierCodec> SqliteReadSource for SqliteKeyedFrontier<C> {
    type Selector = SqliteFrontierSelector<C::Key>;
    type Row = GenericRow;
    type Key = C::Key;
    type Value = C::Value;
    fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }
    fn scope_id(&self) -> i64 {
        0
    }
    fn changed(&self) -> &Notify {
        &self.changed
    }
    fn initial_after(&self, selector: &Self::Selector) -> FrontierRevision {
        match selector {
            SqliteFrontierSelector::All { after } => *after,
            SqliteFrontierSelector::Keys(_) | SqliteFrontierSelector::Namespaces(_) => 0,
        }
    }
    fn row_revision(&self, row: &GenericRow) -> FrontierRevision {
        row.revision
    }
    fn committed_revision(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<FrontierRevision, SqliteReadError>> + Send + '_,
        >,
    > {
        let pool = self.read_pool.clone();
        let id = Arc::clone(&self.frontier_id);
        Box::pin(async move {
            let v: i64 = sqlx::query_scalar(
                "SELECT revision FROM generic_frontier_meta WHERE frontier_id = ?",
            )
            .bind(id.as_ref())
            .fetch_one(&pool)
            .await
            .map_err(|e| Box::new(e) as SqliteReadError)?;
            Ok(u64::try_from(v).expect("revision non-negative"))
        })
    }
    fn fetch_rows<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: FrontierRevision,
        through: FrontierRevision,
        exact: Option<FrontierRevision>,
        limit: Option<usize>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<GenericRow>, SqliteReadError>> + Send + 'a>,
    > {
        let id = Arc::clone(&self.frontier_id);
        let pool = self.read_pool.clone();
        let codec = self.codec.clone();
        let selector = selector.clone();
        Box::pin(async move {
            let mut q = sqlx::QueryBuilder::<Sqlite>::new(
                "SELECT key, revision, value FROM generic_frontier_entries WHERE frontier_id = ",
            );
            q.push_bind(id.as_ref());
            q.push(" AND revision > ");
            q.push_bind(i64::try_from(after).expect("revision fits SQLite"));
            q.push(" AND revision <= ");
            q.push_bind(i64::try_from(through).expect("revision fits SQLite"));
            if let Some(r) = exact {
                q.push(" AND revision = ");
                q.push_bind(i64::try_from(r).expect("revision fits SQLite"));
            }
            match selector {
                SqliteFrontierSelector::All { after: bound } => {
                    if bound > after {
                        q.push(" AND revision > ");
                        q.push_bind(i64::try_from(bound).expect("revision fits SQLite"));
                    }
                }
                SqliteFrontierSelector::Keys(keys) => {
                    if keys.is_empty() {
                        return Ok(Vec::new());
                    }
                    q.push(" AND (");
                    let mut first = true;
                    for (k, bound) in keys {
                        if !first {
                            q.push(" OR ");
                        }
                        first = false;
                        q.push("(key = ");
                        q.push_bind(codec.encode_key(&k));
                        q.push(" AND revision > ");
                        q.push_bind(i64::try_from(bound).expect("revision fits SQLite"));
                        q.push(")");
                    }
                    q.push(")");
                }
                SqliteFrontierSelector::Namespaces(namespaces) => {
                    if namespaces.is_empty() {
                        return Ok(Vec::new());
                    }
                    q.push(" AND (");
                    let mut first = true;
                    for (namespace, bound) in namespaces {
                        if !first {
                            q.push(" OR ");
                        }
                        first = false;
                        q.push("(namespace = ");
                        q.push_bind(namespace);
                        q.push(" AND revision > ");
                        q.push_bind(i64::try_from(bound).expect("revision fits SQLite"));
                        q.push(")");
                    }
                    q.push(")");
                }
            }
            q.push(" ORDER BY revision, key");
            if let Some(n) = limit {
                q.push(" LIMIT ");
                q.push_bind(i64::try_from(n).expect("limit fits SQLite"));
            }
            let rows = q
                .build()
                .fetch_all(&pool)
                .await
                .map_err(|e| Box::new(e) as SqliteReadError)?;
            rows.into_iter()
                .map(|r| {
                    Ok(GenericRow {
                        key: r.try_get("key")?,
                        revision: u64::try_from(r.try_get::<i64, _>("revision")?)
                            .expect("revision non-negative"),
                        value: r.try_get("value").ok(),
                    })
                })
                .collect()
        })
    }
    fn decode_row(
        &self,
        row: GenericRow,
    ) -> Result<Option<FrontierEntry<Self::Key, Self::Value>>, SqliteReadError> {
        Ok(Some(FrontierEntry {
            key: self.codec.decode_key(&row.key)?,
            revision: row.revision,
            // Older frontier rows may encode a deletion as an empty BLOB
            // rather than SQL NULL. Both represent an absent value.
            value: row
                .value
                .filter(|value| !value.is_empty())
                .map(|value| self.codec.decode_value(&value))
                .transpose()?,
        }))
    }
}

#[async_trait]
impl<C: SqliteFrontierCodec> KeyedFrontier<C::Key, C::Value> for SqliteKeyedFrontier<C> {
    type Selector = SqliteFrontierSelector<C::Key>;
    type Context<'a>
        = Transaction<'a, Sqlite>
    where
        Self: 'a;
    type Transaction<'a>
        = SqliteKeyedFrontierTransaction<'a, C>
    where
        Self: 'a;
    async fn begin<'a>(&'a self) -> KeyedFrontierResult<Self::Transaction<'a>> {
        SqliteKeyedFrontier::begin(self).await
    }
    async fn begin_with_context<'a>(
        &'a self,
        context: Self::Context<'a>,
    ) -> KeyedFrontierResult<Self::Transaction<'a>> {
        Ok(self.begin_with_context(context))
    }
    async fn open(
        &self,
        selector: Self::Selector,
        limits: FrontierReadLimits,
    ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<C::Key, C::Value> + '_>> {
        open_sqlite_reader(self.clone(), selector, limits).await
    }
}

pub struct SqliteKeyedFrontierTransaction<'a, C: SqliteFrontierCodec> {
    transaction: Option<Transaction<'a, Sqlite>>,
    frontier_id: Arc<str>,
    codec: C,
    changed: Arc<Notify>,
    staged: BTreeMap<C::Key, Option<C::Value>>,
    reserved_revision: Option<FrontierRevision>,
}

fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> KeyedFrontierError {
    KeyedFrontierError::Backend(Box::new(e))
}

impl<'a, C: SqliteFrontierCodec> SqliteKeyedFrontierTransaction<'a, C> {
    fn tx(&mut self) -> KeyedFrontierResult<&mut Transaction<'a, Sqlite>> {
        self.transaction
            .as_mut()
            .ok_or_else(|| backend(std::io::Error::other("sqlite frontier transaction missing")))
    }
    async fn current_revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        let frontier_id = Arc::clone(&self.frontier_id);
        let v: i64 =
            sqlx::query_scalar("SELECT revision FROM generic_frontier_meta WHERE frontier_id = ?")
                .bind(frontier_id.as_ref())
                .fetch_one(&mut **self.tx()?)
                .await
                .map_err(backend)?;
        u64::try_from(v).map_err(backend)
    }
}

#[async_trait]
impl<'a, C: SqliteFrontierCodec> KeyedFrontierTransaction<C::Key, C::Value>
    for SqliteKeyedFrontierTransaction<'a, C>
{
    type Context = Transaction<'a, Sqlite>;
    fn context_mut(&mut self) -> &mut Self::Context {
        self.transaction
            .as_mut()
            .expect("sqlite frontier transaction present")
    }
    async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        if let Some(r) = self.reserved_revision {
            return Ok(r);
        }
        let frontier_id = Arc::clone(&self.frontier_id);
        let v: i64 = sqlx::query_scalar("UPDATE generic_frontier_meta SET revision = revision + 1 WHERE frontier_id = ? RETURNING revision").bind(frontier_id.as_ref()).fetch_one(&mut **self.tx()?).await.map_err(backend)?;
        let r = u64::try_from(v).map_err(backend)?;
        self.reserved_revision = Some(r);
        Ok(r)
    }
    fn isolation(&self) -> TransactionIsolation {
        TransactionIsolation::Serializable
    }
    async fn get(&mut self, key: &C::Key) -> KeyedFrontierResult<Option<C::Value>> {
        if let Some(v) = self.staged.get(key) {
            return Ok(v.clone());
        }
        let frontier_id = Arc::clone(&self.frontier_id);
        let encoded_key = self.codec.encode_key(key);
        let raw: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT value FROM generic_frontier_entries WHERE frontier_id = ? AND key = ?",
        )
        .bind(frontier_id.as_ref())
        .bind(encoded_key)
        .fetch_optional(&mut **self.tx()?)
        .await
        .map_err(backend)?
        .flatten();
        raw.map(|v| {
            self.codec
                .decode_value(&v)
                .map_err(KeyedFrontierError::Backend)
        })
        .transpose()
    }
    async fn put(&mut self, key: C::Key, value: C::Value) -> KeyedFrontierResult<()> {
        self.staged.insert(key, Some(value));
        Ok(())
    }
    async fn delete(&mut self, key: C::Key) -> KeyedFrontierResult<()> {
        self.staged.insert(key, None);
        Ok(())
    }
    async fn commit(mut self) -> KeyedFrontierResult<FrontierRevision> {
        let changed = !self.staged.is_empty() || self.reserved_revision.is_some();
        let revision = if changed {
            self.revision().await?
        } else {
            self.current_revision().await?
        };
        let frontier_id = Arc::clone(&self.frontier_id);
        let codec = self.codec.clone();
        let staged = std::mem::take(&mut self.staged);
        for (key, value) in staged {
            let namespace = codec.namespace(&key);
            let encoded_key = codec.encode_key(&key);
            let encoded_value = value.map(|value| codec.encode_value(&value));
            let tx = self.tx()?;
            sqlx::query("INSERT INTO generic_frontier_entries (frontier_id, namespace, key, revision, value) VALUES (?, ?, ?, ?, ?) ON CONFLICT(frontier_id, key) DO UPDATE SET namespace = excluded.namespace, revision = excluded.revision, value = excluded.value")
                .bind(frontier_id.as_ref())
                .bind(namespace)
                .bind(encoded_key)
                .bind(i64::try_from(revision).map_err(backend)?)
                .bind(encoded_value)
                .execute(&mut **tx)
                .await
                .map_err(backend)?;
        }
        self.transaction
            .take()
            .expect("sqlite frontier transaction present")
            .commit()
            .await
            .map_err(backend)?;
        if changed {
            self.changed.notify_waiters();
        }
        Ok(revision)
    }
    async fn rollback(mut self) -> KeyedFrontierResult<()> {
        self.transaction
            .take()
            .expect("sqlite frontier transaction present")
            .rollback()
            .await
            .map_err(backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Clone)]
    struct Bytes;
    impl SqliteFrontierCodec for Bytes {
        type Key = String;
        type Value = String;
        fn encode_key(&self, value: &String) -> Vec<u8> {
            value.as_bytes().to_vec()
        }
        fn decode_key(
            &self,
            value: &[u8],
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok(String::from_utf8(value.to_vec())?)
        }
        fn encode_value(&self, value: &String) -> Vec<u8> {
            value.as_bytes().to_vec()
        }
        fn decode_value(
            &self,
            value: &[u8],
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok(String::from_utf8(value.to_vec())?)
        }
    }
    #[derive(Clone)]
    struct Namespaced;
    impl SqliteFrontierCodec for Namespaced {
        type Key = String;
        type Value = String;
        fn encode_key(&self, value: &String) -> Vec<u8> {
            value.as_bytes().to_vec()
        }
        fn decode_key(
            &self,
            value: &[u8],
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok(String::from_utf8(value.to_vec())?)
        }
        fn encode_value(&self, value: &String) -> Vec<u8> {
            value.as_bytes().to_vec()
        }
        fn decode_value(
            &self,
            value: &[u8],
        ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            Ok(String::from_utf8(value.to_vec())?)
        }
        fn namespace(&self, key: &String) -> Option<Vec<u8>> {
            Some(key.as_bytes()[..1].to_vec())
        }
    }

    #[tokio::test]
    async fn transaction_roundtrip_and_rollback() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier =
            SqliteKeyedFrontier::new(pool.clone(), pool, "test", Bytes, Arc::new(Notify::new()))
                .await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "one".into()).await?;
        let revision = tx.commit().await?;
        assert_eq!(revision, 1);
        let mut tx = frontier.begin().await?;
        assert_eq!(tx.get(&"a".into()).await?, Some("one".into()));
        tx.delete("a".into()).await?;
        tx.rollback().await?;
        let mut tx = frontier.begin().await?;
        assert_eq!(tx.get(&"a".into()).await?, Some("one".into()));
        Ok(())
    }

    #[tokio::test]
    async fn replay_boundary_precedes_live_entries() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier =
            SqliteKeyedFrontier::new(pool.clone(), pool, "replay", Bytes, Arc::new(Notify::new()))
                .await?;
        let mut reader = frontier
            .open(
                SqliteFrontierSelector::All { after: 0 },
                FrontierReadLimits::default(),
            )
            .await?;
        assert!(matches!(
            reader.next().await?,
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { .. }
        ));
        let mut tx = frontier.begin().await?;
        tx.put("live".into(), "value".into()).await?;
        tx.commit().await?;
        let read = reader.next().await?;
        assert!(
            matches!(read, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } if entries.len() == 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn exact_key_and_empty_selectors_are_isolated() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier = SqliteKeyedFrontier::new(
            pool.clone(),
            pool,
            "selectors",
            Bytes,
            Arc::new(Notify::new()),
        )
        .await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "A".into()).await?;
        tx.put("b".into(), "B".into()).await?;
        tx.commit().await?;
        let mut keys = BTreeMap::new();
        keys.insert("a".to_string(), 0);
        let mut reader = frontier
            .open(
                SqliteFrontierSelector::Keys(keys),
                FrontierReadLimits::default(),
            )
            .await?;
        assert!(
            matches!(reader.next().await?, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } if entries.iter().all(|entry| entry.key == "a"))
        );
        let mut empty = frontier
            .open(
                SqliteFrontierSelector::Keys(BTreeMap::new()),
                FrontierReadLimits::default(),
            )
            .await?;
        let mut replay_complete = false;
        for _ in 0..2 {
            match empty.next().await? {
                big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } => {
                    assert!(entries.is_empty());
                }
                big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { .. } => {
                    replay_complete = true;
                    break;
                }
            }
        }
        assert!(replay_complete);
        Ok(())
    }

    #[tokio::test]
    async fn namespace_selector_honors_lower_bound_and_empty() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier = SqliteKeyedFrontier::new(
            pool.clone(),
            pool,
            "namespaces",
            Namespaced,
            Arc::new(Notify::new()),
        )
        .await?;
        let mut tx = frontier.begin().await?;
        tx.put("a1".into(), "A".into()).await?;
        tx.put("b1".into(), "B".into()).await?;
        tx.commit().await?;
        let mut tx = frontier.begin().await?;
        tx.put("a2".into(), "A2".into()).await?;
        tx.commit().await?;
        let mut namespaces = BTreeMap::new();
        namespaces.insert(vec![b'a'], 1);
        let mut reader = frontier
            .open(
                SqliteFrontierSelector::Namespaces(namespaces),
                FrontierReadLimits::default(),
            )
            .await?;
        assert!(
            matches!(reader.next().await?, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } if entries.iter().all(|entry| entry.key == "a2"))
        );
        let mut empty = frontier
            .open(
                SqliteFrontierSelector::Namespaces(BTreeMap::new()),
                FrontierReadLimits::default(),
            )
            .await?;
        let mut replay_complete = false;
        for _ in 0..2 {
            match empty.next().await? {
                big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } => {
                    assert!(entries.is_empty());
                }
                big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { .. } => {
                    replay_complete = true;
                    break;
                }
            }
        }
        assert!(replay_complete);
        Ok(())
    }

    #[tokio::test]
    async fn caller_transaction_and_empty_progress_are_atomic() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier = SqliteKeyedFrontier::new(
            pool.clone(),
            pool.clone(),
            "atomic",
            Bytes,
            Arc::new(Notify::new()),
        )
        .await?;
        sqlx::query("CREATE TABLE side (key TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .map_err(backend)?;
        let mut tx = frontier.begin().await?;
        {
            let context = tx.context_mut();
            sqlx::query("INSERT INTO side(key) VALUES ('ok')")
                .execute(&mut **context)
                .await
                .map_err(backend)?;
        }
        tx.put("ok".into(), "value".into()).await?;
        tx.commit().await?;
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT key FROM side")
                .fetch_one(&pool)
                .await
                .map_err(backend)?,
            "ok"
        );
        let mut reader = frontier
            .open(
                SqliteFrontierSelector::All { after: 0 },
                FrontierReadLimits::default(),
            )
            .await?;
        assert!(matches!(
            reader.next().await?,
            big_sync_core::keyed_frontier::FrontierRead::Entries { entries, through }
                if through == 1 && entries.len() == 1 && entries[0].key == "ok"
        ));
        assert!(matches!(
            reader.next().await?,
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { through: 1 }
        ));
        let mut empty = frontier.begin().await?;
        let reserved_revision = empty.revision().await?;
        assert_eq!(reserved_revision, 2);
        assert_eq!(empty.commit().await?, reserved_revision);
        match reader.next().await? {
            big_sync_core::keyed_frontier::FrontierRead::Entries { entries, through } => {
                assert!(entries.is_empty());
                assert_eq!(through, reserved_revision);
            }
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { .. } => {
                panic!("replay boundary emitted more than once")
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn arbitrary_cursor_reads_do_not_advance_frontier() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier =
            SqliteKeyedFrontier::new(pool.clone(), pool, "cursor", Bytes, Arc::new(Notify::new()))
                .await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "one".into()).await?;
        assert_eq!(tx.commit().await?, 1);
        let mut reader = frontier
            .open(
                SqliteFrontierSelector::All { after: 1 },
                FrontierReadLimits::default(),
            )
            .await?;
        assert!(matches!(
            reader.next().await?,
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { through: 1 }
        ));
        let mut tx = frontier.begin().await?;
        assert_eq!(tx.revision().await?, 2);
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn replacement_then_delete_keeps_other_keys_intact() -> KeyedFrontierResult<()> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(backend)?;
        let frontier = SqliteKeyedFrontier::new(
            pool.clone(),
            pool,
            "collapse",
            Bytes,
            Arc::new(Notify::new()),
        )
        .await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "old".into()).await?;
        tx.put("b".into(), "keep".into()).await?;
        tx.commit().await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "new".into()).await?;
        tx.delete("a".into()).await?;
        tx.commit().await?;
        let mut reader = frontier
            .open(
                SqliteFrontierSelector::All { after: 0 },
                FrontierReadLimits::default(),
            )
            .await?;
        let read = reader.next().await?;
        assert!(
            matches!(read, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. }
            if entries.iter().any(|entry| entry.key == "a" && entry.value.is_none())
                && entries.iter().any(|entry| entry.key == "b" && entry.value.as_deref() == Some("keep")))
        );
        Ok(())
    }
}
