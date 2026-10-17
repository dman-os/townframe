#![allow(clippy::disallowed_names)]
use super::sqlite_read::{SqliteReadError, SqliteReadSource, open_sqlite_reader};
use crate::sqlite::queries;
use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierRevision, KeyedFrontier, KeyedFrontierError, KeyedFrontierReader,
    KeyedFrontierResult, KeyedFrontierTransaction, TransactionIsolation,
};
use sqlite_rs::{Database, Sql, Tx};
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

/// A conservative per-row byte cost for the replay read-ahead budget.
///
/// The estimate is the row header plus the stored key and payload, which is what a page
/// materializes. It deliberately over-counts rather than under-counts: a page that comes in
/// under the budget costs a round trip, while one that overruns it materializes a payload the
/// read-ahead asked it not to.
fn row_byte_cost(row: &GenericRow) -> usize {
    const ENTRY_BYTE_OVERHEAD: usize = 32;
    ENTRY_BYTE_OVERHEAD + row.key.len() + row.value.as_ref().map_or(0, Vec::len)
}

#[derive(Clone)]
pub struct SqliteKeyedFrontier<C: SqliteFrontierCodec> {
    pub(crate) sql: Database,
    pub(crate) frontier_id: Arc<str>,
    pub(crate) changed: Arc<Notify>,
    pub(crate) codec: C,
}

impl<C: SqliteFrontierCodec> SqliteKeyedFrontier<C> {
    pub async fn new(
        database: Database,
        frontier_id: impl Into<Arc<str>>,
        codec: C,
        changed: Arc<Notify>,
    ) -> KeyedFrontierResult<Self> {
        let frontier_id: Arc<str> = frontier_id.into();
        database
            .apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await
            .map_err(backend)?;
        queries::frontier_init(&database, frontier_id.to_string())
            .await
            .map_err(backend)?;
        Ok(Self {
            sql: database,
            frontier_id,
            changed,
            codec,
        })
    }

    pub async fn begin(&self) -> KeyedFrontierResult<SqliteKeyedFrontierTransaction<C>> {
        let tx = self.sql.begin_write().await.map_err(backend)?;
        Ok(self.begin_with_context(tx))
    }

    pub async fn latest_revision(&self) -> KeyedFrontierResult<FrontierRevision> {
        let revision = queries::frontier_revision(&self.sql, self.frontier_id.to_string())
            .await
            .map_err(backend)?;
        u64::try_from(revision).map_err(backend)
    }

    pub fn begin_with_context(&self, transaction: Tx) -> SqliteKeyedFrontierTransaction<C> {
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
    pub async fn apply_in_context(
        &self,
        tx: &mut Tx,
        mutations: impl IntoIterator<Item = (C::Key, Option<C::Value>)>,
    ) -> KeyedFrontierResult<FrontierRevision> {
        let mutations: Vec<_> = mutations.into_iter().collect();
        let frontier_id = Arc::clone(&self.frontier_id);
        let revision = queries::frontier_next_revision(tx, frontier_id.to_string())
            .await
            .map_err(backend)?;
        for (key, value) in mutations {
            queries::frontier_upsert_entry(
                tx,
                frontier_id.to_string(),
                self.codec.namespace(&key),
                self.codec.encode_key(&key),
                revision,
                value.map(|value| self.codec.encode_value(&value)),
            )
            .await
            .map_err(backend)?;
        }
        u64::try_from(revision).map_err(backend)
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
    fn database(&self) -> &Database {
        &self.sql
    }
    fn changed(&self) -> &Notify {
        &self.changed
    }
    fn fetch_rows_with_byte_budget<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: FrontierRevision,
        through: FrontierRevision,
        max_entries: usize,
        max_bytes: usize,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(Vec<GenericRow>, FrontierRevision), SqliteReadError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if max_entries == 0 {
                return Ok((Vec::new(), through));
            }
            // The entry limit selects the candidates; the byte budget then decides how far into
            // them the page may go. Both are satisfied by the same re-read as the unbounded path:
            // a page may stop in the middle of one atomic revision, so the revision the budget
            // lands on is re-read in full and the page ends there.
            let candidates = self
                .fetch_rows(selector, after, through, None, Some(max_entries))
                .await?;
            let mut bytes = 0usize;
            let mut cutoff = None;
            for row in &candidates {
                bytes = bytes.saturating_add(row_byte_cost(row));
                cutoff = Some(row.revision);
                if bytes >= max_bytes {
                    break;
                }
            }
            let Some(cutoff) = cutoff else {
                return Ok((Vec::new(), through));
            };
            let rows = self.fetch_rows(selector, after, cutoff, None, None).await?;
            Ok((rows, cutoff))
        })
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
        let database = self.sql.clone();
        let id = Arc::clone(&self.frontier_id);
        Box::pin(async move {
            let v = queries::frontier_revision(&database, id.to_string())
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
        let database = self.sql.clone();
        let codec = self.codec.clone();
        let selector = selector.clone();
        Box::pin(async move {
            let mut q = Sql::new(
                "SELECT key, revision, value FROM generic_frontier_entries WHERE frontier_id = ",
            );
            q.bind(id.as_ref());
            q.push(" AND revision > ");
            q.bind(i64::try_from(after).expect("revision fits SQLite"));
            q.push(" AND revision <= ");
            q.bind(i64::try_from(through).expect("revision fits SQLite"));
            if let Some(r) = exact {
                q.push(" AND revision = ");
                q.bind(i64::try_from(r).expect("revision fits SQLite"));
            }
            match selector {
                SqliteFrontierSelector::All { after: bound } => {
                    if bound > after {
                        q.push(" AND revision > ");
                        q.bind(i64::try_from(bound).expect("revision fits SQLite"));
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
                        q.bind(codec.encode_key(&k));
                        q.push(" AND revision > ");
                        q.bind(i64::try_from(bound).expect("revision fits SQLite"));
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
                        q.bind(namespace);
                        q.push(" AND revision > ");
                        q.bind(i64::try_from(bound).expect("revision fits SQLite"));
                        q.push(")");
                    }
                    q.push(")");
                }
            }
            q.push(" ORDER BY revision, key");
            if let Some(n) = limit {
                q.push(" LIMIT ");
                q.bind(i64::try_from(n).expect("limit fits SQLite"));
            }
            let rows = q
                .query(&database)
                .await
                .map_err(|e| Box::new(e) as SqliteReadError)?
                .rows;
            rows.into_iter()
                .map(|r| {
                    Ok(GenericRow {
                        key: r.get("key")?,
                        revision: u64::try_from(r.get::<i64>("revision")?)
                            .expect("revision non-negative"),
                        value: r.get("value")?,
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
        = Tx
    where
        Self: 'a;
    type Transaction<'a>
        = SqliteKeyedFrontierTransaction<C>
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
    ) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<C::Key, C::Value> + '_>> {
        open_sqlite_reader(self.clone(), selector).await
    }
}

pub struct SqliteKeyedFrontierTransaction<C: SqliteFrontierCodec> {
    transaction: Option<Tx>,
    frontier_id: Arc<str>,
    codec: C,
    changed: Arc<Notify>,
    staged: BTreeMap<C::Key, Option<C::Value>>,
    reserved_revision: Option<FrontierRevision>,
}

fn backend<E: std::error::Error + Send + Sync + 'static>(e: E) -> KeyedFrontierError {
    KeyedFrontierError::Backend(Box::new(e))
}

impl<C: SqliteFrontierCodec> SqliteKeyedFrontierTransaction<C> {
    fn tx(&mut self) -> KeyedFrontierResult<&mut Tx> {
        self.transaction
            .as_mut()
            .ok_or_else(|| backend(std::io::Error::other("sqlite frontier transaction missing")))
    }
    async fn current_revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        let frontier_id = self.frontier_id.to_string();
        let v = queries::frontier_revision(self.tx()?, frontier_id)
            .await
            .map_err(backend)?;
        u64::try_from(v).map_err(backend)
    }
}

#[async_trait]
impl<C: SqliteFrontierCodec> KeyedFrontierTransaction<C::Key, C::Value>
    for SqliteKeyedFrontierTransaction<C>
{
    type Context = Tx;
    fn context_mut(&mut self) -> &mut Self::Context {
        self.transaction
            .as_mut()
            .expect("sqlite frontier transaction present")
    }
    async fn revision(&mut self) -> KeyedFrontierResult<FrontierRevision> {
        if let Some(r) = self.reserved_revision {
            return Ok(r);
        }
        let frontier_id = self.frontier_id.to_string();
        let v = queries::frontier_next_revision(self.tx()?, frontier_id)
            .await
            .map_err(backend)?;
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
        let frontier_id = self.frontier_id.to_string();
        let encoded_key = self.codec.encode_key(key);
        let raw = queries::frontier_entry_value(self.tx()?, frontier_id, encoded_key)
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
        let revision_i64 = i64::try_from(revision).map_err(backend)?;
        for (key, value) in staged {
            queries::frontier_upsert_entry(
                self.tx()?,
                frontier_id.to_string(),
                codec.namespace(&key),
                codec.encode_key(&key),
                revision_i64,
                value.map(|value| codec.encode_value(&value)),
            )
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
    use crate::keyed_frontier::open_sqlite_reader_with_byte_budget;
    use big_sync_core::keyed_frontier::FrontierReadLimits;
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
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "test", Bytes, Arc::new(Notify::new())).await?;
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
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "replay", Bytes, Arc::new(Notify::new())).await?;
        let mut reader = frontier
            .open(SqliteFrontierSelector::All { after: 0 })
            .await?;
        assert!(matches!(
            reader.next(FrontierReadLimits::default()).await?,
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { .. }
        ));
        let mut tx = frontier.begin().await?;
        tx.put("live".into(), "value".into()).await?;
        tx.commit().await?;
        let read = reader.next(FrontierReadLimits::default()).await?;
        assert!(
            matches!(read, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } if entries.len() == 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn exact_key_and_empty_selectors_are_isolated() -> KeyedFrontierResult<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "selectors", Bytes, Arc::new(Notify::new())).await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "A".into()).await?;
        tx.put("b".into(), "B".into()).await?;
        tx.commit().await?;
        let mut keys = BTreeMap::new();
        keys.insert("a".to_string(), 0);
        let mut reader = frontier.open(SqliteFrontierSelector::Keys(keys)).await?;
        assert!(
            matches!(reader.next(FrontierReadLimits::default()).await?, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } if entries.iter().all(|entry| entry.key == "a"))
        );
        let mut empty = frontier
            .open(SqliteFrontierSelector::Keys(BTreeMap::new()))
            .await?;
        let mut replay_complete = false;
        for _ in 0..2 {
            match empty.next(FrontierReadLimits::default()).await? {
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
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "namespaces", Namespaced, Arc::new(Notify::new()))
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
            .open(SqliteFrontierSelector::Namespaces(namespaces))
            .await?;
        assert!(
            matches!(reader.next(FrontierReadLimits::default()).await?, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } if entries.iter().all(|entry| entry.key == "a2"))
        );
        let mut empty = frontier
            .open(SqliteFrontierSelector::Namespaces(BTreeMap::new()))
            .await?;
        let mut replay_complete = false;
        for _ in 0..2 {
            match empty.next(FrontierReadLimits::default()).await? {
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
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql.clone(), "atomic", Bytes, Arc::new(Notify::new())).await?;
        sql.execute("CREATE TABLE side (key TEXT PRIMARY KEY)", ())
            .await
            .map_err(backend)?;
        let mut tx = frontier.begin().await?;
        {
            let context = tx.context_mut();
            context
                .execute("INSERT INTO side(key) VALUES ('ok')", ())
                .await
                .map_err(backend)?;
        }
        tx.put("ok".into(), "value".into()).await?;
        tx.commit().await?;
        assert_eq!(
            sql.query("SELECT key FROM side", ())
                .await
                .and_then(sqlite_rs::ResultSet::one::<String>)
                .map_err(backend)?,
            "ok"
        );
        let mut reader = frontier
            .open(SqliteFrontierSelector::All { after: 0 })
            .await?;
        assert!(matches!(
            reader.next(FrontierReadLimits::default()).await?,
            big_sync_core::keyed_frontier::FrontierRead::Entries { entries, through }
                if through == 1 && entries.len() == 1 && entries[0].key == "ok"
        ));
        assert!(matches!(
            reader.next(FrontierReadLimits::default()).await?,
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { through: 1 }
        ));
        let mut empty = frontier.begin().await?;
        let reserved_revision = empty.revision().await?;
        assert_eq!(reserved_revision, 2);
        assert_eq!(empty.commit().await?, reserved_revision);
        match reader.next(FrontierReadLimits::default()).await? {
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
    async fn walker_state_and_frontier_settle_in_one_context() -> utils_rs::prelude::Res<()> {
        use crate::delta_walker_state::SqliteDeltaWalkerStateRepo;
        use big_sync_core::delta_walker_sparse_state::{
            DeltaWalkerSparseStateRepo, DeltaWalkerSparseStateTransaction,
        };
        use big_sync_core::delta_walker_state::{
            DeltaWalkerStateRepo, DeltaWalkerStateTransaction,
        };
        use utils_rs::prelude::futures::FutureExt;

        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[])?;
        let changed = Arc::new(Notify::new());
        let frontier =
            SqliteKeyedFrontier::new(sql.clone(), "settlement", Bytes, Arc::clone(&changed))
                .await?;
        let repo = SqliteDeltaWalkerStateRepo::new(sql.clone(), "settlement", "consumer").await?;
        let mut wakeup = Box::pin(changed.notified());
        assert!(wakeup.as_mut().now_or_never().is_none());

        let mut state = repo.begin_with_context(sql.begin_write().await?);
        assert_eq!(
            frontier
                .apply_in_context(
                    state.context_mut(),
                    [("key".into(), Some("rollback".into()))]
                )
                .await?,
            1
        );
        state.put(b"key".to_vec(), b"rollback".to_vec()).await?;
        state.advance_from(0, 7).await?;
        state.rollback().await?;
        assert_eq!(frontier.latest_revision().await?, 0);
        assert_eq!(repo.progress().await?.upstream_revision, 0);
        assert_eq!(repo.get(b"key").await?, None);
        assert!(wakeup.as_mut().now_or_never().is_none());

        let mut state = repo.begin_with_context(sql.begin_write().await?);
        assert_eq!(
            frontier
                .apply_in_context(
                    state.context_mut(),
                    [("key".into(), Some("dropped".into()))]
                )
                .await?,
            1
        );
        state.advance_from(0, 7).await?;
        drop(state);
        let mut state = repo.begin_with_context(sql.begin_write().await?);
        assert_eq!(state.progress().await?.upstream_revision, 0);
        assert_eq!(
            frontier
                .apply_in_context(
                    state.context_mut(),
                    [("key".into(), Some("committed".into()))]
                )
                .await?,
            1
        );
        state.put(b"key".to_vec(), b"committed".to_vec()).await?;
        state.advance_from(0, 7).await?;
        assert_eq!(frontier.latest_revision().await?, 0);
        assert_eq!(repo.progress().await?.upstream_revision, 0);
        assert_eq!(repo.get(b"key").await?, None);
        assert!(wakeup.as_mut().now_or_never().is_none());
        state.commit().await?;
        assert!(wakeup.as_mut().now_or_never().is_none());
        frontier.notify_changed();
        wakeup.await;
        assert_eq!(frontier.latest_revision().await?, 1);
        assert_eq!(repo.progress().await?.upstream_revision, 7);
        assert_eq!(repo.get(b"key").await?, Some(b"committed".to_vec()));
        let mut tx = frontier.begin().await?;
        assert_eq!(
            tx.get(&"key".to_owned()).await?,
            Some("committed".to_owned())
        );
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn arbitrary_cursor_reads_do_not_advance_frontier() -> KeyedFrontierResult<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "cursor", Bytes, Arc::new(Notify::new())).await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "one".into()).await?;
        assert_eq!(tx.commit().await?, 1);
        let mut reader = frontier
            .open(SqliteFrontierSelector::All { after: 1 })
            .await?;
        assert!(matches!(
            reader.next(FrontierReadLimits::default()).await?,
            big_sync_core::keyed_frontier::FrontierRead::ReplayComplete { through: 1 }
        ));
        let mut tx = frontier.begin().await?;
        assert_eq!(tx.revision().await?, 2);
        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn replacement_then_delete_keeps_other_keys_intact() -> KeyedFrontierResult<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "collapse", Bytes, Arc::new(Notify::new())).await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "old".into()).await?;
        tx.put("b".into(), "keep".into()).await?;
        tx.commit().await?;
        let mut tx = frontier.begin().await?;
        tx.put("a".into(), "new".into()).await?;
        tx.delete("a".into()).await?;
        tx.commit().await?;
        let mut reader = frontier
            .open(SqliteFrontierSelector::All { after: 0 })
            .await?;
        let read = reader.next(FrontierReadLimits::default()).await?;
        assert!(
            matches!(read, big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. }
            if entries.iter().any(|entry| entry.key == "a" && entry.value.is_none())
                && entries.iter().any(|entry| entry.key == "b" && entry.value.as_deref() == Some("keep")))
        );
        Ok(())
    }

    /// The byte budget bounds the page rather than being dropped, and the first entry is
    /// taken even when it alone exceeds it.
    #[tokio::test]
    async fn a_byte_budget_bounds_the_page_and_always_takes_the_first_entry()
    -> KeyedFrontierResult<()> {
        let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).expect("create sqlite database");
        let frontier =
            SqliteKeyedFrontier::new(sql, "budget", Bytes, Arc::new(Notify::new())).await?;
        // One entry per revision, each with 100 bytes of payload. `row_byte_cost` charges 32
        // bytes of header plus the key and payload, so every entry costs 133 bytes.
        for key in ["a", "b", "c"] {
            let mut tx = frontier.begin().await?;
            tx.put(key.into(), "x".repeat(100)).await?;
            tx.commit().await?;
        }
        let limits = FrontierReadLimits {
            max_entries: std::num::NonZeroUsize::new(8).expect("literal is non-zero"),
        };

        // Larger than one entry, smaller than two: the page stops at the second.
        let mut reader = open_sqlite_reader_with_byte_budget(
            frontier.clone(),
            SqliteFrontierSelector::All { after: 0 },
            Some(200),
        )
        .await?;
        let page = reader.next(limits).await?;
        let big_sync_core::keyed_frontier::FrontierRead::Entries { entries, through } = page else {
            panic!("a page over existing rows is not a replay-complete read");
        };
        assert_eq!(
            through, 2,
            "the page ends at the revision the budget landed on"
        );
        assert_eq!(
            entries.len(),
            2,
            "the budget stops the page before the third entry"
        );

        // A budget the first entry alone exceeds still takes that entry: a page that returns
        // nothing while rows still match loses the walk's position.
        let mut reader = open_sqlite_reader_with_byte_budget(
            frontier.clone(),
            SqliteFrontierSelector::All { after: 0 },
            Some(1),
        )
        .await?;
        let page = reader.next(limits).await?;
        let big_sync_core::keyed_frontier::FrontierRead::Entries { entries, through } = page else {
            panic!("a page over existing rows is not a replay-complete read");
        };
        assert_eq!(through, 1);
        assert_eq!(entries.len(), 1, "the first entry is always taken");

        // The same rows without a budget come back as one page, so the bound above is the
        // budget's doing rather than an entry limit that was going to stop there anyway.
        let mut reader = frontier
            .open(SqliteFrontierSelector::All { after: 0 })
            .await?;
        let page = reader.next(limits).await?;
        let big_sync_core::keyed_frontier::FrontierRead::Entries { entries, .. } = page else {
            panic!("a page over existing rows is not a replay-complete read");
        };
        assert_eq!(entries.len(), 3);
        Ok(())
    }
}
