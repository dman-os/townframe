//! `big_automata`'s okv tables over SQLite, in the same database as the part store.
//!
//! A [`SqliteOkv`] is one okv table: a namespace of `big_automata_okv_entries` with its own rev
//! counter. Its transaction is the part store's ([`SqlitePartTx`]), so one write transaction can
//! stage okv batches of any number of tables together with part store writes, and they commit
//! or roll back as one. That is what lets a consumer land a fetched payload and delete its want
//! in one transaction.
//!
//! - **`apply`** reads the namespace's rev (once per transaction, then from the transaction's
//!   own staging), upserts the batch's rows at `rev + 1` with multi-row statements and writes
//!   the new rev back, all inside the caller's write transaction. A rollback takes the rev with
//!   it. The watch is moved by [`SqlitePartTx::commit`], after the commit is visible.
//! - **`read`** is a keyset-paged range scan of `(namespace_id, rev, key)` in one read
//!   snapshot: each statement asks for at most `max_entries + 1` rows past the last row seen,
//!   and rows are grouped by rev as they arrive so a page never splits a revision. The selector
//!   is part of the statement, so rows it rejects are never read.
//! - **The consumer verbs** are not part of a transaction. Each takes the database's one writer
//!   for a statement or two, so none may be called while holding an open [`SqlitePartTx`].
//!   `prune` is one `DELETE` bounded by the lowest registered cursor.

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::ops::Bound;

use big_automata::okv::KeyRange;
use big_automata::okv::Mutation;
use big_automata::okv::OkvStore;
use big_automata::okv::Pruned;
use big_automata::okv::Selector;
use big_automata::okv::row_bytes;
use big_automata::rev::Entry;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use big_automata::rev::RevWatch;
use sqlite_rs::Sql;
use tokio::sync::watch;

use crate::interlude::*;
use crate::sql;
use crate::sqlite::queries;
use crate::store::SqlitePartStore;
use crate::store::SqlitePartTx;
use sqlite_rs::SqliteDb;

/// Rows a read statement fetches at most, whatever the page's limit: a page that wants more
/// than this continues from the last row it saw.
const READ_CHUNK: usize = 1024;

/// A table's part of a write transaction: the rev its last staged batch took, and the watch to
/// move to it when the transaction commits.
pub(crate) struct Staged {
    rev: Rev,
    wakeups: Arc<watch::Sender<Rev>>,
}

impl Staged {
    /// Move the table's watch to the staged rev. Called after the commit is visible.
    pub(crate) fn publish(self) {
        self.wakeups.send_replace(self.rev);
    }
}

/// One okv table. Clones share the table and its watch.
///
/// Open one handle per table per database, like the part store: the watch is the handle's own,
/// moved by the commits made through it.
pub struct SqliteOkv<D: SqliteDb> {
    db: D,
    namespace: i64,
    wakeups: Arc<watch::Sender<Rev>>,
}

impl<D: SqliteDb> Clone for SqliteOkv<D> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            namespace: self.namespace,
            wakeups: Arc::clone(&self.wakeups),
        }
    }
}

impl<D: SqliteDb> SqliteOkv<D> {
    /// The table called `name` in `store`'s database, created empty if there is none. Its
    /// transactions are `store`'s.
    pub async fn open(store: &SqlitePartStore<D>, name: &str) -> Res<Self> {
        let tx = store.db.begin_write().await?;
        queries::okv_namespace_open(&tx, name.to_owned()).await?;
        let namespace = queries::okv_namespace_by_name(&tx, name.to_owned()).await?;
        D::commit(tx).await?;
        let (wakeups, _) = watch::channel(sql::rev_from_sql(namespace.rev));
        Ok(Self {
            db: store.db.clone(),
            namespace: namespace.namespace_id,
            wakeups: Arc::new(wakeups),
        })
    }
}

/// `AND` the selector's key predicate onto a statement. `Keys` is never empty here: an empty
/// set selects nothing and the caller does not ask.
fn push_selector(query: &mut Sql, selector: &Selector<Vec<u8>>) {
    match selector {
        Selector::All => {}
        Selector::Keys(keys) => {
            // One bound parameter however many keys: a JSON array of hex strings, so the set is
            // never limited by SQLite's variable count.
            let mut hex = String::with_capacity(keys.len() * 6);
            hex.push('[');
            for (index, key) in keys.iter().enumerate() {
                if index != 0 {
                    hex.push(',');
                }
                hex.push('"');
                for byte in key {
                    write!(hex, "{byte:02x}").expect(ERROR_IMPOSSIBLE);
                }
                hex.push('"');
            }
            hex.push(']');
            query
                .push(" AND key IN (SELECT unhex(value) FROM json_each(")
                .bind(hex)
                .push("))");
        }
        Selector::Range(KeyRange { start, end }) => {
            match start {
                Bound::Included(key) => query.push(" AND key >= ").bind(key.clone()),
                Bound::Excluded(key) => query.push(" AND key > ").bind(key.clone()),
                Bound::Unbounded => &mut *query,
            };
            match end {
                Bound::Included(key) => query.push(" AND key <= ").bind(key.clone()),
                Bound::Excluded(key) => query.push(" AND key < ").bind(key.clone()),
                Bound::Unbounded => &mut *query,
            };
        }
    }
}

/// A page being filled one whole revision at a time.
struct Paging<'a> {
    page: &'a mut Page<Vec<u8>, Vec<u8>>,
    max_entries: usize,
    max_bytes: usize,
    bytes: usize,
}

impl Paging<'_> {
    /// Whether a revision of `len` entries and `bytes` bytes may join the page: the first
    /// revision always does, however big, and later ones only if both limits still hold.
    fn fits(&self, len: usize, bytes: usize) -> bool {
        self.page.entries.is_empty()
            || (self.page.entries.len() + len <= self.max_entries
                && self.bytes.saturating_add(bytes) <= self.max_bytes)
    }

    /// Add the finished revision `group` if it fits. When it does not, the page is done and
    /// its `through` is its last revision.
    fn admit(&mut self, group: &mut Vec<Entry<Vec<u8>, Vec<u8>>>, group_bytes: usize) -> bool {
        if !self.fits(group.len(), group_bytes) {
            self.stop();
            return false;
        }
        self.bytes = self.bytes.saturating_add(group_bytes);
        self.page.entries.append(group);
        true
    }

    fn stop(&mut self) {
        self.page.through = self.page.entries.last().expect(ERROR_IMPOSSIBLE).rev;
    }
}

impl<D: SqliteDb> SqliteOkv<D> {
    /// Fill `page` from `(after, until]` inside the snapshot `x`; see [`RevSource::read`].
    async fn read_page(
        &self,
        x: &impl sqlite_rs::QueryExecutor,
        selector: &Selector<Vec<u8>>,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<Vec<u8>, Vec<u8>>,
    ) -> Res<()> {
        page.entries.clear();
        page.through = until;
        if matches!(selector, Selector::Keys(keys) if keys.is_empty()) {
            return Ok(());
        }
        let chunk = limits.max_entries.get().saturating_add(1).min(READ_CHUNK);
        let mut paging = Paging {
            page,
            max_entries: limits.max_entries.get(),
            max_bytes: limits.max_bytes.map_or(usize::MAX, NonZeroUsize::get),
            bytes: 0,
        };
        // The revision being collected: its rows may span statements.
        let mut group = Vec::<Entry<Vec<u8>, Vec<u8>>>::new();
        let mut group_bytes = 0_usize;
        let mut last_seen: Option<(Rev, Vec<u8>)> = None;
        loop {
            // A key set is looked up in the primary key `(namespace_id, key)`, then the matches
            // sorted. Left to itself the planner walks `(namespace_id, rev, key)` instead, to
            // get `ORDER BY rev, key` for free, which reads every entry of the window and
            // probes the set once for each: a cost of the table, not of the key set. The
            // primary key of a WITHOUT ROWID table is the table itself, named like an autoindex.
            let mut query = Sql::new(if matches!(selector, Selector::Keys(_)) {
                "SELECT key
                      , rev
                      , value
                   FROM big_automata_okv_entries INDEXED BY sqlite_autoindex_big_automata_okv_entries_1
                  WHERE namespace_id = "
            } else {
                "SELECT key
                      , rev
                      , value
                   FROM big_automata_okv_entries
                  WHERE namespace_id = "
            });
            query.bind(self.namespace);
            match &last_seen {
                None => query.push(" AND rev > ").bind(sql::rev_to_sql(after)),
                Some((rev, key)) => query
                    .push(" AND (rev, key) > (")
                    .bind(sql::rev_to_sql(*rev))
                    .push(", ")
                    .bind(key.clone())
                    .push(")"),
            };
            query.push(" AND rev <= ").bind(sql::rev_to_sql(until));
            push_selector(&mut query, selector);
            query
                .push(" ORDER BY rev, key LIMIT ")
                .bind(sql::limit(chunk));
            let rows = query
                .query(x)
                .await?
                .all::<(Vec<u8>, i64, Option<Vec<u8>>)>()?;
            let exhausted = rows.len() < chunk;
            for (key, rev, value) in rows {
                let rev = sql::rev_from_sql(rev);
                if group.last().is_some_and(|last| last.rev != rev)
                    && !paging.admit(&mut group, std::mem::take(&mut group_bytes))
                {
                    return Ok(());
                }
                group_bytes = group_bytes.saturating_add(row_bytes(&key, value.as_ref()));
                last_seen = Some((rev, key.clone()));
                group.push(Entry { key, rev, value });
                // A revision that cannot fit once the page has entries is not worth reading
                // to its end.
                if !paging.fits(group.len(), group_bytes) {
                    paging.stop();
                    return Ok(());
                }
            }
            if exhausted {
                if !group.is_empty() {
                    paging.admit(&mut group, group_bytes);
                }
                return Ok(());
            }
        }
    }
}

impl<D: SqliteDb> RevSource for SqliteOkv<D> {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Selector = Selector<Vec<u8>>;

    async fn latest_rev(&self) -> Res<Rev> {
        let reader = self.db.begin_read().await?;
        let rev = queries::okv_namespace_rev(&reader, self.namespace).await?;
        D::end_read(reader).await?;
        Ok(sql::rev_from_sql(rev))
    }

    async fn read(
        &self,
        selector: &Selector<Vec<u8>>,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<Vec<u8>, Vec<u8>>,
    ) -> Res<()> {
        assert!(after < until, "empty read window ({after}, {until}]");
        let reader = self.db.begin_read().await?;
        let latest = sql::rev_from_sql(queries::okv_namespace_rev(&reader, self.namespace).await?);
        assert!(
            until <= latest,
            "read until {until} past the committed rev {latest}"
        );
        self.read_page(&reader, selector, after, until, limits, page)
            .await?;
        D::end_read(reader).await?;
        Ok(())
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }
}

impl<D: SqliteDb> OkvStore for SqliteOkv<D> {
    type Tx = SqlitePartTx<D>;

    async fn apply(
        &self,
        tx: &mut SqlitePartTx<D>,
        mutations: Vec<Mutation<Vec<u8>, Vec<u8>>>,
    ) -> Res<Rev> {
        assert!(
            !mutations.is_empty(),
            "an empty mutation batch has no rows to stamp"
        );
        let base = match tx.okv.get(&self.namespace) {
            Some(staged) => staged.rev,
            None => sql::rev_from_sql(queries::okv_namespace_rev(&tx.tx, self.namespace).await?),
        };
        let rev = base.checked_add(1).expect("okv rev overflow");
        let stamp = sql::rev_to_sql(rev);
        // Within a batch the last mutation of a key wins, and one upsert statement must not
        // name a key twice.
        let rows = mutations
            .into_iter()
            .map(Mutation::into_row)
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(key, value)| (self.namespace, key, stamp, value))
            .collect();
        sql::execute_values(
            &tx.tx,
            "INSERT INTO big_automata_okv_entries (namespace_id, key, rev, value) ",
            rows,
            4,
            " ON CONFLICT (namespace_id, key) DO UPDATE SET
                  rev = excluded.rev
                , value = excluded.value",
        )
        .await?;
        queries::okv_set_namespace_rev(&tx.tx, stamp, self.namespace).await?;
        tx.okv.insert(
            self.namespace,
            Staged {
                rev,
                wakeups: Arc::clone(&self.wakeups),
            },
        );
        Ok(rev)
    }

    async fn get(&self, tx: &mut SqlitePartTx<D>, key: &Vec<u8>) -> Res<Option<Vec<u8>>> {
        // The transaction is the database's writer, so its reads see its own staged rows.
        Ok(queries::okv_get(&tx.tx, self.namespace, key.clone())
            .await?
            .flatten())
    }

    async fn register_consumer(&self, consumer: &str) -> Res<Rev> {
        let tx = self.db.begin_write().await?;
        queries::okv_register_consumer(&tx, self.namespace, consumer.to_owned()).await?;
        let cursor = queries::okv_consumer_cursor(&tx, self.namespace, consumer.to_owned()).await?;
        D::commit(tx).await?;
        Ok(sql::rev_from_sql(cursor.expect(ERROR_IMPOSSIBLE)))
    }

    async fn report_cursor(&self, consumer: &str, cursor: Rev) -> Res<()> {
        let tx = self.db.begin_write().await?;
        let latest = sql::rev_from_sql(queries::okv_namespace_rev(&tx, self.namespace).await?);
        assert!(
            cursor <= latest,
            "okv cursor {cursor} past the committed rev {latest}"
        );
        assert!(
            queries::okv_consumer_cursor(&tx, self.namespace, consumer.to_owned())
                .await?
                .is_some(),
            "okv cursor reported by unregistered consumer {consumer:?}"
        );
        queries::okv_report_cursor(
            &tx,
            sql::rev_to_sql(cursor),
            self.namespace,
            consumer.to_owned(),
        )
        .await?;
        D::commit(tx).await?;
        Ok(())
    }

    async fn unregister_consumer(&self, consumer: &str) -> Res<()> {
        let tx = self.db.begin_write().await?;
        queries::okv_unregister_consumer(&tx, self.namespace, consumer.to_owned()).await?;
        D::commit(tx).await?;
        Ok(())
    }

    async fn consumers(&self) -> Res<BTreeMap<String, Rev>> {
        let reader = self.db.begin_read().await?;
        let rows = queries::okv_consumers(&reader, self.namespace).await?;
        D::end_read(reader).await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.consumer, sql::rev_from_sql(row.cursor)))
            .collect())
    }

    /// One `DELETE` of the namespace's tombstones at or below its lowest cursor, in the same
    /// write transaction that reads the floor it reports.
    async fn prune(&self) -> Res<Pruned> {
        let tx = self.db.begin_write().await?;
        let floor = queries::okv_floor(&tx, self.namespace)
            .await?
            .map_or(0, sql::rev_from_sql);
        let dropped = queries::okv_prune(&tx, self.namespace).await?.rows_affected;
        D::commit(tx).await?;
        Ok(Pruned {
            floor,
            tombstones: usize::try_from(dropped).expect(ERROR_IMPOSSIBLE),
        })
    }
}
