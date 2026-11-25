//! Every statement the store runs is served by an index: no scan whose size is the store's.
//!
//! The store is run through a database wrapper that records each distinct statement text with
//! one set of its parameters, over all the laws; then `EXPLAIN QUERY PLAN` is asked of each.
//! A plan line that scans a table or an index (rather than searching one) fails the test,
//! except scans of the batch's own rows (a `VALUES` list is as big as the batch, not the store).

use std::ops::Bound;
use std::sync::Mutex;

use big_automata::okv::KeyRange;
use big_automata::okv::Mutation;
use big_automata::okv::OkvStore;
use big_automata::okv::Selector;
use big_automata::okv::laws::Backend as OkvBackend;
use big_automata::okv::laws::{self as okv_laws};
use big_automata::part_store::laws::Backend;
use big_automata::part_store::laws::{self};
use big_automata::rev::Page;
use big_automata::rev::RevSource;
use big_automata::rev::laws::limits;
use futures::executor::block_on;
use sqlite_rs::Database;
use sqlite_rs::ExecuteResult;
use sqlite_rs::Executor;
use sqlite_rs::IntoParams;
use sqlite_rs::QueryExecutor;
use sqlite_rs::ReadTx;
use sqlite_rs::ResultSet;
use sqlite_rs::SqlResult;
use sqlite_rs::SqlValue;
use sqlite_rs::Tx;

use super::RUNTIME;
use crate::interlude::*;
use crate::*;

type Log = Arc<Mutex<BTreeMap<String, Vec<SqlValue>>>>;

#[derive(Clone)]
struct Recording {
    db: Database,
    log: Log,
}

struct RecordingWrite {
    tx: Tx,
    log: Log,
}

struct RecordingRead {
    tx: ReadTx,
    log: Log,
}

fn record(log: &Log, sql: &str, params: &[SqlValue]) {
    log.lock()
        .expect(ERROR_MUTEX)
        .entry(sql.to_owned())
        .or_insert_with(|| params.to_vec());
}

impl QueryExecutor for RecordingWrite {
    async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        let params = params.into_params();
        record(&self.log, sql, &params);
        self.tx.query(sql, params).await
    }
}

impl Executor for RecordingWrite {
    async fn execute(&self, sql: &str, params: impl IntoParams) -> SqlResult<ExecuteResult> {
        let params = params.into_params();
        record(&self.log, sql, &params);
        self.tx.execute(sql, params).await
    }
}

impl QueryExecutor for RecordingRead {
    async fn query(&self, sql: &str, params: impl IntoParams) -> SqlResult<ResultSet> {
        let params = params.into_params();
        record(&self.log, sql, &params);
        self.tx.query(sql, params).await
    }
}

impl PartDb for Recording {
    type Write = RecordingWrite;
    type Read = RecordingRead;

    async fn begin_write(&self) -> SqlResult<RecordingWrite> {
        Ok(RecordingWrite {
            tx: self.db.begin_write().await?,
            log: Arc::clone(&self.log),
        })
    }

    async fn begin_read(&self) -> SqlResult<RecordingRead> {
        Ok(RecordingRead {
            tx: self.db.begin_read().await?,
            log: Arc::clone(&self.log),
        })
    }

    async fn commit(tx: RecordingWrite) -> SqlResult<()> {
        tx.tx.commit().await
    }

    async fn rollback(tx: RecordingWrite) -> SqlResult<()> {
        tx.tx.rollback().await
    }

    async fn end_read(tx: RecordingRead) -> SqlResult<()> {
        tx.tx.rollback().await
    }
}

#[derive(Clone)]
struct RecordingBackend {
    log: Log,
}

#[async_trait]
impl Backend for RecordingBackend {
    type Store = SqlitePartStore<Recording>;

    async fn open(&self) -> Res<Self::Store> {
        let db = Recording {
            db: RUNTIME.ephemeral(MIGRATIONS)?,
            log: Arc::clone(&self.log),
        };
        SqlitePartStore::open(db, super::clock()).await
    }

    async fn begin(&self, store: &Self::Store) -> Res<SqlitePartTx<Recording>> {
        store.begin().await
    }

    async fn commit(&self, tx: SqlitePartTx<Recording>) -> Res<()> {
        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, tx: SqlitePartTx<Recording>) -> Res<()> {
        tx.rollback().await
    }
}

/// The okv laws over the recording database, so the okv statements are planned too.
#[derive(Clone)]
struct RecordingOkvBackend {
    log: Log,
    tables: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait]
impl OkvBackend for RecordingOkvBackend {
    type Db = SqlitePartStore<Recording>;
    type Store = SqliteOkv<Recording>;

    async fn open(&self) -> Res<Self::Db> {
        let db = Recording {
            db: RUNTIME.ephemeral(MIGRATIONS)?,
            log: Arc::clone(&self.log),
        };
        SqlitePartStore::open(db, super::clock()).await
    }

    async fn table(&self, db: &Self::Db) -> Res<Self::Store> {
        let table = self
            .tables
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SqliteOkv::open(db, &format!("table-{table}")).await
    }

    async fn begin(&self, db: &Self::Db) -> Res<SqlitePartTx<Recording>> {
        db.begin().await
    }

    async fn commit(&self, tx: SqlitePartTx<Recording>) -> Res<()> {
        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, tx: SqlitePartTx<Recording>) -> Res<()> {
        tx.rollback().await
    }
}

/// A scan of the batch's own rows: a `VALUES` list, the `json_each` of a bound key set, or the CTE of the part count update.
fn batch_sized(detail: &str) -> bool {
    detail.contains("CONSTANT ROW")
        || detail.contains("json_each")
        || detail.contains("VALUES CLAUSE")
        || detail == "SCAN delta"
}

#[test]
fn every_statement_is_served_by_an_index() -> Res<()> {
    block_on(async {
        let backend = RecordingBackend { log: default() };
        laws::replay_reconstructs_membership_and_access(&backend).await?;
        laws::collapse_and_tombstones(&backend).await?;
        laws::pending_goes_live_once(&backend).await?;
        laws::authorization_follows_current_access(&backend).await?;
        laws::revocation_hides_the_parts_buckets(&backend).await?;
        laws::revocation_mid_stream_hides_the_unread_rows(&backend).await?;
        laws::object_routes_follow_the_readable_parts(&backend).await?;
        laws::authorized_reads_carry_their_payloads(&backend).await?;
        laws::removals_reach_every_route_above_its_bound(&backend).await?;
        laws::dirty_count_counts_log_rows(&backend).await?;
        laws::unchanged_writes_stamp_nothing(&backend).await?;

        let statements = backend.log.lock().expect(ERROR_MUTEX).clone();
        assert!(
            statements.len() > 30,
            "the laws should have run many distinct statements, ran {}",
            statements.len()
        );
        assert_no_scans(statements).await
    })
}

/// Every okv statement, the paged reads (each selector, and the continuation of a revision
/// that spans statements) and `prune` included, is served by an index.
#[test]
fn every_okv_statement_is_served_by_an_index() -> Res<()> {
    block_on(async {
        let backend = RecordingOkvBackend {
            log: default(),
            tables: default(),
        };
        okv_laws::collapse(&backend).await?;
        okv_laws::tombstones(&backend).await?;
        okv_laws::soft_limits(&backend).await?;
        okv_laws::get_reads_your_writes(&backend).await?;
        okv_laws::consumer_cursors(&backend).await?;
        okv_laws::prune_by_lowest_cursor(&backend).await?;

        // One revision bigger than a page, so the read continues from the last row it saw, under
        // each kind of selector.
        let db = backend.open().await?;
        let table = backend.table(&db).await?;
        let mut tx = backend.begin(&db).await?;
        let batch = (0..8_u8)
            .map(|key| Mutation::Put {
                key: vec![key],
                value: vec![key],
            })
            .collect();
        table.apply(&mut tx, batch).await?;
        backend.commit(tx).await?;
        let mut page = Page::default();
        for selector in [
            Selector::All,
            Selector::Keys((0..8_u8).map(|key| vec![key]).collect()),
            Selector::Range(KeyRange {
                start: Bound::Included(vec![1]),
                end: Bound::Excluded(vec![6]),
            }),
            Selector::Range(KeyRange {
                start: Bound::Excluded(vec![1]),
                end: Bound::Included(vec![6]),
            }),
        ] {
            table
                .read(&selector, 0, 1, limits(2, None), &mut page)
                .await?;
            assert!(!page.entries.is_empty());
        }

        let statements = backend.log.lock().expect(ERROR_MUTEX).clone();
        assert!(
            statements.keys().any(|sql| sql.contains("(rev, key) >")),
            "the continuation statement was never run"
        );
        assert!(
            statements
                .keys()
                .any(|sql| sql.contains("DELETE FROM big_automata_okv_entries")),
            "prune was never run"
        );
        assert_no_scans(statements).await
    })
}

/// Fail on any plan line that scans, where `statements` are the recorded SQL with a set of
/// parameters each.
async fn assert_no_scans(statements: BTreeMap<String, Vec<SqlValue>>) -> Res<()> {
    {
        let db = RUNTIME.ephemeral(MIGRATIONS)?;
        let mut scans = Vec::new();
        for (sql, params) in &statements {
            // `execute`, not `query`: the statements recorded include writes, and a plan of one
            // is still only a plan.
            let plan = db
                .execute(&format!("EXPLAIN QUERY PLAN {sql}"), params.clone())
                .await?;
            let mut entries_plan = None;
            for row in plan.result_set.rows {
                let detail = row.get::<String>("detail")?;
                if detail.starts_with("SEARCH big_automata_okv_entries") {
                    entries_plan = Some(detail.clone());
                }
                if detail.starts_with("SCAN") && !batch_sized(&detail) {
                    scans.push(format!(
                        "{detail}\n    in {}",
                        sql.split_whitespace().collect::<Vec<_>>().join(" ")
                    ));
                }
            }
            // A key set is looked up by the entries' primary key `(namespace_id, key)`, the
            // index that holds each key once, not walked through `(namespace_id, rev, key)`.
            if sql.contains("json_each") && sql.contains("big_automata_okv_entries") {
                let line = entries_plan.unwrap_or_else(|| {
                    panic!("a key set statement does not search the entries: {sql}")
                });
                println!("PLAN okv key set: {line}");
                assert!(
                    line.contains("PRIMARY KEY")
                        && !line.contains("big_automata_okv_entries_by_rev"),
                    "a key set is not served by the primary key: {line}\n    in {sql}"
                );
            }
        }
        assert!(
            scans.is_empty(),
            "statements that scan:\n{}",
            scans.join("\n")
        );
        Ok(())
    }
}

/// The objects that are members of no part are a range of a partial index, not a scan of the
/// objects: the read the payload-dropping trait will add rests on it.
#[test]
fn part_less_objects_are_a_partial_index_range() -> Res<()> {
    block_on(async {
        let db = RUNTIME.ephemeral(MIGRATIONS)?;
        db.migrate().await?;
        let plan = db
            .query(
                "EXPLAIN QUERY PLAN
                 SELECT obj_key
                   FROM big_automata_objs
                  WHERE part_count = 0
                  ORDER BY obj_key",
                (),
            )
            .await?;
        let details = plan
            .rows
            .iter()
            .map(|row| row.get::<String>("detail"))
            .collect::<Result<Vec<_>, _>>()?;
        assert!(
            details
                .iter()
                .all(|detail| detail.contains("big_automata_objs_partless")),
            "{details:?}"
        );
        Ok(())
    })
}
