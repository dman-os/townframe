//! Every statement the store runs is served by an index, except the reads whose answer is a whole
//! table.
//!
//! The store runs through a database wrapper that records each distinct statement text with one
//! set of its parameters, over every read and write of the surface; then `EXPLAIN QUERY PLAN` is
//! asked of each. A plan line that scans a `kh_` table fails the test unless the statement is one
//! of the whole-table reads, which are pinned to the scan they should do. The scan of an insert's
//! own `VALUES` list is as big as the batch, not the table.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use future_form::Local;
use futures::executor::block_on;
use keyhive_core::store::KeyhiveStore;
use sqlite_rs::Database;
use sqlite_rs::ExecuteResult;
use sqlite_rs::Executor;
use sqlite_rs::IntoParams;
use sqlite_rs::QueryExecutor;
use sqlite_rs::ReadTx;
use sqlite_rs::ResultSet;
use sqlite_rs::SqlResult;
use sqlite_rs::SqlValue;
use sqlite_rs::SqliteDb;
use sqlite_rs::Tx;

use super::Keks;
use super::POOL;
use super::Ref;
use super::content_key;
use super::fresh_db;
use super::kek;
use crate::Kek;
use crate::SqliteKeyhiveStore;
use crate::sqlite::queries;

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
        .expect("the log mutex is not poisoned")
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

impl SqliteDb for Recording {
    type Write = RecordingWrite;
    type Read = RecordingRead;

    fn supports_concurrent_readers(&self) -> bool {
        self.db.supports_concurrent_readers()
    }

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

/// The plan of `sql`, one line per step.
async fn plan(db: &Database, sql: &str, params: &[SqlValue]) -> Vec<String> {
    db.execute(&format!("EXPLAIN QUERY PLAN {sql}"), params.to_vec())
        .await
        .unwrap()
        .result_set
        .rows
        .iter()
        .map(|row| row.get::<String>("detail").unwrap())
        .collect()
}

#[test]
fn every_statement_is_served_by_an_index() {
    block_on(async {
        let log = Log::default();
        let recording = Recording {
            db: fresh_db(),
            log: Arc::clone(&log),
        };
        let store = SqliteKeyhiveStore::<Ref, _>::new(recording.clone(), kek());
        let pool = &*POOL;
        let (who, doc) = (pool.individuals[0], pool.docs[0]);
        let ops: Vec<_> = pool
            .key_ops
            .iter()
            .filter(|(i, _)| *i == 0)
            .map(|(_, op)| Arc::clone(op))
            .collect();
        let cgka: Vec<_> = pool
            .cgka_ops
            .iter()
            .filter(|(d, _)| *d == 0)
            .map(|(_, op)| Arc::clone(op))
            .collect();
        let s = &store;
        KeyhiveStore::<Local, Ref>::put_key_pairs(s, &pool.key_pairs)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::append_prekey_ops(s, who, &ops)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::append_cgka_ops(s, doc, &cgka)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::put_content_heads(s, doc, &pool.refs)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::cache_content_key(s, doc, &pool.refs[0], &content_key(0, 0))
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::put_pending_events(s, &pool.events)
            .await
            .unwrap();
        let digest = pool.events[0].digest();
        KeyhiveStore::<Local, Ref>::drop_pending_events(s, &[digest])
            .await
            .unwrap();

        KeyhiveStore::<Local, Ref>::load_key_pairs(s).await.unwrap();
        KeyhiveStore::<Local, Ref>::load_prekey_ops(s, who)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::load_prekey_ops_by_digest(s, who, &[ops[0].digest()])
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::list_prekey_op_digests(s, who)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::list_individuals(s)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::total_op_count(s).await.unwrap();
        KeyhiveStore::<Local, Ref>::load_cgka_ops(s, doc)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::load_cgka_ops_by_digest(s, doc, &[cgka[0].digest()])
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::list_cgka_op_index(s, doc)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::list_documents(s).await.unwrap();
        KeyhiveStore::<Local, Ref>::load_content_heads(s, doc)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::get_cached_content_key(s, doc, &pool.refs[0])
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::load_pending_events(s)
            .await
            .unwrap();
        let read = recording.begin_read().await.unwrap();
        store.session(&read).doc_epoch(doc).await.unwrap();
        Recording::end_read(read).await.unwrap();
        // A KEK rotation's statements.
        let rotated = SqliteKeyhiveStore::<Ref, _>::new(
            recording.clone(),
            Keks(vec![kek(), Kek::new(2, [9; 32])]),
        );
        let tx = recording.begin_write().await.unwrap();
        assert_eq!(rotated.session(&tx).rewrap_deks().await.unwrap(), 2);
        Recording::commit(tx).await.unwrap();

        let statements = log.lock().unwrap().clone();
        assert!(
            statements.len() >= 20,
            "the workload ran {} distinct statements",
            statements.len()
        );
        let db = fresh_db();
        let whole_table = [
            queries::KEY_PAIRS_ALL_SQL,
            queries::PENDING_EVENTS_ALL_SQL,
            queries::DEKS_STALE_SQL,
        ];
        let mut scans = Vec::new();
        let mut plans = BTreeMap::new();
        for (sql, params) in &statements {
            let lines = plan(&db, sql, params).await;
            let whole = whole_table.contains(&sql.as_str());
            for line in &lines {
                let scan = line.starts_with("SCAN kh_");
                // A search bounded by a key's prefix is the point; a table that has no such prefix
                // to bound by would say `SCAN`.
                if scan && !whole {
                    scans.push(format!(
                        "{line}\n    in {}",
                        sql.split_whitespace().collect::<Vec<_>>().join(" ")
                    ));
                }
            }
            plans.insert(sql.as_str(), lines);
        }
        assert!(
            scans.is_empty(),
            "statements that scan:\n{}",
            scans.join("\n")
        );

        let plan_of = |sql: &str| {
            plans
                .get(sql)
                .unwrap_or_else(|| panic!("never ran: {sql}"))
                .join("\n")
        };
        let has = |sql: &str, needle: &str| {
            let lines = plan_of(sql);
            assert!(lines.contains(needle), "{needle:?} not in\n{lines}");
        };
        // The access paths the schema is for.
        has(queries::PREKEY_OPS_OF_SQL, "PRIMARY KEY (individual_id=?)");
        has(
            queries::PREKEY_OP_DIGESTS_OF_SQL,
            "PRIMARY KEY (individual_id=?)",
        );
        has(
            queries::CGKA_OPS_OF_SQL,
            "sqlite_autoindex_kh_cgka_op_1 (doc_id=?)",
        );
        has(queries::CONTENT_HEADS_GET_SQL, "PRIMARY KEY (doc_id=?)");
        has(
            queries::DOC_EPOCH_SQL,
            "SEARCH kh_doc_epoch USING PRIMARY KEY (doc_id=?)",
        );
        has(
            queries::CONTENT_KEY_GET_SQL,
            "PRIMARY KEY (doc_id=? AND content_ref=?)",
        );
        has(
            queries::DEK_GET_SQL,
            "PRIMARY KEY (dek_id=? AND dek_version=?)",
        );
        // The dependency index lists operations and edges without reading an operation's bytes:
        // the operations side is the covering unique index.
        has(
            queries::CGKA_OP_INDEX_OF_SQL,
            "SEARCH o USING COVERING INDEX sqlite_autoindex_kh_cgka_op_1 (doc_id=?)",
        );
        has(
            queries::CGKA_OP_INDEX_OF_SQL,
            "SEARCH p USING PRIMARY KEY (doc_id=? AND digest=?)",
        );
        // The by-digest reads are point searches, per digest, not scans of the document.
        let by_digest: Vec<_> = plans
            .iter()
            .filter(|(sql, _)| sql.contains(" AND digest IN "))
            .collect();
        assert_eq!(by_digest.len(), 2, "{by_digest:?}");
        for (sql, lines) in by_digest {
            let lines = lines.join("\n");
            assert!(
                lines.contains("digest=?") || lines.contains("(doc_id=? AND digest=?)"),
                "{sql}\n{lines}"
            );
        }
        // The counter is a read of one row by its key, however many operations are held.
        has(
            queries::TOTAL_OP_COUNT_SQL,
            "SEARCH kh_op_count USING INTEGER PRIMARY KEY",
        );
        // The loose index scans step with a search, never a scan of the operations.
        for sql in [queries::INDIVIDUALS_SQL, queries::DOCUMENTS_SQL] {
            let lines = plan_of(sql);
            assert!(lines.contains("SEARCH kh_"), "{lines}");
        }
        for (sql, lines) in &plans {
            println!(
                "PLAN {}\n  {}",
                sql.split_whitespace().collect::<Vec<_>>().join(" "),
                lines.join("\n  ")
            );
        }
    });

    // The reverse edge: who names an operation as a predecessor is answered from the covering
    // index, which no read of the surface needs but a visibility walk does.
    block_on(async {
        let db = fresh_db();
        let lines = plan(
            &db,
            "SELECT digest FROM kh_cgka_op_pred WHERE doc_id = ? AND predecessor = ?",
            &[SqlValue::Blob(vec![0; 32]), SqlValue::Blob(vec![0; 32])],
        )
        .await
        .join("\n");
        assert!(
            lines.contains(
                "COVERING INDEX kh_cgka_op_pred_by_predecessor (doc_id=? AND predecessor=?)"
            ),
            "{lines}"
        );
    });
}
