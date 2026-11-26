//! Every statement the backend runs is served by an index, except the three that read the whole
//! set because their answer is the whole set.
//!
//! The backend runs through a database wrapper that records each distinct statement text with
//! one set of its parameters, over every query and an insert; then `EXPLAIN QUERY PLAN` is
//! asked of each. A plan line that scans `keyline_certificates` fails the test unless the
//! statement is one of the whole-set reads, which are pinned to the scan they should do. The scan
//! of an insert's own `VALUES` list (and the constant row an `EXISTS` is selected from) is as
//! big as the batch, not the set.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use future_form::Local;
use futures::executor::block_on;
use keyline::contract::Keyline;
use keyline::power::Power;

use keyline::test_utils::conformance::ALICE;
use keyline::test_utils::conformance::BOB;
use keyline::test_utils::conformance::DOC;
use keyline::test_utils::conformance::OWNERS;
use keyline::test_utils::conformance::d;
use keyline::test_utils::conformance::r;
use keyline::test_utils::id;
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

use super::fresh_db;
use super::sign;
use crate::SqliteKeyline;
use crate::sqlite::queries;
use sqlite_rs::SqliteDb;

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
        let store = SqliteKeyline::<(), _>::new(recording.clone());
        let bob = d(ALICE, BOB, DOC, Power::Edit);
        let batch = vec![
            sign(d(DOC, OWNERS, DOC, Power::Admin)),
            sign(d(DOC, ALICE, OWNERS, Power::Admin)),
            sign(bob),
            sign(r::<()>(ALICE, &bob)),
        ];
        let digests = [d(DOC, OWNERS, DOC, Power::Admin).digest()];
        let cert = keyline::certificate::CertificateId::from(digests[0]);
        let version = Keyline::<Local>::insert(&store, batch).await.unwrap();
        assert!(version > keyline::contract::Version::INITIAL);
        let k = &store;
        Keyline::<Local>::contains(k, cert).await.unwrap();
        Keyline::<Local>::get(k, cert).await.unwrap();
        Keyline::<Local>::revocations_of(k, bob.digest())
            .await
            .unwrap();
        Keyline::<Local>::delegations_to(k, id(DOC), id(ALICE))
            .await
            .unwrap();
        Keyline::<Local>::subjects(k).await.unwrap();
        Keyline::<Local>::digest(k).await.unwrap();
        Keyline::<Local>::certificates(k).await.unwrap();
        // A store with an empty cache rebuilds from the rows.
        Keyline::<Local>::members(&SqliteKeyline::<(), _>::new(recording), id(DOC))
            .await
            .unwrap();

        let statements = log.lock().unwrap().clone();
        assert!(
            statements.len() >= 10,
            "the workload ran {} distinct statements",
            statements.len()
        );
        let db = fresh_db();
        let mut scans = Vec::new();
        let mut plans = BTreeMap::new();
        for (sql, params) in &statements {
            let lines = plan(&db, sql, params).await;
            let whole_set = [
                queries::ALL_CERTIFICATES_SQL,
                queries::ALL_CERTIFICATE_IDS_SQL,
                queries::SUBJECTS_SQL,
            ]
            .contains(&sql.as_str());
            for line in &lines {
                // A search bounded only by `kind` walks every certificate of that kind: a scan that
                // looks like an index search.
                let kind_range = line.ends_with("PRIMARY KEY (kind=?)");
                let scan = line.starts_with("SCAN")
                    && !line.contains("VALUES CLAUSE")
                    && !line.contains("CONSTANT ROW");
                if (scan && !whole_set) || (kind_range && !whole_set) {
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
        // The access paths the schema's indexes are for.
        let by_key = plan_of(queries::CERTIFICATE_EXISTS_SQL);
        assert!(
            by_key.contains("PRIMARY KEY (kind=? AND digest=?)"),
            "{by_key}"
        );
        let by_key = plan_of(queries::CERTIFICATE_SIGNED_SQL);
        assert!(
            by_key.contains("PRIMARY KEY (kind=? AND digest=?)"),
            "{by_key}"
        );
        let edges = plan_of(queries::DELEGATIONS_TO_SQL);
        assert!(
            edges.contains("keyline_delegations_by_subject_audience (subject=? AND audience=?"),
            "{edges}"
        );
        let revocations = plan_of(queries::REVOCATIONS_OF_SQL);
        assert!(
            revocations.contains("keyline_revocations_by_target (revoke=?"),
            "{revocations}"
        );
        // The whole-set reads scan, and in the order they return: no sort.
        for sql in [
            queries::ALL_CERTIFICATES_SQL,
            queries::ALL_CERTIFICATE_IDS_SQL,
        ] {
            let scan = plan_of(sql);
            assert!(!scan.contains("TEMP B-TREE"), "{scan}");
        }
        let subjects = plan_of(queries::SUBJECTS_SQL);
        assert!(
            subjects.contains("COVERING INDEX keyline_delegations_by_subject_audience"),
            "{subjects}"
        );
        for (sql, lines) in &plans {
            println!(
                "PLAN {}\n  {}",
                sql.split_whitespace().collect::<Vec<_>>().join(" "),
                lines.join("\n  ")
            );
        }
    });
}
