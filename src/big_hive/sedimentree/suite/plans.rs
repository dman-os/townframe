//! Every statement the store runs is served by an index, and what a mutation costs in
//! statements.
//!
//! The store is run through a database wrapper that records each statement text with its
//! parameters, over every kind of operation; then `EXPLAIN QUERY PLAN` is asked of each. A plan
//! line that scans a table or an index (rather than searching one) fails the test, except the
//! listing of every live tree id and the batch's own `VALUES`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use futures::executor::block_on;
use sedimentree_core::depth::CountLeadingZeroBytes;
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
use subduction_core::minimized_tree_store::MinimizedTreeStore;
use subduction_core::storage::traits::Storage;

use super::Spec;
use super::fresh_db;
use super::pcs_hash;
use super::seal;
use super::tree_id;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::RevSource;

use crate::sedimentree::Mutation;
use crate::sedimentree::SedimentreeStore;

type Log = Arc<Mutex<Vec<(String, Vec<SqlValue>)>>>;

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
    log.lock().unwrap().push((sql.to_owned(), params.to_vec()));
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

type Recorded = SedimentreeStore<Recording, CountLeadingZeroBytes>;

fn recorded() -> (Recorded, Log) {
    let log = Log::default();
    let db = Recording {
        db: fresh_db(),
        log: Arc::clone(&log),
    };
    (SedimentreeStore::new(db, CountLeadingZeroBytes), log)
}

fn commit(head: u8, parents: &[u8]) -> Spec {
    Spec::Commit {
        head,
        parents: parents.to_vec(),
        blob: 0,
        pcs: Some(0),
    }
}

async fn save(store: &Recorded, tree: u8, specs: &[Spec]) {
    let (commits, fragments) = seal(tree_id(tree), specs).await;
    store
        .apply(tree_id(tree), Mutation::Save { commits, fragments })
        .await
        .unwrap();
}

/// A scan of the batch's own rows, or the listing of every live tree, which is as big as the
/// hive by definition.
fn allowed_scan(detail: &str, sql: &str) -> bool {
    detail.contains("CONSTANT ROW")
        || detail.contains("VALUES CLAUSE")
        || (detail.starts_with("SCAN sedimentree_trees")
            && sql.contains("WHERE live = 1")
            && !sql.contains("tree_id ="))
}

#[test]
fn every_statement_is_served_by_an_index() {
    block_on(async {
        let (store, log) = recorded();
        // Chains, a fragment that prunes, a dominating fragment, duplicates, equivocation.
        save(
            &store,
            1,
            &[
                commit(0, &[]),
                commit(1, &[0]),
                commit(2, &[1]),
                commit(3, &[2]),
            ],
        )
        .await;
        save(
            &store,
            1,
            &[Spec::Fragment {
                head: 2,
                boundary: vec![5],
                checkpoints: vec![1, 0],
                blob: 0,
                pcs: Some(1),
            }],
        )
        .await;
        save(
            &store,
            1,
            &[Spec::Fragment {
                head: 4,
                boundary: vec![5],
                checkpoints: vec![2],
                blob: 0,
                pcs: None,
            }],
        )
        .await;
        save(&store, 1, &[commit(3, &[2])]).await;
        save(
            &store,
            2,
            &[
                commit(0, &[]),
                Spec::Commit {
                    head: 0,
                    parents: vec![],
                    blob: 1,
                    pcs: None,
                },
            ],
        )
        .await;
        let id = tree_id(1);
        store
            .apply(tree_id(2), Mutation::DeleteCommit(super::pooled(0)))
            .await
            .unwrap();
        store
            .apply(tree_id(2), Mutation::DeleteCommits)
            .await
            .unwrap();
        store
            .apply(id, Mutation::DeleteFragment(super::pooled(4)))
            .await
            .unwrap();
        store.apply(id, Mutation::DeleteFragments).await.unwrap();
        store.apply(tree_id(3), Mutation::Register).await.unwrap();
        store.apply(tree_id(3), Mutation::Retire).await.unwrap();
        save(&store, 1, &[commit(5, &[4])]).await;

        // Every read.
        Storage::<future_form::Sendable>::load_all_sedimentree_ids(&store)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::contains_sedimentree_id(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::list_commit_ids(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::list_fragment_ids(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::load_loose_commits(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::load_fragments(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::load_loose_commit_metas(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::load_fragment_metas(&store, id)
            .await
            .unwrap();
        Storage::<future_form::Sendable>::load_loose_commit(&store, id, super::pooled(5))
            .await
            .unwrap();
        Storage::<future_form::Sendable>::load_fragment(&store, id, super::pooled(4))
            .await
            .unwrap();
        store.blobs_under(id, pcs_hash(0)).await.unwrap();
        store.blobs_under(id, pcs_hash(1)).await.unwrap();
        MinimizedTreeStore::<future_form::Sendable>::get(&store, id)
            .await
            .unwrap();
        let latest = store.latest_rev().await.unwrap();
        let mut page = Page::default();
        let limits = |entries| Limits {
            max_entries: std::num::NonZeroUsize::new(entries).unwrap(),
            max_bytes: None,
        };
        store
            .read(&(), 0, latest, limits(1000), &mut page)
            .await
            .unwrap();
        // A window with a revision over the bound, and one cut mid-window.
        store
            .read(&(), 0, latest, limits(1), &mut page)
            .await
            .unwrap();
        store
            .read(&(), 0, latest, limits(2), &mut page)
            .await
            .unwrap();

        let mut statements = BTreeMap::new();
        for (sql, params) in log.lock().unwrap().iter() {
            statements
                .entry(sql.clone())
                .or_insert_with(|| params.clone());
        }
        assert!(
            statements.len() >= 20,
            "the workload ran {} distinct statements",
            statements.len()
        );

        let db = fresh_db();
        let mut scans = Vec::new();
        let mut plans = BTreeMap::new();
        for (sql, params) in &statements {
            let plan = db
                .execute(&format!("EXPLAIN QUERY PLAN {sql}"), params.clone())
                .await
                .unwrap();
            for row in plan.result_set.rows {
                let detail = row.get::<String>("detail").unwrap();
                if detail.starts_with("SCAN") && !allowed_scan(&detail, sql) {
                    scans.push(format!(
                        "{detail}\n    in {}",
                        sql.split_whitespace().collect::<Vec<_>>().join(" ")
                    ));
                }
                plans.insert(detail, sql.clone());
            }
        }
        assert!(
            scans.is_empty(),
            "statements that scan:\n{}",
            scans.join("\n")
        );

        // The indexes the migration promises are the ones the plans use.
        let used = |name: &str| plans.keys().any(|detail| detail.contains(name));
        for index in [
            "sedimentree_trees_by_rev",
            "sedimentree_commits_by_pcs_update",
            "sedimentree_fragments_by_pcs_update",
            "PRIMARY KEY",
        ] {
            assert!(used(index), "no plan uses {index}: {plans:#?}");
        }
        for detail in plans.keys() {
            println!("PLAN {detail}");
        }
    });
}

/// How many statements one transaction runs per kind of write, counted (the `BEGIN` and `COMMIT`
/// of the transaction are not statements of the store). The numbers are the budget: a change
/// that adds one has to say why.
#[test]
fn statements_per_write() {
    block_on(async {
        let (store, log) = recorded();
        let count = async |what: &str, mutation: Mutation, id: u8| {
            let before = log.lock().unwrap().len();
            store.apply(tree_id(id), mutation).await.unwrap();
            let statements = log.lock().unwrap().len() - before;
            println!("STATEMENTS {what}: {statements}");
            statements
        };
        let save_of = async |specs: &[Spec], id: u8| {
            let (commits, fragments) = seal(tree_id(id), specs).await;
            Mutation::Save { commits, fragments }
        };

        let first = save_of(&[commit(0, &[])], 1).await;
        assert_eq!(count("the first commit of a tree", first, 1).await, 3);
        let next = save_of(&[commit(1, &[0])], 1).await;
        assert_eq!(count("a commit into an existing tree", next, 1).await, 4);
        let duplicate = save_of(&[commit(1, &[0])], 1).await;
        assert_eq!(count("a duplicate commit", duplicate, 1).await, 2);
        let batch = save_of(&[commit(2, &[1]), commit(3, &[2])], 1).await;
        assert_eq!(count("two commits in one batch", batch, 1).await, 4);
        let fragment = save_of(
            &[Spec::Fragment {
                head: 2,
                boundary: vec![5],
                checkpoints: vec![1, 0],
                blob: 0,
                pcs: None,
            }],
            1,
        )
        .await;
        assert_eq!(
            count("a fragment that covers commits", fragment, 1).await,
            5
        );
        assert_eq!(
            count(
                "deleting a commit",
                Mutation::DeleteCommit(super::pooled(3)),
                1
            )
            .await,
            2
        );
        assert_eq!(
            count(
                "deleting a commit that is not there",
                Mutation::DeleteCommit(super::pooled(3)),
                1
            )
            .await,
            1
        );
        assert_eq!(
            count("registering a new tree", Mutation::Register, 2).await,
            1
        );
        assert_eq!(
            count("registering a live tree", Mutation::Register, 2).await,
            1
        );
        assert_eq!(count("retiring a tree", Mutation::Retire, 1).await, 3);
        assert_eq!(count("retiring a dead tree", Mutation::Retire, 1).await, 1);
    });
}
