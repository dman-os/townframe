//! The executor held to its laws: atomicity at every write of a plan, the frontier compare-and-set,
//! one revision per plan, optimistic key-epoch validation, an interrupted write, and a
//! differential against a naive in-memory model of all four stores over random plans.
//!
//! Everything runs on real SQLite databases. Faults are injected by [`flaky::Flaky`], a database
//! handle that fails (or stops forever at) the n-th write statement of a transaction, whichever
//! store issues it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;

use arbitrary::Arbitrary;
use arbitrary::Unstructured;
use automerge::ChangeHash;
use beekem::id::TreeId;
use beekem::operation::CgkaOperation;
use big_automata::ObjKey;
use big_automata::PartKey;
use big_automata::part_store::PartStoreRead;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use big_automata_sqlite::SqlitePartStore;
use ed25519_dalek::SigningKey;
use keyhive_core::principal::document::id::DocumentId;
use keyhive_crypto::identifier::Identifier;
use keyhive_crypto::share_key::ShareSecretKey;
use keyhive_crypto::symmetric_key::SymmetricKey;
use keyhive_crypto::verified::Verified;
use keyhive_sqlite::Kek;
use keyhive_sqlite::SqliteKeyhiveStore;
use keyline::power::Power;
use keyline::test_utils::delegation;
use keyline::test_utils::id;
use keyline::test_utils::signed;
use keyline_sqlite::SqliteKeyline;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::id::SedimentreeId;
use sqlite_rs::Database;
use sqlite_rs::QueryExecutor;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteDb;
use sqlite_rs::SqliteRuntime;

use super::CommitPlan;
use super::DocCommit;
use super::Executed;
use super::Executor;
use super::Frontier;
use super::FrontierWrite;
use super::Verdict;
use crate::coordinator::DocId;
use crate::sedimentree::SedimentreeStore;
use crate::sedimentree::suite::pooled;
use crate::sedimentree::suite::seal_commit;

mod atomic;
mod differential;
mod epoch;
mod flaky;
mod frontier;

pub(crate) type Ref = [u8; 32];
pub(crate) type Exec<D> = Executor<D, CountLeadingZeroBytes, Ref, ()>;

pub(crate) static RUNTIME: LazyLock<SqliteRuntime> = LazyLock::new(|| {
    SqliteRuntime::new(RuntimeConfig {
        workers: 2,
        ..RuntimeConfig::default()
    })
});

/// Bring a database to the schema of all four stores.
pub(crate) async fn migrate(db: &Database) {
    db.apply_migrations(keyline_sqlite::MIGRATION_TABLE, keyline_sqlite::MIGRATIONS)
        .await
        .unwrap();
    db.apply_migrations(keyhive_sqlite::MIGRATION_TABLE, keyhive_sqlite::MIGRATIONS)
        .await
        .unwrap();
    db.apply_migrations(
        big_automata_sqlite::MIGRATION_TABLE,
        big_automata_sqlite::MIGRATIONS,
    )
    .await
    .unwrap();
}

/// A new, empty database on its own ephemeral file, with every store's schema.
pub(crate) async fn fresh_db() -> Database {
    let db = RUNTIME.ephemeral(crate::MIGRATIONS).unwrap();
    migrate(&db).await;
    db
}

/// A database in a file, which can be reopened.
pub(crate) async fn file_db(path: &Path) -> Database {
    let db = RUNTIME.database(path, crate::MIGRATIONS, crate::MIGRATION_TABLE);
    migrate(&db).await;
    db
}

/// The four stores over one database handle, and the executor over them.
pub(crate) struct Rig<D: SqliteDb> {
    pub db: D,
    pub exec: Exec<D>,
    pub trees: SedimentreeStore<D, CountLeadingZeroBytes>,
    pub keyhive: SqliteKeyhiveStore<Ref, D>,
    pub parts: SqlitePartStore<D>,
}

impl<D: SqliteDb> Rig<D> {
    pub(crate) async fn on(db: D) -> Self {
        let trees = SedimentreeStore::new(db.clone(), CountLeadingZeroBytes);
        let keyline = SqliteKeyline::<(), D>::new(db.clone());
        let keyhive = SqliteKeyhiveStore::<Ref, D>::new(db.clone(), Kek::new(1, [7; 32]));
        let parts = SqlitePartStore::open(db.clone(), Arc::new(web_time::Instant::now))
            .await
            .unwrap();
        Self {
            exec: Executor::new(trees.clone(), keyline, keyhive.clone(), parts.clone()),
            db,
            trees,
            keyhive,
            parts,
        }
    }

    pub(crate) async fn run(&self, docs: Vec<Doc>) -> Executed {
        committed(self.exec.execute(plan(docs).await).await.unwrap())
    }

    /// The document's key epoch, read in a snapshot.
    pub(crate) async fn epoch(&self, doc: u8) -> u64 {
        let tx = self.db.begin_read().await.unwrap();
        let epoch = self
            .keyhive
            .session(&tx)
            .doc_epoch(doc_key(doc))
            .await
            .unwrap();
        D::end_read(tx).await.unwrap();
        epoch
    }

    /// The published frontier of a document and the parts its object is in.
    pub(crate) async fn frontier(&self, doc: u8) -> Option<(Frontier, BTreeSet<PartKey>)> {
        let state = self
            .parts
            .obj_states(&[frontier_obj(doc)])
            .await
            .unwrap()
            .remove(0)?;
        let (payload, _) = state.content?;
        Some((Frontier::from_payload(&payload), state.parts))
    }

    pub(crate) async fn trees_latest(&self) -> Rev {
        self.trees.latest_rev().await.unwrap()
    }

    /// The trees as the change feed shows them: tree to its latest revision.
    pub(crate) async fn feed(&self) -> BTreeMap<SedimentreeId, Rev> {
        let latest = self.trees.latest_rev().await.unwrap();
        let mut page = Page::default();
        if latest > 0 {
            let limits = Limits {
                max_entries: NonZeroUsize::new(1000).unwrap(),
                max_bytes: None,
            };
            self.trees
                .read(&(), 0, latest, limits, &mut page)
                .await
                .unwrap();
        }
        page.entries
            .into_iter()
            .map(|entry| (entry.key, entry.rev))
            .collect()
    }
}

pub(crate) fn committed(verdict: Verdict) -> Executed {
    match verdict {
        Verdict::Committed(executed) => executed,
        Verdict::Stale { doc } => panic!("the plan was stale on document {doc}"),
    }
}

pub(crate) fn tree(doc: u8) -> SedimentreeId {
    SedimentreeId::new([doc + 1; 32])
}

/// The keyhive document a test document stands for.
pub(crate) fn doc_key(doc: u8) -> DocumentId {
    DocumentId::from(Identifier::from(&SigningKey::from_bytes(
        &[doc.wrapping_add(100); 32],
    )))
}

pub(crate) fn frontier_obj(doc: u8) -> ObjKey {
    ObjKey::new(&[b'f', doc])
}

pub(crate) fn group() -> PartKey {
    PartKey::new(b"group")
}

pub(crate) fn extra_part() -> PartKey {
    PartKey::new(b"extra")
}

pub(crate) fn head(n: u8) -> ChangeHash {
    ChangeHash([n; 32])
}

pub(crate) fn content_ref(n: u8) -> Ref {
    [n; 32]
}

pub(crate) fn content_key(doc: u8, content: u8) -> SymmetricKey {
    SymmetricKey::from([
        doc, content, 0xC0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28,
    ])
}

pub(crate) fn certificate(n: u8) -> keyline::certificate::VerifiedCertificate<()> {
    signed(delegation(id(1), id(n), id(2), Power::Read))
}

/// The first `count` operations of a document's CGKA history, each naming the one before it. The
/// same arguments give the same operations, so a longer chain extends a shorter one.
pub(crate) fn cgka_chain(doc: u8, count: u64) -> Vec<Arc<Verified<CgkaOperation>>> {
    let signer = SigningKey::from_bytes(&[9; 32]);
    let mut chain: Vec<Arc<Verified<CgkaOperation>>> = Vec::new();
    for index in 0..count {
        let bytes: [u8; 512] = std::array::from_fn(|at| {
            (at as u8)
                .wrapping_mul(31)
                .wrapping_add(doc.wrapping_mul(7))
                .wrapping_add((index as u8).wrapping_mul(13))
        });
        let mut op = CgkaOperation::arbitrary(&mut Unstructured::new(&bytes))
            .expect("512 bytes make an operation");
        op.doc_id = TreeId::from(Identifier::from(doc_key(doc)));
        op.predecessors = chain
            .last()
            .map(|prior| prior.digest())
            .into_iter()
            .collect();
        chain.push(Arc::new(Verified::sign_sync(&op, &signer).unwrap()));
    }
    chain
}

pub(crate) fn secret(n: u8) -> (keyhive_crypto::share_key::ShareKey, ShareSecretKey) {
    let secret = ShareSecretKey::force_from_bytes([n; 32]);
    (secret.share_key(), secret)
}

/// One document's part of a plan, as a test states it.
#[derive(Clone, Debug, Default)]
pub(crate) struct Doc {
    pub id: u8,
    /// Heads of loose commits to save in the document's tree.
    pub commits: Vec<u8>,
    /// Heads to publish, and the revision they are conditional on.
    pub frontier: Option<(Vec<u8>, Rev)>,
    pub if_epoch: Option<u64>,
}

impl Doc {
    pub(crate) fn new(id: u8) -> Self {
        Self {
            id,
            ..Self::default()
        }
    }

    pub(crate) fn commits(mut self, heads: &[u8]) -> Self {
        self.commits = heads.to_vec();
        self
    }

    pub(crate) fn frontier(mut self, heads: &[u8], if_prior_rev: Rev) -> Self {
        self.frontier = Some((heads.to_vec(), if_prior_rev));
        self
    }

    pub(crate) fn if_epoch(mut self, epoch: u64) -> Self {
        self.if_epoch = Some(epoch);
        self
    }

    pub(crate) async fn build(&self) -> DocCommit {
        let mut commits = Vec::new();
        for head in &self.commits {
            commits
                .push(seal_commit(tree(self.id), pooled(*head), BTreeSet::new(), 0, Some(0)).await);
        }
        DocCommit {
            doc: DocId::from(self.id),
            tree: tree(self.id),
            commits,
            fragments: Vec::new(),
            frontier: self
                .frontier
                .as_ref()
                .map(|(heads, if_prior_rev)| FrontierWrite {
                    obj: frontier_obj(self.id),
                    parts: vec![group()],
                    heads: heads.iter().copied().map(head).collect(),
                    if_prior_rev: *if_prior_rev,
                }),
            keyhive_doc: doc_key(self.id),
            if_epoch: self.if_epoch,
        }
    }
}

/// A plan of just these documents.
pub(crate) async fn plan(docs: Vec<Doc>) -> CommitPlan<Ref, ()> {
    let mut plan = CommitPlan::default();
    for doc in docs {
        plan.docs.push(doc.build().await);
    }
    plan
}

/// A plan that writes to every store: two documents (commits, frontier rows), two certificates,
/// a rotation's secret and CGKA operations, a content key, content heads and group membership.
pub(crate) async fn rich_plan() -> CommitPlan<Ref, ()> {
    let mut plan = plan(vec![
        Doc::new(0).commits(&[0, 1]).frontier(&[1], 0).if_epoch(0),
        Doc::new(1).commits(&[2]).frontier(&[2], 0),
    ])
    .await;
    plan.certificates = vec![certificate(3), certificate(4)];
    plan.keyhive.key_pairs = vec![secret(1)];
    plan.keyhive.cgka_ops = vec![(doc_key(0), cgka_chain(0, 2))];
    plan.keyhive.content_heads = vec![(doc_key(0), vec![content_ref(1)])];
    plan.keyhive.content_keys = vec![(doc_key(0), content_ref(1), content_key(0, 1))];
    plan.parts.add = vec![(ObjKey::new(b"extra-object"), extra_part())];
    plan
}

/// Every row of every table (the migration bookkeeping aside), as text, for comparing the whole
/// database before and after.
pub(crate) async fn dump(db: &impl QueryExecutor) -> Vec<(String, Vec<String>)> {
    let tables: Vec<String> = db
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '%migrations%' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            (),
        )
        .await
        .unwrap()
        .all()
        .unwrap();
    let mut out = Vec::new();
    for table in tables {
        let mut rows: Vec<String> = db
            .query(&format!("SELECT * FROM \"{table}\""), ())
            .await
            .unwrap()
            .rows
            .iter()
            .map(|row| format!("{row:?}"))
            .collect();
        rows.sort();
        out.push((table, rows));
    }
    out
}
