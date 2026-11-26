//! The SQLite store against `MemoryKeyhiveStore` over random operation sequences: after every
//! step every read of one equals the same read of the other, and a store reopened from the same
//! file answers like the one that wrote it.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use beekem::operation::CgkaOperation;
use future_form::Local;
use futures::executor::block_on;
use keyhive_core::event::static_event::StaticEvent;
use keyhive_core::principal::document::id::DocumentId;
use keyhive_core::principal::individual::id::IndividualId;
use keyhive_core::principal::individual::op::KeyOp;
use keyhive_core::store::CgkaOpIndexRow;
use keyhive_core::store::KeyhiveStore;
use keyhive_core::store::memory::MemoryKeyhiveStore;
use keyhive_crypto::digest::Digest;
use keyhive_crypto::share_key::ShareKey;
use keyhive_crypto::share_key::ShareSecretKey;
use keyhive_crypto::symmetric_key::SymmetricKey;
use keyhive_crypto::verified::Verified;
use proptest::prelude::*;
use sqlite_rs::Database;

use super::POOL;
use super::RUNTIME;
use super::Ref;
use super::Store;
use super::content_key;
use super::kek;
use crate::MIGRATION_TABLE;
use crate::MIGRATIONS;

/// One operation of the store's surface, over indexes into [`POOL`]. A write names the pool's
/// values by index, so a sequence re-adds what it already holds, and adds batches whose members
/// overlap.
#[derive(Clone, Debug)]
pub(crate) enum Step {
    PutKeyPairs(Vec<usize>),
    AppendPrekeyOps(Vec<usize>),
    AppendCgkaOps(Vec<usize>),
    PutContentHeads {
        doc: usize,
        heads: Vec<usize>,
    },
    CacheContentKey {
        doc: usize,
        content: usize,
    },
    PutPendingEvents(Vec<usize>),
    DropPendingEvents(Vec<usize>),
    /// Close the SQLite store and open another over the same file.
    Reopen,
}

pub(crate) fn step() -> impl Strategy<Value = Step> {
    let pool = &*POOL;
    let few = |n: usize| proptest::collection::vec(0..n, 0..6);
    prop_oneof![
        few(pool.key_pairs.len()).prop_map(Step::PutKeyPairs),
        few(pool.key_ops.len()).prop_map(Step::AppendPrekeyOps),
        few(pool.cgka_ops.len()).prop_map(Step::AppendCgkaOps),
        (0..pool.docs.len(), few(pool.refs.len()))
            .prop_map(|(doc, heads)| Step::PutContentHeads { doc, heads }),
        (0..pool.docs.len(), 0..pool.refs.len())
            .prop_map(|(doc, content)| Step::CacheContentKey { doc, content }),
        few(pool.events.len()).prop_map(Step::PutPendingEvents),
        few(pool.events.len()).prop_map(Step::DropPendingEvents),
        Just(Step::Reopen),
    ]
}

/// Apply `step` to `store`, through the trait, as a keyhive's caller would.
async fn apply<S: KeyhiveStore<Local, Ref>>(store: &S, step: &Step) -> Result<(), S::Error> {
    let pool = &*POOL;
    match step {
        Step::PutKeyPairs(picks) => {
            let pairs: Vec<_> = picks.iter().map(|i| pool.key_pairs[*i].clone()).collect();
            store.put_key_pairs(&pairs).await
        }
        Step::AppendPrekeyOps(picks) => {
            // One call per individual, each with the operations that individual signed.
            for (who, id) in pool.individuals.iter().enumerate() {
                let ops: Vec<_> = picks
                    .iter()
                    .filter(|i| pool.key_ops[**i].0 == who)
                    .map(|i| Arc::clone(&pool.key_ops[*i].1))
                    .collect();
                if !ops.is_empty() {
                    store.append_prekey_ops(*id, &ops).await?;
                }
            }
            Ok(())
        }
        Step::AppendCgkaOps(picks) => {
            for (doc, id) in pool.docs.iter().enumerate() {
                let ops: Vec<_> = picks
                    .iter()
                    .filter(|i| pool.cgka_ops[**i].0 == doc)
                    .map(|i| Arc::clone(&pool.cgka_ops[*i].1))
                    .collect();
                if !ops.is_empty() {
                    store.append_cgka_ops(*id, &ops).await?;
                }
            }
            Ok(())
        }
        Step::PutContentHeads { doc, heads } => {
            let heads: Vec<_> = heads.iter().map(|i| pool.refs[*i]).collect();
            store.put_content_heads(pool.docs[*doc], &heads).await
        }
        Step::CacheContentKey { doc, content } => {
            store
                .cache_content_key(
                    pool.docs[*doc],
                    &pool.refs[*content],
                    &content_key(*doc, *content),
                )
                .await
        }
        Step::PutPendingEvents(picks) => {
            let events: Vec<_> = picks.iter().map(|i| pool.events[*i].clone()).collect();
            store.put_pending_events(&events).await
        }
        Step::DropPendingEvents(picks) => {
            let digests: Vec<_> = picks.iter().map(|i| pool.events[*i].digest()).collect();
            store.drop_pending_events(&digests).await
        }
        Step::Reopen => Ok(()),
    }
}

type PrekeyAnswers = (Vec<Arc<KeyOp>>, Vec<Digest<KeyOp>>, Vec<Arc<KeyOp>>);
type CgkaOps = Vec<Arc<Verified<CgkaOperation>>>;
type IndexEntry = (Digest<CgkaOperation>, Vec<Digest<CgkaOperation>>);

/// Every read of the store, in an order that does not depend on the store's, for comparing.
#[derive(Debug, PartialEq)]
struct Answers {
    key_pairs: BTreeMap<ShareKey, ShareSecretKey>,
    individuals: Vec<IndividualId>,
    /// Per individual: all, the digests, and the by-digest probe.
    prekey_ops: Vec<PrekeyAnswers>,
    documents: Vec<DocumentId>,
    /// Per document: all, the by-digest probe, and the index.
    cgka: Vec<(CgkaOps, CgkaOps, Vec<IndexEntry>)>,
    heads: Vec<Option<Vec<Ref>>>,
    keys: Vec<Option<SymmetricKey>>,
    pending: Vec<StaticEvent<Ref>>,
}

async fn answers<S: KeyhiveStore<Local, Ref>>(store: &S) -> Answers
where
    S::Error: std::fmt::Debug,
{
    let pool = &*POOL;
    let mut individuals = store.list_individuals().await.expect("the store answers");
    individuals.sort_by_key(|id| keyhive_crypto::identifier::Identifier::from(*id).to_bytes());
    let mut documents = store.list_documents().await.expect("the store answers");
    documents.sort_by_key(|id| keyhive_crypto::identifier::Identifier::from(*id).to_bytes());

    let mut prekey_ops = Vec::new();
    for who in &pool.individuals {
        let mut all = store
            .load_prekey_ops(*who)
            .await
            .expect("the store answers");
        all.sort_by_key(|op| op.digest().as_slice().to_vec());
        let mut digests = store
            .list_prekey_op_digests(*who)
            .await
            .expect("the store answers");
        digests.sort_by_key(|digest| digest.as_slice().to_vec());
        // Every digest the pool has for anyone, reversed, one twice, one the store never held: the
        // answer is in this order, for the individual's own and for another's alike.
        let mut probe: Vec<Digest<KeyOp>> =
            pool.key_ops.iter().map(|(_, op)| op.digest()).collect();
        probe.reverse();
        probe.push(probe[0]);
        probe.push(Digest::from([0xEE; 32]));
        let by_digest = store
            .load_prekey_ops_by_digest(*who, &probe)
            .await
            .expect("the store answers");
        prekey_ops.push((all, digests, by_digest));
    }

    let mut cgka = Vec::new();
    for doc in &pool.docs {
        let mut all = store.load_cgka_ops(*doc).await.expect("the store answers");
        all.sort_by_key(|op| op.digest().as_slice().to_vec());
        let mut probe: Vec<Digest<CgkaOperation>> =
            pool.cgka_ops.iter().map(|(_, op)| op.digest()).collect();
        probe.reverse();
        probe.push(probe[0]);
        probe.push(Digest::from([0xEE; 32]));
        let by_digest = store
            .load_cgka_ops_by_digest(*doc, &probe)
            .await
            .expect("the store answers");
        let mut index: Vec<_> = store
            .list_cgka_op_index(*doc)
            .await
            .expect("the store answers")
            .into_iter()
            .map(
                |CgkaOpIndexRow {
                     digest,
                     mut predecessors,
                 }| {
                    predecessors.sort_by_key(|digest| digest.as_slice().to_vec());
                    (digest, predecessors)
                },
            )
            .collect();
        index.sort_by_key(|(digest, _)| digest.as_slice().to_vec());
        cgka.push((all, by_digest, index));
    }

    let mut heads = Vec::new();
    let mut keys = Vec::new();
    for doc in &pool.docs {
        heads.push(
            store
                .load_content_heads(*doc)
                .await
                .expect("the store answers"),
        );
        for content in &pool.refs {
            keys.push(
                store
                    .get_cached_content_key(*doc, content)
                    .await
                    .expect("the store answers"),
            );
        }
    }
    let mut pending = store
        .load_pending_events()
        .await
        .expect("the store answers");
    pending.sort_by_key(|event| event.digest().as_slice().to_vec());

    Answers {
        key_pairs: store.load_key_pairs().await.expect("the store answers"),
        individuals,
        prekey_ops,
        documents,
        cgka,
        heads,
        keys,
        pending,
    }
}

/// A SQLite store on a file that can be opened again, and the directory that holds it.
pub(crate) struct Reopenable {
    _dir: tempfile::TempDir,
    path: PathBuf,
    pub db: Database,
    pub store: Store,
}

impl Reopenable {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keyhive.sqlite");
        let (db, store) = Self::open(&path);
        Self {
            _dir: dir,
            path,
            db,
            store,
        }
    }

    fn open(path: &std::path::Path) -> (Database, Store) {
        let db = RUNTIME.database(path, MIGRATIONS, MIGRATION_TABLE);
        let store = Store::new(db.clone(), kek());
        (db, store)
    }

    /// The directory holding the database's files.
    pub(crate) fn directory(&self) -> &std::path::Path {
        self._dir.path()
    }

    pub(crate) fn reopen(&mut self) {
        (self.db, self.store) = Self::open(&self.path);
    }
}

/// A defect to plant: the step after which to run a statement against the SQLite file, which must
/// change exactly one row.
pub(crate) type Tamper<'a> = Option<(usize, &'a str)>;

/// Run `steps` against both stores, comparing every read after each. `tamper` plants a defect in the
/// SQLite file.
pub(crate) fn agree(steps: &[Step], tamper: Tamper<'_>) -> Result<(), String> {
    block_on(async {
        let memory = MemoryKeyhiveStore::<Ref>::default();
        let mut sqlite = Reopenable::new();
        assert_eq!(
            answers(&sqlite.store).await,
            answers(&memory).await,
            "empty stores"
        );
        for (index, step) in steps.iter().enumerate() {
            apply(&memory, step).await.unwrap();
            apply(&sqlite.store, step).await.unwrap();
            if matches!(step, Step::Reopen) {
                sqlite.reopen();
            }
            if let Some((_, plant)) = tamper.filter(|(at, _)| *at == index) {
                let planted = sqlite.db.execute(plant, ()).await.unwrap();
                assert_eq!(planted.rows_affected, 1, "{plant}");
            }
            let (got, want) = (answers(&sqlite.store).await, answers(&memory).await);
            if got != want {
                return Err(format!(
                    "after step {index} ({step:?}): {got:#?} != {want:#?}"
                ));
            }
        }
        // A fresh handle on the file, as after a restart, whatever the sequence did.
        sqlite.reopen();
        let (got, want) = (answers(&sqlite.store).await, answers(&memory).await);
        if got != want {
            return Err(format!("after the final reopen: {got:#?} != {want:#?}"));
        }
        Ok(())
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn sqlite_matches_memory(steps in proptest::collection::vec(step(), 1..24)) {
        agree(&steps, None).map_err(TestCaseError::fail)?;
    }
}

/// The differential has to be able to fail. Plant what faulty writes would leave and require it to
/// be caught on the step that planted it: an operation whose predecessor edge was not copied out
/// (so the dependency index loses a row) and a prekey operation that was not kept.
#[test]
fn a_planted_defect_is_caught() {
    let steps = [
        Step::AppendCgkaOps((0..24).collect()),
        Step::AppendPrekeyOps((0..12).collect()),
        Step::Reopen,
    ];
    agree(&steps, None).expect("without a defect the stores agree");

    let lose_an_edge = "DELETE FROM kh_cgka_op_pred WHERE (doc_id, digest, predecessor) = (SELECT doc_id, digest, predecessor FROM kh_cgka_op_pred LIMIT 1)";
    let caught = agree(&steps, Some((0, lose_an_edge))).expect_err("a lost edge differs");
    assert!(caught.starts_with("after step 0"), "{caught}");

    let lose_an_op = "DELETE FROM kh_prekey_op WHERE (individual_id, digest) = (SELECT individual_id, digest FROM kh_prekey_op LIMIT 1)";
    let caught = agree(&steps, Some((1, lose_an_op))).expect_err("a lost operation differs");
    assert!(caught.starts_with("after step 1"), "{caught}");
}

/// Batches larger than one statement can bind are split, and the answers still equal the
/// reference's: key pairs past the variable limit of one `INSERT`, and probes of tens of thousands
/// of digests (absent but for a few) through the by-digest reads and `drop_pending_events`.
#[test]
fn batches_beyond_one_statement_are_chunked() {
    block_on(async {
        let memory = MemoryKeyhiveStore::<Ref>::default();
        let (_, sqlite) = super::fresh_store();
        let pairs: Vec<_> = (0_u32..6_500)
            .map(|i| {
                let mut bytes = [1; 32];
                bytes[..4].copy_from_slice(&i.to_be_bytes());
                let secret = ShareSecretKey::force_from_bytes(bytes);
                (secret.share_key(), secret)
            })
            .collect();
        KeyhiveStore::<Local, Ref>::put_key_pairs(&memory, &pairs)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::put_key_pairs(&sqlite, &pairs)
            .await
            .unwrap();
        assert_eq!(
            KeyhiveStore::<Local, Ref>::load_key_pairs(&sqlite)
                .await
                .unwrap(),
            KeyhiveStore::<Local, Ref>::load_key_pairs(&memory)
                .await
                .unwrap(),
        );

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
        KeyhiveStore::<Local, Ref>::append_prekey_ops(&memory, who, &ops)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::append_prekey_ops(&sqlite, who, &ops)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::append_cgka_ops(&memory, doc, &cgka)
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::append_cgka_ops(&sqlite, doc, &cgka)
            .await
            .unwrap();
        let absent = |i: u32| {
            let mut bytes = [0xEE; 32];
            bytes[..4].copy_from_slice(&i.to_be_bytes());
            bytes
        };
        let mut ops_probe: Vec<Digest<KeyOp>> =
            (0..65_000).map(|i| Digest::from(absent(i))).collect();
        ops_probe.insert(30_000, ops[1].digest());
        ops_probe.push(ops[0].digest());
        let mut cgka_probe: Vec<Digest<CgkaOperation>> =
            (0..65_000).map(|i| Digest::from(absent(i))).collect();
        cgka_probe.insert(30_000, cgka[1].digest());
        cgka_probe.push(cgka[0].digest());
        assert_eq!(
            KeyhiveStore::<Local, Ref>::load_prekey_ops_by_digest(&sqlite, who, &ops_probe)
                .await
                .unwrap(),
            KeyhiveStore::<Local, Ref>::load_prekey_ops_by_digest(&memory, who, &ops_probe)
                .await
                .unwrap(),
        );
        assert_eq!(
            KeyhiveStore::<Local, Ref>::load_cgka_ops_by_digest(&sqlite, doc, &cgka_probe)
                .await
                .unwrap(),
            KeyhiveStore::<Local, Ref>::load_cgka_ops_by_digest(&memory, doc, &cgka_probe)
                .await
                .unwrap(),
        );

        let events = &pool.events;
        KeyhiveStore::<Local, Ref>::put_pending_events(&sqlite, events)
            .await
            .unwrap();
        let mut drop: Vec<Digest<StaticEvent<Ref>>> =
            (0..65_000).map(|i| Digest::from(absent(i))).collect();
        drop.push(events[0].digest());
        KeyhiveStore::<Local, Ref>::drop_pending_events(&sqlite, &drop)
            .await
            .unwrap();
        let left = KeyhiveStore::<Local, Ref>::load_pending_events(&sqlite)
            .await
            .unwrap();
        assert_eq!(left.len(), events.len() - 1);
    });
}

/// The one place the stores differ, pinned: appending no operations. The in-memory store makes an
/// entry for the individual or document and lists it; this one has no row to make, and lists a
/// document or an individual only once it holds an operation, as the trait documents it
/// ("every document with CGKA operations here").
#[test]
fn appending_nothing_lists_nothing() {
    block_on(async {
        let (_, sqlite) = super::fresh_store();
        KeyhiveStore::<Local, Ref>::append_cgka_ops(&sqlite, POOL.docs[0], &[])
            .await
            .unwrap();
        KeyhiveStore::<Local, Ref>::append_prekey_ops(&sqlite, POOL.individuals[0], &[])
            .await
            .unwrap();
        assert!(
            KeyhiveStore::<Local, Ref>::list_documents(&sqlite)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            KeyhiveStore::<Local, Ref>::list_individuals(&sqlite)
                .await
                .unwrap()
                .is_empty()
        );
        let memory = MemoryKeyhiveStore::<Ref>::default();
        KeyhiveStore::<Local, Ref>::append_cgka_ops(&memory, POOL.docs[0], &[])
            .await
            .unwrap();
        assert_eq!(
            KeyhiveStore::<Local, Ref>::list_documents(&memory)
                .await
                .unwrap(),
            vec![POOL.docs[0]],
            "the reference lists a document it holds no operation of"
        );
    });
}
