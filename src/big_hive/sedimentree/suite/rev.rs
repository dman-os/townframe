//! The trees as a `RevSource`: big_automata's stream laws, run against the store with okv
//! batches mapped to tree mutations, and the window reads the laws do not pin.
//!
//! A put is a save of a commit no tree has seen (so it always changes the tree, as an okv put
//! always writes a row), a delete is a register and a retire (so deleting a key that was never
//! written still leaves the tombstone an okv delete does). One okv batch is one `Batch`, one
//! rev. The key's bytes, zero padded, are the tree id, which keeps the id order the key order.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use big_automata::okv::Mutation as Okv;
use big_automata::okv::Selector;
use big_automata::okv::laws::Bytes;
use big_automata::okv::oracle::Oracle;
use big_automata::rev::Entry;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use big_automata::rev::RevSource;
use big_automata::rev::laws::StreamBackend;
use big_automata::rev::laws::limits;
use big_automata::rev::laws::stream_scripts;
use big_automata::rev::laws::{self as stream_laws};
use futures::executor::block_on;
use proptest::prelude::*;
use sedimentree_core::id::SedimentreeId;
use sedimentree_core::loose_commit::id::CommitId;
use sqlite_rs::Database;
use sqlite_rs::SqliteDb;
use utils_rs::prelude::Res;

use super::Store;
use super::fresh_db;
use super::seal_commit;
use super::store_on;
use crate::sedimentree::Mutation;

fn tree_of(key: &[u8]) -> SedimentreeId {
    let mut id = [0; 32];
    id[..key.len()].copy_from_slice(key);
    SedimentreeId::new(id)
}

#[derive(Clone)]
struct Trees;

/// The database, the store over it, and the counter that makes every put's commit new.
#[derive(Clone)]
struct Writer {
    db: Database,
    store: Store,
    puts: Arc<AtomicU64>,
}

impl Writer {
    /// Stage `batches` in a transaction, one `Batch` each.
    async fn stage(
        &self,
        batches: Vec<Vec<Okv<Bytes, Bytes>>>,
    ) -> Res<(
        <Database as SqliteDb>::Write,
        Vec<crate::sedimentree::Batch>,
    )> {
        let tx = self.db.begin_write().await?;
        let mut staged = Vec::new();
        for mutations in batches {
            let mut batch = self.store.batch();
            for mutation in mutations {
                let (key, put) = match mutation {
                    Okv::Put { key, .. } => (key, true),
                    Okv::Delete { key } => (key, false),
                };
                let id = tree_of(&key);
                let mutations = if put {
                    let head = self.puts.fetch_add(1, Ordering::SeqCst);
                    let mut bytes = [0xFF; 32];
                    bytes[..8].copy_from_slice(&head.to_be_bytes());
                    let (commits, fragments) = (
                        vec![
                            seal_commit(id, CommitId::new(bytes), Default::default(), 0, None)
                                .await,
                        ],
                        vec![],
                    );
                    vec![Mutation::Save { commits, fragments }]
                } else {
                    vec![Mutation::Register, Mutation::Retire]
                };
                for mutation in mutations {
                    self.store.apply_in(&tx, &mut batch, id, mutation).await?;
                }
            }
            staged.push(batch);
        }
        Ok((tx, staged))
    }
}

#[async_trait]
impl StreamBackend for Trees {
    type Source = Store;
    type Writer = Writer;

    async fn open(&self) -> Res<(Store, Writer)> {
        let db = fresh_db();
        let store = store_on(&db);
        Ok((
            store.clone(),
            Writer {
                db,
                store,
                puts: Arc::default(),
            },
        ))
    }

    async fn commit(&self, writer: &Writer, batches: Vec<Vec<Okv<Bytes, Bytes>>>) -> Res<()> {
        let (tx, staged) = writer.stage(batches).await?;
        Database::commit(tx).await?;
        for batch in staged {
            writer.store.committed(batch);
        }
        Ok(())
    }

    async fn rollback(&self, writer: &Writer, batches: Vec<Vec<Okv<Bytes, Bytes>>>) -> Res<()> {
        let (tx, _staged) = writer.stage(batches).await?;
        Database::rollback(tx).await?;
        Ok(())
    }

    /// Every tree: the source has nothing to select by.
    fn selector(&self, _: &Selector<Bytes>) {}

    /// The oracle's whole window, with each key as its tree and each value as the tree's
    /// existence, cut after the last whole revision that fits the limits (an entry costs its
    /// 32 byte key; the first revision always fits).
    fn expected(
        &self,
        oracle: &Oracle<Bytes, Bytes>,
        _: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        page_limits: Limits,
    ) -> Page<SedimentreeId, ()> {
        let window = oracle.read(&Selector::All, after, until, limits(usize::MAX, None));
        let entries: Vec<_> = window
            .entries
            .into_iter()
            .map(|entry| Entry {
                key: tree_of(&entry.key),
                rev: entry.rev,
                value: entry.value.map(|_| ()),
            })
            .collect();
        let max_entries = page_limits
            .max_bytes
            .map_or(page_limits.max_entries.get(), |bytes| {
                page_limits.max_entries.get().min((bytes.get() / 32).max(1))
            });
        let mut taken = 0;
        let mut through = until;
        for revision in entries.chunk_by(|left, right| left.rev == right.rev) {
            if taken > 0 && taken + revision.len() > max_entries {
                through = entries[taken - 1].rev;
                break;
            }
            taken += revision.len();
        }
        Page {
            entries: entries.into_iter().take(taken).collect(),
            through,
        }
    }
}

#[test]
fn tree_stream_commit_during_replay() {
    block_on(stream_laws::stream_commit_during_replay(&Trees)).unwrap();
}

#[test]
fn tree_stream_overwrite_of_unread_key() {
    block_on(stream_laws::stream_overwrite_of_unread_key(&Trees)).unwrap();
}

#[test]
fn tree_stream_commit_during_read() {
    block_on(stream_laws::stream_commit_during_read(&Trees)).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn tree_stream_follows(script in stream_scripts()) {
        block_on(stream_laws::stream_follows(&Trees, script))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }
}

/// A read bounded below a revision that holds several trees returns the whole revision, and a
/// source another process wrote is read without any watch having moved.
#[test]
fn a_revision_over_the_bound_is_one_page_and_a_foreign_commit_is_read_by_latest_rev() {
    block_on(async {
        let (reader, writer) = Trees.open().await.unwrap();
        let batch = vec![
            (1..=4_u8)
                .map(|key| Okv::Put {
                    key: vec![key],
                    value: vec![],
                })
                .collect::<Vec<_>>(),
        ];
        // The writer is another store over the file, and never reports to the reader's watch.
        let foreign = Writer {
            store: store_on(&writer.db),
            ..writer.clone()
        };
        let (tx, _unreported) = foreign.stage(batch).await.unwrap();
        Database::commit(tx).await.unwrap();

        assert_eq!(reader.watch().latest(), 0);
        let latest = reader.latest_rev().await.unwrap();
        assert_eq!(latest, 1);
        let mut page = Page::default();
        reader
            .read(&(), 0, latest, limits(2, None), &mut page)
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 4);
        assert!(page.entries.iter().all(|entry| entry.rev == 1));
        assert_eq!(page.through, 1);
    });
}
