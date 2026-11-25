use std::pin::pin;
use std::task::Poll;

use future_form::Sendable;
use futures::executor::block_on;

use super::*;
use crate::okv::Mutation;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::okv::laws::Bytes;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;
use crate::okv::oracle::Oracle;
use crate::rev::laws::StreamBackend;
use crate::rev::laws::WakeCount;
use crate::rev::laws::limits;
use crate::rev::laws::{self};

type Store = MemoryOkv<Bytes, Bytes>;

fn put(key: u8, value: &[u8]) -> Mutation<Bytes, Bytes> {
    Mutation::Put {
        key: vec![key],
        value: value.to_vec(),
    }
}

fn entry(key: u8, rev: Rev, value: Option<&[u8]>) -> Entry<Bytes, Bytes> {
    Entry {
        key: vec![key],
        rev,
        value: value.map(<[u8]>::to_vec),
    }
}

/// Commit each batch in its own transaction.
fn commit(db: &MemoryDb, store: &Store, batches: Vec<Vec<Mutation<Bytes, Bytes>>>) {
    for batch in batches {
        let mut tx = db.begin();
        block_on(OkvStore::<Sendable>::apply(store, &mut tx, batch)).expect("a batch applies");
        tx.commit().expect("nothing races the commit");
    }
}

#[test]
fn revisions_are_the_pages_contiguous_rev_runs() {
    let page = Page {
        entries: vec![
            entry(1, 1, None),
            entry(2, 1, None),
            entry(1, 3, None),
            entry(4, 7, None),
            entry(5, 7, None),
        ],
        through: 7,
    };
    let revisions = page.revisions().collect::<Vec<_>>();
    assert_eq!(
        revisions,
        vec![
            &page.entries[0..2],
            &page.entries[2..3],
            &page.entries[3..5]
        ]
    );
}

/// The snapshot is taken by the first read, not by `new`: a commit in between is replay.
#[test]
fn the_snapshot_is_captured_on_the_first_read() -> Res<()> {
    let db = MemoryDb::default();
    let store: Store = db.table();
    commit(&db, &store, vec![vec![put(1, b"a")]]);
    let mut stream = RevStream::<Sendable, _>::new(store.clone(), Selector::All, 0);
    commit(&db, &store, vec![vec![put(2, b"b")]]);
    assert_eq!(stream.snapshot_rev(), None);
    assert!(!stream.caught_up());

    let mut page = Page::default();
    block_on(stream.next_into(&mut page, limits(1, None)))?;
    assert_eq!(stream.snapshot_rev(), Some(2));
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 1, Some(b"a"))],
            through: 1,
        }
    );
    assert!(!stream.caught_up());
    block_on(stream.next_into(&mut page, limits(1, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(2, 2, Some(b"b"))],
            through: 2,
        }
    );
    assert!(stream.caught_up());
    Ok(())
}

/// Revisions the selector skips still move the stream, as empty pages, during replay and live.
#[test]
fn skipped_revisions_come_back_as_progress_only_pages() -> Res<()> {
    let db = MemoryDb::default();
    let store: Store = db.table();
    commit(&db, &store, vec![vec![put(1, b"a")], vec![put(2, b"b")]]);
    let mut stream =
        RevStream::<Sendable, _>::new(store.clone(), Selector::Keys([vec![9]].into()), 0);
    let mut page = Page::default();

    block_on(stream.next_into(&mut page, limits(1, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![],
            through: 2,
        }
    );
    assert!(stream.caught_up());

    commit(&db, &store, vec![vec![put(3, b"c")]]);
    block_on(stream.next_into(&mut page, limits(1, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![],
            through: 3,
        }
    );
    Ok(())
}

/// A caught-up stream waits on the watch, is woken by the next commit, and reads it.
#[test]
fn a_caught_up_stream_waits_for_the_next_commit() -> Res<()> {
    let db = MemoryDb::default();
    let store: Store = db.table();
    commit(&db, &store, vec![vec![put(1, b"a")]]);
    let mut stream = RevStream::<Sendable, _>::new(store.clone(), Selector::All, 0);
    let mut page = Page::default();
    block_on(stream.next_into(&mut page, limits(10, None)))?;
    assert!(stream.caught_up());

    let wakes = WakeCount::default();
    let read = {
        let mut next = pin!(stream.next_into(&mut page, limits(10, None)));
        assert!(wakes.poll(next.as_mut()).is_pending());
        assert_eq!(wakes.count(), 0);
        commit(&db, &store, vec![vec![put(2, b"b")]]);
        assert!(wakes.count() > 0, "the commit did not wake the stream");
        let Poll::Ready(read) = wakes.poll(next.as_mut()) else {
            panic!("the woken stream did not read the commit");
        };
        read
    };
    read?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(2, 2, Some(b"b"))],
            through: 2,
        }
    );
    Ok(())
}

#[test]
fn map_projects_values_and_keeps_tombstones() -> Res<()> {
    let db = MemoryDb::default();
    let store: Store = db.table();
    commit(
        &db,
        &store,
        vec![
            vec![put(1, b"ab"), put(2, b"c")],
            vec![Mutation::Delete { key: vec![2] }],
        ],
    );
    let lengths = RevSourceExt::<Sendable>::map(store, |_key: &Bytes, value: Bytes| value.len());
    let mut page = Page::default();
    block_on(RevSource::<Sendable>::read(
        &lengths,
        &Selector::All,
        0,
        2,
        limits(10, None),
        &mut page,
    ))?;
    assert_eq!(
        page,
        Page {
            entries: vec![
                Entry {
                    key: vec![1],
                    rev: 1,
                    value: Some(2),
                },
                Entry {
                    key: vec![2],
                    rev: 2,
                    value: None,
                },
            ],
            through: 2,
        }
    );
    Ok(())
}

/// A value the closure refuses becomes a tombstone at its rev, so a consumer folding pages
/// forgets a value it kept earlier: latest-state-per-key holds through the projection.
#[test]
fn filter_map_turns_a_refused_value_into_a_tombstone() -> Res<()> {
    let db = MemoryDb::default();
    let store: Store = db.table();
    commit(&db, &store, vec![vec![put(1, b"keep")]]);
    let kept = RevSourceExt::<Sendable>::filter_map(store.clone(), |_key: &Bytes, value: Bytes| {
        (value != b"drop").then_some(value)
    });
    let mut stream = RevStream::<Sendable, _>::new(kept, Selector::All, 0);
    let mut page = Page::default();
    block_on(stream.next_into(&mut page, limits(10, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 1, Some(b"keep"))],
            through: 1,
        }
    );

    commit(
        &db,
        &store,
        vec![
            vec![put(1, b"drop"), put(2, b"drop")],
            vec![put(3, b"keep")],
        ],
    );
    block_on(stream.next_into(&mut page, limits(10, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![
                entry(1, 2, None),
                entry(2, 2, None),
                entry(3, 3, Some(b"keep"))
            ],
            through: 3,
        }
    );
    Ok(())
}

/// Entries of refused keys leave the page, tombstones included, and the page keeps its
/// progress, so a stream still advances through revisions the filter empties.
#[test]
fn filter_keys_drops_entries_and_keeps_progress() -> Res<()> {
    let db = MemoryDb::default();
    let store: Store = db.table();
    commit(
        &db,
        &store,
        vec![
            vec![put(2, b"b")],
            vec![put(1, b"a")],
            vec![Mutation::Delete { key: vec![1] }],
            vec![Mutation::Delete { key: vec![2] }],
        ],
    );
    let odd = RevSourceExt::<Sendable>::filter_keys(store.clone(), |key: &Bytes| key[0] % 2 == 1);
    let mut stream = RevStream::<Sendable, _>::new(odd, Selector::All, 0);
    let mut page = Page::default();

    // Key 2's delete moved its row to rev 4, so nothing is left at revs 1 and 2.
    block_on(stream.next_into(&mut page, limits(1, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![entry(1, 3, None)],
            through: 3,
        }
    );
    // Key 2's tombstone is dropped with its key; the page still reports its progress.
    block_on(stream.next_into(&mut page, limits(1, None)))?;
    assert_eq!(
        page,
        Page {
            entries: vec![],
            through: 4,
        }
    );
    assert!(stream.caught_up());
    Ok(())
}

type Projected = Arc<
    dyn RevSource<Sendable, Key = Bytes, Value = Bytes, Selector = Selector<Bytes>> + Send + Sync,
>;

/// The stream laws over projections of a memory table: the oracle's read, projected the same
/// way, is what each projection must return.
#[derive(Debug, Clone, Copy)]
enum Projection {
    /// `filter_map` keeping empty values and values starting with an even byte, marked with a
    /// trailing `!`; a refused value reads as a tombstone.
    FilterMap,
    /// `filter_keys` keeping the keys whose first byte is even.
    FilterKeys,
}

fn keep_value(value: Bytes) -> Option<Bytes> {
    value.first().is_none_or(|byte| byte % 2 == 0).then(|| {
        let mut value = value;
        value.push(b'!');
        value
    })
}

fn keep_key(key: &Bytes) -> bool {
    key.first().is_some_and(|byte| byte % 2 == 0)
}

impl Projection {
    fn project(
        self,
        Entry { key, rev, value }: Entry<Bytes, Bytes>,
    ) -> Option<Entry<Bytes, Bytes>> {
        match self {
            Self::FilterMap => Some(Entry {
                key,
                rev,
                value: value.and_then(keep_value),
            }),
            Self::FilterKeys => keep_key(&key).then_some(Entry { key, rev, value }),
        }
    }
}

#[async_trait]
impl StreamBackend for Projection {
    type Source = Projected;
    type Writer = (MemoryDb, Store);

    async fn open(&self) -> Res<(Projected, (MemoryDb, Store))> {
        let db = MemoryDb::default();
        let store: Store = db.table();
        let projected: Projected = match self {
            Self::FilterMap => Arc::new(RevSourceExt::<Sendable>::filter_map(
                store.clone(),
                |_key: &Bytes, value: Bytes| keep_value(value),
            )),
            Self::FilterKeys => Arc::new(RevSourceExt::<Sendable>::filter_keys(
                store.clone(),
                keep_key,
            )),
        };
        Ok((projected, (db, store)))
    }

    async fn commit(
        &self,
        (db, store): &(MemoryDb, Store),
        batches: Vec<Vec<Mutation<Bytes, Bytes>>>,
    ) -> Res<()> {
        let mut tx = db.begin();
        for batch in batches {
            OkvStore::<Sendable>::apply(store, &mut tx, batch).await?;
        }
        Ok(tx.commit()?)
    }

    async fn rollback(
        &self,
        (db, store): &(MemoryDb, Store),
        batches: Vec<Vec<Mutation<Bytes, Bytes>>>,
    ) -> Res<()> {
        let mut tx = db.begin();
        for batch in batches {
            OkvStore::<Sendable>::apply(store, &mut tx, batch).await?;
        }
        drop(tx);
        Ok(())
    }

    fn selector(&self, selector: &Selector<Bytes>) -> Selector<Bytes> {
        selector.clone()
    }

    fn expected(
        &self,
        oracle: &Oracle<Bytes, Bytes>,
        selector: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Page<Bytes, Bytes> {
        let Page { entries, through } = oracle.read(selector, after, until, limits);
        let entries = entries
            .into_iter()
            .filter_map(|entry| self.project(entry))
            .collect();
        Page { entries, through }
    }
}

#[test]
fn filter_map_stream_commit_during_replay() -> Res<()> {
    block_on(laws::stream_commit_during_replay(&Projection::FilterMap))
}

#[test]
fn filter_map_stream_overwrite_of_unread_key() -> Res<()> {
    block_on(laws::stream_overwrite_of_unread_key(&Projection::FilterMap))
}

#[test]
fn filter_map_stream_commit_during_read() -> Res<()> {
    block_on(laws::stream_commit_during_read(&Projection::FilterMap))
}

/// Kept `[2]`, refused `[1]`: the key is live exactly when its last write is kept.
#[test]
fn filter_map_stream_follows_current_state() -> Res<()> {
    block_on(laws::stream_follows_current_state(
        &Projection::FilterMap,
        &[2],
        &[1],
    ))
}

#[test]
fn filter_keys_stream_commit_during_replay() -> Res<()> {
    block_on(laws::stream_commit_during_replay(&Projection::FilterKeys))
}

#[test]
fn filter_keys_stream_overwrite_of_unread_key() -> Res<()> {
    block_on(laws::stream_overwrite_of_unread_key(
        &Projection::FilterKeys,
    ))
}

#[test]
fn filter_keys_stream_commit_during_read() -> Res<()> {
    block_on(laws::stream_commit_during_read(&Projection::FilterKeys))
}

proptest::proptest! {
    #[test]
    fn filter_map_stream_follows(script in laws::stream_scripts()) {
        block_on(laws::stream_follows(&Projection::FilterMap, script))
            .map_err(|err| proptest::test_runner::TestCaseError::fail(format!("{err:?}")))?;
    }

    #[test]
    fn filter_keys_stream_follows(script in laws::stream_scripts()) {
        block_on(laws::stream_follows(&Projection::FilterKeys, script))
            .map_err(|err| proptest::test_runner::TestCaseError::fail(format!("{err:?}")))?;
    }
}
