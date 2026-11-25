//! The okv laws and the stream laws run against the memory backend, plus what is particular to
//! it: optimistic commits and their conflicts.

use std::pin::pin;

use futures::executor::block_on;
use proptest::prelude::*;

use super::laws::Backend;
use super::laws::Bytes;
use super::laws::OkvStreams;
use super::laws::put;
use super::laws::{self};
use super::memory::Conflict;
use super::memory::MemoryDb;
use super::memory::MemoryOkv;
use super::memory::MemoryTx;
use super::*;
use crate::rev::Entry;
use crate::rev::Page;
use crate::rev::laws::WakeCount;
use crate::rev::laws::limits;
use crate::rev::laws::{self as stream_laws};

#[derive(Clone)]
struct Memory;

#[async_trait]
impl Backend for Memory {
    type Db = MemoryDb;
    type Store = MemoryOkv<Bytes, Bytes>;

    async fn open(&self) -> Res<MemoryDb> {
        Ok(default())
    }

    async fn table(&self, db: &MemoryDb) -> Res<Self::Store> {
        Ok(db.table())
    }

    async fn begin(&self, db: &MemoryDb) -> Res<MemoryTx> {
        Ok(db.begin())
    }

    async fn commit(&self, tx: MemoryTx) -> Res<()> {
        Ok(tx.commit()?)
    }

    async fn rollback(&self, tx: MemoryTx) -> Res<()> {
        drop(tx);
        Ok(())
    }
}

#[test]
fn collapse() -> Res<()> {
    block_on(laws::collapse(&Memory))
}

#[test]
fn tombstones() -> Res<()> {
    block_on(laws::tombstones(&Memory))
}

#[test]
fn revs_strictly_increase() -> Res<()> {
    block_on(laws::revs_strictly_increase(&Memory))
}

#[test]
#[should_panic(expected = "an empty mutation batch has no rows to stamp")]
fn empty_batch_panics() {
    block_on(laws::empty_batch_panics(&Memory)).expect("the law panics before returning");
}

#[test]
fn rollback_leaves_no_trace() -> Res<()> {
    block_on(laws::rollback_leaves_no_trace(&Memory))
}

#[test]
fn wakeups_follow_commit() -> Res<()> {
    block_on(laws::wakeups_follow_commit(&Memory))
}

#[test]
fn soft_limits() -> Res<()> {
    block_on(laws::soft_limits(&Memory))
}

#[test]
fn get_reads_your_writes() -> Res<()> {
    block_on(laws::get_reads_your_writes(&Memory))
}

#[test]
fn tables_commit_together() -> Res<()> {
    block_on(laws::tables_commit_together(&Memory))
}

#[test]
fn stream_commit_during_replay() -> Res<()> {
    block_on(stream_laws::stream_commit_during_replay(&OkvStreams(
        Memory,
    )))
}

#[test]
fn stream_overwrite_of_unread_key() -> Res<()> {
    block_on(stream_laws::stream_overwrite_of_unread_key(&OkvStreams(
        Memory,
    )))
}

#[test]
fn stream_commit_during_read() -> Res<()> {
    block_on(stream_laws::stream_commit_during_read(&OkvStreams(Memory)))
}

proptest! {
    #[test]
    fn differential(ops in laws::ops()) {
        block_on(laws::differential(&Memory, ops))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }

    #[test]
    fn stream_follows(script in stream_laws::stream_scripts()) {
        block_on(stream_laws::stream_follows(&OkvStreams(Memory), script))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }
}

type Table = MemoryOkv<Bytes, Bytes>;

fn read_all(table: &Table, until: u64) -> Res<Page<Bytes, Bytes>> {
    let mut page = Page::default();
    block_on(table.read(&Selector::All, 0, until, limits(10, None), &mut page))?;
    Ok(page)
}

/// A commit that lost the race to another publishes nothing on any table it touched, consumes
/// no rev and wakes nobody.
#[test]
fn a_conflicting_commit_leaves_no_trace() -> Res<()> {
    let db = MemoryDb::default();
    let table: Table = db.table();
    let bystander: Table = db.table();
    let mut loser = db.begin();
    assert_eq!(block_on(table.apply(&mut loser, vec![put(1, b"lost")]))?, 1);
    assert_eq!(
        block_on(bystander.apply(&mut loser, vec![put(1, b"lost")]))?,
        1
    );
    let mut winner = db.begin();
    assert_eq!(block_on(table.apply(&mut winner, vec![put(2, b"won")]))?, 1);
    winner.commit()?;

    let wakes = WakeCount::default();
    let (mut table_watch, mut bystander_watch) = (table.watch(), bystander.watch());
    let mut table_wait = pin!(table_watch.wait_past(1));
    let mut bystander_wait = pin!(bystander_watch.wait_past(0));
    assert!(wakes.poll(table_wait.as_mut()).is_pending());
    assert!(wakes.poll(bystander_wait.as_mut()).is_pending());

    assert_eq!(
        loser.commit(),
        Err(Conflict {
            base: 0,
            current: 1
        })
    );
    assert_eq!(wakes.count(), 0, "a failed commit woke a waiter");
    assert_eq!(
        (
            block_on(table.latest_rev())?,
            block_on(bystander.latest_rev())?
        ),
        (1, 0)
    );
    assert_eq!(
        read_all(&table, 1)?,
        Page {
            entries: vec![Entry {
                key: vec![2],
                rev: 1,
                value: Some(b"won".to_vec()),
            }],
            through: 1,
        }
    );

    let mut next = db.begin();
    assert_eq!(block_on(table.apply(&mut next, vec![put(3, b"c")]))?, 2);
    assert_eq!(block_on(bystander.apply(&mut next, vec![put(3, b"c")]))?, 1);
    next.commit()?;
    Ok(())
}

/// A transaction that read a key conflicts with a commit that moved the key's table after the
/// read, so a read-modify-write cannot lose an update. A read-only transaction that saw nothing
/// move commits and wakes nobody.
#[test]
fn a_read_conflicts_with_a_later_commit() -> Res<()> {
    let db = MemoryDb::default();
    let table: Table = db.table();
    let mut seed = db.begin();
    block_on(table.apply(&mut seed, vec![put(1, b"a")]))?;
    seed.commit()?;

    let mut reader = db.begin();
    assert_eq!(
        block_on(table.get(&mut reader, &vec![1]))?,
        Some(b"a".to_vec())
    );
    let mut writer = db.begin();
    block_on(table.apply(&mut writer, vec![put(1, b"b")]))?;
    writer.commit()?;
    block_on(table.apply(&mut reader, vec![put(1, b"a+")]))?;
    assert_eq!(
        reader.commit(),
        Err(Conflict {
            base: 1,
            current: 2
        })
    );

    let wakes = WakeCount::default();
    let mut watch = table.watch();
    let mut wait = pin!(watch.wait_past(2));
    assert!(wakes.poll(wait.as_mut()).is_pending());
    let mut quiet = db.begin();
    assert_eq!(
        block_on(table.get(&mut quiet, &vec![1]))?,
        Some(b"b".to_vec())
    );
    quiet.commit()?;
    assert_eq!(wakes.count(), 0, "a read-only commit woke a waiter");
    assert_eq!(block_on(table.latest_rev())?, 2);
    Ok(())
}

#[test]
#[should_panic(expected = "a transaction was used on a table of another db")]
fn apply_to_another_db_panics() {
    let table: Table = MemoryDb::default().table();
    let mut tx = MemoryDb::default().begin();
    block_on(table.apply(&mut tx, vec![put(1, b"a")])).expect("the apply panics first");
}

#[test]
#[should_panic(expected = "read until 2 past the committed rev 1")]
fn read_past_the_committed_rev_panics() {
    let db = MemoryDb::default();
    let table: Table = db.table();
    let mut tx = db.begin();
    block_on(table.apply(&mut tx, vec![put(1, b"a")])).expect("a batch applies");
    tx.commit().expect("nothing races the commit");
    read_all(&table, 2).expect("the read panics first");
}

/// A read refills the caller's page in place: a smaller result keeps the page's buffer.
#[test]
fn read_reuses_the_callers_page() -> Res<()> {
    let db = MemoryDb::default();
    let table: Table = db.table();
    let mut tx = db.begin();
    block_on(table.apply(&mut tx, vec![put(1, b"a"), put(2, b"b"), put(3, b"c")]))?;
    block_on(table.apply(&mut tx, vec![put(4, b"d")]))?;
    tx.commit()?;

    let mut page = Page::default();
    block_on(table.read(&Selector::All, 0, 2, limits(10, None), &mut page))?;
    assert_eq!(page.entries.len(), 4);
    let buffer = page.entries.as_ptr();
    block_on(table.read(&Selector::All, 1, 2, limits(10, None), &mut page))?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(
        page.entries.as_ptr(),
        buffer,
        "the read reallocated the page"
    );
    Ok(())
}
