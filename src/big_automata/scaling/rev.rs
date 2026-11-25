//! The rev stream's replay, its live follow, and a projection over it, at 10k and 100k revs.

use futures::executor::block_on;
use utils_rs::alloc_count::measure;

use super::assert_linear;
use crate::okv::Mutation;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::okv::laws::Bytes;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;
use crate::rev::Page;
use crate::rev::RevSourceExt;
use crate::rev::RevStream;
use crate::rev::laws::limits;

const SMALL: usize = 10_000;

type Table = MemoryOkv<Bytes, Bytes>;

fn put(row: usize) -> Mutation<Bytes, Bytes> {
    Mutation::Put {
        key: u32::try_from(row)
            .expect("rows fit u32")
            .to_be_bytes()
            .to_vec(),
        value: vec![7; 64],
    }
}

fn commit_row(db: &MemoryDb, table: &Table, row: usize) {
    let mut tx = db.begin();
    block_on(table.apply(&mut tx, vec![put(row)])).expect("apply");
    tx.commit().expect("commit");
}

fn filled(rows: usize) -> (MemoryDb, Table) {
    let db = MemoryDb::default();
    let table: Table = db.table();
    for row in 0..rows {
        commit_row(&db, &table, row);
    }
    (db, table)
}

/// A stream over `n` committed revs, replayed to the head in 256-row pages.
#[test]
fn rev_stream_replay_is_linear() {
    assert_linear("rev stream replay", SMALL, |n| {
        let (_db, table) = filled(n);
        let mut stream = RevStream::new(table, Selector::All, 0);
        let mut page = Page::default();
        measure(|| {
            while !stream.caught_up() {
                block_on(stream.next_into(&mut page, limits(256, None))).expect("page");
            }
        })
        .1
    });
}

/// The same replay through `map` and `filter_keys`, which copy each page once more.
#[test]
fn rev_stream_projection_replay_is_linear() {
    assert_linear(
        "rev stream replay through map and filter_keys",
        SMALL,
        |n| {
            let (_db, table) = filled(n);
            let projected = table
                .map(|_key: &Bytes, value: Bytes| value.len())
                .filter_keys(|key: &Bytes| key[3].is_multiple_of(2));
            let mut stream = RevStream::new(projected, Selector::All, 0);
            let mut page = Page::default();
            measure(|| {
                while !stream.caught_up() {
                    block_on(stream.next_into(&mut page, limits(256, None))).expect("page");
                }
            })
            .1
        },
    );
}

/// A caught-up stream following `n` commits, one page per commit.
#[test]
fn rev_stream_follow_is_linear() {
    assert_linear("rev stream follow n commits", SMALL, |n| {
        let (db, table) = filled(1);
        let mut stream = RevStream::new(table.clone(), Selector::All, 0);
        let mut page = Page::default();
        while !stream.caught_up() {
            block_on(stream.next_into(&mut page, limits(256, None))).expect("page");
        }
        measure(|| {
            for row in 1..=n {
                commit_row(&db, &table, row);
                block_on(stream.next_into(&mut page, limits(256, None))).expect("page");
                assert_eq!(page.entries.len(), 1);
            }
        })
        .1
    });
}
