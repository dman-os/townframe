//! okv writes, reads, point reads and pruning at 10k and 100k rows.

use futures::executor::block_on;
use utils_rs::testing::alloc_count::measure;

use super::assert_linear;
use crate::okv::Mutation;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::okv::laws::Bytes;
use crate::okv::memory::MemoryDb;
use crate::okv::memory::MemoryOkv;
use crate::rev::Page;
use crate::rev::RevSource;
use crate::rev::laws::limits;

const SMALL: usize = 10_000;

type Table = MemoryOkv<Bytes, Bytes>;

fn key(row: usize) -> Bytes {
    u32::try_from(row)
        .expect("rows fit u32")
        .to_be_bytes()
        .to_vec()
}

fn put(row: usize) -> Mutation<Bytes, Bytes> {
    Mutation::Put {
        key: key(row),
        value: vec![7; 64],
    }
}

/// `rows` one-row batches in one transaction, committed: `rows` revs.
fn filled(rows: usize) -> (MemoryDb, Table) {
    let db = MemoryDb::default();
    let table: Table = db.table();
    let mut tx = db.begin();
    for row in 0..rows {
        block_on(table.apply(&mut tx, vec![put(row)])).expect("apply");
    }
    tx.commit().expect("commit");
    (db, table)
}

/// Staging `n` one-row batches and committing them.
#[test]
fn okv_writes_in_one_transaction_are_linear() {
    assert_linear("okv write n batches, one tx", SMALL, |n| {
        let db = MemoryDb::default();
        let table: Table = db.table();
        let mut tx = db.begin();
        measure(|| {
            for row in 0..n {
                block_on(table.apply(&mut tx, vec![put(row)])).expect("apply");
            }
            tx.commit().expect("commit");
        })
        .1
    });
}

/// `n` transactions of one row each.
#[test]
fn okv_one_commit_per_row_is_linear() {
    assert_linear("okv write n transactions", SMALL, |n| {
        let db = MemoryDb::default();
        let table: Table = db.table();
        measure(|| {
            for row in 0..n {
                let mut tx = db.begin();
                block_on(table.apply(&mut tx, vec![put(row)])).expect("apply");
                tx.commit().expect("commit");
            }
        })
        .1
    });
}

/// Paging the whole table in rev order, 256 rows a page.
#[test]
fn okv_log_paging_is_linear() {
    assert_linear("okv page the whole log", SMALL, |n| {
        let (_db, table) = filled(n);
        let latest = block_on(table.latest_rev()).expect("latest");
        let mut page = Page::default();
        measure(|| {
            let mut after = 0;
            while after < latest {
                block_on(table.read(&Selector::All, after, latest, limits(256, None), &mut page))
                    .expect("read");
                after = page.through;
            }
        })
        .1
    });
}

/// `n` point reads in one transaction.
#[test]
fn okv_point_reads_are_linear() {
    assert_linear("okv get n keys", SMALL, |n| {
        let (db, table) = filled(n);
        let mut tx = db.begin();
        measure(|| {
            for row in 0..n {
                assert!(
                    block_on(table.get(&mut tx, &key(row)))
                        .expect("get")
                        .is_some()
                );
            }
        })
        .1
    });
}
