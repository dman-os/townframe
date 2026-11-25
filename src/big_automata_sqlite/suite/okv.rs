//! The okv laws, the stream laws and the differential against the oracle, over SQLite; then
//! what SQLite adds to okv: tables that share a transaction with the part store.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use big_automata::okv::Mutation;
use big_automata::okv::OkvStore;
use big_automata::okv::laws::Backend;
use big_automata::okv::laws::Bytes;
use big_automata::okv::laws::OkvStreams;
use big_automata::okv::laws::{self};
use big_automata::part_store::PartStoreWrite;
use big_automata::rev::RevSource;
use big_automata::rev::laws::{self as stream_laws};
use futures::executor::block_on;
use proptest::prelude::*;
use sqlite_rs::Database;

use super::Store;
use super::config;
use super::fresh;
use crate::interlude::*;
use crate::*;

/// Every table of every database gets a name nobody else has.
static TABLES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub(crate) struct SqliteOkvBackend;

#[async_trait]
impl Backend for SqliteOkvBackend {
    type Db = Store;
    type Store = SqliteOkv<Database>;

    async fn open(&self) -> Res<Store> {
        fresh().await
    }

    async fn table(&self, db: &Store) -> Res<Self::Store> {
        let name = format!("table-{}", TABLES.fetch_add(1, Ordering::Relaxed));
        SqliteOkv::open(db, &name).await
    }

    async fn begin(&self, db: &Store) -> Res<SqlitePartTx<Database>> {
        db.begin().await
    }

    async fn commit(&self, tx: SqlitePartTx<Database>) -> Res<()> {
        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, tx: SqlitePartTx<Database>) -> Res<()> {
        tx.rollback().await
    }
}

macro_rules! okv_laws {
    ($($law:ident),+ $(,)?) => {$(
        #[test]
        fn $law() -> Res<()> {
            block_on(laws::$law(&SqliteOkvBackend))
        }
    )+};
}

okv_laws!(
    collapse,
    tombstones,
    revs_strictly_increase,
    rollback_leaves_no_trace,
    wakeups_follow_commit,
    soft_limits,
    get_reads_your_writes,
    tables_commit_together,
    consumer_cursors,
    prune_by_lowest_cursor,
);

#[test]
#[should_panic(expected = "an empty mutation batch has no rows to stamp")]
fn empty_batch_panics() {
    block_on(laws::empty_batch_panics(&SqliteOkvBackend)).expect("the law panics before returning");
}

#[test]
#[should_panic(expected = "past the committed rev")]
fn report_past_the_latest_rev_panics() {
    block_on(laws::report_past_the_latest_rev_panics(&SqliteOkvBackend))
        .expect("the law panics before returning");
}

#[test]
#[should_panic(expected = "unregistered consumer")]
fn report_unregistered_panics() {
    block_on(laws::report_unregistered_panics(&SqliteOkvBackend))
        .expect("the law panics before returning");
}

#[test]
fn okv_stream_commit_during_replay() -> Res<()> {
    block_on(stream_laws::stream_commit_during_replay(&OkvStreams(
        SqliteOkvBackend,
    )))
}

#[test]
fn okv_stream_overwrite_of_unread_key() -> Res<()> {
    block_on(stream_laws::stream_overwrite_of_unread_key(&OkvStreams(
        SqliteOkvBackend,
    )))
}

#[test]
fn okv_stream_commit_during_read() -> Res<()> {
    block_on(stream_laws::stream_commit_during_read(&OkvStreams(
        SqliteOkvBackend,
    )))
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn okv_differential(ops in laws::ops()) {
        block_on(laws::differential(&SqliteOkvBackend, ops))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }

    #[test]
    fn okv_stream_follows(script in stream_laws::stream_scripts()) {
        block_on(stream_laws::stream_follows(&OkvStreams(SqliteOkvBackend), script))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }
}

/// An okv batch and a part store write in one transaction commit together and roll back
/// together: the point of sharing the database.
#[test]
fn okv_and_part_store_share_a_transaction() -> Res<()> {
    block_on(async {
        let store = fresh().await?;
        let table = SqliteOkv::open(&store, "wants").await?;
        let part = PartKey::new(&[1]);
        let put = |value: &[u8]| {
            vec![Mutation::Put {
                key: b"k".to_vec(),
                value: value.to_vec(),
            }]
        };

        let mut tx = store.begin().await?;
        store
            .ensure_parts(&mut tx, std::slice::from_ref(&part))
            .await?;
        table.apply(&mut tx, put(b"lost")).await?;
        tx.rollback().await?;
        assert_eq!(table.latest_rev().await?, 0);

        let mut tx = store.begin().await?;
        table.apply(&mut tx, put(b"kept")).await?;
        let seen: Option<Bytes> = table.get(&mut tx, &b"k".to_vec()).await?;
        assert_eq!(seen.as_deref(), Some(&b"kept"[..]));
        tx.commit().await?;
        assert_eq!(table.latest_rev().await?, 1);

        // A second handle on the same name sees the committed table.
        let reopened = SqliteOkv::open(&store, "wants").await?;
        assert_eq!(reopened.latest_rev().await?, 1);
        Ok(())
    })
}
