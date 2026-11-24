//! The part store laws and the differential against the oracle, over SQLite; then what SQLite
//! adds: crashes and reopening, the peer cursor contract, query plans, planted bugs.

use std::sync::LazyLock;

use big_automata::part_store::laws::Backend;
use big_automata::part_store::laws::LogStreams;
use big_automata::part_store::laws::{self};
use big_automata::rev::laws::{self as stream_laws};
use futures::executor::block_on;
use proptest::prelude::*;
use sqlite_rs::Database;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;

use crate::interlude::*;
use crate::*;

mod bench;
mod crash;
mod cursors;
mod mutations;
mod plans;

pub(crate) type Store = SqlitePartStore<Database>;

pub(crate) static RUNTIME: LazyLock<SqliteRuntime> = LazyLock::new(|| {
    SqliteRuntime::new(RuntimeConfig {
        workers: 2,
        ..default()
    })
});

/// A new, empty store on its own ephemeral database file.
pub(crate) async fn fresh() -> Res<Store> {
    let db = RUNTIME.ephemeral(MIGRATIONS)?;
    Store::open(db).await
}

#[derive(Clone)]
pub(crate) struct Sqlite;

#[async_trait]
impl Backend for Sqlite {
    type Store = Store;

    async fn open(&self) -> Res<Store> {
        fresh().await
    }

    async fn begin(&self, store: &Store) -> Res<SqlitePartTx<Database>> {
        store.begin().await
    }

    async fn commit(&self, tx: SqlitePartTx<Database>) -> Res<()> {
        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, tx: SqlitePartTx<Database>) -> Res<()> {
        tx.rollback().await
    }
}

macro_rules! store_laws {
    ($($law:ident),+ $(,)?) => {$(
        #[test]
        fn $law() -> Res<()> {
            block_on(laws::$law(&Sqlite))
        }
    )+};
}

store_laws!(
    replay_reconstructs_membership_and_access,
    collapse_and_tombstones,
    pending_goes_live_once,
    authorization_follows_current_access,
    revocation_hides_every_row_of_the_part,
    revocation_mid_stream_hides_the_unread_rows,
    object_routes_follow_the_readable_parts,
    revocation_hides_the_parts_buckets,
    an_empty_window_reads_verdicts_only,
    authorized_reads_carry_their_payloads,
    grant_returns_the_backlog,
    removals_reach_every_route_above_its_bound,
    a_payload_with_no_canonical_form_is_refused,
    dirty_count_counts_log_rows,
    writes_are_atomic,
    unchanged_writes_stamp_nothing,
);

#[test]
fn log_stream_commit_during_replay() -> Res<()> {
    block_on(stream_laws::stream_commit_during_replay(&LogStreams::new(
        Sqlite,
    )))
}

#[test]
fn log_stream_overwrite_of_unread_key() -> Res<()> {
    block_on(stream_laws::stream_overwrite_of_unread_key(
        &LogStreams::new(Sqlite),
    ))
}

#[test]
fn log_stream_commit_during_read() -> Res<()> {
    block_on(stream_laws::stream_commit_during_read(&LogStreams::new(
        Sqlite,
    )))
}

/// Each case opens a database and checks the store against the oracle after every commit, so
/// the default is smaller than proptest's; `PROPTEST_CASES` overrides it.
pub(crate) fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(32);
    ProptestConfig {
        cases,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn differential(ops in laws::part_ops()) {
        block_on(laws::differential(&Sqlite, ops))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }

    #[test]
    fn log_stream_follows(script in stream_laws::stream_scripts()) {
        block_on(stream_laws::stream_follows(&LogStreams::new(Sqlite), script))
            .map_err(|err| TestCaseError::fail(format!("{err:?}")))?;
    }
}
