//! What SQLite adds to the laws: interrupted writes, reopening a file, migrations.

use big_automata::part_store::Generation;
use big_automata::part_store::LogSelector;
use big_automata::part_store::PartStoreRead;
use big_automata::part_store::PartStoreServe;
use big_automata::part_store::PartStoreWrite;
use big_automata::part_store::laws::add;
use big_automata::part_store::laws::grant;
use big_automata::part_store::laws::obj;
use big_automata::part_store::laws::part;
use big_automata::part_store::laws::principal;
use big_automata::part_store::laws::remove;
use big_automata::part_store::laws::set;
use big_automata::part_store::laws::stage;
use big_automata::part_store::oracle::PartOracle;
use big_automata::part_store::oracle::PartWrite;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::RevSource;
use futures::executor::block_on;
use sqlite_rs::Database;
use sqlite_rs::MigrationState;

use super::RUNTIME;
use super::Store;
use crate::interlude::*;
use crate::*;

fn open_file(path: &Path) -> Database {
    RUNTIME.database(path, MIGRATIONS, MIGRATION_TABLE)
}

async fn commit(store: &Store, writes: Vec<PartWrite>) -> Res<Option<u64>> {
    let mut tx = store.begin().await?;
    for write in writes {
        stage(store, &mut tx, write).await?;
    }
    tx.commit().await
}

/// The store's observable state is the oracle's: the whole log, the object states, the root
/// bucket of every part.
async fn assert_state(store: &Store, oracle: &PartOracle) -> Res<()> {
    let latest = oracle.latest_rev();
    assert_eq!(RevSource::latest_rev(store).await?, latest);
    if latest > 0 {
        let limits = Limits {
            max_entries: std::num::NonZeroUsize::MAX,
            max_bytes: None,
        };
        let mut page = Page::default();
        store
            .read(&LogSelector::All, 0, latest, limits, &mut page)
            .await?;
        assert_eq!(page, oracle.read(&LogSelector::All, 0, latest, limits));
    }
    let objs = (0..4).map(obj).collect::<Vec<_>>();
    assert_eq!(
        store.obj_states(&objs).await?,
        objs.iter()
            .map(|obj| oracle.obj_state(obj))
            .collect::<Vec<_>>()
    );
    for index in 0..3 {
        assert_eq!(
            store
                .bucket_summaries(&part(index), &[BuckId::ROOT])
                .await?,
            vec![oracle.bucket_summary(&part(index), BuckId::ROOT)]
        );
    }
    Ok(())
}

/// A write transaction that never commits, abandoned part way (dropped, as a crashed task
/// would leave it), leaves nothing; and a file reopened later holds exactly what committed.
#[test]
fn an_interrupted_write_leaves_nothing_and_reopening_sees_the_committed_state() -> Res<()> {
    block_on(async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("parts.sqlite");
        let mut oracle = PartOracle::default();

        let store = Store::open(open_file(&path)).await?;
        let committed = vec![
            vec![set(&[(0, 1), (1, 2)]), add(&[(0, 0), (1, 0), (1, 1)])],
            vec![grant(&[(0, 0)]), remove(&[(1, 0)])],
        ];
        for writes in committed {
            commit(&store, writes.clone()).await?;
            oracle.commit(writes);
        }
        assert_state(&store, &oracle).await?;

        // Staged, applied inside the transaction, never committed.
        let mut tx = store.begin().await?;
        for write in [
            set(&[(2, 3), (0, 9)]),
            add(&[(2, 2), (3, 1)]),
            grant(&[(2, 1)]),
        ] {
            stage(&store, &mut tx, write).await?;
        }
        drop(tx);
        assert_state(&store, &oracle).await?;
        drop(store);

        let reopened = Store::open(open_file(&path)).await?;
        assert_state(&reopened, &oracle).await?;
        // And it carries on from the committed rev.
        let writes = vec![set(&[(2, 3)]), add(&[(2, 2)])];
        assert_eq!(
            commit(&reopened, writes.clone()).await?,
            oracle.commit(writes)
        );
        assert_state(&reopened, &oracle).await
    })
}

/// A rolled back transaction that stamped rows leaves the rev, the watch and every table as
/// they were; the next commit reuses the rev.
#[test]
fn a_rolled_back_stamp_does_not_consume_a_rev() -> Res<()> {
    block_on(async {
        let store = super::fresh().await?;
        commit(&store, vec![set(&[(0, 1)]), add(&[(0, 0)])]).await?;
        let mut tx = store.begin().await?;
        stage(&store, &mut tx, set(&[(0, 2)])).await?;
        tx.rollback().await?;
        assert_eq!(RevSource::latest_rev(&store).await?, 1);
        assert_eq!(RevSource::watch(&store).latest(), 1);
        assert_eq!(commit(&store, vec![set(&[(0, 2)])]).await?, Some(2));
        Ok(())
    })
}

#[test]
fn migrations_apply_from_empty_and_are_idempotent() -> Res<()> {
    block_on(async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("parts.sqlite");
        let db = open_file(&path);
        assert!(
            db.migration_status()
                .await?
                .iter()
                .all(|(_, state)| *state == MigrationState::Pending),
            "a new file has applied nothing"
        );
        db.migrate().await?;
        db.migrate().await?;
        db.apply_migrations(MIGRATION_TABLE, MIGRATIONS).await?;
        let status = db.migration_status().await?;
        assert_eq!(status.len(), MIGRATIONS.len());
        assert!(
            status
                .iter()
                .all(|(_, state)| *state == MigrationState::Applied)
        );

        // Data survives a second registration of the same file, and its seed row is not
        // inserted again.
        let store = Store::open(db).await?;
        commit(&store, vec![set(&[(0, 1)]), add(&[(0, 0)])]).await?;
        drop(store);
        let again = open_file(&path);
        again.migrate().await?;
        let store = Store::open(again).await?;
        assert_eq!(RevSource::latest_rev(&store).await?, 1);
        Ok(())
    })
}

/// A payload written in one transaction is read by the transaction that wrote it before commit
/// (its own writes are visible to it), and by nobody else until commit.
#[test]
fn a_transaction_reads_its_own_writes_and_readers_wait() -> Res<()> {
    block_on(async {
        let store = super::fresh().await?;
        let mut tx = store.begin().await?;
        stage(&store, &mut tx, set(&[(0, 1)])).await?;
        stage(&store, &mut tx, add(&[(0, 0)])).await?;
        // Pending reads by others see the empty store.
        assert_eq!(store.member_count(&part(0)).await?, 0);
        tx.commit().await?;
        assert_eq!(store.member_count(&part(0)).await?, 1);
        Ok(())
    })
}

/// The generation is in the file: a reopen (a restart over the same files) keeps it, a mint
/// is durable across the next reopen, and neither moves the rev.
#[test]
fn the_generation_and_its_mints_survive_reopening() -> Res<()> {
    block_on(async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("parts.sqlite");
        let summarized = async |store: &Store| -> Res<Generation> {
            let summaries = store.part_summaries(&principal(0), &[(part(0), 0)]).await?;
            Ok(summaries[0].expect("the part is readable").generation)
        };
        let store = Store::open(open_file(&path)).await?;
        commit(
            &store,
            vec![set(&[(0, 1)]), add(&[(0, 0)]), grant(&[(0, 0)])],
        )
        .await?;
        let created = store.generation().await?;
        assert_ne!(created, Generation::default());
        drop(store);

        let store = Store::open(open_file(&path)).await?;
        assert_eq!(store.generation().await?, created, "a reopen keeps it");
        assert_eq!(summarized(&store).await?, created);
        let minted = store.mint_generation().await?;
        assert_ne!(minted, created);
        assert_eq!(
            RevSource::latest_rev(&store).await?,
            1,
            "a mint stamps no rev"
        );
        drop(store);

        let store = Store::open(open_file(&path)).await?;
        assert_eq!(store.generation().await?, minted, "a mint is durable");
        assert_eq!(summarized(&store).await?, minted);
        Ok(())
    })
}
