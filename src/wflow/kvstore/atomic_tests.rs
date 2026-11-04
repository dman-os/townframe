use super::*;

use futures::FutureExt;
use sqlite_rs::RuntimeConfig;
use sqlite_rs::SqliteRuntime;
use sqlite_rs::test_support::TestDb;
use std::num::NonZeroUsize;
use wflow_core::kvstore::atomic::MemoryStore;
use wflow_core::kvstore::journal::Journal;
use wflow_core::kvstore::journal::JournalEntry;

const ALL: usize = usize::MAX;

fn key(bytes: &[u8]) -> Arc<[u8]> {
    bytes.into()
}

fn put(name: &[u8], value: &[u8]) -> Mutation {
    Mutation {
        key: key(name),
        value: Some(key(value)),
    }
}

async fn boot() -> Res<(TestDb, SqliteFactory)> {
    let db = sqlite_rs::test_support::db(&[])?;
    let factory = SqliteFactory::boot(db.clone()).await?;
    Ok((db, factory))
}

async fn revision(factory: &SqliteFactory, namespace: &str) -> Res<u64> {
    let read = factory
        .store(namespace)
        .read(&[], (Bound::Unbounded, Bound::Unbounded), 0)
        .await?;
    Ok(read.revision)
}

async fn writers(factory: &SqliteFactory, namespaces: &[&str]) -> Res<Vec<SqliteWriter>> {
    let mut writers = Vec::with_capacity(namespaces.len());
    for namespace in namespaces {
        writers.push(factory.writer(namespace).await?);
    }
    Ok(writers)
}

/// Queues one commit per writer before the committer task runs, so the
/// committer drains all of them as a single group.
async fn commit_together(
    writers: &mut [SqliteWriter],
    mutations: impl Fn(usize) -> Vec<Mutation>,
) -> Vec<Result<u64, CommitError>> {
    let mut commits: Vec<_> = writers
        .iter_mut()
        .enumerate()
        .map(|(index, writer)| writer.commit(0, mutations(index)))
        .collect();
    for commit in &mut commits {
        assert!(futures::poll!(commit.as_mut()).is_pending());
    }
    futures::future::join_all(commits).await
}

#[tokio::test]
async fn conformance_and_writer_ownership() -> Res<()> {
    let (db, factory) = boot().await?;
    let mut writer = factory.writer("conformance").await?;
    wflow_core::kvstore::atomic::tests::conformance(&mut writer).await?;

    let error = factory.writer("conformance").await.err().unwrap();
    assert!(
        error.to_string().contains("already has a writer"),
        "{error:#}"
    );
    let other = SqliteFactory::boot(db.clone()).await?;
    let error = other.writer("conformance").await.err().unwrap();
    assert!(
        error
            .to_string()
            .contains("held by another factory or process"),
        "{error:#}"
    );
    let committed = revision(&factory, "conformance").await?;
    drop(writer);

    let mut writer = other.writer("conformance").await?;
    assert_eq!(writer.commit(committed, Vec::new()).await?, committed + 1);
    assert_eq!(
        *other.store("conformance").subscribe().borrow(),
        committed + 1
    );
    // Revision hints are factory-local; the first factory's readers still see
    // the committed data.
    assert_eq!(
        *factory.store("conformance").subscribe().borrow(),
        committed
    );
    assert_eq!(revision(&factory, "conformance").await?, committed + 1);
    Ok(())
}

#[tokio::test]
async fn scans_match_memory_store() -> Res<()> {
    let (_db, factory) = boot().await?;
    let mut keys: Vec<Arc<[u8]>> = [
        &[][..],
        &[0],
        &[0, 0],
        &[0, 1],
        &[1],
        &[1, 0],
        &[255],
        &[255, 255],
        b"apple",
        b"apples",
    ]
    .into_iter()
    .map(key)
    .collect();
    keys.extend([1_u64, 255, 256, 65_536].map(|position| key(&position.to_be_bytes())));
    let mutations: Vec<Mutation> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| Mutation {
            key: Arc::clone(key),
            value: Some(vec![u8::try_from(index).unwrap(); index % 3].into()),
        })
        .collect();
    let memory = MemoryStore::default();
    memory.writer()?.commit(0, mutations.clone()).await?;
    let mut writer = factory.writer("scan").await?;
    writer.commit(0, mutations).await?;
    let sqlite = writer.reader();

    let mut probes = keys.clone();
    probes.extend([key(&[0, 0, 0]), key(&[2]), key(b"applf")]);
    let bounds: Vec<Bound<Arc<[u8]>>> = std::iter::once(Bound::Unbounded)
        .chain(
            probes
                .iter()
                .map(|probe| Bound::Included(Arc::clone(probe))),
        )
        .chain(
            probes
                .iter()
                .map(|probe| Bound::Excluded(Arc::clone(probe))),
        )
        .collect();
    let mut compared = 0;
    for lower in &bounds {
        for upper in &bounds {
            let range = (lower.clone(), upper.clone());
            if !btree_map_accepts(&range) {
                continue;
            }
            for limit in [1, 3, ALL] {
                let expected = memory.read(&probes, range.clone(), limit).await?;
                let actual = sqlite.read(&probes, range.clone(), limit).await?;
                assert_eq!(actual.revision, expected.revision);
                assert_eq!(actual.values, expected.values, "{range:?}");
                assert_eq!(actual.entries, expected.entries, "{range:?} limit {limit}");
                compared += 1;
            }
        }
    }
    assert!(compared > 1_000, "{compared}");
    Ok(())
}

fn btree_map_accepts(range: &KeyRange) -> bool {
    match range {
        (Bound::Excluded(start), Bound::Excluded(end)) => start < end,
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => start <= end,
        _ => true,
    }
}

#[test]
#[should_panic(expected = "range start and end are equal and excluded")]
fn empty_excluded_range_panics_like_btree_map() {
    assert_ordered(&(Bound::Excluded(key(b"a")), Bound::Excluded(key(b"a"))));
}

#[test]
#[should_panic(expected = "range start is greater than range end")]
fn reversed_range_panics_like_btree_map() {
    assert_ordered(&(Bound::Included(key(b"b")), Bound::Included(key(b"a"))));
}

#[tokio::test]
async fn queued_commits_share_one_transaction() -> Res<()> {
    let (_db, factory) = boot().await?;
    let namespaces: Vec<String> = (0..8).map(|index| format!("group-{index}")).collect();
    let names: Vec<&str> = namespaces.iter().map(String::as_str).collect();
    let mut writers = writers(&factory, &names).await?;
    let before = factory.commit_stats();
    let results = commit_together(&mut writers, |index| vec![put(b"k", &[index as u8])]).await;
    assert!(
        results.iter().all(|result| matches!(result, Ok(1))),
        "{results:?}"
    );
    let after = factory.commit_stats();
    assert_eq!(after.transactions - before.transactions, 1);
    assert_eq!(after.requests - before.requests, 8);
    for (index, writer) in writers.iter().enumerate() {
        let read = writer
            .reader()
            .read(&[key(b"k")], (Bound::Unbounded, Bound::Unbounded), 0)
            .await?;
        assert_eq!(read.values, vec![Some(key(&[index as u8]))]);
    }
    Ok(())
}

#[tokio::test]
async fn failing_request_is_isolated_from_its_group() -> Res<()> {
    let (db, factory) = boot().await?;
    db.execute_batch(
        "CREATE TRIGGER poison BEFORE INSERT ON wflow_atomic_entries
          WHEN NEW.key = X'706f69736f6e'
         BEGIN SELECT RAISE(ABORT, 'poison'); END",
    )
    .await?;
    let mut writers = writers(&factory, &["left", "poisoned", "right"]).await?;
    let before = factory.commit_stats();
    let results = commit_together(&mut writers, |index| match index {
        1 => vec![put(b"poison", b"x")],
        _ => vec![put(b"k", b"v")],
    })
    .await;
    assert!(matches!(results[0], Ok(1)), "{results:?}");
    assert!(
        matches!(&results[1], Err(CommitError::NotCommitted(error)) if format!("{error:#}").contains("poison")),
        "{results:?}"
    );
    assert!(matches!(results[2], Ok(1)), "{results:?}");
    assert_eq!(factory.commit_stats().transactions - before.transactions, 2);
    assert_eq!(revision(&factory, "poisoned").await?, 0);
    assert_eq!(revision(&factory, "right").await?, 1);
    Ok(())
}

#[tokio::test]
async fn failed_commit_reports_unknown_to_the_whole_group() -> Res<()> {
    let (db, factory) = boot().await?;
    // The deferred foreign key passes every statement and fails only COMMIT.
    db.execute_batch(
        "CREATE TABLE doom_parent (id INTEGER PRIMARY KEY);
         CREATE TABLE doom (
             parent INTEGER REFERENCES doom_parent(id) DEFERRABLE INITIALLY DEFERRED
         );
         CREATE TRIGGER doom AFTER INSERT ON wflow_atomic_entries
          WHEN NEW.key = X'646f6f6d'
         BEGIN INSERT INTO doom (parent) VALUES (42); END",
    )
    .await?;
    let mut writers = writers(&factory, &["bystander", "doomed"]).await?;
    let results = commit_together(&mut writers, |index| match index {
        1 => vec![put(b"doom", b"x")],
        _ => vec![put(b"k", b"v")],
    })
    .await;
    assert!(
        results
            .iter()
            .all(|result| matches!(result, Err(CommitError::Unknown(_)))),
        "{results:?}"
    );
    assert_eq!(revision(&factory, "bystander").await?, 0);
    assert_eq!(revision(&factory, "doomed").await?, 0);
    assert_eq!(writers[0].commit(0, vec![put(b"k", b"v")]).await?, 1);
    Ok(())
}

#[tokio::test]
async fn dropped_writer_keeps_ownership_until_queued_commit_lands() -> Res<()> {
    let (db, factory) = boot().await?;
    let mut writer = factory.writer("handoff").await?;
    let blocker = db.begin_write().await?;
    let mut commit = writer.commit(0, vec![put(b"k", b"v")]);
    assert!(futures::poll!(commit.as_mut()).is_pending());
    drop(commit);
    drop(writer);

    let mut next = factory.writer("handoff").boxed();
    for _ in 0..8 {
        assert!(futures::poll!(next.as_mut()).is_pending());
        tokio::task::yield_now().await;
    }
    blocker.rollback().await?;
    let next = next.await?;
    let read = next
        .reader()
        .read(&[key(b"k")], (Bound::Unbounded, Bound::Unbounded), 0)
        .await?;
    assert_eq!(read.revision, 1);
    assert_eq!(read.values, vec![Some(key(b"v"))]);
    Ok(())
}

#[tokio::test]
async fn journal_persists_across_reopen() -> Res<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("wflow.sqlite");
    let limit = NonZeroUsize::new(8).unwrap();
    {
        let runtime = SqliteRuntime::new(RuntimeConfig::default());
        let factory = SqliteFactory::boot(runtime.database(&path, &[], "_unused")).await?;
        let mut journal = Journal::open(factory.writer("journal").await?, 7).await?;
        assert_eq!(journal.authority_epoch(), 1);
        journal
            .append(0, key(b"one"), vec![put(b"idx", b"1")])
            .await?;
        journal
            .append(1, key(b"two"), vec![put(b"idx", b"2")])
            .await?;

        let mut legacy = factory.writer("legacy").await?;
        legacy.commit(0, vec![put(b"raw", b"row")]).await?;
        drop(legacy);
        let error = Journal::open(factory.writer("legacy").await?, 7)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("unversioned"), "{error:#}");
        assert_eq!(revision(&factory, "legacy").await?, 1);
    }

    let runtime = SqliteRuntime::new(RuntimeConfig::default());
    let factory = SqliteFactory::boot(runtime.database(&path, &[], "_unused")).await?;
    let journal = Journal::open(factory.writer("journal").await?, 7).await?;
    assert_eq!(journal.authority_epoch(), 2);
    assert_eq!(journal.frontier(), 2);
    let page = journal.reader().read(1, limit).await?;
    assert_eq!(page.frontier, 2);
    assert_eq!(
        page.entries,
        vec![
            JournalEntry {
                position: 1,
                value: key(b"one")
            },
            JournalEntry {
                position: 2,
                value: key(b"two")
            },
        ]
    );
    assert_eq!(journal.reader().index(b"idx").await?, Some(key(b"2")));
    assert_eq!(revision(&factory, "legacy").await?, 1);
    drop(journal);

    {
        let mut journal = Journal::open(factory.writer("journal").await?, 7).await?;
        assert_eq!(journal.authority_epoch(), 3);
        journal
            .append(2, key(b"three"), vec![put(b"idx", b"3")])
            .await?;
        assert_eq!(journal.save_snapshot(2, key(b"snap-2")).await?, 2);
        assert_eq!(journal.compact(2).await?, 2);
    }

    let runtime = SqliteRuntime::new(RuntimeConfig::default());
    let factory = SqliteFactory::boot(runtime.database(&path, &[], "_unused")).await?;
    let journal = Journal::open(factory.writer("journal").await?, 7).await?;
    assert_eq!(journal.authority_epoch(), 4);
    assert_eq!(journal.frontier(), 3);
    assert_eq!(journal.snapshot_cut(), 2);
    assert_eq!(journal.compacted_through(), 2);
    let snap = journal
        .reader()
        .snapshot()
        .await?
        .expect("published snapshot");
    assert_eq!(snap.cut, 2);
    assert_eq!(snap.compacted_through, 2);
    assert_eq!(snap.frontier, 3);
    assert_eq!(snap.value, key(b"snap-2"));
    assert!(journal.reader().read(2, limit).await.is_err());
    let suffix = journal.reader().read(3, limit).await?;
    assert_eq!(
        suffix.entries,
        vec![JournalEntry {
            position: 3,
            value: key(b"three"),
        }]
    );
    assert_eq!(journal.reader().index(b"idx").await?, Some(key(b"3")));
    Ok(())
}
