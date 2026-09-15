//! Tests for the sqlite store.
//!
//! Everything here is either a property the store's trait cannot state (the schema's invariants,
//! the query plan, the encoding a column must hold) or a regression guard for a decision the
//! migration comments explain.

use super::{SqliteVtreeStore, queries};
use crate::backend::BackendId;
use crate::codec::encode_path;
use crate::delta::Delta;
use crate::entry::{Entry, Kind, StatFingerprint, TimeStamp, Token};
use crate::error::StoredError;
use crate::interlude::*;
use crate::path::PathError;
use crate::path::RelPath;
use crate::store::VtreeStore;
use sqlx::Row;

fn path(text: &str) -> RelPath {
    RelPath::try_new(text.split('/').map(OsString::from).collect()).expect(ERROR_PARSE)
}

fn stat(len: u64) -> StatFingerprint {
    StatFingerprint {
        len,
        mode: 0o644,
        mtime: TimeStamp { secs: 1, nanos: 0 },
    }
}

fn file(digest: u8) -> Entry {
    Entry::file(Token::blake3([digest; 32]), stat(1))
}

/// The plan a test checks cannot drift from the statement the store runs.
#[test]
fn the_explain_files_quote_the_queries_they_explain() {
    for (explain, query) in [
        (queries::EXPLAIN_SELECT_ENTRY, queries::SELECT_ENTRY),
        (
            queries::EXPLAIN_SELECT_PAGE_FROM_START,
            queries::SELECT_PAGE_FROM_START,
        ),
        (
            queries::EXPLAIN_SELECT_PAGE_AFTER,
            queries::SELECT_PAGE_AFTER,
        ),
    ] {
        assert!(
            explain.strip_prefix(queries::EXPLAIN_QUERY_PLAN) == Some(query),
            "the plan query is not its query prefixed with the plan keyword",
        );
    }
}

/// Every read seeks the primary key and walks it in order, so none of them sorts.
#[tokio::test]
async fn reads_seek_the_primary_key_without_sorting() {
    let store = SqliteVtreeStore::ephemeral()
        .await
        .expect("an ephemeral store opens");
    let rep = BackendId::new("fs");

    for (sql, at_the_start) in [
        (queries::EXPLAIN_SELECT_ENTRY, None),
        (queries::EXPLAIN_SELECT_PAGE_FROM_START, Some(true)),
        (queries::EXPLAIN_SELECT_PAGE_AFTER, Some(false)),
    ] {
        let mut query = sqlx::query(sql).bind(rep.as_str());
        query = match at_the_start {
            None => query.bind(encode_path(&path("a.txt"))),
            Some(true) => query.bind(64i64),
            Some(false) => query.bind(encode_path(&path("a.txt"))).bind(64i64),
        };

        let rows = query
            .fetch_all(&store.sql.read_pool)
            .await
            .expect("the plan query runs");
        let plan = rows
            .into_iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            plan.contains("PRIMARY KEY"),
            "the read must seek the primary key, got: {plan}"
        );
        assert!(
            !plan.contains("TEMP B-TREE"),
            "the read must not sort, got: {plan}"
        );
    }
}

#[tokio::test]
async fn rows_and_reps_survive_a_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store_path = dir.path().join(".dtree/store.sqlite3");
    let rep = BackendId::new("fs");

    let generation = {
        let store = SqliteVtreeStore::open(&store_path).await?;
        store
            .apply(
                &rep,
                &[
                    Delta::Added {
                        path: path("notes/plan.md"),
                        entry: file(1),
                    },
                    Delta::Added {
                        path: path("todo.md"),
                        entry: file(2).stubbed(),
                    },
                    Delta::Added {
                        path: path("elsewhere"),
                        entry: Entry::symlink("./todo.md", stat(9)),
                    },
                ],
            )
            .await?
    };

    let store = SqliteVtreeStore::open(&store_path).await?;
    assert_eq!(store.generation(&rep).await?, Some(generation));
    assert_eq!(
        store.entry(&rep, &path("todo.md")).await?,
        Some(file(2).stubbed())
    );
    assert_eq!(
        store.entry(&rep, &path("elsewhere")).await?,
        Some(Entry::symlink("./todo.md", stat(9)))
    );
    assert_eq!(
        store.entry(&rep, &path("notes")).await?,
        Some(Entry::dir(None)),
        "implied directories are recorded"
    );
    let page = store.scan_page(&rep, None, 10).await?;
    assert_eq!(
        page.iter()
            .map(|(path, _)| path.to_string())
            .collect::<Vec<_>>(),
        vec!["elsewhere", "notes", "notes/plan.md", "todo.md"]
    );
    assert_eq!(store.reps().await?, vec![rep.clone()]);
    assert!(store.drop_rep(&rep).await?);
    assert_eq!(store.generation(&rep).await?, None);
    Ok(())
}

/// A dropped rep takes its rows with it: `ON DELETE CASCADE` is the schema's job, and this pins
/// that `foreign_keys` is on for the connections the store opens.
#[tokio::test]
async fn dropping_a_rep_drops_its_entries() -> Result<()> {
    let store = SqliteVtreeStore::ephemeral().await?;
    let rep = BackendId::new("fs");
    store
        .apply(
            &rep,
            &[
                Delta::Added {
                    path: path("notes/plan.md"),
                    entry: file(1),
                },
                Delta::Added {
                    path: path("todo.md"),
                    entry: file(2),
                },
            ],
        )
        .await?;

    assert!(store.drop_rep(&rep).await?);
    assert!(
        store.scan_page(&rep, None, 10).await?.is_empty(),
        "a dropped rep has no rows",
    );
    assert!(
        !store.drop_rep(&rep).await?,
        "and dropping it twice is false"
    );
    Ok(())
}

#[tokio::test]
async fn applying_an_empty_batch_does_not_move_the_rep() -> Result<()> {
    let store = SqliteVtreeStore::ephemeral().await?;
    let rep = BackendId::new("fs");
    assert_eq!(store.generation(&rep).await?, None);
    assert_eq!(store.apply(&rep, &[]).await?, 0, "no rep, no generation");
    assert_eq!(store.generation(&rep).await?, None, "and no rep row either");

    let first = store
        .apply(
            &rep,
            &[Delta::Added {
                path: path("a.txt"),
                entry: file(1),
            }],
        )
        .await?;
    assert_eq!(first, 1);
    assert_eq!(store.apply(&rep, &[]).await?, 1);
    let second = store
        .apply(
            &rep,
            &[Delta::Touched {
                path: path("a.txt"),
                entry: file(1).with_stat(stat(7)),
            }],
        )
        .await?;
    assert_eq!(second, 2);
    assert_eq!(
        store.entry(&rep, &path("a.txt")).await?,
        Some(file(1).with_stat(stat(7)))
    );
    Ok(())
}

#[tokio::test]
async fn pages_resume_strictly_after_the_cursor() -> Result<()> {
    let store = SqliteVtreeStore::ephemeral().await?;
    let rep = BackendId::new("fs");
    store
        .apply(
            &rep,
            &[
                Delta::Added {
                    path: path("a.txt"),
                    entry: file(1),
                },
                Delta::Added {
                    path: path("b.txt"),
                    entry: file(2),
                },
                Delta::Added {
                    path: path("c.txt"),
                    entry: file(3),
                },
            ],
        )
        .await?;

    let first = store.scan_page(&rep, None, 2).await?;
    assert_eq!(first.len(), 2);
    let second = store.scan_page(&rep, Some(&first[1].0), 2).await?;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].0, path("c.txt"));
    assert_eq!(
        store.scan_page(&rep, Some(&path("c.txt")), 2).await?,
        Vec::new()
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_rows_are_refused() -> Result<()> {
    let store = SqliteVtreeStore::ephemeral().await?;
    let rep = BackendId::new("fs");
    store
        .apply(
            &rep,
            &[Delta::Added {
                path: path("a.txt"),
                entry: file(1),
            }],
        )
        .await?;

    // A directory row that still carries a file's origin is not a shape we ever
    // write, and reading it must not pretend otherwise. What comes back names the
    // row the way a human would look for it, and says what about it is wrong —
    // "something went wrong" would send the reader to the wrong place.
    sqlx::query(queries::CORRUPT_KIND)
        .bind(encode_path(&path("a.txt")))
        .execute(&store.sql.write_pool)
        .await?;
    match store.entry(&rep, &path("a.txt")).await {
        Err(Error::Stored {
            path: reported,
            reason,
        }) => {
            assert_eq!(reported, "a.txt", "the same name `RelPath` prints");
            assert_eq!(
                reason,
                StoredError::Shape {
                    kind: Kind::Dir,
                    detail: "carries a payload"
                }
            );
        }
        other => panic!("expected a refused row, got {other:?}"),
    }

    // A path column that is not a decodable component list is refused the same
    // way, with the bytes rendered since the decoded path is exactly what is
    // missing.
    sqlx::query(queries::CORRUPT_PATH)
        .bind(vec![b'a', 0u8])
        .bind(encode_path(&path("a.txt")))
        .execute(&store.sql.write_pool)
        .await?;
    match store.scan_page(&rep, None, 10).await {
        Err(Error::Stored {
            path: reported,
            reason,
        }) => {
            assert_eq!(
                reported, "a/",
                "and the same rendering when it cannot decode"
            );
            assert_eq!(
                reason,
                StoredError::Path {
                    reason: PathError::Empty
                }
            );
        }
        other => panic!("expected a refused row, got {other:?}"),
    }
    Ok(())
}
