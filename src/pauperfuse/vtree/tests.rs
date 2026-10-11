use std::collections::VecDeque;

use super::*;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("observation interrupted")]
struct Interrupted;

struct Observation(VecDeque<Result<TreeEntry, Interrupted>>);
impl BackendTree for Observation {
    type Error = Interrupted;
    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        self.0.pop_front().transpose()
    }
}
fn observation(entries: Vec<TreeEntry>) -> Observation {
    Observation(entries.into_iter().map(Ok).collect())
}
fn path(name: &str) -> RelPath {
    RelPath::parse(name).unwrap()
}
fn directory(name: &str) -> TreeEntry {
    TreeEntry {
        path: path(name),
        description: Description::Directory,
    }
}
fn file(name: &str, source: &str, size: Option<u64>) -> TreeEntry {
    TreeEntry {
        path: path(name),
        description: Description::File {
            source: Source {
                backend: BackendId(source.into()),
                output: OutputVersion {
                    output: vec![0, 255, 1],
                    version: vec![2, 0, 254],
                },
            },
            size,
        },
    }
}
fn limit(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap()
}
async fn store() -> (tempfile::TempDir, VtreeStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = VtreeStore::open(&directory.path().join("trees.sqlite"))
        .await
        .unwrap();
    (directory, store)
}

#[tokio::test]
async fn backend_registry_reuses_identity_and_sources_require_registered_foreign_keys() {
    let (_directory, store) = store().await;
    let owner = store
        .register(&BackendId("filesystem".into()))
        .await
        .unwrap();
    assert_eq!(
        store
            .register(&BackendId("filesystem".into()))
            .await
            .unwrap(),
        owner
    );
    assert_eq!(
        store.lookup(&BackendId("filesystem".into())).await.unwrap(),
        Some(owner)
    );
    assert_eq!(
        store.lookup(&BackendId("missing".into())).await.unwrap(),
        None
    );
    let error = store
        .replace(owner, &mut observation(vec![file("a", "daybook", None)]))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ObservationError::Store(StoreError::UnknownSource(BackendId(name))) if name == "daybook")
    );
    assert_eq!(
        store.version(owner).await.unwrap(),
        TreeVersion {
            backend: owner,
            generation: 0
        }
    );

    // Bypass lookup to pin the actual SQL foreign-key boundary as well.
    let error = sqlx::query(queries::INSERT)
        .bind(owner.0)
        .bind(b"a\0".as_slice())
        .bind(0)
        .bind(owner.0 + 100)
        .bind(b"output".as_slice())
        .bind(b"version".as_slice())
        .bind(None::<i64>)
        .bind(None::<Vec<u8>>)
        .execute(&store.pool)
        .await
        .unwrap_err();
    assert!(
        error
            .as_database_error()
            .unwrap()
            .is_foreign_key_violation()
    );
    let source = store.register(&BackendId("daybook".into())).await.unwrap();
    assert_ne!(source, owner);
    store
        .replace(owner, &mut observation(vec![file("a", "daybook", None)]))
        .await
        .unwrap();
    let error = sqlx::query(queries::INSERT)
        .bind(owner.0 + 100)
        .bind(b"a\0".as_slice())
        .bind(1)
        .bind(None::<i64>)
        .bind(None::<Vec<u8>>)
        .bind(None::<Vec<u8>>)
        .bind(None::<i64>)
        .bind(None::<Vec<u8>>)
        .execute(&store.pool)
        .await
        .unwrap_err();
    assert!(
        error
            .as_database_error()
            .unwrap()
            .is_foreign_key_violation()
    );
}

#[tokio::test]
async fn ordered_metadata_roundtrips_and_persists_without_materializing_bytes() {
    let (location, store) = store().await;
    let backend = store.register(&BackendId("virtual".into())).await.unwrap();
    store.register(&BackendId("producer".into())).await.unwrap();
    let entries = vec![
        directory(""),
        directory("a"),
        file("a/note", "producer", None),
        TreeEntry {
            path: path("b"),
            description: Description::Symlink {
                target: r"../target/\xff".into(),
            },
        },
        file("c", "producer", Some(42)),
    ];
    let version = store
        .replace(backend, &mut observation(entries.clone()))
        .await
        .unwrap();
    assert_eq!(
        version,
        TreeVersion {
            backend,
            generation: 1
        }
    );
    let first = store.page(version, None, limit(2)).await.unwrap();
    assert_eq!(first, entries[..2]);
    let second = store
        .page(version, Some(&first.last().unwrap().path), limit(3))
        .await
        .unwrap();
    assert_eq!(second, entries[2..]);
    assert!(
        store
            .page(version, Some(&path("c")), limit(3))
            .await
            .unwrap()
            .is_empty()
    );
    let reopened = VtreeStore::open(&location.path().join("trees.sqlite"))
        .await
        .unwrap();
    assert_eq!(
        reopened.lookup(&BackendId("virtual".into())).await.unwrap(),
        Some(backend)
    );
    assert_eq!(
        reopened.page(version, None, limit(10)).await.unwrap(),
        entries
    );
}

#[tokio::test]
async fn failed_observation_after_an_insert_keeps_old_tree_and_generation() {
    let (_directory, store) = store().await;
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let old = vec![file("old", "source", None)];
    let version = store
        .replace(backend, &mut observation(old.clone()))
        .await
        .unwrap();
    let mut failing = Observation(VecDeque::from([Ok(directory("new")), Err(Interrupted)]));
    assert!(matches!(
        store.replace(backend, &mut failing).await,
        Err(ObservationError::Backend(Interrupted))
    ));
    assert_eq!(store.version(backend).await.unwrap(), version);
    assert_eq!(store.page(version, None, limit(10)).await.unwrap(), old);
}

#[tokio::test]
async fn duplicate_unordered_invalid_metadata_and_unregistered_entries_roll_back_whole_observation()
{
    let (_directory, store) = store().await;
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let old = vec![directory("old")];
    let version = store
        .replace(backend, &mut observation(old.clone()))
        .await
        .unwrap();
    for (entries, expected_reason) in [
        (vec![directory("a"), directory("a")], "order"),
        (vec![directory("z"), directory("a")], "order"),
        (
            vec![
                directory("a"),
                TreeEntry {
                    path: path("z"),
                    description: Description::Symlink {
                        target: "bad\0target".into(),
                    },
                },
            ],
            "encoding",
        ),
        (
            vec![directory("a"), file("b", "unregistered", None)],
            "source",
        ),
        (
            vec![directory("a"), file("b", "source", Some(u64::MAX))],
            "size",
        ),
        (vec![file("", "source", None)], "root"),
    ] {
        let failure = store
            .replace(backend, &mut observation(entries))
            .await
            .unwrap_err();
        let actual_reason = match failure {
            ObservationError::Store(StoreError::Order(_)) => "order",
            ObservationError::Store(StoreError::Encoding(_)) => "encoding",
            ObservationError::Store(StoreError::UnknownSource(_)) => "source",
            ObservationError::Store(StoreError::Size(_)) => "size",
            ObservationError::Store(StoreError::RootKind) => "root",
            other => panic!("unexpected failure: {other:?}"),
        };
        assert_eq!(actual_reason, expected_reason);
        assert_eq!(store.version(backend).await.unwrap(), version);
        assert_eq!(store.page(version, None, limit(10)).await.unwrap(), old);
    }
}

#[tokio::test]
async fn replacement_between_pages_is_stale_and_complete_empty_tree_advances_generation() {
    let (_directory, store) = store().await;
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let version = store
        .replace(
            backend,
            &mut observation(vec![directory("a"), directory("b")]),
        )
        .await
        .unwrap();
    assert_eq!(
        store.page(version, None, limit(1)).await.unwrap(),
        vec![directory("a")]
    );
    let next = store
        .replace(backend, &mut observation(vec![]))
        .await
        .unwrap();
    assert_eq!(next.generation, version.generation + 1);
    assert!(
        matches!(store.page(version, Some(&path("a")), limit(1)).await,
        Err(StoreError::Stale { expected, actual }) if expected == version && actual == next.generation)
    );
    assert!(store.page(next, None, limit(1)).await.unwrap().is_empty());
}

#[tokio::test]
async fn scan_streams_bounded_pages_in_order_and_finishes_once() {
    let (_directory, store) = store().await;
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let expected = vec![
        directory(""),
        directory("a"),
        directory("a/b"),
        directory("c"),
        directory("d"),
    ];
    let version = store
        .replace(backend, &mut observation(expected.clone()))
        .await
        .unwrap();
    let mut reader = store.scan(version, limit(2));
    let mut actual = Vec::new();
    while let Some(entry) = reader.next_entry().await.unwrap() {
        actual.push(entry);
        assert!(reader.buffered.len() < 2);
    }
    assert_eq!(actual, expected);
    assert!(matches!(reader.state, ScanState::Complete));
    assert_eq!(reader.next_entry().await.unwrap(), None);
    let empty = store
        .replace(backend, &mut observation(vec![]))
        .await
        .unwrap();
    assert_eq!(
        store.scan(empty, limit(2)).next_entry().await.unwrap(),
        None
    );
}

#[tokio::test]
async fn scan_delivers_old_buffer_then_rejects_replacement_before_eof() {
    for page_size in [2, 3] {
        let (_directory, store) = store().await;
        let backend = store.register(&BackendId("source".into())).await.unwrap();
        let entries = vec![directory("a"), directory("b")];
        let version = store
            .replace(backend, &mut observation(entries.clone()))
            .await
            .unwrap();
        let mut reader = store.scan(version, limit(page_size));
        assert_eq!(reader.next_entry().await.unwrap(), Some(entries[0].clone()));
        let next = store
            .replace(backend, &mut observation(vec![directory("new")]))
            .await
            .unwrap();
        // The page is a valid observation of the old generation, not a live view.
        assert_eq!(reader.next_entry().await.unwrap(), Some(entries[1].clone()));
        assert!(matches!(reader.next_entry().await,
            Err(StoreError::Stale { expected, actual })
                if expected == version && actual == next.generation));
        assert!(matches!(
            reader.next_entry().await,
            Err(StoreError::ScanFailed)
        ));
        assert!(reader.buffered.is_empty());
    }
}

#[tokio::test]
async fn scan_pending_page_cancellation_keeps_cursor_and_buffer() {
    use std::future::Future;

    let (_directory, store) = store().await;
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let entries = vec![directory("a"), directory("b"), directory("c")];
    let version = store
        .replace(backend, &mut observation(entries.clone()))
        .await
        .unwrap();
    let mut reader = store.scan(version, limit(2));
    assert_eq!(reader.next_entry().await.unwrap(), Some(entries[0].clone()));
    // An unpolled call cannot consume the remaining buffered entry.
    drop(reader.next_entry());
    assert_eq!(reader.next_entry().await.unwrap(), Some(entries[1].clone()));
    let before = reader.after.clone();
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(store.pool.acquire().await.unwrap());
    }
    // No timer: exhausting the pool makes the next page acquisition pending.
    {
        let mut next = std::pin::pin!(reader.next_entry());
        std::future::poll_fn(|cx| {
            assert!(next.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    assert_eq!(reader.after, before);
    assert!(reader.buffered.is_empty());
    assert!(matches!(reader.state, ScanState::Reading));
    drop(connections);
    assert_eq!(reader.next_entry().await.unwrap(), Some(entries[2].clone()));
    assert_eq!(reader.next_entry().await.unwrap(), None);
}

#[test]
fn utf8_component_encoding_roundtrips_preserves_preorder_and_rejects_malformed_names() {
    let mut paths = vec![
        RelPath::root(),
        path("aa"),
        path("a/z"),
        path("a"),
        path("a/a"),
        path("a-"),
        path(r"\xff"),
    ];
    paths.sort();
    let encoded = paths.iter().map(encoding::encode_path).collect::<Vec<_>>();
    let mut ordered = encoded.clone();
    ordered.sort();
    assert_eq!(ordered, encoded);
    assert_eq!(
        encoded
            .iter()
            .map(|bytes| encoding::decode_path(bytes).unwrap())
            .collect::<Vec<_>>(),
        paths
    );
    for invalid in [
        b"truncated".as_slice(),
        b"\0",
        b"a\0\0",
        b".\0",
        b"..\0",
        b"a/b\0",
        b"\xff\0",
    ] {
        assert!(
            encoding::decode_path(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
    assert!(encoding::encode_target("a\0b").is_err());
    assert!(encoding::decode_target(b"a\0b".to_vec()).is_err());
    assert!(encoding::decode_target(vec![0xff]).is_err());
}

#[tokio::test]
async fn incompatible_encoding_is_rejected_on_open() {
    let (directory, store) = store().await;
    sqlx::query(queries::SET_FORMAT)
        .bind("other-platform-v1")
        .execute(&store.pool)
        .await
        .unwrap();
    assert!(
        matches!(VtreeStore::open(&directory.path().join("trees.sqlite")).await,
        Err(StoreError::Format(format)) if format == "other-platform-v1")
    );
}

#[tokio::test]
async fn query_twins_match_and_ordered_pages_seek_primary_key_without_sorting() {
    for (statement, explain) in [
        (queries::FORMAT, queries::EXPLAIN_FORMAT),
        (queries::REGISTER, queries::EXPLAIN_REGISTER),
        (queries::LOOKUP, queries::EXPLAIN_LOOKUP),
        (queries::GENERATION, queries::EXPLAIN_GENERATION),
        (queries::CLEAR, queries::EXPLAIN_CLEAR),
        (queries::BUMP, queries::EXPLAIN_BUMP),
        (queries::INSERT, queries::EXPLAIN_INSERT),
        (queries::PAGE_START, queries::EXPLAIN_PAGE_START),
        (queries::PAGE_AFTER, queries::EXPLAIN_PAGE_AFTER),
        (queries::SET_FORMAT, queries::EXPLAIN_SET_FORMAT),
    ] {
        assert_eq!(explain, format!("EXPLAIN QUERY PLAN\n{statement}"));
    }
    let (_directory, store) = store().await;
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let start = sqlx::query(queries::EXPLAIN_PAGE_START)
        .bind(backend.0)
        .bind(20)
        .fetch_all(&store.pool)
        .await
        .unwrap();
    let after = sqlx::query(queries::EXPLAIN_PAGE_AFTER)
        .bind(backend.0)
        .bind(encoding::encode_path(&path("a")))
        .bind(20)
        .fetch_all(&store.pool)
        .await
        .unwrap();
    for rows in [&start, &after] {
        let details = rows
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("SEARCH entry USING PRIMARY KEY")),
            "{details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("TEMP B-TREE") || detail.contains("SCAN entry")),
            "{details:?}"
        );
    }
    assert!(
        after
            .iter()
            .any(|row| row.get::<String, _>("detail").contains("path>?"))
    );
}

#[tokio::test]
async fn generic_keys_and_symlink_targets_are_literal_not_native_escape_syntax() {
    let (_location, store) = store().await;
    let owner = store.register(&BackendId("producer".into())).await.unwrap();
    let mut entries = vec![
        file(r"\q", "producer", None),
        file(r"\x00", "producer", None),
        file(r"\xFF", "producer", None),
        TreeEntry {
            path: path("symlink"),
            description: Description::Symlink {
                target: r"../\q/%FF".into(),
            },
        },
    ];
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let version = store
        .replace(owner, &mut observation(entries.clone()))
        .await
        .unwrap();
    assert_eq!(
        store
            .page(version, /*after*/ None, limit(10))
            .await
            .unwrap(),
        entries
    );
}
