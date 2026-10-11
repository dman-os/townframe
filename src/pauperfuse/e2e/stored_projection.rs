use std::num::NonZeroU32;
use std::path::PathBuf;

use crate::backends::fixture::{FixtureError, VersionedProducer};
use crate::backends::tokio_fs::{
    ExpectedFile, FileError, FileEvidence, FilePut, InstalledFile, TokioFs,
};
use crate::backends::{AccessError, Description, Producer, ProducerAccess, Source, TreeEntry};
use crate::vtree::{BackendKey, StoreError, TreeVersion, VtreeStore};

struct Projection {
    _directory: tempfile::TempDir,
    root: PathBuf,
    store: VtreeStore,
    backend: BackendKey,
    producer: VersionedProducer,
    receiver: TokioFs,
}

impl Projection {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("checkout");
        tokio::fs::create_dir(&root).await.unwrap();
        let store = VtreeStore::open(&directory.path().join("trees.sqlite"))
            .await
            .unwrap();
        let producer = VersionedProducer::new();
        let backend = store.register(producer.id()).await.unwrap();
        Self {
            receiver: TokioFs::new(&root),
            _directory: directory,
            root,
            store,
            backend,
            producer,
        }
    }

    async fn record(&self) -> TreeVersion {
        let mut observed = self.producer.observe().await.unwrap();
        self.store
            .replace(self.backend, &mut observed)
            .await
            .unwrap()
    }

    async fn entries(&self, version: TreeVersion) -> Vec<TreeEntry> {
        self.store
            .page(version, None, NonZeroU32::new(10).unwrap())
            .await
            .unwrap()
    }

    fn assert_activity(&self, opens: usize, reads: usize) {
        assert_eq!(
            (
                self.producer
                    .activity
                    .opens
                    .load(std::sync::atomic::Ordering::Relaxed),
                self.producer
                    .activity
                    .reads
                    .load(std::sync::atomic::Ordering::Relaxed)
            ),
            (opens, reads),
        );
    }
}

fn source(entry: &TreeEntry) -> &Source {
    let Description::File { source, .. } = &entry.description else {
        panic!("fixture describes only files");
    };
    source
}

fn put(entry: &TreeEntry, expected: ExpectedFile) -> FilePut {
    FilePut {
        path: entry.path.clone(),
        source: source(entry).clone(),
        expected,
    }
}

fn installed(entry: &TreeEntry, bytes: &[u8]) -> InstalledFile {
    InstalledFile {
        path: entry.path.clone(),
        evidence: FileEvidence {
            length: bytes.len() as u64,
            digest: *blake3::hash(bytes).as_bytes(),
        },
    }
}

#[tokio::test]
async fn stored_description_projects_original_version_after_producer_changes() {
    let mut projection = Projection::new().await;
    let original = vec![0x9b; 65_537];
    projection
        .producer
        .publish("note", "output", "one", &original);
    let version = projection.record().await;
    let entries = projection.entries(version).await;
    projection.assert_activity(0, 0);
    assert_eq!(entries.len(), 1);
    assert!(matches!(
        entries[0].description,
        Description::File { size: None, .. }
    ));
    projection
        .producer
        .publish("note", "output", "two", b"new current bytes");

    tokio::fs::write(projection.root.join("note"), b"old checkout bytes")
        .await
        .unwrap();
    let expected = projection.receiver.observe(&entries[0].path).await.unwrap();
    let mut batch = projection
        .receiver
        .prepare(
            vec![put(&entries[0], expected)],
            &ProducerAccess::new(&projection.producer),
        )
        .await
        .unwrap();
    projection.assert_activity(1, 3); // Two data chunks, then EOF.
    assert_eq!(
        tokio::fs::read(projection.root.join("note")).await.unwrap(),
        b"old checkout bytes"
    );

    let actual = batch.apply().await.unwrap();
    assert_eq!(actual, vec![installed(&entries[0], &original)]);
    assert_eq!(batch.completed(), actual);
    assert_eq!(
        tokio::fs::read(projection.root.join("note")).await.unwrap(),
        original
    );
    assert_eq!(
        projection.receiver.observe(&entries[0].path).await.unwrap(),
        ExpectedFile::Present(actual[0].evidence.clone())
    );
    assert_eq!(
        projection.store.version(projection.backend).await.unwrap(),
        version
    );
    assert_eq!(projection.entries(version).await, entries);
    projection.assert_activity(1, 3); // Applying the prepared batch does not reopen the source.
    let staging = batch.staging_directory().to_owned();
    batch.cleanup().await.unwrap();
    assert!(!staging.exists());
}

#[tokio::test]
async fn evicted_stored_source_aborts_preparation_without_installing_any_file() {
    let mut projection = Projection::new().await;
    projection.producer.publish("a", "first", "one", b"first");
    projection.producer.publish("b", "second", "one", b"second");
    let version = projection.record().await;
    let entries = projection.entries(version).await;
    projection.assert_activity(0, 0);
    projection
        .producer
        .publish("b", "second", "two", b"replacement");
    projection.producer.evict(&source(&entries[1]).output);

    let result = projection
        .receiver
        .prepare(
            entries
                .iter()
                .map(|entry| put(entry, ExpectedFile::Absent))
                .collect(),
            &ProducerAccess::new(&projection.producer),
        )
        .await;
    let Err(failure) = result else {
        panic!("must not substitute current source version");
    };
    let FileError::Source {
        operation,
        path,
        source: error,
    } = *failure.cause
    else {
        panic!("expected source open failure");
    };
    assert_eq!((operation, path), ("open", entries[1].path.clone()));
    let cause = error.downcast_ref::<AccessError<FixtureError>>().unwrap();
    let AccessError::Producer(unavailable) = cause else {
        panic!("source backend matches");
    };
    assert_eq!(
        *unavailable,
        FixtureError::Unavailable(source(&entries[1]).output.clone())
    );
    assert!(failure.cleanup.is_none());
    assert!(failure.staging_directory.is_none());
    assert_eq!(std::fs::read_dir(&projection.root).unwrap().count(), 0);
    assert!(
        !std::fs::read_dir(projection.root.parent().unwrap())
            .unwrap()
            .any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .as_encoded_bytes()
                    .starts_with(b".pauperfuse-stage-")
            })
    );
    assert_eq!(
        projection.store.version(projection.backend).await.unwrap(),
        version
    );
    assert_eq!(projection.entries(version).await, entries);
    projection.assert_activity(2, 2); // Only the first source was read, including EOF.
}

#[tokio::test]
async fn prepared_bytes_survive_source_eviction_without_another_source_read() {
    let mut projection = Projection::new().await;
    projection
        .producer
        .publish("note", "output", "one", b"retained by staging");
    let version = projection.record().await;
    let entries = projection.entries(version).await;
    projection.assert_activity(0, 0);
    let mut batch = projection
        .receiver
        .prepare(
            vec![put(&entries[0], ExpectedFile::Absent)],
            &ProducerAccess::new(&projection.producer),
        )
        .await
        .unwrap();
    projection.assert_activity(1, 2);
    assert!(!projection.root.join("note").exists());
    projection.producer.evict(&source(&entries[0]).output);

    let actual = batch.apply().await.unwrap();
    assert_eq!(actual, vec![installed(&entries[0], b"retained by staging")]);
    assert_eq!(
        tokio::fs::read(projection.root.join("note")).await.unwrap(),
        b"retained by staging"
    );
    assert_eq!(
        projection.store.version(projection.backend).await.unwrap(),
        version
    );
    projection.assert_activity(1, 2);
    let staging = batch.staging_directory().to_owned();
    batch.cleanup().await.unwrap();
    assert!(!staging.exists());
}

#[tokio::test]
async fn replacing_observation_invalidates_cursor_without_invalidating_prepared_bytes() {
    let mut projection = Projection::new().await;
    projection.producer.publish("a", "first", "one", b"first");
    projection.producer.publish("b", "second", "one", b"second");
    let version = projection.record().await;
    let first_page = projection
        .store
        .page(version, None, NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    assert_eq!(first_page.len(), 1);
    projection.assert_activity(0, 0);
    let mut batch = projection
        .receiver
        .prepare(
            vec![put(&first_page[0], ExpectedFile::Absent)],
            &ProducerAccess::new(&projection.producer),
        )
        .await
        .unwrap();
    projection
        .producer
        .publish("a", "first", "two", b"new first");
    let replacement = projection.record().await;
    assert_eq!(replacement.generation, version.generation + 1);
    let result = projection
        .store
        .page(
            version,
            Some(&first_page[0].path),
            NonZeroU32::new(1).unwrap(),
        )
        .await;
    match result {
        Err(StoreError::Stale { expected, actual }) => {
            assert_eq!((expected, actual), (version, replacement.generation))
        }
        other => panic!("expected stale cursor, got {other:?}"),
    }
    assert_eq!(
        batch.apply().await.unwrap(),
        vec![installed(&first_page[0], b"first")]
    );
    assert_eq!(
        projection.store.version(projection.backend).await.unwrap(),
        replacement
    );
    assert_eq!(
        tokio::fs::read(projection.root.join("a")).await.unwrap(),
        b"first"
    );
    assert!(!projection.root.join("b").exists());
    projection.assert_activity(1, 2);
    let staging = batch.staging_directory().to_owned();
    batch.cleanup().await.unwrap();
    assert!(!staging.exists());
}
