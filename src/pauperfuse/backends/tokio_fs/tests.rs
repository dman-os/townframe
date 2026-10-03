use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use std::sync::Arc;

use crate::backends::{BackendId, OutputVersion};

use super::*;

struct Bytes {
    retained: HashMap<OutputVersion, Arc<[u8]>>,
    reads: Arc<AtomicUsize>,
}

struct Reader {
    data: Arc<[u8]>,
    reads: Arc<AtomicUsize>,
}

impl ByteReader for Reader {
    type Error = std::io::Error;

    async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let start = usize::try_from(offset).unwrap();
        let bytes = self.data.get(start..).unwrap_or_default();
        let count = bytes.len().min(buffer.len()).min(8192);
        buffer[..count].copy_from_slice(&bytes[..count]);
        Ok(count)
    }
}

impl ByteAccess for Bytes {
    type Error = std::io::Error;
    type Reader = Reader;

    async fn open(&self, source: &Source) -> Result<Self::Reader, Self::Error> {
        assert_eq!(source.backend, BackendId("fixture".into()));
        let data = self.retained.get(&source.output).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "exact source version unavailable",
            )
        })?;
        Ok(Reader {
            data: Arc::clone(data),
            reads: Arc::clone(&self.reads),
        })
    }
}

fn selector(name: &str) -> OutputVersion {
    OutputVersion {
        output: name.as_bytes().to_vec(),
        version: b"one".to_vec(),
    }
}
fn source(name: &str) -> Source {
    Source {
        backend: BackendId("fixture".into()),
        output: selector(name),
    }
}
fn path(name: &str) -> RelPath {
    RelPath::parse(name).unwrap()
}
fn put(name: &str, expected: ExpectedFile) -> FilePut {
    FilePut {
        path: path(name),
        source: source(name),
        expected,
    }
}
fn bytes(contents: &[(&str, &[u8])]) -> Bytes {
    Bytes {
        retained: contents
            .iter()
            .map(|(name, data)| (selector(name), Arc::from(*data)))
            .collect(),
        reads: Arc::default(),
    }
}
async fn checkout() -> (tempfile::TempDir, TokioFs) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("checkout");
    tokio::fs::create_dir(&root).await.unwrap();
    (directory, TokioFs::new(root))
}
fn expected_evidence(data: &[u8]) -> FileEvidence {
    FileEvidence {
        length: data.len() as u64,
        digest: *blake3::hash(data).as_bytes(),
    }
}

#[tokio::test]
async fn prepare_streams_exact_source_into_private_sibling_without_changing_target() {
    use std::os::unix::fs::PermissionsExt;

    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("a"), b"original")
        .await
        .unwrap();
    let expected = receiver.observe(&path("a")).await.unwrap();
    let data = vec![0x97; 180_000];
    let access = bytes(&[("a", &data)]);
    let mut batch = receiver
        .prepare(vec![put("a", expected)], &access)
        .await
        .unwrap();
    assert!(
        access.reads.load(std::sync::atomic::Ordering::Relaxed) > 2,
        "fixture must exercise multiple short reads"
    );
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        b"original"
    );
    assert!(!batch.staging_directory().starts_with(&receiver.root));
    assert_eq!(
        tokio::fs::metadata(batch.staging_directory())
            .await
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let installed = batch.apply().await.unwrap();
    assert_eq!(
        installed,
        vec![InstalledFile {
            path: path("a"),
            evidence: expected_evidence(&data)
        }]
    );
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        data
    );
    let staging = batch.staging_directory().to_owned();
    batch.cleanup().await.unwrap();
    assert!(!staging.exists());
}

#[tokio::test]
async fn second_source_failure_cleans_staging_and_publishes_nothing() {
    let (directory, receiver) = checkout().await;
    let access = bytes(&[("a", b"first")]);
    let result = receiver
        .prepare(
            vec![
                put("a", ExpectedFile::Absent),
                put("b", ExpectedFile::Absent),
            ],
            &access,
        )
        .await;
    let Err(failure) = result else {
        panic!("missing version must fail preparation");
    };
    assert!(matches!(
        *failure.cause,
        FileError::Source {
            operation: "open",
            ..
        }
    ));
    assert!(failure.cleanup.is_none());
    assert!(failure.staging_directory.is_none());
    assert!(!receiver.root.join("a").exists());
    assert!(!receiver.root.join("b").exists());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn occupied_unbound_target_is_rejected_before_opening_source() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("a"), b"mine")
        .await
        .unwrap();
    let access = bytes(&[("a", b"mine")]);
    let result = receiver
        .prepare(vec![put("a", ExpectedFile::Absent)], &access)
        .await;
    let Err(failure) = result else {
        panic!("unbound destination must be rejected");
    };
    assert!(matches!(*failure.cause, FileError::Changed(_)));
    assert_eq!(access.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        b"mine"
    );
}

#[tokio::test]
async fn changed_second_target_rejects_whole_batch_before_first_rename() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("b"), b"old")
        .await
        .unwrap();
    let expected = receiver.observe(&path("b")).await.unwrap();
    let access = bytes(&[("a", b"first"), ("b", b"new")]);
    let mut batch = receiver
        .prepare(
            vec![put("a", ExpectedFile::Absent), put("b", expected)],
            &access,
        )
        .await
        .unwrap();
    tokio::fs::write(receiver.root.join("b"), b"edited while preparing")
        .await
        .unwrap();
    let failure = batch.apply().await.unwrap_err();
    assert!(matches!(*failure.cause, FileError::Changed(_)));
    assert_eq!(failure.completed, Vec::<InstalledFile>::new());
    assert_eq!(failure.remaining, vec![path("a"), path("b")]);
    assert_eq!(failure.awaiting_verification, None);
    assert!(!receiver.root.join("a").exists());
    assert_eq!(
        tokio::fs::read(receiver.root.join("b")).await.unwrap(),
        b"edited while preparing"
    );
    batch.cleanup().await.unwrap();
}

#[tokio::test]
async fn application_failure_retains_completed_and_prepared_work_for_safe_resume() {
    let (_directory, receiver) = checkout().await;
    let access = bytes(&[("a", b"first"), ("b", b"second")]);
    let mut batch = receiver
        .prepare(
            vec![
                put("a", ExpectedFile::Absent),
                put("b", ExpectedFile::Absent),
            ],
            &access,
        )
        .await
        .unwrap();
    batch.injected_failure = Some(FailurePoint::BeforeRename(1));
    let failure = batch.apply().await.unwrap_err();
    batch.injected_failure = None;
    let first = InstalledFile {
        path: path("a"),
        evidence: expected_evidence(b"first"),
    };
    assert_eq!(failure.completed, vec![first.clone()]);
    assert_eq!(failure.remaining, vec![path("b")]);
    assert_eq!(batch.completed(), &[first]);
    assert!(!receiver.root.join("b").exists());
    assert!(batch.files[1].staged_path.exists());
    tokio::fs::write(receiver.root.join("a"), b"external edit")
        .await
        .unwrap();
    let failure = batch.apply().await.unwrap_err();
    assert!(matches!(*failure.cause, FileError::Changed(_)));
    assert_eq!(failure.completed.len(), 1);
    assert!(!receiver.root.join("b").exists());
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        b"external edit"
    );
    tokio::fs::write(receiver.root.join("a"), b"first")
        .await
        .unwrap();
    let completed = batch.apply().await.unwrap();
    assert_eq!(
        completed,
        vec![
            InstalledFile {
                path: path("a"),
                evidence: expected_evidence(b"first")
            },
            InstalledFile {
                path: path("b"),
                evidence: expected_evidence(b"second")
            },
        ]
    );
    batch.cleanup().await.unwrap();
}

#[tokio::test]
async fn duplicate_ancestor_and_unsafe_native_keys_fail_before_source_access() {
    let (_directory, receiver) = checkout().await;
    let access = bytes(&[]);
    for names in [["a", "a"], ["a/b", "a"]] {
        let result = receiver
            .prepare(
                names.map(|name| put(name, ExpectedFile::Absent)).to_vec(),
                &access,
            )
            .await;
        let Err(failure) = result else {
            panic!("conflicting paths must fail");
        };
        assert!(matches!(*failure.cause, FileError::Invalid { .. }));
    }
    let mut invalid = put("a", ExpectedFile::Absent);
    invalid.path = RelPath::parse(r"\x00").unwrap();
    let result = receiver.prepare(vec![invalid], &access).await;
    let Err(failure) = result else {
        panic!("unsafe native key must fail");
    };
    assert!(matches!(*failure.cause, FileError::Invalid { .. }));
    assert_eq!(access.reads.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[tokio::test]
async fn symlink_parent_and_destination_are_rejected() {
    use std::os::unix::fs::symlink;

    let (directory, receiver) = checkout().await;
    let outside = directory.path().join("outside");
    tokio::fs::create_dir(&outside).await.unwrap();
    tokio::fs::write(outside.join("a"), b"outside")
        .await
        .unwrap();
    symlink(&outside, receiver.root.join("linked")).unwrap();
    symlink(outside.join("a"), receiver.root.join("a")).unwrap();
    let access = bytes(&[]);
    for name in ["linked/a", "a"] {
        let result = receiver
            .prepare(vec![put(name, ExpectedFile::Absent)], &access)
            .await;
        let Err(failure) = result else {
            panic!("symlink traversal must fail");
        };
        assert!(matches!(*failure.cause, FileError::Invalid { .. }));
    }
    assert_eq!(
        tokio::fs::read(outside.join("a")).await.unwrap(),
        b"outside"
    );
}

#[tokio::test]
async fn failed_verification_reports_renamed_but_unacknowledged_path() {
    let (_directory, receiver) = checkout().await;
    let access = bytes(&[("a", b"first"), ("b", b"second")]);
    let mut batch = receiver
        .prepare(
            vec![
                put("a", ExpectedFile::Absent),
                put("b", ExpectedFile::Absent),
            ],
            &access,
        )
        .await
        .unwrap();
    batch.injected_failure = Some(FailurePoint::BeforeVerification(0));
    let failure = batch.apply().await.unwrap_err();
    batch.injected_failure = None;
    assert_eq!(failure.completed, Vec::<InstalledFile>::new());
    assert_eq!(failure.awaiting_verification, Some(path("a")));
    assert_eq!(failure.remaining, vec![path("b")]);
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        b"first"
    );
    tokio::fs::write(receiver.root.join("a"), b"edited before retry")
        .await
        .unwrap();
    let failure = batch.apply().await.unwrap_err();
    assert!(matches!(*failure.cause, FileError::Changed(_)));
    assert_eq!(failure.awaiting_verification, Some(path("a")));
    assert!(!receiver.root.join("b").exists());
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        b"edited before retry"
    );
    batch.cleanup().await.unwrap();
}

#[tokio::test]
async fn explicit_cleanup_reports_failure_and_can_be_retried() {
    let (_directory, receiver) = checkout().await;
    let access = bytes(&[("a", b"first")]);
    let mut batch = receiver
        .prepare(vec![put("a", ExpectedFile::Absent)], &access)
        .await
        .unwrap();
    let moved = batch.staging_directory().with_extension("moved");
    tokio::fs::rename(batch.staging_directory(), &moved)
        .await
        .unwrap();
    let failure = batch.cleanup().await.unwrap_err();
    assert!(matches!(
        failure,
        FileError::Io {
            operation: "remove staging directory",
            ..
        }
    ));
    assert!(!batch.cleaned);
    tokio::fs::rename(&moved, batch.staging_directory())
        .await
        .unwrap();
    batch.cleanup().await.unwrap();
    assert!(batch.cleaned);
    assert!(!receiver.root.join("a").exists());
}

// ---- collect: the ingest-side receiver (bytes flow FROM the checkout) ----

fn take(name: &str, expected: ExpectedFile) -> FileTake {
    FileTake {
        path: path(name),
        expected,
    }
}

#[tokio::test]
async fn collect_returns_bytes_and_evidence_from_the_same_read() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("a"), b"edited body").await.unwrap();
    let expected = receiver.observe(&path("a")).await.unwrap();

    let collected = receiver.collect(&[take("a", expected)], 1 << 20).await.unwrap();
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].path, path("a"));
    assert_eq!(collected[0].evidence, expected_evidence(b"edited body"));
    assert_eq!(collected[0].bytes, b"edited body".to_vec());

    // Destinations are never touched by collection.
    assert_eq!(
        tokio::fs::read(receiver.root.join("a")).await.unwrap(),
        b"edited body".to_vec()
    );
}

#[tokio::test]
async fn collect_rejects_changed_bytes_and_changed_targets() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("a"), b"first").await.unwrap();
    let stale = receiver.observe(&path("a")).await.unwrap();
    tokio::fs::write(receiver.root.join("a"), b"second").await.unwrap();

    let failure = receiver.collect(&[take("a", stale)], 1 << 20).await.unwrap_err();
    assert_eq!(failure.path, path("a"));
    assert!(matches!(failure.cause, FileError::Changed(_)));
}

#[tokio::test]
async fn collect_rechecks_every_path_after_the_whole_batch() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("a"), b"alpha").await.unwrap();
    tokio::fs::write(receiver.root.join("b"), b"beta").await.unwrap();
    let expected_a = receiver.observe(&path("a")).await.unwrap();
    let expected_b = receiver.observe(&path("b")).await.unwrap();

    // The batch itself matches, so the recheck passes...
    receiver
        .collect(&[take("a", expected_a.clone()), take("b", expected_b.clone())], 1 << 20)
        .await
        .unwrap();

    // ...and a change to an earlier take discovered after collection fails the batch.
    tokio::fs::write(receiver.root.join("a"), b"alpha changed").await.unwrap();
    let failure = receiver
        .collect(&[take("a", expected_a), take("b", expected_b)], 1 << 20)
        .await
        .unwrap_err();
    assert_eq!(failure.path, path("a"));
    assert!(matches!(failure.cause, FileError::Changed(_)));
}

#[tokio::test]
async fn collect_enforces_the_caller_supplied_byte_cap_without_reading() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("big"), vec![0u8; 4096]).await.unwrap();
    let expected = receiver.observe(&path("big")).await.unwrap();

    let failure = receiver.collect(&[take("big", expected)], 1024).await.unwrap_err();
    assert_eq!(failure.path, path("big"));
    assert!(
        failure.to_string().contains("capped at 1024"),
        "the cap failure must name the limit: {failure}"
    );
}

#[tokio::test]
async fn collect_refuses_absent_take_targets_non_regular_files_and_ancestors() {
    let (_directory, receiver) = checkout().await;
    tokio::fs::write(receiver.root.join("gone"), b"temp").await.unwrap();
    let expected = receiver.observe(&path("gone")).await.unwrap();
    tokio::fs::remove_file(receiver.root.join("gone")).await.unwrap();

    let failure = receiver.collect(&[take("gone", expected)], 1 << 20).await.unwrap_err();
    assert!(matches!(failure.cause, FileError::Changed(_)));

    tokio::fs::create_dir(receiver.root.join("d")).await.unwrap();
    let observed = receiver.observe(&path("d")).await.unwrap_err();
    assert!(matches!(observed, FileError::Invalid { .. }), "{observed}");

    let failure = receiver
        .collect(&[take("d", ExpectedFile::Absent)], 1 << 20)
        .await
        .unwrap_err();
    assert!(
        failure.to_string().contains("present files"),
        "absent takes must be refused, never mistaken for reads: {failure}"
    );

    let ancestor = receiver
        .collect(
            &[take("a", ExpectedFile::Absent), take("a/b", ExpectedFile::Absent)],
            1 << 20,
        )
        .await
        .unwrap_err();
    assert!(
        ancestor.to_string().contains("ancestor"),
        "ancestor targets must be refused: {ancestor}"
    );
}
