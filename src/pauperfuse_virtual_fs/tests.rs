use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::path::Path;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use std::sync::Arc;

use pauperfuse::backends::{BackendId, OutputVersion, TreeEntry};

use super::*;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum FixtureError {
    #[error("observation interrupted")]
    Interrupted,
    #[error("wrong backend: {0:?}")]
    WrongBackend(BackendId),
    #[error("version unavailable: {0:?}")]
    Unavailable(OutputVersion),
    #[error("source read failed")]
    Read,
}

struct Tree(VecDeque<Result<TreeEntry, FixtureError>>);
impl BackendTree for Tree {
    type Error = FixtureError;
    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        self.0.pop_front().transpose()
    }
}
fn tree(entries: Vec<TreeEntry>) -> Tree {
    Tree(entries.into_iter().map(Ok).collect())
}
fn path(name: &str) -> RelPath {
    RelPath::parse(name).unwrap()
}
fn source(version: &str) -> Source {
    Source {
        backend: BackendId("source".into()),
        output: OutputVersion {
            output: b"note".to_vec(),
            version: version.as_bytes().to_vec(),
        },
    }
}
fn file(name: &str, version: &str, size: Option<u64>) -> TreeEntry {
    TreeEntry {
        path: path(name),
        description: Description::File {
            source: source(version),
            size,
        },
    }
}
fn mounted_entries(view: &VirtualFs) -> Vec<TreeEntry> {
    view.entries()
        .map(|(path, description)| TreeEntry {
            path: path.clone(),
            description: description.clone(),
        })
        .collect()
}
fn directory(name: &str) -> TreeEntry {
    TreeEntry {
        path: path(name),
        description: Description::Directory,
    }
}

#[derive(Default)]
struct Activity {
    opens: AtomicUsize,
    reads: AtomicUsize,
    fail_reads: AtomicBool,
}
#[derive(Default)]
struct Access {
    retained: Mutex<HashMap<OutputVersion, Arc<[u8]>>>,
    activity: Arc<Activity>,
}
impl Access {
    fn retain(&self, version: &str, bytes: &[u8]) {
        let replaced = self
            .retained
            .lock()
            .unwrap()
            .insert(source(version).output, Arc::from(bytes));
        assert!(replaced.is_none(), "version already retained");
    }
}
struct Reader {
    bytes: Arc<[u8]>,
    activity: Arc<Activity>,
}
impl ByteReader for Reader {
    type Error = FixtureError;
    async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        self.activity.reads.fetch_add(1, Ordering::Relaxed);
        if self
            .activity
            .fail_reads
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(FixtureError::Read);
        }
        let Ok(start) = usize::try_from(offset) else {
            return Ok(0);
        };
        let available = self.bytes.get(start..).unwrap_or_default();
        let count = buffer.len().min(available.len());
        buffer[..count].copy_from_slice(&available[..count]);
        Ok(count)
    }
}
impl ByteAccess for Access {
    type Error = FixtureError;
    type Reader = Reader;
    async fn open(&self, requested: &Source) -> Result<Self::Reader, Self::Error> {
        self.activity.opens.fetch_add(1, Ordering::Relaxed);
        if requested.backend != BackendId("source".into()) {
            return Err(FixtureError::WrongBackend(requested.backend.clone()));
        }
        let bytes = self
            .retained
            .lock()
            .unwrap()
            .get(&requested.output)
            .cloned()
            .ok_or_else(|| FixtureError::Unavailable(requested.output.clone()))?;
        Ok(Reader {
            bytes,
            activity: Arc::clone(&self.activity),
        })
    }
}

#[tokio::test]
async fn mounting_and_enumerating_metadata_never_opens_sources() {
    let access = Access::default();
    let entries = vec![
        directory(""),
        directory("notes"),
        file("notes/a", "one", None),
        TreeEntry {
            path: path("shortcut"),
            description: Description::Symlink {
                target: "notes/a".into(),
            },
        },
    ];
    let mut view = VirtualFs::default();
    view.mount(&mut tree(entries.clone())).await.unwrap();
    assert_eq!(mounted_entries(&view), entries);
    assert_eq!(
        view.metadata(&path("notes/a")),
        Some(&file("notes/a", "one", None).description)
    );
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (0, 0)
    );
    for name in ["", "notes", "shortcut"] {
        assert!(
            matches!(view.open(&path(name), &access).await, Err(OpenError::NotFile(found)) if found == path(name))
        );
    }
    assert!(
        matches!(view.open(&path("absent"), &access).await, Err(OpenError::NotFound(found)) if found == path("absent"))
    );
    assert_eq!(
        access
            .activity
            .opens
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn opening_resolves_once_and_reads_ranges_into_caller_buffer() {
    let access = Access::default();
    access.retain("one", b"abcdefgh");
    let mut view = VirtualFs::default();
    view.mount(&mut tree(vec![file("a", "one", Some(8))]))
        .await
        .unwrap();
    let mut handle = view.open(&path("a"), &access).await.unwrap();
    assert_eq!(handle.source(), &source("one"));
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (1, 0)
    );
    let mut buffer = [0xcc; 4];
    assert_eq!(handle.read_at(2, &mut buffer).await.unwrap(), 4);
    assert_eq!(buffer, *b"cdef");
    buffer.fill(0xcc);
    assert_eq!(handle.read_at(6, &mut buffer).await.unwrap(), 2);
    assert_eq!(buffer, [b'g', b'h', 0xcc, 0xcc]);
    buffer.fill(0xcc);
    for offset in [8, 9, u64::MAX] {
        assert_eq!(handle.read_at(offset, &mut buffer).await.unwrap(), 0);
        assert_eq!(buffer, [0xcc; 4]);
    }
    assert_eq!(handle.read_at(0, &mut []).await.unwrap(), 0);
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (1, 6)
    );
    access.activity.fail_reads.store(true, Ordering::Relaxed);
    assert_eq!(
        handle.read_at(0, &mut buffer).await.unwrap_err(),
        FixtureError::Read
    );
}

#[tokio::test]
async fn mounted_and_opened_sources_stay_at_their_version_after_replacement() {
    let access = Access::default();
    access.retain("one", b"old");
    let mut view = VirtualFs::default();
    view.mount(&mut tree(vec![file("a", "one", None)]))
        .await
        .unwrap();
    access.retain("two", b"new");
    let mut old = view.open(&path("a"), &access).await.unwrap();
    view.mount(&mut tree(vec![file("a", "two", None)]))
        .await
        .unwrap();
    let mut new = view.open(&path("a"), &access).await.unwrap();
    let mut buffer = [0; 3];
    assert_eq!(old.read_at(0, &mut buffer).await.unwrap(), 3);
    assert_eq!(buffer, *b"old");
    assert_eq!(new.read_at(0, &mut buffer).await.unwrap(), 3);
    assert_eq!(buffer, *b"new");
    assert_eq!(old.source(), &source("one"));
}

#[tokio::test]
async fn unavailable_and_wrong_backend_errors_keep_the_requested_reference() {
    let access = Access::default();
    access.retain("two", b"latest must not substitute");
    let mut view = VirtualFs::default();
    view.mount(&mut tree(vec![file("a", "one", None)]))
        .await
        .unwrap();
    match view.open(&path("a"), &access).await {
        Err(OpenError::Access {
            source: requested,
            cause,
        }) => {
            assert_eq!(requested, source("one"));
            assert_eq!(cause, FixtureError::Unavailable(requested.output));
        }
        _ => panic!("missing version must be an access error"),
    }
    let mut wrong = file("a", "one", None);
    let Description::File {
        source: requested, ..
    } = &mut wrong.description
    else {
        unreachable!()
    };
    requested.backend = BackendId("other".into());
    let wrong_source = requested.clone();
    view.mount(&mut tree(vec![wrong])).await.unwrap();
    match view.open(&path("a"), &access).await {
        Err(OpenError::Access {
            source: requested,
            cause,
        }) => {
            assert_eq!(requested, wrong_source);
            assert_eq!(cause, FixtureError::WrongBackend(wrong_source.backend));
        }
        _ => panic!("wrong backend must retain its typed error"),
    }
    assert_eq!(
        access
            .activity
            .reads
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn failed_or_cancelled_observation_preserves_the_entire_previous_mount() {
    let mut view = VirtualFs::default();
    let previous = vec![file("old", "one", None)];
    view.mount(&mut tree(previous.clone())).await.unwrap();
    let mut interrupted = Tree(VecDeque::from([
        Ok(directory("new")),
        Err(FixtureError::Interrupted),
    ]));
    assert!(matches!(
        view.mount(&mut interrupted).await,
        Err(MountError::Observation(FixtureError::Interrupted))
    ));
    assert_eq!(mounted_entries(&view), previous);

    struct PendingTree(bool);
    impl BackendTree for PendingTree {
        type Error = FixtureError;
        async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
            if !self.0 {
                self.0 = true;
                return Ok(Some(directory("new")));
            }
            std::future::pending().await
        }
    }
    {
        let mut tree = PendingTree(false);
        let mut mount = std::pin::pin!(view.mount(&mut tree));
        std::future::poll_fn(|cx| {
            assert!(mount.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    assert_eq!(mounted_entries(&view), previous);
}

#[cfg(unix)]
#[tokio::test]
async fn stored_tree_mount_opens_only_its_recorded_version_on_demand() {
    use pauperfuse::vtree::VtreeStore;
    use std::num::NonZeroU32;

    let location = tempfile::tempdir().unwrap();
    let store = VtreeStore::open(&location.path().join("trees.sqlite"))
        .await
        .unwrap();
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let entries = vec![
        directory(""),
        file("a", "one", None),
        file("b", "one", Some(3)),
    ];
    let version = store
        .replace(backend, &mut tree(entries.clone()))
        .await
        .unwrap();
    let access = Access::default();
    access.retain("one", b"old");
    access.retain("two", b"new");
    let mut view = VirtualFs::default();
    view.mount(&mut store.scan(version, NonZeroU32::new(1).unwrap()))
        .await
        .unwrap();
    assert_eq!(mounted_entries(&view), entries);
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (0, 0)
    );
    // A new stored observation does not mutate an already mounted namespace.
    store
        .replace(backend, &mut tree(vec![file("a", "two", Some(3))]))
        .await
        .unwrap();
    let mut handle = view.open(&path("a"), &access).await.unwrap();
    assert_eq!(handle.source(), &source("one"));
    let mut bytes = [0; 3];
    assert_eq!(handle.read_at(0, &mut bytes).await.unwrap(), 3);
    assert_eq!(bytes, *b"old");
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (1, 1)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn stored_native_paths_open_exact_sources_through_explicit_key_path_conversion() {
    use pauperfuse::backends::tokio_fs::TokioFs;
    use pauperfuse::vtree::VtreeStore;
    use std::num::NonZeroU32;
    use std::os::unix::ffi::OsStringExt;

    let location = tempfile::tempdir().unwrap();
    let store = VtreeStore::open(&location.path().join("trees.sqlite"))
        .await
        .unwrap();
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let cases = [
        (OsString::from_vec(vec![0xff]), "raw", r"names/\xff"),
        (OsString::from(r"\xff"), "literal", r"names/\\xff"),
        (OsString::from("café"), "unicode", "names/café"),
        (OsString::from("%FF"), "percent", "names/%FF"),
    ];
    let access = Access::default();
    let mut entries = vec![directory(""), directory("names")];
    for (name, version, _) in &cases {
        access.retain(version, version.as_bytes());
        entries.push(TreeEntry {
            path: TokioFs::from_native_path(&Path::new("names").join(name)).unwrap(),
            description: Description::File {
                source: source(version),
                size: None,
            },
        });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    let version = store
        .replace(backend, &mut tree(entries.clone()))
        .await
        .unwrap();
    let mut view = VirtualFs::default();
    view.mount(&mut store.scan(version, NonZeroU32::new(1).unwrap()))
        .await
        .unwrap();
    assert_eq!(mounted_entries(&view), entries);
    let keys = view
        .entries()
        .map(|(key, _)| key.to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        [
            "",
            "names",
            "names/%FF",
            r"names/\\xff",
            r"names/\xff",
            "names/café"
        ]
    );
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (0, 0)
    );

    // A newer stored source cannot change the source selected by this mount.
    access.retain("new", b"new bytes");
    store
        .replace(
            backend,
            &mut tree(vec![TreeEntry {
                path: RelPath::parse(r"names/\xff").unwrap(),
                description: Description::File {
                    source: source("new"),
                    size: None,
                },
            }]),
        )
        .await
        .unwrap();
    for (name, version, key) in &cases {
        let common = RelPath::parse(key).unwrap();
        assert_eq!(
            TokioFs::to_native_path(&common).unwrap(),
            Path::new("names").join(name)
        );
        assert_eq!(common.to_string(), *key);
        assert_eq!(
            view.metadata(&common),
            Some(&Description::File {
                source: source(version),
                size: None,
            })
        );
        let mut handle = view.open(&common, &access).await.unwrap();
        assert_eq!(handle.source(), &source(version));
        let mut bytes = [0xcc; 16];
        let count = handle.read_at(0, &mut bytes).await.unwrap();
        assert_eq!(count, version.len());
        assert_eq!(&bytes[..count], version.as_bytes());
        assert!(bytes[count..].iter().all(|byte| *byte == 0xcc));
    }
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (4, 4)
    );

    let checkout = location.path().join("checkout");
    tokio::fs::create_dir_all(checkout.join("names"))
        .await
        .unwrap();
    let receiver = TokioFs::new(&checkout);
    let puts = cases
        .iter()
        .map(|(_, _, key)| {
            let path = RelPath::parse(key).unwrap();
            let Description::File { source, .. } = view.metadata(&path).unwrap() else {
                unreachable!()
            };
            let source = source.clone();
            pauperfuse::backends::tokio_fs::FilePut {
                path,
                source,
                expected: pauperfuse::backends::tokio_fs::ExpectedFile::Absent,
            }
        })
        .collect();
    let mut prepared = receiver.prepare(puts, &access).await.unwrap();
    assert_eq!(
        std::fs::read_dir(checkout.join("names")).unwrap().count(),
        0
    );
    let installed = prepared.apply().await.unwrap();
    assert_eq!(installed.len(), cases.len());
    for (name, version, key) in &cases {
        assert_eq!(
            tokio::fs::read(checkout.join("names").join(name))
                .await
                .unwrap(),
            version.as_bytes()
        );
        let expected = receiver
            .observe(&RelPath::parse(key).unwrap())
            .await
            .unwrap();
        let actual = installed
            .iter()
            .find(|entry| entry.path == RelPath::parse(key).unwrap())
            .unwrap();
        assert_eq!(
            expected,
            pauperfuse::backends::tokio_fs::ExpectedFile::Present(actual.evidence.clone())
        );
    }
    prepared.cleanup().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn stale_stored_remount_preserves_previous_namespace_without_reading_bytes() {
    use pauperfuse::vtree::{Scan, StoreError, VtreeStore};
    use std::num::NonZeroU32;

    struct ReplacingTree {
        reader: Scan,
        store: VtreeStore,
        backend: pauperfuse::vtree::BackendKey,
        replace_after_first: bool,
    }
    impl BackendTree for ReplacingTree {
        type Error = StoreError;
        async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
            let entry = self.reader.next_entry().await?;
            if entry.is_some() && self.replace_after_first {
                self.store
                    .replace(
                        self.backend,
                        &mut tree(vec![file("replacement", "two", None)]),
                    )
                    .await
                    .unwrap();
                self.replace_after_first = false;
            }
            Ok(entry)
        }
    }

    let location = tempfile::tempdir().unwrap();
    let store = VtreeStore::open(&location.path().join("trees.sqlite"))
        .await
        .unwrap();
    let backend = store.register(&BackendId("source".into())).await.unwrap();
    let access = Access::default();
    let previous = vec![file("old", "one", None)];
    let mut view = VirtualFs::default();
    view.mount(&mut tree(previous.clone())).await.unwrap();
    for page_size in [1, 3] {
        let version = store
            .replace(
                backend,
                &mut tree(vec![file("a", "one", None), file("b", "one", None)]),
            )
            .await
            .unwrap();
        let mut changing = ReplacingTree {
            reader: store.scan(version, NonZeroU32::new(page_size).unwrap()),
            store: store.clone(),
            backend,
            replace_after_first: true,
        };
        // page_size=3 replaces the final buffered page: EOF must still validate.
        assert!(matches!(view.mount(&mut changing).await,
            Err(MountError::Observation(StoreError::Stale { expected, .. })) if expected == version));
        assert_eq!(mounted_entries(&view), previous);
    }
    assert_eq!(
        (
            access
                .activity
                .opens
                .load(std::sync::atomic::Ordering::Relaxed),
            access
                .activity
                .reads
                .load(std::sync::atomic::Ordering::Relaxed)
        ),
        (0, 0)
    );
}

#[tokio::test]
async fn duplicate_unordered_and_structurally_invalid_entries_do_not_replace_mount() {
    let mut view = VirtualFs::default();
    let previous = vec![file("old", "one", None)];
    view.mount(&mut tree(previous.clone())).await.unwrap();
    for entries in [
        vec![directory("a"), directory("a")],
        vec![directory("z"), directory("a")],
    ] {
        assert!(matches!(
            view.mount(&mut tree(entries)).await,
            Err(MountError::Order(_))
        ));
        assert_eq!(mounted_entries(&view), previous);
    }
    assert!(RelPath::root().join("../escape").is_err());
    assert!(matches!(
        view.mount(&mut tree(vec![file("", "one", None)])).await,
        Err(MountError::RootKind)
    ));
    assert!(
        matches!(view.mount(&mut tree(vec![file("a", "one", None), file("a/b", "one", None)])).await, Err(MountError::NotDirectory(found)) if found == path("a"))
    );
    assert_eq!(mounted_entries(&view), previous);
    view.mount(&mut tree(vec![])).await.unwrap();
    assert_eq!(view.entries().count(), 0);
}
