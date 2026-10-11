use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroU32;
use std::os::unix::ffi::OsStringExt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use pauperfuse::backends::RelPath;
use pauperfuse::backends::{
    BackendId, BackendTree, ByteAccess, ByteReader, Description, OutputVersion, Source, TreeEntry,
};
use pauperfuse::vtree::VtreeStore;
use pauperfuse_virtual_fs::VirtualFs;
use wasmtime::component::{Component, Linker, TypedFunc};
use wasmtime::{Config, Engine, Store};

use crate::{Filesystem, MAX_READ_BYTES, bindings::wasi::filesystem::types::ErrorCode};

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum FixtureError {
    #[error("exact version unavailable: {0:?}")]
    Unavailable(OutputVersion),
    #[error("interrupted source read")]
    Read,
}

#[derive(Default)]
struct Activity {
    opens: AtomicUsize,
    reads: AtomicUsize,
    live: AtomicUsize,
    fail_read: std::sync::atomic::AtomicBool,
    max_buffer: AtomicUsize,
}
#[derive(Default)]
struct Versions {
    current: BTreeMap<RelPath, (OutputVersion, Option<u64>)>,
    retained: HashMap<OutputVersion, Arc<[u8]>>,
}
#[derive(Clone, Default)]
struct Access {
    versions: Arc<Mutex<Versions>>,
    activity: Arc<Activity>,
}
impl Access {
    fn publish(&self, path: RelPath, version: &str, bytes: &[u8], size: Option<u64>) {
        let selected = OutputVersion {
            output: path.to_string().into_bytes(),
            version: version.as_bytes().to_vec(),
        };
        let mut versions = self.versions.lock().unwrap();
        assert!(
            versions
                .retained
                .insert(selected.clone(), Arc::from(bytes))
                .is_none()
        );
        versions.current.insert(path, (selected, size));
    }
    fn observe(&self) -> Tree {
        let versions = self.versions.lock().unwrap();
        let mut entries = vec![
            TreeEntry {
                path: RelPath::root(),
                description: Description::Directory,
            },
            TreeEntry {
                path: path("dir"),
                description: Description::Directory,
            },
        ];
        entries.extend(
            versions
                .current
                .iter()
                .map(|(path, (output, size))| TreeEntry {
                    path: path.clone(),
                    description: Description::File {
                        source: Source {
                            backend: BackendId("source".into()),
                            output: output.clone(),
                        },
                        size: *size,
                    },
                }),
        );
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Tree(entries.into())
    }
    fn assert_activity(&self, opens: usize, reads: usize, live: usize) {
        assert_eq!(
            (
                self.activity.opens.load(Ordering::Relaxed),
                self.activity.reads.load(Ordering::Relaxed),
                self.activity.live.load(Ordering::Relaxed)
            ),
            (opens, reads, live)
        );
    }
}
struct Tree(VecDeque<TreeEntry>);
impl BackendTree for Tree {
    type Error = FixtureError;
    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        Ok(self.0.pop_front())
    }
}
struct Reader {
    bytes: Arc<[u8]>,
    activity: Arc<Activity>,
}
impl Drop for Reader {
    fn drop(&mut self) {
        self.activity.live.fetch_sub(1, Ordering::Relaxed);
    }
}
impl ByteReader for Reader {
    type Error = FixtureError;
    async fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        self.activity.reads.fetch_add(1, Ordering::Relaxed);
        self.activity
            .max_buffer
            .fetch_max(buffer.len(), Ordering::Relaxed);
        if self.activity.fail_read.load(Ordering::Relaxed) {
            return Err(FixtureError::Read);
        }
        let available = usize::try_from(offset)
            .ok()
            .and_then(|offset| self.bytes.get(offset..))
            .unwrap_or_default();
        // Deliberate short reads, even when the selected version has more bytes.
        let count = available.len().min(buffer.len()).min(3);
        buffer[..count].copy_from_slice(&available[..count]);
        Ok(count)
    }
}
impl ByteAccess for Access {
    type Error = FixtureError;
    type Reader = Reader;
    async fn open(&self, source: &Source) -> Result<Self::Reader, Self::Error> {
        assert_eq!(source.backend, BackendId("source".into()));
        self.activity.opens.fetch_add(1, Ordering::Relaxed);
        let bytes = self
            .versions
            .lock()
            .unwrap()
            .retained
            .get(&source.output)
            .cloned()
            .ok_or_else(|| FixtureError::Unavailable(source.output.clone()))?;
        self.activity.live.fetch_add(1, Ordering::Relaxed);
        Ok(Reader {
            bytes,
            activity: Arc::clone(&self.activity),
        })
    }
}
fn path(key: &str) -> RelPath {
    RelPath::parse(key).unwrap()
}

// A real component with standard filesystem imports. The allocator belongs to
// the guest; canonical lowering writes into its bounded linear memory.
const GUEST: &str = r#"
(component
  (type $filesystem (instance
    (export "descriptor" (type $descriptor (sub resource)))
    (type $path-flags-definition (flags "symlink-follow"))
    (export "path-flags" (type $path-flags (eq $path-flags-definition)))
    (type $open-flags-definition (flags "create" "directory" "exclusive" "truncate"))
    (export "open-flags" (type $open-flags (eq $open-flags-definition)))
    (type $descriptor-flags-definition (flags "read" "write" "file-integrity-sync" "data-integrity-sync" "requested-write-sync" "mutate-directory"))
    (export "descriptor-flags" (type $descriptor-flags (eq $descriptor-flags-definition)))
    (type $error-definition (enum "access" "would-block" "already" "bad-descriptor" "busy" "deadlock" "quota" "exist" "file-too-large" "illegal-byte-sequence" "in-progress" "interrupted" "invalid" "io" "is-directory" "loop" "too-many-links" "message-size" "name-too-long" "no-device" "no-entry" "no-lock" "insufficient-memory" "insufficient-space" "not-directory" "not-empty" "not-recoverable" "unsupported" "no-tty" "no-such-device" "overflow" "not-permitted" "pipe" "read-only" "invalid-seek" "text-file-busy" "cross-device"))
    (export "error-code" (type $error (eq $error-definition)))
    (type $descriptor-type-definition (enum "unknown" "block-device" "character-device" "directory" "fifo" "symbolic-link" "regular-file" "socket"))
    (export "descriptor-type" (type $descriptor-type (eq $descriptor-type-definition)))
    (type $datetime-definition (record (field "seconds" u64) (field "nanoseconds" u32)))
    (export "datetime" (type $datetime (eq $datetime-definition)))
    (type $stat-definition (record (field "type" $descriptor-type) (field "link-count" u64) (field "size" u64) (field "data-access-timestamp" (option $datetime)) (field "data-modification-timestamp" (option $datetime)) (field "status-change-timestamp" (option $datetime))))
    (export "descriptor-stat" (type $stat (eq $stat-definition)))
    (export "[method]descriptor.stat-at" (func (param "self" (borrow $descriptor)) (param "path-flags" $path-flags) (param "path" string) (result (result $stat (error $error)))))
    (export "[method]descriptor.open-at" (func (param "self" (borrow $descriptor)) (param "path-flags" $path-flags) (param "path" string) (param "open-flags" $open-flags) (param "flags" $descriptor-flags) (result (result (own $descriptor) (error $error)))))
    (export "[method]descriptor.read" (func (param "self" (borrow $descriptor)) (param "length" u64) (param "offset" u64) (result (result (tuple (list u8) bool) (error $error)))))
  ))
  (import "wasi:filesystem/types@0.2.6" (instance $fs (type $filesystem)))
  (alias export $fs "descriptor" (type $descriptor))
  (alias export $fs "error-code" (type $error))
  (alias export $fs "[method]descriptor.open-at" (func $open))
  (alias export $fs "[method]descriptor.read" (func $read))
  (alias export $fs "[method]descriptor.stat-at" (func $stat))
  (import "wasi:filesystem/preopens@0.2.6" (instance $preopens
    (export "descriptor" (type $pre-descriptor (eq $descriptor)))
    (export "get-directories" (func (result (list (tuple (own $pre-descriptor) string)))))
  ))
  (alias export $preopens "get-directories" (func $directories))
  (core module $allocator
    (memory (export "memory") 16 16)
    (global $next (mut i32) (i32.const 1024))
    (func (export "realloc") (param $old i32) (param $old-size i32) (param $align i32) (param $size i32) (result i32)
      (local $ptr i32)
      (if (i32.eqz (local.get $size)) (then (return (i32.const 0))))
      (local.set $ptr (i32.and (i32.add (global.get $next) (i32.sub (local.get $align) (i32.const 1))) (i32.sub (i32.const 0) (local.get $align))))
      (global.set $next (i32.add (local.get $ptr) (local.get $size)))
      (if (i32.gt_u (global.get $next) (i32.const 1048576)) (then unreachable))
      (if (local.get $old) (then (memory.copy (local.get $ptr) (local.get $old) (local.get $old-size))))
      (local.get $ptr))
  )
  (core instance $allocator (instantiate $allocator))
  (alias core export $allocator "memory" (core memory $memory))
  (alias core export $allocator "realloc" (core func $realloc))
  (core func $directories (canon lower (func $directories) (memory $memory) (realloc $realloc)))
  (core func $open (canon lower (func $open) (memory $memory) (realloc $realloc)))
  (core func $read (canon lower (func $read) (memory $memory) (realloc $realloc)))
  (core func $stat (canon lower (func $stat) (memory $memory) (realloc $realloc)))
  (core func $drop (canon resource.drop $descriptor))
  (core module $probe
    (import "host" "memory" (memory 16 16))
    (import "host" "directories" (func $directories (param i32)))
    (import "host" "open" (func $open (param i32 i32 i32 i32 i32 i32 i32)))
    (import "host" "read" (func $read (param i32 i64 i64 i32)))
    (import "host" "drop" (func $drop (param i32)))
    (import "host" "stat" (func $stat (param i32 i32 i32 i32 i32)))
    (global $root (mut i32) (i32.const -1))
    (func (export "preopen") (result i32)
      (call $directories (i32.const 0))
      (global.set $root (i32.load (i32.load (i32.const 0))))
      (i32.load (i32.const 4)))
    (func (export "finish") (call $drop (global.get $root)))
    (func (export "size") (param $path i32) (param $path-size i32) (result i32)
      (call $stat (global.get $root) (i32.const 0) (local.get $path) (local.get $path-size) (i32.const 256))
      (if (i32.load8_u (i32.const 256))
        (then
          (i32.store8 (i32.const 384) (i32.const 1))
          (i32.store8 (i32.const 392) (i32.load8_u (i32.const 264))))
        (else
          (i32.store8 (i32.const 384) (i32.const 0))
          (i64.store (i32.const 392) (i64.load (i32.const 280)))))
      (i32.const 384))
    (func (export "probe") (param $path i32) (param $path-size i32) (param $offset i64) (param $length i64) (param $open-flags i32) (param $flags i32) (result i32)
      (local $file i32)
      (call $open (global.get $root) (i32.const 0) (local.get $path) (local.get $path-size) (local.get $open-flags) (local.get $flags) (i32.const 64))
      (if (i32.load8_u (i32.const 64))
        (then
          (i32.store8 (i32.const 96) (i32.const 1))
          (i32.store8 (i32.const 100) (i32.load8_u (i32.const 68))))
        (else
          (local.set $file (i32.load (i32.const 68)))
          (call $read (local.get $file) (local.get $length) (local.get $offset) (i32.const 96))
          (call $drop (local.get $file))))
      (i32.const 96))
  )
  (core instance $host
    (export "memory" (memory $memory))
    (export "directories" (func $directories))
    (export "open" (func $open))
    (export "read" (func $read))
    (export "drop" (func $drop))
    (export "stat" (func $stat)))
  (core instance $probe (instantiate $probe (with "host" (instance $host))))
  (func (export "preopen") (result u32) (canon lift (core func $probe "preopen")))
  (func (export "finish") (canon lift (core func $probe "finish")))
  (func (export "size") (param "path" string) (result (result u64 (error $error)))
    (canon lift (core func $probe "size") (memory $memory) (realloc $realloc)))
  (func (export "probe") (param "path" string) (param "offset" u64) (param "length" u64) (param "open-flags" u32) (param "flags" u32)
    (result (result (tuple (list u8) bool) (error $error)))
    (canon lift (core func $probe "probe") (memory $memory) (realloc $realloc)))
)
"#;

type Probe = TypedFunc<(String, u64, u64, u32, u32), (Result<(Vec<u8>, bool), ErrorCode>,)>;
struct Guest {
    _directory: tempfile::TempDir,
    store: Store<Filesystem<Access>>,
    probe: Probe,
    finish: TypedFunc<(), ()>,
    size: TypedFunc<(String,), (Result<u64, ErrorCode>,)>,
    access: Access,
}
impl Guest {
    async fn new(access: Access) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let metadata = VtreeStore::open(&directory.path().join("trees.sqlite"))
            .await
            .unwrap();
        let backend = metadata
            .register(&BackendId("source".into()))
            .await
            .unwrap();
        let version = metadata
            .replace(backend, &mut access.observe())
            .await
            .unwrap();
        let mut view = VirtualFs::default();
        view.mount(&mut metadata.scan(version, NonZeroU32::new(1).unwrap()))
            .await
            .unwrap();
        access.assert_activity(0, 0, 0);
        let config = Config::new();
        let engine = Engine::new(&config).unwrap();
        let component = Component::new(&engine, GUEST).unwrap();
        let mut linker = Linker::new(&engine);
        Filesystem::add_to_linker(&mut linker).unwrap();
        let mut store = Store::new(&engine, Filesystem::new(view, access.clone()));
        let instance = linker
            .instantiate_async(&mut store, &component)
            .await
            .unwrap();
        access.assert_activity(0, 0, 0);
        let preopen = instance
            .get_typed_func::<(), (u32,)>(&mut store, "preopen")
            .unwrap();
        assert_eq!(preopen.call_async(&mut store, ()).await.unwrap(), (1,));

        access.assert_activity(0, 0, 0);
        assert_eq!(store.data().live_descriptors(), 1);
        let probe = instance.get_typed_func(&mut store, "probe").unwrap();
        let finish = instance.get_typed_func(&mut store, "finish").unwrap();
        let size = instance.get_typed_func(&mut store, "size").unwrap();
        Self {
            _directory: directory,
            store,
            probe,
            finish,
            size,
            access,
        }
    }
    async fn request(
        &mut self,
        path: &str,
        offset: u64,
        length: u64,
        open_flags: u32,
        flags: u32,
    ) -> Result<(Vec<u8>, bool), ErrorCode> {
        let (result,) = self
            .probe
            .call_async(
                &mut self.store,
                (path.to_owned(), offset, length, open_flags, flags),
            )
            .await
            .unwrap();

        assert_eq!(
            self.store.data().live_descriptors(),
            1,
            "guest drops every opened reader"
        );
        assert_eq!(self.access.activity.live.load(Ordering::Relaxed), 0);
        result
    }
    async fn size(&mut self, path: &str) -> Result<u64, ErrorCode> {
        self.size
            .call_async(&mut self.store, (path.to_owned(),))
            .await
            .unwrap()
            .0
    }
    async fn finish(mut self) {
        self.finish.call_async(&mut self.store, ()).await.unwrap();

        assert_eq!(self.store.data().live_descriptors(), 0);
    }
}

#[tokio::test]
async fn real_guest_reads_recorded_version_lazily_and_drops_resources() {
    let access = Access::default();
    access.publish(path("dir/note"), "one", b"abcdefgh", Some(8));
    let mut guest = Guest::new(access.clone()).await;
    access.publish(path("dir/note"), "two", b"new current bytes", Some(17));
    assert_eq!(guest.size("dir/note").await, Ok(8));
    access.assert_activity(0, 0, 0);
    assert_eq!(
        guest.request("dir/note", 2, 4, 0, 1).await.unwrap(),
        (b"cde".to_vec(), false)
    );
    assert_eq!(
        guest.request("dir/note", 6, 4, 0, 1).await.unwrap(),
        (b"gh".to_vec(), true)
    );
    assert_eq!(
        guest.request("dir/note", 8, 4, 0, 1).await.unwrap(),
        (vec![], true)
    );
    assert_eq!(
        guest.request("dir/note", 0, 0, 0, 1).await.unwrap(),
        (vec![], false)
    );
    access.assert_activity(4, 4, 0);
    guest.finish().await;
}

#[tokio::test]
async fn real_guest_missing_version_is_io_not_latest_or_empty_success() {
    let access = Access::default();
    access.publish(path("dir/note"), "one", b"old", Some(3));
    let mut guest = Guest::new(access.clone()).await;
    access.publish(path("dir/note"), "two", b"latest", Some(6));
    let old = OutputVersion {
        output: b"dir/note".to_vec(),
        version: b"one".to_vec(),
    };
    assert!(
        access
            .versions
            .lock()
            .unwrap()
            .retained
            .remove(&old)
            .is_some()
    );
    assert_eq!(
        guest.request("dir/note", 0, 3, 0, 1).await,
        Err(ErrorCode::Io)
    );
    let failure = guest.store.data_mut().take_source_failure().unwrap();
    assert_eq!(
        failure.source,
        Source {
            backend: BackendId("source".into()),
            output: old.clone()
        }
    );
    let cause = failure.cause.downcast_ref::<FixtureError>().unwrap();
    assert_eq!(cause, &FixtureError::Unavailable(old));
    access.assert_activity(1, 0, 0);
    guest.finish().await;
}

#[tokio::test]
async fn real_guest_native_names_are_reversible_and_traversal_and_writes_are_denied() {
    let access = Access::default();
    let raw = pauperfuse::backends::tokio_fs::TokioFs::from_native_path(
        &std::path::Path::new("dir").join(std::ffi::OsString::from_vec(vec![0xff])),
    )
    .unwrap();
    access.publish(raw, "raw", b"raw", Some(3));
    access.publish(path(r"dir/\\xff"), "literal", b"lit", Some(3));
    access.publish(path(r"dir/\x00"), "opaque-key", b"key", Some(3));
    let mut guest = Guest::new(access.clone()).await;
    assert_eq!(
        guest.request(r"dir/\xff", 0, 3, 0, 1).await.unwrap(),
        (b"raw".to_vec(), true)
    );
    assert_eq!(
        guest.request(r"dir/\\xff", 0, 3, 0, 1).await.unwrap(),
        (b"lit".to_vec(), true)
    );
    for path in ["../outside", "dir/../../outside", "/outside"] {
        assert_eq!(
            guest.request(path, 0, 3, 0, 1).await,
            Err(ErrorCode::NotPermitted),
            "{path}"
        );
    }
    assert_eq!(
        guest.request("absent/note", 0, 3, 0, 1).await,
        Err(ErrorCode::NotDirectory)
    );
    assert_eq!(
        guest.request("dir/missing", 0, 3, 0, 1).await,
        Err(ErrorCode::NoEntry)
    );
    assert_eq!(
        guest.request(r"dir/\xff", 0, 3, 0, 2).await,
        Err(ErrorCode::ReadOnly)
    );
    assert_eq!(
        guest.request(r"dir/\xff", 0, 3, 1, 1).await,
        Err(ErrorCode::ReadOnly)
    );
    assert_eq!(
        guest.request("dir", 0, 3, 2, 1).await,
        Err(ErrorCode::IsDirectory)
    );
    assert_eq!(
        guest.request(r"dir/\xff", 0, 3, 2, 1).await,
        Err(ErrorCode::NotDirectory)
    );
    assert_eq!(
        guest.request(r"dir/\xff/child", 0, 3, 0, 1).await,
        Err(ErrorCode::NotDirectory)
    );
    assert_eq!(
        guest.request(r"dir/\xff", 0, 3, 0, 4).await,
        Err(ErrorCode::Unsupported)
    );
    for key in [r"dir/\x2foutside", r"dir/\q"] {
        assert_eq!(
            guest.request(key, 0, 3, 0, 1).await,
            Err(ErrorCode::NoEntry)
        );
    }
    assert_eq!(
        guest.request(r"dir/\x00", 0, 3, 0, 1).await.unwrap(),
        (b"key".to_vec(), true)
    );
    access.assert_activity(3, 3, 0);
    guest.finish().await;
}

#[tokio::test]
async fn real_guest_huge_reads_are_bounded_and_read_failures_preserve_source() {
    let access = Access::default();
    access.publish(
        path("dir/note"),
        "one",
        &vec![0xab; MAX_READ_BYTES + 1],
        None,
    );
    let mut guest = Guest::new(access.clone()).await;
    assert_eq!(guest.size("dir/note").await, Err(ErrorCode::Unsupported));
    access.assert_activity(0, 0, 0);
    assert_eq!(
        guest.request("dir/note", 0, u64::MAX, 0, 1).await.unwrap(),
        (vec![0xab; 3], false)
    );
    assert_eq!(
        access.activity.max_buffer.load(Ordering::Relaxed),
        MAX_READ_BYTES
    );
    assert_eq!(
        guest.request("dir/note", u64::MAX, 3, 0, 1).await.unwrap(),
        (vec![], true)
    );
    access.activity.fail_read.store(true, Ordering::Relaxed);
    assert_eq!(
        guest.request("dir/note", 0, 3, 0, 1).await,
        Err(ErrorCode::Io)
    );
    let failure = guest.store.data_mut().take_source_failure().unwrap();
    assert_eq!(failure.source.output.version, b"one");
    assert_eq!(
        failure.cause.downcast_ref::<FixtureError>(),
        Some(&FixtureError::Read)
    );
    access.assert_activity(3, 3, 0);
    guest.finish().await;
}
