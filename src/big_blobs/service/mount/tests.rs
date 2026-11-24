use super::*;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use futures::future::BoxFuture;
use service_client_native_rs::Client;
use service_client_native_rs::InlineRoute;
use service_client_native_rs::Transport;
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::fs::PermissionsExt as _;

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async { panic!("placing a blob never asks the authority") })
    }
}

/// A node's blob service over a store in a fresh directory, which is also where its mounts are.
async fn node(directory: &Path) -> (Blobs, StopToken) {
    let runtime = sqlite_rs::SqliteRuntime::new(Default::default());
    boot(Config {
        upload_hold_grace: None,
        storage: Storage::filesystem(directory.join("store")).await.unwrap(),
        sql: runtime.ephemeral(&[]).unwrap(),
        authority: Client::new(Transport::direct(Arc::new(UnusedAuthority))),
        namespace: Namespace {
            namespace_id: Uuid7::new(),
            capability: CapId::from_bytes([7; 16]),
        },
    })
    .await
    .unwrap()
}

async fn put(blobs: &Blobs, bytes: Vec<u8>) -> BlobInfo {
    blobs
        .service
        .put(ByteBody::from_bytes(Bytes::from(bytes)), MAX_PUT_OCTETS)
        .await
        .unwrap()
}

fn is_read_only(path: &Path) -> bool {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o222 == 0
}

/// A blob that is a file in the store is the same inode in the mount, so no bytes are copied and
/// the guest cannot write through its link; one that lives inline in the store's database (16 KiB
/// or less) has no file to link and is copied. Either way the guest reads the blob's bytes at
/// the name `materialize` answered, and placing again replaces whatever the guest put there.
#[tokio::test]
async fn a_stored_file_is_hard_linked_and_an_inline_blob_is_copied() {
    let directory = tempfile::tempdir().unwrap();
    let (blobs, stop) = node(directory.path()).await;
    let service = &blobs.service;
    let mount = service.working_mounts().create().await.unwrap();

    let large: Vec<u8> = (0..40_000).map(|index| (index % 251) as u8).collect();
    let large_info = put(&blobs, large.clone()).await;
    let small = b"fits in the store's database".to_vec();
    let small_info = put(&blobs, small.clone()).await;

    let large_name = file_name(&large_info.blob_id);
    service
        .place(&mount, &large_info.blob_id, &large_name)
        .await
        .unwrap();
    let linked = mount.dir().join(&large_name);
    let stored = directory
        .path()
        .join("store/data")
        .join(format!("{large_name}.data"));
    assert_eq!(std::fs::read(&linked).unwrap(), large);
    assert_eq!(
        std::fs::metadata(&linked).unwrap().ino(),
        std::fs::metadata(&stored).unwrap().ino(),
        "a complete blob file is linked, not copied"
    );
    assert_eq!(std::fs::metadata(&linked).unwrap().nlink(), 2);
    assert!(
        is_read_only(&linked),
        "the shared inode is read-only, so no write reaches the store"
    );
    assert!(
        std::fs::OpenOptions::new().write(true).open(&linked).is_err(),
        "a guest cannot open its link for writing"
    );

    let small_name = file_name(&small_info.blob_id);
    assert!(
        !directory
            .path()
            .join("store/data")
            .join(format!("{small_name}.data"))
            .exists(),
        "a blob this small is inline in the store's database"
    );
    service
        .place(&mount, &small_info.blob_id, &small_name)
        .await
        .unwrap();
    let copied = mount.dir().join(&small_name);
    assert_eq!(std::fs::read(&copied).unwrap(), small);
    assert_eq!(std::fs::metadata(&copied).unwrap().nlink(), 1);
    assert!(is_read_only(&copied));

    // The guest replaced the name with a file of its own: placing the blob again restores it.
    std::fs::remove_file(&copied).unwrap();
    std::fs::write(&copied, b"not the blob").unwrap();
    service
        .place(&mount, &small_info.blob_id, &small_name)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&copied).unwrap(), small);
    // No temporary name is left behind.
    assert_eq!(std::fs::read_dir(mount.dir()).unwrap().count(), 2);

    drop(blobs);
    stop.stop().await;
}

/// A mount on another filesystem than the store cannot hold a hard link, so it gets a copy of its
/// own (a reflink where the filesystem has one), and the store's file is left as it was.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_mount_on_another_filesystem_gets_a_copy() {
    let directory = tempfile::tempdir().unwrap();
    let elsewhere = tempfile::tempdir_in("/dev/shm").unwrap();
    assert_ne!(
        std::fs::metadata(directory.path()).unwrap().dev(),
        std::fs::metadata(elsewhere.path()).unwrap().dev(),
        "the test needs two filesystems"
    );
    let (blobs, stop) = node(directory.path()).await;
    let mount = WorkingMount {
        dir: elsewhere.path().to_owned(),
        closed: CancellationToken::new(),
        placed: Mutex::default(),
    };
    let bytes: Vec<u8> = (0..40_000).map(|index| (index % 249) as u8).collect();
    let info = put(&blobs, bytes.clone()).await;
    let name = file_name(&info.blob_id);
    blobs
        .service
        .place(&mount, &info.blob_id, &name)
        .await
        .unwrap();

    let copy = mount.dir().join(&name);
    assert_eq!(std::fs::read(&copy).unwrap(), bytes);
    let stored = directory
        .path()
        .join("store/data")
        .join(format!("{name}.data"));
    assert_ne!(
        std::fs::metadata(&copy).unwrap().ino(),
        std::fs::metadata(&stored).unwrap().ino()
    );
    assert_eq!(std::fs::metadata(&stored).unwrap().nlink(), 1);
    assert_eq!(std::fs::read(&stored).unwrap(), bytes);

    drop(blobs);
    stop.stop().await;
}

/// The mounts of an earlier run are removed when the next one opens its mounts, and a mount is
/// removed with everything in it.
#[tokio::test]
async fn leftover_mounts_are_reaped_at_open_and_a_mount_is_removed_whole() {
    let directory = tempfile::tempdir().unwrap();
    let stale = directory.path().join("mounts/mount-stale");
    std::fs::create_dir_all(stale.join("nested")).unwrap();
    std::fs::write(stale.join("nested/file"), b"left behind").unwrap();

    let mounts = WorkingMounts::open(Some(directory.path())).await.unwrap();
    assert_eq!(std::fs::read_dir(mounts.root()).unwrap().count(), 0);

    let mount = mounts.create().await.unwrap();
    assert!(mount.dir().starts_with(mounts.root()));
    std::fs::write(mount.dir().join("scratch"), b"guest output").unwrap();
    let reading = std::fs::File::open(mount.dir().join("scratch")).unwrap();
    mount.remove().await.unwrap();
    assert!(!mount.dir().exists());
    assert!(mount.closed.is_cancelled());
    // What the guest had open still reads.
    assert_eq!(std::io::read_to_string(reading).unwrap(), "guest output");
}

/// A handle's retraction unlinks what only it placed. A blob another live handle also placed
/// stays, and a file already open keeps reading.
#[tokio::test]
async fn revoking_a_handle_unlinks_only_what_no_other_handle_placed() {
    let directory = tempfile::tempdir().unwrap();
    let (blobs, stop) = node(directory.path()).await;
    let service = &blobs.service;
    let mount = service.working_mounts().create().await.unwrap();
    let (first, second) = (CapId::from_bytes([1; 16]), CapId::from_bytes([2; 16]));

    let both = put(&blobs, b"reached by both handles".to_vec()).await;
    let only_first = put(&blobs, b"reached by the first".to_vec()).await;
    for (info, handles) in [(&both, vec![first, second]), (&only_first, vec![first])] {
        for handle in handles {
            mount.track(info.blob_id, handle);
        }
        service
            .place(&mount, &info.blob_id, &file_name(&info.blob_id))
            .await
            .unwrap();
    }
    let kept = mount.dir().join(file_name(&both.blob_id));
    let gone = mount.dir().join(file_name(&only_first.blob_id));
    let open = std::fs::File::open(&gone).unwrap();

    mount.revoke(first).unwrap();
    assert!(kept.exists(), "the second handle still reaches it");
    assert_eq!(
        std::fs::File::open(&gone).unwrap_err().kind(),
        std::io::ErrorKind::NotFound,
        "a new open of what the first handle alone reached fails"
    );
    assert_eq!(
        std::io::read_to_string(open).unwrap(),
        "reached by the first",
        "a file already open keeps reading"
    );

    mount.revoke(second).unwrap();
    assert!(!kept.exists());
    // Revoking a handle that placed nothing, or twice, changes nothing.
    mount.revoke(second).unwrap();

    drop(blobs);
    stop.stop().await;
}

/// A blob the node does not hold yet is waited for, and the wait ends with the call's deadline.
#[tokio::test]
async fn a_blob_that_is_not_local_is_waited_for_until_it_arrives_or_the_call_expires() {
    let directory = tempfile::tempdir().unwrap();
    let (blobs, stop) = node(directory.path()).await;
    let service = Arc::clone(&blobs.service);
    let bytes = b"arrives after the call began".to_vec();
    let blob_id = BlobId::new(*iroh_blobs::Hash::new(&bytes).as_bytes());

    let mut expiring = RequestContext::root();
    expiring = expiring.with_time_budget(std::time::Duration::from_millis(50));
    assert_eq!(
        service.local(&expiring, &blob_id).await.unwrap_err(),
        BlobsError::Context(ContextError::DeadlineExceeded)
    );

    let waiting = tokio::spawn({
        let service = Arc::clone(&service);
        async move { service.local(&RequestContext::root(), &blob_id).await }
    });
    // Whether the put lands before or after the waiter looks, it is found: the waiter reads the
    // feed's position before its first look.
    let info = put(&blobs, bytes).await;
    assert_eq!(waiting.await.unwrap().unwrap(), info);

    drop(blobs);
    drop(service);
    stop.stop().await;
}
