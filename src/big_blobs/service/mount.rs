//! Working mounts and `materialize` (ADR 051 §2): the directory a workload reads blobs from as
//! ordinary files.
//!
//! A [`WorkingMount`] is one directory per workload, empty at start. The host preopens it into the
//! guest read-write and puts blobs in it on request ([`BlobService::materialize`]). A complete
//! blob that lives in the store as a file is hard-linked in, so no bytes are copied and the guest
//! reads the store's own inode. The store never opens that file for writing, and the mount makes
//! the shared inode read-only first, so a guest cannot write through its link either. A blob that
//! has no file of its own (inline in the store's database, which is every blob of 16 KiB or less,
//! or virtual) is copied to a temporary name and renamed into place, and a link that crosses
//! filesystems is replaced by a reflink or a copy. The mounts' root is under the blob store's
//! root, so by default every link is on one filesystem.
//!
//! Revocation acts per mount, immediately: the host follows each blobs handle that placed a file
//! and unlinks what only that handle reached when the handle is retracted. A file the guest has
//! open keeps reading (the inode lives until closed), and a later open fails as not found, which
//! is how a hard-linked file already behaves when its name goes away.

use super::*;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader as _;
use method_core::ContextError;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// The node's working mounts, all under one root that is emptied when the node starts.
pub struct WorkingMounts {
    root: PathBuf,
    /// A memory store has no directory to live under, so its mounts live in a scratch directory
    /// that is removed with the service.
    _scratch: Option<tempfile::TempDir>,
}

impl WorkingMounts {
    /// Opens `<store root>/mounts`, creating it and removing every mount a previous run left
    /// behind. The store's owner lock is held by the caller, so no live mount is removed.
    pub(super) async fn open(store_root: Option<&Path>) -> Res<Self> {
        let (root, scratch) = match store_root {
            Some(store_root) => (store_root.join("mounts"), None),
            None => {
                let scratch = tempfile::tempdir()?;
                (scratch.path().join("mounts"), Some(scratch))
            }
        };
        match tokio::fs::read_dir(&root).await {
            Ok(mut leftovers) => {
                while let Some(leftover) = leftovers.next_entry().await? {
                    tokio::fs::remove_dir_all(leftover.path()).await?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::create_dir_all(&root).await?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(Self {
            root,
            _scratch: scratch,
        })
    }

    /// The directory every mount is created in. A host that preopens a mount allows exactly this
    /// path.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// A new, empty mount for one workload.
    pub async fn create(&self) -> std::io::Result<Arc<WorkingMount>> {
        let dir = self.root.join(format!("mount-{:032x}", rand::random::<u128>()));
        tokio::fs::create_dir(&dir).await?;
        Ok(Arc::new(WorkingMount {
            dir,
            closed: CancellationToken::new(),
            placed: Mutex::default(),
        }))
    }
}

/// One workload's working mount.
pub struct WorkingMount {
    dir: PathBuf,
    /// Cancelled when the mount is removed, which ends the tasks following its handles.
    closed: CancellationToken,
    /// The blobs placed in the mount and the handles each was placed under. A name is unlinked
    /// when its last handle is retracted. Never locked across an await.
    placed: Mutex<Placed>,
}

#[derive(Default)]
struct Placed {
    by_blob: HashMap<BlobId, HashSet<CapId>>,
    /// The handles a task is already following in this mount.
    followed: HashSet<CapId>,
}

impl WorkingMount {
    /// The host path of the directory, which is what the guest's preopen is given.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Removes the directory and everything in it, and ends the tasks following handles placed in
    /// it. A file the guest still has open keeps reading until it is closed.
    pub async fn remove(&self) -> std::io::Result<()> {
        self.closed.cancel();
        tokio::fs::remove_dir_all(&self.dir).await
    }

    /// Records that `handle` placed `blob_id`, and whether a task should start following the
    /// handle (it is the first it placed in this mount). Recorded before the file exists, so a
    /// retraction that races the placement unlinks it rather than missing it.
    pub(super) fn track(&self, blob_id: BlobId, handle: CapId) -> bool {
        let mut placed = self.placed.lock().expect("working mount lock");
        placed.by_blob.entry(blob_id).or_default().insert(handle);
        placed.followed.insert(handle)
    }

    /// Takes back a [`Self::track`] whose placement failed.
    pub(super) fn untrack(&self, blob_id: &BlobId, handle: CapId) {
        let mut placed = self.placed.lock().expect("working mount lock");
        if let Some(handles) = placed.by_blob.get_mut(blob_id) {
            handles.remove(&handle);
            if handles.is_empty() {
                placed.by_blob.remove(blob_id);
            }
        }
    }

    /// Unlinks what only `handle` placed. Done under the lock the placements record under, so a
    /// concurrent placement of the same blob under another handle is never unlinked.
    pub(super) fn revoke(&self, handle: CapId) -> std::io::Result<()> {
        let mut placed = self.placed.lock().expect("working mount lock");
        let mut orphaned = Vec::new();
        placed.by_blob.retain(|blob_id, handles| {
            handles.remove(&handle);
            if handles.is_empty() {
                orphaned.push(*blob_id);
            }
            !handles.is_empty()
        });
        for blob_id in orphaned {
            match std::fs::remove_file(self.dir.join(file_name(&blob_id))) {
                // The guest removed it already, or the mount is gone.
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                other => other?,
            }
        }
        Ok(())
    }
}

/// What [`BlobService::materialize`] made readable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Materialized {
    pub info: BlobInfo,
    /// The file's name within the mount: the blob's digest in lowercase hex, so materializing a
    /// blob again names the same file.
    pub name: String,
}

pub(super) fn file_name(blob_id: &BlobId) -> String {
    data_encoding::HEXLOWER.encode(&blob_id.to_bytes32())
}

impl BlobService {
    /// Makes `blob_id` readable as a file in `mount`, fetching nothing the node does not already
    /// hold, and answers its name.
    ///
    /// The authority is the blobs handle `authorization.authority`, exactly as for a read: it must
    /// be live, bound to the presenting actor (`authorization.signer`) and reach the blob, and
    /// nothing else is consulted. A blob this node does not hold yet is waited for on the
    /// availability feed (the blob workers fetch what an inventory pins), until the call's
    /// deadline or cancellation, so the file is only ever whole.
    pub async fn materialize(
        self: &Arc<Self>,
        context: &RequestContext,
        authorization: Authorization,
        blob_id: BlobId,
        mount: &Arc<WorkingMount>,
    ) -> Result<Materialized, BlobsError> {
        let invocation = self
            .capabilities
            .source::<BlobsScope>(context, authorization)
            .await
            .map_err(authority_error)?;
        let DomainAuthority::Scoped(scope) = invocation.authority else {
            return Err(BlobsError::Denied);
        };
        self.authorize_read(&scope, &blob_id).await?;
        let info = self.local(context, &blob_id).await?;

        let handle = invocation.handle;
        let follow = mount.track(blob_id, handle);
        let name = file_name(&blob_id);
        if let Err(error) = self.place(mount, &blob_id, &name).await {
            mount.untrack(&blob_id, handle);
            return Err(storage_error(error));
        }
        if follow {
            self.follow(mount, handle);
        }
        Ok(Materialized { info, name })
    }

    /// The blob's info once the node holds it whole: now, or when the availability feed announces
    /// it. The feed's position is read before the first look, so an announcement between the two
    /// is replayed.
    pub(super) async fn local(&self, context: &RequestContext, blob_id: &BlobId) -> Result<BlobInfo, BlobsError> {
        let head = self
            .availability_revision()
            .await
            .map_err(storage_error)?;
        if let Some(info) = self.stat(blob_id).await.map_err(storage_error)? {
            return Ok(info);
        }
        let mut feed = self.open_availability(head).await.map_err(storage_error)?;
        loop {
            let read = tokio::select! {
                biased;
                () = context.cancellation.cancelled() => {
                    return Err(ContextError::Cancelled.into());
                }
                () = context.expired() => {
                    return Err(ContextError::DeadlineExceeded.into());
                }
                read = feed.next(RevisionReadLimits::default()) => read.map_err(storage_error)?,
            };
            let RevisionRead::Entries { entries, .. } = read else {
                continue;
            };
            if let Some(info) = entries
                .into_iter()
                .find_map(|entry| (entry.key == *blob_id).then_some(entry.value).flatten())
            {
                return Ok(info);
            }
        }
    }

    /// Writes `name` into the mount by linking the store's file or, failing that, copying, always
    /// through a temporary name in the mount and a rename, so the guest sees the whole file or
    /// none, and a name the guest had put there is replaced.
    pub(super) async fn place(
        &self,
        mount: &WorkingMount,
        blob_id: &BlobId,
        name: &str,
    ) -> std::io::Result<()> {
        let temporary = mount
            .dir
            .join(format!(".{name}.{:016x}.tmp", rand::random::<u64>()));
        let linked = match &self.root {
            Some(root) => {
                let stored = root.join("data").join(format!("{name}.data"));
                let temporary = temporary.clone();
                tokio::task::spawn_blocking(move || link_into(&stored, &temporary))
                    .await
                    .expect("link task panicked")?
            }
            None => false,
        };
        if !linked {
            let mut reader = self.store.blobs().reader(Hash::from(blob_id.to_bytes32()));
            let mut file = tokio::fs::File::create_new(&temporary).await?;
            tokio::io::copy(&mut reader, &mut file).await?;
            drop(file);
            read_only(&temporary).await?;
        }
        tokio::fs::rename(&temporary, mount.dir.join(name)).await
    }

    /// Follows `handle` for as long as the mount lives, unlinking what only it placed when it is
    /// retracted. The task outlives the call that placed the file, so it has a context of its
    /// own rather than the call's deadline.
    fn follow(self: &Arc<Self>, mount: &Arc<WorkingMount>, handle: CapId) {
        let capabilities = self.capabilities.clone();
        let mount = Arc::clone(mount);
        let stop = self.tasks.clone();
        self.tasks.spawn(async move {
            let context = RequestContext::root();
            let retracted = tokio::select! {
                () = stop.cancelled() => return,
                () = mount.closed.cancelled() => return,
                retracted = capabilities.retracted(&context, handle) => retracted,
            };
            retracted.expect("observing a blobs handle");
            mount
                .revoke(handle)
                .expect("unlinking the files of a retracted blobs handle");
        })
        .expect("a live blob service accepts tasks");
    }
}

/// Hard-links the store's file for a blob at `target`, answering whether the store has one: a
/// blob inline in the store's database has no file, and the caller copies it instead.
///
/// The link shares the store's inode, so the inode is made read-only first: the host never writes
/// to a store file, and a guest holding a link to it must not be able to either. (This does not
/// bind a process that ignores file permissions, such as root.)
fn link_into(stored: &Path, target: &Path) -> std::io::Result<bool> {
    let metadata = match std::fs::metadata(stored) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    let mut permissions = metadata.permissions();
    if !permissions.readonly() {
        set_readonly(&mut permissions);
        std::fs::set_permissions(stored, permissions)?;
    }
    match std::fs::hard_link(stored, target) {
        Ok(()) => Ok(true),
        // The mount is on another filesystem than the store.
        Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => {
            reflink_or_copy(stored, target)?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

/// A copy-on-write clone where the filesystem has one, else a plain copy. Only reads `source`.
fn reflink_or_copy(source: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use rustix::io::Errno;
        let original = std::fs::File::open(source)?;
        let clone = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)?;
        match rustix::fs::ioctl_ficlone(&clone, &original) {
            Ok(()) => return Ok(()),
            // The filesystems do not share extents, or this one cannot clone: copy instead.
            Err(Errno::XDEV | Errno::OPNOTSUPP | Errno::INVAL | Errno::NOSYS) => {
                drop(clone);
                std::fs::remove_file(target)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    std::fs::copy(source, target).map(drop)
}

#[cfg(unix)]
fn set_readonly(permissions: &mut std::fs::Permissions) {
    use std::os::unix::fs::PermissionsExt as _;
    permissions.set_mode(0o444);
}

#[cfg(not(unix))]
fn set_readonly(permissions: &mut std::fs::Permissions) {
    permissions.set_readonly(true);
}

async fn read_only(path: &Path) -> std::io::Result<()> {
    let mut permissions = tokio::fs::metadata(path).await?.permissions();
    set_readonly(&mut permissions);
    tokio::fs::set_permissions(path, permissions).await
}

#[cfg(test)]
mod tests;
