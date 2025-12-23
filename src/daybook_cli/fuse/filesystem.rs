use crate::interlude::*;

use daybook_core::drawer::DrawerRepo;
use daybook_types::doc::{Doc, DocPatch};
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, FUSE_ROOT_ID,
};
use libc::{EINVAL, EIO, ENOENT};
use parking_lot::RwLock;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::runtime::Handle;

use super::content::ContentManager;
use super::metadata::MetadataTable;
use super::sync::SyncTask;

const TTL: Duration = Duration::from_secs(1);

pub struct DaybookAsyncFS {
    metadata: Arc<RwLock<MetadataTable>>,
    content: Arc<RwLock<super::content::ContentCache>>,
    content_manager: Arc<ContentManager>,
    repo: Arc<DrawerRepo>,
    next_inode: AtomicU64,
    next_fh: AtomicU64,
    rt_handle: Handle,
    _sync_task: SyncTask,
}

impl DaybookAsyncFS {
    pub async fn new(repo: Arc<DrawerRepo>, rt_handle: Handle) -> Res<Self> {
        let metadata = Arc::new(RwLock::new(MetadataTable::new()));
        let content = Arc::new(RwLock::new(super::content::ContentCache::new()));
        let content_manager =
            Arc::new(ContentManager::new(Arc::clone(&content), Arc::clone(&repo)));

        let start_inode = FUSE_ROOT_ID + 1;
        let sync_task = SyncTask::new(
            Arc::clone(&metadata),
            Arc::clone(&content),
            Arc::clone(&repo),
            rt_handle.clone(),
            start_inode,
        );

        // Initialize metadata table
        sync_task.init().await?;

        Ok(Self {
            metadata,
            content,
            content_manager,
            repo,
            next_inode: AtomicU64::new(start_inode),
            next_fh: AtomicU64::new(1),
            rt_handle,
            _sync_task: sync_task,
        })
    }

    fn allocate_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::SeqCst)
    }
}

impl Filesystem for DaybookAsyncFS {
    fn init(&mut self, _req: &Request<'_>, _config: &mut KernelConfig) -> Result<(), libc::c_int> {
        tracing::info!("DaybookAsyncFS initialized");
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if parent != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }

        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let metadata = self.metadata.read();
        if let Some(file_metadata) = metadata.get_by_path(name_str) {
            reply.entry(&TTL, &file_metadata.attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == FUSE_ROOT_ID {
            // Root directory
            let root_attr = FileAttr {
                ino: FUSE_ROOT_ID,
                size: 0,
                blocks: 0,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
                blksize: 512,
            };
            reply.attr(&TTL, &root_attr);
            return;
        }

        let metadata = self.metadata.read();
        if let Some(file_metadata) = metadata.get_by_inode(ino) {
            reply.attr(&TTL, &file_metadata.attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let metadata = self.metadata.read();
        let file_metadata = match metadata.get_by_inode(ino) {
            Some(m) => m,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let doc_id = &file_metadata.doc_id;

        // Materialize content if needed (blocking call)
        let content = match self
            .rt_handle
            .block_on(self.content_manager.materialize_json(doc_id))
        {
            Ok(c) => c,
            Err(_) => {
                reply.error(ENOENT);
                return;
            }
        };

        let offset = offset as usize;
        let size = size as usize;
        let content_len = content.len();

        if offset >= content_len {
            reply.data(&[]);
            return;
        }

        let end = std::cmp::min(offset + size, content_len);
        reply.data(&content[offset..end]);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if ino != FUSE_ROOT_ID {
            reply.error(ENOENT);
            return;
        }

        let entries = vec![
            (FUSE_ROOT_ID, FileType::Directory, "."),
            (FUSE_ROOT_ID, FileType::Directory, ".."),
        ];

        let metadata = self.metadata.read();
        let mut all_entries: Vec<_> = entries.into_iter().collect();
        for file_metadata in metadata.list_all() {
            all_entries.push((
                file_metadata.inode,
                FileType::RegularFile,
                file_metadata.path.as_str(),
            ));
        }

        for (i, entry) in all_entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.0, (i + 1) as i64, entry.1, entry.2) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let metadata = self.metadata.read();
        let file_metadata = match metadata.get_by_inode(ino) {
            Some(m) => m,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let doc_id = &file_metadata.doc_id;

        let fh = self.allocate_fh();

        // Check if opening for write
        // O_RDONLY is 0, so we check for O_WRONLY (1) or O_RDWR (2)
        // Also check for O_TRUNC (0x200) which is often used with writes
        use libc::{O_RDWR, O_TRUNC, O_WRONLY};
        let is_write = (flags & (O_WRONLY | O_RDWR)) != 0 || (flags & O_TRUNC) != 0;

        if is_write {
            // Create snapshot for write (blocking call)
            match self
                .rt_handle
                .block_on(self.content_manager.create_snapshot(fh, doc_id))
            {
                Ok(_) => reply.opened(fh, 0),
                Err(_) => reply.error(ENOENT),
            }
        } else {
            // Materialize for read (if not already materialized) - blocking call
            match self
                .rt_handle
                .block_on(self.content_manager.materialize_json(doc_id))
            {
                Ok(_) => reply.opened(fh, 0),
                Err(_) => reply.error(ENOENT),
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        // Verify inode matches
        let metadata = self.metadata.read();
        let _file_metadata = match metadata.get_by_inode(ino) {
            Some(m) => m,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Update snapshot
        match self.content_manager.update_snapshot(fh, offset, data) {
            Ok(()) => reply.written(data.len() as u32),
            Err(_) => reply.error(EIO),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let metadata = self.metadata.read();
        let file_metadata = match metadata.get_by_inode(ino) {
            Some(m) => m,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let doc_id = &file_metadata.doc_id;

        // Check if this was a write handle
        if let Some(snapshot_content) = self.content_manager.get_snapshot_content(fh) {
            // Parse JSON and update doc
            let json_str = match String::from_utf8((*snapshot_content).clone()) {
                Ok(s) => s,
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };

            // Validate JSON
            let new_doc: Doc = match serde_json::from_str(&json_str) {
                Ok(d) => d,
                Err(_) => {
                    reply.error(EINVAL);
                    return;
                }
            };

            // Get current doc and apply update (blocking call)
            let result = self.rt_handle.block_on(async {
                let current_doc = self
                    .repo
                    .get(doc_id)
                    .await
                    .map_err(|_| EIO)?
                    .ok_or(ENOENT)?;

                let patch = DocPatch {
                    id: doc_id.clone(),
                    content: if current_doc.content != new_doc.content {
                        Some(new_doc.content)
                    } else {
                        None
                    },
                    props_remove: current_doc
                        .props
                        .keys()
                        .filter(|&key| !current_doc.props.contains_key(key))
                        .cloned()
                        .collect(),
                    props_set: if current_doc.props != new_doc.props {
                        new_doc.props.into_iter().map(Into::into).collect()
                    } else {
                        default()
                    },
                };

                // Check if patch is empty
                if !patch.is_empty() {
                    // Apply update
                    self.repo.update_batch(patch).await.map_err(|_| EIO)?;

                    // Invalidate content cache (event will update metadata)
                    self.content_manager.invalidate(doc_id);
                }

                Ok::<(), libc::c_int>(())
            });

            match result {
                Ok(()) => {
                    // Remove snapshot
                    self.content_manager.remove_snapshot(fh);
                    reply.ok();
                }
                Err(e) => reply.error(e),
            }
        } else {
            // Read handle, just close
            reply.ok();
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if ino == FUSE_ROOT_ID {
            reply.opened(0, 0);
        } else {
            reply.error(ENOENT);
        }
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // Flush is called on close, but we handle writes in release()
        // So we just acknowledge the flush
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Fsync requests data to be written to disk, but we handle writes in release()
        // So we just acknowledge the fsync
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        // Read-only filesystem for creation - files can only be modified, not created
        reply.error(libc::EROFS);
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        // Read-only filesystem for creation
        reply.error(libc::EROFS);
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Allow truncation (size change) for existing files, but not other attribute changes
        // Truncation is handled by write operations, so we just return current attr
        let metadata = self.metadata.read();
        if let Some(file_metadata) = metadata.get_by_inode(ino) {
            reply.attr(&TTL, &file_metadata.attr);
        } else {
            reply.error(ENOENT);
        }
    }
}

// Alias for compatibility
pub type DaybookAdapter = DaybookAsyncFS;
