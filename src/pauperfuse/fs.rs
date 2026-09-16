//! The filesystem backend: the one that decides whether this design works.
//!
//! Three properties drive the implementation, all of them from ADR 010:
//!
//! - **a scan reads names and stats, not bytes** (§3.2). Small files are hashed,
//!   because a content hash is what lets two backends agree that they hold the
//!   same bytes; large files are not, because hashing four gigabytes to notice
//!   that an mtime moved is the failure this design rejects. A large file's
//!   identity is then a *stat-derived token* until something actually needs its
//!   bytes ([`Backend::verify`]).
//! - **identity is confirmed, never recomputed** (§2.3). When a scan finds a
//!   recorded entry whose bytes are unchanged, the recorded provenance stays:
//!   the file the bridge materialized from a doc keeps its render recipe, so
//!   the doc side and the disk side still agree on what they are looking at.
//! - **materialization is atomic per file, and usually free** (§2.4/§2.5):
//!   scratch file plus rename for a write, hard link for content that already
//!   exists on this machine.

use crate::backend::Accepted;
use crate::backend::{Backend, BackendId, Capabilities, Report};
use crate::delta::Delta;
use crate::entry::{Avail, Entry, Payload, StatFingerprint, TimeStamp, Token};
use crate::interlude::*;
use crate::path::RelPath;

/// Files larger than this are not hashed during a scan.
const DEFAULT_HASH_LIMIT: u64 = 1 << 20;

/// Directory names a checkout never descends into: its own metadata.
const DEFAULT_IGNORES: [&str; 2] = [".dtree", ".dnode"];

/// How far a walk may run ahead of the session consuming it.
///
/// Deep enough that `stat` and hash work overlaps the store's writes, shallow
/// enough that a fast walk never outruns the store by a whole tree.
const WALK_AHEAD: usize = 64;

/// A checkout on a real filesystem, by way of tokio's filesystem calls.
///
/// A scan is a synchronous walk on a blocking thread feeding the session
/// through a bounded channel, so the walk's `stat` and hash work stays off the
/// runtime's threads while still overlapping the store writes. Everything else
/// is one file at a time.
#[derive(Debug)]
pub struct TokioFs {
    id: BackendId,
    root: PathBuf,
    ignores: Vec<OsString>,
    hash_limit: u64,
}

impl TokioFs {
    /// A backend over `root`, named `fs`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            id: BackendId::new("fs"),
            root: root.into(),
            ignores: DEFAULT_IGNORES.iter().map(OsString::from).collect(),
            hash_limit: DEFAULT_HASH_LIMIT,
        }
    }

    /// Name this backend (and therefore its rep).
    #[must_use]
    pub fn with_id(mut self, id: impl Into<BackendId>) -> Self {
        self.id = id.into();
        self
    }

    /// Replace the set of directory names to skip.
    #[must_use]
    pub fn with_ignores(mut self, ignores: impl IntoIterator<Item = impl Into<OsString>>) -> Self {
        self.ignores = ignores.into_iter().map(Into::into).collect();
        self
    }

    /// Set the size at which a scan stops hashing and starts recording a
    /// stat-derived identity instead.
    #[must_use]
    pub fn with_hash_limit(mut self, hash_limit: u64) -> Self {
        self.hash_limit = hash_limit;
        self
    }

    /// The checkout root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The size above which a scan does not hash.
    #[must_use]
    pub fn hash_limit(&self) -> u64 {
        self.hash_limit
    }

    /// The directory names a walk skips.
    #[must_use]
    pub fn ignores(&self) -> &[OsString] {
        &self.ignores
    }

    /// The absolute path of a checkout-relative path.
    #[must_use]
    pub fn absolute(&self, path: &RelPath) -> PathBuf {
        let mut out = self.root.clone();
        for component in path.components() {
            out.push(component);
        }
        out
    }

    /// The token this backend records for an observed file.
    ///
    /// A content hash when the file is small enough to read; otherwise a token
    /// derived from the stat, which is honest about what it is: comparable only
    /// against other observations of this same backend, and upgraded by
    /// [`Backend::verify`] when a decision needs more.
    fn observed_token(&self, path: &RelPath, stat: StatFingerprint) -> Result<Token> {
        if stat.len <= self.hash_limit {
            Ok(Token::blake3(hash_file(&self.absolute(path))?))
        } else {
            Ok(Token::opaque(self.id.clone(), stat_key(&stat)))
        }
    }

    /// The entry this backend would record for an observation.
    ///
    /// The claim already recorded for the path is carried over: who put a path
    /// here is not something a filesystem forgets when a file is touched, which
    /// is what lets a pass remove a path whose bytes its user has been rewriting.
    fn observe(&self, item: &WalkItem, recorded: Option<&Entry>) -> Result<Entry> {
        let token = match &item.shape {
            Shape::File => Some(self.observed_token(&item.path, item.stat())?),
            Shape::Dir | Shape::Symlink { .. } => None,
        };
        Ok(self.entry_for(item, token, recorded))
    }

    /// The entry an observation and a content token amount to.
    ///
    /// The claim already recorded for the path is carried over: who put a path
    /// here is not something a filesystem forgets when a file is touched.
    fn entry_for(&self, item: &WalkItem, token: Option<Token>, recorded: Option<&Entry>) -> Entry {
        let mut entry = match &item.shape {
            Shape::File => Entry::file(token.expect(ERROR_IMPOSSIBLE), item.stat()),
            Shape::Dir => Entry::dir(None),
            Shape::Symlink { target } => Entry::symlink(target.clone(), item.stat()),
        };
        // Who put this path here is not something a scan can observe, so the record
        // keeps saying what it said: a user editing a file a doc owns does not
        // transfer ownership (ADR 010 §8.6).
        entry.claim = recorded.and_then(|recorded| recorded.claim.clone());
        entry
    }

    /// Whether the observation contradicts the recorded content identity.
    ///
    /// `None` means the identity stands — which is the common case, and the one
    /// that must not cost a read: a stat that matches is settled without
    /// opening the file at all, and a small file whose digest matches confirms
    /// the recorded provenance rather than replacing it.
    fn settle(&self, item: &WalkItem, recorded: &Entry) -> Result<Option<Entry>> {
        if shape_changed(recorded, item) {
            return Ok(Some(self.observe(item, Some(recorded))?));
        }
        if recorded.stat == item.fingerprint {
            return Ok(None);
        }
        match &item.shape {
            // A directory's identity is its existence; a symlink's is its
            // target, which the shape check above already compared.
            Shape::Dir | Shape::Symlink { .. } => Ok(None),
            Shape::File => {
                if item.stat().len > self.hash_limit {
                    // Refusing to hash is refusing to claim: the recorded
                    // provenance is dropped rather than asserted, and whoever
                    // needs the answer can ask for it (ADR 010 §3.2).
                    return Ok(Some(self.observe(item, Some(recorded))?));
                }
                let token = Token::blake3(hash_file(&self.absolute(&item.path))?);
                if recorded.content_token() == Some(&token) {
                    return Ok(None);
                }
                let mut entry = Entry::file(token, item.stat());
                entry.claim = recorded.claim.clone();
                Ok(Some(entry))
            }
        }
    }
}

#[async_trait]
impl Backend for TokioFs {
    fn id(&self) -> BackendId {
        self.id.clone()
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            // A checkout's files are its user's, so nothing may be taken *from*
            // it by reference: an edit through one path would land in the other.
            // It will still happily *take* a reference when it is offered one
            // (`accept`), which is how a blob store's video materializes without
            // a copy.
            immutable_content: false,
        }
    }

    /// This backend's bytes are files on this machine.
    fn locate(&self, path: &RelPath) -> Option<PathBuf> {
        Some(self.absolute(path))
    }

    async fn report(&self, report: &mut Report<'_>) -> Result<()> {
        if !tokio::fs::try_exists(&self.root)
            .await
            .map_err(|source| Error::fs(FsOp::Check, &self.root, source))?
        {
            // Nothing is there: every recorded path is a removal, which the
            // session's tail pass reports.
            return Ok(());
        }

        let (items, mut walked) = tokio::sync::mpsc::channel::<Result<WalkItem>>(WALK_AHEAD);
        let walk = FsWalk::new(self.root.clone(), self.ignores.clone())?;
        let walking = tokio::task::spawn_blocking(move || {
            for item in walk {
                // A send error means the session stopped consuming, which it
                // only does by erroring: that error is the one worth
                // reporting, and stopping is this thread's whole job.
                if items.blocking_send(item).is_err() {
                    return;
                }
            }
        });

        while let Some(item) = walked.recv().await {
            let item = item?;
            report.removed_before(&item.path).await?;
            let Some(recorded) = report.recorded_at(&item.path).await? else {
                let entry = self.observe(&item, None)?;
                report
                    .emit(Delta::Added {
                        path: item.path,
                        entry,
                    })
                    .await?;
                continue;
            };
            if let Some(entry) = self.settle(&item, &recorded)? {
                report
                    .emit(Delta::Changed {
                        path: item.path,
                        from: recorded,
                        to: entry,
                    })
                    .await?;
            } else if recorded.stat != item.fingerprint || recorded.avail != Avail::Present {
                report
                    .emit(Delta::Touched {
                        path: item.path,
                        entry: recorded.with_stat(item.fingerprint).present(),
                    })
                    .await?;
            }
        }

        // A walk that panicked closed the channel, and saying so is the join's
        // job: this must never look like a scan that simply found nothing.
        walking.await.map_err(Error::backend)?;
        Ok(())
    }

    async fn read(&self, path: &RelPath, range: Option<Range<u64>>) -> Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

        let absolute = self.absolute(path);
        let mut file = tokio::fs::File::open(&absolute)
            .await
            .map_err(|source| Error::fs(FsOp::Open, &absolute, source))?;
        let mut bytes = Vec::new();
        match range {
            None => {
                // A capacity hint, not a promise: the length may be a
                // directory's or may have moved since.
                let len = file.metadata().await.map(|meta| meta.len()).unwrap_or(0);
                bytes.reserve(len.min(DEFAULT_HASH_LIMIT) as usize);
                file.read_to_end(&mut bytes)
                    .await
                    .map_err(|source| Error::fs(FsOp::Read, &absolute, source))?;
            }
            Some(range) => {
                file.seek(std::io::SeekFrom::Start(range.start))
                    .await
                    .map_err(|source| Error::fs(FsOp::Seek, &absolute, source))?;
                let wanted = range.end.saturating_sub(range.start);
                (&mut file)
                    .take(wanted)
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(|source| Error::fs(FsOp::Read, &absolute, source))?;
            }
        }
        Ok(bytes)
    }

    async fn materialize(
        &self,
        path: &RelPath,
        entry: &Entry,
        bytes: &[u8],
    ) -> Result<Option<StatFingerprint>> {
        let absolute = self.absolute(path);
        ensure_parent_dir(&absolute).await?;
        match &entry.payload {
            Payload::File { .. } => {
                // The mode an entry records is part of what it is: an
                // executable file that materialized unexecutable would be a
                // change the next scan has to report.
                write_file_atomic(
                    &absolute,
                    bytes,
                    entry.stat.map(|stat| stat.mode),
                    bytes.len() as u64 > self.hash_limit,
                )
                .await?;
            }
            Payload::Dir => tokio::fs::create_dir_all(&absolute)
                .await
                .map_err(|source| Error::fs(FsOp::CreateDir, &absolute, source))?,
            Payload::Symlink { target } => write_symlink_atomic(&absolute, target).await?,
        }
        let meta = tokio::fs::symlink_metadata(&absolute)
            .await
            .map_err(|source| Error::fs(FsOp::Stat, &absolute, source))?;
        Ok(match &entry.payload {
            // A directory records no fingerprint: its size and mtime are
            // derived from its children, each recorded on its own.
            Payload::Dir => None,
            _ => Some(fingerprint(&meta)),
        })
    }

    async fn link_from(&self, path: &RelPath, source: &Path) -> Result<StatFingerprint> {
        let absolute = self.absolute(path);
        ensure_parent_dir(&absolute).await?;
        let scratch = scratch_path(&absolute);
        // A scratch path left behind by a crashed run is ours to replace.
        drop(tokio::fs::remove_file(&scratch).await);
        tokio::fs::hard_link(source, &scratch)
            .await
            .map_err(|source| Error::fs(FsOp::Link, &absolute, source))?;
        tokio::fs::rename(&scratch, &absolute)
            .await
            .map_err(|source| Error::fs(FsOp::Rename, &absolute, source))?;
        let meta = tokio::fs::symlink_metadata(&absolute)
            .await
            .map_err(|source| Error::fs(FsOp::Stat, &absolute, source))?;
        Ok(fingerprint(&meta))
    }

    async fn verify(&self, path: &RelPath) -> Result<Entry> {
        let absolute = self.absolute(path);
        let meta = tokio::fs::symlink_metadata(&absolute)
            .await
            .map_err(|source| Error::fs(FsOp::Stat, &absolute, source))?;
        let item = observe_at(path.clone(), &absolute, &meta)?;
        // Verifying is exactly the case where the size limit does not apply:
        // whoever asked wants the content hash.
        let token = match &item.shape {
            Shape::File => Some(Token::blake3(hash_file_async(&absolute).await?)),
            Shape::Dir | Shape::Symlink { .. } => None,
        };
        Ok(self.entry_for(&item, token, None))
    }

    /// A checkout answers from what it recorded and what it can see.
    ///
    /// The cheap answer first, because the common case by far is being asked
    /// about a path whose bytes are exactly what the record says: two equal
    /// digests settle it, and a digest is the one identity both sides can produce
    /// whoever minted the tokens. Only when that says nothing does this reach for
    /// the file — and only up to `hash_limit`, because reading a multi-gigabyte
    /// video to answer a question costs the same as copying it and answers less.
    ///
    /// Being asked about a path this checkout has no record of costs one read of
    /// a small file, and saves a copy: the same answer, arrived at the hard way.
    async fn accept(
        &self,
        path: &RelPath,
        recorded: Option<&Entry>,
        offered: &Entry,
    ) -> Result<Accepted> {
        if let Some(recorded) = recorded
            && recorded.kind() == offered.kind()
            && let (Some(ours), Some(theirs)) = (recorded.content_token(), offered.content_token())
            && ours == theirs
        {
            return Ok(Accepted::Current);
        }

        // No record is not the same as no file: a checkout whose records were
        // lost still holds what it holds, so the answer comes from the file
        // itself below.

        let absolute = self.absolute(path);
        // A directory and a symlink are their own answer: the path either has the
        // shape it should have or it does not, and one stat says which.
        match &offered.payload {
            Payload::Dir => {
                return Ok(match tokio::fs::symlink_metadata(&absolute).await {
                    Ok(meta) if meta.is_dir() => Accepted::Current,
                    Ok(_) => Accepted::Bytes,
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => Accepted::Bytes,
                    Err(source) => return Err(Error::fs(FsOp::Stat, &absolute, source)),
                });
            }
            Payload::Symlink { target } => {
                let existing = tokio::fs::read_link(&absolute).await;
                return Ok(match existing {
                    Ok(existing) if existing == Path::new(target) => Accepted::Current,
                    Ok(_) => Accepted::Bytes,
                    Err(source)
                        if matches!(
                            source.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::InvalidInput
                        ) =>
                    {
                        Accepted::Bytes
                    }
                    Err(source) => return Err(Error::fs(FsOp::ReadLink, &absolute, source)),
                });
            }
            Payload::File { .. } => {}
        }

        let Some(wanted) = offered.content_token().cloned() else {
            // No digest on the offer: nothing to compare, so the bytes travel.
            return Ok(Accepted::Bytes);
        };
        let meta = match tokio::fs::symlink_metadata(&absolute).await {
            Ok(meta) => meta,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Accepted::Bytes);
            }
            Err(source) => return Err(Error::fs(FsOp::Stat, &absolute, source)),
        };
        if !meta.is_file() || meta.len() > self.hash_limit {
            return Ok(Accepted::Bytes);
        }
        Ok(
            if Token::blake3(hash_file_async(&absolute).await?) == wanted {
                Accepted::Current
            } else {
                Accepted::Bytes
            },
        )
    }

    /// A checkout removes what a claim says was put here, and nothing else.
    ///
    /// Anything else in the tree is the user's own work or another backend's
    /// bookkeeping (`.git/`, a lock file, a temp file), and a pass that prunes
    /// those is a pass nobody can point at a directory that matters (ADR 010
    /// §8.7).
    async fn may_remove(&self, _path: &RelPath, recorded: &Entry) -> Result<bool> {
        Ok(recorded.claim.is_some())
    }

    /// Remove a path, and nothing under it.
    ///
    /// Deliberately not recursive: a backend reports every path it lost, so a
    /// removed subtree arrives here one path at a time, children first (the
    /// bridge reverses removal order). Recursing would delete paths this
    /// checkout never recorded.
    async fn remove(&self, path: &RelPath) -> Result<()> {
        let absolute = self.absolute(path);
        let meta = match tokio::fs::symlink_metadata(&absolute).await {
            Ok(meta) => meta,
            // Removing what is not there is done, not failed: a removal can be
            // reported twice, and a path can be taken by hand between the scan
            // that scheduled the transfer and the transfer itself.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err).map_err(|source| Error::fs(FsOp::Stat, &absolute, source)),
        };
        let removed = if meta.is_dir() {
            tokio::fs::remove_dir(&absolute).await
        } else {
            tokio::fs::remove_file(&absolute).await
        };
        removed.map_err(|source| Error::fs(FsOp::Remove, &absolute, source))
    }
}

/// What a path is, as the filesystem sees it.
#[derive(Debug)]
enum Shape {
    File,
    Dir,
    Symlink { target: OsString },
}

/// One observed path.
#[derive(Debug)]
struct WalkItem {
    path: RelPath,
    /// The change-detection fingerprint, absent for directories (see
    /// [`WalkItem::stat`]).
    fingerprint: Option<StatFingerprint>,
    shape: Shape,
}

impl WalkItem {
    /// The fingerprint of a non-directory observation.
    ///
    /// Directories record none: their size and mtime are derived from their
    /// children, and a child's own row already says what changed.
    fn stat(&self) -> StatFingerprint {
        self.fingerprint.expect(ERROR_IMPOSSIBLE)
    }
}

/// Build an observation from a stat, reading a symlink's target.
fn observe_at(path: RelPath, absolute: &Path, meta: &std::fs::Metadata) -> Result<WalkItem> {
    let shape = if meta.is_symlink() {
        let target = std::fs::read_link(absolute)
            .map_err(|source| Error::fs(FsOp::ReadLink, absolute, source))?;
        Shape::Symlink {
            target: target.into_os_string(),
        }
    } else if meta.is_dir() {
        Shape::Dir
    } else {
        Shape::File
    };
    // A directory's mtime and size are derived from its children, each of
    // which is recorded on its own: carrying them here would turn one child
    // edit into two deltas and could never detect anything by itself.
    let stat = match shape {
        Shape::Dir => None,
        _ => Some(fingerprint(meta)),
    };
    Ok(WalkItem {
        path,
        fingerprint: stat,
        shape,
    })
}

/// Whether an observation contradicts the recorded shape.
fn shape_changed(recorded: &Entry, item: &WalkItem) -> bool {
    match (&recorded.payload, &item.shape) {
        (Payload::File { .. }, Shape::File) => false,
        (Payload::Dir, Shape::Dir) => false,
        (Payload::Symlink { target }, Shape::Symlink { target: observed }) => target != observed,
        _ => true,
    }
}

/// A walk of a checkout root, in canonical path order (ADR 010 §3.3).
///
/// Directory entries are yielded *before* their children, because the path
/// order that orders a directory before its descendants is the same order the
/// store's primary key uses. Children are sorted by their encoded bytes for the
/// same reason.
struct FsWalk {
    ignores: Vec<OsString>,
    frames: Vec<Frame>,
}

struct Frame {
    rel: RelPath,
    abs: PathBuf,
    names: Vec<OsString>,
    at: usize,
}

impl FsWalk {
    /// Open a walk of `root`, which must be a directory.
    ///
    /// Takes the root by value so the walk can be moved to a blocking thread.
    fn new(root: PathBuf, ignores: Vec<OsString>) -> Result<Self> {
        if !root.is_dir() {
            return Err(Error::NotADirectory { path: root });
        }
        Ok(Self {
            frames: vec![Frame::open(RelPath::root(), root, &ignores)?],
            ignores,
        })
    }
}

impl Frame {
    /// A directory's names, or none of them if it is not there.
    ///
    /// A directory that vanished between being listed and being opened is a
    /// race, not a failure: the session holds rows for whatever it contained
    /// and reports them as removals without a second pass.
    fn open(rel: RelPath, abs: PathBuf, ignores: &[OsString]) -> Result<Self> {
        let read = match std::fs::read_dir(&abs) {
            Ok(read) => read,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    rel,
                    abs,
                    names: Vec::new(),
                    at: 0,
                });
            }
            Err(err) => return Err(err).map_err(|source| Error::fs(FsOp::ReadDir, &abs, source)),
        };
        let mut names = Vec::new();
        for entry in read {
            let entry = entry.map_err(|source| Error::fs(FsOp::ReadDir, &abs, source))?;
            let name = entry.file_name();
            if ignores.contains(&name) {
                continue;
            }
            names.push(name);
        }
        names.sort_by(|mine, theirs| mine.as_encoded_bytes().cmp(theirs.as_encoded_bytes()));
        Ok(Self {
            rel,
            abs,
            names,
            at: 0,
        })
    }
}

impl Iterator for FsWalk {
    type Item = Result<WalkItem>;

    fn next(&mut self) -> Option<Self::Item> {
        let Self { ignores, frames } = self;
        loop {
            let frame = frames.last_mut()?;
            if frame.at == frame.names.len() {
                frames.pop();
                continue;
            }
            let name = frame.names[frame.at].clone();
            frame.at += 1;
            let path = frame.rel.join(&name);
            let absolute = frame.abs.join(&name);
            let meta = match std::fs::symlink_metadata(&absolute) {
                Ok(meta) => meta,
                // A path that vanished between listing and statting is a race,
                // not a failure: the next report sees the removal.
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Some(Err(Error::fs(FsOp::Stat, &absolute, err))),
            };
            let item = match observe_at(path, &absolute, &meta) {
                Ok(item) => item,
                Err(err) => return Some(Err(err)),
            };
            if matches!(item.shape, Shape::Dir) {
                match Frame::open(item.path.clone(), absolute, ignores) {
                    Ok(frame) => frames.push(frame),
                    Err(err) => return Some(Err(err)),
                }
            }
            return Some(Ok(item));
        }
    }
}

/// A file's stat, as change detection sees it.
fn fingerprint(meta: &std::fs::Metadata) -> StatFingerprint {
    StatFingerprint {
        len: meta.len(),
        mode: mode_of(meta),
        // A filesystem that cannot report a modification time is exotic enough
        // that the epoch is a fine stand-in: length and mode still change.
        mtime: meta
            .modified()
            .map(TimeStamp::from)
            .unwrap_or(TimeStamp { secs: 0, nanos: 0 }),
    }
}

#[cfg(unix)]
fn mode_of(meta: &std::fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt as _;
    meta.mode()
}

#[cfg(not(unix))]
fn mode_of(_meta: &std::fs::Metadata) -> u32 {
    0
}

/// A backend-private identity for a file whose bytes we declined to read.
fn stat_key(stat: &StatFingerprint) -> Vec<u8> {
    let mut key = Vec::with_capacity(24);
    key.extend_from_slice(&stat.len.to_le_bytes());
    key.extend_from_slice(&stat.mode.to_le_bytes());
    key.extend_from_slice(&stat.mtime.secs.to_le_bytes());
    key.extend_from_slice(&stat.mtime.nanos.to_le_bytes());
    key
}

/// blake3 of a file's contents, streamed.
///
/// Synchronous on purpose: this runs inside the blocking walk, where one
/// thread is already spending its time on IO rather than on the runtime.
fn hash_file(path: &Path) -> Result<[u8; 32]> {
    use std::io::Read as _;

    let mut file =
        std::fs::File::open(path).map_err(|source| Error::fs(FsOp::Open, path, source))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| Error::fs(FsOp::Read, path, source))?;
        if read == 0 {
            return Ok(*hasher.finalize().as_bytes());
        }
        hasher.update(&buffer[..read]);
    }
}

/// The same digest, through tokio's file handle.
///
/// The scan never needs this — it hashes inside the blocking walk — but
/// [`Backend::verify`] does, where the caller wants a digest for a file the
/// scan declined to read and is not on the walk's thread.
async fn hash_file_async(path: &Path) -> Result<[u8; 32]> {
    use tokio::io::AsyncReadExt as _;

    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| Error::fs(FsOp::Open, path, source))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .await
            .map_err(|source| Error::fs(FsOp::Read, path, source))?;
        if read == 0 {
            return Ok(*hasher.finalize().as_bytes());
        }
        hasher.update(&buffer[..read]);
    }
}

async fn ensure_parent_dir(absolute: &Path) -> Result<()> {
    if let Some(parent) = absolute.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|source| Error::fs(FsOp::CreateDir, parent, source))?;
    }
    Ok(())
}

/// The scratch path a write lands on before it is renamed into place.
///
/// The store lock means one writer per checkout (ADR 011 §1), so the process id
/// is enough to keep two checkouts from colliding in a shared directory.
fn scratch_path(destination: &Path) -> PathBuf {
    let name = destination
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    destination.with_file_name(format!(".{name}.pauperfuse-{}", std::process::id()))
}

/// Write `bytes` to a scratch path and rename it into place.
///
/// The rename is what makes it atomic: a reader sees the old file or the new
/// one, never a half-written one. `durable` asks for the bytes to reach the
/// device before the rename, which is worth a fsync only for files too large to
/// hash on the next scan: a torn small file is caught by its digest, and a torn
/// large one has nothing else looking at it.
async fn write_file_atomic(
    destination: &Path,
    bytes: &[u8],
    mode: Option<u32>,
    durable: bool,
) -> Result<()> {
    use tokio::io::AsyncWriteExt as _;

    let scratch = scratch_path(destination);
    let write = async {
        let mut file = tokio::fs::File::create(&scratch).await?;
        file.write_all(bytes).await?;
        if durable {
            file.sync_all().await?;
        }
        Ok::<(), std::io::Error>(())
    };
    if let Err(err) = write.await {
        // Best effort: the write error is the one worth reporting.
        drop(tokio::fs::remove_file(&scratch).await);
        return Err(Error::fs(FsOp::Write, &scratch, err));
    }
    if let Some(mode) = mode {
        // Before the rename, so the file is never visible with the wrong mode.
        set_mode(&scratch, mode).await?;
    }
    tokio::fs::rename(&scratch, destination)
        .await
        .map_err(|source| Error::fs(FsOp::Rename, destination, source))?;
    Ok(())
}

/// Give a path the permission bits an entry records.
///
/// Permission bits only: setuid and sticky bits describe a host's policy about
/// a path rather than the content a document is carrying, and restoring those
/// from a shared tree is not this backend's call.
#[cfg(unix)]
async fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & 0o777))
        .await
        .map_err(|source| Error::fs(FsOp::SetMode, path, source))
}

#[cfg(not(unix))]
async fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

async fn write_symlink_atomic(destination: &Path, target: &OsStr) -> Result<()> {
    let scratch = scratch_path(destination);
    // A scratch path left behind by a crashed run is ours to replace.
    drop(tokio::fs::remove_file(&scratch).await);
    create_symlink(target, &scratch)
        .map_err(|source| Error::fs(FsOp::CreateSymlink, &scratch, source))?;
    tokio::fs::rename(&scratch, destination)
        .await
        .map_err(|source| Error::fs(FsOp::Rename, destination, source))?;
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &OsStr, at: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, at)
}

/// Symlinks are unix-shaped here on purpose: an entry records a target and not
/// whether it names a directory, and only unix can create a link without
/// knowing.
#[cfg(not(unix))]
fn create_symlink(_target: &OsStr, _at: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "creating symlinks needs a target kind, which an entry does not record",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::VtreeStore;
    use crate::store::mem::MemVtreeStore;
    use crate::test_support::{TestTree, path, scan, stat};

    /// A deployment's identity for a produced path — daybook's recipe, as an
    /// opaque token the core never reads.
    fn produced(state: u8) -> Token {
        Token::opaque(crate::backend::BackendId::new("daybook"), [state; 4])
    }

    /// The entry that deployment would report for it.
    fn rendered(state: u8) -> Entry {
        Entry::file(produced(state), None).with_claim(produced(state))
    }

    async fn empty_store() -> Arc<dyn VtreeStore> {
        Arc::new(MemVtreeStore::new())
    }

    #[tokio::test]
    async fn a_fresh_tree_reports_everything_once_and_then_nothing() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("notes/plan.md", b"# plan\n")?;
        tree.write("notes/2024/goals.md", b"goals\n")?;
        tree.write("empty-dir/.keep", b"")?;
        tree.mkdir("truly-empty")?;
        tree.symlink("link.md", "notes/plan.md")?;

        let backend = TokioFs::new(tree.root());
        let store = empty_store().await;
        let first = scan(&backend, &store).await?;
        let reported = first
            .iter()
            .map(Delta::path)
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert_eq!(
            reported,
            vec![
                "empty-dir",
                "empty-dir/.keep",
                "link.md",
                "notes",
                "notes/2024",
                "notes/2024/goals.md",
                "notes/plan.md",
                "truly-empty",
            ],
            "walk order, directories before their children"
        );
        assert!(first.iter().all(|delta| delta.needs_transfer()));

        // The whole point of a stat cache: the second scan reads no bytes and
        // reports no changes.
        let second = scan(&backend, &store).await?;
        assert!(second.is_empty(), "the second scan re-reported {second:#?}");

        let link = store.entry(&backend.id(), &path("link.md")).await?;
        assert!(
            matches!(
                link.map(|entry| entry.payload),
                Some(Payload::Symlink { target }) if target == "notes/plan.md"
            ),
            "symlinks are recorded as links, targets verbatim"
        );
        Ok(())
    }

    #[tokio::test]
    async fn small_files_are_identified_by_their_content() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("a.txt", b"one")?;
        let backend = TokioFs::new(tree.root());
        let store = empty_store().await;
        scan(&backend, &store).await?;

        let entry = store
            .entry(&backend.id(), &path("a.txt"))
            .await?
            .expect("the file was recorded");
        assert_eq!(
            entry.content_token(),
            Some(&Token::blake3_of(b"one")),
            "a small file's identity is a content hash"
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_edited_file_is_a_change_and_a_moved_clock_is_a_touch() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("a.txt", b"one")?;
        let backend = TokioFs::new(tree.root());
        let store = empty_store().await;
        let first = scan(&backend, &store).await?;
        let recorded = first
            .iter()
            .find(|delta| delta.path() == &path("a.txt"))
            .expect("a.txt was reported")
            .entry()
            .clone();

        // A clock that moved while the bytes did not: the digest settles it, and
        // the recorded provenance is kept rather than recomputed.
        let moved = recorded.clone().with_stat(StatFingerprint {
            len: 999,
            ..recorded.stat.expect("files have a stat")
        });
        store
            .apply(
                &backend.id(),
                &[Delta::Touched {
                    path: path("a.txt"),
                    entry: moved,
                }],
            )
            .await?;
        let touched = scan(&backend, &store).await?;
        assert_eq!(touched.len(), 1, "{touched:#?}");
        assert!(matches!(&touched[0], Delta::Touched { .. }));
        assert_eq!(touched[0].entry().content_token(), recorded.content_token());

        // Real bytes, real change.
        tree.write("a.txt", b"two")?;
        let changed = scan(&backend, &store).await?;
        assert_eq!(changed.len(), 1, "{changed:#?}");
        assert!(matches!(&changed[0], Delta::Changed { .. }));
        assert_eq!(
            changed[0].entry().content_token(),
            Some(&Token::blake3_of(b"two"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn large_files_are_not_read_until_something_needs_them() -> Result<()> {
        let tree = TestTree::new()?;
        let big = vec![b'x'; 4096];
        tree.write("big.bin", &big)?;
        // Every file is "too large to hash", so the scan cannot have read it.
        let backend = TokioFs::new(tree.root()).with_hash_limit(0);
        let store = empty_store().await;
        let reported = scan(&backend, &store).await?;

        let recorded = reported
            .iter()
            .find(|delta| delta.path() == &path("big.bin"))
            .expect("big.bin was reported")
            .entry()
            .clone();
        assert_eq!(recorded.content_token(), None, "{recorded:?}");
        let token = match &recorded.payload {
            Payload::File { origin: token, .. } => token.clone(),
            other => panic!("expected an external origin, got {other:?}"),
        };
        assert!(
            matches!(token.scheme(), crate::entry::TokenScheme::Opaque(_)),
            "a stat-derived token, comparable only within this backend"
        );

        // A stat bump on a large file drops the provenance instead of hashing,
        // and a decision that needs the answer asks for it.
        store
            .apply(
                &backend.id(),
                &[Delta::Touched {
                    path: path("big.bin"),
                    entry: recorded.clone().present(),
                }],
            )
            .await?;
        let verified = backend.verify(&path("big.bin")).await?;
        assert_eq!(verified.content_token(), Some(&Token::blake3_of(&big)));
        Ok(())
    }

    #[tokio::test]
    async fn deleted_paths_are_removed_including_their_children() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("keep.txt", b"keep")?;
        tree.write("gone/one.txt", b"one")?;
        tree.write("gone/deeper/two.txt", b"two")?;
        let backend = TokioFs::new(tree.root());
        let store = empty_store().await;
        scan(&backend, &store).await?;

        std::fs::remove_dir_all(tree.root().join("gone"))?;
        std::fs::remove_file(tree.root().join("keep.txt"))?;
        let removed = scan(&backend, &store).await?;
        assert_eq!(
            removed
                .iter()
                .map(Delta::path)
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "gone",
                "gone/deeper",
                "gone/deeper/two.txt",
                "gone/one.txt",
                "keep.txt"
            ]
        );
        assert!(
            removed
                .iter()
                .all(|delta| matches!(delta, Delta::Removed { .. }))
        );

        // And a vanished root is every recorded path removed, not an error.
        tree.write("back.txt", b"back")?;
        scan(&backend, &store).await?;
        tree.remove_root()?;
        let all_gone = scan(&backend, &store).await?;
        assert!(!all_gone.is_empty(), "the root took recorded paths with it");
        assert!(
            all_gone
                .iter()
                .all(|delta| matches!(delta, Delta::Removed { .. }))
        );
        assert_eq!(store.generation(&backend.id()).await?, Some(4));
        Ok(())
    }

    #[tokio::test]
    async fn materialized_content_is_quiet_on_the_next_scan() -> Result<()> {
        let tree = TestTree::new()?;
        let backend = TokioFs::new(tree.root());
        let store = empty_store().await;
        let entry = rendered(1);
        let bytes = b"# plan\n";

        // What the bridge does after a transfer: write the bytes, then record
        // what it wrote, under the identity it wrote them from.
        let stat = backend
            .materialize(&path("notes/plan.md"), &entry, bytes)
            .await?;
        let recorded = entry
            .clone()
            .with_stat(stat)
            .with_content_evidence(Token::blake3_of(bytes));
        store
            .put_entry(&backend.id(), &path("notes/plan.md"), &recorded)
            .await?;
        backend
            .materialize(&path("notes"), &Entry::dir(None), &[])
            .await?;

        let quiet = scan(&backend, &store).await?;
        assert!(quiet.is_empty(), "materialization echoed back: {quiet:#?}");

        // Editing it does echo: the record's identity is stale, the bytes are
        // the truth, and the claim survives the edit (ADR 010 §8.6).
        std::fs::write(tree.root().join("notes/plan.md"), b"# plan!\n")?;
        let edited = scan(&backend, &store).await?;
        assert!(
            edited
                .iter()
                .any(|delta| delta.path() == &path("notes/plan.md"))
        );
        assert_eq!(
            edited[0].entry().content_token(),
            Some(&Token::blake3_of(b"# plan!\n"))
        );
        assert_eq!(
            edited[0].entry().claim,
            Some(produced(1)),
            "an edit is not a change of ownership"
        );
        Ok(())
    }

    #[tokio::test]
    async fn linking_does_not_copy_bytes() -> Result<()> {
        let tree = TestTree::new()?;
        let elsewhere = tempfile::tempdir()?;
        let source = elsewhere.path().join("photo.bin");
        std::fs::write(&source, b"a photo's worth of bytes")?;

        let backend = TokioFs::new(tree.root()).with_hash_limit(0);
        let stat = backend.link_from(&path("photos/one.bin"), &source).await?;
        assert_eq!(stat.len, 24);

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            let source_meta = std::fs::metadata(&source)?;
            let linked = std::fs::metadata(tree.root().join("photos/one.bin"))?;
            assert_eq!(linked.ino(), source_meta.ino(), "the bytes were not copied");
            assert!(linked.nlink() > 1);
        }
        assert_eq!(
            backend.read(&path("photos/one.bin"), None).await?,
            b"a photo's worth of bytes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialized_files_keep_their_mode() -> Result<()> {
        let tree = TestTree::new()?;
        let backend = TokioFs::new(tree.root());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let executable = Entry::file(
                Token::blake3([1; 32]),
                StatFingerprint {
                    mode: 0o755,
                    ..stat(4)
                },
            );
            backend
                .materialize(&path("run.sh"), &executable, b"#!/bin/sh\n")
                .await?;
            let mode = std::fs::metadata(tree.root().join("run.sh"))?
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o777,
                0o755,
                "a file must land with the mode it records"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn removing_is_idempotent_and_never_recurses() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("dir/file.txt", b"bytes")?;
        let backend = TokioFs::new(tree.root());

        backend.remove(&path("dir/file.txt")).await?;
        assert!(!tree.root().join("dir/file.txt").exists());
        backend
            .remove(&path("dir/file.txt"))
            .await
            .expect("removing what is gone is done, not failed");
        backend
            .remove(&path("nothing/here"))
            .await
            .expect("and so is removing what was never there");

        // A directory with something in it is an error, not a recursive
        // delete: whoever wanted it gone had to name its contents, and
        // anything they did not name is not this backend's to remove.
        tree.write("dir/other.txt", b"other")?;
        assert!(
            backend.remove(&path("dir")).await.is_err(),
            "a non-empty directory is refused, never recursed into"
        );
        assert!(tree.root().join("dir/other.txt").exists());
        Ok(())
    }

    #[tokio::test]
    async fn a_root_that_is_not_a_directory_is_refused() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("a-file", b"bytes")?;
        let backend = TokioFs::new(tree.root().join("a-file"));
        let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::new());

        let error = scan(&backend, &store)
            .await
            .expect_err("a file is not a checkout root");
        assert!(
            matches!(error, Error::NotADirectory { .. }),
            "expected a refusal to walk a non-directory, got {error:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reads_can_be_ranged() -> Result<()> {
        let tree = TestTree::new()?;
        tree.write("a.txt", b"0123456789")?;
        let backend = TokioFs::new(tree.root());
        assert_eq!(backend.read(&path("a.txt"), None).await?, b"0123456789");
        assert_eq!(backend.read(&path("a.txt"), Some(2..5)).await?, b"234");
        assert_eq!(backend.read(&path("a.txt"), Some(8..99)).await?, b"89");
        assert_eq!(backend.read(&path("a.txt"), Some(10..12)).await?, b"");
        Ok(())
    }

    #[tokio::test]
    async fn symlinks_and_directories_materialize_by_shape() -> Result<()> {
        let tree = TestTree::new()?;
        let backend = TokioFs::new(tree.root());
        backend
            .materialize(&path("nested/dir"), &Entry::dir(None), &[])
            .await?;
        backend
            .materialize(
                &path("nested/dir/link"),
                &Entry::symlink("../target", None),
                &[],
            )
            .await?;
        assert!(tree.root().join("nested/dir").is_dir());
        assert_eq!(
            std::fs::read_link(tree.root().join("nested/dir/link"))?,
            PathBuf::from("../target")
        );

        // Materializing over a link replaces it with a file, and back again.
        backend
            .materialize(&path("nested/dir/link"), &rendered(2), b"bytes")
            .await?;
        assert_eq!(
            std::fs::read(tree.root().join("nested/dir/link"))?,
            b"bytes"
        );
        backend
            .materialize(
                &path("nested/dir/link"),
                &Entry::symlink("elsewhere", None),
                &[],
            )
            .await?;
        assert!(std::fs::symlink_metadata(tree.root().join("nested/dir/link"))?.is_symlink());
        assert!(
            !tree.root().join(".link.pauperfuse-scratch").exists(),
            "scratch files do not survive a successful write"
        );
        Ok(())
    }
}
