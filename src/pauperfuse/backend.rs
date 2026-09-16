//! Backends: the things the bridge observes, and writes into.
//!
//! A backend answers three questions: *what changed since last time* (its
//! [`report`](Backend::report), compared against the rep the store recorded for
//! it — its own business, in its own vocabulary); *what do you want done about
//! this* ([`accept`](Backend::accept) and [`may_remove`](Backend::may_remove) —
//! the only place two vocabularies meet, so the backend that holds the path
//! answers); and *give me / put the bytes here* ([`read`](Backend::read),
//! [`materialize`](Backend::materialize), [`link_from`](Backend::link_from)).
//!
//! Reporting is a session, not a return value ([`Report`]): a backend pushes
//! deltas as it walks, so a first materialization of a million-file node
//! streams into the store instead of building a million-element vector. The
//! rep's recorded rows are consumed through the same session, in the same path
//! order, so a scan is one merge join and never a random lookup per file.

use crate::delta::Delta;
use crate::entry::{Entry, StatFingerprint};
use crate::interlude::*;
use crate::path::RelPath;
use crate::store::RepScan;

/// The name of a backend, and of the rep that records its state.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BackendId(String);

impl BackendId {
    /// Name a backend.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The name, as it is stored and logged.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BackendId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<&str> for BackendId {
    fn from(id: &str) -> Self {
        Self::new(id)
    }
}

/// What a backend can do beyond reading and writing bytes.
///
/// The bridge degrades to a byte transfer for anything a backend cannot do, so
/// this grows one capability at a time, each with a fallback. What a backend can
/// *take* is not here: it says so per path, when asked ([`Backend::accept`]),
/// because that is a decision about one path's bytes rather than a fact about
/// the backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// This backend's own bytes may be linked *from*: they never change in
    /// place, so aliasing another path onto them cannot be observed as a
    /// change by anyone.
    ///
    /// A checkout says no — its files are its user's to edit, and an edit
    /// through one path would land in the other. A content-addressed blob
    /// store says yes, and that is the case the cheap path exists for: a
    /// photo library materializes at the speed of directory metadata.
    pub immutable_content: bool,
}

/// What a target wants done about a path the source offers it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Accepted {
    /// Nothing: the target already holds these bytes, whatever name it knows
    /// them by. The records disagreed; this is the backend saying that is its
    /// business and not a reason to move anything.
    Current,
    /// A local path to take by reference — hardlinked, exported, or referenced
    /// in place — instead of a copy (ADR 010 §2.5). The bridge does this only
    /// when the source also says its bytes do not change in place, and when it
    /// can name a path for them; otherwise it copies.
    ByReference,
    /// The bytes themselves.
    Bytes,
}

/// Where a backend pushes the deltas it observes.
///
/// A backend never decides what happens to a delta: it reports, the bridge
/// applies. This is what keeps a million-file scan from becoming a
/// million-element vector.
#[async_trait]
pub trait DeltaSink: Send {
    /// Record one observed delta.
    async fn send(&mut self, delta: Delta) -> Result<()>;
}

#[async_trait]
impl DeltaSink for Vec<Delta> {
    async fn send(&mut self, delta: Delta) -> Result<()> {
        self.push(delta);
        Ok(())
    }
}

/// A backend's change-reporting session (ADR 010 §4.1).
///
/// The session consumes the rep's recorded rows in canonical path order, so a
/// report is a merge join between the backend's own inspection order and the
/// store's key order. Callers must report paths in non-decreasing path order,
/// which is what a walk yields and what [`Report::removed_before`] assumes.
pub struct Report<'a> {
    recorded: RepScan,
    sink: &'a mut dyn DeltaSink,
    finished: bool,
}

impl<'a> Report<'a> {
    /// Open a session over one rep's recorded rows.
    pub fn new(recorded: RepScan, sink: &'a mut dyn DeltaSink) -> Self {
        Self {
            recorded,
            sink,
            finished: false,
        }
    }

    /// Run a backend's report to completion, including the tail of removals a
    /// backend cannot see from inside its own walk.
    ///
    /// This is how a backend should be driven: [`Backend::report`] must not
    /// have to remember to close the session.
    pub async fn run(
        backend: &dyn Backend,
        recorded: RepScan,
        sink: &'a mut dyn DeltaSink,
    ) -> Result<()> {
        let mut report = Self::new(recorded, sink);
        backend.report(&mut report).await?;
        report.finish().await
    }

    /// Emit a delta.
    pub async fn emit(&mut self, delta: Delta) -> Result<()> {
        self.sink.send(delta).await
    }

    /// Emit `Removed` for every recorded path before `path`.
    ///
    /// A backend calls this before handling the path it is looking at, which is
    /// what makes "recorded but gone" fall out of the merge join rather than
    /// out of a second pass.
    pub async fn removed_before(&mut self, path: &RelPath) -> Result<()> {
        while let Some((recorded, entry)) = self.recorded.take_earlier(path).await? {
            self.emit(Delta::Removed {
                path: recorded,
                entry,
            })
            .await?;
        }
        Ok(())
    }

    /// What the rep recorded at `path`, if anything, consuming it.
    pub async fn recorded_at(&mut self, path: &RelPath) -> Result<Option<Entry>> {
        self.recorded.take_at(path).await
    }

    /// Emit `Removed` for every recorded path the backend did not report.
    pub async fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        while let Some((path, entry)) = self.recorded.take_next().await? {
            self.emit(Delta::Removed { path, entry }).await?;
        }
        Ok(())
    }
}

/// One place content lives: a filesystem, a doc-backed source, a wasi scratch
/// tree, a git worktree.
#[async_trait]
pub trait Backend: Send + Sync {
    /// This backend's name, and the name of its rep.
    fn id(&self) -> BackendId;

    /// What this backend can do beyond bytes.
    fn capabilities(&self) -> Capabilities;

    /// Where this backend's bytes for `path` are on this machine, if it has
    /// them here.
    ///
    /// This is what makes [`Accepted::ByReference`] decidable from outside: a
    /// target that asks for a reference gets one when the source can name a local
    /// path, and a copy otherwise. A backend whose content lives elsewhere (behind a network,
    /// in a blob store it has not materialized) answers `None`, and nothing
    /// else has to know why.
    ///
    /// Only asked about files: a directory and a symlink are made from their
    /// entry, not from whatever is on disk.
    fn locate(&self, path: &RelPath) -> Option<PathBuf>;

    /// Report this backend's changes relative to the recorded rep.
    ///
    /// Paths must be reported in non-decreasing canonical order
    /// ([`RelPath`]'s order). A backend that cannot read a path should report
    /// the error rather than silently omit it: an omission looks like a
    /// deletion, and rep state is authoritative for what happens next.
    async fn report(&self, report: &mut Report<'_>) -> Result<()>;

    /// Read a path's bytes, or a byte range of them.
    ///
    /// Ranges are the streaming primitive: callers transfer large content in
    /// ranges rather than asking for a whole file up front. A range shorter
    /// than requested means the file is shorter.
    async fn read(&self, path: &RelPath, range: Option<Range<u64>>) -> Result<Vec<u8>>;

    /// Create or replace `path` so it matches `entry`.
    ///
    /// `bytes` is the file content, and is ignored for directories and
    /// symlinks (whose shape comes from `entry`). Atomic per file: a reader
    /// never observes a half-written file, and an interrupted call leaves
    /// either the old file or the new one.
    ///
    /// Returns the fingerprint the backend now records for the path, absent
    /// when its shape records none (a directory).
    async fn materialize(
        &self,
        path: &RelPath,
        entry: &Entry,
        bytes: &[u8],
    ) -> Result<Option<StatFingerprint>>;

    /// Create or replace `path` by linking bytes that are already on this
    /// machine at `source`.
    ///
    /// Only called when [`Capabilities::link`] is set. This is the cheap path
    /// for blob-backed content: no bytes are copied, and nothing is hashed.
    async fn link_from(&self, path: &RelPath, source: &Path) -> Result<StatFingerprint>;

    /// What this backend wants done about `path`, which the source offers as
    /// `offered` and this backend has recorded as `recorded`.
    ///
    /// Asked only where the two records disagree ([`Entry::agrees_with`]), and
    /// this is the only place that disagreement is settled: whether two
    /// identities stand for the same bytes depends on the schemes in play, and
    /// this backend holds both values, so it can answer with a policy it owns —
    /// compare tokens if the scheme is its own, compare digests, read and hash
    /// the file if that is worth it, or just take the bytes and stop guessing.
    /// Every answer is correct; only the cost differs, and this backend pays it.
    ///
    /// `recorded` is `None` when this backend has no record for the path at all,
    /// which is not the same as not having the file: a backend whose records were
    /// lost can still recognise bytes it holds and answer [`Accepted::Current`].
    async fn accept(
        &self,
        path: &RelPath,
        recorded: Option<&Entry>,
        offered: &Entry,
    ) -> Result<Accepted>;

    /// Whether this backend's own record licenses removing `path`, which the
    /// source no longer has.
    ///
    /// Asked only about paths this backend holds and the source does not. The
    /// claim column is opaque to the core, so the rule that reads it lives here:
    /// a record that says who put the path here may go when that hand drops it,
    /// and a record with nothing to say means "this is my own file", which
    /// nothing deletes (ADR 010 §8.7).
    async fn may_remove(&self, path: &RelPath, recorded: &Entry) -> Result<bool>;

    /// Remove `path`, and nothing under it.
    ///
    /// Removals arrive one path at a time, children before their directory
    /// (the bridge reverses removal order), so this never recurses: recursing
    /// would delete paths this backend never recorded, and a target that
    /// prunes what it does not know about is not a target anyone can trust
    /// with their own files.
    ///
    /// Removing what is not there is done, not failed.
    async fn remove(&self, path: &RelPath) -> Result<()>;

    /// Establish this backend's identity for a path whose bytes have not been
    /// examined.
    ///
    /// A backend may deliberately record an identity it did not compute (an fs
    /// backend refuses to hash a large file on a stat bump, ADR 010 §3.2);
    /// this is how that identity gets upgraded when a decision actually needs
    /// it.
    async fn verify(&self, path: &RelPath) -> Result<Entry>;
}
