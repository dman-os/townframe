//! Fixtures shared by this crate's tests.

use crate::backend::{Accepted, Backend, BackendId, Capabilities, Report};
use crate::delta::Delta;
use crate::entry::{Entry, Payload, StatFingerprint, TimeStamp, Token};
use crate::interlude::*;
use crate::path::RelPath;
use crate::store::{RepScan, VtreeStore};

/// A checkout-relative path from a `notes/plan.md` style string.
pub fn path(text: &str) -> RelPath {
    RelPath::try_new(text.split('/').map(OsString::from).collect()).expect(ERROR_PARSE)
}

/// A stat fingerprint.
pub fn stat(len: u64) -> StatFingerprint {
    StatFingerprint {
        len,
        mode: 0o644,
        mtime: TimeStamp {
            secs: 1_700_000_000,
            nanos: 0,
        },
    }
}

/// A temporary checkout to fill with files.
pub struct TestTree {
    dir: tempfile::TempDir,
}

impl TestTree {
    /// An empty checkout.
    pub fn new() -> Result<Self> {
        Ok(Self {
            dir: tempfile::tempdir()?,
        })
    }

    /// The checkout root.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Write a file, creating its parents.
    pub fn write(&self, path: &str, bytes: &[u8]) -> Result<()> {
        let absolute = self.root().join(path);
        if let Some(parent) = absolute.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&absolute, bytes)?;
        Ok(())
    }

    /// Read a file back.
    pub fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(std::fs::read(self.root().join(path))?)
    }

    /// Create a directory, creating its parents.
    pub fn mkdir(&self, path: &str) -> Result<()> {
        std::fs::create_dir_all(self.root().join(path))?;
        Ok(())
    }

    /// Create a symlink.
    #[cfg(unix)]
    pub fn symlink(&self, path: &str, target: &str) -> Result<()> {
        let absolute = self.root().join(path);
        if let Some(parent) = absolute.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::os::unix::fs::symlink(target, absolute)?;
        Ok(())
    }

    /// Remove the checkout root itself.
    pub fn remove_root(&self) -> Result<()> {
        std::fs::remove_dir_all(self.root())?;
        Ok(())
    }
}

/// A doc-backed backend: what a lens layer presents, without a CRDT behind it.
///
/// The two directions are deliberately asymmetric, and the asymmetry is the thing
/// under test. As a **source** it renders: [`read`](Backend::read) turns a path's
/// doc content into bytes, while [`report`](Backend::report) never renders at
/// all, so scanning a doc costs no renders (ADR 010 §2.5). As a **target** it
/// ingests: [`materialize`](Backend::materialize) is where bytes a checkout wrote
/// become the doc's content, and [`remove`](Backend::remove) is the daybook-side
/// deletion — a move to trash (ADR 011 §4), not a destroy.
///
/// The lens is the identity: content in, the same bytes out. That keeps the
/// fixture honest about what it exercises — the loop through the bridge — without
/// pretending to be markdown.
pub struct LensBackend {
    id: BackendId,
    doc: RwLock<Doc>,
}

/// The doc behind [`LensBackend`].
#[derive(Default)]
struct Doc {
    /// What the doc holds, by path. Ordered, because a report walks in canonical
    /// path order and a diff against a rep is a merge join on that order.
    faces: BTreeMap<RelPath, Vec<u8>>,
    /// What ingest took out. A real doc keeps this at its `/trash/` dpath
    /// (FDR 003 §6) and checkouts exclude it; keeping it here is what makes a
    /// removal recoverable instead of destructive.
    trash: BTreeMap<RelPath, Vec<u8>>,
    /// What the doc renders from. Bumping it is a lens upgrade, and every face
    /// re-renders under the new version.
    lens_ver: u32,
    /// Bytes this doc's lens cannot represent, standing in for a facet that fails
    /// validation. Ingesting them bounces (ADR 011 §5).
    refuses: Option<Vec<u8>>,
}

impl Doc {
    /// What this doc reports for one path: rendered by this lens, owned by this
    /// doc, with the digest of the bytes it renders.
    fn entry(&self, path: &RelPath) -> Entry {
        let content = self
            .faces
            .get(path)
            .unwrap_or_else(|| panic!("the doc holds no face at {path}"));
        self.entry_of(content)
    }

    /// The entry for content this doc holds, without looking the path up again.
    ///
    /// No content evidence, deliberately: a lens has to *run* to know what it
    /// produces, so a producer that could pre-compute the digest of its own output
    /// would be able to answer questions a real one cannot. That is the whole
    /// difference between "these bytes are already what I would produce" (the
    /// target may skip the work) and "I cannot know without producing them" (the
    /// bytes travel) — ADR 010 §2.3, §2.5.
    fn entry_of(&self, content: &[u8]) -> Entry {
        Entry::file(self.identity(content), None).with_claim(self.owner())
    }

    /// The identity this doc mints for one rendered path: the doc, its state for
    /// *that* path, the lens and its version, encoded however the deployment
    /// likes. The core only ever compares these (ADR 010 §2.3).
    ///
    /// The state is the face's own content, which is what makes an edit to one
    /// path leave every *other* path's identity alone — the property incremental
    /// render rests on. A real doc has a CRDT state hash here; this is the thing
    /// that plays its role.
    fn identity(&self, content: &[u8]) -> Token {
        let mut bytes = DOC_REF.to_vec();
        bytes.push(0);
        bytes.extend_from_slice(Token::blake3_of(content).as_bytes());
        bytes.push(0);
        bytes.push(self.lens_ver as u8);
        deployment(bytes)
    }

    /// Which doc owns a path. The state is deliberately absent: an edit does not
    /// change who the path belongs to (ADR 010 §8.6).
    fn owner(&self) -> Token {
        let mut bytes = DOC_REF.to_vec();
        bytes.push(0);
        bytes.push(self.lens_ver as u8);
        deployment(bytes)
    }
}

/// The doc id every face of the fixture hangs off.
const DOC_REF: &[u8] = b"fake-doc";

/// This fixture's scheme: the one identity space the core must never read.
fn deployment(bytes: Vec<u8>) -> Token {
    Token::opaque(BackendId::new("daybook"), bytes)
}

impl LensBackend {
    /// A doc backend named `id`, holding nothing.
    pub fn new(id: &str) -> Self {
        Self {
            id: BackendId::new(id),
            doc: RwLock::new(Doc {
                lens_ver: 1,
                ..Doc::default()
            }),
        }
    }

    /// Write content into the doc, as an upstream edit would.
    pub fn edit(&self, text: &str, bytes: &[u8]) {
        self.with(|doc| {
            doc.faces.insert(path(text), bytes.to_vec());
        });
    }

    /// Take a path out of the doc, as untagging it upstream would.
    pub fn drop_face(&self, text: &str) {
        self.with(|doc| {
            doc.faces.remove(&path(text));
        });
    }

    /// Upgrade the lens: every face now renders under a new version.
    pub fn bump_lens_ver(&self) {
        self.with(|doc| doc.lens_ver += 1);
    }

    /// Refuse to ingest bytes containing `bytes`, as a facet that fails
    /// validation does.
    pub fn refusing(&self, bytes: &[u8]) {
        self.with(|doc| doc.refuses = Some(bytes.to_vec()));
    }

    /// The identity this doc would report for a path.
    pub fn identity(&self, text: &str) -> Token {
        self.read()
            .identity(&self.content(text).expect("the doc holds the face"))
    }

    /// Which doc owns the paths it renders.
    pub fn owner(&self) -> Token {
        self.read().owner()
    }

    /// The doc's content at a path, as it stands.
    #[must_use]
    pub fn content(&self, text: &str) -> Option<Vec<u8>> {
        self.read().faces.get(&path(text)).cloned()
    }

    /// What ingest removed, while it is still recoverable.
    #[must_use]
    pub fn trashed(&self, text: &str) -> Option<Vec<u8>> {
        self.read().trash.get(&path(text)).cloned()
    }

    /// The paths the doc holds, in canonical order.
    #[must_use]
    pub fn faceted(&self) -> Vec<String> {
        self.read().faces.keys().map(RelPath::to_string).collect()
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Doc> {
        self.doc.read().expect(ERROR_MUTEX)
    }

    fn with<T>(&self, change: impl FnOnce(&mut Doc) -> T) -> T {
        change(&mut self.doc.write().expect(ERROR_MUTEX))
    }
}

#[async_trait]
impl Backend for LensBackend {
    fn id(&self) -> BackendId {
        self.id.clone()
    }

    fn capabilities(&self) -> Capabilities {
        // No filesystem, so nothing can be materialized into it by reference or
        // linked from it; its bytes are not immutable because they are not bytes.
        Capabilities::default()
    }

    fn locate(&self, _path: &RelPath) -> Option<PathBuf> {
        None
    }

    async fn report(&self, report: &mut Report<'_>) -> Result<()> {
        // Directories are implied by the paths themselves, so there is nothing to
        // walk but the map — and no render happens on the way. The doc is read
        // once, into a snapshot, so no lock is held across a report's awaits.
        let faces: Vec<(RelPath, Entry)> = {
            let doc = self.read();
            doc.faces
                .values()
                .map(|content| doc.entry_of(content))
                .zip(doc.faces.keys().cloned())
                .map(|(entry, path)| (path, entry))
                .collect()
        };
        for (path, entry) in &faces {
            report.removed_before(path).await?;
            match report.recorded_at(path).await? {
                // A doc is the authority for the identity of the paths it
                // renders, so the comparison is between *recipes*, not between
                // digests. Content evidence cannot see a lens version bump — the
                // new lens may render the same bytes — and a doc that stayed
                // quiet about one would leave the checkout rendering under a
                // lens nobody supports any more (ADR 012 §2).
                Some(recorded) if recorded.origin() == entry.origin() => {
                    if recorded.stat != entry.stat {
                        report
                            .emit(Delta::Touched {
                                path: path.clone(),
                                entry: entry.clone(),
                            })
                            .await?;
                    }
                }
                Some(recorded) => {
                    report
                        .emit(Delta::Changed {
                            path: path.clone(),
                            from: recorded,
                            to: entry.clone(),
                        })
                        .await?;
                }
                None => {
                    report
                        .emit(Delta::Added {
                            path: path.clone(),
                            entry: entry.clone(),
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Render a path: the identity lens, so the doc's content is the bytes.
    async fn read(&self, path: &RelPath, range: Option<Range<u64>>) -> Result<Vec<u8>> {
        let bytes = self
            .read()
            .faces
            .get(path)
            .unwrap_or_else(|| panic!("the doc holds no face at {path}"))
            .clone();
        Ok(match range {
            None => bytes,
            Some(range) => {
                let start = (range.start as usize).min(bytes.len());
                let end = (range.end as usize).clamp(start, bytes.len());
                bytes[start..end].to_vec()
            }
        })
    }

    /// Ingest: bytes a checkout wrote become the doc's content.
    ///
    /// Ingest: bytes a checkout wrote become the doc's content.
    ///
    /// The fixture's refusals are the interesting part. A lens that cannot
    /// represent the bytes bounces, and a doc that already trashed the path does
    /// not re-adopt it: a trash is a decision, and a file reappearing next to it
    /// does not undo that.
    ///
    /// (Both are also answered in `accept`, before any bytes are read; this
    /// enforcement is what an interface that only asks first would miss.)
    async fn materialize(
        &self,
        path: &RelPath,
        _entry: &Entry,
        bytes: &[u8],
    ) -> Result<Option<StatFingerprint>> {
        let refuses = self.read().refuses.clone();
        if let Some(refuse) = refuses
            && !refuse.is_empty()
            && bytes.windows(refuse.len()).any(|window| window == refuse)
        {
            return Err(Error::message(format!(
                "the lens cannot represent the bytes at {path}"
            )));
        }
        let trashed = self.with(|doc| {
            if doc.trash.contains_key(path) {
                return true;
            }
            doc.faces.insert(path.clone(), bytes.to_vec());
            false
        });
        if trashed {
            // Nothing changed, and nothing is reported: the doc's next report is
            // quiet about this path, and the checkout's file is its own.
            return Ok(None);
        }
        // A doc records no stat: there is no filesystem behind it to fingerprint.
        Ok(None)
    }

    /// A doc answers for itself: it holds the path or it does not.
    ///
    /// A path it already holds with the offered bytes needs nothing — the offer
    /// is asking it to ingest what it just produced, and the digest is enough to
    /// say so without rendering or reading anything. A path its trash has refused
    /// is not re-adopted, and a path it has never seen is ingested.
    async fn accept(
        &self,
        path: &RelPath,
        _recorded: Option<&Entry>,
        offered: &Entry,
    ) -> Result<Accepted> {
        if !matches!(offered.payload, Payload::File { .. }) {
            // A doc holds files; the paths it holds imply the directories above
            // them, so there is nothing to take and nothing to report.
            return Ok(Accepted::Current);
        }
        let doc = self.read();
        if doc.trash.contains_key(path) {
            return Ok(Accepted::Current);
        }
        let held = doc.faces.get(path).map(|content| Token::blake3_of(content));
        if held.is_some() && held.as_ref() == offered.content_token() {
            return Ok(Accepted::Current);
        }
        Ok(Accepted::Bytes)
    }

    /// A doc lets go of what a claim says is its own, and of nothing else: the
    /// other paths in a checkout are its user's (ADR 011 §4).
    async fn may_remove(&self, _path: &RelPath, recorded: &Entry) -> Result<bool> {
        Ok(recorded.claim.is_some())
    }

    async fn link_from(&self, _path: &RelPath, _source: &Path) -> Result<StatFingerprint> {
        unimplemented!("a doc backend has no filesystem to link into")
    }

    /// The daybook-side deletion (ADR 011 §4): move to trash, keeping the bytes.
    async fn remove(&self, path: &RelPath) -> Result<()> {
        self.with(|doc| {
            if let Some(content) = doc.faces.remove(path) {
                doc.trash.insert(path.clone(), content);
            }
        });
        Ok(())
    }

    /// Rendering is deterministic, so this backend *can* say what a path's bytes
    /// hash to — which is exactly what a decision that needs a digest asks for.
    async fn verify(&self, path: &RelPath) -> Result<Entry> {
        Ok(self.read().entry(path))
    }
}

/// Observe a backend into its rep, returning what it reported.
pub async fn scan(backend: &dyn Backend, store: &Arc<dyn VtreeStore>) -> Result<Vec<Delta>> {
    let mut deltas = Vec::new();
    Report::run(
        backend,
        RepScan::new(Arc::clone(store), backend.id()),
        &mut deltas,
    )
    .await?;
    store.apply(&backend.id(), &deltas).await?;
    Ok(deltas)
}
