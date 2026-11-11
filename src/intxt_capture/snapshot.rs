//! What differs from the index, and what changed between two looks at it (ADR 039 §3).
//!
//! A snapshot is the files that differ from the git index (modified, deleted, and untracked but
//! not ignored), each with the hash git would store for its content, plus the index listing.
//! The difference of two snapshots is what happened in between to every path: whatever a tool
//! call did, an edit tool's write, a formatter, a checkout, a `git add`. On a repository the
//! size of this one a snapshot costs about 25 ms because only dirty files are hashed.
//!
//! A path's content is its dirty hash when it has one, else its index entry. So a file that is
//! committed or staged after being edited, without changing again, is not a change: the commit
//! is observed through the reflog instead (see [`crate::git`]).

use crate::Error;
use crate::git::Git;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Stdio;

/// Files larger than this are kept as blobs, not collaborative text (ledger D43).
const MAX_TEXT_BYTES: u64 = 256 * 1024;

/// File names whose contents are generated and so never diffed, whatever `.gitattributes` says.
const LOCKFILES: [&str; 9] = [
    "Cargo.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "flake.lock",
    "poetry.lock",
    "uv.lock",
    "Gemfile.lock",
    "go.sum",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Path → blob id, for every path in the index (merge conflicts excluded).
    index: BTreeMap<String, String>,
    /// Path → content hash for every path that differs from the index; `None` is a path the
    /// index has and the worktree lacks.
    dirty: BTreeMap<String, Option<String>>,
}

/// What happened to a path between two snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    /// A path that disappeared and another that appeared with identical content. A file that
    /// was also edited is not recognised: it is a deletion and an addition.
    Renamed {
        from: String,
    },
}

/// Why content is not text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobReason {
    /// Not valid UTF-8, or holds a NUL.
    Binary,
    /// Over 256 KiB.
    Large,
    /// A known lockfile, or `linguist-generated` or `-diff` in `.gitattributes`.
    Generated,
}

/// A path's content after a change (D43).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    /// Valid UTF-8, at most 256 KiB, not generated: collaborative text.
    Text(String),
    /// Everything else. Only the location is returned, so a later layer uploads it from disk
    /// without it being read here; `hash` is what `git hash-object` gave when the snapshot was
    /// taken, to check that the file is still what was observed.
    Blob {
        path: PathBuf,
        size: u64,
        hash: String,
        reason: BlobReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// The path after the change (the old path of a deletion), relative to the worktree root.
    pub path: String,
    pub kind: ChangeKind,
    /// The content after the change; none for a deletion.
    pub content: Option<Content>,
}

impl Snapshot {
    /// Look at the worktree at `root` now.
    pub fn take(root: &Path) -> Result<Self, Error> {
        let git = Git::new(root);
        let paths = nul_separated_paths(&git.run(&[
            "ls-files",
            "-z",
            "-m",
            "-o",
            "--exclude-standard",
        ])?)?;
        let mut dirty = BTreeMap::new();
        let mut to_hash = Vec::new();
        for path in &paths {
            match std::fs::symlink_metadata(root.join(path)) {
                Ok(meta) if meta.is_file() => to_hash.push(path.as_str()),
                // Symlinks and submodule directories are not diffed.
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    dirty.insert(path.clone(), None);
                }
                Err(error) => return Err(error.into()),
            }
        }
        for (path, hash) in to_hash.iter().zip(hash_objects(&git, &to_hash)?) {
            dirty.insert((*path).to_owned(), Some(hash));
        }

        let listing = git.run(&["ls-files", "-s", "-z"])?;
        let mut index = BTreeMap::new();
        for entry in String::from_utf8_lossy(&listing)
            .split('\0')
            .filter(|e| !e.is_empty())
        {
            let (meta, path) = entry
                .split_once('\t')
                .ok_or_else(|| Error::Format(format!("ls-files entry {entry:?}")))?;
            let mut fields = meta.split(' ');
            if let (Some(_mode), Some(blob), Some("0")) =
                (fields.next(), fields.next(), fields.next())
            {
                index.insert(path.to_owned(), blob.to_owned());
            }
        }
        Ok(Self { index, dirty })
    }

    /// The state of a worktree that matches its index exactly. The first observation diffs
    /// against it, so files already dirty are reported as changes against the index.
    pub(crate) fn clean(&self) -> Snapshot {
        Snapshot {
            index: self.index.clone(),
            dirty: BTreeMap::new(),
        }
    }

    /// A path's content hash, `None` when it does not exist.
    fn hash(&self, path: &str) -> Option<&String> {
        match self.dirty.get(path) {
            Some(hash) => hash.as_ref(),
            None => self.index.get(path),
        }
    }

    /// What changed from `self` to `after`, sorted by path, with each changed path's content
    /// read from the worktree at `root` and classified.
    pub fn diff(&self, after: &Snapshot, root: &Path) -> Result<Vec<Change>, Error> {
        let mut candidates: BTreeSet<&String> =
            self.dirty.keys().chain(after.dirty.keys()).collect();
        for path in self.index.keys().chain(after.index.keys()) {
            if self.index.get(path) != after.index.get(path) {
                candidates.insert(path);
            }
        }
        let mut added: Vec<(&String, &String)> = Vec::new();
        let mut deleted: Vec<(&String, &String)> = Vec::new();
        let mut modified: Vec<(&String, &String)> = Vec::new();
        for path in candidates {
            match (self.hash(path), after.hash(path)) {
                (before, now) if before == now => {}
                (None, Some(now)) => added.push((path, now)),
                (Some(before), None) => deleted.push((path, before)),
                (Some(_), Some(now)) => modified.push((path, now)),
                (None, None) => unreachable!("equal hashes were skipped"),
            }
        }

        // A deletion and an addition with one content hash are a rename; each deletion pairs
        // with at most one addition, in path order.
        let mut changes: Vec<(String, ChangeKind, Option<&String>)> = Vec::new();
        let mut unpaired_deletions = Vec::new();
        for (from, hash) in deleted {
            match added.iter().position(|(_, added_hash)| *added_hash == hash) {
                Some(at) => {
                    let (to, hash) = added.remove(at);
                    changes.push((
                        to.clone(),
                        ChangeKind::Renamed { from: from.clone() },
                        Some(hash),
                    ));
                }
                None => unpaired_deletions.push(from),
            }
        }
        changes.extend(
            added
                .into_iter()
                .map(|(path, hash)| (path.clone(), ChangeKind::Added, Some(hash))),
        );
        changes.extend(
            modified
                .into_iter()
                .map(|(path, hash)| (path.clone(), ChangeKind::Modified, Some(hash))),
        );
        changes.extend(
            unpaired_deletions
                .into_iter()
                .map(|path| (path.clone(), ChangeKind::Deleted, None)),
        );
        changes.sort_by(|a, b| a.0.cmp(&b.0));

        let live: Vec<&str> = changes
            .iter()
            .filter(|(_, _, hash)| hash.is_some())
            .map(|(path, _, _)| path.as_str())
            .collect();
        let generated = generated_paths(&Git::new(root), &live)?;
        changes
            .into_iter()
            .map(|(path, kind, hash)| {
                let content = hash
                    .map(|hash| classify(root, &path, hash, generated.contains(path.as_str())))
                    .transpose()?;
                Ok(Change {
                    path,
                    kind,
                    content,
                })
            })
            .collect()
    }
}

fn nul_separated_paths(raw: &[u8]) -> Result<BTreeSet<String>, Error> {
    raw.split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| {
            String::from_utf8(path.to_vec()).map_err(|_| {
                Error::Format(format!(
                    "path is not UTF-8: {:?}",
                    String::from_utf8_lossy(path)
                ))
            })
        })
        .collect()
}

/// The hash git would store for each file's content (clean filters applied, as the index's are),
/// in order.
fn hash_objects(git: &Git, paths: &[&str]) -> Result<Vec<String>, Error> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    let mut input = String::new();
    for path in paths {
        if path.contains('\n') {
            return Err(Error::Format(format!("path with a newline: {path:?}")));
        }
        input.push_str(path);
        input.push('\n');
    }
    let mut child = git
        .command(&["hash-object", "--stdin-paths"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().expect("stdin was piped");
    // Feed and drain concurrently: a large listing fills the pipe in both directions.
    let output = std::thread::scope(|scope| {
        let writer = scope.spawn(move || stdin.write_all(input.as_bytes()));
        let output = child.wait_with_output();
        writer.join().expect("the writer does not panic")?;
        output
    })?;
    if !output.status.success() {
        return Err(Error::Git {
            args: "hash-object --stdin-paths".to_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_owned)
        .collect())
}

/// Which of `paths` `.gitattributes` marks `linguist-generated` or `-diff`.
fn generated_paths(git: &Git, paths: &[&str]) -> Result<BTreeSet<String>, Error> {
    if paths.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut input = Vec::new();
    for path in paths {
        input.extend_from_slice(path.as_bytes());
        input.push(0);
    }
    let mut child = git
        .command(&["check-attr", "-z", "--stdin", "linguist-generated", "diff"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().expect("stdin was piped");
    let output = std::thread::scope(|scope| {
        let writer = scope.spawn(move || stdin.write_all(&input));
        let output = child.wait_with_output();
        writer.join().expect("the writer does not panic")?;
        output
    })?;
    if !output.status.success() {
        return Err(Error::Git {
            args: "check-attr".to_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    // `path NUL attribute NUL value NUL`, repeated.
    let text = String::from_utf8_lossy(&output.stdout);
    let fields: Vec<&str> = text.split('\0').collect();
    Ok(fields
        .as_chunks::<3>()
        .0
        .iter()
        .filter(|entry| match (entry[1], entry[2]) {
            ("linguist-generated", value) => value == "set" || value == "true",
            ("diff", value) => value == "unset",
            _ => false,
        })
        .map(|entry| entry[0].to_owned())
        .collect())
}

/// Decide Text or Blob for the file at `root/path`, reading it only when it could be Text.
fn classify(
    root: &Path,
    path: &str,
    hash: &str,
    attributed_generated: bool,
) -> Result<Content, Error> {
    let file = root.join(path);
    let size = std::fs::metadata(&file)?.len();
    let name = path.rsplit('/').next().unwrap_or(path);
    let reason = if attributed_generated || LOCKFILES.contains(&name) {
        Some(BlobReason::Generated)
    } else if size > MAX_TEXT_BYTES {
        Some(BlobReason::Large)
    } else {
        None
    };
    let blob = |reason| Content::Blob {
        path: file.clone(),
        size,
        hash: hash.to_owned(),
        reason,
    };
    if let Some(reason) = reason {
        return Ok(blob(reason));
    }
    match String::from_utf8(std::fs::read(&file)?) {
        Ok(text) if !text.contains('\0') => Ok(Content::Text(text)),
        _ => Ok(blob(BlobReason::Binary)),
    }
}
