//! What differs from the index, and what changed between two looks at it (ADR 039 §3).
//!
//! A snapshot is the files that differ from the git index (modified, deleted, and untracked but
//! not ignored), each with the id of the blob git stores for its content, plus the index listing.
//! Taking one writes those blobs to the clone's object store (`git hash-object -w`), so the ids
//! it holds always name content that exists; keeping them from `git gc` is the host's business
//! ([`crate::git::Git::keep`]). The difference of two snapshots is what happened in between to
//! every path: whatever a tool call did, an edit tool's write, a formatter, a checkout, a
//! `git add`. On a repository the size of this one a snapshot costs about 25 ms because only
//! dirty files are hashed.
//!
//! A path's content is its dirty blob when it has one, else its index entry. So a file that is
//! committed or staged after being edited, without changing again, is not a change: the commit
//! is observed through the reflog instead (see [`crate::git`]).

use crate::Error;
use crate::git::Git;
use intxt_core::capture::Change;
use intxt_core::capture::ChangeKind;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Path → blob id, for every path in the index (merge conflicts excluded).
    index: BTreeMap<String, String>,
    /// Path → blob id for every path that differs from the index; `None` is a path the
    /// index has and the worktree lacks.
    dirty: BTreeMap<String, Option<String>>,
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

    /// The state of a worktree that matches the tree of `HEAD` exactly. The first observation of
    /// a worktree diffs against it, so what already differs from `HEAD`, staged or not, is
    /// reported as changes: a path's content at an op is then the latest op that lists it, else
    /// `HEAD`'s (ADR 040 §3).
    pub(crate) fn at_head(root: &Path) -> Result<Snapshot, Error> {
        let git = Git::new(root);
        let mut index = BTreeMap::new();
        // An unborn branch has no tree: everything in the worktree is new.
        if git
            .ask(&["rev-parse", "--verify", "--quiet", "HEAD"])?
            .is_some()
        {
            for entry in String::from_utf8_lossy(&git.run(&["ls-tree", "-r", "-z", "HEAD"])?)
                .split('\0')
                .filter(|e| !e.is_empty())
            {
                // `<mode> SP <type> SP <blob> TAB <path>`
                let (meta, path) = entry
                    .split_once('\t')
                    .ok_or_else(|| Error::Format(format!("ls-tree entry {entry:?}")))?;
                let blob = meta
                    .rsplit(' ')
                    .next()
                    .ok_or_else(|| Error::Format(format!("ls-tree entry {entry:?}")))?;
                index.insert(path.to_owned(), blob.to_owned());
            }
        }
        Ok(Snapshot {
            index,
            dirty: BTreeMap::new(),
        })
    }

    /// A path's content hash, `None` when it does not exist.
    fn hash(&self, path: &str) -> Option<&String> {
        match self.dirty.get(path) {
            Some(hash) => hash.as_ref(),
            None => self.index.get(path),
        }
    }

    /// What changed from `self` to `after`, sorted by path. Renames are not paired here: a
    /// reader pairs deletions and additions with `git diff -M` (ADR 040 §3).
    pub fn diff(&self, after: &Snapshot) -> Vec<Change> {
        let mut candidates: BTreeSet<&String> =
            self.dirty.keys().chain(after.dirty.keys()).collect();
        for path in self.index.keys().chain(after.index.keys()) {
            if self.index.get(path) != after.index.get(path) {
                candidates.insert(path);
            }
        }
        candidates
            .into_iter()
            .filter_map(|path| {
                let (kind, blob) = match (self.hash(path), after.hash(path)) {
                    (before, now) if before == now => return None,
                    (None, Some(now)) => (ChangeKind::Added, Some(now.clone())),
                    (Some(_), Some(now)) => (ChangeKind::Modified, Some(now.clone())),
                    (Some(_), None) => (ChangeKind::Deleted, None),
                    (None, None) => unreachable!("equal hashes were skipped"),
                };
                Some(Change {
                    path: path.clone(),
                    kind,
                    blob,
                    extra: Default::default(),
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

/// The blob id git stores for each file's content (clean filters applied, as the index's are),
/// in order, writing the objects to the store.
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
    let output = git.run_with_input(
        &["hash-object", "-w", "--stdin-paths"],
        &[],
        input.as_bytes(),
    )?;
    Ok(String::from_utf8_lossy(&output)
        .lines()
        .map(str::to_owned)
        .collect())
}
