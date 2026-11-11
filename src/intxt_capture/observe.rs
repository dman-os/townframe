//! One observation of a worktree: its identity, what changed in its files since the last look,
//! and what its git state did (ADR 039 §3 and §4).
//!
//! The caller (a hook's `day` command) holds the per-worktree lock, loads the stored
//! [`WorktreeState`], calls [`observe`], records the [`Observation`] and stores the returned
//! state, in that order, so a failed recording is observed again by the next hook.

use crate::Error;
use crate::git;
use crate::git::GitObserved;
use crate::git::GitState;
use crate::identity;
use crate::identity::Identity;
use crate::snapshot::Change;
use crate::snapshot::Snapshot;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;

/// What capture remembers about one worktree between hooks: the last snapshot, and the git state
/// (reflog position, `HEAD` and refs).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorktreeState {
    snapshot: Option<Snapshot>,
    git: GitState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    pub identity: Identity,
    /// What changed in the files since the last observation, by path. On the first observation
    /// of a worktree: what differs from the index.
    pub changes: Vec<Change>,
    pub git: GitObserved,
}

/// Observe the worktree containing `root` against what was `state` at the last observation.
/// `None` when `root` is in no worktree.
pub fn observe(
    state: &WorktreeState,
    root: &Path,
) -> Result<Option<(Observation, WorktreeState)>, Error> {
    let Some(identity) = identity::resolve(root)? else {
        return Ok(None);
    };
    let snapshot = Snapshot::take(&identity.root)?;
    let changes = match &state.snapshot {
        Some(before) => before.diff(&snapshot, &identity.root)?,
        // First sight: what already differs from the index is a change, so its file docs exist
        // from the start.
        None => snapshot.clean().diff(&snapshot, &identity.root)?,
    };
    let (git, git_state) = git::observe(
        &identity.root,
        &identity.git_dir,
        &identity.common_dir,
        &state.git,
    )?;
    Ok(Some((
        Observation {
            identity,
            changes,
            git,
        },
        WorktreeState {
            snapshot: Some(snapshot),
            git: git_state,
        },
    )))
}

#[cfg(test)]
mod tests;
