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
use crate::identity::Identity;
use crate::snapshot::Snapshot;
use intxt_core::capture::Change;
use serde::Deserialize;
use serde::Serialize;

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
    /// What changed in the files in the window being observed, by path. On the first observation
    /// of a worktree: what differs from the index.
    pub changes: Vec<Change>,
    /// What changed between the last observation and the start of the window, when a `pre`
    /// snapshot was given: changes nothing in the window caused.
    pub earlier: Vec<Change>,
    pub git: GitObserved,
}

/// Observe the worktree `identity` names against what was `state` at the last observation.
///
/// `pre` is a snapshot taken when a window opened (before a tool call ran). With it, `changes`
/// are the window's (`pre` to now) and `earlier` are what happened between the last observation
/// and `pre`; without it all changes are in `changes`.
pub fn observe(
    state: &WorktreeState,
    identity: Identity,
    pre: Option<&Snapshot>,
) -> Result<(Observation, WorktreeState), Error> {
    let snapshot = Snapshot::take(&identity.root)?;
    let last = state.snapshot.as_ref();
    // First sight: what already differs from the index is a change, so its file docs exist from
    // the start.
    let clean;
    let (earlier, changes) = match (last, pre) {
        (Some(last), Some(pre)) => (
            last.diff(pre, &identity.root)?,
            pre.diff(&snapshot, &identity.root)?,
        ),
        (None, Some(pre)) => {
            clean = pre.clean();
            (Vec::new(), clean.diff(&snapshot, &identity.root)?)
        }
        (Some(last), None) => (Vec::new(), last.diff(&snapshot, &identity.root)?),
        (None, None) => {
            clean = snapshot.clean();
            (Vec::new(), clean.diff(&snapshot, &identity.root)?)
        }
    };
    let (git, git_state) = git::observe(
        &identity.root,
        &identity.git_dir,
        &identity.common_dir,
        &state.git,
    )?;
    Ok((
        Observation {
            identity,
            changes,
            earlier,
            git,
        },
        WorktreeState {
            snapshot: Some(snapshot),
            git: git_state,
        },
    ))
}

#[cfg(test)]
mod tests;
