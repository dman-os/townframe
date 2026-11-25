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
    /// What changed in the files since the last observation, cut at every boundary snapshot given
    /// to [`observe`]: one entry per boundary, in order (the changes before it and after the
    /// previous one), each possibly empty. On the first observation of a worktree these are
    /// empty and `changes` holds everything that differs from `HEAD`.
    pub earlier: Vec<Vec<Change>>,
    /// What changed after the last boundary (or since the last observation, with none).
    pub changes: Vec<Change>,
    pub git: GitObserved,
}

/// Observe the worktree `identity` names against what was `state` at the last observation.
///
/// `boundaries` are the snapshots taken when windows opened (before tool calls ran) after the
/// last observation, in the order they were taken. The time since the last observation is cut
/// there: last observation -> boundary 1 -> ... -> boundary n -> now, one diff per segment, every
/// diff forward in time. Windows that overlap would otherwise make a diff run backwards (the
/// last observation can be later than a still-open window's snapshot) and replay changes the
/// log already holds, so the caller leaves out the snapshots at or before the last observation.
/// Without boundaries all changes are in `changes`.
///
/// `import` are commits the git observation reports although no reflog move made them
/// ([`git::observe`]).
pub fn observe(
    state: &WorktreeState,
    identity: Identity,
    boundaries: &[&Snapshot],
    import: &[String],
) -> Result<(Observation, WorktreeState), Error> {
    let snapshot = Snapshot::take(&identity.root)?;
    let mut earlier = Vec::with_capacity(boundaries.len());
    let changes = match state.snapshot.as_ref() {
        Some(last) => {
            let mut from = last;
            for boundary in boundaries {
                earlier.push(from.diff(boundary));
                from = boundary;
            }
            from.diff(&snapshot)
        }
        None => {
            // First sight: what already differs from `HEAD` is a change, so every path an op
            // does not list has its content in `HEAD`'s tree. There is no earlier segment to cut.
            earlier.resize(boundaries.len(), Vec::new());
            Snapshot::at_head(&identity.root)?.diff(&snapshot)
        }
    };
    let (git, git_state) = git::observe(
        &identity.root,
        &identity.git_dir,
        &identity.common_dir,
        &state.git,
        import,
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
