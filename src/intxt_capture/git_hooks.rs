//! git's `post-rewrite` hook in the clones capture records (ADR 039 §3, the membership proposal's
//! item 6): git reports the exact old → new pairs of an amend or a rebase, which no later look at
//! the reflog or at patch ids can recover as reliably.
//!
//! Capture installs a small `sh` script that execs `day x intxt capture git-rewrite`, which reads
//! git's stdin ([`parse_post_rewrite`]) and records the pairs through the host
//! ([`crate::host::prepare_rewrite`]). The script carries [`MARKER`] so a later install
//! recognises its own hook. A `post-rewrite` that is not ours is never overwritten or chained:
//! it is the operator's, and capture says so ([`HookState::ForeignPostRewrite`]) instead.

use crate::Error;
use crate::git::Git;
use intxt_core::capture::RewritePair;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use std::path::PathBuf;

/// The line that marks a `post-rewrite` hook as capture's own.
pub const MARKER: &str = "# intxt-capture: managed post-rewrite hook";

fn script() -> String {
    format!("#!/bin/sh\n{MARKER}\nexec day x intxt capture git-rewrite \"$1\"\n")
}

/// What capture knows of the `post-rewrite` hook of a clone, kept in each worktree's local state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HookState {
    /// The hook is capture's and is in place.
    Installed,
    /// A `post-rewrite` that is not capture's is there, so none was installed and rewrites are
    /// not seen exactly (`add` falls back to patch ids).
    ForeignPostRewrite,
    /// Installing failed (a read-only hooks directory, say). Capture goes on recording and tries
    /// again at its next observation, or when `capture install-git-hooks` is run.
    Failed { reason: String },
}

impl HookState {
    /// The sentence for `day x intxt status`.
    pub fn describe(&self) -> String {
        match self {
            Self::Installed => "post-rewrite installed".to_owned(),
            Self::ForeignPostRewrite => "hook not installed: foreign post-rewrite".to_owned(),
            Self::Failed { reason } => format!("hook install failed: {reason}"),
        }
    }

    /// Whether capture should look at the hook again: it has not settled (never looked, or the
    /// last install failed).
    pub fn needs_install(state: &Option<Self>) -> bool {
        matches!(state, None | Some(Self::Failed { .. }))
    }
}

/// What [`install_post_rewrite`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Install {
    /// Wrote the hook (none was there, or ours was out of date).
    Installed(PathBuf),
    /// Ours was already in place; nothing changed.
    AlreadyInstalled(PathBuf),
    /// A `post-rewrite` that is not ours is at the path; nothing was written.
    Foreign(PathBuf),
}

impl Install {
    pub fn state(&self) -> HookState {
        match self {
            Self::Installed(_) | Self::AlreadyInstalled(_) => HookState::Installed,
            Self::Foreign(_) => HookState::ForeignPostRewrite,
        }
    }
}

/// Installs capture's `post-rewrite` hook in the clone of the worktree at `root`.
///
/// The hooks directory is `git rev-parse --git-path hooks`, which follows `core.hooksPath` and is
/// the clone's own directory shared by all its linked worktrees. Installing is idempotent: our
/// hook is rewritten only when its text differs, and made executable if it is not.
pub fn install_post_rewrite(root: &Path) -> Result<Install, Error> {
    let hooks = PathBuf::from(Git::new(root).text(&[
        "rev-parse",
        "--path-format=absolute",
        "--git-path",
        "hooks",
    ])?);
    let path = hooks.join("post-rewrite");
    let wanted = script();
    match std::fs::read(&path) {
        Ok(existing) => {
            let existing = String::from_utf8_lossy(&existing);
            if !existing.lines().any(|line| line == MARKER) {
                return Ok(Install::Foreign(path));
            }
            if existing == wanted {
                make_executable(&path)?;
                return Ok(Install::AlreadyInstalled(path));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::create_dir_all(&hooks)?;
    // Through a temporary file in the same directory, so git never runs a half-written hook.
    let temporary = hooks.join("post-rewrite.intxt-tmp");
    std::fs::write(&temporary, wanted)?;
    make_executable(&temporary)?;
    std::fs::rename(&temporary, &path)?;
    Ok(Install::Installed(path))
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = std::fs::metadata(path)?.permissions();
    if permissions.mode() & 0o111 != 0o111 {
        permissions.set_mode(permissions.mode() | 0o755);
        std::fs::set_permissions(path, permissions)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_: &Path) -> Result<(), Error> {
    Ok(())
}

/// The pairs of `post-rewrite`'s stdin: one `<old> <new>[ <extra>]` per line. A pair whose two
/// ids are equal is no rewrite and is dropped; a line that is not two object ids is an error,
/// since the hook's input is git's and a different shape is a programming error.
pub fn parse_post_rewrite(input: &str) -> Result<Vec<RewritePair>, Error> {
    let mut pairs = Vec::new();
    for line in input.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.split_whitespace();
        let (Some(old), Some(new)) = (fields.next(), fields.next()) else {
            return Err(Error::Format(format!(
                "post-rewrite input line {line:?} is not `<old> <new>`"
            )));
        };
        for id in [old, new] {
            if !matches!(id.len(), 40 | 64) || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(Error::Format(format!(
                    "post-rewrite input line {line:?}: {id:?} is not an object id"
                )));
            }
        }
        if old != new {
            pairs.push(RewritePair {
                old: old.to_ascii_lowercase(),
                new: new.to_ascii_lowercase(),
                extra: Default::default(),
            });
        }
    }
    Ok(pairs)
}

#[cfg(test)]
mod tests;
