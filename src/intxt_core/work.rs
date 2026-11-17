//! Work capture (ADR 040): repositories, clones and worktrees; each worktree's single-writer op
//! log and file documents; git and jj objects observed in them; agent sessions, named after the
//! Agent Client Protocol; and the entry datums an intxt uses to include them (ADR 036 §2).
//!
//! The datums live in submodules by concern, and every one is re-exported here: [`repo`] (repositories,
//! clones, worktrees, their op logs, files, and the git and jj objects observed), [`rewrite`],
//! [`github`], [`session`] (agent sessions) and [`entry`] (what an intxt includes). Callers name
//! them as `work::GitCommit`.

mod entry;
mod github;
mod repo;
mod rewrite;
mod session;

pub use entry::*;
pub use github::*;
pub use repo::*;
pub use rewrite::*;
pub use session::*;
