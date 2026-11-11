//! intxt's capture adapter (ADR 039): turning what an agent harness leaves behind into the work
//! datums of ADR 040.
//!
//! Everything here is observation: it reads a transcript, a worktree or a repository and returns
//! values. Writing them to a node is the caller's business.

//!
//! # Open
//!
//! Known gaps, deferred:
//!
//! - A file renamed and edited in one window is a deletion and an addition: detecting it needs
//!   the old content, which a snapshot (hashes only) does not hold. The recorder may match it
//!   against the previous file document's text.
//! - The stored state holds the whole index listing, which is large for very large repositories.
//! - jj: no `JjObservation`, `JjOperation` or `JjChange` is observed.
//! - Tags: only branches (`refs/heads`) are observed as refs.

pub mod acp;
pub mod git;
pub mod hook;
pub mod host;
pub mod identity;
pub mod observe;
pub mod snapshot;
pub mod transcript;

#[cfg(test)]
mod testing;

/// Why an observation failed. git's own failures carry its stderr; the caller reports them and
/// the next hook retries from the stored state.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("git {args}: {stderr}")]
    Git { args: String, stderr: String },
    /// git printed something this crate cannot read: a path that is not UTF-8 or contains a
    /// newline, or a log line of an unknown shape.
    #[error("{0}")]
    Format(String),
    /// A hook's JSON input lacks a required field or is not JSON.
    #[error("hook input: {0}")]
    Hook(String),
    /// An ACP message a recording cannot follow: not JSON-RPC, a response to a request never
    /// seen, an update outside a prompt turn.
    #[error("acp: {0}")]
    Acp(String),
    #[error(transparent)]
    Transcript(#[from] crate::transcript::ImportError),
    /// A state file is not what this version wrote.
    #[error("state file {path}: {reason}")]
    State { path: String, reason: String },
}
