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

pub use entry::CommitsDoc;
pub use entry::GitCommitEntry;
pub use entry::GitRefEntry;
pub use entry::GithubPullEntry;
pub use entry::JjChangeEntry;
pub use entry::RepoEntry;
pub use entry::SessionEntry;
pub use entry::WorktreeEntry;
pub use github::GithubPull;
pub use github::PullState;
pub use repo::CommitFile;
pub use repo::File;
pub use repo::FileStatus;
pub use repo::GitClone;
pub use repo::GitCommit;
pub use repo::GitIdent;
pub use repo::GitObservation;
pub use repo::GitRef;
pub use repo::GitRepo;
pub use repo::JjChange;
pub use repo::JjObservation;
pub use repo::JjOperation;
pub use repo::OpKind;
pub use repo::OpLogDoc;
pub use repo::Vcs;
pub use repo::Worktree;
pub use repo::WorktreeOp;
pub use rewrite::Rewrite;
pub use rewrite::RewriteHow;
pub use rewrite::RewrittenFrom;
pub use session::AgentSession;
pub use session::Message;
pub use session::MessageKind;
pub use session::Plan;
pub use session::PlanEntry;
pub use session::Priority;
pub use session::PromptTurn;
pub use session::StopReason;
pub use session::ToolCall;
pub use session::ToolDiff;
pub use session::ToolKind;
pub use session::ToolLocation;
pub use session::ToolStatus;
