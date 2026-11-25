//! What a capture hook observed, as the recorder takes it (ADR 039 §4), and what the recorder
//! answers. The host half (`intxt_capture`) builds a [`Batch`]; the client library's recorder
//! (`client::Caller::record_observations`) turns it into the datums of ADR 040 and answers
//! [`RecordedIds`]. They are not datums themselves, but the host persists them and a newer host's
//! batch is carried, not refused, so they are open objects and open string vocabularies like the
//! datums are, and hold no nullable map values.
//!
//! A path's content in a [`Change`] is a git blob id (ADR 040 §3, §4). No bytes travel: the host
//! wrote the object to the clone's store and keeps it reachable under its own ref.

use crate::work::AgentSession;
use crate::work::GitCommit;
use crate::work::GitObservation;
use crate::work::GitRef;
use crate::work::OpKind;
use crate::work::Plan;
use crate::work::PromptTurn;
use big_cabinet_core::datum::Extra;
use jiff::Timestamp;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

/// Everything one hook invocation observed.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    pub session: SessionBatch,
    /// Only worktrees with something to record.
    pub worktrees: Vec<WorktreeBatch>,
    /// Ops of a post hook whose tool call the transcript did not show yet, so they carry no cause
    /// (and are not retried). For the CLI to report.
    pub uncaused_tool_calls: u64,
    #[serde(flatten)]
    pub extra: Extra,
}

impl Batch {
    /// Nothing was observed: the hook can commit without recording.
    pub fn is_empty(&self) -> bool {
        self.session.imported.is_empty()
            && self
                .session
                .subagents
                .iter()
                .all(|agent| agent.imported.is_empty())
            && self.worktrees.is_empty()
    }
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionBatch {
    pub session_id: String,
    /// The session's document, when the host holds its id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    pub imported: Imported,
    pub subagents: Vec<SubagentBatch>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A subagent's session, imported.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubagentBatch {
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    pub imported: Imported,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What a transcript import changed; every datum is its full current value.
#[macros::datum]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Imported {
    /// The `agentSession`, when the import created it or changed its title.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<AgentSession>,
    /// Turn id (the slot of `promptTurn/<id>`) → the turn, for every turn the import touched.
    pub turns: BTreeMap<String, PromptTurn>,
    /// The latest plan of the import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<Plan>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl Imported {
    pub fn is_empty(&self) -> bool {
        self.session.is_none() && self.turns.is_empty() && self.plan.is_none()
    }
}

/// Everything observed in one worktree by one hook.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeBatch {
    pub identity: WorktreeIdentity,
    /// The documents the host already holds for the worktree.
    pub docs: WorktreeDocs,
    /// The sequence number of `ops[0]`.
    pub next_seq: u64,
    pub ops: Vec<Op>,
    /// sha → commit.
    pub commits: BTreeMap<String, GitCommit>,
    /// Ref name → ref.
    pub refs: BTreeMap<String, GitRef>,
    /// The reflog lost entries since the last observation; the last op is a synthetic move.
    pub reflog_gap: bool,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A worktree and what it belongs to, as the host found them in the git directories.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeIdentity {
    /// The checkout directory; also the key the recorder answers this worktree under.
    pub root: String,
    /// The clone's `.git` directory.
    pub clone_path: String,
    /// `gitClone/<uuid>`.
    pub clone_id: String,
    /// The UUID of the worktree.
    pub worktree_id: String,
    pub remotes: Vec<String>,
    /// The repository's root commits; empty before the first commit.
    pub roots: Vec<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The document ids a recorder holds for a worktree; none before it first recorded one.
#[macros::datum]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeDocs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// One op of a worktree's log, without its sequence number (the recorder numbers them).
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Op {
    pub at: Timestamp,
    pub kind: OpKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Cause>,
    pub changes: Vec<Change>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitObservation>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Which tool call a change belongs to, symbolically. The recorder turns it into a `Location`
/// once it knows the session document: the call is `toolCalls[id]` of `promptTurn/<turn>`.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Cause {
    /// The subagent whose session holds the call; none for the main session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The turn holding the call. None when the transcript did not yet have the call at this
    /// hook (the harness writes it concurrently).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<String>,
    pub tool_use_id: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What happened to a path between two looks at the worktree.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Change {
    /// The path after the change (the old path of a deletion), relative to the worktree root.
    pub path: String,
    pub kind: ChangeKind,
    /// The git blob id of the content after the change; none for a deletion. The host has
    /// written the object to the clone's store and keeps it reachable (ADR 040 §4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Renames are not a kind: a reader pairs deletions and additions with `git diff -M` (ADR 040 §3).
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    #[serde(rename = "added")]
    Added,
    #[serde(rename = "modified")]
    Modified,
    #[serde(rename = "deleted")]
    Deleted,
    #[datum(other)]
    Unknown(String),
}

/// What the recorder answers: the document ids it created or knows, and the next sequence number
/// of each worktree it wrote ops to.
#[macros::datum]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RecordedIds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    /// Agent id → the subagent session's document.
    pub subagents: BTreeMap<String, String>,
    /// Worktree root → what was recorded for it.
    pub worktrees: BTreeMap<String, RecordedWorktree>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RecordedWorktree {
    pub docs: WorktreeDocs,
    pub next_seq: u64,
    #[serde(flatten)]
    pub extra: Extra,
}
