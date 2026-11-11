//! Work capture (ADR 040): repositories, clones and worktrees; each worktree's single-writer op
//! log and file documents; git and jj objects observed in them; agent sessions, named after the
//! Agent Client Protocol; and the entry datums an intxt uses to include them (ADR 036 §2).

use big_cabinet_core::ActorUrl;
use big_cabinet_core::DatumReference;
use big_cabinet_core::Location;
use big_cabinet_core::datum::Extra;
use jiff::Timestamp;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

/// A project, as far as recorders can tell (`gitRepo/<uuid>`): git has no repository identity,
/// so the first recorder mints a UUID. Shared root commits only suggest that two clones are one
/// project; they never merge repos.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitRepo {
    /// Root commit shas, for matching clones on other machines.
    pub roots: Vec<String>,
    /// Remote URLs; fetch hints, never identity.
    pub remotes: Vec<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// One `.git` directory or jj repository on one machine (`gitClone/<uuid>`, the UUID kept in
/// `.git/intxt-id`). Refs and the jj op log live here.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitClone {
    /// The `gitRepo` datum.
    pub repo: DatumReference,
    /// The node holding the clone.
    pub machine: ActorUrl,
    /// Absolute path of the git directory; a hint, not identity.
    pub path: String,
    pub vcs: Vec<Vcs>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Vcs {
    #[serde(rename = "git")]
    Git,
    #[serde(rename = "jj")]
    Jj,
    #[datum(other)]
    Unknown(String),
}

/// One checkout directory of a clone (`worktree/main`, one per worktree document). Written only
/// by the recorder on its machine.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Worktree {
    /// The `gitClone` datum.
    pub clone: DatumReference,
    /// The UUID kept in the worktree's own git directory.
    pub id: String,
    /// Absolute path of the checkout; a hint, not identity.
    pub path: String,
    /// Path → file document id, for every path an op touched.
    pub files: BTreeMap<String, String>,
    /// The op-log documents, in sequence order.
    pub op_logs: Vec<OpLogDoc>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// One op-log document and the first sequence number it holds; it holds every op up to the next
/// document's `from`.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpLogDoc {
    pub doc: String,
    pub from: u64,
    #[serde(flatten)]
    pub extra: Extra,
}

/// One observed change in a worktree (`worktreeOp/<seq>`). Sequential keys from one writer make
/// the log linear by construction, like jj's operation log.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeOp {
    pub at: Timestamp,
    pub kind: OpKind,
    /// The tool call whose window the change fell in, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<Location>,
    /// Path → the file document's heads after this op (multibase text, hash-byte order, as
    /// `big_cabinet_core::reference::head_text` spells them).
    pub files: BTreeMap<String, Vec<String>>,
    /// Paths this op deleted. Their documents keep the earlier states.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<String>,
    /// New path → old path, for renames this op continued in the old path's document.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub renames: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git: Option<GitObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jj: Option<JjObservation>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// What an op is. Ref moves are spelled after git's own reflog account of them. A change with no
/// known cause is an `edit` without `cause`, not a kind of its own.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OpKind {
    #[serde(rename = "edit")]
    Edit,
    #[serde(rename = "checkout")]
    Checkout,
    #[serde(rename = "reset")]
    Reset,
    #[serde(rename = "commit")]
    Commit,
    #[serde(rename = "amend")]
    Amend,
    #[serde(rename = "rebase")]
    Rebase,
    #[serde(rename = "refMove")]
    RefMove,
    #[serde(rename = "jjSnapshot")]
    JjSnapshot,
    #[datum(other)]
    Unknown(String),
}

/// Git state an op observed: `HEAD` after it, and the ref it moved.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitObservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    #[serde(rename = "ref", default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    /// The ref's target before the move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// The ref's target after the move.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// Git's reflog message for the move, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reflog: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The jj operation and change an op observed, when the clone is a jj repository.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JjObservation {
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Provenance of a file document (`file/main`), beside the generic `path` and `note`.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct File {
    /// The `worktree` datum.
    pub worktree: DatumReference,
    /// The `Blob` datum, when the file is binary or too large to hold as text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<DatumReference>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A git commit (`gitCommit/<sha>`). Every recorder writes the same value to the same slot, and
/// none rewrites it: a later fact about a commit goes into its own datum. Holds no pins; the op
/// that observed the commit does.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitCommit {
    pub parents: Vec<String>,
    /// The jj change id, when the commit is known to jj.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    pub author: GitIdent,
    pub committer: GitIdent,
    pub message: String,
    pub files: Vec<CommitFile>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A commit's author or committer line, as git records it. It is a claim the content makes, not
/// who wrote the datum.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitIdent {
    pub name: String,
    pub email: String,
    pub at: Timestamp,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A path a commit changed relative to its first parent.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommitFile {
    pub path: String,
    pub status: FileStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_path: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FileStatus {
    #[serde(rename = "added")]
    Added,
    #[serde(rename = "modified")]
    Modified,
    #[serde(rename = "deleted")]
    Deleted,
    #[serde(rename = "renamed")]
    Renamed,
    #[serde(rename = "copied")]
    Copied,
    #[datum(other)]
    Unknown(String),
}

/// A ref of one clone (`gitRef/<clone uuid>/<ref name>`). One writer: the clone's recorder.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GitRef {
    pub target: String,
    /// What a review compares against: the merge base with the upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A jj operation (`jjOperation/<op id>`): ids and structure, not the op store's contents.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JjOperation {
    pub parents: Vec<String>,
    pub description: String,
    pub at: Timestamp,
    pub heads: Vec<String>,
    pub bookmarks: BTreeMap<String, String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// A jj change's evolution (`jjChange/<change id>`): the commits it has been, in evolog order.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct JjChange {
    pub commits: Vec<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An agent session (`agentSession/main`, one per session document), named after ACP's session.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AgentSession {
    /// `claude-code`, `opencode`, …
    pub harness: String,
    /// The harness's own session id.
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// ACP's root set: `cwd` first, then the additional directories.
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_directories: Vec<String>,
    /// The spawning or continued session. ACP has no subagents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<DatumReference>,
    #[serde(flatten)]
    pub extra: Extra,
}

/// One prompt turn (`promptTurn/<harness turn id>`): a prompt and everything until its stop
/// reason. Messages and tool calls live inside it, appended as the turn runs.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PromptTurn {
    pub seq: u64,
    pub at: Timestamp,
    /// The turn this one follows, so rewinds and forks form a tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<StopReason>,
    pub messages: Vec<Message>,
    pub tool_calls: Vec<ToolCall>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum StopReason {
    #[serde(rename = "end_turn")]
    EndTurn,
    #[serde(rename = "max_tokens")]
    MaxTokens,
    #[serde(rename = "max_turn_requests")]
    MaxTurnRequests,
    #[serde(rename = "refusal")]
    Refusal,
    #[serde(rename = "cancelled")]
    Cancelled,
    #[datum(other)]
    Unknown(String),
}

/// A message in a turn. `seq` orders messages and tool calls together within the turn.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub seq: u64,
    pub at: Timestamp,
    pub kind: MessageKind,
    pub text: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// Who a message is from. Records the harness makes on its own (notifications, local commands,
/// interrupts, prompts it gives a subagent) are not the user's.
#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageKind {
    #[serde(rename = "userMessage")]
    UserMessage,
    #[serde(rename = "agentMessage")]
    AgentMessage,
    #[serde(rename = "agentThought")]
    AgentThought,
    #[serde(rename = "notification")]
    Notification,
    #[serde(rename = "localCommand")]
    LocalCommand,
    #[serde(rename = "interrupt")]
    Interrupt,
    #[serde(rename = "spawnPrompt")]
    SpawnPrompt,
    /// Text the harness injected that the model saw: reminders and records it marks as meta.
    #[serde(rename = "meta")]
    Meta,
    /// A message from another agent session.
    #[serde(rename = "crossSession")]
    CrossSession,
    #[serde(rename = "compactionSummary")]
    CompactionSummary,
    #[datum(other)]
    Unknown(String),
}

/// A tool call, named as ACP names it.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ToolCall {
    pub id: String,
    pub seq: u64,
    pub title: String,
    pub kind: ToolKind,
    pub status: ToolStatus,
    /// The working directory the call ran in, when it differs from the session's `cwd`: what
    /// resolves a shell call to the worktree it changed (ADR 039 §3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<ToolLocation>,
    /// The changes the harness reported, as unified patches. The worktree ops this call caused
    /// are found through their `cause`, not listed here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diffs: Vec<ToolDiff>,
    /// The output as the harness showed it to the model, stored whole, truncation the harness
    /// applied included and none of ours added (ledger D37).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ToolKind {
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "edit")]
    Edit,
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "move")]
    Move,
    #[serde(rename = "search")]
    Search,
    #[serde(rename = "execute")]
    Execute,
    #[serde(rename = "think")]
    Think,
    #[serde(rename = "fetch")]
    Fetch,
    #[serde(rename = "switch_mode")]
    SwitchMode,
    #[serde(rename = "other")]
    Other,
    #[datum(other)]
    Unknown(String),
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ToolStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "in_progress")]
    InProgress,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "failed")]
    Failed,
    #[datum(other)]
    Unknown(String),
}

/// A path a tool call touched; absolute, as ACP gives it, and an optional 1-based line.
#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ToolLocation {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ToolDiff {
    pub path: String,
    pub patch: String,
    #[serde(flatten)]
    pub extra: Extra,
}

/// The agent's plan (`plan/main`), replaced whole on every update as ACP does; its history is
/// the plan's history.
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Plan {
    pub entries: Vec<PlanEntry>,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PlanEntry {
    pub content: String,
    pub priority: Priority,
    pub status: crate::annotation::Progress,
    #[serde(flatten)]
    pub extra: Extra,
}

#[macros::datum(open_string)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Priority {
    #[serde(rename = "high")]
    High,
    #[serde(rename = "medium")]
    Medium,
    #[serde(rename = "low")]
    Low,
    #[datum(other)]
    Unknown(String),
}

/// An intxt includes a repository (`repoEntry/<repo uuid>`, on the intxt's descriptor).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RepoEntry {
    pub repo: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt includes a worktree's capture (`worktreeEntry/<worktree uuid>`).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeEntry {
    pub worktree: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}

/// An intxt includes an agent session (`sessionEntry/<session id>`).
#[macros::datum(authority = "org.example.intxt.work")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionEntry {
    pub session: DatumReference,
    #[serde(flatten)]
    pub extra: Extra,
}
