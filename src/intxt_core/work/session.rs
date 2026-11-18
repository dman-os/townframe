use big_cabinet_core::DatumReference;
use big_cabinet_core::datum::Extra;
use jiff::Timestamp;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;

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
