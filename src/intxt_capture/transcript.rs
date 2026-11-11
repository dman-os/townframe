//! Claude Code transcripts into ADR 040's session datums: the `agentSession`, one `promptTurn`
//! per turn, and the latest `plan`.
//!
//! A transcript is JSONL, one record per line, appended as the session runs. A
//! [`TranscriptImporter`] reads it incrementally: [`TranscriptImporter::import`] takes the bytes
//! after [`TranscriptImporter::offset`], consumes whole lines only (a partial last line waits for
//! the next call) and returns the datums that changed. The importer serializes, so a hook process
//! persists it between invocations. Importing in any chunking yields the same datums as one call.
//!
//! One importer reads one transcript file. A subagent's transcript is its own file and its own
//! session (ADR 039, ledger D30): construct its importer with the `parent` reference, which the
//! caller resolves because only it knows the parent session's document. The parent's importer
//! reports the subagents it spawned in [`Imported::spawned`].
//!
//! Turns. A user message that is the user's own, or the first prompt of a subagent, starts a
//! turn; everything until the next such message belongs to it, including the harness's own
//! records (notifications, local commands, tool results). Records before the first prompt open a
//! turn named after their own uuid. A turn's `parentTurn` is the turn of the record its first
//! record descends from, so rewinds and branches show as a tree.
//!
//! Tool calls take the ACP vocabulary. The output the harness showed the model is stored whole in
//! the call's `output` (D37).

use big_cabinet_core::DatumReference;
use big_cabinet_core::datum::Extra;
use intxt_core::annotation::Progress;
use intxt_core::work::AgentSession;
use intxt_core::work::Message;
use intxt_core::work::MessageKind;
use intxt_core::work::Plan;
use intxt_core::work::PlanEntry;
use intxt_core::work::Priority;
use intxt_core::work::PromptTurn;
use intxt_core::work::StopReason;
use intxt_core::work::ToolCall;
use intxt_core::work::ToolDiff;
use intxt_core::work::ToolKind;
use intxt_core::work::ToolLocation;
use intxt_core::work::ToolStatus;
use jiff::Timestamp;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

#[cfg(test)]
mod tests;

/// Why an import failed. The importer's state is unspecified after an error: discard it and
/// resume from the last persisted state.
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("reading the transcript: {0}")]
    Io(#[from] std::io::Error),
    #[error("the transcript is {len} bytes, shorter than the offset {offset} already imported")]
    Shrank { len: u64, offset: u64 },
    #[error("transcript line at byte {at}: {reason}")]
    Line { at: u64, reason: String },
    #[error("transcript record {uuid}: {reason}")]
    Record { uuid: String, reason: String },
}

/// What one [`TranscriptImporter::import`] call changed; every datum is its full current value.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Imported {
    /// The importer's offset after the call: the end of the last whole line consumed.
    pub offset: u64,
    /// The `agentSession`, when this call created it or changed its title.
    pub session: Option<AgentSession>,
    /// Turn id (the key of `promptTurn/<id>`) → the turn, for every turn this call touched.
    pub turns: BTreeMap<String, PromptTurn>,
    /// The latest `TodoWrite` of this call.
    pub plan: Option<Plan>,
    /// Ids of the subagents whose calls returned in this call; each has a transcript
    /// `agent-<id>.jsonl` of its own, and its session id is `agent-<id>`.
    pub spawned: Vec<String>,
}

/// Importer of one Claude Code transcript. All state between calls is here and serializes.
///
/// It keeps a turn's value only while the turn can still change (it is the current turn, or a
/// call of it awaits its result); a closed turn has been emitted whole and is dropped, so the
/// persisted state stays small however large the tool outputs were. The uuid-to-turn map stays,
/// for `parentTurn` and duplicate detection.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TranscriptImporter {
    offset: u64,
    parent: Option<DatumReference>,
    /// Whether `thinking` blocks become `agentThought` messages (FDR 007: off unless asked).
    record_thinking: bool,
    title: Option<String>,
    session: Option<AgentSession>,
    /// Turns started so far; the next turn's `seq`.
    turn_count: u64,
    /// Only the turns that can still change: the current one and those with a call awaiting its
    /// result. A turn leaving here has been emitted in its final form.
    turns: BTreeMap<String, PromptTurn>,
    current_turn: Option<String>,
    /// Record uuid → the turn it belongs to, for every record seen. Resolves `parentTurn`, and a
    /// uuid already here is a duplicate (a resumed session copies earlier records) and skipped.
    turn_of: BTreeMap<String, Option<String>>,
    /// Tool calls issued and not yet answered.
    pending: BTreeMap<String, Pending>,
    #[serde(skip)]
    dirty: BTreeSet<String>,
    #[serde(skip)]
    out: Imported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pending {
    turn: String,
    /// The tool name and input of an edit call, from which its diffs are built when the result
    /// arrives.
    edit: Option<(String, Value)>,
}

// ---- the transcript's records --------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    #[serde(rename = "type")]
    kind: String,
    uuid: Option<String>,
    parent_uuid: Option<String>,
    timestamp: Option<Timestamp>,
    cwd: Option<String>,
    session_id: Option<String>,
    agent_id: Option<String>,
    #[serde(default)]
    is_meta: bool,
    #[serde(default)]
    is_compact_summary: bool,
    #[serde(default)]
    is_sidechain: bool,
    ai_title: Option<String>,
    message: Option<ApiMessage>,
    tool_use_result: Option<Value>,
}

#[derive(Deserialize)]
struct ApiMessage {
    id: Option<String>,
    content: Option<Content>,
    stop_reason: Option<StopReason>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Blocks(Vec<Block>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Block {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: Value,
        #[serde(default)]
        is_error: bool,
    },
    #[serde(other)]
    Other,
}

/// One hunk of a `structuredPatch` in an Edit or Write result.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Hunk {
    old_start: u64,
    old_lines: u64,
    new_start: u64,
    new_lines: u64,
    lines: Vec<String>,
}

#[derive(Deserialize)]
struct Todo {
    content: String,
    status: Progress,
}

// ---- the importer ---------------------------------------------------------------------------

impl TranscriptImporter {
    /// An importer at offset 0. `parent` is the spawning session for a subagent's transcript.
    pub fn new(parent: Option<DatumReference>) -> Self {
        Self {
            parent,
            ..Self::default()
        }
    }

    /// Record the model's thinking as `agentThought` messages. Off by default; the choice is
    /// part of the state, so a resumed import keeps it.
    #[must_use]
    pub fn recording_thinking(mut self) -> Self {
        self.record_thinking = true;
        self
    }

    /// The byte offset of the next unconsumed line; [`Self::import`] takes the file's bytes from
    /// here.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Read the transcript at `path` from [`Self::offset`] and import it.
    pub fn import_file(&mut self, path: &Path) -> Result<Imported, ImportError> {
        let mut file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        if len < self.offset {
            return Err(ImportError::Shrank {
                len,
                offset: self.offset,
            });
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut tail = Vec::with_capacity((len - self.offset) as usize);
        file.read_to_end(&mut tail)?;
        self.import(&tail)
    }

    /// Import `tail`, the transcript's bytes from [`Self::offset`] on. Only whole lines are
    /// consumed; the offset advances past the last newline.
    pub fn import(&mut self, tail: &[u8]) -> Result<Imported, ImportError> {
        let Some(end) = tail.iter().rposition(|byte| *byte == b'\n') else {
            return Ok(Imported {
                offset: self.offset,
                ..Imported::default()
            });
        };
        let session_before = self.session.clone();
        let mut at = self.offset;
        for line in tail[..end].split(|byte| *byte == b'\n') {
            let line_at = at;
            at += line.len() as u64 + 1;
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let record: Record =
                serde_json::from_slice(line).map_err(|error| ImportError::Line {
                    at: line_at,
                    reason: error.to_string(),
                })?;
            self.record(record)?;
        }
        self.offset += end as u64 + 1;

        let mut imported = std::mem::take(&mut self.out);
        imported.offset = self.offset;
        if self.session != session_before {
            imported.session = self.session.clone();
        }
        for id in std::mem::take(&mut self.dirty) {
            let turn = self.turns[&id].clone();
            imported.turns.insert(id, turn);
        }
        let current = self.current_turn.as_ref();
        let pending = &self.pending;
        self.turns
            .retain(|id, _| Some(id) == current || pending.values().any(|call| &call.turn == id));
        Ok(imported)
    }

    fn record(&mut self, record: Record) -> Result<(), ImportError> {
        if record.kind == "ai-title" {
            self.title = record.ai_title.clone();
            if let Some(session) = &mut self.session {
                session.title = self.title.clone();
            }
            return Ok(());
        }
        let Some(uuid) = record.uuid.as_deref() else {
            // Snapshots and queue operations carry no uuid and say nothing about the session.
            return Ok(());
        };
        // A subagent's records are not this session's. (Claude Code keeps them in their own
        // files; older versions inlined them.)
        if record.is_sidechain && self.parent.is_none() {
            return Ok(());
        }
        if self.turn_of.contains_key(uuid) {
            return Ok(());
        }
        match record.kind.as_str() {
            "user" | "assistant" => {
                let at = record
                    .timestamp
                    .ok_or_else(|| Self::invalid(uuid, "no timestamp"))?;
                self.ensure_session(&record, uuid)?;
                if record.kind == "user" {
                    self.user(&record, uuid, at)?;
                } else {
                    self.assistant(&record, uuid, at)?;
                }
                self.turn_of
                    .insert(uuid.to_owned(), self.current_turn.clone());
            }
            // System records (compaction boundaries, hook output) hold nothing of the
            // conversation, but their descendants continue its turn.
            "system" => {
                self.turn_of
                    .insert(uuid.to_owned(), self.current_turn.clone());
            }
            _ => {
                let turn = record
                    .parent_uuid
                    .as_ref()
                    .and_then(|parent| self.turn_of.get(parent).cloned().flatten());
                self.turn_of.insert(uuid.to_owned(), turn);
            }
        }
        Ok(())
    }

    fn invalid(uuid: &str, reason: impl Into<String>) -> ImportError {
        ImportError::Record {
            uuid: uuid.to_owned(),
            reason: reason.into(),
        }
    }

    fn ensure_session(&mut self, record: &Record, uuid: &str) -> Result<(), ImportError> {
        if self.session.is_some() {
            return Ok(());
        }
        let cwd = record
            .cwd
            .clone()
            .ok_or_else(|| Self::invalid(uuid, "the first record has no cwd"))?;
        let session_id = if self.parent.is_some() {
            let agent = record
                .agent_id
                .as_deref()
                .ok_or_else(|| Self::invalid(uuid, "a subagent record has no agentId"))?;
            format!("agent-{agent}")
        } else {
            record
                .session_id
                .clone()
                .ok_or_else(|| Self::invalid(uuid, "the first record has no sessionId"))?
        };
        self.session = Some(AgentSession {
            harness: "claude-code".to_owned(),
            session_id,
            title: self.title.clone(),
            cwd,
            additional_directories: Vec::new(),
            parent: self.parent.clone(),
            extra: Extra::default(),
        });
        Ok(())
    }

    fn start_turn(&mut self, id: &str, at: Timestamp, parent_uuid: Option<&str>) {
        let parent_turn =
            parent_uuid.and_then(|parent| self.turn_of.get(parent).cloned().flatten());
        let turn = PromptTurn {
            seq: self.turn_count,
            at,
            parent_turn,
            stop_reason: None,
            messages: Vec::new(),
            tool_calls: Vec::new(),
            extra: Extra::default(),
        };
        self.turn_count += 1;
        self.turns.insert(id.to_owned(), turn);
        self.dirty.insert(id.to_owned());
        self.current_turn = Some(id.to_owned());
    }

    /// The current turn's id, opening one named after `uuid` for a record before any prompt.
    fn ensure_turn(&mut self, record: &Record, uuid: &str, at: Timestamp) -> String {
        if self.current_turn.is_none() {
            self.start_turn(uuid, at, record.parent_uuid.as_deref());
        }
        self.current_turn.clone().expect("a turn was just opened")
    }

    fn turn_mut(&mut self, id: &str) -> &mut PromptTurn {
        if !self.dirty.contains(id) {
            self.dirty.insert(id.to_owned());
        }
        self.turns
            .get_mut(id)
            .expect("turn ids come from this importer")
    }

    fn push_message(
        &mut self,
        turn: &str,
        id: &str,
        at: Timestamp,
        kind: MessageKind,
        text: String,
    ) {
        let turn = self.turn_mut(turn);
        let seq = (turn.messages.len() + turn.tool_calls.len()) as u64;
        turn.messages.push(Message {
            id: id.to_owned(),
            seq,
            at,
            kind,
            text,
            extra: Extra::default(),
        });
    }

    fn user(&mut self, record: &Record, uuid: &str, at: Timestamp) -> Result<(), ImportError> {
        let (texts, results) = match record
            .message
            .as_ref()
            .and_then(|message| message.content.as_ref())
        {
            Some(Content::Text(text)) => (vec![text.as_str()], Vec::new()),
            Some(Content::Blocks(blocks)) => {
                let mut texts = Vec::new();
                let mut results = Vec::new();
                for block in blocks {
                    match block {
                        Block::Text { text } => texts.push(text.as_str()),
                        Block::ToolResult {
                            tool_use_id,
                            content,
                            is_error,
                        } => results.push((tool_use_id.as_str(), content, *is_error)),
                        // Images and blocks of kinds this importer does not know are not
                        // recorded.
                        _ => {}
                    }
                }
                (texts, results)
            }
            None => (Vec::new(), Vec::new()),
        };

        if !results.is_empty() {
            // `toolUseResult` is the harness's structured form of the result; with several
            // results in one record it is ambiguous, so it is used only for a single one.
            let structured = if results.len() == 1 {
                record.tool_use_result.as_ref()
            } else {
                None
            };
            for (call_id, content, is_error) in results {
                self.tool_result(uuid, call_id, content, is_error, structured)?;
            }
            return Ok(());
        }

        let text = texts.join("\n");
        if text.trim().is_empty() {
            return Ok(());
        }
        if record.is_compact_summary {
            let turn = self.ensure_turn(record, uuid, at);
            self.push_message(&turn, uuid, at, MessageKind::CompactionSummary, text);
            return Ok(());
        }
        let first_of_subagent = self.parent.is_some() && self.current_turn.is_none();
        let kind = if first_of_subagent {
            MessageKind::SpawnPrompt
        } else {
            let t = text.trim_start();
            if t.starts_with("<task-notification>") {
                MessageKind::Notification
            } else if [
                "<local-command-stdout>",
                "<local-command-stderr>",
                "<local-command-caveat>",
                "<command-name>",
                "<command-message>",
            ]
            .iter()
            .any(|tag| t.starts_with(tag))
            {
                MessageKind::LocalCommand
            } else if t.starts_with("[Request interrupted by user") {
                MessageKind::Interrupt
            } else if t.starts_with("Another Claude session sent a message") {
                MessageKind::CrossSession
            } else if record.is_meta || t.starts_with("<system-reminder>") {
                MessageKind::Meta
            } else {
                MessageKind::UserMessage
            }
        };
        if matches!(kind, MessageKind::UserMessage | MessageKind::SpawnPrompt) {
            self.start_turn(uuid, at, record.parent_uuid.as_deref());
        }
        let turn = self.ensure_turn(record, uuid, at);
        if kind == MessageKind::Interrupt {
            self.turn_mut(&turn).stop_reason = Some(StopReason::Cancelled);
        }
        self.push_message(&turn, uuid, at, kind, text);
        Ok(())
    }

    fn tool_result(
        &mut self,
        uuid: &str,
        call_id: &str,
        content: &Value,
        is_error: bool,
        structured: Option<&Value>,
    ) -> Result<(), ImportError> {
        let pending = self
            .pending
            .remove(call_id)
            .ok_or_else(|| Self::invalid(uuid, format!("result of unknown tool call {call_id}")))?;
        let diffs = match (&pending.edit, is_error) {
            (Some((name, input)), false) => Self::diffs(uuid, name, input, structured)?,
            _ => Vec::new(),
        };
        let output = match content {
            Value::String(text) => text.clone(),
            Value::Array(blocks) => blocks
                .iter()
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n"),
            _ => String::new(),
        };
        let call = self
            .turn_mut(&pending.turn)
            .tool_calls
            .iter_mut()
            .find(|call| call.id == call_id)
            .expect("a pending call was pushed to its turn");
        call.status = if is_error {
            ToolStatus::Failed
        } else {
            ToolStatus::Completed
        };
        call.diffs = diffs;
        call.output = Some(output);
        if let Some(agent) = structured
            .and_then(|result| result.get("agentId"))
            .and_then(Value::as_str)
        {
            self.out.spawned.push(agent.to_owned());
        }
        Ok(())
    }

    fn assistant(&mut self, record: &Record, uuid: &str, at: Timestamp) -> Result<(), ImportError> {
        let message = record
            .message
            .as_ref()
            .ok_or_else(|| Self::invalid(uuid, "an assistant record has no message"))?;
        // One API message arrives as several records, one per content block; they share an id.
        let message_id = message.id.as_deref().unwrap_or(uuid);
        let turn = self.ensure_turn(record, uuid, at);
        if let Some(reason @ (StopReason::EndTurn | StopReason::MaxTokens | StopReason::Refusal)) =
            &message.stop_reason
        {
            self.turn_mut(&turn).stop_reason = Some(reason.clone());
        }
        let blocks = match &message.content {
            Some(Content::Blocks(blocks)) => blocks.as_slice(),
            Some(Content::Text(text)) => {
                self.append_text(&turn, message_id, at, MessageKind::AgentMessage, text);
                return Ok(());
            }
            None => return Ok(()),
        };
        for block in blocks {
            match block {
                Block::Text { text } => {
                    self.append_text(&turn, message_id, at, MessageKind::AgentMessage, text)
                }
                Block::Thinking { thinking } if self.record_thinking => self.append_text(
                    &turn,
                    &format!("{message_id}.thought"),
                    at,
                    MessageKind::AgentThought,
                    thinking,
                ),
                Block::ToolUse { id, name, input } => {
                    self.tool_use(record, uuid, &turn, id, name, input)?
                }
                Block::Thinking { .. } | Block::ToolResult { .. } | Block::Other => {}
            }
        }
        Ok(())
    }

    /// Add `text` to the message `id` of `turn`, creating it, or appending a paragraph when
    /// another record of the same API message already made it.
    fn append_text(&mut self, turn: &str, id: &str, at: Timestamp, kind: MessageKind, text: &str) {
        if text.trim().is_empty() {
            return;
        }
        if let Some(message) = self
            .turn_mut(turn)
            .messages
            .iter_mut()
            .find(|message| message.id == id)
        {
            message.text.push_str("\n\n");
            message.text.push_str(text);
        } else {
            self.push_message(turn, id, at, kind, text.to_owned());
        }
    }

    fn tool_use(
        &mut self,
        record: &Record,
        uuid: &str,
        turn: &str,
        id: &str,
        name: &str,
        input: &Value,
    ) -> Result<(), ImportError> {
        let text = |key: &str| input.get(key).and_then(Value::as_str).unwrap_or("");
        let base = |path: &str| path.rsplit('/').next().unwrap_or(path).to_owned();
        let summary = match name {
            "Edit" => format!("{}: replace", base(text("file_path"))),
            "MultiEdit" => format!(
                "{}: {} replacements",
                base(text("file_path")),
                input
                    .get("edits")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len)
            ),
            "Write" => format!("{}: write", base(text("file_path"))),
            "Read" => text("file_path").to_owned(),
            "Grep" => format!("{} in {}", text("pattern"), text("path")),
            "Glob" => text("pattern").to_owned(),
            "Bash" if !text("description").is_empty() => text("description").to_owned(),
            "Bash" => text("command").to_owned(),
            "Agent" | "Task" => text("description").to_owned(),
            _ => input
                .as_object()
                .and_then(|fields| fields.values().find_map(Value::as_str))
                .unwrap_or("")
                .to_owned(),
        };
        let title: String = format!("{name} {summary}")
            .trim()
            .chars()
            .take(200)
            .collect();
        let kind = match name {
            "Edit" | "Write" | "MultiEdit" | "NotebookEdit" => ToolKind::Edit,
            "Read" => ToolKind::Read,
            "Grep" | "Glob" | "WebSearch" | "ToolSearch" => ToolKind::Search,
            "WebFetch" => ToolKind::Fetch,
            "Bash" | "BashOutput" | "KillShell" | "Monitor" | "TaskStop" => ToolKind::Execute,
            "EnterPlanMode" | "ExitPlanMode" => ToolKind::SwitchMode,
            _ => ToolKind::Other,
        };
        let locations = ["file_path", "notebook_path", "path"]
            .iter()
            .find_map(|key| input.get(*key).and_then(Value::as_str))
            .map(|path| ToolLocation {
                path: path.to_owned(),
                line: input
                    .get("offset")
                    .and_then(Value::as_u64)
                    .map(|line| line as u32),
                extra: Extra::default(),
            })
            .into_iter()
            .collect();
        let session_cwd = &self.session.as_ref().expect("session precedes records").cwd;
        let cwd = record.cwd.clone().filter(|cwd| cwd != session_cwd);

        if name == "TodoWrite" {
            let todos: Vec<Todo> =
                serde_json::from_value(input.get("todos").cloned().unwrap_or_default())
                    .map_err(|error| Self::invalid(uuid, format!("TodoWrite input: {error}")))?;
            self.out.plan = Some(Plan {
                // Claude Code's todos have no priority.
                entries: todos
                    .into_iter()
                    .map(|todo| PlanEntry {
                        content: todo.content,
                        priority: Priority::Medium,
                        status: todo.status,
                        extra: Extra::default(),
                    })
                    .collect(),
                extra: Extra::default(),
            });
        }

        let edit = (kind == ToolKind::Edit && name != "NotebookEdit")
            .then(|| (name.to_owned(), input.clone()));
        self.pending.insert(
            id.to_owned(),
            Pending {
                turn: turn.to_owned(),
                edit,
            },
        );
        let at_turn = self.turn_mut(turn);
        let seq = (at_turn.messages.len() + at_turn.tool_calls.len()) as u64;
        at_turn.tool_calls.push(ToolCall {
            id: id.to_owned(),
            seq,
            title,
            kind,
            status: ToolStatus::Pending,
            cwd,
            locations,
            diffs: Vec::new(),
            output: None,
            extra: Extra::default(),
        });
        Ok(())
    }

    /// The unified patches an Edit, Write or MultiEdit made. The result's `structuredPatch` has
    /// real line numbers and context, so it is preferred; without it the patch is built from the
    /// call's input alone.
    fn diffs(
        uuid: &str,
        name: &str,
        input: &Value,
        structured: Option<&Value>,
    ) -> Result<Vec<ToolDiff>, ImportError> {
        let text = |value: &Value, key: &str| {
            value
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned()
        };
        let path = text(input, "file_path");
        let hunks: Vec<Hunk> = match structured.and_then(|result| result.get("structuredPatch")) {
            Some(hunks) => serde_json::from_value(hunks.clone())
                .map_err(|error| Self::invalid(uuid, format!("structuredPatch: {error}")))?,
            None => Vec::new(),
        };
        let from_hunks = || {
            hunks
                .iter()
                .map(|hunk| {
                    let mut patch = format!(
                        "@@ -{},{} +{},{} @@",
                        hunk.old_start, hunk.old_lines, hunk.new_start, hunk.new_lines
                    );
                    for line in &hunk.lines {
                        patch.push('\n');
                        patch.push_str(line);
                    }
                    patch
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        // Without file context every old line is removed and every new line added, and the
        // line numbers are unknown, so they read 1.
        let replace = |old: &str, new: &str| {
            let (old, new): (Vec<&str>, Vec<&str>) = (old.lines().collect(), new.lines().collect());
            let mut patch = format!("@@ -1,{} +1,{} @@", old.len(), new.len());
            for line in old {
                patch.push_str("\n-");
                patch.push_str(line);
            }
            for line in new {
                patch.push_str("\n+");
                patch.push_str(line);
            }
            patch
        };
        let patches = match name {
            "Write" => {
                let created = structured
                    .and_then(|result| result.get("type"))
                    .and_then(Value::as_str)
                    == Some("create");
                if created || hunks.is_empty() {
                    let content = text(input, "content");
                    let lines: Vec<&str> = content.lines().collect();
                    let mut patch = format!("@@ -0,0 +1,{} @@", lines.len());
                    for line in lines {
                        patch.push_str("\n+");
                        patch.push_str(line);
                    }
                    vec![patch]
                } else {
                    vec![from_hunks()]
                }
            }
            "Edit" if hunks.is_empty() => {
                vec![replace(
                    &text(input, "old_string"),
                    &text(input, "new_string"),
                )]
            }
            "MultiEdit" if hunks.is_empty() => input
                .get("edits")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|edit| replace(&text(edit, "old_string"), &text(edit, "new_string")))
                .collect(),
            _ => vec![from_hunks()],
        };
        Ok(patches
            .into_iter()
            .map(|patch| ToolDiff {
                path: path.clone(),
                patch,
                extra: Extra::default(),
            })
            .collect())
    }
}
