//! An ACP (Agent Client Protocol) connection into ADR 040's session datums: the `agentSession`,
//! one `promptTurn` per `session/prompt`, and the latest `plan`.
//!
//! ACP is JSON-RPC 2.0, one message per line, between a client (an editor) and an agent. A
//! [`Recorder`] watches both directions of one connection: [`Recorder::feed`] takes the bytes of
//! either direction as they pass, and returns an [`Emit`] whenever something is complete enough
//! to record:
//!
//! - a session starts (the `session/new` or `session/load` response): the `agentSession`, so the
//!   caller can take its worktree's baseline;
//! - a prompt turn ends (the `session/prompt` response): the turn, with the plan and the session
//!   title when they changed in it;
//! - [`Recorder::finish`], when the connection ends: a turn still open (the agent died or the
//!   client left mid-turn, so it has no stop reason) and a title not yet emitted.
//!
//! How ACP maps to datums:
//!
//! | ACP | datum |
//! | --- | --- |
//! | `initialize` response `agentInfo.name` | `agentSession.harness` (`acp` without one) |
//! | `session/new`, `session/load` | `agentSession` (`sessionId`, `cwd`, `additionalDirectories`) |
//! | `session_info_update.title` | `agentSession.title` |
//! | `session/prompt` request | a `promptTurn` and its `userMessage` (text blocks, resource links by uri) |
//! | `session/prompt` response `stopReason` | `promptTurn.stopReason` |
//! | `agent_message_chunk` | `agentMessage`, consecutive chunks concatenated |
//! | `agent_thought_chunk` | `agentThought`, only when [`Recorder::recording_thinking`] |
//! | `tool_call`, `tool_call_update` | one `toolCall`: the latest title, kind, status and locations |
//! | `diff` content | `toolCall.diffs`, as a unified patch |
//! | text content, else `rawOutput` | `toolCall.output` |
//! | `plan` | `plan`, replaced whole |
//!
//! A message and a tool call share the turn's `seq` order. ACP has no turn ids, so a turn is
//! named by a UUIDv7 and its `parentTurn` is the session's previous turn in this connection.
//!
//! Replays are not recorded: `session/load` makes the agent stream the whole history as
//! `session/update` notifications before it answers, and they are dropped (they are what an
//! earlier recording already holds). `user_message_chunk` is dropped too: the prompt request is
//! the turn's user message. `fs/*`, `terminal/*` and `session/request_permission` are not
//! recorded; terminal content is a `terminalId`, whose output only the client sees.

use crate::Error;
use big_cabinet_core::datum::Extra;
use intxt_core::capture::Imported;
use intxt_core::capture::SessionBatch;
use intxt_core::work::AgentSession;
use intxt_core::work::Message;
use intxt_core::work::MessageKind;
use intxt_core::work::Plan;
use intxt_core::work::PlanEntry;
use intxt_core::work::PromptTurn;
use intxt_core::work::StopReason;
use intxt_core::work::ToolCall;
use intxt_core::work::ToolDiff;
use intxt_core::work::ToolKind;
use intxt_core::work::ToolLocation;
use intxt_core::work::ToolStatus;
use jiff::Timestamp;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use uuid::Uuid;

#[cfg(test)]
mod tests;

/// Which way a message travels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Client to agent: the agent's stdin.
    ToAgent,
    /// Agent to client: the agent's stdout.
    FromAgent,
}

/// What a [`Recorder`] has to record. `session` carries the session's document id once
/// [`Recorder::set_doc`] knows it.
#[derive(Debug, Clone, PartialEq)]
pub struct Emit {
    /// The session's working directory, whose worktree the caller observes.
    pub cwd: PathBuf,
    pub session: SessionBatch,
}

// ---- the protocol's messages ---------------------------------------------------------------

#[derive(Deserialize)]
struct Rpc {
    id: Option<Value>,
    method: Option<String>,
    params: Option<Value>,
    result: Option<Value>,
    error: Option<Value>,
}

#[derive(Deserialize)]
#[serde(
    tag = "sessionUpdate",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum Update {
    UserMessageChunk,
    AgentMessageChunk {
        content: Block,
        message_id: Option<String>,
    },
    AgentThoughtChunk {
        content: Block,
        message_id: Option<String>,
    },
    ToolCall(Call),
    ToolCallUpdate(Call),
    Plan {
        entries: Vec<PlanEntry>,
    },
    SessionInfoUpdate {
        /// `null` and absent both leave the title as it was.
        title: Option<String>,
    },
    /// `available_commands_update`, `usage_update` and the rest say nothing of the conversation.
    #[serde(other)]
    Other,
}

/// A `tool_call` or `tool_call_update`: an update carries only what changed, and a field it
/// carries replaces the call's.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Call {
    tool_call_id: String,
    title: Option<String>,
    kind: Option<ToolKind>,
    status: Option<ToolStatus>,
    locations: Option<Vec<ToolLocation>>,
    content: Option<Vec<ToolContent>>,
    raw_output: Option<Value>,
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum ToolContent {
    Content {
        content: Block,
    },
    Diff {
        path: String,
        old_text: Option<String>,
        new_text: String,
    },
    /// A terminal's content is its `terminalId`; the output is the client's, not recorded.
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Block {
    Text {
        text: String,
    },
    ResourceLink {
        uri: String,
    },
    Resource {
        resource: ResourceContents,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct ResourceContents {
    uri: String,
}

impl Block {
    /// What a prompt shows of the block: its text, or the uri of a file it points at. Embedded
    /// resources are recorded by uri only, not by content: the files are the worktree's to
    /// record. Images and audio are not recorded.
    fn prompt_text(&self) -> Option<&str> {
        match self {
            Self::Text { text } => Some(text),
            Self::ResourceLink { uri } => Some(uri),
            Self::Resource { resource } => Some(&resource.uri),
            Self::Other => None,
        }
    }
}

// ---- the recorder --------------------------------------------------------------------------

/// A request awaiting its response.
enum Request {
    Initialize,
    New {
        cwd: String,
        additional: Vec<String>,
    },
    Load {
        session_id: String,
        cwd: String,
        additional: Vec<String>,
    },
    Prompt {
        session_id: String,
    },
}

/// The tool call's pieces that decide `output`: content text outranks `rawOutput`.
#[derive(Default)]
struct CallState {
    from_content: bool,
}

struct OpenTurn {
    id: String,
    turn: PromptTurn,
    calls: BTreeMap<String, CallState>,
}

struct Session {
    datum: AgentSession,
    cwd: PathBuf,
    doc: Option<String>,
    /// Turns started: the next turn's `seq`.
    turns: u64,
    last_turn: Option<String>,
    open: Option<OpenTurn>,
    /// The plan as it was last updated, until a turn emits it.
    plan: Option<Plan>,
    /// The session datum changed (its title) and has not been emitted.
    dirty: bool,
}

/// Records one ACP connection. All state is here; nothing persists, so a recording lasts as long
/// as the connection (and a `session/load` of an earlier session starts its turns afresh).
pub struct Recorder {
    record_thinking: bool,
    harness: String,
    /// The unfinished last line of each direction.
    partial: [Vec<u8>; 2],
    /// Client requests by their JSON-RPC id, as text.
    requests: BTreeMap<String, Request>,
    sessions: BTreeMap<String, Session>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    pub fn new() -> Self {
        Self {
            record_thinking: false,
            harness: "acp".to_owned(),
            partial: [Vec::new(), Vec::new()],
            requests: BTreeMap::new(),
            sessions: BTreeMap::new(),
        }
    }

    /// Record `agent_thought_chunk`s as `agentThought` messages. Off by default (FDR 007).
    #[must_use]
    pub fn recording_thinking(mut self) -> Self {
        self.record_thinking = true;
        self
    }

    /// The document id the recorder answered for `session_id`, which later emits carry. A
    /// session the recorder does not hold is a bug of the caller.
    pub fn set_doc(&mut self, session_id: &str, doc: String) {
        self.sessions
            .get_mut(session_id)
            .unwrap_or_else(|| panic!("set_doc: no session {session_id}"))
            .doc = Some(doc);
    }

    /// Take `bytes` of `direction`, in any chunking, and return what the lines they complete
    /// made recordable. `at` stamps the turns and messages those lines start. After an error the
    /// recorder's state is unspecified: stop using it.
    pub fn feed(
        &mut self,
        direction: Direction,
        at: Timestamp,
        bytes: &[u8],
    ) -> Result<Vec<Emit>, Error> {
        let slot = direction as usize;
        self.partial[slot].extend_from_slice(bytes);
        let Some(end) = self.partial[slot].iter().rposition(|byte| *byte == b'\n') else {
            return Ok(Vec::new());
        };
        let rest = self.partial[slot].split_off(end + 1);
        let lines = std::mem::replace(&mut self.partial[slot], rest);
        let mut emits = Vec::new();
        for line in lines.split(|byte| *byte == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let message: Rpc = serde_json::from_slice(line)
                .map_err(|error| Error::Acp(format!("not a JSON-RPC message: {error}")))?;
            emits.extend(self.message(direction, at, message)?);
        }
        Ok(emits)
    }

    /// The connection ended: close an open turn, which has no stop reason, and emit what changed
    /// and was not emitted.
    pub fn finish(&mut self) -> Vec<Emit> {
        let mut emits = Vec::new();
        for session in self.sessions.values_mut() {
            if session.open.is_some() {
                emits.push(Self::close_turn(session, None));
            } else if session.dirty {
                emits.push(Self::emit(session, None, None));
            }
        }
        emits
    }

    fn message(
        &mut self,
        direction: Direction,
        at: Timestamp,
        message: Rpc,
    ) -> Result<Option<Emit>, Error> {
        match (direction, message.method.as_deref()) {
            (Direction::ToAgent, Some(method)) => {
                self.request(method, message.id, message.params.unwrap_or_default(), at)?;
                Ok(None)
            }
            (Direction::FromAgent, Some("session/update")) => {
                self.update(message.params.unwrap_or_default(), at)?;
                Ok(None)
            }
            // The agent's own requests (`fs/*`, `terminal/*`, permission) and notifications.
            (Direction::FromAgent, Some(_)) => Ok(None),
            (Direction::FromAgent, None) => {
                let Some(id) = message.id else {
                    return Err(Error::Acp("a response without an id".to_owned()));
                };
                match self.requests.remove(&id.to_string()) {
                    Some(request) => self.response(request, message.result, message.error),
                    None => Err(Error::Acp(format!("a response to unknown request {id}"))),
                }
            }
            // The client's answers to the agent's requests.
            (Direction::ToAgent, None) => Ok(None),
        }
    }

    fn request(
        &mut self,
        method: &str,
        id: Option<Value>,
        params: Value,
        at: Timestamp,
    ) -> Result<(), Error> {
        let field = |name: &str| params.get(name).and_then(Value::as_str).map(str::to_owned);
        let required =
            |name: &str| field(name).ok_or_else(|| Error::Acp(format!("{method} lacks `{name}`")));
        let additional = || -> Vec<String> {
            params
                .get("additionalDirectories")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|dir| dir.as_str().map(str::to_owned))
                .collect()
        };
        let request = match method {
            "initialize" => Request::Initialize,
            "session/new" => Request::New {
                cwd: required("cwd")?,
                additional: additional(),
            },
            "session/load" => Request::Load {
                session_id: required("sessionId")?,
                cwd: required("cwd")?,
                additional: additional(),
            },
            "session/prompt" => {
                let session_id = required("sessionId")?;
                let session = self.sessions.get_mut(&session_id).ok_or_else(|| {
                    Error::Acp(format!("a prompt for unknown session {session_id}"))
                })?;
                if session.open.is_some() {
                    return Err(Error::Acp(format!(
                        "a prompt for session {session_id} while a turn is open"
                    )));
                }
                let text = params
                    .get("prompt")
                    .and_then(|prompt| serde_json::from_value::<Vec<Block>>(prompt.clone()).ok())
                    .unwrap_or_default()
                    .iter()
                    .filter_map(Block::prompt_text)
                    .collect::<Vec<_>>()
                    .join("\n");
                let id = Uuid::now_v7().to_string();
                let mut turn = PromptTurn {
                    seq: session.turns,
                    at,
                    parent_turn: session.last_turn.clone(),
                    stop_reason: None,
                    messages: Vec::new(),
                    tool_calls: Vec::new(),
                    extra: Extra::default(),
                };
                session.turns += 1;
                session.last_turn = Some(id.clone());
                if !text.is_empty() {
                    turn.messages.push(Message {
                        id: format!("{id}.0"),
                        seq: 0,
                        at,
                        kind: MessageKind::UserMessage,
                        text,
                        extra: Extra::default(),
                    });
                }
                session.open = Some(OpenTurn {
                    id,
                    turn,
                    calls: BTreeMap::new(),
                });
                Request::Prompt { session_id }
            }
            // `session/cancel` and the rest: nothing to wait for or to record.
            _ => return Ok(()),
        };
        let id = id.ok_or_else(|| Error::Acp(format!("{method} is a notification")))?;
        self.requests.insert(id.to_string(), request);
        Ok(())
    }

    fn response(
        &mut self,
        request: Request,
        result: Option<Value>,
        error: Option<Value>,
    ) -> Result<Option<Emit>, Error> {
        let result = result.unwrap_or_default();
        match request {
            Request::Initialize => {
                if let Some(name) = result
                    .pointer("/agentInfo/name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty())
                {
                    self.harness = name.to_owned();
                }
                Ok(None)
            }
            Request::New { cwd, additional } => {
                if error.is_some() {
                    return Ok(None);
                }
                let session_id = result
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Error::Acp("session/new answered without a sessionId".to_owned())
                    })?
                    .to_owned();
                Ok(Some(self.start(session_id, cwd, additional)))
            }
            Request::Load {
                session_id,
                cwd,
                additional,
            } => Ok(if error.is_some() {
                None
            } else {
                Some(self.start(session_id, cwd, additional))
            }),
            Request::Prompt { session_id } => {
                let session = self
                    .sessions
                    .get_mut(&session_id)
                    .expect("a prompt request is only made for a held session");
                let stop = result
                    .get("stopReason")
                    .filter(|_| error.is_none())
                    .map(|reason| {
                        serde_json::from_value::<StopReason>(reason.clone())
                            .map_err(|error| Error::Acp(format!("stopReason: {error}")))
                    })
                    .transpose()?;
                Ok(Some(Self::close_turn(session, stop)))
            }
        }
    }

    /// The session exists from here; loading one a connection already holds keeps its turns.
    fn start(&mut self, session_id: String, cwd: String, additional: Vec<String>) -> Emit {
        let harness = self.harness.clone();
        let session = self
            .sessions
            .entry(session_id.clone())
            .or_insert_with(|| Session {
                datum: AgentSession {
                    harness,
                    session_id,
                    title: None,
                    cwd: cwd.clone(),
                    additional_directories: additional,
                    parent: None,
                    extra: Extra::default(),
                },
                cwd: PathBuf::from(&cwd),
                doc: None,
                turns: 0,
                last_turn: None,
                open: None,
                plan: None,
                dirty: false,
            });
        let datum = session.datum.clone();
        Self::emit(session, Some(datum), None)
    }

    fn close_turn(session: &mut Session, stop: Option<StopReason>) -> Emit {
        let mut open = session.open.take().expect("a turn is open");
        open.turn.stop_reason = stop;
        Self::emit(session, None, Some((open.id, open.turn)))
    }

    /// The emit of `session`: its datum when `datum` or its title changed, the plan if it
    /// changed, and `turn`.
    fn emit(
        session: &mut Session,
        datum: Option<AgentSession>,
        turn: Option<(String, PromptTurn)>,
    ) -> Emit {
        let datum = datum.or_else(|| session.dirty.then(|| session.datum.clone()));
        session.dirty = false;
        Emit {
            cwd: session.cwd.clone(),
            session: SessionBatch {
                session_id: session.datum.session_id.clone(),
                doc: session.doc.clone(),
                imported: Imported {
                    session: datum,
                    turns: turn.into_iter().collect(),
                    plan: session.plan.take(),
                    extra: Extra::default(),
                },
                subagents: Vec::new(),
                extra: Extra::default(),
            },
        }
    }

    fn update(&mut self, params: Value, at: Timestamp) -> Result<(), Error> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Acp("session/update lacks `sessionId`".to_owned()))?;
        // A load's replay: the history streams in before the load is answered.
        if self.requests.values().any(
            |request| matches!(request, Request::Load { session_id: loading, .. } if loading == session_id),
        ) {
            return Ok(());
        }
        let update: Update = serde_json::from_value(
            params
                .get("update")
                .cloned()
                .ok_or_else(|| Error::Acp("session/update lacks `update`".to_owned()))?,
        )
        .map_err(|error| Error::Acp(format!("session/update: {error}")))?;
        let session = self
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| Error::Acp(format!("an update for unknown session {session_id}")))?;
        match update {
            Update::UserMessageChunk | Update::Other => Ok(()),
            Update::SessionInfoUpdate { title } => {
                if let Some(title) =
                    title.filter(|title| session.datum.title.as_ref() != Some(title))
                {
                    session.datum.title = Some(title);
                    session.dirty = true;
                }
                Ok(())
            }
            Update::Plan { entries } => {
                session.plan = Some(Plan {
                    entries,
                    extra: Extra::default(),
                });
                Ok(())
            }
            Update::AgentMessageChunk {
                content,
                message_id,
            } => Self::chunk(session, at, MessageKind::AgentMessage, &content, message_id),
            Update::AgentThoughtChunk {
                content,
                message_id,
            } if self.record_thinking => {
                Self::chunk(session, at, MessageKind::AgentThought, &content, message_id)
            }
            Update::AgentThoughtChunk { .. } => Ok(()),
            Update::ToolCall(call) | Update::ToolCallUpdate(call) => Self::tool_call(session, call),
        }
    }

    fn open_turn(session: &mut Session) -> Result<&mut OpenTurn, Error> {
        let id = &session.datum.session_id;
        session
            .open
            .as_mut()
            .ok_or_else(|| Error::Acp(format!("session {id} updated outside a prompt turn")))
    }

    /// Add a chunk to the turn: to the message it names, or else to the turn's last item when
    /// that is a message of the same kind, or else to a message of its own.
    fn chunk(
        session: &mut Session,
        at: Timestamp,
        kind: MessageKind,
        content: &Block,
        message_id: Option<String>,
    ) -> Result<(), Error> {
        let open = Self::open_turn(session)?;
        let Block::Text { text } = content else {
            return Ok(());
        };
        let turn = &mut open.turn;
        let seq = (turn.messages.len() + turn.tool_calls.len()) as u64;
        let target = match &message_id {
            Some(id) => turn.messages.iter().position(|message| &message.id == id),
            None => turn
                .messages
                .last()
                .filter(|last| last.kind == kind && last.seq + 1 == seq)
                .map(|_| turn.messages.len() - 1),
        };
        match target {
            Some(index) => turn.messages[index].text.push_str(text),
            None => turn.messages.push(Message {
                id: message_id.unwrap_or_else(|| format!("{}.{seq}", open.id)),
                seq,
                at,
                kind,
                text: text.clone(),
                extra: Extra::default(),
            }),
        }
        Ok(())
    }

    fn tool_call(session: &mut Session, call: Call) -> Result<(), Error> {
        let open = Self::open_turn(session)?;
        let turn = &mut open.turn;
        let index = match turn
            .tool_calls
            .iter()
            .position(|held| held.id == call.tool_call_id)
        {
            Some(index) => index,
            None => {
                let title = call.title.clone().ok_or_else(|| {
                    Error::Acp(format!(
                        "tool call {} has no title (an update of a call not seen)",
                        call.tool_call_id
                    ))
                })?;
                let seq = (turn.messages.len() + turn.tool_calls.len()) as u64;
                turn.tool_calls.push(ToolCall {
                    id: call.tool_call_id.clone(),
                    seq,
                    title,
                    kind: ToolKind::Other,
                    status: ToolStatus::Pending,
                    cwd: None,
                    locations: Vec::new(),
                    diffs: Vec::new(),
                    output: None,
                    extra: Extra::default(),
                });
                turn.tool_calls.len() - 1
            }
        };
        let state = open.calls.entry(call.tool_call_id).or_default();
        let held = &mut turn.tool_calls[index];
        if let Some(title) = call.title {
            held.title = title;
        }
        if let Some(kind) = call.kind {
            held.kind = kind;
        }
        if let Some(status) = call.status {
            held.status = status;
        }
        if let Some(locations) = call.locations {
            held.locations = locations;
        }
        if let Some(content) = call.content {
            held.diffs = content
                .iter()
                .filter_map(|item| match item {
                    ToolContent::Diff {
                        path,
                        old_text,
                        new_text,
                    } => Some(ToolDiff {
                        path: path.clone(),
                        patch: unified(old_text.as_deref().unwrap_or(""), new_text),
                        extra: Extra::default(),
                    }),
                    ToolContent::Content { .. } | ToolContent::Other => None,
                })
                .collect();
            let texts: Vec<&str> = content
                .iter()
                .filter_map(|item| match item {
                    ToolContent::Content {
                        content: Block::Text { text },
                    } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            if !texts.is_empty() {
                held.output = Some(texts.join("\n"));
                state.from_content = true;
            }
        }
        if let Some(raw) = call.raw_output.filter(|_| !state.from_content) {
            held.output = Some(match raw {
                Value::String(text) => text,
                other => other.to_string(),
            });
        }
        Ok(())
    }
}

/// The unified patch turning `old` into `new`, three lines of context, without file headers.
fn unified(old: &str, new: &str) -> String {
    similar::TextDiff::from_lines(old, new)
        .unified_diff()
        .context_radius(3)
        .to_string()
        .trim_end_matches('\n')
        .to_owned()
}
