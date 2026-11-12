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
//! named by a UUIDv7 and its `parentTurn` is the session's previous turn.
//!
//! A session outlives its connection, so the host keeps a [`Resume`] per session (the file
//! `host::prepare_acp` writes beside the hook sessions' state, committed with each batch the
//! session emits). `session/load` makes the agent stream the whole history as `session/update`
//! notifications before it answers, and what the recorder does with it depends on that file,
//! which [`Recorder::new`]'s directory is searched for when the load request passes:
//!
//! - There is one (an earlier connection recorded the session, or this one did): the replay is
//!   dropped, since the recording already holds it, and the session continues into the same
//!   document: its document id, turn count, last turn (the next turn's `parentTurn`) and title
//!   come from the file.
//! - There is none (the session was never recorded): the replay is the session's history and is
//!   recorded with the load's answer, as one batch. The prompt requests are not there, so a turn
//!   begins at each replayed `user_message_chunk` (consecutive ones are pieces of one prompt
//!   and concatenate, as agent chunks do) and the turns carry no stop reason and the time the
//!   replayed update was seen. Tool calls in a replay have no worktree windows: whatever they
//!   changed happened long ago. A replay the connection ends before the load is answered records
//!   nothing, so the next load of the session replays again. A replayed turn's id is derived
//!   from the session id and its index, so a replay recorded twice writes the same slots.
//!
//! In a live connection `user_message_chunk` is dropped: the prompt request is the turn's user
//! message. `fs/*`, `terminal/*` and `session/request_permission` are not recorded; terminal
//! content is a `terminalId`, whose output only the client sees.

use crate::Error;
use big_cabinet_core::datum::Extra;
use intxt_core::capture::Cause;
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
use serde::Serialize;
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
    pub window: Window,
}

/// What the caller does with the worktrees for an [`Emit`]. Emits come in the order the
/// messages that made them passed, so recording them in order keeps a tool call's ops ahead of
/// the turn-end ops that follow.
#[derive(Debug, Clone, PartialEq)]
pub enum Window {
    /// Observe the worktrees: whatever changed since the last observation has no tool call to
    /// name. A session start, a turn end and the connection's end.
    Boundary,
    /// A tool call began: take the worktrees' pre-snapshot for it. `session` is empty.
    Open(ToolWindow),
    /// A tool call finished: observe the worktrees, the ops caused by
    /// the call. `session` is empty; the call itself is recorded with its turn.
    ///
    /// Calls that overlap cut the time since the last observation into segments at each
    /// opening, one op per segment; a segment with several calls open has no cause, as on the
    /// hook path for parallel `PreToolUse` and `PostToolUse`.
    Close(ToolWindow),
}

/// The tool call of a window.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolWindow {
    /// Names the call: the agent's `toolCallId` and the turn holding it.
    pub cause: Cause,
    /// The files the call's locations name, whose worktrees join the observed ones.
    pub paths: Vec<PathBuf>,
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
enum SessionUpdate {
    UserMessageChunk {
        content: Block,
    },
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
        /// What the host holds of the session; none when it is to be recorded from its replay.
        resume: Option<Resume>,
    },
    Prompt {
        session_id: String,
    },
}

/// The tool call's pieces that decide `output`: content text outranks `rawOutput`.
#[derive(Default)]
struct CallState {
    from_content: bool,
    /// The call's window is open: a pre-snapshot was requested and no close was.
    windowed: bool,
}

struct OpenTurn {
    id: String,
    turn: PromptTurn,
    calls: BTreeMap<String, CallState>,
}

/// What a later connection needs to continue a session the host has recorded: its file's
/// existence says the history up to the last commit is recorded.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Resume {
    /// The session's document, once the node answered with it.
    pub doc: Option<String>,
    /// Turns recorded: the next turn's `seq`.
    pub turns: u64,
    /// The latest turn, which the next one follows.
    pub last_turn: Option<String>,
    /// The session's title, which a datum emitted by the continuing connection must not lose.
    pub title: Option<String>,
}

impl Resume {
    /// Fold in a batch the recorder emitted for the session (not its document: the node's answer
    /// carries that).
    pub(crate) fn absorb(&mut self, batch: &SessionBatch) {
        if let Some(session) = &batch.imported.session {
            self.title = session.title.clone();
        }
        for (id, turn) in &batch.imported.turns {
            if turn.seq >= self.turns {
                self.turns = turn.seq + 1;
                self.last_turn = Some(id.clone());
            }
        }
    }
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
    /// The session is being loaded and its replay recorded: the turns it has made so far, which
    /// the load's answer emits.
    replay: Option<BTreeMap<String, PromptTurn>>,
}

/// Records one ACP connection. The connection's sessions live here; what outlives it is each
/// session's [`Resume`], which the host persists and this reads from `sessions` when a
/// `session/load` passes.
pub struct Recorder {
    /// The directory of the host's session state files.
    sessions_dir: PathBuf,
    record_thinking: bool,
    harness: String,
    /// The unfinished last line of each direction.
    partial: [Vec<u8>; 2],
    /// Client requests by their JSON-RPC id, as text.
    requests: BTreeMap<String, Request>,
    sessions: BTreeMap<String, Session>,
}

impl Recorder {
    /// `sessions_dir` is where the host keeps the state of the sessions it has recorded.
    pub fn new(sessions_dir: PathBuf) -> Self {
        Self {
            sessions_dir,
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
            if session.replay.is_some() {
                // The load was never answered: a partial history is not recorded.
                continue;
            }
            if session.open.is_some() {
                emits.push(Self::close_turn(session, None));
            } else if session.dirty {
                emits.push(Self::emit(session, None, BTreeMap::new()));
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
                self.update(message.params.unwrap_or_default(), at)
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
            "session/load" => {
                let session_id = required("sessionId")?;
                let cwd = required("cwd")?;
                let additional = additional();
                // A session this connection holds keeps its turns. Otherwise the host's file
                // says whether an earlier connection recorded it; with none, the replay is its
                // history and the session is held from here to take it.
                let resume = if self.sessions.contains_key(&session_id) {
                    None
                } else {
                    let resume = crate::host::acp_resume(&self.sessions_dir, &session_id)?;
                    if resume.is_none() {
                        let mut session =
                            self.new_session(&session_id, &cwd, additional.clone(), None);
                        session.replay = Some(BTreeMap::new());
                        self.sessions.insert(session_id.clone(), session);
                    }
                    resume
                };
                Request::Load {
                    session_id,
                    cwd,
                    additional,
                    resume,
                }
            }
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
                Self::begin_turn(session, at, text);
                Request::Prompt { session_id }
            }
            // `session/cancel` and the rest: nothing to wait for or to record.
            _ => return Ok(()),
        };
        let id = id.ok_or_else(|| Error::Acp(format!("{method} is a notification")))?;
        self.requests.insert(id.to_string(), request);
        Ok(())
    }

    /// Open the session's next turn with `text` as its user message, if any.
    ///
    /// A live turn is named by a fresh UUIDv7. A replayed turn is named by a UUIDv8 made from
    /// the ACP session id and the turn's index in the replay, so a replay recorded twice (a
    /// record that reached the node but whose commit did not, then the next load) writes the same
    /// `promptTurn` slots instead of duplicates, and the batch's idempotency key (which names its
    /// last turn) and its commands' content digests come out the same as the first attempt's.
    fn begin_turn(session: &mut Session, at: Timestamp, text: String) {
        let id = if session.replay.is_some() {
            let mut name = blake3::Hasher::new();
            name.update(session.datum.session_id.as_bytes());
            name.update(&[0]);
            name.update(&session.turns.to_le_bytes());
            let digest = name.finalize();
            let bytes: [u8; 16] = digest.as_bytes()[..16].try_into().expect("16 of 32 bytes");
            uuid::Builder::from_custom_bytes(bytes)
                .into_uuid()
                .to_string()
        } else {
            Uuid::now_v7().to_string()
        };
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
    }

    /// A replayed `user_message_chunk`: a streamed piece of the open turn's prompt (concatenated,
    /// as agent chunks are), or else the prompt of a turn of its own.
    fn replay_user(session: &mut Session, at: Timestamp, content: &Block) {
        let Some(text) = content.prompt_text() else {
            return;
        };
        if let Some(open) = &mut session.open
            && open.turn.tool_calls.is_empty()
            && let [only] = &mut open.turn.messages[..]
            && only.kind == MessageKind::UserMessage
        {
            only.text.push_str(text);
            return;
        }
        Self::end_replayed_turn(session);
        Self::begin_turn(session, at, text.to_owned());
    }

    fn end_replayed_turn(session: &mut Session) {
        if let Some(open) = session.open.take() {
            session
                .replay
                .as_mut()
                .expect("only a replay ends a turn without a response")
                .insert(open.id, open.turn);
        }
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
                Ok(Some(self.start(session_id, cwd, additional, None)))
            }
            Request::Load {
                session_id,
                cwd,
                additional,
                resume,
            } => {
                if error.is_some() {
                    // A failed load made no session: drop the one that was to take its replay.
                    if resume.is_none()
                        && self
                            .sessions
                            .get(&session_id)
                            .is_some_and(|session| session.replay.is_some())
                    {
                        self.sessions.remove(&session_id);
                    }
                    return Ok(None);
                }
                Ok(Some(self.start(session_id, cwd, additional, resume)))
            }
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

    /// The session exists from here: one held already (a connection loading a session it holds,
    /// or the one a replay was recorded into) keeps its turns, else it continues from `resume`.
    /// A replay's turns are emitted with the start.
    fn start(
        &mut self,
        session_id: String,
        cwd: String,
        additional: Vec<String>,
        resume: Option<Resume>,
    ) -> Emit {
        if !self.sessions.contains_key(&session_id) {
            let session = self.new_session(&session_id, &cwd, additional, resume);
            self.sessions.insert(session_id.clone(), session);
        }
        let session = self.sessions.get_mut(&session_id).expect("inserted above");
        Self::end_replayed_turn(session);
        let turns = session.replay.take().unwrap_or_default();
        let datum = session.datum.clone();
        Self::emit(session, Some(datum), turns)
    }

    fn new_session(
        &self,
        session_id: &str,
        cwd: &str,
        additional: Vec<String>,
        resume: Option<Resume>,
    ) -> Session {
        let resume = resume.unwrap_or_default();
        Session {
            datum: AgentSession {
                harness: self.harness.clone(),
                session_id: session_id.to_owned(),
                title: resume.title,
                cwd: cwd.to_owned(),
                additional_directories: additional,
                parent: None,
                extra: Extra::default(),
            },
            cwd: PathBuf::from(cwd),
            doc: resume.doc,
            turns: resume.turns,
            last_turn: resume.last_turn,
            open: None,
            plan: None,
            dirty: false,
            replay: None,
        }
    }

    fn close_turn(session: &mut Session, stop: Option<StopReason>) -> Emit {
        let mut open = session.open.take().expect("a turn is open");
        open.turn.stop_reason = stop;
        Self::emit(session, None, BTreeMap::from([(open.id, open.turn)]))
    }

    /// The emit of `session`: its datum when `datum` or its title changed, the plan if it
    /// changed, and `turns`.
    fn emit(
        session: &mut Session,
        datum: Option<AgentSession>,
        turns: BTreeMap<String, PromptTurn>,
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
                    turns,
                    plan: session.plan.take(),
                    extra: Extra::default(),
                },
                subagents: Vec::new(),
                extra: Extra::default(),
            },
            window: Window::Boundary,
        }
    }

    fn update(&mut self, params: Value, at: Timestamp) -> Result<Option<Emit>, Error> {
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Acp("session/update lacks `sessionId`".to_owned()))?;
        // A load's replay: the history streams in before the load is answered. It is recorded
        // when the session's turns are to come from it and dropped otherwise.
        let replaying = self
            .sessions
            .get(session_id)
            .is_some_and(|session| session.replay.is_some());
        if !replaying
            && self.requests.values().any(
                |request| matches!(request, Request::Load { session_id: loading, .. } if loading == session_id),
            )
        {
            return Ok(None);
        }
        let update: SessionUpdate = serde_json::from_value(
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
        if session.replay.is_some() {
            // The first update of a replay may not be a user's, and the tool calls of a replay
            // have no worktree windows.
            if let SessionUpdate::UserMessageChunk { content } = &update {
                Self::replay_user(session, at, content);
                return Ok(None);
            }
            let conversational = match &update {
                SessionUpdate::AgentMessageChunk { .. }
                | SessionUpdate::ToolCall(_)
                | SessionUpdate::ToolCallUpdate(_) => true,
                SessionUpdate::AgentThoughtChunk { .. } => self.record_thinking,
                _ => false,
            };
            if session.open.is_none() && conversational {
                Self::begin_turn(session, at, String::new());
            }
            if let SessionUpdate::ToolCall(call) | SessionUpdate::ToolCallUpdate(call) = update {
                Self::tool_call(session, call)?;
                return Ok(None);
            }
        }
        match update {
            SessionUpdate::UserMessageChunk { .. } | SessionUpdate::Other => Ok(None),
            SessionUpdate::SessionInfoUpdate { title } => {
                if let Some(title) =
                    title.filter(|title| session.datum.title.as_ref() != Some(title))
                {
                    session.datum.title = Some(title);
                    session.dirty = true;
                }
                Ok(None)
            }
            SessionUpdate::Plan { entries } => {
                session.plan = Some(Plan {
                    entries,
                    extra: Extra::default(),
                });
                Ok(None)
            }
            SessionUpdate::AgentMessageChunk {
                content,
                message_id,
            } => Self::chunk(session, at, MessageKind::AgentMessage, &content, message_id)
                .map(|()| None),
            SessionUpdate::AgentThoughtChunk {
                content,
                message_id,
            } if self.record_thinking => {
                Self::chunk(session, at, MessageKind::AgentThought, &content, message_id)
                    .map(|()| None)
            }
            SessionUpdate::AgentThoughtChunk { .. } => Ok(None),
            SessionUpdate::ToolCall(call) | SessionUpdate::ToolCallUpdate(call) => {
                Ok(Self::tool_call(session, call)?.map(|window| Emit {
                    cwd: session.cwd.clone(),
                    session: SessionBatch {
                        session_id: session.datum.session_id.clone(),
                        doc: session.doc.clone(),
                        imported: Imported::default(),
                        subagents: Vec::new(),
                        extra: Extra::default(),
                    },
                    window,
                }))
            }
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

    /// Fold the report into the turn's call, and say whether it opens or closes the call's
    /// window: the call's first report opens it, a terminal status closes it. A call first
    /// reported already finished has no window: nothing was snapshotted before it ran, so its
    /// changes stay with the turn-end observation.
    fn tool_call(session: &mut Session, call: Call) -> Result<Option<Window>, Error> {
        let open = Self::open_turn(session)?;
        let turn = &mut open.turn;
        let first = !turn
            .tool_calls
            .iter()
            .any(|held| held.id == call.tool_call_id);
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
        let finished = matches!(held.status, ToolStatus::Completed | ToolStatus::Failed);
        let tool = || ToolWindow {
            cause: Cause {
                agent_id: None,
                turn: Some(open.id.clone()),
                tool_use_id: held.id.clone(),
                extra: Extra::default(),
            },
            paths: held
                .locations
                .iter()
                .map(|location| PathBuf::from(&location.path))
                .collect(),
        };
        Ok(if first && !finished {
            state.windowed = true;
            Some(Window::Open(tool()))
        } else if state.windowed && finished {
            state.windowed = false;
            Some(Window::Close(tool()))
        } else {
            None
        })
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
