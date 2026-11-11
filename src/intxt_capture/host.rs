//! The host half of capture (ADR 039 §4 and §5): what one hook invocation observes, and the local
//! state that makes the next invocation resume where this one ended.
//!
//! A hook runs [`prepare`], which imports the transcript and observes the session's worktrees
//! while holding their locks, and returns a [`Prepared`] with the [`Batch`] to record. Recording
//! happens elsewhere (the `recordObservations` operation); its answer goes to
//! [`Prepared::commit`], which persists the new state and releases the locks. A [`Prepared`]
//! dropped without committing persists nothing, so a failed record is observed again, from the
//! old state, by the next hook.
//!
//! Locks. A hook takes its session's lock, then each worktree's, in path order, and holds all of
//! them until the [`Prepared`] is committed or dropped. Taking the session lock first serializes
//! the hooks of one session, whose importer offset is not safe to advance concurrently, and keeps
//! the order (session, then worktrees by path) the same for every hook, so hooks cannot deadlock.
//! The worktree lock is what makes op sequence numbers single-writer across sessions.

use crate::Error;
use crate::hook::Event;
use crate::hook::Hook;
use crate::identity;
use crate::identity::Identity;
use crate::observe;
use crate::observe::WorktreeState;
use crate::snapshot::Change;
use crate::snapshot::Snapshot;
use crate::transcript::Imported;
use crate::transcript::TranscriptImporter;
use big_cabinet_core::DatumReference;
use big_cabinet_core::datum::DatumMember;
use intxt_core::work::AgentSession;
use intxt_core::work::GitCommit;
use intxt_core::work::GitObservation;
use intxt_core::work::GitRef;
use intxt_core::work::OpKind;
use jiff::Timestamp;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

// ---- what a hook observed ----------------------------------------------------------------

/// Which tool call a change belongs to, symbolically. The recorder turns it into a `Location`
/// once it knows the session document: the call is `toolCalls[id]` of `promptTurn/<turn>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cause {
    /// The subagent whose session holds the call; none for the main session.
    pub agent_id: Option<String>,
    /// The turn holding the call. None when the transcript did not yet have the call at this
    /// hook (the harness writes it concurrently).
    pub turn: Option<String>,
    pub tool_use_id: String,
}

/// One op of a worktree's log, without its sequence number (the recorder numbers them).
#[derive(Debug, Clone, PartialEq)]
pub struct Op {
    pub at: Timestamp,
    pub kind: OpKind,
    pub cause: Option<Cause>,
    pub changes: Vec<Change>,
    pub git: Option<GitObservation>,
}

/// The document ids the recorder holds for a worktree (§5); none before it first recorded one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeDocs {
    pub worktree: Option<String>,
    pub clone: Option<String>,
    pub repo: Option<String>,
}

/// Everything observed in one worktree by one hook: the input of one `recordObservations` call.
#[derive(Debug, Clone, PartialEq)]
pub struct WorktreeBatch {
    pub identity: Identity,
    pub docs: WorktreeDocs,
    /// The sequence number of `ops[0]`.
    pub next_seq: u64,
    pub ops: Vec<Op>,
    pub commits: BTreeMap<String, GitCommit>,
    pub refs: BTreeMap<String, GitRef>,
    /// The reflog lost entries since the last observation; the last op is a synthetic move.
    pub reflog_gap: bool,
}

/// A subagent's session, imported.
#[derive(Debug, Clone, PartialEq)]
pub struct SubagentBatch {
    pub agent_id: String,
    pub doc: Option<String>,
    pub imported: Imported,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionBatch {
    pub session_id: String,
    pub doc: Option<String>,
    pub imported: Imported,
    pub subagents: Vec<SubagentBatch>,
}

/// What one hook invocation observed, shaped as `recordObservations` takes it (ADR 039 §4).
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    pub session: SessionBatch,
    /// Only worktrees with something to record.
    pub worktrees: Vec<WorktreeBatch>,
    /// Ops of a post hook whose tool call the transcript did not show yet, so they carry no cause
    /// (and are not retried). For the CLI to report.
    pub uncaused_tool_calls: u64,
}

impl Batch {
    /// Nothing was observed: the hook can commit without recording.
    pub fn is_empty(&self) -> bool {
        let quiet = |imported: &Imported| {
            imported.session.is_none() && imported.turns.is_empty() && imported.plan.is_none()
        };
        quiet(&self.session.imported)
            && self
                .session
                .subagents
                .iter()
                .all(|agent| quiet(&agent.imported))
            && self.worktrees.is_empty()
    }
}

/// What the recorder answers: the document ids it created or knows, and the next sequence number
/// of each worktree it wrote ops to, keyed by worktree root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordedIds {
    pub session: Option<String>,
    /// Agent id → the subagent session's document.
    pub subagents: BTreeMap<String, String>,
    pub worktrees: BTreeMap<PathBuf, RecordedWorktree>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWorktree {
    pub docs: WorktreeDocs,
    pub next_seq: u64,
}

// ---- local state ---------------------------------------------------------------------------

/// `<git dir>/intxt/state.json`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WorktreeHost {
    observed: WorktreeState,
    next_seq: u64,
    docs: WorktreeDocs,
    /// Snapshots taken before tool calls, by `tool_use_id`, removed by the call's post hook. A
    /// call that never reports leaves its snapshot here.
    pre: BTreeMap<String, Snapshot>,
}

/// The state file a caller passes in, per session.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SessionHost {
    importer: TranscriptImporter,
    doc: Option<String>,
    /// Roots of the worktrees the session has touched.
    worktrees: BTreeSet<PathBuf>,
    /// Subagents seen spawned whose importers wait for the session's document id, which their
    /// `parent` reference needs.
    waiting_agents: BTreeSet<String>,
    subagents: BTreeMap<String, SubagentHost>,
    /// Tool call id → (agent id, turn id), for the calls the transcript has shown, to name a
    /// post hook's cause. An entry goes when its post hook is committed.
    tool_turns: BTreeMap<String, (Option<String>, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SubagentHost {
    importer: TranscriptImporter,
    doc: Option<String>,
}

fn load<T: Default + DeserializeOwned>(path: &Path) -> Result<T, Error> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|error| Error::State {
            path: path.display().to_string(),
            reason: error.to_string(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error.into()),
    }
}

/// Write `value` to `path` through a temporary file in the same directory, so a reader or a crash
/// sees the old file or the whole new one.
fn save<T: Serialize>(path: &Path, value: &T) -> Result<(), Error> {
    let mut temporary: OsString = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(
        &temporary,
        serde_json::to_vec(value).expect("state serializes"),
    )?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

/// Open (creating) and exclusively lock the file at `path`, waiting for its holder. The lock is
/// released when the returned file is dropped.
fn lock(path: &Path) -> Result<File, Error> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let file = File::options().create(true).append(true).open(path)?;
    file.lock()?;
    Ok(file)
}

// ---- the driver ----------------------------------------------------------------------------

struct PendingWorktree {
    root: PathBuf,
    state_path: PathBuf,
    host: WorktreeHost,
    /// The batch has an entry for it, so the recorder must answer for it.
    batched: bool,
    _lock: File,
}

/// A prepared hook invocation: the batch to record, the state to persist once it is recorded,
/// and the locks held until then.
#[must_use = "dropping a Prepared persists nothing"]
pub struct Prepared {
    pub batch: Batch,
    session_path: PathBuf,
    session: SessionHost,
    worktrees: Vec<PendingWorktree>,
    /// The post hook's call, whose `tool_turns` entry goes with the commit.
    finished_tool: Option<String>,
    _session_lock: File,
}

/// The subagent transcripts live beside the session's, in
/// `<transcript without .jsonl>/subagents/agent-<id>.jsonl`.
fn subagent_transcript(transcript: &Path, agent_id: &str) -> PathBuf {
    transcript
        .with_extension("")
        .join("subagents")
        .join(format!("agent-{agent_id}.jsonl"))
}

/// Import whatever `path` has beyond the importer's offset; a transcript not written yet imports
/// nothing.
fn import(importer: &mut TranscriptImporter, path: &Path) -> Result<Imported, Error> {
    if path.exists() {
        Ok(importer.import_file(path)?)
    } else {
        Ok(Imported {
            offset: importer.offset(),
            ..Imported::default()
        })
    }
}

/// Bring the session's importers up to date: the main transcript, then each subagent's.
fn import_transcripts(
    hook: &Hook,
    session: &mut SessionHost,
) -> Result<(Imported, Vec<SubagentBatch>), Error> {
    // The transcript first: any hook brings the session up to date.
    let imported = import(&mut session.importer, &hook.transcript_path)?;
    for turn_id_and_turn in &imported.turns {
        for call in &turn_id_and_turn.1.tool_calls {
            session
                .tool_turns
                .insert(call.id.clone(), (None, turn_id_and_turn.0.clone()));
        }
    }
    session
        .waiting_agents
        .extend(imported.spawned.iter().cloned());
    if let Some(doc) = &session.doc {
        let parent = DatumReference::parse(&format!(
            "dtxt:datum:{doc}/{}/main",
            <AgentSession as DatumMember>::ID
        ))
        .map_err(|error| Error::Format(format!("session document {doc:?}: {error}")))?;
        for agent in std::mem::take(&mut session.waiting_agents) {
            session.subagents.insert(
                agent,
                SubagentHost {
                    importer: TranscriptImporter::new(Some(parent.clone())),
                    doc: None,
                },
            );
        }
    }
    let mut subagents = Vec::new();
    for (agent_id, agent) in &mut session.subagents {
        let imported = import(
            &mut agent.importer,
            &subagent_transcript(&hook.transcript_path, agent_id),
        )?;
        for (turn_id, turn) in &imported.turns {
            for call in &turn.tool_calls {
                session
                    .tool_turns
                    .insert(call.id.clone(), (Some(agent_id.clone()), turn_id.clone()));
            }
        }
        subagents.push(SubagentBatch {
            agent_id: agent_id.clone(),
            doc: agent.doc.clone(),
            imported,
        });
    }
    Ok((imported, subagents))
}

/// One worktree's part of a hook: a pre-snapshot on `PreToolUse`, otherwise the ops of the
/// window. `None` when there is nothing to record. Also how many of its ops should have had a
/// cause but could not, because the transcript did not show the tool call's turn yet.
///
/// `Stop` and `SessionEnd` drop every pending pre-snapshot: no tool is in flight then, so those
/// belong to calls that never reported.
fn observe_worktree(
    hook: &Hook,
    found: Identity,
    tool_cause: Option<&Cause>,
    host: &mut WorktreeHost,
) -> Result<(Option<WorktreeBatch>, u64), Error> {
    if let (Event::PreToolUse, Some(tool_use_id)) = (&hook.event, &hook.tool_use_id) {
        host.pre
            .insert(tool_use_id.clone(), Snapshot::take(&found.root)?);
        return Ok((None, 0));
    }
    if matches!(hook.event, Event::Stop | Event::SessionEnd) {
        host.pre.clear();
    }
    let pre = tool_cause.and_then(|cause| host.pre.remove(&cause.tool_use_id));
    let cause = tool_cause.filter(|cause| cause.turn.is_some());
    let (observation, next) = observe::observe(&host.observed, found, pre.as_ref())?;
    host.observed = next;
    let git = observation.git;
    let mut ops = Vec::new();
    if !observation.earlier.is_empty() {
        ops.push(Op {
            at: Timestamp::now(),
            kind: OpKind::Edit,
            cause: None,
            changes: observation.earlier,
            git: None,
        });
    }
    let before_window = ops.len();
    // A window in which HEAD moved records its file changes on the move that closed it (ADR 039
    // §3): a checkout is not an edit.
    let mut changes = Some(observation.changes);
    let last_move = git.moves.len().wrapping_sub(1);
    for (index, mv) in git.moves.iter().enumerate() {
        ops.push(Op {
            at: mv.at,
            kind: mv.kind.clone(),
            cause: cause.cloned(),
            changes: if index == last_move {
                changes.take().unwrap_or_default()
            } else {
                Vec::new()
            },
            git: Some(mv.observation.clone()),
        });
    }
    if let Some(changes) = changes.filter(|changes| !changes.is_empty()) {
        ops.push(Op {
            at: Timestamp::now(),
            kind: OpKind::Edit,
            cause: cause.cloned(),
            changes,
            git: None,
        });
    }
    let uncaused = if tool_cause.is_some() && cause.is_none() {
        (ops.len() - before_window) as u64
    } else {
        0
    };
    if ops.is_empty() && git.commits.is_empty() && git.refs.is_empty() {
        return Ok((None, uncaused));
    }
    Ok((
        Some(WorktreeBatch {
            identity: observation.identity,
            docs: host.docs.clone(),
            next_seq: host.next_seq,
            ops,
            commits: git.commits,
            refs: git.refs,
            reflog_gap: git.reflog_gap,
        }),
        uncaused,
    ))
}

/// Observe for `hook`. `session_path` is the session's state file; its lock is `<path>.lock`.
pub fn prepare(hook: &Hook, session_path: &Path) -> Result<Prepared, Error> {
    let mut lock_path: OsString = session_path.as_os_str().to_owned();
    lock_path.push(".lock");
    let session_lock = lock(Path::new(&lock_path))?;
    let mut session: SessionHost = load(session_path)?;

    let (imported, subagents) = import_transcripts(hook, &mut session)?;

    // The worktrees of this window: those the session touched, the one holding `cwd`, and any a
    // tool's paths name.
    let mut candidates: BTreeSet<PathBuf> = session.worktrees.clone();
    candidates.insert(hook.cwd.clone());
    if let Some(input) = &hook.tool_input {
        candidates.extend(
            ["file_path", "notebook_path", "path"]
                .iter()
                .filter_map(|key| input.get(*key).and_then(|value| value.as_str()))
                .map(PathBuf::from),
        );
    }
    let mut identities: BTreeMap<PathBuf, Identity> = BTreeMap::new();
    for candidate in &candidates {
        if let Some(found) = identity::resolve(candidate)? {
            // A stored root that is gone resolves to whatever encloses it: not the same worktree.
            let is_stored = session.worktrees.contains(candidate);
            if !is_stored || found.root == *candidate {
                identities.insert(found.root.clone(), found);
            }
        }
    }
    session.worktrees = identities.keys().cloned().collect();

    let tool = hook.tool_use_id.clone();
    let cause = match (&hook.event, &tool) {
        (Event::PostToolUse | Event::PostToolUseFailure, Some(tool_use_id)) => {
            let located = session.tool_turns.get(tool_use_id);
            Some(Cause {
                agent_id: located.and_then(|(agent, _)| agent.clone()),
                turn: located.map(|(_, turn)| turn.clone()),
                tool_use_id: tool_use_id.clone(),
            })
        }
        _ => None,
    };

    let mut worktrees = Vec::new();
    let mut batches = Vec::new();
    let mut uncaused_tool_calls = 0;
    for (root, found) in identities {
        // Sorted by root: every hook takes worktree locks in the same order.
        let worktree_lock = lock(&found.git_dir.join("intxt/lock"))?;
        let state_path = found.git_dir.join("intxt/state.json");
        let mut host: WorktreeHost = load(&state_path)?;
        let (batch, uncaused) = observe_worktree(hook, found, cause.as_ref(), &mut host)?;
        uncaused_tool_calls += uncaused;
        let batched = batch.is_some();
        batches.extend(batch);
        worktrees.push(PendingWorktree {
            root,
            state_path,
            host,
            batched,
            _lock: worktree_lock,
        });
    }

    Ok(Prepared {
        batch: Batch {
            session: SessionBatch {
                session_id: hook.session_id.clone(),
                doc: session.doc.clone(),
                imported,
                subagents,
            },
            worktrees: batches,
            uncaused_tool_calls,
        },
        session_path: session_path.to_owned(),
        session,
        worktrees,
        finished_tool: cause.map(|cause| cause.tool_use_id),
        _session_lock: session_lock,
    })
}

impl Prepared {
    /// Store what the recorder answered, persist every state file and release the locks.
    ///
    /// Worktree states go first and the session's last: a crash in between leaves the session
    /// behind, so the next hook imports its records again, which the recorder's idempotency keys
    /// absorb.
    pub fn commit(mut self, recorded: RecordedIds) -> Result<(), Error> {
        if let Some(doc) = recorded.session {
            self.session.doc = Some(doc);
        }
        for (agent_id, doc) in recorded.subagents {
            self.session
                .subagents
                .get_mut(&agent_id)
                .ok_or_else(|| Error::Format(format!("recorded an unknown subagent {agent_id}")))?
                .doc = Some(doc);
        }
        for pending in &mut self.worktrees {
            match recorded.worktrees.get(&pending.root) {
                Some(recorded) => {
                    pending.host.docs = recorded.docs.clone();
                    pending.host.next_seq = recorded.next_seq;
                }
                None if pending.batched => {
                    return Err(Error::Format(format!(
                        "the recorder did not answer for worktree {}",
                        pending.root.display()
                    )));
                }
                None => {}
            }
        }
        if let Some(tool_use_id) = &self.finished_tool {
            self.session.tool_turns.remove(tool_use_id);
        }
        for pending in &self.worktrees {
            save(&pending.state_path, &pending.host)?;
        }
        save(&self.session_path, &self.session)
    }
}

#[cfg(test)]
mod tests;
