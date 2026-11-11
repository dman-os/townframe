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
use crate::snapshot::Snapshot;
use crate::transcript::Imported;
use crate::transcript::TranscriptImporter;
use big_cabinet_core::DatumReference;
use big_cabinet_core::datum::DatumMember;
use intxt_core::capture::Batch;
use intxt_core::capture::Cause;
use intxt_core::capture::Op;
use intxt_core::capture::RecordedIds;
use intxt_core::capture::SessionBatch;
use intxt_core::capture::SubagentBatch;
use intxt_core::capture::WorktreeBatch;
use intxt_core::capture::WorktreeDocs;
use intxt_core::capture::WorktreeIdentity;
use intxt_core::work::AgentSession;
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

/// The wire form of an identity: what the recorder is told of a worktree.
fn wire(identity: &Identity) -> WorktreeIdentity {
    WorktreeIdentity {
        root: identity.root.display().to_string(),
        clone_path: identity.common_dir.display().to_string(),
        clone_id: identity.clone_id.clone(),
        worktree_id: identity.worktree_id.clone(),
        remotes: identity.remotes.clone(),
        roots: identity.roots.clone(),
        extra: Default::default(),
    }
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
    /// A hook's session state; none for an ACP session, whose recorder keeps its own in memory.
    session: Option<StoredSession>,
    worktrees: Vec<PendingWorktree>,
    /// The post hook's call, whose `tool_turns` entry goes with the commit.
    finished_tool: Option<String>,
}

struct StoredSession {
    path: PathBuf,
    host: SessionHost,
    _lock: File,
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
) -> Result<(intxt_core::capture::Imported, Vec<SubagentBatch>), Error> {
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
            imported: imported.into_batch(),
            extra: Default::default(),
        });
    }
    Ok((imported.into_batch(), subagents))
}

/// One worktree's part of a hook: a pre-snapshot on `PreToolUse`, otherwise the ops of the
/// window. `None` when there is nothing to record. Also how many of its ops should have had a
/// cause but could not, because the transcript did not show the tool call's turn yet.
///
/// `Stop` and `SessionEnd` drop every pending pre-snapshot: no tool is in flight then, so those
/// belong to calls that never reported.
fn observe_worktree(
    event: &Event,
    tool_use_id: Option<&str>,
    found: Identity,
    tool_cause: Option<&Cause>,
    host: &mut WorktreeHost,
) -> Result<(Option<WorktreeBatch>, u64), Error> {
    if let (Event::PreToolUse, Some(tool_use_id)) = (event, tool_use_id) {
        host.pre
            .insert(tool_use_id.to_owned(), Snapshot::take(&found.root)?);
        return Ok((None, 0));
    }
    if matches!(event, Event::Stop | Event::SessionEnd) {
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
            extra: Default::default(),
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
            extra: Default::default(),
        });
    }
    if let Some(changes) = changes.filter(|changes| !changes.is_empty()) {
        ops.push(Op {
            at: Timestamp::now(),
            kind: OpKind::Edit,
            cause: cause.cloned(),
            changes,
            git: None,
            extra: Default::default(),
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
            identity: wire(&observation.identity),
            docs: host.docs.clone(),
            next_seq: host.next_seq,
            ops,
            commits: git.commits,
            refs: git.refs,
            reflog_gap: git.reflog_gap,
            extra: Default::default(),
        }),
        uncaused,
    ))
}

/// Resolve the worktrees of `candidates` and observe each for one window, holding their locks.
/// `stored` are the roots an earlier window found: one that is gone resolves to whatever encloses
/// it, which is not the same worktree and is dropped.
fn observe_worktrees(
    event: &Event,
    tool_use_id: Option<&str>,
    cause: Option<&Cause>,
    candidates: &BTreeSet<PathBuf>,
    stored: &BTreeSet<PathBuf>,
) -> Result<(Vec<PendingWorktree>, Vec<WorktreeBatch>, u64), Error> {
    let mut identities: BTreeMap<PathBuf, Identity> = BTreeMap::new();
    for candidate in candidates {
        if let Some(found) = identity::resolve(candidate)?
            && (!stored.contains(candidate) || found.root == *candidate)
        {
            identities.insert(found.root.clone(), found);
        }
    }
    let mut worktrees = Vec::new();
    let mut batches = Vec::new();
    let mut uncaused_tool_calls = 0;
    for (root, found) in identities {
        // Sorted by root: every window takes worktree locks in the same order.
        let worktree_lock = lock(&found.git_dir.join("intxt/lock"))?;
        let state_path = found.git_dir.join("intxt/state.json");
        let mut host: WorktreeHost = load(&state_path)?;
        let (batch, uncaused) = observe_worktree(event, tool_use_id, found, cause, &mut host)?;
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
    Ok((worktrees, batches, uncaused_tool_calls))
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
    let cause = match (&hook.event, &hook.tool_use_id) {
        (Event::PostToolUse | Event::PostToolUseFailure, Some(tool_use_id)) => {
            let located = session.tool_turns.get(tool_use_id);
            Some(Cause {
                agent_id: located.and_then(|(agent, _)| agent.clone()),
                turn: located.map(|(_, turn)| turn.clone()),
                tool_use_id: tool_use_id.clone(),
                extra: Default::default(),
            })
        }
        _ => None,
    };
    let (worktrees, batches, uncaused_tool_calls) = observe_worktrees(
        &hook.event,
        hook.tool_use_id.as_deref(),
        cause.as_ref(),
        &candidates,
        &session.worktrees,
    )?;
    session.worktrees = worktrees
        .iter()
        .map(|pending| pending.root.clone())
        .collect();

    Ok(Prepared {
        batch: Batch {
            session: SessionBatch {
                session_id: hook.session_id.clone(),
                doc: session.doc.clone(),
                imported,
                subagents,
                extra: Default::default(),
            },
            worktrees: batches,
            uncaused_tool_calls,
            extra: Default::default(),
        },
        session: Some(StoredSession {
            path: session_path.to_owned(),
            host: session,
            _lock: session_lock,
        }),
        worktrees,
        finished_tool: cause.map(|cause| cause.tool_use_id),
    })
}

/// Observe the worktrees of an ACP session after one of its windows: a session start or the end
/// of a prompt turn. `session` is what the ACP recorder made of the window; the worktrees are
/// those holding `cwd`, those a tool call's locations name and `known`, the roots earlier windows
/// of the connection found ([`Prepared::worktree_roots`]).
///
/// There is no hook and no tool call to cause the ops, and no state file for the session: an ACP
/// recording lives as long as its connection. The worktrees' own state and locks are the same as
/// a hook's, so a session recorded through both agrees on sequence numbers.
pub fn prepare_acp(
    session: SessionBatch,
    cwd: &Path,
    known: &BTreeSet<PathBuf>,
) -> Result<Prepared, Error> {
    let mut candidates = known.clone();
    candidates.insert(cwd.to_owned());
    candidates.extend(
        session
            .imported
            .turns
            .values()
            .flat_map(|turn| &turn.tool_calls)
            .flat_map(|call| &call.locations)
            .map(|location| PathBuf::from(&location.path)),
    );
    // A window's end is `Stop`'s: no tool is in flight, so no pre-snapshot is pending.
    let (worktrees, batches, uncaused_tool_calls) =
        observe_worktrees(&Event::Stop, None, None, &candidates, known)?;
    Ok(Prepared {
        batch: Batch {
            session,
            worktrees: batches,
            uncaused_tool_calls,
            extra: Default::default(),
        },
        session: None,
        worktrees,
        finished_tool: None,
    })
}

impl Prepared {
    /// The roots of the worktrees this window observed.
    pub fn worktree_roots(&self) -> impl Iterator<Item = &Path> {
        self.worktrees.iter().map(|pending| pending.root.as_path())
    }

    /// Store what the recorder answered, persist every state file and release the locks.
    ///
    /// Worktree states go first and the session's last: a crash in between leaves the session
    /// behind, so the next hook imports its records again, which the recorder's idempotency keys
    /// absorb.
    pub fn commit(mut self, recorded: RecordedIds) -> Result<(), Error> {
        if let Some(stored) = &mut self.session {
            if let Some(doc) = recorded.session {
                stored.host.doc = Some(doc);
            }
            for (agent_id, doc) in recorded.subagents {
                stored
                    .host
                    .subagents
                    .get_mut(&agent_id)
                    .ok_or_else(|| {
                        Error::Format(format!("recorded an unknown subagent {agent_id}"))
                    })?
                    .doc = Some(doc);
            }
        }
        for pending in &mut self.worktrees {
            match recorded.worktrees.get(&pending.root.display().to_string()) {
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
        for pending in &self.worktrees {
            save(&pending.state_path, &pending.host)?;
        }
        if let Some(stored) = &mut self.session {
            if let Some(tool_use_id) = &self.finished_tool {
                stored.host.tool_turns.remove(tool_use_id);
            }
            save(&stored.path, &stored.host)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
