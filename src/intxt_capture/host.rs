//! The host half of capture (ADR 039 §4 and §5): what one hook invocation observes, and the local
//! state that makes the next invocation resume where this one ended.
//!
//! A hook runs [`prepare`], which imports the transcript and observes the session's worktrees
//! while holding their locks, and returns a [`Prepared`] with the [`Batch`] to record. Recording
//! happens elsewhere (the client library's `record_observations`, in `intxt_core::client`); its answer goes to
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
use crate::acp::Resume;
use crate::acp::Window;
use crate::git::Git;
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
    /// Windows opened by tool calls, by `tool_use_id` (`<session id>/<toolCallId>` for ACP),
    /// removed by the call's post hook. A call that never reports leaves its window here.
    pre: BTreeMap<String, OpenWindow>,
    /// Counts window openings and observations, to order them: windows (and their parallel
    /// calls) are cut into segments by when they opened relative to the last observation.
    clock: u64,
    /// `clock` at the last observation. A window opened at or before it is no boundary for the
    /// next diff: the changes up to the last observation are already recorded.
    observed_at: u64,
}

/// A tool call's window: the snapshot taken before it ran, when (`WorktreeHost::clock`) and which
/// call.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenWindow {
    snapshot: Snapshot,
    opened: u64,
    /// The call that opened it, to name it when a later observation finds it the only window
    /// open across a segment. A hook's `turn` is unknown when the window opens if the transcript
    /// lags; it is resolved from the session's `tool_turns` at that observation. An ACP call's
    /// cause is complete.
    cause: Cause,
}

impl WorktreeHost {
    /// The boundary snapshots of the next observation and, per segment (one more than there are
    /// boundaries), the call that is the only window open across it.
    fn segments(
        &self,
        closing: Option<&str>,
        tool_cause: Option<&Cause>,
        tool_turns: &BTreeMap<String, (Option<String>, String)>,
    ) -> (Vec<&Snapshot>, Vec<Option<Cause>>) {
        // Overlapping windows (parallel tool calls) are cut into segments so the op log stays
        // linear. The windows that opened after the last observation are boundaries, in opening
        // order: the diffs run last observation -> boundary 1 -> ... -> now, never backwards. A
        // window opened at or before the last observation is not a boundary (its changes up to it
        // are recorded) but is still open across every segment, as is the closing call whose window
        // was never stored (its post hook came without a pre).
        let mut boundaries: Vec<(&str, &OpenWindow)> = self
            .pre
            .iter()
            .filter(|(_, window)| window.opened > self.observed_at)
            .map(|(key, window)| (key.as_str(), window))
            .collect();
        boundaries.sort_by_key(|(_, window)| window.opened);
        let mut open: Vec<&str> = self
            .pre
            .iter()
            .filter(|(_, window)| window.opened <= self.observed_at)
            .map(|(key, _)| key.as_str())
            .collect();
        if let Some(key) = closing.filter(|key| !self.pre.contains_key(*key)) {
            open.push(key);
        }
        // Per segment, the call that is the only window open across it, if there is one: the one
        // case where the segment has a cause to name. With no window open the edit was nobody's;
        // with several open, who made it is unknown, so it has no cause either. Naming the several
        // (or narrowing them by the paths calls report) waits for the operator's decision on
        // `WorktreeOp` `causes` (docs/scratch/intxt-overlapping-windows.md, open question 1). A call
        // that is not the closing one is named by what its window stored, its turn resolved from the
        // session's `tool_turns` as the closing call's is.
        let resolve = |key: &str| -> Option<Cause> {
            if Some(key) == closing {
                return tool_cause.cloned();
            }
            let mut cause = self.pre.get(key)?.cause.clone();
            if cause.turn.is_none()
                && let Some((agent_id, turn)) = tool_turns.get(&cause.tool_use_id)
            {
                cause.agent_id = agent_id.clone();
                cause.turn = Some(turn.clone());
            }
            Some(cause)
        };
        let sole: Vec<Option<Cause>> = std::iter::once(None)
            .chain(boundaries.iter().map(|(key, _)| Some(*key)))
            .map(|opened_here| {
                open.extend(opened_here);
                match open[..] {
                    [only] => resolve(only),
                    _ => None,
                }
            })
            .collect();
        let snapshots = boundaries
            .iter()
            .map(|(_, window)| &window.snapshot)
            .collect();
        (snapshots, sole)
    }
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
    /// A hook's session state; none for an ACP window.
    session: Option<StoredSession>,
    /// An ACP session's [`Resume`], for the windows that record session content.
    acp: Option<StoredAcp>,
    worktrees: Vec<PendingWorktree>,
    /// The post hook's call, whose `tool_turns` entry goes with the commit.
    finished_tool: Option<String>,
}

struct StoredSession {
    path: PathBuf,
    host: SessionHost,
    _lock: File,
}

struct StoredAcp {
    path: PathBuf,
    resume: Resume,
    _lock: File,
}

/// The state file of ACP session `session_id` in `sessions_dir`, the directory of the hook
/// sessions' state files (`<hook session id>.json`). The name is `acp.` and the id with every
/// byte but ASCII alphanumerics, `-` and `_` escaped as `%xx`: ACP ids are arbitrary strings, and
/// a hook's id (checked to be those bytes only) has no `.`, so the two kinds never collide.
pub(crate) fn acp_state_path(sessions_dir: &Path, session_id: &str) -> PathBuf {
    let mut name = String::from("acp.");
    for byte in session_id.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            name.push(byte as char);
        } else {
            name.push_str(&format!("%{byte:02x}"));
        }
    }
    name.push_str(".json");
    sessions_dir.join(name)
}

/// What the host holds of ACP session `session_id`, if an earlier recording committed it.
pub(crate) fn acp_resume(sessions_dir: &Path, session_id: &str) -> Result<Option<Resume>, Error> {
    load(&acp_state_path(sessions_dir, session_id))
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
    tool_turns: &BTreeMap<String, (Option<String>, String)>,
    import: &[String],
    host: &mut WorktreeHost,
) -> Result<(Option<WorktreeBatch>, u64), Error> {
    if let (Event::PreToolUse, Some(tool_use_id)) = (event, tool_use_id) {
        host.clock += 1;
        host.pre.insert(
            tool_use_id.to_owned(),
            OpenWindow {
                snapshot: Snapshot::take(&found.root)?,
                opened: host.clock,
                cause: tool_cause
                    .expect("a window opens with its call's cause")
                    .clone(),
            },
        );
        return Ok((None, 0));
    }
    let ends_session = matches!(event, Event::Stop | Event::SessionEnd);
    let closing = tool_use_id.filter(|_| !ends_session);

    let (snapshots, sole) = host.segments(closing, tool_cause, tool_turns);
    let (observation, next) = observe::observe(&host.observed, found, &snapshots, import)?;
    host.observed = next;
    host.clock += 1;
    host.observed_at = host.clock;
    if ends_session {
        host.pre.clear();
    } else if let Some(key) = closing {
        host.pre.remove(key);
    }

    let git = observation.git;
    let mut ops = Vec::new();
    let mut uncaused = 0;
    // A sole call whose turn is still unknown (the transcript lags) leaves its ops uncaused and
    // counted, as for the closing call.
    let mut segment_cause = |sole: &Option<Cause>, ops_added: usize| {
        let cause = sole.as_ref()?;
        if cause.turn.is_none() {
            uncaused += ops_added as u64;
            return None;
        }
        Some(cause.clone())
    };
    for (changes, sole) in observation.earlier.into_iter().zip(&sole) {
        if changes.is_empty() {
            continue;
        }
        ops.push(Op {
            at: Timestamp::now(),
            kind: OpKind::Edit,
            cause: segment_cause(sole, 1),
            changes,
            git: None,
            extra: Default::default(),
        });
    }
    // The last segment runs to now. A window in which HEAD moved records its file changes on the
    // move that closed it (ADR 039 §3): a checkout is not an edit.
    let last_sole = sole.last().cloned().flatten();
    let mut changes = Some(observation.changes);
    let last_move = git.moves.len().wrapping_sub(1);
    let mut last_ops = Vec::new();
    for (index, mv) in git.moves.iter().enumerate() {
        last_ops.push(Op {
            at: mv.at,
            kind: mv.kind.clone(),
            cause: None,
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
        last_ops.push(Op {
            at: Timestamp::now(),
            kind: OpKind::Edit,
            cause: None,
            changes,
            git: None,
            extra: Default::default(),
        });
    }
    let last_cause = segment_cause(&last_sole, last_ops.len());
    for op in &mut last_ops {
        op.cause = last_cause.clone();
    }
    ops.extend(last_ops);
    // Every blob an op names stays reachable from the worktree's keep ref before the op is
    // recorded, under the worktree lock the caller holds (`Git::keep`).
    let blobs: BTreeSet<&str> = ops
        .iter()
        .flat_map(|op| &op.changes)
        .filter_map(|change| change.blob.as_deref())
        .collect();
    Git::new(&observation.identity.root).keep(&observation.identity.worktree_id, &blobs)?;
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
    tool_turns: &BTreeMap<String, (Option<String>, String)>,
    candidates: &BTreeSet<PathBuf>,
    stored: &BTreeSet<PathBuf>,
    import: &[String],
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
        let (batch, uncaused) = observe_worktree(
            event,
            tool_use_id,
            found,
            cause,
            tool_turns,
            import,
            &mut host,
        )?;
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

/// What capture holds for one worktree on this machine: the documents it recorded the worktree
/// in and how many ops it has recorded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordedWorktree {
    pub docs: WorktreeDocs,
    /// The sequence number of the next op, which is the number of ops recorded.
    pub ops: u64,
}

/// What capture recorded for the worktree `identity` names, from its stored state; empty for a
/// worktree no hook has observed. The state is written atomically, so no lock is taken.
pub fn recorded_worktree(identity: &Identity) -> Result<RecordedWorktree, Error> {
    let host: WorktreeHost = load(&identity.git_dir.join("intxt/state.json"))?;
    Ok(RecordedWorktree {
        docs: host.docs,
        ops: host.next_seq,
    })
}

/// A session capture holds state for, as `sessions_in` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedSession {
    /// The harness's session id: the state file's name.
    pub session_id: String,
    /// The session's document, once the recorder created it.
    pub doc: Option<String>,
    /// When the session's state last changed, which is when a hook last recorded for it.
    pub modified: std::time::SystemTime,
}

/// The sessions in `sessions_dir` (`<node dir>/intxt/sessions`) that touched the worktree rooted
/// at `root`, the most recently recorded first.
pub fn sessions_in(sessions_dir: &Path, root: &Path) -> Result<Vec<RecordedSession>, Error> {
    let entries = match std::fs::read_dir(sessions_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut sessions = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        // ACP sessions' state files (`acp.<id>.json`, [`acp_state_path`]) share the directory
        // but hold an ACP resume point, not a hook session, and record no worktrees.
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with("acp."))
        {
            continue;
        }
        let host: SessionHost = load(&path)?;
        if !host.worktrees.contains(root) {
            continue;
        }
        sessions.push(RecordedSession {
            session_id: path
                .file_stem()
                .expect("a path with an extension has a stem")
                .to_string_lossy()
                .into_owned(),
            doc: host.doc,
            modified: std::fs::metadata(&path)?.modified()?,
        });
    }
    sessions.sort_by_key(|session| std::cmp::Reverse(session.modified));
    Ok(sessions)
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
        (Event::PreToolUse | Event::PostToolUse | Event::PostToolUseFailure, Some(tool_use_id)) => {
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
        &session.tool_turns,
        &candidates,
        &session.worktrees,
        &[],
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
        acp: None,
        worktrees,
        finished_tool: cause
            .filter(|_| !matches!(hook.event, Event::PreToolUse))
            .map(|cause| cause.tool_use_id),
    })
}

/// Observe the worktrees of an ACP session for one [`Window`] of its recording: a session start
/// or the end of a prompt turn (`Boundary`), or a tool call's opening or closing. `session` is
/// what the ACP recorder made of the window; the worktrees are those holding `cwd`, those the
/// session's tool calls (or the window's) name and `known`, the roots earlier windows of the
/// connection found ([`Prepared::worktree_roots`]).
///
/// The session has a state file of its own in `sessions_dir` (see [`acp_state_path`]), a
/// [`Resume`] that outlives the connection: the windows that record session content (`Boundary`)
/// hold its lock, fold the batch into it and commit it with the recorder's answer, so a later
/// connection loading the session continues it. The worktrees' own state and locks are the same
/// as a hook's, so a session recorded through both agrees on sequence numbers, and the window's
/// calls take the hook path's shape: `Open` is a `PreToolUse` (the pre-snapshot goes in the worktree's state, keyed by the
/// call's id, and nothing is batched), `Close` a `PostToolUse` whose ops are caused by the call.
/// A `Boundary` is a `Stop`: no tool is in flight, so it drops the pre-snapshots of calls that
/// never closed.
pub fn prepare_acp(
    session: SessionBatch,
    sessions_dir: &Path,
    cwd: &Path,
    known: &BTreeSet<PathBuf>,
    window: &Window,
) -> Result<Prepared, Error> {
    // The session's lock before the worktrees', as for a hook.
    let acp = if matches!(window, Window::Boundary) {
        let path = acp_state_path(sessions_dir, &session.session_id);
        let mut lock_path: OsString = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let session_lock = lock(Path::new(&lock_path))?;
        let mut resume: Resume = load::<Option<Resume>>(&path)?.unwrap_or_default();
        resume.absorb(&session);
        Some(StoredAcp {
            path,
            resume,
            _lock: session_lock,
        })
    } else {
        None
    };
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
    let (event, cause) = match window {
        Window::Boundary => (Event::Stop, None),
        Window::Open(tool) => {
            candidates.extend(tool.paths.iter().cloned());
            (Event::PreToolUse, Some(&tool.cause))
        }
        Window::Close(tool) => {
            candidates.extend(tool.paths.iter().cloned());
            (Event::PostToolUse, Some(&tool.cause))
        }
    };
    // Worktree state is shared by every session on the worktree and ACP ids are only unique
    // within their session, so the pre-snapshot is keyed by both. (A hook's `tool_use_id` is
    // globally unique, so hooks key by it alone.)
    let key = cause.map(|cause| format!("{}/{}", session.session_id, cause.tool_use_id));
    let (worktrees, batches, uncaused_tool_calls) = observe_worktrees(
        &event,
        key.as_deref(),
        cause,
        &BTreeMap::new(),
        &candidates,
        known,
        &[],
    )?;
    Ok(Prepared {
        batch: Batch {
            session,
            worktrees: batches,
            uncaused_tool_calls,
            extra: Default::default(),
        },
        session: None,
        acp,
        worktrees,
        finished_tool: None,
    })
}

/// Observe the worktree `identity` names for an import of git history: the commits in `import`
/// are recorded with the worktree's identity documents, as a hook's observation records the
/// commits its moves made (FDR 008 W6; [`crate::git::observe`] says how). It is a full
/// observation under the worktree's lock, so state and sequence numbers are the ones hooks
/// continue from: what the observation reports (a first sight's baseline, the refs, moves since
/// the last look) is what the next hook would have reported, and it will not report it twice.
///
/// There is no session, none having been recorded; the batch's session half is empty. The event
/// is no hook's: it neither opens a pre-snapshot nor drops the ones other sessions' tool calls
/// have in flight.
pub fn prepare_import(identity: &Identity, import: &[String]) -> Result<Prepared, Error> {
    let candidates = BTreeSet::from([identity.root.clone()]);
    let (worktrees, batches, uncaused_tool_calls) = observe_worktrees(
        &Event::Other("import".to_owned()),
        None,
        None,
        &BTreeMap::new(),
        &candidates,
        &BTreeSet::new(),
        import,
    )?;
    Ok(Prepared {
        batch: Batch {
            session: SessionBatch {
                session_id: String::new(),
                doc: None,
                imported: intxt_core::capture::Imported::default(),
                subagents: Vec::new(),
                extra: Default::default(),
            },
            worktrees: batches,
            uncaused_tool_calls,
            extra: Default::default(),
        },
        session: None,
        acp: None,
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
            if let Some(doc) = &recorded.session {
                stored.host.doc = Some(doc.clone());
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
        if let Some(mut stored) = self.acp.take() {
            if let Some(doc) = &recorded.session {
                stored.resume.doc = Some(doc.clone());
            }
            save(&stored.path, &stored.resume)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
