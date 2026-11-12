//! `day x intxt record [--] <agent command…>`: a transparent stdio proxy that records an ACP
//! session (FDR 008, "Session recording"). An ACP client (an editor) launches this command in
//! place of the agent; it spawns the agent, forwards the client's stdin to the agent and the
//! agent's stdout to the client byte for byte, passes the agent's stderr through, and tees what
//! passes to `intxt_capture`'s ACP [`Recorder`].
//!
//! Forwarding never waits on recording: the tee is an unbounded channel into a task of its own,
//! and the bytes are forwarded before they are teed. A recording that fails (a message the
//! recorder cannot follow, an unreachable node) says so on stderr and stops; the session goes on
//! unrecorded. That is the one place a failure is reported and survived, because a proxy that
//! broke the session it records would not be transparent. A closed tee is the signal that
//! recording ended; it is not an error of the forwarding.
//!
//! The recorder emits when a session starts, when a prompt turn ends, and when a tool call's
//! window opens and closes. Each is recorded the way `day x intxt capture` records a hook's batch
//! ([`capture::record_prepared`]), after observing the session's `cwd` worktree (and those its
//! tool calls named), so the edits become ops as they do under hooks: one per tool call window,
//! caused by the call, and the changes outside any window in the turn-end op. The first
//! observation of a worktree is the session's start, so the turn's ops are the turn's own. The
//! drain task records emits one at a time in the order they were teed, so a window's ops reach
//! the node before the turn-end ops that follow it.

use super::capture;
use crate::client::NodeClient;
use crate::interlude::*;

use intxt_capture::acp::Direction;
use intxt_capture::acp::Emit;
use intxt_capture::acp::Recorder;
use std::collections::BTreeSet;
use std::pin::pin;
use std::process::ExitStatus;
use std::process::Stdio;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::TrySendError;
use utils_rs::prelude::Timestamp;

/// The largest chunk a pump reads, so the bytes pending in the tee never exceed its chunk
/// capacity times this.
const CHUNK_BYTES: usize = 64 * 1024;

/// The tee holds at most this many chunks: 1024 chunks of at most 64 KiB, a budget of 64 MiB
/// pending for a recorder that falls behind the session.
const TEE_CHUNKS: usize = 1024;

/// Forward `from` to `to` until `from` ends, teeing each chunk after it has been forwarded. A
/// peer that closed its end of the pipe ends the pump quietly: that is the session ending.
///
/// The tee comes after the forward, which keeps the recorder's order causal: the client answers
/// only what it was forwarded, so the recorder has seen a message before it sees its answer.
///
/// The pump never waits on the tee: a full tee means recording fell behind, which `overflow`
/// announces, and the pump stops teeing and goes on forwarding.
async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    direction: Direction,
    mut tee: Option<Sender<(Direction, Timestamp, Vec<u8>)>>,
    overflow: &Notify,
) -> std::io::Result<()> {
    let mut chunk = vec![0; CHUNK_BYTES];
    loop {
        let read = from.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        let at = Timestamp::now();
        let written = async {
            to.write_all(&chunk[..read]).await?;
            to.flush().await
        }
        .await;
        match written {
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => return Ok(()),
            other => other?,
        }
        if let Some(sender) = &tee {
            match sender.try_send((direction, at, chunk[..read].to_vec())) {
                Ok(()) => {}
                // Recording ended; the session goes on.
                Err(TrySendError::Closed(_)) => tee = None,
                Err(TrySendError::Full(_)) => {
                    // A permit is stored, so it is seen even when the recorder is not waiting.
                    overflow.notify_one();
                    tee = None;
                }
            }
        }
    }
    // Closing the agent's stdin is what tells it the client left.
    match to.shutdown().await {
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => other,
    }
}

/// Run `agent` with `input` as its stdin and its stdout copied to `output`, recording the
/// exchange with `recorder`, or not at all when there is none: `record` is called for each
/// [`Emit`] and answers the session's document id once the node holds one. `stopped` is told why when recording stops, which never stops the session.
/// `tee_chunks` bounds the chunks pending for the recorder. Returns the agent's exit status.
async fn proxy(
    agent: &[String],
    recorder: Option<Recorder>,
    input: impl AsyncRead + Unpin,
    output: impl AsyncWrite + Unpin,
    tee_chunks: usize,
    mut record: impl AsyncFnMut(Emit) -> Res<Option<String>>,
    mut stopped: impl FnMut(&str),
) -> Res<ExitStatus> {
    let mut child = tokio::process::Command::new(&agent[0])
        .args(&agent[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .wrap_err_with(|| format!("starting the agent `{}`", agent[0]))?;
    let child_in = child.stdin.take().expect("the agent's stdin is piped");
    let child_out = child.stdout.take().expect("the agent's stdout is piped");

    let (tee, mut teed) = channel(tee_chunks);
    // Without a recorder nothing is teed.
    let tee = recorder.is_some().then_some(tee);
    let overflow = Notify::new();
    let to_agent = pump(input, child_in, Direction::ToAgent, tee.clone(), &overflow);
    let from_agent = pump(child_out, output, Direction::FromAgent, tee, &overflow);
    let pumping = async {
        let mut to_agent = pin!(to_agent);
        let mut from_agent = pin!(from_agent);
        // The agent closing its stdout ends the session whatever the client's input does (it may
        // never end); the client closing its input only ends it once the agent has finished.
        tokio::select! {
            done = &mut from_agent => done,
            done = &mut to_agent => {
                done?;
                from_agent.await
            }
        }
    };
    let recording = async {
        let Some(mut recorder) = recorder else {
            return;
        };
        let mut record_all = async |recorder: &mut Recorder, emits: Vec<Emit>| -> Res<()> {
            for emit in emits {
                let session_id = emit.session.session_id.clone();
                if let Some(doc) = record(emit).await? {
                    recorder.set_doc(&session_id, doc);
                }
            }
            Ok(())
        };
        let drained = async {
            while let Some((direction, at, bytes)) = teed.recv().await {
                let emits = recorder.feed(direction, at, &bytes)?;
                record_all(&mut recorder, emits).await?;
            }
            // Both pumps are done: the connection ended.
            let emits = recorder.finish();
            record_all(&mut recorder, emits).await
        };
        // Overflow cancels the drain wherever it is, including a `record` that never returns.
        let outcome: Res<()> = tokio::select! {
            biased;
            () = overflow.notified() => Err(eyre::eyre!(
                "recording fell behind the session ({tee_chunks} chunks of up to {CHUNK_BYTES} bytes were pending)"
            )),
            outcome = drained => outcome,
        };
        if let Err(error) = outcome {
            stopped(&format!("{error:#}").replace('\n', " "));
        }
    };
    let (pumped, ()) = tokio::join!(pumping, recording);
    pumped.wrap_err("forwarding between the client and the agent")?;
    Ok(child.wait().await?)
}

/// What recording a batch needs: the node and the worktrees found so far.
struct Recording {
    client: Arc<NodeClient>,
    node_dir: PathBuf,
    sessions_dir: PathBuf,
    known: BTreeSet<PathBuf>,
}

impl Recording {
    async fn connect() -> Res<Self> {
        let node_dir = lazy::config().await?.node_dir.clone();
        super::require_daemon(&node_dir).await?;
        Ok(Self {
            client: lazy::node().await?,
            sessions_dir: capture::sessions_dir(node_dir.root()),
            node_dir: node_dir.root().to_owned(),
            known: BTreeSet::new(),
        })
    }

    /// Observe the worktrees of `emit` and record it; the session's document id the node
    /// answered.
    async fn record(&mut self, emit: Emit) -> Res<Option<String>> {
        let Emit {
            cwd,
            session,
            window,
        } = emit;
        let known = self.known.clone();
        let sessions_dir = self.sessions_dir.clone();
        // Locks and git: blocking work, off the runtime's threads (see `capture::run`).
        let prepared = tokio::task::spawn_blocking(move || {
            intxt_capture::host::prepare_acp(session, &sessions_dir, &cwd, &known, &window)
        })
        .await??;
        self.known
            .extend(prepared.worktree_roots().map(Path::to_owned));
        let recorded = capture::record_prepared(&self.client, &self.node_dir, prepared).await?;
        Ok(recorded.and_then(|recorded| recorded.session))
    }
}

/// The exit code the agent's status becomes: its own, or 128 + the signal that ended it.
fn exit_code(status: ExitStatus) -> ExitCode {
    use std::os::unix::process::ExitStatusExt;
    match (status.code(), status.signal()) {
        (Some(code), _) => ExitCode::from(code as u8),
        (None, Some(signal)) => ExitCode::from(128u8.saturating_add(signal as u8)),
        (None, None) => ExitCode::FAILURE,
    }
}

pub(crate) async fn run(agent: Vec<String>, thinking: bool) -> Res<ExitCode> {
    // The node is reached when the first batch is ready, not before: the session must start even
    // when there is no daemon.
    let mut recording: Option<Recording> = None;
    // Intxt never fails a session's start: without the node's directory there is no state to
    // record into, so the session runs unrecorded, as when the daemon is absent.
    let recorder = match lazy::config().await {
        Ok(config) => {
            let recorder = Recorder::new(capture::sessions_dir(config.node_dir.root()));
            Some(if thinking {
                recorder.recording_thinking()
            } else {
                recorder
            })
        }
        Err(error) => {
            eprintln!(
                "intxt record: not recording, the session is not affected: {}",
                format!("{error:#}").replace('\n', " ")
            );
            None
        }
    };
    // `tokio::io::stdin()` reads on a blocking-pool thread that stays parked when the agent exits
    // while the client keeps our stdin open, and dropping a runtime waits for such threads. This
    // does not hang `day`: its runtime lives in a static (`lazy::rt`) that is never dropped, so
    // returning from `main` exits the process regardless (checked with a held-open stdin and an
    // agent that exits: `day x intxt record -- sh -c 'exit 3'` returns 3 at once). Keep it that
    // way: a runtime owned and dropped here would need `std::process::exit` instead.
    let status = proxy(
        &agent,
        recorder,
        tokio::io::stdin(),
        tokio::io::stdout(),
        TEE_CHUNKS,
        async |emit: Emit| {
            let recording = match &mut recording {
                Some(recording) => recording,
                None => recording.insert(Recording::connect().await?),
            };
            recording.record(emit).await
        },
        |reason| {
            eprintln!("intxt record: recording stopped, the session is not affected: {reason}")
        },
    )
    .await?;
    Ok(exit_code(status))
}

#[cfg(test)]
mod tests;
