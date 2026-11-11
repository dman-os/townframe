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
//! The recorder emits a batch when a session starts and when a prompt turn ends. Each is recorded
//! the way `day x intxt capture` records a hook's batch ([`capture::record_prepared`]), after
//! observing the session's `cwd` worktree (and those its tool calls named), so the edits of a turn
//! become ops as they do under hooks. The first observation of a worktree is the session's start,
//! so the turn's ops are the turn's own.

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
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::unbounded_channel;
use utils_rs::prelude::Timestamp;

/// Forward `from` to `to` until `from` ends, teeing each chunk after it has been forwarded. A
/// peer that closed its end of the pipe ends the pump quietly: that is the session ending.
async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    direction: Direction,
    mut tee: Option<UnboundedSender<(Direction, Timestamp, Vec<u8>)>>,
) -> std::io::Result<()> {
    let mut chunk = vec![0; 64 * 1024];
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
        if let Some(sender) = &tee
            && sender
                .send((direction, at, chunk[..read].to_vec()))
                .is_err()
        {
            // Recording ended; the session goes on.
            tee = None;
        }
    }
    // Closing the agent's stdin is what tells it the client left.
    match to.shutdown().await {
        Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => other,
    }
}

/// Run `agent` with `input` as its stdin and its stdout copied to `output`, recording the
/// exchange: `record` is called for each [`Emit`] and answers the session's document id once the
/// node holds one. Returns the agent's exit status.
async fn proxy(
    agent: &[String],
    thinking: bool,
    input: impl AsyncRead + Unpin,
    output: impl AsyncWrite + Unpin,
    mut record: impl AsyncFnMut(Emit) -> Res<Option<String>>,
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

    let (tee, mut teed) = unbounded_channel();
    let to_agent = pump(input, child_in, Direction::ToAgent, Some(tee.clone()));
    let from_agent = pump(child_out, output, Direction::FromAgent, Some(tee));
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
        let mut recorder = Recorder::new();
        if thinking {
            recorder = recorder.recording_thinking();
        }
        let mut record_all = async |recorder: &mut Recorder, emits: Vec<Emit>| -> Res<()> {
            for emit in emits {
                let session_id = emit.session.session_id.clone();
                if let Some(doc) = record(emit).await? {
                    recorder.set_doc(&session_id, doc);
                }
            }
            Ok(())
        };
        let outcome: Res<()> = async {
            while let Some((direction, at, bytes)) = teed.recv().await {
                let emits = recorder.feed(direction, at, &bytes)?;
                record_all(&mut recorder, emits).await?;
            }
            // Both pumps are done: the connection ended.
            let emits = recorder.finish();
            record_all(&mut recorder, emits).await
        }
        .await;
        if let Err(error) = outcome {
            let line = format!("{error:#}").replace('\n', " ");
            eprintln!("intxt record: recording stopped, the session is not affected: {line}");
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
    known: BTreeSet<PathBuf>,
}

impl Recording {
    async fn connect() -> Res<Self> {
        let node_dir = lazy::config().await?.node_dir.clone();
        super::require_daemon(&node_dir).await?;
        Ok(Self {
            client: lazy::node().await?,
            node_dir: node_dir.root().to_owned(),
            known: BTreeSet::new(),
        })
    }

    /// Observe the worktrees of `emit` and record it; the session's document id the node
    /// answered.
    async fn record(&mut self, emit: Emit) -> Res<Option<String>> {
        let Emit { cwd, session } = emit;
        let known = self.known.clone();
        // Locks and git: blocking work, off the runtime's threads (see `capture::run`).
        let prepared = tokio::task::spawn_blocking(move || {
            intxt_capture::host::prepare_acp(session, &cwd, &known)
        })
        .await??;
        self.known
            .extend(prepared.worktree_roots().map(Path::to_owned));
        let mut summary = capture::Summary::default();
        let recorded =
            capture::record_prepared(&self.client, &self.node_dir, prepared, &mut summary).await?;
        super::report(&summary, false);
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
    let status = proxy(
        &agent,
        thinking,
        tokio::io::stdin(),
        tokio::io::stdout(),
        async |emit: Emit| {
            let recording = match &mut recording {
                Some(recording) => recording,
                None => recording.insert(Recording::connect().await?),
            };
            recording.record(emit).await
        },
    )
    .await?;
    Ok(exit_code(status))
}

#[cfg(test)]
mod tests;
