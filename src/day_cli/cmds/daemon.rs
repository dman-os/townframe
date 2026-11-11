//! The node as a background server on its Unix socket (ADR 033 section 5).
//!
//! A daemon is the node booted with the socket attached, holding the node lock for as long as it
//! lives. The lock is the daemon's identity: `stop` finds the pid in it and waits for its release
//! to learn the process has exited, and a socket file nobody listens on while the lock is free
//! is a leftover of a daemon that died without cleaning up. Nothing here sleeps or polls:
//! `start` waits for the daemon's readiness line, `stop` blocks on the lock.

use crate::context::Booted;
use crate::context::NodeDir;
use crate::context::SocketState;
use crate::interlude::*;

use std::future::Future;
use std::io::Write;
use tokio::io::AsyncBufReadExt;

#[derive(Debug, clap::Subcommand)]
pub enum DaemonCommands {
    /// Serve the node on its socket in the foreground until SIGTERM or Ctrl-C
    Run,
    /// Start a background daemon and return once it is serving
    Start,
    /// Stop the background daemon and return once it has exited
    Stop,
}

/// What the foreground daemon prints, once, when it is serving. `start` waits for this line.
const READY_PREFIX: &str = "listening on ";

pub async fn run(command: DaemonCommands) -> Res<ExitCode> {
    let conf = lazy::config().await?;
    let dir = &conf.node_dir;
    match command {
        DaemonCommands::Run => run_foreground(dir).await,
        DaemonCommands::Start => start(dir).await,
        DaemonCommands::Stop => stop(dir).await,
    }
}

/// Boots the node with its socket attached (and its peers service, when `node.json` configures
/// one) and serves until `shutdown` resolves, then stops it. `ready` is called once, when the
/// socket accepts connections, with the booted node.
pub async fn serve(
    dir: &NodeDir,
    ready: impl FnOnce(&Booted) -> Res<()>,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let lock = match dir.try_lock()? {
        Ok(lock) => lock,
        Err(held) => eyre::bail!(
            "{held} already has the node at {} open",
            dir.root().display()
        ),
    };
    dir.ensure_run_dir()?;
    let booted = Booted::boot(dir, lock, true).await?;
    let served = ready(&booted);
    if served.is_ok() {
        shutdown.await;
    }
    // Stopped whether or not readiness could be announced, so a failure never leaves the
    // socket behind.
    booted.stop().await?;
    served
}

async fn run_foreground(dir: &NodeDir) -> Res<ExitCode> {
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;
    // Registered before the node boots, so a stop that arrives while it is booting is held for
    // the shutdown instead of killing the process mid-boot.
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    serve(
        dir,
        |booted| {
            let socket = booted
                .node
                .listeners
                .unix_socket
                .as_ref()
                .expect("the daemon boots with its socket attached");
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{READY_PREFIX}{}", socket.display())?;
            stdout.flush()?;
            Ok(())
        },
        async {
            tokio::select! {
                _ = terminate.recv() => {}
                _ = interrupt.recv() => {}
            }
        },
    )
    .await?;
    Ok(ExitCode::SUCCESS)
}

async fn start(dir: &NodeDir) -> Res<ExitCode> {
    match dir.probe_socket().await? {
        SocketState::Live => {
            println!("daemon already running on {}", dir.socket_path()?.display());
            return Ok(ExitCode::SUCCESS);
        }
        SocketState::Stale | SocketState::Absent => {}
    }
    dir.ensure_run_dir()?;
    let log_path = dir.log_path();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .wrap_err_with(|| format!("opening {}", log_path.display()))?;
    // Its own process group, so a Ctrl-C at this terminal is not also delivered to the daemon.
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .arg("--node")
        .arg(dir.root())
        .args(["daemon", "run"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(log)
        .process_group(0)
        .spawn()
        .wrap_err("spawning the daemon")?;
    let pid = child
        .id()
        .expect("a child that has not been waited on has a pid");
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().expect("stdout is piped"));
    let mut line = String::new();
    // Readiness is the daemon's first line. End of input before it means the daemon exited.
    if stdout.read_line(&mut line).await? == 0 {
        let status = child.wait().await?;
        eyre::bail!(
            "the daemon exited with {status} before it was serving; see {}",
            log_path.display()
        );
    }
    eyre::ensure!(
        line.starts_with(READY_PREFIX),
        "the daemon's first line was not its readiness line: {line:?}"
    );
    println!("daemon started (pid {pid}) {}", line.trim_end());
    Ok(ExitCode::SUCCESS)
}

async fn stop(dir: &NodeDir) -> Res<ExitCode> {
    let held = match dir.try_lock()? {
        Ok(_free) => {
            // Nobody has the node. A socket file left behind is a daemon that died uncleanly.
            if dir.probe_socket().await? == SocketState::Stale {
                std::fs::remove_file(dir.socket_path()?)?;
                println!("daemon not running; removed its stale socket");
            } else {
                println!("daemon not running");
            }
            return Ok(ExitCode::SUCCESS);
        }
        Err(held) => held,
    };
    eyre::ensure!(
        dir.probe_socket().await? == SocketState::Live,
        "{held} has the node open but is not serving its socket, so it is not a running daemon"
    );
    let pid = held
        .pid
        .ok_or_eyre("the daemon holds the node lock but has not recorded its pid")?;
    // SAFETY: kill(2) takes plain integers and has no memory preconditions.
    let sent = unsafe { libc::kill(pid.try_into()?, libc::SIGTERM) };
    if sent != 0 {
        return Err(std::io::Error::last_os_error())
            .wrap_err_with(|| format!("signalling the daemon (pid {pid})"));
    }
    let dir = dir.clone();
    tokio::task::spawn_blocking(move || dir.wait_for_lock_release())
        .await
        .expect(ERROR_TOKIO)?;
    println!("daemon stopped (pid {pid})");
    Ok(ExitCode::SUCCESS)
}
