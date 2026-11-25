//! The node as a background server on its Unix socket (ADR 033 section 5).
//!
//! Whoever holds the node serves it: the serving process is the node booted with the socket
//! attached, holding the node lock for as long as it lives. The lock is its identity: `stop` finds
//! the pid in it and waits for its release to learn the process has exited, and a socket file
//! nobody listens on while the lock is free is a leftover of a server that died without cleaning
//! up. Nothing here sleeps or polls: `start --bg` waits for the server's readiness line, `stop` blocks
//! on the lock.

use crate::interlude::*;
use crate::out::out;
use day_node::Booted;
use day_node::LockRole;
use day_node::NodeDir;
use day_node::SocketState;

use std::future::Future;
use tokio::io::AsyncBufReadExt;

/// What the foreground server prints, once, when it is serving. `start --bg` waits for this line.
const READY_PREFIX: &str = "listening on ";

/// Boots the node with its socket attached (and its peers service, when `node.json` configures
/// one) and serves until `shutdown` resolves, then stops it. `ready` is called once, when the
/// socket accepts connections, with the booted node.
pub async fn serve(
    dir: &NodeDir,
    ready: impl FnOnce(&Booted) -> Res<()>,
    shutdown: impl Future<Output = ()>,
) -> Res<()> {
    let lock = loop {
        match dir.try_lock(LockRole::Serve)? {
            Ok(lock) => break lock,
            Err(held) if held.role == Some(LockRole::Command) => {
                // A command has the node open for the length of its work: wait for it.
                let waiting = dir.clone();
                tokio::task::spawn_blocking(move || waiting.wait_for_lock_release())
                    .await
                    .expect(ERROR_TOKIO)?;
            }
            Err(held) => eyre::bail!(
                "{held} already has the node at {} open",
                dir.root().display()
            ),
        }
    };
    dir.ensure_run_dir()?;
    let booted = Booted::boot(dir, lock, LockRole::Serve, crate::client::empty_schemas()?).await?;
    let served = ready(&booted);
    if served.is_ok() {
        shutdown.await;
    }
    // Stopped whether or not readiness could be announced, so a failure never leaves the
    // socket behind.
    booted.stop().await?;
    served
}

/// `day node start`: serves the node on its socket in the foreground until SIGTERM or Ctrl-C, or
/// with `bg` spawns that as a background process and returns once it is serving.
pub async fn start(bg: bool) -> Res<ExitCode> {
    let conf = lazy::config().await?;
    if bg {
        start_background(&conf.node_dir).await
    } else {
        run_foreground(&conf.node_dir).await
    }
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
                .node()
                .listeners
                .unix_socket
                .as_ref()
                .expect("the server boots with its socket attached");
            let mut out = out();
            writeln!(out, "{READY_PREFIX}{}", socket.display())?;
            out.flush()?;
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

/// What [`ensure_background`] found or did.
pub(crate) enum Background {
    /// It spawned the server, which is now serving on `socket`.
    Started { pid: u32, socket: String },
    /// A server was already serving, or another caller's spawn won the node first.
    Running,
}

/// A server for `dir`, in the background: the running one, or one spawned and awaited until it
/// is serving. It prints nothing, so a hook (whose stdout is the harness's to read) can call it.
///
/// Callers racing here (parallel hooks) each spawn a server, and the node lock admits one: a
/// spawned server that exits before its readiness line because another holds the node to serve
/// is the race lost, which is [`Background::Running`] once the winner is serving.
pub(crate) async fn ensure_background(dir: &NodeDir) -> Res<Background> {
    // A command that has the node open is not the server: wait for it to finish, then start serving.
    // A server that is still booting is awaited, so nobody is told about one that is not serving.
    if let Err(held) = dir.try_lock(LockRole::Command)? {
        if held.role == Some(LockRole::Serve) {
            let waiting = dir.clone();
            tokio::task::spawn_blocking(move || waiting.wait_until_serving())
                .await
                .expect(ERROR_TOKIO)?;
            return Ok(Background::Running);
        }
        let waiting = dir.clone();
        tokio::task::spawn_blocking(move || waiting.wait_for_lock_release())
            .await
            .expect(ERROR_TOKIO)?;
    }
    dir.ensure_run_dir()?;
    let log_path = dir.log_path();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .wrap_err_with(|| format!("opening {}", log_path.display()))?;
    // Its own process group, so a Ctrl-C at this terminal is not also delivered to the server.
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .arg("--node")
        .arg(dir.root())
        .args(["node", "start"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(log)
        .process_group(0)
        .spawn()
        .wrap_err("spawning the server")?;
    let pid = child
        .id()
        .expect("a child that has not been waited on has a pid");
    let mut stdout = tokio::io::BufReader::new(child.stdout.take().expect("stdout is piped"));
    let mut line = String::new();
    // Readiness is the server's first line. End of input before it means the server exited.
    if stdout.read_line(&mut line).await? == 0 {
        let status = child.wait().await?;
        if let Err(held) = dir.try_lock(LockRole::Command)?
            && held.role == Some(LockRole::Serve)
        {
            let waiting = dir.clone();
            tokio::task::spawn_blocking(move || waiting.wait_until_serving())
                .await
                .expect(ERROR_TOKIO)?;
            return Ok(Background::Running);
        }
        eyre::bail!(
            "the server exited with {status} before it was serving; see {}",
            log_path.display()
        );
    }
    eyre::ensure!(
        line.starts_with(READY_PREFIX),
        "the server's first line was not its readiness line: {line:?}"
    );
    let socket = line
        .trim_end()
        .strip_prefix(READY_PREFIX)
        .expect("the line starts with the readiness prefix, as checked above")
        .to_owned();
    Ok(Background::Started { pid, socket })
}

async fn start_background(dir: &NodeDir) -> Res<ExitCode> {
    match ensure_background(dir).await? {
        Background::Started { pid, socket } => {
            writeln!(out(), "node serving in the background (pid {pid}) on {socket}")?
        }
        Background::Running => writeln!(
            out(),
            "a server is already running on {}",
            dir.socket_path()?.display()
        )?,
    }
    Ok(ExitCode::SUCCESS)
}

/// `day node stop`: signals the background server and returns once it has exited.
pub async fn stop() -> Res<ExitCode> {
    let mut out = out();
    let conf = lazy::config().await?;
    let dir = &conf.node_dir;
    let held = match dir.try_lock(LockRole::Command)? {
        Ok(_free) => {
            // Nobody has the node. A socket file left behind is a server that died uncleanly.
            if dir.probe_socket().await? == SocketState::Stale {
                std::fs::remove_file(dir.socket_path()?)?;
                writeln!(out, "no server running; removed its stale socket")?;
            } else {
                writeln!(out, "no server running")?;
            }
            return Ok(ExitCode::SUCCESS);
        }
        Err(held) => held,
    };
    if held.role != Some(LockRole::Serve) {
        writeln!(
            out,
            "no server running; {held} has the node open for a command"
        )?;
        return Ok(ExitCode::SUCCESS);
    }
    let pid = held
        .pid
        .ok_or_eyre("the server holds the node lock but has not recorded its pid")?;
    // SAFETY: kill(2) takes plain integers and has no memory preconditions.
    let sent = unsafe { libc::kill(pid.try_into()?, libc::SIGTERM) };
    if sent != 0 {
        return Err(std::io::Error::last_os_error())
            .wrap_err_with(|| format!("signalling the server (pid {pid})"));
    }
    let dir = dir.clone();
    tokio::task::spawn_blocking(move || dir.wait_for_lock_release())
        .await
        .expect(ERROR_TOKIO)?;
    writeln!(out, "server stopped (pid {pid})")?;
    Ok(ExitCode::SUCCESS)
}
