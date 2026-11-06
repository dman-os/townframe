//! End-to-end tests for the `day` CLI, in two layers.
//!
//! - **Library layer** (`mod library`): the polymorphic [`NodeClient`] against real nodes, with
//!   the daemon served inside the test process by [`crate::cmds::daemon::serve`]. It proves that
//!   one call gives one answer whichever way the node is reached, and covers the lock and the
//!   stale socket. Readiness is the daemon's own `ready` callback and shutdown is its `shutdown`
//!   future, so nothing sleeps or polls. The keyring is the in-process mock that `secrets_rs`
//!   selects under its `test-support` feature.
//! - **Blackbox layer** (`mod blackbox`): the real `day` binary in a sandbox directory, asserting
//!   only on process behavior (exit codes, merged stdout and stderr, state left by earlier
//!   invocations). Cases are literate trycmd `.md` files: one file is one sandbox, and every
//!   ```console fence in it runs in order in the same directory. A case needs a sibling
//!   `*.out/` directory (with at least a `.keep`) to opt into the sandbox. This layer uses the
//!   platform keyring, since the daemon and the commands are separate processes.
//!
//! The blackbox layer needs `cargo build -p day_cli` first, as cargo does not build a package's
//! binary for its own unit tests; it refuses a binary older than the CLI's sources.

#![cfg(test)]

mod library {
    use crate::client::Mode;
    use crate::client::NodeClient;
    use crate::client::Reply;
    use crate::context::Booted;
    use crate::context::NodeDir;
    use crate::context::SocketState;
    use crate::interlude::*;
    use big_nodule_core::protocol::ListNodules;
    use service_utils_rs::Operation;
    use tokio::sync::oneshot;

    fn new_node() -> (tempfile::TempDir, NodeDir) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = NodeDir::new(tmp.path().join("node")).unwrap();
        dir.init().unwrap();
        (tmp, dir)
    }

    /// A daemon served from inside the test process, stopped by [`TestDaemon::stop`].
    struct TestDaemon {
        shutdown: oneshot::Sender<()>,
        task: tokio::task::JoinHandle<Res<()>>,
    }

    impl TestDaemon {
        /// Starts serving and returns once the socket accepts connections.
        async fn start(dir: &NodeDir) -> Self {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (shutdown, shutdown_rx) = oneshot::channel::<()>();
            let dir = dir.clone();
            let task = tokio::spawn(async move {
                crate::cmds::daemon::serve(
                    &dir,
                    |_| ready_tx.send(()).map_err(|()| eyre::eyre!("test is gone")),
                    async {
                        shutdown_rx
                            .await
                            .expect("the test stops the daemon by sending")
                    },
                )
                .await
            });
            if ready_rx.await.is_err() {
                panic!("the daemon failed before it was serving: {:?}", task.await);
            }
            Self { shutdown, task }
        }

        async fn stop(self) {
            self.shutdown.send(()).expect("the daemon is serving");
            self.task.await.unwrap().unwrap();
        }
    }

    async fn call(client: &NodeClient, nsid: &str, input: serde_json::Value) -> Reply {
        client.call(nsid, &input).await.unwrap()
    }

    /// Calls whose answers must not depend on how the node was reached: a query that succeeds, an
    /// operation nothing claims, and a real operation given input it must refuse.
    fn calls() -> Vec<(&'static str, serde_json::Value)> {
        vec![
            (ListNodules::XRPC.nsid, json!({})),
            ("org.example.nothing.here", json!({})),
            (
                big_cabinet_core::doc::CreateDoc::XRPC.nsid,
                json!({"nonsense": true}),
            ),
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn init_then_status_in_process() {
        let (_tmp, dir) = new_node();
        let client = NodeClient::connect(&dir).await.unwrap();
        assert_eq!(client.mode(), Mode::InProcess);
        assert_eq!(client.socket_state(), SocketState::Absent);
        let reply = call(&client, ListNodules::XRPC.nsid, json!({})).await;
        assert_eq!(reply.status, 200, "{reply:?}");
        assert_eq!(reply.body, json!([]));
        client.stop().await.unwrap();
        // Stopping released the node: it can be booted again by the next command.
        NodeClient::connect(&dir)
            .await
            .unwrap()
            .stop()
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_node_id_is_the_repos_peer_id() {
        let (_tmp, dir) = new_node();
        let lock = dir.try_lock().unwrap().unwrap();
        let booted = Booted::boot(&dir, lock, default()).await.unwrap();
        let peer = booted.node.repo.local_peer_id().to_bytes32().unwrap();
        assert_eq!(
            dir.settings().unwrap().node_id(),
            data_encoding::HEXLOWER.encode(&peer)
        );
        booted.stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_same_calls_answer_the_same_in_process_and_over_the_daemon() {
        let (_tmp, dir) = new_node();
        let in_process = NodeClient::connect(&dir).await.unwrap();
        assert_eq!(in_process.mode(), Mode::InProcess);
        let mut local = Vec::new();
        for (nsid, input) in calls() {
            local.push(call(&in_process, nsid, input).await);
        }
        in_process.stop().await.unwrap();

        let daemon = TestDaemon::start(&dir).await;
        let over_socket = NodeClient::connect(&dir).await.unwrap();
        assert_eq!(over_socket.mode(), Mode::Daemon);
        assert_eq!(over_socket.socket_state(), SocketState::Live);
        let mut remote = Vec::new();
        for (nsid, input) in calls() {
            remote.push(call(&over_socket, nsid, input).await);
        }
        over_socket.stop().await.unwrap();
        daemon.stop().await;

        assert_eq!(local, remote);
        assert_eq!(local[0].status, 200);
        assert_eq!(local[1].status, 404, "{:?}", local[1]);
        assert!(!local[2].is_success(), "{:?}", local[2]);
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Absent);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_socket_is_detected_then_replaced_by_the_next_daemon() {
        let (_tmp, dir) = new_node();
        dir.ensure_run_dir().unwrap();
        // A socket file nothing listens on: bind, then close without unlinking.
        drop(std::os::unix::net::UnixListener::bind(dir.socket_path()).unwrap());
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Stale);

        // A command falls back to its own node and says why.
        let client = NodeClient::connect(&dir).await.unwrap();
        assert_eq!(client.mode(), Mode::InProcess);
        assert_eq!(client.socket_state(), SocketState::Stale);
        client.stop().await.unwrap();

        // A daemon takes the path over.
        let daemon = TestDaemon::start(&dir).await;
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Live);
        daemon.stop().await;
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Absent);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn something_that_is_not_a_socket_at_the_socket_path_is_an_error() {
        let (_tmp, dir) = new_node();
        dir.ensure_run_dir().unwrap();
        std::fs::write(dir.socket_path(), b"not a socket").unwrap();
        let error = dir.probe_socket().await.unwrap_err();
        assert!(
            format!("{error:#}").contains("is not a socket"),
            "{error:#}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_process_at_a_time_boots_a_node() {
        let (_tmp, dir) = new_node();
        let daemon = TestDaemon::start(&dir).await;
        let held = dir.try_lock().unwrap().unwrap_err();
        assert_eq!(held.pid, Some(std::process::id()));
        // A second daemon is refused before it touches the node's storage.
        let error = crate::cmds::daemon::serve(&dir, |_| Ok(()), async {})
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("already has the node"),
            "{error:#}"
        );
        daemon.stop().await;

        // A booted node that is not serving its socket (a daemon still starting) is not
        // something a command may open a second copy of.
        let _booting = dir.try_lock().unwrap().unwrap();
        let Err(error) = NodeClient::connect(&dir).await else {
            panic!("a locked node must not be booted again");
        };
        assert!(
            format!("{error:#}").contains("not serving its socket"),
            "{error:#}"
        );
    }

    #[test]
    fn init_refuses_an_existing_node_and_leaves_it_alone() {
        let (_tmp, dir) = new_node();
        let before = dir.settings().unwrap().node_id();
        assert!(dir.init().is_err());
        assert_eq!(dir.settings().unwrap().node_id(), before);
    }

    #[test]
    fn a_socket_path_too_long_for_sun_path_is_refused_up_front() {
        let long = std::path::PathBuf::from("/tmp").join("x".repeat(120));
        assert!(NodeDir::new(long).is_err());
    }
}

mod blackbox {
    use std::path::Path;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::OnceLock;
    use trycmd::TestCases;

    /// The built `day` binary. Cargo does not build a package's binary for its own unit tests
    /// (only for integration tests, which this repo avoids), so it must be built first with
    /// `cargo build -p day_cli`. A binary older than the CLI's own sources is refused: running
    /// the cases against it would test yesterday's code.
    fn day_bin() -> &'static Path {
        static BIN: OnceLock<PathBuf> = OnceLock::new();
        BIN.get_or_init(|| {
            let target_dir = std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
            let bin = target_dir.join("debug/day");
            let built = std::fs::metadata(&bin)
                .unwrap_or_else(|_| {
                    panic!(
                        "e2e tests need the built binary at {}; run `cargo build -p day_cli` first",
                        bin.display()
                    )
                })
                .modified()
                .unwrap();
            let mut sources = vec![PathBuf::from(env!("CARGO_MANIFEST_DIR"))];
            while let Some(path) = sources.pop() {
                for entry in std::fs::read_dir(&path).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        sources.push(entry.path());
                    } else if entry.path().extension().is_some_and(|ext| ext == "rs") {
                        let modified = entry.metadata().unwrap().modified().unwrap();
                        assert!(
                            modified <= built,
                            "{} is newer than {}; run `cargo build -p day_cli` first",
                            entry.path().display(),
                            bin.display()
                        );
                    }
                }
            }
            bin
        })
    }

    fn new_suite() -> TestCases {
        let suite = TestCases::new();
        // The CLI resolves the node from DAY_NODE (cwd is ignored): point every case at a `node`
        // subdir of its own sandbox so cases are isolated from each other and from the default
        // data directory.
        suite.env("DAY_NODE", "node");
        // The binary's absolute path for `sh -c` steps, which cannot resolve it through trycmd.
        suite.env("DAY", day_bin().to_string_lossy().into_owned());
        // Success is reported on stdout; tracing goes to stderr and is silenced suite-wide so
        // snapshots assert real output, while errors still surface.
        suite.env("RUST_LOG", "error");
        suite.env("NO_COLOR", "1");
        suite.register_bin("day", trycmd::schema::Bin::Path(day_bin().to_owned()));
        suite.register_bin("sh", trycmd::schema::Bin::Path("/bin/sh".into()));
        suite
    }

    /// `init`, `--help`, argument errors and completions: nothing that boots a node.
    #[test]
    fn node_cases() {
        new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/node/*.md"));
    }

    /// `status` and `call` with no daemon: the command boots the node itself.
    #[test]
    fn in_process_cases() {
        new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/in_process/*.md"));
    }

    /// `daemon start | status | stop` and the commands reaching the node over its socket.
    #[test]
    fn daemon_cases() {
        new_suite().case(concat!(env!("CARGO_MANIFEST_DIR"), "/e2e/daemon/*.md"));
    }

    /// A daemon killed without cleaning up leaves a socket and no lock: every command still
    /// works, and `daemon stop` and `daemon start` clean up after it.
    #[test]
    fn a_killed_daemon_leaves_a_stale_socket_that_commands_survive() {
        let sandbox = tempfile::tempdir().unwrap();
        let node = sandbox.path().join("node");
        let day = |args: &[&str]| {
            Command::new(day_bin())
                .env("RUST_LOG", "error")
                .env("NO_COLOR", "1")
                .arg("--node")
                .arg(&node)
                .args(args)
                .output()
                .unwrap()
        };
        let text = |output: &std::process::Output| {
            String::from_utf8_lossy(&output.stdout).into_owned()
                + &String::from_utf8_lossy(&output.stderr)
        };
        assert!(day(&["init"]).status.success());
        let started = day(&["daemon", "start"]);
        assert!(started.status.success(), "{}", text(&started));
        let pid: u32 = std::fs::read_to_string(node.join("node.lock"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // SAFETY: kill(2) takes plain integers.
        assert_eq!(
            unsafe { libc::kill(pid.try_into().unwrap(), libc::SIGKILL) },
            0
        );
        // The lock goes with the process; wait for that rather than for a timer.
        std::fs::File::open(node.join("node.lock"))
            .unwrap()
            .lock_shared()
            .unwrap();
        assert!(node.join("run/node.sock").exists());

        let status = day(&["daemon", "status"]);
        assert_eq!(status.status.code(), Some(3), "{}", text(&status));
        assert!(text(&status).contains("stale socket"), "{}", text(&status));
        let node_status = day(&["status"]);
        assert!(node_status.status.success(), "{}", text(&node_status));
        assert!(
            text(&node_status).contains("in-process"),
            "{}",
            text(&node_status)
        );
        assert!(
            text(&node_status).contains("(stale)"),
            "{}",
            text(&node_status)
        );

        let stopped = day(&["daemon", "stop"]);
        assert!(stopped.status.success(), "{}", text(&stopped));
        assert!(
            text(&stopped).contains("removed its stale socket"),
            "{}",
            text(&stopped)
        );
        assert!(!node.join("run/node.sock").exists());

        // And a daemon can start over a stale socket too.
        assert!(day(&["daemon", "start"]).status.success());
        let pid_again = std::fs::read_to_string(node.join("node.lock")).unwrap();
        assert_ne!(pid_again.trim().parse::<u32>().unwrap(), pid);
        let stopped = day(&["daemon", "stop"]);
        assert!(stopped.status.success(), "{}", text(&stopped));
    }
}
