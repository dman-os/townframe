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
//!   platform keyring, since the daemon and the commands are separate processes: each case runs
//!   with its node and its runtime directory in a sandbox of the test's own, and the node's
//!   keyring entries are deleted when the case ends, whether it passed or not.
//!
//! The blackbox layer needs `cargo build -p day_cli` first, as cargo does not build a package's
//! binary for its own unit tests; it refuses a binary older than the CLI's sources.

#![cfg(test)]

mod library {
    use crate::client::Mode;
    use crate::client::NodeClient;
    use crate::client::Reply;
    use crate::context::Booted;
    use crate::context::LockRole;
    use crate::context::NodeDir;
    use crate::context::SocketState;
    use crate::interlude::*;
    use big_node::peers::Peers;
    use big_nodule_core::protocol::ListNodules;
    use method_core::Method;
    use tokio::sync::oneshot;

    fn connect(dir: &NodeDir) -> impl Future<Output = Res<NodeClient>> {
        NodeClient::connect(dir, crate::client::CONNECT_DEADLINE)
    }

    /// A directory under `/tmp` rather than `$TMPDIR`: a socket named by a 64 character node id
    /// has ~35 bytes of directory to spare in `sun_path` on macOS, and `$TMPDIR` can be longer.
    pub(super) fn short_tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("d")
            .tempdir_in("/tmp")
            .unwrap()
    }

    /// An initialized node whose socket directory is `<sandbox>/run`, so no test touches the
    /// real runtime directory.
    fn new_node() -> (tempfile::TempDir, NodeDir) {
        let tmp = short_tempdir();
        let dir = NodeDir::with_socket_dir(tmp.path().join("node"), tmp.path().join("run"));
        dir.init().unwrap();
        (tmp, dir)
    }

    /// Waits until `peers` has a registered connection to the node `node_id`. It subscribes
    /// before it looks, so a connection that registers in between is an event it then sees.
    async fn connected_to(peers: &Peers, node_id: &str) {
        let mut events = peers.subscribe();
        loop {
            if peers
                .connected()
                .await
                .unwrap()
                .iter()
                .any(|peer| data_encoding::HEXLOWER.encode(peer.as_bytes()) == node_id)
            {
                return;
            }
            match events.recv().await {
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("the peers service stopped")
                }
            }
        }
    }

    /// A daemon served from inside the test process, stopped by [`TestDaemon::stop`].
    struct TestDaemon {
        shutdown: oneshot::Sender<()>,
        task: tokio::task::JoinHandle<Res<()>>,
        /// The daemon's peers service, when its `node.json` configures one.
        peers: Option<Peers>,
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
                    |booted| {
                        ready_tx
                            .send(booted.node.peers.clone())
                            .map_err(|_| eyre::eyre!("test is gone"))
                    },
                    async {
                        shutdown_rx
                            .await
                            .expect("the test stops the daemon by sending")
                    },
                )
                .await
            });
            let peers = match ready_rx.await {
                Ok(peers) => peers,
                Err(_) => panic!("the daemon failed before it was serving: {:?}", task.await),
            };
            Self {
                shutdown,
                task,
                peers,
            }
        }

        async fn stop(self) {
            self.shutdown.send(()).expect("the daemon is serving");
            self.task.await.unwrap().unwrap();
        }
    }

    async fn call(
        client: &NodeClient,
        nsid: &str,
        verb: method_core::HttpMethod,
        input: serde_json::Value,
    ) -> Reply {
        client.call(nsid, verb, &input).await.unwrap()
    }

    /// Calls whose answers must not depend on how the node was reached: a query that succeeds, an
    /// operation nothing claims, and a real operation given input it must refuse.
    fn calls() -> Vec<(&'static str, method_core::HttpMethod, serde_json::Value)> {
        use method_core::HttpMethod::Post;
        vec![
            (ListNodules::XRPC.nsid, ListNodules::XRPC.method, json!({})),
            ("org.example.nothing.here", Post, json!({})),
            (
                big_cabinet_core::doc::CreateDoc::XRPC.nsid,
                Post,
                json!({"nonsense": true}),
            ),
        ]
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn init_then_status_in_process() {
        let (_tmp, dir) = new_node();
        let client = connect(&dir).await.unwrap();
        assert_eq!(client.mode(), Mode::InProcess);
        assert_eq!(client.socket_state(), SocketState::Absent);
        let reply = call(
            &client,
            ListNodules::XRPC.nsid,
            ListNodules::XRPC.method,
            json!({}),
        )
        .await;
        assert_eq!(reply.status, 200, "{reply:?}");
        assert_eq!(reply.body, json!({"nodules": []}));
        client.stop().await.unwrap();
        // Stopping released the node: it can be booted again by the next command.
        connect(&dir).await.unwrap().stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_node_id_is_the_repos_peer_id() {
        let (_tmp, dir) = new_node();
        let lock = dir.try_lock(LockRole::Command).unwrap().unwrap();
        let booted = Booted::boot(&dir, lock, LockRole::Command).await.unwrap();
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
        let in_process = connect(&dir).await.unwrap();
        assert_eq!(in_process.mode(), Mode::InProcess);
        let mut local = Vec::new();
        for (nsid, verb, input) in calls() {
            local.push(call(&in_process, nsid, verb, input).await);
        }
        in_process.stop().await.unwrap();

        let daemon = TestDaemon::start(&dir).await;
        let over_socket = connect(&dir).await.unwrap();
        assert_eq!(over_socket.mode(), Mode::Daemon);
        assert_eq!(over_socket.socket_state(), SocketState::Live);
        let mut remote = Vec::new();
        for (nsid, verb, input) in calls() {
            remote.push(call(&over_socket, nsid, verb, input).await);
        }
        over_socket.stop().await.unwrap();
        daemon.stop().await;

        assert_eq!(local, remote);
        assert_eq!(local[0].status, 200);
        assert_eq!(local[1].status, 404, "{:?}", local[1]);
        assert!(!local[2].is_success(), "{:?}", local[2]);
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Absent);
    }

    /// A file put and read back, whole and by `Range`, is the same bytes whichever way the node
    /// is reached: streamed from and to files (ADR 042), never as JSON, so the in-process router
    /// and the daemon's socket carry the raw bodies alike.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_round_trips_as_raw_bytes_in_process_and_over_the_daemon() {
        use crate::client::Raw;
        use method_core::HttpMethod::Get;
        use method_core::HttpMethod::Post;
        const MINT: &str = "org.example.daybook.blobs.mintCapability";
        const PUT: &str = "org.example.daybook.blobs.put";
        const READ: &str = "org.example.daybook.blobs.read";
        let (tmp, dir) = new_node();
        let upload = tmp.path().join("upload.bin");
        // Over the 64 KiB chunks a read yields, so it is several chunks long.
        let content: Vec<u8> = (0..300_000u32).map(|at| (at % 251) as u8).collect();
        std::fs::write(&upload, &content).unwrap();
        let rights = serde_json::to_value(big_cabinet_core::authority::Rights::USE).unwrap();
        let round_trip = async |client: &NodeClient, name: &str| {
            let mint = async |scope: serde_json::Value| {
                let reply = client
                    .call(MINT, Post, &json!({ "scope": scope, "rights": rights }))
                    .await
                    .unwrap();
                assert_eq!(reply.status, 200, "{reply:?}");
                reply.body["capabilityId"].as_str().unwrap().to_owned()
            };
            let put = mint(json!({ "put": true })).await;
            let stored = client
                .call_raw(
                    PUT,
                    Post,
                    &json!({ "capability": put }),
                    Raw {
                        upload: Some(&upload),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(stored.status, 200, "{stored:?}");
            assert_eq!(stored.body["lengthOctets"], content.len());
            let blob_id = stored.body["blobId"].as_str().unwrap().to_owned();
            let read = mint(json!({ "read": [blob_id] })).await;
            let params = json!({ "capability": read, "blobId": blob_id });
            let whole = tmp.path().join(format!("{name}-whole.bin"));
            let reply = client
                .call_raw(
                    READ,
                    Get,
                    &params,
                    Raw {
                        download: Some(&whole),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(reply.status, 200, "{reply:?}");
            assert_eq!(reply.body["octets"], content.len());
            assert!(std::fs::read(&whole).unwrap() == content);
            let part = tmp.path().join(format!("{name}-part.bin"));
            let reply = client
                .call_raw(
                    READ,
                    Get,
                    &params,
                    Raw {
                        range: Some("bytes=70000-70099"),
                        download: Some(&part),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(reply.status, 206, "{reply:?}");
            assert_eq!(
                reply.body["contentRange"],
                format!("bytes 70000-70099/{}", content.len())
            );
            assert_eq!(std::fs::read(&part).unwrap(), &content[70_000..70_100]);
            stored.body
        };
        let in_process = connect(&dir).await.unwrap();
        let local = round_trip(&in_process, "in-process").await;
        in_process.stop().await.unwrap();

        let daemon = TestDaemon::start(&dir).await;
        let over_socket = connect(&dir).await.unwrap();
        assert_eq!(over_socket.mode(), Mode::Daemon);
        let remote = round_trip(&over_socket, "socket").await;
        over_socket.stop().await.unwrap();
        daemon.stop().await;
        // The node's answer is the content's own id, however the bytes arrived.
        assert_eq!(local, remote);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_socket_is_detected_then_replaced_by_the_next_daemon() {
        let (_tmp, dir) = new_node();
        dir.ensure_run_dir().unwrap();
        // A socket file nothing listens on: bind, then close without unlinking.
        drop(std::os::unix::net::UnixListener::bind(dir.socket_path().unwrap()).unwrap());
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Stale);

        // A command falls back to its own node and says why.
        let client = connect(&dir).await.unwrap();
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
        std::fs::write(dir.socket_path().unwrap(), b"not a socket").unwrap();
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
        let held = dir.try_lock(LockRole::Command).unwrap().unwrap_err();
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

        // A node booting elsewhere that never gets its socket up is waited for until the
        // deadline, not opened a second time.
        let booting = dir.try_lock(LockRole::Command).unwrap().unwrap();
        let Err(error) = NodeClient::connect(&dir, std::time::Duration::from_millis(200)).await
        else {
            panic!("a locked node must not be booted again");
        };
        assert!(
            format!("{error:#}").contains("stayed held by another process"),
            "{error:#}"
        );
        // Releasing the lock lets the thread that was waiting on it finish.
        drop(booting);
    }

    /// Two commands at once: one boots the node and serves its socket, the other becomes its
    /// client instead of fighting for the lock, and both are answered.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_concurrent_commands_boot_the_node_once() {
        let (_tmp, dir) = new_node();
        let (first, second) = tokio::join!(connect(&dir), connect(&dir));
        let (first, second) = (first.unwrap(), second.unwrap());
        // The holder found no socket; the client found the holder's.
        let mut states = [first.socket_state(), second.socket_state()];
        states.sort_by_key(|state| matches!(state, SocketState::Live));
        assert_eq!(states, [SocketState::Absent, SocketState::Live]);
        for client in [&first, &second] {
            let reply = call(
                client,
                ListNodules::XRPC.nsid,
                ListNodules::XRPC.method,
                json!({}),
            )
            .await;
            assert_eq!(reply.body, json!({"nodules": []}));
        }
        let (holder, client) = if first.socket_state() == SocketState::Live {
            (second, first)
        } else {
            (first, second)
        };

        // The holder's command is over, but its client is still connected: the node stays up
        // and keeps answering until the client goes.
        let holder_stopped = tokio::spawn(holder.stop());
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(!holder_stopped.is_finished());
        let reply = call(
            &client,
            ListNodules::XRPC.nsid,
            ListNodules::XRPC.method,
            json!({}),
        )
        .await;
        assert_eq!(reply.body, json!({"nodules": []}));
        client.stop().await.unwrap();
        holder_stopped.await.unwrap().unwrap();
        assert_eq!(dir.probe_socket().await.unwrap(), SocketState::Absent);
        // And the node is free for the next command.
        connect(&dir).await.unwrap().stop().await.unwrap();
    }

    /// `node run` against a node a command has open waits for the command, instead of refusing.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_daemon_waits_for_a_command_that_has_the_node_open() {
        let (_tmp, dir) = new_node();
        let holder = connect(&dir).await.unwrap();
        let (ready, serving) = oneshot::channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let daemon = tokio::spawn({
            let dir = dir.clone();
            async move {
                crate::cmds::daemon::serve(
                    &dir,
                    |_| {
                        ready.send(()).expect("the test waits for readiness");
                        Ok(())
                    },
                    async {
                        stopped.await.expect("the test stops the daemon by sending");
                    },
                )
                .await
            }
        });
        holder.stop().await.unwrap();
        serving
            .await
            .expect("the daemon booted once the command finished");
        stop.send(()).unwrap();
        daemon.await.unwrap().unwrap();
    }

    /// A command that arrives while the holder is still booting waits for the socket, and is
    /// answered once it is up.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_client_arriving_during_boot_waits_for_the_socket() {
        let (_tmp, dir) = new_node();
        let booting = dir.try_lock(LockRole::Command).unwrap().unwrap();
        let arriving = tokio::spawn({
            let dir = dir.clone();
            async move { connect(&dir).await }
        });
        // The arrival blocks on the boot lock while the holder boots.
        let holder = Booted::boot(&dir, booting, LockRole::Command)
            .await
            .unwrap();
        let client = arriving.await.unwrap().unwrap();
        assert_eq!(client.socket_state(), SocketState::Live);
        let reply = call(
            &client,
            ListNodules::XRPC.nsid,
            ListNodules::XRPC.method,
            json!({}),
        )
        .await;
        assert_eq!(reply.body, json!({"nodules": []}));
        client.stop().await.unwrap();
        holder.stop().await.unwrap();
    }

    #[test]
    fn init_refuses_an_existing_node_and_leaves_it_alone() {
        let (_tmp, dir) = new_node();
        let before = dir.settings().unwrap().node_id();
        assert!(dir.init().is_err());
        assert_eq!(dir.settings().unwrap().node_id(), before);
    }

    #[test]
    fn a_socket_path_too_long_for_sun_path_is_refused() {
        let (tmp, _) = new_node();
        let long = NodeDir::with_socket_dir(
            tmp.path().join("node"),
            std::path::PathBuf::from("/tmp").join("x".repeat(120)),
        );
        let error = long.socket_path().unwrap_err();
        assert!(format!("{error:#}").contains("longer than a Unix socket path"));
    }

    #[test]
    fn the_socket_is_named_by_the_node_id_in_the_runtime_directory() {
        let (tmp, dir) = new_node();
        assert_eq!(
            dir.socket_path().unwrap(),
            tmp.path()
                .join("run")
                .join(format!("{}.sock", dir.settings().unwrap().node_id()))
        );
        // The node directory is not where the socket goes, so a long node path is fine.
        let far =
            NodeDir::with_socket_dir(tmp.path().join("x".repeat(200)), tmp.path().join("run"));
        far.init().unwrap();
        far.socket_path().unwrap();
    }

    #[test]
    fn the_default_socket_directory_is_the_runtime_directory_or_a_private_one_in_tmp() {
        use crate::context::socket_dir_in;
        assert_eq!(
            socket_dir_in(Some(std::path::Path::new("/run/user/1000")), 1000),
            std::path::Path::new("/run/user/1000/day")
        );
        assert_eq!(
            socket_dir_in(None, 1000),
            std::path::Path::new("/tmp/day-1000")
        );
    }

    /// A directory that is not the user's own is refused as a socket directory, whoever made
    /// it: here, one other users can enter.
    #[test]
    fn a_socket_directory_other_users_can_enter_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let (tmp, dir) = new_node();
        let run = tmp.path().join("run");
        std::fs::create_dir(&run).unwrap();
        std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = dir.ensure_run_dir().unwrap_err();
        assert!(
            format!("{error:#}").contains("accessible to other users"),
            "{error:#}"
        );
    }

    /// Two nodes on one machine (one keyring, one runtime directory) get entries and sockets of
    /// their own: each daemon is live at its own path, and deleting one node's keyring entries
    /// removes exactly one entry and leaves the other node's.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_nodes_on_one_machine_have_distinct_sockets_and_keyring_entries() {
        let tmp = short_tempdir();
        let node = |name: &str| {
            let dir = NodeDir::with_socket_dir(tmp.path().join(name), tmp.path().join("run"));
            dir.init().unwrap();
            dir
        };
        let (first, second) = (node("first"), node("second"));
        assert_ne!(first.socket_path().unwrap(), second.socket_path().unwrap());
        assert_eq!(
            first.socket_path().unwrap().parent(),
            second.socket_path().unwrap().parent()
        );

        let (first_daemon, second_daemon) = (
            TestDaemon::start(&first).await,
            TestDaemon::start(&second).await,
        );
        assert_eq!(first.probe_socket().await.unwrap(), SocketState::Live);
        assert_eq!(second.probe_socket().await.unwrap(), SocketState::Live);
        first_daemon.stop().await;
        second_daemon.stop().await;

        // The mock keyring is process-wide, as the platform keyring is machine-wide.
        let secrets = secrets_rs::SecretStore::boot().await.unwrap();
        for dir in [&first, &second] {
            let seed = dir.settings().unwrap().seed();
            assert_eq!(big_node::delete_secrets(&secrets, &seed).await.unwrap(), 1);
            assert_eq!(big_node::delete_secrets(&secrets, &seed).await.unwrap(), 0);
        }
        secrets.stop().await.unwrap();
    }

    /// A daemon whose `node.json` lists a bootstrap peer dials it at start, with no command
    /// issued. Everything is learned through the node's own operations over the sockets: the
    /// dialed node's address from its `getNodeInfo`, and both ends then report each other as
    /// connected. Waiting is on the peers service's events.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_daemon_dials_its_bootstrap_peer_and_both_report_the_connection() {
        let loopback = |dir: &NodeDir| {
            dir.update_settings(|settings| {
                settings.peers = Some(crate::context::PeerSettings {
                    loopback_only: true,
                    ..default()
                });
                Ok(())
            })
            .unwrap();
        };
        let (_dialed_tmp, dialed_dir) = new_node();
        loopback(&dialed_dir);
        let dialed = TestDaemon::start(&dialed_dir).await;
        let dialed_client = connect(&dialed_dir).await.unwrap();
        assert_eq!(dialed_client.mode(), Mode::Daemon);
        let dialed_info = crate::cmds::status::node_info(&dialed_client)
            .await
            .unwrap();
        assert_eq!(
            dialed_info.node_id,
            dialed_dir.settings().unwrap().node_id()
        );
        assert!(dialed_info.peers.as_ref().unwrap().connected.is_empty());

        let (_dialer_tmp, dialer_dir) = new_node();
        loopback(&dialer_dir);
        let address = dialed_info.peers.unwrap().address;
        assert!(
            dialer_dir
                .update_settings(|settings| settings
                    .peers
                    .as_mut()
                    .unwrap()
                    .add_bootstrap(&address))
                .unwrap()
        );
        // The same endpoint again replaces its entry instead of adding a second.
        assert!(
            !dialer_dir
                .update_settings(|settings| settings
                    .peers
                    .as_mut()
                    .unwrap()
                    .add_bootstrap(&address))
                .unwrap()
        );
        assert_eq!(
            dialer_dir.settings().unwrap().peers.unwrap().bootstrap,
            [address]
        );
        let dialer = TestDaemon::start(&dialer_dir).await;
        let dialer_id = dialer_dir.settings().unwrap().node_id();
        connected_to(dialer.peers.as_ref().unwrap(), &dialed_info.node_id).await;
        connected_to(dialed.peers.as_ref().unwrap(), &dialer_id).await;

        let dialer_client = connect(&dialer_dir).await.unwrap();
        let from_dialer = crate::cmds::status::node_info(&dialer_client)
            .await
            .unwrap();
        assert_eq!(from_dialer.peers.unwrap().connected, [dialed_info.node_id]);
        let from_dialed = crate::cmds::status::node_info(&dialed_client)
            .await
            .unwrap();
        assert_eq!(from_dialed.peers.unwrap().connected, [dialer_id]);

        dialer_client.stop().await.unwrap();
        dialed_client.stop().await.unwrap();
        dialer.stop().await;
        dialed.stop().await;
        let secrets = secrets_rs::SecretStore::boot().await.unwrap();
        for dir in [&dialer_dir, &dialed_dir] {
            big_node::delete_secrets(&secrets, &dir.settings().unwrap().seed())
                .await
                .unwrap();
        }
        secrets.stop().await.unwrap();
    }

    #[test]
    fn an_invalid_bootstrap_ticket_is_refused_and_writes_nothing() {
        let (_tmp, dir) = new_node();
        let error = dir
            .update_settings(|settings| {
                settings
                    .peers
                    .get_or_insert_with(default)
                    .add_bootstrap("not a ticket")
            })
            .unwrap_err();
        assert!(format!("{error:#}").contains("not an endpoint ticket"));
        assert!(
            dir.settings().unwrap().peers.is_none(),
            "nothing was written"
        );
    }
}

mod blackbox {
    use crate::interlude::json;
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

    /// One case's private world: its node (`<sandbox>/node`) and its runtime directory
    /// (`<sandbox>/run`, where the daemon puts its socket, as `XDG_RUNTIME_DIR` would). The node
    /// is booted by the real binary, so it leaves entries in the platform keyring; they are
    /// deleted when the sandbox is dropped, whether the case passed or panicked.
    struct Sandbox {
        tmp: tempfile::TempDir,
    }

    impl Sandbox {
        fn new() -> Self {
            Self {
                tmp: super::library::short_tempdir(),
            }
        }

        fn node(&self) -> PathBuf {
            self.tmp.path().join("node")
        }

        fn runtime_dir(&self) -> PathBuf {
            self.tmp.path().join("run")
        }

        /// The node's socket, from its id: `<runtime dir>/day/<node id>.sock`.
        fn socket(&self) -> PathBuf {
            let id = crate::context::NodeDir::new(self.node())
                .settings()
                .unwrap()
                .node_id();
            self.runtime_dir().join("day").join(format!("{id}.sock"))
        }

        fn suite(&self) -> TestCases {
            let suite = TestCases::new();
            // The CLI resolves the node from DAY_NODE (cwd is ignored): every case gets a node
            // of its own, so cases are isolated from each other and from the default data
            // directory. Its socket goes to this sandbox's runtime directory, not the real one.
            suite.env("DAY_NODE", self.node().to_string_lossy().into_owned());
            suite.env(
                "XDG_RUNTIME_DIR",
                self.runtime_dir().to_string_lossy().into_owned(),
            );
            // The binary's absolute path for `sh -c` steps, which cannot resolve it through
            // trycmd.
            suite.env("DAY", day_bin().to_string_lossy().into_owned());
            // Success is reported on stdout; tracing goes to stderr and is silenced suite-wide so
            // snapshots assert real output, while errors still surface.
            suite.env("RUST_LOG", "error");
            suite.env("NO_COLOR", "1");
            suite.register_bin("day", trycmd::schema::Bin::Path(day_bin().to_owned()));
            suite.register_bin("sh", trycmd::schema::Bin::Path("/bin/sh".into()));
            suite
        }

        /// A `day` invocation against this sandbox's node and runtime directory.
        fn day(&self, args: &[&str]) -> std::process::Output {
            Command::new(day_bin())
                .env("RUST_LOG", "error")
                .env("NO_COLOR", "1")
                .env("XDG_RUNTIME_DIR", self.runtime_dir())
                .arg("--node")
                .arg(self.node())
                .args(args)
                .output()
                .unwrap()
        }
    }

    impl Drop for Sandbox {
        fn drop(&mut self) {
            // A node whose settings do not parse (a case that corrupts them on purpose) never
            // booted, so it owns no keyring entries.
            let Ok(settings) = crate::context::NodeDir::new(self.node()).settings() else {
                return;
            };
            // The test process's `SecretStore::boot` is the in-memory mock (the dev-dependency
            // turns `test-support` on); the binary used the platform keyring.
            let cleaned = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async {
                    let secrets = secrets_rs::SecretStore::boot_platform().await?;
                    big_node::delete_secrets(&secrets, &settings.seed()).await?;
                    secrets.stop().await
                });
            match cleaned {
                Ok(()) => {}
                // A second panic while unwinding would abort and bury the case's own failure.
                Err(error) if std::thread::panicking() => {
                    eprintln!("deleting the sandbox node's keyring entries failed: {error:#}");
                }
                Err(error) => {
                    panic!("deleting the sandbox node's keyring entries failed: {error:#}")
                }
            }
        }
    }

    /// Runs every case in `dir`, each in a sandbox of its own, one after another.
    fn run_cases(dir: &str) {
        let mut cases: Vec<_> =
            std::fs::read_dir(format!("{}/e2e/{dir}", env!("CARGO_MANIFEST_DIR")))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
                .collect();
        cases.sort();
        assert!(!cases.is_empty(), "no cases in e2e/{dir}");
        for case in cases {
            let sandbox = Sandbox::new();
            sandbox.suite().case(&case).run();
        }
    }

    /// `node init`, `--help`, argument errors and completions: nothing that boots a node.
    #[test]
    fn node_cases() {
        run_cases("node");
    }

    /// `node status` and `exec` with no daemon: the command boots the node itself.
    #[test]
    fn in_process_cases() {
        run_cases("in_process");
    }

    /// `node start | status | stop` and the commands reaching the node over its socket.
    #[test]
    fn daemon_cases() {
        run_cases("daemon");
    }

    /// Parallel commands against one node (as a capture hook firing several times at once does)
    /// do not fight over the node lock: one boots it and serves the rest over its socket.
    #[test]
    fn parallel_commands_all_succeed() {
        let sandbox = Sandbox::new();
        assert!(sandbox.day(&["node", "init"]).status.success());
        let runs: Vec<_> = (0..4)
            .map(|_| {
                let mut command = Command::new(day_bin());
                command
                    .env("RUST_LOG", "error")
                    .env("NO_COLOR", "1")
                    .env("XDG_RUNTIME_DIR", sandbox.runtime_dir())
                    .arg("--node")
                    .arg(sandbox.node())
                    .args(["exec", "--get", "org.example.daybook.nodules.listNodules"])
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .unwrap()
            })
            .collect();
        for run in runs {
            let output = run.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let answer: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(answer, json!({"nodules": []}));
        }
        assert!(
            !sandbox.socket().exists(),
            "the last holder removed its socket"
        );
    }

    /// A daemon killed without cleaning up leaves a socket and no lock: every command still
    /// works, and `node stop` and `node start` clean up after it.
    #[test]
    fn a_killed_daemon_leaves_a_stale_socket_that_commands_survive() {
        let sandbox = Sandbox::new();
        let node = sandbox.node();
        let day = |args: &[&str]| sandbox.day(args);
        let text = |output: &std::process::Output| {
            String::from_utf8_lossy(&output.stdout).into_owned()
                + &String::from_utf8_lossy(&output.stderr)
        };
        assert!(day(&["node", "init"]).status.success());
        let started = day(&["node", "start"]);
        assert!(started.status.success(), "{}", text(&started));
        // The lock records the holder's pid on its first line.
        let pid: u32 = std::fs::read_to_string(node.join("node.lock"))
            .unwrap()
            .lines()
            .next()
            .unwrap()
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
        let socket = sandbox.socket();
        assert!(socket.exists());

        let node_status = day(&["node", "status"]);
        assert_eq!(node_status.status.code(), Some(3), "{}", text(&node_status));
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

        // The command that found the stale socket held the node and replaced it with its own,
        // which it removed on exit.
        assert!(!socket.exists());
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());

        let stopped = day(&["node", "stop"]);
        assert!(stopped.status.success(), "{}", text(&stopped));
        assert!(
            text(&stopped).contains("removed its stale socket"),
            "{}",
            text(&stopped)
        );
        assert!(!socket.exists());

        // And a daemon can start over a stale socket too.
        assert!(day(&["node", "start"]).status.success());
        let lock_again = std::fs::read_to_string(node.join("node.lock")).unwrap();
        assert_ne!(
            lock_again.lines().next().unwrap().parse::<u32>().unwrap(),
            pid
        );
        let stopped = day(&["node", "stop"]);
        assert!(stopped.status.success(), "{}", text(&stopped));
    }
}
