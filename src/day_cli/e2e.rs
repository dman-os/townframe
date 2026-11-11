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
    use crate::context::NodeDir;
    use crate::context::SocketState;
    use crate::interlude::*;
    use big_node::peers::Peers;
    use big_nodule_core::protocol::ListNodules;
    use method_core::Method;
    use tokio::sync::oneshot;

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
        let client = NodeClient::connect(&dir).await.unwrap();
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
        let booted = Booted::boot(&dir, lock, false).await.unwrap();
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
        for (nsid, verb, input) in calls() {
            local.push(call(&in_process, nsid, verb, input).await);
        }
        in_process.stop().await.unwrap();

        let daemon = TestDaemon::start(&dir).await;
        let over_socket = NodeClient::connect(&dir).await.unwrap();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn a_stale_socket_is_detected_then_replaced_by_the_next_daemon() {
        let (_tmp, dir) = new_node();
        dir.ensure_run_dir().unwrap();
        // A socket file nothing listens on: bind, then close without unlinking.
        drop(std::os::unix::net::UnixListener::bind(dir.socket_path().unwrap()).unwrap());
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
        let dialed_client = NodeClient::connect(&dialed_dir).await.unwrap();
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

        let dialer_client = NodeClient::connect(&dialer_dir).await.unwrap();
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

    /// Where the handler component is built (see `intxt install --handler`): CI builds it before
    /// these tests, and `INTXT_GUEST_WASM` names it when it is elsewhere.
    fn guest_path() -> PathBuf {
        use std::path::Path;
        std::env::var_os("INTXT_GUEST_WASM")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm")
            })
    }

    /// An initialized node whose `node.json` lets nodules be granted what intxt's handler calls
    /// (the operator's to configure before the node boots), with the real handler component
    /// installed the way `day x intxt install --handler` does and loaded by the node's wasm
    /// executor.
    ///
    /// Needs the component, built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml
    /// -p intxt_nodule_guest --target wasm32-wasip3 --release`.
    pub(super) async fn intxt_node() -> (tempfile::TempDir, NodeClient) {
        use crate::cmds::intxt;
        use big_dispatch_core::Readiness;

        let (tmp, dir) = new_node();
        let path = dir.root().join("node.json");
        let mut settings: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        settings["nodule_operations"] = json!({
            "operations": intxt_core::methods::REQUIRED_OPERATIONS
        });
        std::fs::write(&path, serde_json::to_vec_pretty(&settings).unwrap()).unwrap();

        let wasm_path = guest_path();
        let wasm = std::fs::read(&wasm_path).unwrap_or_else(|error| {
            panic!(
                "{}: {error}; build it with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release` or set INTXT_GUEST_WASM",
                wasm_path.display()
            )
        });
        let client = NodeClient::connect(&dir).await.unwrap();
        let record = intxt::install(&client, &wasm).await.unwrap();
        assert_eq!(record.readiness, Readiness::Ready);
        assert!(record.operations.unwrap().granted);
        (tmp, client)
    }

    /// A document capability the operator mints for `scope`.
    pub(super) async fn doc_capability(
        client: &NodeClient,
        scope: big_cabinet_core::doc::DocScope,
    ) -> Res<big_cabinet_core::authority::CapId> {
        Ok(crate::cmds::intxt::call(
            client,
            big_cabinet_core::doc::MintDocCapability {
                scope,
                rights: big_cabinet_core::authority::Rights::USE,
                subject: None,
            },
        )
        .await?
        .capability_id)
    }

    /// The document's heads and the accepted value of `key` in it at those heads.
    pub(super) async fn read_at(
        client: &NodeClient,
        doc: &big_cabinet_core::DocId,
        key: &big_cabinet_core::DatumKey,
    ) -> Res<(Vec<big_cabinet_core::doc::ChangeHash>, serde_json::Value)> {
        use big_cabinet_core::doc::At;
        use big_cabinet_core::doc::DatumRead;
        use big_cabinet_core::doc::DocScope;
        use big_cabinet_core::doc::ReadDatums;

        let capability = doc_capability(
            client,
            DocScope {
                read: [doc.clone()].into(),
                ..DocScope::default()
            },
        )
        .await?;
        let mut read = crate::cmds::intxt::call(
            client,
            ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: Some([key.clone()].into()),
                optimistic: false,
            },
        )
        .await?;
        match read.datums.remove(key) {
            Some(DatumRead::Accepted { value, .. }) => Ok((read.heads, value)),
            other => eyre::bail!("{key} in {doc} is not readable: {other:?}"),
        }
    }

    /// A command id no earlier write used: the node deduplicates writes by command id.
    pub(super) fn command(kind: &str) -> big_cabinet_core::doc::CommandId {
        let nonce: [u8; 16] = rand::random();
        big_cabinet_core::doc::CommandId::try_from(format!(
            "{kind}-{}",
            data_encoding::HEXLOWER.encode(&nonce)
        ))
        .expect("a kind and a hex nonce make a command id")
    }

    /// `day x <nsid> <input>`: the operation through the router, whose answer must be a success.
    pub(super) async fn day_x(
        client: &NodeClient,
        nsid: &str,
        input: serde_json::Value,
    ) -> serde_json::Value {
        let reply = client
            .call(nsid, method_core::HttpMethod::Post, &input)
            .await
            .unwrap();
        assert!(reply.is_success(), "{nsid} answered {reply:?}");
        reply.body
    }

    /// `org.example.intxt.createIntxt`, through the real installed handler, creates a drawer whose
    /// descriptor document carries the intxt marker, the title and an empty body, readable as
    /// written.
    #[tokio::test(flavor = "multi_thread")]
    async fn create_intxt_creates_a_drawer_marked_as_an_intxt() {
        use big_cabinet_core::drawer::protocol::DrawerId;

        let (_tmp, client) = intxt_node().await;
        let created = day_x(
            &client,
            "org.example.intxt.createIntxt",
            json!({"title": "Datum references move to dtxt:"}),
        )
        .await;
        let drawer: DrawerId = created["drawerId"].as_str().unwrap().parse().unwrap();
        let accepted = |key: &str| {
            let client = &client;
            let drawer = &drawer;
            let key = key.parse().unwrap();
            async move { read_at(client, drawer, &key).await.unwrap().1 }
        };
        assert_eq!(
            accepted("org.example.intxt/main").await,
            json!({"empty": true})
        );
        assert_eq!(
            accepted("org.example.intxt.title/main").await,
            json!({"title": "Datum references move to dtxt:"})
        );
        assert_eq!(
            accepted("org.example.intxt.body/main").await,
            json!({"order": []})
        );
        client.stop().await.unwrap();
    }

    /// Runs `git` in `dir` with nothing taken from the machine.
    fn git(dir: &std::path::Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Ada")
            .env("GIT_AUTHOR_EMAIL", "ada@example.com")
            .env("GIT_COMMITTER_NAME", "Grace")
            .env("GIT_COMMITTER_EMAIL", "grace@example.com")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// A scripted Claude Code session through `capture`: the hooks' JSON in, the real handler's
    /// documents out. A tool call writes a text file and a binary one; the binary file's bytes go
    /// to the blob store and its file document carries the blob datum, which `file.blob`
    /// references (ledger D45).
    #[tokio::test(flavor = "multi_thread")]
    async fn capture_records_a_session_and_stores_a_binary_file_as_a_blob() {
        use crate::cmds::intxt::capture;
        use big_cabinet_core::DocId;
        use big_cabinet_core::datum::DatumMember;
        use big_cabinet_core::drawer::DrawerAccess;
        use big_cabinet_core::drawer::protocol::DrawerId;
        use big_cabinet_core::drawer::protocol::ListDocs;
        use big_cabinet_core::drawer::protocol::MintDrawerCapability;
        use intxt_core::work;
        use std::io::Write;

        let (tmp, client) = intxt_node().await;
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = base.join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join("a.txt"), "one\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "first"]);
        let transcript = base.join("claude/s1.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let mut file = std::fs::File::create(&transcript).unwrap();
        for record in [
            json!({"type": "user", "uuid": "u1", "parentUuid": null, "timestamp": "2026-11-10T10:00:00Z",
                   "cwd": repo, "sessionId": "s1", "message": {"role": "user", "content": "make files"}}),
            json!({"type": "assistant", "uuid": "a1", "parentUuid": "u1", "timestamp": "2026-11-10T10:00:01Z",
                   "cwd": repo, "sessionId": "s1",
                   "message": {"id": "m-t1", "content": [{"type": "tool_use", "id": "t1", "name": "Bash",
                               "input": {"command": "make files"}}]}}),
        ] {
            writeln!(file, "{record}").unwrap();
        }
        let hook = |event: &str, tool: bool| {
            let mut input = json!({
                "session_id": "s1",
                "transcript_path": transcript,
                "cwd": repo,
                "hook_event_name": event,
            });
            if tool {
                input["tool_name"] = json!("Bash");
                input["tool_use_id"] = json!("t1");
                input["tool_input"] = json!({"command": "make files"});
            }
            input.to_string().into_bytes()
        };

        let started = capture::run(&client, &node_root, &hook("SessionStart", false))
            .await
            .unwrap();
        assert!(started.recorded.is_some());
        capture::run(&client, &node_root, &hook("PreToolUse", true))
            .await
            .unwrap();
        std::fs::write(repo.join("a.txt"), "one\ntwo\n").unwrap();
        let binary = [0u8, 159, 146, 150, 1, 2];
        std::fs::write(repo.join("pic.png"), binary).unwrap();
        let edited = capture::run(&client, &node_root, &hook("PostToolUse", true))
            .await
            .unwrap();
        assert_eq!(edited.blobs_put, 1);
        assert_eq!(edited.uncaused_tool_calls, 0);
        assert!(edited.stale_blobs.is_empty() && edited.oversize_blobs.is_empty());
        let recorded = edited.recorded.unwrap();
        let root = repo.display().to_string();
        let docs = &recorded.worktrees[&root].docs;

        // The worktree's first op is the edit this call made, caused by the call.
        let worktree: work::Worktree = serde_json::from_value(
            read_at(
                &client,
                &docs.worktree.as_ref().unwrap().parse::<DocId>().unwrap(),
                &format!("{}/main", work::Worktree::ID).parse().unwrap(),
            )
            .await
            .unwrap()
            .1,
        )
        .unwrap();
        let log: DocId = worktree.op_logs[0].doc.parse().unwrap();
        let op = read_at(
            &client,
            &log,
            &format!("{}/0", work::WorktreeOp::ID).parse().unwrap(),
        )
        .await
        .unwrap()
        .1;
        assert_eq!(op["kind"], "edit");
        assert!(
            op["cause"].as_str().unwrap().contains("promptTurn/u1"),
            "{op}"
        );
        assert!(op["files"].as_object().unwrap().contains_key("pic.png"));
        assert!(op["files"].as_object().unwrap().contains_key("a.txt"));

        // The binary file's document holds the blob datum, and its `File` points at it.
        let picture: DocId = worktree.files["pic.png"].parse().unwrap();
        let blob = read_at(
            &client,
            &picture,
            &"org.example.daybook.blob/main".parse().unwrap(),
        )
        .await
        .unwrap()
        .1;
        assert_eq!(blob["mime"], "image/png");
        assert_eq!(blob["lengthOctets"], binary.len());
        assert!(blob["digest"].as_str().unwrap().starts_with('z'));
        let file = read_at(
            &client,
            &picture,
            &format!("{}/main", work::File::ID).parse().unwrap(),
        )
        .await
        .unwrap()
        .1;
        assert_eq!(
            file["blob"],
            "dtxt:datum:self/org.example.daybook.blob/main"
        );

        // The text file is a note.
        let text: DocId = worktree.files["a.txt"].parse().unwrap();
        let note = read_at(
            &client,
            &text,
            &format!("{}/main", intxt_core::generic::Note::ID)
                .parse()
                .unwrap(),
        )
        .await
        .unwrap()
        .1;
        assert_eq!(note["content"], "one\ntwo\n");

        // A hook with nothing new records nothing; the machine and the capture drawer are the
        // node's, remembered for the next hook.
        let stopped = capture::run(&client, &node_root, &hook("Stop", false))
            .await
            .unwrap();
        assert!(stopped.recorded.is_none(), "{stopped:?}");
        let stored: serde_json::Value =
            serde_json::from_slice(&std::fs::read(node_root.join("intxt/capture.json")).unwrap())
                .unwrap();
        assert!(
            stored["machine"]
                .as_str()
                .unwrap()
                .starts_with("dtxt:actor:")
        );
        let drawer: DrawerId = stored["drawer"].as_str().unwrap().parse().unwrap();
        let listing = crate::cmds::intxt::call(
            &client,
            MintDrawerCapability {
                scope: big_cabinet_core::drawer::protocol::DrawerScope {
                    drawers: [(drawer.clone(), DrawerAccess::Read)].into(),
                    create: false,
                },
                rights: big_cabinet_core::authority::Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
        let listed = crate::cmds::intxt::call(
            &client,
            ListDocs {
                capability: listing,
                drawer_id: drawer,
                offset: 0,
                limit: None,
            },
        )
        .await
        .unwrap();
        for doc in [
            docs.worktree.as_ref().unwrap(),
            recorded.session.as_ref().unwrap(),
        ] {
            let doc: DocId = doc.parse().unwrap();
            assert!(
                listed.docs.iter().any(|listed| listed.doc_id == doc),
                "{doc} is not in the capture drawer: {listed:?}"
            );
        }
        client.stop().await.unwrap();
    }

    /// `capture` is for the daemon: with none serving, its entry point refuses, naming the command
    /// that starts one, without booting the node.
    #[tokio::test(flavor = "multi_thread")]
    async fn capture_refuses_without_a_daemon() {
        let (_tmp, dir) = new_node();
        let error = crate::cmds::intxt::require_daemon(&dir).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("day daemon start"),
            "{error:#}"
        );
    }

    /// On a node whose ceiling does not cover the handler's operations, `install` fails and says
    /// which `day nodules ceiling add` command to run, built from the operations it needs.
    #[tokio::test(flavor = "multi_thread")]
    async fn install_names_the_ceiling_command_when_the_grant_exceeds_it() {
        let (_tmp, dir) = new_node();
        let client = NodeClient::connect(&dir).await.unwrap();
        let error = crate::cmds::intxt::install(&client, b"not a component")
            .await
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains(
                "day nodules ceiling add --authority org.example.daybook.docs --authority org.example.daybook.drawers"
            ),
            "{message}"
        );
        assert!(message.contains("restart the daemon"), "{message}");
        client.stop().await.unwrap();
    }

    /// `org.example.intxt.addToIntxt` includes captured parts: each gets an entry keyed by the
    /// part's identity, the body lists the entries in the order they were added, adding a part
    /// again changes nothing, and the parts' documents are entered in the intxt's drawer so its
    /// members can read them.
    #[tokio::test(flavor = "multi_thread")]
    async fn add_to_intxt_includes_parts_once_and_shares_their_documents() {
        use crate::cmds::intxt;
        use big_cabinet_core::DocId;
        use big_cabinet_core::doc::CreateDoc;
        use big_cabinet_core::doc::DocScope;
        use big_cabinet_core::drawer::DrawerAccess;
        use big_cabinet_core::drawer::protocol::DrawerId;
        use big_cabinet_core::drawer::protocol::ListDocs;
        use big_cabinet_core::drawer::protocol::MintDrawerCapability;
        use big_cabinet_core::reference::DatumReference;

        let (_tmp, client) = intxt_node().await;
        let create = |title: &'static str| {
            let client = &client;
            async move {
                let created = day_x(
                    client,
                    "org.example.intxt.createIntxt",
                    json!({ "title": title }),
                )
                .await;
                created["drawerId"]
                    .as_str()
                    .unwrap()
                    .parse::<DrawerId>()
                    .unwrap()
            }
        };
        let captures = create("captures").await;
        let review = create("review").await;
        let capture = |tag: &str, value: serde_json::Value| {
            let client = &client;
            let captures = &captures;
            let tag = tag.to_owned();
            async move {
                let placement = format!("drawer {captures}");
                let capability = doc_capability(
                    client,
                    DocScope {
                        create: [placement.clone()].into(),
                        ..DocScope::default()
                    },
                )
                .await
                .unwrap();
                let doc: DocId = intxt::call(
                    client,
                    CreateDoc {
                        capability,
                        command: command("capture"),
                        placement,
                        datums: [(format!("{tag}/main").parse().unwrap(), value)].into(),
                    },
                )
                .await
                .unwrap()
                .doc_id;
                format!("dtxt:datum:{doc}/{tag}/main")
                    .parse::<DatumReference>()
                    .unwrap()
            }
        };
        let worktree = capture(
            "org.example.intxt.work.worktree",
            json!({
                "clone": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitClone/0193c",
                "id": "0193a",
                "path": "/work/townframe",
                "files": {},
                "opLogs": []
            }),
        )
        .await;
        let session = capture(
            "org.example.intxt.work.agentSession",
            json!({"harness": "claude-code", "sessionId": "eb26ecaa", "cwd": "/work/townframe"}),
        )
        .await;

        let add = |part: &DatumReference| {
            let client = &client;
            let review = &review;
            let part = part.to_string();
            async move {
                let added = day_x(
                    client,
                    "org.example.intxt.addToIntxt",
                    json!({"intxt": review, "part": part}),
                )
                .await;
                added["entry"].as_str().unwrap().to_owned()
            }
        };
        let added = [
            add(&worktree).await,
            add(&session).await,
            add(&worktree).await,
        ];
        assert_eq!(
            added,
            [
                format!("dtxt:datum:{review}/org.example.intxt.work.worktreeEntry/0193a"),
                format!("dtxt:datum:{review}/org.example.intxt.work.sessionEntry/eb26ecaa"),
                format!("dtxt:datum:{review}/org.example.intxt.work.worktreeEntry/0193a"),
            ]
        );
        let (_, entry) = read_at(
            &client,
            &review,
            &"org.example.intxt.work.worktreeEntry/0193a"
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(entry, json!({"worktree": worktree.to_string()}));
        let (_, body) = read_at(
            &client,
            &review,
            &"org.example.intxt.body/main".parse().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            body,
            json!({"order": [
                "dtxt:datum:self/org.example.intxt.work.worktreeEntry/0193a",
                "dtxt:datum:self/org.example.intxt.work.sessionEntry/eb26ecaa",
            ]})
        );

        let listing = intxt::call(
            &client,
            MintDrawerCapability {
                scope: big_cabinet_core::drawer::protocol::DrawerScope {
                    drawers: [(review.clone(), DrawerAccess::Read)].into(),
                    create: false,
                },
                rights: big_cabinet_core::authority::Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
        let listed = intxt::call(
            &client,
            ListDocs {
                capability: listing,
                drawer_id: review.clone(),
                offset: 0,
                limit: None,
            },
        )
        .await
        .unwrap();
        for part in [&worktree, &session] {
            let big_cabinet_core::reference::ReferenceDocId::Concrete(doc) = part.document() else {
                unreachable!()
            };
            assert!(
                listed.docs.iter().any(|listed| &listed.doc_id == doc),
                "{doc} is not in the intxt's drawer: {listed:?}"
            );
        }
        client.stop().await.unwrap();
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

    /// `init`, `--help`, argument errors and completions: nothing that boots a node.
    #[test]
    fn node_cases() {
        run_cases("node");
    }

    /// `status` and `exec` with no daemon: the command boots the node itself.
    #[test]
    fn in_process_cases() {
        run_cases("in_process");
    }

    /// `daemon start | status | stop` and the commands reaching the node over its socket.
    #[test]
    fn daemon_cases() {
        run_cases("daemon");
    }

    /// A daemon killed without cleaning up leaves a socket and no lock: every command still
    /// works, and `daemon stop` and `daemon start` clean up after it.
    #[test]
    fn a_killed_daemon_leaves_a_stale_socket_that_commands_survive() {
        let sandbox = Sandbox::new();
        let node = sandbox.node();
        let day = |args: &[&str]| sandbox.day(args);
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
        let socket = sandbox.socket();
        assert!(socket.exists());

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
        assert!(!socket.exists());

        // And a daemon can start over a stale socket too.
        assert!(day(&["daemon", "start"]).status.success());
        let pid_again = std::fs::read_to_string(node.join("node.lock")).unwrap();
        assert_ne!(pid_again.trim().parse::<u32>().unwrap(), pid);
        let stopped = day(&["daemon", "stop"]);
        assert!(stopped.status.success(), "{}", text(&stopped));
    }
}
