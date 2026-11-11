//! Many nodes in one process (ADR 033 section 8): a node is a value, so twenty can be booted
//! from bindings, stopped, and some booted again from those bindings, leaving nothing behind.

use super::listeners::owner_only_directory;
use super::listeners::unix;
use super::*;
use std::path::PathBuf;

const NODES: usize = 20;
/// Nodes `0..SHARED_RUNTIME` share one `SqliteRuntime`; the rest each have their own.
const SHARED_RUNTIME: usize = 15;
/// Nodes `0..3` share one in-memory repo database, told apart by their scope keys.
const SHARED_MEMORY_REPO: std::ops::Range<usize> = 0..3;
/// Nodes with a Unix socket, each in a directory of its own with the same file name.
const LISTENING: [usize; 2] = [3, 4];
/// Nodes `5..8` live in a directory each.
const IN_DIRECTORY: std::ops::Range<usize> = 5..8;
/// Nodes `8..10` keep one on-disk repo database between them, told apart by their scope keys.
const SHARED_DISK_REPO: std::ops::Range<usize> = 8..10;
/// The nodes that are booted again from their bindings.
const DURABLE: std::ops::Range<usize> = 5..10;

struct Running {
    index: usize,
    node: BigNode,
    stop: StopToken,
    receipt: WriteReceipt,
    value: Value,
}

/// The pieces the twenty nodes are placed on.
struct Fleet {
    directory: tempfile::TempDir,
    /// One secret store per node: see [`Config::secrets`].
    secrets: Vec<Arc<SecretStore>>,
    runtimes: Vec<Arc<SqliteRuntime>>,
    /// Where each ephemeral database's temp directory was, which must be gone at the end.
    ephemeral_homes: std::sync::Mutex<Vec<PathBuf>>,
}

impl Fleet {
    fn runtime(&self, index: usize) -> &Arc<SqliteRuntime> {
        &self.runtimes[index.saturating_sub(SHARED_RUNTIME - 1)]
    }

    async fn ephemeral(&self, runtime: &SqliteRuntime) -> Database {
        let sql = runtime.ephemeral(&[]).unwrap();
        let (_, _, filename): (i64, String, String) = sql
            .query("PRAGMA database_list", &[])
            .await
            .unwrap()
            .one()
            .unwrap();
        let home = PathBuf::from(filename).parent().unwrap().to_owned();
        assert!(home.exists());
        self.ephemeral_homes.lock().unwrap().push(home);
        sql
    }

    /// The config of node `index`. `shared_repo` is the repo database the nodes of
    /// [`SHARED_MEMORY_REPO`] or [`SHARED_DISK_REPO`] hold together.
    async fn config(&self, index: usize, shared_repo: Option<&Database>) -> Config {
        let runtime = Arc::clone(self.runtime(index));
        let home = self.directory.path().join(format!("node-{index}"));
        let mut config = if IN_DIRECTORY.contains(&index) {
            let bindings = Bindings::in_directory(&runtime, &home, None).await.unwrap();
            let mut config = config(
                Arc::clone(&self.secrets[index]),
                bindings.sql,
                bindings.repo,
            );
            config.blob_storage = bindings.blobs;
            config
        } else if SHARED_DISK_REPO.contains(&index) {
            let storage = big_repo::Storage {
                sql: shared_repo.unwrap().clone(),
                keyhive: big_repo::KeyhiveStorage::Files(
                    big_repo::KeyhiveFiles::open(home.join("keyhive"), None).unwrap(),
                ),
            };
            let sql = runtime.database(
                self.directory.path().join(format!("node-{index}.sqlite")),
                &[],
                "_sqlite_rs_migrations",
            );
            config(Arc::clone(&self.secrets[index]), sql, storage)
        } else {
            let storage = if SHARED_MEMORY_REPO.contains(&index) {
                big_repo::Storage {
                    sql: shared_repo.unwrap().clone(),
                    keyhive: big_repo::KeyhiveStorage::Memory,
                }
            } else {
                big_repo::Storage {
                    sql: self.ephemeral(&runtime).await,
                    keyhive: big_repo::KeyhiveStorage::Memory,
                }
            };
            let sql = self.ephemeral(&runtime).await;
            config(Arc::clone(&self.secrets[index]), sql, storage)
        };
        config.repo.node_identity_seed = [u8::try_from(index).unwrap() + 1; 32];
        config.repo.scope_key = format!("node-{index}").into();
        if LISTENING.contains(&index) {
            let run = owner_only_directory(&self.directory.path().join(format!("run-{index}")));
            config.listeners = unix(&run.join("node.sock"));
        }
        config
    }
}

/// Runs each future on a task of its own and returns their outputs in order.
async fn join<T: Send + 'static>(
    futures: impl IntoIterator<Item = impl Future<Output = T> + Send + 'static>,
) -> Vec<T> {
    let tasks: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    let mut outputs = Vec::with_capacity(tasks.len());
    for task in tasks {
        outputs.push(task.await.unwrap());
    }
    outputs
}

async fn boot(config: Config, index: usize) -> Running {
    let (node, stop) = BigNode::boot(config).await.unwrap();
    let value = json!({"$type": NOTE, "value": format!("node-{index}")});
    let receipt = create(&node, &format!("note-{index}"), value.clone()).await;
    Running {
        index,
        node,
        stop,
        receipt,
        value,
    }
}

async fn assert_note(running: &Running) {
    let read = read(&running.node, &running.receipt.doc_id).await;
    assert!(
        matches!(
            &read.datums[&key()],
            DatumRead::Accepted { value, .. } if *value == running.value
        ),
        "node {} lost its note",
        running.index
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn long_test_twenty_nodes_boot_stop_and_reboot_in_one_process_without_leaking() {
    let baseline_tasks = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    let mut fleet = Fleet {
        directory: tempfile::tempdir().unwrap(),
        secrets: futures::future::join_all((0..NODES).map(|_| secrets())).await,
        runtimes: (0..=NODES - SHARED_RUNTIME)
            .map(|_| Arc::new(SqliteRuntime::new(Default::default())))
            .collect(),
        ephemeral_homes: Default::default(),
    };
    let memory_repo = fleet.ephemeral(&fleet.runtimes[0]).await;
    let disk_repo_path = fleet.directory.path().join("shared-repo.sqlite");
    let disk_repo = fleet.runtimes[0].database(&disk_repo_path, &[], "_sqlite_rs_migrations");

    // Booted together, each on a task of its own: the nodes share nothing but what they were
    // handed.
    let mut configs = Vec::new();
    for index in 0..NODES {
        let shared = if SHARED_MEMORY_REPO.contains(&index) {
            &memory_repo
        } else {
            &disk_repo
        };
        configs.push(fleet.config(index, Some(shared)).await);
    }
    let mut running = join(
        configs
            .into_iter()
            .enumerate()
            .map(|(index, config)| boot(config, index)),
    )
    .await;

    // Twenty distinct nodes: nothing about one was taken from another.
    let identities: BTreeSet<_> = running
        .iter()
        .map(|running| {
            (
                format!("{:?}", running.node.repo.local_peer_id()),
                format!("{:?}", running.node.authority.service.node_public_key()),
            )
        })
        .collect();
    assert_eq!(identities.len(), NODES);
    for running in &running {
        assert_note(running).await;
    }
    // Two sockets with the same file name coexist because their directories differ, and both
    // are being served.
    let sockets: Vec<PathBuf> = LISTENING
        .iter()
        .map(|index| {
            running[*index]
                .node
                .listeners
                .unix_socket
                .clone()
                .expect("the node listens")
        })
        .collect();
    assert_ne!(sockets[0], sockets[1]);
    assert_eq!(sockets[0].file_name(), sockets[1].file_name());
    for socket in &sockets {
        drop(tokio::net::UnixStream::connect(socket).await.unwrap());
    }

    // Stopped together too, keeping what the durable nodes must find again.
    let notes: Vec<_> = running
        .iter()
        .filter(|running| DURABLE.contains(&running.index))
        .map(|running| {
            (
                running.index,
                running.receipt.clone(),
                running.value.clone(),
            )
        })
        .collect();
    join(
        running
            .drain(..)
            .map(|running| async move { shutdown(running.node, running.stop).await }),
    )
    .await;
    for socket in &sockets {
        assert!(!socket.exists(), "{} outlived its node", socket.display());
    }

    // Booted again from the same bindings, the durable nodes have their state.
    let mut reboots = Vec::new();
    for (index, receipt, value) in notes {
        let shared = SHARED_DISK_REPO.contains(&index).then_some(&disk_repo);
        let config = fleet.config(index, shared).await;
        reboots.push(async move {
            let (node, stop) = BigNode::boot(config).await.unwrap();
            Running {
                index,
                node,
                stop,
                receipt,
                value,
            }
        });
    }
    let rebooted = join(reboots).await;
    for running in &rebooted {
        assert_note(running).await;
        let again = create(
            &running.node,
            &format!("note-{}", running.index),
            running.value.clone(),
        )
        .await;
        assert_eq!(again, running.receipt, "node {}", running.index);
    }
    join(
        rebooted
            .into_iter()
            .map(|running| async move { shutdown(running.node, running.stop).await }),
    )
    .await;

    // Nothing is left: every database handle is dropped, no ephemeral database's temp
    // directory remains, and the process holds no task it did not hold before.
    drop((memory_repo, disk_repo));
    for runtime in &fleet.runtimes {
        runtime.drain_workers().await;
        assert_eq!(runtime.live_database_count(), 0);
        assert_eq!(runtime.open_connection_count(), 0);
    }
    for home in fleet.ephemeral_homes.lock().unwrap().iter() {
        assert!(!home.exists(), "{} was leaked", home.display());
    }
    // A dropped runtime's idle-eviction task ends when its wake-up fires, on a worker thread of
    // its own time, so the count settles rather than drops at once. Whatever does not settle is
    // a leak, and the test then runs into nextest's hard timeout.
    fleet.runtimes.clear();
    while tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
        > baseline_tasks
    {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
