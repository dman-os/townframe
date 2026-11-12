use crate::dir::LockRole;
use crate::dir::NodeDir;
use crate::dir::NodeLock;
use crate::interlude::*;

use big_node::BigNode;
use big_node::Listeners;
use secrets_rs::SecretStore;
use sqlite_rs::SqliteRuntime;

/// A node booted in this process, with everything it must stop in order.
pub struct Booted {
    pub node: BigNode,
    stop: big_node::StopToken,
    secrets: Arc<SecretStore>,
    // Held to the end: the node's storage is this process's alone while it is booted.
    _lock: NodeLock,
}

impl Booted {
    /// Boots the node in `dir` on the lock the caller holds, serving its socket. `role` is how
    /// it attaches to the network: a daemon attaches the peers service when `node.json` has a
    /// `peers` section; a command's own node attaches none, since it lives for one command and
    /// must not dial or hold the node's endpoint. The lock announces it is serving once the
    /// socket accepts connections.
    pub async fn boot(dir: &NodeDir, mut lock: NodeLock, role: LockRole) -> Res<Self> {
        let settings = dir.settings()?;
        let listeners = Listeners {
            unix_socket: Some(dir.socket_path()?),
            ..default()
        };
        let peers = match role {
            LockRole::Daemon => settings
                .peers
                .as_ref()
                .map(|peers| peers.peers_config(settings.relay))
                .transpose()?,
            LockRole::Command => None,
        };
        let runtime = Arc::new(SqliteRuntime::new(default()));
        let bindings = big_node::Bindings::in_directory(&runtime, dir.root(), None).await?;
        let secrets = Arc::new(SecretStore::boot().await?);
        let config = big_node::Config {
            repo: big_repo::Config {
                node_identity_seed: settings.seed(),
                storage: bindings.repo,
                scope_key: "day".into(),
                hidden_parts: default(),
                automerge_frontier_group_scope: default(),
                causal_checkpoint_group_scope: default(),
                group_part_group_scope: default(),
            },
            sql: bindings.sql,
            schemas: Arc::new(big_cabinet::SchemaRepo::open(std::iter::empty())?),
            secrets: Arc::clone(&secrets),
            index_concurrency: std::num::NonZeroUsize::new(2).expect("two is not zero"),
            blob_storage: bindings.blobs,
            listeners,
            peers,
            allow_slot_takeover: settings.dev.allow_slot_takeover,
        };
        let (node, stop) = BigNode::boot(config).await?;
        lock.serving()?;
        Ok(Self {
            node,
            stop,
            secrets,
            _lock: lock,
        })
    }

    pub async fn stop(self) -> Res<()> {
        let Self {
            node,
            stop,
            secrets,
            _lock,
        } = self;
        drop(node);
        stop.stop().await?;
        // The node's config held the other reference and went with the boot.
        Arc::try_unwrap(secrets)
            .unwrap_or_else(|_| panic!("the stopped node still holds the secret store"))
            .stop()
            .await
    }
}
