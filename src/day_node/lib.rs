//! A node's directory and the one way a process boots a node from it.
//!
//! The `day` CLI (which finds a running node or holds one) and the app shells that always hold
//! their own (Android has no second process) share this crate, so a node is booted from its
//! directory the same way everywhere (ADR 033 sections 1 and 5).
//!
//! ```text
//! <node>/node.json        identity seed, peers and dev hooks (0600)
//! <node>/node.sqlite, repo/, blobs/   the node's own storage, opened by `big_node::Bindings`
//! ```
//!
//! Every other secret a node keeps is in the platform keyring, under entries that carry the node
//! id too.

use big_node::BigNode;
use big_node::Listeners;
use secrets_rs::SecretStore;
use serde::Deserialize;
use serde::Serialize;
use sqlite_rs::SqliteRuntime;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::default;
use utils_rs::prelude::eyre;

/// What `node.json` records. The identity seed is the node's keyhive identity and, through it,
/// its node id (ADR 033 section 6: one node key).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSettings {
    version: u32,
    /// 32 bytes, lowercase hex.
    identity_seed: String,
    /// The node's network attachment. Absent: the node attaches none, so it neither dials nor
    /// can be dialed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peers: Option<PeerSettings>,
    /// Run the node as a relay (ADR 038 section 3): accept any peer and mirror every drawer the
    /// node is a member of, which a user grants with `day drawers add-member`. A relay needs the
    /// network attachment, so this requires a `peers` section.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub relay: bool,
    /// Development hooks, all off unless set here by hand (a command can't turn one on, since
    /// the node reads this file at boot).
    #[serde(default, skip_serializing_if = "DevSettings::is_default")]
    pub dev: DevSettings,
}

/// The `dev` section of `node.json`. Keys are snake_case, as the file's other top-level keys are.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DevSettings {
    /// Lets `day carts dev-takeover` run an installed dev build as another installed cartridge,
    /// with its actor, data and grants (ADR 045 section 5). Never a side effect of anything
    /// else: with it off the node refuses to set a takeover and does not honor one found in
    /// its config document.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_slot_takeover: bool,
}

impl DevSettings {
    fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

/// The `peers` section of `node.json`: its presence is what makes the node attach the peers
/// service (the Iroh endpoint, keyed by the node's identity).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PeerSettings {
    /// Peers dialed as soon as the node starts and redialed with backoff when the connection
    /// drops, as endpoint tickets (the `address` a node's `getNodeInfo` reports). Which nodes
    /// belong here is the composition's policy, not the node's: a user node lists its own other
    /// devices and a relay, a relay lists none.
    #[serde(default)]
    pub bootstrap: Vec<String>,
    /// Bind the loopback interface only, with relays off (see
    /// `big_node::peers::PeersConfig::loopback_only`).
    #[serde(default)]
    pub loopback_only: bool,
}

impl PeerSettings {
    /// The id (lowercase hex) of the endpoint a bootstrap ticket names.
    fn ticket_node_id(ticket: &str) -> Res<String> {
        let addr = big_node::peers::parse_ticket(ticket)?;
        Ok(data_encoding::HEXLOWER.encode(addr.id.as_bytes()))
    }

    /// Adds `ticket`, replacing an entry for the same endpoint (so a node that changed its
    /// addresses is re-added, not duplicated). Returns whether the list changed.
    pub fn add_bootstrap(&mut self, ticket: &str) -> Res<bool> {
        let id = Self::ticket_node_id(ticket)?;
        for existing in &mut self.bootstrap {
            if Self::ticket_node_id(existing)? == id {
                let changed = existing != ticket;
                ticket.clone_into(existing);
                return Ok(changed);
            }
        }
        self.bootstrap.push(ticket.to_owned());
        Ok(true)
    }

    /// The peers config a node boots with: dial the bootstrap list under the default mirroring
    /// policy, or under the relay policy when the node is a relay.
    pub fn peers_config(&self, relay: bool) -> Res<big_node::peers::PeersConfig> {
        let dial = self
            .bootstrap
            .iter()
            .map(|ticket| big_node::peers::parse_ticket(ticket))
            .collect::<Res<Vec<_>>>()?;
        Ok(big_node::peers::PeersConfig {
            loopback_only: self.loopback_only,
            policy: if relay {
                big_node::peers::PolicyChoice::Custom(Arc::new(big_node::peers::RelayPolicy::new(
                    dial,
                )))
            } else {
                big_node::peers::PolicyChoice::Mirror(big_node::peers::MirrorPolicy { dial })
            },
            ..default()
        })
    }
}

impl NodeSettings {
    const VERSION: u32 = 1;
    const FILE: &'static str = "node.json";

    /// Whether a node has been created in `root`.
    pub fn exists(root: &Path) -> bool {
        root.join(Self::FILE).is_file()
    }

    /// Creates `root` and `node.json` with a fresh identity. The settings file is created
    /// exclusively, so an existing node is refused here and never overwritten, whoever else is
    /// initializing the same directory at the same time.
    pub fn create(root: &Path) -> Res<Self> {
        use rand::RngCore;
        std::fs::create_dir_all(root).wrap_err_with(|| format!("creating {}", root.display()))?;
        let mut seed = [0_u8; 32];
        rand::rng().fill_bytes(&mut seed);
        let settings = Self {
            version: Self::VERSION,
            identity_seed: data_encoding::HEXLOWER.encode(&seed),
            peers: None,
            relay: false,
            dev: DevSettings::default(),
        };
        let path = root.join(Self::FILE);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .wrap_err_with(|| format!("creating {}", path.display()))?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&settings)?)?;
        Ok(settings)
    }

    /// Reads and validates the settings of the node in `root`.
    pub fn load(root: &Path) -> Res<Self> {
        let path = root.join(Self::FILE);
        let bytes = std::fs::read(&path).wrap_err_with(|| {
            format!(
                "reading {}; is there a node here? (`day node init`)",
                path.display()
            )
        })?;
        let settings: Self = serde_json::from_slice(&bytes)
            .wrap_err_with(|| format!("parsing {}", path.display()))?;
        eyre::ensure!(
            settings.version == Self::VERSION,
            "{} is version {}, this day understands version {}",
            path.display(),
            settings.version,
            Self::VERSION
        );
        eyre::ensure!(
            !settings.relay || settings.peers.is_some(),
            "{}: a relay needs a `peers` section (it is reached over the network)",
            path.display()
        );
        if let Some(peers) = &settings.peers {
            // Fail on a bad ticket whichever command reads the settings, not when a node
            // happens to boot it.
            peers
                .peers_config(settings.relay)
                .wrap_err_with(|| format!("{}: bootstrap list", path.display()))?;
        }
        Ok(settings)
    }

    /// Rewrites `node.json` in `root` with `change` applied. The new file is written beside it
    /// and renamed over it, so a reader (a node booting) sees the old settings or the new,
    /// never half. Whether the node is serving does not matter: settings are read once, at boot.
    pub fn update<T>(root: &Path, change: impl FnOnce(&mut Self) -> Res<T>) -> Res<T> {
        let mut settings = Self::load(root)?;
        let out = change(&mut settings)?;
        let staging = root.join("node.json.new");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&staging)
            .wrap_err_with(|| format!("creating {}", staging.display()))?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&settings)?)?;
        file.sync_all()?;
        let path = root.join(Self::FILE);
        std::fs::rename(&staging, &path)
            .wrap_err_with(|| format!("replacing {}", path.display()))?;
        Ok(out)
    }

    pub fn seed(&self) -> [u8; 32] {
        let bytes = data_encoding::HEXLOWER
            .decode(self.identity_seed.as_bytes())
            .expect("node.json holds a hex identity seed");
        <[u8; 32]>::try_from(bytes.as_slice()).expect("node.json holds a 32 byte identity seed")
    }

    /// The node's id: the public half of the identity seed, as the repo derives its own peer id.
    pub fn node_id(&self) -> String {
        big_node::node_id(&self.seed())
    }

    /// The nodes a reference to something this node holds can name as holders (the `via=` hint,
    /// ADR 035): this node, then the peers it dials at start (its other devices and the relay it
    /// syncs through, `peers.bootstrap`). Read from `node.json`, so it names what the node is
    /// configured to sync with, not whoever is connected at the moment.
    pub fn via_keys(&self) -> Res<Vec<big_cabinet_core::dmeta::MultibaseKey>> {
        let own = big_node::node_id(&self.seed());
        let own = data_encoding::HEXLOWER.decode(own.as_bytes())?;
        let bootstrap = self.peers.iter().flat_map(|peers| &peers.bootstrap);
        std::iter::once(Ok(own))
            .chain(bootstrap.map(|ticket| {
                let addr = big_node::peers::parse_ticket(ticket)?;
                Ok(addr.id.as_bytes().to_vec())
            }))
            .map(|key: Res<Vec<u8>>| {
                Ok(big_cabinet_core::dmeta::MultibaseKey::try_from(
                    key?.as_slice(),
                )?)
            })
            .collect()
    }
}

/// A node booted in this process, with everything it must stop, in order.
pub struct HeldNode {
    pub node: BigNode,
    stop: big_node::StopToken,
    secrets: Arc<SecretStore>,
}

impl HeldNode {
    /// Boots the node in `root` (`node.json` read by the caller as `settings`), serving
    /// `listeners`. With `attach_peers` the node attaches the peers service when `settings` has
    /// a `peers` section; without it the node neither dials nor can be dialed, which is right for
    /// a node that lives for one command. `schemas` is the datum vocabulary the node admits.
    pub async fn boot(
        root: &Path,
        settings: &NodeSettings,
        listeners: Listeners,
        attach_peers: bool,
        schemas: Arc<big_cabinet::SchemaRepo>,
    ) -> Res<Self> {
        let peers = if attach_peers {
            settings
                .peers
                .as_ref()
                .map(|peers| peers.peers_config(settings.relay))
                .transpose()?
        } else {
            None
        };
        let runtime = Arc::new(SqliteRuntime::new(default()));
        let bindings = big_node::Bindings::in_directory(&runtime, root, None).await?;
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
            schemas,
            secrets: Arc::clone(&secrets),
            index_concurrency: std::num::NonZeroUsize::new(2).expect("two is not zero"),
            blob_storage: bindings.blobs,
            listeners,
            peers,
            allow_slot_takeover: settings.dev.allow_slot_takeover,
        };
        let (node, stop) = BigNode::boot(config).await?;
        Ok(Self {
            node,
            stop,
            secrets,
        })
    }

    /// Stops the node's services in reverse order of their boot, then the secret store they
    /// shared.
    pub async fn stop(self) -> Res<()> {
        let Self {
            node,
            stop,
            secrets,
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
