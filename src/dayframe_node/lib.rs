#![recursion_limit = "256"]

//! The node-holding half of a native Dayframe host's composition root (Clay on the node,
//! sections 1 to 3).
//!
//! A host with no second process (Android) is always the holder of its node: [`NodeComposition`]
//! boots a `big_node` in this process from the host's data directory, through the same
//! [`day_node::HeldNode`] boot the `day` CLI uses, opens the `micros` subsystem actor and one actor
//! per installed micro below it, and gives each micro a [`MicroEgress`] that answers its
//! `dispatch.local` requests as that actor.
//!
//! Construction order is the node, then the micro actors and their egress. [`NodeComposition::stop`]
//! undoes it in reverse: the egress handles go first because they hold dispatch clients that must
//! not outlive the node they call, then the node stops its own services in reverse of their boot.
//!
//! The node runs on a runtime of its own. Its tasks (the peers endpoint, dispatch, indexing) must
//! not share a host's UI loop, and the host's runtime flavor is not the node's business.

use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use clay_guest_session::egress::DispatchEgress;
use clay_node_egress::MicroEgress;
use day_node::HeldNode;
use day_node::NodeSettings;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use utils_rs::prelude::ERROR_TOKIO;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::default;

#[cfg(test)]
mod e2e;

/// One micro the composition installs, and what node access it is granted.
///
/// INTERIM: the permission model (per-micro `uses` intersected with the cartridge's grants, Clay
/// on the node section 2, and its 6.2 lowering of a domain handle) replaces both fields. Until it
/// lands, the composition names the granted NSIDs and drawers of each micro here, and this table
/// is the only place that does; empty grants nothing, which is every micro's grant today.
#[derive(Debug, Clone)]
pub struct MicroInstall {
    /// The micro's id text, `<did>/<name>`.
    pub micro: String,
    /// The NSIDs of the node methods the micro may call over `dispatch.local`.
    pub methods: BTreeSet<String>,
    /// The drawers whose documents the micro may read (`Read`) or read and write (`Write`).
    /// A micro can only name a document capability to the docs methods it is granted in
    /// `methods` (`readDatums`, `writeDatums`, ...), and cannot mint one, so the composition mints
    /// one bound to the micro's actor over these drawers ([`NodeComposition::doc_capability`]).
    pub drawers: Vec<(DrawerId, DrawerAccess)>,
}

/// The node methods each micro is granted, by its plug name (the `<name>` of `<did>/<name>`).
///
/// INTERIM: see [`MicroInstall`]. This table is the one place the composition names a micro's node
/// access, and it grants nothing today: a micro here gets an actor and an egress, and can call no
/// method.
const INTERIM_GRANTS: &[(&str, &[&str])] = &[];

impl MicroInstall {
    /// The installs of the micros with these ids, each granted its [`INTERIM_GRANTS`].
    pub fn interim(micros: impl IntoIterator<Item = String>) -> Vec<Self> {
        micros
            .into_iter()
            .map(|micro| {
                let name = micro
                    .split_once('/')
                    .unwrap_or_else(|| panic!("micro id {micro:?} is not `<did>/<name>`"))
                    .1;
                let granted = INTERIM_GRANTS
                    .iter()
                    .find(|(granted, _)| *granted == name)
                    .map_or(&[][..], |(_, methods)| methods);
                Self {
                    methods: granted.iter().map(|nsid| (*nsid).to_owned()).collect(),
                    drawers: Vec::new(),
                    micro,
                }
            })
            .collect()
    }
}

pub struct NodeConfig {
    /// The node's directory: `node.json`, `node.sqlite`, `repo/` and `blobs/` (see
    /// [`day_node`]). Created, with a fresh identity, when it holds no node yet.
    pub root: PathBuf,
    /// Endpoint tickets of the peers this node dials (see [`read_tickets`]). They are added to
    /// the node's `node.json` bootstrap list, so they persist and a node booted with none still
    /// dials the ones an earlier boot was given.
    pub bootstrap: Vec<String>,
    pub installs: Vec<MicroInstall>,
}

/// A booted node and the micros installed on it.
pub struct NodeComposition {
    /// Declared first, dropped last: the node's tasks run on it. See [`Self::stop`].
    runtime: tokio::runtime::Runtime,
    installed: BTreeMap<String, Installed>,
    held: HeldNode,
}

/// What the composition holds for one installed micro.
struct Installed {
    egress: Arc<MicroEgress>,
    /// The micro's document capability over its configured drawers; `None` for a micro with none.
    doc_capability: Option<CapId>,
}

impl NodeComposition {
    /// Boots the node in `config.root` and installs every micro in `config.installs`.
    pub async fn boot(config: NodeConfig) -> Res<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("dayframe-node")
            .enable_all()
            .build()?;
        let booted = runtime
            .spawn(async move {
                let NodeConfig {
                    root,
                    bootstrap,
                    installs,
                } = config;
                prepare_settings(&root, &bootstrap)?;
                let settings = NodeSettings::load(&root)?;
                // No listeners: the holder is its own only client, and reaches the node through
                // its dispatch client.
                let held = HeldNode::boot(&root, &settings, default(), true).await?;
                match install_all(&held.node, installs).await {
                    Ok(installed) => Ok((held, installed)),
                    Err(error) => match held.stop().await {
                        Ok(()) => Err(error),
                        Err(stop) => {
                            Err(error.wrap_err(format!("stopping the node also failed: {stop:#}")))
                        }
                    },
                }
            })
            .await
            .expect(ERROR_TOKIO);
        match booted {
            Ok((held, installed)) => Ok(Self {
                runtime,
                installed,
                held,
            }),
            Err(error) => {
                runtime.shutdown_background();
                Err(error)
            }
        }
    }

    pub fn node(&self) -> &big_node::BigNode {
        &self.held.node
    }

    /// The `dispatch.local` egress for `micro` (its id text), to hand to its Wasmtime factory.
    /// `None` for a micro that was not in the install table: it gets no way out.
    pub fn egress(&self, micro: &str) -> Option<Arc<dyn DispatchEgress>> {
        self.installed
            .get(micro)
            .map(|installed| Arc::clone(&installed.egress) as Arc<dyn DispatchEgress>)
    }

    /// The document capability minted for `micro` over its configured drawers, bound to the
    /// micro's actor: what the shell puts in the micro's configuration for the micro to name in
    /// `readDatums` and the other docs methods. `None` for a micro with no drawers, or none
    /// installed.
    ///
    /// INTERIM: see [`MicroInstall`].
    pub fn doc_capability(&self, micro: &str) -> Option<CapId> {
        self.installed.get(micro)?.doc_capability
    }

    /// Installs one more micro on the running node, replacing an earlier install of the same id
    /// (its actor is reused; its capabilities are minted anew).
    pub async fn install_micro(&mut self, install: MicroInstall) -> Res<()> {
        let micro = install.micro.clone();
        let installed = install_one(&self.held.node, install).await?;
        self.installed.insert(micro, installed);
        Ok(())
    }

    /// This node's dialable endpoint ticket: what another node puts in its bootstrap list to dial
    /// it. Always present, since the composition always attaches the peers service.
    pub fn address(&self) -> String {
        self.held
            .node
            .peers
            .as_ref()
            .expect("the composition attaches the peers service")
            .ticket()
    }

    /// Stops the micros' egress, then the node, then the runtime the node ran on. The runtime is
    /// shut down whether or not the node stopped cleanly.
    pub async fn stop(self) -> Res<()> {
        let Self {
            runtime,
            installed,
            held,
        } = self;
        drop(installed);
        let stopped = runtime.spawn(held.stop()).await.expect(ERROR_TOKIO);
        // Dropping a runtime inside an async context panics; this does not wait for threads.
        runtime.shutdown_background();
        stopped
    }
}

/// Creates the node if `root` holds none, makes sure it attaches the peers service (a phone that
/// is never dialed by a laptop can't sync), and records `bootstrap`.
fn prepare_settings(root: &Path, bootstrap: &[String]) -> Res<()> {
    if !NodeSettings::exists(root) {
        NodeSettings::create(root)?;
    }
    NodeSettings::update(root, |settings| {
        let peers = settings.peers.get_or_insert_with(default);
        for ticket in bootstrap {
            peers.add_bootstrap(ticket)?;
        }
        Ok(())
    })
}

async fn install_all(
    node: &big_node::BigNode,
    installs: Vec<MicroInstall>,
) -> Res<BTreeMap<String, Installed>> {
    let mut installed = BTreeMap::new();
    for install in installs {
        let micro = install.micro.clone();
        installed.insert(micro, install_one(node, install).await?);
    }
    Ok(installed)
}

/// Opens the micro's actor below the `micros` subsystem actor, mints its egress and, when it has
/// drawers, its document capability.
async fn install_one(node: &big_node::BigNode, install: MicroInstall) -> Res<Installed> {
    let MicroInstall {
        micro,
        methods,
        drawers,
    } = install;
    let authority = &node.authority;
    let minter = Authorization::identity(authority.root);
    let open = |parent, path: String, rights| {
        authority.client.call(
            RequestContext::root(),
            OpenActor {
                parent,
                path,
                rights,
            },
        )
    };
    // A subsystem actor holds every right, as `big_node` opens `nodules`; a micro's own actor
    // can only be used. Opening an open actor is idempotent.
    let micros = open(authority.root, "micros".into(), Rights::all())
        .await
        .wrap_err("opening the micros subsystem actor")?;
    let actor: OpenedActor = open(
        micros.capability,
        format!("micros/{}", actor_segment(&micro)),
        Rights::USE,
    )
    .await
    .wrap_err_with(|| format!("opening the actor of micro {micro}"))?;
    let egress = MicroEgress::install(node.dispatch.client.clone(), minter, &actor, methods)
        .await
        .wrap_err_with(|| format!("minting the capability of micro {micro}"))?;
    let doc_capability = if drawers.is_empty() {
        None
    } else {
        let minted = node
            .docs
            .client
            .call_with(
                RequestContext::root(),
                minter.metadata(),
                MintDocCapability {
                    scope: DocScope {
                        drawers: drawers.into_iter().collect(),
                        ..DocScope::default()
                    },
                    rights: Rights::USE,
                    subject: Some(actor.actor.actor_id),
                },
            )
            .await
            .wrap_err_with(|| format!("minting the document capability of micro {micro}"))?;
        Some(minted.capability_id)
    };
    Ok(Installed {
        egress: Arc::new(egress),
        doc_capability,
    })
}

/// The one path segment a micro's actor lives at below `micros`, by the nodules' rule (`big_nodule`
/// derives `<name>-<blake3(cartridge id)[..32 hex]>`): the id holds `/` and `:`, which an actor
/// segment cannot, the name keeps the segment readable, and the 128-bit blake3 prefix of the full
/// id keeps it unique across namespaces.
fn actor_segment(micro: &str) -> String {
    let (_, name) = micro
        .split_once('/')
        .unwrap_or_else(|| panic!("micro id {micro:?} is not `<did>/<name>`"));
    let digest = blake3::hash(micro.as_bytes()).to_hex();
    format!("{name}-{}", &digest.as_str()[..32])
}

/// Reads the bootstrap tickets in `path`, one per line; blank lines and `#` comments are skipped.
/// A missing file is no tickets: the operator has not paired this node yet.
pub fn read_tickets(path: &Path) -> Res<Vec<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).wrap_err_with(|| format!("reading {}", path.display())),
    };
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect())
}
