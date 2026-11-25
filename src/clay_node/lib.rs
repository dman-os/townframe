#![recursion_limit = "256"]

//! The node-holding half of a native Clay host's composition root (Clay on the node,
//! sections 1 to 3).
//!
//! A host with no second process (Android) is always the holder of its node: [`NodeComposition`]
//! boots a `big_node` in this process from the host's data directory, through the same
//! [`day_node::Booted`] boot the `day` CLI uses, under the node lock `day` takes, and gives each
//! micro a [`MicroEgress`] that answers its `dispatch.local` requests as the micro's own actor.
//!
//! Construction order is the node, then the handles follower and the micros' egress.
//! [`NodeComposition::stop`] undoes it in reverse: the follower goes first because it calls the
//! node's services, the egress handles with the composition because they hold dispatch clients
//! that must not outlive the node they call, then the node stops its own services in reverse of
//! their boot.
//!
//! Micros come from two places. A micro a cartridge contributes is loaded from the node's catalog
//! and its handles are the node's: the `micros` nodule of the core cartridge mints them and the
//! composition is its client ([`cartridge`], ADR 052). A micro that is in no cartridge keeps the
//! interim path ([`MicroInstall`], which the host builds and names the grants of, and the
//! composition mints) until it is published as one.
//!
//! The node runs on a runtime of its own. Its tasks (the peers endpoint, dispatch, indexing) must
//! not share a host's UI loop, and the host's runtime flavor is not the node's business.

use big_blobs_core::BlobsProtocol;
use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::DrawerReach;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::DrawersProtocol;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_cartridges_core::Revision;
use big_cartridges_core::protocol::CartridgesProtocol;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::DispatchScope;
use big_nodule_core::protocol::NodulesProtocol;
use clay_guest_session::egress::DispatchEgress;
use day_node::Booted;
use day_node::LockRole;
use day_node::NodeDir;
use day_node::NodeSettings;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_client_native_rs::Client;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tower::ServiceExt;
use utils_rs::prelude::ERROR_TOKIO;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::default;
use utils_rs::prelude::ferr;

mod cartridge;
#[cfg(test)]
mod e2e;
mod egress;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use cartridge::CartridgeMicro;
use cartridge::HandlesFollower;
pub use egress::MicroEgress;

/// One micro the composition installs, and what node access it is granted.
///
/// INTERIM, for micros that are in no cartridge only. A micro a cartridge contributes is minted
/// by the node's `micros` nodule ([`NodeComposition::sync_cartridge_micros`]), and this table never
/// applies to it. Until such a micro is published as a cartridge, the host
/// names the granted NSIDs and drawers of each here, and this table is the only place that does;
/// empty grants nothing.
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
    /// The configuration datum the shell writes for the micro at install, if any. The composition
    /// adds the micro's document capability to it as `docCap` (see
    /// [`NodeComposition::config_datum`]), since only the composition can mint one.
    pub config: Option<InstallConfig>,
}

/// A micro's configuration datum as its install table states it: where it goes and the members
/// the installer knows (the drawers the micro works over). INTERIM, like [`MicroInstall`].
#[derive(Debug, Clone)]
pub struct InstallConfig {
    pub tag: String,
    pub key: String,
    pub value: serde_json::Map<String, serde_json::Value>,
    /// The drawers whose blobs the micro reads, if its config carries a blobs capability as
    /// `blobCap`: one minted for its actor that reaches every blob these drawers' inventories hold,
    /// at `Read` (see [`install_one`]). `None`: no `blobCap`.
    pub blob_drawers: Option<Vec<DrawerId>>,
}

pub struct NodeConfig {
    /// The node's directory: `node.json`, `node.sqlite`, `repo/` and `blobs/` (see
    /// [`day_node`]). Created, with a fresh identity, when it holds no node yet.
    pub root: PathBuf,
    /// Endpoint tickets of the peers this node dials (see [`read_tickets`]). They are added to
    /// the node's `node.json` bootstrap list, so they persist and a node booted with none still
    /// dials the ones an earlier boot was given.
    pub bootstrap: Vec<String>,
    /// The datum vocabulary the node admits: the Lexicons of the datums its micros read and
    /// write (a node refuses a datum whose schema it does not know).
    pub schemas: Arc<big_cabinet::SchemaRepo>,
    pub installs: Vec<MicroInstall>,
    /// Whether the holder serves the node's socket (see [`day_node::NodeDir::socket_path`]), so
    /// `day` and other processes can reach it. A host with no second process (Android) serves
    /// none: nothing there can reach one, and its data directory is too deep for a socket path.
    /// A serving host finds the node held by another process as an error, since attaching to a
    /// running node as a client is not built.
    pub serve: bool,
}

/// A booted node and the micros installed on it.
pub struct NodeComposition {
    /// Declared first, dropped last: the node's tasks run on it. See [`Self::stop`].
    runtime: tokio::runtime::Runtime,
    installed: BTreeMap<String, Installed>,
    /// The node and the lock that makes its storage this process's alone, stopped in that order.
    booted: Booted,
    minting: Minting,
    /// Follows the node's re-mints of the cartridge micros' handles.
    follower: HandlesFollower,
}

/// What the composition holds for one installed micro.
struct Installed {
    egress: Arc<MicroEgress>,
    /// The install's configuration datum with `docCap` added: `(tag, key, value)`. Only an
    /// interim install has one.
    config: Option<(String, String, serde_json::Value)>,
    state: Arc<MintedState>,
    /// The pinned revision of the cartridge a cartridge micro was loaded from; `None` for an
    /// interim install.
    revision: Option<Revision>,
}

/// What a micro's handles are, as the host reads them synchronously. A cartridge micro's are the
/// node's, bound by [`HandlesFollower`]; an interim install's are minted once by the composition.
struct MintedState {
    /// The micro's document capability: over its configured drawers for an interim install, and
    /// the docs handle the `micros` nodule minted for a cartridge's micro. `None` for a micro with
    /// none.
    doc_capability: Mutex<Option<CapId>>,
    /// Counts the times the handles were minted again, so a host can tell when a grant it
    /// followed took effect.
    generation: tokio::sync::watch::Sender<u64>,
}

impl MintedState {
    fn new(doc_capability: Option<CapId>) -> Self {
        Self {
            doc_capability: Mutex::new(doc_capability),
            generation: tokio::sync::watch::channel(0).0,
        }
    }
}

impl NodeComposition {
    /// Boots the node in `config.root` and installs every micro in `config.installs`.
    pub async fn boot(config: NodeConfig) -> Res<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("clay-node")
            .enable_all()
            .build()?;
        let booted = runtime
            .spawn(async move {
                let NodeConfig {
                    root,
                    bootstrap,
                    schemas,
                    installs,
                    serve,
                } = config;
                let dir = NodeDir::new(root);
                prepare_settings(&dir, &bootstrap).await?;
                // The holder takes the node's lock as `day` does, so `day` and any other host
                // find the directory held instead of booting the same storage a second time.
                // A serving host (desktop) holds the lock with its socket, which `day` reaches;
                // one with no second process (Android) holds it unserved and is its own only
                // client, through its dispatch client. Either way another holder is a failure
                // here: attaching to it as a client is not built.
                let lock = match serve {
                    true => dir.try_lock(LockRole::Serve)?,
                    false => dir.try_lock_unserved(LockRole::Serve)?,
                };
                let lock = lock.map_err(|held| {
                    ferr!(
                        "the node at {} is held by {held}; attaching to a running node is not implemented yet, so stop that process first",
                        dir.root().display()
                    )
                })?;
                let booted = Booted::boot(&dir, lock, LockRole::Serve, schemas).await?;
                let minting = Minting::of(booted.node());
                // Started here, on the node's runtime, before anything that can fail below, and
                // stopped with it.
                let follower = HandlesFollower::start(minting.clone());
                match install_all(&minting, installs).await {
                    Ok(installed) => Ok((booted, installed, minting, follower)),
                    Err(error) => {
                        follower.stop().await;
                        match booted.stop().await {
                            Ok(()) => Err(error),
                            Err(stop) => Err(error
                                .wrap_err(format!("stopping the node also failed: {stop:#}"))),
                        }
                    }
                }
            })
            .await
            .expect(ERROR_TOKIO);
        match booted {
            Ok((booted, installed, minting, follower)) => Ok(Self {
                runtime,
                installed,
                booted,
                minting,
                follower,
            }),
            Err(error) => {
                runtime.shutdown_background();
                Err(error)
            }
        }
    }

    pub fn node(&self) -> &big_node::BigNode {
        self.booted.node()
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
        *self
            .installed
            .get(micro)?
            .state
            .doc_capability
            .lock()
            .expect("doc capability lock")
    }

    /// A receiver that changes each time `micro`'s handles are bound again, which a cartridge
    /// micro's are when the node re-mints them for moved grants (`micros.awaitHandles`, followed by
    /// [`Self::sync_cartridge_micros`]'s micros); the host reads [`Self::doc_capability`] again
    /// after it. `None` for a micro not installed.
    pub fn handles_changed(&self, micro: &str) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.installed.get(micro)?.state.generation.subscribe())
    }

    /// The configuration datum to write for `micro`: its install's [`InstallConfig`] with `docCap`
    /// set to [`Self::doc_capability`], as `(tag, key, value)`. The shell writes it where the
    /// micro reads its configuration. `None` for a micro with no config in its install.
    pub fn config_datum(&self, micro: &str) -> Option<&(String, String, serde_json::Value)> {
        self.installed.get(micro)?.config.as_ref()
    }

    /// Installs one more micro on the running node, replacing an earlier install of the same id
    /// (its actor is reused; its capabilities are minted anew).
    pub async fn install_micro(&mut self, install: MicroInstall) -> Res<()> {
        let micro = install.micro.clone();
        let installed = install_one(&self.minting, install).await?;
        self.installed.insert(micro, installed);
        Ok(())
    }

    /// A transport that calls this node's methods as its operator, for typed clients
    /// (`service_client_core_rs`). Operator-only: it presents no capability and acts as the
    /// node's root authority, so it is for the host's own installs at boot and is never handed to
    /// a micro (a micro's only way out is its [`Self::egress`]).
    pub fn operator_transport(&self) -> OperatorTransport {
        let node = self.booted.node();
        OperatorTransport {
            router: big_dispatch::xrpc_router(
                node.dispatch.client.clone(),
                big_dispatch::Anonymous::Acts(Authorization::identity(node.authority.root)),
                big_dispatch::Edge::Embedded,
            )
            .into_router(),
        }
    }

    /// This node's dialable endpoint ticket: what another node puts in its bootstrap list to dial
    /// it. Always present, since the composition always attaches the peers service.
    pub fn address(&self) -> String {
        self.booted
            .node()
            .peers
            .as_ref()
            .expect("the composition attaches the peers service")
            .ticket()
    }

    /// Stops the handles follower and drops the micros' egress, then the node, then the runtime
    /// the node ran on. The follower goes first because it calls the node's services. The runtime
    /// is shut down whether or not the node stopped cleanly.
    pub async fn stop(self) -> Res<()> {
        let Self {
            runtime,
            installed,
            booted,
            minting,
            follower,
        } = self;
        follower.stop().await;
        drop((installed, minting));
        let stopped = runtime.spawn(booted.stop()).await.expect(ERROR_TOKIO);
        // Dropping a runtime inside an async context panics; this does not wait for threads.
        runtime.shutdown_background();
        stopped
    }
}

/// The node's own XRPC router, served in process: what a host's typed clients speak over, as the
/// node's operator. Built by [`NodeComposition::operator_transport`]; see there for why it is
/// operator-only. It is the router the `day` CLI serves for the node it holds, so a request takes
/// the path a CLI command's does (`big_dispatch::xrpc_router`), not a second one.
#[derive(Clone)]
pub struct OperatorTransport {
    router: axum::Router,
}

/// The largest JSON answer the transport reads, as the CLI's in-process client bounds it.
const MAX_ANSWER_OCTETS: usize = 16 * 1024 * 1024;

impl HttpTransport for OperatorTransport {
    async fn send(&self, request: XrpcRequest) -> Result<XrpcResponse, HttpTransportError> {
        let failed = |error: &dyn std::fmt::Display| HttpTransportError {
            message: error.to_string(),
        };
        let mut builder = http::Request::builder()
            .method(request.method)
            .uri(format!("/{}", request.path));
        *builder
            .headers_mut()
            .expect("a builder with a valid method and URI has headers") = request.headers;
        let body = match request.body {
            XrpcBody::Json(bytes) => axum::body::Body::from(bytes),
            XrpcBody::Bytes(stream) => axum::body::Body::from_stream(stream),
        };
        let response = self
            .router
            .clone()
            .oneshot(builder.body(body).map_err(|error| failed(&error))?)
            .await
            .unwrap_or_else(|never| match never {});
        let (parts, body) = response.into_parts();
        Ok(XrpcResponse {
            status: parts.status,
            headers: parts.headers,
            body: XrpcBody::json(
                axum::body::to_bytes(body, MAX_ANSWER_OCTETS)
                    .await
                    .map_err(|error| failed(&error))?,
            ),
        })
    }
}

/// Creates the node if `dir` holds none (its identity seed goes to the keyring), makes sure it
/// attaches the peers service (a phone that is never dialed by a laptop can't sync), and records
/// `bootstrap`. The update migrates a version 1 node first, so its seed reaches the keyring
/// before `node.json` is rewritten without it.
async fn prepare_settings(dir: &NodeDir, bootstrap: &[String]) -> Res<()> {
    if !NodeSettings::exists(dir.root()) {
        dir.init().await?;
    }
    dir.update_settings(|settings| {
        let peers = settings.peers.get_or_insert_with(default);
        for ticket in bootstrap {
            peers.add_bootstrap(ticket)?;
        }
        Ok(())
    })
    .await
}

/// The node's service clients and the identities the composition acts with, cloned out of the
/// booted node. The handles follower runs on its own task, so it works on these and not on a
/// borrow of the node; [`NodeComposition::stop`] stops it before the node, so no client outlives
/// the services it calls.
#[derive(Clone)]
struct Minting {
    authority: Client<AuthorityProtocol>,
    /// The node's root capability: the operator every handle is minted as.
    root: CapId,
    dispatch: Client<DispatchProtocol>,
    docs: Client<DocsProtocol>,
    drawers: Client<DrawersProtocol>,
    blobs: Client<BlobsProtocol>,
    nodules: Client<NodulesProtocol>,
    cartridges: Client<CartridgesProtocol>,
    /// The drawers actor, which holds the known-drawers registry (see [`mirror_drawers`]).
    drawers_actor: CapId,
    known_drawers: DrawerId,
}

impl Minting {
    fn of(node: &big_node::BigNode) -> Self {
        Self {
            authority: node.authority.client.clone(),
            root: node.authority.root,
            dispatch: node.dispatch.client.clone(),
            docs: node.docs.client.clone(),
            drawers: node.drawers.client.clone(),
            blobs: node.blobs.client.clone(),
            nodules: node.nodules.client.clone(),
            cartridges: node.cartridges.client.clone(),
            drawers_actor: node.drawers_actor.capability,
            known_drawers: node.drawers.local().known_drawers_id,
        }
    }

    fn operator(&self) -> Authorization {
        Authorization::identity(self.root)
    }
}

async fn install_all(
    minting: &Minting,
    installs: Vec<MicroInstall>,
) -> Res<BTreeMap<String, Installed>> {
    let mut installed = BTreeMap::new();
    for install in installs {
        let micro = install.micro.clone();
        installed.insert(micro, install_one(minting, install).await?);
    }
    Ok(installed)
}

/// Opens the micro's actor below the `micros` subsystem actor, mints its egress and, when it has
/// drawers, its document capability.
async fn install_one(minting: &Minting, install: MicroInstall) -> Res<Installed> {
    let MicroInstall {
        micro,
        methods,
        drawers,
        config,
    } = install;
    let actor = open_micro_actor(minting, &micro).await?;
    let egress = MicroEgress::install(
        minting.dispatch.clone(),
        minting.operator(),
        &actor,
        DispatchScope {
            operations: methods,
            ..DispatchScope::default()
        },
    )
    .await
    .wrap_err_with(|| format!("minting the capability of micro {micro}"))?;
    let mirrored: Vec<DrawerId> = drawers.iter().map(|(drawer, _)| drawer.clone()).collect();
    mirror_drawers(minting, &mirrored, &micro).await?;
    let doc_capability = if drawers.is_empty() {
        None
    } else {
        let scope = DocScope {
            drawers: drawers
                .into_iter()
                .map(|(drawer, access)| (drawer, DrawerReach::all_kinds(access)))
                .collect(),
            ..DocScope::default()
        };
        Some(mint_docs(minting, &actor, scope).await?)
    };
    // INTERIM (until the ADR 049 §6.2 domain handle): a micro whose config names blob drawers is
    // minted a blobs capability bound to its actor that reaches those drawers at `Read`. The
    // blobs node checks a drawer reach at read time against the drawer's blob inventory, so the
    // blobs a micro reads (an intxt's patches) need not exist yet when it is installed. A blob
    // drawer the micro is not also granted is an install table error: its documents would name
    // blobs it can read but never the documents that say what they are. The capability is
    // written into the config as `blobCap`; the install file never decides it, as for `docCap`.
    let blob_capability = match config
        .as_ref()
        .and_then(|config| config.blob_drawers.as_ref())
    {
        Some(blob_drawers) => {
            for drawer in blob_drawers {
                assert!(
                    mirrored.contains(drawer),
                    "micro {micro}'s blob drawer {drawer} is not among its granted drawers"
                );
            }
            let scope = BlobsScope {
                drawers: blob_drawers
                    .iter()
                    .map(|drawer| (drawer.clone(), DrawerAccess::Read))
                    .collect(),
                ..BlobsScope::default()
            };
            Some(
                minting
                    .blobs
                    .call_with(
                        RequestContext::root(),
                        minting.operator().metadata(),
                        MintBlobsCapability {
                            capability: None,
                            scope,
                            rights: Rights::USE,
                            subject: Some(actor.actor.actor_id),
                        },
                    )
                    .await
                    .wrap_err_with(|| format!("minting the blobs capability of micro {micro}"))?
                    .capability_id,
            )
        }
        None => None,
    };
    let config = config.map(
        |InstallConfig {
             tag,
             key,
             mut value,
             blob_drawers: _,
         }| {
            if let Some(capability) = doc_capability {
                value.insert(
                    "docCap".to_owned(),
                    serde_json::to_value(capability).expect("a capability id is JSON"),
                );
            }
            if let Some(capability) = blob_capability {
                value.insert(
                    "blobCap".to_owned(),
                    serde_json::to_value(capability).expect("a capability id is JSON"),
                );
            }
            (tag, key, serde_json::Value::Object(value))
        },
    );
    Ok(Installed {
        egress: Arc::new(egress),
        config,
        state: Arc::new(MintedState::new(doc_capability)),
        revision: None,
    })
}

/// Remembers each of `drawers` as mirrored on this node. The micro reads its drawers through this
/// node, and a node fetches from its peers only the drawers it mirrors: `big_node`'s peer policy
/// wants only the `mirror` entries of the node's own known-drawers list, and another node's read
/// grant makes a drawer available to this one without making it wanted. So every drawer granted to
/// a micro is remembered here, mirrored, or the micro's reads stay empty forever. Remembering a
/// remembered drawer changes nothing.
async fn mirror_drawers(minting: &Minting, drawers: &[DrawerId], micro: &str) -> Res<()> {
    if drawers.is_empty() {
        return Ok(());
    }
    let registry = minting
        .drawers
        .call_with(
            RequestContext::root(),
            Authorization::identity(minting.drawers_actor).metadata(),
            MintDrawerCapability {
                capability: None,
                scope: DrawerScope {
                    drawers: BTreeMap::from([(minting.known_drawers.clone(), DrawerAccess::Admin)]),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .wrap_err("minting the known-drawers capability")?
        .capability_id;
    for drawer in drawers {
        minting
            .drawers
            .call(
                RequestContext::root(),
                RememberDrawer {
                    capability: registry,
                    drawer_id: drawer.clone(),
                    mirror: true,
                },
            )
            .await
            .wrap_err_with(|| format!("mirroring drawer {drawer} for micro {micro}"))?;
    }
    Ok(())
}

/// Opens the micro's actor below the `micros` subsystem actor, which is opened first. A subsystem
/// actor holds every right, as `big_node` opens `nodules`; a micro's own actor can only be used.
/// Opening an open actor is idempotent.
async fn open_micro_actor(minting: &Minting, micro: &str) -> Res<OpenedActor> {
    let open = |parent, path: String, rights| {
        minting.authority.call(
            RequestContext::root(),
            OpenActor {
                parent,
                path,
                rights,
            },
        )
    };
    let micros = open(minting.root, "micros".into(), Rights::all())
        .await
        .wrap_err("opening the micros subsystem actor")?;
    open(
        micros.capability,
        format!("micros/{}", actor_segment(micro)),
        Rights::USE,
    )
    .await
    .wrap_err_with(|| format!("opening the actor of micro {micro}"))
}

/// Mints a document capability for `scope`, as the node's operator, bound to `actor`.
async fn mint_docs(minting: &Minting, actor: &OpenedActor, scope: DocScope) -> Res<CapId> {
    Ok(minting
        .docs
        .call_with(
            RequestContext::root(),
            minting.operator().metadata(),
            MintDocCapability {
                capability: None,
                scope,
                rights: Rights::USE,
                subject: Some(actor.actor.actor_id),
            },
        )
        .await
        .wrap_err("minting a document capability")?
        .capability_id)
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
