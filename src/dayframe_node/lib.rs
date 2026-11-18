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
//! Micros come from two places. A micro a cartridge contributes is loaded from the node's catalog
//! and its handles are minted from its `uses` and the cartridge's grants ([`cartridge`]). A micro
//! that is in no cartridge keeps the interim path below ([`MicroInstall`]) until it is published
//! as one.
//!
//! The node runs on a runtime of its own. Its tasks (the peers endpoint, dispatch, indexing) must
//! not share a host's UI loop, and the host's runtime flavor is not the node's business.

use big_blobs_core::MintBlobsCapability;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DrawerReach;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_dispatch_core::DispatchScope;
use big_nodule::RoutineScopes;
use big_nodule::executor::RoutineCapabilities;
use clay_guest_session::egress::DispatchEgress;
use clay_node_egress::MicroEgress;
use day_node::HeldNode;
use day_node::NodeSettings;
use service_client_core_rs::xrpc::HttpTransport;
use service_client_core_rs::xrpc::HttpTransportError;
use service_client_core_rs::xrpc::XrpcBody;
use service_client_core_rs::xrpc::XrpcRequest;
use service_client_core_rs::xrpc::XrpcResponse;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tower::ServiceExt;
use utils_rs::prelude::ERROR_TOKIO;
use utils_rs::prelude::Res;
use utils_rs::prelude::WrapErr;
use utils_rs::prelude::default;
use utils_rs::prelude::ferr;

mod cartridge;
#[cfg(test)]
mod e2e;

pub use cartridge::CartridgeMicro;

/// One micro the composition installs, and what node access it is granted.
///
/// INTERIM, for micros that are in no cartridge only. A micro a cartridge contributes is minted
/// from its `uses` and the cartridge's grants ([`NodeComposition::sync_cartridge_micros`]), and
/// this table never applies to it. Until `dayframe.review` is published as a cartridge, the
/// composition names the granted NSIDs and drawers of each such micro here, and this table is the
/// only place that does; empty grants nothing.
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
}

/// What one micro is granted by its plug name (the `<name>` of `<did>/<name>`), and the install
/// file that configures it.
///
/// INTERIM: see [`MicroInstall`]. This table is the one place the composition names a micro's node
/// access. A micro not listed here still gets an actor and an egress, but can call no method.
struct InterimGrant {
    name: &'static str,
    /// The node methods the micro may call.
    methods: &'static [&'static str],
    /// The micro's install file, when it has a configuration datum an operator writes.
    config: Option<InterimConfig>,
}

/// The configuration datum of an [`InterimGrant`]: the file an operator writes for it (a JSON
/// object, which becomes the datum's value), where the datum goes, and which of the file's
/// members name drawers. Each member in `drawers` is a drawer id or an array of them; the micro is
/// granted `Read` over every drawer named.
struct InterimConfig {
    file: &'static str,
    tag: &'static str,
    key: &'static str,
    drawers: &'static [&'static str],
}

const INTERIM_GRANTS: &[InterimGrant] = &[InterimGrant {
    // intxt's review micro (D62): the linked intxts' summaries, and the PR and intxt datums it reads
    // with the doc capability in its config.
    name: "dayframe.review",
    methods: &[
        "org.example.intxt.summarizeIntxts",
        "org.example.daybook.docs.readDatums",
        "org.example.daybook.docs.pollDatums",
    ],
    config: Some(InterimConfig {
        file: "review.json",
        tag: micro_review_manifest::CONFIG_TAG,
        key: micro_review_manifest::CONFIG_KEY,
        drawers: &["githubDrawer", "intxtDrawers"],
    }),
}];

impl MicroInstall {
    /// The installs of the micros with these ids, each granted its [`INTERIM_GRANTS`].
    ///
    /// `install_file` answers the text of an install file by its name (`review.json`), `None` when
    /// the operator has not written one. A micro whose file is absent installs unconfigured: no
    /// drawers, no config datum, and so no document capability (the review micro shows
    /// "Unconfigured"). A file that is not a JSON object, or names a drawer that is not a drawer
    /// id, is an error: the operator wrote it, and a half-read file would grant the wrong drawers.
    pub fn interim(
        micros: impl IntoIterator<Item = String>,
        install_file: impl Fn(&str) -> std::io::Result<Option<Vec<u8>>>,
    ) -> Res<Vec<Self>> {
        micros
            .into_iter()
            .map(|micro| {
                let name = micro
                    .split_once('/')
                    .unwrap_or_else(|| panic!("micro id {micro:?} is not `<did>/<name>`"))
                    .1;
                let granted = INTERIM_GRANTS.iter().find(|granted| granted.name == name);
                let (drawers, config) = match granted.and_then(|granted| granted.config.as_ref()) {
                    Some(config) => match install_file(config.file)
                        .wrap_err_with(|| format!("reading {}", config.file))?
                    {
                        Some(bytes) => {
                            let (drawers, config) = parse_install_file(config, &bytes)
                                .wrap_err_with(|| format!("reading {}", config.file))?;
                            (drawers, Some(config))
                        }
                        None => (Vec::new(), None),
                    },
                    None => (Vec::new(), None),
                };
                Ok(Self {
                    methods: granted
                        .into_iter()
                        .flat_map(|granted| granted.methods)
                        .map(|nsid| (*nsid).to_owned())
                        .collect(),
                    drawers,
                    config,
                    micro,
                })
            })
            .collect()
    }
}

/// The drawers (read) and the configuration datum `bytes` states for `config`.
fn parse_install_file(
    config: &InterimConfig,
    bytes: &[u8],
) -> Res<(Vec<(DrawerId, DrawerAccess)>, InstallConfig)> {
    let serde_json::Value::Object(value) = serde_json::from_slice(bytes)? else {
        return Err(ferr!("the file is not a JSON object"));
    };
    let mut drawers = Vec::new();
    for member in config.drawers {
        let listed: Vec<&str> = match value.get(*member) {
            None => continue,
            Some(serde_json::Value::String(id)) => vec![id.as_str()],
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .ok_or_else(|| ferr!("{member} holds something that is not a drawer id"))
                })
                .collect::<Res<_>>()?,
            Some(_) => return Err(ferr!("{member} is not a drawer id or an array of them")),
        };
        for id in listed {
            let drawer = id
                .parse::<DrawerId>()
                .map_err(|error| ferr!("{member}: {id:?} is not a drawer id: {error}"))?;
            if !drawers.iter().any(|(held, _)| *held == drawer) {
                drawers.push((drawer, DrawerAccess::Read));
            }
        }
    }
    Ok((
        drawers,
        InstallConfig {
            tag: config.tag.to_owned(),
            key: config.key.to_owned(),
            value,
        },
    ))
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
    /// The micro's document capability: over its configured drawers for an interim install, and
    /// the docs handle of its [`Minted`] scopes for a cartridge's micro. `None` for a micro with
    /// none.
    doc_capability: Option<CapId>,
    /// The install's configuration datum with `docCap` added: `(tag, key, value)`.
    config: Option<(String, String, serde_json::Value)>,
    /// What a cartridge's micro was minted for; `None` for an interim install.
    minted: Option<Minted>,
}

/// The scopes a cartridge's micro holds handles for, and the handles beyond dispatch's (which its
/// egress holds). A later sync compares the scopes with what its grants now lower to, and retracts
/// and mints again when they differ.
struct Minted {
    scopes: RoutineScopes,
    handles: RoutineCapabilities,
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
                    schemas,
                    installs,
                } = config;
                prepare_settings(&root, &bootstrap)?;
                let settings = NodeSettings::load(&root)?;
                // No listeners: the holder is its own only client, and reaches the node through
                // its dispatch client.
                let held = HeldNode::boot(&root, &settings, default(), true, schemas).await?;
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
        let installed = install_one(&self.held.node, install).await?;
        self.installed.insert(micro, installed);
        Ok(())
    }

    /// A transport that calls this node's methods as its operator, for typed clients
    /// (`service_client_core_rs`). Operator-only: it presents no capability and acts as the
    /// node's root authority, so it is for the host's own installs at boot and is never handed to
    /// a micro (a micro's only way out is its [`Self::egress`]).
    pub fn operator_transport(&self) -> OperatorTransport {
        let node = &self.held.node;
        OperatorTransport {
            router: big_dispatch::xrpc_router(
                node.dispatch.client.clone(),
                big_dispatch::Anonymous::Acts(Authorization::identity(node.authority.root)),
                big_dispatch::Edge::Embedded,
            )
            .into_router(),
        }
    }

    /// Installs intxt on this node from its handler component `guest_wasm` and enables it, as the
    /// node's operator, so the node admits intxt's datum Lexicons and serves its methods (the
    /// review micro's `summarizeIntxts`). Idempotent by cartridge version, so a host calls it at
    /// every boot.
    pub async fn install_intxt(
        &self,
        guest_wasm: &[u8],
    ) -> Result<intxt_core::install::Installed, intxt_core::methods::IntxtError> {
        let operator = intxt_core::client::Caller::new(
            self.operator_transport(),
            RequestContext::root(),
            None,
            "intxt-install".to_owned(),
        );
        intxt_core::install::install(&operator, guest_wasm).await
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
        config,
    } = install;
    let actor = open_micro_actor(node, &micro).await?;
    let minter = Authorization::identity(node.authority.root);
    let egress = MicroEgress::install(
        node.dispatch.client.clone(),
        minter,
        &actor,
        DispatchScope {
            operations: methods,
            ..DispatchScope::default()
        },
    )
    .await
    .wrap_err_with(|| format!("minting the capability of micro {micro}"))?;
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
        Some(mint_docs(node, &actor, scope).await?)
    };
    let config = config.map(
        |InstallConfig {
             tag,
             key,
             mut value,
         }| {
            if let Some(capability) = doc_capability {
                value.insert(
                    "docCap".to_owned(),
                    serde_json::to_value(capability).expect("a capability id is JSON"),
                );
            }
            (tag, key, serde_json::Value::Object(value))
        },
    );
    Ok(Installed {
        egress: Arc::new(egress),
        doc_capability,
        config,
        minted: None,
    })
}

/// Installs a cartridge's micro: its actor, and a handle for each service `scopes` reaches,
/// minted as the node's operator and bound to the micro's actor, so each is refused under any
/// other. The guest never holds them: the host presents dispatch's on every `dispatch.local` call
/// and names the others in the calls it makes for the micro (ADR 049 section 3, asymmetry 2).
///
/// With `previous`, the micro's earlier install, the egress the micro's incarnation holds stays
/// and presents the new dispatch capability from its next call, and every handle of the earlier
/// scopes is retracted (ADR 049 section 6.4).
async fn install_with(
    node: &big_node::BigNode,
    micro: &str,
    scopes: RoutineScopes,
    previous: Option<Installed>,
) -> Res<Installed> {
    let actor = open_micro_actor(node, micro).await?;
    let minter = Authorization::identity(node.authority.root);
    let dispatch = scopes.dispatch.clone().unwrap_or_default();
    let egress = match previous {
        Some(previous) => {
            let replaced = previous
                .egress
                .remint(minter, &actor, dispatch)
                .await
                .wrap_err_with(|| format!("minting the capability of micro {micro}"))?;
            for handle in replaced.into_iter().chain(previous.handle_ids()) {
                retract(node, handle).await?;
            }
            previous.egress
        }
        None => Arc::new(
            MicroEgress::install(node.dispatch.client.clone(), minter, &actor, dispatch)
                .await
                .wrap_err_with(|| format!("minting the capability of micro {micro}"))?,
        ),
    };
    let mut handles = RoutineCapabilities {
        dispatch: egress.capability(),
        ..RoutineCapabilities::default()
    };
    if let Some(scope) = scopes.docs.clone() {
        handles.docs = Some(mint_docs(node, &actor, scope).await?);
    }
    if let Some(scope) = scopes.drawers.clone() {
        handles.drawers = Some(
            node.drawers
                .client
                .call_with(
                    RequestContext::root(),
                    minter.metadata(),
                    MintDrawerCapability {
                        scope,
                        rights: Rights::USE,
                        subject: Some(actor.actor.actor_id),
                    },
                )
                .await
                .wrap_err_with(|| format!("minting the drawers capability of micro {micro}"))?
                .capability_id,
        );
    }
    if let Some(scope) = scopes.blobs.clone() {
        handles.blobs = Some(
            node.blobs
                .client
                .call_with(
                    RequestContext::root(),
                    minter.metadata(),
                    MintBlobsCapability {
                        scope,
                        rights: Rights::USE,
                        subject: Some(actor.actor.actor_id),
                    },
                )
                .await
                .wrap_err_with(|| format!("minting the blobs capability of micro {micro}"))?
                .capability_id,
        );
    }
    Ok(Installed {
        egress,
        doc_capability: handles.docs,
        config: None,
        minted: Some(Minted { scopes, handles }),
    })
}

/// Opens the micro's actor below the `micros` subsystem actor, which is opened first. A subsystem
/// actor holds every right, as `big_node` opens `nodules`; a micro's own actor can only be used.
/// Opening an open actor is idempotent.
async fn open_micro_actor(node: &big_node::BigNode, micro: &str) -> Res<OpenedActor> {
    let authority = &node.authority;
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
    let micros = open(authority.root, "micros".into(), Rights::all())
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
async fn mint_docs(node: &big_node::BigNode, actor: &OpenedActor, scope: DocScope) -> Res<CapId> {
    Ok(node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.authority.root).metadata(),
            MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: Some(actor.actor.actor_id),
            },
        )
        .await
        .wrap_err("minting a document capability")?
        .capability_id)
}

/// Retracts `capability`. One that is already dead is retracted.
async fn retract(node: &big_node::BigNode, capability: CapId) -> Res<()> {
    match node
        .authority
        .client
        .call(
            RequestContext::root(),
            RetractCapability {
                via: capability,
                capability,
            },
        )
        .await
    {
        Ok(()) | Err(ClientError::Method(AuthorityServiceError::InvalidCapability)) => Ok(()),
        Err(error) => Err(error).wrap_err("retracting a micro's capability"),
    }
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
