//! The N-node arena (ADR 033 section 9): several [`BigNode`]s in one process, each on its own
//! bindings, met only through their peers attachments and driven only through their service
//! clients. Scenarios are written against [`Cluster`] in user terms (a drawer the nodes share, a
//! note, a blob) and wait on conditions, not on time.
//!
//! Reuse decision: `big_repo`'s `test2` topology helpers (`Pair`, `Topo`, `Node`) boot a bare
//! `BigRepo` with hand-wired Iroh endpoints, which is exactly the layer the peers attachment
//! replaced, and they are `pub(crate)` to a crate whose dev-only module would have to be exported
//! through a feature to be shared. The only piece that is layer-independent is the peer nickname
//! registry (tens of lines), which is copied here as [`Nicknames`] rather than widening
//! `big_repo`'s public API. The topology itself is just an edge list ([`Topology`]).

use super::blob_discovery::collection;
use super::blob_discovery::invite;
use super::blobs::blob_cap;
use super::blobs::inventory;
use super::*;
use crate::peers::PeersConfig;
use big_blobs::BlobService;
use big_blobs::InventorySelection;
use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_blobs_core::PutBlob;
use big_blobs_core::ReadBlob;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::BranchDoc;
use big_cabinet_core::doc::MergeDoc;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::drawer::protocol::RememberDrawer;
use big_sync_core::PeerKey;
use service_utils_rs::ClientError;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::broadcast;

/// The inventory name every shared drawer's blobs are published under.
///
/// It is the sensitive root's plaintext inventory: what is published and mirrored under it is the
/// blob itself, addressed by its own digest. The public root's inventory distributes ciphertext
/// whose digest is not the blob's.
const BLOBS: &str = "plaintext";

/// Which nodes dial which. Edges are `(dialer, listener)` indexes into the cluster's nodes.
#[derive(Debug, Clone, Copy)]
pub(super) enum Topology {
    /// Node 0 dials node 1.
    Pair,
    /// Every pair, the lower index dialing.
    Mesh,
}

impl Topology {
    fn edges(self, nodes: usize) -> Vec<(usize, usize)> {
        match self {
            Self::Pair => {
                assert_eq!(nodes, 2, "a pair is two nodes");
                vec![(0, 1)]
            }
            Self::Mesh => (0..nodes)
                .flat_map(|from| (from + 1..nodes).map(move |to| (from, to)))
                .collect(),
        }
    }
}

/// Peer key to nickname, so a failure across interleaved logs names `alice` and not hex.
#[derive(Default)]
struct Nicknames(BTreeMap<[u8; 32], String>);

impl Nicknames {
    fn register(&mut self, peer: &PeerKey, nickname: &str) {
        let bytes = peer.to_bytes32().unwrap();
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        tracing::info!(nickname, peer = %hex, "node nickname registered");
        self.0.insert(bytes, nickname.to_owned());
    }
}

struct Running {
    node: BigNode,
    stop: StopToken,
    /// Tasks that wake [`Cluster::until`] when the node reports a peers event or its datum index
    /// moves. They are aborted before the node stops.
    pumps: Vec<tokio::task::JoinHandle<()>>,
}

struct Member {
    nickname: &'static str,
    seed: u8,
    secrets: Arc<SecretStore>,
    home: PathBuf,
    running: Option<Running>,
}

/// A drawer the cluster's nodes share and mirror.
#[derive(Debug, Clone)]
pub(super) struct SharedDrawer {
    pub id: DocId,
    inventory: DocId,
}

/// A document as one node reads it.
pub(super) struct Note(DatumsRead);

impl Note {
    pub(super) fn text(&self, field: &str) -> Option<String> {
        match self.0.datums.get(&field_key(field))? {
            DatumRead::Accepted { value, .. } => Some(value["value"].as_str().unwrap().to_owned()),
            DatumRead::Absent { .. } => None,
            other => panic!("datum {field} is not settled: {other:?}"),
        }
    }
}

fn field_key(field: &str) -> DatumKey {
    format!("{NOTE}/{field}").parse().unwrap()
}

pub(super) struct Cluster {
    _directory: tempfile::TempDir,
    runtime: Arc<SqliteRuntime>,
    members: Vec<Member>,
    /// The topology's edges: the links [`Cluster::reconnect`] restores.
    edges: Vec<(usize, usize)>,
    /// The edges that are linked now. Stopping a node drops its links, and a restarted node is
    /// linked to no one until [`Cluster::reconnect`], so a restart without one partitions it.
    links: BTreeSet<(usize, usize)>,
    nicknames: Nicknames,
    commands: AtomicU64,
    /// Rung by every node's pumps. One waiter, so a ring that finds nobody waiting is kept as a
    /// permit and the next wait returns at once: a change between a check and the wait is not lost.
    wake: Arc<tokio::sync::Notify>,
}

impl Cluster {
    /// Boots one durable node per nickname, each in a directory of its own, unconnected.
    pub(super) async fn boot(nicknames: &[&'static str]) -> Self {
        utils_rs::testing::setup_tracing_once();
        let directory = tempfile::tempdir().unwrap();
        let runtime = Arc::new(SqliteRuntime::new(Default::default()));
        let mut cluster = Self {
            members: Vec::new(),
            edges: Vec::new(),
            links: BTreeSet::new(),
            nicknames: Nicknames::default(),
            commands: AtomicU64::new(0),
            wake: Arc::default(),
            _directory: directory,
            runtime,
        };
        for (index, nickname) in nicknames.iter().enumerate() {
            cluster.members.push(Member {
                nickname,
                seed: 100 + u8::try_from(index).unwrap(),
                secrets: secrets().await,
                home: cluster._directory.path().join(nickname),
                running: None,
            });
            cluster.start(index).await;
        }
        cluster
    }

    async fn start(&mut self, index: usize) {
        let member = &mut self.members[index];
        let bindings = Bindings::in_directory(&self.runtime, &member.home, None)
            .await
            .unwrap();
        let mut cfg = config(Arc::clone(&member.secrets), bindings.sql, bindings.repo);
        cfg.blob_storage = bindings.blobs;
        cfg.repo.node_identity_seed = [member.seed; 32];
        cfg.repo.scope_key = member.nickname.into();
        cfg.peers = Some(PeersConfig {
            loopback_only: true,
            // Only re-reads the dial targets; a peer's parts follow keyhive access changes.
            sweep_interval: std::time::Duration::from_millis(200),
            ..Default::default()
        });
        let (node, stop) = BigNode::boot(cfg).await.unwrap();
        self.nicknames
            .register(&node.repo.local_peer_id(), member.nickname);
        // Two signals that a condition may have become true: a peers event (a connection, a synced
        // document), and the node's datum index advancing, which happens after a synced document
        // is materialized and so readable; the sync event alone arrives before that.
        let mut events = node.peers.as_ref().unwrap().subscribe();
        let wake = Arc::clone(&self.wake);
        let events = tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => wake.notify_one(),
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        let index = Arc::clone(&node.indexes.datum_set);
        let from = index.latest_revision().await.unwrap();
        let wake = Arc::clone(&self.wake);
        let index = tokio::spawn(async move {
            let mut reader = index.open_all(from).await.unwrap();
            loop {
                reader.next(RevisionReadLimits::default()).await.unwrap();
                wake.notify_one();
            }
        });
        member.running = Some(Running {
            node,
            stop,
            pumps: vec![events, index],
        });
    }

    pub(super) fn node(&self, index: usize) -> &BigNode {
        &self.members[index]
            .running
            .as_ref()
            .unwrap_or_else(|| panic!("{} is stopped", self.members[index].nickname))
            .node
    }

    /// The directory node `index` keeps all of its durable state in.
    pub(super) fn home(&self, index: usize) -> &std::path::Path {
        &self.members[index].home
    }

    pub(super) fn peer(&self, index: usize) -> PeerKey {
        self.node(index).repo.local_peer_id()
    }

    /// Links the topology's edges through the peers attachments and runs a keyhive round across
    /// each, so each side holds the other's membership changes.
    pub(super) async fn connect(&mut self, topology: Topology) {
        for (from, to) in topology.edges(self.members.len()) {
            self.edges.push((from, to));
            tracing::info!(from, to, "INSTR connect link start"); // INSTR
            self.link(from, to).await;
            tracing::info!(from, to, "INSTR connect link done"); // INSTR
        }
    }

    async fn link(&mut self, from: usize, to: usize) {
        let link = network::link(self.node(from), self.node(to)).await;
        link.sync_keyhive(self.node(from), self.node(to)).await;
        self.links.insert((from, to));
    }

    /// Runs a keyhive round across every live link, so a membership change made on one node is
    /// held by every node it can reach in one hop.
    async fn sync_keyhive(&self) {
        for (from, to) in &self.links {
            let link = network::Link {
                source: self.peer(*from),
                puller: self.peer(*to),
            };
            link.sync_keyhive(self.node(*from), self.node(*to)).await;
        }
    }

    pub(super) fn is_running(&self, index: usize) -> bool {
        self.members[index].running.is_some()
    }

    /// Whether node `index` is missing a link of the topology to a running node.
    pub(super) fn is_partitioned(&self, index: usize) -> bool {
        self.edges.iter().any(|edge| {
            let other = match *edge {
                (from, to) if from == index => to,
                (from, to) if to == index => from,
                _ => return false,
            };
            self.is_running(other) && !self.links.contains(edge)
        })
    }

    /// Stops node `index`. Its data stays in its directory.
    pub(super) async fn stop(&mut self, index: usize) {
        self.links
            .retain(|(from, to)| *from != index && *to != index);
        let Running { node, stop, pumps } = self.members[index].running.take().unwrap();
        for pump in pumps {
            pump.abort();
            // Aborted is expected; a pump that died of its own accord is a failure.
            if let Err(error) = pump.await {
                assert!(error.is_cancelled(), "a wake pump failed: {error}");
            }
        }
        shutdown(node, stop).await;
    }

    /// Boots a stopped node again from its bindings. It is not connected to anyone until
    /// [`Cluster::reconnect`].
    pub(super) async fn restart(&mut self, index: usize) {
        assert!(self.members[index].running.is_none(), "stop it first");
        self.start(index).await;
    }

    /// Links the edges node `index` is on to the nodes that are running, where they are not
    /// linked already.
    pub(super) async fn reconnect(&mut self, index: usize) {
        for (from, to) in self.edges.clone() {
            if ((from == index && self.is_running(to)) || (to == index && self.is_running(from)))
                && !self.links.contains(&(from, to))
            {
                self.link(from, to).await;
            }
        }
    }

    pub(super) async fn stop_all(&mut self) {
        for index in (0..self.members.len()).rev() {
            if self.is_running(index) {
                self.stop(index).await;
            }
        }
    }

    /// `owner` creates a drawer, with a blob inventory, and gives `members` write access. Every
    /// node mirrors it. Safe to call before or after [`Cluster::connect`].
    pub(super) async fn share_drawer(&self, owner: usize, members: &[usize]) -> SharedDrawer {
        let node = self.node(owner);
        let id = collection(node, "shared").await;
        let inventory = inventory(node, &id, MetaRoot::Sensitive, BLOBS).await;
        self.remember(owner, &id).await;
        for member in members.iter().filter(|member| **member != owner) {
            invite(node, self.node(*member), &id, Access::Write).await;
            self.remember(*member, &id).await;
        }
        self.sync_keyhive().await;
        tracing::info!(%id, %inventory, "INSTR share_drawer done"); // INSTR
        SharedDrawer { id, inventory }
    }

    /// `owner` gives `member` `access` to `drawer`, which `member` then mirrors. Nodes that are
    /// not linked to the grant now learn it when they are next linked.
    pub(super) async fn admit(&self, owner: usize, member: usize, drawer: &DocId, access: Access) {
        invite(self.node(owner), self.node(member), drawer, access).await;
        self.remember(member, drawer).await;
        self.sync_keyhive().await;
    }

    async fn remember(&self, index: usize, drawer: &DocId) {
        let node = self.node(index);
        let capability = node
            .drawers
            .client
            .call_with(
                RequestContext::root(),
                Authorization::identity(node.drawers_actor.capability).metadata(),
                MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: BTreeMap::from([(
                            node.drawers.local().known_drawers_id,
                            Access::Admin,
                        )]),
                        create: false,
                    },
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap()
            .capability_id;
        node.drawers
            .client
            .call(
                RequestContext::root(),
                RememberDrawer {
                    capability,
                    drawer_id: drawer.clone(),
                    mirror: true,
                },
            )
            .await
            .unwrap();
    }

    fn command(&self, what: &str) -> CommandId {
        let ordinal = self.commands.fetch_add(1, Ordering::Relaxed);
        CommandId::try_from(format!("{what}-{ordinal}")).unwrap()
    }

    /// A note in `drawer`, created by node `index`, with `text` in its `main` field.
    pub(super) async fn create_note(
        &self,
        index: usize,
        drawer: &SharedDrawer,
        text: &str,
    ) -> DocId {
        self.try_create_note(index, drawer, text)
            .await
            .unwrap_or_else(|| {
                panic!(
                    "{} was refused a note in {}",
                    self.members[index].nickname, drawer.id
                )
            })
    }

    /// [`Cluster::create_note`], or `None` when node `index` is refused: a node admitted to a
    /// drawer is refused creating in it until the drawer's records have reached it.
    pub(super) async fn try_create_note(
        &self,
        index: usize,
        drawer: &SharedDrawer,
        text: &str,
    ) -> Option<DocId> {
        let node = self.node(index);
        let placement = format!("drawer {}", drawer.id);
        let capability = capability(
            node,
            DocScope {
                create: BTreeSet::from([placement.clone()]),
                ..DocScope::default()
            },
        )
        .await;
        match node
            .docs
            .client
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability,
                    command: self.command("create-note"),
                    placement,
                    datums: BTreeMap::from([(
                        field_key("main"),
                        json!({"$type": NOTE, "value": text}),
                    )]),
                },
            )
            .await
        {
            Ok(receipt) => Some(receipt.doc_id),
            Err(ClientError::Operation(big_cabinet_core::doc::DocsError::Denied)) => None,
            Err(error) => panic!(
                "creating a note on {}: {error:?}",
                self.members[index].nickname
            ),
        }
    }

    /// `None` while the node cannot read the document yet: it does not have it, or has it but has
    /// not materialized it (a restarted node loads documents lazily).
    pub(super) async fn read_note(&self, index: usize, doc: &DocId) -> Option<Note> {
        let node = self.node(index);
        let capability = capability(
            node,
            DocScope {
                read: BTreeSet::from([doc.clone()]),
                ..DocScope::default()
            },
        )
        .await;
        match node
            .docs
            .client
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability,
                    doc_id: doc.clone(),
                    at: At::Current,
                    datums: None,
                    optimistic: false,
                },
            )
            .await
        {
            Ok(read) => Some(Note(read)),
            Err(ClientError::Operation(
                big_cabinet_core::doc::DocsError::DocNotFound
                | big_cabinet_core::doc::DocsError::Pending { .. },
            )) => None,
            Err(error) => panic!(
                "reading {doc} on {}: {error:?}",
                self.members[index].nickname
            ),
        }
    }

    /// Writes `field` on top of what node `index` currently has.
    pub(super) async fn update_note(&self, index: usize, doc: &DocId, field: &str, text: &str) {
        self.write(index, doc, WriteMode::AtCurrent, field, text)
            .await;
    }

    /// Writes `field` on top of the state `base` read, so it is concurrent with every write made
    /// since, wherever it was made.
    pub(super) async fn update_note_from(
        &self,
        index: usize,
        doc: &DocId,
        base: &Note,
        field: &str,
        text: &str,
    ) {
        let mode = WriteMode::AtHeads {
            heads: base.0.heads.clone(),
        };
        self.write(index, doc, mode, field, text).await;
    }

    async fn write(&self, index: usize, doc: &DocId, mode: WriteMode, field: &str, text: &str) {
        let node = self.node(index);
        let capability = capability(
            node,
            DocScope {
                write: BTreeSet::from([doc.clone()]),
                ..DocScope::default()
            },
        )
        .await;
        node.docs
            .client
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability,
                    command: Some(self.command("update-note")),
                    doc_id: doc.clone(),
                    mode,
                    set: BTreeMap::from([(
                        field_key(field),
                        json!({"$type": NOTE, "value": text}),
                    )]),
                    remove: BTreeSet::new(),
                },
            )
            .await
            .unwrap();
    }

    /// A branch of `doc` at node `index`'s current state, in `drawer`.
    pub(super) async fn branch_note(
        &self,
        index: usize,
        doc: &DocId,
        drawer: &SharedDrawer,
        path: &str,
    ) -> DocId {
        let node = self.node(index);
        let placement = format!("drawer {}", drawer.id);
        let capability = capability(
            node,
            DocScope {
                read: BTreeSet::from([doc.clone()]),
                // A branch is announced on its origin, so it takes write access there too.
                write: BTreeSet::from([doc.clone()]),
                create: BTreeSet::from([placement.clone()]),
                ..DocScope::default()
            },
        )
        .await;
        node.docs
            .client
            .call(
                RequestContext::root(),
                BranchDoc {
                    capability,
                    command: self.command("branch-note"),
                    doc_id: doc.clone(),
                    at: At::Current,
                    path: path.to_owned(),
                    stealth: false,
                    placement,
                },
            )
            .await
            .unwrap()
            .doc_id
    }

    /// Merges node `index`'s current `source` into `target`.
    pub(super) async fn merge_note(&self, index: usize, source: &DocId, target: &DocId) {
        let node = self.node(index);
        let capability = capability(
            node,
            DocScope {
                read: BTreeSet::from([source.clone()]),
                write: BTreeSet::from([target.clone()]),
                ..DocScope::default()
            },
        )
        .await;
        node.docs
            .client
            .call(
                RequestContext::root(),
                MergeDoc {
                    capability,
                    command: Some(self.command("merge-note")),
                    source_id: source.clone(),
                    at: At::Current,
                    target_id: target.clone(),
                },
            )
            .await
            .unwrap();
    }

    /// Stores `bytes` on node `index` and publishes them in `drawer`'s blob inventory; answers
    /// once the node's workers have published them.
    pub(super) async fn add_blob(
        &self,
        index: usize,
        drawer: &SharedDrawer,
        bytes: &[u8],
    ) -> BlobId {
        let node = self.node(index);
        let put = blob_cap(node, BlobService::put_object().into(), Vec::new()).await;
        let info = node
            .blobs
            .client
            .call(
                RequestContext::root(),
                PutBlob {
                    capability: put,
                    bytes: bytes.to_vec(),
                },
            )
            .await
            .unwrap();
        let placement = format!("drawer {}", drawer.id);
        node.docs
            .client
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: capability(
                        node,
                        DocScope {
                            create: BTreeSet::from([placement.clone()]),
                            ..DocScope::default()
                        },
                    )
                    .await,
                    command: self.command("add-blob"),
                    placement,
                    datums: BTreeMap::from([(
                        DatumKey::from_text(Blob::ID, "main").unwrap(),
                        json!({
                            "mime": "application/octet-stream",
                            "digest": info.blob_id.digest_str(),
                            "lengthOctets": info.length_octets,
                        }),
                    )]),
                },
            )
            .await
            .unwrap();
        self.wait_blob_published(index, drawer, info.blob_id).await;
        info.blob_id
    }

    /// Waits until node `index` holds `blob`'s inventory part, which is when the bytes are
    /// published by the node that has them and mirrored by the nodes that selected them.
    async fn wait_blob_published(&self, index: usize, drawer: &SharedDrawer, blob: BlobId) {
        let node = self.node(index);
        let mut changes = node
            .blob_workers
            .parts
            .open_revision_reader_all(0)
            .await
            .unwrap()
            .unwrap();
        tracing::info!(node = self.members[index].nickname, drawer = %drawer.id, inventory = %drawer.inventory, %blob, "INSTR wait_blob_published start"); // INSTR
        while !node
            .blob_workers
            .parts
            .obj_parts(blob.into())
            .await
            .unwrap()
            .contains(&big_blobs::workers::inventory_part(&drawer.inventory))
        {
            let parts = node
                .blob_workers
                .parts
                .obj_parts(blob.into())
                .await
                .unwrap(); // INSTR
            tracing::info!(node = self.members[index].nickname, %blob, parts = parts.len(), "INSTR wait_blob_published not yet"); // INSTR
            loop {
                // INSTR
                tokio::select! { // INSTR
                    next = changes.next(RevisionReadLimits::default()) => { next.unwrap(); break; } // INSTR
                    () = tokio::time::sleep(std::time::Duration::from_secs(15)) => { // INSTR
                        let parts = node.blob_workers.parts.obj_parts(blob.into()).await.unwrap(); // INSTR
                        tracing::info!(node = self.members[index].nickname, %blob, parts = ?parts, peers = ?node.peers.as_ref().unwrap().connected().await.unwrap(), "INSTR wait_blob_published still waiting"); // INSTR
                    } // INSTR
                } // INSTR
            } // INSTR
        }
        tracing::info!(node = self.members[index].nickname, %blob, "INSTR wait_blob_published done"); // INSTR
    }

    /// Node `index` takes part in the drawer's blob inventory: it asks to mirror it.
    pub(super) async fn mirror_blobs(&self, index: usize, drawer: &DocId) {
        self.node(index)
            .blob_workers
            .mirrors
            .select_drawer(
                drawer.clone(),
                Some(InventorySelection {
                    root: MetaRoot::Sensitive,
                    name: BLOBS.into(),
                }),
            )
            .await
            .unwrap();
    }

    /// Reads all of `blob` from node `index` through its blob client. `None` while it is not
    /// stored there.
    pub(super) async fn read_blob(&self, index: usize, blob: BlobId) -> Option<Vec<u8>> {
        let node = self.node(index);
        let put = blob_cap(node, BlobService::put_object().into(), Vec::new()).await;
        let read = blob_cap(node, BlobService::read_object(&blob), vec![put]).await;
        let stat = node
            .blobs
            .client
            .call(
                RequestContext::root(),
                big_blobs_core::StatBlob {
                    capability: read,
                    blob_id: blob,
                },
            )
            .await;
        let length = match stat {
            Ok(info) => info.expect("stored blob has info").length_octets,
            Err(ClientError::Operation(big_blobs_core::BlobsError::NotFound)) => return None,
            Err(error) => panic!("stat {blob} on {}: {error:?}", self.members[index].nickname),
        };
        let range = node
            .blobs
            .client
            .call(
                RequestContext::root(),
                ReadBlob {
                    capability: read,
                    blob_id: blob,
                    offset: 0,
                    length: u32::try_from(length).unwrap(),
                },
            )
            .await
            .unwrap();
        Some(range.bytes)
    }

    /// Waits until `condition` holds, re-checking it whenever a node reports a peers event or
    /// its datum index advances. It is checked first, so a condition that already holds returns
    /// at once. There are no sleeps: a condition nothing will ever satisfy runs into nextest's
    /// hard timeout.
    pub(super) async fn until(&self, condition: impl AsyncFn(&Self) -> bool) {
        while !condition(self).await {
            self.wake.notified().await;
        }
    }

    /// Waits until every running node reads `doc` with the same heads and returns that state's
    /// notes in node order.
    pub(super) async fn converged(&self, doc: &DocId) -> Vec<Note> {
        tracing::info!(%doc, "STEP awaiting convergence");
        self.until(async |cluster| {
            let mut heads = Vec::new();
            for index in cluster.running() {
                let Some(note) = cluster.read_note(index, doc).await else {
                    return false;
                };
                heads.push(note.0.heads);
            }
            heads.windows(2).all(|pair| pair[0] == pair[1])
        })
        .await;
        let mut notes = Vec::new();
        for index in self.running() {
            notes.push(self.read_note(index, doc).await.unwrap());
        }
        notes
    }

    pub(super) fn running(&self) -> Vec<usize> {
        (0..self.members.len())
            .filter(|index| self.is_running(*index))
            .collect()
    }

    /// Waits until node `index` can read `doc` with `field` equal to `text`.
    pub(super) async fn note_is(&self, index: usize, doc: &DocId, field: &str, text: &str) {
        tracing::info!(node = self.members[index].nickname, %doc, field, text, "STEP awaiting note");
        self.until(async |cluster| {
            cluster
                .read_note(index, doc)
                .await
                .and_then(|note| note.text(field))
                .as_deref()
                == Some(text)
        })
        .await;
    }

    /// Waits until node `index` has mirrored `blob` and serves it, and returns its bytes.
    pub(super) async fn blob_arrives(
        &self,
        index: usize,
        drawer: &SharedDrawer,
        blob: BlobId,
    ) -> Vec<u8> {
        tracing::info!(node = self.members[index].nickname, %blob, "STEP awaiting blob");
        self.mirror_blobs(index, &drawer.id).await;
        self.wait_blob_published(index, drawer, blob).await;
        tracing::info!(node = self.members[index].nickname, %blob, "STEP blob part present; awaiting coverage");
        let mirrors = &self.node(index).blob_workers.mirrors;
        let mut changes = mirrors.subscribe_changes();
        loop {
            changes.borrow_and_update();
            let coverage = mirrors.coverage(&drawer.id).await.unwrap();
            if coverage.is_complete() {
                break;
            }
            tracing::info!(node = self.members[index].nickname, gaps = ?coverage.gaps, "INSTR blob_arrives coverage gaps"); // INSTR
            loop {
                // INSTR
                tokio::select! { // INSTR
                    changed = changes.changed() => { changed.unwrap(); tracing::info!(node = self.members[index].nickname, "INSTR blob_arrives coverage woke"); break; } // INSTR
                    () = tokio::time::sleep(std::time::Duration::from_secs(15)) => tracing::info!(node = self.members[index].nickname, "INSTR blob_arrives still waiting for coverage change"), // INSTR
                } // INSTR
            } // INSTR
        }
        self.read_blob(index, blob)
            .await
            .expect("a complete mirror stores the blob")
    }
}
