//! Tier 10 — randomized BigRepo stress with topology churn and relays.
//!
//! The shared stress runner owns topology selection. This fixture only creates
//! documents, performs mutations on nodes that are already ready, and checks
//! the resulting durable frontier after the runner reconnects the full mesh.

use super::harness::fixtures::wait_for_agent;
use super::harness::log_nickname;
use super::harness::topo::Node;
use crate::{BigKeyhiveGroup, DocumentId, PeerKey, Res, StorageConfig};
use am_utils_rs::codecs::ThroughJson;
use big_sync::{
    HostPartStore,
    stress_support::{self, StressFixture},
};
use big_sync_core::{ObjKey, PartKey};
use futures::future::try_join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use keyhive_core::access::Access;
use rand::rngs::StdRng;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tempfile::tempdir;
use tokio::sync::Mutex;

pub const DEFAULT_STRESS_SEED: u64 = 0xB1A0_5EED_5EED_0002;

/// How often a settle barrier that is still outstanding reports which nodes hold it.
const SETTLE_STALL_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// How many tracked documents one stall report names per node.
const SETTLE_STALL_REPORT_DOCS: usize = 6;

/// A key's display text, truncated to `limit` characters.
///
/// These diagnostics name ADR 012 keys, whose width is not fixed: a reserved textual key
/// renders as its own text (`/seds` is six bytes), so a fixed *byte* offset panics on any
/// key shorter than it. Truncating by characters bounds the same display for every key
/// and cannot panic.
fn key_prefix(key: &impl std::fmt::Display, limit: usize) -> String {
    key.to_string().chars().take(limit).collect()
}

#[derive(Clone)]
pub struct BigRepoStressConfig {
    pub node_count: usize,
    pub relay_idx: Option<usize>,
    pub seed: u64,
    pub peer_seed_offset: u8,
}

impl Default for BigRepoStressConfig {
    fn default() -> Self {
        Self {
            node_count: 4,
            relay_idx: None,
            seed: utils_rs::testing::test_seed(DEFAULT_STRESS_SEED),
            peer_seed_offset: 0,
        }
    }
}

pub(crate) struct BigRepoStressFixture {
    config: BigRepoStressConfig,
    shared_edit_groups: Arc<Mutex<HashMap<PeerKey, BigKeyhiveGroup>>>,
    shared_edit_group_id: Arc<Mutex<Option<keyhive_core::principal::group::id::GroupId>>>,
    editor_peer_ids: Arc<Mutex<BTreeSet<PeerKey>>>,
    relay_peer_ids: Arc<Mutex<BTreeSet<PeerKey>>>,
    obj_doc_map: Arc<Mutex<HashMap<ObjKey, DocumentId>>>,
    all_docs: Arc<Mutex<BTreeSet<DocumentId>>>,
    node_paths: Arc<Mutex<HashMap<PeerKey, PathBuf>>>,
}

impl BigRepoStressFixture {
    pub fn new(config: BigRepoStressConfig) -> Self {
        Self {
            config,
            shared_edit_groups: Arc::new(Mutex::new(HashMap::new())),
            shared_edit_group_id: Arc::new(Mutex::new(None)),
            editor_peer_ids: Arc::new(Mutex::new(BTreeSet::new())),
            relay_peer_ids: Arc::new(Mutex::new(BTreeSet::new())),
            obj_doc_map: Arc::new(Mutex::new(HashMap::new())),
            all_docs: Arc::new(Mutex::new(BTreeSet::new())),
            node_paths: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn is_relay(&self, node: &Node) -> bool {
        node.label == "relay"
    }

    async fn doc_id(&self, obj: &ObjKey) -> Res<DocumentId> {
        self.obj_doc_map
            .lock()
            .await
            .get(obj)
            .cloned()
            .ok_or_else(|| crate::ferr!("stress object {obj:?} has no mapped document id"))
    }

    async fn tracked_docs(&self) -> BTreeSet<DocumentId> {
        self.all_docs.lock().await.clone()
    }

    async fn collect_heads(&self, node: &Node) -> Res<BTreeMap<DocumentId, BTreeSet<[u8; 32]>>> {
        let mut result = BTreeMap::new();
        for doc_id in self.tracked_docs().await {
            let state = node.repo.doc_head_state(doc_id.clone()).await?;
            result.insert(
                doc_id,
                state.sedimentree_heads.iter().map(|head| head.0).collect(),
            );
        }
        Ok(result)
    }

    /// The heads published inside the object payload: what big_sync advertises to
    /// peers and what the sync backend's convergence fast path compares. Distinct from
    /// `sedimentree_heads`, which is the durable storage frontier.
    async fn collect_payload_heads(
        &self,
        node: &Node,
    ) -> Res<BTreeMap<DocumentId, BTreeSet<[u8; 32]>>> {
        let mut result = BTreeMap::new();
        for doc_id in self.tracked_docs().await {
            let heads = node
                .repo
                .doc_payload_heads(doc_id.clone())
                .await?
                .map(|heads| heads.iter().map(|head| head.0).collect())
                .unwrap_or_default();
            result.insert(doc_id, heads);
        }
        Ok(result)
    }

    async fn collect_parts(&self, node: &Node) -> Res<BTreeMap<DocumentId, Vec<PartKey>>> {
        let mut result = BTreeMap::new();
        for doc_id in self.tracked_docs().await {
            let mut parts = node.store.obj_parts(doc_id.clone()).await?;
            parts.sort_unstable();
            result.insert(doc_id, parts);
        }
        Ok(result)
    }
    /// Returns the local receive cursor for every connected peer and sync part.
    async fn collect_peer_cursors(
        &self,
        node: &Node,
        parts: &[PartKey],
    ) -> Res<BTreeMap<PeerKey, BTreeMap<PartKey, u64>>> {
        let mut result = BTreeMap::new();
        for peer_id in node.connected_peer_ids().await {
            let mut peer_cursors = BTreeMap::new();
            for part_id in parts {
                peer_cursors.insert(
                    part_id.clone(),
                    node.store
                        .get_peer_part_cursor(peer_id.clone(), part_id.clone())
                        .await?,
                );
            }
            result.insert(peer_id, peer_cursors);
        }
        Ok(result)
    }
    /// Cursor diagnostic: per-part local cursors, with parts the store does
    /// not know reported as `unknown` rather than failing — a lagging node
    /// lacking a group-part is precisely what this diagnostic is meant to
    /// reveal (previously it error-returned and hid the real mismatch).
    async fn collect_local_cursors(&self, node: &Node, parts: &[PartKey]) -> Res<String> {
        match node
            .store
            .summarize_parts(parts.iter().cloned().collect())
            .await?
        {
            Ok(summaries) => {
                let mut cursors = BTreeMap::new();
                for (part_id, summary) in summaries {
                    cursors.insert(part_id, summary.latest_cursor);
                }
                Ok(format!("{cursors:?}"))
            }
            Err(big_sync_core::rpc::ListPartsError::UnkownParts { unkown_parts }) => {
                Ok(format!("unknown_parts={unkown_parts:?}"))
            }
        }
    }
    async fn edit_group(&self, node: &Node) -> Res<BigKeyhiveGroup> {
        if let Some(group) = self
            .shared_edit_groups
            .lock()
            .await
            .get(&node.peer_id())
            .cloned()
        {
            return Ok(group);
        }

        let existing_group_id = *self.shared_edit_group_id.lock().await;
        let (group, created) = if let Some(group_id) = existing_group_id {
            let group = node
                .repo
                .keyhive()
                .get_group(group_id)
                .await
                .ok_or_else(|| crate::ferr!("persisted stress edit group is unavailable"))?;
            (group, false)
        } else {
            let group = node.repo.create_group_with_parents(Vec::new()).await?;
            *self.shared_edit_group_id.lock().await = Some(group.id());
            (group, true)
        };
        if created {
            let peers = self.editor_peer_ids.lock().await.clone();
            for peer_id in peers {
                if peer_id == node.peer_id() {
                    continue;
                }
                let agent = wait_for_agent(&node.repo, peer_id).await?;
                node.repo
                    .add_member_to_group(agent, &group, Access::Edit)
                    .await?;
            }
        }
        self.shared_edit_groups
            .lock()
            .await
            .insert(node.peer_id(), group.clone());
        Ok(group)
    }
    async fn sync_parts(&self) -> Vec<PartKey> {
        // The stress cluster is GLOBAL-free by design: the group part is the
        // sync primitive under test. Part selection is an explicit
        // code-level decision — everyone (editors and relay alike) listens on
        // the group part; nothing is derived from keyhive visibility.
        let mut parts = BTreeSet::new();
        for group in self.shared_edit_groups.lock().await.values() {
            parts.insert(crate::runtime2::group_part_id(group.id().to_bytes()));
        }
        parts.into_iter().collect()
    }
    /// A compact `doc:stage` list for one node, capped so a report stays readable.
    async fn doc_stage_summary(&self, node: &Node, docs: &BTreeSet<DocumentId>) -> String {
        let mut stages = Vec::new();
        for doc_id in docs.iter().take(SETTLE_STALL_REPORT_DOCS) {
            let stage = node
                .repo
                .document_sync_snapshot(doc_id.clone())
                .await
                .map(|snapshot| format!("{:?}", snapshot.stage))
                .unwrap_or_else(|error| format!("error({error})"));
            stages.push(format!("{}:{stage}", key_prefix(doc_id, 12)));
        }
        if docs.len() > SETTLE_STALL_REPORT_DOCS {
            stages.push(format!("+{}", docs.len() - SETTLE_STALL_REPORT_DOCS));
        }
        stages.join(",")
    }

    /// Name what a still-outstanding settle barrier is waiting for: which nodes have not
    /// returned, their BigSync part cursors, and their per-document sync stage.
    ///
    /// The hub reports its own internal fence state while a quiescence wait is stalled;
    /// this is the cross-node view, and unlike the hub's report it needs the fence to be
    /// armed at the moment it is written rather than continuously pending.
    async fn report_settle_stall(
        &self,
        phase: &str,
        nodes: &[&Node],
        parts: &[PartKey],
        holding: &BTreeSet<usize>,
    ) {
        let tracked_docs = self.tracked_docs().await;
        let mut per_node = Vec::with_capacity(nodes.len());
        for (idx, node) in nodes.iter().enumerate() {
            let cursors = self
                .collect_local_cursors(node, parts)
                .await
                .unwrap_or_else(|error| format!("error({error})"));
            per_node.push(format!(
                "{}(holding={} cursors={cursors} docs={})",
                log_nickname::nickname(&node.peer_id()),
                holding.contains(&idx),
                self.doc_stage_summary(node, &tracked_docs).await,
            ));
        }
        tracing::warn!(
            phase,
            holding = ?holding,
            nodes = %per_node.join(" "),
            "cluster settle still pending",
        );
    }

    /// Await one settle barrier on every node in parallel, reporting every
    /// [`SETTLE_STALL_REPORT_INTERVAL`] while it is still outstanding.
    ///
    /// The waits stay unbounded: the report is evidence for a kill that arrives from
    /// outside (a harness hard timeout), never a deadline of its own.
    async fn await_settle_with_stall_report<F>(
        &self,
        phase: &str,
        nodes: &[&Node],
        parts: &[PartKey],
        waits: Vec<F>,
    ) -> Res<()>
    where
        F: std::future::Future<Output = Res<()>>,
    {
        assert_eq!(
            waits.len(),
            nodes.len(),
            "a settle barrier needs exactly one wait per node"
        );
        let mut in_flight: FuturesUnordered<_> = waits
            .into_iter()
            .enumerate()
            .map(|(idx, wait)| async move { (idx, wait.await) })
            .collect();
        let mut holding: BTreeSet<usize> = (0..nodes.len()).collect();
        let mut tick = tokio::time::interval(SETTLE_STALL_REPORT_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first interval tick is immediate; the report belongs after a full interval.
        tick.tick().await;
        while !holding.is_empty() {
            tokio::select! {
                Some((idx, result)) = in_flight.next() => {
                    holding.remove(&idx);
                    result?;
                }
                _ = tick.tick() => {
                    self.report_settle_stall(phase, nodes, parts, &holding).await;
                }
            }
        }
        Ok(())
    }

    async fn available_sync_parts(&self, left: &Node, right: &Node) -> Res<Vec<PartKey>> {
        let _left_and_right = (left, right);
        Ok(self.sync_parts().await)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigRepoStressObservation {
    pub sedimentree_heads: BTreeMap<DocumentId, BTreeSet<[u8; 32]>>,
    pub parts: BTreeMap<DocumentId, Vec<PartKey>>,
    /// Published payload heads per document, for comparing against the durable
    /// `sedimentree_heads` of the same node.
    pub payload_heads: BTreeMap<DocumentId, BTreeSet<[u8; 32]>>,
}

#[async_trait::async_trait]
impl StressFixture for BigRepoStressFixture {
    type World = ();
    type Node = Node;
    type StressObj = ObjKey;
    type Observation = BigRepoStressObservation;

    fn label(&self) -> &'static str {
        "big_repo_tier10"
    }

    fn make_stress_obj(&self, rng: &mut StdRng) -> Self::StressObj {
        stress_support::stress_obj(rng)
    }

    async fn boot_node(&self, _world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node> {
        // Unique per-node worker label so big-sync machine spans are
        // attributable (the machine only knows remote peer ids).
        let label: &'static str = match self.config.relay_idx {
            Some(index) if index + 1 == peer_seed as usize => "relay",
            _ => Box::leak(format!("editor-{peer_seed}").into_boxed_str()),
        };
        let path = tempdir()?.keep();
        let actual_peer_seed = peer_seed
            .checked_add(self.config.peer_seed_offset)
            .expect("stress peer seed offset overflowed");
        let node = Node::boot_with_config_and_hidden(
            actual_peer_seed,
            label,
            StorageConfig::Disk { path: path.clone() },
            // GLOBAL-free stress cluster: every node (editors included)
            // opts out of the global part via hidden parts — the group part
            // is the sync primitive under test, and hiding GLOBAL exercises
            // the production relay design (large sets never pay global-sub
            // cost) end to end.
            HashSet::from([crate::global_part_id()]),
        )
        .await?;
        self.node_paths.lock().await.insert(node.peer_id(), path);
        Ok(node)
    }

    async fn stop_node(&self, node: Self::Node) -> Res<()> {
        node.shutdown().await;
        Ok(())
    }

    async fn restart_node(
        &self,
        _world: Arc<Self::World>,
        _peer_seed: u8,
        node: Self::Node,
    ) -> Res<Self::Node> {
        let peer_id = node.peer_id();
        self.shared_edit_groups.lock().await.remove(&peer_id);
        let path = self
            .node_paths
            .lock()
            .await
            .get(&peer_id)
            .cloned()
            .ok_or_else(|| crate::ferr!("no persistent path for restarted node {peer_id}"))?;
        node.restart(StorageConfig::Disk { path }).await
    }

    async fn connect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        // No initial parts: the route set is derived below from the shared
        // groups (GLOBAL is hidden cluster-wide in this fixture).
        let _connection = left.connect_with_parts(right, Vec::new()).await?;
        right.accepted_connection().await;
        // Keyhive convergence is notification-driven. The quiescence waits
        // below only let the resulting work settle; they do not initiate a
        // manual sync round.
        left.repo.wait_for_quiescence(None).await?;
        right.repo.wait_for_quiescence(None).await?;
        // Each side advertises the same explicit route set (the group part).
        // Part selection is a code-level decision, not derived from keyhive
        // visibility, so both directions agree by construction.
        let left_parts = self.available_sync_parts(left, right).await?;
        let right_parts = left_parts.clone();
        left.set_peer_parts(right, left_parts).await?;
        right.set_peer_parts(left, right_parts).await?;
        Ok(())
    }

    async fn disconnect_pair(&self, left: &Self::Node, right: &Self::Node) -> Res<()> {
        left.disconnect_peer(right.peer_id()).await?;
        right.disconnect_peer(left.peer_id()).await?;
        Ok(())
    }

    async fn seed_new_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        let mut document = automerge::Automerge::new();
        document
            .transact(|transaction| {
                autosurgeon::reconcile(transaction, ThroughJson(payload.clone()))
                    .expect("failed reconciling stress document");
                Ok::<_, automerge::AutomergeError>(())
            })
            .expect("failed transacting stress document");

        let group = self.edit_group(node).await?;
        let handle = node
            .repo
            .create_doc_with_parents(document, vec![group.into()])
            .await?;
        let doc_id = handle.document_id();
        // No per-doc relay grants: the relay's access comes from its group
        // membership (Access::Relay on the shared group, set in
        // `prepare_cluster`). Group membership is what gives the relay the
        // group-part membership index — per-doc grants would be an
        // anti-pattern.

        self.obj_doc_map
            .lock()
            .await
            .insert(obj.clone(), doc_id.clone());
        self.all_docs.lock().await.insert(doc_id);
        Ok(())
    }

    async fn seed_obj(
        &self,
        node: &Self::Node,
        obj: &Self::StressObj,
        payload: serde_json::Value,
    ) -> Res<()> {
        let doc_id = self.doc_id(obj).await?;
        let handle = match node.repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(handle) => handle,
            crate::DocLookup::PendingMaterialization => {
                return Err(crate::ferr!(
                    "doc {doc_id} is pending on {}",
                    node.peer_id()
                ));
            }
            crate::DocLookup::Missing => {
                return Err(crate::ferr!(
                    "doc {doc_id} is missing on {}",
                    node.peer_id()
                ));
            }
        };
        handle
            .with_document(|document| {
                document
                    .transact(|transaction| {
                        autosurgeon::reconcile(transaction, ThroughJson(payload.clone()))
                            .expect("failed reconciling stress mutation");
                        Ok::<_, automerge::AutomergeError>(())
                    })
                    .expect("failed transacting stress mutation");
            })
            .await?;
        Ok(())
    }

    async fn observed_state(&self, node: &Self::Node) -> Res<Self::Observation> {
        Ok(BigRepoStressObservation {
            sedimentree_heads: self.collect_heads(node).await?,
            parts: self.collect_parts(node).await?,
            payload_heads: self.collect_payload_heads(node).await?,
        })
    }

    fn peer_id(&self, node: &Self::Node) -> PeerKey {
        node.peer_id()
    }

    async fn prepare_cluster(&self, nodes: &[Option<Self::Node>]) -> Res<()> {
        let live: Vec<&Node> = nodes.iter().filter_map(Option::as_ref).collect();
        let editors: Vec<&Node> = live
            .iter()
            .copied()
            .filter(|node| !self.is_relay(node))
            .collect();
        self.editor_peer_ids
            .lock()
            .await
            .extend(editors.iter().map(|node| node.peer_id()));
        self.relay_peer_ids.lock().await.extend(
            live.iter()
                .copied()
                .filter(|node| self.is_relay(node))
                .map(|node| node.peer_id()),
        );
        // This is the only deliberate bootstrap mesh. The shared runner
        // disconnects it before randomized phase 1 begins.
        for left_index in 0..live.len() {
            for right_index in (left_index + 1)..live.len() {
                live[left_index].connect(live[right_index]).await?;
                live[right_index].accepted_connection().await;
            }
        }

        let group_owner = editors.first().expect("stress cluster must have an editor");
        let group = group_owner
            .repo
            .create_group_with_parents(Vec::new())
            .await?;
        *self.shared_edit_group_id.lock().await = Some(group.id());
        for peer_id in self.editor_peer_ids.lock().await.iter().cloned() {
            if peer_id == group_owner.peer_id() {
                continue;
            }
            // The mesh connection only *triggers* the keyhive handshake; the
            // owner's view of this editor's agent may still be in flight.
            let agent = wait_for_agent(&group_owner.repo, peer_id).await?;
            group_owner
                .repo
                .add_member_to_group(agent, &group, Access::Edit)
                .await?;
        }
        // The relay joins the group as a fetcher (group-level pull access).
        // Per-doc grants cannot give a relay the group-part membership index —
        // group membership is the primitive that makes the relay subscribe to
        // and forward the group part.
        for relay_peer_id in self.relay_peer_ids.lock().await.iter() {
            let relay_agent = wait_for_agent(&group_owner.repo, relay_peer_id.clone()).await?;
            group_owner
                .repo
                .add_member_to_group(relay_agent, &group, Access::Relay)
                .await?;
        }
        // Membership propagation is notification-driven. Keep the initial
        // quiescence barrier, but do not inject explicit Keyhive sync rounds
        // into the stress workload.
        for node in &live {
            node.repo.wait_for_quiescence(None).await?;
        }
        // Store each node's independently reconstructed local group. Sharing
        // the owner's in-process group handle across nodes bypasses the
        // distributed Keyhive reconstruction path this stress test exercises.
        for editor in &editors {
            let local_group = editor
                .repo
                .keyhive()
                .get_group(group.id())
                .await
                .ok_or_else(|| {
                    crate::ferr!(
                        "editor {} reached bootstrap quiescence without the shared Keyhive group",
                        editor.peer_id()
                    )
                })?;
            let previous = self
                .shared_edit_groups
                .lock()
                .await
                .insert(editor.peer_id(), local_group);
            assert!(previous.is_none(), "stress editor group was already cached");
        }
        Ok(())
    }

    fn can_create(&self, node: &Self::Node) -> bool {
        !self.is_relay(node)
    }

    async fn can_mutate(&self, node: &Self::Node, obj: &Self::StressObj) -> Res<bool> {
        if self.is_relay(node) {
            return Ok(false);
        }
        let doc_id = match self.doc_id(obj).await {
            Ok(doc_id) => doc_id,
            Err(_) => return Ok(false),
        };
        if !matches!(
            node.repo.get_doc(&doc_id).await?,
            crate::DocLookup::Ready(_)
        ) {
            return Ok(false);
        }
        let agent = node.repo.keyhive().keyhive_peer_id().to_identifier()?;
        let document = keyhive_core::principal::identifier::Identifier::from(
            ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
                .expect("stress document id must be a verifying key"),
        );
        Ok(node
            .repo
            .keyhive()
            .agent_access_on(&agent, document)
            .await
            .is_some_and(|access| access.is_editor()))
    }

    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
        let parts = self.sync_parts().await;
        let editors: Vec<&Node> = nodes
            .iter()
            .copied()
            .filter(|node| !self.is_relay(node))
            .collect();
        // B12 freeze/reopen barrier: pin EVERY node (relays included — they
        // are normal runtime2 nodes whose store-side applies are also
        // hub-gated) at its quiescent point, then reopen and settle again.
        // The freeze holds any event that was queued-but-unprocessed when
        // quiescence resolved (the post-quiescence drift class — e.g. a
        // latched keyhive notif starting a follow-up sync); reopening forces
        // it to replay before the second quiescence wait resolves, so the
        // alignment observation runs against a genuinely settled snapshot
        // instead of racing that drift.
        let barrier_nodes: Vec<&Node> = nodes.to_vec();
        self.await_settle_with_stall_report(
            "freeze",
            &barrier_nodes,
            &parts,
            barrier_nodes
                .iter()
                .map(|node| async move { node.repo.wait_for_quiescence_freeze(None).await })
                .collect(),
        )
        .await?;
        self.await_settle_with_stall_report(
            "unfreeze",
            &barrier_nodes,
            &parts,
            barrier_nodes
                .iter()
                .map(|node| async move { node.repo.unfreeze().await })
                .collect(),
        )
        .await?;
        self.await_settle_with_stall_report(
            "settle",
            &barrier_nodes,
            &parts,
            barrier_nodes
                .iter()
                .map(|node| async move { node.repo.wait_for_quiescence(None).await })
                .collect(),
        )
        .await?;

        // Reconnect all nodes so that any peers disconnected during randomized phases
        // can participate in settlement.
        for left_index in 0..nodes.len() {
            for right_index in (left_index + 1)..nodes.len() {
                self.connect_pair(nodes[left_index], nodes[right_index])
                    .await?;
            }
        }

        // `connect_pair` installs fresh BigSync peer state. Reach a fixed
        // point only after the full mesh exists: applying a sync session can
        // itself persist a causal checkpoint and advance a local part cursor.
        // A fixed number of full-sync rounds can therefore stop one round too
        // early.
        // Scale with the env multiplier like every other timeout here: under
        // CI's UTILS_RS_TIMEOUT_MULTIPLIER=3 (or full-parallel stress runs)
        // a fixed 60s network-rest window can expire before the relay
        // topology converges.
        super::harness::fixtures::wait_for_network_rest(nodes).await?;

        // Natural convergence: alignment is reached via notifs + automerge CRDT
        // semantics.
        let tracked_docs = self.tracked_docs().await;
        let mut last_report = tokio::time::Instant::now();
        let observations: Vec<(PeerKey, BigRepoStressObservation)> = loop {
            let observations: Vec<(PeerKey, BigRepoStressObservation)> =
                try_join_all(nodes.iter().map(|node| async {
                    Ok::<_, crate::interlude::eyre::Report>((
                        node.peer_id(),
                        self.observed_state(node).await?,
                    ))
                }))
                .await?;
            let reference_heads = &observations[0].1.sedimentree_heads;
            let converged = tracked_docs.iter().all(|doc_id| {
                let expected = reference_heads.get(doc_id).cloned().unwrap_or_default();
                observations.iter().all(|(_, observation)| {
                    observation
                        .sedimentree_heads
                        .get(doc_id)
                        .cloned()
                        .unwrap_or_default()
                        == expected
                })
            });
            if last_report.elapsed() >= Duration::from_secs(10) {
                let per_node = observations
                    .iter()
                    .map(|(peer_id, observation)| {
                        let synced = tracked_docs
                            .iter()
                            .filter(|doc_id| observation.sedimentree_heads.contains_key(*doc_id))
                            .count();
                        format!(
                            "{}:{synced}/{}",
                            key_prefix(peer_id, 12),
                            tracked_docs.len()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                let mismatch_detail = tracked_docs
                    .iter()
                    .filter_map(|doc_id| {
                        let expected = reference_heads.get(doc_id).cloned().unwrap_or_default();
                        let differing: Vec<String> = observations
                            .iter()
                            .filter(|(_, observation)| {
                                observation
                                    .sedimentree_heads
                                    .get(doc_id)
                                    .cloned()
                                    .unwrap_or_default()
                                    != expected
                            })
                            .map(|(peer_id, observation)| {
                                let actual = observation
                                    .sedimentree_heads
                                    .get(doc_id)
                                    .cloned()
                                    .unwrap_or_default();
                                format!(
                                    "{}:heads={actual:?} payload={:?}",
                                    key_prefix(peer_id, 12),
                                    observation
                                        .payload_heads
                                        .get(doc_id)
                                        .cloned()
                                        .unwrap_or_default(),
                                )
                            })
                            .collect();
                        (!differing.is_empty()).then(|| {
                            format!(
                                "{}:reference_heads={expected:?} [{}]",
                                doc_id,
                                differing.join(",")
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                let tracked_docs_ref = &tracked_docs;
                let stage_detail = try_join_all(nodes.iter().map(|node| async move {
                    Ok::<_, crate::interlude::eyre::Report>(format!(
                        "{}={}",
                        log_nickname::nickname(&node.peer_id()),
                        self.doc_stage_summary(node, tracked_docs_ref).await,
                    ))
                }))
                .await?
                .join(" ");
                // TEMP-DIAGNOSTIC: per-node sync-machine state. A head mismatch alone
                // cannot say which part, strategy flag, or stalled object sync is holding
                // the cluster apart; `waiters` names the exact (peer, part) full sync is
                // still waiting on, and `last_synced` ages say which paths went silent.
                let sync_state = try_join_all(nodes.iter().map(|node| async move {
                    let snapshot = node.worker.snapshot().await?;
                    let routes = snapshot
                        .peer_parts
                        .iter()
                        .flat_map(|(peer, parts)| {
                            parts.iter().map(move |(part, backend)| {
                                format!(
                                    "{}:{}=>{backend}",
                                    key_prefix(peer, 8),
                                    key_prefix(part, 10),
                                )
                            })
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    let flags = snapshot
                        .peer_part_sync_flags
                        .iter()
                        .map(|(peer, part, pending, multi, replay_done, cursor_active)| {
                            format!(
                                "{}:{}:pending={pending},multi={multi},replay_done={replay_done},cursor_active={cursor_active}",
                                key_prefix(peer, 8),
                                key_prefix(part, 10),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    let last_synced = snapshot
                        .last_object_syncs
                        .iter()
                        .map(|(peer, part, obj, at)| {
                            format!(
                                "{}:{}->{} age={:.1}s",
                                key_prefix(peer, 8),
                                key_prefix(part, 10),
                                key_prefix(obj, 12),
                                at.elapsed().as_secs_f64(),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    Ok::<_, crate::interlude::eyre::Report>(format!(
                        "{}[routes=[{routes}] flags=[{flags}] waiters={:?} last_synced=[{last_synced}] tasks={:?} machine={} sync={} zombies={}]",
                        log_nickname::nickname(&node.peer_id()),
                        snapshot.full_sync_waiters,
                        snapshot.task_counts,
                        snapshot.active_machine_tasks,
                        snapshot.active_sync_tasks,
                        snapshot.zombie_tasks,
                    ))
                }))
                .await?
                .join(" ");
                tracing::info!(
                    converged,
                    per_node,
                    mismatch = mismatch_detail,
                    stages = %stage_detail,
                    sync_state = %sync_state,
                    "convergence poll",
                );
                last_report = tokio::time::Instant::now();
            }
            if converged {
                break observations;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };

        let format_heads = |heads: &BTreeSet<[u8; 32]>| {
            heads
                .iter()
                .map(|head| {
                    head.iter()
                        .take(8)
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
                .join(",")
        };
        let _reference_peer = observations
            .first()
            .map(|(peer_id, _)| peer_id.clone())
            .expect("stress cluster must contain nodes");
        let _reference_heads = &observations[0].1.sedimentree_heads;
        let mut sedimentree_mismatches = Vec::new();
        for doc_id in &tracked_docs {
            let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32()) else {
                continue;
            };
            let doc_identifier = keyhive_core::principal::identifier::Identifier::from(vk);
            // Filter peers to those with active read access for this document.
            let mut active_peers = Vec::new();
            for (node, observation) in nodes.iter().zip(observations.iter()) {
                let Ok(agent_id) = node.repo.keyhive().keyhive_peer_id().to_identifier() else {
                    continue;
                };
                let access = node
                    .repo
                    .keyhive()
                    .agent_access_on(&agent_id, doc_identifier)
                    .await;
                if access.is_some_and(|a| a >= Access::Read) {
                    active_peers.push((node.peer_id(), &observation.1));
                }
            }
            if active_peers.is_empty() {
                continue;
            }
            let reference_peer = &active_peers[0].0;
            let expected = active_peers[0]
                .1
                .sedimentree_heads
                .get(doc_id)
                .cloned()
                .unwrap_or_default();
            let mut differences = Vec::new();
            for (peer_id, obs) in &active_peers {
                let actual = obs
                    .sedimentree_heads
                    .get(doc_id)
                    .cloned()
                    .unwrap_or_default();
                if actual != expected {
                    let peer_parts = obs.parts.get(doc_id).cloned().unwrap_or_default();
                    differences.push(format!(
                        "peer={peer_id} count={} missing_vs_{reference_peer}=[{}] extra_vs_{reference_peer}=[{}] parts={peer_parts:?}",
                        actual.len(),
                        format_heads(&expected.difference(&actual).copied().collect()),
                        format_heads(&actual.difference(&expected).copied().collect()),
                    ));
                }
            }
            if !differences.is_empty() {
                sedimentree_mismatches.push(format!(
                    "doc={doc_id} reference_peer={reference_peer} reference_count={} differences=[{}]",
                    expected.len(),
                    differences.join("; "),
                ));
            }
        }

        tracing::info!(
            barrier = "editor-automerge-head-equality",
            editor_count = editors.len(),
            document_count = tracked_docs.len(),
            "stress barrier begin"
        );
        let mut materialized_by_peer = Vec::new();
        for node in &editors {
            let mut documents = BTreeMap::new();
            for doc_id in &tracked_docs {
                let materialization = match node.repo.get_doc(doc_id).await? {
                    crate::DocLookup::Ready(handle) => {
                        let mut heads = handle
                            .with_document_read(|document| document.get_heads())
                            .await;
                        heads.sort_unstable();
                        (
                            "ready",
                            Some(
                                heads
                                    .into_iter()
                                    .map(|head| head.0)
                                    .collect::<BTreeSet<_>>(),
                            ),
                        )
                    }
                    crate::DocLookup::PendingMaterialization => ("pending", None),
                    crate::DocLookup::Missing => ("missing", None),
                };
                documents.insert(doc_id.clone(), materialization);
            }
            materialized_by_peer.push((node.peer_id(), documents));
        }

        let materialized_reference_peer = materialized_by_peer
            .first()
            .map(|(peer_id, _)| peer_id.clone())
            .expect("stress cluster must contain an editor");
        let materialized_reference = &materialized_by_peer[0].1;
        let mut materialized_mismatches = Vec::new();
        for doc_id in &tracked_docs {
            let (expected_state, expected) = materialized_reference
                .get(doc_id)
                .cloned()
                .expect("tracked document must have a materialization observation");
            let mut differences = Vec::new();
            for (peer_id, documents) in &materialized_by_peer {
                let (actual_state, actual) = documents
                    .get(doc_id)
                    .cloned()
                    .expect("tracked document must have a materialization observation");
                if actual != expected || actual_state != expected_state {
                    let node = editors
                        .iter()
                        .copied()
                        .find(|node| node.peer_id() == *peer_id)
                        .expect("materialization peer must have a node");
                    let parts = node.store.obj_parts(doc_id.clone()).await?;
                    let blob_lengths = node
                        .repo
                        .inspect_stored_doc_blobs(doc_id.clone())
                        .await?
                        .iter()
                        .map(Vec::len)
                        .collect::<Vec<_>>();
                    let agent_id = keyhive_core::principal::identifier::Identifier::from(
                        ed25519_dalek::VerifyingKey::from_bytes(&peer_id.to_bytes32())
                            .expect("stress peer id must be a verifying key"),
                    );
                    let doc_identifier = keyhive_core::principal::identifier::Identifier::from(
                        ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
                            .expect("stress document id must be a verifying key"),
                    );
                    let access = node
                        .repo
                        .keyhive()
                        .agent_access_on(&agent_id, doc_identifier)
                        .await;
                    differences.push(format!(
                        "peer={peer_id} state={actual_state} heads=[{}] access={access:?} parts={parts:?} stored_blob_lengths={blob_lengths:?}",
                        actual.as_ref().map(&format_heads).unwrap_or_default(),
                    ));
                }
            }
            if expected.is_none() || !differences.is_empty() {
                materialized_mismatches.push(format!(
                    "doc={doc_id} reference_peer={materialized_reference_peer} reference_state={expected_state} reference_heads=[{}] differences=[{}]",
                    expected.as_ref().map(&format_heads).unwrap_or_default(),
                    differences.join("; "),
                ));
            }
        }
        if sedimentree_mismatches.is_empty() && materialized_mismatches.is_empty() {
            tracing::info!(
                barrier = "editor-automerge-head-equality",
                "stress barrier complete"
            );
            return Ok(());
        }
        let peer_cursors = try_join_all(nodes.iter().map(|node| async {
            Ok::<_, crate::interlude::eyre::Report>({
                let local_cursors = self.collect_local_cursors(node, &parts).await?;
                let receive_cursors = self.collect_peer_cursors(node, &parts).await?;
                format!(
                    "node={} local_cursors={local_cursors} receive_cursors={receive_cursors:?}",
                    node.peer_id(),
                )
            })
        }))
        .await?;
        Err(crate::ferr!(
            "stress cluster did not converge after local barriers:\nsedimentree mismatches:\n{}\nmaterialized mismatches:\n{}\npeer cursors:\n{}",
            if sedimentree_mismatches.is_empty() {
                "  none".to_owned()
            } else {
                sedimentree_mismatches
                    .iter()
                    .map(|mismatch| format!("  {mismatch}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            if materialized_mismatches.is_empty() {
                "  none".to_owned()
            } else {
                materialized_mismatches
                    .iter()
                    .map(|mismatch| format!("  {mismatch}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            peer_cursors
                .iter()
                .map(|cursors| format!("  {cursors}"))
                .collect::<Vec<_>>()
                .join("\n"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_sync::stress_support::{
        PHASE1_MUTATIONS, PHASE2_MUTATIONS, PHASE3_MUTATIONS, run_randomized_stress,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn long_test_big_repo_tier10_stress_4_editor_converges() -> Res<()> {
        let config = BigRepoStressConfig::default();
        let fixture = BigRepoStressFixture::new(config.clone());
        run_randomized_stress(
            fixture,
            Arc::new(()),
            config.seed,
            config.node_count,
            PHASE1_MUTATIONS / 2,
            PHASE2_MUTATIONS / 2,
            PHASE3_MUTATIONS / 2,
            None,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn long_test_big_repo_tier10_stress_3_editor_1_relay_converges() -> Res<()> {
        let config = BigRepoStressConfig {
            relay_idx: Some(3),
            peer_seed_offset: 64,
            ..BigRepoStressConfig::default()
        };
        let fixture = BigRepoStressFixture::new(config.clone());
        run_randomized_stress(
            fixture,
            Arc::new(()),
            config.seed,
            config.node_count,
            PHASE1_MUTATIONS,
            PHASE2_MUTATIONS,
            PHASE3_MUTATIONS,
            None,
        )
        .await
    }
}
