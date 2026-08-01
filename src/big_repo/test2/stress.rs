//! Tier 10 — randomized BigRepo stress with topology churn and relays.
//!
//! The shared stress runner owns topology selection. This fixture only creates
//! documents, performs mutations on nodes that are already ready, and checks
//! the resulting durable frontier after the runner reconnects the full mesh.

use super::harness::topo::Node;
use crate::{BigKeyhiveGroup, DocumentId, PeerId, Res, StorageConfig};
use am_utils_rs::codecs::ThroughJson;
use autosurgeon;
use big_sync::{
    stress_support::{self, StressFixture},
    HostPartStore,
};
use big_sync_core::{ObjId, PartId};
use futures::future::try_join_all;
use keyhive_core::access::Access;
use rand::rngs::StdRng;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use subduction_keyhive::KeyhivePeerId;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::timeout;

pub const DEFAULT_STRESS_SEED: u64 = 0xB1A0_5EED_5EED_0002;

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
            seed: DEFAULT_STRESS_SEED,
            peer_seed_offset: 0,
        }
    }
}

pub(crate) struct BigRepoStressFixture {
    config: BigRepoStressConfig,
    shared_edit_groups: Arc<Mutex<HashMap<PeerId, BigKeyhiveGroup>>>,
    shared_edit_group_id: Arc<Mutex<Option<keyhive_core::principal::group::id::GroupId>>>,
    editor_peer_ids: Arc<Mutex<BTreeSet<PeerId>>>,
    relay_peer_ids: Arc<Mutex<BTreeSet<PeerId>>>,
    obj_doc_map: Arc<Mutex<HashMap<ObjId, DocumentId>>>,
    all_docs: Arc<Mutex<BTreeSet<DocumentId>>>,
    node_paths: Arc<Mutex<HashMap<PeerId, PathBuf>>>,
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

    async fn doc_id(&self, obj: &ObjId) -> Res<DocumentId> {
        self.obj_doc_map
            .lock()
            .await
            .get(obj)
            .copied()
            .ok_or_else(|| crate::ferr!("stress object {obj:?} has no mapped document id"))
    }

    async fn tracked_docs(&self) -> BTreeSet<DocumentId> {
        self.all_docs.lock().await.clone()
    }

    async fn collect_heads(&self, node: &Node) -> Res<BTreeMap<DocumentId, BTreeSet<[u8; 32]>>> {
        let mut result = BTreeMap::new();
        for doc_id in self.tracked_docs().await {
            let state = node.repo.doc_head_state(doc_id).await?;
            result.insert(
                doc_id,
                state.sedimentree_heads.iter().map(|head| head.0).collect(),
            );
        }
        Ok(result)
    }

    async fn collect_parts(&self, node: &Node) -> Res<BTreeMap<DocumentId, Vec<PartId>>> {
        let mut result = BTreeMap::new();
        for doc_id in self.tracked_docs().await {
            let mut parts = node.store.obj_parts(doc_id).await?;
            parts.sort_unstable();
            result.insert(doc_id, parts);
        }
        Ok(result)
    }
    /// Returns the local receive cursor for every connected peer and sync part.
    async fn collect_peer_cursors(
        &self,
        node: &Node,
        parts: &[PartId],
    ) -> Res<BTreeMap<PeerId, BTreeMap<PartId, u64>>> {
        let mut result = BTreeMap::new();
        for peer_id in node.connected_peer_ids().await {
            let mut peer_cursors = BTreeMap::new();
            for part_id in parts {
                peer_cursors.insert(
                    *part_id,
                    node.store.get_peer_part_cursor(peer_id, *part_id).await?,
                );
            }
            result.insert(peer_id, peer_cursors);
        }
        Ok(result)
    }
    async fn collect_local_cursors(
        &self,
        node: &Node,
        parts: &[PartId],
    ) -> Res<BTreeMap<PartId, u64>> {
        let summaries = node
            .store
            .summarize_parts(parts.iter().copied().collect())
            .await?
            .map_err(|err| crate::ferr!("unable to summarize local sync parts: {err:?}"))?;
        Ok(summaries
            .into_iter()
            .map(|(part_id, summary)| (part_id, summary.latest_cursor))
            .collect())
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
                let keyhive_peer = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
                let agent = node
                    .repo
                    .keyhive()
                    .get_agent_by_peer_id(&keyhive_peer)
                    .await?
                    .ok_or_else(|| {
                        crate::ferr!("agent {peer_id} is not available on {}", node.peer_id())
                    })?;
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
    async fn sync_parts(&self) -> Vec<PartId> {
        let mut parts = BTreeSet::from([crate::GLOBAL_PART_ID]);
        for group in self.shared_edit_groups.lock().await.values() {
            parts.insert(crate::runtime2::group_part_id(group.id().to_bytes()));
        }
        parts.into_iter().collect()
    }
    async fn available_sync_parts(&self, left: &Node, right: &Node) -> Res<Vec<PartId>> {
        let mut available = Vec::new();
        for part in self.sync_parts().await {
            let left_has_part = left
                .store
                .summarize_parts(HashSet::from([part]))
                .await?
                .is_ok();
            let right_has_part = right
                .store
                .summarize_parts(HashSet::from([part]))
                .await?
                .is_ok();
            if left_has_part && right_has_part {
                available.push(part);
            }
        }
        Ok(available)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BigRepoStressObservation {
    pub sedimentree_heads: BTreeMap<DocumentId, BTreeSet<[u8; 32]>>,
    pub parts: BTreeMap<DocumentId, Vec<PartId>>,
}

#[async_trait::async_trait]
impl StressFixture for BigRepoStressFixture {
    type World = ();
    type Node = Node;
    type StressObj = ObjId;
    type Observation = BigRepoStressObservation;

    fn label(&self) -> &'static str {
        "big_repo_tier10"
    }

    fn make_stress_obj(&self, rng: &mut StdRng) -> Self::StressObj {
        stress_support::stress_obj(rng)
    }

    async fn boot_node(&self, _world: Arc<Self::World>, peer_seed: u8) -> Res<Self::Node> {
        let label = match self.config.relay_idx {
            Some(index) if index + 1 == peer_seed as usize => "relay",
            _ => "editor",
        };
        let path = tempdir()?.keep();
        let actual_peer_seed = peer_seed
            .checked_add(self.config.peer_seed_offset)
            .expect("stress peer seed offset overflowed");
        let node = Node::boot_with_config(
            actual_peer_seed,
            label,
            StorageConfig::Disk { path: path.clone() },
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
        let _connection = left
            .connect_with_parts(right, vec![crate::GLOBAL_PART_ID])
            .await?;
        let _ = right.accepted_connection().await;
        // Keyhive convergence is notification-driven. The quiescence waits
        // below only let the resulting work settle; they do not initiate a
        // manual sync round.
        left.repo
            .wait_for_quiescence(Some(Duration::from_secs(20)))
            .await?;
        right
            .repo
            .wait_for_quiescence(Some(Duration::from_secs(20)))
            .await?;
        let parts = self.available_sync_parts(left, right).await?;
        left.set_peer_parts(right, parts.clone()).await?;
        right.set_peer_parts(left, parts).await?;
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
        for relay_peer_id in self.relay_peer_ids.lock().await.iter().copied() {
            let relay_agent = node
                .repo
                .keyhive()
                .get_agent_by_peer_id(&KeyhivePeerId::from_bytes(*relay_peer_id.as_bytes()))
                .await?
                .ok_or_else(|| crate::ferr!("relay agent {relay_peer_id} is unavailable"))?;
            node.repo
                .grant_doc_access(doc_id, relay_agent, Access::Relay)
                .await?;
        }
        // The Keyhive notification listeners propagate the new relay grants;
        // this stress fixture intentionally does not force a synchronous
        // Keyhive round after every document creation.

        self.obj_doc_map.lock().await.insert(*obj, doc_id);
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
        })
    }

    fn peer_id(&self, node: &Self::Node) -> PeerId {
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
                let _connection = live[left_index].connect(live[right_index]).await?;
                let _ = live[right_index].accepted_connection().await;
            }
        }

        let group_owner = editors.first().expect("stress cluster must have an editor");
        let group = group_owner
            .repo
            .create_group_with_parents(Vec::new())
            .await?;
        *self.shared_edit_group_id.lock().await = Some(group.id());
        for peer_id in self.editor_peer_ids.lock().await.iter().copied() {
            if peer_id == group_owner.peer_id() {
                continue;
            }
            let keyhive_peer = KeyhivePeerId::from_bytes(*peer_id.as_bytes());
            let agent = group_owner
                .repo
                .keyhive()
                .get_agent_by_peer_id(&keyhive_peer)
                .await?
                .ok_or_else(|| crate::ferr!("agent {peer_id} not discovered during bootstrap"))?;
            group_owner
                .repo
                .add_member_to_group(agent, &group, Access::Edit)
                .await?;
        }
        for editor in &editors {
            self.shared_edit_groups
                .lock()
                .await
                .insert(editor.peer_id(), group.clone());
        }
        // Membership propagation is notification-driven. Keep the initial
        // quiescence barrier, but do not inject explicit Keyhive sync rounds
        // into the stress workload.
        for node in &live {
            node.repo
                .wait_for_quiescence(Some(Duration::from_secs(20)))
                .await?;
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
        Ok(matches!(
            node.repo.get_doc(&doc_id).await?,
            crate::DocLookup::Ready(_)
        ))
    }

    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
        let parts = self.sync_parts().await;
        try_join_all(nodes.iter().map(|node| async {
            let peer_ids = node.connected_peer_ids().await;
            let snapshot = node.worker.snapshot().await?;
            for peer_id in peer_ids {
                let peer_parts = snapshot
                    .peer_parts
                    .get(&peer_id)
                    .ok_or_else(|| crate::ferr!("missing BigSync route for peer {peer_id}"))?
                    .keys()
                    .copied();
                timeout(
                    Duration::from_secs(20),
                    node.worker.wait_for_full_sync([peer_id], peer_parts),
                )
                .await
                .map_err(|_| {
                    crate::ferr!(
                        "timed out waiting for existing BigSync route from {} to {peer_id}",
                        node.peer_id(),
                    )
                })??;
            }
            Ok::<_, crate::interlude::eyre::Report>(())
        }))
        .await?;

        let editors: Vec<&Node> = nodes
            .iter()
            .copied()
            .filter(|node| !self.is_relay(node))
            .collect();
        try_join_all(editors.iter().map(|node| async {
            node.repo
                .wait_for_quiescence(Some(Duration::from_secs(20)))
                .await
        }))
        .await?;

        let observations: Vec<(PeerId, BigRepoStressObservation)> =
            try_join_all(nodes.iter().map(|node| async {
                Ok::<_, crate::interlude::eyre::Report>((
                    node.peer_id(),
                    self.observed_state(node).await?,
                ))
            }))
            .await?;

        let tracked_docs = self.tracked_docs().await;
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
        let reference_peer = observations
            .first()
            .map(|(peer_id, _)| *peer_id)
            .expect("stress cluster must contain nodes");
        let reference_heads = &observations[0].1.sedimentree_heads;
        let mut sedimentree_mismatches = Vec::new();
        for doc_id in &tracked_docs {
            let expected = reference_heads.get(doc_id).cloned().unwrap_or_default();
            let mut differences = Vec::new();
            for (peer_id, observation) in &observations {
                let actual = observation
                    .sedimentree_heads
                    .get(doc_id)
                    .cloned()
                    .unwrap_or_default();
                if actual != expected {
                    let peer_parts = observation.parts.get(doc_id).cloned().unwrap_or_default();
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
                documents.insert(*doc_id, materialization);
            }
            materialized_by_peer.push((node.peer_id(), documents));
        }

        let materialized_reference_peer = materialized_by_peer
            .first()
            .map(|(peer_id, _)| *peer_id)
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
                    let parts = node.store.obj_parts(*doc_id).await?;
                    let blob_lengths = node
                        .repo
                        .inspect_stored_doc_blobs(*doc_id)
                        .await?
                        .iter()
                        .map(Vec::len)
                        .collect::<Vec<_>>();
                    let agent_id = keyhive_core::principal::identifier::Identifier::from(
                        ed25519_dalek::VerifyingKey::from_bytes(peer_id.as_bytes())
                            .expect("stress peer id must be a verifying key"),
                    );
                    let doc_identifier = keyhive_core::principal::identifier::Identifier::from(
                        ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
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
            return Ok(());
        }
        let peer_cursors = try_join_all(nodes.iter().map(|node| async {
            Ok::<_, crate::interlude::eyre::Report>({
                let local_cursors = self.collect_local_cursors(node, &parts).await?;
                let receive_cursors = self.collect_peer_cursors(node, &parts).await?;
                format!(
                    "node={} local_cursors={local_cursors:?} receive_cursors={receive_cursors:?}",
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
        run_randomized_stress, PHASE1_MUTATIONS, PHASE2_MUTATIONS, PHASE3_MUTATIONS,
    };
    const SETTLE_TIMEOUT: Duration = Duration::from_secs(60);

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_tier10_stress_4_editor_converges() -> Res<()> {
        let config = BigRepoStressConfig::default();
        let fixture = BigRepoStressFixture::new(config.clone());
        run_randomized_stress(
            fixture,
            Arc::new(()),
            config.seed,
            config.node_count,
            PHASE1_MUTATIONS,
            PHASE2_MUTATIONS,
            PHASE3_MUTATIONS,
            SETTLE_TIMEOUT,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn big_repo_tier10_stress_3_editor_1_relay_converges() -> Res<()> {
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
            SETTLE_TIMEOUT,
        )
        .await
    }
}
