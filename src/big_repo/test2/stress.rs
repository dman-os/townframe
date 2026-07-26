//! Tier 10 — randomized BigRepo stress with topology churn and relays.
//!
//! The shared stress runner owns topology selection. This fixture only creates
//! documents, performs mutations on nodes that are already ready, and checks
//! the resulting durable frontier after the runner reconnects the full mesh.

use super::harness::topo::Node;
use crate::{BigKeyhiveGroup, DocumentId, PeerId, Res, StorageConfig};
use am_utils_rs::codecs::ThroughJson;
use autosurgeon;
use big_sync::{stress_support::{self, StressFixture}, HostPartStore};
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
}

impl Default for BigRepoStressConfig {
    fn default() -> Self {
        Self {
            node_count: 4,
            relay_idx: None,
            seed: DEFAULT_STRESS_SEED,
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
        let node = Node::boot_with_config(
            peer_seed,
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
        let connection = left
            .connect_with_parts(right, vec![crate::GLOBAL_PART_ID])
            .await?;
        let _ = right.accepted_connection().await;
        connection.sync_keyhive_with_peer(Some(Duration::from_secs(10))).await?;
        let reverse = right.connection_to(left.peer_id()).await?;
        reverse
            .sync_keyhive_with_peer(Some(Duration::from_secs(10)))
            .await?;
        left.repo.wait_for_quiescence(Some(Duration::from_secs(20))).await?;
        right.repo.wait_for_quiescence(Some(Duration::from_secs(20))).await?;
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
        for peer_id in node.connected_peer_ids().await {
            node
                .connection_to(peer_id)
                .await?
                .sync_keyhive_with_peer(Some(Duration::from_secs(10)))
                .await?;
        }

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
                return Err(crate::ferr!("doc {doc_id} is pending on {}", node.peer_id()))
            }
            crate::DocLookup::Missing => {
                return Err(crate::ferr!("doc {doc_id} is missing on {}", node.peer_id()))
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
        let editors: Vec<&Node> = live.iter().copied().filter(|node| !self.is_relay(node)).collect();
        self.editor_peer_ids
            .lock()
            .await
            .extend(editors.iter().map(|node| node.peer_id()));
        self.relay_peer_ids
            .lock()
            .await
            .extend(live.iter().copied().filter(|node| self.is_relay(node)).map(|node| node.peer_id()));
        // This is the only deliberate bootstrap mesh. The shared runner
        // disconnects it before randomized phase 1 begins.
        for left_index in 0..live.len() {
            for right_index in (left_index + 1)..live.len() {
                let connection = live[left_index].connect(live[right_index]).await?;
                let _ = live[right_index].accepted_connection().await;
                connection
                    .sync_keyhive_with_peer(Some(Duration::from_secs(10)))
                    .await?;
            }
        }

        let group_owner = editors
            .first()
            .expect("stress cluster must have an editor");
        let group = group_owner.repo.create_group_with_parents(Vec::new()).await?;
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
                .ok_or_else(|| {
                    crate::ferr!("agent {peer_id} not discovered during bootstrap")
                })?;
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

        // Deliver the bootstrap group membership over the connections that
        // already exist; no extra topology is introduced.
        for left in &editors {
            for right in &editors {
                if left.peer_id() >= right.peer_id() {
                    continue;
                }
                left.connection_to(right.peer_id())
                    .await?
                    .sync_keyhive_with_peer(Some(Duration::from_secs(10)))
                    .await?;
                right
                    .connection_to(left.peer_id())
                    .await?
                    .sync_keyhive_with_peer(Some(Duration::from_secs(10)))
                    .await?;
            }
        }
        for node in &live {
            node.repo.wait_for_quiescence(Some(Duration::from_secs(20))).await?;
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
        Ok(matches!(node.repo.get_doc(&doc_id).await?, crate::DocLookup::Ready(_)))
    }

    async fn assert_cluster_alignment(&self, nodes: &[&Self::Node]) -> Res<()> {
        let parts = vec![crate::GLOBAL_PART_ID];
        for node in nodes {
            let peer_ids = node.connected_peer_ids().await;
            timeout(
                Duration::from_secs(20),
                node.worker
                    .wait_for_full_sync(peer_ids.into_iter(), parts.iter().copied()),
            )
            .await
            .map_err(|_| {
                crate::ferr!(
                    "timed out waiting for existing BigSync routes on {}",
                    node.peer_id()
                )
            })??;
        }
        let editors: Vec<&Node> = nodes
            .iter()
            .copied()
            .filter(|node| !self.is_relay(node))
            .collect();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut last_observations = None;
        loop {
            for node in nodes {
                node.repo
                    .wait_for_quiescence(Some(Duration::from_secs(5)))
                    .await?;
            }
            let observations: Vec<(PeerId, BigRepoStressObservation)> = try_join_all(
                nodes.iter().map(|node| async {
                    Ok::<_, crate::interlude::eyre::Report>((node.peer_id(), self.observed_state(node).await?))
                }),
            )
            .await?;
            let sedimentree_aligned = observations
                .windows(2)
                .all(|pair| pair[0].1.sedimentree_heads == pair[1].1.sedimentree_heads);
            let mut materialized = Vec::new();
            for node in &editors {
                let mut heads = BTreeMap::new();
                for doc_id in self.tracked_docs().await {
                    let values = match node.repo.get_doc(&doc_id).await? {
                        crate::DocLookup::Ready(handle) => {
                            let mut values = handle
                                .with_document_read(|document| document.get_heads())
                                .await;
                            values.sort_unstable();
                            Some(values.into_iter().map(|head| head.0).collect::<BTreeSet<_>>())
                        }
                        crate::DocLookup::PendingMaterialization => None,
                        crate::DocLookup::Missing => None,
                    };
                    heads.insert(doc_id, values);
                }
                materialized.push(heads);
            }
            let materialized_ready = materialized
                .iter()
                .flat_map(|heads| heads.values())
                .all(Option::is_some);
            let materialized_aligned = materialized_ready
                && materialized.windows(2).all(|pair| pair[0] == pair[1]);
            if sedimentree_aligned && materialized_aligned {
                return Ok(());
            }
            last_observations = Some(observations);
            if std::time::Instant::now() >= deadline {
                let sedimentree_counts = last_observations.as_ref().map(|observations| {
                    observations
                        .iter()
                        .map(|(peer, observation)| {
                            (*peer, observation.sedimentree_heads.values().map(BTreeSet::len).sum::<usize>())
                        })
                        .collect::<Vec<_>>()
                });
                let materialized_counts = materialized
                    .iter()
                    .map(|heads| {
                        heads
                            .values()
                            .map(|heads| heads.as_ref().map(BTreeSet::len).unwrap_or(0))
                            .sum::<usize>()
                    })
                    .collect::<Vec<_>>();
                return Err(crate::ferr!(
                    "stress cluster did not naturally converge: sedimentree_head_counts={sedimentree_counts:?}; materialized_head_counts={materialized_counts:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
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
