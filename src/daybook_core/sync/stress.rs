use super::*;

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

const NODE_COUNT: usize = 4;
const EVENT_COUNT: usize = 64;
const PHASE_TIMEOUT: Duration = Duration::from_secs(90);
const FULL_SYNC_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STRESS_SEED: u64 = 0xD4B5_51C0_0001;

#[derive(Clone, Copy, Debug)]
enum EventKind {
    CreateDoc,
    ModifyDoc,
    CreateBranch,
    ModifyBranch,
    DeleteBranch,
    PutBlobAttach,
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_randomized_four_node_stress_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let seed = std::env::var("DAYB_SYNC_TEST_SEED")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_STRESS_SEED);
    let mut rng = StdRng::seed_from_u64(seed);
    info!(seed, "starting four-node sync stress test");

    let temp_root = tempfile::tempdir()?;
    let repo_paths = init_and_copy_repo_cluster(temp_root.path()).await?;
    let mut nodes = open_cluster_nodes(&repo_paths).await?;

    let topology_1 = generate_connected_edges(&mut rng);
    info!(?topology_1, "phase-1 topology");
    let mut endpoints = connect_topology(&nodes, &topology_1).await?;
    wait_network_rest(&nodes, &endpoints, FULL_SYNC_TIMEOUT).await?;

    let mut applied = Vec::new();
    for idx in 0..EVENT_COUNT {
        let node_idx = rng.random_range(0..NODE_COUNT);
        if let Some(node) = nodes[node_idx].as_ref() {
            let kind = random_event_kind(&mut rng);
            if let Some(detail) = apply_event(node, kind, idx, &mut rng).await? {
                applied.push(format!("#{idx} node={node_idx} kind={kind:?} {detail}"));
            }
        }
    }
    info!(
        applied_count = applied.len(),
        sample = ?applied.iter().take(12).collect::<Vec<_>>(),
        "phase-1 events applied"
    );
    wait_network_rest(&nodes, &endpoints, PHASE_TIMEOUT).await?;

    let leaving_idx = rng.random_range(0..NODE_COUNT);
    info!(leaving_idx, "transfer phase: leaving node");
    let leaving_node = nodes[leaving_idx]
        .take()
        .ok_or_eyre("leaving node missing from cluster state")?;
    leaving_node.stop().await?;

    for _ in 0..(EVENT_COUNT / 2) {
        let mut active = (0..NODE_COUNT)
            .filter(|idx| nodes[*idx].is_some())
            .collect::<Vec<_>>();
        active.shuffle(&mut rng);
        let node_idx = *active.first().ok_or_eyre("no active nodes in transfer phase")?;
        let node = nodes[node_idx]
            .as_ref()
            .ok_or_eyre("active node unexpectedly missing")?;
        let kind = random_event_kind(&mut rng);
        let _ = apply_event(node, kind, EVENT_COUNT, &mut rng).await?;
    }

    let reopened = open_sync_node(&repo_paths[leaving_idx]).await?;
    nodes[leaving_idx] = Some(reopened);

    let topology_2 = generate_connected_edges(&mut rng);
    info!(?topology_2, "phase-2 topology");
    endpoints = connect_topology(&nodes, &topology_2).await?;
    wait_network_rest(&nodes, &endpoints, PHASE_TIMEOUT).await?;

    for node in nodes.into_iter().flatten() {
        node.stop().await?;
    }
    Ok(())
}

fn random_event_kind(rng: &mut StdRng) -> EventKind {
    match rng.random_range(0..6) {
        0 => EventKind::CreateDoc,
        1 => EventKind::ModifyDoc,
        2 => EventKind::CreateBranch,
        3 => EventKind::ModifyBranch,
        4 => EventKind::DeleteBranch,
        _ => EventKind::PutBlobAttach,
    }
}

fn generate_connected_edges(rng: &mut StdRng) -> Vec<(usize, usize)> {
    let mut edges = HashSet::new();
    let mut order = (0..NODE_COUNT).collect::<Vec<_>>();
    order.shuffle(rng);
    for pair in order.windows(2) {
        let (a, b) = canon_edge(pair[0], pair[1]);
        edges.insert((a, b));
    }

    for i in 0..NODE_COUNT {
        for j in (i + 1)..NODE_COUNT {
            if edges.contains(&(i, j)) {
                continue;
            }
            if rng.random_bool(0.45) {
                edges.insert((i, j));
            }
        }
    }

    let mut out = edges.into_iter().collect::<Vec<_>>();
    out.sort_unstable();
    out
}

fn canon_edge(a: usize, b: usize) -> (usize, usize) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

async fn init_and_copy_repo_cluster(root: &std::path::Path) -> Res<Vec<PathBuf>> {
    let paths = (0..NODE_COUNT)
        .map(|idx| root.join(format!("repo-{idx}")))
        .collect::<Vec<_>>();
    tokio::fs::create_dir_all(&paths[0]).await?;
    let rtx = RepoCtx::init(
        &paths[0],
        RepoOpenOptions {
            ws_connector_url: None,
        },
        "stress-test-device".into(),
    )
    .await?;
    let source_repo_id = rtx.repo_id.clone();
    let source_app_doc_id = rtx.doc_app.document_id().clone();
    let source_drawer_doc_id = rtx.doc_drawer.document_id().clone();
    rtx.shutdown().await?;
    drop(rtx);

    let seed_node = open_sync_node(&paths[0]).await?;
    let ticket = seed_node.sync_repo.get_ticket_url().await?;
    for dst in paths.iter().skip(1) {
        bootstrap_clone_repo_from_url_for_tests(&ticket, dst).await?;

        let ctx = RepoCtx::open(
            dst,
            RepoOpenOptions {
                ws_connector_url: None,
            },
            "stress-test-device".into(),
        )
        .await?;
        if ctx.repo_id != source_repo_id {
            eyre::bail!(
                "stress init repo_id mismatch after clone (source={}, cloned={})",
                source_repo_id,
                ctx.repo_id
            );
        }
        if ctx.doc_app.document_id() != &source_app_doc_id {
            eyre::bail!(
                "stress init app doc mismatch after clone (source={}, cloned={})",
                source_app_doc_id,
                ctx.doc_app.document_id()
            );
        }
        if ctx.doc_drawer.document_id() != &source_drawer_doc_id {
            eyre::bail!(
                "stress init drawer doc mismatch after clone (source={}, cloned={})",
                source_drawer_doc_id,
                ctx.doc_drawer.document_id()
            );
        }
        ctx.shutdown().await?;
    }
    seed_node.stop().await?;
    Ok(paths)
}

async fn open_cluster_nodes(paths: &[PathBuf]) -> Res<Vec<Option<SyncTestNode>>> {
    let mut out = Vec::with_capacity(paths.len());
    for path in paths {
        out.push(Some(open_sync_node(path).await?));
    }
    Ok(out)
}

async fn connect_topology(
    nodes: &[Option<SyncTestNode>],
    edges: &[(usize, usize)],
) -> Res<Vec<HashSet<EndpointId>>> {
    let mut endpoint_sets = vec![HashSet::<EndpointId>::new(); NODE_COUNT];
    for (a, b) in edges {
        let node_a = nodes[*a].as_ref().ok_or_eyre("node missing while connecting")?;
        let node_b = nodes[*b].as_ref().ok_or_eyre("node missing while connecting")?;

        let ticket_b = node_b.sync_repo.get_ticket_url().await?;
        let bootstrap_ab = node_a.sync_repo.connect_url(&ticket_b).await?;
        endpoint_sets[*a].insert(bootstrap_ab.endpoint_id);

        let ticket_a = node_a.sync_repo.get_ticket_url().await?;
        let bootstrap_ba = node_b.sync_repo.connect_url(&ticket_a).await?;
        endpoint_sets[*b].insert(bootstrap_ba.endpoint_id);
    }
    Ok(endpoint_sets)
}

async fn wait_network_rest(
    nodes: &[Option<SyncTestNode>],
    endpoint_sets: &[HashSet<EndpointId>],
    timeout: Duration,
) -> Res<()> {
    for (idx, node_opt) in nodes.iter().enumerate() {
        let Some(node) = node_opt.as_ref() else {
            continue;
        };
        let peers = endpoint_sets[idx].iter().cloned().collect::<Vec<_>>();
        if !peers.is_empty() {
            node.sync_repo.wait_for_full_sync(&peers, timeout).await?;
        }
    }

    let active = nodes
        .iter()
        .enumerate()
        .filter_map(|(idx, node)| node.as_ref().map(|node| (idx, node)))
        .collect::<Vec<_>>();

    for i in 0..active.len() {
        for j in (i + 1)..active.len() {
            let left = active[i].1;
            let right = active[j].1;
            wait_for_doc_set_parity(&left.drawer, &right.drawer, timeout).await?;
            assert_doc_head_parity(left, right).await?;
        }
    }

    assert_blob_parity(nodes, timeout).await?;
    Ok(())
}

async fn assert_doc_head_parity(left: &SyncTestNode, right: &SyncTestNode) -> Res<()> {
    let left_snapshot = collect_doc_branch_heads(left).await?;
    let right_snapshot = collect_doc_branch_heads(right).await?;
    if left_snapshot != right_snapshot {
        let left_only = left_snapshot
            .keys()
            .filter(|key| !right_snapshot.contains_key(*key))
            .take(12)
            .cloned()
            .collect::<Vec<_>>();
        let right_only = right_snapshot
            .keys()
            .filter(|key| !left_snapshot.contains_key(*key))
            .take(12)
            .cloned()
            .collect::<Vec<_>>();
        let mismatched = left_snapshot
            .iter()
            .filter_map(|(key, left_heads)| {
                right_snapshot.get(key).and_then(|right_heads| {
                    if left_heads == right_heads {
                        None
                    } else {
                        Some((key.clone(), left_heads.clone(), right_heads.clone()))
                    }
                })
            })
            .take(12)
            .collect::<Vec<_>>();
        eyre::bail!(
            "doc head parity mismatch: left_docs={} right_docs={} left_only={left_only:?} right_only={right_only:?} mismatched={mismatched:?}",
            left_snapshot.len(),
            right_snapshot.len(),
        );
    }
    Ok(())
}

async fn collect_doc_branch_heads(
    node: &SyncTestNode,
) -> Res<BTreeMap<(String, String), Vec<String>>> {
    let mut out = BTreeMap::new();
    let (_, ids) = node.drawer.list_just_ids().await?;
    let mut doc_ids = ids.into_iter().collect::<Vec<_>>();
    doc_ids.sort_unstable();
    for doc_id in doc_ids {
        let Some(branches) = node.drawer.get_doc_branches(&doc_id).await? else {
            continue;
        };
        let mut branch_names = branches.branches.keys().cloned().collect::<Vec<_>>();
        branch_names.sort_unstable();
        for branch_name in branch_names {
            let branch = daybook_types::doc::BranchPath::from(branch_name.as_str());
            let Some((_doc, heads)) = node.drawer.get_with_heads(&doc_id, &branch, None).await?
            else {
                eyre::bail!(
                    "missing branch heads while collecting stress snapshot: doc_id={} branch={}",
                    doc_id,
                    branch_name
                );
            };
            let mut serialized_heads = heads.iter().map(ToString::to_string).collect::<Vec<_>>();
            serialized_heads.sort_unstable();
            out.insert((doc_id.clone(), branch_name), serialized_heads);
        }
    }
    Ok(out)
}

async fn assert_blob_parity(nodes: &[Option<SyncTestNode>], timeout: Duration) -> Res<()> {
    let active = nodes
        .iter()
        .filter_map(|node| node.as_ref())
        .collect::<Vec<_>>();
    if active.is_empty() {
        return Ok(());
    }
    let expected = collect_blob_hashes(active[0]).await?;
    for node in active.iter().skip(1) {
        let hashes = collect_blob_hashes(node).await?;
        if hashes != expected {
            eyre::bail!(
                "blob hash parity mismatch: expected={} got={}",
                expected.len(),
                hashes.len()
            );
        }
    }
    for node in active {
        for hash in &expected {
            let _ = wait_for_blob_bytes(&node.blobs_repo, hash, timeout).await?;
        }
    }
    Ok(())
}

async fn collect_blob_hashes(node: &SyncTestNode) -> Res<HashSet<String>> {
    let mut out = HashSet::new();
    let (_, ids) = node.drawer.list_just_ids().await?;
    for doc_id in ids {
        let Some(doc) = node
            .drawer
            .get_doc_with_facets_at_branch(
                &doc_id,
                &daybook_types::doc::BranchPath::from("main"),
                None,
            )
            .await?
        else {
            continue;
        };
        let Some(raw) = doc
            .facets
            .get(&FacetKey::from(WellKnownFacetTag::Blob))
            .cloned()
        else {
            continue;
        };
        let wk = daybook_types::doc::WellKnownFacet::from_json(raw, WellKnownFacetTag::Blob)?;
        if let daybook_types::doc::WellKnownFacet::Blob(blob) = wk {
            out.insert(blob.digest);
        }
    }
    Ok(out)
}

async fn apply_event(
    node: &SyncTestNode,
    kind: EventKind,
    idx: usize,
    rng: &mut StdRng,
) -> Res<Option<String>> {
    match kind {
        EventKind::CreateDoc => {
            let mut facets = std::collections::HashMap::new();
            facets.insert(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                FacetRaw::from(WellKnownFacet::TitleGeneric(format!("stress-doc-{idx}"))),
            );
            let id = node
                .drawer
                .add(AddDocArgs {
                    branch_path: daybook_types::doc::BranchPath::from("main"),
                    facets,
                    user_path: Some(daybook_types::doc::UserPath::from(
                        node.ctx.local_user_path.clone(),
                    )),
                })
                .await?;
            Ok(Some(format!("created doc {id}")))
        }
        EventKind::ModifyDoc => {
            let Some(doc_id) = pick_doc_id(node, rng).await? else {
                return Ok(None);
            };
            let branch = daybook_types::doc::BranchPath::from("main");
            let Some((_doc, heads)) = node.drawer.get_with_heads(&doc_id, &branch, None).await? else {
                return Ok(None);
            };
            let mut facets_set = std::collections::HashMap::new();
            facets_set.insert(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                FacetRaw::from(WellKnownFacet::TitleGeneric(format!("mut-{idx}-{}", rng.random::<u64>()))),
            );
            node.drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.clone(),
                        facets_set,
                        facets_remove: vec![],
                        user_path: Some(daybook_types::doc::UserPath::from(
                            node.ctx.local_user_path.clone(),
                        )),
                    },
                    branch,
                    Some(heads),
                )
                .await?;
            Ok(Some(format!("modified doc {doc_id}")))
        }
        EventKind::CreateBranch => {
            let Some(doc_id) = pick_doc_id(node, rng).await? else {
                return Ok(None);
            };
            let new_branch = daybook_types::doc::BranchPath::from(format!(
                "/stress/{}",
                rng.random_range(0..32)
            ));
            let Some((_doc, main_heads)) = node
                .drawer
                .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
                .await?
            else {
                return Ok(None);
            };
            let mut facets_set = std::collections::HashMap::new();
            facets_set.insert(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                FacetRaw::from(WellKnownFacet::TitleGeneric(format!("branch-create-{idx}"))),
            );
            node.drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.clone(),
                        facets_set,
                        facets_remove: vec![],
                        user_path: Some(daybook_types::doc::UserPath::from(
                            node.ctx.local_user_path.clone(),
                        )),
                    },
                    new_branch.clone(),
                    Some(main_heads),
                )
                .await?;
            Ok(Some(format!("created branch {new_branch} on {doc_id}")))
        }
        EventKind::ModifyBranch => {
            let Some((doc_id, branch)) = pick_doc_and_branch(node, rng).await? else {
                return Ok(None);
            };
            let Some((_doc, heads)) = node.drawer.get_with_heads(&doc_id, &branch, None).await? else {
                return Ok(None);
            };
            let mut facets_set = std::collections::HashMap::new();
            facets_set.insert(
                FacetKey::from(WellKnownFacetTag::TitleGeneric),
                FacetRaw::from(WellKnownFacet::TitleGeneric(format!("branch-mod-{idx}"))),
            );
            node.drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.clone(),
                        facets_set,
                        facets_remove: vec![],
                        user_path: Some(daybook_types::doc::UserPath::from(
                            node.ctx.local_user_path.clone(),
                        )),
                    },
                    branch.clone(),
                    Some(heads),
                )
                .await?;
            Ok(Some(format!("modified branch {branch} on {doc_id}")))
        }
        EventKind::DeleteBranch => {
            let Some((doc_id, branch)) = pick_doc_and_non_main_branch(node, rng).await? else {
                return Ok(None);
            };
            let deleted = node.drawer.delete_branch(&doc_id, &branch, None).await?;
            if !deleted {
                return Ok(None);
            }
            Ok(Some(format!("deleted branch {branch} on {doc_id}")))
        }
        EventKind::PutBlobAttach => {
            let Some(doc_id) = pick_doc_id(node, rng).await? else {
                return Ok(None);
            };
            let branch = daybook_types::doc::BranchPath::from("main");
            let Some((_doc, heads)) = node.drawer.get_with_heads(&doc_id, &branch, None).await? else {
                return Ok(None);
            };
            let payload = format!("blob-stress-{idx}-{}", rng.random::<u64>()).into_bytes();
            let hash = node.blobs_repo.put(&payload).await?;
            let mut facets_set = std::collections::HashMap::new();
            facets_set.insert(
                FacetKey::from(WellKnownFacetTag::Blob),
                FacetRaw::from(WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".into(),
                    length_octets: payload.len() as u64,
                    digest: hash.clone(),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                })),
            );
            node.drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_id.clone(),
                        facets_set,
                        facets_remove: vec![],
                        user_path: Some(daybook_types::doc::UserPath::from(
                            node.ctx.local_user_path.clone(),
                        )),
                    },
                    branch,
                    Some(heads),
                )
                .await?;
            Ok(Some(format!("attached blob {hash} to {doc_id}")))
        }
    }
}

async fn pick_doc_id(node: &SyncTestNode, rng: &mut StdRng) -> Res<Option<String>> {
    let mut docs = list_doc_ids(&node.drawer).await?.into_iter().collect::<Vec<_>>();
    if docs.is_empty() {
        return Ok(None);
    }
    docs.shuffle(rng);
    Ok(docs.first().cloned())
}

async fn pick_doc_and_branch(
    node: &SyncTestNode,
    rng: &mut StdRng,
) -> Res<Option<(String, daybook_types::doc::BranchPath)>> {
    let Some(doc_id) = pick_doc_id(node, rng).await? else {
        return Ok(None);
    };
    let Some(branches) = node.drawer.get_doc_branches(&doc_id).await? else {
        return Ok(None);
    };
    let mut names = branches.branches.keys().cloned().collect::<Vec<_>>();
    if names.is_empty() {
        return Ok(None);
    }
    names.shuffle(rng);
    Ok(Some((doc_id, daybook_types::doc::BranchPath::from(names[0].clone()))))
}

async fn pick_doc_and_non_main_branch(
    node: &SyncTestNode,
    rng: &mut StdRng,
) -> Res<Option<(String, daybook_types::doc::BranchPath)>> {
    let mut docs = list_doc_ids(&node.drawer).await?.into_iter().collect::<Vec<_>>();
    docs.shuffle(rng);
    for doc_id in docs {
        let Some(branches) = node.drawer.get_doc_branches(&doc_id).await? else {
            continue;
        };
        let mut non_main = branches
            .branches
            .keys()
            .filter(|name| name.as_str() != "main")
            .cloned()
            .collect::<Vec<_>>();
        if non_main.is_empty() {
            continue;
        }
        non_main.shuffle(rng);
        return Ok(Some((
            doc_id,
            daybook_types::doc::BranchPath::from(non_main[0].clone()),
        )));
    }
    Ok(None)
}
