//! Tier 3 — Topology matrix.
//!
//! Tests exercise document replication and head parity across a range of
//! network topologies. Each topology boots nodes from [`topo::Node`], wires
//! connections along the graph edges, and runs keyhive sync so every peer
//! knows every other peer's agent where the topology permits it. The owner
//! grants Read access to reader nodes and Relay-only access to intermediate
//! nodes; relays store encrypted parts and sedimentree heads but cannot
//! decrypt content.
//!
//! Post-conditions (per Tier 0):
//! - sedimentree-head parity across ALL nodes (relays included);
//! - materialised-head parity across nodes with access;
//! - the reader(s) can read the pre-grant content.
//!
//! # Scenarios
//!
//! | Test                         | Topology            | Nodes           |
//! |------------------------------|---------------------|-----------------|
//! | `relay_replication`          | A ↔ R ↔ B          | 3               |
//! | `line_replication`           | A ↔ B ↔ C          | 3               |
//! | `star_replication`           | hub ↔ leaf1, leaf2 | 3               |
//! | `triangle_replication`       | A↔B, B↔C, C↔A      | 3               |
//! | `partition_then_heal`        | A↔B, partition, heal | 2               |

use super::harness::{fixtures, keyhive as kh_snap, topo::ShutdownGuard, Node, Topo};
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;
// ─── Read helpers ───────────────────────────────────────────────────────────

async fn assert_relay_only(
    repo: &crate::BigRepo,
    relay: &super::harness::Node,
    doc_id: crate::DocumentId,
) -> crate::Res<()> {
    let relay_vk = ed25519_dalek::VerifyingKey::from_bytes(relay.peer_id().as_bytes())
        .map_err(|err| crate::ferr!("relay peer id is not a verifying key: {err}"))?;
    let doc_vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .map_err(|err| crate::ferr!("document id is not a verifying key: {err}"))?;
    let access = repo
        .keyhive()
        .agent_access_on(
            &keyhive_core::principal::identifier::Identifier::from(relay_vk),
            keyhive_core::principal::identifier::Identifier::from(doc_vk),
        )
        .await;
    assert_eq!(access, Some(Access::Relay));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if relay
            .obj_parts_contains(doc_id, crate::GLOBAL_PART_ID)
            .await?
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            return Err(crate::ferr!(
                "Relay access did not place the document in the global fetch partition"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Ok(())
}

async fn read_text(handle: &crate::BigDocHandle, key: &str) -> Option<String> {
    handle
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, key)
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(s) => match s.as_ref() {
                        ScalarValue::Str(v) => Some(v.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await
}

async fn read_title(handle: &crate::BigDocHandle) -> String {
    handle
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "title")
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(s) => match s.as_ref() {
                        ScalarValue::Str(v) => Some(v.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
                .unwrap_or_else(|| panic!("title should exist"))
        })
        .await
}

/// Push doc fragments along a single connection without requiring
/// materialisation (used for relay/intermediate hops).
async fn sync_doc_no_materialize(
    conn: &crate::BigRepoConnection,
    doc_id: crate::DocumentId,
) -> crate::Res<()> {
    conn.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    Ok(())
}

/// Assert sedimentree-head parity across a subset of topology nodes.
async fn assert_sedimentree_parity_across(
    topo: &Topo,
    doc_id: crate::DocumentId,
    indices: &[usize],
) -> crate::Res<()> {
    let mut baseline: Option<Vec<automerge::ChangeHash>> = None;
    for &idx in indices {
        let state = topo.topo_node(idx).repo.doc_head_state(doc_id).await?;
        let mut heads: Vec<_> = state.sedimentree_heads.to_vec();
        heads.sort_by_key(|h| h.0);
        if let Some(ref base) = baseline {
            if &heads != base {
                return Err(crate::ferr!(
                    "sedimentree-heads parity violated at node {} — {:?} vs {:?}",
                    idx,
                    heads,
                    base,
                ));
            }
        } else {
            baseline = Some(heads);
        }
    }
    Ok(())
}

// ─── Relay A ↔ R ↔ B ───────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier3_relay_replication() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    //        ┌─────┐    ┌──────┐    ┌─────┐
    //   A    │  A  │────│  R   │────│  B  │
    // Owner  │(236)│    │(237) │    │(238)│  Reader
    //        └─────┘    └──────┘    └─────┘
    let topo = Topo::boot_relay(236, 237, 238, "Alice", "Relay", "Bob").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "relay-doc"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let a_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = a_doc.document_id();

    // The relay receives only pull/relay capability; B receives the actual
    // Read capability. The relay is never granted decryption access.
    let relay_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, relay_agent, Access::Relay)
        .await?;
    // The public reader is used here because B's individual identity is not
    // directly learned by A across a relay-only connection. This still lets
    // us assert that the relay's own capability remains Relay, not Read.
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Read)
        .await?;

    // Propagate keyhive: A→R, then R→B.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(1, 2).sync_keyhive_with_peer(None).await?;
    assert_relay_only(&topo.topo_node(1).repo, topo.topo_node(1), doc_id).await?;

    // R pulls the doc from A (stores parts, doesn't materialise).
    sync_doc_no_materialize(topo.topo_conn(1, 0), doc_id).await?;
    // Then B pulls from R and materialises.
    let b_doc =
        fixtures::sync_doc_expect_ready(topo.topo_conn(2, 1), &topo.topo_node(2).repo, doc_id)
            .await?;
    assert_eq!(read_title(&b_doc).await, "relay-doc");

    // Tier 0: sedimentree parity across all three nodes.
    assert_sedimentree_parity_across(&topo, doc_id, &[0, 1, 2]).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(2), doc_id).await?;

    drop(a_doc);
    drop(b_doc);
    Ok(())
}

// ─── Relay modes ────────────────────────────────────────────────────────────

/// A relay-only node may pull encrypted content without materializing it.
#[tokio::test(flavor = "multi_thread")]
async fn tier3_pull_only_relay_does_not_materialize() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let topo = Topo::boot_relay(250, 251, 252, "Alice", "PullRelay", "Bob").await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "pull-only"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    let relay_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, relay_agent, Access::Relay)
        .await?;
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(1, 0)
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    assert_relay_only(&topo.topo_node(1).repo, topo.topo_node(1), doc_id).await?;
    let relay_state = topo.topo_node(1).repo.doc_head_state(doc_id).await?;
    assert!(!relay_state.sedimentree_heads.is_empty());
    assert!(relay_state.materialized_heads.is_none());
    drop(owner_doc);
    Ok(())
}

/// A relay with Read access can materialize, unlike a Relay-only node.
#[tokio::test(flavor = "multi_thread")]
async fn tier3_read_only_relay_materializes_without_edit_access() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let topo = Topo::boot_relay(253, 254, 255, "Alice", "ReadRelay", "Bob").await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "read-only-relay"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    let relay_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, relay_agent, Access::Read)
        .await?;
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    let relay_doc =
        fixtures::sync_doc_expect_ready(topo.topo_conn(1, 0), &topo.topo_node(1).repo, doc_id)
            .await?;
    assert_eq!(read_title(&relay_doc).await, "read-only-relay");
    let relay_vk = ed25519_dalek::VerifyingKey::from_bytes(topo.topo_node(1).peer_id().as_bytes())
        .map_err(|err| crate::ferr!("relay peer id is not a verifying key: {err}"))?;
    let doc_vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .map_err(|err| crate::ferr!("document id is not a verifying key: {err}"))?;
    assert_eq!(
        topo.topo_node(1)
            .repo
            .keyhive()
            .agent_access_on(
                &keyhive_core::principal::identifier::Identifier::from(relay_vk),
                keyhive_core::principal::identifier::Identifier::from(doc_vk),
            )
            .await,
        Some(Access::Read),
        "read-only relay must have Read, not Relay or Edit, capability"
    );
    drop(owner_doc);
    drop(relay_doc);
    Ok(())
}

// ─── Line A ↔ B ↔ C ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier3_line_replication() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    //   ┌─────┐    ┌─────┐    ┌─────┐
    //   │  A  │────│  B  │────│  C  │
    //   │(239)│    │(240)│    │(241)│
    //   └─────┘    └─────┘    └─────┘
    let topo = Topo::boot_line(239, 240, 241, "Alice", "Bob", "Carol").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "line-doc"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let a_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = a_doc.document_id();

    // B is only an encrypted-content relay; C receives Read access.
    let b_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, b_agent, Access::Relay)
        .await?;
    // As with the relay case, C's identity is not directly learned by A;
    // use the public reader while asserting B remains Relay-only.
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Read)
        .await?;

    // Propagate keyhive along the line.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(1, 2).sync_keyhive_with_peer(None).await?;
    assert_relay_only(&topo.topo_node(1).repo, topo.topo_node(1), doc_id).await?;

    // B pulls the doc from A (stores parts, doesn't materialise).
    sync_doc_no_materialize(topo.topo_conn(1, 0), doc_id).await?;
    // Then C pulls from B and materialises.
    let c_doc =
        fixtures::sync_doc_expect_ready(topo.topo_conn(2, 1), &topo.topo_node(2).repo, doc_id)
            .await?;
    assert_eq!(read_title(&c_doc).await, "line-doc");

    // Tier 0: sedimentree parity across all three nodes.
    assert_sedimentree_parity_across(&topo, doc_id, &[0, 1, 2]).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(2), doc_id).await?;

    drop(a_doc);
    drop(c_doc);
    Ok(())
}

// ─── Star hub ↔ leaf ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier3_star_replication() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    //        ┌──────┐
    //   L1 ──│ HUB  │── L2
    //  (243) │(242) │ (244)
    //        └──────┘
    let topo = Topo::boot_star(242, 243, 244, "Hub", "Leaf1", "Leaf2").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "star-doc"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let hub_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = hub_doc.document_id();

    // Grant both leaves access via the hub.
    let leaf1_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    let leaf2_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(2)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, leaf1_agent, Access::Read)
        .await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, leaf2_agent, Access::Read)
        .await?;

    // Propagate keyhive hub→leaf1, hub→leaf2.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(0, 2).sync_keyhive_with_peer(None).await?;

    // Leaves pull the doc from the hub.
    let leaf1_doc_l =
        fixtures::sync_doc_expect_ready(topo.topo_conn(1, 0), &topo.topo_node(1).repo, doc_id)
            .await?;
    let leaf2_doc_l =
        fixtures::sync_doc_expect_ready(topo.topo_conn(2, 0), &topo.topo_node(2).repo, doc_id)
            .await?;
    assert_eq!(read_title(&leaf1_doc_l).await, "star-doc");
    assert_eq!(read_title(&leaf2_doc_l).await, "star-doc");

    assert_sedimentree_parity_across(&topo, doc_id, &[0, 1, 2]).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(1), doc_id).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(2), doc_id).await?;

    drop(hub_doc);
    drop(leaf1_doc_l);
    drop(leaf2_doc_l);
    Ok(())
}

// ─── Triangle (full mesh, 3 nodes) ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier3_triangle_replication() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    //   A ────── B
    //   │(245)  (246)│
    //   │             │
    //   └───── C ─────┘
    //        (247)
    // Each node is directly connected to the other two.
    let topo = Topo::boot_triangle(245, 246, 247, "Alice", "Bob", "Carol").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "triangle-doc"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let a_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = a_doc.document_id();

    // All nodes know each other from the triangle keyhive syncs at boot.
    let b_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    let c_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(2)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, b_agent, Access::Read)
        .await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, c_agent, Access::Read)
        .await?;

    // Sync keyhive along all edges.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(1, 2).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(2, 0).sync_keyhive_with_peer(None).await?;

    // B pulls from A, C pulls from A.
    let b_doc =
        fixtures::sync_doc_expect_ready(topo.topo_conn(1, 0), &topo.topo_node(1).repo, doc_id)
            .await?;
    let c_doc =
        fixtures::sync_doc_expect_ready(topo.topo_conn(2, 0), &topo.topo_node(2).repo, doc_id)
            .await?;
    assert_eq!(read_title(&b_doc).await, "triangle-doc");
    assert_eq!(read_title(&c_doc).await, "triangle-doc");

    assert_sedimentree_parity_across(&topo, doc_id, &[0, 1, 2]).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(1), doc_id).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(2), doc_id).await?;

    drop(a_doc);
    drop(b_doc);
    drop(c_doc);
    Ok(())
}

// ─── Partial mesh ──────────────────────────────────────────────────────────

/// A four-node cycle is a partial mesh: every node has a route to every other
/// node, but no node has a direct edge to all peers. The document is pulled
/// hop-by-hop across one side of the cycle.
#[tokio::test(flavor = "multi_thread")]
async fn tier3_partial_mesh_replication() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let a = Node::boot(20, "Alice").await?;
    let b = Node::boot(21, "Bob").await?;
    let c = Node::boot(22, "Carol").await?;
    let d = Node::boot(23, "Dora").await?;

    let a_b = a.connect(&b).await?;
    let b_a = b.accepted_connection().await;
    let b_c = b.connect(&c).await?;
    let c_b = c.accepted_connection().await;
    let c_d = c.connect(&d).await?;
    let d_c = d.accepted_connection().await;
    let d_a = d.connect(&a).await?;
    let a_d = a.accepted_connection().await;
    let guard = ShutdownGuard::from(vec![a, b, c, d]);

    for conn in [&a_b, &b_c, &c_d, &d_a] {
        conn.sync_keyhive_with_peer(None).await?;
    }
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "partial-mesh"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Read)
        .await?;
    a_b.sync_keyhive_with_peer(None).await?;
    b_c.sync_keyhive_with_peer(None).await?;
    c_d.sync_keyhive_with_peer(None).await?;

    // Pull only along A→B→C→D; the D↔A edge is an alternate route that is
    // deliberately not used for this transfer.
    b_a.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    c_b.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    let d_doc = fixtures::sync_doc_expect_ready(&d_c, &guard.node(3).repo, doc_id).await?;
    assert_eq!(read_title(&d_doc).await, "partial-mesh");

    let mut baseline = guard
        .node(0)
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .to_vec();
    baseline.sort_by_key(|head| head.0);
    for idx in 1..4 {
        let mut heads = guard
            .node(idx)
            .repo
            .doc_head_state(doc_id)
            .await?
            .sedimentree_heads
            .to_vec();
        heads.sort_by_key(|head| head.0);
        assert_eq!(heads, baseline, "partial-mesh heads diverged at node {idx}");
    }
    drop(owner_doc);
    drop(d_doc);
    Ok(())
}

// ─── Partition-then-heal ───────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier3_partition_then_heal() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    // Start:  A(owner) ↔ B(reader)  (248, 249)
    // Partition:  A    ‖    B  (remove big_sync routes, close conn)
    // Owner writes offline, heal: reconnect, verify convergence.
    use crate::test2::harness::topo::{Node, ShutdownGuard};

    let guard = ShutdownGuard::from(vec![
        Node::boot(248, "Alice").await?,
        Node::boot(249, "Bob").await?,
    ]);
    let a = guard.node(0);
    let b = guard.node(1);

    // Connect A↔B.
    let a_b_conn = a.connect(b).await?;
    let b_a_conn = b.accepted_connection().await;
    a_b_conn.sync_keyhive_with_peer(None).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "partition-start"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let a_doc = a.repo.create_doc(initial).await?;
    let doc_id = a_doc.document_id();

    // Grant B read access.
    let b_agent = fixtures::agent_of(&a.repo, b).await?;
    a.repo
        .grant_doc_access(doc_id, b_agent, Access::Read)
        .await?;
    a_b_conn.sync_keyhive_with_peer(None).await?;

    // B materialises the initial doc.
    let b_doc = fixtures::sync_doc_expect_ready(&b_a_conn, &b.repo, doc_id).await?;
    assert_eq!(read_title(&b_doc).await, "partition-start");
    drop(b_doc);

    // ── Partition ──────────────────────────────────────────────────────
    a.worker.remove_peer(b.peer_id()).await?;
    b.worker.remove_peer(a.peer_id()).await?;
    drop(a_b_conn);
    drop(b_a_conn);

    // Owner writes while partitioned.
    a_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "partitioned-write"))
                .map_err(|err| crate::ferr!("failed writing while partitioned: {err:?}"))
        })
        .await??;

    // ── Heal ───────────────────────────────────────────────────────────
    let a_b_conn2 = a.connect(b).await?;
    let b_a_conn2 = b.accepted_connection().await;
    a_b_conn2.sync_keyhive_with_peer(None).await?;

    let b_doc2 = fixtures::sync_doc_expect_ready(&b_a_conn2, &b.repo, doc_id).await?;
    let phase = b_doc2
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "phase")
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(s) => match s.as_ref() {
                        ScalarValue::Str(v) => Some(v.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await;
    assert_eq!(
        phase.as_deref(),
        Some("partitioned-write"),
        "reader must see content written during partition"
    );

    // Tier 0: sedimentree parity.
    let a_state = a.repo.doc_head_state(doc_id).await?;
    let b_state = b.repo.doc_head_state(doc_id).await?;
    let (mut a_heads, mut b_heads) = (
        a_state.sedimentree_heads.to_vec(),
        b_state.sedimentree_heads.to_vec(),
    );
    a_heads.sort_by_key(|h| h.0);
    b_heads.sort_by_key(|h| h.0);
    assert_eq!(
        a_heads, b_heads,
        "sedimentree heads must converge after partition heal"
    );

    drop(a_doc);
    drop(b_doc2);
    drop(guard);
    Ok(())
}

// ========================================================================
// Polish: duplicate-delivery, opposite-order membership/payload
// ========================================================================

// ─── Duplicate delivery through mesh paths ───────────────────────────────
//
// A document arrives at node D through two independent paths (direct A↔D
// and indirect A↔B↔C↔D).  Duplicate deliveries must not produce duplicate
// commits or divergent sedimentree heads.
#[tokio::test(flavor = "multi_thread")]
async fn tier3_duplicate_delivery_harmless() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let a = Node::boot(10, "Alice").await?;
    let b = Node::boot(11, "Bob").await?;
    let c = Node::boot(12, "Carol").await?;
    let d = Node::boot(13, "Dora").await?;

    let a_b = a.connect(&b).await?;
    let b_a = b.accepted_connection().await;
    let b_c = b.connect(&c).await?;
    let c_b = c.accepted_connection().await;
    let c_d = c.connect(&d).await?;
    let d_c = d.accepted_connection().await;
    let d_a = d.connect(&a).await?;
    let a_d = a.accepted_connection().await;
    let guard = ShutdownGuard::from(vec![a, b, c, d]);

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "dup-delivery"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant via public agent so all reachable peers can read.
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Read)
        .await?;

    // Sync keyhive along all edges so the public-agent grant propagates.
    for conn in [&a_b, &b_c, &c_d, &d_a, &b_a, &c_b, &d_c, &a_d] {
        conn.sync_keyhive_with_peer(None).await?;
    }

    // Path 1: A→B→C→D.
    b_a.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    c_b.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    d_c.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    guard
        .node(3)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // Path 2: A→D (direct).  This sends the same doc again.  D must
    // converge without duplication.
    a_d.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    guard
        .node(3)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // All four nodes must have identical sedimentree heads.
    let mut baseline = guard
        .node(0)
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .to_vec();
    baseline.sort_by_key(|h| h.0);
    for idx in 1..4 {
        let mut heads = guard
            .node(idx)
            .repo
            .doc_head_state(doc_id)
            .await?
            .sedimentree_heads
            .to_vec();
        heads.sort_by_key(|h| h.0);
        assert_eq!(
            heads, baseline,
            "sedimentree heads diverged at node {idx} after duplicate delivery"
        );
    }

    // D can materialise.
    let d_doc = guard
        .node(3)
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    assert_eq!(read_title(&d_doc).await, "dup-delivery");
    drop(d_doc);
    drop(owner_doc);
    drop(guard);
    Ok(())
}

// ─── Multi-hop opposite-order membership/payload ─────────────────────────
//
// In a line A↔B↔C, C receives the doc payload (through B) BEFORE the
// keyhive membership grant from A arrives.  After the membership sync,
// C must be able to materialise (payload-first ordering).
#[tokio::test(flavor = "multi_thread")]
async fn tier3_opposite_order_membership_payload() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    //   ┌─────┐    ┌─────┐    ┌─────┐
    //   │  A  │────│  B  │────│  C  │
    //   │(30) │    │(31) │    │(32) │
    //   └─────┘    └─────┘    └─────┘
    let topo = Topo::boot_line(30, 31, 32, "Alice", "Bob", "Carol").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "opposite-order"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let a_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = a_doc.document_id();

    // Grant B as relay and C as Reader via public agent (same pattern as
    // the existing relay/line tests where the far-end agent is not directly
    // learned by the owner across a multi-hop connection).
    let b_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, b_agent, Access::Relay)
        .await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Read)
        .await?;

    // Sync keyhive A↔B.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_conn(1, 0).sync_keyhive_with_peer(None).await?;

    // B pulls the doc payload from A (stores encrypted parts).
    sync_doc_no_materialize(topo.topo_conn(1, 0), doc_id).await?;

    // C asks B for the doc payload BEFORE receiving membership. Subduction
    // rejects the incoming payload because C has no local document policy;
    // the rejection must be structured and non-fatal.
    let policy_error = topo
        .topo_conn(2, 1)
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await
        .expect_err("missing local Keyhive document must reject the payload");
    assert!(matches!(
        policy_error,
        crate::SyncDocError::Policy(crate::SyncDocPolicyError::DocumentNotFound)
    ));

    topo.topo_node(2)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // C must not be materialized before membership arrives.
    let c_lookup = topo.topo_node(2).repo.get_doc(&doc_id).await?;
    assert!(
        !matches!(c_lookup, crate::DocLookup::Ready(_)),
        "C must not materialise before membership arrives"
    );

    // Now sync membership from B→C.  C learns about Read access.
    topo.topo_conn(1, 2).sync_keyhive_with_peer(None).await?;

    // C must be able to materialize now.
    let c_doc =
        fixtures::sync_doc_expect_ready(topo.topo_conn(2, 1), &topo.topo_node(2).repo, doc_id)
            .await?;
    assert_eq!(
        read_title(&c_doc).await,
        "opposite-order",
        "C must materialise after payload-first delivery then membership arrival"
    );

    // Tier 0: sedimentree parity across the line.
    assert_sedimentree_parity_across(&topo, doc_id, &[0, 1, 2]).await?;
    kh_snap::assert_document_snapshot_equal(topo.topo_node(0), topo.topo_node(2), doc_id).await?;

    drop(a_doc);
    drop(c_doc);
    Ok(())
}

// ========================================================================
// Polish batch: store-and-forward relay, partial-mesh partition/heal
// ========================================================================

// ─── Store-and-forward relay ──────────────────────────────────────────────
//
// A relay receives encrypted doc parts before the reader connects.  The
// owner and relay then exchange additional updates while the reader is
// still absent.  When the reader later connects through the relay, syncs
// keyhive, and pulls the doc, they must receive BOTH the initial content
// and the later updates.  The relay remains Relay-only throughout.
#[tokio::test(flavor = "multi_thread")]
async fn tier3_store_and_forward_relay() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();

    let owner = Node::boot(36, "Owner").await?;
    let relay = Node::boot(37, "Relay").await?;
    let reader = Node::boot(38, "Reader").await?;
    let guard = ShutdownGuard::from(vec![owner, relay, reader]);

    // Phase 1: connect A↔R only; reader is not yet on the network.
    let a_r = guard.node(0).connect(guard.node(1)).await?;
    let r_a = guard.node(1).accepted_connection().await;
    a_r.sync_keyhive_with_peer(None).await?;

    // Owner creates doc with initial content.
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "initial"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant R Relay access; grant Read via public agent for the future reader.
    let relay_agent = fixtures::agent_of(&guard.node(0).repo, guard.node(1)).await?;
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, relay_agent, Access::Relay)
        .await?;
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Read)
        .await?;
    a_r.sync_keyhive_with_peer(None).await?;
    // Verify the relay only has Relay access (no Read).
    assert_relay_only(&guard.node(1).repo, guard.node(1), doc_id).await?;

    // R pulls the initial doc from A (stores encrypted parts, doesn't
    // materialize because the relay only has Relay access).
    sync_doc_no_materialize(&r_a, doc_id).await?;
    // R must NOT materialise.
    let r_state = guard.node(1).repo.doc_head_state(doc_id).await?;
    assert!(
        r_state.materialized_heads.is_none(),
        "relay must NOT materialise after first sync"
    );

    // Phase 2: owner writes an update while the reader is still absent.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "update1"))
                .map_err(|err| crate::ferr!("failed update1: {err:?}"))
        })
        .await??;
    // R pulls the update.
    r_a.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    guard
        .node(1)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // Phase 3: reader connects to the relay.
    let r_b = guard.node(1).connect(guard.node(2)).await?;
    let b_r = guard.node(2).accepted_connection().await;
    r_b.sync_keyhive_with_peer(None).await?;
    b_r.sync_keyhive_with_peer(None).await?;

    // Reader pulls the doc from the relay — must get both initial and update1.
    let reader_doc = fixtures::sync_doc_expect_ready(&b_r, &guard.node(2).repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc, "phase").await.as_deref(),
        Some("update1"),
        "reader must see the latest content after store-and-forward"
    );

    // Relay retains Relay-only access and sedimentree heads.
    assert_relay_only(&guard.node(1).repo, guard.node(1), doc_id).await?;
    let relay_state = guard.node(1).repo.doc_head_state(doc_id).await?;
    assert!(!relay_state.sedimentree_heads.is_empty());

    // Sedimentree parity across all three nodes.
    let mut baseline = guard
        .node(0)
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .to_vec();
    baseline.sort_by_key(|h| h.0);
    for idx in 1..3 {
        let mut heads = guard
            .node(idx)
            .repo
            .doc_head_state(doc_id)
            .await?
            .sedimentree_heads
            .to_vec();
        heads.sort_by_key(|h| h.0);
        assert_eq!(
            heads, baseline,
            "sedimentree heads diverged at node {idx} after store-and-forward"
        );
    }

    drop(owner_doc);
    drop(reader_doc);
    drop(a_r);
    drop(r_a);
    drop(r_b);
    drop(b_r);
    drop(guard);
    Ok(())
}

// ─── Partial-mesh partition/heal ─────────────────────────────────────────
//
// A four-node cycle (A↔B↔C↔D↔A) is fully converged.  Two edges (A↔B and
// C↔D) are partitioned simultaneously, splitting the mesh into two halves.
// After healing all edges and syncing, every node converges to the same
// head state (no stale divergent heads).
#[tokio::test(flavor = "multi_thread")]
async fn tier3_partial_mesh_partition_heal() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let a = Node::boot(44, "Alice").await?;
    let b = Node::boot(45, "Bob").await?;
    let c = Node::boot(46, "Carol").await?;
    let d = Node::boot(47, "Dora").await?;
    let guard = ShutdownGuard::from(vec![a, b, c, d]);

    // Full mesh: A↔B↔C↔D↔A.
    let a_b = guard.node(0).connect(guard.node(1)).await?;
    let b_a = guard.node(1).accepted_connection().await;
    let b_c = guard.node(1).connect(guard.node(2)).await?;
    let c_b = guard.node(2).accepted_connection().await;
    let c_d = guard.node(2).connect(guard.node(3)).await?;
    let d_c = guard.node(3).accepted_connection().await;
    let d_a = guard.node(3).connect(guard.node(0)).await?;
    let a_d = guard.node(0).accepted_connection().await;

    for conn in [&a_b, &b_c, &c_d, &d_a] {
        conn.sync_keyhive_with_peer(None).await?;
    }

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "mesh-partition"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Edit access lets the two partition components make independent
    // writes; this test is about merge semantics rather than authorization.
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), Access::Edit)
        .await?;

    for conn in [&a_b, &b_c, &c_d, &d_a] {
        conn.sync_keyhive_with_peer(None).await?;
    }
    b_a.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    c_b.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    d_c.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    for idx in 1..4 {
        guard
            .node(idx)
            .repo
            .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
            .await?;
        let _h = guard
            .node(idx)
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id)?;
    }
    let c_doc = guard
        .node(2)
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;

    // ── Partition: remove A↔B and C↔D ──────────────────────────────────
    guard
        .node(0)
        .worker
        .remove_peer(guard.node(1).peer_id())
        .await?;
    guard
        .node(1)
        .worker
        .remove_peer(guard.node(0).peer_id())
        .await?;
    guard
        .node(2)
        .worker
        .remove_peer(guard.node(3).peer_id())
        .await?;
    guard
        .node(3)
        .worker
        .remove_peer(guard.node(2).peer_id())
        .await?;
    drop(a_b);
    drop(b_a);
    drop(c_d);
    drop(d_c);

    // Both surviving components make independent edits while partitioned.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "owner_branch", "from-a"))
                .map_err(|err| crate::ferr!("owner partition edit: {err:?}"))
        })
        .await??;
    c_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "reader_branch", "from-c"))
                .map_err(|err| crate::ferr!("reader partition edit: {err:?}"))
        })
        .await??;

    // ── Heal: reconnect A↔B and C↔D ────────────────────────────────────
    let a_b2 = guard.node(0).connect(guard.node(1)).await?;
    let b_a2 = guard.node(1).accepted_connection().await;
    let c_d2 = guard.node(2).connect(guard.node(3)).await?;
    let d_c2 = guard.node(3).accepted_connection().await;

    a_b2.sync_keyhive_with_peer(None).await?;
    c_d2.sync_keyhive_with_peer(None).await?;

    // Sync doc across A↔B and C↔D, then the existing B↔C and D↔A edges
    // will propagate everything to all nodes.
    b_a2.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    a_b2.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    d_c2.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    c_d2.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    for idx in 0..4 {
        guard
            .node(idx)
            .repo
            .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
            .await?;
    }
    // Push C's edit to B (B↔C was never partitioned, so use b_c).
    b_c.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    guard.node(1).repo.wait_for_quiescence(None).await?;
    guard.node(2).repo.wait_for_quiescence(None).await?;

    // Second round of healing syncs.
    for conn in [&b_a2, &a_b2, &d_c2, &c_d2, &b_c, &c_b] {
        conn.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
            .await?;
    }
    for idx in 0..4 {
        guard
            .node(idx)
            .repo
            .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
            .await?;
    }

    // Both independent branches must survive the heal.
    for handle in [&owner_doc, &c_doc] {
        assert_eq!(
            read_text(handle, "owner_branch").await.as_deref(),
            Some("from-a")
        );
        assert_eq!(
            read_text(handle, "reader_branch").await.as_deref(),
            Some("from-c")
        );
    }
    for idx in [1, 3] {
        let handle = guard
            .node(idx)
            .repo
            .get_doc(&doc_id)
            .await?
            .into_ready(doc_id)?;
        assert_eq!(
            read_text(&handle, "owner_branch").await.as_deref(),
            Some("from-a")
        );
        assert_eq!(
            read_text(&handle, "reader_branch").await.as_deref(),
            Some("from-c")
        );
    }

    // All four nodes must have identical sedimentree heads after heal.
    let mut baseline = guard
        .node(0)
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .to_vec();
    baseline.sort_by_key(|h| h.0);
    for idx in 1..4 {
        let mut heads = guard
            .node(idx)
            .repo
            .doc_head_state(doc_id)
            .await?
            .sedimentree_heads
            .to_vec();
        heads.sort_by_key(|h| h.0);
        assert_eq!(
            heads, baseline,
            "sedimentree heads diverged at node {idx} after partition heal"
        );
    }

    // Sedimentree parity across all four nodes.
    let mut baseline = guard
        .node(0)
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .to_vec();
    baseline.sort_by_key(|h| h.0);
    for idx in 1..4 {
        let mut heads = guard
            .node(idx)
            .repo
            .doc_head_state(doc_id)
            .await?
            .sedimentree_heads
            .to_vec();
        heads.sort_by_key(|h| h.0);
        assert_eq!(
            heads, baseline,
            "sedimentree heads diverged at node {idx} after partition heal"
        );
    }

    drop(owner_doc);
    drop(c_doc);
    drop(a_d);
    drop(b_c);
    drop(c_b);
    drop(a_b2);
    drop(b_a2);
    drop(c_d2);
    drop(d_c2);
    drop(guard);
    Ok(())
}
