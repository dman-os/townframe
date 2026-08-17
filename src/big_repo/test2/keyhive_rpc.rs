//! Keyhive RPC change-notification contract.
//!
//! The cluster's only incremental trigger for a peer to pull a creator's new
//! Keyhive state is the `SubscribeKeyhiveChanges` RPC stream (`rpc.rs`). The
//! stream is served by the
//! [`KeyhiveChangeNotifier`](crate::runtime2::KeyhiveChangeNotifier) →
//! dispatcher: each change batch is classified once against the published
//! cache snapshot and debounced per destination peer, so bursts collapse into
//! a single payload-free wake-up.
//!
//! These tests pin that contract: any local Keyhive mutation (document
//! creation, CGKA-producing commits) must make the change observable on the
//! RPC stream, and that notification must be sufficient for a connected peer
//! to converge **without** a manual `sync_keyhive_with_peer` barrier. The rest
//! of the suite gates every Keyhive assertion behind an explicit sync call, so
//! a missing notification is invisible there; this module is the exception.

use super::harness::{Pair, Topo, fixtures};
use crate::Res;
use automerge::transaction::Transactable;
use std::time::Duration;
use tokio::time::timeout;
use utils_rs::prelude::EyreOptExt;
/// A freshly created document must make the local Keyhive change observable on
/// the RPC `SubscribeKeyhiveChanges` stream.
#[tokio::test(flavor = "multi_thread")]
async fn create_doc_emits_observable_keyhive_change_notification() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(250, 251, "Creator", "Coparent").await?;

    // Subscribe through the RPC stream — the same path remote peers use. The
    // document is created with the well-known public agent as coparent, so the
    // delegation events are public and the dispatcher selects every connected
    // subscriber (a random RPC client cannot observe private delegations).
    let client_endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .clear_ip_transports()
        .bind_addr((std::net::Ipv4Addr::LOCALHOST, 0))?
        .relay_mode(iroh::RelayMode::Disabled)
        .bind()
        .await?;
    let client =
        crate::rpc::IrohBigRepoRpcClient::new(client_endpoint.clone(), pair.left().endpoint.addr());
    let mut changes = client.subscribe_keyhive_changes(8).await?;
    let ready = timeout(
        utils_rs::scale_timeout(Duration::from_secs(5)),
        changes.recv(),
    )
    .await
    .map_err(|_| crate::ferr!("timed out waiting for RPC subscription readiness"))??
    .ok_or_eyre("RPC stream closed before readiness")?;
    assert!(ready.initial);

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "keyhive-rpc"))
        .map_err(|err| crate::ferr!("failed creating notification doc: {err:?}"))?;
    let owner_doc = pair
        .left()
        .repo
        .create_doc_with_parents(initial, vec![fixtures::public_agent().into()])
        .await?;

    // `create_doc` must produce a notification: a cluster peer only learns to
    // pull the new document through this RPC → sync chain. The event is
    // payload-free; arrival is the assertion.
    timeout(
        utils_rs::scale_timeout(Duration::from_secs(5)),
        changes.recv(),
    )
    .await
    .map_err(|_| {
        crate::ferr!(
            "no observable Keyhive change notification after create_doc — \
                 the cluster cannot learn about the new document"
        )
    })??
    .ok_or_eyre("RPC stream closed before Keyhive change")?;

    drop(owner_doc);
    client_endpoint.close().await;
    Ok(())
}

/// End-to-end: the notification alone (no manual `sync_keyhive_with_peer`)
/// must drive a connected coparent's Keyhive to converge on the new document.
#[tokio::test(flavor = "multi_thread")]
async fn create_doc_notification_drives_peer_keyhive_convergence() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(254, 255, "Creator", "Coparent").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "cluster-converge"))
        .map_err(|err| crate::ferr!("failed creating convergence doc: {err:?}"))?;
    let coparent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let owner_doc = pair
        .left()
        .repo
        .create_doc_with_parents(initial, vec![coparent.into()])
        .await?;
    let doc_id = owner_doc.document_id();

    // No explicit sync_keyhive_with_peer: the coparent's harness
    // `start_keyhive_rpc` task reacts to each RPC notification by pulling the
    // creator's Keyhive. `create_doc` emits two CGKA batches (document
    // creation, then the initial content encryption), so the coparent
    // converges through as many notification-driven pulls as needed. Poll
    // until the structural Keyhive state matches on both sides.
    timeout(Duration::from_secs(15), async {
        loop {
            if super::harness::keyhive::assert_document_snapshot_equal(
                pair.left(),
                pair.right(),
                doc_id,
            )
            .await
            .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| {
        crate::ferr!(
            "coparent Keyhive never converged with the creator through notification-driven \
             syncs — cluster Keyhive convergence is broken"
        )
    })?;

    drop(owner_doc);
    Ok(())
}

/// A peer that pulls a remote Keyhive change must advertise that change to its
/// other peers. Otherwise notification-driven convergence only works across a
/// single edge and connected non-mesh topologies remain permanently stale.
#[tokio::test(flavor = "multi_thread")]
async fn pulled_keyhive_change_is_forwarded_across_line_topology() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let topo = Topo::boot_line(246, 247, 248, "Creator", "Bridge", "Coparent").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "multi-hop-converge"))
        .map_err(|err| crate::ferr!("failed creating multi-hop doc: {err:?}"))?;
    let owner_doc = topo
        .topo_node(0)
        .repo
        .create_doc_with_parents(initial, vec![fixtures::public_agent().into()])
        .await?;
    let doc_id = owner_doc.document_id();

    // No explicit sync after document creation. A's notification makes B pull;
    // B must then forward the change hint so C pulls from B.
    timeout(Duration::from_secs(15), async {
        loop {
            if super::harness::keyhive::assert_document_snapshot_equal(
                topo.topo_node(0),
                topo.topo_node(2),
                doc_id,
            )
            .await
            .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| {
        crate::ferr!(
            "far coparent Keyhive never converged across the notification-driven line topology"
        )
    })?;

    // Forwarding is deliberately coarse, but it must still terminate. In
    // particular, a remotely applied change must not invalidate the
    // receiver's freshly advanced Keyhive syncpoint and echo forever around
    // the connected component.
    for node_index in 0..3 {
        topo.topo_node(node_index)
            .repo
            .wait_for_quiescence(None)
            .await?;
    }

    drop(owner_doc);
    Ok(())
}
