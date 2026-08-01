//! Keyhive RPC change-notification contract.
//!
//! The cluster's only incremental trigger for a peer to pull a creator's new
//! Keyhive state is the `SubscribeKeyhiveChanges` RPC stream (`rpc.rs`), which
//! forwards every `keyhive_change_tx` broadcast as a `KeyhiveChangedRpcEvent`.
//! The broadcast is fired by the
//! [`KeyhiveChangeNotifier`](crate::runtime2::KeyhiveChangeNotifier) — the
//! single funnel that cache-busts the protocol and then notifies peers.
//!
//! These tests pin that contract: any local Keyhive mutation (document
//! creation, CGKA-producing commits) must make the change observable on the
//! broadcast, and that notification must be sufficient for a connected peer to
//! converge **without** a manual `sync_keyhive_with_peer` barrier. The rest of
//! the suite gates every Keyhive assertion behind an explicit sync call, so a
//! missing notification is invisible there; this module is the exception.

use super::harness::{fixtures, Pair};
use crate::Res;
use automerge::transaction::Transactable;
use std::time::Duration;
use tokio::time::timeout;

/// A freshly created document must make the local Keyhive change observable on
/// the broadcast that backs the RPC `SubscribeKeyhiveChanges` stream.
#[tokio::test(flavor = "multi_thread")]
async fn create_doc_emits_observable_keyhive_change_notification() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(252, 253, "Creator", "Coparent").await?;

    // Direct observer on the same broadcast `BigRepo::subscribe_keyhive_changes`
    // that `rpc.rs` forwards to remote peers.
    let mut changes = pair.left().repo.subscribe_keyhive_changes();

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "keyhive-rpc"))
        .map_err(|err| crate::ferr!("failed creating notification doc: {err:?}"))?;
    let coparent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let owner_doc = pair
        .left()
        .repo
        .create_doc_with_parents(initial, vec![coparent.into()])
        .await?;

    // `create_doc` must produce a notification: a cluster peer only learns to
    // pull the new document through this broadcast → RPC → sync chain. The
    // event is payload-free; arrival is the assertion.
    timeout(Duration::from_secs(5), changes.recv())
        .await
        .map_err(|_| {
            crate::ferr!(
                "no observable Keyhive change notification after create_doc — \
                 the cluster cannot learn about the new document"
            )
        })?
        .map_err(|e| crate::ferr!("Keyhive change stream failed: {e}"))?;

    drop(owner_doc);
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
