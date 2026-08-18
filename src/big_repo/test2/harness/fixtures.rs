//! Test fixtures: contact-card exchange, access grants, and sync helpers.
//!
//! Helpers here provide precise barriers for keyhive and document sync. Where
//! bounded polling is required (such as `assert_reader_has_access` with a 5-second
//! limit and `sync_doc_expect_ready` with a 15-second limit), failures return
//! diagnostic `Err` results rather than hanging indefinitely.

use super::log_nickname;
use super::topo::{Node, Pair};
use crate::{BigKeyhiveAgent, BigKeyhiveGroup, DocumentId, Res};
use keyhive_core::access::Access;
use std::sync::Arc;
use subduction_keyhive::KeyhivePeerId;

/// Look up `peer`'s agent in `repo`'s keyhive — a single call.
///
/// Valid after [`Pair::boot`] has run the contact-card exchange. A `None` here
/// means the keyhive sync did not actually deliver the agent — a bug.
pub async fn agent_of(repo: &crate::BigRepo, peer: &Node) -> Res<BigKeyhiveAgent> {
    let kh_peer_id = KeyhivePeerId::from_bytes(*peer.peer_id().as_bytes());
    repo.keyhive()
        .get_agent_by_peer_id(&kh_peer_id)
        .await?
        .ok_or_else(|| {
            crate::ferr!(
                "agent for {} not present in {}'s keyhive after contact-card exchange",
                log_nickname::nickname(&peer.peer_id()),
                log_nickname::nickname(&repo.local_peer_id()),
            )
        })
}

/// Construct the well-known public agent in the concrete BigRepo Keyhive type.
/// The public agent is not learned through the contact-card exchange.
pub fn public_agent() -> BigKeyhiveAgent {
    let individual = keyhive_core::principal::public::Public.individual();
    BigKeyhiveAgent::Individual(
        individual.id(),
        Arc::new(futures::lock::Mutex::new(individual)),
    )
}

/// Resolve a BigRepo document as a Keyhive agent. This is used to exercise
/// document-as-member delegation rather than treating the document as a plain
/// individual agent.
pub async fn document_agent(repo: &crate::BigRepo, doc_id: DocumentId) -> Res<BigKeyhiveAgent> {
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .map_err(|_| crate::ferr!("document id is not a valid Keyhive document id"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(verifying_key),
    );
    let document = repo
        .keyhive()
        .clone_keyhive()
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| crate::ferr!("document {doc_id} is not present in Keyhive"))?;
    Ok(BigKeyhiveAgent::Document(kh_doc_id, document))
}

/// Close both connection handles and remove the corresponding big-sync
/// routes. The old right-side handle is dropped before returning so stale
/// connection teardown cannot race the next explicit Keyhive barrier.
pub async fn go_offline(pair: &mut Pair) -> Res<()> {
    pair.disconnect().await?;
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;
    drop(old_right);
    Ok(())
}

/// Grant `access` on `doc_id` to `grantee` (resolved on the owner) and
/// propagate the membership change to the reader via a single bidirectional
/// keyhive sync.
///
/// Post-condition (asserted, not polled): the reader sees its access on the
/// document.
pub async fn grant_and_propagate(
    pair: &Pair,
    doc_id: DocumentId,
    grantee: &BigKeyhiveAgent,
    access: Access,
) -> Res<()> {
    pair.left()
        .repo
        .grant_doc_access(doc_id, grantee.clone(), access)
        .await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    assert_reader_has_access(&pair.right().repo, doc_id).await?;
    super::keyhive::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    Ok(())
}

/// Grant `access` to a group and propagate the resulting Keyhive state.
///
/// This is the group analogue of [`grant_and_propagate`]. The reader's
/// effective access is checked through the transitive group membership path.
pub async fn grant_group_and_propagate(
    pair: &Pair,
    doc_id: DocumentId,
    group: &BigKeyhiveGroup,
    access: Access,
) -> Res<()> {
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), access)
        .await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    assert_reader_has_access(&pair.right().repo, doc_id).await?;
    super::keyhive::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    Ok(())
}

/// Assert the reader's keyhive reflects access on `doc_id` — single lookup.
pub async fn assert_reader_has_access(repo: &crate::BigRepo, doc_id: DocumentId) -> Res<()> {
    let peer = repo.local_peer_id();
    let agent_key = ed25519_dalek::VerifyingKey::from_bytes(peer.as_bytes())
        .expect("peer id must be a verifying key");
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("document id must be a verifying key");
    let agent = keyhive_core::principal::identifier::Identifier::from(agent_key);
    let document = keyhive_core::principal::identifier::Identifier::from(doc_key);
    loop {
        let access = repo.keyhive().agent_access_on(&agent, document).await;
        if access.is_some() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Sync a document and expect it to be fully materialized (Ready) on `repo`.
///
/// One `sync_doc_with_peer` call starts the exchange. Because subduction sends
/// requested commits as fire-and-forget messages, this helper waits for the
/// receiving document to become fully materialized before returning.
pub async fn sync_doc_expect_ready(
    conn: &crate::BigRepoConnection,
    repo: &Arc<crate::BigRepo>,
    doc_id: DocumentId,
) -> Res<crate::BigDocHandle> {
    let receipt = conn.sync_doc_with_peer_receipt(doc_id).await?;
    tracing::debug!(?receipt.outcome, "document sync receipt captured in ready fixture");
    loop {
        match repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(handle) => return Ok(handle),
            crate::DocLookup::PendingMaterialization | crate::DocLookup::Missing => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}
// ─── Bidirectional document sync ─────────────────────────────────────────────

/// Bidirectional document sync: both sides pull from each other, then both
/// repos reach quiescence.  Returns handles from both repos.
///
/// API semantics guarantee that after `A.sync_doc_with_peer(B)` the caller (A)
/// has incorporated B's data, but NOT that B has ingested data A sent.  A
/// single directional sync leaves the non-calling side's sedimentree parity
/// unconstrained.  This helper issues both directions and waits for both repos
/// to settle, so callers can safely assert convergence invariants.
pub async fn sync_doc_bidirectional(
    conn_a_to_b: &crate::BigRepoConnection,
    conn_b_to_a: &crate::BigRepoConnection,
    repo_a: &Arc<crate::BigRepo>,
    repo_b: &Arc<crate::BigRepo>,
    doc_id: DocumentId,
) -> Res<(crate::BigDocHandle, crate::BigDocHandle)> {
    conn_a_to_b.sync_doc_with_peer(doc_id).await?;
    conn_b_to_a.sync_doc_with_peer(doc_id).await?;
    repo_a.wait_for_quiescence(None).await?;
    repo_b.wait_for_quiescence(None).await?;
    let handle_a = expect_ready(repo_a, doc_id).await?;
    let handle_b = expect_ready(repo_b, doc_id).await?;
    Ok((handle_a, handle_b))
}

pub async fn expect_ready(
    repo: &Arc<crate::BigRepo>,
    doc_id: DocumentId,
) -> Res<crate::BigDocHandle> {
    loop {
        match repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(h) => return Ok(h),
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

/// Reach the fixed point of every currently configured BigSync route and all
/// local BigRepo work on `nodes`. A sync round may itself publish new physical
/// document heads (for example a causal healing checkpoint), so one frontier
/// fence is not sufficient.
pub async fn wait_for_network_rest(nodes: &[&super::topo::Node]) -> Res<()> {
    for node in nodes {
        loop {
            let event_tail = node.store.keyhive_event_log_cursor().await?;
            if node.store.keyhive_group_part_cursor().await? >= event_tail {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        node.repo.wait_for_quiescence(None).await?;
    }

    let mut targets = Vec::new();
    for node in nodes {
        let snapshot = node.worker.snapshot().await?;
        tracing::debug!(
            node = %node.repo.local_peer_id(),
            peer_parts = ?snapshot.peer_parts,
            "network-rest BigSync routes"
        );
        for (peer_id, parts) in snapshot.peer_parts {
            targets.push(big_sync::test_support::NetworkRestTarget {
                worker: node.worker.clone(),
                store: Arc::clone(&node.store) as Arc<dyn big_sync::HostPartStore>,
                peer_ids: vec![peer_id],
                part_ids: parts.into_keys().collect(),
            });
        }
    }
    big_sync::test_support::wait_for_network_rest(&targets, || async {
        for node in nodes {
            while {
                let event_tail = node.store.keyhive_event_log_cursor().await?;
                node.store.keyhive_group_part_cursor().await? < event_tail
                    || node.store.causal_checkpoint_cursor().await? < event_tail
            } {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            node.repo.wait_for_quiescence(None).await?;
        }
        Ok(())
    })
    .await
}

/// Convenience wrapper around [`sync_doc_bidirectional`] for a [`Pair`].
/// Returns (left_handle, right_handle).
pub async fn sync_doc_pair(
    pair: &Pair,
    doc_id: DocumentId,
) -> Res<(crate::BigDocHandle, crate::BigDocHandle)> {
    sync_doc_bidirectional(
        pair.left_conn(),
        pair.right_conn(),
        &pair.left().repo,
        &pair.right().repo,
        doc_id,
    )
    .await
}
