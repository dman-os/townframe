//! Keyhive RPC change-notification contract.
//!
//! The cluster's only incremental trigger for a peer to pull a creator's new
//! Keyhive state is the `SubscribeKeyhiveChanges` RPC stream (`rpc.rs`). The
//! stream is served by the durable-incorporation hook → dispatcher: each
//! change batch is classified once against the published
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
                doc_id.clone(),
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
                doc_id.clone(),
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

/// A revocation must reach the peer it revokes through the notification path
/// alone.
///
/// Two load-bearing halves make this work and nothing pins either: Keyhive
/// deliberately emits revocation events *for* an agent that is already fully
/// revoked (so the revoked peer can learn it was revoked), and the dispatcher
/// selects the connected peers a change batch is attributed to. The other
/// notification tests in this module either use the well-known public agent,
/// whose events are visible to every subscriber, or assert on a *creation*
/// batch; none of them pins that a revocation - a batch whose only audience is
/// the agent it revokes - selects that peer at all.
///
/// The connection is established before the document exists and the revoked
/// member is never told to sync explicitly, so the creator's wake-up is the only
/// path available - no connect-time catch-up can cover for a missing
/// notification.
#[tokio::test(flavor = "multi_thread")]
async fn revocation_notification_reaches_the_revoked_member_without_manual_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(228, 229, "Creator", "RevokedMember").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "revocation-notice"))
        .map_err(|err| crate::ferr!("failed creating revocation-notice doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // A real agent, not `fixtures::public_agent()`: the delegation and the
    // revocation naming it are private to the pair, which is the audience the
    // notification classifier has to get right.
    //
    // The revocable shape is a member the creator itself delegated to: a
    // document's genesis members (its coparents) are delegated by the document's
    // own ephemeral signer, which leaves the creator with no revocation proof for
    // them (`RevokeMemberError::NoProof`), so a coparent cannot be revoked by its
    // creator today.
    let revoked_member = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .grant_doc_access(
            doc_id.clone(),
            revoked_member.clone(),
            keyhive_core::access::Access::Read,
        )
        .await?;

    // Non-vacuous precondition: the member must already hold the document's
    // Keyhive state *and* still have access, or the post-revocation assertion
    // below would hold for the wrong reason. The grant reaches it through a
    // notification-driven pull too - no explicit sync was issued for it either.
    wait_for_local_agent_access(&pair.right().repo, doc_id.clone(), true, "RevokedMember").await?;

    pair.left()
        .repo
        .revoke_doc_access(doc_id.clone(), revoked_member)
        .await?;

    // No `sync_keyhive_with_peer` after the revocation: the creator's
    // notification is the only thing that can tell the member to pull, and
    // pulling is the only way it can learn it was revoked.
    wait_for_local_agent_access(&pair.right().repo, doc_id.clone(), false, "RevokedMember").await?;

    // An access lookup can only go empty because a revocation was applied
    // locally, so name that the revocation event itself landed rather than a
    // stale Keyhive view answering for it.
    let snapshot =
        super::harness::keyhive::document_snapshot(&pair.right().repo, doc_id.clone()).await?;
    assert!(
        !snapshot.revocation_heads.is_empty(),
        "the revoked member lost access without holding the revocation that revoked it: \
         {snapshot:?}"
    );

    drop(owner_doc);
    Ok(())
}

/// `repo`'s own agent access on `doc_id`, as that node observes it.
///
/// A node asks about *itself*: its peer id is its agent identity, and the
/// document's Keyhive id derives from the document id - the same lookup
/// `fixtures::assert_reader_has_access` makes, kept here in the direction that
/// needs to observe access *disappearing*.
async fn local_agent_access(
    repo: &crate::BigRepo,
    doc_id: crate::DocumentId,
) -> Res<Option<keyhive_core::access::Access>> {
    use keyhive_core::principal::identifier::Identifier;
    use utils_rs::expect_tags::ERROR_IMPOSSIBLE;

    let agent_key = ed25519_dalek::VerifyingKey::from_bytes(
        &repo.local_peer_id().to_bytes32().expect(ERROR_IMPOSSIBLE),
    )
    .map_err(|_| crate::ferr!("local peer id is not a verifying key"))?;
    let doc_key =
        ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32().expect(ERROR_IMPOSSIBLE))
            .map_err(|_| crate::ferr!("document id is not a verifying key"))?;
    Ok(repo
        .keyhive()
        .agent_access_on(&Identifier::from(agent_key), Identifier::from(doc_key))
        .await)
}

/// Wait until `repo` observes access (`expect_access`) or its absence on
/// `doc_id`, failing with the Keyhive ledger that says which side of the
/// delivery pipeline stalled: never delivered, or delivered but never applied.
async fn wait_for_local_agent_access(
    repo: &crate::BigRepo,
    doc_id: crate::DocumentId,
    expect_access: bool,
    label: &str,
) -> Res<()> {
    let expectation = if expect_access {
        "granted access"
    } else {
        "the revocation that removes its access"
    };
    let deadline = tokio::time::Instant::now() + utils_rs::scale_timeout(Duration::from_secs(20));
    loop {
        let access = local_agent_access(repo, doc_id.clone()).await?;
        if access.is_some() == expect_access {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            let ledger = super::harness::keyhive::describe_ledger(repo).await?;
            return Err(crate::ferr!(
                "{label} never observed {expectation} on {doc_id} without a manual \
                 `sync_keyhive_with_peer`: access={access:?} keyhive_ledger={ledger}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
