//! Test fixtures: contact-card exchange, access grants, and sync helpers.
//!
//! Helpers here provide precise barriers for keyhive and document sync. Where
//! bounded polling is required (such as `assert_reader_has_access` with a 5-second
//! limit and `sync_doc_expect_ready` with a 15-second limit), failures return
//! diagnostic `Err` results rather than hanging indefinitely.

use super::log_nickname;
use super::topo::{Node, Pair};
use crate::{BigKeyhiveAgent, BigKeyhiveGroup, DocumentId, PeerKey, Res};
use keyhive_core::access::Access;
use std::sync::Arc;
use subduction_keyhive::KeyhivePeerId;

/// Look up `peer`'s agent in `repo`'s keyhive — a single call.
///
/// Valid after [`Pair::boot`] has run the contact-card exchange. A `None` here
/// means the keyhive sync did not actually deliver the agent — a bug.
pub async fn agent_of(repo: &crate::BigRepo, peer: &Node) -> Res<BigKeyhiveAgent> {
    let kh_peer_id = KeyhivePeerId::from_bytes(peer.peer_id().to_bytes32());
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

/// Bounded wait for `peer_id`'s agent to reach `repo`'s keyhive.
///
/// A transport connection only *triggers* the keyhive handshake: the
/// contact-card exchange completes asynchronously, so the agent is not
/// necessarily present the moment a connection is accepted. Callers that need
/// the agent right after dialing must wait for it instead of assuming the
/// exchange already ran. Bounded, so a genuine delivery failure still fails.
pub async fn wait_for_agent(repo: &crate::BigRepo, peer_id: PeerKey) -> Res<BigKeyhiveAgent> {
    let kh_peer_id = KeyhivePeerId::from_bytes(peer_id.to_bytes32());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(agent) = repo.keyhive().get_agent_by_peer_id(&kh_peer_id).await? {
            return Ok(agent);
        }
        if std::time::Instant::now() >= deadline {
            return Err(crate::ferr!(
                "agent for {} never reached {}'s keyhive: contact-card exchange did not complete",
                log_nickname::nickname(&peer_id),
                log_nickname::nickname(&repo.local_peer_id()),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
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
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
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
        .grant_doc_access(doc_id.clone(), grantee.clone(), access)
        .await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    assert_reader_has_access(&pair.right().repo, doc_id.clone()).await?;
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
        .grant_doc_access(doc_id.clone(), group.clone(), access)
        .await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    assert_reader_has_access(&pair.right().repo, doc_id.clone()).await?;
    super::keyhive::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    Ok(())
}

/// Whether `repo`'s agent observes access on `doc_id` within `timeout`.
///
/// A short, quiet probe for diagnostics that try several recovery legs: each leg
/// must not pay the full assertion deadline, and the caller decides what the
/// outcome means.
pub(crate) async fn reader_has_access_within(
    repo: &crate::BigRepo,
    doc_id: DocumentId,
    timeout: std::time::Duration,
) -> Res<bool> {
    let peer = repo.local_peer_id();
    let agent_key = ed25519_dalek::VerifyingKey::from_bytes(&peer.to_bytes32())
        .map_err(|_| crate::ferr!("peer id is not a verifying key"))?;
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
        .map_err(|_| crate::ferr!("document id is not a verifying key"))?;
    let agent = keyhive_core::principal::identifier::Identifier::from(agent_key);
    let document = keyhive_core::principal::identifier::Identifier::from(doc_key);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if repo
            .keyhive()
            .agent_access_on(&agent, document)
            .await
            .is_some()
        {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Assert the reader's keyhive reflects access on `doc_id` — single lookup.
pub async fn assert_reader_has_access(repo: &crate::BigRepo, doc_id: DocumentId) -> Res<()> {
    let peer = repo.local_peer_id();
    let agent_key = ed25519_dalek::VerifyingKey::from_bytes(&peer.to_bytes32())
        .expect("peer id must be a verifying key");
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32())
        .expect("document id must be a verifying key");
    let agent = keyhive_core::principal::identifier::Identifier::from(agent_key);
    let document = keyhive_core::principal::identifier::Identifier::from(doc_key);
    // Bounded: a grant that never propagates must name the node and the
    // Keyhive state it is stuck in, not hang the test into its 120s timeout.
    let deadline =
        tokio::time::Instant::now() + utils_rs::scale_timeout(std::time::Duration::from_secs(30));
    let mut next_report = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let access = repo.keyhive().agent_access_on(&agent, document).await;
        if access.is_some() {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        let (_, known) = describe_keyhive_state(repo, doc_id.clone()).await;
        if now >= next_report {
            next_report = now + std::time::Duration::from_secs(5);
            tracing::warn!(
                %doc_id,
                keyhive_knows_doc = known,
                nickname = %log_nickname::nickname(&peer),
                "waiting for reader to observe granted access"
            );
        }
        if now >= deadline {
            // Name the missing link in the grant chain: whether the agent is a
            // member of any group, and what access the Keyhive reports per
            // document. "Knows the doc but no access" and "not a member of the
            // granting group" are different defects.
            let docs_for_agent = repo.keyhive().docs_for_agent(&agent).await;
            let doc_access = docs_for_agent.get(&doc_id).copied();
            let membered = repo.keyhive().membered_for_agent(&agent).await.len();
            let ledger = super::keyhive::describe_ledger(repo).await?;
            return Err(crate::ferr!(
                "reader never observed granted access: keyhive_knows_doc={known} \
                 doc_access_for_agent={doc_access:?} docs_for_agent={} membered={membered} \
                 keyhive_ledger={ledger} node={}",
                docs_for_agent.len(),
                log_nickname::nickname(&peer)
            ));
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
    let receipt = conn.sync_doc_with_peer_receipt(doc_id.clone()).await?;
    tracing::debug!(?receipt.outcome, "document sync receipt captured in ready fixture");
    // Bounded, and the receipt outcome is carried into the failure: `Stored`
    // with a `None` access means the grant never reached this node, while
    // `Pending` means the content is here but its keys are not.
    let deadline =
        tokio::time::Instant::now() + utils_rs::scale_timeout(std::time::Duration::from_secs(30));
    let mut last_state;
    let mut next_report = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(handle) => return Ok(handle),
            crate::DocLookup::PendingMaterialization => last_state = "pending",
            crate::DocLookup::Missing => last_state = "missing",
        }
        let now = tokio::time::Instant::now();
        if now >= next_report {
            next_report = now + std::time::Duration::from_secs(5);
            let (access, known) = describe_keyhive_state(repo, doc_id.clone()).await;
            let ledger = super::keyhive::describe_ledger(repo).await?;
            tracing::warn!(
                %doc_id,
                state = last_state,
                receipt = ?receipt.outcome,
                local_access = ?access,
                keyhive_knows_doc = known,
                keyhive_ledger = %ledger,
                nickname = %log_nickname::nickname(&repo.local_peer_id()),
                "waiting for synced document to become ready"
            );
        }
        if now >= deadline {
            let (access, known) = describe_keyhive_state(repo, doc_id).await;
            let ledger = super::keyhive::describe_ledger(repo).await?;
            return Err(crate::ferr!(
                "synced document never became ready: state={last_state} receipt={:?} \
                 local_access={access:?} keyhive_knows_doc={known} keyhive_ledger={ledger} node={}",
                receipt.outcome,
                log_nickname::nickname(&repo.local_peer_id())
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
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
    conn_a_to_b.sync_doc_with_peer(doc_id.clone()).await?;
    conn_b_to_a.sync_doc_with_peer(doc_id.clone()).await?;
    repo_a.wait_for_quiescence(None).await?;
    repo_b.wait_for_quiescence(None).await?;
    let handle_a = expect_ready(repo_a, doc_id.clone()).await?;
    let handle_b = expect_ready(repo_b, doc_id).await?;
    Ok((handle_a, handle_b))
}

pub async fn expect_ready(
    repo: &Arc<crate::BigRepo>,
    doc_id: DocumentId,
) -> Res<crate::BigDocHandle> {
    // An unbounded wait here turns every "document never materializes" defect
    // into an opaque nextest timeout with no signal about which node, which
    // state, or whether the node even holds access. Bound it and report those.
    let deadline =
        tokio::time::Instant::now() + utils_rs::scale_timeout(std::time::Duration::from_secs(30));
    let mut last_state;
    let mut next_report = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(h) => return Ok(h),
            crate::DocLookup::PendingMaterialization => last_state = "pending",
            crate::DocLookup::Missing => last_state = "missing",
        }
        let now = tokio::time::Instant::now();
        if now >= next_report {
            next_report = now + std::time::Duration::from_secs(5);
            let (access, known) = describe_keyhive_state(repo, doc_id.clone()).await;
            let ledger = super::keyhive::describe_ledger(repo).await?;
            tracing::warn!(
                %doc_id,
                state = last_state,
                local_access = ?access,
                keyhive_knows_doc = known,
                keyhive_ledger = %ledger,
                nickname = %log_nickname::nickname(&repo.local_peer_id()),
                "waiting for document to materialize"
            );
        }
        if now >= deadline {
            let (access, known) = describe_keyhive_state(repo, doc_id).await;
            let ledger = super::keyhive::describe_ledger(repo).await?;
            return Err(crate::ferr!(
                "document did not become ready within the deadline: \
                 state={last_state} local_access={access:?} keyhive_knows_doc={known} \
                 keyhive_ledger={ledger} node={}",
                log_nickname::nickname(&repo.local_peer_id())
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// The node's own Keyhive view of `doc_id`: its agent's access, and whether the
/// document is known at all. Distinguishes "never received the document" from
/// "received the document but not the delegation that grants access".
pub(crate) async fn describe_keyhive_state(
    repo: &crate::BigRepo,
    doc_id: DocumentId,
) -> (Option<Access>, bool) {
    let Ok(local) = ed25519_dalek::VerifyingKey::from_bytes(&repo.local_peer_id().to_bytes32())
    else {
        return (None, false);
    };
    let local = keyhive_core::principal::identifier::Identifier::from(local);
    let Ok(doc) = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.to_bytes32()) else {
        return (None, false);
    };
    let doc_ident = keyhive_core::principal::identifier::Identifier::from(doc);
    let keyhive = repo.keyhive();
    let access = keyhive.agent_access_on(&local, doc_ident).await;
    let known = keyhive
        .document_ids()
        .await
        .contains(&big_sync_core::ObjKey::new(doc_id.as_bytes()));
    (access, known)
}

/// Reach the fixed point of every currently configured BigSync route and all
/// local BigRepo work on `nodes`. A sync round may itself publish new physical
/// document heads (for example a causal healing checkpoint), so one frontier
/// fence is not sufficient.
pub async fn wait_for_network_rest(nodes: &[&super::topo::Node]) -> Res<()> {
    for node in nodes {
        // A cursor that never reaches the head parks this fence until the
        // test's hard timeout. Report the stall (first after 5s, then every
        // 5s) so the log says which node and which cursor stalled.
        let mut stall: Option<(std::time::Instant, std::time::Instant)> = None;
        loop {
            let event_tail = node.store.admission_head().await?;
            let group_cursor = node.store.keyhive_group_part_cursor().await?;
            if group_cursor >= event_tail {
                break;
            }
            let now = std::time::Instant::now();
            let (started, last_report) = *stall.get_or_insert((now, now));
            if now.duration_since(last_report) >= std::time::Duration::from_secs(5) {
                stall = Some((started, now));
                tracing::warn!(
                    node = %node.repo.local_peer_id(),
                    admission_head = event_tail,
                    group_part_cursor = group_cursor,
                    stalled_secs = now.duration_since(started).as_secs(),
                    "network-rest fence stalled: group-part cursor behind admission head"
                );
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
            let mut stall: Option<(std::time::Instant, std::time::Instant)> = None;
            while {
                let event_tail = node.store.admission_head().await?;
                let group_cursor = node.store.keyhive_group_part_cursor().await?;
                let causal_cursor = node.store.causal_checkpoint_cursor().await?;
                let behind = group_cursor < event_tail || causal_cursor < event_tail;
                if behind {
                    let now = std::time::Instant::now();
                    let (started, last_report) = *stall.get_or_insert((now, now));
                    if now.duration_since(last_report) >= std::time::Duration::from_secs(5) {
                        stall = Some((started, now));
                        tracing::warn!(
                            node = %node.repo.local_peer_id(),
                            admission_head = event_tail,
                            group_part_cursor = group_cursor,
                            causal_checkpoint_cursor = causal_cursor,
                            stalled_secs = now.duration_since(started).as_secs(),
                            "network-rest fence stalled: worker cursors behind admission head"
                        );
                    }
                }
                behind
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
