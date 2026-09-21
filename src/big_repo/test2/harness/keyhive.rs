//! Keyhive state snapshots for deterministic test diagnostics.
//!
//! These snapshots deliberately stay in test support. They compare the
//! causal state that controls document decryption without adding a production
//! API: CGKA operation frontier, membership heads, revocation heads, and
//! transitive access.

use super::log_nickname;
use crate::{BigRepo, DocumentId, Res};
use keyhive_crypto::digest::Digest;
use std::collections::{BTreeMap, BTreeSet};
use utils_rs::expect_tags::ERROR_IMPOSSIBLE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocumentKeyhiveSnapshot {
    pub cgka_operation_hashes: BTreeSet<String>,
    pub cgka_frontier: BTreeSet<String>,
    pub delegation_heads: BTreeSet<String>,
    pub revocation_heads: BTreeSet<String>,
    pub members: BTreeMap<[u8; 32], keyhive_core::access::Access>,
}

/// Snapshot the local Keyhive document state relevant to decryption.
pub(crate) async fn document_snapshot(
    repo: &BigRepo,
    doc_id: DocumentId,
) -> Res<DocumentKeyhiveSnapshot> {
    let bytes: [u8; 32] = doc_id.to_bytes32().expect(ERROR_IMPOSSIBLE);
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&bytes)
        .map_err(|_| crate::ferr!("document id is not a valid Ed25519 point"))?;
    let identifier = keyhive_core::principal::identifier::Identifier::from(verifying_key);
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(identifier);
    let keyhive = repo.keyhive().clone_keyhive();
    let doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| crate::ferr!("Keyhive document is missing for {doc_id}"))?;
    let locked = doc.lock().await;

    let epochs = locked
        .cgka_ops()
        .map_err(|error| crate::ferr!("failed reading CGKA ops: {error}"))?;
    let mut operation_hashes = BTreeSet::new();
    let mut predecessor_hashes = BTreeSet::new();
    for epoch in &epochs {
        for operation in epoch.clone() {
            let hash: Digest<_> = Digest::hash(operation.as_ref());
            let hash_string = hash.to_string();
            operation_hashes.insert(hash_string);
            predecessor_hashes.extend(
                operation
                    .payload()
                    .predecessors()
                    .into_iter()
                    .map(|predecessor| predecessor.to_string()),
            );
        }
    }
    let cgka_frontier = operation_hashes
        .difference(&predecessor_hashes)
        .cloned()
        .collect();

    let delegation_heads = locked
        .delegation_heads()
        .keys()
        .map(ToString::to_string)
        .collect();
    let revocation_heads = locked
        .revocation_heads()
        .keys()
        .map(ToString::to_string)
        .collect();
    let members = locked
        .transitive_members()
        .await
        .into_iter()
        .map(|(identifier, (_, access))| (identifier.to_bytes(), access))
        .collect();
    Ok(DocumentKeyhiveSnapshot {
        cgka_operation_hashes: operation_hashes,
        cgka_frontier,
        delegation_heads,
        revocation_heads,
        members,
    })
}

/// Both nodes' Keyhive ingestion ledgers, for a divergence report.
///
/// A node with nothing unapplied has applied every event it received, so an
/// event it lacks was never delivered to it. A non-empty remainder names the
/// delivering peer whose events are stuck, which is an apply-side or ordering
/// defect instead. Without this, "missing delegation" cannot be told apart
/// between the two.
pub(crate) async fn describe_keyhive_ledger(
    left: &crate::test2::harness::Node,
    right: &crate::test2::harness::Node,
) -> Res<String> {
    let (left_ledger, right_ledger) = futures::join!(
        left.store.keyhive_event_ledger(),
        right.store.keyhive_event_ledger()
    );
    Ok(format!(
        "{} {}",
        format_ledger(&log_nickname::nickname(&left.peer_id()), &left_ledger?),
        format_ledger(&log_nickname::nickname(&right.peer_id()), &right_ledger?)
    ))
}

/// One node's ingestion ledger, for diagnostics that only have a repo.
pub(crate) async fn describe_ledger(repo: &crate::BigRepo) -> Res<String> {
    let ledger = repo.sqlite_store().keyhive_event_ledger().await?;
    Ok(format_ledger(
        &log_nickname::nickname(&repo.local_peer_id()),
        &ledger,
    ))
}

fn format_ledger(nickname: &str, ledger: &crate::store::sqlite::KeyhiveEventLedger) -> String {
    let sources = ledger
        .unapplied_by_source
        .iter()
        .map(|(source, count)| match source {
            Some(bytes) if bytes.len() == 32 => format!(
                "{}:{count}",
                bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ),
            Some(bytes) => format!("{}bytes:{count}", bytes.len()),
            None => format!("local:{count}"),
        })
        .collect::<Vec<_>>();
    format!(
        "{nickname}[logged={} admitted={} head={} unapplied=[{}]]",
        ledger.logged,
        ledger.admitted,
        ledger.admission_head,
        sources.join(", ")
    )
}

/// Compare the structural Keyhive document state across a pair. Decrypted
/// content-key caches are intentionally excluded: they advance as each node
/// materializes content and are checked separately after document sync.
pub(crate) async fn assert_document_snapshot_equal(
    left: &crate::test2::harness::Node,
    right: &crate::test2::harness::Node,
    doc_id: DocumentId,
) -> Res<()> {
    let left_snapshot = document_snapshot(&left.repo, doc_id.clone()).await?;
    let right_snapshot = document_snapshot(&right.repo, doc_id).await?;
    if left_snapshot != right_snapshot {
        // Report the symmetric difference per field: dumping both snapshots
        // buries the one line that matters (which delegation or member is
        // missing on which side) in hundreds of characters of hashes.
        let diff = |left: &BTreeSet<String>, right: &BTreeSet<String>| {
            (
                left.difference(right).cloned().collect::<Vec<_>>(),
                right.difference(left).cloned().collect::<Vec<_>>(),
            )
        };
        let (cgka_left_only, cgka_right_only) = diff(
            &left_snapshot.cgka_operation_hashes,
            &right_snapshot.cgka_operation_hashes,
        );
        let (frontier_left_only, frontier_right_only) =
            diff(&left_snapshot.cgka_frontier, &right_snapshot.cgka_frontier);
        let (delegation_left_only, delegation_right_only) = diff(
            &left_snapshot.delegation_heads,
            &right_snapshot.delegation_heads,
        );
        let (revocation_left_only, revocation_right_only) = diff(
            &left_snapshot.revocation_heads,
            &right_snapshot.revocation_heads,
        );
        let member_keys = |snapshot: &DocumentKeyhiveSnapshot| {
            snapshot
                .members
                .iter()
                .map(|(id, access)| {
                    let hexed = id
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>();
                    (format!("0x{hexed}"), format!("{access:?}"))
                })
                .collect::<BTreeSet<_>>()
        };
        let left_members = member_keys(&left_snapshot);
        let right_members = member_keys(&right_snapshot);
        // Name which side of the delivery pipeline lost the missing events:
        // never delivered, delivered but never applied, or applied anyway.
        // Name which side of the delivery pipeline lost the missing events:
        // never delivered, delivered but never applied, or applied anyway.
        let keyhive_ledger = describe_keyhive_ledger(left, right).await?;
        return Err(crate::ferr!(
            "Keyhive document state diverged: {} vs {}: \
             cgka_left_only={cgka_left_only:?} cgka_right_only={cgka_right_only:?} \
             frontier_left_only={frontier_left_only:?} frontier_right_only={frontier_right_only:?} \
             delegation_left_only={delegation_left_only:?} delegation_right_only={delegation_right_only:?} \
             revocation_left_only={revocation_left_only:?} revocation_right_only={revocation_right_only:?} \
             members_left_only={:?} members_right_only={:?} \
             keyhive_ledger={keyhive_ledger}",
            log_nickname::nickname(&left.peer_id()),
            log_nickname::nickname(&right.peer_id()),
            left_members
                .difference(&right_members)
                .cloned()
                .collect::<Vec<_>>(),
            right_members
                .difference(&left_members)
                .cloned()
                .collect::<Vec<_>>(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test2::harness::{Pair, fixtures};
    use automerge::transaction::Transactable;
    use keyhive_core::access::Access;

    /// The divergence report's ledger probe must read an empty unapplied
    /// remainder on a healthy pair, and must count the events both nodes
    /// actually hold. If it could not, a divergence would be reported with a
    /// meaningless ledger and the delivery-vs-apply split would be a guess.
    #[tokio::test(flavor = "multi_thread")]
    async fn keyhive_event_ledger_reflects_applied_state() -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        let pair = Pair::boot(71, 72, "Owner", "Reader").await?;
        let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

        let mut seed = automerge::Automerge::new();
        seed.transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating seed doc: {err:?}"))?;
        let owner_doc = pair.left().repo.create_doc(seed).await?;
        let doc_id = owner_doc.document_id();
        fixtures::grant_and_propagate(&pair, doc_id.clone(), &reader_agent, Access::Read).await?;
        let _reader_doc =
            fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
        pair.left().repo.wait_for_keyhive_reconciliation().await?;
        pair.right().repo.wait_for_keyhive_reconciliation().await?;

        // A change notification can start another Keyhive exchange after the
        // reconciliation fence, so the durable remainder is legitimately allowed
        // to be mid-flight when the ledger is read. Drain it: an apply-side gap
        // keeps the remainder non-empty until the deadline and still fails.
        let deadline = tokio::time::Instant::now()
            + utils_rs::scale_timeout(std::time::Duration::from_secs(20));
        for node in [pair.left(), pair.right()] {
            loop {
                let ledger = node.store.keyhive_event_ledger().await?;
                assert!(ledger.logged > 0, "{} logged no events", node.label);
                assert!(
                    ledger.admission_head > 0,
                    "{} has no admission head",
                    node.label
                );
                if ledger.logged == ledger.admitted {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(crate::ferr!(
                        "{} has unapplied events on a settled grant: logged={} admitted={} unapplied_by_source={:?}",
                        node.label,
                        ledger.logged,
                        ledger.admitted,
                        ledger.unapplied_by_source
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        drop(owner_doc);
        Ok(())
    }
}
