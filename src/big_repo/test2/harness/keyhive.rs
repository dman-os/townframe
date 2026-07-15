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
    let bytes = doc_id.into_bytes();
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

/// Compare the structural Keyhive document state across a pair. Decrypted
/// content-key caches are intentionally excluded: they advance as each node
/// materializes content and are checked separately after document sync.
pub(crate) async fn assert_document_snapshot_equal(
    left: &crate::test2::harness::Node,
    right: &crate::test2::harness::Node,
    doc_id: DocumentId,
) -> Res<()> {
    let left_snapshot = document_snapshot(&left.repo, doc_id).await?;
    let right_snapshot = document_snapshot(&right.repo, doc_id).await?;
    if left_snapshot != right_snapshot {
        return Err(crate::ferr!(
            "Keyhive document state diverged: {} = {left_snapshot:?}, right = {right_snapshot:?}",
            log_nickname::nickname(&left.peer_id()),
        ));
    }
    Ok(())
}
