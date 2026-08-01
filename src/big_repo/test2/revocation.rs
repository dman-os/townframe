//! Tier 6 — explicit-frontier revocation regressions.

use super::harness::{fixtures, Pair};
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;
use std::collections::BTreeSet;

#[tokio::test(flavor = "multi_thread")]
async fn tier6_revoke_uses_authoritative_frontier_and_removes_access() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(234, 235, "Owner", "RevokedReader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before-revoke"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "version", "2"))
                .map_err(|err| crate::ferr!("failed writing version: {err:?}"))
        })
        .await??;

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let revoke_frontier: BTreeSet<Vec<u8>> = pair
        .left()
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .iter()
        .map(|head| head.0.to_vec())
        .collect();

    pair.left()
        .repo
        .revoke_doc_access(doc_id, reader_agent)
        .await?;

    let bytes = doc_id.into_bytes();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&bytes)
        .map_err(|_| crate::ferr!("doc id is not a valid verifying key"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    );
    let keyhive = pair.left().repo.keyhive().clone_keyhive();
    let kh_doc = keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| crate::ferr!("owner keyhive document missing"))?;
    let locked = kh_doc.lock().await;
    let has_frontier = locked.revocation_heads().values().any(|revocation| {
        revocation
            .payload()
            .after()
            .content
            .get(&kh_doc_id)
            .is_some_and(|heads| heads.iter().cloned().collect::<BTreeSet<_>>() == revoke_frontier)
    });
    assert!(
        has_frontier,
        "revocation must carry the authoritative sedimentree frontier"
    );
    drop(locked);

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    assert!(
        pair.right()
            .repo
            .keyhive()
            .agent_access_on(
                &keyhive_core::principal::identifier::Identifier::from(
                    ed25519_dalek::VerifyingKey::from_bytes(pair.right().peer_id().as_bytes())
                        .expect("peer id must be a verifying key"),
                ),
                keyhive_core::principal::identifier::Identifier::from(vk),
            )
            .await
            .is_none(),
        "revoked reader must lose effective document access"
    );

    // The reader may retain already-held historical plaintext; revocation is
    // forward secrecy, not backward erasure.
    let title = reader_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "title")
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(value) => match value.as_ref() {
                        ScalarValue::Str(value) => Some(value.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await;
    assert_eq!(title.as_deref(), Some("before-revoke"));
    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}
