//! Tier 6 — explicit-frontier revocation regressions.

use super::harness::{Pair, fixtures};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
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
    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
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

    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
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

    // Owner writes distinct post-revocation content.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "post-revoke-secret"))
                .map_err(|err| crate::ferr!("owner post-revoke write failed: {err:?}"))
        })
        .await??;

    // Sync doc between nodes.
    pair.left_conn().sync_doc_with_peer(doc_id).await?;
    match pair.right_conn().sync_doc_with_peer(doc_id).await {
        Ok(())
        | Err(
            crate::SyncDocError::Unauthorized
            | crate::SyncDocError::NotFound
            | crate::SyncDocError::Policy(_),
        ) => {}
        Err(err) => {
            return Err(crate::ferr!(
                "unexpected sync error after revocation: {err:?}"
            ));
        }
    }

    // The reader may retain already-held historical plaintext ("before-revoke"),
    // but MUST NOT observe the post-revocation content ("post-revoke-secret").
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
    assert_ne!(title.as_deref(), Some("post-revoke-secret"));
    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

/// A fully-revoked member's write is rejected locally at the worker commit
/// path, the handle is invalidated, and re-acquisition reloads the last
/// persisted state (no ghost commit survives).
#[tokio::test(flavor = "multi_thread")]
async fn tier6_revoked_member_write_is_rejected_locally() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(236, 237, "Owner", "RevokedReader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before-revoke"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent.clone(), Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    // Pre-revoke: the editor can write.
    reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "editor-writes"))
                .map_err(|err| crate::ferr!("pre-revoke write failed: {err:?}"))
        })
        .await??;

    // Revoke the reader outright (no regrant) and let both sides process it.
    pair.left()
        .repo
        .revoke_doc_access(doc_id, reader_agent)
        .await?;
    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;

    // The reader's keyhive must know it lost access.
    let revoked_access = pair
        .right()
        .repo
        .keyhive()
        .agent_access_on(
            &keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(pair.right().peer_id().as_bytes())
                    .expect("peer id must be a verifying key"),
            ),
            keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
                    .expect("doc id must be a verifying key"),
            ),
        )
        .await;
    assert_eq!(
        revoked_access, None,
        "revoked reader must lose effective document access"
    );

    // The first post-revocation write reaches the worker and is rejected at
    // the write-access gate, invalidating the handle.
    let write_result = reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "ghost", "write-after-revoke"))
                .map_err(|err| crate::ferr!("write txn failed: {err:?}"))
        })
        .await;
    let err = write_result.expect_err("revoked write must fail locally");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("write rejected") || msg.contains("access"),
        "revoked write must fail with an access-related error, got: {msg}"
    );

    // The handle is now invalidated: further writes fail fast without running
    // the mutation.
    let again = reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "ghost2", "again"))
                .map_err(|err| crate::ferr!("write txn failed: {err:?}"))
        })
        .await;
    let err2 = again.expect_err("write on invalidated handle must fail fast");
    assert!(
        format!("{err2:?}").contains("invalidated"),
        "invalidated-handle write must fail fast, got: {err2:?}"
    );

    // Re-acquisition reloads the last persisted state: pre-revoke content is
    // still readable, the rejected write is gone, and writes keep failing.
    // (No sync here — the owner's policy now refuses the revoked reader.)
    let reader_doc2 = match pair.right().repo.get_doc(&doc_id).await? {
        crate::DocLookup::Ready(handle) => handle,
        crate::DocLookup::PendingMaterialization => {
            return Err(crate::ferr!(
                "revoked reader re-acquire unexpectedly pending materialization"
            ));
        }
        crate::DocLookup::Missing => {
            return Err(crate::ferr!(
                "revoked reader re-acquire found no local state"
            ));
        }
    };
    let title = reader_doc2
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
    let ghost_gone = reader_doc2
        .with_document_read(|doc| doc.get(automerge::ROOT, "ghost").ok().flatten().is_some())
        .await;
    assert!(
        !ghost_gone,
        "reloaded document must not contain the rejected write"
    );
    let write3 = reader_doc2
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "ghost3", "again"))
                .map_err(|err| crate::ferr!("write txn failed: {err:?}"))
        })
        .await;
    let err3 = write3.expect_err("re-acquired revoked write must still fail");
    assert!(
        format!("{err3:?}").contains("invalidated") || format!("{err3:?}").contains("access"),
        "re-acquired revoked write must fail fast, got: {err3:?}"
    );

    drop(reader_doc);
    drop(reader_doc2);
    drop(owner_doc);
    Ok(())
}

/// A reader whose keyhive is stale (the owner revoked access but the
/// revocation hint has not propagated yet) passes its local fetch gate and
/// reaches the remote, which must reject the sync at the wire level with
/// [`crate::SyncDocError::Unauthorized`]. This is the remote-rejection
/// counterpart of the local `Policy(InsufficientAccess)` fail-fast: the
/// fetcher believes it still has access, the serving peer disagrees.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_stale_reader_sync_is_rejected_unauthorized_by_remote() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(238, 239, "Owner", "StaleReader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "shared"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    drop(reader_doc);
    drop(owner_doc);

    // Revoke without letting the reader's keyhive learn about it. The
    // notification path debounces for at least `quiet_window` (100ms), so the
    // doc-sync attempt below lands while the reader's local gate still passes
    // and the owner's serving policy is the side that rejects.
    pair.left()
        .repo
        .revoke_doc_access(doc_id, reader_agent)
        .await?;

    match pair.right_conn().sync_doc_with_peer(doc_id).await {
        Err(crate::SyncDocError::Unauthorized) => {}
        other => {
            return Err(crate::ferr!(
                "stale reader sync must be rejected by the remote with Unauthorized, got {other:?}"
            ));
        }
    }

    // Once the revocation reaches the reader, its local gate closes too.
    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;
    let access = pair
        .right()
        .repo
        .keyhive()
        .agent_access_on(
            &keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(pair.right().peer_id().as_bytes())
                    .expect("peer id must be a verifying key"),
            ),
            keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
                    .expect("doc id must be a verifying key"),
            ),
        )
        .await;
    assert_eq!(
        access, None,
        "after the revocation propagates the reader must lose effective document access"
    );
    Ok(())
}
