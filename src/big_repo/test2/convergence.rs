//! Tier 4 — convergence and delta-sync regressions.

use super::harness::{fixtures, heads, Pair};
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;
use std::time::Duration;

async fn read_text(handle: &crate::BigDocHandle, key: &str) -> Option<String> {
    handle
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, key)
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
        .await
}

async fn new_doc(pair: &Pair, title: &str) -> crate::Res<(crate::BigDocHandle, crate::DocumentId)> {
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", title))
        .map_err(|err| crate::ferr!("failed creating convergence doc: {err:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let id = doc.document_id();
    Ok((doc, id))
}

async fn grant_and_sync(
    pair: &Pair,
    doc_id: crate::DocumentId,
    access: Access,
) -> crate::Res<crate::BigDocHandle> {
    let agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(pair, doc_id, &agent, access).await?;
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await
}

#[tokio::test(flavor = "multi_thread")]
async fn tier4_repeated_sync_is_idempotent() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(40, 41, "Owner", "Reader").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "idempotent").await?;
    let reader_doc = grant_and_sync(&pair, doc_id, Access::Read).await?;
    let before = pair.right().repo.doc_head_state(doc_id).await?;

    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.right().repo.wait_for_quiescence(None).await?;
    let after = pair.right().repo.doc_head_state(doc_id).await?;
    assert_eq!(before.sedimentree_heads, after.sedimentree_heads);
    assert_eq!(before.materialized_heads, after.materialized_heads);
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier4_new_data_breaks_convergence_then_restores_it() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(42, 43, "Owner", "Reader").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "before-delta").await?;
    let reader_doc = grant_and_sync(&pair, doc_id, Access::Read).await?;
    let before = pair
        .left()
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "new-data"))
                .map_err(|err| crate::ferr!("failed writing convergence delta: {err:?}"))
        })
        .await??;
    let during = pair
        .left()
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads;
    assert_ne!(
        before, during,
        "a new local write must advance the frontier"
    );

    drop(reader_doc);
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc, "phase").await.as_deref(),
        Some("new-data")
    );
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier4_delta_sync_advances_only_the_new_frontier() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(44, 45, "Owner", "Reader").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "delta-base").await?;
    let reader_doc = grant_and_sync(&pair, doc_id, Access::Read).await?;
    let before = pair
        .right()
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "delta", "only"))
                .map_err(|err| crate::ferr!("failed writing delta: {err:?}"))
        })
        .await??;
    drop(reader_doc);
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let after = pair
        .right()
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads;
    assert_ne!(
        after, before,
        "the reader must receive the new delta frontier"
    );
    assert_eq!(
        read_text(&reader_doc, "delta").await.as_deref(),
        Some("only")
    );
    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier4_evicted_doc_can_sync_and_materialize_again() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(46, 47, "Owner", "Reader").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "eviction").await?;
    let reader_doc = grant_and_sync(&pair, doc_id, Access::Read).await?;
    drop(reader_doc);

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "after_drop", "received"))
                .map_err(|err| crate::ferr!("failed writing after handle drop: {err:?}"))
        })
        .await??;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc, "after_drop").await.as_deref(),
        Some("received")
    );
    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier4_fork_then_merge_preserves_decryption() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(48, 49, "Owner", "Editor").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "merge-base").await?;
    let editor_doc = grant_and_sync(&pair, doc_id, Access::Edit).await?;
    fixtures::go_offline(&mut pair).await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "owner_branch", "kept"))
                .map_err(|err| crate::ferr!("failed owner fork: {err:?}"))
        })
        .await??;
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "editor_branch", "kept"))
                .map_err(|err| crate::ferr!("failed editor fork: {err:?}"))
        })
        .await??;

    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    drop(owner_doc);
    let owner_doc =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    drop(editor_doc);
    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&owner_doc, "owner_branch").await.as_deref(),
        Some("kept")
    );
    assert_eq!(
        read_text(&owner_doc, "editor_branch").await.as_deref(),
        Some("kept")
    );
    assert_eq!(
        read_text(&editor_doc, "owner_branch").await.as_deref(),
        Some("kept")
    );
    assert_eq!(
        read_text(&editor_doc, "editor_branch").await.as_deref(),
        Some("kept")
    );
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &editor_doc).await?;
    drop(owner_doc);
    drop(editor_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier4_rapid_fire_then_idle_sync_converges_once() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(50, 51, "Owner", "Reader").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "rapid").await?;
    let reader_doc = grant_and_sync(&pair, doc_id, Access::Read).await?;
    drop(reader_doc);

    for index in 0..6 {
        owner_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "rapid", index.to_string()))
                    .map_err(|err| crate::ferr!("failed rapid mutation: {err:?}"))
            })
            .await??;
    }
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    drop(reader_doc);
    let before = pair.right().repo.doc_head_state(doc_id).await?;
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.right().repo.wait_for_quiescence(None).await?;
    let after = pair.right().repo.doc_head_state(doc_id).await?;
    assert_eq!(before.sedimentree_heads, after.sedimentree_heads);
    let final_reader = pair
        .right()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    assert_eq!(
        read_text(&final_reader, "rapid").await.as_deref(),
        Some("5")
    );
    drop(final_reader);
    drop(owner_doc);
    Ok(())
}

// ─── Concurrent edits while sync is in flight ─────────────────────────────
//
// Both peers have Edit access. A doc sync is in progress while both sides
// make independent local edits. After the sync completes, a follow-up sync
// converges both fields. This exercises the real concurrent scenario where
// edits and sync overlap.
#[tokio::test(flavor = "multi_thread")]
async fn tier4_concurrent_edit_while_sync_in_flight() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(55, 56, "Owner", "Editor").await?;
    let (owner_doc, doc_id) = new_doc(&pair, "concurrent-sync").await?;
    let editor_doc = grant_and_sync(&pair, doc_id, Access::Edit).await?;

    // All three tasks run concurrently: a doc sync (left→right) while
    // both sides make independent local edits.
    let sync_fut = pair
        .left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)));
    let left_edit_fut = owner_doc.with_document(|doc| {
        doc.transact(|tx| tx.put(automerge::ROOT, "field_a", "from_left"))
            .map_err(|err| crate::ferr!("owner concurrent edit failed: {err:?}"))
    });
    let right_edit_fut = editor_doc.with_document(|doc| {
        doc.transact(|tx| tx.put(automerge::ROOT, "field_b", "from_right"))
            .map_err(|err| crate::ferr!("editor concurrent edit failed: {err:?}"))
    });

    let (sync_result, left_result, right_result) =
        tokio::join!(sync_fut, left_edit_fut, right_edit_fut);

    sync_result.map_err(|e| crate::ferr!("concurrent sync failed: {e:?}"))?;
    left_result??;
    right_result??;

    // Follow-up sync to capture any edits that the in-flight sync missed.
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;
    pair.right().repo.wait_for_quiescence(None).await?;

    // Both sides must see both fields.
    let owner_check = pair
        .left()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    let editor_check = pair
        .right()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;

    assert_eq!(
        read_text(&owner_check, "field_a").await.as_deref(),
        Some("from_left"),
        "owner must see its own concurrent edit"
    );
    assert_eq!(
        read_text(&owner_check, "field_b").await.as_deref(),
        Some("from_right"),
        "owner must see editor's concurrent edit after convergence"
    );
    assert_eq!(
        read_text(&editor_check, "field_a").await.as_deref(),
        Some("from_left"),
        "editor must see owner's concurrent edit after convergence"
    );
    assert_eq!(
        read_text(&editor_check, "field_b").await.as_deref(),
        Some("from_right"),
        "editor must see its own concurrent edit"
    );

    heads::tier0_invariants(&pair, doc_id, &owner_check, &editor_check).await?;

    drop(owner_check);
    drop(editor_check);
    drop(owner_doc);
    drop(editor_doc);
    Ok(())
}
