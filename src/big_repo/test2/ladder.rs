//! Tier 1 — two-node ladder (direct A↔B).
//!
//! Each rung uses [`super::harness::Pair`] (RAII teardown, contact-card
//! exchange done at boot) and the synchronous-by-design fixtures: one
//! `sync_*` call per barrier, no retry loops. A missed post-condition
//! surfaces as an `Err`, exposing runtime2 ordering bugs.

use super::harness::{Pair, fixtures, heads};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;

#[tokio::test(flavor = "multi_thread")]
async fn tier1_connected_document_replicates_and_preserves_head_parity() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(31, 32, "Alice", "Bob").await?;

    // Contact-card exchange happened at boot; Alice knows Bob's agent in one
    // lookup.
    let bob = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "ladder rung 1"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let alice_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = alice_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &bob, Access::Read).await?;

    // One doc sync is the barrier: Bob must be able to read after this call.
    let bob_doc = fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id)
        .await?;
    let title = read_title(&bob_doc).await;
    assert_eq!(title, "ladder rung 1");

    heads::tier0_invariants(&pair, doc_id).await?;

    drop(alice_doc);
    drop(bob_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_connected_document_update_propagates_to_reader_after_first_replication(
) -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(33, 34, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "first rung"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    // First replication: reader pulls the initial doc in one sync.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "first rung");

    // Owner edits after first replication.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "second rung"))
                .map_err(|err| crate::ferr!("failed editing document: {err:?}"))
        })
        .await?;

    // Reader pulls the update in one sync.
    drop(reader_doc);
    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "second rung");

    heads::tier0_invariants(&pair, doc_id).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

async fn read_title(handle: &crate::BigDocHandle) -> String {
    handle
        .with_document_read(|doc| {
            let (value, _) = doc
                .get(automerge::ROOT, "title")
                .expect("title lookup should succeed")
                .expect("title should exist");
            let automerge::Value::Scalar(value) = value else {
                panic!("title should be scalar");
            };
            match value.as_ref() {
                ScalarValue::Str(value) => value.to_string(),
                _ => panic!("title should be a string"),
            }
        })
        .await
}
