//! Tier 1 — two-node ladder (direct A↔B).
//!
//! Each rung uses [`super::harness::Pair`] (RAII teardown, contact-card
//! exchange done at boot) and the synchronous-by-design fixtures: one
//! `sync_*` call per barrier, no retry loops. A missed post-condition
//! surfaces as an `Err`, exposing runtime2 ordering bugs.

use super::harness::{Pair, fixtures, heads};
use crate::{BigRepoChangeFilter, BigRepoDocIdFilter, StorageConfig};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;

#[tokio::test(flavor = "multi_thread")]
async fn tier1_document_created_before_connection_replicates() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(29, 30, "Owner", "Reader").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before connection"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "before connection");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

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
    heads::tier0_invariants(&pair, doc_id, &alice_doc, &bob_doc).await?;

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
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    // Owner edits after first replication.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "second rung"))
                .map_err(|err| crate::ferr!("failed editing document: {err:?}"))
        })
        .await??;
    assert_eq!(read_title(&owner_doc).await, "second rung");
    // Reader pulls the update in one sync.
    drop(reader_doc);
    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "second rung");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_reader_edit_propagates_back_to_owner() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(35, 36, "Owner", "Editor").await?;

    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "owner value"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &editor_agent, Access::Edit).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&editor_doc).await, "owner value");

    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "editor value"))
                .map_err(|err| crate::ferr!("failed editing document: {err:?}"))
        })
        .await??;
    assert_eq!(read_title(&editor_doc).await, "editor value");

    // The owner handle predates the editor's commit. Drop it before syncing
    // so the returned handle is the materialized post-edit view.
    drop(owner_doc);
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    assert_eq!(read_title(&owner_doc2).await, "editor value");
    heads::tier0_invariants(&pair, doc_id, &owner_doc2, &editor_doc).await?;

    drop(owner_doc2);
    drop(editor_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_divergent_edits_converge_bidirectionally() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(37, 38, "Owner", "Editor").await?;

    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "base"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    fixtures::grant_and_propagate(&pair, doc_id, &editor_agent, Access::Edit).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    owner_doc
        .with_document(|doc| {
            doc.set_actor(automerge::ActorId::from([37_u8; 16]));
            doc.transact(|tx| tx.put(automerge::ROOT, "owner_note", "owner branch"))
                .map_err(|err| crate::ferr!("failed owner edit: {err:?}"))
        })
        .await??;
    editor_doc
        .with_document(|doc| {
            doc.set_actor(automerge::ActorId::from([38_u8; 16]));
            doc.transact(|tx| tx.put(automerge::ROOT, "editor_note", "editor branch"))
                .map_err(|err| crate::ferr!("failed editor edit: {err:?}"))
        })
        .await??;

    // The editor's commit may advance CGKA state. Propagate that state before
    // asking Owner to decrypt the new document entrypoint.
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;

    // Pull the editor branch into Owner first, then pull the converged state
    // back into Editor. Each call is a synchronization barrier; a concurrent
    // pair of calls can race before either side has the other's new branch.
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.left()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;
    for (label, handle) in [("Owner", &owner_doc), ("Editor", &editor_doc)] {
        assert_eq!(
            read_optional_text(handle, "owner_note").await.as_deref(),
            Some("owner branch"),
            "{label} is missing the owner branch",
        );
        assert_eq!(
            read_optional_text(handle, "editor_note").await.as_deref(),
            Some("editor branch"),
            "{label} is missing the editor branch",
        );
    }
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &editor_doc).await?;

    drop(owner_doc);
    drop(editor_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_noop_sync_emits_no_change_notification() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(43, 44, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "unchanged"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let (_registration, mut notifications) = pair
        .right()
        .repo
        .subscribe_change_listener(BigRepoChangeFilter {
            doc_id: Some(BigRepoDocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;
    let before = pair.right().repo.doc_head_state(doc_id).await?;

    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;
    let after = pair.right().repo.doc_head_state(doc_id).await?;

    assert_eq!(before, after);
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(read_title(&reader_doc).await, "unchanged");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_closed_connection_then_reconnect_syncs_again() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(39, 40, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before reconnect"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "before reconnect");

    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair.right_conn.take().expect("right connection should exist");
    old_left.stop().await?;

    let new_left = pair.left().connect(pair.right()).await?;
    let new_right = pair.right().accepted_connection().await;
    pair.left_conn = Some(new_left);
    pair.right_conn = Some(new_right);

    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "before reconnect");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc);
    drop(reader_doc2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_remote_restart_then_live_reconnect_preserves_document() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair = Pair::boot_persistent(
        45,
        46,
        "Owner",
        "Reader",
        left_path,
        right_path.clone(),
    )
    .await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before restart"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "before restart");
    drop(reader_doc);

    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair.right_conn.take().expect("right connection should exist");
    old_left.stop().await?;
    pair.restart_right(StorageConfig::Disk { path: right_path }).await?;
    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;

    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "before restart");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier1_offline_updates_catch_up() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(41, 42, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "first value"))
        .map_err(|err| crate::ferr!("failed creating ladder document: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    // Reader syncs the initial document.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "first value");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    // --- Go offline: remove both transport and big-sync routes.
    pair.disconnect().await?;
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair.right_conn.take().expect("right connection should exist");
    old_left.stop().await?;

    // Owner edits while Reader's big-repo connection is closed.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "offline update"))
                .map_err(|err| crate::ferr!("failed offline edit: {err:?}"))
        })
        .await??;
    assert_eq!(read_title(&owner_doc).await, "offline update");

    // --- Reconnect.
    let new_left = pair.left().connect(pair.right()).await?;
    let new_right = pair.right().accepted_connection().await;
    pair.left_conn = Some(new_left);
    pair.right_conn = Some(new_right);

    // Reader syncs and catches up on the offline update.
    drop(reader_doc);
    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "offline update");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

async fn read_title(handle: &crate::BigDocHandle) -> String {
    read_text(handle, "title").await
}

async fn read_text(handle: &crate::BigDocHandle, key: &str) -> String {
    read_optional_text(handle, key)
        .await
        .unwrap_or_else(|| panic!("text value {key:?} should exist and be a string"))
}

async fn read_optional_text(handle: &crate::BigDocHandle, key: &str) -> Option<String> {
    handle
        .with_document_read(|doc| {
            let Ok(Some((automerge::Value::Scalar(value), _))) = doc.get(automerge::ROOT, key)
            else {
                return None;
            };
            match value.as_ref() {
                ScalarValue::Str(value) => Some(value.to_string()),
                _ => None,
            }
        })
        .await
}
