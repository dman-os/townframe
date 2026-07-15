//! Tier 2 — access matrix: direct agent grants.
//!
//! Covers the four access levels [`Access::{Read, Edit, Admin}`] plus
//! `None` (no grant) across **grant-before-content** and
//! **grant-after-content** scenarios. Uses direct (A<-->) topology only;
//! Group and nested-group coverage is included below; document-as-member and
//! public grants remain deferred to a later tier.
//!
//! # Structure
//!
//! | Case                          | Grant timing    | Access  | Expect materialized |
//! |-------------------------------|-----------------|---------|---------------------|
//! | grant_before_content_read     | before content  | Read    | yes                 |
//! | grant_before_content_edit     | before content  | Edit    | yes                 |
//! | grant_before_content_admin    | before content  | Admin   | yes                 |
//! | grant_after_content_read      | after content   | Read    | yes                 |
//! | grant_after_content_edit      | after content   | Edit    | yes                 |
//! | grant_after_content_admin     | after content   | Admin   | yes                 |
//! | no_grant_blocks_materialize   | N/A             | None    | no (Missing/…ation) |

use super::harness::{Pair, fixtures, heads};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;

// ─── Grant-before-content ────────────────────────────────────────────────────
//
// Each test: create doc → grant → write content → sync → verify + Tier 0.

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_before_content_read() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(51, 52, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    // Create a doc with minimal content (automerge requires at least one commit).
    // The meaningful content is written after the grant.
    let mut seed = automerge::Automerge::new();
    seed.transact(|tx| tx.put(automerge::ROOT, "_init", true))
        .map_err(|err| crate::ferr!("failed creating seed doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(seed).await?;
    let doc_id = owner_doc.document_id();

    // Grant before the meaningful content exists.
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    // Owner writes meaningful content after the grant has propagated.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "grant-before-content"))
                .map_err(|err| crate::ferr!("failed writing after grant: {err:?}"))
        })
        .await??;

    // Reader syncs and sees the content written after the grant.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "grant-before-content");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_before_content_edit() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(53, 54, "Owner", "Editor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut seed = automerge::Automerge::new();
    seed.transact(|tx| tx.put(automerge::ROOT, "_init", true))
        .map_err(|err| crate::ferr!("failed creating seed doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(seed).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &editor_agent, Access::Edit).await?;

    // Owner writes meaningful content after the edit grant.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "edit-after-grant"))
                .map_err(|err| crate::ferr!("failed writing after edit grant: {err:?}"))
        })
        .await??;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&editor_doc).await, "edit-after-grant");

    // Editor can also write (Edit implies write permission).
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "editor_note", "editor-added"))
                .map_err(|err| crate::ferr!("failed editor write: {err:?}"))
        })
        .await??;

    // Sync back to Owner and verify convergence.
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    drop(owner_doc);
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    assert_eq!(read_optional_text(&owner_doc2, "editor_note").await.as_deref(), Some("editor-added"));
    heads::tier0_invariants(&pair, doc_id, &owner_doc2, &editor_doc).await?;

    drop(owner_doc2);
    drop(editor_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_before_content_admin() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(55, 56, "Owner", "Admin").await?;
    let admin_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut seed = automerge::Automerge::new();
    seed.transact(|tx| tx.put(automerge::ROOT, "_init", true))
        .map_err(|err| crate::ferr!("failed creating seed doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(seed).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &admin_agent, Access::Admin).await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "admin-after-grant"))
                .map_err(|err| crate::ferr!("failed writing after admin grant: {err:?}"))
        })
        .await??;

    let admin_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&admin_doc).await, "admin-after-grant");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &admin_doc).await?;

    drop(owner_doc);
    drop(admin_doc);
    Ok(())
}

// ─── Grant-after-content ─────────────────────────────────────────────────────
//
// Each test: create doc with content → grant → sync → verify + Tier 0.

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_after_content_read() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(57, 58, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "pre-grant-content"))
        .map_err(|err| crate::ferr!("failed creating doc with content: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant after content exists.
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "pre-grant-content");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_after_content_edit() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(59, 60, "Owner", "Editor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "existing-content"))
        .map_err(|err| crate::ferr!("failed creating doc with content: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &editor_agent, Access::Edit).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&editor_doc).await, "existing-content");

    // Editor adds content post-grant.
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "editor_added", "post-grant"))
                .map_err(|err| crate::ferr!("failed editor write: {err:?}"))
        })
        .await??;

    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    drop(owner_doc);
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    assert_eq!(
        read_optional_text(&owner_doc2, "editor_added").await.as_deref(),
        Some("post-grant")
    );
    heads::tier0_invariants(&pair, doc_id, &owner_doc2, &editor_doc).await?;

    drop(owner_doc2);
    drop(editor_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_after_content_admin() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(61, 62, "Owner", "Admin").await?;
    let admin_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "admin-target"))
        .map_err(|err| crate::ferr!("failed creating doc with content: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &admin_agent, Access::Admin).await?;

    let admin_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&admin_doc).await, "admin-target");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &admin_doc).await?;

    drop(owner_doc);
    drop(admin_doc);
    Ok(())
}

// ─── Group grants ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_group_grant_read_materializes_member() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(65, 66, "Owner", "GroupReader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "group-readable"))
        .map_err(|err| crate::ferr!("failed creating grouped doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .add_member_to_group(reader_agent, &group, Access::Read)
        .await?;
    fixtures::grant_group_and_propagate(&pair, doc_id, &group, Access::Read).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "group-readable");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_group_grant_before_content_read() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(73, 74, "Owner", "GroupReaderBeforeContent").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut seed = automerge::Automerge::new();
    seed.transact(|tx| tx.put(automerge::ROOT, "_init", true))
        .map_err(|err| crate::ferr!("failed creating group seed doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(seed).await?;
    let doc_id = owner_doc.document_id();

    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .add_member_to_group(reader_agent, &group, Access::Read)
        .await?;
    fixtures::grant_group_and_propagate(&pair, doc_id, &group, Access::Read).await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "group-before-content"))
                .map_err(|err| crate::ferr!("failed writing after group grant: {err:?}"))
        })
        .await??;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "group-before-content");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_nested_group_edit_propagates_member_update() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(67, 68, "Owner", "NestedEditor").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "nested-group"))
        .map_err(|err| crate::ferr!("failed creating nested-group doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let outer = pair.left().repo.create_group_with_parents(vec![]).await?;
    let inner = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .add_member_to_group(reader_agent, &inner, Access::Edit)
        .await?;
    pair.left()
        .repo
        .add_member_to_group(inner.clone(), &outer, Access::Edit)
        .await?;
    fixtures::grant_group_and_propagate(&pair, doc_id, &outer, Access::Edit).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&editor_doc).await, "nested-group");
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "nested_note", "member-edit"))
                .map_err(|err| crate::ferr!("failed nested-group member edit: {err:?}"))
        })
        .await??;

    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    drop(owner_doc);
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    assert_eq!(
        read_optional_text(&owner_doc2, "nested_note").await.as_deref(),
        Some("member-edit")
    );
    heads::tier0_invariants(&pair, doc_id, &owner_doc2, &editor_doc).await?;

    drop(owner_doc2);
    drop(editor_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_after_content_while_offline_read() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(69, 70, "Owner", "OfflineReader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "offline-grant"))
        .map_err(|err| crate::ferr!("failed creating offline-grant doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Remove both transport and big-sync routes before changing membership.
    pair.disconnect().await?;
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair.right_conn.take().expect("right connection should exist");
    old_left.stop().await?;

    // The owner can make the grant while the reader is disconnected. The
    // checkpoint produced by this read grant is also part of the offline
    // change and must become readable after reconnection.
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;

    let new_left = pair.left().connect(pair.right()).await?;
    let new_right = pair.right().accepted_connection().await;
    pair.left_conn = Some(new_left);
    pair.right_conn = Some(new_right);

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    fixtures::assert_reader_has_access(&pair.right().repo, doc_id).await?;
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let reader_doc = pair.right().repo.get_doc(&doc_id).await?.into_ready(doc_id)?;
    assert_eq!(read_title(&reader_doc).await, "offline-grant");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(owner_doc);
    drop(reader_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier2_grant_after_content_while_offline_edit() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(71, 72, "Owner", "OfflineEditor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "offline-edit"))
        .map_err(|err| crate::ferr!("failed creating offline-edit doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.disconnect().await?;
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair.right_conn.take().expect("right connection should exist");
    old_left.stop().await?;

    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent, Access::Edit)
        .await?;

    let new_left = pair.left().connect(pair.right()).await?;
    let new_right = pair.right().accepted_connection().await;
    pair.left_conn = Some(new_left);
    pair.right_conn = Some(new_right);
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "offline_note", "editor-added"))
                .map_err(|err| crate::ferr!("failed offline editor write: {err:?}"))
        })
        .await??;

    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    drop(owner_doc);
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    assert_eq!(
        read_optional_text(&owner_doc2, "offline_note").await.as_deref(),
        Some("editor-added")
    );
    heads::tier0_invariants(&pair, doc_id, &owner_doc2, &editor_doc).await?;

    drop(owner_doc2);
    drop(editor_doc);
    Ok(())
}

// ─── None / no grant ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier2_no_grant_blocks_materialization() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(63, 64, "Owner", "Stranger").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "secret", "unauthorized"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // No grant — the stranger node has no access to this doc.
    // The stranger never gets a Ready handle.
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // The doc must not be Ready — the get_doc call may error (no doc worker)
    // or return Missing/PendingMaterialization. Both are expected for no-grant.
    let lookup = pair.right().repo.get_doc(&doc_id).await;
    match lookup {
        Ok(crate::DocLookup::Ready(_)) => {
            return Err(crate::ferr!(
                "Stranger got a Ready handle on a doc without any grant — security violation"
            ))
        }
        Ok(crate::DocLookup::Missing | crate::DocLookup::PendingMaterialization) | Err(_) => {
            // Expected — no plaintext leak.
        }
    }

    // Tier-0 sedimentree parity is *not* applicable here: the stranger does
    // not hold the doc in its big_sync partition (no grant → no replication).
    // Skip the parity check and assert the stranger has no sedimentree entry.
    let stranger_state = pair.right().repo.doc_head_state(doc_id).await?;
    if !stranger_state.sedimentree_heads.is_empty() {
        return Err(crate::ferr!(
            "Stranger has sedimentree heads despite no grant: {:?}",
            stranger_state.sedimentree_heads,
        ));
    }

    drop(owner_doc);
    Ok(())
}

// ─── Read helpers (mirror ladder.rs) ─────────────────────────────────────────

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
