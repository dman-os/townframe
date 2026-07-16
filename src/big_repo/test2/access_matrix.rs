//! Tier 2 — access matrix: direct agent grants.
//!
//! Covers the four access levels [`Access::{Read, Edit, Admin}`] plus
//! `None` (no grant) across **grant-before-content** and
//! **grant-after-content** scenarios. Uses direct (A<-->) topology only;
//! including direct agents, groups, nested groups, document-as-member, and
//! public grants. Revocation and downgrade remain part of the later CGKA tier.
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

use super::harness::{fixtures, heads, Pair};
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
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
    assert_eq!(
        read_optional_text(&owner_doc2, "editor_note")
            .await
            .as_deref(),
        Some("editor-added")
    );
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
        read_optional_text(&owner_doc2, "editor_added")
            .await
            .as_deref(),
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
        read_optional_text(&owner_doc2, "nested_note")
            .await
            .as_deref(),
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
    fixtures::go_offline(&mut pair).await?;

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
    let reader_doc = pair
        .right()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
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

    fixtures::go_offline(&mut pair).await?;

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
        read_optional_text(&owner_doc2, "offline_note")
            .await
            .as_deref(),
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
// ─── Remaining direct-agent offline matrix ────────────────────────────────────

async fn run_offline_case(seed: u8, before_content: bool, access: Access) -> crate::Res<()> {
    let mut pair = Pair::boot(seed, seed.wrapping_add(1), "Owner", "OfflineAgent").await?;
    let agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let mut initial = automerge::Automerge::new();
    if before_content {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating offline seed doc: {err:?}"))?;
    } else {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "title", "offline-agent-matrix"))
            .map_err(|err| crate::ferr!("failed creating offline doc: {err:?}"))?;
    }
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::go_offline(&mut pair).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, agent, access)
        .await?;
    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    if before_content {
        owner_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "offline-agent-matrix"))
                    .map_err(|err| crate::ferr!("failed writing offline content: {err:?}"))
            })
            .await??;
    }
    let agent_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&agent_doc).await, "offline-agent-matrix");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &agent_doc).await?;
    if access.is_editor() {
        agent_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "agent_note", "agent-member"))
                    .map_err(|err| crate::ferr!("failed offline agent write: {err:?}"))
            })
            .await??;
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        drop(owner_doc);
        let owner_doc =
            fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
        assert_eq!(
            read_optional_text(&owner_doc, "agent_note")
                .await
                .as_deref(),
            Some("agent-member")
        );
        heads::tier0_invariants(&pair, doc_id, &owner_doc, &agent_doc).await?;
        drop(owner_doc);
    } else {
        drop(owner_doc);
    }
    drop(agent_doc);
    Ok(())
}

utils_rs::table_tests! {
    tier2_offline_agent_cases tokio,
    (seed, before_content, access),
    {
        run_offline_case(seed, before_content, access).await?;
    },
    multi_thread: true,
}

tier2_offline_agent_cases! {
    offline_after_read: (90, false, Access::Read),
    offline_after_edit: (92, false, Access::Edit),
    offline_after_admin: (94, false, Access::Admin),
    offline_before_read: (96, true, Access::Read),
    offline_before_edit: (98, true, Access::Edit),
    offline_before_admin: (100, true, Access::Admin),
}

// ─── Full group/nested-group matrix ──────────────────────────────────────────

utils_rs::table_tests! {
    tier2_group_cases tokio,
    (seed, access, before_content, offline, nested),
    {
        run_group_case(seed, access, before_content, offline, nested).await?;
    },
    multi_thread: true,
}

tier2_group_cases! {
    connected_after_read: (102, Access::Read, false, false, false),
    connected_after_edit: (103, Access::Edit, false, false, false),
    connected_after_admin: (104, Access::Admin, false, false, false),
    connected_before_read: (105, Access::Read, true, false, false),
    connected_before_edit: (106, Access::Edit, true, false, false),
    connected_before_admin: (107, Access::Admin, true, false, false),
    offline_after_read: (108, Access::Read, false, true, false),
    offline_after_edit: (109, Access::Edit, false, true, false),
    offline_after_admin: (110, Access::Admin, false, true, false),
    offline_before_read: (111, Access::Read, true, true, false),
    offline_before_edit: (112, Access::Edit, true, true, false),
    offline_before_admin: (113, Access::Admin, true, true, false),
    nested_connected_after_read: (114, Access::Read, false, false, true),
    nested_connected_after_edit: (115, Access::Edit, false, false, true),
    nested_connected_after_admin: (116, Access::Admin, false, false, true),
    nested_connected_before_read: (117, Access::Read, true, false, true),
    nested_connected_before_edit: (118, Access::Edit, true, false, true),
    nested_connected_before_admin: (119, Access::Admin, true, false, true),
    nested_offline_after_read: (120, Access::Read, false, true, true),
    nested_offline_after_edit: (121, Access::Edit, false, true, true),
    nested_offline_after_admin: (122, Access::Admin, false, true, true),
    nested_offline_before_read: (123, Access::Read, true, true, true),
    nested_offline_before_edit: (124, Access::Edit, true, true, true),
    nested_offline_before_admin: (125, Access::Admin, true, true, true),
}

async fn run_group_case(
    seed: u8,
    access: Access,
    before_content: bool,
    offline: bool,
    nested: bool,
) -> crate::Res<()> {
    let label = if nested {
        "NestedMatrix"
    } else {
        "GroupMatrix"
    };
    let mut pair = Pair::boot(seed, seed.wrapping_add(1), "Owner", label).await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    if before_content {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating group seed doc: {err:?}"))?;
    } else {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "title", "group-matrix"))
            .map_err(|err| crate::ferr!("failed creating group doc: {err:?}"))?;
    }
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let target_group = if nested {
        let inner = pair.left().repo.create_group_with_parents(vec![]).await?;
        let outer = pair.left().repo.create_group_with_parents(vec![]).await?;
        pair.left()
            .repo
            .add_member_to_group(member_agent, &inner, access)
            .await?;
        pair.left()
            .repo
            .add_member_to_group(inner, &outer, access)
            .await?;
        outer
    } else {
        let group = pair.left().repo.create_group_with_parents(vec![]).await?;
        pair.left()
            .repo
            .add_member_to_group(member_agent, &group, access)
            .await?;
        group
    };

    if offline {
        // Establish the group proof chain while connected. The matrix's
        // offline dimension is the document grant, not the creation of the
        // group/member delegation that the grant depends on. Otherwise the
        // reconnect can deliver the document grant before its root proof.
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        fixtures::go_offline(&mut pair).await?;
        pair.left()
            .repo
            .grant_doc_access(doc_id, target_group.clone(), access)
            .await?;
        pair.connect().await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        fixtures::assert_reader_has_access(&pair.right().repo, doc_id).await?;
    } else {
        fixtures::grant_group_and_propagate(&pair, doc_id, &target_group, access).await?;
    }

    if before_content {
        owner_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "group-matrix"))
                    .map_err(|err| crate::ferr!("failed writing after group grant: {err:?}"))
            })
            .await??;
    }

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    if read_title(&member_doc).await != "group-matrix" {
        return Err(crate::ferr!(concat!(
            "{label} {access} case did not materialize the expected title ",
            "(before_content={before_content}, offline={offline})"
        )));
    }

    if access.is_editor() {
        member_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "member_note", "group-member"))
                    .map_err(|err| crate::ferr!("failed group member write: {err:?}"))
            })
            .await??;
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        drop(owner_doc);
        let owner_doc =
            fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
        pair.right_conn()
            .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
            .await?;
        pair.left()
            .repo
            .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
            .await?;
        assert_eq!(
            read_optional_text(&owner_doc, "member_note")
                .await
                .as_deref(),
            Some("group-member")
        );
        drop(owner_doc);
    } else {
        drop(owner_doc);
    }

    drop(member_doc);
    Ok(())
}

// ─── Public and document-as-member cases ────────────────────────────────────

async fn run_public_case(
    seed: u8,
    offline: bool,
    before_content: bool,
    access: Access,
) -> crate::Res<()> {
    let mut pair = Pair::boot(seed, seed.wrapping_add(1), "Owner", "PublicReader").await?;
    let mut initial = automerge::Automerge::new();
    if before_content {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating public seed doc: {err:?}"))?;
    } else {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "title", "public-matrix"))
            .map_err(|err| crate::ferr!("failed creating public doc: {err:?}"))?;
    }
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    if offline {
        fixtures::go_offline(&mut pair).await?;
    }
    pair.left()
        .repo
        .grant_doc_access(doc_id, fixtures::public_agent(), access)
        .await?;
    if offline {
        pair.connect().await?;
    }
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    if before_content {
        owner_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "public-matrix"))
                    .map_err(|err| crate::ferr!("failed writing public content: {err:?}"))
            })
            .await??;
    }
    let public_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&public_doc).await, "public-matrix");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &public_doc).await?;
    if access.is_editor() {
        public_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "public_note", "public-member"))
                    .map_err(|err| crate::ferr!("failed public member write: {err:?}"))
            })
            .await??;
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        drop(owner_doc);
        let owner_doc =
            fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
        assert_eq!(
            read_optional_text(&owner_doc, "public_note")
                .await
                .as_deref(),
            Some("public-member")
        );
        heads::tier0_invariants(&pair, doc_id, &owner_doc, &public_doc).await?;
        drop(owner_doc);
    } else {
        drop(owner_doc);
    }
    drop(public_doc);
    Ok(())
}

utils_rs::table_tests! {
    tier2_public_grant_cases tokio,
    (seed, offline, before_content, access),
    {
        run_public_case(seed, offline, before_content, access).await?;
    },
    multi_thread: true,
}

tier2_public_grant_cases! {
    connected_after_read: (130, false, false, Access::Read),
    connected_after_edit: (132, false, false, Access::Edit),
    connected_after_admin: (134, false, false, Access::Admin),
    connected_before_read: (136, false, true, Access::Read),
    connected_before_edit: (138, false, true, Access::Edit),
    connected_before_admin: (140, false, true, Access::Admin),
    offline_after_read: (142, true, false, Access::Read),
    offline_after_edit: (144, true, false, Access::Edit),
    offline_after_admin: (146, true, false, Access::Admin),
    offline_before_read: (148, true, true, Access::Read),
    offline_before_edit: (150, true, true, Access::Edit),
    offline_before_admin: (152, true, true, Access::Admin),
}

// A document member is introduced as a target document coparent. This keeps
// the delegation rooted in the target's initial membership graph; adding a
// document agent to an already-created target can produce an invalid root
// proof when the two delegation chains arrive concurrently.
utils_rs::table_tests! {
    tier2_document_as_member_cases tokio,
    (seed, offline, before_content, access),
    {
        run_document_as_member_case(seed, offline, before_content, access).await?;
    },
    multi_thread: true,
}

tier2_document_as_member_cases! {
    connected_after_read: (160, false, false, Access::Read),
    connected_after_edit: (162, false, false, Access::Edit),
    connected_after_admin: (164, false, false, Access::Admin),
    connected_before_read: (166, false, true, Access::Read),
    connected_before_edit: (168, false, true, Access::Edit),
    connected_before_admin: (170, false, true, Access::Admin),
    offline_after_read: (172, true, false, Access::Read),
    offline_after_edit: (174, true, false, Access::Edit),
    offline_after_admin: (176, true, false, Access::Admin),
    offline_before_read: (178, true, true, Access::Read),
    offline_before_edit: (180, true, true, Access::Edit),
    offline_before_admin: (182, true, true, Access::Admin),
}

async fn run_document_as_member_case(
    seed: u8,
    offline: bool,
    before_content: bool,
    access: Access,
) -> crate::Res<()> {
    let mut pair = Pair::boot(seed, seed.wrapping_add(1), "Owner", "DocMember").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut source_initial = automerge::Automerge::new();
    if before_content {
        source_initial
            .transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating source seed doc: {err:?}"))?;
    } else {
        source_initial
            .transact(|tx| tx.put(automerge::ROOT, "source", true))
            .map_err(|err| crate::ferr!("failed creating source doc: {err:?}"))?;
    }
    let source_doc = pair.left().repo.create_doc(source_initial).await?;
    let source_id = source_doc.document_id();

    if offline {
        fixtures::go_offline(&mut pair).await?;
    }
    pair.left()
        .repo
        .grant_doc_access(source_id, member_agent, access)
        .await?;
    let source_agent = fixtures::document_agent(&pair.left().repo, source_id).await?;

    let mut target_initial = automerge::Automerge::new();
    if before_content {
        target_initial
            .transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating target seed doc: {err:?}"))?;
    } else {
        target_initial
            .transact(|tx| tx.put(automerge::ROOT, "title", "doc-member-matrix"))
            .map_err(|err| crate::ferr!("failed creating target doc: {err:?}"))?;
    }
    let target_doc = pair
        .left()
        .repo
        .create_doc_with_parents(target_initial, vec![source_agent.into()])
        .await?;
    let target_id = target_doc.document_id();

    if before_content {
        source_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "source", true))
                    .map_err(|err| crate::ferr!("failed writing source content: {err:?}"))
            })
            .await??;
        target_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "title", "doc-member-matrix"))
                    .map_err(|err| crate::ferr!("failed writing target content: {err:?}"))
            })
            .await??;
    }
    if offline {
        pair.connect().await?;
    }
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, target_id).await?;
    assert_eq!(read_title(&member_doc).await, "doc-member-matrix");
    heads::tier0_invariants(&pair, target_id, &target_doc, &member_doc).await?;
    if access.is_editor() {
        member_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "member_note", "doc-member"))
                    .map_err(|err| crate::ferr!("failed document-member write: {err:?}"))
            })
            .await??;
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        drop(target_doc);
        let target_doc =
            fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, target_id).await?;
        pair.right_conn()
            .sync_doc_with_peer(target_id, Some(std::time::Duration::from_secs(10)))
            .await?;
        pair.left()
            .repo
            .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
            .await?;
        assert_eq!(
            read_optional_text(&target_doc, "member_note")
                .await
                .as_deref(),
            Some("doc-member")
        );
        heads::tier0_invariants(&pair, target_id, &target_doc, &member_doc).await?;
        drop(target_doc);
    } else {
        drop(target_doc);
    }
    drop(source_doc);
    drop(member_doc);
    Ok(())
}

// ─── None / no-grant matrix ──────────────────────────────────────────────────

async fn run_no_grant_case(seed: u8, offline: bool, before_content: bool) -> crate::Res<()> {
    let mut pair = Pair::boot(seed, seed.wrapping_add(1), "Owner", "StrangerMatrix").await?;
    let mut initial = automerge::Automerge::new();
    if before_content {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "_init", true))
            .map_err(|err| crate::ferr!("failed creating no-grant seed doc: {err:?}"))?;
    } else {
        initial
            .transact(|tx| tx.put(automerge::ROOT, "secret", "unauthorized"))
            .map_err(|err| crate::ferr!("failed creating no-grant doc: {err:?}"))?;
    }
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    if offline {
        fixtures::go_offline(&mut pair).await?;
        pair.connect().await?;
    }
    if before_content {
        owner_doc
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "secret", "unauthorized"))
                    .map_err(|err| crate::ferr!("failed writing no-grant content: {err:?}"))
            })
            .await??;
    }
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    match pair.right().repo.get_doc(&doc_id).await {
        Ok(crate::DocLookup::Ready(_)) => {
            return Err(crate::ferr!(concat!(
                "stranger materialized a no-grant document ",
                "(offline={offline}, before_content={before_content})"
            )));
        }
        Ok(crate::DocLookup::Missing | crate::DocLookup::PendingMaterialization) | Err(_) => {}
    }
    let state = pair.right().repo.doc_head_state(doc_id).await?;
    if !state.sedimentree_heads.is_empty() {
        return Err(crate::ferr!(
            "stranger received no-grant sedimentree heads (offline={offline}, before_content={before_content}): {:?}",
            state.sedimentree_heads
        ));
    }
    drop(owner_doc);
    Ok(())
}

utils_rs::table_tests! {
    tier2_no_grant_cases tokio,
    (seed, offline, before_content),
    {
        run_no_grant_case(seed, offline, before_content).await?;
    },
    multi_thread: true,
}

tier2_no_grant_cases! {
    connected_before: (190, false, true),
    connected_after: (192, false, false),
    offline_before: (194, true, true),
    offline_after: (196, true, false),
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
