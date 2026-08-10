//! Tier 6 — Keyhive capability semantics, causal ordering, CGKA operations,
//! and history-inclusive access.
//!
//! Tests here exercise the document-level CGKA and BeeKEM state transitions
//! triggered by group membership changes. They intentionally encode the
//! intended contract first; a failing test is evidence for the production or
//! upstream Keyhive fix that follows.
//!
//! # Scenarios
//!
//! | Test                                          | Coverage                                                |
//! |-----------------------------------------------|---------------------------------------------------------|
//! | `group_doc_grant_then_add_user`               | Grant group to doc, add user. CGKA must change,         |
//! |                                               | user materializes history.                              |
//! | `same_group_multiple_docs`                    | One group granted to two docs, add user. Both CGKAs     |
//! |                                               | change.                                                 |
//! | `nested_group_propagates_cgka`                | Nested group has doc access, add user to inner.         |
//! |                                               | CGKA must change through the nesting chain.             |
//! | `multipath_dedup`                             | User in two groups both with doc access. CGKA ops       |
//! |                                               | must not be duplicated per add.                         |
//! | `history_inclusive_access`                    | User can read pre-membership content after CGKA         |
//! |                                               | propagates.                                             |
//! | `group_add_checkpoint`                        | BigRepo emits CGKA membership plus a history checkpoint |
//! |                                               | for a group member added after document creation.       |

use super::harness::{Pair, Topo, fixtures, keyhive as kh_snap};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;
use std::collections::BTreeSet;

async fn read_text(handle: &crate::BigDocHandle, key: &str) -> Option<String> {
    handle
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, key)
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(value) => match value.as_ref() {
                        ScalarValue::Str(s) => Some(s.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await
}

/// A membership rotation and a write from a disconnected old-epoch member are
/// concurrent siblings. Once the writer reconnects, current readers must gain
/// a decryptable causal entrypoint covering that offline head; converging the
/// Keyhive event graph and sedimentree alone is insufficient.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_concurrent_member_add_and_offline_old_epoch_write_converges() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let topo = Topo::boot_triangle(238, 239, 240, "OfflineWriter", "Admin", "NewReader").await?;
    let writer = topo.topo_node(0);
    let admin = topo.topo_node(1);
    let reader = topo.topo_node(2);

    let admin_agent = fixtures::agent_of(&writer.repo, admin).await?;
    let reader_agent = fixtures::agent_of(&admin.repo, reader).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "base"))
        .map_err(|err| crate::ferr!("failed creating base document: {err:?}"))?;
    let writer_doc = writer.repo.create_doc(initial).await?;
    let doc_id = writer_doc.document_id();

    // Admin belongs to the epoch the writer will retain while offline.
    writer
        .repo
        .grant_doc_access(doc_id, admin_agent, Access::Admin)
        .await?;
    topo.topo_conn(1, 0).sync_keyhive_with_peer(None).await?;
    let (_writer_doc, admin_doc) = fixtures::sync_doc_bidirectional(
        topo.topo_conn(0, 1),
        topo.topo_conn(1, 0),
        &writer.repo,
        &admin.repo,
        doc_id,
    )
    .await?;

    // Isolate the writer completely. Admin then adds Reader and emits the
    // normal history checkpoint under the new epoch.
    writer.disconnect_peer(admin.peer_id()).await?;
    admin.disconnect_peer(writer.peer_id()).await?;
    writer.disconnect_peer(reader.peer_id()).await?;
    reader.disconnect_peer(writer.peer_id()).await?;

    admin
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    topo.topo_conn(2, 1).sync_keyhive_with_peer(None).await?;
    let (_admin_doc, reader_doc_before_offline_write) = fixtures::sync_doc_bidirectional(
        topo.topo_conn(1, 2),
        topo.topo_conn(2, 1),
        &admin.repo,
        &reader.repo,
        doc_id,
    )
    .await?;

    // This commit uses the writer's old epoch. It is concurrent with Admin's
    // reader-add checkpoint rather than an ancestor of that checkpoint.
    writer_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "offline", "old-epoch-write"))
                .map_err(|err| crate::ferr!("failed offline write: {err:?}"))
        })
        .await??;

    // Reconnect Writer↔Admin, converge Keyhive first, then propagate the
    // offline payload through Admin to Reader.
    let writer_to_admin = writer.connect(admin).await?;
    let admin_to_writer = admin.accepted_connection().await;
    writer_to_admin.sync_keyhive_with_peer(None).await?;
    admin_to_writer.sync_keyhive_with_peer(None).await?;
    let (_writer_doc, _admin_doc_after_reconnect) = fixtures::sync_doc_bidirectional(
        &writer_to_admin,
        &admin_to_writer,
        &writer.repo,
        &admin.repo,
        doc_id,
    )
    .await?;

    let (admin_doc_after_heal, reader_doc) = fixtures::sync_doc_bidirectional(
        topo.topo_conn(1, 2),
        topo.topo_conn(2, 1),
        &admin.repo,
        &reader.repo,
        doc_id,
    )
    .await?;
    fixtures::wait_for_network_rest(
        [writer, admin, reader].as_slice(),
        std::time::Duration::from_secs(20),
    )
    .await?;
    let settled_blob_counts = futures::future::try_join_all(
        [writer, admin, reader]
            .into_iter()
            .map(|node| node.repo.inspect_stored_doc_blobs(doc_id)),
    )
    .await?
    .into_iter()
    .map(|blobs| blobs.len())
    .collect::<Vec<_>>();
    fixtures::wait_for_network_rest(
        [writer, admin, reader].as_slice(),
        std::time::Duration::from_secs(20),
    )
    .await?;
    let replayed_blob_counts = futures::future::try_join_all(
        [writer, admin, reader]
            .into_iter()
            .map(|node| node.repo.inspect_stored_doc_blobs(doc_id)),
    )
    .await?
    .into_iter()
    .map(|blobs| blobs.len())
    .collect::<Vec<_>>();
    assert_eq!(
        replayed_blob_counts, settled_blob_counts,
        "replayed healing attempts must not create a checkpoint storm"
    );
    let read_offline = |doc: &automerge::Automerge| {
        let Ok(Some((automerge::Value::Scalar(value), _))) = doc.get(automerge::ROOT, "offline")
        else {
            return None;
        };
        match value.as_ref() {
            ScalarValue::Str(value) => Some(value.to_string()),
            _ => None,
        }
    };
    assert_eq!(
        writer_doc.with_document_read(read_offline).await.as_deref(),
        Some("old-epoch-write"),
        "offline writer lost its own persisted write"
    );
    assert_eq!(
        admin_doc_after_heal
            .with_document_read(read_offline)
            .await
            .as_deref(),
        Some("old-epoch-write"),
        "healing admin did not materialize the offline write"
    );
    assert_eq!(
        reader_doc.with_document_read(read_offline).await.as_deref(),
        Some("old-epoch-write"),
        "new reader did not materialize the healed offline write"
    );

    drop(admin_doc);
    drop(reader_doc_before_offline_write);
    Ok(())
}

// ─── Immediate write after parented document creation ──────────────────────

/// Creating a parented document returns a writable handle. The PCS update used
/// for its initial commit must leave enough local secret state for the next
/// commit; no network synchronization is involved in this invariant.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_parented_document_handle_is_immediately_writable() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(198, 199, "Owner", "IdlePeer").await?;

    for parent_count in 0..=2 {
        let mut parents = Vec::new();
        for _ in 0..parent_count {
            parents.push(
                pair.left()
                    .repo
                    .create_group_with_parents(Vec::new())
                    .await?
                    .into(),
            );
        }

        let mut initial = automerge::Automerge::new();
        initial
            .transact(|tx| tx.put(automerge::ROOT, "phase", "initial"))
            .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
        let handle = pair
            .left()
            .repo
            .create_doc_with_parents(initial, parents)
            .await?;

        handle
            .with_document(|doc| {
                doc.transact(|tx| tx.put(automerge::ROOT, "phase", "second"))
                    .map_err(|err| crate::ferr!("failed second write: {err:?}"))
            })
            .await??;
    }

    Ok(())
}

/// Persisting a clone-agent grant and reopening the source must restore the
/// private CGKA material behind its authority groups, not only the public graph.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_reopen_after_authority_grant_keeps_new_documents_writable() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("left");
    let right_path = temp.path().join("right");
    let left_storage = crate::StorageConfig::Disk {
        path: left_path.clone(),
    };
    let mut pair = Pair::boot_persistent(196, 197, "Owner", "Clone", left_path, right_path).await?;

    let repo_agents = pair
        .left()
        .repo
        .create_group_with_parents(Vec::new())
        .await?;
    let content = pair
        .left()
        .repo
        .create_group_with_parents(Vec::new())
        .await?;
    let drawer = pair
        .left()
        .repo
        .create_group_with_parents(Vec::new())
        .await?;
    let content_id = content.id().to_bytes();
    let drawer_id = drawer.id().to_bytes();
    let local_agent = pair.left().repo.local_keyhive_agent().await?;
    pair.left()
        .repo
        .add_admin_member_to_group(local_agent, &repo_agents)
        .await?;
    pair.left()
        .repo
        .add_admin_member_to_group(repo_agents.clone(), &content)
        .await?;
    pair.left()
        .repo
        .add_admin_member_to_group(repo_agents.clone(), &drawer)
        .await?;

    let cloned_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .add_admin_member_to_group(cloned_agent, &repo_agents)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    pair.restart_left(left_storage).await?;
    let content = pair
        .left()
        .repo
        .get_group_by_id(content_id)
        .await
        .expect("content group must survive restart");
    let drawer = pair
        .left()
        .repo
        .get_group_by_id(drawer_id)
        .await
        .expect("drawer group must survive restart");

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "initial"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let handle = pair
        .left()
        .repo
        .create_doc_with_parents(initial, vec![content.into(), drawer.into()])
        .await?;
    let doc_id = handle.document_id();
    handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "second"))
                .map_err(|err| crate::ferr!("failed second write: {err:?}"))
        })
        .await??;

    let snap = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    assert!(!snap.cgka_operation_hashes.is_empty(), "CGKA operations must be present for doc");
    assert_eq!(read_text(&handle, "phase").await.as_deref(), Some("second"), "document must remain usable");

    Ok(())
}

/// A source must retain the private key for an existing document rotated by a
/// transitive authority-group membership change across restart.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_existing_governed_document_survives_grant_and_restart() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("left");
    let right_path = temp.path().join("right");
    let left_storage = crate::StorageConfig::Disk {
        path: left_path.clone(),
    };
    let right_storage = crate::StorageConfig::Disk {
        path: right_path.clone(),
    };
    let mut pair = Pair::boot_persistent(194, 195, "Owner", "Clone", left_path, right_path).await?;

    let repo_agents = pair
        .left()
        .repo
        .create_group_with_parents(Vec::new())
        .await?;
    let core_docs = pair
        .left()
        .repo
        .create_group_with_parents(Vec::new())
        .await?;
    let local_agent = pair.left().repo.local_keyhive_agent().await?;
    pair.left()
        .repo
        .add_admin_member_to_group(local_agent, &repo_agents)
        .await?;
    pair.left()
        .repo
        .add_admin_member_to_group(repo_agents.clone(), &core_docs)
        .await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "before-grant"))
        .map_err(|err| crate::ferr!("failed creating core doc: {err:?}"))?;
    let core_handle = pair
        .left()
        .repo
        .create_doc_with_parents(initial, vec![core_docs.into()])
        .await?;
    let core_doc_id = core_handle.document_id();

    let cloned_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .add_admin_member_to_group(cloned_agent, &repo_agents)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    let clone_handle =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, core_doc_id).await?;
    clone_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "clone_phase", "opened"))
                .map_err(|err| crate::ferr!("failed clone write: {err:?}"))
        })
        .await??;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    drop(clone_handle);
    drop(core_handle);

    pair.restart_left(left_storage).await?;
    pair.restart_right(right_storage).await?;
    let core_handle = match pair.left().repo.get_doc(&core_doc_id).await? {
        crate::DocLookup::Ready(handle) => handle,
        other => {
            return Err(crate::ferr!(
                "core document was not ready after restart: {other:?}"
            ));
        }
    };
    pair.connect().await?;
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&core_doc_id.into_bytes())
        .map_err(|_| crate::ferr!("core document id is not a valid Ed25519 point"))?;
    let kh_doc_id = keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(verifying_key),
    );
    let remote_keyhive = pair.right().repo.keyhive().clone_keyhive();
    let remote_doc = remote_keyhive
        .get_document(kh_doc_id)
        .await
        .ok_or_else(|| crate::ferr!("clone is missing core Keyhive document"))?;
    remote_keyhive.force_pcs_update(remote_doc).await?;
    pair.right()
        .repo
        .wait_for_keyhive_reconciliation(None)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    core_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "after-restart"))
                .map_err(|err| crate::ferr!("failed post-restart write: {err:?}"))
        })
        .await??;

    Ok(())
}

// ─── Group grant, then add user ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier6_group_doc_grant_then_add_user() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(200, 201, "Owner", "NewMember").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "group-cgka"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let before = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;

    pair.left()
        .repo
        .add_member_to_group(member_agent, &group, Access::Read)
        .await?;

    let after = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;

    assert_ne!(
        before.cgka_operation_hashes, after.cgka_operation_hashes,
        "group member add must update the containing document's CGKA"
    );

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // History-inclusive: user reads content written before membership.
    let new_member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let title = new_member_doc
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
    assert_eq!(
        title.as_deref(),
        Some("group-cgka"),
        "new group member must be able to read content written before their membership"
    );
    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(owner_doc);
    Ok(())
}

// ─── Same group, multiple documents ─────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier6_same_group_multiple_docs() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(202, 203, "Owner", "MultiDocMember").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    // Owner creates two documents with distinct content.
    let mut d1 = automerge::Automerge::new();
    d1.transact(|tx| tx.put(automerge::ROOT, "tag", "doc-a"))
        .map_err(|err| crate::ferr!("failed creating doc A: {err:?}"))?;
    let doc_a = pair.left().repo.create_doc(d1).await?;
    let doc_a_id = doc_a.document_id();

    let mut d2 = automerge::Automerge::new();
    d2.transact(|tx| tx.put(automerge::ROOT, "tag", "doc-b"))
        .map_err(|err| crate::ferr!("failed creating doc B: {err:?}"))?;
    let doc_b = pair.left().repo.create_doc(d2).await?;
    let doc_b_id = doc_b.document_id();

    // One group granted to BOTH documents.
    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_a_id, group.clone(), Access::Read)
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_b_id, group.clone(), Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let before_a = kh_snap::document_snapshot(&pair.left().repo, doc_a_id).await?;
    let before_b = kh_snap::document_snapshot(&pair.left().repo, doc_b_id).await?;
    let blobs_before_a = pair
        .left()
        .repo
        .inspect_stored_doc_blobs(doc_a_id)
        .await?
        .len();
    let blobs_before_b = pair
        .left()
        .repo
        .inspect_stored_doc_blobs(doc_b_id)
        .await?
        .len();

    // Add the new member to the group — BOTH documents' CGKA must change.
    pair.left()
        .repo
        .add_member_to_group(member_agent, &group, Access::Read)
        .await?;

    let after_a = kh_snap::document_snapshot(&pair.left().repo, doc_a_id).await?;
    let after_b = kh_snap::document_snapshot(&pair.left().repo, doc_b_id).await?;

    assert_ne!(
        before_a.cgka_operation_hashes, after_a.cgka_operation_hashes,
        "doc A's CGKA must change when a member is added to a governing group"
    );
    assert_eq!(
        pair.left()
            .repo
            .inspect_stored_doc_blobs(doc_a_id)
            .await?
            .len(),
        blobs_before_a + 1,
        "the group rotation must checkpoint doc A exactly once"
    );
    assert_eq!(
        pair.left()
            .repo
            .inspect_stored_doc_blobs(doc_b_id)
            .await?
            .len(),
        blobs_before_b + 1,
        "the group rotation must checkpoint doc B exactly once"
    );
    assert_ne!(
        before_b.cgka_operation_hashes, after_b.cgka_operation_hashes,
        "doc B's CGKA must change when a member is added to the same governing group"
    );

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Both documents must be readable by the new member.
    let reader_a =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_a_id).await?;
    let tag_a = reader_a
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "tag")
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
    assert_eq!(tag_a.as_deref(), Some("doc-a"));

    let reader_b =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_b_id).await?;
    let tag_b = reader_b
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "tag")
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
    assert_eq!(tag_b.as_deref(), Some("doc-b"));

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_a_id).await?;
    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_b_id).await?;

    drop(doc_a);
    drop(doc_b);
    Ok(())
}

// ─── Nested group propagation ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier6_nested_group_propagates_cgka() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(204, 205, "Owner", "NestedMember").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "nested-cgka"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Inner group is nested inside the outer group, and the outer group has
    // Read access to the document.
    let inner = pair.left().repo.create_group_with_parents(vec![]).await?;
    let outer = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .add_member_to_group(inner.clone(), &outer, Access::Read)
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, outer.clone(), Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let before = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;

    // Add the user to the inner group. The user should be seeded into the
    // document's CGKA through the inner → outer → doc chain.
    pair.left()
        .repo
        .add_member_to_group(member_agent, &inner, Access::Read)
        .await?;

    let after = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;

    assert_ne!(
        before.cgka_operation_hashes, after.cgka_operation_hashes,
        "adding a member to the outer group must propagate CGKA to the \
         document through the nested group chain"
    );

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let title = member_doc
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
    assert_eq!(title.as_deref(), Some("nested-cgka"));

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(owner_doc);
    Ok(())
}

// ─── Multipath deduplication ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier6_multipath_dedup() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(206, 207, "Owner", "MultiPathUser").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "multipath"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Two independent groups both granted Read access to the same document.
    let alpha = pair.left().repo.create_group_with_parents(vec![]).await?;
    let beta = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, alpha.clone(), Access::Read)
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, beta.clone(), Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Add user to alpha.
    pair.left()
        .repo
        .add_member_to_group(member_agent.clone(), &alpha, Access::Read)
        .await?;
    let after_alpha = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let count_after_alpha = after_alpha.cgka_operation_hashes.len();

    // Add the same user to beta. The CGKA must NOT add duplicate operations
    // for the same member reaching the same document.
    pair.left()
        .repo
        .add_member_to_group(member_agent, &beta, Access::Read)
        .await?;
    let after_beta = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let count_after_beta = after_beta.cgka_operation_hashes.len();

    assert!(
        count_after_beta <= count_after_alpha + 1,
        "adding the same user through a second group path must not duplicate \
         CGKA operations (was {count_after_alpha}, now {count_after_beta})"
    );

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // The user must be able to read through either path.
    let user_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let title = user_doc
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
    assert_eq!(title.as_deref(), Some("multipath"));
    drop(owner_doc);
    Ok(())
}

// ─── History-inclusive access ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier6_history_inclusive_access() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(208, 209, "Owner", "LateMember").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    // Owner creates a document and writes two distinct pieces of content
    // BEFORE the new member is ever added to any group.
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "one"))
        .map_err(|err| crate::ferr!("failed phase one: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "two"))
                .map_err(|err| crate::ferr!("failed phase two: {err:?}"))
        })
        .await??;

    // Create a group and grant it access.
    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Add the new member.
    pair.left()
        .repo
        .add_member_to_group(member_agent, &group, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // The user must be able to read the full history including content
    // written before they were added.
    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let phase = member_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "phase")
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
    assert_eq!(
        phase.as_deref(),
        Some("two"),
        "new member must read the latest content written before membership"
    );

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(owner_doc);
    Ok(())
}

// ─── Group add checkpoint ────────────────────────────────────────────────────

/// BigRepo adds the structural CGKA membership and immediately checkpoints
/// affected documents because its contract is history-inclusive. A subsequent
/// ordinary write must still succeed without requiring another synthetic commit.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_group_add_checkpoint() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(210, 211, "Owner", "FutureEditor").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "future-pcs"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    pair.left()
        .repo
        .add_member_to_group(member_agent, &group, Access::Read)
        .await?;

    // The group add includes the structural CGKA operation and the
    // history-inclusive checkpoint's PCS update.
    let after_add = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let ops_after_add = after_add.cgka_operation_hashes.len();
    assert!(ops_after_add > 0, "group add must leave CGKA state present");

    // Owner writes new content. The write should succeed without requiring a
    // second synthetic checkpoint.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "status", "post-add"))
                .map_err(|err| crate::ferr!("failed writing after add: {err:?}"))
        })
        .await??;

    let after_write = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let ops_after_write = after_write.cgka_operation_hashes.len();
    assert!(
        ops_after_write >= ops_after_add,
        "content write must not discard the group add/checkpoint CGKA state"
    );
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let status = member_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "status")
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
    assert_eq!(status.as_deref(), Some("post-add"));

    drop(owner_doc);
    Ok(())
}

// ─── Structural Add and checkpoint Update ────────────────────────────────────

/// The group member add must produce structural CGKA state, and BigRepo's
/// history-inclusive policy must leave the document readable immediately.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_structural_add_vs_checkpoint_update() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(212, 213, "Owner", "Structural").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "structural"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Baseline: CGKA ops after initial create + grant.
    let baseline = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let baseline_count = baseline.cgka_operation_hashes.len();

    // Add member to group → should emit `Add` CGKA op(s).
    pair.left()
        .repo
        .add_member_to_group(member_agent, &group, Access::Read)
        .await?;
    let after_add = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let after_add_count = after_add.cgka_operation_hashes.len();

    assert!(
        after_add_count > baseline_count,
        "group member add must emit at least one new CGKA Add operation \
         (was {baseline_count}, now {after_add_count})"
    );

    // The group add already caused BigRepo's history checkpoint. A normal
    // content write must preserve that state.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "step", "one"))
                .map_err(|err| crate::ferr!("failed writing: {err:?}"))
        })
        .await??;
    let after_write = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let after_write_count = after_write.cgka_operation_hashes.len();

    assert!(
        after_write_count >= after_add_count,
        "content write must not reduce CGKA state (was {after_add_count}, now {after_write_count})"
    );
    // A second content write should also emit Update ops (or noop if no
    // membership changes occurred in between).
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "step", "two"))
                .map_err(|err| crate::ferr!("failed writing second: {err:?}"))
        })
        .await??;
    let after_write2 = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let after_write2_count = after_write2.cgka_operation_hashes.len();

    // The second write may or may not produce ops depending on whether the
    // PCS key needs rotation again. Do not assert a specific count, but
    // at least confirm the doc is still healthy.
    assert!(
        after_write2_count >= after_write_count,
        "second content write must not reduce CGKA operation count"
    );

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let step = member_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "step")
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
    assert_eq!(step.as_deref(), Some("two"));

    drop(owner_doc);
    Ok(())
}

// ─── Multipath strongest access ────────────────────────────────────────────

/// Two independent groups grant different access levels to the same document:
/// one grants Edit, the other grants Read. A user who is a member of both
/// groups must receive the strongest effective access (Edit), enabling them
/// both to materialise *and* to write, regardless of the order in which the
/// two grants were created or the user was added.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_multipath_strongest_access() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(224, 225, "Owner", "MultiPathEditor").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "multipath-strongest"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Two independent groups with different access levels.
    let group_edit = pair.left().repo.create_group_with_parents(vec![]).await?;
    let group_read = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group_edit.clone(), Access::Edit)
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group_read.clone(), Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;

    // Add the same user to BOTH groups. The effective access through the Edit
    // path is the strongest, so the user must be able to write.
    pair.left()
        .repo
        .add_member_to_group(member_agent.clone(), &group_read, Access::Read)
        .await?;
    pair.left()
        .repo
        .add_member_to_group(member_agent, &group_edit, Access::Edit)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;

    // Materialise: read the pre-grant content.
    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let title = member_doc
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
    assert_eq!(
        title.as_deref(),
        Some("multipath-strongest"),
        "user must materialise pre-grant content through either path"
    );

    // Write post-grant content — this requires Edit (the strongest access).
    member_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "editor_note", "strongest-path"))
                .map_err(|err| crate::ferr!("failed member write: {err:?}"))
        })
        .await??;

    // Sync the edit back to the owner and verify convergence.
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    let note = owner_doc2
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "editor_note")
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
    assert_eq!(
        note.as_deref(),
        Some("strongest-path"),
        "owner must see the edit written under the strongest (Edit) path"
    );

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(owner_doc2);
    drop(owner_doc);
    Ok(())
}

// ─── Grant-after-content explicit frontier ─────────────────────────────────

/// After a `grant_doc_access` call the signed delegation's `after_content`
/// field must equal the sedimentree frontier at the moment the grant was
/// issued. The newly granted reader must be able to read content written
/// *before* their membership began (history-inclusive access).
///
/// This test captures the sedimentree heads just before granting, performs
/// the grant (which internally writes a checkpoint), then inspects the
/// resulting keyhive delegation to confirm the frontier was correctly
/// recorded.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_grant_after_content_explicit_frontier() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(226, 227, "Owner", "FrontierReader").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    // Create a document with some pre-grant content.
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "pre-grant"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Write a second piece of content so there are multiple changes.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "version", "2"))
                .map_err(|err| crate::ferr!("failed writing version: {err:?}"))
        })
        .await??;

    // Capture the authoritative sedimentree frontier BEFORE the grant.
    let pre_grant_heads: BTreeSet<Vec<u8>> = pair
        .left()
        .repo
        .doc_head_state(doc_id)
        .await?
        .sedimentree_heads
        .iter()
        .map(|h| h.0.to_vec())
        .collect();

    let group = pair.left().repo.create_group_with_parents(vec![]).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Add the user to the group — this also triggers a checkpoint and writes
    // a delegation with the sedimentree frontier as `after_content`.
    pair.left()
        .repo
        .add_member_to_group(member_agent.clone(), &group, Access::Read)
        .await?;

    // ── Verify delegation `after_content` ──────────────────────────────
    // Walk the keyhive document's delegation heads and find the newest
    // delegation whose `after_content` for this document contains the
    // pre-grant sedimentree heads.
    {
        use keyhive_core::principal::document::id::DocumentId as KhDocId;
        use keyhive_core::principal::identifier::Identifier;

        let bytes = doc_id.into_bytes();
        let vk = ed25519_dalek::VerifyingKey::from_bytes(&bytes)
            .map_err(|_| crate::ferr!("doc_id invalid"))?;
        let kh_doc_id = KhDocId::from(Identifier::from(vk));

        let keyhive = pair.left().repo.keyhive().clone_keyhive();
        let kh_doc = keyhive
            .get_document(kh_doc_id)
            .await
            .ok_or_else(|| crate::ferr!("keyhive document missing after grant"))?;
        let locked = kh_doc.lock().await;

        let mut found = false;
        for delegation in locked.delegation_heads().values() {
            let deps = delegation.payload().after();
            if let Some(after_refs) = deps.content.get(&kh_doc_id) {
                let after_set: BTreeSet<Vec<u8>> = after_refs.iter().cloned().collect();
                if after_set == pre_grant_heads {
                    found = true;
                    break;
                }
            }
        }
        assert!(
            found,
            "no delegation found whose after_content matches the \
             pre-grant sedimentree frontier"
        );
    }

    // The checkpoint after the grant adds a new head, so the current
    // sedimentree heads are a strict superset of the delegation's
    // after_content.

    // ── Verify history-inclusive access ──────────────────────────────────
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    let phase = member_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "phase")
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
    assert_eq!(
        phase.as_deref(),
        Some("pre-grant"),
        "newly granted reader must read pre-grant history"
    );

    let version = member_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "version")
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
    assert_eq!(
        version.as_deref(),
        Some("2"),
        "newly granted reader must read the second pre-grant change"
    );

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(member_doc);
    drop(owner_doc);
    Ok(())
}
