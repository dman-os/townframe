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

use super::harness::{fixtures, keyhive as kh_snap, Pair};
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;
use std::collections::BTreeSet;

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
    pair.right_conn().sync_keyhive_with_peer(None).await?;

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
    pair.right_conn().sync_keyhive_with_peer(None).await?;

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
    pair.left_conn().sync_keyhive_with_peer(None).await?;
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
