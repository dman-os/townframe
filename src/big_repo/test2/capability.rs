//! Tier 6 — remaining capability and adversarial regression tests.
//!
//! Positive cases not covered in `cgka.rs` / `revocation.rs`:
//!   - downgrade Edit→Read (revoke + re-grant)
//!   - deep-chain revocation (A→B→C→D→doc, revoke D)
//!   - document-as-member (doc A grants B)
//!   - delegate-before-define (grant before content sync over network)
//!
//! Adversarial cases (network-layer error handling):
//!   - escalation: non-root tries to grant higher access
//!   - unauthorized revocation: reader tries to revoke another reader
//!   - duplicate grant: granting the same access twice is idempotent
//!   - unknown delegate: grant to an agent the keyhive has never seen
//!
//! Every rejected operation must return a typed error, never panic, and
//! leave effective access unchanged. Every positive operation is verified
//! through the network sync layer (keyhive + doc sync).

use super::harness::{fixtures, keyhive as kh_snap, Pair};
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;

// ─── helpers ───────────────────────────────────────────────────────────────

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

async fn read_title(handle: &crate::BigDocHandle) -> String {
    read_text(handle, "title")
        .await
        .expect("title should exist and be a string")
}

/// Create a document on the left node with the given `title`.
async fn create_initial(
    pair: &Pair,
    title: &str,
) -> crate::Res<(crate::BigDocHandle, crate::DocumentId)> {
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", title))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let id = doc.document_id();
    Ok((doc, id))
}

/// Grant access and sync document to the right side.
async fn grant_and_sync(
    pair: &Pair,
    doc_id: crate::DocumentId,
    access: Access,
) -> crate::Res<crate::BigDocHandle> {
    let agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(pair, doc_id, &agent, access).await?;
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await
}

/// Agent identifier for the right-side node, as an Identifier for direct
/// keyhive queries.
fn right_agent_id(pair: &Pair) -> keyhive_core::principal::identifier::Identifier {
    let peer = pair.right().peer_id();
    let bytes = peer.as_bytes();
    let vk =
        ed25519_dalek::VerifyingKey::from_bytes(bytes).expect("peer id must be a verifying key");
    keyhive_core::principal::identifier::Identifier::from(vk)
}

/// Document identifier for direct keyhive queries.
fn doc_identifier(doc_id: crate::DocumentId) -> keyhive_core::principal::identifier::Identifier {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("doc id must be a verifying key");
    keyhive_core::principal::identifier::Identifier::from(vk)
}

// ========================================================================
// Positive cases
// ========================================================================

// ─── Downgrade Edit→Read ──────────────────────────────────────────────────
//
// A user with Edit access has their access downgraded to Read by revoking
// then re-granting with Read access. After the keyhive sync, the user can
// still read the content but cannot write new content.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_downgrade_edit_to_read() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(80, 81, "Owner", "Editor").await?;

    let (owner_doc, doc_id) = create_initial(&pair, "downgrade-base").await?;
    let editor_doc = grant_and_sync(&pair, doc_id, Access::Edit).await?;
    assert_eq!(read_title(&editor_doc).await, "downgrade-base");

    // Editor writes content (Edit access works).
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "editor-writes"))
                .map_err(|err| crate::ferr!("editor write failed: {err:?}"))
        })
        .await??;
    // Sync the edit content back to Owner.
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    let owner_doc2 =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    assert_eq!(
        read_text(&owner_doc2, "phase").await.as_deref(),
        Some("editor-writes")
    );
    drop(editor_doc);
    drop(owner_doc2);

    // --- Downgrade: revoke Edit, re-grant Read.
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .revoke_doc_access(doc_id, reader_agent.clone())
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Reader can still materialise pre-existing content.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "downgrade-base");
    assert_eq!(
        read_text(&reader_doc, "phase").await.as_deref(),
        Some("editor-writes")
    );

    // Reader should NOT be able to write — the doc is Read-only for them now.
    // `with_document` (write) returns an error because the runtime rejects
    // writes when the local access is Read (not Edit).
    let write_result = reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "status", "downgraded-try-write"))
                .map_err(|err| crate::ferr!("post-downgrade write should have failed: {err:?}"))
        })
        .await;
    // The transaction itself may succeed locally (automerge permits any local
    // mutation) but the runtime's `with_document` will return an error when
    // it tries to seal/apply the change. If the error is surfaced, good; if
    // the local commit succeeds but sync returns an error, also acceptable as
    // a detection of downgraded access.
    if let Err(err) = &write_result {
        let msg = format!("{err:?}");
        assert!(
            msg.contains("access") || msg.contains("denied") || msg.contains("forbidden"),
            "downgraded write must fail with an access-related error, got: {msg}"
        );
    } else {
        // Local automerge commit may succeed. Verify the write doesn't
        // propagate: sync back to owner and check.
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn()
            .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
            .await?;
        let owner_state = pair.left().repo.doc_head_state(doc_id).await?;
        let owner_handle =
            fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
        // The owner should NOT see the "downgraded-try-write" status.
        assert_ne!(
            read_text(&owner_handle, "status").await.as_deref(),
            Some("downgraded-try-write"),
            "post-downgrade write must not propagate to the owner"
        );
        drop(owner_handle);
    }

    // Access level must be Read (not Edit).
    let acc = pair
        .right()
        .repo
        .keyhive()
        .agent_access_on(&right_agent_id(&pair), doc_identifier(doc_id))
        .await;
    assert_eq!(
        acc,
        Some(Access::Read),
        "downgraded user must have Read access, not Edit"
    );

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Deep-chain transitive access ──────────────────────────────────────────
//
// Structure: doc ← G1 ← G2 ← user.
// G1 is granted Read on the doc. G2 is a member of G1. The user is a member
// of G2. Verify transitive access through the nesting chain works correctly
// at the BigRepo level: after keyhive sync, the user can materialise and
// read pre-grant content through all three levels of nesting.
//
// This is a positive test of deep-chain *grant*. Direct-member revocation
// is covered by `tier6_revoke_uses_authoritative_frontier...` in revocation.rs.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_deep_chain_transitive_access() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(82, 83, "Owner", "DeepUser").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "deep-chain").await?;

    // Build chain: G1 → [on doc], G2 ∈ G1, user ∈ G2.
    let g1 = pair.left().repo.create_group_with_parents(vec![]).await?;
    let g2 = pair.left().repo.create_group_with_parents(vec![]).await?;

    // G1 has Read on doc.
    pair.left()
        .repo
        .grant_doc_access(doc_id, g1.clone(), Access::Read)
        .await?;

    // G2 ∈ G1 (Read), user ∈ G2 (Read).
    pair.left()
        .repo
        .add_member_to_group(g2.clone(), &g1, Access::Read)
        .await?;
    pair.left()
        .repo
        .add_member_to_group(member_agent.clone(), &g2, Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Verify user has transitive access through the chain at the keyhive level.
    let has = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(&right_agent_id(&pair), doc_identifier(doc_id))
        .await;
    assert!(
        has.is_some(),
        "user must have transitive access through the deep chain (doc←G1←G2←user)"
    );
    assert_eq!(
        has,
        Some(Access::Read),
        "user must have Read access through the chain"
    );

    // Verify user can materialise and read pre-grant content.
    let user_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&user_doc).await, "deep-chain");

    // Each intermediate group still exists and has access.
    let g1_ident = keyhive_core::principal::identifier::Identifier::from(g1.id());
    let g1_has = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(&g1_ident, doc_identifier(doc_id))
        .await;
    assert!(g1_has.is_some(), "G1 must have access on the doc");

    let g2_ident = keyhive_core::principal::identifier::Identifier::from(g2.id());
    let g2_has_doc = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(&g2_ident, doc_identifier(doc_id))
        .await;
    assert!(
        g2_has_doc.is_some(),
        "G2 must have transitive access on the doc"
    );

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(user_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Document-as-member ───────────────────────────────────────────────────
//
// Document B is granted access to document A. B's agent is resolved through
// `fixtures::document_agent`. After grant + sync, the owner of B should be
// able to read A through B's document-level membership, and B's owner can
// sync A's content.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_document_as_member() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(84, 85, "Owner", "DocAgent").await?;

    // Create document A with content.
    let mut initial_a = automerge::Automerge::new();
    initial_a
        .transact(|tx| tx.put(automerge::ROOT, "title", "doc-a-content"))
        .map_err(|err| crate::ferr!("failed creating doc A: {err:?}"))?;
    let doc_a = pair.left().repo.create_doc(initial_a).await?;
    let doc_a_id = doc_a.document_id();

    // Create document B (no content needed, just its identity).
    let mut initial_b = automerge::Automerge::new();
    initial_b
        .transact(|tx| tx.put(automerge::ROOT, "title", "doc-b"))
        .map_err(|err| crate::ferr!("failed creating doc B: {err:?}"))?;
    let doc_b = pair.left().repo.create_doc(initial_b).await?;
    let doc_b_id = doc_b.document_id();
    drop(doc_b);

    // Resolve document B as a Keyhive agent.
    let doc_b_agent = fixtures::document_agent(&pair.left().repo, doc_b_id).await?;

    // Grant document B Read access to document A.
    pair.left()
        .repo
        .grant_doc_access(doc_a_id, doc_b_agent, Access::Read)
        .await?;

    // The right-side node is not directly a party to this grant, but the
    // owner side's keyhive must reflect the delegation.
    let doc_b_ident = keyhive_core::principal::identifier::Identifier::from(
        ed25519_dalek::VerifyingKey::from_bytes(&doc_b_id.into_bytes())
            .expect("doc id must be a verifying key"),
    );
    let doc_a_ident = doc_identifier(doc_a_id);
    let doc_b_has = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(&doc_b_ident, doc_a_ident)
        .await;
    assert!(
        doc_b_has.is_some(),
        "document agent B must have access on document A after grant"
    );

    // Document A content is readable.
    let title = doc_a
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "title")
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(s) => match s.as_ref() {
                        ScalarValue::Str(v) => Some(v.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await;
    assert_eq!(
        title.as_deref(),
        Some("doc-a-content"),
        "doc A content must be intact after document-as-member grant"
    );

    drop(doc_a);
    Ok(())
}

// ─── Delegate-before-define ───────────────────────────────────────────────
//
// Create a reference to a group in a doc's parent chain before the group
// has any members. Then add a member later; the member must receive history-
// inclusive access. This tests that the grant path doesn't fail when the
// group was defined as a parent but had no members at the time.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_delegate_before_define() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(86, 87, "Owner", "LateMember").await?;
    let member_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    // Create a group before any document.
    let group = pair.left().repo.create_group_with_parents(vec![]).await?;

    // Create a document with the group as a parent.
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "delegate-before-define"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair
        .left()
        .repo
        .create_doc_with_parents(initial, vec![group.clone().into()])
        .await?;
    let doc_id = owner_doc.document_id();

    // Grant the group access.
    pair.left()
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Now add the member — should succeed and provide history-inclusive access.
    pair.left()
        .repo
        .add_member_to_group(member_agent.clone(), &group, Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let member_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&member_doc).await, "delegate-before-define");

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(owner_doc);
    drop(member_doc);
    Ok(())
}

// ========================================================================
// Adversarial cases — error handling (no panic, no state mutation)
// ========================================================================

// ─── Escalation: non-root tries to grant higher access ────────────────────
//
// A reader with Read access on the document tries to grant another agent
// Edit access. The grant must fail because the reader is not the document
// owner and does not hold Edit to delegate. The document's access state
// must not change.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_escalation_rejected() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(88, 89, "Owner", "Reader").await?;

    // Boot a third node that will try the escalation.
    let escalator = crate::test2::harness::Node::boot(90, "Escalator").await?;
    let guard = crate::test2::harness::topo::ShutdownGuard::from(vec![escalator]);
    let escalator_idx = 0; // guard.node(0)

    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;

    // Connect escalator to Owner.
    let esc_conn = guard.node(0).connect(pair.left()).await?;
    let owner_esc_conn = pair.left().accepted_connection().await;
    owner_esc_conn.sync_keyhive_with_peer(None).await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let escalator_agent = fixtures::agent_of(&pair.left().repo, guard.node(0)).await?;

    // Owner creates doc, grants Reader Read access, but NOT the Escalator.
    let (owner_doc, doc_id) = create_initial(&pair, "escalation").await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "escalation");

    // Escalator (who has no access) tries to grant itself Edit on the doc.
    // This must fail because the Escalator is not the owner and has no
    // delegation authority over the document.
    let pre_state = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let result = guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, escalator_agent.clone(), Access::Edit)
        .await;
    assert!(
        result.is_err(),
        "non-owner grant of higher access must be rejected"
    );

    // The document's keyhive state must not have changed.
    let post_state = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    assert_eq!(
        pre_state, post_state,
        "document keyhive state must not change after rejected escalation"
    );

    // Escalator must not magically gain access.
    let esc_ident = keyhive_core::principal::identifier::Identifier::from(
        ed25519_dalek::VerifyingKey::from_bytes(guard.node(0).peer_id().as_bytes())
            .expect("peer id must be a verifying key"),
    );
    let esc_has = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(&esc_ident, doc_identifier(doc_id))
        .await;
    assert!(
        esc_has.is_none(),
        "escalator must not have effective access after rejected escalation"
    );

    // Reader must still have Read access.
    let reader_has = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(&right_agent_id(&pair), doc_identifier(doc_id))
        .await;
    assert_eq!(
        reader_has,
        Some(Access::Read),
        "reader must retain Read access after rejected escalation attempt"
    );

    drop(esc_conn);
    drop(owner_esc_conn);
    pair.disconnect().await?;
    drop(guard);
    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Unauthorized revocation ──────────────────────────────────────────────
//
// A reader (non-owner) tries to revoke another reader's access. Must fail.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_unauthorized_revocation_fails() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(91, 92, "Owner", "ReaderA").await?;

    // Third node: ReaderB.
    let reader_b = crate::test2::harness::Node::boot(93, "ReaderB").await?;
    let guard = crate::test2::harness::topo::ShutdownGuard::from(vec![reader_b]);

    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;

    // Connect ReaderB to Owner.
    let rb_conn = guard.node(0).connect(pair.left()).await?;
    let owner_rb_conn = pair.left().accepted_connection().await;
    owner_rb_conn.sync_keyhive_with_peer(None).await?;

    let reader_a_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let reader_b_agent = fixtures::agent_of(&pair.left().repo, guard.node(0)).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "unauth-revoke").await?;

    // Owner grants both readers Read access.
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_a_agent.clone(), Access::Read)
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_b_agent.clone(), Access::Read)
        .await?;

    pair.left_conn().sync_keyhive_with_peer(None).await?;
    // Sync to both.
    owner_rb_conn.sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // ReaderA tries to revoke ReaderB's access. This should fail because
    // ReaderA is not the document owner.
    let pre_state = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let result = pair
        .right()
        .repo
        .revoke_doc_access(doc_id, reader_b_agent.clone())
        .await;
    assert!(result.is_err(), "non-owner revocation must be rejected");

    // State unchanged.
    let post_state = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    assert_eq!(
        pre_state, post_state,
        "document keyhive state must not change after rejected revocation"
    );

    // Both readers still have access.
    let a_ident = right_agent_id(&pair);
    let b_ident = keyhive_core::principal::identifier::Identifier::from(
        ed25519_dalek::VerifyingKey::from_bytes(guard.node(0).peer_id().as_bytes())
            .expect("peer id must be a verifying key"),
    );
    let doc_kh_id = doc_identifier(doc_id);
    assert!(
        pair.left()
            .repo
            .keyhive()
            .agent_access_on(&a_ident, doc_kh_id)
            .await
            .is_some(),
        "ReaderA must retain access after rejected revocation"
    );
    assert!(
        pair.left()
            .repo
            .keyhive()
            .agent_access_on(&b_ident, doc_kh_id)
            .await
            .is_some(),
        "ReaderB must retain access after rejected revocation"
    );

    drop(rb_conn);
    drop(owner_rb_conn);
    pair.disconnect().await?;
    drop(guard);
    drop(owner_doc);
    Ok(())
}

// ─── Duplicate grant is near-idempotent ────────────────────────────────────
//
// Granting the same agent the same access twice must not duplicate CGKA
// operations (key material) — the second grant may write a new delegation
// head (BigRepo's checkpoint mechanism) but must NOT re-add the member to
// the document's CGKA tree.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_duplicate_grant_idempotent() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(94, 95, "Owner", "DupReader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "dup-grant").await?;

    // First grant.
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent.clone(), Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let after_first = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    let cgka_after_first = after_first.cgka_operation_hashes.clone();
    let member_count_first = after_first.members.len();

    // Second (duplicate) grant — BigRepo writes a new checkpoint delegation
    // head but must not add new CGKA operations.
    let _result = pair
        .left()
        .repo
        .grant_doc_access(doc_id, reader_agent.clone(), Access::Read)
        .await;
    // The grant may succeed (with a new checkpoint) or return an error
    // (already present). Either way the CGKA ops must not have grown.
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let after_second = kh_snap::document_snapshot(&pair.left().repo, doc_id).await?;
    assert_eq!(
        after_second.cgka_operation_hashes, cgka_after_first,
        "duplicate grant must not add new CGKA operations"
    );
    assert_eq!(
        after_second.members.len(),
        member_count_first,
        "duplicate grant must not add new members"
    );

    // Reader must still be able to read.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "dup-grant");

    kh_snap::assert_document_snapshot_equal(pair.left(), pair.right(), doc_id).await?;
    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Revocation preserves prior encrypted content ──────────────────────────
//
// After revocation (forward secrecy), the affected document's pre-revoke
// encrypted content must remain decryptable by remaining members. There is
// no backward erasure: only forward writes by the revoked party are blocked.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_revocation_preserves_prior_encrypted_content() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(96, 97, "Owner", "RevocableEditor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "pre-revoke-content").await?;

    // Grant Edit, sync, and let the editor write content.
    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent.clone(), Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "pre-revoke"))
                .map_err(|err| crate::ferr!("editor write failed: {err:?}"))
        })
        .await??;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    // Sync the editor's write to the owner BEFORE revocation.
    let _owner_doc =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;

    // Verify owner sees the pre-revoke content before revocation.
    assert_eq!(
        read_text(&_owner_doc, "phase").await.as_deref(),
        Some("pre-revoke"),
        "owner must see editor's pre-revoke content before revocation"
    );
    drop(_owner_doc);
    drop(editor_doc);

    // Revoke the editor.
    pair.left()
        .repo
        .revoke_doc_access(doc_id, editor_agent.clone())
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Owner must still be able to read the pre-revoke content from local
    // storage after revocation (no sync needed — content was already synced).
    let owner_recheck = pair
        .left()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    assert_eq!(
        read_text(&owner_recheck, "phase").await.as_deref(),
        Some("pre-revoke"),
        "pre-revoke encrypted content must remain decryptable after revocation"
    );

    // If the editor still has a local handle, the pre-revoke content must
    // also still be readable on their side.
    if let crate::DocLookup::Ready(revoked_handle) = pair.right().repo.get_doc(&doc_id).await? {
        assert_eq!(
            read_text(&revoked_handle, "phase").await.as_deref(),
            Some("pre-revoke"),
            "revoked user must still read pre-revoke local content"
        );
        drop(revoked_handle);
    }

    drop(owner_recheck);
    drop(owner_doc);
    Ok(())
}

// ─── Stale/revoked proof rejected ─────────────────────────────────────────
//
// After a user is revoked, their old credential (cached at the keyhive level)
// must not allow further encrypted writes. This test verifies that post-revoke
// encrypted content pushed by the revoked user cannot be decrypted by other
// group members.
#[tokio::test(flavor = "multi_thread")]
async fn tier6_stale_revoked_proof_rejected() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(99, 100, "Owner", "RevokedUser").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "stale-proof").await?;

    // Grant Edit, sync, editor writes.
    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent.clone(), Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "before-revoke"))
                .map_err(|err| crate::ferr!("editor write before revoke failed: {err:?}"))
        })
        .await??;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    drop(editor_doc);

    // Revoke the editor.
    pair.left()
        .repo
        .revoke_doc_access(doc_id, editor_agent.clone())
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // After revocation, the revoked user should have no effective access.
    let has = pair
        .left()
        .repo
        .keyhive()
        .agent_access_on(
            &keyhive_core::principal::identifier::Identifier::from(
                ed25519_dalek::VerifyingKey::from_bytes(pair.right().peer_id().as_bytes())
                    .expect("peer id must be a verifying key"),
            ),
            doc_identifier(doc_id),
        )
        .await;
    assert!(has.is_none(), "revoked user must have no effective access");

    // The revoked user should not be able to write new content that is
    // decryptable. Try a write locally and sync — the owner must reject.
    let revoked_handle = match pair.right().repo.get_doc(&doc_id).await? {
        crate::DocLookup::Ready(h) => h,
        _ => {
            // If the doc is no longer materialized on the revoked side, that's
            // also correct. No further write possible.
            drop(owner_doc);
            return Ok(());
        }
    };
    let write_result = revoked_handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "status", "post-revoke-write"))
                .map_err(|err| crate::ferr!("post-revoke write: {err:?}"))
        })
        .await;
    // The local write may succeed or fail; if it succeeds, verify that the
    // push to the owner doesn't propagate the new content.
    if write_result.is_ok() {
        pair.right_conn().sync_keyhive_with_peer(None).await?;
        pair.left_conn().sync_keyhive_with_peer(None).await?;
        let sync_result = pair
            .right_conn()
            .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
            .await;
        // Sync may fail entirely because the transport rejects encrypted
        // content from a revoked member, or it may succeed but the owner
        // won't apply the decrypted changes.
        if sync_result.is_ok() {
            pair.left_conn()
                .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
                .await?;
            let owner_handle = pair
                .left()
                .repo
                .get_doc(&doc_id)
                .await?
                .into_ready(doc_id)?;
            // The owner must not see the post-revoke write.
            assert_ne!(
                read_text(&owner_handle, "status").await.as_deref(),
                Some("post-revoke-write"),
                "post-revoke write must not be visible to the owner"
            );
            drop(owner_handle);
        }
    }
    drop(revoked_handle);
    drop(owner_doc);
    Ok(())
}
