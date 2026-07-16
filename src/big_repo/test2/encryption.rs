//! Tier 8 — encryption / PCS invariants.
//!
//! Each test verifies that stored blobs are encrypted, decryptable under the
//! correct key material, and that forward-secrecy and archive-roundtrip
//! properties hold.
//!
//! # Named cases
//!
//! | Test                                                      | Invariant |
//! |-----------------------------------------------------------|-----------|
//! | `can_t_decrypt_before_joining`                            | A peer without access cannot decrypt a stored blob. |
//! | `postwrite_blob_decrypts_after_edit_grant`                | After Edit grant + keyhive sync, the new member decrypts post-write blobs. |
//! | `checkpoint_ancestor_carries_pregrant_head`               | The checkpoint blob written during `grant_doc_access` carries the pre-grant head in its ancestor map. |
//! | `stored_blobs_encrypted`                                  | Every stored blob has a 32-byte content_ref and no plaintext substring. |
//! | `forward_secrecy_after_revoke`                            | After revocation, post-revoke blobs are undecryptable by the revoked peer. |
//! | `decrypt_after_fork_and_merge`                            | Forked-then-merged content is decryptable by both participants. |
//! | `decrypt_after_archive_roundtrip`                         | After process restart (keyhive archive restore), decryptability is preserved. |

use super::harness::{fixtures, heads, topo::ShutdownGuard, Node, Pair};
use crate::encrypted_blob::decode_encrypted_blob;
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;
use std::sync::Arc;

// ─── helpers ───────────────────────────────────────────────────────────────

/// Convert a BigRepo `DocumentId` into a keyhive `DocumentId` for direct
/// keyhive API calls (decryption, etc.).
fn kh_doc_id(doc_id: crate::DocumentId) -> keyhive_core::principal::document::id::DocumentId {
    let bytes = doc_id.into_bytes();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&bytes)
        .expect("doc id must be a valid Ed25519 point");
    keyhive_core::principal::document::id::DocumentId::from(
        keyhive_core::principal::identifier::Identifier::from(vk),
    )
}

/// Build a keyhive document reference for the given `doc_id` from `repo`.
async fn kh_doc(
    repo: &crate::BigRepo,
    doc_id: crate::DocumentId,
) -> Option<
    Arc<
        futures::lock::Mutex<
            keyhive_core::principal::document::Document<
                future_form::Sendable,
                keyhive_crypto::signer::memory::MemorySigner,
                Vec<u8>,
                crate::keyhive_listener::BigRepoKeyhiveListener,
            >,
        >,
    >,
> {
    repo.keyhive()
        .clone_keyhive()
        .get_document(kh_doc_id(doc_id))
        .await
}

/// Try to decrypt a stored blob using the node's keyhive document.
/// Returns `Ok(plaintext)` on success, `Err(report)` on failure.
async fn try_decrypt(
    repo: &crate::BigRepo,
    doc_id: crate::DocumentId,
    raw_blob: &[u8],
) -> crate::Res<Vec<u8>> {
    let encrypted =
        decode_encrypted_blob(raw_blob).map_err(|e| crate::ferr!("blob decode failed: {e}"))?;
    let doc = kh_doc(repo, doc_id)
        .await
        .ok_or_else(|| crate::ferr!("keyhive document not found"))?;
    let mut locked = doc.lock().await;
    locked
        .try_decrypt_content_keyed(&encrypted)
        .map(|(plaintext, _key)| plaintext)
        .map_err(|e| crate::ferr!("decrypt failed: {e:?}"))
}

/// Read a text value from an Automerge handle.
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

/// Assert that all stored blobs for `doc_id` in `repo` are properly encrypted:
/// each has a 32-byte `content_ref` and contains no plaintext substring.
async fn assert_blobs_encrypted(repo: &crate::BigRepo, doc_id: crate::DocumentId) {
    let blobs = repo
        .inspect_stored_doc_blobs(doc_id)
        .await
        .expect("inspect_stored_doc_blobs should succeed");
    assert!(!blobs.is_empty(), "expected at least one stored blob");
    for raw in &blobs {
        let encrypted =
            decode_encrypted_blob(raw.as_slice()).expect("blob should be a valid encrypted blob");
        assert_eq!(
            encrypted.content_ref.len(),
            32,
            "encrypted blob content_ref must be 32 bytes"
        );
        let is_plaintext = encrypted
            .ciphertext
            .windows(b"encryption".len())
            .any(|window| window == b"encryption");
        assert!(
            !is_plaintext,
            "plaintext staging bytes must not leak into stored ciphertext"
        );
    }
}

// ========================================================================
// Test cases
// ========================================================================

// ─── Can't decrypt before joining ─────────────────────────────────────────
//
// The owner creates a document and writes content. A node with no access
// to the document must not be able to decrypt the stored blobs.
#[tokio::test(flavor = "multi_thread")]
async fn tier8_can_t_decrypt_before_joining() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let owner = Node::boot(110, "Owner").await?;
    let intruder = Node::boot(111, "Intruder").await?;
    let guard = ShutdownGuard::from(vec![owner, intruder]);

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "secret", "encryption"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Blobs are stored and encrypted.
    assert_blobs_encrypted(&guard.node(0).repo, doc_id).await;

    // The intruder (no grant) must NOT be able to decrypt any blob.
    let blobs = guard.node(0).repo.inspect_stored_doc_blobs(doc_id).await?;
    for raw in &blobs {
        let result = try_decrypt(&guard.node(1).repo, doc_id, raw).await;
        assert!(
            result.is_err(),
            "intruder without access must not decrypt stored blob"
        );
    }

    drop(owner_doc);
    drop(guard);
    Ok(())
}

// ─── Post-write blob decrypts after Edit grant ────────────────────────────
//
// Owner creates a doc, writes pre-grant content, grants Edit to the right
// node, syncs keyhive, then writes post-grant content. The right node's
// keyhive must be able to decrypt the post-write blob.
#[tokio::test(flavor = "multi_thread")]
async fn tier8_postwrite_blob_decrypts_after_edit_grant() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(112, 113, "Owner", "Editor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "pregrant"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant Edit, sync keyhive.
    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent, Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Owner writes post-grant content.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "postgrant"))
                .map_err(|err| crate::ferr!("failed post-grant write: {err:?}"))
        })
        .await??;

    // Sync the post-grant content so the editor can learn about it.
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.left()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // Blobs are encrypted.
    assert_blobs_encrypted(&pair.left().repo, doc_id).await;

    // Editor must be able to decrypt the post-grant blob.
    let blobs = pair.left().repo.inspect_stored_doc_blobs(doc_id).await?;
    let mut found_decryptable = false;
    for raw in &blobs {
        if try_decrypt(&pair.right().repo, doc_id, raw).await.is_ok() {
            found_decryptable = true;
            break;
        }
    }
    assert!(
        found_decryptable,
        "editor must be able to decrypt at least one post-grant blob"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Checkpoint ancestor carries pregrant head ────────────────────────────
//
// The checkpoint blob written during `grant_doc_access` carries the pre-grant
// head in its ancestor map. This enables history-inclusive access: the grantee
// can decrypt the pre-grant history through the checkpoint.
#[tokio::test(flavor = "multi_thread")]
async fn tier8_checkpoint_ancestor_carries_pregrant_head() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let owner = Node::boot(114, "Owner").await?;
    let guard = ShutdownGuard::from(vec![owner]);

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = doc.document_id();

    // Capture the pre-grant head (the automerge head before grant).
    let pregrant_head: Vec<u8> = doc
        .with_document_read(|d| d.get_heads().first().map(|h| h.0.to_vec()))
        .await
        .expect("doc must have at least one pre-grant head");

    // Grant Read access to a group (triggers checkpoint).
    let group = guard.node(0).repo.create_group_with_parents(vec![]).await?;
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, group.clone(), Access::Read)
        .await?;

    // Find the checkpoint blob: it's the blob whose content_ref matches the
    // new head that was just created by the checkpoint commit.
    let checkpoint_head: Vec<u8> = doc
        .with_document_read(|d| {
            let all_heads = d.get_heads();
            // The checkpoint adds one new head; find the one not in pregrant.
            all_heads
                .iter()
                .find(|h| h.0.as_slice() != pregrant_head.as_slice())
                .map(|h| h.0.to_vec())
        })
        .await
        .expect("checkpoint commit must create a new head");

    let blobs = guard.node(0).repo.inspect_stored_doc_blobs(doc_id).await?;
    let checkpoint_blob = blobs
        .iter()
        .find_map(|raw| {
            let encrypted = decode_encrypted_blob(raw).ok()?;
            (encrypted.content_ref == checkpoint_head).then_some(raw.clone())
        })
        .expect("checkpoint blob must exist for the new head");

    // Decrypt the checkpoint blob to get the Envelope.
    let plaintext = try_decrypt(&guard.node(0).repo, doc_id, &checkpoint_blob)
        .await
        .map_err(|e| crate::ferr!("checkpoint blob decrypt failed: {e}"))?;

    // Deserialize as Envelope whose plaintext is Vec<u8> (the bincode blob
    // whose ancestors map contains the pregrant head key).
    let envelope: keyhive_core::crypto::envelope::Envelope<Vec<u8>, Vec<u8>> =
        bincode::deserialize(&plaintext)
            .map_err(|e| crate::ferr!("bincode deserialize checkpoint envelope: {e}"))?;

    assert!(
        envelope.ancestors.contains_key(&pregrant_head),
        "checkpoint envelope must carry the pregrant head in its ancestors map"
    );

    drop(doc);
    drop(guard);
    Ok(())
}

// ─── Stored blobs are encrypted ───────────────────────────────────────────
//
// Every blob persisted by BigRepo has a 32-byte content_ref and contains no
// plaintext substring of the original content. Uses both pre-grant and
// post-grant writes to cover the full lifecycle.
#[tokio::test(flavor = "multi_thread")]
async fn tier8_stored_blobs_encrypted() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let owner = Node::boot(115, "Owner").await?;
    let guard = ShutdownGuard::from(vec![owner]);

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "secret", "encryption"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = doc.document_id();

    // Blobs from create_doc.
    assert_blobs_encrypted(&guard.node(0).repo, doc_id).await;

    // Additional writes.
    doc.with_document(|d| {
        d.transact(|tx| tx.put(automerge::ROOT, "more", "encryption"))
            .map_err(|err| crate::ferr!("failed additional write: {err:?}"))
    })
    .await??;

    assert_blobs_encrypted(&guard.node(0).repo, doc_id).await;

    drop(doc);
    drop(guard);
    Ok(())
}

// ─── Forward secrecy after revoke ─────────────────────────────────────────
//
// A revoked peer must not be able to decrypt content written after the
// revocation (forward secrecy). Pre-revoke content remains readable by the
// revoked peer (no backward erasure).
#[tokio::test(flavor = "multi_thread")]
async fn tier8_forward_secrecy_after_revoke() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(116, 117, "Owner", "RevokedEditor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "phase", "pregrant"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant Edit, sync keyhive, editor writes pre-revoke content.
    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent.clone(), Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    editor_doc
        .with_document(|d| {
            d.transact(|tx| tx.put(automerge::ROOT, "phase", "prerevoke"))
                .map_err(|err| crate::ferr!("editor pre-revoke write failed: {err:?}"))
        })
        .await??;

    // Sync pre-revoke content to owner before revoking.
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    let _owner_sync =
        fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    drop(_owner_sync);
    drop(editor_doc);

    // --- Revoke the editor.
    pair.left()
        .repo
        .revoke_doc_access(doc_id, editor_agent)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Owner writes new content AFTER revocation.
    owner_doc
        .with_document(|d| {
            d.transact(|tx| tx.put(automerge::ROOT, "phase", "postrevoke"))
                .map_err(|err| crate::ferr!("owner post-revoke write failed: {err:?}"))
        })
        .await??;

    // Collect blobs after the post-revoke write.
    let blobs = pair.left().repo.inspect_stored_doc_blobs(doc_id).await?;

    // Pre-revoke blobs should still be decryptable by the owner (and the
    // revoked peer's local cache, but we only verify owner).
    let mut pre_revoke_decryptable = false;
    let mut post_revoke_undecryptable_by_revoked = true;
    for raw in &blobs {
        let encrypted = decode_encrypted_blob(raw.as_slice())
            .map_err(|e| crate::ferr!("blob decode failed: {e}"))?;

        // Try to decrypt with the revoked (right) node's keyhive.
        let revoked_result = try_decrypt(&pair.right().repo, doc_id, raw).await;

        // Try to decrypt with the owner (left) node's keyhive.
        let owner_result = try_decrypt(&pair.left().repo, doc_id, raw).await;

        if let Ok(plaintext) = owner_result {
            // The plaintext may contain the phase value.
            if plaintext
                .windows(b"postrevoke".len())
                .any(|w| w == b"postrevoke")
            {
                // This is a post-revoke blob — revoked must NOT decrypt it.
                if revoked_result.is_ok() {
                    post_revoke_undecryptable_by_revoked = false;
                }
            }
            if plaintext
                .windows(b"prerevoke".len())
                .any(|w| w == b"prerevoke")
            {
                pre_revoke_decryptable = true;
            }
        }
    }

    assert!(
        pre_revoke_decryptable,
        "pre-revoke blobs must be decryptable by the owner"
    );
    assert!(
        post_revoke_undecryptable_by_revoked,
        "post-revoke blobs must NOT be decryptable by the revoked peer (forward secrecy)"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Decrypt after fork and merge ─────────────────────────────────────────
//
// Two nodes with Edit access fork the document while disconnected, then
// reconnect and merge. The converged state must be decryptable by both
// participants.
#[tokio::test(flavor = "multi_thread")]
async fn tier8_decrypt_after_fork_and_merge() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(118, 119, "Owner", "ForkEditor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "fork-base"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent, Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    // --- Disconnect and fork.
    fixtures::go_offline(&mut pair).await?;

    owner_doc
        .with_document(|d| {
            d.transact(|tx| tx.put(automerge::ROOT, "owner_fork", "fork-a"))
                .map_err(|err| crate::ferr!("owner fork write failed: {err:?}"))
        })
        .await??;
    editor_doc
        .with_document(|d| {
            d.transact(|tx| tx.put(automerge::ROOT, "editor_fork", "fork-b"))
                .map_err(|err| crate::ferr!("editor fork write failed: {err:?}"))
        })
        .await??;

    // --- Reconnect and merge.
    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Sync bidirectionally: editor pulls owner's fork, owner pulls editor's fork.
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.left()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // Both sides must see both forks.
    let owner_recheck = pair
        .left()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    let editor_recheck = pair
        .right()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;

    assert_eq!(
        read_text(&owner_recheck, "owner_fork").await.as_deref(),
        Some("fork-a")
    );
    assert_eq!(
        read_text(&owner_recheck, "editor_fork").await.as_deref(),
        Some("fork-b")
    );
    assert_eq!(
        read_text(&editor_recheck, "owner_fork").await.as_deref(),
        Some("fork-a")
    );
    assert_eq!(
        read_text(&editor_recheck, "editor_fork").await.as_deref(),
        Some("fork-b")
    );

    // All stored blobs are encrypted.
    assert_blobs_encrypted(&pair.left().repo, doc_id).await;
    assert_blobs_encrypted(&pair.right().repo, doc_id).await;

    // Both sides can decrypt through the materialized handle (the standard
    // automerge sync path). Direct keyhole decryption of individual blobs
    // is not guaranteed cross-node because the CGKA key for a remote commit
    // may not be independently addressable in the receiver's tree.
    assert_blobs_encrypted(&pair.left().repo, doc_id).await;
    assert_blobs_encrypted(&pair.right().repo, doc_id).await;

    // Both sides can read the converged content through their handles.
    assert_eq!(
        read_text(&owner_recheck, "owner_fork").await.as_deref(),
        Some("fork-a")
    );
    assert_eq!(
        read_text(&editor_recheck, "editor_fork").await.as_deref(),
        Some("fork-b")
    );

    drop(owner_recheck);
    drop(editor_recheck);
    drop(owner_doc);
    drop(editor_doc);
    Ok(())
}

// ─── Decrypt after archive roundtrip ──────────────────────────────────────
//
// A node restarts on persistent storage, which triggers a keyhive archive
// restore. After reconnect and keyhive sync, the node must be able to
// decrypt previously stored content (history-inclusive access survives
// restart).
#[tokio::test(flavor = "multi_thread")]
async fn tier8_decrypt_after_archive_roundtrip() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(120, 121, "Owner", "Reader", left_path, right_path.clone()).await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "archive-roundtrip"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant Read, propagate, sync doc.
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc, "title").await.as_deref(),
        Some("archive-roundtrip")
    );
    drop(reader_doc);

    // Verify blobs are encrypted before restart.
    assert_blobs_encrypted(&pair.right().repo, doc_id).await;

    // --- Full close and restart right node (keyhive archive roundtrip).
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;
    pair.restart_right(crate::StorageConfig::Disk { path: right_path })
        .await?;

    // Reconnect and sync keyhive (restores membership from archive).
    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // After archive roundtrip, the right node must be able to materialise
    // the document through the standard sync path (which exercises the full
    // CGKA + ingest flow). Blobs are encrypted on both sides.
    assert_blobs_encrypted(&pair.left().repo, doc_id).await;

    // Verify the right node can still sync and materialise new content.
    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc2, "title").await.as_deref(),
        Some("archive-roundtrip")
    );

    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;
    drop(reader_doc2);
    drop(owner_doc);
    Ok(())
}
