//! Tier 5 — restart/reconnect matrix.
//!
//! `{remote-restart-live-conn, offline-updates-catch-up, reopen-no-sync,
//!   reopen+sync} × {membership-first, payload-first}`
//!
//! Each test uses `boot_persistent` so the right-side store survives an
//! in-process restart. `restart_right` shuts the node down and re-boots it
//! on the same backing store (seed + db path). After restart the old
//! `BigRepoConnection` handles are stale; every test re-establishes transport
//! connections via `pair.connect()` and then orders membership/doc sync
//! per the matrix cell.
//!
//! # Ordering semantics
//!
//! - **membership-first**: `sync_keyhive_with_peer` then `sync_doc_with_peer`.
//! - **payload-first**: `sync_doc_with_peer` first, then
//!   `sync_keyhive_with_peer`, then a second `sync_doc_with_peer` (because
//!   without membership the first doc sync cannot decrypt the content key;
//!   the second pull materialises after the keyhive state arrives).
//!
//! All cases assert the Tier-0 invariants (sedimentree parity, materialised
//! parity) at the end. Seeds are unique per test; RAII `Pair` teardown
//! handles all cleanup — no manual `.stop()` calls.

use super::harness::{fixtures, heads, Pair};
use crate::StorageConfig;
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
        .map_err(|err| crate::ferr!("failed creating restart doc: {err:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let id = doc.document_id();
    Ok((doc, id))
}

/// Grant read access to the right node and have it sync the document.
async fn grant_and_sync(pair: &Pair, doc_id: crate::DocumentId) -> crate::Res<crate::BigDocHandle> {
    let agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(pair, doc_id, &agent, Access::Read).await?;
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await
}

/// Restart the right-side node **without** reconnecting or syncing.
/// Caller is responsible for `pair.connect()` + sync afterwards.
async fn restart_right(pair: &mut Pair, right_path: std::path::PathBuf) -> crate::Res<()> {
    // Drop old connection handles before restart so stale references are gone.
    pair.left_conn.take();
    pair.right_conn.take();
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await
}

/// After reconnect: sync membership (keyhive) first, then document payload.
///
/// Verifies the document is readable on the right side after one doc sync.
async fn reconcile_membership_first(
    pair: &Pair,
    doc_id: crate::DocumentId,
) -> crate::Res<crate::BigDocHandle> {
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await
}

/// After reconnect: sync document payload first (before membership).
///
/// The first doc sync may not decrypt (membership is stale); we then sync
/// keyhive and issue a second doc sync. Returns the handle from the second
/// sync, when the reader should be able to materialise.
async fn reconcile_payload_first(
    pair: &Pair,
    doc_id: crate::DocumentId,
) -> crate::Res<crate::BigDocHandle> {
    // First doc sync — may produce PendingMaterialization because the reader
    // hasn't synced the updated CGKA key material yet.
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;
    // Now sync membership so the reader learns the new CGKA epoch.
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    // Second doc sync — should decrypt with the fresh key material.
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await
}

// ─── remote-restart-live-conn ──────────────────────────────────────────────
//
// The remote (right) node is shut down and restarted while the left node
// is still live. Old transport connections are stale; new ones are
// established before syncing.

#[tokio::test(flavor = "multi_thread")]
async fn tier5_remote_restart_membership_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(60, 61, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "restart-membership-first").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "restart-membership-first");
    drop(reader_doc);

    // Remote restart — right node comes back on the same store.
    restart_right(&mut pair, right_path).await?;
    pair.connect().await?;

    // membership-first: sync keyhive before document payload.
    let reader_doc2 = reconcile_membership_first(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "restart-membership-first");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier5_remote_restart_payload_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(62, 63, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "restart-payload-first").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "restart-payload-first");
    drop(reader_doc);

    restart_right(&mut pair, right_path).await?;
    pair.connect().await?;

    // payload-first: sync doc, then keyhive, then doc again.
    let reader_doc2 = reconcile_payload_first(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "restart-payload-first");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

// ─── offline-updates-catch-up ──────────────────────────────────────────────
//
// After initial replication the pair goes offline. The owner makes edits
// while disconnected. The right node then restarts (simulating a delayed
// process restart during offline period) and reconnects. The offline
// updates must arrive after sync regardless of ordering.

#[tokio::test(flavor = "multi_thread")]
async fn tier5_offline_updates_membership_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(64, 65, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "offline-base").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "offline-base");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(reader_doc);

    // --- Go offline.
    pair.disconnect().await?;
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;

    // Owner edits while right is offline.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "offline-update"))
                .map_err(|err| crate::ferr!("failed offline edit: {err:?}"))
        })
        .await??;
    assert_eq!(read_title(&owner_doc).await, "offline-update");

    // Right node restarts (shut down + come back on the same store) while
    // the offline edits are still pending.
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;
    pair.connect().await?;

    // membership-first.
    let reader_doc2 = reconcile_membership_first(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "offline-update");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier5_offline_updates_payload_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(66, 67, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "offline-base").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "offline-base");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(reader_doc);

    // --- Go offline.
    pair.disconnect().await?;
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "offline-update"))
                .map_err(|err| crate::ferr!("failed offline edit: {err:?}"))
        })
        .await??;

    // Right node restarts.
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;
    pair.connect().await?;

    // payload-first: doc first, then keyhive, then doc again.
    let reader_doc2 = reconcile_payload_first(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "offline-update");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

// ─── reopen-no-sync ────────────────────────────────────────────────────────
//
// Both connections are fully closed and the right node is restarted on its
// persistent store. After reconnect **no sync at all** is performed.
// The test verifies that the durable BigRepo store and Keyhive state survive
// the restart: sedimentree heads are non-empty, and the local keyhive still
// has the membership entry.
//
// The split between membership-first / payload-first here refers to which
// **post-condition** is checked first in the assertions, since no network
// sync happens.

#[tokio::test(flavor = "multi_thread")]
async fn tier5_reopen_no_sync_membership_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(68, 69, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "no-sync-persist").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "no-sync-persist");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(reader_doc);

    // Take the initial seed DocHeadState for comparison after restart.
    let left_state = pair.left().repo.doc_head_state(doc_id).await?;
    let right_pre_state = pair.right().repo.doc_head_state(doc_id).await?;

    // Full close: stop connections and restart the right node.
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;

    // Reconnect transport but do NOT sync — membership-first assertion order.
    pair.connect().await?;

    // --- Assertions: membership (keyhive) state first.

    // Even without a keyhive sync, the local store persisted the keyhive
    // state. Verify the right node's keyhive still knows the document.
    let right_reader_peer = pair.right().peer_id();
    let reader_agent_key = ed25519_dalek::VerifyingKey::from_bytes(right_reader_peer.as_bytes())
        .expect("peer id must be a verifying key");
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("document id must be a verifying key");
    let agent_id = keyhive_core::principal::identifier::Identifier::from(reader_agent_key);
    let doc_ident = keyhive_core::principal::identifier::Identifier::from(doc_key);

    let access = pair
        .right()
        .repo
        .keyhive()
        .agent_access_on(&agent_id, doc_ident)
        .await;
    assert!(
        access.is_some(),
        "{} should still have access on doc after reopen without sync — \
         keyhive state must survive restart",
        pair.right().label,
    );

    // --- Payload assertions: sedimentree heads are preserved.
    let right_post_state = pair.right().repo.doc_head_state(doc_id).await?;
    assert_eq!(
        right_pre_state.sedimentree_heads,
        right_post_state.sedimentree_heads,
        "sedimentree heads must survive restart without sync on {}",
        pair.right().label,
    );

    // Left's state is unchanged (no sync happened).
    let left_post_state = pair.left().repo.doc_head_state(doc_id).await?;
    assert_eq!(
        left_state.sedimentree_heads,
        left_post_state.sedimentree_heads,
        "sedimentree heads must be unchanged on {} after no-op reopen",
        pair.left().label,
    );

    drop(owner_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier5_reopen_no_sync_payload_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(70, 71, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "no-sync-payload-first").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "no-sync-payload-first");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(reader_doc);

    let right_pre_state = pair.right().repo.doc_head_state(doc_id).await?;

    // Full close and restart.
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;

    // Reconnect transport, no sync — payload-first assertion order.
    pair.connect().await?;

    // --- Assertions: payload (sedimentree heads) first.
    let right_post_state = pair.right().repo.doc_head_state(doc_id).await?;
    assert_eq!(
        right_pre_state.sedimentree_heads,
        right_post_state.sedimentree_heads,
        "sedimentree heads must survive restart without sync on {}",
        pair.right().label,
    );

    // --- Membership assertions.
    let right_reader_peer = pair.right().peer_id();
    let reader_agent_key = ed25519_dalek::VerifyingKey::from_bytes(right_reader_peer.as_bytes())
        .expect("peer id must be a verifying key");
    let doc_key = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("document id must be a verifying key");
    let agent_id = keyhive_core::principal::identifier::Identifier::from(reader_agent_key);
    let doc_ident = keyhive_core::principal::identifier::Identifier::from(doc_key);

    let access = pair
        .right()
        .repo
        .keyhive()
        .agent_access_on(&agent_id, doc_ident)
        .await;
    assert!(
        access.is_some(),
        "{} should still have access on doc after reopen without sync",
        pair.right().label,
    );

    drop(owner_doc);
    Ok(())
}

// ─── reopen+sync ───────────────────────────────────────────────────────────
//
// Full close + right restart + reconnect + sync. This is the "happy path"
// restart: the node comes back, finds its durable state, syncs, and
// reaches full convergence.

#[tokio::test(flavor = "multi_thread")]
async fn tier5_reopen_sync_membership_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(72, 73, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "reopen-sync-mf").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "reopen-sync-mf");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(reader_doc);

    // Owner makes an additional edit before reopening — tests that
    // the restart doesn't lose more recent changes.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "reopen-edit"))
                .map_err(|err| crate::ferr!("failed pre-reopen edit: {err:?}"))
        })
        .await??;

    // Full close and restart.
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;
    pair.connect().await?;

    // membership-first: sync keyhive then doc.
    let reader_doc2 = reconcile_membership_first(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "reopen-edit");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier5_reopen_sync_payload_first() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(74, 75, "Owner", "Reader", left_path, right_path.clone()).await?;

    let (owner_doc, doc_id) = create_initial(&pair, "reopen-sync-pf").await?;
    let reader_doc = grant_and_sync(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "reopen-sync-pf");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;
    drop(reader_doc);

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "reopen-payload-first-edit"))
                .map_err(|err| crate::ferr!("failed pre-reopen edit: {err:?}"))
        })
        .await??;

    // Full close and restart.
    let old_left = pair.left_conn.take().expect("left connection should exist");
    let _old_right = pair
        .right_conn
        .take()
        .expect("right connection should exist");
    old_left.stop().await?;
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;
    pair.connect().await?;

    // payload-first: doc sync, then keyhive, then doc again.
    let reader_doc2 = reconcile_payload_first(&pair, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "reopen-payload-first-edit");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(owner_doc);
    drop(reader_doc2);
    Ok(())
}

// ========================================================================
// Polish: both-endpoint restart, restart after local write
// ========================================================================

// ─── Both endpoints restart using persistent stores ───────────────────────
//
// Both left and right nodes restart on their persistent stores. After
// reconnection and sync, the previously replicated document and its
// content must be preserved.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_both_endpoints_restart_preserve_document() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("left");
    let right_path = temp.path().join("right");
    let mut pair = Pair::boot_persistent(
        150,
        151,
        "Left",
        "Right",
        left_path.clone(),
        right_path.clone(),
    )
    .await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "both-restart"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc).await, "both-restart");
    drop(reader_doc);

    // --- Restart both nodes.
    let old_left = pair.left_conn.take().expect("left connection");
    let _old_right = pair.right_conn.take().expect("right connection");
    old_left.stop().await?;

    pair.restart_left(StorageConfig::Disk { path: left_path })
        .await?;
    pair.restart_right(StorageConfig::Disk { path: right_path })
        .await?;

    // Reconnect and sync.
    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let reader_doc2 =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(read_title(&reader_doc2).await, "both-restart");
    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc2).await?;

    drop(reader_doc2);
    drop(owner_doc);
    Ok(())
}

// ─── Restart after local write, before outbound sync ─────────────────────
//
// The left node writes content but is restarted before syncing the content
// to the right. After restart, reconnect and sync — the write must be
// delivered from the local store.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_restart_after_local_write_delivers_on_reconnect() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("left");
    let right_path = temp.path().join("right");
    let mut pair = Pair::boot_persistent(
        152,
        153,
        "Owner",
        "Reader",
        left_path.clone(),
        right_path.clone(),
    )
    .await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "local-before-restart"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant and sync keyhive so the reader learns the CGKA material — but
    // do NOT sync the document content yet.
    fixtures::grant_and_propagate(&pair, doc_id, &reader_agent, Access::Read).await?;

    // Write content that has NOT been synced to the reader.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "note", "written-before-restart"))
                .map_err(|err| crate::ferr!("local write failed: {err:?}"))
        })
        .await??;

    // --- Restart the left node WITHOUT syncing the content first.
    let old_left = pair.left_conn.take().expect("left connection");
    let _old_right = pair.right_conn.take().expect("right connection");
    old_left.stop().await?;

    pair.restart_left(StorageConfig::Disk { path: left_path })
        .await?;

    // Reconnect.
    pair.connect().await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Now sync the doc — the local write should be pushed to the reader.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc, "note").await.as_deref(),
        Some("written-before-restart"),
        "reader must see the write that was made before left's restart"
    );

    heads::tier0_invariants(&pair, doc_id, &owner_doc, &reader_doc).await?;

    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Payload before restored membership does not panic ────────────────────
//
// The persistent part/subduction store survives this restart, but the
// in-memory Keyhive store does not. Loading the persisted payload without
// the local Keyhive document must leave it pending, not terminate the doc
// worker. The normal archive-restore path is covered separately by Tier 5/8.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_payload_without_keyhive_stays_pending() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp = tempfile::tempdir()?;
    let left_path = temp.path().join("owner");
    let right_path = temp.path().join("reader");
    let mut pair =
        Pair::boot_persistent(174, 175, "Owner", "Reader", left_path, right_path).await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "pending-before-membership"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    drop(reader_doc);

    // Restart with Memory keyhive storage while retaining the shared SQLite
    // subduction/part store. The reader now has encrypted payload but no
    // local Keyhive Document entry.
    let old_left = pair.left_conn.take().expect("left connection");
    let _old_right = pair.right_conn.take().expect("right connection");
    old_left.stop().await?;
    pair.restart_right(StorageConfig::Memory).await?;
    pair.connect_without_keyhive_notifications().await?;

    // The scenario intentionally verifies payload handling while the local
    // Keyhive document is absent, so no direct notification consumer is
    // installed after the restart.
    assert!(
        fixtures::document_agent(&pair.right().repo, doc_id)
            .await
            .is_err(),
        "test setup must leave the restarted reader without its Keyhive document"
    );

    // Produce a new encrypted payload after the reader has restarted without
    // its Keyhive document. Depending on whether the encrypted payload already
    // arrived through the background sync path, the explicit sync either
    // reports the missing local document or completes as a no-op.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "payload-before-keyhive"))
                .map_err(|err| crate::ferr!("failed creating post-restart edit: {err:?}"))
        })
        .await??;
    let sync_result = pair
        .right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await;
    if let Err(sync_error) = sync_result {
        assert!(
            matches!(
                sync_error,
                crate::SyncDocError::Policy(crate::SyncDocPolicyError::DocumentNotFound)
            ),
            "missing-keyhive sync may fail with DocumentNotFound, got {sync_error:?}"
        );
    }

    // The persisted tree remains distinguishable from a missing document and
    // loading it without Keyhive capability is still a normal pending state.
    assert!(matches!(
        pair.right().repo.get_doc(&doc_id).await?,
        crate::DocLookup::PendingMaterialization
    ));

    drop(owner_doc);
    Ok(())
}
