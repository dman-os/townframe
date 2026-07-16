//! Tier 9 — edge-case and error-path regressions.
//!
//! # Cases implemented
//!
//! | Test | Invariant |
//! |------|-----------|
//! | `closed_connection_errors_cleanly` | A stopped connection returns typed errors for both keyhive and doc syncs, rather than panicking or hanging. |
//! | `unauthorized_peer_no_plaintext_leak` | A peer without document access receives `SyncDocError::NotFound` on sync and never materialises plaintext. |
//! | `missing_doc_sync_returns_not_found` | Syncing a document id that does not exist on either side returns `SyncDocError::NotFound`. |
//! | `duplicate_concurrent_sync_converges` | Two concurrent `sync_doc_with_peer` calls converge without duplicate state or errors. |
//! | `reconnect_preserves_live_handles` | After connection loss + reconnect, previously acquired live handles remain valid and can read new content. |
//! | `interrupted_sync_retry_succeeds` | A sync that fails due to closed connection can be retried after reconnect. |
//!
//! # Skipped-by-design
//!
//! - **put_doc conflict recovery**: `PutDocError::IdOccpuied` exists in the runtime
//!   layer but there is no public API to create a document with an explicit id.
//! - **over-cap-frame**: No bounded-frame transport path exposed at the BigRepo API level.
//! - **stop-waits-for-save-tasks**: Every test2 test exercises the RAII
//!   [`ShutdownGuard`] / [`Pair`] teardown path.

use super::harness::{fixtures, topo::ShutdownGuard, Node, Pair};
use crate::SyncDocError;
use automerge::{transaction::Transactable, ReadDoc, ScalarValue};
use keyhive_core::access::Access;
use std::time::Duration;

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

/// Agent identifier for a node, for direct keyhive queries.
fn agent_id(node: &Node) -> keyhive_core::principal::identifier::Identifier {
    let peer = node.peer_id();
    let vk = ed25519_dalek::VerifyingKey::from_bytes(peer.as_bytes())
        .expect("peer id must be a verifying key");
    keyhive_core::principal::identifier::Identifier::from(vk)
}

fn doc_identifier(doc_id: crate::DocumentId) -> keyhive_core::principal::identifier::Identifier {
    let vk = ed25519_dalek::VerifyingKey::from_bytes(&doc_id.into_bytes())
        .expect("doc id must be a verifying key");
    keyhive_core::principal::identifier::Identifier::from(vk)
}

// ========================================================================
// Test cases
// ========================================================================

// ─── Closed connection errors cleanly ──────────────────────────────────────
//
// A connection that has been stopped must fail subsequent keyhive and doc
// syncs with a descriptive error rather than panicking or hanging.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_closed_connection_errors_cleanly() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(130, 131, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "closed-conn"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Clone the left connection, stop the clone, then try to use the
    // original — the `closed` flag is shared via `Arc<AtomicBool>`.
    let closed_left = pair.left_conn().clone();
    closed_left.stop().await?;

    // keyhive sync on closed connection must fail.
    let kh_err = pair
        .left_conn()
        .sync_keyhive_with_peer(None)
        .await
        .expect_err("keyhive sync on closed connection must fail");
    let kh_msg = format!("{kh_err}");
    assert!(
        kh_msg.contains("connection is closed"),
        "keyhive sync on closed connection must say 'connection is closed', got: {kh_msg}"
    );

    // doc sync on closed connection must fail.
    let doc_err = pair
        .left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(5)))
        .await
        .expect_err("doc sync on closed connection must fail");
    assert!(
        matches!(&doc_err, SyncDocError::IoError(_)),
        "doc sync on closed connection must return SyncDocError::IoError, got: {doc_err:?}"
    );
    let doc_msg = format!("{doc_err:?}");
    assert!(
        doc_msg.contains("connection is closed"),
        "doc sync on closed connection must contain 'connection is closed', got: {doc_msg}"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Unauthorized peer → no plaintext leak ────────────────────────────────
//
// A peer that has never been granted access to a document must not be able
// to materialise plaintext. The sync returns `SyncDocError::NotFound`, and
// `get_doc` returns `DocLookup::Missing` or `PendingMaterialization`.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_unauthorized_peer_no_plaintext_leak() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();

    // Three nodes: Owner grants access to Reader but not to Intruder.
    let owner = Node::boot(132, "Owner").await?;
    let reader = Node::boot(133, "Reader").await?;
    let intruder = Node::boot(134, "Intruder").await?;
    let guard = ShutdownGuard::from(vec![owner, reader, intruder]);

    // Owner ↔ Reader connection.
    let owner_reader_conn = guard.node(0).connect(guard.node(1)).await?;
    let reader_owner_conn = guard.node(1).accepted_connection().await;
    // Owner ↔ Intruder connection.
    let owner_intruder_conn = guard.node(0).connect(guard.node(2)).await?;
    let intruder_owner_conn = guard.node(2).accepted_connection().await;

    // Keyhive sync: Owner learns both Reader's and Intruder's agents.
    owner_reader_conn.sync_keyhive_with_peer(None).await?;
    owner_intruder_conn.sync_keyhive_with_peer(None).await?;

    let reader_agent = fixtures::agent_of(&guard.node(0).repo, guard.node(1)).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "secret", "confidential"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = guard.node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant only Reader (node 1) access.
    guard
        .node(0)
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;

    // Sync keyhive to both Reader and Intruder.
    owner_reader_conn.sync_keyhive_with_peer(None).await?;
    reader_owner_conn.sync_keyhive_with_peer(None).await?;
    owner_intruder_conn.sync_keyhive_with_peer(None).await?;
    intruder_owner_conn.sync_keyhive_with_peer(None).await?;

    // Reader can sync and materialise.
    let reader_doc = {
        reader_owner_conn
            .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
            .await?;
        guard.node(1).repo.wait_for_quiescence(None).await?;
        match guard.node(1).repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(h) => h,
            other => {
                return Err(crate::ferr!(
                    "authorized reader should get Ready, got {:?}",
                    other
                ));
            }
        }
    };
    assert_eq!(
        read_text(&reader_doc, "secret").await.as_deref(),
        Some("confidential")
    );
    drop(reader_doc);

    // Intruder must NOT materialise plaintext.
    let intruder_sync = intruder_owner_conn
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await;
    match intruder_sync {
        Ok(()) => {
            let lookup = guard.node(2).repo.get_doc(&doc_id).await?;
            assert!(
                !matches!(lookup, crate::DocLookup::Ready(_)),
                "unauthorized peer must not materialise plaintext"
            );
        }
        Err(err) => {
            assert!(
                matches!(err, SyncDocError::NotFound),
                "unauthorized doc sync should fail with NotFound, got {err:?}"
            );
        }
    }

    // Intruder's keyhive must report no access.
    let intruder_access = guard
        .node(0)
        .repo
        .keyhive()
        .agent_access_on(&agent_id(guard.node(2)), doc_identifier(doc_id))
        .await;
    assert!(
        intruder_access.is_none(),
        "intruder must have no effective access"
    );

    drop(owner_doc);
    drop(owner_reader_conn);
    drop(reader_owner_conn);
    drop(owner_intruder_conn);
    drop(intruder_owner_conn);
    drop(guard);
    Ok(())
}

// ─── Missing doc sync returns NotFound ──────────────────────────────────────
//
// Syncing a document id that does not exist on either peer must return
// `SyncDocError::NotFound`, not hang or panic.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_missing_doc_sync_returns_not_found() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(135, 136, "Owner", "Reader").await?;

    // Construct a document id that has never been created.
    let fake_bytes = [137_u8; 32];
    let fake_doc_id = crate::DocumentId::new(fake_bytes);

    // Verify the doc is missing on both sides.
    assert!(
        matches!(
            pair.left().repo.get_doc(&fake_doc_id).await?,
            crate::DocLookup::Missing
        ),
        "non-existent doc must be Missing on the owner"
    );
    assert!(
        matches!(
            pair.right().repo.get_doc(&fake_doc_id).await?,
            crate::DocLookup::Missing
        ),
        "non-existent doc must be Missing on the reader"
    );

    // Attempting to sync a non-existent doc must return NotFound.
    let err = pair
        .left_conn()
        .sync_doc_with_peer(fake_doc_id, Some(Duration::from_secs(10)))
        .await
        .expect_err("syncing a non-existent doc must fail");
    assert!(
        matches!(err, SyncDocError::NotFound),
        "sync of non-existent doc must return NotFound, got {err:?}"
    );

    Ok(())
}

// ========================================================================
// Polish cases
// ========================================================================

// ─── Duplicate/concurrent sync converges ──────────────────────────────────
//
// Two concurrent `sync_doc_with_peer` calls for the same document must both
// return Ok and converge to the same state without duplicate effects.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_duplicate_concurrent_sync_converges() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(138, 139, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "concurrent-base"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Fire two concurrent doc syncs and wait for both.
    let (r1, r2) = tokio::join!(
        pair.right_conn()
            .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10))),
        pair.right_conn()
            .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10))),
    );

    // Both must succeed.
    r1.map_err(|e| crate::ferr!("first concurrent sync failed: {e:?}"))?;
    r2.map_err(|e| crate::ferr!("second concurrent sync failed: {e:?}"))?;

    pair.right()
        .repo
        .wait_for_quiescence(Some(Duration::from_secs(10)))
        .await?;

    // Verify the doc is fully materialized once and readable.
    let reader_doc = pair
        .right()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    assert_eq!(
        read_text(&reader_doc, "title").await.as_deref(),
        Some("concurrent-base")
    );

    // Check head parity — state is not duplicated.
    let left_state = pair.left().repo.doc_head_state(doc_id).await?;
    let right_state = pair.right().repo.doc_head_state(doc_id).await?;
    assert_eq!(
        left_state.sedimentree_heads, right_state.sedimentree_heads,
        "sedimentree heads must converge after concurrent syncs"
    );

    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Reconnect preserves live handles ─────────────────────────────────────
//
// After closing the transport connection and re-establishing it, previously
// acquired `BigDocHandle` values must remain valid (they hold internal leases
// to the local runtime) and must be able to read newly synced content.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_reconnect_preserves_live_handles() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(140, 141, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "handle-persists"))
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
    assert_eq!(
        read_text(&reader_doc, "title").await.as_deref(),
        Some("handle-persists")
    );

    // --- Close the transport connection but keep the handle.
    let old_left = pair.left_conn.take().expect("left connection");
    let _old_right = pair.right_conn.take().expect("right connection");
    old_left.stop().await?;

    // --- Reconnect.
    let new_left = pair.left().connect(pair.right()).await?;
    let new_right = pair.right().accepted_connection().await;
    pair.left_conn = Some(new_left);
    pair.right_conn = Some(new_right);

    // Sync keyhive to the new connection — the reader must catch up.
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // The OLD handle must still be valid and able to read content.
    let title = read_text(&reader_doc, "title").await;
    assert_eq!(
        title.as_deref(),
        Some("handle-persists"),
        "live handle must remain readable after reconnect"
    );

    // Owner writes new content after reconnect; the handle should pick it up
    // after a fresh sync.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "post-reconnect"))
                .map_err(|err| crate::ferr!("owner post-reconnect write failed: {err:?}"))
        })
        .await??;

    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(Duration::from_secs(10)))
        .await?;

    let phase = read_text(&reader_doc, "phase").await;
    assert_eq!(
        phase.as_deref(),
        Some("post-reconnect"),
        "live handle must see content written after reconnect"
    );

    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

// ─── Interrupted sync retry succeeds ──────────────────────────────────────
//
// A document sync that fails because the connection was closed can be
// retried after re-establishing the connection. The retry must succeed
// and deliver the document content.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_interrupted_sync_retry_succeeds() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(142, 143, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "retry-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // --- Make the connection fail by cloning and stopping one side.
    // The `closed` flag is shared via `Arc<AtomicBool>` so stopping the
    // clone marks the original as closed too.
    let closed_left = pair.left_conn().clone();
    closed_left.stop().await?;

    // Attempt sync on the now-closed connection — must fail with IoError.
    let fail_err = pair
        .left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(5)))
        .await
        .expect_err("sync on closed connection must fail");
    assert!(
        matches!(&fail_err, SyncDocError::IoError(_)),
        "failed sync must return IoError, got: {fail_err:?}"
    );

    // Replace the stale connections with fresh ones (reconnect).
    let old_left = pair.left_conn.take().expect("left connection");
    let _old_right = pair.right_conn.take().expect("right connection");
    drop(old_left);

    let new_left = pair.left().connect(pair.right()).await?;
    let new_right = pair.right().accepted_connection().await;
    pair.left_conn = Some(new_left);
    pair.right_conn = Some(new_right);

    // Sync keyhive on the new connection.
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Retry the doc sync — must succeed.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    assert_eq!(
        read_text(&reader_doc, "title").await.as_deref(),
        Some("retry-test")
    );

    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}
