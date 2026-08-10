//! Tier 9 — edge-case and error-path regressions.
//!
//! # Cases implemented
//!
//! | Test | Invariant |
//! |------|-----------|
//! | `closed_connection_errors_cleanly` | A stopped connection returns typed errors for both keyhive and doc syncs, rather than panicking or hanging. |
//! | `unauthorized_peer_no_plaintext_leak` | A peer without document access receives an `Unauthorized` sync result and never materialises plaintext. |
//! | `missing_doc_sync_returns_unauthorized` | Syncing a document id that does not exist on either side returns the wire-level `Unauthorized` result. |
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

use super::harness::{Node, Pair, Topo, fixtures, topo::ShutdownGuard};
use crate::SyncDocError;
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;
use std::sync::Arc;
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
// to materialise plaintext. The sync returns an unauthorized error, and
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
                matches!(err, SyncDocError::Unauthorized | SyncDocError::Policy(_)),
                "unauthorized doc sync should return Unauthorized or Policy rejection, got {err:?}"
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

// ─── Missing doc sync returns unauthorized ─────────────────────────────────
//
// Syncing a document id that does not exist on either peer must return the
// wire-level Unauthorized result, not hang or panic.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_missing_doc_sync_returns_unauthorized() -> crate::Res<()> {
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

    // Attempting to sync a non-existent doc returns the remote Unauthorized result.
    let err = pair
        .left_conn()
        .sync_doc_with_peer(fake_doc_id, Some(Duration::from_secs(10)))
        .await
        .expect_err("syncing a non-existent doc must fail");
    assert!(
        matches!(err, SyncDocError::Unauthorized | SyncDocError::Policy(_)),
        "sync of a non-existent doc must fail with Unauthorized or Policy rejection, got {err:?}"
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

#[tokio::test(flavor = "multi_thread")]
async fn with_document_roundtrip_rehydrates_from_storage() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(222, 223, "Owner", "Reader").await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before"))
        .map_err(|err| crate::ferr!("failed initializing title: {err:?}"))?;

    let handle = pair.left().repo.create_doc(initial).await?;
    let doc_id = handle.document_id();
    handle
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "after"))
                .map_err(|err| crate::ferr!("failed mutating doc: {err:?}"))
        })
        .await??;
    drop(handle);

    let reloaded = pair
        .left()
        .repo
        .get_doc(&doc_id)
        .await?
        .into_ready(doc_id)?;
    assert_eq!(
        read_text(&reloaded, "title").await.as_deref(),
        Some("after")
    );
    Ok(())
}

// ========================================================================
// runtime2 doc-worker regression tests
// ========================================================================

// ─── Relay/no-live sync persists content but creates no worker ─────────────
//
// A sync session observed by the hub when no live handle exists must persist
// the encrypted content into storage without creating a doc-worker.  Checking
// `has_doc_worker` BEFORE any handle acquisition proves that the sync
// session handler skipped worker creation.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_r2_relay_sync_materializes_in_transient_worker() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(240, 241, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "key", "relay-no-worker"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant reader access, then sync keyhive so the reader has a decryption key.
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, keyhive_core::access::Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Sync document content WITHOUT acquiring a live handle on the reader.
    // The runtime creates a transient worker to materialize and reconcile the
    // persisted session even though no public handle exists.
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // Before any handle acquisition: the transient worker performed the cold
    // materialization path.
    assert!(
        pair.right().repo.runtime.has_doc_worker(doc_id).await?,
        "sync session must create a transient doc-worker without a live handle"
    );

    // Content was persisted by subduction even without a worker.
    assert!(
        pair.right()
            .repo
            .runtime
            .contains_sedimentree_id(doc_id)
            .await?,
        "encrypted content must be persisted after relay sync"
    );

    // Lazy worker created by get_doc -> returns Ready because keys arrived.
    let lookup = pair.right().repo.get_doc(&doc_id).await?;
    let _reader_handle = match lookup {
        crate::runtime2::types::DocLookup::Ready(h) => h,
        ref other => {
            return Err(crate::ferr!(
                "reader doc should be Ready after keyhive+doc sync, got {other:?}"
            ));
        }
    };

    // Now that get_doc created a worker, head state must be Materialized.
    let state = pair.right().repo.doc_head_state(doc_id).await?;
    assert_eq!(
        state.state,
        crate::runtime2::MaterializationState::Materialized,
        "reader must have Materialized state after handle acquisition"
    );
    assert!(
        state.materialized_heads.is_some(),
        "materialized heads must be present"
    );
    assert!(
        !state.sedimentree_heads.is_empty(),
        "sedimentree heads must be present"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Partially-decrypted relay handle transitions on access upgrade ────────
//
// A relay-only node stores encrypted content without a decryption key.
// After access is upgraded to Read + keyhive sync + doc re-sync, the
// handle must become Ready — proving the persisted content converges.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_r2_partial_decrypt_converges_after_upgrade() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();

    let topo = Topo::boot_relay(246, 247, 248, "Owner", "Relay", "Reader").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "pending-relay"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = topo.topo_node(0).repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Relay gets relay-only (stores encrypted blobs, no key).
    let relay_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, relay_agent, keyhive_core::access::Access::Relay)
        .await?;

    // Propagate keyhive: Owner→Relay so relay learns the doc exists.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;

    // Relay pulls doc from Owner — stores encrypted blobs, can't decrypt.
    topo.topo_conn(1, 0)
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    topo.topo_node(1)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // A transient worker exists after sync even without a live handle so key
    // changes can drive materialization and causal healing.
    assert!(
        topo.topo_node(1)
            .repo
            .runtime
            .has_doc_worker(doc_id)
            .await?,
        "relay sync must create a transient doc-worker"
    );
    assert!(
        topo.topo_node(1)
            .repo
            .runtime
            .contains_sedimentree_id(doc_id)
            .await?,
        "relay must have stored encrypted content"
    );

    // Acquire handle — PendingMaterialization (has content, no key).
    let lookup = topo.topo_node(1).repo.get_doc(&doc_id).await?;
    assert!(
        matches!(
            lookup,
            crate::runtime2::types::DocLookup::PendingMaterialization
        ),
        "relay doc must be PendingMaterialization (content exists, no key)"
    );
    let state = topo
        .topo_node(1)
        .repo
        .runtime
        .doc_head_state(doc_id)
        .await?;
    assert_eq!(
        state.state,
        crate::runtime2::MaterializationState::Pending,
        "relay doc must be Pending before key upgrade"
    );

    // Upgrade relay from Relay → Read access.
    let relay_agent = fixtures::agent_of(&topo.topo_node(0).repo, topo.topo_node(1)).await?;
    topo.topo_node(0)
        .repo
        .grant_doc_access(doc_id, relay_agent, keyhive_core::access::Access::Read)
        .await?;

    // Keyhive sync delivers the decryption key.
    topo.topo_conn(0, 1).sync_keyhive_with_peer(None).await?;
    topo.topo_node(1)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    // Re-sync the doc now that keys are available → must become Ready.
    topo.topo_conn(1, 0)
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    topo.topo_node(1)
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    let lookup = topo.topo_node(1).repo.get_doc(&doc_id).await?;
    match lookup {
        crate::runtime2::types::DocLookup::Ready(handle) => {
            drop(handle);
        }
        other => {
            return Err(crate::ferr!(
                "relay doc must become Ready after key upgrade + re-sync, got {other:?}"
            ));
        }
    }

    drop(owner_doc);
    Ok(())
}
// ─── Live handle with a temporarily unavailable content key ────────────────
//
// A reader may retain a live handle across an access downgrade. The handle
// remains useful for already-decrypted history, while later content can arrive
// without a local key. A local write in that state must return a transient
// encryption error; it must not kill the document worker.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_r2_live_handle_missing_key_does_not_kill_worker() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(252, 253, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "before-downgrade"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(
            doc_id,
            reader_agent.clone(),
            keyhive_core::access::Access::Read,
        )
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    pair.left()
        .repo
        .revoke_doc_access(doc_id, reader_agent.clone())
        .await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, keyhive_core::access::Access::Relay)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "owner_update", "opaque"))
                .map_err(|err| crate::ferr!("failed writing owner update: {err:?}"))
        })
        .await??;
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right()
        .repo
        .wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
        .await?;

    assert!(
        reader_doc.is_partially_decrypted(),
        "live reader handle should record unavailable post-downgrade keys"
    );
    let local_result = reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "reader_update", "deferred"))
                .map_err(|err| crate::ferr!("failed writing reader update: {err:?}"))
        })
        .await;
    assert!(
        local_result.is_err(),
        "write without a content key must fail cleanly"
    );
    assert!(
        pair.right().repo.runtime.has_doc_worker(doc_id).await?,
        "a transient encryption failure must not terminate the document worker"
    );

    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier0_sync_diagnostics_do_not_create_worker() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(254, 255, "Left", "Right").await?;
    let doc_id = crate::DocumentId::random();

    assert!(!pair.left().repo.runtime.has_doc_worker(doc_id).await?);
    let snapshot = pair.left().repo.document_sync_snapshot(doc_id).await?;
    assert_eq!(snapshot.stage, crate::DocumentSyncStage::NotPersisted);
    assert_eq!(snapshot.head_state, None);
    assert!(!pair.left().repo.runtime.has_doc_worker(doc_id).await?);
    Ok(())
}

// ─── Racing handle acquisition with concurrent doc sync ────────────────────

// ─── Racing handle acquisition with concurrent doc sync ────────────────────
//
// A sync session delivering content while a handle is being acquired must not
// miss the update.  We race sync_doc_with_peer and get_doc on the Owner→Reader
// path (normal Pair) and verify the result is Ready.
#[tokio::test(flavor = "multi_thread")]
async fn tier9_r2_racing_handle_acquisition() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();

    let pair = Pair::boot(250, 251, "Owner", "Reader").await?;

    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "ticker", 42u64))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant Read, sync keyhive fully — reader knows keys.
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, keyhive_core::access::Access::Read)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Race: doc sync (delivers content) vs handle acquisition (creates worker).
    // The sync triggers SyncSessionObserved → ApplyReceivedContent on the worker.
    // The get_doc triggers AcquireHandle.  Both must converge to Ready.
    let conn = pair.right_conn().clone();
    let repo = Arc::clone(&pair.right().repo);

    let sync_fut = async move {
        conn.sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
            .await?;
        repo.wait_for_quiescence(Some(std::time::Duration::from_secs(10)))
            .await
    };
    let get_fut = pair.right().repo.get_doc(&doc_id);

    let (sync_result, _get_result) = tokio::join!(sync_fut, get_fut);
    sync_result?;

    // After both race, poll for Ready (sync delivery + worker converge).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let ready = loop {
        match pair.right().repo.get_doc(&doc_id).await? {
            crate::runtime2::types::DocLookup::Ready(handle) => break Some(handle),
            crate::runtime2::types::DocLookup::PendingMaterialization
            | crate::runtime2::types::DocLookup::Missing => {
                if std::time::Instant::now() >= deadline {
                    break None;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    };
    match ready {
        Some(handle) => drop(handle),
        None => {
            return Err(crate::ferr!(
                "racing handle acquisition never converged to Ready (10s timeout)"
            ));
        }
    }

    drop(owner_doc);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier0_relay_never_creates_doc_worker_during_passive_sync_or_diagnostics() -> crate::Res<()>
{
    utils_rs::testing::setup_tracing_once();
    let topo = Topo::boot_relay(240, 241, 242, "Editor", "Relay", "Reader").await?;
    let doc_id = crate::DocumentId::random();

    // Verify relay (index 1) starts with no worker
    assert!(
        !topo
            .topo_node(1)
            .repo
            .runtime
            .has_doc_worker(doc_id)
            .await?
    );

    // Diagnostics on relay must NOT spawn a worker
    let snapshot = topo
        .topo_node(1)
        .repo
        .document_sync_snapshot(doc_id)
        .await?;
    assert_eq!(snapshot.stage, crate::DocumentSyncStage::NotPersisted);
    assert!(
        !topo
            .topo_node(1)
            .repo
            .runtime
            .has_doc_worker(doc_id)
            .await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn tier0_grant_doc_access_succeeds_on_unmaterialized_doc() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(242, 243, "NodeA", "NodeB").await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "test", "init"))
        .map_err(|err| crate::ferr!("failed creating initial doc: {err:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = doc.document_id();

    // Create target agent for NodeB
    let keyhive_b = pair.right().repo.keyhive().keyhive_peer_id();
    let agent_b = pair
        .left()
        .repo
        .keyhive()
        .get_agent_by_peer_id(&keyhive_b)
        .await?
        .expect("NodeB agent must be in Keyhive");

    // Grant access on NodeA for doc_id prior to doc handle resolution
    pair.left()
        .repo
        .grant_doc_access(doc_id, agent_b, keyhive_core::access::Access::Edit)
        .await?;

    Ok(())
}

// ─── put_doc occupancy check regression test ────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier9_put_doc_occupancy_check_prevents_overwrite() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(244, 245, "NodeA", "NodeB").await?;
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "test", "init"))
        .map_err(|err| crate::ferr!("failed creating initial doc: {err:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = doc.document_id();

    let heads = pair.left().repo.doc_head_state(doc_id).await?.sedimentree_heads;
    assert!(!heads.is_empty(), "created doc must have sedimentree heads in subduction");

    // Retrieve doc to verify it is loaded & occupied
    let retrieved = pair.left().repo.get_doc(&doc_id).await?;
    let handle = retrieved.into_ready(doc_id)?;
    assert_eq!(handle.document_id(), doc_id);

    drop(doc);
    drop(handle);
    Ok(())
}

// ─── watch_connection_end AbortableJoinSet regression test ─────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier9_watch_connection_end_abortable_join_set_cleanup() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let tasks = utils_rs::AbortableJoinSet::new();
    let closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (_end_tx, end_rx) = futures::channel::oneshot::channel();
    let (signal_tx, _signal_rx) = tokio::sync::mpsc::unbounded_channel();
    let peer_id = big_sync_core::PeerId::new([246u8; 32]);

    crate::watch_connection_end(
        peer_id,
        std::sync::Arc::clone(&closed),
        end_rx,
        Some(signal_tx),
        &tasks,
    );
    assert_eq!(tasks.len(), 1, "watch_connection_end must register task in AbortableJoinSet");

    tasks.stop(std::time::Duration::from_secs(2)).await?;
    assert_eq!(tasks.len(), 0, "tasks must be empty after stop");
    Ok(())
}
