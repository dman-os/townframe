//! Tier 9 — edge-case and error-path regressions.
//!
//! # Cases implemented
//!
//! | Test | Invariant |
//! |------|-----------|
//! | `closed_connection_errors_cleanly` | A stopped connection returns typed errors (`"connection is closed"`) for both keyhive and doc syncs, rather than panicking or hanging. |
//! | `unauthorized_peer_no_plaintext_leak` | A peer without document access receives `SyncDocError::NotFound` on sync and never materialises plaintext (`DocLookup::Missing` / `PendingMaterialization`). |
//! | `missing_doc_sync_returns_not_found` | Syncing a document id that does not exist on either side returns `SyncDocError::NotFound`. |
//!
//! # Skipped-by-design
//!
//! - **put_doc conflict recovery**: `PutDocError::IdOccpuied` exists in the runtime
//!   layer but there is no public API to create a document with an explicit id;
//!   `create_doc` always generates a fresh keyhive-backed id. Not testable through
//!   the BigRepo public API.
//!
//! - **over-cap-frame**: The sedimentree boundary-commit mechanism tested by
//!   `local_boundary_commit_stores_requested_encrypted_fragment` in `test.rs` is an
//!   internal storage optimization (writing fragment blobs on first zero-byte head).
//!   There is no bounded-frame transport path exposed at the BigRepo API level,
//!   and the current runtime2 transport (subduction) handles large messages through
//!   streaming, not fixed-size frames.
//!
//! - **stop-waits-for-save-tasks**: Every test2 test exercises the RAII
//!   [`ShutdownGuard`] / [`Pair`] teardown path, which shuts down the repo runtime
//!   with a timeout. The `Runtime2StopToken::stop` path is exercised on every test
//!   completion. Adding an explicit test would duplicate coverage already provided
//!   by the harness.

use super::harness::{fixtures, topo::ShutdownGuard, Node, Pair};
use crate::SyncDocError;
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
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(5)))
        .await
        .expect_err("doc sync on closed connection must fail");
    // SyncDocError::IoError wraps the inner report; the Display is just
    // "IoError" so we check the variant and the Debug representation.
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
            .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
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
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await;
    match intruder_sync {
        Ok(()) => {
            // Sync succeeded at the transport level, but the doc must not
            // be Materialized (key material is missing).
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
        .sync_doc_with_peer(fake_doc_id, Some(std::time::Duration::from_secs(10)))
        .await
        .expect_err("syncing a non-existent doc must fail");
    assert!(
        matches!(err, SyncDocError::NotFound),
        "sync of non-existent doc must return NotFound, got {err:?}"
    );

    Ok(())
}
