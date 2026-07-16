//! Tier 7 — Notification filter matrix.
//!
//! Tests exercise `BigRepo::subscribe_change_listener` with various
//! combinations of `ChangeFilter` (doc_id, origin, path) and check that
//! only the expected notifications are delivered. Noop mutations must not
//! emit change events. Real local and remote mutations must emit `DocChanged`
//! with the correct origin.
//!
//! # Scenarios
//!
//! | Test                                      | Coverage                                            |
//! |-------------------------------------------|-----------------------------------------------------|
//! | `doc_id_filter`                           | Subscribe per-doc, only that doc's changes fire.    |
//! | `path_prefix_filter`                      | Subscribe to `/title`, only title changes fire.     |
//! | `origin_filter_remote`                    | Subscribe remote-only, local changes don't fire.    |
//! | `noop_mutation_emits_nothing`             | Empty transaction produces no DocChanged.           |
//! | `local_mutation_emits_doc_changed`        | Local write emits DocChanged with Local origin.     |

use super::harness::{fixtures, Pair};
use crate::changes::{
    BigRepoChangeNotification, BigRepoChangeOrigin, ChangeFilter, DocIdFilter, OriginFilter,
};
use automerge::transaction::Transactable;
use keyhive_core::access::Access;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Shared helper: wait for one notification batch or fail on timeout.
async fn recv_one(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
) -> Vec<BigRepoChangeNotification> {
    timeout(Duration::from_secs(3), rx.recv())
        .await
        .expect("timed out waiting for notification batch")
        .expect("listener closed unexpectedly")
}

/// Assert no notification is already queued.
fn assert_no_notification(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
) {
    match rx.try_recv() {
        Ok(notifications) => panic!("unexpected notification(s): {notifications:?}"),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
            panic!("change listener closed unexpectedly")
        }
    }
}

// ─── Doc ID filter ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier7_doc_id_filter() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(214, 215, "Owner", "Listener").await?;

    // Create two documents.
    let mut d1 = automerge::Automerge::new();
    d1.transact(|tx| tx.put(automerge::ROOT, "title", "doc-a"))
        .map_err(|err| crate::ferr!("failed creating doc A: {err:?}"))?;
    let doc_a = pair.left().repo.create_doc(d1).await?;
    let doc_a_id = doc_a.document_id();

    let mut d2 = automerge::Automerge::new();
    d2.transact(|tx| tx.put(automerge::ROOT, "title", "doc-b"))
        .map_err(|err| crate::ferr!("failed creating doc B: {err:?}"))?;
    let doc_b = pair.left().repo.create_doc(d2).await?;

    // Subscribe to doc A only.
    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_a_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    // Write to doc A — must fire notification.
    doc_a
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "updated-a"))
                .map_err(|err| crate::ferr!("failed writing doc A: {err:?}"))
        })
        .await??;

    let batch = recv_one(&mut rx).await;
    let has_doc_a_change = batch.iter().any(|n| match n {
        BigRepoChangeNotification::DocChanged {
            doc_id,
            origin: BigRepoChangeOrigin::Local,
            ..
        } => *doc_id == doc_a_id,
        _ => false,
    });
    assert!(
        has_doc_a_change,
        "doc A change must fire on subscribed listener"
    );

    // Write to doc B — must NOT fire (filtered by doc_id).
    doc_b
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "updated-b"))
                .map_err(|err| crate::ferr!("failed writing doc B: {err:?}"))
        })
        .await??;

    assert_no_notification(&mut rx);

    drop(doc_a);
    drop(doc_b);
    Ok(())
}

// ─── Path prefix filter ─────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier7_path_prefix_filter() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(216, 217, "Owner", "PathListener").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "seed"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Subscribe with path prefix `["title"]`.
    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: vec![autosurgeon::Prop::Key("title".into())],
        })
        .await?;

    // Write to "title" — must fire (matched by path prefix).
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "updated"))
                .map_err(|err| crate::ferr!("failed writing title: {err:?}"))
        })
        .await??;

    let batch = recv_one(&mut rx).await;
    let has_title = batch.iter().any(|n| match n {
        BigRepoChangeNotification::DocChanged {
            doc_id: seen,
            origin: BigRepoChangeOrigin::Local,
            ..
        } => *seen == doc_id,
        _ => false,
    });
    assert!(
        has_title,
        "title mutation must fire for path-filtered listener"
    );

    // Write to "notes" — must NOT fire (not under "title" prefix).
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "notes", "irrelevant"))
                .map_err(|err| crate::ferr!("failed writing notes: {err:?}"))
        })
        .await??;

    assert_no_notification(&mut rx);

    drop(owner_doc);
    Ok(())
}

// ─── Origin filter: remote only ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier7_origin_filter_remote() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(218, 219, "Owner", "OriginListener").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "origin-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Bootstrap the reader with Edit access before subscribing. Initial
    // materialization is Bootstrap-origin; this test observes the reader's
    // later mutation arriving remotely at the owner.
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: Some(OriginFilter::Remote),
            path: Vec::new(),
        })
        .await?;

    reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "remote-change"))
                .map_err(|err| crate::ferr!("failed remote source write: {err:?}"))
        })
        .await??;
    fixtures::sync_doc_expect_ready(pair.left_conn(), &pair.left().repo, doc_id).await?;
    pair.left().repo.wait_for_quiescence(None).await?;

    let batch = recv_one(&mut rx).await;
    let has_remote = batch.iter().any(|n| {
        matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }
        )
    });
    assert!(
        has_remote,
        "remote sync must deliver a DocChanged with Remote origin to the reader"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Noop mutation emits nothing ────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier7_noop_mutation_emits_nothing() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(220, 221, "Owner", "NoopListener").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "noop-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();
    pair.left().repo.wait_for_quiescence(None).await?;

    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    // Perform an empty transaction — no Automerge changes produced.
    owner_doc
        .with_document(|doc| -> Result<(), automerge::AutomergeError> {
            doc.empty_commit(automerge::transaction::CommitOptions::default());
            Ok(())
        })
        .await??;

    assert_no_notification(&mut rx);

    // A real mutation should still fire afterward.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "real-change"))
                .map_err(|err| crate::ferr!("failed real write: {err:?}"))
        })
        .await??;

    let batch = recv_one(&mut rx).await;
    let has_real = batch.iter().any(|n| {
        matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                origin: BigRepoChangeOrigin::Local,
                ..
            }
        )
    });
    assert!(has_real, "a real mutation must emit DocChanged");

    drop(owner_doc);
    Ok(())
}

// ─── Local mutation emits DocChanged with Local origin ──────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tier7_local_mutation_emits_doc_changed() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(222, 223, "Owner", "LocalListener").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "local"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: Some(OriginFilter::Local),
            path: Vec::new(),
        })
        .await?;

    // Local write must fire with Local origin.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "local-update"))
                .map_err(|err| crate::ferr!("failed local write: {err:?}"))
        })
        .await??;

    let batch = recv_one(&mut rx).await;
    let has_local = batch.iter().any(|n| {
        matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                origin: BigRepoChangeOrigin::Local,
                ..
            }
        )
    });
    assert!(has_local, "local mutation must fire with Local origin");

    drop(owner_doc);
    Ok(())
}

// ─── No live handle: remote mutation ───────────────────────────────────────

/// A subscribed change listener must continue to receive remote-mutation
/// notifications even after every document handle has been dropped. The
/// runtime's doc worker survives handle drop and emits both
/// `DocHeadsChanged` and `DocChanged` notifications for incoming remote
/// commits.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_no_live_handle_remote_mutation() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(230, 231, "Owner", "NoHandleReader").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "no-handle-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant Edit to the reader so they can later subscribe and receive
    // remote mutations after dropping the handle.
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Reader materialises once so a doc worker is spawned in the runtime.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    // Subscribe on the reader side before dropping the handle.
    let (_reg, mut rx) = pair
        .right()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: Some(OriginFilter::Remote),
            path: Vec::new(),
        })
        .await?;

    // Drop the handle. The doc worker stays alive in the runtime (weak ref).
    drop(reader_doc);

    // Owner writes, triggering a remote mutation on the reader side.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "title", "post-drop-write"))
                .map_err(|err| crate::ferr!("failed remote write: {err:?}"))
        })
        .await??;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;
    pair.right().repo.wait_for_quiescence(None).await?;

    let batch = recv_one(&mut rx).await;
    let has_remote = batch.iter().any(|n| {
        matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            }
        )
    });
    assert!(
        has_remote,
        "remote mutation must still fire DocChanged after the handle is dropped"
    );

    drop(owner_doc);
    Ok(())
}

// ─── DocCreated heads-only notification ─────────────────────────────────────

/// The `subscribe_change_listener` API delivers both change notifications
/// (with patches) and creation/import notifications (heads-only, no patch).
/// This test verifies a `DocCreated` notification is emitted and its `heads`
/// field is populated, confirming the heads-only notification path works.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_doc_created_heads_only() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(232, 233, "Owner", "HeadsOnlyListener").await?;
    let doc_label = "headsonly";

    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: None,
            origin: None,
            path: Vec::new(),
        })
        .await?;

    // Create a document and capture the notification.
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", doc_label))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    let batch = recv_one(&mut rx).await;
    let mut found_created = false;
    let mut created_heads: Option<std::sync::Arc<[automerge::ChangeHash]>> = None;
    for n in &batch {
        if let BigRepoChangeNotification::DocCreated {
            doc_id: seen,
            heads,
            origin: BigRepoChangeOrigin::Local,
        } = n
        {
            if *seen == doc_id {
                found_created = true;
                created_heads = Some(Arc::clone(heads));
            }
        }
    }
    assert!(
        found_created,
        "DocCreated notification must be emitted when a document is created"
    );
    let heads = created_heads.expect("DocCreated must carry heads");
    assert!(!heads.is_empty(), "DocCreated heads must not be empty");

    // Verify no DocChanged is mixed in — creation is synthesised as
    // DocCreated, not DocChanged.
    for n in &batch {
        assert!(
            !matches!(n, BigRepoChangeNotification::DocChanged { doc_id: seen, .. } if *seen == doc_id),
            "DocCreated must not also emit a DocChanged"
        );
    }

    drop(owner_doc);
    Ok(())
}
