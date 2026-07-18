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
use automerge::{transaction::Transactable, ReadDoc};
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

/// A subscription can observe a DocCreated batch that was queued by the
/// preceding create operation before the switchboard delivered it. Consume
/// such already-admitted batches without assuming a scheduler order.
async fn recv_until_doc_changed(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<BigRepoChangeNotification>>,
) -> Vec<BigRepoChangeNotification> {
    loop {
        let batch = recv_one(rx).await;
        if batch.iter().any(|notification| {
            matches!(notification, BigRepoChangeNotification::DocChanged { .. })
        }) {
            tracing::debug!(
                batch_len = batch.len(),
                "received expected DocChanged batch"
            );
            return batch;
        }
        tracing::debug!(
            batch_len = batch.len(),
            "skipping notification batch without DocChanged while waiting"
        );
    }
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

    loop {
        let batch = recv_one(&mut rx).await;
        if batch.iter().any(|n| {
            matches!(
                n,
                BigRepoChangeNotification::DocChanged {
                    origin: BigRepoChangeOrigin::Local,
                    ..
                }
            )
        }) {
            break;
        }
        // A create notification queued before subscription may be delivered
        // after registration when the switchboard is busy. It is not the
        // mutation under test; keep receiving until that mutation arrives.
    }

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

// ========================================================================
// Polish: dedup, batching, listener lifecycle
// ========================================================================

// ─── Repeated sync does not duplicate notification ────────────────────────
//
// A `sync_doc_with_peer` that produces no new changes (the doc has already
// converged) must NOT emit a `DocChanged` notification. Only the first sync
// that actually delivers new commits fires the notification.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_repeated_sync_no_duplicate_notification() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(240, 241, "Owner", "Reader").await?;
    let reader_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "dedup-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    pair.left()
        .repo
        .grant_doc_access(doc_id, reader_agent, Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    // Subscribe on the owner side to see changes.
    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    // Doc was created + first sync already occurred before subscribing; no
    // bootstrap notification is pending.

    // Reader writes a change.
    let reader_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;
    reader_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "reader-write"))
                .map_err(|err| crate::ferr!("reader write failed: {err:?}"))
        })
        .await??;
    drop(reader_doc);

    // First sync: delivers the change → notification fires.
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;
    let first = recv_one(&mut rx).await;
    assert!(
        first
            .iter()
            .any(|n| matches!(n, BigRepoChangeNotification::DocChanged { .. })),
        "first sync carrying new commits must emit DocChanged"
    );

    // Second sync: no new data → no notification.
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(Duration::from_secs(10)))
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;
    assert_no_notification(&mut rx);

    drop(owner_doc);
    Ok(())
}

// ─── Multiple local mutations notification batching and order ───────────────
//
// Sequential local mutations produce notification batches with the mutations
// in causal order. Each `automerge::Patch` in a notification has a `path`
// and `action`; this test peeks into the patch action to assert the content
// and order of mutations without timing-based sleep.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_local_mutations_notification_batching() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(242, 243, "Owner", "BatchListener").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "batch-base"))
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

    // Doc was created before subscribing; no bootstrap notification pending.

    // Mutation 1: write "step" = "first".
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "step", "first"))
                .map_err(|err| crate::ferr!("first mutation: {err:?}"))
        })
        .await??;

    // Mutation 2: write "step" = "second".
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "step", "second"))
                .map_err(|err| crate::ferr!("second mutation: {err:?}"))
        })
        .await??;

    // Mutation 3: write "counter" = 3.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "counter", 3_i64))
                .map_err(|err| crate::ferr!("third mutation: {err:?}"))
        })
        .await??;

    pair.left().repo.wait_for_quiescence(None).await?;

    // Collect all notification batches. The runtime may deliver multiple
    // batches; aggregate them and inspect the patch actions.
    let mut step_values_in_order: Vec<String> = Vec::new();
    let mut saw_counter = false;
    loop {
        match rx.try_recv() {
            Ok(batch) => {
                for n in &batch {
                    if let BigRepoChangeNotification::DocChanged {
                        doc_id: seen,
                        patch,
                        ..
                    } = n
                    {
                        if *seen != doc_id {
                            continue;
                        }
                        if let automerge::PatchAction::PutMap { key, value, .. } = &patch.action {
                            match key.as_str() {
                                "step" => {
                                    if let (automerge::Value::Scalar(s), _) = value {
                                        if let automerge::ScalarValue::Str(s) = s.as_ref() {
                                            step_values_in_order.push(s.to_string());
                                        }
                                    }
                                }
                                "counter" => {
                                    saw_counter = true;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    assert!(
        !step_values_in_order.is_empty(),
        "must see at least one step mutation"
    );
    assert_eq!(
        step_values_in_order.first().map(String::as_str),
        Some("first"),
        "first step mutation must be 'first' in causal order"
    );
    assert_eq!(
        step_values_in_order.last().map(String::as_str),
        Some("second"),
        "last step mutation must be 'second' in causal order"
    );
    assert!(saw_counter, "must see the counter mutation");

    // Verify the final document state.
    let final_step = owner_doc
        .with_document_read(|doc| {
            doc.get(automerge::ROOT, "step")
                .ok()
                .flatten()
                .and_then(|(value, _)| match value {
                    automerge::Value::Scalar(s) => match s.as_ref() {
                        automerge::ScalarValue::Str(s) => Some(s.to_string()),
                        _ => None,
                    },
                    _ => None,
                })
        })
        .await;
    assert_eq!(
        final_step.as_deref(),
        Some("second"),
        "final doc state must reflect the last mutation"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Listener removal before mutation ─────────────────────────────────────
//
// Dropping the `ChangeListenerRegistration` unregisters the listener.
// Subsequent mutations must NOT be delivered to the old receiver.
// A new subscription after the mutation must receive subsequent ones.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_listener_removal_before_mutation() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(244, 245, "Owner", "DropListener").await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "drop-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Phase 1: subscribe and verify the listener works.
    let (reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    // Doc was created before subscribing; no bootstrap notification pending.

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "while-registered"))
                .map_err(|err| crate::ferr!("mutation during registration: {err:?}"))
        })
        .await??;
    pair.left().repo.wait_for_quiescence(None).await?;
    let batch_before = recv_until_doc_changed(&mut rx).await;
    assert!(
        batch_before
            .iter()
            .any(|n| matches!(n, BigRepoChangeNotification::DocChanged { .. })),
        "DocChanged must fire while listener is registered"
    );

    // Phase 2: drop registration, mutate. The old receiver is now
    // disconnected (channel closed when the listener was removed).
    drop(reg);

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "after-unregister"))
                .map_err(|err| crate::ferr!("mutation after unregister: {err:?}"))
        })
        .await??;
    pair.left().repo.wait_for_quiescence(None).await?;
    // Old rx must be disconnected (channel closed on unregister).
    assert!(
        matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ),
        "old receiver must be disconnected after unregister"
    );

    // Phase 3: re-subscribe, mutate, verify the new subscription fires.
    let (_reg2, mut rx2) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "phase", "after-resubscribe"))
                .map_err(|err| crate::ferr!("mutation after resubscribe: {err:?}"))
        })
        .await??;
    pair.left().repo.wait_for_quiescence(None).await?;
    let batch_after = recv_until_doc_changed(&mut rx2).await;
    assert!(
        batch_after
            .iter()
            .any(|n| matches!(n, BigRepoChangeNotification::DocChanged { .. })),
        "DocChanged must fire for the new subscription"
    );

    drop(owner_doc);
    Ok(())
}

// ─── Nested map path-prefix filter ─────────────────────────────────────────
//
// Subscribe to `["config", "theme"]`. Mutating that nested path fires.
// A sibling key under the same parent (`config.lang`) does NOT fire.
// Deleting the nested key fires `DeleteMap`.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_nested_path_prefix_filter() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(246, 247, "Owner", "NestedListener").await?;

    // Create doc with an initial seed value (BigRepo requires at least one
    // head), then add nested config map.
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "_init", true))
        .map_err(|err| crate::ferr!("init seed: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put_object(automerge::ROOT, "config", automerge::ObjType::Map)
                    .expect("put_object config");
                Ok::<(), automerge::AutomergeError>(())
            })
            .expect("transact create config");
        })
        .await?;

    // Get config ObjMeta.  The tuple from doc.get() is (Value, ObjMeta);
    // ObjMeta can be passed as `obj` to Transaction methods.
    let config_meta = owner_doc
        .with_document_read(|doc| {
            let (_, meta) = doc
                .get(automerge::ROOT, "config")
                .expect("get config")
                .expect("config must exist");
            meta
        })
        .await;

    // Populate config.theme and config.lang.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(&config_meta, "theme", "dark")
                    .expect("put config.theme");
                tx.put(&config_meta, "lang", "en").expect("put config.lang");
                Ok::<(), automerge::AutomergeError>(())
            })
            .expect("transact populate config");
        })
        .await?;

    use autosurgeon::Prop;
    let (_reg, mut rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: vec![Prop::Key("config".into()), Prop::Key("theme".into())],
        })
        .await?;

    // Drain bootstrap notifications.
    while rx.try_recv().is_ok() {}

    // --- Mutation under the subscribed path: write config.theme.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(&config_meta, "theme", "light")
                    .expect("put config.theme");
                Ok::<(), automerge::AutomergeError>(())
            })
            .expect("transact mutate theme");
        })
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;

    let batch = recv_one(&mut rx).await;
    let theme_changed = batch.iter().any(|n| match n {
        BigRepoChangeNotification::DocChanged {
            doc_id: seen,
            patch,
            ..
        } if *seen == doc_id => match &patch.action {
            automerge::PatchAction::PutMap { key, .. } => key.as_str() == "theme",
            _ => false,
        },
        _ => false,
    });
    assert!(
        theme_changed,
        "mutation under subscribed nested path config.theme must fire DocChanged"
    );

    // --- Sibling mutation: write config.lang (different key, same parent).
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.put(&config_meta, "lang", "fr").expect("put config.lang");
                Ok::<(), automerge::AutomergeError>(())
            })
            .expect("transact mutate lang");
        })
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;

    assert_no_notification(&mut rx);

    // --- Delete the nested key: delete config.theme.
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| {
                tx.delete(&config_meta, "theme")
                    .expect("delete config.theme");
                Ok::<(), automerge::AutomergeError>(())
            })
            .expect("transact delete theme");
        })
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;

    let del_batch = recv_one(&mut rx).await;
    let theme_deleted = del_batch.iter().any(|n| match n {
        BigRepoChangeNotification::DocChanged {
            doc_id: seen,
            patch,
            ..
        } if *seen == doc_id => match &patch.action {
            automerge::PatchAction::DeleteMap { key } => key.as_str() == "theme",
            _ => false,
        },
        _ => false,
    });
    assert!(
        theme_deleted,
        "deletion of nested key config.theme must fire DocChanged with DeleteMap"
    );

    drop(owner_doc);
    Ok(())
}

// ========================================================================
// Polish: bidirectional sync notification origins
// ========================================================================

// ─── Local and remote notifications during convergence ────────────────────
//
// Two Edit peers each write an independent field, then synchronise both
// directions.  Each peer must emit a Local notification for its own write
// and a Remote notification for the peer's write, all with correct doc_id.
#[tokio::test(flavor = "multi_thread")]
async fn tier7_bidirectional_sync_origin_correctness() -> crate::Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(248, 249, "Owner", "Editor").await?;
    let editor_agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;

    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", "origin-test"))
        .map_err(|err| crate::ferr!("failed creating doc: {err:?}"))?;
    let owner_doc = pair.left().repo.create_doc(initial).await?;
    let doc_id = owner_doc.document_id();

    // Grant Edit, sync both sides.
    pair.left()
        .repo
        .grant_doc_access(doc_id, editor_agent, Access::Edit)
        .await?;
    pair.left_conn().sync_keyhive_with_peer(None).await?;
    pair.right_conn().sync_keyhive_with_peer(None).await?;

    let editor_doc =
        fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await?;

    // Subscribe on both sides (doc already created, no bootstrap to drain).
    let (_reg_owner, mut owner_rx) = pair
        .left()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;
    let (_reg_editor, mut editor_rx) = pair
        .right()
        .repo
        .subscribe_change_listener(ChangeFilter {
            doc_id: Some(DocIdFilter::new(doc_id)),
            origin: None,
            path: Vec::new(),
        })
        .await?;

    // Owner writes field_a (Local on owner side).
    owner_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "field_a", "from_owner"))
                .map_err(|err| crate::ferr!("owner write: {err:?}"))
        })
        .await??;

    // Editor writes field_b (Local on editor side).
    editor_doc
        .with_document(|doc| {
            doc.transact(|tx| tx.put(automerge::ROOT, "field_b", "from_editor"))
                .map_err(|err| crate::ferr!("editor write: {err:?}"))
        })
        .await??;

    pair.left().repo.wait_for_quiescence(None).await?;
    pair.right().repo.wait_for_quiescence(None).await?;

    // Drain local notifications on each side.
    let owner_local = recv_one(&mut owner_rx).await;
    assert!(
        owner_local.iter().any(|n| matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                doc_id: did,
                origin: BigRepoChangeOrigin::Local,
                ..
            } if *did == doc_id
        )),
        "owner must see Local DocChanged for its own write"
    );
    let editor_local = recv_one(&mut editor_rx).await;
    assert!(
        editor_local.iter().any(|n| matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                doc_id: did,
                origin: BigRepoChangeOrigin::Local,
                ..
            } if *did == doc_id
        )),
        "editor must see Local DocChanged for its own write"
    );

    // --- Sync both directions.
    pair.left_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.right_conn()
        .sync_doc_with_peer(doc_id, Some(std::time::Duration::from_secs(10)))
        .await?;
    pair.left().repo.wait_for_quiescence(None).await?;
    pair.right().repo.wait_for_quiescence(None).await?;

    // Owner receives editor's field_b as Remote.
    let owner_remote = recv_one(&mut owner_rx).await;
    assert!(
        owner_remote.iter().any(|n| matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                doc_id: did,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            } if *did == doc_id
        )),
        "owner must see Remote DocChanged for editor's write"
    );

    // Editor receives owner's field_a as Remote.
    let editor_remote = recv_one(&mut editor_rx).await;
    assert!(
        editor_remote.iter().any(|n| matches!(
            n,
            BigRepoChangeNotification::DocChanged {
                doc_id: did,
                origin: BigRepoChangeOrigin::Remote { .. },
                ..
            } if *did == doc_id
        )),
        "editor must see Remote DocChanged for owner's write"
    );

    drop(editor_doc);
    drop(owner_doc);
    Ok(())
}
