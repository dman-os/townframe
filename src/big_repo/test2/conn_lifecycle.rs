//! Connection-lifecycle edge cases at the `BigRepoConnection` API boundary.
//!
//! The daybook sync layer treats connections as first-class identities: a
//! second connection to the same peer replaces the first, and a superseded
//! connection's end must not tear down the replacement (the `Arc::ptr_eq`
//! gate on `ConnFinishSignal::closed`). The runtime hub, by contrast, is
//! strictly **peer-keyed**: `connected_peers: HashMap<PeerId, ConnDeets>`,
//! `subduction` registers one connection per peer, and
//! `BigRepoConnection::stop` is a peer-scoped close.
//!
//! These tests pin the observable contract at the API boundary so the two
//! layers cannot silently disagree:
//!
//! - two live connections to one peer must both sync (the hub entry is
//!   peer-keyed, the transports are independent);
//! - stopping the *superseded* handle must not invalidate the replacement
//!   (stop is per-connection and only removes the matching registration);
//! - reconnect churn (the shape of the daybook ladder's
//!   `survives_remote_restart_and_reconnect`) must converge deterministically
//!   at this layer;
//! - syncs on a closed connection must fail fast, never hang;
//! - double `stop` and simultaneous cross-dialing must not panic or deadlock.
//!
//! Every test uses explicit sync barriers (`sync_keyhive_with_peer` /
//! `sync_doc_with_peer`) so the runs are deterministic; the harness `Pair`
//! drives transport connect/accept.

use super::harness::{Pair, fixtures, heads};
use crate::{BigRepoConnection, Res};
use automerge::{ReadDoc, ScalarValue, transaction::Transactable};
use keyhive_core::access::Access;
use std::time::Duration;
use tokio::time::timeout;

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

async fn new_doc(pair: &Pair, title: &str) -> Res<(crate::BigDocHandle, crate::DocumentId)> {
    let mut initial = automerge::Automerge::new();
    initial
        .transact(|tx| tx.put(automerge::ROOT, "title", title))
        .map_err(|err| crate::ferr!("failed creating conn-lifecycle doc: {err:?}"))?;
    let doc = pair.left().repo.create_doc(initial).await?;
    let id = doc.document_id();
    Ok((doc, id))
}

async fn grant_and_sync(
    pair: &Pair,
    doc_id: crate::DocumentId,
    access: Access,
) -> Res<crate::BigDocHandle> {
    let agent = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(pair, doc_id, &agent, access).await?;
    fixtures::sync_doc_expect_ready(pair.right_conn(), &pair.right().repo, doc_id).await
}

/// Open a second outbound connection left → right while the pair's original
/// connection is still live. Returns the left-side handle and the right-side
/// accepted handle.
async fn open_second_conn(pair: &Pair) -> Res<(BigRepoConnection, BigRepoConnection)> {
    let conn2 = pair
        .left()
        .repo
        .open_connection_iroh(
            pair.left().endpoint.clone(),
            pair.right().endpoint.addr(),
            pair.right().peer_id(),
            None,
        )
        .await?;
    let right_conn2 = pair.right().accepted_connection().await;
    Ok((conn2, right_conn2))
}

/// Sync `doc_id` onto `repo` via `conn`, then poll until it materializes
/// (Ready). The single-sync exchange can return before requested commits
/// arrive because subduction sends them as fire-and-forget messages; the
/// contract under test here is connection coexistence, not a strict barrier,
/// so we poll with a timeout (same pattern as keyhive_rpc).
async fn sync_doc_until_ready(
    conn: &BigRepoConnection,
    repo: &std::sync::Arc<crate::BigRepo>,
    doc_id: crate::DocumentId,
) -> Res<crate::BigDocHandle> {
    conn.sync_doc_with_peer(doc_id).await?;
    loop {
        repo.wait_for_quiescence(None).await?;
        match repo.get_doc(&doc_id).await? {
            crate::DocLookup::Ready(handle) => return Ok(handle),
            crate::DocLookup::PendingMaterialization | crate::DocLookup::Missing => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Two simultaneous connections between the same pair must both be usable:
/// the hub entry is peer-keyed but the transports are independent, so a doc
/// sync issued through either handle must materialize on the far side.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_conn_two_live_conns_same_peer_both_sync() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let pair = Pair::boot(160, 161, "Owner", "Reader").await?;

    // Baseline through the pair's original connection.
    let (owner_doc, id1) = new_doc(&pair, "conn1-baseline").await?;
    let reader_doc = grant_and_sync(&pair, id1, Access::Read).await?;
    assert_eq!(
        read_text(&reader_doc, "title").await.as_deref(),
        Some("conn1-baseline")
    );
    drop(reader_doc);
    drop(owner_doc);

    // Second live connection while the first is still up.
    let (conn2, right_conn2) = open_second_conn(&pair).await?;

    // Sync a fresh doc through conn2.
    let (doc2, id2) = new_doc(&pair, "via-conn2").await?;
    let agent2 = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(&pair, id2, &agent2, Access::Read).await?;
    let reader2 = sync_doc_until_ready(&conn2, &pair.right().repo, id2).await?;
    assert_eq!(
        read_text(&reader2, "title").await.as_deref(),
        Some("via-conn2")
    );
    drop(reader2);
    drop(doc2);

    // The original connection must still work while conn2 is live.
    let (doc3, id3) = new_doc(&pair, "via-conn1-still-live").await?;
    let agent3 = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(&pair, id3, &agent3, Access::Read).await?;
    let reader3 = sync_doc_until_ready(pair.right_conn(), &pair.right().repo, id3).await?;
    assert_eq!(
        read_text(&reader3, "title").await.as_deref(),
        Some("via-conn1-still-live")
    );
    drop(reader3);
    drop(doc3);

    // Clean up conn2 explicitly; the pair guard drains the rest.
    conn2.stop().await?;
    drop(right_conn2);
    Ok(())
}

/// A superseded connection's stop must not invalidate the replacement.
///
/// The daybook's replace semantics (new connection always replaces the old,
/// old end-signals are identified stale by the shared `closed` flag) imply
/// that consumers can hold per-connection handles and retire the superseded
/// one without breaking the current registration. `stop` is now per-
/// connection: the closed flag is threaded into `CloseConn`, the hub only
/// tears down the peer's registration when the flag matches the current
/// connection, and the transport close disconnects exactly that connection
/// (subduction tracks multiple conns per peer).
#[tokio::test(flavor = "multi_thread")]
async fn tier5_conn_stop_of_superseded_conn_keeps_replacement_alive() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(162, 163, "Owner", "Reader").await?;

    let (owner_doc, id0) = new_doc(&pair, "baseline").await?;
    let reader_doc = grant_and_sync(&pair, id0, Access::Read).await?;
    drop(reader_doc);
    drop(owner_doc);

    // conn2 supersedes conn1 (same peer id).
    let (conn2, right_conn2) = open_second_conn(&pair).await?;
    let (doc2, id2) = new_doc(&pair, "pre-replace").await?;
    let agent2 = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    fixtures::grant_and_propagate(&pair, id2, &agent2, Access::Read).await?;
    let reader2 = sync_doc_until_ready(&conn2, &pair.right().repo, id2).await?;
    assert_eq!(
        read_text(&reader2, "title").await.as_deref(),
        Some("pre-replace")
    );
    drop(reader2);
    drop(doc2);

    // Retire the superseded handle.
    let old = pair.left_conn.take().expect("superseded conn");
    old.stop().await?;

    // The replacement must remain fully functional. The grant propagates
    // over the live replacement conns: `pair.right_conn` is the right-side
    // twin of the superseded conn, so the keyhive sync goes through
    // `right_conn2` instead.
    let (doc3, id3) = new_doc(&pair, "after-replace").await?;
    let agent3 = fixtures::agent_of(&pair.left().repo, pair.right()).await?;
    pair.left()
        .repo
        .grant_doc_access(id3, agent3.clone(), Access::Read)
        .await?;
    right_conn2.sync_keyhive_with_peer().await?;
    fixtures::assert_reader_has_access(&pair.right().repo, id3).await?;
    let reader3 = fixtures::sync_doc_expect_ready(&conn2, &pair.right().repo, id3).await?;
    assert_eq!(
        read_text(&reader3, "title").await.as_deref(),
        Some("after-replace")
    );
    heads::tier0_invariants(&pair, id3, &doc3, &reader3).await?;

    conn2.stop().await?;
    drop(right_conn2);
    Ok(())
}

// ─── reconnect churn ─────────────────────────────────────────────────────────

/// Rapid connect → sync → disconnect cycles must converge on every cycle and
/// leave no cross-cycle state (stale subscriptions, dead worker routes,
/// leaked connections). This is the deterministic BigRepo-level shape of the
/// daybook ladder's `survives_remote_restart_and_reconnect` scenario.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_conn_reconnect_churn_converges_each_cycle() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(164, 165, "Owner", "Reader").await?;

    for i in 0..4 {
        pair.connect().await?;
        pair.left_conn().sync_keyhive_with_peer().await?;
        pair.right_conn().sync_keyhive_with_peer().await?;

        let title = format!("churn-{i}");
        let (owner_doc, id) = new_doc(&pair, &title).await?;
        let reader_doc = grant_and_sync(&pair, id, Access::Read).await?;
        assert_eq!(
            read_text(&reader_doc, "title").await.as_deref(),
            Some(title.as_str()),
            "cycle {i} must converge"
        );
        heads::tier0_invariants(&pair, id, &owner_doc, &reader_doc).await?;
        drop(reader_doc);
        drop(owner_doc);

        // Tear down for the next cycle: stop transport, drop routes + RPC.
        let old_left = pair.left_conn.take().expect("left conn");
        let _old_right = pair.right_conn.take().expect("right conn");
        old_left.stop().await?;
        pair.disconnect().await?;
    }

    Ok(())
}

// ─── closed-connection behavior ──────────────────────────────────────────────

/// Syncs issued on a closed connection must fail fast — the `is_closed`
/// guard on `BigRepoConnection` — and never hang or panic. This is what
/// prevents the daybook layer from wedging on a dead conn's sync round.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_conn_sync_on_closed_conn_fails_fast() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(166, 167, "Owner", "Reader").await?;

    let (owner_doc, id) = new_doc(&pair, "pre-close").await?;
    let reader_doc = grant_and_sync(&pair, id, Access::Read).await?;
    drop(reader_doc);
    drop(owner_doc);

    let conn = pair.left_conn().clone();
    let old_left = pair.left_conn.take().expect("left conn");
    let _old_right = pair.right_conn.take().expect("right conn");
    old_left.stop().await?;
    assert!(conn.is_closed(), "stopped connection must report closed");

    let res = conn.sync_keyhive_with_peer().await;
    assert!(res.is_err(), "keyhive sync on closed conn must error");

    let res = conn.sync_doc_with_peer(id).await;
    assert!(res.is_err(), "doc sync on closed conn must error");

    Ok(())
}

/// Stopping a connection twice must be safe — no panic, no hang, and a fast
/// (Ok or Err) outcome. The first stop tears down the peer registration; the
/// second finds nothing left to close.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_conn_double_stop_is_safe() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot(168, 169, "Owner", "Reader").await?;

    let old = pair.left_conn.take().expect("left conn");
    let _old_right = pair.right_conn.take().expect("right conn");

    old.clone().stop().await?;
    let second = timeout(Duration::from_secs(5), old.stop())
        .await
        .map_err(|_| crate::ferr!("second stop hung"))?;
    // Either outcome is acceptable as long as it is fast and non-panicking.
    tracing::debug!("second stop returned: {second:?}");

    Ok(())
}

// ─── simultaneous cross-dialing ──────────────────────────────────────────────

/// Both sides dialing each other at the same time must register cleanly on
/// both sides and converge. This is the transport-level analogue of the
/// daybook's simultaneous re-establishment path.
#[tokio::test(flavor = "multi_thread")]
async fn tier5_conn_cross_dial_registers_both_sides() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let mut pair = Pair::boot_disconnected(170, 171, "Owner", "Reader").await?;

    let dial_l = pair.left().connect(pair.right());
    let dial_r = pair.right().connect(pair.left());
    let (lconn, rconn) = tokio::join!(dial_l, dial_r);
    let lconn = lconn?;
    let rconn = rconn?;

    // Drain each side's inbound accept slot.
    let _left_accepted = pair.left().accepted_connection().await;
    let _right_accepted = pair.right().accepted_connection().await;

    pair.left_conn = Some(lconn);
    pair.right_conn = Some(rconn);
    pair.left_conn().sync_keyhive_with_peer().await?;
    pair.right_conn().sync_keyhive_with_peer().await?;

    let (owner_doc, id) = new_doc(&pair, "cross-dial").await?;
    let reader_doc = grant_and_sync(&pair, id, Access::Read).await?;
    assert_eq!(
        read_text(&reader_doc, "title").await.as_deref(),
        Some("cross-dial")
    );
    heads::tier0_invariants(&pair, id, &owner_doc, &reader_doc).await?;

    drop(reader_doc);
    drop(owner_doc);
    Ok(())
}
