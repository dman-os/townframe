//! The sync ladder as user stories (ADR 032 section 9), ported from daybook's
//! `sync/tests/ladder.rs`: nodes that meet only through their peers attachments, driven only
//! through their service clients. Daybook's clone bootstrap rung is provisioning, which belongs to
//! the user-node composition, not here.
//!
//! Each story ends by stopping its nodes, which also asserts that nothing outlives them.

use super::harness::Cluster;
use super::harness::Topology;

/// Alice and Bob, unconnected, with a drawer Alice made and both mirror.
async fn alice_and_bob() -> Cluster {
    Cluster::boot(&["alice", "bob"]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_connect() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let (alice, bob) = (cluster.peer(0), cluster.peer(1));
    let connected = cluster
        .node(0)
        .peers
        .as_ref()
        .unwrap()
        .connected()
        .await
        .unwrap();
    assert_eq!(connected, vec![bob.clone()]);
    let connected = cluster
        .node(1)
        .peers
        .as_ref()
        .unwrap()
        .connected()
        .await
        .unwrap();
    assert_eq!(connected, vec![alice]);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_note_written_before_connecting_replicates() {
    let mut cluster = alice_and_bob().await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "written alone").await;
    cluster.connect(Topology::Pair).await;
    cluster.note_is(1, &note, "main", "written alone").await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_added_before_connecting_replicates() {
    let mut cluster = alice_and_bob().await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let blob = cluster.add_blob(0, &drawer, b"a blob added alone").await;
    cluster.connect(Topology::Pair).await;
    assert_eq!(
        cluster.blob_arrives(1, &drawer, blob).await,
        b"a blob added alone"
    );
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_note_written_while_connected_replicates() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "written live").await;
    cluster.note_is(1, &note, "main", "written live").await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_added_while_connected_replicates() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    cluster.mirror_blobs(1, &drawer).await;
    let blob = cluster.add_blob(0, &drawer, b"a blob added live").await;
    assert_eq!(
        cluster.blob_arrives(1, &drawer, blob).await,
        b"a blob added live"
    );
    cluster.stop_all().await;
}

/// The writer of the first update edits the note again after the other has edited it.
#[tokio::test(flavor = "multi_thread")]
async fn updates_propagate_originator_then_other() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "v1").await;
    cluster.note_is(1, &note, "main", "v1").await;

    cluster.update_note(0, &note, "main", "v2 by alice").await;
    cluster.note_is(1, &note, "main", "v2 by alice").await;
    cluster.update_note(1, &note, "main", "v3 by bob").await;
    cluster.note_is(0, &note, "main", "v3 by bob").await;
    cluster.converged(&note).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn updates_propagate_other_then_originator() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "v1").await;
    cluster.note_is(1, &note, "main", "v1").await;

    cluster.update_note(1, &note, "main", "v2 by bob").await;
    cluster.note_is(0, &note, "main", "v2 by bob").await;
    cluster.update_note(0, &note, "main", "v3 by alice").await;
    cluster.note_is(1, &note, "main", "v3 by alice").await;
    cluster.converged(&note).await;
    cluster.stop_all().await;
}

/// Both edit different fields of the same state, neither having seen the other.
async fn divergent_edits(first: usize, second: usize) {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "base").await;
    let notes = cluster.converged(&note).await;
    let base = &notes[first];
    let also = &notes[second];

    cluster
        .update_note_from(first, &note, base, "title", "first's title")
        .await;
    cluster
        .update_note_from(second, &note, also, "aside", "second's aside")
        .await;

    let notes = cluster.converged(&note).await;
    for read in &notes {
        assert_eq!(read.text("main").as_deref(), Some("base"));
        assert_eq!(read.text("title").as_deref(), Some("first's title"));
        assert_eq!(read.text("aside").as_deref(), Some("second's aside"));
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn divergent_updates_converge_originator_then_other() {
    divergent_edits(0, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn divergent_updates_converge_other_then_originator() {
    divergent_edits(1, 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_note_survives_the_remote_restarting_and_reconnecting() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "initial").await;
    cluster.note_is(1, &note, "main", "initial").await;

    cluster.stop(1).await;
    cluster.restart(1).await;
    cluster.reconnect(1).await;
    cluster.note_is(1, &note, "main", "initial").await;

    cluster
        .update_note(0, &note, "main", "updated after restart")
        .await;
    cluster
        .note_is(1, &note, "main", "updated after restart")
        .await;
    cluster.converged(&note).await;
    cluster.stop_all().await;
}

/// Alice goes away; Bob edits an old note and writes a new one; Alice returns and catches up.
#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_was_shut_down_catches_up_on_returning() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let old = cluster.create_note(0, &drawer, "live base").await;
    cluster.note_is(1, &old, "main", "live base").await;

    cluster.stop(0).await;
    cluster
        .update_note(1, &old, "main", "bob while alice was away")
        .await;
    let new = cluster
        .create_note(1, &drawer, "created while alice was away")
        .await;

    cluster.restart(0).await;
    cluster.reconnect(0).await;
    cluster
        .note_is(0, &old, "main", "bob while alice was away")
        .await;
    cluster
        .note_is(0, &new, "main", "created while alice was away")
        .await;
    cluster.converged(&old).await;
    cluster.converged(&new).await;
    cluster.stop_all().await;
}

/// Bob goes away, each of them branches the note while apart, they meet again, and each merges
/// their own branch into the note; both end with both branches' work.
#[tokio::test(flavor = "multi_thread")]
async fn branches_made_apart_merge_and_converge() {
    let mut cluster = alice_and_bob().await;
    cluster.connect(Topology::Pair).await;
    let drawer = cluster.share_drawer(0, &[1]).await;
    let note = cluster.create_note(0, &drawer, "base").await;
    cluster.note_is(1, &note, "main", "base").await;

    cluster.stop(1).await;
    let feature_a = cluster.branch_note(0, &note, &drawer, "feature-a").await;
    cluster
        .update_note(0, &feature_a, "title", "title from feature a")
        .await;

    cluster.restart(1).await;
    let feature_b = cluster.branch_note(1, &note, &drawer, "feature-b").await;
    cluster
        .update_note(1, &feature_b, "aside", "aside from feature b")
        .await;

    cluster.reconnect(1).await;
    cluster.converged(&feature_a).await;
    cluster.converged(&feature_b).await;
    cluster.merge_note(0, &feature_a, &note).await;
    cluster.merge_note(1, &feature_b, &note).await;

    for read in cluster.converged(&note).await {
        assert_eq!(read.text("main").as_deref(), Some("base"));
        assert_eq!(read.text("title").as_deref(), Some("title from feature a"));
        assert_eq!(read.text("aside").as_deref(), Some("aside from feature b"));
    }
    cluster.stop_all().await;
}

/// Three nodes that each dial the others see one node's note and each other's edits.
#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_in_a_mesh_converge() {
    let mut cluster = Cluster::boot(&["alice", "bob", "carol"]).await;
    cluster.connect(Topology::Mesh).await;
    let drawer = cluster.share_drawer(0, &[1, 2]).await;
    let note = cluster.create_note(0, &drawer, "from alice").await;
    cluster.note_is(1, &note, "main", "from alice").await;
    cluster.note_is(2, &note, "main", "from alice").await;

    cluster
        .update_note(2, &note, "main", "edited by carol")
        .await;
    cluster.note_is(0, &note, "main", "edited by carol").await;
    cluster.note_is(1, &note, "main", "edited by carol").await;
    cluster.converged(&note).await;
    cluster.stop_all().await;
}
