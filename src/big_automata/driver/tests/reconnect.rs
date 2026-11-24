//! Laws 6 to 9: connections, incremental routes, facts out and errors.

use futures::FutureExt;

use super::rig::*;
use crate::driver::Fact;
use crate::driver::FactEvent;
use crate::interlude::*;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::set;

/// Law 6: a lost connection drops every call in flight and leaves every part `Unsynced`; a new
/// one is a fresh session that asks each part from its persisted cursor, and work already
/// settled is not redone.
#[tokio::test(start_paused = true)]
async fn law_6_a_new_connection_resumes_from_the_persisted_cursors() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1, 2], 10).await;
    rig.peer.publish(2, &[3], 20).await;
    rig.want(&[1, 2]).await;
    rig.connect();
    rig.synced(1).await;
    rig.synced(2).await;
    let first_session = rig.peer.updates()[0].session_id;
    let persisted = rig.cursors.get(&rig.peer_key(), "memory", &part(1));
    assert!(persisted > 0);

    rig.driver.disconnect();
    rig.fact(1, Fact::Unsynced).await;
    rig.fact(2, Fact::Unsynced).await;
    // the peer moves on while we are away
    rig.peer.commit(vec![set(&[(1, 11)])]).await;
    rig.connect();
    rig.synced(1).await;
    rig.synced(2).await;

    let second_session = rig.peer.updates().last().expect("an update").session_id;
    assert_ne!(first_session, second_session, "a fresh session id");
    assert_eq!(
        rig.peer.first_round_cursors(second_session)[&part(1)],
        persisted,
        "the part is asked from its persisted cursor, not from the start"
    );
    assert_eq!(
        rig.backend
            .count(&BackendEvent::Started(Kind::Record, obj(3))),
        1,
        "an object settled before the loss is not redone"
    );
    rig.assert_converged(1, 3).await;
    rig.assert_converged(2, 3).await;
}

/// Law 6: calls in flight when the connection is lost are dropped, and the work they were doing
/// is redone on the next one (backends are idempotent).
#[tokio::test(start_paused = true)]
async fn law_6_a_lost_connection_drops_the_calls_in_flight() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    rig.backend.hold_fetches(true);
    rig.want(&[1]).await;
    rig.connect();
    rig.backend.running(Kind::Fetch, &obj(1)).await;
    rig.driver.disconnect();
    until(|| {
        rig.backend
            .count(&BackendEvent::Dropped(Kind::Fetch, obj(1)))
            == 1
    })
    .await;
    rig.backend.hold_fetches(false);
    rig.connect();
    rig.synced(1).await;
    rig.assert_converged(1, 1).await;
}

/// Law 7: routes are incremental. Adding a wanted route or removing an unwanted one costs the
/// peer nothing; a removed route leaves no fact behind; a new connection asks for the routes
/// that are wanted by then and no others.
#[tokio::test(start_paused = true)]
async fn law_7_routes_are_incremental_and_re_added_as_they_stand() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    rig.peer.publish(2, &[2], 20).await;
    rig.want(&[1]).await;
    rig.connect();
    rig.synced(1).await;
    let updates = rig.peer.updates().len();

    rig.want(&[1]).await;
    rig.unwant(&[2]).await;
    n0_future::time::sleep(Duration::from_secs(30)).await;
    assert_eq!(rig.peer.updates().len(), updates, "no-ops reached the peer");

    rig.want(&[2]).await;
    rig.synced(2).await;
    assert_eq!(rig.peer.updates().len(), updates + 1);

    rig.unwant(&[1]).await;
    // the machine's own `Unsynced` for the removed part comes later and is not a fact
    n0_future::time::sleep(Duration::from_secs(30)).await;
    let mut stream = rig.facts.subscribe();
    assert_eq!(
        stream.next().await.part,
        part(2),
        "the removed part is still listed"
    );

    rig.driver.disconnect();
    rig.connect();
    n0_future::time::sleep(Duration::from_secs(30)).await;
    let session = rig.peer.updates().last().unwrap().session_id;
    assert_eq!(
        rig.peer
            .first_round_cursors(session)
            .into_keys()
            .collect::<Vec<_>>(),
        [part(2)]
    );
}

/// Drain what a stream has ready.
fn ready(stream: &mut crate::driver::FactStream) -> Vec<FactEvent> {
    let mut events = vec![];
    while let Some(event) = stream.next().now_or_never() {
        events.push(event);
    }
    events
}

/// Law 8: a subscriber reads the current facts first, so it never waits on an edge that
/// already happened.
#[tokio::test(start_paused = true)]
async fn law_8_a_late_subscriber_reads_the_current_facts() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    rig.want(&[1]).await;
    rig.connect();
    rig.synced(1).await;
    let mut late = rig.facts.subscribe();
    let first = late.next().now_or_never().expect("a current fact is ready");
    assert_eq!(
        (first.peer, first.part, first.fact),
        (rig.peer_key(), part(1), Fact::Synced)
    );
}

/// Law 8: a subscriber sees every edge, in order, through a connection cycle and a removal.
#[tokio::test(start_paused = true)]
async fn law_8_a_subscriber_sees_every_edge_in_order() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    let mut stream = rig.facts.subscribe();
    let mut next = async |rig: &Rig, step: &str| {
        let event = within(stream.next()).await;
        assert_eq!(
            (event.peer, event.part),
            (rig.peer_key(), part(1)),
            "{step}"
        );
        event.fact
    };
    rig.want(&[1]).await;
    assert_eq!(next(&rig, "wanted").await, Fact::Unsynced);
    rig.connect();
    assert_eq!(next(&rig, "connected").await, Fact::Synced);
    rig.driver.disconnect();
    assert_eq!(next(&rig, "lost").await, Fact::Unsynced);
    rig.connect();
    assert_eq!(next(&rig, "reconnected").await, Fact::Synced);
    rig.unwant(&[1]).await;
    assert_eq!(next(&rig, "removed").await, Fact::Removed);
}

/// Law 8: subscribers that join at any point of a churn end up agreeing with the facts as they
/// stand: none missed the edge that left the pair where it is.
#[tokio::test(start_paused = true)]
async fn law_8_subscribers_joining_mid_churn_agree_with_the_facts() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    rig.want(&[1]).await;
    let mut streams = vec![];
    for round in 0..6 {
        streams.push(rig.facts.subscribe());
        rig.connect();
        for _ in 0..round {
            tokio::task::yield_now().await;
        }
        rig.driver.disconnect();
        tokio::task::yield_now().await;
    }
    rig.connect();
    rig.synced(1).await;
    n0_future::time::sleep(Duration::from_secs(30)).await;
    for mut stream in streams {
        let last = ready(&mut stream).pop().expect("every stream saw the pair");
        assert_eq!(last.fact, Fact::Synced);
    }
}

/// Law 9: transport failures and backend failures are retried inside the machine, and a part
/// the peer does not serve stays `Unsynced` without taking the driver down.
#[tokio::test(start_paused = true)]
async fn law_9_failures_are_retried_and_nothing_else_is_fatal() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1, 2], 10).await;
    rig.peer.fail_next(6);
    rig.backend.fail_records(2);
    rig.want(&[1, 7]).await;
    rig.connect();
    rig.synced(1).await;
    rig.assert_converged(1, 2).await;
    n0_future::time::sleep(Duration::from_secs(120)).await;
    let mut stream = rig.facts.subscribe();
    let facts: Vec<_> = ready(&mut stream)
        .into_iter()
        .map(|event| (event.part, event.fact))
        .collect();
    assert_eq!(facts, [(part(1), Fact::Synced), (part(7), Fact::Unsynced)]);
}

/// Law 6: a reconnect closes the replaced session through the new connection, so a client that
/// reconnects more often than the peer's per-client session cap keeps being served instead of
/// being refused with `Limit` until the old sessions expire.
#[tokio::test(start_paused = true)]
async fn law_6_reconnecting_past_the_session_cap_closes_the_replaced_sessions() {
    let rig = Rig::new();
    rig.peer.publish(1, &[1], 10).await;
    rig.want(&[1]).await;
    for _ in 0..crate::replay::server::MAX_SESSIONS_PER_CLIENT + 4 {
        rig.connect();
        rig.synced(1).await;
        rig.driver.disconnect();
        rig.fact(1, Fact::Unsynced).await;
    }
    rig.connect();
    rig.synced(1).await;
}
