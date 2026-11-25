//! The client machine against hand-written answers, one test per law it carries.

use super::*;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;

fn part_route(index: u8) -> Route {
    Route::Part {
        part_key: part(index),
    }
}

fn obj_route(index: u8) -> Route {
    Route::Object {
        obj_key: obj(index),
    }
}

fn updated(generation: Generation, refused: &[TargetId]) -> Result<SessionUpdated, SessionError> {
    Ok(SessionUpdated {
        generation,
        refused: refused.to_vec(),
    })
}

fn send(client: &mut ReplayClient) -> UpdateSession {
    client.next_update().expect("an update is due").clone()
}

/// A client whose wanted `routes` are all acknowledged, with the update that opened them.
fn acked(routes: &[Route]) -> (ReplayClient, UpdateSession) {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    for route in routes {
        client.want(route.clone());
    }
    let open = send(&mut client);
    assert_eq!(
        client.update_answered(&open, &updated(0, &[])).unwrap(),
        UpdateOutcome::Applied { blocked: 0 }
    );
    (client, open)
}

fn id_of(client: &ReplayClient, route: &Route) -> TargetId {
    client
        .acked()
        .find(|(_, acked)| *acked == route)
        .expect("the route is acknowledged")
        .0
}

fn page(verdicts: &[(TargetId, Verdict)]) -> Result<SessionPage, SessionError> {
    Ok(SessionPage {
        events: default(),
        verdicts: verdicts.to_vec(),
    })
}

#[test]
fn law_1_the_first_update_opens_with_the_whole_wanted_set() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    assert!(client.next_update().is_none());
    client.want(part_route(1));
    client.want(obj_route(1));
    let open = send(&mut client);
    assert_eq!(open.session_id, Uuid::from_u128(1));
    assert_eq!(open.generation, 0);
    assert!(open.removals.is_empty());
    let mut routes: Vec<_> = open
        .additions
        .iter()
        .map(|(_, route)| route.clone())
        .collect();
    routes.sort();
    assert_eq!(routes, [part_route(1), obj_route(1)]);
    assert_ne!(open.additions[0].0, open.additions[1].0);
}

#[test]
fn law_2_one_update_flies_and_a_lost_one_is_resent_unchanged() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    let open = send(&mut client);
    // Changes made while it flies wait for its answer.
    client.want(part_route(2));
    assert!(client.next_update().is_none());
    client.update_lost(&open);
    assert_eq!(send(&mut client), open);
    assert!(client.next_update().is_none());
    // The retry's answer is the first answer's: generation 0, nothing refused.
    client.update_answered(&open, &updated(0, &[])).unwrap();
    let next = send(&mut client);
    assert_eq!(next.generation, 1);
    assert_eq!(next.additions.len(), 1);
    assert_eq!(next.additions[0].1, part_route(2));
}

#[test]
fn stale_answers_change_nothing() {
    let (mut client, open) = acked(&[part_route(1)]);
    client.want(part_route(2));
    let next = send(&mut client);
    // A duplicate answer of the opening, and an answer naming the update in flight but for
    // another session, are both stale.
    assert_eq!(
        client.update_answered(&open, &updated(0, &[])).unwrap(),
        UpdateOutcome::Stale
    );
    let mut foreign = next.clone();
    foreign.session_id = Uuid::from_u128(2);
    assert_eq!(
        client
            .update_answered(&foreign, &Err(SessionError::UnknownSession))
            .unwrap(),
        UpdateOutcome::Stale
    );
    client.update_lost(&open);
    assert!(
        client.next_update().is_none(),
        "a stale loss resends nothing"
    );
    assert_eq!(
        client.update_answered(&next, &updated(1, &[])).unwrap(),
        UpdateOutcome::Applied { blocked: 0 }
    );
    assert_eq!(client.acked().count(), 2);
}

#[test]
fn law_3_dropped_routes_are_removed_and_never_acknowledged() {
    let (mut client, _) = acked(&[part_route(1), part_route(2)]);
    let one = id_of(&client, &part_route(1));
    // Drop an acknowledged route, add two, then drop one of those before it is sent.
    client.unwant(&part_route(1));
    client.want(part_route(3));
    client.want(part_route(4));
    client.unwant(&part_route(4));
    let next = send(&mut client);
    assert_eq!(next.removals, [one]);
    assert_eq!(next.additions.len(), 1);
    let three = next.additions[0].0;
    // Drop the route in flight, and re-want it at once: it goes back under a fresh id, after
    // the removal of the one in flight.
    client.unwant(&part_route(3));
    client.want(part_route(3));
    client.update_answered(&next, &updated(1, &[])).unwrap();
    assert!(client.route_of(three).is_none());
    let after = send(&mut client);
    assert_eq!(after.removals, [three]);
    assert_eq!(after.additions.len(), 1);
    assert_ne!(after.additions[0].0, three);
    assert_eq!(after.additions[0].1, part_route(3));
}

#[test]
fn law_4_refused_routes_block_stay_wanted_and_return_under_fresh_ids() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    client.want(part_route(2));
    let open = send(&mut client);
    let refused = open
        .additions
        .iter()
        .find(|(_, route)| *route == part_route(2))
        .unwrap()
        .0;
    assert_eq!(
        client
            .update_answered(&open, &updated(0, &[refused]))
            .unwrap(),
        UpdateOutcome::Applied { blocked: 1 }
    );
    assert_eq!(client.blocked().collect::<Vec<_>>(), [&part_route(2)]);
    assert!(client.wanted().any(|route| *route == part_route(2)));
    assert!(client.route_of(refused).is_none());
    // Waiting for the backoff, nothing is due.
    assert!(client.next_update().is_none());
    assert!(client.is_settled());
    client.retry_blocked();
    let retry = send(&mut client);
    assert!(retry.removals.is_empty(), "a refused id was never held");
    assert_eq!(retry.additions.len(), 1);
    assert_ne!(retry.additions[0].0, refused);
    assert_eq!(client.blocked().count(), 0);
}

#[test]
fn a_dropped_route_refused_in_flight_needs_no_removal() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    let open = send(&mut client);
    client.unwant(&part_route(1));
    let id = open.additions[0].0;
    client.update_answered(&open, &updated(0, &[id])).unwrap();
    assert_eq!(client.blocked().count(), 0);
    assert!(client.next_update().is_none());
}

#[test]
fn law_5_unknown_session_reopens_with_the_whole_wanted_set() {
    let (mut client, open) = acked(&[part_route(1), part_route(2)]);
    let old_ids: BTreeSet<_> = client.acked().map(|(id, _)| id).collect();
    client.want(part_route(3));
    let next = send(&mut client);
    assert_eq!(
        client
            .update_answered(&next, &Err(SessionError::UnknownSession))
            .unwrap(),
        UpdateOutcome::Reset
    );
    assert_ne!(client.session_id(), open.session_id);
    assert_eq!(client.acked().count(), 0);
    assert!(client.round(|_| Some(0), 10, 0).is_none());
    let reopen = send(&mut client);
    assert_eq!(reopen.session_id, client.session_id());
    assert_eq!(reopen.generation, 0);
    assert!(reopen.removals.is_empty());
    assert_eq!(reopen.additions.len(), 3);
    assert!(reopen.additions.iter().all(|(id, _)| !old_ids.contains(id)));
}

#[test]
fn law_5_a_round_naming_a_forgotten_session_resets() {
    let (mut client, open) = acked(&[part_route(1)]);
    let request = client.round(|_| Some(0), 10, 0).unwrap();
    let mut outcomes = Vec::new();
    assert_eq!(
        client
            .round_answered(&request, &Err(SessionError::UnknownSession), &mut outcomes)
            .unwrap(),
        RoundOutcome::Reset
    );
    assert_ne!(client.session_id(), open.session_id);
    // Its answers, and the old session's, are stale from now on.
    assert_eq!(
        client
            .round_answered(&request, &page(&[(0, Verdict::Refused)]), &mut outcomes)
            .unwrap(),
        RoundOutcome::Stale
    );
    assert_eq!(
        client
            .update_answered(&open, &Err(SessionError::UnknownSession))
            .unwrap(),
        UpdateOutcome::Stale
    );
}

#[test]
fn an_answer_ahead_of_the_update_resets_and_one_behind_is_an_error() {
    let (mut client, _) = acked(&[part_route(1)]);
    client.unwant(&part_route(1));
    client.want(part_route(2));
    let next = send(&mut client);
    assert!(client.update_answered(&next, &updated(0, &[])).is_err());
    assert_eq!(
        client.update_answered(&next, &updated(5, &[])).unwrap(),
        UpdateOutcome::Reset
    );
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    let open = send(&mut client);
    assert!(client.update_answered(&open, &updated(0, &[99])).is_err());
    assert!(
        client
            .update_answered(&open, &Err(SessionError::Invalid("x".into())))
            .is_err()
    );
}

#[test]
fn law_14_a_limit_unwinds_the_update_as_maybe_applied() {
    let (mut client, _) = acked(&[part_route(1)]);
    let one = id_of(&client, &part_route(1));
    client.unwant(&part_route(1));
    client.want(part_route(2));
    let next = send(&mut client);
    assert_eq!(next.removals, [one]);
    let two = next.additions[0].0;
    assert_eq!(
        client
            .update_answered(&next, &Err(SessionError::Limit))
            .unwrap(),
        UpdateOutcome::Limit
    );
    let again = send(&mut client);
    assert_eq!(again.generation, next.generation + 1);
    assert_eq!(again.removals, [one, two]);
    assert_eq!(again.additions.len(), 1);
    assert_ne!(again.additions[0].0, two);
    assert_eq!(again.additions[0].1, part_route(2));
}

#[test]
fn law_6_rounds_name_acknowledged_targets_with_the_callers_cursors() {
    let (mut client, _) = acked(&[part_route(1), part_route(2)]);
    client.want(part_route(3));
    send(&mut client);
    let one = id_of(&client, &part_route(1));
    let request = client
        .round(|route| (*route != part_route(2)).then_some(7), 50, 100)
        .unwrap();
    // Route 3 is in flight, route 2 got no cursor.
    assert_eq!(request.targets, [(one, 7)]);
    assert_eq!((request.limit, request.hold_ms), (50, 100));
    assert!(client.round(|_| None, 50, 100).is_none());
}

#[test]
fn law_7_8_10_round_outcomes_advance_or_block() {
    let (mut client, _) = acked(&[part_route(1), part_route(2)]);
    let (one, two) = (
        id_of(&client, &part_route(1)),
        id_of(&client, &part_route(2)),
    );
    let request = client.round(|_| Some(3), 10, 0).unwrap();
    let mut outcomes = Vec::new();
    let answer = page(&[
        (
            one,
            Verdict::Events {
                resume: 9,
                drained: true,
            },
        ),
        (two, Verdict::Refused),
    ]);
    assert_eq!(
        client
            .round_answered(&request, &answer, &mut outcomes)
            .unwrap(),
        RoundOutcome::Answered
    );
    assert_eq!(
        outcomes,
        [
            (
                one,
                part_route(1),
                TargetOutcome::Advance {
                    resume: 9,
                    drained: true
                }
            ),
            (two, part_route(2), TargetOutcome::Refused),
        ]
    );
    assert!(client.route_of(two).is_none());
    assert_eq!(client.blocked().collect::<Vec<_>>(), [&part_route(2)]);
    let next = send(&mut client);
    assert_eq!(
        next.removals,
        [two],
        "the server still holds the refused id"
    );
    assert_eq!(next.additions, []);
}

#[test]
fn round_answers_racing_the_clients_own_changes_are_stale() {
    let (mut client, _) = acked(&[part_route(1), part_route(2)]);
    let (one, two) = (
        id_of(&client, &part_route(1)),
        id_of(&client, &part_route(2)),
    );
    let request = client.round(|_| Some(0), 10, 0).unwrap();
    client.unwant(&part_route(2));
    let mut outcomes = Vec::new();
    // The removal overtook the round.
    assert_eq!(
        client
            .round_answered(
                &request,
                &Err(SessionError::UnknownTarget { target: two }),
                &mut outcomes
            )
            .unwrap(),
        RoundOutcome::Stale
    );
    // The page came first: the dropped target's verdict is skipped.
    let answer = page(&[
        (
            one,
            Verdict::Events {
                resume: 1,
                drained: false,
            },
        ),
        (
            two,
            Verdict::Events {
                resume: 1,
                drained: false,
            },
        ),
    ]);
    client
        .round_answered(&request, &answer, &mut outcomes)
        .unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].0, one);
    // A target the client still holds that the server lost: the session is not shared any
    // more.
    assert_eq!(
        client
            .round_answered(
                &request,
                &Err(SessionError::UnknownTarget { target: one }),
                &mut outcomes
            )
            .unwrap(),
        RoundOutcome::Reset
    );
}

#[test]
fn faulty_pages_are_errors() {
    let (mut client, _) = acked(&[part_route(1), part_route(2)]);
    let (one, two) = (
        id_of(&client, &part_route(1)),
        id_of(&client, &part_route(2)),
    );
    let request = client.round(|_| Some(5), 10, 0).unwrap();
    let ok = Verdict::Events {
        resume: 5,
        drained: false,
    };
    let mut outcomes = Vec::new();
    for bad in [
        page(&[
            (
                one,
                Verdict::Events {
                    resume: 4,
                    drained: false,
                },
            ),
            (two, ok),
        ]),
        page(&[(one, ok)]),
        page(&[(one, ok), (two, ok), (one, ok)]),
        page(&[(one, ok), (two, ok), (99, ok)]),
        Err(SessionError::Limit),
    ] {
        assert!(
            client
                .round_answered(&request, &bad, &mut outcomes)
                .is_err()
        );
    }
    assert_eq!(client.acked().count(), 2, "a faulty page changes nothing");
}

#[test]
fn law_4_wanting_a_wanted_route_is_a_no_op_in_every_standing() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    client.want(part_route(2));
    client.want(part_route(3));
    // Pending: no second id.
    client.want(part_route(1));
    let open = send(&mut client);
    assert_eq!(open.additions.len(), 3);
    // In flight: the same id stays, and nothing new is queued.
    client.want(part_route(1));
    assert!(client.pending.is_empty());
    let refused = open
        .additions
        .iter()
        .find(|(_, route)| *route == part_route(3))
        .unwrap()
        .0;
    client
        .update_answered(&open, &updated(0, &[refused]))
        .unwrap();
    let before: Vec<_> = client.acked().map(|(id, _)| id).collect();
    // Acknowledged and blocked: still the same ids, nothing queued, the block stays.
    client.want(part_route(1));
    client.want(part_route(2));
    client.want(part_route(3));
    assert!(client.is_settled());
    assert!(client.next_update().is_none());
    assert_eq!(client.acked().map(|(id, _)| id).collect::<Vec<_>>(), before);
    assert_eq!(client.blocked().collect::<Vec<_>>(), [&part_route(3)]);
}

#[test]
fn law_4_unwanting_an_unwanted_route_is_a_no_op() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.unwant(&part_route(1));
    assert!(client.is_settled());
    assert!(client.next_update().is_none());
    // Also after the route was wanted and dropped: its id is not removed twice.
    let (mut client, _) = acked(&[part_route(1)]);
    let one = id_of(&client, &part_route(1));
    client.unwant(&part_route(1));
    client.unwant(&part_route(1));
    assert_eq!(send(&mut client).removals, [one]);
}

#[test]
fn law_4_unwanting_a_pending_or_blocked_route_leaves_without_a_removal() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    client.unwant(&part_route(1));
    assert!(
        client.is_settled(),
        "a pending route never reached the server"
    );
    assert!(client.next_update().is_none());

    client.want(part_route(2));
    let open = send(&mut client);
    let refused = open.additions[0].0;
    client
        .update_answered(&open, &updated(0, &[refused]))
        .unwrap();
    client.unwant(&part_route(2));
    assert_eq!(client.blocked().count(), 0);
    assert!(client.wanted().next().is_none());
    client.retry_blocked();
    assert!(client.next_update().is_none(), "nothing left to re-add");
}

#[test]
fn law_4_unwanting_an_in_flight_route_removes_it_after_the_update_is_answered() {
    let mut client = ReplayClient::new(Uuid::from_u128(1));
    client.want(part_route(1));
    let open = send(&mut client);
    let id = open.additions[0].0;
    client.unwant(&part_route(1));
    // The update is still out, so nothing goes before its answer.
    assert!(client.next_update().is_none());
    assert!(!client.is_settled());
    client.update_answered(&open, &updated(0, &[])).unwrap();
    assert!(
        client.route_of(id).is_none(),
        "a dropped route is not acknowledged"
    );
    let removal = send(&mut client);
    assert_eq!(removal.generation, 1);
    assert_eq!(removal.removals, [id]);
    assert!(removal.additions.is_empty());
}

#[test]
fn law_4_unwanting_an_acknowledged_route_queues_its_removal() {
    let (mut client, _) = acked(&[part_route(1), part_route(2)]);
    let one = id_of(&client, &part_route(1));
    client.unwant(&part_route(1));
    assert!(client.route_of(one).is_none());
    let update = send(&mut client);
    assert_eq!(update.removals, [one]);
    assert!(update.additions.is_empty());
}

#[test]
fn law_3_unwanting_then_wanting_a_route_gets_a_fresh_id() {
    let (mut client, _) = acked(&[part_route(1)]);
    let one = id_of(&client, &part_route(1));
    client.unwant(&part_route(1));
    client.want(part_route(1));
    let update = send(&mut client);
    assert_eq!(update.removals, [one]);
    assert_eq!(update.additions.len(), 1);
    assert_ne!(update.additions[0].0, one);
    assert_eq!(update.additions[0].1, part_route(1));
}
