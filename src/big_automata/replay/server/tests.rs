//! The server machine's laws, one test per law, and the round's mapping over the memory store.

use futures::executor::block_on;

use super::*;
use crate::part_store::PartStoreServe;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::payload;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;

fn at(secs: u64) -> Instant {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *EPOCH.get_or_init(Instant::now) + Duration::from_secs(secs)
}

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

fn session(index: u128) -> SessionId {
    Uuid::from_u128(index)
}

fn update(
    session_id: SessionId,
    generation: Generation,
    additions: &[(TargetId, Route)],
    removals: &[TargetId],
) -> UpdateSession {
    UpdateSession {
        session_id,
        generation,
        additions: additions.to_vec(),
        removals: removals.to_vec(),
    }
}

fn round(session_id: SessionId, targets: &[(TargetId, Rev)]) -> PageSession {
    PageSession {
        session_id,
        targets: targets.to_vec(),
        limit: 100,
        hold_ms: 0,
        linger_ms: None,
    }
}

fn admit_all(_: &Route) -> bool {
    true
}

fn never_asked(route: &Route) -> bool {
    panic!("admission asked for {route:?} by an update that does not apply")
}

fn targets(server: &ReplayServer, session_id: SessionId) -> BTreeMap<TargetId, Route> {
    server
        .session(&principal(0), &session_id)
        .expect("the session is held")
        .targets
        .iter()
        .map(|(id, route)| (*id, route.clone()))
        .collect()
}

fn updated(generation: Generation, refused: &[TargetId]) -> Result<SessionUpdated, SessionError> {
    Ok(SessionUpdated {
        generation,
        refused: refused.to_vec(),
    })
}

/// A server holding `session(1)` for `principal(0)` at generation 0 with ids 1 and 2 on parts
/// 1 and 2.
fn opened() -> ReplayServer {
    let mut server = ReplayServer::default();
    let open = update(
        session(1),
        0,
        &[(1, part_route(1)), (2, part_route(2))],
        &[],
    );
    assert_eq!(
        server.update(&principal(0), &open, at(0), admit_all),
        updated(0, &[])
    );
    server
}

#[test]
fn law_1_only_generation_zero_opens() {
    let mut server = ReplayServer::default();
    let late = update(session(1), 1, &[(1, part_route(1))], &[]);
    assert_eq!(
        server.update(&principal(0), &late, at(0), never_asked),
        Err(SessionError::UnknownSession)
    );
    assert!(server.session(&principal(0), &session(1)).is_none());

    // The opening's additions are the whole set; its removals have nothing to remove.
    let open = update(session(1), 0, &[(1, part_route(1))], &[7]);
    assert_eq!(
        server.update(&principal(0), &open, at(0), admit_all),
        updated(0, &[])
    );
    assert_eq!(targets(&server, session(1)), [(1, part_route(1))].into());
}

#[test]
fn law_2_a_repeat_reads_the_held_outcome_and_applies_nothing() {
    let mut server = opened();
    let refusing = |route: &Route| *route != part_route(3);
    let next = update(
        session(1),
        1,
        &[(3, part_route(3)), (4, part_route(4))],
        &[1],
    );
    assert_eq!(
        server.update(&principal(0), &next, at(1), refusing),
        updated(1, &[3])
    );
    let held = targets(&server, session(1));

    // The retry of a lost answer, and a stale copy of the opening: both read generation 1's
    // outcome, neither asks admission, neither changes the set.
    assert_eq!(
        server.update(&principal(0), &next, at(2), never_asked),
        updated(1, &[3])
    );
    let stale = update(
        session(1),
        0,
        &[(1, part_route(1)), (2, part_route(2))],
        &[],
    );
    assert_eq!(
        server.update(&principal(0), &stale, at(2), never_asked),
        updated(1, &[3])
    );
    assert_eq!(targets(&server, session(1)), held);
}

#[test]
fn law_3_removals_apply_first_and_unheld_ones_are_no_ops() {
    let mut server = opened();
    // Drop id 1 and re-add its route under a fresh id; remove an id never held.
    let swap = update(session(1), 1, &[(3, part_route(1))], &[1, 99]);
    assert_eq!(
        server.update(&principal(0), &swap, at(1), admit_all),
        updated(1, &[])
    );
    assert_eq!(
        targets(&server, session(1)),
        [(2, part_route(2)), (3, part_route(1))].into()
    );
    // A removal of an id already removed is a no-op too.
    let again = update(session(1), 2, &[], &[1]);
    assert_eq!(
        server.update(&principal(0), &again, at(1), admit_all),
        updated(2, &[])
    );
    assert_eq!(targets(&server, session(1)).len(), 2);
}

#[test]
fn malformed_updates_are_refused_whole() {
    let mut server = opened();
    let held = targets(&server, session(1));
    for bad in [
        update(
            session(1),
            1,
            &[(3, part_route(3)), (3, part_route(4))],
            &[],
        ),
        update(session(1), 1, &[], &[1, 1]),
        // Re-adding a held id: ids are never reused within a session.
        update(session(1), 1, &[(2, part_route(3))], &[]),
    ] {
        assert!(matches!(
            server.update(&principal(0), &bad, at(1), never_asked),
            Err(SessionError::Invalid(_))
        ));
    }
    assert_eq!(targets(&server, session(1)), held);
    assert_eq!(
        server
            .session(&principal(0), &session(1))
            .unwrap()
            .generation,
        0
    );
}

#[test]
fn law_4_admission_holds_the_admitted_and_names_the_rest() {
    let mut server = ReplayServer::default();
    let mut asked = Vec::new();
    let open = update(
        session(1),
        0,
        &[(1, part_route(1)), (2, obj_route(1)), (3, part_route(2))],
        &[],
    );
    let answer = server.update(&principal(0), &open, at(0), |route| {
        asked.push(route.clone());
        *route == part_route(1)
    });
    assert_eq!(answer, updated(0, &[2, 3]));
    assert_eq!(asked, [part_route(1), obj_route(1), part_route(2)]);
    assert_eq!(targets(&server, session(1)), [(1, part_route(1))].into());
}

#[test]
fn law_5_close_sweep_and_restart_forget_alike() {
    let mut round_buf = Round::default();
    let next = update(session(1), 1, &[], &[]);

    let mut closed = opened();
    closed.close(
        &principal(0),
        &CloseSession {
            session_id: session(1),
        },
    );
    let mut swept = opened();
    swept.sweep(at(0) + SESSION_TTL);
    let mut restarted = ReplayServer::default();
    for server in [&mut closed, &mut swept, &mut restarted] {
        assert_eq!(
            server.update(&principal(0), &next, at(1), never_asked),
            Err(SessionError::UnknownSession)
        );
        assert_eq!(
            server.resolve_round(
                &principal(0),
                &round(session(1), &[(1, 0)]),
                at(1),
                &mut round_buf
            ),
            Err(SessionError::UnknownSession)
        );
    }
    // Closing what is not held is a no-op.
    closed.close(
        &principal(0),
        &CloseSession {
            session_id: session(1),
        },
    );
}

#[test]
fn law_5_every_request_naming_a_session_keeps_it_alive() {
    let mut server = opened();
    let mut round_buf = Round::default();
    let half = SESSION_TTL / 2;
    // A stale update and a round both touch.
    let stale = update(session(1), 0, &[], &[]);
    server
        .update(&principal(0), &stale, at(0) + half, never_asked)
        .unwrap();
    server.sweep(at(0) + SESSION_TTL);
    server
        .resolve_round(
            &principal(0),
            &round(session(1), &[(1, 0)]),
            at(0) + SESSION_TTL,
            &mut round_buf,
        )
        .unwrap();
    server.sweep(at(0) + SESSION_TTL + half);
    assert!(server.session(&principal(0), &session(1)).is_some());
    server.sweep(at(0) + SESSION_TTL * 2);
    assert!(server.session(&principal(0), &session(1)).is_none());
}

#[test]
fn sessions_belong_to_their_principal() {
    let mut server = opened();
    let mut round_buf = Round::default();
    let next = update(session(1), 1, &[], &[1]);
    assert_eq!(
        server.update(&principal(1), &next, at(1), never_asked),
        Err(SessionError::UnknownSession)
    );
    assert_eq!(
        server.resolve_round(
            &principal(1),
            &round(session(1), &[(1, 0)]),
            at(1),
            &mut round_buf
        ),
        Err(SessionError::UnknownSession)
    );
    server.close(
        &principal(1),
        &CloseSession {
            session_id: session(1),
        },
    );
    assert_eq!(targets(&server, session(1)).len(), 2);
}

#[test]
fn law_6_rounds_name_held_ids_once() {
    let mut server = opened();
    let mut round_buf = Round::default();
    let mut resolve = |targets: &[(TargetId, Rev)]| {
        server.resolve_round(
            &principal(0),
            &round(session(1), targets),
            at(1),
            &mut round_buf,
        )
    };
    assert_eq!(
        resolve(&[(1, 0), (5, 0)]),
        Err(SessionError::UnknownTarget { target: 5 })
    );
    assert!(matches!(resolve(&[]), Err(SessionError::Invalid(_))));
    assert!(matches!(
        resolve(&[(1, 0), (1, 3)]),
        Err(SessionError::Invalid(_))
    ));
    assert_eq!(resolve(&[(2, 4), (1, 7)]), Ok(()));
    assert_eq!(round_buf.after(), 4);
    assert_eq!(
        round_buf.routes().parts,
        [(part(1), 7), (part(2), 4)].into_iter().collect()
    );
}

#[test]
fn a_round_may_not_name_one_route_twice() {
    let mut server = ReplayServer::default();
    let open = update(
        session(1),
        0,
        &[(1, part_route(1)), (2, part_route(1))],
        &[],
    );
    server
        .update(&principal(0), &open, at(0), admit_all)
        .unwrap();
    let mut round_buf = Round::default();
    assert!(matches!(
        server.resolve_round(
            &principal(0),
            &round(session(1), &[(1, 0), (2, 5)]),
            at(1),
            &mut round_buf
        ),
        Err(SessionError::Invalid(_))
    ));
}

#[test]
fn law_14_target_and_session_caps_refuse_whole() {
    let routes = |ids: std::ops::Range<u32>| ids.map(|id| (id, part_route(1))).collect::<Vec<_>>();
    let mut server = ReplayServer::default();
    let over = update(
        session(1),
        0,
        &routes(0..MAX_TARGETS_PER_SESSION as u32 + 1),
        &[],
    );
    assert_eq!(
        server.update(&principal(0), &over, at(0), never_asked),
        Err(SessionError::Limit)
    );
    let full = update(
        session(1),
        0,
        &routes(0..MAX_TARGETS_PER_SESSION as u32),
        &[],
    );
    // Counted before admission: a refusing admission does not make room.
    server
        .update(&principal(0), &full, at(0), |_| false)
        .unwrap();
    let mut server = ReplayServer::default();
    server
        .update(&principal(0), &full, at(0), admit_all)
        .unwrap();
    let one_more = update(session(1), 1, &[(u32::MAX, part_route(2))], &[]);
    assert_eq!(
        server.update(&principal(0), &one_more, at(0), never_asked),
        Err(SessionError::Limit)
    );
    assert_eq!(
        server
            .session(&principal(0), &session(1))
            .unwrap()
            .generation,
        0
    );
    // A removal in the same update makes room.
    let swap = update(session(1), 1, &[(u32::MAX, part_route(2))], &[0]);
    assert_eq!(
        server.update(&principal(0), &swap, at(0), admit_all),
        updated(1, &[])
    );

    let mut server = ReplayServer::default();
    for index in 0..MAX_SESSIONS_PER_CLIENT as u128 {
        server
            .update(
                &principal(0),
                &update(session(index), 0, &[], &[]),
                at(0),
                admit_all,
            )
            .unwrap();
    }
    let extra = update(session(u128::MAX), 0, &[], &[]);
    assert_eq!(
        server.update(&principal(0), &extra, at(0), never_asked),
        Err(SessionError::Limit)
    );
    // Another principal has caps of its own.
    server
        .update(&principal(1), &extra, at(0), admit_all)
        .unwrap();
}

#[test]
fn law_14_limit_and_hold_are_capped_silently() {
    let mut server = opened();
    let mut round_buf = Round::default();
    let mut request = round(session(1), &[(1, 0)]);
    request.limit = u32::MAX;
    request.hold_ms = u32::MAX;
    server
        .resolve_round(&principal(0), &request, at(1), &mut round_buf)
        .unwrap();
    assert_eq!(
        round_buf.limits().max_entries.get(),
        MAX_PAGE_LIMIT as usize
    );
    assert_eq!(round_buf.hold(), MAX_HOLD);
    request.limit = 0;
    request.hold_ms = 250;
    server
        .resolve_round(&principal(0), &request, at(1), &mut round_buf)
        .unwrap();
    assert_eq!(round_buf.limits().max_entries.get(), 1);
    assert_eq!(round_buf.hold(), Duration::from_millis(250));
}

mod conformance;
mod rounds;
