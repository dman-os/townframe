//! Tests of [`Hub`], by hand: one named test per law, each on the smallest case that shows it.
//! The laws are in the [parent module](super); the simulation in [`super::sim`] is where they
//! meet the real Document and Write coordinator machines, faults and disorder.

use std::collections::BTreeMap;

use big_automata::sessions::interest::Tier;

use super::*;
use crate::document::Outcome;

type H = Hub<u8, u8>;

fn hub() -> H {
    hub_with(Config {
        catch_up: 2,
        window: 2,
        ..Config::default()
    })
}

fn hub_with(config: Config) -> H {
    Hub::new(
        config,
        Boot {
            cursor: 0,
            nonce: [7; 16],
        },
    )
}

fn flush(hub: &mut H, now: Instant) -> Vec<Command<u8, u8>> {
    let mut out = vec![];
    hub.flush(now, &mut out);
    out
}

fn settled(consumed: u64, catching_up: bool, parked: bool, evictable: bool) -> Report {
    Report::Status(Status {
        consumed,
        catching_up,
        parked,
        evictable,
    })
}

fn stale(doc: DocId, rev: Rev) -> FeedRow {
    FeedRow {
        doc,
        rev,
        frontier_rev: 0,
        readable: true,
    }
}

fn starts(out: &[Command<u8, u8>]) -> Vec<(DocId, u64)> {
    out.iter()
        .filter_map(|c| match c {
            Command::StartDocument {
                doc, generation, ..
            } => Some((*doc, *generation)),
            _ => None,
        })
        .collect()
}

#[test]
fn law_1_one_document_per_id_and_a_new_generation_for_each_start() {
    let now = Instant::now();
    let mut h = hub();
    h.demand(1).unwrap();
    h.demand(1).unwrap();
    let [(_, g1)] = starts(&flush(&mut h, now))[..] else {
        panic!("one machine for two demands")
    };
    h.undemand(1);
    h.undemand(1);
    h.reported(1, g1, settled(0, false, false, true));
    h.reported(1, g1, Report::Evictable);
    assert_eq!(
        flush(&mut h, now),
        vec![Command::StopDocument {
            doc: 1,
            generation: g1
        }]
    );
    // wanted while the stop is out: nothing starts until the driver says the machine is gone
    h.demand(1).unwrap();
    assert!(starts(&flush(&mut h, now)).is_empty());
    h.reported(1, g1, Report::Stopped);
    let [(_, g2)] = starts(&flush(&mut h, now))[..] else {
        panic!("a successor starts once the machine is gone")
    };
    assert!(g2 > g1, "a later start is a later generation");
}

#[test]
fn law_2_stop_only_when_evictable_unwanted_and_granted() {
    let now = Instant::now();
    let mut h = hub();
    h.demand(1).unwrap();
    let [(_, g)] = starts(&flush(&mut h, now))[..] else {
        panic!()
    };
    // evictable and past its TTL, but a handle is held
    h.reported(1, g, settled(0, false, false, true));
    h.reported(1, g, Report::Evictable);
    assert!(flush(&mut h, now).is_empty());
    h.undemand(1);
    assert_eq!(
        flush(&mut h, now),
        vec![Command::StopDocument {
            doc: 1,
            generation: g
        }]
    );
    // the machine was busy when the request reached it: it runs on, and is not trusted until it
    // reports again
    h.reported(1, g, Report::StopRefused);
    assert_eq!(h.doc(1).generation, Some(g));
    h.reported(1, g, Report::Evictable);
    assert!(flush(&mut h, now).is_empty(), "no status since the refusal");
    h.reported(1, g, settled(0, false, false, true));
    h.reported(1, g, Report::Evictable);
    assert_eq!(
        flush(&mut h, now),
        vec![Command::StopDocument {
            doc: 1,
            generation: g
        }]
    );
    // a status that is out of date (a notice is unconsumed) is not trusted
    h.reported(1, g, Report::Stopped);
    h.demand(2).unwrap();
    let [(_, g)] = starts(&flush(&mut h, now))[..] else {
        panic!()
    };
    h.reported(2, g, settled(0, false, false, true));
    h.reported(2, g, Report::Evictable);
    h.interest_acquired(2).unwrap();
    h.interest_released(2);
    h.undemand(2);
    assert!(
        flush(&mut h, now)
            .iter()
            .all(|c| !matches!(c, Command::StopDocument { .. })),
        "the machine has not yet consumed the notice that may make it busy"
    );
}

#[test]
fn law_3_a_stale_readable_document_is_started_and_caught_up() {
    let now = Instant::now();
    let mut h = hub();
    h.feed(stale(1, 5));
    let out = flush(&mut h, now);
    let [(doc, g)] = starts(&out)[..] else {
        panic!("{out:?}")
    };
    assert_eq!(doc, 1);
    assert_eq!(h.doc(1).catch, Some(Catch::Running));
    assert_eq!(h.doc(1).unsettled, Some(5));
    // still catching up: nothing ends
    h.reported(1, g, settled(0, true, false, false));
    assert_eq!(h.doc(1).catch, Some(Catch::Running));
    h.reported(1, g, settled(0, false, false, true));
    assert_eq!(h.doc(1).catch, None);
    assert!(
        flush(&mut h, now).is_empty(),
        "the feed is not complete through 5"
    );
    h.feed_complete(5);
    assert_eq!(flush(&mut h, now), vec![Command::SaveCursor { rev: 5 }]);
    // a row that is not stale starts nothing
    h.feed(FeedRow {
        doc: 2,
        rev: 6,
        frontier_rev: 6,
        readable: true,
    });
    h.feed(FeedRow {
        doc: 3,
        rev: 7,
        frontier_rev: 0,
        readable: false,
    });
    h.feed_complete(7);
    assert_eq!(
        flush(&mut h, now),
        vec![Command::SaveCursor { rev: 7 }],
        "rows with nothing to do only move the cursor"
    );
}

#[test]
fn law_6_catch_up_is_bounded_and_queued_documents_start_as_slots_free() {
    let now = Instant::now();
    let mut h = hub();
    for doc in 1..=3 {
        h.feed(stale(doc, doc));
    }
    let out = flush(&mut h, now);
    let started = starts(&out);
    assert_eq!(started.len(), 2);
    assert_eq!(h.catching_up(), 2);
    assert_eq!(h.doc(3).catch, Some(Catch::Queued));
    let (first, g) = started[0];
    h.reported(first, g, settled(0, false, false, true));
    let out = flush(&mut h, now);
    assert_eq!(
        starts(&out).iter().map(|s| s.0).collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(h.catching_up(), 2);
}

#[test]
fn law_4_a_parked_document_wakes_on_its_epoch_move_and_not_before() {
    let now = Instant::now();
    let mut h = hub();
    h.feed(stale(1, 5));
    let [(_, g)] = starts(&flush(&mut h, now))[..] else {
        panic!()
    };
    h.reported(1, g, settled(0, false, true, true));
    assert_eq!(h.doc(1).catch, Some(Catch::Parked));
    // a parked document holds no slot, and its row's mark settled
    assert_eq!(h.catching_up(), 0);
    assert_eq!(h.doc(1).unsettled, None);
    h.epoch_moved(EpochMove {
        doc: 1,
        readable: true,
        tree_rev: 5,
        frontier_rev: 0,
    });
    assert_eq!(h.doc(1).catch, Some(Catch::Queued));
    let out = flush(&mut h, now);
    assert_eq!(h.doc(1).catch, Some(Catch::Running), "{out:?}");
    // the document stopped being readable: it leaves catch-up
    h.epoch_moved(EpochMove {
        doc: 1,
        readable: false,
        tree_rev: 5,
        frontier_rev: 0,
    });
    assert_eq!(h.doc(1).catch, None);
}

#[test]
fn law_5_a_keyhive_epoch_starts_nothing() {
    let now = Instant::now();
    let mut h = hub();
    // a document nobody holds, and one that is not stale
    h.epoch_moved(EpochMove {
        doc: 1,
        readable: true,
        tree_rev: 5,
        frontier_rev: 5,
    });
    assert!(flush(&mut h, now).is_empty());
    assert_eq!(h.doc(1).generation, None);
}

#[test]
fn law_7_the_cursor_waits_for_the_oldest_unsettled_row() {
    let now = Instant::now();
    let mut h = hub();
    h.feed(stale(1, 1));
    h.feed(FeedRow {
        doc: 2,
        rev: 2,
        frontier_rev: 2,
        readable: true,
    });
    let out = flush(&mut h, now);
    assert!(
        out.iter().all(|c| !matches!(c, Command::SaveCursor { .. })),
        "row 2 is settled but row 1 is not: {out:?}"
    );
    h.feed_complete(2);
    assert!(flush(&mut h, now).is_empty(), "row 1 is still unsettled");
    let g = h.doc(1).generation.unwrap();
    h.reported(1, g, settled(0, false, false, true));
    assert_eq!(flush(&mut h, now), vec![Command::SaveCursor { rev: 2 }]);
    // the rows of one revision arrive one by one: the cursor waits for the revision to complete
    h.feed(FeedRow {
        doc: 4,
        rev: 3,
        frontier_rev: 3,
        readable: true,
    });
    assert!(flush(&mut h, now).is_empty());
    h.feed(FeedRow {
        doc: 5,
        rev: 3,
        frontier_rev: 3,
        readable: true,
    });
    h.feed_complete(3);
    assert_eq!(flush(&mut h, now), vec![Command::SaveCursor { rev: 3 }]);
    assert_eq!(h.saved_cursor(), 3);
}

#[test]
fn law_8_notices_coalesce_inside_the_window() {
    let now = Instant::now();
    let mut h = hub_with(Config {
        window: 1,
        ..Config::default()
    });
    h.demand(1).unwrap();
    let g = h.doc(1).generation.unwrap();
    flush(&mut h, now);
    for rev in 1..=3 {
        h.feed(stale(1, rev));
    }
    h.interest_acquired(1).unwrap();
    let sent = flush(&mut h, now)
        .into_iter()
        .filter(|c| matches!(c, Command::ToDocument { .. }))
        .count();
    assert_eq!(sent, 1, "one notice fits the window");
    assert_eq!(h.doc(1).in_flight, 1);
    // it consumes the first: the rest goes as one notice, with the latest of everything
    h.reported(1, g, settled(1, true, false, false));
    let out = flush(&mut h, now);
    let notices: Vec<_> = out
        .iter()
        .filter_map(|c| match c {
            Command::ToDocument {
                input: ToDoc::Notice(notice),
                ..
            } => Some(*notice),
            _ => None,
        })
        .collect();
    assert_eq!(
        notices,
        vec![Notice {
            tree: Some(3),
            readable: None,
            acquired: 1,
            released: 0,
        }]
    );
}

#[test]
fn law_9_rounds_coalesce_defer_and_resolve_once() {
    let now = Instant::now();
    let mut h = hub();
    h.peer_connected(1, 0);
    h.keyhive_wanted(&1, Some(10));
    h.keyhive_wanted(&1, Some(11));
    h.keyhive_wanted(&1, None);
    h.keyhive_wanted(&1, Some(12));
    let out = flush(&mut h, now);
    let [Command::StartRound { call, .. }] = out
        .iter()
        .filter(|c| matches!(c, Command::StartRound { .. }))
        .cloned()
        .collect::<Vec<_>>()[..]
    else {
        panic!("one round for the peer: {out:?}")
    };
    // the round completes ahead of the admitted head: it is deferred, nobody is told yet
    h.round_done(call, Outcome::Done(5));
    assert!(matches!(
        h.round_of(&1),
        Some(rounds::RoundState::Deferred { .. })
    ));
    assert!(
        flush(&mut h, now)
            .iter()
            .all(|c| !matches!(c, Command::RoundResolved { .. }))
    );
    h.admission_advanced(5);
    let out = flush(&mut h, now);
    let resolved: Vec<_> = out
        .iter()
        .filter_map(|c| match c {
            Command::RoundResolved { waiter, ok } => Some((*waiter, *ok)),
            _ => None,
        })
        .collect();
    assert_eq!(
        resolved,
        vec![(10, true)],
        "only the admitted waiter; the rest wait for the next round"
    );
    assert_eq!(
        out.iter()
            .filter(|c| matches!(c, Command::StartRound { .. }))
            .count(),
        1,
        "one follow-up round for everything that arrived meanwhile"
    );
}

#[test]
fn law_10_draining_ends_when_nothing_is_in_flight_and_says_so_once() {
    let now = Instant::now();
    let mut h = hub();
    h.peer_connected(1, 0);
    h.keyhive_wanted(&1, Some(1));
    h.demand(1).unwrap();
    let g = h.doc(1).generation.unwrap();
    let out = flush(&mut h, now);
    let call = out
        .iter()
        .find_map(|c| match c {
            Command::StartRound { call, .. } => Some(*call),
            _ => None,
        })
        .unwrap();
    let dial = out
        .iter()
        .find_map(|c| match c {
            Command::Dial { call, .. } => Some(*call),
            _ => None,
        })
        .expect("the default session is dialed");
    h.drain();
    assert_eq!(h.demand(2), Err(Draining));
    let out = flush(&mut h, now);
    assert!(out.contains(&Command::CancelRound { call }));
    assert!(out.contains(&Command::RoundResolved {
        waiter: 1,
        ok: false
    }));
    assert!(
        !h.drained(),
        "the round, the dial and the machine are still out"
    );
    h.round_done(call, Outcome::Cancelled);
    h.dialed(now, dial, false);
    h.undemand(1);
    assert!(
        flush(&mut h, now)
            .iter()
            .all(|c| !matches!(c, Command::Drained))
    );
    h.reported(1, g, settled(0, false, false, true));
    let out = flush(&mut h, now);
    assert!(
        out.contains(&Command::StopDocument {
            doc: 1,
            generation: g
        }),
        "{out:?}"
    );
    assert!(
        !out.contains(&Command::Drained),
        "the stop is not yet answered"
    );
    h.reported(1, g, Report::Stopped);
    let out = flush(&mut h, now);
    assert!(out.contains(&Command::Drained));
    assert!(h.drained());
    assert!(flush(&mut h, now).is_empty(), "once");
}

#[test]
fn law_11_routes_follow_standing_through_the_wiring() {
    let now = Instant::now();
    let mut h = hub();
    h.peer_connected(1, 0);
    h.interest_acquired(7).unwrap();
    let eligible: BTreeMap<(u8, u8), Tier> = [((1, 0), Tier::Default)].into();
    h.standing(7, eligible);
    let out = flush(&mut h, now);
    let call = out
        .iter()
        .find_map(|c| match c {
            Command::Dial { call, .. } => Some(*call),
            _ => None,
        })
        .expect("the default session is dialed");
    h.dialed(now, call, true);
    let out = flush(&mut h, now);
    assert!(
        out.contains(&Command::AddRoute {
            doc: 7,
            peer: 1,
            principal: 0
        }),
        "{out:?}"
    );
    h.standing(7, BTreeMap::new());
    let out = flush(&mut h, now);
    assert!(
        out.contains(&Command::RemoveRoute {
            doc: 7,
            peer: 1,
            principal: 0
        }),
        "{out:?}"
    );
}

#[test]
fn law_12_every_machine_and_every_request_gets_a_fresh_actor() {
    let now = Instant::now();
    let mut h = hub();
    h.demand(1).unwrap();
    h.demand(2).unwrap();
    let g1 = h.doc(1).generation.unwrap();
    let mut actors = vec![];
    for command in flush(&mut h, now) {
        if let Command::StartDocument { actor, .. } = command {
            actors.push(actor);
        }
    }
    h.reported(1, g1, Report::NeedsActor { call: 9 });
    for command in flush(&mut h, now) {
        if let Command::ToDocument {
            input: ToDoc::ActorAssigned { call: 9, actor },
            ..
        } = command
        {
            actors.push(actor);
        }
    }
    assert_eq!(actors.len(), 3);
    let distinct: std::collections::BTreeSet<_> =
        actors.iter().map(|a| a.to_bytes().to_vec()).collect();
    assert_eq!(distinct.len(), 3);
    // a restarted Hub (another nonce) mints other actors
    let mut other = Hub::<u8, u8>::new(
        Config::default(),
        Boot {
            cursor: 0,
            nonce: [8; 16],
        },
    );
    other.demand(1).unwrap();
    let again = flush(&mut other, now);
    let Command::StartDocument { actor, .. } = &again[0] else {
        panic!()
    };
    assert!(!distinct.contains(actor.to_bytes()));
}
