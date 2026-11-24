//! Law 3: what the peer sends is checked at the edge, before anything reaches a sub-machine.

use super::*;

/// A page of `events` that resumes every target of `page` at 5, drained.
fn page_of(page: &PageSession, events: Vec<PageEvent>) -> SessionPage {
    SessionPage {
        events,
        verdicts: page
            .targets
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Verdict::Events {
                        resume: 5,
                        drained: true,
                    },
                )
            })
            .collect(),
    }
}

/// A valid event followed by the `hostile` ones violate the protocol: the round fails whole,
/// nothing of the page applies, and the same round is repeated from the same cursors after
/// backoff.
fn assert_page_fails(hostile: impl FnOnce(&Rig) -> Vec<PageEvent>) {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let mut events = vec![rig.changed(5, 1, &[part_route(1)])];
    events.extend(hostile(&rig));
    rig.sync
        .round_replied(call, Reply::Answer(Ok(page_of(&page, events))));
    let cmds = rig.flush();
    assert!(
        cmds.records().is_empty() && cmds.rounds().is_empty() && cmds.edges().is_empty(),
        "nothing of the page applies: {cmds:?}"
    );
    assert_eq!(rig.sync.synced(&part(1)), Some(false));
    rig.wait(1);
    let retry = rig.flush().only_round();
    assert_eq!(
        retry.2.targets, page.targets,
        "repeated from the same cursors"
    );
}

fn changed(targets: Vec<TargetId>) -> PageEvent {
    PageEvent::Changed {
        rev: 6,
        obj_key: obj(2),
        targets,
        payload: serde_json::json!(6),
    }
}

#[test]
fn law_3_an_event_naming_no_target_fails_the_whole_page() {
    assert_page_fails(|_| vec![changed(vec![])]);
}

#[test]
fn law_3_a_target_the_round_did_not_ask_for_fails_the_whole_page() {
    assert_page_fails(|_| vec![changed(vec![999])]);
    assert_page_fails(|rig| vec![changed(vec![rig.target(&part_route(1)), 999])]);
    assert_page_fails(|_| {
        vec![PageEvent::Removed {
            rev: 6,
            obj_key: obj(2),
            target: 999,
        }]
    });
}

#[test]
fn law_3_a_page_the_client_rejects_fails_the_round() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let mut answer = page_of(&page, vec![]);
    answer.verdicts.push((999, Verdict::Refused));
    rig.sync.round_replied(call, Reply::Answer(Ok(answer)));
    assert!(rig.flush().rounds().is_empty());
    rig.wait(1);
    assert_eq!(rig.flush().only_round().2.targets, page.targets);
}

#[test]
fn law_3_a_dropped_route_is_skipped_for_that_route_only() {
    let mut rig = Rig::new();
    rig.sync
        .update(&[(part(1), 0), (part(2), 0)], &[], &[], &[]);
    let asked = rig.flush();
    let decided = rig.decide(&asked, &[(1, cursor_summary(0)), (2, cursor_summary(0))]);
    let (call, _, page) = rig.ack(&decided).only_round();
    assert_eq!(page.targets.len(), 2);
    let both = rig.changed(5, 7, &[part_route(1), part_route(2)]);
    let only_dropped = rig.changed(6, 8, &[part_route(1)]);
    rig.remove_part(1);
    let cmds = rig.answer_round(call, &page, vec![both, only_dropped], 6, false);
    let (_, job) = cmds.only_record();
    assert_eq!((job.obj, job.add), (obj(7), vec![part(2)]));
    assert_eq!(
        cmds.rounds().len(),
        1,
        "a legitimate page: the lane carries on"
    );
}

#[test]
fn law_3_a_summary_entry_for_a_part_not_asked_is_ignored() {
    let mut rig = Rig::new();
    let asked = rig.add_part(1, 0);
    let decided = rig.decide(&asked, &[(1, cursor_summary(0)), (2, walk_summary(10))]);
    assert!(decided.lists().is_empty());
    assert_eq!(rig.sync.phase(&part(2)), None);
    let (_, update) = decided.sessions().remove(0);
    assert_eq!(update.additions.len(), 1);
}

#[test]
fn law_3_a_leaf_page_for_a_bucket_not_asked_is_ignored() {
    let mut rig = Rig::new();
    let (list, ..) = rig.walk(1, 0, 10);
    let (leaf, asked) = rig.list_root(list, 10, 2);
    assert_eq!(asked.len(), 1);
    let mut pages = leaf_page(None, &[(1, PartObjDelta::New)], None);
    let stray = leaf_page(None, &[(2, PartObjDelta::New)], None)
        .into_values()
        .next()
        .unwrap();
    pages.insert(BuckId::new(1, 3), stray);
    rig.sync.leaf_replied(leaf, Reply::Answer(pages));
    let records = rig.flush().records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].1.obj, obj(1));
}

#[test]
fn law_3_a_listing_the_walk_cannot_take_is_a_failed_call_not_an_assert() {
    let bad: [(&str, Vec<BucketSummary>); 3] = [
        ("out of order", vec![root_bucket(9, 1), root_bucket(9, 1)]),
        (
            "wrong level",
            vec![BucketSummary {
                id: BuckId::new(1, 0),
                ..root_bucket(9, 1)
            }],
        ),
        (
            "older than the cursor it asked past",
            vec![root_bucket(4, 1)],
        ),
    ];
    for (what, listing) in bad {
        let mut rig = Rig::new();
        let (list, offset, since) = rig.walk(1, 5, 10);
        rig.sync.list_replied(list, Reply::Answer(listing));
        assert!(rig.flush().lists().is_empty(), "{what}: backing off");
        rig.wait(1);
        let (retry, listed, again, again_since) = rig.flush().lists().remove(0);
        assert_ne!(retry, list);
        assert_eq!(
            (listed, again, again_since),
            (part(1), offset, since),
            "{what}"
        );
    }
}

#[test]
fn law_3_a_removal_on_an_object_route_fails_the_whole_page() {
    let mut rig = Rig::new();
    let (call, update) = rig.add_object(5).sessions().remove(0);
    rig.sync.update_replied(call, updated(&update));
    let (round, _, page) = rig.flush().only_round();
    let removal = rig.removed(7, 5, &obj_route(5));
    let answer = SessionPage {
        events: vec![removal],
        verdicts: vec![(
            page.targets[0].0,
            Verdict::Events {
                resume: 7,
                drained: true,
            },
        )],
    };
    rig.sync.round_replied(round, Reply::Answer(Ok(answer)));
    let cmds = rig.flush();
    assert!(cmds.records().is_empty() && cmds.rounds().is_empty());
    rig.wait(1);
    assert_eq!(rig.flush().only_round().2.targets, page.targets);
}

#[test]
fn law_3_a_row_at_or_below_the_cursor_the_target_was_asked_from_fails_the_page() {
    assert_page_fails(|rig| {
        vec![PageEvent::Changed {
            rev: 0,
            obj_key: obj(2),
            targets: vec![rig.target(&part_route(1))],
            payload: serde_json::json!(0),
        }]
    });
}

#[test]
fn law_3_a_row_past_the_resume_of_its_target_fails_the_page() {
    // the page resumes at 5; this row is at 6 and would carry the cursor past rows never read
    assert_page_fails(|rig| {
        vec![PageEvent::Changed {
            rev: 6,
            obj_key: obj(2),
            targets: vec![rig.target(&part_route(1))],
            payload: serde_json::json!(6),
        }]
    });
    assert_page_fails(|rig| vec![rig.removed(6, 2, &part_route(1))]);
}

#[test]
fn law_3_a_row_for_a_target_without_an_events_verdict_fails_the_page() {
    for refused in [true, false] {
        let mut rig = Rig::new();
        let (call, _, page) = rig.follow(1, 0).only_round();
        let event = rig.changed(5, 1, &[part_route(1)]);
        let verdicts = match refused {
            true => vec![(page.targets[0].0, Verdict::Refused)],
            false => vec![],
        };
        let answer = SessionPage {
            events: vec![event],
            verdicts,
        };
        rig.sync.round_replied(call, Reply::Answer(Ok(answer)));
        let cmds = rig.flush();
        assert!(
            cmds.records().is_empty() && cmds.rounds().is_empty(),
            "refused: {refused}"
        );
        assert_eq!(rig.sync.phase(&part(1)), Some(Phase::Cursor));
        rig.wait(1);
        assert_eq!(rig.flush().only_round().2.targets, page.targets);
    }
}

#[test]
fn law_3_a_target_named_twice_is_applied_once() {
    let mut rig = Rig::new();
    let (call, _, page) = rig.follow(1, 0).only_round();
    let id = rig.target(&part_route(1));
    let event = PageEvent::Changed {
        rev: 5,
        obj_key: obj(1),
        targets: vec![id, id],
        payload: serde_json::json!(5),
    };
    let (_, job) = rig
        .answer_round(call, &page, vec![event], 5, false)
        .only_record();
    assert_eq!(job.add, [part(1)]);
}
