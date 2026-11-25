//! Laws 9-11: when a walk upgrades, how walks share the window, and how walk calls retry.

use super::*;

/// Walk part 1 (cursor held 0, decided at 10) through one root bucket of `entries` and return
/// the records its deltas make.
fn walk_with(rig: &mut Rig, entries: &[(u8, PartObjDelta)]) -> Vec<(CallId, ObjJob)> {
    let (list, ..) = rig.walk(1, 0, 10);
    let (leaf, _) = rig.list_root(list, 10, entries.len() as u32);
    rig.sync
        .leaf_replied(leaf, Reply::Answer(leaf_page(None, entries, None)));
    let cmds = rig.flush();
    assert!(cmds.persists().is_empty(), "nothing persists mid-walk");
    cmds.records()
}

#[test]
fn law_9_a_walk_upgrades_when_it_is_done_and_nothing_is_outstanding_in_the_same_step() {
    let mut rig = Rig::new();
    let records = walk_with(
        &mut rig,
        &[(1, PartObjDelta::New), (2, PartObjDelta::Change)],
    );
    assert_eq!(records.len(), 2);
    assert!(rig.sync.walking(&part(1)));
    let first = rig.record_done(records[0].0);
    assert!(first.persists().is_empty() && rig.sync.walking(&part(1)));
    let last = rig.record_done(records[1].0);
    assert_eq!(last.persists(), [(part(1), 10)]);
    assert!(!rig.sync.walking(&part(1)), "its walk is dropped with it");
    assert_eq!(rig.sync.phase(&part(1)), Some(Phase::Cursor));
}

#[test]
fn law_9_a_removal_the_walk_found_holds_the_upgrade() {
    let mut rig = Rig::new();
    let records = walk_with(&mut rig, &[(1, PartObjDelta::Delete)]);
    assert_eq!(records[0].1.remove, [part(1)]);
    assert!(
        rig.sync.walking(&part(1)),
        "the walk is done, its removal is not"
    );
    rig.wait(60);
    assert!(rig.flush().persists().is_empty());
    assert_eq!(rig.record_done(records[0].0).persists(), [(part(1), 10)]);
}

#[test]
fn law_9_a_walking_part_persists_nothing_while_its_tail_applies() {
    let mut rig = Rig::new();
    let asked = rig.add_part(1, 0);
    let decided = rig.decide(&asked, &[(1, walk_summary(10))]);
    let (list, ..) = decided.lists().remove(0);
    assert_eq!(
        decided.sessions()[0].1.additions[0].1,
        part_route(1),
        "law 4: a walking part's route is wanted"
    );
    let round = rig.ack(&decided).only_round();
    assert_eq!(round.2.targets[0].1, 10, "the tail starts at the handover");
    assert_eq!(
        round.2.limit, 2,
        "law 12: while a walk pulls, rounds leave it half the window"
    );
    let event = rig.changed(12, 5, &[part_route(1)]);
    let (record, _) = rig
        .answer_round(round.0, &round.2, vec![event], 12, true)
        .only_record();
    assert!(
        rig.record_done(record).persists().is_empty(),
        "applied at 12, held, not persisted"
    );
    assert_eq!(
        rig.sync.synced(&part(1)),
        Some(false),
        "drained and applied, but still walking"
    );
    // the walk finds nothing: the part upgrades and persists what the tail applied
    rig.sync.list_replied(list, Reply::Answer(vec![]));
    let upgraded = rig.flush();
    assert_eq!(upgraded.persists(), [(part(1), 12)]);
    assert_eq!(upgraded.edges(), [(part(1), true)]);
}

#[test]
fn law_10_walks_share_the_window_in_turn() {
    let mut rig = Rig::new();
    rig.sync
        .update(&[(part(1), None), (part(2), None)], &[], &[], &[]);
    let asked = rig.flush();
    let decided = rig.decide(&asked, &[(1, walk_summary(10)), (2, walk_summary(10))]);
    let lists = decided.lists();
    assert_eq!(lists.len(), 2);
    let big = |base: u8| -> Vec<(u8, PartObjDelta)> {
        (base..base + 6).map(|n| (n, PartObjDelta::New)).collect()
    };
    let mut leaves = vec![];
    for (call, part_key, ..) in lists {
        let (leaf, _) = rig.list_root(call, 10, 6);
        leaves.push((leaf, if part_key == part(1) { 1 } else { 11 }));
    }
    for (leaf, base) in leaves {
        rig.sync
            .leaf_replied(leaf, Reply::Answer(leaf_page(None, &big(base), None)));
    }
    // window 4: one walk must not take all of it
    let per_part = |records: &[(CallId, ObjJob)]| {
        let of = |p| {
            records
                .iter()
                .filter(|(_, job)| job.add == [part(p)])
                .count()
        };
        (of(1), of(2))
    };
    let first = rig.flush().records();
    assert_eq!(per_part(&first), (2, 2));
    let mut settled = vec![];
    for (call, _) in &first {
        settled.extend(rig.record_done(*call).records());
    }
    assert_eq!(per_part(&settled), (2, 2), "and again as room frees");
}

#[test]
fn law_11_a_failed_list_call_is_reissued_unchanged_after_backoff() {
    let mut rig = Rig::new();
    let (list, offset, since) = rig.walk(1, 5, 10);
    rig.sync.list_replied(list, Reply::Lost);
    assert!(rig.flush().lists().is_empty());
    assert_eq!(rig.sync.next_due(), Some(rig.now + Duration::from_secs(1)));
    rig.wait(1);
    let (retry, listed, again, again_since) = rig.flush().lists().remove(0);
    assert_ne!(retry, list, "a retry is a new call");
    assert_eq!((listed, again, again_since), (part(1), offset, since));
    // the old call's late outcome is stale
    rig.sync
        .list_replied(list, Reply::Answer(vec![root_bucket(9, 1)]));
    assert!(rig.flush().leaves().is_empty());
    // a second failure waits twice as long
    rig.sync.list_replied(retry, Reply::Lost);
    rig.flush();
    rig.wait(1);
    assert!(rig.flush().lists().is_empty());
    rig.wait(1);
    assert_eq!(rig.flush().lists().len(), 1);
}

#[test]
fn law_11_a_failed_leaf_call_is_reissued_unchanged_and_the_walk_carries_on() {
    let mut rig = Rig::new();
    let (list, ..) = rig.walk(1, 0, 10);
    let (leaf, asked) = rig.list_root(list, 10, 1);
    rig.sync.leaf_replied(leaf, Reply::Lost);
    assert!(rig.flush().leaves().is_empty());
    rig.wait(1);
    let (retry, _, again) = rig.flush().leaves().remove(0);
    assert_eq!(again, asked);
    let page = leaf_page(None, &[(1, PartObjDelta::New)], None);
    rig.sync.leaf_replied(retry, Reply::Answer(page));
    assert_eq!(rig.flush().records().len(), 1, "the walk never knew");
}

/// Law 3: a leaf page that does not move past the `after` it answers makes no progress; the
/// call fails and is reissued, so a peer cannot page one bucket for ever.
#[test]
fn law_3_a_leaf_page_that_does_not_move_forward_fails_its_call() {
    let mut rig = Rig::new();
    let (list, ..) = rig.walk(1, 0, 10);
    let (leaf, _) = rig.list_root(list, 10, 1);
    let page = leaf_page(None, &[(1, PartObjDelta::New)], Some(obj(2)));
    rig.sync.leaf_replied(leaf, Reply::Answer(page));
    let (next, _, asked) = rig.flush().leaves().remove(0);
    assert_eq!(asked[0].after, Some(obj(2)));
    let stuck = leaf_page(Some(obj(2)), &[], Some(obj(2)));
    rig.sync.leaf_replied(next, Reply::Answer(stuck));
    assert!(rig.flush().leaves().is_empty(), "the call backs off");
    rig.wait(1);
    let (_, _, again) = rig.flush().leaves().remove(0);
    assert_eq!(again[0].after, Some(obj(2)), "the same request is reissued");
}

/// Law 3: a page is forward in the order leaves are paged in (deepest bucket, then key), not in
/// key order. A `next_after` below its `after` by key is forward when it hashes to a later
/// deepest bucket, and a walk that judged by key failed such pages for ever: a part of more
/// than three pages never synced, because the second page's `next_after` was below the first's.
#[test]
fn law_3_a_leaf_page_forward_in_leaf_order_but_below_by_key_is_accepted() {
    let (after, next) = (0..=u8::MAX)
        .flat_map(|a| (0..=u8::MAX).map(move |b| (obj(a), obj(b))))
        .find(|(after, next)| {
            next < after && BuckId::leaf_cmp(next, after) == std::cmp::Ordering::Greater
        })
        .expect("some pair of keys is paged in another order than it sorts");
    let mut rig = Rig::new();
    let (list, ..) = rig.walk(1, 0, 10);
    let (leaf, _) = rig.list_root(list, 10, 4);
    let first = leaf_page(None, &[(1, PartObjDelta::New)], Some(after.clone()));
    rig.sync.leaf_replied(leaf, Reply::Answer(first));
    let (second, _, asked) = rig.flush().leaves().remove(0);
    assert_eq!(asked[0].after, Some(after.clone()));
    let page = leaf_page(Some(after.clone()), &[(2, PartObjDelta::New)], Some(next.clone()));
    rig.sync.leaf_replied(second, Reply::Answer(page));
    let (_, _, asked) = rig.flush().leaves().remove(0);
    assert_eq!(
        asked[0].after,
        Some(next),
        "the walk moves on to the page after it"
    );
}
