//! A round's resolution and mapping over the memory store, run the way the responder runs it.

use super::*;
use crate::part_store::laws;
use crate::part_store::laws::add;
use crate::part_store::laws::grant;
use crate::part_store::laws::remove;
use crate::part_store::laws::revoke;
use crate::part_store::laws::set;
use crate::part_store::oracle::PartWrite;

struct Fixture {
    store: MemoryPartStore,
    server: ReplayServer,
    round: Round,
    read: AuthorizedPage,
    page: SessionPage,
}

impl Fixture {
    /// A store and a session of `principal(0)` holding `targets`.
    fn new(targets: &[(TargetId, Route)]) -> Self {
        let mut server = ReplayServer::default();
        let open = update(session(1), 0, targets, &[]);
        server
            .update(&principal(0), &open, at(0), admit_all)
            .unwrap();
        Self {
            store: default(),
            server,
            round: default(),
            read: default(),
            page: SessionPage {
                events: default(),
                verdicts: default(),
            },
        }
    }

    /// One transaction of the laws' writes; it must change a row.
    fn commit(&self, writes: Vec<PartWrite>) -> Rev {
        let mut tx = self.store.begin();
        for write in writes {
            block_on(laws::stage(&self.store, &mut tx, write)).unwrap();
        }
        tx.commit().expect("the writes change a row")
    }

    /// The responder's sequence for one round, as [`Round`] documents it, without a hold.
    fn serve(&mut self, targets: &[(TargetId, Rev)], limit: u32) -> &SessionPage {
        let mut request = round(session(1), targets);
        request.limit = limit;
        self.server
            .resolve_round(&principal(0), &request, at(1), &mut self.round)
            .unwrap();
        let until = block_on(PartStoreServe::latest_rev(&self.store)).unwrap();
        block_on(PartStoreServe::read_authorized(
            &self.store,
            &principal(0),
            self.round.routes(),
            self.round.after().min(until),
            until,
            self.round.limits(),
            &mut self.read,
        ))
        .unwrap();
        self.round.answer(&mut self.read, until, &mut self.page);
        &self.page
    }
}

fn changed(rev: Rev, index: u8, targets: &[TargetId], value: u8) -> PageEvent {
    PageEvent::Changed {
        rev,
        obj_key: obj(index),
        targets: targets.to_vec(),
        payload: payload(value),
    }
}

fn removed(rev: Rev, index: u8, target: TargetId) -> PageEvent {
    PageEvent::Removed {
        rev,
        obj_key: obj(index),
        target,
    }
}

fn events(resume: Rev, drained: bool) -> Verdict {
    Verdict::Events { resume, drained }
}

#[test]
fn law_7_verdicts_follow_current_access_and_refusals_carry_no_rows() {
    let mut fixture = Fixture::new(&[(1, part_route(1)), (2, part_route(2)), (3, obj_route(2))]);
    let rev = fixture.commit(vec![
        grant(&[(1, 0)]),
        set(&[(1, 10)]),
        add(&[(1, 1)]),
        set(&[(2, 20)]),
        add(&[(2, 2)]),
    ]);
    let page = fixture.serve(&[(1, 0), (2, 0), (3, 0)], 100);
    assert_eq!(page.events, [changed(rev, 1, &[1], 10)]);
    assert_eq!(
        page.verdicts,
        [
            (1, events(rev, true)),
            (2, Verdict::Refused),
            (3, Verdict::Refused)
        ]
    );

    // Access taken away is gone for the backlog too: the rows the client never read stay
    // unread, and its cursor stays put.
    let revoked = fixture.commit(vec![revoke(&[(1, 0)]), grant(&[(2, 0)])]);
    let page = fixture.serve(&[(1, 0), (2, 0), (3, 0)], 100);
    assert_eq!(page.events, [changed(rev, 2, &[2, 3], 20)]);
    assert_eq!(
        page.verdicts,
        [
            (1, Verdict::Refused),
            (2, events(revoked, true)),
            (3, events(revoked, true))
        ]
    );
}

#[test]
fn law_12_object_targets_carry_content_and_part_targets_carry_removals() {
    let mut fixture = Fixture::new(&[(1, part_route(1)), (2, obj_route(1))]);
    let rev = fixture.commit(vec![
        grant(&[(1, 0)]),
        grant(&[(2, 0)]),
        set(&[(1, 10)]),
        add(&[(1, 1)]),
        add(&[(1, 2)]),
    ]);
    // One object's rows of one revision are one event, naming every target they reach.
    let page = fixture.serve(&[(1, 0), (2, 0)], 100);
    assert_eq!(page.events, [changed(rev, 1, &[1, 2], 10)]);

    let gone = fixture.commit(vec![remove(&[(1, 1)])]);
    let page = fixture.serve(&[(1, rev), (2, rev)], 100);
    assert_eq!(page.events, [removed(gone, 1, 1)]);
    assert_eq!(
        page.verdicts,
        [(1, events(gone, true)), (2, events(gone, true))]
    );

    let edited = fixture.commit(vec![set(&[(1, 11)])]);
    let page = fixture.serve(&[(1, gone), (2, gone)], 100);
    assert_eq!(page.events, [changed(edited, 1, &[2], 11)]);
}

#[test]
fn law_8_and_10_resume_is_progress_never_below_the_cursor() {
    let mut fixture = Fixture::new(&[(1, part_route(1)), (2, part_route(2))]);
    let first = fixture.commit(vec![grant(&[(1, 0)]), set(&[(1, 10)]), add(&[(1, 1)])]);
    let second = fixture.commit(vec![set(&[(2, 20)]), add(&[(2, 1)])]);
    let last = fixture.commit(vec![grant(&[(2, 0)]), set(&[(3, 30)]), add(&[(3, 2)])]);

    // A page of one row stops after the first revision. The second target's cursor is past
    // that, so it resumes from its own cursor, not the page's progress.
    let page = fixture.serve(&[(1, 0), (2, second)], 1);
    assert_eq!(page.events, [changed(first, 1, &[1], 10)]);
    assert_eq!(
        page.verdicts,
        [(1, events(first, false)), (2, events(second, false))]
    );

    let page = fixture.serve(&[(1, first), (2, second)], 1);
    assert_eq!(page.events, [changed(second, 2, &[1], 20)]);
    assert_eq!(
        page.verdicts,
        [(1, events(second, false)), (2, events(second, false))]
    );

    let page = fixture.serve(&[(1, second), (2, second)], 100);
    assert_eq!(page.events, [changed(last, 3, &[2], 30)]);
    assert_eq!(
        page.verdicts,
        [(1, events(last, true)), (2, events(last, true))]
    );
}

#[test]
fn law_11_a_removal_of_a_membership_added_after_the_cursor_is_delivered() {
    let mut fixture = Fixture::new(&[(1, part_route(1))]);
    let before = fixture.commit(vec![grant(&[(1, 0)])]);
    let live = fixture.commit(vec![set(&[(1, 10)]), add(&[(1, 1)])]);
    let gone = fixture.commit(vec![remove(&[(1, 1)])]);
    let page = fixture.serve(&[(1, before)], 100);
    assert_eq!(page.events, [removed(gone, 1, 1)]);
    assert_eq!(page.verdicts, [(1, events(gone, true))]);
    let page = fixture.serve(&[(1, live)], 100);
    assert_eq!(page.events, [removed(gone, 1, 1)]);
}

/// The read starts at the round's lowest cursor, so it hands out rows a target with a higher
/// cursor already has; the mapping keeps them from that target.
#[test]
fn a_row_reaches_only_the_targets_whose_cursor_is_below_it() {
    let mut fixture = Fixture::new(&[(1, part_route(1)), (2, obj_route(1))]);
    let rev = fixture.commit(vec![grant(&[(1, 0)]), set(&[(1, 10)]), add(&[(1, 1)])]);
    let page = fixture.serve(&[(1, 0), (2, rev)], 100);
    assert_eq!(page.events, [changed(rev, 1, &[1], 10)]);
    assert_eq!(
        page.verdicts,
        [(1, events(rev, true)), (2, events(rev, true))]
    );
}
