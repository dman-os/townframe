//! A round's resolution and mapping over the memory store, run the way the responder runs it.

use super::*;

/// One write of a test transaction; objects and parts by index, access for `principal(0)`.
enum Write {
    Payload(u8, u8),
    Add(u8, u8),
    Remove(u8, u8),
    Grant(u8),
    Revoke(u8),
}

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

    fn commit(&self, writes: &[Write]) -> Rev {
        let store = &self.store;
        let mut tx = store.begin();
        for write in writes {
            let staged = match *write {
                Write::Payload(index, value) => block_on(PartStoreWrite::<Sendable>::set_payloads(
                    store,
                    &mut tx,
                    vec![(obj(index), payload(value))],
                )),
                Write::Add(index, part_index) => block_on(PartStoreWrite::<Sendable>::add_members(
                    store,
                    &mut tx,
                    &[(obj(index), part(part_index))],
                )),
                Write::Remove(index, part_index) => {
                    block_on(PartStoreWrite::<Sendable>::remove_members(
                        store,
                        &mut tx,
                        &[(obj(index), part(part_index))],
                    ))
                }
                Write::Grant(part_index) => block_on(PartStoreWrite::<Sendable>::grant_access(
                    store,
                    &mut tx,
                    &[(part(part_index), principal(0), AccessLevel::Read)],
                )),
                Write::Revoke(part_index) => block_on(PartStoreWrite::<Sendable>::revoke_access(
                    store,
                    &mut tx,
                    &[(part(part_index), principal(0))],
                )),
            };
            staged.unwrap();
        }
        tx.commit().expect("the writes change a row")
    }

    /// The responder's sequence for one round, as [`Round`] documents it.
    fn serve(&mut self, targets: &[(TargetId, Rev)], limit: u32) -> &SessionPage {
        self.serve_with(targets, limit, |_| {})
    }

    /// [`Fixture::serve`] with the content answers edited before the mapping, to stand in for a
    /// store that changed between the read and the content read.
    fn serve_with(
        &mut self,
        targets: &[(TargetId, Rev)],
        limit: u32,
        edit: impl FnOnce(&mut Vec<Option<ObjState>>),
    ) -> &SessionPage {
        let mut request = round(session(1), targets);
        request.limit = limit;
        self.server
            .resolve_round(&principal(0), &request, at(1), &mut self.round)
            .unwrap();
        let until = block_on(RevSource::<Sendable>::latest_rev(&self.store)).unwrap();
        assert!(
            self.round.after() < until,
            "the test reads a non-empty window"
        );
        block_on(PartStoreServe::<Sendable>::read_authorized(
            &self.store,
            &principal(0),
            self.round.routes(),
            self.round.after(),
            until,
            self.round.limits(),
            &mut self.read,
        ))
        .unwrap();
        let mut contents = block_on(PartStoreServe::<Sendable>::readable_obj_states(
            &self.store,
            &principal(0),
            self.round.content_keys(&self.read),
        ))
        .unwrap();
        edit(&mut contents);
        self.round
            .answer(&self.read, until, &contents, &mut self.page);
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
    let rev = fixture.commit(&[
        Write::Grant(1),
        Write::Payload(1, 10),
        Write::Add(1, 1),
        Write::Payload(2, 20),
        Write::Add(2, 2),
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
    let revoked = fixture.commit(&[Write::Revoke(1), Write::Grant(2)]);
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
    let rev = fixture.commit(&[
        Write::Grant(1),
        Write::Grant(2),
        Write::Payload(1, 10),
        Write::Add(1, 1),
        Write::Add(1, 2),
    ]);
    // One object's rows of one revision are one event, naming every target they reach.
    let page = fixture.serve(&[(1, 0), (2, 0)], 100);
    assert_eq!(page.events, [changed(rev, 1, &[1, 2], 10)]);

    let gone = fixture.commit(&[Write::Remove(1, 1)]);
    let page = fixture.serve(&[(1, rev), (2, rev)], 100);
    assert_eq!(page.events, [removed(gone, 1, 1)]);
    assert_eq!(
        page.verdicts,
        [(1, events(gone, true)), (2, events(gone, true))]
    );

    let edited = fixture.commit(&[Write::Payload(1, 11)]);
    let page = fixture.serve(&[(1, gone), (2, gone)], 100);
    assert_eq!(page.events, [changed(edited, 1, &[2], 11)]);
}

#[test]
fn law_8_and_10_resume_is_progress_never_below_the_cursor() {
    let mut fixture = Fixture::new(&[(1, part_route(1)), (2, part_route(2))]);
    let first = fixture.commit(&[Write::Grant(1), Write::Payload(1, 10), Write::Add(1, 1)]);
    let second = fixture.commit(&[Write::Payload(2, 20), Write::Add(2, 1)]);
    let last = fixture.commit(&[Write::Grant(2), Write::Payload(3, 30), Write::Add(3, 2)]);

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
fn law_11_a_removal_of_a_membership_the_target_never_saw_may_be_omitted() {
    let mut fixture = Fixture::new(&[(1, part_route(1))]);
    let before = fixture.commit(&[Write::Grant(1)]);
    let live = fixture.commit(&[Write::Payload(1, 10), Write::Add(1, 1)]);
    let gone = fixture.commit(&[Write::Remove(1, 1)]);
    let page = fixture.serve(&[(1, before)], 100);
    assert_eq!(page.events, []);
    assert_eq!(page.verdicts, [(1, events(gone, true))]);
    let page = fixture.serve(&[(1, live)], 100);
    assert_eq!(page.events, [removed(gone, 1, 1)]);
}

#[test]
fn content_gone_between_the_reads_ends_the_page_before_its_revision() {
    let mut fixture = Fixture::new(&[(1, part_route(1))]);
    let first = fixture.commit(&[Write::Grant(1), Write::Payload(1, 10), Write::Add(1, 1)]);
    let second = fixture.commit(&[Write::Payload(2, 20), Write::Add(2, 1)]);
    fixture.commit(&[Write::Payload(3, 30), Write::Add(3, 1)]);
    // Object 2 (the second of the sorted content keys) lost its readable content.
    let page = fixture.serve_with(&[(1, 0)], 100, |contents| contents[1] = None);
    assert_eq!(page.events, [changed(first, 1, &[1], 10)]);
    assert_eq!(page.verdicts, [(1, events(second - 1, false))]);
}

/// The read starts at the round's lowest cursor, so it hands out rows a target with a higher
/// cursor already has; the mapping keeps them from that target.
#[test]
fn a_row_reaches_only_the_targets_whose_cursor_is_below_it() {
    let mut fixture = Fixture::new(&[(1, part_route(1)), (2, obj_route(1))]);
    let rev = fixture.commit(&[Write::Grant(1), Write::Payload(1, 10), Write::Add(1, 1)]);
    let page = fixture.serve(&[(1, 0), (2, rev)], 100);
    assert_eq!(page.events, [changed(rev, 1, &[1], 10)]);
    assert_eq!(
        page.verdicts,
        [(1, events(rev, true)), (2, events(rev, true))]
    );
}
