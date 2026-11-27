//! The responder end to end over the memory store and a [`ReplayServer`]: rows, caught-up
//! rounds, holds (woken, expired, dropped), access changes between rounds, object targets and
//! admission.

use std::pin::pin;

use futures::channel::oneshot;
use futures::executor::block_on;

use super::*;
use crate::kit::Instant;
use crate::part_store::laws;
use crate::part_store::laws::add;
use crate::part_store::laws::grant;
use crate::part_store::laws::obj;
use crate::part_store::laws::part;
use crate::part_store::laws::payload;
use crate::part_store::laws::principal;
use crate::part_store::laws::remove;
use crate::part_store::laws::revoke;
use crate::part_store::laws::set;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartWrite;
use crate::replay::server::MAX_HOLD;
use crate::replay::server::ReplayServer;
use crate::replay::wire::PageEvent;
use crate::replay::wire::PageSession;
use crate::replay::wire::SessionId;
use crate::replay::wire::SessionUpdated;
use crate::replay::wire::TargetId;
use crate::replay::wire::Verdict;

mod herd;

/// A timer whose sleeps end only when the test says so.
#[derive(Clone, Default)]
struct ManualTimer {
    /// Every sleep handed out and not yet ended, with its duration and its wake.
    sleeps: Arc<std::sync::Mutex<Vec<Sleep>>>,
}

type Sleep = (Duration, oneshot::Sender<()>);

impl Timer for ManualTimer {
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send + use<> {
        let (wake, woken) = oneshot::channel();
        self.sleeps.lock().unwrap().push((duration, wake));
        async move { woken.await.expect("the test ends every sleep it keeps") }
    }
}

impl ManualTimer {
    /// The durations of the sleeps someone still waits on.
    fn outstanding(&self) -> Vec<Duration> {
        self.sleeps
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, wake)| !wake.is_canceled())
            .map(|(duration, _)| *duration)
            .collect()
    }

    /// How many sleeps were handed out since the last [`ManualTimer::expire`], dropped or not.
    fn handed_out(&self) -> usize {
        self.sleeps.lock().unwrap().len()
    }

    /// End every sleep someone still waits on.
    fn expire(&self) {
        let mut sleeps = self.sleeps.lock().unwrap();
        for (_, wake) in sleeps.drain(..) {
            if !wake.is_canceled() {
                wake.send(()).expect("the sleep's waiter is alive");
            }
        }
    }
}

fn session() -> SessionId {
    Uuid::from_u128(1)
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

/// One transaction of the laws' writes; it must change a row.
async fn commit(store: &MemoryPartStore, writes: Vec<PartWrite>) -> Rev {
    let mut tx = store.begin();
    for write in writes {
        laws::stage(store, &mut tx, write).await.unwrap();
    }
    tx.commit().expect("the writes change a row")
}

struct Fixture {
    store: MemoryPartStore,
    server: ReplayServer,
    timer: ManualTimer,
    responder: Responder<MemoryPartStore, ManualTimer>,
    now: Instant,
}

impl Fixture {
    fn new() -> Self {
        // A frozen clock: the holds' sleeps are exactly the durations asked.
        let now = Instant::now();
        let store = MemoryPartStore::with_clock(Arc::new(move || now));
        let timer = ManualTimer::default();
        Self {
            responder: Responder::new(store.clone(), timer.clone()),
            store,
            server: default(),
            timer,
            now: Instant::now(),
        }
    }

    fn commit(&self, writes: Vec<PartWrite>) -> Rev {
        block_on(commit(&self.store, writes))
    }

    /// Admit and apply an update of `principal(0)`'s on `session()`, as a server would.
    fn update(
        &mut self,
        generation: u64,
        additions: &[(TargetId, Route)],
    ) -> Result<SessionUpdated, SessionError> {
        let update = UpdateSession {
            session_id: session(),
            generation,
            additions: additions.to_vec(),
            removals: default(),
        };
        let admits = block_on(self.responder.admission(&principal(0), &update)).unwrap();
        self.server.update(&principal(0), &update, self.now, admits)
    }

    fn open(&mut self, targets: &[(TargetId, Route)]) {
        assert_eq!(
            self.update(0, targets),
            Ok(SessionUpdated {
                generation: 0,
                refused: default(),
            })
        );
    }

    /// The round's future, for the tests that poll a hold.
    fn page_future<'a>(
        &'a mut self,
        targets: &[(TargetId, Rev)],
        hold_ms: u32,
    ) -> impl Future<Output = SessionPage> + use<'a> {
        let request = PageSession {
            session_id: session(),
            targets: targets.to_vec(),
            limit: 100,
            hold_ms,
            linger_ms: None,
        };
        let Self {
            server,
            responder,
            now,
            ..
        } = self;
        async move {
            responder
                .page(&principal(0), |round| {
                    server.resolve_round(&principal(0), &request, *now, round)
                })
                .await
                .unwrap()
                .unwrap()
                .clone()
        }
    }

    fn page(&mut self, targets: &[(TargetId, Rev)]) -> SessionPage {
        block_on(self.page_future(targets, 0))
    }
}

#[test]
fn a_round_carries_rows_with_their_payloads() {
    let mut fixture = Fixture::new();
    let first = fixture.commit(vec![
        grant(&[(1, 0), (2, 0)]),
        set(&[(1, 10)]),
        add(&[(1, 1)]),
    ]);
    fixture.open(&[(1, part_route(1)), (2, part_route(2))]);
    // Object 1 joins part 2 later: one payload, events at two revs.
    let second = fixture.commit(vec![add(&[(1, 2)])]);
    let page = fixture.page(&[(1, 0), (2, 0)]);
    assert_eq!(
        page.events,
        [changed(first, 1, &[1], 10), changed(second, 1, &[2], 10)]
    );
    assert_eq!(
        page.verdicts,
        [(1, events(second, true)), (2, events(second, true))]
    );

    let gone = fixture.commit(vec![remove(&[(1, 1)])]);
    let page = fixture.page(&[(1, second), (2, second)]);
    assert_eq!(page.events, [removed(gone, 1, 1)]);
}

/// A round whose cursors are all at the head reads the empty window: verdicts, no rows, and the
/// cursors back as `resume`, drained. A cursor past the head (a store that lost revs) reads the
/// empty window too and gets its own cursor back.
#[test]
fn a_caught_up_round_answers_verdicts_only() {
    let mut fixture = Fixture::new();
    fixture.commit(vec![
        grant(&[(1, 0), (2, 0)]),
        set(&[(1, 10)]),
        add(&[(1, 1), (1, 2)]),
    ]);
    fixture.open(&[(1, part_route(1)), (2, part_route(2))]);
    let head = fixture.commit(vec![revoke(&[(2, 0)])]);
    let page = fixture.page(&[(1, head), (2, head)]);
    assert_eq!(page.events, []);
    assert_eq!(
        page.verdicts,
        [(1, events(head, true)), (2, Verdict::Refused)]
    );

    let page = fixture.page(&[(1, head + 5), (2, head + 5)]);
    assert_eq!(page.events, []);
    assert_eq!(
        page.verdicts,
        [(1, events(head + 5, true)), (2, Verdict::Refused)]
    );
}

/// A readable part with no members has nothing to carry: its round is drained at the head,
/// so a client catches up on it without a row ever arriving (shrunk from the stack simulation).
#[test]
fn an_empty_readable_part_is_drained_at_the_head() {
    let mut fixture = Fixture::new();
    let head = fixture.commit(vec![grant(&[(1, 0)])]);
    fixture.open(&[(1, part_route(1))]);
    let page = fixture.page(&[(1, 0)]);
    assert_eq!(page.events, []);
    assert_eq!(page.verdicts, [(1, events(head, true))]);
}

/// Law 13: a held round with nothing to carry waits. A commit on a route it does not name wakes
/// it to read again and wait on; a commit on its route answers it, with the hold's sleep gone.
#[test]
fn a_hold_is_answered_by_a_commit_on_its_routes() {
    let mut fixture = Fixture::new();
    let head = fixture.commit(vec![grant(&[(1, 0), (2, 0)])]);
    fixture.open(&[(1, part_route(1))]);
    let store = fixture.store.clone();
    let timer = fixture.timer.clone();
    block_on(async {
        let mut held = pin!(fixture.page_future(&[(1, head)], 60_000));
        assert!(futures::poll!(held.as_mut()).is_pending());
        assert_eq!(timer.outstanding(), [MAX_HOLD], "the hold is capped");

        commit(&store, vec![set(&[(2, 20)]), add(&[(2, 2)])]).await;
        assert!(
            futures::poll!(held.as_mut()).is_pending(),
            "a commit off the round's routes does not answer it"
        );
        assert_eq!(
            timer.handed_out(),
            1,
            "one deadline for the hold, not a sleep per wake"
        );

        let written = commit(&store, vec![set(&[(1, 10)]), add(&[(1, 1)])]).await;
        let page = held.await;
        assert_eq!(page.events, [changed(written, 1, &[1], 10)]);
        assert_eq!(page.verdicts, [(1, events(written, true))]);
    });
    assert!(
        timer.outstanding().is_empty(),
        "the answered hold dropped its sleep"
    );
}

/// Law 13 with law 7: a hold that runs out answers as of its end. A revocation committed during
/// the hold wakes it, carries nothing, and the answer refuses the target.
#[test]
fn a_hold_that_expires_answers_as_of_its_end() {
    let mut fixture = Fixture::new();
    let head = fixture.commit(vec![grant(&[(1, 0), (2, 0)])]);
    fixture.open(&[(1, part_route(1)), (2, part_route(2))]);
    let store = fixture.store.clone();
    let timer = fixture.timer.clone();
    block_on(async {
        let mut held = pin!(fixture.page_future(&[(1, head), (2, head)], 1_000));
        assert!(futures::poll!(held.as_mut()).is_pending());
        assert_eq!(timer.outstanding(), [Duration::from_secs(1)]);
        let revoked = commit(&store, vec![revoke(&[(2, 0)])]).await;
        assert!(futures::poll!(held.as_mut()).is_pending());

        timer.expire();
        let page = held.await;
        assert_eq!(page.events, []);
        assert_eq!(
            page.verdicts,
            [(1, events(revoked, true)), (2, Verdict::Refused)]
        );
    });
}

/// Cancelling a round is dropping its future: the hold's sleep and its wait go with it, the
/// server holds nothing for the round, and the next round is answered as if the dropped one
/// had never been asked (law 9).
#[test]
fn dropping_a_held_round_leaves_nothing_behind() {
    let mut fixture = Fixture::new();
    let head = fixture.commit(vec![grant(&[(1, 0)])]);
    fixture.open(&[(1, part_route(1))]);
    let held_before = fixture
        .server
        .session(&principal(0), &session())
        .map(|session| (session.generation, session.targets.clone()));
    {
        let mut held = pin!(fixture.page_future(&[(1, head)], 5_000));
        assert!(block_on(async { futures::poll!(held.as_mut()) }).is_pending());
    }
    assert!(
        fixture.timer.outstanding().is_empty(),
        "the sleep was dropped"
    );
    assert_eq!(
        fixture
            .server
            .session(&principal(0), &session())
            .map(|session| (session.generation, session.targets.clone())),
        held_before
    );

    let written = fixture.commit(vec![set(&[(1, 10)]), add(&[(1, 1)])]);
    let page = fixture.page(&[(1, head)]);
    assert_eq!(page.events, [changed(written, 1, &[1], 10)]);
    assert_eq!(page.verdicts, [(1, events(written, true))]);
}

/// Law 7: a revocation between two rounds refuses the target in the second, which carries none
/// of the rows the client had not read, though they were written while it had access.
#[test]
fn a_revocation_between_rounds_refuses_the_unread_backlog() {
    let mut fixture = Fixture::new();
    let first = fixture.commit(vec![grant(&[(1, 0)]), set(&[(1, 10)]), add(&[(1, 1)])]);
    fixture.open(&[(1, part_route(1))]);
    let page = fixture.page(&[(1, 0)]);
    assert_eq!(page.events, [changed(first, 1, &[1], 10)]);
    fixture.commit(vec![set(&[(2, 20)]), add(&[(2, 1)])]);
    fixture.commit(vec![revoke(&[(1, 0)])]);
    let page = fixture.page(&[(1, first)]);
    assert_eq!(page.events, []);
    assert_eq!(page.verdicts, [(1, Verdict::Refused)]);
}

/// A refused target beside a served one: the served target's rows come through, and no event
/// names the refused one.
#[test]
fn a_refused_target_beside_a_served_one() {
    let mut fixture = Fixture::new();
    let rev = fixture.commit(vec![
        grant(&[(1, 0), (2, 0)]),
        set(&[(1, 10), (2, 20)]),
        add(&[(1, 1), (2, 2), (1, 2)]),
    ]);
    fixture.open(&[(1, part_route(1)), (2, part_route(2))]);
    let revoked = fixture.commit(vec![revoke(&[(2, 0)])]);
    let page = fixture.page(&[(1, 0), (2, 0)]);
    assert_eq!(page.events, [changed(rev, 1, &[1], 10)]);
    assert_eq!(
        page.verdicts,
        [(1, events(revoked, true)), (2, Verdict::Refused)]
    );
}

/// Law 12: an object target carries the object's changes in the parts the client may read and
/// none of its removals, and is refused once the object lives in no readable part.
#[test]
fn object_targets_carry_content_in_readable_parts() {
    let mut fixture = Fixture::new();
    let rev = fixture.commit(vec![
        grant(&[(1, 0)]),
        set(&[(1, 10)]),
        add(&[(1, 1), (1, 2)]),
    ]);
    fixture.open(&[(1, obj_route(1))]);
    let page = fixture.page(&[(1, 0)]);
    assert_eq!(page.events, [changed(rev, 1, &[1], 10)]);
    assert_eq!(page.verdicts, [(1, events(rev, true))]);

    // A change in a part the client may not read carries nothing, and is no news.
    let hidden = fixture.commit(vec![remove(&[(1, 2)])]);
    let page = fixture.page(&[(1, rev)]);
    assert_eq!(page.events, []);
    assert_eq!(page.verdicts, [(1, events(hidden, true))]);

    let edited = fixture.commit(vec![set(&[(1, 11)])]);
    let page = fixture.page(&[(1, hidden)]);
    assert_eq!(page.events, [changed(edited, 1, &[1], 11)]);

    fixture.commit(vec![remove(&[(1, 1)])]);
    let page = fixture.page(&[(1, edited)]);
    assert_eq!(page.events, []);
    assert_eq!(page.verdicts, [(1, Verdict::Refused)]);
}

/// Law 4 from the store: a known readable part and an object live in one are admitted; an
/// unreadable part, an unknown part, and objects unknown or only in unreadable parts are
/// refused alike.
#[test]
fn admission_follows_the_store() {
    let mut fixture = Fixture::new();
    fixture.commit(vec![
        grant(&[(1, 0), (2, 1)]),
        set(&[(1, 10), (2, 20)]),
        add(&[(1, 1), (2, 2)]),
    ]);
    let additions = [
        (1, part_route(1)),
        (2, part_route(2)),
        (3, part_route(9)),
        (4, obj_route(1)),
        (5, obj_route(2)),
        (6, obj_route(9)),
    ];
    assert_eq!(
        fixture.update(0, &additions),
        Ok(SessionUpdated {
            generation: 0,
            refused: vec![2, 3, 5, 6],
        })
    );
    // Admission is decided at update time: a grant later admits a fresh id for the route.
    fixture.commit(vec![grant(&[(2, 0)])]);
    assert_eq!(
        fixture.update(1, &[(7, part_route(2)), (8, obj_route(2))]),
        Ok(SessionUpdated {
            generation: 1,
            refused: default(),
        })
    );
}

/// The server's errors are the round's answer, and nothing is read for them.
#[test]
fn a_round_the_server_rejects_is_answered_with_its_error() {
    let mut fixture = Fixture::new();
    fixture.commit(vec![grant(&[(1, 0)])]);
    fixture.open(&[(1, part_route(1))]);
    let request = PageSession {
        session_id: session(),
        targets: vec![(7, 0)],
        limit: 100,
        hold_ms: 1_000,
        linger_ms: None,
    };
    let Fixture {
        server,
        responder,
        now,
        timer,
        ..
    } = &mut fixture;
    let answer = block_on(responder.page(&principal(0), |round| {
        server.resolve_round(&principal(0), &request, *now, round)
    }))
    .unwrap()
    .cloned();
    assert_eq!(answer, Err(SessionError::UnknownTarget { target: 7 }));
    assert!(timer.outstanding().is_empty());
}
