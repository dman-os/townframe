//! Held rounds and the long-poll hub: a commit wakes, and costs a re-read of, only the rounds
//! whose routes it changed, and a linger gathers a burst into one answer.

use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use futures::task::ArcWake;

use super::*;
use crate::long_poll::LongPollHub;
use crate::part_store::LeafPage;
use crate::part_store::LogInterest;
use crate::part_store::ObjState;
use crate::part_store::PartSummary;
use crate::part_store::bucket_summary::BucketSummary;
use crate::rev::RevWatch;

/// A memory store that counts the authorized reads served from it.
#[derive(Clone)]
struct Counted {
    inner: MemoryPartStore,
    reads: Arc<AtomicUsize>,
}

impl Counted {
    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl PartStoreServe for Counted {
    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send {
        PartStoreServe::latest_rev(&self.inner)
    }
    fn watch(&self) -> RevWatch {
        PartStoreServe::watch(&self.inner)
    }
    fn long_poll(&self) -> LongPollHub<LogInterest> {
        self.inner.long_poll()
    }
    fn generation(&self) -> impl Future<Output = Res<crate::part_store::Generation>> + Send {
        self.inner.generation()
    }
    fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &crate::part_store::LogRoutes,
        after: Rev,
        until: Rev,
        limits: crate::rev::Limits,
        page: &mut AuthorizedPage,
    ) -> impl Future<Output = Res<()>> + Send {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner
            .read_authorized(principal, routes, after, until, limits, page)
    }
    fn part_summaries(
        &self,
        principal: &PeerKey,
        parts: &[(PartKey, Rev)],
    ) -> impl Future<Output = Res<Vec<Option<PartSummary>>>> + Send {
        self.inner.part_summaries(principal, parts)
    }
    fn readable_obj_states(
        &self,
        principal: &PeerKey,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send {
        self.inner.readable_obj_states(principal, objs)
    }
    fn bucket_list(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        from: BuckId,
        to_level: crate::BuckLevel,
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<BucketSummary>>>> + Send {
        self.inner
            .bucket_list(principal, part, from, to_level, since, limit)
    }
    fn bucket_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        bucks: &[(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
        byte_budget: usize,
    ) -> impl Future<Output = Res<Option<Vec<LeafPage>>>> + Send {
        self.inner
            .bucket_leaves(principal, part, bucks, since, limit, byte_budget)
    }
}

/// A waker that counts its wakes.
#[derive(Default)]
struct Wakes(AtomicUsize);

impl ArcWake for Wakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// One client's held round on one part, over a shared store.
struct Held {
    server: ReplayServer,
    responder: Responder<Counted, ManualTimer>,
}

impl Held {
    fn open(store: &Counted, timer: &ManualTimer, linger: Duration, route: Route) -> Self {
        let mut held = Self {
            server: default(),
            responder: Responder::new(store.clone(), timer.clone()).with_linger(linger),
        };
        let update = UpdateSession {
            session_id: session(),
            generation: 0,
            additions: vec![(1, route)],
            removals: default(),
        };
        let admits = block_on(held.responder.admission(&principal(0), &update)).unwrap();
        let opened = held
            .server
            .update(&principal(0), &update, Instant::now(), admits)
            .unwrap();
        assert!(opened.refused.is_empty());
        held
    }

    /// The round: target 1 from `cursor`, a page of `limit` rows, held for a minute.
    fn round(
        &mut self,
        cursor: Rev,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = SessionPage> + '_>> {
        let request = PageSession {
            session_id: session(),
            targets: vec![(1, cursor)],
            limit,
            hold_ms: 60_000,
        };
        let Self { server, responder } = self;
        Box::pin(async move {
            responder
                .page(&principal(0), |round| {
                    server.resolve_round(&principal(0), &request, Instant::now(), round)
                })
                .await
                .unwrap()
                .unwrap()
                .clone()
        })
    }
}

fn store() -> Counted {
    let now = Instant::now();
    Counted {
        inner: MemoryPartStore::with_clock(Arc::new(move || now)),
        reads: default(),
    }
}

fn poll_counted(
    round: &mut Pin<Box<dyn Future<Output = SessionPage> + '_>>,
    wakes: &Arc<Wakes>,
) -> Poll<SessionPage> {
    let waker = futures::task::waker(Arc::clone(wakes));
    round.as_mut().poll(&mut Context::from_waker(&waker))
}

/// `ROUNDS` held rounds on disjoint parts and one commit on one of them: one round wakes,
/// one round re-reads, and the machine looks at one waiter. The others are not woken, not
/// even to find nothing (the herd this replaces re-read every round on every commit).
#[test]
fn held_rounds_on_disjoint_parts_cost_one_reread_per_commit() {
    const ROUNDS: u8 = 24;
    let store = store();
    let timer = ManualTimer::default();
    let head = block_on(commit(
        &store.inner,
        vec![grant(
            &(1..=ROUNDS).map(|part| (part, 0)).collect::<Vec<_>>(),
        )],
    ));
    let mut helds = (1..=ROUNDS)
        .map(|part| Held::open(&store, &timer, Duration::ZERO, part_route(part)))
        .collect::<Vec<_>>();
    let mut rounds = helds
        .iter_mut()
        .map(|held| held.round(head, 100))
        .collect::<Vec<_>>();
    let wakes = (0..ROUNDS)
        .map(|_| Arc::new(Wakes::default()))
        .collect::<Vec<_>>();
    for (round, wakes) in rounds.iter_mut().zip(&wakes) {
        assert!(poll_counted(round, wakes).is_pending());
    }
    assert_eq!(store.reads(), usize::from(ROUNDS), "one read to start each");
    let polls = store.inner.long_poll();
    assert_eq!(polls.parked(), usize::from(ROUNDS));

    let touched = polls.touched();
    let written = block_on(commit(&store.inner, vec![set(&[(1, 10)]), add(&[(1, 7)])]));
    assert_eq!(polls.touched() - touched, 1, "one waiter watches part 7");

    let woken = wakes
        .iter()
        .map(|wakes| wakes.0.load(Ordering::SeqCst))
        .collect::<Vec<_>>();
    // Round 7 (index 6) is the only round the commit woke.
    assert_eq!(
        woken.iter().enumerate().filter(|(_, n)| **n > 0).count(),
        1,
        "wakes per round: {woken:?}"
    );
    assert!(woken[6] > 0);
    for (index, (round, wakes)) in rounds.iter_mut().zip(&wakes).enumerate() {
        let polled = poll_counted(round, wakes);
        if index == 6 {
            let Poll::Ready(page) = polled else {
                panic!("the round on the changed part is not answered");
            };
            assert_eq!(page.events, [changed(written, 1, &[1], 10)]);
        } else {
            assert!(polled.is_pending());
        }
    }
    assert_eq!(
        store.reads(),
        usize::from(ROUNDS) + 1,
        "the commit cost one re-read, not {ROUNDS}"
    );
}

/// Linger: a hold answers a burst of commits as one page, the window fixed from the first.
#[test]
fn a_linger_gathers_a_burst_of_commits_into_one_answer() {
    let store = store();
    let timer = ManualTimer::default();
    let head = block_on(commit(&store.inner, vec![grant(&[(1, 0)])]));
    let mut held = Held::open(&store, &timer, Duration::from_millis(100), part_route(1));
    let mut round = held.round(head, 100);
    let wakes = Arc::new(Wakes::default());
    assert!(poll_counted(&mut round, &wakes).is_pending());
    assert_eq!(store.reads(), 1);

    let first = block_on(commit(&store.inner, vec![set(&[(1, 10)]), add(&[(1, 1)])]));
    // The first commit starts the window: the hold waits it out instead of answering.
    assert!(poll_counted(&mut round, &wakes).is_pending());
    assert_eq!(timer.outstanding(), [Duration::from_millis(100)]);
    block_on(commit(&store.inner, vec![set(&[(2, 20)]), add(&[(2, 1)])]));
    let last = block_on(commit(&store.inner, vec![set(&[(3, 30)]), add(&[(3, 1)])]));
    assert!(poll_counted(&mut round, &wakes).is_pending());
    assert_eq!(store.reads(), 1, "nothing is read while the window runs");

    timer.expire();
    let Poll::Ready(page) = poll_counted(&mut round, &wakes) else {
        panic!("the window ended");
    };
    assert_eq!(
        page.events,
        [
            changed(first, 1, &[1], 10),
            changed(first + 1, 2, &[1], 20),
            changed(last, 3, &[1], 30)
        ]
    );
    assert_eq!(store.reads(), 2, "three commits, one read");
}

/// A full page ends the window at once: the hold has nothing to wait for.
#[test]
fn a_full_page_ends_the_linger_early() {
    let store = store();
    let timer = ManualTimer::default();
    let head = block_on(commit(&store.inner, vec![grant(&[(1, 0)])]));
    let mut held = Held::open(&store, &timer, Duration::from_secs(60), part_route(1));
    let mut round = held.round(head, 2);
    let wakes = Arc::new(Wakes::default());
    assert!(poll_counted(&mut round, &wakes).is_pending());

    block_on(commit(&store.inner, vec![set(&[(1, 10)]), add(&[(1, 1)])]));
    assert!(
        poll_counted(&mut round, &wakes).is_pending(),
        "one row of two"
    );
    block_on(commit(&store.inner, vec![set(&[(2, 20)]), add(&[(2, 1)])]));
    let Poll::Ready(page) = poll_counted(&mut round, &wakes) else {
        panic!("a full page does not wait out the window");
    };
    assert_eq!(page.events.len(), 2);
}

/// An object route is refused once its part is revoked, and a held round must hear of it: a
/// revocation is a row of the part's access, which no object route names.
#[test]
fn a_revocation_wakes_a_held_object_route() {
    let store = store();
    let timer = ManualTimer::default();
    let head = block_on(commit(
        &store.inner,
        vec![grant(&[(1, 0)]), set(&[(1, 10)]), add(&[(1, 1)])],
    ));
    let mut held = Held::open(&store, &timer, Duration::ZERO, obj_route(1));
    let mut round = held.round(head, 100);
    let wakes = Arc::new(Wakes::default());
    assert!(poll_counted(&mut round, &wakes).is_pending());
    assert_eq!(store.reads(), 1);

    let revoked = block_on(commit(&store.inner, vec![revoke(&[(1, 0)])]));
    assert!(poll_counted(&mut round, &wakes).is_pending());
    assert_eq!(store.reads(), 2, "the revocation woke the round to read");

    timer.expire();
    let Poll::Ready(page) = poll_counted(&mut round, &wakes) else {
        panic!("the hold ran out");
    };
    assert_eq!(page.events, []);
    assert_eq!(page.verdicts, [(1, Verdict::Refused)]);
    assert!(revoked > head);
}
