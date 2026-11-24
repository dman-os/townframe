//! The walk machine's laws (see the module docs of `bucket`): hand-driven checks of each law on
//! a bare machine, and a differential proptest against a naive oracle.
//!
//! The oracle is the dumbest answer to "what must the walk emit": read every leaf entry of the
//! responder's part and compare it with the requester's object state, with no buckets, pages or
//! buffers. The proptest builds two memory stores, drives one machine through random pulls,
//! answers and redelivered pages, and compares the deltas it emitted with the oracle's set
//! exactly. The invariants (budget, no duplicate, bounded memory, `walk_done` only after every
//! delta) are checked after every step, not only at the end.

use proptest::prelude::*;

use super::harness;
use super::harness::Member;
use super::harness::ObjSpec;
use super::harness::Responder;
use super::harness::Scenario;
use super::*;
use crate::part_store::PartStoreRead;
use crate::part_store::memory::MemoryPartStore;
use crate::sync::rpc::bucket::LeafBucketsRequest;

/// A delta as the oracle names it: the object, and whether it is a removal.
type Delta = (ObjKey, bool);

fn delta_of(cmd: &BucketMachineCommand) -> Option<Delta> {
    match cmd {
        BucketMachineCommand::SyncObj { obj_key, .. } => Some((obj_key.clone(), false)),
        BucketMachineCommand::RemoveObjFromParts { obj_key, .. } => Some((obj_key.clone(), true)),
        BucketMachineCommand::ListBuckets { .. } | BucketMachineCommand::LeafBuckets { .. } => None,
    }
}

// ---------- hand-driven laws on a bare machine ----------

/// The machine for one root bucket of `len` members, after the walk's first listing page.
fn root_machine(
    len: u32,
    buffer_target: usize,
    leaf_hint: usize,
) -> (BucketMachine, Vec<BucketMachineCommand>) {
    let mut machine = BucketMachine::with_limits(
        harness::part(0),
        0,
        u64::from(len),
        0,
        buffer_target,
        leaf_hint,
    );
    let mut out = vec![];
    machine.on_bucket_page(vec![summary(BuckId::ROOT, len, len, 0)], &mut out);
    (machine, out)
}

/// The root bucket's leaf page of members `range` as new objects; `more` when the bucket
/// continues past it.
fn root_page(range: std::ops::Range<u16>, more: bool) -> Map<BuckId, BucketObjLeafPage> {
    let next_after = more.then(|| harness::obj(range.end - 1));
    let after = range.start.checked_sub(1).map(harness::obj);
    Map::from([(
        BuckId::ROOT,
        BucketObjLeafPage {
            after,
            entries: range
                .map(|at| BucketObjEntry {
                    obj_key: harness::obj(at),
                    delta: PartObjDelta::New,
                })
                .collect(),
            next_after,
        },
    )])
}

/// The leaf requests among `out`, as the position each asks from.
fn leaf_requests(out: &[BucketMachineCommand]) -> Vec<Option<ObjKey>> {
    out.iter()
        .filter_map(|cmd| match cmd {
            BucketMachineCommand::LeafBuckets { buckets, .. } => Some(buckets),
            _ => None,
        })
        .flatten()
        .map(|req| req.after.clone())
        .collect()
}

fn deltas(out: &[BucketMachineCommand]) -> Vec<Delta> {
    out.iter().filter_map(delta_of).collect()
}

/// Law 1 on a bucket with every kind of entry: a sync for `New` and `Change`, a removal for
/// `Delete`, in the order the page listed them, and nothing else.
#[test]
fn law_1_each_entry_becomes_its_delta() {
    let (mut machine, out) = root_machine(4, 8, 8);
    assert_eq!(leaf_requests(&out), vec![None]);
    let entry = |at, delta| BucketObjEntry {
        obj_key: harness::obj(at),
        delta,
    };
    let mut out = vec![];
    machine.on_obj_page(
        Map::from([(
            BuckId::ROOT,
            BucketObjLeafPage {
                after: None,
                entries: vec![
                    entry(0, PartObjDelta::New),
                    entry(1, PartObjDelta::Change),
                    entry(2, PartObjDelta::Delete),
                    entry(3, PartObjDelta::New),
                ],
                next_after: None,
            },
        )]),
        &mut out,
    );
    assert!(out.is_empty(), "a page alone emits no delta");
    machine.pull(usize::MAX, &mut out);
    assert_eq!(
        deltas(&out),
        vec![
            (harness::obj(0), false),
            (harness::obj(1), false),
            (harness::obj(2), true),
            (harness::obj(3), false),
        ]
    );
    assert!(out.iter().all(|cmd| delta_of(cmd).is_some()));
    assert!(machine.walk_done());
}

/// Law 2: a pull emits at most its budget, none at 0, and the rest stay buffered for the next.
#[test]
fn law_2_a_pull_never_exceeds_its_budget() {
    let (mut machine, _) = root_machine(6, 8, 8);
    let mut out = vec![];
    machine.on_obj_page(root_page(0..6, false), &mut out);
    out.clear();

    machine.pull(0, &mut out);
    assert_eq!(deltas(&out), vec![], "budget 0 emits no delta");
    machine.pull(2, &mut out);
    assert_eq!(deltas(&out).len(), 2);
    out.clear();
    machine.pull(1, &mut out);
    assert_eq!(deltas(&out), vec![(harness::obj(2), false)]);
    out.clear();
    machine.pull(100, &mut out);
    assert_eq!(deltas(&out).len(), 3, "the remainder, not more");
    assert!(machine.walk_done());
}

/// Law 3: a leaf page delivered again emits nothing a second time, whether the bucket has moved
/// on to its next request or has no request outstanding.
#[test]
fn law_3_a_redelivered_page_emits_nothing_twice() {
    let (mut machine, _) = root_machine(4, 2, 2);
    let mut out = vec![];
    machine.on_obj_page(root_page(0..2, true), &mut out);
    assert_eq!(leaf_requests(&out), vec![], "buffer at the watermark");
    // Delivered again with no request outstanding.
    machine.on_obj_page(root_page(0..2, true), &mut out);
    assert_eq!(machine.buffered, 2);

    out.clear();
    machine.pull(2, &mut out);
    assert_eq!(leaf_requests(&out), vec![Some(harness::obj(1))]);
    // Delivered again while the next request is outstanding: it is the old page, not the
    // answer.
    machine.on_obj_page(root_page(0..2, true), &mut out);
    assert_eq!(machine.buffered, 0);
    machine.on_obj_page(root_page(2..4, false), &mut out);
    machine.on_obj_page(root_page(2..4, false), &mut out);
    out.clear();
    machine.pull(usize::MAX, &mut out);
    assert_eq!(
        deltas(&out),
        vec![(harness::obj(2), false), (harness::obj(3), false)]
    );
    assert!(machine.walk_done());
}

/// Law 4: `walk_done` is false while any entry is buffered, while a request is outstanding and
/// before the first listing page; it holds only after the last delta was pulled.
#[test]
fn law_4_done_means_every_delta_went_out() {
    let mut machine = BucketMachine::with_limits(harness::part(0), 0, 3, 0, 8, 8);
    assert!(!machine.walk_done(), "before the first listing page");
    let mut out = vec![];
    machine.on_bucket_page(vec![summary(BuckId::ROOT, 3, 3, 0)], &mut out);
    assert!(!machine.walk_done(), "a leaf request is outstanding");
    machine.on_obj_page(root_page(0..3, false), &mut out);
    assert!(!machine.walk_done(), "three entries are buffered");
    out.clear();
    machine.pull(2, &mut out);
    assert!(!machine.walk_done(), "one entry is still buffered");
    machine.pull(1, &mut out);
    assert!(machine.walk_done());
    assert_eq!(deltas(&out).len(), 3);
}

/// Law 4, listing side: a bucket page that is not the last keeps the walk open even with every
/// working bucket drained.
#[test]
fn law_4_done_waits_for_the_listing() {
    // Level 1 so that listing can have a next page.
    let mut machine = BucketMachine::with_limits(harness::part(0), 1, 40, 0, 4, 4);
    let mut out = vec![];
    let buck = BuckId::new(1, 0);
    machine.on_bucket_page(vec![summary(buck, 2, 2, 0)], &mut out);
    assert!(
        out.iter()
            .any(|cmd| matches!(cmd, BucketMachineCommand::ListBuckets { .. })),
        "the working set has room, so the next page is asked for"
    );
    machine.on_obj_page(
        Map::from([(
            buck,
            BucketObjLeafPage {
                after: None,
                entries: vec![],
                next_after: None,
            },
        )]),
        &mut out,
    );
    machine.pull(10, &mut out);
    assert!(!machine.walk_done(), "the listing is not finished");
    machine.on_bucket_page(vec![], &mut out);
    assert!(machine.walk_done());
}

/// Law 5 and 6: with no pull the walk reads ahead only to its target, and then stops asking; it
/// asks again only when the buffer is drained below the low watermark.
#[test]
fn law_6_reads_ahead_to_the_target_and_refills_below_the_watermark() {
    // Target 8, so the watermark is 4; pages of 8.
    let (mut machine, out) = root_machine(40, 8, 8);
    assert_eq!(leaf_requests(&out), vec![None]);
    let mut out = vec![];
    machine.on_obj_page(root_page(0..8, true), &mut out);
    assert_eq!(leaf_requests(&out), vec![], "8 buffered: at the target");

    machine.pull(4, &mut out);
    assert_eq!(
        leaf_requests(&out),
        vec![],
        "4 buffered: not below the watermark"
    );
    machine.pull(1, &mut out);
    assert_eq!(
        leaf_requests(&out),
        vec![Some(harness::obj(7))],
        "3 buffered: below it"
    );
    out.clear();
    machine.pull(0, &mut out);
    assert_eq!(
        leaf_requests(&out),
        vec![],
        "the request is outstanding already"
    );
    assert!(machine.buffered < 8 + 8);
}

/// Law 7: pages for a bucket the machine does not work on, or one with no request outstanding,
/// change nothing.
#[test]
fn law_7_a_page_for_an_unknown_bucket_is_ignored() {
    let (mut machine, _) = root_machine(4, 8, 8);
    let mut out = vec![];
    let stranger = BuckId::new(1, 3);
    machine.on_obj_page(
        Map::from([(
            stranger,
            BucketObjLeafPage {
                after: None,
                entries: vec![BucketObjEntry {
                    obj_key: harness::obj(9),
                    delta: PartObjDelta::New,
                }],
                next_after: None,
            },
        )]),
        &mut out,
    );
    assert_eq!((machine.buffered, out.len()), (0, 0));
    machine.pull(10, &mut out);
    assert_eq!(deltas(&out), vec![]);
    assert!(!machine.walk_done(), "the root bucket's page is still due");
}

/// Law 8 by hand: one delta at a time, answering what the machine asks for, the walk finishes.
#[test]
fn law_8_a_caller_pulling_one_at_a_time_reaches_done() {
    let (mut machine, mut out) = root_machine(9, 2, 2);
    let mut served = 0u16;
    let mut emitted = vec![];
    for _ in 0..100 {
        if machine.walk_done() {
            break;
        }
        let asked = leaf_requests(&out);
        out.clear();
        for after in asked {
            let from = after.map_or(0, |after| served_after(&after) + 1);
            let to = (from + 2).min(9);
            served = to;
            machine.on_obj_page(root_page(from..to, to < 9), &mut out);
        }
        machine.pull(1, &mut out);
        emitted.extend(deltas(&out));
        // What is left for the next round is the requests.
        out.retain(|cmd| delta_of(cmd).is_none());
    }
    assert!(machine.walk_done());
    assert_eq!(served, 9);
    assert_eq!(
        emitted,
        (0..9)
            .map(|at| (harness::obj(at), false))
            .collect::<Vec<_>>()
    );
}

/// The index of an object `harness::obj` made.
fn served_after(key: &ObjKey) -> u16 {
    u16::from_be_bytes(key.as_bytes().try_into().expect("two bytes"))
}

// ---------- the differential proptest ----------

/// One step of the caller and the network, interleaved at random.
#[derive(Debug, Clone)]
enum Act {
    /// Pull with this budget.
    Pull(usize),
    /// Answer the outstanding request at this position (mod their number).
    Answer(usize),
    /// Deliver an already-delivered leaf answer again.
    Redeliver(usize),
}

fn acts() -> impl Strategy<Value = Vec<Act>> {
    let act = prop_oneof![
        3 => prop_oneof![Just(0usize), Just(1), Just(2), Just(3), Just(100)].prop_map(Act::Pull),
        3 => any::<usize>().prop_map(Act::Answer),
        2 => any::<usize>().prop_map(Act::Redeliver),
    ];
    proptest::collection::vec(act, 0..40)
}

#[derive(Debug, Clone)]
struct WalkCase {
    scenario: Scenario,
    working_level: BuckLevel,
    buffer_target: usize,
    leaf_hint: usize,
    acts: Vec<Act>,
}

fn walk_cases() -> impl Strategy<Value = WalkCase> {
    (
        harness::scenarios(),
        0u8..=1,
        1usize..=8,
        1usize..=4,
        acts(),
    )
        .prop_map(
            |(scenario, working_level, buffer_target, leaf_hint, acts)| WalkCase {
                scenario,
                working_level,
                buffer_target,
                leaf_hint,
                acts,
            },
        )
}

/// The naive oracle: every responder leaf entry against the requester's state, nothing else.
/// A live entry is a sync unless the requester holds it live with the same digest; a dead one
/// is a removal when the requester is a member, live or pending.
async fn oracle(
    requester: &MemoryPartStore,
    responder: &MemoryPartStore,
    part: &PartKey,
) -> Res<BTreeSet<Delta>> {
    let leaves = harness::leaves(responder, part).await?;
    let objs = leaves
        .iter()
        .map(|entry| entry.obj.clone())
        .collect::<Vec<_>>();
    let states = requester.obj_states(&objs).await?;
    Ok(leaves
        .iter()
        .zip(&states)
        .filter_map(|(entry, state)| match entry.live {
            Some(digest) => {
                (!harness::holds(state.as_ref(), part, digest)).then(|| (entry.obj.clone(), false))
            }
            None => state
                .as_ref()
                .is_some_and(|state| state.parts.contains(part))
                .then(|| (entry.obj.clone(), true)),
        })
        .collect())
}

/// One walk under construction: the machine, its network and what it has emitted.
struct Rig {
    machine: BucketMachine,
    responder: Responder<MemoryPartStore>,
    requester: MemoryPartStore,
    part: PartKey,
    working_level: BuckLevel,
    buffer_target: usize,
    leaf_hint: usize,
    seeds: u64,
    out: Vec<BucketMachineCommand>,
    /// List and leaf requests not answered yet.
    outstanding: Vec<BucketMachineCommand>,
    /// Every leaf answer delivered so far, for redelivery.
    delivered: Vec<Map<BuckId, BucketObjLeafPage>>,
    emitted: Vec<Delta>,
    expected: BTreeSet<Delta>,
}

impl Rig {
    /// File the machine's output: deltas to the caller's log, requests to the network.
    fn absorb(&mut self) {
        for cmd in self.out.drain(..) {
            match delta_of(&cmd) {
                Some(delta) => self.emitted.push(delta),
                None => self.outstanding.push(cmd),
            }
        }
    }

    fn pull(&mut self, budget: usize) -> usize {
        let before = self.emitted.len();
        self.machine.pull(budget, &mut self.out);
        self.absorb();
        let pulled = self.emitted.len() - before;
        assert!(
            pulled <= budget,
            "law 2: {pulled} deltas for a budget of {budget}"
        );
        pulled
    }

    async fn answer(&mut self, at: usize) -> Res<()> {
        let cmd = self.outstanding.remove(at % self.outstanding.len());
        match cmd {
            BucketMachineCommand::ListBuckets { offset, .. } => {
                let dirty = harness::list_dirty(
                    &mut self.responder,
                    &self.requester,
                    &self.part,
                    offset,
                    self.working_level,
                )
                .await?
                .expect("the part is readable");
                self.machine.on_bucket_page(dirty, &mut self.out);
            }
            BucketMachineCommand::LeafBuckets { since, buckets, .. } => {
                let seed = FingerprintSeed::new(self.seeds, 0x5eed);
                self.seeds += 1;
                let pages = self
                    .responder
                    .leaf_buckets(LeafBucketsRequest {
                        part_key: self.part.clone(),
                        since,
                        buckets,
                        seed,
                        limit_hint: Some(u32::try_from(self.leaf_hint).expect(ERROR_IMPOSSIBLE)),
                    })
                    .await?
                    .expect("the part is readable");
                let pages = filter_objects(self.part.clone(), pages, seed, &self.requester).await?;
                self.delivered.push(pages.clone());
                self.machine.on_obj_page(pages, &mut self.out);
            }
            other => unreachable!("not a request: {other:?}"),
        }
        self.absorb();
        Ok(())
    }

    fn redeliver(&mut self, at: usize) {
        if self.delivered.is_empty() {
            return;
        }
        let pages = self.delivered[at % self.delivered.len()].clone();
        self.machine.on_obj_page(pages, &mut self.out);
        self.absorb();
    }

    /// The invariants that hold after every step.
    fn check(&self) {
        let emitted = self.emitted.iter().cloned().collect::<BTreeSet<_>>();
        assert_eq!(emitted.len(), self.emitted.len(), "law 3: a delta twice");
        assert!(
            emitted.is_subset(&self.expected),
            "law 1: {:?} emitted without being a difference",
            emitted.difference(&self.expected).collect::<Vec<_>>()
        );
        assert!(
            self.machine.buffered < self.buffer_target + self.leaf_hint,
            "law 5: {} buffered against a target of {} and pages of {}",
            self.machine.buffered,
            self.buffer_target,
            self.leaf_hint
        );
        assert!(
            self.machine.working_buckets.len() <= self.machine.initial_working_set as usize,
            "law 5: {} working buckets against a bound of {}",
            self.machine.working_buckets.len(),
            self.machine.initial_working_set
        );
        assert_eq!(
            self.machine.buffered,
            self.machine
                .working_buckets
                .values()
                .map(|buck| buck.pending_objs.len())
                .sum::<usize>(),
            "the buffered count drifted"
        );
        if self.machine.walk_done() {
            assert_eq!(
                emitted, self.expected,
                "law 4: done before every delta went out"
            );
            assert!(
                self.outstanding.is_empty(),
                "law 4: done with requests outstanding"
            );
        }
    }
}

/// Walk a case: the random steps first, then a drain that answers everything and pulls one at a
/// time until `walk_done`. Returns what the walk emitted and what the oracle expected.
async fn run_case(case: WalkCase) -> Res<(Vec<Delta>, BTreeSet<Delta>)> {
    let part = harness::part(0);
    let responder_store = MemoryPartStore::default();
    harness::build(
        &responder_store,
        &case.scenario.responder,
        std::slice::from_ref(&part),
    )
    .await?;
    let requester = MemoryPartStore::default();
    harness::build(&requester, &case.scenario.requester, &[]).await?;
    let expected = oracle(&requester, &responder_store, &part).await?;
    let mut responder = Responder::new(responder_store, case.scenario.leaf_cap);
    let Some(remote_size) = responder.part_summary(&part).await? else {
        // No member of the part at the responder: nothing to walk, nothing to emit.
        assert_eq!(expected, BTreeSet::new());
        return Ok((vec![], expected));
    };
    let dirty = harness::list_dirty(
        &mut responder,
        &requester,
        &part,
        BuckId::ROOT,
        case.working_level,
    )
    .await?
    .expect("the part is readable");
    if dirty.is_empty() {
        assert_eq!(
            expected,
            BTreeSet::new(),
            "a clean listing hides a difference"
        );
        return Ok((vec![], expected));
    }
    // The page the machine is asked for is no bigger than the responder will give.
    let leaf_hint = case
        .leaf_hint
        .min(usize::try_from(case.scenario.leaf_cap).expect(ERROR_IMPOSSIBLE));
    let mut machine = BucketMachine::with_limits(
        part.clone(),
        case.working_level,
        remote_size,
        harness::SINCE,
        case.buffer_target,
        leaf_hint,
    );
    let mut out = vec![];
    machine.on_bucket_page(dirty, &mut out);
    let mut rig = Rig {
        machine,
        responder,
        requester,
        part,
        working_level: case.working_level,
        buffer_target: case.buffer_target,
        leaf_hint,
        seeds: 0,
        out,
        outstanding: vec![],
        delivered: vec![],
        emitted: vec![],
        expected,
    };
    rig.absorb();
    rig.check();
    for act in case.acts {
        match act {
            Act::Pull(budget) => {
                rig.pull(budget);
            }
            Act::Answer(at) if !rig.outstanding.is_empty() => rig.answer(at).await?,
            Act::Answer(_) => {}
            Act::Redeliver(at) => rig.redeliver(at),
        }
        rig.check();
    }
    // Law 8: answering everything asked and pulling one at a time must finish.
    for _ in 0..10_000 {
        if rig.machine.walk_done() {
            break;
        }
        let asked = rig.outstanding.len();
        if asked > 0 {
            rig.answer(0).await?;
        }
        let pulled = rig.pull(1);
        rig.check();
        assert!(
            asked > 0 || pulled > 0 || rig.machine.walk_done(),
            "law 8: stalled with nothing outstanding and nothing to pull: {:?}",
            rig.machine.working_buckets
        );
    }
    assert!(rig.machine.walk_done(), "law 8: the walk did not finish");
    assert_eq!(
        rig.emitted.iter().cloned().collect::<BTreeSet<_>>(),
        rig.expected
    );
    assert_eq!(rig.emitted.len(), rig.expected.len());
    Ok((rig.emitted, rig.expected))
}

proptest::proptest! {
    #[test]
    fn walk_emits_the_oracle_difference(case in walk_cases()) {
        futures::executor::block_on(run_case(case))
            .map(drop)
            .map_err(|err| proptest::test_runner::TestCaseError::fail(format!("{err:?}")))?;
    }
}

fn spec(payload: u8, member: Member) -> ObjSpec {
    ObjSpec {
        payload: Some(harness::payload(payload)),
        parts: vec![member, Member::Absent, Member::Absent],
    }
}

/// Law 1 end to end on a fixed pair of stores, with the expected set written out: a new
/// object, a changed payload, a removal of a member and a dead entry for an object that was
/// never a member; the last needs nothing.
#[test]
fn law_1_the_difference_of_two_stores_is_emitted_exactly() -> Res<()> {
    let scenario = Scenario {
        responder: vec![
            spec(0, Member::Live), // same on both sides
            spec(0, Member::Live), // requester holds another payload
            spec(0, Member::Live), // requester lacks it
            spec(0, Member::Dead), // requester is still a member
            spec(0, Member::Dead), // requester never was
        ],
        requester: vec![
            spec(0, Member::Live),
            spec(1, Member::Live),
            spec(0, Member::Absent),
            spec(0, Member::Live),
            spec(0, Member::Absent),
        ],
        readable: vec![true; usize::from(harness::PARTS)],
        leaf_cap: 2,
    };
    let (emitted, expected) = futures::executor::block_on(run_case(WalkCase {
        scenario,
        working_level: 0,
        buffer_target: 2,
        leaf_hint: 2,
        acts: vec![],
    }))?;
    let want = BTreeSet::from([
        (harness::obj(1), false),
        (harness::obj(2), false),
        (harness::obj(3), true),
    ]);
    assert_eq!(expected, want);
    assert_eq!(emitted.into_iter().collect::<BTreeSet<_>>(), want);
    Ok(())
}

/// Law 5: the working set binds on its own when buckets are smaller than the width the working
/// level promised: ten buckets of one entry would all fit the target, but not the set.
#[test]
fn law_5_the_working_set_bounds_small_buckets() {
    // Level 1 over 48 members: buckets of 3, so a set of 3 buckets for a target of 8.
    let mut machine = BucketMachine::with_limits(harness::part(0), 1, 48, 0, 8, 8);
    let mut out = vec![];
    machine.on_bucket_page(
        (0..10)
            .map(|at| summary(BuckId::new(1, at), 1, 1, 0))
            .collect(),
        &mut out,
    );
    assert_eq!(machine.initial_working_set, 3);
    assert_eq!(machine.working_buckets.len(), 3);
    assert_eq!(leaf_requests(&out).len(), 3);
}

/// Law 5 by hand: with no pull, the machine admits at most its working set of buckets and
/// reads ahead only to its target, however many buckets and pages are on offer.
#[test]
fn law_5_memory_is_bounded_without_a_single_pull() {
    // Level 1 over 16 members: buckets of 1, so the working set could hold 4 buckets, but the
    // target of 4 entries in pages of 2 asks for two.
    let mut machine = BucketMachine::with_limits(harness::part(0), 1, 16, 0, 4, 2);
    let mut out = vec![];
    let bucks = (0..6).map(|at| BuckId::new(1, at)).collect::<Vec<_>>();
    machine.on_bucket_page(
        bucks.iter().map(|id| summary(*id, 6, 6, 0)).collect(),
        &mut out,
    );
    assert_eq!(machine.initial_working_set, 4);
    assert_eq!(machine.working_buckets.len(), 2);
    assert_eq!(leaf_requests(&out).len(), 2);

    // Answer every request the machine makes, pulling nothing.
    let mut served = 0u16;
    while !out.is_empty() {
        let asked = out
            .drain(..)
            .filter_map(|cmd| match cmd {
                BucketMachineCommand::LeafBuckets { buckets, .. } => Some(buckets),
                _ => None,
            })
            .flatten()
            .collect::<Vec<_>>();
        for req in asked {
            served += 1;
            assert!(served < 100, "the walk reads ahead without end");
            let key = |at: u16| harness::obj(at + 100 * served);
            machine.on_obj_page(
                Map::from([(
                    req.buck_id,
                    BucketObjLeafPage {
                        after: req.after,
                        entries: (0..2)
                            .map(|at| BucketObjEntry {
                                obj_key: key(at),
                                delta: PartObjDelta::New,
                            })
                            .collect(),
                        next_after: Some(key(1)),
                    },
                )]),
                &mut out,
            );
            assert!(machine.buffered < 4 + 2, "{} buffered", machine.buffered);
            assert!(machine.working_buckets.len() <= 4);
        }
    }
    assert!(machine.buffered >= 2, "it did read ahead");
    assert!(!machine.walk_done());
}

/// Found by the `PeerSync` simulation: a member added to a bucket after it was listed displaces
/// an older one onto the page after the `len` entries the listing counted. The walk stopped at
/// `len`, the older member was never synced, and the part upgraded without it, for good (its
/// row is below the handover cursor, so the tail never delivers it). The page's `next_after`,
/// not the listing's `len`, says where the bucket ends.
#[test]
fn law_4_a_bucket_that_grew_since_it_was_listed_is_read_to_the_end_the_peer_says() {
    let (mut machine, _) = root_machine(2, 8, 8);
    let mut out = vec![];
    machine.on_obj_page(root_page(0..2, true), &mut out);
    assert_eq!(
        leaf_requests(&out),
        vec![Some(harness::obj(1))],
        "the bucket was listed with 2 entries and continues past them"
    );
    assert!(!machine.walk_done());
    let mut out = vec![];
    machine.on_obj_page(root_page(2..3, false), &mut out);
    machine.pull(usize::MAX, &mut out);
    assert_eq!(
        deltas(&out).len(),
        3,
        "every entry of both pages is emitted"
    );
    assert!(machine.walk_done());
}

/// Law 9: a selection wider than a responder answers goes out as several requests, none over
/// `MAX_LEAF_BUCKETS`, and together they name every bucket once.
#[test]
fn law_9_a_leaf_request_never_names_more_than_the_responder_cap() {
    // 200 one-member buckets at level 2 and room for all of them at once.
    let mut machine = BucketMachine::with_limits(harness::part(0), 2, 256, 0, 1000, 1);
    let mut out = vec![];
    let listed = (0..200)
        .map(|at| summary(BuckId::new(2, at), 1, 1, 0))
        .collect();
    machine.on_bucket_page(listed, &mut out);
    let requests = out
        .iter()
        .filter_map(|cmd| match cmd {
            BucketMachineCommand::LeafBuckets { buckets, .. } => Some(buckets),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(requests.len() > 1, "{}", requests.len());
    assert!(
        requests
            .iter()
            .all(|buckets| buckets.len() <= MAX_LEAF_BUCKETS)
    );
    let named = requests
        .iter()
        .flat_map(|buckets| buckets.iter().map(|req| req.buck_id))
        .collect::<BTreeSet<_>>();
    assert_eq!(named.len(), 200);
    assert_eq!(
        requests.iter().map(|buckets| buckets.len()).sum::<usize>(),
        200
    );
}
