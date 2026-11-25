//! An in-process bucket walk: a requester's [`BucketMachine`]s against a responder's store.
//!
//! Both peers are memory part stores reached only through the part store traits. The
//! [`Responder`] answers the bucket RPCs from its store through [`PartStoreServe`], gating every
//! answer on the requester's read verdict and counting what it hands out per part. The
//! [`Driver`] plays the requester's tasks: the strategy decision's first listing, then each
//! machine command in order, with the machine's own `filter_buckets`/`filter_objects` between the
//! wire and the machine. An object sync copies the object's payload and its membership in
//! the walked part from responder to requester.

use std::collections::VecDeque;

use proptest::prelude::*;

use crate::fingerprint::FingerprintSeed;
use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::LEAF_PAGE_BYTE_BUDGET;
use crate::part_store::LeafEntry;
use crate::part_store::ObjPayload;
use crate::part_store::ObjState;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::PayloadDigest;
use crate::part_store::bucket_summary::BucketSummary;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::memory::MemoryPartTx;
use crate::rev::Rev;
use crate::sync::bucket::BucketMachine;
use crate::sync::bucket::BucketMachineCommand;
use crate::sync::bucket::FilteredBuckets;
use crate::sync::bucket::calc_working_level;
use crate::sync::bucket::filter_buckets;
use crate::sync::bucket::filter_objects;
use crate::sync::bucket::summaries_agree;
use crate::sync::rpc::bucket::BucketsError;
use crate::sync::rpc::bucket::GetChangedBucketsRequest;
use crate::sync::rpc::bucket::LeafBucketsRequest;
use crate::sync::rpc::bucket::LeafBucketsResponse;
use crate::sync::rpc::responder::RpcResponder;
use crate::sync::rpc::summary::PartSummariesRequest;

/// Each side's store as the harness reaches it: through the part store traits, plus the memory
/// store's own `begin`.
pub trait Store: PartStoreWrite<Tx = MemoryPartTx> + PartStoreRead + PartStoreServe {
    fn begin_tx(&self) -> MemoryPartTx;
}

impl Store for MemoryPartStore {
    fn begin_tx(&self) -> MemoryPartTx {
        self.begin()
    }
}

/// The requester's cursor on the responder: a fresh peer has seen nothing.
pub const SINCE: Rev = 0;
/// Machine commands one walk may take before it is declared not to terminate. Far above what
/// any generated walk needs: each object costs at most a sync and a share of a leaf page.
const STEP_CAP: usize = 100_000;
/// Listings one [`list_dirty`] may make before it is declared not to terminate.
const LIST_CAP: usize = 256;

pub fn obj(index: u16) -> ObjKey {
    ObjKey::new(&index.to_be_bytes())
}
pub fn part(index: u8) -> PartKey {
    PartKey::new(&[100 + index])
}
/// The index [`part`] built `part` from.
pub fn part_index(part: &PartKey) -> u8 {
    part.as_bytes()[0] - 100
}
pub fn requester_principal() -> PeerKey {
    PeerKey::new(&[200])
}
/// Who the checks read a store as: granted every part the store's objects were added to.
fn inspector() -> PeerKey {
    PeerKey::new(&[201])
}
pub fn payload(value: u8) -> ObjPayload {
    serde_json::json!(value)
}

/// An object's membership in one part, as a store ends up holding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Member {
    Absent,
    Live,
    /// Added, then removed: a tombstone, when the object had a payload; nothing when the
    /// membership was still pending.
    Dead,
}

/// One object of a generated store: object `i` of the spec is [`obj(i)`](obj).
#[derive(Debug, Clone)]
pub struct ObjSpec {
    /// `None` leaves every membership pending.
    pub payload: Option<ObjPayload>,
    /// Indexed by part: entry `p` is the membership in [`part(p)`](part).
    pub parts: Vec<Member>,
}

/// Write `objs` into `store` (payloads and adds, then the removals in a second transaction so
/// they leave tombstones) and grant the requester `readable`.
pub async fn build<S: Store>(store: &S, objs: &[ObjSpec], readable: &[PartKey]) -> Res<()> {
    let index = |at: usize| u16::try_from(at).expect("specs stay under u16::MAX objects");
    let keyed = objs
        .iter()
        .enumerate()
        .map(|(at, spec)| (obj(index(at)), spec.clone()))
        .collect::<Vec<_>>();
    build_keyed(store, &keyed, readable).await
}

/// [`build`] over objects the caller names, for keys that must land in chosen buckets.
pub async fn build_keyed<S: Store>(
    store: &S,
    objs: &[(ObjKey, ObjSpec)],
    readable: &[PartKey],
) -> Res<()> {
    let members_where = |kinds: &[Member]| {
        objs.iter()
            .flat_map(|(key, spec)| {
                spec.parts
                    .iter()
                    .enumerate()
                    .filter(|(_, member)| kinds.contains(member))
                    .map(move |(p, _)| {
                        (key.clone(), part(u8::try_from(p).expect(ERROR_IMPOSSIBLE)))
                    })
            })
            .collect::<Vec<_>>()
    };
    let mut tx = store.begin_tx();
    store
        .set_payloads(
            &mut tx,
            objs.iter()
                .filter_map(|(key, spec)| Some((key.clone(), spec.payload.clone()?)))
                .collect(),
        )
        .await?;
    store
        .add_members(&mut tx, &members_where(&[Member::Live, Member::Dead]))
        .await?;
    // The inspector reads every part an object was added to, so the checks can see each
    // store whole through the same authorized reads; it adds no part a store did not have.
    let added = members_where(&[Member::Live, Member::Dead])
        .into_iter()
        .map(|(_, part)| part)
        .collect::<BTreeSet<_>>();
    let grants = readable
        .iter()
        .map(|part| (part.clone(), requester_principal(), AccessLevel::Read))
        .chain(
            added
                .into_iter()
                .map(|part| (part, inspector(), AccessLevel::Read)),
        )
        .collect::<Vec<_>>();
    store.grant_access(&mut tx, &grants).await?;
    tx.commit();
    let mut tx = store.begin_tx();
    store
        .remove_members(&mut tx, &members_where(&[Member::Dead]))
        .await?;
    tx.commit();
    Ok(())
}

/// What the responder handed out on one part, requests and contents.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Traffic {
    pub summary_requests: u64,
    pub list_requests: u64,
    pub buckets_out: u64,
    pub leaf_requests: u64,
    pub leaf_entries_out: u64,
    pub fetches: u64,
}

/// The responder's side of the bucket RPCs: the production [`RpcResponder`] for one principal,
/// counting what it hands out per part.
pub struct Responder<S> {
    rpc: RpcResponder<S>,
    principal: PeerKey,
    pub traffic: BTreeMap<PartKey, Traffic>,
}

impl<S: Store> Responder<S> {
    pub fn new(store: S, leaf_cap: u32) -> Self {
        Self {
            rpc: RpcResponder::new(store).with_leaf_cap(leaf_cap),
            principal: requester_principal(),
            traffic: default(),
        }
    }

    /// The responder stopping a leaf page at `leaf_byte_budget` encoded bytes.
    pub fn with_leaf_byte_budget(mut self, leaf_byte_budget: usize) -> Self {
        self.rpc = self.rpc.with_leaf_byte_budget(leaf_byte_budget);
        self
    }

    pub fn store(&self) -> &S {
        self.rpc.store()
    }

    /// The part's live member count, or `None` for a part the requester may not read or the
    /// store does not know: both answer as unknown, so a refusal does not confirm the part.
    pub async fn part_summary(&mut self, part: &PartKey) -> Res<Option<u64>> {
        self.traffic
            .entry(part.clone())
            .or_default()
            .summary_requests += 1;
        let mut answer = self
            .rpc
            .part_summaries(
                &self.principal,
                PartSummariesRequest {
                    parts: Map::from([(part.clone(), SINCE)]),
                },
            )
            .await?
            .expect("one part is under the cap");
        Ok(answer.remove(part).map(|summary| summary.member_count))
    }

    pub async fn changed_buckets(
        &mut self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, BucketsError>> {
        self.traffic
            .entry(req.part_key.clone())
            .or_default()
            .list_requests += 1;
        let part_key = req.part_key.clone();
        let answer = self.rpc.changed_buckets(&self.principal, req).await?;
        if let Ok(listed) = &answer {
            self.traffic.entry(part_key).or_default().buckets_out += listed.len() as u64;
        }
        Ok(answer)
    }

    pub async fn leaf_buckets(
        &mut self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketsResponse, BucketsError>> {
        let part_key = req.part_key.clone();
        let traffic = self.traffic.entry(part_key).or_default();
        traffic.leaf_requests += 1;
        let answer = self.rpc.leaf_buckets(&self.principal, req).await?;
        if let Ok(pages) = &answer {
            traffic.leaf_entries_out += pages
                .values()
                .map(|page| page.entries.len() as u64)
                .sum::<u64>();
        }
        Ok(answer)
    }

    /// The object's payload, for a sync of its membership in `part`. The machine only
    /// asks for objects it found in a readable part's leaves, so anything else is a harness or
    /// machine bug.
    pub async fn fetch(&mut self, obj: &ObjKey, part: &PartKey) -> Res<ObjPayload> {
        self.traffic.entry(part.clone()).or_default().fetches += 1;
        let state = self
            .rpc
            .store()
            .readable_obj_states(&self.principal, std::slice::from_ref(obj))
            .await?
            .remove(0)
            .filter(|state| state.parts.contains(part))
            .unwrap_or_else(|| {
                panic!("the machine asked to sync {obj} in {part}, which the requester may not read there")
            });
        let (payload, _) = state.content.expect("a readable object has content");
        Ok(payload)
    }
}

/// The listing loop of the strategy decision and of a `ListBuckets` command: list, filter
/// against the local summaries, relist past a page with nothing dirty at the working
/// level. `None` when the responder refuses the part; otherwise the dirty buckets at the
/// working level, empty when the listing is done.
pub async fn list_dirty<S: Store>(
    responder: &mut Responder<S>,
    requester: &S,
    part: &PartKey,
    mut offset: BuckId,
    working_level: BuckLevel,
) -> Res<Option<Vec<BucketSummary>>> {
    // A listing past a page relists at most once per page of the part's buckets; far above what
    // any part here needs.
    for _ in 0..LIST_CAP {
        let listed = responder
            .changed_buckets(GetChangedBucketsRequest {
                part_key: part.clone(),
                offset,
                to_level: working_level,
                since: SINCE,
                limit_hint: BucketMachine::GET_BUCKET_LIMIT_HINT,
            })
            .await?;
        let Ok(listed) = listed else {
            return Ok(None);
        };
        match filter_buckets(part.clone(), working_level, listed, requester).await? {
            FilteredBuckets::Relist(next) => offset = next,
            FilteredBuckets::Done => return Ok(Some(default())),
            FilteredBuckets::Handoff(dirty) => return Ok(Some(dirty)),
        }
    }
    panic!("the listing of {part} from {offset:?} did not terminate");
}

/// How the requester's walk of one part went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Walk {
    /// The responder answered the part as unknown.
    Refused,
    /// The strategy decision's listing found nothing to walk: no machine was built.
    Clean,
    /// A machine walked the part at this working level to `walk_done`.
    Walked { working_level: BuckLevel },
}

/// What the requester did to its own store, per part.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub syncs: u64,
    pub removes: u64,
}

/// The requester: its store, the responder it pulls from, and every part's outcome.
pub struct Driver<S> {
    pub requester: S,
    pub responder: Responder<S>,
    seeds: u64,
    /// What the requester's jobs admit per part at once: the budget of each pull, less the
    /// object deltas of the part still queued.
    pub room: usize,
    pub walks: BTreeMap<PartKey, Walk>,
    pub applied: BTreeMap<PartKey, Applied>,
    /// The level every part is walked at, when not the one its size picks.
    pub working_level: Option<BuckLevel>,
}

impl<S: Store> Driver<S> {
    pub fn new(requester: S, responder: Responder<S>) -> Self {
        Self {
            requester,
            responder,
            seeds: 0,
            room: 8,
            walks: default(),
            applied: default(),
            working_level: None,
        }
    }

    /// See [`list_dirty`].
    async fn list(
        &mut self,
        part: &PartKey,
        offset: BuckId,
        working_level: BuckLevel,
    ) -> Res<Option<Vec<BucketSummary>>> {
        list_dirty(
            &mut self.responder,
            &self.requester,
            part,
            offset,
            working_level,
        )
        .await
    }

    /// Walk every part to quiescence, the machines' commands interleaved in one queue in the
    /// order they were issued. Panics when a walk does not terminate, or stalls with a machine
    /// that never finished.
    pub async fn run(&mut self, parts: &[PartKey]) -> Res<()> {
        let mut machines = BTreeMap::<PartKey, BucketMachine>::new();
        let mut queue = VecDeque::<(PartKey, BucketMachineCommand)>::new();
        let mut out = Vec::new();
        for part in parts {
            let Some(remote_size) = self.responder.part_summary(part).await? else {
                self.walks.insert(part.clone(), Walk::Refused);
                continue;
            };
            let working_level = self
                .working_level
                .unwrap_or_else(|| calc_working_level(remote_size, BuckId::MAX_LEVEL));
            let dirty = self
                .list(part, BuckId::ROOT, working_level)
                .await?
                .expect("the part was readable a moment ago");
            if dirty.is_empty() {
                self.walks.insert(part.clone(), Walk::Clean);
                continue;
            }
            let mut machine = BucketMachine::with_limits(
                part.clone(),
                working_level,
                remote_size,
                SINCE,
                BucketMachine::BUFFER_TARGET as usize,
                BucketMachine::LEAF_BUCKET_LIMIT_HINT as usize,
            );
            machine.on_bucket_page(dirty, &mut out);
            machine.pull(self.room, &mut out);
            queue.extend(out.drain(..).map(|cmd| (part.clone(), cmd)));
            machines.insert(part.clone(), machine);
            self.walks
                .insert(part.clone(), Walk::Walked { working_level });
        }

        let mut steps = 0;
        while let Some((part, cmd)) = queue.pop_front() {
            steps += 1;
            assert!(steps <= STEP_CAP, "the walk did not terminate");
            let machine = machines
                .get_mut(&part)
                .unwrap_or_else(|| panic!("{cmd:?} for {part} after its machine upgraded"));
            match cmd {
                BucketMachineCommand::ListBuckets {
                    part_key,
                    offset,
                    since,
                    working_level,
                } => {
                    assert_eq!((part_key, since), (part.clone(), SINCE));
                    let dirty = self
                        .list(&part, offset, working_level)
                        .await?
                        .expect("the part stays readable");
                    machine.on_bucket_page(dirty, &mut out);
                }
                BucketMachineCommand::LeafBuckets {
                    part_key,
                    since,
                    buckets,
                } => {
                    assert_eq!((part_key, since), (part.clone(), SINCE));
                    let seed = FingerprintSeed::new(self.seeds, 0x5eed);
                    self.seeds += 1;
                    let pages = self
                        .responder
                        .leaf_buckets(LeafBucketsRequest {
                            part_key: part.clone(),
                            since,
                            buckets,
                            seed,
                            limit_hint: Some(BucketMachine::LEAF_BUCKET_LIMIT_HINT),
                        })
                        .await?
                        .expect("the part stays readable");
                    let pages = filter_objects(part.clone(), pages, seed, &self.requester).await?;
                    machine.on_obj_page(pages, &mut out);
                }
                BucketMachineCommand::SyncObj {
                    obj_key,
                    remote_payload,
                    part_key,
                } => {
                    assert_eq!(part_key, part);
                    assert!(
                        remote_payload.is_none(),
                        "the machine does not carry payloads"
                    );
                    let payload = self.responder.fetch(&obj_key, &part).await?;
                    let mut tx = self.requester.begin_tx();
                    self.requester
                        .set_payloads(&mut tx, vec![(obj_key.clone(), payload)])
                        .await?;
                    self.requester
                        .add_members(&mut tx, &[(obj_key.clone(), part.clone())])
                        .await?;
                    tx.commit();
                    self.applied.entry(part.clone()).or_default().syncs += 1;
                }
                BucketMachineCommand::RemoveObjFromParts { obj_key, part_key } => {
                    assert_eq!(part_key, part);
                    let mut tx = self.requester.begin_tx();
                    self.requester
                        .remove_members(&mut tx, &[(obj_key, part.clone())])
                        .await?;
                    tx.commit();
                    self.applied.entry(part.clone()).or_default().removes += 1;
                }
            }
            // The caller's side of the contract: pull what the room allows, and upgrade the
            // part once the walk is done and none of its jobs is outstanding.
            let outstanding = queue
                .iter()
                .filter(|(queued, cmd)| {
                    *queued == part
                        && matches!(
                            cmd,
                            BucketMachineCommand::SyncObj { .. }
                                | BucketMachineCommand::RemoveObjFromParts { .. }
                        )
                })
                .count();
            // The command just applied is no longer outstanding; the ones queued still are.
            machine.pull(self.room.saturating_sub(outstanding), &mut out);
            queue.extend(out.drain(..).map(|cmd| (part.clone(), cmd)));
            if machine.walk_done()
                && !queue.iter().any(|(queued, cmd)| {
                    *queued == part
                        && matches!(
                            cmd,
                            BucketMachineCommand::SyncObj { .. }
                                | BucketMachineCommand::RemoveObjFromParts { .. }
                        )
                })
            {
                machines.remove(&part);
            }
        }
        assert!(
            machines.is_empty(),
            "walks stalled with nothing queued and no upgrade: {:?}",
            machines.keys().collect::<Vec<_>>()
        );
        Ok(())
    }
}

/// Where a part stands between the two stores before a walk: what the walk may have to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Divergence {
    /// The roots agree, so the strategy decision's one listing settles the part.
    pub root_agrees: bool,
    /// The responder's root bucket length: every leaf entry a walk at level 0 may read.
    pub responder_len: u64,
    /// The responder's live members the requester does not hold live there with the same
    /// digest: the most syncs the walk may make.
    pub live_differing: u64,
    /// The responder's dead members: the most removals. A bound over all of them, not only
    /// those the requester is a member for now: a pending membership is one too.
    pub dead: u64,
}

/// A store's members of `part` with their digests (`None` when dead), as the inspector reads
/// them; none for a part no object was added to.
pub async fn leaves<S: Store>(store: &S, part: &PartKey) -> Res<Vec<LeafEntry>> {
    Ok(store
        .bucket_leaves(
            &inspector(),
            part,
            &[(BuckId::ROOT, None)],
            0,
            usize::MAX,
            usize::MAX,
        )
        .await?
        .map(|mut leaves| leaves.remove(0).entries)
        .unwrap_or_default())
}

/// Whether the requester holds `obj` live in `part` with `digest`.
pub fn holds(state: Option<&ObjState>, part: &PartKey, digest: PayloadDigest) -> bool {
    state.is_some_and(|state| {
        state
            .content
            .as_ref()
            .is_some_and(|(_, held)| *held == digest)
            && state.parts.contains(part)
    })
}

pub async fn divergence<S: Store>(requester: &S, responder: &S, part: &PartKey) -> Res<Divergence> {
    let root = [BuckId::ROOT];
    let ours = requester.bucket_summaries(part, &root).await?.remove(0);
    let theirs = responder.bucket_summaries(part, &root).await?.remove(0);
    let leaves = leaves(responder, part).await?;
    let objs = leaves
        .iter()
        .map(|entry| entry.obj.clone())
        .collect::<Vec<_>>();
    let states = requester.obj_states(&objs).await?;
    let mut live_differing = 0;
    let mut dead = 0;
    for (entry, state) in leaves.iter().zip(&states) {
        match entry.live {
            Some(digest) => live_differing += u64::from(!holds(state.as_ref(), part, digest)),
            None => dead += 1,
        }
    }
    Ok(Divergence {
        root_agrees: summaries_agree(&ours, &theirs),
        responder_len: u64::from(theirs.len),
        live_differing,
        dead,
    })
}

/// The requester's view of `part` matches the responder's: every live member of the
/// responder's is live in the requester's with the same digest, and every dead one is not a
/// member there, live or pending. Members only the requester has stay: the walk is a pull.
pub async fn assert_converged<S: Store>(requester: &S, responder: &S, part: &PartKey) -> Res<()> {
    let leaves = leaves(responder, part).await?;
    let objs = leaves
        .iter()
        .map(|entry| entry.obj.clone())
        .collect::<Vec<_>>();
    let states = requester.obj_states(&objs).await?;
    for (entry, state) in leaves.iter().zip(&states) {
        match entry.live {
            Some(digest) => assert!(
                holds(state.as_ref(), part, digest),
                "{obj} is live in {part} at the responder, the requester holds {state:?}",
                obj = entry.obj,
            ),
            None => assert!(
                state
                    .as_ref()
                    .is_none_or(|state| !state.parts.contains(part)),
                "{obj} was removed from {part} at the responder, the requester holds {state:?}",
                obj = entry.obj,
            ),
        }
    }
    Ok(())
}

/// A store's members of `part`, live or dead, without their digests.
pub async fn membership<S: Store>(store: &S, part: &PartKey) -> Res<BTreeSet<(ObjKey, bool)>> {
    Ok(leaves(store, part)
        .await?
        .into_iter()
        .map(|entry| (entry.obj, entry.live.is_some()))
        .collect())
}

pub const PARTS: u8 = 3;

fn any_payload() -> impl Strategy<Value = Option<ObjPayload>> {
    prop_oneof![
        1 => Just(None),
        4 => (0u8..3).prop_map(|value| Some(payload(value))),
    ]
}

fn any_member() -> impl Strategy<Value = Member> {
    prop_oneof![
        2 => Just(Member::Absent),
        2 => Just(Member::Live),
        1 => Just(Member::Dead),
    ]
}

fn any_obj_spec() -> impl Strategy<Value = ObjSpec> {
    (
        any_payload(),
        proptest::collection::vec(any_member(), usize::from(PARTS)),
    )
        .prop_map(|(payload, parts)| ObjSpec { payload, parts })
}

/// Two peers' generated states and what the requester may read of the responder's.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub responder: Vec<ObjSpec>,
    pub requester: Vec<ObjSpec>,
    /// Indexed by part.
    pub readable: Vec<bool>,
    /// The responder's hard cap on a leaf page.
    pub leaf_cap: u32,
    /// The responder's byte budget for a leaf page.
    pub leaf_bytes: usize,
}

impl Scenario {
    pub fn parts(&self) -> Vec<PartKey> {
        (0..PARTS).map(part).collect()
    }

    /// The most entries a leaf page of this scenario's objects holds: the cap, or what the byte
    /// budget lets through (at least one entry, whatever it costs).
    pub fn page_entries(&self) -> usize {
        let cost = LeafEntry::wire_bytes(obj(0).as_bytes().len());
        usize::try_from(self.leaf_cap)
            .expect(ERROR_IMPOSSIBLE)
            .min((self.leaf_bytes / cost).max(1))
    }

    pub fn readable_parts(&self) -> Vec<PartKey> {
        (0..PARTS)
            .filter(|p| self.readable[usize::from(*p)])
            .map(part)
            .collect()
    }
}

/// Overlapping states: each requester object copies the responder's payload and each
/// membership, or holds its own, independently, so stores come out identical, or divergent in
/// content, membership or both.
pub fn scenarios() -> impl Strategy<Value = Scenario> {
    use proptest::collection::vec;
    let pair = (
        any_obj_spec(),
        any_obj_spec(),
        any::<bool>(),
        vec(any::<bool>(), usize::from(PARTS)),
    )
        .prop_map(|(theirs, own, same_payload, same_parts)| {
            let ours = ObjSpec {
                payload: if same_payload {
                    theirs.payload.clone()
                } else {
                    own.payload
                },
                parts: same_parts
                    .iter()
                    .zip(theirs.parts.iter().zip(&own.parts))
                    .map(|(same, (theirs, own))| if *same { *theirs } else { *own })
                    .collect(),
            };
            (theirs, ours)
        });
    (
        vec(pair, 0..16),
        vec(any::<bool>(), usize::from(PARTS)),
        prop_oneof![
            Just(1u32),
            Just(2),
            Just(BucketMachine::LEAF_BUCKET_LIMIT_HINT)
        ],
        // The entries here cost 14 bytes: one, two or the whole page.
        prop_oneof![Just(1usize), Just(30), Just(LEAF_PAGE_BYTE_BUDGET)],
    )
        .prop_map(|(pairs, readable, leaf_cap, leaf_bytes)| {
            let (responder, requester) = pairs.into_iter().unzip();
            Scenario {
                responder,
                requester,
                readable,
                leaf_cap,
                leaf_bytes,
            }
        })
}
