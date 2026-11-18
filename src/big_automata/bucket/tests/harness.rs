//! An in-process bucket walk: a requester's [`BucketMachine`]s against a responder's store.
//!
//! Both peers are memory part stores reached only through the part store traits. The
//! [`Responder`] answers the bucket RPCs from its store through [`PartStoreServe`], gating every
//! answer on the requester's read verdict and counting what it hands out per part. The
//! [`Driver`] plays the requester's tasks: the strategy decision's first listing, then each
//! machine command in order, with the machine's own `filter_buckets`/`filter_objects` between the
//! wire and the machine. An object sync copies the object's payload state and its membership in
//! the walked part from responder to requester.

use std::collections::VecDeque;

use future_form::Sendable;
use proptest::prelude::*;

use crate::bucket::BucketMachine;
use crate::bucket::BucketMachineCommand;
use crate::bucket::FilteredBuckets;
use crate::bucket::calc_working_level;
use crate::bucket::filter_buckets;
use crate::bucket::filter_objects;
use crate::bucket::summaries_agree;
use crate::fingerprint::Fingerprint;
use crate::fingerprint::FingerprintSeed;
use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::LeafEntry;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::PayloadDigest;
use crate::part_store::PayloadState;
use crate::part_store::ReadVerdict;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::memory::MemoryPartTx;
use crate::rpc::bucket::BucketObjPageEntry;
use crate::rpc::bucket::GetChangedBucketsError;
use crate::rpc::bucket::GetChangedBucketsRequest;
use crate::rpc::bucket::LeafBucketPage;
use crate::rpc::bucket::LeafBucketRequest;
use crate::rpc::bucket::LeafBucketResult;
use crate::rpc::bucket::LeafBucketsError;
use crate::rpc::bucket::LeafBucketsRequest;
use crate::store::bucket::BucketSummary;

/// Each side's store as the harness reaches it: through the part store traits, in the
/// `Sendable` form (the memory store implements both forms, so a concrete call would have to
/// name one every time), plus the memory store's own `begin`.
pub trait Store:
    PartStoreWrite<Sendable, Tx = MemoryPartTx> + PartStoreRead<Sendable> + PartStoreServe<Sendable>
{
    fn begin_tx(&self) -> MemoryPartTx;
}

impl Store for MemoryPartStore {
    fn begin_tx(&self) -> MemoryPartTx {
        self.begin()
    }
}

/// The requester's cursor on the responder: a fresh peer has seen nothing.
const SINCE: CursorIndex = 0;
/// Machine commands one walk may take before it is declared not to terminate. Far above what
/// any generated walk needs: each object costs at most a sync and a share of a leaf page.
const STEP_CAP: usize = 100_000;

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
pub fn payload(value: u8) -> PayloadState {
    PayloadState::Payload(serde_json::json!(value))
}
pub fn withheld(value: u8) -> PayloadState {
    PayloadState::Withheld(PayloadDigest::of(&serde_json::json!(value)).expect(ERROR_IMPOSSIBLE))
}

/// An object's membership in one part, as a store ends up holding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Member {
    Absent,
    Live,
    /// Added, then removed: a tombstone, when the object had a payload state; nothing when the
    /// membership was still pending.
    Dead,
}

/// One object of a generated store: object `i` of the spec is [`obj(i)`](obj).
#[derive(Debug, Clone)]
pub struct ObjSpec {
    /// `None` leaves every membership pending.
    pub payload: Option<PayloadState>,
    /// Indexed by part: entry `p` is the membership in [`part(p)`](part).
    pub parts: Vec<Member>,
}

/// Write `objs` into `store` (payloads and adds, then the removals in a second transaction so
/// they leave tombstones) and grant the requester `readable`.
pub async fn build<S: Store>(store: &S, objs: &[ObjSpec], readable: &[PartKey]) -> Res<()> {
    let index = |at: usize| u16::try_from(at).expect("specs stay under u16::MAX objects");
    let members_where = |kinds: &[Member]| {
        objs.iter()
            .enumerate()
            .flat_map(|(at, spec)| {
                spec.parts
                    .iter()
                    .enumerate()
                    .filter(|(_, member)| kinds.contains(member))
                    .map(move |(p, _)| {
                        (
                            obj(index(at)),
                            part(u8::try_from(p).expect(ERROR_IMPOSSIBLE)),
                        )
                    })
            })
            .collect::<Vec<_>>()
    };
    let mut tx = store.begin_tx();
    store
        .set_payloads(
            &mut tx,
            objs.iter()
                .enumerate()
                .filter_map(|(at, spec)| Some((obj(index(at)), spec.payload.clone()?)))
                .collect(),
        )
        .await?;
    store
        .add_members(&mut tx, &members_where(&[Member::Live, Member::Dead]))
        .await?;
    let grants = readable
        .iter()
        .map(|part| (part.clone(), requester_principal(), AccessLevel::Read))
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

/// The responder's side of the bucket RPCs, answered from its store for one principal.
pub struct Responder<S> {
    pub store: S,
    principal: PeerKey,
    /// The responder's hard cap on a leaf page: a request above it is capped, not refused.
    leaf_cap: u32,
    pub traffic: BTreeMap<PartKey, Traffic>,
}

impl<S: Store> Responder<S> {
    pub fn new(store: S, leaf_cap: u32) -> Self {
        Self {
            store,
            principal: requester_principal(),
            leaf_cap,
            traffic: default(),
        }
    }

    async fn readable(&self, part: &PartKey) -> Res<bool> {
        let verdicts = self
            .store
            .read_verdicts(&self.principal, std::slice::from_ref(part))
            .await?;
        Ok(verdicts == [ReadVerdict::Permitted])
    }

    /// The part's live member count, or `None` for a part the requester may not read or the
    /// store does not know: both answer as unknown, so a refusal does not confirm the part.
    pub async fn part_summary(&mut self, part: &PartKey) -> Res<Option<u64>> {
        self.traffic
            .entry(part.clone())
            .or_default()
            .summary_requests += 1;
        if !self.readable(part).await? {
            return Ok(None);
        }
        Ok(Some(self.store.member_count(part).await?))
    }

    /// The buckets changed past `since` from `offset` through `to_level`, in bucket order:
    /// `limit_hint` of them, plus the remaining siblings of the last one (at most
    /// [`BuckId::ARITY`] more).
    ///
    /// `bucket_list` takes no `since`, so the `since` filter runs here over pages of the list.
    pub async fn changed_buckets(
        &mut self,
        req: GetChangedBucketsRequest,
    ) -> Res<Result<Vec<BucketSummary>, GetChangedBucketsError>> {
        let GetChangedBucketsRequest {
            part_key,
            offset,
            to_level,
            since,
            limit_hint,
        } = req;
        self.traffic
            .entry(part_key.clone())
            .or_default()
            .list_requests += 1;
        if !self.readable(&part_key).await? {
            return Ok(Err(GetChangedBucketsError::UnkownPart));
        }
        // A zero `limit_hint` is a zero-bucket page, not an unspecified one.
        if limit_hint == 0 {
            return Ok(Ok(default()));
        }
        let limit = usize::try_from(limit_hint).expect(ERROR_IMPOSSIBLE);
        let chunk = limit + usize::from(BuckId::ARITY);
        let mut out = Vec::<BucketSummary>::new();
        let mut from = offset;
        'list: loop {
            let listed = self
                .store
                .bucket_list(&part_key, from, to_level, chunk)
                .await?;
            let exhausted = listed.len() < chunk;
            for bucket in listed {
                from = bucket.id.increment();
                if bucket.changed_at <= since {
                    continue;
                }
                if let Some(last) = out.last()
                    && out.len() >= limit
                    && (bucket.id.parent() != last.id.parent() || out.len() >= chunk)
                {
                    break 'list;
                }
                out.push(bucket);
            }
            if exhausted {
                break;
            }
        }
        self.traffic.entry(part_key).or_default().buckets_out += out.len() as u64;
        Ok(Ok(out))
    }

    /// One page of members per requested bucket, after each bucket's `after`, with each live
    /// member's fingerprint under the request's seed. A bucket unchanged past `since` answers
    /// an empty, finished page.
    pub async fn leaf_buckets(
        &mut self,
        req: LeafBucketsRequest,
    ) -> Res<Result<LeafBucketResult, LeafBucketsError>> {
        let LeafBucketsRequest {
            part_key,
            since,
            buckets,
            seed,
            limit_hint,
        } = req;
        self.traffic
            .entry(part_key.clone())
            .or_default()
            .leaf_requests += 1;
        if !self.readable(&part_key).await? {
            return Ok(Err(LeafBucketsError::UnkownPart));
        }
        let limit = usize::try_from(limit_hint.map_or(1, |hint| hint.max(1)).min(self.leaf_cap))
            .expect(ERROR_IMPOSSIBLE);
        let mut bucks = Map::new();
        let mut entries_out = 0;
        for LeafBucketRequest { buck_id, after } in buckets {
            let changed = self
                .store
                .bucket_list(&part_key, buck_id, buck_id.level(), 1)
                .await?
                .first()
                .is_some_and(|bucket| bucket.id == buck_id && bucket.changed_at > since);
            // One past the limit tells whether the page finishes the bucket.
            let mut entries = if changed {
                self.store
                    .bucket_leaves(&part_key, buck_id, after.as_ref(), limit + 1)
                    .await?
            } else {
                default()
            };
            let done = entries.len() <= limit;
            entries.truncate(limit);
            entries_out += entries.len() as u64;
            let next_after = (!done).then(|| entries.last().expect(ERROR_IMPOSSIBLE).obj.clone());
            let entries = entries
                .into_iter()
                .map(|LeafEntry { obj, live }| BucketObjPageEntry {
                    fp: match live {
                        Some(digest) => BucketObjPageEntry::fingerprint(&seed, &obj, digest),
                        // The wire type carries a fingerprint for a dead entry too, and the
                        // requester never compares it: a removal has no content.
                        None => Fingerprint::from_u64(0),
                    },
                    dead: live.is_none(),
                    obj_key: obj,
                })
                .collect();
            bucks.insert(
                buck_id,
                LeafBucketPage {
                    entries,
                    next_after,
                    done,
                },
            );
        }
        self.traffic.entry(part_key).or_default().leaf_entries_out += entries_out;
        Ok(Ok(LeafBucketResult { seed, bucks }))
    }

    /// The object's payload state, for a sync of its membership in `part`. The machine only
    /// asks for objects it found in a readable part's leaves, so anything else is a harness or
    /// machine bug.
    pub async fn fetch(&mut self, obj: &ObjKey, part: &PartKey) -> Res<PayloadState> {
        self.traffic.entry(part.clone()).or_default().fetches += 1;
        let readable = self.store.readable_parts_of(&self.principal, obj).await?;
        assert!(
            readable.contains(part),
            "the machine asked to sync {obj} in {part}, which the requester may not read there"
        );
        let state = self
            .store
            .obj_states(std::slice::from_ref(obj))
            .await?
            .remove(0)
            .expect("a live member has a payload state");
        Ok(state.payload)
    }
}

/// How the requester's walk of one part went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Walk {
    /// The responder answered the part as unknown.
    Refused,
    /// The strategy decision's listing found nothing to walk: no machine was built.
    Clean,
    /// A machine walked the part at this working level and upgraded to the cursor.
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
    pub walks: BTreeMap<PartKey, Walk>,
    pub applied: BTreeMap<PartKey, Applied>,
}

impl<S: Store> Driver<S> {
    pub fn new(requester: S, responder: Responder<S>) -> Self {
        Self {
            requester,
            responder,
            seeds: 0,
            walks: default(),
            applied: default(),
        }
    }

    /// The listing loop of the strategy decision and of a `ListBuckets` command: list, filter
    /// against the local summaries, relist past a page with nothing dirty at the working
    /// level. `None` when the responder refuses the part; otherwise the dirty buckets at the
    /// working level, empty when the listing is done.
    async fn list(
        &mut self,
        part: &PartKey,
        mut offset: BuckId,
        working_level: BuckLevel,
    ) -> Res<Option<Vec<BucketSummary>>> {
        loop {
            let listed = self
                .responder
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
            match filter_buckets::<Sendable, _>(
                part.clone(),
                working_level,
                listed,
                &self.requester,
            )
            .await?
            {
                FilteredBuckets::Relist(next) => offset = next,
                FilteredBuckets::Done => return Ok(Some(default())),
                FilteredBuckets::Handoff(dirty) => return Ok(Some(dirty)),
            }
        }
    }

    /// Walk every part to quiescence, the machines' commands interleaved in one queue in the
    /// order they were issued. Panics when a walk does not terminate, or stalls with a machine
    /// that never upgraded.
    pub async fn run(&mut self, parts: &[PartKey]) -> Res<()> {
        let mut machines = BTreeMap::<PartKey, BucketMachine>::new();
        let mut queue = VecDeque::<(PartKey, BucketMachineCommand)>::new();
        let mut out = Vec::new();
        for part in parts {
            let Some(remote_size) = self.responder.part_summary(part).await? else {
                self.walks.insert(part.clone(), Walk::Refused);
                continue;
            };
            let working_level = calc_working_level(remote_size, BuckId::MAX_LEVEL);
            let dirty = self
                .list(part, BuckId::ROOT, working_level)
                .await?
                .expect("the part was readable a moment ago");
            if dirty.is_empty() {
                self.walks.insert(part.clone(), Walk::Clean);
                continue;
            }
            let mut machine =
                BucketMachine::new(part.clone(), BuckId::MAX_LEVEL, remote_size, SINCE);
            machine.on_bucket_page(dirty, &mut out);
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
                    let result = self
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
                    assert_eq!(result.seed, seed);
                    let pages = filter_objects::<Sendable, _>(
                        part.clone(),
                        result.bucks,
                        seed,
                        &self.requester,
                    )
                    .await?;
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
                    machine.on_obj_sync_completed(&obj_key, &mut out);
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
                BucketMachineCommand::UpgradeToCursor { part_key } => {
                    assert_eq!(part_key, part);
                    machines.remove(&part);
                }
            }
            queue.extend(out.drain(..).map(|cmd| (part.clone(), cmd)));
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
    /// The responder's dead members: the most removals. Not only those the requester holds
    /// now: another part's walk may hand it the object before this part's leaves reach the
    /// dead entry, and then the removal is issued (a no-op for a non-member).
    pub dead: u64,
}

pub async fn divergence<S: Store>(requester: &S, responder: &S, part: &PartKey) -> Res<Divergence> {
    let root = [BuckId::ROOT];
    let ours = requester.bucket_summaries(part, &root).await?.remove(0);
    let theirs = responder.bucket_summaries(part, &root).await?.remove(0);
    let leaves = responder
        .bucket_leaves(part, BuckId::ROOT, None, usize::MAX)
        .await?;
    let objs = leaves
        .iter()
        .map(|entry| entry.obj.clone())
        .collect::<Vec<_>>();
    let states = requester.obj_states(&objs).await?;
    let mut live_differing = 0;
    let mut dead = 0;
    for (entry, state) in leaves.iter().zip(&states) {
        match entry.live {
            Some(digest) => {
                live_differing += u64::from(
                    !state
                        .as_ref()
                        .is_some_and(|state| state.digest == digest && state.parts.contains(part)),
                );
            }
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
/// responder's is live in the requester's with the same digest, and every dead one is not live
/// there. Members only the requester has stay: the walk is a pull.
pub async fn assert_converged<S: Store>(requester: &S, responder: &S, part: &PartKey) -> Res<()> {
    let leaves = responder
        .bucket_leaves(part, BuckId::ROOT, None, usize::MAX)
        .await?;
    let objs = leaves
        .iter()
        .map(|entry| entry.obj.clone())
        .collect::<Vec<_>>();
    let states = requester.obj_states(&objs).await?;
    for (entry, state) in leaves.iter().zip(&states) {
        match entry.live {
            Some(digest) => assert!(
                state
                    .as_ref()
                    .is_some_and(|state| state.digest == digest && state.parts.contains(part)),
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
    Ok(store
        .bucket_leaves(part, BuckId::ROOT, None, usize::MAX)
        .await?
        .into_iter()
        .map(|entry| (entry.obj, entry.live.is_some()))
        .collect())
}

pub const PARTS: u8 = 3;

fn any_payload() -> impl Strategy<Value = Option<PayloadState>> {
    prop_oneof![
        1 => Just(None),
        3 => (0u8..3).prop_map(|value| Some(payload(value))),
        1 => (0u8..3).prop_map(|value| Some(withheld(value))),
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
}

impl Scenario {
    pub fn parts(&self) -> Vec<PartKey> {
        (0..PARTS).map(part).collect()
    }

    pub fn readable_parts(&self) -> Vec<PartKey> {
        (0..PARTS)
            .filter(|p| self.readable[usize::from(*p)])
            .map(part)
            .collect()
    }
}

/// Overlapping states: each requester object copies the responder's payload state and each
/// membership, or holds its own, independently, so stores come out identical, divergent in
/// content, membership or both, with withheld payloads on either side.
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
    )
        .prop_map(|(pairs, readable, leaf_cap)| {
            let (responder, requester) = pairs.into_iter().unzip();
            Scenario {
                responder,
                requester,
                readable,
                leaf_cap,
            }
        })
}
