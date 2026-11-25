//! The laws every part store obeys, as a harness the backends' own tests call.
//!
//! A backend supplies a [`Backend`] (a fresh store per law, its transactions) and calls each law
//! from a test of its own. The harness mirrors every committed transaction into the
//! [`PartOracle`] and, after each, compares the store's whole observable state with it
//! ([`Harness::check`]): object states, bucket summaries (incremental against a recompute), the
//! member counts, and per principal the part summaries, bucket lists and leaves, readable object
//! states, peer cursors and authorized reads of every route, and the full log.
//!
//! | Law | Where |
//! | --- | --- |
//! | replay of the log reconstructs membership and access | [`replay_reconstructs_membership_and_access`] |
//! | collapse and tombstones (members and grants) | [`collapse_and_tombstones`], every check |
//! | pending → live when the payload arrives, stamped exactly once | [`pending_goes_live_once`] |
//! | authorization follows current access, both grant and revoke orders; unknown ≠ denied | [`authorization_follows_current_access`] |
//! | after a revocation commits, no authorized read returns any row of the part, at any rev | [`revocation_hides_every_row_of_the_part`], [`revocation_mid_stream_hides_the_unread_rows`] |
//! | after a grant, an authorized read from the old cursor returns the backlog | [`grant_returns_the_backlog`] |
//! | an object route follows the object's readable parts and carries only their rows | [`object_routes_follow_the_readable_parts`] |
//! | an authorized read equals the oracle's filter of current access | every check, every authorized read in [`differential`] |
//! | a route skips a removal of a membership added after its cursor | [`removals_skip_routes_that_never_saw_the_member`], every route read in [`differential`] |
//! | incremental bucket summaries equal a recompute | every check |
//! | part summaries (member count, dirt) equal a count over the oracle's log | every check, [`dirty_count_counts_log_rows`] |
//! | bucket reads, summaries and object states follow current access, revocation mid-walk included | [`revocation_hides_the_parts_buckets`], every check |
//! | peer cursors only advance | [`peer_cursors_only_advance`], every check |
//! | a transaction is atomic: nothing before commit, everything after, nothing on rollback | [`writes_are_atomic`] |
//! | a write that changes nothing stamps nothing | [`unchanged_writes_stamp_nothing`] |
//! | all of the above over generated sequences, authorized reads by random principals included | [`differential`] |
//! | the rev stream laws over the log | [`LogStreams`] with [`crate::rev::laws`] |

use std::pin::pin;

use proptest::prelude::*;

use crate::interlude::*;
use crate::okv::Mutation;
use crate::okv::Selector;
use crate::okv::laws::Bytes;
use crate::okv::oracle::Oracle;
use crate::part_store::AccessLevel;
use crate::part_store::AuthorizedPage;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::LogRoutes;
use crate::part_store::LogSelector;
use crate::part_store::ObjPayload;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::PayloadDigest;
use crate::part_store::ReadVerdict;
use crate::part_store::oracle::PartOracle;
use crate::part_store::oracle::PartWrite;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::rev::RevStream;
use crate::rev::RevWatch;
use crate::rev::laws::StreamBackend;
use crate::rev::laws::WakeCount;
use crate::rev::laws::any_limits;
use crate::rev::laws::limits;
use future_form::Sendable;

mod generators;
pub use generators::part_ops;
use generators::*;

type Tx<B> = <<B as Backend>::Store as PartStoreWrite<Sendable>>::Tx;

/// What a backend supplies to run the laws.
#[async_trait]
pub trait Backend: Clone + Send + Sync + 'static {
    type Store: PartStoreWrite<Sendable, Tx: Send>
        + PartStoreRead<Sendable>
        + PartStoreServe<Sendable>
        + RevSource<Sendable, Key = LogKey, Value = LogEntry, Selector = LogSelector>
        + Clone
        + Send
        + Sync
        + 'static;

    /// A new, empty store, sharing nothing with any other.
    async fn open(&self) -> Res<Self::Store>;
    async fn begin(&self, store: &Self::Store) -> Res<Tx<Self>>;
    async fn commit(&self, tx: Tx<Self>) -> Res<()>;
    async fn rollback(&self, tx: Tx<Self>) -> Res<()>;
}

/// The small universe every law and generated sequence draws from.
pub fn obj(index: u8) -> ObjKey {
    ObjKey::new(&[index])
}
pub fn part(index: u8) -> PartKey {
    PartKey::new(&[100 + index])
}
pub fn principal(index: u8) -> PeerKey {
    PeerKey::new(&[200 + index])
}
pub fn payload(value: u8) -> ObjPayload {
    serde_json::json!(value)
}
const OBJS: u8 = 4;
const PARTS: u8 = 3;
const PRINCIPALS: u8 = 3;

/// Stage one batch through the trait.
async fn stage<S: PartStoreWrite<Sendable>>(
    store: &S,
    tx: &mut S::Tx,
    write: PartWrite,
) -> Res<()> {
    match write {
        PartWrite::EnsureParts(parts) => store.ensure_parts(tx, &parts).await,
        PartWrite::SetPayloads(payloads) => store.set_payloads(tx, payloads).await,
        PartWrite::AddMembers(members) => store.add_members(tx, &members).await,
        PartWrite::RemoveMembers(members) => store.remove_members(tx, &members).await,
        PartWrite::GrantAccess(grants) => store.grant_access(tx, &grants).await,
        PartWrite::RevokeAccess(revokes) => store.revoke_access(tx, &revokes).await,
    }
}

/// A store, its watch, and the oracle it must agree with.
pub struct Harness<B: Backend> {
    backend: B,
    store: B::Store,
    watch: RevWatch,
    oracle: PartOracle,
}

impl<B: Backend> Harness<B> {
    pub async fn open(backend: &B) -> Res<Self> {
        let store = backend.open().await?;
        let watch = store.watch();
        assert_eq!(
            store.latest_rev().await?,
            0,
            "a fresh store has stamped nothing"
        );
        Ok(Self {
            backend: backend.clone(),
            store,
            watch,
            oracle: default(),
        })
    }

    /// Commit `writes` in one transaction, mirror it into the oracle, and check everything.
    /// Returns the rev it stamped, if any.
    pub async fn commit(&mut self, writes: Vec<PartWrite>) -> Res<Option<Rev>> {
        let before = self.oracle.latest_rev();
        let mut tx = self.backend.begin(&self.store).await?;
        for write in writes.clone() {
            stage(&self.store, &mut tx, write).await?;
        }
        assert_eq!(self.watch.latest(), before, "the watch moved before commit");
        self.backend.commit(tx).await?;
        let stamped = self.oracle.commit(writes);
        assert_eq!(self.store.latest_rev().await?, self.oracle.latest_rev());
        assert_eq!(
            self.watch.latest(),
            self.oracle.latest_rev(),
            "the watch is not at the stamped rev"
        );
        self.check().await?;
        Ok(stamped)
    }

    /// Stage `writes` in one transaction and roll it back: nothing changes.
    pub async fn rollback(&mut self, writes: Vec<PartWrite>) -> Res<()> {
        let mut tx = self.backend.begin(&self.store).await?;
        for write in writes {
            stage(&self.store, &mut tx, write).await?;
        }
        self.backend.rollback(tx).await?;
        assert_eq!(self.store.latest_rev().await?, self.oracle.latest_rev());
        self.check().await
    }

    pub async fn set_peer_part_cursors(&mut self, cursors: &[(PeerKey, PartKey, Rev)]) -> Res<()> {
        self.store.set_peer_part_cursors(cursors).await?;
        self.oracle.set_peer_part_cursors(cursors);
        self.check().await
    }

    /// Compare the store's whole observable state over the universe with the oracle.
    pub async fn check(&self) -> Res<()> {
        let store = &self.store;
        let oracle = &self.oracle;
        let objs = (0..OBJS).map(obj).collect::<Vec<_>>();
        let parts = (0..PARTS).map(part).collect::<Vec<_>>();
        let principals = (0..PRINCIPALS).map(principal).collect::<Vec<_>>();
        let latest = oracle.latest_rev();

        assert_eq!(
            store.obj_states(&objs).await?,
            objs.iter()
                .map(|obj| oracle.obj_state(obj))
                .collect::<Vec<_>>(),
            "object states"
        );
        // Every bucket on the path of every object in the universe, all levels, plus every
        // level-1 bucket: every non-empty bucket and some empty ones.
        let ids = objs
            .iter()
            .flat_map(|obj| {
                let deepest = BuckId::deepest_from_obj_key(obj);
                (0..=BuckId::MAX_LEVEL).map(move |level| deepest.to_level(level))
            })
            .chain((0..BuckId::ARITY).map(|index| BuckId::new(1, index)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for part in &parts {
            assert_eq!(store.member_count(part).await?, oracle.member_count(part));
            // Incremental summaries against a recompute.
            assert_eq!(
                store.bucket_summaries(part, &ids).await?,
                ids.iter()
                    .map(|id| oracle.bucket_summary(part, *id))
                    .collect::<Vec<_>>(),
                "bucket summaries of {part}"
            );
        }
        for principal in &principals {
            for since in [0, latest / 2, latest] {
                let asked = parts
                    .iter()
                    .map(|part| (part.clone(), since))
                    .collect::<Vec<_>>();
                assert_eq!(
                    store.part_summaries(principal, &asked).await?,
                    parts
                        .iter()
                        .map(|part| oracle.part_summary(principal, part, since))
                        .collect::<Vec<_>>(),
                    "summaries for {principal} since {since}"
                );
                for part in &parts {
                    assert_eq!(
                        store
                            .bucket_list(
                                principal,
                                part,
                                BuckId::ROOT,
                                BuckId::MAX_LEVEL,
                                since,
                                usize::MAX
                            )
                            .await?,
                        oracle.bucket_list(
                            principal,
                            part,
                            BuckId::ROOT,
                            BuckId::MAX_LEVEL,
                            since,
                            usize::MAX
                        ),
                        "bucket list of {part} for {principal} since {since}"
                    );
                    let bucks = [(BuckId::ROOT, None), (BuckId::new(1, 3), None)];
                    assert_eq!(
                        store
                            .bucket_leaves(principal, part, &bucks, since, usize::MAX)
                            .await?,
                        oracle.bucket_leaves(principal, part, &bucks, since, usize::MAX),
                        "leaves of {part} for {principal} since {since}"
                    );
                }
            }
            assert_eq!(
                store.readable_obj_states(principal, &objs).await?,
                objs.iter()
                    .map(|obj| oracle.readable_obj_state(principal, obj))
                    .collect::<Vec<_>>(),
                "objects readable by {principal}"
            );
            assert_eq!(
                store.peer_part_cursors(principal, &parts).await?,
                parts
                    .iter()
                    .map(|part| oracle.peer_part_cursor(principal, part))
                    .collect::<Vec<_>>()
            );
        }
        if latest > 0 {
            self.read(&LogSelector::All, 0, latest, limits(usize::MAX, None))
                .await?;
            // Every route at once, and the object routes alone: with part routes beside them,
            // an object route's own verdict is masked by the part routes carrying its rows.
            let every_object = objs
                .iter()
                .map(|obj| (obj.clone(), 0))
                .collect::<BTreeMap<_, _>>();
            let route_sets = [
                LogRoutes {
                    parts: parts.iter().map(|part| (part.clone(), 0)).collect(),
                    objects: every_object.clone(),
                },
                LogRoutes {
                    parts: default(),
                    objects: every_object,
                },
            ];
            for principal in &principals {
                for routes in &route_sets {
                    for after in [0, latest / 2] {
                        if after < latest {
                            self.read_authorized(
                                principal,
                                routes,
                                after,
                                latest,
                                limits(usize::MAX, None),
                            )
                            .await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// One log read, compared exactly with the oracle's.
    pub async fn read(
        &self,
        selector: &LogSelector,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Res<Page<LogKey, LogEntry>> {
        let mut page = Page::default();
        self.store
            .read(selector, after, until, limits, &mut page)
            .await?;
        assert_eq!(page, self.oracle.read(selector, after, until, limits));
        Ok(page)
    }

    /// One authorized read, compared exactly with the oracle's filter of current access.
    pub async fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Res<AuthorizedPage> {
        let mut page = AuthorizedPage::default();
        self.store
            .read_authorized(principal, routes, after, until, limits, &mut page)
            .await?;
        assert_eq!(
            page,
            self.oracle
                .read_authorized(principal, routes, after, until, limits),
            "authorized read by {principal} of {routes:?} in ({after}, {until}]"
        );
        Ok(page)
    }
}

/// The parts of a page's member rows.
fn row_parts(page: &Page<LogKey, LogEntry>) -> BTreeSet<PartKey> {
    page.entries
        .iter()
        .map(|entry| match &entry.key {
            LogKey::Member { part, .. } => part.clone(),
            LogKey::Access { .. } => panic!("a route read returned an access row"),
        })
        .collect()
}

fn add(members: &[(u8, u8)]) -> PartWrite {
    PartWrite::AddMembers(members.iter().map(|&(o, p)| (obj(o), part(p))).collect())
}
fn remove(members: &[(u8, u8)]) -> PartWrite {
    PartWrite::RemoveMembers(members.iter().map(|&(o, p)| (obj(o), part(p))).collect())
}
fn set(payloads: &[(u8, u8)]) -> PartWrite {
    PartWrite::SetPayloads(
        payloads
            .iter()
            .map(|&(o, value)| (obj(o), payload(value)))
            .collect(),
    )
}
fn grant(grants: &[(u8, u8)]) -> PartWrite {
    PartWrite::GrantAccess(
        grants
            .iter()
            .map(|&(p, u)| (part(p), principal(u), AccessLevel::Read))
            .collect(),
    )
}
fn revoke(revokes: &[(u8, u8)]) -> PartWrite {
    PartWrite::RevokeAccess(
        revokes
            .iter()
            .map(|&(p, u)| (part(p), principal(u)))
            .collect(),
    )
}

/// Folding a full replay of the log (live members and grants) gives back exactly the current
/// membership and access.
pub async fn replay_reconstructs_membership_and_access<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![set(&[(0, 1), (1, 1)]), add(&[(0, 0), (0, 1), (1, 0)])])
        .await?;
    harness.commit(vec![grant(&[(0, 0), (1, 1)])]).await?;
    harness
        .commit(vec![remove(&[(0, 1)]), revoke(&[(1, 1)]), add(&[(2, 2)])])
        .await?;
    harness
        .commit(vec![set(&[(2, 0), (0, 2)]), grant(&[(2, 0)])])
        .await?;

    let mut stream = RevStream::<Sendable, _>::new(harness.store.clone(), LogSelector::All, 0);
    let mut page = Page::default();
    let mut members = BTreeSet::new();
    let mut access = BTreeMap::new();
    while !stream.caught_up() {
        stream.next_into(&mut page, limits(2, None)).await?;
        for entry in page.entries.drain(..) {
            match (entry.key, entry.value) {
                (LogKey::Member { obj, part }, Some(LogEntry::Member(_))) => {
                    members.insert((obj, part));
                }
                (LogKey::Member { obj, part }, None) => {
                    members.remove(&(obj, part));
                }
                (LogKey::Access { part, principal }, Some(LogEntry::Access(level))) => {
                    access.insert((part, principal), level);
                }
                (LogKey::Access { part, principal }, None) => {
                    access.remove(&(part, principal));
                }
                (LogKey::Access { .. }, Some(LogEntry::Member(_)))
                | (LogKey::Member { .. }, Some(LogEntry::Access(_))) => {
                    panic!("a log row's value does not match its key")
                }
            }
        }
    }
    assert_eq!(members, harness.oracle.live_members());
    assert_eq!(&access, harness.oracle.grants());
    Ok(())
}

/// A member's row collapses to its last write; a removal and a revocation are tombstones; a
/// re-add goes live at a new rev with a new `added_at`.
pub async fn collapse_and_tombstones<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    assert_eq!(
        harness
            .commit(vec![set(&[(0, 1)]), add(&[(0, 0)]), grant(&[(0, 0)])])
            .await?,
        Some(1)
    );
    assert_eq!(
        harness
            .commit(vec![remove(&[(0, 0)]), revoke(&[(0, 0)])])
            .await?,
        Some(2)
    );
    assert_eq!(harness.commit(vec![add(&[(0, 0)])]).await?, Some(3));
    let page = harness
        .read(&LogSelector::All, 0, 3, limits(10, None))
        .await?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| (entry.key.clone(), entry.rev, entry.value.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                LogKey::Access {
                    part: part(0),
                    principal: principal(0)
                },
                2,
                None
            ),
            (
                LogKey::Member {
                    obj: obj(0),
                    part: part(0)
                },
                3,
                Some(LogEntry::Member(crate::part_store::Membership {
                    added_at: 3
                }))
            ),
        ]
    );
    Ok(())
}

/// Adding an object before it has a payload stamps nothing and fills no bucket; its payload
/// promotes every pending membership in one stamp. Removing a pending membership leaves
/// nothing.
pub async fn pending_goes_live_once<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    assert_eq!(
        harness.commit(vec![add(&[(0, 0), (0, 1), (0, 2)])]).await?,
        None
    );
    assert_eq!(harness.store.member_count(&part(0)).await?, 0);
    assert_eq!(harness.commit(vec![remove(&[(0, 2)])]).await?, None);
    assert_eq!(harness.commit(vec![set(&[(0, 1)])]).await?, Some(1));
    let page = harness
        .read(&LogSelector::All, 0, 1, limits(10, None))
        .await?;
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| (entry.key.clone(), entry.rev))
            .collect::<Vec<_>>(),
        vec![
            (
                LogKey::Member {
                    obj: obj(0),
                    part: part(0)
                },
                1
            ),
            (
                LogKey::Member {
                    obj: obj(0),
                    part: part(1)
                },
                1
            ),
        ],
        "both pending memberships, once, at the payload's rev; the removed one never"
    );
    assert_eq!(harness.store.member_count(&part(0)).await?, 1);
    Ok(())
}

/// What a principal may read follows its *current* access, whichever came first, the write or
/// the grant (and the write or the revocation). An unknown part is not a denial.
pub async fn authorization_follows_current_access<B: Backend>(backend: &B) -> Res<()> {
    for grant_first in [true, false] {
        let mut harness = Harness::open(backend).await?;
        let write = vec![set(&[(0, 1)]), add(&[(0, 0)])];
        if grant_first {
            harness.commit(vec![grant(&[(0, 0)])]).await?;
            harness.commit(write).await?;
        } else {
            harness.commit(write).await?;
            harness.commit(vec![grant(&[(0, 0)])]).await?;
        }
        let routes = LogRoutes {
            parts: [(part(0), 0), (part(1), 0)].into(),
            objects: [(obj(0), 0)].into(),
        };
        let latest = harness.oracle.latest_rev();
        let all = limits(usize::MAX, None);
        let page = harness
            .read_authorized(&principal(0), &routes, 0, latest, all)
            .await?;
        assert_eq!(
            (page.parts, page.objects),
            (
                vec![ReadVerdict::Permitted, ReadVerdict::UnknownPart],
                vec![ReadVerdict::Permitted]
            )
        );
        let page = harness
            .read_authorized(&principal(1), &routes, 0, latest, all)
            .await?;
        assert_eq!(
            (page.parts, page.objects),
            (
                vec![ReadVerdict::Denied, ReadVerdict::UnknownPart],
                vec![ReadVerdict::Denied]
            )
        );
        let readable = harness
            .store
            .readable_obj_states(&principal(0), &[obj(0)])
            .await?
            .remove(0)
            .expect("object 0 lives in a readable part");
        assert_eq!(readable.parts, [part(0)].into());

        // Revocation, before or after a later write: no access either way.
        if grant_first {
            harness.commit(vec![revoke(&[(0, 0)])]).await?;
            harness.commit(vec![set(&[(0, 2)])]).await?;
        } else {
            harness.commit(vec![set(&[(0, 2)])]).await?;
            harness.commit(vec![revoke(&[(0, 0)])]).await?;
        }
        let latest = harness.oracle.latest_rev();
        let page = harness
            .read_authorized(&principal(0), &routes, 0, latest, all)
            .await?;
        assert_eq!(
            (page.parts, page.objects),
            (
                vec![ReadVerdict::Denied, ReadVerdict::UnknownPart],
                vec![ReadVerdict::Denied]
            )
        );
        assert_eq!(
            harness
                .store
                .readable_obj_states(&principal(0), &[obj(0)])
                .await?,
            vec![None]
        );
    }
    Ok(())
}

/// The bucket reads, part summaries and object states a responder serves follow current access
/// like the log does: once a revocation commits, every one of them answers `None` for the part,
/// including the next page of a leaf walk that had started, and an object's state stops naming
/// the part. A grant serves them again.
pub async fn revocation_hides_the_parts_buckets<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            set(&[(0, 1), (1, 1), (2, 1)]),
            add(&[(0, 0), (1, 0), (2, 0), (2, 1)]),
            grant(&[(0, 0), (1, 0)]),
        ])
        .await?;
    let list = |harness: &Harness<B>| {
        let store = harness.store.clone();
        async move {
            store
                .bucket_list(
                    &principal(0),
                    &part(0),
                    BuckId::ROOT,
                    BuckId::MAX_LEVEL,
                    0,
                    usize::MAX,
                )
                .await
        }
    };
    let listed = list(&harness).await?.expect("the part is readable");
    assert!(!listed.is_empty());
    let first = harness
        .store
        .bucket_leaves(&principal(0), &part(0), &[(BuckId::ROOT, None)], 0, 1)
        .await?
        .expect("the part is readable")
        .remove(0)
        .remove(0);

    harness.commit(vec![revoke(&[(0, 0)])]).await?;
    assert_eq!(list(&harness).await?, None);
    assert_eq!(
        harness
            .store
            .bucket_leaves(
                &principal(0),
                &part(0),
                &[(BuckId::ROOT, Some(first.obj))],
                0,
                usize::MAX
            )
            .await?,
        None,
        "the walk's next leaf page"
    );
    assert_eq!(
        harness
            .store
            .part_summaries(&principal(0), &[(part(0), 0), (part(1), 0)])
            .await?
            .into_iter()
            .map(|summary| summary.is_some())
            .collect::<Vec<_>>(),
        vec![false, true]
    );
    let states = harness
        .store
        .readable_obj_states(&principal(0), &[obj(0), obj(2)])
        .await?;
    assert_eq!(states[0], None, "object 0 lives only in the revoked part");
    assert_eq!(
        states[1].as_ref().map(|state| state.parts.clone()),
        Some([part(1)].into()),
        "object 2 is still readable through part 1, and no longer names part 0"
    );

    harness.commit(vec![grant(&[(0, 0)])]).await?;
    assert_eq!(list(&harness).await?, Some(listed));
    Ok(())
}

/// Once a revocation commits, no authorized read by that principal returns a row of the part:
/// not from any cursor, and not the rows written before the revocation either. An access row
/// records a change; it never grants the rows before it. The part route is `Denied`; an object
/// route stays `Permitted` while the object lives in another readable part and then carries only
/// that part's rows; an object only in the revoked part is `Denied`.
pub async fn revocation_hides_every_row_of_the_part<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    // Rows of part 0 at revs 1 to 4, and of part 1 at revs 1 and 2 (object 1 lives in both).
    harness
        .commit(vec![
            set(&[(0, 1), (1, 1), (2, 1)]),
            add(&[(0, 0), (1, 0), (1, 1), (2, 1)]),
            grant(&[(0, 0), (1, 0)]),
        ])
        .await?;
    harness.commit(vec![set(&[(0, 2), (1, 2)])]).await?;
    harness.commit(vec![remove(&[(1, 0)])]).await?;
    harness
        .commit(vec![set(&[(3, 1)]), add(&[(3, 0), (1, 0)])])
        .await?;
    let revoked_at = harness
        .commit(vec![revoke(&[(0, 0)])])
        .await?
        .expect("the revocation stamps");
    let routes = LogRoutes {
        parts: [(part(0), 0), (part(1), 0)].into(),
        objects: [(obj(0), 0), (obj(1), 0), (obj(3), 0)].into(),
    };
    let all = limits(usize::MAX, None);
    let unauthorized = harness
        .read(&LogSelector::Routes(routes.clone()), 0, revoked_at, all)
        .await?;
    assert!(
        row_parts(&unauthorized).contains(&part(0)),
        "the part has rows to hide"
    );
    for after in 0..revoked_at {
        let page = harness
            .read_authorized(&principal(0), &routes, after, revoked_at, all)
            .await?;
        assert_eq!(
            page.parts,
            vec![ReadVerdict::Denied, ReadVerdict::Permitted]
        );
        assert_eq!(
            page.objects,
            vec![
                ReadVerdict::Denied,
                ReadVerdict::Permitted,
                ReadVerdict::Denied
            ]
        );
        assert!(
            !row_parts(&page.rows).contains(&part(0)),
            "a revoked part's row was served from cursor {after}: {page:?}"
        );
    }
    // From 0, part 1's rows still come through, so the read is not vacuously empty.
    let page = harness
        .read_authorized(&principal(0), &routes, 0, revoked_at, all)
        .await?;
    assert_eq!(row_parts(&page.rows), [part(1)].into());
    Ok(())
}

/// A revocation that commits between two pages of an authorized read cuts the reader off at
/// once: the next page carries none of the part's rows, though they were written before the
/// revocation and sit past the reader's progress.
pub async fn revocation_mid_stream_hides_the_unread_rows<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    // Each of revs 1 to 3 writes one row of part 0 and one of part 1, on distinct keys so the
    // log keeps all six.
    harness
        .commit(vec![
            set(&[(0, 1), (1, 1), (2, 1), (3, 1)]),
            add(&[(0, 0), (1, 1)]),
            grant(&[(0, 0), (1, 0)]),
        ])
        .await?;
    harness.commit(vec![add(&[(2, 0), (3, 1)])]).await?;
    harness.commit(vec![add(&[(1, 0), (0, 1)])]).await?;
    let routes = LogRoutes {
        parts: [(part(0), 0), (part(1), 0)].into(),
        objects: default(),
    };
    let written = harness.oracle.latest_rev();
    // One revision per page: the first page reads rev 1, both parts.
    let page = harness
        .read_authorized(&principal(0), &routes, 0, written, limits(1, None))
        .await?;
    assert_eq!(page.rows.through, 1);
    assert_eq!(row_parts(&page.rows), [part(0), part(1)].into());

    let revoked_at = harness
        .commit(vec![revoke(&[(0, 0)])])
        .await?
        .expect("the revocation stamps");
    let mut after = page.rows.through;
    while after < written {
        let page = harness
            .read_authorized(&principal(0), &routes, after, revoked_at, limits(1, None))
            .await?;
        assert_eq!(
            page.parts,
            vec![ReadVerdict::Denied, ReadVerdict::Permitted]
        );
        assert_eq!(
            row_parts(&page.rows),
            [part(1)].into(),
            "the page after the revocation, from {after}"
        );
        after = page.rows.through;
    }
    Ok(())
}

/// An object route is permitted while the object lives in a part the principal may read, and
/// then carries only the object's rows in readable parts: a row names its part, so a row of an
/// unreadable part would disclose that membership. Once the object leaves its last readable
/// part the route is denied and carries nothing, not even that removal; the part's own route
/// still carries it.
pub async fn object_routes_follow_the_readable_parts<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            set(&[(0, 1)]),
            add(&[(0, 0), (0, 1)]),
            grant(&[(0, 0)]),
        ])
        .await?;
    let object_route = |bound| LogRoutes {
        parts: default(),
        objects: [(obj(0), bound)].into(),
    };
    let all = limits(usize::MAX, None);
    let page = harness
        .read_authorized(&principal(0), &object_route(0), 0, 1, all)
        .await?;
    assert_eq!(page.objects, vec![ReadVerdict::Permitted]);
    assert_eq!(row_parts(&page.rows), [part(0)].into());

    let removed_at = harness
        .commit(vec![remove(&[(0, 0)])])
        .await?
        .expect("the removal stamps");
    let page = harness
        .read_authorized(&principal(0), &object_route(1), 1, removed_at, all)
        .await?;
    assert_eq!(page.objects, vec![ReadVerdict::Denied]);
    assert!(page.rows.entries.is_empty(), "{page:?}");
    let part_route = LogRoutes {
        parts: [(part(0), 1)].into(),
        objects: default(),
    };
    let page = harness
        .read_authorized(&principal(0), &part_route, 1, removed_at, all)
        .await?;
    assert_eq!(page.parts, vec![ReadVerdict::Permitted]);
    assert_eq!(
        page.rows
            .entries
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.is_some()))
            .collect::<Vec<_>>(),
        vec![(
            LogKey::Member {
                obj: obj(0),
                part: part(0)
            },
            false
        )]
    );
    Ok(())
}

/// A denied route reads nothing, so its reader's cursor stays put; once access is granted,
/// reading from that cursor returns the whole backlog past it: every row the unauthorized read
/// of the same route returns, removals included, and nothing at or below the cursor.
pub async fn grant_returns_the_backlog<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![set(&[(0, 1), (1, 1)]), add(&[(0, 0), (1, 0)])])
        .await?;
    // The reader's cursor: it saw both memberships go live, before it was denied.
    let cursor = harness.oracle.latest_rev();
    harness.commit(vec![set(&[(0, 2)])]).await?;
    harness.commit(vec![remove(&[(1, 0)])]).await?;
    harness.commit(vec![set(&[(2, 1)]), add(&[(2, 0)])]).await?;
    let routes = LogRoutes {
        parts: [(part(0), cursor)].into(),
        objects: default(),
    };
    let all = limits(usize::MAX, None);
    let latest = harness.oracle.latest_rev();
    let denied = harness
        .read_authorized(&principal(0), &routes, cursor, latest, all)
        .await?;
    assert_eq!(denied.parts, vec![ReadVerdict::Denied]);
    assert!(denied.rows.entries.is_empty());

    let granted_at = harness
        .commit(vec![grant(&[(0, 0)])])
        .await?
        .expect("the grant stamps");
    let backlog = harness
        .read_authorized(&principal(0), &routes, cursor, granted_at, all)
        .await?;
    assert_eq!(backlog.parts, vec![ReadVerdict::Permitted]);
    let expected = harness
        .read(
            &LogSelector::Routes(routes.clone()),
            cursor,
            granted_at,
            all,
        )
        .await?;
    assert_eq!(backlog.rows, expected);
    assert_eq!(
        backlog
            .rows
            .entries
            .iter()
            .map(|entry| (entry.key.clone(), entry.rev, entry.value.is_some()))
            .collect::<Vec<_>>(),
        vec![
            (
                LogKey::Member {
                    obj: obj(0),
                    part: part(0)
                },
                2,
                true
            ),
            (
                LogKey::Member {
                    obj: obj(1),
                    part: part(0)
                },
                3,
                false
            ),
            (
                LogKey::Member {
                    obj: obj(2),
                    part: part(0)
                },
                4,
                true
            ),
        ],
        "the content change, the removal and the new member, all past the cursor"
    );
    Ok(())
}

/// RFC 8785 has no form for an integer beyond 2^53 - 1 in magnitude, so a payload holding one
/// has no digest. The write is refused while staging, nothing of the batch is staged, and the
/// transaction still commits what else it holds.
pub async fn a_payload_with_no_canonical_form_is_refused<B: Backend>(backend: &B) -> Res<()> {
    let harness = Harness::open(backend).await?;
    let unsafe_int = serde_json::json!(1u64 << 60);
    assert!(PayloadDigest::of(&unsafe_int).is_err());
    let mut tx = backend.begin(&harness.store).await?;
    let refused = harness
        .store
        .set_payloads(&mut tx, vec![(obj(1), payload(1)), (obj(0), unsafe_int)])
        .await;
    assert!(
        refused.is_err(),
        "a payload with no canonical form is refused"
    );
    harness
        .store
        .set_payloads(&mut tx, vec![(obj(2), payload(2))])
        .await?;
    backend.commit(tx).await?;
    let states = harness.store.obj_states(&[obj(0), obj(1), obj(2)]).await?;
    assert!(
        states[0].is_none() && states[1].is_none(),
        "nothing of the refused batch was staged"
    );
    assert!(states[2].is_some(), "the transaction kept its other writes");
    Ok(())
}

/// A part's summary counts its live members and its log rows past the cursor: member rows live
/// or removed, and the asking principal's own access row. Its latest cursor is the latest
/// member row's rev, which another principal's grant does not move. A principal that may not
/// read the part gets no summary.
pub async fn dirty_count_counts_log_rows<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![set(&[(0, 1), (1, 1)]), add(&[(0, 0), (1, 0)])])
        .await?;
    harness.commit(vec![grant(&[(0, 0), (0, 1)])]).await?;
    harness
        .commit(vec![remove(&[(1, 0)]), revoke(&[(0, 1)])])
        .await?;
    harness.commit(vec![grant(&[(0, 2)])]).await?;
    let summaries = |principal_index| {
        let store = harness.store.clone();
        async move {
            let asked = [0, 1, 3].map(|since| (part(0), since));
            store
                .part_summaries(&principal(principal_index), &asked)
                .await
        }
    };
    let summary = |member_changes, access_changes| {
        Some(crate::part_store::PartSummary {
            latest_cursor: 3,
            member_count: 1,
            deepest_bucket_level: BuckId::MAX_LEVEL,
            dirty: crate::part_store::PartDirtyCount {
                member_changes,
                access_changes,
            },
        })
    };
    assert_eq!(
        summaries(0).await?,
        vec![summary(2, 1), summary(1, 1), summary(0, 0)]
    );
    assert_eq!(summaries(1).await?, vec![None, None, None], "revoked");
    Ok(())
}

/// A route hands out a removal only to a reader that may have seen the membership: one whose
/// cursor (the larger of the route's bound and the read's `after`) is at or past the
/// membership's `added_at`. `All` keeps every removal.
pub async fn removals_skip_routes_that_never_saw_the_member<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    // Object 0 goes live at 1 and leaves at 2; object 1 goes live at 3 and leaves at 4.
    harness.commit(vec![set(&[(0, 1)]), add(&[(0, 0)])]).await?;
    harness.commit(vec![remove(&[(0, 0)])]).await?;
    harness.commit(vec![set(&[(1, 1)]), add(&[(1, 0)])]).await?;
    harness.commit(vec![remove(&[(1, 0)])]).await?;
    let route = |bound| {
        LogSelector::Routes(LogRoutes {
            parts: [(part(0), bound)].into(),
            objects: default(),
        })
    };
    let removed = |page: Page<LogKey, LogEntry>| {
        page.entries
            .into_iter()
            .map(|entry| match entry.key {
                LogKey::Member { obj, .. } => (obj, entry.rev),
                LogKey::Access { .. } => panic!("a part route selected an access row"),
            })
            .collect::<Vec<_>>()
    };
    let all = limits(usize::MAX, None);
    // A reader at 0 saw neither membership.
    assert_eq!(removed(harness.read(&route(0), 0, 4, all).await?), vec![]);
    // A reader at 1 may have seen object 0, not object 1.
    assert_eq!(
        removed(harness.read(&route(0), 1, 4, all).await?),
        vec![(obj(0), 2)]
    );
    // A route bound counts as the reader's cursor too: from 3, object 1's removal is news.
    assert_eq!(
        removed(harness.read(&route(3), 0, 4, all).await?),
        vec![(obj(1), 4)]
    );
    // The object route follows the same rule.
    let object_route = LogSelector::Routes(LogRoutes {
        parts: default(),
        objects: [(obj(1), 0)].into(),
    });
    assert_eq!(
        removed(harness.read(&object_route, 2, 4, all).await?),
        vec![]
    );
    assert_eq!(
        removed(harness.read(&object_route, 3, 4, all).await?),
        vec![(obj(1), 4)]
    );
    // Object 2 goes live at 5, leaves at 6, comes back at 7 and leaves at 8. A reader at 5 saw
    // the first incarnation; the log collapsed its removal at 6 away, so the removal at 8 is the
    // only way it learns the membership is gone, though the second incarnation is news to it.
    harness.commit(vec![set(&[(2, 1)]), add(&[(2, 0)])]).await?;
    harness.commit(vec![remove(&[(2, 0)])]).await?;
    harness.commit(vec![add(&[(2, 0)])]).await?;
    harness.commit(vec![remove(&[(2, 0)])]).await?;
    assert_eq!(
        removed(harness.read(&route(0), 5, 8, all).await?),
        vec![(obj(2), 8)]
    );
    assert_eq!(removed(harness.read(&route(0), 4, 8, all).await?), vec![]);
    // A full replay keeps both.
    assert_eq!(
        removed(harness.read(&LogSelector::All, 0, 8, all).await?),
        vec![(obj(0), 2), (obj(1), 4), (obj(2), 8)]
    );
    Ok(())
}

/// A peer cursor only moves forward; setting a lower one keeps the higher.
pub async fn peer_cursors_only_advance<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .set_peer_part_cursors(&[(principal(0), part(0), 5), (principal(0), part(1), 2)])
        .await?;
    harness
        .set_peer_part_cursors(&[(principal(0), part(0), 3), (principal(0), part(1), 7)])
        .await?;
    assert_eq!(
        harness
            .store
            .peer_part_cursors(&principal(0), &[part(0), part(1), part(2)])
            .await?,
        vec![Some(5), Some(7), None]
    );
    Ok(())
}

/// A transaction is atomic: none of its batches is visible before commit (log, objects,
/// buckets, watch), all of them are after, stamped with one rev, and a rollback leaves nothing.
pub async fn writes_are_atomic<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    let writes = vec![
        set(&[(0, 1), (1, 1)]),
        add(&[(0, 0), (1, 1)]),
        grant(&[(1, 0)]),
    ];
    let wakes = WakeCount::default();
    let mut watch = harness.store.watch();
    let mut wait = pin!(watch.wait_past(0));
    assert!(wakes.poll(wait.as_mut()).is_pending());

    harness.rollback(writes.clone()).await?;
    assert_eq!(wakes.count(), 0, "a rollback woke a waiter");

    let mut tx = harness.backend.begin(&harness.store).await?;
    for write in writes.clone() {
        stage(&harness.store, &mut tx, write).await?;
    }
    // Staged, not committed: the store is still empty.
    harness.check().await?;
    assert_eq!(wakes.count(), 0, "a staged write woke a waiter");
    harness.backend.commit(tx).await?;
    assert_eq!(harness.oracle.commit(writes), Some(1));
    harness.check().await?;
    assert!(wakes.count() > 0, "the commit woke nobody");
    assert_eq!(wakes.poll(wait.as_mut()), std::task::Poll::Ready(1));
    Ok(())
}

/// Re-writing the payload an object holds, re-adding a live member, removing a non-member,
/// re-granting a level and revoking nothing stamp nothing and wake nobody.
pub async fn unchanged_writes_stamp_nothing<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![set(&[(0, 1)]), add(&[(0, 0)]), grant(&[(0, 0)])])
        .await?;
    assert_eq!(
        harness
            .commit(vec![
                set(&[(0, 1)]),
                add(&[(0, 0)]),
                remove(&[(1, 0)]),
                grant(&[(0, 0)]),
                revoke(&[(1, 1)]),
                PartWrite::EnsureParts(vec![part(2)]),
            ])
            .await?,
        None
    );
    assert_eq!(harness.store.latest_rev().await?, 1);
    Ok(())
}

strike! {
    #[structstruck::each[derive(Debug, Clone)]]
    /// One step of the differential run.
    pub enum PartOp {
        Commit(Vec<PartWrite>),
        Rollback(Vec<PartWrite>),
        SetPeerCursors(Vec<(PeerKey, PartKey, Rev)>),
        /// A log read; `after` and `until` are fractions (of 256) of the stamped revs.
        Read {
            selector: LogSelector,
            after: u8,
            until: u8,
            limits: Limits,
        },
        /// An authorized read by `principal`, windowed like `Read`.
        ReadAuthorized {
            principal: PeerKey,
            routes: LogRoutes,
            after: u8,
            until: u8,
            limits: Limits,
        },
    }
}

/// A read window `(after, until]` from two fractions (of 256) of the stamped revs, or `None`
/// before anything is stamped.
fn window(latest: Rev, after: u8, until: u8) -> Option<(Rev, Rev)> {
    if latest == 0 {
        return None;
    }
    let after = u64::from(after) * latest / 256;
    let until = after + 1 + u64::from(until) * (latest - after) / 256;
    Some((after, until))
}

/// Every law's check over an arbitrary run of commits, rollbacks, cursor moves and log reads.
pub async fn differential<B: Backend>(backend: &B, ops: Vec<PartOp>) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    for op in ops {
        match op {
            PartOp::Commit(writes) => {
                harness.commit(writes).await?;
            }
            PartOp::Rollback(writes) => harness.rollback(writes).await?,
            PartOp::SetPeerCursors(cursors) => harness.set_peer_part_cursors(&cursors).await?,
            PartOp::Read {
                selector,
                after,
                until,
                limits,
            } => {
                if let Some((after, until)) = window(harness.oracle.latest_rev(), after, until) {
                    harness.read(&selector, after, until, limits).await?;
                }
            }
            PartOp::ReadAuthorized {
                principal,
                routes,
                after,
                until,
                limits,
            } => {
                if let Some((after, until)) = window(harness.oracle.latest_rev(), after, until) {
                    harness
                        .read_authorized(&principal, &routes, after, until, limits)
                        .await?;
                }
            }
        }
    }
    Ok(())
}

/// Runs the rev stream laws of [`crate::rev::laws`] over a part store's log.
///
/// The stream laws write okv-shaped batches; each batch becomes one part-store transaction on
/// one part ([`STREAM_PART`]): a put of key `k` sets object `k`'s payload and adds it to the
/// part, a delete removes it. Part-store writes that change nothing stamp nothing, but the laws
/// number one rev per batch, so a batch that would stamp nothing also re-sets a filler object's
/// payload in a part no selector reaches. What the log must return is the part oracle's read,
/// mirrored here as the batches commit.
#[derive(Clone)]
pub struct LogStreams<B: Backend> {
    backend: B,
    oracle: Arc<surelock::mutex::Mutex<PartOracle>>,
}

const STREAM_PART: u8 = 50;
const FILLER_PART: u8 = 51;
const FILLER_OBJ: u8 = 250;

impl<B: Backend> LogStreams<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            oracle: Arc::new(surelock::mutex::Mutex::new(default())),
        }
    }

    fn with_oracle<R>(&self, inspect: impl FnOnce(&mut PartOracle) -> R) -> R {
        surelock::key::lock_scope(|key| {
            let (mut oracle, _key) = key.lock(&self.oracle);
            inspect(&mut oracle)
        })
    }

    fn writes_for(&self, batch: Vec<Mutation<Bytes, Bytes>>) -> Vec<PartWrite> {
        let mut writes = Vec::new();
        for mutation in batch {
            match mutation {
                Mutation::Put { key, value } => {
                    let obj = ObjKey::new(&key);
                    writes.push(PartWrite::SetPayloads(vec![(
                        obj.clone(),
                        serde_json::json!(value),
                    )]));
                    writes.push(PartWrite::AddMembers(vec![(obj, part(STREAM_PART))]));
                }
                Mutation::Delete { key } => {
                    writes.push(PartWrite::RemoveMembers(vec![(
                        ObjKey::new(&key),
                        part(STREAM_PART),
                    )]));
                }
            }
        }
        let stamps = self.with_oracle(|oracle| oracle.clone().commit(writes.clone()).is_some());
        if !stamps {
            let next = self.with_oracle(|oracle| oracle.latest_rev() + 1);
            writes.push(PartWrite::SetPayloads(vec![(
                obj(FILLER_OBJ),
                serde_json::json!(next),
            )]));
            writes.push(PartWrite::AddMembers(vec![(
                obj(FILLER_OBJ),
                part(FILLER_PART),
            )]));
        }
        writes
    }
}

#[async_trait]
impl<B: Backend> StreamBackend for LogStreams<B> {
    type Source = B::Store;
    type Writer = B::Store;

    async fn open(&self) -> Res<(B::Store, B::Store)> {
        self.with_oracle(|oracle| *oracle = default());
        let store = self.backend.open().await?;
        Ok((store.clone(), store))
    }

    async fn commit(&self, store: &B::Store, batches: Vec<Vec<Mutation<Bytes, Bytes>>>) -> Res<()> {
        for batch in batches {
            let writes = self.writes_for(batch);
            let mut tx = self.backend.begin(store).await?;
            for write in writes.clone() {
                stage(store, &mut tx, write).await?;
            }
            self.backend.commit(tx).await?;
            let stamped = self.with_oracle(|oracle| oracle.commit(writes));
            assert!(stamped.is_some(), "a stream batch stamped nothing");
        }
        Ok(())
    }

    async fn rollback(
        &self,
        store: &B::Store,
        batches: Vec<Vec<Mutation<Bytes, Bytes>>>,
    ) -> Res<()> {
        let mut tx = self.backend.begin(store).await?;
        for batch in batches {
            for write in self.writes_for(batch) {
                stage(store, &mut tx, write).await?;
            }
        }
        self.backend.rollback(tx).await
    }

    /// `All` is the stream part's route; a key set or range is the object routes of its keys.
    fn selector(&self, selector: &Selector<Bytes>) -> LogSelector {
        let objects = match selector {
            Selector::All => {
                return LogSelector::Routes(LogRoutes {
                    parts: [(part(STREAM_PART), 0)].into(),
                    objects: default(),
                });
            }
            Selector::Keys(_) | Selector::Range(_) => (0..FILLER_OBJ)
                .map(|key| vec![key])
                .filter(|key| selector.matches(key))
                .map(|key| (ObjKey::new(&key), 0))
                .collect(),
        };
        LogSelector::Routes(LogRoutes {
            parts: default(),
            objects,
        })
    }

    fn expected(
        &self,
        _oracle: &Oracle<Bytes, Bytes>,
        selector: &Selector<Bytes>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Page<LogKey, LogEntry> {
        let selector = self.selector(selector);
        self.with_oracle(|oracle| oracle.read(&selector, after, until, limits))
    }
}
