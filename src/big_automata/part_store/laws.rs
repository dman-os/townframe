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
//! | an authorized read of the empty window decides every verdict and reads no row | [`an_empty_window_reads_verdicts_only`], every check, [`differential`] |
//! | an authorized read carries, from its snapshot, one payload per object with a live row in it, and no other | [`authorized_reads_carry_their_payloads`], every check, [`differential`] |
//! | a store has a generation from its creation, stable across writes and reads, new after a mint, and carried by every summary | [`generations_identify_the_log`], every check, [`differential`] |
//! | the responder's rev and watch follow commits like the log's | every commit and rollback |
//! | the long-poll hub is told, per commit, the interests of the rows it stamped with their counts, and nothing for a commit that stamped none | every commit ([`feed`]) |
//! | an authorized read equals the oracle's filter of current access | every check, every authorized read in [`differential`] |
//! | a route delivers every removal above its bound, whatever the membership's `added_at` | [`removals_reach_every_route_above_its_bound`], every route read in [`differential`] |
//! | incremental bucket summaries equal a recompute | every check |
//! | part summaries (member count, dirt) equal a count over the oracle's log | every check, [`dirty_count_counts_log_rows`] |
//! | bucket reads, summaries and object states follow current access, revocation mid-walk included | [`revocation_hides_the_parts_buckets`], every check |
//! | a dropped payload is no longer served or observable; dropping nothing is a no-op | [`a_dropped_payload_is_gone`] |
//! | dropping the payload of a live member is a caller bug and panics; remove-then-drop in one transaction is valid | [`dropping_the_payload_of_a_member_is_a_caller_bug`], [`removing_then_dropping_in_one_transaction_is_valid`] |
//! | a part-less object is listed, in key order, until its payload is dropped | [`a_partless_object_is_listed_until_its_payload_is_dropped`], every check |
//! | dropping is a batch of its transaction | [`dropping_is_atomic_with_its_transaction`], [`differential`] |
//! | a transaction is atomic: nothing before commit, everything after, nothing on rollback | [`writes_are_atomic`] |
//! | a write that changes nothing stamps nothing | [`unchanged_writes_stamp_nothing`] |
//! | many keys sharing deep buckets: every level counts every member under it and its leaves list them in order, whole or paged | [`crowded_buckets_count_and_list_every_member`] |
//! | a leaf page is the longest prefix of the bucket within the limit and the byte budget; its first entry is always in, and it stops rather than skips | [`leaf_pages_are_the_longest_prefix_within_the_byte_budget`], every check |
//! | a bucket's summary does not depend on the order its members were written, pending first or live first | [`bucket_summaries_ignore_write_order`] |
//! | removals and payload changes inside one deep bucket move its path only, and a change undone restores the fingerprints | [`bucket_summaries_follow_changes_inside_one_bucket`] |
//! | a listing ends at the last bucket of a level: nothing past it at its level, then the next level's own start, and no repeat at the deepest level | [`a_listing_ends_at_the_last_bucket_of_a_level`] |
//! | all of the above over generated sequences, authorized reads by random principals included | [`differential`], and over keys that crowd into shared deep buckets, [`crowded_differential`] |
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
use crate::part_store::Generation;
use crate::part_store::LeafEntry;
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
use crate::part_store::bucket_summary::BucketSummary;
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

mod budget;
pub use budget::leaf_pages_are_the_longest_prefix_within_the_byte_budget;
mod crowd;
pub use crowd::bucket_summaries_follow_changes_inside_one_bucket;
pub use crowd::bucket_summaries_ignore_write_order;
pub use crowd::crowd_ops;
pub use crowd::crowded_buckets_count_and_list_every_member;
pub use crowd::crowded_differential;
pub use crowd::obj_in_bucket;
mod edges;
mod feed;
pub use edges::a_listing_ends_at_the_last_bucket_of_a_level;
mod generators;
pub use generators::part_ops;
pub use generators::part_writes;
use generators::*;

mod payloads;
pub use payloads::a_dropped_payload_is_gone;
pub use payloads::a_partless_object_is_listed_until_its_payload_is_dropped;
pub use payloads::dropping_is_atomic_with_its_transaction;
pub use payloads::dropping_the_payload_of_a_member_is_a_caller_bug;
pub use payloads::removing_then_dropping_in_one_transaction_is_valid;

type Tx<B> = <<B as Backend>::Store as PartStoreWrite>::Tx;

/// What a backend supplies to run the laws.
#[async_trait]
pub trait Backend: Clone + Send + Sync + 'static {
    type Store: PartStoreWrite<Tx: Send>
        + PartStoreRead
        + PartStoreServe
        + RevSource<Key = LogKey, Value = LogEntry, Selector = LogSelector>
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

mod universe;
use universe::OBJS;
use universe::PARTS;
use universe::PRINCIPALS;
pub use universe::add;
pub use universe::drop_payloads;
pub use universe::grant;
pub use universe::obj;
pub use universe::part;
pub use universe::payload;
pub use universe::principal;
pub use universe::remove;
pub use universe::revoke;
pub use universe::set;
pub use universe::stage;

/// A store, its watches, and the oracle it must agree with.
pub struct Harness<B: Backend> {
    backend: B,
    store: B::Store,
    /// The log's watch, as its local consumers see it.
    watch: RevWatch,
    /// The responder's watch ([`PartStoreServe::watch`]).
    serve_watch: RevWatch,
    oracle: PartOracle,
    /// The objects [`Harness::check`] reads states and bucket paths for: the writes the harness
    /// is fed stay within them.
    objs: Vec<ObjKey>,
}

impl<B: Backend> Harness<B> {
    pub async fn open(backend: &B) -> Res<Self> {
        Self::open_over(backend, (0..OBJS).map(obj).collect()).await
    }

    /// A harness whose checks cover `objs`, the universe the writes it is fed are drawn from.
    pub async fn open_over(backend: &B, objs: Vec<ObjKey>) -> Res<Self> {
        let store = backend.open().await?;
        let watch = RevSource::watch(&store);
        let serve_watch = PartStoreServe::watch(&store);
        assert_eq!(
            RevSource::latest_rev(&store).await?,
            0,
            "a fresh store has stamped nothing"
        );
        assert_eq!(PartStoreServe::latest_rev(&store).await?, 0);
        let generation = store.generation().await?;
        assert_ne!(
            generation,
            Generation::default(),
            "a new store mints its generation"
        );
        Ok(Self {
            backend: backend.clone(),
            store,
            watch,
            serve_watch,
            oracle: PartOracle::with_generation(generation),
            objs,
        })
    }

    /// Mint a new generation, which must differ from the last and change nothing else.
    pub async fn mint_generation(&mut self) -> Res<Generation> {
        let before = self.oracle.generation();
        let minted = self.store.mint_generation().await?;
        assert_ne!(minted, before, "a mint gives the generation it replaced");
        assert_ne!(minted, Generation::default());
        self.oracle.set_generation(minted);
        // The log is untouched: no rev, no wake-up, no row (`check` compares all of them).
        self.check_revs().await?;
        self.check().await?;
        Ok(minted)
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
        assert_eq!(self.serve_watch.latest(), before);
        let probes = feed::Probes::park(&self.store, &self.objs, before);
        self.backend.commit(tx).await?;
        let stamped = self.oracle.commit(writes);
        probes.check(&self.store, before, stamped).await?;
        self.check_revs().await?;
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
        self.check_revs().await?;
        self.check().await
    }

    /// Both latest revs and both watches are at the oracle's rev.
    async fn check_revs(&self) -> Res<()> {
        let latest = self.oracle.latest_rev();
        assert_eq!(RevSource::latest_rev(&self.store).await?, latest);
        assert_eq!(PartStoreServe::latest_rev(&self.store).await?, latest);
        assert_eq!(
            self.watch.latest(),
            latest,
            "the watch is not at the stamped rev"
        );
        assert_eq!(
            self.serve_watch.latest(),
            latest,
            "the responder's watch is not at the stamped rev"
        );
        Ok(())
    }

    /// Compare the store's whole observable state over the universe with the oracle.
    pub async fn check(&self) -> Res<()> {
        let store = &self.store;
        let oracle = &self.oracle;
        let objs = &self.objs;
        let parts = (0..PARTS).map(part).collect::<Vec<_>>();
        let principals = (0..PRINCIPALS).map(principal).collect::<Vec<_>>();
        let latest = oracle.latest_rev();

        assert_eq!(store.generation().await?, oracle.generation(), "generation");
        assert_eq!(
            store.obj_states(objs).await?,
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
        for limit in [1, 2, usize::MAX] {
            let mut after: Option<ObjKey> = None;
            loop {
                let page = store.partless_objects(limit, after.as_ref()).await?;
                assert_eq!(
                    page,
                    oracle.partless_objects(limit, after.as_ref()),
                    "part-less objects, {limit} at a time after {after:?}"
                );
                match page.last() {
                    Some(last) if page.len() == limit => after = Some(last.clone()),
                    _ => break,
                }
            }
        }
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
                    self.check_leaves(principal, part, since, &ids).await?;
                }
            }
            assert_eq!(
                store.readable_obj_states(principal, objs).await?,
                objs.iter()
                    .map(|obj| oracle.readable_obj_state(principal, obj))
                    .collect::<Vec<_>>(),
                "objects readable by {principal}"
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
            // From 0, from half way, and the empty window at the head: verdicts only.
            for principal in &principals {
                for routes in &route_sets {
                    for after in [0, latest / 2, latest] {
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
        Ok(())
    }

    /// The store's leaves of `part` for `principal` against the oracle's: every bucket of `ids`
    /// whole, the deep ones being where members share a bucket and its (deepest bucket, key)
    /// order shows; then resumed after each bucket's first member; each under no byte budget and
    /// under one that fits two of the universe's one-byte keys and a bit.
    async fn check_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        since: Rev,
        ids: &[BuckId],
    ) -> Res<()> {
        let (store, oracle) = (&self.store, &self.oracle);
        let narrow = 2 * LeafEntry::wire_bytes(1) + 1;
        let bucks = ids.iter().map(|id| (*id, None)).collect::<Vec<_>>();
        let mut whole = None;
        for budget in [usize::MAX, narrow] {
            let expected = oracle.bucket_leaves(principal, part, &bucks, since, usize::MAX, budget);
            assert_eq!(
                store
                    .bucket_leaves(principal, part, &bucks, since, usize::MAX, budget)
                    .await?,
                expected,
                "leaves of {part} for {principal} since {since}, budget {budget}"
            );
            whole = whole.or(expected);
        }
        // And resumed: each bucket after its first member, two at a time.
        if let Some(whole) = whole {
            let resumed = bucks
                .iter()
                .zip(&whole)
                .filter_map(|((id, _), members)| {
                    Some((*id, Some(members.entries.first()?.obj.clone())))
                })
                .collect::<Vec<_>>();
            for (limit, budget) in [(2, usize::MAX), (3, narrow)] {
                assert_eq!(
                    store
                        .bucket_leaves(principal, part, &resumed, since, limit, budget)
                        .await?,
                    oracle.bucket_leaves(principal, part, &resumed, since, limit, budget),
                    "resumed leaves of {part} for {principal} since {since}, {limit} at a time \
                     within {budget} bytes"
                );
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

    let mut stream = RevStream::new(harness.store.clone(), LogSelector::All, 0);
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
        .bucket_leaves(
            &principal(0),
            &part(0),
            &[(BuckId::ROOT, None)],
            0,
            1,
            usize::MAX,
        )
        .await?
        .expect("the part is readable")
        .remove(0)
        .entries
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
                usize::MAX,
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

/// A read of the empty window `(rev, rev]` answers every route's verdict exactly as a read
/// ending at `rev` would, and reads no row: no rows, no payloads, and progress at `rev`. A fresh
/// store answers it too, at rev 0.
pub async fn an_empty_window_reads_verdicts_only<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    let routes = LogRoutes {
        parts: [(part(0), 0), (part(1), 0), (part(2), 0)].into(),
        objects: [(obj(0), 0), (obj(1), 0)].into(),
    };
    let all = limits(usize::MAX, None);
    let fresh = harness
        .read_authorized(&principal(0), &routes, 0, 0, all)
        .await?;
    assert_eq!(
        fresh.parts,
        vec![ReadVerdict::UnknownPart; 3],
        "a fresh store knows no part"
    );
    assert_eq!(fresh.objects, vec![ReadVerdict::Denied; 2]);

    harness
        .commit(vec![
            set(&[(0, 1), (1, 1)]),
            add(&[(0, 0), (1, 1)]),
            grant(&[(0, 0)]),
        ])
        .await?;
    let latest = harness.oracle.latest_rev();
    for principal in [principal(0), principal(1)] {
        let full = harness
            .read_authorized(&principal, &routes, 0, latest, all)
            .await?;
        let empty = harness
            .read_authorized(&principal, &routes, latest, latest, all)
            .await?;
        assert_eq!((&empty.parts, &empty.objects), (&full.parts, &full.objects));
        assert!(empty.rows.entries.is_empty() && empty.payloads.is_empty());
        assert_eq!(empty.rows.through, latest);
    }
    let page = harness
        .read_authorized(&principal(0), &routes, latest, latest, all)
        .await?;
    assert_eq!(
        page.parts,
        vec![
            ReadVerdict::Permitted,
            ReadVerdict::Denied,
            ReadVerdict::UnknownPart
        ]
    );
    assert_eq!(
        page.objects,
        vec![ReadVerdict::Permitted, ReadVerdict::Denied]
    );
    Ok(())
}

/// An authorized read carries the payload of each object with a live row in the page, once per
/// object however many rows name it, and nothing for an object whose rows in the page are all
/// removals. The payload is the one the snapshot holds, and it is the content the rows stand
/// for: a payload change re-stamps the object's rows, so a read ending before the change no
/// longer finds them.
pub async fn authorized_reads_carry_their_payloads<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    harness
        .commit(vec![
            set(&[(0, 1), (1, 1)]),
            add(&[(0, 0), (1, 0)]),
            grant(&[(0, 0), (1, 0)]),
        ])
        .await?;
    // Object 0 joins part 1 at a later rev: two live rows at two revs, one payload.
    harness.commit(vec![add(&[(0, 1)])]).await?;
    harness.commit(vec![remove(&[(1, 0)])]).await?;
    let routes = LogRoutes {
        parts: [(part(0), 0), (part(1), 0)].into(),
        objects: default(),
    };
    let all = limits(usize::MAX, None);
    let latest = harness.oracle.latest_rev();
    let page = harness
        .read_authorized(&principal(0), &routes, 0, latest, all)
        .await?;
    // Two live rows and object 1's removal, which carries no payload.
    assert_eq!(page.rows.entries.len(), 3, "{page:?}");
    assert!(page.rows.entries.iter().any(|entry| entry.value.is_none()));
    assert_eq!(page.payloads, vec![(obj(0), payload(1))]);
    // From rev 1 the page loses only the first live row.
    let page = harness
        .read_authorized(&principal(0), &routes, 1, latest, all)
        .await?;
    assert_eq!(page.rows.entries.len(), 2, "{page:?}");
    assert!(page.rows.entries.iter().any(|entry| entry.value.is_none()));
    assert_eq!(page.payloads, vec![(obj(0), payload(1))]);

    let edited_at = harness
        .commit(vec![set(&[(0, 2)])])
        .await?
        .expect("the edit stamps");
    let before_edit = harness
        .read_authorized(&principal(0), &routes, 0, edited_at - 1, all)
        .await?;
    assert!(
        before_edit.payloads.is_empty(),
        "the edit moved object 0's rows past the window: {before_edit:?}"
    );
    let page = harness
        .read_authorized(&principal(0), &routes, 0, edited_at, all)
        .await?;
    assert_eq!(page.payloads, vec![(obj(0), payload(2))]);
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
    let generation = harness.oracle.generation();
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
            generation,
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

/// A store has a generation from its creation, and no two stores share one. It is stable across
/// writes, rollbacks and reads; [`mint_generation`](PartStoreWrite::mint_generation) replaces
/// it with another and changes nothing else (the log, the revs and the watches stay put); and
/// every part summary carries the current one, a mint included.
pub async fn generations_identify_the_log<B: Backend>(backend: &B) -> Res<()> {
    let mut harness = Harness::open(backend).await?;
    let other = Harness::open(backend).await?;
    let first = harness.oracle.generation();
    assert_ne!(
        first,
        other.oracle.generation(),
        "two stores, one generation"
    );

    let summarize = |harness: &Harness<B>| {
        let store = harness.store.clone();
        async move {
            store
                .part_summaries(&principal(0), &[(part(0), 0)])
                .await
                .map(|mut summaries| summaries.remove(0).expect("the part is readable"))
        }
    };
    harness
        .commit(vec![set(&[(0, 1)]), add(&[(0, 0)]), grant(&[(0, 0)])])
        .await?;
    harness
        .rollback(vec![set(&[(1, 1)]), add(&[(1, 0)])])
        .await?;
    harness.commit(vec![remove(&[(0, 0)])]).await?;
    assert_eq!(harness.store.generation().await?, first, "writes keep it");
    assert_eq!(summarize(&harness).await?.generation, first);

    let second = harness.mint_generation().await?;
    assert_ne!(second, first);
    assert_eq!(harness.store.generation().await?, second);
    assert_eq!(summarize(&harness).await?.generation, second);
    // The log carries on from where it was, under the new generation.
    harness.commit(vec![set(&[(0, 2)]), add(&[(0, 0)])]).await?;
    assert_eq!(harness.store.generation().await?, second);
    assert_eq!(summarize(&harness).await?.generation, second);
    let third = harness.mint_generation().await?;
    assert!(
        third != second && third != first,
        "a mint repeated an old generation"
    );
    // The other store never moved.
    assert_eq!(other.store.generation().await?, other.oracle.generation());
    Ok(())
}

/// A route hands out every removal above its bound, whatever rev the membership went live at:
/// a reader's persisted cursor only passes settled work, so it may hold a membership that went
/// live above that cursor, and skipping the removal would leave it there for good. The route
/// bound and the read's `after` still cut by rev, and `All` keeps every removal.
pub async fn removals_reach_every_route_above_its_bound<B: Backend>(backend: &B) -> Res<()> {
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
    // A reader at 0 never saw either membership and still hears both removals.
    assert_eq!(
        removed(harness.read(&route(0), 0, 4, all).await?),
        vec![(obj(0), 2), (obj(1), 4)]
    );
    // A reader at 2 gets the removal of object 1, which went live above its cursor.
    assert_eq!(
        removed(harness.read(&route(0), 2, 4, all).await?),
        vec![(obj(1), 4)]
    );
    // A route bound cuts by rev like the read's `after`.
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
        vec![(obj(1), 4)]
    );
    // Object 2 goes live at 5, leaves at 6, comes back at 7 and leaves at 8; the log keeps only
    // the removal at 8, which every reader below it gets.
    harness.commit(vec![set(&[(2, 1)]), add(&[(2, 0)])]).await?;
    harness.commit(vec![remove(&[(2, 0)])]).await?;
    harness.commit(vec![add(&[(2, 0)])]).await?;
    harness.commit(vec![remove(&[(2, 0)])]).await?;
    for after in [4, 5, 7] {
        assert_eq!(
            removed(harness.read(&route(0), after, 8, all).await?),
            vec![(obj(2), 8)]
        );
    }
    // A full replay keeps every removal too.
    assert_eq!(
        removed(harness.read(&LogSelector::All, 0, 8, all).await?),
        vec![(obj(0), 2), (obj(1), 4), (obj(2), 8)]
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
    let mut watch = RevSource::watch(&harness.store);
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
    assert_eq!(RevSource::latest_rev(&harness.store).await?, 1);
    // Unchanged is judged by canonical content: `5.0` is the payload `5` is.
    let as_payload = |value| PartWrite::SetPayloads(vec![(obj(0), value)]);
    let stamped = harness
        .commit(vec![as_payload(serde_json::json!(5))])
        .await?;
    assert_eq!(stamped, Some(2));
    assert_eq!(
        harness
            .commit(vec![as_payload(serde_json::json!(5.0))])
            .await?,
        None,
        "a payload equal in canonical form is unchanged"
    );
    Ok(())
}

strike! {
    #[structstruck::each[derive(Debug, Clone)]]
    /// One step of the differential run.
    pub enum PartOp {
        Commit(Vec<PartWrite>),
        Rollback(Vec<PartWrite>),
        /// An admin mint of a new generation.
        MintGeneration,
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
    differential_over(backend, (0..OBJS).map(obj).collect(), ops).await
}

/// [`differential`] with the checks covering `objs`, the universe `ops` draws from.
async fn differential_over<B: Backend>(
    backend: &B,
    objs: Vec<ObjKey>,
    ops: Vec<PartOp>,
) -> Res<()> {
    let mut harness = Harness::open_over(backend, objs).await?;
    for op in ops {
        match op {
            // generated drops of members' payloads are caller bugs (law 2): not issued
            PartOp::Commit(writes) => {
                let writes = harness.oracle.without_member_drops(writes);
                harness.commit(writes).await?;
            }
            PartOp::Rollback(writes) => {
                let writes = harness.oracle.without_member_drops(writes);
                harness.rollback(writes).await?
            }
            PartOp::MintGeneration => {
                harness.mint_generation().await?;
            }
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
                after: after_at,
                until: until_at,
                limits,
            } => {
                // A zero `until` fraction reads the empty window at `after`: verdicts only.
                let (after, until) = match window(harness.oracle.latest_rev(), after_at, until_at) {
                    Some((after, _)) if until_at == 0 => (after, after),
                    Some(window) => window,
                    None => (0, 0),
                };
                harness
                    .read_authorized(&principal, &routes, after, until, limits)
                    .await?;
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
