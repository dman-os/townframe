//! The peer: the ground truth of the simulation. A memory part store with a twin oracle, the
//! replay server and responder answering sessions and rounds, and summaries, bucket lists and
//! leaves answered from [`PartStoreServe`] for our principal.
//!
//! After every commit it snapshots the membership our principal may read, keyed by rev,
//! because the oracle has no view at a rev: the soundness of `Synced` is judged against it.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use futures::executor::block_on;

use crate::interlude::*;

use crate::fingerprint::FingerprintSeed;
use crate::part_store::Generation;
use crate::part_store::ObjPayload;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::PartSummary;
use crate::part_store::bucket_summary::BucketSummary;
use crate::part_store::laws;
use crate::part_store::laws::principal;
use crate::part_store::memory::MemoryPartStore;
use crate::part_store::oracle::PartOracle;
use crate::part_store::oracle::PartWrite;
use crate::replay::responder::Responder;
use crate::replay::responder::Timer;
use crate::replay::server::ReplayServer;
use crate::replay::wire::PageSession;
use crate::replay::wire::Route;
use crate::replay::wire::SessionError;
use crate::replay::wire::SessionPage;
use crate::replay::wire::SessionUpdated;
use crate::replay::wire::UpdateSession;
use crate::rev::Rev;
use crate::sync::bucket::BucketMachine;
use crate::sync::peer::SummaryReply;
use crate::sync::rpc::bucket::BucketObjPageEntry;
use crate::sync::rpc::bucket::LeafBucketPage;
use crate::sync::rpc::bucket::LeafBucketRequest;

use crate::kit::scheduler::Instant;

/// Our principal at the peer.
pub(super) fn me() -> PeerKey {
    principal(0)
}

/// A timer whose sleeps are over as soon as they start: a held round with no rows answers
/// after one wait on the watch.
#[derive(Clone, Copy)]
pub(super) struct Expired;

impl Timer for Expired {
    fn sleep(&self, _: Duration) -> impl Future<Output = ()> + Send + use<> {
        std::future::ready(())
    }
}

/// The peer's membership as of one rev. Membership is access-agnostic; `readable` is the
/// parts our principal holds a grant on.
#[derive(Debug, Clone, Default)]
pub(super) struct View {
    pub members: BTreeMap<PartKey, BTreeSet<ObjKey>>,
    pub readable: BTreeSet<PartKey>,
}

pub(super) struct Remote {
    store: MemoryPartStore,
    oracle: PartOracle,
    responder: Responder<MemoryPartStore, Expired>,
    server: ReplayServer,
    parts: Vec<PartKey>,
    /// The view after each commit that stamped a row; absent revs read as the last one below.
    views: BTreeMap<Rev, View>,
    /// The rev the log row of `(obj, part)` was last stamped at: a change of its membership,
    /// or of the payload of a live member (which re-stamps every row of the object). Rows
    /// collapse to their last stamp, so a replay from below it delivers the row at this rev.
    row_rev: BTreeMap<(ObjKey, PartKey), Rev>,
    objs: Vec<ObjKey>,
    payloads: BTreeMap<ObjKey, Option<ObjPayload>>,
    /// Summaries claim every part far behind, so the machine walks instead of replaying.
    pub walk_bias: bool,
    /// The most parts one summary call may name.
    pub summary_cap: usize,
    /// A backup of the peer taken by [`Self::snapshot`]: its files, generation included.
    saved: Option<Saved>,
    /// An unminted restore happened: a cursor above the head is a stale one of the old log,
    /// which the part's restart replaces, not a bug.
    pub unminted: bool,
    /// Members the peer had in a part before it lost its data or went back to a backup, and
    /// that the new log does not mention: a pull-only client keeps them, because nothing
    /// removes a row the peer has no tombstone for.
    ghosts: BTreeMap<PartKey, BTreeSet<ObjKey>>,
}

/// A copy of everything that is the peer's data, as a backup holds it.
struct Saved {
    store: MemoryPartStore,
    oracle: PartOracle,
    views: BTreeMap<Rev, View>,
    row_rev: BTreeMap<(ObjKey, PartKey), Rev>,
    payloads: BTreeMap<ObjKey, Option<ObjPayload>>,
}

impl Remote {
    pub(super) fn new(parts: &[PartKey], objs: &[ObjKey], summary_cap: usize) -> Self {
        let store = MemoryPartStore::default();
        Self {
            responder: Responder::new(store.clone(), Expired),
            oracle: PartOracle::with_generation(block_on(store.generation()).unwrap()),
            store,
            server: default(),
            parts: parts.to_vec(),
            views: Self::blank_views(parts),
            row_rev: default(),
            objs: objs.to_vec(),
            payloads: default(),
            walk_bias: false,
            summary_cap,
            saved: None,
            unminted: false,
            ghosts: default(),
        }
    }

    fn blank_views(parts: &[PartKey]) -> BTreeMap<Rev, View> {
        [(
            0,
            View {
                members: parts.iter().map(|part| (part.clone(), default())).collect(),
                readable: default(),
            },
        )]
        .into()
    }

    pub(super) fn generation(&self) -> Generation {
        block_on(self.store.generation()).unwrap()
    }

    /// The peer's server restarts around `store`: its sessions are gone and its responder
    /// serves the new files.
    fn serve(&mut self, store: MemoryPartStore) {
        self.responder = Responder::new(store.clone(), Expired);
        self.store = store;
        self.server = default();
    }

    /// Remember the members the peer is about to stop having.
    fn leave_ghosts(&mut self) {
        for (obj, part) in self.oracle.live_members() {
            self.ghosts.entry(part).or_default().insert(obj);
        }
    }

    /// The peer loses its data: a new store, empty, with a new generation, and no memory of
    /// the old log.
    pub(super) fn lose_data(&mut self) {
        self.leave_ghosts();
        let store = MemoryPartStore::default();
        self.oracle = PartOracle::with_generation(block_on(store.generation()).unwrap());
        self.views = Self::blank_views(&self.parts);
        self.row_rev = default();
        self.payloads = default();
        self.serve(store);
    }

    /// Take a backup of the peer's files, replacing the last one.
    pub(super) fn snapshot(&mut self) {
        self.saved = Some(Saved {
            store: self.store.snapshot(),
            oracle: self.oracle.clone(),
            views: self.views.clone(),
            row_rev: self.row_rev.clone(),
            payloads: self.payloads.clone(),
        });
    }

    /// The head of the backup, if one was taken.
    pub(super) fn saved_head(&self) -> Option<Rev> {
        self.saved.as_ref().map(|saved| saved.oracle.latest_rev())
    }

    /// The generation of the backup's files, if one was taken.
    pub(super) fn saved_generation(&self) -> Option<Generation> {
        self.saved.as_ref().map(|saved| saved.oracle.generation())
    }

    /// Restore the backup's files over the peer's, which brings back the generation they were
    /// taken under; `mint` then tells the store (the operator's step after a restore). Returns
    /// false when there is no backup. The backup stays for another restore.
    pub(super) fn restore(&mut self, mint: bool) -> bool {
        let Some(saved) = &self.saved else {
            return false;
        };
        let (store, oracle) = (saved.store.snapshot(), saved.oracle.clone());
        let (views, row_rev, payloads) = (
            saved.views.clone(),
            saved.row_rev.clone(),
            saved.payloads.clone(),
        );
        self.leave_ghosts();
        self.oracle = oracle;
        self.views = views;
        self.row_rev = row_rev;
        self.payloads = payloads;
        self.serve(store);
        self.unminted |= !mint;
        if mint {
            let minted = block_on(self.store.mint_generation()).unwrap();
            self.oracle.set_generation(minted);
        }
        true
    }

    /// Whether `local` is what a client that pulled from this peer holds of `part`, given
    /// `expected`, the members the peer's log says it has: all of them, and beyond them only
    /// ghosts of a previous log that this log has no row for.
    pub(super) fn holds(
        &self,
        part: &PartKey,
        local: &BTreeSet<ObjKey>,
        expected: &BTreeSet<ObjKey>,
    ) -> bool {
        expected.is_subset(local)
            && local
                .difference(expected)
                .all(|obj| self.is_ghost(part, obj))
    }

    /// A member of an earlier log that this one has no row for.
    pub(super) fn is_ghost(&self, part: &PartKey, obj: &ObjKey) -> bool {
        self.row_rev(obj, part) == 0
            && self
                .ghosts
                .get(part)
                .is_some_and(|ghosts| ghosts.contains(obj))
    }

    pub(super) fn head(&self) -> Rev {
        self.oracle.latest_rev()
    }

    /// Commit `writes` as one transaction to the store and the oracle alike, so they share one
    /// rev, and snapshot what we may read.
    pub(super) fn commit(&mut self, writes: Vec<PartWrite>) -> Option<Rev> {
        let mut tx = self.store.begin();
        for write in writes.iter().cloned() {
            block_on(laws::stage(&self.store, &mut tx, write)).unwrap();
        }
        let rev = tx.commit();
        assert_eq!(rev, self.oracle.commit(writes));
        if let Some(rev) = rev {
            let live = self.oracle.live_members();
            let mut view = View {
                readable: self
                    .parts
                    .iter()
                    .filter(|part| self.oracle.grants().contains_key(&((*part).clone(), me())))
                    .cloned()
                    .collect(),
                ..default()
            };
            for part in &self.parts {
                let members: BTreeSet<ObjKey> = live
                    .iter()
                    .filter(|(_, member_part)| member_part == part)
                    .map(|(obj, _)| obj.clone())
                    .collect();
                // `views` has no entry at `rev` yet: this is the view before the commit.
                let before = self.view_at(rev).members[part].clone();
                for obj in members.symmetric_difference(&before) {
                    self.row_rev.insert((obj.clone(), part.clone()), rev);
                }
                for obj in members.intersection(&before) {
                    if self.payloads.get(obj) != Some(&self.oracle_payload(obj)) {
                        self.row_rev.insert((obj.clone(), part.clone()), rev);
                    }
                }
                view.members.insert(part.clone(), members);
            }
            self.payloads = self
                .objs
                .iter()
                .map(|obj| (obj.clone(), self.oracle_payload(obj)))
                .collect();
            self.views.insert(rev, view);
        }
        rev
    }

    /// The view as of `rev`: the snapshot of the last commit at or below it.
    pub(super) fn view_at(&self, rev: Rev) -> &View {
        self.views
            .range(..=rev)
            .next_back()
            .map(|(_, view)| view)
            .expect("the view at rev 0 always exists")
    }

    /// Every view from `rev` on: the one in force at it, then each later commit's.
    pub(super) fn views_from(&self, rev: Rev) -> impl Iterator<Item = &View> {
        std::iter::once(self.view_at(rev)).chain(self.views.range(rev + 1..).map(|(_, view)| view))
    }

    /// The rev the log row of `(obj, part)` was last stamped at; 0 when it has none.
    pub(super) fn row_rev(&self, obj: &ObjKey, part: &PartKey) -> Rev {
        self.row_rev
            .get(&(obj.clone(), part.clone()))
            .copied()
            .unwrap_or(0)
    }

    /// Whether we may read `route` now: a part we hold a grant on, or an object live in one.
    pub(super) fn readable(&self, route: &Route) -> bool {
        match route {
            Route::Part { part_key } => {
                self.oracle.grants().contains_key(&(part_key.clone(), me()))
            }
            Route::Object { obj_key } => self.oracle.readable_obj_state(&me(), obj_key).is_some(),
        }
    }

    /// The object's payload now, whoever may read it.
    pub(super) fn payload(&self, obj: &ObjKey) -> Option<ObjPayload> {
        self.oracle_payload(obj)
    }

    fn oracle_payload(&self, obj: &ObjKey) -> Option<ObjPayload> {
        self.oracle
            .obj_state(obj)?
            .content
            .map(|(payload, _)| payload)
    }

    pub(super) fn forget_sessions(&mut self) {
        self.server = default();
    }

    pub(super) fn sweep(&mut self, now: Instant) {
        self.server.sweep(now);
    }

    pub(super) fn close(&mut self, session_id: crate::replay::wire::SessionId) {
        self.server
            .close(&me(), &crate::replay::wire::CloseSession { session_id });
    }

    // # Serving

    pub(super) fn summary(&mut self, parts: &Map<PartKey, Rev>) -> SummaryReply {
        if parts.len() > self.summary_cap {
            return SummaryReply::TooManyParts {
                cap: self.summary_cap,
            };
        }
        let mut asks: Vec<(PartKey, Rev)> = parts.iter().map(|(p, r)| (p.clone(), *r)).collect();
        asks.sort();
        let answers = block_on(self.store.part_summaries(&me(), &asks)).unwrap();
        SummaryReply::Summaries(
            asks.into_iter()
                .zip(answers)
                .filter_map(|((part, _), summary)| {
                    let mut summary: PartSummary = summary?;
                    if self.walk_bias {
                        summary.dirty.member_changes += 1000;
                    }
                    Some((part, summary))
                })
                .collect(),
        )
    }

    pub(super) fn update(
        &mut self,
        request: &UpdateSession,
        now: Instant,
    ) -> Result<SessionUpdated, SessionError> {
        let admits = block_on(self.responder.admission(&me(), request)).unwrap();
        self.server.update(&me(), request, now, admits)
    }

    pub(super) fn round(
        &mut self,
        request: &PageSession,
        now: Instant,
    ) -> Result<SessionPage, SessionError> {
        let Self {
            responder, server, ..
        } = self;
        block_on(responder.page(&me(), |round| {
            server.resolve_round(&me(), request, now, round)
        }))
        .expect("the memory store does not fail")
        .cloned()
    }

    /// The part's changed buckets; `None` when we may not read it.
    pub(super) fn list(
        &self,
        part: &PartKey,
        offset: BuckId,
        working_level: BuckLevel,
        since: Rev,
    ) -> Option<Vec<BucketSummary>> {
        block_on(self.store.bucket_list(
            &me(),
            part,
            offset,
            working_level,
            since,
            BucketMachine::GET_BUCKET_LIMIT_HINT as usize,
        ))
        .unwrap()
    }

    /// One page per requested bucket, `page` entries at most; `None` when we may not read the
    /// part. As the wire says, `next_after` is `None` exactly when the page finishes its bucket,
    /// which one entry past the page tells.
    pub(super) fn leaves(
        &self,
        part: &PartKey,
        requests: &[LeafBucketRequest],
        since: Rev,
        seed: &FingerprintSeed,
        page: usize,
    ) -> Option<Map<BuckId, LeafBucketPage>> {
        let asks: Vec<_> = requests
            .iter()
            .map(|req| (req.buck_id, req.after.clone()))
            .collect();
        let answers = block_on(
            self.store
                .bucket_leaves(&me(), part, &asks, since, page + 1),
        )
        .unwrap()?;
        Some(
            requests
                .iter()
                .zip(answers)
                .map(|(req, mut entries)| {
                    let done = entries.len() <= page;
                    entries.truncate(page);
                    let next_after = (!done).then(|| entries[page - 1].obj.clone());
                    let entries = entries
                        .into_iter()
                        .map(|entry| BucketObjPageEntry {
                            fp: entry.live.map(|digest| {
                                BucketObjPageEntry::fingerprint(seed, &entry.obj, digest)
                            }),
                            obj_key: entry.obj,
                        })
                        .collect();
                    (
                        req.buck_id,
                        LeafBucketPage {
                            after: req.after.clone(),
                            entries,
                            next_after,
                        },
                    )
                })
                .collect(),
        )
    }
}
