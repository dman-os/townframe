//! In-memory part store.
//!
//! One `surelock` mutex over one relational state: plain `BTreeMap` tables, shaped like the
//! SQLite tables they stand for, and the log as an embedded [`OkvTable`]. Every operation is one
//! synchronous critical section: a read copies its answer out, a commit applies every write of
//! the transaction and stamps the log in one go. Nothing holds the lock across an await.
//!
//! A [`MemoryPartTx`] only stages its batches; it takes no lock and reads nothing, so it cannot
//! conflict. Its commit applies the batches in the order they were staged, to the state as it is
//! at commit, and then (after releasing the lock) moves the log's watch if any row was stamped.

use tokio::sync::watch;

use crate::interlude::*;
use crate::okv::memory::OkvTable;
use crate::part_store::AccessLevel;
use crate::part_store::AuthorizedPage;
use crate::part_store::LeafEntry;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::LogRoutes;
use crate::part_store::LogSelector;
use crate::part_store::ObjPayload;
use crate::part_store::ObjState;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::PartSummary;
use crate::part_store::PayloadDigest;
use crate::part_store::bucket_summary::BucketSummary;
use crate::part_store::bucket_summary::BucketSummaryState;
use crate::part_store::log_row_bytes;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::rev::RevWatch;

mod access;
mod buckets;
mod objects;

strike! {
    /// The state behind the lock. The log is the truth for membership and access; `members`
    /// and `buckets` are indexes over it, written by the same functions that write the log.
    #[derive(Default)]
    struct State {
        /// `Member` and `Access` rows, stamped with the global cursor.
        log: OkvTable<LogKey, LogEntry>,
        objs: BTreeMap<ObjKey, struct ObjRow {
            payload: ObjPayload,
            /// Computed when the payload was written.
            digest: PayloadDigest,
        }>,
        parts: BTreeMap<PartKey,
            #[derive(Default)]
            struct PartRow {
                /// The rev of the part's latest member row; 0 for none. Access rows do not move
                /// it (see `PartSummary::latest_cursor`).
                latest_cursor: Rev,
            }
        >,
        /// Every object with a member row in a part, live or removed, keyed by the part and
        /// the object's deepest bucket index so a bucket's members are one range.
        members: BTreeSet<(PartKey, u16, ObjKey)>,
        /// Memberships added before the object had a payload: no log row, no bucket entry.
        pending: BTreeSet<(ObjKey, PartKey)>,
        /// Each part's bucket summaries, every level, maintained per transition.
        buckets: BTreeMap<(PartKey, BuckId), BucketSummaryState>,
        peer_cursors: BTreeMap<(PeerKey, PartKey), Rev>,
        /// Scratch for an object's parts while its rows are rewritten.
        part_buf: Vec<PartKey>,
    }
}

/// The in-memory part store. Clones share the store.
#[derive(Clone)]
pub struct MemoryPartStore {
    state: Arc<surelock::mutex::Mutex<State>>,
    /// The log's watch: moved after each stamping commit, outside the lock.
    wakeups: watch::Sender<Rev>,
}

impl Default for MemoryPartStore {
    fn default() -> Self {
        let (wakeups, _) = watch::channel(0);
        Self {
            state: Arc::new(surelock::mutex::Mutex::new(default())),
            wakeups,
        }
    }
}

/// One staged batch.
enum Staged {
    EnsureParts(Vec<PartKey>),
    /// Each payload with its digest, computed while staging so the hashing stays outside the
    /// lock and a payload with no canonical form is refused before anything is staged.
    SetPayloads(Vec<(ObjKey, ObjPayload, PayloadDigest)>),
    AddMembers(Vec<(ObjKey, PartKey)>),
    RemoveMembers(Vec<(ObjKey, PartKey)>),
    GrantAccess(Vec<(PartKey, PeerKey, AccessLevel)>),
    RevokeAccess(Vec<(PartKey, PeerKey)>),
}

/// A transaction on one [`MemoryPartStore`]: staged batches, applied together at
/// [`commit`](Self::commit). Dropping it rolls it back.
pub struct MemoryPartTx {
    store: MemoryPartStore,
    staged: Vec<Staged>,
}

impl MemoryPartStore {
    /// Start a transaction. It takes no lock.
    pub fn begin(&self) -> MemoryPartTx {
        MemoryPartTx {
            store: self.clone(),
            staged: default(),
        }
    }

    fn with_state<R>(&self, inspect: impl FnOnce(&State) -> R) -> R {
        surelock::key::lock_scope(|key| {
            let (state, _key) = key.lock(&self.state);
            inspect(&state)
        })
    }

    fn stage(&self, tx: &mut MemoryPartTx, staged: Staged) {
        assert!(
            Arc::ptr_eq(&tx.store.state, &self.state),
            "a transaction was used on another part store"
        );
        tx.staged.push(staged);
    }
}

impl MemoryPartTx {
    /// Apply every staged batch at once. Returns the rev that stamped the changed log rows, or
    /// `None` when no row changed (and then no rev is consumed and the watch stays put).
    pub fn commit(self) -> Option<Rev> {
        let Self { store, staged } = self;
        let stamped = surelock::key::lock_scope(|key| {
            let (mut state, _key) = key.lock(&store.state);
            let rev = state.log.rev() + 1;
            let mut stamped = false;
            for batch in staged {
                stamped |= state.apply(rev, batch);
            }
            if stamped {
                state.log.publish(rev);
            }
            stamped.then_some(rev)
        });
        // After the lock is released: a woken reader must find the commit in place.
        if let Some(rev) = stamped {
            store.wakeups.send_replace(rev);
        }
        stamped
    }
}

impl State {
    /// Apply one batch at `rev`. Returns whether it wrote any log row.
    fn apply(&mut self, rev: Rev, batch: Staged) -> bool {
        match batch {
            Staged::EnsureParts(parts) => {
                for part in parts {
                    self.parts.entry(part).or_default();
                }
                false
            }
            Staged::SetPayloads(payloads) => self.set_payloads(rev, payloads),
            Staged::AddMembers(members) => self.add_members(rev, &members),
            Staged::RemoveMembers(members) => self.remove_members(rev, &members),
            Staged::GrantAccess(grants) => self.grant_access(rev, &grants),
            Staged::RevokeAccess(revokes) => self.revoke_access(rev, &revokes),
        }
    }

    /// Write one log row at `rev`. A member row moves its part's cursor; any row creates the
    /// part row if the part was not known.
    fn stamp(&mut self, rev: Rev, key: LogKey, value: Option<LogEntry>) {
        let (part, is_member) = match &key {
            LogKey::Member { part, .. } => (part, true),
            LogKey::Access { part, .. } => (part, false),
        };
        let row = match self.parts.get_mut(part) {
            Some(row) => row,
            None => self.parts.entry(part.clone()).or_default(),
        };
        if is_member {
            row.latest_cursor = rev;
        }
        self.log.write(rev, key, value);
    }
}

// Every operation is one synchronous critical section, so the memory store does its work when
// called and hands back a ready future. Staging only records the batch.
impl PartStoreWrite for MemoryPartStore {
    type Tx = MemoryPartTx;

    fn ensure_parts(
        &self,
        tx: &mut MemoryPartTx,
        parts: &[PartKey],
    ) -> impl Future<Output = Res<()>> + Send {
        self.stage(tx, Staged::EnsureParts(parts.to_vec()));
        std::future::ready(Ok(()))
    }

    fn set_payloads(
        &self,
        tx: &mut MemoryPartTx,
        payloads: Vec<(ObjKey, ObjPayload)>,
    ) -> impl Future<Output = Res<()>> + Send {
        let digested = payloads
            .into_iter()
            .map(|(obj, payload)| {
                let digest = PayloadDigest::of(&payload)?;
                Ok((obj, payload, digest))
            })
            .collect::<Res<Vec<_>>>();
        std::future::ready(digested.map(|payloads| self.stage(tx, Staged::SetPayloads(payloads))))
    }

    fn add_members(
        &self,
        tx: &mut MemoryPartTx,
        members: &[(ObjKey, PartKey)],
    ) -> impl Future<Output = Res<()>> + Send {
        self.stage(tx, Staged::AddMembers(members.to_vec()));
        std::future::ready(Ok(()))
    }

    fn remove_members(
        &self,
        tx: &mut MemoryPartTx,
        members: &[(ObjKey, PartKey)],
    ) -> impl Future<Output = Res<()>> + Send {
        self.stage(tx, Staged::RemoveMembers(members.to_vec()));
        std::future::ready(Ok(()))
    }

    fn grant_access(
        &self,
        tx: &mut MemoryPartTx,
        grants: &[(PartKey, PeerKey, AccessLevel)],
    ) -> impl Future<Output = Res<()>> + Send {
        self.stage(tx, Staged::GrantAccess(grants.to_vec()));
        std::future::ready(Ok(()))
    }

    fn revoke_access(
        &self,
        tx: &mut MemoryPartTx,
        revokes: &[(PartKey, PeerKey)],
    ) -> impl Future<Output = Res<()>> + Send {
        self.stage(tx, Staged::RevokeAccess(revokes.to_vec()));
        std::future::ready(Ok(()))
    }
}

impl PartStoreRead for MemoryPartStore {
    fn obj_states(
        &self,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send {
        std::future::ready(Ok(
            self.with_state(|state| objs.iter().map(|obj| state.obj_state(obj)).collect())
        ))
    }

    fn bucket_summaries(
        &self,
        part: &PartKey,
        ids: &[BuckId],
    ) -> impl Future<Output = Res<Vec<BucketSummary>>> + Send {
        std::future::ready(Ok(
            self.with_state(|state| state.bucket_summaries(part, ids))
        ))
    }

    fn member_count(&self, part: &PartKey) -> impl Future<Output = Res<u64>> + Send {
        std::future::ready(Ok(self.with_state(|state| state.member_count(part))))
    }

    fn peer_part_cursors(
        &self,
        peer: &PeerKey,
        parts: &[PartKey],
    ) -> impl Future<Output = Res<Vec<Option<Rev>>>> + Send {
        std::future::ready(Ok(self.with_state(|state| {
            parts
                .iter()
                .map(|part| {
                    state
                        .peer_cursors
                        .get(&(peer.clone(), part.clone()))
                        .copied()
                })
                .collect()
        })))
    }

    fn set_peer_part_cursors(
        &self,
        cursors: &[(PeerKey, PartKey, Rev)],
    ) -> impl Future<Output = Res<()>> + Send {
        surelock::key::lock_scope(|key| {
            let (mut state, _key) = key.lock(&self.state);
            for (peer, part, cursor) in cursors {
                let stored = state
                    .peer_cursors
                    .entry((peer.clone(), part.clone()))
                    .or_default();
                *stored = (*stored).max(*cursor);
            }
        });
        std::future::ready(Ok(()))
    }
}

impl RevSource for MemoryPartStore {
    type Key = LogKey;
    type Value = LogEntry;
    type Selector = LogSelector;

    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send {
        std::future::ready(Ok(self.with_state(|state| state.log.rev())))
    }

    /// Copies one bounded page of the log out under the lock, applying the route rule for
    /// removals with the `added_at` the members table keeps.
    ///
    /// Unauthorized: every selected row, whoever asks. For the store's local consumers; a
    /// responder serves a peer through [`PartStoreServe::read_authorized`].
    fn read(
        &self,
        selector: &LogSelector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut Page<LogKey, LogEntry>,
    ) -> impl Future<Output = Res<()>> + Send {
        self.with_state(|state| {
            state.log.read_into(
                |key, rev, _value| selector.selects(key, rev),
                log_row_bytes,
                after,
                until,
                limits,
                page,
            );
        });
        std::future::ready(Ok(()))
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }
}

impl PartStoreServe for MemoryPartStore {
    fn latest_rev(&self) -> impl Future<Output = Res<Rev>> + Send {
        std::future::ready(Ok(self.with_state(|state| state.log.rev())))
    }

    /// The log's own watch: the one [`RevSource::watch`] hands out.
    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }

    /// Decides every route and copies the rows and their payloads out in one critical section,
    /// so a commit (a revocation included) lands either wholly before the read or wholly after
    /// it.
    fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut AuthorizedPage,
    ) -> impl Future<Output = Res<()>> + Send {
        self.with_state(|state| {
            state.read_authorized(principal, routes, after, until, limits, page);
        });
        std::future::ready(Ok(()))
    }

    fn part_summaries(
        &self,
        principal: &PeerKey,
        parts: &[(PartKey, Rev)],
    ) -> impl Future<Output = Res<Vec<Option<PartSummary>>>> + Send {
        std::future::ready(Ok(self.with_state(|state| {
            parts
                .iter()
                .map(|(part, since)| {
                    state
                        .permits(principal, part)
                        .then(|| state.part_summary(principal, part, *since))
                })
                .collect()
        })))
    }

    fn readable_obj_states(
        &self,
        principal: &PeerKey,
        objs: &[ObjKey],
    ) -> impl Future<Output = Res<Vec<Option<ObjState>>>> + Send {
        std::future::ready(Ok(self.with_state(|state| {
            objs.iter()
                .map(|obj| state.readable_obj_state(principal, obj))
                .collect()
        })))
    }

    fn bucket_list(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<BucketSummary>>>> + Send {
        std::future::ready(Ok(self.with_state(|state| {
            state
                .permits(principal, part)
                .then(|| state.bucket_list(part, from, to_level, since, limit))
        })))
    }

    fn bucket_leaves(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        bucks: &[(BuckId, Option<ObjKey>)],
        since: Rev,
        limit: usize,
    ) -> impl Future<Output = Res<Option<Vec<Vec<LeafEntry>>>>> + Send {
        std::future::ready(Ok(self.with_state(|state| {
            state.permits(principal, part).then(|| {
                bucks
                    .iter()
                    .map(|(buck, after)| {
                        state.bucket_leaves(part, *buck, after.as_ref(), since, limit)
                    })
                    .collect()
            })
        })))
    }
}
