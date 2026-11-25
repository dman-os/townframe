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

use future_form::Local;
use future_form::Sendable;
use future_form::future_form;
use tokio::sync::watch;

use crate::interlude::*;
use crate::okv::memory::OkvTable;
use crate::part_store::AccessLevel;
use crate::part_store::LeafEntry;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::LogSelector;
use crate::part_store::ObjState;
use crate::part_store::PartDirtyCount;
use crate::part_store::PartStoreRead;
use crate::part_store::PartStoreServe;
use crate::part_store::PartStoreWrite;
use crate::part_store::PayloadDigest;
use crate::part_store::PayloadState;
use crate::part_store::ReadVerdict;
use crate::part_store::log_row_bytes;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevSource;
use crate::rev::RevWatch;
use crate::store::bucket::BucketSummary;
use crate::store::bucket::BucketSummaryState;

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
            payload: PayloadState,
            /// Computed when the payload was written.
            digest: PayloadDigest,
        }>,
        parts: BTreeMap<PartKey,
            #[derive(Default)]
            struct PartRow {
                /// The rev of the part's latest log row (member or access); 0 for none.
                latest_cursor: Rev,
            }
        >,
        /// Every object with a member row in a part, live or removed, keyed by the part and
        /// the object's deepest bucket index so a bucket's members are one range.
        members: BTreeMap<(PartKey, u16, ObjKey), struct MemberRow {
            /// When the membership first went live; a re-add does not reset it. Kept when it
            /// is removed: the log's tombstone carries no value, and a route read needs it to
            /// skip the removal for a reader that never saw the membership ([`LogSelector`]).
            first_added_at: Rev,
        }>,
        /// Memberships added before the object had a payload state: no log row, no bucket
        /// entry.
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
    SetPayloads(Vec<(ObjKey, PayloadState, PayloadDigest)>),
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

    /// Write one log row at `rev` and move its part's cursor. The part row is created if the
    /// part was not known.
    fn stamp(&mut self, rev: Rev, key: LogKey, value: Option<LogEntry>) {
        let part = match &key {
            LogKey::Member { part, .. } | LogKey::Access { part, .. } => part,
        };
        match self.parts.get_mut(part) {
            Some(row) => row.latest_cursor = rev,
            None => {
                self.parts
                    .insert(part.clone(), PartRow { latest_cursor: rev });
            }
        }
        self.log.write(rev, key, value);
    }
}

impl State {
    /// For a removed membership's row, when it had gone live: what the route rule needs.
    fn removed_added_at(&self, key: &LogKey, value: Option<&LogEntry>) -> Option<Rev> {
        match (key, value) {
            (LogKey::Member { obj, part }, None) => {
                let index = BuckId::deepest_from_obj_key(obj).index();
                let row = self
                    .members
                    .get(&(part.clone(), index, obj.clone()))
                    .expect("a removed member row without its member");
                Some(row.first_added_at)
            }
            (LogKey::Member { .. }, Some(_)) | (LogKey::Access { .. }, _) => None,
        }
    }
}

// Every operation is one synchronous critical section, so the memory store does its work when
// called and hands back a ready future. Staging only records the batch.
#[future_form(Sendable, Local)]
impl<Async: FutureForm> PartStoreWrite<Async> for MemoryPartStore {
    type Tx = MemoryPartTx;

    fn ensure_parts<'a>(
        &'a self,
        tx: &'a mut MemoryPartTx,
        parts: &'a [PartKey],
    ) -> Async::Future<'a, Res<()>> {
        self.stage(tx, Staged::EnsureParts(parts.to_vec()));
        Async::ready(Ok(()))
    }

    fn set_payloads<'a>(
        &'a self,
        tx: &'a mut MemoryPartTx,
        payloads: Vec<(ObjKey, PayloadState)>,
    ) -> Async::Future<'a, Res<()>> {
        let digested = payloads
            .into_iter()
            .map(|(obj, payload)| {
                let digest = payload.digest()?;
                Ok((obj, payload, digest))
            })
            .collect::<Res<Vec<_>>>();
        Async::ready(digested.map(|payloads| self.stage(tx, Staged::SetPayloads(payloads))))
    }

    fn add_members<'a>(
        &'a self,
        tx: &'a mut MemoryPartTx,
        members: &'a [(ObjKey, PartKey)],
    ) -> Async::Future<'a, Res<()>> {
        self.stage(tx, Staged::AddMembers(members.to_vec()));
        Async::ready(Ok(()))
    }

    fn remove_members<'a>(
        &'a self,
        tx: &'a mut MemoryPartTx,
        members: &'a [(ObjKey, PartKey)],
    ) -> Async::Future<'a, Res<()>> {
        self.stage(tx, Staged::RemoveMembers(members.to_vec()));
        Async::ready(Ok(()))
    }

    fn grant_access<'a>(
        &'a self,
        tx: &'a mut MemoryPartTx,
        grants: &'a [(PartKey, PeerKey, AccessLevel)],
    ) -> Async::Future<'a, Res<()>> {
        self.stage(tx, Staged::GrantAccess(grants.to_vec()));
        Async::ready(Ok(()))
    }

    fn revoke_access<'a>(
        &'a self,
        tx: &'a mut MemoryPartTx,
        revokes: &'a [(PartKey, PeerKey)],
    ) -> Async::Future<'a, Res<()>> {
        self.stage(tx, Staged::RevokeAccess(revokes.to_vec()));
        Async::ready(Ok(()))
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> PartStoreRead<Async> for MemoryPartStore {
    fn obj_states<'a>(
        &'a self,
        objs: &'a [ObjKey],
    ) -> Async::Future<'a, Res<Vec<Option<ObjState>>>> {
        Async::ready(Ok(self.with_state(|state| {
            objs.iter().map(|obj| state.obj_state(obj)).collect()
        })))
    }

    fn bucket_summaries<'a>(
        &'a self,
        part: &'a PartKey,
        ids: &'a [BuckId],
    ) -> Async::Future<'a, Res<Vec<BucketSummary>>> {
        Async::ready(Ok(
            self.with_state(|state| state.bucket_summaries(part, ids))
        ))
    }

    fn member_count<'a>(&'a self, part: &'a PartKey) -> Async::Future<'a, Res<u64>> {
        Async::ready(Ok(self.with_state(|state| state.member_count(part))))
    }

    fn peer_part_cursors<'a>(
        &'a self,
        peer: &'a PeerKey,
        parts: &'a [PartKey],
    ) -> Async::Future<'a, Res<Vec<Option<Rev>>>> {
        Async::ready(Ok(self.with_state(|state| {
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

    fn set_peer_part_cursors<'a>(
        &'a self,
        cursors: &'a [(PeerKey, PartKey, Rev)],
    ) -> Async::Future<'a, Res<()>> {
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
        Async::ready(Ok(()))
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> RevSource<Async> for MemoryPartStore {
    type Key = LogKey;
    type Value = LogEntry;
    type Selector = LogSelector;

    fn latest_rev(&self) -> Async::Future<'_, Res<Rev>> {
        Async::ready(Ok(self.with_state(|state| state.log.rev())))
    }

    /// Copies one bounded page of the log out under the lock, applying the route rule for
    /// removals with the `added_at` the members table keeps.
    fn read<'a>(
        &'a self,
        selector: &'a LogSelector,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &'a mut Page<LogKey, LogEntry>,
    ) -> Async::Future<'a, Res<()>> {
        self.with_state(|state| {
            state.log.read_into(
                |key, rev, value| match selector {
                    LogSelector::All => true,
                    LogSelector::Routes(_) => {
                        selector.selects(key, rev, after, state.removed_added_at(key, value))
                    }
                },
                log_row_bytes,
                after,
                until,
                limits,
                page,
            );
        });
        Async::ready(Ok(()))
    }

    fn watch(&self) -> RevWatch {
        RevWatch::new(self.wakeups.subscribe())
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> PartStoreServe<Async> for MemoryPartStore {
    fn read_verdicts<'a>(
        &'a self,
        principal: &'a PeerKey,
        parts: &'a [PartKey],
    ) -> Async::Future<'a, Res<Vec<ReadVerdict>>> {
        Async::ready(Ok(self.with_state(|state| {
            parts
                .iter()
                .map(|part| state.read_verdict(principal, part))
                .collect()
        })))
    }

    fn readable_parts_of<'a>(
        &'a self,
        principal: &'a PeerKey,
        obj: &'a ObjKey,
    ) -> Async::Future<'a, Res<Vec<PartKey>>> {
        Async::ready(Ok(
            self.with_state(|state| state.readable_parts_of(principal, obj))
        ))
    }

    fn part_dirty_count<'a>(
        &'a self,
        part: &'a PartKey,
        principal: Option<&'a PeerKey>,
        since: Rev,
    ) -> Async::Future<'a, Res<PartDirtyCount>> {
        Async::ready(Ok(
            self.with_state(|state| state.part_dirty_count(part, principal, since))
        ))
    }

    fn bucket_list<'a>(
        &'a self,
        part: &'a PartKey,
        from: BuckId,
        to_level: BuckLevel,
        limit: usize,
    ) -> Async::Future<'a, Res<Vec<BucketSummary>>> {
        Async::ready(Ok(
            self.with_state(|state| state.bucket_list(part, from, to_level, limit))
        ))
    }

    fn bucket_leaves<'a>(
        &'a self,
        part: &'a PartKey,
        buck: BuckId,
        after: Option<&'a ObjKey>,
        limit: usize,
    ) -> Async::Future<'a, Res<Vec<LeafEntry>>> {
        Async::ready(Ok(
            self.with_state(|state| state.bucket_leaves(part, buck, after, limit))
        ))
    }
}
