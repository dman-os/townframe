//! The naive part store the laws check backends against.
//!
//! It keeps what was asked for (payloads, intended memberships, grants) and derives the log by
//! brute force: after each write element it recomputes every live log row from scratch and
//! diffs that against the rows before, stamping what changed. Bucket summaries, dirt counts and
//! authorization are recomputed from the log on every question. Small enough to be obviously
//! right, and slow.

use std::num::NonZeroUsize;

use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::LeafEntry;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::LogSelector;
use crate::part_store::Membership;
use crate::part_store::ObjState;
use crate::part_store::PartDirtyCount;
use crate::part_store::PayloadState;
use crate::part_store::ReadVerdict;
use crate::part_store::log_row_bytes;
use crate::rev::Entry;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::store::bucket::BucketMemberKind;
use crate::store::bucket::BucketSummary;
use crate::store::bucket::BucketSummaryState;

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq)]]
    /// One batch of a write transaction, as [`PartStoreWrite`](crate::part_store::PartStoreWrite)
    /// takes it.
    pub enum PartWrite {
        EnsureParts(Vec<PartKey>),
        SetPayloads(Vec<(ObjKey, PayloadState)>),
        AddMembers(Vec<(ObjKey, PartKey)>),
        RemoveMembers(Vec<(ObjKey, PartKey)>),
        GrantAccess(Vec<(PartKey, PeerKey, AccessLevel)>),
        RevokeAccess(Vec<(PartKey, PeerKey)>),
    }
}

#[derive(Debug, Clone, Default)]
pub struct PartOracle {
    rev: Rev,
    parts: BTreeSet<PartKey>,
    payloads: BTreeMap<ObjKey, PayloadState>,
    /// Memberships asked for and not removed, live or pending.
    intended: BTreeSet<(ObjKey, PartKey)>,
    access: BTreeMap<(PartKey, PeerKey), AccessLevel>,
    /// The collapsed log: each key's last stamp.
    log: BTreeMap<LogKey, (Rev, Option<LogEntry>)>,
    peer_cursors: BTreeMap<(PeerKey, PartKey), Rev>,
}

impl PartOracle {
    pub fn latest_rev(&self) -> Rev {
        self.rev
    }

    /// The live rows the asked-for state implies, with the membership values the log holds
    /// (a membership that is not live yet goes live at `rev`).
    fn live_rows(&self, rev: Rev) -> BTreeMap<LogKey, LogEntry> {
        let mut rows = BTreeMap::new();
        for (obj, part) in &self.intended {
            if self.payloads.contains_key(obj) {
                let key = LogKey::Member {
                    obj: obj.clone(),
                    part: part.clone(),
                };
                let added_at = match self.log.get(&key) {
                    Some((_, Some(LogEntry::Member(membership)))) => membership.added_at,
                    _ => rev,
                };
                rows.insert(key, LogEntry::Member(Membership { added_at }));
            }
        }
        for ((part, principal), level) in &self.access {
            rows.insert(
                LogKey::Access {
                    part: part.clone(),
                    principal: principal.clone(),
                },
                LogEntry::Access(*level),
            );
        }
        rows
    }

    /// Apply one write element at `rev` and stamp the rows it changed. `content` names an
    /// object whose payload changed, whose live rows are re-stamped even though their values
    /// stay.
    fn step(&mut self, rev: Rev, change: impl FnOnce(&mut Self) -> Option<ObjKey>) -> bool {
        let before = self.live_rows(rev);
        let content = change(self);
        let after = self.live_rows(rev);
        let mut stamped = false;
        for (key, value) in &after {
            let restamp = matches!(
                (key, &content),
                (LogKey::Member { obj, .. }, Some(changed)) if obj == changed
            );
            if before.get(key) != Some(value) || restamp {
                self.log.insert(key.clone(), (rev, Some(value.clone())));
                stamped = true;
            }
        }
        for key in before.keys() {
            if !after.contains_key(key) {
                self.log.insert(key.clone(), (rev, None));
                stamped = true;
            }
        }
        if stamped {
            for key in after.keys().chain(before.keys()) {
                let part = match key {
                    LogKey::Member { part, .. } | LogKey::Access { part, .. } => part,
                };
                self.parts.insert(part.clone());
            }
        }
        stamped
    }

    /// Record a committed transaction and return the rev it stamped, if any row changed.
    pub fn commit(&mut self, writes: Vec<PartWrite>) -> Option<Rev> {
        let rev = self.rev + 1;
        let mut stamped = false;
        for write in writes {
            match write {
                PartWrite::EnsureParts(parts) => self.parts.extend(parts),
                PartWrite::SetPayloads(payloads) => {
                    for (obj, payload) in payloads {
                        stamped |= self.step(rev, |oracle| {
                            if oracle.payloads.get(&obj) == Some(&payload) {
                                return None;
                            }
                            oracle.payloads.insert(obj.clone(), payload);
                            Some(obj)
                        });
                    }
                }
                PartWrite::AddMembers(members) => {
                    for (obj, part) in members {
                        stamped |= self.step(rev, |oracle| {
                            oracle.parts.insert(part.clone());
                            oracle.intended.insert((obj, part));
                            None
                        });
                    }
                }
                PartWrite::RemoveMembers(members) => {
                    for member in members {
                        stamped |= self.step(rev, |oracle| {
                            oracle.intended.remove(&member);
                            None
                        });
                    }
                }
                PartWrite::GrantAccess(grants) => {
                    for (part, principal, level) in grants {
                        stamped |= self.step(rev, |oracle| {
                            oracle.parts.insert(part.clone());
                            oracle.access.insert((part, principal), level);
                            None
                        });
                    }
                }
                PartWrite::RevokeAccess(revokes) => {
                    for revoke in revokes {
                        stamped |= self.step(rev, |oracle| {
                            oracle.access.remove(&revoke);
                            None
                        });
                    }
                }
            }
        }
        if stamped {
            self.rev = rev;
        }
        stamped.then_some(rev)
    }

    pub fn set_peer_part_cursors(&mut self, cursors: &[(PeerKey, PartKey, Rev)]) {
        for (peer, part, cursor) in cursors {
            let stored = self
                .peer_cursors
                .entry((peer.clone(), part.clone()))
                .or_default();
            *stored = (*stored).max(*cursor);
        }
    }

    pub fn peer_part_cursor(&self, peer: &PeerKey, part: &PartKey) -> Option<Rev> {
        self.peer_cursors
            .get(&(peer.clone(), part.clone()))
            .copied()
    }

    pub fn obj_state(&self, obj: &ObjKey) -> Option<ObjState> {
        let payload = self.payloads.get(obj)?;
        Some(ObjState {
            payload: payload.clone(),
            digest: payload.digest(),
            parts: self.live_parts_of(obj),
        })
    }

    fn live_parts_of(&self, obj: &ObjKey) -> BTreeSet<PartKey> {
        self.log
            .iter()
            .filter_map(|(key, (_, value))| match (key, value) {
                (LogKey::Member { obj: row_obj, part }, Some(_)) if row_obj == obj => {
                    Some(part.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Every member row of `part`, live or removed, with its rev.
    fn member_rows<'a>(
        &'a self,
        part: &'a PartKey,
    ) -> impl Iterator<Item = (&'a ObjKey, Rev, bool)> + 'a {
        self.log
            .iter()
            .filter_map(move |(key, (rev, value))| match key {
                LogKey::Member {
                    obj,
                    part: row_part,
                } if row_part == part => Some((obj, *rev, value.is_some())),
                _ => None,
            })
    }

    /// Every bucket of `part` recomputed from the log: each member row added from nothing, in
    /// rev order, so a bucket's `changed_at` is its latest member row.
    pub fn buckets(&self, part: &PartKey) -> BTreeMap<BuckId, BucketSummaryState> {
        let mut rows = self.member_rows(part).collect::<Vec<_>>();
        rows.sort_by_key(|(obj, rev, _)| (*rev, (*obj).clone()));
        let mut buckets = BTreeMap::<BuckId, BucketSummaryState>::new();
        for (obj, rev, live) in rows {
            let kind = if live {
                BucketMemberKind::Live(self.payloads[obj].digest())
            } else {
                BucketMemberKind::Dead
            };
            let deepest = BuckId::deepest_from_obj_key(obj);
            for level in 0..=BuckId::MAX_LEVEL {
                let buck = deepest.to_level(level);
                buckets.entry(buck).or_default().apply_transition(
                    buck,
                    obj.clone(),
                    rev,
                    BucketMemberKind::Absent,
                    kind,
                );
            }
        }
        buckets
    }

    pub fn bucket_summary(&self, part: &PartKey, id: BuckId) -> BucketSummary {
        self.buckets(part)
            .get(&id)
            .cloned()
            .unwrap_or_default()
            .summary(id)
    }

    pub fn member_count(&self, part: &PartKey) -> u64 {
        self.member_rows(part).filter(|(_, _, live)| *live).count() as u64
    }

    pub fn bucket_list(
        &self,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        limit: usize,
    ) -> Vec<BucketSummary> {
        self.buckets(part)
            .into_iter()
            .filter(|(id, _)| *id >= from && id.level() <= to_level)
            .map(|(id, state)| state.summary(id))
            .take(limit)
            .collect()
    }

    pub fn bucket_leaves(
        &self,
        part: &PartKey,
        buck: BuckId,
        after: Option<&ObjKey>,
        limit: usize,
    ) -> Vec<LeafEntry> {
        let mut rows = self
            .member_rows(part)
            .filter(|(obj, _, _)| BuckId::deepest_from_obj_key(obj).to_level(buck.level()) == buck)
            .map(|(obj, _, live)| (BuckId::deepest_from_obj_key(obj).index(), obj.clone(), live))
            .collect::<Vec<_>>();
        rows.sort();
        let after = after.map(|after| (BuckId::deepest_from_obj_key(after).index(), after.clone()));
        rows.into_iter()
            .filter(|(index, obj, _)| {
                after
                    .as_ref()
                    .is_none_or(|after| (*index, obj) > (after.0, &after.1))
            })
            .take(limit)
            .map(|(_, obj, live)| LeafEntry {
                live: live.then(|| self.payloads[&obj].digest()),
                obj,
            })
            .collect()
    }

    fn permits(&self, principal: &PeerKey, part: &PartKey) -> bool {
        self.access.contains_key(&(part.clone(), principal.clone()))
    }

    pub fn read_verdict(&self, principal: &PeerKey, part: &PartKey) -> ReadVerdict {
        if !self.parts.contains(part) {
            ReadVerdict::UnknownPart
        } else if self.permits(principal, part) {
            ReadVerdict::Permitted
        } else {
            ReadVerdict::Denied
        }
    }

    pub fn readable_parts_of(&self, principal: &PeerKey, obj: &ObjKey) -> Vec<PartKey> {
        self.live_parts_of(obj)
            .into_iter()
            .filter(|part| self.permits(principal, part))
            .collect()
    }

    pub fn part_dirty_count(
        &self,
        part: &PartKey,
        principal: Option<&PeerKey>,
        since: Rev,
    ) -> PartDirtyCount {
        PartDirtyCount {
            member_changes: self
                .member_rows(part)
                .filter(|(_, rev, _)| *rev > since)
                .count() as u64,
            access_changes: principal.map_or(0, |principal| {
                let key = LogKey::Access {
                    part: part.clone(),
                    principal: principal.clone(),
                };
                self.log
                    .get(&key)
                    .map_or(0, |(rev, _)| u64::from(*rev > since))
            }),
        }
    }

    /// The current live memberships.
    pub fn live_members(&self) -> BTreeSet<(ObjKey, PartKey)> {
        self.intended
            .iter()
            .filter(|(obj, _)| self.payloads.contains_key(obj))
            .cloned()
            .collect()
    }

    /// The current grants.
    pub fn grants(&self) -> &BTreeMap<(PartKey, PeerKey), AccessLevel> {
        &self.access
    }

    /// What a read of the log must return: the selected rows with a rev in `(after, until]` in
    /// `(rev, key)` order, cut after the last whole revision that fits the limits.
    pub fn read(
        &self,
        selector: &LogSelector,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Page<LogKey, LogEntry> {
        let mut window = self
            .log
            .iter()
            .filter(|(key, (rev, _))| *rev > after && *rev <= until && selector.selects(key, *rev))
            .map(|(key, (rev, value))| Entry {
                key: key.clone(),
                rev: *rev,
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        window.sort_by(|left, right| (left.rev, &left.key).cmp(&(right.rev, &right.key)));
        let max_bytes = limits.max_bytes.map_or(usize::MAX, NonZeroUsize::get);
        let mut taken = 0;
        let mut bytes = 0usize;
        let mut through = until;
        for revision in window.chunk_by(|left, right| left.rev == right.rev) {
            let revision_bytes = revision
                .iter()
                .map(|entry| log_row_bytes(&entry.key, entry.value.as_ref()))
                .fold(0usize, usize::saturating_add);
            if taken > 0
                && (taken + revision.len() > limits.max_entries.get()
                    || bytes.saturating_add(revision_bytes) > max_bytes)
            {
                through = window[taken - 1].rev;
                break;
            }
            taken += revision.len();
            bytes = bytes.saturating_add(revision_bytes);
        }
        window.truncate(taken);
        Page {
            entries: window,
            through,
        }
    }
}
