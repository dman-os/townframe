//! The bucket index: each part's summaries at every level, maintained per member transition,
//! and the member ranges a bucket's leaves come from.

use std::ops::Bound;

use super::State;
use crate::interlude::*;
use crate::part_store::LeafEntry;
use crate::part_store::LogKey;
use crate::rev::Rev;
use crate::store::bucket::BucketMemberKind;
use crate::store::bucket::BucketSummary;
use crate::store::bucket::BucketSummaryState;

/// The deepest bucket indices a bucket covers, as a half-open range (the last bucket's end is
/// past `u16::MAX`).
pub(super) fn deepest_range(buck: BuckId) -> (u32, u32) {
    let shift = u32::from(BuckId::BITS_PER_LEVEL) * u32::from(BuckId::MAX_LEVEL - buck.level());
    let start = u32::from(buck.index()) << shift;
    (start, start + (1 << shift))
}

impl State {
    /// Move `obj` from `old` to `new` in every bucket of `part` that contains it, stamping each
    /// with `rev`.
    pub(super) fn bucket_transition(
        &mut self,
        part: &PartKey,
        obj: &ObjKey,
        rev: Rev,
        old: BucketMemberKind,
        new: BucketMemberKind,
    ) {
        let deepest = BuckId::deepest_from_obj_key(obj);
        for level in 0..=BuckId::MAX_LEVEL {
            let buck = deepest.to_level(level);
            self.buckets
                .entry((part.clone(), buck))
                .or_default()
                .apply_transition(buck, obj.clone(), rev, old, new);
        }
    }

    pub(super) fn bucket_summaries(&self, part: &PartKey, ids: &[BuckId]) -> Vec<BucketSummary> {
        ids.iter()
            .map(|id| match self.buckets.get(&(part.clone(), *id)) {
                Some(state) => state.summary(*id),
                None => BucketSummaryState::default().summary(*id),
            })
            .collect()
    }

    pub(super) fn member_count(&self, part: &PartKey) -> u64 {
        self.buckets
            .get(&(part.clone(), BuckId::ROOT))
            .map_or(0, |state| u64::from(state.summary(BuckId::ROOT).live_count))
    }

    pub(super) fn bucket_list(
        &self,
        part: &PartKey,
        from: BuckId,
        to_level: BuckLevel,
        limit: usize,
    ) -> Vec<BucketSummary> {
        let last = BuckId::new(to_level, u16::MAX);
        if from > last {
            return default();
        }
        self.buckets
            .range((part.clone(), from)..=(part.clone(), last))
            .map(|((_, id), state)| state.summary(*id))
            .filter(|summary| summary.len > 0)
            .take(limit)
            .collect()
    }

    pub(super) fn bucket_leaves(
        &self,
        part: &PartKey,
        buck: BuckId,
        after: Option<&ObjKey>,
        limit: usize,
    ) -> Vec<LeafEntry> {
        let (start, end) = deepest_range(buck);
        let lower = match after {
            None => Bound::Included((part.clone(), start as u16, ObjKey::new(&[]))),
            Some(after) => {
                let index = BuckId::deepest_from_obj_key(after).index();
                assert!(
                    (start..end).contains(&u32::from(index)),
                    "leaf cursor {after} is not in bucket {buck:?}"
                );
                Bound::Excluded((part.clone(), index, after.clone()))
            }
        };
        self.members
            .range((lower, Bound::Unbounded))
            .take_while(|((member_part, index, _), _)| {
                member_part == part && u32::from(*index) < end
            })
            .take(limit)
            .map(|((_, _, obj), _)| {
                let key = LogKey::Member {
                    obj: obj.clone(),
                    part: part.clone(),
                };
                let (_, value) = self.log.get(&key).expect(ERROR_IMPOSSIBLE);
                LeafEntry {
                    obj: obj.clone(),
                    live: value
                        .is_some()
                        .then(|| self.objs.get(obj).expect(ERROR_IMPOSSIBLE).digest),
                }
            })
            .collect()
    }
}
