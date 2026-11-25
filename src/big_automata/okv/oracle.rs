//! The naive okv model the laws check backends against.
//!
//! It keeps the whole commit log and answers every question by brute force over it: the current
//! state is a fold of the log, and a read sorts that state and cuts whole revisions off the front.
//! It is meant to be small enough to be obviously right, not fast.

use std::num::NonZeroUsize;

use crate::interlude::*;
use crate::okv::Mutation;
use crate::okv::Pruned;
use crate::okv::Selector;
use crate::okv::row_bytes;
use crate::rev::Entry;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;

strike! {
    #[structstruck::each[derive(Debug, Clone, PartialEq, Eq)]]
    pub struct Oracle<K, V> {
        /// Committed batches, oldest first. Rolled back batches are never recorded.
        log: Vec<struct Batch<K, V> {
            rev: Rev,
            mutations: Vec<Mutation<K, V>>,
        }>,
        /// The registered consumers and their cursors.
        consumers: BTreeMap<String, Rev>,
        /// Tombstones at or below this rev have been pruned: the highest floor any prune used.
        pruned_through: Rev,
    }
}

impl<K, V> Default for Oracle<K, V> {
    fn default() -> Self {
        Self {
            log: default(),
            consumers: default(),
            pruned_through: 0,
        }
    }
}

impl<K, V> Oracle<K, V>
where
    K: Ord + Clone + AsRef<[u8]>,
    V: Clone + AsRef<[u8]>,
{
    pub fn latest_rev(&self) -> Rev {
        self.log.last().map_or(0, |batch| batch.rev)
    }

    /// Record a committed batch and return its rev.
    pub fn commit(&mut self, mutations: Vec<Mutation<K, V>>) -> Rev {
        assert!(!mutations.is_empty(), "an empty batch commits nothing");
        let rev = self.latest_rev() + 1;
        self.log.push(Batch { rev, mutations });
        rev
    }

    /// Register `consumer` and return its cursor.
    pub fn register(&mut self, consumer: &str) -> Rev {
        *self.consumers.entry(consumer.to_owned()).or_insert(0)
    }

    /// Move `consumer`'s cursor forward; a lower cursor changes nothing.
    pub fn report(&mut self, consumer: &str, cursor: Rev) {
        assert!(
            cursor <= self.latest_rev(),
            "cursor {cursor} past the latest rev"
        );
        let held = self
            .consumers
            .get_mut(consumer)
            .expect("a report for a consumer that is not registered");
        *held = (*held).max(cursor);
    }

    pub fn unregister(&mut self, consumer: &str) {
        self.consumers.remove(consumer);
    }

    pub fn consumers(&self) -> &BTreeMap<String, Rev> {
        &self.consumers
    }

    /// Drop every tombstone at or below the lowest registered cursor, none when there is no
    /// consumer.
    pub fn prune(&mut self) -> Pruned {
        let floor = self.consumers.values().copied().min().unwrap_or(0);
        let tombstones = self
            .state()
            .values()
            .filter(|entry| entry.value.is_none() && entry.rev <= floor)
            .count();
        self.pruned_through = self.pruned_through.max(floor);
        Pruned { floor, tombstones }
    }

    /// Every key's current row: its last mutation, at that mutation's rev, minus the
    /// tombstones a prune dropped.
    pub fn state(&self) -> BTreeMap<K, Entry<K, V>> {
        let mut state = BTreeMap::new();
        for batch in &self.log {
            for mutation in &batch.mutations {
                let (key, value) = mutation.clone().into_row();
                state.insert(
                    key.clone(),
                    Entry {
                        key,
                        rev: batch.rev,
                        value,
                    },
                );
            }
        }
        state.retain(|_, entry| entry.value.is_some() || entry.rev > self.pruned_through);
        state
    }

    /// The key's committed value: `None` for a tombstone or a key never written.
    pub fn get(&self, key: &K) -> Option<V> {
        self.log
            .iter()
            .flat_map(|batch| &batch.mutations)
            .filter_map(|mutation| {
                let (written, value) = mutation.clone().into_row();
                (written == *key).then_some(value)
            })
            .last()
            .flatten()
    }

    /// The row `key` was left with by the batch at `rev`, if that batch wrote it.
    pub fn written_at(&self, key: &K, rev: Rev) -> Option<Entry<K, V>> {
        let batch = self.log.iter().find(|batch| batch.rev == rev)?;
        let (key, value) = batch
            .mutations
            .iter()
            .rev()
            .map(|mutation| mutation.clone().into_row())
            .find(|(written, _)| written == key)?;
        Some(Entry { key, rev, value })
    }

    /// The current rows of the selected keys with a rev in `(after, until]`, in `(rev, key)`
    /// order.
    pub fn window(&self, selector: &Selector<K>, after: Rev, until: Rev) -> Vec<Entry<K, V>> {
        let mut rows = self
            .state()
            .into_values()
            .filter(|entry| selector.matches(&entry.key) && entry.rev > after && entry.rev <= until)
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| (left.rev, &left.key).cmp(&(right.rev, &right.key)));
        rows
    }

    /// What a read of `(after, until]` must return: the window, cut after the last whole
    /// revision that fits the limits (the first revision always fits).
    pub fn read(
        &self,
        selector: &Selector<K>,
        after: Rev,
        until: Rev,
        limits: Limits,
    ) -> Page<K, V> {
        let window = self.window(selector, after, until);
        let max_bytes = limits.max_bytes.map_or(usize::MAX, NonZeroUsize::get);
        let mut taken = 0;
        let mut bytes = 0usize;
        let mut through = until;
        for revision in window.chunk_by(|left, right| left.rev == right.rev) {
            let revision_bytes = revision
                .iter()
                .map(|entry| row_bytes(&entry.key, entry.value.as_ref()))
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
        let mut entries = window;
        entries.truncate(taken);
        Page { entries, through }
    }
}
