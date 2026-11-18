//! The guest side of the read waist.
//!
//! A [`Mirror`] is what a reader keeps after pages arrive: the replica the pages fold
//! into, the cursor to acknowledge with on the next read, and whether bootstrap has
//! completed. It is deliberately not a query engine — rows are rendered from what has
//! committed, and a `ResetRequired` clears the mirror before reacquiring, so stale
//! generations cannot survive as display state pretending to be authoritative.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::replica::Entry;
use crate::types::Change;
use crate::types::Cursor;
use crate::types::Page;
use crate::types::Stamped;
use crate::types::WriteBasis;

#[derive(Clone, Debug)]
pub struct ReadTicket {
    identity: Arc<()>,
    lease_epoch: u64,
    sequence: u64,
    base: Option<Cursor>,
    first: bool,
}

impl ReadTicket {
    pub fn committed(&self) -> Option<&Cursor> {
        if self.first { None } else { self.base.as_ref() }
    }

    pub fn base(&self) -> Option<&Cursor> {
        self.base.as_ref()
    }
}

#[derive(Debug)]
pub struct Mirror<K, V> {
    rows: BTreeMap<K, Entry<V>>,
    incarnation: Option<u64>,
    cursor: Option<Cursor>,
    bootstrap_complete: bool,
    identity: Arc<()>,
    lease_epoch: u64,
    first: bool,
    sequence: u64,
    active: Option<u64>,
}

impl<K, V> Default for Mirror<K, V> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<K, V> Mirror<K, V> {
    pub fn empty() -> Self {
        Self {
            rows: BTreeMap::new(),
            incarnation: None,
            cursor: None,
            bootstrap_complete: false,
            identity: Arc::new(()),
            lease_epoch: 0,
            first: true,
            sequence: 0,
            active: None,
        }
    }

    pub fn begin_lease(&mut self, resume: Option<&Cursor>) {
        let lease_epoch = self
            .lease_epoch
            .checked_add(1)
            .expect("mirror lease identity exhausted");
        if let Some(cursor) = resume {
            assert_eq!(
                Some(cursor),
                self.cursor.as_ref(),
                "resume cursor does not match mirror"
            );
        } else {
            self.rows.clear();
            self.incarnation = None;
            self.cursor = None;
        }
        self.lease_epoch = lease_epoch;
        self.first = true;
        self.sequence = 0;
        self.active = None;
        self.bootstrap_complete = false;
    }

    pub fn begin_read(&mut self) -> ReadTicket {
        assert!(self.active.is_none(), "mirror already has an active read");
        self.sequence = self
            .sequence
            .checked_add(1)
            .expect("mirror request identity exhausted");
        self.active = Some(self.sequence);
        ReadTicket {
            identity: Arc::clone(&self.identity),
            lease_epoch: self.lease_epoch,
            sequence: self.sequence,
            base: self.cursor.clone(),
            first: self.first,
        }
    }

    fn accepts(&self, ticket: &ReadTicket) -> bool {
        Arc::ptr_eq(&self.identity, &ticket.identity)
            && self.lease_epoch == ticket.lease_epoch
            && self.active == Some(ticket.sequence)
            && self.cursor == ticket.base
            && self.first == ticket.first
    }

    pub fn cancel_read(&mut self, ticket: &ReadTicket) -> bool {
        if !self.accepts(ticket) {
            return false;
        }
        self.active = None;
        true
    }

    pub fn clear(&mut self) {
        self.begin_lease(None);
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }

    pub fn committed(&self) -> Option<&Cursor> {
        if self.first {
            None
        } else {
            self.cursor.as_ref()
        }
    }

    pub fn bootstrap_complete(&self) -> bool {
        self.bootstrap_complete
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<K: Ord + Clone, V: Clone> Mirror<K, V> {
    pub fn apply(&mut self, ticket: &ReadTicket, page: &Page<K, V>) -> bool {
        if !self.accepts(ticket) {
            return false;
        }
        let mut incarnation = self.incarnation;
        for change in page
            .transactions
            .iter()
            .flat_map(|transaction| &transaction.changes)
        {
            assert_eq!(
                *incarnation.get_or_insert(change.row_version().incarnation),
                change.row_version().incarnation,
                "mirror incarnation changed without an explicit reset"
            );
        }
        for change in page
            .transactions
            .iter()
            .flat_map(|transaction| &transaction.changes)
        {
            if self
                .rows
                .get(change.key())
                .is_some_and(|entry| entry.version >= change.row_version())
            {
                continue;
            }
            match change {
                Change::Upsert {
                    key,
                    row_version,
                    value,
                    evidence,
                    write_context,
                } => {
                    self.rows.insert(
                        key.clone(),
                        Entry {
                            version: *row_version,
                            value: value.clone(),
                            evidence: *evidence,
                            write_context: write_context.clone(),
                        },
                    );
                }
                Change::Remove { key, .. } => {
                    self.rows.remove(key);
                }
            }
        }
        self.incarnation = incarnation;
        self.cursor = Some(page.through.clone());
        self.first = false;
        self.bootstrap_complete |= page.bootstrap_complete;
        self.active = None;
        true
    }

    pub fn rows(&self) -> Vec<(K, V)> {
        self.rows
            .iter()
            .map(|(key, entry)| (key.clone(), entry.value.clone()))
            .collect()
    }

    pub fn row(&self, key: &K) -> Option<&Entry<V>> {
        self.rows.get(key)
    }
}

/// A mirror of a registration collection keeps tombstones as `None` rows so it knows the
/// stamp of everything it has seen deleted; these read the replica as an application
/// does (a tombstoned key is absent) and as a writer does (any stamp is a basis).
impl<K: Ord + Clone, V: Clone> Mirror<K, Stamped<Option<V>>> {
    /// The live value of `key`; a tombstoned or unknown key is absent.
    pub fn live(&self, key: &K) -> Option<&V> {
        self.rows.get(key)?.value.value.as_ref()
    }

    /// Every live row; tombstoned keys are hidden.
    pub fn live_rows(&self) -> Vec<(K, V)> {
        self.rows
            .iter()
            .filter_map(|(key, entry)| Some((key.clone(), entry.value.value.clone()?)))
            .collect()
    }

    /// The basis a write to `key` defaults to: `Observed` with whatever stamp this replica
    /// holds for it, a tombstone's included, so the write supersedes what the writer has
    /// seen; `Concurrent` only for a key the replica has never held.
    pub fn write_basis(&self, key: &K) -> WriteBasis {
        match self.rows.get(key) {
            Some(entry) => WriteBasis::Observed(entry.value.stamp.clone()),
            None => WriteBasis::Concurrent,
        }
    }
}

#[cfg(test)]
mod tests;
