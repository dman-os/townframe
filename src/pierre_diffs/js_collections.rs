//! JavaScript `Map` / `Set` iteration semantics the port must reproduce.
//!
//! Original to this crate (no upstream counterpart). Upstream's Virtualizer and
//! UniversalRenderingManager keep identity-keyed `Map`s and `Set`s and iterate
//! them while the callbacks they invoke add to or delete from the same
//! collection. A JavaScript iterator is live: it visits entries appended during
//! the walk and skips entries deleted before it reaches them, and insertion
//! order is the iteration order. A `Vec` snapshot or a `HashMap` would change
//! which callbacks run in a pass, so this keeps the order explicitly and hands
//! out cursor-based iteration that does not hold a borrow across the callback.

use std::collections::HashMap;
use std::hash::Hash;

/// Insertion-ordered map. `insert` on a present key replaces the value in
/// place (JS `Map.prototype.set`); removing and re-inserting moves the key to
/// the end. Removed slots stay as tombstones until [`Self::compact`] so cursors
/// held by a walk in progress stay valid.
#[derive(Debug)]
pub struct InsertionMap<K, V> {
    slots: Vec<Option<(K, V)>>,
    index: HashMap<K, usize>,
}

impl<K, V> Default for InsertionMap<K, V> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            index: HashMap::new(),
        }
    }
}

impl<K: Clone + Eq + Hash, V> InsertionMap<K, V> {
    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let slot = *self.index.get(key)?;
        self.slots[slot].as_ref().map(|(_, value)| value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let slot = *self.index.get(key)?;
        self.slots[slot].as_mut().map(|(_, value)| value)
    }

    pub fn insert(&mut self, key: K, value: V) {
        if let Some(&slot) = self.index.get(&key) {
            self.slots[slot] = Some((key, value));
        } else {
            self.index.insert(key.clone(), self.slots.len());
            self.slots.push(Some((key, value)));
        }
    }

    /// Returns whether the key was present.
    pub fn remove(&mut self, key: &K) -> bool {
        match self.index.remove(key) {
            Some(slot) => {
                self.slots[slot] = None;
                true
            }
            None => false,
        }
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.index.clear();
    }

    /// Drops tombstones. Only call between walks: it invalidates cursors.
    pub fn compact(&mut self) {
        if self.slots.len() == self.index.len() {
            return;
        }
        self.slots.retain(Option::is_some);
        for (slot, entry) in self.slots.iter().enumerate() {
            if let Some((key, _)) = entry {
                self.index.insert(key.clone(), slot);
            }
        }
    }
}

impl<K: Clone + Eq + Hash, V: Clone> InsertionMap<K, V> {
    /// Advances a live cursor (start at 0): the next present entry at or after
    /// it, including entries appended since the walk began.
    pub fn next_entry(&self, cursor: &mut usize) -> Option<(K, V)> {
        while let Some(slot) = self.slots.get(*cursor) {
            *cursor += 1;
            if let Some((key, value)) = slot {
                return Some((key.clone(), value.clone()));
            }
        }
        None
    }

    pub fn entries(&self) -> Vec<(K, V)> {
        self.slots.iter().flatten().cloned().collect()
    }
}

/// A callback compared the way JavaScript compares functions: by identity
/// (`===`). `Clone` shares the allocation, so a clone equals its original.
pub struct PtrEq<Callback: ?Sized>(pub std::rc::Rc<Callback>);

impl<Callback: ?Sized> PtrEq<Callback> {
    pub fn new(callback: std::rc::Rc<Callback>) -> Self {
        Self(callback)
    }
}

impl<Callback: ?Sized> Clone for PtrEq<Callback> {
    fn clone(&self) -> Self {
        Self(std::rc::Rc::clone(&self.0))
    }
}

impl<Callback: ?Sized> PartialEq for PtrEq<Callback> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(std::rc::Rc::as_ptr(&self.0), std::rc::Rc::as_ptr(&other.0))
    }
}

impl<Callback: ?Sized> std::fmt::Debug for PtrEq<Callback> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PtrEq(..)")
    }
}

impl<Callback: ?Sized> std::ops::Deref for PtrEq<Callback> {
    type Target = Callback;

    fn deref(&self) -> &Callback {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_cursor_sees_appends_and_skips_removals() {
        let mut map = InsertionMap::default();
        map.insert("a", 1);
        map.insert("b", 2);
        let mut cursor = 0;
        assert_eq!(map.next_entry(&mut cursor), Some(("a", 1)));
        map.remove(&"b");
        map.insert("c", 3);
        assert_eq!(map.next_entry(&mut cursor), Some(("c", 3)));
        assert_eq!(map.next_entry(&mut cursor), None);
    }

    #[test]
    fn reinsert_moves_to_the_end_and_set_keeps_position() {
        let mut map = InsertionMap::default();
        map.insert("a", 1);
        map.insert("b", 2);
        map.insert("a", 10);
        assert_eq!(map.entries(), vec![("a", 10), ("b", 2)]);
        map.remove(&"a");
        map.insert("a", 11);
        assert_eq!(map.entries(), vec![("b", 2), ("a", 11)]);
        map.compact();
        assert_eq!(map.entries(), vec![("b", 2), ("a", 11)]);
        assert_eq!(map.get(&"a"), Some(&11));
    }
}
