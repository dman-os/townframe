//! A non-generic LRU policy engine plus a keyed wrapper.
//!
//! [`LruPool`] tracks abstract "IDs" and their associated "costs". When the
//! total cost exceeds capacity, it returns the IDs that should be pruned
//! based on the Least Recently Used strategy. [`KeyedLruPool`] layers typed
//! keys on top so callers can evict by key without managing slot IDs.

use std::collections::HashMap;
use std::sync::Arc;

/// Opaque slot identifier used internally by [`LruPool`].
pub type LruItemId = u64;

/// A shared, mutex-guarded [`KeyedLruPool`].
pub type SharedKeyedLruPool<K> = Arc<surelock::mutex::Mutex<KeyedLruPool<K>>>;

struct LruNode {
    cost: usize,
    prev: Option<LruItemId>,
    next: Option<LruItemId>,
}

/// LRU policy engine: id → cost, with least-recently-used eviction.
pub struct LruPool {
    capacity: usize,
    current_usage: usize,
    items: HashMap<LruItemId, LruNode>,
    head: Option<LruItemId>, // Oldest (least recently used)
    tail: Option<LruItemId>, // Newest (most recently used)
}

impl LruPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            current_usage: 0,
            items: HashMap::new(),
            head: None,
            tail: None,
        }
    }

    fn detach(&mut self, id: LruItemId) {
        let (prev, next) = match self.items.get_mut(&id) {
            Some(node) => {
                let prev_id = node.prev;
                let next_id = node.next;
                node.prev = None;
                node.next = None;
                (prev_id, next_id)
            }
            None => return,
        };
        if let Some(prev_id) = prev {
            if let Some(prev_node) = self.items.get_mut(&prev_id) {
                prev_node.next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(next_id) = next {
            if let Some(next_node) = self.items.get_mut(&next_id) {
                next_node.prev = prev;
            }
        } else {
            self.tail = prev;
        }
    }

    fn attach_tail(&mut self, id: LruItemId) {
        let old_tail = self.tail;
        self.tail = Some(id);
        if let Some(tail_id) = old_tail {
            if let Some(tail_node) = self.items.get_mut(&tail_id) {
                tail_node.next = Some(id);
            }
            if let Some(node) = self.items.get_mut(&id) {
                node.prev = Some(tail_id);
                node.next = None;
            }
        } else {
            self.head = Some(id);
            if let Some(node) = self.items.get_mut(&id) {
                node.prev = None;
                node.next = None;
            }
        }
    }

    /// Adds or updates an item in the pool.
    /// Returns a list of IDs that should be pruned to stay within capacity.
    pub fn add(&mut self, id: LruItemId, cost: usize) -> Vec<LruItemId> {
        if let Some(node) = self.items.get_mut(&id) {
            self.current_usage -= node.cost;
            node.cost = cost;
            self.detach(id);
            self.attach_tail(id);
        } else {
            self.items.insert(
                id,
                LruNode {
                    cost,
                    prev: None,
                    next: None,
                },
            );
            self.attach_tail(id);
        }
        self.current_usage += cost;

        let mut pruned = Vec::new();
        // Prune until we are under capacity.
        // We never prune the item we just added unless it's the only item
        // and its cost is greater than the total capacity.
        while self.current_usage > self.capacity && self.items.len() > 1 {
            let oldest_id = self.head.unwrap();
            let node = self.items.remove(&oldest_id).unwrap();
            self.current_usage -= node.cost;
            self.head = node.next;
            if let Some(head_id) = self.head {
                if let Some(head_node) = self.items.get_mut(&head_id) {
                    head_node.prev = None;
                }
            } else {
                self.tail = None;
            }
            pruned.push(oldest_id);
        }

        // Edge case: single item exceeds capacity
        if self.current_usage > self.capacity && !self.items.is_empty() {
            let only_id = self.head.unwrap();
            let node = self.items.remove(&only_id).unwrap();
            self.current_usage -= node.cost;
            self.head = None;
            self.tail = None;
            pruned.push(only_id);
        }

        pruned
    }

    /// Marks an ID as recently used without changing its cost.
    pub fn touch(&mut self, id: LruItemId) {
        if self.items.contains_key(&id) && self.tail != Some(id) {
            self.detach(id);
            self.attach_tail(id);
        }
    }

    /// Removes an ID from the pool.
    pub fn remove(&mut self, id: LruItemId) {
        if let Some(node) = self.items.remove(&id) {
            self.current_usage -= node.cost;
            let prev = node.prev;
            let next = node.next;
            if let Some(prev_id) = prev {
                if let Some(prev_node) = self.items.get_mut(&prev_id) {
                    prev_node.next = next;
                }
            } else {
                self.head = next;
            }
            if let Some(next_id) = next {
                if let Some(next_node) = self.items.get_mut(&next_id) {
                    next_node.prev = prev;
                }
            } else {
                self.tail = prev;
            }
        }
    }

    pub fn current_usage(&self) -> usize {
        self.current_usage
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// A keyed LRU pool: typed keys, cost-weighted eviction, slot reuse with
/// generation counters so stale slot IDs can never alias a reused slot.
pub struct KeyedLruPool<K> {
    policy: LruPool,
    key_to_slot: HashMap<K, usize>,
    slots: Vec<Option<K>>,
    slot_generations: Vec<u32>,
    free_slots: Vec<usize>,
}

impl<K: std::hash::Hash + Eq + Clone> KeyedLruPool<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            policy: LruPool::new(capacity),
            key_to_slot: HashMap::new(),
            slots: Vec::new(),
            slot_generations: Vec::new(),
            free_slots: Vec::new(),
        }
    }

    /// Insert or update `key` with `cost`. Returns the keys pruned to stay
    /// within capacity (never includes `key` itself unless it is the only
    /// item and its cost exceeds capacity).
    pub fn insert_key(&mut self, key: &K, cost: usize) -> Vec<K> {
        let id = self.id_for_or_insert_key(key);
        let pruned_ids = self.policy.add(id, cost);
        self.prune_ids(pruned_ids)
    }

    /// Mark `key` as recently used without changing its cost.
    pub fn touch_key(&mut self, key: &K) {
        if let Some(&slot) = self.key_to_slot.get(key) {
            let id = encode_id(
                u32::try_from(slot).expect("slot index exceeded u32"),
                self.slot_generations[slot],
            );
            self.policy.touch(id);
        }
    }

    /// Remove `key` from the pool.
    pub fn remove_key(&mut self, key: &K) {
        if let Some(slot) = self.key_to_slot.remove(key) {
            self.remove_slot(slot);
        }
    }

    /// Remove many keys from the pool.
    pub fn remove_keys<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = K>,
    {
        for key in keys {
            self.remove_key(&key);
        }
    }

    fn id_for_or_insert_key(&mut self, key: &K) -> LruItemId {
        if let Some(&slot) = self.key_to_slot.get(key) {
            return encode_id(
                u32::try_from(slot).expect("slot index exceeded u32"),
                self.slot_generations[slot],
            );
        }

        let slot = if let Some(slot) = self.free_slots.pop() {
            slot
        } else {
            self.slots.push(None);
            self.slot_generations.push(0);
            self.slots.len() - 1
        };

        let generation = self.slot_generations[slot].wrapping_add(1);
        self.slot_generations[slot] = generation;

        let slot_u32 = u32::try_from(slot).expect("slot index exceeded u32");
        let id = encode_id(slot_u32, generation);

        self.slots[slot] = Some(key.clone());
        self.key_to_slot.insert(key.clone(), slot);

        id
    }

    fn prune_ids(&mut self, pruned_ids: Vec<LruItemId>) -> Vec<K> {
        let mut pruned_keys = Vec::new();
        for id in pruned_ids {
            let (slot_u32, generation) = decode_id(id);
            let slot = slot_u32 as usize;
            if self.slot_generations.get(slot).copied() == Some(generation)
                && let Some(key) = self.remove_slot(slot)
            {
                pruned_keys.push(key);
            }
        }
        pruned_keys
    }

    fn remove_slot(&mut self, slot: usize) -> Option<K> {
        if let Some(&generation) = self.slot_generations.get(slot) {
            let id = encode_id(
                u32::try_from(slot).expect("slot index exceeded u32"),
                generation,
            );
            self.policy.remove(id);
        }

        let key = self
            .slots
            .get_mut(slot)
            .and_then(|slot_key| slot_key.take());
        if let Some(key) = key.as_ref() {
            self.key_to_slot.remove(key);
        }

        if key.is_some() {
            self.free_slots.push(slot);
        }

        key
    }
}

fn encode_id(slot: u32, generation: u32) -> LruItemId {
    (u64::from(generation) << 32) | u64::from(slot)
}

fn decode_id(id: LruItemId) -> (u32, u32) {
    (id as u32, (id >> 32) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic_eviction() {
        let mut pool = LruPool::new(10);

        // Add items up to capacity
        assert!(pool.add(1u64, 4).is_empty());
        assert!(pool.add(2u64, 4).is_empty());
        assert_eq!(pool.current_usage(), 8);

        // Adding item 3 (cost 4) should evict item 1 (oldest)
        let pruned = pool.add(3u64, 4);
        assert_eq!(pruned, vec![1u64]);
        assert_eq!(pool.current_usage(), 8); // 2(4) + 3(4)

        // Touch item 2, then add item 4 (cost 4)
        pool.touch(2u64);
        let pruned = pool.add(4u64, 4);
        assert_eq!(pruned, vec![3u64]); // 3 was oldest after 2 was touched
        assert_eq!(pool.current_usage(), 8); // 2(4) + 4(4)
    }

    #[test]
    fn test_lru_update_cost() {
        let mut pool = LruPool::new(10);
        pool.add(1u64, 5);
        pool.add(2u64, 3);
        assert_eq!(pool.current_usage(), 8);

        // Update item 1 cost
        let pruned = pool.add(1u64, 8);
        assert_eq!(pruned, vec![2u64]); // 1(8) + 2(3) = 11 > 10, 2 is oldest
        assert_eq!(pool.current_usage(), 8);
    }

    #[test]
    fn test_lru_remove() {
        let mut pool = LruPool::new(10);
        pool.add(1u64, 5);
        pool.add(2u64, 5);
        assert_eq!(pool.current_usage(), 10);

        pool.remove(1u64);
        assert_eq!(pool.current_usage(), 5);

        // Now adding 3 shouldn't evict 2
        assert!(pool.add(3u64, 5).is_empty());
        assert_eq!(pool.current_usage(), 10);
    }

    #[test]
    fn test_lru_oversized_item() {
        let mut pool = LruPool::new(10);

        // Item larger than capacity should be pruned immediately
        let pruned = pool.add(1u64, 15);
        assert_eq!(pruned, vec![1u64]);
        assert_eq!(pool.current_usage(), 0);

        pool.add(2u64, 5);
        let pruned = pool.add(3u64, 15);
        assert_eq!(pruned, vec![2u64, 3u64]);
        assert_eq!(pool.current_usage(), 0);
    }

    #[test]
    fn test_lru_complex_sequence() {
        let mut pool = LruPool::new(100);

        for i in 0u64..10u64 {
            pool.add(i, 10);
        }
        assert_eq!(pool.current_usage(), 100);

        // Touch even items
        for i in (0u64..10u64).step_by(2) {
            pool.touch(i);
        }
        // Order is now: 1, 3, 5, 7, 9, 0, 2, 4, 6, 8

        // Add item 10 (cost 25)
        let pruned = pool.add(10u64, 25);
        assert_eq!(pruned, vec![1u64, 3u64, 5u64]); // 10+10+10 = 30. 100-30 = 70. 70+25=95.
        assert_eq!(pool.current_usage(), 95);

        // Add item 11 (cost 50)
        let pruned = pool.add(11u64, 50);
        // Current usage 95. Need to free 45.
        // Order: 7, 9, 0, 2, 4, 6, 8, 10
        // 7(10), 9(10), 0(10), 2(10), 4(10) -> 50 freed.
        assert_eq!(pruned, vec![7u64, 9u64, 0u64, 2u64, 4u64]);
        assert_eq!(pool.current_usage(), 95); // 6(10)+8(10)+10(25)+11(50) = 95
    }

    #[test]
    fn test_keyed_lru_basic() {
        let mut pool = KeyedLruPool::new(10);
        assert!(pool.insert_key(&"a", 5).is_empty());
        assert!(pool.insert_key(&"b", 5).is_empty());
        pool.touch_key(&"a");
        let pruned = pool.insert_key(&"c", 5);
        assert_eq!(pruned, vec!["b"]);
    }

    #[test]
    fn test_keyed_lru_stale_slot_generation() {
        let mut pool = KeyedLruPool::new(10);
        assert!(pool.insert_key(&"a", 10).is_empty());
        let pruned = pool.insert_key(&"b", 10);
        assert_eq!(pruned, vec!["a"]);
        let pruned = pool.insert_key(&"a", 1);
        assert_eq!(pruned, vec!["b"]);
        pool.remove_key(&"b");
        // Ensure stale IDs cannot remove newly inserted key sharing reused slot.
        assert!(pool.insert_key(&"a", 1).is_empty());
    }
}
