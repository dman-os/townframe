use crate::interlude::*;
use std::collections::VecDeque;

/// A non-generic LRU policy engine.
///
/// It tracks abstract "IDs" and their associated "costs".
/// When the total cost exceeds capacity, it returns the IDs that should be pruned
/// based on the Least Recently Used strategy.
pub type LruItemId = u64;
pub type SharedKeyedLruPool<K> = Arc<std::sync::Mutex<KeyedLruPool<K>>>;

pub struct LruPool {
    capacity: usize,
    current_usage: usize,
    items: HashMap<LruItemId, usize>, // id -> cost
    order: VecDeque<LruItemId>,
}

impl LruPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            current_usage: 0,
            items: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Adds or updates an item in the pool.
    /// Returns a list of IDs that should be pruned to stay within capacity.
    pub fn add(&mut self, id: LruItemId, cost: usize) -> Vec<LruItemId> {
        if let Some(old_cost) = self.items.insert(id, cost) {
            self.current_usage -= old_cost;
            // Move to back (most recently used)
            if let Some(pos) = self.order.iter().position(|order_id| *order_id == id) {
                self.order.remove(pos);
            }
        }
        self.current_usage += cost;
        self.order.push_back(id);

        let mut pruned = Vec::new();
        // Prune until we are under capacity.
        // We never prune the item we just added unless it's the only item
        // and its cost is greater than the total capacity.
        while self.current_usage > self.capacity && self.order.len() > 1 {
            let oldest_id = self.order.pop_front().unwrap();
            if let Some(old_cost) = self.items.remove(&oldest_id) {
                self.current_usage -= old_cost;
                pruned.push(oldest_id);
            }
        }

        // Edge case: single item exceeds capacity
        if self.current_usage > self.capacity && !self.order.is_empty() {
            let only_id = self.order.pop_front().unwrap();
            if let Some(old_cost) = self.items.remove(&only_id) {
                self.current_usage -= old_cost;
                pruned.push(only_id);
            }
        }

        pruned
    }

    /// Marks an ID as recently used without changing its cost.
    pub fn touch(&mut self, id: LruItemId) {
        if self.items.contains_key(&id) {
            if let Some(pos) = self.order.iter().position(|oid| *oid == id) {
                let id = self.order.remove(pos).unwrap();
                self.order.push_back(id);
            }
        }
    }

    /// Removes an ID from the pool.
    pub fn remove(&mut self, id: LruItemId) {
        if let Some(cost) = self.items.remove(&id) {
            self.current_usage -= cost;
            if let Some(pos) = self.order.iter().position(|oid| *oid == id) {
                self.order.remove(pos);
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

pub struct KeyedLruPool<K> {
    policy: LruPool,
    key_to_slot: HashMap<K, usize>,
    slots: Vec<Option<K>>,
    slot_to_id: HashMap<usize, LruItemId>,
    id_to_slot: HashMap<LruItemId, usize>,
    slot_generations: Vec<u32>,
    free_slots: Vec<usize>,
}

impl<K: std::hash::Hash + Eq + Clone> KeyedLruPool<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            policy: LruPool::new(capacity),
            key_to_slot: HashMap::new(),
            slots: Vec::new(),
            slot_to_id: HashMap::new(),
            id_to_slot: HashMap::new(),
            slot_generations: Vec::new(),
            free_slots: Vec::new(),
        }
    }

    pub fn insert_key(&mut self, key: &K, cost: usize) -> Vec<K> {
        let id = self.id_for_or_insert_key(key);
        let pruned_ids = self.policy.add(id, cost);
        self.prune_ids(pruned_ids)
    }

    pub fn touch_key(&mut self, key: &K) {
        if let Some(slot) = self.key_to_slot.get(key).copied() {
            if let Some(id) = self.slot_to_id.get(&slot).copied() {
                self.policy.touch(id);
            }
        }
    }

    pub fn remove_key(&mut self, key: &K) {
        if let Some(slot) = self.key_to_slot.remove(key) {
            self.remove_slot(slot);
        }
    }

    pub fn remove_keys<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = K>,
    {
        for key in keys {
            self.remove_key(&key);
        }
    }

    fn id_for_or_insert_key(&mut self, key: &K) -> LruItemId {
        if let Some(slot) = self.key_to_slot.get(key).copied() {
            return self
                .slot_to_id
                .get(&slot)
                .copied()
                .expect("slot present but no id");
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
        self.slot_to_id.insert(slot, id);
        self.id_to_slot.insert(id, slot);

        id
    }

    fn prune_ids(&mut self, pruned_ids: Vec<LruItemId>) -> Vec<K> {
        let mut pruned_keys = Vec::new();
        for id in pruned_ids {
            if let Some(slot) = self.id_to_slot.get(&id).copied() {
                if let Some(key) = self.remove_slot(slot) {
                    pruned_keys.push(key);
                }
            }
        }
        pruned_keys
    }

    fn remove_slot(&mut self, slot: usize) -> Option<K> {
        let id = self.slot_to_id.remove(&slot);
        if let Some(id) = id {
            self.id_to_slot.remove(&id);
            self.policy.remove(id);
        }

        let key = self.slots[slot].take();
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
