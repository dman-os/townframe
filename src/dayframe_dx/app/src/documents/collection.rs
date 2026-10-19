use std::collections::BTreeMap;

pub(super) enum CollectionChange<K, V> {
    Upsert { key: K, value: V },
    Remove { key: K },
}

pub(super) struct Collection<K, V> {
    base: BTreeMap<K, V>,
    pending: BTreeMap<u64, CollectionChange<K, V>>,
}

impl<K: Ord + Clone, V: Clone> Collection<K, V> {
    pub(super) fn new() -> Self {
        Self {
            base: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    pub(super) fn get(&self, key: &K) -> Option<V> {
        for change in self.pending.values().rev() {
            match change {
                CollectionChange::Upsert {
                    key: changed,
                    value,
                } if changed == key => return Some(value.clone()),
                CollectionChange::Remove { key: changed } if changed == key => return None,
                CollectionChange::Upsert { .. } | CollectionChange::Remove { .. } => {}
            }
        }
        self.base.get(key).cloned()
    }

    pub(super) fn apply_authoritative(&mut self, change: CollectionChange<K, V>) {
        match change {
            CollectionChange::Upsert { key, value } => {
                self.base.insert(key, value);
            }
            CollectionChange::Remove { key } => {
                self.base.remove(&key);
            }
        }
    }

    pub(super) fn apply_optimistic(&mut self, id: u64, change: CollectionChange<K, V>) {
        assert!(
            self.pending.insert(id, change).is_none(),
            "duplicate operation"
        );
    }

    pub(super) fn accept(&mut self, id: u64) {
        let change = self
            .pending
            .remove(&id)
            .expect("operation overlay disappeared");
        self.apply_authoritative(change);
    }

    pub(super) fn rollback(&mut self, id: u64) {
        self.pending
            .remove(&id)
            .expect("operation overlay disappeared");
    }
}
