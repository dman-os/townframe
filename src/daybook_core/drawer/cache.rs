use crate::interlude::*;

use super::DrawerRepo;
use crate::drawer::lru::SharedKeyedLruPool;

use daybook_types::doc::{ChangeHashSet, DocId, FacetRaw};

pub struct FacetCacheState {
    pub entries: HashMap<FacetCacheKey, FacetCacheEntry>,
    by_doc: HashMap<DocId, HashSet<Uuid>>,
    pool: SharedKeyedLruPool<FacetCacheKey>,
    seen_once: HashSet<FacetCacheKey>,
    seen_order: std::collections::VecDeque<FacetCacheKey>,
    seen_capacity: usize,
}

pub type FacetCacheKey = (DocId, Uuid);

pub struct FacetCacheEntry {
    heads: ChangeHashSet,
    value: daybook_types::doc::ArcFacetRaw,
}

impl FacetCacheState {
    pub fn new(pool: SharedKeyedLruPool<FacetCacheKey>) -> Self {
        Self {
            entries: HashMap::new(),
            by_doc: HashMap::new(),
            pool,
            seen_once: HashSet::new(),
            seen_order: std::collections::VecDeque::new(),
            seen_capacity: 4096,
        }
    }

    fn estimate_cost(value: &FacetRaw) -> usize {
        fn approx_json_cost(value: &serde_json::Value, depth: usize, budget: &mut usize) -> usize {
            if *budget == 0 {
                return 64;
            }
            *budget -= 1;
            if depth >= 8 {
                return 64;
            }
            match value {
                serde_json::Value::Null => 4,
                serde_json::Value::Bool(_) => 4,
                serde_json::Value::Number(_) => 8,
                serde_json::Value::String(text) => text.len().min(1024) + 2,
                serde_json::Value::Array(items) => {
                    let mut cost = 8;
                    for item in items.iter().take(64) {
                        cost += approx_json_cost(item, depth + 1, budget);
                    }
                    cost
                }
                serde_json::Value::Object(map) => {
                    let mut cost = 16;
                    for (key, val) in map.iter().take(64) {
                        cost += key.len().min(256) + 1;
                        cost += approx_json_cost(val, depth + 1, budget);
                    }
                    cost
                }
            }
        }

        let mut budget = 512;
        approx_json_cost(value, 0, &mut budget).max(128)
    }

    fn remember_seen_once(&mut self, key: FacetCacheKey) {
        if self.seen_once.contains(&key) {
            return;
        }
        self.seen_once.insert(key.clone());
        self.seen_order.push_back(key);
        while self.seen_order.len() > self.seen_capacity {
            if let Some(evicted) = self.seen_order.pop_front() {
                self.seen_once.remove(&evicted);
            }
        }
    }

    pub(super) fn get_if_heads_match(
        &mut self,
        doc_id: &DocId,
        facet_uuid: &Uuid,
        heads: &ChangeHashSet,
    ) -> Option<daybook_types::doc::ArcFacetRaw> {
        let key = (doc_id.clone(), *facet_uuid);
        let cached = self.entries.get(&key)?;
        if &cached.heads != heads {
            return None;
        }
        self.pool.lock().unwrap().touch_key(&key);
        Some(Arc::clone(&cached.value))
    }

    pub fn put(
        &mut self,
        doc_id: &DocId,
        facet_uuid: Uuid,
        facet_heads: ChangeHashSet,
        value: daybook_types::doc::ArcFacetRaw,
    ) {
        let cache_key = (doc_id.clone(), facet_uuid);
        let cost = Self::estimate_cost(value.as_ref());
        if self.entries.contains_key(&cache_key) {
            let pruned = self.pool.lock().unwrap().insert_key(&cache_key, cost);
            let self_pruned = pruned.iter().any(|pkey| pkey == &cache_key);
            for pkey in pruned {
                self.remove_without_pool(&pkey);
            }
            if self_pruned {
                return;
            }
            let existing_entry = self
                .entries
                .get_mut(&cache_key)
                .expect("entry must exist after non-self prune");
            existing_entry.heads = facet_heads;
            existing_entry.value = value;
            self.by_doc
                .entry(doc_id.clone())
                .or_default()
                .insert(facet_uuid);
            self.seen_once.remove(&cache_key);
            return;
        }

        if !self.seen_once.remove(&cache_key) {
            self.remember_seen_once(cache_key);
            return;
        }

        let pruned = self.pool.lock().unwrap().insert_key(&cache_key, cost);
        let self_pruned = pruned.iter().any(|pkey| pkey == &cache_key);
        for pkey in pruned {
            self.remove_without_pool(&pkey);
        }
        if self_pruned {
            return;
        }

        self.entries.insert(
            cache_key.clone(),
            FacetCacheEntry {
                heads: facet_heads,
                value,
            },
        );
        self.by_doc
            .entry(doc_id.clone())
            .or_default()
            .insert(facet_uuid);
    }

    fn invalidate_facet(&mut self, doc_id: &DocId, facet_uuid: &Uuid) {
        let key = (doc_id.clone(), *facet_uuid);
        self.pool.lock().unwrap().remove_key(&key);
        self.remove_without_pool(&key);
    }

    fn invalidate_doc(&mut self, doc_id: &DocId) {
        let Some(uuids) = self.by_doc.get(doc_id).cloned() else {
            return;
        };
        let keys: Vec<FacetCacheKey> = uuids
            .into_iter()
            .map(|uuid| (doc_id.clone(), uuid))
            .collect();
        self.pool.lock().unwrap().remove_keys(keys.clone());
        for key in keys {
            self.remove_without_pool(&key);
        }
    }

    fn remove_without_pool(&mut self, key: &FacetCacheKey) {
        self.seen_order.retain(|queued_key| queued_key != key);
        let removed = self.entries.remove(key);
        self.seen_once.remove(key);
        if removed.is_none() {
            return;
        }
        let (doc_id, facet_uuid) = key;
        if let Some(per_doc) = self.by_doc.get_mut(doc_id) {
            per_doc.remove(facet_uuid);
            if per_doc.is_empty() {
                self.by_doc.remove(doc_id);
            }
        }
    }
}

impl DrawerRepo {
    pub(super) fn invalidate_entry_cache(&self, id: &DocId) {
        // Keep pool/cache ordering strict: remove from `entry_pool` (via `remove_key`)
        // before deleting from `entry_cache` so the pool cannot retain stale refs.
        let mut pool = self.entry_pool.lock().unwrap();
        pool.remove_key(id);
        self.entry_cache.remove(id);
    }

    pub(super) fn invalidate_facet_cache_entry(&self, doc_id: &DocId, facet_uuid: &Uuid) {
        self.facet_cache
            .lock()
            .unwrap()
            .invalidate_facet(doc_id, facet_uuid);
    }

    pub(super) fn invalidate_facet_cache_doc(&self, doc_id: &DocId) {
        self.facet_cache.lock().unwrap().invalidate_doc(doc_id);
    }

    pub(super) fn facet_cache_get(
        &self,
        doc_id: &DocId,
        facet_uuid: &Uuid,
        facet_heads: &ChangeHashSet,
    ) -> Option<daybook_types::doc::ArcFacetRaw> {
        self.facet_cache
            .lock()
            .unwrap()
            .get_if_heads_match(doc_id, facet_uuid, facet_heads)
    }

    pub(super) fn facet_cache_put(
        &self,
        doc_id: &DocId,
        facet_uuid: Uuid,
        facet_heads: ChangeHashSet,
        value: daybook_types::doc::ArcFacetRaw,
    ) {
        self.facet_cache
            .lock()
            .unwrap()
            .put(doc_id, facet_uuid, facet_heads, value);
    }
}
