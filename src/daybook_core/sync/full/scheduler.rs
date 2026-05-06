//! FIXME: This can be made generic and DRYed up

use super::*;

#[derive(Default)]
pub(super) struct Scheduler {
    docs_to_stop: HashSet<DocSyncTaskKey>,
    blobs_to_stop: HashSet<Hash>,

    // pending set reperesents all tasks that are not active
    pending_docs: HashMap<DocSyncTaskKey, PendingTaskState>,
    // tasks ready to boot and is a subset of pending
    docs_to_boot: HashSet<DocSyncTaskKey>,
    active_docs: HashMap<DocSyncTaskKey, ActiveDocSyncState>,

    blobs_to_boot: HashSet<Hash>,
    pending_blobs: HashMap<Hash, PendingTaskState>,
    active_blobs: HashMap<Hash, ActiveBlobSyncState>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingTaskState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
    pub due_at: std::time::Instant,
}

impl Scheduler {
    pub fn pending_doc_state(&self, task_key: &DocSyncTaskKey) -> Option<PendingTaskState> {
        self.pending_docs.get(&task_key).cloned()
    }

    pub fn pending_blob_state(&self, hash: Hash) -> Option<PendingTaskState> {
        self.pending_blobs.get(&hash).cloned()
    }
    pub fn has_docs_to_boot(&self) -> bool {
        !self.docs_to_boot.is_empty()
    }

    pub fn has_blobs_to_boot(&self) -> bool {
        !self.blobs_to_boot.is_empty()
    }

    pub fn enqueue_start_doc(&mut self, task_key: DocSyncTaskKey) {
        self.docs_to_boot.insert(task_key);
    }

    pub fn enqueue_start_blob(&mut self, hash: Hash) {
        self.blobs_to_boot.insert(hash);
    }

    pub fn enqueue_stop_doc(&mut self, task_key: &DocSyncTaskKey) -> bool {
        self.pending_docs.remove(&task_key);
        self.docs_to_boot.remove(&task_key);
        if self.active_docs.contains_key(&task_key) {
            self.docs_to_stop.insert(task_key.clone());
            true
        } else {
            false
        }
    }
    pub fn clear_doc_task(&mut self, task_key: &DocSyncTaskKey) -> Option<ActiveDocSyncState> {
        self.pending_docs.remove(&task_key);
        self.docs_to_boot.remove(&task_key);
        self.active_docs.remove(&task_key)
    }

    pub fn clear_blob_task(&mut self, hash: Hash) -> Option<ActiveBlobSyncState> {
        self.pending_blobs.remove(&hash);
        self.blobs_to_boot.remove(&hash);
        self.active_blobs.remove(&hash)
    }

    pub fn enqueue_stop_blob(&mut self, hash: Hash) -> bool {
        self.pending_blobs.remove(&hash);
        self.blobs_to_boot.remove(&hash);
        if self.active_blobs.contains_key(&hash) {
            self.blobs_to_stop.insert(hash);
            true
        } else {
            false
        }
    }

    pub fn clear_all_tasks(&mut self) {
        self.pending_docs.clear();
        self.pending_blobs.clear();
        self.docs_to_boot.clear();
        self.blobs_to_boot.clear();
        self.docs_to_stop.extend(self.active_docs.keys().cloned());
        self.blobs_to_stop.extend(self.active_blobs.keys().cloned());
    }
    pub fn activate_doc(&mut self, task_key: DocSyncTaskKey, active: ActiveDocSyncState) {
        self.pending_docs.remove(&task_key);
        self.active_docs.insert(task_key, active);
    }

    pub fn activate_blob(&mut self, hash: Hash, active: ActiveBlobSyncState) {
        self.pending_blobs.remove(&hash);
        self.active_blobs.insert(hash, active);
    }

    pub fn is_doc_active(&self, key: &DocSyncTaskKey) -> bool {
        self.active_docs.contains_key(key)
    }

    pub fn is_blob_active(&self, key: &Hash) -> bool {
        self.active_blobs.contains_key(key)
    }

    pub fn set_doc_pending_now(&mut self, task_key: DocSyncTaskKey) {
        if self.active_docs.contains_key(&task_key) || self.pending_docs.contains_key(&task_key) {
            return;
        }
        let now = std::time::Instant::now();
        self.pending_docs
            .entry(task_key.clone())
            .or_insert(PendingTaskState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: now,
                due_at: now,
            });
        self.enqueue_start_doc(task_key);
    }

    pub fn set_blob_pending_now(&mut self, hash: Hash) {
        // FIXME: why don't we look at enqueue_docs?
        if self.active_blobs.contains_key(&hash) || self.pending_blobs.contains_key(&hash) {
            return;
        }
        let now = std::time::Instant::now();
        self.pending_blobs.entry(hash).or_insert(PendingTaskState {
            attempt_no: 0,
            last_backoff: Duration::from_millis(0),
            last_attempt_at: now,
            due_at: now,
        });
        self.enqueue_start_blob(hash);
    }

    pub fn set_doc_backoff(&mut self, task_key: DocSyncTaskKey, pending: PendingTaskState) {
        self.pending_docs.insert(task_key, pending);
    }

    pub fn set_blob_backoff(&mut self, hash: Hash, pending: PendingTaskState) {
        self.pending_blobs.insert(hash, pending);
    }

    pub fn drain_queued_docs(&mut self, mut budget: usize) -> Vec<DocSyncTaskKey> {
        if budget == 0 {
            return Vec::new();
        }
        while budget > 0 {
            budget -= 1;
        }
        let mut docs = self.docs_to_boot.iter().take(budget).cloned().collect();
        for task_key in &docs {
            self.docs_to_boot.remove(task_key);
        }
        docs
    }

    pub fn drain_queued_blobs(&mut self, budget: usize) -> Vec<Hash> {
        if budget == 0 {
            return Vec::new();
        }
        let blobs = self.blobs_to_boot.iter().take(budget).cloned().collect();
        for hash in &blobs {
            self.blobs_to_boot.remove(hash);
        }
        blobs
    }

    pub fn drain_stop_doc_queue(&mut self) -> Vec<DocSyncTaskKey> {
        self.docs_to_stop.drain().collect()
    }
    pub fn drain_stop_blob_queue(&mut self) -> Vec<Hash> {
        self.blobs_to_stop.drain().collect()
    }

    pub fn active_worker_count(&self) -> usize {
        self.active_docs.len() + self.active_blobs.len()
    }

    pub fn available_total_boot_budget(&self, max_active_sync_workers: usize) -> usize {
        max_active_sync_workers.saturating_sub(self.active_worker_count())
    }

    pub fn available_doc_boot_budget(&self, max_active_sync_workers: usize) -> usize {
        let remaining_total = self.available_total_boot_budget(max_active_sync_workers);
        if remaining_total == 0 {
            return 0;
        }
        let blob_demand = self.active_blobs.len() + self.blobs_to_boot.len();
        let reserved_for_blob = MIN_BLOB_WORKER_FLOOR.min(blob_demand);
        let doc_cap = max_active_sync_workers.saturating_sub(reserved_for_blob);
        let remaining_doc_cap = doc_cap.saturating_sub(self.active_docs.len());
        remaining_total.min(remaining_doc_cap)
    }

    pub fn available_blob_boot_budget(&self, max_active_sync_workers: usize) -> usize {
        let remaining_total = self.available_total_boot_budget(max_active_sync_workers);
        if remaining_total == 0 {
            return 0;
        }
        let doc_demand = self.active_docs.len() + self.blobs_to_boot.len();
        let reserved_for_doc = MIN_DOC_WORKER_FLOOR.min(doc_demand);
        let blob_cap = max_active_sync_workers.saturating_sub(reserved_for_doc);
        let remaining_blob_cap = blob_cap.saturating_sub(self.active_blobs.len());
        remaining_total.min(remaining_blob_cap)
    }

    pub fn backoff_janitor_enqueue_due(&mut self, max_active_sync_workers: usize) {
        let now = std::time::Instant::now();
        let doc_budget = self.available_doc_boot_budget(max_active_sync_workers);
        let blob_budget = self.available_blob_boot_budget(max_active_sync_workers);

        let tasks: Vec<_> = self
            .pending_docs
            .iter()
            .filter_map(|(task_key, pending)| {
                if pending.due_at <= now && !self.active_docs.contains_key(task_key) {
                    Some(task_key.clone())
                } else {
                    None
                }
            })
            .take(doc_budget)
            .collect();
        for task_key in tasks {
            self.enqueue_start_doc(task_key);
        }

        let tasks: Vec<_> = self
            .pending_blobs
            .iter()
            .filter_map(|(hash, pending)| {
                if pending.due_at <= now && !self.active_blobs.contains_key(hash) {
                    Some(hash.clone())
                } else {
                    None
                }
            })
            .take(blob_budget)
            .collect();
        for hash in tasks {
            self.enqueue_start_blob(hash);
        }
    }

    pub fn endpoint_has_doc_work(&self, pid: PeerId) -> bool {
        self.active_docs.keys().any(|key| key.peer_id == pid)
            || self.docs_to_boot.iter().any(|key| key.peer_id == pid)
            || self.pending_docs.keys().any(|key| key.peer_id == pid)
    }

    pub fn doc_task_keys_for_doc(&self, doc_id: &DocumentId) -> HashSet<DocSyncTaskKey> {
        let mut keys = HashSet::new();
        keys.extend(
            self.active_docs
                .keys()
                .filter(|key| &key.doc_id == doc_id)
                .cloned(),
        );
        keys.extend(self.pending_docs.keys().filter_map(|key| {
            if &key.doc_id == doc_id {
                Some(key.clone())
            } else {
                None
            }
        }));
        keys.extend(self.docs_to_boot.iter().filter_map(|key| {
            if &key.doc_id == doc_id {
                Some(key.clone())
            } else {
                None
            }
        }));
        keys
    }

    pub fn doc_task_keys_for_peer(&self, peer_id: PeerId) -> HashSet<DocSyncTaskKey> {
        let mut keys = HashSet::new();
        keys.extend(
            self.active_docs
                .keys()
                .filter(|key| key.peer_id == peer_id)
                .cloned(),
        );
        keys.extend(self.pending_docs.keys().filter_map(|key| {
            if key.peer_id == peer_id {
                Some(key.clone())
            } else {
                None
            }
        }));
        keys.extend(self.docs_to_boot.iter().filter_map(|key| {
            if key.peer_id == peer_id {
                Some(key.clone())
            } else {
                None
            }
        }));
        keys
    }

    pub fn docs_to_stop(&self) -> &HashSet<DocSyncTaskKey> {
        &self.docs_to_stop
    }

    pub fn blobs_to_stop(&self) -> &HashSet<Hash> {
        &self.blobs_to_stop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(seed: u8) -> PeerId {
        PeerId::new([seed; 32])
    }

    fn doc() -> DocumentId {
        DocumentId::random()
    }

    #[test]
    fn endpoint_has_doc_work_includes_doc_tasks() {
        let mut scheduler = Scheduler::default();
        let peer = endpoint(1);
        let task_key = DocSyncTaskKey {
            doc_id: doc(),
            peer_id: peer,
        };

        assert!(!scheduler.endpoint_has_doc_work(peer));
        scheduler.set_doc_pending_now(task_key.clone());
        assert!(scheduler.endpoint_has_doc_work(peer));
        scheduler.enqueue_stop_doc(&task_key);
        assert!(!scheduler.endpoint_has_doc_work(peer));
    }

    #[test]
    fn doc_task_key_discovery_stays_endpoint_scoped() {
        let mut scheduler = Scheduler::default();
        let doc_id = doc();
        let peer_a = endpoint(2);
        let peer_b = endpoint(3);
        let task_a = DocSyncTaskKey {
            doc_id: doc_id.clone(),
            peer_id: peer_a,
        };
        let task_b = DocSyncTaskKey {
            doc_id,
            peer_id: peer_b,
        };

        scheduler.set_doc_pending_now(task_a.clone());
        scheduler.set_doc_pending_now(task_b.clone());

        let peer_keys = scheduler.doc_task_keys_for_peer(peer_a);
        assert!(peer_keys.contains(&task_a));
        assert!(!peer_keys.contains(&task_b));
    }

    #[test]
    fn doc_task_dedup_single_pending_entry() {
        let mut scheduler = Scheduler::default();
        let task_key = DocSyncTaskKey {
            doc_id: doc(),
            peer_id: endpoint(1),
        };

        scheduler.set_doc_pending_now(task_key.clone());
        scheduler.set_doc_pending_now(task_key.clone());
        scheduler.enqueue_start_doc(task_key.clone());

        let batch = scheduler.drain_queued_docs(32);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], task_key);
        assert!(scheduler.pending_docs.contains_key(&batch[0]));
    }

    #[test]
    fn blob_task_dedup_single_pending_entry() {
        let mut scheduler = Scheduler::default();
        let hash: Hash = Hash::new("bafkreigh2akiscaildcv");

        scheduler.set_blob_pending_now(hash);
        scheduler.set_blob_pending_now(hash);
        scheduler.enqueue_start_blob(hash);

        let batch = scheduler.drain_queued_blobs(32);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], hash);
        assert!(scheduler.pending_blob_state(batch[0]).is_some());
    }
}
