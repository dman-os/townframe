use super::*;

#[derive(Default)]
pub(super) struct Scheduler {
    pub docs_to_stop: HashSet<DocumentId>,
    pub queued_tasks: HashSet<SyncTask>,
    pub pending_tasks: HashMap<SyncTask, PendingTaskState>,
    pub partitions_to_refresh: HashSet<PartitionKey>,
    pub peer_sessions_to_refresh: HashSet<PeerId>,
    pub active_docs: HashMap<DocSyncTaskKey, ActiveDocSyncState>,
    pub active_blobs: HashMap<Arc<str>, ActiveBlobSyncState>,
    pub blob_requirements: HashMap<Arc<str>, HashSet<PartitionKey>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) enum SyncTask {
    Doc(DocSyncTaskKey),
    Blob(Arc<str>),
}

#[derive(Debug, Clone)]
pub(super) struct PendingTaskState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
    pub due_at: std::time::Instant,
}

impl Scheduler {
    pub fn has_queued_docs(&self) -> bool {
        self.queued_tasks
            .iter()
            .any(|task| matches!(task, SyncTask::Doc(_)))
    }

    pub fn has_queued_blobs(&self) -> bool {
        self.queued_tasks
            .iter()
            .any(|task| matches!(task, SyncTask::Blob(_)))
    }

    pub fn is_doc_pending(&self, task_key: &DocSyncTaskKey) -> bool {
        self.pending_tasks
            .contains_key(&SyncTask::Doc(task_key.clone()))
    }

    pub fn pending_doc_state(&self, task_key: &DocSyncTaskKey) -> Option<PendingTaskState> {
        self.pending_tasks
            .get(&SyncTask::Doc(task_key.clone()))
            .cloned()
    }

    pub fn pending_blob_state(&self, hash: Arc<str>) -> Option<PendingTaskState> {
        self.pending_tasks
            .get(&SyncTask::Blob(Arc::clone(&hash)))
            .cloned()
    }

    pub fn enqueue_doc(&mut self, task_key: DocSyncTaskKey) {
        self.queued_tasks.insert(SyncTask::Doc(task_key));
    }

    pub fn enqueue_blob(&mut self, hash: Arc<str>) {
        self.queued_tasks.insert(SyncTask::Blob(hash));
    }

    pub fn clear_doc_task(&mut self, task_key: DocSyncTaskKey) {
        self.pending_tasks.remove(&SyncTask::Doc(task_key.clone()));
        self.queued_tasks.remove(&SyncTask::Doc(task_key));
    }

    pub fn clear_blob_task(&mut self, hash: Arc<str>) {
        self.pending_tasks
            .remove(&SyncTask::Blob(Arc::clone(&hash)));
        self.queued_tasks.remove(&SyncTask::Blob(hash));
    }

    pub fn set_doc_pending_now(&mut self, task_key: DocSyncTaskKey) {
        let now = std::time::Instant::now();
        self.pending_tasks
            .entry(SyncTask::Doc(task_key.clone()))
            .or_insert(PendingTaskState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: now,
                due_at: now,
            });
        self.enqueue_doc(task_key);
    }

    pub fn set_blob_pending_now(&mut self, hash: Arc<str>) {
        let now = std::time::Instant::now();
        self.pending_tasks
            .entry(SyncTask::Blob(Arc::clone(&hash)))
            .or_insert(PendingTaskState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: now,
                due_at: now,
            });
        self.enqueue_blob(hash);
    }

    pub fn set_doc_backoff(&mut self, task_key: &DocSyncTaskKey, pending: PendingTaskState) {
        self.pending_tasks
            .insert(SyncTask::Doc(task_key.clone()), pending);
    }

    pub fn set_blob_backoff(&mut self, hash: Arc<str>, pending: PendingTaskState) {
        self.pending_tasks
            .insert(SyncTask::Blob(Arc::clone(&hash)), pending);
    }

    pub fn clear_doc_pending(&mut self, task_key: DocSyncTaskKey) {
        self.pending_tasks.remove(&SyncTask::Doc(task_key));
    }

    pub fn clear_blob_pending(&mut self, hash: Arc<str>) {
        self.pending_tasks.remove(&SyncTask::Blob(hash));
    }

    pub fn drain_queued_docs(&mut self, budget: usize) -> Vec<DocSyncTaskKey> {
        if budget == 0 {
            return Vec::new();
        }
        let docs: Vec<DocSyncTaskKey> = self
            .queued_tasks
            .iter()
            .filter_map(|task| match task {
                SyncTask::Doc(task_key) => Some(task_key.clone()),
                SyncTask::Blob(_) => None,
            })
            .take(budget)
            .collect();
        for task_key in &docs {
            self.queued_tasks.remove(&SyncTask::Doc(task_key.clone()));
        }
        docs
    }

    pub fn drain_queued_blobs(&mut self, budget: usize) -> Vec<Arc<str>> {
        if budget == 0 {
            return Vec::new();
        }
        for hash in self.queued_tasks.iter().filter_map(|task| match task {
            SyncTask::Blob(hash) => Some(Arc::clone(&hash)),
            SyncTask::Doc(_) => None,
        }) {
            self.queued_tasks.remove(&SyncTask::Blob(hash));
        }
        blobs
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
        let blob_demand = self.active_blobs.len()
            + self
                .queued_tasks
                .iter()
                .filter(|task| matches!(task, SyncTask::Blob(_)))
                .count();
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
        let doc_demand = self.active_docs.len()
            + self
                .queued_tasks
                .iter()
                .filter(|task| matches!(task, SyncTask::Doc(_)))
                .count();
        let reserved_for_doc = MIN_DOC_WORKER_FLOOR.min(doc_demand);
        let blob_cap = max_active_sync_workers.saturating_sub(reserved_for_doc);
        let remaining_blob_cap = blob_cap.saturating_sub(self.active_blobs.len());
        remaining_total.min(remaining_blob_cap)
    }

    pub fn backoff_janitor_enqueue_due(&mut self, max_active_sync_workers: usize) {
        let now = std::time::Instant::now();
        let doc_budget = self.available_doc_boot_budget(max_active_sync_workers);
        let blob_budget = self.available_blob_boot_budget(max_active_sync_workers);

        let due_docs: Vec<_> = self
            .pending_tasks
            .iter()
            .filter_map(|(task, pending)| match task {
                SyncTask::Doc(task_key)
                    if pending.due_at <= now && !self.active_docs.contains_key(task_key) =>
                {
                    Some(task_key.clone())
                }
                _ => None,
            })
            .take(doc_budget)
            .collect();
        for task_key in due_docs {
            self.enqueue_doc(task_key);
        }

        let due_blobs: Vec<_> = self
            .pending_tasks
            .iter()
            .filter_map(|(task, pending)| match task {
                SyncTask::Blob(hash) => {
                    if pending.due_at <= now && !self.active_blobs.contains_key(hash) {
                        Some(hash.clone())
                    } else {
                        None
                    }
                }
                SyncTask::Doc(_) => None,
            })
            .take(blob_budget)
            .collect();
        for hash in due_blobs {
            self.enqueue_blob(hash);
        }
    }

    pub fn endpoint_has_doc_work(&self, pid: PeerId) -> bool {
        self.active_docs.keys().any(|key| key.peer_id == pid)
            || self
                .queued_tasks
                .iter()
                .any(|task| matches!(task, SyncTask::Doc(key) if key.peer_id == pid))
            || self
                .pending_tasks
                .keys()
                .any(|task| matches!(task, SyncTask::Doc(key) if key.peer_id == pid))
    }

    pub fn doc_task_keys_for_doc(&self, doc_id: &DocumentId) -> HashSet<DocSyncTaskKey> {
        let mut keys = HashSet::new();
        keys.extend(
            self.active_docs
                .keys()
                .filter(|key| &key.doc_id == doc_id)
                .cloned(),
        );
        keys.extend(self.pending_tasks.keys().filter_map(|task| match task {
            SyncTask::Doc(key) if &key.doc_id == doc_id => Some(key.clone()),
            SyncTask::Doc(_) | SyncTask::Blob(_) => None,
        }));
        keys.extend(self.queued_tasks.iter().filter_map(|task| match task {
            SyncTask::Doc(key) if &key.doc_id == doc_id => Some(key.clone()),
            SyncTask::Doc(_) | SyncTask::Blob(_) => None,
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
        keys.extend(self.pending_tasks.keys().filter_map(|task| match task {
            SyncTask::Doc(key) if key.peer_id == peer_id => Some(key.clone()),
            SyncTask::Doc(_) | SyncTask::Blob(_) => None,
        }));
        keys.extend(self.queued_tasks.iter().filter_map(|task| match task {
            SyncTask::Doc(key) if key.peer_id == peer_id => Some(key.clone()),
            SyncTask::Doc(_) | SyncTask::Blob(_) => None,
        }));
        keys
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
        scheduler.clear_doc_task(task_key);
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
        scheduler.enqueue_doc(task_key.clone());

        let batch = scheduler.drain_queued_docs(32);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], task_key);
        assert!(scheduler.is_doc_pending(&batch[0]));
    }

    #[test]
    fn blob_task_dedup_single_pending_entry() {
        let mut scheduler = Scheduler::default();
        let hash: Arc<str> = "bafkreigh2akiscaildcv".into();

        scheduler.set_blob_pending_now(Arc::clone(&hash));
        scheduler.set_blob_pending_now(Arc::clone(&hash));
        scheduler.enqueue_blob(hash.clone());

        let batch = scheduler.drain_queued_blobs(32);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], hash);
        assert!(scheduler.pending_blob_state(batch[0]).is_some());
    }
}
