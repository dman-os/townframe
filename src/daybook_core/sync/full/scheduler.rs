use super::*;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct Scheduler {
    pub docs_to_stop: HashSet<DocumentId>,
    pub queued_tasks: HashSet<SyncTask>,
    pub pending_tasks: HashMap<SyncTask, PendingTaskState>,
    pub partitions_to_refresh: HashSet<PartitionKey>,
    pub peer_sessions_to_refresh: HashSet<EndpointId>,
    pub active_docs: HashMap<DocumentId, ActiveDocSyncState>,
    pub active_imports: HashMap<DocumentId, ActiveImportSyncState>,
    pub active_blobs: HashMap<String, ActiveBlobSyncState>,
    pub blob_requirements: HashMap<String, HashSet<PartitionKey>>,
    pub cursor_ack_state: HashMap<EndpointId, HashMap<PartitionId, PartitionCursorAckState>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) enum SyncTask {
    Doc(DocumentId),
    Import(DocumentId),
    Blob(String),
}

#[derive(Debug, Clone)]
pub(super) struct PendingTaskState {
    pub attempt_no: usize,
    pub last_backoff: Duration,
    pub last_attempt_at: std::time::Instant,
    pub due_at: std::time::Instant,
}

#[derive(Default)]
pub(super) struct PartitionCursorAckState {
    pub slots: BTreeMap<u64, CursorAckSlotState>,
    pub last_emitted_cursor: Option<u64>,
}

pub(super) enum CursorAckSlotState {
    Pending(HashSet<String>),
    Ready,
}

impl Scheduler {
    pub fn has_queued_docs(&self) -> bool {
        self.queued_tasks
            .iter()
            .any(|task| matches!(task, SyncTask::Doc(_)))
    }

    pub fn has_queued_imports(&self) -> bool {
        self.queued_tasks
            .iter()
            .any(|task| matches!(task, SyncTask::Import(_)))
    }

    pub fn has_queued_blobs(&self) -> bool {
        self.queued_tasks
            .iter()
            .any(|task| matches!(task, SyncTask::Blob(_)))
    }

    pub fn is_doc_pending(&self, doc_id: &DocumentId) -> bool {
        self.pending_tasks
            .contains_key(&SyncTask::Doc(doc_id.clone()))
    }

    pub fn pending_doc_state(&self, doc_id: &DocumentId) -> Option<PendingTaskState> {
        self.pending_tasks
            .get(&SyncTask::Doc(doc_id.clone()))
            .cloned()
    }

    pub fn pending_blob_state(&self, hash: &str) -> Option<PendingTaskState> {
        self.pending_tasks
            .get(&SyncTask::Blob(hash.to_string()))
            .cloned()
    }

    pub fn pending_import_state(&self, doc_id: &DocumentId) -> Option<PendingTaskState> {
        self.pending_tasks
            .get(&SyncTask::Import(doc_id.clone()))
            .cloned()
    }

    pub fn enqueue_doc(&mut self, doc_id: DocumentId) {
        self.queued_tasks.insert(SyncTask::Doc(doc_id));
    }

    pub fn enqueue_blob(&mut self, hash: String) {
        self.queued_tasks.insert(SyncTask::Blob(hash));
    }

    pub fn enqueue_import(&mut self, doc_id: DocumentId) {
        self.queued_tasks.insert(SyncTask::Import(doc_id));
    }

    pub fn clear_doc_task(&mut self, doc_id: &DocumentId) {
        self.pending_tasks.remove(&SyncTask::Doc(doc_id.clone()));
        self.queued_tasks.remove(&SyncTask::Doc(doc_id.clone()));
    }

    pub fn clear_import_task(&mut self, doc_id: &DocumentId) {
        self.pending_tasks.remove(&SyncTask::Import(doc_id.clone()));
        self.queued_tasks.remove(&SyncTask::Import(doc_id.clone()));
    }

    pub fn clear_blob_task(&mut self, hash: &str) {
        self.pending_tasks.remove(&SyncTask::Blob(hash.to_string()));
        self.queued_tasks.remove(&SyncTask::Blob(hash.to_string()));
    }

    pub fn set_doc_pending_now(&mut self, doc_id: &DocumentId) {
        let now = std::time::Instant::now();
        self.pending_tasks
            .entry(SyncTask::Doc(doc_id.clone()))
            .or_insert(PendingTaskState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: now,
                due_at: now,
            });
        self.enqueue_doc(doc_id.clone());
    }

    pub fn set_blob_pending_now(&mut self, hash: &str) {
        let now = std::time::Instant::now();
        self.pending_tasks
            .entry(SyncTask::Blob(hash.to_string()))
            .or_insert(PendingTaskState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: now,
                due_at: now,
            });
        self.enqueue_blob(hash.to_string());
    }

    pub fn set_import_pending_now(&mut self, doc_id: &DocumentId) {
        let now = std::time::Instant::now();
        self.pending_tasks
            .entry(SyncTask::Import(doc_id.clone()))
            .or_insert(PendingTaskState {
                attempt_no: 0,
                last_backoff: Duration::from_millis(0),
                last_attempt_at: now,
                due_at: now,
            });
        self.enqueue_import(doc_id.clone());
    }

    pub fn set_doc_backoff(&mut self, doc_id: &DocumentId, pending: PendingTaskState) {
        self.pending_tasks
            .insert(SyncTask::Doc(doc_id.clone()), pending);
    }

    pub fn set_blob_backoff(&mut self, hash: &str, pending: PendingTaskState) {
        self.pending_tasks
            .insert(SyncTask::Blob(hash.to_string()), pending);
    }

    pub fn set_import_backoff(&mut self, doc_id: &DocumentId, pending: PendingTaskState) {
        self.pending_tasks
            .insert(SyncTask::Import(doc_id.clone()), pending);
    }

    pub fn clear_doc_pending(&mut self, doc_id: &DocumentId) {
        self.pending_tasks.remove(&SyncTask::Doc(doc_id.clone()));
    }

    pub fn clear_blob_pending(&mut self, hash: &str) {
        self.pending_tasks.remove(&SyncTask::Blob(hash.to_string()));
    }

    pub fn clear_import_pending(&mut self, doc_id: &DocumentId) {
        self.pending_tasks.remove(&SyncTask::Import(doc_id.clone()));
    }

    pub fn drain_queued_docs(&mut self, budget: usize) -> Vec<DocumentId> {
        if budget == 0 {
            return Vec::new();
        }
        let docs: Vec<DocumentId> = self
            .queued_tasks
            .iter()
            .filter_map(|task| match task {
                SyncTask::Doc(doc_id) => Some(doc_id.clone()),
                SyncTask::Import(_) | SyncTask::Blob(_) => None,
            })
            .take(budget)
            .collect();
        for doc_id in &docs {
            self.queued_tasks.remove(&SyncTask::Doc(doc_id.clone()));
        }
        docs
    }

    pub fn drain_queued_blobs(&mut self, budget: usize) -> Vec<String> {
        if budget == 0 {
            return Vec::new();
        }
        let blobs: Vec<String> = self
            .queued_tasks
            .iter()
            .filter_map(|task| match task {
                SyncTask::Blob(hash) => Some(hash.clone()),
                SyncTask::Doc(_) | SyncTask::Import(_) => None,
            })
            .take(budget)
            .collect();
        for hash in &blobs {
            self.queued_tasks.remove(&SyncTask::Blob(hash.clone()));
        }
        blobs
    }

    pub fn drain_queued_imports(&mut self, budget: usize) -> Vec<DocumentId> {
        if budget == 0 {
            return Vec::new();
        }
        let docs: Vec<DocumentId> = self
            .queued_tasks
            .iter()
            .filter_map(|task| match task {
                SyncTask::Import(doc_id) => Some(doc_id.clone()),
                SyncTask::Doc(_) | SyncTask::Blob(_) => None,
            })
            .take(budget)
            .collect();
        for doc_id in &docs {
            self.queued_tasks.remove(&SyncTask::Import(doc_id.clone()));
        }
        docs
    }

    pub fn active_worker_count(&self) -> usize {
        self.active_docs.len() + self.active_imports.len() + self.active_blobs.len()
    }

    pub fn available_total_boot_budget(&self, max_active_sync_workers: usize) -> usize {
        max_active_sync_workers.saturating_sub(self.active_worker_count())
    }

    pub fn available_doc_boot_budget(&self, max_active_sync_workers: usize) -> usize {
        let remaining_total = self.available_total_boot_budget(max_active_sync_workers);
        if remaining_total == 0 {
            return 0;
        }
        let import_demand = self.active_imports.len()
            + self
                .queued_tasks
                .iter()
                .filter(|task| matches!(task, SyncTask::Import(_)))
                .count();
        let blob_demand = self.active_blobs.len()
            + self
                .queued_tasks
                .iter()
                .filter(|task| matches!(task, SyncTask::Blob(_)))
                .count();
        let reserved_for_import = MIN_IMPORT_WORKER_FLOOR.min(import_demand);
        let reserved_for_blob = MIN_BLOB_WORKER_FLOOR.min(blob_demand);
        let doc_cap =
            max_active_sync_workers.saturating_sub(reserved_for_import + reserved_for_blob);
        let remaining_doc_cap = doc_cap.saturating_sub(self.active_docs.len());
        remaining_total.min(remaining_doc_cap)
    }

    pub fn available_import_boot_budget(&self, max_active_sync_workers: usize) -> usize {
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
        let blob_demand = self.active_blobs.len()
            + self
                .queued_tasks
                .iter()
                .filter(|task| matches!(task, SyncTask::Blob(_)))
                .count();
        let reserved_for_doc = MIN_DOC_WORKER_FLOOR.min(doc_demand);
        let reserved_for_blob = MIN_BLOB_WORKER_FLOOR.min(blob_demand);
        let import_cap =
            max_active_sync_workers.saturating_sub(reserved_for_doc + reserved_for_blob);
        let remaining_import_cap = import_cap.saturating_sub(self.active_imports.len());
        remaining_total.min(remaining_import_cap)
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
        let import_demand = self.active_imports.len()
            + self
                .queued_tasks
                .iter()
                .filter(|task| matches!(task, SyncTask::Import(_)))
                .count();
        let reserved_for_doc = MIN_DOC_WORKER_FLOOR.min(doc_demand);
        let reserved_for_import = MIN_IMPORT_WORKER_FLOOR.min(import_demand);
        let blob_cap =
            max_active_sync_workers.saturating_sub(reserved_for_doc + reserved_for_import);
        let remaining_blob_cap = blob_cap.saturating_sub(self.active_blobs.len());
        remaining_total.min(remaining_blob_cap)
    }

    pub fn backoff_janitor_enqueue_due(&mut self, max_active_sync_workers: usize) {
        let now = std::time::Instant::now();
        let doc_budget = self.available_doc_boot_budget(max_active_sync_workers);
        let import_budget = self.available_import_boot_budget(max_active_sync_workers);
        let blob_budget = self.available_blob_boot_budget(max_active_sync_workers);

        let due_docs: Vec<_> = self
            .pending_tasks
            .iter()
            .filter_map(|(task, pending)| match task {
                SyncTask::Doc(doc_id)
                    if pending.due_at <= now && !self.active_docs.contains_key(doc_id) =>
                {
                    Some(doc_id.clone())
                }
                _ => None,
            })
            .take(doc_budget)
            .collect();
        for doc_id in due_docs {
            self.enqueue_doc(doc_id);
        }

        let due_imports: Vec<_> = self
            .pending_tasks
            .iter()
            .filter_map(|(task, pending)| match task {
                SyncTask::Import(doc_id)
                    if pending.due_at <= now && !self.active_imports.contains_key(doc_id) =>
                {
                    Some(doc_id.clone())
                }
                _ => None,
            })
            .take(import_budget)
            .collect();
        for doc_id in due_imports {
            self.enqueue_import(doc_id);
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
                SyncTask::Doc(_) | SyncTask::Import(_) => None,
            })
            .take(blob_budget)
            .collect();
        for hash in due_blobs {
            self.enqueue_blob(hash);
        }
    }

    pub fn clear_peer_cursor_acks(&mut self, endpoint_id: EndpointId) {
        self.cursor_ack_state.remove(&endpoint_id);
    }

    pub fn note_doc_sync_requested(
        &mut self,
        endpoint_id: EndpointId,
        partition_id: &PartitionId,
        cursor: u64,
        doc_id: &str,
    ) {
        let part_state = self
            .cursor_ack_state
            .entry(endpoint_id)
            .or_default()
            .entry(partition_id.clone())
            .or_default();
        match part_state.slots.entry(cursor) {
            std::collections::btree_map::Entry::Vacant(vacant) => {
                vacant.insert(CursorAckSlotState::Pending(
                    [doc_id.to_string()].into_iter().collect(),
                ));
            }
            std::collections::btree_map::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    CursorAckSlotState::Pending(pending_docs) => {
                        pending_docs.insert(doc_id.to_string());
                    }
                    CursorAckSlotState::Ready => {
                        occupied.insert(CursorAckSlotState::Pending(
                            [doc_id.to_string()].into_iter().collect(),
                        ));
                    }
                }
            }
        }
    }

    pub fn note_cursor_ready_immediate(
        &mut self,
        endpoint_id: EndpointId,
        partition_id: &PartitionId,
        cursor: u64,
    ) {
        let part_state = self
            .cursor_ack_state
            .entry(endpoint_id)
            .or_default()
            .entry(partition_id.clone())
            .or_default();
        part_state
            .slots
            .entry(cursor)
            .or_insert(CursorAckSlotState::Ready);
    }

    pub fn note_doc_synced(
        &mut self,
        endpoint_id: EndpointId,
        partition_id: &PartitionId,
        cursor: u64,
        doc_id: &str,
    ) {
        let part_state = self
            .cursor_ack_state
            .entry(endpoint_id)
            .or_default()
            .entry(partition_id.clone())
            .or_default();
        match part_state.slots.entry(cursor) {
            std::collections::btree_map::Entry::Vacant(vacant) => {
                vacant.insert(CursorAckSlotState::Ready);
            }
            std::collections::btree_map::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    CursorAckSlotState::Pending(pending_docs) => {
                        pending_docs.remove(doc_id);
                        if pending_docs.is_empty() {
                            occupied.insert(CursorAckSlotState::Ready);
                        }
                    }
                    CursorAckSlotState::Ready => {}
                }
            }
        }
    }

    pub fn next_ready_cursor_to_ack(
        &self,
        endpoint_id: EndpointId,
        partition_id: &PartitionId,
        persisted_cursor: Option<u64>,
    ) -> Option<u64> {
        let part_state = self.cursor_ack_state.get(&endpoint_id)?.get(partition_id)?;
        let floor = persisted_cursor
            .unwrap_or(0)
            .max(part_state.last_emitted_cursor.unwrap_or(0));
        let mut latest_ready = None;
        let mut expected = floor.saturating_add(1);
        for (cursor, slot) in part_state.slots.range(expected..) {
            if *cursor != expected {
                break;
            }
            if !matches!(slot, CursorAckSlotState::Ready) {
                break;
            }
            latest_ready = Some(*cursor);
            expected = expected.saturating_add(1);
        }
        latest_ready
    }

    pub fn commit_ack_cursor(
        &mut self,
        endpoint_id: EndpointId,
        partition_id: &PartitionId,
        persisted_cursor: Option<u64>,
        cursor: u64,
    ) -> Res<()> {
        let Some(part_state) = self
            .cursor_ack_state
            .get_mut(&endpoint_id)
            .and_then(|parts| parts.get_mut(partition_id))
        else {
            return Ok(());
        };
        let floor = persisted_cursor
            .unwrap_or(0)
            .max(part_state.last_emitted_cursor.unwrap_or(0));
        while part_state
            .slots
            .first_key_value()
            .is_some_and(|(slot_cursor, _)| *slot_cursor <= floor)
        {
            part_state.slots.pop_first();
        }
        let mut expected = floor.saturating_add(1);
        while expected <= cursor {
            let Some(slot) = part_state.slots.get(&expected) else {
                eyre::bail!("cannot commit cursor {cursor}; missing slot for cursor {expected}");
            };
            if !matches!(slot, CursorAckSlotState::Ready) {
                eyre::bail!(
                    "cannot commit cursor {cursor}; slot for cursor {expected} is not ready"
                );
            }
            expected = expected.saturating_add(1);
        }
        while part_state
            .slots
            .first_key_value()
            .is_some_and(|(slot_cursor, _)| *slot_cursor <= cursor)
        {
            part_state.slots.pop_first();
        }
        part_state.last_emitted_cursor = Some(cursor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn doc(id: &str) -> DocumentId {
        id.parse().expect("valid doc id")
    }

    #[test]
    fn cursor_ack_advances_contiguously_only() {
        let endpoint = endpoint(7);
        let part: PartitionId = "p-main".into();
        let mut scheduler = Scheduler::default();

        scheduler.note_doc_sync_requested(endpoint, &part, 10, "doc-a");
        scheduler.note_doc_sync_requested(endpoint, &part, 12, "doc-b");
        scheduler.note_doc_sync_requested(endpoint, &part, 11, "doc-c");

        scheduler.note_doc_synced(endpoint, &part, 12, "doc-b");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(9)),
            None
        );

        scheduler.note_doc_synced(endpoint, &part, 10, "doc-a");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(9)),
            Some(10)
        );
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(9)),
            Some(10)
        );
        scheduler
            .commit_ack_cursor(endpoint, &part, Some(9), 10)
            .expect("commit should succeed");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(10)),
            None
        );

        scheduler.note_doc_synced(endpoint, &part, 11, "doc-c");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(10)),
            Some(12)
        );
        scheduler
            .commit_ack_cursor(endpoint, &part, Some(10), 12)
            .expect("commit should succeed");
    }

    #[test]
    fn late_doc_request_downgrades_ready_slot_to_pending() {
        let endpoint = endpoint(8);
        let part: PartitionId = "p-late".into();
        let mut scheduler = Scheduler::default();

        scheduler.note_cursor_ready_immediate(endpoint, &part, 20);
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(19)),
            Some(20)
        );
        scheduler.note_doc_sync_requested(endpoint, &part, 20, "doc-z");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(19)),
            None
        );
        scheduler.note_doc_synced(endpoint, &part, 20, "doc-z");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(19)),
            Some(20)
        );
    }

    #[test]
    fn doc_task_dedup_single_pending_entry() {
        let mut scheduler = Scheduler::default();
        let doc_id = doc("1111111111111111111111111111111111111111111111111111111111111111");

        scheduler.set_doc_pending_now(&doc_id);
        scheduler.set_doc_pending_now(&doc_id);
        scheduler.enqueue_doc(doc_id.clone());

        let batch = scheduler.drain_queued_docs(32);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], doc_id);
        assert!(scheduler.is_doc_pending(&batch[0]));
    }

    #[test]
    fn import_task_dedup_single_pending_entry() {
        let mut scheduler = Scheduler::default();
        let doc_id = doc("2222222222222222222222222222222222222222222222222222222222222222");

        scheduler.set_import_pending_now(&doc_id);
        scheduler.set_import_pending_now(&doc_id);
        scheduler.enqueue_import(doc_id.clone());

        let batch = scheduler.drain_queued_imports(32);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], doc_id);
        assert!(scheduler.pending_import_state(&batch[0]).is_some());
    }

    #[test]
    fn replay_convergence_floor_skips_old_ready_slots() {
        let endpoint = endpoint(3);
        let part: PartitionId = "p-replay".into();
        let mut scheduler = Scheduler::default();

        scheduler.note_cursor_ready_immediate(endpoint, &part, 3);
        scheduler.note_cursor_ready_immediate(endpoint, &part, 4);
        scheduler.note_cursor_ready_immediate(endpoint, &part, 5);

        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(4)),
            Some(5)
        );
        scheduler
            .commit_ack_cursor(endpoint, &part, Some(4), 5)
            .expect("commit should succeed");
        assert_eq!(
            scheduler.next_ready_cursor_to_ack(endpoint, &part, Some(5)),
            None
        );
    }
}
