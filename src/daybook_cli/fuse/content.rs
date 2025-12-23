use crate::interlude::*;

use daybook_types::doc::DocId;
use parking_lot::RwLock;
use std::collections::HashMap;

pub struct ContentCache {
    // Materialized buffers for read operations (shared across FDs)
    materialized: HashMap<DocId, Arc<Vec<u8>>>,
    // Snapshot buffers for write operations (one per file handle)
    write_snapshots: HashMap<u64, Arc<Vec<u8>>>,
}

impl ContentCache {
    pub fn new() -> Self {
        Self {
            materialized: HashMap::new(),
            write_snapshots: HashMap::new(),
        }
    }

    pub fn get_materialized(&self, doc_id: &DocId) -> Option<Arc<Vec<u8>>> {
        self.materialized.get(doc_id).map(Arc::clone)
    }

    pub fn insert_materialized(&mut self, doc_id: DocId, content: Arc<Vec<u8>>) {
        self.materialized.insert(doc_id, content);
    }

    pub fn remove_materialized(&mut self, doc_id: &DocId) {
        self.materialized.remove(doc_id);
    }

    pub fn get_snapshot(&self, fh: u64) -> Option<Arc<Vec<u8>>> {
        self.write_snapshots.get(&fh).map(Arc::clone)
    }

    pub fn create_snapshot(&mut self, fh: u64, content: Arc<Vec<u8>>) {
        self.write_snapshots.insert(fh, content);
    }

    pub fn remove_snapshot(&mut self, fh: u64) -> Option<Arc<Vec<u8>>> {
        self.write_snapshots.remove(&fh)
    }

    pub fn clear_materialized(&mut self) {
        self.materialized.clear();
    }
}

impl Default for ContentCache {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ContentManager {
    cache: Arc<RwLock<ContentCache>>,
    repo: Arc<daybook_core::drawer::DrawerRepo>,
}

impl ContentManager {
    pub fn new(
        cache: Arc<RwLock<ContentCache>>,
        repo: Arc<daybook_core::drawer::DrawerRepo>,
    ) -> Self {
        Self { cache, repo }
    }

    /// Materialize JSON content for a document
    pub async fn materialize_json(&self, doc_id: &DocId) -> Res<Arc<Vec<u8>>> {
        // Check cache first
        if let Some(content) = self.cache.read().get_materialized(doc_id) {
            return Ok(content);
        }

        // Fetch from repo
        let doc = self.repo.get(doc_id).await?;
        let doc = doc.ok_or_eyre("Document not found")?;

        // Serialize to pretty JSON
        let json = serde_json::to_string_pretty(&doc)?;
        let content = Arc::new(json.into_bytes());

        // Cache it
        self.cache
            .write()
            .insert_materialized(doc_id.clone(), Arc::clone(&content));

        Ok(content)
    }

    /// Create a write snapshot from current materialized content
    pub async fn create_snapshot(&self, fh: u64, doc_id: &DocId) -> Res<Arc<Vec<u8>>> {
        // Get current materialized content (or materialize if needed)
        let content = self.materialize_json(doc_id).await?;

        // Create a new Arc with cloned data for the snapshot
        let snapshot = Arc::new((*content).clone());

        self.cache
            .write()
            .create_snapshot(fh, Arc::clone(&snapshot));

        Ok(snapshot)
    }

    /// Update snapshot buffer with new data (for write operations)
    pub fn update_snapshot(&self, fh: u64, offset: i64, data: &[u8]) -> Res<()> {
        let mut cache = self.cache.write();

        let snapshot = cache
            .get_snapshot(fh)
            .ok_or_eyre("Snapshot not found for file handle")?;

        let mut new_data = (*snapshot).clone();

        if offset == 0 {
            // New write, replace content
            new_data.clear();
        }

        let offset = offset as usize;
        if offset > new_data.len() {
            // Pad with zeros if needed
            new_data.resize(offset, 0);
        }

        if offset >= new_data.len() {
            // Append beyond current size
            new_data.extend_from_slice(data);
        } else {
            // Overwrite/append
            let end = std::cmp::min(offset + data.len(), new_data.len());
            new_data[offset..end].copy_from_slice(&data[..end - offset]);
            if data.len() > end - offset {
                new_data.extend_from_slice(&data[end - offset..]);
            }
        }

        cache.create_snapshot(fh, Arc::new(new_data));

        Ok(())
    }

    /// Get snapshot content (for write release)
    pub fn get_snapshot_content(&self, fh: u64) -> Option<Arc<Vec<u8>>> {
        self.cache.read().get_snapshot(fh)
    }

    /// Remove snapshot (after write is complete)
    pub fn remove_snapshot(&self, fh: u64) {
        self.cache.write().remove_snapshot(fh);
    }

    /// Invalidate materialized content for a doc (on update)
    pub fn invalidate(&self, doc_id: &DocId) {
        self.cache.write().remove_materialized(doc_id);
    }
}
