use crate::interlude::*;

use super::content;
use super::metadata::{MetadataBuilder, MetadataTable};

use daybook_core::drawer::{DrawerEvent, DrawerRepo};
use daybook_core::repos::{ListenerRegistration, Repo};
use parking_lot::RwLock;
use std::time::SystemTime;
use tokio::runtime::Handle;

pub struct SyncTask {
    metadata: Arc<RwLock<MetadataTable>>,
    content: Arc<RwLock<content::ContentCache>>,
    repo: Arc<DrawerRepo>,
    rt_handle: Handle,
    builder: Arc<RwLock<MetadataBuilder>>,
    _listener_reg: ListenerRegistration,
}

impl SyncTask {
    pub fn new(
        metadata: Arc<RwLock<MetadataTable>>,
        content: Arc<RwLock<content::ContentCache>>,
        repo: Arc<DrawerRepo>,
        rt_handle: Handle,
        start_inode: u64,
    ) -> Self {
        let builder = Arc::new(RwLock::new(MetadataBuilder::new(start_inode)));

        // Register listener
        let listener_metadata = Arc::clone(&metadata);
        let listener_content = Arc::clone(&content);
        let listener_repo = Arc::clone(&repo);
        let listener_rt = rt_handle.clone();
        let listener_builder = Arc::clone(&builder);

        let listener_reg = repo.register_listener(move |event: Arc<DrawerEvent>| {
            let event = (*event).clone();
            match event {
                DrawerEvent::DocUpdated { id, .. } => {
                    // Invalidate content cache (synchronous, no await)
                    {
                        let mut cache = listener_content.write();
                        cache.remove_materialized(&id);
                    }
                    tracing::debug!(?id, "Invalidated content cache for updated doc");

                    // Update metadata (size, mtime) - need to fetch doc
                    let repo_clone = Arc::clone(&listener_repo);
                    let metadata_clone = Arc::clone(&listener_metadata);
                    let builder_clone = Arc::clone(&listener_builder);
                    let id_clone = id.clone();
                    listener_rt.spawn(async move {
                        if let Ok(Some(doc)) = repo_clone.get(&id_clone).await {
                            if let Ok(json) = serde_json::to_string_pretty(&doc) {
                                let json_size = json.len();
                                let mtime = SystemTime::UNIX_EPOCH
                                    + std::time::Duration::from_secs(
                                        doc.updated_at.unix_timestamp() as u64,
                                    );
                                let new_metadata = {
                                    let mut builder = builder_clone.write();
                                    builder.build_metadata(id_clone.clone(), json_size, mtime)
                                };
                                metadata_clone.write().insert(new_metadata);
                            }
                        }
                    });
                }
                DrawerEvent::DocDeleted { id, .. } => {
                    // Remove from content cache (synchronous, no await)
                    {
                        let mut cache = listener_content.write();
                        cache.remove_materialized(&id);
                    }

                    // Remove from metadata
                    listener_metadata.write().remove_by_doc_id(&id);
                    tracing::debug!(?id, "Removed metadata for deleted doc");
                }
                DrawerEvent::ListChanged => {
                    // Refresh entire metadata table
                    let repo_clone = Arc::clone(&listener_repo);
                    let metadata_clone = Arc::clone(&listener_metadata);
                    let builder_clone = Arc::clone(&listener_builder);
                    listener_rt.spawn(async move {
                        let doc_ids = repo_clone.list().await;

                        // Collect all metadata entries first (without holding lock across await)
                        let mut entries = Vec::new();
                        for doc_id in doc_ids {
                            // Fetch doc to get size and mtime
                            if let Ok(Some(doc)) = repo_clone.get(&doc_id).await {
                                if let Ok(json) = serde_json::to_string_pretty(&doc) {
                                    let json_size = json.len();
                                    let mtime = SystemTime::UNIX_EPOCH
                                        + std::time::Duration::from_secs(
                                            doc.updated_at.unix_timestamp() as u64,
                                        );
                                    entries.push((doc_id, json_size, mtime));
                                }
                            }
                        }

                        // Now update metadata table (holding lock only briefly)
                        {
                            let mut metadata = metadata_clone.write();
                            metadata.clear();

                            let mut builder = builder_clone.write();
                            for (doc_id, json_size, mtime) in entries {
                                let file_metadata =
                                    builder.build_metadata(doc_id, json_size, mtime);
                                metadata.insert(file_metadata);
                            }
                            tracing::debug!(
                                count = metadata.list_all().len(),
                                "Refreshed metadata table"
                            );
                        }
                    });
                }
                DrawerEvent::DocAdded { .. } => {
                    // Handled by ListChanged
                }
            }
        });

        Self {
            metadata,
            content,
            repo,
            rt_handle,
            builder,
            _listener_reg: listener_reg,
        }
    }

    /// Initialize metadata table with current docs
    pub async fn init(&self) -> Res<()> {
        let doc_ids = self.repo.list().await;
        let mut metadata = self.metadata.write();
        metadata.clear();

        let mut builder = self.builder.write();
        for doc_id in doc_ids {
            // Fetch doc to get size and mtime
            if let Some(doc) = self.repo.get(&doc_id).await? {
                let json = serde_json::to_string_pretty(&doc)?;
                let json_size = json.len();
                let mtime = SystemTime::UNIX_EPOCH
                    + std::time::Duration::from_secs(doc.updated_at.unix_timestamp() as u64);
                let file_metadata = builder.build_metadata(doc_id, json_size, mtime);
                metadata.insert(file_metadata);
            }
        }

        tracing::info!(
            count = metadata.list_all().len(),
            "Initialized metadata table"
        );
        Ok(())
    }
}
