// use crate::interlude::*;

use daybook_types::doc::DocId;
use fuser::{FileAttr, FileType};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct FileMetadata {
    pub inode: u64,
    pub path: String, // e.g., "docId.json"
    pub doc_id: DocId,
    pub attr: FileAttr,
    pub content_type: ContentType,
    pub passthrough: Option<PassthroughInfo>, // None = materialized buffer
}

#[derive(Debug, Clone)]
pub enum ContentType {
    Json, // For now, only JSON
          // Future: Blob, Markdown, etc.
}

#[derive(Debug, Clone)]
pub struct PassthroughInfo {
    // Future: blob path, file handle, etc.
    // For now, empty struct
}

pub struct MetadataTable {
    by_doc_id: HashMap<DocId, FileMetadata>,
    by_inode: HashMap<u64, DocId>,
    by_path: HashMap<String, DocId>,
}

impl MetadataTable {
    pub fn new() -> Self {
        Self {
            by_doc_id: HashMap::new(),
            by_inode: HashMap::new(),
            by_path: HashMap::new(),
        }
    }

    pub fn get_by_doc_id(&self, doc_id: &DocId) -> Option<&FileMetadata> {
        self.by_doc_id.get(doc_id)
    }

    pub fn get_by_inode(&self, inode: u64) -> Option<&FileMetadata> {
        self.by_inode
            .get(&inode)
            .and_then(|doc_id| self.by_doc_id.get(doc_id))
    }

    pub fn get_by_path(&self, path: &str) -> Option<&FileMetadata> {
        self.by_path
            .get(path)
            .and_then(|doc_id| self.by_doc_id.get(doc_id))
    }

    pub fn insert(&mut self, metadata: FileMetadata) {
        let doc_id = metadata.doc_id.clone();
        let inode = metadata.inode;
        let path = metadata.path.clone();

        // Remove old entry if it exists
        if let Some(old) = self.by_doc_id.remove(&doc_id) {
            self.by_inode.remove(&old.inode);
            self.by_path.remove(&old.path);
        }

        self.by_doc_id.insert(doc_id.clone(), metadata);
        self.by_inode.insert(inode, doc_id.clone());
        self.by_path.insert(path, doc_id);
    }

    pub fn remove_by_doc_id(&mut self, doc_id: &DocId) -> Option<FileMetadata> {
        if let Some(metadata) = self.by_doc_id.remove(doc_id) {
            self.by_inode.remove(&metadata.inode);
            self.by_path.remove(&metadata.path);
            Some(metadata)
        } else {
            None
        }
    }

    pub fn list_all(&self) -> Vec<&FileMetadata> {
        self.by_doc_id.values().collect()
    }

    pub fn clear(&mut self) {
        self.by_doc_id.clear();
        self.by_inode.clear();
        self.by_path.clear();
    }
}

impl Default for MetadataTable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetadataBuilder {
    next_inode: u64,
}

impl MetadataBuilder {
    pub fn new(start_inode: u64) -> Self {
        Self {
            next_inode: start_inode,
        }
    }

    pub fn build_metadata(
        &mut self,
        doc_id: DocId,
        json_size: usize,
        mtime: SystemTime,
    ) -> FileMetadata {
        let inode = self.next_inode;
        self.next_inode += 1;

        let path = format!("{}.json", doc_id);
        let size = json_size as u64;

        FileMetadata {
            inode,
            path,
            doc_id,
            attr: FileAttr {
                ino: inode,
                size,
                blocks: (size + 511) / 512,
                atime: mtime,
                mtime,
                ctime: mtime,
                crtime: UNIX_EPOCH,
                kind: FileType::RegularFile,
                perm: 0o644,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
                blksize: 512,
            },
            content_type: ContentType::Json,
            passthrough: None, // Always materialized for now
        }
    }
}
