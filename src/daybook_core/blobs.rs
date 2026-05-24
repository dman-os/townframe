use crate::interlude::*;

use big_repo::SharedPartStore;
use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::store::fs::FsStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Component, Path};
use std::str::FromStr;
use tokio::io::AsyncWriteExt;

pub mod sync;

#[async_trait]
pub trait PartitionMembershipWriter: Send + Sync {
    async fn upsert_item(
        &self,
        partition_id: Arc<str>,
        member_id: BlobId,
        payload: &serde_json::Value,
    ) -> Res<()>;
    async fn remove_item(&self, partition_id: Arc<str>, member_id: BlobId) -> Res<()>;
}

#[derive(Clone)]
pub struct PartitionStoreMembershipWriter {
    partition_store: SharedPartStore,
}

impl PartitionStoreMembershipWriter {
    pub fn new(partition_store: SharedPartStore) -> Self {
        Self { partition_store }
    }
}

#[async_trait]
impl PartitionMembershipWriter for PartitionStoreMembershipWriter {
    async fn upsert_item(
        &self,
        partition_id: Arc<str>,
        member_id: BlobId,
        payload: &serde_json::Value,
    ) -> Res<()> {
        let part_id = crate::part_id_from_label(&partition_id);
        self.partition_store
            .set_obj_payload(member_id, payload.clone(), vec![part_id], None)
            .await?;
        Ok(())
    }

    async fn remove_item(&self, partition_id: Arc<str>, member_id: BlobId) -> Res<()> {
        let part_id = crate::part_id_from_label(&partition_id);
        self.partition_store
            .remove_obj_from_part(member_id, part_id, None)
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct NoopPartitionMembershipWriter;

#[async_trait]
impl PartitionMembershipWriter for NoopPartitionMembershipWriter {
    async fn upsert_item(
        &self,
        _partition_id: Arc<str>,
        _member_id: BlobId,
        _payload: &serde_json::Value,
    ) -> Res<()> {
        Ok(())
    }
    async fn remove_item(&self, _partition_id: Arc<str>, _member_id: BlobId) -> Res<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct BlobsRepo {
    root: PathBuf,
    src_local_user_path: UserPathBuf,
    iroh_store: iroh_blobs::api::Store,
    hash_locks: Arc<std::sync::Mutex<HashMap<BlobId, Arc<tokio::sync::Mutex<()>>>>>,
    partition_writer: Arc<dyn PartitionMembershipWriter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BlobMode {
    OwnedCopy,
    Reference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlobMetaV1 {
    version: u32,
    hash: BlobId,
    mode: BlobMode,
    size_bytes: u64,
    mime: Option<String>,
    src_local_user_path: UserPathBuf,
    source_paths: Vec<String>,
    created_at_unix_secs: i64,
    iroh_ingested: bool,
}

struct ObjectPaths {
    dir: PathBuf,
    blob: PathBuf,
    meta: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobMaterializeRequest {
    Filename(String),
    Extension(String),
}

pub const BLOB_SCHEME: &str = "db+blob";
pub const BLOB_SCOPE_DOCS_PARTITION_ID: &str = "blob_scope/docs";
pub const BLOB_SCOPE_PLUGS_PARTITION_ID: &str = "blob_scope/plugs";

pub type BlobId = ObjId;

pub(crate) fn blob_id_from_hash(hash: &str) -> BlobId {
    BlobId::from_str(hash).expect("invalid blob hash")
}

pub(crate) fn blob_hash_from_id(blob_id: BlobId) -> String {
    blob_id.to_string()
}

fn blob_id_from_bytes(bytes: [u8; 32]) -> BlobId {
    BlobId::new(bytes)
}

async fn blob_id_from_reader(reader: tokio::fs::File) -> Result<BlobId, eyre::Report> {
    use tokio::io::AsyncReadExt;

    let mut hasher = blake3::Hasher::new();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buf = vec![0u8; 65536];
    loop {
        let read = reader.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(blob_id_from_bytes(*hasher.finalize().as_bytes()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum BlobScope {
    Docs,
    Plugs,
}

impl BlobScope {
    pub fn partition_id(self) -> &'static str {
        match self {
            Self::Docs => BLOB_SCOPE_DOCS_PARTITION_ID,
            Self::Plugs => BLOB_SCOPE_PLUGS_PARTITION_ID,
        }
    }

    pub fn from_partition_id(partition_id: &str) -> Option<Self> {
        match partition_id {
            BLOB_SCOPE_DOCS_PARTITION_ID => Some(Self::Docs),
            BLOB_SCOPE_PLUGS_PARTITION_ID => Some(Self::Plugs),
            _ => None,
        }
    }
}

impl BlobsRepo {
    pub async fn new(
        root: PathBuf,
        src_local_user_path: UserPathBuf,
        partition_writer: Arc<dyn PartitionMembershipWriter>,
    ) -> Result<Arc<Self>, eyre::Report> {
        let objects_root = root.join("objects");
        tokio::fs::create_dir_all(&objects_root).await?;
        let iroh_root = root.join("iroh");
        tokio::fs::create_dir_all(&iroh_root).await?;
        let fs_store = FsStore::load(&iroh_root)
            .await
            .map_err(|err| eyre::eyre!("error loading iroh fs store: {err:?}"))?;

        Ok(Arc::new(Self {
            root,
            src_local_user_path,
            iroh_store: fs_store.into(),
            hash_locks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            partition_writer,
        }))
    }

    pub async fn add_hash_to_scope(&self, scope: BlobScope, blob_id: BlobId) -> Res<()> {
        let payload = serde_json::json!({});
        self.partition_writer
            .upsert_item(scope.partition_id().into(), blob_id, &payload)
            .await
    }

    pub async fn remove_hash_from_scope(&self, scope: BlobScope, blob_id: BlobId) -> Res<()> {
        self.partition_writer
            .remove_item(scope.partition_id().into(), blob_id)
            .await
    }

    pub async fn put_path_copy(&self, source_path: &Path) -> Res<BlobId> {
        let source_path = source_path.canonicalize()?;
        let source_meta = tokio::fs::metadata(&source_path).await?;
        if !source_meta.is_file() {
            eyre::bail!("source path is not a file: {}", source_path.display());
        }

        let source_snapshot = self.create_source_snapshot(&source_path).await?;
        let result = async {
            let hash = blob_id_from_reader(tokio::fs::File::open(&source_snapshot).await?).await?;
            let object_paths = self.object_paths(hash)?;

            tokio::fs::create_dir_all(&object_paths.dir).await?;
            if !tokio::fs::try_exists(&object_paths.blob).await? {
                self.atomic_copy_file(&source_snapshot, &object_paths.blob)
                    .await?;
            }

            let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
            let mut meta = self.build_meta(
                hash,
                BlobMode::OwnedCopy,
                blob_meta.len(),
                Vec::new(),
                false,
            );
            self.write_meta(&object_paths.meta, &meta).await?;
            self.ingest_path_with_iroh(&object_paths.blob, hash).await?;
            meta.iroh_ingested = true;
            self.write_meta(&object_paths.meta, &meta).await?;

            Ok(hash)
        }
        .await;
        let _ = tokio::fs::remove_file(&source_snapshot).await;
        result
    }

    pub async fn put_path_reference(&self, source_path: &Path) -> Res<BlobId> {
        if !source_path.is_absolute() {
            eyre::bail!("reference path must be absolute: {}", source_path.display());
        }

        let source_meta = tokio::fs::metadata(source_path).await?;
        if !source_meta.is_file() {
            eyre::bail!("source path is not a file: {}", source_path.display());
        }

        let source_snapshot = self.create_source_snapshot(source_path).await?;
        let result = async {
            let snapshot_meta = tokio::fs::metadata(&source_snapshot).await?;
            let hash = blob_id_from_reader(tokio::fs::File::open(&source_snapshot).await?).await?;
            let object_paths = self.object_paths(hash)?;
            tokio::fs::create_dir_all(&object_paths.dir).await?;
            let hash_lock = self.lock_for_hash(hash);
            let _hash_guard = hash_lock.lock().await;

            let source_path_string = source_path
                .to_str()
                .ok_or_else(|| eyre::eyre!("reference path must be valid UTF-8"))?
                .to_string();
            let mut meta = self.build_meta(
                hash,
                BlobMode::Reference,
                snapshot_meta.len(),
                vec![source_path_string.clone()],
                false,
            );
            if let Some(existing) = self.read_meta(&object_paths.meta).await? {
                let mut merged = existing.source_paths;
                if !merged.iter().any(|value| value == &source_path_string) {
                    merged.push(source_path_string);
                }
                meta.source_paths = merged;
            }

            self.write_meta(&object_paths.meta, &meta).await?;
            self.ingest_path_with_iroh(&source_snapshot, hash).await?;
            meta.iroh_ingested = true;
            self.write_meta(&object_paths.meta, &meta).await?;
            Ok(hash)
        }
        .await;
        let _ = tokio::fs::remove_file(&source_snapshot).await;
        result
    }

    /// Compatibility alias that ingests bytes as an owned blob.
    pub async fn put(&self, data: &[u8]) -> Result<BlobId, eyre::Report> {
        let hash = blob_id_from_bytes(*blake3::hash(data).as_bytes());
        let object_paths = self.object_paths(hash)?;

        tokio::fs::create_dir_all(&object_paths.dir).await?;
        if !tokio::fs::try_exists(&object_paths.blob).await? {
            self.atomic_write(&object_paths.blob, data).await?;
        }

        let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
        let mut meta = self.build_meta(
            hash,
            BlobMode::OwnedCopy,
            blob_meta.len(),
            Vec::new(),
            false,
        );
        self.write_meta(&object_paths.meta, &meta).await?;
        self.ingest_path_with_iroh(&object_paths.blob, hash).await?;
        meta.iroh_ingested = true;
        self.write_meta(&object_paths.meta, &meta).await?;

        Ok(hash)
    }

    pub async fn get_path(&self, blob_id: BlobId) -> Result<PathBuf, eyre::Report> {
        let hash = blob_hash_from_id(blob_id);
        let object_paths = self.object_paths(blob_id)?;
        if tokio::fs::try_exists(&object_paths.blob).await? {
            if self.read_meta(&object_paths.meta).await?.is_none() {
                let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
                let recovered = self.build_meta(
                    blob_id,
                    BlobMode::OwnedCopy,
                    blob_meta.len(),
                    Vec::new(),
                    false,
                );
                self.write_meta(&object_paths.meta, &recovered).await?;
            }
            return Ok(object_paths.blob);
        }

        let Some(meta) = self.read_meta(&object_paths.meta).await? else {
            eyre::bail!("Blob not found: {hash}");
        };

        match meta.mode {
            BlobMode::OwnedCopy => eyre::bail!("Blob not found: {hash}"),
            BlobMode::Reference => {
                if meta.source_paths.is_empty() {
                    eyre::bail!("reference metadata missing source_paths");
                }
                let mut drift_error: Option<String> = None;
                for source_path in &meta.source_paths {
                    let source_path = PathBuf::from(source_path);
                    if !tokio::fs::try_exists(&source_path).await? {
                        continue;
                    }
                    let source_hash =
                        blob_id_from_reader(tokio::fs::File::open(&source_path).await?).await?;
                    if source_hash == meta.hash {
                        return Ok(source_path);
                    } else if tokio::fs::try_exists(&object_paths.blob).await? {
                        return Ok(object_paths.blob);
                    } else if drift_error.is_none() {
                        drift_error = Some(format!(
                            "Referenced blob hash diverged for {}: expected={}, got={}",
                            source_path.display(),
                            blob_hash_from_id(meta.hash),
                            blob_hash_from_id(source_hash)
                        ));
                    }
                }
                if let Some(err) = drift_error {
                    eyre::bail!(err);
                }
                eyre::bail!("Referenced blob source missing for hash {hash}");
            }
        }
    }

    pub fn iroh_store(&self) -> iroh_blobs::api::Store {
        self.iroh_store.clone()
    }

    pub async fn shutdown(&self) -> Res<()> {
        self.iroh_store
            .shutdown()
            .await
            .map_err(|err| eyre::eyre!("error shutting down iroh blob store: {err:?}"))?;
        Ok(())
    }

    pub async fn has_hash(&self, blob_id: BlobId) -> Res<bool> {
        Ok(self.get_path(blob_id).await.is_ok())
    }

    pub async fn cleanup_staging(&self) -> Res<()> {
        let staging_root = self.root.join("staging");
        if tokio::fs::try_exists(&staging_root).await? {
            tokio::fs::remove_dir_all(&staging_root).await?;
        }
        tokio::fs::create_dir_all(&staging_root).await?;
        Ok(())
    }

    pub async fn materialize(
        &self,
        blob_id: BlobId,
        request: BlobMaterializeRequest,
    ) -> Res<PathBuf> {
        let hash = blob_hash_from_id(blob_id);
        self.ensure_local_object_no_meta_rewrite(blob_id).await?;
        let source_path = self.object_paths(blob_id)?.blob;
        let filename = match request {
            BlobMaterializeRequest::Filename(name) => Self::sanitize_requested_filename(&name)?,
            BlobMaterializeRequest::Extension(ext) => {
                let ext = Self::sanitize_requested_extension(&ext)?;
                format!("{hash}.{ext}")
            }
        };
        let staging_dir = self.root.join("staging").join(&hash);
        tokio::fs::create_dir_all(&staging_dir).await?;
        let materialized_path = staging_dir.join(filename);
        if tokio::fs::try_exists(&materialized_path).await? {
            return Ok(materialized_path);
        }
        match tokio::fs::hard_link(&source_path, &materialized_path).await {
            Ok(_) => Ok(materialized_path),
            Err(_) => {
                self.atomic_copy_file(&source_path, &materialized_path)
                    .await?;
                Ok(materialized_path)
            }
        }
    }

    pub async fn materialize_id(
        &self,
        blob_id: BlobId,
        request: BlobMaterializeRequest,
    ) -> Res<PathBuf> {
        self.materialize(blob_id, request).await
    }

    pub async fn materialize_with_meta_extension(
        &self,
        blob_id: BlobId,
        filename_stem: &str,
    ) -> Res<PathBuf> {
        let hash = blob_hash_from_id(blob_id);
        let object_paths = self.object_paths(blob_id)?;
        let meta = self
            .read_meta(&object_paths.meta)
            .await?
            .ok_or_else(|| eyre::eyre!("blob metadata not found for hash {hash}"))?;
        eyre::ensure!(
            meta.mime.as_deref().is_some() || !meta.source_paths.is_empty(),
            "materialize_with_meta_extension requires blob metadata with mime or source_paths for hash {hash}"
        );
        let ext = self.preferred_extension_from_meta(blob_id).await?;
        let stem = Self::sanitize_requested_stem(filename_stem)?;
        self.materialize(
            blob_id,
            BlobMaterializeRequest::Filename(format!("{stem}.{ext}")),
        )
        .await
    }

    pub async fn put_from_store(&self, blob_id: BlobId) -> Res<BlobId> {
        let object_paths = self.object_paths(blob_id)?;
        tokio::fs::create_dir_all(&object_paths.dir).await?;

        if !tokio::fs::try_exists(&object_paths.blob).await? {
            let iroh_hash = blob_id_to_iroh_hash(blob_id);
            self.iroh_store
                .blobs()
                .export(iroh_hash, &object_paths.blob)
                .await
                .map_err(|err| eyre::eyre!("error exporting blob from iroh store: {err:?}"))?;
        }

        let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
        let meta = self.build_meta(
            blob_id,
            BlobMode::OwnedCopy,
            blob_meta.len(),
            Vec::new(),
            true,
        );
        self.write_meta(&object_paths.meta, &meta).await?;

        Ok(blob_id)
    }

    async fn ensure_local_object_no_meta_rewrite(&self, blob_id: BlobId) -> Res<()> {
        let object_paths = self.object_paths(blob_id)?;
        tokio::fs::create_dir_all(&object_paths.dir).await?;
        if !tokio::fs::try_exists(&object_paths.blob).await? {
            let iroh_hash = blob_id_to_iroh_hash(blob_id);
            self.iroh_store
                .blobs()
                .export(iroh_hash, &object_paths.blob)
                .await
                .map_err(|err| eyre::eyre!("error exporting blob from iroh store: {err:?}"))?;
        }
        Ok(())
    }

    fn object_paths(&self, blob_id: BlobId) -> Res<ObjectPaths> {
        let hash = blob_hash_from_id(blob_id);
        if hash.len() < 4 {
            eyre::bail!("invalid blob hash: {hash}");
        }
        let Some(l0) = hash.get(0..2) else {
            eyre::bail!("invalid blob hash: {hash}");
        };
        let Some(l1) = hash.get(2..4) else {
            eyre::bail!("invalid blob hash: {hash}");
        };
        let dir = self.root.join("objects").join(l0).join(l1);
        Ok(ObjectPaths {
            blob: dir.join(format!("{hash}.blob")),
            meta: dir.join(format!("{hash}.meta")),
            dir,
        })
    }

    async fn preferred_extension_from_meta(&self, blob_id: BlobId) -> Res<String> {
        let hash = blob_hash_from_id(blob_id);
        let object_paths = self.object_paths(blob_id)?;
        if let Some(meta) = self.read_meta(&object_paths.meta).await? {
            if let Some(mime) = meta.mime.as_deref() {
                if let Some(ext) = Self::extension_from_mime(mime) {
                    return Ok(ext.to_string());
                }
            }
            if let Some(source_ext) = meta
                .source_paths
                .iter()
                .filter_map(|path| Path::new(path).extension())
                .filter_map(|ext| ext.to_str())
                .find(|ext| !ext.is_empty())
            {
                return Self::sanitize_requested_extension(source_ext);
            }
        }
        eyre::bail!("blob metadata has no materializable extension for hash {hash}");
    }

    fn extension_from_mime(mime: &str) -> Option<&'static str> {
        match mime.split(';').next().map(str::trim) {
            Some("image/jpeg" | "image/jpg") => Some("jpg"),
            Some("image/png") => Some("png"),
            Some("image/gif") => Some("gif"),
            Some("image/bmp") => Some("bmp"),
            Some("image/webp") => Some("webp"),
            Some("text/plain") => Some("txt"),
            Some("application/json") => Some("json"),
            Some("application/yaml" | "text/yaml" | "text/x-yaml") => Some("yaml"),
            _ => None,
        }
    }

    fn sanitize_requested_stem(stem: &str) -> Res<String> {
        let stem = stem.trim();
        eyre::ensure!(!stem.is_empty(), "filename stem must not be empty");
        let mut components = Path::new(stem).components();
        let Some(first) = components.next() else {
            eyre::bail!("filename stem must not be empty");
        };
        eyre::ensure!(
            matches!(first, Component::Normal(_)) && components.next().is_none(),
            "filename stem must be a single normal path component"
        );
        eyre::ensure!(
            !stem.contains(':'),
            "filename stem must not contain path prefixes"
        );
        Ok(stem.to_string())
    }

    fn sanitize_requested_extension(extension: &str) -> Res<String> {
        let ext = extension.trim().trim_start_matches('.');
        eyre::ensure!(!ext.is_empty(), "extension must not be empty");
        eyre::ensure!(
            !ext.contains('/') && !ext.contains('\\'),
            "extension must not contain path separators"
        );
        eyre::ensure!(
            ext.bytes()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'-'),
            "extension contains invalid characters: '{ext}'"
        );
        Ok(ext.to_ascii_lowercase())
    }

    fn sanitize_requested_filename(filename: &str) -> Res<String> {
        let filename = filename.trim();
        eyre::ensure!(!filename.is_empty(), "filename must not be empty");
        let mut components = Path::new(filename).components();
        let Some(first) = components.next() else {
            eyre::bail!("filename must not be empty");
        };
        eyre::ensure!(
            matches!(first, Component::Normal(_)) && components.next().is_none(),
            "filename must be a single normal path component"
        );
        eyre::ensure!(
            !filename.contains(':'),
            "filename must not contain path prefixes"
        );
        Ok(filename.to_string())
    }

    async fn read_meta(&self, path: &Path) -> Res<Option<BlobMetaV1>> {
        if !tokio::fs::try_exists(path).await? {
            return Ok(None);
        }
        let raw = tokio::fs::read(path).await?;
        let meta = serde_json::from_slice::<BlobMetaV1>(&raw)
            .wrap_err_with(|| format!("invalid blob metadata json at {}", path.display()))?;
        Ok(Some(meta))
    }

    async fn write_meta(&self, path: &Path, meta: &BlobMetaV1) -> Res<()> {
        let data = serde_json::to_vec(meta)?;
        self.atomic_write(path, &data).await
    }

    fn build_meta(
        &self,
        hash: BlobId,
        mode: BlobMode,
        size_bytes: u64,
        source_paths: Vec<String>,
        iroh_ingested: bool,
    ) -> BlobMetaV1 {
        BlobMetaV1 {
            version: 1,
            hash,
            mode,
            size_bytes,
            mime: None,
            src_local_user_path: self.src_local_user_path.clone(),
            source_paths,
            created_at_unix_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_secs() as i64,
            iroh_ingested,
        }
    }

    async fn ingest_path_with_iroh(&self, path: &Path, blob_id: BlobId) -> Res<()> {
        self.iroh_store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path: path.to_path_buf(),
                format: iroh_blobs::BlobFormat::Raw,
                mode: ImportMode::TryReference,
            })
            .with_named_tag(blob_hash_from_id(blob_id).as_bytes())
            .await
            .map_err(|err| eyre::eyre!("error ingesting path into iroh store: {err:?}"))?;
        Ok(())
    }

    fn lock_for_hash(&self, blob_id: BlobId) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.hash_locks.lock().expect(ERROR_MUTEX);
        Arc::clone(
            guard
                .entry(blob_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    fn is_exists_error(err: &std::io::Error) -> bool {
        err.kind() == std::io::ErrorKind::AlreadyExists
            || matches!(err.raw_os_error(), Some(17 | 183))
    }

    async fn atomic_copy_file(&self, source: &Path, dest: &Path) -> Res<()> {
        let dir = dest
            .parent()
            .ok_or_eyre("destination path for copy has no parent directory")?;
        tokio::fs::create_dir_all(dir).await?;

        let temp = dir.join(format!(
            ".{}.{}.tmp",
            dest.file_name()
                .expect("destination path should have filename")
                .to_string_lossy(),
            rand::random::<u64>()
        ));

        tokio::fs::copy(source, &temp).await?;

        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        match tokio::fs::rename(&temp, dest).await {
            Ok(_) => {}
            Err(err) if Self::is_exists_error(&err) => {
                let _ = tokio::fs::remove_file(&temp).await;
            }
            Err(err) => return Err(err.into()),
        }
        self.sync_dir(dir).await?;
        Ok(())
    }

    async fn create_source_snapshot(&self, source: &Path) -> Res<PathBuf> {
        let snapshot_dir = self.root.join("snapshots");
        tokio::fs::create_dir_all(&snapshot_dir).await?;
        let snapshot_path = snapshot_dir.join(format!(
            "blob-src-{}-{}.snapshot",
            std::process::id(),
            rand::random::<u64>()
        ));
        self.atomic_copy_file(source, &snapshot_path).await?;
        Ok(snapshot_path)
    }

    async fn atomic_write(&self, path: &Path, data: &[u8]) -> Res<()> {
        let dir = path
            .parent()
            .ok_or_eyre("target path for atomic write has no parent directory")?;
        tokio::fs::create_dir_all(dir).await?;

        let temp = dir.join(format!(
            ".{}.{}.tmp",
            path.file_name()
                .expect("target path should have filename")
                .to_string_lossy(),
            rand::random::<u64>()
        ));

        let mut file = tokio::fs::File::create(&temp).await?;
        file.write_all(data).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        match tokio::fs::rename(&temp, path).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                match tokio::fs::remove_file(path).await {
                    Ok(_) => {}
                    Err(remove_err) if remove_err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(remove_err) => return Err(remove_err.into()),
                }
                tokio::fs::rename(&temp, path).await?;
            }
            Err(err) => return Err(err.into()),
        }
        self.sync_dir(dir).await?;
        Ok(())
    }

    async fn sync_dir(&self, dir: &Path) -> Res<()> {
        let dir_file = tokio::fs::OpenOptions::new().read(true).open(dir).await?;
        dir_file.sync_all().await?;
        Ok(())
    }
}

pub(crate) fn daybook_hash_to_iroh_hash(hash: &str) -> Res<iroh_blobs::Hash> {
    let blob_id = BlobId::from_str(hash).wrap_err("invalid daybook blob hash")?;
    Ok(blob_id_to_iroh_hash(blob_id))
}

pub(crate) fn blob_id_to_iroh_hash(blob_id: BlobId) -> iroh_blobs::Hash {
    iroh_blobs::Hash::from_bytes(*blob_id.as_bytes())
}

pub(crate) fn blob_id_to_digest_str(blob_id: BlobId) -> String {
    utils_rs::hash::encode_base58_multibase_blake3(*blob_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (Arc<BlobsRepo>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo = BlobsRepo::new(
            temp_dir.path().to_path_buf(),
            "/local/test-user".into(),
            Arc::new(NoopPartitionMembershipWriter),
        )
        .await
        .unwrap();
        (repo, temp_dir)
    }

    fn bytes_hash_to_iroh_hash(bytes: &[u8]) -> iroh_blobs::Hash {
        iroh_blobs::Hash::new(bytes)
    }

    #[test]
    fn blob_scope_partition_mapping_is_stable() {
        assert_eq!(BlobScope::Docs.partition_id(), BLOB_SCOPE_DOCS_PARTITION_ID);
        assert_eq!(
            BlobScope::Plugs.partition_id(),
            BLOB_SCOPE_PLUGS_PARTITION_ID
        );
        assert_eq!(
            BlobScope::from_partition_id(BLOB_SCOPE_DOCS_PARTITION_ID),
            Some(BlobScope::Docs)
        );
        assert_eq!(
            BlobScope::from_partition_id(BLOB_SCOPE_PLUGS_PARTITION_ID),
            Some(BlobScope::Plugs)
        );
        assert_eq!(BlobScope::from_partition_id("blob_scope/unknown"), None);
    }

    #[tokio::test]
    async fn put_bytes_smoke_owned_copy() -> Res<()> {
        let (repo, _temp) = setup().await;
        let data = b"hello world";

        let hash = repo.put(data).await?;
        let expected_hash = blob_id_from_bytes(*blake3::hash(data).as_bytes());
        assert_eq!(hash, expected_hash);

        let path = repo.get_path(hash).await?;
        let saved_data = tokio::fs::read(path).await?;
        assert_eq!(saved_data, data);

        let object_paths = repo.object_paths(hash)?;
        let meta: BlobMetaV1 = serde_json::from_slice(&tokio::fs::read(&object_paths.meta).await?)?;
        assert_eq!(meta.mode, BlobMode::OwnedCopy);
        assert_eq!(meta.src_local_user_path, "/local/test-user");

        Ok(())
    }

    #[tokio::test]
    async fn put_bytes_dedup() -> Res<()> {
        let (repo, _temp) = setup().await;
        let data = b"duplicate data";

        let hash1 = repo.put(data).await?;
        let hash2 = repo.put(data).await?;

        assert_eq!(hash1, hash2);

        let object_paths = repo.object_paths(hash1)?;
        assert!(tokio::fs::try_exists(&object_paths.blob).await?);

        Ok(())
    }

    #[tokio::test]
    async fn put_path_copy_survives_source_delete() -> Res<()> {
        let (repo, temp) = setup().await;
        let source = temp.path().join("source.bin");
        tokio::fs::write(&source, b"copy me").await?;

        let hash = repo.put_path_copy(&source).await?;
        tokio::fs::remove_file(&source).await?;

        let path = repo.get_path(hash).await?;
        let saved = tokio::fs::read(path).await?;
        assert_eq!(saved, b"copy me");
        Ok(())
    }

    #[tokio::test]
    async fn put_path_reference_breaks_if_source_deleted() -> Res<()> {
        let (repo, temp) = setup().await;
        let source = temp.path().join("source-ref.bin");
        tokio::fs::write(&source, b"ref me").await?;

        let source_abs = source.canonicalize()?;
        let hash = repo.put_path_reference(&source_abs).await?;
        tokio::fs::remove_file(&source_abs).await?;

        let err = repo.get_path(hash).await.unwrap_err();
        assert!(
            err.to_string().contains("Referenced blob source missing"),
            "unexpected error: {err:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_path_prefers_owned_over_reference() -> Res<()> {
        let (repo, temp) = setup().await;
        let source = temp.path().join("owned.bin");
        tokio::fs::write(&source, b"owned wins").await?;

        let hash = repo.put_path_copy(&source).await?;
        let object_paths = repo.object_paths(hash)?;

        let bogus_ref = BlobMetaV1 {
            version: 1,
            hash,
            mode: BlobMode::Reference,
            size_bytes: 123,
            mime: None,
            src_local_user_path: "/local/test-user".into(),
            source_paths: vec!["/tmp/does/not/exist".to_string()],
            created_at_unix_secs: 1,
            iroh_ingested: true,
        };
        repo.write_meta(&object_paths.meta, &bogus_ref).await?;

        let got = repo.get_path(hash).await?;
        assert_eq!(got, object_paths.blob);
        Ok(())
    }

    #[tokio::test]
    async fn metadata_roundtrip() -> Res<()> {
        let (repo, _temp) = setup().await;
        let data = b"roundtrip";

        let hash = repo.put(data).await?;
        let object_paths = repo.object_paths(hash)?;
        let meta: BlobMetaV1 = serde_json::from_slice(&tokio::fs::read(&object_paths.meta).await?)?;

        assert_eq!(meta.hash, hash);
        assert_eq!(meta.mode, BlobMode::OwnedCopy);
        assert_eq!(meta.size_bytes, data.len() as u64);
        assert_eq!(meta.src_local_user_path, "/local/test-user");
        assert!(meta.iroh_ingested);

        Ok(())
    }

    #[tokio::test]
    async fn iroh_ingest_presence_smoke() -> Res<()> {
        let (repo, temp) = setup().await;

        let hash_a = repo.put(b"bytes-path").await?;
        let has_a = repo
            .iroh_store
            .blobs()
            .has(bytes_hash_to_iroh_hash(b"bytes-path"))
            .await
            .map_err(|err| eyre::eyre!("iroh has check failed: {err:?}"))?;
        assert!(has_a, "iroh should contain bytes from put, hash={hash_a}");

        let copy_src = temp.path().join("copy-src.bin");
        tokio::fs::write(&copy_src, b"copy-path").await?;
        repo.put_path_copy(&copy_src).await?;
        let has_b = repo
            .iroh_store
            .blobs()
            .has(bytes_hash_to_iroh_hash(b"copy-path"))
            .await
            .map_err(|err| eyre::eyre!("iroh has check failed: {err:?}"))?;
        assert!(has_b, "iroh should contain bytes from put_path_copy");

        let ref_src = temp.path().join("ref-src.bin");
        tokio::fs::write(&ref_src, b"ref-path").await?;
        let ref_src_abs = ref_src.canonicalize()?;
        repo.put_path_reference(&ref_src_abs).await?;
        let has_c = repo
            .iroh_store
            .blobs()
            .has(bytes_hash_to_iroh_hash(b"ref-path"))
            .await
            .map_err(|err| eyre::eyre!("iroh has check failed: {err:?}"))?;
        assert!(has_c, "iroh should contain bytes from put_path_reference");

        Ok(())
    }

    #[tokio::test]
    async fn put_from_store_materializes_owned_blob() -> Res<()> {
        let (repo, _temp) = setup().await;
        let data = b"materialize-from-store";
        let hash = blob_id_from_bytes(*blake3::hash(data).as_bytes());

        repo.iroh_store
            .blobs()
            .add_bytes(data.to_vec())
            .await
            .map_err(|err| eyre::eyre!("iroh add bytes failed: {err:?}"))?;

        assert!(repo.get_path(hash).await.is_err());

        repo.put_from_store(hash).await?;
        let path = repo.get_path(hash).await?;
        let got = tokio::fs::read(path).await?;
        assert_eq!(got, data);

        Ok(())
    }

    #[tokio::test]
    async fn legacy_put_api_still_works() -> Res<()> {
        let (repo, _temp) = setup().await;
        let hash = repo.put(b"legacy").await?;
        let path = repo.get_path(hash).await?;
        assert_eq!(tokio::fs::read(path).await?, b"legacy");
        Ok(())
    }

    #[tokio::test]
    async fn blob_url_contract_unchanged() -> Res<()> {
        let (repo, _temp) = setup().await;
        let hash = repo.put(b"url").await?;
        let url = format!("{BLOB_SCHEME}:///{hash}");
        let parsed_hash = url
            .strip_prefix(&format!("{BLOB_SCHEME}:///"))
            .ok_or_eyre("invalid blob URL")?;
        let path = repo.get_path(parsed_hash.parse::<BlobId>()?).await?;
        assert!(tokio::fs::try_exists(path).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_blobs_missing() -> Res<()> {
        let (repo, _temp) = setup().await;
        assert!("nonexistent".parse::<BlobId>().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn put_path_reference_keeps_multiple_source_candidates() -> Res<()> {
        let (repo, temp) = setup().await;
        let source_a = temp.path().join("source-a.bin");
        let source_b = temp.path().join("source-b.bin");
        tokio::fs::write(&source_a, b"same-bytes").await?;
        tokio::fs::write(&source_b, b"same-bytes").await?;
        let source_a_abs = source_a.canonicalize()?;
        let source_b_abs = source_b.canonicalize()?;

        let hash_a = repo.put_path_reference(&source_a_abs).await?;
        let hash_b = repo.put_path_reference(&source_b_abs).await?;
        assert_eq!(hash_a, hash_b);

        tokio::fs::remove_file(&source_a_abs).await?;
        let got = repo.get_path(hash_a).await?;
        assert_eq!(got, source_b_abs);
        Ok(())
    }

    #[tokio::test]
    async fn put_path_reference_merges_source_paths_under_concurrency() -> Res<()> {
        let (repo, temp) = setup().await;
        let source_a = temp.path().join("source-a-concurrent.bin");
        let source_b = temp.path().join("source-b-concurrent.bin");
        tokio::fs::write(&source_a, b"same-bytes").await?;
        tokio::fs::write(&source_b, b"same-bytes").await?;
        let source_a_abs = source_a.canonicalize()?;
        let source_b_abs = source_b.canonicalize()?;

        let repo_a = Arc::clone(&repo);
        let repo_b = Arc::clone(&repo);
        let (hash_a, hash_b) = tokio::try_join!(
            repo_a.put_path_reference(&source_a_abs),
            repo_b.put_path_reference(&source_b_abs)
        )?;
        assert_eq!(hash_a, hash_b);

        let object_paths = repo.object_paths(hash_a)?;
        let meta = repo
            .read_meta(&object_paths.meta)
            .await?
            .ok_or_eyre("expected metadata for concurrent references")?;
        assert!(
            meta.source_paths
                .iter()
                .any(|value| value == &source_a_abs.to_string_lossy()),
            "expected first source path to be preserved"
        );
        assert!(
            meta.source_paths
                .iter()
                .any(|value| value == &source_b_abs.to_string_lossy()),
            "expected second source path to be preserved"
        );
        Ok(())
    }

    #[tokio::test]
    async fn blob_hash_must_be_valid_base58() -> Res<()> {
        assert!("bafakehash".parse::<BlobId>().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn materialize_uses_hash_filename_layout() -> Res<()> {
        let (repo, _temp) = setup().await;
        let hash = repo.put(b"materialize-layout").await?;
        let out = repo
            .materialize(
                hash,
                BlobMaterializeRequest::Filename("preview.yaml".into()),
            )
            .await?;
        let expected = repo
            .root
            .join("staging")
            .join(blob_hash_from_id(hash))
            .join("preview.yaml");
        assert_eq!(out, expected);
        assert!(tokio::fs::try_exists(&out).await?);
        Ok(())
    }

    #[tokio::test]
    async fn cleanup_staging_removes_materialized_files() -> Res<()> {
        let (repo, _temp) = setup().await;
        let hash = repo.put(b"cleanup-me").await?;
        let out = repo
            .materialize(hash, BlobMaterializeRequest::Extension("jpg".into()))
            .await?;
        assert!(tokio::fs::try_exists(&out).await?);
        repo.cleanup_staging().await?;
        assert!(!tokio::fs::try_exists(&out).await?);
        Ok(())
    }

    #[tokio::test]
    async fn materialize_reference_blob_survives_source_mutation_and_delete() -> Res<()> {
        let (repo, temp) = setup().await;
        let source = temp.path().join("ref-source.txt");
        tokio::fs::write(&source, b"original-reference-bytes").await?;
        let source_abs = source.canonicalize()?;

        let hash = repo.put_path_reference(&source_abs).await?;
        let out = repo
            .materialize(
                hash,
                BlobMaterializeRequest::Filename("snapshot.txt".into()),
            )
            .await?;
        let before = tokio::fs::read(&out).await?;
        assert_eq!(before, b"original-reference-bytes");

        tokio::fs::write(&source_abs, b"mutated-reference-bytes").await?;
        let after_mutation = tokio::fs::read(&out).await?;
        assert_eq!(after_mutation, b"original-reference-bytes");

        tokio::fs::remove_file(&source_abs).await?;
        assert!(tokio::fs::try_exists(&out).await?);
        let after_delete = tokio::fs::read(&out).await?;
        assert_eq!(after_delete, b"original-reference-bytes");

        repo.cleanup_staging().await?;
        assert!(!tokio::fs::try_exists(&out).await?);
        Ok(())
    }
}
