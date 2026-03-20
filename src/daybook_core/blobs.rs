use crate::interlude::*;

use am_utils_rs::partition::PartitionStore;
use iroh_blobs::api::blobs::{AddPathOptions, ImportMode};
use iroh_blobs::store::fs::FsStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait PartitionMembershipWriter: Send + Sync {
    async fn add_member(
        &self,
        partition_id: &str,
        member_id: &str,
        payload: &serde_json::Value,
    ) -> Res<()>;
    async fn remove_member(
        &self,
        partition_id: &str,
        member_id: &str,
        payload: &serde_json::Value,
    ) -> Res<()>;
}

#[derive(Clone)]
pub struct PartitionStoreMembershipWriter {
    partition_store: Arc<PartitionStore>,
}

impl PartitionStoreMembershipWriter {
    pub fn new(partition_store: Arc<PartitionStore>) -> Self {
        Self { partition_store }
    }
}

#[async_trait]
impl PartitionMembershipWriter for PartitionStoreMembershipWriter {
    async fn add_member(
        &self,
        partition_id: &str,
        member_id: &str,
        payload: &serde_json::Value,
    ) -> Res<()> {
        self.partition_store
            .add_member(&partition_id.to_string(), member_id, payload)
            .await
    }

    async fn remove_member(
        &self,
        partition_id: &str,
        member_id: &str,
        payload: &serde_json::Value,
    ) -> Res<()> {
        self.partition_store
            .remove_member(&partition_id.to_string(), member_id, payload)
            .await
    }
}

#[derive(Clone)]
pub struct NoopPartitionMembershipWriter;

#[async_trait]
impl PartitionMembershipWriter for NoopPartitionMembershipWriter {
    async fn add_member(
        &self,
        _partition_id: &str,
        _member_id: &str,
        _payload: &serde_json::Value,
    ) -> Res<()> {
        Ok(())
    }

    async fn remove_member(
        &self,
        _partition_id: &str,
        _member_id: &str,
        _payload: &serde_json::Value,
    ) -> Res<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct BlobsRepo {
    root: PathBuf,
    src_local_user_path: String,
    iroh_store: iroh_blobs::api::Store,
    hash_locks: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
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
    hash: String,
    mode: BlobMode,
    size_bytes: u64,
    mime: Option<String>,
    src_local_user_path: String,
    source_paths: Vec<String>,
    created_at_unix_secs: i64,
    iroh_ingested: bool,
}

struct ObjectPaths {
    dir: PathBuf,
    blob: PathBuf,
    meta: PathBuf,
}

pub const BLOB_SCHEME: &str = "db+blob";
pub const BLOB_SCOPE_DOCS_PARTITION_ID: &str = "blob_scope/docs";
pub const BLOB_SCOPE_PLUGS_PARTITION_ID: &str = "blob_scope/plugs";

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

    fn as_payload_scope(self) -> &'static str {
        match self {
            Self::Docs => "docs",
            Self::Plugs => "plugs",
        }
    }
}

impl BlobsRepo {
    pub async fn new(
        root: PathBuf,
        src_local_user_path: String,
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

    pub async fn add_hash_to_scope(&self, scope: BlobScope, hash: &str) -> Res<()> {
        let payload = self.partition_member_payload(scope, hash).await?;
        self.partition_writer
            .add_member(scope.partition_id(), hash, &payload)
            .await
    }

    pub async fn remove_hash_from_scope(&self, scope: BlobScope, hash: &str) -> Res<()> {
        let payload = self.partition_member_payload(scope, hash).await?;
        self.partition_writer
            .remove_member(scope.partition_id(), hash, &payload)
            .await
    }

    async fn partition_member_payload(
        &self,
        scope: BlobScope,
        hash: &str,
    ) -> Res<serde_json::Value> {
        let object_paths = self.object_paths(hash)?;
        let size_bytes = if let Some(meta) = self.read_meta(&object_paths.meta).await? {
            meta.size_bytes
        } else if tokio::fs::try_exists(&object_paths.blob).await? {
            tokio::fs::metadata(&object_paths.blob).await?.len()
        } else {
            0
        };
        Ok(serde_json::json!({
            "scope": scope.as_payload_scope(),
            "size_bytes": size_bytes,
        }))
    }

    pub async fn put_path_copy(&self, source_path: &Path) -> Res<String> {
        let source_path = source_path.canonicalize()?;
        let source_meta = tokio::fs::metadata(&source_path).await?;
        if !source_meta.is_file() {
            eyre::bail!("source path is not a file: {}", source_path.display());
        }

        let source_snapshot = self.create_source_snapshot(&source_path).await?;
        let result = async {
            let hash =
                utils_rs::hash::blake3_hash_reader(tokio::fs::File::open(&source_snapshot).await?)
                    .await?;
            let object_paths = self.object_paths(&hash)?;

            tokio::fs::create_dir_all(&object_paths.dir).await?;
            if !tokio::fs::try_exists(&object_paths.blob).await? {
                self.atomic_copy_file(&source_snapshot, &object_paths.blob)
                    .await?;
            }

            let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
            let mut meta = self.build_meta(
                hash.clone(),
                BlobMode::OwnedCopy,
                blob_meta.len(),
                Vec::new(),
                false,
            );
            self.write_meta(&object_paths.meta, &meta).await?;
            self.ingest_path_with_iroh(&object_paths.blob, &hash)
                .await?;
            meta.iroh_ingested = true;
            self.write_meta(&object_paths.meta, &meta).await?;

            Ok(hash)
        }
        .await;
        let _ = tokio::fs::remove_file(&source_snapshot).await;
        result
    }

    pub async fn put_path_reference(&self, source_path: &Path) -> Res<String> {
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
            let hash =
                utils_rs::hash::blake3_hash_reader(tokio::fs::File::open(&source_snapshot).await?)
                    .await?;
            let object_paths = self.object_paths(&hash)?;
            tokio::fs::create_dir_all(&object_paths.dir).await?;
            let hash_lock = self.lock_for_hash(&hash);
            let _hash_guard = hash_lock.lock().await;

            let source_path_string = source_path
                .to_str()
                .ok_or_else(|| eyre::eyre!("reference path must be valid UTF-8"))?
                .to_string();
            let mut meta = self.build_meta(
                hash.clone(),
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
            self.ingest_path_with_iroh(&source_snapshot, &hash).await?;
            meta.iroh_ingested = true;
            self.write_meta(&object_paths.meta, &meta).await?;
            Ok(hash)
        }
        .await;
        let _ = tokio::fs::remove_file(&source_snapshot).await;
        result
    }

    /// Compatibility alias that ingests bytes as an owned blob.
    pub async fn put(&self, data: &[u8]) -> Result<String, eyre::Report> {
        let hash = utils_rs::hash::blake3_hash_bytes(data);
        let object_paths = self.object_paths(&hash)?;

        tokio::fs::create_dir_all(&object_paths.dir).await?;
        if !tokio::fs::try_exists(&object_paths.blob).await? {
            self.atomic_write(&object_paths.blob, data).await?;
        }

        let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
        let mut meta = self.build_meta(
            hash.clone(),
            BlobMode::OwnedCopy,
            blob_meta.len(),
            Vec::new(),
            false,
        );
        self.write_meta(&object_paths.meta, &meta).await?;
        self.ingest_path_with_iroh(&object_paths.blob, &hash)
            .await?;
        meta.iroh_ingested = true;
        self.write_meta(&object_paths.meta, &meta).await?;

        Ok(hash)
    }

    pub async fn get_path(&self, hash: &str) -> Result<PathBuf, eyre::Report> {
        let object_paths = self.object_paths(hash)?;
        if tokio::fs::try_exists(&object_paths.blob).await? {
            if self.read_meta(&object_paths.meta).await?.is_none() {
                let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
                let recovered = self.build_meta(
                    hash.to_string(),
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
                    let source_hash = utils_rs::hash::blake3_hash_reader(
                        tokio::fs::File::open(&source_path).await?,
                    )
                    .await?;
                    if source_hash == meta.hash {
                        return Ok(source_path);
                    } else if tokio::fs::try_exists(&object_paths.blob).await? {
                        return Ok(object_paths.blob);
                    } else if drift_error.is_none() {
                        drift_error = Some(format!(
                            "Referenced blob hash diverged for {}: expected={}, got={}",
                            source_path.display(),
                            meta.hash,
                            source_hash
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

    pub async fn has_hash(&self, hash: &str) -> Res<bool> {
        Ok(self.get_path(hash).await.is_ok())
    }

    pub async fn put_from_store(&self, hash: &str) -> Res<String> {
        let object_paths = self.object_paths(hash)?;
        tokio::fs::create_dir_all(&object_paths.dir).await?;

        if !tokio::fs::try_exists(&object_paths.blob).await? {
            let iroh_hash = daybook_hash_to_iroh_hash(hash)?;
            self.iroh_store
                .blobs()
                .export(iroh_hash, &object_paths.blob)
                .await
                .map_err(|err| eyre::eyre!("error exporting blob from iroh store: {err:?}"))?;
        }

        let blob_meta = tokio::fs::metadata(&object_paths.blob).await?;
        let meta = self.build_meta(
            hash.to_string(),
            BlobMode::OwnedCopy,
            blob_meta.len(),
            Vec::new(),
            true,
        );
        self.write_meta(&object_paths.meta, &meta).await?;

        Ok(hash.to_string())
    }

    fn object_paths(&self, hash: &str) -> Res<ObjectPaths> {
        if hash.len() < 4 {
            eyre::bail!("invalid blob hash: {hash}");
        }
        utils_rs::hash::decode_base58_multibase(hash)?;
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
        hash: String,
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

    async fn ingest_path_with_iroh(&self, path: &Path, hash: &str) -> Res<()> {
        self.iroh_store
            .blobs()
            .add_path_with_opts(AddPathOptions {
                path: path.to_path_buf(),
                format: iroh_blobs::BlobFormat::Raw,
                mode: ImportMode::TryReference,
            })
            .with_named_tag(hash.as_bytes())
            .await
            .map_err(|err| eyre::eyre!("error ingesting path into iroh store: {err:?}"))?;
        Ok(())
    }

    fn lock_for_hash(&self, hash: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.hash_locks.lock().expect(ERROR_MUTEX);
        Arc::clone(
            guard
                .entry(hash.to_string())
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
    let decoded = utils_rs::hash::decode_base58_multibase(hash)?;
    if decoded.len() < 34 {
        eyre::bail!("invalid daybook blob hash bytes");
    }
    let digest = &decoded[decoded.len() - 32..];
    let digest: [u8; 32] = digest.try_into().expect("length checked");
    Ok(iroh_blobs::Hash::from_bytes(digest))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (Arc<BlobsRepo>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo = BlobsRepo::new(
            temp_dir.path().to_path_buf(),
            "/local/test-user".to_string(),
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
        let expected_hash = utils_rs::hash::blake3_hash_bytes(data);
        assert_eq!(hash, expected_hash);

        let path = repo.get_path(&hash).await?;
        let saved_data = tokio::fs::read(path).await?;
        assert_eq!(saved_data, data);

        let object_paths = repo.object_paths(&hash)?;
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

        let object_paths = repo.object_paths(&hash1)?;
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

        let path = repo.get_path(&hash).await?;
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

        let err = repo.get_path(&hash).await.unwrap_err();
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
        let object_paths = repo.object_paths(&hash)?;

        let bogus_ref = BlobMetaV1 {
            version: 1,
            hash: hash.clone(),
            mode: BlobMode::Reference,
            size_bytes: 123,
            mime: None,
            src_local_user_path: "/local/test-user".to_string(),
            source_paths: vec!["/tmp/does/not/exist".to_string()],
            created_at_unix_secs: 1,
            iroh_ingested: true,
        };
        repo.write_meta(&object_paths.meta, &bogus_ref).await?;

        let got = repo.get_path(&hash).await?;
        assert_eq!(got, object_paths.blob);
        Ok(())
    }

    #[tokio::test]
    async fn metadata_roundtrip() -> Res<()> {
        let (repo, _temp) = setup().await;
        let data = b"roundtrip";

        let hash = repo.put(data).await?;
        let object_paths = repo.object_paths(&hash)?;
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
        let hash = utils_rs::hash::blake3_hash_bytes(data);

        repo.iroh_store
            .blobs()
            .add_bytes(data.to_vec())
            .await
            .map_err(|err| eyre::eyre!("iroh add bytes failed: {err:?}"))?;

        assert!(repo.get_path(&hash).await.is_err());

        repo.put_from_store(&hash).await?;
        let path = repo.get_path(&hash).await?;
        let got = tokio::fs::read(path).await?;
        assert_eq!(got, data);

        Ok(())
    }

    #[tokio::test]
    async fn legacy_put_api_still_works() -> Res<()> {
        let (repo, _temp) = setup().await;
        let hash = repo.put(b"legacy").await?;
        let path = repo.get_path(&hash).await?;
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
        let path = repo.get_path(parsed_hash).await?;
        assert!(tokio::fs::try_exists(path).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_blobs_missing() -> Res<()> {
        let (repo, _temp) = setup().await;
        let res = repo.get_path("nonexistent").await;
        assert!(res.is_err());
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
        let got = repo.get_path(&hash_a).await?;
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

        let object_paths = repo.object_paths(&hash_a)?;
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
    async fn blob_hash_must_be_base58_multibase() -> Res<()> {
        let (repo, _temp) = setup().await;
        assert!(repo.get_path("bafakehash").await.is_err());
        assert!(repo.put_from_store("bafakehash").await.is_err());
        Ok(())
    }
}
