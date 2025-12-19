use crate::interlude::*;

#[derive(Clone)]
pub struct BlobsRepo {
    root: PathBuf,
}

impl BlobsRepo {
    pub async fn new(root: PathBuf) -> Result<Self, eyre::Report> {
        tokio::fs::create_dir_all(&root).await?;
        Ok(Self { root })
    }

    pub async fn put(&self, data: &[u8]) -> Result<String, eyre::Report> {
        let hash = utils_rs::hash::blake3_hash_bytes(data);
        let path = self.root.join(&hash);
        
        if !path.exists() {
            tokio::fs::write(&path, data).await?;
        }

        Ok(hash)
    }

    pub async fn get_path(&self, hash: &str) -> Result<PathBuf, eyre::Report> {
        // We can just construct the path directly since we use hash as filename
        let path = self.root.join(hash);
        if path.exists() {
            Ok(path)
        } else {
            Err(eyre::eyre!("Blob not found: {}", hash))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (BlobsRepo, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let repo = BlobsRepo::new(temp_dir.path().to_path_buf()).await.unwrap();
        (repo, temp_dir)
    }

    #[tokio::test]
    async fn test_blobs_smoke() -> Res<()> {
        let (repo, _temp) = setup().await;
        let data = b"hello world";
        
        let hash = repo.put(data).await?;
        let expected_hash = utils_rs::hash::blake3_hash_bytes(data);
        assert_eq!(hash, expected_hash);

        let path = repo.get_path(&hash).await?;
        let saved_data = tokio::fs::read(path).await?;
        assert_eq!(saved_data, data);

        Ok(())
    }

    #[tokio::test]
    async fn test_blobs_deduplication() -> Res<()> {
        let (repo, temp) = setup().await;
        let data = b"duplicate data";
        
        let hash1 = repo.put(data).await?;
        let hash2 = repo.put(data).await?;
        
        assert_eq!(hash1, hash2);

        // Check filesystem
        let mut entries = tokio::fs::read_dir(temp.path()).await?;
        let mut count = 0;
        while let Some(_) = entries.next_entry().await? {
            count += 1;
        }
        assert_eq!(count, 1, "Should only have one file for duplicate data");

        Ok(())
    }

    #[tokio::test]
    async fn test_blobs_missing() -> Res<()> {
        let (repo, _temp) = setup().await;
        let res = repo.get_path("nonexistent").await;
        assert!(res.is_err());
        Ok(())
    }
}
