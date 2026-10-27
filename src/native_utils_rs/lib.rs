#[cfg(target_family = "wasm")]
compile_error!("native_utils_rs requires a native target; use utils_rs or wasm_utils_rs instead");

pub mod downloader;

pub async fn file_exists(path: &std::path::Path) -> Result<bool, std::io::Error> {
    match tokio::fs::try_exists(path).await {
        Ok(exists) => Ok(exists),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn file_exists_distinguishes_present_and_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        assert!(!super::file_exists(&path).await.unwrap());
        tokio::fs::write(&path, b"contents").await.unwrap();
        assert!(super::file_exists(&path).await.unwrap());
    }
}
