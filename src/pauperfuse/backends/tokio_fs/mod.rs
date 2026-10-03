//! Unix filesystem delivery: native names, evidence, staging, and verified file puts.
//! Whole-tree producer observation and retained filesystem versions are not implemented.

pub(crate) mod names;
pub use names::NativePathError;

use crate::interlude::*;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::backends::RelPath;
use crate::backends::{ByteAccess, ByteReader, Source};

/// Content evidence at a path. The checkout policy supplies ownership separately.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEvidence {
    pub length: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpectedFile {
    Absent,
    Present(FileEvidence),
}

#[derive(Clone, Debug)]
pub struct FilePut {
    pub path: RelPath,
    pub source: Source,
    pub expected: ExpectedFile,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstalledFile {
    pub path: RelPath,
    pub evidence: FileEvidence,
}

#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("invalid destination {path:?}: {reason}")]
    Invalid { path: PathBuf, reason: String },
    #[error("target changed: {0:?}")]
    Changed(PathBuf),
    #[error("{operation} failed at {path:?}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} failed for source at {path:?}: {source}")]
    Source {
        operation: &'static str,
        path: RelPath,
        #[source]
        // Send+Sync flows through async Source-open futures across runtime consumers.
        source: Box<dyn Error + Send + Sync>,
    },
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> FileError {
    FileError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
#[error("preparation failed: {cause}; staging cleanup failure: {cleanup:?}")]
pub struct PrepareError {
    #[source]
    pub cause: Box<FileError>,
    pub cleanup: Option<FileError>,
    pub staging_directory: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
#[error("application stopped after {completed:?}: {cause}")]
pub struct ApplyError {
    #[source]
    pub cause: Box<FileError>,
    pub completed: Vec<InstalledFile>,
    pub awaiting_verification: Option<RelPath>,
    pub remaining: Vec<RelPath>,
}

struct StagedFile {
    put: FilePut,
    staged_path: PathBuf,
    evidence: FileEvidence,
}

/// Owns staging until `cleanup` succeeds. Dropping this value does not delete files;
/// retain it after an error to retry or explicitly clean up its staging directory.
/// Run asynchronous operations to completion: cancellation is not rollback or recovery.
#[must_use = "prepared files require explicit application or cleanup"]
pub struct PreparedFiles {
    root: PathBuf,
    staging: PathBuf,
    files: Vec<StagedFile>,
    installed: Vec<InstalledFile>,
    awaiting_verification: bool,
    cleaned: bool,
    #[cfg(test)]
    injected_failure: Option<FailurePoint>,
}

#[cfg(test)]
#[derive(PartialEq, Eq)]
enum FailurePoint {
    BeforeRename(usize),
    BeforeVerification(usize),
}

pub struct TokioFs {
    root: PathBuf,
}

impl TokioFs {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub async fn observe(&self, path: &RelPath) -> Result<ExpectedFile, FileError> {
        let destination = checked_destination(&self.root, path).await?;
        evidence(&destination).await
    }

    /// Stages a whole batch without touching destinations. Cancellation can leave staging
    /// behind; this slice provides explicit cleanup, not a durable restart ledger.
    pub async fn prepare<A: ByteAccess>(
        &self,
        puts: Vec<FilePut>,
        access: &A,
    ) -> Result<PreparedFiles, PrepareError>
    where
        A::Error: Send + Sync + 'static,
        <A::Reader as ByteReader>::Error: Send + Sync + 'static,
    {
        let metadata = tokio::fs::symlink_metadata(&self.root)
            .await
            .map_err(|error| preparation_error(io("inspect checkout", &self.root, error)))?;
        if !metadata.is_dir() || metadata.is_symlink() {
            return Err(preparation_error(FileError::Invalid {
                path: self.root.clone(),
                reason: "checkout is not a real directory".into(),
            }));
        }
        let root = tokio::fs::canonicalize(&self.root)
            .await
            .map_err(|error| preparation_error(io("canonicalize checkout", &self.root, error)))?;
        let mut paths = puts.iter().map(|put| &put.path).collect::<Vec<_>>();
        paths.sort();
        for adjacent in paths.windows(2) {
            if adjacent[0].is_prefix_of(adjacent[1]) {
                return Err(preparation_error(FileError::Invalid {
                    path: root.clone(),
                    reason: "duplicate or ancestor destinations".into(),
                }));
            }
        }
        for put in &puts {
            validate_target(&root, &put.path, &put.expected)
                .await
                .map_err(preparation_error)?;
        }
        let staging = create_staging(&root).await.map_err(preparation_error)?;
        let mut prepared = PreparedFiles {
            root,
            staging,
            files: Vec::new(),
            installed: Vec::new(),
            awaiting_verification: false,
            cleaned: false,
            #[cfg(test)]
            injected_failure: None,
        };
        for (index, put) in puts.into_iter().enumerate() {
            let staged_path = prepared.staging.join(index.to_string());
            let result = stage_file(&put, &staged_path, access).await;
            match result {
                Ok(evidence) => prepared.files.push(StagedFile {
                    put,
                    staged_path,
                    evidence,
                }),
                Err(cause) => {
                    let cleanup = prepared.cleanup().await.err();
                    let staging_directory = cleanup.as_ref().map(|_| prepared.staging.clone());
                    return Err(PrepareError {
                        cause: Box::new(cause),
                        cleanup,
                        staging_directory,
                    });
                }
            }
        }
        Ok(prepared)
    }
}

fn preparation_error(cause: FileError) -> PrepareError {
    PrepareError {
        cause: Box::new(cause),
        cleanup: None,
        staging_directory: None,
    }
}

impl PreparedFiles {
    pub fn completed(&self) -> &[InstalledFile] {
        &self.installed
    }
    pub fn staging_directory(&self) -> &Path {
        &self.staging
    }

    pub async fn apply(&mut self) -> Result<Vec<InstalledFile>, ApplyError> {
        assert!(!self.cleaned, "cannot apply a cleaned batch");
        // Recheck completed entries too: resuming never treats changed bytes as ours.
        for installed in &self.installed {
            let expected = ExpectedFile::Present(installed.evidence.clone());
            if let Err(cause) = validate_target(&self.root, &installed.path, &expected).await {
                return Err(self.interrupted(cause));
            }
        }
        if self.awaiting_verification
            && let Err(cause) = self.verify_renamed().await
        {
            return Err(self.interrupted(cause));
        }
        for staged in &self.files[self.installed.len()..] {
            if let Err(cause) =
                validate_target(&self.root, &staged.put.path, &staged.put.expected).await
            {
                return Err(self.interrupted(cause));
            }
        }
        while self.installed.len() < self.files.len() {
            let index = self.installed.len();
            let staged = &self.files[index];
            #[cfg(test)]
            if self.injected_failure == Some(FailurePoint::BeforeRename(index)) {
                return Err(self.interrupted(io(
                    "injected rename",
                    &staged.staged_path,
                    std::io::Error::other("injected application failure"),
                )));
            }
            let destination = match checked_destination(&self.root, &staged.put.path).await {
                Ok(path) => path,
                Err(cause) => return Err(self.interrupted(cause)),
            };
            if let Err(cause) =
                validate_target(&self.root, &staged.put.path, &staged.put.expected).await
            {
                return Err(self.interrupted(cause));
            }
            if let Err(error) = tokio::fs::rename(&staged.staged_path, &destination).await {
                return Err(self.interrupted(io("rename staged file", &destination, error)));
            }
            self.awaiting_verification = true;
            #[cfg(test)]
            if self.injected_failure == Some(FailurePoint::BeforeVerification(index)) {
                return Err(self.interrupted(io(
                    "injected verification",
                    &destination,
                    std::io::Error::other("injected verification failure"),
                )));
            }
            if let Err(cause) = self.verify_renamed().await {
                return Err(self.interrupted(cause));
            }
        }
        Ok(self.installed.clone())
    }

    async fn verify_renamed(&mut self) -> Result<(), FileError> {
        let staged = &self.files[self.installed.len()];
        let destination = checked_destination(&self.root, &staged.put.path).await?;
        match evidence(&destination).await? {
            ExpectedFile::Present(actual) if actual == staged.evidence => {
                self.installed.push(InstalledFile {
                    path: staged.put.path.clone(),
                    evidence: actual,
                });
                self.awaiting_verification = false;
                Ok(())
            }
            _ => Err(FileError::Changed(destination)),
        }
    }

    fn interrupted(&self, cause: FileError) -> ApplyError {
        let next = self.installed.len();
        ApplyError {
            cause: Box::new(cause),
            completed: self.installed.clone(),
            awaiting_verification: self
                .awaiting_verification
                .then(|| self.files[next].put.path.clone()),
            remaining: self.files[next + usize::from(self.awaiting_verification)..]
                .iter()
                .map(|file| file.put.path.clone())
                .collect(),
        }
    }

    pub async fn cleanup(&mut self) -> Result<(), FileError> {
        if !self.cleaned {
            tokio::fs::remove_dir_all(&self.staging)
                .await
                .map_err(|error| io("remove staging directory", &self.staging, error))?;
            self.cleaned = true;
        }
        Ok(())
    }
}

async fn checked_destination(root: &Path, relative: &RelPath) -> Result<PathBuf, FileError> {
    if relative.is_root() {
        return Err(FileError::Invalid {
            path: root.to_owned(),
            reason: "not a file path".into(),
        });
    }
    let native = TokioFs::to_native_path(relative).map_err(|error| FileError::Invalid {
        path: root.to_owned(),
        reason: error.to_string(),
    })?;
    use std::os::unix::fs::MetadataExt;

    let components = native.components().collect::<Vec<_>>();
    let mut path = root.to_owned();
    let mut root_device = None;
    // Root and every parent must already be real directories on the same filesystem.
    for component in
        std::iter::once(None).chain(components[..components.len() - 1].iter().map(Some))
    {
        if let Some(component) = component {
            path.push(component);
        }
        let metadata = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(|error| io("inspect parent", &path, error))?;
        if !metadata.is_dir() || metadata.is_symlink() {
            return Err(FileError::Invalid {
                path,
                reason: "parent is not a real directory".into(),
            });
        }
        if *root_device.get_or_insert(metadata.dev()) != metadata.dev() {
            return Err(FileError::Invalid {
                path,
                reason: "destination is on another filesystem".into(),
            });
        }
    }
    path.push(components.last().unwrap());
    Ok(path)
}

async fn validate_target(
    root: &Path,
    relative: &RelPath,
    expected: &ExpectedFile,
) -> Result<(), FileError> {
    let path = checked_destination(root, relative).await?;
    if &evidence(&path).await? != expected {
        return Err(FileError::Changed(path));
    }
    Ok(())
}

async fn evidence(path: &Path) -> Result<ExpectedFile, FileError> {
    match tokio::fs::symlink_metadata(path).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ExpectedFile::Absent);
        }
        Err(error) => return Err(io("inspect file", path, error)),
        Ok(metadata) if !metadata.is_file() || metadata.is_symlink() => {
            return Err(FileError::Invalid {
                path: path.to_owned(),
                reason: "target is not a regular file".into(),
            });
        }
        Ok(_) => {}
    }
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| io("open file", path, error))?;
    let mut buffer = [0; 64 * 1024];
    let mut digest = blake3::Hasher::new();
    let mut length = 0;
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|error| io("hash file", path, error))?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        length += count as u64;
    }
    Ok(ExpectedFile::Present(FileEvidence {
        length,
        digest: *digest.finalize().as_bytes(),
    }))
}

async fn stage_file<A: ByteAccess>(
    put: &FilePut,
    path: &Path,
    access: &A,
) -> Result<FileEvidence, FileError>
where
    A::Error: Send + Sync + 'static,
    <A::Reader as ByteReader>::Error: Send + Sync + 'static,
{
    let mut reader = access
        .open(&put.source)
        .await
        .map_err(|source| FileError::Source {
            operation: "open",
            path: put.path.clone(),
            source: Box::new(source),
        })?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .map_err(|error| io("create staged file", path, error))?;
    let mut buffer = [0; 64 * 1024];
    let mut digest = blake3::Hasher::new();
    let mut length = 0;
    loop {
        let count = reader
            .read_at(length, &mut buffer)
            .await
            .map_err(|source| FileError::Source {
                operation: "read",
                path: put.path.clone(),
                source: Box::new(source),
            })?;
        assert!(
            count <= buffer.len(),
            "reader returned an impossible byte count"
        );
        if count == 0 {
            break;
        }
        file.write_all(&buffer[..count])
            .await
            .map_err(|error| io("write staged file", path, error))?;
        digest.update(&buffer[..count]);
        length = length
            .checked_add(count as u64)
            .expect("file offset overflow");
    }
    file.flush()
        .await
        .map_err(|error| io("flush staged file", path, error))?;
    Ok(FileEvidence {
        length,
        digest: *digest.finalize().as_bytes(),
    })
}

async fn create_staging(root: &Path) -> Result<PathBuf, FileError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = root.parent().ok_or_else(|| FileError::Invalid {
        path: root.to_owned(),
        reason: "checkout has no sibling staging location".into(),
    })?;
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        let root_metadata = tokio::fs::metadata(root)
            .await
            .map_err(|error| io("inspect checkout filesystem", root, error))?;
        let parent_metadata = tokio::fs::metadata(parent)
            .await
            .map_err(|error| io("inspect staging filesystem", parent, error))?;
        if root_metadata.dev() != parent_metadata.dev() {
            return Err(FileError::Invalid {
                path: parent.to_owned(),
                reason: "staging must be on the checkout filesystem".into(),
            });
        }
        loop {
            let path = parent.join(format!(
                ".pauperfuse-stage-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let location = path.clone();
            let result = tokio::task::spawn_blocking(move || {
                std::fs::DirBuilder::new().mode(0o700).create(location)
            })
            .await
            .unwrap();
            match result {
                Ok(()) => return Ok(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(io("create staging directory", &path, error)),
            }
        }
    }
}

#[cfg(test)]
mod tests;
