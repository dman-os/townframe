//! The filesystem binding of a repo's keyhive storage.

use crate::keyhive_storage::create_layout;
use secrets_rs::SecretStore;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

/// An opened keyhive archive directory: the filesystem binding of a repo's keyhive storage.
///
/// The composition root opens it (creating the directory layout) and hands it to
/// [`BigRepo::boot`](crate::BigRepo::boot) in [`Storage`](crate::Storage); the repo never opens
/// a location itself. Event log and secret blobs are in the repo's SQL database; this holds the
/// archives, and where the data-encryption keys for the secret blobs come from.
#[derive(Clone)]
pub struct KeyhiveFiles {
    pub(crate) root: PathBuf,
    pub(crate) secrets: Option<Arc<SecretStore>>,
}

impl KeyhiveFiles {
    /// Opens (creating if needed) the directory layout at `root`.
    ///
    /// `secrets` sources the data-encryption keys from the OS keyring; with `None` they are
    /// 0600 files under `root` (headless systems without a keyring).
    pub fn open(root: PathBuf, secrets: Option<Arc<SecretStore>>) -> io::Result<Self> {
        create_layout(&root)?;
        Ok(Self { root, secrets })
    }
}

impl std::fmt::Debug for KeyhiveFiles {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyhiveFiles")
            .field("root", &self.root)
            .field("keyring", &self.secrets.is_some())
            .finish()
    }
}
