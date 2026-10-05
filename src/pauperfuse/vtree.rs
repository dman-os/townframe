//! Recorded backend trees. Observations are not application acknowledgement or checkout state.

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::LazyLock;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::backends::RelPath;
use crate::backends::{BackendId, BackendTree, Description, OutputVersion, Source, TreeEntry};

mod encoding;
mod migrate;
mod queries;
#[cfg(test)]
mod tests;

static MIGRATOR: LazyLock<sqlx::migrate::Migrator> = LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./sql/migrations");
    migrator.dangerous_set_table_name("_pauperfuse_migrations");
    migrator
});

/// Store-local registry identity, not a producer's output selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendKey(i64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TreeVersion {
    pub backend: BackendKey,
    pub generation: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("unsupported tree encoding: {0}")]
    Format(String),
    #[error("invalid tree encoding: {0}")]
    Encoding(String),
    #[error("backend is not registered: {0:?}")]
    UnknownBackend(BackendKey),
    #[error("source backend is not registered: {0:?}")]
    UnknownSource(BackendId),
    #[error("entries are duplicated or out of order at {0}")]
    Order(RelPath),
    #[error("non-directory entry at the tree root")]
    RootKind,
    #[error("file size exceeds SQLite integer range: {0}")]
    Size(u64),
    #[error("tree generation changed: expected {expected:?}, got {actual}")]
    Stale { expected: TreeVersion, actual: u64 },
    #[error("recorded tree scan already failed")]
    ScanFailed,
}

#[derive(Debug, thiserror::Error)]
pub enum ObservationError<E: Error> {
    #[error("backend observation failed: {0}")]
    Backend(#[source] E),
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone)]
pub struct VtreeStore {
    pool: SqlitePool,
}

impl VtreeStore {
    /// Reads one bounded page at a time, without pinning a database snapshot.
    pub fn scan(&self, version: TreeVersion, page_size: NonZeroU32) -> Scan {
        Scan {
            store: self.clone(),
            version,
            page_size,
            buffered: VecDeque::new(),
            after: None,
            state: ScanState::Reading,
        }
    }

    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        migrate::ensure_format(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn register(&self, name: &BackendId) -> Result<BackendKey, StoreError> {
        let id = sqlx::query_scalar(queries::REGISTER)
            .bind(&name.0)
            .fetch_one(&self.pool)
            .await?;
        Ok(BackendKey(id))
    }

    pub async fn lookup(&self, name: &BackendId) -> Result<Option<BackendKey>, StoreError> {
        let id = sqlx::query_scalar(queries::LOOKUP)
            .bind(&name.0)
            .fetch_optional(&self.pool)
            .await?;
        Ok(id.map(BackendKey))
    }

    pub async fn version(&self, backend: BackendKey) -> Result<TreeVersion, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let generation = generation(&mut transaction, backend).await?;
        transaction.commit().await?;
        Ok(TreeVersion {
            backend,
            generation,
        })
    }

    /// Streams a complete observation into one transaction; any failure rolls it back.
    pub async fn replace<T: BackendTree>(
        &self,
        backend: BackendKey,
        tree: &mut T,
    ) -> Result<TreeVersion, ObservationError<T::Error>> {
        let mut transaction = self.pool.begin().await.map_err(StoreError::from)?;
        generation(&mut transaction, backend).await?;
        sqlx::query(queries::CLEAR)
            .bind(backend.0)
            .execute(&mut *transaction)
            .await
            .map_err(StoreError::from)?;
        let mut previous = None;
        let mut sources = HashMap::new();
        while let Some(entry) = tree.next_entry().await.map_err(ObservationError::Backend)? {
            if previous.as_ref().is_some_and(|path| path >= &entry.path) {
                return Err(StoreError::Order(entry.path).into());
            }
            insert(&mut transaction, backend, &entry, &mut sources).await?;
            previous = Some(entry.path);
        }
        let next: i64 = sqlx::query_scalar(queries::BUMP)
            .bind(backend.0)
            .fetch_one(&mut *transaction)
            .await
            .map_err(StoreError::from)?;
        transaction.commit().await.map_err(StoreError::from)?;
        Ok(TreeVersion {
            backend,
            generation: next.try_into().expect("negative generation"),
        })
    }

    /// Each bounded page checks its generation in the same read transaction as its rows.
    /// A replacement between pages yields Stale instead of a mixed-generation tree.
    pub async fn page(
        &self,
        version: TreeVersion,
        after: Option<&RelPath>,
        limit: NonZeroU32,
    ) -> Result<Vec<TreeEntry>, StoreError> {
        let mut transaction = self.pool.begin().await?;
        let actual = generation(&mut transaction, version.backend).await?;
        if actual != version.generation {
            return Err(StoreError::Stale {
                expected: version,
                actual,
            });
        }
        let rows = if let Some(path) = after {
            sqlx::query(queries::PAGE_AFTER)
                .bind(version.backend.0)
                .bind(encoding::encode_path(path))
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *transaction)
                .await?
        } else {
            sqlx::query(queries::PAGE_START)
                .bind(version.backend.0)
                .bind(i64::from(limit.get()))
                .fetch_all(&mut *transaction)
                .await?
        };
        let entries = rows.iter().map(decode).collect::<Result<_, _>>()?;
        transaction.commit().await?;
        Ok(entries)
    }
}

enum ScanState {
    Reading,
    Complete,
    Failed,
}

/// Buffered entries retain their version. Replacement is detected at the next
/// page request, including the empty page that confirms EOF.
pub struct Scan {
    store: VtreeStore,
    version: TreeVersion,
    page_size: NonZeroU32,
    buffered: VecDeque<TreeEntry>,
    after: Option<RelPath>,
    state: ScanState,
}

impl BackendTree for Scan {
    type Error = StoreError;

    async fn next_entry(&mut self) -> Result<Option<TreeEntry>, Self::Error> {
        match self.state {
            ScanState::Complete => return Ok(None),
            ScanState::Failed => return Err(StoreError::ScanFailed),
            ScanState::Reading => {}
        }
        if self.buffered.is_empty() {
            // Do not change the cursor or buffer before this await succeeds.
            // Dropping a pending call leaves the reader at the same position.
            let page = match self
                .store
                .page(self.version, self.after.as_ref(), self.page_size)
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    self.state = ScanState::Failed;
                    return Err(error);
                }
            };
            if page.is_empty() {
                self.state = ScanState::Complete;
                return Ok(None);
            }
            self.buffered = page.into();
        }
        let entry = self.buffered.pop_front().expect("page is not empty");
        self.after = Some(entry.path.clone());
        Ok(Some(entry))
    }
}

async fn generation(
    transaction: &mut Transaction<'_, Sqlite>,
    backend: BackendKey,
) -> Result<u64, StoreError> {
    let value: Option<i64> = sqlx::query_scalar(queries::GENERATION)
        .bind(backend.0)
        .fetch_optional(&mut **transaction)
        .await?;
    let value = value.ok_or(StoreError::UnknownBackend(backend))?;
    Ok(value.try_into().expect("negative generation"))
}

async fn insert(
    transaction: &mut Transaction<'_, Sqlite>,
    backend: BackendKey,
    entry: &TreeEntry,
    sources: &mut HashMap<BackendId, i64>,
) -> Result<(), StoreError> {
    let path = encoding::encode_path(&entry.path);
    if entry.path.is_root() && !matches!(entry.description, Description::Directory) {
        return Err(StoreError::RootKind);
    }
    let (kind, source_id, output, version, size, target) = match &entry.description {
        Description::File { source, size } => {
            let source_id = if let Some(id) = sources.get(&source.backend) {
                *id
            } else {
                let id: Option<i64> = sqlx::query_scalar(queries::LOOKUP)
                    .bind(&source.backend.0)
                    .fetch_optional(&mut **transaction)
                    .await?;
                let id = id.ok_or_else(|| StoreError::UnknownSource(source.backend.clone()))?;
                let previous = sources.insert(source.backend.clone(), id);
                assert!(previous.is_none(), "source resolved twice");
                id
            };
            let size = size
                .map(|size| i64::try_from(size).map_err(|_| StoreError::Size(size)))
                .transpose()?;
            (
                0,
                Some(source_id),
                Some(source.output.output.as_slice()),
                Some(source.output.version.as_slice()),
                size,
                None,
            )
        }
        Description::Directory => (1, None, None, None, None, None),
        Description::Symlink { target } => (
            2,
            None,
            None,
            None,
            None,
            Some(encoding::encode_target(target)?),
        ),
    };
    sqlx::query(queries::INSERT)
        .bind(backend.0)
        .bind(path)
        .bind(kind)
        .bind(source_id)
        .bind(output)
        .bind(version)
        .bind(size)
        .bind(target)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

fn decode(row: &SqliteRow) -> Result<TreeEntry, StoreError> {
    let encoded: Vec<u8> = row.try_get("path")?;
    let path = encoding::decode_path(&encoded)?;
    let description = match row.try_get::<i64, _>("kind")? {
        0 => {
            let size: Option<i64> = row.try_get("size")?;
            Description::File {
                source: Source {
                    backend: BackendId(row.try_get("source_name")?),
                    output: OutputVersion {
                        output: row.try_get("source_output")?,
                        version: row.try_get("source_version")?,
                    },
                },
                size: size.map(|value| u64::try_from(value).expect("negative stored size")),
            }
        }
        1 => Description::Directory,
        2 => Description::Symlink {
            target: encoding::decode_target(row.try_get("target")?)?,
        },
        kind => return Err(StoreError::Encoding(format!("unknown entry kind {kind}"))),
    };
    if path.is_root() && !matches!(description, Description::Directory) {
        return Err(StoreError::RootKind);
    }
    Ok(TreeEntry { path, description })
}
