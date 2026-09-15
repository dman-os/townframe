//! A SQLite-backed [`WillowStore`].
//!
//! The store is generic: it knows namespaces, subspaces, paths, timestamps and payloads, and
//! nothing about any embedder's concepts. `scope` is the one storage-level concession, fixed
//! when the store is constructed so several embedder instances can share a database, and it is
//! never a parameter of an entry or an [`Area`].
//!
//! # How the schema serves the contract
//!
//! The primary key is `(scope_id, namespace, subspace, path)` with `path` holding
//! [`encode_path`] output, so it orders exactly like [`EntryKey`]. A read seeks to
//! `(scope, namespace[, subspace])` and walks the key in order, which is why neither read query
//! needs a sort. `WITHOUT ROWID` makes that key the table itself.
//!
//! `timestamp` and `payload_length` hold big-endian `u64` bytes rather than INTEGERs, so every
//! recency and time comparison in SQL is memcmp and matches Rust's unsigned ordering. See the
//! migration for why a signed column would be wrong.
//!
//! Payloads live in their own table with `ON DELETE CASCADE`, which is what makes "a payload
//! row never outlives its entry" a schema invariant rather than a convention. A missing payload
//! row is exactly the contract's "entry present, payload not arrived".
//!
//! # What an insert does to a retained payload
//!
//! [`WillowStore::insert_entry`] must retain a payload when the stored entry is unchanged and
//! drop it when the entry changed. Both fall out of the writes, with no explicit branch:
//!
//! - The pruning delete removes a row when the new entry is newer than or equal to it, and the
//!   cascade takes its payload with it. A row at the new entry's own path that ties is excluded
//!   from the delete, so it keeps its payload; that is exactly the case where the entry did not
//!   change. The `prunes` helper in [`crate::store`] states the same rule in Rust.
//! - The entry upsert is an `ON CONFLICT DO UPDATE`, not a replace, so updating that unchanged
//!   row in place does not disturb its payload.
//!
//! So an unchanged entry keeps its payload, and a changed one has already lost it.
//!
//! A re-insert of the entry that is already stored is the degenerate case: it prunes nothing at
//! all, not even the strictly older descendants it would otherwise remove. See [`stage_entry`].

use std::sync::Arc;

use sqlx::sqlite::Sqlite;
use sqlx::{Row, Transaction};
use sqlx_utils_rs::SqlCtx;
use ufotofu::codec::{Decodable, Encodable};
use ufotofu::prelude::*;
use utils_rs::prelude::{Res, WrapErr};
use willow25::prelude::*;

use crate::interlude::*;
use crate::path_codec::{encode_path, prefix_range};
use crate::store::{AreaPage, AreaReadLimits, EntryKey, InsertOutcome, StoreError, WillowStore};

mod queries;

#[cfg(test)]
mod test;

/// Installed under a private table name so the schema can share a database with other modules'
/// schemas without either one claiming the default `_sqlx_migrations`.
static MIGRATOR: std::sync::LazyLock<sqlx::migrate::Migrator> = std::sync::LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./sql/migrations");
    migrator.dangerous_set_table_name("_big_willow_migrations");
    migrator
});

/// A Willow store persisted in SQLite.
pub struct SqliteWillowStore {
    ctx: SqlCtx,
    scope_id: i64,
}

impl SqliteWillowStore {
    /// Opens the store in `ctx`, partitioned to `scope_key`.
    ///
    /// The scope is resolved or created once here, so no later operation takes one.
    pub async fn new(ctx: SqlCtx, scope_key: impl Into<Arc<str>>) -> Res<Self> {
        MIGRATOR
            .run(&ctx.write_pool)
            .await
            .wrap_err("failed running big_willow migrations")?;

        let scope_key = scope_key.into();
        sqlx::query(queries::INSERT_SCOPE)
            .bind(scope_key.as_ref())
            .execute(&ctx.write_pool)
            .await
            .wrap_err("failed inserting the big_willow scope")?;
        let scope_id: i64 = sqlx::query_scalar(queries::SELECT_SCOPE_ID)
            .bind(scope_key.as_ref())
            .fetch_one(&ctx.write_pool)
            .await
            .wrap_err("failed reading the big_willow scope id")?;

        Ok(Self { ctx, scope_id })
    }

    /// Runs `entry` through the write path in one transaction.
    async fn stage(
        &self,
        entry: AuthorisedEntry,
        payload: Option<&[u8]>,
    ) -> Result<InsertOutcome, StoreError> {
        let scope_id = self.scope_id;
        // `with_write_tx` takes a closure that must yield a future for *any* transaction
        // lifetime, which only a `'static` future satisfies, so the borrowed payload is lifted
        // into an owned buffer here. Payloads in this store are advertisements, not bulk data.
        let payload = payload.map(<[u8]>::to_vec);
        self.ctx
            .with_write_tx(|mut tx| {
                Box::pin(async move {
                    let outcome =
                        stage_entry(&mut tx, scope_id, &entry, payload.as_deref()).await?;
                    Ok((outcome, tx))
                })
            })
            .await
            .map_err(backend_error)
    }
}

#[async_trait]
impl WillowStore for SqliteWillowStore {
    async fn insert_entry(&self, entry: AuthorisedEntry) -> Result<InsertOutcome, StoreError> {
        self.stage(entry, None).await
    }

    async fn insert_entry_with_payload(
        &self,
        entry: AuthorisedEntry,
        payload: &[u8],
    ) -> Result<InsertOutcome, StoreError> {
        // Validated before the transaction opens, so the write path only ever yields backend
        // errors and can share `with_write_tx`'s `sqlx::Error` channel.
        if payload.len() as u64 != entry.payload_length() {
            return Err(StoreError::PayloadLengthMismatch {
                expected: entry.payload_length(),
                actual: payload.len() as u64,
            });
        }
        if PayloadDigest::from_payload(payload) != *entry.payload_digest() {
            return Err(StoreError::PayloadDigestMismatch);
        }

        self.stage(entry, Some(payload)).await
    }

    async fn get_entry(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<Option<AuthorisedEntry>, StoreError> {
        let row = sqlx::query(queries::SELECT_ENTRY)
            .bind(self.scope_id)
            .bind(namespace_id.as_bytes().to_vec())
            .bind(subspace_id.as_bytes().to_vec())
            .bind(encode_path(path))
            .fetch_optional(&self.ctx.read_pool)
            .await
            .map_err(backend_error)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let blob: Vec<u8> = row.try_get("entry").map_err(backend_error)?;
        Ok(Some(decode_authorised_entry(&blob).await))
    }

    async fn get_payload(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let row = sqlx::query(queries::SELECT_PAYLOAD)
            .bind(self.scope_id)
            .bind(namespace_id.as_bytes().to_vec())
            .bind(subspace_id.as_bytes().to_vec())
            .bind(encode_path(path))
            .fetch_optional(&self.ctx.read_pool)
            .await
            .map_err(backend_error)?;

        let Some(row) = row else {
            return Ok(None);
        };

        Ok(Some(row.try_get("payload").map_err(backend_error)?))
    }

    async fn read_area(
        &self,
        namespace_id: &NamespaceId,
        area: &Area,
        resume_after: Option<&EntryKey>,
        limits: AreaReadLimits,
    ) -> Result<AreaPage, StoreError> {
        let filters = AreaFilters::of(area);
        let namespace = namespace_id.as_bytes().to_vec();

        let Some(bound) = ResumeBound::of(area.subspace(), resume_after) else {
            // Resuming past the end of the scanned subspace leaves nothing to read.
            return Ok(AreaPage {
                entries: Vec::new(),
                next: None,
            });
        };

        // One row beyond the page, so `next` reports drained-ness exactly instead of guessing
        // that a full page implies more.
        let max_entries = limits.max_entries.get();
        let limit = max_entries.saturating_add(1) as i64;

        let rows = match &filters.subspace {
            Some(subspace) => sqlx::query(queries::READ_AREA_IN_SUBSPACE)
                .bind(self.scope_id)
                .bind(&namespace)
                .bind(subspace)
                .bind(bound.path.as_ref())
                .bind(filters.from_path.as_ref())
                .bind(filters.until_path.as_ref())
                .bind(u64_bytes(filters.from_time))
                .bind(filters.until_time.map(u64_bytes))
                .bind(limit)
                .fetch_all(&self.ctx.read_pool)
                .await,
            None => sqlx::query(queries::READ_AREA_ANY_SUBSPACE)
                .bind(self.scope_id)
                .bind(&namespace)
                .bind(bound.subspace.as_ref())
                .bind(bound.path.as_ref())
                .bind(filters.from_path.as_ref())
                .bind(filters.until_path.as_ref())
                .bind(u64_bytes(filters.from_time))
                .bind(filters.until_time.map(u64_bytes))
                .bind(limit)
                .fetch_all(&self.ctx.read_pool)
                .await,
        }
        .map_err(backend_error)?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let blob: Vec<u8> = row.try_get("entry").map_err(backend_error)?;
            entries.push(decode_authorised_entry(&blob).await);
        }

        let next = if entries.len() > max_entries {
            entries.truncate(max_entries);
            let last = entries
                .last()
                .expect("a page that exceeded its limit contains at least one entry");
            Some((last.subspace_id().clone(), last.path().clone()))
        } else {
            None
        };

        Ok(AreaPage { entries, next })
    }

    async fn forget_entry(
        &self,
        namespace_id: &NamespaceId,
        subspace_id: &SubspaceId,
        path: &Path,
    ) -> Result<bool, StoreError> {
        let scope_id = self.scope_id;
        let namespace = namespace_id.as_bytes().to_vec();
        let subspace = subspace_id.as_bytes().to_vec();
        let path = encode_path(path);

        self.ctx
            .with_write_tx(|mut tx| {
                Box::pin(async move {
                    let result = sqlx::query(queries::DELETE_ENTRY)
                        .bind(scope_id)
                        .bind(namespace)
                        .bind(subspace)
                        .bind(path)
                        .execute(&mut *tx)
                        .await?;
                    Ok((result.rows_affected() > 0, tx))
                })
            })
            .await
            .map_err(backend_error)
    }

    async fn forget_area(&self, namespace_id: &NamespaceId, area: &Area) -> Result<(), StoreError> {
        let filters = AreaFilters::of(area);
        let scope_id = self.scope_id;
        let namespace = namespace_id.as_bytes().to_vec();

        self.ctx
            .with_write_tx(|mut tx| {
                Box::pin(async move {
                    match filters.subspace {
                        Some(subspace) => sqlx::query(queries::DELETE_AREA_IN_SUBSPACE)
                            .bind(scope_id)
                            .bind(namespace)
                            .bind(subspace)
                            .bind(filters.from_path)
                            .bind(filters.until_path)
                            .bind(u64_bytes(filters.from_time))
                            .bind(filters.until_time.map(u64_bytes))
                            .execute(&mut *tx)
                            .await?,
                        None => sqlx::query(queries::DELETE_AREA_ANY_SUBSPACE)
                            .bind(scope_id)
                            .bind(namespace)
                            .bind(filters.from_path)
                            .bind(filters.until_path)
                            .bind(u64_bytes(filters.from_time))
                            .bind(filters.until_time.map(u64_bytes))
                            .execute(&mut *tx)
                            .await?,
                    };

                    Ok(((), tx))
                })
            })
            .await
            .map_err(backend_error)
    }

    async fn forget_namespace(&self, namespace_id: &NamespaceId) -> Result<(), StoreError> {
        let scope_id = self.scope_id;
        let namespace = namespace_id.as_bytes().to_vec();

        self.ctx
            .with_write_tx(|mut tx| {
                Box::pin(async move {
                    sqlx::query(queries::DELETE_NAMESPACE)
                        .bind(scope_id)
                        .bind(namespace)
                        .execute(&mut *tx)
                        .await?;
                    Ok(((), tx))
                })
            })
            .await
            .map_err(backend_error)
    }

    async fn flush(&self) -> Result<(), StoreError> {
        sqlx::query(queries::WAL_CHECKPOINT)
            .execute(&self.ctx.write_pool)
            .await
            .map_err(backend_error)?;
        Ok(())
    }
}

/// Runs the whole insert in the caller's transaction: reject an entry an existing one prunes,
/// delete the entries this one prunes, then store the entry and its payload.
///
/// A re-insert of the entry that is already stored prunes nothing. That is not just an
/// optimisation: it is observable, because it leaves the entries this entry prunes in place.
/// `willow25`'s in-memory store returns early on the same case, and the `store_pruning` corpus
/// pins it.
async fn stage_entry(
    tx: &mut Transaction<'_, Sqlite>,
    scope_id: i64,
    entry: &AuthorisedEntry,
    payload: Option<&[u8]>,
) -> Result<InsertOutcome, sqlx::Error> {
    let namespace = entry.namespace_id().as_bytes().to_vec();
    let subspace = entry.subspace_id().as_bytes().to_vec();
    let path = encode_path(entry.path());
    let recency = Recency::of(entry);
    // An existing entry prunes this one when its path is a prefix of this path and it is
    // strictly newer. Those candidates are the encodings of this path's prefixes, which is not
    // a contiguous range, so it costs one point lookup per prefix: the component count plus one,
    // each an index seek on the primary key inside one transaction. A single statement would
    // need either assembled SQL or byte-level work to recover the prefixes in the query.
    for prefix in entry.path().all_prefixes() {
        let prefix = encode_path(&prefix);
        let stored = select_recency(tx, scope_id, &namespace, &subspace, &prefix).await?;
        if stored.is_some_and(|stored| stored.is_newer_than(&recency)) {
            return Ok(InsertOutcome::Outdated);
        }
    }

    let encoded = encode_authorised_entry(entry).await;

    // Comparing the stored bytes against a fresh encoding is exact rather than approximate: the
    // store only ever writes `encode_authorised_entry` output, so equal bytes are an equal
    // entry. Reading bytes rather than decoding the stored entry also keeps this off the
    // expensive side of the insert path.
    let stored_entry: Option<Vec<u8>> = sqlx::query(queries::SELECT_ENTRY)
        .bind(scope_id)
        .bind(&namespace)
        .bind(&subspace)
        .bind(&path)
        .fetch_optional(&mut **tx)
        .await?
        .map(|row| row.try_get("entry"))
        .transpose()?;
    let already_stored = stored_entry.as_deref() == Some(encoded.as_slice());

    let pruned = if already_stored {
        // A re-insert of the stored entry prunes nothing, not even strictly older descendants.
        0
    } else {
        let range = prefix_range(entry.path());
        sqlx::query(queries::DELETE_PRUNED_ENTRIES)
            .bind(scope_id)
            .bind(&namespace)
            .bind(&subspace)
            .bind(&range.lo)
            .bind(range.hi.as_ref())
            .bind(u64_bytes(recency.timestamp))
            .bind(&recency.digest)
            .bind(u64_bytes(recency.length))
            .execute(&mut **tx)
            .await?
            .rows_affected() as usize
    };

    sqlx::query(queries::UPSERT_ENTRY)
        .bind(scope_id)
        .bind(&namespace)
        .bind(&subspace)
        .bind(&path)
        .bind(u64_bytes(recency.timestamp))
        .bind(&recency.digest)
        .bind(u64_bytes(recency.length))
        .bind(encoded)
        .execute(&mut **tx)
        .await?;

    if let Some(payload) = payload {
        sqlx::query(queries::UPSERT_PAYLOAD)
            .bind(scope_id)
            .bind(&namespace)
            .bind(&subspace)
            .bind(&path)
            .bind(payload)
            .execute(&mut **tx)
            .await?;
    }

    Ok(InsertOutcome::Inserted { pruned })
}

/// The recency triple of the entry at one key, or `None` when no entry is there.
async fn select_recency(
    tx: &mut Transaction<'_, Sqlite>,
    scope_id: i64,
    namespace: &[u8],
    subspace: &[u8],
    path: &[u8],
) -> Result<Option<Recency>, sqlx::Error> {
    let row = sqlx::query(queries::SELECT_RECENCY_AT_PATH)
        .bind(scope_id)
        .bind(namespace)
        .bind(subspace)
        .bind(path)
        .fetch_optional(&mut **tx)
        .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(Recency {
        timestamp: u64_from_bytes(&row.try_get::<Vec<u8>, _>("timestamp")?),
        digest: row.try_get("payload_digest")?,
        length: u64_from_bytes(&row.try_get::<Vec<u8>, _>("payload_length")?),
    }))
}

/// The `(timestamp, payload_digest, payload_length)` triple `willow25` orders entries by.
#[derive(Debug, Clone)]
struct Recency {
    timestamp: u64,
    digest: Vec<u8>,
    length: u64,
}

impl Recency {
    fn of(entry: &AuthorisedEntry) -> Self {
        Self {
            timestamp: u64::from(entry.timestamp()),
            digest: entry.payload_digest().as_bytes().to_vec(),
            length: entry.payload_length(),
        }
    }

    /// Whether this is strictly newer, matching `EntrylikeExt::is_newer_than`.
    fn is_newer_than(&self, other: &Self) -> bool {
        (self.timestamp, self.digest.as_slice(), self.length)
            > (other.timestamp, other.digest.as_slice(), other.length)
    }
}

/// The parts of an [`Area`] that SQL can evaluate directly.
struct AreaFilters {
    subspace: Option<Vec<u8>>,
    /// `None` when the area's path is the empty path, which is a prefix of every path and so
    /// imposes no lower bound.
    from_path: Option<Vec<u8>>,
    /// `None` for the same reason, on the upper side.
    until_path: Option<Vec<u8>>,
    from_time: u64,
    until_time: Option<u64>,
}

impl AreaFilters {
    fn of(area: &Area) -> Self {
        let prefix = prefix_range(area.path());
        let times = area.times();

        Self {
            subspace: area.subspace().map(|id| id.as_bytes().to_vec()),
            from_path: (!prefix.lo.is_empty()).then_some(prefix.lo),
            until_path: prefix.hi,
            from_time: u64::from(*times.start()),
            until_time: times.end().map(|end| u64::from(*end)),
        }
    }
}

/// Where an ordered scan starts, as the exclusive key it resumes after.
struct ResumeBound {
    /// `None` when the scan spans every subspace of the namespace.
    subspace: Option<Vec<u8>>,
    /// `None` when the scan starts at the beginning of its subspace.
    path: Option<Vec<u8>>,
}

impl ResumeBound {
    /// Resolves `resume_after` against the scan's own start, mirroring the in-memory store:
    /// the bound is `max((subspace, empty path), resume key)`, exclusive, and resuming past the
    /// scanned subspace leaves nothing.
    ///
    /// `None` means the scan can match nothing at all.
    fn of(area_subspace: Option<&SubspaceId>, resume_after: Option<&EntryKey>) -> Option<Self> {
        let Some((resume_subspace, resume_path)) = resume_after else {
            return Some(Self {
                subspace: None,
                path: None,
            });
        };

        let resume_subspace = resume_subspace.as_bytes().to_vec();
        let resume_path = encode_path(resume_path);

        match area_subspace {
            Some(area_subspace) => {
                let area_subspace = area_subspace.as_bytes().to_vec();
                match resume_subspace.cmp(&area_subspace) {
                    std::cmp::Ordering::Greater => None,
                    // The scan's own start dominates, which is the empty path.
                    std::cmp::Ordering::Less => Some(Self {
                        subspace: None,
                        path: Some(encode_path(&Path::new())),
                    }),
                    std::cmp::Ordering::Equal => Some(Self {
                        subspace: None,
                        path: Some(resume_path),
                    }),
                }
            }
            None => Some(Self {
                subspace: Some(resume_subspace),
                path: Some(resume_path),
            }),
        }
    }
}

/// Big-endian `u64` bytes, so SQLite's memcmp ordering matches unsigned ordering.
fn u64_bytes(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn u64_from_bytes(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(
        bytes
            .try_into()
            .expect("a stored u64 column always holds eight bytes"),
    )
}

/// Encodes an entry with `willow25`'s own `encode_authorised_entry`, so the stored bytes are the
/// specification's, not an invented representation.
async fn encode_authorised_entry(entry: &AuthorisedEntry) -> Vec<u8> {
    let mut consumer = Vec::<u8>::new().into_consumer();
    entry
        .encode(&mut consumer)
        .await
        .expect("a `Vec` consumer cannot fail");
    Vec::from(consumer)
}

/// Decodes bytes this store wrote.
///
/// The bytes are always its own encoding of an entry it validated, so a failure here is
/// corruption or a codec bug, not a boundary condition: it crashes rather than returning an
/// error callers could not act on.
///
/// The producer is `clone_from_slice` rather than the `Vec` one: the `Vec` producer holds raw
/// pointers and is therefore not `Send`, which would make every future in this module not `Send`
/// and so unusable through `Arc<dyn WillowStore>` from a multi-threaded task.
async fn decode_authorised_entry(blob: &[u8]) -> AuthorisedEntry {
    let mut producer = ufotofu::producer::clone_from_slice(blob);
    AuthorisedEntry::decode(&mut producer)
        .await
        .expect("the store only ever stores entries it encoded itself")
}

fn backend_error(error: sqlx::Error) -> StoreError {
    StoreError::Backend(Box::new(error))
}
