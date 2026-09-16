//! The sqlite store: one database per checkout (ADR 010 §3.1).
//!
//! Two tables and nothing else:
//!
//! - `pauperfuse_rep(name, generation, updated_at)` — one row per rep;
//! - `pauperfuse_entry(rep, path, …)` — one row per recorded path, keyed
//!   `(rep, path)` and stored `WITHOUT ROWID`, so the primary key *is* the
//!   walk order and a page of a scan is a range scan.
//!
//! Deliberately absent: content bytes (rows carry provenance) and any history
//! (ADR 010 §2.2, §5.1). A rep update is one transaction; a crash between two
//! updates loses nothing but the cache, because the next change report rebuilds
//! it.
//!
//! The schema is in `sql/migrations` and every statement this store runs is in
//! `sql/queries`, one file each, pulled in by [`queries`] — so the schema and
//! the statements that serve it can be read together, and nothing is assembled
//! at runtime.

use std::collections::BTreeSet;

use super::{VtreeStore, implied_parents};
use crate::backend::BackendId;
use crate::codec::{StoredEntry, decode_path, encode_path};
use crate::delta::Delta;
use crate::entry::Entry;
use crate::interlude::*;
use crate::path::RelPath;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use sqlx_utils_rs::SqlCtx;

mod queries;

#[cfg(test)]
mod test;

/// Installed under a private table name so this schema can share a database
/// with another module's without either one claiming the default
/// `_sqlx_migrations`.
static MIGRATOR: std::sync::LazyLock<sqlx::migrate::Migrator> = std::sync::LazyLock::new(|| {
    let mut migrator = sqlx::migrate!("./sql/migrations");
    migrator.dangerous_set_table_name("_pauperfuse_migrations");
    migrator
});

/// A sqlite backed [`VtreeStore`].
#[derive(Clone, Debug)]
pub struct SqliteVtreeStore {
    sql: SqlCtx,
}

impl SqliteVtreeStore {
    /// Open (creating if needed) the store at `path`.
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| Error::fs(FsOp::CreateDir, parent, source))?;
        }
        Self::open_url(&format!("sqlite://{}", path.display())).await
    }

    /// Open a store from a sqlite url.
    pub async fn open_url(url: &str) -> Result<Self> {
        let sql = SqlCtx::url(url)
            .await
            .map_err(|err| Error::message(format!("{err:?}")))?;
        let store = Self { sql };
        store.migrate().await?;
        Ok(store)
    }

    /// Open a store in a temporary directory that is removed on drop.
    pub async fn ephemeral() -> Result<Self> {
        let sql = SqlCtx::ephemeral_file()
            .await
            .map_err(|err| Error::message(format!("{err:?}")))?;
        let store = Self { sql };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        MIGRATOR
            .run(&self.sql.write_pool)
            .await
            .map_err(Error::from)?;
        Ok(())
    }
}

/// Render a `path` column for a message, decoded or not.
///
/// A row whose path does not decode is exactly the row worth naming, so the
/// rendering works from the column rather than from the decoded path — which is
/// what keeps the name in an error identical to what `RelPath` would print for
/// the same row when it does decode.
fn render_path(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        // The root: no components to join, and an empty name would read as a bug.
        return "/".to_string();
    }
    bytes
        .split(|byte| *byte == 0)
        .map(|component| String::from_utf8_lossy(component).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Rebuild a row's path and entry.
fn decode_row(row: &SqliteRow) -> Result<(RelPath, Entry)> {
    let path_raw: Vec<u8> = row.try_get("path")?;
    let stored = StoredEntry {
        kind: row.try_get("kind")?,
        origin: row.try_get("origin")?,
        content: row.try_get("content")?,
        target: row.try_get("target")?,
        avail: row.try_get("avail")?,
        stat: row.try_get("stat")?,
        claim: row.try_get("claim")?,
    };
    let path =
        decode_path(&path_raw).map_err(|reason| Error::stored(render_path(&path_raw), reason))?;
    let entry = stored
        .into_entry()
        .map_err(|reason| Error::stored(render_path(&path_raw), reason))?;
    Ok((path, entry))
}

#[async_trait]
impl VtreeStore for SqliteVtreeStore {
    async fn apply(&self, rep: &BackendId, deltas: &[Delta]) -> Result<u64> {
        if deltas.is_empty() {
            return Ok(self.generation(rep).await?.unwrap_or(0));
        }

        // Encode outside the transaction: the only errors left inside are
        // sqlite's own.
        let mut upserts = Vec::new();
        let mut removals = Vec::new();
        let mut implied = BTreeSet::new();
        for delta in deltas {
            match delta {
                Delta::Removed { path, .. } => removals.push(encode_path(path)),
                Delta::Added { path, entry }
                | Delta::Touched { path, entry }
                | Delta::Changed {
                    path, to: entry, ..
                } => {
                    for parent in implied_parents(path) {
                        implied.insert(encode_path(&parent));
                    }
                    upserts.push((encode_path(path), StoredEntry::of(entry)));
                }
            }
        }

        let rep_name = rep.as_str().to_string();
        let updated_at = jiff::Timestamp::now().as_millisecond();

        // One transaction, one rep update (ADR 010 §2.2).
        let generation = self
            .sql
            .with_write_tx(|tx| {
                Box::pin(async move {
                    let mut tx = tx;
                    // The rep row comes first: entries reference it.
                    sqlx::query(queries::INSERT_REP)
                        .bind(&rep_name)
                        .bind(updated_at)
                        .execute(&mut *tx)
                        .await?;
                    for path in &implied {
                        sqlx::query(queries::INSERT_IMPLIED_DIR)
                            .bind(&rep_name)
                            .bind(path.as_slice())
                            .execute(&mut *tx)
                            .await?;
                    }
                    for (path, entry) in &upserts {
                        sqlx::query(queries::UPSERT_ENTRY)
                            .bind(&rep_name)
                            .bind(path.as_slice())
                            .bind(entry.kind)
                            .bind(entry.origin.as_deref())
                            .bind(entry.content.as_deref())
                            .bind(entry.target.as_deref())
                            .bind(entry.avail)
                            .bind(entry.stat.as_deref())
                            .bind(entry.claim.as_deref())
                            .execute(&mut *tx)
                            .await?;
                    }
                    for path in &removals {
                        sqlx::query(queries::DELETE_ENTRY)
                            .bind(&rep_name)
                            .bind(path.as_slice())
                            .execute(&mut *tx)
                            .await?;
                    }
                    sqlx::query(queries::BUMP_REP)
                        .bind(&rep_name)
                        .bind(updated_at)
                        .execute(&mut *tx)
                        .await?;
                    let row = sqlx::query(queries::SELECT_GENERATION)
                        .bind(&rep_name)
                        .fetch_one(&mut *tx)
                        .await?;
                    let generation: i64 = row.try_get("generation")?;
                    Ok((generation.max(0) as u64, tx))
                })
            })
            .await?;

        Ok(generation)
    }

    async fn entry(&self, rep: &BackendId, path: &RelPath) -> Result<Option<Entry>> {
        let row = sqlx::query(queries::SELECT_ENTRY)
            .bind(rep.as_str())
            .bind(encode_path(path))
            .fetch_optional(&self.sql.read_pool)
            .await?;
        match row {
            Some(row) => Ok(Some(decode_row(&row)?.1)),
            None => Ok(None),
        }
    }

    async fn scan_page(
        &self,
        rep: &BackendId,
        from: Option<&RelPath>,
        limit: usize,
    ) -> Result<Vec<(RelPath, Entry)>> {
        let rows = match from {
            Some(from) => {
                sqlx::query(queries::SELECT_PAGE_AFTER)
                    .bind(rep.as_str())
                    .bind(encode_path(from))
                    .bind(limit as i64)
                    .fetch_all(&self.sql.read_pool)
                    .await?
            }
            None => {
                sqlx::query(queries::SELECT_PAGE_FROM_START)
                    .bind(rep.as_str())
                    .bind(limit as i64)
                    .fetch_all(&self.sql.read_pool)
                    .await?
            }
        };
        rows.iter().map(decode_row).collect()
    }

    async fn generation(&self, rep: &BackendId) -> Result<Option<u64>> {
        let row = sqlx::query(queries::SELECT_GENERATION)
            .bind(rep.as_str())
            .fetch_optional(&self.sql.read_pool)
            .await?;
        match row {
            Some(row) => {
                let generation: i64 = row.try_get("generation")?;
                Ok(Some(generation.max(0) as u64))
            }
            None => Ok(None),
        }
    }

    async fn reps(&self) -> Result<Vec<BackendId>> {
        let rows = sqlx::query(queries::LIST_REPS)
            .fetch_all(&self.sql.read_pool)
            .await?;
        let mut reps = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String = row.try_get("name")?;
            reps.push(BackendId::new(name));
        }
        Ok(reps)
    }

    async fn drop_rep(&self, rep: &BackendId) -> Result<bool> {
        // The rep's rows go with it: `ON DELETE CASCADE` on the entry table is
        // the whole story, and `dropping_a_rep_drops_its_entries` pins it.
        let removed = self
            .sql
            .with_write_tx(|mut tx| {
                let rep_name = rep.as_str().to_string();
                Box::pin(async move {
                    let result = sqlx::query(queries::DELETE_REP)
                        .bind(&rep_name)
                        .execute(&mut *tx)
                        .await?;
                    Ok((result.rows_affected() > 0, tx))
                })
            })
            .await?;
        Ok(removed)
    }
}
