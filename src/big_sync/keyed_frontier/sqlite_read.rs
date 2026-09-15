//! Shared SQLite keyed-frontier replay and live handoff.

use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierRead, FrontierReadLimits, FrontierRevision, KeyedFrontierError,
    KeyedFrontierReader, KeyedFrontierResult,
};
use sqlx::SqlitePool;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Notify;
use utils_rs::prelude::async_trait;

pub(crate) type SqliteReadError = Box<dyn std::error::Error + Send + Sync>;

fn backend_error(error: SqliteReadError) -> KeyedFrontierError {
    KeyedFrontierError::Backend(error)
}
pub(crate) trait SqliteReadSource: Clone + Send + Sync + 'static {
    type Selector: Clone + Send + Sync + 'static;
    type Row: Send + 'static;
    type Key: Send + Sync + 'static;
    type Value: Send + Sync + 'static;

    fn read_pool(&self) -> &SqlitePool;
    fn changed(&self) -> &Notify;

    /// Return the source cursor represented by a selector's lower bound.
    /// Selectors with independent per-key bounds retain the shared cursor at
    /// zero; the collapsed `All` selector can initialize directly at its
    /// requested bound and avoid an empty replay-progress batch.
    fn initial_after(&self, _selector: &Self::Selector) -> FrontierRevision {
        0
    }

    #[expect(clippy::type_complexity)]
    fn fetch_rows<'a>(
        &'a self,
        selector: &'a Self::Selector,
        after: FrontierRevision,
        through: FrontierRevision,
        exact_revision: Option<FrontierRevision>,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Self::Row>, SqliteReadError>> + Send + 'a>>;

    fn row_revision(&self, row: &Self::Row) -> FrontierRevision;

    #[expect(clippy::type_complexity)]
    fn decode_row(
        &self,
        row: Self::Row,
    ) -> Result<Option<FrontierEntry<Self::Key, Self::Value>>, SqliteReadError>;

    /// Reads the committed source revision after the write transaction has committed.
    fn committed_revision(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<FrontierRevision, SqliteReadError>> + Send + '_>>;
}

struct SqliteRows<R> {
    rows: Vec<R>,
    /// The source cursor represented by this read, independent of how many
    /// rows survive typed decoding.
    through: FrontierRevision,
}

struct SqliteReader<S: SqliteReadSource> {
    source: S,
    selector: S::Selector,
    initial_through: Option<FrontierRevision>,
    after: FrontierRevision,
}

async fn query_rows<S>(
    source: &S,
    selector: &S::Selector,
    after: FrontierRevision,
    through: FrontierRevision,
    exact_revision: Option<FrontierRevision>,
    limit: Option<usize>,
) -> Result<Vec<S::Row>, SqliteReadError>
where
    S: SqliteReadSource,
{
    source
        .fetch_rows(selector, after, through, exact_revision, limit)
        .await
}

async fn read_page<S>(
    source: &S,
    selector: &S::Selector,
    after: FrontierRevision,
    through: FrontierRevision,
    max_entries: usize,
) -> Result<SqliteRows<S::Row>, SqliteReadError>
where
    S: SqliteReadSource,
{
    let mut rows = query_rows(source, selector, after, through, None, Some(max_entries)).await?;
    if rows.len() < max_entries {
        return Ok(SqliteRows { rows, through });
    }

    // A page may stop in the middle of one atomic revision.  Re-read that
    // revision and include it in full; the limit is soft by contract.
    let cutoff = source.row_revision(rows.last().expect("limited query returned a row"));
    let boundary_rows = query_rows(source, selector, after, through, Some(cutoff), None).await?;
    rows.retain(|row| source.row_revision(row) < cutoff);
    rows.extend(boundary_rows);
    Ok(SqliteRows {
        rows,
        through: cutoff,
    })
}

/// Opens a reader after capturing exactly one committed replay boundary.
pub(crate) async fn open_sqlite_reader<S>(
    source: S,
    selector: S::Selector,
) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<S::Key, S::Value>>>
where
    S: SqliteReadSource,
{
    let initial_through = source.committed_revision().await.map_err(backend_error)?;
    let after = source.initial_after(&selector);
    Ok(Box::new(SqliteReader {
        source,
        selector,
        initial_through: Some(initial_through),
        after,
    }))
}

#[async_trait]
impl<S> KeyedFrontierReader<S::Key, S::Value> for SqliteReader<S>
where
    S: SqliteReadSource,
{
    async fn next(
        &mut self,
        limits: FrontierReadLimits,
    ) -> KeyedFrontierResult<FrontierRead<S::Key, S::Value>> {
        loop {
            let Some(phase_through) = self.initial_through else {
                // Registration precedes the confirming cursor query.  The
                // enabled Notified future cannot miss a commit between these
                // two operations.
                let notified = self.source.changed().notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let current = self
                    .source
                    .committed_revision()
                    .await
                    .map_err(backend_error)?;
                let previous_after = self.after;
                let page = read_page(
                    &self.source,
                    &self.selector,
                    self.after,
                    current,
                    limits.max_entries.get(),
                )
                .await
                .map_err(backend_error)?;
                self.after = page.through;
                if page.through > previous_after {
                    return self.decode_page(page);
                }
                notified.await;
                continue;
            };

            let previous_after = self.after;
            let page = read_page(
                &self.source,
                &self.selector,
                self.after,
                phase_through,
                limits.max_entries.get(),
            )
            .await
            .map_err(backend_error)?;
            self.after = page.through;
            if page.through > previous_after {
                return self.decode_page(page);
            }
            self.initial_through = None;
            return Ok(FrontierRead::ReplayComplete {
                through: phase_through,
            });
        }
    }
}

impl<S> SqliteReader<S>
where
    S: SqliteReadSource,
{
    fn decode_page(
        &self,
        page: SqliteRows<S::Row>,
    ) -> KeyedFrontierResult<FrontierRead<S::Key, S::Value>> {
        let mut entries = Vec::with_capacity(page.rows.len());
        for row in page.rows {
            if let Some(entry) = self.source.decode_row(row).map_err(backend_error)? {
                entries.push(entry);
            }
        }
        Ok(FrontierRead::Entries {
            entries,
            through: page.through,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_page_cutoff_is_a_complete_revision() {
        let mut rows = vec![1_u64, 1, 2, 2, 2, 3];
        let max = 3;
        let cutoff = rows[max - 1];
        rows.retain(|revision| *revision < cutoff);
        rows.extend([2, 2, 2]);
        assert_eq!(rows, vec![1, 1, 2, 2, 2]);
    }
}
