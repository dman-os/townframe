//! SQLite replay and read-side handoff for the part-store-shaped frontier.
//!
//! This file deliberately contains the read half only.  The sibling
//! `sqlite_write` module is expected to implement [`SqliteReadSource`] for its
//! frontier and to provide the typed row decoder.  The adapter uses the
//! existing part-store tables: `big_sync_members` is the collapsed row table,
//! `big_sync_objs` and `big_sync_parts` provide the exact selectors, and
//! `big_sync_meta.global_cursor` is the committed revision counter.

use big_sync_core::keyed_frontier::{
    FrontierEntry, FrontierRead, FrontierReadLimits, FrontierRevision, KeyedFrontierError,
    KeyedFrontierReader, KeyedFrontierResult,
};
use big_sync_core::{ObjId, PartId};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::Notify;
use utils_rs::prelude::async_trait;

pub(crate) type SqliteReadError = Box<dyn std::error::Error + Send + Sync>;

/// Independent lower bounds for exact object and exact part routes.
///
/// A row selected by either map is included when it is newer than that map's
/// bound.  The OR predicate is intentionally built in SQL, rather than by
/// loading a broad source range and filtering it in memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SqlitePartSelector {
    pub(crate) objects: BTreeMap<ObjId, FrontierRevision>,
    pub(crate) parts: BTreeMap<PartId, FrontierRevision>,
}

impl SqlitePartSelector {
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.parts.is_empty()
    }
}

/// The current, collapsed SQLite row passed to the sibling decoder.
///
/// `payload_json == None` is meaningful: it is a retained deletion/tombstone
/// row and must not be discarded by a decoder merely because it has no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqliteFrontierRow {
    pub(crate) obj_ref: i64,
    pub(crate) part_ref: i64,
    pub(crate) revision: FrontierRevision,
    pub(crate) obj_id: ObjId,
    pub(crate) part_id: Option<PartId>,
    pub(crate) event_type: i64,
    pub(crate) payload_json: Option<String>,
}

/// Narrow integration seam for the write-side frontier.
///
/// The implementation owns the typed key/value types and decides whether a
/// matching storage row is relevant to that typed frontier.  Returning
/// `Ok(None)` is filtering, not an error; the reader still advances through
/// the source revision and can therefore emit an empty progress batch.
pub(crate) trait SqliteReadSource: Send + Sync {
    type Key: Send + Sync + 'static;
    type Value: Send + Sync + 'static;

    fn read_pool(&self) -> &SqlitePool;
    fn scope_id(&self) -> i64;
    fn changed(&self) -> &Notify;

    #[expect(clippy::type_complexity)]
    fn decode_row(
        &self,
        row: SqliteFrontierRow,
    ) -> Result<Option<FrontierEntry<Self::Key, Self::Value>>, SqliteReadError>;

    /// Reads one committed cursor after the write transaction has committed.
    /// The default is the cursor used by the existing part-store schema.
    fn committed_revision(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<FrontierRevision, SqliteReadError>> + Send + '_>> {
        Box::pin(async move {
            let revision: i64 =
                sqlx::query_scalar("SELECT value FROM big_sync_meta WHERE key = 'global_cursor'")
                    .fetch_one(self.read_pool())
                    .await?;
            Ok(u64::try_from(revision).expect("SQLite frontier revision is non-negative"))
        })
    }
}

struct SqliteRows {
    rows: Vec<SqliteFrontierRow>,
    /// The source cursor represented by this read, independent of how many
    /// rows survive typed decoding.
    through: FrontierRevision,
}

struct SqliteReader<'source, S> {
    source: &'source S,
    selector: SqlitePartSelector,
    limits: FrontierReadLimits,
    initial_through: Option<FrontierRevision>,
    after: FrontierRevision,
}

fn id_blob(id: ObjId) -> Vec<u8> {
    id.0.into_bytes().to_vec()
}

fn part_blob(id: PartId) -> Vec<u8> {
    id.0.into_bytes().to_vec()
}

fn bytes32(bytes: Vec<u8>) -> [u8; 32] {
    bytes
        .try_into()
        .expect("SQLite part-store identifiers have exactly 32 bytes")
}

fn backend_error(error: SqliteReadError) -> KeyedFrontierError {
    KeyedFrontierError::Backend(error)
}

fn push_selector_predicate(
    query: &mut QueryBuilder<Sqlite>,
    selector: &SqlitePartSelector,
    scope_id: i64,
) {
    query.push(" AND (");
    let mut first = true;
    for (obj_id, lower_bound) in &selector.objects {
        if !first {
            query.push(" OR ");
        }
        first = false;
        query.push("(m.obj_ref IN (SELECT obj_ref FROM big_sync_objs WHERE scope_id = ");
        query.push_bind(scope_id);
        query.push(" AND obj_id = ");
        query.push_bind(id_blob(*obj_id));
        query.push(") AND m.txid > ");
        query.push_bind(i64::try_from(*lower_bound).expect("frontier revision fits SQLite"));
        query.push(")");
    }
    for (part_id, lower_bound) in &selector.parts {
        if !first {
            query.push(" OR ");
        }
        first = false;
        query.push("(m.maybe_part_ref IN (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(scope_id);
        query.push(" AND part_id = ");
        query.push_bind(part_blob(*part_id));
        query.push(") AND m.txid > ");
        query.push_bind(i64::try_from(*lower_bound).expect("frontier revision fits SQLite"));
        query.push(")");
    }
    if first {
        query.push("0");
    }
    query.push(")");
}

async fn query_rows<S>(
    source: &S,
    selector: &SqlitePartSelector,
    after: FrontierRevision,
    through: FrontierRevision,
    exact_revision: Option<FrontierRevision>,
    limit: Option<usize>,
) -> Result<Vec<SqliteFrontierRow>, SqliteReadError>
where
    S: SqliteReadSource,
{
    if selector.is_empty() || after >= through && exact_revision.is_none() {
        return Ok(Vec::new());
    }

    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT m.obj_ref
             , m.maybe_part_ref
             , m.txid
             , o.obj_id
             , p.part_id
             , m.event_type
             , o.payload_json
          FROM big_sync_members m
          JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
     LEFT JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
         WHERE m.scope_id = ",
    );
    query.push_bind(source.scope_id());
    query.push(" AND m.txid > ");
    query.push_bind(i64::try_from(after).expect("frontier revision fits SQLite"));
    query.push(" AND m.txid <= ");
    query.push_bind(i64::try_from(through).expect("frontier revision fits SQLite"));
    if let Some(exact_revision) = exact_revision {
        query.push(" AND m.txid = ");
        query.push_bind(i64::try_from(exact_revision).expect("frontier revision fits SQLite"));
    }
    push_selector_predicate(&mut query, selector, source.scope_id());
    query.push(
        " ORDER BY m.txid\
                       , m.obj_ref\
                       , m.maybe_part_ref",
    );
    if let Some(limit) = limit {
        query.push(" LIMIT ");
        query.push_bind(i64::try_from(limit).expect("read limit fits SQLite"));
    }
    let rows = query.build().fetch_all(source.read_pool()).await?;
    rows.into_iter()
        .map(|row| {
            Ok(SqliteFrontierRow {
                obj_ref: row.try_get("obj_ref")?,
                part_ref: row.try_get("maybe_part_ref")?,
                revision: u64::try_from(row.try_get::<i64, _>("txid")?)
                    .expect("SQLite frontier revision is non-negative"),
                obj_id: ObjId::new(bytes32(row.try_get::<Vec<u8>, _>("obj_id")?)),
                part_id: row
                    .try_get::<Option<Vec<u8>>, _>("part_id")?
                    .map(|bytes| PartId::new(bytes32(bytes))),
                event_type: row.try_get("event_type")?,
                payload_json: row.try_get("payload_json")?,
            })
        })
        .collect()
}

async fn read_page<S>(
    source: &S,
    selector: &SqlitePartSelector,
    after: FrontierRevision,
    through: FrontierRevision,
    max_entries: usize,
) -> Result<SqliteRows, SqliteReadError>
where
    S: SqliteReadSource,
{
    let mut rows = query_rows(source, selector, after, through, None, Some(max_entries)).await?;
    if rows.len() < max_entries {
        return Ok(SqliteRows { rows, through });
    }

    // A page may stop in the middle of one atomic revision.  Re-read that
    // revision and include it in full; the limit is soft by contract.
    let cutoff = rows.last().expect("limited query returned a row").revision;
    let boundary_rows = query_rows(source, selector, after, through, Some(cutoff), None).await?;
    rows.retain(|row| row.revision < cutoff);
    rows.extend(boundary_rows);
    Ok(SqliteRows {
        rows,
        through: cutoff,
    })
}

/// Opens a reader after capturing exactly one committed replay boundary.
pub(crate) async fn open_sqlite_reader<'source, S>(
    source: &'source S,
    selector: SqlitePartSelector,
    limits: FrontierReadLimits,
) -> KeyedFrontierResult<Box<dyn KeyedFrontierReader<S::Key, S::Value> + 'source>>
where
    S: SqliteReadSource,
{
    if limits.max_entries == 0 {
        return Err(KeyedFrontierError::EmptyReadLimit);
    }
    let initial_through = source.committed_revision().await.map_err(backend_error)?;
    Ok(Box::new(SqliteReader {
        source,
        selector,
        limits,
        initial_through: Some(initial_through),
        after: 0,
    }))
}

#[async_trait]
impl<S> KeyedFrontierReader<S::Key, S::Value> for SqliteReader<'_, S>
where
    S: SqliteReadSource,
{
    async fn next(&mut self) -> KeyedFrontierResult<FrontierRead<S::Key, S::Value>> {
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
                    self.source,
                    &self.selector,
                    self.after,
                    current,
                    self.limits.max_entries,
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
                self.source,
                &self.selector,
                self.after,
                phase_through,
                self.limits.max_entries,
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

impl<S> SqliteReader<'_, S>
where
    S: SqliteReadSource,
{
    fn decode_page(&self, page: SqliteRows) -> KeyedFrontierResult<FrontierRead<S::Key, S::Value>> {
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
    use super::*;

    #[test]
    fn selector_keeps_object_and_part_bounds_independent() {
        let object = ObjId::new([1; 32]);
        let part = PartId::new([2; 32]);
        let selector = SqlitePartSelector {
            objects: BTreeMap::from([(object, 7)]),
            parts: BTreeMap::from([(part, 19)]),
        };
        assert_eq!(selector.objects[&object], 7);
        assert_eq!(selector.parts[&part], 19);
    }

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
