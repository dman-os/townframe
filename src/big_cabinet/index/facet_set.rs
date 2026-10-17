//! Durable Cabinet facet-set membership projection.
//!
//! This module stores metadata about readable facets, not facet values. The
//! input adapter supplies accepted facet heads and document heads; a later
//! reader can hydrate values from those exact heads. Membership and its
//! source/projected frontiers are committed in one SQLite transaction.

use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Row, Sqlite, SqliteConnection, SqlitePool};
use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, Write};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FacetRoute {
    pub document_id: String,
    pub facet_tag: String,
    pub facet_key_id: String,
}

impl FacetRoute {
    pub fn new(
        document_id: impl Into<String>,
        facet_tag: impl Into<String>,
        facet_key_id: impl Into<String>,
    ) -> Self {
        Self {
            document_id: document_id.into(),
            facet_tag: facet_tag.into(),
            facet_key_id: facet_key_id.into(),
        }
    }

    fn validate(&self) -> Result<(), FacetSetError> {
        if self.document_id.is_empty() {
            return Err(FacetSetError::InvalidRoute("document_id"));
        }
        if self.facet_tag.is_empty() {
            return Err(FacetSetError::InvalidRoute("facet_tag"));
        }
        if self.facet_key_id.is_empty() {
            return Err(FacetSetError::InvalidRoute("facet_key_id"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Availability {
    UnknownSchema,
    Unreadable,
    PendingVerification,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FacetMembership {
    Present {
        branch_heads: Vec<String>,
        facet_heads: Vec<String>,
        actor_id: String,
    },
    Removed {
        branch_heads: Option<Vec<String>>,
        removed_local: bool,
    },
    Unavailable {
        branch_heads: Option<Vec<String>>,
        availability: Availability,
    },
}

impl FacetMembership {
    fn is_removed(&self) -> bool {
        matches!(self, Self::Removed { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetRecord {
    pub route: FacetRoute,
    pub membership: FacetMembership,
    pub source_revision: u64,
    pub projected_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCursor {
    pub source_revision: u64,
    pub projected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionOutcome {
    Applied(ProjectionCursor),
    AlreadyApplied(ProjectionCursor),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FacetSetDelta {
    pub route: FacetRoute,
    pub membership: FacetMembership,
}

impl FacetSetDelta {
    pub fn present(
        route: FacetRoute,
        branch_heads: Vec<String>,
        facet_heads: Vec<String>,
        actor_id: impl Into<String>,
    ) -> Self {
        Self {
            route,
            membership: FacetMembership::Present {
                branch_heads,
                facet_heads,
                actor_id: actor_id.into(),
            },
        }
    }

    pub fn removed(
        route: FacetRoute,
        branch_heads: Option<Vec<String>>,
        removed_local: bool,
    ) -> Self {
        Self {
            route,
            membership: FacetMembership::Removed {
                branch_heads,
                removed_local,
            },
        }
    }

    pub fn unavailable(
        route: FacetRoute,
        branch_heads: Option<Vec<String>>,
        availability: Availability,
    ) -> Self {
        Self {
            route,
            membership: FacetMembership::Unavailable {
                branch_heads,
                availability,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FacetSetQuery {
    All {
        include_removed: bool,
    },
    Documents {
        document_ids: Vec<String>,
        include_removed: bool,
    },
    FacetTags {
        facet_tags: Vec<String>,
        include_removed: bool,
    },
    Routes {
        routes: Vec<FacetRoute>,
        include_removed: bool,
    },
}

impl FacetSetQuery {
    pub fn all() -> Self {
        Self::All {
            include_removed: false,
        }
    }

    pub fn documents(document_ids: Vec<String>) -> Self {
        Self::Documents {
            document_ids,
            include_removed: false,
        }
    }

    pub fn facet_tags(facet_tags: Vec<String>) -> Self {
        Self::FacetTags {
            facet_tags,
            include_removed: false,
        }
    }

    pub fn routes(routes: Vec<FacetRoute>) -> Self {
        Self::Routes {
            routes,
            include_removed: false,
        }
    }

    pub fn including_removed(self) -> Self {
        match self {
            Self::All { .. } => Self::All {
                include_removed: true,
            },
            Self::Documents { document_ids, .. } => Self::Documents {
                document_ids,
                include_removed: true,
            },
            Self::FacetTags { facet_tags, .. } => Self::FacetTags {
                facet_tags,
                include_removed: true,
            },
            Self::Routes { routes, .. } => Self::Routes {
                routes,
                include_removed: true,
            },
        }
    }

    fn include_removed(&self) -> bool {
        match self {
            Self::All { include_removed }
            | Self::Documents {
                include_removed, ..
            }
            | Self::FacetTags {
                include_removed, ..
            }
            | Self::Routes {
                include_removed, ..
            } => *include_removed,
        }
    }
}

#[derive(Debug, Error)]
pub enum FacetSetError {
    #[error("facet-set SQL error: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("facet-set JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("facet route field is empty: {0}")]
    InvalidRoute(&'static str),
    #[error("the same facet route occurs more than once in one source revision: {0:?}")]
    DuplicateRoute(FacetRoute),
    #[error("projection revision {revision} is older than the current cursor {current}")]
    StaleRevision { revision: u64, current: u64 },
    #[error(
        "replay payload differs for source revision {source_revision} and projected revision {projected_revision}"
    )]
    ReplayMismatch {
        source_revision: u64,
        projected_revision: u64,
    },
    #[error("projection revision does not fit SQLite INTEGER: {0}")]
    RevisionOverflow(u64),
    #[error("SQLite stored revision is negative: {0}")]
    InvalidStoredRevision(i64),
}

impl fmt::Display for ProjectionCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "source={}, projected={}",
            self.source_revision, self.projected_revision
        )
    }
}

#[derive(Clone)]
pub struct FacetSetStore {
    pool: SqlitePool,
}

impl FacetSetStore {
    /// Open a ready facet-set store and initialize its schema before returning.
    ///
    /// Read and write methods intentionally do not perform lazy schema work;
    /// callers must retain the successfully opened store for its lifecycle.
    pub async fn open(pool: SqlitePool) -> Result<Self, FacetSetError> {
        let store = Self { pool };
        store.initialize_schema().await?;
        Ok(store)
    }

    async fn initialize_schema(&self) -> Result<(), FacetSetError> {
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_membership (
                document_id TEXT NOT NULL
                , facet_tag TEXT NOT NULL
                , facet_key_id TEXT NOT NULL
                , state_json TEXT NOT NULL
                , is_removed INTEGER NOT NULL
                , source_revision INTEGER NOT NULL
                , projected_revision INTEGER NOT NULL
                , PRIMARY KEY (document_id, facet_tag, facet_key_id)
            )"#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS big_cabinet_facet_set_tag_idx
                 ON big_cabinet_facet_set_membership (facet_tag, document_id, facet_key_id)"#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_set_cursor (
                projection_id INTEGER PRIMARY KEY CHECK (projection_id = 1)
                , source_revision INTEGER NOT NULL
                , projected_revision INTEGER NOT NULL
                , batch_digest BLOB NOT NULL
            )"#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"INSERT OR IGNORE INTO big_cabinet_facet_set_cursor (
                projection_id
                , source_revision
                , projected_revision
                , batch_digest
            ) VALUES (1, 0, 0, ?)"#,
        )
        .bind(empty_batch_digest())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Return the one cursor for this shared projection. Membership rows and
    /// this cursor form one global projection; independent consumer cursors
    /// must be built as separate stores/projections rather than overwriting
    /// these shared rows.
    pub async fn cursor(&self) -> Result<ProjectionCursor, FacetSetError> {
        let row = sqlx::query(
            r#"SELECT source_revision
                 , projected_revision
              FROM big_cabinet_facet_set_cursor
             WHERE projection_id = 1"#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(ProjectionCursor {
            source_revision: u64_from_sql(row.get::<i64, _>("source_revision"))?,
            projected_revision: u64_from_sql(row.get::<i64, _>("projected_revision"))?,
        })
    }

    /// Apply one source revision and its interpreted projection atomically.
    ///
    /// A managed SQLite transaction started with `BEGIN IMMEDIATE` serializes
    /// cursor validation with row settlement. The bounded cryptographic batch
    /// digest rejects same-cursor/different-payload replays before
    /// any row can be rewritten. There is one global cursor because membership
    /// rows are shared; a consumer-specific projection needs its own store.
    pub async fn apply_revision(
        &self,
        source_revision: u64,
        projected_revision: u64,
        deltas: &[FacetSetDelta],
    ) -> Result<ProjectionOutcome, FacetSetError> {
        self.apply_revision_internal(source_revision, projected_revision, deltas, false)
            .await
    }

    async fn apply_revision_internal(
        &self,
        source_revision: u64,
        projected_revision: u64,
        deltas: &[FacetSetDelta],
        fail_after_first_row: bool,
    ) -> Result<ProjectionOutcome, FacetSetError> {
        let batch_digest = normalized_batch_digest(deltas)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let result = apply_revision_locked(
            &mut transaction,
            source_revision,
            projected_revision,
            &batch_digest,
            deltas,
            fail_after_first_row,
        )
        .await;
        match result {
            Ok(outcome) => {
                transaction.commit().await?;
                Ok(outcome)
            }
            Err(error) => Err(error),
        }
    }

    pub async fn query(&self, selector: FacetSetQuery) -> Result<Vec<FacetRecord>, FacetSetError> {
        let mut query = QueryBuilder::<Sqlite>::new(
            r#"SELECT document_id
                 , facet_tag
                 , facet_key_id
                 , state_json
                 , source_revision
                 , projected_revision
              FROM big_cabinet_facet_set_membership
             WHERE 1 = 1"#,
        );
        if !selector.include_removed() {
            query.push(" AND is_removed = 0");
        }
        match selector {
            FacetSetQuery::All { .. } => {}
            FacetSetQuery::Documents { document_ids, .. } => {
                push_text_in(&mut query, "document_id", &document_ids);
            }
            FacetSetQuery::FacetTags { facet_tags, .. } => {
                push_text_in(&mut query, "facet_tag", &facet_tags);
            }
            FacetSetQuery::Routes { routes, .. } => {
                if routes.is_empty() {
                    query.push(" AND 1 = 0");
                } else {
                    query.push(" AND (");
                    for (index, route) in routes.iter().enumerate() {
                        if index > 0 {
                            query.push(" OR ");
                        }
                        query
                            .push("(document_id = ")
                            .push_bind(&route.document_id)
                            .push(" AND facet_tag = ")
                            .push_bind(&route.facet_tag)
                            .push(" AND facet_key_id = ")
                            .push_bind(&route.facet_key_id)
                            .push(")");
                    }
                    query.push(")");
                }
            }
        }
        query.push(" ORDER BY document_id, facet_tag, facet_key_id");
        let rows = query.build().fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(FacetRecord {
                    route: FacetRoute {
                        document_id: row.get("document_id"),
                        facet_tag: row.get("facet_tag"),
                        facet_key_id: row.get("facet_key_id"),
                    },
                    membership: serde_json::from_str(row.get("state_json"))?,
                    source_revision: u64_from_sql(row.get("source_revision"))?,
                    projected_revision: u64_from_sql(row.get("projected_revision"))?,
                })
            })
            .collect()
    }
}

async fn apply_revision_locked(
    connection: &mut SqliteConnection,
    source_revision: u64,
    projected_revision: u64,
    batch_digest: &[u8],
    deltas: &[FacetSetDelta],
    fail_after_first_row: bool,
) -> Result<ProjectionOutcome, FacetSetError> {
    let source_revision_sql = i64_revision(source_revision)?;
    let projected_revision_sql = i64_revision(projected_revision)?;
    let row = sqlx::query(
        r#"SELECT source_revision
             , projected_revision
             , batch_digest
          FROM big_cabinet_facet_set_cursor
         WHERE projection_id = 1"#,
    )
    .fetch_one(&mut *connection)
    .await?;
    let current_source = u64_from_sql(row.get::<i64, _>("source_revision"))?;
    let current_projected = u64_from_sql(row.get::<i64, _>("projected_revision"))?;
    if source_revision < current_source {
        return Err(FacetSetError::StaleRevision {
            revision: source_revision,
            current: current_source,
        });
    }
    if projected_revision < current_projected {
        return Err(FacetSetError::StaleRevision {
            revision: projected_revision,
            current: current_projected,
        });
    }
    let replay = source_revision == current_source && projected_revision == current_projected;
    if replay {
        if batch_digest != row.get::<Vec<u8>, _>("batch_digest") {
            return Err(FacetSetError::ReplayMismatch {
                source_revision,
                projected_revision,
            });
        }
        return Ok(ProjectionOutcome::AlreadyApplied(ProjectionCursor {
            source_revision,
            projected_revision,
        }));
    }
    for (index, delta) in deltas.iter().enumerate() {
        let state_json = serde_json::to_string(&delta.membership)?;
        sqlx::query(
            r#"INSERT INTO big_cabinet_facet_set_membership (
                document_id
                , facet_tag
                , facet_key_id
                , state_json
                , is_removed
                , source_revision
                , projected_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(document_id, facet_tag, facet_key_id) DO UPDATE SET
                state_json = excluded.state_json
                , is_removed = excluded.is_removed
                , source_revision = excluded.source_revision
                , projected_revision = excluded.projected_revision"#,
        )
        .bind(&delta.route.document_id)
        .bind(&delta.route.facet_tag)
        .bind(&delta.route.facet_key_id)
        .bind(state_json)
        .bind(delta.membership.is_removed())
        .bind(source_revision_sql)
        .bind(projected_revision_sql)
        .execute(&mut *connection)
        .await?;
        if fail_after_first_row && index == 0 {
            sqlx::query("SELECT injected_facet_set_failure")
                .execute(&mut *connection)
                .await?;
        }
    }
    sqlx::query(
        r#"UPDATE big_cabinet_facet_set_cursor
              SET source_revision = ?
                , projected_revision = ?
                , batch_digest = ?
            WHERE projection_id = 1"#,
    )
    .bind(source_revision_sql)
    .bind(projected_revision_sql)
    .bind(batch_digest)
    .execute(&mut *connection)
    .await?;
    Ok(ProjectionOutcome::Applied(ProjectionCursor {
        source_revision,
        projected_revision,
    }))
}

fn normalized_batch_digest(deltas: &[FacetSetDelta]) -> Result<Vec<u8>, FacetSetError> {
    let mut routes = BTreeSet::new();
    let mut sorted = deltas.to_vec();
    for delta in &sorted {
        delta.route.validate()?;
        if !routes.insert(delta.route.clone()) {
            return Err(FacetSetError::DuplicateRoute(delta.route.clone()));
        }
    }
    sorted.sort_by(|left, right| left.route.cmp(&right.route));
    let mut writer = DigestWriter::new();
    serde_json::to_writer(&mut writer, &sorted)?;
    Ok(writer.finish())
}

fn empty_batch_digest() -> Vec<u8> {
    normalized_batch_digest(&[]).expect("empty batch digest")
}

struct DigestWriter {
    hasher: blake3::Hasher,
}

impl DigestWriter {
    fn new() -> Self {
        Self {
            hasher: blake3::Hasher::new(),
        }
    }

    fn finish(self) -> Vec<u8> {
        self.hasher.finalize().as_bytes().to_vec()
    }
}

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.hasher.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn push_text_in(query: &mut QueryBuilder<Sqlite>, column: &str, values: &[String]) {
    if values.is_empty() {
        query.push(" AND 1 = 0");
        return;
    }
    query.push(" AND ").push(column).push(" IN (");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            query.push(", ");
        }
        query.push_bind(value);
    }
    query.push(")");
}

fn i64_revision(revision: u64) -> Result<i64, FacetSetError> {
    i64::try_from(revision).map_err(|_| FacetSetError::RevisionOverflow(revision))
}

fn u64_from_sql(revision: i64) -> Result<u64, FacetSetError> {
    u64::try_from(revision).map_err(|_| FacetSetError::InvalidStoredRevision(revision))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;
    use tempfile::TempDir;

    async fn memory_store() -> FacetSetStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite pool");
        FacetSetStore::open(pool).await.expect("facet-set schema")
    }

    async fn file_store(directory: &TempDir, name: &str) -> FacetSetStore {
        let path = directory.path().join(name);
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("file-backed SQLite pool");
        FacetSetStore::open(pool).await.expect("facet-set schema")
    }

    #[tokio::test]
    async fn open_reports_initialization_failure_before_returning_store() {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite pool");
        pool.close().await;
        assert!(FacetSetStore::open(pool).await.is_err());
    }

    fn route(document_id: &str, tag: &str, key_id: &str) -> FacetRoute {
        FacetRoute::new(document_id, tag, key_id)
    }

    #[tokio::test]
    async fn exact_records_and_selectors_preserve_all_states() {
        let store = memory_store().await;
        store
            .apply_revision(
                4,
                9,
                &[
                    FacetSetDelta::present(
                        route("doc-a", "note", "main"),
                        vec!["branch-head".into()],
                        vec!["facet-head".into()],
                        "actor-a",
                    ),
                    FacetSetDelta::unavailable(
                        route("doc-b", "note", "main"),
                        Some(vec!["branch-b".into()]),
                        Availability::UnknownSchema,
                    ),
                    FacetSetDelta::unavailable(
                        route("doc-c", "note", "main"),
                        Some(vec!["branch-c".into()]),
                        Availability::Unreadable,
                    ),
                    FacetSetDelta::unavailable(
                        route("doc-d", "note", "main"),
                        None,
                        Availability::PendingVerification,
                    ),
                ],
            )
            .await
            .expect("apply current state");
        let expected_present = FacetRecord {
            route: route("doc-a", "note", "main"),
            membership: FacetMembership::Present {
                branch_heads: vec!["branch-head".into()],
                facet_heads: vec!["facet-head".into()],
                actor_id: "actor-a".into(),
            },
            source_revision: 4,
            projected_revision: 9,
        };
        assert_eq!(
            store
                .query(FacetSetQuery::documents(vec!["doc-a".into()]))
                .await
                .expect("query document selector"),
            vec![expected_present.clone()]
        );
        let selected = store
            .query(FacetSetQuery::documents(vec![
                "doc-a".into(),
                "doc-c".into(),
            ]))
            .await
            .expect("query multiple document selector");
        assert_eq!(selected[0], expected_present);
        assert_eq!(
            selected[1],
            FacetRecord {
                route: route("doc-c", "note", "main"),
                membership: FacetMembership::Unavailable {
                    branch_heads: Some(vec!["branch-c".into()]),
                    availability: Availability::Unreadable,
                },
                source_revision: 4,
                projected_revision: 9,
            }
        );
        assert_eq!(
            store
                .query(FacetSetQuery::facet_tags(vec!["note".into()]))
                .await
                .expect("query facet tag selector"),
            vec![
                expected_present,
                FacetRecord {
                    route: route("doc-b", "note", "main"),
                    membership: FacetMembership::Unavailable {
                        branch_heads: Some(vec!["branch-b".into()]),
                        availability: Availability::UnknownSchema,
                    },
                    source_revision: 4,
                    projected_revision: 9,
                },
                FacetRecord {
                    route: route("doc-c", "note", "main"),
                    membership: FacetMembership::Unavailable {
                        branch_heads: Some(vec!["branch-c".into()]),
                        availability: Availability::Unreadable,
                    },
                    source_revision: 4,
                    projected_revision: 9,
                },
                FacetRecord {
                    route: route("doc-d", "note", "main"),
                    membership: FacetMembership::Unavailable {
                        branch_heads: None,
                        availability: Availability::PendingVerification,
                    },
                    source_revision: 4,
                    projected_revision: 9,
                },
            ]
        );
        assert_eq!(
            store
                .query(FacetSetQuery::routes(vec![route("doc-d", "note", "main")]))
                .await
                .expect("query pending route")[0]
                .membership,
            FacetMembership::Unavailable {
                branch_heads: None,
                availability: Availability::PendingVerification,
            }
        );
    }

    #[tokio::test]
    async fn removal_and_readd_preserve_typed_provenance() {
        let store = memory_store().await;
        let key = route("doc", "note", "main");
        store
            .apply_revision(
                1,
                1,
                &[FacetSetDelta::present(
                    key.clone(),
                    vec!["branch-1".into()],
                    vec!["facet-1".into()],
                    "actor",
                )],
            )
            .await
            .expect("insert present");
        store
            .apply_revision(
                2,
                2,
                &[FacetSetDelta::removed(
                    key.clone(),
                    Some(vec!["branch-1".into()]),
                    true,
                )],
            )
            .await
            .expect("insert tombstone");
        let expected_removed = FacetRecord {
            route: key.clone(),
            membership: FacetMembership::Removed {
                branch_heads: Some(vec!["branch-1".into()]),
                removed_local: true,
            },
            source_revision: 2,
            projected_revision: 2,
        };
        assert_eq!(
            store
                .query(FacetSetQuery::routes(vec![key.clone()]).including_removed())
                .await
                .expect("query tombstone"),
            vec![expected_removed]
        );
        assert!(
            store
                .query(FacetSetQuery::routes(vec![key.clone()]))
                .await
                .expect("query live membership")
                .is_empty()
        );
        store
            .apply_revision(
                3,
                3,
                &[FacetSetDelta::present(
                    key.clone(),
                    vec!["branch-2".into()],
                    vec!["facet-2".into()],
                    "actor-2",
                )],
            )
            .await
            .expect("re-add current state");
        assert_eq!(
            store
                .query(FacetSetQuery::routes(vec![key.clone()]))
                .await
                .expect("query re-added membership")[0],
            FacetRecord {
                route: key,
                membership: FacetMembership::Present {
                    branch_heads: vec!["branch-2".into()],
                    facet_heads: vec!["facet-2".into()],
                    actor_id: "actor-2".into(),
                },
                source_revision: 3,
                projected_revision: 3,
            }
        );
        assert_eq!(store.cursor().await.expect("cursor").source_revision, 3);
    }

    #[tokio::test]
    async fn replay_mismatch_and_duplicate_routes_are_non_mutating() {
        let store = memory_store().await;
        let key = route("doc", "note", "main");
        store
            .apply_revision(
                7,
                11,
                &[FacetSetDelta::present(
                    key.clone(),
                    vec!["branch".into()],
                    vec!["facet".into()],
                    "actor-a",
                )],
            )
            .await
            .expect("first apply");
        let restarted = FacetSetStore::open(store.pool.clone())
            .await
            .expect("reopen facet-set store");
        assert!(matches!(
            restarted
                .apply_revision(
                    7,
                    11,
                    &[FacetSetDelta::present(
                        key.clone(),
                        vec!["branch".into()],
                        vec!["facet".into()],
                        "actor-a",
                    )],
                )
                .await
                .expect("identical replay"),
            ProjectionOutcome::AlreadyApplied(_)
        ));
        assert!(matches!(
            restarted
                .apply_revision(
                    7,
                    11,
                    &[FacetSetDelta::present(
                        key.clone(),
                        vec!["branch".into()],
                        vec!["facet".into()],
                        "actor-b",
                    )],
                )
                .await,
            Err(FacetSetError::ReplayMismatch { .. })
        ));
        assert_eq!(
            restarted
                .query(FacetSetQuery::routes(vec![key.clone()]))
                .await
                .expect("query after mismatch")[0]
                .membership,
            FacetMembership::Present {
                branch_heads: vec!["branch".into()],
                facet_heads: vec!["facet".into()],
                actor_id: "actor-a".into(),
            }
        );
        assert!(matches!(
            restarted
                .apply_revision(
                    8,
                    12,
                    &[
                        FacetSetDelta::present(key.clone(), vec![], vec![], "actor",),
                        FacetSetDelta::present(key, vec![], vec![], "actor")
                    ],
                )
                .await,
            Err(FacetSetError::DuplicateRoute(_))
        ));
        assert_eq!(restarted.cursor().await.expect("cursor").source_revision, 7);
    }

    #[tokio::test]
    async fn managed_abort_after_row_write_rolls_back_and_pool_reuses_transaction() {
        let store = memory_store().await;
        let delta = [FacetSetDelta::present(
            route("doc", "note", "main"),
            vec!["branch".into()],
            vec!["facet".into()],
            "actor",
        )];
        let batch_digest = normalized_batch_digest(&delta).expect("batch digest");
        let mut transaction = store
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect("managed transaction");
        let result = apply_revision_locked(&mut transaction, 1, 1, &batch_digest, &delta, false)
            .await
            .expect("write before deliberate abort");
        assert!(matches!(result, ProjectionOutcome::Applied(_)));
        drop(transaction);

        assert_eq!(
            store
                .cursor()
                .await
                .expect("rolled-back cursor")
                .source_revision,
            0
        );
        assert!(
            store
                .query(FacetSetQuery::all().including_removed())
                .await
                .expect("rolled-back membership")
                .is_empty()
        );
        store
            .apply_revision(1, 1, &delta)
            .await
            .expect("same pool remains usable");
        assert_eq!(
            store
                .cursor()
                .await
                .expect("committed cursor")
                .source_revision,
            1
        );
    }

    #[tokio::test]
    async fn injected_failure_after_first_row_rolls_back_and_reopen_sees_no_pending_write() {
        let store = memory_store().await;
        let delta = [
            FacetSetDelta::present(
                route("doc-a", "note", "main"),
                vec!["branch-a".into()],
                vec!["facet-a".into()],
                "actor-a",
            ),
            FacetSetDelta::present(
                route("doc-b", "note", "main"),
                vec!["branch-b".into()],
                vec!["facet-b".into()],
                "actor-b",
            ),
        ];
        let result = store.apply_revision_internal(5, 5, &delta, true).await;
        assert!(matches!(result, Err(FacetSetError::Sql(_))));
        assert_eq!(
            store
                .cursor()
                .await
                .expect("failure cursor")
                .source_revision,
            0
        );
        assert!(
            store
                .query(FacetSetQuery::all().including_removed())
                .await
                .expect("failure membership")
                .is_empty()
        );

        let directory = tempfile::tempdir().expect("temporary directory");
        let pending_store = file_store(&directory, "pending.sqlite").await;
        let file_delta = [FacetSetDelta::present(
            route("pending", "note", "main"),
            vec!["branch".into()],
            vec!["facet".into()],
            "actor",
        )];
        let digest = normalized_batch_digest(&file_delta).expect("file batch digest");
        let mut transaction = pending_store
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect("file transaction");
        apply_revision_locked(&mut transaction, 8, 8, &digest, &file_delta, false)
            .await
            .expect("pending file write");
        drop(transaction);
        pending_store.pool.close().await;

        let reopened = file_store(&directory, "pending.sqlite").await;
        assert_eq!(
            reopened
                .cursor()
                .await
                .expect("reopened pending cursor")
                .source_revision,
            0
        );
        assert!(
            reopened
                .query(FacetSetQuery::all().including_removed())
                .await
                .expect("reopened pending membership")
                .is_empty()
        );
        reopened.pool.close().await;
    }

    #[tokio::test]
    async fn commit_order_and_frontier_advancement_matrix_is_monotonic() {
        let old_then_new = memory_store().await;
        let old = [FacetSetDelta::present(
            route("doc", "note", "main"),
            vec!["old-branch".into()],
            vec!["old-facet".into()],
            "old",
        )];
        let new = [FacetSetDelta::present(
            route("doc", "note", "main"),
            vec!["new-branch".into()],
            vec!["new-facet".into()],
            "new",
        )];
        old_then_new
            .apply_revision(1, 1, &old)
            .await
            .expect("old commit first");
        old_then_new
            .apply_revision(2, 2, &new)
            .await
            .expect("new commit second");
        assert_eq!(
            old_then_new
                .cursor()
                .await
                .expect("old-new cursor")
                .source_revision,
            2
        );

        let new_then_old = memory_store().await;
        new_then_old
            .apply_revision(2, 2, &new)
            .await
            .expect("new commit first");
        assert!(matches!(
            new_then_old.apply_revision(1, 1, &old).await,
            Err(FacetSetError::StaleRevision { .. })
        ));
        assert_eq!(
            new_then_old
                .cursor()
                .await
                .expect("new-old cursor")
                .source_revision,
            2
        );

        let matrix = memory_store().await;
        matrix
            .apply_revision(
                1,
                2,
                &[FacetSetDelta::present(
                    route("one", "note", "main"),
                    vec![],
                    vec![],
                    "actor",
                )],
            )
            .await
            .expect("advance projected frontier");
        matrix
            .apply_revision(
                2,
                2,
                &[FacetSetDelta::present(
                    route("two", "note", "main"),
                    vec![],
                    vec![],
                    "actor",
                )],
            )
            .await
            .expect("advance source frontier");
        matrix
            .apply_revision(
                2,
                3,
                &[FacetSetDelta::present(
                    route("three", "note", "main"),
                    vec![],
                    vec![],
                    "actor",
                )],
            )
            .await
            .expect("advance projected frontier again");
        assert_eq!(
            matrix.cursor().await.expect("matrix cursor"),
            ProjectionCursor {
                source_revision: 2,
                projected_revision: 3,
            }
        );
        assert!(matches!(
            matrix.apply_revision(1, 4, &[]).await,
            Err(FacetSetError::StaleRevision { .. })
        ));
        assert!(matches!(
            matrix.apply_revision(3, 2, &[]).await,
            Err(FacetSetError::StaleRevision { .. })
        ));
    }

    #[tokio::test]
    async fn invalid_batch_rolls_back_rows_and_cursor() {
        let store = memory_store().await;
        let result = store
            .apply_revision(
                8,
                12,
                &[
                    FacetSetDelta::present(
                        route("doc-2", "note", "main"),
                        vec!["branch".into()],
                        vec!["facet".into()],
                        "actor",
                    ),
                    FacetSetDelta::present(
                        FacetRoute::new("", "note", "main"),
                        vec![],
                        vec![],
                        "actor",
                    ),
                ],
            )
            .await;
        assert!(matches!(
            result,
            Err(FacetSetError::InvalidRoute("document_id"))
        ));
        assert_eq!(store.cursor().await.expect("cursor").source_revision, 0);
        assert!(
            store
                .query(FacetSetQuery::routes(vec![route("doc-2", "note", "main")]))
                .await
                .expect("query rolled-back row")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn file_backed_close_and_reopen_preserves_projection() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = file_store(&directory, "facet-set.sqlite").await;
        let expected = FacetRecord {
            route: route("doc", "note", "main"),
            membership: FacetMembership::Present {
                branch_heads: vec!["branch".into()],
                facet_heads: vec!["facet".into()],
                actor_id: "actor".into(),
            },
            source_revision: 4,
            projected_revision: 5,
        };
        store
            .apply_revision(
                4,
                5,
                &[FacetSetDelta::present(
                    expected.route.clone(),
                    vec!["branch".into()],
                    vec!["facet".into()],
                    "actor",
                )],
            )
            .await
            .expect("file-backed apply");
        store.pool.close().await;
        let reopened = file_store(&directory, "facet-set.sqlite").await;
        assert_eq!(
            reopened
                .cursor()
                .await
                .expect("reopened cursor")
                .source_revision,
            4
        );
        assert_eq!(
            reopened
                .query(FacetSetQuery::all())
                .await
                .expect("reopened query"),
            vec![expected]
        );
        reopened.pool.close().await;
    }

    #[tokio::test]
    async fn concurrent_pools_cannot_regress_global_cursor_or_rows() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let first = file_store(&directory, "concurrent.sqlite").await;
        first.pool.close().await;
        let store_a = file_store(&directory, "concurrent.sqlite").await;
        let store_b = file_store(&directory, "concurrent.sqlite").await;
        let newer_delta = [FacetSetDelta::present(
            route("doc", "note", "main"),
            vec!["new-branch".into()],
            vec!["new-facet".into()],
            "new-actor",
        )];
        let older_delta = [FacetSetDelta::removed(
            route("doc", "note", "main"),
            None,
            false,
        )];
        let (newer, older) = tokio::join!(
            store_a.apply_revision(10, 10, &newer_delta),
            store_b.apply_revision(1, 1, &older_delta)
        );
        assert!(matches!(newer, Ok(ProjectionOutcome::Applied(_))));
        assert!(matches!(
            older,
            Ok(ProjectionOutcome::Applied(_)) | Err(FacetSetError::StaleRevision { .. })
        ));
        let final_store = file_store(&directory, "concurrent.sqlite").await;
        assert_eq!(
            final_store
                .cursor()
                .await
                .expect("final cursor")
                .source_revision,
            10
        );
        assert_eq!(
            final_store
                .query(FacetSetQuery::all().including_removed())
                .await
                .expect("final query")[0]
                .source_revision,
            10
        );
        store_a.pool.close().await;
        store_b.pool.close().await;
        final_store.pool.close().await;
    }
}
