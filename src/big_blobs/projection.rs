use big_blobs_core::Blob;
use big_blobs_core::BlobId;
use big_cabinet::index::DatumMembership;
use big_cabinet::index::DatumRoute;
use big_cabinet::index::DatumSetStore;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ReadDatums;
use big_sync::keyed_frontier::SqliteFrontierCodec;
use big_sync::keyed_frontier::SqliteFrontierSelector;
use big_sync::keyed_frontier::SqliteKeyedFrontier;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::keyed_frontier::KeyedFrontier;
use big_sync_core::keyed_frontier::KeyedFrontierError;
use big_sync_core::revisioned_store::KeyedFrontierRevisionReader;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStore;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use serde::Deserialize;
use serde::Serialize;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Row;
use sqlite_rs::Sql;
use sqlite_rs::Tx;
use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::Notify;
use utils_rs::prelude::Res;
use utils_rs::prelude::async_trait;
use utils_rs::prelude::ferr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobReference {
    pub route: DatumRoute,
    pub blob_id: BlobId,
    pub length_octets: u64,
}

#[derive(Clone)]
struct BlobCodec;

impl SqliteFrontierCodec for BlobCodec {
    type Key = DatumRoute;
    type Value = BlobReference;

    fn encode_key(&self, key: &Self::Key) -> Vec<u8> {
        serde_json::to_vec(key).expect("datum routes serialize")
    }

    fn decode_key(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Key, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }

    fn encode_value(&self, value: &Self::Value) -> Vec<u8> {
        serde_json::to_vec(value).expect("blob references serialize")
    }

    fn decode_value(
        &self,
        bytes: &[u8],
    ) -> Result<Self::Value, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

#[derive(Clone)]
pub struct BlobProjection {
    sql: Database,
    frontier: SqliteKeyedFrontier<BlobCodec>,
    rearm: Arc<Notify>,
}

#[derive(Debug, PartialEq, Eq)]
enum Prepared {
    Ready(Option<BlobReference>),
    Parked,
}

enum ReadSource {
    Frontier,
    Current,
}

enum Progress {
    Incomplete,
    Complete(u64),
}

impl BlobProjection {
    pub async fn new(sql: Database) -> Res<Self> {
        let transaction = sql.begin_write().await?;
        for statement in [
            r#"CREATE TABLE IF NOT EXISTS big_blobs_projection_routes (
                   doc_id TEXT NOT NULL
                 , datum_key TEXT NOT NULL
                 , source_revision INTEGER NOT NULL CHECK (source_revision >= 0)
                 , blob_id BLOB
                 , length_octets TEXT
                 , PRIMARY KEY (doc_id, datum_key)
                 , CHECK ((blob_id IS NULL) = (length_octets IS NULL))
                 , CHECK (blob_id IS NULL OR length(blob_id) = 32)
               ) WITHOUT ROWID"#,
            r#"CREATE INDEX IF NOT EXISTS big_blobs_projection_digest
                   ON big_blobs_projection_routes (blob_id, doc_id, datum_key)"#,
            r#"CREATE TABLE IF NOT EXISTS big_blobs_projection_parked (
                   doc_id TEXT NOT NULL
                 , datum_key TEXT NOT NULL
                 , source_revision INTEGER NOT NULL CHECK (source_revision >= 0)
                 , membership BLOB NOT NULL
                 , PRIMARY KEY (doc_id, datum_key)
               ) WITHOUT ROWID"#,
            r#"CREATE TABLE IF NOT EXISTS big_blobs_projection_progress (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1)
                 , source_revision INTEGER NOT NULL CHECK (source_revision >= 0)
               )"#,
            r#"INSERT OR IGNORE INTO big_blobs_projection_progress (singleton, source_revision)
               VALUES (1, 0)"#,
        ] {
            Sql::new(statement).execute(&transaction).await?;
        }
        transaction.commit().await?;
        let frontier = SqliteKeyedFrontier::new(
            sql.clone(),
            "big-blobs-projection-v1",
            BlobCodec,
            Arc::new(Notify::new()),
        )
        .await?;
        Ok(Self {
            sql,
            frontier,
            rearm: Arc::new(Notify::new()),
        })
    }

    pub fn rearm(&self) {
        self.rearm.notify_one();
    }

    pub async fn query_digest(&self, blob_id: &BlobId) -> Res<Vec<DatumRoute>> {
        Sql::new(
            r#"SELECT doc_id
                 , datum_key
              FROM big_blobs_projection_routes
             WHERE blob_id = "#,
        )
        .bind(blob_id.to_bytes32().to_vec())
        .push(" ORDER BY doc_id, datum_key")
        .query(&self.sql)
        .await?
        .rows
        .into_iter()
        .map(|row| {
            Ok(DatumRoute {
                doc_id: row.get::<String>("doc_id")?.parse()?,
                datum_key: row.get::<String>("datum_key")?.parse()?,
            })
        })
        .collect()
    }

    pub async fn query_doc(&self, doc_id: &DocId) -> Res<Vec<BlobReference>> {
        Sql::new(
            r#"SELECT doc_id
                 , datum_key
                 , blob_id
                 , length_octets
              FROM big_blobs_projection_routes
             WHERE blob_id IS NOT NULL AND doc_id = "#,
        )
        .bind(doc_id.to_string())
        .push(" ORDER BY datum_key")
        .query(&self.sql)
        .await?
        .rows
        .into_iter()
        .map(reference_from_row)
        .collect()
    }

    pub async fn source_revision(&self) -> Res<u64> {
        let rows = Sql::new(
            "SELECT source_revision FROM big_blobs_projection_progress WHERE singleton = 1",
        )
        .query(&self.sql)
        .await?
        .rows;
        Ok(u64::try_from(rows[0].get::<i64>("source_revision")?)?)
    }

    pub async fn open(
        &self,
        after: u64,
    ) -> Result<KeyedFrontierRevisionReader<'_, DatumRoute, BlobReference>, KeyedFrontierError>
    {
        Ok(KeyedFrontierRevisionReader::new(
            self.frontier
                .open(SqliteFrontierSelector::All { after })
                .await?,
        ))
    }

    async fn superseded(
        executor: &impl QueryExecutor,
        route: &DatumRoute,
        revision: u64,
    ) -> Res<bool> {
        let rows = Sql::new(
            r#"SELECT source_revision
                 , 0 AS parked
              FROM big_blobs_projection_routes
             WHERE doc_id = "#,
        )
        .bind(route.doc_id.to_string())
        .push(" AND datum_key = ")
        .bind(route.datum_key.to_string())
        .push(
            r#" UNION ALL
            SELECT source_revision
                 , 1 AS parked
              FROM big_blobs_projection_parked
             WHERE doc_id = "#,
        )
        .bind(route.doc_id.to_string())
        .push(" AND datum_key = ")
        .bind(route.datum_key.to_string())
        .query(executor)
        .await?
        .rows;
        for row in rows {
            let stored = u64::try_from(row.get::<i64>("source_revision")?)?;
            if stored > revision || (stored == revision && row.get::<i64>("parked")? == 0) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn parked(
        &self,
        before: Option<u64>,
    ) -> Res<Vec<FrontierEntry<DatumRoute, DatumMembership>>> {
        let mut query = Sql::new(
            r#"SELECT doc_id
                 , datum_key
                 , source_revision
                 , membership
              FROM big_blobs_projection_parked"#,
        );
        if let Some(before) = before {
            query
                .push(" WHERE source_revision < ")
                .bind(i64::try_from(before)?);
        }
        query
            .push(" ORDER BY doc_id, datum_key")
            .query(&self.sql)
            .await?
            .rows
            .into_iter()
            .map(|row| {
                Ok(FrontierEntry {
                    revision: u64::try_from(row.get::<i64>("source_revision")?)?,
                    key: DatumRoute {
                        doc_id: row.get::<String>("doc_id")?.parse()?,
                        datum_key: row.get::<String>("datum_key")?.parse()?,
                    },
                    value: Some(serde_json::from_slice(&row.get::<Vec<u8>>("membership")?)?),
                })
            })
            .collect()
    }

    async fn park(
        &self,
        entry: &FrontierEntry<DatumRoute, DatumMembership>,
        progress: Progress,
    ) -> Res<()> {
        let transaction = self.sql.begin_write().await?;
        if !Self::superseded(&transaction, &entry.key, entry.revision).await? {
            let membership = entry.value.as_ref().expect("only present routes park");
            assert!(matches!(membership, DatumMembership::Present { .. }));
            Sql::new(
                r#"INSERT INTO big_blobs_projection_parked (
                       doc_id
                     , datum_key
                     , source_revision
                     , membership
                   ) VALUES ("#,
            )
            .bind(entry.key.doc_id.to_string())
            .push(", ")
            .bind(entry.key.datum_key.to_string())
            .push(", ")
            .bind(i64::try_from(entry.revision)?)
            .push(", ")
            .bind(serde_json::to_vec(membership)?)
            .push(
                r#") ON CONFLICT (doc_id, datum_key) DO UPDATE SET
                       source_revision = excluded.source_revision
                     , membership = excluded.membership
                   WHERE big_blobs_projection_parked.source_revision < excluded.source_revision"#,
            )
            .execute(&transaction)
            .await?;
        }
        Self::advance_in_context(&transaction, progress).await?;
        transaction.commit().await?;
        Ok(())
    }

    #[cfg(test)]
    async fn projected_revision(
        executor: &impl QueryExecutor,
        route: &DatumRoute,
    ) -> Res<Option<u64>> {
        Sql::new(
            r#"SELECT source_revision
              FROM big_blobs_projection_routes
             WHERE doc_id = "#,
        )
        .bind(route.doc_id.to_string())
        .push(" AND datum_key = ")
        .bind(route.datum_key.to_string())
        .query(executor)
        .await?
        .rows
        .into_iter()
        .next()
        .map(|row| Ok(u64::try_from(row.get::<i64>("source_revision")?)?))
        .transpose()
    }

    async fn advance_in_context(transaction: &Tx, progress: Progress) -> Res<()> {
        if let Progress::Complete(revision) = progress {
            Sql::new(
                r#"UPDATE big_blobs_projection_progress
                      SET source_revision = MAX(source_revision, "#,
            )
            .bind(i64::try_from(revision)?)
            .push(") WHERE singleton = 1")
            .execute(transaction)
            .await?;
        }
        Ok(())
    }

    async fn settle_in_context(
        &self,
        transaction: &mut Tx,
        route: &DatumRoute,
        reference: Option<BlobReference>,
        revision: u64,
        progress: Progress,
    ) -> Res<bool> {
        let stale = Self::superseded(transaction, route, revision).await?;
        if !stale {
            if let Some(reference) = &reference {
                assert_eq!(&reference.route, route);
            }
            Sql::new(
                r#"INSERT INTO big_blobs_projection_routes (
                       doc_id
                     , datum_key
                     , source_revision
                     , blob_id
                     , length_octets
                   ) VALUES ("#,
            )
            .bind(route.doc_id.to_string())
            .push(", ")
            .bind(route.datum_key.to_string())
            .push(", ")
            .bind(i64::try_from(revision)?)
            .push(", ")
            .bind(
                reference
                    .as_ref()
                    .map(|value| value.blob_id.to_bytes32().to_vec()),
            )
            .push(", ")
            .bind(
                reference
                    .as_ref()
                    .map(|value| value.length_octets.to_string()),
            )
            .push(
                r#") ON CONFLICT (doc_id, datum_key) DO UPDATE SET
                       source_revision = excluded.source_revision
                     , blob_id = excluded.blob_id
                     , length_octets = excluded.length_octets"#,
            )
            .execute(transaction)
            .await?;
            Sql::new("DELETE FROM big_blobs_projection_parked WHERE doc_id = ")
                .bind(route.doc_id.to_string())
                .push(" AND datum_key = ")
                .bind(route.datum_key.to_string())
                .execute(transaction)
                .await?;
            self.frontier
                .apply_in_context(transaction, [(route.clone(), reference)])
                .await?;
        }
        Self::advance_in_context(transaction, progress).await?;
        Ok(!stale)
    }

    async fn settle(
        &self,
        route: &DatumRoute,
        reference: Option<BlobReference>,
        revision: u64,
        progress: Progress,
    ) -> Res<()> {
        let mut transaction = self.sql.begin_write().await?;
        let changed = self
            .settle_in_context(&mut transaction, route, reference, revision, progress)
            .await?;
        transaction.commit().await?;
        if changed {
            self.frontier.notify_changed();
        }
        Ok(())
    }

    async fn prepare(
        &self,
        docs: &Client<DocsProtocol>,
        capability: &CapId,
        entry: &FrontierEntry<DatumRoute, DatumMembership>,
        source: ReadSource,
    ) -> Res<Prepared> {
        let FrontierEntry {
            revision,
            key: route,
            value,
        } = entry;
        if Self::superseded(&self.sql, route, *revision).await? {
            return Ok(Prepared::Ready(None));
        }
        match value {
            Some(DatumMembership::Present { doc_heads, .. }) => {
                let at = match source {
                    ReadSource::Frontier => At::Heads(doc_heads.clone()),
                    ReadSource::Current => At::Current,
                };
                let mut read = match docs
                    .call(
                        RequestContext::root(),
                        ReadDatums {
                            capability: *capability,
                            doc_id: route.doc_id.clone(),
                            at: at.clone(),
                            datums: Some([route.datum_key.clone()].into()),
                            optimistic: false,
                        },
                    )
                    .await
                {
                    Ok(read) => read,
                    Err(
                        ClientError::Transport { .. }
                        | ClientError::Operation(
                            DocsError::Denied
                            | DocsError::Authority(
                                AuthorityServiceError::InvalidCapability
                                | AuthorityServiceError::Unauthorized
                                | AuthorityServiceError::Escalation
                                | AuthorityServiceError::UnrecognizedActor
                                | AuthorityServiceError::ActorNotFound,
                            )
                            | DocsError::AuthorityUnavailable(_)
                            | DocsError::Access(_)
                            | DocsError::Repository(_)
                            | DocsError::DocNotFound
                            | DocsError::UnknownHeads { .. }
                            | DocsError::Pending { .. }
                            | DocsError::ResourceRefused { .. },
                        ),
                    ) => return Ok(Prepared::Parked),
                    Err(error) => {
                        return Err(ferr!("blob projection read failed for {route:?}: {error}"));
                    }
                };
                if let At::Heads(mut requested) = at {
                    requested.sort_unstable();
                    requested.dedup();
                    read.heads.sort_unstable();
                    read.heads.dedup();
                    assert_eq!(
                        read.heads, requested,
                        "Docs must read the requested exact heads"
                    );
                }
                let datum = read
                    .datums
                    .remove(&route.datum_key)
                    .expect("Docs returns every requested datum");
                Ok(reference_from_read(route, datum))
            }
            Some(DatumMembership::Removed { .. }) | None => Ok(Prepared::Ready(None)),
        }
    }

    async fn process(
        &self,
        docs: &Client<DocsProtocol>,
        capability: &CapId,
        entry: &FrontierEntry<DatumRoute, DatumMembership>,
        source: ReadSource,
        progress: Progress,
        shutdown: &mut (impl Future<Output = ()> + Unpin + Send),
    ) -> Res<ControlFlow<()>> {
        let prepared = tokio::select! {
            biased;
            () = shutdown => return Ok(ControlFlow::Break(())),
            result = self.prepare(docs, capability, entry, source) => result?,
        };
        match prepared {
            Prepared::Ready(reference) => {
                self.settle(&entry.key, reference, entry.revision, progress)
                    .await?;
            }
            Prepared::Parked => self.park(entry, progress).await?,
        }
        Ok(ControlFlow::Continue(()))
    }

    async fn retry_parked(
        &self,
        docs: &Client<DocsProtocol>,
        capability: &CapId,
        before: Option<u64>,
        shutdown: &mut (impl Future<Output = ()> + Unpin + Send),
    ) -> Res<ControlFlow<()>> {
        let entries = tokio::select! {
            biased;
            () = &mut *shutdown => return Ok(ControlFlow::Break(())),
            entries = self.parked(before) => entries?,
        };
        for entry in entries {
            if self
                .process(
                    docs,
                    capability,
                    &entry,
                    ReadSource::Current,
                    Progress::Incomplete,
                    shutdown,
                )
                .await?
                .is_break()
            {
                return Ok(ControlFlow::Break(()));
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    pub async fn run(
        &self,
        datum_set: Arc<DatumSetStore>,
        docs: Client<DocsProtocol>,
        capability: CapId,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Res<()> {
        let mut reader = datum_set
            .open(DatumTag::new(Blob::ID)?, self.source_revision().await?)
            .await?;
        self.consume(&mut reader, &docs, &capability, shutdown)
            .await
    }

    async fn consume(
        &self,
        reader: &mut impl RevisionedStoreReader<
            u64,
            FrontierEntry<DatumRoute, DatumMembership>,
            KeyedFrontierError,
        >,
        docs: &Client<DocsProtocol>,
        capability: &CapId,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Res<()> {
        tokio::pin!(shutdown);
        if self
            .retry_parked(docs, capability, None, &mut shutdown)
            .await?
            .is_break()
        {
            return Ok(());
        }
        loop {
            let read = tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                () = self.rearm.notified() => {
                    if self.retry_parked(docs, capability, None, &mut shutdown).await?.is_break() {
                        return Ok(());
                    }
                    continue;
                }
                read = reader.next(RevisionReadLimits::default()) => read?,
            };
            let through = match &read {
                RevisionRead::Entries { revision, .. } => *revision,
                RevisionRead::ReplayComplete { through } => *through,
            };
            match read {
                RevisionRead::Entries { revision, entries } if !entries.is_empty() => {
                    let mut entries = entries.into_iter().peekable();
                    while let Some(entry) = entries.next() {
                        let progress = if entries.peek().is_none() {
                            Progress::Complete(revision)
                        } else {
                            Progress::Incomplete
                        };
                        if self
                            .process(
                                docs,
                                capability,
                                &entry,
                                ReadSource::Frontier,
                                progress,
                                &mut shutdown,
                            )
                            .await?
                            .is_break()
                        {
                            return Ok(());
                        }
                    }
                }
                RevisionRead::Entries { revision, .. }
                | RevisionRead::ReplayComplete { through: revision } => {
                    let transaction = self.sql.begin_write().await?;
                    Self::advance_in_context(&transaction, Progress::Complete(revision)).await?;
                    transaction.commit().await?;
                }
            }
            if self
                .retry_parked(docs, capability, Some(through), &mut shutdown)
                .await?
                .is_break()
            {
                return Ok(());
            }
        }
    }
}

#[async_trait]
impl RevisionedStore for BlobProjection {
    type Revision = u64;
    type Entry = FrontierEntry<DatumRoute, BlobReference>;
    type Selector = ();
    type Error = KeyedFrontierError;
    type Reader<'a> = KeyedFrontierRevisionReader<'a, DatumRoute, BlobReference>;

    async fn latest_revision(&self) -> Result<u64, Self::Error> {
        self.frontier.latest_revision().await
    }

    async fn open<'a>(&'a self, (): (), after: u64) -> Result<Self::Reader<'a>, Self::Error> {
        BlobProjection::open(self, after).await
    }
}

fn reference_from_read(route: &DatumRoute, datum: DatumRead) -> Prepared {
    match datum {
        DatumRead::Accepted { value, .. } => {
            let Ok(blob) = serde_json::from_value::<Blob>(value) else {
                return Prepared::Ready(None);
            };
            Prepared::Ready(plaintext_blob_id(&blob).map(|blob_id| BlobReference {
                route: route.clone(),
                blob_id,
                length_octets: blob.length_octets,
            }))
        }
        DatumRead::Absent { .. } | DatumRead::Rejected { .. } | DatumRead::Undecodable { .. } => {
            Prepared::Ready(None)
        }
        DatumRead::Pending { .. } => Prepared::Parked,
    }
}

fn plaintext_blob_id(blob: &Blob) -> Option<BlobId> {
    blob.digest.parse().ok().or_else(|| {
        blob.urls
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find_map(|text| {
                let url = text.parse::<url::Url>().ok()?;
                if !matches!(url.scheme(), "db+blob" | "daybook-blob") {
                    return None;
                }
                url.path().trim_start_matches('/').parse().ok()
            })
    })
}

fn reference_from_row(row: Row) -> Res<BlobReference> {
    let bytes: [u8; 32] = row
        .get::<Vec<u8>>("blob_id")?
        .try_into()
        .expect("blob projection stores 32-byte digests");
    Ok(BlobReference {
        route: DatumRoute {
            doc_id: row.get::<String>("doc_id")?.parse()?,
            datum_key: row.get::<String>("datum_key")?.parse()?,
        },
        blob_id: BlobId::new(bytes),
        length_octets: row.get::<String>("length_octets")?.parse()?,
    })
}

#[cfg(test)]
mod tests;
