mod sqlite;
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
        sql.apply_migrations(
            sqlite::queries::MIGRATION_TABLE,
            sqlite::queries::MIGRATIONS,
        )
        .await?;
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
        sqlite::queries::query_digest(&self.sql, blob_id.to_bytes32().to_vec())
            .await?
            .into_iter()
            .map(|row| {
                Ok(DatumRoute {
                    doc_id: row.doc_id.parse()?,
                    datum_key: row.datum_key.parse()?,
                })
            })
            .collect::<Res<Vec<_>>>()
    }

    pub async fn query_doc(&self, doc_id: &DocId) -> Res<Vec<BlobReference>> {
        sqlite::queries::query_doc(&self.sql, doc_id.to_string())
            .await?
            .into_iter()
            .map(|row| {
                let bytes: [u8; 32] = row
                    .blob_id
                    .try_into()
                    .expect("blob projection stores 32-byte digests");
                Ok(BlobReference {
                    route: DatumRoute {
                        doc_id: row.doc_id.parse()?,
                        datum_key: row.datum_key.parse()?,
                    },
                    blob_id: BlobId::new(bytes),
                    length_octets: row.length_octets.parse()?,
                })
            })
            .collect::<Res<Vec<_>>>()
    }

    pub async fn source_revision(&self) -> Res<u64> {
        let rows = sqlite::queries::source_revision(&self.sql).await?;
        Ok(u64::try_from(rows[0])?)
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
        let rows = sqlite::queries::superseded(
            executor,
            route.doc_id.to_string(),
            route.datum_key.to_string(),
        )
        .await?;
        for row in rows {
            let stored = u64::try_from(row.source_revision)?;
            if stored > revision || (stored == revision && row.parked == 0) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    async fn parked(
        &self,
        before: Option<u64>,
    ) -> Res<Vec<FrontierEntry<DatumRoute, DatumMembership>>> {
        sqlite::queries::parked(&self.sql, before.map(i64::try_from).transpose()?)
            .await?
            .into_iter()
            .map(|row| {
                Ok(FrontierEntry {
                    revision: u64::try_from(row.source_revision)?,
                    key: DatumRoute {
                        doc_id: row.doc_id.parse()?,
                        datum_key: row.datum_key.parse()?,
                    },
                    value: Some(serde_json::from_slice(&row.membership)?),
                })
            })
            .collect::<Res<Vec<_>>>()
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
            sqlite::queries::insert_parked(
                &transaction,
                entry.key.doc_id.to_string(),
                entry.key.datum_key.to_string(),
                i64::try_from(entry.revision)?,
                serde_json::to_vec(membership)?,
            )
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
        sqlite::queries::projected_revision(
            executor,
            route.doc_id.to_string(),
            route.datum_key.to_string(),
        )
        .await?
        .into_iter()
        .next()
        .map(|row| Ok(u64::try_from(row)?))
        .transpose()
    }

    async fn advance_in_context(transaction: &Tx, progress: Progress) -> Res<()> {
        if let Progress::Complete(revision) = progress {
            sqlite::queries::advance(transaction, i64::try_from(revision)?).await?;
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
            sqlite::queries::settle(
                transaction,
                route.doc_id.to_string(),
                route.datum_key.to_string(),
                i64::try_from(revision)?,
                reference
                    .as_ref()
                    .map(|value| value.blob_id.to_bytes32().to_vec()),
                reference
                    .as_ref()
                    .map(|value| value.length_octets.to_string()),
            )
            .await?;
            sqlite::queries::delete_parked(
                transaction,
                route.doc_id.to_string(),
                route.datum_key.to_string(),
            )
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

#[cfg(test)]
mod tests;
