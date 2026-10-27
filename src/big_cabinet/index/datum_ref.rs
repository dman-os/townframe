//! The declared-reference index: which datum values point at which other datums.
//!
//! datum_ref is a pure consumer of the datum-set frontier, narrowed to the tags whose schema
//! declares at least one `reference` position ([`SchemaRepo::reference_tags`]). For every live
//! route of those tags it loads the origin document, verifies the datum's value against the gate
//! — only a value that is exactly what its signatures cover may publish edges — hydrates it with
//! its schema, and records every reference the schema declares. Edges are replaced per route, so
//! replay and re-settlement are idempotent.
//!
//! A route whose origin document is not yet local is parked and retried when the repository says
//! the document materialized. A parked route is durable (`big_cabinet_datum_ref_parked`) because
//! its tag cursor keeps moving for the other routes of the tag: a restart reloads the parked rows
//! and retries them before consuming the frontier. One route holds at most one parked entry, the
//! newest frontier revision seen for it; a retry of an older revision after a newer one settled or
//! parked is dropped, so a late materialization never replaces newer edges with older ones.

use automerge::Automerge;
use automerge::ChangeHash;
use automerge::ObjType;
use automerge::PatchLog;
use automerge::ReadDoc;
use automerge::Value;
use big_cabinet_core::DatumTag;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::declared_references;
use big_cabinet_core::datum::hydrate_value;
use big_repo::BigRepoLocalFilter;
use big_repo::BigRepoLocalNotification;
use big_repo::DocLookup;
use big_repo::SharedBigRepo;
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use sqlite_rs::Database;
use sqlite_rs::Sql;
use std::collections::BTreeMap;
use std::sync::Arc;
use utils_rs::prelude::Res;
use utils_rs::prelude::ferr;

use super::datum_set::DatumMembership;
use super::datum_set::DatumRoute;
use super::datum_set::DatumSetStore;
use crate::doc::gate::Gate;
use crate::doc::gate::Verdict;
use crate::schema::SchemaRepo;

mod queries {
    include!("datum_ref/sqlite/gen.rs");
}

/// One recorded reference: a position in one datum's value that addresses another datum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatumRefEdge {
    /// The document and datum the reference was found in.
    pub origin: DatumRoute,
    /// Where in the origin datum's value the reference sits, as a JSON pointer.
    pub path: String,
    /// The addressed datum; a `self` reference resolves against the origin document.
    pub target: DatumRoute,
    /// The frontier the sibling `refHeads` pins the target to; empty follows the target's
    /// current frontier.
    pub ref_heads: Vec<ChangeHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatumRefQuery {
    /// Every edge out of one datum route.
    Origin(DatumRoute),
    /// Every edge out of any datum in one document.
    OriginDoc(big_cabinet_core::DocId),
    /// Every edge pointing at one datum route.
    Target(DatumRoute),
    /// Every edge pointing at any datum in one document.
    TargetDoc(big_cabinet_core::DocId),
}

#[derive(Clone)]
pub struct DatumRefStore {
    sql: Database,
    /// Bumped after every committed settle, so a test can wait for the edge set it expects by
    /// re-querying on each change instead of polling the table.
    #[cfg(test)]
    settled: Arc<tokio::sync::watch::Sender<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Parked {
    membership: DatumMembership,
    revision: u64,
}

impl DatumRefStore {
    pub async fn new(sql: Database) -> Res<Self> {
        sql.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        Ok(Self {
            sql,
            #[cfg(test)]
            settled: Arc::new(tokio::sync::watch::channel(0).0),
        })
    }

    /// Changes after every committed settle. Subscribe before querying: a settle that lands
    /// between the query and the wait is then still seen as a change.
    #[cfg(test)]
    pub(crate) fn subscribe_settled(&self) -> tokio::sync::watch::Receiver<u64> {
        self.settled.subscribe()
    }

    pub async fn query(&self, selector: DatumRefQuery) -> Res<Vec<DatumRefEdge>> {
        let mut query = Sql::new(
            r#"SELECT origin_doc
                 , origin_datum
                 , origin_path
                 , target_doc
                 , target_datum
                 , ref_heads
              FROM big_cabinet_datum_ref_edges
             WHERE "#,
        );
        match &selector {
            DatumRefQuery::Origin(route) => {
                query
                    .push("origin_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND origin_datum = ")
                    .bind(route.datum_key.to_string());
            }
            DatumRefQuery::OriginDoc(doc_id) => {
                query.push("origin_doc = ").bind(doc_id.to_string());
            }
            DatumRefQuery::Target(route) => {
                query
                    .push("target_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND target_datum = ")
                    .bind(route.datum_key.to_string());
            }
            DatumRefQuery::TargetDoc(doc_id) => {
                query.push("target_doc = ").bind(doc_id.to_string());
            }
        }
        query.push(" ORDER BY origin_doc, origin_datum, origin_path, target_doc, target_datum");
        let rows = query.query(&self.sql).await?.rows;
        rows.into_iter()
            .map(|row| {
                Ok(DatumRefEdge {
                    origin: DatumRoute {
                        doc_id: row.get::<String>("origin_doc")?.parse()?,
                        datum_key: row.get::<String>("origin_datum")?.parse()?,
                    },
                    path: row.get("origin_path")?,
                    target: DatumRoute {
                        doc_id: row.get::<String>("target_doc")?.parse()?,
                        datum_key: row.get::<String>("target_datum")?.parse()?,
                    },
                    ref_heads: serde_json::from_slice(&row.get::<Vec<u8>>("ref_heads")?)?,
                })
            })
            .collect()
    }

    async fn cursors(&self) -> Res<BTreeMap<String, u64>> {
        queries::cursors(&self.sql)
            .await?
            .into_iter()
            .map(|row| {
                let revision = u64::try_from(row.revision)
                    .map_err(|_| ferr!("datum-ref revision is out of range"))?;
                Ok((row.datum_tag, revision))
            })
            .collect()
    }

    /// The routes a previous run parked, newest revision per route.
    async fn parked(&self) -> Res<BTreeMap<DatumRoute, Parked>> {
        queries::parked(&self.sql)
            .await?
            .into_iter()
            .map(|row| {
                let route = DatumRoute {
                    doc_id: row.origin_doc.parse()?,
                    datum_key: row.origin_datum.parse()?,
                };
                let revision = u64::try_from(row.revision)
                    .map_err(|_| ferr!("datum-ref parked revision is out of range"))?;
                let membership = serde_json::from_slice(&row.membership)?;
                Ok((
                    route,
                    Parked {
                        membership,
                        revision,
                    },
                ))
            })
            .collect()
    }

    /// Hold `route` at `revision` until its origin document materializes. Durable, so a restart
    /// retries it; the in-memory map is the working copy the run loop consults.
    async fn park(
        &self,
        route: DatumRoute,
        membership: DatumMembership,
        revision: u64,
        parked: &mut BTreeMap<DatumRoute, Parked>,
    ) -> Res<()> {
        queries::park(
            &self.sql,
            route.doc_id.to_string(),
            route.datum_key.to_string(),
            i64::try_from(revision).map_err(|_| ferr!("datum-ref revision is out of range"))?,
            serde_json::to_vec(&membership)?,
        )
        .await?;
        parked.insert(
            route,
            Parked {
                membership,
                revision,
            },
        );
        Ok(())
    }

    /// Replace one route's edges with the ones its current value states, record that the route's
    /// tag was processed up to `revision`, and release any parked entry the route held. All of it
    /// is one transaction; a late settlement never moves the cursor backwards.
    async fn settle(
        &self,
        route: &DatumRoute,
        edges: &[DatumRefEdge],
        revision: u64,
        parked: &mut BTreeMap<DatumRoute, Parked>,
    ) -> Res<()> {
        let transaction = self.sql.begin_write().await?;
        queries::delete_edges(
            &transaction,
            route.doc_id.to_string(),
            route.datum_key.to_string(),
        )
        .await?;
        queries::delete_parked(
            &transaction,
            route.doc_id.to_string(),
            route.datum_key.to_string(),
        )
        .await?;
        for edge in edges {
            queries::insert_edge(
                &transaction,
                edge.origin.doc_id.to_string(),
                edge.origin.datum_key.to_string(),
                edge.path.clone(),
                edge.target.doc_id.to_string(),
                edge.target.datum_key.to_string(),
                serde_json::to_vec(&edge.ref_heads)?,
            )
            .await?;
        }
        queries::advance_cursor(
            &transaction,
            route.datum_key.tag().to_string(),
            i64::try_from(revision).map_err(|_| ferr!("datum-ref revision is out of range"))?,
        )
        .await?;
        transaction.commit().await?;
        #[cfg(test)]
        self.settled.send_modify(|settles| *settles += 1);
        parked.remove(route);
        Ok(())
    }

    /// Process one frontier entry for `route`. `None` is the frontier's deletion tombstone.
    ///
    /// A parked entry for the same route at a newer revision wins: this call is then a replay or
    /// a late retry of state the frontier has already moved past, and it is dropped.
    async fn process(
        &self,
        repo: &SharedBigRepo,
        schemas: &SchemaRepo,
        route: DatumRoute,
        membership: Option<DatumMembership>,
        revision: u64,
        parked: &mut BTreeMap<DatumRoute, Parked>,
    ) -> Res<()> {
        if parked
            .get(&route)
            .is_some_and(|held| held.revision > revision)
        {
            return Ok(());
        }
        let Some(membership) = membership else {
            return self.settle(&route, &[], revision, parked).await;
        };
        let DatumMembership::Present { doc_heads, .. } = &membership else {
            return self.settle(&route, &[], revision, parked).await;
        };
        let tag = route.datum_key.tag().as_str();
        let Some(schema) = schemas.resolve_for_read(tag) else {
            // The subscription is the reference tags, so this is unreachable in the run loop;
            // an unknown tag has no declared references and settles empty either way.
            return self.settle(&route, &[], revision, parked).await;
        };
        let handle = match repo.get_doc_for_projection(&route.doc_id).await? {
            DocLookup::Ready(handle) => handle,
            DocLookup::Missing => return self.settle(&route, &[], revision, parked).await,
            DocLookup::PendingMaterialization => {
                return self.park(route, membership, revision, parked).await;
            }
        };
        let edges = handle
            .with_document(|doc| edges_from(doc, &route, doc_heads, schema.compiled()))
            .await?;
        match edges? {
            Some(edges) => self.settle(&route, &edges, revision, parked).await,
            None => self.park(route, membership, revision, parked).await,
        }
    }

    /// Consume the datum-set frontier for every reference-declaring tag, forever.
    pub async fn run(
        &self,
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        datum_set: DatumSetStore,
    ) -> Res<()> {
        self.run_until(repo, schemas, datum_set, std::future::pending(), &mut None)
            .await
    }

    pub(crate) async fn run_until(
        &self,
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        datum_set: DatumSetStore,
        shutdown: impl std::future::Future<Output = ()> + Send,
        ready: &mut Option<tokio::sync::oneshot::Sender<Res<()>>>,
    ) -> Res<()> {
        let cursors = self.cursors().await?;
        let tags: Vec<(DatumTag, u64)> = schemas
            .reference_tags()
            .into_iter()
            .filter_map(|tag| {
                let cursor = cursors.get(&tag).copied().unwrap_or(0);
                DatumTag::new(tag).ok().map(|tag| (tag, cursor))
            })
            .collect();
        let mut reader = datum_set.open_tags(tags).await?;
        let (_registration, mut wakes) = repo
            .subscribe_local_listener(BigRepoLocalFilter { doc_id: None })
            .await?;
        let mut parked = self.parked().await?;
        if let Some(ready) = ready.take() {
            ready.send(Ok(())).expect("index boot waits for readiness");
        }
        // The previous run's parked routes: their documents may have materialized while this
        // index was down, and no notification will ever say so again.
        for (
            route,
            Parked {
                membership,
                revision,
            },
        ) in std::mem::take(&mut parked)
        {
            self.process(
                &repo,
                &schemas,
                route,
                Some(membership),
                revision,
                &mut parked,
            )
            .await?;
        }
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(()),
                read = reader.next(RevisionReadLimits::default()) => {
                    match read? {
                        RevisionRead::ReplayComplete { .. } => {}
                        RevisionRead::Entries { entries, .. } => {
                            for FrontierEntry { revision, key: route, value } in entries {
                                self.process(&repo, &schemas, route, value, revision, &mut parked).await?;
                            }
                        }
                    }
                }
                notifications = wakes.recv() => {
                    for notification in notifications.ok_or_else(|| ferr!("datum-ref materialization listener closed"))? {
                        let doc_id = match notification {
                            BigRepoLocalNotification::DocCreated { doc_id, .. }
                            | BigRepoLocalNotification::DocImported { doc_id, .. }
                            | BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. }
                            | BigRepoLocalNotification::DocMaterializationReady { doc_id, .. } => doc_id,
                            BigRepoLocalNotification::DocMaterializationPending { .. } => continue,
                        };
                        let retry: Vec<DatumRoute> = parked
                            .keys()
                            .filter(|route| route.doc_id == doc_id)
                            .cloned()
                            .collect();
                        for route in retry {
                            let Parked { membership, revision } = parked.remove(&route).expect("key was listed");
                            self.process(&repo, &schemas, route, Some(membership), revision, &mut parked).await?;
                        }
                    }
                }
            }
        }
    }
}

/// The edges one route's value states at `heads`, or `None` when the document does not have those
/// heads yet and the route must be parked.
///
/// Only a gate-accepted value publishes edges: a rejected or pending datum's value is not what
/// its signatures cover, and an accepted but undecodable value states nothing the schema can read.
fn edges_from(
    doc: &mut Automerge,
    route: &DatumRoute,
    heads: &[ChangeHash],
    schema: &CompiledSchema,
) -> Res<Option<Vec<DatumRefEdge>>> {
    if !doc.get_missing_deps(heads).is_empty() {
        return Ok(None);
    }
    let Verdict::Accepted {
        native: Some(_), ..
    } = Gate::new(doc, heads).verify(&route.datum_key)
    else {
        return Ok(Some(Vec::new()));
    };
    let tx = doc
        .transaction_at(PatchLog::null(), heads)
        .expect("a null patch log matches any document");
    let edges = (|| -> Res<Vec<DatumRefEdge>> {
        let Some((Value::Object(ObjType::Map), datums)) = tx.get(automerge::ROOT, "datums")? else {
            return Ok(Vec::new());
        };
        let value = match hydrate_value(
            &tx,
            &datums,
            route.datum_key.to_string().as_str().into(),
            schema,
        ) {
            Ok(value) => value,
            Err(_) => return Ok(Vec::new()),
        };
        Ok(declared_references(schema, &value)
            .into_iter()
            .map(|found| DatumRefEdge {
                origin: route.clone(),
                path: found.path,
                target: DatumRoute {
                    doc_id: found.reference.document().resolve(&route.doc_id),
                    datum_key: found.reference.key().clone(),
                },
                ref_heads: found.heads,
            })
            .collect())
    })();
    tx.rollback();
    edges.map(Some)
}

#[cfg(test)]
mod tests;
