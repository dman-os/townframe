//! The declared-reference index: which facet values point at which other facets.
//!
//! facet_ref is a pure consumer of the facet-set frontier, narrowed to the tags whose schema
//! declares at least one `reference` position ([`SchemaRepo::reference_tags`]). For every live
//! route of those tags it loads the origin document, verifies the facet's value against the gate
//! — only a value that is exactly what its signatures cover may publish edges — hydrates it with
//! its schema, and records every reference the schema declares. Edges are replaced per route, so
//! replay and re-settlement are idempotent.
//!
//! A route whose origin document is not yet local is parked and retried when the repository says
//! the document materialized. A parked route is durable (`big_cabinet_facet_ref_parked`) because
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
use big_cabinet_core::FacetTag;
use big_cabinet_core::facet::CompiledSchema;
use big_cabinet_core::facet::declared_references;
use big_cabinet_core::facet::hydrate_value;
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

use super::facet_set::FacetMembership;
use super::facet_set::FacetRoute;
use super::facet_set::FacetSetStore;
use crate::doc::gate::Gate;
use crate::doc::gate::Verdict;
use crate::schema::SchemaRepo;

mod queries {
    include!("facet_ref/sqlite/gen.rs");
}

/// One recorded reference: a position in one facet's value that addresses another facet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetRefEdge {
    /// The document and facet the reference was found in.
    pub origin: FacetRoute,
    /// Where in the origin facet's value the reference sits, as a JSON pointer.
    pub path: String,
    /// The addressed facet; a `self` reference resolves against the origin document.
    pub target: FacetRoute,
    /// The frontier the sibling `refHeads` pins the target to; empty follows the target's
    /// current frontier.
    pub ref_heads: Vec<ChangeHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FacetRefQuery {
    /// Every edge out of one facet route.
    Origin(FacetRoute),
    /// Every edge out of any facet in one document.
    OriginDoc(big_cabinet_core::DocId),
    /// Every edge pointing at one facet route.
    Target(FacetRoute),
    /// Every edge pointing at any facet in one document.
    TargetDoc(big_cabinet_core::DocId),
}

#[derive(Clone)]
pub struct FacetRefStore {
    sql: Database,
    /// Bumped after every committed settle, so a test can wait for the edge set it expects by
    /// re-querying on each change instead of polling the table.
    #[cfg(test)]
    settled: Arc<tokio::sync::watch::Sender<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Parked {
    membership: FacetMembership,
    revision: u64,
}

impl FacetRefStore {
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

    pub async fn query(&self, selector: FacetRefQuery) -> Res<Vec<FacetRefEdge>> {
        let mut query = Sql::new(
            r#"SELECT origin_doc
                 , origin_facet
                 , origin_path
                 , target_doc
                 , target_facet
                 , ref_heads
              FROM big_cabinet_facet_ref_edges
             WHERE "#,
        );
        match &selector {
            FacetRefQuery::Origin(route) => {
                query
                    .push("origin_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND origin_facet = ")
                    .bind(route.facet_key.to_string());
            }
            FacetRefQuery::OriginDoc(doc_id) => {
                query.push("origin_doc = ").bind(doc_id.to_string());
            }
            FacetRefQuery::Target(route) => {
                query
                    .push("target_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND target_facet = ")
                    .bind(route.facet_key.to_string());
            }
            FacetRefQuery::TargetDoc(doc_id) => {
                query.push("target_doc = ").bind(doc_id.to_string());
            }
        }
        query.push(" ORDER BY origin_doc, origin_facet, origin_path, target_doc, target_facet");
        let rows = query.query(&self.sql).await?.rows;
        rows.into_iter()
            .map(|row| {
                Ok(FacetRefEdge {
                    origin: FacetRoute {
                        doc_id: row.get::<String>("origin_doc")?.parse()?,
                        facet_key: row.get::<String>("origin_facet")?.parse()?,
                    },
                    path: row.get("origin_path")?,
                    target: FacetRoute {
                        doc_id: row.get::<String>("target_doc")?.parse()?,
                        facet_key: row.get::<String>("target_facet")?.parse()?,
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
                    .map_err(|_| ferr!("facet-ref revision is out of range"))?;
                Ok((row.facet_tag, revision))
            })
            .collect()
    }

    /// The routes a previous run parked, newest revision per route.
    async fn parked(&self) -> Res<BTreeMap<FacetRoute, Parked>> {
        queries::parked(&self.sql)
            .await?
            .into_iter()
            .map(|row| {
                let route = FacetRoute {
                    doc_id: row.origin_doc.parse()?,
                    facet_key: row.origin_facet.parse()?,
                };
                let revision = u64::try_from(row.revision)
                    .map_err(|_| ferr!("facet-ref parked revision is out of range"))?;
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
        route: FacetRoute,
        membership: FacetMembership,
        revision: u64,
        parked: &mut BTreeMap<FacetRoute, Parked>,
    ) -> Res<()> {
        queries::park(
            &self.sql,
            route.doc_id.to_string(),
            route.facet_key.to_string(),
            i64::try_from(revision).map_err(|_| ferr!("facet-ref revision is out of range"))?,
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
        route: &FacetRoute,
        edges: &[FacetRefEdge],
        revision: u64,
        parked: &mut BTreeMap<FacetRoute, Parked>,
    ) -> Res<()> {
        let transaction = self.sql.begin_write().await?;
        queries::delete_edges(
            &transaction,
            route.doc_id.to_string(),
            route.facet_key.to_string(),
        )
        .await?;
        queries::delete_parked(
            &transaction,
            route.doc_id.to_string(),
            route.facet_key.to_string(),
        )
        .await?;
        for edge in edges {
            queries::insert_edge(
                &transaction,
                edge.origin.doc_id.to_string(),
                edge.origin.facet_key.to_string(),
                edge.path.clone(),
                edge.target.doc_id.to_string(),
                edge.target.facet_key.to_string(),
                serde_json::to_vec(&edge.ref_heads)?,
            )
            .await?;
        }
        queries::advance_cursor(
            &transaction,
            route.facet_key.tag().to_string(),
            i64::try_from(revision).map_err(|_| ferr!("facet-ref revision is out of range"))?,
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
        route: FacetRoute,
        membership: Option<FacetMembership>,
        revision: u64,
        parked: &mut BTreeMap<FacetRoute, Parked>,
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
        let FacetMembership::Present { doc_heads, .. } = &membership else {
            return self.settle(&route, &[], revision, parked).await;
        };
        let tag = route.facet_key.tag().as_str();
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

    /// Consume the facet-set frontier for every reference-declaring tag, forever.
    pub async fn run(
        &self,
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        facet_set: FacetSetStore,
    ) -> Res<()> {
        self.run_until(repo, schemas, facet_set, std::future::pending(), &mut None)
            .await
    }

    pub(crate) async fn run_until(
        &self,
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        facet_set: FacetSetStore,
        shutdown: impl std::future::Future<Output = ()> + Send,
        ready: &mut Option<tokio::sync::oneshot::Sender<Res<()>>>,
    ) -> Res<()> {
        let cursors = self.cursors().await?;
        let tags: Vec<(FacetTag, u64)> = schemas
            .reference_tags()
            .into_iter()
            .filter_map(|tag| {
                let cursor = cursors.get(&tag).copied().unwrap_or(0);
                FacetTag::new(tag).ok().map(|tag| (tag, cursor))
            })
            .collect();
        let mut reader = facet_set.open_tags(tags).await?;
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
                    for notification in notifications.ok_or_else(|| ferr!("facet-ref materialization listener closed"))? {
                        let doc_id = match notification {
                            BigRepoLocalNotification::DocCreated { doc_id, .. }
                            | BigRepoLocalNotification::DocImported { doc_id, .. }
                            | BigRepoLocalNotification::DocHeadsUpdated { doc_id, .. }
                            | BigRepoLocalNotification::DocMaterializationReady { doc_id, .. } => doc_id,
                            BigRepoLocalNotification::DocMaterializationPending { .. } => continue,
                        };
                        let retry: Vec<FacetRoute> = parked
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
/// Only a gate-accepted value publishes edges: a rejected or pending facet's value is not what
/// its signatures cover, and an accepted but undecodable value states nothing the schema can read.
fn edges_from(
    doc: &mut Automerge,
    route: &FacetRoute,
    heads: &[ChangeHash],
    schema: &CompiledSchema,
) -> Res<Option<Vec<FacetRefEdge>>> {
    if !doc.get_missing_deps(heads).is_empty() {
        return Ok(None);
    }
    let Verdict::Accepted {
        native: Some(_), ..
    } = Gate::new(doc, heads).verify(&route.facet_key)
    else {
        return Ok(Some(Vec::new()));
    };
    let tx = doc
        .transaction_at(PatchLog::null(), heads)
        .expect("a null patch log matches any document");
    let edges = (|| -> Res<Vec<FacetRefEdge>> {
        let Some((Value::Object(ObjType::Map), facets)) = tx.get(automerge::ROOT, "facets")? else {
            return Ok(Vec::new());
        };
        let value = match hydrate_value(
            &tx,
            &facets,
            route.facet_key.to_string().as_str().into(),
            schema,
        ) {
            Ok(value) => value,
            Err(_) => return Ok(Vec::new()),
        };
        Ok(declared_references(schema, &value)
            .into_iter()
            .map(|found| FacetRefEdge {
                origin: route.clone(),
                path: found.path,
                target: FacetRoute {
                    doc_id: found.reference.document().resolve(&route.doc_id),
                    facet_key: found.reference.key().clone(),
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
