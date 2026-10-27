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
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use big_cabinet_core::DocId;
    use big_cabinet_core::FacetKey;

    fn route(doc: u8, name: &str) -> FacetRoute {
        FacetRoute {
            doc_id: DocId::new([doc; 32]),
            facet_key: FacetKey::from_text("org.example.note", name).unwrap(),
        }
    }

    fn edge(origin: &FacetRoute, path: &str, target: &FacetRoute, heads: &[u8]) -> FacetRefEdge {
        FacetRefEdge {
            origin: origin.clone(),
            path: path.to_owned(),
            target: target.clone(),
            ref_heads: heads.iter().map(|byte| ChangeHash([*byte; 32])).collect(),
        }
    }

    #[cfg(test)]
    async fn progress(store: &FacetRefStore, tag: &str) -> Option<u64> {
        store.cursors().await.unwrap().get(tag).copied()
    }

    #[tokio::test]
    async fn settlement_replaces_edges_serves_both_directions_and_never_regresses() {
        let sql = sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap();
        let store = FacetRefStore::new(sql.clone()).await.unwrap();
        let mut parked = BTreeMap::new();
        let origin = route(1, "main");
        let first = route(2, "first");
        let second = route(3, "second");
        let third = route(4, "third");
        store
            .settle(
                &origin,
                &[
                    edge(&origin, "/link", &first, &[7; 2]),
                    edge(&origin, "/nested/inner", &second, &[]),
                ],
                1,
                &mut parked,
            )
            .await
            .unwrap();
        assert_eq!(progress(&store, "org.example.note").await, Some(1));
        assert_eq!(
            store
                .query(FacetRefQuery::Origin(origin.clone()))
                .await
                .unwrap(),
            vec![
                edge(&origin, "/link", &first, &[7; 2]),
                edge(&origin, "/nested/inner", &second, &[]),
            ]
        );
        assert_eq!(
            store
                .query(FacetRefQuery::Target(first.clone()))
                .await
                .unwrap(),
            vec![edge(&origin, "/link", &first, &[7; 2])]
        );
        assert_eq!(
            store
                .query(FacetRefQuery::OriginDoc(origin.doc_id.clone()))
                .await
                .unwrap(),
            vec![
                edge(&origin, "/link", &first, &[7; 2]),
                edge(&origin, "/nested/inner", &second, &[]),
            ]
        );
        assert_eq!(
            store
                .query(FacetRefQuery::TargetDoc(first.doc_id.clone()))
                .await
                .unwrap(),
            vec![edge(&origin, "/link", &first, &[7; 2])]
        );
        store
            .settle(
                &origin,
                &[edge(&origin, "/link", &third, &[])],
                2,
                &mut parked,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .query(FacetRefQuery::Origin(origin.clone()))
                .await
                .unwrap(),
            vec![edge(&origin, "/link", &third, &[])]
        );
        assert!(
            store
                .query(FacetRefQuery::Target(first.clone()))
                .await
                .unwrap()
                .is_empty()
        );
        // A replayed or late settlement re-replaces idempotently and the cursor never moves back.
        store.settle(&origin, &[], 1, &mut parked).await.unwrap();
        assert!(
            store
                .query(FacetRefQuery::Origin(origin.clone()))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(progress(&store, "org.example.note").await, Some(2));
        let reopened = FacetRefStore::new(sql).await.unwrap();
        assert_eq!(progress(&reopened, "org.example.note").await, Some(2));
    }

    #[tokio::test]
    async fn failed_settlement_preserves_edges_cursor_and_parked_replay_without_notification() {
        let sql = sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap();
        let store = FacetRefStore::new(sql.clone()).await.unwrap();
        let origin = route(1, "main");
        let original = vec![edge(&origin, "/link", &route(2, "first"), &[7])];
        let replacement = vec![edge(&origin, "/link", &route(3, "second"), &[])];
        let mut parked = BTreeMap::new();
        store
            .settle(&origin, &original, 3, &mut parked)
            .await
            .unwrap();
        store
            .park(
                origin.clone(),
                present(vec![ChangeHash([9; 32])]),
                5,
                &mut parked,
            )
            .await
            .unwrap();
        let expected_parked = parked.clone();
        let mut settled = store.subscribe_settled();
        sql.execute_batch("CREATE TRIGGER reject_facet_ref BEFORE INSERT ON big_cabinet_facet_ref_progress BEGIN SELECT RAISE(ABORT, 'injected'); END")
            .await.unwrap();
        assert!(
            store
                .settle(&origin, &replacement, 6, &mut parked)
                .await
                .is_err()
        );
        assert_eq!(
            store
                .query(FacetRefQuery::Origin(origin.clone()))
                .await
                .unwrap(),
            original
        );
        assert_eq!(
            store.cursors().await.unwrap(),
            BTreeMap::from([("org.example.note".into(), 3)])
        );
        assert_eq!(store.parked().await.unwrap(), expected_parked);
        assert_eq!(parked, expected_parked);
        assert!(!settled.has_changed().unwrap());
        sql.execute_batch("DROP TRIGGER reject_facet_ref")
            .await
            .unwrap();
        store
            .settle(&origin, &replacement, 6, &mut parked)
            .await
            .unwrap();
        settled.changed().await.unwrap();
        assert_eq!(
            store.query(FacetRefQuery::Origin(origin)).await.unwrap(),
            replacement
        );
        assert_eq!(
            store.cursors().await.unwrap(),
            BTreeMap::from([("org.example.note".into(), 6)])
        );
        assert!(store.parked().await.unwrap().is_empty());
        assert!(parked.is_empty());
    }

    /// A document carrying one unsigned branch facet: a reference-declaring tag whose value the
    /// gate refuses, so it settles with no edges and exercises the route plumbing alone.
    fn branch_document() -> (Automerge, FacetKey) {
        let facet_key =
            FacetKey::from_text(big_cabinet_core::dmeta::BRANCH_FACET_TAG, "main").unwrap();
        let mut document = Automerge::new();
        let mut tx = document.transaction();
        let facets = tx
            .put_object(automerge::ROOT, "facets", ObjType::Map)
            .unwrap();
        let facet = tx
            .put_object(&facets, facet_key.to_string(), ObjType::Map)
            .unwrap();
        tx.put(&facet, "from", "db+facet:/self/org.example.note/main")
            .unwrap();
        tx.commit();
        (document, facet_key)
    }

    fn present(heads: Vec<ChangeHash>) -> FacetMembership {
        FacetMembership::Present {
            doc_heads: heads,
            facet_heads: Vec::new(),
            clock: 0,
        }
    }

    async fn close(repo: SharedBigRepo, stop: big_repo::BigRepoStopToken, sql: Database) {
        drop(repo);
        stop.stop().await.unwrap();
        drop(sql);
    }

    #[tokio::test]
    async fn a_newer_revision_releases_or_outranks_a_parked_route_and_parking_survives_reopen() {
        let schemas = SchemaRepo::open(Vec::new()).unwrap();
        let (document, facet_key) = branch_document();
        let (_directory, repo, stop, doc_id) =
            crate::index::test_support::reopen_document(document).await;
        let sql = sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap();
        let store = FacetRefStore::new(sql.clone()).await.unwrap();
        let target = route(2, "other");
        let route = FacetRoute { doc_id, facet_key };
        let unknown = present(vec![ChangeHash([9; 32])]);
        let tag = big_cabinet_core::dmeta::BRANCH_FACET_TAG;
        let mut parked = BTreeMap::new();

        // Revision 1 parks (the heads are not local); revision 2 settles the route and releases
        // the parked entry, so no later materialization can retry revision 1.
        store
            .process(
                &repo,
                &schemas,
                route.clone(),
                Some(unknown.clone()),
                1,
                &mut parked,
            )
            .await
            .unwrap();
        assert_eq!(parked[&route].revision, 1);
        assert_eq!(store.parked().await.unwrap(), parked);
        store
            .settle(
                &route,
                &[edge(&route, "/link", &target, &[])],
                2,
                &mut parked,
            )
            .await
            .unwrap();
        assert!(parked.is_empty());
        assert!(store.parked().await.unwrap().is_empty());
        assert_eq!(progress(&store, tag).await, Some(2));

        // Revision 3 parks; a replay of revision 2 (a removal) is outranked by it: the edges
        // revision 2 settled stay and the parked entry is untouched.
        store
            .process(
                &repo,
                &schemas,
                route.clone(),
                Some(unknown.clone()),
                3,
                &mut parked,
            )
            .await
            .unwrap();
        store
            .process(
                &repo,
                &schemas,
                route.clone(),
                Some(FacetMembership::Removed {
                    doc_heads: None,
                    facet_heads: Vec::new(),
                    clock: 0,
                }),
                2,
                &mut parked,
            )
            .await
            .unwrap();
        assert_eq!(
            parked,
            BTreeMap::from([(
                route.clone(),
                Parked {
                    membership: unknown.clone(),
                    revision: 3,
                }
            )])
        );
        assert_eq!(
            store
                .query(FacetRefQuery::Origin(route.clone()))
                .await
                .unwrap(),
            vec![edge(&route, "/link", &target, &[])]
        );
        assert_eq!(progress(&store, tag).await, Some(2));

        // A tombstone for the route at revision 4 clears both the edges and the parked entry.
        store
            .process(&repo, &schemas, route.clone(), None, 4, &mut parked)
            .await
            .unwrap();
        assert!(parked.is_empty());
        assert!(
            store
                .query(FacetRefQuery::Origin(route.clone()))
                .await
                .unwrap()
                .is_empty()
        );

        // A parked route outlives the store: a reopened store sees it and retries it the way the
        // run loop does at boot, and it stays parked while the heads are still not local.
        store
            .process(
                &repo,
                &schemas,
                route.clone(),
                Some(unknown.clone()),
                5,
                &mut parked,
            )
            .await
            .unwrap();
        drop(store);
        let reopened = FacetRefStore::new(sql.clone()).await.unwrap();
        let mut reloaded = reopened.parked().await.unwrap();
        assert_eq!(reloaded, parked);
        for (
            route,
            Parked {
                membership,
                revision,
            },
        ) in std::mem::take(&mut reloaded)
        {
            reopened
                .process(
                    &repo,
                    &schemas,
                    route,
                    Some(membership),
                    revision,
                    &mut reloaded,
                )
                .await
                .unwrap();
        }
        assert_eq!(reloaded, parked);
        assert_eq!(progress(&reopened, tag).await, Some(4));
        drop(reopened);
        close(repo, stop, sql).await;
    }

    #[tokio::test]
    async fn reference_lookup_does_not_acquire_sync_demand() {
        let schemas = SchemaRepo::open(Vec::new()).unwrap();
        let (document, facet_key) = branch_document();
        let heads = document.get_heads();
        let (_directory, repo, stop, doc_id) =
            crate::index::test_support::reopen_document(document).await;
        let mut demand = repo.sync_demand();
        let sql = sqlite_rs::SqliteRuntime::new(Default::default())
            .ephemeral(&[])
            .unwrap();
        let store = FacetRefStore::new(sql.clone()).await.unwrap();
        let route = FacetRoute {
            doc_id: doc_id.clone(),
            facet_key,
        };
        let mut parked = BTreeMap::new();
        store
            .process(
                &repo,
                &schemas,
                route.clone(),
                Some(FacetMembership::Present {
                    doc_heads: heads,
                    facet_heads: Vec::new(),
                    clock: 0,
                }),
                1,
                &mut parked,
            )
            .await
            .unwrap();
        assert!(parked.is_empty());
        assert_eq!(
            progress(&store, big_cabinet_core::dmeta::BRANCH_FACET_TAG).await,
            Some(1)
        );
        assert!(
            store
                .query(FacetRefQuery::Origin(route.clone()))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(demand.documents().is_empty());
        let pending = FacetMembership::Present {
            doc_heads: vec![ChangeHash([9; 32])],
            facet_heads: Vec::new(),
            clock: 0,
        };
        store
            .process(
                &repo,
                &schemas,
                route.clone(),
                Some(pending.clone()),
                2,
                &mut parked,
            )
            .await
            .unwrap();
        assert_eq!(parked.len(), 1);
        assert_eq!(parked[&route].membership, pending);
        assert_eq!(parked[&route].revision, 2);
        assert!(demand.documents().is_empty());
        let mut changed = Box::pin(demand.changed());
        assert!(futures::poll!(&mut changed).is_pending());
        drop(changed);
        let caller = repo
            .get_doc(&doc_id)
            .await
            .unwrap()
            .into_ready(doc_id.clone())
            .unwrap();
        assert_eq!(
            *demand.documents(),
            std::collections::HashSet::from([doc_id])
        );
        drop(caller);
        drop(store);
        close(repo, stop, sql).await;
    }

    #[test]
    fn an_unsigned_value_publishes_no_edges() {
        let schema = CompiledSchema::compile(serde_json::json!({
            "type": "object",
            "properties": {"link": {"type": "string", "x-automerge": "reference"}},
        }))
        .unwrap();
        let mut doc = Automerge::new();
        let mut tx = doc.transaction();
        let facets = tx
            .put_object(automerge::ROOT, "facets", ObjType::Map)
            .unwrap();
        let facet = tx
            .put_object(&facets, "org.example.note/main", ObjType::Map)
            .unwrap();
        tx.put(&facet, "link", "db+facet:/self/org.example.note/other")
            .unwrap();
        tx.commit();
        let heads = doc.get_heads();
        // The gate rejects a value no signature covers; nothing it states becomes an edge.
        assert_eq!(
            edges_from(&mut doc, &route(1, "main"), &heads, &schema).unwrap(),
            Some(Vec::new())
        );
        // Heads the document does not have park the route instead.
        assert_eq!(
            edges_from(&mut doc, &route(1, "main"), &[ChangeHash([9; 32])], &schema).unwrap(),
            None
        );
    }
}
