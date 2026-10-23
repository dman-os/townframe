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
//! the document materialized. Parked entries are in-memory: a restart loses them (their tag
//! cursor has already moved past), and the edge is then rebuilt on the origin's next change.
//! That is the same derived-state trade the whole index makes — nothing here is authoritative.

use automerge::{Automerge, ChangeHash, ObjType, PatchLog, ReadDoc, Value};
use big_cabinet_core::FacetTag;
use big_cabinet_core::facet::{CompiledSchema, declared_references, hydrate_value};
use big_repo::{BigRepoLocalFilter, BigRepoLocalNotification, DocLookup, SharedBigRepo};
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits, RevisionedStoreReader};
use sqlx::{QueryBuilder, Row, Sqlite};
use sqlx_utils_rs::SqlCtx;
use std::collections::BTreeMap;
use std::sync::Arc;
use utils_rs::prelude::{Res, ferr};

use super::facet_set::{FacetMembership, FacetRoute, FacetSetStore};
use crate::doc::gate::{Gate, Verdict};
use crate::schema::SchemaRepo;

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
    sql: SqlCtx,
}

struct Parked {
    membership: FacetMembership,
    revision: u64,
}

impl FacetRefStore {
    pub async fn new(sql: SqlCtx) -> Res<Self> {
        for statement in [
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_ref_edges (
                origin_doc TEXT NOT NULL
                , origin_facet TEXT NOT NULL
                , origin_path TEXT NOT NULL
                , target_doc TEXT NOT NULL
                , target_facet TEXT NOT NULL
                , ref_heads BLOB NOT NULL
                , PRIMARY KEY (origin_doc, origin_facet, origin_path, target_doc, target_facet)
            ) STRICT"#,
            r#"CREATE INDEX IF NOT EXISTS big_cabinet_facet_ref_edges_origin_idx
                 ON big_cabinet_facet_ref_edges (origin_doc, origin_facet)"#,
            r#"CREATE INDEX IF NOT EXISTS big_cabinet_facet_ref_edges_target_idx
                 ON big_cabinet_facet_ref_edges (target_doc, target_facet)"#,
            r#"CREATE TABLE IF NOT EXISTS big_cabinet_facet_ref_progress (
                facet_tag TEXT PRIMARY KEY NOT NULL
                , revision INTEGER NOT NULL
            ) STRICT"#,
        ] {
            sqlx::query(statement).execute(&sql.write_pool).await?;
        }
        Ok(Self { sql })
    }

    pub async fn query(&self, selector: FacetRefQuery) -> Res<Vec<FacetRefEdge>> {
        let mut query = QueryBuilder::<Sqlite>::new(
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
                    .push_bind(route.doc_id.to_string())
                    .push(" AND origin_facet = ")
                    .push_bind(route.facet_key.to_string());
            }
            FacetRefQuery::OriginDoc(doc_id) => {
                query.push("origin_doc = ").push_bind(doc_id.to_string());
            }
            FacetRefQuery::Target(route) => {
                query
                    .push("target_doc = ")
                    .push_bind(route.doc_id.to_string())
                    .push(" AND target_facet = ")
                    .push_bind(route.facet_key.to_string());
            }
            FacetRefQuery::TargetDoc(doc_id) => {
                query.push("target_doc = ").push_bind(doc_id.to_string());
            }
        }
        query.push(" ORDER BY origin_doc, origin_facet, origin_path, target_doc, target_facet");
        let rows = query.build().fetch_all(&self.sql.read_pool).await?;
        rows.into_iter()
            .map(|row| {
                Ok(FacetRefEdge {
                    origin: FacetRoute {
                        doc_id: row.get::<String, _>("origin_doc").parse()?,
                        facet_key: row.get::<String, _>("origin_facet").parse()?,
                    },
                    path: row.get("origin_path"),
                    target: FacetRoute {
                        doc_id: row.get::<String, _>("target_doc").parse()?,
                        facet_key: row.get::<String, _>("target_facet").parse()?,
                    },
                    ref_heads: serde_json::from_slice(row.get("ref_heads"))?,
                })
            })
            .collect()
    }

    async fn cursors(&self) -> Res<BTreeMap<String, u64>> {
        let rows = sqlx::query("SELECT facet_tag, revision FROM big_cabinet_facet_ref_progress")
            .fetch_all(&self.sql.read_pool)
            .await?;
        rows.into_iter()
            .map(|row| {
                let tag: String = row.get("facet_tag");
                let revision = u64::try_from(row.get::<i64, _>("revision"))
                    .map_err(|_| ferr!("facet-ref revision is out of range"))?;
                Ok((tag, revision))
            })
            .collect()
    }

    /// Replace one route's edges with the ones its current value states, and record that the
    /// route's tag was processed up to `revision`. Both happen in one transaction; a late
    /// settlement (a parked entry that caught up) never moves the cursor backwards.
    async fn settle(&self, route: &FacetRoute, edges: &[FacetRefEdge], revision: u64) -> Res<()> {
        let mut transaction = self.sql.write_pool.begin().await?;
        sqlx::query(
            "DELETE FROM big_cabinet_facet_ref_edges WHERE origin_doc = ? AND origin_facet = ?",
        )
        .bind(route.doc_id.to_string())
        .bind(route.facet_key.to_string())
        .execute(&mut *transaction)
        .await?;
        for edge in edges {
            sqlx::query(
                r#"INSERT INTO big_cabinet_facet_ref_edges (
                    origin_doc
                    , origin_facet
                    , origin_path
                    , target_doc
                    , target_facet
                    , ref_heads
                ) VALUES (?, ?, ?, ?, ?, ?)"#,
            )
            .bind(edge.origin.doc_id.to_string())
            .bind(edge.origin.facet_key.to_string())
            .bind(&edge.path)
            .bind(edge.target.doc_id.to_string())
            .bind(edge.target.facet_key.to_string())
            .bind(serde_json::to_vec(&edge.ref_heads)?)
            .execute(&mut *transaction)
            .await?;
        }
        sqlx::query(
            r#"INSERT INTO big_cabinet_facet_ref_progress (facet_tag, revision) VALUES (?, ?)
               ON CONFLICT (facet_tag) DO UPDATE SET revision = MAX(revision, excluded.revision)"#,
        )
        .bind(route.facet_key.tag().as_str())
        .bind(i64::try_from(revision).map_err(|_| ferr!("facet-ref revision is out of range"))?)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn process(
        &self,
        repo: &SharedBigRepo,
        schemas: &SchemaRepo,
        route: FacetRoute,
        membership: FacetMembership,
        revision: u64,
        parked: &mut BTreeMap<FacetRoute, Parked>,
    ) -> Res<()> {
        let FacetMembership::Present { ref doc_heads, .. } = membership else {
            return self.settle(&route, &[], revision).await;
        };
        let tag = route.facet_key.tag().as_str();
        let Some(schema) = schemas.resolve_for_read(tag) else {
            // The subscription is the reference tags, so this is unreachable in the run loop;
            // an unknown tag has no declared references and settles empty either way.
            return self.settle(&route, &[], revision).await;
        };
        let handle = match repo.get_doc(&route.doc_id).await? {
            DocLookup::Ready(handle) => handle,
            DocLookup::Missing => return self.settle(&route, &[], revision).await,
            DocLookup::PendingMaterialization => {
                parked.insert(
                    route,
                    Parked {
                        membership,
                        revision,
                    },
                );
                return Ok(());
            }
        };
        let edges = handle
            .with_document(|doc| edges_from(doc, &route, doc_heads, schema.compiled()))
            .await?;
        match edges? {
            Some(edges) => self.settle(&route, &edges, revision).await?,
            None => {
                parked.insert(
                    route,
                    Parked {
                        membership,
                        revision,
                    },
                );
            }
        }
        Ok(())
    }

    /// Consume the facet-set frontier for every reference-declaring tag, forever.
    pub async fn run(
        &self,
        repo: SharedBigRepo,
        schemas: Arc<SchemaRepo>,
        facet_set: FacetSetStore,
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
        let mut parked: BTreeMap<FacetRoute, Parked> = BTreeMap::new();
        loop {
            tokio::select! {
                read = reader.next(RevisionReadLimits::default()) => {
                    match read? {
                        RevisionRead::ReplayComplete { .. } => {}
                        RevisionRead::Entries { entries, .. } => {
                            for FrontierEntry { revision, key: route, value } in entries {
                                let Some(membership) = value else { continue };
                                self.process(&repo, &schemas, route, membership, revision, &mut parked).await?;
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
                            self.process(&repo, &schemas, route, membership, revision, &mut parked).await?;
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
        let sql = SqlCtx::memory().await.unwrap();
        let store = FacetRefStore::new(sql.clone()).await.unwrap();
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
        store
            .settle(&origin, &[edge(&origin, "/link", &third, &[])], 2)
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
        store.settle(&origin, &[], 1).await.unwrap();
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
