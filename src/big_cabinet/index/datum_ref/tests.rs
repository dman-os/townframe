use super::*;
use big_cabinet_core::DatumKey;
use sqlite_rs::IntoValue;
use std::sync::Arc;

fn route(doc: u8, name: &str) -> DatumRoute {
    DatumRoute {
        doc_id: DocId::new([doc; 32]),
        datum_key: DatumKey::from_text("org.example.note", name).unwrap(),
    }
}

fn edge(origin: &DatumRoute, path: &str, target: &DatumRoute, heads: &[u8]) -> DatumRefEdge {
    DatumRefEdge {
        origin: origin.clone(),
        path: path.to_owned(),
        target: DatumRefTarget::Route {
            route: target.clone(),
            heads: heads.iter().map(|byte| ChangeHash([*byte; 32])).collect(),
        },
    }
}

fn key_edge(origin: &DatumRoute, path: &str, key: &DatumKey) -> DatumRefEdge {
    DatumRefEdge {
        origin: origin.clone(),
        path: path.to_owned(),
        target: DatumRefTarget::Key(key.clone()),
    }
}

fn ephemeral() -> Database {
    sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .unwrap()
}

/// Put rows in the edge table directly: the datum-set worker is the only writer of these edges, and
/// this store is the read surface over what it wrote, so the tests seed the table it reads.
async fn seed(sql: &Database, edges: &[DatumRefEdge]) {
    for edge in edges {
        match &edge.target {
            DatumRefTarget::Route { route, heads } => {
                Sql::new(
                    r#"INSERT INTO big_cabinet_datum_ref_edges (
                           origin_doc
                         , origin_datum
                         , origin_path
                         , target_doc
                         , target_datum
                         , ref_heads
                       ) VALUES "#,
                )
                .bind_list([
                    edge.origin.doc_id.to_string().into_value(),
                    edge.origin.datum_key.to_string().into_value(),
                    edge.path.clone().into_value(),
                    route.doc_id.to_string().into_value(),
                    route.datum_key.to_string().into_value(),
                    serde_json::to_vec(heads).unwrap().into_value(),
                ])
                .execute(sql)
                .await
                .unwrap();
            }
            DatumRefTarget::Key(key) => {
                Sql::new(
                    r#"INSERT INTO big_cabinet_datum_ref_key_edges (
                           origin_doc
                         , origin_datum
                         , origin_path
                         , target_key
                       ) VALUES "#,
                )
                .bind_list([
                    edge.origin.doc_id.to_string().into_value(),
                    edge.origin.datum_key.to_string().into_value(),
                    edge.path.clone().into_value(),
                    key.to_string().into_value(),
                ])
                .execute(sql)
                .await
                .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn every_selector_serves_the_direction_it_names_in_route_order() {
    let sql = ephemeral();
    let store = DatumRefStore::new(sql.clone()).await.unwrap();
    let main = route(1, "main");
    let sibling = route(1, "sibling");
    let first = route(2, "first");
    let second = route(3, "second");
    seed(
        &sql,
        &[
            edge(&main, "/nested/inner", &second, &[]),
            edge(&sibling, "/link", &first, &[7]),
            edge(&main, "/link", &first, &[7]),
        ],
    )
    .await;
    assert_eq!(
        store
            .query(DatumRefQuery::Origin(main.clone()))
            .await
            .unwrap(),
        vec![
            edge(&main, "/link", &first, &[7]),
            edge(&main, "/nested/inner", &second, &[]),
        ]
    );
    assert_eq!(
        store
            .query(DatumRefQuery::OriginDoc(main.doc_id.clone()))
            .await
            .unwrap(),
        vec![
            edge(&main, "/link", &first, &[7]),
            edge(&main, "/nested/inner", &second, &[]),
            edge(&sibling, "/link", &first, &[7]),
        ]
    );
    let into_first = vec![
        edge(&main, "/link", &first, &[7]),
        edge(&sibling, "/link", &first, &[7]),
    ];
    assert_eq!(
        store
            .query(DatumRefQuery::Target(first.clone()))
            .await
            .unwrap(),
        into_first
    );
    assert_eq!(
        store
            .query(DatumRefQuery::TargetDoc(first.doc_id.clone()))
            .await
            .unwrap(),
        into_first
    );
    assert_eq!(
        store
            .query(DatumRefQuery::Target(second.clone()))
            .await
            .unwrap(),
        vec![edge(&main, "/nested/inner", &second, &[])]
    );
    // A route nothing points at and a document nothing was read from are empty, not errors.
    assert!(
        store
            .query(DatumRefQuery::Target(route(1, "main")))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .query(DatumRefQuery::OriginDoc(DocId::new([9; 32])))
            .await
            .unwrap()
            .is_empty()
    );
}

/// A `#keyRef` names no document, so it is found by its key alone: from every document that holds a
/// referrer, and never by a document selector.
#[tokio::test]
async fn a_key_reference_is_found_by_its_key_whichever_document_holds_the_referrer() {
    let sql = ephemeral();
    let store = DatumRefStore::new(sql.clone()).await.unwrap();
    let key = DatumKey::from_text("org.example.blob", "aa").unwrap();
    let other = DatumKey::from_text("org.example.blob", "bb").unwrap();
    let (first, second, third) = (route(1, "main"), route(2, "main"), route(3, "main"));
    let pointee = route(4, "main");
    seed(
        &sql,
        &[
            key_edge(&second, "/target", &key),
            key_edge(&first, "/target", &key),
            key_edge(&third, "/target", &other),
            edge(&first, "/link", &pointee, &[]),
        ],
    )
    .await;
    assert_eq!(
        store.query(DatumRefQuery::Key(key.clone())).await.unwrap(),
        vec![
            key_edge(&first, "/target", &key),
            key_edge(&second, "/target", &key)
        ]
    );
    assert!(
        store
            .query(DatumRefQuery::Key(
                DatumKey::from_text("org.example.blob", "cc").unwrap()
            ))
            .await
            .unwrap()
            .is_empty()
    );
    // From the referrer's side the edge is an ordinary one whose target is the key.
    assert_eq!(
        store
            .query(DatumRefQuery::Origin(first.clone()))
            .await
            .unwrap(),
        vec![
            edge(&first, "/link", &pointee, &[]),
            key_edge(&first, "/target", &key)
        ]
    );
    // No document selector can match it: its row has no document.
    assert!(
        store
            .query(DatumRefQuery::TargetDoc(DocId::new([0; 32])))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .query(DatumRefQuery::Target(pointee.clone()))
            .await
            .unwrap(),
        vec![edge(&first, "/link", &pointee, &[])]
    );
}

#[tokio::test]
async fn a_store_on_a_database_the_worker_never_ran_on_finds_the_shared_edge_table() {
    let schemas = Arc::new(crate::schema::SchemaRepo::open(Vec::new()).unwrap());
    // Both migrations create the edge table, so either construction order leaves the one table the
    // read surface queries, and a store that only ever answers edge needs no worker beside it.
    for worker_first in [true, false] {
        let sql = ephemeral();
        if worker_first {
            crate::index::DatumSetStore::new(sql.clone(), Arc::clone(&schemas))
                .await
                .unwrap();
        }
        let store = DatumRefStore::new(sql.clone()).await.unwrap();
        if !worker_first {
            crate::index::DatumSetStore::new(sql.clone(), Arc::clone(&schemas))
                .await
                .unwrap();
        }
        let main = route(1, "main");
        seed(&sql, &[edge(&main, "/link", &route(2, "first"), &[7])]).await;
        assert_eq!(
            store
                .query(DatumRefQuery::Origin(main))
                .await
                .unwrap()
                .len(),
            1,
            "worker_first={worker_first}"
        );
    }
}
