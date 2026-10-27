use super::*;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::authority::Authorization;
use big_cabinet::authority::CapId;
use big_cabinet::index::FacetMembership;
use big_cabinet::index::FacetRoute;
use big_cabinet::index::FacetSetQuery;
use big_cabinet::index::FacetSetStore;
use big_cabinet_core::DocId;
use big_cabinet_core::FacetKey;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::FacetRead;
use big_cabinet_core::doc::FacetsRead;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadFacets;
use big_cabinet_core::doc::WriteReceipt;
use big_repo::StorageConfig;
use big_sync_core::revisioned_store::RevisionRead;
use big_sync_core::revisioned_store::RevisionReadLimits;
use big_sync_core::revisioned_store::RevisionedStore;
use big_sync_core::revisioned_store::RevisionedStoreReader;
use serde_json::Value;
use serde_json::json;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

const NOTE: &str = "org.example.note";

fn key() -> FacetKey {
    format!("{NOTE}/main").parse().unwrap()
}

fn schemas() -> Arc<SchemaRepo> {
    Arc::new(
        SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [SchemaRegistration::new(
                NOTE,
                json!({
                    "lexicon": 1,
                    "id": NOTE,
                    "defs": {"main": {"type": "record", "key": "tid", "record": {
                        "type": "object",
                        "required": ["value"],
                        "properties": {"value": {"type": "string"}}
                    }}}
                }),
            )],
            Vec::new(),
        )])
        .unwrap(),
    )
}

async fn secrets() -> SecretStore {
    let keyring = keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
    SecretStore::boot_with_store(keyring).await.unwrap()
}

fn config<'a>(
    secrets: &'a SecretStore,
    runtime: &Arc<SqliteRuntime>,
    sql: Database,
    storage: StorageConfig,
) -> Config<'a> {
    Config {
        sqlite_runtime: Arc::clone(runtime),
        repo: big_repo::Config {
            node_identity_seed: [47; 32],
            storage,
            scope_key: "big-node-e2e".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        },
        sql,
        schemas: schemas(),
        secrets,
        index_concurrency: NonZeroUsize::new(2).unwrap(),
    }
}

async fn capability(node: &BigNode, scope: DocScope) -> CapId {
    node.docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id
}

async fn create(node: &BigNode, command: &str, value: Value) -> WriteReceipt {
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let capability = capability(
        node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    node.docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: CommandId::try_from(command.to_owned()).unwrap(),
                placement,
                facets: BTreeMap::from([(key(), value)]),
            },
        )
        .await
        .unwrap()
}

async fn read(node: &BigNode, document: &DocId) -> FacetsRead {
    let capability = capability(
        node,
        DocScope {
            read: BTreeSet::from([document.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    node.docs
        .client
        .call(
            RequestContext::root(),
            ReadFacets {
                capability,
                doc_id: document.clone(),
                at: At::Current,
                facets: Some(BTreeSet::from([key()])),
                optimistic: false,
            },
        )
        .await
        .unwrap()
}

async fn indexed(node: &BigNode, receipt: &WriteReceipt) {
    let route = FacetRoute {
        doc_id: receipt.doc_id.clone(),
        facet_key: key(),
    };
    let mut reader = node
        .indexes
        .facet_set
        .open(route.facet_key.tag().clone(), 0)
        .await
        .unwrap();
    loop {
        let RevisionRead::Entries { entries, .. } =
            reader.next(RevisionReadLimits::default()).await.unwrap()
        else {
            continue;
        };
        if entries.into_iter().any(|entry| {
            entry.key == route
                && matches!(
                    entry.value,
                    Some(FacetMembership::Present { doc_heads, .. }) if doc_heads == receipt.heads
                )
        }) {
            break;
        }
    }
    let records = node
        .indexes
        .facet_set
        .query(FacetSetQuery::Route(route))
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].doc_heads, receipt.heads);
}

async fn shutdown(node: BigNode, stop: StopToken) {
    let repo = Arc::downgrade(&node.repo);
    let docs = Arc::downgrade(&node.docs.host);
    let drawers = Arc::downgrade(&node.drawers.host);
    let policy = Arc::downgrade(&node.drawer_access);
    let authority = Arc::downgrade(&node.authority.host);
    let authority_service = Arc::downgrade(&node.authority.service);
    let indexes = Arc::clone(&node.indexes);
    assert_eq!(indexes.live_tasks(), 2);
    drop(node);
    stop.stop().await.unwrap();
    assert_eq!(indexes.live_tasks(), 0);
    drop(indexes);
    assert!(docs.upgrade().is_none());
    assert!(drawers.upgrade().is_none());
    assert!(policy.upgrade().is_none());
    assert!(repo.upgrade().is_none());
    assert!(authority.upgrade().is_none());
    assert!(authority_service.upgrade().is_none());
}

#[tokio::test]
async fn memory_boot_owns_and_joins_every_component() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Memory,
    ))
    .await
    .unwrap();
    let value = json!({"$type": NOTE, "value": "memory"});
    let receipt = create(&node, "memory-note", value.clone()).await;
    let read = read(&node, &receipt.doc_id).await;
    assert!(
        matches!(&read.facets[&key()], FacetRead::Accepted { value: accepted, .. } if *accepted == value)
    );
    indexed(&node, &receipt).await;
    shutdown(node, stop).await;
    drop(sql);
}

#[tokio::test]
async fn disk_reopen_recovers_identity_drawers_documents_commands_and_index_cursor() {
    let directory = tempfile::tempdir().unwrap();
    let repo_path = directory.path().join("repo");
    let sql_path = directory.path().join("cabinet.sqlite");
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let secrets = secrets().await;
    let sql = runtime.database(&sql_path, &[], "_sqlite_rs_migrations");
    let (node, stop) = BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk {
            path: repo_path.clone(),
        },
    ))
    .await
    .unwrap();
    let identity = (
        node.repo.local_peer_id(),
        node.authority.node_issuer.clone(),
        node.docs_actor.actor_id,
        node.docs_actor.public_key.clone(),
        node.drawers_actor.actor_id,
        node.drawers_actor.public_key.clone(),
        node.docs_namespace.namespace_id,
        node.drawers_namespace.namespace_id,
    );
    let local = node.drawers.local();
    let value = json!({"$type": NOTE, "value": "persisted"});
    let receipt = create(&node, "persistent-note", value.clone()).await;
    let before = read(&node, &receipt.doc_id).await;
    assert!(
        matches!(&before.facets[&key()], FacetRead::Accepted { value: accepted, .. } if *accepted == value)
    );
    indexed(&node, &receipt).await;
    let index = Arc::clone(&node.indexes.facet_set);
    shutdown(node, stop).await;
    let cursor = index.source_revision().await.unwrap();
    let output_revision = index.latest_revision().await.unwrap();
    assert!(cursor > 0);
    assert!(output_revision > 0);
    let records = index
        .query(FacetSetQuery::Doc(receipt.doc_id.clone()))
        .await
        .unwrap();
    assert!(!records.is_empty());
    drop(index);
    drop(sql);
    drop(runtime);

    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.database(&sql_path, &[], "_sqlite_rs_migrations");
    let persisted = FacetSetStore::new(sql.clone()).await.unwrap();
    assert_eq!(persisted.source_revision().await.unwrap(), cursor);
    assert_eq!(persisted.latest_revision().await.unwrap(), output_revision);
    assert_eq!(
        persisted
            .query(FacetSetQuery::Doc(receipt.doc_id.clone()))
            .await
            .unwrap(),
        records
    );
    drop(persisted);
    let (node, stop) = BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk { path: repo_path },
    ))
    .await
    .unwrap();
    assert_eq!(
        (
            node.repo.local_peer_id(),
            node.authority.node_issuer.clone(),
            node.docs_actor.actor_id,
            node.docs_actor.public_key.clone(),
            node.drawers_actor.actor_id,
            node.drawers_actor.public_key.clone(),
            node.docs_namespace.namespace_id,
            node.drawers_namespace.namespace_id,
        ),
        identity,
    );
    assert_eq!(node.drawers.local(), local);
    assert_eq!(read(&node, &receipt.doc_id).await, before);
    assert_eq!(create(&node, "persistent-note", value).await, receipt);
    let new_receipt = create(
        &node,
        "after-reopen",
        json!({"$type": NOTE, "value": "new"}),
    )
    .await;
    assert_ne!(new_receipt.doc_id, receipt.doc_id);
    indexed(&node, &new_receipt).await;
    shutdown(node, stop).await;
    drop(sql);
}

#[tokio::test]
async fn failed_docs_recovery_unwinds_the_partial_node() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (_, _, filename): (i64, String, String) = sql
        .query("PRAGMA database_list", &[])
        .await
        .unwrap()
        .one()
        .unwrap();
    let database = std::path::PathBuf::from(filename);
    sql.execute_batch("CREATE TABLE big_cabinet_commands (command_id TEXT PRIMARY KEY) STRICT")
        .await
        .unwrap();
    let error = match BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Memory,
    ))
    .await
    {
        Err(error) => error,
        Ok((node, stop)) => {
            shutdown(node, stop).await;
            panic!("a malformed command ledger must reject Docs recovery");
        }
    };
    assert!(
        matches!(
            error.downcast_ref::<big_cabinet_core::doc::DocsError>(),
            Some(big_cabinet_core::doc::DocsError::Repository(message))
                if message.contains("command ledger:") && message.contains("no such column: kind")
        ),
        "unexpected boot error: {error:#}"
    );
    assert!(database.exists());
    drop(sql);
    assert!(
        !database.exists(),
        "partial-node cleanup retained a Cabinet SQL context"
    );
}

#[tokio::test]
async fn failed_reference_initialization_joins_the_started_facet_worker() {
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    big_cabinet::index::FacetRefStore::new(sql.clone())
        .await
        .unwrap();
    sql.execute(
        "INSERT INTO big_cabinet_facet_ref_progress VALUES (?, -1)",
        (NOTE,),
    )
    .await
    .unwrap();
    let secrets = secrets().await;
    let (repo, stop) = BigRepo::boot(
        Arc::clone(&runtime),
        config(&secrets, &runtime, sql.clone(), StorageConfig::Memory).repo,
    )
    .await
    .unwrap();
    let references = Arc::strong_count(&repo);
    match Indexes::boot(
        Arc::clone(&repo),
        schemas(),
        sql.clone(),
        NonZeroUsize::new(2).unwrap(),
    )
    .await
    {
        Err(error) => assert!(
            error
                .to_string()
                .contains("facet-ref revision is out of range")
        ),
        Ok(_) => panic!("a negative reference cursor must reject index startup"),
    }
    assert_eq!(Arc::strong_count(&repo), references);
    drop(repo);
    stop.stop().await.unwrap();
    drop(sql);
}

#[tokio::test]
async fn failed_reference_initialization_unwinds_disk_node_and_allows_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let repo_path = directory.path().join("repo");
    let sql_path = directory.path().join("cabinet.sqlite");
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let secrets = secrets().await;
    let sql = runtime.database(&sql_path, &[], "_sqlite_rs_migrations");
    big_cabinet::index::FacetRefStore::new(sql.clone())
        .await
        .unwrap();
    sql.execute(
        "INSERT INTO big_cabinet_facet_ref_progress (facet_tag, revision) VALUES (?, -1)",
        (NOTE,),
    )
    .await
    .unwrap();
    match BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk {
            path: repo_path.clone(),
        },
    ))
    .await
    {
        Err(error) => assert_eq!(error.to_string(), "facet-ref revision is out of range"),
        Ok((node, stop)) => {
            shutdown(node, stop).await;
            panic!("a negative reference cursor must reject node startup");
        }
    }
    let repaired = sql
        .execute(
            "UPDATE big_cabinet_facet_ref_progress SET revision = 0 WHERE facet_tag = ?",
            (NOTE,),
        )
        .await
        .unwrap();
    assert_eq!(repaired.rows_affected, 1);
    drop(sql);
    drop(runtime);

    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.database(&sql_path, &[], "_sqlite_rs_migrations");
    let revision: i64 = sql
        .query(
            "SELECT revision FROM big_cabinet_facet_ref_progress WHERE facet_tag = ?",
            (NOTE,),
        )
        .await
        .unwrap()
        .one()
        .unwrap();
    assert_eq!(revision, 0);
    let (node, stop) = BigNode::boot(config(
        &secrets,
        &runtime,
        sql.clone(),
        StorageConfig::Disk { path: repo_path },
    ))
    .await
    .unwrap();
    shutdown(node, stop).await;
    drop(sql);
}

#[tokio::test]
async fn shutdown_joins_reverse_construction_order_even_after_a_task_panics() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut stop = StopToken::default();
    let mut workers = Vec::new();
    for ordinal in 0..3 {
        let (component_stop, tasks) = service_utils_rs::StopToken::new();
        let owner = tasks.clone();
        let order = Arc::clone(&order);
        tasks
            .spawn(async move {
                owner.cancelled().await;
                order.lock().unwrap().push(ordinal);
                assert_ne!(ordinal, 1, "injected component failure");
            })
            .unwrap();
        workers.push(tasks);
        stop.stages.push(Stage::Service(component_stop));
    }
    assert!(AssertUnwindSafe(stop.stop()).catch_unwind().await.is_err());
    assert_eq!(*order.lock().unwrap(), vec![2, 1, 0]);
    assert!(workers.iter().all(|tasks| tasks.live_tasks() == 0));
}
