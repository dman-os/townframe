use super::*;
use crate::authority::Authority;
use crate::doc::AccessRequest;
use crate::doc::DocAccess;
use crate::doc::DocsConfig;
use crate::doc::DocsRepo;
use crate::doc::LedgerAccess;
use crate::doc::boot as boot_docs;
use crate::index::DatumSetStore;
use crate::schema::SchemaRegistration;
use crate::schema::SchemaRegistrationGroup;
use crate::schema::SchemaRepo;
use automerge::Automerge;
use automerge::ROOT;
use automerge::ReadDoc;
use automerge::transaction::Transactable;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumPatch;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::Config;
use big_repo::SharedBigRepo;
use big_repo::Storage;
use secrets_rs::SecretStore;
use serde_json::Value;
use serde_json::json;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

const CFG_TAG: &str = "org.example.cfg";
const OTHER_TAG: &str = "org.example.other";
const PLACEMENT: &str = "drawer test";

fn cfg_key(rkey: &str) -> DatumKey {
    format!("{CFG_TAG}/{rkey}").parse().unwrap()
}

fn other_key(rkey: &str) -> DatumKey {
    format!("{OTHER_TAG}/{rkey}").parse().unwrap()
}

fn cfg_val(text: &str) -> Value {
    json!({"$type": CFG_TAG, "value": text})
}

fn other_val(text: &str) -> Value {
    json!({"$type": OTHER_TAG, "value": text})
}

fn test_schemas() -> Arc<SchemaRepo> {
    let make = |id: &str| {
        json!({
            "lexicon": 1,
            "id": id,
            "defs": {"main": {"type": "record", "key": "tid", "record": {
                "type": "object",
                "required": ["value"],
                "properties": {"value": {"type": "string"}}
            }}}
        })
    };
    Arc::new(
        SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [
                SchemaRegistration::new(CFG_TAG, make(CFG_TAG)),
                SchemaRegistration::new(OTHER_TAG, make(OTHER_TAG)),
            ],
            Vec::new(),
        )])
        .unwrap(),
    )
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TestConfigStore {
    entries: BTreeMap<String, String>,
    rejected: BTreeMap<String, String>,
}

impl DatumStore for TestConfigStore {
    fn owns(key: &DatumKey) -> bool {
        key.tag().as_str() == CFG_TAG
    }

    fn hydrate(read: &DatumsRead) -> Self {
        let mut entries = BTreeMap::new();
        let mut rejected = BTreeMap::new();
        for (key, datum) in &read.datums {
            if !Self::owns(key) {
                continue;
            }
            let rkey = std::str::from_utf8(key.slot_bytes()).unwrap().to_owned();
            match datum {
                DatumRead::Accepted { value, .. } => {
                    if let Some(text) = value.get("value").and_then(Value::as_str) {
                        entries.insert(rkey, text.to_owned());
                    }
                }
                DatumRead::Pending { reason, .. }
                | DatumRead::Rejected { reason }
                | DatumRead::Undecodable { reason, .. } => {
                    rejected.insert(rkey, reason.clone());
                }
                DatumRead::Absent { .. } => {}
            }
        }
        Self { entries, rejected }
    }

    fn diff(before: &Self, after: &Self) -> Result<DatumPatch, StoreError> {
        let mut patch = DatumPatch::default();
        for (rkey, after_val) in &after.entries {
            if after_val.is_empty() {
                return Err(StoreError::InvalidEdit("empty value is forbidden".into()));
            }
            if before.entries.get(rkey) != Some(after_val) {
                patch.set.insert(cfg_key(rkey), cfg_val(after_val));
            }
        }
        for rkey in before.entries.keys() {
            if !after.entries.contains_key(rkey) {
                patch.remove.insert(cfg_key(rkey));
            }
        }
        Ok(patch)
    }
}

struct TestHarness {
    authority: Authority,
    authority_stop: Option<StopToken>,
    access: Arc<LedgerAccess>,
    repo: SharedBigRepo,
    repo_stop: Option<BigRepoStopToken>,
    docs: Option<DocsRepo>,
    docs_stop: Option<StopToken>,
    datum_stop: Option<StopToken>,
}

impl TestHarness {
    async fn boot() -> Self {
        let keyring =
            keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring).await.unwrap();
        let (authority, authority_stop) = crate::authority::boot(
            &secrets,
            "test-node",
            sqlite_rs::test_support::db(&[]).unwrap().clone(),
        )
        .await
        .unwrap();
        let docs_actor = authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: authority.root,
                    path: "documents".into(),
                    rights: Rights::all(),
                },
            )
            .await
            .unwrap();
        let docs_namespace: Namespace = authority
            .service
            .open_namespace(Uuid7::new())
            .await
            .unwrap();
        let sql: Database = sqlite_rs::test_support::db(&[]).unwrap().clone();
        let access = Arc::new(
            LedgerAccess::open(sql.clone(), authority.client.clone(), docs_namespace)
                .await
                .unwrap(),
        );
        let (repo, repo_stop) = BigRepo::boot(Config {
            node_identity_seed: [53; 32],
            storage: Storage::memory(&sqlite_rs::SqliteRuntime::new(Default::default())).unwrap(),
            scope_key: "datum-store-tests".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await
        .unwrap();
        let datum_set = Arc::new(
            DatumSetStore::new(
                sqlite_rs::test_support::db(&[]).unwrap().clone(),
                test_schemas(),
            )
            .await
            .unwrap(),
        );
        let (datum_stop, datum_tasks) = StopToken::new();
        {
            let store = Arc::clone(&datum_set);
            let repo_clone = Arc::clone(&repo);
            let owner = datum_tasks.clone();
            datum_tasks
                .spawn(async move {
                    store
                        .run_until(
                            repo_clone,
                            NonZeroUsize::new(4).unwrap(),
                            owner.cancelled(),
                            &mut None,
                        )
                        .await
                        .unwrap();
                })
                .unwrap();
        }
        let (docs, docs_stop) = boot_docs(DocsConfig {
            big_repo: Arc::clone(&repo),
            schema_repo: test_schemas(),
            authority: authority.client.clone(),
            issuer: docs_actor.capability,
            namespace: docs_namespace,
            node_issuer: authority.node_issuer.clone(),
            access: Arc::clone(&access) as Arc<dyn DocAccess>,
            sql,
            datum_set: Some(datum_set),
            datum_ref: None,
        })
        .await
        .unwrap();
        Self {
            authority,
            authority_stop: Some(authority_stop),
            access,
            repo,
            repo_stop: Some(repo_stop),
            docs: Some(docs),
            docs_stop: Some(docs_stop),
            datum_stop: Some(datum_stop),
        }
    }

    fn client(&self) -> Client<DocsProtocol> {
        self.docs.as_ref().unwrap().client.clone()
    }

    async fn actor(&self, path: &str) -> OpenedActor {
        let actor = self
            .authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: self.authority.root,
                    path: path.into(),
                    rights: Rights::USE | Rights::NARROW,
                },
            )
            .await
            .unwrap();
        self.grant(
            &actor,
            LedgerAccess::object(AccessRequest::Create {
                placement: PLACEMENT,
            }),
        )
        .await;
        actor
    }

    async fn grant(&self, actor: &OpenedActor, object: String) {
        self.access
            .grants
            .grant(
                &RequestContext::root(),
                actor.actor.actor_id,
                object,
                Rights::USE,
                None,
            )
            .await
            .unwrap();
    }

    async fn revoke(&self, actor: &OpenedActor, objects: &[String]) {
        self.access
            .grants
            .revoke(&RequestContext::root(), actor.actor.actor_id, objects)
            .await
            .unwrap();
    }

    async fn capability(&self, actor: &OpenedActor, scope: DocScope) -> CapId {
        self.client()
            .call_with(
                RequestContext::root(),
                Authorization::identity(actor.capability).metadata(),
                MintDocCapability {
                    scope,
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .unwrap()
            .capability_id
    }

    async fn create_doc(
        &self,
        actor: &OpenedActor,
        command: &str,
        datums: impl IntoIterator<Item = (DatumKey, Value)>,
    ) -> WriteReceipt {
        let cap = self
            .capability(
                actor,
                DocScope {
                    create: BTreeSet::from([PLACEMENT.into()]),
                    ..DocScope::default()
                },
            )
            .await;
        let receipt = self
            .client()
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: cap,
                    command: CommandId::try_from(command.to_owned()).unwrap(),
                    placement: PLACEMENT.into(),
                    datums: datums.into_iter().collect(),
                },
            )
            .await
            .unwrap();
        self.grant(
            actor,
            LedgerAccess::object(AccessRequest::Read(&receipt.doc_id)),
        )
        .await;
        self.grant(
            actor,
            LedgerAccess::object(AccessRequest::Write(&receipt.doc_id)),
        )
        .await;
        receipt
    }

    async fn sabotage(&self, doc_id: &DocId, edit: impl FnOnce(&mut Automerge)) -> Vec<ChangeHash> {
        let handle = match self.repo.get_doc(doc_id).await.unwrap() {
            big_repo::DocLookup::Ready(handle) => handle,
            _ => panic!("document is local"),
        };
        handle
            .with_document(|doc| {
                edit(doc);
                let mut heads = doc.get_heads();
                heads.sort();
                heads
            })
            .await
            .unwrap()
    }

    async fn stop(mut self) {
        self.docs.take();
        if let Some(stop) = self.docs_stop.take() {
            stop.stop().await;
        }
        if let Some(stop) = self.datum_stop.take() {
            stop.stop().await;
        }
        if let Some(stop) = self.repo_stop.take() {
            stop.stop().await.unwrap();
        }
        self.authority_stop.take().unwrap().stop().await;
    }
}

#[tokio::test]
async fn load_mutate_at_and_noop_preserve_sibling_datums() {
    let harness = TestHarness::boot().await;
    let alice = harness.actor("alice").await;
    let created = harness
        .create_doc(
            &alice,
            "create-config",
            [
                (cfg_key("alpha"), cfg_val("one")),
                (other_key("foreign"), other_val("untouched")),
            ],
        )
        .await;
    let doc_id = created.doc_id.clone();
    let rw_cap = harness
        .capability(
            &alice,
            DocScope {
                read: BTreeSet::from([doc_id.clone()]),
                write: BTreeSet::from([doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await;

    let (store, store_stop) = DatumStoreHandle::<TestConfigStore>::load(
        &RequestContext::root(),
        harness.client(),
        rw_cap,
        doc_id.clone(),
    )
    .await
    .unwrap();

    let initial = store.snapshot().await.unwrap();
    assert_eq!(initial.heads, created.heads);
    assert_eq!(
        initial.value.entries,
        BTreeMap::from([("alpha".into(), "one".into())])
    );

    let (noop_out, noop_receipt) = store
        .mutate(&RequestContext::root(), None, |_cfg| Ok(42usize))
        .await
        .unwrap();
    assert_eq!(noop_out, 42);
    assert!(noop_receipt.is_none());
    assert_eq!(store.snapshot().await.unwrap().heads, created.heads);

    let invalid = store
        .mutate(&RequestContext::root(), None, |cfg| {
            cfg.entries.insert("alpha".into(), String::new());
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(matches!(invalid, StoreError::InvalidEdit(_)), "{invalid:?}");
    assert_eq!(
        store.snapshot().await.unwrap().value.entries["alpha"],
        "one"
    );

    let (out, receipt) = store
        .mutate(
            &RequestContext::root(),
            Some(CommandId::try_from("set-beta".to_owned()).unwrap()),
            |cfg| {
                cfg.entries.insert("alpha".into(), "two".into());
                cfg.entries.insert("beta".into(), "three".into());
                Ok("updated")
            },
        )
        .await
        .unwrap();
    assert_eq!(out, "updated");
    let receipt = receipt.expect("mutation changed keys");
    let updated = store.snapshot().await.unwrap();
    assert_eq!(updated.heads, receipt.heads);
    assert_eq!(
        updated.value.entries,
        BTreeMap::from([
            ("alpha".into(), "two".into()),
            ("beta".into(), "three".into()),
        ])
    );

    let historical = store.at(created.heads.clone()).await.unwrap();
    assert_eq!(historical.heads, created.heads);
    assert_eq!(
        historical.value.entries,
        BTreeMap::from([("alpha".into(), "one".into())])
    );

    let full_read = harness
        .client()
        .call(
            RequestContext::root(),
            ReadDatums {
                capability: rw_cap,
                doc_id: doc_id.clone(),
                at: At::Current,
                datums: None,
                optimistic: false,
            },
        )
        .await
        .unwrap();
    assert!(full_read.datums.contains_key(&other_key("foreign")));

    store_stop.stop().await;
    harness.stop().await;
}

#[tokio::test]
async fn concurrent_foreign_write_merges_at_loaded_heads_and_watch_streams_updates() {
    let harness = TestHarness::boot().await;
    let alice = harness.actor("alice").await;
    let created = harness
        .create_doc(&alice, "create-merge", [(cfg_key("a"), cfg_val("v1"))])
        .await;
    let doc_id = created.doc_id.clone();
    let rw_cap = harness
        .capability(
            &alice,
            DocScope {
                read: BTreeSet::from([doc_id.clone()]),
                write: BTreeSet::from([doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await;

    let (store, store_stop) = DatumStoreHandle::<TestConfigStore>::load(
        &RequestContext::root(),
        harness.client(),
        rw_cap,
        doc_id.clone(),
    )
    .await
    .unwrap();
    let mut watch = store.watch().await.unwrap();
    let first = watch.next().await.unwrap().unwrap();
    assert_eq!(
        first.value.entries,
        BTreeMap::from([("a".into(), "v1".into())])
    );

    harness
        .client()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: rw_cap,
                command: None,
                doc_id: doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(cfg_key("b"), cfg_val("foreign"))]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();

    let (_, receipt) = store
        .mutate(&RequestContext::root(), None, |cfg| {
            cfg.entries.insert("c".into(), "local".into());
            Ok(())
        })
        .await
        .unwrap();
    let receipt = receipt.unwrap();
    let merged = store.snapshot().await.unwrap();
    assert_eq!(merged.heads, receipt.heads);
    assert_eq!(
        merged.value.entries,
        BTreeMap::from([
            ("a".into(), "v1".into()),
            ("b".into(), "foreign".into()),
            ("c".into(), "local".into()),
        ])
    );

    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if next.value.entries == merged.value.entries {
            break;
        }
    }

    harness
        .client()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: rw_cap,
                command: None,
                doc_id: doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::new(),
                remove: BTreeSet::from([cfg_key("a")]),
            },
        )
        .await
        .unwrap();

    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), watch.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if !next.value.entries.contains_key("a") {
            assert_eq!(
                next.value.entries,
                BTreeMap::from([("b".into(), "foreign".into()), ("c".into(), "local".into())])
            );
            break;
        }
    }

    drop(watch);
    store_stop.stop().await;
    harness.stop().await;
}

#[tokio::test]
async fn rejected_entry_is_preserved_and_access_loss_surfaces_on_empty_and_populated_docs() {
    let harness = TestHarness::boot().await;
    let alice = harness.actor("alice").await;
    let bob = harness.actor("bob").await;
    let created = harness
        .create_doc(
            &alice,
            "create-sabotage",
            [
                (cfg_key("good"), cfg_val("ok")),
                (cfg_key("bad"), cfg_val("tamper-target")),
            ],
        )
        .await;
    let doc_id = created.doc_id.clone();

    harness
        .sabotage(&doc_id, |doc| {
            let mut tx = doc.transaction();
            let (_, datums) = tx.get(ROOT, "datums").unwrap().unwrap();
            let (_, bad) = tx
                .get(&datums, cfg_key("bad").to_string())
                .unwrap()
                .unwrap();
            tx.put(&bad, "value", "forged").unwrap();
            tx.commit();
        })
        .await;

    let rw_cap = harness
        .capability(
            &alice,
            DocScope {
                read: BTreeSet::from([doc_id.clone()]),
                write: BTreeSet::from([doc_id.clone()]),
                ..DocScope::default()
            },
        )
        .await;
    let (store, store_stop) = DatumStoreHandle::<TestConfigStore>::load(
        &RequestContext::root(),
        harness.client(),
        rw_cap,
        doc_id.clone(),
    )
    .await
    .unwrap();
    let snap = store.snapshot().await.unwrap();
    assert_eq!(
        snap.value.entries,
        BTreeMap::from([("good".into(), "ok".into())])
    );
    assert!(snap.value.rejected.contains_key("bad"), "{:?}", snap.value);

    store
        .mutate(&RequestContext::root(), None, |cfg| {
            cfg.entries.insert("good".into(), "still-ok".into());
            Ok(())
        })
        .await
        .unwrap();
    let after_mutate = store.snapshot().await.unwrap();
    assert_eq!(after_mutate.value.entries["good"], "still-ok");
    assert!(after_mutate.value.rejected.contains_key("bad"));

    let empty_doc = harness.create_doc(&alice, "create-empty", []).await.doc_id;
    harness
        .grant(&bob, LedgerAccess::object(AccessRequest::Read(&empty_doc)))
        .await;
    let bob_cap = harness
        .capability(
            &bob,
            DocScope {
                read: BTreeSet::from([empty_doc.clone()]),
                ..DocScope::default()
            },
        )
        .await;
    let (bob_store, bob_stop) = DatumStoreHandle::<TestConfigStore>::load(
        &RequestContext::root(),
        harness.client(),
        bob_cap,
        empty_doc.clone(),
    )
    .await
    .unwrap();

    let denied_write = bob_store
        .mutate(&RequestContext::root(), None, |cfg| {
            cfg.entries.insert("x".into(), "y".into());
            Ok(())
        })
        .await
        .unwrap_err();
    assert_eq!(denied_write, StoreError::Docs(DocsError::Denied));
    assert!(bob_store.snapshot().await.is_ok());

    let mut bob_watch = bob_store.watch().await.unwrap();
    let initial_empty = bob_watch.next().await.unwrap().unwrap();
    assert!(initial_empty.value.entries.is_empty());

    harness
        .revoke(
            &bob,
            &[LedgerAccess::object(AccessRequest::Read(&empty_doc))],
        )
        .await;
    let lost = tokio::time::timeout(Duration::from_secs(5), bob_watch.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lost, Err(StoreError::AccessLost));
    assert_eq!(bob_store.snapshot().await, Err(StoreError::AccessLost));

    drop(bob_watch);
    bob_stop.stop().await;
    store_stop.stop().await;
    harness.stop().await;
}
