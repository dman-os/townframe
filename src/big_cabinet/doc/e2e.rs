//! Repository conformance: the guarded read/write contract of ADR 020 §7, exercised end to end
//! through the hosted client against a booted authority and BigRepo.
//!
//! Forgeries are planted with raw Automerge edits on documents the repository wrote correctly, so
//! each test states exactly which invariant the sabotage breaks.

mod branches;
mod capabilities;
mod commands;
mod gate;
mod model;
mod observe;
mod peer;
mod wire;
mod writes;

use super::*;
use crate::authority::Authority;
use crate::index::DatumSetStore;
use crate::schema::SchemaRegistration;
use crate::schema::SchemaRegistrationGroup;
use crate::schema::SchemaRepo;
use automerge::Automerge;
use automerge::ChangeHash;
use big_cabinet_core::DatumKey;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::DocId;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::Config;
use big_repo::SharedBigRepo;
use big_repo::StorageConfig;
use secrets_rs::SecretStore;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

const NOTE: &str = "org.example.note";
const PLACEMENT: &str = "drawer test";

fn note(text: &str) -> Value {
    json!({"$type": NOTE, "value": text})
}

fn key(id: &str) -> DatumKey {
    format!("{NOTE}/{id}").parse().unwrap()
}

fn schemas() -> Arc<SchemaRepo> {
    let lexicon = json!({
        "lexicon": 1,
        "id": NOTE,
        "defs": {"main": {"type": "record", "key": "tid", "record": {
            "type": "object",
            "required": ["value"],
            "properties": {"value": {"type": "string"}}
        }}}
    });
    Arc::new(
        SchemaRepo::open([SchemaRegistrationGroup::new(
            None,
            [SchemaRegistration::new(NOTE, lexicon)],
            Vec::new(),
        )])
        .unwrap(),
    )
}

struct Node {
    authority: Authority,
    authority_stop: Option<StopToken>,
    docs_actor: Actor,
    docs_namespace: Namespace,
    access: Arc<LedgerAccess>,
    repo: SharedBigRepo,
    repo_stop: Option<BigRepoStopToken>,
    sql: Database,
    docs: Option<DocsRepo>,
    docs_stop: Option<StopToken>,
    datum_set: Arc<DatumSetStore>,
    datum_stop: Option<StopToken>,
}

impl Node {
    async fn boot() -> Self {
        Self::boot_with_seed(41).await
    }

    async fn boot_with_seed(seed: u8) -> Self {
        let keyring =
            keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring).await.unwrap();
        let (authority, authority_stop) =
            crate::authority::boot(&secrets, sqlite_rs::test_support::db(&[]).unwrap().clone())
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
        let docs_namespace = authority
            .service
            .open_namespace(Uuid7::new())
            .await
            .unwrap();
        let sql = sqlite_rs::test_support::db(&[]).unwrap().clone();
        let access = Arc::new(
            LedgerAccess::open(sql.clone(), authority.client.clone(), docs_namespace)
                .await
                .unwrap(),
        );
        let (repo, repo_stop) = BigRepo::boot(
            Arc::new(sqlite_rs::SqliteRuntime::new(Default::default())),
            Config {
                node_identity_seed: [seed; 32],
                storage: StorageConfig::Memory,
                scope_key: "documents-e2e".into(),
                hidden_parts: Default::default(),
                automerge_frontier_group_scope: Default::default(),
                causal_checkpoint_group_scope: Default::default(),
                group_part_group_scope: Default::default(),
            },
        )
        .await
        .unwrap();
        let datum_set = Arc::new(
            DatumSetStore::new(sqlite_rs::test_support::db(&[]).unwrap().clone())
                .await
                .unwrap(),
        );
        let (datum_stop, datum_tasks) = StopToken::new();
        {
            let store = Arc::clone(&datum_set);
            let repo = Arc::clone(&repo);
            let owner = datum_tasks.clone();
            datum_tasks
                .spawn(async move {
                    store
                        .run_until(
                            repo,
                            std::num::NonZeroUsize::new(4).unwrap(),
                            owner.cancelled(),
                            &mut None,
                        )
                        .await
                        .unwrap();
                })
                .unwrap();
        }
        let mut node = Self {
            authority,
            authority_stop: Some(authority_stop),
            docs_actor,
            docs_namespace,
            access,
            repo,
            repo_stop: Some(repo_stop),
            sql,
            docs: None,
            docs_stop: None,
            datum_set,
            datum_stop: Some(datum_stop),
        };
        node.start().await.unwrap();
        node
    }

    async fn start(&mut self) -> Result<(), DocsError> {
        self.start_with(Arc::clone(&self.access) as Arc<dyn DocAccess>, None)
            .await
    }

    /// Boot DocsRepo over this node's ledger and BigRepo with a substitute access policy and,
    /// optionally, a declared-reference index.
    async fn start_with(
        &mut self,
        access: Arc<dyn DocAccess>,
        datum_ref: Option<Arc<crate::index::DatumRefStore>>,
    ) -> Result<(), DocsError> {
        let (docs, stop) = boot(DocsConfig {
            big_repo: Arc::clone(&self.repo),
            schema_repo: schemas(),
            authority: self.authority.client.clone(),
            issuer: self.docs_actor.capability,
            namespace: self.docs_namespace,
            node_issuer: self.authority.node_issuer.clone(),
            access,
            sql: self.sql.clone(),
            datum_set: Some(Arc::clone(&self.datum_set)),
            datum_ref,
        })
        .await?;
        self.docs = Some(docs);
        self.docs_stop = Some(stop);
        Ok(())
    }

    async fn reopen_namespace(&mut self) {
        self.docs_namespace = self
            .authority
            .service
            .open_namespace(self.docs_namespace.namespace_id)
            .await
            .unwrap();
        self.access = Arc::new(
            LedgerAccess::open(
                self.sql.clone(),
                self.authority.client.clone(),
                self.docs_namespace,
            )
            .await
            .unwrap(),
        );
    }

    /// Stop DocsRepo and reopen its namespace, as after a crash, without booting it again.
    async fn crash_docs(&mut self) {
        self.docs.take();
        self.docs_stop.take().unwrap().stop().await;
        self.reopen_namespace().await;
    }

    /// Stop and boot DocsRepo again over the same ledger and BigRepo, as after a crash.
    async fn restart(&mut self) -> Result<(), DocsError> {
        self.crash_docs().await;
        self.start().await
    }

    fn client(&self) -> &service_utils_rs::Client<DocsProtocol> {
        &self.docs.as_ref().unwrap().client
    }

    /// A fresh actor allowed to create documents in [`PLACEMENT`].
    async fn writer(&self, path: &str) -> Actor {
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

    async fn grant(&self, actor: &Actor, object: String) {
        self.access
            .grants
            .grant(
                &RequestContext::root(),
                actor.actor_id,
                object,
                Rights::USE,
                None,
            )
            .await
            .unwrap();
    }

    async fn capability(&self, actor: &Actor, scope: DocScope) -> Result<CapId, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                MintDocCapability {
                    authorization: Authorization::identity(actor.capability),
                    scope,
                    rights: Rights::USE,
                },
            )
            .await)
        .map(|capability| capability.capability_id)
    }

    async fn create(
        &self,
        actor: &Actor,
        command: &str,
        datums: impl IntoIterator<Item = (DatumKey, Value)>,
    ) -> Result<WriteReceipt, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: self
                        .capability(
                            actor,
                            DocScope {
                                create: BTreeSet::from([PLACEMENT.into()]),
                                ..DocScope::default()
                            },
                        )
                        .await?,
                    command: CommandId::try_from(command.to_owned()).unwrap(),
                    placement: PLACEMENT.into(),
                    datums: datums.into_iter().collect(),
                },
            )
            .await)
    }

    async fn write(
        &self,
        actor: &Actor,
        document: &DocId,
        mode: WriteMode,
        set: impl IntoIterator<Item = (DatumKey, Value)>,
        remove: impl IntoIterator<Item = DatumKey>,
    ) -> Result<WriteReceipt, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: self
                        .capability(
                            actor,
                            DocScope {
                                write: BTreeSet::from([document.clone()]),
                                ..DocScope::default()
                            },
                        )
                        .await?,
                    command: None,
                    doc_id: document.clone(),
                    mode,
                    set: set.into_iter().collect(),
                    remove: remove.into_iter().collect(),
                },
            )
            .await)
    }

    async fn read(
        &self,
        actor: &Actor,
        document: &DocId,
        at: At,
        optimistic: bool,
    ) -> Result<DatumsRead, DocsError> {
        op(self
            .client()
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability: self
                        .capability(
                            actor,
                            DocScope {
                                read: BTreeSet::from([document.clone()]),
                                ..DocScope::default()
                            },
                        )
                        .await?,
                    doc_id: document.clone(),
                    at,
                    datums: None,
                    optimistic,
                },
            )
            .await)
    }

    async fn read_one(
        &self,
        actor: &Actor,
        document: &DocId,
        at: At,
        datum: &DatumKey,
    ) -> DatumRead {
        let read = self.client().call(
            RequestContext::root(),
            ReadDatums {
                capability: self
                    .capability(
                        actor,
                        DocScope {
                            read: BTreeSet::from([document.clone()]),
                            ..DocScope::default()
                        },
                    )
                    .await
                    .unwrap(),
                doc_id: document.clone(),
                at,
                datums: Some(BTreeSet::from([datum.clone()])),
                optimistic: false,
            },
        );
        let mut read = op(read.await).unwrap();
        read.datums.remove(datum).unwrap()
    }

    /// Apply a raw Automerge edit, bypassing the repository, and return the heads after it.
    async fn sabotage(
        &self,
        document: &DocId,
        edit: impl FnOnce(&mut Automerge),
    ) -> Vec<ChangeHash> {
        let handle = match self.repo.get_doc(document).await.unwrap() {
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

fn op<T>(result: Result<T, ClientError<DocsError>>) -> Result<T, DocsError> {
    result.map_err(|error| match error {
        ClientError::Operation(error) => error,
        ClientError::Transport { message, .. } => panic!("transport: {message}"),
    })
}

fn accepted(read: &DatumRead) -> (&Value, &[ChangeHash]) {
    match read {
        DatumRead::Accepted { value, version } => (value, version.heads.as_slice()),
        other => panic!("expected an accepted datum, got {other:?}"),
    }
}

fn datum_map(entries: &[(&DatumKey, Value)]) -> BTreeMap<DatumKey, Value> {
    entries
        .iter()
        .map(|(key, value)| ((*key).clone(), value.clone()))
        .collect()
}
