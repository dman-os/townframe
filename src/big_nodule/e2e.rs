//! Service-level tests: every call goes through `Client<NodulesProtocol>` against a real
//! authority, so admission, actor provisioning, persistence and the capability checks are
//! exercised the way an embedder sees them.

use crate::Nodules;
use crate::NodulesConfig;
use crate::boot;
use big_cabinet::Authority;
use big_cabinet::DocAccess;
use big_cabinet::DocsConfig;
use big_cabinet::DocsRepo;
use big_cabinet::DrawerAccess as PolicyAccess;
use big_cabinet::DrawerConfig;
use big_cabinet::Drawers;
use big_cabinet::SchemaRegistration;
use big_cabinet::SchemaRegistrationGroup;
use big_cabinet::SchemaRepo;
use big_cabinet::drawer::access::DrawerWorker;
use big_cabinet::index::Indexes;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_nodule_core::Config;
use big_nodule_core::Nodule;
use big_nodule_core::config::NoduleSettings;
use big_nodule_core::inventory::Readiness;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::ConfigureNodule;
use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::MintNoduleCapability;
use big_nodule_core::protocol::NoduleGrantStatus;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NoduleScope;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::protocol::NodulesMessage;
use big_nodule_core::protocol::NodulesProtocol;
use big_nodule_core::protocol::SetNoduleDrawerGrants;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::UninstallNodule;
use big_plugs_core::Contribution;
use big_plugs_core::NoduleContribution;
use big_plugs_core::Plug;
use big_plugs_core::PlugId;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::StorageConfig;
use indexmap::IndexMap;
use irpc::Channels;
use irpc::WithChannels;
use irpc::channel::none::NoReceiver;
use secrets_rs::SecretStore;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
use service_utils_rs::Operation;
use service_utils_rs::Reply;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use sqlite_rs::SqliteRuntime;
use sqlite_rs::test_support;
use sqlite_rs::test_support::TestDb;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

const NODULES_PATH: &str = "nodules";
const NOTE: &str = "org.example.note";

/// A package whose nodule declares one record lexicon and, when `settings` is set, a config
/// schema with one boolean `thumbnails` setting defaulting to `true`.
fn plug(id: &str, version: &str, settings: bool) -> Plug {
    let nodule_id = format!("org.example.{}", id.rsplit('/').next().unwrap());
    let mut nodule = Nodule::new(&nodule_id);
    nodule.lexicons.push(lexicon(json!({
        "lexicon": 1,
        "id": format!("{nodule_id}.item"),
        "defs": {
            "main": {
                "type": "record",
                "key": "tid",
                "record": { "type": "object", "properties": {} }
            }
        }
    })));
    if settings {
        nodule.lexicons.push(lexicon(json!({
            "lexicon": 1,
            "id": format!("{nodule_id}.settings"),
            "defs": {
                "main": {
                    "type": "object",
                    "properties": { "thumbnails": { "type": "boolean" } }
                }
            }
        })));
        nodule.config = Some(Config {
            schema: format!("{nodule_id}.settings"),
            defaults: Some(settings_of(true)),
            extra: Default::default(),
        });
    }
    let mut plug = Plug::new(id.parse().unwrap(), version.parse().unwrap());
    plug.contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: lexicon(serde_json::to_value(nodule).unwrap()),
            extra: Default::default(),
        }));
    plug
}

fn lexicon(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).unwrap()
}

fn settings_of(thumbnails: bool) -> IndexMap<String, Value> {
    [("thumbnails".to_owned(), json!(thumbnails))]
        .into_iter()
        .collect()
}

fn id(plug_id: &str) -> PlugId {
    plug_id.parse().unwrap()
}

struct Node {
    authority: Authority,
    authority_stop: StopToken,
    repo_stop: BigRepoStopToken,
    index_stop: big_cabinet::index::StopToken,
    policy_stop: StopToken,
    docs_stop: StopToken,
    worker: Option<DrawerWorker>,
    worker_stop: StopToken,
    drawers_stop: StopToken,
    drawers_actor: Actor,
    nodules_actor: Actor,
    docs: DocsRepo,
    drawers: Drawers,
    sql: TestDb,
    nodules: Nodules,
    stop: Option<StopToken>,
}

async fn node() -> Node {
    let keyring = keyring_core::mock::Store::new().expect("mock keyring")
        as Arc<keyring_core::CredentialStore>;
    let secrets = SecretStore::boot_with_store(keyring)
        .await
        .expect("mock keyring");
    let sql = test_support::db(&[]).expect("database");
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (authority, authority_stop) = big_cabinet::authority::boot(&secrets, sql.clone())
        .await
        .expect("authority boot");
    let open = |path: &str| {
        authority.client.call(
            RequestContext::root(),
            OpenActor {
                parent: authority.root,
                path: path.into(),
                rights: Rights::all(),
            },
        )
    };
    let docs_actor = open("documents").await.expect("docs actor");
    let drawers_actor = open("drawers").await.expect("drawers actor");
    let nodules_actor = open(NODULES_PATH).await.expect("nodules actor");
    let docs_namespace = authority
        .service
        .open_namespace(docs_actor.actor_id)
        .await
        .expect("docs namespace");
    let drawers_namespace = authority
        .service
        .open_namespace(drawers_actor.actor_id)
        .await
        .expect("drawers namespace");
    let (repo, repo_stop) = BigRepo::boot(
        runtime,
        big_repo::Config {
            node_identity_seed: [19; 32],
            storage: StorageConfig::Memory,
            scope_key: "big-nodule-e2e".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        },
    )
    .await
    .expect("repo boot");
    let schemas = Arc::new(
        SchemaRepo::open([
            crate::schema_group(),
            SchemaRegistrationGroup::new(
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
            ),
        ])
        .expect("schemas"),
    );
    let (indexes, index_stop) = Indexes::boot(
        Arc::clone(&repo),
        Arc::clone(&schemas),
        (*sql).clone(),
        NonZeroUsize::new(2).unwrap(),
    )
    .await
    .expect("indexes boot");
    let (drawer_access, provider, policy_stop) = PolicyAccess::open(
        Arc::clone(&repo),
        authority.client.clone(),
        drawers_actor.capability,
        drawers_namespace,
        (*sql).clone(),
        Arc::clone(&indexes.datum_set),
    )
    .await
    .expect("drawer access");
    let (docs, docs_stop) = big_cabinet::doc::prepare(DocsConfig {
        big_repo: Arc::clone(&repo),
        schema_repo: schemas,
        authority: authority.client.clone(),
        issuer: docs_actor.capability,
        namespace: docs_namespace,
        node_issuer: authority.node_issuer.clone(),
        access: Arc::clone(&drawer_access) as Arc<dyn DocAccess>,
        sql: (*sql).clone(),
        datum_set: Some(Arc::clone(&indexes.datum_set)),
        datum_ref: Some(Arc::clone(&indexes.datum_ref)),
    })
    .await
    .expect("docs prepare");
    let (worker, worker_stop) = provider.start(docs.client.clone());
    docs.recover().await.expect("docs recover");
    let (drawers, drawers_stop) = big_cabinet::boot_drawers(DrawerConfig { worker: &worker })
        .await
        .expect("drawers boot");
    grant_drawer_admin(
        &drawers,
        &drawers_actor,
        &drawers.local().config_id,
        nodules_actor.actor_id,
    )
    .await;
    let (nodules, stop) = boot_nodules(&authority, &docs, &drawers, &nodules_actor, &sql)
        .await
        .expect("nodule service boot");
    Node {
        authority,
        authority_stop,
        repo_stop,
        index_stop,
        policy_stop,
        docs_stop,
        worker: Some(worker),
        worker_stop,
        drawers_stop,
        drawers_actor,
        nodules_actor,
        docs,
        drawers,
        sql,
        nodules,
        stop: Some(stop),
    }
}

async fn grant_drawer_admin(
    drawers: &Drawers,
    grantor: &Actor,
    drawer_id: &DrawerId,
    subject_id: big_cabinet_core::dmeta::Uuid7,
) {
    let cap = drawers
        .client
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization::identity(grantor.capability),
                scope: DrawerScope {
                    drawers: BTreeMap::from([(drawer_id.clone(), DrawerAccess::Admin)]),
                    create: false,
                },
                rights: Rights::USE | Rights::DELEGATE,
            },
        )
        .await
        .expect("mint drawer cap");
    drawers
        .client
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: cap.capability_id,
                drawer_id: drawer_id.clone(),
                subject_id,
                access: DrawerAccess::Admin,
            },
        )
        .await
        .expect("grant drawer");
}

async fn boot_nodules(
    authority: &Authority,
    docs: &DocsRepo,
    drawers: &Drawers,
    nodules_actor: &Actor,
    sql: &TestDb,
) -> Result<(Nodules, StopToken), NodulesError> {
    let namespace = authority
        .service
        .open_namespace(nodules_actor.actor_id)
        .await
        .expect("nodules namespace");
    boot(NodulesConfig {
        authority: authority.client.clone(),
        docs: docs.client.clone(),
        drawers: drawers.client.clone(),
        config_drawer: drawers.local().config_id.clone(),
        actor: nodules_actor.clone(),
        actor_path: NODULES_PATH.into(),
        namespace,
        operator: authority.service.operator_uuid(),
        sql: (**sql).clone(),
    })
    .await
}

impl Node {
    /// Stop the nodule service alone, the way a node restart retires it before the authority.
    async fn restart_nodules(&mut self) -> Result<(), NodulesError> {
        self.stop.take().expect("booted").stop().await;
        let (nodules, stop) = boot_nodules(
            &self.authority,
            &self.docs,
            &self.drawers,
            &self.nodules_actor,
            &self.sql,
        )
        .await?;
        self.nodules = nodules;
        self.stop = Some(stop);
        Ok(())
    }

    async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            stop.stop().await;
        }
        self.drawers_stop.stop().await;
        self.worker_stop.stop().await;
        drop(self.worker.take());
        self.docs_stop.stop().await;
        self.policy_stop.stop().await;
        self.index_stop.stop().await;
        self.repo_stop.stop().await.expect("repo stop");
        self.authority_stop.stop().await;
    }

    fn operator(&self) -> Authorization {
        Authorization::identity(self.authority.root)
    }

    async fn call<P>(&self, params: P) -> Result<P::Output, NodulesError>
    where
        P: Operation<Output: service_utils_rs::Wire, Error = NodulesError>,
        NodulesProtocol: From<Envelope<P>>,
        NodulesMessage: From<WithChannels<Envelope<P>, NodulesProtocol>>,
        Envelope<P>: Channels<NodulesProtocol, Tx = Reply<P>, Rx = NoReceiver>,
    {
        self.nodules
            .client
            .call(RequestContext::root(), params)
            .await
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })
    }

    async fn admit(
        &self,
        authorization: Authorization,
        plug: Plug,
    ) -> Result<NoduleRecord, NodulesError> {
        self.call(AdmitNodule {
            authorization,
            plug,
            drawers: None,
        })
        .await
    }

    async fn get(
        &self,
        authorization: Authorization,
        plug_id: &str,
    ) -> Result<NoduleRecord, NodulesError> {
        self.call(GetNodule {
            authorization,
            plug_id: id(plug_id),
        })
        .await
    }

    async fn list(&self, authorization: Authorization) -> Result<Vec<PlugId>, NodulesError> {
        Ok(self
            .call(ListNodules { authorization })
            .await?
            .into_iter()
            .map(|record| record.plug_id)
            .collect())
    }

    /// An actor under the operator presenting its own identity: known to the authority, no
    /// standing with this domain.
    async fn bystander(&self) -> Authorization {
        let actor = self
            .authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: self.authority.root,
                    path: "bystander".into(),
                    rights: Rights::all(),
                },
            )
            .await
            .expect("bystander actor");
        Authorization::identity(actor.capability)
    }

    async fn mint(
        &self,
        authorization: Authorization,
        scope: NoduleScope,
        rights: Rights,
    ) -> Result<Authorization, NodulesError> {
        let capability = self
            .call(MintNoduleCapability {
                authorization,
                scope,
                rights,
            })
            .await?;
        Ok(Authorization {
            authority: capability.capability_id,
            signer: authorization.signer,
        })
    }

    /// The actor the nodule path names right now, opened by the test through the subsystem
    /// actor it holds, so a record's actor can be checked without the service's handle.
    async fn actor_at(&self, nodule_id: &str) -> Actor {
        self.authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: self.nodules_actor.capability,
                    path: format!("{NODULES_PATH}/{nodule_id}"),
                    rights: Rights::USE | Rights::NARROW,
                },
            )
            .await
            .expect("nodule actor")
    }
}

fn scope(admit: bool, plugs: &[&str]) -> NoduleScope {
    NoduleScope {
        admit,
        plugs: plugs.iter().map(|plug| id(plug)).collect(),
    }
}

#[tokio::test]
async fn admission_provisions_a_durable_actor_and_management_state_persists() {
    let mut node = node().await;
    let operator = node.operator();

    let record = node
        .admit(operator, plug("@example/photos", "1.0.0", true))
        .await
        .unwrap();
    assert_eq!(record.plug_id, id("@example/photos"));
    assert_eq!(record.nodule_id.as_deref(), Some("org.example.photos"));
    assert!(!record.enabled);
    assert_eq!(record.readiness, Readiness::Disabled);
    assert_eq!(record.config, Some(settings_of(true)));
    assert_eq!(record.error, None);
    assert!(!record.observed_heads.is_empty());
    assert_eq!(record.applied_heads.as_ref(), Some(&record.observed_heads));
    let actor = node.actor_at("org.example.photos").await;
    let record_actor = record.actor.as_ref().expect("provisioned actor");
    assert_eq!(record_actor.actor_id, actor.actor_id);
    assert_eq!(record_actor.public_key, actor.public_key);
    assert_eq!(actor.parent_actor_id, Some(node.nodules_actor.actor_id));

    let enabled = node
        .call(SetNoduleEnabled {
            authorization: operator,
            plug_id: id("@example/photos"),
            enabled: true,
        })
        .await
        .unwrap();
    assert_eq!(enabled.readiness, Readiness::ExecutorUnavailable);
    let configured = node
        .call(ConfigureNodule {
            authorization: operator,
            plug_id: id("@example/photos"),
            settings: settings_of(false),
        })
        .await
        .unwrap();
    assert_eq!(configured.config, Some(settings_of(false)));
    assert!(matches!(
        node.call(ConfigureNodule {
            authorization: operator,
            plug_id: id("@example/photos"),
            settings: [("thumbnails".to_owned(), json!("no"))]
                .into_iter()
                .collect(),
        })
        .await,
        Err(NodulesError::Rejected(_))
    ));
    node.admit(operator, plug("@example/schema", "0.1.0", false))
        .await
        .unwrap();
    assert!(matches!(
        node.call(ConfigureNodule {
            authorization: operator,
            plug_id: id("@example/schema"),
            settings: IndexMap::new(),
        })
        .await,
        Err(NodulesError::NoConfigSchema(_))
    ));
    assert!(matches!(
        node.get(operator, "@example/missing").await,
        Err(NodulesError::UnknownPlug(_))
    ));

    // An upgrade keeps the nodule's actor and the operator's settings.
    let upgraded = node
        .admit(operator, plug("@example/photos", "1.1.0", true))
        .await
        .unwrap();
    assert_eq!(upgraded.version, Some("1.1.0".parse().unwrap()));
    assert_eq!(
        upgraded.actor.as_ref().expect("provisioned actor").actor_id,
        actor.actor_id
    );
    assert!(upgraded.enabled);
    assert_eq!(upgraded.config, Some(settings_of(false)));

    node.restart_nodules().await.unwrap();
    let recovered = node.get(operator, "@example/photos").await.unwrap();
    assert_eq!(recovered, upgraded);
    assert_eq!(
        node.list(operator).await.unwrap(),
        vec![id("@example/photos"), id("@example/schema")]
    );
    let schema = node.get(operator, "@example/schema").await.unwrap();
    assert!(!schema.enabled);
    assert_eq!(schema.config, None);
    node.shutdown().await;
}

#[tokio::test]
async fn only_the_operator_and_handles_it_narrowed_administer_nodules() {
    let node = node().await;
    let operator = node.operator();
    let bystander = node.bystander().await;
    node.admit(operator, plug("@example/photos", "1.0.0", true))
        .await
        .unwrap();
    node.admit(operator, plug("@example/schema", "0.1.0", false))
        .await
        .unwrap();

    assert_eq!(
        node.admit(bystander, plug("@example/intruder", "1.0.0", false))
            .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(node.list(bystander).await, Err(NodulesError::Denied));
    assert_eq!(
        node.mint(bystander, scope(false, &[]), Rights::USE).await,
        Err(NodulesError::Denied)
    );

    let photos_only = node
        .mint(
            operator,
            scope(false, &["@example/photos"]),
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();
    assert_eq!(
        node.list(photos_only).await.unwrap(),
        vec![id("@example/photos")]
    );
    assert!(node.get(photos_only, "@example/photos").await.is_ok());
    assert_eq!(
        node.get(photos_only, "@example/schema").await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.admit(photos_only, plug("@example/photos", "1.1.0", true))
            .await,
        Err(NodulesError::Denied)
    );
    assert!(
        node.call(SetNoduleEnabled {
            authorization: photos_only,
            plug_id: id("@example/photos"),
            enabled: true,
        })
        .await
        .is_ok()
    );

    // A handle narrows to what it covers and nothing wider.
    assert_eq!(
        node.mint(photos_only, scope(true, &["@example/photos"]), Rights::USE)
            .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.mint(
            photos_only,
            scope(false, &["@example/photos", "@example/schema"]),
            Rights::USE
        )
        .await,
        Err(NodulesError::Denied)
    );
    let read_only = node
        .mint(photos_only, scope(false, &["@example/photos"]), Rights::USE)
        .await
        .unwrap();
    assert_eq!(
        node.list(read_only).await.unwrap(),
        vec![id("@example/photos")]
    );
    assert!(matches!(
        node.mint(read_only, scope(false, &[]), Rights::USE).await,
        Err(NodulesError::Authority(_))
    ));
    assert_eq!(
        node.call(SetNoduleDrawerGrants {
            authorization: photos_only,
            plug_id: id("@example/schema"),
            drawers: BTreeMap::new(),
        })
        .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.call(UninstallNodule {
            authorization: photos_only,
            plug_id: id("@example/schema"),
        })
        .await,
        Err(NodulesError::Denied)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_admit_scoped_handle_admits_only_its_own_plugs() {
    let node = node().await;
    let operator = node.operator();
    let photos_admin = node
        .mint(
            operator,
            scope(true, &["@example/photos"]),
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();

    // Install and upgrade of the named plug pass; any other plug id is denied, installed or not.
    node.admit(photos_admin, plug("@example/photos", "1.0.0", true))
        .await
        .unwrap();
    node.admit(photos_admin, plug("@example/photos", "1.1.0", true))
        .await
        .unwrap();
    assert_eq!(
        node.admit(photos_admin, plug("@example/schema", "0.1.0", false))
            .await,
        Err(NodulesError::Denied)
    );
    node.admit(operator, plug("@example/schema", "0.1.0", false))
        .await
        .unwrap();
    assert_eq!(
        node.admit(photos_admin, plug("@example/schema", "0.2.0", false))
            .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.get(operator, "@example/schema").await.unwrap().version,
        Some("0.1.0".parse().unwrap())
    );
    node.shutdown().await;
}

/// `plug(.., true)` whose settings schema also requires a string `endpoint`, so settings written
/// against the plain revision no longer conform to it.
fn plug_requiring_endpoint(id: &str, version: &str) -> Plug {
    let mut plug = plug(id, version, true);
    let Some(Contribution::Nodule(contribution)) = plug.contributions.first_mut() else {
        panic!("test plugs carry a nodule contribution");
    };
    let settings = &mut contribution.description["lexicons"][1]["defs"]["main"];
    settings["required"] = json!(["endpoint"]);
    settings["properties"]["endpoint"] = json!({ "type": "string" });
    contribution.description["config"]["defaults"]["endpoint"] = json!("https://example.org");
    plug
}

#[tokio::test]
async fn an_upgrade_that_invalidates_settings_is_refused_and_repaired_without_losing_actor_or_grants()
 {
    let node = node().await;
    let operator = node.operator();
    let present = node.drawers.local().present_id.clone();
    grant_drawer_admin(
        &node.drawers,
        &node.drawers_actor,
        &present,
        node.nodules_actor.actor_id,
    )
    .await;
    let v1 = node
        .call(AdmitNodule {
            authorization: operator,
            plug: plug("@example/photos", "1.0.0", true),
            drawers: Some(BTreeMap::from([(present.clone(), DrawerAccess::Write)])),
        })
        .await
        .unwrap();
    assert_eq!(v1.error, None);
    assert!(v1.drawers[&present].granted);
    node.call(ConfigureNodule {
        authorization: operator,
        plug_id: id("@example/photos"),
        settings: settings_of(false),
    })
    .await
    .unwrap();

    // The upgrade is admitted; the nodule is refused because the saved settings lack `endpoint`.
    let refused = node
        .admit(
            operator,
            plug_requiring_endpoint("@example/photos", "2.0.0"),
        )
        .await
        .unwrap();
    assert_eq!(refused.version, Some("2.0.0".parse().unwrap()));
    assert_eq!(refused.readiness, Readiness::Refused);
    assert!(refused.error.as_deref().unwrap().contains("endpoint"));
    assert_eq!(refused.config, Some(settings_of(false)));
    assert_eq!(refused.actor, v1.actor, "the durable actor is kept");
    // Existing reconcile semantics: a refused nodule is not Admitted, so its applied grants are
    // retracted. The desired grants stay in config and return on repair.
    assert!(refused.drawers.is_empty());

    let mut repaired = settings_of(false);
    repaired.insert("endpoint".into(), json!("https://example.org"));
    let record = node
        .call(ConfigureNodule {
            authorization: operator,
            plug_id: id("@example/photos"),
            settings: repaired.clone(),
        })
        .await
        .unwrap();
    assert_eq!(record.error, None);
    assert_eq!(record.readiness, Readiness::Disabled);
    assert_eq!(record.config, Some(repaired));
    assert_eq!(record.actor, v1.actor);
    assert!(record.drawers[&present].granted);
    node.shutdown().await;
}

#[tokio::test]
async fn drawer_grants_apply_downgrade_revoke_and_uninstall_cleanly() {
    let mut node = node().await;
    let operator = node.operator();
    let present = node.drawers.local().present_id.clone();
    let placement = format!("drawer {present}");
    let note_key: DatumKey = format!("{NOTE}/main").parse().unwrap();

    // Admit with a desired grant on `present` before `nodules_actor` has Admin on `present`:
    // the desired grant is persisted in config, but the binding is reported as blocked.
    let blocked = node
        .call(AdmitNodule {
            authorization: operator,
            plug: plug("@example/photos", "1.0.0", true),
            drawers: Some(BTreeMap::from([(present.clone(), DrawerAccess::Write)])),
        })
        .await
        .unwrap();
    assert_eq!(blocked.applied_heads, None);
    assert!(blocked.error.is_some());
    assert_eq!(
        blocked.drawers.get(&present),
        Some(&NoduleGrantStatus {
            desired: DrawerAccess::Write,
            granted: false,
            error: blocked.drawers[&present].error.clone(),
        })
    );
    assert!(blocked.drawers[&present].error.is_some());

    let actor = node.actor_at("org.example.photos").await;
    assert!(matches!(
        node.docs
            .client
            .call(
                RequestContext::root(),
                MintDocCapability {
                    authorization: Authorization::identity(actor.capability),
                    scope: DocScope {
                        create: BTreeSet::from([placement.clone()]),
                        ..DocScope::default()
                    },
                    rights: Rights::USE,
                },
            )
            .await,
        Err(ClientError::Operation(DocsError::Denied))
    ));

    // Once the operator grants `nodules_actor` Admin on `present`, reconciliation applies the
    // desired grant and the nodule actor can mint doc capabilities on `present`.
    grant_drawer_admin(
        &node.drawers,
        &node.drawers_actor,
        &present,
        node.nodules_actor.actor_id,
    )
    .await;
    let applied = node.get(operator, "@example/photos").await.unwrap();
    assert_eq!(applied.error, None);
    assert_eq!(
        applied.applied_heads.as_ref(),
        Some(&applied.observed_heads)
    );
    assert_eq!(
        applied.drawers.get(&present),
        Some(&NoduleGrantStatus {
            desired: DrawerAccess::Write,
            granted: true,
            error: None,
        })
    );

    let create_cap = node
        .docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(actor.capability),
                scope: DocScope {
                    create: BTreeSet::from([placement.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .expect("nodule actor can mint create cap");
    let receipt = node
        .docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability: create_cap.capability_id,
                command: CommandId::try_from("nodule-doc-1".to_owned()).unwrap(),
                placement: placement.clone(),
                datums: BTreeMap::from([(
                    note_key.clone(),
                    json!({"$type": NOTE, "value": "hello"}),
                )]),
            },
        )
        .await
        .expect("nodule actor creates doc");

    // Downgrading to Read retracts the Edit source capability (invalidating `create_cap`) and
    // leaves only Read access.
    let downgraded = node
        .call(SetNoduleDrawerGrants {
            authorization: operator,
            plug_id: id("@example/photos"),
            drawers: BTreeMap::from([(present.clone(), DrawerAccess::Read)]),
        })
        .await
        .unwrap();
    assert_eq!(
        downgraded.drawers.get(&present),
        Some(&NoduleGrantStatus {
            desired: DrawerAccess::Read,
            granted: true,
            error: None,
        })
    );
    assert!(matches!(
        node.docs
            .client
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: create_cap.capability_id,
                    command: CommandId::try_from("nodule-doc-2".to_owned()).unwrap(),
                    placement: placement.clone(),
                    datums: BTreeMap::from([(
                        note_key.clone(),
                        json!({"$type": NOTE, "value": "denied"}),
                    )]),
                },
            )
            .await,
        Err(ClientError::Operation(DocsError::Denied))
    ));
    let read_cap = node
        .docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(actor.capability),
                scope: DocScope {
                    read: BTreeSet::from([receipt.doc_id.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .expect("nodule actor can still mint read cap");
    node.docs
        .client
        .call(
            RequestContext::root(),
            ReadDatums {
                capability: read_cap.capability_id,
                doc_id: receipt.doc_id.clone(),
                at: At::Current,
                datums: Some(BTreeSet::from([note_key.clone()])),
                optimistic: false,
            },
        )
        .await
        .expect("nodule actor reads doc");

    // Restarting nodules preserves the applied Read grant across restarts.
    node.restart_nodules().await.unwrap();
    let after_restart = node.get(operator, "@example/photos").await.unwrap();
    assert_eq!(after_restart, downgraded);

    // Uninstalling removes the config datums and retracts the Read grant capability, while
    // re-admitting the same plug reopens the same durable actor.
    node.call(UninstallNodule {
        authorization: operator,
        plug_id: id("@example/photos"),
    })
    .await
    .unwrap();
    assert!(node.list(operator).await.unwrap().is_empty());
    assert!(matches!(
        node.get(operator, "@example/photos").await,
        Err(NodulesError::UnknownPlug(_))
    ));
    assert!(matches!(
        node.docs
            .client
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability: read_cap.capability_id,
                    doc_id: receipt.doc_id.clone(),
                    at: At::Current,
                    datums: Some(BTreeSet::from([note_key])),
                    optimistic: false,
                },
            )
            .await,
        Err(ClientError::Operation(DocsError::Denied))
    ));
    let readmitted = node
        .admit(operator, plug("@example/photos", "1.0.0", true))
        .await
        .unwrap();
    assert_eq!(
        readmitted
            .actor
            .as_ref()
            .expect("provisioned actor")
            .actor_id,
        actor.actor_id
    );
    node.shutdown().await;
}

#[tokio::test]
async fn invalid_imported_config_is_refused_per_nodule_and_repairable() {
    let node = node().await;
    let operator = node.operator();
    node.admit(operator, plug("@example/photos", "1.0.0", true))
        .await
        .unwrap();
    node.admit(operator, plug("@example/schema", "0.1.0", false))
        .await
        .unwrap();

    // Simulate an invalid imported settings datum written directly to the config drawer doc.
    let config_doc = DocId::from(node.drawers.local().config_id.clone());
    let doc_cap = node
        .docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DocScope {
                    write: BTreeSet::from([config_doc.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let bad_settings_key: DatumKey = format!("{}/@example/photos", NoduleSettings::ID)
        .parse()
        .unwrap();
    let bad_receipt = node
        .docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: doc_cap.capability_id,
                command: None,
                doc_id: config_doc,
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(
                    bad_settings_key,
                    json!({
                        "$type": NoduleSettings::ID,
                        "values": { "thumbnails": "not-a-bool" }
                    }),
                )]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();

    let refused = loop {
        let record = node.get(operator, "@example/photos").await.unwrap();
        if record.observed_heads == bad_receipt.heads {
            break record;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(refused.readiness, Readiness::Refused);
    assert!(refused.error.is_some());
    let healthy = node.get(operator, "@example/schema").await.unwrap();
    assert_eq!(healthy.readiness, Readiness::Disabled);
    assert_eq!(healthy.error, None);

    // Repairing the settings via ConfigureNodule restores the nodule to Admitted.
    let repaired = node
        .call(ConfigureNodule {
            authorization: operator,
            plug_id: id("@example/photos"),
            settings: settings_of(false),
        })
        .await
        .unwrap();
    assert_eq!(repaired.readiness, Readiness::Disabled);
    assert_eq!(repaired.config, Some(settings_of(false)));
    assert_eq!(repaired.error, None);
    node.shutdown().await;
}
