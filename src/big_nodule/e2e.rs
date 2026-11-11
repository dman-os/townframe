//! Service-level tests: every call goes through `Client<NodulesProtocol>` against a real
//! authority, so admission, actor provisioning, persistence and the capability checks are
//! exercised the way an embedder sees them.

use crate::Nodules;
use crate::NodulesConfig;
use crate::boot;
use crate::executor::NoduleExecutor;
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
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenActor;
use big_cabinet_core::authority::OpenedActor;
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
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::Contribution;
use big_cartridges_core::NoduleContribution;
use big_cartridges_core::example_cartridge;
use big_dispatch::Dispatch;
use big_dispatch::DispatchConfig;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::MintDispatchCapability;
use big_dispatch_core::Readiness;
use big_nodule_core::Config;
use big_nodule_core::Nodule;
use big_nodule_core::config::NoduleSettings;
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
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::Storage;
use indexmap::IndexMap;
use irpc::Channels;
use irpc::WithChannels;
use irpc::channel::none::NoReceiver;
use method_core::Method;
use secrets_rs::SecretStore;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::ClientError;
use service_utils_rs::Envelope;
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

mod dispatch;
mod executor;
mod init;
mod resolution;
mod schemas;
mod substitution;

const NODULES_PATH: &str = "nodules";
const NOTE: &str = "org.example.note";

/// A package whose nodule declares one record lexicon and, when `settings` is set, a config
/// schema with one boolean `thumbnails` setting defaulting to `true`.
fn cartridge(id: &str, version: &str, settings: bool) -> Cartridge {
    let nodule_id = format!("org.example.{}", id.rsplit('/').next().unwrap());
    let mut nodule = Nodule::default();
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
    let mut cartridge = Cartridge::new(id.parse().unwrap(), version.parse().unwrap());
    cartridge.authority = Some(nodule_id);
    cartridge
        .contributions
        .push(Contribution::Nodule(NoduleContribution {
            schema: Nodule::ID.into(),
            description: lexicon(serde_json::to_value(nodule).unwrap()),
            extra: Default::default(),
        }));
    cartridge
}

fn lexicon(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).unwrap()
}

fn settings_of(thumbnails: bool) -> IndexMap<String, Value> {
    [("thumbnails".to_owned(), json!(thumbnails))]
        .into_iter()
        .collect()
}

/// The schemas the nodule config document and its test notes need.
fn test_schemas() -> Arc<SchemaRepo> {
    Arc::new(
        SchemaRepo::open([
            crate::schema_group(),
            blob_schemas(),
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
    )
}

/// The operator introduces the nodule service to dispatch over every authority, privileged
/// methods excepted, as `big_node` boots it: a handle
/// bound to `nodules_actor`, with `DELEGATE` so the service can bind each nodule's own handle
/// to that nodule.
async fn grant_nodule_dispatch(
    authority: &Authority,
    dispatch: &Dispatch,
    nodules_actor: big_cabinet_core::dmeta::Uuid7,
) -> CapId {
    dispatch
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(authority.root).metadata(),
            MintDispatchCapability {
                scope: DispatchScope {
                    authorities: [DispatchScope::ALL_AUTHORITIES.to_owned()].into(),
                    ..Default::default()
                },
                rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                subject: Some(nodules_actor),
            },
        )
        .await
        .expect("dispatch authority for the nodule service")
        .capability_id
}

fn id(cartridge_id: &str) -> CartridgeId {
    cartridge_id.parse().unwrap()
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
    drawers_actor: OpenedActor,
    nodules_actor: OpenedActor,
    docs: DocsRepo,
    drawers: Drawers,
    sql: TestDb,
    schemas: Arc<SchemaRepo>,
    dispatch: Dispatch,
    dispatch_stop: StopToken,
    /// What the nodule service mints every nodule's dispatch handle from: the operator's grant of
    /// every method that is not privileged.
    dispatch_authority: CapId,
    nodules: Nodules,
    executor: Option<Arc<dyn NoduleExecutor>>,
    stop: Option<StopToken>,
}

/// The `Blob` datum Lexicon, which the nodule config document's artifact references need and the
/// embedder (big_node, through big_blobs) registers beside [`crate::schema_group`].
fn blob_schemas() -> SchemaRegistrationGroup<'static> {
    let bundle = big_cabinet_core::datum::Bundle::new(&big_blobs_core::UNIVERSE, Vec::new());
    let registrations = bundle
        .lexicons()
        .expect("blob declarations publish lexicons")
        .into_iter()
        .map(|lexicon| SchemaRegistration::new(lexicon.id, lexicon.file));
    SchemaRegistrationGroup::new(Some(bundle), registrations, Vec::new())
}

async fn node() -> Node {
    node_with(None).await
}

async fn node_with(executor: Option<Arc<dyn NoduleExecutor>>) -> Node {
    let keyring = keyring_core::mock::Store::new().expect("mock keyring")
        as Arc<keyring_core::CredentialStore>;
    let secrets = SecretStore::boot_with_store(keyring)
        .await
        .expect("mock keyring");
    let sql = test_support::db(&[]).expect("database");
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let (authority, authority_stop) =
        big_cabinet::authority::boot(&secrets, "test-node", sql.clone())
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
    let dispatch_actor = open("dispatch").await.expect("dispatch actor");
    let dispatch_namespace = authority
        .service
        .open_namespace(dispatch_actor.actor.actor_id)
        .await
        .expect("dispatch namespace");
    let (dispatch, dispatch_stop) = big_dispatch::boot(DispatchConfig {
        authority: authority.client.clone(),
        namespace: dispatch_namespace,
        operator: authority.service.operator_uuid(),
    });
    let docs_namespace = authority
        .service
        .open_namespace(docs_actor.actor.actor_id)
        .await
        .expect("docs namespace");
    let drawers_namespace = authority
        .service
        .open_namespace(drawers_actor.actor.actor_id)
        .await
        .expect("drawers namespace");
    let (repo, repo_stop) = BigRepo::boot(big_repo::Config {
        node_identity_seed: [19; 32],
        storage: Storage::memory(&runtime).unwrap(),
        scope_key: "big-nodule-e2e".into(),
        hidden_parts: Default::default(),
        automerge_frontier_group_scope: Default::default(),
        causal_checkpoint_group_scope: Default::default(),
        group_part_group_scope: Default::default(),
    })
    .await
    .expect("repo boot");
    let schemas = test_schemas();
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
        authority.service.operator_uuid(),
        (*sql).clone(),
        Arc::clone(&indexes.datum_set),
    )
    .await
    .expect("drawer access");
    let (docs, docs_stop) = big_cabinet::doc::prepare(DocsConfig {
        big_repo: Arc::clone(&repo),
        schema_repo: Arc::clone(&schemas),
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
        nodules_actor.actor.actor_id,
    )
    .await;
    let dispatch_authority =
        grant_nodule_dispatch(&authority, &dispatch, nodules_actor.actor.actor_id).await;
    let (nodules, stop) = boot_nodules(
        &authority,
        &docs,
        &drawers,
        &nodules_actor,
        &dispatch,
        dispatch_authority,
        &sql,
        &schemas,
        executor.clone(),
    )
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
        schemas,
        dispatch,
        dispatch_stop,
        dispatch_authority,
        nodules,
        executor,
        stop: Some(stop),
    }
}

async fn grant_drawer_admin(
    drawers: &Drawers,
    grantor: &OpenedActor,
    drawer_id: &DrawerId,
    subject_id: big_cabinet_core::dmeta::Uuid7,
) {
    let cap = drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(grantor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    drawers: BTreeMap::from([(drawer_id.clone(), DrawerAccess::Admin)]),
                    create: false,
                },
                rights: Rights::USE | Rights::DELEGATE,
                subject: None,
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

#[expect(
    clippy::too_many_arguments,
    reason = "test wiring of already-built services"
)]
async fn boot_nodules(
    authority: &Authority,
    docs: &DocsRepo,
    drawers: &Drawers,
    nodules_actor: &OpenedActor,
    dispatch: &Dispatch,
    dispatch_authority: CapId,
    sql: &TestDb,
    schemas: &Arc<SchemaRepo>,
    executor: Option<Arc<dyn NoduleExecutor>>,
) -> Result<(Nodules, StopToken), NodulesError> {
    let namespace = authority
        .service
        .open_namespace(nodules_actor.actor.actor_id)
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
        dispatch: Arc::clone(&dispatch.router),
        dispatch_client: dispatch.client.clone(),
        dispatch_authority,
        executor,
        schemas: Arc::clone(schemas),
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
            &self.dispatch,
            self.dispatch_authority,
            &self.sql,
            &self.schemas,
            self.executor.clone(),
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
        self.dispatch_stop.stop().await;
        self.authority_stop.stop().await;
    }

    fn operator(&self) -> Authorization {
        Authorization::identity(self.authority.root)
    }

    async fn call<P>(
        &self,
        authorization: Authorization,
        params: P,
    ) -> Result<P::Output, NodulesError>
    where
        P: Method<Output: method_core::Wire, Error = NodulesError>,
        NodulesProtocol: From<Envelope<P>>,
        NodulesMessage: From<WithChannels<Envelope<P>, NodulesProtocol>>,
        Envelope<P>: Channels<NodulesProtocol, Tx = Reply<P>, Rx = NoReceiver>,
    {
        self.nodules
            .client
            .call_with(RequestContext::root(), authorization.metadata(), params)
            .await
            .map_err(|error| match error {
                ClientError::Method(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })
    }

    async fn admit(
        &self,
        authorization: Authorization,
        cartridge: Cartridge,
    ) -> Result<NoduleRecord, NodulesError> {
        self.call(
            authorization,
            AdmitNodule {
                cartridge,
                drawers: None,
            },
        )
        .await
    }

    async fn get(
        &self,
        authorization: Authorization,
        cartridge_id: &str,
    ) -> Result<NoduleRecord, NodulesError> {
        self.call(
            authorization,
            GetNodule {
                cartridge_id: id(cartridge_id),
            },
        )
        .await
    }

    async fn list(&self, authorization: Authorization) -> Result<Vec<CartridgeId>, NodulesError> {
        Ok(self
            .call(authorization, ListNodules {})
            .await?
            .nodules
            .into_iter()
            .map(|record| record.cartridge_id)
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
            .call(authorization, MintNoduleCapability { scope, rights })
            .await?;
        Ok(Authorization {
            authority: capability.capability_id,
            signer: authorization.signer,
        })
    }

    /// The actor the nodule path names right now, opened by the test through the subsystem
    /// actor it holds, so a record's actor can be checked without the service's handle.
    async fn actor_at(&self, cartridge_id: &CartridgeId) -> OpenedActor {
        self.authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: self.nodules_actor.capability,
                    path: format!(
                        "{NODULES_PATH}/{}",
                        crate::store::actor_segment(cartridge_id)
                    ),
                    rights: Rights::USE | Rights::NARROW,
                },
            )
            .await
            .expect("nodule actor")
    }
}

fn scope(admit: bool, cartridges: &[&str]) -> NoduleScope {
    NoduleScope {
        admit,
        cartridges: cartridges.iter().map(|cartridge| id(cartridge)).collect(),
    }
}

#[tokio::test]
async fn admission_provisions_a_durable_actor_and_management_state_persists() {
    let mut node = node().await;
    let operator = node.operator();

    let record = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "1.0.0", true),
        )
        .await
        .unwrap();
    assert_eq!(record.cartridge_id, id(example_cartridge!("photos")));
    assert!(!record.enabled);
    assert_eq!(record.readiness, Readiness::Disabled);
    assert_eq!(record.config, Some(settings_of(true)));
    assert_eq!(record.error, None);
    assert!(!record.observed_heads.is_empty());
    assert_eq!(record.applied_heads.as_ref(), Some(&record.observed_heads));
    let actor = node.actor_at(&id(example_cartridge!("photos"))).await;
    let record_actor = record.actor.as_ref().expect("provisioned actor");
    assert_eq!(record_actor.actor_id, actor.actor.actor_id);
    assert_eq!(record_actor.public_key, actor.actor.public_key);
    assert_eq!(
        actor.actor.parent_actor_id,
        Some(node.nodules_actor.actor.actor_id)
    );

    let enabled = node
        .call(
            operator,
            SetNoduleEnabled {
                cartridge_id: id(example_cartridge!("photos")),
                enabled: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(enabled.readiness, Readiness::Ready);
    let configured = node
        .call(
            operator,
            ConfigureNodule {
                cartridge_id: id(example_cartridge!("photos")),
                settings: settings_of(false),
            },
        )
        .await
        .unwrap();
    assert_eq!(configured.config, Some(settings_of(false)));
    assert!(matches!(
        node.call(
            operator,
            ConfigureNodule {
                cartridge_id: id(example_cartridge!("photos")),
                settings: [("thumbnails".to_owned(), json!("no"))]
                    .into_iter()
                    .collect(),
            }
        )
        .await,
        Err(NodulesError::Rejected(_))
    ));
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();
    assert!(matches!(
        node.call(
            operator,
            ConfigureNodule {
                cartridge_id: id(example_cartridge!("schema")),
                settings: IndexMap::new(),
            }
        )
        .await,
        Err(NodulesError::NoConfigSchema(_))
    ));
    assert!(matches!(
        node.get(operator, example_cartridge!("missing")).await,
        Err(NodulesError::UnknownCartridge(_))
    ));

    // An upgrade keeps the nodule's actor and the operator's settings.
    let upgraded = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "1.1.0", true),
        )
        .await
        .unwrap();
    assert_eq!(upgraded.version, Some("1.1.0".parse().unwrap()));
    assert_eq!(
        upgraded.actor.as_ref().expect("provisioned actor").actor_id,
        actor.actor.actor_id
    );
    assert!(upgraded.enabled);
    assert_eq!(upgraded.config, Some(settings_of(false)));

    node.restart_nodules().await.unwrap();
    let recovered = node
        .get(operator, example_cartridge!("photos"))
        .await
        .unwrap();
    assert_eq!(recovered, upgraded);
    assert_eq!(
        node.list(operator).await.unwrap(),
        vec![
            id(example_cartridge!("photos")),
            id(example_cartridge!("schema"))
        ]
    );
    let schema = node
        .get(operator, example_cartridge!("schema"))
        .await
        .unwrap();
    assert!(!schema.enabled);
    assert_eq!(schema.config, None);
    node.shutdown().await;
}

#[tokio::test]
async fn an_enabled_cartridge_holds_its_authority_against_equal_and_nested_prefixes() {
    let node = node().await;
    let operator = node.operator();
    node.admit(
        operator,
        cartridge(example_cartridge!("photos"), "1.0.0", false),
    )
    .await
    .unwrap();
    node.set_enabled(example_cartridge!("photos"), true).await;

    // `org.example.photos` holds itself and everything under it, in either direction.
    for (name, authority) in [
        ("same", "org.example.photos"),
        ("nested", "org.example.photos.gallery"),
        ("parent", "org.example"),
    ] {
        // A nodule that declares nothing, so only its authority is in question.
        let mut rival = Cartridge::new(
            big_cartridges_core::test_support::cartridge_id(name),
            "1.0.0".parse().unwrap(),
        );
        rival
            .contributions
            .push(Contribution::Nodule(NoduleContribution {
                schema: Nodule::ID.into(),
                description: lexicon(serde_json::to_value(Nodule::default()).unwrap()),
                extra: Default::default(),
            }));
        rival.authority = Some(authority.into());
        // Installing a rival is not holding: the prefix is refused at enablement, which is what
        // lets a substitute for a cartridge be installed beside it.
        let rival_id = rival.id.clone();
        node.admit(operator, rival).await.unwrap();
        let refused = node
            .call(
                operator,
                SetNoduleEnabled {
                    cartridge_id: rival_id,
                    enabled: true,
                },
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            refused.contains("overlaps `org.example.photos`"),
            "{name}: {refused}"
        );
    }

    // Whole segments only: a sibling that merely shares a string prefix is no conflict.
    node.admit(
        operator,
        cartridge(example_cartridge!("photosynthesis"), "1.0.0", false),
    )
    .await
    .unwrap();
    node.shutdown().await;
}

#[tokio::test]
async fn only_the_operator_and_handles_it_narrowed_administer_nodules() {
    let node = node().await;
    let operator = node.operator();
    let bystander = node.bystander().await;
    node.admit(
        operator,
        cartridge(example_cartridge!("photos"), "1.0.0", true),
    )
    .await
    .unwrap();
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();

    assert_eq!(
        node.admit(
            bystander,
            cartridge(example_cartridge!("intruder"), "1.0.0", false)
        )
        .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(node.list(bystander).await, Err(NodulesError::Denied));
    assert_eq!(
        node.mint(bystander, scope(false, &[]), Rights::USE).await,
        Err(NodulesError::Denied)
    );

    // A handle is bound to the actor it was minted for: another actor presenting the id under
    // its own signer is refused, whatever the handle would let its holder do.
    let held = node
        .mint(
            operator,
            scope(true, &[example_cartridge!("photos")]),
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();
    let stolen = Authorization {
        authority: held.authority,
        signer: bystander.signer,
    };
    assert_eq!(node.list(stolen).await, Err(NodulesError::Denied));
    assert_eq!(
        node.mint(stolen, scope(false, &[]), Rights::USE).await,
        Err(NodulesError::Denied)
    );
    assert!(node.list(held).await.is_ok());

    let photos_only = node
        .mint(
            operator,
            scope(false, &[example_cartridge!("photos")]),
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();
    assert_eq!(
        node.list(photos_only).await.unwrap(),
        vec![id(example_cartridge!("photos"))]
    );
    assert!(
        node.get(photos_only, example_cartridge!("photos"))
            .await
            .is_ok()
    );
    assert_eq!(
        node.get(photos_only, example_cartridge!("schema")).await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.admit(
            photos_only,
            cartridge(example_cartridge!("photos"), "1.1.0", true)
        )
        .await,
        Err(NodulesError::Denied)
    );
    assert!(
        node.call(
            photos_only,
            SetNoduleEnabled {
                cartridge_id: id(example_cartridge!("photos")),
                enabled: true,
            }
        )
        .await
        .is_ok()
    );

    // A handle narrows to what it covers and nothing wider.
    assert_eq!(
        node.mint(
            photos_only,
            scope(true, &[example_cartridge!("photos")]),
            Rights::USE
        )
        .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.mint(
            photos_only,
            scope(
                false,
                &[example_cartridge!("photos"), example_cartridge!("schema")]
            ),
            Rights::USE
        )
        .await,
        Err(NodulesError::Denied)
    );
    let read_only = node
        .mint(
            photos_only,
            scope(false, &[example_cartridge!("photos")]),
            Rights::USE,
        )
        .await
        .unwrap();
    assert_eq!(
        node.list(read_only).await.unwrap(),
        vec![id(example_cartridge!("photos"))]
    );
    assert!(matches!(
        node.mint(read_only, scope(false, &[]), Rights::USE).await,
        Err(NodulesError::Authority(_))
    ));
    assert_eq!(
        node.call(
            photos_only,
            SetNoduleDrawerGrants {
                cartridge_id: id(example_cartridge!("schema")),
                drawers: BTreeMap::new(),
            }
        )
        .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.call(
            photos_only,
            UninstallNodule {
                cartridge_id: id(example_cartridge!("schema")),
            }
        )
        .await,
        Err(NodulesError::Denied)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_admit_scoped_handle_admits_only_its_own_cartridges() {
    let node = node().await;
    let operator = node.operator();
    let photos_admin = node
        .mint(
            operator,
            scope(true, &[example_cartridge!("photos")]),
            Rights::USE | Rights::NARROW,
        )
        .await
        .unwrap();

    // Install and upgrade of the named cartridge pass; any other cartridge id is denied, installed or not.
    node.admit(
        photos_admin,
        cartridge(example_cartridge!("photos"), "1.0.0", true),
    )
    .await
    .unwrap();
    node.admit(
        photos_admin,
        cartridge(example_cartridge!("photos"), "1.1.0", true),
    )
    .await
    .unwrap();
    assert_eq!(
        node.admit(
            photos_admin,
            cartridge(example_cartridge!("schema"), "0.1.0", false)
        )
        .await,
        Err(NodulesError::Denied)
    );
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();
    assert_eq!(
        node.admit(
            photos_admin,
            cartridge(example_cartridge!("schema"), "0.2.0", false)
        )
        .await,
        Err(NodulesError::Denied)
    );
    assert_eq!(
        node.get(operator, example_cartridge!("schema"))
            .await
            .unwrap()
            .version,
        Some("0.1.0".parse().unwrap())
    );
    node.shutdown().await;
}

/// `cartridge(.., true)` whose settings schema also requires a string `endpoint`, so settings written
/// against the plain revision no longer conform to it.
fn cartridge_requiring_endpoint(id: &str, version: &str) -> Cartridge {
    let mut cartridge = cartridge(id, version, true);
    let Some(Contribution::Nodule(contribution)) = cartridge.contributions.first_mut() else {
        panic!("test cartridges carry a nodule contribution");
    };
    let settings = &mut contribution.description["lexicons"][1]["defs"]["main"];
    settings["required"] = json!(["endpoint"]);
    settings["properties"]["endpoint"] = json!({ "type": "string" });
    contribution.description["config"]["defaults"]["endpoint"] = json!("https://example.org");
    cartridge
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
        node.nodules_actor.actor.actor_id,
    )
    .await;
    let v1 = node
        .call(
            operator,
            AdmitNodule {
                cartridge: cartridge(example_cartridge!("photos"), "1.0.0", true),
                drawers: Some(BTreeMap::from([(present.clone(), DrawerAccess::Write)])),
            },
        )
        .await
        .unwrap();
    assert_eq!(v1.error, None);
    assert!(v1.drawers[&present].granted);
    node.call(
        operator,
        ConfigureNodule {
            cartridge_id: id(example_cartridge!("photos")),
            settings: settings_of(false),
        },
    )
    .await
    .unwrap();

    // The upgrade is admitted; the nodule is refused because the saved settings lack `endpoint`.
    let refused = node
        .admit(
            operator,
            cartridge_requiring_endpoint(example_cartridge!("photos"), "2.0.0"),
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
        .call(
            operator,
            ConfigureNodule {
                cartridge_id: id(example_cartridge!("photos")),
                settings: repaired.clone(),
            },
        )
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
        .call(
            operator,
            AdmitNodule {
                cartridge: cartridge(example_cartridge!("photos"), "1.0.0", true),
                drawers: Some(BTreeMap::from([(present.clone(), DrawerAccess::Write)])),
            },
        )
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

    let actor = node.actor_at(&id(example_cartridge!("photos"))).await;
    assert!(matches!(
        node.docs
            .client
            .call_with(
                RequestContext::root(),
                Authorization::identity(actor.capability).metadata(),
                MintDocCapability {
                    scope: DocScope {
                        create: BTreeSet::from([placement.clone()]),
                        ..DocScope::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await,
        Err(ClientError::Method(DocsError::Denied))
    ));

    // Once the operator grants `nodules_actor` Admin on `present`, reconciliation applies the
    // desired grant and the nodule actor can mint doc capabilities on `present`.
    grant_drawer_admin(
        &node.drawers,
        &node.drawers_actor,
        &present,
        node.nodules_actor.actor.actor_id,
    )
    .await;
    let applied = node
        .get(operator, example_cartridge!("photos"))
        .await
        .unwrap();
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
        .call_with(
            RequestContext::root(),
            Authorization::identity(actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    create: BTreeSet::from([placement.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
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
        .call(
            operator,
            SetNoduleDrawerGrants {
                cartridge_id: id(example_cartridge!("photos")),
                drawers: BTreeMap::from([(present.clone(), DrawerAccess::Read)]),
            },
        )
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
        Err(ClientError::Method(DocsError::Denied))
    ));
    let read_cap = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    read: BTreeSet::from([receipt.doc_id.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
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
    let after_restart = node
        .get(operator, example_cartridge!("photos"))
        .await
        .unwrap();
    assert_eq!(after_restart, downgraded);

    // Uninstalling removes the config datums and retracts the Read grant capability, while
    // re-admitting the same cartridge reopens the same durable actor.
    node.call(
        operator,
        UninstallNodule {
            cartridge_id: id(example_cartridge!("photos")),
        },
    )
    .await
    .unwrap();
    assert!(node.list(operator).await.unwrap().is_empty());
    assert!(matches!(
        node.get(operator, example_cartridge!("photos")).await,
        Err(NodulesError::UnknownCartridge(_))
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
        Err(ClientError::Method(DocsError::Denied))
    ));
    let readmitted = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "1.0.0", true),
        )
        .await
        .unwrap();
    assert_eq!(
        readmitted
            .actor
            .as_ref()
            .expect("provisioned actor")
            .actor_id,
        actor.actor.actor_id
    );
    node.shutdown().await;
}

#[tokio::test]
async fn invalid_imported_config_is_refused_per_nodule_and_repairable() {
    let node = node().await;
    let operator = node.operator();
    node.admit(
        operator,
        cartridge(example_cartridge!("photos"), "1.0.0", true),
    )
    .await
    .unwrap();
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();

    // Simulate an invalid imported settings datum written directly to the config drawer doc.
    let config_doc = DocId::from(node.drawers.local().config_id.clone());
    let doc_cap = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    write: BTreeSet::from([config_doc.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap();
    let bad_settings_key: DatumKey =
        format!("{}/{}", NoduleSettings::ID, example_cartridge!("photos"))
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
        let record = node
            .get(operator, example_cartridge!("photos"))
            .await
            .unwrap();
        if record.observed_heads == bad_receipt.heads {
            break record;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(refused.readiness, Readiness::Refused);
    assert!(refused.error.is_some());
    let healthy = node
        .get(operator, example_cartridge!("schema"))
        .await
        .unwrap();
    assert_eq!(healthy.readiness, Readiness::Disabled);
    assert_eq!(healthy.error, None);

    // Repairing the settings via ConfigureNodule restores the nodule to Admitted.
    let repaired = node
        .call(
            operator,
            ConfigureNodule {
                cartridge_id: id(example_cartridge!("photos")),
                settings: settings_of(false),
            },
        )
        .await
        .unwrap();
    assert_eq!(repaired.readiness, Readiness::Disabled);
    assert_eq!(repaired.config, Some(settings_of(false)));
    assert_eq!(repaired.error, None);
    node.shutdown().await;
}
