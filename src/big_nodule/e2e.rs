//! Service-level tests: every call goes through `Client<NodulesProtocol>` against a real
//! authority, so admission, actor provisioning, persistence and the capability checks are
//! exercised the way an embedder sees them.

use crate::Nodules;
use crate::NodulesConfig;
use crate::boot;
use crate::executor::NoduleExecutor;
use big_blobs_core::BlobsScope;
use big_blobs_core::MintBlobsCapability;
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
use big_cabinet_core::DatumPin;
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
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::EntryAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::GrantDrawer;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::ComponentExport;
use big_cartridges_core::MicroContribution;
use big_cartridges_core::Necessity;
use big_cartridges_core::Nodule as ManifestNodule;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindMethod;

use big_cartridges_core::Routine;
use big_cartridges_core::RoutineImpl;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::config::CartridgeInstall;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::example_cartridge;
use big_cartridges_core::protocol::CartridgeInstallRecord;
use big_cartridges_core::protocol::CartridgesError;
use big_cartridges_core::protocol::CartridgesScope;
use big_cartridges_core::protocol::ClearCartridgeConfig;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::GetCartridgeVersion;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::ListCartridgeInstalls;
use big_cartridges_core::protocol::MintCartridgesCapability;
use big_cartridges_core::protocol::ObserveCartridgeInstalls;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::protocol::UninstallCartridge;
use big_cartridges_core::registry::version_pin;
use big_cartridges_core::test_support::FixtureCartridge;
use big_dispatch::Dispatch;
use big_dispatch::DispatchConfig;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::MintDispatchCapability;
use big_dispatch_core::Readiness;

use big_nodule_core::protocol::GetNodule;
use big_nodule_core::protocol::ListNodules;
use big_nodule_core::protocol::MintNoduleCapability;
use big_nodule_core::protocol::NoduleGrantStatus;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NoduleScope;
use big_nodule_core::protocol::NodulesError;
use big_nodule_core::protocol::NodulesMessage;
use big_nodule_core::protocol::NodulesProtocol;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::Storage;
use futures::StreamExt;
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

mod dev_takeover;
mod dispatch;
mod executor;
mod init;
mod resolution;
mod routing;
mod schemas;

const NODULES_PATH: &str = "nodules";
const NOTE: &str = "org.example.note";

/// A package whose nodule declares one record lexicon and, when `settings` is set, one settings
/// datum with a boolean `thumbnails` member.
fn cartridge(id: &str, version: &str, settings: bool) -> FixtureCartridge {
    let nodule_id = format!("org.example.{}", id.rsplit('/').next().unwrap());
    let item = lexicon(json!({
        "lexicon": 1,
        "id": format!("{nodule_id}.item"),
        "defs": {
            "main": {
                "type": "record",
                "key": "tid",
                "record": { "type": "object", "properties": {} }
            }
        }
    }));
    let mut lexicons = vec![item];
    let nodule = ManifestNodule::default();
    if settings {
        let settings_id = format!("{nodule_id}.settings");
        lexicons.push(lexicon(json!({
            "lexicon": 1,
            "id": settings_id,
            "defs": {
                "main": {
                    "type": "object",
                    "properties": { "thumbnails": { "type": "boolean" } }
                }
            }
        })));
    }
    let mut cartridge = FixtureCartridge::new(Cartridge::new(
        id.parse().unwrap(),
        version.parse().unwrap(),
    ));
    cartridge.authority = Some(nodule_id);
    cartridge.nodule = Some(nodule);
    cartridge.settings = settings
        .then(|| {
            format!("org.example.{}.settings", id.rsplit('/').next().unwrap())
                .parse()
                .unwrap()
        })
        .into_iter()
        .collect();
    for file in lexicons {
        cartridge.with_lexicon(file);
    }
    cartridge
}

fn nodule_mut(cartridge: &mut FixtureCartridge) -> &mut ManifestNodule {
    cartridge.nodule.as_mut().expect("fixture carries a nodule")
}

fn add_routine(
    cartridge: &mut FixtureCartridge,
    key: &str,
    operation: &str,
    component: &str,
    export: &str,
    uses: BTreeMap<String, RoutineUse>,
) {
    nodule_mut(cartridge).routines.insert(
        key.into(),
        Routine {
            r#impl: RoutineImpl::ComponentExport(ComponentExport {
                component: component.into(),
                export: export.into(),
                extra: Default::default(),
            }),
            implements: operation.parse().unwrap(),
            uses,
            extra: Default::default(),
        },
    );
}

fn add_method(
    cartridge: &mut FixtureCartridge,
    binding: &str,
    operation: &str,
    necessity: Necessity,
    min_revision: Option<u32>,
) {
    cartridge.permbinds.insert(
        binding.into(),
        Permbind::Method(PermbindMethod {
            nsid: operation.parse().unwrap(),
            description: binding.into(),
            necessity,
            min_revision,
            extra: Default::default(),
        }),
    );
}

fn lexicon(value: Value) -> IndexMap<String, Value> {
    serde_json::from_value(value).unwrap()
}

/// The schemas the nodule config document and its test notes need.
fn test_schemas() -> Arc<SchemaRepo> {
    Arc::new(
        SchemaRepo::open(
            [crate::schema_group()]
                .into_iter()
                .chain(big_cartridges::schema_groups())
                .chain([
                    Node::blob_schemas(),
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
                ]),
        )
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
    /// Where the pinned versions live (a registry drawer, as `big_node` creates it) and where
    /// the cartridge config documents live (the shared config drawer the service creates).
    registry_drawer: DrawerId,
    shared_config_drawer: DrawerId,
    /// The blob store the registry stages component bytes from, and the registry service the
    /// nodule service resolves pinned versions through, booted as `big_node` boots them.
    blobs: big_blobs::Blobs,
    blobs_stop: StopToken,
    cartridges: big_cartridges::Cartridges,
    cartridges_stop: StopToken,
    /// The read-only handle the nodule service presents to the registry (see
    /// [`NodulesConfig::cartridges_authorization`]).
    cartridges_authorization: Authorization,
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
    allow_slot_takeover: bool,
    stop: Option<StopToken>,
}

async fn node() -> Node {
    node_with(None).await
}

async fn node_with(executor: Option<Arc<dyn NoduleExecutor>>) -> Node {
    node_with_takeover(executor, false).await
}

/// [`node_with`] on a node whose `dev.allow_slot_takeover` is `allow_slot_takeover`.
async fn node_with_takeover(
    executor: Option<Arc<dyn NoduleExecutor>>,
    allow_slot_takeover: bool,
) -> Node {
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
    let (dispatch, dispatch_stop) = Node::boot_dispatch(&authority, &dispatch_actor).await;
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
    let (registry_drawer, shared_config_drawer) = boot_config_drawers(
        &drawers,
        &drawers_actor,
        &drawer_access,
        &nodules_actor,
        &sql,
    )
    .await;
    let (blobs, blobs_stop, cartridges, cartridges_stop, cartridges_authorization) =
        Node::boot_registry(
            &authority,
            &docs,
            &drawers,
            &drawers_actor,
            &registry_drawer,
            &nodules_actor,
            &sql,
        )
        .await;
    let dispatch_authority =
        grant_nodule_dispatch(&authority, &dispatch, nodules_actor.actor.actor_id).await;
    let (nodules, stop) = boot_nodules(
        &authority,
        &docs,
        &drawers,
        &nodules_actor,
        &registry_drawer,
        &shared_config_drawer,
        &dispatch,
        dispatch_authority,
        &cartridges,
        cartridges_authorization,
        &sql,
        &schemas,
        executor.clone(),
        allow_slot_takeover,
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
        registry_drawer,
        shared_config_drawer,
        blobs,
        blobs_stop,
        cartridges,
        cartridges_stop,
        cartridges_authorization,
        docs,
        drawers,
        sql,
        schemas,
        dispatch,
        dispatch_stop,
        dispatch_authority,
        nodules,
        executor,
        allow_slot_takeover,
        stop: Some(stop),
    }
}

/// The drawers the nodule service works in, as `big_node` boots them: the node's config drawer
/// (Admin to the service), the registry drawer whose versions installs pin (Read), and the shared
/// config drawer the service creates for the cartridge config documents. Returns the last two.
async fn boot_config_drawers(
    drawers: &Drawers,
    drawers_actor: &OpenedActor,
    drawer_access: &PolicyAccess,
    nodules_actor: &OpenedActor,
    sql: &TestDb,
) -> (DrawerId, DrawerId) {
    grant_drawer(
        drawers,
        drawers_actor,
        &drawers.local().config_id,
        nodules_actor.actor.actor_id,
        DrawerAccess::Admin,
    )
    .await;
    let registry_drawer =
        big_cartridges::ensure_registry_drawer(big_cartridges::RegistryDrawerConfig {
            drawers,
            actor: drawers_actor,
            access: drawer_access,
            sql: (**sql).clone(),
        })
        .await
        .expect("registry drawer");
    grant_drawer(
        drawers,
        drawers_actor,
        &registry_drawer,
        nodules_actor.actor.actor_id,
        DrawerAccess::Read,
    )
    .await;
    let shared_config_drawer =
        crate::ensure_shared_config_drawer(crate::SharedConfigDrawerConfig {
            drawers,
            actor: nodules_actor,
            access: drawer_access,
            sql: (**sql).clone(),
        })
        .await
        .expect("shared config drawer");
    (registry_drawer, shared_config_drawer)
}

/// The drawers actor, as the drawer's owner, grants `access` on `drawer_id` to `subject_id`.
async fn grant_drawer(
    drawers: &Drawers,
    grantor: &OpenedActor,
    drawer_id: &DrawerId,
    subject_id: big_cabinet_core::dmeta::Uuid7,
    access: DrawerAccess,
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
                access,
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
    registry_drawer: &DrawerId,
    shared_config_drawer: &DrawerId,
    dispatch: &Dispatch,
    dispatch_authority: CapId,
    cartridges: &big_cartridges::Cartridges,
    cartridges_authorization: Authorization,
    sql: &TestDb,
    schemas: &Arc<SchemaRepo>,
    executor: Option<Arc<dyn NoduleExecutor>>,
    allow_slot_takeover: bool,
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
        registry_drawer: registry_drawer.clone(),
        shared_config_drawer: shared_config_drawer.clone(),
        actor: nodules_actor.clone(),
        actor_path: NODULES_PATH.into(),
        namespace,
        operator: authority.service.operator_uuid(),
        dispatch: Arc::clone(&dispatch.router),
        dispatch_client: dispatch.client.clone(),
        dispatch_authority,
        cartridges: cartridges.client.clone(),
        cartridges_authorization,
        executor,
        schemas: Arc::clone(schemas),
        allow_slot_takeover,
        sql: (**sql).clone(),
    })
    .await
}

impl Node {
    /// Boots dispatch under its own actor's namespace.
    async fn boot_dispatch(
        authority: &Authority,
        dispatch_actor: &OpenedActor,
    ) -> (Dispatch, StopToken) {
        let namespace = authority
            .service
            .open_namespace(dispatch_actor.actor.actor_id)
            .await
            .expect("dispatch namespace");
        big_dispatch::boot(DispatchConfig {
            authority: authority.client.clone(),
            namespace,
            operator: authority.service.operator_uuid(),
        })
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

    /// Boots the blob store and the registry service over `registry_drawer`, as `big_node` does,
    /// and mints the read-only handle the nodule service presents to the registry.
    async fn boot_registry(
        authority: &Authority,
        docs: &DocsRepo,
        drawers: &Drawers,
        drawers_actor: &OpenedActor,
        registry_drawer: &DrawerId,
        nodules_actor: &OpenedActor,
        sql: &TestDb,
    ) -> (
        big_blobs::Blobs,
        StopToken,
        big_cartridges::Cartridges,
        StopToken,
        Authorization,
    ) {
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
        let cartridges_actor = open("cartridges").await.expect("cartridges actor");
        let blobs_actor = open("blobs").await.expect("blobs actor");
        let blobs_namespace = authority
            .service
            .open_namespace(blobs_actor.actor.actor_id)
            .await
            .expect("blobs namespace");
        let (blobs, blobs_stop) = big_blobs::boot(big_blobs::Config {
            storage: big_blobs::Storage::memory().expect("blob storage"),
            sql: (*sql).clone(),
            authority: authority.client.clone(),
            namespace: blobs_namespace,
            operator: authority.service.operator_uuid(),
        })
        .await
        .expect("blobs boot");
        grant_drawer(
            drawers,
            drawers_actor,
            registry_drawer,
            cartridges_actor.actor.actor_id,
            DrawerAccess::Admin,
        )
        .await;
        let cartridges_namespace = authority
            .service
            .open_namespace(cartridges_actor.actor.actor_id)
            .await
            .expect("cartridges namespace");
        let (cartridges, cartridges_stop) =
            big_cartridges::boot(big_cartridges::CartridgesConfig {
                authority: authority.client.clone(),
                docs: docs.client.clone(),
                drawers: drawers.client.clone(),
                registry_drawer: registry_drawer.clone(),
                actor: cartridges_actor,
                namespace: cartridges_namespace,
                operator: authority.service.operator_uuid(),
                blobs: Arc::clone(&blobs.service),
                node_key: big_cartridges::NodeKey {
                    public_key: authority.service.node_public_key(),
                    sign: Arc::new({
                        let service = Arc::clone(&authority.service);
                        move |message: &[u8]| {
                            let payload = message
                                .strip_prefix(big_cartridges_core::signature::SIGNING_DOMAIN)
                                .expect("the registry only signs cartridge signing messages");
                            service.sign_as_node(
                                big_cartridges_core::signature::SIGNING_DOMAIN,
                                payload,
                            )
                        }
                    }),
                },
                sql: (*sql).clone(),
            })
            .await
            .expect("cartridges boot");
        let cartridges_authorization = Authorization {
            authority: cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    Authorization::identity(authority.root).metadata(),
                    MintCartridgesCapability {
                        scope: CartridgesScope::default(),
                        rights: Rights::USE,
                        subject: Some(nodules_actor.actor.actor_id),
                    },
                )
                .await
                .expect("a read-only registry handle")
                .capability_id,
            signer: nodules_actor.capability,
        };
        (
            blobs,
            blobs_stop,
            cartridges,
            cartridges_stop,
            cartridges_authorization,
        )
    }

    /// Writes `set` straight into the node's config drawer document, as any client holding a
    /// capability on it can, bypassing the nodule service's own validation.
    async fn write_config_datums(&self, set: BTreeMap<DatumKey, Value>) -> WriteReceipt {
        let config_doc = DocId::from(self.drawers.local().config_id.clone());
        let doc_cap = self
            .docs
            .client
            .call_with(
                RequestContext::root(),
                Authorization::identity(self.drawers_actor.capability).metadata(),
                MintDocCapability {
                    scope: DocScope {
                        write: std::collections::BTreeMap::from([(
                            config_doc.clone(),
                            big_cabinet_core::doc::DatumKinds::All,
                        )]),
                        ..DocScope::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .unwrap();
        self.docs
            .client
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability: doc_cap.capability_id,
                    command: None,
                    doc_id: config_doc,
                    mode: WriteMode::AtCurrent,
                    set,
                    remove: BTreeSet::new(),
                },
            )
            .await
            .unwrap()
    }

    /// Writes `set` straight into the cartridge's config document, past the nodule service.
    async fn write_cartridge_datums(
        &self,
        cartridge_id: &str,
        set: BTreeMap<DatumKey, Value>,
    ) -> WriteReceipt {
        let doc_id = self.install_of(cartridge_id).await.config_doc;
        let capability = self
            .docs
            .client
            .call_with(
                RequestContext::root(),
                self.operator().metadata(),
                MintDocCapability {
                    scope: DocScope {
                        write: std::collections::BTreeMap::from([(
                            doc_id.clone(),
                            big_cabinet_core::doc::DatumKinds::All,
                        )]),
                        ..DocScope::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .unwrap()
            .capability_id;
        self.docs
            .client
            .call(
                RequestContext::root(),
                WriteDatums {
                    capability,
                    command: None,
                    doc_id,
                    mode: WriteMode::AtCurrent,
                    set,
                    remove: BTreeSet::new(),
                },
            )
            .await
            .unwrap()
    }

    /// Waits for the nodule service to reconcile the node's config document at `receipt`'s heads.
    /// The heads a record observes are the node document's, then each cartridge document's.
    async fn reconciled_at(&self, receipt: &WriteReceipt, cartridge_id: &str) -> NoduleRecord {
        loop {
            let record = self.get(self.operator(), cartridge_id).await.unwrap();
            if record.observed_heads.starts_with(&receipt.heads) {
                break record;
            }
            tokio::task::yield_now().await;
        }
    }

    /// [`Self::restart_nodules`] on a node whose `dev.allow_slot_takeover` is now `allow`.
    async fn restart_with_takeover(&mut self, allow: bool) {
        self.allow_slot_takeover = allow;
        self.restart_nodules().await.unwrap();
    }

    /// Stop the nodule service alone, the way a node restart retires it before the authority.
    async fn restart_nodules(&mut self) -> Result<(), NodulesError> {
        self.stop.take().expect("booted").stop().await;
        let (nodules, stop) = boot_nodules(
            &self.authority,
            &self.docs,
            &self.drawers,
            &self.nodules_actor,
            &self.registry_drawer,
            &self.shared_config_drawer,
            &self.dispatch,
            self.dispatch_authority,
            &self.cartridges,
            self.cartridges_authorization,
            &self.sql,
            &self.schemas,
            self.executor.clone(),
            self.allow_slot_takeover,
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
        self.cartridges_stop.stop().await;
        self.blobs_stop.stop().await;
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

    async fn call<P>(&self, authorization: Authorization, params: P) -> Result<P::Output, P::Error>
    where
        P: Method<Output: method_core::Wire>,
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

    /// Publishes `cartridge` through the registry as the operator, the way a publisher does:
    /// its component bytes go into the blob store, its payloads are staged into the registry
    /// document, and the envelope (signed as the example namespace when unsigned) carrying the
    /// staged pins is published. Returns the pin of the version at the heads it was published at.
    async fn publish(&self, cartridge: FixtureCartridge) -> DatumPin {
        let put = self
            .blobs
            .client
            .call_with(
                RequestContext::root(),
                self.operator().metadata(),
                MintBlobsCapability {
                    scope: BlobsScope {
                        put: true,
                        ..Default::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .expect("a blob put handle")
            .capability_id;
        for bytes in cartridge.blobs.values() {
            self.blobs
                .put(
                    RequestContext::root(),
                    put,
                    method_core::ByteBody::from_bytes(bytes.clone()),
                )
                .await
                .expect("a component's bytes");
        }
        // A version is published once: the registry refuses another envelope under its label, so a
        // test that installs the same fixture again (after a refusal at install, say) reuses it.
        let existing = self
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                self.operator().metadata(),
                GetCartridgeVersion {
                    id: cartridge.id.clone(),
                    version: cartridge.version.clone(),
                },
            )
            .await;
        if existing.is_err() {
            let staged = self
                .cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    self.operator().metadata(),
                    cartridge.stage_request(),
                )
                .await
                .expect("the version's payloads stage");
            self.cartridges
                .client
                .call_with(
                    RequestContext::root(),
                    self.operator().metadata(),
                    PublishCartridgeVersion {
                        envelope: cartridge.publishable(staged),
                    },
                )
                .await
                .expect("the version publishes");
        }
        let record = self
            .cartridges
            .client
            .call_with(
                RequestContext::root(),
                self.operator().metadata(),
                GetCartridge {
                    id: cartridge.id.clone(),
                },
            )
            .await
            .expect("the published cartridge");
        let heads = self.read_doc(record.doc_id.clone()).await.heads;
        version_pin(record.doc_id, &cartridge.version, heads)
    }

    /// Installs `cartridge` as `authorization`.
    async fn install_cartridge(
        &self,
        authorization: Authorization,
        cartridge: FixtureCartridge,
    ) -> Result<CartridgeInstallRecord, CartridgesError> {
        let pin = self.publish(cartridge).await;
        self.call(authorization, InstallCartridge { cartridge: pin })
            .await
    }

    /// Installs `cartridge` as `authorization` and reports its nodule as it stands.
    async fn admit(
        &self,
        authorization: Authorization,
        cartridge: FixtureCartridge,
    ) -> Result<NoduleRecord, CartridgesError> {
        let installed = self.install_cartridge(authorization, cartridge).await?;
        // The caller may not read the nodule it just installed (a handle minted with `admit` only).
        Ok(self
            .get(self.operator(), &installed.cartridge_id.to_string())
            .await
            .expect("an installed cartridge's nodule"))
    }

    /// The install row of `cartridge_id`.
    async fn install_of(&self, cartridge_id: &str) -> CartridgeInstall {
        self.call(self.operator(), ListCartridgeInstalls {})
            .await
            .unwrap()
            .installs
            .into_iter()
            .find(|record| record.cartridge_id == id(cartridge_id))
            .expect("an installed cartridge")
            .install
    }

    /// Everything the cartridge's config document holds, read as the operator.
    async fn cartridge_doc(&self, cartridge_id: &str) -> DatumsRead {
        self.read_doc(self.install_of(cartridge_id).await.config_doc)
            .await
    }

    /// Everything the document holds, read as the operator.
    async fn read_doc(&self, doc_id: DocId) -> DatumsRead {
        let capability = self
            .docs
            .client
            .call_with(
                RequestContext::root(),
                self.operator().metadata(),
                MintDocCapability {
                    scope: DocScope {
                        read: BTreeSet::from([doc_id.clone()]),
                        ..DocScope::default()
                    },
                    rights: Rights::USE,
                    subject: None,
                },
            )
            .await
            .unwrap()
            .capability_id;
        self.docs
            .client
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability,
                    doc_id,
                    at: At::Current,
                    datums: None,
                    optimistic: false,
                },
            )
            .await
            .unwrap()
    }

    async fn set_grant(
        &self,
        authorization: Authorization,
        cartridge_id: &str,
        grant: GrantKey,
        value: Option<GrantBody>,
    ) -> Result<(), CartridgesError> {
        self.call(
            authorization,
            SetCartridgeGrant {
                cartridge_id: id(cartridge_id),
                component: Component::Nodule,
                grant,
                value,
            },
        )
        .await
    }

    /// Sets the nodule's drawer grants to exactly `drawers`: the other drawer grants it holds are
    /// revoked and each of these is granted as a datum of its own.
    async fn set_drawers(
        &self,
        cartridge_id: &str,
        drawers: BTreeMap<DrawerId, DrawerAccess>,
    ) -> NoduleRecord {
        let held: Vec<GrantKey> = self
            .cartridge_doc(cartridge_id)
            .await
            .datums
            .keys()
            .filter(|key| key.tag().as_str() == big_cartridges_core::config::NoduleGrant::ID)
            .filter_map(|key| GrantKey::from_slot(key.slot_bytes()))
            .filter(|grant| matches!(grant, GrantKey::Drawer(_)))
            .collect();
        for grant in held {
            let GrantKey::Drawer(drawer) = &grant else {
                unreachable!("filtered to drawer grants");
            };
            if !drawers.contains_key(drawer) {
                self.set_grant(self.operator(), cartridge_id, grant, None)
                    .await
                    .unwrap();
            }
        }
        for (drawer, access) in drawers {
            self.set_grant(
                self.operator(),
                cartridge_id,
                GrantKey::Drawer(drawer),
                Some(GrantBody::drawer(access.into())),
            )
            .await
            .unwrap();
        }
        self.get(self.operator(), cartridge_id).await.unwrap()
    }

    /// Sets the methods the nodule may invoke to exactly `methods`: the other method and
    /// authority grants it holds are revoked and each of these is granted as a datum of its own.
    async fn set_methods(&self, cartridge_id: &str, methods: DispatchScope) -> NoduleRecord {
        let held: Vec<GrantKey> = self
            .cartridge_doc(cartridge_id)
            .await
            .datums
            .keys()
            .filter(|key| key.tag().as_str() == big_cartridges_core::config::NoduleGrant::ID)
            .filter_map(|key| GrantKey::from_slot(key.slot_bytes()))
            .filter(|grant| !matches!(grant, GrantKey::Drawer(_)))
            .collect();
        let wanted: BTreeSet<GrantKey> = methods
            .operations
            .iter()
            .cloned()
            .map(GrantKey::Method)
            .chain(methods.authorities.iter().cloned().map(GrantKey::Authority))
            .collect();
        for grant in held {
            if !wanted.contains(&grant) {
                self.set_grant(self.operator(), cartridge_id, grant, None)
                    .await
                    .unwrap();
            }
        }
        for grant in wanted {
            self.set_grant(
                self.operator(),
                cartridge_id,
                grant,
                Some(GrantBody::method(methods.privileged)),
            )
            .await
            .unwrap();
        }
        self.get(self.operator(), cartridge_id).await.unwrap()
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
            SetCartridgeEnabled {
                cartridge_id: id(example_cartridge!("photos")),
                component: Component::Nodule,
                enabled: true,
            },
        )
        .await
        .unwrap();
    assert!(enabled.install.nodule.is_some_and(|nodule| nodule.enabled));
    assert_eq!(
        node.get(operator, example_cartridge!("photos"))
            .await
            .unwrap()
            .readiness,
        Readiness::Ready
    );
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();
    assert!(matches!(
        node.get(operator, example_cartridge!("missing")).await,
        Err(NodulesError::UnknownCartridge(_))
    ));

    // An upgrade keeps the nodule's actor and its enablement.
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
        let mut rival = FixtureCartridge::new(Cartridge::new(
            big_cartridges_core::test_support::cartridge_id(name),
            "1.0.0".parse().unwrap(),
        ));
        rival.nodule = Some(Default::default());
        rival.authority = Some(authority.into());
        // Installing a rival is not holding: the prefix is refused at enablement, which is what
        // lets a dev build for a cartridge be installed beside it.
        let rival_id = rival.id.clone();
        node.admit(operator, rival).await.unwrap();
        let refused = node
            .call(
                operator,
                SetCartridgeEnabled {
                    cartridge_id: rival_id,
                    component: Component::Nodule,
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
        Err(CartridgesError::Denied)
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
        Err(CartridgesError::Denied)
    );
    assert!(
        node.call(
            photos_only,
            SetCartridgeEnabled {
                cartridge_id: id(example_cartridge!("photos")),
                component: Component::Nodule,
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
        node.set_grant(
            photos_only,
            example_cartridge!("schema"),
            GrantKey::Drawer(node.drawers.local().present_id.clone()),
            None
        )
        .await,
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        node.call(
            photos_only,
            UninstallCartridge {
                cartridge_id: id(example_cartridge!("schema")),
            }
        )
        .await,
        Err(CartridgesError::Denied)
    );
    assert_eq!(
        node.call(
            photos_only,
            ClearCartridgeConfig {
                cartridge_id: id(example_cartridge!("schema")),
            }
        )
        .await,
        Err(CartridgesError::Denied)
    );
    // The install rows a handle reads are its own cartridges'.
    let rows = node
        .call(photos_only, ListCartridgeInstalls {})
        .await
        .unwrap()
        .installs;
    assert_eq!(
        rows.iter()
            .map(|row| row.cartridge_id.clone())
            .collect::<Vec<_>>(),
        vec![id(example_cartridge!("photos"))]
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
        Err(CartridgesError::Denied)
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
        Err(CartridgesError::Denied)
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

/// A package whose nodule lists a settings NSID its Lexicons do not declare, which admission
/// refuses.
fn cartridge_with_undeclared_settings(id: &str, version: &str) -> FixtureCartridge {
    let mut cartridge = cartridge(id, version, true);
    cartridge
        .settings
        .insert("org.example.photos.missing".parse().unwrap());
    cartridge
}

#[tokio::test]
async fn an_upgrade_that_fails_admission_is_rejected_and_a_good_one_keeps_actor_and_grants() {
    let node = node().await;
    let operator = node.operator();
    let present = node.drawers.local().present_id.clone();
    grant_drawer(
        &node.drawers,
        &node.drawers_actor,
        &present,
        node.nodules_actor.actor.actor_id,
        DrawerAccess::Admin,
    )
    .await;
    let installed = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "1.0.0", true),
        )
        .await
        .unwrap();
    let v1 = node
        .set_drawers(
            example_cartridge!("photos"),
            BTreeMap::from([(present.clone(), DrawerAccess::Write)]),
        )
        .await;
    assert_eq!(v1.error, None);
    assert!(v1.drawers[&present].granted);
    assert_eq!(v1.actor, installed.actor);

    // A version admission refuses is never pinned: the install keeps running 1.0.0.
    assert!(matches!(
        node.admit(
            operator,
            cartridge_with_undeclared_settings(example_cartridge!("photos"), "2.0.0"),
        )
        .await,
        Err(CartridgesError::Rejected(reason)) if reason.contains("org.example.photos.missing")
    ));
    assert_eq!(
        node.get(operator, example_cartridge!("photos"))
            .await
            .unwrap(),
        v1
    );

    // A good upgrade keeps the durable actor and the grants, which live in the config document.
    let upgraded = node
        .admit(
            operator,
            cartridge(example_cartridge!("photos"), "2.1.0", true),
        )
        .await
        .unwrap();
    assert_eq!(upgraded.version, Some("2.1.0".parse().unwrap()));
    assert_eq!(upgraded.actor, v1.actor, "the durable actor is kept");
    assert_eq!(upgraded.error, None);
    assert!(upgraded.drawers[&present].granted);
    node.shutdown().await;
}

#[tokio::test]
async fn drawer_grants_apply_downgrade_revoke_and_uninstall_cleanly() {
    let mut node = node().await;
    let operator = node.operator();
    let present = node.drawers.local().present_id.clone();
    let placement = format!("drawer {present}");
    let note_key: DatumKey = format!("{NOTE}/main").parse().unwrap();

    // Grant on `present` before `nodules_actor` has Admin on `present`: the desired grant is
    // persisted in the cartridge's config document, but the binding is reported as blocked.
    node.admit(
        operator,
        cartridge(example_cartridge!("photos"), "1.0.0", true),
    )
    .await
    .unwrap();
    let blocked = node
        .set_drawers(
            example_cartridge!("photos"),
            BTreeMap::from([(present.clone(), DrawerAccess::Write)]),
        )
        .await;
    assert_ne!(
        blocked.applied_heads.as_ref(),
        Some(&blocked.observed_heads)
    );
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
    grant_drawer(
        &node.drawers,
        &node.drawers_actor,
        &present,
        node.nodules_actor.actor.actor_id,
        DrawerAccess::Admin,
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
        .set_drawers(
            example_cartridge!("photos"),
            BTreeMap::from([(present.clone(), DrawerAccess::Read)]),
        )
        .await;
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

    // Uninstalling removes the install datums and retracts the Read grant capability, while
    // re-installing the same cartridge reopens the same durable actor and finds the grant
    // the cartridge's config document kept.
    node.call(
        operator,
        UninstallCartridge {
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
    assert_eq!(
        readmitted.drawers.get(&present),
        Some(&NoduleGrantStatus {
            desired: DrawerAccess::Read,
            granted: true,
            error: None,
        })
    );
    node.shutdown().await;
}

#[tokio::test]
async fn invalid_imported_grants_are_refused_per_nodule_and_repairable() {
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

    // Simulate a grant datum that states a value its kind of grant does not take (an access level
    // on a method grant), written straight into the cartridge's config document as a synced edit
    // would land.
    let bad_grant = GrantKey::Method("org.example.remote.ping".to_owned());
    let bad_receipt = node
        .write_cartridge_datums(
            example_cartridge!("photos"),
            BTreeMap::from([(
                bad_grant.datum_key(Component::Nodule),
                json!({ "$type": big_cartridges_core::config::NoduleGrant::ID, "access": "Read" }),
            )]),
        )
        .await;

    let refused = loop {
        let record = node
            .get(operator, example_cartridge!("photos"))
            .await
            .unwrap();
        if bad_receipt
            .heads
            .iter()
            .all(|head| record.observed_heads.contains(head))
        {
            break record;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(refused.readiness, Readiness::Refused);
    assert!(
        refused
            .error
            .as_deref()
            .unwrap()
            .contains("org.example.remote.ping")
    );
    let healthy = node
        .get(operator, example_cartridge!("schema"))
        .await
        .unwrap();
    assert_eq!(healthy.readiness, Readiness::Disabled);
    assert_eq!(healthy.error, None);

    // Setting the grant again replaces the damaged datum and restores the nodule.
    node.set_grant(
        operator,
        example_cartridge!("photos"),
        bad_grant,
        Some(GrantBody::method(false)),
    )
    .await
    .unwrap();
    let repaired = node
        .get(operator, example_cartridge!("photos"))
        .await
        .unwrap();
    assert_eq!(repaired.readiness, Readiness::Disabled);
    assert_eq!(repaired.error, None);
    node.shutdown().await;
}

/// `cartridge(.., true)` that also contributes a micro (its component is a second artifact), so
/// the install has a member for each component, under one pin.
fn cartridge_with_micro(id: &str, version: &str) -> FixtureCartridge {
    let mut cartridge = cartridge(id, version, true);
    cartridge.with_wasm("micro", b"micro component");
    cartridge.micro = Some(MicroContribution {
        component: "micro".into(),
        schema: "org.example.clay.micro".into(),
        description: Default::default(),
        extra: Default::default(),
    });
    cartridge
}

/// The datum the photos nodule declares as its settings, under its own NSID.
fn photos_settings_key() -> DatumKey {
    DatumKey::from_text("org.example.photos.settings", "main").unwrap()
}

fn photos_settings(thumbnails: bool) -> Value {
    json!({ "$type": "org.example.photos.settings", "thumbnails": thumbnails })
}

#[tokio::test]
async fn install_creates_the_cartridge_config_document_and_reinstall_reuses_it_with_its_settings() {
    let mut node = node().await;
    let operator = node.operator();
    let photos = example_cartridge!("photos");
    node.admit(operator, cartridge(photos, "1.0.0", true))
        .await
        .unwrap();
    node.admit(
        operator,
        cartridge(example_cartridge!("schema"), "0.1.0", false),
    )
    .await
    .unwrap();

    // The install names a document in the shared config drawer that is the cartridge's own: it
    // says whose it is, and another cartridge has another.
    let installed = node.install_of(photos).await;
    let schema_doc = node
        .install_of(example_cartridge!("schema"))
        .await
        .config_doc;
    assert_ne!(installed.config_doc, schema_doc);
    let doc = node.cartridge_doc(photos).await;
    let DatumRead::Accepted { value, .. } =
        &doc.datums[&big_cartridges_core::config::config_identity_key()]
    else {
        panic!("the document names its cartridge");
    };
    assert_eq!(value["cartridge"], json!(photos));
    assert!(
        installed.nodule.is_some() && installed.micro.is_none(),
        "{installed:?}"
    );

    // Settings are ordinary datums in the document, written once the nodule's Lexicon is
    // registered (when it is enabled), and a grant is a datum of its own beside them.
    node.call(
        operator,
        SetCartridgeEnabled {
            cartridge_id: id(photos),
            component: Component::Nodule,
            enabled: true,
        },
    )
    .await
    .unwrap();
    node.write_cartridge_datums(
        photos,
        BTreeMap::from([(photos_settings_key(), photos_settings(false))]),
    )
    .await;
    let grant = GrantKey::Method("org.example.remote.ping".to_owned());
    node.set_grant(
        operator,
        photos,
        grant.clone(),
        Some(GrantBody::method(false)),
    )
    .await
    .unwrap();

    // Uninstalling keeps the document, so a reinstall (this node's, or a sister's) finds the
    // settings and the grant where they were left, in the same document.
    node.call(
        operator,
        UninstallCartridge {
            cartridge_id: id(photos),
        },
    )
    .await
    .unwrap();
    assert!(
        node.list(operator)
            .await
            .unwrap()
            .contains(&id(example_cartridge!("schema")))
    );
    let left = node.read_doc(installed.config_doc.clone()).await;
    assert!(left.datums.contains_key(&photos_settings_key()));
    assert!(
        left.datums
            .contains_key(&grant.datum_key(Component::Nodule))
    );

    node.restart_nodules().await.unwrap();
    node.admit(operator, cartridge(photos, "1.0.0", true))
        .await
        .unwrap();
    let reinstalled = node.install_of(photos).await;
    assert_eq!(
        reinstalled.config_doc, installed.config_doc,
        "the document is reused"
    );
    let doc = node.cartridge_doc(photos).await;
    let DatumRead::Accepted { value, .. } = &doc.datums[&photos_settings_key()] else {
        panic!("the settings survived the uninstall");
    };
    assert_eq!(value["thumbnails"], json!(false));
    assert!(doc.datums.contains_key(&grant.datum_key(Component::Nodule)));
    // The reinstall starts disabled, as any install does: enablement is per node and per install.
    assert!(!node.get(operator, photos).await.unwrap().enabled);
    node.shutdown().await;
}

#[tokio::test]
async fn clearing_the_config_deletes_settings_and_grants_of_both_components_and_keeps_the_install()
{
    let node = node().await;
    let operator = node.operator();
    let photos = example_cartridge!("photos");
    node.admit(operator, cartridge_with_micro(photos, "1.0.0"))
        .await
        .unwrap();
    node.call(
        operator,
        SetCartridgeEnabled {
            cartridge_id: id(photos),
            component: Component::Nodule,
            enabled: true,
        },
    )
    .await
    .unwrap();
    node.write_cartridge_datums(
        photos,
        BTreeMap::from([(photos_settings_key(), photos_settings(true))]),
    )
    .await;
    let nodule_grant = GrantKey::Method("org.example.remote.ping".to_owned());
    let micro_grant = GrantKey::Authority("org.example.remote".to_owned());
    node.set_grant(
        operator,
        photos,
        nodule_grant.clone(),
        Some(GrantBody::method(false)),
    )
    .await
    .unwrap();
    node.call(
        operator,
        SetCartridgeGrant {
            cartridge_id: id(photos),
            component: Component::Micro,
            grant: micro_grant.clone(),
            value: Some(GrantBody::method(false)),
        },
    )
    .await
    .unwrap();
    let before = node.cartridge_doc(photos).await;
    for key in [
        photos_settings_key(),
        nodule_grant.datum_key(Component::Nodule),
        micro_grant.datum_key(Component::Micro),
    ] {
        assert!(before.datums.contains_key(&key), "{key}");
    }

    node.call(
        operator,
        ClearCartridgeConfig {
            cartridge_id: id(photos),
        },
    )
    .await
    .unwrap();
    let after = node.cartridge_doc(photos).await;
    assert_eq!(
        after.datums.keys().cloned().collect::<Vec<_>>(),
        vec![big_cartridges_core::config::config_identity_key()],
        "only the identity that names the document is left"
    );
    let record = node.get(operator, photos).await.unwrap();
    assert!(record.enabled, "the install is untouched");
    assert_eq!(record.methods, None);

    // A cartridge that is not installed has a document too, and clearing it works the same.
    let schema = example_cartridge!("schema");
    node.admit(operator, cartridge(schema, "0.1.0", true))
        .await
        .unwrap();
    node.set_grant(
        operator,
        schema,
        GrantKey::Method("org.example.remote.ping".to_owned()),
        Some(GrantBody::method(false)),
    )
    .await
    .unwrap();
    let doc_id = node.install_of(schema).await.config_doc;
    node.call(
        operator,
        UninstallCartridge {
            cartridge_id: id(schema),
        },
    )
    .await
    .unwrap();
    node.call(
        operator,
        ClearCartridgeConfig {
            cartridge_id: id(schema),
        },
    )
    .await
    .unwrap();
    assert_eq!(node.read_doc(doc_id).await.datums.len(), 1);
    assert!(matches!(
        node.call(
            operator,
            ClearCartridgeConfig {
                cartridge_id: id(example_cartridge!("never")),
            },
        )
        .await,
        Err(CartridgesError::UnknownCartridge(_))
    ));
    node.shutdown().await;
}

#[tokio::test]
async fn grants_are_separate_datums_so_two_made_separately_both_apply() {
    let node = node().await;
    let operator = node.operator();
    let photos = example_cartridge!("photos");
    let present = node.drawers.local().present_id.clone();
    let originated = node.drawers.local().originated_id.clone();
    for drawer in [&present, &originated] {
        grant_drawer(
            &node.drawers,
            &node.drawers_actor,
            drawer,
            node.nodules_actor.actor.actor_id,
            DrawerAccess::Admin,
        )
        .await;
    }
    node.admit(operator, cartridge(photos, "1.0.0", true))
        .await
        .unwrap();

    // Each grant is written alone, and the second does not disturb the first: they are two
    // datums, so the document never has to be rewritten as a whole.
    node.set_grant(
        operator,
        photos,
        GrantKey::Drawer(present.clone()),
        Some(GrantBody::drawer(EntryAccess::Read)),
    )
    .await
    .unwrap();
    let one = node.cartridge_doc(photos).await;
    node.set_grant(
        operator,
        photos,
        GrantKey::Drawer(originated.clone()),
        Some(GrantBody::drawer(EntryAccess::Write)),
    )
    .await
    .unwrap();
    let both = node.cartridge_doc(photos).await;
    let key = |drawer: &DrawerId| GrantKey::Drawer(drawer.clone()).datum_key(Component::Nodule);
    assert_eq!(both.datums[&key(&present)], one.datums[&key(&present)]);
    assert!(both.datums.contains_key(&key(&originated)));

    let record = node.get(operator, photos).await.unwrap();
    assert_eq!(record.error, None);
    assert_eq!(
        record.drawers.get(&present),
        Some(&NoduleGrantStatus {
            desired: DrawerAccess::Read,
            granted: true,
            error: None,
        })
    );
    assert_eq!(
        record.drawers.get(&originated),
        Some(&NoduleGrantStatus {
            desired: DrawerAccess::Write,
            granted: true,
            error: None,
        })
    );

    // A grant states what its kind takes, and a component the cartridge does not contribute has
    // no grants.
    assert!(matches!(
        node.set_grant(
            operator,
            photos,
            GrantKey::Method("org.example.remote.ping".to_owned()),
            Some(GrantBody::drawer(EntryAccess::Read)),
        )
        .await,
        Err(CartridgesError::Rejected(_))
    ));
    assert!(matches!(
        node.call(
            operator,
            SetCartridgeGrant {
                cartridge_id: id(photos),
                component: Component::Micro,
                grant: GrantKey::Method("org.example.remote.ping".to_owned()),
                value: Some(GrantBody::method(false)),
            },
        )
        .await,
        Err(CartridgesError::Rejected(_))
    ));
    node.shutdown().await;
}

#[tokio::test]
async fn a_nodule_and_its_micro_share_one_pin_and_are_enabled_independently() {
    let node = node().await;
    let operator = node.operator();
    let photos = example_cartridge!("photos");
    node.admit(operator, cartridge_with_micro(photos, "1.0.0"))
        .await
        .unwrap();
    let install = node.install_of(photos).await;
    assert_eq!(
        install.nodule.as_ref().map(|member| member.enabled),
        Some(false)
    );
    assert_eq!(
        install.micro.as_ref().map(|member| member.enabled),
        Some(false)
    );

    // The live feed shows the rows now and after each change.
    let mut feed = node
        .nodules
        .client
        .subscribe_with_metadata(Envelope::with_metadata(
            RequestContext::root(),
            operator.metadata(),
            ObserveCartridgeInstalls {},
        ))
        .await
        .unwrap()
        .output;
    let first = feed.next().await.unwrap().unwrap();
    assert_eq!(first.installs.len(), 1);
    assert_eq!(first.installs[0].install, install);

    let micro_on = node
        .call(
            operator,
            SetCartridgeEnabled {
                cartridge_id: id(photos),
                component: Component::Micro,
                enabled: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(micro_on.install.cartridge, install.cartridge, "one pin");
    assert_eq!(
        micro_on
            .install
            .nodule
            .as_ref()
            .map(|member| member.enabled),
        Some(false)
    );
    assert_eq!(
        micro_on.install.micro.as_ref().map(|member| member.enabled),
        Some(true)
    );
    let next = feed.next().await.unwrap().unwrap();
    assert_eq!(next.installs[0].install, micro_on.install);
    assert_eq!(
        node.get(operator, photos).await.unwrap().readiness,
        Readiness::Disabled,
        "the nodule stays disabled while its micro runs"
    );

    node.call(
        operator,
        SetCartridgeEnabled {
            cartridge_id: id(photos),
            component: Component::Nodule,
            enabled: true,
        },
    )
    .await
    .unwrap();
    let next = feed.next().await.unwrap().unwrap();
    let row = &next.installs[0].install;
    assert!(row.nodule.as_ref().unwrap().enabled && row.micro.as_ref().unwrap().enabled);
    assert_eq!(row.cartridge, install.cartridge);

    // A micro-only cartridge installs and is no nodule at all.
    let mut micro_only = FixtureCartridge::new(Cartridge::new(
        example_cartridge!("panel").parse().unwrap(),
        "1.0.0".parse().unwrap(),
    ));
    let with_micro = cartridge_with_micro(photos, "1.0.0");
    micro_only.with_wasm("micro", b"micro component");
    micro_only.micro = with_micro.micro.clone();
    node.install_cartridge(operator, micro_only).await.unwrap();
    assert_eq!(
        node.install_of(example_cartridge!("panel")).await.nodule,
        None
    );
    assert_eq!(node.list(operator).await.unwrap(), vec![id(photos)]);
    node.shutdown().await;
}
