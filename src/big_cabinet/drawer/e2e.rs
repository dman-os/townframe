//! Drawer conformance, end to end through the hosted clients: an authority, a BigRepo, a DocsRepo
//! booted over [`DrawerAccess`], and the DrawerService on top, the way an embedder assembles them.

use super::*;
use crate::authority::{Authority, OpenActor};
use crate::doc::{
    At, CreateDoc, DocsConfig, DocsRepo, FacetObserved, FacetRead, FacetRefSelector, FacetSelector,
    ObserveFacets, ObserveScope, ObservedFacet, QueriedFacetRef, QueryFacetRefs, QueryFacets,
    ReadFacets, WriteFacets,
};
use crate::index::{FacetRefStore, FacetSetStore};
use crate::schema::SchemaRepo;
use big_cabinet_core::drawer::{Drawer, LayoutKind};
use big_cabinet_core::facet::{Bundle, FacetDecl, Universe};
use big_repo::{BigRepo, BigRepoStopToken, Config, StorageConfig};
use futures::StreamExt;
use secrets_rs::SecretStore;
use serde_json::{Value, json};
use service_utils_rs::RequestContext;
use sqlx_utils_rs::SqlCtx;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;

const NOTE: &str = "org.example.note";

fn note(text: &str) -> (FacetKey, Value) {
    (
        format!("{NOTE}/main").parse().unwrap(),
        json!({"$type": NOTE, "value": text}),
    )
}

fn note_linking(text: &str, target_doc: &DocId) -> (FacetKey, Value) {
    (
        format!("{NOTE}/main").parse().unwrap(),
        json!({
            "$type": NOTE,
            "value": text,
            "link": format!("db+facet:/{target_doc}/{NOTE}/main"),
        }),
    )
}

fn schemas() -> Arc<SchemaRepo> {
    let lexicon = json!({
        "lexicon": 1,
        "id": NOTE,
        "defs": {"main": {"type": "record", "key": "tid", "record": {
            "type": "object",
            "required": ["value"],
            "properties": {
                "value": {"type": "string"},
                "link": {"type": "string"}
            }
        }}}
    });
    let supplement = json!({
        "type": "object",
        "properties": {
            "value": {"type": "string"},
            "link": {"type": "string", "x-automerge": "reference"}
        }
    });
    let universe = Universe {
        facets: vec![FacetDecl {
            id: NOTE.to_owned(),
            name: "Note".to_owned(),
            schema: schemars::Schema::try_from(supplement).unwrap(),
            flags: Vec::new(),
        }],
        types: Vec::new(),
    };
    let bundle = Bundle::new(&universe, Vec::new());
    Arc::new(
        SchemaRepo::open([crate::schema::SchemaRegistrationGroup::new(
            Some(bundle),
            [crate::schema::SchemaRegistration::new(NOTE, lexicon)],
            Vec::new(),
        )])
        .unwrap(),
    )
}

fn command(name: &str) -> CommandId {
    CommandId::try_from(name.to_owned()).unwrap()
}

struct Node {
    authority: Authority,
    authority_stop: Option<StopToken>,
    docs_actor: Actor,
    docs_namespace: Namespace,
    drawers_actor: Actor,
    drawers_namespace: Namespace,
    repo: SharedBigRepo,
    repo_stop: Option<BigRepoStopToken>,
    sql: SqlCtx,
    facet_set: Arc<FacetSetStore>,
    facet_set_task: Option<tokio::task::JoinHandle<()>>,
    facet_ref: Arc<FacetRefStore>,
    facet_ref_task: Option<tokio::task::JoinHandle<()>>,
    docs: Option<DocsRepo>,
    docs_stop: Option<StopToken>,
    drawers: Option<Drawers>,
    drawers_stop: Option<StopToken>,
}

impl Node {
    async fn boot() -> Self {
        let keyring =
            keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring).await.unwrap();
        let (authority, authority_stop) =
            crate::authority::boot(&secrets, SqlCtx::memory().await.unwrap())
                .await
                .unwrap();
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
        let docs_actor = open("documents").await.unwrap();
        let drawers_actor = open("drawers").await.unwrap();
        let docs_namespace = authority
            .service
            .open_namespace(docs_actor.actor_id)
            .await
            .unwrap();
        let drawers_namespace = authority
            .service
            .open_namespace(drawers_actor.actor_id)
            .await
            .unwrap();
        let (repo, repo_stop) = BigRepo::boot(Config {
            node_identity_seed: [43; 32],
            storage: StorageConfig::Memory,
            scope_key: "drawers-e2e".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        })
        .await
        .unwrap();
        let sql = SqlCtx::memory().await.unwrap();
        let facet_set = Arc::new(FacetSetStore::new(sql.clone()).await.unwrap());
        let facet_set_task = {
            let store = (*facet_set).clone();
            let repo = Arc::clone(&repo);
            tokio::spawn(async move {
                store
                    .run(repo, NonZeroUsize::new(2).unwrap())
                    .await
                    .unwrap();
            })
        };
        let facet_ref = Arc::new(FacetRefStore::new(sql.clone()).await.unwrap());
        let facet_ref_task = {
            let store = (*facet_ref).clone();
            let repo = Arc::clone(&repo);
            let schemas = schemas();
            let facet_set = (*facet_set).clone();
            tokio::spawn(async move {
                store.run(repo, schemas, facet_set).await.unwrap();
            })
        };
        let mut node = Self {
            authority,
            authority_stop: Some(authority_stop),
            docs_actor,
            docs_namespace,
            drawers_actor,
            drawers_namespace,
            repo,
            repo_stop: Some(repo_stop),
            sql,
            facet_set,
            facet_set_task: Some(facet_set_task),
            facet_ref,
            facet_ref_task: Some(facet_ref_task),
            docs: None,
            docs_stop: None,
            drawers: None,
            drawers_stop: None,
        };
        node.start().await.unwrap();
        node
    }

    /// The embedder's assembly order: the policy first, DocsRepo over it, the drawers on top.
    async fn start(&mut self) -> Result<(), DrawersError> {
        let access = DrawerAccess::open(
            Arc::clone(&self.repo),
            self.authority.client.clone(),
            self.drawers_actor.capability,
            self.drawers_namespace,
            self.sql.clone(),
        )
        .await?;
        let (docs, docs_stop) = crate::doc::boot(DocsConfig {
            big_repo: Arc::clone(&self.repo),
            schema_repo: schemas(),
            authority: self.authority.client.clone(),
            issuer: self.docs_actor.capability,
            namespace: self.docs_namespace,
            node_issuer: self.authority.node_issuer.clone(),
            access: Arc::clone(&access) as Arc<dyn crate::doc::DocAccess>,
            sql: self.sql.clone(),
            facet_set: Some(Arc::clone(&self.facet_set)),
            facet_ref: Some(Arc::clone(&self.facet_ref)),
        })
        .await
        .unwrap();
        let (drawers, drawers_stop) = boot(DrawerConfig {
            big_repo: Arc::clone(&self.repo),
            authority: self.authority.client.clone(),
            issuer: self.drawers_actor.capability,
            namespace: self.drawers_namespace,
            docs: docs.client.clone(),
            access,
        })
        .await?;
        self.docs = Some(docs);
        self.docs_stop = Some(docs_stop);
        self.drawers = Some(drawers);
        self.drawers_stop = Some(drawers_stop);
        Ok(())
    }

    async fn stop_services(&mut self) {
        self.drawers.take();
        if let Some(stop) = self.drawers_stop.take() {
            stop.stop().await;
        }
        self.docs.take();
        if let Some(stop) = self.docs_stop.take() {
            stop.stop().await;
        }
    }

    /// Stop both services and boot them again over the same stores, as after a crash.
    async fn restart(&mut self) -> Result<(), DrawersError> {
        self.stop_services().await;
        self.docs_namespace = self
            .authority
            .service
            .open_namespace(self.docs_actor.actor_id)
            .await
            .unwrap();
        self.drawers_namespace = self
            .authority
            .service
            .open_namespace(self.drawers_actor.actor_id)
            .await
            .unwrap();
        self.start().await
    }

    fn drawers(&self) -> &Client<DrawersProtocol> {
        &self.drawers.as_ref().unwrap().client
    }

    fn docs(&self) -> &Client<DocsProtocol> {
        &self.docs.as_ref().unwrap().client
    }

    fn local(&self) -> LocalDrawers {
        self.drawers.as_ref().unwrap().local()
    }

    fn core(&self) -> &Arc<Core> {
        &self.drawers.as_ref().unwrap().core
    }

    /// A fresh actor holding nothing but its own capability.
    async fn actor(&self, path: &str) -> Actor {
        self.authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: self.authority.root,
                    path: path.into(),
                    rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                },
            )
            .await
            .unwrap()
    }

    async fn mint_drawer(&self, actor: &Actor, scope: DrawerScope) -> Result<CapId, DrawersError> {
        self.drawers()
            .call(
                RequestContext::root(),
                MintDrawerCapability {
                    authorization: Authorization::identity(actor.capability),
                    scope,
                    rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
                },
            )
            .await
            .map(|cap| cap.capability_id)
            .map_err(op)
    }

    async fn drawer_capability(
        &self,
        actor: &Actor,
        drawers: &[&DrawerId],
    ) -> Result<CapId, DrawersError> {
        let mut scope = DrawerScope::default();
        for drawer in drawers {
            let (access, _) = self
                .core()
                .effective(&RequestContext::root(), actor.actor_id, drawer)
                .await?
                .ok_or(DrawersError::Denied)?;
            scope.drawers.insert((*drawer).clone(), access);
        }
        self.mint_drawer(actor, scope).await
    }

    async fn mint_doc(&self, actor: &Actor, scope: DocScope) -> Result<CapId, DocsError> {
        self.docs()
            .call(
                RequestContext::root(),
                MintDocCapability {
                    authorization: Authorization::identity(actor.capability),
                    scope,
                    rights: Rights::USE,
                },
            )
            .await
            .map(|cap| cap.capability_id)
            .map_err(docs_op)
    }

    /// An actor allowed to create collection drawers.
    async fn creator(&self, path: &str) -> Actor {
        let actor = self.actor(path).await;
        self.core()
            .grants
            .grant(
                &RequestContext::root(),
                actor.actor_id,
                CREATE_DRAWER_OBJECT.into(),
                Rights::USE,
                None,
            )
            .await
            .unwrap();
        actor
    }

    async fn collection(&self, actor: &Actor, name: &str) -> DrawerId {
        self.drawers()
            .call(
                RequestContext::root(),
                CreateDrawer {
                    capability: self
                        .mint_drawer(
                            actor,
                            DrawerScope {
                                create: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap(),
                    command: command(name),
                    layout: CreateLayout::SingleGroup,
                },
            )
            .await
            .unwrap()
            .drawer_id
    }

    async fn create_doc(
        &self,
        actor: &Actor,
        drawer: &DrawerId,
        name: &str,
    ) -> Result<DocId, DocsError> {
        let (key, value) = note(name);
        self.docs()
            .call(
                RequestContext::root(),
                CreateDoc {
                    capability: self
                        .mint_doc(
                            actor,
                            DocScope {
                                create: [drawer_placement(drawer)].into(),
                                ..Default::default()
                            },
                        )
                        .await?,
                    command: command(name),
                    placement: drawer_placement(drawer),
                    facets: BTreeMap::from([(key, value)]),
                },
            )
            .await
            .map(|receipt| receipt.doc_id)
            .map_err(docs_op)
    }

    async fn read_note(&self, actor: &Actor, doc: &DocId) -> Result<Value, DocsError> {
        let (key, _) = note("");
        let read = self
            .docs()
            .call(
                RequestContext::root(),
                ReadFacets {
                    capability: self
                        .mint_doc(
                            actor,
                            DocScope {
                                read: [doc.clone()].into(),
                                ..Default::default()
                            },
                        )
                        .await?,
                    doc_id: doc.clone(),
                    at: At::Current,
                    facets: Some(BTreeSet::from([key.clone()])),
                    optimistic: false,
                },
            )
            .await
            .map_err(docs_op)?;
        match read.facets.get(&key).unwrap() {
            FacetRead::Accepted { value, .. } => Ok(value.clone()),
            other => panic!("expected an accepted note, got {other:?}"),
        }
    }

    async fn write_note(&self, actor: &Actor, doc: &DocId, text: &str) -> Result<(), DocsError> {
        let (key, value) = note(text);
        self.docs()
            .call(
                RequestContext::root(),
                WriteFacets {
                    capability: self
                        .mint_doc(
                            actor,
                            DocScope {
                                write: [doc.clone()].into(),
                                ..Default::default()
                            },
                        )
                        .await?,
                    command: None,
                    doc_id: doc.clone(),
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::from([(key, value)]),
                    remove: BTreeSet::new(),
                },
            )
            .await
            .map(|_| ())
            .map_err(docs_op)
    }

    /// The roster of `drawer` as its doc states it, read as the service: the authoritative view
    /// the projection must agree with.
    async fn roster_facets(&self, drawer: &DrawerId) -> BTreeMap<FacetKey, Value> {
        let read = self
            .docs()
            .call(
                RequestContext::root(),
                ReadFacets {
                    capability: self
                        .mint_doc(
                            &self.drawers_actor,
                            DocScope {
                                read: [drawer.clone()].into(),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap(),
                    doc_id: drawer.clone(),
                    at: At::Current,
                    facets: None,
                    optimistic: false,
                },
            )
            .await
            .unwrap();
        read.facets
            .into_iter()
            .filter(|(key, _)| key.tag().as_str().starts_with(DRAWER_FACET_TAG))
            .map(|(key, read)| match read {
                FacetRead::Accepted { value, .. } => (key, value),
                other => panic!("drawer facet {key} is not accepted: {other:?}"),
            })
            .collect()
    }

    async fn view(&self, actor: &Actor, drawer: &DrawerId) -> Result<DrawerView, DrawersError> {
        self.drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: self.drawer_capability(actor, &[drawer]).await?,
                    drawer_id: drawer.clone(),
                },
            )
            .await
            .map_err(op)
    }

    async fn add_doc(
        &self,
        actor: &Actor,
        name: &str,
        drawer: &DrawerId,
        doc: &DocId,
        access: Access,
    ) -> Result<WriteReceipt, DrawersError> {
        let mut roots = self
            .core()
            .state
            .ancestors(state::ScopeTarget::Doc(doc))
            .await?;
        roots.insert(drawer.clone(), Access::Admin);
        let mut scope = DrawerScope::default();
        for root in roots.keys() {
            if let Some((access, _)) = self
                .core()
                .effective(&RequestContext::root(), actor.actor_id, root)
                .await?
            {
                scope.drawers.insert(root.clone(), access);
            }
        }
        self.drawers()
            .call(
                RequestContext::root(),
                AddDoc {
                    capability: self.mint_drawer(actor, scope).await?,
                    command: command(name),
                    drawer_id: drawer.clone(),
                    doc_id: doc.clone(),
                    access,
                },
            )
            .await
            .map_err(op)
    }

    async fn remove_doc(
        &self,
        actor: &Actor,
        name: &str,
        drawer: &DrawerId,
        doc: &DocId,
    ) -> Result<WriteReceipt, DrawersError> {
        self.drawers()
            .call(
                RequestContext::root(),
                RemoveDoc {
                    capability: self.drawer_capability(actor, &[drawer]).await?,
                    command: command(name),
                    drawer_id: drawer.clone(),
                    doc_id: doc.clone(),
                },
            )
            .await
            .map_err(op)
    }

    async fn grant(&self, actor: &Actor, drawer: &DrawerId, subject: &Actor, access: Access) {
        self.drawers()
            .call(
                RequestContext::root(),
                GrantDrawer {
                    capability: self.drawer_capability(actor, &[drawer]).await.unwrap(),
                    drawer_id: drawer.clone(),
                    subject_id: subject.actor_id,
                    access,
                },
            )
            .await
            .unwrap();
    }

    async fn list_docs(
        &self,
        actor: &Actor,
        drawer: &DrawerId,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Vec<ListedDoc>, DrawersError> {
        self.drawers()
            .call(
                RequestContext::root(),
                ListDocs {
                    capability: self.drawer_capability(actor, &[drawer]).await?,
                    drawer_id: drawer.clone(),
                    offset,
                    limit,
                },
            )
            .await
            .map_err(op)
    }

    async fn remember(&self, actor: &Actor, drawer: &DrawerId, mirror: bool) {
        self.drawers()
            .call(
                RequestContext::root(),
                RememberDrawer {
                    capability: self
                        .drawer_capability(actor, &[&self.local().registry_id])
                        .await
                        .unwrap(),
                    drawer_id: drawer.clone(),
                    mirror,
                },
            )
            .await
            .unwrap();
    }

    async fn forget(&self, actor: &Actor, drawer: &DrawerId) {
        self.drawers()
            .call(
                RequestContext::root(),
                ForgetDrawer {
                    capability: self
                        .drawer_capability(actor, &[&self.local().registry_id])
                        .await
                        .unwrap(),
                    drawer_id: drawer.clone(),
                },
            )
            .await
            .unwrap();
    }

    async fn known_map(&self, actor: &Actor) -> std::collections::BTreeMap<DrawerId, bool> {
        self.known(actor)
            .await
            .into_iter()
            .map(|known| (known.drawer_id, known.mirror))
            .collect()
    }

    async fn known(&self, actor: &Actor) -> Vec<KnownDrawer> {
        self.drawers()
            .call(
                RequestContext::root(),
                ListKnownDrawers {
                    capability: self
                        .drawer_capability(actor, &[&self.local().registry_id])
                        .await
                        .unwrap(),
                },
            )
            .await
            .unwrap()
    }

    /// The Keyhive level `group` holds on `membered` (a doc or another group).
    /// The Keyhive level the group named by `member` holds on the group named by `group`:
    /// `agent_access_on(member, group)` — the member is the agent, the group is what it holds.
    async fn group_membership_level(
        &self,
        group: GroupKey,
        member: GroupKey,
    ) -> Option<KeyhiveAccess> {
        use big_repo::keyhive_core::principal::identifier::Identifier;
        let identifier = |bytes: [u8; 32]| {
            Identifier::from(ed25519_dalek::VerifyingKey::from_bytes(&bytes).unwrap())
        };
        self.repo
            .keyhive()
            .agent_access_on(&identifier(member.0), identifier(group.0))
            .await
    }

    /// The Keyhive level `group` holds on `membered` (a doc or another group).
    async fn keyhive_level(&self, group: GroupKey, membered: [u8; 32]) -> Option<KeyhiveAccess> {
        use big_repo::keyhive_core::principal::identifier::Identifier;
        let identifier = |bytes: [u8; 32]| {
            Identifier::from(ed25519_dalek::VerifyingKey::from_bytes(&bytes).unwrap())
        };
        self.repo
            .keyhive()
            .agent_access_on(&identifier(group.0), identifier(membered))
            .await
    }

    async fn stop(mut self) {
        self.stop_services().await;
        if let Some(task) = self.facet_ref_task.take() {
            task.abort();
            drop(task.await);
        }
        if let Some(task) = self.facet_set_task.take() {
            task.abort();
            drop(task.await);
        }
        if let Some(stop) = self.repo_stop.take() {
            stop.stop().await.unwrap();
        }
        self.authority_stop.take().unwrap().stop().await;
    }
}

fn op<T>(error: ClientError<T>) -> T {
    match error {
        ClientError::Operation(error) => error,
        ClientError::Transport { message, .. } => panic!("transport: {message}"),
    }
}

fn docs_op(error: ClientError<DocsError>) -> DocsError {
    op(error)
}

fn groups_of(view: &DrawerView) -> (GroupKey, GroupKey) {
    assert_eq!(
        view.layout.known(),
        Some(LayoutKind::SingleGroup),
        "only a grouped layout has groups"
    );
    let groups = view
        .descriptor
        .groups()
        .expect("a single-group descriptor names its groups");
    (groups.meta_group, groups.docs_group)
}

#[tokio::test]
async fn boot_creates_the_local_drawers_once() {
    let mut node = Node::boot().await;
    let local = node.local();
    assert_ne!(local.present_id, local.originated_id);
    // The service itself administers them: it created them.
    let view = node
        .view(&node.drawers_actor, &local.present_id)
        .await
        .unwrap();
    assert_eq!(view.access, Access::Admin);
    assert_eq!(view.layout.known(), Some(LayoutKind::SingleGroup));
    assert!(view.docs.is_empty(), "{view:?}");
    let roster = node.roster_facets(&local.present_id).await;
    assert!(roster.contains_key(&drawer_key()), "{roster:?}");
    assert!(roster.contains_key(&roster_key()), "{roster:?}");

    node.restart().await.unwrap();
    assert_eq!(node.local(), local, "a second boot finds the same drawers");
    node.stop().await;
}

#[tokio::test]
async fn drawer_shutdown_retires_shared_grants_and_downstream_document_caps() {
    use crate::authority::InspectCapability;

    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let drawer = node.collection(&alice, "workspace").await;
    let doc = node.create_doc(&alice, &drawer, "note").await.unwrap();
    node.grant(&alice, &drawer, &bob, Access::Read).await;

    let old_core = Arc::clone(node.core());
    let old_drawer_namespace = node.drawers_namespace;
    let old_docs_namespace = node.docs_namespace;
    for namespace in [old_drawer_namespace, old_docs_namespace] {
        assert_eq!(
            node.authority
                .service
                .open_namespace(namespace.namespace_id)
                .await
                .unwrap(),
            namespace
        );
    }
    let doc_scope = DocScope {
        read: [doc.clone()].into(),
        ..Default::default()
    };
    let mut drawer_caps = Vec::new();
    let mut doc_caps = Vec::new();
    let mut retired = vec![old_drawer_namespace.capability];
    for actor in [&alice, &bob] {
        drawer_caps.push(node.drawer_capability(actor, &[&drawer]).await.unwrap());
        doc_caps.push(node.mint_doc(actor, doc_scope.clone()).await.unwrap());
        let (_, dependencies) = old_core
            .effective(&RequestContext::root(), actor.actor_id, &drawer)
            .await
            .unwrap()
            .unwrap();
        assert!(!dependencies.is_empty());
        retired.extend(dependencies);
    }
    doc_caps.push(
        node.mint_doc(&node.drawers_actor, doc_scope.clone())
            .await
            .unwrap(),
    );
    retired.extend(&drawer_caps);
    retired.extend(&doc_caps);
    for capability in &retired {
        node.authority
            .client
            .call(
                RequestContext::root(),
                InspectCapability {
                    capability: *capability,
                },
            )
            .await
            .unwrap();
    }
    let read = |capability| ReadFacets {
        capability,
        doc_id: doc.clone(),
        at: At::Current,
        facets: None,
        optimistic: false,
    };
    for capability in &doc_caps {
        node.docs()
            .call(RequestContext::root(), read(*capability))
            .await
            .unwrap();
    }
    let mut events = node
        .docs()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability: doc_caps[1],
                scope: ObserveScope::Tag {
                    tag: big_cabinet_core::FacetTag::new(NOTE).unwrap(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    let mut present = false;
    let mut replay_complete = false;
    while !present || !replay_complete {
        match events.next().await.unwrap().unwrap() {
            FacetObserved::Entry {
                doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } => {
                assert_eq!(doc_id, doc);
                present = true;
            }
            FacetObserved::ReplayComplete { .. } => replay_complete = true,
            other => panic!("expected the note or replay completion, got {other:?}"),
        }
    }

    node.drawers_stop.take().unwrap().stop().await;
    node.authority
        .client
        .call(
            RequestContext::root(),
            InspectCapability {
                capability: old_docs_namespace.capability,
            },
        )
        .await
        .unwrap();
    for capability in &retired {
        assert_eq!(
            node.authority
                .client
                .call(
                    RequestContext::root(),
                    InspectCapability {
                        capability: *capability,
                    }
                )
                .await
                .unwrap_err(),
            ClientError::Operation(AuthorityServiceError::InvalidCapability)
        );
    }
    for capability in &doc_caps {
        assert_eq!(
            node.docs()
                .call(RequestContext::root(), read(*capability))
                .await
                .unwrap_err(),
            ClientError::Operation(DocsError::Denied)
        );
    }
    assert_eq!(
        events.next().await.unwrap(),
        Err(ClientError::Operation(DocsError::Denied))
    );
    assert!(events.next().await.is_none());
    drop(events);
    for actor in [&alice, &bob, &node.drawers_actor] {
        assert_eq!(
            node.mint_doc(actor, doc_scope.clone()).await.unwrap_err(),
            DocsError::Denied
        );
    }

    node.restart().await.unwrap();
    for (old, current) in [
        (old_drawer_namespace, node.drawers_namespace),
        (old_docs_namespace, node.docs_namespace),
    ] {
        assert_eq!(old.namespace_id, current.namespace_id);
        assert_ne!(old.capability, current.capability);
        assert_eq!(
            node.authority
                .service
                .open_namespace(current.namespace_id)
                .await
                .unwrap(),
            current
        );
    }
    assert!(!Arc::ptr_eq(&old_core, node.core()));
    assert!(!Arc::ptr_eq(&old_core.grants, &node.core().grants));
    assert!(matches!(
        old_core
            .grants
            .grant(
                &RequestContext::root(),
                bob.actor_id,
                drawer_object(Access::Admin, &drawer),
                Rights::USE,
                None,
            )
            .await,
        Err(GrantError::Authority(ClientError::Operation(
            AuthorityServiceError::InvalidCapability
        )))
    ));
    assert_eq!(
        node.view(&alice, &drawer).await.unwrap().access,
        Access::Admin
    );
    assert_eq!(node.read_note(&alice, &doc).await.unwrap(), note("note").1);
    assert_eq!(
        node.view(&bob, &drawer).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );
    node.grant(&alice, &drawer, &bob, Access::Read).await;
    assert_eq!(node.read_note(&bob, &doc).await.unwrap(), note("note").1);
    retired.push(old_docs_namespace.capability);
    for capability in retired {
        assert_eq!(
            node.authority
                .client
                .call(RequestContext::root(), InspectCapability { capability })
                .await
                .unwrap_err(),
            ClientError::Operation(AuthorityServiceError::InvalidCapability)
        );
    }
    for capability in drawer_caps {
        assert_eq!(
            node.drawers()
                .call(
                    RequestContext::root(),
                    GetDrawer {
                        capability,
                        drawer_id: drawer.clone(),
                    }
                )
                .await
                .unwrap_err(),
            ClientError::Operation(DrawersError::Denied)
        );
    }
    for capability in doc_caps {
        assert_eq!(
            node.docs()
                .call(RequestContext::root(), read(capability))
                .await
                .unwrap_err(),
            ClientError::Operation(DocsError::Denied)
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn a_doc_created_in_a_drawer_belongs_to_it_and_to_the_local_drawers() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let drawer = node.collection(&alice, "work").await;

    // Creating needs Write on the placement drawer.
    assert_eq!(
        node.create_doc(&bob, &drawer, "bobs").await.unwrap_err(),
        DocsError::Denied
    );
    let doc = node.create_doc(&alice, &drawer, "plan").await.unwrap();

    let view = node.view(&alice, &drawer).await.unwrap();
    assert_eq!(
        view.access,
        Access::Admin,
        "the creator administers the drawer"
    );
    assert_eq!(view.docs, BTreeMap::from([(doc.clone(), Access::Admin)]));
    let local = node.local();
    for drawer in local.both() {
        let view = node.view(&node.drawers_actor, drawer).await.unwrap();
        assert_eq!(
            view.docs.get(&doc),
            Some(&Access::Admin),
            "{drawer}: {view:?}"
        );
    }
    // The roster doc agrees with the projection, and the docs group holds the grant.
    let roster = node.roster_facets(&drawer).await;
    assert_eq!(roster[&doc_entry_key(&doc)], json!({"access": "Admin"}));
    let (_, docs_group) = groups_of(&view);
    assert_eq!(
        node.keyhive_level(docs_group, doc.to_bytes32().unwrap())
            .await,
        Some(KeyhiveAccess::Admin)
    );

    // Containment: access to the doc is access through the drawer.
    assert_eq!(
        node.read_note(&alice, &doc).await.unwrap(),
        json!({"$type": NOTE, "value": "plan"})
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.view(&bob, &drawer).await.unwrap_err(),
        DrawersError::Denied
    );

    node.grant(&alice, &drawer, &bob, Access::Read).await;
    assert!(node.read_note(&bob, &doc).await.is_ok());
    assert_eq!(
        node.write_note(&bob, &doc, "edited").await.unwrap_err(),
        DocsError::Denied,
        "Read on the drawer is not Write on its docs"
    );
    assert_eq!(node.view(&bob, &drawer).await.unwrap().access, Access::Read);

    node.grant(&alice, &drawer, &bob, Access::Write).await;
    node.write_note(&bob, &doc, "edited").await.unwrap();

    node.drawers()
        .call(
            RequestContext::root(),
            RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );

    // Only an admin hands out grants.
    let carol = node.actor("carol").await;
    node.grant(&alice, &drawer, &carol, Access::Write).await;
    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: node.drawer_capability(&carol, &[&drawer]).await.unwrap(),
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
                access: Access::Read,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::Denied);
    node.stop().await;
}

#[tokio::test]
async fn an_entry_is_added_grant_first_and_removed_entry_first() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let doc = node.create_doc(&alice, &home, "recipe").await.unwrap();
    let shared_docs = groups_of(&node.view(&alice, &shared).await.unwrap()).1;
    let doc_bytes = doc.to_bytes32().unwrap();

    let receipt = node
        .add_doc(&alice, "share recipe", &shared, &doc, Access::Read)
        .await
        .unwrap();
    assert_eq!(receipt.doc_id, shared);
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&doc)],
        json!({"access": "Read"})
    );
    assert_eq!(
        node.keyhive_level(shared_docs, doc_bytes).await,
        Some(KeyhiveAccess::Read)
    );
    assert_eq!(
        node.view(&alice, &shared).await.unwrap().docs,
        BTreeMap::from([(doc.clone(), Access::Read)])
    );
    // The same command replays its receipt; a different request under it is refused.
    assert_eq!(
        node.add_doc(&alice, "share recipe", &shared, &doc, Access::Read)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        node.add_doc(&alice, "share recipe", &shared, &doc, Access::Write)
            .await
            .unwrap_err(),
        DrawersError::CommandReused {
            command: command("share recipe")
        }
    );
    // The doc stays in its first drawer: an entry is per roster.
    assert!(
        node.view(&alice, &home)
            .await
            .unwrap()
            .docs
            .contains_key(&doc)
    );

    // Widen, then narrow: both end with the roster and the grant agreeing.
    node.add_doc(&alice, "widen", &shared, &doc, Access::Write)
        .await
        .unwrap();
    assert_eq!(
        node.keyhive_level(shared_docs, doc_bytes).await,
        Some(KeyhiveAccess::Edit)
    );
    node.add_doc(&alice, "narrow", &shared, &doc, Access::Relay)
        .await
        .unwrap();
    assert_eq!(
        node.keyhive_level(shared_docs, doc_bytes).await,
        Some(KeyhiveAccess::Relay)
    );
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&doc)],
        json!({"access": "Relay"})
    );

    // Only Write on the target drawer may add; Admin entries need Admin; and conferring a
    // level on the doc takes holding that level on the doc through some path of one's own.
    let bob = node.actor("bob").await;
    node.grant(&alice, &shared, &bob, Access::Write).await;
    assert_eq!(
        node.add_doc(&bob, "bob admin", &shared, &doc, Access::Admin)
            .await
            .unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.add_doc(&bob, "bob read", &shared, &doc, Access::Read)
            .await
            .unwrap_err(),
        DrawersError::Denied,
        "bob holds Write on the drawer but nothing on the doc yet"
    );
    node.grant(&alice, &home, &bob, Access::Read).await;
    node.add_doc(&bob, "bob read", &shared, &doc, Access::Read)
        .await
        .unwrap();

    node.remove_doc(&alice, "unshare", &shared, &doc)
        .await
        .unwrap();
    assert!(
        !node
            .roster_facets(&shared)
            .await
            .contains_key(&doc_entry_key(&doc))
    );
    assert_eq!(node.keyhive_level(shared_docs, doc_bytes).await, None);
    assert!(node.view(&alice, &shared).await.unwrap().docs.is_empty());
    assert_eq!(
        node.remove_doc(&alice, "unshare again", &shared, &doc)
            .await
            .unwrap_err(),
        DrawersError::NotAnEntry(doc.clone())
    );
    // Removal from one drawer is not deletion: the doc is still readable through home.
    assert!(node.read_note(&alice, &doc).await.is_ok());
    node.stop().await;
}

#[tokio::test]
async fn a_child_drawer_confers_access_through_its_parent() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let parent = node.collection(&alice, "team").await;
    let child = node.collection(&alice, "project").await;
    let doc = node.create_doc(&alice, &child, "spec").await.unwrap();
    node.grant(&alice, &parent, &bob, Access::Write).await;
    let parent_cap = node
        .mint_drawer(
            &bob,
            DrawerScope {
                drawers: [(parent.clone(), Access::Write)].into(),
                create: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: parent_cap,
                    drawer_id: child.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );

    let (parent_meta, parent_docs) = groups_of(&node.view(&alice, &parent).await.unwrap());
    let (child_meta, child_docs) = groups_of(&node.view(&alice, &child).await.unwrap());

    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("self"),
                parent_id: parent.clone(),
                child_id: parent.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::SelfChild);

    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child])
                    .await
                    .unwrap(),
                command: command("adopt"),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.roster_facets(&parent).await[&child_entry_key(&child)],
        json!({"access": "Read"})
    );
    assert_eq!(
        node.view(&alice, &parent).await.unwrap().children,
        BTreeMap::from([(child.clone(), Access::Read)])
    );
    assert_eq!(
        node.keyhive_level(parent_docs, child_docs.0).await,
        Some(KeyhiveAccess::Read)
    );
    assert_eq!(
        node.keyhive_level(parent_meta, child_meta.0).await,
        Some(KeyhiveAccess::Read)
    );
    // Bob's Write on the parent is Read on the child: the entry is the weakest link.
    assert!(node.read_note(&bob, &doc).await.is_ok());
    assert_eq!(
        node.write_note(&bob, &doc, "edit").await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(node.view(&bob, &child).await.unwrap().access, Access::Read);
    assert_eq!(
        node.drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: parent_cap,
                    drawer_id: child.clone(),
                }
            )
            .await
            .unwrap()
            .access,
        Access::Read
    );

    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&parent]).await.unwrap(),
                command: command("disown"),
                parent_id: parent.clone(),
                child_id: child.clone(),
            },
        )
        .await
        .unwrap();
    assert!(
        !node
            .roster_facets(&parent)
            .await
            .contains_key(&child_entry_key(&child))
    );
    assert_eq!(node.keyhive_level(parent_docs, child_docs.0).await, None);
    assert_eq!(node.keyhive_level(parent_meta, child_meta.0).await, None);
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: parent_cap,
                    drawer_id: child.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );
    node.stop().await;
}

#[tokio::test]
async fn a_self_doc_drawer_is_the_doc_itself() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let doc = node.create_doc(&alice, &home, "letter").await.unwrap();

    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node
                    .mint_drawer(&bob, DrawerScope::default())
                    .await
                    .unwrap(),
                command: command("bob declares"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::Denied);

    let created = node
        .drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(created.drawer_id, doc);
    assert_eq!(
        node.roster_facets(&doc).await[&drawer_key()],
        serde_json::to_value(Drawer::self_doc()).unwrap()
    );
    let err = node
        .drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare twice"),
                layout: CreateLayout::SelfDoc {
                    doc_id: doc.clone(),
                },
            },
        )
        .await
        .unwrap_err();
    assert_eq!(op(err), DrawersError::AlreadyDrawer(doc.clone()));

    // Sharing the self-doc drawer is sharing the doc.
    node.grant(&alice, &doc, &bob, Access::Read).await;
    assert!(node.read_note(&bob, &doc).await.is_ok());
    let view = node.view(&bob, &doc).await.unwrap();
    assert_eq!(view.layout.known(), Some(LayoutKind::SelfDoc));
    assert_eq!(view.descriptor.groups(), None);
    assert!(view.docs.is_empty());

    // It holds only itself.
    let other = node.create_doc(&alice, &home, "other").await.unwrap();
    assert_eq!(
        node.add_doc(&alice, "grow", &doc, &other, Access::Read)
            .await
            .unwrap_err(),
        DrawersError::SelfDoc(doc.clone())
    );

    let listed = node
        .drawers()
        .call(
            RequestContext::root(),
            ListDrawers {
                capability: node.drawer_capability(&bob, &[&doc]).await.unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|listed| (listed.drawer_id.clone(), listed.access))
            .collect::<Vec<_>>(),
        vec![(doc.clone(), Access::Read)]
    );
    node.stop().await;
}

#[tokio::test]
async fn an_add_interrupted_after_its_grant_is_finished_at_boot() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let doc = node.create_doc(&alice, &home, "draft").await.unwrap();
    let shared_docs = groups_of(&node.view(&alice, &shared).await.unwrap()).1;

    // The crash: the row is begun and the grant landed, the entry was never published.
    let cmd = command("interrupted");
    node.core()
        .ledger
        .plant(
            &cmd,
            crate::doc::pipeline::digest(&("addDoc", alice.actor_id, &shared, &doc, Access::Write)),
            alice.actor_id,
            &ledger::Op::AddDoc {
                drawer_id: shared.clone(),
                doc_id: doc.clone(),
                access: Access::Write,
            },
        )
        .await
        .unwrap();
    let group = node.repo.get_group_by_id(shared_docs.0).await.unwrap();
    node.repo
        .grant_doc_access(doc.clone(), group, KeyhiveAccess::Edit)
        .await
        .unwrap();
    assert!(
        !node
            .roster_facets(&shared)
            .await
            .contains_key(&doc_entry_key(&doc))
    );

    // A retry of the same command resumes the row rather than being refused, and finishes it.
    let receipt = node
        .add_doc(&alice, "interrupted", &shared, &doc, Access::Write)
        .await
        .unwrap();
    assert_eq!(receipt.doc_id, shared);
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&doc)],
        json!({"access": "Write"})
    );
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    assert_eq!(
        node.add_doc(&alice, "interrupted", &shared, &doc, Access::Write)
            .await
            .unwrap(),
        receipt,
        "and from then on the command answers with its receipt"
    );

    // The same crash with nobody retrying: boot finishes it.
    let other = node.create_doc(&alice, &home, "other").await.unwrap();
    let cmd = command("interrupted at boot");
    node.core()
        .ledger
        .plant(
            &cmd,
            crate::doc::pipeline::digest(&(
                "addDoc",
                alice.actor_id,
                &shared,
                &other,
                Access::Read,
            )),
            alice.actor_id,
            &ledger::Op::AddDoc {
                drawer_id: shared.clone(),
                doc_id: other.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    node.restart().await.unwrap();
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&other)],
        json!({"access": "Read"})
    );
    assert_eq!(
        node.keyhive_level(shared_docs, other.to_bytes32().unwrap())
            .await,
        Some(KeyhiveAccess::Read)
    );
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    node.stop().await;
}

#[tokio::test]
async fn list_docs_is_one_check_then_the_scoped_roster() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let sub = node.collection(&alice, "sub").await;
    let first = node.create_doc(&alice, &home, "first").await.unwrap();
    let second = node.create_doc(&alice, &home, "second").await.unwrap();
    let inner = node.create_doc(&alice, &sub, "inner").await.unwrap();

    // The drawer is read by nobody else yet: bob cannot list it at all, even though a
    // doc in it might be shared with him later through another path.
    assert_eq!(
        node.list_docs(&bob, &home, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );

    // A relay enumerates: relay entry-capped docs appear as Relay, and relay docs are
    // filtered out — Relay exists to enumerate, not to read content through.
    node.grant(&alice, &home, &bob, Access::Relay).await;
    let listed = node.list_docs(&bob, &home, 0, None).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|listed| (listed.doc_id.clone(), listed.access))
            .collect::<std::collections::BTreeMap<_, _>>(),
        [
            (first.clone(), Access::Relay),
            (second.clone(), Access::Relay),
        ]
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>(),
        "bob holds Relay on the drawer, so entries cap at Relay"
    );

    // A child drawer's docs list through the parent at the weaker of the link and the
    // child's own entry; `via` names the drawer that lists it.
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&home, &sub])
                    .await
                    .unwrap(),
                command: command("adopt"),
                parent_id: home.clone(),
                child_id: sub.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    let listed = node.list_docs(&bob, &home, 0, None).await.unwrap();
    let inner_listing = listed
        .iter()
        .find(|listed| listed.doc_id == inner)
        .expect("bob enumerates the child doc through the parent's link");
    assert_eq!(
        inner_listing.access,
        Access::Relay,
        "bob's Relay on the parent confers only enumeration on the child"
    );
    let listed = node.list_docs(&alice, &home, 0, None).await.unwrap();
    let inner_listing = listed
        .iter()
        .find(|listed| listed.doc_id == inner)
        .expect("alice's Write grant reads the child link");
    assert_eq!(inner_listing.access, Access::Read);
    assert_eq!(inner_listing.via, Some(sub.clone()));

    // Paging skips and caps the ordered result.
    let page = node.list_docs(&alice, &home, 1, Some(1)).await.unwrap();
    assert_eq!(page.len(), 1);
    let all = node.list_docs(&alice, &home, 0, None).await.unwrap();
    assert_eq!(page[0].doc_id, all[1].doc_id, "pages slice the same order");

    // Revoking the drawer grant ends the listing, whatever the projection holds.
    node.drawers()
        .call(
            RequestContext::root(),
            crate::drawer::RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                drawer_id: home.clone(),
                subject_id: bob.actor_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.list_docs(&bob, &home, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );
    node.stop().await;
}

#[tokio::test]
async fn the_known_drawers_registry_is_explicit_and_survives_restart() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let registry = node.local().registry_id;

    // The registry is a self-doc drawer, readable only through a grant — here the service
    // actor's, as the embedder's own handle — and remembering grants nobody anything.
    let registry_descriptor = node.roster_facets(&registry).await[&drawer_key()].clone();
    let registry_layout = serde_json::from_value::<Drawer>(registry_descriptor).unwrap();
    assert_eq!(registry_layout.layout.known(), Some(LayoutKind::SelfDoc));
    assert_eq!(
        node.view(&alice, &registry).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.list_docs(&bob, &registry, 0, None).await.unwrap_err(),
        DrawersError::Denied
    );
    node.core()
        .grants
        .grant(
            &RequestContext::root(),
            alice.actor_id,
            drawer_object(Access::Admin, &registry),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    assert!(node.known(&alice).await.is_empty());

    node.remember(&alice, &home, true).await;
    node.remember(&alice, &shared, false).await;
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(home.clone(), true), (shared.clone(), false)])
    );
    assert_eq!(
        node.roster_facets(&registry).await[&known_entry_key(&home)],
        json!({"mirror": true})
    );

    // A policy change restates the same entry; forget removes it and nothing else —
    // the drawer, its access, and its bytes are untouched.
    node.drawers()
        .call(
            RequestContext::root(),
            SetDrawerPolicy {
                capability: node.drawer_capability(&alice, &[&registry]).await.unwrap(),
                drawer_id: shared.clone(),
                mirror: true,
            },
        )
        .await
        .unwrap();
    assert!(node.known_map(&alice).await[&shared]);
    node.forget(&alice, &home).await;
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(shared.clone(), true)])
    );
    assert!(
        node.view(&alice, &home).await.is_ok(),
        "the drawer itself is unaffected"
    );

    // The list lives on the registry doc: a restart reads it back from the doc, not from
    // a separate database row.
    node.restart().await.unwrap();
    assert_eq!(
        node.known_map(&alice).await,
        std::collections::BTreeMap::from([(shared.clone(), true)])
    );

    // Observing a drawer never writes the registry: creating and listing drawers does
    // not grow the known list.
    let _other = node.collection(&alice, "other").await;
    assert_eq!(node.known(&alice).await.len(), 1);
    node.stop().await;
}

#[tokio::test]
async fn an_invite_interrupted_between_its_memberships_is_finished_at_boot() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let home = node.collection(&alice, "home").await;
    // The invitee is a group: a peer principal would need its contact card registered with
    // the node's Keyhive first, and the membership machinery is the same either way.
    let invitee = node
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    let invitee_key = GroupKey(invitee.id().to_bytes());
    let (meta, docs) = groups_of(&node.view(&alice, &home).await.unwrap());

    // The crash: the docs membership landed, the meta membership never did, and the row
    // was never finished.
    node.repo
        .add_member_to_group(
            invitee.clone(),
            &node
                .repo
                .get_group_by_id(docs.0)
                .await
                .expect("the docs group is in keyhive"),
            KeyhiveAccess::Read,
        )
        .await
        .unwrap();
    let command = command("invite interrupted");
    node.core()
        .ledger
        .plant(
            &command,
            crate::doc::pipeline::digest(&(
                "invitePrincipal",
                alice.actor_id,
                &home,
                &crate::drawer::Principal::Group {
                    group_id: invitee_key,
                },
                Access::Read,
            )),
            alice.actor_id,
            &ledger::Op::Invite {
                drawer_id: home.clone(),
                principal: crate::drawer::Principal::Group {
                    group_id: invitee_key,
                },
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.keyhive_level(meta, invitee_key.0).await,
        None,
        "the meta membership never landed (docs level: {:?})",
        node.keyhive_level(docs, invitee_key.0).await
    );

    node.restart().await.unwrap();
    let rows = node.core().ledger.unfinished().await.unwrap();
    assert_eq!(
        node.group_membership_level(meta, invitee_key).await,
        Some(KeyhiveAccess::Read),
        "boot replay adds the missing membership (unfinished rows after boot: {rows:?})"
    );
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    node.stop().await;
}

#[tokio::test]
async fn an_entry_with_an_unknown_access_level_is_carried_without_granting_or_breaking_the_roster()
{
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let home = node.collection(&alice, "home").await;
    let shared = node.collection(&alice, "shared").await;
    let child = node.collection(&alice, "child").await;
    let known_doc = node.create_doc(&alice, &home, "known").await.unwrap();
    let foreign_doc = node.create_doc(&alice, &home, "foreign").await.unwrap();
    node.grant(&alice, &shared, &bob, Access::Read).await;
    node.drawers()
        .call(
            RequestContext::root(),
            AddDoc {
                capability: node
                    .drawer_capability(&alice, &[&home, &shared])
                    .await
                    .unwrap(),
                command: command("enter known"),
                drawer_id: shared.clone(),
                doc_id: known_doc.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    // Write a docEntry and a childEntry carrying a future access string directly onto the drawer
    // doc: the schema admits them (`knownValues`), rescan skips them for the projection, and they
    // remain on the doc untouched without granting Bob access to `foreign_doc` or `child`.
    node.docs()
        .call(
            RequestContext::root(),
            WriteFacets {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: [shared.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: None,
                doc_id: shared.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([
                    (
                        doc_entry_key(&foreign_doc),
                        json!({"access": "Pull", "future": 1}),
                    ),
                    (child_entry_key(&child), json!({"access": "Preview"})),
                ]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    let view = node.view(&bob, &shared).await.unwrap();
    assert_eq!(
        view.docs,
        BTreeMap::from([(known_doc.clone(), Access::Read)])
    );
    assert!(view.children.is_empty());
    assert_eq!(
        node.list_docs(&bob, &shared, 0, None).await.unwrap(),
        vec![ListedDoc {
            doc_id: known_doc.clone(),
            access: Access::Read,
            via: None,
        }]
    );
    assert!(node.read_note(&bob, &known_doc).await.is_ok());
    assert_eq!(
        node.read_note(&bob, &foreign_doc).await.unwrap_err(),
        DocsError::Denied
    );
    assert_eq!(
        node.view(&bob, &child).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.roster_facets(&shared).await[&doc_entry_key(&foreign_doc)],
        json!({"access": "Pull", "future": 1})
    );
    node.stop().await;
}

/// Wait until the `facet_set` index has settled `doc_id` with at least one route.
async fn wait_for_facet_set(node: &Node, actor: &Actor, doc_id: &DocId) {
    let capability = node
        .mint_doc(
            actor,
            DocScope {
                read: [doc_id.clone()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    loop {
        let queried = node
            .docs()
            .call(
                RequestContext::root(),
                QueryFacets {
                    capability,
                    drawer_id: None,
                    selector: FacetSelector::Doc {
                        doc_id: doc_id.clone(),
                    },
                },
            )
            .await
            .unwrap();
        if !queried.is_empty() {
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// Wait until the `facet_ref` index has settled at least `expected` outgoing edges from `doc_id`.
async fn wait_for_facet_ref(node: &Node, actor: &Actor, doc_id: &DocId, expected: usize) {
    let capability = node
        .mint_doc(
            actor,
            DocScope {
                read: [doc_id.clone()].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    loop {
        let edges = node
            .docs()
            .call(
                RequestContext::root(),
                QueryFacetRefs {
                    capability,
                    drawer_id: None,
                    selector: FacetRefSelector::Origin {
                        doc_id: doc_id.clone(),
                        facet_key: None,
                    },
                },
            )
            .await
            .unwrap();
        if edges.len() >= expected {
            return;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn drawer_scoped_queries_and_reference_queries_obey_containment_and_attenuation() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let carol = node.actor("carol").await;

    let parent = node.collection(&alice, "parent").await;
    let child_read = node.collection(&alice, "child-read").await;
    let child_relay = node.collection(&alice, "child-relay").await;
    let other = node.collection(&alice, "other").await;

    let doc_direct = node.create_doc(&alice, &parent, "direct").await.unwrap();
    let doc_relay_entry = node
        .create_doc(&alice, &other, "relay-entry")
        .await
        .unwrap();
    let doc_in_child_read = node
        .create_doc(&alice, &child_read, "in-child-read")
        .await
        .unwrap();
    let doc_in_child_relay = node
        .create_doc(&alice, &child_relay, "in-child-relay")
        .await
        .unwrap();
    let doc_outside = node.create_doc(&alice, &other, "outside").await.unwrap();

    // Link `doc_direct -> doc_in_child_read` and `doc_outside -> doc_direct` so `facet_ref`
    // records both an in-drawer edge and an incoming edge from outside `parent`.
    let (note_key, linking_child) = note_linking("points to child", &doc_in_child_read);
    node.docs()
        .call(
            RequestContext::root(),
            WriteFacets {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: [doc_direct.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: None,
                doc_id: doc_direct.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(note_key.clone(), linking_child)]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    let (_, linking_direct) = note_linking("outside points to direct", &doc_direct);
    node.docs()
        .call(
            RequestContext::root(),
            WriteFacets {
                capability: node
                    .mint_doc(
                        &alice,
                        DocScope {
                            write: [doc_outside.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: None,
                doc_id: doc_outside.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(note_key.clone(), linking_direct)]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();

    for doc in [
        &doc_direct,
        &doc_relay_entry,
        &doc_in_child_read,
        &doc_in_child_relay,
        &doc_outside,
    ] {
        wait_for_facet_set(&node, &alice, doc).await;
    }
    wait_for_facet_ref(&node, &alice, &doc_direct, 1).await;
    wait_for_facet_ref(&node, &alice, &doc_outside, 1).await;

    // Enter `doc_relay_entry` into `parent` at `Relay`, and link `child_read` at `Read` and
    // `child_relay` at `Relay`.
    node.add_doc(
        &alice,
        "add-relay-doc",
        &parent,
        &doc_relay_entry,
        Access::Relay,
    )
    .await
    .unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child_read])
                    .await
                    .unwrap(),
                command: command("link-child-read"),
                parent_id: parent.clone(),
                child_id: child_read.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child_relay])
                    .await
                    .unwrap(),
                command: command("link-child-relay"),
                parent_id: parent.clone(),
                child_id: child_relay.clone(),
                access: Access::Relay,
            },
        )
        .await
        .unwrap();

    // Grant Bob `Read` on `parent`, and also `Read` on `other` (so Bob can read `doc_outside` and
    // `doc_relay_entry` through `other`, but NOT through `parent`). Grant Carol only `Relay` on
    // `parent`.
    node.grant(&alice, &parent, &bob, Access::Read).await;
    node.grant(&alice, &other, &bob, Access::Read).await;
    node.grant(&alice, &parent, &carol, Access::Relay).await;

    let note_tag = big_cabinet_core::FacetTag::new(NOTE).unwrap();

    // Carol holds only `Relay` on `parent`: she can `ListDocs`, but `QueryFacets` and
    // `QueryFacetRefs` on `parent` are refused (`Denied`).
    assert_eq!(
        node.list_docs(&carol, &parent, 0, None)
            .await
            .unwrap()
            .len(),
        4
    );
    let carol_query = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: node
                    .mint_doc(
                        &carol,
                        DocScope {
                            drawers: [(parent.clone(), Access::Relay)].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: Some(parent.clone()),
                selector: FacetSelector::Tag {
                    tag: note_tag.clone(),
                },
            },
        )
        .await;
    assert_eq!(
        carol_query.unwrap_err(),
        ClientError::Operation(DocsError::Denied)
    );

    // Bob's drawer-scoped `QueryFacets` on `parent` returns ONLY `doc_direct` and
    // `doc_in_child_read`: `doc_relay_entry` and `doc_in_child_relay` are attenuated to `Relay` in
    // `parent`, and `doc_outside` is not in `parent` even though Bob can read it via `other`.
    let bob_in_parent: BTreeSet<DocId> = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: node
                    .mint_doc(
                        &bob,
                        DocScope {
                            drawers: [(parent.clone(), Access::Read)].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: Some(parent.clone()),
                selector: FacetSelector::Tag {
                    tag: note_tag.clone(),
                },
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.doc_id)
        .collect();
    assert_eq!(
        bob_in_parent,
        BTreeSet::from([doc_direct.clone(), doc_in_child_read.clone()])
    );

    // Unscoped `QueryFacets` by tag for Bob includes `doc_outside` and `doc_relay_entry` too,
    // because Bob reaches them via `other`, while still hiding `doc_in_child_relay`.
    let bob_unscoped: BTreeSet<DocId> = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacets {
                capability: node
                    .mint_doc(
                        &bob,
                        DocScope {
                            drawers: [
                                (parent.clone(), Access::Read),
                                (other.clone(), Access::Read),
                            ]
                            .into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: None,
                selector: FacetSelector::Tag {
                    tag: note_tag.clone(),
                },
            },
        )
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.doc_id)
        .collect();
    assert_eq!(
        bob_unscoped,
        BTreeSet::from([
            doc_direct.clone(),
            doc_in_child_read.clone(),
            doc_relay_entry.clone(),
            doc_outside.clone(),
        ])
    );

    // Drawer-scoped `QueryFacetRefs` on `parent` for incoming edges to `doc_direct` omits the edge
    // from `doc_outside` (because `doc_outside` is not in `parent`), while unscoped
    // `QueryFacetRefs` for Bob includes it (because Bob can read `doc_outside`), and unscoped for
    // an actor who cannot read `other` omits it (no backlink leak from unreadable origin docs).
    let dave = node.actor("dave").await;
    node.grant(&alice, &parent, &dave, Access::Read).await;

    let scoped_backlinks = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: node
                    .mint_doc(
                        &bob,
                        DocScope {
                            drawers: [(parent.clone(), Access::Read)].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: Some(parent.clone()),
                selector: FacetRefSelector::Target {
                    doc_id: doc_direct.clone(),
                    facet_key: None,
                },
            },
        )
        .await
        .unwrap();
    assert!(scoped_backlinks.is_empty());

    let dave_unscoped_backlinks = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: node
                    .mint_doc(
                        &dave,
                        DocScope {
                            drawers: [(parent.clone(), Access::Read)].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: None,
                selector: FacetRefSelector::Target {
                    doc_id: doc_direct.clone(),
                    facet_key: None,
                },
            },
        )
        .await
        .unwrap();
    assert!(dave_unscoped_backlinks.is_empty());

    let bob_unscoped_backlinks = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: node
                    .mint_doc(
                        &bob,
                        DocScope {
                            drawers: [
                                (parent.clone(), Access::Read),
                                (other.clone(), Access::Read),
                            ]
                            .into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: None,
                selector: FacetRefSelector::Target {
                    doc_id: doc_direct.clone(),
                    facet_key: None,
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        bob_unscoped_backlinks,
        vec![QueriedFacetRef {
            origin_doc_id: doc_outside.clone(),
            origin_facet_key: note_key.clone(),
            path: "/link".to_owned(),
            target_doc_id: doc_direct.clone(),
            target_facet_key: note_key.clone(),
            ref_heads: Vec::new(),
        }]
    );

    let scoped_outgoing = node
        .docs()
        .call(
            RequestContext::root(),
            QueryFacetRefs {
                capability: node
                    .mint_doc(
                        &dave,
                        DocScope {
                            drawers: [(parent.clone(), Access::Read)].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                drawer_id: Some(parent.clone()),
                selector: FacetRefSelector::Origin {
                    doc_id: doc_direct.clone(),
                    facet_key: Some(note_key.clone()),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(
        scoped_outgoing,
        vec![QueriedFacetRef {
            origin_doc_id: doc_direct,
            origin_facet_key: note_key.clone(),
            path: "/link".to_owned(),
            target_doc_id: doc_in_child_read,
            target_facet_key: note_key,
            ref_heads: Vec::new(),
        }]
    );

    node.stop().await;
}

#[tokio::test]
async fn drawer_scoped_observation_replays_and_emits_live_membership_and_grant_changes() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;

    let drawer = node.collection(&alice, "observed-drawer").await;
    let child = node.collection(&alice, "observed-child").await;
    let other = node.collection(&alice, "other-drawer").await;

    let doc_initial = node.create_doc(&alice, &drawer, "initial").await.unwrap();
    let doc_added_later = node
        .create_doc(&alice, &other, "added-later")
        .await
        .unwrap();
    let doc_in_child = node.create_doc(&alice, &child, "in-child").await.unwrap();
    for doc in [&doc_initial, &doc_added_later, &doc_in_child] {
        wait_for_facet_set(&node, &alice, doc).await;
    }

    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let doc_scope = DocScope {
        drawers: [(drawer.clone(), Access::Read)].into(),
        ..Default::default()
    };
    let observation = ObserveFacets {
        capability: node.mint_doc(&bob, doc_scope.clone()).await.unwrap(),
        scope: ObserveScope::Drawer {
            drawer_id: drawer.clone(),
            tag: Some(big_cabinet_core::FacetTag::new(NOTE).unwrap()),
        },
        after: 0,
    };
    let mut events = node
        .docs()
        .subscribe(RequestContext::root(), observation.clone())
        .await
        .unwrap();

    // 1. Opening replay delivers `doc_initial` and then `ReplayComplete`.
    let first = events.next().await.unwrap().unwrap();
    let FacetObserved::Entry {
        doc_id,
        facet: ObservedFacet::Present { .. },
        ..
    } = first
    else {
        panic!("expected initial Present entry, got {first:?}");
    };
    assert_eq!(doc_id, doc_initial);
    let replay_done = events.next().await.unwrap().unwrap();
    assert!(
        matches!(replay_done, FacetObserved::ReplayComplete { .. }),
        "expected ReplayComplete, got {replay_done:?}"
    );

    // 2. Adding an already-settled doc (`doc_added_later`) to `drawer` at `Read` emits `Present`
    //    even though `doc_added_later` itself was not edited.
    node.add_doc(&alice, "add-later", &drawer, &doc_added_later, Access::Read)
        .await
        .unwrap();
    let added = events.next().await.unwrap().unwrap();
    let FacetObserved::Entry {
        doc_id,
        facet: ObservedFacet::Present { .. },
        ..
    } = added
    else {
        panic!("expected Present for newly added doc, got {added:?}");
    };
    assert_eq!(doc_id, doc_added_later);

    // 3. Narrowing `doc_added_later` to `Relay` in `drawer` emits `AccessLost`.
    node.add_doc(
        &alice,
        "narrow-later-to-relay",
        &drawer,
        &doc_added_later,
        Access::Relay,
    )
    .await
    .unwrap();
    let narrowed = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            narrowed,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::AccessLost,
                ..
            } if *doc_id == doc_added_later
        ),
        "expected AccessLost on narrowing to Relay, got {narrowed:?}"
    );

    // 4. Widening `doc_added_later` back to `Read` resumes observation (`Present`).
    node.add_doc(
        &alice,
        "widen-later-to-read",
        &drawer,
        &doc_added_later,
        Access::Read,
    )
    .await
    .unwrap();
    let widened = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            widened,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } if *doc_id == doc_added_later
        ),
        "expected Present on widening back to Read, got {widened:?}"
    );

    // 5. Removing `doc_added_later` from `drawer` emits `AccessLost`.
    node.remove_doc(&alice, "remove-later", &drawer, &doc_added_later)
        .await
        .unwrap();
    let removed = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            removed,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::AccessLost,
                ..
            } if *doc_id == doc_added_later
        ),
        "expected AccessLost on RemoveDoc, got {removed:?}"
    );

    // 6. Linking `child` at `Read` emits `Present` for `doc_in_child`; unlinking `child` emits
    //    `AccessLost` for `doc_in_child`.
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&drawer, &child])
                    .await
                    .unwrap(),
                command: command("link-child"),
                parent_id: drawer.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    let child_added = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            child_added,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } if *doc_id == doc_in_child
        ),
        "expected Present for child doc on AddChild, got {child_added:?}"
    );
    node.drawers()
        .call(
            RequestContext::root(),
            RemoveChild {
                capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                command: command("unlink-child"),
                parent_id: drawer.clone(),
                child_id: child.clone(),
            },
        )
        .await
        .unwrap();
    let child_removed = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            child_removed,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::AccessLost,
                ..
            } if *doc_id == doc_in_child
        ),
        "expected AccessLost for child doc on RemoveChild, got {child_removed:?}"
    );

    node.drawers()
        .call(
            RequestContext::root(),
            RevokeDrawer {
                capability: node.drawer_capability(&alice, &[&drawer]).await.unwrap(),
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        events.next().await.unwrap(),
        Err(ClientError::Operation(DocsError::Denied))
    );
    assert!(events.next().await.is_none());
    drop(events);
    assert_eq!(
        node.mint_doc(&bob, doc_scope.clone()).await.unwrap_err(),
        DocsError::Denied
    );
    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let refused = node
        .docs()
        .subscribe(RequestContext::root(), observation.clone())
        .await;
    assert!(
        matches!(refused, Err(ClientError::Operation(DocsError::Denied))),
        "regrant must not revive the withdrawn observation capability"
    );
    let mut events = node
        .docs()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability: node.mint_doc(&bob, doc_scope).await.unwrap(),
                ..observation
            },
        )
        .await
        .unwrap();
    let regranted = events.next().await.unwrap().unwrap();
    assert!(
        matches!(
            regranted,
            FacetObserved::Entry {
                ref doc_id,
                facet: ObservedFacet::Present { .. },
                ..
            } if *doc_id == doc_initial
        ),
        "expected Present on re-granting drawer Read, got {regranted:?}"
    );

    drop(events);
    node.stop().await;
}

#[tokio::test]
async fn synced_in_drawer_docs_are_discovered_and_projected_without_polluting_known_registry() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;

    // Create a parent drawer, a child drawer, and a doc inside the child drawer, then wipe the
    // local SQLite drawer projection rows for `parent` and `child` to simulate those drawer docs
    // and their member doc having arrived via BigRepo sync without local `CreateDrawer` calls.
    let parent = node.collection(&alice, "synced-parent").await;
    let child = node.collection(&alice, "synced-child").await;
    let doc = node
        .create_doc(&alice, &child, "synced-payload")
        .await
        .unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            AddChild {
                capability: node
                    .drawer_capability(&alice, &[&parent, &child])
                    .await
                    .unwrap(),
                command: command("link-synced-child"),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access: Access::Read,
            },
        )
        .await
        .unwrap();

    for id in [&parent, &child] {
        sqlx::query("DELETE FROM big_cabinet_drawer_docs WHERE drawer_id = ?")
            .bind(id.to_string())
            .execute(&node.sql.write_pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_cabinet_drawer_children WHERE parent_id = ? OR child_id = ?")
            .bind(id.to_string())
            .bind(id.to_string())
            .execute(&node.sql.write_pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM big_cabinet_drawers WHERE drawer_id = ?")
            .bind(id.to_string())
            .execute(&node.sql.write_pool)
            .await
            .unwrap();
    }

    // Grant Bob Read on `parent`. Neither `parent` nor `child` is in `big_cabinet_drawers`.
    node.grant(&alice, &parent, &bob, Access::Read).await;

    // 1. Reading `doc` as Bob triggers discovery of `parent` and its child `child` from the
    //    synced-in drawer docs and authorizes the read through `parent -> child -> doc`.
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap(),
        json!({"$type": "org.example.note", "value": "synced-payload"})
    );

    // 2. `ListDrawers` and `ListDocs` see the newly projected drawers and their transitive docs.
    let listed = node
        .drawers()
        .call(
            RequestContext::root(),
            ListDrawers {
                capability: node.drawer_capability(&bob, &[&parent]).await.unwrap(),
            },
        )
        .await
        .unwrap();
    let listed_ids: BTreeSet<DocId> = listed.iter().map(|d| d.drawer_id.clone()).collect();
    assert!(listed_ids.contains(&parent));
    assert!(listed_ids.contains(&child));

    let docs = node
        .drawers()
        .call(
            RequestContext::root(),
            ListDocs {
                capability: node.drawer_capability(&bob, &[&parent]).await.unwrap(),
                drawer_id: parent.clone(),
                offset: 0,
                limit: Some(20),
            },
        )
        .await
        .unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].doc_id, doc);
    assert_eq!(docs[0].access, Access::Read);

    // 3. Discovering and projecting synced-in drawers never auto-populates `registry_id`; only an
    //    explicit `RememberDrawer` adds a drawer to `ListKnownDrawers`.
    let known_before = node
        .drawers()
        .call(
            RequestContext::root(),
            ListKnownDrawers {
                capability: node
                    .drawer_capability(&node.drawers_actor, &[&node.local().registry_id])
                    .await
                    .unwrap(),
            },
        )
        .await
        .unwrap();
    assert!(known_before.is_empty());

    node.drawers()
        .call(
            RequestContext::root(),
            RememberDrawer {
                capability: node
                    .drawer_capability(&node.drawers_actor, &[&node.local().registry_id])
                    .await
                    .unwrap(),
                drawer_id: parent.clone(),
                mirror: true,
            },
        )
        .await
        .unwrap();
    let known_after = node
        .drawers()
        .call(
            RequestContext::root(),
            ListKnownDrawers {
                capability: node
                    .drawer_capability(&node.drawers_actor, &[&node.local().registry_id])
                    .await
                    .unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(known_after.len(), 1);
    assert_eq!(known_after[0].drawer_id, parent);

    node.stop().await;
}

#[tokio::test]
async fn delegated_drawer_capabilities_bind_signers_without_standing_grants() {
    use crate::authority::{AttenuateCapability, RetractCapability};

    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let plug = node.actor("plug").await;
    let drawer = node.collection(&alice, "workspace").await;
    let doc = node
        .create_doc(&alice, &drawer, "initial-note")
        .await
        .unwrap();
    node.grant(&alice, &drawer, &bob, Access::Write).await;
    let scope = DrawerScope {
        drawers: [(drawer.clone(), Access::Write)].into(),
        create: false,
    };
    let source = node.mint_drawer(&bob, scope.clone()).await.unwrap();
    let bound = node
        .drawers()
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization {
                    authority: source,
                    signer: plug.capability,
                },
                scope: scope.clone(),
                rights: Rights::USE | Rights::NARROW | Rights::DELEGATE,
            },
        )
        .await
        .unwrap();
    let delegated = node
        .authority
        .client
        .call(
            RequestContext::root(),
            AttenuateCapability {
                capability: bound.capability_id,
                rights: Rights::USE | Rights::DELEGATE,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let caller = node
        .core()
        .invocation(&RequestContext::root(), delegated)
        .await
        .unwrap();
    assert_eq!(caller.signer, plug);
    assert_eq!(caller.originating_actor, Some(bob.actor_id));
    assert_eq!(
        node.mint_drawer(&plug, scope).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        node.drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: delegated,
                    drawer_id: drawer.clone(),
                }
            )
            .await
            .unwrap()
            .access,
        Access::Write
    );

    for capability in [alice.capability, bob.capability, plug.capability] {
        assert_eq!(
            op(node
                .drawers()
                .call(
                    RequestContext::root(),
                    GetDrawer {
                        capability,
                        drawer_id: drawer.clone(),
                    }
                )
                .await
                .unwrap_err()),
            DrawersError::Denied
        );
    }
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GrantDrawer {
                    capability: delegated,
                    drawer_id: drawer.clone(),
                    subject_id: plug.actor_id,
                    access: Access::Write,
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                MintDrawerCapability {
                    authorization: Authorization {
                        authority: source,
                        signer: alice.capability
                    },
                    scope: DrawerScope {
                        drawers: [(drawer.clone(), Access::Admin)].into(),
                        create: false
                    },
                    rights: Rights::USE,
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );

    node.drawers()
        .call(
            RequestContext::root(),
            RemoveDoc {
                capability: delegated,
                command: command("plug removes entry"),
                drawer_id: drawer.clone(),
                doc_id: doc,
            },
        )
        .await
        .unwrap();
    let signer: String =
        sqlx::query_scalar("SELECT actor_uuid FROM big_cabinet_drawer_ops WHERE command_id = ?")
            .bind("plug removes entry")
            .fetch_one(&node.sql.read_pool)
            .await
            .unwrap();
    assert_eq!(signer, plug.actor_id.to_string());
    node.authority
        .client
        .call(
            RequestContext::root(),
            RetractCapability {
                via: source,
                capability: source,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: delegated,
                    drawer_id: drawer,
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    node.stop().await;
}

#[tokio::test]
async fn drawer_grant_withdrawal_retracts_issued_drawer_and_document_caps() {
    use crate::authority::{AttenuateCapability, RetractCapability};

    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let drawer = node.collection(&alice, "workspace").await;
    let doc = node.create_doc(&alice, &drawer, "note").await.unwrap();
    node.grant(&alice, &drawer, &bob, Access::Relay).await;
    let admin = node
        .mint_drawer(
            &alice,
            DrawerScope {
                drawers: [(drawer.clone(), Access::Admin)].into(),
                create: false,
            },
        )
        .await
        .unwrap();
    let no_delegate = node
        .authority
        .client
        .call(
            RequestContext::root(),
            AttenuateCapability {
                capability: admin,
                rights: Rights::USE | Rights::NARROW,
            },
        )
        .await
        .unwrap()
        .capability_id;
    for capability in [alice.capability, no_delegate] {
        assert_eq!(
            op(node
                .drawers()
                .call(
                    RequestContext::root(),
                    GrantDrawer {
                        capability,
                        drawer_id: drawer.clone(),
                        subject_id: bob.actor_id,
                        access: Access::Write,
                    }
                )
                .await
                .unwrap_err()),
            DrawersError::Denied
        );
    }
    node.drawers()
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: admin,
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
                access: Access::Write,
            },
        )
        .await
        .unwrap();
    let drawer_cap = node
        .mint_drawer(
            &bob,
            DrawerScope {
                drawers: [(drawer.clone(), Access::Write)].into(),
                create: false,
            },
        )
        .await
        .unwrap();
    let scopes = [
        DocScope {
            read: [doc.clone()].into(),
            ..Default::default()
        },
        DocScope {
            write: [doc.clone()].into(),
            ..Default::default()
        },
        DocScope {
            create: [drawer_placement(&drawer)].into(),
            ..Default::default()
        },
        DocScope {
            drawers: [(drawer.clone(), Access::Write)].into(),
            ..Default::default()
        },
    ];
    let mut doc_caps = Vec::new();
    for scope in &scopes {
        doc_caps.push(node.mint_doc(&bob, scope.clone()).await.unwrap());
    }
    node.core()
        .grants
        .revoke(
            &RequestContext::root(),
            bob.actor_id,
            &[drawer_object(Access::Relay, &drawer)],
        )
        .await
        .unwrap();
    for capability in std::iter::once(&drawer_cap).chain(&doc_caps) {
        node.authority
            .client
            .call(
                RequestContext::root(),
                crate::authority::InspectCapability {
                    capability: *capability,
                },
            )
            .await
            .unwrap();
    }
    node.drawers()
        .call(
            RequestContext::root(),
            RevokeDrawer {
                capability: admin,
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: drawer_cap,
                    drawer_id: drawer.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    for capability in doc_caps {
        assert_eq!(
            node.authority
                .client
                .call(
                    RequestContext::root(),
                    crate::authority::InspectCapability { capability }
                )
                .await
                .unwrap_err(),
            ClientError::Operation(AuthorityServiceError::InvalidCapability)
        );
    }
    for scope in scopes {
        assert_eq!(
            node.mint_doc(&bob, scope).await.unwrap_err(),
            DocsError::Denied
        );
    }
    node.drawers()
        .call(
            RequestContext::root(),
            GrantDrawer {
                capability: admin,
                drawer_id: drawer.clone(),
                subject_id: bob.actor_id,
                access: Access::Read,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: drawer_cap,
                    drawer_id: drawer.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert!(node.read_note(&bob, &doc).await.is_ok());
    node.authority
        .client
        .call(
            RequestContext::root(),
            RetractCapability {
                via: admin,
                capability: admin,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.read_note(&bob, &doc).await.unwrap_err(),
        DocsError::Denied
    );
    node.grant(&alice, &drawer, &bob, Access::Read).await;
    let temporary = node.drawer_capability(&bob, &[&drawer]).await.unwrap();
    node.restart().await.unwrap();
    assert_eq!(
        node.view(&bob, &drawer).await.unwrap_err(),
        DrawersError::Denied
    );
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                GetDrawer {
                    capability: temporary,
                    drawer_id: drawer.clone(),
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    assert_eq!(
        node.view(&alice, &drawer).await.unwrap().access,
        Access::Admin
    );
    node.stop().await;
}

#[tokio::test]
async fn alternative_ancestor_grants_only_retract_caps_using_withdrawn_support() {
    use crate::authority::InspectCapability;

    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let bob = node.actor("bob").await;
    let selected = node.collection(&alice, "selected-ancestor").await;
    let alternative = node.collection(&alice, "alternative-ancestor").await;
    let drawer = node.collection(&alice, "shared-child").await;
    for (parent, name) in [
        (&selected, "link-selected"),
        (&alternative, "link-alternative"),
    ] {
        node.drawers()
            .call(
                RequestContext::root(),
                AddChild {
                    capability: node
                        .drawer_capability(&alice, &[parent, &drawer])
                        .await
                        .unwrap(),
                    command: command(name),
                    parent_id: parent.clone(),
                    child_id: drawer.clone(),
                    access: Access::Write,
                },
            )
            .await
            .unwrap();
    }
    let doc = node.create_doc(&alice, &drawer, "note").await.unwrap();
    node.grant(&alice, &selected, &bob, Access::Write).await;
    node.grant(&alice, &alternative, &bob, Access::Read).await;

    let context = RequestContext::root();
    assert_eq!(
        node.core()
            .effective(&context, bob.actor_id, &alternative)
            .await
            .unwrap()
            .unwrap()
            .0,
        Access::Read
    );
    let selected_proof = node
        .core()
        .effective(&context, bob.actor_id, &selected)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(selected_proof.0, Access::Write);
    assert!(!selected_proof.1.is_empty());
    assert_eq!(
        node.core()
            .effective(&context, bob.actor_id, &drawer)
            .await
            .unwrap(),
        Some(selected_proof.clone())
    );
    assert_eq!(
        node.core()
            .reach(&context, bob.actor_id, &doc)
            .await
            .unwrap(),
        Some(selected_proof)
    );
    let drawer_scope = DrawerScope {
        drawers: [(drawer.clone(), Access::Read)].into(),
        ..Default::default()
    };
    let doc_scope = DocScope {
        read: [doc.clone()].into(),
        ..Default::default()
    };
    let drawer_cap = node.mint_drawer(&bob, drawer_scope.clone()).await.unwrap();
    let doc_cap = node.mint_doc(&bob, doc_scope.clone()).await.unwrap();
    let get = |capability| {
        node.drawers().call(
            context.child(),
            GetDrawer {
                capability,
                drawer_id: drawer.clone(),
            },
        )
    };
    let (note_key, note_value) = note("note");
    let read = |capability| {
        node.docs().call(
            context.child(),
            ReadFacets {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                facets: Some([note_key.clone()].into()),
                optimistic: false,
            },
        )
    };
    let expected_view = get(drawer_cap).await.unwrap();
    assert_eq!(expected_view.access, Access::Read);
    assert_eq!(
        expected_view.docs,
        BTreeMap::from([(doc.clone(), Access::Admin)])
    );
    let expected_read = read(doc_cap).await.unwrap();
    assert!(matches!(
        expected_read.facets.get(&note_key),
        Some(FacetRead::Accepted { value, .. }) if *value == note_value
    ));

    let admin = node
        .drawer_capability(&alice, &[&selected, &alternative])
        .await
        .unwrap();
    let revoke = |drawer_id: &DrawerId| {
        node.drawers().call(
            context.child(),
            RevokeDrawer {
                capability: admin,
                drawer_id: drawer_id.clone(),
                subject_id: bob.actor_id,
            },
        )
    };
    revoke(&alternative).await.unwrap();
    assert_eq!(
        node.core()
            .effective(&context, bob.actor_id, &alternative)
            .await
            .unwrap(),
        None
    );
    assert_eq!(get(drawer_cap).await.unwrap(), expected_view);
    assert_eq!(read(doc_cap).await.unwrap(), expected_read);

    node.grant(&alice, &alternative, &bob, Access::Read).await;
    revoke(&selected).await.unwrap();
    for capability in [drawer_cap, doc_cap] {
        assert_eq!(
            node.authority
                .client
                .call(context.child(), InspectCapability { capability })
                .await
                .unwrap_err(),
            ClientError::Operation(AuthorityServiceError::InvalidCapability)
        );
    }
    let fresh_drawer_cap = node.mint_drawer(&bob, drawer_scope).await.unwrap();
    let fresh_doc_cap = node.mint_doc(&bob, doc_scope).await.unwrap();
    assert_ne!(fresh_drawer_cap, drawer_cap);
    assert_ne!(fresh_doc_cap, doc_cap);
    assert_eq!(get(fresh_drawer_cap).await.unwrap(), expected_view);
    assert_eq!(read(fresh_doc_cap).await.unwrap(), expected_read);
    assert_eq!(
        get(drawer_cap).await.unwrap_err(),
        ClientError::Operation(DrawersError::Denied)
    );
    assert_eq!(
        read(doc_cap).await.unwrap_err(),
        ClientError::Operation(DocsError::Denied)
    );
    node.stop().await;
}

#[tokio::test]
async fn delegated_creation_preserves_owner_and_records_the_bound_signer() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let plug = node.actor("plug").await;
    let home = node.collection(&alice, "home").await;
    let doc = node.create_doc(&alice, &home, "note").await.unwrap();
    let scope = DrawerScope {
        drawers: [(home.clone(), Access::Write)].into(),
        create: true,
    };
    let source = node.mint_drawer(&alice, scope.clone()).await.unwrap();
    let delegated = node
        .drawers()
        .call(
            RequestContext::root(),
            MintDrawerCapability {
                authorization: Authorization {
                    authority: source,
                    signer: plug.capability,
                },
                scope,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id;
    assert_eq!(
        op(node
            .drawers()
            .call(
                RequestContext::root(),
                CreateDrawer {
                    capability: alice.capability,
                    command: command("identity cannot create"),
                    layout: CreateLayout::SingleGroup,
                }
            )
            .await
            .unwrap_err()),
        DrawersError::Denied
    );
    let mut created = Vec::new();
    for (name, layout) in [
        ("delegated collection", CreateLayout::SingleGroup),
        (
            "delegated self doc",
            CreateLayout::SelfDoc {
                doc_id: doc.clone(),
            },
        ),
    ] {
        let receipt = node
            .drawers()
            .call(
                RequestContext::root(),
                CreateDrawer {
                    capability: delegated,
                    command: command(name),
                    layout,
                },
            )
            .await
            .unwrap();
        let (actor, operation): (String, String) = sqlx::query_as(
            "SELECT actor_uuid
                  , op_json
               FROM big_cabinet_drawer_ops
              WHERE command_id = ?",
        )
        .bind(name)
        .fetch_one(&node.sql.read_pool)
        .await
        .unwrap();
        assert_eq!(actor, plug.actor_id.to_string());
        let operation: ledger::Op = serde_json::from_str(&operation).unwrap();
        match operation {
            ledger::Op::Create { owner_id, .. } | ledger::Op::CreateSelfDoc { owner_id, .. } => {
                assert_eq!(owner_id, Some(alice.actor_id));
            }
            other => panic!("unexpected creation operation: {other:?}"),
        }
        assert_eq!(
            node.view(&alice, &receipt.drawer_id).await.unwrap().access,
            Access::Admin
        );
        assert_eq!(
            node.view(&plug, &receipt.drawer_id).await.unwrap_err(),
            DrawersError::Denied
        );
        created.push(receipt.drawer_id);
    }
    node.core()
        .grants
        .revoke(
            &RequestContext::root(),
            alice.actor_id,
            &[CREATE_DRAWER_OBJECT.into()],
        )
        .await
        .unwrap();
    for capability in [source, delegated] {
        assert_eq!(
            op(node
                .drawers()
                .call(
                    RequestContext::root(),
                    CreateDrawer {
                        capability,
                        command: command("withdrawn creation grant"),
                        layout: CreateLayout::SingleGroup,
                    },
                )
                .await
                .unwrap_err()),
            DrawersError::Denied
        );
    }
    assert_eq!(
        node.mint_drawer(
            &alice,
            DrawerScope {
                create: true,
                ..Default::default()
            },
        )
        .await
        .unwrap_err(),
        DrawersError::Denied
    );
    node.restart().await.unwrap();
    for drawer in created {
        assert_eq!(
            node.view(&alice, &drawer).await.unwrap().access,
            Access::Admin
        );
        assert_eq!(
            node.view(&plug, &drawer).await.unwrap_err(),
            DrawersError::Denied
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn interrupted_delegated_creation_recovers_owner_and_bound_signer_for_both_layouts() {
    let mut node = Node::boot().await;
    let alice = node.creator("alice").await;
    let plug = node.actor("plug").await;
    let home = node.collection(&alice, "home").await;
    let doc = node.create_doc(&alice, &home, "note").await.unwrap();
    let context = RequestContext::root();
    let scope = DrawerScope {
        drawers: [(home, Access::Write)].into(),
        create: true,
    };
    let source = node.mint_drawer(&alice, scope.clone()).await.unwrap();
    let delegated = node
        .drawers()
        .call(
            context.child(),
            MintDrawerCapability {
                authorization: Authorization {
                    authority: source,
                    signer: plug.capability,
                },
                scope,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id;
    let caller = node.core().invocation(&context, delegated).await.unwrap();
    assert_eq!(caller.signer, plug);
    assert_eq!(caller.originating_actor, Some(alice.actor_id));
    drop(
        node.core()
            .authorize_create(&context, &caller.authority)
            .await
            .unwrap(),
    );
    assert_eq!(
        node.core()
            .doc_level(&context, &caller.authority, &doc)
            .await
            .unwrap(),
        Some(Access::Write)
    );

    let collection_command = command("interrupted delegated collection");
    node.core()
        .ledger
        .plant(
            &collection_command,
            crate::doc::pipeline::digest(&(
                "createDrawer",
                caller.signer.actor_id,
                caller.originating_actor,
            )),
            caller.signer.actor_id,
            &ledger::Op::Create {
                groups: None,
                owner_id: caller.originating_actor,
            },
        )
        .await
        .unwrap();
    let meta = node
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    let docs = node
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    let groups = DrawerGroups {
        meta_group: group_key(&meta),
        docs_group: group_key(&docs),
    };
    let collection_op = ledger::Op::Create {
        groups: Some(groups),
        owner_id: caller.originating_actor,
    };
    node.core()
        .ledger
        .record(&collection_command, &collection_op)
        .await
        .unwrap();

    let self_doc_command = command("interrupted delegated self doc");
    let self_doc_op = ledger::Op::CreateSelfDoc {
        doc_id: doc.clone(),
        owner_id: caller.originating_actor,
    };
    node.core()
        .ledger
        .plant(
            &self_doc_command,
            crate::doc::pipeline::digest(&(
                "createSelfDoc",
                caller.signer.actor_id,
                caller.originating_actor,
                &doc,
            )),
            caller.signer.actor_id,
            &self_doc_op,
        )
        .await
        .unwrap();
    let written = node
        .docs()
        .call(
            context.child(),
            WriteFacets {
                capability: node
                    .core()
                    .doc_capability(
                        &context,
                        caller.signer.capability,
                        DocScope {
                            write: [doc.clone()].into(),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                command: Some(self_doc_command.clone()),
                doc_id: doc.clone(),
                mode: WriteMode::AtCurrent,
                set: [(
                    drawer_key(),
                    serde_json::to_value(Drawer::self_doc()).unwrap(),
                )]
                .into(),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.roster_facets(&doc).await,
        [(
            drawer_key(),
            serde_json::to_value(Drawer::self_doc()).unwrap()
        )]
        .into()
    );
    assert!(node.core().state.drawer(&doc).await.unwrap().is_none());
    assert!(
        node.core()
            .grants
            .matching(
                &context,
                alice.actor_id,
                &[drawer_object(Access::Admin, &doc)],
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .rights
            .is_empty()
    );
    let unfinished = node.core().ledger.unfinished().await.unwrap();
    assert_eq!(unfinished.len(), 2);
    assert!(
        unfinished
            .iter()
            .all(|row| row.step == ledger::Step::Begun && row.receipt.is_none())
    );

    node.restart().await.unwrap();
    assert!(node.core().ledger.unfinished().await.unwrap().is_empty());
    let mut recovered = Vec::new();
    for (command, expected_op, descriptor) in [
        (
            &collection_command,
            &collection_op,
            Drawer::single_group(groups.meta_group, groups.docs_group),
        ),
        (&self_doc_command, &self_doc_op, Drawer::self_doc()),
    ] {
        let (actor, operation, step, receipt): (String, String, String, String) = sqlx::query_as(
            "SELECT actor_uuid
                  , op_json
                  , step
                  , receipt_json
               FROM big_cabinet_drawer_ops
              WHERE command_id = ?",
        )
        .bind(command.as_str())
        .fetch_one(&node.sql.read_pool)
        .await
        .unwrap();
        assert_eq!(actor, plug.actor_id.to_string());
        assert_eq!(
            serde_json::from_str::<ledger::Op>(&operation).unwrap(),
            *expected_op
        );
        assert_eq!(step, "done");
        let created: DrawerCreated = serde_json::from_str(&receipt).unwrap();
        if command == &self_doc_command {
            assert_eq!(
                created,
                DrawerCreated {
                    drawer_id: doc.clone(),
                    heads: written.heads.clone(),
                }
            );
        }
        let view = node.view(&alice, &created.drawer_id).await.unwrap();
        assert_eq!(view.access, Access::Admin);
        assert_eq!(view.descriptor, descriptor);
        assert_eq!(
            node.view(&plug, &created.drawer_id).await.unwrap_err(),
            DrawersError::Denied
        );
        recovered.push(created.drawer_id);
    }
    node.restart().await.unwrap();
    for drawer in recovered {
        assert_eq!(
            node.view(&alice, &drawer).await.unwrap().access,
            Access::Admin
        );
        assert_eq!(
            node.view(&plug, &drawer).await.unwrap_err(),
            DrawersError::Denied
        );
    }
    node.stop().await;
}

mod model {
    use super::*;
    use proptest::collection::vec as prop_vec;
    use proptest::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const DRAWERS: usize = 3;
    const DOCS: usize = 2;

    #[derive(Clone, Debug)]
    enum RecordedCommand {
        AddDoc(AddDoc),
        RemoveDoc(RemoveDoc),
        AddChild(AddChild),
        RemoveChild(RemoveChild),
    }

    impl RecordedCommand {
        async fn replay(
            &self,
            node: &Node,
            capability: CapId,
        ) -> Result<WriteReceipt, DrawersError> {
            match self.clone() {
                Self::AddDoc(mut req) => {
                    req.capability = capability;
                    node.drawers()
                        .call(RequestContext::root(), req)
                        .await
                        .map_err(op)
                }
                Self::RemoveDoc(mut req) => {
                    req.capability = capability;
                    node.drawers()
                        .call(RequestContext::root(), req)
                        .await
                        .map_err(op)
                }
                Self::AddChild(mut req) => {
                    req.capability = capability;
                    node.drawers()
                        .call(RequestContext::root(), req)
                        .await
                        .map_err(op)
                }
                Self::RemoveChild(mut req) => {
                    req.capability = capability;
                    node.drawers()
                        .call(RequestContext::root(), req)
                        .await
                        .map_err(op)
                }
            }
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ModelOp {
        AddDoc {
            drawer: usize,
            doc: usize,
            access: Access,
        },
        RemoveDoc {
            drawer: usize,
            doc: usize,
        },
        AddChild {
            parent: usize,
            child: usize,
            access: Access,
        },
        RemoveChild {
            parent: usize,
            child: usize,
        },
        Grant {
            drawer: usize,
            access: Access,
        },
        Revoke {
            drawer: usize,
        },
        Remember {
            drawer: usize,
            mirror: bool,
        },
        SetPolicy {
            drawer: usize,
            mirror: bool,
        },
        Forget {
            drawer: usize,
        },
        Replay(usize),
        Restart,
    }

    struct DrawerModel {
        drawers: [DrawerId; DRAWERS],
        docs: [DocId; DOCS],
        roster_docs: [BTreeMap<DocId, Access>; DRAWERS],
        roster_children: [BTreeMap<DrawerId, Access>; DRAWERS],
        bob_grants: [BTreeSet<Access>; DRAWERS],
        known: BTreeMap<DrawerId, bool>,
        commands: Vec<(RecordedCommand, WriteReceipt)>,
    }

    impl DrawerModel {
        fn new(drawers: [DrawerId; DRAWERS], docs: [DocId; DOCS]) -> Self {
            Self {
                drawers,
                docs,
                roster_docs: std::array::from_fn(|_| BTreeMap::new()),
                roster_children: std::array::from_fn(|_| BTreeMap::new()),
                bob_grants: std::array::from_fn(|_| BTreeSet::new()),
                known: BTreeMap::new(),
                commands: Vec::new(),
            }
        }

        fn drawer_index(&self, id: &DrawerId) -> Option<usize> {
            self.drawers.iter().position(|d| d == id)
        }

        fn held_bob(&self, drawer: usize) -> Option<Access> {
            self.bob_grants[drawer].iter().copied().max()
        }

        /// Strongest effective access Bob holds on `target`, directly or through any parent path
        /// (weakest link along each path, strongest path winning, cycle-safe).
        fn effective_bob(&self, target: usize) -> Option<Access> {
            let mut cap: [Option<Access>; DRAWERS] = [None; DRAWERS];
            cap[target] = Some(Access::Admin);
            for _ in 0..DRAWERS {
                for parent in 0..DRAWERS {
                    for (child_id, &edge) in &self.roster_children[parent] {
                        if let Some(child) = self.drawer_index(child_id)
                            && let Some(child_cap) = cap[child]
                        {
                            let via = edge.min(child_cap);
                            if cap[parent].is_none_or(|cur| via > cur) {
                                cap[parent] = Some(via);
                            }
                        }
                    }
                }
            }
            (0..DRAWERS)
                .filter_map(|d| self.held_bob(d).zip(cap[d]).map(|(h, c)| h.min(c)))
                .max()
        }

        /// Reference `ListDocs` output for Bob on `root`.
        fn scoped_docs_bob(&self, root: usize) -> Option<Vec<ListedDoc>> {
            let eff = self.effective_bob(root)?;
            let mut bounds: BTreeMap<DrawerId, Access> =
                BTreeMap::from([(self.drawers[root].clone(), eff)]);
            let mut frontier = vec![(root, eff)];
            while let Some((curr, bound)) = frontier.pop() {
                for (child_id, &edge) in &self.roster_children[curr] {
                    let via = bound.min(edge);
                    if bounds.get(child_id).is_some_and(|known| *known >= via) {
                        continue;
                    }
                    bounds.insert(child_id.clone(), via);
                    if let Some(child_idx) = self.drawer_index(child_id) {
                        frontier.push((child_idx, via));
                    }
                }
            }
            let mut entries: BTreeMap<DocId, (Access, Option<DrawerId>)> = BTreeMap::new();
            for (doc_id, &entry) in &self.roster_docs[root] {
                entries.insert(doc_id.clone(), (eff.min(entry), None));
            }
            for (child_id, &via) in &bounds {
                if *child_id == self.drawers[root] {
                    continue;
                }
                if let Some(child_idx) = self.drawer_index(child_id) {
                    for (doc_id, &entry) in &self.roster_docs[child_idx] {
                        let reached = via.min(entry);
                        match entries.get(doc_id) {
                            Some((known, _)) if *known >= reached => {}
                            _ => {
                                entries.insert(doc_id.clone(), (reached, Some(child_id.clone())));
                            }
                        }
                    }
                }
            }
            Some(
                entries
                    .into_iter()
                    .map(|(doc_id, (access, via))| ListedDoc {
                        doc_id,
                        access,
                        via,
                    })
                    .collect(),
            )
        }

        /// Strongest access Bob reaches on `doc_idx` across all drawers.
        fn reach_bob(&self, doc_idx: usize) -> Option<Access> {
            let doc = &self.docs[doc_idx];
            (0..DRAWERS)
                .filter_map(|d| {
                    self.effective_bob(d)
                        .zip(self.roster_docs[d].get(doc).copied())
                        .map(|(eff, entry)| eff.min(entry))
                })
                .max()
        }

        async fn check(&self, node: &Node, alice: &Actor, bob: &Actor, context: &str) {
            let expected_listed: BTreeMap<DrawerId, Access> = (0..DRAWERS)
                .filter_map(|d| {
                    self.effective_bob(d)
                        .map(|eff| (self.drawers[d].clone(), eff))
                })
                .collect();
            let actual_listed: BTreeMap<DrawerId, Access> = node
                .drawers()
                .call(
                    RequestContext::root(),
                    ListDrawers {
                        capability: node
                            .mint_drawer(
                                bob,
                                DrawerScope {
                                    drawers: (0..DRAWERS)
                                        .filter_map(|index| {
                                            self.held_bob(index)
                                                .map(|access| (self.drawers[index].clone(), access))
                                        })
                                        .collect(),
                                    create: false,
                                },
                            )
                            .await
                            .unwrap(),
                    },
                )
                .await
                .unwrap()
                .into_iter()
                .map(|d| (d.drawer_id, d.access))
                .collect();
            assert_eq!(actual_listed, expected_listed, "{context}: ListDrawers");

            for d in 0..DRAWERS {
                let drawer_id = &self.drawers[d];
                match self.effective_bob(d) {
                    None => {
                        assert_eq!(
                            node.view(bob, drawer_id).await.unwrap_err(),
                            DrawersError::Denied,
                            "{context}: GetDrawer denied on D{d}"
                        );
                        assert_eq!(
                            node.list_docs(bob, drawer_id, 0, None).await.unwrap_err(),
                            DrawersError::Denied,
                            "{context}: ListDocs denied on D{d}"
                        );
                    }
                    Some(eff) => {
                        let view = node.view(bob, drawer_id).await.unwrap();
                        assert_eq!(view.access, eff, "{context}: GetDrawer access on D{d}");
                        assert_eq!(
                            view.docs, self.roster_docs[d],
                            "{context}: GetDrawer docs on D{d}"
                        );
                        assert_eq!(
                            view.children, self.roster_children[d],
                            "{context}: GetDrawer children on D{d}"
                        );
                        let listed_docs = node.list_docs(bob, drawer_id, 0, None).await.unwrap();
                        assert_eq!(
                            Some(listed_docs),
                            self.scoped_docs_bob(d),
                            "{context}: ListDocs on D{d}"
                        );
                    }
                }
            }

            for doc_idx in 0..DOCS {
                let doc_id = &self.docs[doc_idx];
                let reached = self.reach_bob(doc_idx);
                let read_res = node.read_note(bob, doc_id).await;
                if reached >= Some(Access::Read) {
                    assert!(
                        read_res.is_ok(),
                        "{context}: expected Read on Doc{doc_idx} (reached {reached:?}), got {read_res:?}"
                    );
                } else {
                    assert_eq!(
                        read_res.unwrap_err(),
                        DocsError::Denied,
                        "{context}: expected Read Denied on Doc{doc_idx} (reached {reached:?})"
                    );
                }
                let write_res = node.write_note(bob, doc_id, context).await;
                if reached >= Some(Access::Write) {
                    assert!(
                        write_res.is_ok(),
                        "{context}: expected Write on Doc{doc_idx} (reached {reached:?}), got {write_res:?}"
                    );
                } else {
                    assert_eq!(
                        write_res.unwrap_err(),
                        DocsError::Denied,
                        "{context}: expected Write Denied on Doc{doc_idx} (reached {reached:?})"
                    );
                }
            }

            let actual_known: BTreeMap<DrawerId, bool> = node
                .known_map(alice)
                .await
                .into_iter()
                .filter(|(id, _)| self.drawer_index(id).is_some())
                .collect();
            assert_eq!(actual_known, self.known, "{context}: ListKnownDrawers");
        }
    }

    async fn run_script(node: &mut Node, case: &str, script: &[ModelOp]) {
        let alice = node.creator(&format!("alice-{case}")).await;
        let bob = node.actor(&format!("bob-{case}")).await;
        node.core()
            .grants
            .grant(
                &RequestContext::root(),
                alice.actor_id,
                drawer_object(Access::Admin, &node.local().registry_id),
                Rights::USE,
                None,
            )
            .await
            .unwrap();
        let home = node.collection(&alice, &format!("{case}/home")).await;
        let drawers = [
            node.collection(&alice, &format!("{case}/d0")).await,
            node.collection(&alice, &format!("{case}/d1")).await,
            node.collection(&alice, &format!("{case}/d2")).await,
        ];
        let docs = [
            node.create_doc(&alice, &home, "doc0").await.unwrap(),
            node.create_doc(&alice, &home, "doc1").await.unwrap(),
        ];
        let mut model = DrawerModel::new(drawers, docs);
        model
            .check(node, &alice, &bob, &format!("{case}/init"))
            .await;

        for (step, &op_step) in script.iter().enumerate() {
            let ctx = format!("{case}/{step}:{op_step:?}");
            let cmd = command(&format!("{case}/{step}"));
            let capability = node
                .mint_drawer(
                    &alice,
                    DrawerScope {
                        drawers: model
                            .drawers
                            .iter()
                            .chain([&home, &node.local().registry_id])
                            .map(|drawer| (drawer.clone(), Access::Admin))
                            .collect(),
                        create: false,
                    },
                )
                .await
                .unwrap();
            match op_step {
                ModelOp::AddDoc {
                    drawer,
                    doc,
                    access,
                } => {
                    let req = AddDoc {
                        capability,
                        command: cmd,
                        drawer_id: model.drawers[drawer].clone(),
                        doc_id: model.docs[doc].clone(),
                        access,
                    };
                    let receipt = node
                        .drawers()
                        .call(RequestContext::root(), req.clone())
                        .await
                        .unwrap();
                    model.roster_docs[drawer].insert(model.docs[doc].clone(), access);
                    model.commands.push((RecordedCommand::AddDoc(req), receipt));
                }
                ModelOp::RemoveDoc { drawer, doc } => {
                    let doc_id = model.docs[doc].clone();
                    let req = RemoveDoc {
                        capability,
                        command: cmd,
                        drawer_id: model.drawers[drawer].clone(),
                        doc_id: doc_id.clone(),
                    };
                    let res = node
                        .drawers()
                        .call(RequestContext::root(), req.clone())
                        .await
                        .map_err(op);
                    if model.roster_docs[drawer].remove(&doc_id).is_some() {
                        let receipt = res.expect(&ctx);
                        model
                            .commands
                            .push((RecordedCommand::RemoveDoc(req), receipt));
                    } else {
                        assert_eq!(res.unwrap_err(), DrawersError::NotAnEntry(doc_id), "{ctx}");
                    }
                }
                ModelOp::AddChild {
                    parent,
                    child,
                    access,
                } => {
                    let req = AddChild {
                        capability,
                        command: cmd,
                        parent_id: model.drawers[parent].clone(),
                        child_id: model.drawers[child].clone(),
                        access,
                    };
                    let res = node
                        .drawers()
                        .call(RequestContext::root(), req.clone())
                        .await
                        .map_err(op);
                    if parent == child {
                        assert_eq!(res.unwrap_err(), DrawersError::SelfChild, "{ctx}");
                    } else {
                        let receipt = res.expect(&ctx);
                        model.roster_children[parent].insert(model.drawers[child].clone(), access);
                        model
                            .commands
                            .push((RecordedCommand::AddChild(req), receipt));
                    }
                }
                ModelOp::RemoveChild { parent, child } => {
                    let child_id = model.drawers[child].clone();
                    let req = RemoveChild {
                        capability,
                        command: cmd,
                        parent_id: model.drawers[parent].clone(),
                        child_id: child_id.clone(),
                    };
                    let res = node
                        .drawers()
                        .call(RequestContext::root(), req.clone())
                        .await
                        .map_err(op);
                    if model.roster_children[parent].remove(&child_id).is_some() {
                        let receipt = res.expect(&ctx);
                        model
                            .commands
                            .push((RecordedCommand::RemoveChild(req), receipt));
                    } else {
                        assert_eq!(
                            res.unwrap_err(),
                            DrawersError::NotAnEntry(child_id),
                            "{ctx}"
                        );
                    }
                }
                ModelOp::Grant { drawer, access } => {
                    node.grant(&alice, &model.drawers[drawer], &bob, access)
                        .await;
                    model.bob_grants[drawer].insert(access);
                }
                ModelOp::Revoke { drawer } => {
                    node.drawers()
                        .call(
                            RequestContext::root(),
                            RevokeDrawer {
                                capability,
                                drawer_id: model.drawers[drawer].clone(),
                                subject_id: bob.actor_id,
                            },
                        )
                        .await
                        .unwrap();
                    model.bob_grants[drawer].clear();
                }
                ModelOp::Remember { drawer, mirror } => {
                    let drawer_id = model.drawers[drawer].clone();
                    node.remember(&alice, &drawer_id, mirror).await;
                    model.known.insert(drawer_id, mirror);
                }
                ModelOp::SetPolicy { drawer, mirror } => {
                    let drawer_id = model.drawers[drawer].clone();
                    let res = node
                        .drawers()
                        .call(
                            RequestContext::root(),
                            SetDrawerPolicy {
                                capability,
                                drawer_id: drawer_id.clone(),
                                mirror,
                            },
                        )
                        .await
                        .map_err(op);
                    if let Some(entry) = model.known.get_mut(&drawer_id) {
                        res.expect(&ctx);
                        *entry = mirror;
                    } else {
                        assert_eq!(
                            res.unwrap_err(),
                            DrawersError::UnknownDrawer(drawer_id),
                            "{ctx}"
                        );
                    }
                }
                ModelOp::Forget { drawer } => {
                    let drawer_id = model.drawers[drawer].clone();
                    node.forget(&alice, &drawer_id).await;
                    model.known.remove(&drawer_id);
                }
                ModelOp::Replay(seed) => {
                    if !model.commands.is_empty() {
                        let (recorded, expected) = &model.commands[seed % model.commands.len()];
                        assert_eq!(
                            &recorded.replay(node, capability).await.unwrap(),
                            expected,
                            "{ctx}"
                        );
                    }
                }
                ModelOp::Restart => {
                    node.restart().await.unwrap();
                    for grants in &mut model.bob_grants {
                        grants.clear();
                    }
                }
            }
            model.check(node, &alice, &bob, &ctx).await;
        }
    }

    #[tokio::test]
    async fn deterministic_drawer_model_scenarios_match_reference() {
        let mut node = Node::boot().await;

        // Scenario 1: Diamond + cycle hierarchy with weakest-link attenuation, strongest-path
        // winning, known-drawer lifecycle, replay, and restart.
        let diamond_and_cycle = [
            ModelOp::AddDoc {
                drawer: 2,
                doc: 0,
                access: Access::Write,
            },
            ModelOp::AddDoc {
                drawer: 1,
                doc: 1,
                access: Access::Read,
            },
            // Link D0 -> D1 (Read) -> D2 (Write), and D2 -> D0 (Relay) forming a cycle.
            ModelOp::AddChild {
                parent: 0,
                child: 1,
                access: Access::Read,
            },
            ModelOp::AddChild {
                parent: 1,
                child: 2,
                access: Access::Write,
            },
            ModelOp::AddChild {
                parent: 2,
                child: 0,
                access: Access::Relay,
            },
            ModelOp::AddChild {
                parent: 1,
                child: 1,
                access: Access::Admin,
            },
            // Grant Bob Write on D0: reaches D1 at Read, D2 at Read (capped by D0->D1 Read).
            ModelOp::Grant {
                drawer: 0,
                access: Access::Write,
            },
            // Add direct link D0 -> D2 at Write: now Bob reaches D2 at Write (strongest path wins)!
            ModelOp::AddChild {
                parent: 0,
                child: 2,
                access: Access::Write,
            },
            ModelOp::Remember {
                drawer: 0,
                mirror: false,
            },
            ModelOp::SetPolicy {
                drawer: 0,
                mirror: true,
            },
            ModelOp::SetPolicy {
                drawer: 2,
                mirror: false,
            },
            ModelOp::Restart,
            ModelOp::Replay(0),
            ModelOp::Replay(2),
            // Remove direct link D0 -> D2: Bob drops back to Read on D2 via D0 -> D1 -> D2.
            ModelOp::RemoveChild {
                parent: 0,
                child: 2,
            },
            ModelOp::Forget { drawer: 0 },
            ModelOp::Forget { drawer: 0 },
            ModelOp::Revoke { drawer: 0 },
            ModelOp::RemoveDoc { drawer: 2, doc: 0 },
            ModelOp::RemoveDoc { drawer: 2, doc: 0 },
        ];
        run_script(&mut node, "diamond-cycle", &diamond_and_cycle).await;
        node.stop().await;
    }

    fn any_access() -> impl Strategy<Value = Access> {
        prop_oneof![
            Just(Access::Relay),
            Just(Access::Read),
            Just(Access::Write),
            Just(Access::Admin),
        ]
    }

    fn any_model_op() -> impl Strategy<Value = ModelOp> {
        prop_oneof![
            3 => (0..DRAWERS, 0..DOCS, any_access()).prop_map(|(drawer, doc, access)| {
                ModelOp::AddDoc { drawer, doc, access }
            }),
            2 => (0..DRAWERS, 0..DOCS).prop_map(|(drawer, doc)| ModelOp::RemoveDoc { drawer, doc }),
            3 => (0..DRAWERS, 0..DRAWERS, any_access()).prop_map(|(parent, child, access)| {
                ModelOp::AddChild { parent, child, access }
            }),
            2 => (0..DRAWERS, 0..DRAWERS).prop_map(|(parent, child)| {
                ModelOp::RemoveChild { parent, child }
            }),
            3 => (0..DRAWERS, any_access()).prop_map(|(drawer, access)| {
                ModelOp::Grant { drawer, access }
            }),
            2 => (0..DRAWERS).prop_map(|drawer| ModelOp::Revoke { drawer }),
            2 => (0..DRAWERS, any::<bool>()).prop_map(|(drawer, mirror)| {
                ModelOp::Remember { drawer, mirror }
            }),
            1 => (0..DRAWERS, any::<bool>()).prop_map(|(drawer, mirror)| {
                ModelOp::SetPolicy { drawer, mirror }
            }),
            1 => (0..DRAWERS).prop_map(|drawer| ModelOp::Forget { drawer }),
            1 => any::<usize>().prop_map(ModelOp::Replay),
            1 => Just(ModelOp::Restart),
        ]
    }

    static CASES: AtomicUsize = AtomicUsize::new(0);

    proptest! {
        #![proptest_config(ProptestConfig { cases: 4, ..ProptestConfig::default() })]

        #[test]
        fn generated_drawer_scripts_match_reference_model_long_test(
            script in prop_vec(any_model_op(), 3..=8),
        ) {
            let case = CASES.fetch_add(1, Ordering::Relaxed);
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("a tokio runtime");
            runtime.block_on(async {
                let mut node = Node::boot().await;
                run_script(&mut node, &format!("gen-{case}"), &script).await;
                node.stop().await;
            });
        }
    }
}
