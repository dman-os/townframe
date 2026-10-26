//! Drawer conformance, end to end through the hosted clients: an authority, a BigRepo, a DocsRepo
//! booted over [`DrawerAccess`], and the DrawerService on top, the way an embedder assembles them.

use super::*;
use crate::authority::{Authority, OpenActor};
use crate::doc::{
    At, CreateDoc, DocsConfig, DocsRepo, FacetObserved, FacetRead, FacetRefSelector, FacetSelector,
    ObserveFacets, ObserveScope, ObservedFacet, QueriedFacetRef, QueryFacetRefs, QueryFacets,
    ReadFacets, WriteFacets,
};
use crate::index::{FacetMembership, FacetRefQuery, FacetRefStore, FacetRoute, FacetSetStore};
use crate::schema::SchemaRepo;
use automerge::ChangeHash;
use big_cabinet_core::FacetTag;
use big_cabinet_core::drawer::{Drawer, LayoutKind};
use big_cabinet_core::facet::{Bundle, FacetDecl, Universe};
use big_repo::{BigRepo, BigRepoStopToken, Config, StorageConfig};
use big_sync_core::keyed_frontier::FrontierEntry;
use big_sync_core::revisioned_store::{RevisionRead, RevisionReadLimits, RevisionedStoreReader};
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
    drawer_worker: Option<DrawerWorker>,
    drawer_worker_stop: Option<StopToken>,
    drawer_policy_stop: Option<StopToken>,
}

impl Node {
    async fn boot() -> Self {
        Self::boot_with_seed(43).await
    }

    async fn boot_with_seed(seed: u8) -> Self {
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
            node_identity_seed: [seed; 32],
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
            drawer_worker: None,
            drawer_worker_stop: None,
            drawer_policy_stop: None,
        };
        node.start().await.unwrap();
        node
    }

    /// The embedder's assembly order: the policy first, DocsRepo over it, the drawers on top.
    async fn start(&mut self) -> Result<(), DrawersError> {
        let (access, provider, policy_stop) = DrawerAccess::open(
            Arc::clone(&self.repo),
            self.authority.client.clone(),
            self.drawers_actor.capability,
            self.drawers_namespace,
            self.sql.clone(),
            Arc::clone(&self.facet_set),
        )
        .await?;
        self.drawer_policy_stop = Some(policy_stop);
        let prepared = crate::doc::prepare(DocsConfig {
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
        .await;
        let (docs, docs_stop) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.stop_services().await;
                return Err(DrawersError::Docs(error));
            }
        };
        let (worker, worker_stop) = provider.start(docs.client.clone());
        self.drawer_worker = Some(worker);
        self.drawer_worker_stop = Some(worker_stop);
        self.docs = Some(docs);
        self.docs_stop = Some(docs_stop);
        if let Err(error) = self.docs.as_ref().unwrap().recover().await {
            self.stop_services().await;
            return Err(DrawersError::Docs(error));
        }
        match boot(DrawerConfig {
            worker: self.drawer_worker.as_ref().unwrap(),
        })
        .await
        {
            Ok((drawers, drawers_stop)) => {
                self.drawers = Some(drawers);
                self.drawers_stop = Some(drawers_stop);
                Ok(())
            }
            Err(error) => {
                self.stop_services().await;
                Err(error)
            }
        }
    }

    async fn stop_services(&mut self) {
        if let Some(stop) = self.drawers_stop.take() {
            stop.stop().await;
        }
        self.drawers.take();
        if let Some(stop) = self.drawer_worker_stop.take() {
            stop.stop().await;
        }
        self.drawer_worker.take();
        if let Some(stop) = self.docs_stop.take() {
            stop.stop().await;
        }
        self.docs.take();
        if let Some(stop) = self.drawer_policy_stop.take() {
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

    /// Follow this node's `facet_set` frontier for `tag`, replay first and then live, until an
    /// entry satisfies `indexed`.
    async fn wait_indexed(
        &self,
        tag: &str,
        indexed: impl Fn(&FacetRoute, &FacetMembership) -> bool,
    ) {
        let mut reader = self
            .facet_set
            .open_tags([(FacetTag::new(tag).unwrap(), 0)])
            .await
            .unwrap();
        loop {
            if let RevisionRead::Entries { entries, .. } =
                reader.next(RevisionReadLimits::default()).await.unwrap()
                && entries.iter().any(|FrontierEntry { key, value, .. }| {
                    value
                        .as_ref()
                        .is_some_and(|membership| indexed(key, membership))
                })
            {
                return;
            }
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

    /// Make the next operation reaching `point` fail there, as a process dying would.
    fn arm_crash(&self, point: CrashPoint) {
        let armed = self.core().crash.lock().unwrap().replace(point);
        assert_eq!(armed, None, "a previous crash was never reached");
    }

    async fn invite(
        &self,
        actor: &Actor,
        drawer: &DrawerId,
        principal: Principal,
        access: Access,
    ) -> Result<(), DrawersError> {
        self.drawers()
            .call(
                RequestContext::root(),
                InvitePrincipal {
                    capability: self.drawer_capability(actor, &[drawer]).await?,
                    drawer_id: drawer.clone(),
                    principal,
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

mod delegation;
mod lifecycle;
mod membership;
mod model;
mod peer;
mod queries;
