use super::*;
use crate::authority::Authority;
use big_cabinet_core::authority::RetractCapability;
use big_repo::BigRepo;
use big_repo::BigRepoStopToken;
use big_repo::Config;
use big_repo::StorageConfig;
use secrets_rs::SecretStore;

struct ScopeAccess {
    changes: tokio::sync::watch::Sender<u64>,
    drawers: std::sync::Mutex<BTreeMap<DocId, BTreeSet<DocId>>>,
}

impl ScopeAccess {
    fn set_drawer(&self, drawer: DocId, documents: BTreeSet<DocId>) {
        self.drawers.lock().unwrap().insert(drawer, documents);
        self.changes.send_modify(|revision| *revision += 1);
    }
}

impl DocAccess for ScopeAccess {
    fn prove<'a>(
        &'a self,
        context: &'a RequestContext,
        _actor: Uuid7,
        _scope: &'a DocScope,
    ) -> AccessFuture<'a, Vec<CapId>> {
        Box::pin(async move {
            context.validate().unwrap();
            Ok(Vec::new())
        })
    }

    fn check<'a>(
        &'a self,
        _context: &'a RequestContext,
        _actor: Uuid7,
        _request: AccessRequest<'a>,
    ) -> AccessFuture<'a, bool> {
        panic!("availability must not fall back to identity authorization")
    }

    fn place<'a>(
        &'a self,
        _context: &'a RequestContext,
        _actor: Option<Uuid7>,
        _document: &'a DocId,
        _placement: &'a str,
    ) -> AccessFuture<'a, ()> {
        panic!("availability must not place documents")
    }

    fn scoped_docs<'a>(
        &'a self,
        context: &'a RequestContext,
        drawers: &'a BTreeMap<DocId, big_cabinet_core::drawer::DrawerAccess>,
        documents: Option<&'a BTreeSet<DocId>>,
    ) -> AccessFuture<'a, BTreeMap<DocId, big_cabinet_core::drawer::DrawerAccess>> {
        Box::pin(async move {
            context.validate().unwrap();
            let membership = self.drawers.lock().unwrap();
            let mut allowed = BTreeMap::new();
            for (drawer, held) in drawers {
                if let Some(members) = membership.get(drawer) {
                    for doc in members {
                        if documents.is_none_or(|documents| documents.contains(doc)) {
                            allowed.insert(doc.clone(), *held);
                        }
                    }
                }
            }
            Ok(allowed)
        })
    }

    fn subscribe_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.changes.subscribe())
    }
}

struct Node {
    authority: Authority,
    authority_stop: StopToken,
    docs: DocsRepo,
    access: Arc<ScopeAccess>,
    docs_stop: StopToken,
    repo_stop: BigRepoStopToken,
}

impl Node {
    async fn boot() -> Self {
        let keyring =
            keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
        let secrets = SecretStore::boot_with_store(keyring).await.unwrap();
        let (authority, authority_stop) =
            crate::authority::boot(&secrets, sqlite_rs::test_support::db(&[]).unwrap().clone())
                .await
                .unwrap();
        let namespace = authority
            .service
            .open_namespace(Uuid7::new())
            .await
            .unwrap();
        let (big_repo, repo_stop) = BigRepo::boot(
            Arc::new(sqlite_rs::SqliteRuntime::new(Default::default())),
            Config {
                node_identity_seed: [73; 32],
                storage: StorageConfig::Memory,
                scope_key: "availability-tests".into(),
                hidden_parts: Default::default(),
                automerge_frontier_group_scope: Default::default(),
                causal_checkpoint_group_scope: Default::default(),
                group_part_group_scope: Default::default(),
            },
        )
        .await
        .unwrap();
        let access = Arc::new(ScopeAccess {
            changes: tokio::sync::watch::channel(0).0,
            drawers: Default::default(),
        });
        let (docs, docs_stop) = super::super::boot(DocsConfig {
            big_repo,
            schema_repo: Arc::new(SchemaRepo::open([]).unwrap()),
            authority: authority.client.clone(),
            issuer: authority.root,
            namespace,
            node_issuer: authority.node_issuer.clone(),
            access: Arc::<ScopeAccess>::clone(&access),
            sql: sqlite_rs::test_support::db(&[]).unwrap().clone(),
            facet_set: None,
            facet_ref: None,
        })
        .await
        .unwrap();
        Self {
            authority,
            authority_stop,
            docs,
            access,
            docs_stop,
            repo_stop,
        }
    }

    async fn capability(&self, scope: DocScope) -> CapId {
        self.docs
            .client
            .call(
                RequestContext::root(),
                MintDocCapability {
                    authorization: Authorization::identity(self.authority.root),
                    scope,
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap()
            .capability_id
    }

    async fn query(&self, params: GetDocAvailability) -> Result<DocAvailability, DocsError> {
        self.docs
            .client
            .call(RequestContext::root(), params)
            .await
            .map_err(|error| match error {
                ClientError::Operation(error) => error,
                error => panic!("availability transport failed: {error:?}"),
            })
    }

    async fn observe(
        &self,
        context: RequestContext,
        params: ObserveDocAvailability,
    ) -> Result<EventStream<DocAvailability, DocsError>, DocsError> {
        Handler::call(self.docs.service.as_ref(), Envelope::new(context, params))
            .await
            .map(|response| response.output)
    }

    async fn retract(&self, capability: CapId) {
        self.authority
            .client
            .call(
                RequestContext::root(),
                RetractCapability {
                    via: self.authority.root,
                    capability,
                },
            )
            .await
            .unwrap();
    }

    fn unvalidated_content() -> Automerge {
        let mut raw = Automerge::new();
        let mut transaction = raw.transaction();
        transaction
            .put(automerge::ROOT, "unvalidated", "no facets or schema")
            .unwrap();
        transaction.commit();
        raw
    }

    async fn stop(self) {
        self.docs_stop.stop().await;
        self.repo_stop.stop().await.unwrap();
        self.authority_stop.stop().await;
    }
}

#[test]
fn usability_is_local_not_freshness_or_full_sync() {
    for (state, expected) in [
        (None, LocalDocUsability::NotObserved),
        (
            Some(MaterializationState::Missing),
            LocalDocUsability::NoLocalContent,
        ),
        (
            Some(MaterializationState::Pending),
            LocalDocUsability::PendingMaterialization,
        ),
        (
            Some(MaterializationState::PartiallyMaterialized),
            LocalDocUsability::Usable,
        ),
        (
            Some(MaterializationState::Materialized),
            LocalDocUsability::Usable,
        ),
    ] {
        assert_eq!(local_usability(state), expected);
    }
}

#[tokio::test]
async fn passive_query_neither_creates_workers_nor_acquires_demand() {
    let node = Node::boot().await;
    let doc_id = DocId::new([1; 32]);
    let repo = &node.docs.service.big_repo;
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let params = GetDocAvailability {
        capability,
        doc_id: doc_id.clone(),
    };
    for _ in 0..2 {
        assert_eq!(
            node.query(params.clone()).await,
            Ok(DocAvailability {
                doc_id: doc_id.clone(),
                local_usability: LocalDocUsability::NotObserved,
                sync_demand: false,
            })
        );
        assert_eq!(
            repo.inspect_doc_head_state(doc_id.clone()).await.unwrap(),
            None
        );
        assert!(repo.sync_demand().documents().is_empty());
    }
    let interest = repo.interest(doc_id.clone()).await.unwrap();
    assert_eq!(
        node.query(params).await,
        Ok(DocAvailability {
            doc_id,
            local_usability: LocalDocUsability::NoLocalContent,
            sync_demand: true,
        })
    );
    drop(interest);
    node.stop().await;
}

#[tokio::test]
async fn denied_queries_disclose_neither_known_nor_unknown_documents() {
    let node = Node::boot().await;
    let document = node
        .docs
        .service
        .big_repo
        .create_doc(Node::unvalidated_content())
        .await
        .unwrap();
    let known = document.document_id();
    let unknown = DocId::new([2; 32]);
    let empty = node.capability(DocScope::default()).await;
    for capability in [empty, node.authority.root, CapId::from_bytes([99; 16])] {
        for doc_id in [&known, &unknown] {
            assert_eq!(
                node.query(GetDocAvailability {
                    capability,
                    doc_id: doc_id.clone()
                })
                .await,
                Err(DocsError::Denied)
            );
        }
    }
    let capability = node
        .capability(DocScope {
            write: BTreeSet::from([known.clone()]),
            ..DocScope::default()
        })
        .await;
    assert_eq!(
        node.query(GetDocAvailability {
            capability,
            doc_id: known.clone()
        })
        .await,
        Ok(DocAvailability {
            doc_id: known.clone(),
            local_usability: LocalDocUsability::Usable,
            sync_demand: true,
        })
    );
    node.retract(capability).await;
    assert_eq!(
        node.query(GetDocAvailability {
            capability,
            doc_id: known.clone()
        })
        .await,
        Err(DocsError::Denied)
    );
    assert_eq!(
        node.docs
            .service
            .big_repo
            .inspect_doc_head_state(unknown.clone())
            .await
            .unwrap(),
        None
    );
    drop(document);
    let service = Arc::clone(&node.docs.service);
    node.repo_stop.stop().await.unwrap();
    for doc_id in [known, unknown] {
        assert_eq!(
            service
                .availability(
                    &RequestContext::root(),
                    GetDocAvailability {
                        capability: empty,
                        doc_id
                    }
                )
                .await,
            Err(DocsError::Denied)
        );
    }
    node.docs_stop.stop().await;
    node.authority_stop.stop().await;
}

#[tokio::test]
async fn scoped_stream_acquires_demand_and_reports_materialization_through_binding() {
    let node = Node::boot().await;
    let repo = &node.docs.service.big_repo;
    let doc_id = repo.allocate_id().await.unwrap();
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let mut events = node
        .docs
        .client
        .subscribe(
            RequestContext::root(),
            ObserveDocAvailability {
                capability,
                doc_id: doc_id.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        events.next().await.unwrap().unwrap(),
        DocAvailability {
            doc_id: doc_id.clone(),
            local_usability: LocalDocUsability::NoLocalContent,
            sync_demand: true,
        }
    );
    let document = repo
        .commit_id(doc_id.clone(), Node::unvalidated_content(), Vec::new(), &[])
        .await
        .unwrap();
    assert_eq!(
        events.next().await.unwrap().unwrap(),
        DocAvailability {
            doc_id,
            local_usability: LocalDocUsability::Usable,
            sync_demand: true,
        }
    );
    drop(events);
    drop(document);
    node.stop().await;
}

#[tokio::test]
async fn denied_streams_neither_disclose_content_nor_acquire_interest() {
    let node = Node::boot().await;
    let repo = &node.docs.service.big_repo;
    let document = repo.create_doc(Node::unvalidated_content()).await.unwrap();
    let known = document.document_id();
    let unknown = DocId::new([4; 32]);
    let capability = node.capability(DocScope::default()).await;
    let retracted = node
        .capability(DocScope {
            read: BTreeSet::from([known.clone(), unknown.clone()]),
            ..DocScope::default()
        })
        .await;
    node.retract(retracted).await;
    let demanded = repo.sync_demand().documents();
    for capability in [
        capability,
        retracted,
        node.authority.root,
        CapId::from_bytes([99; 16]),
    ] {
        for doc_id in [&known, &unknown] {
            assert!(matches!(
                node.observe(
                    RequestContext::root(),
                    ObserveDocAvailability {
                        capability,
                        doc_id: doc_id.clone()
                    },
                )
                .await,
                Err(DocsError::Denied)
            ));
            assert_eq!(repo.sync_demand().documents(), demanded);
            assert_eq!(
                repo.inspect_doc_head_state(unknown.clone()).await.unwrap(),
                None
            );
        }
    }
    drop(document);
    node.stop().await;
}

#[tokio::test]
async fn quiet_retraction_closes_even_with_an_unread_initial_snapshot() {
    let node = Node::boot().await;
    let doc_id = node.docs.service.big_repo.allocate_id().await.unwrap();
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let mut events = node
        .observe(
            RequestContext::root(),
            ObserveDocAvailability {
                capability,
                doc_id: doc_id.clone(),
            },
        )
        .await
        .unwrap();
    let repo = &node.docs.service.big_repo;
    let mut demand = repo.sync_demand();
    if !demand.documents().contains(&doc_id) {
        assert!(demand.changed().await.unwrap().contains(&doc_id));
    }
    let document = repo
        .commit_id(doc_id, Node::unvalidated_content(), Vec::new(), &[])
        .await
        .unwrap();
    node.retract(capability).await;
    assert_eq!(events.next().await, None);
    drop(document);
    node.stop().await;
}

#[tokio::test]
async fn quiet_retraction_ends_a_stream_without_repository_changes() {
    let node = Node::boot().await;
    let doc_id = DocId::new([5; 32]);
    let capability = node
        .capability(DocScope {
            write: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let mut events = node
        .observe(
            RequestContext::root(),
            ObserveDocAvailability {
                capability,
                doc_id: doc_id.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        events.next().await.unwrap().unwrap(),
        DocAvailability {
            doc_id,
            local_usability: LocalDocUsability::NoLocalContent,
            sync_demand: true,
        }
    );
    node.retract(capability).await;
    assert_eq!(events.next().await, None);
    node.stop().await;
}

#[tokio::test]
async fn scoped_drawer_membership_loss_ends_availability_quietly() {
    let node = Node::boot().await;
    let drawer_id = DocId::new([6; 32]);
    let doc_id = DocId::new([7; 32]);
    node.access
        .set_drawer(drawer_id.clone(), BTreeSet::from([doc_id.clone()]));
    let capability = node
        .capability(DocScope {
            drawers: BTreeMap::from([(
                drawer_id.clone(),
                big_cabinet_core::drawer::DrawerAccess::Read,
            )]),
            ..DocScope::default()
        })
        .await;
    let mut events = node
        .observe(
            RequestContext::root(),
            ObserveDocAvailability {
                capability,
                doc_id: doc_id.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        events.next().await.unwrap().unwrap(),
        DocAvailability {
            doc_id,
            local_usability: LocalDocUsability::NoLocalContent,
            sync_demand: true,
        }
    );
    node.access.set_drawer(drawer_id, BTreeSet::new());
    assert_eq!(events.next().await, None);
    node.stop().await;
}

#[tokio::test]
async fn stream_context_cancellation_and_service_stop_are_quiet() {
    let node = Node::boot().await;
    let doc_id = DocId::new([8; 32]);
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let expected = DocAvailability {
        doc_id: doc_id.clone(),
        local_usability: LocalDocUsability::NoLocalContent,
        sync_demand: true,
    };
    let params = ObserveDocAvailability { capability, doc_id };
    let context = RequestContext::root();
    let mut unread_cancelled = node.observe(context.clone(), params.clone()).await.unwrap();
    let mut cancelled = node.observe(context.clone(), params.clone()).await.unwrap();
    assert_eq!(cancelled.next().await.unwrap().unwrap(), expected);
    context.cancel();
    assert_eq!(cancelled.next().await, None);
    assert_eq!(unread_cancelled.next().await, None);
    let mut unread_stopped = node
        .observe(RequestContext::root(), params.clone())
        .await
        .unwrap();
    let mut stopped = node
        .observe(RequestContext::root(), params.clone())
        .await
        .unwrap();
    assert_eq!(stopped.next().await.unwrap().unwrap(), expected);
    let (release, blocked) = tokio::sync::oneshot::channel();
    let blocker = node
        .docs
        .service
        .tasks
        .spawn(async move { blocked.await.unwrap() })
        .unwrap();
    let mut stopping = Box::pin(node.docs_stop.stop());
    assert!(futures::poll!(&mut stopping).is_pending());
    assert!(node.docs.service.tasks.is_cancelled());
    assert_eq!(futures::poll!(stopped.next()), std::task::Poll::Ready(None));
    assert_eq!(
        futures::poll!(unread_stopped.next()),
        std::task::Poll::Ready(None)
    );
    release.send(()).unwrap();
    stopping.await;
    blocker.await.unwrap();
    assert_eq!(node.docs.service.tasks.live_tasks(), 0);
    assert!(matches!(
        Handler::call(
            node.docs.service.as_ref(),
            Envelope::new(RequestContext::root(), params),
        )
        .await,
        Err(DocsError::Context(ContextError::Cancelled))
    ));
    node.repo_stop.stop().await.unwrap();
    node.authority_stop.stop().await;
}

#[tokio::test]
async fn dropping_a_consumer_releases_its_observer_task() {
    let node = Node::boot().await;
    let doc_id = DocId::new([9; 32]);
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let mut events = node
        .observe(
            RequestContext::root(),
            ObserveDocAvailability { capability, doc_id },
        )
        .await
        .unwrap();
    assert!(events.next().await.unwrap().is_ok());
    assert_eq!(node.docs.service.tasks.live_tasks(), 1);
    drop(events);
    while node.docs.service.tasks.live_tasks() != 0 {
        tokio::task::yield_now().await;
    }
    node.stop().await;
}

#[tokio::test]
async fn repository_failure_is_reported_once_and_ends_observation() {
    let node = Node::boot().await;
    let doc_id = DocId::new([10; 32]);
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    node.repo_stop.stop().await.unwrap();
    assert!(matches!(
        Handler::call(
            node.docs.service.as_ref(),
            Envelope::new(
                RequestContext::root(),
                GetDocAvailability {
                    capability,
                    doc_id: doc_id.clone(),
                },
            ),
        )
        .await,
        Err(DocsError::Repository(_))
    ));
    let mut events = Handler::call(
        node.docs.service.as_ref(),
        Envelope::new(
            RequestContext::root(),
            ObserveDocAvailability { capability, doc_id },
        ),
    )
    .await
    .unwrap()
    .output;
    assert!(matches!(
        events.next().await,
        Some(Err(DocsError::Repository(_)))
    ));
    assert_eq!(events.next().await, None);
    node.docs_stop.stop().await;
    node.authority_stop.stop().await;
}

#[tokio::test]
async fn expired_contexts_are_rejected_before_repository_observation() {
    let node = Node::boot().await;
    let doc_id = DocId::new([11; 32]);
    let capability = node
        .capability(DocScope {
            read: BTreeSet::from([doc_id.clone()]),
            ..DocScope::default()
        })
        .await;
    let context = RequestContext::root().with_time_budget(std::time::Duration::ZERO);
    assert!(matches!(
        Handler::call(
            node.docs.service.as_ref(),
            Envelope::new(
                context.clone(),
                GetDocAvailability {
                    capability,
                    doc_id: doc_id.clone(),
                },
            ),
        )
        .await,
        Err(DocsError::Context(ContextError::DeadlineExceeded))
    ));
    assert!(matches!(
        node.observe(
            context,
            ObserveDocAvailability {
                capability,
                doc_id: doc_id.clone(),
            },
        )
        .await,
        Err(DocsError::Context(ContextError::DeadlineExceeded))
    ));
    assert_eq!(
        node.docs
            .service
            .big_repo
            .inspect_doc_head_state(doc_id)
            .await
            .unwrap(),
        None
    );
    assert!(
        node.docs
            .service
            .big_repo
            .sync_demand()
            .documents()
            .is_empty()
    );
    assert_eq!(node.docs.service.tasks.live_tasks(), 0);
    node.stop().await;
}

#[tokio::test]
async fn handler_honors_context_cancellation_and_service_stop() {
    let node = Node::boot().await;
    let params = GetDocAvailability {
        capability: node.authority.root,
        doc_id: DocId::new([3; 32]),
    };
    let context = RequestContext::root();
    context.cancel();
    assert!(matches!(
        Handler::call(
            node.docs.service.as_ref(),
            Envelope::new(context, params.clone())
        )
        .await,
        Err(DocsError::Context(ContextError::Cancelled))
    ));
    node.docs_stop.stop().await;
    assert!(matches!(
        Handler::call(
            node.docs.service.as_ref(),
            Envelope::new(RequestContext::root(), params)
        )
        .await,
        Err(DocsError::Context(ContextError::Cancelled))
    ));
    node.repo_stop.stop().await.unwrap();
    node.authority_stop.stop().await;
}
