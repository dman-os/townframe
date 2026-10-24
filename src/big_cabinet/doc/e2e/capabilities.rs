use super::*;
use crate::authority::{AttenuateCapability, InspectCapability, MintCapability, RetractCapability};
use futures::StreamExt;

async fn actor(node: &Node, path: &str, rights: Rights) -> Actor {
    node.authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: node.authority.root,
                path: path.into(),
                rights,
            },
        )
        .await
        .unwrap()
}

async fn mint(
    node: &Node,
    authorization: Authorization,
    scope: DocScope,
    rights: Rights,
) -> Capability {
    node.client()
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization,
                scope,
                rights,
            },
        )
        .await
        .unwrap()
}

async fn next_facet(
    events: &mut (impl futures::Stream<Item = Result<FacetObserved, ClientError<DocsError>>> + Unpin),
) -> (DocId, FacetKey, ObservedFacet) {
    loop {
        match events.next().await.unwrap().unwrap() {
            FacetObserved::Entry {
                doc_id,
                facet_key,
                facet,
                ..
            } => return (doc_id, facet_key, facet),
            FacetObserved::ReplayComplete { .. } => {}
        }
    }
}

#[tokio::test]
async fn bound_signer_survives_attenuation_and_only_domain_mint_rebinds() {
    let node = Node::boot().await;
    let owner = node.writer("owner").await;
    let signer = actor(&node, "plugin", Rights::USE).await;
    let created = node
        .create(&owner, "create", [(key("main"), note("before"))])
        .await
        .unwrap();
    let other = node
        .create(&owner, "other", [(key("main"), note("private"))])
        .await
        .unwrap();
    let scope = DocScope {
        write: BTreeSet::from([created.doc_id.clone()]),
        ..DocScope::default()
    };
    let bound = mint(
        &node,
        Authorization {
            authority: owner.capability,
            signer: signer.capability,
        },
        scope.clone(),
        Rights::USE | Rights::NARROW,
    )
    .await;
    assert_eq!(bound.issuer_id, node.docs_namespace.namespace_id);
    assert_ne!(bound.issuer_id, signer.actor_id);
    assert_ne!(bound.issuer_id, owner.actor_id);
    assert_ne!(bound.issuer_id, node.docs_actor.actor_id);
    let object: Value =
        serde_json::from_str(bound.object.strip_prefix("DocsRepo/scope/v2/").unwrap()).unwrap();
    let binding_id: Uuid7 = serde_json::from_value(object["bindingId"].clone()).unwrap();
    assert_eq!(
        object,
        json!({
            "scope": scope,
            "bindingId": binding_id,
            "originatingActor": owner.actor_id,
        })
    );
    let attenuated = node
        .authority
        .client
        .call(
            RequestContext::root(),
            AttenuateCapability {
                capability: bound.capability_id,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    assert_eq!(attenuated.object, bound.object);
    let rebound = mint(
        &node,
        Authorization {
            authority: bound.capability_id,
            signer: owner.capability,
        },
        scope,
        Rights::USE,
    )
    .await;
    for (index, capability, expected_signer) in [
        (0, bound.capability_id, &signer),
        (1, attenuated.capability_id, &signer),
        (2, rebound.capability_id, &owner),
        (3, bound.capability_id, &signer),
    ] {
        node.client()
            .call(
                RequestContext::root(),
                WriteFacets {
                    capability,
                    command: None,
                    doc_id: created.doc_id.clone(),
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::from([(key("main"), note(&format!("write {index}")))]),
                    remove: BTreeSet::new(),
                },
            )
            .await
            .unwrap();
        let read = node
            .client()
            .call(
                RequestContext::root(),
                ReadFacets {
                    capability,
                    doc_id: created.doc_id.clone(),
                    at: At::Current,
                    facets: Some(BTreeSet::from([key("main")])),
                    optimistic: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            accepted(&read.facets[&key("main")]).0,
            &note(&format!("write {index}"))
        );
        let evidence = node.claims(&created.doc_id, &key("main")).await;
        assert_eq!(evidence.len(), 1);
        match &evidence[0].result {
            SignatureEvidence::VerifiedSignature { evidence } => {
                assert_eq!(evidence.claim.actor_uuid, expected_signer.actor_id);
                assert_eq!(evidence.actor_public_key, expected_signer.public_key);
            }
            result => panic!("expected verified bound attribution, got {result:?}"),
        }
    }
    assert!(
        !node
            .access
            .check(
                &RequestContext::root(),
                signer.actor_id,
                AccessRequest::Write(&created.doc_id),
            )
            .await
            .unwrap()
    );
    assert_eq!(
        op(node
            .client()
            .call(
                RequestContext::root(),
                ReadFacets {
                    capability: bound.capability_id,
                    doc_id: other.doc_id.clone(),
                    at: At::Current,
                    facets: None,
                    optimistic: false,
                }
            )
            .await),
        Err(DocsError::Denied)
    );
    for source in [bound.capability_id, attenuated.capability_id] {
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    MintDocCapability {
                        authorization: Authorization {
                            authority: source,
                            signer: owner.capability
                        },
                        scope: DocScope {
                            read: BTreeSet::from([other.doc_id.clone()]),
                            ..DocScope::default()
                        },
                        rights: Rights::USE,
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
    }
    let narrowed = mint(
        &node,
        Authorization {
            authority: bound.capability_id,
            signer: owner.capability,
        },
        DocScope {
            read: BTreeSet::from([created.doc_id.clone()]),
            ..DocScope::default()
        },
        Rights::USE,
    )
    .await;
    assert_eq!(
        op(node
            .client()
            .call(
                RequestContext::root(),
                WriteFacets {
                    capability: narrowed.capability_id,
                    command: None,
                    doc_id: created.doc_id,
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::from([(key("main"), note("not authorized"))]),
                    remove: BTreeSet::new(),
                }
            )
            .await),
        Err(DocsError::Denied)
    );
    node.stop().await;
}

#[tokio::test]
async fn resource_requests_refuse_identity_unbound_and_wrong_namespace_without_fallback() {
    let node = Node::boot().await;
    let owner = node.writer("owner").await;
    let created = node
        .create(&owner, "create", [(key("main"), note("private"))])
        .await
        .unwrap();
    let bound = mint(
        &node,
        Authorization::identity(owner.capability),
        DocScope {
            write: BTreeSet::from([created.doc_id.clone()]),
            create: BTreeSet::from([PLACEMENT.into()]),
            ..DocScope::default()
        },
        Rights::USE | Rights::NARROW,
    )
    .await;
    let no_use = node
        .authority
        .client
        .call(
            RequestContext::root(),
            AttenuateCapability {
                capability: bound.capability_id,
                rights: Rights::empty(),
            },
        )
        .await
        .unwrap();
    let identity_child = node
        .authority
        .client
        .call(
            RequestContext::root(),
            AttenuateCapability {
                capability: owner.capability,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let unbound = node
        .authority
        .client
        .call(
            RequestContext::root(),
            MintCapability {
                issuer: node.docs_namespace.capability,
                parent: None,
                dependencies: Vec::new(),
                object: format!(
                    "DocsRepo/scope/v2/{}",
                    serde_json::to_string(&DocScope {
                        write: BTreeSet::from([created.doc_id.clone()]),
                        create: BTreeSet::from([PLACEMENT.into()]),
                        ..DocScope::default()
                    })
                    .unwrap()
                ),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let namespace = node
        .authority
        .service
        .open_namespace(owner.actor_id)
        .await
        .unwrap();
    let wrong = node
        .authority
        .client
        .call(
            RequestContext::root(),
            MintCapability {
                issuer: namespace.capability,
                parent: None,
                dependencies: Vec::new(),
                object: bound.object,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    for capability in [
        owner.capability,
        identity_child.capability_id,
        wrong.capability_id,
        unbound.capability_id,
        no_use.capability_id,
        CapId::default(),
    ] {
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    ReadFacets {
                        capability,
                        doc_id: created.doc_id.clone(),
                        at: At::Current,
                        facets: None,
                        optimistic: false,
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    ListFacets {
                        capability,
                        doc_id: created.doc_id.clone(),
                        at: At::Current,
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    WriteFacets {
                        capability,
                        command: None,
                        doc_id: created.doc_id.clone(),
                        mode: WriteMode::AtCurrent,
                        set: BTreeMap::new(),
                        remove: BTreeSet::new(),
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    CreateDoc {
                        capability,
                        command: "denied-create".to_owned().try_into().unwrap(),
                        placement: PLACEMENT.into(),
                        facets: BTreeMap::new(),
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    BranchDoc {
                        capability,
                        command: "denied-branch".to_owned().try_into().unwrap(),
                        doc_id: created.doc_id.clone(),
                        at: At::Current,
                        path: "/denied".into(),
                        stealth: false,
                        placement: PLACEMENT.into(),
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    MergeDoc {
                        capability,
                        command: None,
                        source_id: created.doc_id.clone(),
                        at: At::Current,
                        target_id: created.doc_id.clone(),
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    QueryFacets {
                        capability,
                        drawer_id: None,
                        selector: FacetSelector::Doc {
                            doc_id: created.doc_id.clone()
                        },
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    QueryFacetRefs {
                        capability,
                        drawer_id: None,
                        selector: FacetRefSelector::Origin {
                            doc_id: created.doc_id.clone(),
                            facet_key: None
                        },
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        assert!(matches!(
            node.client()
                .subscribe(
                    RequestContext::root(),
                    ObserveFacets {
                        capability,
                        scope: ObserveScope::Doc {
                            doc_id: created.doc_id.clone()
                        },
                        after: 0,
                    }
                )
                .await,
            Err(ClientError::Operation(DocsError::Denied))
        ));
    }
    node.stop().await;
}

#[tokio::test]
async fn creation_and_recovery_grant_originating_actor_not_bound_signer() {
    use super::super::commands::{Commands, Detail, Kind, State};

    struct InterruptedPlacement {
        ledger: Arc<LedgerAccess>,
        entered: tokio::sync::mpsc::UnboundedSender<(Option<Uuid7>, DocId, String)>,
    }

    impl DocAccess for InterruptedPlacement {
        fn prove<'a>(
            &'a self,
            context: &'a RequestContext,
            actor: Uuid7,
            scope: &'a DocScope,
        ) -> AccessFuture<'a, Vec<CapId>> {
            self.ledger.prove(context, actor, scope)
        }

        fn check<'a>(
            &'a self,
            context: &'a RequestContext,
            actor: Uuid7,
            request: AccessRequest<'a>,
        ) -> AccessFuture<'a, bool> {
            self.ledger.check(context, actor, request)
        }

        fn place<'a>(
            &'a self,
            _context: &'a RequestContext,
            actor: Option<Uuid7>,
            document: &'a DocId,
            placement: &'a str,
        ) -> AccessFuture<'a, ()> {
            Box::pin(async move {
                self.entered
                    .send((actor, document.clone(), placement.to_owned()))
                    .unwrap();
                std::future::pending().await
            })
        }
    }

    let mut node = Node::boot().await;
    let owner = node.writer("owner").await;
    let signer = actor(&node, "plugin", Rights::USE).await;
    let origin = node
        .create(&owner, "origin", [(key("main"), note("content"))])
        .await
        .unwrap();
    node.docs.take();
    node.docs_stop.take().unwrap().stop().await;
    node.reopen_namespace().await;
    let (entered, mut placements) = tokio::sync::mpsc::unbounded_channel();
    let access = Arc::new(InterruptedPlacement {
        ledger: Arc::clone(&node.access),
        entered,
    });
    let (docs, stop) = boot(DocsConfig {
        big_repo: Arc::clone(&node.repo),
        schema_repo: schemas(),
        authority: node.authority.client.clone(),
        issuer: node.docs_actor.capability,
        namespace: node.docs_namespace,
        node_issuer: node.authority.node_issuer.clone(),
        access,
        sql: node.sql.clone(),
        facet_set: Some(Arc::clone(&node.facet_set)),
        facet_ref: None,
    })
    .await
    .unwrap();
    node.docs = Some(docs);
    node.docs_stop = Some(stop);
    let source = mint(
        &node,
        Authorization::identity(owner.capability),
        DocScope {
            create: BTreeSet::from([PLACEMENT.into()]),
            ..DocScope::default()
        },
        Rights::USE | Rights::NARROW,
    )
    .await;
    let bound = mint(
        &node,
        Authorization {
            authority: source.capability_id,
            signer: signer.capability,
        },
        DocScope {
            create: BTreeSet::from([PLACEMENT.into()]),
            ..DocScope::default()
        },
        Rights::USE | Rights::NARROW,
    )
    .await;
    let attenuated = node
        .authority
        .client
        .call(
            RequestContext::root(),
            AttenuateCapability {
                capability: bound.capability_id,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let mut create = CreateDoc {
        capability: attenuated.capability_id,
        command: "bound-create".to_owned().try_into().unwrap(),
        placement: PLACEMENT.into(),
        facets: BTreeMap::from([(key("main"), note("content"))]),
    };
    let branch_scope = DocScope {
        write: BTreeSet::from([origin.doc_id.clone()]),
        create: BTreeSet::from([PLACEMENT.into()]),
        ..DocScope::default()
    };
    let branch_cap = mint(
        &node,
        Authorization {
            authority: owner.capability,
            signer: signer.capability,
        },
        branch_scope.clone(),
        Rights::USE,
    )
    .await;
    let mut branch = BranchDoc {
        capability: branch_cap.capability_id,
        command: "bound-branch".to_owned().try_into().unwrap(),
        doc_id: origin.doc_id.clone(),
        at: At::Current,
        path: "/plugin".into(),
        stealth: false,
        placement: PLACEMENT.into(),
    };
    let creating = {
        let client = node.client().clone();
        let request = create.clone();
        tokio::spawn(async move { client.call(RequestContext::root(), request).await })
    };
    let branching = {
        let client = node.client().clone();
        let request = branch.clone();
        tokio::spawn(async move { client.call(RequestContext::root(), request).await })
    };
    let entered = [
        placements.recv().await.unwrap(),
        placements.recv().await.unwrap(),
    ];
    let ledger = Commands::open(node.sql.clone()).await.unwrap();
    let rows = [
        ledger.get(&create.command).await.unwrap().unwrap(),
        ledger.get(&branch.command).await.unwrap().unwrap(),
    ];
    for (row, kind, detail) in [
        (
            &rows[0],
            Kind::Create,
            Detail::Create {
                owner_id: Some(owner.actor_id),
            },
        ),
        (
            &rows[1],
            Kind::Branch,
            Detail::Branch {
                origin: origin.doc_id.clone(),
                path: branch.path.clone(),
                stealth: branch.stealth,
                owner_id: Some(owner.actor_id),
            },
        ),
    ] {
        assert_eq!(
            (row.kind, row.actor, row.state, row.detail.as_ref()),
            (kind, signer.actor_id, State::Committed, Some(&detail))
        );
        assert_eq!(row.placement.as_deref(), Some(PLACEMENT));
        assert!(!row.heads.is_empty());
    }
    let receipts = rows.each_ref().map(|row| WriteReceipt {
        doc_id: row.document.clone().unwrap(),
        heads: row.heads.clone(),
    });
    assert_ne!(receipts[0].doc_id, receipts[1].doc_id);
    assert_eq!(
        entered.into_iter().collect::<BTreeSet<_>>(),
        receipts
            .iter()
            .map(|receipt| {
                (
                    Some(owner.actor_id),
                    receipt.doc_id.clone(),
                    PLACEMENT.to_owned(),
                )
            })
            .collect::<BTreeSet<_>>()
    );
    for receipt in &receipts {
        for request in [
            AccessRequest::Read(&receipt.doc_id),
            AccessRequest::Write(&receipt.doc_id),
        ] {
            for subject in [&owner, &signer] {
                assert!(
                    !node
                        .access
                        .check(&RequestContext::root(), subject.actor_id, request)
                        .await
                        .unwrap()
                );
            }
        }
    }
    assert_eq!(
        op(node
            .client()
            .call(RequestContext::root(), create.clone())
            .await),
        Err(DocsError::CommandInFlight {
            command: create.command.clone(),
        })
    );
    assert_eq!(
        op(node
            .client()
            .call(RequestContext::root(), branch.clone())
            .await),
        Err(DocsError::CommandInFlight {
            command: branch.command.clone(),
        })
    );
    let other_owner = node.writer("other-owner").await;
    for authorization in [
        Authorization::identity(owner.capability),
        Authorization {
            authority: other_owner.capability,
            signer: signer.capability,
        },
    ] {
        let different = mint(
            &node,
            authorization,
            DocScope {
                create: BTreeSet::from([PLACEMENT.into()]),
                ..DocScope::default()
            },
            Rights::USE,
        )
        .await;
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    CreateDoc {
                        capability: different.capability_id,
                        ..create.clone()
                    },
                )
                .await),
            Err(DocsError::CommandReused {
                command: create.command.clone(),
            })
        );
    }
    assert!(!creating.is_finished());
    assert!(!branching.is_finished());
    node.restart().await.unwrap();
    for result in [creating.await.unwrap(), branching.await.unwrap()] {
        assert_eq!(op(result), Err(DocsError::Context(ContextError::Cancelled)));
    }
    assert_eq!(placements.recv().await, None);
    assert!(ledger.unresolved().await.unwrap().is_empty());
    for row in &rows {
        let recovered = ledger.get(&row.command).await.unwrap().unwrap();
        assert_eq!(
            (
                recovered.kind,
                recovered.digest,
                recovered.actor,
                recovered.document,
                recovered.placement,
                recovered.detail,
                recovered.state,
                recovered.heads,
            ),
            (
                row.kind,
                row.digest,
                signer.actor_id,
                row.document.clone(),
                row.placement.clone(),
                row.detail.clone(),
                State::Done,
                row.heads.clone(),
            )
        );
    }
    let refreshed = mint(
        &node,
        Authorization {
            authority: owner.capability,
            signer: signer.capability,
        },
        branch_scope,
        Rights::USE,
    )
    .await;
    create.capability = refreshed.capability_id;
    branch.capability = refreshed.capability_id;
    assert_eq!(
        node.client()
            .call(RequestContext::root(), create)
            .await
            .unwrap(),
        receipts[0]
    );
    assert_eq!(
        node.client()
            .call(RequestContext::root(), branch)
            .await
            .unwrap(),
        receipts[1]
    );
    let reopened = LedgerAccess::open(
        node.sql.clone(),
        node.authority.client.clone(),
        node.docs_namespace,
    )
    .await
    .unwrap();
    for receipt in &receipts {
        assert!(
            matches!(node.read_one(&owner, &receipt.doc_id, At::Current, &key("main")).await, FacetRead::Accepted { value, .. } if value == note("content"))
        );
        for request in [
            AccessRequest::Read(&receipt.doc_id),
            AccessRequest::Write(&receipt.doc_id),
        ] {
            for (subject, expected) in [(&owner, true), (&signer, false), (&other_owner, false)] {
                assert_eq!(
                    reopened
                        .check(&RequestContext::root(), subject.actor_id, request)
                        .await
                        .unwrap(),
                    expected
                );
            }
        }
    }
    node.stop().await;
}

#[tokio::test]
async fn identity_mint_proves_the_whole_scope_once_and_retains_every_grant_dependency() {
    struct Policy {
        ledger: Arc<LedgerAccess>,
        proofs: std::sync::Mutex<Vec<(Uuid7, DocScope)>>,
    }

    impl DocAccess for Policy {
        fn prove<'a>(
            &'a self,
            context: &'a RequestContext,
            actor: Uuid7,
            scope: &'a DocScope,
        ) -> AccessFuture<'a, Vec<CapId>> {
            self.proofs.lock().unwrap().push((actor, scope.clone()));
            self.ledger.prove(context, actor, scope)
        }

        fn check<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Uuid7,
            _request: AccessRequest<'a>,
        ) -> AccessFuture<'a, bool> {
            panic!("mint and scoped invocation must not perform identity checks")
        }

        fn place<'a>(
            &'a self,
            context: &'a RequestContext,
            actor: Option<Uuid7>,
            document: &'a DocId,
            placement: &'a str,
        ) -> AccessFuture<'a, ()> {
            self.ledger.place(context, actor, document, placement)
        }
    }

    let mut node = Node::boot().await;
    let owner = actor(&node, "owner", Rights::USE | Rights::NARROW).await;
    let signer = actor(&node, "plugin", Rights::USE).await;
    let read_doc = DocId::new([71; 32]);
    let write_doc = DocId::new([72; 32]);
    let scope = DocScope {
        read: BTreeSet::from([read_doc.clone()]),
        write: BTreeSet::from([write_doc.clone()]),
        create: BTreeSet::from([PLACEMENT.into()]),
        ..DocScope::default()
    };
    let objects = [
        LedgerAccess::object(AccessRequest::Read(&read_doc)),
        LedgerAccess::object(AccessRequest::Read(&write_doc)),
        LedgerAccess::object(AccessRequest::Write(&write_doc)),
        LedgerAccess::object(AccessRequest::Create {
            placement: PLACEMENT,
        }),
    ];
    for object in &objects {
        node.grant(&owner, object.clone()).await;
    }
    node.docs.take();
    node.docs_stop.take().unwrap().stop().await;
    node.reopen_namespace().await;
    let context = RequestContext::root();
    let mut dependencies: Vec<_> = node
        .access
        .grants
        .matching(&context, owner.actor_id, &objects)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|proof| proof.dependencies)
        .collect();
    dependencies.sort_unstable_by_key(|capability| *capability.as_bytes());
    dependencies.dedup();
    assert_eq!(dependencies.len(), objects.len());
    assert_eq!(
        node.access
            .prove(&context, owner.actor_id, &scope)
            .await
            .unwrap(),
        dependencies
    );
    let mut overlapping = scope.clone();
    overlapping.read.insert(write_doc.clone());
    assert_eq!(
        node.access
            .prove(&context, owner.actor_id, &overlapping)
            .await
            .unwrap(),
        dependencies
    );
    let policy = Arc::new(Policy {
        ledger: Arc::clone(&node.access),
        proofs: Default::default(),
    });
    let (docs, stop) = boot(DocsConfig {
        big_repo: Arc::clone(&node.repo),
        schema_repo: schemas(),
        authority: node.authority.client.clone(),
        issuer: node.docs_actor.capability,
        namespace: node.docs_namespace,
        node_issuer: node.authority.node_issuer.clone(),
        access: Arc::<Policy>::clone(&policy),
        sql: node.sql.clone(),
        facet_set: Some(Arc::clone(&node.facet_set)),
        facet_ref: None,
    })
    .await
    .unwrap();
    node.docs = Some(docs);
    node.docs_stop = Some(stop);
    let authorization = Authorization {
        authority: owner.capability,
        signer: signer.capability,
    };
    for object in &objects {
        let issued = mint(
            &node,
            authorization,
            scope.clone(),
            Rights::USE | Rights::NARROW,
        )
        .await;
        let narrowed = mint(
            &node,
            Authorization {
                authority: issued.capability_id,
                signer: signer.capability,
            },
            DocScope {
                read: BTreeSet::from([write_doc.clone()]),
                ..DocScope::default()
            },
            Rights::USE,
        )
        .await;
        node.access
            .grants
            .revoke(&context, owner.actor_id, std::slice::from_ref(object))
            .await
            .unwrap();
        for capability in [issued.capability_id, narrowed.capability_id] {
            assert_eq!(
                op(node
                    .client()
                    .call(
                        context.child(),
                        ReadFacets {
                            capability,
                            doc_id: write_doc.clone(),
                            at: At::Current,
                            facets: None,
                            optimistic: false,
                        },
                    )
                    .await),
                Err(DocsError::Denied)
            );
        }
        for rights in [Rights::empty(), Rights::NARROW] {
            node.access
                .grants
                .grant(&context, owner.actor_id, object.clone(), rights, None)
                .await
                .unwrap();
            assert_eq!(
                op(node
                    .client()
                    .call(
                        context.child(),
                        MintDocCapability {
                            authorization,
                            scope: scope.clone(),
                            rights: Rights::USE,
                        },
                    )
                    .await),
                Err(DocsError::Denied)
            );
        }
        node.grant(&owner, object.clone()).await;
    }
    let mut unsupported = scope.clone();
    unsupported.drawers.insert(
        DocId::new([73; 32]),
        big_cabinet_core::drawer::DrawerAccess::Read,
    );
    assert_eq!(
        node.capability(&owner, unsupported.clone()).await,
        Err(DocsError::Denied)
    );
    let mut expected = vec![(owner.actor_id, scope); objects.len() * 3];
    expected.push((owner.actor_id, unsupported));
    assert_eq!(*policy.proofs.lock().unwrap(), expected);
    node.stop().await;
}

#[tokio::test]
async fn mint_requires_narrow_bounded_rights_and_current_source_entitlements() {
    let node = Node::boot().await;
    let owner = node.writer("owner").await;
    let created = node
        .create(&owner, "create", [(key("main"), note("unchanged"))])
        .await
        .unwrap();
    let scope = DocScope {
        read: BTreeSet::from([created.doc_id.clone()]),
        ..DocScope::default()
    };
    let use_only = actor(&node, "use-only", Rights::USE).await;
    node.grant(
        &use_only,
        LedgerAccess::object(AccessRequest::Read(&created.doc_id)),
    )
    .await;
    for (source, rights) in [(&use_only, Rights::USE), (&owner, Rights::all())] {
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    MintDocCapability {
                        authorization: Authorization::identity(source.capability),
                        scope: scope.clone(),
                        rights,
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
    }
    let old_capability = node.capability(&owner, scope.clone()).await.unwrap();
    let object = LedgerAccess::object(AccessRequest::Read(&created.doc_id));
    node.access
        .grants
        .revoke(
            &RequestContext::root(),
            owner.actor_id,
            std::slice::from_ref(&object),
        )
        .await
        .unwrap();
    assert_eq!(
        node.capability(&owner, scope.clone()).await,
        Err(DocsError::Denied)
    );
    node.grant(&owner, object).await;
    let capability = node.capability(&owner, scope).await.unwrap();
    assert_ne!(capability, old_capability);
    assert_eq!(
        op(node
            .client()
            .call(
                RequestContext::root(),
                ReadFacets {
                    capability: old_capability,
                    doc_id: created.doc_id.clone(),
                    at: At::Current,
                    facets: None,
                    optimistic: false,
                },
            )
            .await),
        Err(DocsError::Denied)
    );
    node.client()
        .call(
            RequestContext::root(),
            ReadFacets {
                capability,
                doc_id: created.doc_id,
                at: At::Current,
                facets: None,
                optimistic: false,
            },
        )
        .await
        .unwrap();
    node.stop().await;
}

#[tokio::test]
async fn shutdown_drains_before_retiring_the_namespace_and_restart_refreshes_grant_gates() {
    let mut node = Node::boot().await;
    let owner = node.writer("owner").await;
    let created = node
        .create(&owner, "create", [(key("main"), note("durable"))])
        .await
        .unwrap();
    let scope = DocScope {
        write: BTreeSet::from([created.doc_id.clone()]),
        create: BTreeSet::from([PLACEMENT.into()]),
        ..DocScope::default()
    };
    let resource = mint(
        &node,
        Authorization::identity(owner.capability),
        scope.clone(),
        Rights::USE | Rights::NARROW,
    )
    .await;
    let context = RequestContext::root();
    let attenuated = node
        .authority
        .client
        .call(
            context.child(),
            AttenuateCapability {
                capability: resource.capability_id,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let inspection = node
        .docs
        .as_ref()
        .unwrap()
        .mint_inspection_handle(created.doc_id.clone(), owner.capability)
        .await
        .unwrap();
    let namespace = node.docs_namespace;
    let access = Arc::clone(&node.access);
    let gates = access
        .prove(&context, owner.actor_id, &scope)
        .await
        .unwrap();
    assert_eq!(gates.len(), 3);
    let mut capabilities = Vec::new();
    for capability in [
        namespace.capability,
        resource.capability_id,
        attenuated.capability_id,
        inspection.capability_id,
    ]
    .into_iter()
    .chain(gates.iter().copied())
    {
        capabilities.push(
            node.authority
                .client
                .call(context.child(), InspectCapability { capability })
                .await
                .unwrap(),
        );
    }
    let read = |capability| ReadFacets {
        capability,
        doc_id: created.doc_id.clone(),
        at: At::Current,
        facets: None,
        optimistic: false,
    };
    let inspect = |capability| InspectFacetClaims {
        capability,
        doc_id: created.doc_id.clone(),
        facet_key: key("main"),
        heads: created.heads.clone(),
    };
    let expected_read = node
        .client()
        .call(context.child(), read(attenuated.capability_id))
        .await
        .unwrap();
    let expected_claims = node
        .client()
        .call(context.child(), inspect(inspection.capability_id))
        .await
        .unwrap();
    let docs = node.docs.as_ref().unwrap();
    let mint_tasks = docs.mint_spawner.clone();
    let service_tasks = docs.service.tasks.clone();
    let mut phases = Vec::new();
    for tasks in [mint_tasks.clone(), service_tasks.clone()] {
        let (release, released) = tokio::sync::oneshot::channel();
        let cancellation = tasks.clone();
        let task = tasks
            .spawn(async move {
                cancellation.cancelled().await;
                released.await.unwrap();
            })
            .unwrap();
        phases.push((tasks, release, task));
    }
    let stopping = tokio::spawn(node.docs_stop.take().unwrap().stop());
    for (index, (tasks, release, task)) in phases.into_iter().enumerate() {
        tasks.cancelled().await;
        assert!(mint_tasks.is_cancelled());
        assert_eq!(service_tasks.is_cancelled(), index == 1);
        assert!(!stopping.is_finished());
        assert_eq!(
            op(node
                .client()
                .call(context.child(), read(resource.capability_id))
                .await),
            Err(DocsError::Context(ContextError::Cancelled))
        );
        assert_eq!(
            docs.mint_inspection_handle(created.doc_id.clone(), owner.capability)
                .await,
            Err(DocsError::Context(ContextError::Cancelled))
        );
        for capability in &capabilities {
            assert_eq!(
                node.authority
                    .client
                    .call(
                        context.child(),
                        InspectCapability {
                            capability: capability.capability_id,
                        },
                    )
                    .await
                    .unwrap(),
                *capability
            );
        }
        release.send(()).unwrap();
        task.await.unwrap();
    }
    stopping.await.unwrap();
    assert_eq!(mint_tasks.live_tasks(), 0);
    assert_eq!(service_tasks.live_tasks(), 0);
    for capability in &capabilities {
        assert_eq!(
            node.authority
                .client
                .call(
                    context.child(),
                    InspectCapability {
                        capability: capability.capability_id,
                    },
                )
                .await,
            Err(ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            ))
        );
    }
    for actor in [&owner, &node.docs_actor] {
        assert_eq!(
            node.authority
                .client
                .call(
                    context.child(),
                    ResolveActor {
                        capability: actor.capability,
                    },
                )
                .await
                .unwrap(),
            *actor
        );
    }
    node.docs.take();
    node.reopen_namespace().await;
    assert_eq!(node.docs_namespace.namespace_id, namespace.namespace_id);
    assert_ne!(node.docs_namespace.capability, namespace.capability);
    assert_eq!(
        node.authority
            .service
            .open_namespace(namespace.namespace_id)
            .await
            .unwrap(),
        node.docs_namespace
    );
    node.start().await.unwrap();
    let fresh_gates = node
        .access
        .prove(&context, owner.actor_id, &scope)
        .await
        .unwrap();
    assert_eq!(fresh_gates.len(), gates.len());
    assert!(fresh_gates.iter().all(|gate| !gates.contains(gate)));
    assert!(matches!(
        access.prove(&context, owner.actor_id, &scope).await,
        Err(AccessError::Refused(_))
    ));
    let fresh = node.capability(&owner, scope).await.unwrap();
    assert_ne!(fresh, resource.capability_id);
    assert_eq!(
        node.client()
            .call(context.child(), read(fresh))
            .await
            .unwrap(),
        expected_read
    );
    let fresh_inspection = node
        .docs
        .as_ref()
        .unwrap()
        .mint_inspection_handle(created.doc_id.clone(), owner.capability)
        .await
        .unwrap();
    assert_ne!(fresh_inspection.capability_id, inspection.capability_id);
    assert_eq!(
        node.client()
            .call(context.child(), inspect(fresh_inspection.capability_id))
            .await
            .unwrap(),
        expected_claims
    );
    for capability in &capabilities {
        assert_eq!(
            node.authority
                .client
                .call(
                    context.child(),
                    InspectCapability {
                        capability: capability.capability_id,
                    },
                )
                .await,
            Err(ClientError::Operation(
                AuthorityServiceError::InvalidCapability
            ))
        );
    }
    for capability in [resource.capability_id, attenuated.capability_id] {
        assert_eq!(
            op(node.client().call(context.child(), read(capability)).await),
            Err(DocsError::Denied)
        );
    }
    assert_eq!(
        op(node
            .client()
            .call(context.child(), inspect(inspection.capability_id))
            .await),
        Err(DocsError::Denied)
    );
    node.stop().await;
}

#[tokio::test]
async fn quiet_support_retraction_terminates_narrowed_and_attenuated_observation() {
    enum Target {
        Authority,
        Source,
        Namespace,
        Signer,
        Grant,
        TemporaryGrant,
    }
    for target in [
        Target::Authority,
        Target::Source,
        Target::Namespace,
        Target::Signer,
        Target::Grant,
        Target::TemporaryGrant,
    ] {
        let node = Node::boot().await;
        let owner = node.writer("owner").await;
        let signer = actor(&node, "plugin", Rights::USE).await;
        let created = node
            .create(&owner, "create", [(key("main"), note("unchanged"))])
            .await
            .unwrap();
        let object = LedgerAccess::object(AccessRequest::Read(&created.doc_id));
        let temporary_source = if matches!(target, Target::TemporaryGrant) {
            let source = actor(&node, "grant-source", Rights::USE | Rights::DELEGATE).await;
            node.access
                .grants
                .revoke(
                    &RequestContext::root(),
                    owner.actor_id,
                    std::slice::from_ref(&object),
                )
                .await
                .unwrap();
            node.access
                .grants
                .grant(
                    &RequestContext::root(),
                    owner.actor_id,
                    object.clone(),
                    Rights::USE,
                    Some(source.capability),
                )
                .await
                .unwrap();
            Some(source.capability)
        } else {
            None
        };
        let scope = DocScope {
            read: BTreeSet::from([created.doc_id.clone()]),
            ..DocScope::default()
        };
        let capability = mint(
            &node,
            Authorization::identity(owner.capability),
            scope.clone(),
            Rights::USE | Rights::NARROW,
        )
        .await;
        let narrowed = mint(
            &node,
            Authorization {
                authority: capability.capability_id,
                signer: signer.capability,
            },
            scope,
            Rights::USE | Rights::NARROW,
        )
        .await;
        let attenuated = node
            .authority
            .client
            .call(
                RequestContext::root(),
                AttenuateCapability {
                    capability: narrowed.capability_id,
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
        assert_eq!(attenuated.object, narrowed.object);
        assert_eq!(attenuated.issuer_id, node.docs_namespace.namespace_id);
        let mut events = node
            .client()
            .subscribe(
                RequestContext::root(),
                ObserveFacets {
                    capability: attenuated.capability_id,
                    scope: ObserveScope::Doc {
                        doc_id: created.doc_id,
                    },
                    after: 0,
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            next_facet(&mut events).await.2,
            ObservedFacet::Present { .. }
        ));
        let retracted = match target {
            Target::Authority => Some(capability.capability_id),
            Target::Source => Some(owner.capability),
            Target::Namespace => Some(node.docs_namespace.capability),
            Target::Signer => Some(signer.capability),
            Target::TemporaryGrant => Some(temporary_source.unwrap()),
            Target::Grant => {
                node.access
                    .grants
                    .revoke(&RequestContext::root(), owner.actor_id, &[object])
                    .await
                    .unwrap();
                None
            }
        };
        if let Some(retracted) = retracted {
            node.authority
                .client
                .call(
                    RequestContext::root(),
                    RetractCapability {
                        via: retracted,
                        capability: retracted,
                    },
                )
                .await
                .unwrap();
        }
        loop {
            match events.next().await {
                Some(Ok(FacetObserved::ReplayComplete { .. })) => {}
                terminal => {
                    assert_eq!(
                        terminal,
                        Some(Err(ClientError::Operation(DocsError::Denied)))
                    );
                    break;
                }
            }
        }
        assert_eq!(events.next().await, None);
        node.stop().await;
    }
}

#[tokio::test]
async fn authorized_absent_document_observation_acquires_interest_but_tag_observation_does_not() {
    let node = Node::boot().await;
    let reader = actor(&node, "reader", Rights::USE | Rights::NARROW).await;
    let absent = DocId::new([73; 32]);
    node.grant(&reader, LedgerAccess::object(AccessRequest::Read(&absent)))
        .await;
    let capability = node
        .capability(
            &reader,
            DocScope {
                read: BTreeSet::from([absent.clone()]),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    assert!(!node.repo.sync_demand().documents().contains(&absent));
    let mut tags = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability,
                scope: ObserveScope::Tag {
                    tag: big_cabinet_core::FacetTag::new(NOTE).unwrap(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        tags.next().await,
        Some(Ok(FacetObserved::ReplayComplete { .. }))
    ));
    assert!(!node.repo.sync_demand().documents().contains(&absent));
    let mut document = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                capability,
                scope: ObserveScope::Doc {
                    doc_id: absent.clone(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        document.next().await,
        Some(Ok(FacetObserved::ReplayComplete { .. }))
    ));
    assert!(node.repo.sync_demand().documents().contains(&absent));
    drop(document);
    drop(tags);
    node.stop().await;
}
