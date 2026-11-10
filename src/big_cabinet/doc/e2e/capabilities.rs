use super::*;
use crate::test_poll::Poller;
use big_cabinet_core::authority::AttenuateCapability;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;

async fn actor(node: &Node, path: &str, rights: Rights) -> OpenedActor {
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
        .call_with(
            RequestContext::root(),
            authorization.metadata(),
            MintDocCapability {
                scope,
                rights,
                subject: None,
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn a_handle_stays_bound_to_its_subject_and_signs_as_whichever_handle_the_caller_presents() {
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
    // The authorization's `authority` decides the new handle's scope and rights and its subject
    // is inherited from it (the owner); its `signer` only decides who signs the call.
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
    assert_eq!(bound.subject, Some(owner.actor.actor_id));
    assert_eq!(bound.issuer_id, node.docs_namespace.namespace_id);
    assert_ne!(bound.issuer_id, signer.actor.actor_id);
    assert_ne!(bound.issuer_id, owner.actor.actor_id);
    assert_ne!(bound.issuer_id, node.docs_actor.actor.actor_id);
    let object: Value =
        serde_json::from_str(bound.object.strip_prefix("DocsRepo/scope/v2/").unwrap()).unwrap();
    assert_eq!(
        object,
        json!({
            "scope": scope,
            "originatingActor": owner.actor.actor_id,
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
    assert_eq!(attenuated.subject, Some(owner.actor.actor_id));
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
    assert_eq!(rebound.subject, Some(owner.actor.actor_id));
    for (index, capability, signing, expected_signer) in [
        (0, bound.capability_id, Some(signer.capability), &signer),
        (
            1,
            attenuated.capability_id,
            Some(signer.capability),
            &signer,
        ),
        (2, rebound.capability_id, Some(owner.capability), &owner),
        // Presented bare, a handle signs as its own subject.
        (3, bound.capability_id, None, &owner),
    ] {
        let write = WriteDatums {
            capability,
            command: None,
            doc_id: created.doc_id.clone(),
            mode: WriteMode::AtCurrent,
            set: BTreeMap::from([(key("main"), note(&format!("write {index}")))]),
            remove: BTreeSet::new(),
        };
        match signing {
            Some(signer) => node
                .client()
                .call_with(
                    RequestContext::root(),
                    Authorization {
                        authority: capability,
                        signer,
                    }
                    .metadata(),
                    write,
                )
                .await
                .unwrap(),
            None => node
                .client()
                .call(RequestContext::root(), write)
                .await
                .unwrap(),
        };
        let read = node
            .client()
            .call(
                RequestContext::root(),
                ReadDatums {
                    capability,
                    doc_id: created.doc_id.clone(),
                    at: At::Current,
                    datums: Some(BTreeSet::from([key("main")])),
                    optimistic: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            accepted(&read.datums[&key("main")]).0,
            &note(&format!("write {index}"))
        );
        let evidence = node.claims(&created.doc_id, &key("main")).await;
        assert_eq!(evidence.len(), 1);
        match &evidence[0].result {
            SignatureEvidence::VerifiedSignature { evidence } => {
                assert_eq!(evidence.claim.actor_uuid, expected_signer.actor.actor_id);
                assert_eq!(evidence.actor_public_key, expected_signer.actor.public_key);
            }
            result => panic!("expected verified bound attribution, got {result:?}"),
        }
    }
    assert!(
        !node
            .access
            .check(
                &RequestContext::root(),
                signer.actor.actor_id,
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
                ReadDatums {
                    capability: bound.capability_id,
                    doc_id: other.doc_id.clone(),
                    at: At::Current,
                    datums: None,
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
                .call_with(
                    RequestContext::root(),
                    Authorization {
                        authority: source,
                        signer: owner.capability
                    }
                    .metadata(),
                    MintDocCapability {
                        scope: DocScope {
                            read: BTreeSet::from([other.doc_id.clone()]),
                            ..DocScope::default()
                        },
                        rights: Rights::USE,
                        subject: None,
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
                WriteDatums {
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

/// The regression ADR 037 question 2 rules out: a holder of a DELEGATE handle naming any actor it
/// likes as the subject of a new handle, then writing through it so the datum is signed as that
/// actor. A handle is trusted for its own subject and that subject's descendants, nothing else.
#[tokio::test]
async fn a_delegate_handle_cannot_be_rebound_to_another_actor_nor_sign_as_one() {
    let node = Node::boot().await;
    let owner = actor(
        &node,
        "owner",
        Rights::USE | Rights::NARROW | Rights::DELEGATE | Rights::MINT_CHILD,
    )
    .await;
    node.grant(
        &owner,
        LedgerAccess::object(AccessRequest::Create {
            placement: PLACEMENT,
        }),
    )
    .await;
    let victim = actor(&node, "victim", Rights::USE).await;
    let agent = node
        .authority
        .client
        .call(
            RequestContext::root(),
            OpenActor {
                parent: owner.capability,
                path: "owner/agent".into(),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let created = node
        .create(&owner, "create", [(key("main"), note("before"))])
        .await
        .unwrap();
    let scope = DocScope {
        write: BTreeSet::from([created.doc_id.clone()]),
        ..DocScope::default()
    };
    let delegate = mint(
        &node,
        Authorization::identity(owner.capability),
        scope.clone(),
        Rights::USE | Rights::NARROW | Rights::DELEGATE,
    )
    .await;
    assert_eq!(delegate.subject, Some(owner.actor.actor_id));

    // Neither the owner's identity nor its DELEGATE handle can name a sibling, the docs
    // service's own actor or the operator.
    for named in [
        victim.actor.actor_id,
        node.docs_actor.actor.actor_id,
        node.authority.service.operator_uuid(),
    ] {
        for authority in [owner.capability, delegate.capability_id] {
            assert_eq!(
                op(node
                    .client()
                    .call_with(
                        RequestContext::root(),
                        Authorization::identity(authority).metadata(),
                        MintDocCapability {
                            scope: scope.clone(),
                            rights: Rights::USE,
                            subject: Some(named),
                        },
                    )
                    .await),
                Err(DocsError::Authority(AuthorityServiceError::Escalation)),
                "naming {named}"
            );
        }
    }

    // The attribution stays the owner's however the handle is presented: a signer that is not
    // a live handle with USE is refused outright, and a handle without USE is not a signer.
    let no_use = mint(
        &node,
        Authorization::identity(delegate.capability_id),
        scope.clone(),
        Rights::NARROW,
    )
    .await;
    for signer in [CapId::new_random(), no_use.capability_id] {
        assert_eq!(
            op(node
                .client()
                .call_with(
                    RequestContext::root(),
                    Authorization {
                        authority: delegate.capability_id,
                        signer,
                    }
                    .metadata(),
                    WriteDatums {
                        capability: delegate.capability_id,
                        command: None,
                        doc_id: created.doc_id.clone(),
                        mode: WriteMode::AtCurrent,
                        set: BTreeMap::from([(key("main"), note("forged"))]),
                        remove: BTreeSet::new(),
                    },
                )
                .await),
            Err(DocsError::Denied)
        );
    }
    node.client()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: delegate.capability_id,
                command: None,
                doc_id: created.doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key("main"), note("by the owner"))]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    let signed_by = |evidence: &[DatumClaimEvidence]| match &evidence[0].result {
        SignatureEvidence::VerifiedSignature { evidence } => evidence.claim.actor_uuid,
        result => panic!("expected a verified signature, got {result:?}"),
    };
    assert_eq!(
        signed_by(&node.claims(&created.doc_id, &key("main")).await),
        owner.actor.actor_id
    );

    // A descendant is the owner's to name, and the handle then signs as that descendant.
    let introduced = op(node
        .client()
        .call_with(
            RequestContext::root(),
            Authorization::identity(delegate.capability_id).metadata(),
            MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: Some(agent.actor.actor_id),
            },
        )
        .await)
    .unwrap();
    assert_eq!(introduced.subject, Some(agent.actor.actor_id));
    node.client()
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: introduced.capability_id,
                command: None,
                doc_id: created.doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key("main"), note("by the agent"))]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        signed_by(&node.claims(&created.doc_id, &key("main")).await),
        agent.actor.actor_id
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
                subject: None,
            },
        )
        .await
        .unwrap();
    let namespace = node
        .authority
        .service
        .open_namespace(owner.actor.actor_id)
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
                subject: None,
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
                    ReadDatums {
                        capability,
                        doc_id: created.doc_id.clone(),
                        at: At::Current,
                        datums: None,
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
                    ListDatums {
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
                    WriteDatums {
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
                        datums: BTreeMap::new(),
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
                    PollDatums {
                        capability,
                        predicate: Predicate::IsDoc {
                            doc_id: created.doc_id.clone()
                        },
                        datums: DatumSelection::All,
                        after: 0,
                        limit: 0,
                        hold_ms: 0,
                    }
                )
                .await),
            Err(DocsError::Denied)
        );
        for drawer_id in [None, Some(created.doc_id.clone())] {
            assert_eq!(
                op(node
                    .client()
                    .call(
                        RequestContext::root(),
                        QueryDatumRefs {
                            capability,
                            drawer_id,
                            selector: DatumRefSelector::Origin {
                                doc_id: created.doc_id.clone(),
                                datum_key: None,
                            },
                        },
                    )
                    .await),
                Err(DocsError::Denied),
            );
        }
        assert!(matches!(
            node.client()
                .call(
                    RequestContext::root(),
                    PollDatums {
                        capability,
                        predicate: Predicate::IsDoc {
                            doc_id: created.doc_id.clone()
                        },
                        datums: DatumSelection::All,
                        after: 0,
                        limit: 0,
                        hold_ms: 0,
                    }
                )
                .await,
            Err(ClientError::Method(DocsError::Denied))
        ));
    }
    node.stop().await;
}

/// Delegates to the ledger, but parks every placement forever after reporting it, so a create or
/// branch stops between committing its document and granting its owner access.
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

/// Asserts, for every subject, whether `access` grants it both read and write on each receipt.
async fn assert_doc_access(
    access: &LedgerAccess,
    receipts: &[WriteReceipt],
    subjects: &[(&OpenedActor, bool)],
) {
    for receipt in receipts {
        for request in [
            AccessRequest::Read(&receipt.doc_id),
            AccessRequest::Write(&receipt.doc_id),
        ] {
            for &(subject, expected) in subjects {
                assert_eq!(
                    access
                        .check(&RequestContext::root(), subject.actor.actor_id, request)
                        .await
                        .unwrap(),
                    expected
                );
            }
        }
    }
}

/// The create and branch requests the recovery test interrupts, each with a handle minted for
/// the owner (the plugin signs only by being presented as the call's signer), and the scopes the
/// handles were minted for.
async fn bound_creation_requests(
    node: &Node,
    owner: &OpenedActor,
    signer: &OpenedActor,
    origin: &WriteReceipt,
) -> (DocScope, DocScope, CreateDoc, BranchDoc) {
    let create_scope = DocScope {
        create: BTreeSet::from([PLACEMENT.into()]),
        ..DocScope::default()
    };
    let source = mint(
        node,
        Authorization::identity(owner.capability),
        create_scope.clone(),
        Rights::USE | Rights::NARROW,
    )
    .await;
    let bound = mint(
        node,
        Authorization {
            authority: source.capability_id,
            signer: signer.capability,
        },
        create_scope.clone(),
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
    let create = CreateDoc {
        capability: attenuated.capability_id,
        command: "bound-create".to_owned().try_into().unwrap(),
        placement: PLACEMENT.into(),
        datums: BTreeMap::from([(key("main"), note("content"))]),
    };
    let branch_scope = DocScope {
        write: BTreeSet::from([origin.doc_id.clone()]),
        ..create_scope.clone()
    };
    let branch_cap = mint(
        node,
        Authorization {
            authority: owner.capability,
            signer: signer.capability,
        },
        branch_scope.clone(),
        Rights::USE,
    )
    .await;
    let branch = BranchDoc {
        capability: branch_cap.capability_id,
        command: "bound-branch".to_owned().try_into().unwrap(),
        doc_id: origin.doc_id.clone(),
        at: At::Current,
        path: "/plugin".into(),
        stealth: false,
        placement: PLACEMENT.into(),
    };
    (create_scope, branch_scope, create, branch)
}

#[tokio::test]
async fn creation_and_recovery_grant_originating_actor_not_bound_signer() {
    use super::super::commands::Commands;
    use super::super::commands::Detail;
    use super::super::commands::Kind;
    use super::super::commands::Row;
    use super::super::commands::State;

    let mut node = Node::boot().await;
    let owner = node.writer("owner").await;
    let signer = actor(&node, "plugin", Rights::USE).await;
    let origin = node
        .create(&owner, "origin", [(key("main"), note("content"))])
        .await
        .unwrap();
    node.crash_docs().await;
    let (entered, mut placements) = tokio::sync::mpsc::unbounded_channel();
    let access = Arc::new(InterruptedPlacement {
        ledger: Arc::clone(&node.access),
        entered,
    });
    node.start_with(access, None).await.unwrap();
    let (create_scope, branch_scope, mut create, mut branch) =
        bound_creation_requests(&node, &owner, &signer, &origin).await;
    // Each handle is bound to the owner; the plugin signs because the call presents the plugin's
    // handle as its signer.
    let through = |handle: CapId, signer: CapId| {
        Authorization {
            authority: handle,
            signer,
        }
        .metadata()
    };
    let creating = {
        let client = node.client().clone();
        let request = create.clone();
        let metadata = through(request.capability, signer.capability);
        tokio::spawn(async move {
            client
                .call_with(RequestContext::root(), metadata, request)
                .await
        })
    };
    let branching = {
        let client = node.client().clone();
        let request = branch.clone();
        let metadata = through(request.capability, signer.capability);
        tokio::spawn(async move {
            client
                .call_with(RequestContext::root(), metadata, request)
                .await
        })
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
                owner_id: Some(owner.actor.actor_id),
            },
        ),
        (
            &rows[1],
            Kind::Branch,
            Detail::Branch {
                origin: origin.doc_id.clone(),
                path: branch.path.clone(),
                stealth: branch.stealth,
                owner_id: Some(owner.actor.actor_id),
            },
        ),
    ] {
        assert_eq!(
            (row.kind, row.actor, row.state, row.detail.as_ref()),
            (kind, signer.actor.actor_id, State::Committed, Some(&detail))
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
                    Some(owner.actor.actor_id),
                    receipt.doc_id.clone(),
                    PLACEMENT.to_owned(),
                )
            })
            .collect::<BTreeSet<_>>()
    );
    assert_doc_access(
        &node.access,
        &receipts,
        &[(&owner, false), (&signer, false)],
    )
    .await;
    assert_eq!(
        op(node
            .client()
            .call_with(
                RequestContext::root(),
                through(create.capability, signer.capability),
                create.clone()
            )
            .await),
        Err(DocsError::CommandInFlight {
            command: create.command.clone(),
        })
    );
    assert_eq!(
        op(node
            .client()
            .call_with(
                RequestContext::root(),
                through(branch.capability, signer.capability),
                branch.clone()
            )
            .await),
        Err(DocsError::CommandInFlight {
            command: branch.command.clone(),
        })
    );
    let other_owner = node.writer("other-owner").await;
    for (authorization, presenting) in [
        (Authorization::identity(owner.capability), owner.capability),
        (
            Authorization {
                authority: other_owner.capability,
                signer: signer.capability,
            },
            signer.capability,
        ),
    ] {
        let different = mint(&node, authorization, create_scope.clone(), Rights::USE).await;
        assert_eq!(
            op(node
                .client()
                .call_with(
                    RequestContext::root(),
                    through(different.capability_id, presenting),
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
        assert_eq!(
            ledger.get(&row.command).await.unwrap().unwrap(),
            Row {
                state: State::Done,
                ..row.clone()
            }
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
            .call_with(
                RequestContext::root(),
                through(create.capability, signer.capability),
                create
            )
            .await
            .unwrap(),
        receipts[0]
    );
    assert_eq!(
        node.client()
            .call_with(
                RequestContext::root(),
                through(branch.capability, signer.capability),
                branch
            )
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
            matches!(node.read_one(&owner, &receipt.doc_id, At::Current, &key("main")).await, DatumRead::Accepted { value, .. } if value == note("content"))
        );
    }
    assert_doc_access(
        &reopened,
        &receipts,
        &[(&owner, true), (&signer, false), (&other_owner, false)],
    )
    .await;
    node.stop().await;
}

#[tokio::test]
async fn datum_ref_selector_is_authorized_before_lookup_and_origins_are_batched() {
    use crate::index::DatumRefStore;
    use big_cabinet_core::drawer::DrawerAccess;
    use sqlite_rs::IntoValue;
    use std::sync::Mutex;

    struct Policy {
        sql: Database,
        drawers: BTreeMap<DocId, DrawerAccess>,
        selector: DocId,
        denied: DocId,
        origins: BTreeSet<DocId>,
        calls: Mutex<Vec<BTreeSet<DocId>>>,
    }

    impl DocAccess for Policy {
        fn prove<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Uuid7,
            scope: &'a DocScope,
        ) -> AccessFuture<'a, Vec<CapId>> {
            assert_eq!(scope.drawers, self.drawers);
            Box::pin(async { Ok(Vec::new()) })
        }

        fn check<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Uuid7,
            _request: AccessRequest<'a>,
        ) -> AccessFuture<'a, bool> {
            panic!("reference queries must not check identity grants")
        }

        fn place<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Option<Uuid7>,
            _document: &'a DocId,
            _placement: &'a str,
        ) -> AccessFuture<'a, ()> {
            panic!("reference queries must not place documents")
        }

        fn scoped_docs<'a>(
            &'a self,
            _context: &'a RequestContext,
            drawers: &'a BTreeMap<DocId, DrawerAccess>,
            documents: Option<&'a BTreeSet<DocId>>,
        ) -> AccessFuture<'a, BTreeMap<DocId, DrawerAccess>> {
            Box::pin(async move {
                assert_eq!(drawers, &self.drawers);
                let documents = documents.expect("reference authorization must be bounded");
                self.calls.lock().unwrap().push(documents.clone());
                if documents == &BTreeSet::from([self.denied.clone()]) {
                    return Ok(BTreeMap::new());
                }
                if documents == &BTreeSet::from([self.selector.clone()]) {
                    for origin in &self.origins {
                        self.sql
                            .execute(
                                include_str!("../sqlite/fixtures/insert_ref_edge.sql"),
                                &[
                                    origin.to_string().into_value(),
                                    key("main").to_string().into_value(),
                                    self.selector.to_string().into_value(),
                                    "[]".as_bytes().into_value(),
                                ],
                            )
                            .await
                            .unwrap();
                    }
                    return Ok(BTreeMap::from([(
                        self.selector.clone(),
                        DrawerAccess::Read,
                    )]));
                }
                assert_eq!(documents, &self.origins);
                Ok(self
                    .origins
                    .iter()
                    .enumerate()
                    .map(|(index, origin)| {
                        (
                            origin.clone(),
                            if index == 0 {
                                DrawerAccess::Read
                            } else {
                                DrawerAccess::Relay
                            },
                        )
                    })
                    .collect())
            })
        }
    }

    let mut node = Node::boot().await;
    let owner = actor(&node, "owner", Rights::USE | Rights::NARROW).await;
    let [selector, first_origin, second_origin, denied, drawer] =
        [81_u8, 82, 83, 84, 85].map(|value| DocId::new([value; 32]));
    let index_sql = sqlite_rs::test_support::db(&[]).unwrap().clone();
    let datum_ref = Arc::new(DatumRefStore::new(index_sql.clone()).await.unwrap());
    let policy = Arc::new(Policy {
        sql: index_sql.clone(),
        drawers: BTreeMap::from([(drawer.clone(), DrawerAccess::Read)]),
        selector: selector.clone(),
        denied: denied.clone(),
        origins: BTreeSet::from([first_origin.clone(), second_origin]),
        calls: Mutex::new(Vec::new()),
    });
    node.crash_docs().await;
    node.start_with(Arc::<Policy>::clone(&policy), Some(datum_ref))
        .await
        .unwrap();
    let capability = node
        .capability(
            &owner,
            DocScope {
                drawers: policy.drawers.clone(),
                ..DocScope::default()
            },
        )
        .await
        .unwrap();
    let mut expected_calls = Vec::new();
    for drawer_id in [None, Some(drawer.clone())] {
        assert_eq!(
            op(node
                .client()
                .call(
                    RequestContext::root(),
                    QueryDatumRefs {
                        capability,
                        drawer_id,
                        selector: DatumRefSelector::Target {
                            doc_id: selector.clone(),
                            datum_key: None,
                        },
                    },
                )
                .await),
            Ok(QueriedDatumRefs {
                refs: vec![QueriedDatumRef {
                    origin_doc_id: first_origin.clone(),
                    origin_datum_key: key("main"),
                    path: "/link".to_owned(),
                    target_doc_id: selector.clone(),
                    target_datum_key: key("main"),
                    ref_heads: Vec::new(),
                }]
            }),
        );
        expected_calls.extend([BTreeSet::from([selector.clone()]), policy.origins.clone()]);
    }
    assert_eq!(*policy.calls.lock().unwrap(), expected_calls);
    index_sql
        .execute_batch(include_str!("../sqlite/fixtures/drop_ref_edges.sql"))
        .await
        .unwrap();
    for drawer_id in [None, Some(drawer)] {
        for selector in [
            DatumRefSelector::Origin {
                doc_id: denied.clone(),
                datum_key: None,
            },
            DatumRefSelector::Origin {
                doc_id: denied.clone(),
                datum_key: Some(key("main")),
            },
            DatumRefSelector::Target {
                doc_id: denied.clone(),
                datum_key: None,
            },
            DatumRefSelector::Target {
                doc_id: denied.clone(),
                datum_key: Some(key("main")),
            },
        ] {
            let expected = if drawer_id.is_some() {
                Ok(QueriedDatumRefs { refs: Vec::new() })
            } else {
                Err(DocsError::Denied)
            };
            assert_eq!(
                op(node
                    .client()
                    .call(
                        RequestContext::root(),
                        QueryDatumRefs {
                            capability,
                            drawer_id: drawer_id.clone(),
                            selector
                        },
                    )
                    .await),
                expected,
            );
            expected_calls.push(BTreeSet::from([denied.clone()]));
        }
    }
    assert_eq!(*policy.calls.lock().unwrap(), expected_calls);
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
    node.crash_docs().await;
    let context = RequestContext::root();
    let mut dependencies: Vec<_> = node
        .access
        .grants
        .matching(&context, owner.actor.actor_id, &objects)
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
            .prove(&context, owner.actor.actor_id, &scope)
            .await
            .unwrap(),
        dependencies
    );
    let mut overlapping = scope.clone();
    overlapping.read.insert(write_doc.clone());
    assert_eq!(
        node.access
            .prove(&context, owner.actor.actor_id, &overlapping)
            .await
            .unwrap(),
        dependencies
    );
    let policy = Arc::new(Policy {
        ledger: Arc::clone(&node.access),
        proofs: Default::default(),
    });
    node.start_with(Arc::<Policy>::clone(&policy), None)
        .await
        .unwrap();
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
            .revoke(&context, owner.actor.actor_id, std::slice::from_ref(object))
            .await
            .unwrap();
        for capability in [issued.capability_id, narrowed.capability_id] {
            assert_eq!(
                op(node
                    .client()
                    .call(
                        context.child(),
                        ReadDatums {
                            capability,
                            doc_id: write_doc.clone(),
                            at: At::Current,
                            datums: None,
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
                .grant(&context, owner.actor.actor_id, object.clone(), rights, None)
                .await
                .unwrap();
            assert_eq!(
                op(node
                    .client()
                    .call_with(
                        context.child(),
                        authorization.metadata(),
                        MintDocCapability {
                            scope: scope.clone(),
                            rights: Rights::USE,
                            subject: None,
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
    let mut expected = vec![(owner.actor.actor_id, scope); objects.len() * 3];
    expected.push((owner.actor.actor_id, unsupported));
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
                .call_with(
                    RequestContext::root(),
                    Authorization::identity(source.capability).metadata(),
                    MintDocCapability {
                        scope: scope.clone(),
                        rights,
                        subject: None,
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
            owner.actor.actor_id,
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
                ReadDatums {
                    capability: old_capability,
                    doc_id: created.doc_id.clone(),
                    at: At::Current,
                    datums: None,
                    optimistic: false,
                },
            )
            .await),
        Err(DocsError::Denied)
    );
    node.client()
        .call(
            RequestContext::root(),
            ReadDatums {
                capability,
                doc_id: created.doc_id,
                at: At::Current,
                datums: None,
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
        .mint_inspection_handle(created.doc_id.clone(), node.docs_actor.actor.actor_id)
        .await
        .unwrap();
    let namespace = node.docs_namespace;
    let access = Arc::clone(&node.access);
    let gates = access
        .prove(&context, owner.actor.actor_id, &scope)
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
    let read = |capability| ReadDatums {
        capability,
        doc_id: created.doc_id.clone(),
        at: At::Current,
        datums: None,
        optimistic: false,
    };
    let inspect = |capability| InspectDatumClaims {
        capability,
        doc_id: created.doc_id.clone(),
        datum_key: key("main"),
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
            docs.mint_inspection_handle(created.doc_id.clone(), node.docs_actor.actor.actor_id)
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
            Err(ClientError::Method(
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
                    big_cabinet_core::authority::ResolveActor {
                        actor_id: actor.actor.actor_id,
                    },
                )
                .await
                .unwrap(),
            actor.actor
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
        .prove(&context, owner.actor.actor_id, &scope)
        .await
        .unwrap();
    assert_eq!(fresh_gates.len(), gates.len());
    assert!(fresh_gates.iter().all(|gate| !gates.contains(gate)));
    assert!(matches!(
        access.prove(&context, owner.actor.actor_id, &scope).await,
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
        .mint_inspection_handle(created.doc_id.clone(), node.docs_actor.actor.actor_id)
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
            Err(ClientError::Method(
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
        let owner = actor(
            &node,
            "owner",
            Rights::USE | Rights::NARROW | Rights::DELEGATE | Rights::MINT_CHILD,
        )
        .await;
        node.grant(
            &owner,
            LedgerAccess::object(AccessRequest::Create {
                placement: PLACEMENT,
            }),
        )
        .await;
        // The plugin is the owner's own descendant, the only kind of actor the owner can
        // introduce a handle to.
        let signer = node
            .authority
            .client
            .call(
                RequestContext::root(),
                OpenActor {
                    parent: owner.capability,
                    path: "owner/plugin".into(),
                    rights: Rights::USE,
                },
            )
            .await
            .unwrap();
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
                    owner.actor.actor_id,
                    std::slice::from_ref(&object),
                )
                .await
                .unwrap();
            node.access
                .grants
                .grant(
                    &RequestContext::root(),
                    owner.actor.actor_id,
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
            Rights::USE | Rights::NARROW | Rights::DELEGATE,
        )
        .await;
        // Introduced to the plugin, so it lives only as long as the plugin does.
        let narrowed = node
            .client()
            .call_with(
                RequestContext::root(),
                Authorization::identity(capability.capability_id).metadata(),
                MintDocCapability {
                    scope,
                    rights: Rights::USE | Rights::NARROW,
                    subject: Some(signer.actor.actor_id),
                },
            )
            .await
            .unwrap();
        assert_eq!(narrowed.subject, Some(signer.actor.actor_id));
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
        let mut poll = Poller::doc(
            node.client(),
            attenuated.capability_id,
            created.doc_id.clone(),
        );
        assert!(matches!(
            poll.next_datum().await.2,
            ObservedDatum::Present { .. }
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
                    .revoke(&RequestContext::root(), owner.actor.actor_id, &[object])
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
        assert_eq!(
            poll.page(0).await,
            Err(ClientError::Method(DocsError::Denied))
        );
        node.stop().await;
    }
}

#[tokio::test]
async fn a_held_document_poll_acquires_interest_in_an_absent_document() {
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
    // `IsDoc` takes a `DocInterest` for the poll's duration (ADR 025 §6), so holding an empty
    // page open puts the absent document into the sync demand set.
    let mut held = Poller::doc(node.client(), capability, absent.clone());
    let mut page = Box::pin(held.page(1_000));
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        tokio::select! {
            result = &mut page => panic!("the held poll answered before interest was observed: {result:?}"),
            () = async {
                loop {
                    if node.repo.sync_demand().documents().contains(&absent) {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            } => {}
        }
    })
    .await
    .expect("the held poll never put the document in the sync demand set");
    drop(page);
    node.stop().await;
}
