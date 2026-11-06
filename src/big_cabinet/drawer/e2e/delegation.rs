//! Delegated drawer capabilities: bound signers, withdrawal, retraction cascades, and recovery.

use super::*;

async fn assert_three_group_closure(
    node: &Node,
    chain: &[(DrawerView, DocId)],
    self_doc: &DocId,
    principal: GroupKey,
    expected: [Option<Access>; 3],
    case: &str,
) {
    for (depth, ((view, doc), access)) in chain.iter().zip(expected).enumerate() {
        let levels = match access {
            None => [None; 3],
            Some(Access::Relay) => [Some(KeyhiveAccess::Read), Some(KeyhiveAccess::Relay), None],
            Some(Access::Read) => [Some(KeyhiveAccess::Read); 3],
            Some(Access::Write) => [Some(KeyhiveAccess::Edit); 3],
            Some(Access::Admin) => [Some(KeyhiveAccess::Admin); 3],
        };
        let (meta, docs, sensitive) = groups_of(view);
        assert_eq!(
            [
                node.group_membership_level(meta, principal).await,
                node.group_membership_level(docs, principal).await,
                node.group_membership_level(sensitive, principal).await,
            ],
            levels,
            "{case}: principal {principal}, drawer {} at depth {depth}, expected {access:?}",
            view.drawer_id,
        );
        assert_eq!(
            [
                node.keyhive_level(principal, view.drawer_id.to_bytes32().unwrap())
                    .await,
                node.keyhive_level(principal, doc.to_bytes32().unwrap())
                    .await,
                node.keyhive_level(
                    principal,
                    view.descriptor
                        .sensitive_root()
                        .unwrap()
                        .to_bytes32()
                        .unwrap(),
                )
                .await,
            ],
            levels,
            "{case}: principal {principal}, roots/content of {}, expected {access:?}",
            view.drawer_id,
        );
        for metadata in [meta, sensitive] {
            assert_eq!(
                node.keyhive_level(metadata, self_doc.to_bytes32().unwrap())
                    .await,
                None,
                "metadata group {metadata} must never reach self-doc {self_doc}",
            );
        }
    }
    let self_doc_level = match expected[2] {
        None => None,
        Some(Access::Relay) => Some(KeyhiveAccess::Relay),
        Some(Access::Read) => Some(KeyhiveAccess::Read),
        Some(Access::Write) => Some(KeyhiveAccess::Edit),
        Some(Access::Admin) => Some(KeyhiveAccess::Admin),
    };
    assert_eq!(
        node.keyhive_level(principal, self_doc.to_bytes32().unwrap())
            .await,
        self_doc_level,
        "{case}: principal {principal}, self-doc {self_doc}",
    );
}

#[tokio::test]
async fn three_group_root_invitations_bound_transitive_links_and_revoke_both_roots() {
    let node = Node::boot().await;
    let alice = node.creator("alice").await;
    let parent = node.collection(&alice, "parent").await;
    let child = node.collection(&alice, "child").await;
    let grandchild = node.collection(&alice, "grandchild").await;
    let home = node.collection(&alice, "unlinked home").await;
    let self_doc = node.create_doc(&alice, &home, "self-doc").await.unwrap();
    node.drawers()
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: node.drawer_capability(&alice, &[&home]).await.unwrap(),
                command: command("declare self-doc"),
                layout: CreateLayout::SelfDoc {
                    doc_id: self_doc.clone(),
                },
            },
        )
        .await
        .unwrap();
    let mut chain = Vec::new();
    for drawer in [&parent, &child, &grandchild] {
        let doc = node
            .create_doc(&alice, drawer, &format!("content/{drawer}"))
            .await
            .unwrap();
        chain.push((node.view(&alice, drawer).await.unwrap(), doc));
    }
    let groups: BTreeSet<_> = chain
        .iter()
        .flat_map(|(view, _)| {
            let (meta, docs, sensitive) = groups_of(view);
            [meta, docs, sensitive]
        })
        .collect();
    assert_eq!(groups.len(), 9);
    let roots: BTreeSet<_> = chain
        .iter()
        .flat_map(|(view, _)| {
            [
                view.drawer_id.clone(),
                view.descriptor.sensitive_root().unwrap(),
            ]
        })
        .collect();
    assert_eq!(roots.len(), 6);

    let mut invitees = Vec::new();
    for access in [Access::Relay, Access::Read, Access::Write, Access::Admin] {
        let group = node
            .repo
            .create_group_with_parents(Vec::new())
            .await
            .unwrap();
        let principal = group_key(&group);
        assert_three_group_closure(
            &node,
            &chain,
            &self_doc,
            principal,
            [None; 3],
            "uninvited or revoked",
        )
        .await;
        node.invite(
            &alice,
            &parent,
            Principal::Group {
                group_id: principal,
            },
            access,
        )
        .await
        .unwrap();
        assert_three_group_closure(
            &node,
            &chain,
            &self_doc,
            principal,
            [Some(access), None, None],
            "root invitation before linking",
        )
        .await;
        invitees.push((access, principal));
    }
    let capability = node
        .drawer_capability(&alice, &[&parent, &child, &grandchild, &self_doc])
        .await
        .unwrap();
    let link = |name: String, parent: &DrawerId, child: &DrawerId, access| {
        node.drawers().call(
            RequestContext::root(),
            AddChild {
                capability,
                command: command(&name),
                parent_id: parent.clone(),
                child_id: child.clone(),
                access,
            },
        )
    };
    link(
        "link self-doc".into(),
        &grandchild,
        &self_doc,
        Access::Admin,
    )
    .await
    .unwrap();
    for parent_bound in [Access::Admin, Access::Write, Access::Read, Access::Relay] {
        link(
            format!("parent link/{parent_bound:?}"),
            &parent,
            &child,
            parent_bound,
        )
        .await
        .unwrap();
        for child_bound in [Access::Admin, Access::Write, Access::Read, Access::Relay] {
            link(
                format!("child link/{parent_bound:?}/{child_bound:?}"),
                &child,
                &grandchild,
                child_bound,
            )
            .await
            .unwrap();
            for (ancestor, descendant, bound) in [
                (&chain[0].0, &chain[1].0, parent_bound),
                (&chain[1].0, &chain[2].0, child_bound),
            ] {
                let (parent_meta, parent_docs, parent_sensitive) = groups_of(ancestor);
                let (child_meta, child_docs, child_sensitive) = groups_of(descendant);
                for (member, expected) in [
                    (
                        parent_meta,
                        [Some(keyhive_access(bound.max(Access::Read))), None, None],
                    ),
                    (parent_docs, [None, Some(keyhive_access(bound)), None]),
                    (
                        parent_sensitive,
                        [
                            None,
                            None,
                            (bound >= Access::Read).then_some(keyhive_access(bound)),
                        ],
                    ),
                ] {
                    assert_eq!(
                        [
                            node.group_membership_level(child_meta, member).await,
                            node.group_membership_level(child_docs, member).await,
                            node.group_membership_level(child_sensitive, member).await,
                        ],
                        expected,
                        "parent link {parent_bound:?}, child link {child_bound:?}: member {member}, child {}, bound {bound:?}",
                        descendant.drawer_id,
                    );
                }
            }
            for (access, principal) in &invitees {
                let child_access = (*access).min(parent_bound);
                let grandchild_access = child_access.min(child_bound);
                assert_three_group_closure(
                    &node,
                    &chain,
                    &self_doc,
                    *principal,
                    [Some(*access), Some(child_access), Some(grandchild_access)],
                    &format!("invite {access:?}, parent link {parent_bound:?}, child link {child_bound:?}"),
                )
                .await;
            }
        }
    }
    for (name, ancestor, descendant) in [
        ("restore parent", &parent, &child),
        ("restore child", &child, &grandchild),
    ] {
        link(name.into(), ancestor, descendant, Access::Admin)
            .await
            .unwrap();
    }
    for (access, principal) in &invitees {
        assert_three_group_closure(
            &node,
            &chain,
            &self_doc,
            *principal,
            [Some(*access); 3],
            "restored links",
        )
        .await;
    }
    for (name, ancestor, descendant, remaining) in [
        ("unlink child", &child, &grandchild, 2),
        ("unlink parent", &parent, &child, 1),
    ] {
        node.drawers()
            .call(
                RequestContext::root(),
                RemoveChild {
                    capability,
                    command: command(name),
                    parent_id: ancestor.clone(),
                    child_id: descendant.clone(),
                },
            )
            .await
            .unwrap();
        for (access, principal) in &invitees {
            let expected = std::array::from_fn(|index| (index < remaining).then_some(*access));
            assert_three_group_closure(&node, &chain, &self_doc, *principal, expected, name).await;
        }
    }
    for (name, ancestor, descendant) in [
        ("relink parent", &parent, &child),
        ("relink child", &child, &grandchild),
    ] {
        link(name.into(), ancestor, descendant, Access::Admin)
            .await
            .unwrap();
    }
    for (access, principal) in invitees {
        assert_three_group_closure(
            &node,
            &chain,
            &self_doc,
            principal,
            [Some(access); 3],
            "relinked before revocation",
        )
        .await;
        node.drawers()
            .call(
                RequestContext::root(),
                RevokePrincipal {
                    capability,
                    drawer_id: parent.clone(),
                    principal: Principal::Group {
                        group_id: principal,
                    },
                },
            )
            .await
            .unwrap();
        assert_three_group_closure(
            &node,
            &chain,
            &self_doc,
            principal,
            [None; 3],
            "uninvited or revoked",
        )
        .await;
    }
    node.stop().await;
}

#[tokio::test]
async fn drawer_policy_shutdown_retires_shared_grants_and_downstream_document_caps() {
    use big_cabinet_core::authority::InspectCapability;

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
    let read = |capability| ReadDatums {
        capability,
        doc_id: doc.clone(),
        at: At::Current,
        datums: None,
        optimistic: false,
    };
    for capability in &doc_caps {
        node.docs()
            .call(RequestContext::root(), read(*capability))
            .await
            .unwrap();
    }
    let mut poll = crate::test_poll::Poller::new(
        node.docs(),
        PollDatums {
            capability: doc_caps[1],
            predicate: Predicate::IsDoc {
                doc_id: doc.clone(),
            },
            datums: DatumSelection::Of {
                tags: [big_cabinet_core::DatumTag::new(NOTE).unwrap()]
                    .into_iter()
                    .collect(),
                keys: BTreeSet::new(),
            },
            after: 0,
            limit: 0,
            hold_ms: 0,
        },
    );
    let mut present = false;
    while !present {
        match poll.next().await.unwrap().expect("the note never replayed") {
            DatumEvent::Datum {
                doc_id,
                datum: ObservedDatum::Present { .. },
                ..
            } => {
                assert_eq!(doc_id, doc);
                present = true;
            }
            DatumEvent::Datum { .. } | DatumEvent::Leave { .. } => {}
        }
    }

    node.drawers_stop.take().unwrap().stop().await;
    for capability in &doc_caps {
        node.docs()
            .call(RequestContext::root(), read(*capability))
            .await
            .unwrap();
    }
    node.drawer_policy_stop.take().unwrap().stop().await;
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
    let cursor = poll.cursor();
    assert_eq!(
        poll.next().await,
        Err(ClientError::Operation(DocsError::Denied))
    );
    assert_eq!(
        poll.cursor(),
        cursor,
        "a denied poll must not move the cursor"
    );
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
async fn delegated_drawer_capabilities_bind_signers_without_standing_grants() {
    use big_cabinet_core::authority::AttenuateCapability;
    use big_cabinet_core::authority::RetractCapability;

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
        .call_with(
            RequestContext::root(),
            Authorization {
                authority: source,
                signer: plug.capability,
            }
            .metadata(),
            MintDrawerCapability {
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

    let descriptor = node
        .core()
        .state
        .drawer(&drawer)
        .await
        .unwrap()
        .unwrap()
        .descriptor;
    assert_eq!(
        node.drawers()
            .call(
                RequestContext::root(),
                ListDrawers {
                    capability: delegated,
                },
            )
            .await
            .unwrap(),
        vec![ListedDrawer {
            drawer_id: drawer.clone(),
            layout: descriptor.layout.clone(),
            descriptor,
            access: Access::Write,
        }],
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
            .call_with(
                RequestContext::root(),
                Authorization {
                    authority: source,
                    signer: alice.capability
                }
                .metadata(),
                MintDrawerCapability {
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
    let signer = sqlite::get_op(&node.sql, "plug removes entry".to_owned())
        .await
        .unwrap()
        .unwrap()
        .actor_uuid;
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
    use big_cabinet_core::authority::AttenuateCapability;
    use big_cabinet_core::authority::RetractCapability;

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
                big_cabinet_core::authority::InspectCapability {
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
                    big_cabinet_core::authority::InspectCapability { capability }
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
    use big_cabinet_core::authority::InspectCapability;

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
            ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: Some([note_key.clone()].into()),
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
        expected_read.datums.get(&note_key),
        Some(DatumRead::Accepted { value, .. }) if *value == note_value
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
        .call_with(
            RequestContext::root(),
            Authorization {
                authority: source,
                signer: plug.capability,
            }
            .metadata(),
            MintDrawerCapability {
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
        let row = sqlite::get_op(&node.sql, name.to_owned())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.actor_uuid, plug.actor_id.to_string());
        let operation: ledger::Op = serde_json::from_str(&row.op_json).unwrap();
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
        .call_with(
            context.child(),
            Authorization {
                authority: source,
                signer: plug.capability,
            }
            .metadata(),
            MintDrawerCapability {
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
                sensitive_root: None,
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
    let sensitive = node
        .repo
        .create_group_with_parents(Vec::new())
        .await
        .unwrap();
    let groups = DrawerGroups {
        meta_group: group_key(&meta),
        docs_group: group_key(&docs),
        sensitive_group: group_key(&sensitive),
    };
    let collection_op = ledger::Op::Create {
        groups: Some(groups),
        sensitive_root: Some(DocId::new([42; 32])),
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
        .core()
        .as_signer(
            &context,
            caller.signer.capability,
            DocScope {
                write: [doc.clone()].into(),
                ..Default::default()
            },
            |capability| {
                let (self_doc_command, doc, node, context) =
                    (&self_doc_command, &doc, &node, &context);
                async move {
                    node.docs()
                        .call(
                            context.child(),
                            WriteDatums {
                                capability,
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
                        .map_err(docs_error)
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.roster_datums(&doc).await,
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
            Drawer::single_group(
                groups.meta_group,
                groups.docs_group,
                groups.sensitive_group,
                DocId::new([42; 32]),
            ),
        ),
        (&self_doc_command, &self_doc_op, Drawer::self_doc()),
    ] {
        let row = sqlite::get_op(&node.sql, command.as_str().to_owned())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.actor_uuid, plug.actor_id.to_string());
        assert_eq!(
            serde_json::from_str::<ledger::Op>(&row.op_json).unwrap(),
            *expected_op
        );
        assert_eq!(row.step, "done");
        let created: DrawerCreated = serde_json::from_str(&row.receipt_json.unwrap()).unwrap();
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
