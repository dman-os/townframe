//! Delegated drawer capabilities: bound signers, withdrawal, retraction cascades, and recovery.

use super::*;

#[tokio::test]
async fn drawer_policy_shutdown_retires_shared_grants_and_downstream_document_caps() {
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
                            WriteFacets {
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
