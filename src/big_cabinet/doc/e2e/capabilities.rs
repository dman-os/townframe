use super::*;
use crate::authority::{CheckGrant, RetractCapability, RevokeAccess};
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

async fn owner(node: &Node) -> Actor {
    let owner = actor(node, "owner", Rights::all()).await;
    node.grant(
        &owner,
        LedgerAccess::object(AccessRequest::Create {
            placement: PLACEMENT,
        }),
    )
    .await;
    owner
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
async fn scoped_authority_uses_independent_signer_without_grants_or_fallback() {
    let node = Node::boot().await;
    let owner = owner(&node).await;
    let signer = actor(&node, "plugin", Rights::USE).await;
    let created = node
        .create(&owner, "create", [(key("main"), note("before"))])
        .await
        .unwrap();
    let other = node
        .create(&owner, "other", [(key("main"), note("private"))])
        .await
        .unwrap();
    let capability = mint(
        &node,
        Authorization::identity(owner.capability),
        DocScope {
            read: BTreeSet::from([created.doc_id.clone()]),
            write: BTreeSet::from([created.doc_id.clone()]),
            ..DocScope::default()
        },
        Rights::USE | Rights::NARROW,
    )
    .await;
    let authorization = Authorization {
        authority: capability.capability_id,
        signer: signer.capability,
    };
    node.client()
        .call(
            RequestContext::root(),
            WriteFacets {
                authorization,
                command: None,
                doc_id: created.doc_id.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key("main"), note("signed by plugin"))]),
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
                authorization,
                doc_id: created.doc_id.clone(),
                at: At::Current,
                facets: Some(BTreeSet::from([key("main")])),
                optimistic: false,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(&read.facets[&key("main")], FacetRead::Accepted { value, .. } if *value == note("signed by plugin"))
    );
    let inspection = node
        .docs
        .as_ref()
        .unwrap()
        .mint_inspection_handle(created.doc_id.clone())
        .await
        .unwrap();
    let evidence = node
        .client()
        .call(
            RequestContext::root(),
            InspectFacetClaims {
                authorization: Authorization {
                    authority: inspection.capability_id,
                    signer: owner.capability,
                },
                doc_id: created.doc_id.clone(),
                facet_key: key("main"),
                heads: read.heads,
            },
        )
        .await
        .unwrap();
    assert_eq!(evidence.evidence.len(), 1);
    match &evidence.evidence[0].result {
        SignatureEvidence::VerifiedSignature { evidence } => {
            assert_eq!(evidence.claim.actor_uuid, signer.actor_id);
            assert_eq!(evidence.actor_public_key, signer.public_key);
        }
        result => panic!("expected verified plugin attribution, got {result:?}"),
    }
    assert_eq!(
        node.authority
            .client
            .call(
                RequestContext::root(),
                CheckGrant {
                    via: node.docs_actor.capability,
                    subject_id: signer.actor_id,
                    object: LedgerAccess::object(AccessRequest::Write(&created.doc_id)),
                }
            )
            .await
            .unwrap(),
        Rights::empty()
    );
    assert_eq!(
        op(node
            .client()
            .call(
                RequestContext::root(),
                ReadFacets {
                    authorization: Authorization {
                        signer: owner.capability,
                        ..authorization
                    },
                    doc_id: other.doc_id.clone(),
                    at: At::Current,
                    facets: None,
                    optimistic: false,
                }
            )
            .await),
        Err(DocsError::Denied)
    );
    let narrowed = mint(
        &node,
        authorization,
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
                    authorization: Authorization {
                        authority: narrowed.capability_id,
                        signer: owner.capability
                    },
                    command: None,
                    doc_id: created.doc_id.clone(),
                    mode: WriteMode::AtCurrent,
                    set: BTreeMap::from([(key("main"), note("not authorized"))]),
                    remove: BTreeSet::new(),
                }
            )
            .await),
        Err(DocsError::Denied)
    );
    node.authority
        .client
        .call(
            RequestContext::root(),
            RevokeAccess {
                via: node.docs_actor.capability,
                subject_id: owner.actor_id,
                object: LedgerAccess::object(AccessRequest::Read(&created.doc_id)),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        node.read(&owner, &created.doc_id, At::Current, false).await,
        Err(DocsError::Denied)
    );
    node.client()
        .call(
            RequestContext::root(),
            ReadFacets {
                authorization,
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
async fn independent_signer_does_not_receive_creation_grants() {
    let mut node = Node::boot().await;
    let owner = owner(&node).await;
    let signer = actor(&node, "plugin", Rights::USE).await;
    let created = node
        .client()
        .call(
            RequestContext::root(),
            CreateDoc {
                authorization: Authorization {
                    authority: owner.capability,
                    signer: signer.capability,
                },
                command: CommandId::try_from("independent-create".to_owned()).unwrap(),
                placement: PLACEMENT.into(),
                facets: BTreeMap::from([(key("main"), note("plugin creation"))]),
            },
        )
        .await
        .unwrap();
    node.restart().await.unwrap();
    assert!(
        matches!(node.read_one(&owner, &created.doc_id, At::Current, &key("main")).await, FacetRead::Accepted { value, .. } if value == note("plugin creation"))
    );
    assert_eq!(
        node.read(&signer, &created.doc_id, At::Current, false)
            .await,
        Err(DocsError::Denied)
    );
    let scope = mint(
        &node,
        Authorization::identity(owner.capability),
        DocScope {
            create: BTreeSet::from([PLACEMENT.into()]),
            ..DocScope::default()
        },
        Rights::USE,
    )
    .await;
    let scoped = node
        .client()
        .call(
            RequestContext::root(),
            CreateDoc {
                authorization: Authorization {
                    authority: scope.capability_id,
                    signer: signer.capability,
                },
                command: CommandId::try_from("scoped-create".to_owned()).unwrap(),
                placement: PLACEMENT.into(),
                facets: BTreeMap::from([(key("main"), note("scoped creation"))]),
            },
        )
        .await
        .unwrap();
    for request in [
        AccessRequest::Read(&scoped.doc_id),
        AccessRequest::Write(&scoped.doc_id),
    ] {
        assert_eq!(
            node.authority
                .client
                .call(
                    RequestContext::root(),
                    CheckGrant {
                        via: node.docs_actor.capability,
                        subject_id: signer.actor_id,
                        object: LedgerAccess::object(request),
                    }
                )
                .await
                .unwrap(),
            Rights::empty()
        );
    }
    node.stop().await;
}

#[tokio::test]
async fn quiet_identity_grant_loss_and_restoration_update_document_observation() {
    let node = Node::boot().await;
    let owner = owner(&node).await;
    let created = node
        .create(&owner, "create", [(key("main"), note("unchanged"))])
        .await
        .unwrap();
    let mut events = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                authorization: Authorization::identity(owner.capability),
                scope: ObserveScope::Doc {
                    doc_id: created.doc_id.clone(),
                },
                after: 0,
            },
        )
        .await
        .unwrap();
    let present = next_facet(&mut events).await;
    assert!(matches!(present.2, ObservedFacet::Present { .. }));
    let object = LedgerAccess::object(AccessRequest::Read(&created.doc_id));
    node.authority
        .client
        .call(
            RequestContext::root(),
            RevokeAccess {
                via: node.docs_actor.capability,
                subject_id: owner.actor_id,
                object: object.clone(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        next_facet(&mut events).await,
        (
            created.doc_id.clone(),
            key("main"),
            ObservedFacet::AccessLost
        )
    );
    node.grant(&owner, object).await;
    assert_eq!(next_facet(&mut events).await, present);
    drop(events);
    node.stop().await;
}

#[tokio::test]
async fn quiet_authority_or_signer_retraction_terminates_scoped_observation() {
    enum Target {
        Authority,
        Signer,
    }
    for target in [Target::Authority, Target::Signer] {
        let node = Node::boot().await;
        let owner = owner(&node).await;
        let signer = actor(&node, "plugin", Rights::USE).await;
        let created = node
            .create(&owner, "create", [(key("main"), note("unchanged"))])
            .await
            .unwrap();
        let capability = mint(
            &node,
            Authorization::identity(owner.capability),
            DocScope {
                read: BTreeSet::from([created.doc_id.clone()]),
                ..DocScope::default()
            },
            Rights::USE,
        )
        .await;
        let mut events = node
            .client()
            .subscribe(
                RequestContext::root(),
                ObserveFacets {
                    authorization: Authorization {
                        authority: capability.capability_id,
                        signer: signer.capability,
                    },
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
        node.authority
            .client
            .call(
                RequestContext::root(),
                RetractCapability {
                    via: node.authority.root,
                    capability: match target {
                        Target::Authority => capability.capability_id,
                        Target::Signer => signer.capability,
                    },
                },
            )
            .await
            .unwrap();
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
    let reader = actor(&node, "reader", Rights::USE).await;
    let absent = DocId::new([73; 32]);
    node.grant(&reader, LedgerAccess::object(AccessRequest::Read(&absent)))
        .await;
    assert!(!node.repo.sync_demand().documents().contains(&absent));
    let mut tags = node
        .client()
        .subscribe(
            RequestContext::root(),
            ObserveFacets {
                authorization: Authorization::identity(reader.capability),
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
                authorization: Authorization::identity(reader.capability),
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
