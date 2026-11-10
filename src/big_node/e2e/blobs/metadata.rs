use super::super::*;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::*;
use service_utils_rs::ClientError;

async fn drawer_capability(node: &BigNode, scope: DrawerScope) -> CapId {
    node.drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                scope,
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .capability_id
}

async fn metadata(node: &BigNode, drawer: &DocId, root: MetaRoot, name: &str) -> DocId {
    let created = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            CreateMetaDoc {
                capability: drawer_capability(
                    node,
                    DrawerScope {
                        drawers: BTreeMap::from([(drawer.clone(), Access::Admin)]),
                        create: false,
                    },
                )
                .await,
                command: format!("{drawer}-{root}-{name}").try_into().unwrap(),
                drawer_id: drawer.clone(),
                kind: "notes".into(),
                name: name.into(),
                root,
            },
        )
        .await
        .unwrap();
    node.drawers
        .client
        .call(
            RequestContext::root(),
            GetDrawer {
                capability: drawer_capability(
                    node,
                    DrawerScope {
                        drawers: BTreeMap::from([(drawer.clone(), Access::Admin)]),
                        create: false,
                    },
                )
                .await,
                drawer_id: drawer.clone(),
            },
        )
        .await
        .unwrap();
    created.doc_id
}

#[tokio::test]
async fn metadata_issuer_mints_only_requested_documents_without_root_or_content_authority() {
    let secrets = secrets().await;
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let (node, stop) = BigNode::boot(config(
        Arc::clone(&secrets),
        sql.clone(),
        big_repo::Storage::memory(&runtime).unwrap(),
    ))
    .await
    .unwrap();
    let issuer = crate::blob_boot::metadata_capability(
        sql.clone(),
        node.docs.client.clone(),
        node.drawers_actor.capability,
    );
    node.drawer_access
        .grants()
        .grant(
            &RequestContext::root(),
            node.drawers_actor.actor_id,
            CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let drawer = node
        .drawers
        .client
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability: drawer_capability(
                    &node,
                    DrawerScope {
                        create: true,
                        drawers: BTreeMap::new(),
                    },
                )
                .await,
                command: "unlinked-metadata-drawer".to_owned().try_into().unwrap(),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .unwrap()
        .drawer_id;
    assert!(
        sql.query(
            "SELECT parent_id FROM big_cabinet_drawer_children WHERE child_id = ?",
            (drawer.to_string(),),
        )
        .await
        .unwrap()
        .all::<String>()
        .unwrap()
        .is_empty()
    );
    let public = metadata(&node, &drawer, MetaRoot::Public, "first").await;
    let sensitive = metadata(&node, &drawer, MetaRoot::Sensitive, "first").await;
    let exact = |document: DocId, write| DocScope {
        read: BTreeSet::from([document.clone()]),
        write: if write {
            BTreeSet::from([document])
        } else {
            BTreeSet::new()
        },
        ..Default::default()
    };
    let public_cap = issuer(exact(public.clone(), false)).await.unwrap();
    let sensitive_cap = issuer(exact(sensitive.clone(), false)).await.unwrap();
    let later = metadata(&node, &drawer, MetaRoot::Public, "later").await;
    let content = create(
        &node,
        "ordinary-metadata-denial",
        json!({"$type": NOTE, "value": "content"}),
    )
    .await
    .doc_id;
    let read = |capability, doc_id| {
        node.docs.client.call(
            RequestContext::root(),
            ReadDatums {
                capability,
                doc_id,
                at: At::Current,
                datums: None,
                optimistic: false,
            },
        )
    };
    read(public_cap, public.clone()).await.unwrap();
    read(sensitive_cap, sensitive.clone()).await.unwrap();
    for (capability, document) in [
        (public_cap, &drawer),
        (public_cap, &later),
        (public_cap, &sensitive),
        (sensitive_cap, &public),
        (public_cap, &content),
        (sensitive_cap, &content),
    ] {
        assert!(matches!(
            read(capability, document.clone()).await,
            Err(ClientError::Method(DocsError::Denied))
        ));
    }
    for write in [false, true] {
        assert!(matches!(
            issuer(exact(content.clone(), write)).await,
            Err(ClientError::Method(DocsError::Denied))
        ));
    }
    let write = |capability| {
        node.docs.client.call(
            RequestContext::root(),
            WriteDatums {
                capability,
                command: None,
                doc_id: public.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(key(), json!({"$type": NOTE, "value": "metadata"}))]),
                remove: BTreeSet::new(),
            },
        )
    };
    assert!(matches!(
        write(public_cap).await,
        Err(ClientError::Method(DocsError::Denied))
    ));
    write(issuer(exact(public.clone(), true)).await.unwrap())
        .await
        .unwrap();
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: capability(
                    &node,
                    DocScope {
                        write: BTreeSet::from([drawer.clone()]),
                        ..Default::default()
                    },
                )
                .await,
                command: None,
                doc_id: drawer.clone(),
                mode: WriteMode::AtCurrent,
                set: BTreeMap::new(),
                remove: BTreeSet::from([DatumKey::from_text(
                    big_cabinet_core::drawer::DrawerMetaDoc::ID,
                    "notes/first",
                )
                .unwrap()]),
            },
        )
        .await
        .unwrap();
    node.drawers
        .client
        .call(
            RequestContext::root(),
            GetDrawer {
                capability: drawer_capability(
                    &node,
                    DrawerScope {
                        drawers: BTreeMap::from([(drawer.clone(), Access::Admin)]),
                        create: false,
                    },
                )
                .await,
                drawer_id: drawer,
            },
        )
        .await
        .unwrap();
    read(public_cap, public.clone())
        .await
        .expect("unlinking metadata does not revoke an existing exact-document capability");
    assert!(matches!(
        issuer(exact(public, false)).await,
        Err(ClientError::Method(DocsError::Denied))
    ));
    read(issuer(exact(later.clone(), false)).await.unwrap(), later)
        .await
        .unwrap();
    drop(issuer);
    shutdown(node, stop).await;
}
