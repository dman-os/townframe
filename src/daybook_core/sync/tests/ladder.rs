use daybook_types::doc::{BranchPath, BranchPathBuf};

use super::*;

async fn boot_connected_sync_pair()
-> Res<(tempfile::TempDir, SyncTestNode, SyncTestNode, EndpointId)> {
    info!("XXX boot_connected_sync_pair");
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    info!("XXX opening node a");
    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    info!("XXX cloning node");
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    info!("XXX opening node b");
    let node_b = open_sync_node(&repo_b_path).await?;

    info!("XXX connecting");
    let addr_a = node_a.sync_repo.endpoint_addr();
    let endpoint_id_a = addr_a.id;
    node_b.sync_repo.connect_endpoint_addr(addr_a).await?;
    info!("XXX waiting");
    wait_for_sync_convergence(&node_a, &node_b, endpoint_id_a).await?;

    Ok((temp_root, node_a, node_b, endpoint_id_a))
}

async fn wait_for_facet_manifest(node: &SyncTestNode, tag: WellKnownFacetTag) -> Res<()> {
    let tag_str = daybook_types::doc::FacetTag::from(tag).to_string();
    tokio::time::timeout(utils_rs::scale_timeout(Duration::from_secs(30)), async {
        loop {
            if matches!(
                node.plugs_repo.get_facet_manifest_by_tag(&tag_str).await,
                crate::plugs::FacetManifestLookup::Found(_)
            ) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| eyre::eyre!("timed out waiting for facet manifest: {tag_str}"))
}

async fn update_title_at_main_branch(node: &SyncTestNode, doc_id: &String, title: &str) -> Res<()> {
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    wait_for_facet_manifest(node, WellKnownFacetTag::TitleGeneric).await?;
    let branch = BranchPathBuf::from("main");
    let Some((_, heads)) = node.drawer.get_with_heads(doc_id, &branch, None).await? else {
        eyre::bail!("missing doc while updating title: {doc_id}");
    };
    node.drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.to_string(),
                facets_set: [(title_key, WellKnownFacet::TitleGeneric(title.into()).into())].into(),
                facets_remove: vec![],
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node.ctx.local_user_path.clone(),
                )),
            },
            &branch,
            Some(heads),
        )
        .await?;
    Ok(())
}

async fn update_title_at_heads(
    node: &SyncTestNode,
    doc_id: &str,
    heads: &ChangeHashSet,
    title: &str,
) -> Res<()> {
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    wait_for_facet_manifest(node, WellKnownFacetTag::TitleGeneric).await?;
    node.drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.to_owned(),
                facets_set: [(title_key, WellKnownFacet::TitleGeneric(title.into()).into())].into(),
                facets_remove: vec![],
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node.ctx.local_user_path.clone(),
                )),
            },
            BranchPath::new("main"),
            Some(heads.clone()),
        )
        .await?;
    Ok(())
}

async fn update_note_at_heads(
    node: &SyncTestNode,
    doc_id: &str,
    heads: &ChangeHashSet,
    note: &str,
) -> Res<()> {
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    wait_for_facet_manifest(node, WellKnownFacetTag::Note).await?;
    node.drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.to_owned(),
                facets_set: [(
                    note_key,
                    WellKnownFacet::Note(daybook_types::doc::Note {
                        mime: "text/plain".into(),
                        content: note.into(),
                    })
                    .into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node.ctx.local_user_path.clone(),
                )),
            },
            BranchPath::new("main"),
            Some(heads.clone()),
        )
        .await?;
    Ok(())
}

async fn assert_title_synced(
    node_a: &SyncTestNode,
    node_b: &SyncTestNode,
    doc_id: &String,
    expected_title: &str,
) -> Res<()> {
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let branch = BranchPathBuf::from("main");
    wait_for_doc_head_parity(node_a, node_b, doc_id, &branch, Duration::from_secs(30)).await?;
    let doc_on_a = node_a
        .drawer
        .get_with_heads(doc_id, &branch, None)
        .await?
        .ok_or_eyre("node_a lost the doc while asserting title sync")?;
    let doc_on_b = node_b
        .drawer
        .get_with_heads(doc_id, &branch, None)
        .await?
        .ok_or_eyre("node_b lost the doc while asserting title sync")?;

    assert_eq!(doc_on_a.0.id, doc_on_b.0.id);
    assert_eq!(doc_on_a.1, doc_on_b.1);
    assert_eq!(doc_on_a.0.facets, doc_on_b.0.facets);
    assert_eq!(
        doc_on_b.0.facets.get(&title_key),
        Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
            expected_title.into()
        ))),
    );
    Ok(())
}

async fn assert_title_and_note_synced(
    node_a: &SyncTestNode,
    node_b: &SyncTestNode,
    doc_id: &String,
    expected_title: &str,
    expected_note: &str,
) -> Res<()> {
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    let branch = BranchPathBuf::from("main");
    wait_for_doc_head_parity(node_a, node_b, doc_id, &branch, Duration::from_secs(30)).await?;
    let doc_on_a = node_a
        .drawer
        .get_with_heads(doc_id, &branch, None)
        .await?
        .ok_or_eyre("node_a lost the doc while asserting merged sync")?;
    let doc_on_b = node_b
        .drawer
        .get_with_heads(doc_id, &branch, None)
        .await?
        .ok_or_eyre("node_b lost the doc while asserting merged sync")?;

    assert_eq!(doc_on_a.0.id, doc_on_b.0.id);
    assert_eq!(doc_on_a.1, doc_on_b.1);
    assert_eq!(doc_on_a.0.facets, doc_on_b.0.facets);
    assert_eq!(
        doc_on_b.0.facets.get(&title_key),
        Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
            expected_title.into()
        ))),
    );
    assert_eq!(
        doc_on_b.0.facets.get(&note_key),
        Some(&serde_json::Value::from(WellKnownFacet::Note(
            daybook_types::doc::Note {
                mime: "text/plain".into(),
                content: expected_note.into(),
            }
        ))),
    );
    Ok(())
}

async fn wait_for_synced_doc_on_both_sides(
    left: &SyncTestNode,
    right: &SyncTestNode,
    doc_id: &String,
    branch: &BranchPathBuf,
) -> Res<(Arc<daybook_types::doc::Doc>, Arc<daybook_types::doc::Doc>)> {
    loop {
        let left_doc = left
            .drawer
            .get_doc_bundle_at_branch(doc_id, branch, None)
            .await?;
        let right_doc = right
            .drawer
            .get_doc_bundle_at_branch(doc_id, branch, None)
            .await?;
        if let (Some(left_doc), Some(right_doc)) = (left_doc, right_doc)
            && left_doc.doc.id == right_doc.doc.id
            && left_doc.doc.facets == right_doc.doc.facets
        {
            return eyre::Ok((Arc::new(left_doc.doc), Arc::new(right_doc.doc)));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_two_nodes_can_connect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (_temp_root, node_a, node_b, endpoint_id) = boot_connected_sync_pair().await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_id).await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_created_before_connect_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_on_a = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Pre-connect sync doc".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        let addr_a = node_a.sync_repo.endpoint_addr();
        let endpoint_id_a = addr_a.id;
        node_b.sync_repo.connect_endpoint_addr(addr_a).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_id_a).await?;

        let doc_on_a = node_a
            .drawer
            .get_with_heads(&doc_on_a, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_a lost the pre-connect doc")?;
        wait_for_drawer_doc_parity(
            &node_a,
            &node_b,
            &doc_on_a.0.id,
            &BranchPathBuf::from("main"),
            Duration::from_secs(30),
        )
        .await?;
        let doc_on_b = node_b
            .drawer
            .get_with_heads(&doc_on_a.0.id, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_b did not receive the pre-connect doc")?;

        assert_eq!(doc_on_a.0.id, doc_on_b.0.id);
        assert_eq!(doc_on_a.0.facets, doc_on_b.0.facets);
        assert_eq!(
            doc_on_b.0.facets.get(&title_key),
            Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
                "Pre-connect sync doc".into()
            ))),
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_blob_created_before_connect_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let payload = b"pre-connect sync blob".to_vec();
    let hash = node_a.blobs_repo.put(&payload).await?;
    let blob_key = FacetKey::from(WellKnownFacetTag::Blob);
    {
        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    blob_key.clone(),
                    WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: payload.len() as u64,
                        digest: crate::blobs::blob_id_to_digest_str(hash.clone()),
                        inline: None,
                        urls: Some(vec![format!("db+blob:///{hash}")]),
                    })
                    .into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        let addr_a = node_a.sync_repo.endpoint_addr();
        let endpoint_id_a = addr_a.id;
        node_b.sync_repo.connect_endpoint_addr(addr_a).await?;
        wait_for_sync_convergence(&node_a, &node_b, endpoint_id_a).await?;

        let doc_on_a = node_a
            .drawer
            .get_with_heads(&doc_id, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_a lost the pre-connect blob doc")?;
        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
        wait_for_drawer_doc_parity(
            &node_a,
            &node_b,
            &doc_id,
            &BranchPathBuf::from("main"),
            Duration::from_secs(60),
        )
        .await?;
        let doc_on_b = node_b
            .drawer
            .get_with_heads(&doc_id, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_b did not receive the pre-connect blob doc")?;

        assert_eq!(doc_on_a.0.id, doc_on_b.0.id);
        assert_eq!(doc_on_a.0.facets, doc_on_b.0.facets);
        assert_eq!(
            doc_on_b.0.facets.get(&blob_key),
            Some(&serde_json::Value::from(WellKnownFacet::Blob(
                daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(hash.clone()),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                },
            )))
        );

        let blob_part = crate::blobs::blob_inventory_part_id(&node_a.ctx.docs_inventory_doc_id);
        node_a
            .sync_repo
            .rcx
            .blob_part_store
            .add_obj_to_parts(
                ObjKey::from(crate::blobs::blob_id_from_hash(&hash.to_string())?),
                vec![blob_part.clone()],
            )
            .await?;
        let peer_id_a = PeerKey::new(*endpoint_id_a.as_bytes());
        node_b
            .sync_repo
            .wait_for_full_sync(&[peer_id_a], &[blob_part], None)
            .await?;
        let got = wait_for_blob_bytes(&node_b.blobs_repo, hash, None).await?;
        assert_eq!(got, payload);
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_created_while_connected_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let addr_a = node_a.sync_repo.endpoint_addr();
    let endpoint_id_a = addr_a.id;
    node_b.sync_repo.connect_endpoint_addr(addr_a).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_id_a).await?;

    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_on_a = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Connected doc sync".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;
        wait_for_doc_head_parity(
            &node_a,
            &node_b,
            &doc_on_a,
            &BranchPathBuf::from("main"),
            Duration::from_secs(60),
        )
        .await?;
        let doc_on_a = node_a
            .drawer
            .get_with_heads(&doc_on_a, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_a lost the connected doc")?;
        let doc_on_b = node_b
            .drawer
            .get_with_heads(&doc_on_a.0.id, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_b did not receive the connected doc")?;

        assert_eq!(doc_on_a.0.id, doc_on_b.0.id);
        assert_eq!(doc_on_a.1, doc_on_b.1);
        assert_eq!(doc_on_a.0.facets, doc_on_b.0.facets);
        assert_eq!(
            doc_on_b.0.facets.get(&title_key),
            Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
                "Connected doc sync".into()
            ))),
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_blob_created_while_connected_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let addr_a = node_a.sync_repo.endpoint_addr();
    let endpoint_id_a = addr_a.id;
    node_b.sync_repo.connect_endpoint_addr(addr_a).await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_id_a).await?;

    {
        let payload = b"connected sync blob".to_vec();
        let hash = node_a.blobs_repo.put(&payload).await?;
        let blob_key = FacetKey::from(WellKnownFacetTag::Blob);
        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    blob_key.clone(),
                    WellKnownFacet::Blob(daybook_types::doc::Blob {
                        mime: "application/octet-stream".to_string(),
                        length_octets: payload.len() as u64,
                        digest: crate::blobs::blob_id_to_digest_str(hash.clone()),
                        inline: None,
                        urls: Some(vec![format!("db+blob:///{hash}")]),
                    })
                    .into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
        let blob_part = crate::blobs::blob_inventory_part_id(&node_a.ctx.docs_inventory_doc_id);
        let endpoint_id_a = node_a.sync_repo.endpoint_addr().id;
        let peer_id_a = PeerKey::new(*endpoint_id_a.as_bytes());
        node_b
            .sync_repo
            .wait_for_full_sync(&[peer_id_a], &[blob_part], None)
            .await?;
        let got = wait_for_blob_bytes(&node_b.blobs_repo, hash.clone(), None).await?;
        assert_eq!(got, payload);
        wait_for_doc_head_parity(
            &node_a,
            &node_b,
            &doc_id,
            &BranchPathBuf::from("main"),
            Duration::from_secs(60),
        )
        .await?;

        let doc_on_a = node_a
            .drawer
            .get_with_heads(&doc_id, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_a lost the connected blob doc")?;
        let doc_on_b = node_b
            .drawer
            .get_with_heads(&doc_id, &BranchPathBuf::from("main"), None)
            .await?
            .ok_or_eyre("node_b did not receive the connected blob doc")?;

        assert_eq!(doc_on_a.0.id, doc_on_b.0.id);
        assert_eq!(doc_on_a.1, doc_on_b.1);
        assert_eq!(doc_on_a.0.facets, doc_on_b.0.facets);
        assert_eq!(
            doc_on_b.0.facets.get(&blob_key),
            Some(&serde_json::Value::from(WellKnownFacet::Blob(
                daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: crate::blobs::blob_id_to_digest_str(hash.clone()),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                },
            ))),
        );
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_doc_updates_propagate_originator_then_other() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Base title".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
        assert_title_synced(&node_a, &node_b, &doc_id, "Base title").await?;

        update_title_at_main_branch(&node_a, &doc_id, "A update 1").await?;
        assert_title_synced(&node_a, &node_b, &doc_id, "A update 1").await?;

        update_title_at_main_branch(&node_b, &doc_id, "B update 2").await?;
        assert_title_synced(&node_a, &node_b, &doc_id, "B update 2").await?;
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_doc_updates_propagate_other_then_originator() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Base title".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
        assert_title_synced(&node_a, &node_b, &doc_id, "Base title").await?;

        update_title_at_main_branch(&node_b, &doc_id, "B update 1").await?;
        assert_title_synced(&node_a, &node_b, &doc_id, "B update 1").await?;

        update_title_at_main_branch(&node_a, &doc_id, "A update 2").await?;
        assert_title_synced(&node_a, &node_b, &doc_id, "A update 2").await?;
    }
    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_divergent_facet_updates_propagate_originator_then_other() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Base title".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
        wait_for_drawer_doc_parity(
            &node_a,
            &node_b,
            &doc_id,
            &BranchPathBuf::from("main"),
            Duration::from_secs(60),
        )
        .await?;
        let branch = BranchPathBuf::from("main");
        let Some((_, base_heads)) = node_a.drawer.get_with_heads(&doc_id, &branch, None).await?
        else {
            eyre::bail!("missing base heads before divergent updates");
        };

        update_title_at_heads(&node_a, &doc_id, &base_heads, "A title").await?;
        update_note_at_heads(&node_b, &doc_id, &base_heads, "B note").await?;

        assert_title_and_note_synced(&node_a, &node_b, &doc_id, "A title", "B note").await?;
    }

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_divergent_facet_updates_propagate_other_then_originator() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_id = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Base title".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;

        wait_for_drawer_doc_parity(
            &node_a,
            &node_b,
            &doc_id,
            &BranchPathBuf::from("main"),
            Duration::from_secs(60),
        )
        .await?;
        let branch = BranchPathBuf::from("main");
        let Some((_, base_heads)) = node_a.drawer.get_with_heads(&doc_id, &branch, None).await?
        else {
            eyre::bail!("missing base heads before divergent updates");
        };

        update_note_at_heads(&node_b, &doc_id, &base_heads, "B note").await?;
        update_title_at_heads(&node_a, &doc_id, &base_heads, "A title").await?;

        assert_title_and_note_synced(&node_a, &node_b, &doc_id, "A title", "B note").await?;
    }
    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_survives_remote_restart_and_reconnect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (temp_root, node_a, node_b, _bootstrap_id) = boot_connected_sync_pair().await?;
    let repo_b_path = temp_root.path().join("repo-b");
    let ticket_a = node_a.sync_repo.get_clone_ticket_url().await?;

    {
        let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
        let doc_on_a = node_a
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: BranchPathBuf::from("main"),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Initial title".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        {
            wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60))
                .await?;

            let doc_on_b = node_b
                .drawer
                .get_with_heads(&doc_on_a, &BranchPathBuf::from("main"), None)
                .await?
                .ok_or_eyre("node_b could not load synced doc after initial connect")?;
            assert_eq!(
                doc_on_b.0.facets.get(&title_key),
                Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
                    "Initial title".into()
                ))),
                "synced doc content did not match on node_b after initial sync"
            );
        }

        node_b.stop().await?;

        let reopened_b = open_sync_node(&repo_b_path).await?;
        let reopened_endpoint_addr = reopened_b.sync_repo.connect_url(&ticket_a).await?;
        wait_for_sync_convergence(&node_a, &reopened_b, reopened_endpoint_addr.id).await?;
        {
            let Some((_, heads)) = node_a
                .drawer
                .get_with_heads(&doc_on_a, &BranchPathBuf::from("main"), None)
                .await?
            else {
                eyre::bail!("node_a lost doc before update after remote restart: {doc_on_a}");
            };
            node_a
                .drawer
                .update_at_heads(
                    daybook_types::doc::DocPatch {
                        id: doc_on_a.clone(),
                        facets_set: [(
                            title_key.clone(),
                            WellKnownFacet::TitleGeneric("Updated after restart".into()).into(),
                        )]
                        .into(),
                        facets_remove: vec![],
                        user_path: Some(daybook_types::doc::UserPathBuf::from(
                            node_a.ctx.local_user_path.clone(),
                        )),
                    },
                    BranchPath::new("main"),
                    Some(heads),
                )
                .await?;

            wait_for_doc_head_parity(
                &node_a,
                &reopened_b,
                &doc_on_a,
                BranchPath::new("main"),
                Duration::from_secs(30),
            )
            .await?;

            let doc_on_b = reopened_b
                .drawer
                .get_with_heads(&doc_on_a, &BranchPathBuf::from("main"), None)
                .await?
                .ok_or_eyre("reopened node_b could not load synced doc after reconnect")?;
            assert_eq!(
                doc_on_b.0.facets.get(&title_key),
                Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
                    "Updated after restart".into()
                ))),
                "synced doc content did not match on reopened node_b after reconnect"
            );
        }
        reopened_b.stop().await?;
    }

    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_shutdown_peer_updates_catch_up_after_reconnect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (temp_root, node_a, node_b, _bootstrap_id) = boot_connected_sync_pair().await?;
    info!(
        addr_a = ?node_a.sync_repo.endpoint_addr(),
        path_a = ?node_a.ctx.layout,
        addr_b = ?node_b.sync_repo.endpoint_addr(),
        path_b = ?node_b.ctx.layout,
    );
    let repo_a_path = temp_root.path().join("repo-a");

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let branch = BranchPathBuf::from("main");
    let doc_on_a = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: branch.clone(),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Live base title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    {
        wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;
        assert_title_synced(&node_a, &node_b, &doc_on_a, "Live base title").await?;

        let Some((_, base_heads)) = node_b
            .drawer
            .get_with_heads(&doc_on_a, &branch, None)
            .await?
        else {
            eyre::bail!("missing base heads on node_b before shutdown updates: {doc_on_a}");
        };

        node_a.stop().await?;

        update_title_at_heads(&node_b, &doc_on_a, &base_heads, "B offline updated title").await?;
    }

    {
        let doc_on_b = node_b
            .drawer
            .add(daybook_types::doc::AddDocArgs {
                branch_path: branch.clone(),
                facets: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("B offline created title".into()).into(),
                )]
                .into(),
                user_path: Some(daybook_types::doc::UserPathBuf::from(
                    node_b.ctx.local_user_path.clone(),
                )),
            })
            .await?;
        update_title_at_main_branch(&node_b, &doc_on_b, "B offline created title v2").await?;
        let reopened_a = open_sync_node(&repo_a_path).await?;
        let reopened_addr_a = reopened_a.sync_repo.endpoint_addr();
        let reopened_endpoint_id = reopened_addr_a.id;
        info!(
            ?reopened_addr_a,
            path_a = ?reopened_a.ctx.layout,
            addr_b = ?node_b.sync_repo.endpoint_addr(),
            path_b = ?node_b.ctx.layout,
        );
        node_b
            .sync_repo
            .connect_endpoint_addr(reopened_addr_a)
            .await?;

        wait_for_sync_convergence(&reopened_a, &node_b, reopened_endpoint_id).await?;

        wait_for_doc_presence_with_activity(&reopened_a, &doc_on_a, Duration::from_secs(60))
            .await?;

        let branch = BranchPathBuf::from("main");
        let (doc_a_on_reopened_a, doc_a_on_b) =
            wait_for_synced_doc_on_both_sides(&reopened_a, &node_b, &doc_on_a, &branch).await?;
        let (doc_b_on_reopened_a, doc_b_on_b) =
            wait_for_synced_doc_on_both_sides(&reopened_a, &node_b, &doc_on_b, &branch).await?;

        assert_eq!(doc_a_on_reopened_a.id, doc_a_on_b.id);
        assert_eq!(doc_a_on_reopened_a.facets, doc_a_on_b.facets);
        assert_eq!(
            doc_a_on_b.facets.get(&title_key),
            Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
                "B offline updated title".into()
            ))),
        );
        assert_eq!(doc_b_on_reopened_a.id, doc_b_on_b.id);
        assert_eq!(doc_b_on_reopened_a.facets, doc_b_on_b.facets);
        assert_eq!(
            doc_b_on_b.facets.get(&title_key),
            Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
                "B offline created title v2".into()
            ))),
        );
        reopened_a.stop().await?;
    }

    node_b.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_offline_divergent_branch_merge_converges() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let (temp_root, node_a, node_b, _bootstrap_id) = boot_connected_sync_pair().await?;

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    wait_for_facet_manifest(&node_a, WellKnownFacetTag::TitleGeneric).await?;
    wait_for_facet_manifest(&node_a, WellKnownFacetTag::Note).await?;
    wait_for_facet_manifest(&node_b, WellKnownFacetTag::TitleGeneric).await?;
    wait_for_facet_manifest(&node_b, WellKnownFacetTag::Note).await?;

    let main_branch = BranchPathBuf::from("main");

    // 1. Node A creates a base document on main.
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: main_branch.clone(),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Base Title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPathBuf::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    // 2. Wait for document to replicate to Node B while connected.
    wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
    assert_title_synced(&node_a, &node_b, &doc_id, "Base Title").await?;

    // 3. Disconnect/stop Node B to simulate offline divergent edits.
    let repo_b_path = temp_root.path().join("repo-b");
    node_b.stop().await?;

    // 4. Node A creates branch '/tmp/feature-a' offline and updates title.
    let branch_a = BranchPathBuf::from("/tmp/feature-a");
    let Some((_, heads_a)) = node_a
        .drawer
        .get_with_heads(&doc_id, &main_branch, None)
        .await?
    else {
        eyre::bail!("missing main heads on node_a: {doc_id}");
    };
    let user_path_a = daybook_types::doc::UserPathBuf::from(node_a.ctx.local_user_path.clone());
    node_a
        .drawer
        .create_branch_at_heads_from_branch(
            &doc_id,
            &branch_a,
            &main_branch,
            &heads_a,
            Some(&user_path_a),
        )
        .await?;
    node_a
        .drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    title_key.clone(),
                    WellKnownFacet::TitleGeneric("Title Updated on Feature A".into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(user_path_a.clone()),
            },
            &branch_a,
            None,
        )
        .await?;

    // 5. Node B reopens offline, creates branch '/tmp/feature-b', and adds a note facet.
    let reopened_b = open_sync_node(&repo_b_path).await?;
    let branch_b = BranchPathBuf::from("/tmp/feature-b");
    let Some((_, heads_b)) = reopened_b
        .drawer
        .get_with_heads(&doc_id, &main_branch, None)
        .await?
    else {
        eyre::bail!("missing main heads on reopened_b: {doc_id}");
    };
    let user_path_b = daybook_types::doc::UserPathBuf::from(reopened_b.ctx.local_user_path.clone());
    reopened_b
        .drawer
        .create_branch_at_heads_from_branch(
            &doc_id,
            &branch_b,
            &main_branch,
            &heads_b,
            Some(&user_path_b),
        )
        .await?;
    reopened_b
        .drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    note_key.clone(),
                    WellKnownFacet::Note(daybook_types::doc::Note {
                        mime: "text/markdown".into(),
                        content: "Note Added on Feature B".into(),
                    })
                    .into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(user_path_b.clone()),
            },
            &branch_b,
            None,
        )
        .await?;

    // 6. Connect Node A and Reopened Node B, sync, and merge branches into main.
    let addr_a = node_a.sync_repo.endpoint_addr();
    reopened_b
        .sync_repo
        .connect_endpoint_addr(addr_a.clone())
        .await?;
    wait_for_sync_convergence(&node_a, &reopened_b, addr_a.id).await?;

    // Merge feature-a into main on Node A, and feature-b into main on Node B.
    node_a
        .drawer
        .merge_from_branch(&doc_id, &main_branch, &branch_a, Some(&user_path_a))
        .await?;
    reopened_b
        .drawer
        .merge_from_branch(&doc_id, &main_branch, &branch_b, Some(&user_path_b))
        .await?;

    wait_for_sync_convergence(&node_a, &reopened_b, addr_a.id).await?;

    // 7. Verify both nodes reach identical merged facet state on main branch.
    let (doc_a, doc_b) =
        wait_for_synced_doc_on_both_sides(&node_a, &reopened_b, &doc_id, &main_branch).await?;

    assert_eq!(doc_a.id, doc_b.id);
    assert_eq!(doc_a.facets, doc_b.facets);
    assert_eq!(
        doc_a.facets.get(&title_key),
        Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
            "Title Updated on Feature A".into()
        )))
    );
    assert_eq!(
        doc_a.facets.get(&note_key),
        Some(&serde_json::Value::from(WellKnownFacet::Note(
            daybook_types::doc::Note {
                mime: "text/markdown".into(),
                content: "Note Added on Feature B".into(),
            }
        )))
    );

    reopened_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn clone_bootstrap_populates_all_globals_and_can_open() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");

    tokio::fs::create_dir_all(&repo_a_path).await?;
    let device_name = "test-device".to_string();
    let rtx = RepoCtx::init(
        &repo_a_path,
        RepoOpenOptions::default(),
        device_name.clone(),
        device_name,
    )
    .await?;
    let source_repo_id = rtx.repo_id.clone();
    let source_repo_name = rtx.repo_name.clone();
    let source_doc_app = rtx.doc_app.document_id().clone();
    let source_doc_drawer = rtx.doc_drawer.document_id().clone();
    rtx.shutdown().await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket = node_a.sync_repo.get_clone_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket, &repo_b_path).await?;
    node_a.stop().await?;

    let cloned = RepoCtx::open(
        &repo_b_path,
        RepoOpenOptions::default(),
        "clone-device".to_string(),
    )
    .await?;

    assert_eq!(
        cloned.repo_id, source_repo_id,
        "clone must share the source repo_id"
    );
    assert_eq!(
        cloned.repo_name, source_repo_name,
        "clone must share the source repo_name"
    );
    assert_eq!(
        cloned.local_device_name, "clone-device",
        "clone must accept a local device name on open"
    );

    let user_id = crate::repo::globals::get_string_global(&cloned.sql, "global.user_id")
        .await?
        .ok_or_eyre("global.user_id missing from cloned repo")?;
    assert!(
        user_id.starts_with(daybook_types::doc::user_path::USER_ID_PREFIX),
        "user_id must start with USER_ID_PREFIX, got: {user_id}"
    );
    assert!(
        cloned.local_user_path.as_str().contains(&user_id),
        "local_user_path must embed the user_id: {} not in {}",
        user_id,
        cloned.local_user_path
    );

    assert_eq!(
        cloned.doc_app.document_id(),
        ObjKey::new(source_doc_app.as_bytes()),
        "cloned app_doc must reference the source's app doc id"
    );
    assert_eq!(
        cloned.doc_drawer.document_id(),
        ObjKey::new(source_doc_drawer.as_bytes()),
        "cloned drawer_doc must reference the source's drawer doc id"
    );

    cloned.shutdown().await?;
    Ok(())
}

mod captured_recovery {
    use super::*;
    use crate::rt::dispatch::{
        ActiveDispatchArgs, ActiveDispatchDeets, CapturedWflowExecution, DispatchAttempt,
        DispatchStatus, RoutineInvocation,
    };
    use crate::rt::{DispatchAdmissionCut, DispatchArgs};
    use futures::StreamExt;
    use wflow::wflow_core::partition::job_events::{JobRunResult, JobWaitResultDeets};
    use wflow::wflow_core::partition::log::PartitionLogEntry;
    use wflow::wflow_tokio::partition::PartitionLogRef;

    struct Fixture {
        temp: tempfile::TempDir,
        node: SyncTestNode,
        manifest_doc: String,
        manifest: daybook_types::manifest::PlugManifest,
    }

    impl Fixture {
        async fn open() -> Res<Self> {
            let temp = tempfile::tempdir()?;
            let root = temp.path().join("repo");
            let ctx = RepoCtx::init(
                &root,
                RepoOpenOptions::default(),
                "test-device".into(),
                "test-device".into(),
            )
            .await?;
            ctx.shutdown().await?;
            let node = open_sync_node(&root).await?;
            let artifact = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/oci/@daybook/test");
            eyre::ensure!(
                artifact.exists(),
                "build the real test artifact first: cargo x build-plug-oci --plug-root ./src/plug_test"
            );
            let imported = node
                .plugs_repo
                .import_from_oci_layout(&artifact, crate::plugs::OciImportOptions::default())
                .await?;
            let manifest_doc = imported
                .doc_id
                .ok_or_eyre("imported plug has no manifest")?;
            let reference: url::Url = format!(
                "db+facet:///{manifest_doc}/org.example.daybook.plugManifest/main?branch=main"
            )
            .parse()?;
            let activation = node.plugs_repo.enable_plug(&reference).await?;
            let status = node
                .rt
                .triage_worker
                .wait_for_activation(&node.plugs_repo, activation)
                .await?;
            eyre::ensure!(
                matches!(status, crate::rt::triage::ActivationStatus::Active(_)),
                "test plug activation failed: {status:?}"
            );
            let (_, manifest) = node
                .plugs_repo
                .materialize_active("@daybook/test", &reference)
                .await?
                .ok_or_eyre("test manifest unavailable")?;
            let fixture = Self {
                temp,
                node,
                manifest_doc,
                manifest: (*manifest).clone(),
            };
            fixture.config_marker("early").await?;
            Ok(fixture)
        }

        async fn config_marker(&self, name: &str) -> Res<()> {
            let config = self
                .node
                .plugs_repo
                .get_plug_config_doc_id("@daybook/test")
                .await
                .ok_or_eyre("missing plug configuration")?;
            self.node
                .drawer
                .update_at_heads(
                    DocPatch {
                        id: config,
                        facets_set: [(
                            FacetKey {
                                tag: "org.example.test.config".into(),
                                id: name.into(),
                            },
                            serde_json::json!({"marker": name}),
                        )]
                        .into(),
                        facets_remove: vec![],
                        user_path: None,
                    },
                    BranchPath::new("main"),
                    None,
                )
                .await?;
            Ok(())
        }

        async fn document(&self, label: &str) -> Res<String> {
            self.node
                .drawer
                .add(AddDocArgs {
                    branch_path: "main".into(),
                    facets: [(
                        FacetKey::from(WellKnownFacetTag::LabelGeneric),
                        WellKnownFacet::LabelGeneric(label.into()).into(),
                    )]
                    .into(),
                    user_path: None,
                })
                .await
                .map_err(Into::into)
        }

        async fn args(&self, doc: &str, await_message: bool) -> Res<DispatchArgs> {
            let (_, heads) = self
                .node
                .drawer
                .get_with_heads(&doc.to_owned(), BranchPath::new("main"), None)
                .await?
                .ok_or_eyre("missing report target")?;
            Ok(DispatchArgs::DocRoutine {
                doc_id: doc.into(),
                branch_path: "main".into(),
                heads,
                invocation: RoutineInvocation::Command,
                changed_facet_keys: vec![],
                wflow_args_json: Some(
                    serde_json::json!({"await_message": await_message}).to_string(),
                ),
            })
        }

        async fn submit(
            &self,
            doc: &str,
            await_message: bool,
            dependencies: Vec<String>,
        ) -> Res<String> {
            self.node
                .rt
                .dispatch_no_gate(
                    "@daybook/test",
                    "report-full-command",
                    self.args(doc, await_message).await?,
                    vec![],
                    dependencies,
                )
                .await
        }

        async fn publish_b(&mut self) -> Res<()> {
            let daybook_types::manifest::RoutineImpl::Wflow { bundle, .. } =
                &self.manifest.routines["report-full-command"].r#impl;
            let bundle_name = bundle.0.clone();
            let component = &self.manifest.wflow_bundles[bundle_name.as_str()].component_urls[0];
            let blob: BlobId = component.path().trim_start_matches('/').parse()?;
            let mut bytes = self.node.blobs_repo.get_bytes(blob).await?;
            // This valid custom section makes B content-address distinct while
            // retaining the same SDK handler. B's minimal ACL changes its report.
            bytes.extend_from_slice(&[0, 10, 9]);
            bytes.extend_from_slice(b"capture-b");
            let file = self.temp.path().join("revision-b.wasm");
            tokio::fs::write(&file, bytes).await?;
            let bundle = Arc::make_mut(
                self.manifest
                    .wflow_bundles
                    .get_mut(bundle_name.as_str())
                    .unwrap(),
            );
            eyre::ensure!(
                bundle.component_urls.len() == 1,
                "fixture requires one component"
            );
            bundle.component_urls = vec![url::Url::from_file_path(&file).unwrap()];
            self.manifest.routines.insert(
                "report-full-command".into(),
                Arc::clone(&self.manifest.routines["report-minimal-command"]),
            );
            self.manifest.version.major += 1;
            self.node
                .drawer
                .update_at_heads(
                    DocPatch {
                        id: self.manifest_doc.clone(),
                        facets_set: [(
                            FacetKey::from(WellKnownFacetTag::PlugManifest),
                            WellKnownFacet::PlugManifest(self.manifest.clone()).into(),
                        )]
                        .into(),
                        facets_remove: vec![],
                        user_path: None,
                    },
                    BranchPath::new("main"),
                    None,
                )
                .await?;
            let reference: url::Url = format!(
                "db+facet:///{}/org.example.daybook.plugManifest/main?branch=main",
                self.manifest_doc
            )
            .parse()?;
            self.node.plugs_repo.enable_plug(&reference).await?;
            self.config_marker("late").await?;
            Ok(())
        }
    }

    fn job_id(attempt: &DispatchAttempt) -> &str {
        let ActiveDispatchDeets::Wflow { wflow_job_id, .. } = &attempt.deets;
        wflow_job_id.as_deref().expect("captured attempt has a job")
    }

    async fn message_wait(node: &SyncTestNode, job: &str) -> Res<()> {
        let log = PartitionLogRef::new(Arc::clone(&node.rt.wcx.logstore));
        let mut entries = log.tail(1);
        while let Some(entry) = entries.next().await {
            if let (_, Some(PartitionLogEntry::JobEffectResult(event))) = entry?
                && event.job_id.as_ref() == job
                && matches!(event.result, JobRunResult::StepWait(wait)
                    if matches!(wait.deets, JobWaitResultDeets::Message { .. }))
            {
                return Ok(());
            }
        }
        eyre::bail!("workflow log closed before the real message wait")
    }

    async fn admissions(node: &SyncTestNode, job: &str) -> Res<Vec<u64>> {
        let latest = node.rt.wcx.logstore.latest_idx().await?;
        if latest == 0 {
            return Ok(vec![]);
        }
        let log = PartitionLogRef::new(Arc::clone(&node.rt.wcx.logstore));
        let mut entries = log.tail(1);
        let mut result = vec![];
        while let Some(entry) = entries.next().await {
            let (index, entry) = entry?;
            if let Some(PartitionLogEntry::JobInit(init)) = entry
                && init.job_id.as_ref() == job
            {
                result.push(index);
            }
            if index >= latest {
                break;
            }
        }
        Ok(result)
    }

    async fn resume(node: &SyncTestNode, job: &str) -> Res<()> {
        node.rt
            .wflow_ingress
            .send_message(
                Arc::from(job),
                Arc::from(format!("resume-{job}")),
                "{\"resume\":true}".into(),
            )
            .await?;
        Ok(())
    }

    async fn report(node: &SyncTestNode, doc: &str) -> Res<Option<serde_json::Value>> {
        let file = node
            .rt
            .sqlite_local_state_repo
            .get_sqlite_file_path("@daybook/test/capability-report")
            .await?;
        let sql = sqlx_utils_rs::SqlCtx::url(&format!("sqlite://{}", file.display())).await?;
        let report: Option<String> =
            sqlx::query_scalar("SELECT summary_json FROM capability_report WHERE doc_id = ?")
                .bind(doc)
                .fetch_optional(&sql.read_pool)
                .await?;
        report
            .map(|report| serde_json::from_str(&report).map_err(Into::into))
            .transpose()
    }

    fn assert_a_report(report: &serde_json::Value) {
        let keys: Vec<Vec<String>> =
            serde_json::from_value(report["config_doc_facet_keys"].clone()).unwrap();
        assert!(
            keys.iter()
                .flatten()
                .any(|key| key == "org.example.test.config/early")
        );
        assert!(
            keys.iter()
                .flatten()
                .all(|key| key != "org.example.test.config/late")
        );
        assert_eq!(report["invocation"]["kind"], "Command");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn captured_archived_outcome_settles_after_disk_reopen() -> Res<()> {
        for artifact in ["present", "missing", "corrupt"] {
            let fixture = Fixture::open().await?;
            let doc = fixture.document("archived-a").await?;
            let dispatch_id = fixture.submit(&doc, true, vec![]).await?;
            let original = fixture
                .node
                .rt
                .dispatch_repo
                .get_any(&dispatch_id)
                .await
                .unwrap();
            let job = job_id(&original).to_owned();
            message_wait(&fixture.node, &job).await?;
            let admission = admissions(&fixture.node, &job).await?;
            assert_eq!(admission.len(), 1);
            let (reached, reached_rx) = tokio::sync::oneshot::channel();
            let (_resume, resume_rx) = tokio::sync::oneshot::channel();
            fixture.node.rt.pause_finalization(reached, resume_rx).await;
            resume(&fixture.node, &job).await?;
            reached_rx.await?;
            let terminal = {
                let mut changes = fixture.node.rt.wflow_part_state.change_receiver();
                loop {
                    let state = fixture.node.rt.wflow_part_state.read_jobs().await;
                    if let Some(archived) = state.archive.get(job.as_str()) {
                        assert_eq!(archived.init_entry_id, admission[0]);
                        break archived.runs.last().unwrap().result.clone();
                    }
                    drop(state);
                    changes.changed().await?;
                }
            };
            assert_eq!(
                fixture
                    .node
                    .rt
                    .dispatch_repo
                    .get_any(&dispatch_id)
                    .await
                    .unwrap()
                    .status,
                DispatchStatus::Active
            );
            assert_a_report(&report(&fixture.node, &doc).await?.unwrap());
            let root = fixture.temp.path().join("repo");
            let CapturedWflowExecution::V1 {
                component_blobs, ..
            } = &original.execution;
            let component = fixture
                .node
                .blobs_repo
                .get_path(component_blobs[0].clone())
                .await?;
            fixture.node.stop().await?;
            match artifact {
                "missing" => tokio::fs::remove_file(&component).await?,
                "corrupt" => tokio::fs::write(&component, b"not-the-captured-component").await?,
                _ => {}
            }
            let reopened = open_sync_node(&root).await?;
            reopened
                .rt
                .wait_for_dispatch_end(&dispatch_id, Duration::from_secs(120))
                .await?;
            let receipt = reopened
                .rt
                .dispatch_repo
                .get_any(&dispatch_id)
                .await
                .unwrap();
            assert_eq!(receipt.status, DispatchStatus::Succeeded);
            assert_eq!(job_id(&receipt), job);
            assert_eq!(
                serde_json::to_value(&receipt.execution)?,
                serde_json::to_value(&original.execution)?
            );
            assert_eq!(admissions(&reopened, &job).await?, admission);
            {
                let state = reopened.rt.wflow_part_state.read_jobs().await;
                let archived = state.archive.get(job.as_str()).unwrap();
                assert_eq!(archived.init_entry_id, admission[0]);
                assert_eq!(
                    serde_json::to_value(&archived.runs.last().unwrap().result)?,
                    serde_json::to_value(&terminal)?
                );
            }
            assert_a_report(&report(&reopened, &doc).await?.unwrap());
            reopened.stop().await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn captured_waiting_and_queued_wasm_resume_after_disk_reopen() -> Res<()> {
        let mut fixture = Fixture::open().await?;
        let queued_doc = fixture.document("queued-a").await?;
        let queued = fixture.submit(&queued_doc, true, vec![]).await?;
        let queued_a = fixture
            .node
            .rt
            .dispatch_repo
            .get_any(&queued)
            .await
            .unwrap();
        let queued_job = job_id(&queued_a).to_owned();
        message_wait(&fixture.node, &queued_job).await?;
        let waiting_doc = fixture.document("waiting-a").await?;
        let waiting = fixture
            .submit(&waiting_doc, false, vec![queued.clone()])
            .await?;
        let waiting_a = fixture
            .node
            .rt
            .dispatch_repo
            .get_any(&waiting)
            .await
            .unwrap();
        assert_eq!(waiting_a.status, DispatchStatus::Waiting);
        fixture.publish_b().await?;
        let b_doc = fixture.document("current-b").await?;
        let current = fixture.submit(&b_doc, false, vec![]).await?;
        fixture
            .node
            .rt
            .wait_for_dispatch_end(&current, Duration::from_secs(120))
            .await?;
        assert_eq!(
            report(&fixture.node, &b_doc).await?.unwrap()["config_doc_facet_keys"],
            serde_json::json!([])
        );
        let root = fixture.temp.path().join("repo");
        let original_admission = admissions(&fixture.node, &queued_job).await?;
        assert_eq!(original_admission.len(), 1);
        fixture.node.stop().await?;

        let reopened = open_sync_node(&root).await?;
        let recovered = reopened.rt.dispatch_repo.get_any(&queued).await.unwrap();
        assert_eq!(job_id(&recovered), queued_job);
        assert_eq!(
            serde_json::to_value(&recovered.execution)?,
            serde_json::to_value(&queued_a.execution)?
        );
        assert_eq!(
            serde_json::to_value(&recovered.args)?,
            serde_json::to_value(&queued_a.args)?
        );
        assert_eq!(
            admissions(&reopened, &queued_job).await?,
            original_admission
        );
        resume(&reopened, &queued_job).await?;
        reopened
            .rt
            .wait_for_dispatch_end(&queued, Duration::from_secs(120))
            .await?;
        reopened
            .rt
            .wait_for_dispatch_end(&waiting, Duration::from_secs(120))
            .await?;
        let recovered_waiting = reopened.rt.dispatch_repo.get_any(&waiting).await.unwrap();
        assert_eq!(recovered_waiting.status, DispatchStatus::Succeeded);
        assert_eq!(
            serde_json::to_value(&recovered_waiting.execution)?,
            serde_json::to_value(&waiting_a.execution)?
        );
        assert_eq!(
            serde_json::to_value(&recovered_waiting.args)?,
            serde_json::to_value(&waiting_a.args)?
        );
        assert_a_report(&report(&reopened, &queued_doc).await?.unwrap());
        assert_a_report(&report(&reopened, &waiting_doc).await?.unwrap());
        assert_eq!(
            admissions(&reopened, &queued_job).await?,
            original_admission
        );
        assert_eq!(
            admissions(&reopened, job_id(&recovered_waiting))
                .await?
                .len(),
            1
        );
        reopened.stop().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn captured_prepared_admission_recovers_both_crash_cuts() -> Res<()> {
        for deferred in [false, true] {
            for cut in [
                DispatchAdmissionCut::BeforeJobInit,
                DispatchAdmissionCut::AfterJobInit,
            ] {
                let mut fixture = Fixture::open().await?;
                let target = fixture.document("cut-a").await?;
                let (reached, reached_rx) = tokio::sync::oneshot::channel();
                let mut reached_rx = Some(reached_rx);
                let (resume_gate, resume_rx) = tokio::sync::oneshot::channel();
                let mut caller = None;
                let dispatch_id;
                if deferred {
                    let barrier_doc = fixture.document("real-dependency").await?;
                    let barrier = fixture.submit(&barrier_doc, true, vec![]).await?;
                    let barrier_attempt = fixture
                        .node
                        .rt
                        .dispatch_repo
                        .get_any(&barrier)
                        .await
                        .unwrap();
                    message_wait(&fixture.node, job_id(&barrier_attempt)).await?;
                    dispatch_id = fixture.submit(&target, true, vec![barrier]).await?;
                    fixture
                        .node
                        .rt
                        .pause_admission(cut, reached, resume_rx)
                        .await;
                    resume(&fixture.node, job_id(&barrier_attempt)).await?;
                } else {
                    fixture
                        .node
                        .rt
                        .pause_admission(cut, reached, resume_rx)
                        .await;
                    let rt = Arc::clone(&fixture.node.rt);
                    let args = fixture.args(&target, true).await?;
                    caller = Some(tokio::spawn(async move {
                        rt.dispatch_no_gate(
                            "@daybook/test",
                            "report-full-command",
                            args,
                            vec![],
                            vec![],
                        )
                        .await
                    }));
                    // Observe the durable source-owned row at the actual boundary,
                    // not a manufactured Active record.
                    reached_rx.take().unwrap().await?;
                    let attempts = fixture.node.rt.dispatch_repo.list_unsettled().await;
                    dispatch_id = attempts
                        .iter()
                        .find_map(|(id, attempt)| {
                            let ActiveDispatchArgs::FacetRoutine(args) = &attempt.args;
                            (args.doc_id == target).then(|| id.clone())
                        })
                        .ok_or_eyre("prepared dispatch not persisted")?;
                    caller.as_ref().unwrap().abort();
                }
                if deferred {
                    reached_rx.take().unwrap().await?;
                }
                if let Some(caller) = caller {
                    assert!(caller.await.unwrap_err().is_cancelled());
                }
                let prepared = fixture
                    .node
                    .rt
                    .dispatch_repo
                    .get_any(&dispatch_id)
                    .await
                    .unwrap();
                assert_eq!(prepared.status, DispatchStatus::Active);
                let ActiveDispatchDeets::Wflow { entry_id, .. } = &prepared.deets;
                assert_eq!(*entry_id, None);
                let retained_job = job_id(&prepared).to_owned();
                let before = admissions(&fixture.node, &retained_job).await?;
                assert_eq!(
                    before.len(),
                    usize::from(cut == DispatchAdmissionCut::AfterJobInit)
                );
                fixture.publish_b().await?;
                let root = fixture.temp.path().join("repo");
                // Deferred admission is owned by the real finalization worker.
                // Runtime stop cancels its test barrier before taking snapshots.
                fixture.node.stop().await?;
                drop(resume_gate);
                let reopened = open_sync_node(&root).await?;
                message_wait(&reopened, &retained_job).await?;
                let recovered = reopened
                    .rt
                    .dispatch_repo
                    .get_any(&dispatch_id)
                    .await
                    .unwrap();
                assert_eq!(job_id(&recovered), retained_job);
                assert_eq!(
                    serde_json::to_value(&recovered.execution)?,
                    serde_json::to_value(&prepared.execution)?
                );
                let entries = admissions(&reopened, &retained_job).await?;
                assert_eq!(entries.len(), 1);
                if !before.is_empty() {
                    assert_eq!(entries, before);
                }
                let jobs = reopened.rt.wflow_part_state.read_jobs().await;
                assert_eq!(jobs.active[retained_job.as_str()].init_entry_id, entries[0]);
                drop(jobs);
                resume(&reopened, &retained_job).await?;
                reopened
                    .rt
                    .wait_for_dispatch_end(&dispatch_id, Duration::from_secs(120))
                    .await?;
                assert_eq!(
                    reopened
                        .rt
                        .dispatch_repo
                        .get_any(&dispatch_id)
                        .await
                        .unwrap()
                        .status,
                    DispatchStatus::Succeeded
                );
                assert_a_report(&report(&reopened, &target).await?.unwrap());
                assert_eq!(admissions(&reopened, &retained_job).await?, entries);
                reopened.stop().await?;
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn captured_blob_loss_or_corruption_fails_closed_on_disk_reopen() -> Res<()> {
        for corrupt in [false, true] {
            let mut fixture = Fixture::open().await?;
            let a_doc = fixture.document("unavailable-a").await?;
            let a_id = fixture.submit(&a_doc, true, vec![]).await?;
            let attempt = fixture.node.rt.dispatch_repo.get_any(&a_id).await.unwrap();
            let retained_job = job_id(&attempt).to_owned();
            message_wait(&fixture.node, &retained_job).await?;
            let CapturedWflowExecution::V1 {
                component_blobs, ..
            } = &attempt.execution;
            let component = fixture
                .node
                .blobs_repo
                .get_path(component_blobs[0].clone())
                .await?;
            let original = tokio::fs::read(&component).await?;
            fixture.publish_b().await?;
            let b_doc = fixture.document("available-b").await?;
            let b_id = fixture.submit(&b_doc, false, vec![]).await?;
            fixture
                .node
                .rt
                .wait_for_dispatch_end(&b_id, Duration::from_secs(120))
                .await?;
            assert_eq!(
                report(&fixture.node, &b_doc).await?.unwrap()["config_doc_facet_keys"],
                serde_json::json!([])
            );
            let root = fixture.temp.path().join("repo");
            fixture.node.stop().await?;
            if corrupt {
                tokio::fs::write(&component, b"not-the-captured-component").await?;
            } else {
                tokio::fs::remove_file(&component).await?;
            }
            let error = match open_sync_node(&root).await {
                Ok(node) => {
                    node.stop().await?;
                    eyre::bail!("unavailable captured A must not execute available B");
                }
                Err(error) => error,
            };
            assert!(format!("{error:?}").contains("cannot restore captured execution"));
            if corrupt {
                assert!(format!("{error:?}").contains("incorrect content"));
            } else {
                assert!(format!("{error:?}").contains("Blob not found"));
            }
            // Restoring bytes allows a whole-node reopen to observe that the
            // failed startup settled A, rather than stranding it Active.
            tokio::fs::write(&component, original).await?;
            let reopened = open_sync_node(&root).await?;
            assert_eq!(
                reopened
                    .rt
                    .dispatch_repo
                    .get_any(&a_id)
                    .await
                    .unwrap()
                    .status,
                DispatchStatus::Failed
            );
            assert_eq!(
                reopened
                    .rt
                    .dispatch_repo
                    .get_any(&b_id)
                    .await
                    .unwrap()
                    .status,
                DispatchStatus::Succeeded
            );
            assert!(report(&reopened, &a_doc).await?.is_none());
            assert_eq!(admissions(&reopened, &retained_job).await?.len(), 1);
            reopened.stop().await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_only_executor_command_settles_captured_effects() -> Res<()> {
        native_command_scenario(/*restart_executor*/ false).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_command_survives_producer_disconnect_and_executor_restart() -> Res<()> {
        native_command_scenario(/*restart_executor*/ true).await
    }

    async fn native_command_scenario(restart_executor: bool) -> Res<()> {
        use crate::blobs::encrypt::{JwkOct, MasterKey};
        use crate::rt::task_adapter::{COMMAND_DOMAIN, CapturedRoutineAdapter};
        use crate::tasks::driver::SchedulingTiming;
        use crate::tasks::storage::PinnedJwk;
        use crate::tasks::*;
        use automerge::transaction::Transactable;
        use big_repo::keyhive_core::access::Access;
        use big_sync::HostPartStore;
        use big_sync_core::revisioned_store::RevisionReadLimits;
        use big_sync_core::rpc::{SubPartsRequest, SubscriptionTarget};
        use big_sync_core::{ObjKey, PartKey};

        eprintln!("native command: opening producer; restart={restart_executor}");
        let fixture = Fixture::open().await?;
        let doc = fixture.document("native-distributed-command").await?;
        let a = &fixture.node;
        let b_root = fixture.temp.path().join("executor");
        let ticket = a.sync_repo.get_clone_ticket_url().await?;
        eprintln!("native command: cloning executor");
        bootstrap_clone_repo_from_url_for_tests(&ticket, &b_root).await?;
        let mut b = open_sync_node(&b_root).await?;
        let endpoint = b.sync_repo.connect_url(&ticket).await?;
        eprintln!("native command: converging source inputs");
        wait_for_sync_convergence(a, &b, endpoint.id).await?;
        let a_peer = a.ctx.big_repo.local_peer_id();
        let b_peer = b.ctx.big_repo.local_peer_id();
        let b_agent = a
            .ctx
            .big_repo
            .receive_keyhive_contact_card(&b.ctx.big_repo.local_keyhive_contact_card())
            .await?;
        let group = a.ctx.big_repo.create_group_with_parents(vec![]).await?;
        a.ctx
            .big_repo
            .add_admin_member_to_group(a.ctx.big_repo.local_keyhive_agent().await?, &group)
            .await?;
        a.ctx
            .big_repo
            .add_member_to_group(b_agent, &group, Access::Edit)
            .await?;
        let key = MasterKey::random();
        let key_facet = FacetKey::from(WellKnownFacetTag::Jwk);
        let mut metadata_doc = automerge::Automerge::new();
        let mut tx = metadata_doc.transaction();
        let facets = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;
        autosurgeon::reconcile_prop(
            &mut tx,
            &facets,
            autosurgeon::Prop::Key(key_facet.to_string().into()),
            am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(&key))?),
        )?;
        tx.commit();
        let metadata = a
            .ctx
            .big_repo
            .create_doc_with_parents(metadata_doc, vec![group.clone().into()])
            .await?;
        let heads = metadata
            .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
            .await;
        let key_ref =
            daybook_types::url::build_facet_ref(&metadata.document_id().to_string(), &key_facet)?;
        let pool = TaskPoolId::from_label("native-captured-command");
        let descriptor = TaskPoolDescriptor {
            pool_id: pool.clone(),
            authority_group: group.id().to_bytes(),
            active_task_part: PartKey::new(b"native-captured-command/tasks"),
            register_scope: b"native-captured-command/register".to_vec(),
            register_incarnation: [91; 32],
            allowed_key_refs: [key_ref.clone()].into(),
            publication_key: PinnedJwk { key_ref, heads },
            archive_part: None,
            router_slot: ObjKey::new(b"native-captured-command/router"),
            router_heartbeat_topic: big_repo::BigEphemeralTopic::new([92; 32]),
            allowed_rpc_transports: [RpcTransport::IrpcIroh].into(),
            retention_class: RetentionClass::UntilAuthoritativeRemoval,
            routing_defaults: RoutingDefaults::SharedElection,
        };
        let a_pools = PoolRepo::new(
            Arc::clone(&a.ctx.big_repo),
            a.ctx.doc_config.document_id(),
            automerge::ActorId::from(b"command-provisioner".as_slice()),
        );
        let reference = a_pools
            .register(metadata.document_id(), descriptor.clone())
            .await?;
        a.ctx.big_repo.wait_for_quiescence(None).await?;
        b.ctx
            .big_repo
            .sync_keyhive_with_peer(a_peer.clone())
            .await?;
        b.ctx
            .big_repo
            .sync_doc_with_peer(a.ctx.doc_config.document_id(), a_peer.clone())
            .await?;
        b.ctx
            .big_repo
            .sync_doc_with_peer(metadata.document_id(), a_peer)
            .await?;
        b.ctx.big_repo.wait_for_quiescence(None).await?;
        let b_pools = PoolRepo::new(
            Arc::clone(&b.ctx.big_repo),
            b.ctx.doc_config.document_id(),
            automerge::ActorId::from(b"command-executor".as_slice()),
        );
        let PoolDiscovery::Ready(a_snapshot) = a_pools
            .load_descriptor(&reference, &pool, descriptor.authority_group)
            .await?
        else {
            eyre::bail!("producer pool is not ready");
        };
        let PoolDiscovery::Ready(b_snapshot) = b_pools
            .load_descriptor(&reference, &pool, descriptor.authority_group)
            .await?
        else {
            eyre::bail!("executor pool is not ready");
        };
        let a_adapter = Arc::new(CapturedRoutineAdapter::new(
            Arc::clone(&a.rt),
            a_snapshot.clone(),
            a.sync_repo.attach_task_pool(a_snapshot.clone()).await?,
        ));
        let b_adapter = Arc::new(CapturedRoutineAdapter::new(
            Arc::clone(&b.rt),
            b_snapshot.clone(),
            b.sync_repo.attach_task_pool(b_snapshot.clone()).await?,
        ));
        let captured = a_adapter
            .capture(
                "@daybook/test",
                "report-full-command",
                fixture.args(&doc, /*await_message*/ true).await?,
            )
            .await?;
        let timing = SchedulingTiming {
            heartbeat_interval: Duration::from_millis(100),
            router_settle: Duration::from_millis(100),
            router_takeover_after: Duration::from_secs(1),
        };
        eprintln!("native command: attaching scheduling actors");
        let a_driver = a
            .sync_repo
            .attach_scheduling_pool(
                a_snapshot,
                a_adapter,
                CapabilitySummary::default(),
                Capacity::new(1),
                timing,
            )
            .await?;
        let _b_driver = b
            .sync_repo
            .attach_scheduling_pool(
                b_snapshot.clone(),
                b_adapter,
                CapabilitySummary::default(),
                Capacity::new(1),
                timing,
            )
            .await?;
        let task_id = PoolTaskId::new(*blake3::hash(b"native-only-executor-command").as_bytes());
        let declaration = TaskDeclaration {
            task_id,
            pool: pool.clone(),
            domain: DomainId::from_label(COMMAND_DOMAIN),
            producer: Some(NodePubkey::new(
                a.ctx.big_repo.local_peer_id().to_bytes32()?,
            )),
            handler: HandlerRef::from_label(COMMAND_DOMAIN),
            input: serde_json::to_vec(&captured)?,
            capabilities: CapabilitySummary::default(),
            coordination_ref: None,
            placement: Preference::Only(NodePubkey::new(b_peer.to_bytes32()?)),
            effect_policy: EffectPolicy::AuthoritativePlacement,
            not_before_secs: None,
            not_after_secs: None,
            result_retention: ResultRetention::TicketAuthoritative { retain_until: None },
        };
        let digest = declaration.canonical_digest();
        let dispatches = b.rt.dispatch_repo.subscribe(SubscribeOpts::new(64));
        let mut revisions = b
            .sync_repo
            .task_backend()
            .shared_store()
            .open_revision_reader(SubPartsRequest {
                lower_bound: 0,
                targets: [SubscriptionTarget::Part {
                    part_id: descriptor.active_task_part.clone(),
                    cursor: 0,
                }]
                .into(),
            })
            .await?
            .map_err(|error| ferr!("command observer: {error:?}"))?;
        eprintln!("native command: publishing captured declaration");
        a_driver.submit(declaration, Vec::new()).await?;
        eprintln!("native command: declaration published");
        let attempt = loop {
            let attempts = b.rt.dispatch_repo.task_attempts(&pool).await?;
            if let Some(attempt) = attempts
                .into_iter()
                .find(|attempt| attempt.request.key.task_id == task_id && attempt.job_id.is_some())
            {
                break attempt;
            }
            match dispatches.recv_async().await {
                Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}
                Err(crate::repos::RecvError::Closed) => {
                    eyre::bail!("command dispatch observer closed")
                }
            }
        };
        let job = attempt.job_id.as_deref().unwrap();
        eprintln!("native command: awaiting guest message wait; job={job}");
        message_wait(&b, job).await?;
        eprintln!("native command: guest waiting");
        assert!(a.rt.dispatch_repo.task_attempts(&pool).await?.is_empty());
        assert_eq!(admissions(&b, job).await?.len(), 1);
        assert!(
            b.rt.dispatch_repo
                .task_receipt(&task_id, digest)
                .await?
                .is_none()
        );
        let producer = if restart_executor {
            eprintln!("native command: changing config, disconnecting and reopening");
            fixture.config_marker("late").await?;
            wait_for_sync_convergence(a, &b, endpoint.id).await?;
            fixture.node.stop().await?;
            b.stop().await?;
            b = open_sync_node(&b_root).await?;
            let adapter = Arc::new(CapturedRoutineAdapter::new(
                Arc::clone(&b.rt),
                b_snapshot.clone(),
                b.sync_repo.attach_task_pool(b_snapshot.clone()).await?,
            ));
            let _driver = b
                .sync_repo
                .attach_scheduling_pool(
                    b_snapshot,
                    adapter,
                    CapabilitySummary::default(),
                    Capacity::new(1),
                    timing,
                )
                .await?;
            let retained = b.rt.dispatch_repo.task_attempts(&pool).await?;
            assert_eq!(retained.len(), 1);
            assert_eq!(retained[0].request.key, attempt.request.key);
            assert_eq!(retained[0].dispatch_id, attempt.dispatch_id);
            assert_eq!(retained[0].job_id, attempt.job_id);
            assert_eq!(retained[0].staging_id, attempt.staging_id);
            assert_eq!(retained[0].capture_digest, attempt.capture_digest);
            revisions = b
                .sync_repo
                .task_backend()
                .shared_store()
                .open_revision_reader(SubPartsRequest {
                    lower_bound: 0,
                    targets: [SubscriptionTarget::Part {
                        part_id: descriptor.active_task_part,
                        cursor: 0,
                    }]
                    .into(),
                })
                .await?
                .map_err(|error| ferr!("reopened command observer: {error:?}"))?;
            None
        } else {
            Some(fixture.node)
        };
        eprintln!("native command: resuming retained guest");
        resume(&b, job).await?;
        let tasks = b.sync_repo.task_store(&pool).unwrap();
        eprintln!("native command: awaiting signed terminal");
        let terminal = loop {
            if let Some(ticket) = tasks.ticket(task_id).await?
                && let Some(summary) = ticket.terminal_summary()
            {
                break summary;
            }
            revisions.next(RevisionReadLimits::default()).await?;
        };
        assert!(matches!(terminal, TerminalSummary::Succeeded { .. }));
        let receipt =
            b.rt.dispatch_repo
                .task_receipt(&task_id, digest)
                .await?
                .unwrap();
        assert_eq!(receipt.summary, terminal);
        assert_a_report(&report(&b, &doc).await?.unwrap());
        assert_eq!(admissions(&b, job).await?.len(), 1);
        b.stop().await?;
        if let Some(producer) = producer {
            producer.stop().await?;
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn native_distributed_triage_incorporates_exact_capture_before_terminal() -> Res<()> {
        native_triage_scenario(/*remote_executor*/ false).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_remote_triage_incorporates_exact_capture_before_terminal() -> Res<()> {
        native_triage_scenario(/*remote_executor*/ true).await
    }

    async fn native_triage_scenario(remote_executor: bool) -> Res<()> {
        utils_rs::testing::setup_tracing_once();
        use crate::blobs::encrypt::{JwkOct, MasterKey};
        use crate::rt::triage::domain::*;
        use crate::tasks::driver::SchedulingTiming;
        use crate::tasks::storage::PinnedJwk;
        use crate::tasks::*;
        use automerge::transaction::Transactable;
        use big_sync::HostPartStore as _;
        use big_sync_core::revisioned_store::RevisionReadLimits;
        use big_sync_core::rpc::{SubPartsRequest, SubscriptionTarget};
        use big_sync_core::{ObjKey, PartKey};

        eprintln!("native triage: opening real disk/runtime fixture");
        let mut fixture = Fixture::open().await?;
        let executor = if remote_executor {
            let ticket = fixture.node.sync_repo.get_clone_ticket_url().await?;
            let root = fixture.temp.path().join("triage-executor");
            bootstrap_clone_repo_from_url_for_tests(&ticket, &root).await?;
            let executor = open_sync_node(&root).await?;
            let endpoint = executor.sync_repo.connect_url(&ticket).await?;
            wait_for_sync_convergence(&fixture.node, &executor, endpoint.id).await?;
            Some(executor)
        } else {
            None
        };
        fixture.manifest.version.major += 1;
        eprintln!("native triage: replacing processor policy and awaiting activation");
        fixture
            .manifest
            .processors
            .retain(|name, _| name.as_str() == "test-label");
        if !remote_executor {
            // The guest fixture normally stops matching once it writes a label.
            // This scenario must remain eligible after subsequent source edits.
            let daybook_types::manifest::ProcessorDeets::DocProcessor { predicate, .. } =
                &mut Arc::make_mut(fixture.manifest.processors.get_mut("test-label").unwrap())
                    .deets;
            *predicate =
                daybook_types::manifest::DocPredicateClause::HasTag(WellKnownFacetTag::Note.into());
            // LabelGeneric is an output, not a source dependency: reading it
            // would intentionally trigger another run after every label write.
            Arc::make_mut(fixture.manifest.routines.get_mut("test-label").unwrap()).doc_acls[0]
                .facet_acl[0]
                .read = false;
        }
        Arc::make_mut(fixture.manifest.processors.get_mut("test-label").unwrap()).coordination =
            daybook_types::manifest::ProcessorCoordination::Distributed(
                daybook_types::manifest::DistributedProcessorPolicy {
                    placement: executor.as_ref().map_or(
                        daybook_types::manifest::ProcessorPlacement::AnyNode,
                        |executor| {
                            daybook_types::manifest::ProcessorPlacement::Only(
                                iroh::PublicKey::from_bytes(
                                    &executor.ctx.big_repo.local_peer_id().to_bytes32().unwrap(),
                                )
                                .unwrap()
                                .to_string(),
                            )
                        },
                    ),
                    duplicates: daybook_types::manifest::ProcessorDuplicatePolicy::Idempotent,
                },
            );
        let node = &fixture.node;
        node.drawer
            .update_at_heads(
                DocPatch {
                    id: fixture.manifest_doc.clone(),
                    facets_set: [(
                        FacetKey::from(WellKnownFacetTag::PlugManifest),
                        WellKnownFacet::PlugManifest(fixture.manifest.clone()).into(),
                    )]
                    .into(),
                    facets_remove: vec![],
                    user_path: None,
                },
                BranchPath::new("main"),
                None,
            )
            .await?;
        let manifest_ref = format!(
            "db+facet:///{}/org.example.daybook.plugManifest/main?branch=main",
            fixture.manifest_doc
        )
        .parse()?;
        let activation = node.plugs_repo.enable_plug(&manifest_ref).await?;
        assert!(matches!(
            node.rt
                .triage_worker
                .wait_for_activation(&node.plugs_repo, activation)
                .await?,
            crate::rt::triage::ActivationStatus::Active(_)
        ));

        let group = node.ctx.big_repo.create_group_with_parents(vec![]).await?;
        node.ctx
            .big_repo
            .add_admin_member_to_group(node.ctx.big_repo.local_keyhive_agent().await?, &group)
            .await?;
        if let Some(executor) = &executor {
            let agent = node
                .ctx
                .big_repo
                .receive_keyhive_contact_card(&executor.ctx.big_repo.local_keyhive_contact_card())
                .await?;
            node.ctx
                .big_repo
                .add_member_to_group(agent, &group, big_repo::keyhive_core::access::Access::Edit)
                .await?;
        }
        let mut seed = automerge::Automerge::new();
        let mut tx = seed.transaction();
        let facets = tx.put_object(automerge::ROOT, "facets", automerge::ObjType::Map)?;
        let key_facet = FacetKey::from(WellKnownFacetTag::Jwk);
        autosurgeon::reconcile_prop(
            &mut tx,
            &facets,
            autosurgeon::Prop::Key(key_facet.to_string().into()),
            am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(
                &MasterKey::random(),
            ))?),
        )?;
        tx.commit();
        let pool_doc = node
            .ctx
            .big_repo
            .create_doc_with_parents(seed.clone(), vec![group.clone().into()])
            .await?;
        let mut tx = seed.transaction();
        autosurgeon::reconcile_prop(
            &mut tx,
            &facets,
            autosurgeon::Prop::Key(key_facet.to_string().into()),
            am_utils_rs::codecs::ThroughJson(serde_json::to_value(JwkOct::from_master_key(
                &MasterKey::random(),
            ))?),
        )?;
        tx.commit();
        let domain_doc = node
            .ctx
            .big_repo
            .create_doc_with_parents(seed, vec![group.clone().into()])
            .await?;
        let pinned = |document: String, heads| -> Res<PinnedJwk> {
            Ok(PinnedJwk {
                key_ref: daybook_types::url::build_facet_ref(&document, &key_facet)?,
                heads,
            })
        };
        let pool_key = pinned(
            pool_doc.document_id().to_string(),
            pool_doc
                .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                .await,
        )?;
        let pool_id = TaskPoolId::from_label("native-triage");
        let descriptor = TaskPoolDescriptor {
            pool_id: pool_id.clone(),
            authority_group: group.id().to_bytes(),
            active_task_part: PartKey::new(b"native-triage/tasks"),
            register_scope: b"native-triage/register".to_vec(),
            register_incarnation: [81; 32],
            allowed_key_refs: [pool_key.key_ref.clone()].into(),
            publication_key: pool_key,
            archive_part: None,
            router_slot: ObjKey::new(b"native-triage/router"),
            router_heartbeat_topic: big_repo::BigEphemeralTopic::new([82; 32]),
            allowed_rpc_transports: [RpcTransport::IrpcIroh].into(),
            retention_class: RetentionClass::UntilAuthoritativeRemoval,
            routing_defaults: RoutingDefaults::SharedElection,
        };
        let pools = PoolRepo::new(
            Arc::clone(&node.ctx.big_repo),
            node.ctx.doc_config.document_id(),
            node.ctx.local_actor_id.clone(),
        );
        let pool_ref = pools
            .register(pool_doc.document_id(), descriptor.clone())
            .await?;
        let domain_ref = ProcessorDomainReference {
            document: domain_doc.document_id(),
            authority_group: group.id().to_bytes(),
        };
        let domain_key = pinned(
            domain_ref.document.to_string(),
            domain_doc
                .with_document_read(|doc| ChangeHashSet(doc.get_heads().into()))
                .await,
        )?;
        let processor = "@daybook/test/test-label";
        ProcessorDomainRepo::new(
            Arc::clone(&node.ctx.big_repo),
            node.ctx.local_actor_id.clone(),
        )
        .provision(&domain_ref, processor.into(), pool_ref.clone(), domain_key)
        .await?;
        let PoolDiscovery::Ready(snapshot) = pools
            .load_descriptor(&pool_ref, &pool_id, group.id().to_bytes())
            .await?
        else {
            eyre::bail!("native pool unavailable");
        };
        eprintln!("native triage: attaching native slot and task actors");
        assert!(
            node.sync_repo
                .attach_distributed_processor(
                    Arc::clone(&node.rt),
                    processor.into(),
                    domain_ref.clone(),
                    snapshot,
                    CapabilitySummary::empty(),
                    Capacity::new(1),
                    SchedulingTiming {
                        heartbeat_interval: Duration::from_millis(100),
                        router_settle: Duration::from_millis(100),
                        router_takeover_after: Duration::from_secs(1)
                    }
                )
                .await?
        );
        if let Some(executor) = &executor {
            node.ctx.big_repo.wait_for_quiescence(None).await?;
            let peer = node.ctx.big_repo.local_peer_id();
            executor
                .ctx
                .big_repo
                .sync_keyhive_with_peer(peer.clone())
                .await?;
            for document in [
                node.ctx.doc_config.document_id(),
                fixture.manifest_doc.parse()?,
                pool_doc.document_id(),
                domain_ref.document.clone(),
            ] {
                executor
                    .ctx
                    .big_repo
                    .sync_doc_with_peer(document, peer.clone())
                    .await?;
            }
            executor.ctx.big_repo.wait_for_quiescence(None).await?;
            let pools = PoolRepo::new(
                Arc::clone(&executor.ctx.big_repo),
                executor.ctx.doc_config.document_id(),
                executor.ctx.local_actor_id.clone(),
            );
            let PoolDiscovery::Ready(snapshot) = pools
                .load_descriptor(&pool_ref, &pool_id, group.id().to_bytes())
                .await?
            else {
                eyre::bail!("remote processor pool unavailable");
            };
            assert!(
                executor
                    .sync_repo
                    .attach_distributed_processor(
                        Arc::clone(&executor.rt),
                        processor.into(),
                        domain_ref.clone(),
                        snapshot,
                        CapabilitySummary::empty(),
                        Capacity::new(1),
                        SchedulingTiming {
                            heartbeat_interval: Duration::from_millis(100),
                            router_settle: Duration::from_millis(100),
                            router_takeover_after: Duration::from_secs(1),
                        },
                    )
                    .await?
            );
        }
        let mut revisions = node
            .sync_repo
            .task_backend()
            .shared_store()
            .open_revision_reader(SubPartsRequest {
                lower_bound: 0,
                targets: [SubscriptionTarget::Part {
                    part_id: descriptor.active_task_part,
                    cursor: 0,
                }]
                .into(),
            })
            .await?
            .map_err(|error| ferr!("triage observer: {error:?}"))?;
        let doc = node
            .drawer
            .add(AddDocArgs {
                branch_path: "main".into(),
                facets: [(
                    FacetKey::from(WellKnownFacetTag::Note),
                    WellKnownFacet::Note(daybook_types::doc::Note {
                        mime: "text/plain".into(),
                        content: "distributed triage".into(),
                    })
                    .into(),
                )]
                .into(),
                user_path: None,
            })
            .await?;
        if let Some(executor) = &executor {
            node.ctx.big_repo.wait_for_quiescence(None).await?;
            executor
                .ctx
                .big_repo
                .sync_keyhive_with_peer(node.ctx.big_repo.local_peer_id())
                .await?;
            executor
                .ctx
                .big_repo
                .sync_doc_with_peer(doc.parse()?, node.ctx.big_repo.local_peer_id())
                .await?;
        }
        let executing = executor.as_ref().unwrap_or(node);
        let tasks = node.sync_repo.task_store(&pool_id).unwrap();
        eprintln!("native triage: observing durable task preparation");
        let dispatch_events = executing.rt.dispatch_repo.subscribe(SubscribeOpts::new(64));
        let mut merged_target = None;
        let mut preceding_generation = None;
        let attempt = loop {
            let attempt = loop {
                let attempts = executing.rt.dispatch_repo.task_attempts(&pool_id).await?;
                if let Some(attempt) = attempts.into_iter().find(|attempt| matches!(&serde_json::from_slice::<crate::rt::task_adapter::ResolvedRoutineInput>(&attempt.request.invocation.args).unwrap().input,
                crate::rt::task_adapter::CapturedRoutineInput::V1 { processor: Some(captured), .. } if captured.slot.document_id == doc && captured.slot.branch_path.as_str() == "main" && merged_target.as_ref().is_none_or(|heads| &captured.capture.heads == heads))) { break attempt; }
                tokio::select! {
                    result = revisions.next(RevisionReadLimits::default()) => { result?; }
                    result = dispatch_events.recv_async() => {
                        match result {
                            Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}
                            Err(crate::repos::RecvError::Closed) => eyre::bail!("native triage dispatch observer closed"),
                        }
                    }
                }
            };
            eprintln!("native triage: observing terminal after native effects");
            let terminal = loop {
                if let Some(ticket) = tasks.ticket(attempt.request.key.task_id).await?
                    && let Some(summary) = ticket.terminal_summary()
                {
                    break summary;
                }
                let dispatch = executing
                    .rt
                    .dispatch_repo
                    .get_any(&attempt.dispatch_id)
                    .await
                    .unwrap();
                eyre::ensure!(
                    dispatch.status != crate::rt::dispatch::DispatchStatus::Failed,
                    "native processor attempt failed: {:?}",
                    executing
                        .rt
                        .dispatch_repo
                        .task_local_failure(&attempt.request.key)
                        .await?
                );
                tokio::select! {
                    result = revisions.next(RevisionReadLimits::default()) => { result?; }
                    result = dispatch_events.recv_async() => {
                        match result {
                            Ok(_) | Err(crate::repos::RecvError::Dropped { .. }) => {}
                            Err(crate::repos::RecvError::Closed) => eyre::bail!("native triage dispatch observer closed"),
                        }
                    }
                }
            };
            assert!(matches!(terminal, TerminalSummary::Succeeded { .. }));
            let crate::rt::task_adapter::CapturedRoutineInput::V1 {
                processor: Some(captured),
                ..
            } = serde_json::from_slice::<crate::rt::task_adapter::ResolvedRoutineInput>(
                &attempt.request.invocation.args,
            )?
            .input
            else {
                unreachable!()
            };
            if let Some(previous) = preceding_generation {
                assert_ne!(captured.capture.generation, previous);
            }
            let store = node
                .rt
                .processor_slot_store(processor, Some(&domain_ref))
                .await?
                .unwrap();
            let settled = store.slot(&captured.slot).await?;
            assert!(settled.settled(&captured.capture.generation));
            assert_eq!(
                settled
                    .settlements()
                    .find(|settlement| settlement.capture.generation == captured.capture.generation)
                    .unwrap()
                    .capture,
                captured.capture
            );
            let actual = node
                .drawer
                .get_doc_with_facets_at_branch_heads(
                    &doc,
                    BranchPath::new("main"),
                    &node
                        .drawer
                        .get_branch_heads_for_path(&doc, BranchPath::new("main"))
                        .await?
                        .unwrap(),
                    None,
                )
                .await?
                .unwrap();
            let expected: daybook_types::doc::FacetRaw =
                WellKnownFacet::LabelGeneric("test_label".into()).into();
            assert_eq!(
                actual.facets[&FacetKey::from(WellKnownFacetTag::LabelGeneric)],
                expected
            );
            if !remote_executor && merged_target.is_none() {
                // Keep the branch-edit fixture out of the already large native
                // runtime future so Tokio's test thread does not overflow its stack.
                let heads = Box::pin(async {
                    let main = BranchPath::new("main");
                    let base = node
                        .drawer
                        .get_branch_heads_for_path(&doc, main)
                        .await?
                        .unwrap();
                    let left = BranchPath::new("/tmp/triage-left");
                    let right = BranchPath::new("/tmp/triage-right");
                    for branch in [left, right] {
                        node.drawer
                            .create_branch_at_heads_from_branch(&doc, branch, main, &base, None)
                            .await?;
                    }
                    for (branch, content) in
                        [(left, "concurrent left"), (right, "concurrent right")]
                    {
                        node.drawer
                            .update_at_heads(
                                DocPatch {
                                    id: doc.clone(),
                                    facets_set: [(
                                        FacetKey::from(WellKnownFacetTag::Note),
                                        WellKnownFacet::Note(daybook_types::doc::Note {
                                            mime: "text/plain".into(),
                                            content: content.into(),
                                        })
                                        .into(),
                                    )]
                                    .into(),
                                    facets_remove: vec![],
                                    user_path: None,
                                },
                                branch,
                                None,
                            )
                            .await?;
                    }
                    node.drawer
                        .merge_from_branch(&doc, right, left, None)
                        .await?;
                    node.drawer
                        .merge_from_branch(&doc, main, right, None)
                        .await?;
                    let heads = node
                        .drawer
                        .get_branch_heads_for_path(&doc, main)
                        .await?
                        .unwrap();
                    eyre::Ok(heads)
                })
                .await?;
                assert_ne!(heads, captured.capture.heads);
                preceding_generation = Some(captured.capture.generation);
                merged_target = Some(heads);
                continue;
            }
            break attempt;
        };
        if let Some(executor) = executor {
            assert!(
                node.rt
                    .dispatch_repo
                    .task_attempts(&pool_id)
                    .await?
                    .is_empty()
            );
            assert_eq!(
                admissions(&executor, attempt.job_id.as_deref().unwrap())
                    .await?
                    .len(),
                1
            );
            executor.stop().await?;
        }
        fixture.node.stop().await?;
        Ok(())
    }
}
