use super::*;

async fn boot_connected_sync_pair() -> Res<(tempfile::TempDir, SyncTestNode, SyncTestNode, EndpointId)> {
    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let rtx = RepoCtx::init(&repo_a_path, RepoOpenOptions {}, "test-device".into()).await?;
    rtx.shutdown().await?;
    drop(rtx);

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let bootstrap_a = node_a.sync_repo.current_bootstrap_state().await;
    node_b
        .sync_repo
        .connect_endpoint_addr(bootstrap_a.endpoint_addr.clone())
        .await?;
    wait_for_sync_convergence(
        &node_a,
        &node_b,
        bootstrap_a.endpoint_id,
        Duration::from_secs(20),
    )
    .await?;
    Ok((temp_root, node_a, node_b, bootstrap_a.endpoint_id))
}

async fn update_title_at_main_branch(
    node: &SyncTestNode,
    doc_id: &String,
    title: &str,
) -> Res<()> {
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let branch = daybook_types::doc::BranchPath::from("main");
    let Some((_, heads)) = node.drawer.get_with_heads(doc_id, &branch, None).await? else {
        eyre::bail!("missing doc while updating title: {doc_id}");
    };
    node.drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.to_string(),
                facets_set: [(
                    title_key,
                    WellKnownFacet::TitleGeneric(title.into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(daybook_types::doc::UserPath::from(
                    node.ctx.local_user_path.clone(),
                )),
            },
            branch,
            Some(heads),
        )
        .await?;
    Ok(())
}

async fn update_title_at_heads(
    node: &SyncTestNode,
    doc_id: &String,
    heads: &ChangeHashSet,
    title: &str,
) -> Res<()> {
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    node.drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    title_key,
                    WellKnownFacet::TitleGeneric(title.into()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: Some(daybook_types::doc::UserPath::from(
                    node.ctx.local_user_path.clone(),
                )),
            },
            daybook_types::doc::BranchPath::from("main"),
            Some(heads.clone()),
        )
        .await?;
    Ok(())
}

async fn update_note_at_heads(
    node: &SyncTestNode,
    doc_id: &String,
    heads: &ChangeHashSet,
    note: &str,
) -> Res<()> {
    let note_key = FacetKey::from(WellKnownFacetTag::Note);
    node.drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.clone(),
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
                user_path: Some(daybook_types::doc::UserPath::from(
                    node.ctx.local_user_path.clone(),
                )),
            },
            daybook_types::doc::BranchPath::from("main"),
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
    let branch = daybook_types::doc::BranchPath::from("main");
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
    let branch = daybook_types::doc::BranchPath::from("main");
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
        Some(&serde_json::Value::from(WellKnownFacet::Note(daybook_types::doc::Note {
            mime: "text/plain".into(),
            content: expected_note.into(),
        }))),
    );
    Ok(())
}

async fn wait_for_synced_doc_on_both_sides(
    left: &SyncTestNode,
    right: &SyncTestNode,
    doc_id: &String,
    branch: &daybook_types::doc::BranchPath,
    timeout: Duration,
) -> Res<(
    Arc<daybook_types::doc::Doc>,
    Arc<daybook_types::doc::Doc>,
)> {
    Ok(tokio::time::timeout(timeout, async {
        loop {
            let left_doc = left.drawer.get_doc_bundle_at_branch(doc_id, branch, None).await?;
            let right_doc = right.drawer.get_doc_bundle_at_branch(doc_id, branch, None).await?;
            if let (Some(left_doc), Some(right_doc)) = (left_doc, right_doc) {
                if left_doc.doc.id == right_doc.doc.id && left_doc.doc.facets == right_doc.doc.facets {
                    return eyre::Ok((Arc::new(left_doc.doc), Arc::new(right_doc.doc)));
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??)
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_two_nodes_can_connect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (_temp_root, node_a, node_b, endpoint_id) = boot_connected_sync_pair().await?;
    wait_for_sync_convergence(&node_a, &node_b, endpoint_id, Duration::from_secs(20)).await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_created_before_connect_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let rtx = RepoCtx::init(&repo_a_path, RepoOpenOptions {}, "test-device".into()).await?;
    rtx.shutdown().await?;
    drop(rtx);

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_on_a = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Pre-connect sync doc".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    let bootstrap_a = node_a.sync_repo.current_bootstrap_state().await;
    node_b
        .sync_repo
        .connect_endpoint_addr(bootstrap_a.endpoint_addr.clone())
        .await?;
    wait_for_sync_convergence(
        &node_a,
        &node_b,
        bootstrap_a.endpoint_id,
        Duration::from_secs(20),
    )
    .await?;

    let doc_on_a = node_a
        .drawer
        .get_with_heads(&doc_on_a, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("node_a lost the pre-connect doc")?;
    let doc_on_b = node_b
        .drawer
        .get_with_heads(&doc_on_a.0.id, &daybook_types::doc::BranchPath::from("main"), None)
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

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_blob_created_before_connect_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let rtx = RepoCtx::init(&repo_a_path, RepoOpenOptions {}, "test-device".into()).await?;
    rtx.shutdown().await?;
    drop(rtx);

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let payload = b"pre-connect sync blob".to_vec();
    let hash = node_a.blobs_repo.put(&payload).await?;
    let blob_key = FacetKey::from(WellKnownFacetTag::Blob);
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                blob_key.clone(),
                WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: hash.clone(),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                })
                .into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    let bootstrap_a = node_a.sync_repo.current_bootstrap_state().await;
    node_b
        .sync_repo
        .connect_endpoint_addr(bootstrap_a.endpoint_addr.clone())
        .await?;
    wait_for_sync_convergence(
        &node_a,
        &node_b,
        bootstrap_a.endpoint_id,
        Duration::from_secs(20),
    )
    .await?;

    let doc_on_a = node_a
        .drawer
        .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("node_a lost the pre-connect blob doc")?;
    let doc_on_b = node_b
        .drawer
        .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
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
                digest: hash.clone(),
                inline: None,
                urls: Some(vec![format!("db+blob:///{hash}")]),
            },
        )))
    );

    let got = wait_for_blob_bytes(&node_b.blobs_repo, &hash, Duration::from_secs(60)).await?;
    assert_eq!(got, payload);

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_created_while_connected_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let rtx = RepoCtx::init(&repo_a_path, RepoOpenOptions {}, "test-device".into()).await?;
    rtx.shutdown().await?;
    drop(rtx);

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let bootstrap_a = node_a.sync_repo.current_bootstrap_state().await;
    node_b
        .sync_repo
        .connect_endpoint_addr(bootstrap_a.endpoint_addr.clone())
        .await?;
    wait_for_sync_convergence(
        &node_a,
        &node_b,
        bootstrap_a.endpoint_id,
        Duration::from_secs(20),
    )
    .await?;

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_on_a = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Connected doc sync".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;
    let doc_on_a = node_a
        .drawer
        .get_with_heads(&doc_on_a, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("node_a lost the connected doc")?;
    let doc_on_b = node_b
        .drawer
        .get_with_heads(&doc_on_a.0.id, &daybook_types::doc::BranchPath::from("main"), None)
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

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_blob_created_while_connected_replicates() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    tokio::fs::create_dir_all(&repo_a_path).await?;
    let rtx = RepoCtx::init(&repo_a_path, RepoOpenOptions {}, "test-device".into()).await?;
    rtx.shutdown().await?;
    drop(rtx);

    let node_a = open_sync_node(&repo_a_path).await?;
    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    bootstrap_clone_repo_from_url_for_tests(&ticket_a, &repo_b_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let bootstrap_a = node_a.sync_repo.current_bootstrap_state().await;
    node_b
        .sync_repo
        .connect_endpoint_addr(bootstrap_a.endpoint_addr.clone())
        .await?;
    wait_for_sync_convergence(
        &node_a,
        &node_b,
        bootstrap_a.endpoint_id,
        Duration::from_secs(20),
    )
    .await?;

    let payload = b"connected sync blob".to_vec();
    let hash = node_a.blobs_repo.put(&payload).await?;
    let blob_key = FacetKey::from(WellKnownFacetTag::Blob);
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                blob_key.clone(),
                WellKnownFacet::Blob(daybook_types::doc::Blob {
                    mime: "application/octet-stream".to_string(),
                    length_octets: payload.len() as u64,
                    digest: hash.clone(),
                    inline: None,
                    urls: Some(vec![format!("db+blob:///{hash}")]),
                })
                .into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
    let got = wait_for_blob_bytes(&node_b.blobs_repo, &hash, Duration::from_secs(60)).await?;
    assert_eq!(got, payload);

    let doc_on_a = node_a
        .drawer
        .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("node_a lost the connected blob doc")?;
    let doc_on_b = node_b
        .drawer
        .get_with_heads(&doc_id, &daybook_types::doc::BranchPath::from("main"), None)
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
                digest: hash.clone(),
                inline: None,
                urls: Some(vec![format!("db+blob:///{hash}")]),
            },
        ))),
    );

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_doc_updates_propagate_originator_then_other() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Base title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
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

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_doc_updates_propagate_other_then_originator() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Base title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
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

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_divergent_facet_updates_propagate_originator_then_other() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Base title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
    let branch = daybook_types::doc::BranchPath::from("main");
    let Some((_, base_heads)) = node_a.drawer.get_with_heads(&doc_id, &branch, None).await? else {
        eyre::bail!("missing base heads before divergent updates");
    };

    update_title_at_heads(&node_a, &doc_id, &base_heads, "A title").await?;
    update_note_at_heads(&node_b, &doc_id, &base_heads, "B note").await?;

    assert_title_and_note_synced(&node_a, &node_b, &doc_id, "A title", "B note").await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_connected_divergent_facet_updates_propagate_other_then_originator() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (_temp_root, node_a, node_b, _) = boot_connected_sync_pair().await?;
    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_id = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Base title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;

    wait_for_doc_presence_with_activity(&node_b, &doc_id, Duration::from_secs(60)).await?;
    let branch = daybook_types::doc::BranchPath::from("main");
    let Some((_, base_heads)) = node_a.drawer.get_with_heads(&doc_id, &branch, None).await? else {
        eyre::bail!("missing base heads before divergent updates");
    };

    update_note_at_heads(&node_b, &doc_id, &base_heads, "B note").await?;
    update_title_at_heads(&node_a, &doc_id, &base_heads, "A title").await?;

    assert_title_and_note_synced(&node_a, &node_b, &doc_id, "A title", "B note").await?;

    node_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_survives_remote_restart_and_reconnect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (temp_root, node_a, node_b, _bootstrap_id) = boot_connected_sync_pair().await?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    let ticket_a = node_a.sync_repo.get_ticket_url().await?;

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let doc_on_a = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: daybook_types::doc::BranchPath::from("main"),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Initial title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;

    let doc_on_b = node_b
        .drawer
        .get_with_heads(&doc_on_a, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("node_b could not load synced doc after initial connect")?;
    assert_eq!(
        doc_on_b.0.facets.get(&title_key),
        Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
            "Initial title".into()
        ))),
        "synced doc content did not match on node_b after initial sync"
    );

    node_b.stop().await?;

    let reopened_b = open_sync_node(&repo_b_path).await?;
    let reopened_bootstrap_ba = reopened_b.sync_repo.connect_url(&ticket_a).await?;
    wait_for_sync_convergence(
        &node_a,
        &reopened_b,
        reopened_bootstrap_ba.endpoint_id,
        Duration::from_secs(20),
    )
    .await?;

    let Some((_, heads)) = node_a
        .drawer
        .get_with_heads(&doc_on_a, &daybook_types::doc::BranchPath::from("main"), None)
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
                user_path: Some(daybook_types::doc::UserPath::from(
                    node_a.ctx.local_user_path.clone(),
                )),
            },
            daybook_types::doc::BranchPath::from("main"),
            Some(heads),
        )
        .await?;

    wait_for_doc_head_parity(
        &node_a,
        &reopened_b,
        &doc_on_a,
        &daybook_types::doc::BranchPath::from("main"),
        Duration::from_secs(30),
    )
    .await?;

    let doc_on_b = reopened_b
        .drawer
        .get_with_heads(&doc_on_a, &daybook_types::doc::BranchPath::from("main"), None)
        .await?
        .ok_or_eyre("reopened node_b could not load synced doc after reconnect")?;
    assert_eq!(
        doc_on_b.0.facets.get(&title_key),
        Some(&serde_json::Value::from(WellKnownFacet::TitleGeneric(
            "Updated after restart".into()
        ))),
        "synced doc content did not match on reopened node_b after reconnect"
    );

    reopened_b.stop().await?;
    node_a.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_shutdown_peer_updates_catch_up_after_reconnect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let (temp_root, node_a, node_b, _bootstrap_id) = boot_connected_sync_pair().await?;
    let repo_a_path = temp_root.path().join("repo-a");

    let title_key = FacetKey::from(WellKnownFacetTag::TitleGeneric);
    let branch = daybook_types::doc::BranchPath::from("main");
    let doc_on_a = node_a
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: branch.clone(),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("Live base title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_a.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    wait_for_doc_presence_with_activity(&node_b, &doc_on_a, Duration::from_secs(60)).await?;
    assert_title_synced(&node_a, &node_b, &doc_on_a, "Live base title").await?;

    let Some((_, base_heads)) = node_b.drawer.get_with_heads(&doc_on_a, &branch, None).await? else {
        eyre::bail!("missing base heads on node_b before shutdown updates: {doc_on_a}");
    };

    node_a.stop().await?;

    update_title_at_heads(&node_b, &doc_on_a, &base_heads, "B offline updated title").await?;

    let doc_on_b = node_b
        .drawer
        .add(daybook_types::doc::AddDocArgs {
            branch_path: branch.clone(),
            facets: [(
                title_key.clone(),
                WellKnownFacet::TitleGeneric("B offline created title".into()).into(),
            )]
            .into(),
            user_path: Some(daybook_types::doc::UserPath::from(
                node_b.ctx.local_user_path.clone(),
            )),
        })
        .await?;
    update_title_at_main_branch(&node_b, &doc_on_b, "B offline created title v2").await?;

    let reopened_a = open_sync_node(&repo_a_path).await?;
    let reopened_bootstrap_a = reopened_a.sync_repo.current_bootstrap_state().await;
    node_b
        .sync_repo
        .connect_endpoint_addr(reopened_bootstrap_a.endpoint_addr.clone())
        .await?;
    node_b
        .sync_repo
        .wait_for_full_sync(
            std::slice::from_ref(&reopened_bootstrap_a.endpoint_id),
            Duration::from_secs(120),
        )
    .await?;

    // Debug: list what each node knows after full sync
    let reopened_a_items = reopened_a.ctx.partition_store.list_known_item_ids().await?;
    let node_b_items = node_b.ctx.partition_store.list_known_item_ids().await?;
    let reopened_a_branches = reopened_a.drawer.list().await?;
    let node_b_branches = node_b.drawer.list().await?;
    eprintln!("=== POST FULL-SYNC DEBUG ===");
    eprintln!("reopened_a partition items: {:?}", reopened_a_items);
    eprintln!("node_b partition items: {:?}", node_b_items);
    eprintln!("reopened_a drawer docs: {:?}", reopened_a_branches.iter().map(|d| &d.doc_id).collect::<Vec<_>>());
    eprintln!("node_b drawer docs: {:?}", node_b_branches.iter().map(|d| &d.doc_id).collect::<Vec<_>>());
    // Check branch doc for doc_on_b
    if let Some(entry_b) = node_b.drawer.get_entry(&doc_on_b).await? {
        eprintln!("node_b doc_on_b entry branches: {:?}", entry_b.branches.keys().collect::<Vec<_>>());
        if let Some(main_branch) = entry_b.branches.get("main") {
            eprintln!("node_b doc_on_b main branch_doc_id: {}", main_branch.branch_doc_id);
            let branch_doc_in_a = reopened_a.ctx.big_repo.get_doc(&main_branch.branch_doc_id.parse().unwrap()).await?;
            let branch_doc_in_b = node_b.ctx.big_repo.get_doc(&main_branch.branch_doc_id.parse().unwrap()).await?;
            eprintln!("branch doc in reopened_a big_repo: {}", branch_doc_in_a.is_some());
            eprintln!("branch doc in node_b big_repo: {}", branch_doc_in_b.is_some());
            // Check which partitions contain the branch doc via SQL
            let partitions_for_branch: Vec<(String, i64)> = sqlx::query_as(
                "SELECT partition_id, present FROM partition_membership_state WHERE item_id = ?"
            )
            .bind(&main_branch.branch_doc_id)
            .fetch_all(node_b.ctx.partition_store.state_pool())
            .await?;
            eprintln!("node_b branch doc partitions: {:?}", partitions_for_branch);
            let partitions_for_branch_a: Vec<(String, i64)> = sqlx::query_as(
                "SELECT partition_id, present FROM partition_membership_state WHERE item_id = ?"
            )
            .bind(&main_branch.branch_doc_id)
            .fetch_all(reopened_a.ctx.partition_store.state_pool())
            .await?;
            eprintln!("reopened_a branch doc partitions: {:?}", partitions_for_branch_a);
        }
    }
    eprintln!("=== END DEBUG ===");

    let branch = daybook_types::doc::BranchPath::from("main");
    let (doc_a_on_reopened_a, doc_a_on_b) = wait_for_synced_doc_on_both_sides(
        &reopened_a,
        &node_b,
        &doc_on_a,
        &branch,
        Duration::from_secs(60),
    )
    .await?;
    let (doc_b_on_reopened_a, doc_b_on_b) = wait_for_synced_doc_on_both_sides(
        &reopened_a,
        &node_b,
        &doc_on_b,
        &branch,
        Duration::from_secs(60),
    )
    .await?;

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
    node_b.stop().await?;
    Ok(())
}
