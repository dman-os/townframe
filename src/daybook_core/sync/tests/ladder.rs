use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn iroh_sync_single_doc_survives_remote_restart_and_reconnect() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    std::env::set_var("DAYB_DISABLE_KEYRING", "1");

    let temp_root = tempfile::tempdir()?;
    let repo_a_path = temp_root.path().join("repo-a");
    let repo_b_path = temp_root.path().join("repo-b");
    init_and_copy_repo_pair(&repo_a_path, &repo_b_path).await?;

    let node_a = open_sync_node(&repo_a_path).await?;
    let node_b = open_sync_node(&repo_b_path).await?;

    let ticket_a = node_a.sync_repo.get_ticket_url().await?;
    let bootstrap_ba = node_b.sync_repo.connect_url(&ticket_a).await?;
    wait_for_sync_convergence(
        &node_a,
        &node_b,
        bootstrap_ba.endpoint_id,
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
