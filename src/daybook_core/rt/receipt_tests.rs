use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn staging_merge_error_preserves_source_for_receipt_replay() -> Res<()> {
    let cx = crate::test_support::test_cx(utils_rs::function_full!()).await?;
    let staging =
        daybook_types::doc::BranchPathBuf::from(format!("/tmp/receipt-{}", Uuid::new_v4().bs58()));
    let target = daybook_types::doc::BranchPathBuf::from(format!(
        "/tmp/receipt-target-{}",
        Uuid::new_v4().bs58()
    ));
    let doc_id = cx
        .drawer_repo
        .add(daybook_types::doc::AddDocArgs {
            branch_path: "main".into(),
            facets: [(
                daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::Note),
                daybook_types::doc::WellKnownFacet::Note("staged".into()).into(),
            )]
            .into(),
            user_path: None,
            idempotency_key: None,
        })
        .await?;
    let heads = cx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, daybook_types::doc::BranchPath::new("main"))
        .await?
        .expect("main exists");
    cx.drawer_repo
        .create_branch_at_heads_from_branch(
            &doc_id,
            &staging,
            daybook_types::doc::BranchPath::new("main"),
            &heads,
            None,
        )
        .await?;
    let heads = cx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &staging)
        .await?
        .expect("staging exists");
    let tokens = dispatch::DocFacetTokens {
        doc_id: doc_id.clone(),
        branch_path: target.clone(),
        staging_branch_path: staging.clone(),
        heads: heads.clone(),
        facet_acl: vec![],
    };
    let dispatch = ActiveDispatch {
        deets: ActiveDispatchDeets::Wflow {
            plug_id: "test".into(),
            routine_name: "test".into(),
            bundle_name: "test".into(),
            wflow_key: "test".into(),
            wflow_job_id: Some("receipt-replay".into()),
        },
        args: ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
            doc_id: doc_id.clone(),
            branch_path: target.clone(),
            staging_branch_path: staging.clone(),
            heads: heads.clone(),
            invocation: dispatch::RoutineInvocation::Command,
            primary_doc: tokens,
            config_docs: vec![],
            local_state_acl: vec![],
            command_invoke_acl_snapshot: vec![],
            wflow_args_json: None,
        }),
        status: dispatch::DispatchStatus::Active,
        waiting_on_dispatch_ids: vec![],
        on_success_hooks: vec![],
    };

    // The source exists, but the target does not. Losing the target must not
    // turn a successful workflow receipt into a successful, lossy merge.
    assert!(
        cx.rt
            .settle_dispatch_staging("receipt-replay", &dispatch, true)
            .await
            .is_err()
    );
    assert_eq!(
        cx.drawer_repo
            .get_branch_heads_for_path(&doc_id, &staging)
            .await?,
        Some(heads.clone())
    );
    cx.drawer_repo
        .create_branch_at_heads_from_branch(&doc_id, &target, &staging, &heads, None)
        .await?;
    cx.rt
        .settle_dispatch_staging("receipt-replay", &dispatch, true)
        .await?;
    assert!(
        cx.drawer_repo
            .get_branch_heads_for_path(&doc_id, &staging)
            .await?
            .is_none()
    );
    let merged_heads = cx
        .drawer_repo
        .get_branch_heads_for_path(&doc_id, &target)
        .await?
        .expect("merged target exists");
    // Replaying after staging deletion is harmless and preserves the target.
    cx.rt
        .settle_dispatch_staging("receipt-replay", &dispatch, true)
        .await?;
    assert_eq!(
        cx.drawer_repo
            .get_branch_heads_for_path(&doc_id, &target)
            .await?,
        Some(merged_heads)
    );
    cx.stop().await?;
    Ok(())
}
