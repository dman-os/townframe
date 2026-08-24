use crate::interlude::*;

use daybook_core::rt::dispatch::{ActiveDispatchArgs, FacetRoutineArgs};
use daybook_types::doc::{AddDocArgs, FacetKey, FacetTag, WellKnownFacet, WellKnownFacetTag};

#[tokio::test(flavor = "multi_thread")]
async fn test_labeler_workflow() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx(utils_rs::function_full!()).await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    // Create and add a document to the drawer
    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
        facets: [
            //
            (
                FacetKey::from(WellKnownFacetTag::Note),
                WellKnownFacet::Note("Hello, world!".into()).into(),
            ),
        ]
        .into(),
        user_path: None,
    };

    // Add the document - DocTriageWorker will automatically queue the workflow job
    let doc_id = test_cx.drawer_repo.add(new_doc).await?;

    // Find the test-label dispatch and wait for it to complete
    let mut dispatch_id: Option<String> = None;
    for _ in 0..600 {
        if let Some((id, _dispatch)) = test_cx
            .dispatch_repo
            .get_any_by_wflow_key("test-label")
            .await
        {
            dispatch_id = Some(id.clone());
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let dispatch_id = dispatch_id.ok_or_eyre("test-label dispatch not found")?;

    // Wait for the dispatch to complete
    test_cx
        .rt
        .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
        .await?;

    // Give a small delay to ensure document updates are propagated
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify the doc has a LabelGeneric tag from test-labeler
    let updated_doc = test_cx
        .drawer_repo
        .get_doc_with_facets_at_branch(
            &doc_id,
            &daybook_types::doc::BranchPathBuf::from("main"),
            None,
        )
        .await?
        .ok_or_eyre("doc not found")?;

    let has_test_label = updated_doc.facets.keys().any(|key| {
        matches!(
            key.tag,
            FacetTag::WellKnown(WellKnownFacetTag::LabelGeneric)
        )
    });

    info!(?updated_doc, "result");

    assert!(
        has_test_label,
        "doc should have a LabelGeneric tag after test-labeler workflow completes. Props: {:?}",
        updated_doc.facets
    );

    // Cleanup
    test_cx.stop().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_staging_branch_workflow() -> Res<()> {
    let test_cx = daybook_core::test_support::test_cx(utils_rs::function_full!()).await?;
    super::common::import_test_plug_oci(&test_cx).await?;

    // Create and add a document to the drawer
    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPathBuf::from("main"),
        facets: [(
            FacetKey::from(WellKnownFacetTag::Note),
            WellKnownFacet::Note("Test staging branch".into()).into(),
        )]
        .into(),
        user_path: None,
    };

    // Add the document - DocTriageWorker will automatically queue the workflow job
    let doc_id = test_cx.drawer_repo.add(new_doc).await?;

    // Wait for the dispatch to be created
    let mut dispatch_id: Option<String> = None;
    let mut staging_branch_path: Option<daybook_types::doc::BranchPathBuf> = None;

    for _ in 0..600 {
        if let Some((id, dispatch)) = test_cx
            .dispatch_repo
            .get_any_by_wflow_key("test-label")
            .await
        {
            if !matches!(&dispatch.args, ActiveDispatchArgs::FacetRoutine(_)) {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            dispatch_id = Some(id.clone());
            let ActiveDispatchArgs::FacetRoutine(FacetRoutineArgs {
                staging_branch_path: path,
                ..
            }) = &dispatch.args;
            staging_branch_path = Some(path.clone());
            info!(?staging_branch_path, "found staging branch path");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let dispatch_id = dispatch_id.ok_or_eyre("test-label dispatch should be created")?;
    let staging_branch = staging_branch_path.expect("staging branch path should be set");

    // Wait a bit for the workflow to make modifications to the staging branch
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Verify that modifications go to the staging branch (if it exists)
    // The staging branch may not exist yet if no writes have happened, or it may have been created
    let branches = test_cx.drawer_repo.get_doc_branches(&doc_id).await;
    if let Ok(Some(branches)) = branches {
        let staging_branch_str = staging_branch.to_string();
        let staging_exists = branches.branches.contains_key(&staging_branch_str);

        if staging_exists {
            // If staging branch exists, verify we can read from it
            if let Some(staging_doc) = test_cx
                .drawer_repo
                .get_doc_with_facets_at_branch(&doc_id, &staging_branch, None)
                .await?
            {
                info!(?staging_doc, "staging branch doc");
                // The staging branch should have the modifications from the workflow
            }
        } else {
            info!("staging branch not yet created (no writes yet)");
        }
    }

    // Wait for the dispatch to complete
    test_cx
        .rt
        .wait_for_dispatch_end(&dispatch_id, std::time::Duration::from_secs(90))
        .await?;

    // Give a small delay to ensure document updates are propagated
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify the staging branch has been cleaned up (merged or deleted)
    let final_branches = test_cx.drawer_repo.get_doc_branches(&doc_id).await;
    if let Ok(Some(branches)) = final_branches {
        let staging_branch_str = staging_branch.to_string();
        let staging_still_exists = branches.branches.contains_key(&staging_branch_str);

        assert!(
            !staging_still_exists,
            "staging branch should be cleaned up after workflow completion. Branches: {:?}",
            branches.branches.keys()
        );
    }

    // Verify the final result is in the target branch (main)
    let final_doc = test_cx
        .drawer_repo
        .get_doc_with_facets_at_branch(
            &doc_id,
            &daybook_types::doc::BranchPathBuf::from("main"),
            None,
        )
        .await?
        .ok_or_eyre("doc not found")?;

    // Verify the doc has a LabelGeneric tag from test-labeler (workflow succeeded)
    let has_test_label = final_doc.facets.keys().any(|key| {
        matches!(
            key.tag,
            FacetTag::WellKnown(WellKnownFacetTag::LabelGeneric)
        )
    });

    assert!(
        has_test_label,
        "doc should have a LabelGeneric tag after test-labeler workflow completes. Props: {:?}",
        final_doc.facets
    );

    // Verify that no new dispatches were created for /tmp/ branch changes.
    let final_dispatches = test_cx.dispatch_repo.list().await;
    let staging_dispatches = final_dispatches
        .into_iter()
        .filter(|(_id, dispatch)| match &dispatch.args {
            ActiveDispatchArgs::FacetRoutine(args) => {
                args.branch_path == "/tmp" || args.branch_path.starts_with("/tmp/")
            }
        })
        .collect::<Vec<_>>();

    assert!(
        staging_dispatches.is_empty(),
        "no dispatches should be created for staging branches. Dispatches: {:?}",
        staging_dispatches
    );

    // Cleanup
    test_cx.stop().await?;

    Ok(())
}
