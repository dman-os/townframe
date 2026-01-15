use crate::interlude::*;

use crate::rt::dispatch::PropRoutineArgs;
use daybook_types::doc::{
    AddDocArgs, DocContent, DocPropKey, DocPropTag, WellKnownProp, WellKnownPropTag,
};

#[tokio::test(flavor = "multi_thread")]
async fn test_labeler_workflow() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let test_cx = crate::e2e::test_cx(utils_rs::function_full!()).await?;

    // Create and add a document to the drawer
    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        props: [
            //
            (
                DocPropKey::from(WellKnownPropTag::Content),
                WellKnownProp::Content(DocContent::Text(
                    //
                    "Hello, world!".into(),
                ))
                .into(),
            ),
        ]
        .into(),
        user_path: None,
    };

    // Add the document - DocTriageWorker will automatically queue the workflow job
    let doc_id = test_cx.drawer_repo.add(new_doc).await?;

    // Find the test-label dispatch and wait for it to complete
    let mut dispatch_id: Option<String> = None;
    for _ in 0..300 {
        let dispatches = test_cx.dispatch_repo.list().await;
        if let Some((id, _d)) = dispatches.iter().find(|(_, d)| {
            matches!(
                &d.deets,
                crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == "test-label"
            )
        }) {
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
        .get(&doc_id, &daybook_types::doc::BranchPath::from("main"))
        .await?
        .ok_or_eyre("doc not found")?;

    let has_test_label = updated_doc.props.keys().any(|tag| {
        matches!(
            tag,
            DocPropKey::Tag(DocPropTag::WellKnown(WellKnownPropTag::LabelGeneric))
        )
    });

    info!(?updated_doc, "result");

    assert!(
        has_test_label,
        "doc should have a LabelGeneric tag after test-labeler workflow completes. Props: {:?}",
        updated_doc.props
    );

    // Cleanup
    test_cx.stop().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_staging_branch_workflow() -> Res<()> {
    utils_rs::testing::setup_tracing_once();
    let test_cx = crate::e2e::test_cx(utils_rs::function_full!()).await?;

    // Create and add a document to the drawer
    let new_doc = AddDocArgs {
        branch_path: daybook_types::doc::BranchPath::from("main"),
        props: [(
            DocPropKey::from(WellKnownPropTag::Content),
            WellKnownProp::Content(DocContent::Text("Test staging branch".into())).into(),
        )]
        .into(),
        user_path: None,
    };

    // Add the document - DocTriageWorker will automatically queue the workflow job
    let doc_id = test_cx.drawer_repo.add(new_doc).await?;

    // Wait for the dispatch to be created
    let mut dispatch_id: Option<String> = None;
    let mut staging_branch_path: Option<daybook_types::doc::BranchPath> = None;

    for _ in 0..300 {
        let dispatches = test_cx.dispatch_repo.list().await;
        if let Some((id, dispatch)) = dispatches.iter().find(|(_, d)| {
            matches!(
                &d.args,
                crate::rt::dispatch::ActiveDispatchArgs::PropRoutine(_)
            ) && matches!(
                &d.deets,
                crate::rt::dispatch::ActiveDispatchDeets::Wflow { wflow_key, .. } if wflow_key == "test-label"
            )
        }) {
            dispatch_id = Some(id.clone());
            let crate::rt::dispatch::ActiveDispatchArgs::PropRoutine(PropRoutineArgs {
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
    if let Some(branches) = branches {
        let staging_branch_str = staging_branch.to_string_lossy().to_string();
        let staging_exists = branches.branches.contains_key(&staging_branch_str);

        if staging_exists {
            // If staging branch exists, verify we can read from it
            if let Some(staging_doc) = test_cx.drawer_repo.get(&doc_id, &staging_branch).await? {
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
    if let Some(branches) = final_branches {
        let staging_branch_str = staging_branch.to_string_lossy().to_string();
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
        .get(&doc_id, &daybook_types::doc::BranchPath::from("main"))
        .await?
        .ok_or_eyre("doc not found")?;

    // Verify the doc has a LabelGeneric tag from test-labeler (workflow succeeded)
    let has_test_label = final_doc.props.keys().any(|tag| {
        matches!(
            tag,
            DocPropKey::Tag(DocPropTag::WellKnown(WellKnownPropTag::LabelGeneric))
        )
    });

    assert!(
        has_test_label,
        "doc should have a LabelGeneric tag after test-labeler workflow completes. Props: {:?}",
        final_doc.props
    );

    // Verify that no new dispatches were created for /tmp/ branch changes
    // (triage should ignore /tmp/ branches)
    let final_dispatches = test_cx.dispatch_repo.list().await;
    assert!(
        final_dispatches.is_empty(),
        "no dispatches should remain after workflow completion. Dispatches: {:?}",
        final_dispatches
    );

    // Cleanup
    test_cx.stop().await?;

    Ok(())
}
