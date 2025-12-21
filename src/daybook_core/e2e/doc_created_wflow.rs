use crate::interlude::*;

use daybook_types::{Doc, DocContent, DocProp};

#[tokio::test(flavor = "multi_thread")]
async fn test_pseudo_labeler_workflow() -> Res<()> {
    let test_cx = crate::e2e::test_cx(utils_rs::function_full!()).await?;

    // Create and add a document to the drawer
    let new_doc = Doc {
        id: "test-doc-1".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Text("Hello, world!".to_string()),
        props: vec![],
    };

    // Add the document - DocTriageWorker will automatically queue the workflow job
    let doc_id = test_cx.drawer_repo.add(new_doc).await?;

    // Wait for the workflow to complete
    test_cx.wflow_test_cx.wait_until_no_active_jobs(90).await?;

    // Give a small delay to ensure document updates are propagated
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify the doc has a PseudoLabel tag
    let updated_doc = test_cx
        .drawer_repo
        .get(&doc_id)
        .await?
        .ok_or_eyre("doc not found")?;

    let has_pseudo_label = updated_doc
        .props
        .iter()
        .any(|tag| matches!(tag, DocProp::PseudoLabel(v) if !v.is_empty()));

    info!(?updated_doc, "result");

    assert!(
        has_pseudo_label,
        "doc should have a PseudoLabel tag after pseudo-labeler workflow completes. Props: {:?}",
        updated_doc.props
    );

    // Cleanup
    test_cx.close().await?;

    Ok(())
}
