use crate::interlude::*;

use crate::gen::doc::{Doc, DocContent};

#[tokio::test(flavor = "multi_thread")]
async fn test_doc_created_workflow() -> Res<()> {
    utils_rs::testing::setup_tracing()?;

    let test_cx = crate::e2e::test_cx(utils_rs::function_full!()).await?;

    // Create and add a document to the drawer
    let new_doc = Doc {
        id: "test-doc-1".to_string(),
        created_at: OffsetDateTime::now_utc(),
        updated_at: OffsetDateTime::now_utc(),
        content: DocContent::Text("Hello, world!".to_string()),
        tags: vec![],
    };

    // Add the document - DocChangesWorker will automatically queue the workflow job
    let _doc_id = test_cx.drawer_repo.add(new_doc).await?;

    // Wait for the workflow to complete
    test_cx.wflow_test_cx.wait_until_no_active_jobs(90).await?;

    // Assert snapshot
    test_cx
        .wflow_test_cx
        .assert_partition_log_snapshot("doc_created_workflow_partition_log")
        .await?;

    // Cleanup
    test_cx.close().await?;

    Ok(())
}
