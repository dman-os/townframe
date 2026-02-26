use crate::interlude::*;

use crate::test::{test_wflows_wasm_path, WflowTestContext};

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_once() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let test_cx = WflowTestContext::builder().build().await?.start().await?;

    // Register the test_wflows workload
    test_cx
        .register_workload(&test_wflows_wasm_path()?, vec!["fails_once".to_string()])
        .await?;

    // Schedule the job - it should fail the first time
    let job_id: Arc<str> = "test-fails-once-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-counter"
    }))?;

    test_cx
        .schedule_job(Arc::clone(&job_id), "fails_once", args_json.clone())
        .await?;

    // Wait until there are no active jobs (job completed or archived)
    test_cx.wait_until_no_active_jobs(10).await?;

    tracing::info!("wait_until_no_active_jobs completed, getting snapshot");

    // Snapshot the full partition log
    test_cx
        .assert_partition_log_snapshot("fails_once_partition_log")
        .await?;

    // Cleanup
    test_cx.stop().await?;
    tracing::info!("test complete");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_once_sqlite() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    // Build Ctx with SQLite stores
    let cx = crate::Ctx::init("sqlite::memory:").await?;

    // Build test context with SQLite stores
    let test_cx = WflowTestContext::builder()
        .with_metastore(cx.metastore)
        .with_logstore(cx.logstore)
        .with_snapstore(cx.snapstore)
        .build()
        .await?
        .start()
        .await?;

    // Register the test_wflows workload
    test_cx
        .register_workload(&test_wflows_wasm_path()?, vec!["fails_once".to_string()])
        .await?;

    // Schedule the job - it should fail the first time
    let job_id: Arc<str> = "test-fails-once-sqlite-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-counter"
    }))?;

    test_cx
        .schedule_job(Arc::clone(&job_id), "fails_once", args_json.clone())
        .await?;

    // Wait until there are no active jobs (job completed or archived)
    test_cx.wait_until_no_active_jobs(10).await?;

    tracing::info!("wait_until_no_active_jobs completed, getting snapshot");

    // Snapshot the full partition log
    test_cx
        .assert_partition_log_snapshot("fails_once_sqlite_partition_log")
        .await?;

    // Cleanup
    test_cx.stop().await?;
    tracing::info!("test complete");

    Ok(())
}
