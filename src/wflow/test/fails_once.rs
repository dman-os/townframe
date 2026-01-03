use crate::interlude::*;

use crate::test::WflowTestContext;

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_once() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    let test_cx = WflowTestContext::builder().build().await?.start().await?;

    // Register the test_wflows workload
    test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/test_wflows.wasm",
            vec!["fails_once".to_string()],
        )
        .await?;

    // Schedule the job - it should fail the first time
    let job_id: Arc<str> = "test-fails-once-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-counter"
    }))?;

    test_cx
        .schedule_job(job_id.clone(), "fails_once", args_json.clone())
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
    utils_rs::testing::setup_tracing().unwrap();

    // Create an in-memory SQLite database
    use std::str::FromStr;
    let db_pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:")?.create_if_missing(true),
    )
    .await
    .wrap_err("failed to create in-memory SQLite database")?;

    let cx = crate::Ctx::init(&db_pool).await?;

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
        .register_workload(
            "../../target/wasm32-wasip2/debug/test_wflows.wasm",
            vec!["fails_once".to_string()],
        )
        .await?;

    // Schedule the job - it should fail the first time
    let job_id: Arc<str> = "test-fails-once-sqlite-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-counter"
    }))?;

    test_cx
        .schedule_job(job_id.clone(), "fails_once", args_json.clone())
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
