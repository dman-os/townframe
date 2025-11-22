use crate::interlude::*;

use crate::test::WflowTestContext;

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_once() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    let test_cx = WflowTestContext::new().await?;

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
        .schedule_job(job_id.clone(), "fails_once".to_string(), args_json.clone())
        .await?;

    // Wait until there are no active jobs (job completed or archived)
    test_cx.wait_until_no_active_jobs(10).await?;

    tracing::info!("wait_until_no_active_jobs completed, getting snapshot");

    // Snapshot the full partition log
    let log_snapshot = test_cx.get_partition_log_snapshot().await?;
    insta::with_settings!({
        filters => vec![
            (r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", "[timesamp]"),
            (r"\w*Location.*:\d+:\d+", "[location]"),
        ]
    }, {
        insta::assert_yaml_snapshot!("fails_once_partition_log", log_snapshot);
    });

    // Cleanup
    test_cx.close().await?;
    tracing::info!("test complete");

    Ok(())
}
