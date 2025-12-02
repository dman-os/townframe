//! Test for pglite integration

use crate::interlude::*;

use crate::test::WflowTestContextBuilder;
use std::sync::Arc;
use wash_plugin_pglite::{Config, PglitePlugin};

#[tokio::test(flavor = "multi_thread")]
async fn test_pglite_select_one() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    // Build test context with pglite plugin
    let temp_dir = tempfile::tempdir()?;
    let test_dir = temp_dir.path().join("townframe-test-pglite");
    tokio::fs::create_dir_all(&test_dir).await?;
    
    let config = Config::with_paths(
        test_dir.join("runtime"),
        test_dir.join("data"),
    );
    let pglite_plugin = Arc::new(PglitePlugin::new(config).await?);
    
    let test_cx = WflowTestContextBuilder::new()
        .with_plugin(pglite_plugin)
        .build()
        .await?
        .start()
        .await?;

    // Register the test_wflows workload with pglite interface
    test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/tests_pglite.wasm",
            vec!["pglite_select_one".to_string()],
        )
        .await?;

    // Schedule the job
    let job_id: Arc<str> = "test-pglite-select-one-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({}))?;

    test_cx
        .schedule_job(job_id.clone(), "pglite_select_one", args_json.clone())
        .await?;

    // Wait for completion
    test_cx.wait_until_no_active_jobs(60).await?;

    tracing::info!("wait_until_no_active_jobs completed, getting snapshot");

    // Snapshot the full partition log
    test_cx
        .assert_partition_log_snapshot("pglite_select_one_partition_log")
        .await?;

    // Cleanup
    test_cx.close().await?;
    tracing::info!("test complete");

    Ok(())
}

