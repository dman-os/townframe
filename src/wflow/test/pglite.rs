//! Test for pglite integration

use crate::interlude::*;

use crate::test::WflowTestContextBuilder;
use wash_runtime::host::HostApi;
use wash_runtime::{types, wit::WitInterface};
use wash_plugin_pglite::{Config, PglitePlugin};

#[tokio::test(flavor = "multi_thread")]
async fn test_pglite_select_one() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    // Build test context with pglite plugin
    let test_cx = WflowTestContextBuilder::new()
        // .with_plugin(pglite_plugin)
        .build()
        .await?
        .start()
        .await?;

    // Register the test_wflows workload with pglite interface
    let host = test_cx
        .host
        .as_ref()
        .ok_or_else(|| ferr!("host not started"))?;

    test_cx
        .register_workload(
            "../../target/wasm32-wasip2/debug/test_wflows.wasm",
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

