use crate::interlude::*;

use crate::test::{InitialWorkload, WflowTestContext};

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_until_told() -> Res<()> {
    utils_rs::testing::setup_tracing().unwrap();

    // First run: job will fail transiently
    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: "../../target/wasm32-wasip2/debug/test_wflows.wasm".into(),
            wflow_keys: vec!["fails_until_told".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    // Schedule the job - it should fail transiently
    let job_id: Arc<str> = "test-fails-until-told-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-flag"
    }))?;

    test_cx
        .schedule_job(job_id.clone(), "fails_until_told", args_json.clone())
        .await?;

    // Wait until we see a job run that fails with "waiting for flag to be set"
    // This confirms the job is running and checking the flag
    test_cx
        .wait_until_entry(0, 10, |_entry_id, entry| {
            use wflow_core::partition::job_events::{JobError, JobRunResult};
            use wflow_core::partition::log::PartitionLogEntry;

            if let PartitionLogEntry::JobEffectResult(event) = entry {
                if event.job_id == job_id {
                    if let JobRunResult::WflowErr(JobError::Transient { error_json, .. }) =
                        &event.result
                    {
                        if error_json.contains("waiting for flag to be set") {
                            return true;
                        }
                    }
                }
            }
            false
        })
        .await?;

    tracing::info!("First run completed, saw expected failure message, tearing down");

    // Save the stores and keyvalue plugin to share between runs
    // Note: We'll create a new metastore for the second run so we can register
    // the workload fresh, but we'll reuse logstore and snap_store which contain
    // the job state and snapshots
    let logstore = test_cx.logstore.clone();
    let snap_store = test_cx.snapstore.clone();
    let keyvalue_plugin = test_cx.keyvalue_plugin.clone();

    // Cleanup first run
    test_cx.stop().await?;

    // Set the keyvalue flag to true
    keyvalue_plugin
        .set_value("workload_123", "default", "test-flag", vec![1])
        .await
        .to_eyre()?;

    tracing::info!("Flag set, starting second run");

    // Second run: create a new context with the same AmCtx, shared log/snap stores,
    // and the SAME keyvalue storage so the flag is visible. We also register the
    // workload before the worker starts by using the initial_workloads option.
    let shared_keyvalue = Arc::new(keyvalue_plugin.with_shared_storage());

    let test_cx = WflowTestContext::builder()
        .with_logstore(logstore)
        .with_snapstore(snap_store)
        .with_keyvalue_plugin(shared_keyvalue)
        .initial_workloads(vec![InitialWorkload {
            wasm_path: "../../target/wasm32-wasip2/debug/test_wflows.wasm".into(),
            wflow_keys: vec!["fails_until_told".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    // The snapshot will recover the job state, and it should automatically retry
    // This time it should succeed because the flag is set
    // Wait until there are no active jobs (job completed or archived)
    test_cx.wait_until_no_active_jobs(10).await?;

    tracing::info!("Second run completed, getting snapshot");

    // Snapshot the full partition log
    test_cx
        .assert_partition_log_snapshot("fails_until_told_partition_log")
        .await?;

    // Cleanup
    test_cx.stop().await?;
    tracing::info!("test complete");

    Ok(())
}
