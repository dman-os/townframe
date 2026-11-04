use crate::interlude::*;

use crate::test::InitialWorkload;
use crate::test::WflowTestContext;
use crate::test::test_wflows_wasm_path;

#[tokio::test(flavor = "multi_thread")]
async fn test_fails_until_told() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    // First run: job will fail transiently
    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
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
        .schedule_job(Arc::clone(&job_id), "fails_until_told", args_json.clone())
        .await?;

    // Wait until we see a job run that fails with "waiting for flag to be set"
    // This confirms the job is running and checking the flag
    test_cx
        .wait_until_entry(0, 10, |_entry_id, entry| {
            use wflow_core::partition::job_events::JobError;
            use wflow_core::partition::job_events::JobRunResult;
            use wflow_core::partition::log::PartitionLogEntry;

            if let PartitionLogEntry::JobEffectResult(event) = entry
                && event.job_id == job_id
                && let JobRunResult::WflowErr(JobError::Transient { error_json, .. }) =
                    &event.result
                && error_json.contains("waiting for flag to be set")
            {
                return true;
            }
            false
        })
        .await?;

    // Save the stores and keyvalue plugin to share between runs
    // Note: We'll create a new metastore for the second run so we can register
    // the workload fresh, but we'll reuse logstore and snap_store which contain
    // the job state and snapshots
    let logstore = Arc::clone(&test_cx.logstore);
    let snap_store = Arc::clone(&test_cx.snapstore);
    let keyvalue_plugin = Arc::clone(&test_cx.keyvalue_plugin);

    // Set the keyvalue flag to true so that retries succeed
    keyvalue_plugin
        .set_value("workload_123", "default", "test-flag", vec![1])
        .await
        .to_eyre()?;

    tracing::info!("First run completed, saw expected failure message, tearing down");

    // Cleanup first run
    test_cx.stop().await?;

    tracing::info!("Starting second run");

    // Second run: create a new context with the same AmCtx, shared log/snap stores,
    // and the SAME keyvalue storage so the flag is visible. We also register the
    // workload before the worker starts by using the initial_workloads option.
    let shared_keyvalue = Arc::new(keyvalue_plugin.with_shared_storage());

    let test_cx = WflowTestContext::builder()
        .with_logstore(logstore)
        .with_snapstore(snap_store)
        .with_keyvalue_plugin(shared_keyvalue)
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
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

    tracing::info!("Second run completed, checking the run trace");

    // The legacy scheduler re-issues a transiently failed run immediately, so
    // the guest's next flag read races the `set_value` above: how many
    // transient rounds land before the flag is visible depends on scheduling.
    // Assert the trace shape instead of an exact log snapshot.
    let log = test_cx.get_partition_log_snapshot().await?;
    let trace = run_trace(&log, &job_id);
    let transient_rounds = trace
        .iter()
        .take_while(|run| run.starts_with("transient:"))
        .count();
    assert!(
        transient_rounds >= 1,
        "expected at least one transient round, got {trace:?}"
    );
    assert!(
        trace[..transient_rounds]
            .iter()
            .all(|run| run.contains("waiting for flag to be set")),
        "unexpected transient error in {trace:?}"
    );
    assert_eq!(
        &trace[transient_rounds..],
        ["step 0 attempt 0 ok", "success"],
        "unexpected tail in {trace:?}"
    );

    // Cleanup
    test_cx.stop().await?;
    tracing::info!("test complete");

    Ok(())
}

/// One entry per run of `job_id`, in run order, classified by its result.
fn run_trace(
    log: &[(u64, wflow_core::partition::log::PartitionLogEntry)],
    job_id: &str,
) -> Vec<String> {
    use wflow_core::partition::job_events::JobEffectResultDeets;
    use wflow_core::partition::job_events::JobError;
    use wflow_core::partition::job_events::JobRunResult;
    use wflow_core::partition::log::PartitionLogEntry;

    let mut trace = Vec::new();
    for (_, entry) in log {
        let PartitionLogEntry::JobEffectResult(event) = entry else {
            continue;
        };
        if event.job_id.as_ref() != job_id {
            continue;
        }
        assert_eq!(
            event.run_id as usize,
            trace.len(),
            "runs must be contiguous"
        );
        trace.push(match &event.result {
            JobRunResult::WflowErr(JobError::Transient { error_json, .. }) => {
                format!("transient:{error_json}")
            }
            JobRunResult::StepEffect(step) => {
                let outcome = match &step.deets {
                    JobEffectResultDeets::Success { .. } => "ok",
                    JobEffectResultDeets::EffectErr(_) => "err",
                };
                format!(
                    "step {} attempt {} {outcome}",
                    step.step_id, step.attempt_id
                )
            }
            JobRunResult::Success { .. } => "success".to_string(),
            other => format!("{other:?}"),
        });
    }
    trace
}
