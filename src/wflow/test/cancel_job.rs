use crate::interlude::*;

use crate::test::{InitialWorkload, WflowTestContext};

#[tokio::test(flavor = "multi_thread")]
async fn test_cancel_job() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: "../../target/wasm32-wasip2/debug/test_wflows.wasm".into(),
            wflow_keys: vec!["fails_until_told".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    let job_id: Arc<str> = "test-cancel-job-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "key": "test-cancel-flag"
    }))?;

    test_cx
        .schedule_job(Arc::clone(&job_id), "fails_until_told", args_json)
        .await?;

    // Wait until we see at least one transient failure (job is running and retrying)
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

    tracing::info!("Saw transient failure, cancelling job");

    test_cx
        .cancel_job(Arc::clone(&job_id), "test requested cancel".to_string())
        .await?;

    // Verify the cancel command was persisted for this job.
    test_cx
        .wait_until_entry(0, 10, |_entry_id, entry| {
            use wflow_core::partition::log::PartitionLogEntry;

            if let PartitionLogEntry::JobCancel(event) = entry {
                return event.job_id == job_id && event.reason.as_ref() == "test requested cancel";
            }
            false
        })
        .await?;

    // Verify reducer emitted an abort effect for this job.
    test_cx
        .wait_until_entry(0, 10, |_entry_id, entry| {
            use wflow_core::partition::log::PartitionLogEntry;

            let PartitionLogEntry::JobPartitionEffects(event) = entry else {
                return false;
            };
            event.effects.iter().any(|effect| {
                effect.job_id == job_id
                    && matches!(
                        effect.deets,
                        wflow_core::partition::effects::PartitionEffectDeets::AbortRun { .. }
                    )
            })
        })
        .await?;

    test_cx.wait_until_no_active_jobs(10).await?;

    test_cx.stop().await?;
    tracing::info!("test complete");

    Ok(())
}
