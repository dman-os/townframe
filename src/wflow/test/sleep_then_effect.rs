use crate::interlude::*;

use crate::test::{test_wflows_wasm_path, InitialWorkload, WflowTestContext};

#[tokio::test(flavor = "multi_thread")]
async fn test_sleep_then_effect_succeeds() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
            wflow_keys: vec!["sleep_then_effect".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    let job_id: Arc<str> = "test-sleep-then-effect-1".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "millis": 50_u64
    }))?;
    test_cx
        .schedule_job(Arc::clone(&job_id), "sleep_then_effect", args_json)
        .await?;

    test_cx
        .wait_until_entry(0, 10, |_entry_id, entry| {
            use wflow_core::partition::job_events::JobRunResult;
            use wflow_core::partition::log::PartitionLogEntry;
            matches!(
                entry,
                PartitionLogEntry::JobEffectResult(event)
                    if event.job_id == job_id && matches!(event.result, JobRunResult::StepWait(_))
            )
        })
        .await?;

    test_cx.wait_until_no_active_jobs(10).await?;
    test_cx
        .wait_until_entry(0, 10, |_entry_id, entry| {
            use wflow_core::partition::job_events::JobRunResult;
            use wflow_core::partition::log::PartitionLogEntry;
            matches!(
                entry,
                PartitionLogEntry::JobEffectResult(event)
                    if event.job_id == job_id && matches!(event.result, JobRunResult::Success { .. })
            )
        })
        .await?;

    test_cx.stop().await?;
    Ok(())
}
