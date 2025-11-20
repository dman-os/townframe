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

    // Wait for it to fail (transient error)
    use futures::StreamExt;
    use tokio::time::{sleep, Duration};
    use wflow_core::log::LogStore;
    use wflow_core::partition::job_events::{JobEventDeets, JobRunResult};
    use wflow_core::partition::log::PartitionLogEntry;

    let mut stream = test_cx.log_store.tail(0).await;
    let mut timeout = sleep(Duration::from_secs(10));
    tokio::pin!(timeout);

    let mut first_run_complete = false;
    loop {
        let entry = tokio::select! {
            _ = &mut timeout => {
                return Err(eyre::eyre!("timeout waiting for first run"));
            }
            entry = stream.next() => {
                entry
            }
        };
        let Some(Ok((_, entry_bytes))) = entry else {
            continue;
        };

        let log_entry: PartitionLogEntry =
            serde_json::from_slice(&entry_bytes[..]).wrap_err("failed to parse log entry")?;

        match log_entry {
            PartitionLogEntry::JobEvent(job_event) => {
                if job_event.job_id.as_ref() != job_id.as_ref() {
                    continue;
                }
                match job_event.deets {
                    JobEventDeets::Run(run_event) => {
                        match run_event.result {
                            JobRunResult::Success { .. } => {
                                if !first_run_complete {
                                    return Err(eyre::eyre!(
                                        "first run should have failed, but it succeeded"
                                    ));
                                }
                                // Second run succeeded, we're done
                                break;
                            }
                            JobRunResult::WflowErr(err) => {
                                if !first_run_complete {
                                    // First run failed as expected (transient error should trigger automatic retry)
                                    tracing::info!("First run failed as expected: {:?}", err);
                                    first_run_complete = true;
                                    // Reset timeout for second run (automatic retry)
                                    timeout.as_mut().reset(
                                        tokio::time::Instant::now() + Duration::from_secs(10),
                                    );
                                    continue;
                                } else {
                                    return Err(eyre::eyre!(
                                        "second run failed with workflow error: {:?}",
                                        err
                                    ));
                                }
                            }
                            JobRunResult::WorkerErr(err) => {
                                return Err(eyre::eyre!("worker error: {:?}", err));
                            }
                            JobRunResult::StepEffect(_) => {
                                // Still processing, continue waiting
                            }
                        }
                    }
                    JobEventDeets::Init(_) => {
                        // Job initialized, continue waiting
                    }
                }
            }
            PartitionLogEntry::NewPartitionEffects(_) => {
                // Effects entry, continue waiting
            }
        }
    }

    // Drop the stream before closing
    drop(stream);

    // Cleanup
    test_cx.close().await?;

    Ok(())
}
