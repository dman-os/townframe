use crate::interlude::*;

use std::collections::HashMap;

use crate::test::{test_wflows_wasm_path, InitialWorkload, WflowTestContext};

fn source_effect_counts(
    log_snapshot: &[(u64, wflow_core::partition::log::PartitionLogEntry)],
) -> HashMap<u64, usize> {
    let mut counts = HashMap::<u64, usize>::new();
    for (_, entry) in log_snapshot {
        if let wflow_core::partition::log::PartitionLogEntry::JobPartitionEffects(entry) = entry {
            *counts.entry(entry.source_entry_id).or_default() += 1;
        }
    }
    counts
}

#[tokio::test(flavor = "multi_thread")]
async fn recovers_partition_from_log_only_without_duplicate_effects() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
            wflow_keys: vec!["fails_once".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    let job_id: Arc<str> = "recover-from-log-only".into();
    let args_json = serde_json::to_string(&serde_json::json!({
        "foo": "bar"
    }))?;
    test_cx
        .schedule_job(job_id, "fails_once", args_json)
        .await?;
    test_cx.wait_until_no_active_jobs(10).await?;

    let before = test_cx.get_partition_log_snapshot().await?;
    let before_counts = source_effect_counts(&before);
    let logstore = Arc::clone(&test_cx.logstore);
    test_cx.stop().await?;

    let test_cx = WflowTestContext::builder()
        .with_logstore(logstore)
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
            wflow_keys: vec!["fails_once".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    test_cx.wait_until_no_active_jobs(10).await?;
    let after = test_cx.get_partition_log_snapshot().await?;
    let after_counts = source_effect_counts(&after);
    test_cx.stop().await?;

    assert_eq!(
        before_counts, after_counts,
        "log-only replay should not append duplicate JobPartitionEffects"
    );
    assert_eq!(
        before.len(),
        after.len(),
        "log-only replay should not append new partition log entries for already-reduced sources"
    );

    Ok(())
}
