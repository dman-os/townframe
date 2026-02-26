use crate::interlude::*;

use crate::test::{test_wflows_wasm_path, InitialWorkload, WflowTestContext};

#[tokio::test(flavor = "multi_thread")]
async fn test_effect_chain_latency_smoke_baseline() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    const STEPS: u64 = 24;

    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
            wflow_keys: vec!["effect_chain".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    let job_id: Arc<str> = format!("effect-chain-{STEPS}-steps").into();
    let args_json = serde_json::to_string(&serde_json::json!({ "steps": STEPS }))?;

    let t0 = std::time::Instant::now();
    let _entry_id = test_cx
        .schedule_job(Arc::clone(&job_id), "effect_chain", args_json)
        .await?;
    test_cx.wait_until_no_active_jobs(20).await?;
    let elapsed = t0.elapsed();

    let log_snapshot = test_cx.get_partition_log_snapshot().await?;

    let mut step_effect_success_count = 0usize;
    let mut run_success_count = 0usize;
    let mut run_effect_result_count = 0usize;
    let mut partition_runjob_effect_count = 0usize;
    let mut run_effect_worker_id_count = 0usize;
    let mut runjob_preferred_worker_hint_count = 0usize;

    for (_idx, entry) in &log_snapshot {
        match entry {
            wflow_core::partition::log::PartitionLogEntry::JobEffectResult(evt)
                if evt.job_id == job_id =>
            {
                run_effect_result_count += 1;
                if evt.worker_id.is_some() {
                    run_effect_worker_id_count += 1;
                }
                match &evt.result {
                    wflow_core::partition::job_events::JobRunResult::StepEffect(step) => {
                        if matches!(
                            step.deets,
                            wflow_core::partition::job_events::JobEffectResultDeets::Success { .. }
                        ) {
                            step_effect_success_count += 1;
                        }
                    }
                    wflow_core::partition::job_events::JobRunResult::Success { .. } => {
                        run_success_count += 1;
                    }
                    _ => {}
                }
            }
            wflow_core::partition::log::PartitionLogEntry::JobPartitionEffects(batch) => {
                for effect in &batch.effects {
                    if effect.job_id == job_id
                        && matches!(
                            &effect.deets,
                            wflow_core::partition::effects::PartitionEffectDeets::RunJob(_)
                        )
                    {
                        partition_runjob_effect_count += 1;
                        if let wflow_core::partition::effects::PartitionEffectDeets::RunJob(run) =
                            &effect.deets
                        {
                            if run.preferred_worker_id.is_some() {
                                runjob_preferred_worker_hint_count += 1;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    assert_eq!(
        step_effect_success_count, STEPS as usize,
        "expected one successful StepEffect per effect_chain step"
    );
    assert_eq!(
        run_success_count, 1,
        "expected exactly one terminal Success result for effect_chain job"
    );
    assert_eq!(
        run_effect_result_count,
        STEPS as usize + 1,
        "expected N StepEffect results plus final Success run result"
    );
    assert_eq!(
        run_effect_worker_id_count, run_effect_result_count,
        "all JobEffectResult events should include worker_id metadata"
    );
    assert_eq!(
        partition_runjob_effect_count,
        STEPS as usize + 1,
        "expected N+1 RunJob schedules for current rerun-per-step design"
    );
    assert_eq!(
        runjob_preferred_worker_hint_count, STEPS as usize,
        "follow-up RunJob effects after StepEffect success should carry preferred_worker_id"
    );

    eprintln!(
        "EFFECT_CHAIN_BASELINE steps={} elapsed_ms={} run_results={} step_effect_success={} runjob_effects={}",
        STEPS,
        elapsed.as_millis(),
        run_effect_result_count,
        step_effect_success_count,
        partition_runjob_effect_count
    );

    test_cx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_effect_chain_multi_job_latency_baseline() -> Res<()> {
    utils_rs::testing::setup_tracing_once();

    const STEPS: u64 = 24;
    const JOBS: usize = 12;

    let test_cx = WflowTestContext::builder()
        .initial_workloads(vec![InitialWorkload {
            wasm_path: test_wflows_wasm_path()?,
            wflow_keys: vec!["effect_chain".to_string()],
        }])
        .build()
        .await?
        .start()
        .await?;

    let args_json = serde_json::to_string(&serde_json::json!({ "steps": STEPS }))?;

    let t0 = std::time::Instant::now();
    for ii in 0..JOBS {
        let job_id: Arc<str> = format!("effect-chain-batch-{JOBS}-steps-{STEPS}-job-{ii}").into();
        let _entry_id = test_cx
            .schedule_job(job_id, "effect_chain", args_json.clone())
            .await?;
    }
    test_cx.wait_until_no_active_jobs(30).await?;
    let elapsed = t0.elapsed();

    let log_snapshot = test_cx.get_partition_log_snapshot().await?;
    let mut step_effect_success_count = 0usize;
    let mut run_success_count = 0usize;
    let mut run_effect_result_count = 0usize;
    let mut partition_runjob_effect_count = 0usize;
    let mut run_effect_worker_id_count = 0usize;
    let mut runjob_preferred_worker_hint_count = 0usize;

    for (_idx, entry) in &log_snapshot {
        match entry {
            wflow_core::partition::log::PartitionLogEntry::JobEffectResult(evt)
                if evt.job_id.starts_with("effect-chain-batch-") =>
            {
                run_effect_result_count += 1;
                if evt.worker_id.is_some() {
                    run_effect_worker_id_count += 1;
                }
                match &evt.result {
                    wflow_core::partition::job_events::JobRunResult::StepEffect(step) => {
                        if matches!(
                            step.deets,
                            wflow_core::partition::job_events::JobEffectResultDeets::Success { .. }
                        ) {
                            step_effect_success_count += 1;
                        }
                    }
                    wflow_core::partition::job_events::JobRunResult::Success { .. } => {
                        run_success_count += 1;
                    }
                    _ => {}
                }
            }
            wflow_core::partition::log::PartitionLogEntry::JobPartitionEffects(batch) => {
                for effect in &batch.effects {
                    if effect.job_id.starts_with("effect-chain-batch-")
                        && matches!(
                            &effect.deets,
                            wflow_core::partition::effects::PartitionEffectDeets::RunJob(_)
                        )
                    {
                        partition_runjob_effect_count += 1;
                        if let wflow_core::partition::effects::PartitionEffectDeets::RunJob(run) =
                            &effect.deets
                        {
                            if run.preferred_worker_id.is_some() {
                                runjob_preferred_worker_hint_count += 1;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    assert_eq!(step_effect_success_count, JOBS * STEPS as usize);
    assert_eq!(run_success_count, JOBS);
    assert_eq!(run_effect_result_count, JOBS * (STEPS as usize + 1));
    assert_eq!(run_effect_worker_id_count, run_effect_result_count);
    assert_eq!(partition_runjob_effect_count, JOBS * (STEPS as usize + 1));
    assert_eq!(runjob_preferred_worker_hint_count, JOBS * STEPS as usize);

    let avg_ms_per_job = elapsed.as_secs_f64() * 1000.0 / JOBS as f64;
    eprintln!(
        "EFFECT_CHAIN_BATCH_BASELINE jobs={} steps={} elapsed_ms={} avg_ms_per_job={:.2} run_results={} step_effect_success={} runjob_effects={}",
        JOBS,
        STEPS,
        elapsed.as_millis(),
        avg_ms_per_job,
        run_effect_result_count,
        step_effect_success_count,
        partition_runjob_effect_count
    );

    test_cx.stop().await?;
    Ok(())
}
