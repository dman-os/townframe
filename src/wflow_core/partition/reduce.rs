use crate::interlude::*;

use crate::partition::effects::PartitionEffect;
use crate::partition::job_events::JobError;
use crate::partition::{effects, job_events, state};

pub fn reduce_job_init_event(
    state: &mut state::PartitionJobsState,
    effects: &mut Vec<PartitionEffect>,
    event: job_events::JobInitEvent,
) {
    if state.active.contains_key(&event.job_id) || state.archive.contains_key(&event.job_id) {
        info!("duplicate job id, skipping");
        return;
    }

    state.active.insert(
        Arc::clone(&event.job_id),
        state::JobState {
            init_args_json: Arc::clone(&event.args_json),
            override_wflow_retry_policy: event.override_wflow_retry_policy,
            wflow: event.wflow,
            cancelling: false,
            runs: default(),
            steps: default(),
        },
    );

    effects.push(PartitionEffect {
        job_id: event.job_id,
        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
            run_id: 0,
            preferred_worker_id: None,
        }),
    })
}

pub fn reduce_job_cancel_event(
    state: &mut state::PartitionJobsState,
    effects: &mut Vec<PartitionEffect>,
    event: job_events::JobCancelEvent,
) {
    let Some(job_state) = state.active.get_mut(&event.job_id) else {
        info!("cancel for unknown or already-archived job, skipping");
        return;
    };
    job_state.cancelling = true;
    effects.push(PartitionEffect {
        job_id: event.job_id,
        deets: effects::PartitionEffectDeets::AbortRun {
            reason: event.reason,
        },
    });
}

pub fn reduce_job_run_event(
    state: &mut state::PartitionJobsState,
    effects: &mut Vec<PartitionEffect>,
    event: job_events::JobRunEvent,
) {
    // let mut cx = ReduceCtx {
    //     state,
    //     timestamp: event.timestamp,
    //     job_id: Arc::clone(&event.job_id),
    //     effect_commands,
    // };
    let worker_id_for_hint = event.worker_id.clone();
    let job_id = Arc::clone(&event.job_id);
    let Some(state::JobState {
        ref mut runs,
        ref mut steps,
        ref override_wflow_retry_policy,
        ref cancelling,
        ..
    }) = get_job_state(state, &job_id)
    else {
        return effects.push(PartitionEffect {
            job_id: Arc::clone(&job_id),
            deets: effects::PartitionEffectDeets::AbortRun {
                reason: "event for unrecognized job".into(),
            },
        });
    };

    assert!((event.run_id as usize) == runs.len());
    runs.push(event);

    let event = runs.last_mut().unwrap();
    match &event.result {
        job_events::JobRunResult::Success { .. }
        | job_events::JobRunResult::WorkerErr(_)
        | job_events::JobRunResult::WflowErr(JobError::Terminal { .. })
        | job_events::JobRunResult::Aborted => {
            archive_job(state, &job_id);
        }
        job_events::JobRunResult::WflowErr(JobError::Transient { retry_policy, .. }) => {
            if *cancelling {
                archive_job(state, &job_id);
            } else {
                let retry_policy = retry_policy
                    .clone()
                    .or(override_wflow_retry_policy.clone())
                    .unwrap_or(crate::partition::RetryPolicy::Immediate);
                match retry_policy {
                    crate::partition::RetryPolicy::Immediate => effects.push(PartitionEffect {
                        job_id,
                        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                            run_id: runs.len() as u64,
                            preferred_worker_id: None,
                        }),
                    }),
                }
            }
        }
        job_events::JobRunResult::StepEffect(res) => {
            if steps.len() == res.step_id as usize {
                steps.push(state::JobStepState::Effect {
                    attempts: default(),
                });
            }
            let step = &mut steps[res.step_id as usize];
            let state::JobStepState::Effect { attempts } = step;
            assert!((res.attempt_id as usize) == attempts.len());
            attempts.push(res.clone());

            match &res.deets {
                job_events::JobEffectResultDeets::EffectErr(JobError::Terminal { .. }) => {
                    archive_job(state, &job_id);
                }
                job_events::JobEffectResultDeets::Success { .. } => {
                    if *cancelling {
                        archive_job(state, &job_id);
                    } else {
                        effects.push(PartitionEffect {
                            job_id,
                            deets: effects::PartitionEffectDeets::RunJob(
                                effects::RunJobAttemptDeets {
                                    run_id: runs.len() as u64,
                                    preferred_worker_id: worker_id_for_hint.clone(),
                                },
                            ),
                        });
                    }
                }
                job_events::JobEffectResultDeets::EffectErr(JobError::Transient {
                    retry_policy,
                    ..
                }) => {
                    if *cancelling {
                        archive_job(state, &job_id);
                    } else {
                        match retry_policy
                            .as_ref()
                            .or(override_wflow_retry_policy.as_ref())
                            .unwrap_or(&crate::partition::RetryPolicy::Immediate)
                        {
                            crate::partition::RetryPolicy::Immediate => {
                                effects.push(PartitionEffect {
                                    job_id,
                                    deets: effects::PartitionEffectDeets::RunJob(
                                        effects::RunJobAttemptDeets {
                                            run_id: runs.len() as u64,
                                            preferred_worker_id: None,
                                        },
                                    ),
                                })
                            }
                        }
                    }
                }
            }
        }
    }
}

fn archive_job(state: &mut state::PartitionJobsState, job_id: &Arc<str>) {
    let job_state = state.active.remove(job_id).unwrap();
    state.archive.insert(Arc::clone(job_id), job_state);
}

fn get_job_state<'a>(
    state: &'a mut state::PartitionJobsState,
    job_id: &Arc<str>,
) -> Option<&'a mut state::JobState> {
    state.active.get_mut(job_id)
}
