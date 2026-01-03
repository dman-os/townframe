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
        event.job_id.clone(),
        state::JobState {
            init_args_json: event.args_json.clone(),
            override_wflow_retry_policy: event.override_wflow_retry_policy,
            wflow: event.wflow,
            runs: default(),
            steps: default(),
        },
    );

    effects.push(PartitionEffect {
        job_id: event.job_id,
        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets { run_id: 0 }),
    })
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
    let job_id = event.job_id.clone();
    let Some(state::JobState {
        ref mut runs,
        ref mut steps,
        ref override_wflow_retry_policy,
        ..
    }) = get_job_state(state, &job_id)
    else {
        return effects.push(PartitionEffect {
            job_id: job_id.clone(),
            deets: effects::PartitionEffectDeets::AbortJob {
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
        | job_events::JobRunResult::WflowErr(JobError::Terminal { .. }) => {
            archive_job(state, &job_id);
        }
        job_events::JobRunResult::WflowErr(JobError::Transient { retry_policy, .. }) => {
            let retry_policy = retry_policy
                .clone()
                .or(override_wflow_retry_policy.clone())
                .unwrap_or(crate::partition::RetryPolicy::Immediate);
            match retry_policy {
                crate::partition::RetryPolicy::Immediate => effects.push(PartitionEffect {
                    job_id,
                    deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                        run_id: runs.len() as u64,
                    }),
                }),
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
                job_events::JobEffectResultDeets::Success { .. } => effects.push(PartitionEffect {
                    job_id,
                    deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                        run_id: runs.len() as u64,
                    }),
                }),
                job_events::JobEffectResultDeets::EffectErr(JobError::Transient {
                    retry_policy,
                    ..
                }) => match retry_policy
                    .as_ref()
                    .or(override_wflow_retry_policy.as_ref())
                    .unwrap_or(&crate::partition::RetryPolicy::Immediate)
                {
                    crate::partition::RetryPolicy::Immediate => effects.push(PartitionEffect {
                        job_id,
                        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                            run_id: runs.len() as u64,
                        }),
                    }),
                },
            }
        }
    }
}

fn archive_job<'a>(state: &'a mut state::PartitionJobsState, job_id: &Arc<str>) {
    let job_state = state.active.remove(job_id).unwrap();
    state.archive.insert(job_id.clone(), job_state);
}

fn get_job_state<'a>(
    state: &'a mut state::PartitionJobsState,
    job_id: &Arc<str>,
) -> Option<&'a mut state::JobState> {
    state.active.get_mut(job_id)
}
