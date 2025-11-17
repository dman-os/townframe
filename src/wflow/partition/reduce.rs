use crate::interlude::*;

use crate::partition::effects::PartitionEffect;
use crate::partition::job_events::JobError;
use crate::partition::{effects, job_events, state};

struct ReduceCtx<'a> {
    state: &'a state::PartitionJobsState,
    timestamp: OffsetDateTime,
    job_id: Arc<str>,
    effects: &'a mut Vec<PartitionEffect>,
}

pub fn reduce_job_event(
    state: &state::PartitionJobsState,
    event: job_events::JobEvent,
    effects: &mut Vec<PartitionEffect>,
) {
    let cx = ReduceCtx {
        state,
        timestamp: event.timestamp,
        job_id: event.job_id,
        effects,
    };
    match event.deets {
        job_events::JobEventDeets::Init(deets) => match reduce_job_init_event(cx, deets) {
            Ok(val) => effects.push(val),
            Err(val) => effects.push(val),
        },
        job_events::JobEventDeets::Run(deets) => match reduce_job_run_event(cx, deets) {
            Ok(None) => {}
            Ok(Some(val)) => effects.push(val),
            Err(val) => effects.push(val),
        },
    }
}

fn reduce_job_init_event(
    ReduceCtx { state, job_id, .. }: ReduceCtx,
    deets: job_events::JobInitEvent,
) -> Result<PartitionEffect, PartitionEffect> {
    if state.active.contains_key(&job_id) {
        return Err(PartitionEffect {
            job_id: job_id,
            deets: effects::PartitionEffectDeets::AbortJob {
                reason: "duplicate job id".into(),
            },
        });
    }

    state.active.insert(
        job_id.clone(),
        state::JobState {
            init_args_json: deets.args_json.clone(),
            override_wflow_retry_policy: deets.override_wflow_retry_policy,
            wflow: deets.wflow,
            runs: default(),
            steps: default(),
        },
    );

    Ok(PartitionEffect {
        job_id,
        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets { run_id: 0 }),
    })
}

fn get_job_state<'a>(
    state: &'a state::PartitionJobsState,
    job_id: &Arc<str>,
) -> Result<DHashMapMutRef<'a, Arc<str>, state::JobState>, PartitionEffect> {
    let Some(job_state) = state.active.get_mut(job_id) else {
        return Err(PartitionEffect {
            job_id: job_id.clone(),
            deets: effects::PartitionEffectDeets::AbortJob {
                reason: "event for unrecognized job".into(),
            },
        });
    };
    Ok(job_state)
}

fn reduce_job_run_event(
    ReduceCtx { state, job_id, .. }: ReduceCtx,
    event: job_events::JobRunEvent,
) -> Result<Option<PartitionEffect>, PartitionEffect> {
    // deref destructure to avoid borrow checker
    // confusin on cross field refs
    let state::JobState {
        ref mut runs,
        ref mut steps,
        ref override_wflow_retry_policy,
        ..
    } = &mut *get_job_state(&state, &job_id)?;

    assert!((event.run_id as usize) == runs.len());
    runs.push(event);

    let event = runs.last_mut().unwrap();

    let effect = match &event.result {
        // that's it for the job, archive it
        job_events::JobRunResult::Success { .. }
        | job_events::JobRunResult::WorkerErr(_)
        | job_events::JobRunResult::WflowErr(JobError::Terminal { .. }) => {
            let (_, job_state) = state.active.remove(&job_id).unwrap();
            state.archive.insert(job_id, job_state);
            None
        }
        // try another run
        job_events::JobRunResult::WflowErr(JobError::Transient { retry_policy, .. }) => {
            // FIXME: double clone
            match retry_policy
                .clone()
                .or(override_wflow_retry_policy.clone())
                .unwrap_or(crate::partition::RetryPolicy::Immediate)
            {
                crate::partition::RetryPolicy::Immediate => Some(PartitionEffect {
                    job_id,
                    deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                        run_id: runs.len() as u64,
                    }),
                }),
            }
        }
        // steps require special attention
        job_events::JobRunResult::StepEffect(res) => {
            // update our state first for quick ref later
            {
                if steps.len() == res.step_id as usize {
                    steps.push(state::JobStepState::Effect {
                        attempts: default(),
                    });
                }
                let step = &mut steps[res.step_id as usize];
                let state::JobStepState::Effect { attempts } = step;
                assert!((res.attempt_id as usize) == attempts.len());
                attempts.push(res.clone());
            }
            match &res.deets {
                // no more job runs, archive it
                job_events::JobEffectResultDeets::EffectErr(JobError::Terminal { .. }) => {
                    let (_, job_state) = state.active.remove(&job_id).unwrap();
                    state.archive.insert(job_id, job_state);
                    None
                }
                // step succeeded, let's run again
                job_events::JobEffectResultDeets::Success { .. } => Some(PartitionEffect {
                    job_id,
                    deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                        run_id: runs.len() as u64,
                    }),
                }),
                job_events::JobEffectResultDeets::EffectErr(JobError::Transient {
                    retry_policy,
                    ..
                }) => {
                    match retry_policy
                        .as_ref()
                        .or(override_wflow_retry_policy.as_ref())
                        .unwrap_or(&crate::partition::RetryPolicy::Immediate)
                    {
                        crate::partition::RetryPolicy::Immediate => Some(PartitionEffect {
                            job_id,
                            deets: effects::PartitionEffectDeets::RunJob(
                                effects::RunJobAttemptDeets {
                                    run_id: runs.len() as u64,
                                },
                            ),
                        }),
                    }
                }
            }
        }
    };

    Ok(effect)
}
