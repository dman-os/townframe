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
        job_events::JobEventDeets::Effect(deets) => match reduce_job_effect_event(cx, deets) {
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
        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
            run_id: 0,
            args_json: deets.args_json,
        }),
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
    deets: job_events::JobRunEvent,
) -> Result<Option<PartitionEffect>, PartitionEffect> {
    let mut job_state = get_job_state(&state, &job_id)?;

    assert!((deets.run_id as usize) == job_state.runs.len());
    job_state.runs.push(deets);

    let deets = &job_state.runs[job_state.runs.len()];

    match &deets.result {
        job_events::JobRunResult::Success { .. }
        | job_events::JobRunResult::WorkerErr(_)
        | job_events::JobRunResult::WflowErr(JobError::Terminal { .. }) => {
            drop(job_state);
            let (_, job_state) = state.active.remove(&job_id).unwrap();
            state.archive.insert(job_id, job_state);
            Ok(None)
        }
        job_events::JobRunResult::WflowErr(JobError::Transient { retry_policy, .. }) => {
            match retry_policy
                .as_ref()
                .or(job_state.override_wflow_retry_policy.as_ref())
                .unwrap_or(&crate::partition::RetryPolicy::Immediate)
            {
                crate::partition::RetryPolicy::Immediate => Ok(Some(PartitionEffect {
                    job_id,
                    deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                        run_id: job_state.runs.len() as u64,
                        args_json: job_state.init_args_json.clone(),
                    }),
                })),
            }
        }
        job_events::JobRunResult::EffectInterrupt { .. } => Ok(Some(PartitionEffect {
            job_id,
            deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                run_id: job_state.runs.len() as u64,
                args_json: job_state.init_args_json.clone(),
            }),
        })),
    }
}

fn reduce_job_effect_event(
    ReduceCtx { state, job_id, .. }: ReduceCtx,
    deets: job_events::JobEffectEvent,
) -> Result<Option<PartitionEffect>, PartitionEffect> {
    let mut job_state = get_job_state(&state, &job_id)?;
    assert!((deets.step_id as usize) >= (job_state.steps.len() - 1));

    let step_id = deets.step_id;
    if job_state.steps.len() == step_id as usize {
        job_state.steps.push(state::JobStepState::Effect {
            attempts: default(),
        });
    }
    {
        let step = &mut job_state.steps[deets.step_id as usize];
        let state::JobStepState::Effect { attempts } = step;
        assert!((deets.attempt_id as usize) == attempts.len());
        attempts.push(deets);
    }
    let step = &job_state.steps[step_id as usize];
    let state::JobStepState::Effect { attempts } = step;
    let deets = &attempts[attempts.len()];

    match &deets.result {
        job_events::JobEffectResult::Success { .. } => Ok(None),
        job_events::JobEffectResult::EffectErr(JobError::Terminal { .. }) => {
            drop(job_state);
            let (_, job_state) = state.active.remove(&job_id).unwrap();
            state.archive.insert(job_id, job_state);
            Ok(None)
        }
        job_events::JobEffectResult::EffectErr(JobError::Transient { retry_policy, .. }) => {
            match retry_policy
                .as_ref()
                .or(job_state.override_wflow_retry_policy.as_ref())
                .unwrap_or(&crate::partition::RetryPolicy::Immediate)
            {
                crate::partition::RetryPolicy::Immediate => Ok(Some(PartitionEffect {
                    job_id,
                    deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                        run_id: job_state.runs.len() as u64,
                        args_json: job_state.init_args_json.clone(),
                    }),
                })),
            }
        }
    }
}
