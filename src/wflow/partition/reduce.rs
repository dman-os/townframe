use crate::interlude::*;

use crate::partition::effects::PartitionEffect;
use crate::partition::job_events::JobError;
use crate::partition::{effects, job_events, state};

struct ReduceCtx<'a> {
    state: &'a mut state::PartitionJobsState,
    timestamp: OffsetDateTime,
    job_id: Arc<str>,
    effects: &'a mut Vec<PartitionEffect>,
}

pub fn reduce_job_event(
    state: &mut state::PartitionJobsState,
    event: job_events::JobEvent,
    effects: &mut Vec<PartitionEffect>,
) {
    let mut cx = ReduceCtx {
        state,
        timestamp: event.timestamp,
        job_id: Arc::clone(&event.job_id),
        effects,
    };
    match event.deets {
        job_events::JobEventDeets::Init(deets) => match reduce_job_init_event(&mut cx, deets) {
            Ok(val) => cx.effects.push(val),
            Err(val) => cx.effects.push(val),
        },
        job_events::JobEventDeets::Run(deets) => match reduce_job_run_event(&mut cx, deets) {
            Ok(None) => {}
            Ok(Some(val)) => cx.effects.push(val),
            Err(val) => cx.effects.push(val),
        },
    }
}

fn reduce_job_init_event(
    cx: &mut ReduceCtx<'_>,
    deets: job_events::JobInitEvent,
) -> Result<PartitionEffect, PartitionEffect> {
    if cx.state.active.contains_key(&cx.job_id) {
        return Err(PartitionEffect {
            job_id: cx.job_id.clone(),
            deets: effects::PartitionEffectDeets::AbortJob {
                reason: "duplicate job id".into(),
            },
        });
    }

    cx.state.active.insert(
        cx.job_id.clone(),
        state::JobState {
            init_args_json: deets.args_json.clone(),
            override_wflow_retry_policy: deets.override_wflow_retry_policy,
            wflow: deets.wflow,
            runs: default(),
            steps: default(),
        },
    );

    Ok(PartitionEffect {
        job_id: cx.job_id.clone(),
        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets { run_id: 0 }),
    })
}

fn reduce_job_run_event(
    ctx: &mut ReduceCtx<'_>,
    event: job_events::JobRunEvent,
) -> Result<Option<PartitionEffect>, PartitionEffect> {
    let job_id = ctx.job_id.clone();
    let state::JobState {
        ref mut runs,
        ref mut steps,
        ref override_wflow_retry_policy,
        ..
    } = get_job_state(ctx.state, &job_id)?;

    assert!((event.run_id as usize) == runs.len());
    runs.push(event);

    let event = runs.last_mut().unwrap();
    let effect = match &event.result {
        job_events::JobRunResult::Success { .. }
        | job_events::JobRunResult::WorkerErr(_)
        | job_events::JobRunResult::WflowErr(JobError::Terminal { .. }) => {
            let job_state = ctx.state.active.remove(&job_id).unwrap();
            ctx.state.archive.insert(job_id.clone(), job_state);
            None
        }
        job_events::JobRunResult::WflowErr(JobError::Transient { retry_policy, .. }) => {
            let retry_policy = retry_policy
                .clone()
                .or(override_wflow_retry_policy.clone())
                .unwrap_or(crate::partition::RetryPolicy::Immediate);
            match retry_policy {
                crate::partition::RetryPolicy::Immediate => Some(PartitionEffect {
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
                    let job_state = ctx.state.active.remove(&job_id).unwrap();
                    ctx.state.archive.insert(job_id.clone(), job_state);
                    None
                }
                job_events::JobEffectResultDeets::Success { .. } => Some(PartitionEffect {
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
                    crate::partition::RetryPolicy::Immediate => Some(PartitionEffect {
                        job_id,
                        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                            run_id: runs.len() as u64,
                        }),
                    }),
                },
            }
        }
    };

    Ok(effect)
}

fn get_job_state<'a>(
    state: &'a mut state::PartitionJobsState,
    job_id: &Arc<str>,
) -> Result<&'a mut state::JobState, PartitionEffect> {
    state.active.get_mut(job_id).ok_or_else(|| PartitionEffect {
        job_id: job_id.clone(),
        deets: effects::PartitionEffectDeets::AbortJob {
            reason: "event for unrecognized job".into(),
        },
    })
}
