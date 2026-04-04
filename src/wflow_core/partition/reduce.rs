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
            pending_messages: default(),
            active_wait: None,
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

    if let Some(wait_state) = &job_state.active_wait {
        if matches!(
            wait_state.deets,
            job_events::JobWaitResultDeets::Timer { .. }
        ) {
            effects.push(PartitionEffect {
                job_id: Arc::clone(&event.job_id),
                deets: effects::PartitionEffectDeets::CancelWait(effects::CancelWaitDeets {
                    wait_id: wait_state.wait_id,
                    reason: Arc::clone(&event.reason),
                }),
            });
        }
        job_state.active_wait = None;
        archive_job(state, &event.job_id);
        return;
    }

    job_state.cancelling = true;
    effects.push(PartitionEffect {
        job_id: event.job_id,
        deets: effects::PartitionEffectDeets::AbortRun {
            reason: event.reason,
        },
    });
}

pub fn reduce_job_message_event(
    state: &mut state::PartitionJobsState,
    effects: &mut Vec<PartitionEffect>,
    event: job_events::JobMessageEvent,
) {
    let Some(job_state) = state.active.get_mut(&event.job_id) else {
        info!("message for unknown or archived job, skipping");
        return;
    };
    job_state
        .pending_messages
        .push_back(state::JobInboxMessage {
            message_id: event.message_id,
            timestamp: event.timestamp,
            payload_json: event.payload_json,
        });

    let Some(wait_state) = job_state.active_wait.clone() else {
        return;
    };
    let run_id = wait_state.run_id;
    let preferred_worker_id = wait_state.preferred_worker_id.clone();
    match &wait_state.deets {
        job_events::JobWaitResultDeets::Message { .. } => {
            let msg = job_state
                .pending_messages
                .pop_front()
                .expect("just pushed one message");
            complete_wait_step_success_for_wait_state(
                &mut job_state.steps,
                &wait_state,
                msg.payload_json,
                event.timestamp,
            );
            job_state.active_wait = None;

            effects.push(PartitionEffect {
                job_id: Arc::clone(&event.job_id),
                deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
                    run_id,
                    preferred_worker_id,
                }),
            });
        }
        job_events::JobWaitResultDeets::Timer { .. } => {}
    }
}

pub fn reduce_job_timer_fired_event(
    state: &mut state::PartitionJobsState,
    effects: &mut Vec<PartitionEffect>,
    event: job_events::JobTimerFiredEvent,
) {
    let Some(job_state) = state.active.get_mut(&event.job_id) else {
        info!("timer fired for unknown or archived job, skipping");
        return;
    };
    let Some(wait_state) = job_state.active_wait.clone() else {
        info!("timer fired but no active wait, skipping");
        return;
    };
    let job_events::JobWaitResultDeets::Timer { wait_id, .. } = &wait_state.deets else {
        info!("timer fired but active wait is not timer, skipping");
        return;
    };
    if *wait_id != event.wait_id {
        info!("timer fired for stale wait id, skipping");
        return;
    }

    complete_wait_step_success_for_wait_state(
        &mut job_state.steps,
        &wait_state,
        "null".into(),
        event.timestamp,
    );
    job_state.active_wait = None;
    effects.push(PartitionEffect {
        job_id: event.job_id,
        deets: effects::PartitionEffectDeets::RunJob(effects::RunJobAttemptDeets {
            run_id: wait_state.run_id,
            preferred_worker_id: wait_state.preferred_worker_id.clone(),
        }),
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
        ref mut pending_messages,
        ref mut active_wait,
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
    let next_run_id = runs.len() as u64;

    let event = runs.last_mut().unwrap();
    match &event.result {
        job_events::JobRunResult::Success { .. }
        | job_events::JobRunResult::WorkerErr(_)
        | job_events::JobRunResult::WflowErr(JobError::Terminal { .. })
        | job_events::JobRunResult::Aborted => {
            *active_wait = None;
            archive_job(state, &job_id);
        }
        job_events::JobRunResult::WflowErr(JobError::Transient { retry_policy, .. }) => {
            if *cancelling {
                archive_job(state, &job_id);
            } else {
                *active_wait = None;
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
                    *active_wait = None;
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
                                    run_id: next_run_id,
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
                                            run_id: next_run_id,
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
        job_events::JobRunResult::StepWait(wait) => {
            if *cancelling {
                *active_wait = None;
                archive_job(state, &job_id);
                return;
            }

            assert!(
                active_wait.is_none(),
                "job already has an active wait; wait completion must clear before adding new wait"
            );
            let run_id = next_run_id;
            let wait_state = state::JobWaitState {
                wait_id: wait.wait_id(),
                run_id,
                preferred_worker_id: worker_id_for_hint.clone(),
                step_id: wait.step_id,
                attempt_id: wait.attempt_id,
                start_at: wait.start_at,
                deets: wait.deets.clone(),
            };
            *active_wait = Some(wait_state.clone());

            match wait.deets.clone() {
                job_events::JobWaitResultDeets::Timer { wait_id, fire_at } => {
                    effects.push(PartitionEffect {
                        job_id,
                        deets: effects::PartitionEffectDeets::WaitTimer(effects::WaitTimerDeets {
                            wait_id,
                            fire_at,
                            step_id: wait.step_id,
                            attempt_id: wait.attempt_id,
                        }),
                    });
                }
                job_events::JobWaitResultDeets::Message { .. } => {
                    if let Some(msg) = pending_messages.pop_front() {
                        complete_wait_step_success_for_wait_state(
                            steps,
                            &wait_state,
                            msg.payload_json,
                            msg.timestamp,
                        );
                        *active_wait = None;
                        effects.push(PartitionEffect {
                            job_id,
                            deets: effects::PartitionEffectDeets::RunJob(
                                effects::RunJobAttemptDeets {
                                    run_id,
                                    preferred_worker_id: wait_state.preferred_worker_id.clone(),
                                },
                            ),
                        });
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

trait WaitIdExt {
    fn wait_id(&self) -> u64;
}

impl WaitIdExt for job_events::JobWaitResult {
    fn wait_id(&self) -> u64 {
        match &self.deets {
            job_events::JobWaitResultDeets::Timer { wait_id, .. }
            | job_events::JobWaitResultDeets::Message { wait_id } => *wait_id,
        }
    }
}

fn complete_wait_step_success_for_wait_state(
    steps: &mut Vec<state::JobStepState>,
    wait_state: &state::JobWaitState,
    value_json: Arc<str>,
    end_at: Timestamp,
) {
    if steps.len() == wait_state.step_id as usize {
        steps.push(state::JobStepState::Effect {
            attempts: default(),
        });
    }
    let step = &mut steps[wait_state.step_id as usize];
    let state::JobStepState::Effect { attempts } = step;
    assert!((wait_state.attempt_id as usize) == attempts.len());
    attempts.push(job_events::JobEffectResult {
        step_id: wait_state.step_id,
        attempt_id: wait_state.attempt_id,
        start_at: wait_state.start_at,
        end_at,
        deets: job_events::JobEffectResultDeets::Success { value_json },
    });
}
