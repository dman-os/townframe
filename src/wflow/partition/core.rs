use crate::interlude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobEvent {
    Init {
        job_id: Arc<str>,
        args_json: Arc<str>,
        meta: crate::plugin::bindings_metadata_store::townframe::wflow::metadata_store::WflowMeta,
    },
    EffectAttempt {
        job_id: Arc<str>,
        step_id: u64,
        attempt_id: u64,
        started_at: OffsetDateTime,
        completed_at: Option<OffsetDateTime>,
    },
    EffectValue {
        job_id: Arc<str>,
        step_id: u64,
        attempt_id: u64,
        value_json: Vec<u8>,
    },
    EffectError {
        job_id: Arc<str>,
        step_id: u64,
        attempt_id: u64,
        error: JobError,
    },
    Error {
        job_id: Arc<str>,
        error: JobError,
    },
    Fin {
        job_id: Arc<str>,
        results_json: Arc<str>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobError {
    Transient {
        error_json: Vec<u8>,
        retry_policy: RetryPolicy,
    },
    Terminal {
        error_json: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetryPolicy {
    Immediate,
}

pub struct JobState {
    status: JobStatus,
    steps: Vec<JobStepState>,
}

pub enum JobStatus {
    Running,
    Completed,
    Failed,
}

enum JobStepState {
    Effect {
        attempts: Vec<JobEffectAttemptState>,
    },
}

enum JobEffectAttemptState {
    Ongoing { started_at: OffsetDateTime },
    Error { error: JobError },
    Completed { result: Arc<str> },
}

impl JobState {
    fn apply(&mut self, event: JobEvent) {
        match event {
            JobEvent::Init {
                job_id: _,
                args_json: _,
                meta: _,
            } => {
                self.status = JobStatus::Running;
            }
            JobEvent::EffectAttempt {
                job_id: _,
                step_id,
                attempt_id,
                started_at,
                completed_at: _,
            } => {
                // Ensure we have enough steps
                while self.steps.len() <= step_id as usize {
                    self.steps.push(JobStepState::Effect {
                        attempts: Vec::new(),
                    });
                }
                // Ensure we have enough attempts for this step
                let JobStepState::Effect { attempts } = &mut self.steps[step_id as usize];
                while attempts.len() <= attempt_id as usize {
                    attempts.push(JobEffectAttemptState::Ongoing {
                        started_at: OffsetDateTime::now_utc(),
                    });
                }
                // Update the attempt with the actual started_at time
                attempts[attempt_id as usize] = JobEffectAttemptState::Ongoing { started_at };
            }
            JobEvent::EffectValue {
                job_id: _,
                step_id,
                attempt_id,
                value_json,
            } => {
                // Convert value_json to Arc<str> for storage
                let result = String::from_utf8(value_json)
                    .map(Arc::from)
                    .unwrap_or_else(|_| Arc::from(""));

                if let Some(JobStepState::Effect { attempts }) =
                    self.steps.get_mut(step_id as usize)
                {
                    if let Some(attempt) = attempts.get_mut(attempt_id as usize) {
                        *attempt = JobEffectAttemptState::Completed { result };
                    }
                }
            }
            JobEvent::EffectError {
                job_id: _,
                step_id,
                attempt_id,
                error,
            } => {
                if let Some(JobStepState::Effect { attempts }) =
                    self.steps.get_mut(step_id as usize)
                {
                    if let Some(attempt) = attempts.get_mut(attempt_id as usize) {
                        *attempt = JobEffectAttemptState::Error { error };
                    }
                }
            }
            JobEvent::Error {
                job_id: _,
                error: _,
            } => {
                self.status = JobStatus::Failed;
            }
            JobEvent::Fin {
                job_id: _,
                results_json: _,
            } => {
                self.status = JobStatus::Completed;
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum JobTapeEffect {
    InitJob { job_id: Arc<str>, args: Arc<str> },
}

pub fn move_job_tape_head(_state: &JobState, evt: &JobEvent, effects: &mut Vec<JobTapeEffect>) {
    match evt {
        JobEvent::Init {
            job_id,
            args_json: args,
            meta: _,
        } => {
            effects.push(JobTapeEffect::InitJob {
                job_id: job_id.clone(),
                args: args.clone(),
            });
        }
        JobEvent::EffectAttempt {
            job_id: _,
            step_id: _,
            attempt_id: _,
            started_at: _,
            completed_at: _,
        } => {
            // Effects for EffectAttempt can be added here if needed
        }
        JobEvent::EffectValue {
            job_id: _,
            step_id: _,
            attempt_id: _,
            value_json: _,
        } => {
            // Effects for EffectValue can be added here if needed
        }
        JobEvent::EffectError {
            job_id: _,
            step_id: _,
            attempt_id: _,
            error: _,
        } => {
            // Effects for EffectError can be added here if needed
        }
        JobEvent::Error {
            job_id: _,
            error: _,
        } => {
            // Effects for Error can be added here if needed
        }
        JobEvent::Fin {
            job_id: _,
            results_json: _,
        } => {
            // Effects for Fin can be added here if needed
        }
    }
}

// Partition-level effects (used by partition.rs)
#[derive(Debug, Serialize, Deserialize)]
pub enum PartitionEffect {
    LookupWflowForInvocation(JobEvent),
}

pub type PartitionEvent = JobEvent;

pub fn reduce(evt: JobEvent, effects: &mut Vec<PartitionEffect>) {
    // Convert JobTapeEffect to PartitionEffect if needed
    // For now, we'll create a temporary state and use move_job_tape_head
    // In a real implementation, you'd want to maintain state across calls
    let state = JobState {
        status: JobStatus::Running,
        steps: Vec::new(),
    };
    let mut job_effects = Vec::new();
    move_job_tape_head(&state, &evt, &mut job_effects);

    // Convert job effects to partition effects
    // For Init events, we might want to trigger a lookup
    match evt {
        JobEvent::Init { .. } => {
            effects.push(PartitionEffect::LookupWflowForInvocation(evt));
        }
        _ => {
            // Other effects can be added here as needed
        }
    }
}
