use crate::StepKind;
use crate::StepMeta;
use crate::interlude::*;
use crate::types;

#[derive(Debug)]
pub enum SessionYield {
    PersistStep {
        step_id: u64,
        start_at: Timestamp,
        end_at: Timestamp,
        value_json: Arc<str>,
        /// The boundary the workflow entered, recorded durably so a replay that
        /// renames or re-kinds the step is rejected as divergent.
        meta: StepMeta,
    },
    WaitStep {
        step_id: u64,
        start_at: Timestamp,
        deets: WaitYield,
        meta: StepMeta,
    },
    Complete(types::JobResult),
    Panicked(String),
}

#[derive(Debug)]
pub enum WaitYield {
    Timer { wait_id: u64, fire_at: Timestamp },
    Message { wait_id: u64 },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayStep {
    /// `None` when the replay source records no kind: the legacy partition
    /// journal only pins the kind of steps that durably waited.
    pub kind: Option<StepKind>,
    /// `None` when the replay source records no name; `Some(None)` pins the
    /// step as durably unnamed, `Some(Some(name))` pins its boundary name.
    pub name: Option<Option<Arc<str>>>,
    pub value_json: Option<Arc<str>>,
}

impl ReplayStep {
    pub fn completed(value_json: impl Into<Arc<str>>) -> Self {
        Self {
            value_json: Some(value_json.into()),
            ..default()
        }
    }

    /// A replay step whose identity is fully pinned by a durable journal.
    pub fn durable(meta: &StepMeta, value_json: impl Into<Arc<str>>) -> Self {
        Self {
            kind: Some(meta.kind),
            name: Some(meta.name.as_deref().map(Arc::from)),
            value_json: Some(value_json.into()),
        }
    }
}
