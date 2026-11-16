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
    /// The invocation stopped before its durable boundary stream was exhausted,
    /// or entered a boundary that disagrees with it, as seen by the adapter.
    ///
    /// A divergence the adapter can see never reaches the guest as an engine
    /// outcome: the guest's own result taxonomy has no engine class, so the
    /// adapter reports it here instead and the runtime settles the job with the
    /// same durable fault an authority-side divergence produces.
    ReplayDiverged(ReplayDivergence),
    /// The invocation returned `result` after traversing `steps` durable
    /// boundaries.
    ///
    /// `steps` is the session's own cursor, not the durable log length: the
    /// runtime forwards it as the completion's expected step count, so a
    /// workflow that stops early is fenced instead of committing a receipt over
    /// boundaries it never executed.
    Complete {
        steps: u64,
        result: types::JobResult,
    },
    Panicked(String),
}

/// One adapter-observed replay divergence, in the terms the authority can
/// re-validate from its own log: the position that diverged and the boundary
/// kind the invocation offered there.
///
/// The adapter's richer error (the expected kind and name) is deliberately not
/// carried: it is not part of the durable fault record, and the two adapters
/// must settle an identical fork into an identical receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayDivergence {
    pub step: u64,
    pub replayed: StepKind,
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
