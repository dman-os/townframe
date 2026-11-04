mod interlude {
    pub use utils_rs::prelude::*;
}

use crate::interlude::*;

#[expect(unused)]
pub mod r#gen;
pub mod kvstore;
pub mod log;
pub mod metastore;
pub mod partition;
pub mod snapstore;

pub type StepId = u64;

/// The kind of host boundary that produced or was entered at a workflow step.
/// Replay checks this so an invocation that reaches a committed index with
/// another boundary kind is rejected as nondeterministic instead of reading a
/// foreign value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepKind {
    Effect,
    Sleep,
    Recv,
    Defer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepMeta {
    pub kind: StepKind,
    pub name: Option<String>,
}

impl StepMeta {
    pub fn new(kind: StepKind, name: Option<&str>) -> Self {
        Self {
            kind,
            name: name.map(str::to_owned),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveStepState {
    pub id: StepId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedStepState {
    pub id: StepId,
    pub value_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepState {
    Completed(CompletedStepState),
    Active(ActiveStepState),
}
