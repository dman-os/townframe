//! What the sample service says on the wire: its request types, outputs,
//! events, error vocabulary, and the irpc protocol that carries them.
//!
//! Nothing here knows how the service is implemented. A consumer that only
//! talks to the sample over IRPC or HTTP needs exactly this module.

use crate::ContextError;
use crate::HttpMetadata;
use crate::RequestContext;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueSnapshot {
    pub value: String,
    pub source_revision: u64,
    pub service_revision: u64,
}

/// A service observation, not a durable semantic event journal. Consumers
/// that miss it reread the current interpreted state from the source cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleEvent {
    pub source_revision: u64,
    pub service_revision: u64,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum SampleError {
    #[error("request context failed: {0}")]
    Context(#[from] ContextError),
    #[error("invalid value: {message}")]
    InvalidValue { message: String },
    #[error("subscription lagged by {skipped}; reread current service state")]
    SubscriptionLagged { skipped: u64 },
}

/// Query: the current interpreted value and the context and metadata the
/// handler saw, so a caller can check what each transport delivered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetValue {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetValueOutput {
    pub snapshot: ValueSnapshot,
    pub observed_context: RequestContext,
    pub observed_metadata: HttpMetadata,
}

crate::service_operation!(GetValue => GetValueOutput, SampleError, query("org.example.sample.getValue"));

/// Procedure: write the source and wait until the projector has interpreted it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutValue {
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutValueOutput {
    pub snapshot: ValueSnapshot,
}

crate::service_operation!(PutValue => PutValueOutput, SampleError, procedure("org.example.sample.putValue"));

/// Query: the revisioned read a lagged observer uses to catch up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadState {
    pub after_source_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatePage {
    pub snapshot: ValueSnapshot,
    pub reset_required: bool,
}

crate::service_operation!(ReadState => StatePage, SampleError, query("org.example.sample.readState"));

/// Subscription: interpreted-state changes after a source revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeEvents {
    pub after_source_revision: u64,
}

crate::service_operation!(SubscribeEvents => stream SampleEvent, SampleError, "org.example.sample.subscribeEvents");

crate::service_protocol! {
    pub enum SampleProtocol => SampleMessage {
        unary { GetValue(GetValue), PutValue(PutValue), ReadState(ReadState) }
        stream { SubscribeEvents(SubscribeEvents) }
    }
}
