//! What the sample service says on the wire: its request types, outputs,
//! events, error vocabulary, and the irpc protocol that carries them.
//!
//! Nothing here knows how the service is implemented. A consumer that only
//! talks to the sample over IRPC or HTTP needs exactly this module.

use crate::{
    ContextError, EventStream, HttpMetadata, Operation, RequestContext, Subscription, XrpcOperation,
};
use serde::{Deserialize, Serialize};

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

impl Operation for GetValue {
    type Output = GetValueOutput;
    type Error = SampleError;
    const XRPC: XrpcOperation = XrpcOperation::query("org.example.sample.getValue")
        .with_schemas("GetValueOutput", "SampleError");
}

/// Procedure: write the source and wait until the projector has interpreted it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutValue {
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PutValueOutput {
    pub snapshot: ValueSnapshot,
}

impl Operation for PutValue {
    type Output = PutValueOutput;
    type Error = SampleError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.sample.putValue")
        .with_schemas("PutValueOutput", "SampleError");
}

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

impl Operation for ReadState {
    type Output = StatePage;
    type Error = SampleError;
    const XRPC: XrpcOperation = XrpcOperation::query("org.example.sample.readState")
        .with_schemas("StatePage", "SampleError");
}

/// Subscription: interpreted-state changes after a source revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeEvents {
    pub after_source_revision: u64,
}

impl Operation for SubscribeEvents {
    type Output = EventStream<SampleEvent, SampleError>;
    type Error = SampleError;
    const XRPC: XrpcOperation = XrpcOperation::subscription("org.example.sample.subscribeEvents")
        .with_schemas("SampleEvent", "SampleError");
}

impl Subscription for SubscribeEvents {
    type Event = SampleEvent;
}

crate::service_protocol! {
    pub enum SampleProtocol => SampleMessage {
        unary { GetValue(GetValue), PutValue(PutValue), ReadState(ReadState) }
        stream { SubscribeEvents(SubscribeEvents) }
    }
}
