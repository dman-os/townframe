//! What the sample service says on the wire: its request types, outputs,
//! events, error vocabulary, and the irpc protocol that carries them.
//!
//! Nothing here knows how the service is implemented. A consumer that only
//! talks to the sample over IRPC or HTTP needs exactly this module.

use crate::{
    ContextError, Dispatch, Envelope, EventReply, EventStream, Handler, Host, HttpMetadata,
    Operation, Reply, RequestContext, Route, Subscription, XrpcOperation,
    xrpc::{XrpcRegistrationError, XrpcRouter},
};
use futures::future::BoxFuture;
use irpc::rpc_requests;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

/// The irpc protocol. Each variant wraps one request in an [`Envelope`] and
/// names its reply channel through the request's [`Operation`] impl, so the
/// output and error types are never repeated here.
#[rpc_requests(message = SampleMessage)]
#[derive(Debug, Serialize, Deserialize)]
pub enum SampleProtocol {
    #[rpc(tx = Reply<GetValue>)]
    GetValue(Envelope<GetValue>),
    #[rpc(tx = Reply<PutValue>)]
    PutValue(Envelope<PutValue>),
    #[rpc(tx = Reply<ReadState>)]
    ReadState(Envelope<ReadState>),
    #[rpc(tx = EventReply<SubscribeEvents>)]
    SubscribeEvents(Envelope<SubscribeEvents>),
}

/// Any handler stack that serves all four operations can host this protocol.
impl<H> Route<H> for SampleProtocol
where
    H: Handler<GetValue> + Handler<PutValue> + Handler<ReadState> + Handler<SubscribeEvents>,
{
    fn route(
        host: Arc<Host<H>>,
        dispatch: Dispatch,
        message: SampleMessage,
    ) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            match message {
                SampleMessage::GetValue(message) => host.unary(dispatch, message).await,
                SampleMessage::PutValue(message) => host.unary(dispatch, message).await,
                SampleMessage::ReadState(message) => host.unary(dispatch, message).await,
                SampleMessage::SubscribeEvents(message) => host.stream(dispatch, message).await,
            }
        })
    }

    fn mount(router: &mut XrpcRouter, host: Arc<Host<H>>) -> Result<(), XrpcRegistrationError> {
        router.mount::<GetValue, H>(Arc::clone(&host))?;
        router.mount::<PutValue, H>(Arc::clone(&host))?;
        router.mount::<ReadState, H>(host)
    }
}
