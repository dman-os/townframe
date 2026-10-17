//! Propagated per-request state: identity, tracing, budget, and cancellation.
//!
//! [`RequestContext`] is the native equivalent of the HTTP request metadata
//! every transport shares. It travels inside an [`Envelope`](crate::Envelope)
//! and is chained Go-style with [`RequestContext::child`] when one service
//! calls another on the same request.

use crate::admission::{CancellationSignal, CancellationTransport};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Errors found before a service handler is admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ContextError {
    #[error("request context is missing a request id")]
    MissingRequestId,
    #[error("request context is missing a trace id")]
    MissingTraceId,
    #[error("request was cancelled")]
    Cancelled,
    #[error("request cancellation key is already admitted")]
    CancellationAlreadyAdmitted,
    #[error("request cancellation scope capacity exhausted (limit {limit})")]
    CancellationCapacityExceeded { limit: usize },
    #[error("subscription capacity exhausted (limit {limit})")]
    StreamCapacityExceeded { limit: usize },
    #[error("request exceeded its time budget")]
    DeadlineExceeded,
}

/// Transport metadata carried beside domain parameters. The HTTP adapter maps
/// these fields to headers; native callers carry the same typed values without
/// putting them into domain JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpMetadata {
    pub authorization: Option<String>,
    pub idempotency_key: Option<String>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
    pub timeout: Option<Duration>,
}

/// Propagated state for a native request. `time_budget` is the serializable
/// transport value; `deadline` is rebuilt by a receiver when it accepts the
/// envelope locally, so elapsed budget is never reset by a hop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub trace_id: Uuid,
    pub span_id: Uuid,
    pub parent_span_id: Option<Uuid>,
    pub command_id: Option<Uuid>,
    pub time_budget: Option<Duration>,
    pub cancellation: CancellationSignal,
    /// Absolute wall-clock deadline carried across serialized boundaries.
    /// Receiver-local `Instant` state is rebuilt from this value.
    deadline_unix_ms: Option<u64>,
    #[serde(skip)]
    deadline: Option<Instant>,
}

impl RequestContext {
    pub fn root() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            trace_id: Uuid::now_v7(),
            span_id: Uuid::now_v7(),
            parent_span_id: None,
            command_id: None,
            time_budget: None,
            cancellation: CancellationSignal::default(),
            deadline_unix_ms: None,
            deadline: None,
        }
    }

    /// A child span on the same request: shares identity, budget, and
    /// cancellation with its parent.
    pub fn child(&self) -> Self {
        Self {
            request_id: self.request_id,
            trace_id: self.trace_id,
            span_id: Uuid::now_v7(),
            parent_span_id: Some(self.span_id),
            command_id: self.command_id,
            time_budget: self.time_budget,
            cancellation: self.cancellation.clone(),
            deadline_unix_ms: self.deadline_unix_ms,
            deadline: self.deadline,
        }
    }

    pub fn with_command_id(mut self, command_id: Uuid) -> Self {
        self.command_id = Some(command_id);
        self
    }

    pub fn with_time_budget(mut self, time_budget: Duration) -> Self {
        self.time_budget = Some(time_budget);
        self.deadline = Some(Instant::now() + time_budget);
        self.deadline_unix_ms =
            Some(wall_clock_millis().saturating_add(time_budget.as_millis() as u64));
        self
    }

    /// Rebuilds receiver-local deadline state without resetting elapsed budget.
    pub fn for_receiver(mut self) -> Self {
        self.deadline = self.deadline_unix_ms.map(|deadline| {
            let remaining = deadline.saturating_sub(wall_clock_millis());
            Instant::now() + Duration::from_millis(remaining)
        });
        self
    }

    /// Attaches a caller-owned cancellation transport. [`crate::Client`] opens
    /// and closes the request scope around an IRPC call; a caller that
    /// serializes a context separately must hold
    /// [`CancellationSignal::begin_transport_request`] until that request's
    /// terminal result or drop.
    pub fn with_cancellation_transport(mut self, transport: CancellationTransport) -> Self {
        self.cancellation = self.cancellation.bound_to(transport);
        self
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn validate(&self) -> Result<(), ContextError> {
        if self.request_id.is_nil() {
            return Err(ContextError::MissingRequestId);
        }
        if self.trace_id.is_nil() {
            return Err(ContextError::MissingTraceId);
        }
        if self.cancellation.is_cancelled() {
            return Err(ContextError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
            || self
                .deadline_unix_ms
                .is_some_and(|deadline| wall_clock_millis() >= deadline)
        {
            return Err(ContextError::DeadlineExceeded);
        }
        Ok(())
    }
}

fn wall_clock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after UNIX epoch")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::cancellation_channel;

    #[test]
    fn context_carries_command_budget_and_cancellation() {
        let command_id = Uuid::now_v7();
        let context = RequestContext::root()
            .with_command_id(command_id)
            .with_time_budget(Duration::from_secs(1));
        let child = context.child();
        assert_eq!(child.command_id, Some(command_id));
        assert_eq!(child.time_budget, context.time_budget);
        assert!(child.validate().is_ok());

        child.cancel();
        assert_eq!(child.validate(), Err(ContextError::Cancelled));
        assert_eq!(context.validate(), Err(ContextError::Cancelled));

        let expired = RequestContext::root().with_time_budget(Duration::ZERO);
        assert_eq!(expired.validate(), Err(ContextError::DeadlineExceeded));
    }

    #[tokio::test]
    async fn cancellation_uses_transport_owned_state() {
        let (transport, _receiver) = cancellation_channel(8);
        let context = RequestContext::root().with_cancellation_transport(transport);
        let _request = context
            .cancellation
            .begin_transport_request()
            .expect("scope result")
            .expect("transport scope");
        context.cancel();
        assert_eq!(context.validate(), Err(ContextError::Cancelled));
    }

    #[test]
    fn serialized_context_preserves_elapsed_budget_and_cancellation() {
        let context = RequestContext::root().with_time_budget(Duration::from_millis(5));
        let wire = serde_json::to_vec(&context).expect("context serializes");
        std::thread::sleep(Duration::from_millis(10));
        let received: RequestContext = serde_json::from_slice(&wire).expect("context deserializes");
        assert_eq!(
            received.for_receiver().validate(),
            Err(ContextError::DeadlineExceeded)
        );

        let context = RequestContext::root();
        context.cancel();
        let wire = serde_json::to_vec(&context).expect("context serializes");
        let received: RequestContext = serde_json::from_slice(&wire).expect("context deserializes");
        assert_eq!(received.validate(), Err(ContextError::Cancelled));
    }
}
