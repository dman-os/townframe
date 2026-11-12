use crate::cancellation::CancellationSignal;
use crate::cancellation::CancellationTransport;
use method_core::ContextError;
use opentelemetry::Context;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::time::Instant;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpMetadata {
    pub authorization: Option<String>,
    pub idempotency_key: Option<String>,
    /// The `Range` of a request for part of a byte body (ADR 042 §2). Only a method whose
    /// output is bytes reads it, so a method that does not ignores it, as a server may ignore
    /// a `Range` it cannot honour.
    #[serde(default)]
    pub range: Option<method_core::ByteRange>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceContext {
    headers: HashMap<String, String>,
}

impl TraceContext {
    pub fn current() -> Self {
        let mut headers = HashMap::new();
        TraceContextPropagator::new()
            .inject_context(&tracing::Span::current().context(), &mut headers);
        Self { headers }
    }

    pub fn from_headers(traceparent: Option<&str>, tracestate: Option<&str>) -> Self {
        let mut headers = HashMap::new();
        if let Some(value) = traceparent {
            headers.insert("traceparent".to_owned(), value.to_owned());
        }
        if let Some(value) = tracestate {
            headers.insert("tracestate".to_owned(), value.to_owned());
        }
        let context = TraceContextPropagator::new().extract_with_context(&Context::new(), &headers);
        headers.clear();
        TraceContextPropagator::new().inject_context(&context, &mut headers);
        Self { headers }
    }

    /// The trace of the next hop: the same trace id and flags, a fresh span id, so the hop's
    /// span is a child of this one (W3C Trace Context). No trace stays no trace: a hop does not
    /// invent one. A random span id comes from the random bits of a v7 uuid.
    pub fn child_span(&self) -> Self {
        let Some(traceparent) = self.traceparent() else {
            return self.clone();
        };
        let mut parts = traceparent.split('-');
        let (Some(version), Some(trace_id), Some(_parent_span), Some(flags)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return self.clone();
        };
        let span_id = u64::from_be_bytes(
            Uuid::now_v7().as_bytes()[8..]
                .try_into()
                .expect("a uuid has 8 trailing bytes"),
        )
        .max(1);
        Self::from_headers(
            Some(&format!("{version}-{trace_id}-{span_id:016x}-{flags}")),
            self.tracestate(),
        )
    }

    pub fn traceparent(&self) -> Option<&str> {
        self.headers.get("traceparent").map(String::as_str)
    }

    pub fn tracestate(&self) -> Option<&str> {
        self.headers.get("tracestate").map(String::as_str)
    }

    pub fn parent(&self) -> Option<Context> {
        let context =
            TraceContextPropagator::new().extract_with_context(&Context::new(), &self.headers);
        context.span().span_context().is_valid().then_some(context)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub trace: TraceContext,
    pub command_id: Option<Uuid>,
    pub cancellation: CancellationSignal,
    deadline_unix_ms: Option<u64>,
    #[serde(skip)]
    deadline: Option<Instant>,
}

impl RequestContext {
    pub fn root() -> Self {
        Self {
            request_id: Uuid::now_v7(),
            trace: TraceContext::current(),
            command_id: None,
            cancellation: CancellationSignal::default(),
            deadline_unix_ms: None,
            deadline: None,
        }
    }

    pub fn child(&self) -> Self {
        let current = TraceContext::current();
        Self {
            trace: if current.parent().is_some() {
                current
            } else {
                self.trace.clone()
            },
            ..self.clone()
        }
    }

    /// The context of the next hop of a chain (a nested call, or delivery to a handler): a fresh
    /// request id, because the id is this hop's admission and cancellation key and a nested
    /// call runs while its outer call is in flight, and a child span of the same trace. The
    /// remaining budget carries over; the chain is the trace, not the request id.
    pub fn next_hop(&self) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            trace: self.trace.child_span(),
            cancellation: CancellationSignal::default(),
            ..self.clone()
        }
    }

    pub fn with_command_id(mut self, command_id: Uuid) -> Self {
        self.command_id = Some(command_id);
        self
    }

    pub fn with_time_budget(mut self, budget: Duration) -> Self {
        self = self.for_receiver();
        let deadline = Instant::now()
            .checked_add(budget)
            .expect("request budget exceeds clock range");
        self.deadline = Some(self.deadline.map_or(deadline, |old| old.min(deadline)));
        let millis = u64::try_from(budget.as_millis()).expect("request budget exceeds wire range");
        let wall = wall_clock_millis().saturating_add(millis);
        self.deadline_unix_ms = Some(self.deadline_unix_ms.map_or(wall, |old| old.min(wall)));
        self
    }

    pub fn for_receiver(mut self) -> Self {
        if self.deadline.is_none() {
            self.deadline = self.deadline_unix_ms.map(|deadline| {
                Instant::now() + Duration::from_millis(deadline.saturating_sub(wall_clock_millis()))
            });
        }
        self
    }

    pub fn with_cancellation_transport(mut self, transport: CancellationTransport) -> Self {
        self.cancellation = self.cancellation.bound_to(transport);
        self
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn remaining_budget(&self) -> Option<Duration> {
        match self.deadline {
            Some(deadline) => Some(deadline.saturating_duration_since(Instant::now())),
            None => self.deadline_unix_ms.map(|deadline| {
                Duration::from_millis(deadline.saturating_sub(wall_clock_millis()))
            }),
        }
    }

    pub async fn expired(&self) {
        match self.remaining_budget() {
            Some(budget) => tokio::time::sleep(budget).await,
            None => std::future::pending().await,
        }
    }

    pub fn validate(&self) -> Result<(), ContextError> {
        if self.request_id.is_nil() {
            return Err(ContextError::MissingRequestId);
        }
        if self.cancellation.is_cancelled() {
            return Err(ContextError::Cancelled);
        }
        if self
            .remaining_budget()
            .is_some_and(|budget| budget.is_zero())
        {
            return Err(ContextError::DeadlineExceeded);
        }
        Ok(())
    }
}

fn wall_clock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before UNIX epoch")
        .as_millis()
        .try_into()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_next_hop_has_a_new_request_id_and_a_child_span_of_the_same_trace() {
        let mut context = RequestContext::root();
        context.trace = TraceContext::from_headers(
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            Some("vendor=1"),
        );
        let hop = context.next_hop();
        assert_ne!(hop.request_id, context.request_id);
        let (outer, inner) = (
            context
                .trace
                .traceparent()
                .unwrap()
                .split('-')
                .collect::<Vec<_>>(),
            hop.trace
                .traceparent()
                .unwrap()
                .split('-')
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            (inner[0], inner[1], inner[3]),
            (outer[0], outer[1], outer[3])
        );
        assert_ne!(inner[2], outer[2]);
        assert_eq!(hop.trace.tracestate(), Some("vendor=1"));
        let untraced = TraceContext::default();
        assert_eq!(untraced.child_span(), untraced);
    }

    #[tokio::test(start_paused = true)]
    async fn child_and_receiver_do_not_extend_deadlines() {
        let context = RequestContext::root().with_time_budget(Duration::from_secs(5));
        tokio::time::advance(Duration::from_secs(2)).await;
        let child = context
            .child()
            .with_time_budget(Duration::from_secs(30))
            .for_receiver();
        assert_eq!(child.remaining_budget(), Some(Duration::from_secs(3)));
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(child.validate(), Err(ContextError::DeadlineExceeded));
        child.cancel();
        assert_eq!(context.validate(), Err(ContextError::Cancelled));
    }

    #[tokio::test(start_paused = true)]
    async fn narrowing_a_deserialized_context_does_not_extend_its_deadline() {
        let original = RequestContext::root().with_time_budget(Duration::from_secs(5));
        let wire = serde_json::to_vec(&original).unwrap();
        let received: RequestContext = serde_json::from_slice(&wire).unwrap();
        let received = received.with_time_budget(Duration::from_secs(30));
        assert_eq!(received.deadline_unix_ms, original.deadline_unix_ms);
        assert!(received.remaining_budget().unwrap() <= Duration::from_secs(5));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(received.validate(), Err(ContextError::DeadlineExceeded));
    }

    #[test]
    fn serialization_preserves_elapsed_budget_and_cancellation() {
        let context = RequestContext::root().with_time_budget(Duration::ZERO);
        let wire = serde_json::to_vec(&context).unwrap();
        let received: RequestContext = serde_json::from_slice(&wire).unwrap();
        assert_eq!(
            received.for_receiver().validate(),
            Err(ContextError::DeadlineExceeded)
        );
        context.cancel();
        let wire = serde_json::to_vec(&context).unwrap();
        let received: RequestContext = serde_json::from_slice(&wire).unwrap();
        assert_eq!(received.validate(), Err(ContextError::Cancelled));
    }

    #[test]
    fn standard_propagator_preserves_tracestate_and_rejects_invalid_parents() {
        let parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let trace = TraceContext::from_headers(Some(parent), Some("vendor=value"));
        assert_eq!(trace.traceparent(), Some(parent));
        assert_eq!(trace.tracestate(), Some("vendor=value"));
        for bad in [
            "garbage",
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        ] {
            assert!(
                TraceContext::from_headers(Some(bad), None)
                    .parent()
                    .is_none()
            );
        }
    }
}
