# Big Cabinet services

## What this crate is

The reusable service foundation is the `service_utils_rs` crate; the reference service is `service_utils_rs::sample`. This is a
typed, transport-neutral service seam, not a completed Cabinet server. Nothing here implements Cabinet authorization, drawer
flows, or repository integration.

A service is declared once and reached three ways, all of which run the same hosted handler:

```text
Client<Proto>::call(ctx, params)
    ├─ Direct ─ routed inline on the caller's task ──┐
    ├─ Irpc ─── bounded adapter spawns one task ──────┼─► Host<H> ─► layers ─► Handler<P>
    └─ Xrpc ─── HTTP router decodes the request ──────┘
```

XRPC over HTTP is the canonical interface; direct and IRPC are same-process and same-node optimizations of it. With the
`iroh-http` feature the same HTTP/1 wire is also served over Iroh streams.

Where things live:

* [`operation.rs`](../src/service_utils_rs/operation.rs): the `Operation` trait a request type implements, `XrpcOperation`
  identity, `Envelope`, the `Reply`/`EventReply` channel aliases, and `ClientError`.
* [`context.rs`](../src/service_utils_rs/context.rs): `RequestContext`, `HttpMetadata`, `ContextError`.
* [`middleware.rs`](../src/service_utils_rs/middleware.rs): `Handler<P>` and tower-shaped `Layer`/`Stack`/`ServiceBuilder`.
* [`host.rs`](../src/service_utils_rs/host.rs): `Host<H>` (tracing, validation, cancellation, deadlines, dispatch), `Route`, and
  the small `service_protocol!` routing declaration.
* [`client.rs`](../src/service_utils_rs/client.rs): the one `Client<Proto>` and `ClientStream`.
* [`admission.rs`](../src/service_utils_rs/admission.rs): bounded permits, activity counters, and cancellation transport for
  received requests.
* [`irpc_adapter.rs`](../src/service_utils_rs/irpc_adapter.rs): serves a protocol from an IRPC queue.
* [`lifecycle.rs`](../src/service_utils_rs/lifecycle.rs): `StopToken`/`Spawner`, the one shutdown idiom.
* [`xrpc.rs`](../src/service_utils_rs/xrpc.rs) and `xrpc/`: HTTP router, client, TCP server, Iroh wire, and their tests.
* [`sample.rs`](../src/service_utils_rs/sample.rs), `sample/protocol.rs`, `sample/service.rs`, `sample/tests.rs`: the reference
  service, laid out the way a new service should be.
* [`e2e.rs`](../src/service_utils_rs/e2e.rs) and `e2e/`: contracts that must hold across all three transports, exercised
  against the sample.

## Declare a protocol

Request types implement ordinary traits; `service_protocol!` generates only the mechanical IRPC enum, native routing, and unary
HTTP mounting from one operation table, as shown in `sample/protocol.rs`.

**The request type names its operation.** Output, error, and XRPC identity hang off the params type, so no transport ever
re-spells them:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutValue { pub value: String }

impl Operation for PutValue {
    type Output = PutValueOutput;
    type Error = SampleError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.sample.putValue")
        .with_schemas("PutValueOutput", "SampleError");
}
```

`XrpcOperation::query`/`procedure`/`subscription` fix the conventional HTTP mapping (queries and subscriptions are `GET` with
query-string input, procedures are `POST` with a JSON body). A subscription additionally implements `Subscription { type Event }`
and its `Output` is the handler-side `EventStream<Event, Error>`.

**The routing declaration lists each request once.** It expands through irpc's own `rpc_requests` derive; output and error types
come from `Operation`, and the generated `Route<H>` requires the handler traits for every listed request:

```rust
service_utils_rs::service_protocol! {
    pub enum SampleProtocol => SampleMessage {
        unary { GetValue(GetValue), PutValue(PutValue), ReadState(ReadState) }
        stream { SubscribeEvents(SubscribeEvents) }
    }
}
```

Adding an operation means a request type, its `Operation` impl, a handler impl, and one entry in the routing table.
The declaration uses irpc's derive machinery; consumers depend on `serde` with derives, `irpc` with derives, and `tracing` as well as
this crate. This crate may be renamed in the consumer's dependencies.
Hand-written `rpc_requests` enums and `Route` implementations remain valid for protocols needing custom routing.
Params, outputs, events, and errors are domain-owned types. Every service error type must implement `From<ContextError>` because
the host, not the handler, rejects invalid, expired, or cancelled requests and reports that through the operation's own error.

## Implement and host a service

A service implements `Handler<P>` once per operation. The whole `Envelope` is passed so a handler or layer can read the request
context and HTTP metadata without a side channel:

```rust
impl Handler<PutValue> for SampleService {
    async fn call(&self, request: Envelope<PutValue>) -> Result<ResponseEnvelope<PutValueOutput>, SampleError> { .. }
}
```

Middleware is tower-shaped: a `Layer` wraps a handler in another handler, generic over `P`, so one layer applies to every
operation without naming any of them. `ServiceBuilder` uses Tower ordering: the first `.layer(..)` runs outermost. The sample
adds a demonstration `Log` layer; tracing belongs to the host and cannot be omitted from a transport.

```rust
let handler = ServiceBuilder::new()
    .layer(Log::new(calls.clone()))
    .service(Arc::clone(&service));
let (host, host_stop) = Host::new(handler);
```

`Host` validates context before the stack runs and races execution against the same request deadline, request cancellation, and
host shutdown. A real `service.call` tracing span covers handler execution and, for subscriptions, all stream polling and delivery.
Authorization and idempotency policy remain layers. Trusted `Envelope::transport()` data is installed by the receiving adapter,
not deserialized from client claims: direct, IRPC, HTTP, or the authenticated Iroh connection peer.

## Call a service

`Client<Proto>` is named by the protocol, not the implementation, so a consumer holds a `Client<SampleProtocol>` whether the
service runs in-process, in another process on the node, or over HTTP. The request value selects the operation:

```rust
let client = Client::<SampleProtocol>::direct(host);          // or ::irpc(irpc_client) / ::xrpc(xrpc_client)
let snapshot = client.call(ctx.child(), GetValue {}).await?;
let mut events = client.subscribe(ctx.child(), SubscribeEvents { after_source_revision: 0 }).await?;
while let Some(event) = events.recv().await? { .. }
```

`call_with_metadata`/`subscribe_with_metadata` take an explicit request `Envelope` and return `ResponseEnvelope<Output>` or
`ResponseEnvelope<ClientStream<P>>`. `HttpMetadata` carries caller-supplied authorization and idempotency keys; tracing and deadlines
belong only to `RequestContext`. `ResponseMetadata` carries ETag, Location, Retry-After, Cache-Control, and repeated Warning values
outside domain payloads. HTTP and Iroh map those values to response headers; native subscriptions send establishment metadata before
events. `call`/`subscribe` are output-only conveniences. Response metadata currently accompanies successful responses, not errors.
Carrying metadata is not a claim that a service implements authorization, caching, or retry policy.

`ClientError<E>` keeps transport failure distinct from a semantic `Operation(E)`. `Transport { outcome_unknown }` is `true` for a
procedure or subscription whose response was lost, because the server may have committed before the loss; a query is safe to retry.
Subscription establishment failures return from `subscribe` itself, before a `ClientStream` is available. Errors after establishment
are terminal stream items and immediately release the receiver and cancellation scope. Native streams send an explicit end marker on
successful completion; a missing end marker is a transport failure, not successful EOF. The unary HTTP transport does not carry
subscriptions and says so with a transport error.

Over HTTP a typed domain error travels in the standard XRPC error envelope with status `DOMAIN_ERROR_STATUS` (400): `error` is the
operation's declared error schema name, `message` is the error's `Debug`, and `operationError` (`DOMAIN_ERROR_FIELD`) is the
serialized error value the client decodes back into `E`.

## IRPC and bounded work

`IrpcAdapter::local(host, admission, queue)` returns an `irpc::Client` for `Client::irpc` plus a `StopToken`. The queue bounds
waiting messages; `Admission::new(max_handlers, max_stream_pumps)` bounds unary work and subscription pumps independently. Unary
admission obtains a permit before spawning, so a bounded queue is never drained into an unbounded set of parked tasks. A
subscription that cannot obtain a pump permit fails establishment with `ContextError::StreamCapacityExceeded`. A pump ends on
stream EOF, a terminal error, request cancellation, adapter stop, or the subscriber going away, and releases its permit on each path.

## Lifecycle

Every long-lived component boots into `(Arc<Self>, StopToken)`. Background work is spawned through the token's `Spawner` and
watches its cancellation. `StopToken::stop` cancels, closes, and *joins*: it never aborts, so a task that ignores cancellation is
a liveness bug that shows up as a hung stop rather than silently lost work. Tokens compose in construction order reversed with
`then`: `sample::boot` returns `host_stop.then(service_stop)` so in-flight calls end before the projector they may be waiting on.
Work admitted by the IRPC adapter also observes the adapter's own stop, so stopping the adapter never waits on the host.

## Context, clocks, and cancellation

`RequestContext::root()` allocates a request ID and captures the current tracing span's OpenTelemetry context. `child()` retains
request identity and captures the current service span for a nested call; it does not invent span IDs. The SDK W3C propagator
extracts and injects `traceparent`/`tracestate`, and the host uses the propagated context as the actual service span parent. An
application installs its own `tracing-opentelemetry` subscriber to export traces; calls also work with ordinary tracing or no subscriber.

`with_command_id` adds an optional command identity. `with_time_budget` narrows, never extends, the existing deadline. Local waits
use a monotonic clock; IRPC serializes an absolute Unix-millisecond deadline, and HTTP sends the remaining budget as `x-timeout-ms`.
`Envelope::for_receiver()` reconstructs a monotonic deadline only after serialization, without resetting an existing local deadline.
Client queueing and waiting, HTTP body reading, host execution, IRPC admission, and stream polling/backpressure are all bounded by
that request budget. Iroh request-stream EOF is a half-close, not cancellation; resetting the response stream cancels its handler.

Cancellation is a one-shot state transition, not rollback: `cancel()` wakes the host's select, but a completed source write is not
undone. Transport cancellation is bounded by `cancellation_channel(scope_capacity)`; admitted work is keyed by `(request_id, token)`
plus the transport lease. `Client` opens the transport scope around an IRPC call and keeps it inside the returned `ClientStream` for
a subscription, so a caller can still cancel after establishment.

## The sample

`sample::boot("initial")` returns a `Sample { service, host, calls }` and one stop token. `SampleService` models a
source-and-projector service: writers allocate source revisions at one boundary, an owned projector task interprets the latest
source into readable state (coalescing bursts), and `SubscribeEvents` observes interpreted changes. `PutValue` writes only the
source and waits for the projector watermark, which is a completion contract, not a direct projector call. `inject_source` is the
same path for externally synchronised changes, so those are not mutation events. `ReadState` returns `reset_required` when the
caller's cursor is ahead of the source, and a lagged subscriber receives `SubscriptionLagged` as a terminal item and must reread.

## Evidence boundaries

`sample/tests.rs` covers the sample's own semantics through the direct transport. `e2e/` boots the full stack and proves parity
of outputs, typed domain errors, host-side context rejection, and metadata delivery across direct, IRPC, and HTTP, plus IRPC
admission bounds, cancellation, lost-reply classification, and joined shutdown. `xrpc/tests.rs` covers the HTTP wire itself: body
and concurrency bounds, ambiguous procedure outcomes, request-id correlation, and server shutdown, with the Iroh wire under
`iroh-http`. None of this establishes Cabinet authorization, distributed atomicity, or Root-history permissions.
