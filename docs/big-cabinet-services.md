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
* [`middleware.rs`](../src/service_utils_rs/middleware.rs): `Handler<P>`, tower-shaped `Layer`/`Stack`/`ServiceBuilder`, and the
  built-in `Trace` layer.
* [`host.rs`](../src/service_utils_rs/host.rs): `Host<H>` (validation, cancellation, dispatch) and the per-protocol `Route` trait.
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

There is no declaration macro. A protocol is three ordinary items, shown here from `sample/protocol.rs`.

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

**The protocol enum is irpc's own derive.** Each variant wraps one request in an `Envelope` and names its reply channel through
the request's `Operation` impl:

```rust
#[rpc_requests(message = SampleMessage)]
#[derive(Debug, Serialize, Deserialize)]
pub enum SampleProtocol {
    #[rpc(tx = Reply<GetValue>)]            GetValue(Envelope<GetValue>),
    #[rpc(tx = Reply<PutValue>)]            PutValue(Envelope<PutValue>),
    #[rpc(tx = Reply<ReadState>)]           ReadState(Envelope<ReadState>),
    #[rpc(tx = EventReply<SubscribeEvents>)] SubscribeEvents(Envelope<SubscribeEvents>),
}
```

**`Route` is the one hand-written match.** Its `where` clause is the honest statement of what a service must implement to host
the protocol; `mount` lists the unary operations for the HTTP router:

```rust
impl<H> Route<H> for SampleProtocol
where
    H: Handler<GetValue> + Handler<PutValue> + Handler<ReadState> + Handler<SubscribeEvents>,
{
    fn route(host: Arc<Host<H>>, dispatch: Dispatch, message: SampleMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            match message {
                SampleMessage::GetValue(m) => host.unary(dispatch, m).await,
                SampleMessage::PutValue(m) => host.unary(dispatch, m).await,
                SampleMessage::ReadState(m) => host.unary(dispatch, m).await,
                SampleMessage::SubscribeEvents(m) => host.stream(dispatch, m).await,
            }
        })
    }

    fn mount(router: &mut XrpcRouter, host: Arc<Host<H>>) -> Result<(), XrpcRegistrationError> {
        router.mount::<GetValue, H>(Arc::clone(&host))?;
        router.mount::<PutValue, H>(Arc::clone(&host))?;
        router.mount::<ReadState, H>(host)
    }
}
```

Adding an operation means a new request type, a new variant, and a new match arm; the compiler reports every site that is missing.
Params, outputs, events, and errors are domain-owned types. Every service error type must implement `From<ContextError>` because
the host, not the handler, rejects invalid, expired, or cancelled requests and reports that through the operation's own error.

## Implement and host a service

A service implements `Handler<P>` once per operation. The whole `Envelope` is passed so a handler or layer can read the request
context and HTTP metadata without a side channel:

```rust
impl Handler<PutValue> for SampleService {
    async fn call(&self, request: Envelope<PutValue>) -> Result<PutValueOutput, SampleError> { .. }
}
```

Middleware is tower-shaped: a `Layer` wraps a handler in another handler, generic over `P`, so one layer applies to every
operation without naming any of them. `ServiceBuilder` composes layers; the first `.layer(..)` is closest to the handler. The
crate ships `Trace` (a `tracing` span per call with NSID and request identity); the sample adds a demonstration `Log` layer.

```rust
let handler = ServiceBuilder::new()
    .layer(Trace::new("sample"))
    .layer(Log::new(calls.clone()))
    .service(Arc::clone(&service));
let (host, host_stop) = Host::new(handler);
```

`Host` enforces the two guarantees no transport may skip: the request context is validated before the stack runs, and the stack is
raced against request cancellation and host shutdown. Those are not optional layers. Everything else (authorization, idempotency,
tracing) is a layer.

## Call a service

`Client<Proto>` is named by the protocol, not the implementation, so a consumer holds a `Client<SampleProtocol>` whether the
service runs in-process, in another process on the node, or over HTTP. The request value selects the operation:

```rust
let client = Client::<SampleProtocol>::direct(host);          // or ::irpc(irpc_client) / ::xrpc(xrpc_client)
let snapshot = client.call(ctx.child(), GetValue {}).await?;
let mut events = client.subscribe(ctx.child(), SubscribeEvents { after_source_revision: 0 }).await?;
while let Some(event) = events.recv().await? { .. }
```

`call_with_metadata`/`subscribe_with_metadata` take an explicit `Envelope` when `HttpMetadata` (authorization, idempotency key,
traceparent/tracestate, timeout) matters. Carrying metadata is not a claim that a service implements those semantics.

`ClientError<E>` keeps transport failure distinct from a semantic `Operation(E)`. `Transport { outcome_unknown }` is `true` for a
procedure or subscription whose response was lost, because the server may have committed before the loss; a query is safe to retry.
Subscription establishment failures arrive as the first, terminal item of the `ClientStream` on every transport; `Err` from
`subscribe` itself means the client could not send the request. The unary HTTP transport does not carry subscriptions and says so
with a transport error.

Over HTTP a typed domain error travels in the standard XRPC error envelope with status `DOMAIN_ERROR_STATUS` (400): `error` is the
operation's declared error schema name, `message` is the error's `Debug`, and `operationError` (`DOMAIN_ERROR_FIELD`) is the
serialized error value the client decodes back into `E`.

## IRPC and bounded work

`IrpcAdapter::local(host, admission, queue)` returns an `irpc::Client` for `Client::irpc` plus a `StopToken`. The queue bounds
waiting messages; `Admission::new(max_handlers, max_stream_pumps)` bounds unary work and subscription pumps independently. Unary
admission obtains a permit before spawning, so a bounded queue is never drained into an unbounded set of parked tasks. A
subscription that cannot obtain a pump permit receives `ContextError::StreamCapacityExceeded` as its first item. A pump ends on
stream EOF, a terminal error, request cancellation, adapter stop, or the subscriber going away, and releases its permit on each path.

## Lifecycle

Every long-lived component boots into `(Arc<Self>, StopToken)`. Background work is spawned through the token's `Spawner` and
watches its cancellation. `StopToken::stop` cancels, closes, and *joins*: it never aborts, so a task that ignores cancellation is
a liveness bug that shows up as a hung stop rather than silently lost work. Tokens compose in construction order reversed with
`then`: `sample::boot` returns `host_stop.then(service_stop)` so in-flight calls end before the projector they may be waiting on.
Work admitted by the IRPC adapter also observes the adapter's own stop, so stopping the adapter never waits on the host.

## Context, clocks, and cancellation

`RequestContext::root()` supplies request, trace, and span IDs; `child()` keeps request/trace identity and opens a child span.
`with_command_id` and `with_time_budget` add optional propagation data. `Envelope::for_receiver()` rebuilds a receiver-local
deadline from the serialized absolute Unix-millisecond boundary without resetting elapsed budget; the IRPC adapter applies it on
admission. Wall-clock transport data and monotonic local waiting are deliberately distinct.

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
