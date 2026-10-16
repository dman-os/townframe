# Big Cabinet services

## What this module is

The reusable service foundation is `utils_rs::service`; the reference service is `utils_rs::service::sample`. This is a typed,
transport-neutral service seam, not a completed Cabinet server. The historical name `SampleRepo` does not exist in this checkout:
use the public `SampleService` and generated `SampleClient`.

The current scope is:

```text
caller ── direct ───────────────► SampleService
       ── native IRPC ───────────► OwnedIrpcAdapter ─► SampleService
       ── XRPC-shaped executor ──► HttpRequest/HttpResponse seam

real HTTP/Iroh transport and Cabinet integration are outside this module
```

`LocalXrpcExecutor` is a fake local executor for JSON mapping tests. It is not a real HTTP server, a real network, or an Iroh
implementation. Production HTTP/Iroh wiring is not integrated here; do not infer that it is from the XRPC-shaped names.

Useful source and tests:

* [`service.rs`](../src/utils_rs/service.rs) defines context, metadata, the
declaration macro, dispatch traits, cancellation, and owned IRPC lifecycle.
* [`sample.rs`](../src/utils_rs/service/sample.rs) defines the reference service
and its generated client.
* [`sample/tests.rs`](../src/utils_rs/service/sample/tests.rs) exercises all
three local transport shapes.
* [`external_consumer_fixture.rs`](../src/utils_rs/external_consumer_fixture.rs)
is a renamed-package consumer fixture for macro hygiene and public API use.

## Declare one closed operation vocabulary

`declare_typed_service!` is the single operation declaration. In the sample it is the block beginning at `service/sample.rs:95`.
Each row supplies the Rust operation variant, typed params, typed output, XRPC kind, HTTP method/input, external NSID, and schema
names:

```rust
unary {
    GetValue => get_value / get_value_with_metadata(GetValueParams)
        -> GetValueOutput = Query, Get, Query,
        "org.example.sample.getValue", "GetValueOutput", "SampleError";
}
stream {
    SubscribeEvents => subscribe_events /
        subscribe_events_with_metadata(SubscribeEventsParams) -> SampleEvent
        = Subscription, Get, Query,
        "org.example.sample.subscribeEvents", "SampleEvent", "SampleError";
}
```

The real declaration also has `PutValue` and `ReadState`, plus named generated transport/IRPC/dispatch items. The macro emits the
closed operation enum, `XRPC_OPERATIONS`, typed methods, native IRPC messages, and transport adapters from these rows. Adding an
operation means changing this declaration and its handler; callers do not register arbitrary operation strings at runtime.

Params, outputs, stream items, and errors are domain-owned types. The sample's `GetValueParams` is a closed unit struct;
`PutValueParams`, `ReadStateParams`, and `SubscribeEventsParams` carry only their declared fields. `SampleError` contains context
failure, invalid input, subscription lag, and bounded stream capacity errors. Keep domain errors separate from transport errors:
`super::ClientError<SampleError>` (exposed as the sample's `ClientError`) distinguishes `Operation(SampleError)` from `Transport { message, outcome_unknown }`. Query transport failure is known not to have committed; procedure and subscription failure can have an unknown outcome, so do not blindly retry them.

The declaration also connects the shared request context and HTTP metadata. Use `Request<P>` when metadata is needed; the
convenience method constructs it with default metadata. `HttpMetadata` carries authorization, idempotency key,
traceparent/tracestate, and timeout beside domain JSON. It is not a claim that a service has implemented authorization or
idempotency semantics.

## Calling the sample directly

Direct execution is caller-owned and inline: `SampleClient::direct` stores an `Arc<SampleService>` and invokes the service
dispatch implementation without spawning a handler for each call. This is a useful local composition boundary, not a hidden
server.

This complete example uses only actual public symbols from `utils_rs::service::sample`:

```rust
use std::sync::Arc;
use utils_rs::service::RequestContext;
use utils_rs::service::sample::{
    ClientError, GetValueParams, PutValueParams, SampleClient, SampleService,
};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let service = SampleService::new("initial");
    let client = SampleClient::direct(Arc::clone(&service));
    let context = RequestContext::root();

    let before = client.get_value(context.child(), GetValueParams).await?;
    let after = client
        .put_value(
            context.child(),
            PutValueParams { value: "ready".into() },
        )
        .await?;

    println!("{} -> {}", before.snapshot.value, after.snapshot.value);
    service.shutdown().await;
    Ok(())
}
```

`SampleService::new` starts its projector task. `put_value` writes the source, then waits for the independent projector watermark
before returning. The projector is also the path for `inject_source`; external source changes are not mutation events.
`read_state` returns a snapshot and a `reset_required` signal when the caller's cursor is ahead. `subscribe_events` is a separate
process observation receiver, while `SampleClient::subscribe_events` is the typed client subscription operation.

## Native IRPC and bounded work

`SampleClient::irpc` sends generated messages through an `irpc::Client`. The sample helper `spawn_sample_irpc(service,
queue_capacity, max_handlers, max_stream_pumps)` creates the bounded Tokio input queue and a `SampleIrpcAdapter`. The queue
bounds waiting messages; `max_handlers` bounds unary admitted/spawned work; `max_stream_pumps` independently bounds active
subscription pumps. Unary admission obtains a permit before spawning, so a bounded queue is not drained into an unbounded set of
parked tasks. A stream that cannot obtain its pump permit receives the typed `SubscriptionCapacityExceeded` error.

`OwnedIrpcAdapter` owns child join handles and exposes activity counters for qualification. `shutdown()` on `SampleIrpcAdapter`
stops the dispatcher and joins owned handlers. `OwnedIrpcAdapter::shutdown_children` is the explicit join path. `Drop` can only
abort: synchronous drop cannot join and therefore must not be described as proof of clean completion. Shut down service, adapter,
and executor explicitly in reverse ownership order when their tasks matter.

## Context, clocks, and cancellation

`RequestContext::root()` supplies request, trace, and span IDs. `child()` keeps the request/trace identity while creating a child
span. `with_command_id` and `with_time_budget` add optional propagation data. `for_receiver()` rebuilds a receiver-local `Instant`
from the serialized absolute Unix-millisecond boundary; it does not reset elapsed budget. Validate at the receiving boundary.
Wall-clock transport data and monotonic local waiting are deliberately distinct; clock skew and clock failure are not converted
into an authorization claim.

Cancellation is a one-shot state transition, not rollback. `cancel()` marks the request state and wakes waiters; a handler may
stop at the cancellation select, but an already completed source write is not undone. Transport cancellation is bounded by
`cancellation_channel(scope_capacity)`, and admitted work is keyed by the full `(request_id, token)` plus the transport
epoch/ordinal lease. A request token is one-shot: do not reuse it for another transport request.

The producer owns the transport scope. Keep the returned `CancellationRequestGuard` until a unary terminal result or deliberate
drop. For a stream, generated IRPC stores the guard in the returned subscription so it remains live through EOF, an error, or
subscription drop. Guard retirement removes the live lease; dropping a guard is not an implicit cancellation and cancellation is
not a rollback. Terminal scope history is bounded rather than an ever-growing tombstone set. The receiver's cancellation admission
and the adapter's handler/stream permits are separate bounds. On server EOF or error, the stream pump stops and releases its task/permit/registration; the client-side guard remains in the returned subscription until that subscription is dropped.

## Serialization and macro hygiene

The generated full IRPC expansion uses the library's `$crate` paths, but the upstream `irpc` derive emits absolute `::irpc`,
`::serde`, and `::tracing` paths. A full consumer therefore declares those direct dependencies as shown in the module
documentation and gives each declaration a unique `serde_module` name. The renamed-package fixture demonstrates this arrangement.

Use the public service types and generated client. The `service::__serde`, `__irpc`, `__tokio`, and `__tracing` reexports exist to
support generated code; they are hidden implementation support, not an application API. Do not expose cancellation keys or promise
atomicity, authorization, real HTTP, Iroh, or Doc-drawer flows that this seam does not implement.

## Evidence boundaries

The sample tests prove source/compile/runtime behavior for direct dispatch, local XRPC-shaped JSON mapping, native IRPC bounds,
metadata propagation, context validation, cancellation/lifecycle behavior, and joined shutdown. They do not prove a production
HTTP server, network transport, Iroh behavior, Cabinet authorization, distributed atomicity, or Root-history permissions. Those
are integration gates for the eventual owning service, not properties of this reference module.
