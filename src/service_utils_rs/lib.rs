//! Shared runtime for typed services whose canonical interface is XRPC over
//! HTTP, with direct calls and IRPC as same-process and same-node optimizations.
//!
//! A service is declared once and reached three ways, all of which run the
//! same hosted handler:
//!
//! ```text
//! Client<Proto>::call(ctx, params)
//!     ├─ Direct ─ routed inline on the caller's task ──┐
//!     ├─ Irpc ─── bounded adapter spawns one task ──────┼─► Host<H> ─► layers ─► Handler<P>
//!     └─ Xrpc ─── HTTP router decodes the request ──────┘
//! ```
//!
//! * [`Operation`] is implemented on each request type and names its output,
//!   error, and XRPC identity. The request value *is* the method: there is no
//!   per-service client type and no generated method list.
//! * The protocol enum is declared with irpc's own `#[rpc_requests]` derive
//!   and wraps each request in an [`Envelope`]; see [`sample::protocol`].
//! * [`Handler<P>`] is implemented by the service for each request type it
//!   serves. Middleware wraps handlers tower-style with [`Layer`] and
//!   [`ServiceBuilder`].
//! * [`Host`] owns the layered handler and enforces what every transport must
//!   share: context validation and request cancellation.
//! * [`Route`] is the one hand-written per-protocol item: a match from the
//!   protocol's message enum to `Host::unary`/`Host::stream`, plus the XRPC
//!   mount list.
//! * Every long-lived component boots into an `(Arc<Self>, StopToken)` pair;
//!   [`StopToken::stop`] cancels and then joins, in construction order reversed.
//!
//! [`sample`] is the reference implementation and the layout new services
//! should copy.

pub mod admission;
pub mod client;
pub mod context;
pub mod host;
pub mod irpc_adapter;
pub mod lifecycle;
pub mod middleware;
pub mod operation;
pub mod sample;
pub mod xrpc;

pub use admission::{
    Admission, CancellationKey, CancellationReceiver, CancellationRequestGuard, CancellationSignal,
    CancellationTransport, TaskActivity, cancellation_channel,
};
pub use client::Client;
pub use context::{ContextError, HttpMetadata, RequestContext};
pub use host::{Dispatch, Host, Route};
pub use irpc_adapter::IrpcAdapter;
pub use lifecycle::{Spawner, StopToken};
pub use middleware::{Handler, Identity, Layer, ServiceBuilder, Stack, Trace, Traced};
pub use operation::{
    ClientError, Envelope, EventReply, EventStream, HttpInput, HttpMethod, Operation, Reply,
    ServiceError, Subscription, Unary, Wire, XrpcKind, XrpcOperation,
};
pub use xrpc::{XrpcClient, XrpcRouter};

#[cfg(test)]
mod e2e;
