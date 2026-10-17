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
//! * [`service_protocol!`] declares the IRPC enum and routing table, wrapping
//!   each request in an [`Envelope`]; see [`sample::protocol`].
//! * [`Handler<P>`] is implemented by the service for each request type it
//!   serves. Middleware wraps handlers tower-style with [`Layer`] and
//!   [`ServiceBuilder`].
//! * [`Host`] owns the layered handler and enforces what every transport must
//!   share: tracing, context validation, deadlines, and request cancellation.
//! * [`Route`] binds native dispatch and HTTP mounting; the declaration macro
//!   generates it, or protocols can implement it for custom routing.
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
pub use context::{ContextError, HttpMetadata, RequestContext, TraceContext};
pub use host::{Dispatch, Host, Route};
pub use irpc as __irpc;
pub use irpc_adapter::IrpcAdapter;
pub use lifecycle::{Spawner, StopToken};
pub use middleware::{Handler, Identity, Layer, ServiceBuilder, Stack};
pub use operation::{
    ClientError, Envelope, EventFrame, EventReply, EventStream, HttpInput, HttpMethod, Operation,
    Reply, ResponseEnvelope, ResponseMetadata, ServiceError, Subscription, TransportContext, Unary,
    Wire, XrpcKind, XrpcOperation,
};
pub use xrpc::{XrpcClient, XrpcRouter};

#[cfg(test)]
mod e2e;
