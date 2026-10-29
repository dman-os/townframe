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

pub use admission::Admission;
pub use admission::CancellationKey;
pub use admission::CancellationReceiver;
pub use admission::CancellationRequestGuard;
pub use admission::CancellationSignal;
pub use admission::CancellationTransport;
pub use admission::TaskActivity;
pub use admission::cancellation_channel;
pub use client::Client;
pub use client::ClientStream;
pub use client::InlineRoute;
pub use context::ContextError;
pub use context::HttpMetadata;
pub use context::RequestContext;
pub use context::TraceContext;
pub use host::Dispatch;
pub use host::Host;
pub use host::Route;
pub use host::ServerBinding;
pub use irpc as __irpc;
pub use irpc_adapter::IrpcAdapter;
pub use lifecycle::Spawner;
pub use lifecycle::StopToken;
pub use middleware::Handler;
pub use middleware::Identity;
pub use middleware::Layer;
pub use middleware::ServiceBuilder;
pub use middleware::Stack;
pub use operation::ClientError;
pub use operation::Envelope;
pub use operation::EventFrame;
pub use operation::EventReply;
pub use operation::EventStream;
pub use operation::HttpInput;
pub use operation::HttpMethod;
pub use operation::Operation;
pub use operation::Reply;
pub use operation::ResponseEnvelope;
pub use operation::ResponseMetadata;
pub use operation::ServiceError;
pub use operation::Subscription;
pub use operation::TransportContext;
pub use operation::Unary;
pub use operation::Wire;
pub use operation::XrpcKind;
pub use operation::XrpcOperation;
pub use service_core_rs::Protocol;
pub use service_core_rs::ProtocolOperations;
pub use service_core_rs::declare_service_protocol;
pub use service_core_rs::service_operation;
pub use xrpc::XrpcClient;
pub use xrpc::XrpcRouter;

#[cfg(test)]
mod e2e;
