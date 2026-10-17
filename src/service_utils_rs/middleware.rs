//! Handlers and the middleware that wraps them, in tower's shape.
//!
//! [`Handler<P>`] is the service-side contract: one `impl Handler<GetDoc> for
//! DocsService` per operation the service serves. The request type names the
//! operation, so there is no per-service trait and no match over an
//! operation enum.
//!
//! Middleware is a [`Layer`] that wraps a handler in another handler, exactly
//! as `tower::Layer` wraps a `Service`. A layer is generic over `P`, so one
//! `Trace` wraps every operation of a service without naming any of them. The
//! stack is built once with [`ServiceBuilder`] and handed to a
//! [`Host`](crate::Host); every transport then runs the same layers because
//! they all reach the handler through that host.
//!
//! What a layer must *not* do is replace the host's own guarantees: context
//! validation and request cancellation are enforced by the host before and
//! around the stack, so a service cannot forget them.

use crate::operation::{Envelope, Operation};
use std::{future::Future, sync::Arc};

/// Serves one operation. Implemented by a service for each request type it
/// handles, and by every layer for every request type its inner handler
/// handles.
///
/// The whole [`Envelope`] is passed, not just the params, so a layer can read
/// the request context and HTTP metadata (auth headers, request id) without a
/// side channel.
pub trait Handler<P: Operation>: Send + Sync + 'static {
    fn call(
        &self,
        request: Envelope<P>,
    ) -> impl Future<Output = Result<P::Output, P::Error>> + Send;
}

impl<P: Operation, H: Handler<P> + ?Sized> Handler<P> for Arc<H> {
    fn call(
        &self,
        request: Envelope<P>,
    ) -> impl Future<Output = Result<P::Output, P::Error>> + Send {
        H::call(self, request)
    }
}

/// Wraps a handler in middleware, producing another handler.
pub trait Layer<H> {
    type Handler;
    fn layer(&self, inner: H) -> Self::Handler;
}

/// The layer that adds nothing. `ServiceBuilder::new()` starts here.
#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl<H> Layer<H> for Identity {
    type Handler = H;
    fn layer(&self, inner: H) -> H {
        inner
    }
}

/// Two layers composed. `Stack<Inner, Outer>` applies `Inner` first, so
/// `Outer` ends up outermost at call time.
#[derive(Debug, Clone, Copy)]
pub struct Stack<Inner, Outer> {
    inner: Inner,
    outer: Outer,
}

impl<H, Inner, Outer> Layer<H> for Stack<Inner, Outer>
where
    Inner: Layer<H>,
    Outer: Layer<Inner::Handler>,
{
    type Handler = Outer::Handler;
    fn layer(&self, inner: H) -> Self::Handler {
        self.outer.layer(self.inner.layer(inner))
    }
}

/// Builds a middleware stack. Layers are applied in the order they are added:
/// the first `.layer(..)` is closest to the handler and the last runs first.
///
/// ```ignore
/// let handler = ServiceBuilder::new()
///     .layer(Trace::new("docs"))
///     .service(docs_service);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ServiceBuilder<L> {
    layer: L,
}

impl Default for ServiceBuilder<Identity> {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceBuilder<Identity> {
    pub fn new() -> Self {
        Self { layer: Identity }
    }
}

impl<L> ServiceBuilder<L> {
    pub fn layer<New>(self, layer: New) -> ServiceBuilder<Stack<L, New>> {
        ServiceBuilder {
            layer: Stack {
                inner: self.layer,
                outer: layer,
            },
        }
    }

    pub fn service<H>(&self, handler: H) -> L::Handler
    where
        L: Layer<H>,
    {
        self.layer.layer(handler)
    }
}

/// Instruments every call with a `tracing` span carrying the operation's NSID
/// and the request identity, so logs from any transport correlate.
#[derive(Debug, Clone, Copy)]
pub struct Trace {
    target: &'static str,
}

impl Trace {
    pub const fn new(target: &'static str) -> Self {
        Self { target }
    }
}

impl<H> Layer<H> for Trace {
    type Handler = Traced<H>;
    fn layer(&self, inner: H) -> Traced<H> {
        Traced {
            inner,
            target: self.target,
        }
    }
}

pub struct Traced<H> {
    inner: H,
    target: &'static str,
}

impl<P: Operation, H: Handler<P>> Handler<P> for Traced<H> {
    async fn call(&self, request: Envelope<P>) -> Result<P::Output, P::Error> {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "service.call",
            target = self.target,
            nsid = P::XRPC.nsid,
            request_id = %request.context.request_id,
            span_id = %request.context.span_id,
        );
        self.inner.call(request).instrument(span).await
    }
}
