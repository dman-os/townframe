use crate::operation::Envelope;
use crate::operation::Operation;
use std::future::Future;
use std::sync::Arc;

/// Serves one operation, including its native envelope and response metadata.
pub trait Handler<P: Operation>: Send + Sync + 'static {
    fn call(
        &self,
        request: Envelope<P>,
    ) -> impl Future<Output = Result<crate::ResponseEnvelope<P::Output>, P::Error>> + Send;
}

impl<P: Operation, H: Handler<P> + ?Sized> Handler<P> for Arc<H> {
    fn call(
        &self,
        request: Envelope<P>,
    ) -> impl Future<Output = Result<crate::ResponseEnvelope<P::Output>, P::Error>> + Send {
        H::call(self, request)
    }
}

/// Wraps a handler in middleware, producing another handler.
pub trait Layer<H> {
    type Handler;
    fn layer(&self, inner: H) -> Self::Handler;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl<H> Layer<H> for Identity {
    type Handler = H;
    fn layer(&self, inner: H) -> H {
        inner
    }
}

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
    pub fn layer<New>(self, layer: New) -> ServiceBuilder<Stack<New, L>> {
        ServiceBuilder {
            layer: Stack {
                inner: layer,
                outer: self.layer,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContextError;
    use crate::RequestContext;
    use crate::ResponseEnvelope;
    use crate::XrpcOperation;
    use serde::Deserialize;
    use serde::Serialize;
    use std::sync::Mutex;

    #[derive(Serialize, Deserialize)]
    struct Query;

    impl Operation for Query {
        type Output = ();
        type Error = ContextError;
        const XRPC: XrpcOperation = XrpcOperation::query("org.example.middleware.test");
    }

    struct Record(&'static str, Arc<Mutex<Vec<&'static str>>>);
    struct Wrapped<H>(Record, H);
    struct Leaf(Arc<Mutex<Vec<&'static str>>>);

    impl<H> Layer<H> for Record {
        type Handler = Wrapped<H>;
        fn layer(&self, inner: H) -> Self::Handler {
            Wrapped(Record(self.0, Arc::clone(&self.1)), inner)
        }
    }

    impl<P: Operation, H: Handler<P>> Handler<P> for Wrapped<H> {
        async fn call(
            &self,
            request: Envelope<P>,
        ) -> Result<ResponseEnvelope<P::Output>, P::Error> {
            self.0.1.lock().unwrap().push(self.0.0);
            self.1.call(request).await
        }
    }

    impl Handler<Query> for Leaf {
        async fn call(&self, _: Envelope<Query>) -> Result<ResponseEnvelope<()>, ContextError> {
            self.0.lock().unwrap().push("handler");
            Ok(().into())
        }
    }

    #[tokio::test]
    async fn first_layer_runs_outermost() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let handler = ServiceBuilder::new()
            .layer(Record("outer", Arc::clone(&order)))
            .layer(Record("inner", Arc::clone(&order)))
            .service(Leaf(Arc::clone(&order)));
        handler
            .call(Envelope::new(RequestContext::root(), Query))
            .await
            .unwrap();
        assert_eq!(*order.lock().unwrap(), ["outer", "inner", "handler"]);
    }
}
