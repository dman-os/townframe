use crate::admission::Admission;
use crate::context::RequestContext;
use crate::lifecycle::Spawner;
use crate::lifecycle::StopToken;
use crate::middleware::Handler;
use crate::operation::Envelope;
use crate::operation::EventReply;
use crate::operation::Reply;
use crate::operation::TransportContext;
use crate::xrpc::XrpcRegistrationError;
use crate::xrpc::XrpcRouter;
use futures::StreamExt;
use futures::future::BoxFuture;
use irpc::Channels;
use irpc::Service;
use irpc::WithChannels;
use method_core::ContextError;
use method_core::Method;
use method_core::Subscription;
use method_core::Unary;
use std::sync::Arc;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[derive(Clone)]
pub enum Dispatch {
    Inline,
    Admitted {
        admission: Arc<Admission>,
        spawner: Spawner,
    },
}

pub struct Host<H> {
    handler: H,
    spawner: Spawner,
}

impl<H: Send + Sync + 'static> Host<H> {
    pub fn new(handler: H) -> (Arc<Self>, StopToken) {
        let (stop, spawner) = StopToken::new();
        (Arc::new(Self { handler, spawner }), stop)
    }

    /// The XRPC router spawns its WebSocket subscription pumps here, so stopping the host ends
    /// and joins them.
    pub(crate) fn spawner(&self) -> &Spawner {
        &self.spawner
    }

    pub(crate) fn request_span<P: Method>(request: &Envelope<P>) -> tracing::Span {
        let span = tracing::info_span!("service.call", nsid = P::XRPC.nsid, request_id = %request.context.request_id, worker = P::XRPC.nsid); // INSTR worker=nsid so the census keys service calls by operation
        if let Some(parent) = request.context.trace.parent() {
            match span.set_parent(parent) {
                Ok(())
                | Err(
                    tracing_opentelemetry::SetParentError::LayerNotFound
                    | tracing_opentelemetry::SetParentError::SpanDisabled,
                ) => {}
                Err(tracing_opentelemetry::SetParentError::AlreadyStarted) => {
                    panic!("new service span was already started")
                }
            }
        }
        span
    }

    pub async fn invoke<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<crate::ResponseEnvelope<P::Output>, P::Error>
    where
        P: Method,
        H: Handler<P>,
    {
        let span = Self::request_span(&request);
        self.invoke_inner(request).instrument(span).await
    }

    async fn invoke_inner<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<crate::ResponseEnvelope<P::Output>, P::Error>
    where
        P: Method,
        H: Handler<P>,
    {
        let _tracked = self.spawner.track().ok_or(ContextError::Cancelled)?;
        request.context.validate()?;
        let context = request.context.clone();
        // The handler's future is boxed here, at the one point every call passes through. Left
        // inline, it would be a field of this future, of `invoke`'s, of `unary`'s and of the
        // protocol's `route` block, and each of those poll frames would carry a temporary of its
        // full size while constructing it. A handler that calls another service through a direct
        // client nests that chain once per hop, and in a debug build three in-process hops were
        // enough to overflow a 2 MiB test thread.
        //
        // The handler is polled first: a result that is ready in the same poll as a cancel or the
        // deadline is the call's outcome, not discarded for a `Cancelled` the caller would have to
        // treat as unknown.
        tokio::select! {
            biased;
            result = Box::pin(self.handler.call(request)) => result,
            _ = self.spawner.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.expired() => Err(ContextError::DeadlineExceeded.into()),
        }
    }

    async fn invoke_owned<P>(
        &self,
        request: Envelope<P>,
        owner: &Spawner,
    ) -> Result<crate::ResponseEnvelope<P::Output>, P::Error>
    where
        P: Method,
        H: Handler<P>,
    {
        // The call is polled first for the same reason as in `invoke_inner`.
        tokio::select! {
            biased;
            result = self.invoke_inner(request) => result,
            _ = owner.cancelled() => Err(ContextError::Cancelled.into()),
        }
    }

    pub async fn unary<P, Proto>(
        self: Arc<Self>,
        dispatch: Dispatch,
        message: WithChannels<Envelope<P>, Proto>,
    ) where
        P: Unary,
        Proto: Service,
        Envelope<P>: Channels<Proto, Tx = Reply<P>>,
        H: Handler<P>,
    {
        let parent = message
            .parent_span_opt()
            .cloned()
            .unwrap_or_else(tracing::Span::current);
        let WithChannels {
            inner: request, tx, ..
        } = message;
        check_in_process_transport(&dispatch, request.transport());
        async move {
            match dispatch {
                Dispatch::Inline => { drop(tx.send(self.invoke(request).await).await); }
                Dispatch::Admitted { admission, spawner } => {
                    // An IRPC queue is same-node, so the reply sender is a tokio oneshot, whose
                    // `closed` resolves when the caller drops its receiver; irpc's own sender has
                    // no such signal. A remote (boxed) sender here is a programming error.
                    let mut tx: tokio::sync::oneshot::Sender<_> = tx
                        .try_into()
                        .expect("an IRPC adapter serves a same-node queue, whose replies are tokio oneshots");
                    let request = request.for_receiver();
                    // The sends below fail only when the caller has already left.
                    let permit = tokio::select! {
                        biased;
                        _ = spawner.cancelled() => { drop(tx.send(Err(ContextError::Cancelled.into()))); return; }
                        _ = request.context.cancellation.cancelled() => { drop(tx.send(Err(ContextError::Cancelled.into()))); return; }
                        _ = request.context.expired() => { drop(tx.send(Err(ContextError::DeadlineExceeded.into()))); return; }
                        () = tx.closed() => return,
                        permit = admission.acquire_unary() => permit,
                    };
                    let registration = match admission.register_cancellation(request.context.cancellation.clone()) {
                        Ok(registration) => registration,
                        Err(error) => { drop(tx.send(Err(error.into()))); return; }
                    };
                    let activity = admission.activity();
                    let admitted = activity.admitted();
                    let spawned = activity.spawned();
                    let owner = spawner.clone();
                    let span = Self::request_span(&request);
                    let _task = spawner.spawn(async move {
                        let (_permit, _admitted, _spawned, _registration) = (permit, admitted, spawned, registration);
                        let _active = activity.active();
                        // A caller that drops its call cancels the handler, as dropping the future
                        // does on a direct call and resetting the stream does on XRPC. The handler
                        // is polled first, so a result ready in the same poll is still sent.
                        let result = tokio::select! {
                            biased;
                            result = self.invoke_owned(request, &owner) => result,
                            () = tx.closed() => return,
                        };
                        drop(tx.send(result));
                    }.instrument(span));
                }
            }
        }.instrument(parent).await;
    }

    pub async fn stream<P, Proto>(
        self: Arc<Self>,
        dispatch: Dispatch,
        message: WithChannels<Envelope<P>, Proto>,
    ) where
        P: Subscription,
        Proto: Service,
        Envelope<P>: Channels<Proto, Tx = EventReply<P>>,
        H: Handler<P>,
    {
        let parent = message
            .parent_span_opt()
            .cloned()
            .unwrap_or_else(tracing::Span::current);
        let WithChannels {
            inner: request, tx, ..
        } = message;
        check_in_process_transport(&dispatch, request.transport());
        async move {
            match dispatch {
                Dispatch::Inline => {
                    let context = request.context.clone();
                    let span = Self::request_span(&request);
                    let response = match self.invoke_inner(request).instrument(span.clone()).await {
                        Ok(response) => response,
                        Err(error) => {
                            drop(tx.try_send(Err(error)).await);
                            return;
                        }
                    };
                    let owner = self.spawner.clone();
                    let _task = self
                        .spawner
                        .spawn(pump::<P>(response, tx, context, owner).instrument(span));
                }
                Dispatch::Admitted { admission, spawner } => {
                    let request = request.for_receiver();
                    let Some(permit) = admission.try_acquire_stream() else {
                        drop(
                            tx.try_send(Err(ContextError::StreamCapacityExceeded {
                                limit: admission.max_stream_pumps(),
                            }
                            .into()))
                                .await,
                        );
                        return;
                    };
                    let registration = match admission
                        .register_cancellation(request.context.cancellation.clone())
                    {
                        Ok(registration) => registration,
                        Err(error) => {
                            drop(tx.try_send(Err(error.into())).await);
                            return;
                        }
                    };
                    let pump_guard = admission.activity().stream_pump();
                    let owner = spawner.clone();
                    let span = Self::request_span(&request);
                    let _task = spawner.spawn(
                        async move {
                            let (_permit, _pump_guard, _registration) =
                                (permit, pump_guard, registration);
                            let context = request.context.clone();
                            let response = match self.invoke_owned(request, &owner).await {
                                Ok(response) => response,
                                Err(error) => {
                                    drop(tx.try_send(Err(error)).await);
                                    return;
                                }
                            };
                            pump::<P>(response, tx, context, owner).await;
                        }
                        .instrument(span),
                    );
                }
            }
        }
        .instrument(parent)
        .await;
    }
}

/// Asserts that an in-process request was stamped by the client kind its dispatch implies:
/// `Transport::direct` for inline routing, `Transport::irpc` for an admitted IRPC queue. Both
/// stamp the origin their caller states, which is how an in-process call says where it comes
/// from; a message that reaches a host without one was built around the client.
fn check_in_process_transport(dispatch: &Dispatch, transport: &TransportContext) {
    match (dispatch, transport) {
        (Dispatch::Inline, TransportContext::Direct { .. })
        | (Dispatch::Admitted { .. }, TransportContext::Irpc { .. }) => {}
        (Dispatch::Inline, other) => {
            panic!(
                "an inline call must come from a direct client, which states its origin; got {other:?}"
            )
        }
        (Dispatch::Admitted { .. }, other) => {
            panic!(
                "an IRPC call must come from an IRPC client, which states its origin; got {other:?}"
            )
        }
    }
}

/// Delivers a subscription's frames to `tx` until the stream ends, the receiver closes, the
/// owner stops, or the request is cancelled or runs out of budget; the last two end it with a
/// terminal error frame. Every transport's stream is pumped here.
pub(crate) async fn pump<P: Subscription>(
    response: crate::ResponseEnvelope<P::Output>,
    tx: EventReply<P>,
    context: RequestContext,
    owner: Spawner,
) {
    let crate::ResponseEnvelope {
        output: mut stream,
        metadata,
    } = response;
    let delivery = async {
        if tx
            .send(Ok(crate::EventFrame::Open(metadata)))
            .await
            .is_err()
        {
            return;
        }
        while let Some(item) = stream.next().await {
            let terminal = item.is_err();
            if tx.send(item.map(crate::EventFrame::Event)).await.is_err() || terminal {
                return;
            }
        }
        drop(tx.send(Ok(crate::EventFrame::End)).await);
    };
    let terminal = tokio::select! {
        biased;
        _ = owner.cancelled() => Some(ContextError::Cancelled),
        _ = context.cancellation.cancelled() => Some(ContextError::Cancelled),
        _ = context.expired() => Some(ContextError::DeadlineExceeded),
        _ = tx.closed() => None,
        () = delivery => None,
    };
    drop(stream);
    if let Some(error) = terminal {
        drop(tx.try_send(Err(error.into())).await);
    }
}

/// Binds a protocol's message routing and HTTP mounting to a handler stack.
pub trait Route<H>: Service {
    fn route(
        host: Arc<Host<H>>,
        dispatch: Dispatch,
        message: Self::Message,
    ) -> BoxFuture<'static, ()>;
    fn mount(router: &mut XrpcRouter, host: Arc<Host<H>>) -> Result<(), XrpcRegistrationError>;
}

#[macro_export]
macro_rules! service_protocol {
    ($vis:vis enum $protocol:ident => $message:ident {
        unary { $( $variant:ident($params:ty) ),* $(,)? }
        stream { $( $stream_variant:ident($stream_params:ty) ),* $(,)? }
    }) => {
        $crate::declare_service_protocol! {
            $vis enum $protocol => $message {
                unary { $( $variant($params) ),* }
                stream { $( $stream_variant($stream_params) ),* }
            }
        }
        impl<H> $crate::Route<H> for $protocol
        where H: Send + Sync + 'static $(+ $crate::Handler<$params>)* $(+ $crate::Handler<$stream_params>)* {
            fn route(host: std::sync::Arc<$crate::Host<H>>, dispatch: $crate::Dispatch, message: $message) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                Box::pin(async move {
                    match message {
                        $( $message::$variant(message) => host.unary(dispatch, message).await, )*
                        $( $message::$stream_variant(message) => host.stream(dispatch, message).await, )*
                    }
                })
            }
            fn mount(router: &mut $crate::XrpcRouter, host: std::sync::Arc<$crate::Host<H>>) -> Result<(), $crate::xrpc::XrpcRegistrationError> {
                $( router.mount::<$params, H>(std::sync::Arc::clone(&host))?; )*
                $( router.mount_subscription::<$stream_params, H>(std::sync::Arc::clone(&host))?; )*
                Ok(())
            }
        }
    };
}

use service_client_native_rs::InlineRoute;

pub trait ServerBinding<Proto: Service>: InlineRoute<Proto> + 'static {
    fn route(
        self: Arc<Self>,
        dispatch: Dispatch,
        message: Proto::Message,
    ) -> BoxFuture<'static, ()>;
    fn mount(router: &mut XrpcRouter, binding: Arc<Self>) -> Result<(), XrpcRegistrationError>;
}

impl<H, Proto> ServerBinding<Proto> for Host<H>
where
    H: Send + Sync + 'static,
    Proto: Route<H>,
{
    fn route(
        self: Arc<Self>,
        dispatch: Dispatch,
        message: Proto::Message,
    ) -> BoxFuture<'static, ()> {
        Proto::route(self, dispatch, message)
    }

    fn mount(router: &mut XrpcRouter, binding: Arc<Self>) -> Result<(), XrpcRegistrationError> {
        Proto::mount(router, binding)
    }
}

#[macro_export]
macro_rules! bind_service_protocol {
    ($vis:vis struct $binding:ident for $protocol:path => $message:path {
        unary { $( $variant:ident($params:ty) ),* $(,)? }
        stream { $( $stream_variant:ident($stream_params:ty) ),* $(,)? }
    }) => {
        $vis struct $binding<H>(std::sync::Arc<$crate::Host<H>>);

        impl<H> $binding<H> {
            pub fn new(host: std::sync::Arc<$crate::Host<H>>) -> std::sync::Arc<Self> {
                std::sync::Arc::new(Self(host))
            }
        }

        impl<H> $crate::__client_native::InlineRoute<$protocol> for $binding<H>
        where H: Send + Sync + 'static $(+ $crate::Handler<$params>)* $(+ $crate::Handler<$stream_params>)* {
            fn route_inline(self: std::sync::Arc<Self>, message: $message) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                <Self as $crate::ServerBinding<$protocol>>::route(self, $crate::Dispatch::Inline, message)
            }
        }

        impl<H> $crate::ServerBinding<$protocol> for $binding<H>
        where H: Send + Sync + 'static $(+ $crate::Handler<$params>)* $(+ $crate::Handler<$stream_params>)* {
            fn route(self: std::sync::Arc<Self>, dispatch: $crate::Dispatch, message: $message) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                use $message as Message;
                Box::pin(async move {
                    let host = std::sync::Arc::clone(&self.0);
                    match message {
                        $( Message::$variant(message) => host.unary(dispatch, message).await, )*
                        $( Message::$stream_variant(message) => host.stream(dispatch, message).await, )*
                    }
                })
            }

            fn mount(router: &mut $crate::XrpcRouter, binding: std::sync::Arc<Self>) -> Result<(), $crate::xrpc::XrpcRegistrationError> {
                let host = &binding.0;
                $( router.mount::<$params, H>(std::sync::Arc::clone(host))?; )*
                $( router.mount_subscription::<$stream_params, H>(std::sync::Arc::clone(host))?; )*
                Ok(())
            }
        }
    };
}

impl<H, Proto> InlineRoute<Proto> for Host<H>
where
    H: Send + Sync + 'static,
    Proto: Route<H>,
{
    fn route_inline(self: Arc<Self>, message: Proto::Message) -> BoxFuture<'static, ()> {
        Proto::route(self, Dispatch::Inline, message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventFrame;
    use crate::ResponseEnvelope;
    use crate::ResponseMetadata;
    use crate::sample::SampleEvent;
    use crate::sample::SubscribeEvents;
    use method_core::XrpcMethod;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Poll;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Race;

    impl Method for Race {
        type Output = ();
        type Error = ContextError;
        const XRPC: XrpcMethod = XrpcMethod::query("org.example.race.get");
    }

    /// Pending until its flag is set, then done. A test sets the flag together with a cancel
    /// between two polls, so the host's next poll finds both ready.
    struct Racer(Arc<AtomicBool>);

    impl Handler<Race> for Racer {
        fn call(
            &self,
            _: Envelope<Race>,
        ) -> impl Future<Output = Result<ResponseEnvelope<()>, ContextError>> + Send {
            let ready = Arc::clone(&self.0);
            std::future::poll_fn(move |_| {
                if ready.load(Ordering::Acquire) {
                    Poll::Ready(Ok(ResponseEnvelope::from(())))
                } else {
                    Poll::Pending
                }
            })
        }
    }

    #[tokio::test]
    async fn a_result_ready_in_the_same_poll_as_a_cancel_wins() {
        // The request's own cancellation, which every call watches.
        let ready = Arc::new(AtomicBool::new(false));
        let (host, stop) = Host::new(Racer(Arc::clone(&ready)));
        let context = RequestContext::root();
        let mut call = std::pin::pin!(host.invoke(Envelope::new(context.clone(), Race)));
        assert!(futures::poll!(&mut call).is_pending());
        ready.store(true, Ordering::Release);
        context.cancel();
        assert_eq!(call.await, Ok(ResponseEnvelope::from(())));
        stop.stop().await;

        // The owner of an admitted call, its IRPC adapter, stopping. The owner tracks nothing
        // here, so stopping it only cancels.
        let ready = Arc::new(AtomicBool::new(false));
        let (host, stop) = Host::new(Racer(Arc::clone(&ready)));
        let (owner_stop, owner) = StopToken::new();
        let mut call =
            std::pin::pin!(host.invoke_owned(Envelope::new(RequestContext::root(), Race), &owner));
        assert!(futures::poll!(&mut call).is_pending());
        ready.store(true, Ordering::Release);
        owner_stop.stop().await;
        assert_eq!(call.await, Ok(ResponseEnvelope::from(())));
        stop.stop().await;
    }

    #[tokio::test]
    async fn finite_subscription_delivers_end_after_all_events() {
        let event = SampleEvent {
            source_revision: 1,
            service_revision: 1,
            value: "one".into(),
        };
        let metadata = ResponseMetadata {
            etag: Some("revision-1".into()),
            ..ResponseMetadata::default()
        };
        let (sender, mut receiver) = irpc::channel::mpsc::channel(3);
        let (stop, spawner) = StopToken::new();
        pump::<SubscribeEvents>(
            ResponseEnvelope {
                output: Box::pin(futures::stream::iter([Ok(event.clone())])),
                metadata: metadata.clone(),
            },
            sender,
            RequestContext::root(),
            spawner,
        )
        .await;
        assert_eq!(
            receiver.recv().await.unwrap(),
            Some(Ok(EventFrame::Open(metadata)))
        );
        assert_eq!(
            receiver.recv().await.unwrap(),
            Some(Ok(EventFrame::Event(event)))
        );
        assert_eq!(receiver.recv().await.unwrap(), Some(Ok(EventFrame::End)));
        assert_eq!(receiver.recv().await.unwrap(), None);
        stop.stop().await;
    }
}
