use crate::{
    admission::Admission,
    context::{ContextError, RequestContext},
    lifecycle::{Spawner, StopToken},
    middleware::Handler,
    operation::{Envelope, EventReply, Operation, Reply, Subscription, TransportContext, Unary},
    xrpc::{XrpcRegistrationError, XrpcRouter},
};
use futures::{StreamExt, future::BoxFuture};
use irpc::{Channels, Service, WithChannels};
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

    fn request_span<P: Operation>(request: &Envelope<P>) -> tracing::Span {
        let span = tracing::info_span!("service.call", nsid = P::XRPC.nsid, request_id = %request.context.request_id);
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
        P: Operation,
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
        P: Operation,
        H: Handler<P>,
    {
        let _tracked = self.spawner.track().ok_or(ContextError::Cancelled)?;
        request.context.validate()?;
        let context = request.context.clone();
        tokio::select! {
            biased;
            _ = self.spawner.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.expired() => Err(ContextError::DeadlineExceeded.into()),
            result = self.handler.call(request) => result,
        }
    }

    async fn invoke_owned<P>(
        &self,
        request: Envelope<P>,
        owner: &Spawner,
    ) -> Result<crate::ResponseEnvelope<P::Output>, P::Error>
    where
        P: Operation,
        H: Handler<P>,
    {
        tokio::select! {
            biased;
            _ = owner.cancelled() => Err(ContextError::Cancelled.into()),
            result = self.invoke_inner(request) => result,
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
            inner: mut request,
            tx,
            ..
        } = message;
        request.transport = match dispatch {
            Dispatch::Inline => TransportContext::Direct,
            Dispatch::Admitted { .. } => TransportContext::Irpc,
        };
        async move {
            match dispatch {
                Dispatch::Inline => { drop(tx.send(self.invoke(request).await).await); }
                Dispatch::Admitted { admission, spawner } => {
                    let request = request.for_receiver();
                    let permit = tokio::select! {
                        biased;
                        _ = spawner.cancelled() => { drop(tx.send(Err(ContextError::Cancelled.into())).await); return; }
                        _ = request.context.cancellation.cancelled() => { drop(tx.send(Err(ContextError::Cancelled.into())).await); return; }
                        _ = request.context.expired() => { drop(tx.send(Err(ContextError::DeadlineExceeded.into())).await); return; }
                        permit = admission.acquire_unary() => permit,
                    };
                    let registration = match admission.register_cancellation(request.context.cancellation.clone()) {
                        Ok(registration) => registration,
                        Err(error) => { drop(tx.send(Err(error.into())).await); return; }
                    };
                    let activity = admission.activity();
                    let admitted = activity.admitted();
                    let spawned = activity.spawned();
                    let owner = spawner.clone();
                    let span = Self::request_span(&request);
                    let _task = spawner.spawn(async move {
                        let (_permit, _admitted, _spawned, _registration) = (permit, admitted, spawned, registration);
                        let _active = activity.active();
                        drop(tx.send(self.invoke_owned(request, &owner).await).await);
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
            inner: mut request,
            tx,
            ..
        } = message;
        request.transport = match dispatch {
            Dispatch::Inline => TransportContext::Direct,
            Dispatch::Admitted { .. } => TransportContext::Irpc,
        };
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

async fn pump<P: Subscription>(
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
        #[$crate::__irpc::rpc_requests(message = $message)]
        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        $vis enum $protocol {
            $( #[rpc(tx = $crate::Reply<$params>)] $variant($crate::Envelope<$params>), )*
            $( #[rpc(tx = $crate::EventReply<$stream_params>)] $stream_variant($crate::Envelope<$stream_params>), )*
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
                Ok(())
            }
        }
    };
}

pub(crate) trait InlineRoute<Proto: Service>: Send + Sync {
    fn route_inline(self: Arc<Self>, message: Proto::Message) -> BoxFuture<'static, ()>;
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
    use crate::{
        EventFrame, ResponseEnvelope, ResponseMetadata,
        sample::{SampleEvent, SubscribeEvents},
    };

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
