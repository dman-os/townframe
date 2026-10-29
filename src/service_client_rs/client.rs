#[cfg(feature = "http")]
use crate::xrpc::XrpcClient;
#[cfg(feature = "http")]
use crate::xrpc::XrpcClientError;
use futures::Stream;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use irpc::Channels;
use irpc::Service;
use irpc::WithChannels;
use irpc::channel::mpsc;
use irpc::channel::none::NoReceiver;
use irpc::channel::oneshot;
use service_core_rs::CancellationRequestGuard;
use service_core_rs::ClientError;
use service_core_rs::ContextError;
use service_core_rs::Envelope;
use service_core_rs::EventFrame;
use service_core_rs::EventReply;
use service_core_rs::Reply;
use service_core_rs::RequestContext;
use service_core_rs::ResponseEnvelope;
use service_core_rs::Subscription;
use service_core_rs::Unary;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

const DEFAULT_EVENT_BUFFER: usize = 64;

pub trait InlineRoute<Proto: Service>: Send + Sync {
    fn route_inline(self: Arc<Self>, message: Proto::Message) -> BoxFuture<'static, ()>;
}

pub struct Client<Proto: Service> {
    transport: Transport<Proto>,
    event_buffer: usize,
}

enum Transport<Proto: Service> {
    Direct(Arc<dyn InlineRoute<Proto>>),
    Irpc(irpc::Client<Proto>),
    #[cfg(feature = "http")]
    Xrpc(XrpcClient),
}

impl<Proto: Service> Clone for Client<Proto> {
    fn clone(&self) -> Self {
        let transport = match &self.transport {
            Transport::Direct(host) => Transport::Direct(Arc::clone(host)),
            Transport::Irpc(client) => Transport::Irpc(client.clone()),
            #[cfg(feature = "http")]
            Transport::Xrpc(client) => Transport::Xrpc(client.clone()),
        };
        Self {
            transport,
            event_buffer: self.event_buffer,
        }
    }
}

impl<Proto: Service> std::fmt::Debug for Client<Proto> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let transport = match &self.transport {
            Transport::Direct(_) => "direct",
            Transport::Irpc(_) => "irpc",
            #[cfg(feature = "http")]
            Transport::Xrpc(_) => "xrpc",
        };
        formatter
            .debug_struct("Client")
            .field("transport", &transport)
            .field("event_buffer", &self.event_buffer)
            .finish()
    }
}

impl<Proto: Service> Client<Proto> {
    pub fn direct<R: InlineRoute<Proto> + 'static>(host: Arc<R>) -> Self {
        Self {
            transport: Transport::Direct(host),
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    pub fn irpc(client: irpc::Client<Proto>) -> Self {
        Self {
            transport: Transport::Irpc(client),
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    #[cfg(feature = "http")]
    pub fn xrpc(client: XrpcClient) -> Self {
        Self {
            transport: Transport::Xrpc(client),
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    pub fn with_event_buffer(mut self, events: usize) -> Self {
        assert!(events > 0, "event buffer must be positive");
        self.event_buffer = events;
        self
    }

    pub async fn call<P>(
        &self,
        context: RequestContext,
        params: P,
    ) -> Result<P::Output, ClientError<P::Error>>
    where
        P: Unary,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = Reply<P>, Rx = NoReceiver>,
    {
        self.call_with_metadata(Envelope::new(context, params))
            .await
            .map(|response| response.output)
    }

    pub async fn call_with_metadata<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<ResponseEnvelope<P::Output>, ClientError<P::Error>>
    where
        P: Unary,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = Reply<P>, Rx = NoReceiver>,
    {
        let context = request.context.clone();
        context.validate()?;
        let call = async {
            match &self.transport {
                Transport::Direct(host) => {
                    let (tx, rx) = oneshot::channel();
                    let message = Proto::Message::from(WithChannels::from((request, tx)));
                    Arc::clone(host).route_inline(message).await;
                    rx.await
                        .map_err(|error| ClientError::transport(error, false))?
                        .map_err(ClientError::Operation)
                }
                Transport::Irpc(client) => {
                    let _scope = request.context.cancellation.begin_transport_request()?;
                    client
                        .rpc(request)
                        .await
                        .map_err(|error| {
                            ClientError::transport(error, P::XRPC.kind.transport_outcome_unknown())
                        })?
                        .map_err(ClientError::Operation)
                }
                #[cfg(feature = "http")]
                Transport::Xrpc(client) => {
                    request.context.validate()?;
                    let Envelope {
                        context,
                        metadata,
                        params,
                        ..
                    } = request;
                    client
                        .execute_with_metadata::<P, P::Output, P::Error>(
                            P::XRPC,
                            context,
                            &metadata,
                            &params,
                        )
                        .await
                        .map_err(ClientError::from)
                }
            }
        };
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.expired() => Err(ContextError::DeadlineExceeded.into()),
            result = call => result,
        }
    }

    pub async fn subscribe<P>(
        &self,
        context: RequestContext,
        params: P,
    ) -> Result<ClientStream<P>, ClientError<P::Error>>
    where
        P: Subscription,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = EventReply<P>, Rx = NoReceiver>,
    {
        self.subscribe_with_metadata(Envelope::new(context, params))
            .await
            .map(|response| response.output)
    }

    pub async fn subscribe_with_metadata<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<ResponseEnvelope<ClientStream<P>>, ClientError<P::Error>>
    where
        P: Subscription,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = EventReply<P>, Rx = NoReceiver>,
    {
        let context = request.context.clone();
        context.validate()?;
        let call = async {
            match &self.transport {
                Transport::Direct(host) => {
                    let (tx, rx) = mpsc::channel(self.event_buffer);
                    let message = Proto::Message::from(WithChannels::from((request, tx)));
                    Arc::clone(host).route_inline(message).await;
                    ClientStream::open(rx, None, context.clone()).await
                }
                Transport::Irpc(client) => {
                    let scope = request.context.cancellation.begin_transport_request()?;
                    let receiver = client
                        .server_streaming(request, self.event_buffer)
                        .await
                        .map_err(|error| ClientError::transport(error, true))?;
                    ClientStream::open(receiver, scope, context.clone()).await
                }
                #[cfg(feature = "http")]
                Transport::Xrpc(client) => {
                    let Envelope {
                        context,
                        metadata,
                        params,
                        ..
                    } = request;
                    let receiver = client
                        .subscribe::<P>(context.clone(), &metadata, &params, self.event_buffer)
                        .await
                        .map_err(ClientError::from)?;
                    ClientStream::open(receiver, None, context.clone()).await
                }
            }
        };
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.expired() => Err(ContextError::DeadlineExceeded.into()),
            result = call => result,
        }
    }
}

#[cfg(feature = "http")]
impl<E> From<XrpcClientError<E>> for ClientError<E> {
    fn from(error: XrpcClientError<E>) -> Self {
        match error {
            XrpcClientError::Domain { error, .. } => Self::Operation(error),
            XrpcClientError::Encode(message) => Self::Transport {
                message,
                outcome_unknown: false,
            },
            XrpcClientError::Transport {
                message,
                outcome_unknown,
            }
            | XrpcClientError::Protocol {
                message,
                outcome_unknown,
            } => Self::Transport {
                message,
                outcome_unknown,
            },
            XrpcClientError::Http {
                status,
                body,
                outcome_unknown,
            } => Self::Transport {
                message: format!("HTTP {status}: {body}"),
                outcome_unknown,
            },
        }
    }
}

pub struct ClientStream<P: Subscription> {
    inner: BoxStream<'static, Result<P::Event, ClientError<P::Error>>>,
}

impl<P: Subscription> ClientStream<P> {
    async fn open(
        mut receiver: mpsc::Receiver<Result<EventFrame<P::Event>, P::Error>>,
        scope: Option<CancellationRequestGuard>,
        context: RequestContext,
    ) -> Result<ResponseEnvelope<Self>, ClientError<P::Error>> {
        let metadata = match receiver.recv().await {
            Ok(Some(Ok(EventFrame::Open(metadata)))) => metadata,
            Ok(Some(Err(error))) => return Err(ClientError::Operation(error)),
            Ok(Some(Ok(EventFrame::Event(_) | EventFrame::End))) => {
                return Err(ClientError::transport(
                    "subscription event preceded its response metadata",
                    false,
                ));
            }
            Ok(None) => {
                return Err(ClientError::transport(
                    "subscription closed before establishment",
                    false,
                ));
            }
            Err(error) => return Err(ClientError::transport(error, false)),
        };
        struct State<R> {
            receiver: R,
            context: RequestContext,
            _scope: Option<CancellationRequestGuard>,
        }
        let state = State {
            receiver,
            context,
            _scope: scope,
        };
        let inner = futures::stream::unfold(Some(state), |state| async move {
            let mut state = state?;
            let item = tokio::select! {
                biased;
                _ = state.context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
                _ = state.context.expired() => Err(ContextError::DeadlineExceeded.into()),
                received = state.receiver.recv() => match received {
                    Ok(Some(Ok(EventFrame::Event(event)))) => Ok(event),
                    Ok(Some(Ok(EventFrame::Open(_)))) => Err(ClientError::transport("subscription sent response metadata twice", false)),
                    Ok(Some(Ok(EventFrame::End))) => return None,
                    Ok(Some(Err(error))) => Err(ClientError::Operation(error)),
                    Ok(None) => Err(ClientError::transport("subscription closed without an end marker", false)),
                    Err(error) => Err(ClientError::transport(error, false)),
                },
            };
            let next = item.is_ok().then_some(state);
            Some((item, next))
        });
        Ok(ResponseEnvelope {
            output: Self {
                inner: Box::pin(inner),
            },
            metadata,
        })
    }

    pub async fn recv(&mut self) -> Result<Option<P::Event>, ClientError<P::Error>> {
        use futures::StreamExt;
        self.inner.next().await.transpose()
    }
}

impl<P: Subscription> Stream for ClientStream<P> {
    type Item = Result<P::Event, ClientError<P::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

impl<P: Subscription> std::fmt::Debug for ClientStream<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientStream")
            .field("nsid", &P::XRPC.nsid)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
