//! The one client type. `Client<Proto>` is named by the protocol, not by the
//! service that implements it, so a consumer can hold a `Client<DocsProtocol>`
//! whether the service runs in-process, in another process on the node, or
//! across the network.
//!
//! ```ignore
//! let docs: Client<DocsProtocol> = Client::direct(host);
//! let snapshot = docs.call(ctx.child(), GetDoc { id }).await?;
//! let mut events = docs.subscribe(ctx.child(), SubscribeDocs { after }).await?;
//! ```
//!
//! The request value selects the operation; the trait bounds on [`Client::call`]
//! are exactly the ones irpc's own `Client::rpc` demands and are satisfied by
//! the protocol's `#[rpc_requests]` derive.

use crate::{
    admission::CancellationRequestGuard,
    context::RequestContext,
    host::{Host, InlineRoute, Route},
    operation::{ClientError, Envelope, EventReply, Reply, Subscription, Unary},
    xrpc::{XrpcClient, XrpcClientError},
};
use futures::{Stream, stream::BoxStream};
use irpc::{
    Channels, Service, WithChannels,
    channel::{mpsc, none::NoReceiver, oneshot},
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

const DEFAULT_EVENT_BUFFER: usize = 64;

pub struct Client<Proto: Service> {
    transport: Transport<Proto>,
    event_buffer: usize,
}

enum Transport<Proto: Service> {
    Direct(Arc<dyn InlineRoute<Proto>>),
    Irpc(irpc::Client<Proto>),
    Xrpc(XrpcClient),
}

impl<Proto: Service> Clone for Client<Proto> {
    fn clone(&self) -> Self {
        let transport = match &self.transport {
            Transport::Direct(host) => Transport::Direct(Arc::clone(host)),
            Transport::Irpc(client) => Transport::Irpc(client.clone()),
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
    /// Calls run inline on the caller's task against a hosted handler.
    pub fn direct<H>(host: Arc<Host<H>>) -> Self
    where
        H: Send + Sync + 'static,
        Proto: Route<H>,
    {
        Self {
            transport: Transport::Direct(host),
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    /// Calls cross a same-node IRPC channel served by
    /// [`IrpcAdapter`](crate::IrpcAdapter).
    pub fn irpc(client: irpc::Client<Proto>) -> Self {
        Self {
            transport: Transport::Irpc(client),
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    /// Calls are canonical XRPC over HTTP.
    pub fn xrpc(client: XrpcClient) -> Self {
        Self {
            transport: Transport::Xrpc(client),
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    /// Buffered events per subscription before the producer is backpressured.
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

    /// [`Client::call`] with explicit transport metadata.
    pub async fn call_with_metadata<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<crate::ResponseEnvelope<P::Output>, ClientError<P::Error>>
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
            _ = context.cancellation.cancelled() => Err(crate::ContextError::Cancelled.into()),
            _ = context.expired() => Err(crate::ContextError::DeadlineExceeded.into()),
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
    ) -> Result<crate::ResponseEnvelope<ClientStream<P>>, ClientError<P::Error>>
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
            _ = context.cancellation.cancelled() => Err(crate::ContextError::Cancelled.into()),
            _ = context.expired() => Err(crate::ContextError::DeadlineExceeded.into()),
            result = call => result,
        }
    }
}

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

/// A client-side subscription. Items are events until a terminal item: an
/// operation error from the service or a transport failure. The stream ends
/// after a terminal item or when the service closes it.
pub struct ClientStream<P: Subscription> {
    inner: BoxStream<'static, Result<P::Event, ClientError<P::Error>>>,
}

impl<P: Subscription> ClientStream<P> {
    async fn open(
        mut receiver: mpsc::Receiver<Result<crate::EventFrame<P::Event>, P::Error>>,
        scope: Option<CancellationRequestGuard>,
        context: RequestContext,
    ) -> Result<crate::ResponseEnvelope<Self>, ClientError<P::Error>> {
        let metadata = match receiver.recv().await {
            Ok(Some(Ok(crate::EventFrame::Open(metadata)))) => metadata,
            Ok(Some(Err(error))) => return Err(ClientError::Operation(error)),
            Ok(Some(Ok(crate::EventFrame::Event(_) | crate::EventFrame::End))) => {
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
                _ = state.context.cancellation.cancelled() => Err(crate::ContextError::Cancelled.into()),
                _ = state.context.expired() => Err(crate::ContextError::DeadlineExceeded.into()),
                received = state.receiver.recv() => match received {
                    Ok(Some(Ok(crate::EventFrame::Event(event)))) => Ok(event),
                    Ok(Some(Ok(crate::EventFrame::Open(_)))) => Err(ClientError::transport(
                        "subscription sent response metadata twice",
                        false,
                    )),
                    Ok(Some(Ok(crate::EventFrame::End))) => return None,
                    Ok(Some(Err(error))) => Err(ClientError::Operation(error)),
                    Ok(None) => Err(ClientError::transport("subscription closed without an end marker", false)),
                    Err(error) => Err(ClientError::transport(error, false)),
                },
            };
            let next = item.is_ok().then_some(state);
            Some((item, next))
        });
        Ok(crate::ResponseEnvelope {
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
mod tests {
    use super::*;
    use crate::{
        ContextError, EventFrame, ResponseMetadata,
        sample::{SampleEvent, SubscribeEvents},
    };

    #[tokio::test]
    async fn end_marker_distinguishes_completion_from_truncation() {
        let event = SampleEvent {
            source_revision: 1,
            service_revision: 1,
            value: "one".into(),
        };
        for ended in [true, false] {
            let (sender, receiver) = mpsc::channel(3);
            sender
                .send(Ok(EventFrame::Open(ResponseMetadata::default())))
                .await
                .unwrap();
            sender
                .send(Ok(EventFrame::Event(event.clone())))
                .await
                .unwrap();
            if ended {
                sender.send(Ok(EventFrame::End)).await.unwrap();
            }
            drop(sender);
            let mut stream =
                ClientStream::<SubscribeEvents>::open(receiver, None, RequestContext::root())
                    .await
                    .unwrap()
                    .output;
            assert_eq!(stream.recv().await.unwrap(), Some(event.clone()));
            if !ended {
                assert_eq!(
                    stream.recv().await,
                    Err(ClientError::transport(
                        "subscription closed without an end marker",
                        false
                    ))
                );
            }
            assert_eq!(stream.recv().await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn malformed_establishment_and_duplicate_metadata_are_terminal() {
        for first in [
            EventFrame::End,
            EventFrame::Open(ResponseMetadata::default()),
        ] {
            let (sender, receiver) = mpsc::channel(2);
            sender.send(Ok(first.clone())).await.unwrap();
            sender
                .send(Ok(EventFrame::Open(ResponseMetadata::default())))
                .await
                .unwrap();
            drop(sender);
            let opened =
                ClientStream::<SubscribeEvents>::open(receiver, None, RequestContext::root()).await;
            match first {
                EventFrame::End => assert_eq!(
                    opened.unwrap_err(),
                    ClientError::transport(
                        "subscription event preceded its response metadata",
                        false
                    )
                ),
                EventFrame::Open(_) => {
                    let mut stream = opened.unwrap().output;
                    assert_eq!(
                        stream.recv().await,
                        Err(ClientError::transport(
                            "subscription sent response metadata twice",
                            false
                        ))
                    );
                    assert_eq!(stream.recv().await.unwrap(), None);
                }
                EventFrame::Event(_) => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn cancellation_preempts_buffered_events_and_is_terminal() {
        let context = RequestContext::root();
        let (sender, receiver) = mpsc::channel(2);
        sender
            .send(Ok(EventFrame::Open(ResponseMetadata::default())))
            .await
            .unwrap();
        sender
            .send(Ok(EventFrame::Event(SampleEvent {
                source_revision: 1,
                service_revision: 1,
                value: "buffered".into(),
            })))
            .await
            .unwrap();
        let mut stream = ClientStream::<SubscribeEvents>::open(receiver, None, context.clone())
            .await
            .unwrap()
            .output;
        context.cancel();
        assert_eq!(
            stream.recv().await,
            Err(ClientError::Operation(ContextError::Cancelled.into()))
        );
        sender.closed().await;
        assert_eq!(stream.recv().await.unwrap(), None);
    }
}
