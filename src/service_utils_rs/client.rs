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
    }

    /// [`Client::call`] with explicit transport metadata.
    pub async fn call_with_metadata<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<P::Output, ClientError<P::Error>>
    where
        P: Unary,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = Reply<P>, Rx = NoReceiver>,
    {
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
                } = request;
                client
                    .execute::<P, P::Output, P::Error>(P::XRPC, context, &metadata, &params)
                    .await
                    .map_err(ClientError::from)
            }
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
    }

    /// Opens a subscription. Establishment failures reported by the service
    /// arrive as the first, terminal item of the stream on every transport;
    /// `Err` here means the client itself could not send the request.
    pub async fn subscribe_with_metadata<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<ClientStream<P>, ClientError<P::Error>>
    where
        P: Subscription,
        Proto: From<Envelope<P>>,
        Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
        Envelope<P>: Channels<Proto, Tx = EventReply<P>, Rx = NoReceiver>,
    {
        match &self.transport {
            Transport::Direct(host) => {
                let (tx, rx) = mpsc::channel(self.event_buffer);
                let message = Proto::Message::from(WithChannels::from((request, tx)));
                Arc::clone(host).route_inline(message).await;
                Ok(ClientStream::new(rx, None))
            }
            Transport::Irpc(client) => {
                let scope = request.context.cancellation.begin_transport_request()?;
                let receiver = client
                    .server_streaming(request, self.event_buffer)
                    .await
                    .map_err(|error| ClientError::transport(error, true))?;
                Ok(ClientStream::new(receiver, scope))
            }
            Transport::Xrpc(_) => Err(ClientError::transport(
                "subscriptions are not supported by the unary HTTP transport",
                false,
            )),
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
    /// `scope` keeps the IRPC cancellation lease open for the life of the
    /// stream so a caller can still cancel after the subscription was
    /// established.
    fn new(
        receiver: mpsc::Receiver<Result<P::Event, P::Error>>,
        scope: Option<CancellationRequestGuard>,
    ) -> Self {
        struct State<R> {
            receiver: R,
            terminal: bool,
            _scope: Option<CancellationRequestGuard>,
        }
        let state = State {
            receiver,
            terminal: false,
            _scope: scope,
        };
        let inner = futures::stream::unfold(state, |mut state| async move {
            if state.terminal {
                return None;
            }
            let item = match state.receiver.recv().await {
                Ok(Some(Ok(event))) => Ok(event),
                Ok(Some(Err(error))) => Err(ClientError::Operation(error)),
                Ok(None) => return None,
                Err(error) => Err(ClientError::transport(error, false)),
            };
            state.terminal = item.is_err();
            Some((item, state))
        });
        Self {
            inner: Box::pin(inner),
        }
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
