use futures::StreamExt;
use futures::future::BoxFuture;
use irpc::Channels;
use irpc::Service;
use irpc::WithChannels;
use irpc::channel::mpsc;
use irpc::channel::none::NoReceiver;
use irpc::channel::oneshot;
use method_core::ContextError;
use method_core::Subscription;
use method_core::Unary;
use service_client_core_rs::Subscribed;
use service_client_core_rs::SubscriptionTransport;
use service_client_core_rs::UnaryTransport;
use service_core_rs::CallOrigin;
use service_core_rs::ClientError;
use service_core_rs::Envelope;
use service_core_rs::EventFrame;
use service_core_rs::EventReply;
use service_core_rs::Reply;
use service_core_rs::RequestContext;
use service_core_rs::ResponseEnvelope;
use service_core_rs::TransportContext;
use std::future::Future;
use std::sync::Arc;

/// The client service code stores: the core client over the native [`Transport`] of `Proto`.
pub type Client<Proto> = service_client_core_rs::Client<Transport<Proto>>;

/// Routes one protocol message into a host in-process, without a wire.
pub trait InlineRoute<Proto: Service>: Send + Sync {
    fn route_inline(self: Arc<Self>, message: Proto::Message) -> BoxFuture<'static, ()>;
}

/// The in-process transports carry the [`CallOrigin`] their caller states, stamped on every
/// request as its [`TransportContext`]; the host trusts it because the message never left the
/// process. XRPC carries none: the server derives the transport context from the connection.
pub enum Transport<Proto: Service> {
    /// Messages are handed to a host in this process.
    Direct(Arc<dyn InlineRoute<Proto>>, CallOrigin),
    /// Messages go through a same-node IRPC queue.
    Irpc(irpc::Client<Proto>, CallOrigin),
    #[cfg(feature = "xrpc")]
    Xrpc(crate::xrpc::XrpcClient),
    /// XRPC to a peer over iroh streams; unary only.
    #[cfg(feature = "iroh")]
    Iroh(service_client_iroh_rs::IrohHttp),
}

impl<Proto: Service> Transport<Proto> {
    pub fn direct<R: InlineRoute<Proto> + 'static>(host: Arc<R>, origin: CallOrigin) -> Self {
        Self::Direct(host, origin)
    }

    pub fn irpc(client: irpc::Client<Proto>, origin: CallOrigin) -> Self {
        Self::Irpc(client, origin)
    }

    #[cfg(feature = "xrpc")]
    pub fn xrpc(client: crate::xrpc::XrpcClient) -> Self {
        Self::Xrpc(client)
    }

    #[cfg(feature = "iroh")]
    pub fn iroh(client: service_client_iroh_rs::IrohHttp) -> Self {
        Self::Iroh(client)
    }

    /// Establishes how `request` reaches the host, overwriting whatever the caller put there:
    /// only the transport says where a call comes from. XRPC sends no transport context at all.
    fn stamp<P>(&self, request: &mut Envelope<P>) {
        match self {
            Self::Direct(_, origin) => {
                request.set_transport(TransportContext::Direct { origin: *origin })
            }
            Self::Irpc(_, origin) => {
                request.set_transport(TransportContext::Irpc { origin: *origin })
            }
            #[cfg(feature = "xrpc")]
            Self::Xrpc(_) => {}
            #[cfg(feature = "iroh")]
            Self::Iroh(_) => {}
        }
    }
}

impl<Proto: Service> Clone for Transport<Proto> {
    fn clone(&self) -> Self {
        match self {
            Self::Direct(host, origin) => Self::Direct(Arc::clone(host), *origin),
            Self::Irpc(client, origin) => Self::Irpc(client.clone(), *origin),
            #[cfg(feature = "xrpc")]
            Self::Xrpc(client) => Self::Xrpc(client.clone()),
            #[cfg(feature = "iroh")]
            Self::Iroh(client) => Self::Iroh(client.clone()),
        }
    }
}

impl<Proto: Service> std::fmt::Debug for Transport<Proto> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Direct(..) => "direct",
            Self::Irpc(..) => "irpc",
            #[cfg(feature = "xrpc")]
            Self::Xrpc(_) => "xrpc",
            #[cfg(feature = "iroh")]
            Self::Iroh(_) => "iroh",
        })
    }
}

impl<Proto, P> UnaryTransport<P> for Transport<Proto>
where
    Proto: Service + From<Envelope<P>>,
    P: Unary,
    Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
    Envelope<P>: Channels<Proto, Tx = Reply<P>, Rx = NoReceiver>,
{
    async fn call(
        &self,
        mut request: Envelope<P>,
    ) -> Result<ResponseEnvelope<P::Output>, ClientError<P::Error>> {
        let context = request.context.clone();
        self.stamp(&mut request);
        within_deadline(
            &context,
            Box::pin(async {
                match self {
                    Self::Direct(host, _) => {
                        let (tx, rx) = oneshot::channel();
                        let message = Proto::Message::from(WithChannels::from((request, tx)));
                        Arc::clone(host).route_inline(message).await;
                        rx.await
                            .map_err(|error| ClientError::transport(error, false))?
                            .map_err(ClientError::Method)
                    }
                    Self::Irpc(client, _) => {
                        let _scope = request.context.cancellation.begin_transport_request()?;
                        client
                            .rpc(request)
                            .await
                            .map_err(|error| {
                                ClientError::transport(
                                    error,
                                    P::XRPC.kind.transport_outcome_unknown(),
                                )
                            })?
                            .map_err(ClientError::Method)
                    }
                    #[cfg(feature = "xrpc")]
                    Self::Xrpc(client) => {
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
                    #[cfg(feature = "iroh")]
                    Self::Iroh(client) => {
                        request.context.validate()?;
                        UnaryTransport::<P>::call(client, request).await
                    }
                }
            }),
        )
        .await
    }
}

impl<Proto, P> SubscriptionTransport<P> for Transport<Proto>
where
    Proto: Service + From<Envelope<P>>,
    P: Subscription,
    Proto::Message: From<WithChannels<Envelope<P>, Proto>>,
    Envelope<P>: Channels<Proto, Tx = EventReply<P>, Rx = NoReceiver>,
{
    async fn subscribe(
        &self,
        mut request: Envelope<P>,
        buffer: usize,
    ) -> Result<Subscribed<P>, ClientError<P::Error>> {
        let context = request.context.clone();
        self.stamp(&mut request);
        let subscribed = within_deadline(
            &context,
            Box::pin(async {
                match self {
                    Self::Direct(host, _) => {
                        let (tx, rx) = mpsc::channel(buffer);
                        let message = Proto::Message::from(WithChannels::from((request, tx)));
                        Arc::clone(host).route_inline(message).await;
                        Ok(frames(rx, None))
                    }
                    Self::Irpc(client, _) => {
                        let scope = request.context.cancellation.begin_transport_request()?;
                        let receiver = client
                            .server_streaming(request, buffer)
                            .await
                            .map_err(|error| ClientError::transport(error, true))?;
                        Ok(frames(receiver, scope))
                    }
                    #[cfg(feature = "xrpc")]
                    Self::Xrpc(client) => {
                        let Envelope {
                            context,
                            metadata,
                            params,
                            ..
                        } = request;
                        let receiver = client
                            .subscribe::<P>(context, &metadata, &params, buffer)
                            .await
                            .map_err(ClientError::from)?;
                        Ok(frames(receiver, None))
                    }
                    // `service_client_iroh_rs` carries unary calls only; see its crate docs.
                    #[cfg(feature = "iroh")]
                    Self::Iroh(_) => Err(ClientError::transport(
                        "XRPC subscriptions are not carried over iroh",
                        false,
                    )),
                }
            }),
        )
        .await?;
        Ok(bounded(subscribed, context))
    }
}

/// Fails with [`ContextError::DeadlineExceeded`] when `operation` outlives the context's
/// remaining budget. The transport enforces the deadline, as a gRPC channel does, so the core
/// client holds no timer and runs on a guest with no tokio runtime. Callers box `operation`: it
/// holds the whole request, and an unboxed copy per transport call stacks up through the nested
/// client calls of a node and overflows the stack of a debug build.
async fn within_deadline<T, E: From<ContextError>>(
    context: &RequestContext,
    operation: impl Future<Output = Result<T, ClientError<E>>>,
) -> Result<T, ClientError<E>> {
    match context.remaining_budget() {
        Some(budget) => n0_future::time::timeout(budget, operation)
            .await
            .unwrap_or_else(|_| Err(ContextError::DeadlineExceeded.into())),
        None => operation.await,
    }
}

/// Ends `subscribed` with [`ContextError::DeadlineExceeded`] when the budget runs out between
/// frames. A spent budget wins over a frame that is already buffered.
fn bounded<P: Subscription>(subscribed: Subscribed<P>, context: RequestContext) -> Subscribed<P> {
    let Subscribed { frames, scope } = subscribed;
    let frames = futures::stream::unfold((frames, context), |(mut frames, context)| async move {
        let frame = match context.remaining_budget() {
            Some(budget) if budget.is_zero() => Some(Err(ContextError::DeadlineExceeded.into())),
            Some(budget) => n0_future::time::timeout(budget, frames.next())
                .await
                .unwrap_or_else(|_| Some(Err(ContextError::DeadlineExceeded.into()))),
            None => frames.next().await,
        };
        frame.map(|frame| (frame, (frames, context)))
    });
    Subscribed {
        frames: frames.boxed(),
        scope,
    }
}

/// The frames of an irpc receiver as a [`Subscribed`]. The receiver closing ends the stream.
fn frames<P: Subscription>(
    receiver: mpsc::Receiver<Result<EventFrame<P::Event>, P::Error>>,
    scope: Option<service_core_rs::CancellationRequestGuard>,
) -> Subscribed<P> {
    let frames = futures::stream::unfold(receiver, |mut receiver| async move {
        let frame = match receiver.recv().await {
            Ok(Some(Ok(frame))) => Ok(frame),
            Ok(Some(Err(error))) => Err(ClientError::Method(error)),
            Ok(None) => return None,
            Err(error) => Err(ClientError::transport(error, false)),
        };
        Some((frame, receiver))
    });
    Subscribed {
        frames: frames.boxed(),
        scope,
    }
}

#[cfg(test)]
mod tests;
