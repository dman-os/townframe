use futures::Stream;
use futures::stream::BoxStream;
use method_core::ContextError;
use method_core::Method;
use method_core::Subscription;
use method_core::Unary;
use service_core_rs::CancellationRequestGuard;
use service_core_rs::ClientError;
use service_core_rs::Envelope;
use service_core_rs::EventFrame;
use service_core_rs::HttpMetadata;
use service_core_rs::RequestContext;
use service_core_rs::ResponseEnvelope;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

const DEFAULT_EVENT_BUFFER: usize = 64;

/// Carries one unary method `P` to its server and its response back.
///
/// The transport enforces the call's deadline (`context.remaining_budget()`), like a gRPC
/// channel does, and answers [`ContextError::DeadlineExceeded`] when it runs out. [`Client`]
/// only validates the context and honours cancellation, so it holds no timer and runs on a
/// guest with no tokio runtime.
///
/// The trait is generic over the method instead of object-safe. An in-process or irpc
/// transport moves the typed envelope without serializing it, and its bounds name the protocol
/// message conversions of each method, so they belong on the transport's own
/// `impl<P> UnaryTransport<P>`. A transport that only moves bytes implements
/// [`crate::xrpc::HttpTransport`] and is wrapped in [`crate::xrpc::Xrpc`].
///
/// The returned future carries no `Send` bound so that a wasm guest transport, whose futures are
/// not `Send`, can implement it. A native transport's future is `Send` when its parts are, and
/// callers holding a concrete [`Client`] see that.
pub trait UnaryTransport<P: Unary> {
    fn call(
        &self,
        request: Envelope<P>,
    ) -> impl Future<Output = Result<ResponseEnvelope<P::Output>, ClientError<P::Error>>>;
}

/// Opens the event stream of one subscription method `P`. See [`UnaryTransport`] for why this
/// is generic over the method. Like a unary transport it enforces the deadline, both while
/// opening and on every frame it yields afterwards.
pub trait SubscriptionTransport<P: Subscription> {
    /// `buffer` is the number of events the transport may hold ahead of the consumer.
    fn subscribe(
        &self,
        request: Envelope<P>,
        buffer: usize,
    ) -> impl Future<Output = Result<Subscribed<P>, ClientError<P::Error>>>;
}

type FrameStream<P> = BoxStream<
    'static,
    Result<EventFrame<<P as Subscription>::Event>, ClientError<<P as Method>::Error>>,
>;

/// An open subscription as a transport hands it over, before [`ClientStream`] has read the
/// response metadata from its first frame.
pub struct Subscribed<P: Subscription> {
    /// Frames in arrival order. A transport maps its failures to `Err`; ending the stream
    /// without an [`EventFrame::End`] is reported to the consumer as a truncated subscription.
    pub frames: FrameStream<P>,
    /// Keeps the transport-level cancellation request registered for the stream's lifetime.
    pub scope: Option<CancellationRequestGuard>,
}

#[derive(Debug, Clone)]
pub struct Client<T> {
    transport: T,
    event_buffer: usize,
}

impl<T> Client<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            event_buffer: DEFAULT_EVENT_BUFFER,
        }
    }

    pub fn with_event_buffer(mut self, events: usize) -> Self {
        assert!(events > 0, "event buffer must be positive");
        self.event_buffer = events;
        self
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub async fn call<P>(
        &self,
        context: RequestContext,
        params: P,
    ) -> Result<P::Output, ClientError<P::Error>>
    where
        P: Unary,
        T: UnaryTransport<P>,
    {
        self.call_with_metadata(Envelope::new(context, params))
            .await
            .map(|response| response.output)
    }

    /// [`Self::call`] with the request's HTTP metadata, which is where the caller's credential
    /// (`Authorization`) and idempotency key travel.
    pub async fn call_with<P>(
        &self,
        context: RequestContext,
        metadata: HttpMetadata,
        params: P,
    ) -> Result<P::Output, ClientError<P::Error>>
    where
        P: Unary,
        T: UnaryTransport<P>,
    {
        self.call_with_metadata(Envelope::with_metadata(context, metadata, params))
            .await
            .map(|response| response.output)
    }

    pub async fn call_with_metadata<P>(
        &self,
        request: Envelope<P>,
    ) -> Result<ResponseEnvelope<P::Output>, ClientError<P::Error>>
    where
        P: Unary,
        T: UnaryTransport<P>,
    {
        let context = request.context.clone();
        context.validate()?;
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            result = self.transport.call(request) => result,
        }
    }

    pub async fn subscribe<P>(
        &self,
        context: RequestContext,
        params: P,
    ) -> Result<ClientStream<P>, ClientError<P::Error>>
    where
        P: Subscription,
        T: SubscriptionTransport<P>,
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
        T: SubscriptionTransport<P>,
    {
        let context = request.context.clone();
        context.validate()?;
        let open = async {
            let subscribed = self.transport.subscribe(request, self.event_buffer).await?;
            ClientStream::open(subscribed, context.clone()).await
        };
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            result = open => result,
        }
    }
}

pub struct ClientStream<P: Subscription> {
    inner: BoxStream<'static, Result<P::Event, ClientError<P::Error>>>,
}

impl<P: Subscription> ClientStream<P> {
    /// Reads the response metadata from the first frame, then yields the events, ending at the
    /// end marker. Cancellation preempts buffered events and is terminal; so is the deadline, which the
    /// transport enforces on the frames it yields (see [`SubscriptionTransport`]).
    pub async fn open(
        subscribed: Subscribed<P>,
        context: RequestContext,
    ) -> Result<ResponseEnvelope<Self>, ClientError<P::Error>> {
        use futures::StreamExt;
        let Subscribed { mut frames, scope } = subscribed;
        let metadata = match frames.next().await {
            Some(Ok(EventFrame::Open(metadata))) => metadata,
            Some(Err(error)) => return Err(error),
            Some(Ok(EventFrame::Event(_) | EventFrame::End)) => {
                return Err(ClientError::transport(
                    "subscription event preceded its response metadata",
                    false,
                ));
            }
            None => {
                return Err(ClientError::transport(
                    "subscription closed before establishment",
                    false,
                ));
            }
        };
        struct State<S> {
            frames: S,
            context: RequestContext,
            _scope: Option<CancellationRequestGuard>,
        }
        let state = State {
            frames,
            context,
            _scope: scope,
        };
        let inner = futures::stream::unfold(Some(state), |state| async move {
            let mut state = state?;
            let item = tokio::select! {
                biased;
                _ = state.context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
                frame = state.frames.next() => match frame {
                    Some(Ok(EventFrame::Event(event))) => Ok(event),
                    Some(Ok(EventFrame::Open(_))) => Err(ClientError::transport("subscription sent response metadata twice", false)),
                    Some(Ok(EventFrame::End)) => return None,
                    Some(Err(error)) => Err(error),
                    None => Err(ClientError::transport("subscription closed without an end marker", false)),
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
