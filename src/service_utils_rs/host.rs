//! The hosted service: where direct, IRPC, and HTTP requests converge.
//!
//! A [`Host`] owns the layered handler built with
//! [`ServiceBuilder`](crate::ServiceBuilder) and enforces the two guarantees
//! no transport may skip: the request context is validated before the stack
//! runs, and the stack is raced against request cancellation and host
//! shutdown. Everything else — authorization, tracing, idempotency — is a
//! [`Layer`](crate::Layer) inside the handler.
//!
//! [`Route`] is the one hand-written item per protocol. The protocol's message
//! enum (from irpc's `#[rpc_requests]`) is matched once, each arm forwarding to
//! [`Host::unary`] or [`Host::stream`]; `mount` lists the same operations for
//! the HTTP router. Its `where` clause is the honest statement of what a
//! service must implement to host that protocol.

use crate::{
    admission::{Admission, CancellationSignal},
    context::ContextError,
    lifecycle::{Spawner, StopToken},
    middleware::Handler,
    operation::{Envelope, EventReply, Operation, Reply, Subscription, Unary},
    xrpc::{XrpcRegistrationError, XrpcRouter},
};
use futures::{StreamExt, future::BoxFuture};
use irpc::{Channels, Service, WithChannels};
use std::sync::Arc;

/// How a routed message is executed.
#[derive(Clone)]
pub enum Dispatch {
    /// Run on the current task. Used by [`Client::direct`](crate::Client::direct):
    /// no queue, no permit, no spawn, and the caller's context is used as is.
    Inline,
    /// Admit against bounded permits, then run on a task owned by `spawner`.
    /// Used by the IRPC adapter; the envelope crosses a transport boundary so
    /// its deadline is rebuilt with [`Envelope::for_receiver`].
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
    /// Hosts a layered handler. The stop token joins the pump tasks that
    /// deliver direct subscriptions and cancels any call still in flight.
    pub fn new(handler: H) -> (Arc<Self>, StopToken) {
        let (stop, spawner) = StopToken::new();
        (Arc::new(Self { handler, spawner }), stop)
    }

    /// The single execution path. Validates the context, then runs the
    /// layered handler racing request cancellation and host shutdown.
    pub async fn invoke<P>(&self, request: Envelope<P>) -> Result<P::Output, P::Error>
    where
        P: Operation,
        H: Handler<P>,
    {
        request.context.validate()?;
        let cancellation = request.context.cancellation.clone();
        tokio::select! {
            result = self.handler.call(request) => result,
            _ = cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = self.spawner.cancelled() => Err(ContextError::Cancelled.into()),
        }
    }

    /// [`Host::invoke`] on a task owned by another component (the IRPC
    /// adapter): the owner's stop must also end the call, or its stop would
    /// wait on work only the host could cancel.
    async fn invoke_owned<P>(
        &self,
        request: Envelope<P>,
        owner: &Spawner,
    ) -> Result<P::Output, P::Error>
    where
        P: Operation,
        H: Handler<P>,
    {
        tokio::select! {
            result = self.invoke(request) => result,
            _ = owner.cancelled() => Err(ContextError::Cancelled.into()),
        }
    }

    /// Serves one unary message and replies on its channel.
    ///
    /// A failed reply means the caller dropped its receiver, which is the
    /// caller abandoning the request; there is nobody left to report that to.
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
        let WithChannels {
            inner: request, tx, ..
        } = message;
        match dispatch {
            Dispatch::Inline => {
                let result = self.invoke(request).await;
                drop(tx.send(result).await);
            }
            Dispatch::Admitted { admission, spawner } => {
                // Admission happens before spawning, so a bounded input queue
                // cannot be drained into an unbounded set of parked tasks.
                let permit = admission.acquire_unary().await;
                let registration =
                    match admission.register_cancellation(request.context.cancellation.clone()) {
                        Ok(registration) => registration,
                        Err(error) => {
                            drop(tx.send(Err(error.into())).await);
                            return;
                        }
                    };
                let activity = admission.activity();
                let admitted = activity.admitted();
                let spawned = activity.spawned();
                let task_spawner = spawner.clone();
                spawner.spawn(async move {
                    let _permit = permit;
                    let _admitted = admitted;
                    let _spawned = spawned;
                    let _registration = registration;
                    let _active = activity.active();
                    let result = self
                        .invoke_owned(request.for_receiver(), &task_spawner)
                        .await;
                    drop(tx.send(result).await);
                });
            }
        }
    }

    /// Serves one subscription message: the handler's stream is pumped into
    /// the reply channel by a tracked task until EOF, a terminal error,
    /// cancellation, or the receiver going away.
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
        let WithChannels {
            inner: request, tx, ..
        } = message;
        match dispatch {
            Dispatch::Inline => {
                let cancellation = request.context.cancellation.clone();
                let stream = match self.invoke(request).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        drop(tx.send(Err(error)).await);
                        return;
                    }
                };
                let spawner = self.spawner.clone();
                self.spawner
                    .spawn(pump::<P>(stream, tx, cancellation, spawner));
            }
            Dispatch::Admitted { admission, spawner } => {
                let Some(permit) = admission.try_acquire_stream() else {
                    let limit = admission.max_stream_pumps();
                    drop(
                        tx.send(Err(ContextError::StreamCapacityExceeded { limit }.into()))
                            .await,
                    );
                    return;
                };
                let registration =
                    match admission.register_cancellation(request.context.cancellation.clone()) {
                        Ok(registration) => registration,
                        Err(error) => {
                            drop(tx.send(Err(error.into())).await);
                            return;
                        }
                    };
                let pump_guard = admission.activity().stream_pump();
                let task_spawner = spawner.clone();
                spawner.spawn(async move {
                    let _permit = permit;
                    let _pump_guard = pump_guard;
                    let _registration = registration;
                    let request = request.for_receiver();
                    let cancellation = request.context.cancellation.clone();
                    let stream = match self.invoke_owned(request, &task_spawner).await {
                        Ok(stream) => stream,
                        Err(error) => {
                            drop(tx.send(Err(error)).await);
                            return;
                        }
                    };
                    pump::<P>(stream, tx, cancellation, task_spawner).await;
                });
            }
        }
    }
}

async fn pump<P: Subscription>(
    mut stream: P::Output,
    tx: EventReply<P>,
    cancellation: CancellationSignal,
    shutdown: Spawner,
) {
    loop {
        // A subscriber that went away is observed here, not only on the next
        // send, so an idle subscription releases its permit promptly.
        let item = tokio::select! {
            next = stream.next() => match next {
                Some(item) => item,
                None => break,
            },
            _ = tx.closed() => break,
            _ = cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = shutdown.cancelled() => Err(ContextError::Cancelled.into()),
        };
        let terminal = item.is_err();
        if tx.send(item).await.is_err() || terminal {
            break;
        }
    }
}

/// Binds a protocol enum to a hosted handler type.
///
/// ```ignore
/// impl<H> Route<H> for DocsProtocol
/// where
///     H: Handler<GetDoc> + Handler<WriteDoc> + Handler<SubscribeDocs>,
/// {
///     fn route(host: Arc<Host<H>>, dispatch: Dispatch, message: DocsMessage) -> BoxFuture<'static, ()> {
///         Box::pin(async move {
///             match message {
///                 DocsMessage::GetDoc(m) => host.unary(dispatch, m).await,
///                 DocsMessage::WriteDoc(m) => host.unary(dispatch, m).await,
///                 DocsMessage::SubscribeDocs(m) => host.stream(dispatch, m).await,
///             }
///         })
///     }
///
///     fn mount(router: &mut XrpcRouter, host: Arc<Host<H>>) -> Result<(), XrpcRegistrationError> {
///         router.mount::<GetDoc, H>(Arc::clone(&host))?;
///         router.mount::<WriteDoc, H>(host)
///     }
/// }
/// ```
pub trait Route<H>: Service {
    fn route(
        host: Arc<Host<H>>,
        dispatch: Dispatch,
        message: Self::Message,
    ) -> BoxFuture<'static, ()>;

    /// Registers the protocol's unary operations on an HTTP router.
    fn mount(router: &mut XrpcRouter, host: Arc<Host<H>>) -> Result<(), XrpcRegistrationError>;
}

/// Object-safe inline routing, so a [`Client`](crate::Client) can hold a host
/// without naming its handler type.
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
