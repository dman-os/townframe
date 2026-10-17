//! Serves a protocol from a same-node IRPC queue.
//!
//! Each received message is admitted against the bounded [`Admission`] and
//! then run on its own task; the receiving loop never executes a handler. The
//! loop and every task it admits are tracked by one [`StopToken`], which
//! cancels them cooperatively (handlers observe it through
//! [`Host::invoke`](crate::Host::invoke)) and then joins them.

use crate::{
    admission::Admission,
    host::{Dispatch, Host, Route},
    lifecycle::StopToken,
};
use std::sync::Arc;

pub struct IrpcAdapter;

impl IrpcAdapter {
    /// Serves `Proto` from an in-process queue of `queue` messages and returns
    /// the irpc client that feeds it. Hand the client to
    /// [`Client::irpc`](crate::Client::irpc).
    pub fn local<H, Proto>(
        host: Arc<Host<H>>,
        admission: Arc<Admission>,
        queue: usize,
    ) -> (irpc::Client<Proto>, StopToken)
    where
        H: Send + Sync + 'static,
        Proto: Route<H>,
    {
        let (sender, receiver) = tokio::sync::mpsc::channel(queue);
        let stop = Self::serve::<H, Proto>(host, admission, receiver);
        (irpc::Client::local(sender), stop)
    }

    /// Serves `Proto` from any message receiver. The loop ends when the
    /// receiver closes or the returned token is stopped.
    pub fn serve<H, Proto>(
        host: Arc<Host<H>>,
        admission: Arc<Admission>,
        mut receiver: tokio::sync::mpsc::Receiver<Proto::Message>,
    ) -> StopToken
    where
        H: Send + Sync + 'static,
        Proto: Route<H>,
    {
        let (stop, spawner) = StopToken::new();
        let loop_spawner = spawner.clone();
        spawner.spawn(async move {
            loop {
                let message = tokio::select! {
                    message = receiver.recv() => match message {
                        Some(message) => message,
                        None => break,
                    },
                    _ = loop_spawner.cancelled() => break,
                };
                let dispatch = Dispatch::Admitted {
                    admission: Arc::clone(&admission),
                    spawner: loop_spawner.clone(),
                };
                Proto::route(Arc::clone(&host), dispatch, message).await;
            }
        });
        stop
    }
}
