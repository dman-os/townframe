// Channel correlation is opt-in. It identifies message attempts and observations,
// not the queue's linearization order or a replayable serialization of payloads.

use crate::interlude::*;

#[cfg(not(feature = "channel-trace"))]
type Item<T> = T;
#[cfg(feature = "channel-trace")]
use trace::{Item, Trace};

#[derive(Debug)]
pub struct Receiver<T> {
    from: Arc<str>,
    inner: async_channel::Receiver<Item<T>>,
    #[cfg(feature = "channel-trace")]
    trace: Arc<Trace>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            from: Arc::clone(&self.from),
            inner: self.inner.clone(),
            #[cfg(feature = "channel-trace")]
            trace: Arc::clone(&self.trace),
        }
    }
}

/// Error recieving from actor {from}
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub struct RecvError {
    from: Arc<str>,
}

impl<T> Receiver<T> {
    pub async fn recv(&self) -> Result<T, RecvError> {
        let item = self.inner.recv().await.map_err(|_| RecvError {
            from: Arc::clone(&self.from),
        })?;
        #[cfg(feature = "channel-trace")]
        let item = self.trace.received(item);
        Ok(item)
    }

    pub fn try_recv(&self) -> Result<T, async_channel::TryRecvError> {
        let item = self.inner.try_recv()?;
        #[cfg(feature = "channel-trace")]
        let item = self.trace.received(item);
        Ok(item)
    }

    pub async fn recv_many(&self, limit: usize) -> Result<Vec<T>, RecvError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let first = self.recv().await?;
        let mut batch = Vec::with_capacity(limit.min(64));
        batch.push(first);
        while batch.len() < limit {
            match self.try_recv() {
                Ok(item) => batch.push(item),
                Err(async_channel::TryRecvError::Empty) => break,
                Err(async_channel::TryRecvError::Closed) => break,
            }
        }
        Ok(batch)
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    to: Arc<str>,
    inner: async_channel::Sender<Item<T>>,
    #[cfg(feature = "channel-trace")]
    trace: Arc<Trace>,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            to: Arc::clone(&self.to),
            inner: self.inner.clone(),
            #[cfg(feature = "channel-trace")]
            trace: Arc::clone(&self.trace),
        }
    }
}

/// Error sending to actor {to}
#[derive(Debug, thiserror::Error, displaydoc::Display)]
pub struct SendError {
    to: Arc<str>,
}

impl<T> Sender<T> {
    pub async fn send(&self, val: T) -> Result<(), SendError> {
        #[cfg(feature = "channel-trace")]
        let (val, mut attempt) = self.trace.attempt(val);
        let result = self.inner.send(val).await;
        #[cfg(feature = "channel-trace")]
        attempt.finished(if result.is_ok() {
            "send_complete"
        } else {
            "send_closed"
        });
        result.map_err(|_| SendError {
            to: Arc::clone(&self.to),
        })
    }
    pub fn try_send(&self, val: T) -> Result<(), SendError> {
        #[cfg(feature = "channel-trace")]
        let (val, mut attempt) = self.trace.attempt(val);
        let result = self.inner.try_send(val);
        #[cfg(feature = "channel-trace")]
        attempt.finished(match &result {
            Ok(()) => "send_complete",
            Err(async_channel::TrySendError::Full(_)) => "send_full",
            Err(async_channel::TrySendError::Closed(_)) => "send_closed",
        });
        result.map_err(|_| SendError {
            to: Arc::clone(&self.to),
        })
    }
}

pub fn bounded<T>(cap: usize, from: Arc<str>, to: Arc<str>) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = async_channel::bounded(cap);
    #[cfg(feature = "channel-trace")]
    let trace = Arc::new(Trace::new(Arc::clone(&from), Arc::clone(&to)));
    (
        Sender {
            inner: tx,
            to,
            #[cfg(feature = "channel-trace")]
            trace: Arc::clone(&trace),
        },
        Receiver {
            inner: rx,
            from,
            #[cfg(feature = "channel-trace")]
            trace,
        },
    )
}

pub fn unbounded<T>(from: Arc<str>, to: Arc<str>) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = async_channel::unbounded();
    #[cfg(feature = "channel-trace")]
    let trace = Arc::new(Trace::new(Arc::clone(&from), Arc::clone(&to)));
    (
        Sender {
            inner: tx,
            to,
            #[cfg(feature = "channel-trace")]
            trace: Arc::clone(&trace),
        },
        Receiver {
            inner: rx,
            from,
            #[cfg(feature = "channel-trace")]
            trace,
        },
    )
}

#[cfg(feature = "channel-trace")]
mod trace {
    use std::{
        marker::PhantomData,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    static NEXT_CHANNEL: AtomicU64 = AtomicU64::new(1);

    #[derive(Debug)]
    pub(super) struct Item<T> {
        id: u64,
        value: T,
    }

    #[derive(Debug)]
    pub(super) struct Trace {
        process: u32,
        id: u64,
        next_message: AtomicU64,
        from: Arc<str>,
        to: Arc<str>,
    }

    impl Trace {
        pub(super) fn new(from: Arc<str>, to: Arc<str>) -> Self {
            let id = NEXT_CHANNEL.fetch_add(1, Ordering::Relaxed);
            assert!(id != u64::MAX, "channel trace identity exhausted");
            Self {
                process: std::process::id(),
                id,
                next_message: AtomicU64::new(1),
                from,
                to,
            }
        }

        pub(super) fn attempt<T>(&self, value: T) -> (Item<T>, Attempt<'_, T>) {
            let id = self.next_message.fetch_add(1, Ordering::Relaxed);
            assert!(id != u64::MAX, "channel trace message identity exhausted");
            self.event::<T>(id, "send_attempt");
            (
                Item { id, value },
                Attempt {
                    trace: self,
                    id,
                    done: false,
                    marker: PhantomData,
                },
            )
        }

        pub(super) fn received<T>(&self, item: Item<T>) -> T {
            self.event::<T>(item.id, "receive");
            item.value
        }

        fn event<T>(&self, message: u64, phase: &'static str) {
            tracing::trace!(
                target: "big_sync_core::mpsc",
                process_id = self.process,
                channel_id = self.id,
                message_id = message,
                from = %self.from,
                to = %self.to,
                message_type = std::any::type_name::<T>(),
                phase,
                "actor channel edge"
            );
        }
    }

    pub(super) struct Attempt<'a, T> {
        trace: &'a Trace,
        id: u64,
        done: bool,
        marker: PhantomData<fn() -> T>,
    }

    impl<T> Attempt<'_, T> {
        pub(super) fn finished(&mut self, phase: &'static str) {
            self.done = true;
            self.trace.event::<T>(self.id, phase);
        }
    }

    impl<T> Drop for Attempt<'_, T> {
        fn drop(&mut self) {
            if !self.done {
                self.trace.event::<T>(self.id, "send_cancelled");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        task::{Context, Poll},
    };

    #[test]
    fn cancelling_a_blocked_send_does_not_publish_its_value() {
        let (sender, receiver) = bounded(1, "producer".into(), "consumer".into());
        sender.try_send(1).unwrap();
        {
            let mut blocked = std::pin::pin!(sender.send(2));
            let mut context = Context::from_waker(futures::task::noop_waker_ref());
            assert!(matches!(blocked.as_mut().poll(&mut context), Poll::Pending));
        }
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert_eq!(receiver.try_recv(), Err(async_channel::TryRecvError::Empty));
        sender.try_send(3).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 3);
        drop(sender);
        assert_eq!(
            receiver.try_recv(),
            Err(async_channel::TryRecvError::Closed)
        );
    }
}
