// TODO: use AtomicPtr to tag messages with send and recv
// counters to allow determinstic replay

use crate::interlude::*;

#[derive(Debug)]
pub struct Receiver<T> {
    from: Arc<str>,
    inner: async_channel::Receiver<T>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            from: Arc::clone(&self.from),
            inner: self.inner.clone(),
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
        self.inner.recv().await.map_err(|_| RecvError {
            from: Arc::clone(&self.from),
        })
    }

    pub fn try_recv(&self) -> Result<T, async_channel::TryRecvError> {
        self.inner.try_recv()
    }

    pub async fn recv_many(&self, limit: usize) -> Result<Vec<T>, RecvError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let first = self.recv().await?;
        let mut batch = Vec::with_capacity(limit.min(64));
        batch.push(first);
        while batch.len() < limit {
            match self.inner.try_recv() {
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
    inner: async_channel::Sender<T>,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            to: Arc::clone(&self.to),
            inner: self.inner.clone(),
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
        self.inner.send(val).await.map_err(|_| SendError {
            to: Arc::clone(&self.to),
        })
    }
    pub fn try_send(&self, val: T) -> Result<(), SendError> {
        self.inner.try_send(val).map_err(|_| SendError {
            to: Arc::clone(&self.to),
        })
    }
}

pub fn bounded<T>(cap: usize, from: Arc<str>, to: Arc<str>) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = async_channel::bounded(cap);
    (Sender { inner: tx, to }, Receiver { inner: rx, from })
}

pub fn unbounded<T>(from: Arc<str>, to: Arc<str>) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = async_channel::unbounded();
    (Sender { inner: tx, to }, Receiver { inner: rx, from })
}
